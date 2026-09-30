package loadgen_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/loadgen"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/server"
)

const testKey = "app.key:secret"

// startServer runs an in-process ably-server in memory mode and returns
// its host:port.
func startServer(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	var out bytes.Buffer
	var outMu sync.Mutex
	go func() {
		done <- server.Run(ctx, server.Opts{
			Args:   []string{"--mode=memory", "--listen=127.0.0.1:0", "--log-level=error"},
			Getenv: func(k string) string { return map[string]string{"ABLY_SERVER_KEYS": testKey}[k] },
			Out:    lockedWriter{&out, &outMu},
			Ready:  ready,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})
	select {
	case addr := <-ready:
		return addr.String()
	case code := <-done:
		outMu.Lock()
		defer outMu.Unlock()
		t.Fatalf("server exited (%d): %s", code, out.String())
	case <-time.After(10 * time.Second):
		t.Fatal("server not ready")
	}
	return ""
}

type lockedWriter struct {
	b  *bytes.Buffer
	mu *sync.Mutex
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// frameCollector records frames handed to a Conn's handler.
type frameCollector struct {
	mu     sync.Mutex
	frames []*protocol.ProtocolMessage
	signal chan struct{}
}

func newCollector() *frameCollector { return &frameCollector{signal: make(chan struct{}, 1000)} }

func (f *frameCollector) OnFrame(_ *loadgen.Conn, pm *protocol.ProtocolMessage) {
	f.mu.Lock()
	f.frames = append(f.frames, pm)
	f.mu.Unlock()
	f.signal <- struct{}{}
}

func (f *frameCollector) waitFor(t *testing.T, pred func(*protocol.ProtocolMessage) bool) *protocol.ProtocolMessage {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		f.mu.Lock()
		frames := append([]*protocol.ProtocolMessage(nil), f.frames...)
		f.mu.Unlock()
		for _, pm := range frames {
			if pred(pm) {
				return pm
			}
		}
		select {
		case <-f.signal:
		case <-deadline:
			t.Fatal("timed out waiting for frame")
			return nil
		}
	}
}

func (f *frameCollector) messages(channel string) []loadgen.Payload {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []loadgen.Payload
	for _, pm := range f.frames {
		if pm.Action != protocol.ActionMessage || pm.GetChannel() != channel {
			continue
		}
		for _, m := range pm.Messages {
			if p, ok := loadgen.PayloadFromData(m.Data); ok {
				out = append(out, p)
			}
		}
	}
	return out
}

func dial(t *testing.T, addr string, h loadgen.Handler) (*loadgen.Conn, chan error) {
	t.Helper()
	c, err := loadgen.Dial(context.Background(), loadgen.DialConfig{
		Endpoint: addr, Key: testKey, Format: protocol.FormatMsgpack, HandshakeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.ConnectionID == "" || c.ConnectionKey == "" || c.IdleTimeout <= 0 {
		t.Fatalf("CONNECTED details missing: %+v", c)
	}
	errc := make(chan error, 1)
	go func() { errc <- c.ReadLoop(context.Background(), h) }()
	t.Cleanup(func() { _ = c.Abort() })
	return c, errc
}

func publish(t *testing.T, c *loadgen.Conn, channel string, seq int64) {
	t.Helper()
	done := make(chan error, 1)
	data := loadgen.EncodePayload(loadgen.Payload{PubID: "p", Seq: seq, SentAtUS: time.Now().UnixMicro(), Node: 0}, 64)
	msg := &protocol.Message{ID: loadgen.MessageID("p", seq), Name: "lg", Data: data}
	if err := c.Publish(channel, []*protocol.Message{msg}, func(err error) { done <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("publish %d: %v", seq, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no ACK for publish %d", seq)
	}
}

func TestConnPublishSubscribeAndResume(t *testing.T) {
	addr := startServer(t)
	const ch = "lgtest-conn"

	sub := newCollector()
	subConn, subErr := dial(t, addr, sub)
	if err := subConn.Attach(ch, "", protocol.FlagSubscribe); err != nil {
		t.Fatal(err)
	}
	sub.waitFor(t, func(pm *protocol.ProtocolMessage) bool { return pm.Action == protocol.ActionAttached })

	pubConn, _ := dial(t, addr, newCollector())
	for i := int64(0); i < 3; i++ {
		publish(t, pubConn, ch, i)
	}
	last := sub.waitFor(t, func(pm *protocol.ProtocolMessage) bool {
		return pm.Action == protocol.ActionMessage && len(sub.messages(ch)) == 3
	})
	_ = last
	got := sub.messages(ch)
	for i, p := range got {
		if p.Seq != int64(i) || p.PubID != "p" {
			t.Fatalf("delivery %d = %+v", i, p)
		}
	}
	// Last serial seen, for the resume.
	var lastSerial string
	sub.mu.Lock()
	for _, pm := range sub.frames {
		if pm.Action == protocol.ActionMessage {
			lastSerial = pm.ChannelSerial
		}
	}
	sub.mu.Unlock()

	// Drop the subscriber abruptly, publish while it is away, then
	// resume from the last serial: the gap must be replayed and RESUMED set.
	_ = subConn.Abort()
	<-subErr
	for i := int64(3); i < 6; i++ {
		publish(t, pubConn, ch, i)
	}
	sub2 := newCollector()
	subConn2, _ := dial(t, addr, sub2)
	if err := subConn2.Attach(ch, lastSerial, protocol.FlagSubscribe); err != nil {
		t.Fatal(err)
	}
	att := sub2.waitFor(t, func(pm *protocol.ProtocolMessage) bool { return pm.Action == protocol.ActionAttached })
	if att.Flags&protocol.FlagResumed == 0 {
		t.Fatalf("ATTACHED after resume lacks RESUMED: flags=%b", att.Flags)
	}
	sub2.waitFor(t, func(*protocol.ProtocolMessage) bool { return len(sub2.messages(ch)) >= 3 })
	for i, p := range sub2.messages(ch) {
		if p.Seq != int64(3+i) {
			t.Fatalf("replayed delivery %d = %+v, want seq %d", i, p, 3+i)
		}
	}

	// A republish with the same id is deduplicated by the server.
	publish(t, pubConn, ch, 5)
	publish(t, pubConn, ch, 6)
	sub2.waitFor(t, func(*protocol.ProtocolMessage) bool { return len(sub2.messages(ch)) >= 4 })
	time.Sleep(200 * time.Millisecond)
	if n := len(sub2.messages(ch)); n != 4 {
		t.Fatalf("after an idempotent republish got %d deliveries, want 4: %+v", n, sub2.messages(ch))
	}
}

func TestConnPingAndClose(t *testing.T) {
	addr := startServer(t)
	c, errc := dial(t, addr, newCollector())
	if err := c.Ping("abc"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatal(err)
	}
	select {
	case <-errc:
	case <-time.After(5 * time.Second):
		t.Fatal("ReadLoop did not return after Close")
	}
	if err := c.Attach("x", "", 0); err != loadgen.ErrConnClosed {
		t.Fatalf("write after close: %v", err)
	}
}

func TestDialRejectsBadKey(t *testing.T) {
	addr := startServer(t)
	_, err := loadgen.Dial(context.Background(), loadgen.DialConfig{
		Endpoint: addr, Key: "app.key:wrong", Format: protocol.FormatMsgpack, HandshakeTimeout: 5 * time.Second,
	})
	if err == nil {
		t.Fatal("dial with a bad key succeeded")
	}
	if pe, ok := err.(*loadgen.ProtocolError); !ok || pe.Info.Code/100 != 401 {
		t.Fatalf("want a 401xx protocol error, got %v", err)
	}
}

const e2eScenario = `
name = "E"
shape = "M"
message_bytes = 300
sample_percent = 50

[timing]
ramp = "1s"
hold = "4s"
drain = "1500ms"

[connections]
count = 24

[churn]
connects_per_sec = 4
channel_opens_per_sec = 4
resume = true

[[class]]
name = "hot"
channels = 1
subscribers = 12
publish_rate = 20
publisher = "realtime"
streams = 2
scale_by = "subscribers"

[[class]]
name = "tail"
channels = 40
subscribers_min = 0
subscribers_max = 2
subscribers_dist = "uniform"
publish_rate_total = 120

[presence]
enabled = true
channels = 2
members_per_channel = 3
events_per_sec = 6
subscribe = true
`

func TestJobsEndToEnd(t *testing.T) {
	addr := startServer(t)
	sc, err := loadgen.ParseScenario([]byte(e2eScenario))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().Add(500 * time.Millisecond).UnixMicro()
	base := loadgen.JobSpec{
		RunID: "e2e", RunTag: "e2e1", Scenario: *sc, Endpoints: []string{addr, addr},
		Key: testKey, StartAtUS: start, Workers: 8, RealtimeConns: 2,
	}
	specs := []loadgen.JobSpec{}
	for i := 0; i < 2; i++ {
		s := base
		s.ID, s.Role, s.Index, s.Count = fmt.Sprintf("sub-%d", i), loadgen.RoleSubscriber, i, 2
		specs = append(specs, s)
	}
	for _, r := range []string{loadgen.RoleREST, loadgen.RoleRealtime, loadgen.RolePresence} {
		s := base
		s.ID, s.Role, s.Index, s.Count = r, r, 0, 1
		specs = append(specs, s)
	}
	m := loadgen.NewMetrics()
	summaries := make([]*loadgen.Summary, len(specs))
	var wg sync.WaitGroup
	for i, s := range specs {
		job, err := loadgen.NewJob(s, m)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); summaries[i] = job.Run(context.Background()) }()
	}
	wg.Wait()

	published := map[string]map[string]loadgen.StreamRecord{}
	var seen []map[string]*loadgen.ChannelSeen
	var received, acked, resumes, checked, opens int64
	for _, s := range summaries {
		for k, n := range s.Correctness.Violations {
			if n != 0 {
				t.Errorf("%s: %s = %d; first: %+v; errors: %v", s.Role, k, n, s.Correctness.FirstViolations, s.Errors)
			}
		}
		received += s.Deliveries.Received
		acked += s.Publishes.Acked
		resumes += s.Correctness.Resumes
		checked += s.Correctness.CheckedMessages
		opens += s.Attachments.ChannelOpens
		for ch, m := range s.Streams {
			if published[ch] == nil {
				published[ch] = map[string]loadgen.StreamRecord{}
			}
			for p, r := range m {
				published[ch][p] = r
			}
		}
		if s.Role == loadgen.RoleSubscriber {
			seen = append(seen, s.Correctness.Channels)
			if s.Connections.OpenAtMeasureEnd != int64(s.Connections.Target) {
				t.Errorf("subscriber %d: %d of %d connections open at the end of the hold", s.Index, s.Connections.OpenAtMeasureEnd, s.Connections.Target)
			}
			if s.Latency[loadgen.LatDelivery].Count() == 0 || s.Latency[loadgen.LatConnectAttach].Count() == 0 {
				t.Errorf("subscriber %d recorded no latency", s.Index)
			}
		}
		if s.Role == loadgen.RoleREST && (s.Publishes.AchievedRate < 0.8*s.Publishes.TargetRate || s.Publishes.Unresolved != 0) {
			t.Errorf("rest publisher achieved %.1f of %.1f/s, unresolved %d: %v", s.Publishes.AchievedRate, s.Publishes.TargetRate, s.Publishes.Unresolved, s.Errors)
		}
		if s.Role == loadgen.RolePresence && (s.Presence.Entered < int64(s.Presence.Members) || s.Presence.Left == 0) {
			t.Errorf("presence: %+v", s.Presence)
		}
	}
	if received == 0 || acked == 0 || checked == 0 {
		t.Fatalf("received=%d acked=%d checked=%d", received, acked, checked)
	}
	if resumes == 0 || opens == 0 {
		t.Errorf("churn did not run: resumes=%d opens=%d", resumes, opens)
	}
	tail := loadgen.TailCheck(published, seen, 500*time.Millisecond)
	if tail.Lost != 0 {
		t.Errorf("tail loss: %+v", tail)
	}
	if tail.Checked == 0 {
		t.Errorf("tail check covered nothing")
	}
	t.Logf("received=%d acked=%d checked=%d resumes=%d opens=%d tail=%+v", received, acked, checked, resumes, opens, tail)
}

// fakeServer speaks just enough of the protocol to deliver a scripted
// sequence of messages on one channel, including a duplicate, a gap and
// a reorder.
func fakeServer(t *testing.T, script []int64) string {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.Close() }()
		send := func(pm *protocol.ProtocolMessage) {
			b, _ := protocol.Marshal(pm, protocol.FormatMsgpack)
			_ = ws.WriteMessage(websocket.BinaryMessage, b)
		}
		send(&protocol.ProtocolMessage{Action: protocol.ActionConnected, ConnectionID: "fake", ConnectionDetails: &protocol.ConnectionDetails{ConnectionKey: "fake!k", MaxIdleIntervalMs: 15000}})
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var in protocol.ProtocolMessage
			if protocol.Unmarshal(data, protocol.FormatMsgpack, &in) != nil || in.Action != protocol.ActionAttach {
				continue
			}
			ch := in.GetChannel()
			send(&protocol.ProtocolMessage{Action: protocol.ActionAttached, Channel: &ch, ChannelSerial: fmt.Sprintf("%014d-000@f", 0), Flags: protocol.FlagSubscribe})
			go func() {
				time.Sleep(700 * time.Millisecond) // inside the measurement window
				for i, seq := range script {
					data := loadgen.EncodePayload(loadgen.Payload{PubID: "e2e2.0", Seq: seq, SentAtUS: time.Now().UnixMicro(), Node: 0}, 50)
					send(&protocol.ProtocolMessage{
						Action: protocol.ActionMessage, Channel: &ch,
						ChannelSerial: fmt.Sprintf("%014d-000@f", i+1),
						Messages:      []*protocol.Message{{Name: "lg", Data: data}},
					})
				}
			}()
		}
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestSubscriberJobDetectsInjectedFaults(t *testing.T) {
	// duplicate 1; gap at 3 (never sent); 6 arrives after 7 (reorder).
	addr := fakeServer(t, []int64{0, 1, 1, 2, 4, 5, 7, 6, 8, 9})
	sc, err := loadgen.ParseScenario([]byte(`
name = "F"
shape = "M"
message_bytes = 50
sample_percent = 100
[timing]
ramp = "100ms"
hold = "1500ms"
drain = "500ms"
[connections]
count = 1
[[class]]
name = "one"
channels = 1
subscribers = 1
`))
	if err != nil {
		t.Fatal(err)
	}
	job, err := loadgen.NewJob(loadgen.JobSpec{
		ID: "sub", RunTag: "e2e2", Scenario: *sc, Role: loadgen.RoleSubscriber, Count: 1,
		Endpoints: []string{addr}, Key: testKey, StartAtUS: time.Now().UnixMicro(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := job.Run(context.Background())
	v := s.Correctness.Violations
	if v["duplicate"] != 1 || v["gap"] != 1 || v["reorder"] != 1 || v["serial_regression"] != 0 {
		t.Fatalf("violations = %v, want duplicate=1 gap=1 reorder=1; first: %+v; errors: %v", v, s.Correctness.FirstViolations, s.Errors)
	}
	if s.Deliveries.Received != 10 {
		t.Errorf("received %d, want 10", s.Deliveries.Received)
	}
}
