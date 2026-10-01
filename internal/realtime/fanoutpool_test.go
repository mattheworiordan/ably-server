package realtime

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// appendRound appends cm and waits for want frames to be queued. It
// returns how many of them the fan-out pool queued.
func (r *fanoutRig) appendRound(tb testing.TB, cm *protocol.ChannelMessage, want int) int64 {
	tb.Helper()
	before := r.pooled.Load()
	r.queued.Add(want)
	r.ch.Append(cm)
	waitGroup(tb, &r.queued)
	return r.pooled.Load() - before
}

func waitGroup(tb testing.TB, wg *sync.WaitGroup) {
	tb.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		tb.Fatal("frames not queued in time")
	}
}

// warmPool appends message cms until one round is queued entirely by the
// fan-out pool: every attachment has joined it (each joins on the Next
// after the frame its goroutine delivered, once any hold after a refused
// entry has passed). It allows 300 rounds, more than the 64-entry hold a
// refused append sets (core fanoutHoldEntries), which
// TestFanoutPoolAppendDelta waits out. The caller drains the frames
// queued.
func (r *fanoutRig) warmPool(tb testing.TB, perRound int) {
	tb.Helper()
	for i := range 300 {
		cm := &protocol.ChannelMessage{
			ChannelSerial: fmt.Sprintf("warm%03d", i),
			Messages:      []*protocol.Message{{Name: "warm", Serial: fmt.Sprintf("warm%03d:000", i), ConnectionID: "conn-other"}},
		}
		if r.appendRound(tb, cm, perRound) == int64(perRound) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Fatal("the attachments did not join the fan-out pool")
}

func decodeFrames(tb testing.TB, fs []outFrame) []*protocol.ProtocolMessage {
	tb.Helper()
	out := make([]*protocol.ProtocolMessage, len(fs))
	for i, f := range fs {
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(f.data, protocol.FormatJSON, &m); err != nil {
			tb.Fatal(err)
		}
		out[i] = &m
	}
	return out
}

// TestFanoutPoolEchoSuppressed checks echo=false under the fan-out pool
// (DESIGN.md §2.1, §5.1): a pooled attachment whose connection published
// a message does not receive it, receives everyone else's, and the other
// attachments receive both, all as frames queued by the pool.
func TestFanoutPoolEchoSuppressed(t *testing.T) {
	const n = 10
	pool := core.NewFanoutPool(4, 2)
	r := newFanoutRigWith(t, n, []protocol.Format{protocol.FormatJSON}, protocol.FlagSubscribe, rigOptions{
		pool:  pool,
		setup: func(i int, a *attachment) { a.echo = i != 0 },
	})
	defer r.close()
	r.warmPool(t, n)
	r.drain()

	own := &protocol.ChannelMessage{ChannelSerial: "s1", Messages: []*protocol.Message{{Name: "own", Serial: "s1:000", ConnectionID: "conn-0"}}}
	if got := r.appendRound(t, own, n-1); got != n-1 {
		t.Fatalf("pool queued %d of %d frames for conn-0's message", got, n-1)
	}
	other := &protocol.ChannelMessage{ChannelSerial: "s2", Messages: []*protocol.Message{{Name: "other", Serial: "s2:000", ConnectionID: "conn-5"}}}
	if got := r.appendRound(t, other, n); got != n {
		t.Fatalf("pool queued %d of %d frames for conn-5's message", got, n)
	}
	for i, fs := range r.drain() {
		var names []string
		for _, m := range decodeFrames(t, fs) {
			names = append(names, m.Messages[0].Name)
		}
		want := "[own other]"
		if i == 0 {
			want = "[other]"
		}
		if got := fmt.Sprint(names); got != want {
			t.Errorf("attachment %d received %s, want %s", i, got, want)
		}
	}
}

// TestFanoutPoolAppendDelta checks a pooled channel carrying appends
// (DESIGN.md §5.1, §13.3): the pool records the messages it delivers in
// each attachment's seen set, hands an append back to the attachment
// goroutines, which send the delta to an attachment that saw the message
// and the full version under appendMode=full, and the attachments rejoin
// for the next message, each receiving every cm once and in order.
func TestFanoutPoolAppendDelta(t *testing.T) {
	const n = 6
	pool := core.NewFanoutPool(3, 2)
	r := newFanoutRigWith(t, n, []protocol.Format{protocol.FormatJSON}, protocol.FlagSubscribe, rigOptions{
		pool:  pool,
		setup: func(i int, a *attachment) { a.appendModeFull = i == n-1 },
	})
	defer r.close()
	r.warmPool(t, n)
	r.drain()

	create := &protocol.ChannelMessage{ChannelSerial: "s1", Messages: []*protocol.Message{{Action: protocol.MessageCreate, Serial: "s1:000", Data: "start"}}}
	if got := r.appendRound(t, create, n); got != n {
		t.Fatalf("pool queued %d of %d frames for the create", got, n)
	}
	delta := &protocol.Message{Action: protocol.MessageAppend, Serial: "s1:000", Data: "+more"}
	appendCM := &protocol.ChannelMessage{ChannelSerial: "s2", Messages: []*protocol.Message{{
		Action: protocol.MessageUpdate, Serial: "s1:000", Data: "start+more",
		Alt: map[string]*protocol.Message{protocol.DeltaAppend: delta},
	}}}
	if got := r.appendRound(t, appendCM, n); got != 0 {
		t.Fatalf("pool queued %d frames for an append; want 0 (per-attachment path)", got)
	}
	// The goroutines rejoin once they have delivered the append.
	r.warmPool(t, n)

	for i, fs := range r.drain() {
		ms := decodeFrames(t, fs)
		if len(ms) < 3 || ms[0].ChannelSerial != "s1" || ms[1].ChannelSerial != "s2" {
			t.Fatalf("attachment %d: frames %v, want s1, s2, then the warm-up", i, serialsOf(ms))
		}
		for j := 2; j < len(ms); j++ {
			if ms[j].ChannelSerial <= ms[j-1].ChannelSerial && j > 2 {
				t.Fatalf("attachment %d: frames out of order: %v", i, serialsOf(ms))
			}
		}
		got := ms[1].Messages[0]
		if i == n-1 {
			if got.Action != protocol.MessageUpdate || got.Data != "start+more" {
				t.Errorf("appendMode=full attachment got %v %v, want the full version", got.Action, got.Data)
			}
		} else if got.Action != protocol.MessageAppend || got.Data != "+more" {
			t.Errorf("attachment %d got %v %v, want the delta (the pool recorded the create as seen)", i, got.Action, got.Data)
		}
	}
}

func serialsOf(ms []*protocol.ProtocolMessage) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ChannelSerial
	}
	return out
}

// TestFanoutPoolFramesMatchGoroutinePath checks that a frame the
// fan-out pool queues is the frame the attachment goroutines queue for
// the same cm (DESIGN.md §5.1): for a message and a presence cm, on JSON
// and msgpack connections, the pooled frames decode to the goroutine
// path's frames, and within a format every pooled attachment shares one
// encoding.
func TestFanoutPoolFramesMatchGoroutinePath(t *testing.T) {
	const n = 12
	formats := []protocol.Format{protocol.FormatJSON, protocol.FormatMsgpack}
	modes := protocol.FlagSubscribe | protocol.FlagPresenceSubscribe
	cms := []*protocol.ChannelMessage{
		{ChannelSerial: "s1", Messages: []*protocol.Message{{Name: "m", Data: "hello", ID: "id1", Serial: "s1:000", ConnectionID: "conn-x", Timestamp: 1}}},
		{ChannelSerial: "s2", Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "c1", ConnectionID: "conn-x", Data: "hi", ID: "p1", Timestamp: 2}}},
	}
	frames := func(pool *core.FanoutPool) [][]outFrame {
		r := newFanoutRigWith(t, n, formats, modes, rigOptions{shared: true, pool: pool})
		defer r.close()
		if pool != nil {
			r.warmPool(t, n)
			r.drain()
		}
		for _, cm := range cms {
			got := r.appendRound(t, cm, n)
			if pool != nil && got != n {
				t.Fatalf("pool queued %d of %d frames for %s", got, n, cm.ChannelSerial)
			}
		}
		return r.drain()
	}
	pooled := frames(core.NewFanoutPool(4, 2))
	own := frames(nil)
	for i := range n {
		format := formats[i%len(formats)]
		if len(pooled[i]) != len(cms) || len(own[i]) != len(cms) {
			t.Fatalf("attachment %d: %d pooled / %d goroutine frames, want %d", i, len(pooled[i]), len(own[i]), len(cms))
		}
		for k := range cms {
			if !bytes.Equal(pooled[i][k].data, own[i][k].data) {
				t.Errorf("attachment %d (%v), %s: pooled frame %q, goroutine frame %q", i, format, cms[k].ChannelSerial, pooled[i][k].data, own[i][k].data)
			}
			if first := pooled[i%len(formats)][k].data; &first[0] != &pooled[i][k].data[0] {
				t.Errorf("attachment %d (%v), %s: its own encoding; want the format's shared one", i, format, cms[k].ChannelSerial)
			}
		}
	}
}

// TestFanoutPoolOrderAcrossBoundary checks per-attachment order and
// exactly-once delivery while attachments move between the pool and
// their own goroutines (DESIGN.md §5.1): appends (handed back),
// presence and messages (pooled) are interleaved, and a third of the
// connections have queues too small for a second frame, so the pool's
// non-waiting push fails and their goroutines push under backpressure
// while the queues are drained concurrently. An append every 7th entry
// sets the 64-entry hold (core fanoutHoldEntries) again soon after each
// rejoin, so the attachments are pooled in short windows between holds;
// the test asserts that both paths and the full queue were exercised.
func TestFanoutPoolOrderAcrossBoundary(t *testing.T) {
	const (
		n       = 12
		entries = 600
	)
	pool := core.NewFanoutPool(4, 4)
	r := newFanoutRigWith(t, n, []protocol.Format{protocol.FormatJSON}, protocol.FlagSubscribe|protocol.FlagPresenceSubscribe, rigOptions{pool: pool})
	defer r.close()
	r.warmPool(t, n)
	r.drain()
	for i, c := range r.conns {
		if i%3 == 0 {
			c.out.max = 1
		}
	}

	// Drain every queue concurrently, as a write loop would.
	got := make([][]outFrame, n)
	stop := make(chan struct{})
	var drainers sync.WaitGroup
	for i, c := range r.conns {
		drainers.Go(func() {
			for {
				if f, ok := c.out.pop(); ok {
					got[i] = append(got[i], f)
					continue
				}
				select {
				case <-c.out.ready:
				case <-stop:
					for f, ok := c.out.pop(); ok; f, ok = c.out.pop() {
						got[i] = append(got[i], f)
					}
					return
				}
			}
		})
	}

	r.queued.Add(n * entries)
	pooledBefore := r.pooled.Load()
	for k := 1; k <= entries; k++ {
		serial := fmt.Sprintf("x%05d", k)
		cm := &protocol.ChannelMessage{ChannelSerial: serial}
		switch {
		case k%7 == 0:
			// An append to the previous message: always the full version
			// here or a delta; either way the per-attachment path.
			full := &protocol.Message{Action: protocol.MessageUpdate, Serial: fmt.Sprintf("x%05d:000", k-1), Data: "v"}
			full.Alt = map[string]*protocol.Message{protocol.DeltaAppend: {Action: protocol.MessageAppend, Serial: full.Serial, Data: "+"}}
			cm.Messages = []*protocol.Message{full}
		case k%11 == 0:
			cm.Presence = []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "c", ConnectionID: "conn-x", ID: serial}}
		default:
			cm.Messages = []*protocol.Message{{Name: "m", Serial: serial + ":000", ConnectionID: "conn-x"}}
		}
		r.ch.Append(cm)
		if k%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	waitGroup(t, &r.queued)
	close(stop)
	drainers.Wait()

	pooled := r.pooled.Load() - pooledBefore
	if pooled == 0 || pooled == n*entries || r.full.Load() == 0 {
		t.Fatalf("pool queued %d of %d frames and found %d queues full; the test must exercise the pool, the handback and the full queue", pooled, n*entries, r.full.Load())
	}
	for i := range got {
		ms := decodeFrames(t, got[i])
		if len(ms) != entries {
			t.Fatalf("attachment %d received %d frames, want %d", i, len(ms), entries)
		}
		for k, m := range ms {
			if want := fmt.Sprintf("x%05d", k+1); m.ChannelSerial != want {
				t.Fatalf("attachment %d: frame %d is %s, want %s (duplicate, gap or reorder)", i, k, m.ChannelSerial, want)
			}
		}
	}
}

// TestFanoutPoolWebSocketExactlyOnce is the end-to-end check of the
// fan-out pool (DESIGN.md §5.1): 2,000 WebSocket subscribers on one
// channel, over the default threshold of 1,000, receive every message a
// WebSocket publisher sends, exactly once and in publish order, with the
// pool queuing nearly all of the frames. The publisher is itself
// attached with echo=false and receives none of its own messages.
func TestFanoutPoolWebSocketExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("2,000 WebSocket connections")
	}
	const (
		subscribers = 2000
		messages    = 30
		channel     = "pool-e2e"
	)
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := core.NewFanoutPool(4, core.DefaultFanoutThreshold)
	defer pool.Close()
	manager := core.NewManagerWithOptions(memory.New(memory.Options{}), core.Options{FanoutPool: pool})
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	open := func(query url.Values) (ws *websocket.Conn, err error) {
		u, _ := url.Parse(srv.URL)
		u.Scheme = "ws"
		query.Set("key", testKey)
		u.RawQuery = query.Encode()
		// A dial that fails is retried: 2,000 dials can meet a dropped SYN
		// on a busy loopback, and the dial is not what is under test.
		dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
		for attempt := 0; ; attempt++ {
			ws, _, err = dialer.Dial(u.String(), nil)
			if err == nil {
				break
			}
			if attempt == 2 {
				return nil, err
			}
		}
		read := func(want protocol.Action) error {
			_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, data, err := ws.ReadMessage()
			if err != nil {
				return err
			}
			var m protocol.ProtocolMessage
			if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
				return err
			}
			if m.Action != want {
				return fmt.Errorf("got %v, want %v", m.Action, want)
			}
			return nil
		}
		if err := read(protocol.ActionConnected); err != nil {
			ws.Close()
			return nil, err
		}
		data, _ := protocol.Marshal(&protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: new(channel)}, protocol.FormatJSON)
		if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
			ws.Close()
			return nil, err
		}
		if err := read(protocol.ActionAttached); err != nil {
			ws.Close()
			return nil, err
		}
		return ws, nil
	}

	subs := make([]*websocket.Conn, subscribers)
	var dialers sync.WaitGroup
	errs := make(chan error, subscribers)
	next := make(chan int)
	for range 32 {
		dialers.Go(func() {
			for i := range next {
				ws, err := open(url.Values{})
				if err != nil {
					errs <- fmt.Errorf("subscriber %d: %w", i, err)
					continue
				}
				subs[i] = ws
			}
		})
	}
	for i := range subs {
		next <- i
	}
	close(next)
	dialers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	defer func() {
		for _, ws := range subs {
			ws.Close()
		}
	}()
	pub, err := open(url.Values{"echo": {"false"}})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	msgSerial := int64(0)
	publish := func(name string) {
		data, _ := protocol.Marshal(&protocol.ProtocolMessage{
			Action:    protocol.ActionMessage,
			Channel:   new(channel),
			MsgSerial: &msgSerial,
			Messages:  []*protocol.Message{{Name: name, Data: "payload"}},
		}, protocol.FormatJSON)
		if err := pub.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
		msgSerial++
	}
	// readMessages reads want MESSAGE frames from ws, skipping nothing:
	// any other frame is an error.
	readMessages := func(ws *websocket.Conn, want int) ([]string, []string, error) {
		var names, serials []string
		for len(names) < want {
			_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, data, err := ws.ReadMessage()
			if err != nil {
				return names, serials, err
			}
			var m protocol.ProtocolMessage
			if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
				return names, serials, err
			}
			if m.Action != protocol.ActionMessage {
				return names, serials, fmt.Errorf("got %v, want MESSAGE", m.Action)
			}
			for _, msg := range m.Messages {
				names = append(names, msg.Name)
			}
			serials = append(serials, m.ChannelSerial)
		}
		return names, serials, nil
	}

	// Warm-up: each subscriber's goroutine joins the pool on the Next
	// after its first delivery (the first 1,000 were parked before the
	// channel passed the threshold).
	for w := range 2 {
		publish(fmt.Sprintf("warm-%d", w))
		for i, ws := range subs {
			if _, _, err := readMessages(ws, 1); err != nil {
				t.Fatalf("subscriber %d warm-up: %v", i, err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	before := pool.Deliveries()
	for k := range messages {
		publish(fmt.Sprintf("m-%02d", k))
	}
	for i, ws := range subs {
		names, serials, err := readMessages(ws, messages)
		if err != nil {
			t.Fatalf("subscriber %d after %d messages: %v", i, len(names), err)
		}
		for k, name := range names {
			if want := fmt.Sprintf("m-%02d", k); name != want {
				t.Fatalf("subscriber %d: message %d is %s, want %s (duplicate, gap or reorder)", i, k, name, want)
			}
			if k > 0 && serials[k] <= serials[k-1] {
				t.Fatalf("subscriber %d: channelSerial %s after %s", i, serials[k], serials[k-1])
			}
		}
	}
	// Nothing further for anyone: no duplicate after the last message, and
	// no self-echo to the publisher (it reads ACKs only).
	for i, ws := range append([]*websocket.Conn{pub}, subs[:50]...) {
		_ = ws.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				break
			}
			var m protocol.ProtocolMessage
			_ = protocol.Unmarshal(data, protocol.FormatJSON, &m)
			if m.Action == protocol.ActionMessage {
				t.Fatalf("connection %d (0 is the publisher): unexpected MESSAGE %s", i, m.ChannelSerial)
			}
		}
	}
	// Every subscriber, and the publisher's echo=false skip, through the
	// pool; allow a few attachments that had not rejoined.
	want := uint64((subscribers + 1) * messages)
	if got := pool.Deliveries() - before; got < want*9/10 {
		t.Fatalf("pool delivered %d of %d entries; want nearly all", got, want)
	}
}
