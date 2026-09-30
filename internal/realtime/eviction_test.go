package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// End-to-end idle-channel eviction through the WebSocket (DESIGN.md
// §5.1): the races named in the scale plan, each driven by real clients.

const evictIdle = 60 * time.Millisecond

// releaseGateStorage wraps the memory backend so a test can hold an
// eviction's storage Release open (the window in which the channel is
// marked evicted but not yet released) and count binds.
type releaseGateStorage struct {
	storage.Storage

	mu       sync.Mutex
	gate     chan struct{} // Release blocks on it when non-nil
	entered  chan string   // receives the name when a Release starts (non-blocking send)
	binds    map[string]int
	releases map[string]int
}

func newReleaseGateStorage() *releaseGateStorage {
	return &releaseGateStorage{
		Storage:  memory.New(memory.Options{}),
		entered:  make(chan string, 16),
		binds:    make(map[string]int),
		releases: make(map[string]int),
	}
}

func (s *releaseGateStorage) Channel(ctx context.Context, name string, a storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	s.binds[name]++
	s.mu.Unlock()
	return s.Storage.Channel(ctx, name, a)
}

func (s *releaseGateStorage) Release(ctx context.Context, name string) error {
	s.mu.Lock()
	gate := s.gate
	s.mu.Unlock()
	select {
	case s.entered <- name:
	default:
	}
	if gate != nil {
		<-gate
	}
	err := s.Storage.Release(ctx, name)
	s.mu.Lock()
	s.releases[name]++
	s.mu.Unlock()
	return err
}

func (s *releaseGateStorage) setGate(g chan struct{}) {
	s.mu.Lock()
	s.gate = g
	s.mu.Unlock()
}

func (s *releaseGateStorage) counts(name string) (binds, releases int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binds[name], s.releases[name]
}

type evictHarness struct {
	srv     *httptest.Server
	rt      *Server
	manager *core.Manager
	store   *releaseGateStorage
}

func newEvictServer(t *testing.T) *evictHarness {
	t.Helper()
	return newEvictServerIdle(t, evictIdle)
}

func newEvictServerIdle(t *testing.T, idle time.Duration) *evictHarness {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	st := newReleaseGateStorage()
	manager := core.NewManagerWithOptions(st, core.Options{IdleTimeout: idle, SweepInterval: 5 * time.Millisecond})
	t.Cleanup(manager.Close)
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &evictHarness{srv: srv, rt: rt, manager: manager, store: st}
}

// waitBound waits until the Manager holds exactly n bound channels.
func (h *evictHarness) waitBound(t *testing.T, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for h.manager.BoundChannels() != n {
		if time.Now().After(deadline) {
			t.Fatalf("BoundChannels = %d after %v, want %d", h.manager.BoundChannels(), within, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// publishVia publishes one message on a server-side Channel handle and
// returns its serial.
func (h *evictHarness) publishVia(t *testing.T, channel, data string) string {
	t.Helper()
	ctx := context.Background()
	ch, err := h.manager.GetChannel(ctx, channel)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	cm, _, err := ch.Publish(ctx, []*protocol.Message{{Data: data}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return cm.ChannelSerial
}

func attachAt(t *testing.T, ws *websocket.Conn, channel, channelSerial string) *protocol.ProtocolMessage {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       new(channel),
		ChannelSerial: channelSerial,
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED on %q, got %v (%+v)", channel, msg.Action, msg.Error)
	}
	return msg
}

func detach(t *testing.T, ws *websocket.Conn, channel string) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: new(channel)})
	for {
		if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action == protocol.ActionDetached {
			return
		}
	}
}

func readMessageData(t *testing.T, ws *websocket.Conn) (serial string, data any) {
	t.Helper()
	for {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionMessage {
			return f.Messages[0].Serial, f.Messages[0].Data
		}
	}
}

// An evicted channel re-seeds from the store's watermark on its next
// attach, and a client resuming from a serial it saw before the
// eviction receives every message committed since, then live messages.
func TestEvictionResumeAfterRebindDeliversEverything(t *testing.T) {
	h := newEvictServer(t)
	ws := dial(t, h.srv, "")
	drainConnected(t, ws)

	attached := attachAt(t, ws, "room", "")
	first := h.publishVia(t, "room", "m1")
	if serial, _ := readMessageData(t, ws); serial != first+":000" {
		t.Fatalf("live message serial = %q, want %q", serial, first+":000")
	}
	lastSeen := first
	detach(t, ws, "room")
	h.waitBound(t, 0, 2*time.Second)

	// Published while this client is away. Each publish binds the channel
	// and it is evicted again once idle.
	m2 := h.publishVia(t, "room", "m2")
	m3 := h.publishVia(t, "room", "m3")
	h.waitBound(t, 0, 2*time.Second)
	if binds, releases := h.store.counts("room"); binds < 2 || releases < 2 {
		t.Fatalf("binds=%d releases=%d, want at least 2 of each (evicted twice)", binds, releases)
	}

	resumed := attachAt(t, ws, "room", lastSeen)
	if resumed.Flags&protocol.FlagResumed == 0 {
		t.Fatalf("resume ATTACHED flags = %b, want RESUMED (attach point %q, first attach %q)", resumed.Flags, resumed.ChannelSerial, attached.ChannelSerial)
	}
	for _, want := range []string{m2, m3} {
		if serial, _ := readMessageData(t, ws); serial != want+":000" {
			t.Fatalf("replayed serial = %q, want %q", serial, want+":000")
		}
	}
	m4 := h.publishVia(t, "room", "m4")
	if serial, _ := readMessageData(t, ws); serial != m4+":000" {
		t.Fatalf("live serial after resume = %q, want %q", serial, m4+":000")
	}
}

// An ATTACH that arrives while the channel is being evicted waits for
// the storage Release to finish, then binds afresh and is live.
func TestEvictionAttachDuringEviction(t *testing.T) {
	h := newEvictServer(t)
	gate := make(chan struct{})
	h.store.setGate(gate)
	h.publishVia(t, "room", "m1")

	select {
	case <-h.store.entered: // the sweeper is inside Release, holding it open
	case <-time.After(2 * time.Second):
		t.Fatal("eviction did not start")
	}

	ws := dial(t, h.srv, "")
	drainConnected(t, ws)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: new("room")})
	next := readAsync(ws)
	assertPending(t, next, 50*time.Millisecond) // ATTACH waits for the Release

	h.store.setGate(nil)
	close(gate)
	if f := awaitFrame(t, next); f.Action != protocol.ActionAttached {
		t.Fatalf("frame = %v, want ATTACHED once the release finished", f.Action)
	}
	m2 := h.publishVia(t, "room", "m2")
	if serial, _ := readMessageData(t, ws); serial != m2+":000" {
		t.Fatalf("live serial on rebound channel = %q, want %q", serial, m2+":000")
	}
	if binds, releases := h.store.counts("room"); binds != 2 || releases != 1 {
		t.Fatalf("binds=%d releases=%d, want 2 and 1", binds, releases)
	}
}

// A publish that arrives while the channel is being evicted is held
// until the Release finishes, then commits on the rebound channel and
// is ACKed; it is not lost and not written to released storage.
func TestEvictionPublishDuringEviction(t *testing.T) {
	h := newEvictServer(t)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)

	// Bind "room" through a subscriber, then let it go idle.
	attachAt(t, sub, "room", "")
	detach(t, sub, "room")

	gate := make(chan struct{})
	h.store.setGate(gate)
	select {
	case <-h.store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("eviction did not start")
	}

	sendPublish(t, pub, "room", 1, "during")
	next := readAsync(pub)
	assertPending(t, next, 50*time.Millisecond) // the publish waits for the Release

	h.store.setGate(nil)
	close(gate)
	ack := awaitFrame(t, next)
	if ack.Action != protocol.ActionAck || ack.GetMsgSerial() != 1 {
		t.Fatalf("frame = %v/%d, want ACK/1", ack.Action, ack.GetMsgSerial())
	}
	// The publish is in the channel's log: a rewind attach replays it.
	sendFrame(t, sub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: new("room"),
		Params:  map[string]string{"rewind": "1"},
	})
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAttached {
		t.Fatalf("frame = %v, want ATTACHED", f.Action)
	}
	if _, data := readMessageData(t, sub); data != "during" {
		t.Fatalf("rewound message data = %v, want %q", data, "during")
	}
}

// A channel with a presence member is not evicted, even with no
// attachment: here the member's connection dropped abruptly and the
// member is held for the presence grace window (DESIGN.md §12.5). Once
// the delayed LEAVE lands the channel is evicted.
func TestEvictionHeldOffByPresenceMember(t *testing.T) {
	h := newEvictServer(t)
	const grace = 1500 * time.Millisecond
	h.rt.remainPresentFor = grace

	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	_ = pub.Close() // abrupt: the member stays for the grace window
	dropped := time.Now()

	// Well past the idle timeout (60ms) and well inside the grace
	// window, the channel must still be bound.
	time.Sleep(5 * evictIdle)
	if elapsed := time.Since(dropped); elapsed >= grace/2 {
		t.Fatalf("test machine too slow: %v elapsed, cannot check inside the %v grace window", elapsed, grace)
	}
	if got := h.manager.BoundChannels(); got != 1 {
		t.Fatalf("BoundChannels = %d during the presence grace window, want 1", got)
	}
	if _, releases := h.store.counts("room"); releases != 0 {
		t.Fatalf("channel with a presence member was released %d times", releases)
	}
	h.waitBound(t, 0, grace+2*time.Second)
	if time.Since(dropped) < grace {
		t.Fatalf("channel evicted after %v, before the member's grace window (%v) ended", time.Since(dropped), grace)
	}
}

// A realtime client that publishes to a channel without attaching keeps
// it bound while it publishes more often than the idle timeout; once it
// stops the channel is evicted, and its next publish rebinds and is
// ACKed.
func TestEvictionRealtimePublisherWithoutSubscriber(t *testing.T) {
	// A longer idle timeout than the other tests, so a slow publish+ACK
	// round trip under -race cannot let the channel fall idle between
	// publishes.
	const idle = 400 * time.Millisecond
	h := newEvictServerIdle(t, idle)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)

	var msgSerial int64
	for range 10 {
		msgSerial++
		sendPublish(t, pub, "feed", msgSerial, "tick")
		if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
			t.Fatalf("frame = %v, want ACK", ack.Action)
		}
		time.Sleep(idle / 8)
	}
	if binds, releases := h.store.counts("feed"); binds != 1 || releases != 0 {
		t.Fatalf("while publishing: binds=%d releases=%d, want 1 and 0", binds, releases)
	}

	h.waitBound(t, 0, 2*time.Second)
	msgSerial++
	sendPublish(t, pub, "feed", msgSerial, "again")
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck || ack.GetMsgSerial() != msgSerial {
		t.Fatalf("frame = %v/%d, want ACK/%d after rebind", ack.Action, ack.GetMsgSerial(), msgSerial)
	}
	if binds, _ := h.store.counts("feed"); binds != 2 {
		t.Fatalf("binds = %d, want 2 (first use + rebind)", binds)
	}
}

type frameResult struct {
	msg *protocol.ProtocolMessage
	err error
}

// readAsync reads the next frame on a goroutine, so a test can assert
// that nothing has arrived yet without a read deadline (a timed-out read
// leaves a gorilla/websocket conn unusable).
func readAsync(ws *websocket.Conn) <-chan frameResult {
	out := make(chan frameResult, 1)
	go func() {
		_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := ws.ReadMessage()
		if err != nil {
			out <- frameResult{err: err}
			return
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
			out <- frameResult{err: err}
			return
		}
		out <- frameResult{msg: &m}
	}()
	return out
}

func assertPending(t *testing.T, next <-chan frameResult, within time.Duration) {
	t.Helper()
	select {
	case r := <-next:
		t.Fatalf("expected no frame yet, got %+v (err %v)", r.msg, r.err)
	case <-time.After(within):
	}
}

func awaitFrame(t *testing.T, next <-chan frameResult) *protocol.ProtocolMessage {
	t.Helper()
	select {
	case r := <-next:
		if r.err != nil {
			t.Fatalf("read: %v", r.err)
		}
		return r.msg
	case <-time.After(5 * time.Second):
		t.Fatal("no frame within 5s")
		return nil
	}
}
