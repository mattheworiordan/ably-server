package realtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// Presence liveness at the connection layer (DESIGN.md §12.5): the
// grace-window LEAVE decided from the connection's own entered set, its
// error counter, the resume hand-over, and lease-lapse re-entry.

// faultyStorage wraps the memory backend so a test can make every
// channel's Members read, or its presence writes, fail.
type faultyStorage struct {
	storage.Storage
	failMembers  atomic.Bool
	failPresence atomic.Bool
	// presenceHook, when set, wraps every StorePresence: store writes
	// the operation to the memory backend, and the hook decides when to
	// call it and what to return.
	presenceHook atomic.Pointer[presenceHookFunc]
	// storeHook, when set, runs before every message Store (a test
	// blocks the publish worker with it).
	storeHook atomic.Pointer[func()]
}

type presenceHookFunc func(ctx context.Context, p []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error)

// storeFunc writes presence messages to the memory backend: by default
// the ones the hook was given (store(nil)), or others (store(copies)).
type storeFunc func(p []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error)

func (s *faultyStorage) setPresenceHook(f presenceHookFunc) {
	if f == nil {
		s.presenceHook.Store(nil)
		return
	}
	s.presenceHook.Store(&f)
}

func (s *faultyStorage) Channel(ctx context.Context, name string, a storage.Appender) (storage.ChannelStore, error) {
	cs, err := s.Storage.Channel(ctx, name, a)
	if err != nil {
		return nil, err
	}
	return &faultyChannel{ChannelStore: cs, s: s}, nil
}

type faultyChannel struct {
	storage.ChannelStore
	s *faultyStorage
}

var errInjected = errors.New("test: injected storage failure")

func (c *faultyChannel) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	if c.s.failMembers.Load() {
		return nil, "", errInjected
	}
	return c.ChannelStore.Members(ctx)
}

func (c *faultyChannel) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if h := c.s.storeHook.Load(); h != nil {
		(*h)()
	}
	return c.ChannelStore.Store(ctx, msgs)
}

func (c *faultyChannel) StorePresence(ctx context.Context, p []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if c.s.failPresence.Load() {
		return nil, false, errInjected
	}
	if h := c.s.presenceHook.Load(); h != nil {
		return (*h)(ctx, p, func(q []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
			if q == nil {
				q = p
			}
			return c.ChannelStore.StorePresence(context.WithoutCancel(ctx), q)
		})
	}
	return c.ChannelStore.StorePresence(ctx, p)
}

type livenessHarness struct {
	srv     *httptest.Server
	rt      *Server
	store   *faultyStorage
	manager *core.Manager
	metrics *metrics.Metrics
}

func newLivenessServer(t *testing.T, grace time.Duration) *livenessHarness {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	st := &faultyStorage{Storage: memory.New(memory.Options{})}
	manager := core.NewManager(st)
	t.Cleanup(manager.Close)
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), m, nil)
	rt.remainPresentFor = grace
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &livenessHarness{srv: srv, rt: rt, store: st, manager: manager, metrics: m}
}

// metricLine returns the exposition line for name{labels} ("" if absent).
func (h *livenessHarness) metricLine(t *testing.T, series string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, series+" ") {
			return line
		}
	}
	return ""
}

// TestGraceLeaveWithoutMembersRead: the grace-window LEAVE of an
// abruptly dropped connection is decided from the connection's own
// entered set, so it is still published when the store's member read
// fails (it used to be dropped without a trace, leaving the member
// behind).
func TestGraceLeaveWithoutMembersRead(t *testing.T) {
	const grace = 300 * time.Millisecond
	h := newLivenessServer(t, grace)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence || f.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("frame = %+v, want ENTER", f)
	}

	h.store.failMembers.Store(true)
	_ = pub.Close() // abrupt

	f := readFrame(t, sub, protocol.FormatJSON, grace+2*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceLeave || f.Presence[0].ClientID != "alice" {
		t.Fatalf("frame = %+v, want alice's LEAVE", f)
	}
}

// TestGraceLeaveErrorCounted: a grace-window LEAVE the store refuses is
// logged and counted in ably_presence_grace_leave_errors_total{stage}.
func TestGraceLeaveErrorCounted(t *testing.T) {
	const grace = 200 * time.Millisecond
	h := newLivenessServer(t, grace)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	h.store.failPresence.Store(true)
	_ = pub.Close()
	const series = `ably_presence_grace_leave_errors_total{stage="publish"}`
	deadline := time.Now().Add(grace + 3*time.Second)
	for h.metricLine(t, series) != series+" 1" {
		if time.Now().After(deadline) {
			t.Fatalf("%s = %q, want 1", series, h.metricLine(t, series))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestGraceResumeTakesOverMembers: a connection that resumes the dropped
// connectionId within the grace window, without re-entering, keeps the
// member: no LEAVE at the window's end, and the resumed connection owns
// it, so its own clean close leaves it at once.
func TestGraceResumeTakesOverMembers(t *testing.T) {
	const grace = 600 * time.Millisecond
	h := newLivenessServer(t, grace)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, h.srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if connected.ConnectionDetails == nil {
		t.Fatalf("first frame = %+v, want CONNECTED with details", connected)
	}
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("frame = %+v, want ENTER", f)
	}

	_ = pub.Close()
	// Let the dropped connection's teardown schedule the grace LEAVE
	// before the resume arrives.
	time.Sleep(grace / 4)
	pub2 := dialResumeClientID(t, h.srv, connected.ConnectionDetails.ConnectionKey, "alice")
	if f := readFrame(t, pub2, protocol.FormatJSON, 2*time.Second); f.ConnectionID != connected.ConnectionID {
		t.Fatalf("resumed connectionId = %q, want %q", f.ConnectionID, connected.ConnectionID)
	}

	// Past the window: no LEAVE.
	if err := sub.SetReadDeadline(time.Now().Add(grace + 300*time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, data, err := sub.ReadMessage(); err == nil {
		t.Fatalf("frame after a resume within the grace window: %s (want no LEAVE)", data)
	}
	_ = sub.Close()

	// The resumed connection owns alice: a clean close leaves her now.
	ch, err := h.manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if members, _, err := ch.Members(context.Background()); err != nil || len(members) != 1 {
		t.Fatalf("members after the window = %v (err %v), want alice", members, err)
	}
	closed := time.Now()
	sendFrame(t, pub2, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionClose})
	if f := readFrame(t, pub2, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionClosed {
		t.Fatalf("frame = %v, want CLOSED", f.Action)
	}
	_ = pub2.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		members, _, err := ch.Members(context.Background())
		if err == nil && len(members) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice still a member %v after the resumed connection closed cleanly", time.Since(closed))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if took := time.Since(closed); took > grace/2 {
		t.Errorf("LEAVE after the resumed connection's clean close took %v, want it immediate", took)
	}
}

// TestReenterPresence: after a lease lapse the server re-enters every
// member its live connections hold, with the member's last data, and
// leaves out members that have left.
func TestReenterPresence(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(1),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "first"}},
	})
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceUpdate, Data: "latest"}},
	})
	for range 2 {
		if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAck {
			t.Fatalf("frame = %v, want ACK", f.Action)
		}
	}
	bob := dialClient(t, h.srv, "bob")
	drainConnected(t, bob)
	attach(t, bob, "room", protocol.FlagPresence)
	enter(t, bob, "room", 1)
	sendFrame(t, bob, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceLeave}},
	})
	if f := readFrame(t, bob, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAck {
		t.Fatalf("frame = %v, want ACK", f.Action)
	}
	for range 4 { // alice ENTER, UPDATE; bob ENTER, LEAVE
		if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
			t.Fatalf("frame = %v, want PRESENCE", f.Action)
		}
	}

	h.rt.ReenterPresence(context.Background())
	f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 {
		t.Fatalf("frame = %+v, want one re-ENTER", f)
	}
	p := f.Presence[0]
	if p.Action != protocol.PresenceEnter || p.ClientID != "alice" || p.Data != "latest" {
		t.Errorf("re-entered %v %q data %v, want ENTER alice with her latest data", p.Action, p.ClientID, p.Data)
	}
	if err := sub.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, data, err := sub.ReadMessage(); err == nil {
		t.Fatalf("further frame %s, want none (bob left)", data)
	}
	if line := h.metricLine(t, "ably_presence_reentries_total"); line != "ably_presence_reentries_total 1" {
		t.Errorf("metric = %q, want ably_presence_reentries_total 1", line)
	}
}

// TestReenterPresenceIncludesGraceHeld: members held for an abruptly
// dropped connection's grace window are re-entered too, since a resume
// takes them over expecting them to be present.
func TestReenterPresenceIncludesGraceHeld(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
		t.Fatalf("frame = %v, want ENTER", f.Action)
	}
	_ = pub.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.rt.graceMu.Lock()
		n := len(h.rt.grace)
		h.rt.graceMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the dropped connection's members were never held for the grace window")
		}
		time.Sleep(10 * time.Millisecond)
	}

	h.rt.ReenterPresence(context.Background())
	f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceEnter || f.Presence[0].ClientID != "alice" {
		t.Fatalf("frame = %+v, want alice re-entered", f)
	}
}

// TestReenterGraceSkipsHandedOver: once a resume has taken over a grace
// entry's members, a re-entry of that entry (one already under way when
// the resume landed) writes nothing, so it cannot bring back a member the
// resumed client has since left.
func TestReenterGraceSkipsHandedOver(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	pub := dialClient(t, h.srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if connected.ConnectionDetails == nil {
		t.Fatalf("first frame = %+v, want CONNECTED with details", connected)
	}
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	_ = pub.Close()
	var g *graceLeave
	deadline := time.Now().Add(2 * time.Second)
	for g == nil {
		h.rt.graceMu.Lock()
		g = h.rt.grace[connected.ConnectionID]
		h.rt.graceMu.Unlock()
		if g == nil && time.Now().After(deadline) {
			t.Fatal("the dropped connection's members were never held for the grace window")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Resume, then leave explicitly.
	pub2 := dialResumeClientID(t, h.srv, connected.ConnectionDetails.ConnectionKey, "alice")
	drainConnected(t, pub2)
	attach(t, pub2, "room", protocol.FlagPresence)
	sendFrame(t, pub2, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceLeave}},
	})
	if f := readFrame(t, pub2, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAck {
		t.Fatalf("frame = %v, want ACK", f.Action)
	}

	if n, _ := h.rt.reenterGrace(context.Background(), g); n != 0 {
		t.Errorf("re-entry of a handed-over grace entry wrote %d members, want 0", n)
	}
	ch, err := h.manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if members, _, err := ch.Members(context.Background()); err != nil || len(members) != 0 {
		t.Fatalf("members = %v (err %v), want none: alice left after resuming", members, err)
	}
}

// TestRefusedLeaveKeepsMemberForGraceLeave: the entered set records a
// presence operation only once the store has committed it. A LEAVE the
// store refuses (NACKed) therefore keeps its member in the set, and the
// member still gets its synthesised LEAVE at the end of the grace window
// when the connection then drops (DESIGN.md §12.5). Recording the LEAVE
// before the store answered forgot the member and left it present for as
// long as the node lived.
func TestRefusedLeaveKeepsMemberForGraceLeave(t *testing.T) {
	const grace = 200 * time.Millisecond
	h := newLivenessServer(t, grace)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
		t.Fatalf("frame = %v, want ENTER", f.Action)
	}

	// The LEAVE is refused by the store and NACKed.
	h.store.failPresence.Store(true)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceLeave}},
	})
	if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionNack {
		t.Fatalf("frame = %v, want NACK for the refused LEAVE", f.Action)
	}
	h.store.failPresence.Store(false)

	// The connection drops abruptly: alice is still a member, so the
	// grace window ends with her synthesised LEAVE.
	_ = pub.Close()
	f := readFrame(t, sub, protocol.FormatJSON, grace+3*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceLeave || f.Presence[0].ClientID != "alice" {
		t.Fatalf("frame = %+v, want alice's grace LEAVE", f)
	}
}

// TestClaimDuringReentryReentersAdoptedMembers: a resume that claims its
// grace entry while a lease-lapse re-entry pass is running may be missed
// by the pass (the entry already claimed, the connection already done, or
// registered after the pass took its snapshot), so the claiming
// connection re-enters the members it adopted itself (DESIGN.md §12.5).
// The passes here are real ReenterPresence calls held mid-pass by a
// store hook. Two overlap, as when the lapse hook fires again during a
// long pass, and the first ends before the resume: the second is still
// running, so the claimant must still re-enter. With a flag rather than a
// count of running passes, the first pass's end cleared it and the
// claimant did not.
func TestClaimDuringReentryReentersAdoptedMembers(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if connected.ConnectionDetails == nil {
		t.Fatalf("first frame = %+v, want CONNECTED with details", connected)
	}
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
		t.Fatalf("frame = %v, want ENTER", f.Action)
	}
	// bob stays connected, on another channel; his re-entry is what holds
	// each pass open.
	bob := dialClient(t, h.srv, "bob")
	drainConnected(t, bob)
	attach(t, bob, "lobby", protocol.FlagPresence)
	enter(t, bob, "lobby", 1)

	_ = pub.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.rt.graceMu.Lock()
		n := len(h.rt.grace)
		h.rt.graceMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the dropped connection's members were never held for the grace window")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Every re-entry of bob waits for the test to release it.
	held := make(chan chan struct{}, 2)
	h.store.setPresenceHook(func(ctx context.Context, p []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
		if storage.IsPresenceReentry(ctx) && len(p) > 0 && p[0].ClientID == "bob" {
			release := make(chan struct{})
			held <- release
			<-release
		}
		return store(nil)
	})
	t.Cleanup(func() { h.store.setPresenceHook(nil) })
	pass := func() chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.rt.ReenterPresence(context.Background())
		}()
		return done
	}
	nextHeld := func() chan struct{} {
		select {
		case release := <-held:
			return release
		case <-time.After(2 * time.Second):
			t.Fatal("no re-entry pass reached bob")
			return nil
		}
	}
	// The first pass holds bob's re-entry in the store, and with it bob's
	// presence lock, so the second waits on that lock behind it.
	done1 := pass()
	release1 := nextHeld()
	done2 := pass()
	// Each pass re-entered alice's grace-held members.
	for range 2 {
		if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence || f.Presence[0].ClientID != "alice" {
			t.Fatalf("frame = %+v, want alice re-entered by a pass", f)
		}
	}
	// The first pass ends; the second reaches bob's store and is held there.
	close(release1)
	<-done1
	release2 := nextHeld()
	t.Cleanup(func() { close(release2); <-done2 })

	// The resume claims the grace entry while the second pass runs, and
	// re-enters what it adopted: the subscriber sees alice's ENTER again.
	// That re-entry is held in the store too, and the resume's CONNECTED
	// does not wait for it: it runs off the handshake.
	aliceHeld, aliceRelease := make(chan struct{}), make(chan struct{})
	var holdAlice atomic.Bool
	holdAlice.Store(true)
	h.store.setPresenceHook(func(ctx context.Context, p []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
		if storage.IsPresenceReentry(ctx) && len(p) > 0 {
			switch {
			case p[0].ClientID == "bob":
				release := make(chan struct{})
				held <- release
				<-release
			case p[0].ClientID == "alice" && holdAlice.CompareAndSwap(true, false):
				close(aliceHeld)
				<-aliceRelease
			}
		}
		return store(nil)
	})
	pub2 := dialResumeClientID(t, h.srv, connected.ConnectionDetails.ConnectionKey, "alice")
	drainConnected(t, pub2)
	select {
	case <-aliceHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("the resumed connection never re-entered alice")
	}
	close(aliceRelease)
	f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceEnter || f.Presence[0].ClientID != "alice" {
		t.Fatalf("frame = %+v, want alice re-entered by the resumed connection", f)
	}
}

// TestGraceEntryCreatedDuringReentryIsOwed: a connection that drops while
// a re-entry pass is running may be read by the pass after its teardown
// took its set, and its grace entry is in no pass's snapshot. The entry
// is marked owed, so the connection that resumes it re-enters the members
// even after the pass has ended (DESIGN.md §12.5).
func TestGraceEntryCreatedDuringReentryIsOwed(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	h.rt.graceMu.Lock()
	h.rt.reentering++ // a pass is running
	h.rt.graceMu.Unlock()
	h.rt.scheduleConnectionLeaves("conn-1", map[string]map[string]*protocol.PresenceMessage{
		"room": {"alice": {Action: protocol.PresenceEnter, ClientID: "alice"}},
	}, false)
	h.rt.graceMu.Lock()
	h.rt.reentering-- // and has ended
	g := h.rt.grace["conn-1"]
	h.rt.graceMu.Unlock()
	if g == nil || !g.owed {
		t.Fatalf("grace entry = %+v, want one marked owed", g)
	}
	c := &connection{id: "conn-1", entered: make(map[string]map[string]*protocol.PresenceMessage)}
	h.rt.graceMu.Lock()
	adopted, reenter := h.rt.adoptGraceLocked(c, g)
	h.rt.graceMu.Unlock()
	if !adopted || !reenter {
		t.Fatalf("adopt = %v, reenter = %v, want both", adopted, reenter)
	}
	g.timer.Stop()

	// The adopter drops before its re-entry has read its set: the duty
	// passes to the next grace entry, with no pass running any more.
	all, owed := c.closeEntered()
	if !owed || len(all["room"]) != 1 {
		t.Fatalf("teardown took %v, owed %v; want alice and the re-entry still owed", all, owed)
	}
	h.rt.scheduleConnectionLeaves("conn-1", all, owed)
	h.rt.graceMu.Lock()
	g2 := h.rt.grace["conn-1"]
	h.rt.graceMu.Unlock()
	if g2 == nil || !g2.owed {
		t.Fatalf("grace entry = %+v, want the re-entry carried as owed", g2)
	}
	g2.timer.Stop()
}

// TestMergedSetsPreferCommittedRecords: when a grace entry's members
// merge with another set (a second drop in the window, a hand-over to a
// resumed connection), a committed record is kept over an uncertain one
// for the same member, so the member is still re-entered after a lapse.
func TestMergedSetsPreferCommittedRecords(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	committed := &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", Data: "c"}
	uncertain := &protocol.PresenceMessage{Action: uncertainAction, ClientID: "alice", Data: "u"}
	h.rt.scheduleConnectionLeaves("conn-1", map[string]map[string]*protocol.PresenceMessage{"room": {"alice": committed}}, false)
	h.rt.scheduleConnectionLeaves("conn-1", map[string]map[string]*protocol.PresenceMessage{"room": {"alice": uncertain}}, false)
	h.rt.graceMu.Lock()
	g := h.rt.grace["conn-1"]
	got := g.members["room"]["alice"]
	h.rt.graceMu.Unlock()
	g.timer.Stop()
	if got != committed {
		t.Errorf("grace merge kept %+v, want the committed record", got)
	}
	c := &connection{id: "conn-1", entered: map[string]map[string]*protocol.PresenceMessage{"room": {"alice": uncertain}}}
	c.adoptPresence(map[string]map[string]*protocol.PresenceMessage{"room": {"alice": committed}}, false)
	if got := c.entered["room"]["alice"]; got != committed {
		t.Errorf("adoption kept %+v, want the committed record over the connection's uncertain one", got)
	}
}

// liveConn returns the server's live connection with connectionId id.
func (h *livenessHarness) liveConn(t *testing.T, id string) *connection {
	t.Helper()
	h.rt.mu.Lock()
	defer h.rt.mu.Unlock()
	c := h.rt.byKey[id]
	if c == nil {
		t.Fatalf("no live connection %q", id)
	}
	return c
}

// TestEnterCommittedAfterTeardownIsLeft: an ENTER whose batch is still in
// flight when the connection drops commits after the teardown has
// cancelled the connection's context. The store call does not run on
// that context (handlePresence), so the commit is answered and the member
// recorded as entered (not merely uncertain) before teardown takes the
// entered set, and the grace window ends with its LEAVE (DESIGN.md
// §12.5). Run on the connection's context, the call answered with the
// context's error; before uncertain records existed the committed member
// was never recorded, and it stayed present for as long as the node
// lived.
func TestEnterCommittedAfterTeardownIsLeft(t *testing.T) {
	const grace = 200 * time.Millisecond
	h := newLivenessServer(t, grace)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	attach(t, pub, "room", protocol.FlagPresence)
	conn := h.liveConn(t, connected.ConnectionID)

	// The ENTER's batch stalls until the test releases it, then commits
	// and, as the Postgres lanes do, answers with the caller's context
	// error if the caller has given up meanwhile.
	inFlight := make(chan struct{})
	release := make(chan struct{})
	h.store.setPresenceHook(func(ctx context.Context, _ []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
		close(inFlight)
		<-release
		cm, idem, err := store(nil)
		if err == nil && ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return cm, idem, err
	})
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(1),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("the ENTER never reached the store")
	}

	// The connection drops; its teardown cancels the connection's context
	// and then waits for the publish worker.
	_ = pub.Close()
	select {
	case <-conn.loopCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the connection's context was never cancelled")
	}
	h.store.setPresenceHook(nil)
	close(release)

	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence || f.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("frame = %+v, want alice's ENTER", f)
	}
	// The teardown held alice for the grace window as committed: the
	// write's answer was its commit, not the connection's cancellation.
	var held *protocol.PresenceMessage
	deadline := time.Now().Add(2 * time.Second)
	for held == nil && time.Now().Before(deadline) {
		h.rt.graceMu.Lock()
		if g := h.rt.grace[connected.ConnectionID]; g != nil {
			held = g.members["room"]["alice"]
		}
		h.rt.graceMu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	if held == nil || held.Action != protocol.PresenceEnter {
		t.Fatalf("grace record = %+v, want alice held as entered", held)
	}
	f := readFrame(t, sub, protocol.FormatJSON, grace+3*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceLeave || f.Presence[0].ClientID != "alice" {
		t.Fatalf("frame = %+v, want alice's grace LEAVE", f)
	}
}

// TestUncertainPresenceOutcome: a presence write whose outcome is
// unknown, a store error that does not prove nothing was stored or a
// write that outlived the presence write timeout, is NACKed, and its
// member is recorded as uncertain (DESIGN.md §12.5): the connection's
// LEAVE still covers it, since the write may have committed, but a lapse
// re-entry does not bring it back, since the client was told it failed.
// A write the store refused before storing anything (42910) records
// nothing.
func TestUncertainPresenceOutcome(t *testing.T) {
	var stamping sync.WaitGroup
	t.Cleanup(stamping.Wait)
	for _, tc := range []struct {
		name string
		hook presenceHookFunc
		// committed: the hook stored the ENTER, so the subscriber sees it
		// and the connection must leave it.
		committed bool
	}{
		{
			name: "unavailable",
			hook: func(_ context.Context, _ []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
				if _, _, err := store(nil); err != nil {
					return nil, false, err
				}
				return nil, false, storage.ErrUnavailable
			},
			committed: true,
		},
		{
			name: "timeout",
			hook: func(ctx context.Context, _ []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
				<-ctx.Done()
				if _, _, err := store(nil); err != nil {
					return nil, false, err
				}
				return nil, false, ctx.Err()
			},
			committed: true,
		},
		{
			// As the Postgres lanes do: the caller stops waiting, and the
			// batch goes on to stamp the messages while it commits. The
			// record must not be read from them (run under -race).
			name: "stamped after the timeout",
			hook: func(ctx context.Context, p []*protocol.PresenceMessage, store storeFunc) (*protocol.ChannelMessage, bool, error) {
				<-ctx.Done()
				// The backend keeps what it stores; it gets copies, so only
				// the caller's messages are stamped late.
				cp := make([]*protocol.PresenceMessage, len(p))
				for i, m := range p {
					c := *m
					cp[i] = &c
				}
				if _, _, err := store(cp); err != nil {
					return nil, false, err
				}
				stamping.Add(1)
				go func() {
					defer stamping.Done()
					time.Sleep(20 * time.Millisecond)
					p[0].Timestamp = 1
					p[0].Serial = "stamped"
				}()
				return nil, false, ctx.Err()
			},
			committed: true,
		},
		{
			name: "refused",
			hook: func(context.Context, []*protocol.PresenceMessage, storeFunc) (*protocol.ChannelMessage, bool, error) {
				return nil, false, storage.ErrOverloaded
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLivenessServer(t, time.Hour)
			h.rt.presenceWriteTimeout = 200 * time.Millisecond
			sub := dial(t, h.srv, "")
			drainConnected(t, sub)
			attach(t, sub, "room", 0)
			pub := dialClient(t, h.srv, "alice")
			connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
			attach(t, pub, "room", protocol.FlagPresence)
			conn := h.liveConn(t, connected.ConnectionID)

			h.store.setPresenceHook(tc.hook)
			sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
				Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(1),
				Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "d"}},
			})
			if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionNack {
				t.Fatalf("frame = %v, want NACK", f.Action)
			}
			h.store.setPresenceHook(nil)

			stamping.Wait()
			conn.enteredMu.Lock()
			rec := conn.entered["room"]["alice"]
			conn.enteredMu.Unlock()
			switch {
			case !tc.committed && rec != nil:
				t.Fatalf("refused ENTER recorded as %+v, want nothing", rec)
			case tc.committed && (rec == nil || rec.Action != uncertainAction):
				t.Fatalf("record = %+v, want alice recorded as uncertain", rec)
			}
			if !tc.committed {
				return
			}
			if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence || f.Presence[0].Action != protocol.PresenceEnter {
				t.Fatalf("frame = %+v, want alice's ENTER", f)
			}

			// A lapse re-entry leaves the uncertain member alone.
			h.rt.ReenterPresence(context.Background())
			if err := sub.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
				t.Fatalf("SetReadDeadline: %v", err)
			}
			if _, data, err := sub.ReadMessage(); err == nil {
				t.Fatalf("frame after the re-entry pass: %s (want none)", data)
			}
			_ = sub.Close()
			sub2 := dial(t, h.srv, "")
			drainConnected(t, sub2)
			attach(t, sub2, "room", 0)
			if f := readFrame(t, sub2, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionSync {
				t.Fatalf("frame = %v, want the SYNC of alice", f.Action)
			}

			// A clean close leaves her.
			sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionClose})
			if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionClosed {
				t.Fatalf("frame = %v, want CLOSED", f.Action)
			}
			_ = pub.Close()
			f := readFrame(t, sub2, protocol.FormatJSON, 2*time.Second)
			if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceLeave || f.Presence[0].ClientID != "alice" {
				t.Fatalf("frame = %+v, want alice's LEAVE", f)
			}
		})
	}
}

// TestDetachAfterQueuedEnterLeavesTheMember: an ENTER queued on the
// publish worker behind an earlier publish, followed at once by a
// DETACH, must not leave its member present on the detached channel.
// The DETACH's leave runs on the worker after the ENTER (DESIGN.md
// §12.5), so the member is entered and then left, and DETACHED follows.
// Run on the read goroutine, the leave found nothing yet to leave, the
// ENTER committed afterwards, and the member stayed until the connection
// closed.
func TestDetachAfterQueuedEnterLeavesTheMember(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	attach(t, pub, "room", protocol.FlagPresence)
	conn := h.liveConn(t, connected.ConnectionID)

	// A publish on another channel holds the worker in the store.
	inFlight, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	hook := func() {
		if once.CompareAndSwap(false, true) {
			close(inFlight)
			<-release
		}
	}
	h.store.storeHook.Store(&hook)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: new("other"), MsgSerial: msgSerialPtr(1),
		Messages: []*protocol.Message{{Name: "m", Data: "x"}},
	})
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("the publish never reached the store")
	}
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: new("room")})
	time.Sleep(100 * time.Millisecond) // let the DETACH reach the read goroutine
	close(release)

	var acks int
	for detached := false; !detached || acks < 2; {
		f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
		switch f.Action {
		case protocol.ActionAck:
			acks++
		case protocol.ActionDetached:
			detached = true
		default:
			t.Fatalf("publisher frame %v, want ACKs and DETACHED", f.Action)
		}
	}
	for _, want := range []protocol.PresenceAction{protocol.PresenceEnter, protocol.PresenceLeave} {
		f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
		if f.Action != protocol.ActionPresence || len(f.Presence) != 1 || f.Presence[0].Action != want || f.Presence[0].ClientID != "alice" {
			t.Fatalf("subscriber frame %+v, want alice's %v", f, want)
		}
	}
	ch, err := h.manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if members, _, err := ch.Members(context.Background()); err != nil || len(members) != 0 {
		t.Fatalf("members after the DETACH = %v (err %v), want none", members, err)
	}
	conn.enteredMu.Lock()
	n := len(conn.entered)
	conn.enteredMu.Unlock()
	if n != 0 {
		t.Fatalf("entered set holds %d channels after the DETACH, want none", n)
	}
}

// TestReenterAdoptedUsesTheConnectionsOwnRecord: a re-entry of members
// adopted from a grace entry re-enters only those the connection still
// holds, with its own record of each (DESIGN.md §12.5). The connection
// may be running by then, so a member its client has left since the
// hand-over is not brought back, and one it has updated keeps the
// update; replaying the grace entry's copy did both wrong.
func TestReenterAdoptedUsesTheConnectionsOwnRecord(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	pub := dialClient(t, h.srv, "*")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	attach(t, pub, "room", protocol.FlagPresence)
	conn := h.liveConn(t, connected.ConnectionID)
	for i, p := range []*protocol.PresenceMessage{
		{Action: protocol.PresenceEnter, ClientID: "alice", Data: "old"},
		{Action: protocol.PresenceUpdate, ClientID: "alice", Data: "new"},
	} {
		sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(int64(i + 1)),
			Presence: []*protocol.PresenceMessage{p},
		})
		if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAck {
			t.Fatalf("frame = %v, want ACK", f.Action)
		}
		if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
			t.Fatalf("frame = %v, want PRESENCE", f.Action)
		}
	}

	// The grace entry's copy: alice before her update, and bob, whom the
	// client has left since.
	n, f := conn.reenterMembers(context.Background(), map[string]map[string]*protocol.PresenceMessage{
		"room": {
			"alice": {Action: protocol.PresenceEnter, ClientID: "alice", Data: "old"},
			"bob":   {Action: protocol.PresenceEnter, ClientID: "bob", Data: "gone"},
		},
	})
	if n != 1 || f != 0 {
		t.Fatalf("re-entered %d, failed %d; want 1 and 0", n, f)
	}
	got := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if got.Action != protocol.ActionPresence || len(got.Presence) != 1 || got.Presence[0].ClientID != "alice" || got.Presence[0].Data != "new" {
		t.Fatalf("frame = %+v, want alice re-entered with her update only", got)
	}
}

// TestDiscontinuityWithAFailedSeedKeepsMembers: a channel update whose
// presence re-seed fails, after its retries, still carries HAS_PRESENCE,
// so the SDK keeps its members rather than taking the set as empty and
// leaving every member (RTP19a); the SYNC it then waits for is sent once
// a read succeeds.
func TestDiscontinuityWithAFailedSeedKeepsMembers(t *testing.T) {
	h := newLivenessServer(t, time.Hour)
	pub := dialClient(t, h.srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)
	sub := dial(t, h.srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionSync {
		t.Fatalf("frame = %v, want the attach SYNC", f.Action)
	}

	h.store.failMembers.Store(true)
	ch, err := h.manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	ch.Discontinuity(storage.DiscontinuityRetention)
	update := readFrame(t, sub, protocol.FormatJSON, 3*time.Second)
	if update.Action != protocol.ActionAttached || update.Error == nil || update.Error.Code != 80016 {
		t.Fatalf("frame = %+v, want the 80016 channel update", update)
	}
	if update.Flags&protocol.FlagHasPresence == 0 {
		t.Fatalf("flags = %d, want HAS_PRESENCE although the re-seed failed", update.Flags)
	}
	// Once the store answers again, the owed SYNC follows (the client
	// is waiting for it to complete its sync), on a quiet channel too.
	h.store.failMembers.Store(false)
	f := readFrame(t, sub, protocol.FormatJSON, 3*time.Second)
	if f.Action != protocol.ActionSync || len(f.Presence) != 1 || f.Presence[0].ClientID != "alice" || !strings.HasSuffix(f.ChannelSerial, ":") {
		t.Fatalf("frame = %v %+v (serial %q), want the owed SYNC of alice, complete", f.Action, f.Presence, f.ChannelSerial)
	}
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: new("room"), MsgSerial: msgSerialPtr(2),
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceUpdate, Data: "x"}},
	})
	if f := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence {
		t.Fatalf("frame = %v, want the live UPDATE", f.Action)
	}
}
