package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// newShutdownServer exposes the realtime.Server and its Manager so tests
// can drive Shutdown directly and inspect channel state.
func newShutdownServer(t *testing.T, hb time.Duration) (*httptest.Server, *Server, *core.Manager) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rt := NewServer([]auth.APIKey{parsed}, manager, hb, logging.New(slog.DiscardHandler), nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, rt, manager
}

// TestShutdownPacesDisconnects: Shutdown sends DISCONNECTED (action 6) to
// every live connection and spreads the closures across the grace window
// rather than firing them all at once (DESIGN.md §11).
func TestShutdownPacesDisconnects(t *testing.T) {
	srv, rt, _ := newShutdownServer(t, time.Hour)

	const n = 4
	const window = 800 * time.Millisecond
	interval := window / n

	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conns[i] = dialClient(t, srv, "")
		drainConnected(t, conns[i])
	}

	// One reader per connection records when its DISCONNECTED arrives.
	got := make(chan time.Time, n)
	for _, ws := range conns {
		go func(ws *websocket.Conn) {
			_ = ws.SetReadDeadline(time.Now().Add(window + 2*time.Second))
			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					return
				}
				var m protocol.ProtocolMessage
				if protocol.Unmarshal(data, protocol.FormatJSON, &m) != nil {
					continue
				}
				if m.Action == protocol.ActionDisconnected {
					got <- time.Now()
					return
				}
			}
		}(ws)
	}

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	rt.Shutdown(ctx)

	times := make([]time.Time, 0, n)
	deadline := time.After(window + 2*time.Second)
	for len(times) < n {
		select {
		case ts := <-got:
			times = append(times, ts)
		case <-deadline:
			t.Fatalf("only %d/%d connections received DISCONNECTED", len(times), n)
		}
	}

	minT, maxT := times[0], times[0]
	for _, ts := range times {
		if ts.Before(minT) {
			minT = ts
		}
		if ts.After(maxT) {
			maxT = ts
		}
	}
	span := maxT.Sub(minT)
	// Even pacing over the window should spread the closures across roughly
	// (n-1)*interval; require at least ~1.5 intervals so an "all at once"
	// implementation (span ~0) fails clearly.
	if span < interval+interval/2 {
		t.Errorf("DISCONNECTED span = %v, want closures paced across the window (>= %v)", span, interval+interval/2)
	}
}

// TestShutdownSynthesisesPresenceLeave: the graceful shutdown close path
// still drives the connection-loop teardown, which synthesises presence
// LEAVEs: after Shutdown the member is gone from the channel's presence
// set, a subscriber elsewhere sees exactly one LEAVE for it, and the
// departing connection's last frame is DISCONNECTED (DESIGN.md §12.5,
// §11).
//
// alice attaches with PRESENCE_SUBSCRIBE, as an SDK does, so her ENTER
// draws two frames on her own connection: the ACK, queued by the publish
// worker once the store commits (§5.2), and her own PRESENCE echo,
// queued by her attachment as soon as the cm is linked onto the live
// list (§12.2, §4.4). The two are queued by different goroutines, and
// neither the server (§5.2) nor the Ably protocol orders an ACK against
// a MESSAGE/PRESENCE delivery (only ACKs against each other, by
// msgSerial), so they may arrive in either order. The test accepts
// both; asserting ACK first failed on loaded CI runners when the worker
// was descheduled between the commit and queueing the ACK.
func TestShutdownSynthesisesPresenceLeave(t *testing.T) {
	srv, rt, manager := newShutdownServer(t, time.Hour)

	// The observer connects through a second front end over the same
	// core, which is not shut down, so it is still attached when alice's
	// teardown LEAVE is published, whatever order Shutdown closes in.
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	obsRT := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	obsMux := http.NewServeMux()
	obsMux.HandleFunc("GET /", obsRT.HandleWebSocket)
	obsSrv := httptest.NewServer(obsMux)
	t.Cleanup(obsSrv.Close)
	obs := dial(t, obsSrv, "")
	drainConnected(t, obs)
	attach(t, obs, "room", protocol.FlagPresenceSubscribe)

	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPresence|protocol.FlagPresenceSubscribe)

	// Enter presence; the ACK and the self-echo may arrive in either order.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   new("room"),
		MsgSerial: msgSerialPtr(1),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "hi"}},
	})
	var acked, echoed bool
	for range 2 {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		switch {
		case f.Action == protocol.ActionAck && !acked:
			if f.GetMsgSerial() != 1 || f.Count != 1 {
				t.Fatalf("enter ACK msgSerial/count = %d/%d, want 1/1", f.GetMsgSerial(), f.Count)
			}
			acked = true
		case f.Action == protocol.ActionPresence && !echoed:
			if len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceEnter || f.Presence[0].ClientID != "alice" {
				t.Fatalf("enter echo = %+v, want one ENTER for alice", f.Presence)
			}
			echoed = true
		default:
			t.Fatalf("enter frame = %v, want one ACK and one PRESENCE echo", f.Action)
		}
	}

	ch, err := manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if !waitMember(t, ch, "alice", true, time.Second) {
		t.Fatal("alice not present after ENTER")
	}
	if f := readFrame(t, obs, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionPresence ||
		len(f.Presence) != 1 || f.Presence[0].Action != protocol.PresenceEnter || f.Presence[0].ClientID != "alice" {
		t.Fatalf("observer frame = %v %+v, want alice's ENTER", f.Action, f.Presence)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	rt.Shutdown(ctx)

	// DISCONNECTED is alice's last frame: the write loop closes the socket
	// right after writing it (§11), before teardown publishes the LEAVE, so
	// her own LEAVE echo never reaches her.
	var last *protocol.ProtocolMessage
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
			t.Fatalf("unmarshal %q: %v", data, err)
		}
		last = &m
	}
	if last == nil || last.Action != protocol.ActionDisconnected {
		t.Fatalf("alice's last frame = %+v, want DISCONNECTED", last)
	}

	// Teardown runs as the connection loop exits; the synthesised LEAVE
	// removes alice from the presence set.
	if waitMember(t, ch, "alice", false, 2*time.Second) {
		t.Fatal("alice still present after shutdown; teardown LEAVE not synthesised")
	}

	// The observer sees exactly one LEAVE for alice, synthesised (no id).
	f := readFrame(t, obs, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionPresence || len(f.Presence) != 1 ||
		f.Presence[0].Action != protocol.PresenceLeave || f.Presence[0].ClientID != "alice" {
		t.Fatalf("observer frame = %v %+v, want alice's LEAVE", f.Action, f.Presence)
	}
	if f.Presence[0].ID != "" {
		t.Errorf("teardown LEAVE id = %q, want none (synthesised, RTP2b1)", f.Presence[0].ID)
	}
	expectNoFrame(t, obs, 300*time.Millisecond)
}

// waitMember polls the channel's presence set until clientID's presence
// matches want, or the timeout elapses; it returns the final observed
// present state.
func waitMember(t *testing.T, ch *core.Channel, clientID string, want bool, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		members, _, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		found := false
		for _, m := range members {
			if m.ClientID == clientID {
				found = true
				break
			}
		}
		if found == want || time.Now().After(deadline) {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestShutdownWaitsForInFlightReapers: a delayed-LEAVE goroutine that is
// already firing through the storage must finish before Shutdown returns,
// so no grace LEAVE writes after the storage is closed; the wait is
// bounded by the shutdown context.
func TestShutdownWaitsForInFlightReapers(t *testing.T) {
	_, rt, _ := newShutdownServer(t, time.Hour)

	release := make(chan struct{})
	var finished atomic.Bool
	rt.reaperWG.Go(func() {
		<-release
		finished.Store(true)
	})

	returned := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rt.Shutdown(ctx)
		close(returned)
	}()

	select {
	case <-returned:
		t.Fatal("Shutdown returned while a reaper was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after the reaper finished")
	}
	if !finished.Load() {
		t.Error("Shutdown returned before the reaper finished")
	}
}

// A reaper stuck past the shutdown deadline does not hold Shutdown beyond it.
func TestShutdownReaperWaitIsBoundedByContext(t *testing.T) {
	_, rt, _ := newShutdownServer(t, time.Hour)
	stuck := make(chan struct{})
	defer close(stuck)
	rt.reaperWG.Go(func() { <-stuck })

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	rt.Shutdown(ctx)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Shutdown took %v with a stuck reaper, want it bounded by the 300ms context", took)
	}
}
