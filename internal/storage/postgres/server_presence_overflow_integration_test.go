//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// stallCommits makes every batch commit attempt block, ignoring its
// context as a wedged connection would, until the returned func is
// called.
func stallCommits() (release func()) {
	gate := make(chan struct{})
	hook := func() error { <-gate; return nil }
	commitBatchHook.Store(&hook)
	var once sync.Once
	return func() {
		once.Do(func() {
			commitBatchHook.Store(nil)
			close(gate)
		})
	}
}

// TestServerPresenceOverflowWritesUnbatched: with the lane's commit
// stalled, server-synthesised LEAVEs fill the lane's server-presence
// bound (8 x QueueMax) and the rest are written in transactions of their
// own, so they land while the lane is still stalled and the queue stays
// bounded. Once the stall ends, the queued ones land too.
func TestServerPresenceOverflowWritesUnbatched(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	const queueMax = 2
	s, err := Open(ctx, Options{DSN: dsn, Batching: Batching{Lanes: 1, QueueMax: queueMax, LingerMax: time.Hour}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	const n = 30
	var channels []string
	for i := range n {
		channels = append(channels, fmt.Sprintf("room-%d", i))
	}
	enterMembers(t, s, channels, "m", n) // m-i on room-i

	release := stallCommits()
	defer release()
	errs := make(chan error, n)
	for i := range n {
		go func() {
			lctx, cancel := context.WithTimeout(storage.WithServerPresence(ctx), 20*time.Second)
			defer cancel()
			ch, err := s.Channel(lctx, channels[i], nil)
			if err == nil {
				id := fmt.Sprintf("m-%d", i)
				_, _, err = ch.StorePresence(lctx, []*protocol.PresenceMessage{{
					Action: protocol.PresenceLeave, ClientID: id, ConnectionID: "conn-" + id,
				}})
			}
			errs <- err
		}()
	}

	// Some LEAVEs are in the stalled batch and 8 x QueueMax are queued;
	// the rest are written around the lane while it is still stalled.
	const bound = serverQueueFactor * queueMax
	l := s.lanes.lanes[0]
	var depth, server, inflight int
	waitFor(t, 10*time.Second, "the overflowed LEAVEs to land with the lane stalled", func() bool {
		l.mu.Lock()
		depth, server, inflight = len(l.queue), l.serverQueued, 0
		for b := range l.running {
			inflight += len(*b)
		}
		l.mu.Unlock()
		overflowed := int(counterValue(t, s.wmetrics.serverUnbatched))
		landed := n - countPresenceRows(t, s, `true`)
		return inflight+depth+overflowed == n && landed == overflowed
	})
	if depth != bound || server != bound {
		t.Errorf("lane queue %d (%d server) with the commit stalled, want the bound %d", depth, server, bound)
	}
	if overflowed := n - inflight - depth; overflowed < 1 {
		t.Fatalf("%d LEAVEs written around the stalled lane, want some (%d in flight, %d queued)", overflowed, inflight, depth)
	}

	release()
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("server LEAVE: %v", err)
		}
	}
	if got := countPresenceRows(t, s, `true`); got != 0 {
		t.Errorf("%d members left after every LEAVE returned, want 0", got)
	}
}

// TestServerPresenceOverflowKeepsChannelOrder: a server LEAVE turned
// away by a full lane is written around it only after the channel's
// earlier publishes: here a client ENTER of the same member stuck in a
// stalled batch. Written first, the LEAVE would delete nothing and the
// ENTER would then leave a member behind for good.
func TestServerPresenceOverflowKeepsChannelOrder(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	const queueMax = 1
	s, err := Open(ctx, Options{DSN: dsn, Batching: Batching{Lanes: 1, QueueMax: queueMax, LingerMax: time.Hour}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	ch, err := s.Channel(ctx, "c", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	release := stallCommits()
	defer release()
	entered := make(chan error, 1)
	go func() {
		_, _, err := ch.StorePresence(ctx, []*protocol.PresenceMessage{{
			Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "conn-a",
		}})
		entered <- err
	}()
	l := s.lanes.lanes[0]
	waitFor(t, 5*time.Second, "the ENTER's batch to start", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.running) == 1
	})
	// Fill the lane's server-presence bound on other channels.
	const bound = serverQueueFactor * queueMax
	filled := make(chan error, bound)
	for i := range bound {
		go func() {
			other, err := s.Channel(ctx, fmt.Sprintf("other-%d", i), nil)
			if err == nil {
				_, _, err = other.StorePresence(storage.WithServerPresence(ctx), []*protocol.PresenceMessage{{
					Action: protocol.PresenceLeave, ClientID: "x", ConnectionID: "conn-x",
				}})
			}
			filled <- err
		}()
	}
	waitFor(t, 5*time.Second, "the bound to fill", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.serverQueued == bound
	})
	left := make(chan error, 1)
	go func() {
		lctx, cancel := context.WithTimeout(storage.WithServerPresence(ctx), 20*time.Second)
		defer cancel()
		_, _, err := ch.StorePresence(lctx, []*protocol.PresenceMessage{{
			Action: protocol.PresenceLeave, ClientID: "alice", ConnectionID: "conn-a",
		}})
		left <- err
	}()
	select {
	case err := <-left:
		t.Fatalf("the overflowed LEAVE returned (%v) while the ENTER before it was still stalled", err)
	case <-time.After(500 * time.Millisecond):
	}

	release()
	if err := <-entered; err != nil {
		t.Fatalf("ENTER: %v", err)
	}
	if err := <-left; err != nil {
		t.Fatalf("LEAVE: %v", err)
	}
	for range bound {
		if err := <-filled; err != nil {
			t.Errorf("filler LEAVE: %v", err)
		}
	}
	if got := countPresenceRows(t, s, `client_id = 'alice'`); got != 0 {
		t.Fatalf("alice has %d rows after ENTER then LEAVE, want 0 (the LEAVE overtook the ENTER)", got)
	}
	if got := counterValue(t, s.wmetrics.serverUnbatched); got != 1 {
		t.Errorf("ably_publish_server_presence_unbatched_total = %v, want 1", got)
	}
}
