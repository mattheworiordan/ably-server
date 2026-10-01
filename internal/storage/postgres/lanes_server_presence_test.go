package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/storage"
)

// serverPending is a queued server-synthesised presence publish.
func serverPending(channel string) *pending {
	p := newPending(context.Background(), channel)
	p.mustAdmit = true
	return p
}

// TestLaneServerPresenceBound: server-synthesised presence is exempt from
// QueueMax (it is queued past it while ordinary publishes are refused)
// but has a bound of its own, serverQueueFactor x QueueMax; past it
// submit answers errServerQueueFull, so a stalled commit during a mass
// disconnect cannot grow the queue without bound.
func TestLaneServerPresenceBound(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	const queueMax = 2
	ls := testLanes(t, Batching{Lanes: 1, QueueMax: queueMax, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)

	var queued []<-chan error
	for range queueMax {
		queued = append(queued, publishAsync(ls, newPending(context.Background(), "a")))
	}
	waitQueued(t, ls.lanes[0], queueMax)
	if _, _, err := ls.publish(newPending(context.Background(), "a")); !errors.Is(err, storage.ErrOverloaded) {
		t.Fatalf("ordinary publish past QueueMax: err = %v, want ErrOverloaded", err)
	}
	const bound = serverQueueFactor * queueMax
	for range bound {
		queued = append(queued, publishAsync(ls, serverPending("a")))
	}
	waitQueued(t, ls.lanes[0], queueMax+bound)
	if _, _, err := ls.publish(serverPending("a")); !errors.Is(err, errServerQueueFull) {
		t.Fatalf("server presence past %d queued: err = %v, want errServerQueueFull", bound, err)
	}
	l := ls.lanes[0]
	l.mu.Lock()
	depth, server := len(l.queue), l.serverQueued
	l.mu.Unlock()
	if depth != queueMax+bound || server != bound {
		t.Fatalf("queue depth %d (%d server), want %d (%d server)", depth, server, queueMax+bound, bound)
	}

	close(f.gate)
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	for _, q := range queued {
		if err := <-q; err != nil {
			t.Errorf("queued publish: %v", err)
		}
	}
	l.mu.Lock()
	server = l.serverQueued
	l.mu.Unlock()
	if server != 0 {
		t.Errorf("serverQueued = %d after the queue drained, want 0", server)
	}
	// The bound frees as the queue drains.
	if _, _, err := ls.publish(serverPending("a")); err != nil {
		t.Errorf("server presence after the drain: %v", err)
	}
}
