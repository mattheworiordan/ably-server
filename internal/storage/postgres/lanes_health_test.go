package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestLaneHealth: a lane reports unhealthy, naming itself, once its
// oldest queued publish has waited longer than one commit attempt or a
// batch has been in flight longer than two, and healthy again once the
// stalled batch completes.
func TestLaneHealth(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 2, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)
	second := publishAsync(ls, newPending(context.Background(), "a")) // queued behind it
	l := ls.laneFor("a")
	waitQueued(t, l, 1)

	now := time.Now()
	if err := ls.health(now); err != nil {
		t.Fatalf("health with a fresh batch in flight: %v", err)
	}
	err := ls.health(now.Add(commitAttemptTimeout + time.Second))
	if err == nil || !strings.Contains(err.Error(), "publish lane") || !strings.Contains(err.Error(), "queued") {
		t.Fatalf("health with a publish queued past one attempt = %v, want a queued-publish error naming the lane", err)
	}
	// With nothing queued, only the in-flight batch's age counts.
	l.mu.Lock()
	q := l.queue
	l.queue = nil
	l.mu.Unlock()
	if err := ls.health(now.Add(commitAttemptTimeout + time.Second)); err != nil {
		t.Fatalf("health with a batch in flight for one attempt: %v, want healthy (a retry may be running)", err)
	}
	if err := ls.health(now.Add(2*commitAttemptTimeout + time.Second)); err == nil || !strings.Contains(err.Error(), "committing") {
		t.Fatalf("health with a batch in flight past two attempts = %v, want an in-flight error", err)
	}
	l.mu.Lock()
	l.queue = q
	l.mu.Unlock()

	close(f.gate)
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second: %v", err)
	}
	if err := ls.health(time.Now().Add(commitAttemptTimeout + time.Second)); err != nil {
		t.Errorf("health after the stall ended: %v", err)
	}
}
