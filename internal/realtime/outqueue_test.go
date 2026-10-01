package realtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

func frameOf(n int) outFrame {
	return outFrame{data: make([]byte, n), action: protocol.ActionMessage}
}

func TestOutQueueFIFOAndByteAccounting(t *testing.T) {
	q := newOutQueue(100, time.Second)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := q.push(ctx, frameOf(i*10)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if got := q.queuedBytes(); got != 60 {
		t.Fatalf("queuedBytes = %d, want 60", got)
	}
	for i := 1; i <= 3; i++ {
		f, ok := q.pop()
		if !ok || len(f.data) != i*10 {
			t.Fatalf("pop %d = %d bytes (ok=%v), want %d", i, len(f.data), ok, i*10)
		}
	}
	if _, ok := q.pop(); ok {
		t.Fatal("pop on an empty queue returned a frame")
	}
	if got := q.queuedBytes(); got != 0 {
		t.Fatalf("queuedBytes after draining = %d, want 0", got)
	}
	if q.frames != nil && cap(q.frames) > 32 {
		t.Fatalf("drained queue kept a %d-slot backing array", cap(q.frames))
	}
}

func TestOutQueueAdmitsOversizeFrameWhenEmpty(t *testing.T) {
	q := newOutQueue(10, 20*time.Millisecond)
	if err := q.push(context.Background(), frameOf(1000)); err != nil {
		t.Fatalf("oversize frame into an empty queue: %v", err)
	}
	// The next push must wait: the queue is over its limit.
	if err := q.push(context.Background(), frameOf(1)); !errors.Is(err, errSlowConsumer) {
		t.Fatalf("push into an over-limit queue = %v, want errSlowConsumer", err)
	}
}

func TestOutQueuePushWaitsForRoom(t *testing.T) {
	q := newOutQueue(100, 2*time.Second)
	ctx := context.Background()
	if err := q.push(ctx, frameOf(80)); err != nil {
		t.Fatalf("push: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- q.push(ctx, frameOf(50)) }()
	select {
	case err := <-done:
		t.Fatalf("push over the limit returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	q.pop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("push after room appeared: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("push did not wake when room appeared")
	}
}

func TestOutQueueSlowConsumerAfterTimeout(t *testing.T) {
	q := newOutQueue(100, 50*time.Millisecond)
	ctx := context.Background()
	_ = q.push(ctx, frameOf(100))
	start := time.Now()
	err := q.push(ctx, frameOf(1))
	if !errors.Is(err, errSlowConsumer) {
		t.Fatalf("push = %v, want errSlowConsumer", err)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > time.Second {
		t.Fatalf("slow-consumer verdict after %v, want about the 50ms timeout", d)
	}
}

func TestOutQueuePushHonoursContext(t *testing.T) {
	q := newOutQueue(10, time.Hour)
	_ = q.push(context.Background(), frameOf(10))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.push(ctx, frameOf(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("push with a cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestOutQueueReplaceWithDropsBacklogAndCloses(t *testing.T) {
	q := newOutQueue(1000, time.Hour)
	ctx := context.Background()
	for range 5 {
		_ = q.push(ctx, frameOf(100))
	}
	q.replaceWith(outFrame{data: []byte("bye"), action: protocol.ActionDisconnected})
	f, ok := q.pop()
	if !ok || f.action != protocol.ActionDisconnected {
		t.Fatalf("first frame after replaceWith = %v (ok=%v), want DISCONNECTED", f.action, ok)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("backlog survived replaceWith")
	}
	if err := q.push(ctx, frameOf(1)); !errors.Is(err, errQueueClosed) {
		t.Fatalf("push after replaceWith = %v, want errQueueClosed", err)
	}
}

func TestOutQueueCloseWakesWaiters(t *testing.T) {
	q := newOutQueue(10, time.Hour)
	_ = q.push(context.Background(), frameOf(10))
	done := make(chan error, 1)
	go func() { done <- q.push(context.Background(), frameOf(5)) }()
	time.Sleep(20 * time.Millisecond)
	q.close()
	select {
	case err := <-done:
		if !errors.Is(err, errQueueClosed) {
			t.Fatalf("waiting push after close = %v, want errQueueClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not wake a waiting push")
	}
}

// Under a burst from many pushers against a slow writer, the queue never
// holds more than its byte limit.
func TestOutQueueBoundHeldUnderBurst(t *testing.T) {
	const limit = 4096
	q := newOutQueue(limit, 5*time.Second)
	var peak atomic.Int64
	stop := make(chan struct{})
	popped := make(chan struct{})
	go func() {
		defer close(popped)
		for {
			select {
			case <-stop:
				return
			case <-q.ready:
			}
			for {
				if b := q.queuedBytes(); b > peak.Load() {
					peak.Store(b)
				}
				if _, ok := q.pop(); !ok {
					break
				}
				time.Sleep(50 * time.Microsecond) // a slow client
			}
		}
	}()
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if err := q.push(context.Background(), frameOf(300)); err != nil {
					t.Errorf("push: %v", err)
					return
				}
				if b := q.queuedBytes(); b > peak.Load() {
					peak.Store(b)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-popped
	if p := peak.Load(); p > limit {
		t.Fatalf("queue peaked at %d bytes, over its %d-byte limit", p, limit)
	}
}

// A queue that never fully drains must not grow its backing array by one
// slot per frame ever pushed: one frame is always left queued while ten
// thousand pass through, and the order is preserved across compaction.
func TestOutQueueNeverDrainingStaysBounded(t *testing.T) {
	q := newOutQueue(1<<20, time.Second)
	ctx := context.Background()
	if err := q.push(ctx, frameOf(1)); err != nil {
		t.Fatalf("seed push: %v", err)
	}
	for i := 2; i <= 10000; i++ {
		if err := q.push(ctx, frameOf(i%50+1)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
		f, ok := q.pop()
		if !ok {
			t.Fatalf("pop %d: empty", i)
		}
		// Each pop returns the frame pushed one step earlier.
		want := 1
		if i > 2 {
			want = (i-1)%50 + 1
		}
		if len(f.data) != want {
			t.Fatalf("pop %d = %d bytes, want %d (order lost across compaction)", i, len(f.data), want)
		}
	}
	q.mu.Lock()
	capFrames, live := cap(q.frames), len(q.frames)-q.head
	q.mu.Unlock()
	if live != 1 {
		t.Fatalf("live frames = %d, want 1", live)
	}
	if capFrames > 256 {
		t.Fatalf("backing array cap = %d after 10000 pushes with one frame always queued; want it bounded (<= 256)", capFrames)
	}
}
