package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// fakeCommitter records every batch it is handed. By default it commits
// every pending immediately; gate, defer and fail hooks let a test hold a
// commit open, defer a hot channel, or fail an attempt.
type fakeCommitter struct {
	mu      sync.Mutex
	batches [][]*pending
	order   map[string][]int // per channel, the seq of each committed publish

	gate     chan struct{}                   // when non-nil, the first commit blocks on it
	gated    bool                            //
	started  chan int                        // receives the batch size as each commit starts
	deferIf  func(p *pending, call int) bool // defer p on this call
	failCall func(call int) error            // fail this call
	latency  func() time.Duration            //
	seqOf    func(p *pending) int            //
}

func newFakeCommitter() *fakeCommitter {
	return &fakeCommitter{order: map[string][]int{}, started: make(chan int, 1000)}
}

func (f *fakeCommitter) commitBatch(ctx context.Context, batch []*pending) ([]*pending, error) {
	f.mu.Lock()
	call := len(f.batches)
	f.batches = append(f.batches, append([]*pending(nil), batch...))
	gate := f.gate
	if gate != nil && !f.gated {
		f.gated = true
	} else {
		gate = nil
	}
	f.mu.Unlock()
	f.started <- len(batch)

	if gate != nil {
		<-gate
	}
	if f.latency != nil {
		time.Sleep(f.latency())
	}
	if f.failCall != nil {
		if err := f.failCall(call); err != nil {
			return nil, err
		}
	}
	var deferred []*pending
	f.mu.Lock()
	defer f.mu.Unlock()
	// Like the real committer, deferral is per channel: a channel's row
	// lock is taken or skipped once per batch, so once one of its
	// publishes is deferred every later one in the batch is too.
	hot := map[string]bool{}
	for _, p := range batch {
		if hot[p.channel] || (f.deferIf != nil && f.deferIf(p, call)) {
			hot[p.channel] = true
			deferred = append(deferred, p)
			continue
		}
		if f.seqOf != nil {
			f.order[p.channel] = append(f.order[p.channel], f.seqOf(p))
		}
		p.res = pendingResult{cm: &protocol.ChannelMessage{ChannelSerial: fmt.Sprintf("%s#%d", p.channel, call)}}
	}
	return deferred, nil
}

func (f *fakeCommitter) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func (f *fakeCommitter) batch(i int) []*pending {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batches[i]
}

func testLanes(t *testing.T, b Batching, c committer) *laneSet {
	t.Helper()
	ls := newLaneSet(b.resolve(), c, newWriteMetrics())
	t.Cleanup(ls.close)
	return ls
}

// publishAsync submits a publish and returns a channel with its outcome.
func publishAsync(ls *laneSet, p *pending) <-chan error {
	out := make(chan error, 1)
	go func() {
		_, _, err := ls.publish(p)
		out <- err
	}()
	return out
}

func waitStarted(t *testing.T, f *fakeCommitter) int {
	t.Helper()
	select {
	case n := <-f.started:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("no commit started within 2s")
		return 0
	}
}

func TestLaneFirstPublishCommitsImmediately(t *testing.T) {
	f := newFakeCommitter()
	ls := testLanes(t, Batching{Lanes: 1, LingerMax: time.Hour}, f)
	start := time.Now()
	_, _, err := ls.publish(newPending(context.Background(), "a"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Errorf("idle lane took %v to commit one publish, want no waiting", took)
	}
	if f.calls() != 1 || len(f.batch(0)) != 1 {
		t.Errorf("commits = %d, first batch = %d; want one commit of one publish", f.calls(), len(f.batch(0)))
	}
}

func TestLaneAccumulatesWhileCommitInFlight(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, LingerMax: time.Hour}, f)

	first := publishAsync(ls, newPending(context.Background(), "a"))
	if n := waitStarted(t, f); n != 1 {
		t.Fatalf("first batch size = %d, want 1", n)
	}
	var rest []<-chan error
	for i := range 10 {
		rest = append(rest, publishAsync(ls, newPending(context.Background(), fmt.Sprintf("c%d", i))))
	}
	waitQueued(t, ls.lanes[0], 10)
	close(f.gate)
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	if n := waitStarted(t, f); n != 10 {
		t.Errorf("second batch size = %d, want all 10 accumulated publishes", n)
	}
	for _, r := range rest {
		if err := <-r; err != nil {
			t.Errorf("publish: %v", err)
		}
	}
	if f.calls() != 2 {
		t.Errorf("commits = %d, want 2", f.calls())
	}
}

func TestLaneBatchMaxCapsBatch(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, BatchMax: 4, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)
	var rest []<-chan error
	for range 10 {
		rest = append(rest, publishAsync(ls, newPending(context.Background(), "a")))
	}
	waitQueued(t, ls.lanes[0], 10)
	close(f.gate)
	<-first
	for _, r := range rest {
		<-r
	}
	for i := 1; i < f.calls(); i++ {
		if n := len(f.batch(i)); n > 4 {
			t.Errorf("batch %d has %d publishes, want at most BatchMax 4", i, n)
		}
	}
	if f.calls() != 4 { // 1 + 4 + 4 + 2
		t.Errorf("commits = %d, want 4", f.calls())
	}
}

func TestLaneLingerCapStartsSecondBatchForOtherChannels(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, LingerMax: 20 * time.Millisecond}, f)

	stalled := publishAsync(ls, newPending(context.Background(), "hot"))
	waitStarted(t, f)
	sameChannel := publishAsync(ls, newPending(context.Background(), "hot"))
	cold := publishAsync(ls, newPending(context.Background(), "cold"))

	// After the linger cap the cold channel commits in a second batch
	// while the first is still stalled; the stalled channel's next
	// publish must wait for its own batch to finish.
	select {
	case err := <-cold:
		if err != nil {
			t.Fatalf("cold publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cold publish did not commit while another channel's commit was stalled")
	}
	if b := f.batch(1); len(b) != 1 || b[0].channel != "cold" {
		t.Errorf("second batch = %v, want only the cold publish", channelsOf(b))
	}
	select {
	case <-sameChannel:
		t.Fatal("a publish on the stalled channel committed before the stalled batch finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(f.gate)
	if err := <-stalled; err != nil {
		t.Fatalf("stalled: %v", err)
	}
	if err := <-sameChannel; err != nil {
		t.Fatalf("same-channel publish: %v", err)
	}
}

func TestLaneQueueBoundRejectsWithOverloaded(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, QueueMax: 3, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)
	var queued []<-chan error
	for range 3 {
		queued = append(queued, publishAsync(ls, newPending(context.Background(), "a")))
	}
	waitQueued(t, ls.lanes[0], 3)
	if _, _, err := ls.publish(newPending(context.Background(), "a")); !errors.Is(err, storage.ErrOverloaded) {
		t.Errorf("publish beyond the queue bound: err = %v, want storage.ErrOverloaded", err)
	}
	close(f.gate)
	<-first
	for _, q := range queued {
		if err := <-q; err != nil {
			t.Errorf("queued publish: %v", err)
		}
	}
}

// TestLaneQueueBoundReclaimsAbandonedPublishes: publishes whose callers
// gave up while their channel's batch was stalled do not keep holding
// queue slots: a live publish on another channel is queued, not refused
// with ErrOverloaded, and commits once the stall ends.
func TestLaneQueueBoundReclaimsAbandonedPublishes(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, QueueMax: 3, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	var gaveUp []<-chan error
	for range 3 {
		gaveUp = append(gaveUp, publishAsync(ls, newPending(ctx, "a")))
	}
	waitQueued(t, ls.lanes[0], 3)
	cancel()
	for _, g := range gaveUp {
		if err := <-g; !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned publish: err = %v, want context.Canceled", err)
		}
	}
	live := publishAsync(ls, newPending(context.Background(), "b"))
	select {
	case err := <-live:
		close(f.gate) // let the stalled commit finish so the lane can close
		<-first
		t.Fatalf("live publish returned %v while the stalled batch held the lane; want it queued", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(f.gate)
	<-first
	if err := <-live; err != nil {
		t.Fatalf("live publish after the stall: %v", err)
	}
}

func TestLaneHotChannelDeferralKeepsOrder(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	f.deferIf = func(p *pending, call int) bool { return p.channel == "hot" && call == 1 }
	f.seqOf = func(p *pending) int { return p.ctx.Value(seqKey{}).(int) }
	ls := testLanes(t, Batching{Lanes: 1, LingerMax: time.Hour}, f)

	withSeq := func(ch string, n int) *pending {
		return newPending(context.WithValue(context.Background(), seqKey{}, n), ch)
	}
	first := publishAsync(ls, withSeq("warm", 0))
	waitStarted(t, f)
	var outs []<-chan error
	outs = append(outs, publishAsync(ls, withSeq("hot", 1)))
	waitQueued(t, ls.lanes[0], 1)
	outs = append(outs, publishAsync(ls, withSeq("cold", 1)))
	waitQueued(t, ls.lanes[0], 2)
	outs = append(outs, publishAsync(ls, withSeq("hot", 2)))
	waitQueued(t, ls.lanes[0], 3)
	close(f.gate)
	<-first
	for _, o := range outs {
		if err := <-o; err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	// Batch 1 held hot#1, cold#1, hot#2; the hot ones were deferred, cold
	// committed. Batch 2 must carry hot#1 then hot#2, in that order.
	if got := channelsOf(f.batch(2)); fmt.Sprint(got) != "[hot hot]" {
		t.Errorf("batch after deferral = %v, want [hot hot]", got)
	}
	if got := f.order["hot"]; fmt.Sprint(got) != "[1 2]" {
		t.Errorf("hot commit order = %v, want [1 2]", got)
	}
	for _, p := range f.batch(2) {
		if p.deferrals != 1 {
			t.Errorf("deferred publish deferrals = %d, want 1", p.deferrals)
		}
	}
}

func TestLaneRetriesFailedBatchOnceThenNACKs(t *testing.T) {
	t.Run("retry succeeds", func(t *testing.T) {
		f := newFakeCommitter()
		f.failCall = func(call int) error {
			if call == 0 {
				return errors.New("connection reset")
			}
			return nil
		}
		ls := testLanes(t, Batching{Lanes: 1}, f)
		if _, _, err := ls.publish(newPending(context.Background(), "a")); err != nil {
			t.Fatalf("publish after one transient failure: %v", err)
		}
		if f.calls() != 2 {
			t.Errorf("attempts = %d, want 2", f.calls())
		}
	})
	t.Run("second failure NACKs every publish", func(t *testing.T) {
		f := newFakeCommitter()
		f.gate = make(chan struct{})
		f.failCall = func(call int) error {
			if call >= 1 {
				return errors.New("connection reset")
			}
			return nil
		}
		ls := testLanes(t, Batching{Lanes: 1, LingerMax: time.Hour}, f)
		first := publishAsync(ls, newPending(context.Background(), "a"))
		waitStarted(t, f)
		var outs []<-chan error
		for i := range 3 {
			outs = append(outs, publishAsync(ls, newPending(context.Background(), fmt.Sprintf("c%d", i))))
		}
		waitQueued(t, ls.lanes[0], 3)
		close(f.gate)
		<-first
		for _, o := range outs {
			if err := <-o; !errors.Is(err, storage.ErrUnavailable) {
				t.Errorf("publish in a twice-failed batch: err = %v, want storage.ErrUnavailable", err)
			}
		}
		if f.calls() != 3 { // the first batch, then the second batch twice
			t.Errorf("attempts = %d, want 3", f.calls())
		}
	})
}

func TestLaneSkipsPublishWhoseCallerGaveUp(t *testing.T) {
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	waitStarted(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	gaveUp := publishAsync(ls, newPending(ctx, "b"))
	kept := publishAsync(ls, newPending(context.Background(), "c"))
	waitQueued(t, ls.lanes[0], 2)
	cancel()
	if err := <-gaveUp; !errors.Is(err, context.Canceled) {
		t.Errorf("abandoned publish: err = %v, want context.Canceled", err)
	}
	close(f.gate)
	<-first
	if err := <-kept; err != nil {
		t.Fatalf("kept publish: %v", err)
	}
	if got := channelsOf(f.batch(1)); fmt.Sprint(got) != "[c]" {
		t.Errorf("batch after cancel = %v, want [c] (the abandoned publish skipped)", got)
	}
}

// TestLanePerChannelOrderUnderLoad drives many channels from one
// submitter each, with random commit latency and random hot-channel
// deferrals, and checks every channel commits in submission order.
func TestLanePerChannelOrderUnderLoad(t *testing.T) {
	f := newFakeCommitter()
	f.latency = func() time.Duration { return time.Duration(rand.IntN(3000)) * time.Microsecond }
	f.deferIf = func(p *pending, call int) bool { return p.deferrals == 0 && rand.IntN(10) == 0 }
	f.seqOf = func(p *pending) int { return p.ctx.Value(seqKey{}).(int) }
	ls := testLanes(t, Batching{Lanes: 3, BatchMax: 16, LingerMax: time.Millisecond}, f)

	const channels, perChannel = 40, 50
	var wg sync.WaitGroup
	errs := make(chan error, channels*perChannel)
	for c := range channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var outs []<-chan error
			for i := range perChannel {
				ctx := context.WithValue(context.Background(), seqKey{}, i)
				outs = append(outs, publishAsync(ls, newPending(ctx, fmt.Sprintf("ch%d", c))))
				waitQueuedOrDone(ls, fmt.Sprintf("ch%d", c), i+1, f)
			}
			for _, o := range outs {
				if err := <-o; err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("publish: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range channels {
		got := f.order[fmt.Sprintf("ch%d", c)]
		if len(got) != perChannel {
			t.Fatalf("ch%d committed %d publishes, want %d", c, len(got), perChannel)
		}
		for i, seq := range got {
			if seq != i {
				t.Fatalf("ch%d commit order = %v, want 0..%d in order", c, got, perChannel-1)
			}
		}
	}
}

type seqKey struct{}

func channelsOf(b []*pending) []string {
	out := make([]string, len(b))
	for i, p := range b {
		out[i] = p.channel
	}
	return out
}

// waitQueued waits until the lane has n publishes queued.
func waitQueued(t *testing.T, l *lane, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		q := len(l.queue)
		l.mu.Unlock()
		if q >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("lane never reached %d queued publishes", n)
}

// waitQueuedOrDone blocks the per-channel submitter until its i-th
// publish has entered the lane (queued, in flight or committed), so each
// channel's submission order is well defined.
func waitQueuedOrDone(ls *laneSet, channel string, n int, f *fakeCommitter) {
	l := ls.laneFor(channel)
	for {
		seen := map[*pending]bool{}
		l.mu.Lock()
		for _, p := range l.queue {
			if p.channel == channel {
				seen[p] = true
			}
		}
		l.mu.Unlock()
		f.mu.Lock()
		for _, b := range f.batches {
			for _, p := range b {
				if p.channel == channel {
					seen[p] = true
				}
			}
		}
		f.mu.Unlock()
		if len(seen) >= n {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// hangingCommitter blocks its first two attempts until their context
// ends (a stuck connection), then commits normally.
type hangingCommitter struct {
	mu    sync.Mutex
	calls int
}

func (h *hangingCommitter) commitBatch(ctx context.Context, batch []*pending) ([]*pending, error) {
	h.mu.Lock()
	h.calls++
	n := h.calls
	h.mu.Unlock()
	if n <= 2 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for _, p := range batch {
		p.res = pendingResult{cm: &protocol.ChannelMessage{ChannelSerial: "ok"}}
	}
	return nil, nil
}

// TestLaneStuckCommitIsBounded: an attempt that never returns on its own
// is cut off by commitAttemptTimeout, so the batch fails retriably and
// the lane goes on to commit the next one instead of wedging.
func TestLaneStuckCommitIsBounded(t *testing.T) {
	orig := commitAttemptTimeout
	commitAttemptTimeout = 50 * time.Millisecond
	defer func() { commitAttemptTimeout = orig }()
	ls := testLanes(t, Batching{Lanes: 1}, &hangingCommitter{})

	start := time.Now()
	_, _, err := ls.publish(newPending(context.Background(), "a"))
	if !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("publish whose batch hung twice: err = %v, want storage.ErrUnavailable", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("stuck batch took %v to fail, want about two attempt timeouts", took)
	}
	if _, _, err := ls.publish(newPending(context.Background(), "a")); err != nil {
		t.Errorf("next publish on the same channel after the stuck batch: %v", err)
	}
}

// TestLaneLingerMinAccumulatesWhenIdle: with a linger floor, an idle
// lane holds its first publish for LingerMin so publishes arriving
// meanwhile share its commit, instead of committing it at once.
func TestLaneLingerMinAccumulatesWhenIdle(t *testing.T) {
	const floor = 80 * time.Millisecond
	f := newFakeCommitter()
	ls := testLanes(t, Batching{Lanes: 1, LingerMin: floor, LingerMax: time.Hour}, f)
	start := time.Now()
	first := publishAsync(ls, newPending(context.Background(), "a"))
	var rest []<-chan error
	for i := range 4 {
		rest = append(rest, publishAsync(ls, newPending(context.Background(), fmt.Sprintf("c%d", i))))
	}
	if n := waitStarted(t, f); n != 5 {
		t.Fatalf("first batch = %d publishes, want all 5 held by the linger floor", n)
	}
	if err := <-first; err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if took := time.Since(start); took < floor-10*time.Millisecond {
		t.Errorf("first publish committed after %v, want at least the %v floor", took, floor)
	}
	for _, c := range rest {
		if err := <-c; err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if f.calls() != 1 {
		t.Errorf("commits = %d, want 1", f.calls())
	}
}

// TestLaneLingerMinFullBatchCommitsAtOnce: a full batch does not wait
// for the linger floor.
func TestLaneLingerMinFullBatchCommitsAtOnce(t *testing.T) {
	f := newFakeCommitter()
	ls := testLanes(t, Batching{Lanes: 1, LingerMin: time.Hour, BatchMax: 3, LingerMax: time.Hour}, f)
	var outs []<-chan error
	for i := range 3 {
		outs = append(outs, publishAsync(ls, newPending(context.Background(), fmt.Sprintf("c%d", i))))
	}
	if n := waitStarted(t, f); n != 3 {
		t.Fatalf("batch = %d publishes, want the full 3", n)
	}
	for _, c := range outs {
		if err := <-c; err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
}

// TestLaneLingerMinAfterInFlightCommit: publishes queued behind an
// in-flight commit for longer than the floor commit as soon as it
// returns; the floor never adds to a wait they have already served.
func TestLaneLingerMinAfterInFlightCommit(t *testing.T) {
	const floor = 400 * time.Millisecond
	f := newFakeCommitter()
	f.gate = make(chan struct{})
	ls := testLanes(t, Batching{Lanes: 1, LingerMin: floor, LingerMax: time.Hour}, f)
	first := publishAsync(ls, newPending(context.Background(), "a"))
	if n := waitStarted(t, f); n != 1 {
		t.Fatalf("first batch = %d, want 1", n)
	}
	second := publishAsync(ls, newPending(context.Background(), "b"))
	time.Sleep(2 * floor) // b has now waited longer than the floor
	released := time.Now()
	close(f.gate)
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	if n := waitStarted(t, f); n != 1 {
		t.Fatalf("second batch = %d, want 1", n)
	}
	if took := time.Since(released); took > floor/2 {
		t.Errorf("second batch started %v after the first returned, want at once", took)
	}
	if err := <-second; err != nil {
		t.Fatalf("second: %v", err)
	}
}

// TestLaneConfigGauges: ably_publish_lanes and the linger gauges report
// the lane set's configuration, so a run can confirm what it ran with.
func TestLaneConfigGauges(t *testing.T) {
	m := newWriteMetrics()
	ls := newLaneSet(Batching{Lanes: 3, LingerMax: 10 * time.Millisecond, LingerMin: 2 * time.Millisecond}.resolve(), newFakeCommitter(), m)
	t.Cleanup(ls.close)
	for name, c := range map[string]struct {
		g    prometheus.Gauge
		want float64
	}{
		"lanes":      {m.lanes, 3},
		"linger_max": {m.lingerMax, 0.01},
		"linger_min": {m.lingerMin, 0.002},
	} {
		var got dto.Metric
		if err := c.g.Write(&got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := got.GetGauge().GetValue(); got != c.want {
			t.Errorf("%s gauge = %v, want %v", name, got, c.want)
		}
	}
}
