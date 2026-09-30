package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Publish batching defaults (DESIGN.md §6.3 "Publish batching").
const (
	DefaultPublishLanes     = 4
	DefaultPublishBatchMax  = 200
	DefaultPublishLingerMax = 5 * time.Millisecond
	DefaultPublishQueueMax  = 10000
)

// maxInflightPerLane bounds how many batches one lane has in flight: one
// normally, a second only when the first has been in flight longer than
// the linger cap (see lane.pumpLocked).
const maxInflightPerLane = 2

// deferralsBeforeWait is how many times a publish may be deferred
// because its channel's row was locked by another transaction before its
// next batch waits for that lock instead of skipping it, so a hot
// channel written from many nodes cannot be starved.
const deferralsBeforeWait = 2

// errLanesClosed is returned for publishes submitted to, or still queued
// in, a closed lane set: retriable, since nothing was stored.
var errLanesClosed = fmt.Errorf("%w: publish lanes closed", storage.ErrUnavailable)

// Batching configures leading-edge publish batching (DESIGN.md §6.3).
type Batching struct {
	// Lanes is the number of publish lanes; a channel always maps to the
	// same lane (by hash), so its publishes keep their order. Zero or
	// negative disables batching: every publish is its own transaction.
	// The server's --publish-lanes defaults to DefaultPublishLanes.
	Lanes int

	// BatchMax caps the publishes in one transaction. Zero means
	// DefaultPublishBatchMax.
	BatchMax int

	// LingerMax caps how long publishes accumulate behind a commit that
	// has stalled: once a lane's batch has been in flight this long,
	// queued publishes on channels not in it are committed in a second
	// transaction. Zero means DefaultPublishLingerMax.
	LingerMax time.Duration

	// QueueMax bounds each lane's queue; beyond it a publish fails with
	// storage.ErrOverloaded. Zero means DefaultPublishQueueMax.
	QueueMax int
}

func (b Batching) enabled() bool { return b.Lanes > 0 }

func (b Batching) resolve() Batching {
	if b.BatchMax <= 0 {
		b.BatchMax = DefaultPublishBatchMax
	}
	if b.LingerMax <= 0 {
		b.LingerMax = DefaultPublishLingerMax
	}
	if b.QueueMax <= 0 {
		b.QueueMax = DefaultPublishQueueMax
	}
	return b
}

// pending is one publish waiting in a lane for its batch to commit.
type pending struct {
	channel string
	cs      *channelStore // nil in lane unit tests
	msgs    []*protocol.Message
	batchID string
	// checkIDs is true when the ids were supplied by the client, so the
	// publish must be checked for idempotency; server-generated ids are
	// unique by construction and skip the lookup.
	checkIDs bool

	ctx       context.Context
	enqueued  time.Time
	deferrals int // times deferred because the channel row was locked elsewhere
	// retried is set before a failed batch's second attempt. The first
	// attempt's COMMIT may have reached the database with only its reply
	// lost, so the retry checks every publish's (stamped) ids, server-
	// generated ones included, and returns the original on a hit.
	retried bool
	// minted and mintedPrev are the serial (and its predecessor) the
	// first attempt gave this publish. A retry that finds the publish
	// stored under minted knows the first attempt committed: the publish
	// is this caller's own, not a duplicate, and its post-commit bus hook
	// never ran, so the retry runs it.
	minted, mintedPrev string

	res  pendingResult
	done chan struct{} // closed once res is final
}

type pendingResult struct {
	cm         *protocol.ChannelMessage
	idempotent bool
	err        error
}

func newPending(ctx context.Context, channel string) *pending {
	return &pending{channel: channel, ctx: ctx, done: make(chan struct{})}
}

// finish records the result and releases the waiting caller.
func (p *pending) finish(res pendingResult) {
	p.res = res
	close(p.done)
}

// committer commits a batch of publishes as one transaction.
type committer interface {
	// commitBatch commits batch atomically. It returns the pendings it
	// deferred (their channel's row was locked by another transaction),
	// in batch order, and must have set res on every other pending.
	// Deferral is per channel: if one publish of a channel is deferred,
	// every later publish of that channel in the batch must be too, or
	// the channel's order would break. An
	// error means the transaction did not commit and no res was set
	// (the caller cannot tell whether a failed COMMIT reached the
	// database, which is why a retry relies on idempotency).
	commitBatch(ctx context.Context, batch []*pending) (deferred []*pending, err error)
}

// laneSet routes publishes to lanes by channel hash.
type laneSet struct {
	lanes   []*lane
	metrics *writeMetrics
	cancel  context.CancelFunc // cancels in-flight batches at close
}

func newLaneSet(b Batching, c committer, m *writeMetrics) *laneSet {
	ls := &laneSet{metrics: m}
	var ctx context.Context
	ctx, ls.cancel = context.WithCancel(context.Background())
	for i := range b.Lanes {
		ls.lanes = append(ls.lanes, &lane{
			idx: i, opts: b, c: c, metrics: m, ctx: ctx,
			busy:  make(map[string]int),
			depth: m.queueDepth.WithLabelValues(strconv.Itoa(i)),
		})
	}
	return ls
}

// laneFor returns the lane a channel's publishes use.
func (ls *laneSet) laneFor(channel string) *lane {
	h := fnv.New32a()
	_, _ = h.Write([]byte(channel))
	return ls.lanes[h.Sum32()%uint32(len(ls.lanes))]
}

// publish enqueues p and waits for its result or for p.ctx to end. If
// the caller gives up while p is queued, p is skipped; once its batch
// has started, the publish may still commit.
func (ls *laneSet) publish(p *pending) (*protocol.ChannelMessage, bool, error) {
	if err := ls.laneFor(p.channel).submit(p); err != nil {
		return nil, false, err
	}
	select {
	case <-p.done:
		return p.res.cm, p.res.idempotent, p.res.err
	case <-p.ctx.Done():
		return nil, false, p.ctx.Err()
	}
}

// laneCloseGrace is how long close lets in-flight batches finish before
// cancelling them.
const laneCloseGrace = 5 * time.Second

// close stops accepting publishes, fails the queued ones, and waits for
// the batches in flight, cancelling them after laneCloseGrace.
func (ls *laneSet) close() {
	// Stop every lane first, so none keeps accepting or starting
	// batches while another drains; then wait for their batches.
	for _, l := range ls.lanes {
		l.stop()
	}
	done := make(chan struct{})
	go func() {
		for _, l := range ls.lanes {
			l.wg.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(laneCloseGrace):
	}
	ls.cancel()
	<-done
}

// lane is one leading-edge batcher (DESIGN.md §6.3). When idle, the
// first publish commits at once. While a batch is in flight, arrivals
// queue; when it completes, everything queued (up to BatchMax) commits
// as the next batch. So at low load a publish costs one commit, and at
// high load the batch grows to match the commit latency.
//
// A channel is in at most one in-flight batch, so its publishes commit
// in the order they were queued. If a batch has been in flight longer
// than LingerMax, a second batch may start with the queued publishes of
// other channels, so a stalled commit delays only the channels in it.
type lane struct {
	idx     int
	opts    Batching
	c       committer
	metrics *writeMetrics
	depth   prometheus.Gauge
	ctx     context.Context

	mu           sync.Mutex
	queue        []*pending
	inflight     int
	busy         map[string]int // channels in in-flight batches
	lastDispatch time.Time
	timer        *time.Timer
	closed       bool
	wg           sync.WaitGroup
}

// submit enqueues p, failing fast when the queue is full or the lane is
// closed.
func (l *lane) submit(p *pending) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLanesClosed
	}
	if len(l.queue) >= l.opts.QueueMax {
		l.metrics.nacks.WithLabelValues("queue_full").Inc()
		return storage.ErrOverloaded
	}
	p.enqueued = time.Now()
	l.queue = append(l.queue, p)
	l.depth.Set(float64(len(l.queue)))
	l.pumpLocked()
	return nil
}

// pumpLocked starts as many batches as the rules allow: one at once when
// nothing is in flight (the leading edge); a further one only when the
// newest in-flight batch has been running for LingerMax, and then only
// with publishes whose channels are not already in flight.
func (l *lane) pumpLocked() {
	for len(l.queue) > 0 && !l.closed && l.inflight < maxInflightPerLane {
		if l.inflight > 0 {
			if wait := l.opts.LingerMax - time.Since(l.lastDispatch); wait > 0 {
				l.armTimerLocked(wait)
				return
			}
		}
		batch := l.takeLocked()
		if len(batch) == 0 {
			return // every queued channel is in flight; a completion pumps again
		}
		l.inflight++
		l.lastDispatch = time.Now()
		for _, p := range batch {
			l.busy[p.channel]++
		}
		l.wg.Add(1)
		go l.run(batch)
	}
}

// armTimerLocked schedules a pump after d, for the linger cap.
func (l *lane) armTimerLocked(d time.Duration) {
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(d, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.pumpLocked()
	})
}

// takeLocked removes and returns up to BatchMax queued publishes whose
// channels are not in flight, in queue order. Publishes whose caller has
// already given up are dropped.
func (l *lane) takeLocked() []*pending {
	var batch, rest []*pending
	for _, p := range l.queue {
		switch {
		case len(batch) >= l.opts.BatchMax, l.busy[p.channel] > 0:
			rest = append(rest, p)
		case p.ctx.Err() != nil:
			p.finish(pendingResult{err: p.ctx.Err()})
		default:
			batch = append(batch, p)
		}
	}
	l.queue = rest
	l.depth.Set(float64(len(l.queue)))
	return batch
}

// run commits one batch, retrying once on failure, then hands deferred
// publishes back to the head of the queue and releases the rest.
func (l *lane) run(batch []*pending) {
	defer l.wg.Done()
	start := time.Now()
	deferred, err := l.attempt(batch)
	if err != nil && l.ctx.Err() == nil {
		l.metrics.retries.Inc()
		for _, p := range batch {
			p.retried = true
		}
		deferred, err = l.attempt(batch)
	}
	l.metrics.commitSeconds.Observe(time.Since(start).Seconds())

	isDeferred := make(map[*pending]bool, len(deferred))
	for _, p := range deferred {
		isDeferred[p] = true
		p.deferrals++
	}
	if err == nil {
		l.metrics.commits.Inc()
		l.metrics.batchSize.Observe(float64(len(batch) - len(deferred)))
		l.metrics.deferred.Add(float64(len(deferred)))
	} else {
		l.metrics.nacks.WithLabelValues("commit_failed").Add(float64(len(batch)))
	}

	l.mu.Lock()
	for _, p := range batch {
		if l.busy[p.channel]--; l.busy[p.channel] <= 0 {
			delete(l.busy, p.channel)
		}
	}
	l.inflight--
	if err == nil && len(deferred) > 0 {
		if l.closed {
			for _, p := range deferred {
				p.finish(pendingResult{err: errLanesClosed})
			}
		} else {
			l.queue = append(append(make([]*pending, 0, len(deferred)+len(l.queue)), deferred...), l.queue...)
			l.depth.Set(float64(len(l.queue)))
		}
	}
	l.pumpLocked()
	l.mu.Unlock()

	for _, p := range batch {
		switch {
		case err != nil:
			p.finish(pendingResult{err: fmt.Errorf("%w: %w", storage.ErrUnavailable, err)})
		case !isDeferred[p]:
			close(p.done)
		}
	}
}

// commitAttemptTimeout bounds one commit attempt of a batch, so a stuck
// connection or statement cannot keep the batch's channels busy, and its
// lane stalled, indefinitely. The retry that follows finds anything the
// timed-out attempt did commit. A var so tests can shrink it.
var commitAttemptTimeout = 15 * time.Second

// attempt runs one commit attempt of batch under commitAttemptTimeout.
func (l *lane) attempt(batch []*pending) ([]*pending, error) {
	ctx, cancel := context.WithTimeout(l.ctx, commitAttemptTimeout)
	defer cancel()
	return l.c.commitBatch(ctx, batch)
}

// close fails queued publishes and waits for in-flight batches.
func (l *lane) close() {
	l.stop()
	l.wg.Wait()
}

// stop marks the lane closed and fails its queued publishes, without
// waiting for the batches in flight.
func (l *lane) stop() {
	l.mu.Lock()
	l.closed = true
	if l.timer != nil {
		l.timer.Stop()
	}
	queued := l.queue
	l.queue = nil
	l.depth.Set(0)
	l.mu.Unlock()
	for _, p := range queued {
		p.finish(pendingResult{err: errLanesClosed})
	}
}

// writeMetrics are the ably_publish_* batching series (DESIGN.md §10).
type writeMetrics struct {
	batchSize     prometheus.Histogram
	commits       prometheus.Counter
	commitSeconds prometheus.Histogram
	queueDepth    *prometheus.GaugeVec
	deferred      prometheus.Counter
	retries       prometheus.Counter
	nacks         *prometheus.CounterVec
}

func newWriteMetrics() *writeMetrics {
	return &writeMetrics{
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_publish_batch_size",
			Help:    "Publishes committed per batch transaction.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 9),
		}),
		commits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_publish_commits_total",
			Help: "Publish batch transactions committed.",
		}),
		commitSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_publish_commit_seconds",
			Help:    "Time to commit one publish batch, including a retry.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
		}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ably_publish_lane_queue_depth",
			Help: "Publishes queued in a lane, waiting for a batch.",
		}, []string{"lane"}),
		deferred: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_publish_deferred_total",
			Help: "Publishes deferred to a later batch because their channel's row was locked by another transaction (a hot channel).",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_publish_batch_retries_total",
			Help: "Publish batches retried after a failed commit.",
		}),
		nacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_publish_nacks_total",
			Help: "Publishes refused by the batching layer, by reason (queue_full, commit_failed).",
		}, []string{"reason"}),
	}
}

func (m *writeMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.batchSize, m.commits, m.commitSeconds, m.queueDepth, m.deferred, m.retries, m.nacks}
}
