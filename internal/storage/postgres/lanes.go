package postgres

import (
	"context"
	"errors"
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

// serverQueueFactor sizes each lane's bound on queued server-synthesised
// presence (mustAdmit): serverQueueFactor x QueueMax, beyond the normal
// bound those publishes are exempt from.
const serverQueueFactor = 8

// errServerQueueFull is submit's answer to server-synthesised presence
// past its lane's bound. It never reaches a client: the caller writes the
// operation in a transaction of its own instead (storePresenceBatched).
var errServerQueueFull = errors.New("storage/postgres: lane queue full of server-synthesised presence")

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

	// PresenceUnbatched commits every presence operation in its own
	// transaction even when publishes are batched, as before presence
	// batching existed. The zero value routes presence through the lanes
	// with messages (DESIGN.md §6.3, §12.5); the server's
	// --presence-batching=false sets it.
	PresenceUnbatched bool
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

// pending is one publish waiting in a lane for its batch to commit: a
// message publish (msgs) or a presence operation (presence), never both.
type pending struct {
	channel string
	cs      *channelStore // nil in lane unit tests
	msgs    []*protocol.Message
	batchID string
	// presence is a presence publish's operations, folded into the
	// presence table in the batch's transaction (DESIGN.md §12.5);
	// static marks fixture members, stored with a non-expiring lease.
	presence []*protocol.PresenceMessage
	static   bool
	// mustAdmit exempts a server-synthesised presence publish from the
	// queue bound and from being dropped when its caller stops waiting:
	// nothing retries it, and a dropped LEAVE leaves its member behind
	// (DESIGN.md §12.5). It has a bound of its own, serverQueueFactor x
	// QueueMax per lane; past that its caller writes it unbatched.
	mustAdmit bool
	// after is set when submit turns a mustAdmit publish away: the last
	// publish of its channel then queued or in flight on the lane, which
	// the unbatched write must wait for so the channel's operations stay
	// in order (nil: none).
	after *pending
	// checkIDs is true when the ids were supplied by the client, so the
	// publish must be checked for idempotency; server-generated ids are
	// unique by construction and skip the lookup. Presence ids are always
	// checked, as StorePresence does unbatched.
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

// ids returns the publish's non-empty item ids, in item order: the ids
// the idempotency lookup and the in-batch duplicate check compare. A
// message publish's ids are all stamped (storage.StampMessageIDs); a
// synthesised presence event (a teardown or reaper LEAVE) has none.
func (p *pending) ids() []string {
	if p.presence != nil {
		return nonEmptyPresenceIDs(p.presence)
	}
	return nonEmptyIDs(p.msgs)
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
	m.lanes.Set(float64(b.Lanes))
	m.lingerMax.Set(b.LingerMax.Seconds())
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
	grace := time.NewTimer(laneCloseGrace)
	select {
	case <-done:
	case <-grace.C:
	}
	grace.Stop()
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
	serverQueued int // queued publishes with mustAdmit set
	// last is, per channel, the most recently submitted publish still
	// queued or in flight. A channel's publishes complete in queue order,
	// so once it is done every earlier one is.
	last         map[string]*pending
	inflight     int
	running      map[*[]*pending]time.Time // in-flight batches, by start time
	busy         map[string]int            // channels in in-flight batches
	lastDispatch time.Time
	timer        *time.Timer
	closed       bool
	wg           sync.WaitGroup
}

// submit enqueues p, failing fast when the queue is full or the lane is
// closed. Server-synthesised presence (mustAdmit) is exempt from
// QueueMax but has its own bound, serverQueueFactor x QueueMax; past it
// submit returns errServerQueueFull.
func (l *lane) submit(p *pending) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLanesClosed
	}
	if p.mustAdmit {
		if l.serverQueued >= serverQueueFactor*l.opts.QueueMax {
			p.after = l.last[p.channel]
			return errServerQueueFull
		}
	} else {
		if len(l.queue) >= l.opts.QueueMax {
			// Publishes whose callers gave up still hold queue slots until
			// they are taken; reclaim them before refusing a live one.
			l.reapLocked()
		}
		if len(l.queue) >= l.opts.QueueMax {
			l.metrics.nacks.WithLabelValues("queue_full").Inc()
			return storage.ErrOverloaded
		}
	}
	l.enqueueLocked(p)
	return nil
}

// enqueueLocked appends p to the queue and pumps.
func (l *lane) enqueueLocked(p *pending) {
	p.enqueued = time.Now()
	if p.mustAdmit {
		l.serverQueued++
	}
	if l.last == nil {
		l.last = make(map[string]*pending)
	}
	l.last[p.channel] = p
	l.queue = append(l.queue, p)
	l.depth.Set(float64(len(l.queue)))
	l.pumpLocked()
}

// forgetLocked drops p from last once it is done.
func (l *lane) forgetLocked(p *pending) {
	if l.last[p.channel] == p {
		delete(l.last, p.channel)
	}
}

// after returns the last publish of channel still queued or in flight
// on its lane, or nil.
func (ls *laneSet) after(channel string) *pending {
	l := ls.laneFor(channel)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last[channel]
}

// forceSubmit queues server-synthesised presence past its bound: an
// operation whose unbatched write could not wait for its channel's
// earlier publishes. Queued, it still commits after them. It reports
// false if the lane is closed.
func (ls *laneSet) forceSubmit(p *pending) bool {
	l := ls.laneFor(p.channel)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.metrics.serverForced.Inc()
	l.enqueueLocked(p)
	return true
}

// pumpLocked starts as many batches as the rules allow: one at once when
// nothing is in flight (the leading edge); a further one only when the newest in-flight batch has been running for
// LingerMax, and then only with publishes whose channels are not already
// in flight.
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
		if l.running == nil {
			l.running = make(map[*[]*pending]time.Time)
		}
		key := &batch
		l.running[key] = l.lastDispatch
		for _, p := range batch {
			l.busy[p.channel]++
		}
		l.wg.Add(1)
		go l.run(key, batch)
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
// already given up are dropped, whether or not their channel is busy,
// except server-synthesised presence (mustAdmit): a LEAVE whose bounded
// caller stopped waiting must still be stored, or its member stays.
func (l *lane) takeLocked() []*pending {
	var batch, rest []*pending
	for _, p := range l.queue {
		switch {
		case p.ctx.Err() != nil && !p.mustAdmit:
			l.forgetLocked(p)
			p.finish(pendingResult{err: p.ctx.Err()})
		case len(batch) >= l.opts.BatchMax, l.busy[p.channel] > 0:
			rest = append(rest, p)
		default:
			batch = append(batch, p)
		}
	}
	l.setQueueLocked(rest)
	return batch
}

// setQueueLocked replaces the queue, recounting its server-synthesised
// publishes.
func (l *lane) setQueueLocked(q []*pending) {
	l.queue = q
	l.serverQueued = 0
	for _, p := range q {
		if p.mustAdmit {
			l.serverQueued++
		}
	}
	l.depth.Set(float64(len(l.queue)))
}

// reapLocked drops queued publishes whose caller has given up.
func (l *lane) reapLocked() {
	kept := l.queue[:0]
	for _, p := range l.queue {
		if p.ctx.Err() != nil && !p.mustAdmit {
			l.forgetLocked(p)
			p.finish(pendingResult{err: p.ctx.Err()})
			continue
		}
		kept = append(kept, p)
	}
	clear(l.queue[len(kept):])
	l.setQueueLocked(kept)
}

// run commits one batch, retrying once on failure, then hands deferred
// publishes back to the head of the queue and releases the rest.
func (l *lane) run(key *[]*pending, batch []*pending) {
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
	delete(l.running, key)
	for _, p := range batch {
		if err != nil || !isDeferred[p] || l.closed {
			l.forgetLocked(p)
		}
	}
	if err == nil && len(deferred) > 0 {
		if l.closed {
			for _, p := range deferred {
				p.finish(pendingResult{err: errLanesClosed})
			}
		} else {
			l.setQueueLocked(append(append(make([]*pending, 0, len(deferred)+len(l.queue)), deferred...), l.queue...))
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

// health reports an error naming the first lane that looks stuck
// (Storage.Ping, /readyz, DESIGN.md §11): its oldest queued publish has
// waited longer than commitAttemptTimeout, or a batch has been in flight
// longer than two attempts (a commit and its retry). A healthy lane
// commits within one attempt, so either means publishes on this node are
// not completing.
func (ls *laneSet) health(now time.Time) error {
	for _, l := range ls.lanes {
		if err := l.health(now); err != nil {
			return err
		}
	}
	return nil
}

func (l *lane) health(now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.queue) > 0 {
		if waited := now.Sub(l.queue[0].enqueued); waited > commitAttemptTimeout {
			return fmt.Errorf("publish lane %d: the oldest of %d queued publishes has waited %v (over %v)",
				l.idx, len(l.queue), waited.Round(time.Millisecond), commitAttemptTimeout)
		}
	}
	for _, started := range l.running {
		if took := now.Sub(started); took > 2*commitAttemptTimeout {
			return fmt.Errorf("publish lane %d: a batch has been committing for %v (over %v)",
				l.idx, took.Round(time.Millisecond), 2*commitAttemptTimeout)
		}
	}
	return nil
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

// stop marks the lane closed and fails its queued publishes, without
// waiting for the batches in flight.
func (l *lane) stop() {
	l.mu.Lock()
	l.closed = true
	if l.timer != nil {
		l.timer.Stop()
	}
	queued := l.queue
	l.setQueueLocked(nil)
	clear(l.last)
	l.mu.Unlock()
	for _, p := range queued {
		p.finish(pendingResult{err: errLanesClosed})
	}
}

// writeMetrics are the ably_publish_* batching series (DESIGN.md §10).
type writeMetrics struct {
	lanes         prometheus.Gauge
	lingerMax     prometheus.Gauge
	batchSize     prometheus.Histogram
	commits       prometheus.Counter
	commitSeconds prometheus.Histogram
	queueDepth    *prometheus.GaugeVec
	deferred      prometheus.Counter
	retries       prometheus.Counter
	nacks         *prometheus.CounterVec
	// serverUnbatched counts server-synthesised presence written in its
	// own transaction because its lane's server-presence bound was full;
	// serverForced, that queued past the bound instead because its
	// channel's earlier publishes did not complete in its caller's time.
	serverUnbatched prometheus.Counter
	serverForced    prometheus.Counter
}

func newWriteMetrics() *writeMetrics {
	return &writeMetrics{
		lanes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ably_publish_lanes",
			Help: "Publish batching lanes (--publish-lanes); 0 when every publish commits in its own transaction.",
		}),
		lingerMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ably_publish_linger_max_seconds",
			Help: "In-flight time after which a lane starts a second batch (--publish-linger-max); 0 when batching is off.",
		}),
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
			Help: "Publishes refused by the batching layer, by reason (queue_full, commit_failed), and unbatched presence writes refused over the in-flight bound (presence_inflight).",
		}, []string{"reason"}),
		serverUnbatched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_publish_server_presence_unbatched_total",
			Help: "Server-synthesised presence (teardown and grace LEAVEs) written in a transaction of its own, after its channel's earlier publishes, because its lane already held " + strconv.Itoa(serverQueueFactor) + " x --publish-queue-max of it (DESIGN.md §6.3).",
		}),
		serverForced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_publish_server_presence_forced_total",
			Help: "Server-synthesised presence queued past its lane's bound because its channel's earlier publishes did not complete within its caller's deadline, so it could not be written around them (DESIGN.md §6.3).",
		}),
	}
}

func (m *writeMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.lanes, m.lingerMax, m.batchSize, m.commits, m.commitSeconds, m.queueDepth, m.deferred, m.retries, m.nacks, m.serverUnbatched, m.serverForced}
}
