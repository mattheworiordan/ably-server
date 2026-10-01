package core

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// DefaultFanoutThreshold is the attachment count above which a channel's
// live cms are fanned out by the FanoutPool rather than by one goroutine
// per attachment (DESIGN.md §5.1).
const DefaultFanoutThreshold = 1000

// fanoutHoldEntries is how many entries a channel stays off the pool
// after most of a stripe refused one (Channel.poolHold): an append or an
// annotation, which take the per-attachment path. Without the hold, a
// channel streaming appends would hand every attachment back, and have
// each rejoin, on every entry, which costs more than not pooling it.
const fanoutHoldEntries = 64

// fanoutHoldMinRefused is the fewest refusals in one walk that can set
// the hold, so that in a stripe of a few members one full connection
// queue does not.
const fanoutHoldMinRefused = 4

// DefaultFanoutWorkers is the default FanoutPool size: one worker per P.
func DefaultFanoutWorkers() int {
	return runtime.GOMAXPROCS(0)
}

// FanoutPool is a node's bounded fan-out worker pool (DESIGN.md §5.1).
// On a channel with more than its threshold of attachments, a Stream
// whose owner has set a delivery function (Stream.SetDeliver) stops
// parking on the entry's wake channels and joins one of the channel's
// stripes; the stripe's worker then walks every member's cursor to the
// tail after each Append and calls the delivery function for each
// entry, while the Stream's own goroutine stays parked. One fan-out
// thus readies at most one goroutine per worker, not one per
// attachment.
//
// Stripe k of every channel belongs to worker k, so a member's cursor is
// only ever moved by one worker at a time, and a worker's queue holds a
// channel at most once (Channel.stripe pending), so its length is
// bounded by the number of pooled channels.
type FanoutPool struct {
	threshold int64
	// leaveAt is the attachment count at or below which a stripe hands
	// every member back to its goroutine: half the threshold, so a
	// channel that hovers at the threshold does not flip all its
	// attachments on every attach and detach.
	leaveAt int64
	workers []*fanoutWorker

	queued     atomic.Int64 // channel stripes waiting in a worker queue
	busy       atomic.Int64 // workers walking a stripe
	deliveries atomic.Uint64

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// fanoutWorker is one pool goroutine and its FIFO of channels whose
// stripe (the worker's index) has entries to deliver.
type fanoutWorker struct {
	p     *FanoutPool
	idx   int
	mu    sync.Mutex
	tasks []*Channel
	head  int
	ready chan struct{}
}

// NewFanoutPool starts a pool of workers goroutines that takes over the
// fan-out of channels with more than threshold attachments. It returns
// nil, which disables the pool, when workers is zero or less. A
// threshold of zero or less means DefaultFanoutThreshold. Close stops
// the workers.
func NewFanoutPool(workers, threshold int) *FanoutPool {
	if workers <= 0 {
		return nil
	}
	if threshold <= 0 {
		threshold = DefaultFanoutThreshold
	}
	p := &FanoutPool{
		threshold: int64(threshold),
		leaveAt:   int64(threshold) / 2,
		stop:      make(chan struct{}),
	}
	for i := range workers {
		w := &fanoutWorker{p: p, idx: i, ready: make(chan struct{}, 1)}
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go w.run()
	}
	return p
}

// Close stops the workers and waits for them to exit. A Stream still in
// a stripe gets no further deliveries; its goroutine leaves the stripe
// when its context ends. Idempotent; a nil pool is a no-op.
func (p *FanoutPool) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
}

// Workers returns the number of workers (zero for a nil pool).
func (p *FanoutPool) Workers() int {
	if p == nil {
		return 0
	}
	return len(p.workers)
}

// Threshold returns the attachment count above which a channel is
// pooled.
func (p *FanoutPool) Threshold() int {
	if p == nil {
		return 0
	}
	return int(p.threshold)
}

// QueueDepth returns the number of channel stripes waiting for a worker
// (ably_delivery_fanout_pool_queue_depth).
func (p *FanoutPool) QueueDepth() int64 {
	if p == nil {
		return 0
	}
	return p.queued.Load()
}

// Busy returns the number of workers walking a stripe
// (ably_delivery_fanout_pool_busy).
func (p *FanoutPool) Busy() int64 {
	if p == nil {
		return 0
	}
	return p.busy.Load()
}

// Deliveries returns the number of entries the pool has handed to
// delivery functions since it started.
func (p *FanoutPool) Deliveries() uint64 {
	if p == nil {
		return 0
	}
	return p.deliveries.Load()
}

// enqueue queues c's stripe for this worker. Called with c.mu held,
// once per stripe until the worker clears the stripe's pending flag.
func (w *fanoutWorker) enqueue(c *Channel) {
	w.mu.Lock()
	w.tasks = append(w.tasks, c)
	w.mu.Unlock()
	w.p.queued.Add(1)
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

// next pops the oldest queued channel, or returns nil when the queue is
// empty.
func (w *fanoutWorker) next() *Channel {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.head == len(w.tasks) {
		return nil
	}
	c := w.tasks[w.head]
	w.tasks[w.head] = nil
	w.head++
	if w.head == len(w.tasks) {
		w.head = 0
		if cap(w.tasks) > 1024 {
			w.tasks = nil
		} else {
			w.tasks = w.tasks[:0]
		}
	}
	return c
}

func (w *fanoutWorker) run() {
	defer w.p.wg.Done()
	for {
		select {
		case <-w.p.stop:
			return
		case <-w.ready:
		}
		for {
			c := w.next()
			if c == nil {
				break
			}
			// Busy before leaving the queue, so a reader never sees the
			// walk in neither.
			w.p.busy.Add(1)
			w.p.queued.Add(-1)
			n := c.walkStripe(w.idx)
			w.p.busy.Add(-1)
			w.p.deliveries.Add(n)
			select {
			case <-w.p.stop:
				return
			default:
			}
		}
	}
}

// fanoutStripe is one worker's share of a pooled channel's attachments.
//
// A Stream joins under the Channel's mu (Channel.join), into joins; the
// worker moves joins into subs at the start of each walk. Lock order is
// mu (the stripe's) before Channel.mu: the worker holds mu for its whole
// walk and takes Channel.mu briefly inside it, and a Stream leaving on
// its context's end does the same, so once it holds mu no worker is
// delivering to it. Append and join take only Channel.mu, so a walk
// never holds up an Append.
type fanoutStripe struct {
	// joins are the Streams that joined since the worker last took them,
	// and pending is set while a walk of this stripe is queued and not
	// yet started. Both guarded by Channel.mu.
	joins   []*Stream
	pending bool

	mu sync.Mutex
	// subs are the members the worker walks. Guarded by mu.
	subs []*Stream
	// n is the number of members taken from joins and not yet handed
	// back. Raised under Channel.mu (with joins emptied in the same
	// critical section), lowered under mu, read by Append under
	// Channel.mu: a stale read is high, never low, so at worst it queues
	// a walk that finds nothing to do.
	n atomic.Int64
	// Pads a stripe to 128 bytes, so neighbouring stripes, walked by
	// different workers, do not share a cache line.
	_ [56]byte
}

// join moves s into its stripe if the channel is pooled, not on hold
// (poolHold), and s has no entry waiting, returning the channel the
// stripe's worker sends on when it hands s back. Returns nil when s
// stays on its own goroutine.
func (c *Channel) join(s *Stream) chan struct{} {
	if c.subs.Load() <= c.pool.threshold {
		return nil
	}
	// Without the lock: the cursor is at or before the tail, so a cursor
	// before the hold means the tail is too, or an entry is waiting; and
	// a closed wake slot means the next entry is linked. Either way s
	// does not join.
	if s.cursor.seq < c.poolHold.Load() || s.cursor.woken(s.slot) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// An entry already linked after the cursor would not be walked until
	// the next Append; the goroutine delivers it, then tries again.
	if s.cursor != c.tail {
		return nil
	}
	if c.stripes == nil {
		c.stripes = make([]fanoutStripe, len(c.pool.workers))
	}
	st := &c.stripes[s.slot%uint64(len(c.stripes))]
	if s.handback == nil {
		s.handback = make(chan struct{}, 1)
	}
	s.joinIdx = len(st.joins)
	st.joins = append(st.joins, s)
	return s.handback
}

// leave takes s back from its stripe when its goroutine's context ends.
// On return no worker is delivering to s and none will.
func (c *Channel) leave(s *Stream) {
	st := &c.stripes[s.slot%uint64(len(c.stripes))]
	st.mu.Lock()
	defer st.mu.Unlock()
	c.mu.Lock()
	if i := s.joinIdx; i >= 0 {
		last := len(st.joins) - 1
		st.joins[i] = st.joins[last]
		st.joins[i].joinIdx = i
		st.joins[last] = nil
		st.joins = st.joins[:last]
		s.joinIdx = -1
	}
	c.mu.Unlock()
	if s.subIdx >= 0 {
		st.remove(s.subIdx)
	}
	// A handback that raced the context's end leaves its token behind.
	select {
	case <-s.handback:
	default:
	}
}

// remove drops subs[i], moving the last member into its place. Called
// with mu held.
func (st *fanoutStripe) remove(i int) {
	s := st.subs[i]
	last := len(st.subs) - 1
	st.subs[i] = st.subs[last]
	st.subs[i].subIdx = i
	st.subs[last] = nil
	st.subs = st.subs[:last]
	s.subIdx = -1
	st.n.Add(-1)
}

// handBack returns subs[i] to its goroutine. Called with mu held.
func (st *fanoutStripe) handBack(i int) {
	s := st.subs[i]
	st.remove(i)
	s.handback <- struct{}{}
}

// markStripes queues a walk of every stripe that has members and none
// queued. Called by Append with mu held, after linking.
func (c *Channel) markStripes() {
	for k := range c.stripes {
		st := &c.stripes[k]
		if st.pending || (len(st.joins) == 0 && st.n.Load() == 0) {
			continue
		}
		st.pending = true
		c.pool.workers[k].enqueue(c)
	}
}

// walkStripe delivers to every member of stripe k each entry between
// its cursor and the tail as of the walk's start, in list order, and
// returns the number of entries delivered. A member whose delivery
// function refuses an entry is handed back with its cursor before that
// entry, so its goroutine delivers it next; when the channel has fallen
// to the pool's leaveAt, every member is handed back.
func (c *Channel) walkStripe(k int) uint64 {
	st := &c.stripes[k]
	st.mu.Lock()
	defer st.mu.Unlock()

	c.mu.Lock()
	st.pending = false
	tail := c.tail
	joins := st.joins
	st.joins = nil
	st.n.Add(int64(len(joins)))
	for _, s := range joins {
		s.joinIdx = -1
	}
	c.mu.Unlock()
	for _, s := range joins {
		s.subIdx = len(st.subs)
		st.subs = append(st.subs, s)
	}

	if c.subs.Load() <= c.pool.leaveAt {
		for i := len(st.subs) - 1; i >= 0; i-- {
			st.handBack(i)
		}
		return 0
	}
	// Every member's cursor is at or before tail: a member was at the
	// tail when it joined, which was before this walk read tail, and only
	// this worker moves it. Entries up to tail were linked before the
	// read under c.mu, so their next pointers are safe to follow.
	var n uint64
	members := len(st.subs)
	var refused, markers []*Stream
	for i := len(st.subs) - 1; i >= 0; i-- {
		s := st.subs[i]
		for s.cursor != tail {
			// A discontinuity marker (Channel.Discontinuity) goes to the
			// goroutine, which tells the client; it sets no hold.
			if s.cursor.next.discontinuity {
				st.remove(i)
				markers = append(markers, s)
				break
			}
			prev := s.cursor
			s.cursor = prev.next
			if !s.deliver(s.cursor.cm) {
				s.cursor = prev
				st.remove(i)
				refused = append(refused, s)
				break
			}
			n++
		}
	}
	// Most of the stripe refused an entry: it is one every attachment
	// takes on its own goroutine (an append, an annotation), not one
	// slow connection. Hold the channel off the pool for a while, before
	// the handbacks, so that no goroutine rejoins ahead of the hold.
	if len(refused) >= fanoutHoldMinRefused && 2*len(refused) >= members {
		hold := tail.seq + fanoutHoldEntries
		for {
			cur := c.poolHold.Load()
			if hold <= cur || c.poolHold.CompareAndSwap(cur, hold) {
				break
			}
		}
	}
	for _, s := range refused {
		s.handback <- struct{}{}
	}
	for _, s := range markers {
		s.handback <- struct{}{}
	}
	return n
}
