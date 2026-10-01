package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/ably/ably-server/internal/protocol"
)

func TestFanoutStripeSize(t *testing.T) {
	if n := unsafe.Sizeof(fanoutStripe{}); n%64 != 0 {
		t.Fatalf("fanoutStripe is %d bytes, want a multiple of the 64-byte cache line", n)
	}
}

func TestNewFanoutPoolDisabled(t *testing.T) {
	if p := NewFanoutPool(0, 10); p != nil {
		t.Fatal("NewFanoutPool(0, …) returned a pool, want nil (disabled)")
	}
	var p *FanoutPool
	p.Close()
	if p.Workers() != 0 || p.QueueDepth() != 0 || p.Busy() != 0 || p.Deliveries() != 0 {
		t.Fatal("a nil pool reports non-zero stats")
	}
	if p := NewFanoutPool(2, 0); p.Threshold() != DefaultFanoutThreshold {
		t.Fatalf("threshold 0 gave %d, want the default %d", p.Threshold(), DefaultFanoutThreshold)
	} else {
		p.Close()
	}
}

// pooledReader drives one Stream the way an attachment does: a delivery
// function for the pool and a goroutine calling Next. Both record into
// got, so the race detector checks that the cursor's handovers between
// the pool and the goroutine order those writes (DESIGN.md §5.1).
type pooledReader struct {
	s       *Stream
	got     []string
	pooled  int // entries delivered by the pool
	refused int // entries the delivery function refused (handed back)
	rnd     *rand.Rand
	n       atomic.Int64 // len(got), for the test to wait on
	exited  atomic.Bool
	t       *testing.T
}

func (r *pooledReader) deliver(cm *protocol.ChannelMessage) bool {
	if r.exited.Load() {
		r.t.Errorf("delivery after the stream's goroutine left Next")
	}
	if r.s.cursor.cm != cm {
		r.t.Errorf("delivery of %s with the cursor on another entry", cm.ChannelSerial)
	}
	// Refuse about one entry in five: the stream is then handed back and
	// its goroutine delivers this entry.
	if r.rnd.IntN(5) == 0 {
		r.refused++
		return false
	}
	r.got = append(r.got, cm.ChannelSerial)
	r.n.Add(1)
	r.pooled++
	return true
}

// run reads until ctx ends. A pooled Stream's goroutine stays parked in
// Next while the pool delivers, so the test cancels ctx once n shows
// every entry recorded.
func (r *pooledReader) run(ctx context.Context, done func()) {
	defer done()
	defer r.s.Close()
	defer r.exited.Store(true)
	r.s.SetDeliver(r.deliver)
	for {
		cm, err := r.s.Next(ctx)
		if err != nil {
			return
		}
		r.got = append(r.got, cm.ChannelSerial)
		r.n.Add(1)
	}
}

// TestFanoutPoolToggleExactlyOnceInOrder checks that a Stream moving
// between the pool and its own goroutine sees every entry exactly once
// and in list order (DESIGN.md §5.1). Streams switch whenever the
// delivery function refuses an entry, and whenever the channel's
// attachment count crosses the pool's threshold (join) or half of it
// (leave), which extra Streams opening and closing drive throughout.
// Some Streams are cancelled part way: each must have a gap-free prefix
// and get no delivery after leaving.
func TestFanoutPoolToggleExactlyOnceInOrder(t *testing.T) {
	const (
		threshold = 16
		readers   = 6
		cancelled = 2
		entries   = 3000
	)
	pool := NewFanoutPool(4, threshold)
	defer pool.Close()
	c := newReadyChannel("test", "000000")
	c.pool = pool

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	rs := make([]*pooledReader, readers+cancelled)
	cancels := make([]context.CancelFunc, len(rs))
	for i := range rs {
		s, err := c.Attach(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rs[i] = &pooledReader{s: s, rnd: rand.New(rand.NewPCG(uint64(i), 7)), t: t}
		rctx, rcancel := context.WithCancel(ctx)
		cancels[i] = rcancel
		wg.Add(1)
		go rs[i].run(rctx, wg.Done)
	}

	// Extra Streams move the attachment count (8 readers' Streams, 6
	// once two are cancelled, plus the extras) through every regime, a
	// phase per 100 entries: at or below half the threshold (pooled
	// Streams are handed back), above the threshold (Streams join), and
	// in between (no change, so pooled and unpooled Streams mix).
	var extras []*Stream
	churn := func(i int) {
		target := []int{0, 20, 12, 2}[(i/100)%4]
		for len(extras) < target {
			s, err := c.Attach(ctx)
			if err != nil {
				t.Fatal(err)
			}
			extras = append(extras, s)
		}
		for len(extras) > target {
			extras[len(extras)-1].Close()
			extras = extras[:len(extras)-1]
		}
	}

	for i := 1; i <= entries; i++ {
		churn(i)
		c.Append(newCM(fmt.Sprintf("%06d", i), "m"))
		if i == entries/3 {
			cancels[readers]()
		}
		if i == entries/2 {
			cancels[readers+1]()
		}
		if i%50 == 0 {
			time.Sleep(time.Millisecond) // let the pool and the goroutines interleave
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for i := range readers {
		for rs[i].n.Load() < entries {
			if time.Now().After(deadline) {
				t.Fatalf("reader %d received %d of %d entries", i, rs[i].n.Load(), entries)
			}
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	wg.Wait()
	for _, s := range extras {
		s.Close()
	}

	var pooled, refused int
	for i, r := range rs {
		if i < readers && len(r.got) != entries {
			t.Fatalf("reader %d got %d entries, want %d", i, len(r.got), entries)
		}
		for j, serial := range r.got {
			if want := fmt.Sprintf("%06d", j+1); serial != want {
				t.Fatalf("reader %d: entry %d is %s, want %s (duplicate, gap or reorder)", i, j, serial, want)
			}
		}
		pooled += r.pooled
		refused += r.refused
	}
	if pooled == 0 || refused == 0 {
		t.Fatalf("pool delivered %d and handed back %d entries; the test must exercise both paths", pooled, refused)
	}
	// The counters are updated after a walk releases its stripe, so wait
	// for them to settle.
	waitFor(t, func() bool { return pool.Deliveries() == uint64(pooled) && pool.QueueDepth() == 0 && pool.Busy() == 0 })
}

// TestFanoutPoolLeaveOnCancel checks that a pooled Stream whose context
// ends leaves its stripe (whether still in joins or already walked), so
// a later Append delivers nothing to it, and that its Next returns the
// context's error.
func TestFanoutPoolLeaveOnCancel(t *testing.T) {
	pool := NewFanoutPool(2, 2)
	defer pool.Close()
	c := newReadyChannel("test", "000")
	c.pool = pool
	var ss []*Stream
	for range 6 {
		s, err := c.Attach(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ss = append(ss, s)
	}
	var delivered sync.Map // *Stream -> count
	errs := make(chan error, len(ss))
	ctxs := make([]context.CancelFunc, len(ss))
	for i, s := range ss {
		s.SetDeliver(func(cm *protocol.ChannelMessage) bool {
			n, _ := delivered.LoadOrStore(s, new(atomic.Int32))
			n.(*atomic.Int32).Add(1)
			return true
		})
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		go func() {
			_, err := s.Next(ctx)
			errs <- err
		}()
	}
	// Wait until every Stream has joined.
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		n := 0
		for k := range c.stripes {
			n += len(c.stripes[k].joins)
		}
		return n == len(ss)
	})
	// Cancel half while they are still in joins.
	for i := range 3 {
		ctxs[i]()
		if err := <-errs; err != context.Canceled {
			t.Fatalf("Next returned %v, want context.Canceled", err)
		}
	}
	c.Append(newCM("001", "m"))
	waitFor(t, func() bool { return pool.Deliveries() == 3 })
	// Cancel the rest, now walked members.
	for i := 3; i < len(ss); i++ {
		ctxs[i]()
		if err := <-errs; err != context.Canceled {
			t.Fatalf("Next returned %v, want context.Canceled", err)
		}
	}
	c.Append(newCM("002", "m"))
	// The walk for 002 has run once its stripes are no longer queued or
	// busy (the stripes still had members when 002 was appended).
	waitFor(t, func() bool { return pool.QueueDepth() == 0 && pool.Busy() == 0 })
	if n := pool.Deliveries(); n != 3 {
		t.Fatalf("pool delivered %d entries, want 3 (only to the members still pooled at 001)", n)
	}
	for i, s := range ss {
		v, _ := delivered.Load(s)
		var n int32
		if v != nil {
			n = v.(*atomic.Int32).Load()
		}
		if want := int32(min(i/3, 1)); n != want {
			t.Fatalf("stream %d: %d deliveries, want %d", i, n, want)
		}
	}
	for k := range c.stripes {
		st := &c.stripes[k]
		st.mu.Lock()
		c.mu.Lock()
		if len(st.subs) != 0 || len(st.joins) != 0 || st.n.Load() != 0 {
			t.Errorf("stripe %d holds %d subs, %d joins, n=%d after every stream left", k, len(st.subs), len(st.joins), st.n.Load())
		}
		c.mu.Unlock()
		st.mu.Unlock()
	}
}

// TestFanoutPoolHoldAfterRefusal checks the hold (Channel.poolHold,
// DESIGN.md §5.1): when every member refuses an entry (as for an
// append), the Streams go back to their goroutines and stay there for
// fanoutHoldEntries entries, then rejoin; a refusal by one member of
// many (one full connection queue) sets no hold.
func TestFanoutPoolHoldAfterRefusal(t *testing.T) {
	pool := NewFanoutPool(1, 2)
	defer pool.Close()
	c := newReadyChannel("test", "000000")
	c.pool = pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 8
	var refuse atomic.Value // serial every member refuses
	refuse.Store("")
	var lone atomic.Value // serial only stream 0 refuses
	lone.Store("")
	var pooledN atomic.Int64
	var goroutineN [n]atomic.Int64
	var got [n]atomic.Int64
	var wg sync.WaitGroup
	for i := range n {
		s, err := c.Attach(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.SetDeliver(func(cm *protocol.ChannelMessage) bool {
			if cm.ChannelSerial == refuse.Load() || (i == 0 && cm.ChannelSerial == lone.Load()) {
				return false
			}
			pooledN.Add(1)
			got[i].Add(1)
			return true
		})
		wg.Go(func() {
			for {
				if _, err := s.Next(ctx); err != nil {
					return
				}
				goroutineN[i].Add(1)
				got[i].Add(1)
			}
		})
	}
	seq := 0
	appendAll := func(k int) {
		for range k {
			seq++
			c.Append(newCM(fmt.Sprintf("%06d", seq), "m"))
			want := int64(seq)
			waitFor(t, func() bool {
				for i := range n {
					if got[i].Load() < want {
						return false
					}
				}
				return true
			})
		}
	}
	// Warm up until every Stream is pooled.
	for pooledN.Load() < n {
		before := pooledN.Load()
		appendAll(1)
		if pooledN.Load()-before == n {
			break
		}
	}

	// One member refuses: no hold, it rejoins on its next step.
	lone.Store(fmt.Sprintf("%06d", seq+1))
	appendAll(1)
	if h := c.poolHold.Load(); h != 0 {
		t.Fatalf("hold %d after one member of %d refused, want none", h, n)
	}

	// Every member refuses: a hold of fanoutHoldEntries.
	refuse.Store(fmt.Sprintf("%06d", seq+1))
	appendAll(1)
	hold := c.poolHold.Load()
	if hold == 0 {
		t.Fatal("no hold after every member refused an entry")
	}
	before := pooledN.Load()
	appendAll(int(hold) - seq - 1)
	if p := pooledN.Load() - before; p != 0 {
		t.Fatalf("pool delivered %d entries during the hold, want 0", p)
	}
	// Past the hold the Streams rejoin.
	for range 3 * fanoutHoldEntries {
		before := pooledN.Load()
		appendAll(1)
		if pooledN.Load()-before == n {
			cancel()
			wg.Wait()
			return
		}
	}
	t.Fatal("the Streams did not rejoin the pool after the hold")
}

// TestFanoutPoolDiscontinuityHandedBack checks that a discontinuity
// marker (Channel.Discontinuity, DESIGN.md §7.2) on a pooled channel
// reaches each Stream's goroutine, as Next returning the marker with
// Discontinuity set, in order between the cms around it, without
// setting the hold, and that the Streams rejoin for the cm after it.
func TestFanoutPoolDiscontinuityHandedBack(t *testing.T) {
	// One worker, so all 6 Streams share a stripe: enough members
	// (fanoutHoldMinRefused) that counting the marker as a refusal would
	// set the hold.
	pool := NewFanoutPool(1, 2)
	defer pool.Close()
	c := newReadyChannel("test", "000000")
	c.pool = pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const n = 6
	type rec struct {
		mu  sync.Mutex
		got []string
	}
	recs := make([]*rec, n)
	var wg sync.WaitGroup
	ss := make([]*Stream, n)
	for i := range ss {
		var err error
		if ss[i], err = c.Attach(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i, s := range ss {
		r := &rec{}
		recs[i] = r
		s.SetDeliver(func(cm *protocol.ChannelMessage) bool {
			r.mu.Lock()
			r.got = append(r.got, "pool:"+cm.ChannelSerial)
			r.mu.Unlock()
			return true
		})
		wg.Go(func() {
			for {
				cm, err := s.Next(ctx)
				if err != nil {
					return
				}
				tag := "own:"
				if s.Discontinuity() {
					tag = "gap:"
				}
				r.mu.Lock()
				r.got = append(r.got, tag+cm.ChannelSerial)
				r.mu.Unlock()
			}
		})
	}
	settled := func(want int) {
		waitFor(t, func() bool {
			for _, r := range recs {
				r.mu.Lock()
				l := len(r.got)
				r.mu.Unlock()
				if l < want {
					return false
				}
			}
			return true
		})
	}
	// Every Stream joins at its first Next (6 > 2, at the tail).
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		k := 0
		for i := range c.stripes {
			k += len(c.stripes[i].joins)
		}
		return k == n
	})
	c.Append(newCM("000001", "m"))
	settled(1)
	c.Discontinuity("test")
	settled(2)
	if h := c.poolHold.Load(); h != 0 {
		t.Fatalf("hold %d after a discontinuity marker, want none", h)
	}
	// The goroutines rejoin once they have signalled the marker.
	for k := 2; k < 200; k++ {
		c.Append(newCM(fmt.Sprintf("%06d", k), "m"))
		settled(k + 1)
		all := true
		for _, r := range recs {
			r.mu.Lock()
			all = all && r.got[len(r.got)-1] == fmt.Sprintf("pool:%06d", k)
			r.mu.Unlock()
		}
		if all {
			break
		}
	}
	cancel()
	wg.Wait()
	for i, r := range recs {
		if r.got[0] != "pool:000001" || r.got[1] != "gap:000001" {
			t.Fatalf("stream %d: %v; want pool:000001 then the marker on its goroutine", i, r.got)
		}
		for k := 2; k < len(r.got); k++ {
			if want := fmt.Sprintf("%06d", k); r.got[k][len(r.got[k])-6:] != want {
				t.Fatalf("stream %d: entry %d is %s, want %s", i, k, r.got[k], want)
			}
		}
		if last := r.got[len(r.got)-1]; last[:5] != "pool:" {
			t.Fatalf("stream %d did not rejoin the pool after the marker: %v", i, r.got)
		}
	}
}

// TestFanoutPoolJoinsOnlyAtTail checks that a Stream with an entry
// waiting after its cursor does not join (the pool would not walk that
// entry until the next Append), and that it joins once caught up.
func TestFanoutPoolJoinsOnlyAtTail(t *testing.T) {
	pool := NewFanoutPool(1, 1)
	defer pool.Close()
	c := newReadyChannel("test", "000")
	c.pool = pool
	var ss []*Stream
	for range 3 {
		s, err := c.Attach(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ss = append(ss, s)
	}
	s := ss[0]
	var pooled []string
	s.SetDeliver(func(cm *protocol.ChannelMessage) bool {
		pooled = append(pooled, cm.ChannelSerial)
		return true
	})
	c.Append(newCM("001", "m"))
	cm, err := s.Next(context.Background())
	if err != nil || cm.ChannelSerial != "001" {
		t.Fatalf("Next = %v, %v; want 001 from the goroutine (it was waiting)", cm, err)
	}
	if hb := c.join(s); hb == nil {
		t.Fatal("a caught-up stream did not join")
	}
	c.Append(newCM("002", "m"))
	waitFor(t, func() bool { return pool.Deliveries() == 1 })
	if len(pooled) != 1 || pooled[0] != "002" {
		t.Fatalf("pool delivered %v, want [002]", pooled)
	}
	c.leave(s)
}
