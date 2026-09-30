//go:build integration

package postgres

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// settledGoroutines returns the goroutine count once it has stopped
// moving: the lowest of several samples taken a little apart, so a
// transient goroutine (a pgx background read, a timer) is not counted.
func settledGoroutines() int {
	low := runtime.NumGoroutine()
	for range 10 {
		time.Sleep(20 * time.Millisecond)
		low = min(low, runtime.NumGoroutine())
	}
	return low
}

// TestNATSBusGoroutinesDoNotScaleWithChannels: binding N channels on
// the nats bus adds O(1) goroutines, not O(N) (DESIGN.md §7.2, receive
// fan-in). nats.go's async Subscribe starts one goroutine per
// subscription; the bus instead feeds every subscription into a fixed
// set of dispatch shards. Each channel still has its own subject (the
// node holds one subscription per bound channel), and delivery on every
// channel still works cross-node.
func TestNATSBusGoroutinesDoNotScaleWithChannels(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	bus2 := natsBusOf(t, s2)

	// Warm up: a first bind grows the pool and starts what a bind
	// starts once.
	for i := range 10 {
		bindChannel(t, s2, fmt.Sprintf("warm-%d", i), &cmRecorder{})
	}
	before := settledGoroutines()

	const channels = 1000
	recs := make([]*cmRecorder, channels)
	for i := range channels {
		recs[i] = &cmRecorder{}
		bindChannel(t, s2, fmt.Sprintf("ch-%d", i), recs[i])
	}
	after := settledGoroutines()

	if got := bus2.nc.NumSubscriptions(); got != channels+10 {
		t.Fatalf("node2 holds %d NATS subscriptions, want %d (one per bound channel)", got, channels+10)
	}
	grew := after - before
	t.Logf("binding %d channels: goroutines %d -> %d (%+d, %.3f per channel)", channels, before, after, grew, float64(grew)/channels)
	if grew > 20 {
		t.Fatalf("binding %d channels added %d goroutines; want O(1) (at most 20)", channels, grew)
	}

	// Delivery still reaches every channel, cross-node, in order.
	want := make([][]string, channels)
	for i := 0; i < channels; i += 97 {
		ch1 := bindChannel(t, s1, fmt.Sprintf("ch-%d", i), &cmRecorder{})
		for j := range 3 {
			cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m-%d-%d", i, j)}})
			if err != nil {
				t.Fatalf("Store ch-%d: %v", i, err)
			}
			want[i] = append(want[i], cm.ChannelSerial)
		}
	}
	for i := 0; i < channels; i += 97 {
		recs[i].waitFor(t, len(want[i]), 5*time.Second)
		assertSerials(t, fmt.Sprintf("node2 ch-%d", i), recs[i].serials(), want[i])
	}
}

// TestNATSBusRecordsDeliveryLag: every cm a node receives over the bus
// from another node is recorded in the delivery-lag histogram on the
// inline path (ably_bus_delivery_lag_seconds, DESIGN.md §10); the
// publisher's own fast-path deliveries are not.
func TestNATSBusRecordsDeliveryLag(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	bindChannel(t, s2, "room", a2)

	const total = 10
	for i := range total {
		if _, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m-%d", i)}}); err != nil {
			t.Fatalf("Store %d: %v", i, err)
		}
	}
	a2.waitFor(t, total, 5*time.Second)
	a1.waitFor(t, total, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // the echoes reach node1 and are dropped

	lag2 := s2.BusStats().DeliveryLag["inline"]
	if lag2.Count != total {
		t.Errorf("node2 inline lag observations = %d, want %d", lag2.Count, total)
	}
	if lag2.Sum <= 0 || lag2.Sum/float64(lag2.Count) > 1 {
		t.Errorf("node2 mean lag %.4fs (sum %.4f), want above 0 and well under 1 s on one host", lag2.Sum/float64(max(lag2.Count, 1)), lag2.Sum)
	}
	for path, h := range s1.BusStats().DeliveryLag {
		if h.Count != 0 {
			t.Errorf("node1 (the publisher) recorded %d %s lag observations, want 0", h.Count, path)
		}
	}
}

// swapFanIn sets the fan-in tuning for one test; nodes opened after it
// snapshot the values at dial.
func swapFanIn(t *testing.T, shards, queueLen, fetches int) {
	t.Helper()
	os, oq, of := natsDispatchShards, natsDispatchQueueLen, natsPointerFetches
	natsDispatchShards, natsDispatchQueueLen, natsPointerFetches = shards, queueLen, fetches
	t.Cleanup(func() { natsDispatchShards, natsDispatchQueueLen, natsPointerFetches = os, oq, of })
}

// TestNATSBusPointersInterleavedWithInlineStayOrdered: large cms travel
// as pointers whose bodies are read off the dispatch worker, with a
// single read allowed in flight so later pointers take the fallback (no
// body, filled from the log after the hold). Small cms between them
// arrive inline and may overtake a pointer's read. The receiver still
// appends every cm exactly once, in serial order (DESIGN.md §7.2,
// receive fan-in).
func TestNATSBusPointersInterleavedWithInlineStayOrdered(t *testing.T) {
	origSweep := natsSweepDefault
	natsSweepDefault = time.Hour
	t.Cleanup(func() { natsSweepDefault = origSweep })
	c := pgtest.Start(t)
	n := natstest.Start(t)

	// One read in flight sends later pointers down the fallback; the
	// default lets reads run concurrently, so inline cms overtake them.
	for _, fetches := range []int{1, natsPointerFetches} {
		t.Run(fmt.Sprintf("fetches=%d", fetches), func(t *testing.T) {
			swapFanIn(t, natsDispatchShards, natsDispatchQueueLen, fetches)
			dsn := c.FreshSchemaDSN(t)
			ctx := context.Background()

			s1 := openNATSNode(t, dsn, n.URL, func(o *Options) { o.NATSInlineMaxBytes = 1024 })
			s2 := openNATSNode(t, dsn, n.URL)
			a2 := &cmRecorder{}
			ch1 := bindChannel(t, s1, "room", &cmRecorder{})
			bindChannel(t, s2, "room", a2)

			big := strings.Repeat("x", 4*1024)
			var want []string
			for i := range 40 {
				data := fmt.Sprintf("small-%d", i)
				if i%3 != 2 {
					data = big
				}
				want = append(want, publish(t, ctx, ch1, data))
			}
			a2.waitFor(t, len(want), 10*time.Second)
			time.Sleep(300 * time.Millisecond) // room for a (wrong) duplicate
			assertSerials(t, "node2", a2.serials(), want)
			for i, cm := range a2.cms() {
				if i%3 != 2 && cm.Messages[0].Data != big {
					t.Fatalf("cm %d: pointer body has %d bytes, want %d", i, len(fmt.Sprint(cm.Messages[0].Data)), len(big))
				}
			}
			if pub := s1.BusStats().Pointers; pub == 0 {
				t.Fatal("no cm travelled as a pointer")
			}
			st := s2.BusStats()
			t.Logf("node2: inline=%d fetched=%d filled=%d holds=%d gapFills=%d", st.Inline, st.Fetched, st.Filled, st.Held, st.GapFills)
		})
	}
}

// gatedRecorder is a cmRecorder whose first Append blocks until gate is
// closed, which stalls the dispatch worker delivering to it.
type gatedRecorder struct {
	cmRecorder
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (g *gatedRecorder) Append(cm *protocol.ChannelMessage) {
	g.once.Do(func() {
		close(g.entered)
		<-g.gate
	})
	g.cmRecorder.Append(cm)
}

// TestNATSBusShardOverflowRecoversFromLog: with one dispatch shard of
// four messages, a stalled delivery on one channel fills the shard and
// NATS drops messages for it and for another channel on the same shard
// (a slow consumer). Once the stall ends, every cm on both channels is
// appended exactly once and in serial order, the dropped ones read from
// the log by the gap fill or the sweep (DESIGN.md §7.2).
func TestNATSBusShardOverflowRecoversFromLog(t *testing.T) {
	swapFanIn(t, 1, 4, natsPointerFetches)
	swapNATSTimings(t, 50*time.Millisecond, 200*time.Millisecond)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	hot := &gatedRecorder{gate: make(chan struct{}), entered: make(chan struct{})}
	cold := &cmRecorder{}
	chHot := bindChannel(t, s1, "hot", &cmRecorder{})
	chCold := bindChannel(t, s1, "cold", &cmRecorder{})
	bindChannel(t, s2, "hot", hot)
	bindChannel(t, s2, "cold", cold)

	wantHot := []string{publish(t, ctx, chHot, "stall")}
	select {
	case <-hot.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("node2 never delivered the first hot cm")
	}
	var wantCold []string
	for i := range 60 {
		wantHot = append(wantHot, publish(t, ctx, chHot, fmt.Sprintf("h-%d", i)))
		if i%3 == 0 {
			wantCold = append(wantCold, publish(t, ctx, chCold, fmt.Sprintf("c-%d", i)))
		}
	}
	waitFor(t, 5*time.Second, "NATS to drop messages for the full shard", func() bool { return s2.BusStats().Drops > 0 })
	close(hot.gate)

	hot.waitFor(t, len(wantHot), 15*time.Second)
	cold.waitFor(t, len(wantCold), 15*time.Second)
	time.Sleep(500 * time.Millisecond) // room for a (wrong) duplicate
	assertSerials(t, "node2 hot", hot.serials(), wantHot)
	assertSerials(t, "node2 cold", cold.serials(), wantCold)
	st := s2.BusStats()
	t.Logf("node2: drops=%d inline=%d filled=%d gapFills=%d sweepCatchUps=%d", st.Drops, st.Inline, st.Filled, st.GapFills, st.SweepCatchUps)
}
