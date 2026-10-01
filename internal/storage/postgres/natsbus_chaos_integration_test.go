//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestNATSBusReorderedEnvelopes (plan §5 cost 6): envelopes injected
// from a raw NATS connection arrive out of order (n+1 before n), twice
// (a duplicate), and with a gap NATS never fills. The receiving node
// holds what arrives early, delivers in serial order exactly once, and
// fills the gap from the log after the 100 ms hold (DESIGN.md §7.2,
// chained delivery). The cms are committed by a node on another bus, so
// no bus message for them exists except the ones the test sends, and
// the sweep is disabled so only the chain can repair the gap.
func TestNATSBusReorderedEnvelopes(t *testing.T) {
	origSweep, origGap := natsSweepDefault, gapFillDelay
	natsSweepDefault, gapFillDelay = time.Hour, 100*time.Millisecond
	t.Cleanup(func() { natsSweepDefault, gapFillDelay = origSweep, origGap })

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s2 := openNATSNode(t, dsn, n.URL)
	silent := openListenNode(t, dsn)
	a2 := &cmRecorder{}
	bindChannel(t, s2, "room", a2)
	cs2 := boundStoreOf(t, s2, "room")
	bus2 := natsBusOf(t, s2)
	w0 := cs2.watermark()

	chSilent, err := silent.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel silent: %v", err)
	}
	var cms []*protocol.ChannelMessage
	for i := range 4 {
		cm, _, err := chSilent.Store(ctx, []*protocol.Message{{ID: fmt.Sprintf("m%d", i), Data: fmt.Sprintf("d%d", i)}})
		if err != nil {
			t.Fatalf("Store %d: %v", i, err)
		}
		cms = append(cms, cm)
	}
	prev := []string{w0, cms[0].ChannelSerial, cms[1].ChannelSerial, cms[2].ChannelSerial}
	want := []string{cms[0].ChannelSerial, cms[1].ChannelSerial, cms[2].ChannelSerial, cms[3].ChannelSerial}

	raw, err := nats.Connect(n.URL)
	if err != nil {
		t.Fatalf("raw NATS connect: %v", err)
	}
	t.Cleanup(raw.Close)
	subject := natsSubject(bus2.prefix, "room")
	inject := func(i int) {
		t.Helper()
		data, _, err := encodeNATSEnvelope(bus2.deployment, "room", cms[i], prev[i], DefaultNATSInlineMaxBytes)
		if err != nil {
			t.Fatalf("encode envelope %d: %v", i, err)
		}
		if err := raw.Publish(subject, data); err != nil {
			t.Fatalf("publish envelope %d: %v", i, err)
		}
		if err := raw.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	held := func() int { return storeCounters(cs2).held }

	// n+1 before n: held, nothing delivered.
	inject(1)
	waitFor(t, 5*time.Second, "cm 1 to be held", func() bool { return held() >= 1 })
	if got := a2.count(); got != 0 {
		t.Fatalf("delivered %d cms after cm 1 arrived ahead of cm 0, want 0 (held)", got)
	}

	// n arrives, then again: both delivered in order, the duplicate dropped.
	inject(0)
	inject(0)
	a2.waitFor(t, 2, 5*time.Second)
	waitFor(t, 5*time.Second, "the duplicate of cm 0 to be dropped", func() bool { return storeCounters(cs2).duplicates >= 1 })
	assertSerials(t, "after the reorder", a2.serials(), want[:2])
	if got := storeCounters(cs2).gapFills; got != 0 {
		t.Fatalf("gapFills = %d after cm 0 arrived, want 0: the chain, not a log read, must repair the reorder", got)
	}

	// cm 3 names cm 2 as its predecessor; NATS never carries cm 2. The
	// node holds cm 3, then reads cm 2 from the log after the hold.
	fillsBefore := storeCounters(cs2).gapFills
	sent := time.Now()
	inject(3)
	a2.waitFor(t, 4, 5*time.Second)
	filledAfter := time.Since(sent)
	if filledAfter < 90*time.Millisecond {
		t.Errorf("gap filled %s after cm 3 arrived; want the 100 ms hold first", filledAfter)
	}
	if got := storeCounters(cs2).gapFills; got <= fillsBefore {
		t.Errorf("gapFills = %d, want > %d (cm 2 read from the log)", got, fillsBefore)
	}

	// Exactly once, in serial order, with the bodies as stored.
	time.Sleep(300 * time.Millisecond)
	assertSerials(t, "node2", a2.serials(), want)
	for i, cm := range a2.cms() {
		if len(cm.Messages) != 1 || cm.Messages[0].ID != fmt.Sprintf("m%d", i) || cm.Messages[0].Data != fmt.Sprintf("d%d", i) {
			t.Errorf("cm %d = %+v, want m%d/d%d", i, cm.Messages, i, i)
		}
	}
	t.Logf("gap filled %s after the out-of-order cm; held=%d duplicates=%d gapFills=%d",
		filledAfter.Round(time.Millisecond), held(), storeCounters(cs2).duplicates, storeCounters(cs2).gapFills)
}

// TestNATSBusServerKillLosesNoCommittedMessage (plan §8, NATS server
// kill; chaos at the storage layer): the only NATS server dies while
// node1 publishes a steady stream, stays down, and is replaced by a
// fresh one. Every publish node1 acknowledged, before, during and after
// the outage, reaches node2 exactly once and in log order once the bus
// is back: late is allowed, loss is not.
func TestNATSBusServerKillLosesNoCommittedMessage(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, time.Second)

	c := pgtest.Start(t)
	srv := natstest.StartRestartable(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, srv.URL())
	s2 := openNATSNode(t, dsn, srv.URL())
	a1 := &cmRecorder{}
	const channels = 4
	var chs []storage.ChannelStore
	var recs []*cmRecorder
	for i := range channels {
		name := fmt.Sprintf("room-%d", i)
		chs = append(chs, bindChannel(t, s1, name, a1))
		r := &cmRecorder{}
		recs = append(recs, r)
		bindChannel(t, s2, name, r)
	}

	var (
		mu      sync.Mutex
		acked   = make([][]string, channels)
		stop    = make(chan struct{})
		pubDone = make(chan struct{})
	)
	go func() {
		defer close(pubDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ch := i % channels
			cm, _, err := chs[ch].Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m-%d", i)}})
			if err != nil {
				t.Errorf("Store during the outage: %v", err)
				return
			}
			mu.Lock()
			acked[ch] = append(acked[ch], cm.ChannelSerial)
			mu.Unlock()
			time.Sleep(3 * time.Millisecond)
		}
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, a := range acked {
			n += len(a)
		}
		return n
	}

	waitFor(t, 10*time.Second, "publishes before the kill", func() bool { return count() >= 40 })
	srv.Kill(t)
	killedAt := time.Now()
	waitFor(t, 10*time.Second, "node2 to see the bus down", func() bool { return !natsBusOf(t, s2).nc.IsConnected() })
	duringFrom := count()
	waitFor(t, 10*time.Second, "publishes during the outage", func() bool { return count() >= duringFrom+40 })
	if err := s2.Ping(ctx); err == nil {
		t.Error("node2 ready with the NATS server down, want not ready")
	}

	srv.Replace(t)
	waitFor(t, 30*time.Second, "both nodes to reconnect", func() bool {
		return natsBusOf(t, s1).nc.IsConnected() && natsBusOf(t, s2).nc.IsConnected()
	})
	reconnected := time.Since(killedAt)
	afterFrom := count()
	waitFor(t, 10*time.Second, "publishes after the reconnect", func() bool { return count() >= afterFrom+40 })
	close(stop)
	<-pubDone

	mu.Lock()
	want := make([][]string, channels)
	for i := range acked {
		want[i] = append([]string(nil), acked[i]...)
	}
	mu.Unlock()
	total := 0
	for i := range channels {
		recs[i].waitFor(t, len(want[i]), 30*time.Second)
		total += len(want[i])
	}
	a1.waitFor(t, total, 30*time.Second)
	time.Sleep(500 * time.Millisecond) // room for a (wrong) duplicate
	for i := range channels {
		assertSerials(t, fmt.Sprintf("node2 room-%d", i), recs[i].serials(), want[i])
	}
	if got := a1.count(); got != total {
		t.Errorf("node1 delivered %d cms, want %d (publisher fast path, exactly once)", got, total)
	}
	if err := s2.Ping(ctx); err != nil {
		t.Errorf("node2 not ready after the reconnect: %v", err)
	}
	st := s2.BusStats()
	t.Logf("%d acknowledged publishes (%d during the outage); bus back %s after the kill; node2 inline=%d filled=%d gapFills=%d reconciles=%d sweepCatchUps=%d",
		total, afterFrom-duringFrom, reconnected.Round(time.Millisecond), st.Inline, st.Filled, st.GapFills, st.ReconcileRuns, st.SweepCatchUps)
	if st.ReconcileRuns == 0 {
		t.Error("node2 never reconciled after the reconnect")
	}
}
