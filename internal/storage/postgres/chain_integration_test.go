//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestChainPagesGapFillsAndCatchUps shrinks the log page to three cms and
// checks that the shared delivery point (DESIGN.md §7.2) pages through
// a long gap: a gap fill revealed by a predecessor (node B, whose sweep
// is parked so only the gap fill can deliver), and a lost tail found by
// the sweep (node C). A node on the pgnotify bus stands in for a
// publisher whose bus messages never arrive.
func TestChainPagesGapFillsAndCatchUps(t *testing.T) {
	origPage, origGap := rangePageSize, gapFillDelay
	rangePageSize, gapFillDelay = 3, 50*time.Millisecond
	t.Cleanup(func() { rangePageSize, gapFillDelay = origPage, origGap })

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	openWithSweep := func(sweep time.Duration) *Storage {
		opts := pgBusOptions(dsn)
		opts.SweepInterval = sweep
		s, err := Open(ctx, opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a := openNode(t, dsn)
	b := openWithSweep(time.Hour)
	cNode := openWithSweep(200 * time.Millisecond)
	silent := openListenNode(t, dsn)

	recB, recC := &recorder{}, &recorder{}
	if _, err := b.Channel(ctx, "room", recB); err != nil {
		t.Fatalf("B binds room: %v", err)
	}
	if _, err := cNode.Channel(ctx, "room", recC); err != nil {
		t.Fatalf("C binds room: %v", err)
	}
	pub, err := a.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("A opens room: %v", err)
	}
	quiet, err := silent.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("silent opens room: %v", err)
	}

	// A gap of seven, revealed by the predecessor of an eighth cm.
	var want []string
	for i := range 7 {
		want = append(want, publish(t, ctx, quiet, fmt.Sprintf("gap-%d", i)))
	}
	want = append(want, publish(t, ctx, pub, "reveals"))
	waitForCount(t, recB, len(want), 10*time.Second)
	if got := b.BusStats().GapFills; got == 0 {
		t.Error("B gapFills = 0: the gap was not filled from the log")
	}

	// A lost tail of seven, found by C's sweep.
	for i := range 7 {
		want = append(want, publish(t, ctx, quiet, fmt.Sprintf("tail-%d", i)))
	}
	waitForCount(t, recC, len(want), 10*time.Second)
	time.Sleep(300 * time.Millisecond) // room for a (wrong) duplicate
	for name, rec := range map[string]*recorder{"C": recC} {
		got := rec.serials()
		if len(got) != len(want) {
			t.Fatalf("%s saw %d cms, want exactly %d", name, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s cm[%d] = %s, want %s: paging broke order or de-dup", name, i, got[i], want[i])
			}
		}
	}
	if got := recB.serials(); len(got) != 8 {
		t.Fatalf("B saw %d cms, want exactly the 8 before the tail (its sweep is parked)", len(got))
	} else {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("B cm[%d] = %s, want %s", i, got[i], want[i])
			}
		}
	}
	if got := cNode.BusStats().SweepCatchUps; got == 0 {
		t.Error("C sweepCatchUps = 0: the lost tail was not caught up by the sweep")
	}
}

// TestReleaseRacingBindLeavesAWorkingBinding runs Release against an
// in-flight Channel bind for the same name, many times, on both chaining
// buses. Whatever the interleaving, nothing deadlocks, and a clean
// re-bind afterwards is subscribed: it receives a publish from another
// node exactly once (a stray UNLISTEN or unsubscribe would lose it).
func TestReleaseRacingBindLeavesAWorkingBinding(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	for _, bus := range []string{BusPostgres, BusNATS} {
		t.Run(bus, func(t *testing.T) {
			dsn := c.FreshSchemaDSN(t)
			ctx := context.Background()
			open := func() *Storage {
				if bus == BusNATS {
					return openNATSNode(t, dsn, n.URL)
				}
				return openNode(t, dsn)
			}
			a, b := open(), open()

			for i := range 30 {
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, _ = b.Channel(ctx, "room", &recorder{})
				}()
				go func() {
					defer wg.Done()
					_ = b.Release(ctx, "room")
				}()
				wg.Wait()
				if i%2 == 0 {
					_ = b.Release(ctx, "room")
				}
			}
			if err := b.Release(ctx, "room"); err != nil {
				t.Fatalf("Release: %v", err)
			}
			final := &recorder{}
			if _, err := b.Channel(ctx, "room", final); err != nil {
				t.Fatalf("final Channel: %v", err)
			}
			pub, err := a.Channel(ctx, "room", nil)
			if err != nil {
				t.Fatalf("A opens room: %v", err)
			}
			want := publish(t, ctx, pub, "after-the-race")
			waitForCount(t, final, 1, 10*time.Second)
			time.Sleep(200 * time.Millisecond)
			if got := final.serials(); len(got) != 1 || got[0] != want {
				t.Fatalf("final binding saw %v, want [%s]", got, want)
			}
			if got := b.BusStats().BoundChannels; got != 1 {
				t.Errorf("bound channels = %d, want 1", got)
			}
		})
	}
}

// TestPGNotifyReadyzReflectsListenConnection is
// TestPGBusReadyzReflectsListenConnection on the pgnotify bus: a node
// whose one LISTEN connection is down is not ready (DESIGN.md §7.2), and
// is ready again once it has redialled.
func TestPGNotifyReadyzReflectsListenConnection(t *testing.T) {
	defer swapReconnectDelays(750*time.Millisecond, time.Second)()

	c := pgtest.Start(t)
	appName := fmt.Sprintf("pgnotify_readyz_%d", time.Now().UnixNano())
	ctx := context.Background()
	s, err := Open(ctx, Options{DSN: withApplicationName(t, c.FreshSchemaDSN(t), appName), Bus: BusPGNotify})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping while connected: %v", err)
	}
	terminateListenBackend(t, c.BaseDSN(), appName)
	waitFor(t, 5*time.Second, "Ping to report the LISTEN connection down", func() bool { return s.Ping(ctx) != nil })
	waitFor(t, 10*time.Second, "Ping to recover after the redial", func() bool { return s.Ping(ctx) == nil })
}

// TestPGBusReadyzReflectsListenConnection: on the postgres bus a node
// whose LISTEN connection is down is not ready (DESIGN.md §7.2), and is
// ready again once it has redialled.
func TestPGBusReadyzReflectsListenConnection(t *testing.T) {
	defer swapReconnectDelays(750*time.Millisecond, time.Second)()

	c := pgtest.Start(t)
	appName := fmt.Sprintf("pgbus_readyz_%d", time.Now().UnixNano())
	ctx := context.Background()
	s, err := Open(ctx, pgBusOptions(withApplicationName(t, c.FreshSchemaDSN(t), appName)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Channel(ctx, "room", &recorder{}); err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping while connected: %v", err)
	}
	terminateListenBackend(t, c.BaseDSN(), appName)
	waitFor(t, 5*time.Second, "Ping to report the LISTEN connection down", func() bool { return s.Ping(ctx) != nil })
	waitFor(t, 10*time.Second, "Ping to recover after the redial", func() bool { return s.Ping(ctx) == nil })
}

// TestCatchUpDeliversEveryCmAfterAnOldMark: a catch-up from a mark older
// than the retention floor must deliver every cm after the mark that the
// log still holds, including cms that are themselves older than the
// floor (the leaves holding them have not been dropped yet). With the
// cm at the mark still in the log the read is proven continuous, so no
// discontinuity is signalled (DESIGN.md §6.3, §7.2). A read bounded
// below by the floor would have skipped the surviving cm silently.
func TestCatchUpDeliversEveryCmAfterAnOldMark(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	opts := pgBusOptions(dsn)
	opts.SweepInterval = time.Hour
	opts.Retention = Retention{Message: 2 * time.Minute}
	b, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	silent := openListenNode(t, dsn) // its bus messages never reach B

	rec := &discontinuityRecorder{}
	own, err := b.Channel(ctx, "room", rec)
	if err != nil {
		t.Fatalf("B binds room: %v", err)
	}
	quiet, err := silent.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("silent opens room: %v", err)
	}
	cs := b.boundStore("room")
	if cs == nil {
		t.Fatal("room not bound on B")
	}
	// The mark is a real cm (B's own publish, delivered by the fast path),
	// so the anchor check can find it; a mark with no cm behind it (a bind
	// watermark) is unprovable by design.
	s1 := publish(t, ctx, own, "seen")
	waitFor(t, 5*time.Second, "B's own publish to be appended", func() bool { return cs.watermark() == s1 })
	mark := cs.watermark()
	s2 := publish(t, ctx, quiet, "missed")

	// Age everything: the floor moves past both the mark and s2, but no
	// partition is dropped, so both cms are still in the log.
	b.SetClockSkew(5 * time.Minute)
	if cs.hwmMu.Lock(); !cs.mustProveLocked(mark) {
		cs.hwmMu.Unlock()
		t.Fatalf("mark %s should be below the floor after the skew", mark)
	} else {
		cs.hwmMu.Unlock()
	}
	if err := cs.catchUp(ctx); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if got := rec.serials; len(got) != 2 || got[0] != s1 || got[1] != s2 {
		t.Fatalf("delivered %v, want %s then the missed cm %s", got, s1, s2)
	}
	if rec.discontinuities != 0 {
		t.Fatalf("discontinuities = %d, want 0: the cm at the mark %s is still in the log", rec.discontinuities, mark)
	}
}

// TestCatchUpAfterTheChannelsRowWasPrunedSignals: a channel whose row was
// pruned after idling past retention has no current serial; a catch-up
// from an old mark cannot prove what happened after it and must signal a
// discontinuity rather than report nothing missed (DESIGN.md §7.2).
func TestCatchUpAfterTheChannelsRowWasPrunedSignals(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	opts := pgBusOptions(dsn)
	opts.SweepInterval = time.Hour
	opts.Retention = Retention{Message: 2 * time.Minute}
	b, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	rec := &discontinuityRecorder{}
	ch, err := b.Channel(ctx, "room", rec)
	if err != nil {
		t.Fatalf("B binds room: %v", err)
	}
	publish(t, ctx, ch, "m1")
	cs := b.boundStore("room")
	mark := cs.watermark()

	// What the prune does once the row is idle past retention: the row and
	// the aged-out log leaf go; the mark's cm is gone with it.
	if _, err := b.pool.Exec(ctx, `DELETE FROM channel_messages WHERE channel = 'room'`); err != nil {
		t.Fatalf("delete log rows: %v", err)
	}
	if _, err := b.pool.Exec(ctx, `DELETE FROM channels WHERE name = 'room'`); err != nil {
		t.Fatalf("delete channels row: %v", err)
	}
	b.SetClockSkew(5 * time.Minute)
	if err := cs.catchUp(ctx); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if rec.discontinuities != 1 {
		t.Fatalf("discontinuities = %d, want 1 (mark %s, row pruned)", rec.discontinuities, mark)
	}
}

// TestFirstPublishAfterThePruneIsContinuous: a subscribed, caught-up
// channel idles past retention and its channels row is pruned while the
// nodes stay on the bus. The sweep keeps proving it (no row means nothing
// published since the prune), so the first publish afterwards, whose
// predecessor is the recreated row's fresh seed and which is therefore
// held and filled from the old mark, is delivered on every node with no
// discontinuity (DESIGN.md §6.3 "Pruning channels rows", §7.2). Before
// the sweep proved a missing row, the proof aged out with it, the fill
// was checked, found the mark's cm gone, and signalled 80016 on every
// node although nothing was lost.
func TestFirstPublishAfterThePruneIsContinuous(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	open := func() *Storage {
		opts := pgBusOptions(dsn)
		opts.SweepInterval = time.Hour // swept by hand below
		opts.Retention = Retention{Message: 2 * time.Minute}
		// With the publish lanes, as the server runs, a publish that
		// recreates the row chains on its fresh seed.
		opts.Batching = Batching{Lanes: 1}
		s, err := Open(ctx, opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	b, other := open(), open()
	recB, recO := &discontinuityRecorder{}, &discontinuityRecorder{}
	ch, err := b.Channel(ctx, "room", recB)
	if err != nil {
		t.Fatalf("B binds room: %v", err)
	}
	if _, err := other.Channel(ctx, "room", recO); err != nil {
		t.Fatalf("other binds room: %v", err)
	}
	csB, csO := b.boundStore("room"), other.boundStore("room")
	m1 := publish(t, ctx, ch, "m1")
	for _, cs := range []*channelStore{csB, csO} {
		waitFor(t, 5*time.Second, "m1 to be delivered", func() bool { return cs.watermark() == m1 })
	}
	sweep := func() {
		t.Helper()
		for _, s := range []*Storage{b, other} {
			if err := s.sweepWatermarks(ctx); err != nil {
				t.Fatalf("sweep: %v", err)
			}
		}
	}
	sweep()

	// The channel idles past retention and is pruned: its row and its
	// aged-out log go. The nodes keep sweeping meanwhile.
	if _, err := b.pool.Exec(ctx, `DELETE FROM channel_messages WHERE channel = 'room'`); err != nil {
		t.Fatalf("delete log rows: %v", err)
	}
	if _, err := b.pool.Exec(ctx, `DELETE FROM channels WHERE name = 'room'`); err != nil {
		t.Fatalf("delete channels row: %v", err)
	}
	b.SetClockSkew(5 * time.Minute)
	other.SetClockSkew(5 * time.Minute)
	sweep()

	m2 := publish(t, ctx, ch, "m2")
	for name, n := range map[string]struct {
		cs  *channelStore
		rec *discontinuityRecorder
	}{"B": {csB, recB}, "other": {csO, recO}} {
		waitFor(t, 5*time.Second, name+" to deliver m2", func() bool { return n.cs.watermark() == m2 })
		n.cs.hwmMu.Lock() // Append runs under hwmMu
		got, disc := append([]string(nil), n.rec.serials...), n.rec.discontinuities
		n.cs.hwmMu.Unlock()
		if disc != 0 || len(got) != 2 || got[0] != m1 || got[1] != m2 {
			t.Errorf("%s delivered %v with %d discontinuities, want [%s %s] and none", name, got, disc, m1, m2)
		}
	}
}
