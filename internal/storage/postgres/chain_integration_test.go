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
