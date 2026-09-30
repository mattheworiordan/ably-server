//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// Tests for NotifyCoalesced mode (DESIGN.md §7.2, notify_coalesced.go).

func openCoalesced(t *testing.T, dsn string, window, poll time.Duration) *Storage {
	t.Helper()
	s, err := Open(context.Background(), Options{DSN: dsn, NotifyMode: NotifyCoalesced, NotifyWindow: window, PollInterval: poll})
	if err != nil {
		t.Fatalf("Open coalesced: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestCoalescedChannelStoreContract runs the shared storage contract
// suite against a coalesced-mode Storage.
func TestCoalescedChannelStoreContract(t *testing.T) {
	c := pgtest.Start(t)
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		return openCoalesced(t, c.FreshSchemaDSN(t), 0, 0)
	})
}

// TestCoalescedDeliversWithoutPerPublishNotify publishes a burst from
// node A in coalesced mode. Node B must receive every cm exactly once in
// order, while Postgres carries far fewer notifications than writes: the
// writes committed without NOTIFY and A's notifier sent at most one
// wake-up per channel per window.
func TestCoalescedDeliversWithoutPerPublishNotify(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openCoalesced(t, dsn, 50*time.Millisecond, 0)
	b := openCoalesced(t, dsn, 50*time.Millisecond, 0)
	pub, err := a.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("A opens room: %v", err)
	}
	rec := &recorder{}
	if _, err := b.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("B binds room: %v", err)
	}
	raw := rawListener(t, dsn, pgChannelName(b.namespace, "room"))

	const n = 60
	var want []string
	for i := range n {
		want = append(want, publish(t, ctx, pub, fmt.Sprintf("m-%d", i)))
	}
	waitForCount(t, rec, n, 10*time.Second)
	time.Sleep(200 * time.Millisecond)

	got := rec.serials()
	if len(got) != n {
		t.Fatalf("B's appender saw %d cms, want exactly %d", len(got), n)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("cm[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	wire := rawNotifications(t, raw, 200*time.Millisecond)
	sent := a.BusStats().WakeupsSent
	if len(wire) == 0 || len(wire) >= n/3 || sent >= n/3 {
		t.Fatalf("%d writes produced %d notifications on the wire (%d wake-ups sent), want a few coalesced wake-ups", n, len(wire), sent)
	}
	st := b.BusStats()
	t.Logf("%d writes: %d wake-ups on the wire; B pulls=%d filled=%d", n, len(wire), st.Pulls, st.Filled)
}

// TestCoalescedConcurrentPublishersKeepOrder publishes on one channel
// from two coalesced-mode nodes at once; both appenders must match the
// storage order exactly.
func TestCoalescedConcurrentPublishersKeepOrder(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openCoalesced(t, dsn, 20*time.Millisecond, 0)
	b := openCoalesced(t, dsn, 20*time.Millisecond, 0)
	recA, recB := &recorder{}, &recorder{}
	chA, err := a.Channel(ctx, "room", recA)
	if err != nil {
		t.Fatalf("A binds room: %v", err)
	}
	chB, err := b.Channel(ctx, "room", recB)
	if err != nil {
		t.Fatalf("B binds room: %v", err)
	}

	const perNode = 150
	var wg sync.WaitGroup
	for _, ch := range []storage.ChannelStore{chA, chB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perNode {
				if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m-%d", i)}}); err != nil {
					t.Errorf("Store: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	page, err := chA.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 10 * perNode})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var canonical []string
	for _, cm := range page.ChannelMessages {
		canonical = append(canonical, cm.ChannelSerial)
	}
	waitForCount(t, recA, len(canonical), 15*time.Second)
	waitForCount(t, recB, len(canonical), 15*time.Second)
	time.Sleep(200 * time.Millisecond)
	for name, rec := range map[string]*recorder{"A": recA, "B": recB} {
		got := rec.serials()
		if len(got) != len(canonical) {
			t.Fatalf("node %s appender saw %d cms, want exactly %d", name, len(got), len(canonical))
		}
		for i := range got {
			if got[i] != canonical[i] {
				t.Fatalf("node %s cm[%d] = %s, want %s", name, i, got[i], canonical[i])
			}
		}
	}
}

// TestCoalescedPollRecoversLostWakeup gives the publishing node a window
// far longer than the test, so its wake-up never arrives in time. The
// receiver's safety-net poll must still deliver the write.
func TestCoalescedPollRecoversLostWakeup(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openCoalesced(t, dsn, time.Hour, 0)
	b := openCoalesced(t, dsn, 50*time.Millisecond, 200*time.Millisecond)
	pub, err := a.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("A opens room: %v", err)
	}
	rec := &recorder{}
	if _, err := b.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("B binds room: %v", err)
	}

	want := publish(t, ctx, pub, "no wake-up for this one")
	start := time.Now()
	waitForCount(t, rec, 1, 5*time.Second)
	if got := rec.serials(); got[0] != want {
		t.Fatalf("B got %s, want %s", got[0], want)
	}
	if st := b.BusStats(); st.PollPulls == 0 || st.Wakeups != 0 {
		t.Fatalf("B pollPulls=%d wakeups=%d, want the poll (not a wake-up) to have delivered it", st.PollPulls, st.Wakeups)
	}
	t.Logf("poll delivered the write %s after publish", time.Since(start))
}
