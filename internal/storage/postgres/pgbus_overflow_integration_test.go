//go:build integration

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestCoalescedOverflowFallsBackToSweep exercises the coalesced send-side
// overflow policy (DESIGN.md §7.2): node A may hold only two channels
// pending a wake-up, and writes one message to each of five channels in
// one window. Two channels are announced; the other three overflow and
// are counted, and node B still delivers all five, the overflowed ones
// through its sweep.
func TestCoalescedOverflowFallsBackToSweep(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	openOpts := func(o Options) *Storage {
		s, err := Open(ctx, o)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a := openOpts(Options{DSN: dsn, Bus: BusPostgres, NotifyMode: NotifyCoalesced, NotifyWindow: 2 * time.Second, NotifyMaxPending: 2})
	b := openOpts(Options{DSN: dsn, Bus: BusPostgres, NotifyMode: NotifyCoalesced, SweepInterval: 200 * time.Millisecond})

	const channels = 5
	recs := make([]*recorder, channels)
	pubs := make([]storage.ChannelStore, channels)
	for i := range channels {
		name := fmt.Sprintf("ch-%d", i)
		recs[i] = &recorder{}
		if _, err := b.Channel(ctx, name, recs[i]); err != nil {
			t.Fatalf("B binds %s: %v", name, err)
		}
		var err error
		if pubs[i], err = a.Channel(ctx, name, nil); err != nil {
			t.Fatalf("A opens %s: %v", name, err)
		}
	}

	want := make([]string, channels)
	for i := range channels {
		want[i] = publish(t, ctx, pubs[i], fmt.Sprintf("m-%d", i))
	}
	if got := a.BusStats().Overflow; got != channels-2 {
		t.Fatalf("A overflow = %d, want %d (five channels, cap two)", got, channels-2)
	}
	for i := range channels {
		waitForCount(t, recs[i], 1, 10*time.Second)
		if got := recs[i].serials(); len(got) != 1 || got[0] != want[i] {
			t.Fatalf("B ch-%d = %v, want [%s]", i, got, want[i])
		}
	}
	waitFor(t, 5*time.Second, "A's window to flush its two wake-ups", func() bool {
		return a.BusStats().WakeupsSent == 2
	})
	st := b.BusStats()
	if st.SweepCatchUps < channels-2 {
		t.Errorf("B sweepCatchUps = %d, want at least %d (the overflowed channels)", st.SweepCatchUps, channels-2)
	}
	t.Logf("A overflow=%d wakeupsSent=%d; B wakeups=%d sweepCatchUps=%d filled=%d", a.BusStats().Overflow, a.BusStats().WakeupsSent, st.WakeupsReceived, st.SweepCatchUps, st.Filled)
}

// TestPGBusQueueOverflowCatchesUpFromLog exercises the receive-side
// overflow policy (DESIGN.md §7.2): with a four-item channel queue and a
// slow appender, a burst overflows the queue. Dropped notifications are
// counted, the LISTEN goroutine never blocks, and the channel's worker
// catches up from the log, so every cm still arrives exactly once and in
// order.
func TestPGBusQueueOverflowCatchesUpFromLog(t *testing.T) {
	orig := channelQueueDepth
	channelQueueDepth = 4
	t.Cleanup(func() { channelQueueDepth = orig })

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	b := openNode(t, dsn)
	slow := &slowAppender{delay: 20 * time.Millisecond}
	if _, err := b.Channel(ctx, "slow", slow); err != nil {
		t.Fatalf("B binds slow: %v", err)
	}
	fast := &recorder{}
	if _, err := b.Channel(ctx, "fast", fast); err != nil {
		t.Fatalf("B binds fast: %v", err)
	}
	slowPub, err := a.Channel(ctx, "slow", nil)
	if err != nil {
		t.Fatalf("A opens slow: %v", err)
	}
	fastPub, err := a.Channel(ctx, "fast", nil)
	if err != nil {
		t.Fatalf("A opens fast: %v", err)
	}

	const burst = 30
	var want []string
	for i := range burst {
		want = append(want, publish(t, ctx, slowPub, fmt.Sprintf("slow-%d", i)))
	}
	fastSerial := publish(t, ctx, fastPub, "fast-0")
	waitForCount(t, fast, 1, 5*time.Second)
	if got := fast.serials(); got[0] != fastSerial {
		t.Fatalf("fast = %v, want [%s]", got, fastSerial)
	}

	waitForCount(t, &slow.recorder, burst, 20*time.Second)
	time.Sleep(300 * time.Millisecond) // room for a (wrong) duplicate
	got := slow.serials()
	if len(got) != burst {
		t.Fatalf("slow appender saw %d cms, want exactly %d", len(got), burst)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slow cm[%d] = %s, want %s (order broken)", i, got[i], want[i])
		}
	}
	st := b.BusStats()
	if st.Drops == 0 {
		t.Fatal("B dropped nothing: the burst did not overflow a four-item queue, so the test did not exercise the policy")
	}
	if st.Filled == 0 {
		t.Fatal("B delivered nothing from the log: the overflow catch-up did not run")
	}
	t.Logf("B drops=%d filled=%d inline=%d duplicates=%d", st.Drops, st.Filled, st.Inline, st.Duplicates)
}
