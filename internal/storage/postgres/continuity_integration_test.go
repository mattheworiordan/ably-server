//go:build integration

package postgres

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// The continuity tests (DESIGN.md §7.2, "Can a delivery be lost"): a
// node off the bus for longer than the retention window catches up from
// a log that no longer holds the cms it missed. It must tell its
// appender (storage.Discontinuous) once, at the point of the gap, append
// nothing from the gap, and carry on; a channel that missed nothing must
// not be told.

// continuityRecorder records appends and discontinuities in delivery
// order; a discontinuity is recorded as "|".
type continuityRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *continuityRecorder) Initialize(current, initial string) {}

func (r *continuityRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, cm.ChannelSerial)
}

func (r *continuityRecorder) Discontinuity(reason storage.DiscontinuityReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "|"+string(reason))
}

func (r *continuityRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *continuityRecorder) waitLen(t *testing.T, n int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ev := r.snapshot(); len(ev) >= n {
			return ev
		}
		time.Sleep(20 * time.Millisecond)
	}
	ev := r.snapshot()
	t.Fatalf("timed out waiting for %d events; have %v", n, ev)
	return nil
}

// ageOutLog drops every leaf the retention sweep would drop ten minutes
// from now (the live class keeps 2 minutes), then recreates the leaves
// for the real now, so later publishes have somewhere to land. Each node
// in nodes reads the database clock ten minutes later from then on, so
// its retention floor agrees with what was dropped.
func ageOutLog(t *testing.T, s *Storage, nodes ...*Storage) {
	t.Helper()
	const skew = 10 * time.Minute
	ctx := context.Background()
	for _, n := range nodes {
		n.SetClockSkew(skew)
	}
	if err := s.MaintainPartitionsAt(ctx, skew); err != nil {
		t.Fatalf("sweep at +%v: %v", skew, err)
	}
	if err := s.MaintainPartitionsAt(ctx, 0); err != nil {
		t.Fatalf("recreate the current leaves: %v", err)
	}
}

// assertContinuity checks a node's events: the pre-outage cms, one
// retention discontinuity, then the post-outage cms, and nothing from
// the gap.
func assertContinuity(t *testing.T, got, pre, gap, post []string) {
	t.Helper()
	want := slices.Concat(pre, []string{"|" + string(storage.DiscontinuityRetention)}, post)
	if !slices.Equal(got, want) {
		t.Errorf("events = %v\nwant     %v", got, want)
	}
	for _, s := range gap {
		if slices.Contains(got, s) {
			t.Errorf("cm %s from the aged-out gap was appended", s)
		}
	}
}

// TestNATSBusOutageLongerThanRetentionSignalsDiscontinuity: node B is
// cut off NATS, node A publishes, the gap ages out of the log, and B's
// connection is restored. The reconnect reconcile reads past B's mark,
// finds nothing (the cm at the mark is gone too and the channel has
// moved on), and signals one discontinuity before the cms published
// after the outage. A channel B also holds, with no publish during the
// outage, gets none, though its mark is just as far below the floor.
func TestNATSBusOutageLongerThanRetentionSignalsDiscontinuity(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, time.Hour) // reconcile only, no sweep

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	proxy := natstest.NewProxy(t, n.Addr)
	a := openNATSNode(t, dsn, n.URL)
	b := openNATSNode(t, dsn, proxy.URL())
	room := bindChannel(t, a, "room", &cmRecorder{})
	quiet := bindChannel(t, a, "quiet", &cmRecorder{})
	recRoom, recQuiet := &continuityRecorder{}, &continuityRecorder{}
	bindChannel(t, b, "room", recRoom)
	bindChannel(t, b, "quiet", recQuiet)
	busB := natsBusOf(t, b)

	var pre, gap, post []string
	for i := range 3 {
		pre = append(pre, publish(t, ctx, room, fmt.Sprintf("pre-%d", i)))
	}
	quietPre := publish(t, ctx, quiet, "quiet-pre")
	recRoom.waitLen(t, len(pre), 5*time.Second)
	recQuiet.waitLen(t, 1, 5*time.Second)

	proxy.Cut()
	waitFor(t, 5*time.Second, "node B to notice the outage", func() bool { return !busB.nc.IsConnected() })
	reconcilesBefore := b.BusStats().ReconcileRuns
	for i := range 3 {
		gap = append(gap, publish(t, ctx, room, fmt.Sprintf("gap-%d", i)))
	}
	ageOutLog(t, a, a, b)

	proxy.Restore()
	waitFor(t, 10*time.Second, "node B to reconcile", func() bool { return b.BusStats().ReconcileRuns > reconcilesBefore })
	recRoom.waitLen(t, len(pre)+1, 5*time.Second)

	for i := range 3 {
		post = append(post, publish(t, ctx, room, fmt.Sprintf("post-%d", i)))
	}
	quietPost := publish(t, ctx, quiet, "quiet-post")
	recRoom.waitLen(t, len(pre)+1+len(post), 5*time.Second)
	recQuiet.waitLen(t, 2, 5*time.Second)

	// A later reconcile, its marks still below the skewed floor, finds the
	// cms at its marks held and signals nothing more.
	reconcilesBefore = b.BusStats().ReconcileRuns
	b.requestReconcile()
	waitFor(t, 10*time.Second, "a second reconcile", func() bool { return b.BusStats().ReconcileRuns > reconcilesBefore })
	time.Sleep(200 * time.Millisecond)

	assertContinuity(t, recRoom.snapshot(), pre, gap, post)
	if got, want := recQuiet.snapshot(), []string{quietPre, quietPost}; !slices.Equal(got, want) {
		t.Errorf("quiet channel events = %v, want %v and no discontinuity", got, want)
	}
}

// TestNATSBusSweepPastRetentionSignalsDiscontinuity: the watermark sweep
// catches up from the same mark as the reconcile, so a lost tail that
// has aged out by the time the sweep reads it is a discontinuity too.
// Here B stays connected but never hears of the cms (its subscription is
// dropped), so only the sweep can find the gap; the test runs B's sweeps
// itself.
func TestNATSBusSweepPastRetentionSignalsDiscontinuity(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNATSNode(t, dsn, n.URL)
	b := openNATSNode(t, dsn, n.URL)
	room := bindChannel(t, a, "room", &cmRecorder{})
	rec := &continuityRecorder{}
	bindChannel(t, b, "room", rec)
	sweep := func() {
		t.Helper()
		if err := b.sweepWatermarks(ctx); err != nil {
			t.Fatalf("sweep: %v", err)
		}
	}

	pre := []string{publish(t, ctx, room, "pre")}
	rec.waitLen(t, 1, 5*time.Second)
	sweep() // caught up: the mark is proven as of now

	cs := b.boundStore("room")
	cs.subMu.Lock()
	sub := cs.sub
	cs.subMu.Unlock()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	gap := []string{publish(t, ctx, room, "gap-0"), publish(t, ctx, room, "gap-1")}
	sweep() // records the watermark B is behind
	ageOutLog(t, a, a, b)
	sweep() // catches up past the floor: the gap is gone
	sweep()
	sweep() // and nothing more is signalled

	assertContinuity(t, rec.snapshot(), pre, gap, nil)
}

// TestPGNotifyListenOutageLongerThanRetentionSignalsDiscontinuity: the
// pgnotify bus has no sweep and reconciles from history
// after a LISTEN reconnect. With the LISTEN connection down for longer
// than retention, the reconcile signals one discontinuity, so a node's
// local presence member set (§12.4) is
// re-seeded rather than served stale. A quiet channel is not told.
func TestPGNotifyListenOutageLongerThanRetentionSignalsDiscontinuity(t *testing.T) {
	// A long first backoff holds the LISTEN connection down while the gap
	// is published and aged out.
	defer swapReconnectDelays(3*time.Second, 3*time.Second)()
	c := pgtest.Start(t)
	appName := fmt.Sprintf("continuity_%d", time.Now().UnixNano())
	base := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openPGNotifyNode(t, base)
	room, err := a.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatal(err)
	}
	quiet, err := a.Channel(ctx, "quiet", nil)
	if err != nil {
		t.Fatal(err)
	}
	b := openPGNotifyNode(t, withApplicationName(t, base, appName))
	recRoom, recQuiet := &continuityRecorder{}, &continuityRecorder{}
	if _, err := b.Channel(ctx, "room", recRoom); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel(ctx, "quiet", recQuiet); err != nil {
		t.Fatal(err)
	}

	var pre, gap, post []string
	for i := range 3 {
		pre = append(pre, publish(t, ctx, room, fmt.Sprintf("pre-%d", i)))
	}
	quietPre := publish(t, ctx, quiet, "quiet-pre")
	recRoom.waitLen(t, len(pre), 5*time.Second)
	recQuiet.waitLen(t, 1, 5*time.Second)

	reconcilesBefore := b.BusStats().ReconcileRuns
	terminateListenBackend(t, c.BaseDSN(), appName)
	for i := range 3 {
		gap = append(gap, publish(t, ctx, room, fmt.Sprintf("gap-%d", i)))
	}
	ageOutLog(t, a, a, b)
	if b.BusStats().ReconcileRuns != reconcilesBefore {
		t.Fatal("node B reconnected before the gap aged out; the test cannot tell")
	}

	waitFor(t, 15*time.Second, "node B to reconcile", func() bool { return b.BusStats().ReconcileRuns > reconcilesBefore })
	recRoom.waitLen(t, len(pre)+1, 5*time.Second)
	for i := range 3 {
		post = append(post, publish(t, ctx, room, fmt.Sprintf("post-%d", i)))
	}
	quietPost := publish(t, ctx, quiet, "quiet-post")
	recRoom.waitLen(t, len(pre)+1+len(post), 5*time.Second)
	recQuiet.waitLen(t, 2, 5*time.Second)
	time.Sleep(200 * time.Millisecond)

	assertContinuity(t, recRoom.snapshot(), pre, gap, post)
	if got, want := recQuiet.snapshot(), []string{quietPre, quietPost}; !slices.Equal(got, want) {
		t.Errorf("quiet channel events = %v, want %v and no discontinuity", got, want)
	}
}
