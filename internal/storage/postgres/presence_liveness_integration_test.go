//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// leaseModes are the presence lease modes every liveness test runs in
// (DESIGN.md §12.5).
var leaseModes = []string{PresenceLeaseNode, PresenceLeaseMember}

// forEachLeaseMode runs f once per presence lease mode.
func forEachLeaseMode(t *testing.T, f func(t *testing.T, mode string)) {
	for _, mode := range leaseModes {
		t.Run(mode, func(t *testing.T) { f(t, mode) })
	}
}

// crash stops a Storage as a crashed process would stop: its lease-bump
// loop ends and it never releases its lease.
func crash(t *testing.T, s *Storage) {
	t.Helper()
	if err := s.close(false); err != nil {
		t.Fatalf("crash: %v", err)
	}
}

// TestCrashedNodePresenceReaped enters a presence member on node A,
// kills node A without teardown (so it stops bumping its lease), and
// verifies node B reaps the orphan within a lease window: the member
// disappears from Members and a synthetic LEAVE reaches node B's
// appender exactly once. In both lease modes.
func TestCrashedNodePresenceReaped(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) { testCrashedNodePresenceReaped(t, Batching{}, mode) })
}

// TestCrashedNodePresenceReapedBatched is TestCrashedNodePresenceReaped
// with presence writes batched (DESIGN.md §12.5): the ENTER and the
// reaper's synthesised LEAVE both commit through the publish lanes.
func TestCrashedNodePresenceReapedBatched(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) { testCrashedNodePresenceReaped(t, Batching{Lanes: 4}, mode) })
}

func testCrashedNodePresenceReaped(t *testing.T, batching Batching, mode string) {
	// Shrink the lease/bump/reaper cadences so the test runs in seconds.
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	// Node A: the node that will "crash". Enters presence; no appender.
	sa, err := Open(ctx, Options{DSN: dsn, Batching: batching, PresenceLeaseMode: mode})
	if err != nil {
		t.Fatalf("Open node A: %v", err)
	}
	// Close (idempotent) is deferred so it also runs on a failure path;
	// registered after the timing-restore defer so both nodes' goroutines
	// stop before the shared timing vars are restored (else the restore
	// write races their reads).
	defer func() { _ = sa.Close() }()
	chA, err := sa.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node A: %v", err)
	}

	// Node B: the surviving node. Observes presence via its appender.
	sb, err := Open(ctx, Options{DSN: dsn, Batching: batching, PresenceLeaseMode: mode})
	if err != nil {
		t.Fatalf("Open node B: %v", err)
	}
	defer func() { _ = sb.Close() }()
	recB := &presenceRecorder{}
	chB, err := sb.Channel(ctx, "room", recB)
	if err != nil {
		t.Fatalf("Channel node B: %v", err)
	}

	// Member enters on node A.
	if _, _, err := chA.StorePresence(ctx, []*protocol.PresenceMessage{{
		Action:       protocol.PresenceEnter,
		ClientID:     "alice",
		ConnectionID: "connA",
	}}); err != nil {
		t.Fatalf("enter presence: %v", err)
	}

	// Node B must first observe alice as a live member (the ENTER folded
	// into the table and its NOTIFY reached B).
	waitFor(t, 10*time.Second, "node B to see alice enter", func() bool {
		return memberPresent(t, ctx, chB, "alice")
	})

	// Crash node A: its lease-bump loop stops, so its lease lapses.
	crash(t, sa)

	// Within a lease window, node B's reaper deletes the orphan and emits
	// a synthetic LEAVE.
	waitFor(t, 10*time.Second, "alice to be reaped from Members", func() bool {
		return !memberPresent(t, ctx, chB, "alice")
	})
	waitFor(t, 10*time.Second, "synthetic LEAVE to reach node B", func() bool {
		return recB.leaveCount("alice") >= 1
	})

	// The LEAVE must be emitted exactly once (single reap, single node).
	// Give any duplicate reap a chance to (wrongly) fire, then assert.
	time.Sleep(2 * presenceReaperInterval)
	if got := recB.leaveCount("alice"); got != 1 {
		t.Fatalf("node B saw %d synthetic LEAVEs for alice, want exactly 1", got)
	}
}

// TestStaticFixturePresenceSurvivesReaper enters a static fixture member
// (storage.WithStaticPresence) on node A, then crashes node A so its
// lease-bump loop stops. A normal member would lapse and be reaped, but a
// fixture member carries an 'infinity' lease and a sentinel owner, so
// node B's reaper never removes it. In both lease modes.
func TestStaticFixturePresenceSurvivesReaper(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) { testStaticFixturePresenceSurvivesReaper(t, Batching{}, mode) })
}

// TestStaticFixturePresenceSurvivesReaperBatched is
// TestStaticFixturePresenceSurvivesReaper with presence writes batched:
// the batch's upsert must stamp the fixture's sentinel owner and
// 'infinity' lease as the unbatched one does.
func TestStaticFixturePresenceSurvivesReaperBatched(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) { testStaticFixturePresenceSurvivesReaper(t, Batching{Lanes: 4}, mode) })
}

func testStaticFixturePresenceSurvivesReaper(t *testing.T, batching Batching, mode string) {
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	sa, err := Open(ctx, Options{DSN: dsn, Batching: batching, PresenceLeaseMode: mode})
	if err != nil {
		t.Fatalf("Open node A: %v", err)
	}
	defer func() { _ = sa.Close() }()
	chA, err := sa.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node A: %v", err)
	}

	sb, err := Open(ctx, Options{DSN: dsn, Batching: batching, PresenceLeaseMode: mode})
	if err != nil {
		t.Fatalf("Open node B: %v", err)
	}
	defer func() { _ = sb.Close() }()
	chB, err := sb.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node B: %v", err)
	}

	// Seed a static fixture member on node A.
	if _, _, err := chA.StorePresence(storage.WithStaticPresence(ctx), []*protocol.PresenceMessage{{
		Action:       protocol.PresenceEnter,
		ClientID:     "fixture_client",
		ConnectionID: "connFixture",
		Data:         "true",
	}}); err != nil {
		t.Fatalf("seed fixture presence: %v", err)
	}

	waitFor(t, 10*time.Second, "node B to see the fixture member", func() bool {
		return memberPresent(t, ctx, chB, "fixture_client")
	})

	// Node A "crashes": its bump loop stops. A normal member would lapse
	// within a lease window; wait several reaper cycles and assert the
	// fixture member is still present on node B.
	crash(t, sa)
	time.Sleep(presenceLeaseWindow + 10*presenceReaperInterval)
	if !memberPresent(t, ctx, chB, "fixture_client") {
		t.Fatal("static fixture member was reaped, want it to survive indefinitely")
	}
}

// swapPresenceTimings overrides the presence lease/bump/reaper vars and
// returns a restore func.
func swapPresenceTimings(lease, bump, reap time.Duration) func() {
	ol, ob, or := presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval
	presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval = lease, bump, reap
	return func() {
		presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval = ol, ob, or
	}
}

func memberPresent(t *testing.T, ctx context.Context, ch storage.ChannelStore, clientID string) bool {
	t.Helper()
	members, _, err := ch.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	for _, m := range members {
		if m.ClientID == clientID {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// presenceRecorder is a storage.Appender that records delivered presence
// cms so tests can count LEAVEs per client.
type presenceRecorder struct {
	mu       sync.Mutex
	presence []*protocol.PresenceMessage
}

func (r *presenceRecorder) Initialize(current, initial string) {}

func (r *presenceRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.presence = append(r.presence, cm.Presence...)
	r.mu.Unlock()
}

func (r *presenceRecorder) leaveCount(clientID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.presence {
		if p.Action == protocol.PresenceLeave && p.ClientID == clientID {
			n++
		}
	}
	return n
}
