//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// These tests cover the cluster bus (DESIGN.md §7.2, bus.go). Like
// TestPostgresClusterBrokerDeliversCrossNode they run two or more
// Storage instances ("nodes") against one schema.

// openNode opens a Storage on dsn and closes it at test end.
func openNode(t *testing.T, dsn string) *Storage {
	t.Helper()
	s, err := Open(context.Background(), Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// rawListener LISTENs on one Postgres notification channel on its own
// connection, so a test can observe exactly what Postgres sends.
func rawListener(t *testing.T, dsn, pgChan string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("raw listener connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{pgChan}.Sanitize()); err != nil {
		t.Fatalf("raw LISTEN: %v", err)
	}
	return conn
}

// rawNotifications returns the notifications conn receives within d.
func rawNotifications(t *testing.T, conn *pgx.Conn, d time.Duration) []string {
	t.Helper()
	var got []string
	deadline := time.Now().Add(d)
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		n, err := conn.WaitForNotification(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || time.Now().After(deadline) {
				return got
			}
			t.Fatalf("raw WaitForNotification: %v", err)
		}
		got = append(got, n.Payload)
	}
}

// TestBusNodeHearsOnlyChannelsItHolds is test (a): a node that has not
// bound a channel receives no notification for it. Node B holds only
// "beta"; node A publishes on "alpha". B's LISTEN connection must receive
// nothing at all, and a raw listener on beta's Postgres channel confirms
// Postgres sent nothing there. A publish on "beta" (positive control)
// then reaches B exactly once.
func TestBusNodeHearsOnlyChannelsItHolds(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	b := openNode(t, dsn)

	recA := &recorder{}
	alphaA, err := a.Channel(ctx, "alpha", recA)
	if err != nil {
		t.Fatalf("A binds alpha: %v", err)
	}
	recB := &recorder{}
	if _, err := b.Channel(ctx, "beta", recB); err != nil {
		t.Fatalf("B binds beta: %v", err)
	}
	raw := rawListener(t, dsn, pgChannelName(b.namespace, "beta"))

	const n = 20
	for i := range n {
		publish(t, ctx, alphaA, fmt.Sprintf("alpha-%d", i))
	}
	waitForCount(t, recA, n, 10*time.Second)

	if got := rawNotifications(t, raw, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("beta's Postgres channel carried %d notifications for alpha publishes: %v", len(got), got)
	}
	if got := b.BusStats().Notifications; got != 0 {
		t.Fatalf("node B received %d notifications for a channel it does not hold, want 0", got)
	}
	if got := recB.count(); got != 0 {
		t.Fatalf("node B's beta appender saw %d cms, want 0", got)
	}

	// Positive control: a publish on beta (from A, which has not bound
	// beta) reaches B, and only B's one notification is sent.
	betaA, err := a.Channel(ctx, "beta", nil)
	if err != nil {
		t.Fatalf("A opens beta unbound: %v", err)
	}
	want := publish(t, ctx, betaA, "beta-0")
	waitForCount(t, recB, 1, 10*time.Second)
	if got := recB.serials(); len(got) != 1 || got[0] != want {
		t.Fatalf("node B's beta appender = %v, want [%s]", got, want)
	}
	if got := rawNotifications(t, raw, 300*time.Millisecond); len(got) != 1 {
		t.Fatalf("beta's Postgres channel carried %d notifications, want 1", len(got))
	}
	if got := b.BusStats().Notifications; got != 1 {
		t.Fatalf("node B received %d notifications, want exactly 1 (beta)", got)
	}
	if got := a.BusStats().Notifications; got != n {
		t.Fatalf("node A received %d notifications, want %d (its own alpha publishes only)", got, n)
	}
}

// TestBusChannelNamesAreShortIdentifiers checks the mapping from Ably
// channel names to Postgres channel names: fixed length, lower-case,
// inside the 63-byte identifier limit, distinct per name and per
// namespace.
func TestBusChannelNamesAreShortIdentifiers(t *testing.T) {
	long := make([]byte, 2000)
	for i := range long {
		long[i] = 'x'
	}
	names := []string{"", "room", "Room", "a:b@c", "ünïcødé", string(long)}
	seen := map[string]string{}
	for _, ns := range []string{"public", "test_1"} {
		for _, name := range names {
			pc := pgChannelName(ns, name)
			if len(pc) > 63 {
				t.Fatalf("pgChannelName(%q, %.20q) = %q is %d bytes, over the 63-byte limit", ns, name, pc, len(pc))
			}
			for _, r := range pc {
				if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
					t.Fatalf("pgChannelName(%q, %.20q) = %q has a character that needs quoting", ns, name, pc)
				}
			}
			key := ns + "/" + name
			if prev, dup := seen[pc]; dup {
				t.Fatalf("pgChannelName collision: %q and %q both map to %q", prev, key, pc)
			}
			seen[pc] = key
		}
	}
}
