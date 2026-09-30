//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
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

// cmRecorder is a storage.Appender that keeps every delivered cm.
type cmRecorder struct {
	mu  sync.Mutex
	cms []*protocol.ChannelMessage
}

func (r *cmRecorder) Initialize(current, initial string) {}

func (r *cmRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.cms = append(r.cms, cm)
	r.mu.Unlock()
}

func (r *cmRecorder) all() []*protocol.ChannelMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*protocol.ChannelMessage(nil), r.cms...)
}

func (r *cmRecorder) waitFor(t *testing.T, n int, timeout time.Duration) []*protocol.ChannelMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := r.all(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d cms; have %d", n, len(r.all()))
	return nil
}

// TestBusInlineAndPointerPayloadsDeliverOnce is test (b): a cm whose
// NOTIFY fits under the inline limit is delivered from the payload with
// no read-back, a cm above it goes by pointer and is read back, and each
// reaches the receiving node's appender exactly once with the content
// that was stored.
func TestBusInlineAndPointerPayloadsDeliverOnce(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	b := openNode(t, dsn)

	pub, err := a.Channel(ctx, "room", nil) // A publishes without holding the channel
	if err != nil {
		t.Fatalf("A opens room: %v", err)
	}
	rec := &cmRecorder{}
	if _, err := b.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("B binds room: %v", err)
	}

	small := "small payload"
	big := strings.Repeat("x", 2*inlinePayloadLimit) // row payload alone is over the limit
	smallCM, _, err := pub.Store(ctx, []*protocol.Message{{Name: "s", Data: small}})
	if err != nil {
		t.Fatalf("Store small: %v", err)
	}
	bigCM, _, err := pub.Store(ctx, []*protocol.Message{{Name: "b", Data: big}})
	if err != nil {
		t.Fatalf("Store big: %v", err)
	}

	got := rec.waitFor(t, 2, 10*time.Second)
	time.Sleep(300 * time.Millisecond) // room for a (wrong) second delivery
	got = rec.all()
	if len(got) != 2 {
		t.Fatalf("B's appender saw %d cms, want exactly 2", len(got))
	}
	if got[0].ChannelSerial != smallCM.ChannelSerial || got[1].ChannelSerial != bigCM.ChannelSerial {
		t.Fatalf("B's appender serials = [%s %s], want [%s %s]", got[0].ChannelSerial, got[1].ChannelSerial, smallCM.ChannelSerial, bigCM.ChannelSerial)
	}
	if d, _ := got[0].Messages[0].Data.(string); d != small || got[0].Messages[0].Serial != smallCM.Messages[0].Serial {
		t.Fatalf("inline delivery = %+v, want data %q serial %s", got[0].Messages[0], small, smallCM.Messages[0].Serial)
	}
	if d, _ := got[1].Messages[0].Data.(string); d != big || got[1].Messages[0].Serial != bigCM.Messages[0].Serial {
		t.Fatalf("pointer delivery lost its data or serial (serial %s)", got[1].Messages[0].Serial)
	}

	st := b.BusStats()
	if st.Inline != 1 || st.Fetched != 1 {
		t.Fatalf("B delivery paths inline=%d fetched=%d, want 1 and 1", st.Inline, st.Fetched)
	}
	if st.Notifications != 2 {
		t.Fatalf("B received %d notifications, want 2", st.Notifications)
	}
}

// slowAppender records serials like recorder but sleeps in every Append,
// standing in for a slow subscriber-side append.
type slowAppender struct {
	recorder
	delay time.Duration
}

func (a *slowAppender) Append(cm *protocol.ChannelMessage) {
	time.Sleep(a.delay)
	a.recorder.Append(cm)
}

// TestBusSlowChannelDoesNotDelayOthers is test (c): node B holds two
// channels, one with an artificially slow appender. A backlog on the slow
// channel must not delay the other channel's deliveries, because each
// bound channel is drained by its own ordered worker. With one consume
// loop per node (the previous design), the fast channel's cm waited
// behind the whole slow backlog.
func TestBusSlowChannelDoesNotDelayOthers(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	b := openNode(t, dsn)

	const backlog = 6
	const delay = 250 * time.Millisecond
	slow := &slowAppender{delay: delay}
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

	var want []string
	for i := range backlog {
		want = append(want, publish(t, ctx, slowPub, fmt.Sprintf("slow-%d", i)))
	}
	published := time.Now()
	fastSerial := publish(t, ctx, fastPub, "fast-0")

	waitForCount(t, fast, 1, 10*time.Second)
	latency := time.Since(published)
	slowDone := slow.count()
	if latency > time.Duration(backlog)*delay/2 {
		t.Fatalf("fast channel delivery took %s behind a %s slow backlog: channels are not consumed in parallel", latency, time.Duration(backlog)*delay)
	}
	if slowDone >= backlog {
		t.Fatalf("slow channel had already delivered all %d cms when fast delivered; the test did not overlap them", backlog)
	}
	if got := fast.serials(); len(got) != 1 || got[0] != fastSerial {
		t.Fatalf("fast appender = %v, want [%s]", got, fastSerial)
	}

	// The slow channel still gets its whole backlog, in order, once each.
	waitForCount(t, &slow.recorder, backlog, 10*time.Second)
	got := slow.serials()
	if len(got) != backlog {
		t.Fatalf("slow appender saw %d cms, want %d", len(got), backlog)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slow cm[%d] = %s, want %s (order broken)\n got=%v\nwant=%v", i, got[i], want[i], got, want)
		}
	}
	t.Logf("fast channel delivered in %s while the slow channel had delivered %d of %d", latency, slowDone, backlog)
}
