//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
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

// TestBusPublisherReceivesOwnPublishOnce is test (d): the publishing
// node's own appender receives each cm exactly once even though it is
// delivered twice over: by the fast path straight after commit, and by
// the NOTIFY that follows, which the high-water mark must drop.
func TestBusPublisherReceivesOwnPublishOnce(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	rec := &recorder{}
	ch, err := a.Channel(ctx, "room", rec)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	const n = 20
	var want []string
	for i := range n {
		want = append(want, publish(t, ctx, ch, fmt.Sprintf("m-%d", i)))
		// The fast path appends before Store returns.
		if got := rec.count(); got != i+1 {
			t.Fatalf("after publish %d the appender has %d cms, want %d (fast path did not deliver)", i, got, i+1)
		}
	}

	// Every NOTIFY comes back and must be dropped as a duplicate.
	waitFor(t, 10*time.Second, "the node's own NOTIFYs to arrive and be dropped", func() bool {
		st := a.BusStats()
		return st.Notifications == n && st.Duplicates == n
	})
	time.Sleep(200 * time.Millisecond)
	got := rec.serials()
	if len(got) != n {
		t.Fatalf("appender saw %d cms, want exactly %d", len(got), n)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("cm[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	if st := a.BusStats(); st.FastPath != n || st.Inline != 0 || st.Fetched != 0 {
		t.Fatalf("delivery paths fastPath=%d inline=%d fetched=%d, want %d, 0, 0", st.FastPath, st.Inline, st.Fetched, n)
	}
}

// TestBusConcurrentPublishersKeepOrderOnEveryNode publishes on one
// channel from two nodes at once. Each node's fast path must step aside
// whenever the other node's earlier cm has not reached it yet, so both
// appenders still see every cm exactly once in the canonical (storage)
// order.
func TestBusConcurrentPublishersKeepOrderOnEveryNode(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	a := openNode(t, dsn)
	b := openNode(t, dsn)
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
	if len(canonical) != 2*perNode {
		t.Fatalf("history has %d cms, want %d", len(canonical), 2*perNode)
	}

	waitForCount(t, recA, 2*perNode, 15*time.Second)
	waitForCount(t, recB, 2*perNode, 15*time.Second)
	time.Sleep(300 * time.Millisecond) // room for a (wrong) duplicate
	for name, rec := range map[string]*recorder{"A": recA, "B": recB} {
		got := rec.serials()
		if len(got) != len(canonical) {
			t.Fatalf("node %s appender saw %d cms, want exactly %d", name, len(got), len(canonical))
		}
		for i := range got {
			if got[i] != canonical[i] {
				t.Fatalf("node %s cm[%d] = %s, want %s: order or de-dup broken", name, i, got[i], canonical[i])
			}
		}
	}
	sa, sb := a.BusStats(), b.BusStats()
	t.Logf("node A: fastPath=%d inline=%d fetched=%d gapFills=%d dup=%d; node B: fastPath=%d inline=%d fetched=%d gapFills=%d dup=%d",
		sa.FastPath, sa.Inline, sa.Fetched, sa.GapFills, sa.Duplicates, sb.FastPath, sb.Inline, sb.Fetched, sb.GapFills, sb.Duplicates)
}

// TestBusReconcileAfterDroppedListenPerChannel is test (e): node B holds
// two channels and its LISTEN connection is killed. Node A publishes on
// both while B is disconnected (those NOTIFYs are lost) and after it is
// back. Each of B's channels must still receive every cm exactly once, in
// order, the in-gap ones through its own reconcile.
func TestBusReconcileAfterDroppedListenPerChannel(t *testing.T) {
	// A long first backoff keeps B disconnected while the gap publishes run.
	defer swapReconnectDelays(750*time.Millisecond, time.Second)()

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	appName := fmt.Sprintf("bus_reconcile_%d", time.Now().UnixNano())
	ctx := context.Background()

	a, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer func() { _ = a.Close() }()
	b, err := Open(ctx, Options{DSN: withApplicationName(t, dsn, appName)})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer func() { _ = b.Close() }()

	names := []string{"one", "two"}
	recs := map[string]*recorder{}
	pubs := map[string]storage.ChannelStore{}
	want := map[string][]string{}
	for _, name := range names {
		recs[name] = &recorder{}
		if _, err := b.Channel(ctx, name, recs[name]); err != nil {
			t.Fatalf("B binds %s: %v", name, err)
		}
		if pubs[name], err = a.Channel(ctx, name, nil); err != nil {
			t.Fatalf("A opens %s: %v", name, err)
		}
	}
	publishAll := func(phase string, n int) {
		for i := range n {
			for _, name := range names {
				want[name] = append(want[name], publish(t, ctx, pubs[name], fmt.Sprintf("%s-%s-%d", name, phase, i)))
			}
		}
	}

	publishAll("pre", 3)
	for _, name := range names {
		waitForCount(t, recs[name], len(want[name]), 10*time.Second)
	}
	before := b.BusStats()

	terminateListenBackend(t, c.BaseDSN(), appName)
	publishAll("gap", 5) // B is not listening: these NOTIFYs are lost
	publishAll("post", 3)

	for _, name := range names {
		waitForCount(t, recs[name], len(want[name]), 15*time.Second)
	}
	time.Sleep(300 * time.Millisecond) // room for a (wrong) duplicate
	for _, name := range names {
		got := recs[name].serials()
		if len(got) != len(want[name]) {
			t.Fatalf("channel %s: appender saw %d cms, want exactly %d", name, len(got), len(want[name]))
		}
		for i := range got {
			if got[i] != want[name][i] {
				t.Fatalf("channel %s cm[%d] = %s, want %s: order or de-dup broken\n got=%v\nwant=%v", name, i, got[i], want[name][i], got, want[name])
			}
		}
	}
	after := b.BusStats()
	if after.Reconciles-before.Reconciles < uint64(len(names)) {
		t.Fatalf("B ran %d reconciles, want at least one per bound channel (%d)", after.Reconciles-before.Reconciles, len(names))
	}
	if after.Filled-before.Filled == 0 {
		t.Fatal("B delivered nothing from its reconcile: the in-gap cms were not recovered from the log")
	}
	t.Logf("B after reconnect: reconciles=%d filled=%d duplicates=%d", after.Reconciles-before.Reconciles, after.Filled-before.Filled, after.Duplicates-before.Duplicates)
}

// TestBusListenConnAcceptsPoolDSNSettings opens a Storage whose DSN
// carries a pgxpool setting (pool_max_conns). The LISTEN connection must
// strip it like the pool does, instead of sending it to the server as an
// unknown runtime parameter (which fails the connection), and the bus
// must still deliver.
func TestBusListenConnAcceptsPoolDSNSettings(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	q := u.Query()
	q.Set("pool_max_conns", "7")
	u.RawQuery = q.Encode()
	ctx := context.Background()

	a := openNode(t, u.String())
	rec := &recorder{}
	if _, err := a.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("Channel: %v", err)
	}
	b := openNode(t, u.String())
	pub, err := b.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	want := publish(t, ctx, pub, "hello")
	waitForCount(t, rec, 1, 10*time.Second)
	if got := rec.serials(); got[0] != want {
		t.Fatalf("got %s, want %s", got[0], want)
	}
}
