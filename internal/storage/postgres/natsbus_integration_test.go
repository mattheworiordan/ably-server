//go:build integration

package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// The NATS bus tests mirror TestPostgresClusterBrokerDeliversCrossNode:
// two (or three) Storage instances share one Postgres schema, and, for
// the NATS bus, one NATS server (DESIGN.md §7.2).

// TestNATSBusChannelStoreContract runs the shared storage contract suite
// against the Postgres backend with the NATS bus, so the bus swap is
// shown not to change any storage semantics.
func TestNATSBusChannelStoreContract(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		return openNATSNode(t, c.FreshSchemaDSN(t), n.URL)
	})
}

// TestNATSBusDeliversCrossNodeInSerialOrder (a): publishes through node1
// reach node2's appender exactly once, in serial order, with the body
// carried inline (no read-back), and the publish transaction emits no
// NOTIFY at all.
func TestNATSBusDeliversCrossNodeInSerialOrder(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	bindChannel(t, s2, "room", a2)

	// A raw LISTEN on the LISTEN broker's channel: in NATS mode nothing
	// may be NOTIFYed, so this must stay silent.
	listen := listenForNotify(t, dsn)

	const total = 20
	var want []string
	for i := range total {
		cm, idempotent, err := ch1.Store(ctx, []*protocol.Message{{ID: fmt.Sprintf("m%d", i), Data: fmt.Sprintf("hi-%d", i)}})
		if err != nil || idempotent {
			t.Fatalf("Store %d: idempotent=%v err=%v", i, idempotent, err)
		}
		want = append(want, cm.ChannelSerial)
	}

	a2.waitFor(t, total, 5*time.Second)
	a1.waitFor(t, total, 5*time.Second)
	assertSerials(t, "node2", a2.serials(), want)
	assertSerials(t, "node1", a1.serials(), want)
	for i, cm := range a2.cms() {
		if len(cm.Messages) != 1 || cm.Messages[0].ID != fmt.Sprintf("m%d", i) || cm.Messages[0].Data != fmt.Sprintf("hi-%d", i) {
			t.Fatalf("node2 cm[%d] = %+v, want one Message m%d / hi-%d", i, cm.Messages, i, i)
		}
		if cm.Messages[0].Serial == "" || cm.Messages[0].Version == nil {
			t.Fatalf("node2 cm[%d] lost its server stamps: %+v", i, cm.Messages[0])
		}
	}
	if got := s1.BusStats().Pointers; got != 0 {
		t.Errorf("pointers published = %d, want 0 (small cms travel inline)", got)
	}

	// Exactly once: nothing further arrives.
	time.Sleep(300 * time.Millisecond)
	if got := a2.count(); got != total {
		t.Errorf("node2 appender count = %d, want %d", got, total)
	}

	nctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if note, err := listen.WaitForNotification(nctx); err == nil {
		t.Errorf("NATS bus emitted a NOTIFY on %q: %q", note.Channel, note.Payload)
	}
}

// TestNATSBusPublisherFastPathDeliversOnce (b): the publisher's own
// appender holds the cm by the time Store returns (the fast path), and
// the bus echo of every publish then arrives and is dropped, so the
// appender sees each cm exactly once.
func TestNATSBusPublisherFastPathDeliversOnce(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	a1 := &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)

	const total = 50
	var want []string
	for i := range total {
		cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("x-%d", i)}})
		if err != nil {
			t.Fatalf("Store %d: %v", i, err)
		}
		want = append(want, cm.ChannelSerial)
		if got := a1.count(); got != i+1 {
			t.Fatalf("after Store %d the publisher's appender has %d cms, want %d (fast path)", i, got, i+1)
		}
	}

	cs := boundStoreOf(t, s1, "room")
	waitFor(t, 5*time.Second, "every bus echo to arrive", func() bool {
		return storeCounters(cs).duplicates == total
	})
	counters := storeCounters(cs)
	if counters.delivered != total {
		t.Errorf("delivered = %d, want %d", counters.delivered, total)
	}
	assertSerials(t, "publisher", a1.serials(), want)
}

// TestNATSBusConcurrentPublishesStayOrdered (c): several goroutines on
// each of two nodes publish to one channel at once. Serials in the log
// stay strictly monotonic, and each node's appender receives every cm
// exactly once in log order, although NATS gives no ordering across the
// two publishing connections and each node's fast path races its bus.
func TestNATSBusConcurrentPublishesStayOrdered(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	ch2 := bindChannel(t, s2, "room", a2)

	const writersPerNode, perWriter = 4, 50
	const total = 2 * writersPerNode * perWriter
	var wg sync.WaitGroup
	for _, ch := range []storage.ChannelStore{ch1, ch2} {
		for w := range writersPerNode {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range perWriter {
					if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("w%d-%d", w, i)}}); err != nil {
						t.Errorf("Store: %v", err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()

	page, err := ch1.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 10 * total})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var logOrder []string
	var prev string
	for i, cm := range page.ChannelMessages {
		if cm.ChannelSerial <= prev {
			t.Fatalf("log[%d] serial %q not > previous %q", i, cm.ChannelSerial, prev)
		}
		prev = cm.ChannelSerial
		logOrder = append(logOrder, cm.ChannelSerial)
	}
	if len(logOrder) != total {
		t.Fatalf("log has %d cms, want %d", len(logOrder), total)
	}

	a1.waitFor(t, total, 10*time.Second)
	a2.waitFor(t, total, 10*time.Second)
	time.Sleep(300 * time.Millisecond) // anything extra would be a duplicate
	assertSerials(t, "node1", a1.serials(), logOrder)
	assertSerials(t, "node2", a2.serials(), logOrder)

	// Evidence that the reorder path ran: cms that arrived ahead of
	// their predecessor and were held, rather than dropped.
	for i, s := range []*Storage{s1, s2} {
		c := storeCounters(boundStoreOf(t, s, "room"))
		t.Logf("node%d: delivered=%d held-out-of-order=%d duplicates-dropped=%d gap-fills=%d", i+1, c.delivered, c.held, c.duplicates, c.gapFills)
	}
}

// TestNATSBusReconcilesAfterOutage (d): node2 reaches NATS through a
// proxy the test cuts. cms published through node1 during the outage
// never reach node2 on the bus; once the link is restored node2's
// reconnect reconcile replays them from the log, with no later publish
// to reveal the gap, and with no duplicates.
func TestNATSBusReconcilesAfterOutage(t *testing.T) {
	// Keep the sweep out of the way so only the reconnect reconcile can
	// deliver the gap; shrink the reconnect wait so the test is quick.
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	proxy := natstest.NewProxy(t, n.Addr)
	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, proxy.URL())
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	bindChannel(t, s2, "room", a2)
	bus2 := natsBusOf(t, s2)

	var want []string
	for i := range 5 {
		want = append(want, publish(t, ctx, ch1, fmt.Sprintf("pre-%d", i)))
	}
	a2.waitFor(t, len(want), 5*time.Second)

	proxy.Cut()
	waitFor(t, 5*time.Second, "node2 to notice the outage", func() bool {
		return !bus2.nc.IsConnected()
	})
	reconcilesBefore := s2.BusStats().ReconcileRuns

	for i := range 5 {
		want = append(want, publish(t, ctx, ch1, fmt.Sprintf("gap-%d", i)))
	}
	time.Sleep(300 * time.Millisecond)
	if got := a2.count(); got != 5 {
		t.Fatalf("node2 received %d cms during the outage, want 5 (only the pre-outage ones)", got)
	}

	proxy.Restore()
	a2.waitFor(t, len(want), 10*time.Second)
	if got := s2.BusStats().ReconcileRuns; got <= reconcilesBefore {
		t.Errorf("reconciles = %d, want > %d: the gap was not delivered by the reconnect reconcile", got, reconcilesBefore)
	}

	for i := range 5 {
		want = append(want, publish(t, ctx, ch1, fmt.Sprintf("post-%d", i)))
	}
	a2.waitFor(t, len(want), 5*time.Second)
	a1.waitFor(t, len(want), 5*time.Second)
	time.Sleep(300 * time.Millisecond)
	assertSerials(t, "node2", a2.serials(), want)
	assertSerials(t, "node1", a1.serials(), want)
}

// TestNATSBusSubscribeOnBind (e): a node receives only the channels it
// has bound. node2 binds "room" but not "other"; node1 publishes to
// both. node2 holds one subscription and its connection receives the
// "room" message and nothing for "other".
func TestNATSBusSubscribeOnBind(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	a1room, a1other, a2 := &cmRecorder{}, &cmRecorder{}, &cmRecorder{}
	room1 := bindChannel(t, s1, "room", a1room)
	other1 := bindChannel(t, s1, "other", a1other)
	bindChannel(t, s2, "room", a2)
	bus2 := natsBusOf(t, s2)

	if got := bus2.nc.NumSubscriptions(); got != 1 {
		t.Fatalf("node2 subscriptions = %d, want 1 (room only)", got)
	}
	inBefore := bus2.nc.Stats().InMsgs

	for i := range 10 {
		publish(t, ctx, other1, fmt.Sprintf("other-%d", i))
	}
	// A "room" publish after them is a barrier: once node2 has it, any
	// "other" traffic addressed to node2 would already have arrived.
	publish(t, ctx, room1, "barrier")
	a2.waitFor(t, 1, 5*time.Second)
	a1other.waitFor(t, 10, 5*time.Second)

	if got := bus2.nc.Stats().InMsgs - inBefore; got != 1 {
		t.Errorf("node2 received %d NATS messages, want 1 (the room barrier only)", got)
	}
	if boundStoreOrNil(s2, "other") != nil {
		t.Error("node2 has a bound store for a channel it never bound")
	}
	time.Sleep(200 * time.Millisecond)
	if got := a2.count(); got != 1 {
		t.Errorf("node2 room appender count = %d, want 1", got)
	}
}

// TestNATSBusFillsGapFromLog: a cm committed without a bus message (by
// a node on the LISTEN bus, standing in for a publisher that died
// between commit and publish) leaves a hole the next bus message
// reveals through its predecessor serial. node2 holds that message,
// reads the hole from the log, and delivers both in order.
func TestNATSBusFillsGapFromLog(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	silent := openListenNode(t, dsn)
	a2 := &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", &cmRecorder{})
	bindChannel(t, s2, "room", a2)
	chSilent, err := silent.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel silent: %v", err)
	}

	want := []string{publish(t, ctx, ch1, "first")}
	a2.waitFor(t, 1, 5*time.Second)
	want = append(want, publish(t, ctx, chSilent, "lost-on-the-bus"))
	want = append(want, publish(t, ctx, ch1, "reveals-the-gap"))

	a2.waitFor(t, 3, 5*time.Second)
	time.Sleep(200 * time.Millisecond)
	assertSerials(t, "node2", a2.serials(), want)
	if got := a2.cms()[1].Messages[0].Data; got != "lost-on-the-bus" {
		t.Errorf("gap cm data = %v, want lost-on-the-bus", got)
	}
	if got := storeCounters(boundStoreOf(t, s2, "room")).gapFills; got < 1 {
		t.Errorf("gapFills = %d, want >= 1", got)
	}
}

// TestNATSBusSweepRecoversLostTail: a cm committed without a bus
// message and followed by no further publish leaves no gap for the
// chain to see. The watermark sweep notices the channel is behind and
// replays it from the log.
func TestNATSBusSweepRecoversLostTail(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, 200*time.Millisecond)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s2 := openNATSNode(t, dsn, n.URL)
	silent := openListenNode(t, dsn)
	a2 := &cmRecorder{}
	bindChannel(t, s2, "room", a2)
	chSilent, err := silent.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel silent: %v", err)
	}

	want := []string{publish(t, ctx, chSilent, "lost-tail")}
	a2.waitFor(t, 1, 5*time.Second)
	time.Sleep(500 * time.Millisecond)
	assertSerials(t, "node2", a2.serials(), want)
	if got := storeCounters(boundStoreOf(t, s2, "room")).sweepCatchUps; got < 1 {
		t.Errorf("sweepCatchUps = %d, want >= 1", got)
	}
}

// TestNATSBusLargeCMTravelsAsPointer: a cm whose encoding exceeds the
// inline threshold is published as a (channel, serial) pointer, and the
// receiver fetches the body by serial; a small cm still travels inline.
func TestNATSBusLargeCMTravelsAsPointer(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL, func(o *Options) { o.NATSInlineMaxBytes = 1024 })
	s2 := openNATSNode(t, dsn, n.URL)
	a2 := &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", &cmRecorder{})
	bindChannel(t, s2, "room", a2)

	big := strings.Repeat("x", 8*1024)
	want := []string{
		publish(t, ctx, ch1, big),
		publish(t, ctx, ch1, "small"),
	}
	a2.waitFor(t, 2, 5*time.Second)
	assertSerials(t, "node2", a2.serials(), want)
	if got := a2.cms()[0].Messages[0].Data; got != big {
		t.Errorf("pointer cm body has %d bytes, want %d", len(fmt.Sprint(got)), len(big))
	}
	if got := s1.BusStats().Pointers; got != 1 {
		t.Errorf("pointers published = %d, want 1 (only the large cm)", got)
	}
}

// TestNATSBusSummarySnapshotIsCrossNodeDeterministic is the NATS-bus
// twin of TestPostgresClusterSummarySnapshotIsCrossNodeDeterministic:
// the post-fold summary snapshot, which Annotation's msgpack encoding
// omits, rides the envelope so node2 delivers node1's exact fold.
func TestNATSBusSummarySnapshotIsCrossNodeDeterministic(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	bindChannel(t, s2, "room", a2)

	target, _, err := ch1.Store(ctx, []*protocol.Message{{ID: "m1", Data: "post"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	targetSerial := target.Messages[0].Serial
	for _, client := range []string{"alice", "bob"} {
		if _, _, err := ch1.StoreAnnotation(ctx, []*protocol.Annotation{{
			Action: protocol.AnnotationCreate, ClientID: client,
			Type: "reaction:distinct.v1", Name: "👍", MessageSerial: targetSerial,
		}}); err != nil {
			t.Fatalf("StoreAnnotation %s: %v", client, err)
		}
	}
	a1.waitFor(t, 3, 5*time.Second)
	a2.waitFor(t, 3, 5*time.Second)

	last1, last2 := a1.cms()[2].Annotations[0], a2.cms()[2].Annotations[0]
	agg := last2.Summary["reaction:distinct.v1"]
	if agg == nil || agg.Values["👍"] == nil || !slices.Equal(agg.Values["👍"].ClientIDs, []string{"alice", "bob"}) {
		t.Fatalf("node2 snapshot = %#v, want 👍:[alice,bob]", last2.Summary)
	}
	if !slices.Equal(agg.Values["👍"].ClientIDs, last1.Summary["reaction:distinct.v1"].Values["👍"].ClientIDs) {
		t.Errorf("cross-node snapshot mismatch: node1=%#v node2=%#v", last1.Summary, last2.Summary)
	}
}

// TestNATSSubjectIsSafe checks the subject scheme: one NATS token per
// channel whatever characters the name holds, a hashed token for a name
// too long to encode, and a namespace token that keeps two schemas', or
// two clusters' (deployment ids), channels of the same name apart.
func TestNATSSubjectIsSafe(t *testing.T) {
	prefix := natsNamespacePrefix("dep", "public")
	ns, ok := strings.CutPrefix(prefix, natsSubjectPrefix)
	if !ok || !strings.HasSuffix(ns, ".") || strings.ContainsAny(strings.TrimSuffix(ns, "."), ".*> \t\r\n") {
		t.Fatalf("natsNamespacePrefix(public) = %q, want %q plus one literal token and a dot", prefix, natsSubjectPrefix)
	}
	for _, name := range []string{"room", "a.b.c", "*", ">", "chat:room one", "ünïcødé", strings.Repeat("n", 1000)} {
		subj := natsSubject(prefix, name)
		rest, ok := strings.CutPrefix(subj, prefix)
		if !ok {
			t.Fatalf("natsSubject(%q) = %q, want prefix %q", name, subj, prefix)
		}
		rest = strings.TrimPrefix(rest, "h.")
		if rest == "" || strings.ContainsAny(rest, ".*> \t\r\n") {
			t.Errorf("natsSubject(%q) = %q: token %q is not a single literal NATS token", name, subj, rest)
		}
	}
	if natsSubject(prefix, "a") == natsSubject(prefix, "b") {
		t.Error("distinct names share a subject")
	}
	if natsSubject(natsNamespacePrefix("dep", "s1"), "room") == natsSubject(natsNamespacePrefix("dep", "s2"), "room") {
		t.Error("the same channel in two schemas shares a subject")
	}
	if natsSubject(natsNamespacePrefix("dep1", "public"), "room") == natsSubject(natsNamespacePrefix("dep2", "public"), "room") {
		t.Error("the same channel and schema in two clusters shares a subject")
	}
}

// --- helpers ---

// openNATSNode opens a Storage on the NATS bus, closed on t.Cleanup.
func openNATSNode(t *testing.T, dsn, natsURL string, opts ...func(*Options)) *Storage {
	t.Helper()
	o := Options{DSN: dsn, Bus: BusNATS, NATSURL: natsURL}
	for _, f := range opts {
		f(&o)
	}
	s, err := Open(context.Background(), o)
	if err != nil {
		t.Fatalf("Open (NATS bus): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// openListenNode opens a Storage on the default LISTEN/NOTIFY bus. Its
// publishes never reach the NATS bus, which is how these tests stand in
// for a bus message lost after commit. A real cluster refuses a node on
// another bus (DESIGN.md §11), so this one skips the cluster identity
// check.
func openListenNode(t *testing.T, dsn string) *Storage {
	t.Helper()
	s, err := Open(context.Background(), Options{DSN: dsn, skipClusterIdentity: true})
	if err != nil {
		t.Fatalf("Open (LISTEN bus): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func bindChannel(t *testing.T, s *Storage, name string, a storage.Appender) storage.ChannelStore {
	t.Helper()
	ch, err := s.Channel(context.Background(), name, a)
	if err != nil {
		t.Fatalf("Channel %q: %v", name, err)
	}
	return ch
}

func natsBusOf(t *testing.T, s *Storage) *natsBus {
	t.Helper()
	b, ok := s.bus.(*natsBus)
	if !ok {
		t.Fatalf("storage bus is %T, want *natsBus", s.bus)
	}
	return b
}

func boundStoreOf(t *testing.T, s *Storage, name string) *channelStore {
	t.Helper()
	cs := boundStoreOrNil(s, name)
	if cs == nil {
		t.Fatalf("no bound store for %q", name)
	}
	return cs
}

func boundStoreOrNil(s *Storage, name string) *channelStore {
	return s.boundStore(name)
}

type deliveryCounters struct {
	delivered, duplicates, held, gapFills, sweepCatchUps int
}

func storeCounters(cs *channelStore) deliveryCounters {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return deliveryCounters{cs.delivered, cs.duplicates, cs.held, cs.gapFills, cs.sweepCatchUps}
}

// swapNATSTimings shrinks the NATS reconnect wait and the gap-fill
// delay and sets the sweep interval for one test. The restore is a
// t.Cleanup registered before any node opens, so it runs after every
// node has closed.
func swapNATSTimings(t *testing.T, reconnectWait, sweepInterval time.Duration) {
	t.Helper()
	origWait, origSweep, origRetry := natsReconnectWait, natsSweepDefault, natsReconcileRetryWait
	origGap, origJitter := gapFillDelay, reconcileJitterMax
	natsReconnectWait, natsSweepDefault, natsReconcileRetryWait = reconnectWait, sweepInterval, reconnectWait
	gapFillDelay = 50 * time.Millisecond
	reconcileJitterMax = reconnectWait // the reconcile's spread is not under test here
	t.Cleanup(func() {
		natsReconnectWait, natsSweepDefault, natsReconcileRetryWait = origWait, origSweep, origRetry
		gapFillDelay, reconcileJitterMax = origGap, origJitter
	})
}

// listenForNotify opens a raw connection LISTENing on the LISTEN
// broker's channel, closed on t.Cleanup.
func listenForNotify(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect LISTEN probe: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, `LISTEN `+pgx.Identifier{notifyChannelName}.Sanitize()); err != nil {
		t.Fatalf("LISTEN probe: %v", err)
	}
	return conn
}

func assertSerials(t *testing.T, who string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s appender saw %d cms, want %d, in order and exactly once\n got=%v\nwant=%v", who, len(got), len(want), got, want)
	}
}

// cmRecorder is a storage.Appender that records every delivered cm.
type cmRecorder struct {
	mu  sync.Mutex
	got []*protocol.ChannelMessage
}

func (r *cmRecorder) Initialize(current, initial string) {}

func (r *cmRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.got = append(r.got, cm)
	r.mu.Unlock()
}

func (r *cmRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func (r *cmRecorder) cms() []*protocol.ChannelMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

func (r *cmRecorder) all() []*protocol.ChannelMessage { return r.cms() }

func (r *cmRecorder) serials() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.got))
	for i, cm := range r.got {
		out[i] = cm.ChannelSerial
	}
	return out
}

func (r *cmRecorder) waitFor(t *testing.T, n int, timeout time.Duration) []*protocol.ChannelMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := r.cms(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d cms; have %d", n, r.count())
	return nil
}
