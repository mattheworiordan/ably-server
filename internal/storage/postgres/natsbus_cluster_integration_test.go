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
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestNATSBusClusterSurvivesServerKill runs two nodes against a
// three-server NATS cluster (DESIGN.md §7.2) and kills the server the
// receiving node is connected to while the other node keeps publishing.
// The receiver moves to another server and reconciles; delivery resumes
// well inside the plan's 30 s bound, and every acknowledged publish
// reaches both nodes exactly once, in log order.
func TestNATSBusClusterSurvivesServerKill(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, 300*time.Millisecond)

	c := pgtest.Start(t)
	cluster := natstest.StartCluster(t, 3)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, cluster.URL())
	s2 := openNATSNode(t, dsn, cluster.URL())
	a1, a2 := &cmRecorder{}, &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", a1)
	bindChannel(t, s2, "room", a2)
	bus2 := natsBusOf(t, s2)

	// Warm up: the channel works across the cluster before the kill.
	var acked []string
	for i := range 5 {
		acked = append(acked, publish(t, ctx, ch1, fmt.Sprintf("warm-%d", i)))
	}
	a2.waitFor(t, len(acked), 10*time.Second)

	killed := cluster.ByURL(bus2.nc.ConnectedUrl())
	if killed == nil {
		t.Fatalf("node2 is connected to %q, not a cluster server", bus2.nc.ConnectedUrl())
	}
	reconcilesBefore := s2.BusStats().ReconcileRuns

	// Publish continuously from node1 while the server goes away.
	var (
		mu      sync.Mutex
		stop    = make(chan struct{})
		pubDone = make(chan struct{})
	)
	go func() {
		defer close(pubDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("live-%d", i)}})
			if err != nil {
				t.Errorf("Store during the kill: %v", err)
				return
			}
			mu.Lock()
			acked = append(acked, cm.ChannelSerial)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	killedAt := time.Now()
	killed.Kill(t)
	waitFor(t, 30*time.Second, "node2 to reconnect to another server", func() bool {
		return bus2.nc.IsConnected() && bus2.nc.ConnectedUrl() != killed.URL
	})

	// A publish made after the reconnect must arrive over the bus.
	mu.Lock()
	n := len(acked)
	mu.Unlock()
	a2.waitFor(t, n+1, 30*time.Second)
	resumed := time.Since(killedAt)
	if resumed > 30*time.Second {
		t.Errorf("delivery resumed %s after the kill, want within 30s", resumed)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-pubDone

	mu.Lock()
	want := append([]string(nil), acked...)
	mu.Unlock()
	a1.waitFor(t, len(want), 15*time.Second)
	a2.waitFor(t, len(want), 15*time.Second)
	time.Sleep(500 * time.Millisecond) // room for a (wrong) duplicate

	page, err := ch1.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 10 * len(want)})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var logOrder []string
	for _, cm := range page.ChannelMessages {
		logOrder = append(logOrder, cm.ChannelSerial)
	}
	assertSerials(t, "log", logOrder, want)
	assertSerials(t, "node1", a1.serials(), want)
	assertSerials(t, "node2", a2.serials(), want)

	if got := s2.BusStats().ReconcileRuns; got <= reconcilesBefore {
		t.Errorf("node2 reconciles = %d, want > %d after moving servers", got, reconcilesBefore)
	}
	if err := s2.Ping(ctx); err != nil {
		t.Errorf("node2 not ready after reconnecting: %v", err)
	}
	st := s2.BusStats()
	t.Logf("killed %s; node2 now on %s; delivery resumed %s after the kill; %d publishes; node2 fastPath=%d inline=%d filled=%d gapFills=%d reconciles=%d sweepCatchUps=%d",
		killed.Name, bus2.nc.ConnectedUrl(), resumed.Round(time.Millisecond), len(want), st.FastPath, st.Inline, st.Filled, st.GapFills, st.ReconcileRuns, st.SweepCatchUps)
}

// TestNATSBusReadyzReflectsConnection: in nats mode a node with no NATS
// connection is not ready (DESIGN.md §7.2), and is ready again once the
// client reconnects.
func TestNATSBusReadyzReflectsConnection(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	c := pgtest.Start(t)
	n := natstest.Start(t)
	proxy := natstest.NewProxy(t, n.Addr)
	s := openNATSNode(t, c.FreshSchemaDSN(t), proxy.URL())
	ctx := context.Background()
	bus := natsBusOf(t, s)

	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping while connected: %v", err)
	}
	if st := s.BusStats(); !st.Connected || st.Bus != BusNATS {
		t.Fatalf("BusStats connected=%v bus=%q, want true, nats", st.Connected, st.Bus)
	}
	proxy.Cut()
	waitFor(t, 5*time.Second, "the node to notice the outage", func() bool { return !bus.nc.IsConnected() })
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping while disconnected from NATS = nil, want an error (not ready)")
	}
	if s.BusStats().Connected {
		t.Error("BusStats().Connected = true while disconnected")
	}
	proxy.Restore()
	waitFor(t, 10*time.Second, "the node to reconnect", func() bool { return bus.nc.IsConnected() })
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
}

// TestNATSBusSchemasShareNATSWithoutCrossTalk: two deployments in
// different schemas share one NATS server (DESIGN.md §7.2). A publish on
// "room" in one never reaches a node bound to "room" in the other: the
// subject carries a namespace token per schema.
func TestNATSBusSchemasShareNATSWithoutCrossTalk(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	ctx := context.Background()

	pubNode := openNATSNode(t, c.FreshSchemaDSN(t), n.URL)
	otherNode := openNATSNode(t, c.FreshSchemaDSN(t), n.URL)
	mine, theirs := &cmRecorder{}, &cmRecorder{}
	ch := bindChannel(t, pubNode, "room", mine)
	bindChannel(t, otherNode, "room", theirs)
	before := natsBusOf(t, otherNode).nc.Stats().InMsgs

	for i := range 5 {
		publish(t, ctx, ch, fmt.Sprintf("m-%d", i))
	}
	mine.waitFor(t, 5, 5*time.Second)
	time.Sleep(300 * time.Millisecond)
	if got := theirs.count(); got != 0 {
		t.Fatalf("the other schema's node received %d cms, want 0", got)
	}
	if got := natsBusOf(t, otherNode).nc.Stats().InMsgs - before; got != 0 {
		t.Fatalf("the other schema's node received %d NATS messages, want 0", got)
	}
}
