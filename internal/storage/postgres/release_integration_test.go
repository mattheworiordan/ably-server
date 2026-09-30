//go:build integration

package postgres

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// initRecorder records an appender's Initialize and Append calls in
// order: "init:<current>", then each appended cm's serial.
type initRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *initRecorder) Initialize(current, _ string) {
	r.mu.Lock()
	r.events = append(r.events, "init:"+current)
	r.mu.Unlock()
}

func (r *initRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.events = append(r.events, cm.ChannelSerial)
	r.mu.Unlock()
}

func (r *initRecorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *initRecorder) waitEvents(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := r.got(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("appender events = %v within 10s, want %d", r.got(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// clusterBuses opens node constructors for every cluster bus, so a
// cross-node test runs once per bus (DESIGN.md §7.2).
func clusterBuses(t *testing.T) map[string]func(t *testing.T, dsn string) *Storage {
	t.Helper()
	return map[string]func(t *testing.T, dsn string) *Storage{
		BusPGNotify: func(t *testing.T, dsn string) *Storage {
			s, err := Open(context.Background(), Options{DSN: dsn})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		},
		"postgres-transactional": func(t *testing.T, dsn string) *Storage { return openNode(t, dsn) },
		"postgres-coalesced": func(t *testing.T, dsn string) *Storage {
			return openCoalesced(t, dsn, 20*time.Millisecond, 0)
		},
		BusNATS: func(t *testing.T, dsn string) *Storage {
			return openNATSNode(t, dsn, natstest.Start(t).URL) // one NATS server per test binary
		},
	}
}

// TestReleaseAcrossNodesReseedsAndResumesDelivery is the cluster rebind
// rule of idle-channel eviction (DESIGN.md §5.1), on every bus: node A
// binds a channel, releases it, node B publishes while A is unbound,
// then A rebinds. The rebound appender must be Initialized at B's
// publish (so a resume reaches it through History), must receive B's
// later publishes over the bus exactly once, the released appender must
// receive nothing more, and A's bus subscription must be gone after the
// release.
func TestReleaseAcrossNodesReseedsAndResumesDelivery(t *testing.T) {
	c := pgtest.Start(t)
	for bus, open := range clusterBuses(t) {
		t.Run(bus, func(t *testing.T) {
			dsn := c.FreshSchemaDSN(t)
			ctx := context.Background()
			nodeA := open(t, dsn)
			nodeB := open(t, dsn)

			old := &initRecorder{}
			if _, err := nodeA.Channel(ctx, "room", old); err != nil {
				t.Fatalf("A Channel: %v", err)
			}
			onB, err := nodeB.Channel(ctx, "room", nil)
			if err != nil {
				t.Fatalf("B Channel: %v", err)
			}
			before := publish(t, ctx, onB, "before")
			old.waitEvents(t, 2) // init + before

			if err := nodeA.Release(ctx, "room"); err != nil {
				t.Fatalf("A Release: %v", err)
			}
			if got := nodeA.BusStats().BoundChannels; got != 0 {
				t.Fatalf("A bound channels after Release = %d, want 0", got)
			}
			assertUnsubscribed(t, nodeA, bus)

			unbound := publish(t, ctx, onB, "while-unbound")

			fresh := &initRecorder{}
			csA, err := nodeA.Channel(ctx, "room", fresh)
			if err != nil {
				t.Fatalf("A rebind: %v", err)
			}
			if got := fresh.got(); len(got) != 1 || got[0] != "init:"+unbound {
				t.Fatalf("rebound appender events = %v, want [init:%s]", got, unbound)
			}
			page, err := csA.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, AfterChannelSerial: before})
			if err != nil {
				t.Fatalf("History: %v", err)
			}
			if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].ChannelSerial != unbound {
				t.Fatalf("history after %s = %d cms, want the one published while unbound", before, len(page.ChannelMessages))
			}

			var after []string
			for _, d := range []string{"after-1", "after-2", "after-3"} {
				after = append(after, publish(t, ctx, onB, d))
			}
			got := fresh.waitEvents(t, 1+len(after))
			time.Sleep(200 * time.Millisecond) // room for a (wrong) late or duplicate delivery
			got = fresh.got()
			if want := append([]string{"init:" + unbound}, after...); !slices.Equal(got, want) {
				t.Fatalf("rebound appender events = %v, want %v", got, want)
			}
			if got := old.got(); len(got) != 2 {
				t.Fatalf("released appender events = %v, want only [init before]", got)
			}
		})
	}
}

// assertUnsubscribed checks the released channel left no bus
// subscription on the node: no NATS subscription, no LISTEN.
func assertUnsubscribed(t *testing.T, s *Storage, bus string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		switch b := s.bus.(type) {
		case *natsBus:
			ok = b.nc.NumSubscriptions() == 0
		case *pgBus:
			b.mu.Lock()
			ok = len(b.bound) == 0
			b.mu.Unlock()
			ok = ok && s.BusStats().Unlistens >= 1
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: bus subscription still present 5s after Release", bus)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
