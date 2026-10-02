//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// subRecorder is a recorder that reports whether it has a subscriber
// (storage.SubscriberReporter), as core.Channel does.
type subRecorder struct {
	recorder
	sub atomic.Bool
}

func (r *subRecorder) HasSubscribers() bool { return r.sub.Load() }

// sweptNames collects the channel names of every watermark sweep query
// while installed.
type sweptNames struct {
	mu    sync.Mutex
	names map[string]int
}

func installSweptNames(t *testing.T) *sweptNames {
	t.Helper()
	sn := &sweptNames{names: map[string]int{}}
	h := func(names []string) {
		sn.mu.Lock()
		for _, n := range names {
			sn.names[n]++
		}
		sn.mu.Unlock()
	}
	sweepNamesHook.Store(&h)
	t.Cleanup(func() { sweepNamesHook.Store(nil) })
	return sn
}

func (sn *sweptNames) count(name string) int {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return sn.names[name]
}

// TestSweepScopeSubscribedOnly checks the sweep's scope, the bound
// channels with a local subscriber (DESIGN.md §7.2), on both chaining
// buses. A lost tail (a cm committed by
// a node whose bus messages never arrive) on a channel with a local
// subscriber is delivered within two sweep intervals. A channel bound
// here without a subscriber (a REST-only bind) is never read by the
// sweep; once it gains a subscriber its lost tail is delivered within two
// intervals of that.
func TestSweepScopeSubscribedOnly(t *testing.T) {
	const interval = 500 * time.Millisecond
	const slack = 500 * time.Millisecond // query time and scheduling under -race
	c := pgtest.Start(t)
	n := natstest.Start(t)
	for _, bus := range []string{BusPostgres, BusNATS} {
		t.Run(bus, func(t *testing.T) {
			dsn := c.FreshSchemaDSN(t)
			ctx := context.Background()
			open := func() *Storage {
				o := pgBusOptions(dsn)
				if bus == BusNATS {
					o = Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL}
				}
				o.SweepInterval = interval
				s, err := Open(ctx, o)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			silent := openListenNode(t, dsn)
			swept := installSweptNames(t)
			b := open()

			sub, rest := &subRecorder{}, &subRecorder{}
			sub.sub.Store(true)
			if _, err := b.Channel(ctx, "sub-room", sub); err != nil {
				t.Fatalf("bind sub-room: %v", err)
			}
			if _, err := b.Channel(ctx, "rest-room", rest); err != nil {
				t.Fatalf("bind rest-room: %v", err)
			}
			quietSub, _ := silent.Channel(ctx, "sub-room", nil)
			quietRest, _ := silent.Channel(ctx, "rest-room", nil)

			wantSub := publish(t, ctx, quietSub, "lost-tail")
			wantRest := publish(t, ctx, quietRest, "lost-tail-rest")
			published := time.Now()
			waitForCount(t, &sub.recorder, 1, 2*interval+slack)
			if took := time.Since(published); took > 2*interval+slack {
				t.Errorf("lost tail on the subscribed channel took %s, want at most two intervals (%s)", took, 2*interval)
			}
			assertSerials(t, "sub-room", sub.serials(), []string{wantSub})

			time.Sleep(3 * interval)
			if got := rest.count(); got != 0 {
				t.Fatalf("rest-room (no subscriber) saw %d cms from the sweep, want 0", got)
			}
			if got := swept.count("rest-room"); got != 0 {
				t.Fatalf("the sweep read rest-room %d times, want 0 (it has no subscriber)", got)
			}
			if got := swept.count("sub-room"); got < 2 {
				t.Errorf("the sweep read sub-room %d times, want at least 2", got)
			}

			// The channel gains a subscriber: its lost tail is delivered
			// within two intervals of that.
			rest.sub.Store(true)
			gained := time.Now()
			waitForCount(t, &rest.recorder, 1, 2*interval+slack)
			if took := time.Since(gained); took > 2*interval+slack {
				t.Errorf("lost tail after gaining a subscriber took %s, want at most two intervals", took)
			}
			assertSerials(t, "rest-room", rest.serials(), []string{wantRest})
			if got := b.BusStats().SweepChannels; got == 0 {
				t.Error("SweepChannels = 0, want the channels the sweeps read")
			}
		})
	}
}

// TestReconcileScopedAndBounded (DESIGN.md §7.2): a bus reconnect
// reconciles only the bound channels with a subscriber on this node (the
// sweep scope), not every bound channel, and the node runs at most
// catchUpConcurrency of its batched catch-up queries at once. Before,
// every bound channel was reconciled, one query of 500 after another
// with no jitter, so a cluster-wide bus blip made every node read every
// channel it held against the same primaries at the same moment.
func TestReconcileScopedAndBounded(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	origChunk := reconcileChunk
	reconcileChunk = 2
	t.Cleanup(func() { reconcileChunk = origChunk })

	o := pgBusOptions(dsn)
	o.SweepInterval = time.Hour // no sweep during the test; the reconcile is run directly below, without the jitter
	s, err := Open(ctx, o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const bound, subscribed = 40, 12
	want := map[string]bool{}
	for i := range bound {
		r := &subRecorder{}
		name := fmt.Sprintf("room-%d", i)
		if i < subscribed {
			r.sub.Store(true)
			want[name] = true
		}
		if _, err := s.Channel(ctx, name, r); err != nil {
			t.Fatalf("bind %s: %v", name, err)
		}
	}

	var (
		mu                sync.Mutex
		read              = map[string]int{}
		inFlight, maxSeen int
	)
	h := func(names []string) func() {
		mu.Lock()
		for _, n := range names {
			read[n]++
		}
		inFlight++
		maxSeen = max(maxSeen, inFlight)
		mu.Unlock()
		time.Sleep(50 * time.Millisecond) // hold the query so concurrent ones overlap
		return func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}
	}
	catchUpHook.Store(&h)
	t.Cleanup(func() { catchUpHook.Store(nil) })

	n, err := s.reconcileBound(ctx)
	if err != nil {
		t.Fatalf("reconcileBound: %v", err)
	}
	if n != subscribed {
		t.Errorf("reconciled %d channels, want the %d subscribed of %d bound", n, subscribed, bound)
	}
	mu.Lock()
	defer mu.Unlock()
	for name := range read {
		if !want[name] {
			t.Errorf("reconcile read %s, which has no subscriber", name)
		}
	}
	for name := range want {
		if read[name] != 1 {
			t.Errorf("reconcile read %s %d times, want once", name, read[name])
		}
	}
	if maxSeen > catchUpConcurrency {
		t.Errorf("%d catch-up queries in flight at once, want at most %d", maxSeen, catchUpConcurrency)
	}
	if maxSeen < 2 {
		t.Errorf("%d catch-up query in flight at most; the %d chunks should overlap", maxSeen, subscribed/reconcileChunk)
	}
	if got := s.BusStats().Reconciles; got != subscribed {
		t.Errorf("ably_bus_reconciled_channels_total = %d, want %d", got, subscribed)
	}
}

// TestCatchUpBoundSharedAcrossShards: the catch-up bound is per node, so
// one sharded node's shards share it.
func TestCatchUpBoundSharedAcrossShards(t *testing.T) {
	s := openShardedT(t, Options{Bus: BusPostgres, NotifyMode: NotifyTransactional}, shardDSNs(t, 2))
	if s.Shard(0).catchUpSlots == nil || s.Shard(0).catchUpSlots != s.Shard(1).catchUpSlots || cap(s.Shard(0).catchUpSlots) != catchUpConcurrency {
		t.Errorf("shards' catch-up slots are not one shared bound of %d", catchUpConcurrency)
	}
}
