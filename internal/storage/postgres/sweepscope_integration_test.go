//go:build integration

package postgres

import (
	"context"
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

// TestSweepScopeSubscribedOnly checks the subscribed sweep scope
// (DESIGN.md §7.2) on both chaining buses. A lost tail (a cm committed by
// a node whose bus messages never arrive) on a channel with a local
// subscriber is delivered within two sweep intervals. A channel bound
// here without a subscriber (a REST-only bind) is never read by the
// sweep; once it gains a subscriber its lost tail is delivered within two
// intervals of that. Under the bound scope the REST-only channel is read.
func TestSweepScopeSubscribedOnly(t *testing.T) {
	const interval = 500 * time.Millisecond
	const slack = 500 * time.Millisecond // query time and scheduling under -race
	c := pgtest.Start(t)
	n := natstest.Start(t)
	for _, bus := range []string{BusPostgres, BusNATS} {
		t.Run(bus, func(t *testing.T) {
			dsn := c.FreshSchemaDSN(t)
			ctx := context.Background()
			open := func(scope string) *Storage {
				o := pgBusOptions(dsn)
				if bus == BusNATS {
					o = Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL}
				}
				o.SweepInterval, o.SweepScope = interval, scope
				s, err := Open(ctx, o)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			silent := openListenNode(t, dsn)
			swept := installSweptNames(t)
			b := open("") // the default scope: subscribed

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

			// Bound scope: every bound channel is read, subscriber or not.
			bb := open(SweepBound)
			idle := &subRecorder{}
			if _, err := bb.Channel(ctx, "idle-room", idle); err != nil {
				t.Fatalf("bind idle-room: %v", err)
			}
			quietIdle, _ := silent.Channel(ctx, "idle-room", nil)
			wantIdle := publish(t, ctx, quietIdle, "lost-tail-idle")
			waitForCount(t, &idle.recorder, 1, 2*interval+slack)
			assertSerials(t, "idle-room", idle.serials(), []string{wantIdle})
			if got := swept.count("idle-room"); got == 0 {
				t.Error("the bound scope never read idle-room")
			}
		})
	}
}
