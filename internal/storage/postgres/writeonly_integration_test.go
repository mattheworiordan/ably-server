//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// writeOnlyBuses returns the Options of every bus (no DSN).
func writeOnlyBuses(t *testing.T) map[string]Options {
	t.Helper()
	return map[string]Options{
		"pgnotify":               {},
		"postgres-transactional": {Bus: BusPostgres, NotifyMode: NotifyTransactional},
		"postgres-coalesced":     {Bus: BusPostgres, NotifyMode: NotifyCoalesced, NotifyWindow: 5 * time.Millisecond},
		"nats":                   {Bus: BusNATS, NATSURL: natstest.Start(t).URL},
	}
}

func openOptsT(t *testing.T, o Options) *Storage {
	t.Helper()
	s, err := Open(context.Background(), o)
	if err != nil {
		t.Fatalf("Open (%s): %v", o.Bus, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// assertNothingBound checks that s holds no binding of any kind: no
// channel store, so nothing for the sweep to read or eviction to
// release, and no bus subscription.
func assertNothingBound(t *testing.T, who string, s *Storage) {
	t.Helper()
	s.mu.RLock()
	n := len(s.channels)
	s.mu.RUnlock()
	if n != 0 {
		t.Errorf("%s holds %d channel stores after write-only publishes, want 0", who, n)
	}
	if got := s.BusStats().BoundChannels; got != 0 {
		t.Errorf("%s bound channels = %d, want 0", who, got)
	}
	if got := len(s.sweepStores()); got != 0 {
		t.Errorf("%s sweep set = %d channels, want 0", who, got)
	}
}

// TestUnboundPublishCrossNodeEveryBus is the write-only publish path
// (DESIGN.md §6.3) on every bus, with and without publish batching.
// Node A publishes through unbound stores (a fresh one per publish, as
// the REST handler does) on a channel only node B has bound. B receives
// every cm exactly once, in order, with the predecessor chain intact on
// the chaining buses (nothing held, no gap fill); A binds nothing and
// never runs ensure_channel. A later bind on A initialises from the
// watermark, reading the row A's batches made without ensure_channel,
// serves the history, and receives B's next publish live.
func TestUnboundPublishCrossNodeEveryBus(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	for name, o := range writeOnlyBuses(t) {
		for _, lanes := range []int{0, 4} {
			t.Run(fmt.Sprintf("%s/lanes=%d", name, lanes), func(t *testing.T) {
				o.DSN = c.FreshSchemaDSN(t)
				o.Batching = Batching{Lanes: lanes}
				a, b := openOptsT(t, o), openOptsT(t, o)

				recB := &cmRecorder{}
				bindChannel(t, b, "room", recB)

				var want []string
				for i := range 20 {
					want = append(want, publish(t, ctx, a.UnboundChannel("room"), fmt.Sprintf("m%d", i)))
				}
				recB.waitFor(t, len(want), 10*time.Second)
				time.Sleep(200 * time.Millisecond) // room for a (wrong) duplicate
				assertSerials(t, "B", recB.serials(), want)
				if b.bus.chains() {
					if got := storeCounters(boundStoreOf(t, b, "room")); got.held != 0 || got.gapFills != 0 {
						t.Errorf("B held=%d gapFills=%d, want 0: the predecessor chain from A's write-only publishes is broken", got.held, got.gapFills)
					}
				}
				assertNothingBound(t, "A", a)
				if got := a.ensureCalls.Load(); got != 0 {
					t.Errorf("A ran ensure_channel %d times for write-only publishes, want 0", got)
				}

				// A later bind on A re-seeds from the watermark.
				recA := &recordingAppender{}
				csA, err := a.Channel(ctx, "room", recA)
				if err != nil {
					t.Fatalf("A binds room: %v", err)
				}
				if got := recA.current; got != want[len(want)-1] {
					t.Fatalf("A's bind initialised at %q, want the watermark %q", got, want[len(want)-1])
				}
				if e, r := a.ensureCalls.Load(), a.rowReads.Load(); e != 0 || r != 1 {
					t.Errorf("A's bind: ensure_channel %d, row reads %d; want 0 and 1 (the row is known from A's own publishes)", e, r)
				}
				page, err := csA.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 100})
				if err != nil {
					t.Fatalf("A History: %v", err)
				}
				var hist []string
				for _, cm := range page.ChannelMessages {
					hist = append(hist, cm.ChannelSerial)
				}
				assertSerials(t, "A history", hist, want)

				live := publish(t, ctx, boundStoreOf(t, b, "room"), "after-attach")
				waitFor(t, 10*time.Second, "A to receive B's publish after its bind", func() bool {
					got := recA.got()
					return len(got) > 0 && got[len(got)-1] == live
				})
				if got := recA.got(); len(got) != 2 || got[1] != live {
					t.Fatalf("A saw %v after its bind, want [init:%s %s]", got, want[len(want)-1], live)
				}
			})
		}
	}
}

// TestUnboundPublishRacingBindEveryBus races write-only publishes on a
// node against a bind of the same channel on that node, many times, on
// every bus. Whatever the interleaving, the binding's appender receives
// every cm committed after its watermark exactly once and in order: the
// publisher fast path (or, on pgnotify, the NOTIFY round trip) reaches a
// binding made while a write-only publish was in flight.
func TestUnboundPublishRacingBindEveryBus(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	for name, o := range writeOnlyBuses(t) {
		t.Run(name, func(t *testing.T) {
			o.DSN = c.FreshSchemaDSN(t)
			o.Batching = Batching{Lanes: 4}
			a := openOptsT(t, o)
			for iter := range 10 {
				channel := fmt.Sprintf("race-%d", iter)
				var (
					mu        sync.Mutex
					published []string
					wg        sync.WaitGroup
				)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 30 {
						cm, _, err := a.UnboundChannel(channel).Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m%d", i)}})
						if err != nil {
							t.Errorf("Store: %v", err)
							return
						}
						mu.Lock()
						published = append(published, cm.ChannelSerial)
						mu.Unlock()
					}
				}()
				time.Sleep(time.Duration(iter) * time.Millisecond)
				rec := &recordingAppender{}
				if _, err := a.Channel(ctx, channel, rec); err != nil {
					t.Fatalf("bind %s: %v", channel, err)
				}
				wg.Wait()
				var want []string
				for _, s := range published {
					if s > rec.current {
						want = append(want, s)
					}
				}
				waitFor(t, 10*time.Second, channel+": the binding to receive every cm after its watermark", func() bool {
					return len(rec.got()) >= len(want)+1
				})
				time.Sleep(100 * time.Millisecond) // room for a (wrong) duplicate
				got := rec.got()
				if len(got) == 0 || !strings.HasPrefix(got[0], "init:") {
					t.Fatalf("%s: first event %v, want the Initialize", channel, got)
				}
				assertSerials(t, channel, got[1:], want)
			}
		})
	}
}

// TestShardedUnboundPublishRoutesByHash: a write-only publish on a shard
// list lands on the shard its channel hashes to, like everything else
// (DESIGN.md §6.4), and binds nothing on any shard.
func TestShardedUnboundPublishRoutesByHash(t *testing.T) {
	ctx := context.Background()
	o := Options{Bus: BusNATS, NATSURL: natstest.Start(t).URL, Batching: Batching{Lanes: 4}}
	s := openShardedT(t, o, shardDSNs(t, 2))
	names := namesOnEveryShard(t, "wo", 8, 2)
	for _, name := range names {
		publish(t, ctx, s.UnboundChannel(name), "x")
	}
	for _, name := range names {
		owner := ShardFor(name, 2)
		for i := range 2 {
			var n int
			if err := s.Shard(i).pool.QueryRow(ctx, `SELECT count(*) FROM channel_messages WHERE channel = $1`, name).Scan(&n); err != nil {
				t.Fatalf("shard %d count: %v", i, err)
			}
			if want := map[bool]int{true: 1, false: 0}[i == owner]; n != want {
				t.Errorf("%s: shard %d holds %d rows, want %d (owner shard %d)", name, i, n, want, owner)
			}
		}
	}
	for i := range 2 {
		assertNothingBound(t, fmt.Sprintf("shard %d", i), s.Shard(i))
	}
}

// TestUnboundPublishBindOnWrite: under Options.BindOnWrite the pre-write-
// only behaviour holds: a bind runs ensure_channel even for a channel
// this node has published on.
func TestUnboundPublishBindOnWrite(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	a := openOptsT(t, Options{DSN: c.FreshSchemaDSN(t), Batching: Batching{Lanes: 4}, BindOnWrite: true})
	st := a.UnboundChannel("room")
	publish(t, ctx, st, "x")
	if _, err := a.Channel(ctx, "room", &recordingAppender{}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if e, r := a.ensureCalls.Load(), a.rowReads.Load(); e != 1 || r != 0 {
		t.Errorf("bind under BindOnWrite: ensure_channel %d, row reads %d; want 1 and 0", e, r)
	}
}
