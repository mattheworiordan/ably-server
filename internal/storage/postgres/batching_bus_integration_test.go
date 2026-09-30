//go:build integration

package postgres

import (
	"context"
	"fmt"
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

// batchedBuses returns, per bus, an Options builder with batching on.
func batchedBuses(t *testing.T) map[string]func(dsn string) Options {
	t.Helper()
	lanes := Batching{Lanes: 4}
	return map[string]func(string) Options{
		"pgnotify": func(dsn string) Options { return Options{DSN: dsn, Batching: lanes} },
		"postgres-transactional": func(dsn string) Options {
			return Options{DSN: dsn, Bus: BusPostgres, NotifyMode: NotifyTransactional, Batching: lanes}
		},
		"postgres-coalesced": func(dsn string) Options {
			return Options{DSN: dsn, Bus: BusPostgres, NotifyMode: NotifyCoalesced, NotifyWindow: 5 * time.Millisecond, SweepInterval: 200 * time.Millisecond, Batching: lanes}
		},
		"nats": func(dsn string) Options {
			n := natstest.Start(t)
			return Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL, Batching: lanes}
		},
	}
}

func openOpts(t *testing.T, o Options) *Storage {
	t.Helper()
	s, err := Open(context.Background(), o)
	if err != nil {
		t.Fatalf("Open (%s): %v", o.Bus, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestBatchedChannelStoreContractEveryBus runs the storage contract suite
// with publish batching on, on every bus (DESIGN.md §6.3, §7.2).
func TestBatchedChannelStoreContractEveryBus(t *testing.T) {
	c := pgtest.Start(t)
	for name, opts := range batchedBuses(t) {
		t.Run(name, func(t *testing.T) {
			storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
				return openOpts(t, opts(c.FreshSchemaDSN(t)))
			})
		})
	}
}

// TestOneLaneLingerChannelStoreContractEveryBus runs the storage contract
// suite with one lane and a linger floor and cap (--publish-lanes=1
// --publish-linger-min=2ms --publish-linger-max=10ms), the batch-depth
// settings the scale runs compare (DESIGN.md §6.3), on every bus.
func TestOneLaneLingerChannelStoreContractEveryBus(t *testing.T) {
	c := pgtest.Start(t)
	for name, opts := range batchedBuses(t) {
		t.Run(name, func(t *testing.T) {
			storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
				o := opts(c.FreshSchemaDSN(t))
				o.Batching = Batching{Lanes: 1, LingerMin: 2 * time.Millisecond, LingerMax: 10 * time.Millisecond}
				return openOpts(t, o)
			})
		})
	}
}

// orderAppender records every delivered serial and whether any arrived
// out of order or twice.
type orderAppender struct {
	mu      sync.Mutex
	init    string
	serials []string
	bad     []string
}

func (a *orderAppender) Initialize(current, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.init = current
}

func (a *orderAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	last := a.init
	if n := len(a.serials); n > 0 {
		last = a.serials[n-1]
	}
	if cm.ChannelSerial <= last {
		a.bad = append(a.bad, fmt.Sprintf("%s after %s", cm.ChannelSerial, last))
	}
	a.serials = append(a.serials, cm.ChannelSerial)
}

func (a *orderAppender) snapshot() ([]string, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.serials...), append([]string(nil), a.bad...)
}

// TestBatchedCrossNodeDeliveryEveryBus: two nodes with batching on
// publish concurrently to a few shared channels, so batches carry several
// cms of one channel. Every node's appender must receive every channel's
// cms exactly once, in log order. (The chain can heal a wrong
// predecessor by a gap fill, so this is an end-to-end check;
// TestBatchedPredecessorChain checks the predecessors themselves.)
func TestBatchedCrossNodeDeliveryEveryBus(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	for name, opts := range batchedBuses(t) {
		t.Run(name, func(t *testing.T) {
			dsn := c.FreshSchemaDSN(t)
			o := opts(dsn)
			nodes := []*Storage{openOpts(t, o), openOpts(t, o)}
			channels := []string{"a", "b", "c"}
			apps := map[string][]*orderAppender{}
			stores := map[string][]storage.ChannelStore{}
			for _, ch := range channels {
				for _, n := range nodes {
					app := &orderAppender{}
					cs, err := n.Channel(ctx, ch, app)
					if err != nil {
						t.Fatalf("Channel: %v", err)
					}
					apps[ch] = append(apps[ch], app)
					stores[ch] = append(stores[ch], cs)
				}
			}

			const writers, each = 6, 30
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range each {
						ch := channels[(w+i)%len(channels)]
						if _, _, err := stores[ch][w%2].Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("%d-%d", w, i)}}); err != nil {
							t.Errorf("Store: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()

			for _, ch := range channels {
				page, err := stores[ch][0].History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
				if err != nil {
					t.Fatalf("History: %v", err)
				}
				var want []string
				for _, cm := range page.ChannelMessages {
					want = append(want, cm.ChannelSerial)
				}
				for i, app := range apps[ch] {
					deadline := time.Now().Add(10 * time.Second)
					for {
						got, _ := app.snapshot()
						if len(got) >= len(want) || time.Now().After(deadline) {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					got, bad := app.snapshot()
					if len(bad) > 0 {
						t.Errorf("%s node %d: out of order or duplicate: %v", ch, i, bad)
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s node %d: delivered %d cms, log has %d (delivered %v, log %v)", ch, i, len(got), len(want), got, want)
					}
				}
			}
			var batches uint64
			var committed float64
			for _, n := range nodes {
				c, s := n.batchStats()
				batches += c
				committed += s
			}
			t.Logf("%s: %v publishes in %d batches", name, committed, batches)
		})
	}
}

// batchStats returns batches committed and the publishes in them.
func (s *Storage) batchStats() (uint64, float64) {
	return s.BatchSizeSum()
}

// commitCall is one afterCommit the committer made.
type commitCall struct{ channel, serial, prev string }

// recordingBus wraps a Bus and records every afterCommit call; the
// variant with batch=true also forwards (and records) the batched
// in-transaction hook, the other hides it so the committer takes the
// per-cm connTx fallback.
type recordingBus struct {
	Bus
	mu      sync.Mutex
	commits []commitCall
	batches [][]string // channel of each item, per beforeCommitBatch call
}

func (r *recordingBus) afterCommit(cs *channelStore, cm *protocol.ChannelMessage, prev string) {
	r.mu.Lock()
	r.commits = append(r.commits, commitCall{cs.name, cm.ChannelSerial, prev})
	r.mu.Unlock()
	r.Bus.afterCommit(cs, cm, prev)
}

type recordingBatchBus struct{ *recordingBus }

func (r recordingBatchBus) beforeCommitBatch(ctx context.Context, b *pgx.Batch, items []batchItem) error {
	names := make([]string, len(items))
	for i, it := range items {
		names[i] = it.cs.name
	}
	r.mu.Lock()
	r.batches = append(r.batches, names)
	r.mu.Unlock()
	return r.Bus.(batchBeforeCommitter).beforeCommitBatch(ctx, b, items)
}

// TestBatchedPredecessorChain checks the predecessor every cm of a batch
// announces to a chaining bus directly: on each channel, each cm's prev
// must be the serial of the cm before it in the log (the first one's,
// the channel's serial before the test), earlier cms of the same batch
// included. It runs with the batched hook and with the per-cm connTx
// fallback, on the transactional postgres bus (chaining, NOTIFY in the
// transaction) and on nats.
func TestBatchedPredecessorChain(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	n := natstest.Start(t)
	for _, busName := range []string{"postgres-transactional", "nats"} {
		for _, batched := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/batchHook=%v", busName, batched), func(t *testing.T) {
				dsn := c.FreshSchemaDSN(t)
				o := Options{DSN: dsn, Bus: BusPostgres, NotifyMode: NotifyTransactional, Batching: Batching{Lanes: 1}}
				if busName == "nats" {
					o = Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL, Batching: Batching{Lanes: 1}}
				}
				s := openOpts(t, o)
				rec := &recordingBus{Bus: s.bus}
				if batched {
					s.bus = recordingBatchBus{rec}
				} else {
					s.bus = rec
				}
				channels := []string{"x", "y"}
				start := map[string]string{}
				stores := map[string]storage.ChannelStore{}
				for _, ch := range channels {
					cs, err := s.Channel(ctx, ch, &orderAppender{})
					if err != nil {
						t.Fatalf("Channel: %v", err)
					}
					stores[ch] = cs
					var cur string
					if err := s.pool.QueryRow(ctx, `SELECT channel_serial FROM channels WHERE name = $1`, ch).Scan(&cur); err != nil {
						t.Fatalf("channels row: %v", err)
					}
					start[ch] = cur
				}
				var wg sync.WaitGroup
				for w := range 16 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := range 15 {
							ch := channels[(w+i)%2]
							if _, _, err := stores[ch].Store(ctx, []*protocol.Message{{Data: "p"}}); err != nil {
								t.Errorf("Store: %v", err)
								return
							}
						}
					}()
				}
				wg.Wait()

				rec.mu.Lock()
				calls := append([]commitCall(nil), rec.commits...)
				multi := false
				for _, b := range rec.batches {
					seen := map[string]bool{}
					for _, name := range b {
						if seen[name] {
							multi = true
						}
						seen[name] = true
					}
				}
				rec.mu.Unlock()
				prevOf := map[string]string{}
				for _, cc := range calls {
					prevOf[cc.channel+"|"+cc.serial] = cc.prev
				}
				for _, ch := range channels {
					page, err := stores[ch].History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
					if err != nil {
						t.Fatalf("History: %v", err)
					}
					want := start[ch]
					for _, cm := range page.ChannelMessages {
						got, ok := prevOf[ch+"|"+cm.ChannelSerial]
						if !ok {
							t.Fatalf("%s: no afterCommit for stored cm %s", ch, cm.ChannelSerial)
						}
						if got != want {
							t.Fatalf("%s: cm %s announced prev %s, want %s (the cm before it in the log)", ch, cm.ChannelSerial, got, want)
						}
						want = cm.ChannelSerial
					}
				}
				if batched && !multi {
					t.Log("no batch carried two cms of one channel this run; the chain check still covered consecutive batches")
				}
			})
		}
	}
}

// TestBatchedLostCommitReplyStillDelivers: when a batch's COMMIT lands
// but its reply is lost, the retry recovers the publish as the caller's
// own (not a duplicate) and runs the bus's post-commit hook the lost
// attempt skipped, so other nodes get the cm at once rather than on the
// next watermark sweep.
func TestBatchedLostCommitReplyStillDelivers(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	ctx := context.Background()
	dsn := c.FreshSchemaDSN(t)
	o := Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL, SweepInterval: time.Hour, Batching: Batching{Lanes: 1}}
	pub, sub := openOpts(t, o), openOpts(t, o)
	app := &orderAppender{}
	if _, err := sub.Channel(ctx, "lost", app); err != nil {
		t.Fatalf("Channel sub: %v", err)
	}
	ch, err := pub.Channel(ctx, "lost", &orderAppender{})
	if err != nil {
		t.Fatalf("Channel pub: %v", err)
	}
	var once sync.Once
	lost := func() error {
		var fail bool
		once.Do(func() { fail = true })
		if fail {
			return fmt.Errorf("injected: COMMIT reply lost")
		}
		return nil
	}
	commitBatchAfterHook.Store(&lost)
	defer commitBatchAfterHook.Store(nil)

	cm, idem, err := ch.Store(ctx, []*protocol.Message{{Data: "once"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if idem {
		t.Error("the caller's own publish, recovered by the retry, was reported idempotent")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := app.snapshot(); len(got) > 0 {
			if got[0] != cm.ChannelSerial {
				t.Fatalf("remote node got %v, want %s", got, cm.ChannelSerial)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("remote node did not receive the recovered publish within 2s (the sweep is an hour away)")
}

// TestPublishBatchLockCreatesMissingRow drives the function's path for a
// channel with no row (the default Go path's first publish on a cold
// channel; under BindOnWrite the row is made before queueing instead):
// the row is created, locked and advanced, and the cm's prev is the new
// row's seed serial.
func TestPublishBatchLockCreatesMissingRow(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	s := openOpts(t, Options{DSN: c.FreshSchemaDSN(t)})
	var ord int
	var status, serial, prev string
	if err := s.pool.QueryRow(ctx,
		`SELECT ord, status, serial, prev FROM publish_batch_lock('ser', ARRAY['nobody'], ARRAY[false], ARRAY['0'], ARRAY[]::int[], ARRAY[]::text[])`,
	).Scan(&ord, &status, &serial, &prev); err != nil {
		t.Fatalf("publish_batch_lock: %v", err)
	}
	var cur, initial string
	if err := s.pool.QueryRow(ctx, `SELECT channel_serial, initial_channel_serial FROM channels WHERE name = 'nobody'`).Scan(&cur, &initial); err != nil {
		t.Fatalf("channels row not created: %v", err)
	}
	if status != "ok" || prev != initial || cur != serial || serial <= prev {
		t.Errorf("status=%s serial=%s prev=%s; row current=%s initial=%s: want ok, prev = seed, row advanced to serial", status, serial, prev, cur, initial)
	}
}
