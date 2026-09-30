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
// cms exactly once, in log order. On the chaining buses this checks that
// each cm of a batch carries its channel's true predecessor, earlier cms
// of the same batch included (a wrong prev would hold the cm, gap-fill,
// or deliver out of order).
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
