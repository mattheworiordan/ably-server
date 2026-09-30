//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

func openBatched(t *testing.T, dsn string, b postgres.Batching) *postgres.Storage {
	t.Helper()
	s, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn, Batching: b})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestBatchedConcurrentPublishesAcrossChannels: many concurrent
// publishers on two nodes across many channels. Every publish gets
// exactly one serial, serials are strictly increasing per channel in the
// log, nothing is duplicated, and publishes really were batched.
func TestBatchedConcurrentPublishesAcrossChannels(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	nodes := []*postgres.Storage{openBatched(t, dsn, postgres.Batching{Lanes: 4}), openBatched(t, dsn, postgres.Batching{Lanes: 4})}

	const channels, publishers, perPublisher = 25, 32, 40
	type pub struct{ channel, serial string }
	var (
		mu   sync.Mutex
		pubs []pub
		wg   sync.WaitGroup
	)
	for w := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := nodes[w%2]
			for i := range perPublisher {
				name := fmt.Sprintf("ch-%d", (w*7+i)%channels)
				ch, err := node.Channel(ctx, name, nil)
				if err != nil {
					t.Errorf("Channel: %v", err)
					return
				}
				cm, idem, err := ch.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("w%d-%d", w, i)}})
				if err != nil || idem {
					t.Errorf("Store: idempotent=%v err=%v", idem, err)
					return
				}
				mu.Lock()
				pubs = append(pubs, pub{name, cm.ChannelSerial})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(pubs) != publishers*perPublisher {
		t.Fatalf("acknowledged %d publishes, want %d", len(pubs), publishers*perPublisher)
	}
	seen := map[string]bool{}
	for _, p := range pubs {
		key := p.channel + "|" + p.serial
		if seen[key] {
			t.Fatalf("serial %s on %s returned to two publishes", p.serial, p.channel)
		}
		seen[key] = true
	}
	for i := range channels {
		name := fmt.Sprintf("ch-%d", i)
		ch, _ := nodes[0].Channel(ctx, name, nil)
		page, err := ch.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		prev := ""
		for _, cm := range page.ChannelMessages {
			if cm.ChannelSerial <= prev {
				t.Fatalf("%s: serial %s not after %s", name, cm.ChannelSerial, prev)
			}
			prev = cm.ChannelSerial
			if !seen[name+"|"+cm.ChannelSerial] {
				t.Fatalf("%s: stored serial %s was never acknowledged", name, cm.ChannelSerial)
			}
		}
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages`); n != publishers*perPublisher {
		t.Errorf("rows stored = %d, want %d", n, publishers*perPublisher)
	}
	var batches uint64
	var committed float64
	for _, n := range nodes {
		c, s := n.BatchSizeSum()
		batches += c
		committed += s
	}
	if committed != publishers*perPublisher {
		t.Errorf("publishes committed through batches = %v, want %d", committed, publishers*perPublisher)
	}
	if batches >= uint64(publishers*perPublisher) {
		t.Errorf("batches = %d for %d publishes: nothing was batched", batches, publishers*perPublisher)
	}
	t.Logf("%d publishes in %d batches (mean %.1f)", publishers*perPublisher, batches, committed/float64(batches))
	for i, n := range nodes {
		c, r, d := n.BatchCounters()
		t.Logf("node %d: commits=%v retries=%v deferred=%v", i, c, r, d)
	}
}

// TestBatchedIdempotentRetryDedupes: a retried publish with the same
// client id dedupes whether the original is already committed or is in
// the same batch.
func TestBatchedIdempotentRetryDedupes(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s := openBatched(t, dsn, postgres.Batching{Lanes: 1})
	ch, err := s.Channel(ctx, "idem", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	// Committed original, then a retry.
	first, _, err := ch.Store(ctx, []*protocol.Message{{ID: "a", Data: "1"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	again, idem, err := ch.Store(ctx, []*protocol.Message{{ID: "a", Data: "1"}})
	if err != nil || !idem || again.ChannelSerial != first.ChannelSerial {
		t.Errorf("retry after commit: idempotent=%v serial=%s err=%v, want true %s nil", idem, again.ChannelSerial, err, first.ChannelSerial)
	}

	// Concurrent copies, so several land in one batch.
	for round := range 5 {
		id := fmt.Sprintf("b%d", round)
		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			fresh int
			sers  = map[string]bool{}
		)
		start := make(chan struct{})
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				cm, idem, err := ch.Store(ctx, []*protocol.Message{{ID: id}})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					t.Errorf("Store: %v", err)
					return
				}
				if !idem {
					fresh++
				}
				sers[cm.ChannelSerial] = true
			}()
		}
		close(start)
		wg.Wait()
		if fresh != 1 || len(sers) != 1 {
			t.Errorf("round %d: fresh = %d, distinct serials = %d; want 1 and 1", round, fresh, len(sers))
		}
		if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE id = $1`, id); n != 1 {
			t.Errorf("round %d: rows for id %s = %d, want 1", round, id, n)
		}
	}
}

// TestBatchedHotChannelDoesNotDelayColdChannels: while another
// transaction holds a channel's row lock (a hot channel being written
// elsewhere), publishes to other channels on the same lane still commit
// within about one commit; the hot channel's publish is deferred, then
// waits its turn and commits once the lock is released.
func TestBatchedHotChannelDoesNotDelayColdChannels(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s := openBatched(t, dsn, postgres.Batching{Lanes: 1})
	hot, err := s.Channel(ctx, "hot", nil)
	if err != nil {
		t.Fatalf("Channel hot: %v", err)
	}
	cold, err := s.Channel(ctx, "cold", nil)
	if err != nil {
		t.Fatalf("Channel cold: %v", err)
	}

	// Both channels' rows must exist for the holder to lock one (a
	// Channel call without an appender does not create it).
	for _, ch := range []storage.ChannelStore{hot, cold} {
		if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "warm-up"}}); err != nil {
			t.Fatalf("warm-up Store: %v", err)
		}
	}

	holder, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect holder: %v", err)
	}
	defer holder.Close(ctx)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM channels WHERE name = 'hot' FOR UPDATE`); err != nil {
		t.Fatalf("lock hot: %v", err)
	}

	hotDone := make(chan error, 1)
	go func() {
		_, _, err := hot.Store(ctx, []*protocol.Message{{Data: "hot"}})
		hotDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if _, _, err := cold.Store(ctx, []*protocol.Message{{Data: "cold"}}); err != nil {
		t.Fatalf("cold Store: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("cold publish took %v behind a locked hot channel, want about one commit", took)
	}
	select {
	case err := <-hotDone:
		t.Fatalf("hot publish finished (%v) while its row was locked elsewhere", err)
	default:
	}
	if _, _, deferred := s.BatchCounters(); deferred < 1 {
		t.Errorf("deferred = %v, want the hot publish deferred at least once", deferred)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("release hot: %v", err)
	}
	select {
	case err := <-hotDone:
		if err != nil {
			t.Fatalf("hot Store: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hot publish never committed after its row was released")
	}
}

// TestBatchedTransientFailureRetriesOnce: a failed batch commit is
// retried once and then succeeds; two failures fail every publish in the
// batch with storage.ErrUnavailable and store nothing.
func TestBatchedTransientFailureRetriesOnce(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s := openBatched(t, dsn, postgres.Batching{Lanes: 1})
	ch, err := s.Channel(ctx, "flaky", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	var attempts atomic.Int32
	postgres.SetCommitBatchHook(func() error {
		if attempts.Add(1) == 1 {
			return errors.New("injected: connection reset")
		}
		return nil
	})
	defer postgres.SetCommitBatchHook(nil)
	if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
		t.Fatalf("Store after one transient failure: %v", err)
	}
	if _, retries, _ := s.BatchCounters(); retries != 1 {
		t.Errorf("retries = %v, want 1", retries)
	}

	postgres.SetCommitBatchHook(func() error { return errors.New("injected: primary down") })
	_, _, err = ch.Store(ctx, []*protocol.Message{{Data: "y"}})
	if !errors.Is(err, storage.ErrUnavailable) {
		t.Errorf("Store after two failures: err = %v, want storage.ErrUnavailable", err)
	}
	postgres.SetCommitBatchHook(nil)
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'flaky'`); n != 1 {
		t.Errorf("rows stored = %d, want 1 (the failed publish stored nothing)", n)
	}
}

// TestBatchedOverlappingChannelSetsDoNotDeadlock: two nodes publish to
// the same channels in opposite orders as fast as they can. Channel rows
// are locked in sorted order, so no batch ever deadlocks (a deadlock
// would surface as a retried batch).
func TestBatchedOverlappingChannelSetsDoNotDeadlock(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	nodes := []*postgres.Storage{openBatched(t, dsn, postgres.Batching{Lanes: 1}), openBatched(t, dsn, postgres.Batching{Lanes: 1})}
	names := []string{"a", "b", "c", "d", "e", "f"}

	var wg sync.WaitGroup
	for n, node := range nodes {
		for w := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 60 {
					k := (w + i) % len(names)
					if n == 1 {
						k = len(names) - 1 - k
					}
					ch, err := node.Channel(ctx, names[k], nil)
					if err != nil {
						t.Errorf("Channel: %v", err)
						return
					}
					if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
						t.Errorf("Store: %v", err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	for i, node := range nodes {
		if _, retries, _ := node.BatchCounters(); retries != 0 {
			t.Errorf("node %d retried %v batches, want 0 (a deadlock or failed commit)", i, retries)
		}
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages`); n != 2*6*60 {
		t.Errorf("rows = %d, want %d", n, 2*6*60)
	}
}

// TestBatchedRetryAfterLostCommitReplyDoesNotDuplicate: the batch's
// COMMIT reaches the database but its reply is lost, so the lane retries
// the batch. The retry must find every publish already stored (server-
// generated ids included) and return the originals, not store them again
// under new serials.
func TestBatchedRetryAfterLostCommitReplyDoesNotDuplicate(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s := openBatched(t, dsn, postgres.Batching{Lanes: 1})
	ch, err := s.Channel(ctx, "lost-reply", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	var once atomic.Bool
	postgres.SetCommitBatchAfterHook(func() error {
		if once.CompareAndSwap(false, true) {
			return errors.New("injected: COMMIT reply lost")
		}
		return nil
	})
	defer postgres.SetCommitBatchAfterHook(nil)

	cm, _, err := ch.Store(ctx, []*protocol.Message{{Data: "no client id"}})
	if err != nil {
		t.Fatalf("Store across a lost COMMIT reply: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'lost-reply'`); n != 1 {
		t.Errorf("rows stored = %d, want 1 (the retry must dedupe the committed first attempt)", n)
	}
	if got := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'lost-reply' AND channel_serial = $1`, cm.ChannelSerial); got != 1 {
		t.Errorf("returned serial %s is not the stored one", cm.ChannelSerial)
	}
}

// TestBatchedBadIDFailsOnlyItsPublish: an id Postgres cannot store (a
// NUL byte, invalid UTF-8) is refused before the publish is queued, so it
// cannot fail the batch of other publishes it would have joined.
func TestBatchedBadIDFailsOnlyItsPublish(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s := openBatched(t, dsn, postgres.Batching{Lanes: 1})
	ch, err := s.Channel(ctx, "bad-id", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	for _, id := range []string{"a\x00b", "\xff\xfe"} {
		if _, _, err := ch.Store(ctx, []*protocol.Message{{ID: id}}); !errors.Is(err, storage.ErrInvalidMessageID) {
			t.Errorf("Store with id %q: err = %v, want storage.ErrInvalidMessageID", id, err)
		}
	}
	if _, _, err := ch.Store(ctx, []*protocol.Message{{ID: "fine"}}); err != nil {
		t.Errorf("a good publish after bad ones: %v", err)
	}
}

// TestBatchedNewChannelsFromTwoNodesDoNotDeadlock: two nodes publish at
// once to the same set of channels that have no rows yet, in opposite
// orders. The rows are created before publishes are queued, and a batch
// never waits while holding a channel row it skipped others for, so no
// batch deadlocks or fails.
func TestBatchedNewChannelsFromTwoNodesDoNotDeadlock(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	nodes := []*postgres.Storage{openBatched(t, dsn, postgres.Batching{Lanes: 1}), openBatched(t, dsn, postgres.Batching{Lanes: 1})}
	var names []string
	for i := range 12 {
		names = append(names, fmt.Sprintf("fresh-%02d", i))
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for n, node := range nodes {
		for w := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := range 40 {
					k := (w*3 + i) % len(names)
					if n == 1 {
						k = len(names) - 1 - k
					}
					ch, err := node.Channel(ctx, names[k], nil)
					if err != nil {
						t.Errorf("Channel: %v", err)
						return
					}
					if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
						t.Errorf("Store: %v", err)
						return
					}
				}
			}()
		}
	}
	close(start)
	wg.Wait()
	var deferred float64
	for i, node := range nodes {
		_, retries, d := node.BatchCounters()
		deferred += d
		if retries != 0 {
			t.Errorf("node %d retried %v batches, want 0 (a deadlock or failed commit)", i, retries)
		}
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages`); n != 2*6*40 {
		t.Errorf("rows = %d, want %d", n, 2*6*40)
	}
	t.Logf("deferrals under contention: %v", deferred)
}
