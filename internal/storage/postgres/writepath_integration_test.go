//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestConcurrentPublishesWithSameIDDedupe is the regression test for the
// idempotency race the claims audit found: the id lookup used to run
// before the channels-row lock, so two concurrent publishes carrying the
// same id could both miss it. The second then failed on the UNIQUE index
// (a spurious error rather than the idempotent result). Every publish
// must now succeed, exactly one must be fresh, and all must return the
// same cm.
func TestConcurrentPublishesWithSameIDDedupe(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	// Two nodes, so the race spans pools as it would in a cluster.
	var nodes []*postgres.Storage
	for range 2 {
		s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		nodes = append(nodes, s)
	}

	const rounds = 5
	const writers = 16
	for round := range rounds {
		id := "dup-" + string(rune('a'+round))
		var (
			start = make(chan struct{})
			wg    sync.WaitGroup
			mu    sync.Mutex
			fresh int
			sers  = map[string]int{}
			errs  []error
		)
		for w := range writers {
			ch, err := nodes[w%2].Channel(ctx, "idem", nil)
			if err != nil {
				t.Fatalf("Channel: %v", err)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				cm, idem, err := ch.Store(ctx, []*protocol.Message{{ID: id, Data: "x"}})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				if !idem {
					fresh++
				}
				sers[cm.ChannelSerial]++
			}()
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			t.Errorf("round %d: concurrent Store with a shared id failed: %v", round, err)
		}
		if fresh != 1 {
			t.Errorf("round %d: fresh publishes = %d, want exactly 1", round, fresh)
		}
		if len(sers) != 1 {
			t.Errorf("round %d: publishes returned %d distinct channelSerials %v, want 1", round, len(sers), sers)
		}
		if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'idem' AND id = $1`, id); n != 1 {
			t.Errorf("round %d: rows stored for id %q = %d, want 1", round, id, n)
		}
	}
}

// countRows runs a count(*) query on a fresh connection.
func countRows(t *testing.T, dsn, query string, args ...any) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// TestConcurrentMutationsPresenceAnnotationsWithSameIDDedupe covers the
// lock-first idempotency order in Mutate, StorePresence and
// StoreAnnotation, the other three write paths that check ids.
func TestConcurrentMutationsPresenceAnnotationsWithSameIDDedupe(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ch, err := s.Channel(ctx, "kinds", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	target, _, err := ch.Store(ctx, []*protocol.Message{{Name: "target", Data: "v1"}})
	if err != nil {
		t.Fatalf("Store target: %v", err)
	}
	targetSerial := target.Messages[0].Serial

	writes := map[string]func(id string) (*protocol.ChannelMessage, bool, error){
		"mutate": func(id string) (*protocol.ChannelMessage, bool, error) {
			return ch.Mutate(ctx, &protocol.Message{ID: id, Action: protocol.MessageUpdate, Serial: targetSerial, Data: "v2"})
		},
		"presence": func(id string) (*protocol.ChannelMessage, bool, error) {
			return ch.StorePresence(ctx, []*protocol.PresenceMessage{{ID: id, Action: protocol.PresenceEnter, ClientID: "c", ConnectionID: "conn"}})
		},
		"annotation": func(id string) (*protocol.ChannelMessage, bool, error) {
			return ch.StoreAnnotation(ctx, []*protocol.Annotation{{ID: id, Action: protocol.AnnotationCreate, ClientID: "c", Type: "reaction:distinct.v1", Name: "+1", MessageSerial: targetSerial}})
		},
	}
	for kind, write := range writes {
		t.Run(kind, func(t *testing.T) {
			id := "same-" + kind
			const writers = 12
			var (
				start = make(chan struct{})
				wg    sync.WaitGroup
				mu    sync.Mutex
				fresh int
				sers  = map[string]bool{}
			)
			for range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					cm, idem, err := write(id)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						t.Errorf("%s with a shared id failed: %v", kind, err)
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
				t.Errorf("%s: fresh = %d, distinct serials = %d; want 1 and 1", kind, fresh, len(sers))
			}
			if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'kinds' AND id = $1`, id); n != 1 {
				t.Errorf("%s: rows stored for id %q = %d, want 1", kind, id, n)
			}
		})
	}
}
