//go:build integration

package postgres

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
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// TestPresenceUnbatchedContract runs the storage contract suite with
// publish batching on but presence committed unbatched
// (--presence-batching=false, DESIGN.md §12.5), on the default and the
// nats bus. TestBatchedChannelStoreContractEveryBus covers presence
// batched on every bus; TestPostgresChannelStoreContract with no lanes.
func TestPresenceUnbatchedContract(t *testing.T) {
	c := pgtest.Start(t)
	lanes := Batching{Lanes: 4, PresenceUnbatched: true}
	for name, opts := range map[string]func(dsn string) Options{
		"pgnotify": func(dsn string) Options { return Options{DSN: dsn, Batching: lanes} },
		"nats": func(dsn string) Options {
			return Options{DSN: dsn, Bus: BusNATS, NATSURL: natstest.Start(t).URL, Batching: lanes}
		},
	} {
		t.Run(name, func(t *testing.T) {
			storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
				return openOpts(t, opts(c.FreshSchemaDSN(t)))
			})
		})
	}
}

// TestPresenceBatchedConcurrentEnters: 2,000 ENTERs into one room from 4
// nodes, all batched. Each ENTER is committed in exactly one batch, with
// its own channelSerial, and none is lost: the store's set and each
// node's delivered stream hold all 2,000 members. Then every member
// leaves, and the set is empty. The row lock is taken once per batch,
// not per ENTER, so the ENTERs share far fewer commits than there are
// ENTERs.
func TestPresenceBatchedConcurrentEnters(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	dsn := c.FreshSchemaDSN(t)
	const nodes, members, workers = 4, 2000, 25 // workers per node

	var ns []*Storage
	var stores []storage.ChannelStore
	var recs []*presenceRecorder
	for range nodes {
		s := openOpts(t, Options{DSN: dsn, Batching: Batching{Lanes: 4}})
		rec := &presenceRecorder{}
		cs, err := s.Channel(ctx, "hot-room", rec)
		if err != nil {
			t.Fatalf("Channel: %v", err)
		}
		ns, stores, recs = append(ns, s), append(stores, cs), append(recs, rec)
	}

	run := func(action protocol.PresenceAction) map[string]bool {
		var (
			mu      sync.Mutex
			serials = map[string]bool{}
			wg      sync.WaitGroup
			next    atomic.Int32
		)
		for n := range nodes {
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1)) - 1
						if i >= members {
							return
						}
						cm, idem, err := stores[n].StorePresence(ctx, []*protocol.PresenceMessage{{
							ID:     fmt.Sprintf("conn-%d:%d:0", i, action),
							Action: action, ConnectionID: fmt.Sprintf("conn-%d", i), ClientID: fmt.Sprintf("client-%d", i),
						}})
						if err != nil || idem {
							t.Errorf("StorePresence %d: idempotent=%v err=%v", i, idem, err)
							return
						}
						mu.Lock()
						if serials[cm.ChannelSerial] {
							t.Errorf("serial %s minted twice", cm.ChannelSerial)
						}
						serials[cm.ChannelSerial] = true
						mu.Unlock()
					}
				}()
			}
		}
		wg.Wait()
		return serials
	}

	entered := run(protocol.PresenceEnter)
	if len(entered) != members {
		t.Fatalf("%d distinct serials for %d ENTERs", len(entered), members)
	}
	got, _, err := stores[0].Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(got) != members {
		t.Fatalf("store set has %d members, want %d", len(got), members)
	}
	var commits, sum float64
	for _, s := range ns {
		n, sz := s.BatchSizeSum()
		commits += float64(n)
		sum += sz
	}
	if sum != members {
		t.Errorf("batches committed %v ENTERs, want each of %d exactly once", sum, members)
	}
	if commits >= members {
		t.Errorf("%v commits for %d ENTERs: not batched", commits, members)
	}
	t.Logf("%d ENTERs from %d nodes in %v commits", members, nodes, commits)
	for i, rec := range recs {
		waitFor(t, 20*time.Second, fmt.Sprintf("node %d to deliver every ENTER", i), func() bool {
			return rec.count(protocol.PresenceEnter) >= members
		})
		if n := rec.count(protocol.PresenceEnter); n != members {
			t.Errorf("node %d delivered %d ENTERs, want %d", i, n, members)
		}
	}

	left := run(protocol.PresenceLeave)
	if len(left) != members {
		t.Fatalf("%d distinct serials for %d LEAVEs", len(left), members)
	}
	got, _, err = stores[0].Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("store set has %d members after every member left", len(got))
	}
}

// TestPresenceBatchFoldsOperationsInOrder: operations on one member
// that land in one batch fold in batch order. A commit hook holds the
// lane's first batch while the next operations queue, so they commit
// together: ENTER then LEAVE removes the member; LEAVE then ENTER keeps
// it, with the later ENTER's data.
func TestPresenceBatchFoldsOperationsInOrder(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	s := openOpts(t, Options{DSN: c.FreshSchemaDSN(t), Batching: Batching{Lanes: 1}})
	cs, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	enter := func(conn, client, data string) []*protocol.PresenceMessage {
		return []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ConnectionID: conn, ClientID: client, Data: data}}
	}
	leave := func(conn, client string) []*protocol.PresenceMessage {
		return []*protocol.PresenceMessage{{Action: protocol.PresenceLeave, ConnectionID: conn, ClientID: client}}
	}

	gate := make(chan struct{})
	var first atomic.Bool
	SetCommitBatchHook(func() error {
		if first.CompareAndSwap(false, true) {
			<-gate
		}
		return nil
	})
	defer SetCommitBatchHook(nil)

	var wg sync.WaitGroup
	store := func(p []*protocol.PresenceMessage) {
		defer wg.Done()
		if _, _, err := cs.StorePresence(ctx, p); err != nil {
			t.Errorf("StorePresence: %v", err)
		}
	}
	// The held first batch: an unrelated member.
	wg.Add(1)
	go store(enter("c0", "zed", "z"))
	waitFor(t, 5*time.Second, "the first batch to start", first.Load)
	// Queued behind it, in this order, then committed as one batch. The
	// lane keeps each channel's publishes in queue order, so submit them
	// one at a time.
	ops := [][]*protocol.PresenceMessage{
		enter("c1", "alice", "a1"), leave("c1", "alice"),
		leave("c2", "bob"), enter("c2", "bob", "b2"),
	}
	lane := s.lanes.laneFor("room")
	for i, p := range ops {
		wg.Add(1)
		go store(p)
		waitFor(t, 5*time.Second, fmt.Sprintf("operation %d to queue", i), func() bool {
			lane.mu.Lock()
			defer lane.mu.Unlock()
			return len(lane.queue) == i+1
		})
	}
	close(gate)
	wg.Wait()

	if n, _ := s.BatchSizeSum(); n != 2 {
		t.Errorf("%d batches, want 2 (the held one, then the four queued operations together)", n)
	}
	members, _, err := cs.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	got := map[string]any{}
	for _, m := range members {
		got[m.ClientID] = m.Data
	}
	if _, ok := got["alice"]; ok {
		t.Error("alice entered then left in one batch, but is still a member")
	}
	if got["bob"] != "b2" {
		t.Errorf("bob left then re-entered in one batch: member data = %v, want b2", got["bob"])
	}
	if got["zed"] != "z" {
		t.Errorf("zed = %v, want z", got["zed"])
	}
}

// TestPresenceInflightBoundRefusesPromptly: with unbatched presence
// bounded to 2 in flight and the room's row lock held by another
// transaction, two ENTERs wait on the lock; a third is refused at once
// with storage.ErrOverloaded rather than queued for a pool connection,
// and SYNC reads (Members), binds and publishes on other channels still
// complete. Once the lock is released the two waiting ENTERs commit. It
// runs with batching off, and with batching on but presence unbatched.
func TestPresenceInflightBoundRefusesPromptly(t *testing.T) {
	c := pgtest.Start(t)
	for name, b := range map[string]Batching{
		"no-lanes":           {},
		"presence-unbatched": {Lanes: 4, PresenceUnbatched: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dsn := c.FreshSchemaDSN(t)
			s := openOpts(t, Options{DSN: dsn, Batching: b, PresenceMaxInflight: 2})
			room, err := s.Channel(ctx, "room", nil)
			if err != nil {
				t.Fatalf("Channel: %v", err)
			}
			if _, _, err := room.StorePresence(ctx, []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ConnectionID: "c0", ClientID: "zed"}}); err != nil {
				t.Fatalf("first ENTER: %v", err)
			}

			// Stall the room: hold its row lock from outside.
			locker, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer locker.Close(ctx)
			tx, err := locker.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if _, err := tx.Exec(ctx, `SELECT 1 FROM channels WHERE name = 'room' FOR UPDATE`); err != nil {
				t.Fatalf("lock: %v", err)
			}

			errs := make(chan error, 2)
			for i := 1; i <= 2; i++ {
				go func() {
					_, _, err := room.StorePresence(ctx, []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ConnectionID: fmt.Sprintf("c%d", i), ClientID: fmt.Sprintf("m%d", i)}})
					errs <- err
				}()
			}
			waitFor(t, 5*time.Second, "two ENTERs to be in flight", func() bool { return len(s.presenceSlots) == 2 })

			start := time.Now()
			_, _, err = room.StorePresence(ctx, []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ConnectionID: "c3", ClientID: "m3"}})
			if !errors.Is(err, storage.ErrOverloaded) {
				t.Fatalf("third ENTER: err = %v, want storage.ErrOverloaded", err)
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("third ENTER refused after %v, want at once", d)
			}

			// Attach-path work is not blocked behind the convoy.
			opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, _, err := room.Members(opCtx); err != nil {
				t.Errorf("Members during the stall: %v", err)
			}
			other, err := s.Channel(opCtx, "other", nil)
			if err != nil {
				t.Fatalf("bind another channel during the stall: %v", err)
			}
			if _, _, err := other.Store(opCtx, []*protocol.Message{{Data: "x"}}); err != nil {
				t.Errorf("publish to another channel during the stall: %v", err)
			}

			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("release lock: %v", err)
			}
			for range 2 {
				if err := <-errs; err != nil {
					t.Errorf("waiting ENTER: %v", err)
				}
			}
			members, _, err := room.Members(ctx)
			if err != nil {
				t.Fatalf("Members: %v", err)
			}
			if len(members) != 3 {
				t.Errorf("%d members, want zed, m1 and m2", len(members))
			}
		})
	}
}

// TestPresenceMembersAsOfIsExact: Members' set is exactly the fold of
// the presence log up to its as-of serial, while presence operations
// and publishes run concurrently, batched and not (DESIGN.md §12.4). A
// node seeding its local member set relies on this.
func TestPresenceMembersAsOfIsExact(t *testing.T) {
	c := pgtest.Start(t)
	for name, b := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := openOpts(t, Options{DSN: c.FreshSchemaDSN(t), Batching: b})
			room, err := s.Channel(ctx, "room", nil)
			if err != nil {
				t.Fatalf("Channel: %v", err)
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for w := range 6 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						if w == 0 {
							_, _, _ = room.Store(ctx, []*protocol.Message{{Data: i}})
							continue
						}
						action := protocol.PresenceEnter
						if i%3 == 2 {
							action = protocol.PresenceLeave
						}
						m := fmt.Sprintf("m%d", (w*7+i)%12)
						if _, _, err := room.StorePresence(ctx, []*protocol.PresenceMessage{{Action: action, ConnectionID: "conn-" + m, ClientID: m, Data: i}}); err != nil {
							t.Errorf("StorePresence: %v", err)
							return
						}
					}
				}()
			}

			type read struct {
				members []*protocol.PresenceMessage
				asOf    string
			}
			var reads []read
			for range 40 {
				members, asOf, err := room.Members(ctx)
				if err != nil {
					t.Fatalf("Members: %v", err)
				}
				reads = append(reads, read{members, asOf})
				time.Sleep(5 * time.Millisecond)
			}
			close(stop)
			wg.Wait()

			page, err := room.History(ctx, storage.HistoryQuery{Kind: storage.KindPresence, Direction: storage.DirectionForwards})
			if err != nil {
				t.Fatalf("History: %v", err)
			}
			for i, r := range reads {
				want := map[string]any{}
				for _, cm := range page.ChannelMessages {
					if cm.ChannelSerial > r.asOf {
						break
					}
					for _, p := range cm.Presence {
						key := storage.MemberKey(p.ConnectionID, p.ClientID)
						if p.Action == protocol.PresenceLeave {
							delete(want, key)
						} else {
							want[key] = p.Data
						}
					}
				}
				got := map[string]any{}
				for _, m := range r.members {
					got[storage.MemberKey(m.ConnectionID, m.ClientID)] = m.Data
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("read %d as of %s: set %v, log fold %v", i, r.asOf, got, want)
				}
			}
		})
	}
}

func (r *presenceRecorder) count(action protocol.PresenceAction) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.presence {
		if p.Action == action {
			n++
		}
	}
	return n
}
