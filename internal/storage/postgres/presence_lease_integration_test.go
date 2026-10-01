//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// Per-node presence leases (DESIGN.md §12.5, --presence-lease-mode).

// openLeaseNode opens a Storage in the given lease mode with an
// optional fixed node id. The caller closes it (after any timing
// restore it deferred earlier, so no loop reads the timing vars while
// they are restored).
func openLeaseNode(t *testing.T, dsn, mode, nodeID string, batching Batching) *Storage {
	t.Helper()
	s, err := Open(context.Background(), Options{DSN: dsn, Batching: batching, PresenceLeaseMode: mode, nodeID: nodeID})
	if err != nil {
		t.Fatalf("Open (%s): %v", mode, err)
	}
	return s
}

// enterMembers enters n members (client "<prefix>-<i>", connection
// "conn-<prefix>-<i>") spread over channels, one presence write each.
func enterMembers(t *testing.T, s *Storage, channels []string, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		ch, err := s.Channel(ctx, channels[i%len(channels)], nil)
		if err != nil {
			t.Fatalf("Channel: %v", err)
		}
		id := fmt.Sprintf("%s-%d", prefix, i)
		if _, _, err := ch.StorePresence(ctx, []*protocol.PresenceMessage{{
			Action: protocol.PresenceEnter, ClientID: id, ConnectionID: "conn-" + id,
		}}); err != nil {
			t.Fatalf("enter %s: %v", id, err)
		}
	}
}

// memberRowVersions returns, for every presence row node owns, its
// physical version (xmin and ctid): any UPDATE of the row changes both.
func memberRowVersions(t *testing.T, s *Storage, node string) map[string]string {
	t.Helper()
	rows, err := s.pool.Query(context.Background(),
		`SELECT channel || '/' || client_id, xmin::text || '@' || ctid::text FROM presence WHERE node_id = $1`, node)
	if err != nil {
		t.Fatalf("read presence rows: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// nodeLease returns node's lease expiry and whether it has a lease row.
func nodeLease(t *testing.T, s *Storage, node string) (time.Time, bool) {
	t.Helper()
	var exp time.Time
	err := s.pool.QueryRow(context.Background(), `SELECT expires_at FROM presence_nodes WHERE node_id = $1`, node).Scan(&exp)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, false
		}
		t.Fatalf("read lease of %s: %v", node, err)
	}
	return exp, true
}

func countPresenceRows(t *testing.T, s *Storage, where string, args ...any) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM presence WHERE `+where, args...).Scan(&n); err != nil {
		t.Fatalf("count presence rows: %v", err)
	}
	return n
}

// TestNodeLeaseBumpLeavesMemberRows: in node lease mode the bump renews
// the node's one presence_nodes row and never writes a member row (every
// member row keeps its physical version, xmin and ctid, across many
// bumps), and the node's members outlive several lease windows while
// another node's reaper runs. The member lease mode is the control: its
// bump rewrites every row, so the check would see it.
func TestNodeLeaseBumpLeavesMemberRows(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) {
		for name, batching := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
			t.Run(name, func(t *testing.T) {
				defer swapPresenceTimings(1*time.Second, 100*time.Millisecond, 100*time.Millisecond)()
				dsn := pgtest.Start(t).FreshSchemaDSN(t)
				sa := openLeaseNode(t, dsn, mode, "", batching)
				defer func() { _ = sa.Close() }()
				sb := openLeaseNode(t, dsn, mode, "", batching) // reaps
				defer func() { _ = sb.Close() }()

				const n = 40
				enterMembers(t, sa, []string{"room-1", "room-2", "room-3"}, "m", n)
				before := memberRowVersions(t, sa, sa.node)
				if len(before) != n {
					t.Fatalf("node A owns %d rows, want %d", len(before), n)
				}
				lease0, _ := nodeLease(t, sa, sa.node)

				// Several bump intervals and more than two lease windows.
				time.Sleep(2*presenceLeaseWindow + 5*presenceLeaseBumpInterval)

				after := memberRowVersions(t, sa, sa.node)
				if len(after) != n {
					t.Fatalf("node A owns %d rows after %v, want all %d (a live node's members were reaped)",
						len(after), 2*presenceLeaseWindow, n)
				}
				changed := 0
				for k, v := range before {
					if after[k] != v {
						changed++
					}
				}
				switch mode {
				case PresenceLeaseNode:
					if changed != 0 {
						t.Errorf("node lease bumps rewrote %d of %d member rows, want 0", changed, n)
					}
					if got := countPresenceRows(t, sa, `node_id = $1 AND expires_at <> 'infinity'`, sa.node); got != 0 {
						t.Errorf("%d member rows carry a finite lease in node mode, want 0", got)
					}
					lease1, ok := nodeLease(t, sa, sa.node)
					if !ok || !lease1.After(lease0) {
						t.Errorf("node lease = %v (row %v), want renewed past %v", lease1, ok, lease0)
					}
				case PresenceLeaseMember:
					if changed != n {
						t.Errorf("member lease bumps rewrote %d of %d member rows, want all (the control)", changed, n)
					}
					if _, ok := nodeLease(t, sa, sa.node); ok {
						t.Error("member lease mode wrote a presence_nodes row")
					}
				}
			})
		}
	})
}

// TestCrashedNodeMembersReapedInChunks: node A (node lease mode) owns
// members on two channels and crashes; node B's reaper removes all of
// them, in ten chunks within one reaper round (one chunk per round would
// take five seconds), within the lease window plus two reaper rounds,
// publishes exactly one LEAVE for each, and then deletes A's lease row.
// B's own lease row stays.
func TestCrashedNodeMembersReapedInChunks(t *testing.T) {
	for name, batching := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
		t.Run(name, func(t *testing.T) {
			defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 500*time.Millisecond)()
			oldChunk := reapChunk
			reapChunk = 3
			defer func() { reapChunk = oldChunk }()

			ctx := context.Background()
			dsn := pgtest.Start(t).FreshSchemaDSN(t)
			sa := openLeaseNode(t, dsn, PresenceLeaseNode, "", batching)
			defer func() { _ = sa.Close() }()
			sb := openLeaseNode(t, dsn, PresenceLeaseNode, "", batching)
			defer func() { _ = sb.Close() }()
			channels := []string{"room-a", "room-b"}
			recs := map[string]*presenceRecorder{}
			for _, name := range channels {
				recs[name] = &presenceRecorder{}
				if _, err := sb.Channel(ctx, name, recs[name]); err != nil {
					t.Fatalf("Channel node B: %v", err)
				}
			}

			const n = 30
			enterMembers(t, sa, channels, "m", n)
			if got := countPresenceRows(t, sb, `node_id = $1`, sa.node); got != n {
				t.Fatalf("node A owns %d rows, want %d", got, n)
			}
			start := time.Now()
			crash(t, sa)

			waitFor(t, 10*time.Second, "node A's members to be reaped", func() bool {
				return countPresenceRows(t, sb, `node_id = $1`, sa.node) == 0
			})
			if took, bound := time.Since(start), presenceLeaseWindow+2*presenceReaperInterval+time.Second; took > bound {
				t.Errorf("reaping took %v, want within the lease window plus two reaper rounds (%v)", took, bound)
			}
			waitFor(t, 10*time.Second, "a LEAVE for every member", func() bool {
				total := 0
				for i := range n {
					total += min(1, recs[channels[i%len(channels)]].leaveCount(fmt.Sprintf("m-%d", i)))
				}
				return total == n
			})
			waitFor(t, 10*time.Second, "node A's lease row to be deleted", func() bool {
				_, ok := nodeLease(t, sb, sa.node)
				return !ok
			})
			time.Sleep(2 * presenceReaperInterval)
			for i := range n {
				id := fmt.Sprintf("m-%d", i)
				if got := recs[channels[i%len(channels)]].leaveCount(id); got != 1 {
					t.Errorf("%s: %d LEAVEs, want exactly 1", id, got)
				}
			}
			if _, ok := nodeLease(t, sb, sb.node); !ok {
				t.Error("live node B lost its lease row")
			}
		})
	}
}

// TestRestartedNodeKeepsMembers: node ids are per process in production,
// but a Storage reopened under the same id (Options.nodeID) right after a
// crash takes the lease over in Open, before the old one lapses, so a
// third node's reaper never removes the members that id owns, however
// long the new process runs. When the new process then closes
// gracefully it deletes the lease row, and the members it still owns
// are reaped (with their LEAVEs) at the next reaper round rather than a
// lease window later.
func TestRestartedNodeKeepsMembers(t *testing.T) {
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	const id = "node-restart"

	sa := openLeaseNode(t, dsn, PresenceLeaseNode, id, Batching{})
	defer func() { _ = sa.Close() }()
	sc := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{}) // the reaper
	defer func() { _ = sc.Close() }()
	rec := &presenceRecorder{}
	if _, err := sc.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("Channel node C: %v", err)
	}
	enterMembers(t, sa, []string{"room"}, "alice", 1)

	crash(t, sa)
	sb := openLeaseNode(t, dsn, PresenceLeaseNode, id, Batching{})
	defer func() { _ = sb.Close() }()

	time.Sleep(3 * presenceLeaseWindow)
	if got := countPresenceRows(t, sc, `node_id = $1`, id); got != 1 {
		t.Fatalf("restarted node owns %d rows after %v, want its 1 member kept", got, 3*presenceLeaseWindow)
	}
	if got := rec.leaveCount("alice-0"); got != 0 {
		t.Fatalf("%d LEAVEs for a member of the restarted node, want 0", got)
	}

	// A graceful close ends the lease: the row stays, expired, while the
	// node still owns a member, which is reaped promptly, and then goes.
	start := time.Now()
	if err := sb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, 10*time.Second, "the closed node's member to be reaped", func() bool {
		return countPresenceRows(t, sc, `node_id = $1`, id) == 0
	})
	if took := time.Since(start); took >= presenceLeaseWindow {
		t.Errorf("reaping after a graceful close took %v, want under the lease window %v", took, presenceLeaseWindow)
	}
	waitFor(t, 10*time.Second, "the reaped member's LEAVE", func() bool { return rec.leaveCount("alice-0") == 1 })
	waitFor(t, 10*time.Second, "the closed node's lease row to be deleted", func() bool {
		_, ok := nodeLease(t, sc, id)
		return !ok
	})
}

// TestGracefulCloseDeletesLease: a node-mode node that owns no member
// when it closes gracefully deletes its lease row at once.
func TestGracefulCloseDeletesLease(t *testing.T) {
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	sa := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{})
	defer func() { _ = sa.Close() }()
	sb := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{})
	defer func() { _ = sb.Close() }()
	if _, ok := nodeLease(t, sb, sa.node); !ok {
		t.Fatal("Open took no lease row")
	}
	if err := sa.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, ok := nodeLease(t, sb, sa.node); ok {
		t.Fatal("graceful Close of a node owning no member left its lease row")
	}
}

// TestPresenceLeaseModesMixed: nodes in different lease modes sharing a
// database (a rolling switch of --presence-lease-mode) never reap each
// other's live members, and each mode's reaper removes the other mode's
// members once their node is dead.
func TestPresenceLeaseModesMixed(t *testing.T) {
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)

	sn := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{Lanes: 4})
	defer func() { _ = sn.Close() }()
	sm := openLeaseNode(t, dsn, PresenceLeaseMember, "", Batching{Lanes: 4})
	defer func() { _ = sm.Close() }()
	enterMembers(t, sn, []string{"room"}, "n", 3)
	enterMembers(t, sm, []string{"room"}, "m", 3)

	time.Sleep(3 * presenceLeaseWindow)
	if got := countPresenceRows(t, sn, `channel = 'room'`); got != 6 {
		t.Fatalf("%d members after %v with both nodes alive, want 6", got, 3*presenceLeaseWindow)
	}

	// The member-mode node dies: the node-mode reaper takes its rows once
	// their own leases lapse.
	crash(t, sm)
	waitFor(t, 10*time.Second, "the member-mode node's members to be reaped", func() bool {
		return countPresenceRows(t, sn, `node_id = $1`, sm.node) == 0
	})
	if got := countPresenceRows(t, sn, `node_id = $1`, sn.node); got != 3 {
		t.Fatalf("the live node-mode node owns %d members, want 3", got)
	}

	// A fresh member-mode node; then the node-mode node dies and the
	// member-mode reaper takes its 'infinity' rows and its lease row.
	sm2 := openLeaseNode(t, dsn, PresenceLeaseMember, "", Batching{Lanes: 4})
	defer func() { _ = sm2.Close() }()
	rec := &presenceRecorder{}
	if _, err := sm2.Channel(ctx, "room", rec); err != nil {
		t.Fatalf("Channel: %v", err)
	}
	crash(t, sn)
	waitFor(t, 10*time.Second, "the node-mode node's members to be reaped", func() bool {
		return countPresenceRows(t, sm2, `node_id = $1`, sn.node) == 0
	})
	waitFor(t, 10*time.Second, "their LEAVEs", func() bool {
		return rec.leaveCount("n-0")+rec.leaveCount("n-1")+rec.leaveCount("n-2") == 3
	})
	waitFor(t, 10*time.Second, "the dead node's lease row to be deleted", func() bool {
		_, ok := nodeLease(t, sm2, sn.node)
		return !ok
	})
}

// TestFixtureMembersSurviveNodeModeReaper: a static fixture member is
// owned by the sentinel, which has no lease row; the node-mode reaper,
// which treats any owner without a live lease as dead, must still leave
// it alone (and must not list the sentinel as a dead node).
func TestFixtureMembersSurviveNodeModeReaper(t *testing.T) {
	defer swapPresenceTimings(1*time.Second, 100*time.Millisecond, 100*time.Millisecond)()
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	s := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{Lanes: 4})
	defer func() { _ = s.Close() }()
	ch, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, _, err := ch.StorePresence(storage.WithStaticPresence(ctx), []*protocol.PresenceMessage{{
		Action: protocol.PresenceEnter, ClientID: "fixture", ConnectionID: "connFixture",
	}}); err != nil {
		t.Fatalf("seed fixture: %v", err)
	}
	time.Sleep(3 * presenceLeaseWindow)
	if !memberPresent(t, ctx, ch, "fixture") {
		t.Fatal("fixture member was reaped by the node-mode reaper")
	}
	rows, err := s.pool.Query(ctx, sqlDeadNodes, fixtureNodeID, deadLeaseGrace())
	if err != nil {
		t.Fatalf("dead nodes: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Errorf("dead-node query lists %q, want nothing", id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dead nodes: %v", err)
	}
}

// TestShardedNodeLeaseOnEveryShard: a sharded node has one node id and
// holds its presence lease row in every shard's database, beside the
// members it owns there (DESIGN.md §6.4, §12.5).
func TestShardedNodeLeaseOnEveryShard(t *testing.T) {
	dsns := shardDSNs(t, 2)
	s, err := OpenSharded(context.Background(), Options{}, dsns)
	if err != nil {
		t.Fatalf("OpenSharded: %v", err)
	}
	defer func() { _ = s.Close() }()
	node := s.Shard(0).node
	for i := range s.Shards() {
		sh := s.Shard(i)
		if sh.node != node {
			t.Errorf("shard %d node id %q, want %q (one id per node)", i, sh.node, node)
		}
		if _, ok := nodeLease(t, sh, node); !ok {
			t.Errorf("shard %d has no lease row for node %s", i, node)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestReapChunkSparesLiveNode: the chunk DELETE itself re-checks the
// lease, so it takes nothing from a node whose lease is live (as when a
// node renews while another node is part way through reaping it), and
// takes its members once the lease has expired.
func TestReapChunkSparesLiveNode(t *testing.T) {
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, time.Hour)() // no reaper round runs
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	sa := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{})
	defer func() { _ = sa.Close() }()
	sb := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{})
	defer func() { _ = sb.Close() }()
	enterMembers(t, sa, []string{"room"}, "m", 5)

	got, err := sb.deleteReturning(ctx, sqlReapNodeChunk, sa.node, 1000, deadLeaseGrace())
	if err != nil || len(got) != 0 {
		t.Fatalf("chunk DELETE of a live node took %d rows (err %v), want 0", len(got), err)
	}
	crash(t, sa)
	if _, err := sb.pool.Exec(ctx, `UPDATE presence_nodes SET expires_at = now() - interval '1 second' WHERE node_id = $1`, sa.node); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	got, err = sb.deleteReturning(ctx, sqlReapNodeChunk, sa.node, 1000, deadLeaseGrace())
	if err != nil || len(got) != 5 {
		t.Fatalf("chunk DELETE of a dead node took %d rows (err %v), want 5", len(got), err)
	}
}

// TestLeaseBumpDoesNotBlockPresenceBatches holds a lease bump's
// transaction open and runs batched presence UPDATEs of members the node
// owns meanwhile. In node lease mode the bump writes only the node's
// presence_nodes row, so every UPDATE commits at once. In member lease
// mode (the control) the bump has row-locked every member row the node
// owns, so the UPDATEs wait for it: the convoy fleet B measured.
func TestLeaseBumpDoesNotBlockPresenceBatches(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) {
		// No background bump or reaper round runs during the test.
		defer swapPresenceTimings(time.Hour, time.Hour, time.Hour)()
		ctx := context.Background()
		dsn := pgtest.Start(t).FreshSchemaDSN(t)
		s := openLeaseNode(t, dsn, mode, "", Batching{Lanes: 4})
		defer func() { _ = s.Close() }()
		enterMembers(t, s, []string{"room"}, "m", 50)
		ch, err := s.Channel(ctx, "room", nil)
		if err != nil {
			t.Fatalf("Channel: %v", err)
		}

		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		bump, args := sqlTakeNodeLease, []any{s.node, 3600.0}
		if mode == PresenceLeaseMember {
			bump = sqlBumpMemberLeases
		}
		if _, err := tx.Exec(ctx, bump, args...); err != nil {
			t.Fatalf("bump: %v", err)
		}

		const ops = 10
		errs := make(chan error, ops)
		start := time.Now()
		for i := range ops {
			go func() {
				opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				id := fmt.Sprintf("m-%d", i)
				_, _, err := ch.StorePresence(opCtx, []*protocol.PresenceMessage{{
					Action: protocol.PresenceUpdate, ClientID: id, ConnectionID: "conn-" + id, Data: "updated",
				}})
				errs <- err
			}()
		}
		failed := 0
		for range ops {
			if err := <-errs; err != nil {
				failed++
			}
		}
		took := time.Since(start)
		_ = tx.Rollback(ctx)
		switch mode {
		case PresenceLeaseNode:
			if failed != 0 || took > time.Second {
				t.Errorf("%d of %d presence UPDATEs failed, all done in %v, with a node lease bump open; want all done well under 1s", failed, ops, took)
			}
		case PresenceLeaseMember:
			if failed != ops {
				t.Errorf("%d of %d presence UPDATEs completed with a member lease bump open; want all held by its row locks (the control)", ops-failed, ops)
			}
		}
	})
}

// TestLeaseBumpPresenceLatency measures batched presence UPDATE latency
// on 5,000 members one node owns, first alone and then with that node's
// lease bump running back to back. In node lease mode the bump must not
// add to the p99 (a generous 3x + 25ms, for a noisy test container); in
// member lease mode the numbers are only logged, as the comparison.
func TestLeaseBumpPresenceLatency(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) {
		defer swapPresenceTimings(time.Hour, time.Hour, time.Hour)()
		ctx := context.Background()
		dsn := pgtest.Start(t).FreshSchemaDSN(t)
		s := openLeaseNode(t, dsn, mode, "", Batching{Lanes: 4})
		defer func() { _ = s.Close() }()
		const members, rooms = 5000, 10
		var names []string
		for i := range rooms {
			names = append(names, fmt.Sprintf("room-%d", i))
		}
		seed := make(chan int)
		seedErr := make(chan error, 16)
		for range 16 {
			go func() {
				for i := range seed {
					ch, err := s.Channel(ctx, names[i%rooms], nil)
					if err == nil {
						id := fmt.Sprintf("m-%d", i)
						_, _, err = ch.StorePresence(ctx, []*protocol.PresenceMessage{{
							Action: protocol.PresenceEnter, ClientID: id, ConnectionID: "conn-" + id,
						}})
					}
					if err != nil {
						seedErr <- err
						return
					}
				}
				seedErr <- nil
			}()
		}
		for i := range members {
			seed <- i
		}
		close(seed)
		for range 16 {
			if err := <-seedErr; err != nil {
				t.Fatalf("seed: %v", err)
			}
		}

		measure := func(withBump bool) time.Duration {
			stop := make(chan struct{})
			bumpDone := make(chan int)
			if withBump {
				go func() {
					n := 0
					for {
						select {
						case <-stop:
							bumpDone <- n
							return
						default:
						}
						if mode == PresenceLeaseNode {
							_, _ = s.takeNodeLease(ctx)
						} else {
							s.bumpMemberLeases(ctx)
						}
						n++
					}
				}()
			}
			var (
				mu  sync.Mutex
				all []time.Duration
			)
			deadline := time.Now().Add(2 * time.Second)
			var workers = 16
			done := make(chan struct{})
			for w := range workers {
				go func() {
					defer func() { done <- struct{}{} }()
					for k := 0; time.Now().Before(deadline); k++ {
						i := (w*7919 + k*104729) % members
						ch, err := s.Channel(ctx, names[i%rooms], nil)
						if err != nil {
							return
						}
						id := fmt.Sprintf("m-%d", i)
						t0 := time.Now()
						if _, _, err := ch.StorePresence(ctx, []*protocol.PresenceMessage{{
							Action: protocol.PresenceUpdate, ClientID: id, ConnectionID: "conn-" + id, Data: k,
						}}); err != nil {
							return
						}
						d := time.Since(t0)
						mu.Lock()
						all = append(all, d)
						mu.Unlock()
					}
				}()
			}
			for range workers {
				<-done
			}
			bumps := 0
			if withBump {
				close(stop)
				bumps = <-bumpDone
			}
			if len(all) == 0 {
				t.Fatal("no presence UPDATE completed")
			}
			slices.Sort(all)
			p50, p99 := all[len(all)/2], all[len(all)*99/100]
			t.Logf("%s mode, bump running %v (%d bumps): %d UPDATEs, p50 %v, p99 %v, max %v",
				mode, withBump, bumps, len(all), p50, p99, all[len(all)-1])
			return p99
		}
		alone := measure(false)
		withBump := measure(true)
		if mode == PresenceLeaseNode && withBump > 3*alone+25*time.Millisecond {
			t.Errorf("node lease bump raised presence UPDATE p99 from %v to %v", alone, withBump)
		}
	})
}
