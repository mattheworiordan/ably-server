//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// Presence lease outages (DESIGN.md §12.5): the reaper guard, lapse
// re-entry, and the reaper's conditional LEAVE.

// failLeaseBumps makes every lease renewal of the given nodes (every
// node when none is named) fail until the returned func is called, as a
// Postgres outage would.
func failLeaseBumps(nodes ...string) (restore func()) {
	hook := func(node string) error {
		if len(nodes) == 0 || slices.Contains(nodes, node) {
			return errors.New("test: lease bump blacked out")
		}
		return nil
	}
	leaseBumpHook.Store(&hook)
	return func() { leaseBumpHook.Store(nil) }
}

// actions returns the presence actions delivered for clientID, in order.
func (r *presenceRecorder) actions(clientID string) []protocol.PresenceAction {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []protocol.PresenceAction
	for _, p := range r.presence {
		if p.ClientID == clientID {
			out = append(out, p.Action)
		}
	}
	return out
}

func (r *presenceRecorder) totalLeaves() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.presence {
		if p.Action == protocol.PresenceLeave {
			n++
		}
	}
	return n
}

// TestBlackoutReapsNoLiveMember: two live nodes own members; every lease
// renewal fails for well over a lease window (a Postgres failover as the
// nodes see it), so both leases lapse together. No member of either node
// may be reaped and no LEAVE published, during the outage or in the
// window after it; then a node that dies for real (no graceful release)
// is still reaped within the lease window plus one bump interval plus
// the reaper rounds. Before the reaper guard, each node reaped the
// other's members during the outage.
func TestBlackoutReapsNoLiveMember(t *testing.T) {
	forEachLeaseMode(t, func(t *testing.T, mode string) {
		defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 100*time.Millisecond)()
		ctx := context.Background()
		dsn := pgtest.Start(t).FreshSchemaDSN(t)
		sa := openLeaseNode(t, dsn, mode, "", Batching{Lanes: 4})
		defer func() { _ = sa.Close() }()
		sb := openLeaseNode(t, dsn, mode, "", Batching{Lanes: 4})
		defer func() { _ = sb.Close() }()
		recA, recB := &presenceRecorder{}, &presenceRecorder{}
		if _, err := sa.Channel(ctx, "room", recA); err != nil {
			t.Fatalf("Channel A: %v", err)
		}
		if _, err := sb.Channel(ctx, "room", recB); err != nil {
			t.Fatalf("Channel B: %v", err)
		}
		const n = 5
		enterMembers(t, sa, []string{"room"}, "a", n)
		enterMembers(t, sb, []string{"room"}, "b", n)
		// Both nodes past their first window, so both reapers are armed.
		time.Sleep(presenceLeaseWindow + 2*presenceLeaseBumpInterval)

		restore := failLeaseBumps()
		time.Sleep(3 * presenceLeaseWindow)
		restore()
		time.Sleep(presenceLeaseWindow + 2*presenceLeaseBumpInterval)

		if got := countPresenceRows(t, sa, `channel = 'room'`); got != 2*n {
			t.Fatalf("%d members after the outage, want all %d (live nodes' members were reaped)", got, 2*n)
		}
		if a, b := recA.totalLeaves(), recB.totalLeaves(); a+b != 0 {
			t.Fatalf("LEAVEs published across the outage: %d on A, %d on B, want 0", a, b)
		}
		if got := counterValue(t, sb.lmetrics.deferred); got == 0 {
			t.Error("ably_presence_reaps_deferred_total = 0, want the outage's reaper rounds counted")
		}
		if got := counterValue(t, sb.lmetrics.lapses); got != 1 {
			t.Errorf("ably_presence_lease_lapses_total = %v on B, want 1", got)
		}

		// A real death is still reaped promptly.
		start := time.Now()
		crash(t, sa)
		waitFor(t, 10*time.Second, "the dead node's members to be reaped", func() bool {
			return countPresenceRows(t, sb, `node_id = $1`, sa.node) == 0
		})
		if took, bound := time.Since(start), presenceLeaseWindow+presenceLeaseBumpInterval+3*presenceReaperInterval+time.Second; took > bound {
			t.Errorf("reaping a dead node took %v, want within %v", took, bound)
		}
		waitFor(t, 10*time.Second, "the dead node's LEAVEs", func() bool { return recB.totalLeaves() == n })
		if got := countPresenceRows(t, sb, `node_id = $1`, sb.node); got != n {
			t.Errorf("the live node owns %d members, want %d", got, n)
		}
	})
}

// reenterRecorder is an Options.OnPresenceLeaseLapse that re-enters a
// fixed set of members, as realtime.Server.ReenterPresence re-enters a
// node's connected members, and counts its calls.
type reenterRecorder struct {
	calls   atomic.Int32
	store   func(channel string) storage.ChannelStore
	members map[string][]*protocol.PresenceMessage // channel -> ENTERs
}

func (r *reenterRecorder) hook(ctx context.Context) {
	r.calls.Add(1)
	for channel, enters := range r.members {
		msgs := make([]*protocol.PresenceMessage, len(enters))
		for i, e := range enters {
			c := *e
			msgs[i] = &c
		}
		_, _, _ = r.store(channel).StorePresence(storage.WithPresenceReentry(ctx), msgs)
	}
}

// TestLeaseLapseReentersMembers: node A's renewals fail for longer than
// the lease window while node B is healthy, so B reaps A's members and
// publishes their LEAVEs. When A's renewals succeed again its lapse hook
// fires exactly once, the members are back in the table with their
// original data, and a subscriber on B saw each leave and then enter
// again.
func TestLeaseLapseReentersMembers(t *testing.T) {
	for name, batching := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
		t.Run(name, func(t *testing.T) {
			defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 100*time.Millisecond)()
			ctx := context.Background()
			dsn := pgtest.Start(t).FreshSchemaDSN(t)
			rec := &reenterRecorder{members: map[string][]*protocol.PresenceMessage{"room": {
				{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "conn-a", Data: "alice-data"},
				{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "conn-b", Data: "bob-data"},
			}}}
			sa, err := Open(ctx, Options{DSN: dsn, Batching: batching, OnPresenceLeaseLapse: rec.hook})
			if err != nil {
				t.Fatalf("Open A: %v", err)
			}
			defer func() { _ = sa.Close() }()
			rec.store = func(channel string) storage.ChannelStore {
				cs, _ := sa.Channel(ctx, channel, nil)
				return cs
			}
			sb := openLeaseNode(t, dsn, PresenceLeaseNode, "", batching)
			defer func() { _ = sb.Close() }()
			recB := &presenceRecorder{}
			chB, err := sb.Channel(ctx, "room", recB)
			if err != nil {
				t.Fatalf("Channel B: %v", err)
			}
			chA, err := sa.Channel(ctx, "room", nil)
			if err != nil {
				t.Fatalf("Channel A: %v", err)
			}
			if _, _, err := chA.StorePresence(ctx, slices.Clone(rec.members["room"])); err != nil {
				t.Fatalf("enter: %v", err)
			}
			time.Sleep(presenceLeaseWindow + 2*presenceLeaseBumpInterval)

			restore := failLeaseBumps(sa.node)
			waitFor(t, 10*time.Second, "node B to reap node A's members", func() bool {
				return countPresenceRows(t, sb, `node_id = $1`, sa.node) == 0
			})
			waitFor(t, 10*time.Second, "the reaper's LEAVEs", func() bool { return recB.leaveCount("alice")+recB.leaveCount("bob") == 2 })
			if got := rec.calls.Load(); got != 0 {
				t.Fatalf("lapse hook ran %d times while the lease was still down", got)
			}
			restore()

			waitFor(t, 10*time.Second, "the members to be re-entered", func() bool {
				return countPresenceRows(t, sb, `node_id = $1`, sa.node) == 2
			})
			time.Sleep(3 * presenceLeaseBumpInterval) // a second call would come within this
			if got := rec.calls.Load(); got != 1 {
				t.Fatalf("lapse hook ran %d times, want exactly 1", got)
			}
			if got := counterValue(t, sa.lmetrics.lapses); got != 1 {
				t.Errorf("ably_presence_lease_lapses_total = %v, want 1", got)
			}
			members, _, err := chB.Members(ctx)
			if err != nil {
				t.Fatalf("Members: %v", err)
			}
			data := map[string]any{}
			for _, m := range members {
				data[m.ClientID] = m.Data
			}
			if data["alice"] != "alice-data" || data["bob"] != "bob-data" {
				t.Errorf("members after re-entry = %v, want alice and bob with their original data", data)
			}
			for _, id := range []string{"alice", "bob"} {
				want := []protocol.PresenceAction{protocol.PresenceEnter, protocol.PresenceLeave, protocol.PresenceEnter}
				waitFor(t, 10*time.Second, id+"'s re-ENTER on node B", func() bool { return len(recB.actions(id)) >= len(want) })
				if got := recB.actions(id); !slices.Equal(got, want) {
					t.Errorf("%s: node B saw %v, want %v", id, got, want)
				}
			}
		})
	}
}

// TestShardedLeaseLapseReentersOnce: a sharded node whose lease lapses on
// both shards in one outage (each shard renews on its own tick) runs its
// lapse hook once, not once per shard.
func TestShardedLeaseLapseReentersOnce(t *testing.T) {
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 100*time.Millisecond)()
	ctx := context.Background()
	dsns := shardDSNs(t, 2)
	var calls atomic.Int32
	// Opened and closed here, not with openShardedT's cleanup, so the
	// stores stop before the deferred timing restore runs.
	sx, err := OpenSharded(ctx, Options{OnPresenceLeaseLapse: func(context.Context) { calls.Add(1) }}, dsns)
	if err != nil {
		t.Fatalf("OpenSharded X: %v", err)
	}
	defer func() { _ = sx.Close() }()
	sy, err := OpenSharded(ctx, Options{}, dsns) // reaps
	if err != nil {
		t.Fatalf("OpenSharded Y: %v", err)
	}
	defer func() { _ = sy.Close() }()

	// One member on each shard.
	var rooms []string
	for i := 0; len(rooms) < 2; i++ {
		name := fmt.Sprintf("room-%d", i)
		if ShardFor(name, 2) == len(rooms) {
			rooms = append(rooms, name)
		}
	}
	for i, room := range rooms {
		ch, err := sx.Channel(ctx, room, nil)
		if err != nil {
			t.Fatalf("Channel: %v", err)
		}
		id := fmt.Sprintf("m-%d", i)
		if _, _, err := ch.StorePresence(ctx, []*protocol.PresenceMessage{{
			Action: protocol.PresenceEnter, ClientID: id, ConnectionID: "conn-" + id,
		}}); err != nil {
			t.Fatalf("enter: %v", err)
		}
	}
	time.Sleep(presenceLeaseWindow + 2*presenceLeaseBumpInterval)

	node := sx.Shard(0).node
	restore := failLeaseBumps(node)
	waitFor(t, 10*time.Second, "both shards' members to be reaped", func() bool {
		return countPresenceRows(t, sy.Shard(0), `node_id = $1`, node)+countPresenceRows(t, sy.Shard(1), `node_id = $1`, node) == 0
	})
	restore()
	waitFor(t, 10*time.Second, "the lapse hook", func() bool { return calls.Load() > 0 })
	time.Sleep(3 * presenceLeaseBumpInterval)
	if got := calls.Load(); got != 1 {
		t.Fatalf("lapse hook ran %d times for one outage on 2 shards, want 1", got)
	}
	lapses := counterValue(t, sx.Shard(0).lmetrics.lapses) + counterValue(t, sx.Shard(1).lmetrics.lapses)
	if lapses != 2 {
		t.Errorf("lapses counted on the shards = %v, want 2 (one each)", lapses)
	}
}

// TestReapedLeaveSkipsPresentMember: a reaper LEAVE for a member that is
// in the presence table again by the time the LEAVE holds the channel's
// row lock (its node re-entered it) is not stored or delivered, and the
// member stays; one for an absent member is published as before.
func TestReapedLeaveSkipsPresentMember(t *testing.T) {
	for name, batching := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
		t.Run(name, func(t *testing.T) {
			defer swapPresenceTimings(time.Hour, time.Hour, time.Hour)() // no background round
			ctx := context.Background()
			dsn := pgtest.Start(t).FreshSchemaDSN(t)
			s := openLeaseNode(t, dsn, PresenceLeaseNode, "", batching)
			defer func() { _ = s.Close() }()
			rec := &presenceRecorder{}
			if _, err := s.Channel(ctx, "room", rec); err != nil {
				t.Fatalf("Channel: %v", err)
			}
			enterMembers(t, s, []string{"room"}, "m", 1) // m-0 on conn-m-0
			waitFor(t, 5*time.Second, "the ENTER", func() bool { return len(rec.actions("m-0")) == 1 })

			s.publishReapedLeave(ctx, "room", []reapedMember{
				{channel: "room", connID: "conn-m-0", clientID: "m-0"},     // present: skipped
				{channel: "room", connID: "conn-gone", clientID: "gone-0"}, // absent: published
			})
			waitFor(t, 5*time.Second, "the absent member's LEAVE", func() bool { return rec.leaveCount("gone-0") == 1 })
			time.Sleep(200 * time.Millisecond)
			if got := rec.leaveCount("m-0"); got != 0 {
				t.Errorf("%d LEAVEs delivered for a member that is present, want 0", got)
			}
			if got := countPresenceRows(t, s, `client_id = 'm-0'`); got != 1 {
				t.Errorf("present member's rows = %d after a reaper LEAVE, want 1", got)
			}

			// Every member present: nothing is stored at all.
			var before int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM channel_messages WHERE channel = 'room'`).Scan(&before); err != nil {
				t.Fatalf("count: %v", err)
			}
			s.publishReapedLeave(ctx, "room", []reapedMember{{channel: "room", connID: "conn-m-0", clientID: "m-0"}})
			var after int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM channel_messages WHERE channel = 'room'`).Scan(&after); err != nil {
				t.Fatalf("count: %v", err)
			}
			if after != before {
				t.Errorf("a reaper LEAVE of only present members stored %d cms, want none", after-before)
			}
		})
	}
}

// TestReentrySkipsPresentMembers: a lease-lapse re-entry
// (storage.WithPresenceReentry) writes only the members missing from the
// table, so the re-entry after an outage that reaped nothing (every
// node's lease lapsed together, and the reaper guard held) is not a storm
// of repeated ENTERs.
func TestReentrySkipsPresentMembers(t *testing.T) {
	for name, batching := range map[string]Batching{"unbatched": {}, "batched": {Lanes: 4}} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dsn := pgtest.Start(t).FreshSchemaDSN(t)
			s := openLeaseNode(t, dsn, PresenceLeaseNode, "", batching)
			defer func() { _ = s.Close() }()
			rec := &presenceRecorder{}
			ch, err := s.Channel(ctx, "room", rec)
			if err != nil {
				t.Fatalf("Channel: %v", err)
			}
			if _, _, err := ch.StorePresence(ctx, []*protocol.PresenceMessage{
				{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "conn-a", Data: "a"},
			}); err != nil {
				t.Fatalf("enter: %v", err)
			}
			cm, _, err := ch.StorePresence(storage.WithPresenceReentry(ctx), []*protocol.PresenceMessage{
				{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "conn-a", Data: "a"},
				{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "conn-b", Data: "b"},
			})
			if err != nil {
				t.Fatalf("re-entry: %v", err)
			}
			if cm == nil || len(cm.Presence) != 1 || cm.Presence[0].ClientID != "bob" {
				t.Fatalf("re-entry stored %+v, want only bob", cm)
			}
			waitFor(t, 5*time.Second, "bob's ENTER", func() bool { return len(rec.actions("bob")) == 1 })
			if got := rec.actions("alice"); len(got) != 1 {
				t.Errorf("alice: %v delivered, want only her own ENTER (the re-entry skips a present member)", got)
			}
			// Every member present: nothing is stored.
			cm, _, err = ch.StorePresence(storage.WithPresenceReentry(ctx), []*protocol.PresenceMessage{
				{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "conn-b", Data: "b"},
			})
			if err != nil || cm != nil {
				t.Fatalf("re-entry of present members = %+v, %v; want nothing stored", cm, err)
			}
		})
	}
}

// TestReaperStatementTimeout: every reaper statement runs under
// reapStatementTimeout, so one that started before a node renewed cannot
// run on into that node's re-entry.
func TestReaperStatementTimeout(t *testing.T) {
	defer swapPresenceTimings(time.Hour, 200*time.Millisecond, time.Hour)()
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	s := openLeaseNode(t, dsn, PresenceLeaseNode, "", Batching{})
	defer func() { _ = s.Close() }()
	start := time.Now()
	_, err := s.deleteReturning(ctx, `SELECT 'c', 'k', 'l' FROM pg_sleep(2)`)
	if err == nil || !strings.Contains(err.Error(), "statement timeout") {
		t.Fatalf("slow reaper statement: err = %v, want a statement timeout", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("slow reaper statement took %v, want it cancelled at %v", took, reapStatementTimeout())
	}
}
