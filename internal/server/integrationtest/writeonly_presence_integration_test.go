//go:build integration

package integrationtest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestIntegrationClusterWriteOnlyPublishWithPresenceMembers is the rule
// for a channel that has both write-only REST publishes and presence
// members (DESIGN.md §5.1, §12.4), with the default flags (write-only
// path on, local SYNC source), on the bus ABLY_INTEGRATION_BUS selects.
// Members enter on node A; node B, which holds no Channel for the room,
// takes REST publishes on the write-only path. A's subscriber receives
// each exactly once and in order, B binds nothing, and A's local member
// set is untouched by them: a fresh attach on A gets a SYNC equal to the
// store's set, before and after a member leaves. A's own REST publishes
// take the normal path, since A holds the Channel. Once a presence
// subscriber attaches on B, B binds the room, serves the same SYNC, and
// its REST publishes leave the write-only path.
func TestIntegrationClusterWriteOnlyPublishWithPresenceMembers(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	addrA, debugA := startNode(t, dsn)
	addrB, debugB := startNode(t, dsn)
	const room = "wo-presence-room"

	// An observer on A binds the room there, receives its messages and
	// presence events, and seeds A's local member set while it is empty.
	observer := dialRawAs(t, addrA, "")
	if got := rawAttach(t, observer, room, protocol.FlagSubscribe|protocol.FlagPresenceSubscribe); len(got) != 0 {
		t.Fatalf("observer SYNC = %v, want none on an empty room", got)
	}

	const n = 3
	members := make([]*websocket.Conn, n)
	for i := range members {
		members[i] = dialRawAs(t, addrA, fmt.Sprintf("m%02d", i))
		rawAttach(t, members[i], room, protocol.FlagPresence)
		rawPresence(t, members[i], room, protocol.PresenceEnter, fmt.Sprintf("d%02d", i))
	}
	rawAwaitPresence(t, observer, n)

	// Write-only publishes on B reach A's subscriber exactly once, in
	// order, and bind nothing on B.
	var want []string
	for i := range 10 {
		want = append(want, restPublish(t, addrB, room, "b"+strconv.Itoa(i)))
	}
	for i, w := range want {
		if got := rawNextMessage(t, observer); got != w {
			t.Fatalf("A message %d = %q, want %q (exactly once, in order)", i, got, w)
		}
	}
	if got := boundChannels(t, debugB); got != 0 {
		t.Fatalf("node B bound channels after write-only publishes = %d, want 0", got)
	}
	if got := metricValue(t, debugB, "ably_channel_unbound_publishes_total"); got != float64(len(want)) {
		t.Fatalf("node B ably_channel_unbound_publishes_total = %v, want %d", got, len(want))
	}

	// A holds the Channel (attachments and members), so its REST publish
	// takes the normal path.
	ownA := restPublish(t, addrA, room, "a0")
	if got := rawNextMessage(t, observer); got != ownA {
		t.Fatalf("A message after its own publish = %q, want %q", got, ownA)
	}
	if got := metricValue(t, debugA, "ably_channel_unbound_publishes_total"); got != 0 {
		t.Fatalf("node A ably_channel_unbound_publishes_total = %v, want 0 (A holds the Channel)", got)
	}

	// The message cms change no member: A's SYNC is still the store's set.
	assertSyncMatchesStore(t, addrA, addrB, room, n)

	// A member leaves on A; a further write-only publish on B; A's SYNC
	// drops the member and nothing else.
	rawPresence(t, members[1], room, protocol.PresenceLeave, "")
	rawAwaitPresence(t, observer, 1)
	after := restPublish(t, addrB, room, "b-after-leave")
	if got := rawNextMessage(t, observer); got != after {
		t.Fatalf("A message after the LEAVE = %q, want %q", got, after)
	}
	assertSyncMatchesStore(t, addrA, addrB, room, n-1)
	if seeds := scrapeCounters(t, debugA, "ably_presence_sync_seeds_total")[""]; seeds != 1 {
		t.Errorf("node A seeded its member set %v times, want once for the one bind", seeds)
	}

	// A presence subscriber on B binds the room there: B's SYNC matches
	// the store, and B's next REST publish takes the normal path.
	assertSyncMatchesStore(t, addrB, addrA, room, n-1)
	unbound := metricValue(t, debugB, "ably_channel_unbound_publishes_total")
	last := restPublish(t, addrB, room, "b-bound")
	if got := rawNextMessage(t, observer); got != last {
		t.Fatalf("A message after B bound = %q, want %q", got, last)
	}
	if got := metricValue(t, debugB, "ably_channel_unbound_publishes_total"); got != unbound {
		t.Fatalf("node B unbound publishes went %v -> %v after B bound the room, want unchanged", unbound, got)
	}
	rawNoMessage(t, observer, 300*time.Millisecond)
}
