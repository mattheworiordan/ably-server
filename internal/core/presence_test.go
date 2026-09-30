package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// membersStore is a ChannelStore whose Members returns a fixed set and
// as-of serial, optionally blocking until release is closed, and counts
// its calls. Every other method is unused here.
type membersStore struct {
	storage.ChannelStore
	members []*protocol.PresenceMessage
	asOf    string
	err     error
	release chan struct{} // nil: return at once
	entered chan struct{} // closed on the first call, when non-nil
	calls   atomic.Int32
}

func (s *membersStore) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	if s.calls.Add(1) == 1 && s.entered != nil {
		close(s.entered)
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return s.members, s.asOf, s.err
}

// pres builds one presence operation stamped as storage would: serial
// "<channelSerial>:<idx>".
func pres(channelSerial string, idx int, action protocol.PresenceAction, conn, client, data string) *protocol.PresenceMessage {
	return &protocol.PresenceMessage{
		Serial: fmt.Sprintf("%s:%03d", channelSerial, idx), Action: action,
		ConnectionID: conn, ClientID: client, Data: data,
	}
}

// presCM builds a presence cm from operations given as
// action/conn/client/data, stamping each with its index.
func presCM(channelSerial string, ops ...*protocol.PresenceMessage) *protocol.ChannelMessage {
	for i, p := range ops {
		p.Serial = fmt.Sprintf("%s:%03d", channelSerial, i)
	}
	return &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: ops}
}

func op(action protocol.PresenceAction, conn, client, data string) *protocol.PresenceMessage {
	return &protocol.PresenceMessage{Action: action, ConnectionID: conn, ClientID: client, Data: data}
}

// testChannel returns a ready Channel over store with a controllable
// clock.
func testChannel(t *testing.T, store storage.ChannelStore, seed string) (*Channel, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	c := newReadyChannel("room", seed)
	c.store = store
	c.mgr = &Manager{now: clk.now, logger: logging.Default()}
	return c, clk
}

// setOf renders members as sorted "conn:client=data" strings.
func setOf(members []*protocol.PresenceMessage) []string {
	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, fmt.Sprintf("%s=%v", storage.MemberKey(m.ConnectionID, m.ClientID), m.Data))
	}
	sort.Strings(out)
	return out
}

func TestMemberViewFold(t *testing.T) {
	v := &memberView{members: map[string]*protocol.PresenceMessage{}, asOf: "010"}
	steps := []struct {
		name string
		cm   *protocol.ChannelMessage
		fold bool
		want []string
	}{
		{"enter", presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a1")), true, []string{"c1:alice=a1"}},
		{"second member", presCM("012", op(protocol.PresenceEnter, "c2", "bob", "b1")), true, []string{"c1:alice=a1", "c2:bob=b1"}},
		{"update replaces", presCM("013", op(protocol.PresenceUpdate, "c1", "alice", "a2")), true, []string{"c1:alice=a2", "c2:bob=b1"}},
		{"leave removes", presCM("014", op(protocol.PresenceLeave, "c2", "bob", "")), true, []string{"c1:alice=a2"}},
		{"duplicate enter (cm delivered twice) ignored", presCM("012", op(protocol.PresenceEnter, "c2", "bob", "b1")), false, []string{"c1:alice=a2"}},
		{"cm at or before the seed ignored", presCM("009", op(protocol.PresenceEnter, "c9", "zed", "z")), false, []string{"c1:alice=a2"}},
		{"enter then leave in one cm", presCM("015", op(protocol.PresenceEnter, "c3", "carol", "c"), op(protocol.PresenceLeave, "c3", "carol", "")), true, []string{"c1:alice=a2"}},
		{"leave then enter in one cm", presCM("016", op(protocol.PresenceLeave, "c1", "alice", ""), op(protocol.PresenceEnter, "c1", "alice", "a3")), true, []string{"c1:alice=a3"}},
	}
	for _, s := range steps {
		if got := v.fold(s.cm); got != s.fold {
			t.Errorf("%s: fold = %v, want %v", s.name, got, s.fold)
		}
		members := make([]*protocol.PresenceMessage, 0, len(v.members))
		for _, m := range v.members {
			members = append(members, m)
		}
		if got := setOf(members); fmt.Sprint(got) != fmt.Sprint(s.want) {
			t.Errorf("%s: set = %v, want %v", s.name, got, s.want)
		}
	}
	if v.asOf != "016" {
		t.Errorf("asOf = %q, want 016", v.asOf)
	}

	// A stale LEAVE: an operation whose member serial is older than the
	// member's current state is skipped, even in a later cm.
	stale := &protocol.ChannelMessage{ChannelSerial: "017", Presence: []*protocol.PresenceMessage{
		pres("015", 0, protocol.PresenceLeave, "c1", "alice", ""),
	}}
	v.fold(stale)
	if _, ok := v.members[storage.MemberKey("c1", "alice")]; !ok {
		t.Error("stale LEAVE removed a member that re-entered after it")
	}
}

func TestCompareMemberSerialIndexIsNumeric(t *testing.T) {
	if compareMemberSerial("001-000@s:1000", "001-000@s:999") <= 0 {
		t.Error("index 1000 must sort after 999")
	}
	if compareMemberSerial("002-000@s:000", "001-000@s:999") <= 0 {
		t.Error("a later channelSerial must sort after an earlier one")
	}
}

func TestPresenceSyncSeedsOnceAndFoldsDeliveredCMs(t *testing.T) {
	store := &membersStore{
		members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")},
		asOf:    "005",
	}
	c, _ := testChannel(t, store, "005")
	ctx := context.Background()

	snap, gap, err := c.PresenceSync(ctx, "005")
	if err != nil || gap != nil {
		t.Fatalf("PresenceSync: gap=%v err=%v", gap, err)
	}
	if got := setOf(snap.Members); fmt.Sprint(got) != "[c1:alice=a]" {
		t.Fatalf("seeded set = %v", got)
	}
	for _, m := range snap.Members {
		if m.Action != protocol.PresencePresent {
			t.Errorf("snapshot member action = %v, want PRESENT", m.Action)
		}
	}

	c.Append(presCM("006", op(protocol.PresenceEnter, "c2", "bob", "b")))
	c.Append(newCM("007", "m1")) // a message cm changes no member
	c.Append(presCM("008", op(protocol.PresenceLeave, "c1", "alice", "")))

	snap, _, err = c.PresenceSync(ctx, "")
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	if got := setOf(snap.Members); fmt.Sprint(got) != "[c2:bob=b]" {
		t.Errorf("set after cms = %v, want [c2:bob=b]", got)
	}
	if snap.AsOf != "008" {
		t.Errorf("asOf = %q, want 008", snap.AsOf)
	}
	c.Append(newCM("009", "m2"))
	if got, _, _ := c.PresenceSync(ctx, ""); got != snap {
		t.Error("a message cm invalidated the cached snapshot")
	}
	if n := store.calls.Load(); n != 1 {
		t.Errorf("store Members calls = %d, want 1 (seed once per bind)", n)
	}
}

// TestPresenceSyncSeedBuffersConcurrentDeliveries: cms delivered while
// the seed read is in flight fold on top of it exactly: one at or below
// the store's as-of serial is already in the seed, one after it is not.
func TestPresenceSyncSeedBuffersConcurrentDeliveries(t *testing.T) {
	store := &membersStore{
		// The store's snapshot is as of 006: alice entered at 005, bob at
		// 006.
		members: []*protocol.PresenceMessage{
			pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a"),
			pres("006", 0, protocol.PresenceEnter, "c2", "bob", "b"),
		},
		asOf:    "006",
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	c, _ := testChannel(t, store, "004")
	ctx := context.Background()

	type result struct {
		snap *PresenceSnapshot
		err  error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			snap, _, err := c.PresenceSync(ctx, "")
			results <- result{snap, err}
		}()
	}
	<-store.entered
	// Delivered during the read: 005 and 006 are in the seed; 007 (bob
	// leaves) and 008 (carol enters) are not.
	c.Append(presCM("005", op(protocol.PresenceEnter, "c1", "alice", "a")))
	c.Append(presCM("006", op(protocol.PresenceEnter, "c2", "bob", "b")))
	c.Append(presCM("007", op(protocol.PresenceLeave, "c2", "bob", "")))
	c.Append(presCM("008", op(protocol.PresenceEnter, "c3", "carol", "c")))
	close(store.release)

	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("PresenceSync: %v", r.err)
		}
		if got := setOf(r.snap.Members); fmt.Sprint(got) != "[c1:alice=a c3:carol=c]" {
			t.Errorf("set = %v, want [c1:alice=a c3:carol=c]", got)
		}
	}
	if n := store.calls.Load(); n != 1 {
		t.Errorf("store Members calls = %d, want 1 (concurrent SYNCs share one seed)", n)
	}
}

func TestPresenceSyncStaleSnapshotWithinRefreshWindow(t *testing.T) {
	store := &membersStore{asOf: "010"}
	c, clk := testChannel(t, store, "010")
	ctx := context.Background()

	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a"))) // before the seed: not tracked
	first, _, err := c.PresenceSync(ctx, "011")
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	if len(first.Members) != 0 {
		t.Fatalf("seed = %v, want the store's (empty) set", setOf(first.Members))
	}

	// Two members enter after the snapshot. Within the window, an attach
	// anchored at 012 gets the old snapshot plus 012's operations; 013
	// reaches it on its stream.
	c.Append(presCM("012", op(protocol.PresenceEnter, "c2", "bob", "b")))
	c.Append(presCM("013", op(protocol.PresenceEnter, "c3", "carol", "c")))
	clk.ns.Add(int64(10 * time.Millisecond))
	snap, gap, err := c.PresenceSync(ctx, "012")
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	if snap != first {
		t.Fatal("within the refresh window the snapshot was rebuilt")
	}
	if len(gap) != 1 || gap[0].ClientID != "bob" {
		t.Errorf("gap = %v, want bob's ENTER only", setOf(gap))
	}

	// A client-initiated SYNC (no anchor) always gets a current snapshot.
	cur, gap, err := c.PresenceSync(ctx, "")
	if err != nil || gap != nil {
		t.Fatalf("PresenceSync: gap=%v err=%v", gap, err)
	}
	if got := setOf(cur.Members); fmt.Sprint(got) != "[c2:bob=b c3:carol=c]" {
		t.Errorf("current set = %v", got)
	}

	// Past the window a stale snapshot is rebuilt.
	c.Append(presCM("014", op(protocol.PresenceLeave, "c2", "bob", "")))
	clk.ns.Add(int64(DefaultPresenceSyncRefresh))
	rebuilt, gap, err := c.PresenceSync(ctx, "014")
	if err != nil || gap != nil {
		t.Fatalf("PresenceSync: gap=%v err=%v", gap, err)
	}
	if rebuilt == cur || fmt.Sprint(setOf(rebuilt.Members)) != "[c3:carol=c]" {
		t.Errorf("after the window: set = %v, want a rebuilt [c3:carol=c]", setOf(rebuilt.Members))
	}
}

// TestPresenceSyncDropsExpiredSnapshot: a stale snapshot older than the
// refresh window is dropped on the next presence cm, so a channel whose
// members churn with no attaches pins no cms.
func TestPresenceSyncDropsExpiredSnapshot(t *testing.T) {
	c, clk := testChannel(t, &membersStore{asOf: "010"}, "010")
	if _, _, err := c.PresenceSync(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	if c.pv.snap == nil || len(c.pv.since) != 1 {
		t.Fatalf("within the window: snap=%v since=%d, want kept with one cm", c.pv.snap != nil, len(c.pv.since))
	}
	clk.ns.Add(int64(DefaultPresenceSyncRefresh))
	c.Append(presCM("012", op(protocol.PresenceEnter, "c2", "bob", "b")))
	if c.pv.snap != nil || c.pv.since != nil {
		t.Error("an expired stale snapshot was kept")
	}
}

func TestPresenceSyncStoreModeReadsStoreEveryTime(t *testing.T) {
	store := &membersStore{members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")}, asOf: "005"}
	c, _ := testChannel(t, store, "005")
	c.syncSource = PresenceSyncStore
	for range 3 {
		snap, gap, err := c.PresenceSync(context.Background(), "005")
		if err != nil || gap != nil {
			t.Fatalf("PresenceSync: gap=%v err=%v", gap, err)
		}
		if snap.AsOf != "005" || fmt.Sprint(setOf(snap.Members)) != "[c1:alice=a]" {
			t.Errorf("store snapshot = %v as of %q", setOf(snap.Members), snap.AsOf)
		}
	}
	if n := store.calls.Load(); n != 3 {
		t.Errorf("store Members calls = %d, want 3", n)
	}
	if c.pv.seeded {
		t.Error("store mode seeded a local set")
	}
}

func TestPresenceSyncSeedFailureFallsBackAndRetries(t *testing.T) {
	store := &membersStore{err: errors.New("database down")}
	c, _ := testChannel(t, store, "005")
	if _, _, err := c.PresenceSync(context.Background(), ""); err == nil {
		t.Fatal("PresenceSync succeeded with a failing store")
	}
	if c.pv.seeded || c.pv.seeding != nil {
		t.Fatal("a failed seed left the view seeded or seeding")
	}
	store.err = nil
	store.asOf = "005"
	if _, _, err := c.PresenceSync(context.Background(), ""); err != nil {
		t.Fatalf("PresenceSync after recovery: %v", err)
	}
	if !c.pv.seeded {
		t.Error("the view did not seed once the store recovered")
	}
}

func TestPresenceSnapshotMemoBuildsOnce(t *testing.T) {
	s := newSnapshot(nil, "001")
	var builds atomic.Int32
	build := func() any { builds.Add(1); return "frame" }
	for range 5 {
		if v := s.Memo(1, build); v != "frame" {
			t.Fatalf("Memo = %v", v)
		}
	}
	s.Memo(2, build)
	if n := builds.Load(); n != 2 {
		t.Errorf("builds = %d, want one per key", n)
	}
}

func TestParsePresenceSyncSource(t *testing.T) {
	for in, want := range map[string]string{"": PresenceSyncLocal, "local": PresenceSyncLocal, "store": PresenceSyncStore} {
		if got, err := ParsePresenceSyncSource(in); err != nil || got != want {
			t.Errorf("ParsePresenceSyncSource(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := ParsePresenceSyncSource("postgres"); err == nil {
		t.Error("an unknown source was accepted")
	}
}
