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
	// A SYNC that waits out the refresh window moves the fake clock on.
	c.mgr.sleep = func(_ context.Context, d time.Duration) error {
		clk.ns.Add(int64(d))
		return nil
	}
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

	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
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

	snap, err = c.PresenceSync(ctx)
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
	if got, _ := c.PresenceSync(ctx); got != snap {
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
			snap, err := c.PresenceSync(ctx)
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

// TestPresenceSyncWaitsOutRefreshWindow: a snapshot out of date but
// younger than the refresh window is not served (it would miss members
// that entered before the attach) and not rebuilt at once: the attach
// waits out the rest of the window, and one rebuild serves every attach
// that waited.
func TestPresenceSyncWaitsOutRefreshWindow(t *testing.T) {
	store := &membersStore{asOf: "010"}
	c, clk := testChannel(t, store, "010")
	ctx := context.Background()
	var slept []time.Duration
	c.mgr.sleep = func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		clk.ns.Add(int64(d))
		return nil
	}

	first, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	clk.ns.Add(int64(10 * time.Millisecond))

	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	if fmt.Sprint(slept) != fmt.Sprint([]time.Duration{DefaultPresenceSyncRefresh - 10*time.Millisecond}) {
		t.Errorf("slept %v, want the rest of the window once", slept)
	}
	if snap == first || fmt.Sprint(setOf(snap.Members)) != "[c1:alice=a]" {
		t.Errorf("after the wait: set %v, want a rebuilt [c1:alice=a]", setOf(snap.Members))
	}
	if again, _ := c.PresenceSync(ctx); again != snap {
		t.Error("an unchanged set was rebuilt")
	}

	// Past the window an out-of-date snapshot is rebuilt without waiting.
	slept = nil
	c.Append(presCM("012", op(protocol.PresenceLeave, "c1", "alice", "")))
	clk.ns.Add(int64(DefaultPresenceSyncRefresh))
	rebuilt, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatalf("PresenceSync: %v", err)
	}
	if len(slept) != 0 || len(rebuilt.Members) != 0 {
		t.Errorf("past the window: slept %v, set %v; want no wait and an empty set", slept, setOf(rebuilt.Members))
	}
}

// TestPresenceSyncWaitersShareOneRebuild: attaches that wait out the
// window together get the same rebuilt snapshot.
func TestPresenceSyncWaitersShareOneRebuild(t *testing.T) {
	c, _ := testChannel(t, &membersStore{asOf: "010"}, "010")
	ctx := context.Background()
	release := make(chan struct{})
	var waiting atomic.Int32
	c.mgr.sleep = func(ctx context.Context, d time.Duration) error {
		waiting.Add(1)
		<-release
		return nil
	}
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	const n = 5
	got := make(chan *PresenceSnapshot, n)
	for range n {
		go func() {
			snap, err := c.PresenceSync(ctx)
			if err != nil {
				t.Error(err)
			}
			got <- snap
		}()
	}
	for waiting.Load() < n {
		time.Sleep(time.Millisecond)
	}
	// The window passes (the fake clock never moved, so make the rebuild
	// due by ageing the snapshot).
	c.mu.Lock()
	c.pv.snapBuilt -= int64(DefaultPresenceSyncRefresh)
	c.mu.Unlock()
	close(release)
	first := <-got
	for range n - 1 {
		if s := <-got; s != first {
			t.Fatal("waiting attaches got different snapshots: more than one rebuild")
		}
	}
	if fmt.Sprint(setOf(first.Members)) != "[c1:alice=a]" {
		t.Errorf("set = %v", setOf(first.Members))
	}
}

func TestPresenceSyncSeedFailureFallsBackAndRetries(t *testing.T) {
	store := &membersStore{err: errors.New("database down")}
	c, _ := testChannel(t, store, "005")
	if _, err := c.PresenceSync(context.Background()); err == nil {
		t.Fatal("PresenceSync succeeded with a failing store")
	}
	if c.pv.seeded || c.pv.seeding != nil {
		t.Fatal("a failed seed left the view seeded or seeding")
	}
	store.err = nil
	store.asOf = "005"
	if _, err := c.PresenceSync(context.Background()); err != nil {
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

// TestPresenceSyncDiscontinuityReseeds: when the backend skips cms it
// could not deliver, the local set is dropped and the next SYNC seeds
// it again from the store, including when the skip lands while a seed
// read is in flight.
func TestPresenceSyncDiscontinuityReseeds(t *testing.T) {
	store := &membersStore{members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")}, asOf: "005"}
	c, _ := testChannel(t, store, "005")
	ctx := context.Background()
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}

	// A LEAVE for alice was skipped; the store no longer has her.
	store.members, store.asOf = nil, "007"
	c.Discontinuity(storage.DiscontinuityLogGap)
	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Members) != 0 || store.calls.Load() != 2 {
		t.Errorf("after a discontinuity: set %v with %d store reads, want empty after a re-seed", setOf(snap.Members), store.calls.Load())
	}

	// A discontinuity during the seed read discards that read.
	c.Discontinuity(storage.DiscontinuityLogGap)
	store.release, store.entered = make(chan struct{}), make(chan struct{})
	store.calls.Store(0)
	done := make(chan *PresenceSnapshot, 1)
	go func() {
		snap, _ := c.PresenceSync(ctx)
		done <- snap
	}()
	<-store.entered
	c.Discontinuity(storage.DiscontinuityLogGap)
	store.members, store.asOf = []*protocol.PresenceMessage{pres("009", 0, protocol.PresenceEnter, "c2", "bob", "b")}, "009"
	close(store.release)
	snap = <-done
	if n := store.calls.Load(); n != 2 {
		t.Errorf("store reads = %d, want 2 (the interrupted seed, then a fresh one)", n)
	}
	if got := setOf(snap.Members); fmt.Sprint(got) != "[c2:bob=b]" {
		t.Errorf("set = %v, want the fresh seed [c2:bob=b]", got)
	}
}

// TestPresenceSyncWaitsAtMostOneWindow: members keep changing while an
// attach waits. It must not wait again because the snapshot went out of
// date once more: a snapshot built after it began is complete for it.
func TestPresenceSyncWaitsAtMostOneWindow(t *testing.T) {
	c, clk := testChannel(t, &membersStore{asOf: "010"}, "010")
	ctx := context.Background()
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))

	var sleeps int
	var other *PresenceSnapshot
	c.mgr.sleep = func(_ context.Context, d time.Duration) error {
		sleeps++
		clk.ns.Add(int64(d))
		// Meanwhile another attach rebuilds, and then the set changes
		// again before this one wakes.
		var err error
		if other, err = c.PresenceSync(ctx); err != nil {
			t.Error(err)
		}
		c.Append(presCM(fmt.Sprintf("%03d", 11+sleeps), op(protocol.PresenceEnter, fmt.Sprintf("c%d", 1+sleeps), "bob", "b")))
		return nil
	}
	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sleeps != 1 {
		t.Errorf("waited %d times, want once", sleeps)
	}
	if snap != other {
		t.Error("the waiter did not take the snapshot built while it waited")
	}
	if fmt.Sprint(setOf(snap.Members)) != "[c1:alice=a]" {
		t.Errorf("set = %v, want [c1:alice=a] (complete as of the attach)", setOf(snap.Members))
	}
}

func TestPresenceSyncNowDoesNotWait(t *testing.T) {
	c, _ := testChannel(t, &membersStore{asOf: "010"}, "010")
	c.mgr.sleep = func(context.Context, time.Duration) error {
		t.Error("PresenceSyncNow waited")
		return nil
	}
	ctx := context.Background()
	if _, err := c.PresenceSyncNow(ctx); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	snap, err := c.PresenceSyncNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(setOf(snap.Members)) != "[c1:alice=a]" {
		t.Errorf("set = %v, want a current [c1:alice=a]", setOf(snap.Members))
	}
}

func TestPresenceSyncWaitCancelled(t *testing.T) {
	c, _ := testChannel(t, &membersStore{asOf: "010"}, "010")
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	c.mgr.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}
	if _, err := c.PresenceSync(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// TestPresenceSyncDiscontinuityDuringWait: the set is dropped while an
// attach waits; it re-seeds and serves the store's set.
func TestPresenceSyncDiscontinuityDuringWait(t *testing.T) {
	store := &membersStore{asOf: "010"}
	c, clk := testChannel(t, store, "010")
	ctx := context.Background()
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Append(presCM("011", op(protocol.PresenceEnter, "c1", "alice", "a")))
	c.mgr.sleep = func(_ context.Context, d time.Duration) error {
		clk.ns.Add(int64(d))
		store.members, store.asOf = []*protocol.PresenceMessage{pres("012", 0, protocol.PresenceEnter, "c2", "bob", "b")}, "012"
		c.Discontinuity(storage.DiscontinuityLogGap)
		return nil
	}
	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(setOf(snap.Members)) != "[c2:bob=b]" || store.calls.Load() != 2 {
		t.Errorf("set = %v after %d store reads, want the re-seeded [c2:bob=b] after 2", setOf(snap.Members), store.calls.Load())
	}
}

// TestChannelWithoutSubscribersDropsMemberSet: the local member set is
// folded from the delivered cms, and the bus sweep is what repairs one
// lost on the bus, but the sweep skips a channel with no attachment and
// no member of its own (DESIGN.md §7.2, §12.4). So such a channel drops its set when the sweep finds it
// without subscribers, and the next attach's SYNC seeds afresh from the
// store: an operation lost while nobody was attached is not served
// stale. While an attachment is open, or a member of this node's
// remains, the set is kept and the channel counts as subscribed.
func TestChannelWithoutSubscribersDropsMemberSet(t *testing.T) {
	ctx := context.Background()
	store := &membersStore{members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")}, asOf: "005"}
	c, _ := testChannel(t, store, "005")

	// An attachment seeds the set; while it is open the set is kept.
	if _, err := c.pin(ctx, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}
	if !c.HasSubscribers() {
		t.Fatal("a channel with an open attachment reports no subscribers")
	}
	if _, err := c.PresenceSync(ctx); err != nil || store.calls.Load() != 1 {
		t.Fatalf("SYNC with the attachment open: %d store reads (err %v), want the one seed", store.calls.Load(), err)
	}

	// A member of this node's keeps the channel subscribed, and the set
	// with it, after the attachment closes.
	c.Append(presCM("006", op(protocol.PresenceEnter, "c2", "bob", "b")))
	c.unpin(true)
	if !c.HasSubscribers() {
		t.Fatal("a channel with a tracked presence member reports no subscribers")
	}
	if !c.pv.seeded {
		t.Fatal("the member set was dropped while the channel still had a subscriber")
	}

	// bob leaves; nobody is attached. The sweep's check drops the set.
	c.Append(presCM("007", op(protocol.PresenceLeave, "c2", "bob", "")))
	if c.HasSubscribers() {
		t.Fatal("a channel with no attachment and no member reports subscribers")
	}
	if c.pv.seeded {
		t.Fatal("a channel the sweep skips kept its member set")
	}

	// alice's LEAVE is lost on the bus while nobody is attached (the store
	// has it, this node never delivered it). The next attach's SYNC seeds
	// again and does not serve alice.
	store.members, store.asOf = nil, "008"
	if _, err := c.pin(ctx, true); err != nil {
		t.Fatal(err)
	}
	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Members) != 0 || store.calls.Load() != 2 {
		t.Errorf("SYNC after re-attach = %v with %d store reads, want empty from a fresh seed (2 reads)", setOf(snap.Members), store.calls.Load())
	}
	c.unpin(true)
}

// TestChannelDropDuringSeedRetriesTheSeed: the sweep finds the channel
// with no subscribers while a seed read is in flight (an attachment that
// closed mid-seed, or one not yet counted). The drop discards that read,
// and the SYNC waiting on it seeds again, so it never serves a set the
// drop meant to discard.
func TestChannelDropDuringSeedRetriesTheSeed(t *testing.T) {
	ctx := context.Background()
	store := &membersStore{
		members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")}, asOf: "005",
		release: make(chan struct{}), entered: make(chan struct{}),
	}
	c, _ := testChannel(t, store, "005")
	done := make(chan *PresenceSnapshot, 1)
	go func() {
		snap, err := c.PresenceSync(ctx)
		if err != nil {
			t.Error(err)
		}
		done <- snap
	}()
	<-store.entered
	if c.HasSubscribers() {
		t.Fatal("a channel with no attachment and no member reports subscribers")
	}
	// The store moved on while the first read was in flight.
	store.members, store.asOf = []*protocol.PresenceMessage{pres("009", 0, protocol.PresenceEnter, "c2", "bob", "b")}, "009"
	close(store.release)
	snap := <-done
	if n := store.calls.Load(); n != 2 {
		t.Errorf("store reads = %d, want 2 (the discarded seed, then a fresh one)", n)
	}
	if got := setOf(snap.Members); fmt.Sprint(got) != "[c2:bob=b]" {
		t.Errorf("set = %v, want the fresh seed [c2:bob=b]", got)
	}
}

// TestDiscontinuityMarkerReachesEveryStreamInOrder: a discontinuity the
// backend signals between two appends (storage.Discontinuous, DESIGN.md
// §7.2) reaches every open Stream as a marker between the two cms, with
// the serial of the cm before it, parked Streams are woken for it, and
// the local member set is dropped so the next SYNC re-seeds.
func TestDiscontinuityMarkerReachesEveryStreamInOrder(t *testing.T) {
	store := &membersStore{members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")}, asOf: "005"}
	c, _ := testChannel(t, store, "005")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.PresenceSync(ctx); err != nil {
		t.Fatal(err)
	}

	const streams = 3
	type step struct {
		serial string
		marker bool
	}
	results := make(chan []step, streams)
	parked := make(chan struct{}, streams)
	for range streams {
		s, err := c.Attach(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer s.Close()
			var got []step
			parked <- struct{}{}
			for len(got) < 3 {
				cm, err := s.Next(ctx)
				if err != nil {
					t.Errorf("Next: %v", err)
					break
				}
				got = append(got, step{cm.ChannelSerial, s.Discontinuity()})
				if s.Discontinuity() && (len(cm.Messages) != 0 || s.ChannelSerial() != "006") {
					t.Errorf("marker carries %d messages at %q, want none at 006", len(cm.Messages), s.ChannelSerial())
				}
			}
			results <- got
		}()
	}
	for range streams {
		<-parked
	}

	c.Append(newCM("006", "m1"))
	c.Discontinuity(storage.DiscontinuityRetention)
	c.Append(newCM("008", "m2"))

	want := []step{{"006", false}, {"006", true}, {"008", false}}
	for range streams {
		select {
		case got := <-results:
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("stream saw %v, want %v", got, want)
			}
		case <-ctx.Done():
			t.Fatal("a stream did not see all three entries")
		}
	}

	// The set was dropped with the marker: the next SYNC reads the store.
	store.members, store.asOf = nil, "008"
	snap, err := c.PresenceSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Members) != 0 || store.calls.Load() != 2 {
		t.Errorf("after the marker: set %v with %d store reads, want empty after a re-seed", setOf(snap.Members), store.calls.Load())
	}
}

// TestDiscontinuityBeforeInitializeLinksNoMarker: before Initialize no
// Stream can exist, so a discontinuity only drops the member set and the
// list stays as it was.
func TestDiscontinuityBeforeInitializeLinksNoMarker(t *testing.T) {
	c := newChannel("room")
	head := c.tail
	c.Discontinuity(storage.DiscontinuityLogGap)
	if c.tail != head || head.next != nil {
		t.Error("a marker was linked before Initialize")
	}
}

// TestPresenceSyncSeedOutlivesCancelledStarter: the seed read is shared,
// so the attach that started it closing does not cancel it. The other
// waiters get the set from that one read instead of each starting
// another (DESIGN.md §12.4).
func TestPresenceSyncSeedOutlivesCancelledStarter(t *testing.T) {
	store := &membersStore{
		members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")},
		asOf:    "005",
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	c, _ := testChannel(t, store, "005")

	starter, cancel := context.WithCancel(context.Background())
	starterErr := make(chan error, 1)
	go func() {
		_, err := c.PresenceSync(starter)
		starterErr <- err
	}()
	<-store.entered // the starter's seed read is in flight

	type result struct {
		snap *PresenceSnapshot
		err  error
	}
	waiter := make(chan result, 1)
	go func() {
		snap, err := c.PresenceSync(context.Background())
		waiter <- result{snap, err}
	}()
	waitForSeedWaiters(t, c, 2)
	cancel()
	if err := <-starterErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("starter err = %v, want context.Canceled", err)
	}
	close(store.release)
	r := <-waiter
	if r.err != nil {
		t.Fatalf("waiter: %v", r.err)
	}
	if got := setOf(r.snap.Members); fmt.Sprint(got) != "[c1:alice=a]" {
		t.Errorf("waiter set = %v, want [c1:alice=a]", got)
	}
	if n := store.calls.Load(); n != 1 {
		t.Errorf("store Members calls = %d, want 1: the starter's cancellation restarted the seed", n)
	}
}

// waitForSeedWaiters waits until n attaches are waiting on c's in-flight
// seed read.
func waitForSeedWaiters(t *testing.T, c *Channel, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		got := 0
		if run := c.pv.seeding; run != nil {
			got = run.waiters
		}
		c.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed waiters = %d, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPresenceSyncSeedCancelledWhenAbandoned: once no attach is waiting
// for the seed read it is cancelled, so it does not hold a pool
// connection for nobody; the next attach seeds afresh (DESIGN.md §12.4).
func TestPresenceSyncSeedCancelledWhenAbandoned(t *testing.T) {
	store := &membersStore{
		members: []*protocol.PresenceMessage{pres("005", 0, protocol.PresenceEnter, "c1", "alice", "a")},
		asOf:    "005",
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	c, _ := testChannel(t, store, "005")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.PresenceSync(ctx)
		errc <- err
	}()
	<-store.entered
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// The abandoned read ends (membersStore returns on its context);
	// the next attach does not inherit its cancellation.
	close(store.release)
	snap, err := c.PresenceSync(context.Background())
	if err != nil {
		t.Fatalf("PresenceSync after the abandoned seed: %v", err)
	}
	if got := setOf(snap.Members); fmt.Sprint(got) != "[c1:alice=a]" {
		t.Errorf("set = %v, want [c1:alice=a]", got)
	}
	if n := store.calls.Load(); n != 2 {
		t.Errorf("store Members calls = %d, want 2 (the abandoned seed, then a fresh one)", n)
	}
}

// TestPresenceSyncSeedErrorReachesEveryWaiter: a seed read that fails
// fails for every attach waiting on it, and each reads the store for its
// SYNC; the next attach tries to seed again (DESIGN.md §12.4).
func TestPresenceSyncSeedErrorReachesEveryWaiter(t *testing.T) {
	store := &membersStore{
		asOf:    "005",
		err:     errors.New("database down"),
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	c, _ := testChannel(t, store, "005")
	errc := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := c.PresenceSync(context.Background())
			errc <- err
		}()
	}
	<-store.entered
	waitForSeedWaiters(t, c, 2)
	close(store.release)
	for range 2 {
		if err := <-errc; err == nil || err.Error() != "database down" {
			t.Errorf("err = %v, want the store's error from the fallback read", err)
		}
	}
	// One seed read, then one fallback store read per waiter.
	if n := store.calls.Load(); n != 3 {
		t.Errorf("store Members calls = %d, want 3", n)
	}
	c.mu.Lock()
	seeded, seeding := c.pv.seeded, c.pv.seeding
	c.mu.Unlock()
	if seeded || seeding != nil {
		t.Errorf("after a failed seed: seeded %v, seeding %v; want neither, so the next SYNC seeds again", seeded, seeding)
	}
}
