package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
)

// Unit tests for the pgnotify bind (pgnotify.go), taken from WS3's
// scale/channel-lifecycle (582331f) so both branches test one contract.

// recordingAppender records Initialize and Append calls in order.
type recordingAppender struct {
	mu      sync.Mutex
	current string
	events  []string // "init:<current>" or the appended cm's serial
}

func (a *recordingAppender) Initialize(current, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current = current
	a.events = append(a.events, "init:"+current)
}

func (a *recordingAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, cm.ChannelSerial)
}

func (a *recordingAppender) got() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.events...)
}

func cmAt(serial string) *protocol.ChannelMessage {
	return &protocol.ChannelMessage{ChannelSerial: serial, Messages: []*protocol.Message{{Name: "x"}}}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A NOTIFY can reach a channelStore between its dispatch-map insert and
// the watermark read (DESIGN.md §5.1, §7.2). Such cms must be held until
// Initialize, then replayed in order, dropping those the watermark
// already covers — never appended before Initialize.
func TestDeliverBeforeInitializeIsHeldAndFiltered(t *testing.T) {
	a := &recordingAppender{}
	cs := &channelStore{name: "c", appender: a}

	cs.deliver(cmAt("003"))
	cs.deliver(cmAt("001")) // at or below the watermark: covered by it
	cs.deliver(cmAt("002"))
	if got := a.got(); len(got) != 0 {
		t.Fatalf("appender saw %v before Initialize", got)
	}

	if err := cs.initialize(context.Background(), "002", "000"); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	cs.deliver(cmAt("003")) // duplicate of a held cm
	cs.deliver(cmAt("004"))

	want := []string{"init:002", "003", "004"}
	if got := a.got(); !equal(got, want) {
		t.Fatalf("appender events = %v, want %v", got, want)
	}
}

// A LISTEN reconnect while a channel is binding asks for a reconcile
// before the watermark is known. initialize must run it from the
// watermark and merge it with the NOTIFYs held meanwhile, so a held
// NOTIFY for a later serial cannot push the high-water mark past cms
// whose NOTIFYs were lost in the drop.
func TestReconcileDuringBindIsMergedWithHeldNotifies(t *testing.T) {
	a := &recordingAppender{}
	cs := &channelStore{name: "c", appender: a}
	var loads []string
	cs.loadAfterFn = func(_ context.Context, after string) ([]*protocol.ChannelMessage, error) {
		loads = append(loads, after)
		return []*protocol.ChannelMessage{cmAt("011"), cmAt("012"), cmAt("013")}, nil
	}

	// No pool: a reconcile that reached the database would panic.
	if err := cs.reconcileFromHistory(context.Background()); err != nil {
		t.Fatalf("reconcileFromHistory: %v", err)
	}
	if len(loads) != 0 {
		t.Fatal("reconcile before initialize read the log without a watermark")
	}
	cs.deliver(cmAt("013")) // NOTIFY after the reconnect; 011 and 012 were lost

	if err := cs.initialize(context.Background(), "010", "000"); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if want := []string{"010"}; !equal(loads, want) {
		t.Fatalf("log reads after = %v, want %v", loads, want)
	}
	want := []string{"init:010", "011", "012", "013"}
	if got := a.got(); !equal(got, want) {
		t.Fatalf("appender events = %v, want %v", got, want)
	}
}

// A second reconcile request (another LISTEN drop) that lands while the
// bind-time log read is in flight is honoured before the store goes
// live, and a NOTIFY that arrives during the read is held, not appended
// ahead of the read's results.
func TestReconcileRequestedAgainDuringBindRead(t *testing.T) {
	a := &recordingAppender{}
	cs := &channelStore{name: "c", appender: a}
	calls := 0
	cs.loadAfterFn = func(_ context.Context, after string) ([]*protocol.ChannelMessage, error) {
		calls++
		if calls == 1 {
			// While this read runs: a NOTIFY for 014 and another reconnect.
			cs.deliver(cmAt("014"))
			if err := cs.reconcileFromHistory(context.Background()); err != nil {
				t.Errorf("reconcileFromHistory: %v", err)
			}
			return []*protocol.ChannelMessage{cmAt("011")}, nil
		}
		if after != "014" {
			t.Errorf("second log read after %q, want after the high-water mark 014", after)
		}
		return []*protocol.ChannelMessage{cmAt("015")}, nil
	}
	_ = cs.reconcileFromHistory(context.Background())
	if err := cs.initialize(context.Background(), "010", "000"); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if calls != 2 {
		t.Fatalf("log reads = %d, want 2", calls)
	}
	want := []string{"init:010", "011", "014", "015"}
	if got := a.got(); !equal(got, want) {
		t.Fatalf("appender events = %v, want %v", got, want)
	}
	cs.deliver(cmAt("016"))
	if got := a.got(); got[len(got)-1] != "016" {
		t.Fatalf("store not live after initialize: %v", got)
	}
}

func TestReleasedStoreDeliversNothing(t *testing.T) {
	a := &recordingAppender{}
	cs := &channelStore{name: "c", appender: a}
	_ = cs.initialize(context.Background(), "001", "000")
	cs.release()
	cs.deliver(cmAt("002"))
	if err := cs.reconcileFromHistory(context.Background()); err != nil {
		t.Fatalf("reconcileFromHistory: %v", err)
	}
	if got, want := a.got(), []string{"init:001"}; !equal(got, want) {
		t.Fatalf("appender events = %v, want %v", got, want)
	}

	// A store released while binding never Initializes its appender.
	b := &recordingAppender{}
	cs2 := &channelStore{name: "c", appender: b}
	cs2.deliver(cmAt("005"))
	cs2.release()
	_ = cs2.initialize(context.Background(), "004", "000")
	if got := b.got(); len(got) != 0 {
		t.Fatalf("released-while-binding appender events = %v, want none", got)
	}
}

// TestFailedRepairMarksChannelDirty: a reconnect reconcile whose log read
// fails leaves the channel dirty, so the next notification repairs from
// the mark instead of delivering its own cm alone past the cms the
// failed read never delivered.
func TestFailedRepairMarksChannelDirty(t *testing.T) {
	a := &recordingAppender{}
	cs := &channelStore{name: "c", appender: a}
	if err := cs.initialize(context.Background(), "010", "000"); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	b := &pgNotifyBus{s: &Storage{logger: logging.Default()}}
	fail := true
	cs.loadAfterFn = func(_ context.Context, after string) ([]*protocol.ChannelMessage, error) {
		if fail {
			return nil, errors.New("injected: log read failed")
		}
		return []*protocol.ChannelMessage{cmAt("011"), cmAt("012")}, nil
	}

	b.repair(context.Background(), cs) // the reconnect's reconcile, failing
	if !cs.isDirty() {
		t.Fatal("a failed repair left the channel clean: the next NOTIFY would skip the missed cms")
	}
	fail = false
	b.repair(context.Background(), cs) // the next notification repairs
	if cs.isDirty() {
		t.Error("a successful repair left the channel dirty")
	}
	if want := []string{"init:010", "011", "012"}; !equal(a.got(), want) {
		t.Fatalf("appender events = %v, want %v", a.got(), want)
	}
}
