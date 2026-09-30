package core

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// fakeClock is a manually advanced monotonic clock for the Manager.
type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ns.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// trackingStorage wraps a real backend and records the binding
// lifecycle per channel name, failing the test if a Channel call for a
// name overlaps a Release of the same name. releaseGate, when set,
// blocks Release until it is closed.
type trackingStorage struct {
	storage.Storage
	t *testing.T

	mu          sync.Mutex
	releasing   map[string]bool
	releases    map[string]int
	binds       map[string]int
	releaseGate chan struct{}
	storeGate   chan struct{}
}

func newTrackingStorage(t *testing.T) *trackingStorage {
	return &trackingStorage{
		Storage:   memory.New(memory.Options{}),
		t:         t,
		releasing: make(map[string]bool),
		releases:  make(map[string]int),
		binds:     make(map[string]int),
	}
}

func (s *trackingStorage) Channel(ctx context.Context, name string, a storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	if s.releasing[name] {
		s.t.Errorf("Channel(%q) called while its Release is in progress", name)
	}
	s.binds[name]++
	s.mu.Unlock()
	cs, err := s.Storage.Channel(ctx, name, a)
	if err != nil {
		return nil, err
	}
	return &gatedStore{ChannelStore: cs, parent: s}, nil
}

func (s *trackingStorage) Release(ctx context.Context, name string) error {
	s.mu.Lock()
	s.releasing[name] = true
	gate := s.releaseGate
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	err := s.Storage.Release(ctx, name)
	s.mu.Lock()
	s.releasing[name] = false
	s.releases[name]++
	s.mu.Unlock()
	return err
}

func (s *trackingStorage) counts(name string) (binds, releases int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binds[name], s.releases[name]
}

func (s *trackingStorage) setReleaseGate(g chan struct{}) {
	s.mu.Lock()
	s.releaseGate = g
	s.mu.Unlock()
}

func (s *trackingStorage) setStoreGate(g chan struct{}) {
	s.mu.Lock()
	s.storeGate = g
	s.mu.Unlock()
}

// gatedStore blocks Store on the parent's storeGate, so a test can hold
// a publish in flight across a sweep.
type gatedStore struct {
	storage.ChannelStore
	parent *trackingStorage
}

func (g *gatedStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	g.parent.mu.Lock()
	gate := g.parent.storeGate
	g.parent.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return g.ChannelStore.Store(ctx, msgs)
}

const testIdle = time.Minute

// newEvictingManager returns a Manager with a 1-minute idle timeout
// driven by a fake clock; the background sweeper is parked (1h cadence)
// so tests call sweep directly.
func newEvictingManager(t *testing.T, store storage.Storage) (*Manager, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	m := newManager(store, Options{IdleTimeout: testIdle, SweepInterval: time.Hour}, clk.now)
	t.Cleanup(m.Close)
	return m, clk
}

func publishOne(t *testing.T, ch *Channel, name string) *protocol.ChannelMessage {
	t.Helper()
	cm, _, err := ch.Publish(context.Background(), []*protocol.Message{{Name: name}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return cm
}

func TestEvictionIdleChannelIsReleased(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	ch := mustGetChannel(t, m, "idle")
	publishOne(t, ch, "a")
	if got := m.BoundChannels(); got != 1 {
		t.Fatalf("BoundChannels = %d, want 1", got)
	}

	clk.advance(testIdle - time.Second)
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep before the timeout evicted %d channels", n)
	}
	clk.advance(time.Second)
	if n := m.sweep(); n != 1 {
		t.Fatalf("sweep after the timeout evicted %d channels, want 1", n)
	}
	if got := m.BoundChannels(); got != 0 {
		t.Fatalf("BoundChannels after eviction = %d, want 0", got)
	}
	if _, releases := st.counts("idle"); releases != 1 {
		t.Fatalf("Release calls = %d, want 1", releases)
	}
}

func TestEvictionDisabledByZeroTimeout(t *testing.T) {
	m := NewManagerWithOptions(memory.New(memory.Options{}), Options{})
	defer m.Close()
	mustGetChannel(t, m, "kept")
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep with eviction disabled evicted %d", n)
	}
	if got := m.BoundChannels(); got != 1 {
		t.Fatalf("BoundChannels = %d, want 1", got)
	}
}

func TestEvictionHeldOffByOpenStream(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	ch := mustGetChannel(t, m, "attached")
	s, err := ch.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	clk.advance(10 * testIdle)
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep evicted %d channels with an open stream", n)
	}
	s.Close()
	s.Close() // idempotent
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep evicted a channel the moment its stream closed (idle clock not restarted)")
	}
	clk.advance(testIdle)
	if n := m.sweep(); n != 1 {
		t.Fatalf("sweep after close + timeout evicted %d, want 1", n)
	}
}

func TestEvictionHeldOffByInflightPublish(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	ch := mustGetChannel(t, m, "busy")

	gate := make(chan struct{})
	st.setStoreGate(gate)
	done := make(chan error, 1)
	go func() {
		_, _, err := ch.Publish(context.Background(), []*protocol.Message{{Name: "slow"}})
		done <- err
	}()
	// Wait until the publish is pinned (inflight) and blocked in Store.
	waitFor(t, func() bool {
		ch.life.Lock()
		defer ch.life.Unlock()
		return ch.inflight == 1
	})
	clk.advance(10 * testIdle)
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep evicted %d channels with a publish in flight", n)
	}
	st.setStoreGate(nil)
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("Publish: %v", err)
	}
	clk.advance(testIdle)
	if n := m.sweep(); n != 1 {
		t.Fatalf("sweep after the publish finished evicted %d, want 1", n)
	}
}

func TestEvictionHeldOffByPresenceMember(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	ch := mustGetChannel(t, m, "present")
	ctx := context.Background()
	enter := &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "c1", ConnectionID: "conn1"}
	if _, _, err := ch.PublishPresence(ctx, []*protocol.PresenceMessage{enter}); err != nil {
		t.Fatalf("enter: %v", err)
	}
	clk.advance(10 * testIdle)
	if n := m.sweep(); n != 0 {
		t.Fatalf("sweep evicted %d channels with a presence member", n)
	}
	leave := &protocol.PresenceMessage{Action: protocol.PresenceLeave, ClientID: "c1", ConnectionID: "conn1"}
	if _, _, err := ch.PublishPresence(ctx, []*protocol.PresenceMessage{leave}); err != nil {
		t.Fatalf("leave: %v", err)
	}
	clk.advance(testIdle)
	if n := m.sweep(); n != 1 {
		t.Fatalf("sweep after the member left evicted %d, want 1", n)
	}
}

func TestEvictionStaleChannelRebindsTransparently(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	stale := mustGetChannel(t, m, "rebind")
	first := publishOne(t, stale, "before")

	clk.advance(testIdle)
	if n := m.sweep(); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}

	// Operations on the stale pointer rebind through the Manager.
	second := publishOne(t, stale, "after")
	if second.ChannelSerial <= first.ChannelSerial {
		t.Fatalf("post-rebind serial %q not after %q", second.ChannelSerial, first.ChannelSerial)
	}
	if binds, _ := st.counts("rebind"); binds != 2 {
		t.Fatalf("storage binds = %d, want 2 (first use + rebind)", binds)
	}
	fresh := mustGetChannel(t, m, "rebind")
	if fresh == stale {
		t.Fatal("GetChannel returned the evicted Channel")
	}

	// The fresh channel is re-seeded from the watermark: a stream opened
	// on the stale pointer lands on the fresh Channel, at the last serial.
	s, err := stale.Attach(context.Background())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if s.Channel() != fresh {
		t.Fatal("stream on a stale Channel is not attached to the fresh Channel")
	}
	if got := s.ChannelSerial(); got != second.ChannelSerial {
		t.Fatalf("stream attach point = %q, want watermark %q", got, second.ChannelSerial)
	}
	third := publishOne(t, stale, "live")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.ChannelSerial != third.ChannelSerial {
		t.Fatalf("delivered %q, want %q", got.ChannelSerial, third.ChannelSerial)
	}

	// History spans the eviction.
	page, err := fresh.History(context.Background(), storage.HistoryQuery{Direction: storage.DirectionForwards})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(page.ChannelMessages) != 3 {
		t.Fatalf("history length = %d, want 3", len(page.ChannelMessages))
	}
	if stale.InitialChannelSerial() != fresh.InitialChannelSerial() {
		t.Fatalf("initial serial changed across rebind: %q vs %q", stale.InitialChannelSerial(), fresh.InitialChannelSerial())
	}
}

func TestEvictionGetChannelWaitsForRelease(t *testing.T) {
	st := newTrackingStorage(t)
	m, clk := newEvictingManager(t, st)
	mustGetChannel(t, m, "slow-release")
	clk.advance(testIdle)

	gate := make(chan struct{})
	st.setReleaseGate(gate)
	swept := make(chan int, 1)
	go func() { swept <- m.sweep() }()
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.releasing["slow-release"]
	})

	got := make(chan *Channel, 1)
	go func() {
		ch, err := m.GetChannel(context.Background(), "slow-release")
		if err != nil {
			t.Errorf("GetChannel: %v", err)
		}
		got <- ch
	}()
	select {
	case <-got:
		t.Fatal("GetChannel returned while the evicted channel's Release was still in progress")
	case <-time.After(50 * time.Millisecond):
	}
	st.setReleaseGate(nil)
	close(gate)
	if n := <-swept; n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	select {
	case ch := <-got:
		if ch == nil {
			t.Fatal("GetChannel returned nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetChannel did not return after Release finished")
	}
	if binds, releases := st.counts("slow-release"); binds != 2 || releases != 1 {
		t.Fatalf("binds=%d releases=%d, want 2 and 1", binds, releases)
	}
}

// TestEvictionConcurrentChurn races publishes, attaches and sweeps on a
// small set of channels with a real sweeper running flat out, checking
// that every publish made while a stream is open reaches that stream.
// Run under -race it also checks the lifecycle locking.
func TestEvictionConcurrentChurn(t *testing.T) {
	st := newTrackingStorage(t)
	m := NewManagerWithOptions(st, Options{IdleTimeout: time.Millisecond, SweepInterval: time.Millisecond})
	t.Cleanup(m.Close)

	const workers = 8
	const rounds = 200
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for r := range rounds {
				name := fmt.Sprintf("churn-%d", (w+r)%4)
				ch, err := m.GetChannel(ctx, name)
				if err != nil {
					t.Errorf("GetChannel: %v", err)
					return
				}
				if r%3 == 0 {
					time.Sleep(2 * time.Millisecond) // let the sweeper evict ch
				}
				s, err := ch.Attach(ctx)
				if err != nil {
					t.Errorf("Attach: %v", err)
					return
				}
				cm, _, err := ch.Publish(ctx, []*protocol.Message{{Name: "x"}})
				if err != nil {
					t.Errorf("Publish: %v", err)
					s.Close()
					return
				}
				// The stream must see its own publish (and possibly others'
				// first) — never miss it.
				nctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				for {
					got, err := s.Next(nctx)
					if err != nil {
						t.Errorf("stream on %s missed publish %s: %v", name, cm.ChannelSerial, err)
						break
					}
					if got.ChannelSerial == cm.ChannelSerial {
						break
					}
				}
				cancel()
				s.Close()
			}
		}()
	}
	wg.Wait()
	total := 0
	for i := range 4 {
		b, _ := st.counts(fmt.Sprintf("churn-%d", i))
		total += b
	}
	if total <= 4 {
		t.Fatalf("storage binds = %d across 4 channels: the sweeper never evicted, so the race was not exercised", total)
	}
	t.Logf("storage binds across 4 channels: %d", total)
}

// TestEvictionChurnReturnsToBaseline is the channel-churn scenario of
// the scale plan (DESIGN.md §5.1): 200k channels each touched once must
// leave the bound-channel count, and the core's heap, back at baseline
// once they have gone idle.
func TestEvictionChurnReturnsToBaseline(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 20_000
	}
	m, clk := newEvictingManager(t, nopStorage{})

	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}
	before := heap()
	ctx := context.Background()
	for i := range n {
		ch, err := m.GetChannel(ctx, "churn:"+fmt.Sprint(i))
		if err != nil {
			t.Fatalf("GetChannel: %v", err)
		}
		if _, _, err := ch.Publish(ctx, []*protocol.Message{{Name: "x"}}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if got := m.BoundChannels(); got != n {
		t.Fatalf("BoundChannels after touching = %d, want %d", got, n)
	}
	peak := heap()
	clk.advance(testIdle)
	if evicted := m.sweep(); evicted != n {
		t.Fatalf("sweep evicted %d, want %d", evicted, n)
	}
	if got := m.BoundChannels(); got != 0 {
		t.Fatalf("BoundChannels after sweep = %d, want 0 (baseline)", got)
	}
	after := heap()
	t.Logf("heap: before=%d KiB peak=%d KiB after=%d KiB for %d channels (%.0f B/channel bound)",
		before>>10, peak>>10, after>>10, n, float64(peak-before)/float64(n))
	// Go maps keep their buckets after deletes, so the shard maps retain
	// roughly one pointer-sized slot per peak channel; everything else
	// (Channel, lists, strings) must be gone. Allow 64 B per peak channel.
	if after > before+uint64(n)*64 {
		t.Fatalf("heap after eviction %d KiB exceeds baseline %d KiB + 64 B/channel", after>>10, before>>10)
	}
}

// nopStorage is a storage.Storage that keeps no per-channel state, so
// the churn test measures the core's own footprint.
type nopStorage struct{}

func (nopStorage) Channel(_ context.Context, _ string, a storage.Appender) (storage.ChannelStore, error) {
	if a != nil {
		a.Initialize("00000000000000-000@s", "00000000000000-000@s")
	}
	return nopStore{}, nil
}
func (nopStorage) Release(context.Context, string) error { return nil }
func (nopStorage) Close() error                          { return nil }

type nopStore struct{ storage.ChannelStore }

func (nopStore) Store(context.Context, []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	return &protocol.ChannelMessage{}, false, nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

// blockingBindStorage blocks Channel until release is closed, then fails
// with the caller's context error if it ended meanwhile.
type blockingBindStorage struct {
	storage.Storage
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockingBindStorage) Channel(ctx context.Context, name string, a storage.Appender) (storage.ChannelStore, error) {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		<-s.release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return s.Storage.Channel(ctx, name, a)
}

// A GetChannel that waited on another caller's bind does not inherit
// that caller's cancellation: it binds itself.
func TestGetChannelWaiterRetriesAfterBinderCancelled(t *testing.T) {
	st := &blockingBindStorage{Storage: memory.New(memory.Options{}), entered: make(chan struct{}), release: make(chan struct{})}
	m := NewManager(st)
	binderCtx, cancelBinder := context.WithCancel(context.Background())
	binderErr := make(chan error, 1)
	go func() {
		_, err := m.GetChannel(binderCtx, "shared")
		binderErr <- err
	}()
	<-st.entered
	waiter := make(chan error, 1)
	go func() {
		_, err := m.GetChannel(context.Background(), "shared")
		waiter <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter park on the bind
	cancelBinder()
	close(st.release)
	if err := <-binderErr; err == nil {
		t.Fatal("cancelled binder got no error")
	}
	if err := <-waiter; err != nil {
		t.Fatalf("waiter inherited the binder's cancellation: %v", err)
	}
}
