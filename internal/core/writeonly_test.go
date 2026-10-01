package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// unboundStorage is a trackingStorage that also offers unbound stores
// (storage.UnboundPublisher), counting how many it handed out.
type unboundStorage struct {
	*trackingStorage
	mu       sync.Mutex
	unbounds map[string]int
}

func newUnboundStorage(t *testing.T) *unboundStorage {
	return &unboundStorage{trackingStorage: newTrackingStorage(t), unbounds: map[string]int{}}
}

// UnboundChannel hands out the backend's store with no appender: memory
// then stores without delivering, as a write-only store would.
func (s *unboundStorage) UnboundChannel(name string) storage.ChannelStore {
	s.mu.Lock()
	s.unbounds[name]++
	s.mu.Unlock()
	cs, err := s.trackingStorage.Storage.Channel(context.Background(), "unbound:"+name, nil)
	if err != nil {
		s.t.Fatalf("unbound store: %v", err)
	}
	return cs
}

func (s *unboundStorage) unboundCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unbounds[name]
}

// TestWriteOnlyStoreDoesNotBind: with the write-only path on, a publish
// to a channel with no Channel on this node goes through an unbound store
// and creates no Channel, no storage binding and nothing to evict
// (DESIGN.md §5.1). A channel that is bound, or still binding, keeps the
// normal path.
func TestWriteOnlyStoreDoesNotBind(t *testing.T) {
	ctx := context.Background()
	st := newUnboundStorage(t)
	m := newManager(st, Options{WriteOnlyPublish: true}, func() int64 { return 0 })

	ws := m.WriteOnlyStore("cold")
	if ws == nil {
		t.Fatal("WriteOnlyStore(cold) = nil, want an unbound store")
	}
	if _, _, err := ws.Store(ctx, []*protocol.Message{{Name: "a"}}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if got := m.BoundChannels(); got != 0 {
		t.Fatalf("BoundChannels = %d after a write-only publish, want 0", got)
	}
	if binds, _ := st.counts("cold"); binds != 0 {
		t.Fatalf("storage Channel(cold) called %d times, want 0", binds)
	}
	sh := m.shardFor("cold")
	sh.mu.Lock()
	_, created := sh.channels["cold"]
	sh.mu.Unlock()
	if created {
		t.Fatal("a write-only publish created a core.Channel")
	}
	if got := m.sweep(); got != 0 {
		t.Fatalf("sweep evicted %d, want nothing to evict", got)
	}

	// A bound channel keeps the normal path.
	if _, err := m.GetChannel(ctx, "warm"); err != nil {
		t.Fatalf("GetChannel(warm): %v", err)
	}
	if ws := m.WriteOnlyStore("warm"); ws != nil {
		t.Fatal("WriteOnlyStore(warm) returned a store for a bound channel")
	}
	if got := st.unboundCount("warm"); got != 0 {
		t.Fatalf("unbound stores for warm = %d, want 0", got)
	}
}

// TestWriteOnlyStoreDuringBindAndEviction: a channel whose bind is in flight is not
// sent down the write-only path; one being evicted is, since its binding
// is going away.
func TestWriteOnlyStoreDuringBindAndEviction(t *testing.T) {
	st := newUnboundStorage(t)
	clk := &fakeClock{}
	m := newManager(st, Options{IdleTimeout: testIdle, SweepInterval: time.Hour, WriteOnlyPublish: true}, clk.now)
	t.Cleanup(m.Close)

	// Binding: GetChannel has put the Channel in the map but not finished.
	sh := m.shardFor("binding")
	sh.mu.Lock()
	pendingCh := newChannel("binding")
	pendingCh.mgr = m
	sh.channels["binding"] = pendingCh
	sh.mu.Unlock()
	if ws := m.WriteOnlyStore("binding"); ws != nil {
		t.Fatal("WriteOnlyStore returned a store for a channel that is binding")
	}

	// Evicting: the sweeper has marked it and its Release is blocked.
	mustGetChannel(t, m, "idle")
	clk.advance(2 * testIdle)
	gate := make(chan struct{})
	st.setReleaseGate(gate)
	done := make(chan int)
	go func() { done <- m.sweep() }()
	waitUntil(t, func() bool {
		s := m.shardFor("idle")
		s.mu.Lock()
		defer s.mu.Unlock()
		ch := s.channels["idle"]
		if ch == nil {
			return false
		}
		ch.life.Lock()
		defer ch.life.Unlock()
		return ch.evicted
	})
	if ws := m.WriteOnlyStore("idle"); ws == nil {
		t.Fatal("WriteOnlyStore(idle) = nil for a channel being evicted, want an unbound store")
	}
	close(gate)
	<-done
	if ws := m.WriteOnlyStore("idle"); ws == nil {
		t.Fatal("WriteOnlyStore(idle) = nil after eviction")
	}
}

// TestWriteOnlyStoreOff: with the option off, or a backend with no
// unbound stores, every publish goes through GetChannel.
func TestWriteOnlyStoreOff(t *testing.T) {
	m := newManager(newUnboundStorage(t), Options{}, func() int64 { return 0 })
	if ws := m.WriteOnlyStore("cold"); ws != nil {
		t.Fatal("WriteOnlyStore with WriteOnlyPublish off returned a store")
	}
	m2 := newManager(newTrackingStorage(t), Options{WriteOnlyPublish: true}, func() int64 { return 0 })
	if ws := m2.WriteOnlyStore("cold"); ws != nil {
		t.Fatal("WriteOnlyStore on a backend without unbound stores returned a store")
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}
