//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// churnBinding records one binding's life: the watermark it was
// initialised at and every serial appended to it, in order.
type churnBinding struct {
	mu      sync.Mutex
	init    string
	appends []string
}

func (b *churnBinding) Initialize(current, _ string) {
	b.mu.Lock()
	b.init = current
	b.mu.Unlock()
}

func (b *churnBinding) Append(cm *protocol.ChannelMessage) {
	b.mu.Lock()
	b.appends = append(b.appends, cm.ChannelSerial)
	b.mu.Unlock()
}

func (b *churnBinding) snapshot() (string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.init, slices.Clone(b.appends)
}

// TestNATSBusServerKillDuringChurn kills the NATS server a node is
// connected to while publishes (through bound and write-only stores) and
// bind/release churn run on the same channels on three nodes of a
// three-server NATS cluster (the degraded run in the scale proof saw one
// duplicate and one serial regression in these conditions; the clean
// rerun saw none). Every binding, released or not, must have received a
// gap-free, duplicate-free prefix of its channel's log after its
// watermark, in order; the bindings held at the end must have received
// all of it.
func TestNATSBusServerKillDuringChurn(t *testing.T) {
	swapNATSTimings(t, 50*time.Millisecond, 300*time.Millisecond)

	c := pgtest.Start(t)
	cluster := natstest.StartCluster(t, 3)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	lanes := func(o *Options) { o.Batching = Batching{Lanes: 4} }
	pubNode := openNATSNode(t, dsn, cluster.URL(), lanes)
	nodes := []*Storage{openNATSNode(t, dsn, cluster.URL(), lanes), openNATSNode(t, dsn, cluster.URL(), lanes)}

	channels := []string{"churn-0", "churn-1", "churn-2", "churn-3"}
	pubStores := make(map[string]storage.ChannelStore)
	for _, ch := range channels {
		pubStores[ch] = bindChannel(t, pubNode, ch, &churnBinding{})
	}

	var (
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		bmu      sync.Mutex
		bindings = map[string][]*churnBinding{} // channel -> every binding on the churning nodes
		finals   = map[string][]*churnBinding{} // channel -> the binding each node holds at the end
	)
	record := func(ch string, b *churnBinding) {
		bmu.Lock()
		bindings[ch] = append(bindings[ch], b)
		bmu.Unlock()
	}

	// Publishers: half through the bound store, half write-only.
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ch := channels[(i+w)%len(channels)]
				st := pubStores[ch]
				if w == 1 {
					st = pubNode.UnboundChannel(ch)
				}
				if _, _, err := st.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("w%d-%d", w, i)}}); err != nil {
					t.Errorf("Store: %v", err)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	// Churners: each (node, channel) pair binds, holds for a while, and
	// releases, over and over (the caller serialises one name's binds and
	// releases, as core.Manager does).
	for _, s := range nodes {
		for _, ch := range channels {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					b := &churnBinding{}
					if _, err := s.Channel(ctx, ch, b); err != nil {
						// A bind can fail while the NATS connection is down;
						// the next attempt retries.
						time.Sleep(20 * time.Millisecond)
						continue
					}
					record(ch, b)
					select {
					case <-stop:
						// Keep this binding: it must catch up fully.
						bmu.Lock()
						finals[ch] = append(finals[ch], b)
						bmu.Unlock()
						return
					case <-time.After(time.Duration(20+rand.IntN(120)) * time.Millisecond):
					}
					if err := s.Release(ctx, ch); err != nil {
						t.Errorf("Release: %v", err)
						return
					}
				}
			}()
		}
	}

	time.Sleep(time.Second)
	bus := natsBusOf(t, nodes[0])
	killed := cluster.ByURL(bus.nc.ConnectedUrl())
	if killed == nil {
		t.Fatalf("node is connected to %q, not a cluster server", bus.nc.ConnectedUrl())
	}
	killed.Kill(t)
	waitFor(t, 30*time.Second, "the node to reconnect to another server", func() bool {
		return bus.nc.IsConnected() && bus.nc.ConnectedUrl() != killed.URL
	})
	time.Sleep(2 * time.Second) // churn and publishes carry on after the move
	close(stop)
	wg.Wait()

	logOf := func(ch string) []string {
		page, err := pubStores[ch].History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 100000})
		if err != nil {
			t.Fatalf("History %s: %v", ch, err)
		}
		out := make([]string, 0, len(page.ChannelMessages))
		for _, cm := range page.ChannelMessages {
			out = append(out, cm.ChannelSerial)
		}
		return out
	}
	after := func(log []string, mark string) []string {
		i, _ := slices.BinarySearch(log, mark)
		if i < len(log) && log[i] == mark {
			i++
		}
		return log[i:]
	}

	checked := 0
	for _, ch := range channels {
		log := logOf(ch)
		bmu.Lock()
		all := slices.Clone(bindings[ch])
		final := slices.Clone(finals[ch])
		bmu.Unlock()
		if len(final) != len(nodes) {
			t.Fatalf("%s: %d bindings held at the end, want one per node (%d)", ch, len(final), len(nodes))
		}
		// The bindings still held must catch up with the whole log.
		for _, b := range final {
			waitFor(t, 30*time.Second, ch+": a held binding to receive its whole log", func() bool {
				init, got := b.snapshot()
				return len(got) >= len(after(log, init))
			})
		}
		time.Sleep(200 * time.Millisecond) // room for a (wrong) duplicate
		for i, b := range all {
			init, got := b.snapshot()
			want := after(log, init)
			if len(got) > len(want) || !slices.Equal(got, want[:len(got)]) {
				t.Fatalf("%s binding %d (init %s): appended %v, want a prefix of %v (a gap, duplicate or reorder)", ch, i, init, got, want)
			}
			checked += len(got)
		}
		for _, b := range final {
			init, got := b.snapshot()
			if want := after(log, init); !slices.Equal(got, want) {
				t.Fatalf("%s held binding (init %s): appended %d, want all %d cms after its watermark", ch, init, len(got), len(want))
			}
		}
	}
	bmu.Lock()
	n := 0
	for _, bs := range bindings {
		n += len(bs)
	}
	bmu.Unlock()
	t.Logf("killed %s; %d bindings over %d channels, %d appends checked", killed.Name, n, len(channels), checked)
}
