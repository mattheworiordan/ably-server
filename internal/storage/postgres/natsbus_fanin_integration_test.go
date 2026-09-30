//go:build integration

package postgres

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// settledGoroutines returns the goroutine count once it has stopped
// moving: the lowest of several samples taken a little apart, so a
// transient goroutine (a pgx background read, a timer) is not counted.
func settledGoroutines() int {
	low := runtime.NumGoroutine()
	for range 10 {
		time.Sleep(20 * time.Millisecond)
		low = min(low, runtime.NumGoroutine())
	}
	return low
}

// TestNATSBusGoroutinesDoNotScaleWithChannels: binding N channels on
// the nats bus adds O(1) goroutines, not O(N) (DESIGN.md §7.2, receive
// fan-in). nats.go's async Subscribe starts one goroutine per
// subscription; the bus instead feeds every subscription into a fixed
// set of dispatch shards. Each channel still has its own subject (the
// node holds one subscription per bound channel), and delivery on every
// channel still works cross-node.
func TestNATSBusGoroutinesDoNotScaleWithChannels(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1 := openNATSNode(t, dsn, n.URL)
	s2 := openNATSNode(t, dsn, n.URL)
	bus2 := natsBusOf(t, s2)

	// Warm up: a first bind grows the pool and starts what a bind
	// starts once.
	for i := range 10 {
		bindChannel(t, s2, fmt.Sprintf("warm-%d", i), &cmRecorder{})
	}
	before := settledGoroutines()

	const channels = 1000
	recs := make([]*cmRecorder, channels)
	for i := range channels {
		recs[i] = &cmRecorder{}
		bindChannel(t, s2, fmt.Sprintf("ch-%d", i), recs[i])
	}
	after := settledGoroutines()

	if got := bus2.nc.NumSubscriptions(); got != channels+10 {
		t.Fatalf("node2 holds %d NATS subscriptions, want %d (one per bound channel)", got, channels+10)
	}
	grew := after - before
	t.Logf("binding %d channels: goroutines %d -> %d (%+d, %.3f per channel)", channels, before, after, grew, float64(grew)/channels)
	if grew > 20 {
		t.Fatalf("binding %d channels added %d goroutines; want O(1) (at most 20)", channels, grew)
	}

	// Delivery still reaches every channel, cross-node, in order.
	want := make([][]string, channels)
	for i := 0; i < channels; i += 97 {
		ch1 := bindChannel(t, s1, fmt.Sprintf("ch-%d", i), &cmRecorder{})
		for j := range 3 {
			cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("m-%d-%d", i, j)}})
			if err != nil {
				t.Fatalf("Store ch-%d: %v", i, err)
			}
			want[i] = append(want[i], cm.ChannelSerial)
		}
	}
	for i := 0; i < channels; i += 97 {
		recs[i].waitFor(t, len(want[i]), 5*time.Second)
		assertSerials(t, fmt.Sprintf("node2 ch-%d", i), recs[i].serials(), want[i])
	}
}
