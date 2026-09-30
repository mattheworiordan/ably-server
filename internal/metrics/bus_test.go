package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/storage"
)

type fakeBus struct {
	st    storage.BusStats
	calls int
}

func (f *fakeBus) BusStats() storage.BusStats {
	f.calls++
	return f.st
}

// TestRegisterBusExposesBusStats checks the cluster bus counters reach
// the /metrics exposition as ably_bus_* series (DESIGN.md §10), read
// from one snapshot per scrape.
func TestRegisterBusExposesBusStats(t *testing.T) {
	m := New()
	src := &fakeBus{st: storage.BusStats{
		Bus: "postgres", Mode: "coalesced", Connected: true, BoundChannels: 4, ReceiveQueueDepth: 9,
		Received: 7, Inline: 3, FastPath: 2, WakeupsSent: 5, Overflow: 1, Drops: 6, ReconcileSeconds: 1.5,
		DeliveryLag: map[string]storage.LagHistogram{
			"inline": {Counts: lagCounts(map[float64]uint64{0.001: 1, 0.005: 3, 30: 4}), Count: 5, Sum: 40.012},
		},
	}}
	m.RegisterBus(src)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`ably_bus_info{bus="postgres",mode="coalesced"} 1`,
		"ably_bus_connected 1",
		"ably_bus_bound_channels 4",
		"ably_bus_receive_queue_depth 9",
		"# TYPE ably_bus_receive_queue_depth gauge",
		"ably_bus_received_total 7",
		"ably_bus_inline_deliveries_total 3",
		"ably_bus_fast_path_deliveries_total 2",
		"ably_bus_coalesced_wakeups_sent_total 5",
		"ably_bus_coalesced_overflow_total 1",
		"ably_bus_drops_total 6",
		"ably_bus_reconcile_seconds_total 1.5",
		"# TYPE ably_bus_received_total counter",
		"# TYPE ably_bus_bound_channels gauge",
		"# TYPE ably_bus_delivery_lag_seconds histogram",
		`ably_bus_delivery_lag_seconds_bucket{path="inline",le="0.001"} 1`,
		`ably_bus_delivery_lag_seconds_bucket{path="inline",le="0.0025"} 1`,
		`ably_bus_delivery_lag_seconds_bucket{path="inline",le="0.005"} 3`,
		`ably_bus_delivery_lag_seconds_bucket{path="inline",le="30"} 4`,
		`ably_bus_delivery_lag_seconds_bucket{path="inline",le="+Inf"} 5`,
		`ably_bus_delivery_lag_seconds_count{path="inline"} 5`,
		`ably_bus_delivery_lag_seconds_sum{path="inline"} 40.012`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if src.calls != 1 {
		t.Errorf("BusStats called %d times in one scrape, want 1", src.calls)
	}
}

// lagCounts builds cumulative storage.BusLagBuckets counts from the
// cumulative count at a few bucket bounds (each bound holds until the
// next one given).
func lagCounts(at map[float64]uint64) []uint64 {
	out := make([]uint64, len(storage.BusLagBuckets))
	var cur uint64
	for i, le := range storage.BusLagBuckets {
		if v, ok := at[le]; ok {
			cur = v
		}
		out[i] = cur
	}
	return out
}
