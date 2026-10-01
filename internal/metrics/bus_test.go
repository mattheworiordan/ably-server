package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// TestBusRejectionReasons checks the unrouted and malformed counters are
// split by reason (DESIGN.md §7.2, §10): each reason's series is its own
// count, and the rest of the total is the original reason.
func TestBusRejectionReasons(t *testing.T) {
	m := New()
	m.RegisterBus(&fakeBus{st: storage.BusStats{
		Bus: "nats", Unrouted: 5, UnroutedForeign: 2,
		Malformed: 9, MalformedSerial: 3, MalformedFuture: 4,
	}})
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`ably_bus_unrouted_total{reason="unbound"} 3`,
		`ably_bus_unrouted_total{reason="foreign"} 2`,
		`ably_bus_malformed_total{reason="decode"} 2`,
		`ably_bus_malformed_total{reason="serial"} 3`,
		`ably_bus_malformed_total{reason="future"} 4`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
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

// TestRegisterBusExposesStages checks the receive-side stage histograms
// reach /metrics as ably_bus_<stage>_seconds on StageBuckets (DESIGN.md
// §10), and that a stage the collector does not know is not exported.
func TestRegisterBusExposesStages(t *testing.T) {
	m := New()
	counts := make([]uint64, len(storage.StageBuckets))
	for i := range counts {
		counts[i] = 2
	}
	counts[0] = 1
	m.RegisterBus(&fakeBus{st: storage.BusStats{Stages: map[string]storage.LagHistogram{
		"receive_queue_wait": {Counts: counts, Count: 3, Sum: 0.5},
		"hold":               {Counts: make([]uint64, len(storage.StageBuckets))},
		"append":             {Counts: counts, Count: 2, Sum: 0.01},
		"unknown":            {Counts: counts, Count: 9},
	}}})
	body := scrape(t, m)
	for _, want := range []string{
		"# TYPE ably_bus_receive_queue_wait_seconds histogram",
		`ably_bus_receive_queue_wait_seconds_bucket{le="0.0001"} 1`,
		`ably_bus_receive_queue_wait_seconds_bucket{le="0.00025"} 2`,
		`ably_bus_receive_queue_wait_seconds_bucket{le="+Inf"} 3`,
		"ably_bus_receive_queue_wait_seconds_sum 0.5",
		"ably_bus_hold_seconds_count 0",
		"ably_bus_append_seconds_count 2",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if strings.Contains(body, "ably_bus_unknown") {
		t.Error("/metrics exports a stage the collector does not describe")
	}
}

// TestDeliveryStageMetrics checks the delivery-stage series after the
// append (DESIGN.md §10): the fan-out size gauge keeps the largest value
// since the last scrape and resets on reading, and the two histograms
// count their observations.
func TestDeliveryStageMetrics(t *testing.T) {
	m := New()
	m.DeliveryFanoutSize(5)
	m.DeliveryFanoutSize(20000)
	m.DeliveryFanoutSize(3)
	m.DeliveryFanout(2 * time.Millisecond)
	m.ConnWriteWait(300 * time.Microsecond)
	m.ConnWriteWait(time.Second)
	body := scrape(t, m)
	for _, want := range []string{
		"ably_delivery_fanout_size 20000",
		"# TYPE ably_delivery_fanout_size gauge",
		`ably_delivery_fanout_seconds_bucket{le="0.0025"} 1`,
		"ably_delivery_fanout_seconds_count 1",
		`ably_conn_write_wait_seconds_bucket{le="0.0005"} 1`,
		"ably_conn_write_wait_seconds_count 2",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if body := scrape(t, m); !strings.Contains(body, "ably_delivery_fanout_size 0") {
		t.Error("ably_delivery_fanout_size did not reset after a scrape")
	}
	var nilM *Metrics
	nilM.DeliveryFanoutSize(1)
	nilM.DeliveryFanout(time.Second)
	nilM.ConnWriteWait(time.Second)
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
