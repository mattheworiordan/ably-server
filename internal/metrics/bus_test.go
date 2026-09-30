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
		Bus: "postgres", Mode: "coalesced", Connected: true, BoundChannels: 4,
		Received: 7, Inline: 3, FastPath: 2, WakeupsSent: 5, Overflow: 1, Drops: 6, ReconcileSeconds: 1.5,
	}}
	m.RegisterBus(src)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`ably_bus_info{bus="postgres",mode="coalesced"} 1`,
		"ably_bus_connected 1",
		"ably_bus_bound_channels 4",
		"ably_bus_received_total 7",
		"ably_bus_inline_deliveries_total 3",
		"ably_bus_fast_path_deliveries_total 2",
		"ably_bus_coalesced_wakeups_sent_total 5",
		"ably_bus_coalesced_overflow_total 1",
		"ably_bus_drops_total 6",
		"ably_bus_reconcile_seconds_total 1.5",
		"# TYPE ably_bus_received_total counter",
		"# TYPE ably_bus_bound_channels gauge",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if src.calls != 1 {
		t.Errorf("BusStats called %d times in one scrape, want 1", src.calls)
	}
}
