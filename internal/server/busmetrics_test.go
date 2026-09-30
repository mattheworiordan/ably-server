package server

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/storage/postgres"
)

type fakeBus struct{ st postgres.BusStats }

func (f fakeBus) BusStats() postgres.BusStats { return f.st }

// TestBusCollectorsExposeBusStats checks the cluster bus counters reach
// the /metrics exposition.
func TestBusCollectorsExposeBusStats(t *testing.T) {
	m := metrics.New()
	m.Register(busCollectors(fakeBus{st: postgres.BusStats{Notifications: 7, Inline: 3, FastPath: 2, WakeupsSent: 5, BoundChannels: 4}})...)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		"ably_pgbus_notifications_received_total 7",
		"ably_pgbus_inline_deliveries_total 3",
		"ably_pgbus_fast_path_deliveries_total 2",
		"ably_pgbus_wakeups_sent_total 5",
		"ably_pgbus_bound_channels 4",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}
