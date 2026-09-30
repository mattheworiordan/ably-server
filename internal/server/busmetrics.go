package server

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/storage/postgres"
)

// busStatser is implemented by the cluster-mode storage (postgres.Storage),
// whose bus keeps its own counters (DESIGN.md §7.2).
type busStatser interface {
	BusStats() postgres.BusStats
}

// busCollectors exposes the cluster bus counters as Prometheus series
// (ably_pgbus_*), read from BusStats on every scrape. They show which
// path deliveries took (inline, read back, publisher fast path, gap fill,
// reconcile, coalesced pull) and how many notifications a node receives.
func busCollectors(src busStatser) []prometheus.Collector {
	counter := func(name, help string, get func(postgres.BusStats) uint64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "ably_pgbus_" + name, Help: help},
			func() float64 { return float64(get(src.BusStats())) })
	}
	return []prometheus.Collector{
		counter("notifications_received_total", "NOTIFYs received on this node's LISTEN connection.", func(s postgres.BusStats) uint64 { return s.Notifications }),
		counter("notifications_unrouted_total", "NOTIFYs for a Postgres channel with no bound store.", func(s postgres.BusStats) uint64 { return s.Unrouted }),
		counter("notifications_malformed_total", "NOTIFY payloads that did not parse.", func(s postgres.BusStats) uint64 { return s.Malformed }),
		counter("inline_deliveries_total", "cms delivered from an inline NOTIFY payload, with no read-back.", func(s postgres.BusStats) uint64 { return s.Inline }),
		counter("fetched_deliveries_total", "cms delivered after a read-back by (channel, serial).", func(s postgres.BusStats) uint64 { return s.Fetched }),
		counter("fast_path_deliveries_total", "cms the publishing node delivered to itself straight after commit.", func(s postgres.BusStats) uint64 { return s.FastPath }),
		counter("duplicates_dropped_total", "notifications dropped because the cm was already delivered.", func(s postgres.BusStats) uint64 { return s.Duplicates }),
		counter("gap_fills_total", "range reads triggered by a predecessor mismatch.", func(s postgres.BusStats) uint64 { return s.GapFills }),
		counter("reconciles_total", "per-channel range reads after a LISTEN reconnect.", func(s postgres.BusStats) uint64 { return s.Reconciles }),
		counter("filled_deliveries_total", "cms delivered by gap fills, reconciles and coalesced pulls.", func(s postgres.BusStats) uint64 { return s.Filled }),
		counter("fetch_errors_total", "failed reads on the delivery path.", func(s postgres.BusStats) uint64 { return s.FetchErrors }),
		counter("listens_total", "LISTEN statements issued, including re-LISTENs after a reconnect.", func(s postgres.BusStats) uint64 { return s.Listens }),
		counter("wakeups_sent_total", "coalesced-mode wake-ups sent by this node's notifier.", func(s postgres.BusStats) uint64 { return s.WakeupsSent }),
		counter("wakeups_received_total", "coalesced-mode wake-ups received.", func(s postgres.BusStats) uint64 { return s.Wakeups }),
		counter("pulls_total", "coalesced-mode range reads (wake-ups, polls, fast-path gaps).", func(s postgres.BusStats) uint64 { return s.Pulls }),
		counter("polls_total", "coalesced-mode safety-net polls.", func(s postgres.BusStats) uint64 { return s.Polls }),
		counter("poll_pulls_total", "range reads queued by the safety-net poll.", func(s postgres.BusStats) uint64 { return s.PollPulls }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "ably_pgbus_bound_channels", Help: "Channels this node LISTENs for."},
			func() float64 { return float64(src.BusStats().BoundChannels) }),
	}
}
