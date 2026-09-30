package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/storage"
)

// busMetric is one ably_bus_* series: how to read it from a BusStats
// snapshot, and whether it is a counter or a gauge.
type busMetric struct {
	desc  *prometheus.Desc
	gauge bool
	get   func(storage.BusStats) float64
}

// busCollector exposes a cluster bus's counters (DESIGN.md §7.2) as
// ably_bus_* series. It reads one BusStats snapshot per scrape. The
// series show how much the bus carries, which path each delivery took
// (inline, read back, publisher fast path, log range read), how often the
// recovery paths ran (holds, gap fills, reconciles, sweeps) and, in the
// postgres bus's coalesced mode, the wake-ups and the overflow policy at
// work. ably_bus_info carries the bus kind and notify mode as labels.
type busCollector struct {
	src     storage.BusStatser
	info    *prometheus.Desc
	metrics []busMetric
}

func newBusCollector(src storage.BusStatser) *busCollector {
	u := func(f func(storage.BusStats) uint64) func(storage.BusStats) float64 {
		return func(s storage.BusStats) float64 { return float64(f(s)) }
	}
	c := &busCollector{
		src: src,
		info: prometheus.NewDesc("ably_bus_info",
			"The cluster bus this node runs (label bus) and the postgres bus notify mode (label mode). Always 1.",
			[]string{"bus", "mode"}, nil),
	}
	add := func(name, help string, gauge bool, get func(storage.BusStats) float64) {
		c.metrics = append(c.metrics, busMetric{desc: prometheus.NewDesc("ably_bus_"+name, help, nil, nil), gauge: gauge, get: get})
	}
	add("connected", "1 while the bus connection (NATS, or the LISTEN connection) is up.", true, func(s storage.BusStats) float64 {
		if s.Connected {
			return 1
		}
		return 0
	})
	add("bound_channels", "Channels bound on this node.", true, func(s storage.BusStats) float64 { return float64(s.BoundChannels) })
	add("receive_queue_depth", "Bus messages received and waiting to be dispatched to their channel (nats bus dispatch shards; 0 on other buses).", true, func(s storage.BusStats) float64 { return float64(s.ReceiveQueueDepth) })
	add("published_total", "Bus messages sent for committed cms (NATS publishes, or NOTIFYs in committed publish transactions).", false, u(func(s storage.BusStats) uint64 { return s.Published }))
	add("publish_errors_total", "NATS publishes that failed after commit (receivers recover the cm from the log).", false, u(func(s storage.BusStats) uint64 { return s.PublishErrors }))
	add("pointers_total", "cms sent as a (channel, serial) pointer because they were too big to inline.", false, u(func(s storage.BusStats) uint64 { return s.Pointers }))
	add("received_total", "Bus messages received.", false, u(func(s storage.BusStats) uint64 { return s.Received }))
	add("unrouted_total", "Bus messages for a channel with no bound store.", false, u(func(s storage.BusStats) uint64 { return s.Unrouted }))
	add("malformed_total", "Bus messages that did not decode.", false, u(func(s storage.BusStats) uint64 { return s.Malformed }))
	add("inline_deliveries_total", "cms delivered from a body carried by the bus message.", false, u(func(s storage.BusStats) uint64 { return s.Inline }))
	add("fetched_deliveries_total", "cms delivered after a read-back by (channel, serial).", false, u(func(s storage.BusStats) uint64 { return s.Fetched }))
	add("fast_path_deliveries_total", "cms the publishing node delivered to its own subscribers straight after commit.", false, u(func(s storage.BusStats) uint64 { return s.FastPath }))
	add("filled_deliveries_total", "cms delivered by log range reads (gap fills, reconciles, sweeps, wake-ups).", false, u(func(s storage.BusStats) uint64 { return s.Filled }))
	add("duplicates_total", "Offers dropped because the cm was already delivered.", false, u(func(s storage.BusStats) uint64 { return s.Duplicates }))
	add("holds_total", "cms that arrived ahead of their predecessor and were held.", false, u(func(s storage.BusStats) uint64 { return s.Held }))
	add("gap_fills_total", "Log reads that filled a gap the bus left.", false, u(func(s storage.BusStats) uint64 { return s.GapFills }))
	add("fetch_errors_total", "Failed log reads on the delivery path.", false, u(func(s storage.BusStats) uint64 { return s.FetchErrors }))
	add("drops_total", "Bus messages dropped before delivery (NATS slow consumer, full postgres-bus queue); recovered from the log.", false, u(func(s storage.BusStats) uint64 { return s.Drops }))
	add("reconciles_total", "Reconciles after a bus reconnect.", false, u(func(s storage.BusStats) uint64 { return s.ReconcileRuns }))
	add("reconciled_channels_total", "Channels caught up by reconciles.", false, u(func(s storage.BusStats) uint64 { return s.Reconciles }))
	add("reconcile_seconds_total", "Total time spent in reconciles.", false, func(s storage.BusStats) float64 { return s.ReconcileSeconds })
	add("sweeps_total", "Watermark sweeps.", false, u(func(s storage.BusStats) uint64 { return s.Sweeps }))
	add("sweep_catch_ups_total", "Channels a sweep found behind and caught up.", false, u(func(s storage.BusStats) uint64 { return s.SweepCatchUps }))
	add("sweep_seconds_total", "Total time spent in watermark sweeps.", false, func(s storage.BusStats) float64 { return s.SweepSeconds })
	add("listens_total", "LISTEN statements issued by the postgres bus.", false, u(func(s storage.BusStats) uint64 { return s.Listens }))
	add("unlistens_total", "UNLISTEN statements issued by the postgres bus.", false, u(func(s storage.BusStats) uint64 { return s.Unlistens }))
	add("coalesced_wakeups_sent_total", "Coalesced-mode wake-ups sent.", false, u(func(s storage.BusStats) uint64 { return s.WakeupsSent }))
	add("coalesced_wakeups_received_total", "Coalesced-mode wake-ups received.", false, u(func(s storage.BusStats) uint64 { return s.WakeupsReceived }))
	add("coalesced_flushes_total", "Coalesced-mode wake-up statements sent.", false, u(func(s storage.BusStats) uint64 { return s.Flushes }))
	add("coalesced_flush_errors_total", "Coalesced-mode wake-up statements that failed (retried next window).", false, u(func(s storage.BusStats) uint64 { return s.FlushErrors }))
	add("coalesced_flush_seconds_total", "Total time spent flushing coalesced wake-ups.", false, func(s storage.BusStats) float64 { return s.FlushSeconds })
	add("coalesced_overflow_total", "Wake-ups dropped by the coalesced overflow policy (delivered by the sweep instead).", false, u(func(s storage.BusStats) uint64 { return s.Overflow }))
	return c
}

// Describe implements prometheus.Collector.
func (c *busCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	for _, m := range c.metrics {
		ch <- m.desc
	}
}

// Collect implements prometheus.Collector: one snapshot per scrape.
func (c *busCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.src.BusStats()
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1, st.Bus, st.Mode)
	for _, m := range c.metrics {
		vt := prometheus.CounterValue
		if m.gauge {
			vt = prometheus.GaugeValue
		}
		ch <- prometheus.MustNewConstMetric(m.desc, vt, m.get(st))
	}
}
