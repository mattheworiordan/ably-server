// Package metrics exposes ably-server's process-wide Prometheus metrics
// (DESIGN.md §10). The metrics here are deliberately low-cardinality:
// process-wide counters, gauges, and histograms with no per-channel,
// per-connection, or per-clientId labels (those live in a separate task).
package metrics

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ably/ably-server/internal/storage"
)

// Metrics holds the process-wide collectors and the registry they are
// registered against. All instrumentation methods are safe to call on a
// nil *Metrics: they no-op, so callers that were handed no Metrics (e.g.
// unit tests) need no guards.
type Metrics struct {
	registry *prometheus.Registry

	connectionsOpened  prometheus.Counter
	connectionsOpen    prometheus.Gauge
	connectionLifetime prometheus.Histogram
	attachments        prometheus.Counter
	messagesPublished  prometheus.Counter
	messagesDelivered  prometheus.Counter
	publishLatency     prometheus.Histogram
	httpRequests       *prometheus.CounterVec
	httpCounters       sync.Map // httpKey -> prometheus.Counter

	channelsBound        prometheus.Gauge
	channelBinds         prometheus.Counter
	channelEvictions     prometheus.Counter
	channelReleaseErrors prometheus.Counter

	slowConsumerDisconnects *prometheus.CounterVec

	presenceSyncs     *prometheus.CounterVec
	presenceSyncByKey sync.Map // snapshot label -> prometheus.Counter
	presenceSeeds     prometheus.Counter
}

// New builds a Metrics with its own registry (so instances are isolated
// and tests don't collide on the global default registry) and registers
// the process collectors — Go runtime and process stats plus the
// ably-server series enumerated in DESIGN.md §10.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		connectionsOpened: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_connections_opened_total",
			Help: "Total WebSocket connections opened (successful upgrades).",
		}),
		connectionsOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ably_connections_open",
			Help: "Current number of open WebSocket connections.",
		}),
		connectionLifetime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_connection_lifetime_seconds",
			Help:    "WebSocket connection lifetime in seconds, from upgrade to teardown.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		attachments: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_attachments_total",
			Help: "Total channel attachments established.",
		}),
		messagesPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_messages_published_total",
			Help: "Total inbound publishes accepted (WebSocket and REST).",
		}),
		messagesDelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_messages_delivered_total",
			Help: "Total outbound MESSAGE frames forwarded to attachments.",
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_publish_latency_seconds",
			Help:    "Latency from an inbound publish to its storage commit / ACK, in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_http_requests_total",
			Help: "Total HTTP requests, labelled by matched route pattern, method, and response status.",
		}, []string{"route", "method", "status"}),
		channelsBound: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ably_channels_bound",
			Help: "Channels currently bound on this node (live list plus storage binding).",
		}),
		channelBinds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_channel_binds_total",
			Help: "Total channel binds on this node: first use of a channel, or a rebind after eviction.",
		}),
		channelEvictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_channel_evictions_total",
			Help: "Total idle channels evicted on this node.",
		}),
		channelReleaseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_channel_release_errors_total",
			Help: "Total storage Release calls that returned an error during eviction.",
		}),
		slowConsumerDisconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_slow_consumer_disconnects_total",
			Help: "Total WebSocket connections disconnected for not reading fast enough, by reason (queue_full: the outbound queue stayed at its byte limit for the write timeout; write_timeout: a frame write missed its deadline).",
		}, []string{"reason"}),
		presenceSyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_presence_syncs_total",
			Help: "Presence SYNC snapshots served on attach or client SYNC, by how the snapshot was obtained (DESIGN.md §12.4): cached (the channel's current snapshot), stale (a snapshot within the refresh window plus the presence events after it), built (rebuilt from the local member set), store (read from the store: --presence-sync-source=store), fallback (read from the store because seeding the local set failed).",
		}, []string{"snapshot"}),
		presenceSeeds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_presence_sync_seeds_total",
			Help: "Local presence member sets seeded from the store: at most one per channel bind (DESIGN.md §12.4).",
		}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.connectionsOpened,
		m.connectionsOpen,
		m.connectionLifetime,
		m.attachments,
		m.messagesPublished,
		m.messagesDelivered,
		m.publishLatency,
		m.httpRequests,
		m.channelsBound,
		m.channelBinds,
		m.channelEvictions,
		m.channelReleaseErrors,
		m.slowConsumerDisconnects,
		m.presenceSyncs,
		m.presenceSeeds,
	)
	return m
}

// RegisterBus exports a cluster bus's counters (DESIGN.md §7.2, §10) as
// ably_bus_* series, read from src on every scrape. No-op on a nil
// Metrics.
func (m *Metrics) RegisterBus(src storage.BusStatser) {
	if m == nil {
		return
	}
	m.registry.MustRegister(newBusCollector(src))
}

// Register adds further collectors to the registry, for components that
// own their own series (the Postgres backend's ably_storage_* retention
// series). A nil Metrics ignores the call.
func (m *Metrics) Register(cs ...prometheus.Collector) {
	if m == nil {
		return
	}
	m.registry.MustRegister(cs...)
}

// Handler returns the HTTP handler that serves the registry in the
// Prometheus text exposition format. Registered at /metrics on the debug
// listener (--debug-listen), alongside pprof — not the main listener
// (DESIGN.md §10).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ConnectionOpened records a successful WebSocket upgrade: it bumps the
// opened counter and the current-open gauge.
func (m *Metrics) ConnectionOpened() {
	if m == nil {
		return
	}
	m.connectionsOpened.Inc()
	m.connectionsOpen.Inc()
}

// ConnectionClosed records a WebSocket teardown: it decrements the
// current-open gauge and observes the connection's lifetime in seconds.
func (m *Metrics) ConnectionClosed(lifetimeSeconds float64) {
	if m == nil {
		return
	}
	m.connectionsOpen.Dec()
	m.connectionLifetime.Observe(lifetimeSeconds)
}

// AttachmentOpened records a channel attachment being established.
func (m *Metrics) AttachmentOpened() {
	if m == nil {
		return
	}
	m.attachments.Inc()
}

// MessagePublished records one accepted inbound publish and observes the
// latency (in seconds) from the inbound publish to its storage commit.
func (m *Metrics) MessagePublished(latencySeconds float64) {
	if m == nil {
		return
	}
	m.messagesPublished.Inc()
	m.publishLatency.Observe(latencySeconds)
}

// MessageDelivered records one outbound MESSAGE frame forwarded to an
// attachment.
func (m *Metrics) MessageDelivered() {
	if m == nil {
		return
	}
	m.messagesDelivered.Inc()
}

// HTTPRequest records one served HTTP request against the matched route
// pattern, method, and response status.
func (m *Metrics) HTTPRequest(route, method string, status int) {
	if m == nil {
		return
	}
	key := httpKey{route: route, method: method, status: status}
	if c, ok := m.httpCounters.Load(key); ok {
		c.(prometheus.Counter).Inc()
		return
	}
	c := m.httpRequests.WithLabelValues(route, method, strconv.Itoa(status))
	m.httpCounters.Store(key, c)
	c.Inc()
}

// httpKey identifies one ably_http_requests_total series. HTTPRequest
// caches each series' counter under it, so a request costs one map load
// rather than a label hash, an Itoa and a vector lookup (DESIGN.md §2.2).
type httpKey struct {
	route, method string
	status        int
}

// ChannelBound records a channel bind (first use or rebind after
// eviction): it bumps the binds counter and the bound gauge.
func (m *Metrics) ChannelBound() {
	if m == nil {
		return
	}
	m.channelBinds.Inc()
	m.channelsBound.Inc()
}

// ChannelEvicted records an idle channel eviction: it bumps the
// evictions counter and decrements the bound gauge.
func (m *Metrics) ChannelEvicted() {
	if m == nil {
		return
	}
	m.channelEvictions.Inc()
	m.channelsBound.Dec()
}

// ChannelReleaseError records a storage Release failure during eviction.
func (m *Metrics) ChannelReleaseError() {
	if m == nil {
		return
	}
	m.channelReleaseErrors.Inc()
}

// SlowConsumerDisconnect records a connection disconnected for not
// reading fast enough; reason is "queue_full" or "write_timeout".
func (m *Metrics) SlowConsumerDisconnect(reason string) {
	if m == nil {
		return
	}
	m.slowConsumerDisconnects.WithLabelValues(reason).Inc()
}

// PresenceSync records one SYNC snapshot served, by how it was obtained
// (the ably_presence_syncs_total snapshot label).
func (m *Metrics) PresenceSync(snapshot string) {
	if m == nil {
		return
	}
	c, ok := m.presenceSyncByKey.Load(snapshot)
	if !ok {
		c, _ = m.presenceSyncByKey.LoadOrStore(snapshot, m.presenceSyncs.WithLabelValues(snapshot))
	}
	c.(prometheus.Counter).Inc()
}

// PresenceSeed records a local member set seeded from the store.
func (m *Metrics) PresenceSeed() {
	if m == nil {
		return
	}
	m.presenceSeeds.Inc()
}
