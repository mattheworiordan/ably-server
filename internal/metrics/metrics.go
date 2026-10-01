// Package metrics exposes ably-server's process-wide Prometheus metrics
// (DESIGN.md §10). The metrics here are deliberately low-cardinality:
// process-wide counters, gauges, and histograms with no per-channel,
// per-connection, or per-clientId labels (those live in a separate task).
package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	unboundPublishes     prometheus.Counter

	slowConsumerDisconnects *prometheus.CounterVec

	presenceSyncs     *prometheus.CounterVec
	presenceSyncByKey sync.Map // snapshot label -> prometheus.Counter
	presenceSeeds     prometheus.Counter

	channelDiscontinuities *prometheus.CounterVec
	// Presence liveness (DESIGN.md §12.5): grace-window LEAVEs that could
	// not be written, by stage, and members re-entered after a lease lapse.
	presenceGraceLeaveErrors *prometheus.CounterVec
	presenceReentries        prometheus.Counter

	// Attach and presence SYNC timing (DESIGN.md §10, §12.4): the time
	// from an ATTACH being read to its ATTACHED and its final SYNC frame
	// being written, the stages of a SYNC, the size of each SYNC frame
	// written, the SYNCs skipped by reason, and the write timeouts by the
	// action of the frame whose write missed its deadline. The label
	// children are resolved once (attachUntil etc.) so the per-attach path
	// does no label lookup.
	connectSeconds         prometheus.Histogram
	clientAttachDelay      prometheus.Histogram
	attachSeconds          *prometheus.HistogramVec
	attachAttached         prometheus.Observer
	attachSynced           prometheus.Observer
	presenceSyncStage      *prometheus.HistogramVec
	presenceSyncSnapshot   prometheus.Observer
	presenceSyncQueue      prometheus.Observer
	presenceSyncWrite      prometheus.Observer
	presenceSyncFrameBytes prometheus.Histogram
	presenceSyncsSkipped   *prometheus.CounterVec
	writeTimeouts          *prometheus.CounterVec

	// Delivery stages after Append (DESIGN.md §10): the fan-out time to
	// each sampled attachment's frame being queued, the wait of a sampled
	// connection's frame in its outbound queue, and the largest fan-out
	// of any Append since the last scrape.
	deliveryFanout prometheus.Histogram
	connWriteWait  prometheus.Histogram
	fanoutMax      atomic.Int64
}

// DeliverySampleEvery is the sampling rate of the per-attachment and
// per-connection delivery histograms (ably_delivery_fanout_seconds,
// ably_conn_write_wait_seconds): one connection in this many records
// them, so a fan-out to tens of thousands of attachments does not make
// as many observations on one histogram (DESIGN.md §10).
const DeliverySampleEvery = 8

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
		unboundPublishes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_channel_unbound_publishes_total",
			Help: "REST publishes sent down the write-only path: to a channel not bound on this node, stored without binding it.",
		}),
		slowConsumerDisconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_slow_consumer_disconnects_total",
			Help: "Total WebSocket connections disconnected for not reading fast enough, by reason (queue_full: the outbound queue stayed at its byte limit for the write timeout; write_timeout: a frame write missed its deadline).",
		}, []string{"reason"}),
		presenceSyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_presence_syncs_total",
			Help: "Presence SYNC snapshots served on attach or client SYNC, by how the snapshot was obtained (DESIGN.md §12.4): cached (the channel's current snapshot), waited (rebuilt by another attach after waiting out the refresh window), built (rebuilt from the local member set), store (read from the store: --presence-sync-source=store), fallback (read from the store because seeding the local set failed).",
		}, []string{"snapshot"}),
		presenceSeeds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_presence_sync_seeds_total",
			Help: "Local presence member sets seeded from the store: at most one per channel bind (DESIGN.md §12.4).",
		}),
		channelDiscontinuities: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_channel_discontinuities_total",
			Help: "Discontinuities signalled on channels bound on this node, each sent to every attachment as an ATTACHED without RESUMED and error 80016 (DESIGN.md §7.2), by reason: retention (a catch-up from the log started below the retention floor and could not prove nothing had aged out), log_gap (a gap the bus revealed was not in the log and was skipped).",
		}, []string{"reason"}),
		presenceGraceLeaveErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_presence_grace_leave_errors_total",
			Help: "Presence LEAVEs due at the end of an abruptly dropped connection's grace window that could not be written, by stage (get_channel, publish); the member stays until its node's lease ends (DESIGN.md §12.5).",
		}, []string{"stage"}),
		presenceReentries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_presence_reentries_total",
			Help: "Presence members re-entered by this node after its presence lease lapsed (DESIGN.md §12.5).",
		}),
		connectSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_connect_seconds",
			Help:    "Time from a WebSocket upgrade request reaching the server to its CONNECTED frame being written (DESIGN.md §10).",
			Buckets: AttachBuckets,
		}),
		clientAttachDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_client_attach_delay_seconds",
			Help:    "Time from a connection's CONNECTED frame being written to its first ATTACH being read: the client's turnaround plus the network, which the server does not control (DESIGN.md §10).",
			Buckets: AttachBuckets,
		}),
		attachSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ably_attach_seconds",
			Help:    "Time from a new attachment's ATTACH frame being read to a frame of its attach being written to the socket, by which frame (DESIGN.md §10): attached (the ATTACHED frame), synced (the final presence SYNC frame, on an attach that delivers one).",
			Buckets: AttachBuckets,
		}, []string{"until"}),
		presenceSyncStage: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ably_presence_sync_stage_seconds",
			Help:    "Stages of an attach's presence SYNC (DESIGN.md §12.4): snapshot (obtaining the snapshot: the seed read, the refresh-window wait, the rebuild), queue (encoding and queueing the SYNC frame, including backpressure), write (from queueing starting to the frame being written, so it includes queue and the frames queued ahead of it).",
			Buckets: AttachBuckets,
		}, []string{"stage"}),
		presenceSyncFrameBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_presence_sync_frame_bytes",
			Help:    "Encoded size of each presence SYNC frame written to a connection (DESIGN.md §12.4).",
			Buckets: prometheus.ExponentialBuckets(256, 2, 14),
		}),
		presenceSyncsSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_presence_syncs_skipped_total",
			Help: "Attach and client SYNCs that delivered no presence set, by reason (DESIGN.md §12.4): closed (the attachment or connection ended while the snapshot was being obtained), error (the snapshot could not be read).",
		}, []string{"reason"}),
		writeTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_write_timeouts_total",
			Help: "Socket writes that missed the write timeout, closing the connection, by the action of the frame being written (DESIGN.md §5.2).",
		}, []string{"frame"}),
		deliveryFanout: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_delivery_fanout_seconds",
			Help:    "Time from a cm's append to the channel's live list to its frame being queued on an attachment's connection, for live cms on one connection in " + strconv.Itoa(DeliverySampleEvery) + " (DESIGN.md §10).",
			Buckets: storage.StageBuckets,
		}),
		connWriteWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_conn_write_wait_seconds",
			Help:    "Time from a frame being queued on a connection's outbound queue to its socket write completing, for one connection in " + strconv.Itoa(DeliverySampleEvery) + " (DESIGN.md §10).",
			Buckets: storage.StageBuckets,
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
		m.unboundPublishes,
		m.slowConsumerDisconnects,
		m.presenceSyncs,
		m.presenceSeeds,
		m.channelDiscontinuities,
		m.presenceGraceLeaveErrors,
		m.presenceReentries,
		m.deliveryFanout,
		m.connWriteWait,
		m.connectSeconds,
		m.clientAttachDelay,
		m.attachSeconds,
		m.presenceSyncStage,
		m.presenceSyncFrameBytes,
		m.presenceSyncsSkipped,
		m.writeTimeouts,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ably_delivery_fanout_size",
			Help: "The largest number of attachments open on a channel when a cm was appended to it, since the previous scrape (DESIGN.md §10). Reading it resets it.",
		}, func() float64 { return float64(m.fanoutMax.Swap(0)) }),
	)
	m.attachAttached = m.attachSeconds.WithLabelValues("attached")
	m.attachSynced = m.attachSeconds.WithLabelValues("synced")
	m.presenceSyncSnapshot = m.presenceSyncStage.WithLabelValues("snapshot")
	m.presenceSyncQueue = m.presenceSyncStage.WithLabelValues("queue")
	m.presenceSyncWrite = m.presenceSyncStage.WithLabelValues("write")
	return m
}

// AttachBuckets are the upper bounds, in seconds, of the attach and
// presence SYNC histograms (ably_attach_seconds,
// ably_presence_sync_stage_seconds): 1 ms to about 33 s, so the tail of
// an attach under load stays inside the buckets.
var AttachBuckets = prometheus.ExponentialBuckets(0.001, 2, 16)

// RegisterBus exports a cluster bus's counters (DESIGN.md §7.2, §10) as
// ably_bus_* series, read from src on every scrape. No-op on a nil
// Metrics.
func (m *Metrics) RegisterBus(src storage.BusStatser) {
	if m == nil {
		return
	}
	m.registry.MustRegister(newBusCollector(src))
}

// FanoutPoolStatser is the fan-out pool's view the
// ably_delivery_fanout_pool_* series read on every scrape (DESIGN.md
// §5.1, §10).
type FanoutPoolStatser interface {
	// QueueDepth is the number of channel stripes waiting for a worker.
	QueueDepth() int64
	// Busy is the number of workers walking a stripe.
	Busy() int64
}

// RegisterFanoutPool exports the fan-out pool's queue depth and busy
// workers as gauges read from src on every scrape. No-op on a nil
// Metrics.
func (m *Metrics) RegisterFanoutPool(src FanoutPoolStatser) {
	if m == nil {
		return
	}
	m.registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ably_delivery_fanout_pool_queue_depth",
			Help: "Channel stripes waiting for a fan-out pool worker: each is a channel above --delivery-fanout-threshold with entries appended that the stripe's worker has not started walking (DESIGN.md §5.1, §10).",
		}, func() float64 { return float64(src.QueueDepth()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ably_delivery_fanout_pool_busy",
			Help: "Fan-out pool workers walking a stripe at the scrape, out of --delivery-fanout-pool (DESIGN.md §5.1, §10).",
		}, func() float64 { return float64(src.Busy()) }),
	)
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

// UnboundPublish records a publish sent down the write-only path: to a
// channel not bound on this node, stored without binding it (DESIGN.md
// §5.1).
func (m *Metrics) UnboundPublish() {
	if m == nil {
		return
	}
	m.unboundPublishes.Inc()
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

// ChannelDiscontinuity records a discontinuity signalled on a channel,
// by reason ("retention" or "log_gap").
func (m *Metrics) ChannelDiscontinuity(reason string) {
	if m == nil {
		return
	}
	m.channelDiscontinuities.WithLabelValues(reason).Inc()
}

// PresenceGraceLeaveError records a grace-window LEAVE that could not be
// written; stage is "get_channel" or "publish".
func (m *Metrics) PresenceGraceLeaveError(stage string) {
	if m == nil {
		return
	}
	m.presenceGraceLeaveErrors.WithLabelValues(stage).Inc()
}

// PresenceReentries records n members re-entered after a lease lapse.
func (m *Metrics) PresenceReentries(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.presenceReentries.Add(float64(n))
}

// DeliveryFanoutSize records the number of attachments open on a channel
// at an append, keeping the largest since the last scrape
// (ably_delivery_fanout_size).
func (m *Metrics) DeliveryFanoutSize(n int64) {
	if m == nil {
		return
	}
	for {
		cur := m.fanoutMax.Load()
		if n <= cur || m.fanoutMax.CompareAndSwap(cur, n) {
			return
		}
	}
}

// DeliveryFanout observes the time from a cm's append to its frame being
// queued for one sampled attachment (ably_delivery_fanout_seconds).
func (m *Metrics) DeliveryFanout(d time.Duration) {
	if m == nil {
		return
	}
	m.deliveryFanout.Observe(d.Seconds())
}

// ConnWriteWait observes the time one sampled frame spent between being
// queued and being written (ably_conn_write_wait_seconds).
func (m *Metrics) ConnWriteWait(d time.Duration) {
	if m == nil {
		return
	}
	m.connWriteWait.Observe(d.Seconds())
}

// ConnectWritten observes the time from a WebSocket upgrade request to
// its CONNECTED frame being written (ably_connect_seconds).
func (m *Metrics) ConnectWritten(d time.Duration) {
	if m == nil {
		return
	}
	m.connectSeconds.Observe(d.Seconds())
}

// ClientAttachDelay observes the time from a connection's CONNECTED
// frame being written to its first ATTACH being read
// (ably_client_attach_delay_seconds).
func (m *Metrics) ClientAttachDelay(d time.Duration) {
	if m == nil {
		return
	}
	m.clientAttachDelay.Observe(d.Seconds())
}

// AttachWritten observes the time from an ATTACH being read to its
// ATTACHED frame being written (ably_attach_seconds{until="attached"}).
func (m *Metrics) AttachWritten(d time.Duration) {
	if m == nil {
		return
	}
	m.attachAttached.Observe(d.Seconds())
}

// AttachSynced observes the time from an ATTACH being read to its final
// presence SYNC frame being written (ably_attach_seconds{until="synced"}).
func (m *Metrics) AttachSynced(d time.Duration) {
	if m == nil {
		return
	}
	m.attachSynced.Observe(d.Seconds())
}

// PresenceSyncSnapshot observes the time an attach took to obtain its
// SYNC snapshot (ably_presence_sync_stage_seconds{stage="snapshot"}).
func (m *Metrics) PresenceSyncSnapshot(d time.Duration) {
	if m == nil {
		return
	}
	m.presenceSyncSnapshot.Observe(d.Seconds())
}

// PresenceSyncQueued observes the time an attach took to encode and
// queue its SYNC frame (ably_presence_sync_stage_seconds{stage="queue"}).
func (m *Metrics) PresenceSyncQueued(d time.Duration) {
	if m == nil {
		return
	}
	m.presenceSyncQueue.Observe(d.Seconds())
}

// PresenceSyncWritten observes the time from an attach starting to queue
// its SYNC frame to the frame being written
// (ably_presence_sync_stage_seconds{stage="write"}).
func (m *Metrics) PresenceSyncWritten(d time.Duration) {
	if m == nil {
		return
	}
	m.presenceSyncWrite.Observe(d.Seconds())
}

// PresenceSyncFrame observes the encoded size of one SYNC frame written
// (ably_presence_sync_frame_bytes).
func (m *Metrics) PresenceSyncFrame(bytes int) {
	if m == nil {
		return
	}
	m.presenceSyncFrameBytes.Observe(float64(bytes))
}

// PresenceSyncSkipped records a SYNC that delivered no presence set;
// reason is "closed" or "error" (ably_presence_syncs_skipped_total).
func (m *Metrics) PresenceSyncSkipped(reason string) {
	if m == nil {
		return
	}
	m.presenceSyncsSkipped.WithLabelValues(reason).Inc()
}

// WriteTimeout records a socket write that missed its deadline; frame is
// the action of the frame being written, lower case
// (ably_write_timeouts_total).
func (m *Metrics) WriteTimeout(frame string) {
	if m == nil {
		return
	}
	m.writeTimeouts.WithLabelValues(frame).Inc()
}
