package loadgen

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a generator process's Prometheus series, served at /metrics.
// Series are ably_loadgen_* with low-cardinality labels only.
type Metrics struct {
	reg *prometheus.Registry

	Connections     prometheus.Gauge
	Attachments     prometheus.Gauge
	Connects        *prometheus.CounterVec // result: ok, fail
	Reconnects      prometheus.Counter
	AttachFailures  prometheus.Counter
	ChannelOpens    prometheus.Counter
	Deliveries      prometheus.Counter
	PresenceEvents  prometheus.Counter
	PublishOffered  *prometheus.CounterVec // transport
	Publishes       *prometheus.CounterVec // transport, result: acked, retried, rejected
	PublishBacklog  prometheus.Gauge
	Violations      *prometheus.CounterVec // kind
	OpenGaps        prometheus.Gauge
	Resumes         *prometheus.CounterVec // result: resumed, discontinuity
	DeliveryLatency *prometheus.HistogramVec
	AckLatency      *prometheus.HistogramVec // transport
	ConnectAttach   prometheus.Histogram
}

var latencyBuckets = []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// NewMetrics builds a registry with the Go and process collectors and the
// generator series.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg:            reg,
		Connections:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "ably_loadgen_connections", Help: "Open realtime connections."}),
		Attachments:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "ably_loadgen_attachments", Help: "Attached channels across connections."}),
		Connects:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ably_loadgen_connects_total", Help: "Connection attempts by result."}, []string{"result"}),
		Reconnects:     prometheus.NewCounter(prometheus.CounterOpts{Name: "ably_loadgen_reconnects_total", Help: "Connections re-established after a drop (churn or failure)."}),
		AttachFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "ably_loadgen_attach_failures_total", Help: "ATTACHes answered with ERROR."}),
		ChannelOpens:   prometheus.NewCounter(prometheus.CounterOpts{Name: "ably_loadgen_channel_opens_total", Help: "Churn attaches to never-used channels."}),
		Deliveries:     prometheus.NewCounter(prometheus.CounterOpts{Name: "ably_loadgen_deliveries_total", Help: "Generator messages received by subscribers."}),
		PresenceEvents: prometheus.NewCounter(prometheus.CounterOpts{Name: "ably_loadgen_presence_events_total", Help: "Presence messages received (PRESENCE and SYNC)."}),
		PublishOffered: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ably_loadgen_publish_offered_total", Help: "Publishes the schedule called for."}, []string{"transport"}),
		Publishes:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ably_loadgen_publishes_total", Help: "Publish attempts by transport and result."}, []string{"transport", "result"}),
		PublishBacklog: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ably_loadgen_publish_backlog", Help: "Scheduled publishes waiting behind an in-flight publish on the same stream."}),
		Violations:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ably_loadgen_violations_total", Help: "Correctness violations on sampled channels by kind."}, []string{"kind"}),
		OpenGaps:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "ably_loadgen_open_gaps", Help: "Currently missing messages on sampled channels (provisional)."}),
		Resumes:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ably_loadgen_resumes_total", Help: "Re-attaches with a channelSerial cursor by result."}, []string{"result"}),
		DeliveryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ably_loadgen_delivery_latency_seconds", Help: "One-way latency from publisher send to subscriber receipt.", Buckets: latencyBuckets,
		}, []string{"path"}),
		AckLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ably_loadgen_publish_ack_seconds", Help: "Publish latency from scheduled send to acknowledgement.", Buckets: latencyBuckets,
		}, []string{"transport"}),
		ConnectAttach: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ably_loadgen_connect_attach_seconds", Help: "Dial to last ATTACHED for a connection's channel set.", Buckets: latencyBuckets,
		}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.Connections, m.Attachments, m.Connects, m.Reconnects, m.AttachFailures, m.ChannelOpens,
		m.Deliveries, m.PresenceEvents, m.PublishOffered, m.Publishes, m.PublishBacklog,
		m.Violations, m.OpenGaps, m.Resumes, m.DeliveryLatency, m.AckLatency, m.ConnectAttach,
	)
	for _, k := range ViolationKinds() {
		m.Violations.WithLabelValues(k.String())
	}
	return m
}

// Handler serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
