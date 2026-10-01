package loadgen

// SummaryVersion is bumped when the summary format changes incompatibly.
const SummaryVersion = 1

// Latency histogram names used in summaries.
const (
	LatDelivery          = "delivery"            // every generator message, measurement window
	LatDeliveryCrossNode = "delivery_cross_node" // publisher and subscriber on different nodes
	LatDeliverySameNode  = "delivery_same_node"
	LatRESTAck           = "rest_ack"     // scheduled send to 201, retries included
	LatRESTService       = "rest_service" // request start to response, one attempt
	LatRealtimeAck       = "realtime_ack" // scheduled send to ACK
	LatConnectAttach     = "connect_attach"
	LatReconnectAttach   = "reconnect_attach"
	LatChannelOpen       = "channel_open" // churn ATTACH to ATTACHED
	LatPresenceAck       = "presence_ack"
)

// Summary is what one generator job writes at the end of a run. The
// conductor merges summaries across processes: counters add, histograms
// merge bucket by bucket, stream records are compared for tail loss.
type Summary struct {
	Version    int      `json:"version"`
	RunID      string   `json:"run_id"`
	RunTag     string   `json:"run_tag"`
	Scenario   string   `json:"scenario"`
	Shape      string   `json:"shape"`
	Multiplier float64  `json:"multiplier"`
	Scale      float64  `json:"scale"`
	Role       string   `json:"role"`
	Index      int      `json:"index"`
	Count      int      `json:"count"`
	Host       string   `json:"host"`
	Endpoints  []string `json:"endpoints"`

	StartUS        int64 `json:"start_us"`
	MeasureStartUS int64 `json:"measure_start_us"`
	MeasureEndUS   int64 `json:"measure_end_us"`
	EndUS          int64 `json:"end_us"`

	Connections ConnStats     `json:"connections"`
	Attachments AttachStats   `json:"attachments"`
	Publishes   PublishStats  `json:"publishes"`
	Deliveries  DeliveryStats `json:"deliveries"`
	Presence    PresenceStats `json:"presence"`

	Latency     map[string]*Histogram              `json:"latency"`
	Correctness CorrectnessSummary                 `json:"correctness"`
	Streams     map[string]map[string]StreamRecord `json:"streams,omitempty"`
	Resources   ResourceStats                      `json:"resources"`
	Errors      []string                           `json:"errors,omitempty"`
}

// ConnStats counts connections.
type ConnStats struct {
	Target int   `json:"target"`
	Opened int64 `json:"opened"`
	Peak   int64 `json:"peak"`
	// OpenAtMeasureStart is taken just after the hold starts (see
	// holdSettle); with OpenAtMeasureEnd it shows the generator held its
	// load steady.
	OpenAtMeasureStart int64 `json:"open_at_measure_start"`
	OpenAtMeasureEnd   int64 `json:"open_at_measure_end"`
	ConnectFailures    int64 `json:"connect_failures"`
	Reconnects         int64 `json:"reconnects"`
	ChurnDrops         int64 `json:"churn_drops"`
	UnplannedDrops     int64 `json:"unplanned_drops"`
}

// AttachStats counts attachments.
type AttachStats struct {
	Target                 int64 `json:"target"`
	AttachedAtMeasureStart int64 `json:"attached_at_measure_start"`
	AttachedAtMeasureEnd   int64 `json:"attached_at_measure_end"`
	Failures               int64 `json:"failures"`
	ChannelOpens           int64 `json:"channel_opens"`
	Discontinuities        int64 `json:"discontinuities"`
}

// PublishStats counts publishes. Offered is what the schedule called
// for; Sent the first attempts started; Acked the publishes confirmed.
// The *InWindow counts cover the measurement window (the hold) only and
// give the offered and achieved rates.
type PublishStats struct {
	Transport       string  `json:"transport,omitempty"`
	Streams         int     `json:"streams"`
	TargetRate      float64 `json:"target_rate"`
	Offered         int64   `json:"offered"`
	Dropped         int64   `json:"dropped"`
	Sent            int64   `json:"sent"`
	Acked           int64   `json:"acked"`
	Retries         int64   `json:"retries"`
	Rejected        int64   `json:"rejected"`
	Unresolved      int64   `json:"unresolved"`
	OfferedInWindow int64   `json:"offered_in_window"`
	AckedInWindow   int64   `json:"acked_in_window"`
	OfferedRate     float64 `json:"offered_rate"`
	AchievedRate    float64 `json:"achieved_rate"`
	// SerialsDropped counts serials not logged for the attach-point check
	// because the process had reached MaxSerialLogEntries.
	SerialsDropped int64 `json:"serials_dropped,omitempty"`
}

// DeliveryStats counts deliveries.
type DeliveryStats struct {
	Received        int64   `json:"received"`
	InWindow        int64   `json:"in_window"`
	Rate            float64 `json:"rate"`
	NegativeLatency int64   `json:"negative_latency"`
	Foreign         int64   `json:"foreign"`
}

// PresenceStats counts presence operations.
type PresenceStats struct {
	Members  int   `json:"members"`
	Entered  int64 `json:"entered"`
	Left     int64 `json:"left"`
	Nacks    int64 `json:"nacks"`
	Received int64 `json:"received"`
	// Member-set check at the end of the hold (see presenceCheck):
	// ChecksPlanned is the sampled presence channels with a member in
	// this job (MembersPlanned: those members), ChecksDone those whose set was fetched and compared,
	// ChecksFailed those that could not be fetched. MembersCompared counts
	// members whose state was stable and so compared; Indeterminate those
	// skipped because their connection or an operation was in flight.
	ChecksPlanned   int64 `json:"checks_planned,omitempty"`
	ChecksDone      int64 `json:"checks_done,omitempty"`
	ChecksFailed    int64 `json:"checks_failed,omitempty"`
	MembersPlanned  int64 `json:"members_planned,omitempty"`
	MembersCompared int64 `json:"members_compared,omitempty"`
	Indeterminate   int64 `json:"indeterminate,omitempty"`
}

// ResourceSample is one sample of the generator's own footprint.
type ResourceSample struct {
	AtUS        int64  `json:"at_us"`
	Goroutines  int    `json:"goroutines"`
	HeapInuse   uint64 `json:"heap_inuse"`
	StackInuse  uint64 `json:"stack_inuse"`
	Sys         uint64 `json:"sys"`
	RSS         uint64 `json:"rss"`
	Connections int64  `json:"connections"`
}

// ResourceStats is the generator's footprint over the run.
// BytesPerConnection is (heap in use + stacks) at the end of the ramp,
// less the same before the first connection, over the connections open;
// for a subscriber job with no publishers it is the memory cost of one
// idle connection with its attachments.
type ResourceStats struct {
	Samples            []ResourceSample `json:"samples,omitempty"`
	BytesPerConnection float64          `json:"bytes_per_connection"`
	RSSPerConnection   float64          `json:"rss_per_connection"`
}
