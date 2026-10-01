package loadgen

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// RunRecordVersion is bumped when RunRecord changes incompatibly.
const RunRecordVersion = 1

// RunRecord is the conductor's result for one run: results/<run-id>/
// summary.json. It carries shape names only, never anything that
// identifies a customer, so it can be committed to the fork.
type RunRecord struct {
	Version     int               `json:"version"`
	RunID       string            `json:"run_id"`
	RunTag      string            `json:"run_tag"`
	Scenario    string            `json:"scenario"`
	Shape       string            `json:"shape"`
	Multiplier  float64           `json:"multiplier"`
	Scale       float64           `json:"scale"`
	Bus         string            `json:"bus,omitempty"`
	Nodes       int               `json:"nodes"`
	Shards      int               `json:"shards,omitempty"`
	Environment map[string]string `json:"environment,omitempty"`

	StartUS        int64 `json:"start_us"`
	MeasureStartUS int64 `json:"measure_start_us"`
	MeasureEndUS   int64 `json:"measure_end_us"`
	EndUS          int64 `json:"end_us"`
	// GrowthBaselineUS is where node memory and goroutine growth are
	// measured from: hold start plus the servers' channel idle timeout
	// (0 in records written before it existed: hold start).
	GrowthBaselineUS int64 `json:"growth_baseline_us,omitempty"`

	Plan      Totals       `json:"plan"`
	Result    RunResult    `json:"result"`
	NodeStats NodeStats    `json:"node_stats"`
	Footprint Footprint    `json:"footprint"`
	Fault     *FaultRecord `json:"fault,omitempty"`
	// ServerBoundPerAttachment is BoundRatio: server channels bound over
	// generator attachments at the end of the hold.
	ServerBoundPerAttachment float64 `json:"server_channels_bound_per_generator_attachment,omitempty"`
	Checks                   []Check `json:"checks"`
	// Clocks is each generator box's clock offset from NTP at the start
	// and end of the run (one-way latency is only as good as these).
	Clocks []ClockRecord `json:"clocks,omitempty"`
	// AgentCPU is each generator and publisher box's busy CPU fraction
	// over the hold.
	AgentCPU []AgentCPU `json:"agent_cpu,omitempty"`
	// UnmeasuredWaived records that the run was started with
	// --allow-unmeasured: node metrics and box CPU coverage are then
	// reported, not gated, and the run is not fit to quote.
	UnmeasuredWaived bool `json:"unmeasured_waived,omitempty"`
	// ServerConfig is the configuration each node reported on /metrics
	// (publish lanes, linger, bus, storage shards): what the run ran with.
	ServerConfig []NodeConfig `json:"server_config,omitempty"`
	// UnsampledNote says what covers the channels outside the sample.
	UnsampledNote string   `json:"unsampled_note,omitempty"`
	Pass          bool     `json:"pass"`
	Verdict       string   `json:"verdict"`
	Errors        []string `json:"errors,omitempty"`
	Jobs          []JobRef `json:"jobs"`
}

// ClockRecord is one agent's clock offsets, measured over the agent's
// GET /v1/clock before the jobs started and after the drain. Error is set
// when the agent could not measure (no --ntp-server, NTP unreachable).
type ClockRecord struct {
	Agent string       `json:"agent"`
	Start *ClockOffset `json:"start,omitempty"`
	End   *ClockOffset `json:"end,omitempty"`
	Error string       `json:"error,omitempty"`
}

// NodeConfig is the server configuration one node reported on /metrics,
// from its last good scrape. Flags is keyed by the server flag the value
// came from; a flag the node did not export is absent.
type NodeConfig struct {
	Node  string            `json:"node"`
	Flags map[string]string `json:"flags"`
}

// Server flag names used as NodeConfig keys.
const (
	FlagPublishLanes     = "publish-lanes"
	FlagLingerMax        = "publish-linger-max"
	FlagLingerMin        = "publish-linger-min"
	FlagStorageShards    = "storage-shards"
	FlagBus              = "bus"
	FlagBusNotifyMode    = "bus-notify-mode"
	FlagBusSweepInterval = "bus-sweep-interval"
)

var serverFlagOrder = []string{FlagPublishLanes, FlagLingerMax, FlagLingerMin, FlagBus, FlagBusNotifyMode, FlagBusSweepInterval, FlagStorageShards}

// ComputeServerConfig reads each node's configuration from its last good
// scrape. Nodes appear in name order; one that never exported any of the
// configuration gauges (memory or disk mode, or no metrics URL) has empty
// Flags.
func ComputeServerConfig(samples []NodeSample) []NodeConfig {
	last := map[string]NodeSample{}
	for _, s := range samples {
		if s.Error != "" || s.Values == nil {
			continue
		}
		if old, ok := last[s.Node]; !ok || s.AtUS >= old.AtUS {
			last[s.Node] = s
		}
	}
	names := make([]string, 0, len(last))
	for n := range last {
		names = append(names, n)
	}
	sort.Strings(names)
	secs := func(v float64) string { return time.Duration(math.Round(v*1e6) * 1e3).String() }
	var out []NodeConfig
	for _, n := range names {
		s := last[n]
		f := map[string]string{}
		if v, ok := s.Values[metricPublishLanes]; ok {
			f[FlagPublishLanes] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		if v, ok := s.Values[metricLingerMax]; ok {
			f[FlagLingerMax] = secs(v)
		}
		if v, ok := s.Values[metricLingerMin]; ok {
			f[FlagLingerMin] = secs(v)
		}
		if v, ok := s.Values[metricStorageShards]; ok {
			f[FlagStorageShards] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		if v, ok := s.Values[metricBusSweepInterval]; ok {
			f[FlagBusSweepInterval] = secs(v)
		}
		if b := s.Info["bus"]; b != "" {
			f[FlagBus] = b
		}
		if m := s.Info["mode"]; m != "" {
			f[FlagBusNotifyMode] = m
		}
		out = append(out, NodeConfig{Node: n, Flags: f})
	}
	return out
}

// flagString renders a node's flags in a fixed order.
func flagString(f map[string]string) string {
	var parts []string
	for _, k := range serverFlagOrder {
		if v, ok := f[k]; ok {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, " ")
}

// ServerConfigVariants groups the reporting nodes by identical flags,
// most nodes first; unreported counts nodes that exported none.
func ServerConfigVariants(cfgs []NodeConfig) (variants []ConfigVariant, unreported int) {
	idx := map[string]int{}
	for _, c := range cfgs {
		if len(c.Flags) == 0 {
			unreported++
			continue
		}
		k := flagString(c.Flags)
		i, ok := idx[k]
		if !ok {
			i = len(variants)
			idx[k] = i
			variants = append(variants, ConfigVariant{Flags: c.Flags, Text: k})
		}
		variants[i].Nodes = append(variants[i].Nodes, c.Node)
	}
	sort.SliceStable(variants, func(a, b int) bool { return len(variants[a].Nodes) > len(variants[b].Nodes) })
	return variants, unreported
}

// ConfigVariant is one distinct server configuration and the nodes that
// reported it.
type ConfigVariant struct {
	Flags map[string]string
	Text  string
	Nodes []string
}

// AgentCPU is one agent box's busy CPU fraction between the start and the
// end of the hold, from its GET /v1/host. Kind is "generator" or
// "publisher" (an agent that takes only REST publish jobs).
type AgentCPU struct {
	Agent        string  `json:"agent"`
	Kind         string  `json:"kind"`
	CPUs         int     `json:"cpus,omitempty"`
	BusyFraction float64 `json:"busy_fraction"`
	Measured     bool    `json:"measured"`
	Error        string  `json:"error,omitempty"`
}

// NodeCoverage is how well one inventory node was sampled: scrapes taken
// and failed, and whether it has a good sample at hold start, at the
// growth baseline and at hold end.
type NodeCoverage struct {
	Node       string `json:"node"`
	HasURL     bool   `json:"has_metrics_url"`
	Samples    int    `json:"samples"`
	Errors     int    `json:"errors"`
	AtStart    bool   `json:"at_hold_start"`
	AtBaseline bool   `json:"at_baseline"`
	AtEnd      bool   `json:"at_hold_end"`
}

// Sampled reports whether the node has a good sample at hold start and
// end, and at the baseline when one was due.
func (c NodeCoverage) Sampled(baselineDue bool) bool {
	return c.AtStart && c.AtEnd && (c.AtBaseline || !baselineDue)
}

// JobRef names one generator job of the run.
type JobRef struct {
	ID    string `json:"id"`
	Role  string `json:"role"`
	Agent string `json:"agent"`
	Host  string `json:"host,omitempty"`
}

// RunResult is the merge of every job's summary.
type RunResult struct {
	Connections      ConnStats             `json:"connections"`
	Attachments      AttachStats           `json:"attachments"`
	Publishes        PublishStats          `json:"publishes"`
	Deliveries       DeliveryStats         `json:"deliveries"`
	Presence         PresenceStats         `json:"presence"`
	Violations       map[string]int64      `json:"violations"`
	CheckedMessages  int64                 `json:"checked_messages"`
	Tail             TailResult            `json:"tail"`
	Attach           AttachResult          `json:"attach"`
	FirstViolations  []Violation           `json:"first_violations,omitempty"`
	Latency          map[string]*Histogram `json:"latency"`
	GeneratorBytesPC float64               `json:"generator_bytes_per_connection,omitempty"`
}

// NodeStats summarises the server nodes' metrics over the hold.
type NodeStats struct {
	Samples []NodeSample `json:"samples,omitempty"`
	// MemoryGrowth is the largest fractional RSS growth of any node from
	// its first sample at or after the growth baseline (hold start plus
	// the servers' channel idle timeout) to its last sample of the hold.
	MemoryGrowth float64 `json:"memory_growth"`
	// GoroutineGrowth is the same for goroutines.
	GoroutineGrowth float64 `json:"goroutine_growth"`
	// CoresUsed is the nodes' summed CPU use over the hold.
	CoresUsed float64 `json:"cores_used"`
	// RSSBytes is the nodes' summed RSS at the end of the hold.
	RSSBytes float64 `json:"rss_bytes"`
	// ConnectionsOpen is the nodes' summed open connections at the end of
	// the hold, as the server counts them.
	ConnectionsOpen float64 `json:"connections_open"`
	// Server-side counts summed over nodes at the first and last sample
	// of the hold, to set against the generator's own counts.
	ConnectionsAtStart   float64 `json:"connections_at_start"`
	ChannelsBoundAtStart float64 `json:"channels_bound_at_start"`
	ChannelsBoundAtEnd   float64 `json:"channels_bound_at_end"`
	Measured             bool    `json:"measured"`
	// Growth is measured from the baseline, not the hold start.
	// GrowthMeasured is false when no node has two samples between the
	// baseline and the end of the hold (a hold not longer than the idle
	// timeout): growth is then not judged. The baseline values are summed
	// over the nodes, so a plateau of bound channels is visible beside
	// the start and end.
	GrowthMeasured          bool    `json:"growth_measured"`
	GrowthBaselineUS        int64   `json:"growth_baseline_us,omitempty"`
	ChannelsBoundAtBaseline float64 `json:"channels_bound_at_baseline"`
	RSSBytesAtBaseline      float64 `json:"rss_bytes_at_baseline"`
	GoroutinesAtBaseline    float64 `json:"goroutines_at_baseline"`
	GoroutinesAtEnd         float64 `json:"goroutines_at_end"`
	// Coverage is per inventory node (ComputeNodeCoverage);
	// BaselineDue says the growth baseline scrape was due before the hold
	// ended.
	Coverage    []NodeCoverage `json:"coverage,omitempty"`
	BaselineDue bool           `json:"baseline_due,omitempty"`
}

// Footprint is plan §8's footprint: provisioned and used resources per
// 100k connections, per 100k deliveries/s and per 10k writes/s.
type Footprint struct {
	VCPU                  float64 `json:"vcpu"`
	MemoryGB              float64 `json:"memory_gb"`
	CoresUsed             float64 `json:"cores_used"`
	RSSGB                 float64 `json:"rss_gb"`
	Connections           float64 `json:"connections"`
	DeliveriesPerSec      float64 `json:"deliveries_per_sec"`
	WritesPerSec          float64 `json:"writes_per_sec"`
	VCPUPer100kConns      float64 `json:"vcpu_per_100k_connections"`
	MemGBPer100kConns     float64 `json:"memory_gb_per_100k_connections"`
	VCPUPer100kDeliveries float64 `json:"vcpu_per_100k_deliveries_per_sec"`
	VCPUPer10kWrites      float64 `json:"vcpu_per_10k_writes_per_sec"`
	UsedPer100kConns      float64 `json:"used_cores_per_100k_connections"`
	UsedPer100kDeliveries float64 `json:"used_cores_per_100k_deliveries_per_sec"`
	UsedPer10kWrites      float64 `json:"used_cores_per_10k_writes_per_sec"`
	RSSGBPer100kConns     float64 `json:"rss_gb_per_100k_connections"`
}

// FaultRecord is the failure injection step, if any.
type FaultRecord struct {
	Command  string `json:"command"`
	AtUS     int64  `json:"at_us"`
	ExitCode int    `json:"exit_code"`
	Output   string `json:"output,omitempty"`
}

// Check is one pass criterion.
type Check struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Limit  string `json:"limit"`
	Pass   bool   `json:"pass"`
	Gating bool   `json:"gating"`
	Note   string `json:"note,omitempty"`
}

// MergeSummaries combines job summaries: counters add, histograms merge
// bucket by bucket, and publisher stream records are checked against
// subscriber records for tail loss.
func MergeSummaries(sums []*Summary, tailMargin time.Duration) RunResult {
	r := RunResult{Violations: make(map[string]int64), Latency: make(map[string]*Histogram)}
	published := map[string]map[string]StreamRecord{}
	var seen []map[string]*ChannelSeen
	var claims []AttachClaim
	var claimsDropped int64
	var pubTarget float64
	var bpcSum float64
	var bpcN int
	for _, s := range sums {
		if s == nil {
			continue
		}
		c := &r.Connections
		c.Target += s.Connections.Target
		c.Opened += s.Connections.Opened
		c.Peak += s.Connections.Peak
		c.OpenAtMeasureStart += s.Connections.OpenAtMeasureStart
		c.OpenAtMeasureEnd += s.Connections.OpenAtMeasureEnd
		c.ConnectFailures += s.Connections.ConnectFailures
		c.Reconnects += s.Connections.Reconnects
		c.ChurnDrops += s.Connections.ChurnDrops
		c.UnplannedDrops += s.Connections.UnplannedDrops
		a := &r.Attachments
		a.Target += s.Attachments.Target
		a.AttachedAtMeasureStart += s.Attachments.AttachedAtMeasureStart
		a.AttachedAtMeasureEnd += s.Attachments.AttachedAtMeasureEnd
		a.Failures += s.Attachments.Failures
		a.ChannelOpens += s.Attachments.ChannelOpens
		a.Discontinuities += s.Attachments.Discontinuities
		p := &r.Publishes
		p.Streams += s.Publishes.Streams
		pubTarget += s.Publishes.TargetRate
		p.Offered += s.Publishes.Offered
		p.Dropped += s.Publishes.Dropped
		p.Sent += s.Publishes.Sent
		p.Acked += s.Publishes.Acked
		p.Retries += s.Publishes.Retries
		p.Rejected += s.Publishes.Rejected
		p.Unresolved += s.Publishes.Unresolved
		p.Throttled += s.Publishes.Throttled
		p.SerialsDropped += s.Publishes.SerialsDropped
		p.OfferedInWindow += s.Publishes.OfferedInWindow
		p.AckedInWindow += s.Publishes.AckedInWindow
		p.OfferedRate += s.Publishes.OfferedRate
		p.AchievedRate += s.Publishes.AchievedRate
		d := &r.Deliveries
		d.Received += s.Deliveries.Received
		d.InWindow += s.Deliveries.InWindow
		d.Rate += s.Deliveries.Rate
		d.NegativeLatency += s.Deliveries.NegativeLatency
		d.Foreign += s.Deliveries.Foreign
		pr := &r.Presence
		pr.Members += s.Presence.Members
		pr.Entered += s.Presence.Entered
		pr.Left += s.Presence.Left
		pr.Nacks += s.Presence.Nacks
		pr.Received += s.Presence.Received
		pr.ChecksPlanned += s.Presence.ChecksPlanned
		pr.ChecksDone += s.Presence.ChecksDone
		pr.ChecksFailed += s.Presence.ChecksFailed
		pr.MembersPlanned += s.Presence.MembersPlanned
		pr.MembersCompared += s.Presence.MembersCompared
		pr.Indeterminate += s.Presence.Indeterminate
		for k, v := range s.Correctness.Violations {
			r.Violations[k] += v
		}
		r.CheckedMessages += s.Correctness.CheckedMessages
		claims = append(claims, s.Correctness.AttachClaims...)
		claimsDropped += s.Correctness.AttachClaimsDropped
		for _, v := range s.Correctness.FirstViolations {
			if len(r.FirstViolations) < MaxLoggedViolations {
				r.FirstViolations = append(r.FirstViolations, v)
			}
		}
		for name, h := range s.Latency {
			if h == nil {
				continue
			}
			m := r.Latency[name]
			if m == nil {
				m = NewHistogram()
				r.Latency[name] = m
			}
			m.Merge(h)
		}
		for ch, streams := range s.Streams {
			if published[ch] == nil {
				published[ch] = map[string]StreamRecord{}
			}
			for pub, rec := range streams {
				if old, ok := published[ch][pub]; !ok || rec.LastAckedSeq > old.LastAckedSeq {
					published[ch][pub] = rec
				}
			}
		}
		if s.Role == RoleSubscriber {
			seen = append(seen, s.Correctness.Channels)
			if s.Resources.BytesPerConnection > 0 {
				bpcSum += s.Resources.BytesPerConnection
				bpcN++
			}
		}
	}
	r.Publishes.TargetRate = pubTarget
	r.Tail = TailCheck(published, seen, tailMargin)
	r.Violations[TailLoss.String()] += r.Tail.Lost
	for _, v := range r.Tail.Examples {
		if len(r.FirstViolations) < MaxLoggedViolations {
			r.FirstViolations = append(r.FirstViolations, v)
		}
	}
	r.Attach = AttachCheck(published, claims)
	r.Attach.Dropped = claimsDropped
	r.Violations[AttachGap.String()] += r.Attach.Missed
	for _, v := range r.Attach.Examples {
		if len(r.FirstViolations) < MaxLoggedViolations {
			r.FirstViolations = append(r.FirstViolations, v)
		}
	}
	if bpcN > 0 {
		r.GeneratorBytesPC = bpcSum / float64(bpcN)
	}
	return r
}

// ComputeNodeStats derives growth and use over the hold from node
// samples. Use (CPU, connections, bound channels at start and end) spans
// the whole hold; memory and goroutine growth span baselineUS to the
// end of the hold (baselineUS at or before measureStartUS means the
// hold start).
func ComputeNodeStats(samples []NodeSample, measureStartUS, measureEndUS, baselineUS int64) NodeStats {
	ns := NodeStats{Samples: samples}
	if baselineUS < measureStartUS {
		baselineUS = measureStartUS
	}
	ns.GrowthBaselineUS = baselineUS
	byNode := map[string][]NodeSample{}
	for _, s := range samples {
		if s.Error != "" || s.Values == nil {
			continue
		}
		if s.AtUS < measureStartUS || s.AtUS > measureEndUS {
			continue
		}
		byNode[s.Node] = append(byNode[s.Node], s)
	}
	for _, ss := range byNode {
		if len(ss) < 2 {
			continue
		}
		sort.Slice(ss, func(i, j int) bool { return ss[i].AtUS < ss[j].AtUS })
		first, last := ss[0], ss[len(ss)-1]
		ns.Measured = true
		// The growth baseline is this node's first sample at or after
		// baselineUS; growth needs a later sample too.
		if i := sort.Search(len(ss), func(i int) bool { return ss[i].AtUS >= baselineUS }); i < len(ss)-1 {
			base := ss[i]
			ns.GrowthMeasured = true
			growth := func(name string) float64 {
				a, b := base.Values[name], last.Values[name]
				if a <= 0 {
					return 0
				}
				return (b - a) / a
			}
			ns.MemoryGrowth = max(ns.MemoryGrowth, growth("process_resident_memory_bytes"))
			ns.GoroutineGrowth = max(ns.GoroutineGrowth, growth("go_goroutines"))
			ns.ChannelsBoundAtBaseline += base.Values["ably_channels_bound"]
			ns.RSSBytesAtBaseline += base.Values["process_resident_memory_bytes"]
			ns.GoroutinesAtBaseline += base.Values["go_goroutines"]
		}
		ns.GoroutinesAtEnd += last.Values["go_goroutines"]
		if dt := float64(last.AtUS-first.AtUS) / 1e6; dt > 0 {
			ns.CoresUsed += (last.Values["process_cpu_seconds_total"] - first.Values["process_cpu_seconds_total"]) / dt
		}
		ns.RSSBytes += last.Values["process_resident_memory_bytes"]
		ns.ConnectionsOpen += last.Values["ably_connections_open"]
		ns.ConnectionsAtStart += first.Values["ably_connections_open"]
		ns.ChannelsBoundAtStart += first.Values["ably_channels_bound"]
		ns.ChannelsBoundAtEnd += last.Values["ably_channels_bound"]
	}
	return ns
}

// Phases of the three scrapes the conductor takes at fixed points of the
// hold, in NodeSample.Phase.
const (
	PhaseHoldStart = "hold-start"
	PhaseBaseline  = "baseline"
	PhaseHoldEnd   = "hold-end"
)

// ComputeNodeCoverage counts, for every node of the inventory, the
// scrapes taken and failed and whether it was sampled at the three fixed
// points. A node with no metrics URL is listed with HasURL false.
func ComputeNodeCoverage(inv *Inventory, samples []NodeSample, measureEndUS, baselineUS int64) (cov []NodeCoverage, baselineDue bool) {
	baselineDue = baselineUS+int64(time.Second/time.Microsecond) <= measureEndUS
	if inv == nil {
		return nil, baselineDue
	}
	byNode := map[string]*NodeCoverage{}
	for _, n := range inv.Nodes {
		byNode[n.Name] = &NodeCoverage{Node: n.Name, HasURL: n.Metrics != ""}
	}
	for _, s := range samples {
		c := byNode[s.Node]
		if c == nil {
			continue
		}
		c.Samples++
		if s.Error != "" || s.Values == nil {
			c.Errors++
			continue
		}
		switch s.Phase {
		case PhaseHoldStart:
			c.AtStart = true
		case PhaseBaseline:
			c.AtBaseline = true
		case PhaseHoldEnd:
			c.AtEnd = true
		}
	}
	for _, n := range inv.Nodes {
		cov = append(cov, *byNode[n.Name])
	}
	return cov, baselineDue
}

// ComputeFootprint derives plan §8's footprint figures.
func ComputeFootprint(inv *Inventory, res RunResult, ns NodeStats) Footprint {
	f := Footprint{
		CoresUsed:        ns.CoresUsed,
		RSSGB:            ns.RSSBytes / (1 << 30),
		Connections:      float64(res.Connections.OpenAtMeasureEnd),
		DeliveriesPerSec: res.Deliveries.Rate,
		WritesPerSec:     res.Publishes.AchievedRate,
	}
	if inv != nil {
		for _, n := range inv.Nodes {
			f.VCPU += n.VCPU
			f.MemoryGB += n.MemoryGB
		}
	}
	per := func(total, load, unit float64) float64 {
		if load <= 0 || total <= 0 {
			return 0
		}
		return total / (load / unit)
	}
	f.VCPUPer100kConns = per(f.VCPU, f.Connections, 1e5)
	f.MemGBPer100kConns = per(f.MemoryGB, f.Connections, 1e5)
	f.VCPUPer100kDeliveries = per(f.VCPU, f.DeliveriesPerSec, 1e5)
	f.VCPUPer10kWrites = per(f.VCPU, f.WritesPerSec, 1e4)
	f.UsedPer100kConns = per(f.CoresUsed, f.Connections, 1e5)
	f.UsedPer100kDeliveries = per(f.CoresUsed, f.DeliveriesPerSec, 1e5)
	f.UsedPer10kWrites = per(f.CoresUsed, f.WritesPerSec, 1e4)
	f.RSSGBPer100kConns = per(f.RSSGB, f.Connections, 1e5)
	return f
}

func relChange(from, to int64) float64 {
	if from <= 0 {
		return 0
	}
	return float64(to-from) / float64(from)
}

func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// BoundRatio is the server's bound channels over the generator's
// attachments at the end of the hold (0 when either is unknown). Near
// the number of nodes a channel's subscribers span (1 when each channel
// lives on one node) means the server holds what the generator
// attached; much higher means channels are retained past their use.
func (rec *RunRecord) BoundRatio() float64 {
	a := rec.Result.Attachments.AttachedAtMeasureEnd
	if a <= 0 || rec.NodeStats.ChannelsBoundAtEnd <= 0 {
		return 0
	}
	return rec.NodeStats.ChannelsBoundAtEnd / float64(a)
}

// faultRelaxed reports whether the run's fault injection ran and
// succeeded: only then do the steady-state gates (growth, load drift,
// coverage) stand down. A fault hook that failed relaxes nothing, and
// Evaluate fails the run for it.
func faultRelaxed(rec *RunRecord) bool {
	return rec.Fault != nil && rec.Fault.ExitCode == 0
}

// pctOf renders a fraction as a percentage without trailing zeros.
func pctOf(f float64) string {
	return strconv.FormatFloat(math.Round(f*10000)/100, 'f', -1, 64) + "%"
}

func msOf(us uint64) string { return fmt.Sprintf("%.1f ms", float64(us)/1000) }

// Evaluate applies the pass criteria (plan §8) to a run record, filling
// Checks, Pass and Verdict. A criterion whose input was not measured
// (no REST publishes, no node metrics) is reported but does not gate.
func Evaluate(rec *RunRecord, spec PassSpec) {
	spec = spec.withDefaults()
	res := &rec.Result
	var checks []Check
	add := func(c Check) { checks = append(checks, c) }

	// Delivery latency: publisher to a subscriber on another node, when
	// there is such traffic; otherwise every delivery.
	del := res.Latency[LatDeliveryCrossNode]
	delName := "cross-node"
	if del == nil || del.Count() == 0 {
		del = res.Latency[LatDelivery]
		delName = "all"
	}
	if del != nil && del.Count() > 0 {
		p50, p99 := del.Quantile(0.50), del.Quantile(0.99)
		add(Check{Name: "delivery p50 (" + delName + ")", Value: msOf(p50), Limit: "<= " + spec.DeliveryP50.String(),
			Pass: time.Duration(p50)*time.Microsecond <= spec.DeliveryP50.Duration, Gating: true})
		add(Check{Name: "delivery p99 (" + delName + ")", Value: msOf(p99), Limit: "<= " + spec.DeliveryP99.String(),
			Pass: time.Duration(p99)*time.Microsecond <= spec.DeliveryP99.Duration, Gating: true})
		add(Check{Name: "delivery p99 stretch", Value: msOf(p99), Limit: "< " + spec.DeliveryP99Str.String(),
			Pass: time.Duration(p99)*time.Microsecond < spec.DeliveryP99Str.Duration, Gating: false})
	} else if rec.Plan.DeliveriesPerSec > 0 {
		add(Check{Name: "delivery latency", Value: "no deliveries in window", Limit: "deliveries expected", Pass: false, Gating: true})
	}
	if h := res.Latency[LatDeliveryFromSend]; h != nil && h.Count() > 0 {
		add(Check{Name: "delivery latency from send (reported)", Value: fmt.Sprintf("p50 %s, p99 %s", msOf(h.Quantile(0.50)), msOf(h.Quantile(0.99))),
			Limit: "reported", Pass: true, Gating: false,
			Note: "from the moment the generator handed the publish to its sender; the gated figures above are from the scheduled time, so they include any wait the generator imposed"})
	}
	if d := res.Deliveries; d.InWindow > 0 {
		frac := float64(d.NegativeLatency) / float64(d.InWindow)
		add(Check{Name: "negative latency (clock skew)", Value: fmt.Sprintf("%d of %d in-window deliveries (%.3f%%)", d.NegativeLatency, d.InWindow, frac*100),
			Limit: "< " + pctOf(spec.MaxNegativeLatency), Pass: frac < spec.MaxNegativeLatency, Gating: true,
			Note: "latency from the actual send time below zero: the subscriber's clock is behind the publisher's"})
	}
	if len(rec.ServerConfig) > 0 {
		variants, unreported := ServerConfigVariants(rec.ServerConfig)
		switch {
		case len(variants) == 0:
			add(Check{Name: "server configuration", Value: "not reported by any node", Limit: "reported", Pass: true, Gating: false,
				Note: "the nodes export their write-path flags only in cluster mode, and the inventory needs their metrics URLs"})
		default:
			add(Check{Name: "server configuration identical on every node", Value: fmt.Sprintf("%d configuration(s) on %d reporting nodes", len(variants), len(rec.ServerConfig)-unreported),
				Limit: "1", Pass: len(variants) == 1, Gating: true, Note: fmt.Sprintf("%d nodes reported none", unreported)})
			v := variants[0].Flags
			if rec.Shards > 0 && v[FlagStorageShards] != "" && v[FlagStorageShards] != strconv.Itoa(rec.Shards) {
				add(Check{Name: "recorded shard count matches the nodes", Value: fmt.Sprintf("record %d, nodes report %s", rec.Shards, v[FlagStorageShards]),
					Limit: "equal", Pass: false, Gating: true, Note: "the run would be quoted under the wrong shard count"})
			}
			if rec.Bus != "" && v[FlagBus] != "" && !strings.EqualFold(rec.Bus, v[FlagBus]) {
				add(Check{Name: "recorded bus matches the nodes", Value: fmt.Sprintf("record %q, nodes report %q", rec.Bus, v[FlagBus]),
					Limit: "equal", Pass: false, Gating: false, Note: "reported only: the inventory may use another name for the same bus"})
			}
		}
	}
	if len(rec.Clocks) > 0 {
		var measured, failed int
		var worst int64
		var worstAgent string
		for _, c := range rec.Clocks {
			got := false
			for _, o := range []*ClockOffset{c.Start, c.End} {
				if o == nil {
					continue
				}
				got = true
				if a := absInt64(o.OffsetUS); a >= worst {
					worst, worstAgent = a, c.Agent
				}
			}
			if got {
				measured++
			} else {
				failed++
			}
		}
		if measured == 0 {
			add(Check{Name: "generator clock offset", Value: "not measured", Limit: "reported", Pass: true, Gating: false,
				Note: fmt.Sprintf("%d agents could not measure (no --ntp-server, or NTP unreachable); only the negative-latency check guards against skew", failed)})
		} else {
			add(Check{Name: "generator clock offset", Value: fmt.Sprintf("worst %.2f ms (%s) over %d of %d boxes, start and end of run", float64(worst)/1000, worstAgent, measured, measured+failed),
				Limit: "<= " + spec.MaxClockOffset.String(), Pass: time.Duration(worst)*time.Microsecond <= spec.MaxClockOffset.Duration, Gating: true,
				Note: fmt.Sprintf("%d boxes unmeasured", failed)})
		}
	}
	if h := res.Latency[LatRESTAck]; h != nil && h.Count() > 0 {
		p99 := h.Quantile(0.99)
		add(Check{Name: "REST publish ACK p99", Value: msOf(p99), Limit: "<= " + spec.RestAckP99.String(),
			Pass: time.Duration(p99)*time.Microsecond <= spec.RestAckP99.Duration, Gating: true,
			Note: "from the scheduled send time, retries included"})
	}
	if h := res.Latency[LatRealtimeAck]; h != nil && h.Count() > 0 {
		p99 := h.Quantile(0.99)
		add(Check{Name: "realtime publish ACK p99", Value: msOf(p99), Limit: "<= " + spec.RestAckP99.String(),
			Pass: time.Duration(p99)*time.Microsecond <= spec.RestAckP99.Duration, Gating: false})
	}
	if h := res.Latency[LatConnectAttach]; h != nil && h.Count() > 0 {
		p99 := h.Quantile(0.99)
		add(Check{Name: "connect+attach p99", Value: msOf(p99), Limit: "<= " + spec.ConnectAttachP99.String(),
			Pass: time.Duration(p99)*time.Microsecond <= spec.ConnectAttachP99.Duration, Gating: true,
			Note: fmt.Sprintf("%d samples, churn included", h.Count())})
	}
	var bad int64
	var parts []string
	for _, k := range ViolationKinds() {
		n := res.Violations[k.String()]
		bad += n
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, n))
		}
	}
	value := fmt.Sprintf("0 of %d checked", res.CheckedMessages)
	if bad > 0 {
		value = strings.Join(parts, " ")
	}
	add(Check{Name: "loss, duplicate, reorder on the sample", Value: value, Limit: "0", Pass: bad == 0, Gating: true,
		Note: fmt.Sprintf("tail check covered %d streams, attach-point check %d claims", res.Tail.Checked, res.Attach.Checked)})
	if res.CheckedMessages == 0 && rec.Plan.SampledChannels > 0 && rec.Plan.PublishesPerSec > 0 {
		add(Check{Name: "sample coverage", Value: "0 messages checked", Limit: "> 0", Pass: false, Gating: true})
	}
	if rec.Plan.PublishesPerSec > 0 && rec.Plan.SampledSubscribed == 0 {
		// Publishing with no sampled channel that has a subscriber means no
		// message was checked at all.
		add(Check{Name: "sample coverage", Value: "no sampled channel has a subscriber", Limit: "> 0", Pass: false, Gating: true,
			Note: "raise sample_percent"})
	}
	if rec.Plan.PresenceMembers > 0 {
		pr := res.Presence
		minCompared := spec.MinPresenceCompared
		cov := 0.0
		if pr.MembersPlanned > 0 {
			cov = float64(pr.MembersCompared) / float64(pr.MembersPlanned)
		}
		ok := pr.ChecksPlanned > 0 && pr.ChecksDone >= int64(rec.Plan.PresenceSampled) && pr.ChecksFailed == 0 && pr.MembersCompared > 0
		add(Check{Name: "sample coverage (presence)",
			Value: fmt.Sprintf("%d of %d sampled presence channels compared (%d planned, %d fetch failures), %d of %d members settled (%.0f%%), %d indeterminate",
				pr.ChecksDone, rec.Plan.PresenceSampled, pr.ChecksPlanned, pr.ChecksFailed, pr.MembersCompared, pr.MembersPlanned, cov*100, pr.Indeterminate),
			Limit: fmt.Sprintf("every sampled channel, >= %.0f%% of members settled", minCompared*100),
			Pass:  ok && (cov >= minCompared || faultRelaxed(rec)), Gating: true})
		mism := res.Violations[PresenceSetMismatch.String()]
		add(Check{Name: "presence correctness",
			Value: fmt.Sprintf("%d nacks, %d member-set mismatches over %d members compared", pr.Nacks, mism, pr.MembersCompared),
			Limit: "0 nacks, 0 mismatches", Pass: mism == 0 && (pr.Nacks == 0 || faultRelaxed(rec)), Gating: true,
			Note: "end-of-hold REST presence set against the members' own enter and leave record"})
	}
	if rec.Plan.SampledStreams > 0 {
		// The tail check is the only one that sees a stream an attachment
		// received nothing from. It skips a stream on a subscriber whose last
		// acknowledgement is within the margin of that subscriber's latest
		// attach (a late churn re-attach, a slow stream): legitimately, so the
		// floor is 50%, not more (bench/aws/README.md says why). The row
		// prints the fraction and what was skipped for which reason.
		t := res.Tail
		cov := float64(t.StreamsChecked) / float64(rec.Plan.SampledStreams)
		other := max(int64(rec.Plan.SampledStreams)-t.StreamsChecked-t.StreamsSkippedMargin, 0)
		add(Check{Name: "tail check coverage",
			Value: fmt.Sprintf("%d of %d sampled streams checked (%.1f%%); skipped %d for the margin (last acknowledgement within %s of a late attach), %d with no continuous subscriber or no acknowledgement",
				t.StreamsChecked, rec.Plan.SampledStreams, cov*100, t.StreamsSkippedMargin, t.Margin, other),
			Limit: fmt.Sprintf(">= %.0f%% and > 0", spec.MinTailCoverage*100), Pass: t.StreamsChecked > 0 && cov >= spec.MinTailCoverage, Gating: !faultRelaxed(rec),
			Note: fmt.Sprintf("%d (subscriber, stream) pairs skipped for the margin", t.SkippedMargin)})
	}
	if rec.Plan.SampledChannels > 0 && rec.Plan.PublishesPerSec > 0 {
		// The attach-point check settles each attachment's first message
		// against the publishers' serial logs. If it settled few claims it
		// proved little, so coverage is gated like the others; a run that
		// settled no claim at all fails whatever fault ran.
		a := res.Attach
		total := a.Claims + a.Dropped
		cov := 0.0
		if total > 0 {
			cov = float64(a.Checked) / float64(total)
		}
		add(Check{Name: "sample coverage (attach)",
			Value: fmt.Sprintf("%d of %d claims settled (%.0f%%), %d unverifiable, %d dropped; %d messages missed after an attach point",
				a.Checked, total, cov*100, a.Unverifiable, a.Dropped, a.Missed),
			Limit: fmt.Sprintf(">= %.0f%% and > 0", spec.MinAttachCoverage*100), Pass: a.Checked > 0 && cov >= spec.MinAttachCoverage,
			Gating: a.Checked == 0 || !faultRelaxed(rec),
			Note:   "claims: each attachment's first message per stream, settled against the publishers' serial logs"})
	}
	if p := res.Publishes; p.TargetRate > 0 {
		ratio := 0.0
		if p.OfferedRate > 0 {
			ratio = p.AchievedRate / p.OfferedRate
		}
		add(Check{Name: "publish rate achieved", Value: fmt.Sprintf("%.0f of %.0f/s offered (%.0f%%)", p.AchievedRate, p.OfferedRate, ratio*100),
			Limit: fmt.Sprintf(">= %.0f%%", spec.MinAchievedRatio*100), Pass: ratio >= spec.MinAchievedRatio, Gating: true})
		add(Check{Name: "offered rate vs target", Value: fmt.Sprintf("%.0f of %.0f/s", p.OfferedRate, p.TargetRate),
			Limit: fmt.Sprintf(">= %.0f%% (generator kept up)", spec.MinAchievedRatio*100), Pass: p.OfferedRate >= spec.MinAchievedRatio*p.TargetRate, Gating: true,
			Note: fmt.Sprintf("generator dropped %d scheduled publishes", p.Dropped)})
		add(Check{Name: "rejected publishes", Value: fmt.Sprint(p.Rejected), Limit: "0", Pass: p.Rejected == 0, Gating: true})
		// A publish that is retried or never resolved is a stream held back
		// by the server; the achieved-rate gate alone would let a few
		// percent of them through unremarked.
		if p.Sent > 0 {
			ratio := float64(p.Retries) / float64(p.Sent)
			add(Check{Name: "publish retries", Value: fmt.Sprintf("%d retries of %d sent (%.3f%%), %d answered 429", p.Retries, p.Sent, ratio*100, p.Throttled),
				Limit: "< " + pctOf(spec.MaxRetryRatio), Pass: ratio < spec.MaxRetryRatio, Gating: !faultRelaxed(rec)})
		}
		add(Check{Name: "unresolved publishes", Value: fmt.Sprint(p.Unresolved), Limit: "0", Pass: p.Unresolved == 0, Gating: !faultRelaxed(rec),
			Note: "still in flight when the generator stopped waiting"})
	}
	// Channels outside the sample have no per-message check: this is the
	// only thing that would notice loss on them.
	minDel := spec.MinDeliveryRatio
	if faultRelaxed(rec) {
		minDel = min(minDel, spec.MinDeliveryRatioFault)
	}
	rec.UnsampledNote = fmt.Sprintf("Unsampled channels are covered only by the deliveries-vs-plan gate (>= %s of planned deliveries/s); loss below %s there is not detected.",
		pctOf(minDel), pctOf(1-minDel))
	if want := rec.Plan.DeliveriesPerSec; want > 0 {
		ratio := res.Deliveries.Rate / want
		add(Check{Name: "deliveries vs plan", Value: fmt.Sprintf("%.0f of %.0f/s (%.2f%%)", res.Deliveries.Rate, want, ratio*100),
			Limit: ">= " + pctOf(minDel), Pass: ratio >= minDel, Gating: true,
			Note: fmt.Sprintf("%d of %d channels sampled; the rest are covered only by this gate", rec.Plan.SampledSubscribed, rec.Plan.SubscribedChannels)})
	}
	if c := res.Connections; c.Target > 0 {
		want := float64(c.Target) * (1 - spec.MaxConnectionLoss)
		add(Check{Name: "connections open at end of hold", Value: fmt.Sprintf("%d of %d", c.OpenAtMeasureEnd, c.Target),
			Limit: fmt.Sprintf(">= %.0f", want), Pass: float64(c.OpenAtMeasureEnd) >= want, Gating: true})
	}
	// The generator must hold its load steady through the hold, or node
	// memory growth measures the generator rather than the server.
	if c, a := res.Connections, res.Attachments; c.OpenAtMeasureStart > 0 {
		dc := relChange(c.OpenAtMeasureStart, c.OpenAtMeasureEnd)
		da := relChange(a.AttachedAtMeasureStart, a.AttachedAtMeasureEnd)
		gate := !faultRelaxed(rec)
		add(Check{Name: "generator load steady over hold",
			Value: fmt.Sprintf("connections %d to %d (%+.1f%%), attachments %d to %d (%+.1f%%)", c.OpenAtMeasureStart, c.OpenAtMeasureEnd, dc*100,
				a.AttachedAtMeasureStart, a.AttachedAtMeasureEnd, da*100),
			Limit: fmt.Sprintf("within ±%.0f%%", spec.MaxLoadDrift*100), Pass: abs(dc) <= spec.MaxLoadDrift && abs(da) <= spec.MaxLoadDrift, Gating: gate})
	}
	ns := rec.NodeStats
	if f := rec.Fault; f != nil {
		// A fault that did not run, or whose hook failed, relaxes nothing
		// (faultRelaxed) and fails the run: the scenario said it would
		// inject one.
		add(Check{Name: "fault injection", Value: fmt.Sprintf("hook exited %d", f.ExitCode), Limit: "exit 0", Pass: f.ExitCode == 0, Gating: true,
			Note: strings.TrimSpace(f.Output)})
	}
	if len(ns.Coverage) > 0 {
		sampled, errs, scrapes := 0, 0, 0
		for _, c := range ns.Coverage {
			if c.Sampled(ns.BaselineDue) {
				sampled++
			}
			errs += c.Errors
			scrapes += c.Samples
		}
		note := fmt.Sprintf("%d scrape errors in %d scrapes", errs, scrapes)
		if !ns.BaselineDue {
			note += "; no baseline scrape was due in a hold this short"
		}
		gate := !faultRelaxed(rec) && !rec.UnmeasuredWaived
		if rec.UnmeasuredWaived {
			note += "; waived with --allow-unmeasured"
		}
		add(Check{Name: "node metrics coverage", Value: fmt.Sprintf("%d of %d nodes sampled", sampled, len(ns.Coverage)),
			Limit: "every node at hold start, baseline and end", Pass: sampled == len(ns.Coverage), Gating: gate, Note: note})
	}
	cpuCheck := func(kind string) {
		var n, unmeasured int
		var sum, worst float64
		var worstAgent string
		for _, c := range rec.AgentCPU {
			if c.Kind != kind {
				continue
			}
			if !c.Measured {
				unmeasured++
				continue
			}
			n++
			sum += c.BusyFraction
			if c.BusyFraction >= worst {
				worst, worstAgent = c.BusyFraction, c.Agent
			}
		}
		if n+unmeasured == 0 {
			return
		}
		gate := !faultRelaxed(rec) && !rec.UnmeasuredWaived
		if n == 0 {
			add(Check{Name: kind + " CPU", Value: "not measured", Limit: "< " + pctOf(spec.MaxGeneratorCPU), Pass: false, Gating: gate,
				Note: fmt.Sprintf("%d boxes could not be read (GET /v1/host)", unmeasured)})
			return
		}
		add(Check{Name: kind + " CPU", Value: fmt.Sprintf("worst %.0f%% (%s), mean %.0f%% over %d boxes", worst*100, worstAgent, sum/float64(n)*100, n),
			Limit: "< " + pctOf(spec.MaxGeneratorCPU), Pass: worst < spec.MaxGeneratorCPU && unmeasured == 0, Gating: gate,
			Note: fmt.Sprintf("busy fraction of the whole box over the hold; %d boxes unmeasured; a saturated box measures itself", unmeasured)})
	}
	cpuCheck("generator")
	cpuCheck("publisher")
	switch {
	case ns.Measured && !ns.GrowthMeasured:
		add(Check{Name: "node memory and goroutines", Value: "not measured", Limit: "flat", Pass: true, Gating: false,
			Note: "hold not longer than the servers' channel idle timeout: growth is measured from hold start + idle timeout"})
	case ns.Measured:
		// A killed node moves its connections to the survivors, so growth
		// across the hold is expected in a fault run: reported, not gated.
		gate, note := !faultRelaxed(rec), ""
		if !gate {
			note = "fault run: surviving nodes take the killed node's load"
		}
		add(Check{Name: "node memory growth over hold", Value: fmt.Sprintf("%.1f%%", ns.MemoryGrowth*100),
			Limit: fmt.Sprintf("<= %.0f%%", spec.MaxMemoryGrowth*100), Pass: ns.MemoryGrowth <= spec.MaxMemoryGrowth, Gating: gate, Note: note})
		add(Check{Name: "node goroutine growth over hold", Value: fmt.Sprintf("%.1f%%", ns.GoroutineGrowth*100),
			Limit: fmt.Sprintf("<= %.0f%%", spec.MaxGoroutineGrowth*100), Pass: ns.GoroutineGrowth <= spec.MaxGoroutineGrowth, Gating: gate, Note: note})
	default:
		add(Check{Name: "node memory and goroutines", Value: "not measured", Limit: "flat", Pass: true, Gating: false,
			Note: "no node metrics URLs in the inventory"})
	}
	if res.Attachments.Discontinuities > 0 {
		add(Check{Name: "resume discontinuities", Value: fmt.Sprint(res.Attachments.Discontinuities), Limit: "reported",
			Pass: true, Gating: false, Note: "server declined a resume (outside the continuity window or replay cap)"})
	}
	rec.ServerBoundPerAttachment = rec.BoundRatio()
	rec.Checks = checks
	rec.Pass = true
	for _, c := range checks {
		if c.Gating && !c.Pass {
			rec.Pass = false
		}
	}
	rec.Verdict = "PASS"
	if !rec.Pass {
		rec.Verdict = "FAIL"
	}
}

// Markdown renders the run record as the tables a run leaves behind.
func (rec *RunRecord) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Run %s: shape %s at %gx, scale %g: %s\n\n", rec.RunID, rec.Shape, rec.Multiplier, rec.Scale, rec.Verdict)
	fmt.Fprintf(&b, "Scenario `%s`, bus `%s`, %d nodes", rec.Scenario, rec.Bus, rec.Nodes)
	if rec.Shards > 1 {
		fmt.Fprintf(&b, ", %d shards", rec.Shards)
	}
	start := time.UnixMicro(rec.StartUS).UTC()
	fmt.Fprintf(&b, ". Ramp from %s, hold %s.\n\n", start.Format(time.RFC3339), time.Duration(rec.MeasureEndUS-rec.MeasureStartUS)*time.Microsecond)
	if len(rec.Environment) > 0 {
		keys := make([]string, 0, len(rec.Environment))
		for k := range rec.Environment {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "- %s: %s\n", k, rec.Environment[k])
		}
		b.WriteString("\n")
	}
	b.WriteString("| Check | Value | Limit | Result |\n|---|---|---|---|\n")
	for _, c := range rec.Checks {
		res := "pass"
		if !c.Pass {
			res = "FAIL"
		}
		if !c.Gating {
			res += " (reported)"
		}
		note := ""
		if c.Note != "" {
			note = " (" + c.Note + ")"
		}
		fmt.Fprintf(&b, "| %s | %s%s | %s | %s |\n", c.Name, c.Value, note, c.Limit, res)
	}
	if rec.UnsampledNote != "" {
		fmt.Fprintf(&b, "\n%s\n", rec.UnsampledNote)
	}
	r := rec.Result
	b.WriteString("\n| Measure | Planned | Measured |\n|---|---|---|\n")
	fmt.Fprintf(&b, "| Connections | %d | %d open at end of hold (peak %d) |\n", rec.Plan.Connections, r.Connections.OpenAtMeasureEnd, r.Connections.Peak)
	fmt.Fprintf(&b, "| Attachments | %d | %d attached at end of hold |\n", rec.Plan.Attachments, r.Attachments.AttachedAtMeasureEnd)
	fmt.Fprintf(&b, "| Publishes/s | %.0f | %.0f achieved (%.0f offered) |\n", rec.Plan.PublishesPerSec, r.Publishes.AchievedRate, r.Publishes.OfferedRate)
	fmt.Fprintf(&b, "| Deliveries/s | %.0f | %.0f |\n", rec.Plan.DeliveriesPerSec, r.Deliveries.Rate)
	fmt.Fprintf(&b, "| Connects/s (churn) | %.0f | %d reconnects (%d churn, %d unplanned) |\n", rec.Plan.ConnectsPerSec, r.Connections.Reconnects, r.Connections.ChurnDrops, r.Connections.UnplannedDrops)
	fmt.Fprintf(&b, "| Channel opens/s (churn) | %.0f | %d opens |\n", rec.Plan.ChannelOpensPerSec, r.Attachments.ChannelOpens)
	if rec.Plan.PresenceMembers > 0 {
		fmt.Fprintf(&b, "| Presence members | %d | %d entered, %d left, %d nacks |\n", rec.Plan.PresenceMembers, r.Presence.Entered, r.Presence.Left, r.Presence.Nacks)
	}
	if p := r.Publishes; p.Offered > 0 {
		fmt.Fprintf(&b, "\nPublishes: %d offered, %d dropped by the generator, %d sent, %d acked, %d retries, %d answered 429, %d rejected, %d unresolved at the end.\n",
			p.Offered, p.Dropped, p.Sent, p.Acked, p.Retries, p.Throttled, p.Rejected, p.Unresolved)
	}
	if d := r.Deliveries; d.InWindow > 0 {
		fmt.Fprintf(&b, "\nDeliveries in the hold: %d; %d with negative latency from the send (clock skew detector).\n", d.InWindow, d.NegativeLatency)
	}
	if len(rec.ServerConfig) > 0 {
		variants, unreported := ServerConfigVariants(rec.ServerConfig)
		b.WriteString("\nServer flags in effect, read from the nodes' /metrics")
		switch len(variants) {
		case 0:
			b.WriteString(": none reported (the nodes export them only in cluster mode, and the inventory needs their metrics URLs).\n")
		case 1:
			fmt.Fprintf(&b, " (identical on %d of %d nodes): %s", len(variants[0].Nodes), len(rec.ServerConfig), variants[0].Text)
			if _, ok := variants[0].Flags[FlagBusSweepInterval]; !ok {
				b.WriteString(" (bus sweep interval: not exported by the nodes)")
			}
			b.WriteString(".\n")
		default:
			b.WriteString(": THE NODES DISAGREE.\n\n")
			for _, v := range variants {
				fmt.Fprintf(&b, "- %s: %s\n", strings.Join(v.Nodes, ", "), v.Text)
			}
		}
		if unreported > 0 && len(variants) > 0 {
			fmt.Fprintf(&b, "%d nodes reported no flags.\n", unreported)
		}
	}
	if len(rec.AgentCPU) > 0 {
		b.WriteString("\n| Agent box | Kind | CPU busy over the hold |\n|---|---|---|\n")
		for _, c := range rec.AgentCPU {
			v := "not measured"
			if c.Measured {
				v = fmt.Sprintf("%.0f%% of %d CPUs", c.BusyFraction*100, c.CPUs)
			}
			fmt.Fprintf(&b, "| %s | %s | %s |\n", c.Agent, c.Kind, v)
		}
	}
	if len(rec.Clocks) > 0 {
		b.WriteString("\n| Generator box | Clock offset from NTP at start | at end |\n|---|---|---|\n")
		off := func(o *ClockOffset) string {
			if o == nil {
				return "not measured"
			}
			return fmt.Sprintf("%+.2f ms (rtt %.1f ms)", float64(o.OffsetUS)/1000, float64(o.RTTUS)/1000)
		}
		for _, c := range rec.Clocks {
			end := off(c.End)
			if c.Error != "" && c.Start == nil && c.End == nil {
				end = c.Error
			}
			fmt.Fprintf(&b, "| %s | %s | %s |\n", c.Agent, off(c.Start), end)
		}
	}
	ns := rec.NodeStats
	b.WriteString("\n| Over the hold | Start | End |\n|---|---|---|\n")
	fmt.Fprintf(&b, "| Generator connections | %d | %d |\n", r.Connections.OpenAtMeasureStart, r.Connections.OpenAtMeasureEnd)
	fmt.Fprintf(&b, "| Generator attachments | %d | %d (plan %d + %d churn slots) |\n", r.Attachments.AttachedAtMeasureStart, r.Attachments.AttachedAtMeasureEnd, rec.Plan.Attachments, rec.Plan.ChurnSlots)
	if len(ns.Coverage) > 0 {
		sampled := 0
		for _, c := range ns.Coverage {
			if c.Sampled(ns.BaselineDue) {
				sampled++
			}
		}
		fmt.Fprintf(&b, "| Nodes sampled at hold start, baseline and end | %d of %d | |\n", sampled, len(ns.Coverage))
	}
	if ns.Measured {
		fmt.Fprintf(&b, "| Server connections (sum of nodes) | %.0f | %.0f |\n", ns.ConnectionsAtStart, ns.ConnectionsOpen)
		fmt.Fprintf(&b, "| Server channels bound (sum of nodes) | %.0f | %.0f |\n", ns.ChannelsBoundAtStart, ns.ChannelsBoundAtEnd)
	}
	if ns.GrowthMeasured {
		off := time.Duration(ns.GrowthBaselineUS-rec.MeasureStartUS) * time.Microsecond
		fmt.Fprintf(&b, "\n| Growth window (from hold start + %s) | Baseline | End |\n|---|---|---|\n", off.Round(time.Second))
		fmt.Fprintf(&b, "| Server channels bound (sum of nodes) | %.0f | %.0f |\n", ns.ChannelsBoundAtBaseline, ns.ChannelsBoundAtEnd)
		fmt.Fprintf(&b, "| Node RSS (sum, MiB) | %.0f | %.0f |\n", ns.RSSBytesAtBaseline/(1<<20), ns.RSSBytes/(1<<20))
		fmt.Fprintf(&b, "| Node goroutines (sum) | %.0f | %.0f |\n", ns.GoroutinesAtBaseline, ns.GoroutinesAtEnd)
	}
	if ratio := rec.BoundRatio(); ratio > 0 {
		fmt.Fprintf(&b, "\nServer channels bound / generator attachments at the end of the hold: %.3f.\n", ratio)
	}
	b.WriteString("\n| Latency | n | p50 | p90 | p99 | p99.9 | max |\n|---|---|---|---|---|---|---|\n")
	names := make([]string, 0, len(r.Latency))
	for n, h := range r.Latency {
		if h.Count() > 0 {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		s := r.Latency[n].Snapshot()
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s |\n", n, s.Count, msOf(s.P50US), msOf(s.P90US), msOf(s.P99US), msOf(s.P999US), msOf(s.MaxUS))
	}
	f := rec.Footprint
	if f.VCPU > 0 || f.CoresUsed > 0 {
		b.WriteString("\n| Footprint | Provisioned vCPU | Used cores |\n|---|---|---|\n")
		fmt.Fprintf(&b, "| per 100k connections | %.2f | %.2f |\n", f.VCPUPer100kConns, f.UsedPer100kConns)
		fmt.Fprintf(&b, "| per 100k deliveries/s | %.2f | %.2f |\n", f.VCPUPer100kDeliveries, f.UsedPer100kDeliveries)
		fmt.Fprintf(&b, "| per 10k writes/s | %.2f | %.2f |\n", f.VCPUPer10kWrites, f.UsedPer10kWrites)
		fmt.Fprintf(&b, "\nNode memory: %.1f GB provisioned, %.2f GB RSS at end of hold (%.2f GB per 100k connections).\n", f.MemoryGB, f.RSSGB, f.RSSGBPer100kConns)
	}
	if rec.Fault != nil {
		fmt.Fprintf(&b, "\nFault injected at %s: `%s` (exit %d).\n", time.UnixMicro(rec.Fault.AtUS).UTC().Format(time.RFC3339), rec.Fault.Command, rec.Fault.ExitCode)
	}
	if len(r.FirstViolations) > 0 {
		b.WriteString("\nFirst violations:\n\n")
		for _, v := range r.FirstViolations {
			fmt.Fprintf(&b, "- %s\n", v.String())
		}
	}
	if len(rec.Errors) > 0 {
		fmt.Fprintf(&b, "\n%d generator errors logged; first: %s\n", len(rec.Errors), rec.Errors[0])
	}
	return b.String()
}

// LogLine is the one-line summary for LOG.md.
func (rec *RunRecord) LogLine() string {
	del := rec.Result.Latency[LatDeliveryCrossNode]
	if del == nil || del.Count() == 0 {
		del = rec.Result.Latency[LatDelivery]
	}
	var p50, p99 uint64
	if del != nil {
		p50, p99 = del.Quantile(0.5), del.Quantile(0.99)
	}
	var bad int64
	for _, v := range rec.Result.Violations {
		bad += v
	}
	return fmt.Sprintf("%s UTC | run %s shape %s %gx scale %g bus %s nodes %d | %s: conns %d/%d, pub %.0f/s, del %.0f/s, p50 %s p99 %s, violations %d | see results/%s/summary.md",
		time.Now().UTC().Format("2006-01-02 15:04"), rec.RunID, rec.Shape, rec.Multiplier, rec.Scale, rec.Bus, rec.Nodes, rec.Verdict,
		rec.Result.Connections.OpenAtMeasureEnd, rec.Result.Connections.Target, rec.Result.Publishes.AchievedRate, rec.Result.Deliveries.Rate,
		msOf(p50), msOf(p99), bad, rec.RunID)
}
