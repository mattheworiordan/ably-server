package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func histOf(vals ...int64) *Histogram {
	h := NewHistogram()
	for _, v := range vals {
		h.RecordMicros(v)
	}
	return h
}

// fixtureSerials is a publisher serial log of n messages all committed
// after serialAt(0).
func fixtureSerials(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = serialAt(1 + i)
	}
	return out
}

// fixtureSummaries is a small passing run: one subscriber, one REST
// publisher, fast deliveries, full connections.
func fixtureSummaries() []*Summary {
	sub := &Summary{
		Role: RoleSubscriber, Index: 0, Count: 1,
		Connections: ConnStats{Target: 100, OpenAtMeasureEnd: 100, Peak: 100},
		Attachments: AttachStats{Target: 150, AttachedAtMeasureEnd: 150},
		Deliveries:  DeliveryStats{Received: 1000, InWindow: 900, Rate: 90},
		Latency: map[string]*Histogram{
			LatDelivery:          histOf(2000, 3000, 4000, 5000),
			LatDeliveryCrossNode: histOf(3000, 4000, 5000),
			LatConnectAttach:     histOf(20000, 30000),
		},
		Correctness: CorrectnessSummary{
			CheckedMessages: 500,
			Violations:      map[string]int64{"duplicate": 0, "gap": 0},
			Channels: map[string]*ChannelSeen{
				"ch": {LatestAttachUS: 1_000_000, Attachments: 1, MinMaxSeq: map[string]int64{"p": 9}},
			},
			// The attachment attached at serialAt(0) and its first seq
			// was 0.
			AttachClaims: []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(0), FirstSeq: 0}},
		},
	}
	pub := &Summary{
		Role: RoleREST, Index: 0, Count: 1,
		Publishes: PublishStats{TargetRate: 100, OfferedRate: 100, AchievedRate: 99, Offered: 1000, Acked: 990},
		Latency:   map[string]*Histogram{LatRESTAck: histOf(5000, 6000, 7000)},
		Streams:   map[string]map[string]StreamRecord{"ch": {"p": {LastAckedSeq: 9, LastAckedUS: 5_000_000, Serials: fixtureSerials(10)}}},
	}
	return []*Summary{sub, pub}
}

func evalFixture(t *testing.T, mutate func(sums []*Summary, rec *RunRecord)) *RunRecord {
	t.Helper()
	sums := fixtureSummaries()
	rec := &RunRecord{Plan: Totals{Connections: 100, PublishesPerSec: 100, DeliveriesPerSec: 90, SampledChannels: 1, SampledSubscribed: 1, SubscribedChannels: 2, SampledStreams: 1}}
	if mutate != nil {
		mutate(sums, rec)
	}
	rec.Result = MergeSummaries(sums, time.Second)
	if mutate != nil {
		mutate(nil, rec)
	}
	Evaluate(rec, PassSpec{})
	return rec
}

func failing(rec *RunRecord) []string {
	var out []string
	for _, c := range rec.Checks {
		if c.Gating && !c.Pass {
			out = append(out, c.Name)
		}
	}
	return out
}

func TestEvaluatePassingRun(t *testing.T) {
	rec := evalFixture(t, nil)
	if !rec.Pass || rec.Verdict != "PASS" {
		t.Fatalf("want PASS, failing: %v\n%s", failing(rec), rec.Markdown())
	}
	if rec.Result.Tail.Checked != 1 || rec.Result.Tail.Lost != 0 {
		t.Errorf("tail %+v", rec.Result.Tail)
	}
}

func TestEvaluateFailures(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(sums []*Summary, rec *RunRecord)
		want   string
	}{
		{"slow p50", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Latency[LatDeliveryCrossNode] = histOf(60000, 70000, 80000)
			}
		}, "delivery p50"},
		{"slow p99", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Latency[LatDeliveryCrossNode] = histOf(append(make([]int64, 98), 300000, 300000)...)
			}
		}, "delivery p99"},
		{"slow REST ack", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Latency[LatRESTAck] = histOf(150000)
			}
		}, "REST publish ACK p99"},
		{"slow attach", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Latency[LatConnectAttach] = histOf(900000)
			}
		}, "connect+attach p99"},
		{"duplicate", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Correctness.Violations["duplicate"] = 1
			}
		}, "loss, duplicate, reorder"},
		{"tail loss", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 12, LastAckedUS: 5_000_000}
			}
		}, "loss, duplicate, reorder"},
		{"rate not achieved", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Publishes.AchievedRate = 50
			}
		}, "publish rate achieved"},
		{"generator fell behind", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Publishes.OfferedRate = 50
				s[1].Publishes.AchievedRate = 50
			}
		}, "offered rate vs target"},
		{"connections lost", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Connections.OpenAtMeasureEnd = 90
			}
		}, "connections open"},
		{"memory growth", func(s []*Summary, r *RunRecord) {
			if s == nil {
				r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, MemoryGrowth: 0.2}
			}
		}, "node memory growth"},
		{"goroutine growth", func(s []*Summary, r *RunRecord) {
			if s == nil {
				r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, GoroutineGrowth: 0.5}
			}
		}, "node goroutine growth"},
		{"generator attachments drift", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Connections.OpenAtMeasureStart = 100
				s[0].Attachments.AttachedAtMeasureStart = 150
				s[0].Attachments.AttachedAtMeasureEnd = 180
			}
		}, "generator load steady"},
		{"planned load never delivered", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Deliveries.Rate = 15
			}
		}, "deliveries vs plan"},
		{"messages lost between the attach point and the first delivery", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				// The attachment's first delivery was seq 3, but seqs 0-2
				// were published after its attach point.
				s[0].Correctness.AttachClaims[0].FirstSeq = 3
			}
		}, "loss, duplicate, reorder"},
		{"attach-point check settles nothing", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Correctness.AttachClaims = nil
			}
		}, "sample coverage (attach)"},
		{"attach-point check cannot settle claims", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 9, LastAckedUS: 5_000_000} // no serial log
			}
		}, "sample coverage (attach)"},
		{"tail check skipped every stream on the margin", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				// The last acknowledgement came 0.5 s after the attach (at
				// 1.0 s): inside the 1 s margin, so nothing is checkable.
				s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 9, LastAckedUS: 1_500_000, Serials: fixtureSerials(10)}
			}
		}, "tail check coverage"},
		{"tail check covers under half the sampled streams", func(_ []*Summary, r *RunRecord) {
			r.Plan.SampledStreams = 3 // one of three checked
		}, "tail check coverage"},
		{"nothing checked", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[0].Correctness.CheckedMessages = 0
			}
		}, "sample coverage"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := evalFixture(t, c.mutate)
			if rec.Pass {
				t.Fatalf("want FAIL on %q\n%s", c.want, rec.Markdown())
			}
			found := false
			for _, n := range failing(rec) {
				if strings.HasPrefix(n, c.want) {
					found = true
				}
			}
			if !found {
				t.Fatalf("failing checks %v, want one starting %q", failing(rec), c.want)
			}
		})
	}
}

func TestEvaluateStretchIsReportedNotGating(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			vals := make([]int64, 100)
			for i := range vals {
				vals[i] = 10000
			}
			vals[98], vals[99] = 150000, 150000 // p99 150ms: passes 250ms, misses the 100ms stretch
			s[0].Latency[LatDeliveryCrossNode] = histOf(vals...)
		}
	})
	if !rec.Pass {
		t.Fatalf("stretch miss must not fail the run: %v", failing(rec))
	}
	for _, c := range rec.Checks {
		if c.Name == "delivery p99 stretch" && c.Pass {
			t.Fatal("stretch check should report a miss")
		}
	}
}

func TestMergeSummariesAddsAndMerges(t *testing.T) {
	a, b := fixtureSummaries(), fixtureSummaries()
	r := MergeSummaries(append(a, b...), time.Second)
	if r.Connections.Target != 200 || r.Deliveries.Received != 2000 || r.Publishes.Acked != 1980 {
		t.Fatalf("merge %+v", r)
	}
	if r.Latency[LatDelivery].Count() != 8 {
		t.Fatalf("delivery count %d", r.Latency[LatDelivery].Count())
	}
	if r.Tail.Checked != 2 {
		t.Fatalf("tail %+v", r.Tail)
	}
}

func TestParseMetricSums(t *testing.T) {
	text := `# HELP go_goroutines Number of goroutines.
# TYPE go_goroutines gauge
go_goroutines 42
process_resident_memory_bytes 1.2e+08
ably_http_requests_total{method="POST",route="/x",status="201"} 10
ably_http_requests_total{method="GET",route="/y",status="200"} 5
ably_connections_open 7
other_metric{a="b"} 1
`
	m, err := ParseMetricSums(strings.NewReader(text), "go_goroutines", "process_resident_memory_bytes", "ably_http_requests_total", "ably_connections_open", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if m["go_goroutines"] != 42 || m["process_resident_memory_bytes"] != 1.2e8 || m["ably_http_requests_total"] != 15 || m["ably_connections_open"] != 7 {
		t.Fatalf("parsed %v", m)
	}
	if _, ok := m["missing"]; ok {
		t.Fatal("missing metric present")
	}
}

func TestComputeNodeStats(t *testing.T) {
	s := func(node string, at int64, rss, gor, cpu float64) NodeSample {
		return NodeSample{Node: node, AtUS: at, Values: map[string]float64{
			"process_resident_memory_bytes": rss, "go_goroutines": gor, "process_cpu_seconds_total": cpu, "ably_connections_open": 10,
		}}
	}
	samples := []NodeSample{
		s("a", 0, 100, 100, 0), // before the hold: ignored
		s("a", 10_000_000, 200, 1000, 10),
		s("a", 20_000_000, 210, 1050, 30),
		s("b", 10_000_000, 100, 500, 0),
		s("b", 20_000_000, 130, 500, 10),
		{Node: "c", AtUS: 15_000_000, Error: "down"},
	}
	ns := ComputeNodeStats(samples, 5_000_000, 25_000_000, 0) // baseline 0: from hold start
	if !ns.Measured || ns.MemoryGrowth < 0.299 || ns.MemoryGrowth > 0.301 || ns.GoroutineGrowth < 0.049 || ns.GoroutineGrowth > 0.051 {
		t.Fatalf("stats %+v", ns)
	}
	if ns.CoresUsed != 3 || ns.RSSBytes != 340 || ns.ConnectionsOpen != 20 {
		t.Fatalf("use %+v", ns)
	}
}

// TestComputeNodeStatsGrowthFromBaseline: growth is measured from the
// first sample at or after hold start + the servers' idle timeout, so
// bound channels (and memory) growing into their working set early in
// the hold do not count, while the bound-channel counts at hold start,
// baseline and end are all reported.
func TestComputeNodeStatsGrowthFromBaseline(t *testing.T) {
	s := func(node string, at int64, rss, gor, bound float64) NodeSample {
		return NodeSample{Node: node, AtUS: at, Values: map[string]float64{
			"process_resident_memory_bytes": rss, "go_goroutines": gor, "ably_channels_bound": bound,
		}}
	}
	// Hold 0..120 s, idle timeout 60 s: RSS and bound channels climb for
	// the first minute, then plateau.
	samples := []NodeSample{
		s("a", 1_000_000, 100, 100, 1000),
		s("a", 30_000_000, 150, 150, 1500),
		s("a", 61_000_000, 200, 200, 2000),
		s("a", 90_000_000, 202, 200, 2010),
		s("a", 118_000_000, 204, 201, 2000),
		s("b", 1_000_000, 100, 100, 1000),
		s("b", 61_000_000, 180, 190, 1900),
		s("b", 118_000_000, 180, 190, 1900),
	}
	ns := ComputeNodeStats(samples, 0, 120_000_000, 60_000_000)
	if !ns.Measured || !ns.GrowthMeasured {
		t.Fatalf("measured %v growth measured %v", ns.Measured, ns.GrowthMeasured)
	}
	if ns.MemoryGrowth < 0.019 || ns.MemoryGrowth > 0.021 || ns.GoroutineGrowth < 0.004 || ns.GoroutineGrowth > 0.006 {
		t.Fatalf("growth from baseline: memory %.4f goroutines %.4f, want 0.02 and 0.005", ns.MemoryGrowth, ns.GoroutineGrowth)
	}
	if ns.ChannelsBoundAtStart != 2000 || ns.ChannelsBoundAtBaseline != 3900 || ns.ChannelsBoundAtEnd != 3900 {
		t.Fatalf("bound start/baseline/end = %.0f/%.0f/%.0f, want 2000/3900/3900", ns.ChannelsBoundAtStart, ns.ChannelsBoundAtBaseline, ns.ChannelsBoundAtEnd)
	}
	if ns.GoroutinesAtBaseline != 390 || ns.GoroutinesAtEnd != 391 {
		t.Fatalf("goroutines baseline/end = %.0f/%.0f", ns.GoroutinesAtBaseline, ns.GoroutinesAtEnd)
	}
	// From hold start the same samples show 100% growth.
	if ns0 := ComputeNodeStats(samples, 0, 120_000_000, 0); ns0.MemoryGrowth < 1 {
		t.Fatalf("growth from hold start = %.2f, want >= 1", ns0.MemoryGrowth)
	}

	// A hold no longer than the idle timeout leaves nothing to measure
	// growth over: reported, not gated.
	short := ComputeNodeStats(samples[:2], 0, 60_000_000, 60_000_000)
	if !short.Measured || short.GrowthMeasured {
		t.Fatalf("short hold: measured %v growth measured %v", short.Measured, short.GrowthMeasured)
	}
	rec := &RunRecord{MeasureStartUS: 0, MeasureEndUS: 60_000_000, NodeStats: short}
	Evaluate(rec, DefaultPass())
	var found bool
	for _, c := range rec.Checks {
		if c.Name == "node memory and goroutines" {
			found = true
			if c.Gating || c.Value != "not measured" {
				t.Fatalf("short-hold growth check %+v, want non-gating not measured", c)
			}
		}
		if c.Name == "node memory growth over hold" {
			t.Fatalf("short hold gated memory growth: %+v", c)
		}
	}
	if !found {
		t.Fatal("no growth check for a short hold")
	}

	full := &RunRecord{MeasureStartUS: 0, MeasureEndUS: 120_000_000, NodeStats: ns}
	Evaluate(full, DefaultPass())
	if md := full.Markdown(); !strings.Contains(md, "Growth window (from hold start + 1m0s)") || !strings.Contains(md, "| Server channels bound (sum of nodes) | 3900 | 3900 |") {
		t.Fatalf("report lacks the growth window table:\n%s", md)
	}
}

func TestComputeFootprint(t *testing.T) {
	inv := &Inventory{Nodes: []InventoryNode{{VCPU: 8, MemoryGB: 16}, {VCPU: 8, MemoryGB: 16}}}
	res := RunResult{Connections: ConnStats{OpenAtMeasureEnd: 200000}, Deliveries: DeliveryStats{Rate: 50000}, Publishes: PublishStats{AchievedRate: 20000}}
	f := ComputeFootprint(inv, res, NodeStats{CoresUsed: 4, RSSBytes: 4 << 30})
	if f.VCPUPer100kConns != 8 || f.VCPUPer100kDeliveries != 32 || f.VCPUPer10kWrites != 8 || f.UsedPer100kConns != 2 || f.RSSGBPer100kConns != 2 {
		t.Fatalf("footprint %+v", f)
	}
}

func TestBuildJobsAssignsRoles(t *testing.T) {
	sc, err := ParseScenario([]byte(testScenario))
	if err != nil {
		t.Fatal(err)
	}
	p, err := sc.Resolve(1, 0.1, "t")
	if err != nil {
		t.Fatal(err)
	}
	inv := &Inventory{
		Key:   "a.b:c",
		Nodes: []InventoryNode{{Endpoint: "n1:80"}, {Endpoint: "n2:80"}},
		Agents: []InventoryAgent{
			{Name: "g1", URL: "http://g1", Roles: []string{RoleSubscriber, RoleRealtime}},
			{Name: "g2", URL: "http://g2", Roles: []string{RoleSubscriber}},
			{Name: "p1", URL: "http://p1", Roles: []string{RoleREST}, Workers: 1000},
		},
	}
	cfg := &ConductorConfig{Scenario: sc, RunID: "r1", RunTag: "t", Inventory: inv}
	jobs, err := BuildJobs(cfg, p, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, j := range jobs {
		got[j.spec.ID] = j.agent.Name + " " + j.spec.Role + " " + string(rune('0'+j.spec.Index)) + "/" + string(rune('0'+j.spec.Count))
		if err := j.spec.Validate(); err != nil {
			t.Errorf("job %s invalid: %v", j.spec.ID, err)
		}
	}
	want := map[string]string{
		"r1-subscriber-0":         "g1 subscriber 0/2",
		"r1-subscriber-1":         "g2 subscriber 1/2",
		"r1-rest-publisher-0":     "p1 rest-publisher 0/1",
		"r1-realtime-publisher-0": "g1 realtime-publisher 0/1",
	}
	if len(got) != len(want) {
		t.Fatalf("jobs %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	// A needed role with no agent is refused.
	inv.Agents = inv.Agents[:2]
	if _, err := BuildJobs(cfg, p, time.Now()); err == nil {
		t.Error("missing rest-publisher agent accepted")
	}
}

func TestReportGroupsRepeatsAndCurves(t *testing.T) {
	mk := func(id, shape string, mult float64, nodes int, writes float64, pass bool) *RunRecord {
		rec := evalFixture(t, nil)
		rec.RunID, rec.Shape, rec.Multiplier, rec.Scale, rec.Bus, rec.Nodes, rec.Version = id, shape, mult, 1, "nats", nodes, 1
		rec.Result.Publishes.AchievedRate = writes
		rec.Pass = pass
		return rec
	}
	recs := []*RunRecord{
		mk("m1", "M", 1, 10, 7000, true), mk("m2", "M", 1, 10, 7200, true), mk("m3", "M", 1, 10, 6800, false),
		mk("m5", "M", 1, 5, 7000, true), mk("m20", "M", 1, 20, 7000, true),
		mk("d1", "D", 2, 10, 104000, true),
	}
	smoke := mk("s", "D", 1, 3, 500, true)
	smoke.Scale = 0.01
	recs = append(recs, smoke)
	md := Report(recs)
	for _, want := range []string{
		"## Runs", "| s | D | 1x | 0.01 |", "## Envelope by deployment size and shape",
		"M 1x, bus nats, 10 nodes | 2 of 3", "7000 (6800 to 7200, ±3%)", "## Footprint", "## Node curve", "| M 1x, nats | 20 |",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q\n%s", want, md)
		}
	}
	if strings.Contains(md, "## Shard curve") {
		t.Error("shard curve without shard variation")
	}
}

// Fault records: a node kill and a bus kill that ran and succeeded, and a
// hook that failed.
func nodeKill() *FaultRecord { return &FaultRecord{Command: "kill node2", Kind: FaultNodeKill} }
func busKill() *FaultRecord  { return &FaultRecord{Command: "kill nats2", Kind: FaultBusKill} }
func failedKill() *FaultRecord {
	return &FaultRecord{Command: "kill node2", Kind: FaultNodeKill, ExitCode: 3, Output: "no such node"}
}

func TestEvaluateNodeKillReportsGrowthWithoutGating(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, MemoryGrowth: 1.4, GoroutineGrowth: 1.1}
			r.Fault = nodeKill()
		}
	})
	if !rec.Pass {
		t.Fatalf("fault run failed on growth: %v", failing(rec))
	}
}

func TestEvaluateSteadyLoadAndBoundRatio(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s != nil {
			s[0].Connections.OpenAtMeasureStart = 100
			s[0].Attachments.AttachedAtMeasureStart = 151
		} else {
			r.NodeStats = NodeStats{Measured: true, ChannelsBoundAtStart: 140, ChannelsBoundAtEnd: 165}
		}
	})
	if !rec.Pass {
		t.Fatalf("steady run failed: %v", failing(rec))
	}
	if r := rec.ServerBoundPerAttachment; r < 1.09 || r > 1.11 {
		t.Fatalf("bound ratio %v, want 165/150", r)
	}
	md := rec.Markdown()
	for _, want := range []string{"| Generator attachments | 151 | 150", "| Server channels bound (sum of nodes) | 140 | 165 |", "generator attachments at the end of the hold: 1.100"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q\n%s", want, md)
		}
	}
}

func TestEvaluateAttachGapIsCountedAsAViolation(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			s[0].Correctness.AttachClaims[0].FirstSeq = 3
		}
	})
	if rec.Result.Violations["attach_gap"] != 3 || rec.Result.Attach.Missed != 3 {
		t.Fatalf("violations %v attach %+v", rec.Result.Violations, rec.Result.Attach)
	}
	if len(rec.Result.FirstViolations) == 0 || rec.Result.FirstViolations[0].Kind != "attach_gap" {
		t.Fatalf("first violations %+v", rec.Result.FirstViolations)
	}
}

func TestEvaluateAttachCoverageStaysGatedInAFaultRun(t *testing.T) {
	// A run with no claim at all, or too few settled, fails "sample coverage
	// (attach)" whatever fault ran: no kind of fault relaxes it.
	for _, kind := range []*FaultRecord{nil, nodeKill(), busKill()} {
		rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[0].Correctness.AttachClaims = nil
			} else {
				r.Fault = kind
			}
		})
		mustFail(t, rec, "sample coverage (attach)")
	}
}

func TestEvaluateZeroAttachClaimsFailsSampleCoverageAttach(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			s[0].Correctness.AttachClaims = nil
		}
	})
	mustFail(t, rec, "sample coverage (attach)")
	for _, c := range rec.Checks {
		if c.Name == "sample coverage (attach)" && !strings.Contains(c.Value, "0 of 0 claims settled") {
			t.Fatalf("%q", c.Value)
		}
	}
}

func TestEvaluateTailCoverageThresholdAndFault(t *testing.T) {
	// One of two sampled streams checked is exactly half: passes.
	rec := evalFixture(t, func(_ []*Summary, r *RunRecord) { r.Plan.SampledStreams = 2 })
	for _, n := range failing(rec) {
		if n == "tail check coverage" {
			t.Fatalf("half coverage passes: %v", failing(rec))
		}
	}
	// Skipped for the margin on its one subscriber: not checked, and no fault
	// excuses that.
	rec = evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s != nil {
			s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 9, LastAckedUS: 1_500_000, Serials: fixtureSerials(10)}
		} else {
			r.Fault = nodeKill()
		}
	})
	mustFail(t, rec, "tail check coverage")
	md := rec.Markdown()
	if !strings.Contains(md, "0 of 1 sampled streams checked (0.0%)") || !strings.Contains(md, "skipped 1 for the margin") ||
		!strings.Contains(md, "1 (subscriber, stream) pairs skipped for the margin") {
		t.Fatalf("the fraction and the skips by reason must be printed:\n%s", md)
	}
}

func TestTailCoverageRowSplitsSkipsByReason(t *testing.T) {
	// Of four planned streams: one checked, one skipped for the margin, two
	// never seen by a continuous subscriber.
	rec := evalFixture(t, func(_ []*Summary, r *RunRecord) { r.Plan.SampledStreams = 4 })
	var row Check
	for _, c := range rec.Checks {
		if c.Name == "tail check coverage" {
			row = c
		}
	}
	if !strings.Contains(row.Value, "1 of 4 sampled streams checked (25.0%)") || !strings.Contains(row.Value, "skipped 0 for the margin") || !strings.Contains(row.Value, "3 with no continuous subscriber or no acknowledgement") {
		t.Fatalf("%q", row.Value)
	}
	if row.Pass || !row.Gating {
		t.Fatalf("25%% is under the 50%% floor: %+v", row)
	}
}

// presenceFixtureRecord is a presence-only run record that passed its
// member-set check.
func presenceRecord(mutate func(*RunRecord)) *RunRecord {
	rec := &RunRecord{Plan: Totals{PresenceMembers: 12, PresenceSampled: 2}}
	rec.Result.Presence = PresenceStats{
		Members: 12, Entered: 12, ChecksPlanned: 2, ChecksDone: 2, MembersPlanned: 8, MembersCompared: 8,
	}
	rec.Result.Violations = map[string]int64{}
	if mutate != nil {
		mutate(rec)
	}
	Evaluate(rec, PassSpec{})
	return rec
}

func TestEvaluatePresenceRun(t *testing.T) {
	rec := presenceRecord(nil)
	if !rec.Pass {
		t.Fatalf("a presence run that compared its sets must pass: %v\n%s", failing(rec), rec.Markdown())
	}
	cases := []struct {
		name   string
		mutate func(*RunRecord)
		want   string
	}{
		{"no presence check ran", func(r *RunRecord) {
			r.Result.Presence.ChecksPlanned, r.Result.Presence.ChecksDone = 0, 0
			r.Result.Presence.MembersPlanned, r.Result.Presence.MembersCompared = 0, 0
		}, "sample coverage"},
		{"a sampled channel was not compared", func(r *RunRecord) { r.Result.Presence.ChecksDone = 1 }, "sample coverage"},
		{"a set could not be fetched", func(r *RunRecord) { r.Result.Presence.ChecksFailed = 1 }, "sample coverage"},
		{"most members unsettled", func(r *RunRecord) { r.Result.Presence.MembersCompared = 4 }, "sample coverage"},
		{"member-set mismatch", func(r *RunRecord) { r.Result.Violations["presence_set_mismatch"] = 1 }, "presence correctness"},
		{"nack", func(r *RunRecord) { r.Result.Presence.Nacks = 1 }, "presence correctness"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := presenceRecord(c.mutate)
			if rec.Pass {
				t.Fatalf("want FAIL on %q\n%s", c.want, rec.Markdown())
			}
			found := false
			for _, n := range failing(rec) {
				found = found || strings.HasPrefix(n, c.want)
			}
			if !found {
				t.Fatalf("failing %v, want %q", failing(rec), c.want)
			}
		})
	}
}

func TestEvaluatePresenceNacksAreToleratedOnlyAfterANodeKill(t *testing.T) {
	nacked := func(fault *FaultRecord) func(*RunRecord) {
		return func(r *RunRecord) { r.Result.Presence.Nacks = 3; r.Fault = fault }
	}
	if rec := presenceRecord(nacked(nodeKill())); !rec.Pass {
		t.Fatalf("nacks after a node kill: %v", failing(rec))
	}
	if rec := presenceRecord(nacked(busKill())); rec.Pass {
		t.Fatal("a bus kill does not NACK presence operations: nothing excuses them")
	}
	if rec := presenceRecord(nacked(failedKill())); rec.Pass {
		t.Fatal("a fault hook that failed relaxes nothing")
	}
}

func TestEvaluatePublishingWithNoSampledChannelFailsSampleCoverage(t *testing.T) {
	rec := evalFixture(t, func(_ []*Summary, r *RunRecord) {
		r.Plan.SampledChannels, r.Plan.SampledSubscribed, r.Plan.SampledStreams = 0, 0, 0
	})
	found := false
	for _, n := range failing(rec) {
		found = found || n == "sample coverage"
	}
	if !found {
		t.Fatalf("sample_percent = 0 with publishing must not pass as 0 of 0 checked: %v", failing(rec))
	}
}

func mustFail(t *testing.T, rec *RunRecord, want string) {
	t.Helper()
	if rec.Pass {
		t.Fatalf("want FAIL on %q\n%s", want, rec.Markdown())
	}
	for _, n := range failing(rec) {
		if n == want {
			return
		}
	}
	t.Fatalf("failing %v, want %q", failing(rec), want)
}

func mustNotFail(t *testing.T, rec *RunRecord, name string) {
	t.Helper()
	for _, n := range failing(rec) {
		if n == name {
			t.Fatalf("%q must not gate here: %v", name, failing(rec))
		}
	}
}

func TestEvaluateNegativeLatencyDetectsClockSkew(t *testing.T) {
	// 0.2% of 900 in-window deliveries: over the 0.1% limit.
	rec := evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			s[0].Deliveries.NegativeLatency = 2
		}
	})
	mustFail(t, rec, "negative latency (clock skew)")
	rec = evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			s[0].Deliveries.NegativeLatency = 0
		}
	})
	mustNotFail(t, rec, "negative latency (clock skew)")
}

func TestEvaluateReportsLatencyFromSendBesideFromSchedule(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, _ *RunRecord) {
		if s != nil {
			s[0].Latency[LatDeliveryFromSend] = histOf(1000, 2000, 3000)
		}
	})
	var got *Check
	for i, c := range rec.Checks {
		if c.Name == "delivery latency from send (reported)" {
			got = &rec.Checks[i]
		}
	}
	if got == nil || got.Gating || !strings.Contains(got.Value, "p50") {
		t.Fatalf("from-send latency must be reported, not gated: %+v", got)
	}
	if !rec.Pass {
		t.Fatalf("%v", failing(rec))
	}
}

func clockRecord(startUS, endUS int64) ClockRecord {
	return ClockRecord{Agent: "gen-1", Start: &ClockOffset{OffsetUS: startUS, RTTUS: 300}, End: &ClockOffset{OffsetUS: endUS, RTTUS: 300}}
}

func TestEvaluateClockOffset(t *testing.T) {
	skewed := evalFixture(t, func(_ []*Summary, r *RunRecord) { r.Clocks = []ClockRecord{clockRecord(100, -6000)} })
	mustFail(t, skewed, "generator clock offset")
	ok := evalFixture(t, func(_ []*Summary, r *RunRecord) { r.Clocks = []ClockRecord{clockRecord(100, -900)} })
	mustNotFail(t, ok, "generator clock offset")
	unmeasured := evalFixture(t, func(_ []*Summary, r *RunRecord) { r.Clocks = []ClockRecord{{Agent: "gen-1", Error: "HTTP 501"}} })
	mustNotFail(t, unmeasured, "generator clock offset")
	for _, c := range unmeasured.Checks {
		if c.Name == "generator clock offset" && (c.Gating || c.Value != "not measured") {
			t.Fatalf("an unmeasured clock is reported as such: %+v", c)
		}
	}
	if md := skewed.Markdown(); !strings.Contains(md, "-6.00 ms") || !strings.Contains(md, "gen-1") {
		t.Fatalf("summary.md must carry each box's offset:\n%s", md)
	}
}

func TestEvaluatePublishRetriesAndUnresolved(t *testing.T) {
	retries := func(n int64, fault *FaultRecord) func([]*Summary, *RunRecord) {
		return func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[1].Publishes.Sent, s[1].Publishes.Retries, s[1].Publishes.Throttled = 1000, n, n
			} else {
				r.Fault = fault
			}
		}
	}
	mustFail(t, evalFixture(t, retries(20, nil)), "publish retries") // 2%
	mustNotFail(t, evalFixture(t, retries(5, nil)), "publish retries")
	mustNotFail(t, evalFixture(t, retries(20, nodeKill())), "publish retries") // REST publishes aimed at the dead node retry
	mustFail(t, evalFixture(t, retries(20, busKill())), "publish retries")
	rec := evalFixture(t, retries(20, nil))
	for _, c := range rec.Checks {
		if c.Name == "publish retries" && !strings.Contains(c.Value, "20 answered 429") {
			t.Fatalf("429s must be printed: %q", c.Value)
		}
	}
	unresolved := func(fault *FaultRecord) func([]*Summary, *RunRecord) {
		return func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[1].Publishes.Unresolved = 3
			} else {
				r.Fault = fault
			}
		}
	}
	mustFail(t, evalFixture(t, unresolved(nil)), "unresolved publishes")
	mustFail(t, evalFixture(t, unresolved(nodeKill())), "unresolved publishes") // a publish is retried until it succeeds: none may be left
	if md := evalFixture(t, unresolved(nil)).Markdown(); !strings.Contains(md, "3 unresolved") {
		t.Fatalf("unresolved must be printed:\n%s", md)
	}
}

func nodeCovRecord(t *testing.T, fault *FaultRecord, mutate func(*RunRecord)) *RunRecord {
	t.Helper()
	return evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.Fault = fault
			r.NodeStats.Coverage = []NodeCoverage{
				{Node: "n1", HasURL: true, Samples: 6, AtStart: true, AtBaseline: true, AtEnd: true},
				{Node: "n2", HasURL: true, Samples: 6, AtStart: true, AtBaseline: true, AtEnd: true},
			}
			r.NodeStats.BaselineDue = true
			if mutate != nil {
				mutate(r)
			}
		}
	})
}

func TestEvaluateNodeMetricsCoverage(t *testing.T) {
	t.Run("every node sampled", func(t *testing.T) {
		mustNotFail(t, nodeCovRecord(t, nil, nil), "node metrics coverage")
	})
	t.Run("a node without samples at the baseline fails", func(t *testing.T) {
		rec := nodeCovRecord(t, nil, func(r *RunRecord) { r.NodeStats.Coverage[1].AtBaseline = false; r.NodeStats.Coverage[1].Errors = 3 })
		mustFail(t, rec, "node metrics coverage")
		for _, c := range rec.Checks {
			if c.Name == "node metrics coverage" && !strings.Contains(c.Value, "1 of 2 nodes sampled") {
				t.Fatalf("%q", c.Value)
			}
		}
	})
	t.Run("a node with no metrics URL fails", func(t *testing.T) {
		mustFail(t, nodeCovRecord(t, nil, func(r *RunRecord) { r.NodeStats.Coverage[1] = NodeCoverage{Node: "n2"} }), "node metrics coverage")
	})
	t.Run("no baseline due in a short hold", func(t *testing.T) {
		mustNotFail(t, nodeCovRecord(t, nil, func(r *RunRecord) {
			r.NodeStats.BaselineDue = false
			r.NodeStats.Coverage[1].AtBaseline = false
		}), "node metrics coverage")
	})
	t.Run("a successful fault run does not gate it", func(t *testing.T) {
		mustNotFail(t, nodeCovRecord(t, nodeKill(), func(r *RunRecord) { r.NodeStats.Coverage[1].AtEnd = false }), "node metrics coverage")
	})
	t.Run("a bus kill does not stop a node's metrics", func(t *testing.T) {
		mustFail(t, nodeCovRecord(t, busKill(), func(r *RunRecord) { r.NodeStats.Coverage[1].AtEnd = false }), "node metrics coverage")
	})
	t.Run("a failed fault hook relaxes nothing and fails the run", func(t *testing.T) {
		rec := nodeCovRecord(t, failedKill(), func(r *RunRecord) { r.NodeStats.Coverage[1].AtEnd = false })
		mustFail(t, rec, "node metrics coverage")
		mustFail(t, rec, "fault injection")
	})
	t.Run("waived", func(t *testing.T) {
		rec := nodeCovRecord(t, nil, func(r *RunRecord) { r.NodeStats.Coverage[1] = NodeCoverage{Node: "n2"}; r.UnmeasuredWaived = true })
		mustNotFail(t, rec, "node metrics coverage")
		if !strings.Contains(rec.Markdown(), "waived with --allow-unmeasured") {
			t.Fatal("a waiver must be visible in the summary")
		}
	})
}

func TestEvaluateFailedFaultHookDoesNotRelaxGrowthOrSteadiness(t *testing.T) {
	growing := func(fault *FaultRecord) *RunRecord {
		return evalFixture(t, func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[0].Connections.OpenAtMeasureStart = 100
				s[0].Attachments.AttachedAtMeasureStart = 150
				s[0].Attachments.AttachedAtMeasureEnd = 180
			} else {
				r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, MemoryGrowth: 0.5}
				r.Fault = fault
			}
		})
	}
	mustFail(t, growing(failedKill()), "node memory growth over hold")
	mustFail(t, growing(failedKill()), "generator load steady over hold")
	rec := growing(nodeKill())
	mustNotFail(t, rec, "node memory growth over hold")
	mustNotFail(t, rec, "generator load steady over hold")
}

func TestComputeNodeCoverage(t *testing.T) {
	inv := &Inventory{Nodes: []InventoryNode{{Name: "n1", Metrics: "u1"}, {Name: "n2", Metrics: "u2"}, {Name: "n3"}}}
	ok := func(node, phase string) NodeSample {
		return NodeSample{Node: node, Phase: phase, Values: map[string]float64{"x": 1}}
	}
	samples := []NodeSample{
		ok("n1", PhaseHoldStart), ok("n1", PhaseBaseline), ok("n1", PhaseHoldEnd), ok("n1", "hold"),
		ok("n2", PhaseHoldStart), {Node: "n2", Phase: PhaseBaseline, Error: "timeout"}, ok("n2", PhaseHoldEnd),
	}
	cov, due := ComputeNodeCoverage(inv, samples, 10_000_000, 2_000_000)
	if !due || len(cov) != 3 {
		t.Fatalf("due %v cov %+v", due, cov)
	}
	if !cov[0].Sampled(due) || cov[0].Samples != 4 {
		t.Errorf("n1 %+v", cov[0])
	}
	if cov[1].Sampled(due) || cov[1].Errors != 1 || !cov[1].Sampled(false) {
		t.Errorf("n2 %+v: the failed baseline scrape must not count, and only matters when a baseline was due", cov[1])
	}
	if cov[2].HasURL || cov[2].Sampled(false) {
		t.Errorf("n3 %+v", cov[2])
	}
	if _, due := ComputeNodeCoverage(inv, samples, 2_500_000, 2_000_000); due {
		t.Error("a baseline less than a second before the end of the hold was not due")
	}
}

func agentCPURecord(t *testing.T, mutate func(*RunRecord)) *RunRecord {
	t.Helper()
	return evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.AgentCPU = []AgentCPU{
				{Agent: "gen-1", Kind: "generator", CPUs: 32, BusyFraction: 0.41, Measured: true},
				{Agent: "gen-2", Kind: "generator", CPUs: 32, BusyFraction: 0.38, Measured: true},
				{Agent: "pub-1", Kind: "publisher", CPUs: 16, BusyFraction: 0.55, Measured: true},
			}
			if mutate != nil {
				mutate(r)
			}
		}
	})
}

func TestEvaluateGeneratorCPU(t *testing.T) {
	rec := agentCPURecord(t, nil)
	mustNotFail(t, rec, "generator CPU")
	mustNotFail(t, rec, "publisher CPU")
	if md := rec.Markdown(); !strings.Contains(md, "| gen-1 | generator | 41% of 32 CPUs |") || !strings.Contains(md, "| pub-1 | publisher |") {
		t.Fatalf("summary.md must print each box's CPU:\n%s", md)
	}
	mustFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[1].BusyFraction = 0.85 }), "generator CPU")
	mustFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[2].BusyFraction = 0.71 }), "publisher CPU")
	mustFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[0].Measured = false }), "generator CPU")                          // one box unread: not a pass
	mustFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[1].BusyFraction = 0.85; r.Fault = nodeKill() }), "generator CPU") // no fault excuses a saturated box
	mustNotFail(t, agentCPURecord(t, func(r *RunRecord) {
		for i := range r.AgentCPU {
			r.AgentCPU[i].Measured = false
		}
		r.UnmeasuredWaived = true
	}), "generator CPU")
	rec = agentCPURecord(t, func(r *RunRecord) {
		for i := range r.AgentCPU {
			r.AgentCPU[i].Measured = false
		}
	})
	mustFail(t, rec, "generator CPU")
	for _, c := range rec.Checks {
		if c.Name == "generator CPU" && c.Value != "not measured" {
			t.Fatalf("%q", c.Value)
		}
	}
}

const nodeMetricsText = `# HELP ably_publish_lanes Publish batching lanes
# TYPE ably_publish_lanes gauge
ably_publish_lanes 2
ably_publish_linger_max_seconds 0.005
ably_publish_linger_min_seconds 0
ably_storage_shards 1
ably_bus_info{bus="nats",mode="notify"} 1
process_resident_memory_bytes 1.5e+09
ably_connections_open 100
`

func TestParseMetricLabels(t *testing.T) {
	got, err := ParseMetricLabels(strings.NewReader(nodeMetricsText+"ably_bus_info{bus=\"a\\\"b\",mode=\"\"} 1\nother{x=\"1\"} 2\n"), "ably_bus_info")
	if err != nil || len(got) != 2 {
		t.Fatalf("%v %v", got, err)
	}
	if got[0]["bus"] != "nats" || got[0]["mode"] != "notify" || got[1]["bus"] != `a"b` || got[1]["mode"] != "" {
		t.Fatalf("%v", got)
	}
}

func TestScrapeNodeReadsServerConfiguration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(nodeMetricsText)) }))
	defer srv.Close()
	s := scrapeNode(context.Background(), srv.Client(), "n1", srv.URL, PhaseHoldStart)
	if s.Error != "" || s.Values[metricPublishLanes] != 2 || s.Info["bus"] != "nats" || s.Info["mode"] != "notify" {
		t.Fatalf("%+v", s)
	}
	cfgs := ComputeServerConfig([]NodeSample{s})
	if len(cfgs) != 1 || flagString(cfgs[0].Flags) != "publish-lanes=2 publish-linger-max=5ms publish-linger-min=0s bus=nats bus-notify-mode=notify storage-shards=1" {
		t.Fatalf("%+v: %q", cfgs, flagString(cfgs[0].Flags))
	}
}

func cfgNode(node, lanes string) NodeConfig {
	return NodeConfig{Node: node, Flags: map[string]string{FlagPublishLanes: lanes, FlagBus: "nats", FlagStorageShards: "1"}}
}

func TestEvaluateServerConfiguration(t *testing.T) {
	with := func(cfgs ...NodeConfig) *RunRecord {
		return evalFixture(t, func(s []*Summary, r *RunRecord) {
			if s == nil {
				r.ServerConfig = cfgs
				r.Bus, r.Shards = "nats", 1
			}
		})
	}
	rec := with(cfgNode("n1", "2"), cfgNode("n2", "2"), cfgNode("n3", "2"))
	mustNotFail(t, rec, "server configuration identical on every node")
	md := rec.Markdown()
	if !strings.Contains(md, "Server flags in effect, read from the nodes' /metrics (identical on 3 of 3 nodes): publish-lanes=2 bus=nats storage-shards=1 (bus sweep interval: not exported by the nodes).") {
		t.Fatalf("summary.md must print the flags the nodes ran with:\n%s", md)
	}
	mixed := with(cfgNode("n1", "2"), cfgNode("n2", "2"), cfgNode("n3", "4"))
	mustFail(t, mixed, "server configuration identical on every node")
	if md := mixed.Markdown(); !strings.Contains(md, "THE NODES DISAGREE") || !strings.Contains(md, "- n3: publish-lanes=4") {
		t.Fatalf("%s", md)
	}
	// The record's shard count must be the nodes'.
	rec = evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.ServerConfig = []NodeConfig{cfgNode("n1", "2")}
			r.Shards = 3
		}
	})
	mustFail(t, rec, "recorded shard count matches the nodes")
	// A bus name that differs is reported, not gated.
	rec = evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.ServerConfig = []NodeConfig{cfgNode("n1", "2")}
			r.Bus = "pgnotify"
		}
	})
	mustNotFail(t, rec, "recorded bus matches the nodes")
	// Memory-mode nodes export none of it: reported as such.
	rec = with(NodeConfig{Node: "n1", Flags: map[string]string{}})
	mustNotFail(t, rec, "server configuration")
	if !strings.Contains(rec.Markdown(), "none reported") {
		t.Fatal(rec.Markdown())
	}
}

func TestEvaluateDeliveryGateIs99PercentEvenInAFaultRun(t *testing.T) {
	// 94% of the plan's 90/s: the old 90% gate passed this. No fault relaxes
	// the gate.
	short := func(fault *FaultRecord) func([]*Summary, *RunRecord) {
		return func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[0].Deliveries.Rate = 84.6
			} else {
				r.Fault = fault
			}
		}
	}
	for _, fault := range []*FaultRecord{nil, nodeKill(), busKill()} {
		mustFail(t, evalFixture(t, short(fault)), "deliveries vs plan")
	}
}

func TestEvaluateStatesWhatCoversUnsampledChannels(t *testing.T) {
	rec := evalFixture(t, nil)
	want := "Unsampled channels are covered only by the deliveries-vs-plan gate (>= 99% of planned deliveries/s); loss below 1% there is not detected."
	if !strings.Contains(rec.Markdown(), want) {
		t.Fatalf("summary.md lacks the unsampled-channel statement:\n%s", rec.Markdown())
	}
}

func TestEvaluateFaultRelaxationTable(t *testing.T) {
	// For each kind of fault, each gate it invalidates is relaxed (reported,
	// not gating) and every other stays gating: the run is built to fail
	// every gate in the table at once.
	breakAll := func(kind *FaultRecord) *RunRecord {
		return evalFixture(t, func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[0].Latency[LatDeliveryCrossNode] = histOf(append(make([]int64, 98), 300000, 300000)...) // delivery p99
				s[0].Latency[LatConnectAttach] = histOf(900000)
				s[0].Connections.OpenAtMeasureStart, s[0].Attachments.AttachedAtMeasureStart, s[0].Attachments.AttachedAtMeasureEnd = 100, 150, 180
				s[1].Publishes.Sent, s[1].Publishes.Retries = 1000, 50
				s[1].Publishes.Unresolved = 2
			} else {
				r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, MemoryGrowth: 0.5, GoroutineGrowth: 0.5, BaselineDue: true,
					Coverage: []NodeCoverage{{Node: "n1", HasURL: true, AtStart: true, AtBaseline: true}}}
				r.Fault = kind
			}
		})
	}
	gated := func(rec *RunRecord) map[string]bool {
		out := map[string]bool{}
		for _, c := range rec.Checks {
			if !c.Pass && c.Gating {
				out[c.Name] = true
			}
		}
		return out
	}
	names := []string{"delivery p99 (cross-node)", "connect+attach p99", "generator load steady over hold", "node memory growth over hold",
		"node goroutine growth over hold", "node metrics coverage", "publish retries",
		"unresolved publishes"} // the last is relaxed by no kind
	for _, c := range []struct {
		fault   *FaultRecord
		relaxed []string
	}{
		{nil, nil},
		{nodeKill(), []string{"connect+attach p99", "generator load steady over hold", "node memory growth over hold", "node goroutine growth over hold", "node metrics coverage", "publish retries"}},
		{busKill(), []string{"delivery p99 (cross-node)"}},
		{&FaultRecord{Command: "x", Kind: FaultOther}, nil},
	} {
		name := "no fault"
		if c.fault != nil {
			name = c.fault.Kind
		}
		t.Run(name, func(t *testing.T) {
			got := gated(breakAll(c.fault))
			for _, n := range names {
				want := !slices.Contains(c.relaxed, n)
				if got[n] != want {
					t.Errorf("%q: gating=%v, want %v (relaxed set %v)", n, got[n], want, c.relaxed)
				}
			}
		})
	}
}

func TestFaultRelaxationsAreWhatTheREADMEsTableSays(t *testing.T) {
	b, err := os.ReadFile("../../bench/aws/README.md")
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		for _, kind := range FaultKinds() {
			if strings.HasPrefix(line, "| `"+kind+"` |") {
				rows[kind] = line
			}
		}
	}
	for _, kind := range FaultKinds() {
		row, ok := rows[kind]
		if !ok {
			t.Errorf("README.md has no relaxation table row for fault kind %q", kind)
			continue
		}
		for key, check := range RelaxationCheck {
			has := strings.Contains(row, check)
			want := slices.Contains(FaultRelaxations[kind], key)
			if has != want {
				t.Errorf("README row for %s: mentions %q = %v, code relaxes it = %v\n%s", kind, check, has, want, row)
			}
		}
	}
}

func TestFailedOrMissingFaultMakesTheVerdictInvalid(t *testing.T) {
	for _, f := range []*FaultRecord{failedKill(), {Command: "kill", Kind: FaultNodeKill, ExitCode: -1, Output: "the fault hook did not run before the end of the run"}} {
		// Gates failing too (slow p50): the verdict is still INVALID, not FAIL.
		rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
			if s != nil {
				s[0].Latency[LatDeliveryCrossNode] = histOf(60000, 70000, 80000)
			} else {
				r.Fault = f
			}
		})
		if rec.Verdict != VerdictInvalidFault || rec.Pass {
			t.Fatalf("verdict %q pass %v for %+v", rec.Verdict, rec.Pass, f)
		}
		mustFail(t, rec, "fault injection")
		if !strings.Contains(rec.Markdown(), "INVALID (fault not injected)") {
			t.Fatal("the verdict must be in the title of summary.md")
		}
	}
	if rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.Fault = nodeKill()
		}
	}); rec.Verdict != "PASS" {
		t.Fatalf("a fault that ran and passed every gate that still applies: %q (%v)", rec.Verdict, failing(rec))
	}
}

func TestPresenceOnlyRunSaysTheMessageRowIsNotApplicable(t *testing.T) {
	rec := presenceRecord(nil)
	var row *Check
	for i, c := range rec.Checks {
		if c.Name == "loss, duplicate, reorder on the sample" {
			row = &rec.Checks[i]
		}
	}
	if row == nil || row.Value != "not applicable (no sampled message streams)" || row.Gating {
		t.Fatalf("row %+v", row)
	}
	if strings.Contains(rec.Markdown(), "0 of 0 checked") {
		t.Fatalf("a presence run must not print '0 of 0 checked':\n%s", rec.Markdown())
	}
	// Its correctness gates are the presence rows.
	for _, want := range []string{"sample coverage (presence)", "presence correctness"} {
		found := false
		for _, c := range rec.Checks {
			found = found || (c.Name == want && c.Gating)
		}
		if !found {
			t.Fatalf("no gating %q row", want)
		}
	}
	// A message plan with sampled streams keeps the real row.
	rec = evalFixture(t, nil)
	for _, c := range rec.Checks {
		if c.Name == "loss, duplicate, reorder on the sample" && (!c.Gating || strings.HasPrefix(c.Value, "not applicable")) {
			t.Fatalf("%+v", c)
		}
	}
}
