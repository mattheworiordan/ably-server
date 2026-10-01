package loadgen

import (
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
		}, "attach-point check coverage"},
		{"attach-point check cannot settle claims", func(s []*Summary, _ *RunRecord) {
			if s != nil {
				s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 9, LastAckedUS: 5_000_000} // no serial log
			}
		}, "attach-point check coverage"},
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

func TestEvaluateFaultRunReportsGrowthWithoutGating(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s == nil {
			r.NodeStats = NodeStats{Measured: true, GrowthMeasured: true, MemoryGrowth: 1.4, GoroutineGrowth: 1.1}
			r.Fault = &FaultRecord{Command: "kill node2"}
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

func TestEvaluateAttachCoverageStandsDownInAFaultRun(t *testing.T) {
	rec := evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s != nil {
			s[0].Correctness.AttachClaims = nil
		} else {
			r.Fault = &FaultRecord{Command: "kill", ExitCode: 0}
		}
	})
	for _, n := range failing(rec) {
		if strings.HasPrefix(n, "attach-point") {
			t.Fatalf("a successful fault run does not gate attach coverage: %v", failing(rec))
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
	// A run whose fault ran reports it without gating.
	rec = evalFixture(t, func(s []*Summary, r *RunRecord) {
		if s != nil {
			s[1].Streams["ch"]["p"] = StreamRecord{LastAckedSeq: 9, LastAckedUS: 1_500_000, Serials: fixtureSerials(10)}
		} else {
			r.Fault = &FaultRecord{Command: "kill", ExitCode: 0}
		}
	})
	for _, n := range failing(rec) {
		if n == "tail check coverage" {
			t.Fatalf("a successful fault run does not gate tail coverage: %v", failing(rec))
		}
	}
	if !strings.Contains(rec.Markdown(), "1 (subscriber, stream) pairs skipped") {
		t.Fatalf("skipped pairs and the reason must be printed:\n%s", rec.Markdown())
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

func TestEvaluatePresenceNacksAreToleratedOnlyInASuccessfulFaultRun(t *testing.T) {
	nacked := func(fault *FaultRecord) func(*RunRecord) {
		return func(r *RunRecord) { r.Result.Presence.Nacks = 3; r.Fault = fault }
	}
	if rec := presenceRecord(nacked(&FaultRecord{Command: "kill", ExitCode: 0})); !rec.Pass {
		t.Fatalf("nacks in a fault run: %v", failing(rec))
	}
	if rec := presenceRecord(nacked(&FaultRecord{Command: "kill", ExitCode: 1})); rec.Pass {
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
	mustNotFail(t, evalFixture(t, retries(20, &FaultRecord{Command: "kill"})), "publish retries")
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
	mustNotFail(t, evalFixture(t, unresolved(&FaultRecord{Command: "kill"})), "unresolved publishes")
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
		mustNotFail(t, nodeCovRecord(t, &FaultRecord{Command: "kill"}, func(r *RunRecord) { r.NodeStats.Coverage[1].AtEnd = false }), "node metrics coverage")
	})
	t.Run("a failed fault hook relaxes nothing and fails the run", func(t *testing.T) {
		rec := nodeCovRecord(t, &FaultRecord{Command: "kill", ExitCode: 1, Output: "no such node"}, func(r *RunRecord) { r.NodeStats.Coverage[1].AtEnd = false })
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
	mustFail(t, growing(&FaultRecord{Command: "kill", ExitCode: 3}), "node memory growth over hold")
	mustFail(t, growing(&FaultRecord{Command: "kill", ExitCode: 3}), "generator load steady over hold")
	rec := growing(&FaultRecord{Command: "kill"})
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
	mustFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[0].Measured = false }), "generator CPU") // one box unread: not a pass
	mustNotFail(t, agentCPURecord(t, func(r *RunRecord) { r.AgentCPU[1].BusyFraction = 0.85; r.Fault = &FaultRecord{Command: "kill"} }), "generator CPU")
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
