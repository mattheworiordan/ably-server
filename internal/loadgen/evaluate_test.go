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
		},
	}
	pub := &Summary{
		Role: RoleREST, Index: 0, Count: 1,
		Publishes: PublishStats{TargetRate: 100, OfferedRate: 100, AchievedRate: 99, Offered: 1000, Acked: 990},
		Latency:   map[string]*Histogram{LatRESTAck: histOf(5000, 6000, 7000)},
		Streams:   map[string]map[string]StreamRecord{"ch": {"p": {LastAckedSeq: 9, LastAckedUS: 5_000_000}}},
	}
	return []*Summary{sub, pub}
}

func evalFixture(t *testing.T, mutate func(sums []*Summary, rec *RunRecord)) *RunRecord {
	t.Helper()
	sums := fixtureSummaries()
	rec := &RunRecord{Plan: Totals{Connections: 100, PublishesPerSec: 100, DeliveriesPerSec: 90, SampledChannels: 1}}
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
				r.NodeStats = NodeStats{Measured: true, MemoryGrowth: 0.2}
			}
		}, "node memory growth"},
		{"goroutine growth", func(s []*Summary, r *RunRecord) {
			if s == nil {
				r.NodeStats = NodeStats{Measured: true, GoroutineGrowth: 0.5}
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
	ns := ComputeNodeStats(samples, 5_000_000, 25_000_000)
	if !ns.Measured || ns.MemoryGrowth < 0.299 || ns.MemoryGrowth > 0.301 || ns.GoroutineGrowth < 0.049 || ns.GoroutineGrowth > 0.051 {
		t.Fatalf("stats %+v", ns)
	}
	if ns.CoresUsed != 3 || ns.RSSBytes != 340 || ns.ConnectionsOpen != 20 {
		t.Fatalf("use %+v", ns)
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
			r.NodeStats = NodeStats{Measured: true, MemoryGrowth: 1.4, GoroutineGrowth: 1.1}
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
