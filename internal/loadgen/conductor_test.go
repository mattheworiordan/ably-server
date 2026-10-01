package loadgen_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/loadgen"
)

const conductorScenario = `
name = "C"
shape = "D"
message_bytes = 200
sample_percent = 50

[timing]
ramp = "1s"
hold = "4s"
drain = "1s"

[connections]
count = 20

[churn]
connects_per_sec = 2
channel_opens_per_sec = 2
resume = true

[[class]]
name = "hot"
channels = 1
subscribers = 4
publish_rate = 20
streams = 4
scale_by = "rate"

[[class]]
name = "live"
channels = 30
subscribers = 1
publish_rate_total = 60

[[class]]
name = "dark"
channels = 30
subscribers = 0
publish_rate_total = 30

# The test checks the pipeline, not the speed of a shared laptop running
# the race detector: generous limits, strict correctness.
[pass]
delivery_p50 = "10s"
delivery_p99 = "20s"
rest_ack_p99 = "20s"
connect_attach_p99 = "20s"
min_achieved_ratio = 0.5
max_memory_growth = 10.0
max_goroutine_growth = 10.0
`

// startFakeNTP serves SNTP on loopback from a clock 2 ms ahead of this one.
func startFakeNTP(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 64)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 48 {
				continue
			}
			now := time.Now().Add(2 * time.Millisecond)
			sec := uint32(now.Unix() + 2208988800)
			frac := uint32(uint64(now.Nanosecond()) << 32 / 1_000_000_000)
			var resp [48]byte
			resp[0], resp[1] = 0x24, 2
			copy(resp[24:32], buf[40:48])
			for _, off := range []int{32, 40} {
				binary.BigEndian.PutUint32(resp[off:], sec)
				binary.BigEndian.PutUint32(resp[off+4:], frac)
			}
			_, _ = pc.WriteTo(resp[:], addr)
		}
	}()
	return pc.LocalAddr().String()
}

func TestConductorRunsAScenarioEndToEnd(t *testing.T) {
	addr, metricsURL := startServerWithMetrics(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agents []loadgen.InventoryAgent
	for i, roles := range [][]string{
		{loadgen.RoleSubscriber},
		{loadgen.RoleSubscriber, loadgen.RoleREST},
	} {
		a := loadgen.NewAgent(ctx, nil)
		if i == 0 {
			a.NTPServer = startFakeNTP(t) // the second agent has none: recorded as not measured
		}
		srv := httptest.NewServer(a.Handler())
		t.Cleanup(srv.Close)
		agents = append(agents, loadgen.InventoryAgent{Name: "a" + string(rune('1'+i)), URL: srv.URL, Roles: roles, Workers: 8})
	}
	inv := &loadgen.Inventory{
		Key:         testKey,
		Environment: map[string]string{"bus": "memory", "storage": "none"},
		Nodes: []loadgen.InventoryNode{
			{Name: "n1", Endpoint: addr, Metrics: metricsURL, VCPU: 2, MemoryGB: 4},
			{Name: "n2", Endpoint: addr, VCPU: 2, MemoryGB: 4},
		},
		Agents: agents,
	}
	sc, err := loadgen.ParseScenario([]byte(conductorScenario))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	logFile := filepath.Join(dir, "LOG.md")
	stateFile := filepath.Join(dir, "STATE.json")
	if err := os.WriteFile(stateFile, []byte(`{"resources":[],"images":{},"runs":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	rec, err := loadgen.RunConductor(ctx, loadgen.ConductorConfig{
		Scenario: sc, RunID: "c1", RunTag: "c1", Inventory: inv, ResultsDir: dir,
		LogFile: logFile, StateFile: stateFile, StartDelay: time.Second, Poll: 500 * time.Millisecond,
		FaultHook: "echo injected", FaultAt: time.Second, Out: &out,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Pass {
		t.Fatalf("verdict %s\n%s\nconductor output:\n%s", rec.Verdict, rec.Markdown(), out.String())
	}
	if rec.Fault == nil || rec.Fault.ExitCode != 0 || !strings.Contains(rec.Fault.Output, "injected") {
		t.Errorf("fault %+v", rec.Fault)
	}
	if !rec.NodeStats.Measured {
		t.Error("node metrics not sampled over the hold")
	}
	if rec.Footprint.VCPU != 4 {
		t.Errorf("footprint %+v", rec.Footprint)
	}
	// Node coverage: n1 has a metrics URL and was scraped at the fixed
	// points; n2 has none and so counts as not sampled. (A fault run does
	// not gate on it.)
	if cov := rec.NodeStats.Coverage; len(cov) != 2 || !cov[0].Sampled(rec.NodeStats.BaselineDue) || cov[1].HasURL || cov[1].Sampled(false) {
		t.Errorf("node coverage %+v", rec.NodeStats.Coverage)
	}
	if len(rec.AgentCPU) != 2 || rec.AgentCPU[0].Kind != "generator" {
		t.Errorf("agent CPU records %+v", rec.AgentCPU)
	}
	if len(rec.Clocks) != 2 {
		t.Fatalf("clock records %+v", rec.Clocks)
	}
	for _, c := range rec.Clocks {
		switch c.Agent {
		case "a1":
			if c.Start == nil || c.End == nil || c.Start.OffsetUS > -1000 || c.Start.OffsetUS < -4000 {
				t.Errorf("a1 clock %+v, want offsets of about -2000 us at start and end", c)
			}
		case "a2":
			if c.Start != nil || c.End != nil || c.Error == "" {
				t.Errorf("a2 has no NTP server and must be recorded as not measured: %+v", c)
			}
		}
	}
	if !strings.Contains(rec.Markdown(), "Generator box") {
		t.Error("summary.md lacks the clock table")
	}
	if rec.Result.CheckedMessages == 0 || rec.Result.Tail.Checked == 0 {
		t.Errorf("correctness not exercised: checked=%d tail=%+v", rec.Result.CheckedMessages, rec.Result.Tail)
	}
	for _, f := range []string{"summary.json", "summary.md", "plan.json", "agents/c1-subscriber-0.json", "agents/c1-subscriber-1.json", "agents/c1-rest-publisher-0.json"} {
		if _, err := os.Stat(filepath.Join(dir, "c1", f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
	plan, _ := os.ReadFile(filepath.Join(dir, "c1", "plan.json"))
	if strings.Contains(string(plan), "secret") {
		t.Error("plan.json carries the API key")
	}
	logb, _ := os.ReadFile(logFile)
	if !strings.Contains(string(logb), "run c1 shape D") || !strings.Contains(string(logb), "PASS") {
		t.Errorf("LOG line: %q", logb)
	}
	var state map[string]any
	b, _ := os.ReadFile(stateFile)
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatal(err)
	}
	if runs, _ := state["runs"].([]any); len(runs) != 1 {
		t.Errorf("STATE.json runs = %v", state["runs"])
	}
	if _, ok := state["images"]; !ok {
		t.Error("STATE.json lost its other keys")
	}
	// Re-evaluating from the saved summaries gives the same verdict.
	again, err := loadgen.EvaluateRunDir(filepath.Join(dir, "c1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.Verdict != rec.Verdict || again.Result.Deliveries.Received != rec.Result.Deliveries.Received || again.Footprint.VCPU != 4 {
		t.Errorf("re-evaluation differs: %s vs %s", again.Verdict, rec.Verdict)
	}
	recs, err := loadgen.LoadRunRecords([]string{filepath.Join(dir, "c1", "summary.json")})
	if err != nil || len(recs) != 1 {
		t.Fatalf("load: %v", err)
	}
	if md := loadgen.Report(recs); !strings.Contains(md, "D 1x, bus memory, 2 nodes | 1 of 1") {
		t.Errorf("report:\n%s", md)
	}
}

func TestConductorTimeLimitStopsAgents(t *testing.T) {
	addr := startServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := loadgen.NewAgent(ctx, nil)
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	sc, err := loadgen.ParseScenario([]byte(conductorScenario))
	if err != nil {
		t.Fatal(err)
	}
	sc.Timing.Hold.Duration = time.Minute
	sc.Pass = loadgen.PassSpec{}
	dir := t.TempDir()
	marker := filepath.Join(dir, "timeout-ran")
	start := time.Now()
	rec, err := loadgen.RunConductor(ctx, loadgen.ConductorConfig{
		Scenario: sc, RunID: "tl", RunTag: "tl", ResultsDir: dir, StartDelay: 500 * time.Millisecond, Poll: 200 * time.Millisecond,
		TimeLimit: 3 * time.Second, OnTimeout: "touch " + marker,
		Inventory: &loadgen.Inventory{Key: testKey, Nodes: []loadgen.InventoryNode{{Name: "n", Endpoint: addr}},
			Agents: []loadgen.InventoryAgent{{Name: "a", URL: srv.URL, Roles: []string{loadgen.RoleSubscriber, loadgen.RoleREST}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Verdict != "ABORTED" || rec.Pass {
		t.Fatalf("verdict %s", rec.Verdict)
	}
	if time.Since(start) > 30*time.Second {
		t.Fatalf("time limit not enforced: took %s", time.Since(start))
	}
	if _, err := os.Stat(marker); err != nil {
		t.Error("on-timeout hook did not run")
	}
}

// A fault hook that is configured but never runs (the run ends first) is
// recorded as a failed fault and fails the run: it must not read as a
// fault-free pass.
func TestConductorFaultThatNeverRanFailsTheRun(t *testing.T) {
	addr, metricsURL := startServerWithMetrics(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := loadgen.NewAgent(ctx, nil)
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	inv := &loadgen.Inventory{
		Key:    testKey,
		Nodes:  []loadgen.InventoryNode{{Name: "n1", Endpoint: addr, Metrics: metricsURL}},
		Agents: []loadgen.InventoryAgent{{Name: "a1", URL: srv.URL, Roles: []string{loadgen.RoleSubscriber, loadgen.RoleREST}, Workers: 8}},
	}
	sc, err := loadgen.ParseScenario([]byte(strings.Replace(conductorScenario, `hold = "4s"`, `hold = "2s"`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := loadgen.RunConductor(ctx, loadgen.ConductorConfig{
		Scenario: sc, RunID: "nf", RunTag: "nf", Inventory: inv, ResultsDir: t.TempDir(),
		StartDelay: time.Second, Poll: 500 * time.Millisecond,
		FaultHook: "echo never", FaultAt: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Pass || rec.Fault == nil || rec.Fault.ExitCode == 0 {
		t.Fatalf("pass=%v fault=%+v\n%s", rec.Pass, rec.Fault, rec.Markdown())
	}
	found := false
	for _, c := range rec.Checks {
		found = found || (c.Name == "fault injection" && !c.Pass && c.Gating)
	}
	if !found {
		t.Fatalf("no failing fault injection check:\n%s", rec.Markdown())
	}
}
