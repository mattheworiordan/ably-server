package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConductorConfig is one run's inputs (plan §7: the conductor reads a
// scenario, assigns work, ramps, holds, injects failures, collects and
// evaluates).
type ConductorConfig struct {
	Scenario   *Scenario
	Multiplier float64
	Scale      float64
	RunID      string
	RunTag     string
	Inventory  *Inventory
	// ResultsDir is the results root; the run writes ResultsDir/RunID/.
	ResultsDir string
	// LogFile, if set, gets the run's one-line summary appended (LOG.md).
	LogFile string
	// StateFile, if set, gets the run appended to its "runs" array
	// (STATE.json).
	StateFile string
	// StartDelay is the gap between sending the jobs and the ramp start,
	// so every agent has its job before it begins (default 10s).
	StartDelay time.Duration
	// Poll is the status and node-metrics interval (default 10s).
	Poll time.Duration
	// FaultHook, if set, is run with sh -c at FaultAt into the hold.
	FaultHook string
	FaultAt   time.Duration
	// TimeLimit caps the whole run (default: its planned length plus 5
	// minutes). On expiry every agent is stopped and OnTimeout runs.
	TimeLimit time.Duration
	OnTimeout string
	// Format is the realtime wire format for the jobs.
	Format string
	// Out receives progress lines.
	Out io.Writer
	// HTTP is the client for agents and node metrics (default: 10s timeout).
	HTTP *http.Client
}

// RolesNeeded lists the roles a plan needs, in a stable order.
func RolesNeeded(p *Plan) []string {
	var roles []string
	if p.Connections > 0 && p.Attachments > 0 {
		roles = append(roles, RoleSubscriber)
	}
	var rest, rt bool
	for _, c := range p.Classes {
		if c.RatePerChannel <= 0 {
			continue
		}
		if c.Publisher == "realtime" {
			rt = true
		} else {
			rest = true
		}
	}
	if rest {
		roles = append(roles, RoleREST)
	}
	if rt {
		roles = append(roles, RoleRealtime)
	}
	if p.Presence.Enabled {
		roles = append(roles, RolePresence)
	}
	return roles
}

type plannedJob struct {
	agent InventoryAgent
	spec  JobSpec
}

// BuildJobs assigns every needed role to the inventory's agents: the k
// agents that take a role run jobs index 0..k-1 of count k.
func BuildJobs(cfg *ConductorConfig, plan *Plan, startAt time.Time) ([]plannedJob, error) {
	key := cfg.Inventory.Key
	if key == "" {
		key = strings.Split(os.Getenv("ABLY_SERVER_KEYS"), ",")[0]
	}
	if key == "" {
		return nil, fmt.Errorf("no API key: set inventory key or ABLY_SERVER_KEYS")
	}
	var jobs []plannedJob
	for _, role := range RolesNeeded(plan) {
		agents := cfg.Inventory.agentsFor(role)
		if len(agents) == 0 {
			return nil, fmt.Errorf("the plan needs role %s but no inventory agent takes it", role)
		}
		for i, a := range agents {
			jobs = append(jobs, plannedJob{agent: a, spec: JobSpec{
				ID: fmt.Sprintf("%s-%s-%d", cfg.RunID, role, i), RunID: cfg.RunID, RunTag: cfg.RunTag,
				Scenario: *cfg.Scenario, Multiplier: plan.Multiplier, Scale: plan.Scale,
				Role: role, Index: i, Count: len(agents),
				Endpoints: cfg.Inventory.Endpoints(), Key: key, StartAtUS: startAt.UnixMicro(),
				Workers: a.Workers, RealtimeConns: a.RealtimeConns, Format: cfg.Format,
			}})
		}
	}
	return jobs, nil
}

func (cfg *ConductorConfig) logf(format string, args ...any) {
	if cfg.Out != nil {
		fmt.Fprintf(cfg.Out, "%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
	}
}

func agentCall(ctx context.Context, client *http.Client, method, url string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(b, out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func runHook(ctx context.Context, command string, env []string) (int, string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		code = -1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	s := string(out)
	if len(s) > 4000 {
		s = s[:4000] + "..."
	}
	return code, s
}

// RunConductor runs a scenario end to end against the inventory and
// returns the evaluated run record, also written to
// ResultsDir/RunID/{summary.json,summary.md,plan.json,agents/*.json}.
func RunConductor(ctx context.Context, cfg ConductorConfig) (*RunRecord, error) {
	if cfg.StartDelay <= 0 {
		cfg.StartDelay = 10 * time.Second
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 10 * time.Second
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	plan, err := cfg.Scenario.Resolve(cfg.Multiplier, cfg.Scale, cfg.RunTag)
	if err != nil {
		return nil, err
	}
	totals, classTotals := plan.Totals()
	runDir := filepath.Join(cfg.ResultsDir, cfg.RunID)
	if err := os.MkdirAll(filepath.Join(runDir, "agents"), 0o755); err != nil {
		return nil, err
	}
	t := cfg.Scenario.Timing
	startAt := time.Now().Add(cfg.StartDelay)
	measureStart := startAt.Add(t.Ramp.Duration)
	measureEnd := measureStart.Add(t.Hold.Duration)
	end := measureEnd.Add(t.Drain.Duration)
	limit := cfg.TimeLimit
	if limit <= 0 {
		limit = time.Until(end) + 5*time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()

	jobs, err := BuildJobs(&cfg, plan, startAt)
	if err != nil {
		return nil, err
	}
	redacted := *cfg.Inventory
	redacted.Key = ""
	_ = WriteJSONFile(filepath.Join(runDir, "plan.json"), map[string]any{
		"run_id": cfg.RunID, "run_tag": cfg.RunTag, "scenario": cfg.Scenario, "multiplier": plan.Multiplier,
		"scale": plan.Scale, "totals": totals, "classes": classTotals, "inventory": &redacted,
		"start": startAt.UTC(), "measure_start": measureStart.UTC(), "measure_end": measureEnd.UTC(), "end": end.UTC(),
	})
	cfg.logf("run %s: shape %s %gx scale %g, %d connections, %.0f publishes/s, %.0f deliveries/s; %d jobs; ramp at %s",
		cfg.RunID, plan.Scenario.Shape, plan.Multiplier, plan.Scale, totals.Connections, totals.PublishesPerSec, totals.DeliveriesPerSec, len(jobs), startAt.UTC().Format(time.RFC3339))

	stopAll := func() {
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		seen := map[string]bool{}
		for _, j := range jobs {
			if !seen[j.agent.URL] {
				seen[j.agent.URL] = true
				_, _ = agentCall(sctx, cfg.HTTP, http.MethodPost, strings.TrimRight(j.agent.URL, "/")+"/v1/stop", nil, nil)
			}
		}
	}
	for _, j := range jobs {
		if _, err := agentCall(runCtx, cfg.HTTP, http.MethodPost, strings.TrimRight(j.agent.URL, "/")+"/v1/jobs", j.spec, nil); err != nil {
			stopAll()
			return nil, fmt.Errorf("start job %s on %s: %w", j.spec.ID, j.agent.Name, err)
		}
	}
	if time.Now().After(startAt) {
		cfg.logf("warning: jobs were sent after the ramp start; raise --start-delay")
	}

	// Node metrics and progress, until the end of the drain.
	var samplesMu sync.Mutex
	var samples []NodeSample
	phase := func(now time.Time) string {
		switch {
		case now.Before(startAt):
			return "setup"
		case now.Before(measureStart):
			return "ramp"
		case now.Before(measureEnd):
			return "hold"
		default:
			return "drain"
		}
	}
	scrape := func() {
		ph := phase(time.Now())
		var wg sync.WaitGroup
		for _, n := range cfg.Inventory.Nodes {
			if n.Metrics == "" {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := scrapeNode(runCtx, cfg.HTTP, n.Name, n.Metrics, ph)
				samplesMu.Lock()
				samples = append(samples, s)
				samplesMu.Unlock()
			}()
		}
		wg.Wait()
	}
	progress := func() {
		var conns, att, acked, recv, viol int64
		for _, j := range jobs {
			var st JobStatus
			if _, err := agentCall(runCtx, cfg.HTTP, http.MethodGet, strings.TrimRight(j.agent.URL, "/")+"/v1/jobs/"+j.spec.ID, nil, &st); err == nil {
				conns += st.Connections
				att += st.Attached
				acked += st.Acked
				recv += st.Received
				viol += st.Violations
			}
		}
		cfg.logf("%s: connections %d, attached %d, acked %d, received %d, violations %d", phase(time.Now()), conns, att, acked, recv, viol)
	}

	var fault *FaultRecord
	faultTimer := make(<-chan time.Time)
	if cfg.FaultHook != "" {
		faultTimer = time.After(time.Until(measureStart.Add(cfg.FaultAt)))
	}
	// Extra scrapes just inside the hold bound the growth estimate.
	holdStart := time.After(time.Until(measureStart.Add(2 * time.Second)))
	holdEnd := time.After(time.Until(measureEnd.Add(-2 * time.Second)))
	tick := time.NewTicker(cfg.Poll)
	defer tick.Stop()
	timedOut := false
loop:
	for {
		select {
		case <-runCtx.Done():
			timedOut = ctx.Err() == nil
			break loop
		case <-tick.C:
			scrape()
			progress()
			if time.Now().After(end) {
				break loop
			}
		case <-holdStart:
			scrape()
		case <-holdEnd:
			scrape()
		case <-faultTimer:
			at := time.Now()
			cfg.logf("fault: running %q", cfg.FaultHook)
			code, out := runHook(runCtx, cfg.FaultHook, []string{"RUN_ID=" + cfg.RunID})
			fault = &FaultRecord{Command: cfg.FaultHook, AtUS: at.UnixMicro(), ExitCode: code, Output: out}
			cfg.logf("fault: exit %d", code)
		}
	}
	if timedOut {
		cfg.logf("time limit %s reached: stopping every agent", limit)
		stopAll()
		if cfg.OnTimeout != "" {
			code, out := runHook(context.Background(), cfg.OnTimeout, []string{"RUN_ID=" + cfg.RunID})
			cfg.logf("on-timeout hook exit %d: %s", code, strings.TrimSpace(out))
		}
	} else if ctx.Err() != nil {
		cfg.logf("interrupted: stopping every agent")
		stopAll()
	}

	// Collect summaries.
	collectCtx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer ccancel()
	sums := make([]*Summary, len(jobs))
	refs := make([]JobRef, len(jobs))
	var errs []string
	for i, j := range jobs {
		refs[i] = JobRef{ID: j.spec.ID, Role: j.spec.Role, Agent: j.agent.Name}
		url := strings.TrimRight(j.agent.URL, "/") + "/v1/jobs/" + j.spec.ID + "/summary"
		for {
			var s Summary
			code, err := agentCall(collectCtx, cfg.HTTP, http.MethodGet, url, nil, &s)
			if err == nil && code == http.StatusOK {
				sums[i] = &s
				refs[i].Host = s.Host
				_ = WriteJSONFile(filepath.Join(runDir, "agents", j.spec.ID+".json"), &s)
				break
			}
			if collectCtx.Err() != nil {
				errs = append(errs, fmt.Sprintf("no summary from %s (%s): %v", j.spec.ID, j.agent.Name, err))
				break
			}
			time.Sleep(time.Second)
		}
	}

	samplesMu.Lock()
	allSamples := append([]NodeSample(nil), samples...)
	samplesMu.Unlock()
	rec := &RunRecord{
		Version: RunRecordVersion, RunID: cfg.RunID, RunTag: cfg.RunTag,
		Scenario: cfg.Scenario.Name, Shape: cfg.Scenario.Shape, Multiplier: plan.Multiplier, Scale: plan.Scale,
		Bus: cfg.Scenario.Bus, Nodes: len(cfg.Inventory.Nodes), Shards: cfg.Scenario.Shards,
		Environment: cfg.Inventory.Environment,
		StartUS:     startAt.UnixMicro(), MeasureStartUS: measureStart.UnixMicro(), MeasureEndUS: measureEnd.UnixMicro(), EndUS: time.Now().UnixMicro(),
		Plan: totals, Fault: fault, Jobs: refs,
	}
	if b := cfg.Inventory.Environment["bus"]; b != "" {
		rec.Bus = b
	}
	rec.Result = MergeSummaries(sums, plan.Pass.TailMargin.Duration)
	rec.NodeStats = ComputeNodeStats(allSamples, rec.MeasureStartUS, rec.MeasureEndUS)
	rec.Footprint = ComputeFootprint(cfg.Inventory, rec.Result, rec.NodeStats)
	Evaluate(rec, plan.Pass)
	rec.Errors = errs
	for _, s := range sums {
		if s != nil {
			for _, e := range s.Errors {
				if len(rec.Errors) < 50 {
					rec.Errors = append(rec.Errors, s.Role+"/"+fmt.Sprint(s.Index)+": "+e)
				}
			}
		}
	}
	if len(errs) > 0 {
		rec.Pass = false
		rec.Verdict = "FAIL"
		rec.Checks = append(rec.Checks, Check{Name: "summaries collected", Value: fmt.Sprintf("%d missing", len(errs)), Limit: "all", Pass: false, Gating: true})
	}
	if timedOut {
		rec.Pass = false
		rec.Verdict = "ABORTED"
	}
	if err := WriteJSONFile(filepath.Join(runDir, "summary.json"), rec); err != nil {
		return rec, err
	}
	if err := os.WriteFile(filepath.Join(runDir, "summary.md"), []byte(rec.Markdown()), 0o644); err != nil {
		return rec, err
	}
	if cfg.LogFile != "" {
		if err := appendLine(cfg.LogFile, rec.LogLine()); err != nil {
			cfg.logf("log: %v", err)
		}
	}
	if cfg.StateFile != "" {
		if err := appendStateRun(cfg.StateFile, rec, runDir); err != nil {
			cfg.logf("state: %v", err)
		}
	}
	cfg.logf("run %s: %s (results in %s)", cfg.RunID, rec.Verdict, runDir)
	return rec, nil
}

func appendLine(path, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(f, line); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// appendStateRun adds the run to STATE.json's "runs" array, keeping every
// other key as it is.
func appendStateRun(path string, rec *RunRecord, runDir string) error {
	state := map[string]any{}
	if b, err := os.ReadFile(path); err == nil && len(bytes.TrimSpace(b)) > 0 {
		if err := json.Unmarshal(b, &state); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	runs, _ := state["runs"].([]any)
	runs = append(runs, map[string]any{
		"run_id": rec.RunID, "scenario": rec.Scenario, "shape": rec.Shape, "multiplier": rec.Multiplier,
		"scale": rec.Scale, "bus": rec.Bus, "nodes": rec.Nodes, "verdict": rec.Verdict,
		"start": time.UnixMicro(rec.StartUS).UTC(), "results": runDir,
	})
	state["runs"] = runs
	return WriteJSONFile(path, state)
}

// EvaluateRunDir re-evaluates a run from the agent summaries saved in its
// directory (for instance after changing a pass criterion), keeping the
// node samples and fault record of the original summary.json.
func EvaluateRunDir(runDir string, inv *Inventory) (*RunRecord, error) {
	var rec RunRecord
	b, err := os.ReadFile(filepath.Join(runDir, "summary.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(runDir, "agents", "*.json"))
	if err != nil {
		return nil, err
	}
	var sums []*Summary
	var sc *Scenario
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var s Summary
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		sums = append(sums, &s)
	}
	var pb struct {
		Scenario  Scenario   `json:"scenario"`
		Inventory *Inventory `json:"inventory"`
	}
	if b, err := os.ReadFile(filepath.Join(runDir, "plan.json")); err == nil {
		if json.Unmarshal(b, &pb) == nil {
			sc = &pb.Scenario
			if inv == nil {
				inv = pb.Inventory
			}
		}
	}
	spec := DefaultPass()
	if sc != nil {
		spec = sc.Pass.withDefaults()
	}
	rec.Result = MergeSummaries(sums, spec.TailMargin.Duration)
	rec.NodeStats = ComputeNodeStats(rec.NodeStats.Samples, rec.MeasureStartUS, rec.MeasureEndUS)
	rec.Footprint = ComputeFootprint(inv, rec.Result, rec.NodeStats)
	Evaluate(&rec, spec)
	return &rec, nil
}
