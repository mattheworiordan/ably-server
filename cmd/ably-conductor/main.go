// Command ably-conductor runs a load scenario end to end (plan §7): it
// resolves a scenario file at a multiplier and scale, assigns the work to
// ably-loadgen agents, ramps, holds, optionally injects a failure,
// collects every job's summary and the nodes' metrics, evaluates the
// pass criteria (plan §8) and writes a run record.
//
//	ably-conductor plan --scenario bench/scenarios/shape-m.toml --multiplier 2
//	ably-conductor run --scenario ... --inventory inventory.json --results results/
//	ably-conductor run --scenario ... --scale 0.01 --local 2 --endpoints localhost:8081,localhost:8082
//	ably-conductor evaluate results/<run-id>
//	ably-conductor report results/*/summary.json
//
// See bench/aws/README.md.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/loadgen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	// Bare flags mean run: "ably-conductor --scenario=... --inventory=..."
	// is how bench/aws/60-run.sh invokes it.
	if strings.HasPrefix(args[0], "-") && args[0] != "-h" && args[0] != "--help" {
		return cmdRun(ctx, args, stdout, stderr)
	}
	switch args[0] {
	case "plan":
		return cmdPlan(args[1:], stdout, stderr)
	case "run":
		return cmdRun(ctx, args[1:], stdout, stderr)
	case "evaluate":
		return cmdEvaluate(args[1:], stdout, stderr)
	case "report":
		return cmdReport(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: ably-conductor <command> [flags]

commands:
  plan      print a scenario's derived load at a multiplier and scale
  run       run a scenario against an inventory (or local agents) and evaluate it
            (the default: "ably-conductor --scenario=... --inventory=..." means run)
  evaluate  re-evaluate a run directory from its saved agent summaries
  report    build the report tables from one or more run summary.json files

run "ably-conductor <command> -h" for the command's flags`)
}

// scenarioFlags are shared by plan and run.
type scenarioFlags struct {
	scenario          string
	multiplier, scale float64
	ramp, hold, drain time.Duration
}

func (sf *scenarioFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&sf.scenario, "scenario", "", "scenario TOML file (bench/scenarios/*.toml)")
	fs.Float64Var(&sf.multiplier, "multiplier", 0, "load multiplier: 1 (envelope) or 2 (headline); 0 takes the scenario's")
	fs.Float64Var(&sf.scale, "scale", 0, "scale factor: 0.01 and 0.1 for the smoke steps; 0 takes the scenario's (1)")
	fs.DurationVar(&sf.ramp, "ramp", -1, "override the scenario's ramp")
	fs.DurationVar(&sf.hold, "hold", -1, "override the scenario's hold (30m for the headline)")
	fs.DurationVar(&sf.drain, "drain", -1, "override the scenario's drain")
}

func (sf *scenarioFlags) load() (*loadgen.Scenario, error) {
	if sf.scenario == "" {
		return nil, errors.New("--scenario is required")
	}
	sc, err := loadgen.LoadScenario(sf.scenario)
	if err != nil {
		return nil, err
	}
	if sf.ramp >= 0 {
		sc.Timing.Ramp.Duration = sf.ramp
	}
	if sf.hold >= 0 {
		sc.Timing.Hold.Duration = sf.hold
	}
	if sf.drain >= 0 {
		sc.Timing.Drain.Duration = sf.drain
	}
	return sc, sc.Validate()
}

func cmdPlan(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-conductor plan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var sf scenarioFlags
	sf.register(fs)
	asJSON := fs.Bool("json", false, "print JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sc, err := sf.load()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	p, err := sc.Resolve(sf.multiplier, sf.scale, "plan")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	t, cts := p.Totals()
	if *asJSON {
		fmt.Fprintln(stdout, loadgen.JSON(map[string]any{"totals": t, "classes": cts, "roles": loadgen.RolesNeeded(p)}))
		return 0
	}
	fmt.Fprintf(stdout, "shape %s (%s) at %gx, scale %g: ramp %s, hold %s, drain %s\n\n", sc.Shape, sc.Name, p.Multiplier, p.Scale,
		sc.Timing.Ramp.Duration, sc.Timing.Hold.Duration, sc.Timing.Drain.Duration)
	fmt.Fprintf(stdout, "| Measure | Value |\n|---|---|\n")
	fmt.Fprintf(stdout, "| Connections | %d |\n| Attachments | %d |\n| Channels | %d (%d with subscribers, %d sampled) |\n",
		t.Connections, t.Attachments, t.Channels, t.SubscribedChannels, t.SampledChannels)
	fmt.Fprintf(stdout, "| Publishes/s | %.0f (REST %.0f, realtime %.0f) |\n| Deliveries/s | %.0f (fan-out %.2f) |\n",
		t.PublishesPerSec, t.RESTPublishesPerS, t.RTPublishesPerSec, t.DeliveriesPerSec, t.FanOut)
	fmt.Fprintf(stdout, "| Inbound bytes/s | %.0f |\n| Streams | %d (fastest %.2f/s) |\n| Connects/s | %.0f |\n| Channel opens/s | %.0f |\n",
		t.InboundBytesPerSec, t.Streams, t.MaxStreamRate, t.ConnectsPerSec, t.ChannelOpensPerSec)
	if t.PresenceMembers > 0 {
		fmt.Fprintf(stdout, "| Presence members | %d |\n| Presence events/s | %.0f |\n", t.PresenceMembers, t.PresenceEventsPerS)
	}
	fmt.Fprintf(stdout, "\n| Class | Channels | Subscribers (min-max) | Attachments | Publisher | Publishes/s | Deliveries/s | Bytes |\n|---|---|---|---|---|---|---|---|\n")
	for _, c := range cts {
		clamp := ""
		if c.Clamped > 0 {
			clamp = fmt.Sprintf(" (%d clamped)", c.Clamped)
		}
		fmt.Fprintf(stdout, "| %s | %d | %d-%d%s | %d | %s | %.1f | %.0f | %d |\n", c.Name, c.Channels, c.MinSubscribers, c.MaxSubscribers, clamp,
			c.Attachments, c.Publisher, c.PublishesPerSec, c.DeliveriesPerSec, c.MessageBytes)
	}
	fmt.Fprintf(stdout, "\nroles needed: %s\n", strings.Join(loadgen.RolesNeeded(p), ", "))
	return 0
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-conductor run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var sf scenarioFlags
	sf.register(fs)
	invPath := fs.String("inventory", "", "inventory JSON (nodes and agents; see bench/aws/README.md)")
	endpoints := fs.String("endpoints", "", "without --inventory: comma-separated node host:port list")
	nodeMetrics := fs.String("node-metrics", "", "without --inventory: comma-separated node /metrics URLs, same order as --endpoints")
	var agents multiFlag
	fs.Var(&agents, "agent", "without --inventory: an agent as URL or roles=URL (roles comma-separated); repeatable")
	local := fs.Int("local", 0, "spawn this many local ably-loadgen agents (every role) instead of --agent")
	loadgenBin := fs.String("loadgen-bin", "", "ably-loadgen binary for --local (default: next to this binary, then PATH)")
	key := fs.String("key", "", "API key (default: inventory key, then ABLY_SERVER_KEYS)")
	runID := fs.String("run-id", "", "run id; when given, the run writes straight into --results (default <shape>-<mult>x-<UTC time> under --results)")
	runTag := fs.String("run-tag", "", "run tag for channel names and message ids ([A-Za-z0-9_], default random)")
	results := fs.String("results", "results", "results directory: the run's own directory when --run-id is given, else the root for <results>/<run-id>/")
	nodeVCPU := fs.Float64("node-vcpu", 0, "vCPU per node for the footprint, when the inventory does not say")
	nodeMemGB := fs.Float64("node-memory-gb", 0, "memory (GB) per node for the footprint, when the inventory does not say")
	logFile := fs.String("log", "", "append the run's one-line summary to this file (LOG.md)")
	stateFile := fs.String("state", "", "append the run to this STATE.json's runs array")
	startDelay := fs.Duration("start-delay", 10*time.Second, "time between sending jobs and the ramp start")
	poll := fs.Duration("poll", 10*time.Second, "status and node-metrics interval")
	faultHook := fs.String("fault-hook", "", "shell command run at --fault-at into the hold (kill a node, kill a NATS server)")
	faultAt := fs.Duration("fault-at", 5*time.Minute, "offset into the hold for --fault-hook")
	timeLimit := fs.Duration("time-limit", 0, "hard limit for the whole run (default planned length + 5m)")
	onTimeout := fs.String("on-timeout", "", "shell command run when the time limit is hit (for instance the teardown script)")
	format := fs.String("format", "msgpack", "realtime wire format for the jobs: msgpack | json")
	var envs multiFlag
	fs.Var(&envs, "env", "environment label key=value for the run record (bus, storage, instance types); repeatable")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sc, err := sf.load()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	var inv *loadgen.Inventory
	if *invPath != "" {
		if inv, err = loadgen.LoadInventory(*invPath); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	} else {
		inv = &loadgen.Inventory{}
		metrics := splitList(*nodeMetrics)
		for i, ep := range splitList(*endpoints) {
			n := loadgen.InventoryNode{Name: fmt.Sprintf("node-%d", i+1), Endpoint: ep}
			if i < len(metrics) {
				n.Metrics = metrics[i]
			}
			inv.Nodes = append(inv.Nodes, n)
		}
		for i, a := range agents {
			roles := []string{loadgen.RoleSubscriber, loadgen.RoleREST, loadgen.RoleRealtime, loadgen.RolePresence}
			url := a
			if r, u, ok := strings.Cut(a, "="); ok {
				roles, url = splitList(r), u
			}
			inv.Agents = append(inv.Agents, loadgen.InventoryAgent{Name: fmt.Sprintf("agent-%d", i+1), URL: url, Roles: roles})
		}
	}
	if *key != "" {
		inv.Key = *key
	}
	for i := range inv.Nodes {
		if inv.Nodes[i].VCPU == 0 {
			inv.Nodes[i].VCPU = *nodeVCPU
		}
		if inv.Nodes[i].MemoryGB == 0 {
			inv.Nodes[i].MemoryGB = *nodeMemGB
		}
	}
	for _, e := range envs {
		k, v, ok := strings.Cut(e, "=")
		if !ok {
			fmt.Fprintf(stderr, "--env %q: want key=value\n", e)
			return 2
		}
		if inv.Environment == nil {
			inv.Environment = map[string]string{}
		}
		inv.Environment[k] = v
	}
	if *local > 0 {
		stopAgents, err := spawnLocalAgents(ctx, inv, *local, *loadgenBin, filepath.Join(os.TempDir(), fmt.Sprintf("ably-conductor-agents-%d", os.Getpid())), stderr)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		defer stopAgents()
	}
	if err := inv.Validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	mult := sf.multiplier
	if mult == 0 {
		mult = max(sc.Multiplier, 1)
	}
	id := *runID
	runDir := *results
	if id == "" {
		id = fmt.Sprintf("%s-%gx-%s", sc.Shape, mult, time.Now().UTC().Format("20060102T150405Z"))
		runDir = filepath.Join(*results, id)
	}
	tag := *runTag
	if tag == "" {
		tag = randomTag()
	}
	rec, err := loadgen.RunConductor(ctx, loadgen.ConductorConfig{
		Scenario: sc, Multiplier: sf.multiplier, Scale: sf.scale, RunID: id, RunTag: tag, Inventory: inv,
		ResultsDir: *results, RunDir: runDir, LogFile: *logFile, StateFile: *stateFile, StartDelay: *startDelay, Poll: *poll,
		FaultHook: *faultHook, FaultAt: *faultAt, TimeLimit: *timeLimit, OnTimeout: *onTimeout,
		Format: *format, Out: stdout,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stdout)
	fmt.Fprint(stdout, rec.Markdown())
	if !rec.Pass {
		return 1
	}
	return 0
}

// spawnLocalAgents starts n ably-loadgen agents on 127.0.0.1 and adds
// them to the inventory with every role.
func spawnLocalAgents(ctx context.Context, inv *loadgen.Inventory, n int, bin, dir string, stderr io.Writer) (func(), error) {
	if bin == "" {
		if self, err := os.Executable(); err == nil {
			cand := filepath.Join(filepath.Dir(self), "ably-loadgen")
			if _, err := os.Stat(cand); err == nil {
				bin = cand
			}
		}
	}
	if bin == "" {
		p, err := exec.LookPath("ably-loadgen")
		if err != nil {
			return nil, errors.New("ably-loadgen not found: pass --loadgen-bin")
		}
		bin = p
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var cmds []*exec.Cmd
	stop := func() {
		for _, c := range cmds {
			_ = c.Process.Signal(syscall.SIGTERM)
		}
		for _, c := range cmds {
			done := make(chan struct{})
			go func() { _ = c.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = c.Process.Kill()
			}
		}
	}
	for i := 0; i < n; i++ {
		addrFile := filepath.Join(dir, fmt.Sprintf("agent-%d.addr", i))
		_ = os.Remove(addrFile)
		logf, err := os.Create(filepath.Join(dir, fmt.Sprintf("agent-%d.log", i)))
		if err != nil {
			stop()
			return nil, err
		}
		cmd := exec.CommandContext(ctx, bin, "serve", "--listen", "127.0.0.1:0", "--addr-file", addrFile)
		cmd.Stdout, cmd.Stderr = logf, logf
		if err := cmd.Start(); err != nil {
			stop()
			return nil, fmt.Errorf("start %s: %w", bin, err)
		}
		cmds = append(cmds, cmd)
		var addr []byte
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if addr, err = os.ReadFile(addrFile); err == nil && len(addr) > 0 {
				break
			}
		}
		if len(addr) == 0 {
			stop()
			return nil, fmt.Errorf("local agent %d did not start (see %s)", i, logf.Name())
		}
		inv.Agents = append(inv.Agents, loadgen.InventoryAgent{
			Name: fmt.Sprintf("local-%d", i), URL: "http://" + string(addr),
			Roles: []string{loadgen.RoleSubscriber, loadgen.RoleREST, loadgen.RoleRealtime, loadgen.RolePresence},
		})
	}
	fmt.Fprintf(stderr, "spawned %d local agents (%s)\n", n, bin)
	return stop, nil
}

func cmdEvaluate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-conductor evaluate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	invPath := fs.String("inventory", "", "inventory JSON for the footprint (default: the one saved in plan.json)")
	write := fs.Bool("write", false, "rewrite summary.json and summary.md in the run directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: ably-conductor evaluate [--write] <run-dir>")
		return 2
	}
	var inv *loadgen.Inventory
	if *invPath != "" {
		var err error
		if inv, err = loadgen.LoadInventory(*invPath); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	rec, err := loadgen.EvaluateRunDir(fs.Arg(0), inv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if *write {
		if err := loadgen.WriteJSONFile(filepath.Join(fs.Arg(0), "summary.json"), rec); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := os.WriteFile(filepath.Join(fs.Arg(0), "summary.md"), []byte(rec.Markdown()), 0o644); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	fmt.Fprint(stdout, rec.Markdown())
	if !rec.Pass {
		return 1
	}
	return 0
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func randomTag() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
