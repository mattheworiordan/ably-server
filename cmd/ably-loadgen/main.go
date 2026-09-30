// Command ably-loadgen generates realtime and REST load against
// ably-server nodes and checks delivery correctness (plan §6 item 8).
//
// It runs in one of two modes:
//
//	# Long-running agent on a generator box; the conductor drives it.
//	ably-loadgen serve --listen=:9200 --metrics-listen=:9101 [--role=publisher]
//
//	# One job from flags (or a JobSpec file), summary to a file.
//	ably-loadgen run --scenario bench/scenarios/shape-m.toml \
//	    --role subscriber --index 0 --count 3 --scale 0.01 \
//	    --endpoints 10.0.0.1:8080,10.0.0.2:8080 --summary sub-0.json
//
// See bench/aws/README.md for the roles, the flags and the summary format.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	switch args[0] {
	case "agent", "serve":
		return runAgent(ctx, args[1:], stdout, stderr)
	case "run":
		return runJob(ctx, args[1:], stdout, stderr)
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
	fmt.Fprintln(w, `usage: ably-loadgen <command> [flags]

commands:
  serve   run the HTTP control endpoint the conductor drives ("agent" is an alias)
  run     run one job from flags or a JobSpec file and write its summary

run "ably-loadgen <command> -h" for the command's flags`)
}

func runAgent(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-loadgen serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", envOr("ABLY_LOADGEN_LISTEN", ":9200"), "control listen address; also serves /metrics (env ABLY_LOADGEN_LISTEN)")
	metricsListen := fs.String("metrics-listen", envOr("ABLY_LOADGEN_METRICS_LISTEN", ""), "also serve /metrics on this address, for Prometheus (env ABLY_LOADGEN_METRICS_LISTEN)")
	role := fs.String("role", envOr("ABLY_LOADGEN_ROLE", "all"), "job roles this agent accepts: generator (subscriber, realtime-publisher, presence), publisher (rest-publisher), all, or a comma-separated list of roles (env ABLY_LOADGEN_ROLE)")
	summaryDir := fs.String("summary-dir", envOr("ABLY_LOADGEN_SUMMARY_DIR", ""), "directory to also write each finished job's summary to (env ABLY_LOADGEN_SUMMARY_DIR)")
	addrFile := fs.String("addr-file", "", "write the bound address to this file once listening (for local spawning with --listen 127.0.0.1:0)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	roles, err := agentRoles(*role)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	a := loadgen.NewAgent(ctx, nil)
	a.SummaryDir = *summaryDir
	a.Roles = roles
	if *metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", a.M.Handler())
		msrv := &http.Server{Addr: *metricsListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintf(stderr, "metrics listen: %v\n", err)
			}
		}()
		defer func() { _ = msrv.Close() }()
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "listen: %v\n", err)
		return 1
	}
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(ln.Addr().String()), 0o644); err != nil {
			fmt.Fprintf(stderr, "addr file: %v\n", err)
			return 1
		}
	}
	srv := &http.Server{Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	fmt.Fprintf(stdout, "ably-loadgen serve listening on %s (roles %v)\n", ln.Addr(), roles)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return 1
	}
	return 0
}

func runJob(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-loadgen run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jobFile := fs.String("job", "", "JobSpec JSON file (flags below are ignored when set, except --summary and --metrics-listen)")
	scenario := fs.String("scenario", "", "scenario TOML file")
	role := fs.String("role", loadgen.RoleSubscriber, "subscriber | rest-publisher | realtime-publisher | presence")
	index := fs.Int("index", 0, "this process's index within its role")
	count := fs.Int("count", 1, "number of processes in this role")
	endpoints := fs.String("endpoints", envOr("ABLY_LOADGEN_ENDPOINTS", "localhost:8080"), "comma-separated server nodes host:port, same order on every process (env ABLY_LOADGEN_ENDPOINTS)")
	key := fs.String("key", envOr("ABLY_SERVER_KEYS", "app.key:secret"), "API key appId.keyId:secret (env ABLY_SERVER_KEYS)")
	multiplier := fs.Float64("multiplier", 0, "load multiplier (1 or 2); 0 takes the scenario's")
	scale := fs.Float64("scale", 0, "scale factor (0.01, 0.1 for smoke); 0 takes the scenario's")
	runTag := fs.String("run-tag", "", "run tag shared by every process of a run ([A-Za-z0-9_], default random)")
	runID := fs.String("run-id", "", "run id recorded in the summary")
	startIn := fs.Duration("start-in", 2*time.Second, "delay before the ramp starts (ignored with --start-at)")
	startAt := fs.String("start-at", "", "RFC 3339 time the ramp starts, the same on every process")
	ramp := fs.Duration("ramp", -1, "override the scenario's ramp")
	hold := fs.Duration("hold", -1, "override the scenario's hold")
	drain := fs.Duration("drain", -1, "override the scenario's drain")
	workers := fs.Int("workers", 256, "REST publisher: concurrent HTTP requests")
	rtConns := fs.Int("realtime-conns", 16, "realtime publisher: WebSocket connections")
	format := fs.String("format", "msgpack", "realtime wire format: msgpack | json")
	summary := fs.String("summary", "", "write the JSON summary here (default stdout)")
	metricsListen := fs.String("metrics-listen", "", "serve /metrics on this address during the run")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	var spec loadgen.JobSpec
	if *jobFile != "" {
		b, err := os.ReadFile(*jobFile)
		if err != nil {
			fmt.Fprintf(stderr, "job: %v\n", err)
			return 2
		}
		if err := json.Unmarshal(b, &spec); err != nil {
			fmt.Fprintf(stderr, "job: %v\n", err)
			return 2
		}
	} else {
		if *scenario == "" {
			fmt.Fprintln(stderr, "--scenario or --job is required")
			return 2
		}
		sc, err := loadgen.LoadScenario(*scenario)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		if *ramp >= 0 {
			sc.Timing.Ramp.Duration = *ramp
		}
		if *hold >= 0 {
			sc.Timing.Hold.Duration = *hold
		}
		if *drain >= 0 {
			sc.Timing.Drain.Duration = *drain
		}
		tag := *runTag
		if tag == "" {
			tag = randomTag()
		}
		start := time.Now().Add(*startIn)
		if *startAt != "" {
			t, err := time.Parse(time.RFC3339Nano, *startAt)
			if err != nil {
				fmt.Fprintf(stderr, "--start-at: %v\n", err)
				return 2
			}
			start = t
		}
		spec = loadgen.JobSpec{
			ID: fmt.Sprintf("%s-%d", *role, *index), RunID: *runID, RunTag: tag,
			Scenario: *sc, Multiplier: *multiplier, Scale: *scale,
			Role: *role, Index: *index, Count: *count,
			Endpoints: splitList(*endpoints), Key: *key, StartAtUS: start.UnixMicro(),
			Workers: *workers, RealtimeConns: *rtConns, Format: *format,
		}
	}
	m := loadgen.NewMetrics()
	job, err := loadgen.NewJob(spec, m)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if *metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", m.Handler())
		srv := &http.Server{Addr: *metricsListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = srv.ListenAndServe() }()
		defer func() { _ = srv.Close() }()
	}
	s := job.Run(ctx)
	if *summary != "" {
		if err := loadgen.WriteJSONFile(*summary, s); err != nil {
			fmt.Fprintf(stderr, "summary: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "summary written to %s\n", *summary)
	} else {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(s)
	}
	var v int64
	for _, n := range s.Correctness.Violations {
		v += n
	}
	if v > 0 {
		fmt.Fprintf(stderr, "correctness violations: %v\n", s.Correctness.Violations)
		return 1
	}
	return 0
}

// agentRoles expands the --role value of serve.
func agentRoles(v string) ([]string, error) {
	switch v {
	case "", "all":
		return nil, nil
	case "generator":
		return []string{loadgen.RoleSubscriber, loadgen.RoleRealtime, loadgen.RolePresence}, nil
	case "publisher":
		return []string{loadgen.RoleREST}, nil
	}
	roles := splitList(v)
	for _, r := range roles {
		switch r {
		case loadgen.RoleSubscriber, loadgen.RoleREST, loadgen.RoleRealtime, loadgen.RolePresence:
		default:
			return nil, fmt.Errorf("--role %q: want generator, publisher, all or a list of roles", v)
		}
	}
	return roles, nil
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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
