package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

// TestServerIdleTimeoutFlag: unset defers to the scenario, an explicit
// 0 measures growth from hold start, a positive value overrides, and a
// negative value is refused.
func TestServerIdleTimeoutFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 0},
		{[]string{"--server-idle-timeout", "0"}, -1},
		{[]string{"--server-idle-timeout", "0s"}, -1},
		{[]string{"--server-idle-timeout", "15s"}, 15 * time.Second},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		var o optionalDuration
		fs.Var(&o, "server-idle-timeout", "")
		if err := fs.Parse(tc.args); err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if got := o.conductorIdle(); got != tc.want {
			t.Errorf("%v: ServerIdleTimeout = %s, want %s", tc.args, got, tc.want)
		}
	}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var o optionalDuration
	fs.Var(&o, "server-idle-timeout", "")
	if err := fs.Parse([]string{"--server-idle-timeout", "-1s"}); err == nil {
		t.Error("a negative --server-idle-timeout was accepted")
	}
	if o.String() != "" {
		t.Errorf("unset flag prints %q, want empty", o.String())
	}
}

// A run record from before the newer checks existed re-evaluates with
// "not recorded in this run" rows and the verdict it had.
func TestEvaluateALegacyRunDirectory(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"evaluate", "../../internal/loadgen/testdata/legacy-shape-m"}, &out, &errb)
	if code != 1 { // the record failed its latency gates when it was run
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	md := out.String()
	for _, want := range []string{"# Run run-20261001T141856Z-shape-m: shape M at 5x, scale 1: FAIL", "| sample coverage (attach) | not recorded in this run", "| node metrics coverage | not recorded in this run",
		"| generator CPU | not recorded in this run", "| generator clock offset | not recorded in this run", "| server configuration | not recorded in this run"} {
		if !strings.Contains(md, want) {
			t.Errorf("output lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "| FAIL |") && strings.Contains(md, "not recorded in this run | n/a | FAIL") {
		t.Error("a not-recorded row failed")
	}
}
