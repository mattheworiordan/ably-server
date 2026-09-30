package main

import (
	"flag"
	"io"
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
