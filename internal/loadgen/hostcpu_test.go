package loadgen

import (
	"strings"
	"testing"
)

const procStat = `cpu  100 10 50 800 20 5 15 0 0 0
cpu0 50 5 25 400 10 2 8 0 0 0
cpu1 50 5 25 400 10 3 7 0 0 0
intr 12345
ctxt 999
`

func TestParseProcStat(t *testing.T) {
	h, err := ParseProcStat(strings.NewReader(procStat))
	if err != nil {
		t.Fatal(err)
	}
	// busy = user+nice+system+irq+softirq+steal = 100+10+50+5+15+0; idle = 800+20.
	if h.Busy != 180 || h.Total != 1000 || h.CPUs != 2 {
		t.Fatalf("%+v", h)
	}
	for _, bad := range []string{"", "intr 1\n", "cpu 1 2 3\n", "cpu a b c d e f g h\n"} {
		if _, err := ParseProcStat(strings.NewReader(bad)); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestBusyFraction(t *testing.T) {
	a := HostCPU{Busy: 100, Total: 1000}
	b := HostCPU{Busy: 400, Total: 1400}
	if got := BusyFraction(a, b); got != 0.75 {
		t.Fatalf("got %v, want 0.75", got)
	}
	if BusyFraction(a, a) != 0 {
		t.Fatal("no elapsed time must read as 0, not NaN")
	}
}
