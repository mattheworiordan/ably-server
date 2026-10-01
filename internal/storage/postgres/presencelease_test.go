package postgres

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads a counter's current value.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// TestReaperGuardAfterOpen: a node reaps nothing until it has held its
// own lease without a break for one lease window, and a failed renewal
// restarts that wait.
func TestReaperGuardAfterOpen(t *testing.T) {
	var r leaseRun
	window := presenceLeaseWindow
	t0 := time.Now()
	r.begin(t0)
	if ok, _, _ := r.reapable(t0.Add(window / 2)); ok {
		t.Fatal("reapable half a window after Open")
	}
	r.renewed(t0.Add(window/3), t0.Add(window/3), false)
	r.renewed(t0.Add(2*window/3), t0.Add(2*window/3), false)
	r.renewed(t0.Add(window), t0.Add(window), false)
	if ok, why, _ := r.reapable(t0.Add(window + time.Millisecond)); !ok {
		t.Fatalf("not reapable after a full window of renewals: %s", why)
	}
	r.broken()
	if ok, _, _ := r.reapable(t0.Add(window + 2*time.Millisecond)); ok {
		t.Fatal("reapable after a failed renewal")
	}
	t1 := t0.Add(window + window/3)
	r.renewed(t1, t1, false)
	if ok, _, _ := r.reapable(t1.Add(window / 2)); ok {
		t.Fatal("reapable half a window into a new run")
	}
	r.renewed(t1.Add(window/3), t1.Add(window/3), false)
	r.renewed(t1.Add(2*window/3), t1.Add(2*window/3), false)
	r.renewed(t1.Add(window), t1.Add(window), false)
	if ok, why, _ := r.reapable(t1.Add(window + time.Millisecond)); !ok {
		t.Fatalf("not reapable a full window into the new run: %s", why)
	}
	// A renewal more than a window after the last is a lapse: a new run.
	t2 := t1.Add(3 * window)
	if lapsed, _ := r.renewed(t2, t2, false); !lapsed {
		t.Fatal("a renewal 2 windows late is not a lapse")
	}
	if ok, _, _ := r.reapable(t2.Add(window / 2)); ok {
		t.Fatal("reapable half a window after a lapse")
	}
	// A renewal that started in time but completed more than a window
	// after the previous one began (it waited for a connection) may have
	// been stamped after the lease expired: a lapse.
	t3 := t2.Add(window / 2)
	if lapsed, _ := r.renewed(t3, t3, false); lapsed {
		t.Fatal("a timely renewal reported a lapse")
	}
	if lapsed, _ := r.renewed(t3.Add(window/2), t3.Add(window+time.Second), false); !lapsed {
		t.Fatal("a renewal that completed more than a window after the previous one started is not a lapse")
	}
	// The node's own lease going stale stops reaping too.
	if ok, _, _ := r.reapable(t3.Add(3 * window)); ok {
		t.Fatal("reapable with its own lease a window stale")
	}
}
