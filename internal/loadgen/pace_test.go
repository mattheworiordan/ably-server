package loadgen

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func countFires(t *testing.T, rate float64, d time.Duration) (fired, dropped int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := time.Now().Add(d)
	var f, dr atomic.Int64
	Pace(ctx, func(now time.Time) float64 {
		if !now.Before(stop) {
			return -1
		}
		return rate
	}, time.Second, func(time.Time) { f.Add(1) }, func(n int64) { dr.Add(n) })
	return f.Load(), dr.Load()
}

func TestPaceLowRateFires(t *testing.T) {
	// 0.5/s is below 1/maxCatchUp: it must still fire (it once never did).
	fired, _ := countFires(t, 0.5, 4200*time.Millisecond)
	if fired < 1 || fired > 3 {
		t.Fatalf("0.5/s over 4.2s fired %d times, want about 2", fired)
	}
}

func TestPaceRateAccuracy(t *testing.T) {
	fired, dropped := countFires(t, 2000, time.Second)
	if fired < 1800 || fired > 2100 || dropped != 0 {
		t.Fatalf("2000/s over 1s: fired %d dropped %d", fired, dropped)
	}
}

func TestPaceCountsDroppedWhenFireStalls(t *testing.T) {
	ctx := context.Background()
	stop := time.Now().Add(1500 * time.Millisecond)
	var fired, dropped atomic.Int64
	first := true
	Pace(ctx, func(now time.Time) float64 {
		if !now.Before(stop) {
			return -1
		}
		return 100
	}, 100*time.Millisecond, func(time.Time) {
		if first {
			first = false
			time.Sleep(time.Second) // a stalled send
		}
		fired.Add(1)
	}, func(n int64) { dropped.Add(n) })
	if dropped.Load() < 50 {
		t.Fatalf("a 1s stall at 100/s with 100ms catch-up dropped only %d (fired %d)", dropped.Load(), fired.Load())
	}
}

func TestRampRate(t *testing.T) {
	start := time.Unix(100, 0)
	r := RampRate(10, start, 10*time.Second, start.Add(20*time.Second))
	for _, c := range []struct {
		at   time.Duration
		want float64
	}{{-time.Second, 0}, {0, 0}, {5 * time.Second, 5}, {10 * time.Second, 10}, {19 * time.Second, 10}, {20 * time.Second, -1}} {
		if got := r(start.Add(c.at)); got != c.want {
			t.Errorf("rate at %s = %v, want %v", c.at, got, c.want)
		}
	}
}
