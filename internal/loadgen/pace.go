package loadgen

import (
	"context"
	"time"
)

// paceTick is how often Pace wakes. 5 ms keeps per-tick bursts small at
// tens of thousands of events a second while costing little CPU.
const paceTick = 5 * time.Millisecond

// Pace calls fire at the rate rate(t) (events/s, evaluated each tick)
// until ctx ends or the rate function returns a negative value. It is
// open-loop: the schedule does not slow down when fire is slow, so a
// saturated generator shows up as a backlog rather than as a quietly
// lower rate. At most maxCatchUp of missed schedule is replayed after a
// stall; the rest is reported to dropped (events that were due and never
// fired). fire receives the scheduled time of the event.
func Pace(ctx context.Context, rate func(time.Time) float64, maxCatchUp time.Duration, fire func(at time.Time), dropped func(n int64)) {
	t := time.NewTicker(paceTick)
	defer t.Stop()
	last := time.Now()
	var owed float64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			r := rate(now)
			if r < 0 {
				// Events owed when the schedule ends were never fired.
				if dropped != nil && owed >= 1 {
					dropped(int64(owed))
				}
				return
			}
			dt := now.Sub(last)
			last = now
			owed += r * dt.Seconds()
			// The cap is at least one event, or a rate below
			// 1/maxCatchUp could never accumulate a whole event.
			if limit := max(r*maxCatchUp.Seconds(), 1); owed > limit {
				if dropped != nil {
					dropped(int64(owed - limit))
				}
				owed = limit
			}
			n := int64(owed)
			if n <= 0 {
				continue
			}
			owed -= float64(n)
			step := dt / time.Duration(n)
			start := now.Add(-dt)
			for i := int64(0); i < n; i++ {
				if ctx.Err() != nil {
					return
				}
				fire(start.Add(step * time.Duration(i+1)))
			}
		}
	}
}

// RampRate returns a rate function that rises linearly from 0 at start
// to target at start+ramp, holds target until stop, and returns -1 (end)
// from stop on.
func RampRate(target float64, start time.Time, ramp time.Duration, stop time.Time) func(time.Time) float64 {
	return func(now time.Time) float64 {
		if !now.Before(stop) {
			return -1
		}
		if now.Before(start) {
			return 0
		}
		if ramp <= 0 || now.Sub(start) >= ramp {
			return target
		}
		return target * float64(now.Sub(start)) / float64(ramp)
	}
}
