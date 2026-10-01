package postgres

import (
	"math"
	"slices"
	"testing"
	"time"
)

func TestParseRangeBound(t *testing.T) {
	cases := []struct {
		in     string
		lo, hi int64
		ok     bool
	}{
		{"FOR VALUES FROM ('01727700000000') TO ('01727700060000')", 1727700000000, 1727700060000, true},
		{"FOR VALUES FROM (MINVALUE) TO ('01727700060000')", math.MinInt64, 1727700060000, true},
		{"FOR VALUES FROM ('01727700000000') TO (MAXVALUE)", 1727700000000, math.MaxInt64, true},
		{"FOR VALUES IN (false)", 0, 0, false},
		{"FOR VALUES FROM ('abc') TO ('01727700060000')", 0, 0, false},
	}
	for _, c := range cases {
		lo, hi, err := parseRangeBound(c.in)
		if (err == nil) != c.ok {
			t.Errorf("parseRangeBound(%q) err = %v, want ok=%v", c.in, err, c.ok)
			continue
		}
		if c.ok && (lo != c.lo || hi != c.hi) {
			t.Errorf("parseRangeBound(%q) = %d, %d; want %d, %d", c.in, lo, hi, c.lo, c.hi)
		}
	}
}

func TestFloorTo(t *testing.T) {
	for _, c := range []struct{ ms, width, want int64 }{
		{125, 60, 120}, {120, 60, 120}, {0, 60, 0}, {-1, 60, -60}, {-60, 60, -60},
	} {
		if got := floorTo(c.ms, c.width); got != c.want {
			t.Errorf("floorTo(%d, %d) = %d, want %d", c.ms, c.width, got, c.want)
		}
	}
}

func TestPlanPartitions(t *testing.T) {
	const w = 60 // width
	cases := []struct {
		name     string
		existing []partition
		now, end int64
		want     [][2]int64
	}{
		{
			name: "fresh: aligned leaves from the current slot through the end slot",
			now:  125, end: 250,
			want: [][2]int64{{120, 180}, {180, 240}, {240, 300}},
		},
		{
			name:     "steady state: only the leaves past the newest existing one",
			existing: []partition{{lo: 120, hi: 180}, {lo: 180, hi: 240}},
			now:      130, end: 250,
			want: [][2]int64{{240, 300}},
		},
		{
			name:     "already covered",
			existing: []partition{{lo: 120, hi: 180}, {lo: 180, hi: 240}, {lo: 240, hi: 300}},
			now:      130, end: 250,
			want: nil,
		},
		{
			name:     "unaligned legacy leaf: the first new leaf starts at its bound",
			existing: []partition{{lo: math.MinInt64, hi: 150}},
			now:      125, end: 250,
			want: [][2]int64{{150, 180}, {180, 240}, {240, 300}},
		},
		{
			name:     "gap between leaves is filled, clipped to the next leaf",
			existing: []partition{{lo: 120, hi: 180}, {lo: 270, hi: 300}},
			now:      125, end: 250,
			want: [][2]int64{{180, 240}, {240, 270}},
		},
		{
			name:     "expired leaves before now are ignored",
			existing: []partition{{lo: 0, hi: 60}},
			now:      125, end: 170,
			want: [][2]int64{{120, 180}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := planPartitions(c.existing, c.now, c.end, w)
			if !slices.Equal(got, c.want) {
				t.Errorf("planPartitions = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRetentionResolve(t *testing.T) {
	r := Retention{}.resolve()
	if r.Message != 2*time.Minute || r.Persisted != 24*time.Hour {
		t.Errorf("defaults = %v / %v, want 2m / 24h", r.Message, r.Persisted)
	}
	if r.LivePartition != time.Minute || r.PersistedPartition != time.Hour {
		t.Errorf("derived widths = %v / %v, want 1m / 1h", r.LivePartition, r.PersistedPartition)
	}
	if r.MaintenanceInterval != 30*time.Second {
		t.Errorf("derived sweep = %v, want 30s", r.MaintenanceInterval)
	}
	r = Retention{Message: 10 * time.Minute, Persisted: 72 * time.Hour, MaintenanceInterval: 5 * time.Second}.resolve()
	if r.LivePartition != 5*time.Minute || r.PersistedPartition != time.Hour || r.MaintenanceInterval != 5*time.Second {
		t.Errorf("resolve = %+v", r)
	}
}
