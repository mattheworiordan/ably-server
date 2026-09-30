package loadgen

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestHistogramBucketBoundsAreConsistent(t *testing.T) {
	for i := 0; i < histBuckets; i++ {
		lo, hi := histLowerBound(i), histUpperBound(i)
		if lo > hi {
			t.Fatalf("bucket %d: lower %d > upper %d", i, lo, hi)
		}
		if histIndex(lo) != i || histIndex(hi) != i {
			t.Fatalf("bucket %d [%d,%d]: index(lo)=%d index(hi)=%d", i, lo, hi, histIndex(lo), histIndex(hi))
		}
		if i > 0 && histUpperBound(i-1)+1 != lo {
			t.Fatalf("bucket %d not contiguous with %d", i, i-1)
		}
		if lo >= histSubBuckets && float64(hi-lo+1)/float64(lo) > 1.0/64+1e-9 {
			t.Fatalf("bucket %d [%d,%d] wider than 1/64 of its value", i, lo, hi)
		}
	}
}

func TestHistogramExactSmallValues(t *testing.T) {
	h := NewHistogram()
	for v := int64(1); v <= 100; v++ {
		h.RecordMicros(v)
	}
	if h.Count() != 100 || h.Min() != 1 || h.Max() != 100 {
		t.Fatalf("count/min/max = %d/%d/%d", h.Count(), h.Min(), h.Max())
	}
	if q := h.Quantile(0.5); q != 50 {
		t.Errorf("p50 = %d, want 50", q)
	}
	if q := h.Quantile(0.99); q != 99 {
		t.Errorf("p99 = %d, want 99", q)
	}
	if m := h.Mean(); m != 50.5 {
		t.Errorf("mean = %v", m)
	}
}

func TestHistogramQuantileErrorBound(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	h := NewHistogram()
	vals := make([]int64, 200000)
	for i := range vals {
		// Log-normal-ish latencies from ~100µs to seconds.
		v := int64(math.Exp(r.NormFloat64()*1.5 + 8))
		vals[i] = v
		h.RecordMicros(v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := vals[int(math.Ceil(q*float64(len(vals))))-1]
		got := int64(h.Quantile(q))
		if rel := math.Abs(float64(got-want)) / float64(max(want, 1)); rel > 1.0/64 {
			t.Errorf("q%.3f: got %d want %d (rel err %.4f)", q, got, want, rel)
		}
	}
	if int64(h.Max()) != vals[len(vals)-1] {
		t.Errorf("max %d want %d", h.Max(), vals[len(vals)-1])
	}
}

func TestHistogramMergeAndJSON(t *testing.T) {
	a, b, all := NewHistogram(), NewHistogram(), NewHistogram()
	for i := int64(0); i < 5000; i++ {
		v := i * 37 % 100000
		if i%3 == 0 {
			a.RecordMicros(v)
		} else {
			b.RecordMicros(v)
		}
		all.RecordMicros(v)
	}
	m := NewHistogram()
	m.Merge(a)
	m.Merge(b)
	if m.Snapshot().P99US != all.Snapshot().P99US || m.Count() != all.Count() || m.Min() != all.Min() || m.Max() != all.Max() {
		t.Fatalf("merge differs: %+v vs %+v", m.Snapshot(), all.Snapshot())
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back Histogram
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	bs, ms := back.Snapshot(), m.Snapshot()
	if bs.Count != ms.Count || bs.P50US != ms.P50US || bs.P999US != ms.P999US || bs.MinUS != ms.MinUS || bs.MaxUS != ms.MaxUS || bs.SumUS != ms.SumUS {
		t.Fatalf("json round trip differs: %+v vs %+v", bs, ms)
	}
	// An empty histogram round-trips and merges as a no-op.
	var empty Histogram
	raw, _ = json.Marshal(NewHistogram())
	if err := json.Unmarshal(raw, &empty); err != nil {
		t.Fatal(err)
	}
	m.Merge(&empty)
	if m.Count() != all.Count() {
		t.Fatal("merging empty changed count")
	}
}

func TestHistogramConcurrentRecord(t *testing.T) {
	h := NewHistogram()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				h.Record(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if h.Count() != 80000 || h.Max() != 9999 || h.Min() != 0 {
		t.Fatalf("count=%d max=%d min=%d", h.Count(), h.Max(), h.Min())
	}
}

func TestHistogramNegativeAndHuge(t *testing.T) {
	h := NewHistogram()
	h.RecordMicros(-5)
	h.RecordMicros(math.MaxInt64)
	if h.Min() != 0 || h.Count() != 2 {
		t.Fatalf("min=%d count=%d", h.Min(), h.Count())
	}
	if h.Quantile(1) == 0 {
		t.Fatal("huge value lost")
	}
}
