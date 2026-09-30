package loadgen

import (
	"encoding/json"
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram is a log-linear latency histogram over whole microseconds,
// in the style of HDR histograms: values below histSubBuckets are exact,
// and above that every power of two is split into histSubBuckets/2
// linear sub-buckets, so any recorded value is reported within 1/64
// (about 1.6%) of its true value. It covers 0 to 2^40 µs (about 12 days),
// which is far more than any latency a run can observe; larger values are
// clamped into the top bucket and still counted.
//
// Record is lock-free and safe for concurrent use. Histograms from
// different goroutines or processes merge exactly (Merge, and the JSON
// form carries the raw buckets), which is how the conductor combines
// generator processes into one distribution instead of averaging their
// percentiles.
type Histogram struct {
	counts [histBuckets]atomic.Uint64
	count  atomic.Uint64
	sum    atomic.Uint64
	min    atomic.Uint64 // stored as value+1 so 0 means "unset"
	max    atomic.Uint64
}

const (
	histSubBits    = 7
	histSubBuckets = 1 << histSubBits // 128
	histHalf       = histSubBuckets / 2
	histMaxExp     = 40
	histBuckets    = histSubBuckets + (histMaxExp-histSubBits)*histHalf
	histMaxValue   = uint64(1)<<histMaxExp - 1
)

// NewHistogram returns an empty histogram.
func NewHistogram() *Histogram { return &Histogram{} }

func histIndex(v uint64) int {
	if v > histMaxValue {
		v = histMaxValue
	}
	if v < histSubBuckets {
		return int(v)
	}
	exp := bits.Len64(v) - 1 // v in [2^exp, 2^(exp+1)), exp >= histSubBits
	shift := exp - histSubBits + 1
	sub := int(v>>shift) - histHalf // in [0, histHalf)
	return histSubBuckets + (exp-histSubBits)*histHalf + sub
}

// histLowerBound returns the smallest value that maps to bucket i.
func histLowerBound(i int) uint64 {
	if i < histSubBuckets {
		return uint64(i)
	}
	j := i - histSubBuckets
	exp := j/histHalf + histSubBits
	sub := j % histHalf
	shift := exp - histSubBits + 1
	return uint64(sub+histHalf) << shift
}

// histUpperBound returns the largest value that maps to bucket i.
func histUpperBound(i int) uint64 {
	if i+1 >= histBuckets {
		return histMaxValue
	}
	return histLowerBound(i+1) - 1
}

// RecordMicros adds one observation of us microseconds. Negative values
// (a receiver clock behind the sender's) are recorded as 0; callers that
// care count them separately.
func (h *Histogram) RecordMicros(us int64) {
	if us < 0 {
		us = 0
	}
	v := uint64(us)
	h.counts[histIndex(v)].Add(1)
	h.count.Add(1)
	h.sum.Add(v)
	h.lowerMin(v)
	h.raiseMax(v)
}

// lowerMin sets the recorded minimum to v if v is smaller.
func (h *Histogram) lowerMin(v uint64) {
	enc := v + 1
	for {
		cur := h.min.Load()
		if cur != 0 && cur <= enc {
			return
		}
		if h.min.CompareAndSwap(cur, enc) {
			return
		}
	}
}

// raiseMax sets the recorded maximum to v if v is larger.
func (h *Histogram) raiseMax(v uint64) {
	for {
		cur := h.max.Load()
		if cur >= v {
			return
		}
		if h.max.CompareAndSwap(cur, v) {
			return
		}
	}
}

// Record adds one observation of d.
func (h *Histogram) Record(d time.Duration) { h.RecordMicros(d.Microseconds()) }

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Max returns the largest observation in microseconds (0 when empty).
func (h *Histogram) Max() uint64 { return h.max.Load() }

// Min returns the smallest observation in microseconds (0 when empty).
func (h *Histogram) Min() uint64 {
	m := h.min.Load()
	if m == 0 {
		return 0
	}
	return m - 1
}

// Mean returns the mean observation in microseconds (0 when empty).
func (h *Histogram) Mean() float64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	return float64(h.sum.Load()) / float64(n)
}

// Quantile returns the value at quantile q (0..1) in microseconds: the
// upper bound of the bucket holding the ceil(q*count)-th observation,
// clamped to the observed min and max so an exact low range and the
// true maximum are reported exactly. Returns 0 when empty.
func (h *Histogram) Quantile(q float64) uint64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	if q <= 0 {
		return h.Min()
	}
	if q >= 1 {
		return h.Max()
	}
	rank := uint64(math.Ceil(q * float64(n)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i := range h.counts {
		seen += h.counts[i].Load()
		if seen >= rank {
			v := histUpperBound(i)
			return min(max(v, h.Min()), h.Max())
		}
	}
	return h.Max()
}

// QuantileDuration is Quantile as a time.Duration.
func (h *Histogram) QuantileDuration(q float64) time.Duration {
	return time.Duration(h.Quantile(q)) * time.Microsecond
}

// Merge adds every observation in o to h.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil || o.count.Load() == 0 {
		return
	}
	for i := range o.counts {
		if c := o.counts[i].Load(); c != 0 {
			h.counts[i].Add(c)
		}
	}
	h.count.Add(o.count.Load())
	h.sum.Add(o.sum.Load())
	h.lowerMin(o.Min())
	h.raiseMax(o.Max())
}

// HistogramJSON is the serialised form: summary statistics for humans and
// the sparse raw buckets for exact merging.
type HistogramJSON struct {
	Count   uint64      `json:"count"`
	MinUS   uint64      `json:"min_us"`
	MaxUS   uint64      `json:"max_us"`
	MeanUS  float64     `json:"mean_us"`
	P50US   uint64      `json:"p50_us"`
	P90US   uint64      `json:"p90_us"`
	P99US   uint64      `json:"p99_us"`
	P999US  uint64      `json:"p999_us"`
	SumUS   uint64      `json:"sum_us"`
	Buckets [][2]uint64 `json:"buckets,omitempty"` // [bucket index, count]
}

// Snapshot returns the serialisable form of h.
func (h *Histogram) Snapshot() HistogramJSON {
	s := HistogramJSON{
		Count:  h.Count(),
		MinUS:  h.Min(),
		MaxUS:  h.Max(),
		MeanUS: math.Round(h.Mean()*10) / 10,
		P50US:  h.Quantile(0.50),
		P90US:  h.Quantile(0.90),
		P99US:  h.Quantile(0.99),
		P999US: h.Quantile(0.999),
		SumUS:  h.sum.Load(),
	}
	for i := range h.counts {
		if c := h.counts[i].Load(); c != 0 {
			s.Buckets = append(s.Buckets, [2]uint64{uint64(i), c})
		}
	}
	return s
}

// MarshalJSON encodes the snapshot.
func (h *Histogram) MarshalJSON() ([]byte, error) { return json.Marshal(h.Snapshot()) }

// UnmarshalJSON restores a histogram from its snapshot, so it can be
// merged with others.
func (h *Histogram) UnmarshalJSON(b []byte) error {
	var s HistogramJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	h.reset()
	h.LoadSnapshot(s)
	return nil
}

func (h *Histogram) reset() {
	for i := range h.counts {
		h.counts[i].Store(0)
	}
	h.count.Store(0)
	h.sum.Store(0)
	h.min.Store(0)
	h.max.Store(0)
}

// LoadSnapshot adds the observations in s to h.
func (h *Histogram) LoadSnapshot(s HistogramJSON) {
	var n uint64
	for _, b := range s.Buckets {
		if b[0] >= histBuckets {
			continue
		}
		h.counts[b[0]].Add(b[1])
		n += b[1]
	}
	if n == 0 {
		return
	}
	h.count.Add(n)
	h.sum.Add(s.SumUS)
	h.lowerMin(s.MinUS)
	h.raiseMax(s.MaxUS)
}
