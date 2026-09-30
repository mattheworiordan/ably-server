package loadgen

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// LoadRunRecords reads run summary.json files.
func LoadRunRecords(paths []string) ([]*RunRecord, error) {
	var out []*RunRecord
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var r RunRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if r.Version == 0 || r.RunID == "" {
			return nil, fmt.Errorf("%s: not a run summary", p)
		}
		out = append(out, &r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartUS < out[j].StartUS })
	return out, nil
}

// runMetrics are the per-run figures the report aggregates.
type runMetrics struct {
	writes, deliveries, conns       float64
	p50, p99, restP99, attachP99    float64 // ms
	violations                      int64
	vcpu100kConns, vcpu100kDel      float64
	vcpu10kWrites, used100kConns    float64
	used100kDel, used10kWrites, rss float64
}

func metricsOf(r *RunRecord) runMetrics {
	q := func(name string, p float64) float64 {
		h := r.Result.Latency[name]
		if h == nil || h.Count() == 0 {
			return math.NaN()
		}
		return float64(h.Quantile(p)) / 1000
	}
	del := LatDeliveryCrossNode
	if h := r.Result.Latency[del]; h == nil || h.Count() == 0 {
		del = LatDelivery
	}
	var v int64
	for _, n := range r.Result.Violations {
		v += n
	}
	f := r.Footprint
	return runMetrics{
		writes: r.Result.Publishes.AchievedRate, deliveries: r.Result.Deliveries.Rate,
		conns: float64(r.Result.Connections.OpenAtMeasureEnd),
		p50:   q(del, 0.5), p99: q(del, 0.99), restP99: q(LatRESTAck, 0.99), attachP99: q(LatConnectAttach, 0.99),
		violations:    v,
		vcpu100kConns: f.VCPUPer100kConns, vcpu100kDel: f.VCPUPer100kDeliveries, vcpu10kWrites: f.VCPUPer10kWrites,
		used100kConns: f.UsedPer100kConns, used100kDel: f.UsedPer100kDeliveries, used10kWrites: f.UsedPer10kWrites,
		rss: f.RSSGBPer100kConns,
	}
}

func fmtNum(v float64) string {
	switch {
	case math.IsNaN(v):
		return "n/a"
	case v >= 10000:
		return fmt.Sprintf("%.0fk", v/1000)
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2g", v)
	}
}

func fmtMS(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.1f ms", v)
}

// stats returns min, median and max of the non-NaN values.
func stats(vs []float64) (lo, med, hi float64, n int) {
	var xs []float64
	for _, v := range vs {
		if !math.IsNaN(v) {
			xs = append(xs, v)
		}
	}
	if len(xs) == 0 {
		return math.NaN(), math.NaN(), math.NaN(), 0
	}
	sort.Float64s(xs)
	m := xs[len(xs)/2]
	if len(xs)%2 == 0 {
		m = (xs[len(xs)/2-1] + xs[len(xs)/2]) / 2
	}
	return xs[0], m, xs[len(xs)-1], len(xs)
}

func spread(vs []float64) string {
	lo, med, hi, n := stats(vs)
	if n == 0 {
		return "n/a"
	}
	if n == 1 || med == 0 {
		return fmtNum(med)
	}
	return fmt.Sprintf("%s (%s to %s, ±%.0f%%)", fmtNum(med), fmtNum(lo), fmtNum(hi), (hi-lo)/2/med*100)
}

func spreadMS(vs []float64) string {
	lo, med, hi, n := stats(vs)
	if n == 0 {
		return "n/a"
	}
	if n == 1 {
		return fmtMS(med)
	}
	return fmt.Sprintf("%s (%s to %s)", fmtMS(med), fmtMS(lo), fmtMS(hi))
}

type groupKey struct {
	bus, shape string
	mult       float64
	nodes      int
	shards     int
}

func (k groupKey) String() string {
	s := fmt.Sprintf("%s %gx, bus %s, %d nodes", k.shape, k.mult, k.bus, k.nodes)
	if k.shards > 1 {
		s += fmt.Sprintf(", %d shards", k.shards)
	}
	return s
}

// Report renders the plan §13 tables from run records: every run; the
// envelope per deployment size (bus) and shape with repeats and spread;
// the footprint; and the node and shard curves. Only full-scale runs
// (scale 1) enter the envelope, footprint and curve tables; smoke runs
// are listed. Everything is keyed by shape name, so the output is
// anonymised by construction.
func Report(recs []*RunRecord) string {
	var b strings.Builder
	b.WriteString("# Scale proof results\n\n")
	fmt.Fprintf(&b, "%d runs. Latency is one-way, publisher to a subscriber on another node where there was such traffic. Spread is half the min-to-max range over the median.\n\n", len(recs))

	b.WriteString("## Runs\n\n| Run | Shape | Load | Scale | Bus | Nodes | Verdict | Connections | Writes/s | Deliveries/s | Delivery p50 | p99 | REST ACK p99 | Connect+attach p99 | Violations |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range recs {
		m := metricsOf(r)
		nodes := fmt.Sprint(r.Nodes)
		if r.Shards > 1 {
			nodes += fmt.Sprintf(" (%d shards)", r.Shards)
		}
		fmt.Fprintf(&b, "| %s | %s | %gx | %g | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
			r.RunID, r.Shape, r.Multiplier, r.Scale, r.Bus, nodes, r.Verdict, fmtNum(m.conns), fmtNum(m.writes), fmtNum(m.deliveries),
			fmtMS(m.p50), fmtMS(m.p99), fmtMS(m.restP99), fmtMS(m.attachP99), m.violations)
	}

	groups := map[groupKey][]*RunRecord{}
	var keys []groupKey
	for _, r := range recs {
		if r.Scale != 1 {
			continue
		}
		k := groupKey{bus: r.Bus, shape: r.Shape, mult: r.Multiplier, nodes: r.Nodes, shards: max(r.Shards, 1)}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], r)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, c := keys[i], keys[j]
		if a.bus != c.bus {
			return a.bus < c.bus
		}
		if a.shape != c.shape {
			return a.shape < c.shape
		}
		if a.mult != c.mult {
			return a.mult < c.mult
		}
		if a.nodes != c.nodes {
			return a.nodes < c.nodes
		}
		return a.shards < c.shards
	})
	if len(keys) == 0 {
		b.WriteString("\nNo full-scale runs yet: the envelope, footprint and curve tables need runs at scale 1.\n")
		return b.String()
	}

	col := func(rs []*RunRecord, f func(runMetrics) float64) []float64 {
		out := make([]float64, len(rs))
		for i, r := range rs {
			out[i] = f(metricsOf(r))
		}
		return out
	}
	b.WriteString("\n## Envelope by deployment size and shape\n\n| Configuration | Runs passed | Writes/s | Deliveries/s | Connections | Delivery p50 | Delivery p99 | REST ACK p99 | Connect+attach p99 |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, k := range keys {
		rs := groups[k]
		passed := 0
		for _, r := range rs {
			if r.Pass {
				passed++
			}
		}
		fmt.Fprintf(&b, "| %s | %d of %d | %s | %s | %s | %s | %s | %s | %s |\n", k, passed, len(rs),
			spread(col(rs, func(m runMetrics) float64 { return m.writes })),
			spread(col(rs, func(m runMetrics) float64 { return m.deliveries })),
			spread(col(rs, func(m runMetrics) float64 { return m.conns })),
			spreadMS(col(rs, func(m runMetrics) float64 { return m.p50 })),
			spreadMS(col(rs, func(m runMetrics) float64 { return m.p99 })),
			spreadMS(col(rs, func(m runMetrics) float64 { return m.restP99 })),
			spreadMS(col(rs, func(m runMetrics) float64 { return m.attachP99 })))
	}

	b.WriteString("\n## Footprint (median across repeats)\n\nProvisioned is the nodes' vCPU; used is their measured CPU over the hold. Node memory is RSS at the end of the hold.\n\n| Configuration | vCPU per 100k connections | per 100k deliveries/s | per 10k writes/s | Used cores per 100k connections | per 100k deliveries/s | per 10k writes/s | RSS GB per 100k connections |\n|---|---|---|---|---|---|---|---|\n")
	med := func(rs []*RunRecord, f func(runMetrics) float64) string {
		_, m, _, n := stats(col(rs, func(x runMetrics) float64 {
			v := f(x)
			if v == 0 {
				return math.NaN()
			}
			return v
		}))
		if n == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.2f", m)
	}
	for _, k := range keys {
		rs := groups[k]
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n", k,
			med(rs, func(m runMetrics) float64 { return m.vcpu100kConns }),
			med(rs, func(m runMetrics) float64 { return m.vcpu100kDel }),
			med(rs, func(m runMetrics) float64 { return m.vcpu10kWrites }),
			med(rs, func(m runMetrics) float64 { return m.used100kConns }),
			med(rs, func(m runMetrics) float64 { return m.used100kDel }),
			med(rs, func(m runMetrics) float64 { return m.used10kWrites }),
			med(rs, func(m runMetrics) float64 { return m.rss }))
	}

	// Curves: the same shape, bus and load at different node or shard
	// counts.
	curve := func(title, axis string, vary func(groupKey) int, same func(a, c groupKey) bool) {
		type row struct {
			k  groupKey
			rs []*RunRecord
		}
		var families [][]row
		for _, k := range keys {
			placed := false
			for i := range families {
				if same(families[i][0].k, k) {
					families[i] = append(families[i], row{k, groups[k]})
					placed = true
					break
				}
			}
			if !placed {
				families = append(families, []row{{k, groups[k]}})
			}
		}
		wrote := false
		for _, fam := range families {
			if len(fam) < 2 {
				continue
			}
			if !wrote {
				fmt.Fprintf(&b, "\n## %s\n\n| Shape, load, bus | %s | Writes/s | Deliveries/s | Connections | Delivery p99 | Runs passed |\n|---|---|---|---|---|---|---|\n", title, axis)
				wrote = true
			}
			sort.Slice(fam, func(i, j int) bool { return vary(fam[i].k) < vary(fam[j].k) })
			for _, rw := range fam {
				passed := 0
				for _, r := range rw.rs {
					if r.Pass {
						passed++
					}
				}
				fmt.Fprintf(&b, "| %s %gx, %s | %d | %s | %s | %s | %s | %d of %d |\n", rw.k.shape, rw.k.mult, rw.k.bus, vary(rw.k),
					spread(col(rw.rs, func(m runMetrics) float64 { return m.writes })),
					spread(col(rw.rs, func(m runMetrics) float64 { return m.deliveries })),
					spread(col(rw.rs, func(m runMetrics) float64 { return m.conns })),
					spreadMS(col(rw.rs, func(m runMetrics) float64 { return m.p99 })), passed, len(rw.rs))
			}
		}
	}
	curve("Node curve", "Nodes", func(k groupKey) int { return k.nodes }, func(a, c groupKey) bool {
		return a.shape == c.shape && a.bus == c.bus && a.mult == c.mult && a.shards == c.shards
	})
	curve("Shard curve", "Shards", func(k groupKey) int { return k.shards }, func(a, c groupKey) bool {
		return a.shape == c.shape && a.bus == c.bus && a.mult == c.mult && a.nodes == c.nodes
	})
	fmt.Fprintf(&b, "\nGenerated %s from %d run records.\n", time.Now().UTC().Format(time.RFC3339), len(recs))
	return b.String()
}
