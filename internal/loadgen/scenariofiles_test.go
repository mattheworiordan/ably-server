package loadgen

import (
	"math"
	"path/filepath"
	"testing"
)

// The committed scenario files must parse, resolve at every multiplier
// and scale the runs use, and stay on the plan §3 envelope.
func TestScenarioFilesMatchThePlanTargets(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "scenarios")
	near := func(t *testing.T, what string, got, want, tol float64) {
		t.Helper()
		if math.Abs(got-want) > tol*want {
			t.Errorf("%s = %.0f, want %.0f ± %.0f%%", what, got, want, tol*100)
		}
	}
	type target struct {
		conns, writes, deliveries float64
		minFanOut, maxFanOut      float64
	}
	cases := map[string]target{
		"shape-m.toml": {conns: 550000, writes: 7000, deliveries: 120000, minFanOut: 15, maxFanOut: 20},
		"shape-d.toml": {conns: 330000, writes: 52000, deliveries: 33400, minFanOut: 0.6, maxFanOut: 0.7},
		"shape-f.toml": {conns: 120000, writes: 46000, deliveries: 104000, minFanOut: 2, maxFanOut: 2.6},
	}
	for file, want := range cases {
		t.Run(file, func(t *testing.T) {
			sc, err := LoadScenario(filepath.Join(dir, file))
			if err != nil {
				t.Fatal(err)
			}
			for _, mult := range []float64{1, 2} {
				p, err := sc.Resolve(mult, 1, "t")
				if err != nil {
					t.Fatal(err)
				}
				tot, _ := p.Totals()
				near(t, "connections", float64(tot.Connections), want.conns*mult, 0.001)
				near(t, "publishes/s", tot.PublishesPerSec, want.writes*mult, 0.02)
				near(t, "deliveries/s", tot.DeliveriesPerSec, want.deliveries*mult, 0.08)
				if tot.FanOut < want.minFanOut || tot.FanOut > want.maxFanOut*1.01 && mult == 1 {
					t.Errorf("%vx fan-out %.2f outside [%v, %v]", mult, tot.FanOut, want.minFanOut, want.maxFanOut)
				}
				if tot.MaxStreamRate > 50 {
					t.Errorf("%vx: a stream at %.0f/s is too fast for one sequential REST publisher", mult, tot.MaxStreamRate)
				}
			}
			for _, scale := range []float64{0.01, 0.1} {
				p, err := sc.Resolve(1, scale, "t")
				if err != nil {
					t.Fatal(err)
				}
				tot, _ := p.Totals()
				near(t, "smoke connections", float64(tot.Connections), want.conns*scale, 0.01)
				if tot.SampledChannels == 0 {
					t.Error("smoke plan samples no channel")
				}
			}
		})
	}
	sc, err := LoadScenario(filepath.Join(dir, "presence-m.toml"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := sc.Resolve(1, 1, "t")
	if err != nil {
		t.Fatal(err)
	}
	if tot, _ := p.Totals(); tot.PresenceSampled < 2 || !p.PresenceSampled(0) {
		t.Errorf("presence-m must compare the member sets of the first channel and a sample: %+v", tot)
	}
	if tot, _ := p.Totals(); tot.PresenceMembers != 1000000 || tot.PresenceEventsPerS != 1200 {
		t.Errorf("presence totals %+v", tot)
	}
}

// The smoke scenarios are shapes M and D together at 1% and 10%: their
// classes must stay copies of the shape files'.
func TestSmokeScenariosAreShapesMAndD(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "scenarios")
	m, err := LoadScenario(filepath.Join(dir, "shape-m.toml"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := LoadScenario(filepath.Join(dir, "shape-d.toml"))
	if err != nil {
		t.Fatal(err)
	}
	var want []Class
	for _, c := range m.Classes {
		c.MessageBytes = m.MessageBytes
		want = append(want, c)
	}
	for _, c := range d.Classes {
		if c.Name == "hot" {
			c.Name = "hotpub"
		}
		c.MessageBytes = d.MessageBytes
		want = append(want, c)
	}
	for file, scale := range map[string]float64{"smoke-1pct.toml": 0.01, "smoke-10pct.toml": 0.1} {
		s, err := LoadScenario(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		if s.Scale != scale || s.Connections != m.Connections || s.Churn != m.Churn {
			t.Errorf("%s: scale %v connections %+v churn %+v", file, s.Scale, s.Connections, s.Churn)
		}
		if len(s.Classes) != len(want) {
			t.Fatalf("%s: %d classes, want %d", file, len(s.Classes), len(want))
		}
		for i := range want {
			if s.Classes[i] != want[i] {
				t.Errorf("%s class %d = %+v, want %+v", file, i, s.Classes[i], want[i])
			}
		}
		p, err := s.Resolve(0, 0, "t")
		if err != nil {
			t.Fatal(err)
		}
		tot, _ := p.Totals()
		if math.Abs(float64(tot.Connections)-550000*scale) > 1 || math.Abs(tot.PublishesPerSec-59000*scale)/(59000*scale) > 0.03 {
			t.Errorf("%s: %d connections, %.0f publishes/s", file, tot.Connections, tot.PublishesPerSec)
		}
	}
}
