package loadgen

import (
	"math"
	"path/filepath"
	"testing"
)

// The committed scenario files must parse, resolve at every multiplier
// and scale the runs use, and stay on the plan §3 envelope.
func TestScenarioFilesMatchThePlanTargets(t *testing.T) {
	dir := filepath.Join("..", "..", "bench", "aws", "scenarios")
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
	if tot, _ := p.Totals(); tot.PresenceMembers != 1000000 || tot.PresenceEventsPerS != 1200 {
		t.Errorf("presence totals %+v", tot)
	}
}
