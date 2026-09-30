package loadgen

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

func churnJob(t *testing.T, index, count int) *Job {
	t.Helper()
	sc, err := ParseScenario([]byte(strings.Replace(testScenario, "resume = true", "resume = true\nchannel_lifetime = \"10s\"", 1)))
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewJob(JobSpec{ID: "s", RunTag: "t", Scenario: *sc, Role: RoleSubscriber, Index: index, Count: count, Endpoints: []string{"x:1"}, Key: "a.b:c"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestChurnSlotsAreSpreadAndCounted(t *testing.T) {
	j := churnJob(t, 0, 1)
	p := j.Plan
	if p.ChurnSlots != 200 { // 20 opens/s x 10s lifetime
		t.Fatalf("churn slots %d, want 200", p.ChurnSlots)
	}
	holders := 0
	for c := 0; c < p.Connections; c++ {
		if p.ChurnSlot(c) {
			holders++
		}
	}
	if holders != p.ChurnSlots {
		t.Fatalf("%d holders, want %d", holders, p.ChurnSlots)
	}
	// Spread: every block of 5 connections has one holder.
	for c := 0; c < p.Connections; c += 5 {
		n := 0
		for k := c; k < c+5; k++ {
			if p.ChurnSlot(k) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("connections %d-%d hold %d slots", c, c+4, n)
		}
	}
	// Across processes the holders partition too.
	total := 0
	for i := 0; i < 3; i++ {
		for _, cp := range p.SubscriberSlice(i, 3) {
			if cp.ChurnSlot {
				total++
			}
		}
	}
	if total != p.ChurnSlots {
		t.Fatalf("holders across 3 processes %d, want %d", total, p.ChurnSlots)
	}
}

// After the ramp, channel churn replaces and never adds: the number of
// attachments the generator holds stays exactly the plan's
// attachments plus its churn slots, whatever the number of opens.
func TestChurnKeepsAttachmentsConstantAfterRamp(t *testing.T) {
	for _, procs := range []int{1, 3} {
		var total int64
		var holders []*subSession
		var jobs []*Job
		for i := 0; i < procs; i++ {
			j := churnJob(t, i, procs)
			jobs = append(jobs, j)
			for _, s := range j.newSubSessions() {
				// The ramp: every attachment attached.
				for _, a := range s.atts {
					a.state = attAttached
					j.trackAttached(1)
				}
				if s.slot {
					holders = append(holders, s)
				}
			}
			total += j.c.attached.Load()
		}
		p := jobs[0].Plan
		want := p.Attachments + int64(p.ChurnSlots)
		if total != want {
			t.Fatalf("%d procs: %d attachments after ramp, want %d (plan %d + %d slots)", procs, total, want, p.Attachments, p.ChurnSlots)
		}
		r := rand.New(rand.NewPCG(7, 9))
		for k := 0; k < 20000; k++ {
			s := holders[r.IntN(len(holders))]
			s.mu.Lock()
			old, a := s.rotateChurn(fmt.Sprintf("new-%d", k))
			if old == nil {
				t.Fatal("a slot holder had no churn channel to replace")
			}
			// Its ATTACHED arrives.
			a.state = attAttached
			s.j.trackAttached(1)
			s.mu.Unlock()
			if k%1000 == 0 {
				var now int64
				for _, j := range jobs {
					now += j.c.attached.Load()
				}
				if now != want {
					t.Fatalf("%d procs: after %d opens %d attachments, want %d", procs, k+1, now, want)
				}
			}
		}
		for _, s := range holders {
			n := 0
			for _, a := range s.atts {
				if a.churn {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("holder %d has %d churn channels", s.global, n)
			}
		}
	}
}

func TestChurnSkipsNonHolders(t *testing.T) {
	j := churnJob(t, 0, 1)
	for _, s := range j.newSubSessions() {
		if !s.slot {
			before := len(s.atts)
			s.openChurnChannel("x")
			if len(s.atts) != before || s.churnAtt != nil {
				t.Fatal("a non-holder gained a churn channel")
			}
			return
		}
	}
}
