package loadgen

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testScenario = `
name = "T"
shape = "M"
message_bytes = 200
sample_percent = 5

[timing]
ramp = "10s"
hold = "20s"
drain = "5s"

[connections]
count = 1000

[churn]
connects_per_sec = 10
channel_opens_per_sec = 20
resume = true

[[class]]
name = "hot"
channels = 1
subscribers = 800
publish_rate = 2
publisher = "realtime"
streams = 2
scale_by = "subscribers"

[[class]]
name = "rooms"
channels = 20
subscribers_min = 10
subscribers_max = 100
subscribers_dist = "harmonic"
publish_rate = 0.5
scale_by = "subscribers"

[[class]]
name = "tail"
channels = 3000
subscribers_min = 0
subscribers_max = 2
subscribers_dist = "uniform"
publish_rate_total = 300

[[class]]
name = "hotpub"
channels = 1
subscribers = 3
publish_rate = 100
streams = 4
scale_by = "rate"
`

func mustPlan(t *testing.T, mult, scale float64) *Plan {
	t.Helper()
	sc, err := ParseScenario([]byte(testScenario))
	if err != nil {
		t.Fatal(err)
	}
	p, err := sc.Resolve(mult, scale, "tag1")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestScenarioRejectsUnknownKeysAndBadValues(t *testing.T) {
	if _, err := ParseScenario([]byte(testScenario + "\nbogus = 1\n")); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("unknown key accepted: %v", err)
	}
	bad := strings.Replace(testScenario, `subscribers_dist = "harmonic"`, `subscribers_dist = "pareto"`, 1)
	if _, err := ParseScenario([]byte(bad)); err == nil {
		t.Error("bad distribution accepted")
	}
	bad = strings.Replace(testScenario, `publisher = "realtime"`, `publisher = "carrier-pigeon"`, 1)
	if _, err := ParseScenario([]byte(bad)); err == nil {
		t.Error("bad publisher accepted")
	}
	sc, _ := ParseScenario([]byte(testScenario))
	if _, err := sc.Resolve(1, 1, "bad-tag"); err == nil {
		t.Error("run tag with '-' accepted")
	}
	// More subscribers on one channel than connections is impossible at
	// full scale, and clamped (and reported) at a smoke scale.
	sc.Classes[0].Subscribers = 5000
	sc.Classes[0].ScaleBy = "rate"
	if _, err := sc.Resolve(1, 1, "t"); err == nil {
		t.Error("channel with more subscribers than connections accepted")
	}
	p, err := sc.Resolve(1, 0.1, "t")
	if err != nil {
		t.Fatal(err)
	}
	if _, ct := p.Totals(); ct[0].MaxSubscribers != p.Connections || ct[0].Clamped != 1 {
		t.Errorf("smoke clamp: %+v (connections %d)", ct[0], p.Connections)
	}
}

func TestPlanScaling(t *testing.T) {
	p1 := mustPlan(t, 1, 1)
	p2 := mustPlan(t, 2, 1)
	ps := mustPlan(t, 1, 0.1)
	t1, c1 := p1.Totals()
	t2, c2 := p2.Totals()
	ts, _ := ps.Totals()
	if t2.Connections != 2*t1.Connections || ts.Connections != t1.Connections/10 {
		t.Errorf("connections %d %d %d", t1.Connections, t2.Connections, ts.Connections)
	}
	// hot: subscribers scale, channel count and rate do not.
	if c1[0].Channels != 1 || c2[0].Channels != 1 || c2[0].MaxSubscribers != 2*c1[0].MaxSubscribers {
		t.Errorf("hot class: %+v vs %+v", c1[0], c2[0])
	}
	// tail: channels scale; class total rate scales.
	if c2[2].Channels != 2*c1[2].Channels || math.Abs(c2[2].PublishesPerSec-2*c1[2].PublishesPerSec) > 1e-6 {
		t.Errorf("tail class: %+v vs %+v", c1[2], c2[2])
	}
	// hotpub: rate scales.
	if c2[3].PublishesPerSec != 2*c1[3].PublishesPerSec || c2[3].MaxSubscribers != c1[3].MaxSubscribers {
		t.Errorf("hotpub class: %+v vs %+v", c1[3], c2[3])
	}
	// Classes scaled by subscribers keep their publish rate (their 2x
	// load is fan-out); the others double.
	wantPub := c1[0].PublishesPerSec + c1[1].PublishesPerSec + 2*(c1[2].PublishesPerSec+c1[3].PublishesPerSec)
	if math.Abs(t2.PublishesPerSec-wantPub) > 1e-6 {
		t.Errorf("publishes %v, want %v", t2.PublishesPerSec, wantPub)
	}
	if math.Abs(t2.DeliveriesPerSec-2*t1.DeliveriesPerSec)/t1.DeliveriesPerSec > 0.05 {
		t.Errorf("deliveries %v vs %v", t1.DeliveriesPerSec, t2.DeliveriesPerSec)
	}
	if t2.ConnectsPerSec != 20 || t2.ChannelOpensPerSec != 40 {
		t.Errorf("churn %v %v", t2.ConnectsPerSec, t2.ChannelOpensPerSec)
	}
	// harmonic: channel 0 at the max, the tail at the min.
	if c1[1].MaxSubscribers != 100 || c1[1].MinSubscribers != 10 {
		t.Errorf("harmonic range %+v", c1[1])
	}
	if t1.MaxStreamRate != 25 {
		t.Errorf("max stream rate %v, want 25 (100/s over 4 streams)", t1.MaxStreamRate)
	}
}

func TestSubscriberSliceIsAPartition(t *testing.T) {
	p := mustPlan(t, 1, 1)
	for _, procs := range []int{1, 3, 7} {
		perChannel := map[string]int{}
		conns := map[int]bool{}
		for i := 0; i < procs; i++ {
			for _, cp := range p.SubscriberSlice(i, procs) {
				if cp.Global%procs != i {
					t.Fatalf("conn %d in process %d of %d", cp.Global, i, procs)
				}
				if conns[cp.Global] {
					t.Fatalf("conn %d assigned twice", cp.Global)
				}
				conns[cp.Global] = true
				seen := map[string]bool{}
				for _, a := range cp.Attach {
					if seen[a.Channel] {
						t.Fatalf("conn %d attaches %s twice", cp.Global, a.Channel)
					}
					seen[a.Channel] = true
					perChannel[a.Channel]++
				}
			}
		}
		if len(conns) != p.Connections {
			t.Fatalf("%d procs: %d connections, want %d", procs, len(conns), p.Connections)
		}
		var total int64
		for ci := range p.Classes {
			rc := &p.Classes[ci]
			for j := 0; j < rc.ChannelCount; j++ {
				want := p.Subscribers(rc, j)
				if got := perChannel[p.ChannelName(rc, j)]; got != want {
					t.Fatalf("%d procs: %s has %d subscribers, want %d", procs, p.ChannelName(rc, j), got, want)
				}
				total += int64(want)
			}
		}
		if total != p.Attachments {
			t.Fatalf("attachments %d vs plan %d", total, p.Attachments)
		}
	}
}

func TestSubscriberSliceDeterministic(t *testing.T) {
	a := mustPlan(t, 1, 1).SubscriberSlice(1, 3)
	b := mustPlan(t, 1, 1).SubscriberSlice(1, 3)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two resolutions of the same inputs differ")
	}
}

func TestPublisherSliceIsAPartition(t *testing.T) {
	p := mustPlan(t, 1, 1)
	for _, transport := range []string{"rest", "realtime"} {
		for _, procs := range []int{1, 2, 5} {
			streams := map[string]int{}
			var rate float64
			for i := 0; i < procs; i++ {
				for _, cs := range p.PublisherSlice(transport, i, procs) {
					if p.Classes[cs.Class].Publisher != transport {
						t.Fatalf("class %d in %s slice", cs.Class, transport)
					}
					rate += cs.TotalShare
					for _, s := range cs.Streams {
						streams[s.Channel+"|"+s.PubID]++
					}
				}
			}
			var wantStreams int
			var wantRate float64
			for ci := range p.Classes {
				rc := &p.Classes[ci]
				if rc.Publisher == transport && rc.RatePerChannel > 0 {
					wantStreams += rc.ChannelCount * rc.StreamCount
					wantRate += rc.RatePerChannel * float64(rc.ChannelCount)
				}
			}
			if len(streams) != wantStreams {
				t.Fatalf("%s %d procs: %d streams, want %d", transport, procs, len(streams), wantStreams)
			}
			for k, n := range streams {
				if n != 1 {
					t.Fatalf("stream %s owned %d times", k, n)
				}
			}
			if math.Abs(rate-wantRate) > 1e-6 {
				t.Fatalf("%s %d procs: rate %v want %v", transport, procs, rate, wantRate)
			}
		}
	}
}

func TestSampledChannels(t *testing.T) {
	p := mustPlan(t, 1, 1)
	for ci := range p.Classes {
		if !p.Sampled(&p.Classes[ci], 0) {
			t.Errorf("first channel of class %s not sampled", p.Classes[ci].Name)
		}
	}
	tail := &p.Classes[2]
	n := 0
	for j := 0; j < tail.ChannelCount; j++ {
		if p.Sampled(tail, j) {
			n++
		}
	}
	frac := float64(n) / float64(tail.ChannelCount)
	if frac < 0.03 || frac > 0.07 {
		t.Errorf("sampled fraction %.3f, want about 0.05", frac)
	}
	// Sampling agrees between the subscriber and publisher views.
	subSampled := map[string]bool{}
	for _, cp := range p.SubscriberSlice(0, 1) {
		for _, a := range cp.Attach {
			subSampled[a.Channel] = a.Sampled
		}
	}
	for _, cs := range p.PublisherSlice("rest", 0, 1) {
		for _, s := range cs.Streams {
			if v, ok := subSampled[s.Channel]; ok && v != s.Sampled {
				t.Fatalf("channel %s sampled=%v for publisher, %v for subscriber", s.Channel, s.Sampled, v)
			}
		}
	}
}

func TestRunTagIsolatesRuns(t *testing.T) {
	sc, _ := ParseScenario([]byte(testScenario))
	a, _ := sc.Resolve(1, 1, "runA")
	b, _ := sc.Resolve(1, 1, "runB")
	if a.ChannelName(&a.Classes[0], 0) == b.ChannelName(&b.Classes[0], 0) {
		t.Error("channel names shared across runs")
	}
	if MessageID(a.PubID(0), 1) == MessageID(b.PubID(0), 1) {
		t.Error("message ids shared across runs")
	}
}

func TestPresenceSlice(t *testing.T) {
	sc, _ := ParseScenario([]byte(testScenario + `
[presence]
enabled = true
channels = 4
members_per_channel = 5
events_per_sec = 10
`))
	p, err := sc.Resolve(1, 1, "t")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		for _, m := range p.PresenceSlice(i, 3) {
			if seen[m.ClientID] {
				t.Fatalf("member %s twice", m.ClientID)
			}
			seen[m.ClientID] = true
		}
	}
	if len(seen) != 20 {
		t.Fatalf("%d members, want 20", len(seen))
	}
}

// TestScenarioServerIdleTimeout: server_idle_timeout reads from TOML and
// sets the growth baseline; absent (or "0s") it is the server default.
func TestScenarioServerIdleTimeout(t *testing.T) {
	sc, err := ParseScenario([]byte(testScenario))
	if err != nil {
		t.Fatal(err)
	}
	if got := sc.IdleTimeout(); got != DefaultServerIdleTimeout {
		t.Errorf("absent: IdleTimeout = %s, want %s", got, DefaultServerIdleTimeout)
	}
	for toml, want := range map[string]time.Duration{
		`server_idle_timeout = "15s"`: 15 * time.Second,
		`server_idle_timeout = "0s"`:  DefaultServerIdleTimeout,
	} {
		sc, err := ParseScenario([]byte(toml + "\n" + testScenario))
		if err != nil {
			t.Fatalf("%s: %v", toml, err)
		}
		if got := sc.IdleTimeout(); got != want {
			t.Errorf("%s: IdleTimeout = %s, want %s", toml, got, want)
		}
	}
}
