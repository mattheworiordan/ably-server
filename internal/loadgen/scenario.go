package loadgen

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Scenario is a workload shape as written in a scenario file
// (bench/aws/scenarios/*.toml). Rates, counts and sizes are given at 1×
// and full scale; Resolve applies the multiplier (1× or 2×, plan §3) and
// the scale (1.0 for a real run, 0.01 and 0.1 for the smoke steps,
// plan §7) to produce a Plan.
type Scenario struct {
	Name          string  `toml:"name"          json:"name"`
	Shape         string  `toml:"shape"         json:"shape"`
	Description   string  `toml:"description"   json:"description,omitempty"`
	Bus           string  `toml:"bus"           json:"bus,omitempty"`
	Nodes         int     `toml:"nodes"         json:"nodes,omitempty"`
	Shards        int     `toml:"shards"        json:"shards,omitempty"`
	Multiplier    float64 `toml:"multiplier"    json:"multiplier,omitempty"`
	Scale         float64 `toml:"scale"         json:"scale,omitempty"`
	MessageBytes  int     `toml:"message_bytes" json:"message_bytes"`
	SamplePercent float64 `toml:"sample_percent" json:"sample_percent"`

	Timing      Timing      `toml:"timing"      json:"timing"`
	Connections Connections `toml:"connections" json:"connections"`
	Churn       Churn       `toml:"churn"       json:"churn"`
	Presence    Presence    `toml:"presence"    json:"presence"`
	Classes     []Class     `toml:"class"       json:"classes"`
	Pass        PassSpec    `toml:"pass"        json:"pass"`
}

// Duration is a time.Duration that reads and writes as "10m", "30s".
type Duration struct{ time.Duration }

// UnmarshalText parses a Go duration string.
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// MarshalText renders a Go duration string.
func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// Timing is the run's phases: connections and publish rates ramp
// linearly over Ramp, are held for Hold (the measurement window), and
// subscribers keep reading for Drain after publishing stops so late
// deliveries are checked.
type Timing struct {
	Ramp  Duration `toml:"ramp"  json:"ramp"`
	Hold  Duration `toml:"hold"  json:"hold"`
	Drain Duration `toml:"drain" json:"drain"`
}

// Connections is the subscriber connection count at 1× and full scale.
// Attachments (the subscribers of every channel) are spread over these
// connections, so a connection may hold several attachments.
type Connections struct {
	Count int `toml:"count" json:"count"`
}

// Churn is the steady connection and channel turnover during the hold.
type Churn struct {
	// ConnectsPerSec: connections dropped and re-established per second
	// (half abruptly, half with CLOSE), each re-attaching all its
	// channels.
	ConnectsPerSec float64 `toml:"connects_per_sec"      json:"connects_per_sec"`
	// ChannelOpensPerSec: attaches to never-used channel names per
	// second, each on an existing connection that detaches its previous
	// churn channel.
	ChannelOpensPerSec float64 `toml:"channel_opens_per_sec" json:"channel_opens_per_sec"`
	// Resume: a re-established connection re-attaches with the last
	// channelSerial it saw and the checker holds it to continuity
	// (DESIGN.md §4.3).
	Resume bool `toml:"resume" json:"resume"`
}

// Presence is the presence load: members that enter a presence channel
// and churn (leave then re-enter) at a rate. Off unless Enabled.
type Presence struct {
	Enabled bool `toml:"enabled" json:"enabled"`
	// Channels is the number of presence channels (scaled).
	Channels int `toml:"channels" json:"channels"`
	// MembersPerChannel is the number of members per channel (each its
	// own connection).
	MembersPerChannel int `toml:"members_per_channel" json:"members_per_channel"`
	// EventsPerSec is the total presence event rate during the hold
	// (scaled); one churn is a leave plus an enter, two events.
	EventsPerSec float64 `toml:"events_per_sec" json:"events_per_sec"`
	// Subscribe: members also subscribe to presence (they receive every
	// other member's events).
	Subscribe bool `toml:"subscribe" json:"subscribe"`
}

// Class is a group of channels with the same subscriber and publish
// profile.
type Class struct {
	Name     string `toml:"name"     json:"name"`
	Channels int    `toml:"channels" json:"channels"`
	// Subscribers per channel: a fixed count, or a distribution between
	// SubscribersMin and SubscribersMax ("uniform": each channel draws a
	// deterministic count; "harmonic": channel i gets max/(i+1) clamped
	// to [min, max], a few large and many at the minimum).
	Subscribers     int    `toml:"subscribers"      json:"subscribers,omitempty"`
	SubscribersMin  int    `toml:"subscribers_min"  json:"subscribers_min,omitempty"`
	SubscribersMax  int    `toml:"subscribers_max"  json:"subscribers_max,omitempty"`
	SubscribersDist string `toml:"subscribers_dist" json:"subscribers_dist,omitempty"`
	// PublishRate is messages/s per channel; PublishRateTotal, if set
	// instead, is messages/s across the whole class.
	PublishRate      float64 `toml:"publish_rate"       json:"publish_rate,omitempty"`
	PublishRateTotal float64 `toml:"publish_rate_total" json:"publish_rate_total,omitempty"`
	// Publisher is "rest" (HTTP POST, the default) or "realtime" (MESSAGE
	// frames on a WebSocket).
	Publisher string `toml:"publisher" json:"publisher,omitempty"`
	// Streams is the number of independent sequential publishers per
	// channel (default 1). Each stream has at most one publish in flight,
	// so its order is defined; a hot channel needs several streams.
	Streams int `toml:"streams" json:"streams,omitempty"`
	// MessageBytes overrides the scenario's message size.
	MessageBytes int `toml:"message_bytes" json:"message_bytes,omitempty"`
	// ScaleBy names what absorbs the multiplier and scale: "channels"
	// (default: channel count scales, per-channel profile fixed),
	// "subscribers" (fixed channels, subscribers per channel scale) or
	// "rate" (fixed channels and subscribers, publish rate scales).
	ScaleBy string `toml:"scale_by" json:"scale_by,omitempty"`
}

// PassSpec holds the pass criteria of plan §8. Zero values take the
// plan's defaults (DefaultPass).
type PassSpec struct {
	DeliveryP50      Duration `toml:"delivery_p50"       json:"delivery_p50"`
	DeliveryP99      Duration `toml:"delivery_p99"       json:"delivery_p99"`
	DeliveryP99Str   Duration `toml:"delivery_p99_stretch" json:"delivery_p99_stretch"`
	RestAckP99       Duration `toml:"rest_ack_p99"       json:"rest_ack_p99"`
	ConnectAttachP99 Duration `toml:"connect_attach_p99" json:"connect_attach_p99"`
	// MinAchievedRatio: achieved publish rate over offered (default 0.95).
	MinAchievedRatio float64 `toml:"min_achieved_ratio" json:"min_achieved_ratio"`
	// MaxMemoryGrowth: allowed fractional growth of node RSS across the
	// hold (default 0.10).
	MaxMemoryGrowth float64 `toml:"max_memory_growth" json:"max_memory_growth"`
	// MaxGoroutineGrowth: allowed fractional growth of node goroutines
	// across the hold (default 0.10).
	MaxGoroutineGrowth float64 `toml:"max_goroutine_growth" json:"max_goroutine_growth"`
	// MaxConnectionLoss: allowed fraction of target connections not open
	// at the end of the hold (default 0.01).
	MaxConnectionLoss float64 `toml:"max_connection_loss" json:"max_connection_loss"`
	// TailMargin: clock margin for the tail-loss check (default 1s).
	TailMargin Duration `toml:"tail_margin" json:"tail_margin"`
}

// DefaultPass returns plan §8's criteria.
func DefaultPass() PassSpec {
	return PassSpec{
		DeliveryP50:        Duration{50 * time.Millisecond},
		DeliveryP99:        Duration{250 * time.Millisecond},
		DeliveryP99Str:     Duration{100 * time.Millisecond},
		RestAckP99:         Duration{100 * time.Millisecond},
		ConnectAttachP99:   Duration{500 * time.Millisecond},
		MinAchievedRatio:   0.95,
		MaxMemoryGrowth:    0.10,
		MaxGoroutineGrowth: 0.10,
		MaxConnectionLoss:  0.01,
		TailMargin:         Duration{time.Second},
	}
}

// withDefaults fills zero pass criteria from DefaultPass.
func (p PassSpec) withDefaults() PassSpec {
	d := DefaultPass()
	if p.DeliveryP50.Duration == 0 {
		p.DeliveryP50 = d.DeliveryP50
	}
	if p.DeliveryP99.Duration == 0 {
		p.DeliveryP99 = d.DeliveryP99
	}
	if p.DeliveryP99Str.Duration == 0 {
		p.DeliveryP99Str = d.DeliveryP99Str
	}
	if p.RestAckP99.Duration == 0 {
		p.RestAckP99 = d.RestAckP99
	}
	if p.ConnectAttachP99.Duration == 0 {
		p.ConnectAttachP99 = d.ConnectAttachP99
	}
	if p.MinAchievedRatio == 0 {
		p.MinAchievedRatio = d.MinAchievedRatio
	}
	if p.MaxMemoryGrowth == 0 {
		p.MaxMemoryGrowth = d.MaxMemoryGrowth
	}
	if p.MaxGoroutineGrowth == 0 {
		p.MaxGoroutineGrowth = d.MaxGoroutineGrowth
	}
	if p.MaxConnectionLoss == 0 {
		p.MaxConnectionLoss = d.MaxConnectionLoss
	}
	if p.TailMargin.Duration == 0 {
		p.TailMargin = d.TailMargin
	}
	return p
}

// LoadScenario reads and validates a scenario file.
func LoadScenario(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseScenario(b)
}

// ParseScenario decodes TOML scenario text and validates it.
func ParseScenario(b []byte) (*Scenario, error) {
	var s Scenario
	md, err := toml.Decode(string(b), &s)
	if err != nil {
		return nil, fmt.Errorf("scenario: %w", err)
	}
	if und := md.Undecoded(); len(und) > 0 {
		keys := make([]string, len(und))
		for i, k := range und {
			keys[i] = k.String()
		}
		return nil, fmt.Errorf("scenario: unknown keys: %s", strings.Join(keys, ", "))
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate checks the scenario for values that cannot produce a plan.
func (s *Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("scenario: name is required")
	}
	if strings.ContainsAny(s.Name, "|: \t") {
		return fmt.Errorf("scenario %s: name must not contain '|', ':' or whitespace", s.Name)
	}
	if s.Shape == "" {
		return fmt.Errorf("scenario %s: shape is required", s.Name)
	}
	if s.MessageBytes <= 0 {
		return fmt.Errorf("scenario %s: message_bytes must be > 0", s.Name)
	}
	if s.SamplePercent < 0 || s.SamplePercent > 100 {
		return fmt.Errorf("scenario %s: sample_percent must be in [0, 100]", s.Name)
	}
	if s.Timing.Hold.Duration <= 0 {
		return fmt.Errorf("scenario %s: timing.hold must be > 0", s.Name)
	}
	if s.Timing.Ramp.Duration < 0 || s.Timing.Drain.Duration < 0 {
		return fmt.Errorf("scenario %s: timing durations must be >= 0", s.Name)
	}
	if len(s.Classes) == 0 && !s.Presence.Enabled {
		return fmt.Errorf("scenario %s: at least one [[class]] or an enabled [presence] is required", s.Name)
	}
	seen := map[string]bool{}
	for i := range s.Classes {
		c := &s.Classes[i]
		if c.Name == "" || strings.ContainsAny(c.Name, "|: \t-") {
			return fmt.Errorf("scenario %s: class %d: name is required and must not contain '|', ':', '-' or whitespace", s.Name, i)
		}
		if seen[c.Name] {
			return fmt.Errorf("scenario %s: duplicate class %q", s.Name, c.Name)
		}
		seen[c.Name] = true
		if c.Channels < 1 {
			return fmt.Errorf("scenario %s: class %s: channels must be >= 1", s.Name, c.Name)
		}
		switch c.SubscribersDist {
		case "":
			if c.SubscribersMin != 0 || c.SubscribersMax != 0 {
				return fmt.Errorf("scenario %s: class %s: subscribers_min/max need subscribers_dist", s.Name, c.Name)
			}
			if c.Subscribers < 0 {
				return fmt.Errorf("scenario %s: class %s: subscribers must be >= 0", s.Name, c.Name)
			}
		case "uniform", "harmonic":
			if c.Subscribers != 0 {
				return fmt.Errorf("scenario %s: class %s: use subscribers or subscribers_min/max, not both", s.Name, c.Name)
			}
			if c.SubscribersMin < 0 || c.SubscribersMax < c.SubscribersMin {
				return fmt.Errorf("scenario %s: class %s: need 0 <= subscribers_min <= subscribers_max", s.Name, c.Name)
			}
		default:
			return fmt.Errorf("scenario %s: class %s: subscribers_dist must be uniform or harmonic", s.Name, c.Name)
		}
		if c.PublishRate < 0 || c.PublishRateTotal < 0 || (c.PublishRate > 0 && c.PublishRateTotal > 0) {
			return fmt.Errorf("scenario %s: class %s: set at most one of publish_rate, publish_rate_total (>= 0)", s.Name, c.Name)
		}
		switch c.Publisher {
		case "", "rest", "realtime":
		default:
			return fmt.Errorf("scenario %s: class %s: publisher must be rest or realtime", s.Name, c.Name)
		}
		if c.Streams < 0 {
			return fmt.Errorf("scenario %s: class %s: streams must be >= 0", s.Name, c.Name)
		}
		switch c.ScaleBy {
		case "", "channels", "subscribers", "rate":
		default:
			return fmt.Errorf("scenario %s: class %s: scale_by must be channels, subscribers or rate", s.Name, c.Name)
		}
	}
	if s.Connections.Count < 1 && len(s.Classes) > 0 {
		return fmt.Errorf("scenario %s: connections.count must be >= 1", s.Name)
	}
	if s.Presence.Enabled && (s.Presence.Channels < 1 || s.Presence.MembersPerChannel < 1) {
		return fmt.Errorf("scenario %s: presence needs channels and members_per_channel >= 1", s.Name)
	}
	return nil
}

// ResolvedClass is a Class after scaling.
type ResolvedClass struct {
	Class
	Index          int
	ChannelCount   int
	subScale       float64 // factor applied to subscribers per channel
	maxSubs        int     // clamp (the connection count) for smoke scales, 0 for none
	// Clamped counts channels whose subscriber count was clamped to the
	// connection count (only at a smoke scale below 1).
	Clamped int
	RatePerChannel float64 // messages/s per channel
	StreamCount    int
	MsgBytes       int
	// firstAttachment is the global index of this class's first attachment.
	firstAttachment int64
	attachments     int64
}

// Plan is a scenario resolved at a multiplier and scale, with the
// derived totals and the deterministic assignment of work to processes.
// Every generator process and the conductor build the same Plan from the
// same inputs.
type Plan struct {
	Scenario    Scenario
	Multiplier  float64
	Scale       float64
	Factor      float64
	RunTag      string
	Prefix      string
	Connections int
	Classes     []ResolvedClass
	Churn       Churn
	Presence    Presence
	Pass        PassSpec
	Attachments int64
	sampleCut   uint64
}

// Resolve scales the scenario. multiplier and scale of 0 take the
// scenario file's values, and those default to 1. runTag makes channel
// names and message ids unique to one run so a rerun never collides with
// an earlier run's idempotency keys (DESIGN.md §8).
func (s *Scenario) Resolve(multiplier, scale float64, runTag string) (*Plan, error) {
	if multiplier == 0 {
		multiplier = s.Multiplier
	}
	if multiplier == 0 {
		multiplier = 1
	}
	if scale == 0 {
		scale = s.Scale
	}
	if scale == 0 {
		scale = 1
	}
	if multiplier < 0 || scale < 0 {
		return nil, fmt.Errorf("multiplier and scale must be > 0")
	}
	if runTag == "" || strings.ContainsAny(runTag, "|:-. \t") {
		return nil, fmt.Errorf("run tag %q must be non-empty without '|', ':', '-', '.' or whitespace", runTag)
	}
	f := multiplier * scale
	p := &Plan{
		Scenario:    *s,
		Multiplier:  multiplier,
		Scale:       scale,
		Factor:      f,
		RunTag:      runTag,
		Prefix:      "lg-" + runTag + "-" + s.Name,
		Connections: scaleCount(s.Connections.Count, f),
		Churn: Churn{
			ConnectsPerSec:     s.Churn.ConnectsPerSec * f,
			ChannelOpensPerSec: s.Churn.ChannelOpensPerSec * f,
			Resume:             s.Churn.Resume,
		},
		Presence: s.Presence,
		Pass:     s.Pass.withDefaults(),
	}
	if p.Presence.Enabled {
		p.Presence.Channels = scaleCount(s.Presence.Channels, f)
		p.Presence.EventsPerSec = s.Presence.EventsPerSec * f
	}
	p.sampleCut = uint64(math.Round(s.SamplePercent * 100))
	var next int64
	for i, c := range s.Classes {
		rc := ResolvedClass{Class: c, Index: i, ChannelCount: c.Channels, subScale: 1, StreamCount: max(c.Streams, 1), MsgBytes: s.MessageBytes}
		if c.MessageBytes > 0 {
			rc.MsgBytes = c.MessageBytes
		}
		if rc.Publisher == "" {
			rc.Publisher = "rest"
		}
		rateScale := 1.0
		switch c.ScaleBy {
		case "", "channels":
			rc.ChannelCount = scaleCount(c.Channels, f)
			rc.ScaleBy = "channels"
		case "subscribers":
			rc.subScale = f
		case "rate":
			rateScale = f
		}
		switch {
		case c.PublishRate > 0:
			rc.RatePerChannel = c.PublishRate * rateScale
		case c.PublishRateTotal > 0:
			// A class total is given at base channel count; it scales with
			// the factor whatever absorbs it.
			rc.RatePerChannel = c.PublishRateTotal * f / float64(rc.ChannelCount)
		}
		rc.firstAttachment = next
		for j := 0; j < rc.ChannelCount; j++ {
			n := int64(rc.subscribers(p.Prefix, j))
			if n > int64(p.Connections) {
				if scale >= 1 {
					return nil, fmt.Errorf("class %s channel %d has %d subscribers but only %d connections", c.Name, j, n, p.Connections)
				}
				// A smoke scale shrinks connections below a fixed-size
				// channel's fan-out: clamp so every connection holds it.
				rc.maxSubs = p.Connections
				rc.Clamped++
				n = int64(p.Connections)
			}
			rc.attachments += n
		}
		next += rc.attachments
		p.Classes = append(p.Classes, rc)
	}
	p.Attachments = next
	return p, nil
}

func scaleCount(n int, f float64) int {
	if n <= 0 {
		return 0
	}
	return max(1, int(math.Round(float64(n)*f)))
}

// ChannelName returns the name of channel j of class c.
func (p *Plan) ChannelName(c *ResolvedClass, j int) string {
	return p.Prefix + "-" + c.Name + "-" + strconv.Itoa(j)
}

// subscribers returns channel j's subscriber count.
func (c *ResolvedClass) subscribers(prefix string, j int) int {
	var base int
	switch c.SubscribersDist {
	case "":
		base = c.Subscribers
	case "uniform":
		span := uint64(c.SubscribersMax - c.SubscribersMin + 1)
		base = c.SubscribersMin + int(hash64(prefix+"-"+c.Name+"-"+strconv.Itoa(j))%span)
	case "harmonic":
		base = min(c.SubscribersMax, max(c.SubscribersMin, int(math.Round(float64(c.SubscribersMax)/float64(j+1)))))
	}
	n := base
	if c.subScale != 1 && base != 0 {
		n = max(1, int(math.Round(float64(base)*c.subScale)))
	}
	if c.maxSubs > 0 && n > c.maxSubs {
		n = c.maxSubs
	}
	return n
}

// Subscribers returns channel j's subscriber count.
func (p *Plan) Subscribers(c *ResolvedClass, j int) int { return c.subscribers(p.Prefix, j) }

func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Sampled reports whether the serial-continuity check covers channel j of
// class c: a deterministic SamplePercent of channels by name hash, plus
// the first channel of every class so hot and shared channels are always
// covered.
func (p *Plan) Sampled(c *ResolvedClass, j int) bool {
	if j == 0 && p.Scenario.SamplePercent > 0 {
		return true
	}
	return hash64(p.ChannelName(c, j))%10000 < p.sampleCut
}

// PubID names stream l of a channel. It carries the run tag so two runs
// never share an idempotency key.
func (p *Plan) PubID(stream int) string {
	return p.RunTag + "." + strconv.Itoa(stream)
}

// Totals are the plan's derived load figures.
type Totals struct {
	Connections        int     `json:"connections"`
	Attachments        int64   `json:"attachments"`
	Channels           int     `json:"channels"`
	SubscribedChannels int     `json:"subscribed_channels"`
	SampledChannels    int     `json:"sampled_channels"`
	PublishesPerSec    float64 `json:"publishes_per_sec"`
	RESTPublishesPerS  float64 `json:"rest_publishes_per_sec"`
	RTPublishesPerSec  float64 `json:"realtime_publishes_per_sec"`
	DeliveriesPerSec   float64 `json:"deliveries_per_sec"`
	FanOut             float64 `json:"fan_out"`
	Streams            int64   `json:"streams"`
	MaxStreamRate      float64 `json:"max_stream_rate"`
	ConnectsPerSec     float64 `json:"connects_per_sec"`
	ChannelOpensPerSec float64 `json:"channel_opens_per_sec"`
	PresenceMembers    int     `json:"presence_members"`
	PresenceEventsPerS float64 `json:"presence_events_per_sec"`
	InboundBytesPerSec float64 `json:"inbound_bytes_per_sec"`
}

// ClassTotals are one class's derived figures.
type ClassTotals struct {
	Name             string  `json:"name"`
	Channels         int     `json:"channels"`
	Attachments      int64   `json:"attachments"`
	MinSubscribers   int     `json:"min_subscribers"`
	MaxSubscribers   int     `json:"max_subscribers"`
	PublishesPerSec  float64 `json:"publishes_per_sec"`
	DeliveriesPerSec float64 `json:"deliveries_per_sec"`
	StreamRate       float64 `json:"stream_rate"`
	Publisher        string  `json:"publisher"`
	MessageBytes     int     `json:"message_bytes"`
	Clamped          int     `json:"clamped,omitempty"`
}

// Totals computes the plan's derived load.
func (p *Plan) Totals() (Totals, []ClassTotals) {
	t := Totals{
		Connections:        p.Connections,
		Attachments:        p.Attachments,
		ConnectsPerSec:     p.Churn.ConnectsPerSec,
		ChannelOpensPerSec: p.Churn.ChannelOpensPerSec,
	}
	if p.Presence.Enabled {
		t.PresenceMembers = p.Presence.Channels * p.Presence.MembersPerChannel
		t.PresenceEventsPerS = p.Presence.EventsPerSec
	}
	var cts []ClassTotals
	for i := range p.Classes {
		c := &p.Classes[i]
		ct := ClassTotals{Name: c.Name, Channels: c.ChannelCount, Attachments: c.attachments, Publisher: c.Publisher, MessageBytes: c.MsgBytes, MinSubscribers: math.MaxInt, Clamped: c.Clamped}
		for j := 0; j < c.ChannelCount; j++ {
			n := p.Subscribers(c, j)
			ct.MinSubscribers = min(ct.MinSubscribers, n)
			ct.MaxSubscribers = max(ct.MaxSubscribers, n)
			if n > 0 {
				t.SubscribedChannels++
			}
			if p.Sampled(c, j) {
				t.SampledChannels++
			}
			ct.DeliveriesPerSec += float64(n) * c.RatePerChannel
		}
		ct.PublishesPerSec = c.RatePerChannel * float64(c.ChannelCount)
		ct.StreamRate = c.RatePerChannel / float64(c.StreamCount)
		t.Channels += c.ChannelCount
		t.PublishesPerSec += ct.PublishesPerSec
		if c.Publisher == "realtime" {
			t.RTPublishesPerSec += ct.PublishesPerSec
		} else {
			t.RESTPublishesPerS += ct.PublishesPerSec
		}
		t.DeliveriesPerSec += ct.DeliveriesPerSec
		t.InboundBytesPerSec += ct.PublishesPerSec * float64(c.MsgBytes)
		if ct.PublishesPerSec > 0 {
			t.Streams += int64(c.ChannelCount * c.StreamCount)
			t.MaxStreamRate = max(t.MaxStreamRate, ct.StreamRate)
		}
		cts = append(cts, ct)
	}
	if t.PublishesPerSec > 0 {
		t.FanOut = t.DeliveriesPerSec / t.PublishesPerSec
	}
	return t, cts
}

// JSON renders v indented, for plan output and summaries.
func JSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
