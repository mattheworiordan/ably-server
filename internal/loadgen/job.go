package loadgen

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// Roles a generator job can play.
const (
	RoleSubscriber = "subscriber"
	RoleREST       = "rest-publisher"
	RoleRealtime   = "realtime-publisher"
	RolePresence   = "presence"
)

// JobSpec is everything a generator process needs for its share of a run.
// The conductor sends one per (role, index); every process derives the
// same Plan from Scenario, Multiplier, Scale and RunTag, and takes its
// slice by Index and Count.
type JobSpec struct {
	ID         string   `json:"id"`
	RunID      string   `json:"run_id"`
	RunTag     string   `json:"run_tag"`
	Scenario   Scenario `json:"scenario"`
	Multiplier float64  `json:"multiplier"`
	Scale      float64  `json:"scale"`
	Role       string   `json:"role"`
	Index      int      `json:"index"`
	Count      int      `json:"count"`
	// Endpoints are the server nodes (host:port), in a fixed order shared
	// by every process: the index of a node in this list is its identity
	// in payloads and in the cross-node latency split.
	Endpoints []string `json:"endpoints"`
	Key       string   `json:"key"`
	// StartAtUS is when the ramp begins (unix µs, every box's clock
	// synced by chrony); 0 means two seconds after the job starts.
	StartAtUS int64 `json:"start_at_us,omitempty"`
	// Workers is the REST publisher's concurrent HTTP requests.
	Workers int `json:"workers,omitempty"`
	// RealtimeConns is the realtime publisher's connection count.
	RealtimeConns int `json:"realtime_conns,omitempty"`
	// Format is "msgpack" (default) or "json".
	Format string `json:"format,omitempty"`
	// HandshakeTimeout bounds dial plus CONNECTED (default 10s).
	HandshakeTimeout Duration `json:"handshake_timeout,omitempty"`
}

var runTagRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,16}$`)

// Validate checks the spec before a job is built.
func (s *JobSpec) Validate() error {
	switch s.Role {
	case RoleSubscriber, RoleREST, RoleRealtime, RolePresence:
	default:
		return fmt.Errorf("job %s: unknown role %q", s.ID, s.Role)
	}
	if s.Count < 1 || s.Index < 0 || s.Index >= s.Count {
		return fmt.Errorf("job %s: need 0 <= index < count", s.ID)
	}
	if len(s.Endpoints) == 0 {
		return fmt.Errorf("job %s: no endpoints", s.ID)
	}
	if s.Key == "" {
		return fmt.Errorf("job %s: no key", s.ID)
	}
	if !runTagRe.MatchString(s.RunTag) {
		return fmt.Errorf("job %s: run tag %q must be 1-16 of [A-Za-z0-9_]", s.ID, s.RunTag)
	}
	return s.Scenario.Validate()
}

type counters struct {
	opened, open, peak, connectFailures, reconnects, churnDrops, unplannedDrops atomic.Int64
	attached, attachFailures, channelOpens, discontinuities                     atomic.Int64
	offered, dropped, sent, acked, retries, rejected, unresolved                atomic.Int64
	offeredInWindow, ackedInWindow                                              atomic.Int64
	received, inWindow, negLatency, foreign                                     atomic.Int64
	presEntered, presLeft, presNacks, presReceived                              atomic.Int64
	openAtEnd, attachedAtEnd, openAtStart, attachedAtStart                      atomic.Int64
}

// Job runs one role for one run.
type Job struct {
	Spec    JobSpec
	Plan    *Plan
	M       *Metrics
	checker *Checker
	format  protocol.Format
	dialer  func() DialConfig

	hist map[string]*Histogram
	c    counters

	start, measureStart, measureEnd, end time.Time

	streamsMu sync.Mutex
	streams   map[string]map[string]StreamRecord

	pubStreams  int
	pubTarget   float64
	transport   string
	presMembers int
	targetConns int
	targetAtts  int64

	mu       sync.Mutex
	state    string
	errors   []string
	samples  []ResourceSample
	baseline ResourceSample
	rampEnd  *ResourceSample
	summary  *Summary
	cancel   context.CancelFunc
	done     chan struct{}
}

// JobStatus is a live view of a job.
type JobStatus struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	State       string `json:"state"`
	Connections int64  `json:"connections"`
	Attached    int64  `json:"attached"`
	Offered     int64  `json:"offered"`
	Acked       int64  `json:"acked"`
	Received    int64  `json:"received"`
	Violations  int64  `json:"violations"`
	OpenGaps    int64  `json:"open_gaps"`
	Errors      int    `json:"errors"`
}

// NewJob validates spec and resolves its plan. m may be shared by several
// jobs in one process.
func NewJob(spec JobSpec, m *Metrics) (*Job, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	plan, err := spec.Scenario.Resolve(spec.Multiplier, spec.Scale, spec.RunTag)
	if err != nil {
		return nil, err
	}
	format := protocol.FormatMsgpack
	if spec.Format == "json" {
		format = protocol.FormatJSON
	}
	if m == nil {
		m = NewMetrics()
	}
	j := &Job{
		Spec:    spec,
		Plan:    plan,
		M:       m,
		checker: NewChecker(),
		format:  format,
		hist:    make(map[string]*Histogram),
		streams: make(map[string]map[string]StreamRecord),
		state:   "pending",
		done:    make(chan struct{}),
	}
	for _, name := range []string{LatDelivery, LatDeliveryCrossNode, LatDeliverySameNode, LatRESTAck, LatRESTService, LatRealtimeAck, LatConnectAttach, LatReconnectAttach, LatChannelOpen, LatPresenceAck} {
		j.hist[name] = NewHistogram()
	}
	j.checker.OnViolation = func(k ViolationKind, n int64) {
		m.Violations.WithLabelValues(k.String()).Add(float64(n))
	}
	ht := spec.HandshakeTimeout.Duration
	if ht <= 0 {
		ht = 10 * time.Second
	}
	d := NewDialer(ht)
	j.dialer = func() DialConfig {
		return DialConfig{Key: spec.Key, Format: format, Dialer: d, HandshakeTimeout: ht}
	}
	return j, nil
}

func (j *Job) logErr(format string, args ...any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.errors) < 50 {
		j.errors = append(j.errors, time.Now().UTC().Format(time.RFC3339Nano)+" "+fmt.Sprintf(format, args...))
	}
}

func (j *Job) setState(s string) {
	j.mu.Lock()
	j.state = s
	j.mu.Unlock()
}

// inWindow reports whether a unix-µs time falls in the measurement window.
func (j *Job) inWindow(us int64) bool {
	return us >= j.measureStart.UnixMicro() && us < j.measureEnd.UnixMicro()
}

// Status returns a live view.
func (j *Job) Status() JobStatus {
	j.mu.Lock()
	state, nerr := j.state, len(j.errors)
	j.mu.Unlock()
	var v int64
	for _, k := range ViolationKinds() {
		v += j.checker.Count(k)
	}
	return JobStatus{
		ID: j.Spec.ID, Role: j.Spec.Role, State: state,
		Connections: j.c.open.Load(), Attached: j.c.attached.Load(),
		Offered: j.c.offered.Load(), Acked: j.c.acked.Load(), Received: j.c.received.Load(),
		Violations: v, OpenGaps: j.checker.OpenGaps(), Errors: nerr,
	}
}

// Stop cancels a running job; Run still writes its summary.
func (j *Job) Stop() {
	j.mu.Lock()
	cancel := j.cancel
	j.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Done is closed when Run returns.
func (j *Job) Done() <-chan struct{} { return j.done }

// Summary returns the final summary, or nil while the job runs.
func (j *Job) Summary() *Summary {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.summary
}

// Run executes the job to completion (or until ctx or Stop ends it) and
// returns its summary.
func (j *Job) Run(ctx context.Context) *Summary {
	defer close(j.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	j.mu.Lock()
	j.cancel = cancel
	j.mu.Unlock()

	t := j.Plan.Scenario.Timing
	if j.Spec.StartAtUS > 0 {
		j.start = time.UnixMicro(j.Spec.StartAtUS)
	} else {
		j.start = time.Now().Add(2 * time.Second)
	}
	j.measureStart = j.start.Add(t.Ramp.Duration)
	j.measureEnd = j.measureStart.Add(t.Hold.Duration)
	j.end = j.measureEnd.Add(t.Drain.Duration)

	stopSampler := j.startSampler(ctx)
	j.setState("running")
	switch j.Spec.Role {
	case RoleSubscriber:
		j.runSubscriber(ctx)
	case RoleREST:
		j.runPublisher(ctx, "rest")
	case RoleRealtime:
		j.runPublisher(ctx, "realtime")
	case RolePresence:
		j.runPresence(ctx)
	}
	stopSampler()
	state := "done"
	if ctx.Err() != nil && time.Now().Before(j.end) {
		state = "stopped"
	}
	s := j.buildSummary()
	j.mu.Lock()
	j.summary = s
	j.state = state
	j.mu.Unlock()
	return s
}

// sleepUntil waits for t or ctx; reports false if ctx ended first.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-tm.C:
		return true
	}
}

func (j *Job) sample() ResourceSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ResourceSample{
		AtUS:        time.Now().UnixMicro(),
		Goroutines:  runtime.NumGoroutine(),
		HeapInuse:   ms.HeapInuse,
		StackInuse:  ms.StackInuse,
		Sys:         ms.Sys,
		RSS:         readRSS(),
		Connections: j.c.open.Load(),
	}
}

// startSampler records the process footprint every 10 s, plus a baseline
// now and a sample at the end of the ramp. It returns a stop function.
func (j *Job) startSampler(ctx context.Context) func() {
	runtime.GC()
	j.baseline = j.sample()
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		rampEnd := time.NewTimer(time.Until(j.measureStart))
		defer rampEnd.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-rampEnd.C:
				runtime.GC()
				s := j.sample()
				j.mu.Lock()
				j.rampEnd = &s
				j.samples = append(j.samples, s)
				j.mu.Unlock()
			case <-tick.C:
				s := j.sample()
				j.mu.Lock()
				if len(j.samples) < 2000 {
					j.samples = append(j.samples, s)
				}
				j.mu.Unlock()
			}
		}
	}()
	return func() { cancel(); <-done }
}

// readRSS returns the resident set size on Linux (0 elsewhere).
func readRSS() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

func (j *Job) recordStream(channel, pubID string, seq int64, atUS int64) {
	j.streamsMu.Lock()
	defer j.streamsMu.Unlock()
	m := j.streams[channel]
	if m == nil {
		m = make(map[string]StreamRecord)
		j.streams[channel] = m
	}
	if r, ok := m[pubID]; !ok || seq > r.LastAckedSeq {
		m[pubID] = StreamRecord{LastAckedSeq: seq, LastAckedUS: atUS}
	}
}

func (j *Job) buildSummary() *Summary {
	host, _ := os.Hostname()
	window := j.measureEnd.Sub(j.measureStart).Seconds()
	rate := func(n int64) float64 {
		if window <= 0 {
			return 0
		}
		return float64(n) / window
	}
	s := &Summary{
		Version: SummaryVersion, RunID: j.Spec.RunID, RunTag: j.Spec.RunTag,
		Scenario: j.Plan.Scenario.Name, Shape: j.Plan.Scenario.Shape,
		Multiplier: j.Plan.Multiplier, Scale: j.Plan.Scale,
		Role: j.Spec.Role, Index: j.Spec.Index, Count: j.Spec.Count, Host: host,
		Endpoints: j.Spec.Endpoints,
		StartUS:   j.start.UnixMicro(), MeasureStartUS: j.measureStart.UnixMicro(),
		MeasureEndUS: j.measureEnd.UnixMicro(), EndUS: time.Now().UnixMicro(),
		Connections: ConnStats{
			Target: j.targetConns, Opened: j.c.opened.Load(), Peak: j.c.peak.Load(),
			OpenAtMeasureStart: j.c.openAtStart.Load(),
			OpenAtMeasureEnd:   j.c.openAtEnd.Load(), ConnectFailures: j.c.connectFailures.Load(),
			Reconnects: j.c.reconnects.Load(), ChurnDrops: j.c.churnDrops.Load(), UnplannedDrops: j.c.unplannedDrops.Load(),
		},
		Attachments: AttachStats{
			Target: j.targetAtts, AttachedAtMeasureStart: j.c.attachedAtStart.Load(), AttachedAtMeasureEnd: j.c.attachedAtEnd.Load(), Failures: j.c.attachFailures.Load(),
			ChannelOpens: j.c.channelOpens.Load(), Discontinuities: j.c.discontinuities.Load(),
		},
		Publishes: PublishStats{
			Transport: j.transport, Streams: j.pubStreams, TargetRate: j.pubTarget,
			Offered: j.c.offered.Load(), Dropped: j.c.dropped.Load(), Sent: j.c.sent.Load(),
			Acked: j.c.acked.Load(), Retries: j.c.retries.Load(), Rejected: j.c.rejected.Load(),
			Unresolved:      j.c.unresolved.Load(),
			OfferedInWindow: j.c.offeredInWindow.Load(), AckedInWindow: j.c.ackedInWindow.Load(),
			OfferedRate: rate(j.c.offeredInWindow.Load()), AchievedRate: rate(j.c.ackedInWindow.Load()),
		},
		Deliveries: DeliveryStats{
			Received: j.c.received.Load(), InWindow: j.c.inWindow.Load(), Rate: rate(j.c.inWindow.Load()),
			NegativeLatency: j.c.negLatency.Load(), Foreign: j.c.foreign.Load(),
		},
		Presence: PresenceStats{
			Members: j.presMembers, Entered: j.c.presEntered.Load(), Left: j.c.presLeft.Load(),
			Nacks: j.c.presNacks.Load(), Received: j.c.presReceived.Load(),
		},
		Latency:     j.hist,
		Correctness: j.checker.Summary(),
	}
	j.streamsMu.Lock()
	if len(j.streams) > 0 {
		s.Streams = j.streams
	}
	j.streamsMu.Unlock()
	j.mu.Lock()
	s.Resources.Samples = append([]ResourceSample(nil), j.samples...)
	if j.rampEnd != nil && j.rampEnd.Connections > 0 {
		inuse := func(r ResourceSample) float64 { return float64(r.HeapInuse + r.StackInuse) }
		s.Resources.BytesPerConnection = (inuse(*j.rampEnd) - inuse(j.baseline)) / float64(j.rampEnd.Connections)
		if j.rampEnd.RSS > 0 {
			s.Resources.RSSPerConnection = (float64(j.rampEnd.RSS) - float64(j.baseline.RSS)) / float64(j.rampEnd.Connections)
		}
	}
	s.Errors = append([]string(nil), j.errors...)
	j.mu.Unlock()
	return s
}

// trackOpen adjusts the open-connection count and its peak.
func (j *Job) trackOpen(delta int64) {
	n := j.c.open.Add(delta)
	j.M.Connections.Add(float64(delta))
	for {
		p := j.c.peak.Load()
		if n <= p || j.c.peak.CompareAndSwap(p, n) {
			return
		}
	}
}

func (j *Job) trackAttached(delta int64) {
	j.c.attached.Add(delta)
	j.M.Attachments.Add(float64(delta))
}
