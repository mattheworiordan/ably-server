package loadgen

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

type attState int

const (
	attPending attState = iota
	attAttaching
	attAttached
	attFailed
)

// subAtt is one attachment of a subscriber connection.
type subAtt struct {
	channel         string
	check           *AttachmentCheck // nil when the channel is not sampled
	lastSerial      string
	state           attState
	resumeRequested bool
	churn           bool
	inSet           bool      // counted in the session's pending attach set
	openStart       time.Time // churn open: when the ATTACH was sent
}

// subSession is one subscriber connection and its attachments. It owns
// reconnection: when the connection drops, planned (churn) or not, the
// session dials again and re-attaches every channel, resuming from the
// last channelSerial it saw (DESIGN.md §4.3).
type subSession struct {
	j        *Job
	global   int
	endpoint int

	mu           sync.Mutex
	conn         *Conn
	atts         map[string]*subAtt
	order        []string // attachment order (initial plan), for stable re-attach
	churnAtt     *subAtt
	pending      int // initial/re-attach set still awaiting ATTACHED
	connectStart time.Time
	reconnect    bool
	churned      atomic.Bool
	ended        bool
}

func (j *Job) runSubscriber(ctx context.Context) {
	plans := j.Plan.SubscriberSlice(j.Spec.Index, j.Spec.Count)
	j.targetConns = len(plans)
	for _, cp := range plans {
		j.targetAtts += int64(len(cp.Attach))
	}
	sessions := make([]*subSession, len(plans))
	for i, cp := range plans {
		s := &subSession{
			j:        j,
			global:   cp.Global,
			endpoint: cp.Global % len(j.Spec.Endpoints),
			atts:     make(map[string]*subAtt, len(cp.Attach)),
			order:    make([]string, 0, len(cp.Attach)),
		}
		for _, ap := range cp.Attach {
			a := &subAtt{channel: ap.Channel}
			if ap.Sampled {
				a.check = j.checker.NewAttachment(ap.Channel, "")
			}
			s.atts[ap.Channel] = a
			s.order = append(s.order, ap.Channel)
		}
		sessions[i] = s
	}
	plans = nil

	if !sleepUntil(ctx, j.start) {
		return
	}
	runCtx, stopSessions := context.WithCancel(ctx)
	var wg sync.WaitGroup
	// Open connections evenly across the ramp.
	ramp := j.Plan.Scenario.Timing.Ramp.Duration
	n := len(sessions)
	launched := make(chan struct{})
	go func() {
		defer close(launched)
		for i, s := range sessions {
			at := j.start
			if n > 0 && ramp > 0 {
				at = j.start.Add(time.Duration(int64(ramp) * int64(i) / int64(n)))
			}
			if !sleepUntil(runCtx, at) {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.run(runCtx)
			}()
		}
	}()

	// Churn during the hold.
	churnCtx, stopChurn := context.WithDeadline(runCtx, j.measureEnd)
	var churnWG sync.WaitGroup
	if sleepUntil(runCtx, j.measureStart) {
		share := func(total float64) float64 { return total / float64(j.Spec.Count) }
		if r := share(j.Plan.Churn.ConnectsPerSec); r > 0 && n > 0 {
			churnWG.Add(1)
			go func() {
				defer churnWG.Done()
				var k int64
				Pace(churnCtx, func(time.Time) float64 { return r }, time.Second, func(time.Time) {
					k++
					sessions[rand.IntN(n)].churnConnection(k%2 == 0)
				}, nil)
			}()
		}
		if r := share(j.Plan.Churn.ChannelOpensPerSec); r > 0 && n > 0 {
			churnWG.Add(1)
			go func() {
				defer churnWG.Done()
				var k int
				Pace(churnCtx, func(time.Time) float64 { return r }, time.Second, func(time.Time) {
					k++
					name := j.Plan.Prefix + "-open-" + strconv.Itoa(j.Spec.Index) + "-" + strconv.Itoa(k)
					sessions[rand.IntN(n)].openChurnChannel(name)
				}, nil)
			}()
		}
	}
	churnWG.Wait()
	stopChurn()
	if sleepUntil(runCtx, j.measureEnd) {
		j.c.openAtEnd.Store(j.c.open.Load())
		j.c.attachedAtEnd.Store(j.c.attached.Load())
	}
	// Keep reading through the drain so late deliveries are checked.
	sleepUntil(runCtx, j.end)
	// Freeze every attachment's verdict while connections are still up,
	// then close them.
	for _, s := range sessions {
		s.finish()
	}
	stopSessions()
	<-launched
	wg.Wait()
}

// run dials and re-dials until ctx ends.
func (s *subSession) run(ctx context.Context) {
	j := s.j
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		cfg := j.dialer()
		cfg.Endpoint = j.Spec.Endpoints[s.endpoint]
		start := time.Now()
		conn, err := Dial(ctx, cfg)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			j.c.connectFailures.Add(1)
			j.M.Connects.WithLabelValues("fail").Inc()
			j.logErr("subscriber conn %d: %v", s.global, err)
			// Try the next node: the one we used may be down.
			s.endpoint = (s.endpoint + 1) % len(j.Spec.Endpoints)
			if !sleepUntil(ctx, time.Now().Add(jitter(backoff))) {
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		j.M.Connects.WithLabelValues("ok").Inc()
		j.c.opened.Add(1)
		j.trackOpen(1)
		if !s.attachAll(conn, start) {
			j.trackOpen(-1)
			_ = conn.Abort()
			return
		}
		err = conn.ReadLoop(ctx, s)
		j.trackOpen(-1)
		s.detachedAll()
		if ctx.Err() != nil {
			return
		}
		j.c.reconnects.Add(1)
		j.M.Reconnects.Inc()
		if s.churned.Swap(false) {
			j.c.churnDrops.Add(1)
			continue // churn: reconnect at once
		}
		j.c.unplannedDrops.Add(1)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Action != protocol.ActionDisconnected {
			// An abrupt failure may mean the node is gone: move on.
			s.endpoint = (s.endpoint + 1) % len(j.Spec.Endpoints)
		}
		j.logErr("subscriber conn %d dropped: %v", s.global, err)
		if !sleepUntil(ctx, time.Now().Add(jitter(backoff))) {
			return
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// attachAll sends ATTACH for every attachment on a fresh connection.
// It returns false if the session has already finished.
func (s *subSession) attachAll(conn *Conn, start time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return false
	}
	s.conn = conn
	s.connectStart = start
	s.pending = 0
	resume := s.j.Plan.Churn.Resume
	send := func(a *subAtt) {
		a.resumeRequested = resume && a.lastSerial != ""
		from := ""
		if a.resumeRequested {
			from = a.lastSerial
		}
		if a.check != nil {
			a.check.SetConn(conn.ConnectionID)
			if !a.resumeRequested && a.lastSerial != "" {
				// Not resuming: the old attachment's verdict is final and
				// a new one starts.
				a.check.Finish(false)
				a.check = s.j.checker.NewAttachment(a.channel, conn.ConnectionID)
			}
		}
		a.state = attAttaching
		a.inSet = true
		a.openStart = time.Time{}
		s.pending++
		if err := conn.Attach(a.channel, from, protocol.FlagSubscribe); err != nil {
			// The write failed, so the connection is dead; ReadLoop will
			// return and the session reconnects.
			a.state = attPending
		}
	}
	for _, name := range s.order {
		send(s.atts[name])
	}
	if s.churnAtt != nil {
		send(s.churnAtt)
	}
	if s.pending == 0 {
		s.j.hist[LatConnectAttach].Record(time.Since(start))
	}
	return true
}

// detachedAll marks every attachment detached after the connection ended.
func (s *subSession) detachedAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = nil
	for _, a := range s.atts {
		if a.state == attAttached {
			s.j.trackAttached(-1)
		}
		a.state = attPending
		a.inSet = false
	}
	s.pending = 0
	s.reconnect = true
}

// finish freezes every attachment's checker verdict at the end of the
// run: an attachment that is attached now, and never lost continuity,
// counts as continuous for the tail check.
func (s *subSession) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
	for _, a := range s.atts {
		if a.check != nil {
			a.check.Finish(a.state == attAttached)
		}
	}
}

// churnConnection drops the connection, abruptly or with CLOSE; run
// reconnects at once and resumes.
func (s *subSession) churnConnection(abrupt bool) {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return
	}
	s.churned.Store(true)
	if abrupt {
		_ = conn.Abort()
	} else {
		_ = conn.Close()
	}
}

// openChurnChannel attaches a never-used channel on this connection,
// detaching the previous churn channel.
func (s *subSession) openChurnChannel(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.ended {
		return
	}
	if old := s.churnAtt; old != nil {
		delete(s.atts, old.channel)
		if old.state == attAttached {
			s.j.trackAttached(-1)
		}
		_ = s.conn.Detach(old.channel)
	}
	a := &subAtt{channel: name, churn: true, state: attAttaching, openStart: time.Now()}
	s.churnAtt = a
	s.atts[name] = a
	if err := s.conn.Attach(name, "", protocol.FlagSubscribe); err != nil {
		a.state = attPending
	}
}

// OnFrame handles ATTACHED, DETACHED, channel ERROR, MESSAGE and presence
// frames for the session.
func (s *subSession) OnFrame(c *Conn, pm *protocol.ProtocolMessage) {
	j := s.j
	switch pm.Action {
	case protocol.ActionMessage:
		s.onMessage(pm)
	case protocol.ActionPresence, protocol.ActionSync:
		j.c.presReceived.Add(int64(len(pm.Presence)))
		j.M.PresenceEvents.Add(float64(len(pm.Presence)))
	case protocol.ActionAttached:
		s.mu.Lock()
		defer s.mu.Unlock()
		a := s.atts[pm.GetChannel()]
		if a == nil || a.state != attAttaching {
			return // a churn channel already replaced, or a repeat
		}
		resumed := pm.Flags&protocol.FlagResumed != 0
		if a.resumeRequested {
			if resumed {
				j.M.Resumes.WithLabelValues("resumed").Inc()
			} else {
				j.M.Resumes.WithLabelValues("discontinuity").Inc()
				j.c.discontinuities.Add(1)
			}
		}
		if a.check != nil {
			a.check.Attached(pm.ChannelSerial, a.resumeRequested, resumed)
		}
		if pm.ChannelSerial != "" && (!a.resumeRequested || pm.ChannelSerial > a.lastSerial) {
			a.lastSerial = pm.ChannelSerial
		}
		a.state = attAttached
		j.trackAttached(1)
		if a.inSet {
			a.inSet = false
			s.settle()
		} else if !a.openStart.IsZero() {
			j.hist[LatChannelOpen].Record(time.Since(a.openStart))
			j.c.channelOpens.Add(1)
			j.M.ChannelOpens.Inc()
			a.openStart = time.Time{}
		}
	case protocol.ActionDetached:
		// Only churn channels are detached, and they were removed from
		// atts when the detach was sent.
	case protocol.ActionError:
		s.mu.Lock()
		defer s.mu.Unlock()
		a := s.atts[pm.GetChannel()]
		if a == nil || a.state != attAttaching {
			return
		}
		a.state = attFailed
		inSet := a.inSet
		a.inSet = false
		j.c.attachFailures.Add(1)
		j.M.AttachFailures.Inc()
		msg := "no error info"
		if pm.Error != nil {
			msg = pm.Error.Message
		}
		j.logErr("attach %s failed: %s", a.channel, msg)
		if inSet {
			s.settle()
		}
	}
}

// settle counts down the attach set of the current connection; when the
// last ATTACHED (or ERROR) arrives, it records connect-plus-attach.
// Caller holds s.mu.
func (s *subSession) settle() {
	if s.pending <= 0 {
		return
	}
	s.pending--
	if s.pending > 0 {
		return
	}
	d := time.Since(s.connectStart)
	s.j.hist[LatConnectAttach].Record(d)
	s.j.M.ConnectAttach.Observe(d.Seconds())
	if s.reconnect {
		s.j.hist[LatReconnectAttach].Record(d)
	}
}

func (s *subSession) onMessage(pm *protocol.ProtocolMessage) {
	j := s.j
	now := time.Now().UnixMicro()
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.atts[pm.GetChannel()]
	var payloads []Payload
	for _, m := range pm.Messages {
		p, ok := PayloadFromData(m.Data)
		if !ok {
			j.c.foreign.Add(1)
			continue
		}
		j.c.received.Add(1)
		j.M.Deliveries.Inc()
		lat := now - p.SentAtUS
		if lat < 0 {
			j.c.negLatency.Add(1)
		}
		if j.inWindow(p.SentAtUS) {
			j.c.inWindow.Add(1)
			j.hist[LatDelivery].RecordMicros(lat)
			path := "cross_node"
			if p.Node == s.endpoint {
				path = "same_node"
				j.hist[LatDeliverySameNode].RecordMicros(lat)
			} else {
				j.hist[LatDeliveryCrossNode].RecordMicros(lat)
			}
			j.M.DeliveryLatency.WithLabelValues(path).Observe(float64(lat) / 1e6)
		}
		if a != nil && a.check != nil {
			payloads = append(payloads, p)
		}
	}
	if a == nil {
		return
	}
	if a.check != nil {
		a.check.Frame(pm.ChannelSerial, payloads)
		j.M.OpenGaps.Set(float64(j.checker.OpenGaps()))
	}
	if pm.ChannelSerial > a.lastSerial {
		a.lastSerial = pm.ChannelSerial
	}
}
