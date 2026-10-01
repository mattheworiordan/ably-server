package loadgen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// presenceMember is one member: its own connection, attached to one
// presence channel with the presence mode (and presence-subscribe when
// the scenario asks), entered once attached. Churn is a LEAVE followed,
// on its ACK, by an ENTER (DESIGN.md §12.2).
type presenceMember struct {
	j   *Job
	pm  PresenceMember
	sub bool

	mu       sync.Mutex
	conn     *Conn
	entered  bool
	attached bool
	busy     atomic.Bool
	// inflight counts presence operations sent and not yet ACKed or
	// NACKed (an enter chained after a leave keeps it above zero). The
	// member-set check compares only members with none in flight.
	inflight atomic.Int32
}

func (j *Job) runPresence(ctx context.Context) {
	members := j.Plan.PresenceSlice(j.Spec.Index, j.Spec.Count)
	j.presMembers = len(members)
	j.targetConns = len(members)
	j.targetAtts = int64(len(members))
	if len(members) == 0 {
		return
	}
	ms := make([]*presenceMember, len(members))
	for i, m := range members {
		ms[i] = &presenceMember{j: j, pm: m, sub: j.Plan.Presence.Subscribe}
	}
	if !sleepUntil(ctx, j.start) {
		return
	}
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	ramp := j.Plan.Scenario.Timing.Ramp.Duration
	launched := make(chan struct{})
	go func() {
		defer close(launched)
		for i, m := range ms {
			at := j.start
			if ramp > 0 {
				at = j.start.Add(time.Duration(int64(ramp) * int64(i) / int64(len(ms))))
			}
			if !sleepUntil(runCtx, at) {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); m.run(runCtx) }()
		}
	}()
	if sleepUntil(runCtx, j.measureStart) {
		churnCtx, stopChurn := context.WithDeadline(runCtx, j.measureEnd)
		go func() {
			if sleepUntil(churnCtx, j.measureStart.Add(holdSettle(j.Plan.Scenario.Timing.Hold.Duration))) {
				j.c.openAtStart.Store(j.c.open.Load())
				j.c.attachedAtStart.Store(j.c.attached.Load())
			}
		}()
		// One churn is two events (leave, enter).
		rate := j.Plan.Presence.EventsPerSec / 2 / float64(j.Spec.Count)
		if rate > 0 {
			Pace(churnCtx, func(time.Time) float64 { return rate }, time.Second, func(time.Time) {
				ms[rand.IntN(len(ms))].churn()
			}, nil)
		}
		stopChurn()
	}
	if sleepUntil(runCtx, j.measureEnd) {
		j.c.openAtEnd.Store(j.c.open.Load())
		j.c.attachedAtEnd.Store(j.c.attached.Load())
		// Churn has stopped; let its last operations finish, then compare
		// the servers' member sets with what the members believe.
		settle := min(time.Second, j.Plan.Scenario.Timing.Drain.Duration/2)
		if sleepUntil(runCtx, j.measureEnd.Add(settle)) {
			j.presenceCheck(runCtx, ms)
		}
	}
	sleepUntil(runCtx, j.end)
	stop()
	<-launched
	wg.Wait()
}

func (m *presenceMember) run(ctx context.Context) {
	j := m.j
	endpoint := m.pm.Global % len(j.Spec.Endpoints)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		cfg := j.dialer()
		cfg.Endpoint = j.Spec.Endpoints[endpoint]
		cfg.ClientID = m.pm.ClientID
		start := time.Now()
		c, err := Dial(ctx, cfg)
		if err != nil {
			j.c.connectFailures.Add(1)
			j.M.Connects.WithLabelValues("fail").Inc()
			endpoint = (endpoint + 1) % len(j.Spec.Endpoints)
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
		m.mu.Lock()
		m.conn = c
		m.entered = false
		m.attached = false
		m.mu.Unlock()
		flags := protocol.FlagPresence
		if m.sub {
			flags |= protocol.FlagPresenceSubscribe
		}
		h := &presenceHandler{m: m, start: start}
		if err := c.Attach(m.pm.Channel, "", flags); err == nil {
			err = c.ReadLoop(ctx, h)
			if ctx.Err() == nil {
				j.logErr("presence member %s dropped: %v", m.pm.ClientID, err)
			}
		}
		if h.attached {
			j.trackAttached(-1)
		}
		m.mu.Lock()
		m.conn = nil
		m.attached = false
		m.mu.Unlock()
		j.trackOpen(-1)
		if ctx.Err() != nil {
			return
		}
		j.c.reconnects.Add(1)
		j.c.unplannedDrops.Add(1)
		endpoint = (endpoint + 1) % len(j.Spec.Endpoints)
	}
}

type presenceHandler struct {
	m        *presenceMember
	start    time.Time
	attached bool
}

func (h *presenceHandler) OnFrame(c *Conn, pm *protocol.ProtocolMessage) {
	j := h.m.j
	switch pm.Action {
	case protocol.ActionAttached:
		if h.attached {
			return
		}
		h.attached = true
		h.m.mu.Lock()
		h.m.attached = true
		h.m.mu.Unlock()
		j.trackAttached(1)
		j.hist[LatConnectAttach].Record(time.Since(h.start))
		h.m.send(c, protocol.PresenceEnter)
	case protocol.ActionPresence, protocol.ActionSync:
		j.c.presReceived.Add(int64(len(pm.Presence)))
		j.M.PresenceEvents.Add(float64(len(pm.Presence)))
	case protocol.ActionError:
		j.c.attachFailures.Add(1)
		j.M.AttachFailures.Inc()
	}
}

// send publishes one presence action; a LEAVE's ACK triggers the ENTER.
func (m *presenceMember) send(c *Conn, action protocol.PresenceAction) {
	j := m.j
	start := time.Now()
	pms := []*protocol.PresenceMessage{{Action: action, ClientID: m.pm.ClientID, Data: "lg"}}
	m.inflight.Add(1)
	err := c.Presence(m.pm.Channel, pms, func(err error) {
		defer m.inflight.Add(-1)
		if err != nil {
			j.c.presNacks.Add(1)
			m.busy.Store(false)
			return
		}
		if j.inWindow(start.UnixMicro()) {
			j.hist[LatPresenceAck].Record(time.Since(start))
		}
		switch action {
		case protocol.PresenceEnter:
			j.c.presEntered.Add(1)
			m.mu.Lock()
			m.entered = true
			m.mu.Unlock()
			m.busy.Store(false)
		case protocol.PresenceLeave:
			j.c.presLeft.Add(1)
			m.mu.Lock()
			m.entered = false
			m.mu.Unlock()
			m.send(c, protocol.PresenceEnter)
		}
	})
	if err != nil {
		m.inflight.Add(-1)
		j.c.presNacks.Add(1)
		m.busy.Store(false)
	}
}

// snapshot returns whether the member believes it is entered and whether
// that belief is settled: connected, attached, nothing in flight.
func (m *presenceMember) snapshot() (entered, stable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entered, m.conn != nil && m.attached && m.inflight.Load() == 0
}

// churn leaves and re-enters, unless the member is not entered or a churn
// is already in progress.
func (m *presenceMember) churn() {
	m.mu.Lock()
	c, entered := m.conn, m.entered
	m.mu.Unlock()
	if c == nil || !entered || !m.busy.CompareAndSwap(false, true) {
		return
	}
	m.send(c, protocol.PresenceLeave)
}

// presenceCheck is the member-set check. At the end of the hold, for each
// sampled presence channel (the first, plus a deterministic SamplePercent;
// Plan.PresenceSampled) that has a member in this job, it fetches the
// channel's member set over REST (GET /channels/{name}/presence,
// paginated) and compares it with what this job's members believe:
//
//   - a member that is settled and entered must be in the set ("missing");
//   - a member that is settled and not entered must not be ("stale");
//   - a client in the set that is not a member of the channel at all is
//     "stray" (members of other presence jobs are not judged: this job
//     does not know their state).
//
// A member whose connection, attach or operation was in flight (before or
// after the fetch) is skipped and counted as indeterminate. Each
// disagreement is a presence_set_mismatch violation. A channel whose set
// could not be fetched counts as a failed check, which fails the run.
func (j *Job) presenceCheck(ctx context.Context, ms []*presenceMember) {
	per := j.Plan.Presence.MembersPerChannel
	byChannel := map[int][]*presenceMember{}
	for _, m := range ms {
		if c := m.pm.Global / per; j.Plan.PresenceSampled(c) {
			byChannel[c] = append(byChannel[c], m)
		}
	}
	channels := make([]int, 0, len(byChannel))
	for c := range byChannel {
		channels = append(channels, c)
	}
	slices.Sort(channels)
	j.c.presChecksPlanned.Store(int64(len(channels)))
	for _, c := range channels {
		j.c.presMembersPlanned.Add(int64(len(byChannel[c])))
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DisableCompression: true}}
	defer client.CloseIdleConnections()
	for _, c := range channels {
		if ctx.Err() != nil {
			return
		}
		members := byChannel[c]
		type state struct{ entered, stable bool }
		before := make([]state, len(members))
		for i, m := range members {
			before[i].entered, before[i].stable = m.snapshot()
		}
		endpoint := j.Spec.Endpoints[(j.Spec.Index+c)%len(j.Spec.Endpoints)]
		name := j.Plan.PresenceChannel(c)
		var set map[string]int
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if set, err = fetchPresenceSet(ctx, client, endpoint, j.Spec.Key, name); err == nil {
				break
			}
			if !sleepUntil(ctx, time.Now().Add(time.Duration(attempt+1)*200*time.Millisecond)) {
				return
			}
		}
		if err != nil {
			j.c.presChecksFailed.Add(1)
			j.logErr("presence member-set check of %s: %v", name, err)
			continue
		}
		for i, m := range members {
			entered, stable := m.snapshot()
			if !before[i].stable || !stable || before[i].entered != entered {
				j.c.presIndeterminate.Add(1)
				continue
			}
			j.c.presCompared.Add(1)
			n := set[m.pm.ClientID]
			switch {
			case entered && n == 0:
				j.checker.count(PresenceSetMismatch, 1, Violation{Channel: name, PubID: m.pm.ClientID, Detail: "member entered but missing from the REST set"})
			case !entered && n > 0:
				j.checker.count(PresenceSetMismatch, 1, Violation{Channel: name, PubID: m.pm.ClientID, Detail: "member not entered but present in the REST set"})
			}
		}
		for id := range set {
			if ch, ok := j.Plan.PresenceMemberChannel(id); !ok || ch != c {
				j.checker.count(PresenceSetMismatch, 1, Violation{Channel: name, PubID: id, Detail: "client in the REST set is not a member of the channel"})
			}
		}
		j.c.presChecksDone.Add(1)
	}
}

// fetchPresenceSet reads a channel's whole presence set over REST,
// following the Link rel="next" pages, and returns clientId to the number
// of entries it has.
func fetchPresenceSet(ctx context.Context, client *http.Client, endpoint, key, channel string) (map[string]int, error) {
	next, err := url.Parse("http://" + endpoint + "/channels/" + url.PathEscape(channel) + "/presence?limit=1000")
	if err != nil {
		return nil, err
	}
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(key))
	set := map[string]int{}
	for page := 0; next != nil; page++ {
		if page > 10000 {
			return nil, fmt.Errorf("presence set of %s: more than %d pages", channel, page)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, next.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", auth)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		var members []struct {
			ClientID string `json:"clientId"`
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			_ = resp.Body.Close()
			return nil, fmt.Errorf("GET %s: HTTP %d: %s", next.Path, resp.StatusCode, strings.TrimSpace(string(b)))
		}
		err = json.NewDecoder(resp.Body).Decode(&members)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", next.Path, err)
		}
		for _, m := range members {
			set[m.ClientID]++
		}
		ref := nextLink(resp.Header.Values("Link"))
		if ref == "" {
			break
		}
		u, err := url.Parse(ref)
		if err != nil {
			return nil, fmt.Errorf("presence set of %s: bad Link %q: %w", channel, ref, err)
		}
		next = next.ResolveReference(u)
	}
	return set, nil
}

// nextLink returns the URL of the rel="next" entry among Link header
// lines, or "".
func nextLink(lines []string) string {
	for _, l := range lines {
		for _, part := range strings.Split(l, ",") {
			part = strings.TrimSpace(part)
			if !strings.Contains(part, `rel="next"`) {
				continue
			}
			if i, k := strings.IndexByte(part, '<'), strings.IndexByte(part, '>'); i >= 0 && k > i {
				return part[i+1 : k]
			}
		}
	}
	return ""
}
