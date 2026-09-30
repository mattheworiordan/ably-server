package loadgen

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// A publish stream is sequential: at most one publish in flight, and a
// failed publish is retried with the same message id (so the server's
// idempotency makes the retry safe, DESIGN.md §8) until it succeeds or
// the run ends. A stream therefore never skips a seq, which is what lets
// a subscriber treat any missing seq as loss. Publishes the schedule
// calls for while one is in flight wait in the stream's backlog, which
// is how a slow server shows up (the backlog grows and the achieved rate
// falls below the offered rate).

const maxBacklog = 10000

type pubStream struct {
	plan     StreamPlan
	msgBytes int
	conn     int // realtime connection index

	mu       sync.Mutex
	next     int64
	inflight bool
	backlog  []time.Time
	dropped  int64
}

type pubReq struct {
	s         *pubStream
	seq       int64
	intended  time.Time
	firstSent time.Time
	node      int
	attempts  int
	body      []byte // REST request body, fixed across retries
	data      string // payload, fixed across retries
}

type schedClass struct {
	cs      ClassStreams
	streams []*pubStream
	next    int
}

type sender interface {
	// send starts r; done is called exactly once with its outcome.
	send(ctx context.Context, r *pubReq, done func(error))
}

type scheduler struct {
	j         *Job
	transport string
	snd       sender
	stopping  atomic.Bool
	inflight  atomic.Int64
	backlog   atomic.Int64

	readyMu sync.Mutex
	ready   []*pubReq
	wake    chan struct{}

	sendCtx context.Context
	nodeRR  atomic.Uint64
	ackHist *Histogram
}

func (j *Job) runPublisher(ctx context.Context, transport string) {
	j.transport = transport
	slices := j.Plan.PublisherSlice(transport, j.Spec.Index, j.Spec.Count)
	sc := &scheduler{j: j, transport: transport, wake: make(chan struct{}, 1)}
	sc.ackHist = j.hist[LatRESTAck]
	if transport == "realtime" {
		sc.ackHist = j.hist[LatRealtimeAck]
	}
	var classes []*schedClass
	var allStreams []*pubStream
	for _, cs := range slices {
		c := &schedClass{cs: cs}
		for _, sp := range cs.Streams {
			ps := &pubStream{plan: sp, msgBytes: cs.MsgBytes}
			c.streams = append(c.streams, ps)
			allStreams = append(allStreams, ps)
		}
		if len(c.streams) > 0 {
			// Start each process at a different point in its stream list so
			// processes do not all hit the same channels first.
			c.next = rand.IntN(len(c.streams))
			classes = append(classes, c)
			j.pubStreams += len(c.streams)
			j.pubTarget += cs.TotalShare
		}
	}

	// Sending outlives the schedule by a grace period so in-flight
	// publishes and their retries can finish.
	grace := min(max(j.Plan.Scenario.Timing.Drain.Duration/2, 2*time.Second), 15*time.Second)
	sendCtx, cancelSend := context.WithDeadline(ctx, j.measureEnd.Add(grace))
	defer cancelSend()
	sc.sendCtx = sendCtx

	var closeSender func()
	switch transport {
	case "rest":
		sc.snd, closeSender = newRESTSender(sendCtx, j)
	case "realtime":
		sc.snd, closeSender = newRealtimeSender(sendCtx, j, allStreams)
	}
	defer closeSender()

	readyDone := make(chan struct{})
	go func() {
		defer close(readyDone)
		sc.readyLoop(sendCtx)
	}()

	if !sleepUntil(ctx, j.start) {
		return
	}
	var wg sync.WaitGroup
	ramp := j.Plan.Scenario.Timing.Ramp.Duration
	for _, c := range classes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Pace(ctx, RampRate(c.cs.TotalShare, j.start, ramp, j.measureEnd), time.Second,
				func(at time.Time) { sc.fire(c, at) },
				func(n int64) {
					j.c.dropped.Add(n)
					j.c.offered.Add(n)
					j.M.PublishOffered.WithLabelValues(transport).Add(float64(n))
				})
		}()
	}
	wg.Wait()
	sc.stopping.Store(true)
	// Wait for in-flight publishes (and their retries) to finish.
	for sc.inflight.Load() > 0 && sendCtx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	j.c.unresolved.Store(sc.inflight.Load())
	cancelSend()
	<-readyDone
}

// fire is one scheduled publish on class c: it goes to the next stream in
// round-robin order, so every stream in a class gets the same rate.
func (sc *scheduler) fire(c *schedClass, at time.Time) {
	j := sc.j
	s := c.streams[c.next]
	c.next = (c.next + 1) % len(c.streams)
	j.c.offered.Add(1)
	j.M.PublishOffered.WithLabelValues(sc.transport).Inc()
	if j.inWindow(at.UnixMicro()) {
		j.c.offeredInWindow.Add(1)
	}
	s.mu.Lock()
	if s.inflight {
		if len(s.backlog) < maxBacklog {
			s.backlog = append(s.backlog, at)
			sc.backlog.Add(1)
			j.M.PublishBacklog.Inc()
		} else {
			s.dropped++
			j.c.dropped.Add(1)
		}
		s.mu.Unlock()
		return
	}
	s.inflight = true
	r := &pubReq{s: s, seq: s.next, intended: at}
	s.mu.Unlock()
	sc.inflight.Add(1)
	sc.dispatch(r)
}

func (sc *scheduler) dispatch(r *pubReq) {
	j := sc.j
	if r.attempts == 0 {
		r.firstSent = time.Now()
		r.node = int(sc.nodeRR.Add(1) % uint64(len(j.Spec.Endpoints)))
		r.data = EncodePayload(Payload{PubID: r.s.plan.PubID, Seq: r.seq, SentAtUS: r.firstSent.UnixMicro(), Node: r.node}, r.s.msgBytes)
		j.c.sent.Add(1)
	}
	sc.snd.send(sc.sendCtx, r, func(err error) { sc.complete(r, err) })
}

func (sc *scheduler) complete(r *pubReq, err error) {
	j := sc.j
	now := time.Now()
	if err != nil {
		r.attempts++
		j.c.retries.Add(1)
		j.M.Publishes.WithLabelValues(sc.transport, "retried").Inc()
		if pe, ok := err.(*restStatusError); ok && pe.permanent() {
			j.c.rejected.Add(1)
			j.M.Publishes.WithLabelValues(sc.transport, "rejected").Inc()
		}
		if r.attempts <= 3 || r.attempts%100 == 0 {
			j.logErr("%s publish %s %s seq %d attempt %d: %v", sc.transport, r.s.plan.Channel, r.s.plan.PubID, r.seq, r.attempts, err)
		}
		if sc.sendCtx.Err() != nil {
			return // left in flight; counted as unresolved
		}
		delay := min(50*time.Millisecond<<min(r.attempts-1, 5), time.Second)
		time.AfterFunc(jitter(delay), func() { sc.pushReady(r) })
		return
	}
	j.c.acked.Add(1)
	j.M.Publishes.WithLabelValues(sc.transport, "acked").Inc()
	if j.inWindow(now.UnixMicro()) {
		j.c.ackedInWindow.Add(1)
	}
	lat := now.Sub(r.intended)
	if j.inWindow(r.intended.UnixMicro()) {
		sc.ackHist.Record(lat)
	}
	j.M.AckLatency.WithLabelValues(sc.transport).Observe(lat.Seconds())
	if r.s.plan.Sampled {
		j.recordStream(r.s.plan.Channel, r.s.plan.PubID, r.seq, now.UnixMicro())
	}
	s := r.s
	s.mu.Lock()
	s.next++
	var nxt *pubReq
	if len(s.backlog) > 0 && !sc.stopping.Load() {
		at := s.backlog[0]
		s.backlog = s.backlog[1:]
		sc.backlog.Add(-1)
		j.M.PublishBacklog.Dec()
		nxt = &pubReq{s: s, seq: s.next, intended: at}
	} else {
		s.inflight = false
	}
	s.mu.Unlock()
	if nxt != nil {
		sc.pushReady(nxt)
	} else {
		sc.inflight.Add(-1)
	}
}

func (sc *scheduler) pushReady(r *pubReq) {
	sc.readyMu.Lock()
	sc.ready = append(sc.ready, r)
	sc.readyMu.Unlock()
	select {
	case sc.wake <- struct{}{}:
	default:
	}
}

// readyLoop dispatches retries and backlog successors. It is the only
// place besides fire that calls dispatch, so completions never block on a
// full send queue.
func (sc *scheduler) readyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sc.wake:
		}
		for {
			sc.readyMu.Lock()
			batch := sc.ready
			sc.ready = nil
			sc.readyMu.Unlock()
			if len(batch) == 0 {
				break
			}
			for _, r := range batch {
				if ctx.Err() != nil {
					return
				}
				sc.dispatch(r)
			}
		}
	}
}

// restStatusError is a non-201 REST publish response.
type restStatusError struct {
	status int
	body   string
}

func (e *restStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.status, e.body)
}

// permanent reports a client error that a retry will not fix; it is still
// retried (a stream never skips a seq) but counted as rejected so the run
// fails loudly.
func (e *restStatusError) permanent() bool {
	return e.status >= 400 && e.status < 500 && e.status != 408 && e.status != 429
}

type restSender struct {
	j      *Job
	client *http.Client
	auth   string
	work   chan restJob
}

type restJob struct {
	r    *pubReq
	done func(error)
}

// newRESTSender starts the worker pool: Workers goroutines sharing a
// keep-alive HTTP client, so each worker holds one TCP connection per
// node it talks to.
func newRESTSender(ctx context.Context, j *Job) (sender, func()) {
	workers := j.Spec.Workers
	if workers <= 0 {
		workers = 256
	}
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        workers * len(j.Spec.Endpoints),
		MaxIdleConnsPerHost: workers,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	rs := &restSender{
		j:      j,
		client: &http.Client{Transport: tr, Timeout: 10 * time.Second},
		auth:   "Basic " + base64.StdEncoding.EncodeToString([]byte(j.Spec.Key)),
		work:   make(chan restJob, workers*2),
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-rs.work:
					job.done(rs.do(ctx, job.r))
				}
			}
		}()
	}
	return rs, func() { wg.Wait(); tr.CloseIdleConnections() }
}

func (rs *restSender) send(ctx context.Context, r *pubReq, done func(error)) {
	select {
	case rs.work <- restJob{r: r, done: done}:
	case <-ctx.Done():
		done(ctx.Err())
	}
}

func (rs *restSender) do(ctx context.Context, r *pubReq) error {
	j := rs.j
	if r.body == nil {
		// The payload is plain ASCII with no characters JSON must escape.
		id := MessageID(r.s.plan.PubID, r.seq)
		r.body = []byte(`{"id":"` + id + `","name":"lg","data":"` + r.data + `"}`)
	}
	node := (r.node + r.attempts) % len(j.Spec.Endpoints)
	u := "http://" + j.Spec.Endpoints[node] + "/channels/" + url.PathEscape(r.s.plan.Channel) + "/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(r.body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", rs.auth)
	start := time.Now()
	resp, err := rs.client.Do(req)
	if err != nil {
		return err
	}
	var buf [256]byte
	n, _ := io.ReadFull(resp.Body, buf[:])
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if j.inWindow(start.UnixMicro()) {
		j.hist[LatRESTService].Record(time.Since(start))
	}
	if resp.StatusCode != http.StatusCreated {
		return &restStatusError{status: resp.StatusCode, body: string(buf[:n])}
	}
	return nil
}

// realtimeSender publishes over a small pool of WebSocket connections.
// Each stream is pinned to one connection, and the server applies one
// connection's publishes in order (DESIGN.md §5.2).
type realtimeSender struct {
	j     *Job
	conns []*rtConn
}

type rtConn struct {
	mu   sync.Mutex
	conn *Conn
}

func (rc *rtConn) current() *Conn {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.conn
}

type discardHandler struct{}

func (discardHandler) OnFrame(*Conn, *protocol.ProtocolMessage) {}

func newRealtimeSender(ctx context.Context, j *Job, streams []*pubStream) (sender, func()) {
	n := j.Spec.RealtimeConns
	if n <= 0 {
		n = 16
	}
	n = max(1, min(n, len(streams)))
	rs := &realtimeSender{j: j, conns: make([]*rtConn, n)}
	for i, s := range streams {
		s.conn = i % n
	}
	var wg sync.WaitGroup
	for i := range rs.conns {
		rc := &rtConn{}
		rs.conns[i] = rc
		wg.Add(1)
		go func() {
			defer wg.Done()
			endpoint := (j.Spec.Index*n + i) % len(j.Spec.Endpoints)
			backoff := 100 * time.Millisecond
			for ctx.Err() == nil {
				cfg := j.dialer()
				cfg.Endpoint = j.Spec.Endpoints[endpoint]
				c, err := Dial(ctx, cfg)
				if err != nil {
					j.M.Connects.WithLabelValues("fail").Inc()
					j.c.connectFailures.Add(1)
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
				rc.mu.Lock()
				rc.conn = c
				rc.mu.Unlock()
				err = c.ReadLoop(ctx, discardHandler{})
				rc.mu.Lock()
				rc.conn = nil
				rc.mu.Unlock()
				j.trackOpen(-1)
				if ctx.Err() != nil {
					return
				}
				j.c.reconnects.Add(1)
				j.c.unplannedDrops.Add(1)
				j.logErr("realtime publisher conn %d dropped: %v", i, err)
				endpoint = (endpoint + 1) % len(j.Spec.Endpoints)
			}
		}()
	}
	return rs, wg.Wait
}

var errNoConn = fmt.Errorf("loadgen: realtime connection not up")

func (rs *realtimeSender) send(_ context.Context, r *pubReq, done func(error)) {
	c := rs.conns[r.s.conn].current()
	if c == nil {
		done(errNoConn)
		return
	}
	msg := &protocol.Message{ID: MessageID(r.s.plan.PubID, r.seq), Name: "lg", Data: r.data}
	if err := c.Publish(r.s.plan.Channel, []*protocol.Message{msg}, done); err != nil {
		done(err)
	}
}
