package realtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// connection is one live WebSocket connection. It owns two goroutines:
// the request-handler goroutine runs the read loop, and one goroutine is
// spawned for the write loop (gorilla/websocket requires a single writer
// per connection). Attachments spawn additional goroutines, one per
// channel, that push frames onto the connection's outbound channel.
type connection struct {
	ws     *websocket.Conn
	format protocol.Format
	id     string
	// key is the connectionKey advertised to the client (DESIGN.md §8):
	// id plus an HMAC suffix authenticating the pairing, so a resume/
	// recover request can be verified rather than just trusting a bare
	// connectionId. Set once at construction; never mutated.
	key               string
	clientID          string              // resolved clientId for this connection ("" = anonymous, "*" = wildcard); see DESIGN.md §3.2
	principal         *auth.Principal     // verified credential + token claims; clientId resolution consumes this
	authn             *auth.Authenticator // verifies tokens supplied via inband AUTH (DESIGN.md §3)
	heartbeatInterval time.Duration
	// echo is the connection's `echo` upgrade param (default true). When
	// false, the fan-out skips delivering this connection's own published
	// MESSAGEs back to it (DESIGN.md §2.1); presence is always delivered.
	echo    bool
	logger  *logging.Logger
	manager *core.Manager
	metrics *metrics.Metrics
	// tracer is nil unless OTEL tracing is enabled; guarded on every use
	// so the disabled path creates no spans and no context allocations.
	tracer trace.Tracer

	// out is the bounded outbound queue drained by writeLoop (DESIGN.md
	// §5.2); writeTimeout bounds each frame write and each wait for
	// room in out.
	out          *outQueue
	writeTimeout time.Duration
	// slow latches the slow-consumer disconnect so it runs once.
	slow atomic.Bool
	// sampled is set on one connection in metrics.DeliverySampleEvery:
	// it records each frame's wait in the outbound queue
	// (ably_conn_write_wait_seconds) and its attachments the fan-out time
	// (ably_delivery_fanout_seconds, DESIGN.md §10).
	sampled bool
	// requested is when the WebSocket upgrade request reached the server,
	// and connectedAt (unix nanoseconds, set by the write loop) when the
	// CONNECTED frame was written: the two ends of ably_connect_seconds,
	// and connectedAt the start of ably_client_attach_delay_seconds
	// (DESIGN.md §10). connectedAt is read once, by the first ATTACH.
	requested   time.Time
	connectedAt atomic.Int64
	attachSeen  bool // the read loop has handled an ATTACH; read loop only
	attachments map[string]*attachment

	// publishQ is the per-connection publish pipeline: one buffered
	// channel of tasks drained in FIFO order by a single publishLoop
	// goroutine (DESIGN.md §5.2). Each inbound MESSAGE/mutation/PRESENCE
	// frame enqueues exactly one task; the worker performs the storage
	// write off the read goroutine and emits the frame's ACK/NACK on
	// completion. A single FIFO worker keeps ACKs in msgSerial order and
	// keeps this connection's channel appends in publish order, while the
	// read loop stays free to decode the next frame.
	publishQ chan func()

	// loopCtx is the connection's run context, and loops tracks the
	// publish worker and authLoop goroutines. Both are started on
	// demand (startPublishLoop, startAuthLoop) rather than per
	// connection, so an idle subscriber connection runs only its read
	// and write loops plus one goroutine per attachment (DESIGN.md
	// §5.2). publishStarted and authStarted are touched only by the
	// read goroutine.
	loopCtx        context.Context
	loops          sync.WaitGroup
	publishStarted bool
	authStarted    bool

	// entered tracks the presence members this connection has entered,
	// per channel: channel -> clientId -> a copy of the member's last
	// ENTER/UPDATE/PRESENT. Used to synthesise LEAVE on DETACH and on
	// connection teardown, and to re-enter the members after a presence
	// lease lapse (DESIGN.md §12.5). The read goroutine writes it; the
	// lease-lapse re-entry and a grace hand-over read or extend it from
	// other goroutines, so enteredMu guards it. presenceClosed, set under
	// enteredMu when teardown takes the set, ends both.
	enteredMu      sync.Mutex
	entered        map[string]map[string]*protocol.PresenceMessage
	presenceClosed bool

	// presMu orders this connection's presence writes with a lease-lapse
	// re-entry of its members (DESIGN.md §12.5): a client's ENTER, UPDATE
	// or LEAVE, a DETACH's LEAVEs and teardown each hold it while they
	// publish, and the re-entry holds it from reading entered until its
	// ENTERs are stored, so it cannot resurrect a member the client has
	// just left.
	presMu sync.Mutex

	// srv is the owning Server, used at teardown to schedule the delayed
	// presence LEAVE for an abrupt disconnect (DESIGN.md §12.5). Set once
	// at construction.
	srv *Server

	// clientClosed records that the client sent a CLOSE frame — a clean,
	// intentional departure, so teardown synthesises presence LEAVEs
	// immediately rather than delaying them for a possible resume
	// (DESIGN.md §12.5). Written only by the read goroutine in handleClose,
	// read only by the same goroutine after the read loop exits.
	clientClosed bool

	// shuttingDown records that this connection is being closed by a
	// graceful server Shutdown (§11) — a deliberate departure, so teardown
	// leaves its presence members immediately rather than delaying them
	// (DESIGN.md §12.5). Set on the Shutdown goroutine and read on the run
	// goroutine at teardown, so it is atomic.
	shuttingDown atomic.Bool

	// authMu guards the mutable authorisation state that inband re-auth
	// (DESIGN.md §3) updates — the capability set and token
	// expiry — since it is read from the read goroutine and the publish
	// worker but written by the read goroutine on an AUTH frame.
	authMu      sync.Mutex
	cap         auth.Capability
	tokenExpiry time.Time

	// reauth signals the authLoop with a new token expiry after a
	// successful inband re-auth, so it reschedules its prompt/expiry
	// timers. Buffered (cap 1) so the read goroutine never blocks on it.
	reauth chan time.Time

	// lastMsgSerial is the highest publish msgSerial accepted on this
	// connection (-1 until the first publish). A publish/presence frame that
	// repeats or goes backward from it is a client retransmit (e.g. the SDK
	// re-flushing a queued publish after a reconnect with the same msgSerial,
	// RTL6c2) — it is dropped without an ACK so the SDK's pending-publish
	// accounting stays consistent (DESIGN.md §5.2). Touched only by the read
	// goroutine.
	lastMsgSerial int64

	// resumeError, when non-nil, is carried on the initial CONNECTED frame
	// to decline a resume/recover the server cannot honour (DESIGN.md §4.3):
	// a malformed resume/recover key. The fresh connectionId plus this error
	// tell the SDK the resume failed so it resets msgSerial and re-attaches
	// (RTN15c7, RTN16e). A well-formed key is left un-errored — the server
	// still starts a fresh connection (connection-state resume is a non-goal)
	// but the SDK recovers message flow via per-channel re-attach.
	resumeError *protocol.ErrorInfo
}

// Connection limits advertised in ConnectionDetails on CONNECTED
// (DESIGN.md §2.1, §8). SDKs adopt them (e.g. rejecting oversize publishes
// client-side against maxMessageSize). maxMessageSize is also enforced
// server-side on every publish (NACK 40009, DESIGN.md §2.2) and inbound
// frames are capped at protocol.MaxRequestBodyBytes; the rest are advisory.
const (
	// defaultMaxMessageSize is Ably's 64 KiB single-publish payload cap.
	defaultMaxMessageSize = protocol.MaxMessageSize
	// defaultMaxFrameSize is Ably's 512 KiB frame / POST-body cap.
	defaultMaxFrameSize int64 = 524288
	// defaultMaxInboundRate is the advisory per-connection publish rate
	// ceiling in messages/second.
	defaultMaxInboundRate int64 = 1000
	// defaultConnectionStateTTL is how long an SDK should treat a
	// dropped connection's state as recoverable (Ably's DF1a default).
	defaultConnectionStateTTL = 120 * time.Second
)

// run drives the connection until either side terminates. It returns
// once both loops have exited.
func (c *connection) run(ctx context.Context) {
	defer c.ws.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// One span per connection covers its whole lifecycle (DESIGN.md §10);
	// publish spans nest under it via the context. Skipped entirely when
	// tracing is disabled (c.tracer nil) so the hot path is untouched.
	if c.tracer != nil {
		var span trace.Span
		ctx, span = c.tracer.Start(ctx, "ws.connection",
			trace.WithAttributes(attribute.String("ably.connection_id", c.id)))
		defer span.End()
	}

	// CONNECTED is the first frame we emit; buffer is empty here.
	// resumeError (a declined resume/recover, DESIGN.md §4.3) rides along
	// so the SDK sees the fresh connectionId as a resume failure.
	if !c.queueObserved(ctx, &protocol.ProtocolMessage{
		Action:            protocol.ActionConnected,
		ConnectionID:      c.id,
		ConnectionDetails: c.connectionDetails(),
		Error:             c.resumeError,
	}, nil, c.connectedWritten) {
		return
	}

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		c.writeLoop(ctx)
	}()

	// The publish worker starts on the first publish (startPublishLoop).
	// authLoop enforces token expiry and prompts inband re-auth
	// (DESIGN.md §3); a Basic connection (zero expiry) needs none until a
	// re-auth supplies an expiry, so it starts only then.
	c.loopCtx = ctx
	if !c.initialTokenExpiry().IsZero() {
		c.startAuthLoop()
	}

	c.readLoop(ctx)

	// The read loop has exited — the connection is terminating (client
	// disconnect, network error, or the socket being closed under us on
	// shutdown). Cancel the context and drain the publish worker first so
	// no in-flight task races the teardown below and no further ACKs are
	// queued for a dying connection.
	cancel()
	c.loops.Wait()

	// Synthesise LEAVE for every presence member this connection still
	// holds, so other subscribers see the departures (DESIGN.md §12.5).
	// The worker has stopped; teardown takes the entered set under
	// enteredMu and closes it, so no re-entry or hand-over adds to it
	// afterwards.
	//
	// Deliberate departures — a clean client CLOSE or a graceful server
	// shutdown — leave immediately. An abrupt disconnect (transport drop,
	// heartbeat/token-expiry disconnect) instead schedules the LEAVE after
	// a short grace window: if the client resumes and re-enters within it,
	// the member never flickers out (DESIGN.md §12.5).
	if c.clientClosed || c.shuttingDown.Load() || c.srv == nil {
		c.emitTeardownLeaves()
	} else {
		c.scheduleTeardownLeaves()
	}

	<-writeDone
}

// publishLoop drains the connection's publish pipeline in FIFO order,
// running one task at a time until the context is cancelled. Serialising
// the tasks keeps this connection's channel appends in publish order and
// its ACK/NACK frames in msgSerial order, while the read goroutine is free
// to decode the next frame (DESIGN.md §5.2).
func (c *connection) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.publishQ:
			task()
		}
	}
}

// enqueuePublish hands a task to the publish worker, blocking only under
// backpressure (the buffer is full because earlier writes are still in
// flight) — never on the storage write itself. Returns false if the
// connection's context is cancelled before the task is accepted.
func (c *connection) enqueuePublish(ctx context.Context, task func()) bool {
	c.startPublishLoop()
	select {
	case c.publishQ <- task:
		return true
	case <-ctx.Done():
		return false
	}
}

// startPublishLoop starts the publish worker on first use. Called only
// on the read goroutine.
func (c *connection) startPublishLoop() {
	if c.publishStarted {
		return
	}
	c.publishStarted = true
	c.loops.Add(1)
	go func() {
		defer c.loops.Done()
		c.publishLoop(c.loopCtx)
	}()
}

// startAuthLoop starts the token-expiry loop on first need: at connect
// for a token credential, or on the first inband re-auth. Called only
// on the read goroutine.
func (c *connection) startAuthLoop() {
	if c.authStarted {
		return
	}
	c.authStarted = true
	c.loops.Add(1)
	go func() {
		defer c.loops.Done()
		c.authLoop(c.loopCtx, c.initialTokenExpiry())
	}()
}

// enqueueNack routes a validation-rejection NACK through the publish
// worker so it is emitted in msgSerial order behind any publishes still
// in flight on this connection — a directly-queued NACK could otherwise
// overtake an earlier publish's ACK and corrupt the SDK's ack accounting.
func (c *connection) enqueueNack(ctx context.Context, msgSerial int64, errInfo *protocol.ErrorInfo) {
	c.enqueuePublish(ctx, func() { c.nack(ctx, msgSerial, errInfo) })
}

// connectionDetails builds the ConnectionDetails advertised on CONNECTED
// (DESIGN.md §2.1, §8): the resolved clientId (omitted when anonymous,
// "*" for a wildcard bearer), the connectionKey (the connectionId plus an
// HMAC suffix authenticating it, so a resume/recover request can be
// verified — connection-state resume itself is still a non-goal), the
// connection limits, and maxIdleInterval aligned to the server heartbeat
// cadence so the SDK knows how long a quiet server→client direction is
// expected.
func (c *connection) connectionDetails() *protocol.ConnectionDetails {
	return &protocol.ConnectionDetails{
		ClientID:             c.clientID,
		ConnectionKey:        c.key,
		MaxMessageSize:       defaultMaxMessageSize,
		MaxFrameSize:         defaultMaxFrameSize,
		MaxInboundRate:       defaultMaxInboundRate,
		ConnectionStateTTLMs: defaultConnectionStateTTL.Milliseconds(),
		MaxIdleIntervalMs:    c.heartbeatInterval.Milliseconds(),
	}
}

// readLoop decodes inbound frames and dispatches on Action.
func (c *connection) readLoop(ctx context.Context) {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			if !isExpectedClose(err) {
				c.logger.Debug("read error", "err", err)
			}
			return
		}

		var msg protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, c.format, &msg); err != nil {
			c.logger.Warn("decode error", "err", err)
			continue
		}
		c.dispatch(ctx, &msg)
	}
}

func (c *connection) dispatch(ctx context.Context, msg *protocol.ProtocolMessage) {
	c.logger.Trace("frame received", "action", msg.Action.String())
	switch msg.Action {
	case protocol.ActionAttach:
		c.handleAttach(ctx, msg)
	case protocol.ActionDetach:
		c.handleDetach(ctx, msg.GetChannel())
	case protocol.ActionMessage:
		if !c.acceptMsgSerial(msg.Action, msg.GetMsgSerial()) {
			return
		}
		c.handleMessage(ctx, msg)
	case protocol.ActionPresence:
		if !c.acceptMsgSerial(msg.Action, msg.GetMsgSerial()) {
			return
		}
		c.handlePresence(ctx, msg)
	case protocol.ActionAnnotation:
		if !c.acceptMsgSerial(msg.Action, msg.GetMsgSerial()) {
			return
		}
		c.handleAnnotation(ctx, msg)
	case protocol.ActionSync:
		c.handleSync(ctx, msg)
	case protocol.ActionAuth:
		c.handleAuth(ctx, msg)
	case protocol.ActionHeartbeat:
		c.handleHeartbeat(ctx, msg)
	case protocol.ActionClose:
		c.handleClose(ctx)
	default:
		c.logger.Debug("unhandled frame action; ignoring", "action", msg.Action.String())
	}
}

// acceptMsgSerial reports whether an inbound publish/presence/annotation
// frame with the given msgSerial should be processed. It tracks the
// connection's msgSerial (shared across all three actions, matching the
// SDK's single per-connection counter) and drops a non-monotonic
// retransmit — a serial at or below the next expected — so a publish the
// SDK re-flushes after a reconnect with the same msgSerial (RTL6c2) is not
// re-ACKed. A second ACK for a msgSerial the SDK has already dequeued
// corrupts its positional pending-publish accounting (it panics). A forward
// skip is allowed. Mirrors the reference server's checkMsgSerial (drop,
// don't close). Called only on the read goroutine.
//
// c.lastMsgSerial only ever advances on acceptance — a dropped frame must
// not move the baseline, or a later genuine retransmit of an already-seen
// serial could be wrongly re-accepted against the regressed baseline.
func (c *connection) acceptMsgSerial(action protocol.Action, serial int64) bool {
	last := c.lastMsgSerial
	// First publish on the connection: adopt it as the baseline (the SDK
	// may not reset msgSerial to 0 after a resume).
	accept := last == -1 || serial >= last+1
	if accept {
		c.lastMsgSerial = serial
	}
	c.logger.Trace("msgSerial gate", "action", action.String(), "serial", serial, "last", last, "accepted", accept)
	return accept
}

// handleHeartbeat replies to a client-initiated HEARTBEAT with a
// HEARTBEAT that echoes the inbound frame's id. connection.ping() sends a
// HEARTBEAT with a random id and resolves only when it sees a HEARTBEAT
// carrying the same id (SDKs correlate by id), so the echo is what
// makes the ping resolve. The server's idle write-loop HEARTBEAT carries
// no id and never satisfies a ping. Mirrors the reference frontdoor's
// inbound-HEARTBEAT handler (id copied straight back, omitted on the wire
// when empty).
func (c *connection) handleHeartbeat(ctx context.Context, msg *protocol.ProtocolMessage) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action: protocol.ActionHeartbeat,
		ID:     msg.ID,
	})
}

// handleClose responds to a client-initiated CLOSE with CLOSED. The
// client then closes its end of the WebSocket, which causes readLoop's
// ReadMessage to return and the connection to terminate normally.
func (c *connection) handleClose(ctx context.Context) {
	// A client-initiated CLOSE is a clean departure: teardown leaves its
	// presence members immediately, with no resume grace (DESIGN.md §12.5).
	c.clientClosed = true
	c.queue(ctx, &protocol.ProtocolMessage{Action: protocol.ActionClosed})
}

// handleAttach starts an attachment for the channel named by msg, or —
// when this connection is already attached — updates the existing
// attachment's modes/params in place (DESIGN.md §4.1). For a fresh
// attach, msg.ChannelSerial, if non-empty, is the client's resume
// cursor: the attachment will replay the gap from that cursor to the
// live anchor before entering the live MESSAGE forwarding loop (subject
// to the replay cap).
func (c *connection) handleAttach(ctx context.Context, msg *protocol.ProtocolMessage) {
	received := time.Now()
	if !c.attachSeen {
		c.attachSeen = true
		if at := c.connectedAt.Load(); at != 0 {
			c.metrics.ClientAttachDelay(received.Sub(time.Unix(0, at)))
		}
	}
	name := msg.GetChannel()
	// An invalid channel name (empty, a reserved leading character such as
	// ':' or '[', a line break) is rejected with ERROR 40010 (DESIGN.md §4,
	// mirroring the reference's channel-name predicate). The channel goes
	// FAILED on the SDK while the connection stays open; no attachment is
	// created.
	if !core.ValidChannelName(name) {
		c.logger.Warn("ATTACH with invalid channel name; rejecting", "channel", name)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action: protocol.ActionError,
			// Emit the channel field explicitly (pointer-to-name), even for the
			// empty name, so the SDK routes this as a channel-scoped failure
			// (the channel goes FAILED) rather than treating a channel-less
			// ERROR as connection-fatal.
			Channel: new(name),
			Error: &protocol.ErrorInfo{
				Message:    "invalid channel name",
				Code:       40010,
				StatusCode: 400,
			},
		})
		return
	}
	// Effective mode set = requested ∩ capability-permitted (DESIGN.md
	// §3.1, §4.2). An empty intersection means the credential grants no
	// mode on this channel: reject with ERROR 40160 and create no
	// attachment (and, for a repeat ATTACH, leave the existing one
	// untouched — mirroring the reference, which rejects the request while
	// leaving the already-asserted channel in place).
	requested := resolveRequestedModes(msg.Flags, msg.Params)
	effective := requested & c.permittedModes(name)
	if effective == 0 {
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:  protocol.ActionError,
			Channel: new(name),
			Error: &protocol.ErrorInfo{
				Message:    "insufficient capability to attach to channel",
				Code:       40160,
				StatusCode: 401,
			},
		})
		return
	}

	// A repeat ATTACH for a channel this connection is already attached to
	// is a mode/param update, not a detach+re-attach (DESIGN.md §4.1): the
	// SDK sends ATTACH whenever its channel is not locally
	// ATTACHED/ATTACHING and blocks until it sees an ATTACHED, and it also
	// re-attaches to change modes (setOptions). Tearing the attachment down
	// and rebuilding it can drop undelivered messages (a no-cursor
	// re-ATTACH discards entries linked but not yet forwarded) or duplicate
	// delivered ones (a cursor re-ATTACH replays). Instead we mutate the
	// live attachment in place — swap in the freshly-resolved modes/params
	// and reply ATTACHED (RESUMED, current channelSerial, negotiated
	// modes/params) without disturbing the stream or its goroutine, so
	// continuity holds by construction (matching the reference's
	// statefulconnection.ts assertChannel: reuse, update modes, reply
	// ATTACHED with the current serial). Presence is deliberately not
	// resynced (the reference does not on an in-place re-attach). An
	// explicit backwards cursor on a live attachment is ignored for now,
	// and we reply at the current position. (The server does send append
	// deltas, DESIGN.md §13.3: to an attachment that has already been
	// delivered the target message, tracked per attachment in its seen
	// set, and a full version otherwise. Ignoring the cursor leaves that
	// set and the stream untouched, which is why replying at the current
	// position is safe.)
	if a, exists := c.attachments[name]; exists {
		serial := a.applyReattach(requested, effective, msg.Params)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:        protocol.ActionAttached,
			Channel:       new(name),
			ChannelSerial: serial,
			Flags:         effective | protocol.FlagResumed,
			Params:        a.echoParams(),
		})
		return
	}

	ch, err := c.manager.GetChannel(ctx, name)
	if err != nil {
		c.logger.Warn("GetChannel failed", "channel", name, "err", err)
		info := &protocol.ErrorInfo{Message: "failed to attach to channel", Code: 50000, StatusCode: 500}
		if errors.Is(err, storage.ErrUnavailable) {
			// The channel's database is unreachable (a shard that is down,
			// DESIGN.md §6.4): a retriable 50003.
			info = &protocol.ErrorInfo{Message: "failed to attach to channel: storage unavailable, retry", Code: 50003, StatusCode: 503}
		}
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:  protocol.ActionError,
			Channel: new(name),
			Error:   info,
		})
		return
	}
	stream, err := ch.Attach(ctx)
	if err != nil {
		c.logger.Warn("Attach failed", "channel", name, "err", err)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:  protocol.ActionError,
			Channel: new(name),
			Error: &protocol.ErrorInfo{
				Message:    "failed to attach to channel",
				Code:       50000,
				StatusCode: 500,
			},
		})
		return
	}
	// The stream's Channel, not ch: if ch was evicted between GetChannel
	// and Attach, the stream is on the freshly bound Channel (DESIGN.md
	// §5.1).
	a := newAttachment(ctx, name, stream.Channel(), stream, msg.ChannelSerial, msg.Flags&protocol.FlagAttachResume != 0, requested, effective, msg.Params, c.queue, c.id, c.echo, c.metrics, c.logger.With("channel", name))
	a.outShared = c.queueShared
	a.outObserved = c.queueObserved
	a.received = received
	if c.srv != nil {
		t := c.srv.appendTracking
		a.seen = newSeenSet(t.SeenMax)
		a.trackCreates = t.tracksCreates(name)
	}
	a.sampled = c.sampled
	c.attachments[name] = a
	c.metrics.AttachmentOpened()
	c.logger.Debug("channel attached", "channel", name)
	go a.run()
}

// connectedWritten runs on the write loop once the CONNECTED frame is on
// the wire: it records the time since the upgrade request
// (ably_connect_seconds) and notes when, for the first ATTACH
// (ably_client_attach_delay_seconds, DESIGN.md §10).
func (c *connection) connectedWritten(int) {
	now := time.Now()
	c.connectedAt.Store(now.UnixNano())
	if !c.requested.IsZero() {
		c.metrics.ConnectWritten(now.Sub(c.requested))
	}
}

// permittedModes maps the connection's capability to the channel-mode
// bits it is allowed on channel (DESIGN.md §3.1, §4.2): subscribe grants
// SUBSCRIBE and PRESENCE_SUBSCRIBE, publish grants PUBLISH, presence
// grants PRESENCE.
func (c *connection) permittedModes(channel string) int64 {
	cap := c.capability()
	var m int64
	if cap.Permits(channel, auth.OpSubscribe) {
		m |= protocol.FlagSubscribe | protocol.FlagPresenceSubscribe
	}
	if cap.Permits(channel, auth.OpPublish) {
		m |= protocol.FlagPublish
	}
	if cap.Permits(channel, auth.OpPresence) {
		m |= protocol.FlagPresence
	}
	// Annotation modes (DESIGN.md §4.2, §14.3): permitted iff the
	// capability grants the matching annotation op. ANNOTATION_PUBLISH is in
	// the default set; ANNOTATION_SUBSCRIBE is opt-in (resolveModes excludes
	// it from the no-mode-bits default) but granted here when requested and
	// permitted.
	if cap.Permits(channel, auth.OpAnnotationPublish) {
		m |= protocol.FlagAnnotationPublish
	}
	if cap.Permits(channel, auth.OpAnnotationSubscribe) {
		m |= protocol.FlagAnnotationSubscribe
	}
	return m
}

// reconcileAttachmentCapabilities re-evaluates every attached channel's
// effective mode set against the connection's capability, following an
// inband reauth that changed it (DESIGN.md §3, RTC8a1). A channel whose
// originally-requested modes no longer intersect the new capability at
// all is failed — a channel-scoped ERROR, which drives the SDK's channel
// to FAILED (mirroring the reference server's permissionError path) —
// since there is nothing left for it to do. One that retains at least one
// mode has its effective set narrowed or widened in place via
// recheckCapability; only reconcileAttachmentCapabilities' caller
// (handleAuth) needs to know a downgrade happened at all, so no frame is
// sent for that case: the mode change takes effect for the next
// publish/subscribe/presence check, same as a repeat ATTACH would.
func (c *connection) reconcileAttachmentCapabilities(ctx context.Context) {
	for name, a := range c.attachments {
		if a.recheckCapability(c.permittedModes(name)) != 0 {
			continue
		}
		a.stop()
		delete(c.attachments, name)
		c.leaveChannel(ctx, name)
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:  protocol.ActionError,
			Channel: new(name),
			Error: &protocol.ErrorInfo{
				Message:    "insufficient capability to remain attached to channel",
				Code:       40160,
				StatusCode: 401,
			},
		})
	}
}

// handleDetach stops the matching attachment (waiting for its goroutine
// to exit so no further MESSAGE frames slip past the DETACHED ack) and
// queues DETACHED. DETACH for a channel with no live attachment is
// idempotent — we still ack so the client can transition cleanly.
func (c *connection) handleDetach(ctx context.Context, name string) {
	if name == "" {
		c.logger.Warn("DETACH with empty channel name; ignoring")
		return
	}
	// Detaching from a channel leaves any presence members this
	// connection entered on it (DESIGN.md §12.5).
	c.leaveChannel(ctx, name)
	if a, ok := c.attachments[name]; ok {
		a.stop()
		delete(c.attachments, name)
		c.logger.Debug("channel detached", "channel", name)
	}
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:  protocol.ActionDetached,
		Channel: new(name),
	})
}

// handleSync answers a client-initiated SYNC frame (ably-js
// RealtimeChannel.sync, RTP19): it re-delivers the current presence set for
// an already-attached channel so the client can reconcile its local presence
// map. A SYNC for a channel this connection is not attached to is ignored —
// there is nothing to resync against. Runs on the read goroutine, which owns
// c.attachments.
func (c *connection) handleSync(ctx context.Context, msg *protocol.ProtocolMessage) {
	a, ok := c.attachments[msg.GetChannel()]
	if !ok {
		c.logger.Debug("SYNC for a channel this connection is not attached to; ignoring", "channel", msg.GetChannel())
		return
	}
	a.resync(ctx)
}

// handleMessage validates and stamps the inbound payload on the read
// goroutine, then hands the storage write to the publish worker, which
// ACKs after a durable commit (or NACKs on failure). Validation
// rejections are also enqueued so their NACK stays ordered behind any
// still-pending publishes on this connection.
func (c *connection) handleMessage(ctx context.Context, msg *protocol.ProtocolMessage) {
	msgSerial := msg.GetMsgSerial()
	// A publish to an invalid channel name (empty, reserved leading
	// character, line break) is NACKed with 40010 (DESIGN.md §4) — the SDK
	// publishes without a prior attach, so the name is validated here.
	if !core.ValidChannelName(msg.GetChannel()) {
		c.logger.Warn("MESSAGE with invalid channel name; rejecting", "channel", msg.GetChannel(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "invalid channel name",
			Code:       40010,
			StatusCode: 400,
		})
		return
	}
	if len(msg.Messages) == 0 {
		c.logger.Warn("MESSAGE with no payload; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
		return
	}
	// A publish over the advertised maxMessageSize is NACKed 40009 before
	// anything is queued (DESIGN.md §2.2); mutations are held to the same
	// limit.
	if size := protocol.PublishSize(msg.Messages); size > defaultMaxMessageSize {
		c.logger.Warn("MESSAGE over maxMessageSize; rejecting", "channel", msg.GetChannel(), "msgSerial", msgSerial, "size", size)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    fmt.Sprintf("maximum message length exceeded (%d > %d bytes)", size, defaultMaxMessageSize),
			Code:       40009,
			StatusCode: 400,
		})
		return
	}
	// Mutations (update/delete/append) reuse the MESSAGE frame,
	// distinguished by a non-create Message-level action (DESIGN.md
	// §13.6); they take the mutation path rather than a fresh publish.
	for _, m := range msg.Messages {
		if m.Action.IsMutation() {
			c.handleMutation(ctx, msg)
			return
		}
	}

	// A publish on a channel this connection is attached to is gated by
	// the attachment's `publish` mode: an attachment that did not request
	// publish (e.g. ChannelOptions.modes / params.modes excluding it) may
	// not publish and is NACKed 40160, mirroring the reference, which
	// refuses a publish op the attachment's mode does not permit
	// (lib/channel/attachment.go handlePublish). A publish with no
	// attachment is a transient publish — the reference materialises a
	// publish-capable transient attachment — so it is gated only by
	// capability below.
	if a, ok := c.attachments[msg.GetChannel()]; ok && !a.hasMode(protocol.FlagPublish) {
		c.logger.Warn("publish rejected: attachment lacks the publish mode",
			"channel", msg.GetChannel(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "publish requires an attachment with the publish mode",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// A create publish requires the `publish` capability on the channel
	// (DESIGN.md §3.1). Insufficient capability → NACK 40160.
	if !c.capability().Permits(msg.GetChannel(), auth.OpPublish) {
		c.logger.Warn("publish rejected: insufficient capability",
			"channel", msg.GetChannel(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "insufficient capability to publish",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Resolve and stamp each message's clientId against the connection's
	// identity (DESIGN.md §3.2): a message may omit it (we stamp the
	// connection's), match it, or — for a wildcard connection — assert any
	// concrete identity. Asserting a disallowed identity is rejected.
	for _, m := range msg.Messages {
		cid, ok := auth.MessageClientID(c.clientID, m.ClientID)
		if !ok {
			c.logger.Warn("message clientId not permitted; NACKing",
				"channel", msg.GetChannel(), "msgSerial", msgSerial, "msgClientId", m.ClientID)
			c.enqueueNack(ctx, msgSerial, nil)
			return
		}
		m.ClientID = cid
		// Stamp the publishing connection so subscribers see the origin
		// (Ably delivers Message.connectionId) and the fan-out can honour
		// echo=false (DESIGN.md §2.1, §8).
		m.ConnectionID = c.id
	}

	channel := msg.GetChannel()
	messages := msg.Messages
	// Timed from here — the point the publish is accepted and handed to
	// the worker — so publish latency spans the queue wait plus the
	// storage commit, i.e. inbound publish to ACK (DESIGN.md §10).
	accepted := time.Now()
	c.enqueuePublish(ctx, func() {
		pubCtx := ctx
		if c.tracer != nil {
			var span trace.Span
			pubCtx, span = c.tracer.Start(ctx, "publish",
				trace.WithAttributes(attribute.String("ably.channel", channel)))
			defer span.End()
		}
		ch, err := c.manager.GetChannel(pubCtx, channel)
		if err != nil {
			c.logger.Warn("publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, nil)
			return
		}
		cm, _, err := ch.Publish(pubCtx, messages)
		if err != nil {
			c.logger.Warn("publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			c.nack(ctx, msgSerial, publishErrorInfo(err))
			return
		}
		c.metrics.MessagePublished(time.Since(accepted).Seconds())
		c.logger.Trace("publish committed; ACKing", "channel", channel, "msgSerial", msgSerial, "channelSerial", cm.ChannelSerial)
		// Count is 1: an ACK acknowledges protocol messages (one msgSerial
		// per frame), not the inner messages. We emit one ACK per inbound
		// frame and never batch-ack, so it is always 1. The per-message
		// serials ride the single Res entry (Ably's TR4s) so the publisher
		// still learns every serial it was assigned (DESIGN.md §8). The ACK
		// is emitted only now, after storage has durably committed.
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: &msgSerial,
			Count:     1,
			Res:       []*protocol.PublishResult{{Serials: messageSerials(cm.Messages)}},
		})
	})
}

// publishErrorInfo maps a storage publish failure to the ErrorInfo its
// NACK carries: the retriable batching failures (DESIGN.md §6.3) get
// their Ably codes so a client can back off and retry; anything else
// NACKs without one, as before.
func publishErrorInfo(err error) *protocol.ErrorInfo {
	switch {
	case errors.Is(err, storage.ErrInvalidChannelName):
		return &protocol.ErrorInfo{Message: err.Error(), Code: 40010, StatusCode: 400}
	case errors.Is(err, storage.ErrOverloaded):
		return &protocol.ErrorInfo{Message: "publish rejected: server busy, retry later", Code: 42910, StatusCode: 429}
	case errors.Is(err, storage.ErrUnavailable):
		return &protocol.ErrorInfo{Message: "publish could not be committed, retry", Code: 50003, StatusCode: 503}
	}
	return nil
}

// messageSerials returns the server-assigned Serial of each message in
// idx order — the serials carried in the frame's ACK Res entry.
func messageSerials(msgs []*protocol.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Serial
	}
	return out
}

// deliverySampleSeq numbers connections for the delivery-stage sampling
// (connection.sampled).
var deliverySampleSeq atomic.Uint64

// queue encodes a frame and pushes it onto the outbound queue, waiting
// under backpressure for at most the write timeout (DESIGN.md §5.2).
// Returns false if the frame was not queued: the connection's context
// was cancelled, the connection is closing, or the queue stayed full for
// the write timeout — in which case the connection is disconnected as a
// slow consumer.
func (c *connection) queue(ctx context.Context, msg *protocol.ProtocolMessage) bool {
	f, err := c.encode(msg)
	if err != nil {
		c.logger.Warn("encode error; dropping frame", "action", msg.Action.String(), "err", err)
		return false
	}
	return c.push(ctx, f)
}

// memoizer keeps one value per key; *core.PresenceSnapshot and
// *core.Stream (for the cm it last returned) are memoizers.
type memoizer interface {
	Memo(key any, build func() any) any
}

// sharedFrameKey keys a shared frame's encoding on its memoizer: one per
// frame action and wire format. It is a small integer so that boxing it
// as the memo key does not allocate (Go keeps the values below 256
// preallocated); actions and formats are well inside that.
type sharedFrameKey uint16

func frameKey(action protocol.Action, format protocol.Format) sharedFrameKey {
	return sharedFrameKey(uint16(action)<<2 | uint16(format)&3)
}

// queueShared is queue for a frame that every connection sends
// unchanged, such as a channel's SYNC snapshot (DESIGN.md §12.4) or a
// live MESSAGE with no per-attachment transform (§5.1): its encoding in
// this connection's wire format is kept on memo, so the frame is encoded
// once per format however many attachments it is sent to. The encoded
// bytes are shared read-only between connections.
func (c *connection) queueShared(ctx context.Context, msg *protocol.ProtocolMessage, memo memoizer) bool {
	return c.queueObserved(ctx, msg, memo, nil)
}

// queueObserved queues msg like queueShared (or like queue when memo is
// nil) and, when written is set, has the write loop call it once the
// frame is on the wire. An attach uses it to time its ATTACHED and SYNC
// frames (ably_attach_seconds, DESIGN.md §10).
func (c *connection) queueObserved(ctx context.Context, msg *protocol.ProtocolMessage, memo memoizer, written func(bytes int)) bool {
	var (
		f   outFrame
		err error
	)
	if memo == nil {
		f, err = c.encode(msg)
	} else {
		type encoded struct {
			f   outFrame
			err error
		}
		e := memo.Memo(frameKey(msg.Action, c.format), func() any {
			f, err := c.encode(msg)
			return encoded{f, err}
		}).(encoded)
		f, err = e.f, e.err
	}
	if err != nil {
		c.logger.Warn("encode error; dropping frame", "action", msg.Action.String(), "err", err)
		return false
	}
	f.written = written
	return c.push(ctx, f)
}

// push queues an encoded frame (see queue).
func (c *connection) push(ctx context.Context, f outFrame) bool {
	if c.sampled {
		f.queued = time.Now()
	}
	switch err := c.out.push(ctx, f); {
	case err == nil:
		return true
	case errors.Is(err, errSlowConsumer):
		c.slowConsumer("queue_full")
	}
	return false
}

// encode marshals msg in the connection's wire format.
func (c *connection) encode(msg *protocol.ProtocolMessage) (outFrame, error) {
	data, err := protocol.Marshal(msg, c.format)
	if err != nil {
		return outFrame{}, err
	}
	wsType := websocket.TextMessage
	if c.format == protocol.FormatMsgpack {
		wsType = websocket.BinaryMessage
	}
	return outFrame{data: data, wsType: wsType, action: msg.Action}, nil
}

// slowConsumerError is the error carried on the DISCONNECTED sent to a
// slow consumer: 80003 (connection disconnected), a retriable code, so
// the SDK reconnects and resumes each channel from the last
// channelSerial it received, gap-filling from the log. Nothing is
// dropped on the server, so 80020 (continuity lost, rate exceeded) would
// be wrong (DESIGN.md §5.2).
var slowConsumerError = &protocol.ErrorInfo{
	Message:    "connection disconnected: client not reading fast enough (outbound buffer full)",
	Code:       80003,
	StatusCode: 400,
}

// slowConsumer disconnects a client that is not keeping up (DESIGN.md
// §5.2): its queued backlog is dropped, a DISCONNECTED goes out as the
// last frame, and the write loop closes the socket after it (or the
// write deadline does, if the client is not reading at all). Runs once;
// safe from any goroutine.
func (c *connection) slowConsumer(reason string) {
	if !c.slow.CompareAndSwap(false, true) {
		return
	}
	c.logger.Warn("disconnecting slow consumer", "reason", reason, "queuedBytes", c.out.queuedBytes())
	c.metrics.SlowConsumerDisconnect(reason)
	f, err := c.encode(&protocol.ProtocolMessage{Action: protocol.ActionDisconnected, Error: slowConsumerError})
	if err != nil {
		c.forceClose()
		return
	}
	c.out.replaceWith(f)
}

// writeLoop serialises all outbound frames and emits HEARTBEAT on idle.
// Every write carries a deadline of the write timeout; a write that
// misses it means the client stopped reading, and the socket is closed
// so the read loop unblocks and teardown runs (DESIGN.md §5.2).
func (c *connection) writeLoop(ctx context.Context) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	defer c.out.close()

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.out.ready:
			for {
				f, ok := c.out.pop()
				if !ok {
					break
				}
				if err := c.writeFrame(f); err != nil {
					return
				}
				if f.written != nil {
					f.written(len(f.data))
				}
				if !f.queued.IsZero() {
					c.metrics.ConnWriteWait(time.Since(f.queued))
				}
				// A server-initiated DISCONNECTED (shutdown, DESIGN.md §11;
				// slow consumer, §5.2) is the connection's last frame: once
				// it is on the wire, close the socket so the read loop
				// unblocks and normal teardown runs (synthesising presence
				// LEAVEs). Closing here — after the write — guarantees the
				// client receives the frame before the close.
				if f.action == protocol.ActionDisconnected {
					_ = c.ws.Close()
					return
				}
			}
			ticker.Reset(c.heartbeatInterval)

		case <-ticker.C:
			f, err := c.encode(&protocol.ProtocolMessage{Action: protocol.ActionHeartbeat})
			if err != nil {
				return
			}
			if err := c.writeFrame(f); err != nil {
				return
			}
		}
	}
}

// writeFrame writes one frame under the write deadline. On failure it
// closes the socket (a timed-out gorilla/websocket conn is unusable) and
// records a write timeout as a slow-consumer disconnect.
func (c *connection) writeFrame(f outFrame) error {
	if c.writeTimeout > 0 {
		_ = c.ws.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	err := c.ws.WriteMessage(f.wsType, f.data)
	if err == nil {
		return nil
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.logger.Warn("write timed out; closing connection", "timeout", c.writeTimeout,
			"frame", f.action.String(), "bytes", len(f.data), "queuedBytes", c.out.queuedBytes())
		c.metrics.WriteTimeout(f.action.String())
		if c.slow.CompareAndSwap(false, true) {
			c.metrics.SlowConsumerDisconnect("write_timeout")
		}
	} else {
		c.logger.Debug("write error", "err", err)
	}
	_ = c.ws.Close()
	return err
}

// disconnect initiates a graceful, server-side close (DESIGN.md §11): it
// enqueues a DISCONNECTED frame, which the write loop flushes before
// closing the socket. If the outbound buffer cannot accept the frame
// (the writer is gone or backed up), the socket is force-closed directly
// so the connection still tears down. Safe to call from the shutdown
// goroutine — it never touches per-connection state owned by the read
// loop.
func (c *connection) disconnect() {
	// A graceful shutdown is a deliberate departure: teardown leaves this
	// connection's presence members immediately, not after the grace window
	// (DESIGN.md §12.5). Set before the close that unblocks the read loop so
	// the teardown observes it.
	c.shuttingDown.Store(true)
	f, err := c.encode(&protocol.ProtocolMessage{
		Action: protocol.ActionDisconnected,
		Error: &protocol.ErrorInfo{
			Message:    "server is shutting down; please reconnect",
			Code:       80003, // ErrDisconnected — a retryable disconnect
			StatusCode: 503,
		},
	})
	if err != nil || !c.out.tryPush(f) {
		c.forceClose()
	}
}

// forceClose closes the underlying socket immediately, unblocking the
// read loop. Used for stragglers still open at the shutdown deadline.
func (c *connection) forceClose() {
	_ = c.ws.Close()
}

func isExpectedClose(err error) bool {
	if errors.Is(err, websocket.ErrCloseSent) {
		return true
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		return true
	}
	return false
}
