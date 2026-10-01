// Package realtime implements the WebSocket realtime endpoint.
package realtime

import (
	"context"
	"crypto/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/trace"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// DefaultHeartbeatInterval is the cadence of server-driven HEARTBEAT
// frames when no other outbound frame has been sent.
const DefaultHeartbeatInterval = 15 * time.Second

// DefaultRemainPresentFor is how long a member entered by a connection
// that drops abruptly stays in the presence set before its LEAVE is
// synthesised (DESIGN.md §12.5) — the grace window that lets a resume +
// re-enter preserve the member without a flicker. Mirrors the reference
// server's connection.defaultRemainPresentFor (15s).
const DefaultRemainPresentFor = 15 * time.Second

// Connection-layer defaults (DESIGN.md §5.2, §9).
const (
	// DefaultConnOutboundMaxBytes bounds the encoded frames queued for one
	// connection.
	DefaultConnOutboundMaxBytes = 1 << 20
	// DefaultConnWriteTimeout bounds one frame write, and how long a push
	// waits for room in a full outbound queue before the connection is
	// disconnected as a slow consumer.
	DefaultConnWriteTimeout = 10 * time.Second
	// DefaultWSReadBufferSize is the per-connection WebSocket read buffer.
	// Inbound traffic on a subscriber connection is small (ATTACH,
	// HEARTBEAT, ACKable publishes), and a larger frame is read in several
	// fills, so 1 KiB keeps the resident cost of 100k+ connections low.
	DefaultWSReadBufferSize = 1024
	// DefaultWSWriteBufferSize is the WebSocket write buffer. Write
	// buffers come from a shared pool and are held only during a write,
	// so this costs memory per concurrent write, not per connection.
	DefaultWSWriteBufferSize = 4096
)

// ConnLimits configures the per-connection outbound path (DESIGN.md
// §5.2). Zero fields take the Default* values.
type ConnLimits struct {
	OutboundMaxBytes int64
	WriteTimeout     time.Duration
	ReadBufferSize   int
	WriteBufferSize  int
}

func (l ConnLimits) withDefaults() ConnLimits {
	if l.OutboundMaxBytes <= 0 {
		l.OutboundMaxBytes = DefaultConnOutboundMaxBytes
	}
	if l.WriteTimeout <= 0 {
		l.WriteTimeout = DefaultConnWriteTimeout
	}
	if l.ReadBufferSize <= 0 {
		l.ReadBufferSize = DefaultWSReadBufferSize
	}
	if l.WriteBufferSize <= 0 {
		l.WriteBufferSize = DefaultWSWriteBufferSize
	}
	return l
}

// Server holds the realtime endpoint's state. Its HTTP handlers are
// exported methods; callers register them on their own ServeMux.
type Server struct {
	authn             *auth.Authenticator
	manager           *core.Manager
	heartbeatInterval time.Duration
	logger            *logging.Logger
	metrics           *metrics.Metrics
	tracer            trace.Tracer
	upgrader          websocket.Upgrader
	limits            ConnLimits

	// connKeySecret authenticates connectionKeys (id.NewConnectionKey /
	// VerifyConnectionKey, DESIGN.md §8): random per process, never
	// persisted. A connectionKey issued by this process is only ever
	// verified by this same process (resume/publish-on-behalf-of are
	// already documented as per-node-only, DESIGN.md §1), so there is no
	// need to share or persist it across a restart or cluster.
	connKeySecret []byte

	// mu guards conns and byKey, the registry of live connections. conns is
	// used by Shutdown to disconnect them gracefully on SIGTERM (DESIGN.md
	// §11); byKey indexes them by connectionId (the identity a
	// connectionKey authenticates) so a REST publish-on-behalf can resolve
	// a connectionKey to its connection (DESIGN.md §3.2, §8, ResolveConnectionKey).
	mu    sync.Mutex
	conns map[*connection]struct{}
	byKey map[string]*connection
	// connWG tracks the connection goroutines HandleWebSocket starts, so
	// Shutdown can wait for their teardown (presence LEAVEs included)
	// before the storage is closed.
	connWG sync.WaitGroup
	// closing is set under mu when Shutdown begins; register refuses new
	// connections from then on.
	closing bool

	// remainPresentFor is the presence grace window for an abrupt
	// disconnect (DESIGN.md §12.5); see DefaultRemainPresentFor. Tests
	// shorten it.
	remainPresentFor time.Duration

	// reaperDone is closed by Shutdown to abandon any pending delayed
	// presence LEAVEs (their members go with the departing node); reaperWG
	// tracks the in-flight reaper goroutines. reaperStop guards the close
	// against a double Shutdown.
	reaperDone chan struct{}
	reaperStop sync.Once
	reaperWG   sync.WaitGroup

	// appendTracking configures each attachment's seen set for append
	// delivery (DESIGN.md §13.3); see SetAppendTracking.
	appendTracking AppendTracking
}

// AppendTracking configures how attachments remember delivered message
// serials so a later append can be sent as a delta (DESIGN.md §13.3).
type AppendTracking struct {
	// Mutable reports whether a channel is in a namespace with mutable
	// messages enabled. On such a channel every delivered message is
	// recorded, so a create's first append is a delta; elsewhere only
	// messages that carry an append delta are recorded. Nil treats every
	// channel as mutable.
	Mutable func(channel string) bool
	// SeenMax caps the serials one attachment records; the least
	// recently recorded are evicted first, a generation at a time
	// (seenSet). Zero or less means DefaultAttachmentSeenMax.
	SeenMax int
}

func (t AppendTracking) withDefaults() AppendTracking {
	if t.SeenMax <= 0 {
		t.SeenMax = DefaultAttachmentSeenMax
	}
	return t
}

// tracksCreates reports whether an attachment on channel records every
// delivered message, not only those carrying an append delta.
func (t AppendTracking) tracksCreates(channel string) bool {
	return t.Mutable == nil || t.Mutable(channel)
}

// SetAppendTracking overrides the attachment append-tracking settings
// (DESIGN.md §13.3, AppendTracking). Intended to be called once, right
// after NewServer and before the server handles connections.
func (s *Server) SetAppendTracking(t AppendTracking) {
	s.appendTracking = t.withDefaults()
}

// SetRemainPresentFor overrides the presence grace window (DESIGN.md §12.5,
// DefaultRemainPresentFor). Intended to be called once, right after
// NewServer and before the server handles connections; a value <= 0 makes
// an abrupt disconnect leave immediately (grace disabled).
func (s *Server) SetRemainPresentFor(d time.Duration) {
	s.remainPresentFor = d
}

// SetConnLimits overrides the connection-layer limits (DESIGN.md §5.2).
// Intended to be called once, right after NewServer and before the
// server handles connections.
func (s *Server) SetConnLimits(l ConnLimits) {
	s.limits = l.withDefaults()
	s.upgrader.ReadBufferSize = s.limits.ReadBufferSize
	s.upgrader.WriteBufferSize = s.limits.WriteBufferSize
}

// NewServer constructs a Server. The Manager pairs each Channel with
// its storage facet — publishes go through Channel.Publish, which
// delegates to the storage backend.
func NewServer(keys []auth.APIKey, manager *core.Manager, heartbeatInterval time.Duration, logger *logging.Logger, m *metrics.Metrics, tracer trace.Tracer) *Server {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		// crypto/rand on supported platforms never returns an error.
		panic(err)
	}
	limits := ConnLimits{}.withDefaults()
	return &Server{
		limits:            limits,
		authn:             auth.NewAuthenticator(keys...),
		manager:           manager,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
		metrics:           m,
		tracer:            tracer,
		connKeySecret:     secret,
		conns:             make(map[*connection]struct{}),
		byKey:             make(map[string]*connection),
		remainPresentFor:  DefaultRemainPresentFor,
		reaperDone:        make(chan struct{}),
		appendTracking:    AppendTracking{}.withDefaults(),
		upgrader: websocket.Upgrader{
			// Tests use httptest.Server which sets up a same-origin
			// connection; production deployments terminate TLS at a
			// reverse proxy, so cross-origin is the norm. Auth is the
			// real boundary, not Origin.
			CheckOrigin: func(*http.Request) bool { return true },
			// Per-connection read buffer; write buffers are pooled and
			// held only while a frame is being written (DESIGN.md §5.2).
			ReadBufferSize:  limits.ReadBufferSize,
			WriteBufferSize: limits.WriteBufferSize,
			WriteBufferPool: &sync.Pool{},
		},
	}
}

// HandleWebSocket authenticates the request, upgrades to a WebSocket,
// and runs the connection loop. A fatal auth failure does NOT reject the
// upgrade with HTTP 401 — Ably SDKs treat a failed upgrade as a
// transport error and retry (DISCONNECTED), never reaching FAILED. Instead
// the upgrade completes and the server sends an in-band ERROR frame
// carrying the Ably auth error (40101 invalid credentials, 40142 token
// expired, etc.), which the SDK maps to the FAILED state, then closes
// (DESIGN.md §2.1, §3; mirrors the reference frontdoor's closeWithError).
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Resolve the wire format before upgrading; a bad format is a genuine
	// bad request, not an auth failure, so it stays an HTTP-level rejection.
	format, err := protocol.FormatFromQuery(r.URL.Query().Get("format"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Authenticate and resolve the clientId, but defer surfacing any failure
	// until after the upgrade so it can be sent as an in-band ERROR.
	principal, authErr := s.authn.Authenticate(r)
	var clientID string
	if authErr == nil {
		clientID, authErr = auth.ResolveClientID(principal, r.URL.Query().Get("clientId"))
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written the error response.
		s.logger.Debug("websocket upgrade failed", "err", err)
		return
	}

	// Bound the inbound frame size: gorilla/websocket has no default limit,
	// so without this a client could send a frame of any size
	// (DESIGN.md §2.2). An oversize frame closes the connection with 1009.
	ws.SetReadLimit(protocol.MaxRequestBodyBytes)

	if authErr != nil {
		s.rejectWithError(ws, format, authErr)
		return
	}

	connID := id.NewConnectionID()

	// A resume/recover key that authenticates (VerifyConnectionKey, DESIGN.md
	// §8) retains its connectionId on the new connection — identity
	// continuity is cheap and doesn't require replaying any state — while
	// one that doesn't (wrong pairing, or someone replaying a bare
	// connectionId observed elsewhere) is declined per protocol (DESIGN.md
	// §4.3): the connection still succeeds, with a fresh connectionId, but
	// the CONNECTED carries error 80018 so the SDK treats the resume/recover
	// as failed (RTN15c7, RTN16e). Either way connection-state resume itself
	// is a non-goal — the SDK recovers message flow via per-channel
	// re-attach, not replayed state.
	resumeKey := r.URL.Query().Get("resume")
	if resumeKey == "" {
		resumeKey = r.URL.Query().Get("recover")
	}
	var resumeError *protocol.ErrorInfo
	if resumeKey != "" {
		if resumedID, ok := id.VerifyConnectionKey(s.connKeySecret, resumeKey); ok {
			connID = resumedID
		} else {
			resumeError = &protocol.ErrorInfo{
				Code:       80018,
				StatusCode: 400,
				Message:    "invalid connection key; resume/recover could not be satisfied",
			}
		}
	}
	connKey := id.NewConnectionKey(s.connKeySecret, connID)

	conn := &connection{
		srv:               s,
		ws:                ws,
		format:            format,
		id:                connID,
		key:               connKey,
		clientID:          clientID,
		principal:         principal,
		authn:             s.authn,
		cap:               principal.Capabilities(),
		tokenExpiry:       principal.ExpiresAt,
		heartbeatInterval: s.heartbeatInterval,
		echo:              echoFromQuery(r.URL.Query().Get("echo")),
		logger:            s.logger.With("connId", connID),
		manager:           s.manager,
		metrics:           s.metrics,
		tracer:            s.tracer,
		out:               newOutQueue(s.limits.OutboundMaxBytes, s.limits.WriteTimeout),
		writeTimeout:      s.limits.WriteTimeout,
		attachments:       make(map[string]*attachment),
		entered:           make(map[string]map[string]struct{}),
		publishQ:          make(chan func(), 16),
		reauth:            make(chan time.Time, 1),
		resumeError:       resumeError,
		lastMsgSerial:     -1,
		sampled:           deliverySampleSeq.Add(1)%metrics.DeliverySampleEvery == 0,
	}

	// The upgrade succeeded: count the connection and time its lifetime,
	// bracketing conn.run so the gauge and lifetime histogram stay
	// balanced whichever way the loop exits (DESIGN.md §10).
	if !s.register(conn) {
		// Shutdown has begun: this connection would not be drained.
		_ = ws.Close()
		return
	}
	opened := time.Now()
	s.metrics.ConnectionOpened()
	conn.logger.Info("connection opened", "clientId", clientID)

	// Run the connection on its own goroutine and return from the HTTP
	// handler. The socket is hijacked, so net/http no longer serves it,
	// but while the handler runs net/http keeps its per-connection
	// state alive, including a 4 KiB read buffer and a 4 KiB write
	// buffer the WebSocket never uses: about 8 KiB per connection, 800
	// MiB at 100k connections (DESIGN.md §5.2). The request context is
	// cancelled when the handler returns, so the connection runs under
	// a context that keeps its values (the tracing span) but not its
	// cancellation.
	ctx := context.WithoutCancel(r.Context())
	go func() {
		defer s.connWG.Done()
		defer func() { s.metrics.ConnectionClosed(time.Since(opened).Seconds()) }()
		defer s.deregister(conn)
		defer conn.logger.Info("connection closed")
		conn.run(ctx)
	}()
}

// register adds a live connection to the registry, indexing it by its
// connectionKey (== connectionId) for publish-on-behalf resolution, and
// counts it in connWG. It refuses (false) once Shutdown has begun, so
// every connWG.Add happens before Shutdown's Wait.
func (s *Server) register(c *connection) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conns[c] = struct{}{}
	s.byKey[c.id] = c
	s.connWG.Add(1)
	return true
}

// deregister removes a connection from the registry once its run loop has
// returned.
func (s *Server) deregister(c *connection) {
	s.mu.Lock()
	delete(s.conns, c)
	// Only drop the key index if it still points at this connection — a
	// connectionId is unique so this is defensive, but it avoids evicting a
	// re-registered entry sharing the key.
	if s.byKey[c.id] == c {
		delete(s.byKey, c.id)
	}
	s.mu.Unlock()
}

// scheduleConnectionLeaves defers the synthesised presence LEAVE for a
// connection that dropped abruptly (DESIGN.md §12.5). It captures, now,
// the member entries the connection still owns on each channel (from the
// authoritative store, keyed by member serial), then after remainPresentFor
// writes their LEAVEs — unless a member has since been superseded, i.e. the
// same connectionId resumed and re-entered, in which case its LEAVE is
// suppressed at write time. Doing the suppression check against the store at
// write time (rather than cancelling a local timer) keeps it correct when
// the resume lands on a different node in cluster mode: the re-enter is
// visible in the shared presence set there too.
//
// Runs synchronously to capture (fast store read) on the terminating
// connection's goroutine, then hands off to a reaper goroutine for the wait.
func (s *Server) scheduleConnectionLeaves(connID string, channels []string) {
	select {
	case <-s.reaperDone:
		return // shutting down: the node's presence goes with it
	default:
	}

	// Capture the member serials to leave, now, from the authoritative set.
	capCtx, cancel := context.WithTimeout(context.Background(), teardownLeaveTimeout)
	stale := make(map[string]map[string]string, len(channels)) // channel -> clientId -> member serial
	for _, channel := range channels {
		ch, err := s.manager.GetChannel(capCtx, channel)
		if err != nil {
			continue
		}
		members, _, err := ch.Members(capCtx)
		if err != nil {
			continue
		}
		owned := make(map[string]string)
		for _, m := range members {
			if m.ConnectionID == connID {
				owned[m.ClientID] = m.Serial
			}
		}
		if len(owned) > 0 {
			stale[channel] = owned
		}
	}
	cancel()
	if len(stale) == 0 {
		return
	}

	s.reaperWG.Go(func() {
		t := time.NewTimer(s.remainPresentFor)
		defer t.Stop()
		select {
		case <-t.C:
		case <-s.reaperDone:
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), teardownLeaveTimeout)
		defer cancel()
		for channel, owned := range stale {
			s.fireConnectionLeaves(ctx, connID, channel, owned)
		}
	})
}

// fireConnectionLeaves writes the deferred LEAVEs for connID's members on
// channel, suppressing any member that has been superseded since capture —
// an entry now absent (already left) or carrying a different serial (the
// connection resumed and re-entered), which must not be torn down.
func (s *Server) fireConnectionLeaves(ctx context.Context, connID, channel string, owned map[string]string) {
	ch, err := s.manager.GetChannel(ctx, channel)
	if err != nil {
		s.logger.Warn("delayed leave: GetChannel failed", "channel", channel, "err", err)
		return
	}
	members, _, err := ch.Members(ctx)
	if err != nil {
		s.logger.Warn("delayed leave: Members failed", "channel", channel, "err", err)
		return
	}
	current := make(map[string]string, len(members))
	for _, m := range members {
		if m.ConnectionID == connID {
			current[m.ClientID] = m.Serial
		}
	}
	var leaves []*protocol.PresenceMessage
	for clientID, capturedSerial := range owned {
		if current[clientID] != capturedSerial {
			continue // gone or re-entered: suppress
		}
		leaves = append(leaves, &protocol.PresenceMessage{
			Action:       protocol.PresenceLeave,
			ClientID:     clientID,
			ConnectionID: connID,
		})
	}
	if len(leaves) == 0 {
		return
	}
	if _, _, err := ch.PublishPresence(storage.WithServerPresence(ctx), leaves); err != nil {
		s.logger.Warn("delayed leave publish failed", "channel", channel, "err", err)
	}
}

// stopReaper abandons any pending delayed presence LEAVEs. Called from
// Shutdown: the node is going away, so its connections' members go with it.
func (s *Server) stopReaper() {
	s.reaperStop.Do(func() { close(s.reaperDone) })
}

// ResolveConnectionKey resolves a REST publish's connectionKey to the live
// connection it names, returning that connection's connectionId (DESIGN.md
// §3.2, §8). ok is false when the key doesn't authenticate (VerifyConnectionKey)
// or no live connection on this node holds the connectionId it names — the
// caller maps that to Ably error 40006 (invalid connectionKey). Resolution is
// per-node only: connection-state and the registry are process-local
// (DESIGN.md §1, §11), so a key issued by another cluster node is unknown
// here and yields 40006 rather than being routed.
//
// Only connectionId is derived from the key: the target connection's
// clientId is never stamped onto the message, matching the reference server
// (whose connectionKey is a signed value verified against a caller-supplied
// clientId, not a lookup that returns one) — the message's clientId always
// comes from the REST request's own resolved identity (§3.2).
func (s *Server) ResolveConnectionKey(key string) (connID string, ok bool) {
	connID, ok = id.VerifyConnectionKey(s.connKeySecret, key)
	if !ok {
		return "", false
	}
	s.mu.Lock()
	c := s.byKey[connID]
	s.mu.Unlock()
	if c == nil {
		return "", false
	}
	return connID, true
}

// Shutdown gracefully disconnects every live WebSocket connection,
// pacing the closures evenly across the window implied by ctx's deadline
// to avoid a thundering-herd reconnect against the next node (DESIGN.md
// §11). Each connection is sent a DISCONNECTED frame and then closed,
// which drives its normal teardown (including synthesised presence
// LEAVEs). When ctx's deadline is reached, any remaining stragglers are
// force-closed at once. Shutdown returns once every connection has been
// disconnected (or the deadline forced them closed).
func (s *Server) Shutdown(ctx context.Context) {
	// Abandon pending delayed presence LEAVEs first: the connections about
	// to be disconnected below would otherwise schedule fresh ones, and the
	// node's presence set departs with the node anyway (DESIGN.md §12.5).
	s.stopReaper()
	// A reaper already past its timer is firing LEAVEs through the storage;
	// wait for it (bounded by ctx) once every connection has finished its
	// teardown, so no grace LEAVE writes after the caller closes the
	// storage. Connection teardown is what starts reapers, so none starts
	// after this wait begins.
	defer s.waitReapers(ctx)

	s.mu.Lock()
	s.closing = true
	conns := make([]*connection, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if len(conns) == 0 {
		return
	}

	interval := pacingInterval(ctx, len(conns))
	for i, c := range conns {
		c.disconnect()
		if i == len(conns)-1 {
			break
		}
		select {
		case <-ctx.Done():
			// Deadline hit: force-close the remaining stragglers now
			// rather than continuing to pace past the window.
			for _, straggler := range conns[i+1:] {
				straggler.forceClose()
			}
			s.waitConns(ctx)
			return
		case <-time.After(interval):
		}
	}
	s.waitConns(ctx)
}

// waitReapers waits for the in-flight delayed-LEAVE goroutines to finish,
// or for ctx to end. Each is bounded by teardownLeaveTimeout.
func (s *Server) waitReapers(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.reaperWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// waitConns waits for every connection goroutine to finish its teardown,
// or for ctx to end.
func (s *Server) waitConns(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// pacingInterval spreads n closures evenly across the window implied by
// ctx's deadline: closures fire at 0, interval, 2*interval, …, leaving a
// 1/n slice of headroom before the deadline. With no deadline (or none
// left) it returns 0, closing everything promptly.
func pacingInterval(ctx context.Context, n int) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok || n <= 0 {
		return 0
	}
	window := time.Until(dl)
	if window <= 0 {
		return 0
	}
	return window / time.Duration(n)
}

// echoFromQuery resolves the `echo` upgrade param. Ably defaults echo to
// true; only an explicit, valid boolean flips it (a malformed value is
// ignored, keeping the safe default of echoing).
func echoFromQuery(v string) bool {
	if v == "" {
		return true
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return b
}

// rejectWithError sends a connection-level ERROR frame carrying the Ably
// auth error (code/statusCode from auth.AuthErrorInfo) on the freshly
// upgraded socket, then closes it. No CONNECTED precedes it, so the SDK
// treats the connection as never established and moves it to FAILED for a
// non-renewable error (40101/40102), or attempts token renewal for a
// renewable one (40142) — never a plain transport retry (DESIGN.md §3).
// The close code mirrors the reference: a normal closure for renewable
// token errors, a policy violation otherwise.
func (s *Server) rejectWithError(ws *websocket.Conn, format protocol.Format, err error) {
	code, statusCode, msg := auth.AuthErrorInfo(err)
	frame := &protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Message:    msg,
			Code:       code,
			StatusCode: statusCode,
		},
	}
	if data, mErr := protocol.Marshal(frame, format); mErr == nil {
		wsType := websocket.TextMessage
		if format == protocol.FormatMsgpack {
			wsType = websocket.BinaryMessage
		}
		_ = ws.WriteMessage(wsType, data)
	}
	closeCode := websocket.ClosePolicyViolation
	if code >= 40140 && code < 40150 {
		closeCode = websocket.CloseNormalClosure
	}
	_ = ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCode, msg),
		time.Now().Add(time.Second),
	)
	_ = ws.Close()
}
