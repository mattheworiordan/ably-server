package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// DialConfig is what a Conn needs to reach a node.
type DialConfig struct {
	// Endpoint is host:port of an ably-server node (plain WebSocket;
	// TLS terminates at a load balancer in any real deployment, plan §1).
	Endpoint string
	// Key is an API key "appId.keyId:secret", sent as the key query
	// parameter (DESIGN.md §2.1, §3).
	Key string
	// ClientID, if set, is requested as the connection's clientId (needed
	// for presence, DESIGN.md §12.3).
	ClientID string
	// Format is the wire encoding; the generator uses msgpack.
	Format protocol.Format
	// Echo controls whether the connection receives its own publishes.
	Echo bool
	// Dialer is shared across connections so buffer settings and the
	// write-buffer pool are shared too. NewDialer builds a suitable one.
	Dialer *websocket.Dialer
	// HandshakeTimeout bounds dial plus the CONNECTED frame.
	HandshakeTimeout time.Duration
}

// NewDialer returns a websocket.Dialer tuned for many mostly idle
// connections: small read buffers and a pooled write buffer, so an idle
// connection holds about 1 KB of buffers.
func NewDialer(timeout time.Duration) *websocket.Dialer {
	return &websocket.Dialer{
		Proxy:            nil,
		HandshakeTimeout: timeout,
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
		WriteBufferPool:  &sync.Pool{},
		NetDialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: -1, // the protocol's HEARTBEAT is the liveness check
		}).DialContext,
	}
}

// ErrConnClosed is returned by writes on a closed Conn.
var ErrConnClosed = errors.New("loadgen: connection closed")

// ProtocolError is an Ably ErrorInfo received in an ERROR, NACK or
// DISCONNECTED frame.
type ProtocolError struct {
	Action protocol.Action
	Info   protocol.ErrorInfo
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("%s: code=%d status=%d %s", e.Action, e.Info.Code, e.Info.StatusCode, e.Info.Message)
}

// Handler receives the frames a Conn does not consume itself. All
// methods run on the goroutine that called ReadLoop, one at a time.
type Handler interface {
	// OnFrame is called for ATTACHED, DETACHED, MESSAGE, PRESENCE, SYNC
	// and channel-scoped ERROR frames.
	OnFrame(c *Conn, pm *protocol.ProtocolMessage)
}

// PublishCallback reports a publish outcome: nil on ACK, a
// *ProtocolError on NACK, ErrConnClosed if the connection ended first.
type PublishCallback func(err error)

// ResultCallback is a PublishCallback that also receives the ACK's
// publish result (the server-assigned serials, DESIGN.md §8); res is nil
// on a NACK, an error or an ACK without one.
type ResultCallback func(res *protocol.PublishResult, err error)

// Conn is one realtime WebSocket connection. Frames are written under a
// mutex from any goroutine; frames are read only by ReadLoop.
type Conn struct {
	ws      *websocket.Conn
	format  protocol.Format
	msgType int

	ConnectionID  string
	ConnectionKey string
	// IdleTimeout is how long ReadLoop waits for any frame before it
	// declares the connection dead: the server's maxIdleInterval plus a
	// margin (DESIGN.md §2.1).
	IdleTimeout time.Duration
	// ConnectErr is the error carried on CONNECTED, if any (for example
	// 80018 when a resume key is refused, DESIGN.md §4.3).
	ConnectErr *protocol.ErrorInfo

	wmu       sync.Mutex
	closed    bool
	msgSerial int64
	pending   map[int64]ResultCallback
}

// Dial opens a connection and waits for CONNECTED. The context bounds the
// whole handshake.
func Dial(ctx context.Context, cfg DialConfig) (*Conn, error) {
	if cfg.HandshakeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.HandshakeTimeout)
		defer cancel()
	}
	q := url.Values{}
	q.Set("v", "2")
	q.Set("format", cfg.Format.String())
	q.Set("key", cfg.Key)
	if !cfg.Echo {
		q.Set("echo", "false")
	}
	if cfg.ClientID != "" {
		q.Set("clientId", cfg.ClientID)
	}
	u := url.URL{Scheme: "ws", Host: cfg.Endpoint, Path: "/", RawQuery: q.Encode()}
	d := cfg.Dialer
	if d == nil {
		d = NewDialer(10 * time.Second)
	}
	ws, resp, err := d.DialContext(ctx, u.String(), http.Header{})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Endpoint, err)
	}
	c := &Conn{
		ws:      ws,
		format:  cfg.Format,
		msgType: websocket.TextMessage,
		pending: make(map[int64]ResultCallback),
	}
	if cfg.Format == protocol.FormatMsgpack {
		c.msgType = websocket.BinaryMessage
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = ws.SetReadDeadline(dl)
	}
	stop := context.AfterFunc(ctx, func() { _ = ws.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()
	pm, err := c.read()
	if err != nil {
		_ = ws.Close()
		return nil, fmt.Errorf("waiting for CONNECTED from %s: %w", cfg.Endpoint, err)
	}
	switch pm.Action {
	case protocol.ActionConnected:
	case protocol.ActionError, protocol.ActionDisconnected:
		_ = ws.Close()
		if pm.Error != nil {
			return nil, &ProtocolError{Action: pm.Action, Info: *pm.Error}
		}
		return nil, fmt.Errorf("connect to %s: %s without error info", cfg.Endpoint, pm.Action)
	default:
		_ = ws.Close()
		return nil, fmt.Errorf("connect to %s: first frame was %s, want connected", cfg.Endpoint, pm.Action)
	}
	c.ConnectionID = pm.ConnectionID
	c.ConnectErr = pm.Error
	idle := 15 * time.Second
	if d := pm.ConnectionDetails; d != nil {
		c.ConnectionKey = d.ConnectionKey
		if d.MaxIdleIntervalMs > 0 {
			idle = time.Duration(d.MaxIdleIntervalMs) * time.Millisecond
		}
	}
	c.IdleTimeout = idle + max(idle/2, 5*time.Second)
	return c, nil
}

func (c *Conn) read() (*protocol.ProtocolMessage, error) {
	_, data, err := c.ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	pm := &protocol.ProtocolMessage{}
	if err := protocol.Unmarshal(data, c.format, pm); err != nil {
		return nil, fmt.Errorf("decode frame: %w", err)
	}
	return pm, nil
}

// write sends one frame. Callers hold no lock.
func (c *Conn) write(pm *protocol.ProtocolMessage) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeLocked(pm)
}

func (c *Conn) writeLocked(pm *protocol.ProtocolMessage) error {
	if c.closed {
		return ErrConnClosed
	}
	data, err := protocol.Marshal(pm, c.format)
	if err != nil {
		return err
	}
	_ = c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.ws.WriteMessage(c.msgType, data)
}

// Attach sends ATTACH for channel. resumeFrom, if non-empty, is the last
// channelSerial received on the channel: the server replays everything
// after it (DESIGN.md §4.3). flags carries mode bits (0 for the default
// set, DESIGN.md §4.2).
func (c *Conn) Attach(channel, resumeFrom string, flags int64) error {
	ch := channel
	return c.write(&protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       &ch,
		ChannelSerial: resumeFrom,
		Flags:         flags,
	})
}

// Detach sends DETACH for channel.
func (c *Conn) Detach(channel string) error {
	ch := channel
	return c.write(&protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: &ch})
}

// Publish sends one MESSAGE frame carrying msgs on channel and calls cb
// on its ACK or NACK. msgSerial is assigned here, under the write lock,
// so wire order and msgSerial order agree (DESIGN.md §8).
func (c *Conn) Publish(channel string, msgs []*protocol.Message, cb PublishCallback) error {
	var rcb ResultCallback
	if cb != nil {
		rcb = func(_ *protocol.PublishResult, err error) { cb(err) }
	}
	return c.PublishWithResult(channel, msgs, rcb)
}

// PublishWithResult is Publish with the ACK's serials passed to cb.
func (c *Conn) PublishWithResult(channel string, msgs []*protocol.Message, cb ResultCallback) error {
	ch := channel
	return c.sendWithSerial(&protocol.ProtocolMessage{Action: protocol.ActionMessage, Channel: &ch, Messages: msgs}, cb)
}

// Presence sends one PRESENCE frame (enter, update or leave) and calls cb
// on its ACK or NACK (DESIGN.md §12.2).
func (c *Conn) Presence(channel string, pms []*protocol.PresenceMessage, cb PublishCallback) error {
	ch := channel
	var rcb ResultCallback
	if cb != nil {
		rcb = func(_ *protocol.PublishResult, err error) { cb(err) }
	}
	return c.sendWithSerial(&protocol.ProtocolMessage{Action: protocol.ActionPresence, Channel: &ch, Presence: pms}, rcb)
}

func (c *Conn) sendWithSerial(pm *protocol.ProtocolMessage, cb ResultCallback) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return ErrConnClosed
	}
	serial := c.msgSerial
	pm.MsgSerial = &serial
	if cb != nil {
		c.pending[serial] = cb
	}
	if err := c.writeLocked(pm); err != nil {
		delete(c.pending, serial)
		return err
	}
	c.msgSerial++
	return nil
}

// Ping sends a client HEARTBEAT; the server echoes the id (DESIGN.md §2.1).
func (c *Conn) Ping(id string) error {
	return c.write(&protocol.ProtocolMessage{Action: protocol.ActionHeartbeat, ID: id})
}

// Close sends CLOSE and closes the socket without waiting for CLOSED.
// Pending publish callbacks are failed with ErrConnClosed.
func (c *Conn) Close() error {
	_ = c.write(&protocol.ProtocolMessage{Action: protocol.ActionClose})
	return c.Abort()
}

// Abort closes the socket abruptly, as a network failure would. Pending
// publish callbacks are failed with ErrConnClosed.
func (c *Conn) Abort() error {
	c.wmu.Lock()
	if c.closed {
		c.wmu.Unlock()
		return nil
	}
	c.closed = true
	pending := c.pending
	c.pending = nil
	c.wmu.Unlock()
	err := c.ws.Close()
	for _, cb := range pending {
		cb(nil, ErrConnClosed)
	}
	return err
}

// ReadLoop reads frames until the connection ends, handling HEARTBEAT,
// ACK and NACK itself and passing the rest to h. It returns the reason
// the connection ended: ctx's error, a *ProtocolError for a
// DISCONNECTED or connection-level ERROR, or the transport error. The
// Conn is closed when ReadLoop returns.
func (c *Conn) ReadLoop(ctx context.Context, h Handler) error {
	stop := context.AfterFunc(ctx, func() { _ = c.Abort() })
	defer stop()
	defer func() { _ = c.Abort() }()
	for {
		_ = c.ws.SetReadDeadline(time.Now().Add(c.IdleTimeout))
		pm, err := c.read()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		switch pm.Action {
		case protocol.ActionHeartbeat:
			// Any frame proves liveness; the deadline is reset above.
		case protocol.ActionAck:
			c.resolve(pm.GetMsgSerial(), pm.Count, pm.Res, nil)
		case protocol.ActionNack:
			info := protocol.ErrorInfo{Message: "nack"}
			if pm.Error != nil {
				info = *pm.Error
			}
			c.resolve(pm.GetMsgSerial(), pm.Count, nil, &ProtocolError{Action: pm.Action, Info: info})
		case protocol.ActionDisconnected, protocol.ActionClosed:
			info := protocol.ErrorInfo{Message: pm.Action.String()}
			if pm.Error != nil {
				info = *pm.Error
			}
			return &ProtocolError{Action: pm.Action, Info: info}
		case protocol.ActionError:
			if pm.Channel != nil {
				h.OnFrame(c, pm)
				continue
			}
			info := protocol.ErrorInfo{Message: "error"}
			if pm.Error != nil {
				info = *pm.Error
			}
			return &ProtocolError{Action: pm.Action, Info: info}
		default:
			h.OnFrame(c, pm)
		}
	}
}

// resolve completes the publishes msgSerial .. msgSerial+count-1; res
// holds one result per publish in order, when the server sent them.
func (c *Conn) resolve(serial int64, count int, res []*protocol.PublishResult, err error) {
	if count < 1 {
		count = 1
	}
	c.wmu.Lock()
	type resolved struct {
		cb  ResultCallback
		res *protocol.PublishResult
	}
	cbs := make([]resolved, 0, count)
	for s := serial; s < serial+int64(count); s++ {
		if cb, ok := c.pending[s]; ok {
			r := resolved{cb: cb}
			if i := int(s - serial); i < len(res) {
				r.res = res[i]
			}
			cbs = append(cbs, r)
			delete(c.pending, s)
		}
	}
	c.wmu.Unlock()
	for _, r := range cbs {
		r.cb(r.res, err)
	}
}
