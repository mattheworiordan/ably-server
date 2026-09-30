package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// DefaultNATSInlineMaxBytes is the default cap on the encoded size of a
// cm carried inline on the NATS bus (DESIGN.md §7.2). A larger cm
// travels as a (channel, serial) pointer and receivers fetch it from the
// log. It sits well under NATS's default 1 MiB max_payload.
const DefaultNATSInlineMaxBytes = 256 << 10

// Subject scheme (DESIGN.md §7.2): one subject per Ably channel,
// "ably.cm." plus the unpadded URL-safe base64 of the channel name,
// which never contains a NATS token separator or wildcard. A name
// whose encoding exceeds natsMaxSubjectToken is hashed instead
// ("ably.cm.h.<sha256 hex>"); the envelope carries the channel name,
// so a receiver drops anything not addressed to its channel.
const (
	natsSubjectPrefix   = "ably.cm."
	natsMaxSubjectToken = 200
)

// NATS connection tuning. Package vars so integration tests can shrink
// them; effectively constant in production. natsPingInterval and
// natsMaxPingsOut bound how long a silently dead server (a partition,
// not a closed socket) goes unnoticed before the client moves to another
// server in the cluster.
var (
	natsReconnectWait      = 250 * time.Millisecond
	natsFlushTimeout       = 5 * time.Second
	natsReconcileRetryWait = 250 * time.Millisecond
	natsPingInterval       = 5 * time.Second
	natsMaxPingsOut        = 2
)

// DefaultNATSSweepInterval is the nats bus's default watermark sweep
// interval (DESIGN.md §7.2).
const DefaultNATSSweepInterval = 5 * time.Second

// natsSubject derives the NATS subject for an Ably channel name.
func natsSubject(channel string) string {
	enc := base64.RawURLEncoding.EncodeToString([]byte(channel))
	if len(enc) <= natsMaxSubjectToken {
		return natsSubjectPrefix + enc
	}
	sum := sha256.Sum256([]byte(channel))
	return natsSubjectPrefix + "h." + hex.EncodeToString(sum[:])
}

// natsEnvelope is the body of a NATS bus message: the channel, the cm's
// serial and its predecessor's, and the cm itself exactly as the
// publish stored it. CM is nil for a pointer. Annotation.Summary is
// msgpack:"-" (the log carries it in its own column), so the summary
// snapshots ride alongside, index-aligned with CM.Annotations.
type natsEnvelope struct {
	_msgpack  struct{} `msgpack:",as_array"`
	Channel   string
	Serial    string
	Prev      string
	CM        *protocol.ChannelMessage
	Summaries [][]byte
}

// encodeNATSEnvelope encodes cm for the bus, inline when the encoding
// fits within inlineMax and as a pointer otherwise.
func encodeNATSEnvelope(channel string, cm *protocol.ChannelMessage, prev string, inlineMax int) (data []byte, pointer bool, err error) {
	env := natsEnvelope{Channel: channel, Serial: cm.ChannelSerial, Prev: prev, CM: cm}
	if len(cm.Annotations) > 0 {
		env.Summaries = make([][]byte, len(cm.Annotations))
		for i, a := range cm.Annotations {
			if a.Summary == nil {
				continue
			}
			if env.Summaries[i], err = msgpack.Marshal(a.Summary); err != nil {
				env.CM, env.Summaries = nil, nil
				break
			}
		}
	}
	if env.CM != nil {
		data, err = msgpack.Marshal(&env)
		if err == nil && len(data) <= inlineMax {
			return data, false, nil
		}
	}
	env.CM, env.Summaries = nil, nil
	data, err = msgpack.Marshal(&env)
	if err != nil {
		return nil, true, fmt.Errorf("storage/postgres: encode NATS pointer: %w", err)
	}
	return data, true, nil
}

// decodeNATSEnvelope decodes a bus message into the event it announces
// and the channel it is addressed to.
func decodeNATSEnvelope(data []byte) (busEvent, string, error) {
	var env natsEnvelope
	if err := msgpack.Unmarshal(data, &env); err != nil {
		return busEvent{}, "", err
	}
	if env.Serial == "" {
		return busEvent{}, "", errors.New("envelope has no serial")
	}
	if env.CM != nil {
		env.CM.ChannelSerial = env.Serial
		for i, a := range env.CM.Annotations {
			if i >= len(env.Summaries) || len(env.Summaries[i]) == 0 {
				continue
			}
			if err := msgpack.Unmarshal(env.Summaries[i], &a.Summary); err != nil {
				return busEvent{}, "", fmt.Errorf("decode annotation summary %d: %w", i, err)
			}
		}
	}
	return busEvent{serial: env.Serial, prev: env.Prev, cm: env.CM}, env.Channel, nil
}

// natsBus is the NATS core pub/sub Bus (DESIGN.md §7.2). After a publish
// commits, the publishing node publishes the cm (or a pointer) to the
// channel's subject and appends it locally straight away. A node
// subscribes to a channel's subject when it first binds the channel, so
// it receives only publishes for channels it holds; each subscription
// runs its own delivery goroutine, so channels do not queue behind one
// another. Deliveries are chained on the predecessor serial (chain.go)
// because NATS orders per publishing connection, not across nodes.
//
// The URL may list several servers of one NATS cluster, comma-separated.
// The client connects to one, learns the rest from the cluster, and on a
// disconnect moves to another; every reconnect re-sends the node's
// subscriptions and triggers a reconcile of every bound channel from the
// log, because NATS core does not replay what was published while the
// node was away.
type natsBus struct {
	s         *Storage
	nc        *nats.Conn
	inlineMax int

	// Tuning snapshotted at dial so goroutines never read the vars.
	flushTimeout, retryWait time.Duration
}

// dialNATSBus connects the bus. Like the LISTEN connection, the first
// dial must succeed (fail fast on a bad URL); after that the client
// reconnects indefinitely, across the cluster's servers, and every
// reconnect triggers a reconcile.
func dialNATSBus(s *Storage, opts Options) (*natsBus, error) {
	b := &natsBus{
		s:            s,
		inlineMax:    opts.NATSInlineMaxBytes,
		flushTimeout: natsFlushTimeout,
		retryWait:    natsReconcileRetryWait,
	}
	if b.inlineMax <= 0 {
		b.inlineMax = DefaultNATSInlineMaxBytes
	}
	nc, err := nats.Connect(opts.NATSURL,
		nats.Name("ably-server/"+s.node),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(natsReconnectWait),
		nats.PingInterval(natsPingInterval),
		nats.MaxPingsOutstanding(natsMaxPingsOut),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			s.logger.Warn("storage/postgres: NATS bus disconnected; reconnecting", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			s.logger.Info("storage/postgres: NATS bus reconnected; reconciling bound channels", "server", nc.ConnectedUrlRedacted())
			s.requestReconcile()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				// NATS dropped messages for this subscription; the chain
				// sees the gap on the next message, or the sweep does.
				s.stats.drops.Add(1)
			}
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			s.logger.Warn("storage/postgres: NATS bus error", "subject", subject, "err", err)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: connect NATS bus: %w", err)
	}
	b.nc = nc
	return b, nil
}

// start runs the shared chain loop: the reconnect reconcile, which first
// flushes so the server has registered every re-sent SUB, and the
// watermark sweep.
func (b *natsBus) start(ctx context.Context) {
	b.s.startChainLoop(ctx, b.retryWait, b.flush)
}

// flush round-trips to the server, confirming it has processed
// everything the client sent before it (on a reconnect: every SUB).
func (b *natsBus) flush(ctx context.Context) error {
	fctx, cancel := context.WithTimeout(ctx, b.flushTimeout)
	defer cancel()
	return b.nc.FlushWithContext(fctx)
}

func (b *natsBus) chains() bool { return true }

// bind subscribes to the channel's subject, then round-trips to the
// server so the SUB is registered before Storage.Channel reads the
// watermark: any cm committed after that read is then published after
// the SUB took effect and is received. Messages that arrive before the
// channel is seeded are held by the delivery point.
func (b *natsBus) bind(ctx context.Context, cs *channelStore) error {
	sub, err := b.nc.Subscribe(natsSubject(cs.name), func(m *nats.Msg) {
		b.s.stats.received.Add(1)
		ev, channel, err := decodeNATSEnvelope(m.Data)
		if err != nil {
			b.s.stats.malformed.Add(1)
			b.s.logger.Warn("storage/postgres: undecodable NATS bus message", "subject", m.Subject, "err", err)
			return
		}
		if channel != cs.name {
			b.s.stats.unrouted.Add(1)
			return // a hashed-subject collision: not addressed to this channel
		}
		cs.deliverChained(cs.resolvePointer(ev))
	})
	if err != nil {
		return fmt.Errorf("storage/postgres: NATS subscribe %q: %w", cs.name, err)
	}
	cs.subMu.Lock()
	cs.sub = sub
	cs.subMu.Unlock()

	// In a NATS cluster the flush confirms only that this node's server
	// has the SUB; a publish through another server in the moments before
	// the interest reaches it over the route can miss this node. That cm
	// is late, not lost: the next message's predecessor or the sweep
	// recovers it from the log (DESIGN.md §7.2).
	if err := b.flush(ctx); err != nil {
		// Not fatal: while the connection is down the SUB is re-sent on
		// reconnect, which reconciles every bound channel, and the
		// watermark sweep catches anything later.
		b.s.logger.Warn("storage/postgres: NATS flush after subscribe failed", "channel", cs.name, "err", err)
	}
	return nil
}

// unbind unsubscribes the channel's subject. A callback already running
// finds the store released and delivers nothing.
func (b *natsBus) unbind(cs *channelStore) {
	cs.subMu.Lock()
	sub := cs.sub
	cs.sub = nil
	cs.subMu.Unlock()
	if sub != nil {
		_ = sub.Unsubscribe()
	}
}

// beforeCommit is a no-op: nothing may leave the node before the
// publish is durable.
func (b *natsBus) beforeCommit(context.Context, pgx.Tx, *channelStore, *busWrite) error {
	return nil
}

// afterCommit publishes the committed cm to its channel's subject and
// takes the publisher fast path: the cm goes straight to this node's own
// delivery point for the channel, without waiting for the bus. The echo
// that the subscription later receives is then a duplicate the mark
// drops. The fast path targets the channel's bound store, not cs, so a
// publish through a transient store (the presence reaper's) still
// reaches local subscribers.
func (b *natsBus) afterCommit(cs *channelStore, cm *protocol.ChannelMessage, prev string) {
	data, pointer, err := encodeNATSEnvelope(cs.name, cm, prev, b.inlineMax)
	switch {
	case err != nil:
		b.s.stats.publishErrors.Add(1)
		b.s.logger.Warn("storage/postgres: NATS bus encode failed; remote nodes recover the cm from the log", "channel", cs.name, "serial", cm.ChannelSerial, "err", err)
	default:
		if pointer {
			b.s.stats.pointers.Add(1)
		}
		if err := b.nc.Publish(natsSubject(cs.name), data); err != nil {
			b.s.stats.publishErrors.Add(1)
			b.s.logger.Warn("storage/postgres: NATS bus publish failed; remote nodes recover the cm from the log", "channel", cs.name, "serial", cm.ChannelSerial, "err", err)
		} else {
			b.s.stats.published.Add(1)
		}
	}
	b.s.fastPath(cs.name, cm, prev)
}

// errNATSDisconnected is ready's answer while the NATS connection is
// down: the node cannot deliver cross-node, so /readyz takes it out of
// rotation until the client reconnects (DESIGN.md §7.2).
var errNATSDisconnected = errors.New("storage/postgres: NATS bus not connected")

func (b *natsBus) ready() error {
	if !b.nc.IsConnected() {
		return errNATSDisconnected
	}
	return nil
}

func (b *natsBus) isConnected() bool { return b.nc.IsConnected() }

// close drops the NATS connection, which also ends every subscription.
func (b *natsBus) close() {
	b.nc.Close()
}
