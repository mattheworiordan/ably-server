package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// DefaultNATSInlineMaxBytes is the default cap on the encoded size of a
// cm carried inline on the NATS bus (DESIGN.md §7.3). A larger cm
// travels as a (channel, serial) pointer and receivers fetch it from the
// log. It sits well under NATS's default 1 MiB max_payload.
const DefaultNATSInlineMaxBytes = 256 << 10

// Subject scheme (DESIGN.md §7.3): one subject per Ably channel,
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
// them; effectively constant in production.
var (
	natsReconnectWait      = 250 * time.Millisecond
	natsFlushTimeout       = 5 * time.Second
	natsWatermarkInterval  = 5 * time.Second
	natsReconcileRetryWait = 250 * time.Millisecond
)

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

// natsBus is the NATS core pub/sub Bus (DESIGN.md §7.3). After a publish
// commits, the publishing node publishes the cm (or a pointer) to the
// channel's subject and appends it locally straight away. A node
// subscribes to a channel's subject when it first binds the channel, so
// it receives only publishes for channels it holds; each subscription
// runs its own delivery goroutine, so channels do not queue behind one
// another. Deliveries are chained on the predecessor serial because
// NATS orders per publishing connection, not across nodes.
type natsBus struct {
	s           *Storage
	nc          *nats.Conn
	inlineMax   int
	reconcileCh chan struct{}

	// Tuning snapshotted at dial so goroutines never read the vars.
	flushTimeout, sweepInterval, retryWait time.Duration

	// Counters for tests; the obvious seed for a metrics surface.
	reconciles  atomic.Int64 // completed reconnect reconciles
	sweeps      atomic.Int64 // completed watermark sweeps
	pointers    atomic.Int64 // cms published as pointers
	publishErrs atomic.Int64 // NATS publishes that failed after commit
}

// dialNATSBus connects the bus. Like the LISTEN connection, the first
// dial must succeed (fail fast on a bad URL); after that the client
// reconnects indefinitely and every reconnect triggers a reconcile.
func dialNATSBus(s *Storage, opts Options) (*natsBus, error) {
	b := &natsBus{
		s:             s,
		inlineMax:     opts.NATSInlineMaxBytes,
		reconcileCh:   make(chan struct{}, 1),
		flushTimeout:  natsFlushTimeout,
		sweepInterval: natsWatermarkInterval,
		retryWait:     natsReconcileRetryWait,
	}
	if b.inlineMax <= 0 {
		b.inlineMax = DefaultNATSInlineMaxBytes
	}
	nc, err := nats.Connect(opts.NATSURL,
		nats.Name("ably-server/"+s.node),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(natsReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			s.logger.Warn("storage/postgres: NATS bus disconnected; reconnecting", "err", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			s.logger.Info("storage/postgres: NATS bus reconnected; reconciling bound channels")
			b.requestReconcile()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
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

func (b *natsBus) start(ctx context.Context) {
	b.s.wg.Add(1)
	go b.reconcileLoop(ctx)
}

func (b *natsBus) chains() bool { return true }

// bind subscribes to the channel's subject, then round-trips to the
// server so the SUB is registered before Storage.Channel reads the
// watermark: any cm committed after that read is then published after
// the SUB took effect and is received. Messages that arrive before the
// channel is seeded are held by the delivery point.
func (b *natsBus) bind(ctx context.Context, cs *channelStore) error {
	sub, err := b.nc.Subscribe(natsSubject(cs.name), func(m *nats.Msg) {
		ev, channel, err := decodeNATSEnvelope(m.Data)
		if err != nil {
			b.s.logger.Warn("storage/postgres: undecodable NATS bus message", "subject", m.Subject, "err", err)
			return
		}
		if channel != cs.name {
			return // a hashed-subject collision: not addressed to this channel
		}
		cs.deliverChained(ev)
	})
	if err != nil {
		return fmt.Errorf("storage/postgres: NATS subscribe %q: %w", cs.name, err)
	}
	cs.sub = sub

	fctx, cancel := context.WithTimeout(ctx, b.flushTimeout)
	defer cancel()
	if err := b.nc.FlushWithContext(fctx); err != nil {
		// Not fatal: while the connection is down the SUB is re-sent on
		// reconnect, which reconciles every bound channel, and the
		// watermark sweep catches anything later.
		b.s.logger.Warn("storage/postgres: NATS flush after subscribe failed", "channel", cs.name, "err", err)
	}
	return nil
}

func (b *natsBus) unbind(cs *channelStore) {
	if cs.sub != nil {
		_ = cs.sub.Unsubscribe()
		cs.sub = nil
	}
}

// beforeCommit is a no-op: nothing may leave the node before the
// publish is durable.
func (b *natsBus) beforeCommit(context.Context, pgx.Tx, string, string) error { return nil }

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
		b.publishErrs.Add(1)
		b.s.logger.Warn("storage/postgres: NATS bus encode failed; remote nodes recover the cm from the log", "channel", cs.name, "serial", cm.ChannelSerial, "err", err)
	default:
		if pointer {
			b.pointers.Add(1)
		}
		if err := b.nc.Publish(natsSubject(cs.name), data); err != nil {
			b.publishErrs.Add(1)
			b.s.logger.Warn("storage/postgres: NATS bus publish failed; remote nodes recover the cm from the log", "channel", cs.name, "serial", cm.ChannelSerial, "err", err)
		}
	}
	if bound := b.s.boundStore(cs.name); bound != nil {
		bound.deliverChained(busEvent{serial: cm.ChannelSerial, prev: prev, cm: cm})
	}
}

// close drops the NATS connection, which also ends every subscription.
func (b *natsBus) close() {
	b.nc.Close()
}

// requestReconcile queues a reconcile; requests coalesce.
func (b *natsBus) requestReconcile() {
	select {
	case b.reconcileCh <- struct{}{}:
	default:
	}
}

// reconcileLoop runs the reconnect reconcile and the periodic watermark
// sweep, off the NATS client's callback goroutine.
//
// Reconcile (DESIGN.md §7.3): NATS core is at-most-once, so publishes
// made while this node was disconnected are gone. After a reconnect the
// client has re-sent every SUB; a flush round-trip confirms the server
// has registered them, and only then is each bound channel replayed from
// its mark. Any cm committed after that read is published after the
// SUBs took effect and arrives on the bus. This is the LISTEN broker's
// re-LISTEN-then-reconcile ordering (§7.2).
//
// Sweep: a cm whose bus message was lost with no later publish on the
// channel (the publisher died between commit and publish, a slow
// consumer dropped it, a reconnect buffer overflowed) leaves no gap for
// the chain to notice. Every natsWatermarkInterval the node reads the
// watermark of every bound channel in one query and catches up any
// channel still behind a watermark it saw on the previous sweep, so no
// committed cm stays undelivered for more than about two intervals.
func (b *natsBus) reconcileLoop(ctx context.Context) {
	defer b.s.wg.Done()
	sweep := time.NewTicker(b.sweepInterval)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweep.C:
			if err := b.s.sweepWatermarks(ctx); err != nil && ctx.Err() == nil {
				b.s.logger.Warn("storage/postgres: bus watermark sweep failed", "err", err)
			}
			b.sweeps.Add(1)
			continue
		case <-b.reconcileCh:
		}

		fctx, cancel := context.WithTimeout(ctx, b.flushTimeout)
		err := b.nc.FlushWithContext(fctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.s.logger.Warn("storage/postgres: NATS flush before reconcile failed; retrying", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(b.retryWait):
			}
			b.requestReconcile()
			continue
		}
		for _, cs := range b.s.boundStores() {
			if err := cs.catchUp(ctx); err != nil && ctx.Err() == nil {
				b.s.logger.Warn("storage/postgres: NATS bus reconcile failed", "channel", cs.name, "err", err)
			}
		}
		b.reconciles.Add(1)
		b.s.logger.Info("storage/postgres: NATS bus reconciled")
	}
}
