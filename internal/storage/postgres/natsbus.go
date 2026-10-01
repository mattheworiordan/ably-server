package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
)

// DefaultNATSInlineMaxBytes is the default cap on the encoded size of a
// cm carried inline on the NATS bus (DESIGN.md §7.2). A larger cm
// travels as a (channel, serial) pointer and receivers fetch it from the
// log. It sits well under NATS's default 1 MiB max_payload.
const DefaultNATSInlineMaxBytes = 256 << 10

// Subject scheme (DESIGN.md §7.2): one subject per Ably channel,
// "ably.cm.", a namespace token, ".", then the unpadded URL-safe base64
// of the channel name, which never contains a NATS token separator or
// wildcard. The namespace token is the hex of the first 8 bytes of
// SHA-256 of the cluster's deployment id (its cluster identity row,
// DESIGN.md §11), a NUL, and the schema the Storage works in
// (current_schema()), so two clusters sharing one NATS cluster never
// hear each other's channels, even with the same schema name. A name
// whose encoding exceeds natsMaxSubjectToken is hashed instead
// ("ably.cm.<ns>.h.<sha256 hex>"); the envelope carries the channel
// name and the deployment id, so a receiver drops anything not
// addressed to its channel of its cluster.
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

// NATS receive fan-in (DESIGN.md §7.2). Every channel subscription
// feeds one of natsDispatchShards bounded Go channels (ChanSubscribe),
// chosen by a hash of the channel name, and one worker goroutine per
// shard drains it into the channel's delivery point. The goroutine count
// is therefore fixed, whatever the number of bound channels; nats.go's
// async Subscribe would start one goroutine per subscription. One
// channel always lands on the same shard, so a channel's bus messages
// are dispatched in the order the connection read them. A shard whose
// queue is full makes NATS drop the message (a slow consumer); the
// chain recovers it from the log. natsPointerFetches bounds the pointer
// reads in flight: they run off the shard worker so a slow log read
// never stalls the other channels of its shard, and the chain holds any
// later cm of that channel until the pointer's body is in. A pointer
// that finds them all busy is handed to the delivery point without a
// body, and the gap fill reads it after the hold. Package vars so tests
// can shrink them; Open snapshots them.
var (
	natsDispatchShards   = 16
	natsDispatchQueueLen = 8192
	natsPointerFetches   = 32
)

// DefaultNATSSweepInterval is the nats bus's default watermark sweep
// interval (DESIGN.md §7.2).
const DefaultNATSSweepInterval = 30 * time.Second

// natsFutureSkew is how far ahead of the receiver's database-corrected
// clock an envelope's serial may be minted before the receiver drops it
// (DESIGN.md §7.2). Serials are minted from the database clock, so a
// genuine one is never more than one commit ahead of it; one far in the
// future would sort above every serial the channel mints for that long,
// and the delivery point would then drop them all as duplicates. A
// dropped genuine cm is recovered from the log by the chain or the sweep.
// natsFutureWarnEvery rate-limits the warning, per channel. Package vars
// so tests can shrink them.
var (
	natsFutureSkew      = 5 * time.Minute
	natsFutureWarnEvery = time.Minute
)

// natsNamespacePrefix is the subject prefix for every channel of one
// cluster and schema: "ably.cm.<ns>.".
func natsNamespacePrefix(deploymentID, schema string) string {
	sum := sha256.Sum256([]byte(deploymentID + "\x00" + schema))
	return natsSubjectPrefix + hex.EncodeToString(sum[:8]) + "."
}

// natsSubject derives the NATS subject for an Ably channel name under a
// namespace prefix (natsNamespacePrefix).
func natsSubject(prefix, channel string) string {
	enc := base64.RawURLEncoding.EncodeToString([]byte(channel))
	if len(enc) <= natsMaxSubjectToken {
		return prefix + enc
	}
	sum := sha256.Sum256([]byte(channel))
	return prefix + "h." + hex.EncodeToString(sum[:])
}

// natsEnvelope is the body of a NATS bus message: the channel, the cm's
// serial and its predecessor's, and the cm itself exactly as the
// publish stored it. CM is nil for a pointer. Annotation.Summary is
// msgpack:"-" (the log carries it in its own column), so the summary
// snapshots ride alongside, index-aligned with CM.Annotations. SentAt is
// when the publishing node sent it, straight after the commit (Unix
// nanoseconds), from which a receiver measures the bus delivery lag.
// Deployment is the publishing cluster's deployment id (DESIGN.md §11);
// a receiver drops an envelope of another cluster.
type natsEnvelope struct {
	_msgpack   struct{} `msgpack:",as_array"`
	Channel    string
	Serial     string
	Prev       string
	CM         *protocol.ChannelMessage
	Summaries  [][]byte
	SentAt     int64
	Deployment string
}

// natsEnvelopeError is why decodeNATSEnvelope refused a message: reason
// is the ably_bus_malformed_total label ("decode" or "serial").
type natsEnvelopeError struct {
	reason string
	err    error
}

func (e *natsEnvelopeError) Error() string { return e.err.Error() }
func (e *natsEnvelopeError) Unwrap() error { return e.err }

// encodeNATSEnvelope encodes cm for the bus, inline when the encoding
// fits within inlineMax and as a pointer otherwise.
func encodeNATSEnvelope(deployment, channel string, cm *protocol.ChannelMessage, prev string, inlineMax int) (data []byte, pointer bool, err error) {
	env := natsEnvelope{Channel: channel, Serial: cm.ChannelSerial, Prev: prev, CM: cm, SentAt: time.Now().UnixNano(), Deployment: deployment}
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

// decodedEnvelope is a bus message decodeNATSEnvelope accepted: the
// event it announces, the channel and cluster it is addressed to, and
// its serial's mint time (ms since epoch).
type decodedEnvelope struct {
	ev         busEvent
	channel    string
	deployment string
	mintedMs   int64
}

// decodeNATSEnvelope decodes a bus message and checks its serials: the
// serial and a non-empty predecessor must be channelSerials in the
// fixed-width form the database mints (serial.ParseChannelSerial,
// DESIGN.md §8), and the predecessor must sort below the serial. A
// refusal is a *natsEnvelopeError. The body is not otherwise checked:
// the bus is a trusted network (DESIGN.md §7.2).
func decodeNATSEnvelope(data []byte) (decodedEnvelope, error) {
	var env natsEnvelope
	if err := msgpack.Unmarshal(data, &env); err != nil {
		return decodedEnvelope{}, &natsEnvelopeError{"decode", err}
	}
	minted, err := serial.ParseChannelSerial(env.Serial)
	if err != nil {
		return decodedEnvelope{}, &natsEnvelopeError{"serial", err}
	}
	if env.Prev != "" {
		if _, err := serial.ParseChannelSerial(env.Prev); err != nil {
			return decodedEnvelope{}, &natsEnvelopeError{"serial", fmt.Errorf("predecessor: %w", err)}
		}
		if env.Prev >= env.Serial {
			return decodedEnvelope{}, &natsEnvelopeError{"serial", fmt.Errorf("predecessor %q does not sort below serial %q", env.Prev, env.Serial)}
		}
	}
	if env.CM != nil {
		env.CM.ChannelSerial = env.Serial
		for i, a := range env.CM.Annotations {
			if i >= len(env.Summaries) || len(env.Summaries[i]) == 0 {
				continue
			}
			if err := msgpack.Unmarshal(env.Summaries[i], &a.Summary); err != nil {
				return decodedEnvelope{}, &natsEnvelopeError{"decode", fmt.Errorf("decode annotation summary %d: %w", i, err)}
			}
		}
	}
	return decodedEnvelope{
		ev:         busEvent{serial: env.Serial, prev: env.Prev, cm: env.CM, sentAt: env.SentAt},
		channel:    env.Channel,
		deployment: env.Deployment,
		mintedMs:   minted,
	}, nil
}

// natsBus is the NATS core pub/sub Bus (DESIGN.md §7.2). After a publish
// commits, the publishing node publishes the cm (or a pointer) to the
// channel's subject and appends it locally straight away. A node
// subscribes to a channel's subject when it first binds the channel, so
// it receives only publishes for channels it holds. Subscriptions feed a
// fixed set of dispatch shards (natsDispatchShards), so the goroutine
// count does not grow with bound channels. Deliveries are chained on the
// predecessor serial (chain.go) because NATS orders per publishing
// connection, not across nodes.
//
// The URL may list several servers of one NATS cluster, comma-separated.
// The client connects to one, learns the rest from the cluster, and on a
// disconnect moves to another; every reconnect re-sends the node's
// subscriptions and triggers a reconcile of every bound channel from the
// log, because NATS core does not replay what was published while the
// node was away.
type natsBus struct {
	s          *Storage
	nc         *nats.Conn
	inlineMax  int
	deployment string // the cluster's deployment id (DESIGN.md §11)
	prefix     string // natsNamespacePrefix(deployment, schema)

	// futureSkew and warnEvery are natsFutureSkew and
	// natsFutureWarnEvery, snapshotted at dial; futureWarned is when each
	// channel last logged a far-future serial (dispatch workers share it).
	futureSkew, warnEvery time.Duration
	futureMu              sync.Mutex
	futureWarned          map[string]time.Time

	// Tuning snapshotted at dial so goroutines never read the vars.
	flushTimeout, retryWait time.Duration

	// shards are the dispatch queues every subscription feeds; one
	// worker per shard drains each (dispatch). fetches bounds the pointer
	// reads in flight (a counting semaphore).
	shards  []chan *nats.Msg
	fetches chan struct{}
}

// dialNATSBus connects the bus. Like the LISTEN connection, the first
// dial must succeed (fail fast on a bad URL); after that the client
// reconnects indefinitely, across the cluster's servers, and every
// reconnect triggers a reconcile.
func dialNATSBus(s *Storage, opts Options) (*natsBus, error) {
	b := &natsBus{
		s:            s,
		inlineMax:    opts.NATSInlineMaxBytes,
		deployment:   s.deploymentID,
		prefix:       natsNamespacePrefix(s.deploymentID, s.namespace),
		futureSkew:   natsFutureSkew,
		warnEvery:    natsFutureWarnEvery,
		futureWarned: make(map[string]time.Time),
		flushTimeout: natsFlushTimeout,
		retryWait:    natsReconcileRetryWait,
	}
	if b.inlineMax <= 0 {
		b.inlineMax = DefaultNATSInlineMaxBytes
	}
	b.shards = make([]chan *nats.Msg, max(natsDispatchShards, 1))
	for i := range b.shards {
		b.shards[i] = make(chan *nats.Msg, max(natsDispatchQueueLen, 1))
	}
	b.fetches = make(chan struct{}, max(natsPointerFetches, 1))
	security, err := natsSecurityOptions(opts)
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(opts.NATSURL, append(security,
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
			select {
			case <-s.done:
				// Closing: the dispatch workers have stopped and the shards
				// fill until the connection closes. Nothing is lost that
				// matters, and a warning per subscription is noise.
				return
			default:
			}
			if errors.Is(err, nats.ErrSlowConsumer) {
				// A dispatch shard was full and NATS dropped messages for
				// this subscription (reported once per slow-consumer
				// episode); the chain sees the gap on the next message, or
				// the sweep does.
				s.stats.drops.Add(1)
			}
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			s.logger.Warn("storage/postgres: NATS bus error", "subject", subject, "err", err)
		}),
	)...)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: connect NATS bus: %w", err)
	}
	b.nc = nc
	return b, nil
}

// natsSecurityOptions returns the connect options for the bus's
// credentials and TLS settings (DESIGN.md §7.2, §9). A client
// certificate needs its key and the other way round. Anything given in
// the URL (nats://user:pass@host, tls://host) applies as well.
func natsSecurityOptions(opts Options) ([]nats.Option, error) {
	var out []nats.Option
	if opts.NATSCredsFile != "" {
		if _, err := os.Stat(opts.NATSCredsFile); err != nil {
			return nil, fmt.Errorf("storage/postgres: NATS credentials file: %w", err)
		}
		out = append(out, nats.UserCredentials(opts.NATSCredsFile))
	}
	if opts.NATSTLSCA != "" {
		out = append(out, nats.RootCAs(opts.NATSTLSCA))
	}
	switch {
	case opts.NATSTLSCert != "" && opts.NATSTLSKey != "":
		out = append(out, nats.ClientCert(opts.NATSTLSCert, opts.NATSTLSKey))
	case opts.NATSTLSCert != "" || opts.NATSTLSKey != "":
		return nil, errors.New("storage/postgres: a NATS client certificate needs both --nats-tls-cert and --nats-tls-key")
	}
	return out, nil
}

// start runs one dispatch worker per shard and the shared chain loop:
// the reconnect reconcile, which first flushes so the server has
// registered every re-sent SUB, and the watermark sweep.
func (b *natsBus) start(ctx context.Context) {
	for _, q := range b.shards {
		b.s.wg.Add(1)
		go b.dispatch(ctx, q)
	}
	b.s.startChainLoop(ctx, b.retryWait, b.flush)
}

// shardFor returns the dispatch queue a channel's subscription feeds.
// The same channel always maps to the same shard, which keeps its bus
// messages in the order the connection read them.
func (b *natsBus) shardFor(channel string) chan *nats.Msg {
	return b.shards[shardIndex(channel, len(b.shards))]
}

// shardIndex hashes a channel name onto n shards (FNV-1a).
func shardIndex(channel string, n int) int {
	h := uint32(2166136261)
	for i := 0; i < len(channel); i++ {
		h ^= uint32(channel[i])
		h *= 16777619
	}
	return int(h % uint32(n))
}

// dispatch drains one shard until the Storage closes. Messages still
// queued at close are dropped; they are in the log.
func (b *natsBus) dispatch(ctx context.Context, q <-chan *nats.Msg) {
	defer b.s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-q:
			b.handle(m)
		}
	}
}

// handle routes one bus message to the delivery point of the channel it
// is addressed to (DESIGN.md §7.2). A message whose envelope does not
// decode, or whose serials are not well formed, is malformed. One of
// another cluster (its deployment id is not this cluster's) is unrouted
// as foreign. The envelope names the channel; a message whose subject is
// not that channel's (a hashed-subject collision), or for a channel no
// longer bound here (released while the message was queued), is
// unrouted. A serial minted more than futureSkew ahead of the database
// clock is dropped and counted as malformed (future). A pointer's body
// is read off the shard worker, bounded by natsPointerFetches; beyond
// that the gap fill reads it.
func (b *natsBus) handle(m *nats.Msg) {
	b.s.stats.received.Add(1)
	env, err := decodeNATSEnvelope(m.Data)
	if err != nil {
		b.s.stats.malformed.Add(1)
		var ee *natsEnvelopeError
		if errors.As(err, &ee) && ee.reason == "serial" {
			b.s.stats.malformedSerial.Add(1)
		}
		b.s.logger.Warn("storage/postgres: malformed NATS bus message dropped", "subject", m.Subject, "err", err)
		return
	}
	if env.deployment != b.deployment {
		b.s.stats.unrouted.Add(1)
		b.s.stats.unroutedForeign.Add(1)
		return
	}
	ev, channel := env.ev, env.channel
	var cs *channelStore
	if m.Subject == natsSubject(b.prefix, channel) {
		cs = b.s.boundStore(channel)
	}
	if cs == nil {
		b.s.stats.unrouted.Add(1)
		return
	}
	if limit := time.Now().UnixMilli() + b.s.clockOffset.Load() + b.futureSkew.Milliseconds(); env.mintedMs > limit {
		b.s.stats.malformed.Add(1)
		b.s.stats.malformedFuture.Add(1)
		b.warnFuture(channel, ev.serial, env.mintedMs-limit+b.futureSkew.Milliseconds())
		return
	}
	if ev.sentAt > 0 {
		b.s.stats.observeStage(stageQueueWait, time.Since(time.Unix(0, ev.sentAt)))
	}
	if ev.cm != nil {
		cs.deliverChained(ev)
		return
	}
	select {
	case b.fetches <- struct{}{}:
	default:
		// natsPointerFetches reads are in flight already. Never block the
		// shard on them: offer the pointer without a body, which the
		// delivery point holds, and the gap fill reads it from the log
		// after the hold.
		cs.deliverChained(ev)
		return
	}
	b.s.wg.Add(1)
	go func() {
		defer b.s.wg.Done()
		defer func() { <-b.fetches }()
		cs.deliverChained(cs.resolvePointer(ev))
	}()
}

// warnFuture logs a far-future serial dropped on channel, at most once
// per warnEvery per channel.
func (b *natsBus) warnFuture(channel, cmSerial string, aheadMs int64) {
	now := time.Now()
	b.futureMu.Lock()
	last, ok := b.futureWarned[channel]
	if ok && now.Sub(last) < b.warnEvery {
		b.futureMu.Unlock()
		return
	}
	if len(b.futureWarned) >= 1024 {
		for ch, at := range b.futureWarned {
			if now.Sub(at) >= b.warnEvery {
				delete(b.futureWarned, ch)
			}
		}
	}
	b.futureWarned[channel] = now
	b.futureMu.Unlock()
	b.s.logger.Warn("storage/postgres: NATS bus message with a serial minted in the future dropped; the chain or the sweep recovers a genuine cm from the log",
		"channel", channel, "serial", cmSerial, "ahead", time.Duration(aheadMs)*time.Millisecond)
}

// queueDepth is the number of bus messages received and waiting in the
// dispatch shards (ably_bus_receive_queue_depth).
func (b *natsBus) queueDepth() int {
	n := 0
	for _, q := range b.shards {
		n += len(q)
	}
	return n
}

// flush round-trips to the server, confirming it has processed
// everything the client sent before it (on a reconnect: every SUB).
func (b *natsBus) flush(ctx context.Context) error {
	fctx, cancel := context.WithTimeout(ctx, b.flushTimeout)
	defer cancel()
	return b.nc.FlushWithContext(fctx)
}

func (b *natsBus) chains() bool { return true }

// bind subscribes to the channel's subject, feeding the channel's
// dispatch shard, then round-trips to the server so the SUB is
// registered before Storage.Channel reads the watermark: any cm
// committed after that read is then published after the SUB took effect
// and is received. Messages that arrive before the channel is seeded are
// held by the delivery point. The store is already in the Storage's
// channel map (Storage.Channel puts it there before bind), so a message
// that reaches the shard at once finds it.
func (b *natsBus) bind(ctx context.Context, cs *channelStore) error {
	sub, err := b.nc.ChanSubscribe(natsSubject(b.prefix, cs.name), b.shardFor(cs.name))
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

// unbind unsubscribes the channel's subject. A message already queued in
// a shard finds the store released (or gone) and delivers nothing.
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
	data, pointer, err := encodeNATSEnvelope(b.deployment, cs.name, cm, prev, b.inlineMax)
	switch {
	case err != nil:
		b.s.stats.publishErrors.Add(1)
		b.s.logger.Warn("storage/postgres: NATS bus encode failed; remote nodes recover the cm from the log", "channel", cs.name, "serial", cm.ChannelSerial, "err", err)
	default:
		if pointer {
			b.s.stats.pointers.Add(1)
		}
		if err := b.nc.Publish(natsSubject(b.prefix, cs.name), data); err != nil {
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
