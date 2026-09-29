// Package natsjs is an experimental storage backend on NATS JetStream. It
// is a feasibility spike for the storage boundary (internal/storage,
// DESIGN.md §6.4): it implements storage.Storage and storage.ChannelStore
// with no Postgres and no authoritative process-local state, so any number
// of ably-server nodes can share one JetStream cluster.
//
// Mapping:
//
//   - The log. Channels are hashed across Options.Shards streams named
//     "<Prefix>_LOG_<k>". A channel's cms go to one subject per kind,
//     "<Prefix>.log.<k>.<chan>.{m,p,a}", where <chan> is the base64url
//     channel name (Ably names may contain '.', '*' and '>', which NATS
//     subjects reserve). The payload is the msgpack ChannelMessage plus
//     the annotation summary snapshots (which the protocol type does not
//     persist). The channelSerial also rides in the Ably-Serial header.
//   - Ordering. Serials are minted by the serial package, never taken from
//     stream sequences. Each append is a compare-and-set: the publish
//     carries Nats-Expected-Last-Subject-Sequence for the channel's
//     wildcard subject "<...>.<chan>.*", and the serial is minted as the
//     successor of the channel's last serial. JetStream rejects the
//     publish if any node appended to the channel in between, so stream
//     order, serial order and delivery order are one order across every
//     node. This replaces the Postgres channels-row lock.
//   - Idempotency. The first client-supplied id rides in Nats-Msg-Id,
//     namespaced by channel, so the stream's duplicate window rejects a
//     repeat atomically with the append and returns the original's
//     sequence.
//   - Projections. The latest-version view, the versions index, the
//     annotations index and the summary are derived from the log when
//     read, with subject-filtered scans: every mutation cm carries the
//     complete merged version and every annotation cm the post-fold
//     summary, so the log alone is the source of truth. The presence
//     membership set must outlive log retention, so it lives in a KV
//     bucket ("<Prefix>_presence"), folded last-writer-wins by serial.
//     Each channel's immutable initial serial lives in "<Prefix>_channels".
//   - Delivery. Each channel bound with an Appender gets an ordered
//     consumer on its wildcard subject, starting after the watermark
//     handed to Initialize. It is the only path to the Appender, for local
//     and remote publishes alike (DESIGN.md §7.2), and it re-syncs itself
//     after a disconnect, so there is no separate reconcile path.
//   - Retention. Stream MaxAge and MaxMsgsPerSubject (per channel, per
//     kind).
package natsjs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

const (
	// hdrSerial carries a cm's channelSerial so reads that need only the
	// serial (the head lookup) never decode the payload.
	hdrSerial = "Ably-Serial"

	defaultPrefix          = "ABLY"
	defaultDuplicateWindow = 2 * time.Minute

	// maxCASAttempts bounds the optimistic-concurrency retry loop for one
	// append. A retry happens only when another node appended to the same
	// channel between our head read and our publish.
	maxCASAttempts = 64
)

// Subject tokens for the three cm kinds that share a channel's log.
const (
	kindTokMessage    = "m"
	kindTokPresence   = "p"
	kindTokAnnotation = "a"
)

// Options configures the JetStream backend.
type Options struct {
	// URL is the NATS server URL (a comma-separated list is accepted).
	// Required unless Conn is set.
	URL string

	// Conn, when non-nil, is used instead of dialling URL. Close does not
	// close a supplied Conn.
	Conn *nats.Conn

	// Prefix namespaces every stream, bucket and subject this Storage
	// uses, so several deployments (or test cases) can share one
	// JetStream. Default "ABLY". Must be a valid stream-name token.
	Prefix string

	// Shards is the number of log streams channels are hashed across.
	// Each stream is its own Raft group with its own leader, so shards
	// spread the write load across JetStream servers. Default 1. Changing
	// it re-maps channels to streams, so it is fixed for a deployment.
	Shards int

	// Replicas is the replication factor of every stream and bucket.
	// Default 1.
	Replicas int

	// MemoryStorage selects in-memory JetStream storage instead of file
	// storage.
	MemoryStorage bool

	// MaxAge is the message retention window (stream MaxAge). Zero means
	// no age limit, matching the other backends today (DESIGN.md §6).
	MaxAge time.Duration

	// MaxMsgsPerChannel caps each channel's log per kind (stream
	// MaxMsgsPerSubject). Zero means no cap.
	MaxMsgsPerChannel int64

	// DuplicateWindow is the idempotency window (stream Duplicates).
	// Default 2 minutes; capped at MaxAge when MaxAge is set.
	DuplicateWindow time.Duration

	// Now is the serial clock. Nil means time.Now().UnixMilli.
	Now func() int64

	// Logger receives operational events. Nil means logging.Default().
	Logger *logging.Logger
}

// Storage is the JetStream-backed storage.Storage.
type Storage struct {
	nc      *nats.Conn
	ownConn bool
	js      jetstream.JetStream
	streams []jetstream.Stream
	chans   jetstream.KeyValue // channel token -> immutable initial serial
	members jetstream.KeyValue // "<channel token>.<member token>" -> memberRecord
	prefix  string
	series  string
	now     func() int64
	logger  *logging.Logger

	// conflicts counts appends that lost a compare-and-set race to another
	// writer and were rebuilt on the new head (see WriteConflicts).
	conflicts atomic.Uint64

	mu       sync.Mutex
	channels map[string]*channelStore
	closed   bool
}

// WriteConflicts returns how many appends this Storage has had to rebuild
// because another node appended to the same channel first. It is the cost
// of cross-node ordering: zero on a single node, rising with contention on
// shared channels.
func (s *Storage) WriteConflicts() uint64 { return s.conflicts.Load() }

// Open connects to NATS, creates (or updates) the log streams and the KV
// buckets, and returns a Storage ready for use. The seriesId is freshly
// generated per process, as for the other backends (DESIGN.md §8).
func Open(ctx context.Context, opts Options) (*Storage, error) {
	if opts.Prefix == "" {
		opts.Prefix = defaultPrefix
	}
	if opts.Shards <= 0 {
		opts.Shards = 1
	}
	if opts.Replicas <= 0 {
		opts.Replicas = 1
	}
	if opts.DuplicateWindow <= 0 {
		opts.DuplicateWindow = defaultDuplicateWindow
	}
	if opts.MaxAge > 0 && opts.DuplicateWindow > opts.MaxAge {
		opts.DuplicateWindow = opts.MaxAge
	}
	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	nc, own := opts.Conn, false
	if nc == nil {
		if opts.URL == "" {
			return nil, errors.New("storage/natsjs: Open requires a URL")
		}
		var err error
		nc, err = nats.Connect(opts.URL,
			nats.Name("ably-server"),
			nats.MaxReconnects(-1),
		)
		if err != nil {
			return nil, fmt.Errorf("storage/natsjs: connect %q: %w", opts.URL, err)
		}
		own = true
	}
	fail := func(err error) (*Storage, error) {
		if own {
			nc.Close()
		}
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return fail(fmt.Errorf("storage/natsjs: jetstream: %w", err))
	}
	storageType := jetstream.FileStorage
	if opts.MemoryStorage {
		storageType = jetstream.MemoryStorage
	}

	s := &Storage{
		nc:       nc,
		ownConn:  own,
		js:       js,
		prefix:   opts.Prefix,
		series:   serial.NewSeriesID(),
		now:      opts.Now,
		logger:   logger,
		channels: make(map[string]*channelStore),
	}

	for k := range opts.Shards {
		stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:              s.streamName(k),
			Description:       "ably-server channel log",
			Subjects:          []string{s.shardSubject(k) + ".>"},
			Storage:           storageType,
			Replicas:          opts.Replicas,
			Retention:         jetstream.LimitsPolicy,
			Discard:           jetstream.DiscardOld,
			MaxAge:            opts.MaxAge,
			MaxMsgsPerSubject: opts.MaxMsgsPerChannel,
			Duplicates:        opts.DuplicateWindow,
			// AllowDirect stays off: nats.go serves last-by-subject direct
			// gets through the subject-form API, which cannot carry the
			// wildcard filter the head lookup needs.
		})
		if err != nil {
			return fail(fmt.Errorf("storage/natsjs: create stream %s: %w", s.streamName(k), err))
		}
		s.streams = append(s.streams, stream)
	}

	if s.chans, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      opts.Prefix + "_channels",
		Description: "ably-server channel initial serials",
		History:     1,
		Storage:     storageType,
		Replicas:    opts.Replicas,
	}); err != nil {
		return fail(fmt.Errorf("storage/natsjs: create channels bucket: %w", err))
	}
	if s.members, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      opts.Prefix + "_presence",
		Description: "ably-server presence membership sets",
		History:     1,
		Storage:     storageType,
		Replicas:    opts.Replicas,
	}); err != nil {
		return fail(fmt.Errorf("storage/natsjs: create presence bucket: %w", err))
	}
	return s, nil
}

func (s *Storage) streamName(k int) string   { return fmt.Sprintf("%s_LOG_%d", s.prefix, k) }
func (s *Storage) shardSubject(k int) string { return fmt.Sprintf("%s.log.%d", s.prefix, k) }

// shardOf maps a channel name to its log stream.
func (s *Storage) shardOf(name string) int {
	if len(s.streams) == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int(h.Sum32() % uint32(len(s.streams)))
}

// Channel returns the ChannelStore for name, binding it to appender on
// first access. Subsequent calls with the same name return the same
// instance and ignore the new appender. With an appender, the channel's
// initial serial is loaded (or created, first writer wins across nodes),
// appender.Initialize is called with the channel's current head, and an
// ordered consumer starts delivering every cm appended after that head.
func (s *Storage) Channel(ctx context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	if name == "" {
		return nil, errors.New("storage/natsjs: empty channel name")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("storage/natsjs: storage closed")
	}
	if cs, ok := s.channels[name]; ok {
		s.mu.Unlock()
		return cs, nil
	}
	k := s.shardOf(name)
	tok := base64.RawURLEncoding.EncodeToString([]byte(name))
	base := s.shardSubject(k) + "." + tok
	cs := &channelStore{
		st:       s,
		name:     name,
		tok:      tok,
		stream:   s.streams[k],
		base:     base,
		filter:   base + ".*",
		appender: appender,
	}
	s.channels[name] = cs
	s.mu.Unlock()

	if appender == nil {
		return cs, nil
	}
	if err := cs.bind(ctx); err != nil {
		s.mu.Lock()
		delete(s.channels, name)
		s.mu.Unlock()
		return nil, err
	}
	return cs, nil
}

// Close stops every delivery consumer and, if Storage dialled it, closes
// the NATS connection.
func (s *Storage) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stores := make([]*channelStore, 0, len(s.channels))
	for _, cs := range s.channels {
		stores = append(stores, cs)
	}
	s.mu.Unlock()

	for _, cs := range stores {
		cs.stopDelivery()
	}
	if s.ownConn {
		s.nc.Close()
	}
	return nil
}

// Ping reports whether JetStream is reachable. It satisfies
// storage.Pinger for the /readyz check (DESIGN.md §2.2).
func (s *Storage) Ping(ctx context.Context) error {
	_, err := s.js.AccountInfo(ctx)
	return err
}

// nextSerial mints the channelSerial that follows prev, the channel's
// last persisted serial (or its initial serial when the log is empty),
// with this process's seriesId. It seeds a serial.Generator with prev's
// timestamp and counter, so the result is strictly greater than prev
// whichever node minted prev: wall clock if it has advanced, otherwise
// prev's timestamp with the counter bumped (DESIGN.md §8).
func (s *Storage) nextSerial(prev string) string {
	g := serial.NewGenerator(s.series, s.now)
	if ts, err := serial.Timestamp(prev); err == nil {
		g.Restore(ts, serialCounter(prev))
	}
	return g.Mint()
}

// serialBefore returns a serial minted just before cs's timestamp, used to
// seed an initial serial for a channel that already has cms but lost its
// recorded initial.
func (s *Storage) serialBefore(cs string) string {
	ts, err := serial.Timestamp(cs)
	if err != nil || ts <= 1 {
		return serial.NewGenerator(s.series, func() int64 { return 1 }).Mint()
	}
	return serial.NewGenerator(s.series, func() int64 { return ts - 1 }).Mint()
}

// serialCounter parses the 3-digit counter of a channelSerial
// ("<14-digit ts>-<3-digit counter>@<series>", DESIGN.md §8).
func serialCounter(s string) int {
	if len(s) < 18 || s[14] != '-' {
		return 0
	}
	n, err := strconv.Atoi(s[15:18])
	if err != nil {
		return 0
	}
	return n
}

// logEntry is the payload of one log message. Annotation.Summary is
// excluded from the protocol type's msgpack encoding (it is server-internal
// delivery state), so the post-fold snapshots ride alongside, index-aligned
// with CM.Annotations, as the Postgres backend's summary column does.
type logEntry struct {
	CM        *protocol.ChannelMessage `msgpack:"cm"`
	Summaries []protocol.Summary       `msgpack:"sums,omitempty"`
}

func encodeEntry(cm *protocol.ChannelMessage) ([]byte, error) {
	e := logEntry{CM: cm}
	if len(cm.Annotations) > 0 {
		e.Summaries = make([]protocol.Summary, len(cm.Annotations))
		for i, a := range cm.Annotations {
			e.Summaries[i] = a.Summary
		}
	}
	return msgpack.Marshal(&e)
}

func decodeEntry(b []byte) (*protocol.ChannelMessage, error) {
	var e logEntry
	if err := msgpack.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("storage/natsjs: decode log entry: %w", err)
	}
	if e.CM == nil {
		return nil, errors.New("storage/natsjs: log entry has no cm")
	}
	for i, a := range e.CM.Annotations {
		if i < len(e.Summaries) {
			a.Summary = e.Summaries[i]
		}
	}
	return e.CM, nil
}

// isCASConflict reports whether err is JetStream rejecting an append
// because the channel's last sequence moved (another writer won the race).
// Replicated streams report the in-flight variant as a separate code.
func isCASConflict(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}

// channelStore is the per-channel facet.
type channelStore struct {
	st       *Storage
	name     string
	tok      string // base64url channel name, the subject and key token
	stream   jetstream.Stream
	base     string // "<prefix>.log.<shard>.<tok>"
	filter   string // base + ".*": every kind on this channel
	appender storage.Appender

	// wmu serialises this node's appends to the channel, so concurrent
	// local publishers queue rather than race each other's CAS. It guards
	// the head cache and the initial serial below.
	wmu     sync.Mutex
	headOK  bool
	headSeq uint64 // stream sequence of the channel's last cm (0 = none)
	headSer string // channelSerial of that cm
	initial string

	// hwmMu guards lastSeen, the highest channelSerial delivered to the
	// appender: the per-channel de-duplication point every delivery
	// passes through (DESIGN.md §7.2).
	hwmMu    sync.Mutex
	lastSeen string
	cc       jetstream.ConsumeContext
}

func (cs *channelStore) subject(kind storage.Kind) string {
	switch kind.Normalize() {
	case storage.KindPresence:
		return cs.base + "." + kindTokPresence
	case storage.KindAnnotation:
		return cs.base + "." + kindTokAnnotation
	}
	return cs.base + "." + kindTokMessage
}

// msgID namespaces an idempotency key by channel: JetStream's duplicate
// window is stream-wide, Ably's is per channel. The id is base64url
// encoded so any client string is a safe header value.
func (cs *channelStore) msgID(id string) string {
	return cs.tok + ":" + base64.RawURLEncoding.EncodeToString([]byte(id))
}

// readHead returns the stream sequence and channelSerial of the channel's
// last cm of any kind, or (0, "") when the channel has none.
func (cs *channelStore) readHead(ctx context.Context) (uint64, string, error) {
	raw, err := cs.stream.GetLastMsgForSubject(ctx, cs.filter)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("storage/natsjs: read head of %q: %w", cs.name, err)
	}
	return raw.Sequence, raw.Header.Get(hdrSerial), nil
}

// ensureInitialLocked loads the channel's immutable initial serial, or
// creates it (first writer wins across nodes) if the channel has none.
// Every append path calls it before minting its first serial, and the
// first cm is minted as the initial's successor, so the initial sorts
// strictly before every cm on the channel whichever node wrote it.
// Callers hold wmu.
func (cs *channelStore) ensureInitialLocked(ctx context.Context) error {
	if cs.initial != "" {
		return nil
	}
	for range 3 {
		entry, err := cs.st.chans.Get(ctx, cs.tok)
		if err == nil {
			cs.initial = string(entry.Value())
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("storage/natsjs: load initial serial of %q: %w", cs.name, err)
		}
		// No recorded initial. If the log already holds cms (the bucket
		// was lost), seed just before the first of them.
		seed := cs.st.nextSerial("")
		first, err := cs.stream.GetMsg(ctx, 1, jetstream.WithGetMsgSubject(cs.filter))
		switch {
		case err == nil:
			seed = cs.st.serialBefore(first.Header.Get(hdrSerial))
		case !errors.Is(err, jetstream.ErrMsgNotFound):
			return fmt.Errorf("storage/natsjs: read first cm of %q: %w", cs.name, err)
		}
		if _, err := cs.st.chans.Create(ctx, cs.tok, []byte(seed)); err == nil {
			cs.initial = seed
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("storage/natsjs: record initial serial of %q: %w", cs.name, err)
		}
		// Another node created it first; loop to read theirs.
	}
	return fmt.Errorf("storage/natsjs: could not settle the initial serial of %q", cs.name)
}

// bind hands the appender its watermark and starts delivery. The head is
// read before the consumer starts, and the consumer starts at the next
// stream sequence, so no cm appended after the watermark is missed.
func (cs *channelStore) bind(ctx context.Context) error {
	cs.wmu.Lock()
	err := cs.ensureInitialLocked(ctx)
	initial := cs.initial
	cs.wmu.Unlock()
	if err != nil {
		return err
	}
	seq, current, err := cs.readHead(ctx)
	if err != nil {
		return err
	}
	if current == "" {
		current = initial
	}
	cs.hwmMu.Lock()
	cs.lastSeen = current
	cs.hwmMu.Unlock()
	cs.appender.Initialize(current, initial)

	cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{cs.filter}}
	if seq > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = seq + 1
	}
	cons, err := cs.stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("storage/natsjs: delivery consumer for %q: %w", cs.name, err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		cm, err := decodeEntry(msg.Data())
		if err != nil {
			cs.st.logger.Warn("storage/natsjs: undecodable log entry", "channel", cs.name, "err", err)
			return
		}
		cs.deliver(cm)
	})
	if err != nil {
		return fmt.Errorf("storage/natsjs: start delivery for %q: %w", cs.name, err)
	}
	cs.hwmMu.Lock()
	cs.cc = cc
	cs.hwmMu.Unlock()
	return nil
}

func (cs *channelStore) stopDelivery() {
	cs.hwmMu.Lock()
	cc := cs.cc
	cs.cc = nil
	cs.hwmMu.Unlock()
	if cc != nil {
		cc.Stop()
	}
}

// deliver hands cm to the appender exactly once and in order: a cm whose
// serial is not strictly greater than the last delivered one is dropped
// (an ordered consumer re-sync can redeliver).
func (cs *channelStore) deliver(cm *protocol.ChannelMessage) {
	cs.hwmMu.Lock()
	if cm.ChannelSerial <= cs.lastSeen {
		cs.hwmMu.Unlock()
		return
	}
	cs.lastSeen = cm.ChannelSerial
	cs.hwmMu.Unlock()
	cs.appender.Append(cm)
}

// commit appends one cm to the channel's log as a compare-and-set against
// the channel's last sequence. build is called with the freshly minted
// channelSerial and returns the cm to persist; it runs again on every
// retry, so reads it makes (the merge base of a mutation, the summary
// before a fold) are re-taken against the new head. dedupID, when set,
// is the idempotency key: a repeat within the duplicate window returns
// the original cm with idempotent=true.
func (cs *channelStore) commit(
	ctx context.Context,
	kind storage.Kind,
	dedupID string,
	build func(ctx context.Context, channelSerial string) (*protocol.ChannelMessage, error),
) (*protocol.ChannelMessage, bool, error) {
	cs.wmu.Lock()
	defer cs.wmu.Unlock()

	if err := cs.ensureInitialLocked(ctx); err != nil {
		return nil, false, err
	}
	for attempt := range maxCASAttempts {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if !cs.headOK {
			seq, ser, err := cs.readHead(ctx)
			if err != nil {
				return nil, false, err
			}
			cs.headSeq, cs.headSer, cs.headOK = seq, ser, true
		}
		prev := cs.headSer
		if prev == "" {
			prev = cs.initial
		}
		channelSerial := cs.st.nextSerial(prev)
		cm, err := build(ctx, channelSerial)
		if err != nil {
			return nil, false, err
		}
		payload, err := encodeEntry(cm)
		if err != nil {
			return nil, false, err
		}
		msg := nats.NewMsg(cs.subject(kind))
		msg.Data = payload
		msg.Header.Set(hdrSerial, channelSerial)
		opts := []jetstream.PublishOpt{jetstream.WithExpectLastSequenceForSubject(cs.headSeq, cs.filter)}
		if dedupID != "" {
			opts = append(opts, jetstream.WithMsgID(cs.msgID(dedupID)))
		}
		ack, err := cs.st.js.PublishMsg(ctx, msg, opts...)
		if isCASConflict(err) {
			// Another node appended first: re-read the head and rebuild.
			cs.st.conflicts.Add(1)
			cs.headOK = false
			if attempt > 2 {
				time.Sleep(time.Duration(rand.IntN(attempt*500)) * time.Microsecond)
			}
			continue
		}
		if err != nil {
			cs.headOK = false
			return nil, false, fmt.Errorf("storage/natsjs: append to %q: %w", cs.name, err)
		}
		if ack.Duplicate {
			original, err := cs.loadSeq(ctx, ack.Sequence)
			if err != nil {
				return nil, false, err
			}
			return original, true, nil
		}
		cs.headSeq, cs.headSer = ack.Sequence, channelSerial
		return cm, false, nil
	}
	return nil, false, fmt.Errorf("storage/natsjs: append to %q: gave up after %d write conflicts", cs.name, maxCASAttempts)
}

// loadSeq fetches and decodes the cm at a stream sequence.
func (cs *channelStore) loadSeq(ctx context.Context, seq uint64) (*protocol.ChannelMessage, error) {
	raw, err := cs.stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, fmt.Errorf("storage/natsjs: load cm at seq %d of %q: %w", seq, cs.name, err)
	}
	return decodeEntry(raw.Data)
}

// scan walks, in stream order, every cm on one subject (or subject
// filter) of this channel, up to the channel's head as observed when the
// scan starts, so a scan reads a consistent prefix of the log. fn returns
// false to stop early.
//
// Each step is a message get with next_by_subj, which the server answers
// from its per-subject index. That keeps the spike simple and exact; a
// production backend would batch (ordered consumer or 2.11 batched direct
// get) and seek by time or sequence rather than start at the stream head.
func (cs *channelStore) scan(ctx context.Context, subject string, fn func(cm *protocol.ChannelMessage) bool) error {
	last, err := cs.stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage/natsjs: scan %q: %w", cs.name, err)
	}
	end := last.Sequence
	for seq := uint64(1); seq <= end; {
		raw, err := cs.stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("storage/natsjs: scan %q: %w", cs.name, err)
		}
		if raw.Sequence > end {
			return nil
		}
		cm, err := decodeEntry(raw.Data)
		if err != nil {
			return err
		}
		if !fn(cm) {
			return nil
		}
		seq = raw.Sequence + 1
	}
	return nil
}

// loadKind returns every cm of one kind on the channel, in stream (and
// therefore serial) order.
func (cs *channelStore) loadKind(ctx context.Context, kind storage.Kind) ([]*protocol.ChannelMessage, error) {
	var out []*protocol.ChannelMessage
	err := cs.scan(ctx, cs.subject(kind), func(cm *protocol.ChannelMessage) bool {
		out = append(out, cm)
		return true
	})
	return out, err
}

// versionsOf returns every version of the message identity in version
// order: the create (one message of its cm) and each mutation.
func (cs *channelStore) versionsOf(ctx context.Context, identity string) ([]*protocol.Message, error) {
	var out []*protocol.Message
	err := cs.scan(ctx, cs.subject(storage.KindMessage), func(cm *protocol.ChannelMessage) bool {
		for _, m := range cm.Messages {
			if m.Serial == identity {
				out = append(out, m)
			}
		}
		return true
	})
	return out, err
}

// annotationsOf returns the annotations targeting the message identity, in
// stream order, each carrying its post-fold summary snapshot.
func (cs *channelStore) annotationsOf(ctx context.Context, identity string) ([]*protocol.Annotation, error) {
	var out []*protocol.Annotation
	err := cs.scan(ctx, cs.subject(storage.KindAnnotation), func(cm *protocol.ChannelMessage) bool {
		for _, a := range cm.Annotations {
			if a.MessageSerial == identity {
				out = append(out, a)
			}
		}
		return true
	})
	return out, err
}

// latest derives the latest-version projection entry for identity
// (DESIGN.md §13.4) from the log: the last version, with the summary taken
// from the newest annotation snapshot when any annotation exists (a
// mutation carries forward the summary it merged against, so the newest
// snapshot is always current).
func (cs *channelStore) latest(ctx context.Context, identity string) (*protocol.Message, error) {
	versions, err := cs.versionsOf(ctx, identity)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, storage.ErrTargetNotFound
	}
	m := *versions[len(versions)-1]
	anns, err := cs.annotationsOf(ctx, identity)
	if err != nil {
		return nil, err
	}
	if n := len(anns); n > 0 {
		m.Summary = anns[n-1].Summary
	}
	return &m, nil
}

// Store persists a create publish (DESIGN.md §6, §8).
func (cs *channelStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/natsjs: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	clientIDs := false
	for _, m := range msgs {
		if m.ID != "" {
			clientIDs = true
			break
		}
	}
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}
	// Only a client-supplied id is an idempotency key. A server-generated
	// batch id is fresh every time, so it needs no duplicate tracking.
	var dedupID string
	if clientIDs {
		dedupID = msgs[0].ID
	}
	return cs.commit(ctx, storage.KindMessage, dedupID, func(_ context.Context, channelSerial string) (*protocol.ChannelMessage, error) {
		for i, m := range msgs {
			m.Serial = serial.MessageSerial(channelSerial, i)
			m.Action = protocol.MessageCreate
			storage.StampCreateVersion(m)
		}
		return &protocol.ChannelMessage{ID: batchID, ChannelSerial: channelSerial, Messages: msgs}, nil
	})
}

// Mutate appends an update/delete/append version (DESIGN.md §13.2): the
// merge base is the target's latest version as derived from the log at the
// head the append is conditional on, so a concurrent mutation on another
// node forces a re-merge rather than a lost update.
func (cs *channelStore) Mutate(ctx context.Context, mut *protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.Action.IsMutation() {
		return nil, false, errors.New("storage/natsjs: Mutate requires a mutation action")
	}
	if mut.Serial == "" {
		return nil, false, errors.New("storage/natsjs: Mutate requires a target serial")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	return cs.commit(ctx, storage.KindMessage, mut.ID, func(ctx context.Context, channelSerial string) (*protocol.ChannelMessage, error) {
		current, err := cs.latest(ctx, mut.Serial)
		if err != nil {
			return nil, err
		}
		version, err := storage.MergeVersion(current, mut, serial.MessageSerial(channelSerial, 0))
		if err != nil {
			return nil, err
		}
		return &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: []*protocol.Message{version}}, nil
	})
}

// LatestVersion returns the derived projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cs.latest(ctx, serial)
}

// Versions returns every version of serial ordered by version, append runs
// collapsed to their aggregate (DESIGN.md §13.3, §13.4).
func (cs *channelStore) Versions(ctx context.Context, serial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	all, err := cs.versionsOf(ctx, serial)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	if len(all) == 0 {
		return storage.HistoryPage{}, storage.ErrTargetNotFound
	}
	return storage.PaginateVersions(storage.CollapseAppendVersions(all), q), nil
}

// StoreAnnotation appends an annotation cm (DESIGN.md §14.1). Every target
// must resolve; each annotation is folded into its target's summary in
// batch order and stamped with the post-fold snapshot, which the log entry
// persists so every node delivers the identical summary (§14.2).
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*protocol.Annotation) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/natsjs: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var dedupID string
	for _, a := range annotations {
		if a.ID != "" {
			dedupID = a.ID
			break
		}
	}
	return cs.commit(ctx, storage.KindAnnotation, dedupID, func(ctx context.Context, channelSerial string) (*protocol.ChannelMessage, error) {
		sums := make(map[string]protocol.Summary)
		for _, a := range annotations {
			if _, ok := sums[a.MessageSerial]; ok {
				continue
			}
			target, err := cs.latest(ctx, a.MessageSerial)
			if err != nil {
				return nil, err
			}
			sums[a.MessageSerial] = target.Summary
		}
		for i, a := range annotations {
			a.Serial = serial.MessageSerial(channelSerial, i)
			folded := sums[a.MessageSerial].Apply(a)
			sums[a.MessageSerial] = folded
			a.Summary = folded.Clone()
		}
		return &protocol.ChannelMessage{ChannelSerial: channelSerial, Annotations: annotations}, nil
	})
}

// Annotations returns the annotations attached to messageSerial in stream
// order (DESIGN.md §14.4). An unknown target yields an empty page.
func (cs *channelStore) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	all, err := cs.annotationsOf(ctx, messageSerial)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	return storage.PaginateAnnotations(all, q), nil
}

// memberRecord is one membership-set entry in the presence bucket. A LEAVE
// is kept as a tombstone rather than a KV delete, so a late-arriving fold of
// an older ENTER (from a slower node) cannot resurrect the member: every
// fold is last-writer-wins by presence serial.
type memberRecord struct {
	Serial string                    `msgpack:"s"`
	Left   bool                      `msgpack:"l,omitempty"`
	Member *protocol.PresenceMessage `msgpack:"m,omitempty"`
}

func (cs *channelStore) memberKey(p *protocol.PresenceMessage) string {
	return cs.tok + "." + base64.RawURLEncoding.EncodeToString([]byte(storage.MemberKey(p.ConnectionID, p.ClientID)))
}

// foldMember applies one committed presence op to the membership set.
func (cs *channelStore) foldMember(ctx context.Context, p *protocol.PresenceMessage) error {
	left := p.Action == protocol.PresenceLeave || p.Action == protocol.PresenceAbsent
	rec := memberRecord{Serial: p.Serial, Left: left}
	if !left {
		rec.Member = p
	}
	val, err := msgpack.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("storage/natsjs: encode member: %w", err)
	}
	key := cs.memberKey(p)
	for range maxCASAttempts {
		entry, err := cs.st.members.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err := cs.st.members.Create(ctx, key, val); err == nil {
				return nil
			} else if !errors.Is(err, jetstream.ErrKeyExists) {
				return fmt.Errorf("storage/natsjs: fold member: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("storage/natsjs: read member: %w", err)
		}
		var cur memberRecord
		if err := msgpack.Unmarshal(entry.Value(), &cur); err == nil && cur.Serial >= rec.Serial {
			return nil // a newer op for this member is already folded
		}
		if _, err := cs.st.members.Update(ctx, key, val, entry.Revision()); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return fmt.Errorf("storage/natsjs: fold member: %w", err)
		}
	}
	return fmt.Errorf("storage/natsjs: fold member on %q: too much contention", cs.name)
}

// StorePresence appends a presence cm and then folds it into the
// membership set (DESIGN.md §12.2, §12.5). The append and the fold are two
// JetStream writes, not one transaction: a crash between them leaves the
// log ahead of the set until the member's next op (see the spike report).
func (cs *channelStore) StorePresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/natsjs: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var dedupID string
	for _, p := range presence {
		if p.ID != "" {
			dedupID = p.ID
			break
		}
	}
	cm, idempotent, err := cs.commit(ctx, storage.KindPresence, dedupID, func(_ context.Context, channelSerial string) (*protocol.ChannelMessage, error) {
		for i, p := range presence {
			storage.StampPresenceMember(p, channelSerial, i)
		}
		return &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: presence}, nil
	})
	if err != nil || idempotent {
		return cm, idempotent, err
	}
	for _, p := range cm.Presence {
		if err := cs.foldMember(ctx, p); err != nil {
			return nil, false, err
		}
	}
	return cm, false, nil
}

// Members returns the membership set (sorted by serial) and the channel's
// current head serial (DESIGN.md §12.4).
func (cs *channelStore) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	_, asOf, err := cs.readHead(ctx)
	if err != nil {
		return nil, "", err
	}
	w, err := cs.st.members.Watch(ctx, cs.tok+".*", jetstream.IgnoreDeletes())
	if err != nil {
		return nil, "", fmt.Errorf("storage/natsjs: watch members of %q: %w", cs.name, err)
	}
	defer func() { _ = w.Stop() }()

	var out []*protocol.PresenceMessage
	for {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return nil, "", fmt.Errorf("storage/natsjs: members watch of %q closed early", cs.name)
			}
			if entry == nil { // initial values delivered
				sort.Slice(out, func(i, j int) bool { return out[i].Serial < out[j].Serial })
				return out, asOf, nil
			}
			var rec memberRecord
			if err := msgpack.Unmarshal(entry.Value(), &rec); err != nil {
				return nil, "", fmt.Errorf("storage/natsjs: decode member: %w", err)
			}
			if !rec.Left && rec.Member != nil {
				out = append(out, rec.Member)
			}
		}
	}
}

// History returns cms in publish order (DESIGN.md §6). A raw read loads the
// kind's subject and paginates at item granularity exactly as the bbolt
// backend does; the collapsed message view is derived from the versions.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}
	kind := q.Kind.Normalize()
	if q.Collapse && kind == storage.KindMessage {
		return cs.collapsedHistory(ctx, q)
	}
	cms, err := cs.loadKind(ctx, kind)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	return paginate(cms, kind, q), nil
}

// paginate slices a serial-ordered list of cms of one kind into a page per
// the query's bounds, direction, cursor and limit, at item granularity (a
// cursor or limit may split a batch). The persisted cms are not mutated.
func paginate(cms []*protocol.ChannelMessage, kind storage.Kind, q storage.HistoryQuery) storage.HistoryPage {
	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	inRange := func(cs string) bool {
		if timeLower != "" && cs < timeLower {
			return false
		}
		if timeUpper != "" && cs >= timeUpper {
			return false
		}
		if q.AfterChannelSerial != "" && cs <= q.AfterChannelSerial {
			return false
		}
		if q.EndChannelSerial != "" && cs > q.EndChannelSerial {
			return false
		}
		return true
	}
	forwards := q.Direction == storage.DirectionForwards

	var page storage.HistoryPage
	count := 0
	emit := func(channelSerial string, put func(dst *protocol.ChannelMessage)) bool {
		if q.Limit > 0 && count >= q.Limit {
			page.HasMore = true
			return false
		}
		var current *protocol.ChannelMessage
		if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
			current = page.ChannelMessages[n-1]
		} else {
			current = &protocol.ChannelMessage{ChannelSerial: channelSerial}
			page.ChannelMessages = append(page.ChannelMessages, current)
		}
		put(current)
		count++
		return true
	}

	if forwards {
		for _, cm := range cms {
			if !inRange(cm.ChannelSerial) {
				continue
			}
			for _, it := range storage.CMItems(cm, kind) {
				if q.Cursor != "" && it.Serial <= q.Cursor {
					continue
				}
				if !emit(cm.ChannelSerial, it.Append) {
					return page
				}
			}
		}
		return page
	}
	for i := len(cms) - 1; i >= 0; i-- {
		cm := cms[i]
		if !inRange(cm.ChannelSerial) {
			continue
		}
		items := storage.CMItems(cm, kind)
		for idx := len(items) - 1; idx >= 0; idx-- {
			if q.Cursor != "" && items[idx].Serial >= q.Cursor {
				continue
			}
			if !emit(cm.ChannelSerial, items[idx].Append) {
				return page
			}
		}
	}
	return page
}

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), derived from one scan of the
// message subject and one of the annotation subject. Bounds, cursor and
// limit apply to the identity as in the bbolt backend; EndChannelSerial
// bounds each entry by its CREATE channelSerial.
func (cs *channelStore) collapsedHistory(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	latest := make(map[string]*protocol.Message)
	if err := cs.scan(ctx, cs.subject(storage.KindMessage), func(cm *protocol.ChannelMessage) bool {
		for _, m := range cm.Messages {
			latest[m.Serial] = m
		}
		return true
	}); err != nil {
		return storage.HistoryPage{}, err
	}
	if len(latest) == 0 {
		return storage.HistoryPage{}, nil
	}
	summaries := make(map[string]protocol.Summary)
	if err := cs.scan(ctx, cs.subject(storage.KindAnnotation), func(cm *protocol.ChannelMessage) bool {
		for _, a := range cm.Annotations {
			summaries[a.MessageSerial] = a.Summary
		}
		return true
	}); err != nil {
		return storage.HistoryPage{}, err
	}

	identities := make([]string, 0, len(latest))
	for id := range latest {
		identities = append(identities, id)
	}
	sort.Strings(identities)

	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	inBounds := func(identity string) bool {
		if timeLower != "" && identity < timeLower {
			return false
		}
		if timeUpper != "" && identity >= timeUpper {
			return false
		}
		if q.EndChannelSerial != "" && storage.CreateChannelSerial(identity) > q.EndChannelSerial {
			return false
		}
		return true
	}
	forwards := q.Direction == storage.DirectionForwards

	var page storage.HistoryPage
	count := 0
	emit := func(identity string) bool {
		if q.Limit > 0 && count >= q.Limit {
			page.HasMore = true
			return false
		}
		m := *latest[identity]
		if sum, ok := summaries[identity]; ok {
			m.Summary = sum
		}
		ccs := storage.CreateChannelSerial(identity)
		if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == ccs {
			page.ChannelMessages[n-1].Messages = append(page.ChannelMessages[n-1].Messages, &m)
		} else {
			page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
				ChannelSerial: ccs,
				Messages:      []*protocol.Message{&m},
			})
		}
		count++
		return true
	}

	if forwards {
		for _, id := range identities {
			if !inBounds(id) || (q.Cursor != "" && id <= q.Cursor) {
				continue
			}
			if !emit(id) {
				break
			}
		}
		return page, nil
	}
	for i := len(identities) - 1; i >= 0; i-- {
		id := identities[i]
		if !inBounds(id) || (q.Cursor != "" && id >= q.Cursor) {
			continue
		}
		if !emit(id) {
			break
		}
	}
	return page, nil
}
