package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Bus kinds accepted by Options.Bus (DESIGN.md §7.2).
const (
	// BusPGNotify is the shipped bus and the default: one global
	// LISTEN channel, a NOTIFY inside every publish transaction, and one
	// read-back per notification on one goroutine per node.
	BusPGNotify = "pgnotify"
	// BusPostgres is the rebuilt Postgres bus: per-channel LISTEN,
	// inline payloads, one ordered worker per channel, and (by default)
	// coalesced wake-ups sent outside the publish transaction.
	BusPostgres = "postgres"
	// BusNATS is NATS core pub/sub on a per-channel subject. Postgres
	// stays the store; only cross-node delivery moves.
	BusNATS = "nats"
)

// ParseBus validates a bus name. The empty string is the default,
// BusPGNotify.
func ParseBus(s string) (string, error) {
	switch s {
	case "", BusPGNotify:
		return BusPGNotify, nil
	case BusPostgres, BusNATS:
		return s, nil
	}
	return "", fmt.Errorf("unknown bus %q (valid: %s, %s, %s)", s, BusPGNotify, BusPostgres, BusNATS)
}

// Bus is the cross-node delivery mechanism of cluster mode: how a cm
// committed on one node reaches the channelStore bound to that channel
// on every node (DESIGN.md §7.2). Postgres is the store under every Bus;
// a Bus decides only how the existence of a committed cm (and, for the
// postgres and nats buses, its body) travels between nodes.
//
// Three implementations live in this package: pgNotifyBus (the shipped
// LISTEN/NOTIFY broker), pgBus (per-channel LISTEN, transactional or
// coalesced) and natsBus. The methods are unexported because a Bus
// reaches into the per-channel delivery point (channelStore) and the
// publish transaction; the abstraction is a seam inside the backend,
// not a public plug-in API.
type Bus interface {
	// start launches the bus's background goroutines. They are tracked
	// by the Storage WaitGroup and stop when ctx is cancelled (Close).
	start(ctx context.Context)

	// chains reports whether deliveries carry their predecessor serial.
	// When true the publish transaction captures the channel's previous
	// serial (channelStore.advanceSerial) and deliveries are ordered on
	// it (channelStore.deliverChained, chain.go); when false the delivery
	// point relies on the bus delivering each channel's cms in commit
	// order.
	chains() bool

	// bind is called when Storage.Channel first binds a channel with a
	// non-nil appender, before the channel's watermark is read, so a bus
	// that routes per channel can subscribe without a gap. It returns
	// once the subscription is in effect.
	bind(ctx context.Context, cs *channelStore) error

	// unbind reverses bind: when the bind cannot complete, and when
	// Storage.Release drops the channel (UNLISTEN, NATS unsubscribe).
	unbind(cs *channelStore)

	// beforeCommit runs inside the publish transaction once the cm's
	// rows are written, just before COMMIT.
	beforeCommit(ctx context.Context, tx pgx.Tx, cs *channelStore, w *busWrite) error

	// afterCommit runs once the publish transaction has committed. prev
	// is the channel's serial before this publish ("" when the bus does
	// not chain, or the channel had no row).
	afterCommit(cs *channelStore, cm *protocol.ChannelMessage, prev string)

	// ready reports whether the bus can currently deliver: nil, or why
	// not. Storage.Ping (the cluster /readyz check) includes it.
	ready() error

	// close releases the bus's connections. Called by Storage.Close
	// after the background goroutines have stopped.
	close()
}

// busWrite describes a publish about to commit, for Bus.beforeCommit:
// its serial, the serial it follows (prev, set only for a chaining bus)
// and its rows exactly as stored: one msgpack payload per
// channel_messages row in idx order, plus that row's annotation summary
// column (sums is nil unless the cm is annotations).
type busWrite struct {
	serial, prev string
	kind         storage.Kind
	rows, sums   [][]byte

	// Set by beforeCommit: a NOTIFY went into the transaction, and it was
	// the pointer form. commitWrite counts them once the commit succeeds.
	notified, pointer bool
}

// busStats are this node's bus counters; see storage.BusStats for what
// each means.
type busStats struct {
	published, publishErrors, pointers                 atomic.Uint64
	received, unrouted, malformed                      atomic.Uint64
	inline, fetched, fastPath, filled                  atomic.Uint64
	duplicates, held, gapFills, fetchErrors            atomic.Uint64
	reconcileRuns, reconciles, reconcileNanos          atomic.Uint64
	sweeps, sweepCatchUps, sweepNanos                  atomic.Uint64
	drops, listens, unlistens                          atomic.Uint64
	wakeupsSent, wakeupsReceived, flushes, flushErrors atomic.Uint64
	overflow, flushNanos                               atomic.Uint64
}

// discardStats absorbs the counts of a channelStore built without a
// Storage (the chain unit tests).
var discardStats busStats

// st returns the node's bus counters.
func (cs *channelStore) st() *busStats {
	if cs.stats != nil {
		return cs.stats
	}
	return &discardStats
}

// BusStats returns a snapshot of this node's bus counters (DESIGN.md
// §7.2). It implements storage.BusStatser for the ably_bus_* metrics.
func (s *Storage) BusStats() storage.BusStats {
	c := &s.stats
	out := storage.BusStats{
		Bus:              s.busKind,
		Mode:             s.notifyMode,
		Connected:        s.busConnected(),
		BoundChannels:    s.boundCount(),
		Published:        c.published.Load(),
		PublishErrors:    c.publishErrors.Load(),
		Pointers:         c.pointers.Load(),
		Received:         c.received.Load(),
		Unrouted:         c.unrouted.Load(),
		Malformed:        c.malformed.Load(),
		Inline:           c.inline.Load(),
		Fetched:          c.fetched.Load(),
		FastPath:         c.fastPath.Load(),
		Filled:           c.filled.Load(),
		Duplicates:       c.duplicates.Load(),
		Held:             c.held.Load(),
		GapFills:         c.gapFills.Load(),
		FetchErrors:      c.fetchErrors.Load(),
		ReconcileRuns:    c.reconcileRuns.Load(),
		Reconciles:       c.reconciles.Load(),
		ReconcileSeconds: time.Duration(c.reconcileNanos.Load()).Seconds(),
		Sweeps:           c.sweeps.Load(),
		SweepCatchUps:    c.sweepCatchUps.Load(),
		SweepSeconds:     time.Duration(c.sweepNanos.Load()).Seconds(),
		Drops:            c.drops.Load(),
		Listens:          c.listens.Load(),
		Unlistens:        c.unlistens.Load(),
		WakeupsSent:      c.wakeupsSent.Load(),
		WakeupsReceived:  c.wakeupsReceived.Load(),
		Flushes:          c.flushes.Load(),
		FlushErrors:      c.flushErrors.Load(),
		Overflow:         c.overflow.Load(),
		FlushSeconds:     time.Duration(c.flushNanos.Load()).Seconds(),
	}
	return out
}

// boundCount counts the channels bound with a live appender, without
// allocating.
func (s *Storage) boundCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, cs := range s.channels {
		if cs.appender != nil {
			n++
		}
	}
	return n
}

// busConnected reports whether the bus's connection is up: NATS, or the
// LISTEN connection of the Postgres buses.
func (s *Storage) busConnected() bool {
	if c, ok := s.bus.(interface{ isConnected() bool }); ok {
		return c.isConnected()
	}
	return true
}

// pgNotifyBus is the shipped Bus: every publish emits a NOTIFY on the
// single "ably_channel" LISTEN channel inside its transaction, and one
// LISTEN goroutine per node dispatches every notification, reading the
// cm back from the log (DESIGN.md §7.2). Its mechanics (listenLoop,
// consume, redial, reconcile, deliver) live in pgnotify.go, unchanged
// from before the bus seam; this type is the adapter onto the seam.
type pgNotifyBus struct {
	s *Storage

	initialConn *pgx.Conn   // first LISTEN conn, dialed by Open; owned by listenLoop thereafter
	connected   atomic.Bool // the LISTEN connection is up
}

func (b *pgNotifyBus) start(ctx context.Context) {
	b.s.wg.Add(1)
	go b.listenLoop(ctx)
}

func (b *pgNotifyBus) chains() bool { return false }

func (b *pgNotifyBus) bind(context.Context, *channelStore) error { return nil }

func (b *pgNotifyBus) unbind(*channelStore) {}

// beforeCommit emits the NOTIFY inside the publish transaction: PG
// buffers the payload until commit, so listeners only see it if the
// publish lands. The LISTEN goroutine on every node (including this
// one) routes the cm to the channel's appender (DESIGN.md §7.2).
func (b *pgNotifyBus) beforeCommit(ctx context.Context, tx pgx.Tx, cs *channelStore, w *busWrite) error {
	body, err := json.Marshal(notifyPayload{Channel: cs.name, Serial: w.serial})
	if err != nil {
		return fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}
	w.notified = true
	return nil
}

// afterCommit is a no-op: delivery, including to the publisher's own
// subscribers, arrives via the NOTIFY round-trip (DESIGN.md §7.2).
func (b *pgNotifyBus) afterCommit(*channelStore, *protocol.ChannelMessage, string) {}

// ready is always nil: as before the bus seam, readiness is the pool
// ping alone (a dropped LISTEN connection redials in the background).
func (b *pgNotifyBus) ready() error { return nil }

func (b *pgNotifyBus) close() {}

// batchItem is one cm of a batched publish transaction (DESIGN.md §6.3),
// in the batch's order: the channel it was written to, the cm as it will
// be delivered, and its busWrite. w.prev is the channel's serial before
// this cm, counting earlier cms of the same channel in the batch, so a
// chaining bus sees the same predecessor chain as for single publishes.
type batchItem struct {
	cs *channelStore
	cm *protocol.ChannelMessage
	w  *busWrite
}

// batchBeforeCommitter is implemented by a Bus whose in-transaction hook
// can be queued into a batched publish's pipelined round trip: it queues
// on b, in item order, what beforeCommit would run for each item (and
// sets each w's notified and pointer flags). A Bus that does not
// implement it gets its beforeCommit run once per item on the open
// transaction instead (beforeCommitBatch), at the cost of a round trip
// each; every bus in this package implements it.
type batchBeforeCommitter interface {
	beforeCommitBatch(ctx context.Context, b *pgx.Batch, items []batchItem) error
}

// beforeCommitBatch runs bus's in-transaction hook for a batch: queued
// on b when the bus supports it (queued is true), otherwise per item on
// tx, which the caller has brought up to date with the batch's inserts.
func beforeCommitBatch(ctx context.Context, bus Bus, b *pgx.Batch, tx pgx.Tx, items []batchItem) (queued bool, err error) {
	if bb, ok := bus.(batchBeforeCommitter); ok {
		return true, bb.beforeCommitBatch(ctx, b, items)
	}
	if tx == nil {
		return false, nil
	}
	for _, it := range items {
		if err := bus.beforeCommit(ctx, tx, it.cs, it.w); err != nil {
			return false, err
		}
	}
	return false, nil
}

// beforeCommitBatch queues one broker NOTIFY per cm, in batch order, as
// a single statement: the batched form of pgNotifyBus.beforeCommit.
func (b *pgNotifyBus) beforeCommitBatch(_ context.Context, batch *pgx.Batch, items []batchItem) error {
	if len(items) == 0 {
		return nil
	}
	payloads := make([]string, len(items))
	for i, it := range items {
		body, err := json.Marshal(notifyPayload{Channel: it.cs.name, Serial: it.w.serial})
		if err != nil {
			return fmt.Errorf("storage/postgres: encode notify: %w", err)
		}
		payloads[i] = string(body)
		it.w.notified = true
	}
	batch.Queue(`SELECT pg_notify($1, p) FROM unnest($2::text[]) WITH ORDINALITY AS t(p, n) ORDER BY n`,
		notifyChannelName, payloads)
	return nil
}

// beforeCommitBatch is the batched form of natsBus.beforeCommit: nothing
// goes into the transaction; the publish happens in afterCommit.
func (b *natsBus) beforeCommitBatch(context.Context, *pgx.Batch, []batchItem) error { return nil }

// beforeCommitBatch is the batched form of pgBus.beforeCommit: in
// transactional mode one per-channel NOTIFY per cm (inline rows when
// they fit), queued as a single statement in batch order; in coalesced
// mode nothing.
func (b *pgBus) beforeCommitBatch(_ context.Context, batch *pgx.Batch, items []batchItem) error {
	if b.mode == NotifyCoalesced || len(items) == 0 {
		return nil
	}
	chans := make([]string, len(items))
	payloads := make([]string, len(items))
	for i, it := range items {
		payload, inline, err := encodeNotification(it.cs.name, it.w.serial, it.w.prev, it.w.kind, it.w.rows, it.w.sums)
		if err != nil {
			return err
		}
		chans[i], payloads[i] = it.cs.pgChan, payload
		it.w.notified, it.w.pointer = true, !inline
	}
	batch.Queue(`SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS t(c, p, n) ORDER BY n`,
		chans, payloads)
	return nil
}

// connTx adapts a *pgx.Conn on which a transaction was opened with an
// explicit BEGIN to pgx.Tx, so a Bus without a batched hook can run its
// beforeCommit inside a batched publish transaction. Commit and Rollback
// are the caller's; Begin (a savepoint) is not supported.
type connTx struct{ conn *pgx.Conn }

var _ pgx.Tx = connTx{}

func (t connTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("storage/postgres: nested transaction in a batched publish")
}
func (t connTx) Commit(ctx context.Context) error {
	_, err := t.conn.Exec(ctx, "COMMIT")
	return err
}
func (t connTx) Rollback(ctx context.Context) error {
	_, err := t.conn.Exec(ctx, "ROLLBACK")
	return err
}
func (t connTx) CopyFrom(ctx context.Context, name pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	return t.conn.CopyFrom(ctx, name, cols, src)
}
func (t connTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.conn.SendBatch(ctx, b)
}
func (t connTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} } // unused by the buses
func (t connTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return t.conn.Prepare(ctx, name, sql)
}
func (t connTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.conn.Exec(ctx, sql, args...)
}
func (t connTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.conn.Query(ctx, sql, args...)
}
func (t connTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.conn.QueryRow(ctx, sql, args...)
}
func (t connTx) Conn() *pgx.Conn { return t.conn }
