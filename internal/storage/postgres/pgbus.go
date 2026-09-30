package postgres

// The postgres bus (DESIGN.md §7.2): cluster delivery over Postgres
// LISTEN/NOTIFY, rebuilt so it no longer has the shipped bus's
// ceilings. Each Ably channel has its own Postgres notification channel,
// and a node LISTENs on it only once it binds that channel, so a node
// receives notifications only for channels it holds. The LISTEN
// goroutine only receives and dispatches: each notification goes to its
// channel's bounded, ordered queue, and a worker per busy channel does
// the parsing, reads and appends, so a slow read or a slow appender on
// one channel never stalls another. Delivery is chained on the
// predecessor serial (chain.go), shared with the nats bus.
//
// Two notify modes (NotifyMode):
//   - transactional: one NOTIFY per write, inside the write's
//     transaction, carrying the cm inline when it fits.
//   - coalesced (the default): writes commit without NOTIFY; a per-node
//     notifier sends at most one wake-up per channel per window, outside
//     any transaction (pgbus_coalesced.go), and a receiver answers a
//     wake-up with one range read. Postgres serialises NOTIFYing commits
//     on one cluster-wide lock held through the WAL flush, so taking
//     NOTIFY out of the write transaction lets writes group-commit.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

const (
	// pgChannelPrefix starts every Postgres notification channel the
	// postgres bus uses. The rest of the name is a hash (pgChannelName),
	// so the whole name is a lower-case identifier well inside Postgres's
	// 63-byte identifier limit, whatever the Ably channel name is.
	pgChannelPrefix = "ably_c_"

	// inlinePayloadLimit is the size below which the bus carries a cm
	// inline in its NOTIFY. pg_notify rejects payloads of 8000 bytes or
	// more (with the default 8 kB block size), so the bus keeps a margin
	// and sends a pointer (channel, serial) for anything bigger.
	inlinePayloadLimit = 7900

	// listenBatchSize caps the LISTEN/UNLISTEN statements sent in one
	// round trip (bind bursts, and the re-LISTEN after a reconnect).
	listenBatchSize = 500
)

// channelQueueDepth bounds each bound channel's delivery queue. A
// notification that finds the queue full is dropped and the channel is
// marked for one catch-up read from the log, which delivers everything
// the dropped notifications announced (the receive-side overflow
// policy, DESIGN.md §7.2). A package var so tests can shrink it.
var channelQueueDepth = 1024

// errStorageClosed is returned by Channel when the storage is closed
// while the bind waits for its LISTEN.
var errStorageClosed = errors.New("storage/postgres: storage closed")

// pgChannelName maps an Ably channel name to its Postgres notification
// channel: a fixed prefix plus the hex of the first 16 bytes of
// SHA-256(namespace, NUL, name). namespace is the schema the Storage
// works in (current_schema() at Open), because NOTIFY is scoped per
// database, not per schema: two deployments that share a database but
// not a schema must not hear each other's channels.
func pgChannelName(namespace, name string) string {
	sum := sha256.Sum256([]byte(namespace + "\x00" + name))
	return pgChannelPrefix + hex.EncodeToString(sum[:16])
}

// busNotification is the JSON NOTIFY payload of the postgres bus.
// Channel and Serial are the pointer form (Channel is omitted when the
// name alone would not fit in a NOTIFY; the receiver knows its channel
// from the Postgres channel it LISTENs on). Prev is the channel's serial
// immediately before Serial. Kind, Rows and Sums, when present, carry
// the cm inline exactly as stored: one msgpack payload per
// channel_messages row in idx order, plus that row's annotation summary
// column (nil for other kinds). encoding/json base64-encodes the []byte
// fields. Wake marks a coalesced-mode wake-up: Serial is the latest
// write the sender committed on the channel in its window.
type busNotification struct {
	Channel string   `json:"channel,omitempty"`
	Serial  string   `json:"serial"`
	Wake    bool     `json:"wake,omitempty"`
	Prev    string   `json:"prev,omitempty"`
	Kind    string   `json:"kind,omitempty"`
	Rows    [][]byte `json:"rows,omitempty"`
	Sums    [][]byte `json:"sums,omitempty"`
}

// encodeNotification builds the NOTIFY payload for a freshly written cm:
// inline when it fits under inlinePayloadLimit, otherwise the pointer
// form. sums is optional (only annotation rows carry a summary).
func encodeNotification(channel, serial, prev string, kind storage.Kind, rows, sums [][]byte) (payload string, inline bool, err error) {
	n := busNotification{Channel: channel, Serial: serial, Prev: prev, Kind: string(kind), Rows: rows}
	for _, s := range sums {
		if s != nil {
			n.Sums = sums
			break
		}
	}
	b, err := json.Marshal(n)
	if err != nil {
		return "", false, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if len(b) < inlinePayloadLimit {
		return string(b), true, nil
	}
	n.Kind, n.Rows, n.Sums = "", nil, nil
	if b, err = json.Marshal(n); err != nil {
		return "", false, fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if len(b) >= inlinePayloadLimit {
		n.Channel = ""
		if b, err = json.Marshal(n); err != nil {
			return "", false, fmt.Errorf("storage/postgres: encode notify: %w", err)
		}
	}
	return string(b), false, nil
}

// inlineCM decodes an inline payload into the cm a SELECT by
// (channel, serial) returns. It returns nil (and no error) for a pointer
// payload.
func (n *busNotification) inlineCM() (*protocol.ChannelMessage, error) {
	if n.Kind == "" || len(n.Rows) == 0 {
		return nil, nil
	}
	if len(n.Sums) != 0 && len(n.Sums) != len(n.Rows) {
		return nil, fmt.Errorf("storage/postgres: inline payload %s:%s has %d summaries for %d rows", n.Channel, n.Serial, len(n.Sums), len(n.Rows))
	}
	cm := &protocol.ChannelMessage{ChannelSerial: n.Serial}
	for i, row := range n.Rows {
		var sum []byte
		if len(n.Sums) != 0 {
			sum = n.Sums[i]
		}
		if err := decodeRowInto(cm, n.Channel, i, n.Kind, row, sum); err != nil {
			return nil, err
		}
	}
	return cm, nil
}

// dialListenConn opens a raw connection for a LISTEN goroutine. The DSN
// is parsed as a pool DSN so pgxpool's own settings (pool_max_conns and
// the like) are stripped rather than sent to the server as unknown
// runtime parameters. The deadline context-watcher is set explicitly (it
// is also pgx's default): the postgres bus interrupts
// WaitForNotification by cancelling its context, and the deadline
// handler turns that into a read timeout, which pgconn treats as
// non-fatal, so the connection stays usable. A cancel-request handler
// would instead race a server-side cancel against the next LISTEN.
func dialListenConn(ctx context.Context, dsn string) (*pgx.Conn, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: parse DSN for LISTEN conn: %w", err)
	}
	cfg := poolCfg.ConnConfig
	cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.DeadlineContextWatcherHandler{Conn: pgConn.Conn()}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: dial LISTEN conn: %w", err)
	}
	return conn, nil
}

// listenOp asks the LISTEN goroutine to LISTEN on (or UNLISTEN from) one
// Postgres channel. done, when set, is closed once a LISTEN is active on
// the live connection.
type listenOp struct {
	pgChan   string
	unlisten bool
	done     chan struct{}
}

// pgBus is the postgres Bus (see the file comment).
type pgBus struct {
	s        *Storage
	mode     NotifyMode
	notifier *wakeNotifier // non-nil only in NotifyCoalesced mode

	initialConn *pgx.Conn
	connected   atomic.Bool

	// mu guards the bound set, the queued LISTEN/UNLISTEN operations and
	// the cancel func that interrupts WaitForNotification. Operations are
	// queued under the same lock that changes the bound set, so they
	// reach Postgres in the order the set changed.
	mu         sync.Mutex
	bound      map[string]*channelStore // by Postgres channel name: the LISTEN set
	ops        []listenOp
	waitCancel context.CancelFunc

	// workersMu guards workersClosed, which stops new per-channel workers
	// starting once Close has begun; workerWG tracks the running ones.
	workersMu     sync.Mutex
	workersClosed bool
	workerWG      sync.WaitGroup
}

func newPGBus(s *Storage, mode NotifyMode, conn *pgx.Conn, window time.Duration, maxPending int) *pgBus {
	b := &pgBus{s: s, mode: mode, initialConn: conn, bound: make(map[string]*channelStore)}
	b.connected.Store(true) // conn is live; listenLoop tracks it from here
	if mode == NotifyCoalesced {
		b.notifier = newWakeNotifier(s, window, maxPending)
	}
	return b
}

func (b *pgBus) start(ctx context.Context) {
	b.s.wg.Add(1)
	go b.listenLoop(ctx)
	if b.notifier != nil {
		b.s.wg.Add(1)
		go b.notifier.run(ctx)
	}
	b.s.startChainLoop(ctx, b.s.reconnectBase, nil)
}

func (b *pgBus) chains() bool { return true }

// bind makes this node LISTEN on the channel's own Postgres channel and
// waits until the LISTEN is active, so every NOTIFY sent after the
// watermark read that follows reaches this node. If the LISTEN
// connection is down, the request is satisfied by the re-LISTEN that
// follows the reconnect.
func (b *pgBus) bind(ctx context.Context, cs *channelStore) error {
	done := make(chan struct{})
	b.mu.Lock()
	b.bound[cs.pgChan] = cs
	b.ops = append(b.ops, listenOp{pgChan: cs.pgChan, done: done})
	interrupt := b.waitCancel
	b.mu.Unlock()
	if interrupt != nil {
		interrupt() // wake WaitForNotification so the loop runs the LISTEN now
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.s.done:
		return errStorageClosed
	}
}

// unbind drops the channel from the LISTEN set and queues its UNLISTEN,
// unless a newer bind of the same channel has already replaced it.
func (b *pgBus) unbind(cs *channelStore) {
	b.mu.Lock()
	if b.bound[cs.pgChan] != cs {
		b.mu.Unlock()
		return
	}
	delete(b.bound, cs.pgChan)
	b.ops = append(b.ops, listenOp{pgChan: cs.pgChan, unlisten: true})
	interrupt := b.waitCancel
	b.mu.Unlock()
	if interrupt != nil {
		interrupt()
	}
}

// beforeCommit emits the NOTIFY inside the write transaction in
// transactional mode, on the channel's own Postgres channel, with the
// stored rows inline when they fit. Postgres holds the notification
// until commit, so listeners see it only if the write commits. In
// coalesced mode the write commits without NOTIFY.
func (b *pgBus) beforeCommit(ctx context.Context, tx pgx.Tx, cs *channelStore, w *busWrite) error {
	if b.mode == NotifyCoalesced {
		return nil
	}
	payload, inline, err := encodeNotification(cs.name, w.serial, w.prev, w.kind, w.rows, w.sums)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, cs.pgChan, payload); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}
	w.notified, w.pointer = true, !inline
	return nil
}

// afterCommit marks the channel for this window's wake-up (coalesced
// mode) and takes the shared publisher fast path.
func (b *pgBus) afterCommit(cs *channelStore, cm *protocol.ChannelMessage, prev string) {
	if b.notifier != nil {
		b.notifier.mark(cs.pgChan, cm.ChannelSerial)
	}
	b.s.fastPath(cs.name, cm, prev)
}

// errListenDisconnected is ready's answer while the postgres bus's
// LISTEN connection is down: the node receives nothing cross-node until
// it redials, so /readyz takes it out of rotation (DESIGN.md §7.2).
var errListenDisconnected = errors.New("storage/postgres: postgres bus LISTEN connection down")

func (b *pgBus) ready() error {
	if !b.connected.Load() {
		return errListenDisconnected
	}
	return nil
}

func (b *pgBus) isConnected() bool { return b.connected.Load() }

// close stops new workers starting and waits for the running ones.
// Called by Storage.Close after the loop context is cancelled, so
// running workers discard what is left in their queues.
func (b *pgBus) close() {
	b.workersMu.Lock()
	b.workersClosed = true
	b.workersMu.Unlock()
	b.workerWG.Wait()
}

// execOps sends ops to Postgres in order, batched into a few
// simple-protocol round trips.
func (b *pgBus) execOps(ctx context.Context, conn *pgx.Conn, ops []listenOp) error {
	for start := 0; start < len(ops); start += listenBatchSize {
		end := min(start+listenBatchSize, len(ops))
		var sb strings.Builder
		var listens, unlistens uint64
		for _, op := range ops[start:end] {
			if op.unlisten {
				sb.WriteString("UNLISTEN ")
				unlistens++
			} else {
				sb.WriteString("LISTEN ")
				listens++
			}
			sb.WriteString(pgx.Identifier{op.pgChan}.Sanitize())
			sb.WriteString(";")
		}
		if _, err := conn.Exec(ctx, sb.String()); err != nil {
			return fmt.Errorf("storage/postgres: LISTEN: %w", err)
		}
		b.s.stats.listens.Add(listens)
		b.s.stats.unlistens.Add(unlistens)
	}
	return nil
}

// relisten dials a fresh LISTEN connection and LISTENs on every bound
// channel. Queued operations are taken in the same critical section as
// the bound set is read: the set already reflects them, so each is done
// (a LISTEN) or moot (an UNLISTEN on a connection that never LISTENed).
func (b *pgBus) relisten(ctx context.Context) (*pgx.Conn, error) {
	conn, err := dialListenConn(ctx, b.s.dsn)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	queued := b.ops
	b.ops = nil
	ops := make([]listenOp, 0, len(b.bound))
	for pgChan := range b.bound {
		ops = append(ops, listenOp{pgChan: pgChan})
	}
	b.mu.Unlock()
	if err := b.execOps(ctx, conn, ops); err != nil {
		b.requeue(queued)
		_ = conn.Close(context.Background())
		return nil, err
	}
	for _, op := range queued {
		if op.done != nil {
			close(op.done)
		}
	}
	return conn, nil
}

// requeue puts operations back at the head of the queue after a failed
// LISTEN, for the next connection to satisfy.
func (b *pgBus) requeue(ops []listenOp) {
	if len(ops) == 0 {
		return
	}
	b.mu.Lock()
	b.ops = append(ops, b.ops...)
	b.mu.Unlock()
}

// runQueuedOps executes the operations queued since the last call on
// conn. An error means conn is unusable: the operations are requeued and
// the caller reconnects.
func (b *pgBus) runQueuedOps(ctx context.Context, conn *pgx.Conn) error {
	b.mu.Lock()
	ops := b.ops
	b.ops = nil
	b.mu.Unlock()
	if len(ops) == 0 {
		return nil
	}
	if err := b.execOps(ctx, conn, ops); err != nil {
		b.requeue(ops)
		return err
	}
	for _, op := range ops {
		if op.done != nil {
			close(op.done)
		}
	}
	return nil
}

// listenLoop owns the LISTEN connection (DESIGN.md §7.2). It runs
// consume() until the connection fails; unless the storage is closing it
// then re-dials with capped exponential backoff, re-LISTENs every bound
// channel and requests a reconcile of every bound channel from the log,
// so every cm whose NOTIFY was lost in the gap is delivered exactly
// once. It exits only when the storage is Close()d.
func (b *pgBus) listenLoop(ctx context.Context) {
	defer b.s.wg.Done()

	conn := b.initialConn
	b.connected.Store(true)
	for {
		err := b.consume(ctx, conn)
		b.connected.Store(false)
		_ = conn.Close(context.Background())
		if ctx.Err() != nil {
			return // Close(): expected shutdown
		}
		b.s.logger.Warn("storage/postgres: LISTEN connection lost; reconnecting", "err", err)

		conn = b.redial(ctx)
		if conn == nil {
			return // ctx cancelled during backoff
		}
		b.connected.Store(true)
		b.s.requestReconcile()
		b.s.logger.Info("storage/postgres: LISTEN reconnected; reconcile requested")
	}
}

// consume runs the steady state on conn: execute any queued operations,
// wait for a notification (or an interrupt from a new bind or release),
// dispatch, repeat. It returns the error that ended it: a failed
// connection, or ctx cancellation on Close.
func (b *pgBus) consume(ctx context.Context, conn *pgx.Conn) error {
	for {
		if err := b.runQueuedOps(ctx, conn); err != nil {
			return err
		}

		waitCtx, cancel := context.WithCancel(ctx)
		b.mu.Lock()
		b.waitCancel = cancel
		queued := len(b.ops) > 0
		b.mu.Unlock()
		if queued {
			cancel() // an operation raced in after runQueuedOps: go round again
		}

		n, err := conn.WaitForNotification(waitCtx)

		b.mu.Lock()
		b.waitCancel = nil
		b.mu.Unlock()
		interrupted := waitCtx.Err() != nil
		cancel()

		if n != nil {
			b.dispatch(n)
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if interrupted && !conn.IsClosed() {
				continue // woken to run an operation; the conn is fine
			}
			return err
		}
	}
}

// dispatch hands one notification to its channel's ordered queue. It
// does no parsing, decoding or I/O (the channel's worker does that) and
// never blocks, so the LISTEN goroutine only receives and dispatches.
func (b *pgBus) dispatch(n *pgconn.Notification) {
	b.s.stats.received.Add(1)
	b.mu.Lock()
	cs := b.bound[n.Channel]
	b.mu.Unlock()
	if cs == nil {
		b.s.stats.unrouted.Add(1)
		return
	}
	b.enqueue(cs, n.Payload)
}

// redial re-establishes the LISTEN connection with capped exponential
// backoff, retrying until it succeeds or ctx is cancelled (Close). A nil
// return means ctx was cancelled.
func (b *pgBus) redial(ctx context.Context) *pgx.Conn {
	delay := b.s.reconnectBase
	for {
		if !sleepCtx(ctx, delay) {
			return nil
		}
		conn, err := b.relisten(ctx)
		if err == nil {
			return conn
		}
		if ctx.Err() != nil {
			return nil
		}
		b.s.logger.Warn("storage/postgres: LISTEN re-dial failed; backing off", "err", err, "delay", delay)
		if delay *= 2; delay > b.s.reconnectMax {
			delay = b.s.reconnectMax
		}
	}
}

// busQueue is a bound channel's ordered delivery queue on the postgres
// bus. A worker goroutine starts when the queue becomes non-empty and
// exits when it drains, so there is at most one worker per channel and
// the channel's items are handled strictly in order. overflow records
// that a notification was dropped because the queue was full; the worker
// then catches the channel up from the log before anything else.
type busQueue struct {
	mu       sync.Mutex
	items    []string // raw NOTIFY payloads
	running  bool
	overflow bool
}

// enqueue appends a payload to the channel's queue and starts the
// channel's worker if none is running. It never blocks: when the queue
// is full the payload is dropped and the channel is marked for a
// catch-up read, which delivers the dropped cms from the log.
func (b *pgBus) enqueue(cs *channelStore, payload string) {
	q := &cs.q
	q.mu.Lock()
	if len(q.items) >= channelQueueDepth {
		q.overflow = true
		b.s.stats.drops.Add(1)
	} else {
		q.items = append(q.items, payload)
	}
	start := !q.running
	q.running = true
	q.mu.Unlock()
	if start && !b.startWorker(func() { b.drain(cs) }) {
		q.mu.Lock()
		q.running = false // closing: nothing will drain this queue
		q.mu.Unlock()
	}
}

// markCatchUp asks the channel's worker to catch the channel up from
// the log before anything else it has queued.
func (b *pgBus) markCatchUp(cs *channelStore) {
	cs.q.mu.Lock()
	cs.q.overflow = true
	cs.q.mu.Unlock()
}

// startWorker runs fn on a tracked goroutine unless the storage is
// closing.
func (b *pgBus) startWorker(fn func()) bool {
	b.workersMu.Lock()
	defer b.workersMu.Unlock()
	if b.workersClosed {
		return false
	}
	b.workerWG.Add(1)
	go func() {
		defer b.workerWG.Done()
		fn()
	}()
	return true
}

// drain is the channel's worker: it waits until the channel's bind has
// finished (so a wake-up is never skipped as "not yet seeded"), then
// handles queued items in order until the queue is empty.
func (b *pgBus) drain(cs *channelStore) {
	ctx := b.s.loopCtx
	select {
	case <-cs.ready:
	case <-ctx.Done():
		return
	}
	q := &cs.q
	for {
		q.mu.Lock()
		if q.overflow {
			q.overflow = false
			q.items = q.items[:0] // the catch-up covers everything queued so far
			q.mu.Unlock()
			if ctx.Err() == nil && !cs.isReleased() {
				if err := cs.catchUp(ctx); err != nil && ctx.Err() == nil {
					// Retry after a pause: the cms are in the log, and nothing
					// else would read them until the next sweep.
					b.s.logger.Warn("storage/postgres: catch-up from the log failed; retrying", "channel", cs.name, "err", err)
					b.markCatchUp(cs)
					sleepCtx(ctx, cs.timing.gapDelay)
				}
			}
			continue
		}
		if len(q.items) == 0 {
			q.items = nil
			q.running = false
			q.mu.Unlock()
			return
		}
		payload := q.items[0]
		q.items[0] = ""
		q.items = q.items[1:]
		q.mu.Unlock()

		if ctx.Err() != nil || cs.isReleased() {
			continue // closing, or released: discard
		}
		b.process(ctx, cs, payload)
	}
}

// process handles one notification on the channel's worker: a wake-up,
// which reads everything after the mark, or a cm announcement, inline or
// a pointer, offered to the chained delivery point.
func (b *pgBus) process(ctx context.Context, cs *channelStore, payload string) {
	st := &b.s.stats
	var n busNotification
	if err := json.Unmarshal([]byte(payload), &n); err != nil || n.Serial == "" {
		st.malformed.Add(1)
		return
	}
	if n.Wake {
		st.wakeupsReceived.Add(1)
		if n.Serial <= cs.watermark() {
			st.duplicates.Add(1)
			return
		}
		if err := cs.catchUp(ctx); err != nil && ctx.Err() == nil {
			b.s.logger.Warn("storage/postgres: wake-up read failed; retrying", "channel", cs.name, "err", err)
			b.markCatchUp(cs)
		}
		return
	}
	cm, err := n.inlineCM()
	if err != nil {
		b.s.logger.Warn("storage/postgres: bad inline payload; reading the cm instead", "channel", cs.name, "serial", n.Serial, "err", err)
		cm = nil
	}
	cs.deliverChained(cs.resolvePointer(busEvent{serial: n.Serial, prev: n.Prev, cm: cm, src: srcBus}))
}
