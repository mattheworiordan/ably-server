package postgres

// This file is the cluster pub/sub bus that rides Postgres LISTEN/NOTIFY
// (DESIGN.md §7.2). Each Ably channel has its own Postgres notification
// channel, and a node LISTENs on it only once it binds that channel
// (Storage.Channel with an appender), so a node receives notifications
// only for channels it holds. A cm small enough to fit in a NOTIFY
// travels inline, so receivers skip the SELECT read-back; bigger cms
// send a pointer. The LISTEN goroutine only receives and dispatches: each
// notification goes to its channel's bounded, ordered delivery queue, and
// a worker per busy channel does the parsing, reads and appends, so a
// slow read or a slow appender on one channel never stalls another.
//
// Every notification also carries the channel's previous serial. That
// predecessor check makes the publisher's fast path safe (the publishing
// node appends its own cm straight after commit, and the NOTIFY that
// follows is a no-op) and lets a receiver repair any gap from the log.

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

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

const (
	// pgChannelPrefix starts every Postgres notification channel the bus
	// uses. The rest of the name is a hash (see pgChannelName), so the
	// whole name is a lower-case identifier well inside Postgres's
	// 63-byte identifier limit, whatever the Ably channel name is.
	pgChannelPrefix = "ably_c_"

	// inlinePayloadLimit is the size below which the bus carries a cm
	// inline in its NOTIFY. pg_notify rejects payloads of 8000 bytes or
	// more (with the default 8 kB block size), so the bus keeps a margin
	// and sends a pointer (channel, serial) for anything bigger.
	inlinePayloadLimit = 7900

	// listenBatchSize caps the LISTEN statements sent in one round trip
	// (bind bursts, and the re-LISTEN after a reconnect).
	listenBatchSize = 500

	// channelQueueDepth bounds each bound channel's delivery queue. When
	// a queue is full the LISTEN goroutine blocks on it, so backpressure
	// is explicit instead of an unbounded buffer.
	channelQueueDepth = 1024

	// fillPageSize caps the cms one gap-fill or reconcile read returns;
	// the fill loops until it has caught up.
	fillPageSize = 500
)

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

// busNotification is the JSON NOTIFY payload. Channel and Serial are the
// pointer form. Prev is the channel's serial immediately before Serial,
// the receiver's gap check (empty only for the first publish on a
// channel whose row did not exist yet). Kind, Rows and Sums, when
// present, carry the cm inline exactly as stored: one msgpack payload per
// channel_messages row in idx order, plus that row's annotation summary
// column (nil for other kinds). encoding/json base64-encodes the []byte
// fields.
type busNotification struct {
	Channel string   `json:"channel"`
	Serial  string   `json:"serial"`
	Wake    bool     `json:"wake,omitempty"` // coalesced-mode wake-up: read everything after the mark
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
		if err := decodeRow(cm, n.Kind, row, sum); err != nil {
			return nil, fmt.Errorf("storage/postgres: inline payload %s:%s idx=%d: %w", n.Channel, n.Serial, i, err)
		}
	}
	return cm, nil
}

// busCounters are this node's bus counters (see BusStats).
type busCounters struct {
	notifications atomic.Uint64
	unrouted      atomic.Uint64
	malformed     atomic.Uint64
	inline        atomic.Uint64
	fetched       atomic.Uint64
	fastPath      atomic.Uint64
	duplicates    atomic.Uint64
	gapFills      atomic.Uint64
	reconciles    atomic.Uint64
	filled        atomic.Uint64
	fetchErrors   atomic.Uint64
	listens       atomic.Uint64
	wakeupsSent   atomic.Uint64
	wakeups       atomic.Uint64
	pulls         atomic.Uint64
	polls         atomic.Uint64
	pollPulls     atomic.Uint64
}

// BusStats is a point-in-time copy of this node's bus counters
// (DESIGN.md §7.2). Integration tests use it to prove which delivery
// path a cm took.
type BusStats struct {
	// Notifications counts NOTIFYs received on this node's LISTEN
	// connection. With per-channel LISTEN it grows with the traffic on
	// the channels this node holds, not with the cluster's traffic.
	Notifications uint64
	// Unrouted counts NOTIFYs for a Postgres channel with no bound store
	// (only possible after a failed bind left a LISTEN behind).
	Unrouted uint64
	// Malformed counts NOTIFY payloads that did not parse.
	Malformed uint64
	// Inline counts cms delivered from an inline payload, with no SELECT.
	// Inline and Fetched count deliveries, not payloads received.
	Inline uint64
	// Fetched counts cms delivered after a SELECT by (channel, serial):
	// the pointer path for cms too big to inline.
	Fetched uint64
	// FastPath counts cms the publishing node delivered to its own
	// appender straight after commit.
	FastPath uint64
	// Duplicates counts NOTIFYs dropped because the cm was already
	// delivered (by the fast path, a gap fill or a reconcile).
	Duplicates uint64
	// GapFills counts range reads triggered by a predecessor mismatch.
	GapFills uint64
	// Reconciles counts per-channel range reads after a LISTEN reconnect.
	Reconciles uint64
	// Filled counts cms delivered by gap fills and reconciles.
	Filled uint64
	// FetchErrors counts failed reads on the delivery path; the next
	// notification on the channel repairs the gap from the log.
	FetchErrors uint64
	// Listens counts LISTEN statements this node has issued, including
	// re-LISTENs after a reconnect.
	Listens uint64
	// WakeupsSent counts coalesced-mode wake-ups this node's notifier sent
	// (at most one per channel per window).
	WakeupsSent uint64
	// Wakeups counts coalesced-mode wake-ups this node received.
	Wakeups uint64
	// Pulls counts range reads run for wake-ups, polls and fast-path gaps
	// in coalesced mode.
	Pulls uint64
	// Polls counts safety-net polls; PollPulls the range reads they queued.
	Polls     uint64
	PollPulls uint64
	// BoundChannels is the number of channels this node LISTENs for.
	BoundChannels int
}

// BusStats returns a snapshot of this node's bus counters.
func (s *Storage) BusStats() BusStats {
	s.mu.RLock()
	bound := len(s.bound)
	s.mu.RUnlock()
	c := &s.stats
	return BusStats{
		Notifications: c.notifications.Load(),
		Unrouted:      c.unrouted.Load(),
		Malformed:     c.malformed.Load(),
		Inline:        c.inline.Load(),
		Fetched:       c.fetched.Load(),
		FastPath:      c.fastPath.Load(),
		Duplicates:    c.duplicates.Load(),
		GapFills:      c.gapFills.Load(),
		Reconciles:    c.reconciles.Load(),
		Filled:        c.filled.Load(),
		FetchErrors:   c.fetchErrors.Load(),
		Listens:       c.listens.Load(),
		WakeupsSent:   c.wakeupsSent.Load(),
		Wakeups:       c.wakeups.Load(),
		Pulls:         c.pulls.Load(),
		Polls:         c.polls.Load(),
		PollPulls:     c.pollPulls.Load(),
		BoundChannels: bound,
	}
}

// listenRequest asks the LISTEN goroutine to LISTEN on one Postgres
// channel. done is closed once the LISTEN is active on the live
// connection.
type listenRequest struct {
	pgChan string
	done   chan struct{}
}

// listen makes the LISTEN goroutine LISTEN on pgChan and waits until the
// LISTEN is active. If the LISTEN connection is down, the request is
// satisfied by the re-LISTEN that follows the reconnect.
func (s *Storage) listen(ctx context.Context, pgChan string) error {
	req := &listenRequest{pgChan: pgChan, done: make(chan struct{})}
	s.listenMu.Lock()
	s.listenPending = append(s.listenPending, req)
	interrupt := s.waitCancel
	s.listenMu.Unlock()
	if interrupt != nil {
		interrupt() // wake WaitForNotification so the loop runs the LISTEN now
	}
	select {
	case <-req.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.loopCtx.Done():
		return errStorageClosed
	}
}

// dialListenConn opens a raw connection for the LISTEN goroutine. The
// deadline context-watcher is set explicitly (it is also pgx's default):
// the loop interrupts WaitForNotification by cancelling its context, and
// the deadline handler turns that into a read timeout, which pgconn
// treats as non-fatal, so the connection stays usable. A cancel-request
// handler would instead race a server-side cancel against the next
// LISTEN.
func dialListenConn(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: parse DSN for LISTEN conn: %w", err)
	}
	cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.DeadlineContextWatcherHandler{Conn: pgConn.Conn()}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: dial LISTEN conn: %w", err)
	}
	return conn, nil
}

// execListen issues LISTEN for every channel in chans, batching the
// statements into a few simple-protocol round trips.
func (s *Storage) execListen(ctx context.Context, conn *pgx.Conn, chans []string) error {
	for start := 0; start < len(chans); start += listenBatchSize {
		end := min(start+listenBatchSize, len(chans))
		var b strings.Builder
		for _, c := range chans[start:end] {
			b.WriteString("LISTEN ")
			b.WriteString(pgx.Identifier{c}.Sanitize())
			b.WriteString(";")
		}
		if _, err := conn.Exec(ctx, b.String()); err != nil {
			return fmt.Errorf("storage/postgres: LISTEN: %w", err)
		}
		s.stats.listens.Add(uint64(end - start))
	}
	return nil
}

// relisten dials a fresh LISTEN connection and LISTENs on every bound
// channel plus every pending request, then marks those requests done.
// Used for the reconnect path. The pending requests are taken before the
// bound set is read: a store joins the bound set before it queues its
// request, so the union covers every channel that must be LISTENed.
func (s *Storage) relisten(ctx context.Context) (*pgx.Conn, error) {
	conn, err := dialListenConn(ctx, s.dsn)
	if err != nil {
		return nil, err
	}
	s.listenMu.Lock()
	reqs := s.listenPending
	s.listenPending = nil
	s.listenMu.Unlock()

	seen := make(map[string]struct{})
	var chans []string
	s.mu.RLock()
	for pgChan := range s.bound {
		seen[pgChan] = struct{}{}
		chans = append(chans, pgChan)
	}
	s.mu.RUnlock()
	for _, r := range reqs {
		if _, ok := seen[r.pgChan]; !ok {
			seen[r.pgChan] = struct{}{}
			chans = append(chans, r.pgChan)
		}
	}
	if err := s.execListen(ctx, conn, chans); err != nil {
		s.requeueListens(reqs)
		_ = conn.Close(context.Background())
		return nil, err
	}
	for _, r := range reqs {
		close(r.done)
	}
	return conn, nil
}

// requeueListens puts requests back at the head of the pending list after
// a failed LISTEN, for the next connection to satisfy.
func (s *Storage) requeueListens(reqs []*listenRequest) {
	if len(reqs) == 0 {
		return
	}
	s.listenMu.Lock()
	s.listenPending = append(reqs, s.listenPending...)
	s.listenMu.Unlock()
}

// runPendingListens executes the LISTEN requests queued since the last
// call on conn. An error means conn is unusable: the requests are
// requeued and the caller reconnects.
func (s *Storage) runPendingListens(ctx context.Context, conn *pgx.Conn) error {
	s.listenMu.Lock()
	reqs := s.listenPending
	s.listenPending = nil
	s.listenMu.Unlock()
	if len(reqs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(reqs))
	chans := make([]string, 0, len(reqs))
	for _, r := range reqs {
		if _, ok := seen[r.pgChan]; !ok {
			seen[r.pgChan] = struct{}{}
			chans = append(chans, r.pgChan)
		}
	}
	if err := s.execListen(ctx, conn, chans); err != nil {
		s.requeueListens(reqs)
		return err
	}
	for _, r := range reqs {
		close(r.done)
	}
	return nil
}

// listenLoop owns the LISTEN connection (DESIGN.md §7.2). It runs
// consume() until the connection fails; unless the storage is closing it
// then re-dials with capped exponential backoff, re-LISTENs every bound
// channel and queues a reconcile on each bound channel before it resumes,
// so every cm whose NOTIFY was lost in the gap is replayed exactly once.
// It exits only when the storage is Close()d.
func (s *Storage) listenLoop(ctx context.Context) {
	defer s.wg.Done()

	conn := s.initialListenConn
	for {
		err := s.consume(ctx, conn)
		_ = conn.Close(context.Background())
		if ctx.Err() != nil {
			return // Close(): expected shutdown
		}
		s.logger.Warn("storage/postgres: LISTEN connection lost; reconnecting", "err", err)

		conn = s.redial(ctx)
		if conn == nil {
			return // ctx cancelled during backoff
		}
		// Re-LISTEN is done by redial. Queue a reconcile on every bound
		// channel before resuming: it sits ahead of any notification the
		// new connection delivers on that channel's queue, and deliver()
		// drops the overlap.
		s.reconcile(ctx)
		s.logger.Info("storage/postgres: LISTEN reconnected; reconcile queued")
	}
}

// consume runs the steady state on conn: execute any pending LISTENs,
// wait for a notification (or an interrupt from a new bind), dispatch,
// repeat. It returns the error that ended it: a failed connection, or
// ctx cancellation on Close.
func (s *Storage) consume(ctx context.Context, conn *pgx.Conn) error {
	for {
		if err := s.runPendingListens(ctx, conn); err != nil {
			return err
		}

		waitCtx, cancel := context.WithCancel(ctx)
		s.listenMu.Lock()
		s.waitCancel = cancel
		pending := len(s.listenPending) > 0
		s.listenMu.Unlock()
		if pending {
			cancel() // a bind raced in after runPendingListens: go round again
		}

		n, err := conn.WaitForNotification(waitCtx)

		s.listenMu.Lock()
		s.waitCancel = nil
		s.listenMu.Unlock()
		interrupted := waitCtx.Err() != nil
		cancel()

		if n != nil {
			s.dispatch(ctx, n)
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if interrupted && !conn.IsClosed() {
				continue // woken to run a LISTEN; the conn is fine
			}
			return err
		}
	}
}

// dispatch hands one notification to its channel's ordered queue. It does
// no parsing, decoding or I/O (the channel's worker does that), so the
// LISTEN goroutine only receives and dispatches. It blocks while the
// channel's queue is full: explicit backpressure.
func (s *Storage) dispatch(ctx context.Context, n *pgconn.Notification) {
	s.stats.notifications.Add(1)
	s.mu.RLock()
	cs := s.bound[n.Channel]
	s.mu.RUnlock()
	if cs == nil {
		s.stats.unrouted.Add(1)
		return
	}
	cs.enqueue(ctx, busItem{payload: n.Payload})
}

// redial re-establishes the LISTEN connection with capped exponential
// backoff, retrying until it succeeds or ctx is cancelled (Close). A nil
// return means ctx was cancelled.
func (s *Storage) redial(ctx context.Context) *pgx.Conn {
	delay := s.reconnectBase
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		conn, err := s.relisten(ctx)
		if err == nil {
			return conn
		}
		if ctx.Err() != nil {
			return nil
		}
		s.logger.Warn("storage/postgres: LISTEN re-dial failed; backing off", "err", err, "delay", delay)
		if delay *= 2; delay > s.reconnectMax {
			delay = s.reconnectMax
		}
	}
}

// reconcile queues, on every bound and initialised channel, a replay of
// every cm minted past the channel's last delivered serial: the cms whose
// NOTIFY was lost while the LISTEN connection was down (DESIGN.md §7.2).
// Each channel reconciles on its own worker, so channels catch up in
// parallel and each stays ordered behind its reconcile.
func (s *Storage) reconcile(ctx context.Context) {
	s.mu.RLock()
	stores := make([]*channelStore, 0, len(s.bound))
	for _, cs := range s.bound {
		stores = append(stores, cs)
	}
	s.mu.RUnlock()
	for _, cs := range stores {
		if cs.isReady() { // a bind in progress reads its watermark after its LISTEN
			cs.enqueue(ctx, busItem{reconcile: true})
		}
	}
}

// startWorker runs fn on a tracked goroutine unless the storage is
// closing. Workers are started lazily, one per channel with queued work,
// and exit when their queue drains, so idle channels cost no goroutine.
func (s *Storage) startWorker(fn func()) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	if s.workersClosed {
		return false
	}
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		fn()
	}()
	return true
}

// stopWorkers stops new workers starting and waits for running ones to
// finish. Called by Close after the loop context is cancelled, so
// running workers discard what is left in their queues.
func (s *Storage) stopWorkers() {
	s.workersMu.Lock()
	s.workersClosed = true
	s.workersMu.Unlock()
	s.workerWG.Wait()
}

// busItem is one unit of work on a channel's delivery queue: a raw NOTIFY
// payload, a reconcile request, or (coalesced mode) a pull: a range read
// of everything after the high-water mark.
type busItem struct {
	payload   string
	reconcile bool
	pull      bool
}

// busQueue is a bound channel's ordered delivery queue. slots holds one
// token per queued item, so a full queue blocks the producer. A worker
// goroutine starts when the queue becomes non-empty and exits when it
// drains, so there is at most one worker per channel and the channel's
// items are handled strictly in order.
type busQueue struct {
	slots   chan struct{}
	mu      sync.Mutex
	items   []busItem
	running bool
}

// enqueue appends it to the channel's queue, blocking while the queue is
// full, and starts the channel's worker if none is running. It returns
// false if ctx ends first.
func (cs *channelStore) enqueue(ctx context.Context, it busItem) bool {
	select {
	case cs.q.slots <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	cs.q.mu.Lock()
	cs.q.items = append(cs.q.items, it)
	start := !cs.q.running
	cs.q.running = true
	cs.q.mu.Unlock()
	if start && !cs.s.startWorker(cs.drain) {
		cs.q.mu.Lock()
		cs.q.running = false // closing: nothing will drain this queue
		cs.q.mu.Unlock()
	}
	return true
}

// drain is the channel's worker: it waits until the channel's bind has
// finished, then handles queued items in order until the queue is empty.
func (cs *channelStore) drain() {
	ctx := cs.s.loopCtx
	select {
	case <-cs.ready:
	case <-ctx.Done():
		return
	}
	for {
		cs.q.mu.Lock()
		if len(cs.q.items) == 0 {
			cs.q.items = nil
			cs.q.running = false
			cs.q.mu.Unlock()
			return
		}
		it := cs.q.items[0]
		cs.q.items[0] = busItem{}
		cs.q.items = cs.q.items[1:]
		cs.q.mu.Unlock()
		<-cs.q.slots

		if ctx.Err() != nil || cs.abandoned.Load() {
			continue // closing, or the bind failed: discard
		}
		cs.process(ctx, it)
	}
}

// process handles one queued item on the channel's worker: a reconcile,
// or a notification whose cm it decodes inline or reads back by serial.
func (cs *channelStore) process(ctx context.Context, it busItem) {
	st := &cs.s.stats
	switch {
	case it.reconcile:
		st.reconciles.Add(1)
		cs.fill(ctx, "")
		return
	case it.pull:
		cs.pullQueued.Store(false)
		st.pulls.Add(1)
		cs.fill(ctx, "")
		return
	}

	var n busNotification
	if err := json.Unmarshal([]byte(it.payload), &n); err != nil || n.Serial == "" {
		st.malformed.Add(1)
		return
	}
	last := cs.watermark()
	if n.Wake {
		// Coalesced-mode wake-up: n.Serial is the latest write the sender
		// made on this channel in its window. Read everything after the
		// mark, unless we are already past it.
		st.wakeups.Add(1)
		if n.Serial <= last {
			st.duplicates.Add(1)
			return
		}
		st.pulls.Add(1)
		cs.fill(ctx, "")
		return
	}
	if n.Serial <= last {
		st.duplicates.Add(1) // already delivered (fast path, fill): no read needed
		return
	}
	if n.Prev != "" && n.Prev > last {
		// A cm between our last delivery and this one has not been
		// delivered here (a lost NOTIFY, or a read that failed): repair the
		// gap, and this cm with it, from the log.
		st.gapFills.Add(1)
		cs.fill(ctx, n.Serial)
		return
	}

	cm, err := n.inlineCM()
	if err != nil {
		cs.s.logger.Warn("storage/postgres: bad inline payload; reading the cm instead", "channel", cs.name, "serial", n.Serial, "err", err)
	}
	inline := cm != nil
	if !inline {
		if cm, err = cs.s.loadChannelMessage(ctx, cs.name, n.Serial); err != nil {
			// The next notification's predecessor check sees the gap and
			// fills it from the log.
			st.fetchErrors.Add(1)
			return
		}
	}
	switch cs.deliver(cm, n.Prev) {
	case deliverOK:
		if inline {
			st.inline.Add(1)
		} else {
			st.fetched.Add(1)
		}
	case deliverDuplicate: // the fast path delivered it while we decoded or read
		st.duplicates.Add(1)
	case deliverGap: // cannot happen after the check above; stay safe
		st.gapFills.Add(1)
		cs.fill(ctx, n.Serial)
	}
}

// fill delivers, in order, every cm after the high-water mark up to and
// including upTo (no upper bound when upTo is empty), reading the log in
// pages. Messages, presence and annotations share the read, which decodes
// rows exactly as the pointer path does. A fill reads a contiguous range
// that starts at the mark, so only the duplicate check applies.
func (cs *channelStore) fill(ctx context.Context, upTo string) {
	st := &cs.s.stats
	for {
		after := cs.watermark()
		cms, err := cs.s.loadRange(ctx, cs.name, after, upTo, fillPageSize)
		if err != nil {
			st.fetchErrors.Add(1)
			if ctx.Err() == nil {
				cs.s.logger.Warn("storage/postgres: gap fill failed", "channel", cs.name, "after", after, "err", err)
			}
			return
		}
		for _, cm := range cms {
			if cs.deliver(cm, "") == deliverOK {
				st.filled.Add(1)
			}
		}
		if len(cms) < fillPageSize {
			return
		}
	}
}

// publishLocal is the publisher's fast path (DESIGN.md §7.2): straight
// after its own transaction commits, the publishing node delivers the cm
// to its own appender, and the NOTIFY that follows is dropped by the
// high-water mark. If another node's publish on this channel committed
// just before ours and has not reached this node yet (prev is above the
// mark), the fast path steps aside: our cm then arrives through the
// NOTIFY path behind the earlier one, so the appender still sees every
// cm once and in serial order.
//
// In coalesced mode it also marks the channel for this window's wake-up,
// and on a gap it queues a range read at once rather than waiting for a
// wake-up.
func (cs *channelStore) publishLocal(cm *protocol.ChannelMessage, prev string) {
	if cs.s.notifier != nil {
		cs.s.notifier.mark(cs.pgChan, cs.name, cm.ChannelSerial)
	}
	if cs.appender == nil || !cs.isReady() {
		return // not bound here, or the bind is still in progress: NOTIFY delivers
	}
	switch cs.deliver(cm, prev) {
	case deliverOK:
		cs.s.stats.fastPath.Add(1)
	case deliverGap:
		if cs.s.notifier != nil {
			cs.requestPull(cs.s.loopCtx)
		}
	}
}

// isReady reports whether the channel is bound and initialised.
func (cs *channelStore) isReady() bool {
	select {
	case <-cs.ready:
		return !cs.abandoned.Load()
	default:
		return false
	}
}

// abandon marks a store whose bind failed, so nothing waits for an
// Initialize that will never come.
func (cs *channelStore) abandon() {
	cs.abandoned.Store(true)
	close(cs.ready)
}

// watermark returns the channel's high-water mark: the highest serial
// delivered to the appender (seeded with the bind-time watermark).
func (cs *channelStore) watermark() string {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return cs.lastSeen
}

// deliverResult is the outcome of deliver.
type deliverResult int

const (
	deliverOK        deliverResult = iota // appended
	deliverDuplicate                      // serial at or below the high-water mark
	deliverGap                            // an earlier cm is not delivered yet
)

// deliver appends cm if it is the next cm on the channel: its serial is
// above the high-water mark, and prev (the channel's serial just before
// it) is not above the mark. An empty prev skips the gap check (range
// reads, and the first publish on a brand-new channel row). Appends are
// serialised per channel by hwmMu, held across appender.Append, so the
// appender sees cms strictly in serial order even with the publisher's
// fast path and the channel's worker both delivering.
func (cs *channelStore) deliver(cm *protocol.ChannelMessage, prev string) deliverResult {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if cm.ChannelSerial <= cs.lastSeen {
		return deliverDuplicate
	}
	if prev != "" && prev > cs.lastSeen {
		return deliverGap
	}
	cs.lastSeen = cm.ChannelSerial
	cs.appender.Append(cm)
	return deliverOK
}

// advanceSerial mints the next channelSerial for channel inside tx and
// also returns the serial it follows (prev), in one round trip: the
// batch first locks the channels row (SELECT ... FOR UPDATE waits for any
// concurrent publisher, then reads the latest value) and then calls
// advance_channel_serial, which advances from that same locked value.
// prev is empty only for the first publish on a channel whose row did
// not exist yet.
func advanceSerial(ctx context.Context, tx pgx.Tx, channel, series string) (prev, next string, err error) {
	b := &pgx.Batch{}
	b.Queue(`SELECT channel_serial FROM channels WHERE name = $1 FOR UPDATE`, channel)
	b.Queue(`SELECT advance_channel_serial($1, $2)`, channel, series)
	br := tx.SendBatch(ctx, b)
	if err := br.QueryRow().Scan(&prev); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		_ = br.Close()
		return "", "", fmt.Errorf("storage/postgres: lock channel row: %w", err)
	}
	if err := br.QueryRow().Scan(&next); err != nil {
		_ = br.Close()
		return "", "", fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	if err := br.Close(); err != nil {
		return "", "", fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	return prev, next, nil
}

const sqlLoadRange = `
SELECT channel_serial, idx, kind, payload, summary FROM channel_messages
WHERE channel = $1 AND channel_serial IN (
	SELECT DISTINCT channel_serial FROM channel_messages
	WHERE channel = $1 AND channel_serial > $2 AND ($3 = '' OR channel_serial <= $3)
	ORDER BY channel_serial
	LIMIT $4)
ORDER BY channel_serial, idx
`

// loadRange reads up to limit cms on channel with a serial above after
// and at most upTo (no upper bound when upTo is empty), in serial order,
// every kind together, decoded as the pointer path decodes them.
func (s *Storage) loadRange(ctx context.Context, channel, after, upTo string, limit int) ([]*protocol.ChannelMessage, error) {
	rows, err := s.pool.Query(ctx, sqlLoadRange, channel, after, upTo, limit)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: load range %s after %s: %w", channel, after, err)
	}
	defer rows.Close()

	var out []*protocol.ChannelMessage
	for rows.Next() {
		var (
			cs      string
			idx     int
			kind    string
			payload []byte
			summary []byte
		)
		if err := rows.Scan(&cs, &idx, &kind, &payload, &summary); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan range %s: %w", channel, err)
		}
		if n := len(out); n == 0 || out[n-1].ChannelSerial != cs {
			out = append(out, &protocol.ChannelMessage{ChannelSerial: cs})
		}
		if err := decodeRow(out[len(out)-1], kind, payload, summary); err != nil {
			return nil, fmt.Errorf("storage/postgres: range %s:%s idx=%d: %w", channel, cs, idx, err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: range rows %s: %w", channel, err)
	}
	return out, nil
}

// notifyTx emits the bus NOTIFY for a freshly written cm inside tx, on
// the channel's own Postgres notification channel, carrying the cm's
// stored rows inline when they fit. Postgres holds the notification
// until commit, so listeners see it only if the write commits.
func (cs *channelStore) notifyTx(ctx context.Context, tx pgx.Tx, serial, prev string, kind storage.Kind, rows, sums [][]byte) error {
	if cs.s.notifier != nil {
		return nil // coalesced mode: the notifier wakes receivers after commit
	}
	payload, _, err := encodeNotification(cs.name, serial, prev, kind, rows, sums)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, cs.pgChan, payload); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}
	return nil
}
