package postgres

// This file is the cluster pub/sub bus that rides Postgres LISTEN/NOTIFY
// (DESIGN.md §7.2). Each Ably channel has its own Postgres notification
// channel, and a node LISTENs on it only once it binds that channel
// (Storage.Channel with an appender), so a node receives notifications
// only for channels it holds.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

const (
	// pgChannelPrefix starts every Postgres notification channel the bus
	// uses. The rest of the name is a hash (see pgChannelName), so the
	// whole name is a lower-case identifier well inside Postgres's
	// 63-byte identifier limit, whatever the Ably channel name is.
	pgChannelPrefix = "ably_c_"

	// listenBatchSize caps the LISTEN statements sent in one round trip
	// (bind bursts, and the re-LISTEN after a reconnect).
	listenBatchSize = 500
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

// busNotification is the JSON NOTIFY payload: a pointer (channel,
// serial) to the committed cm, which the receiver reads back.
type busNotification struct {
	Channel string `json:"channel"`
	Serial  string `json:"serial"`
}

// encodeNotification builds the NOTIFY payload for a freshly written cm.
func encodeNotification(channel, serial string) (string, error) {
	b, err := json.Marshal(busNotification{Channel: channel, Serial: serial})
	if err != nil {
		return "", fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	return string(b), nil
}

// busCounters are this node's bus counters (see BusStats).
type busCounters struct {
	notifications atomic.Uint64
	unrouted      atomic.Uint64
	malformed     atomic.Uint64
	fetched       atomic.Uint64
	duplicates    atomic.Uint64
	fetchErrors   atomic.Uint64
	listens       atomic.Uint64
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
	// Fetched counts cms delivered after a SELECT by (channel, serial).
	Fetched uint64
	// Duplicates counts NOTIFYs dropped because the cm was already
	// delivered.
	Duplicates uint64
	// FetchErrors counts failed reads on the delivery path.
	FetchErrors uint64
	// Listens counts LISTEN statements this node has issued, including
	// re-LISTENs after a reconnect.
	Listens uint64
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
		Fetched:       c.fetched.Load(),
		Duplicates:    c.duplicates.Load(),
		FetchErrors:   c.fetchErrors.Load(),
		Listens:       c.listens.Load(),
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
// channel and reconciles each bound channel before it resumes, so every
// cm whose NOTIFY was lost in the gap is replayed exactly once. It exits
// only when the storage is Close()d.
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
		// Re-LISTEN is done by redial; reconcile before resuming so any
		// cm minted during the gap is replayed exactly once (deliver()
		// dedups against a subsequent buffered NOTIFY).
		s.reconcile(ctx)
		s.logger.Info("storage/postgres: LISTEN reconnected and reconciled")
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

// dispatch routes one notification to the channel store bound to its
// Postgres channel, reads the cm back and delivers it. It waits for a
// bind still in progress: the LISTEN is active before the bind reads its
// watermark, so a cm committed in between must not be dropped.
func (s *Storage) dispatch(ctx context.Context, n *pgconn.Notification) {
	s.stats.notifications.Add(1)
	s.mu.RLock()
	cs := s.bound[n.Channel]
	s.mu.RUnlock()
	if cs == nil {
		s.stats.unrouted.Add(1)
		return
	}
	select {
	case <-cs.ready:
	case <-ctx.Done():
		return
	}
	if cs.abandoned.Load() {
		return
	}

	var p busNotification
	if err := json.Unmarshal([]byte(n.Payload), &p); err != nil || p.Serial == "" {
		s.stats.malformed.Add(1)
		return
	}
	if p.Serial <= cs.watermark() {
		s.stats.duplicates.Add(1)
		return
	}
	cm, err := s.loadChannelMessage(ctx, cs.name, p.Serial)
	if err != nil {
		s.stats.fetchErrors.Add(1)
		return // best-effort; nothing we can do without the cm
	}
	s.stats.fetched.Add(1)
	cs.deliver(cm)
}

// redial re-establishes the LISTEN connection with capped exponential
// backoff, retrying until it succeeds or ctx is cancelled (Close). A nil
// return means ctx was cancelled.
func (s *Storage) redial(ctx context.Context) *pgx.Conn {
	delay := listenReconnectBaseDelay
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
		if delay *= 2; delay > listenReconnectMaxDelay {
			delay = listenReconnectMaxDelay
		}
	}
}

// reconcile replays, per bound and initialised channel, every cm minted
// past the channel's last-delivered serial: the cms whose NOTIFY was lost
// while the LISTEN connection was down (DESIGN.md §7.2). Each is
// delivered through cs.deliver, whose high-water mark makes replay
// idempotent against the normal NOTIFY path.
func (s *Storage) reconcile(ctx context.Context) {
	s.mu.RLock()
	stores := make([]*channelStore, 0, len(s.bound))
	for _, cs := range s.bound {
		stores = append(stores, cs)
	}
	s.mu.RUnlock()

	for _, cs := range stores {
		if !cs.isReady() {
			continue // bind in progress: it reads its watermark after its LISTEN
		}
		if err := cs.reconcileFromHistory(ctx); err != nil {
			s.logger.Warn("storage/postgres: reconcile failed", "channel", cs.name, "err", err)
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

// notifyTx emits the bus NOTIFY for a freshly written cm inside tx, on
// the channel's own Postgres notification channel. Postgres holds the
// notification until commit, so listeners see it only if the write
// commits.
func (cs *channelStore) notifyTx(ctx context.Context, tx pgx.Tx, serial string) error {
	payload, err := encodeNotification(cs.name, serial)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, cs.pgChan, payload); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}
	return nil
}
