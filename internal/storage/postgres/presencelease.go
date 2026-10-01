package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// reapChunk caps the members one reaper DELETE removes, so reaping a
// dead node with many members never holds that many row locks in one
// statement. A package var so tests can shrink it.
var reapChunk = 1000

// reapLeaveConcurrency bounds the reaper's LEAVE transactions in flight
// at once (one per channel with reaped members, publishReapedLeaves), so
// a dead node's LEAVEs do not take more of the pool than live traffic
// can spare.
const reapLeaveConcurrency = 8

// leaseBumpHook, when set (tests), runs before each lease renewal (the
// node lease upsert) with the renewing node's id and can fail it, as a
// Postgres outage would. Nil in production.
var leaseBumpHook atomic.Pointer[func(node string) error]

// deadLeaseGrace is how long past its expiry a lease must be before the
// reaper treats its node (or a legacy row's own lease) as dead: one bump
// interval, which absorbs clock skew between nodes and a bump that is
// late by up to one tick (DESIGN.md §12.5).
func deadLeaseGrace() float64 { return presenceLeaseBumpInterval.Seconds() }

// leaseReleaseTimeout bounds the statement a graceful Close runs to
// delete the node's lease row.
const leaseReleaseTimeout = 5 * time.Second

// sqlTakeNodeLease creates or renews this node's lease row. It reports
// whether the row had to be created, which after Open means the reaper
// had already treated the node as dead and deleted it.
const sqlTakeNodeLease = `
INSERT INTO presence_nodes (node_id, expires_at) VALUES ($1, now() + make_interval(secs => $2))
ON CONFLICT (node_id) DO UPDATE SET expires_at = EXCLUDED.expires_at
RETURNING (xmax = 0)`

// sqlDeadNodes lists the nodes whose members the reaper removes: every
// node id that owns a presence row (a skip scan over
// presence_node_idx, one index probe per distinct owner) without a live
// lease row, other than the fixture sentinel, plus every dead lease row,
// so a dead node with no members left still has its row deleted. A lease
// is dead once it has been expired for longer than $2 seconds
// (deadLeaseGrace). A node id with no lease row at all is dead too: a
// node takes its lease in Open before it writes any member (the rows of
// an earlier version's member-mode node, which has no lease row, are
// still kept while their own leases run: sqlReapNodeChunk).
const sqlDeadNodes = `
WITH RECURSIVE owners(node_id) AS (
  (SELECT node_id FROM presence ORDER BY node_id LIMIT 1)
  UNION ALL
  SELECT (SELECT p.node_id FROM presence p WHERE p.node_id > o.node_id ORDER BY p.node_id LIMIT 1)
  FROM owners o WHERE o.node_id IS NOT NULL
)
SELECT node_id FROM owners
WHERE node_id IS NOT NULL AND node_id <> $1
  AND NOT EXISTS (SELECT 1 FROM presence_nodes n WHERE n.node_id = owners.node_id
                  AND n.expires_at >= now() - make_interval(secs => $2))
UNION
SELECT node_id FROM presence_nodes WHERE expires_at < now() - make_interval(secs => $2)`

// sqlReapNodeChunk deletes up to $2 members of node $1, provided the
// node still has no live lease (one dead for longer than $3 seconds,
// deadLeaseGrace), and returns their keys. The lease
// is re-checked by every chunk, so a node that renews its lease part way
// through stops being reaped. Only rows with no lease of their own
// ('infinity', which every member row is written with) or a dead one are
// taken: a node of an earlier version that ran the retired member lease
// mode (DESIGN.md §9 "Removed settings") wrote rows with a lease of
// their own that it keeps ahead of now and no presence_nodes row, so the
// reaper must not take them while that lease runs. Once such a node is
// gone and its rows' leases are dead, they are reaped like any other.
// Rows a presence write holds locked are skipped, as in every lease
// statement, and reaped by a later chunk or round.
const sqlReapNodeChunk = `
DELETE FROM presence WHERE (channel, connection_id, client_id) IN (
  SELECT channel, connection_id, client_id FROM presence
  WHERE node_id = $1 AND (expires_at = 'infinity' OR expires_at < now() - make_interval(secs => $3))
    AND NOT EXISTS (SELECT 1 FROM presence_nodes n WHERE n.node_id = $1
                    AND n.expires_at >= now() - make_interval(secs => $3))
  LIMIT $2 FOR UPDATE SKIP LOCKED)
RETURNING channel, connection_id, client_id`

// sqlDropNodeLease deletes a dead node's lease row once it owns no
// member. The expiry is re-checked under the row lock, so a node that
// renewed its lease meanwhile keeps it.
const sqlDropNodeLease = `
DELETE FROM presence_nodes
WHERE node_id = $1 AND expires_at < now() - make_interval(secs => $2)
  AND NOT EXISTS (SELECT 1 FROM presence WHERE node_id = $1)`

// takeNodeLease creates or renews this node's lease row, reporting
// whether the row was missing.
func (s *Storage) takeNodeLease(ctx context.Context) (created bool, err error) {
	if hook := leaseBumpHook.Load(); hook != nil {
		if err := (*hook)(s.node); err != nil {
			return false, err
		}
	}
	err = s.pool.QueryRow(ctx, sqlTakeNodeLease, s.node, presenceLeaseWindow.Seconds()).Scan(&created)
	return created, err
}

// releaseNodeLease ends this node's lease, the last step of a graceful
// Close. The node's connections have left by then
// (DESIGN.md §11), so it normally owns no member and its lease row is
// deleted. A member it still owns (a delayed LEAVE the shutdown
// abandoned) keeps the row, marked expired instead, so the next reaper
// round on any node removes the member, publishes its LEAVE and then
// deletes the row.
func (s *Storage) releaseNodeLease() {
	ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM presence_nodes WHERE node_id = $1 AND NOT EXISTS (SELECT 1 FROM presence WHERE node_id = $1)`, s.node)
	if err == nil && tag.RowsAffected() == 0 {
		_, err = s.pool.Exec(ctx, `UPDATE presence_nodes SET expires_at = '-infinity' WHERE node_id = $1`, s.node)
	}
	if err != nil {
		s.logger.Warn("storage/postgres: presence lease release failed; the lease expires instead", "node", s.node, "err", err)
	}
}

// leaseRun tracks this node's own presence lease (DESIGN.md §12.5): the
// start of its last successful renewal and the start of the current
// unbroken run of renewals. The reaper consults it (reapable) so a node
// that has itself just lost and regained Postgres, as every node does
// after a failover longer than the lease window, does not treat the
// other live nodes, whose leases lapsed in the same outage, as dead.
type leaseRun struct {
	mu     sync.Mutex
	start  time.Time // first renewal of the current unbroken run; zero after a failed bump
	last   time.Time // start of the last successful renewal
	logged time.Time // when the reaper last logged a deferral
}

// begin starts a run at Open, which took the lease.
func (r *leaseRun) begin(at time.Time) {
	r.mu.Lock()
	r.start, r.last = at, at
	r.mu.Unlock()
}

// renewed records a successful renewal that started at at and returned
// at done. It reports whether the lease may have lapsed: the row had to
// be re-created (created), or more than a lease window passed between the
// previous renewal's start and this one's completion. The database
// stamps each renewal somewhere between its statement's start and its
// completion, so measuring from the earliest possible previous stamp to
// the latest possible new one never misses a lapse (a renewal that waited
// for a pool connection or a slow network is caught), and can only
// over-report one, which costs a redundant re-entry. A lapse, or a
// renewal after a failed one, starts a new run.
func (r *leaseRun) renewed(at, done time.Time, created bool) (lapsed bool, gap time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	gap = done.Sub(r.last)
	lapsed = created || gap > presenceLeaseWindow
	if lapsed || r.start.IsZero() {
		r.start = at
	}
	r.last = at
	return lapsed, gap
}

// broken records a failed renewal: the run ends, and the reaper waits a
// full window of renewals again before it acts.
func (r *leaseRun) broken() {
	r.mu.Lock()
	r.start = time.Time{}
	r.mu.Unlock()
}

// reapable reports whether this node may reap at now: its own lease is
// live and has been renewed without a break for at least one lease
// window. Otherwise it returns why not, and whether that is worth a log
// line (at most one per lease window).
func (r *leaseRun) reapable(now time.Time) (ok bool, why string, log bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.start.IsZero():
		why = "this node's last lease renewal failed"
	case now.Sub(r.last) >= presenceLeaseWindow:
		why = fmt.Sprintf("this node's own lease has not been renewed for %v", now.Sub(r.last).Round(time.Millisecond))
	case now.Sub(r.start) < presenceLeaseWindow:
		why = fmt.Sprintf("this node has held its lease without a break for %v, under one lease window", now.Sub(r.start).Round(time.Millisecond))
	default:
		return true, "", false
	}
	if now.Sub(r.logged) >= presenceLeaseWindow {
		r.logged = now
		log = true
	}
	return false, why, log
}

// leaseMetrics are the presence liveness series (DESIGN.md §10, §12.5).
type leaseMetrics struct {
	deferred prometheus.Counter
	lapses   prometheus.Counter
}

func newLeaseMetrics() *leaseMetrics {
	return &leaseMetrics{
		deferred: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_presence_reaps_deferred_total",
			Help: "Presence reaper rounds skipped because this node had not itself held its lease without a break for one lease window (DESIGN.md §12.5).",
		}),
		lapses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_presence_lease_lapses_total",
			Help: "Renewals that found this node's presence lease had lapsed (re-created, or not renewed for longer than the lease window), after which the node re-enters its members (DESIGN.md §12.5).",
		}),
	}
}

func (m *leaseMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.deferred, m.lapses}
}

// lapseNotifier turns lease lapses into calls of
// Options.OnPresenceLeaseLapse (DESIGN.md §12.5). A lapse schedules one
// call a bump interval later, and every lapse reported before that call
// starts joins it: the shards of a sharded node, which share one
// notifier and renew on their own ticks, detect one outage within a bump
// interval of each other, and a reaper statement that was already
// running when the lease came back has finished by then. A lapse
// reported while the call runs schedules another.
type lapseNotifier struct {
	hook   func(context.Context)
	logger *logging.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	pending bool
	stopped bool
}

// newLapseNotifier returns a notifier for hook, or nil when hook is nil.
func newLapseNotifier(hook func(context.Context), logger *logging.Logger) *lapseNotifier {
	if hook == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &lapseNotifier{hook: hook, logger: logger, ctx: ctx, cancel: cancel}
}

// lapsed reports one lapse. Safe on a nil notifier.
func (n *lapseNotifier) lapsed() {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pending || n.stopped {
		return
	}
	n.pending = true
	delay := presenceLeaseBumpInterval
	n.wg.Go(func() {
		if !sleepCtx(n.ctx, delay) {
			return
		}
		n.mu.Lock()
		n.pending = false
		n.mu.Unlock()
		n.logger.Info("storage/postgres: presence lease lapsed; re-entering this node's members")
		n.hook(n.ctx)
	})
}

// stop cancels a pending or running call and waits for it. Safe on a
// nil notifier.
func (n *lapseNotifier) stop() {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.stopped = true
	n.mu.Unlock()
	n.cancel()
	n.wg.Wait()
}

// presenceLeaseBumpLoop refreshes this node's presence liveness lease,
// its one presence_nodes row, on the bump cadence (DESIGN.md §12.5). A
// live node thus keeps its lease ahead of now,
// so only a dead node's members become reapable. The cadence does not
// depend on traffic: an idle node renews as often as a busy one.
func (s *Storage) presenceLeaseBumpLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceLeaseBumpInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.bumpLease(ctx)
		}
	}
}

// bumpLease renews the lease once and records the outcome in leaseRun. A
// renewal that finds the lease had lapsed (other nodes may have reaped
// this node's members and published their LEAVEs while it could not
// renew) is counted, logged and reported to the lapse notifier, whose
// hook re-enters the node's members.
func (s *Storage) bumpLease(ctx context.Context) {
	// The database stamps the lease between the statement's start and its
	// completion; leaseRun.renewed measures conservatively from both.
	start := time.Now()
	created, err := s.takeNodeLease(ctx)
	if err != nil {
		s.leaseRun.broken()
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence lease bump failed", "err", err)
		}
		return
	}
	lapsed, gap := s.leaseRun.renewed(start, time.Now(), created)
	if !lapsed {
		return
	}
	s.lmetrics.lapses.Inc()
	s.logger.Warn("storage/postgres: presence lease had lapsed and is renewed; members this node owned may have been reaped and will be re-entered",
		"node", s.node, "sinceLastRenewal", gap, "leaseWindow", presenceLeaseWindow, "rowRecreated", created)
	s.lapse.lapsed()
}

// presenceReaperLoop periodically removes the members of dead nodes
// (DESIGN.md §12.5). See reapDeadNodes. A round
// runs only while this node itself holds an unbroken lease of at least
// one window (leaseRun.reapable).
func (s *Storage) presenceReaperLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !s.mayReap() {
				continue
			}
			s.reapDeadNodes(ctx)
		}
	}
}

// mayReap applies the reaper guard (DESIGN.md §12.5), counting and
// (once per lease window) logging a deferred round.
func (s *Storage) mayReap() bool {
	ok, why, log := s.leaseRun.reapable(time.Now())
	if ok {
		return true
	}
	s.lmetrics.deferred.Inc()
	if log {
		s.logger.Info("storage/postgres: presence reaper deferred", "node", s.node, "reason", why, "leaseWindow", presenceLeaseWindow)
	}
	return false
}

// reapedMember is a presence row the reaper deleted, owed a LEAVE.
type reapedMember struct{ channel, connID, clientID string }

// reapDeadNodes is the reaper: for each node with no live
// lease it deletes all of the node's members in chunks, then publishes
// their LEAVEs and deletes the node's lease row (reapNode). When several
// nodes reap the same dead node at once, the row lock each DELETE takes
// means exactly one of them returns (and so publishes the LEAVE for) any
// given member.
func (s *Storage) reapDeadNodes(ctx context.Context) {
	s.reapNodes(ctx, sqlDeadNodes, fixtureNodeID, deadLeaseGrace())
}

// reapNodes reaps every node the query lists (reapNode).
func (s *Storage) reapNodes(ctx context.Context, sql string, args ...any) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence dead-node query failed", "err", err)
		}
		return
	}
	var dead []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			s.logger.Warn("storage/postgres: presence dead-node scan failed", "err", err)
			return
		}
		dead = append(dead, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence dead-node rows failed", "err", err)
		}
		return
	}
	for _, id := range dead {
		if ctx.Err() != nil {
			return
		}
		s.reapNode(ctx, id)
	}
}

// reapNode deletes dead node id's members chunk by chunk, then
// publishes their LEAVEs and drops the node's lease row once it owns no
// member. Every chunk is deleted before the first LEAVE is published, so
// the whole set leaves the presence table within one reaper round rather
// than at LEAVE-publish speed.
func (s *Storage) reapNode(ctx context.Context, id string) {
	var reaped []reapedMember
	for {
		members, err := s.deleteReturning(ctx, sqlReapNodeChunk, id, reapChunk, deadLeaseGrace())
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("storage/postgres: presence reap query failed", "node", id, "err", err)
			}
			// What the earlier chunks deleted is gone: publish its LEAVEs.
			s.publishReapedLeaves(ctx, reaped)
			return
		}
		if len(members) == 0 {
			break
		}
		reaped = append(reaped, members...)
	}
	s.publishReapedLeaves(ctx, reaped)
	if _, err := s.pool.Exec(ctx, sqlDropNodeLease, id, deadLeaseGrace()); err != nil && ctx.Err() == nil {
		s.logger.Warn("storage/postgres: presence lease drop failed", "node", id, "err", err)
	}
	if len(reaped) > 0 {
		s.logger.Info("storage/postgres: reaped the presence members of a dead node", "node", id, "members", len(reaped))
	}
}

// reapStatementTimeout bounds every reaper DELETE, server side: half a
// bump interval. A DELETE re-checks its node's lease as of the moment it
// started, so one that started just before the node renewed could
// otherwise run on and delete members the node re-entered (lapse re-entry
// starts a bump interval after the renewal, §12.5). A DELETE that times
// out deletes nothing; the next round tries again.
func reapStatementTimeout() time.Duration { return presenceLeaseBumpInterval / 2 }

// deleteReturning runs a reaper DELETE ... RETURNING channel,
// connection_id, client_id under reapStatementTimeout and collects the
// deleted keys. Rows are drained before any LEAVE publishes because
// those acquire their own pooled conn.
func (s *Storage) deleteReturning(ctx context.Context, sql string, args ...any) ([]reapedMember, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ms := max(reapStatementTimeout().Milliseconds(), 1)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", ms)); err != nil {
		return nil, err
	}
	out, err := collectReaped(ctx, tx, sql, args...)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// collectReaped runs a reaper DELETE in tx and collects the keys it
// returns.
func collectReaped(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]reapedMember, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reapedMember
	for rows.Next() {
		var m reapedMember
		if err := rows.Scan(&m.channel, &m.connID, &m.clientID); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// publishReapedLeaves synthesises the LEAVEs of reaped members through
// the normal publish path, so subscribers on every node observe the
// departures: one presence publish per channel, up to
// reapLeaveConcurrency channels at once.
func (s *Storage) publishReapedLeaves(ctx context.Context, members []reapedMember) {
	if len(members) == 0 {
		return
	}
	byChannel := make(map[string][]reapedMember)
	var order []string
	for _, m := range members {
		if _, ok := byChannel[m.channel]; !ok {
			order = append(order, m.channel)
		}
		byChannel[m.channel] = append(byChannel[m.channel], m)
	}
	sem := make(chan struct{}, reapLeaveConcurrency)
	var wg sync.WaitGroup
	for _, channel := range order {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			s.publishReapedLeave(ctx, channel, byChannel[channel])
		})
	}
	wg.Wait()
}

// publishReapedLeave publishes the LEAVEs of one channel's reaped
// members in one transaction, through a transient channelStore rather
// than the registered one: the registered store (if any) must keep its
// appender binding, and the transient one still mints a serial, inserts
// the LEAVE cm and announces it on the bus, reaching every node's
// appender. Each LEAVE matches the teardown-LEAVE shape (§12.5): action +
// connectionId + clientId, no id (a fresh publish, which must not collide
// with the ENTER's idempotency key).
//
// It is written in its own transaction, never batched, so that a member
// present again by the time the LEAVE holds the channel's row lock (its
// node's lease came back and the node re-entered it, §12.5) is left out:
// a LEAVE that committed after that ENTER would delete the live member
// and tell every subscriber it had gone.
func (s *Storage) publishReapedLeave(ctx context.Context, channel string, members []reapedMember) {
	cs := s.newChannelStore(channel, nil)
	leaves := make([]*protocol.PresenceMessage, 0, len(members))
	for _, m := range members {
		leaves = append(leaves, &protocol.PresenceMessage{
			Action:       protocol.PresenceLeave,
			ClientID:     m.clientID,
			ConnectionID: m.connID,
		})
	}
	cm, _, err := cs.storePresenceTx(storage.WithServerPresence(ctx), leaves, true)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap LEAVE failed", "channel", channel, "members", len(leaves), "err", err)
		}
		return
	}
	published := 0
	if cm != nil {
		published = len(cm.Presence)
	}
	s.logger.Debug("storage/postgres: reaped orphaned presence members", "channel", channel,
		"members", len(members), "leaves", published)
}
