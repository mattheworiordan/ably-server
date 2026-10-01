package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Presence lease modes accepted by Options.PresenceLeaseMode (DESIGN.md
// §12.5).
const (
	// PresenceLeaseNode keeps one liveness lease per node, a row in
	// presence_nodes that the node refreshes on the bump cadence. Member
	// rows record their owning node and an 'infinity' expires_at, and the
	// reaper removes the members of every node that has no unexpired
	// lease. The default.
	PresenceLeaseNode = "node"
	// PresenceLeaseMember keeps a lease on every member row, refreshed by
	// one UPDATE of all of the node's rows per bump: the behaviour before
	// node leases existed.
	PresenceLeaseMember = "member"
)

// ParsePresenceLeaseMode validates a presence lease mode. The empty
// string is the default, PresenceLeaseNode.
func ParsePresenceLeaseMode(s string) (string, error) {
	switch s {
	case "", PresenceLeaseNode:
		return PresenceLeaseNode, nil
	case PresenceLeaseMember:
		return PresenceLeaseMember, nil
	}
	return "", fmt.Errorf("unknown presence lease mode %q (valid: %s, %s)", s, PresenceLeaseNode, PresenceLeaseMember)
}

// reapChunk caps the members one reaper DELETE removes in node lease
// mode, so reaping a dead node with many members never holds that many
// row locks in one statement. A package var so tests can shrink it.
var reapChunk = 1000

// reapLeaveConcurrency bounds the synthesised LEAVEs the reaper has in
// flight at once when presence writes are batched, so a dead node's
// LEAVEs share batch commits instead of committing one after another.
const reapLeaveConcurrency = 32

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

// sqlDeadNodes lists the nodes whose members the node-mode reaper
// removes: every node id that owns a presence row (a skip scan over
// presence_node_idx, one index probe per distinct owner) without an
// unexpired lease row, other than the fixture sentinel, plus every
// expired lease row, so a dead node with no members left still has its
// row deleted. A node id with no lease row at all is dead too: a node
// takes its lease in Open before it writes any member.
const sqlDeadNodes = `
WITH RECURSIVE owners(node_id) AS (
  (SELECT node_id FROM presence ORDER BY node_id LIMIT 1)
  UNION ALL
  SELECT (SELECT p.node_id FROM presence p WHERE p.node_id > o.node_id ORDER BY p.node_id LIMIT 1)
  FROM owners o WHERE o.node_id IS NOT NULL
)
SELECT node_id FROM owners
WHERE node_id IS NOT NULL AND node_id <> $1
  AND NOT EXISTS (SELECT 1 FROM presence_nodes n WHERE n.node_id = owners.node_id AND n.expires_at >= now())
UNION
SELECT node_id FROM presence_nodes WHERE expires_at < now()`

// sqlReapNodeChunk deletes up to $2 members of node $1, provided the
// node still has no unexpired lease, and returns their keys. The lease
// is re-checked by every chunk, so a node that renews its lease part way
// through stops being reaped. Only rows with no lease of their own
// ('infinity', written in node mode) or an expired one are taken: a
// member-mode node's rows carry a lease it keeps ahead of now and no
// presence_nodes row, so a node-mode reaper must not take them while
// that lease runs. Rows a presence write holds locked are skipped, as
// in every lease statement, and reaped by a later chunk or round.
const sqlReapNodeChunk = `
DELETE FROM presence WHERE (channel, connection_id, client_id) IN (
  SELECT channel, connection_id, client_id FROM presence
  WHERE node_id = $1 AND (expires_at = 'infinity' OR expires_at < now())
    AND NOT EXISTS (SELECT 1 FROM presence_nodes n WHERE n.node_id = $1 AND n.expires_at >= now())
  LIMIT $2 FOR UPDATE SKIP LOCKED)
RETURNING channel, connection_id, client_id`

// sqlDropNodeLease deletes a dead node's lease row once it owns no
// member. The expiry is re-checked under the row lock, so a node that
// renewed its lease meanwhile keeps it.
const sqlDropNodeLease = `
DELETE FROM presence_nodes
WHERE node_id = $1 AND expires_at < now() AND NOT EXISTS (SELECT 1 FROM presence WHERE node_id = $1)`

// sqlBumpMemberLeases is the member-mode lease bump: every row this node
// owns gets a fresh expires_at.
const sqlBumpMemberLeases = `
UPDATE presence SET expires_at = now() + make_interval(secs => $2)
WHERE (channel, connection_id, client_id) IN (
  SELECT channel, connection_id, client_id FROM presence WHERE node_id = $1 FOR UPDATE SKIP LOCKED)`

// sqlReapMemberLeases is the member-mode reaper: every row whose own
// lease has expired.
const sqlReapMemberLeases = `
DELETE FROM presence WHERE (channel, connection_id, client_id) IN (
  SELECT channel, connection_id, client_id FROM presence WHERE expires_at < now() FOR UPDATE SKIP LOCKED)
RETURNING channel, connection_id, client_id`

// sqlExpiredNodeLeases lists the node-mode nodes whose lease row has
// expired. The member-mode reaper reaps their members too, so switching
// a cluster back to member mode does not strand the 'infinity' rows of
// node-mode nodes that died (or closed with members still owned).
const sqlExpiredNodeLeases = `SELECT node_id FROM presence_nodes WHERE expires_at < now()`

// takeNodeLease creates or renews this node's lease row (node lease
// mode), reporting whether the row was missing.
func (s *Storage) takeNodeLease(ctx context.Context) (created bool, err error) {
	err = s.pool.QueryRow(ctx, sqlTakeNodeLease, s.node, presenceLeaseWindow.Seconds()).Scan(&created)
	return created, err
}

// releaseNodeLease ends this node's lease, the last step of a graceful
// Close in node lease mode. The node's connections have left by then
// (DESIGN.md §11), so it normally owns no member and its lease row is
// deleted. A member it still owns (a delayed LEAVE the shutdown
// abandoned) keeps the row, marked expired instead, so the next reaper
// round on any node, in either lease mode, removes the member, publishes
// its LEAVE and then deletes the row.
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

// presenceLeaseBumpLoop refreshes this node's presence liveness lease on
// the bump cadence (DESIGN.md §12.5): in node lease mode its one
// presence_nodes row, in member lease mode the expires_at of every
// presence row it owns. A live node thus keeps its lease ahead of now,
// so only a dead node's members become reapable.
func (s *Storage) presenceLeaseBumpLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceLeaseBumpInterval)
	defer t.Stop()
	last := time.Now() // node mode: Open took the lease
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !s.leaseNode {
				s.bumpMemberLeases(ctx)
				continue
			}
			// The database stamps the lease from the statement's start, so
			// measure the gap from there too.
			start := time.Now()
			created, err := s.takeNodeLease(ctx)
			if err != nil {
				if ctx.Err() == nil {
					s.logger.Warn("storage/postgres: presence lease bump failed", "err", err)
				}
				continue
			}
			if gap := time.Since(last); created || gap > presenceLeaseWindow {
				// The lease lapsed: other nodes may have reaped this
				// node's members and published their LEAVEs while it was
				// stalled.
				s.logger.Warn("storage/postgres: presence lease had lapsed and is renewed; members this node owned may have been reaped",
					"node", s.node, "sinceLastRenewal", gap, "leaseWindow", presenceLeaseWindow, "rowRecreated", created)
			}
			last = start
		}
	}
}

// bumpMemberLeases is the member-mode bump: one UPDATE of every row
// this node owns.
//
// Rows another transaction holds locked are skipped rather than waited
// for: that transaction is a presence write, which stamps a fresh lease
// or deletes the row, or the reaper. A batched presence write locks
// several rows in one transaction, so a bump that waited could deadlock
// with it; a row skipped once is bumped next round, well inside the
// lease window.
func (s *Storage) bumpMemberLeases(ctx context.Context) {
	if _, err := s.pool.Exec(ctx, sqlBumpMemberLeases, s.node, presenceLeaseWindow.Seconds()); err != nil && ctx.Err() == nil {
		s.logger.Warn("storage/postgres: presence lease bump failed", "err", err)
	}
}

// presenceReaperLoop periodically removes the members of dead nodes
// (DESIGN.md §12.5). See reapDeadNodes and reapExpiredMembers.
func (s *Storage) presenceReaperLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(presenceReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.leaseNode {
				s.reapDeadNodes(ctx)
			} else {
				s.reapExpiredMembers(ctx)
			}
		}
	}
}

// reapedMember is a presence row the reaper deleted, owed a LEAVE.
type reapedMember struct{ channel, connID, clientID string }

// reapDeadNodes is the node-mode reaper: for each node with no
// unexpired lease it deletes all of the node's members in chunks, then
// publishes their LEAVEs and deletes the node's lease row (reapNode). When several nodes reap the same dead node at
// once, the row lock each DELETE takes means exactly one of them returns
// (and so publishes the LEAVE for) any given member.
func (s *Storage) reapDeadNodes(ctx context.Context) {
	s.reapNodes(ctx, sqlDeadNodes, fixtureNodeID)
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
// the whole set leaves the presence table within one reaper round, as a
// member-mode reap does, rather than at LEAVE-publish speed.
func (s *Storage) reapNode(ctx context.Context, id string) {
	var reaped []reapedMember
	for {
		members, err := s.deleteReturning(ctx, sqlReapNodeChunk, id, reapChunk)
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
	if _, err := s.pool.Exec(ctx, sqlDropNodeLease, id); err != nil && ctx.Err() == nil {
		s.logger.Warn("storage/postgres: presence lease drop failed", "node", id, "err", err)
	}
	if len(reaped) > 0 {
		s.logger.Info("storage/postgres: reaped the presence members of a dead node", "node", id, "members", len(reaped))
	}
}

// reapExpiredMembers is the member-mode reaper: it deletes every row
// past its own lease in one statement and synthesises a LEAVE for each.
// Rows another transaction holds locked are skipped (a presence write is
// renewing or removing them), as in the lease bump, so the reaper cannot
// deadlock with a batched presence write; a row still lapsed is reaped
// next round. It then reaps the members of node-mode nodes whose lease
// row has expired (none in a cluster that only ever ran member mode).
func (s *Storage) reapExpiredMembers(ctx context.Context) {
	members, err := s.deleteReturning(ctx, sqlReapMemberLeases)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap query failed", "err", err)
		}
		return
	}
	s.publishReapedLeaves(ctx, members)
	s.reapNodes(ctx, sqlExpiredNodeLeases)
}

// deleteReturning runs a reaper DELETE ... RETURNING channel,
// connection_id, client_id and collects the deleted keys. Rows are
// drained before any LEAVE publishes because StorePresence acquires its
// own pooled conn.
func (s *Storage) deleteReturning(ctx context.Context, sql string, args ...any) ([]reapedMember, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
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

// publishReapedLeaves synthesises a LEAVE for each reaped member through
// the normal publish path, so subscribers on every node observe the
// departure. With presence batched they run up to reapLeaveConcurrency
// at once and share batch commits; otherwise one at a time, since each
// would hold a pool connection for its whole transaction.
func (s *Storage) publishReapedLeaves(ctx context.Context, members []reapedMember) {
	if s.presenceLanes == nil || len(members) <= 1 {
		for _, m := range members {
			s.publishReapedLeave(ctx, m)
		}
		return
	}
	sem := make(chan struct{}, reapLeaveConcurrency)
	var wg sync.WaitGroup
	for _, m := range members {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.publishReapedLeave(ctx, m)
		}()
	}
	wg.Wait()
}

// publishReapedLeave publishes one reaped member's LEAVE through a
// transient channelStore rather than the registered one: the registered
// store (if any) must keep its appender binding, and the transient one
// still mints a serial, inserts the LEAVE cm and announces it on the
// bus, reaching every node's appender. It matches the teardown-LEAVE
// shape (§12.5): action + connectionId + clientId, no id (a fresh
// publish, which must not collide with the ENTER's idempotency key).
func (s *Storage) publishReapedLeave(ctx context.Context, m reapedMember) {
	cs := s.newChannelStore(m.channel, nil)
	leave := &protocol.PresenceMessage{
		Action:       protocol.PresenceLeave,
		ClientID:     m.clientID,
		ConnectionID: m.connID,
	}
	if _, _, err := cs.StorePresence(storage.WithServerPresence(ctx), []*protocol.PresenceMessage{leave}); err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("storage/postgres: presence reap LEAVE failed", "channel", m.channel, "err", err)
		}
		return
	}
	s.logger.Debug("storage/postgres: reaped orphaned presence member", "channel", m.channel, "clientId", m.clientID, "connectionId", m.connID)
}
