package postgres

// The pgnotify bus (DESIGN.md §7.2): the cluster broker as shipped
// before the bus seam, kept as an explicit choice (--bus=pgnotify; the
// server never infers it) and as the "before" picture for the scale
// runs. Every publish NOTIFYs the one global channel
// "ably_channel" inside its transaction; one LISTEN goroutine per node
// receives every notification, reads the cm back by (channel, serial)
// and delivers it. The mechanics below are main's, moved here from
// postgres.go, with four changes that leave the delivery mechanism as it
// was: the LISTEN connection strips pgxpool DSN settings (so a DSN with
// pool_max_conns no longer fails it); the reconnect backoff is copied at
// Open; a released channel (Storage.Release) is not delivered to; and
// the bind fix WS3 made on scale/channel-lifecycle (582331f): the
// per-channel de-dup mark is seeded with the watermark at bind, a
// notification that arrives while the bind is still reading the
// watermark is held until then (before, it could reach the appender
// before Initialize), and a reconcile a LISTEN reconnect requests
// during a bind is run from the watermark and merged with the held
// notifications before the channel goes live (before, it replayed the
// whole log or could drop messages). A re-bind after Release needs the
// last two to honour the Appender contract. A reconcile from a mark
// below the retention floor also checks that the log still proves its
// continuity and, when it cannot, tells the appender (readMissed,
// DESIGN.md §7.2), as the chaining buses do.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// notifyChannelName is the LISTEN channel used by the pgnotify bus —
// distinct concept from an Ably channel.
const notifyChannelName = "ably_channel"

// notifyPayload is the JSON-encoded pgnotify NOTIFY body. Keeping it
// JSON avoids ambiguity in the face of Ably channel names that contain
// arbitrary characters (including ':' and '@').
type notifyPayload struct {
	Channel string `json:"channel"`
	Serial  string `json:"serial"`
}

// dialAndListen opens a fresh raw LISTEN connection and issues the
// broker LISTEN on it. Used both for the initial conn in Open and for
// every post-drop re-dial in the LISTEN goroutine.
func dialAndListen(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := dialListenConn(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `LISTEN `+pgx.Identifier{notifyChannelName}.Sanitize()); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("storage/postgres: LISTEN: %w", err)
	}
	return conn, nil
}

// listenLoop dispatches NOTIFY events to the registered channelStore
// for each channel, surviving dropped LISTEN connections (DESIGN.md
// §7.2). It runs consume() on the current conn until a
// WaitForNotification error; unless that error is Close() cancelling
// the context, it re-dials a fresh conn with capped-exponential
// backoff, re-LISTENs, reconciles each channel's missed cms from
// history, and resumes. It exits only when the storage is Close()d.
//
// A NOTIFY for an unregistered channel is dropped: local attachments
// materialise the channelStore on demand via Storage.Channel, so
// events that arrive before any local interest are intentionally lost
// (the canonical cm is still in storage and picked up by a subsequent
// ATTACH+resume via History).
func (b *pgNotifyBus) listenLoop(ctx context.Context) {
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
		// Re-LISTEN is already done by redial; reconcile before resuming
		// so any cm minted during the gap is replayed exactly once
		// (deliver() dedups against a subsequent buffered NOTIFY).
		b.reconcile(ctx)
		b.s.logger.Info("storage/postgres: LISTEN reconnected and reconciled")
	}
}

// consume runs the steady-state WaitForNotification dispatch on conn,
// returning the error that ended it (a dropped conn, or ctx
// cancellation on Close).
func (b *pgNotifyBus) consume(ctx context.Context, conn *pgx.Conn) error {
	s := b.s
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		s.stats.received.Add(1)

		var p notifyPayload
		if err := json.Unmarshal([]byte(n.Payload), &p); err != nil {
			s.stats.malformed.Add(1)
			continue // malformed; nothing actionable
		}

		s.mu.RLock()
		cs, ok := s.channels[p.Channel]
		s.mu.RUnlock()
		if !ok || cs.appender == nil {
			s.stats.unrouted.Add(1)
			continue
		}

		// A channel whose earlier read-back failed is repaired by a range
		// read from its high-water mark, never by delivering this cm
		// alone: that would advance the mark past the missed one and lose
		// it for good (claims audit, "a failed steady-state read-back can
		// silently lose a delivery").
		if cs.isDirty() {
			b.repair(ctx, cs)
			continue
		}
		cm, err := s.loadChannelMessage(ctx, p.Channel, p.Serial)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // Close() interrupted the read, not a fault
			}
			s.stats.fetchErrors.Add(1)
			s.logger.Warn("storage/postgres: read-back failed; repairing from history", "channel", p.Channel, "serial", p.Serial, "err", err)
			cs.setDirty(true)
			b.repair(ctx, cs)
			continue
		}
		if cs.deliver(cm) {
			s.stats.fetched.Add(1)
		}
	}
}

// redial re-establishes the LISTEN connection with capped exponential
// backoff, retrying until it succeeds or ctx is cancelled (Close). A
// nil return means ctx was cancelled.
func (b *pgNotifyBus) redial(ctx context.Context) *pgx.Conn {
	s := b.s
	delay := s.reconnectBase
	for {
		if !sleepCtx(ctx, delay) {
			return nil
		}

		conn, err := dialAndListen(ctx, s.dsn)
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

// reconcile replays, per registered channel, every cm minted past the
// channel's last-delivered serial — the cms whose NOTIFY was lost while
// the LISTEN conn was down (DESIGN.md §7.2). Each is delivered through
// cs.deliver, whose high-water mark makes replay idempotent against the
// normal NOTIFY path.
func (b *pgNotifyBus) reconcile(ctx context.Context) {
	stores := b.s.boundStores()
	for _, cs := range stores {
		b.repair(ctx, cs)
	}
	b.s.stats.reconcileRuns.Add(1)
	b.s.stats.reconciles.Add(uint64(len(stores)))
}

func (b *pgNotifyBus) isConnected() bool { return b.connected.Load() }

// deliver hands cm to the appender exactly once and in order, advancing
// the per-channel high-water mark, and reports whether it did. A cm
// whose serial is not strictly greater than the last delivered serial
// is dropped — the case where a reconnect's history replay and a
// subsequently-buffered NOTIFY both carry it. All appends (steady-state
// NOTIFY dispatch and reconcile) funnel through here, from the single
// LISTEN goroutine. hwmMu is held across the append so Release cannot
// return while an append to the old appender is in flight.
func (cs *channelStore) deliver(cm *protocol.ChannelMessage) bool {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return cs.deliverLocked(cm)
}

// deliverLocked is deliver with hwmMu held.
func (cs *channelStore) deliverLocked(cm *protocol.ChannelMessage) bool {
	if cs.released {
		return false
	}
	if !cs.seeded {
		// The bind has not read its watermark yet: keep the cm until
		// initialize knows which side of the watermark it is on.
		cs.preSeed = append(cs.preSeed, cm)
		return false
	}
	if cm.ChannelSerial <= cs.lastSeen {
		cs.st().duplicates.Add(1)
		return false
	}
	cs.lastSeen = cm.ChannelSerial
	cs.appendTimed(cm)
	cs.st().observeLag(lagFetched, 0, cm)
	return true
}

// initialize finishes a pgnotify bind (after WS3's fix on
// scale/channel-lifecycle): it hands the watermark to the appender,
// seeds the high-water mark with it, and delivers what arrived while
// the store was being bound, before deliver may reach the appender
// directly:
//
//   - cms deliver held in preSeed (a NOTIFY between the dispatch-map
//     insert and the watermark read), and
//   - if a LISTEN reconnect asked for a reconcile while the store was
//     unseeded, every cm committed after the watermark, read from the
//     log (their NOTIFYs may have been lost in the drop).
//
// Both sets are merged in serial order and filtered against the mark
// under hwmMu. Per channel, serial order is commit order, so a log read
// holds every held cm at or below its newest serial, and nothing is
// skipped. The store is marked seeded only once no reconcile request is
// outstanding; until then deliver keeps buffering, so no NOTIFY can
// advance the mark past a cm the reconcile is about to deliver. A
// log-read error is returned after the store is seeded anyway (a later
// reconnect reconciles again). A store released while binding never
// Initializes its appender.
func (cs *channelStore) initialize(ctx context.Context, current, initial string) error {
	cs.hwmMu.Lock()
	if cs.released {
		cs.hwmMu.Unlock()
		return nil
	}
	cs.appender.Initialize(current, initial)
	if current > cs.lastSeen {
		cs.lastSeen = current
	}
	for {
		if cs.released {
			cs.preSeed = nil
			cs.hwmMu.Unlock()
			return nil
		}
		if !cs.needsReconcile {
			cs.deliverSortedLocked(cs.preSeed)
			cs.preSeed = nil
			cs.seeded = true
			cs.hwmMu.Unlock()
			return nil
		}
		cs.needsReconcile = false
		after := cs.lastSeen
		check := cs.mustProveLocked(after)
		cs.hwmMu.Unlock()

		missed, err := cs.readMissed(ctx, after, check)

		cs.hwmMu.Lock()
		if err != nil {
			cs.deliverSortedLocked(cs.preSeed)
			cs.preSeed = nil
			cs.seeded = true
			cs.hwmMu.Unlock()
			return err
		}
		signalled := cs.signalUnprovenLocked(missed)
		cs.deliverSortedLocked(append(missed.cms, cs.preSeed...))
		cs.preSeed = nil
		cs.passExpiredLocked(missed, signalled)
	}
}

// missedRead is a pgnotify reconcile's read of the cms after a mark:
// the cms, and, when the mark is below the retention floor, whether the
// read could not prove it saw every cm after the mark (rangeRead
// documents unproven and current; here current is read before the cms
// and the cm at the mark is looked for after them).
type missedRead struct {
	after    string
	cms      []*protocol.ChannelMessage
	unproven bool
	current  string
}

// readMissed reads every cm committed after the mark (loadAfter) and,
// when check is set (the mark is below the retention floor,
// mustProveLocked, DESIGN.md §7.2), checks the read's continuity: the
// channel's serial before the read (nothing to prove if it has not moved
// past the mark), and, after it, whether the cm at the mark is still in
// the log. Partitions are dropped oldest first, so a cm at the mark
// still held after the read means nothing after it had aged out when
// the read ran. The check costs two point reads, only for a channel
// whose mark is that old.
func (cs *channelStore) readMissed(ctx context.Context, after string, check bool) (missedRead, error) {
	r := missedRead{after: after}
	if check {
		current, err := cs.currentSerial(ctx)
		if err != nil {
			return r, err
		}
		// Nothing to prove if the channel has not moved past the mark; a
		// channel with no row (pruned after idling past retention) is
		// unprovable, as in unprovenRead.
		r.current, check = current, current == "" || current > after
	}
	cms, err := cs.loadAfter(ctx, after)
	if err != nil {
		return r, err
	}
	r.cms = cms
	if check {
		held, err := cs.serialInLog(ctx, after)
		if err != nil {
			return r, err
		}
		r.unproven = !held
	}
	return r, nil
}

// currentSerial reads the channel's current serial ("" for a channel
// with no row).
func (cs *channelStore) currentSerial(ctx context.Context) (string, error) {
	var current string
	err := cs.pool.QueryRow(ctx, `SELECT channel_serial FROM channels WHERE name = $1`, cs.name).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("storage/postgres: read channel serial %s: %w", cs.name, err)
	}
	return current, nil
}

// serialInLog reports whether the log still holds the cm at serial.
func (cs *channelStore) serialInLog(ctx context.Context, serial string) (bool, error) {
	var held bool
	err := cs.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM channel_messages WHERE channel = $1 AND channel_serial = $2)`, cs.name, serial).Scan(&held)
	if err != nil {
		return false, fmt.Errorf("storage/postgres: look up %s %s: %w", cs.name, serial, err)
	}
	return held, nil
}

// signalUnprovenLocked tells the appender of a discontinuity before the
// cms of a read that could not prove its continuity, unless the mark
// has moved since the read was taken (then a delivery, which on this
// bus comes from the same log, already passed the mark). It reports
// whether it did. Called with hwmMu held.
func (cs *channelStore) signalUnprovenLocked(r missedRead) bool {
	if !r.unproven || cs.lastSeen != r.after {
		return false
	}
	cs.signalDiscontinuityLocked(storage.DiscontinuityRetention)
	return true
}

// passExpiredLocked moves the mark, after a signalled discontinuity, to
// the channel's serial as of the read: every cm at or below it was
// committed before the read, so it was delivered or has aged out, and
// the next reconcile does not look for the expired ones again. Called
// with hwmMu held.
func (cs *channelStore) passExpiredLocked(r missedRead, signalled bool) {
	if signalled && r.current > cs.lastSeen {
		cs.lastSeen = r.current
	}
}

// deliverSortedLocked delivers cms in serial order through the mark.
// Called with hwmMu held on an Initialized store.
func (cs *channelStore) deliverSortedLocked(cms []*protocol.ChannelMessage) {
	sort.Slice(cms, func(i, j int) bool { return cms[i].ChannelSerial < cms[j].ChannelSerial })
	for _, cm := range cms {
		if cm.ChannelSerial <= cs.lastSeen {
			cs.st().duplicates.Add(1)
			continue
		}
		cs.lastSeen = cm.ChannelSerial
		cs.st().fetched.Add(1)
		cs.appendTimed(cm)
		cs.st().observeLag(lagFetched, 0, cm)
	}
}

// reconcileFromHistory replays every cm minted after the channel's
// last-delivered serial — both message and presence kinds, merged in
// channelSerial order — through deliver (DESIGN.md §7.2). Called after
// a LISTEN reconnect to recover cms whose NOTIFY was lost in the gap. A
// store still binding has no watermark yet: its initialize runs the
// reconcile once it has one, so this does not replay the whole history.
func (cs *channelStore) reconcileFromHistory(ctx context.Context) error {
	cs.hwmMu.Lock()
	if cs.released {
		cs.hwmMu.Unlock()
		return nil
	}
	if !cs.seeded {
		cs.needsReconcile = true
		cs.hwmMu.Unlock()
		return nil
	}
	after := cs.lastSeen
	check := cs.mustProveLocked(after)
	cs.hwmMu.Unlock()

	missed, err := cs.readMissed(ctx, after, check)
	if err != nil {
		return err
	}
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if cs.released {
		return nil
	}
	signalled := cs.signalUnprovenLocked(missed)
	for _, cm := range missed.cms {
		if cs.deliverLocked(cm) {
			cs.st().filled.Add(1)
		}
	}
	cs.passExpiredLocked(missed, signalled)
	return nil
}

// loadAfter reads every cm committed after the given serial from the
// log; loadAfterFn replaces it in unit tests.
func (cs *channelStore) loadAfter(ctx context.Context, after string) ([]*protocol.ChannelMessage, error) {
	if cs.loadAfterFn != nil {
		return cs.loadAfterFn(ctx, after)
	}
	return cs.missedAfter(ctx, after)
}

// missedAfter reads the cms of every kind (messages, presence and
// annotations) committed after the given serial, in channelSerial order.
// Annotations were once left out, so an annotation whose NOTIFY was lost
// in a LISTEN drop never reached the node.
func (cs *channelStore) missedAfter(ctx context.Context, after string) ([]*protocol.ChannelMessage, error) {
	var all []*protocol.ChannelMessage
	for _, kind := range []storage.Kind{storage.KindMessage, storage.KindPresence, storage.KindAnnotation} {
		page, err := cs.History(ctx, storage.HistoryQuery{
			Kind:               kind,
			Direction:          storage.DirectionForwards,
			AfterChannelSerial: after,
		})
		if err != nil {
			return nil, fmt.Errorf("reconcile %s: %w", kind, err)
		}
		all = mergeByChannelSerial(all, page.ChannelMessages)
	}
	return all, nil
}

// mergeByChannelSerial merges two channelSerial-ascending cm slices
// (the message and presence streams share one channelSerial namespace
// but never collide on a serial) into a single ascending slice.
func mergeByChannelSerial(a, b []*protocol.ChannelMessage) []*protocol.ChannelMessage {
	out := make([]*protocol.ChannelMessage, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].ChannelSerial <= b[j].ChannelSerial {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// repair replays cs's log from its high-water mark and clears the dirty
// flag on success. On failure the channel is marked dirty (a reconnect
// reconcile may not have marked it), so its next notification repairs
// from the mark instead of delivering that cm alone, which would move
// the mark past the cms the failed read did not deliver; the next
// notification or reconnect retries.
func (b *pgNotifyBus) repair(ctx context.Context, cs *channelStore) {
	if err := cs.reconcileFromHistory(ctx); err != nil {
		cs.setDirty(true)
		if ctx.Err() == nil {
			b.s.logger.Warn("storage/postgres: repair from history failed; will retry", "channel", cs.name, "err", err)
		}
		return
	}
	cs.setDirty(false)
}

// isDirty and setDirty guard the pgnotify repair flag: set when a
// read-back failed, cleared once a range read from the mark succeeded.
func (cs *channelStore) isDirty() bool {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return cs.dirty
}

func (cs *channelStore) setDirty(d bool) {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	cs.dirty = d
}
