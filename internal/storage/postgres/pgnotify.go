package postgres

// The pgnotify bus (DESIGN.md §7.2): the cluster broker as shipped
// before the bus seam, kept as the default and as the "before" picture
// for the scale runs. Every publish NOTIFYs the one global channel
// "ably_channel" inside its transaction; one LISTEN goroutine per node
// receives every notification, reads the cm back by (channel, serial)
// and delivers it. The mechanics below are main's, moved here from
// postgres.go, with four changes that leave the delivery mechanism as it
// was: the LISTEN connection strips pgxpool DSN settings (so a DSN with
// pool_max_conns no longer fails it); the reconnect backoff is copied at
// Open; a released channel (Storage.Release) is not delivered to; and
// the per-channel de-dup mark is seeded with the watermark at bind
// (Storage.Channel), and a notification that arrives while the bind is
// still reading the watermark is kept until then, so a cm the watermark
// already covers is dropped rather than appended (before this, such a
// cm could even reach the appender before Initialize), and a reconnect
// reconcile replays from the watermark rather than from the start of
// the log. The last is needed for a re-bind after Release to honour the
// Appender contract.

import (
	"context"
	"encoding/json"
	"fmt"

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

		cm, err := s.loadChannelMessage(ctx, p.Channel, p.Serial)
		if err != nil {
			s.stats.fetchErrors.Add(1)
			continue // best-effort; nothing we can do without the cm
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
		if err := cs.reconcileFromHistory(ctx); err != nil {
			b.s.logger.Warn("storage/postgres: reconcile failed", "channel", cs.name, "err", err)
		}
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
	if cs.released {
		return false
	}
	if !cs.seeded {
		// The bind has not read its watermark yet: keep the cm until seed
		// knows which side of the watermark it is on (flushPreSeedLocked).
		cs.preSeed = append(cs.preSeed, cm)
		return false
	}
	if cm.ChannelSerial <= cs.lastSeen {
		cs.st().duplicates.Add(1)
		return false
	}
	cs.lastSeen = cm.ChannelSerial
	cs.appender.Append(cm)
	return true
}

// flushPreSeedLocked delivers the cms the pgnotify bus received for the
// channel while its bind was reading the watermark: those above the
// watermark, in the order they arrived (NOTIFYs arrive in commit order),
// once each. Called by seed with hwmMu held.
func (cs *channelStore) flushPreSeedLocked() {
	held := cs.preSeed
	cs.preSeed = nil
	for _, cm := range held {
		if cm.ChannelSerial <= cs.lastSeen {
			cs.st().duplicates.Add(1)
			continue
		}
		cs.lastSeen = cm.ChannelSerial
		cs.st().fetched.Add(1)
		cs.appender.Append(cm)
	}
}

// reconcileFromHistory replays every cm minted after the channel's
// last-delivered serial — both message and presence kinds, merged in
// channelSerial order — through deliver (DESIGN.md §7.2). Called after
// a LISTEN reconnect to recover cms whose NOTIFY was lost in the gap.
func (cs *channelStore) reconcileFromHistory(ctx context.Context) error {
	cs.hwmMu.Lock()
	after := cs.lastSeen
	cs.hwmMu.Unlock()

	messages, err := cs.History(ctx, storage.HistoryQuery{
		Kind:               storage.KindMessage,
		Direction:          storage.DirectionForwards,
		AfterChannelSerial: after,
	})
	if err != nil {
		return fmt.Errorf("reconcile messages: %w", err)
	}
	presence, err := cs.History(ctx, storage.HistoryQuery{
		Kind:               storage.KindPresence,
		Direction:          storage.DirectionForwards,
		AfterChannelSerial: after,
	})
	if err != nil {
		return fmt.Errorf("reconcile presence: %w", err)
	}

	for _, cm := range mergeByChannelSerial(messages.ChannelMessages, presence.ChannelMessages) {
		if cs.deliver(cm) {
			cs.st().filled.Add(1)
		}
	}
	return nil
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
