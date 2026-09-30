package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

// rollbackTimeout bounds the ROLLBACK sent after a failed batch.
const rollbackTimeout = 5 * time.Second

// validText reports whether s can be stored in a Postgres TEXT column:
// valid UTF-8 with no NUL byte. Postgres rejects anything else, which in
// a batch would fail every publish in it.
func validText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// storeBatched validates a publish, queues it on its channel's lane and
// waits for the batch that commits it (DESIGN.md §6.3). Everything that
// can reject one publish on its own (the id format, text Postgres would
// refuse) is checked here, before it is queued, so one bad publish cannot
// fail a batch. The channel's row is created here if this store has
// never seen it, so a batch never has to insert one.
func (cs *channelStore) storeBatched(ctx context.Context, lanes *laneSet, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if !validText(cs.name) {
		return nil, false, fmt.Errorf("storage/postgres: channel name is not valid UTF-8 text")
	}
	clientIDs := len(nonEmptyIDs(msgs)) > 0
	for _, m := range msgs {
		if !validText(m.ID) {
			return nil, false, storage.ErrInvalidMessageID
		}
	}
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}
	if !cs.rowEnsured.Load() {
		if _, err := cs.pool.Exec(ctx, `SELECT ensure_channel($1, $2)`, cs.name, cs.series); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: ensure_channel: %w", err)
		}
		cs.rowEnsured.Store(true)
	}
	p := newPending(ctx, cs.name)
	p.cs, p.msgs, p.batchID, p.checkIDs = cs, msgs, batchID, clientIDs
	return lanes.publish(p)
}

// batchSlot is one publish's place in a batch transaction.
type batchSlot struct {
	p     *pending
	dupOf int // index of an earlier slot of the same channel sharing an id, or -1
	ord   int // 1-based position among the slots sent to publish_batch_lock, or 0

	status    string // from publish_batch_lock: ok, deferred, duplicate
	serial    string
	prev      string // the channel's serial before this cm (earlier cms of the batch included)
	dupSerial string

	cm *protocol.ChannelMessage // an ok slot's cm
}

// commitBatch implements committer: it commits a lane's batch of message
// publishes as one transaction in two round trips (DESIGN.md §6.3).
//
//  1. BEGIN and publish_batch_lock: lock every channel's row (sorted,
//     skipping rows locked elsewhere), check ids, and mint one
//     channelSerial per publish in batch order, each with its
//     predecessor.
//  2. The rows, as one multi-row INSERT per table, the bus's
//     in-transaction hook (queued by beforeCommitBatch), and COMMIT.
//
// The serials must be known before the rows can be encoded (each stored
// Message carries its own serial), hence two round trips rather than one.
// After the commit the bus's afterCommit runs for each cm in batch
// order, so a chaining bus sees each channel's cms in serial order with
// their predecessors, exactly as for single publishes.
func (s *Storage) commitBatch(ctx context.Context, batch []*pending) ([]*pending, error) {
	if hook := commitBatchHook.Load(); hook != nil {
		if err := (*hook)(); err != nil {
			return nil, err
		}
	}
	slots := planBatch(batch)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: acquire for batch: %w", err)
	}
	items, err := s.commitBatchTx(ctx, conn, slots)
	conn.Release()
	if err != nil {
		return nil, err
	}
	if hook := commitBatchAfterHook.Load(); hook != nil {
		// Test hook: the COMMIT went through but its reply is treated as
		// lost, so nothing after it runs, as in a real network failure.
		if err := (*hook)(); err != nil {
			return nil, err
		}
	}

	for _, it := range items {
		if it.w.notified {
			s.stats.published.Add(1)
		}
		if it.w.pointer {
			s.stats.pointers.Add(1)
		}
		s.bus.afterCommit(it.cs, it.cm, it.w.prev)
	}

	// Results. A publish the database recognised as a duplicate returns
	// the original cm, read now that the batch's connection is back in
	// the pool; one that repeats an id of an earlier publish in this
	// batch shares that publish's result.
	var deferred []*pending
	for i := range slots {
		sl := &slots[i]
		primary := sl
		if sl.dupOf >= 0 {
			primary = &slots[sl.dupOf]
		}
		switch primary.status {
		case "deferred":
			deferred = append(deferred, sl.p)
		case "duplicate":
			if sl.dupOf >= 0 {
				continue // filled from its primary below
			}
			original, err := s.loadChannelMessage(ctx, sl.p.channel, sl.dupSerial)
			sl.p.res = pendingResult{cm: original, idempotent: err == nil, err: err}
		case "ok":
			sl.p.res = pendingResult{cm: primary.cm, idempotent: sl.dupOf >= 0}
		default:
			sl.p.res = pendingResult{err: fmt.Errorf("storage/postgres: publish_batch_lock returned no row for %s", sl.p.channel)}
		}
	}
	for i := range slots {
		sl := &slots[i]
		if sl.dupOf >= 0 && slots[sl.dupOf].status == "duplicate" {
			sl.p.res = slots[sl.dupOf].p.res
		}
	}
	return deferred, nil
}

// commitBatchTx runs the batch's transaction on conn and returns the bus
// items of the cms it committed, in batch order. On any error the
// transaction is rolled back (or the connection closed if that fails).
func (s *Storage) commitBatchTx(ctx context.Context, conn *pgxpool.Conn, slots []batchSlot) ([]batchItem, error) {
	var (
		channels []string
		wait     []bool
		floors   []string
		idCM     []int32
		ids      []string
	)
	byOrd := make(map[int]*batchSlot, len(slots))
	for i := range slots {
		sl := &slots[i]
		if sl.dupOf >= 0 {
			continue
		}
		channels = append(channels, sl.p.channel)
		wait = append(wait, sl.p.deferrals >= deferralsBeforeWait)
		floors = append(floors, sl.p.cs.idempotencyFloor())
		sl.ord = len(channels)
		byOrd[sl.ord] = sl
		if sl.p.checkIDs || sl.p.retried {
			for _, m := range sl.p.msgs {
				idCM = append(idCM, int32(sl.ord))
				ids = append(ids, m.ID)
			}
		}
	}

	rollback := func() {
		rctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer cancel()
		if _, err := conn.Exec(rctx, "ROLLBACK"); err != nil {
			// The connection is in an unknown state; do not return it.
			_ = conn.Conn().Close(rctx)
		}
	}

	// Round trip 1: lock and mint.
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(`SELECT ord, status, serial, prev, dup_serial FROM publish_batch_lock($1, $2, $3, $4, $5, $6)`,
		s.series, channels, wait, floors, idCM, ids)
	err := func() error {
		br := conn.SendBatch(ctx, b)
		defer br.Close()
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("begin: %w", err)
		}
		rows, err := br.Query()
		if err != nil {
			return fmt.Errorf("publish_batch_lock: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				ord                       int
				status                    string
				minted, prev, dupOfSerial *string
			)
			if err := rows.Scan(&ord, &status, &minted, &prev, &dupOfSerial); err != nil {
				return fmt.Errorf("scan publish_batch_lock: %w", err)
			}
			sl := byOrd[ord]
			if sl == nil {
				return fmt.Errorf("publish_batch_lock returned unknown ord %d", ord)
			}
			sl.status = status
			if minted != nil {
				sl.serial = *minted
			}
			if prev != nil {
				sl.prev = *prev
			}
			if dupOfSerial != nil {
				sl.dupSerial = *dupOfSerial
			}
		}
		return rows.Err()
	}()
	if err != nil {
		rollback()
		return nil, fmt.Errorf("storage/postgres: batch lock: %w", err)
	}

	// Stamp and encode the fresh publishes.
	var (
		rChannel, rSerial, rKind, rMsgSerial []string
		rIdx                                 []int32
		rID                                  []*string
		rPayload                             [][]byte
		rPersisted                           []bool
		items                                []batchItem
	)
	for i := range slots {
		sl := &slots[i]
		if sl.dupOf >= 0 || sl.status != "ok" {
			continue
		}
		p := sl.p
		w := &busWrite{serial: sl.serial, kind: storage.KindMessage}
		if s.bus.chains() {
			w.prev = sl.prev
		}
		for idx, m := range p.msgs {
			m.Serial = serial.MessageSerial(sl.serial, idx)
			m.Action = protocol.MessageCreate
			storage.StampCreateVersion(m)
			payload, err := msgpack.Marshal(m)
			if err != nil {
				rollback()
				return nil, fmt.Errorf("storage/postgres: encode message %d: %w", idx, err)
			}
			var id *string
			if m.ID != "" {
				id = &m.ID
			}
			rChannel = append(rChannel, p.channel)
			rSerial = append(rSerial, sl.serial)
			rIdx = append(rIdx, int32(idx))
			rID = append(rID, id)
			rKind = append(rKind, string(storage.KindMessage))
			rPayload = append(rPayload, payload)
			rMsgSerial = append(rMsgSerial, m.Serial)
			rPersisted = append(rPersisted, p.cs.persisted)
			w.rows = append(w.rows, payload)
		}
		sl.cm = &protocol.ChannelMessage{ID: p.batchID, ChannelSerial: sl.serial, Messages: p.msgs}
		items = append(items, batchItem{cs: p.cs, cm: sl.cm, w: w})
	}

	// Round trip 2: rows, the bus hook, COMMIT.
	b = &pgx.Batch{}
	if len(rChannel) > 0 {
		b.Queue(`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial, persisted)
			SELECT * FROM unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::text[], $6::bytea[], $7::text[], $8::bool[])`,
			rChannel, rSerial, rIdx, rID, rKind, rPayload, rMsgSerial, rPersisted)
		b.Queue(`INSERT INTO messages (channel, message_serial, payload, deleted, persisted)
			SELECT c, s, p, FALSE, e FROM unnest($1::text[], $2::text[], $3::bytea[], $4::bool[]) AS t(c, s, p, e)`,
			rChannel, rMsgSerial, rPayload, rPersisted)
	}
	queued, err := beforeCommitBatch(ctx, s.bus, b, nil, items)
	if err != nil {
		rollback()
		return nil, err
	}
	if !queued {
		// A bus without a batched hook: send the rows, run its hook per
		// cm on the open transaction, then commit.
		if err := sendAll(ctx, conn.Conn(), b); err != nil {
			rollback()
			return nil, fmt.Errorf("storage/postgres: batch insert: %w", err)
		}
		if _, err := beforeCommitBatch(ctx, s.bus, nil, connTx{conn.Conn()}, items); err != nil {
			rollback()
			return nil, err
		}
		b = &pgx.Batch{}
	}
	b.Queue("COMMIT")
	if err := sendAll(ctx, conn.Conn(), b); err != nil {
		rollback()
		return nil, fmt.Errorf("storage/postgres: batch commit: %w", err)
	}
	return items, nil
}

// sendAll sends b and checks every statement's result. When b ends in
// COMMIT it also checks the transaction really committed: an aborted
// transaction answers COMMIT with ROLLBACK.
func sendAll(ctx context.Context, conn *pgx.Conn, b *pgx.Batch) error {
	n := b.Len()
	if n == 0 {
		return nil
	}
	br := conn.SendBatch(ctx, b)
	defer br.Close()
	for i := range n {
		tag, err := br.Exec()
		if err != nil {
			return err
		}
		if i == n-1 && b.QueuedQueries[i].SQL == "COMMIT" && tag.String() != "COMMIT" {
			return fmt.Errorf("commit: transaction ended with %q", tag.String())
		}
	}
	return nil
}

// planBatch lays a batch out as slots and marks each publish that
// repeats an id of an earlier publish of the same channel in the batch:
// it is not sent to the database and takes its result from that earlier
// publish, the in-batch analogue of the idempotency lookup. Ids are
// compared whenever the database would look them up: client ids always,
// and every id on a retried batch.
func planBatch(batch []*pending) []batchSlot {
	slots := make([]batchSlot, len(batch))
	seen := map[string]map[string]int{} // channel -> id -> slot
	for i, p := range batch {
		slots[i] = batchSlot{p: p, dupOf: -1}
		if !p.checkIDs && !p.retried {
			continue
		}
		byID := seen[p.channel]
		if byID == nil {
			byID = map[string]int{}
			seen[p.channel] = byID
		}
		for _, m := range p.msgs {
			if j, ok := byID[m.ID]; ok {
				slots[i].dupOf = j
				break
			}
		}
		if slots[i].dupOf < 0 {
			for _, m := range p.msgs {
				byID[m.ID] = i
			}
		}
	}
	return slots
}
