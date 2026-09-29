package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// Bus kinds accepted by Options.Bus (DESIGN.md §7.2, §7.3).
const (
	// BusPostgres is the default bus: LISTEN/NOTIFY on the store itself.
	BusPostgres = "postgres"
	// BusNATS is NATS core pub/sub on a per-channel subject. Postgres
	// stays the store; only cross-node delivery moves.
	BusNATS = "nats"
)

// Bus is the cross-node delivery mechanism of cluster mode: how a cm
// committed on one node reaches the channelStore bound to that channel
// on every node (DESIGN.md §7.2, §7.3). Postgres is the store under
// every Bus; a Bus decides only how the existence of a committed cm
// (and, for the NATS bus, its body) travels between nodes.
//
// Two implementations live in this package: pgNotifyBus, the default
// LISTEN/NOTIFY broker, and natsBus. The methods are unexported because
// a Bus reaches into the per-channel delivery point (channelStore) and
// the publish transaction; the abstraction is a seam inside the
// backend, not a public plug-in API.
type Bus interface {
	// start launches the bus's background goroutines. They are tracked
	// by the Storage WaitGroup and stop when ctx is cancelled (Close).
	start(ctx context.Context)

	// chains reports whether deliveries carry their predecessor serial.
	// When true the publish transaction captures the channel's previous
	// serial (channelStore.advanceSerial) and deliveries are ordered on
	// it (channelStore.deliverChained); when false the delivery point
	// relies on the bus delivering each channel's cms in commit order.
	chains() bool

	// bind is called when Storage.Channel first binds a channel with a
	// non-nil appender, before the channel's watermark is read, so a bus
	// that routes per channel can subscribe without a gap.
	bind(ctx context.Context, cs *channelStore) error

	// unbind reverses bind when the bind cannot complete.
	unbind(cs *channelStore)

	// beforeCommit runs inside the publish transaction once the cm's
	// rows are written, just before COMMIT.
	beforeCommit(ctx context.Context, tx pgx.Tx, channel, channelSerial string) error

	// afterCommit runs once the publish transaction has committed. prev
	// is the channel's serial before this publish ("" when the bus does
	// not chain, or the channel had no row).
	afterCommit(cs *channelStore, cm *protocol.ChannelMessage, prev string)

	// close releases the bus's connections. Called by Storage.Close
	// after the background goroutines have stopped.
	close()
}

// pgNotifyBus is the default Bus: every publish emits a NOTIFY on the
// single "ably_channel" LISTEN channel inside its transaction, and one
// LISTEN goroutine per node dispatches every notification, reading the
// cm back from the log (DESIGN.md §7.2). Its mechanics (listenLoop,
// consume, redial, reconcile) stay on Storage, unchanged; this type is
// only the adapter onto the Bus seam.
type pgNotifyBus struct {
	s *Storage
}

func (b *pgNotifyBus) start(ctx context.Context) {
	b.s.wg.Add(1)
	go b.s.listenLoop(ctx)
}

func (b *pgNotifyBus) chains() bool { return false }

func (b *pgNotifyBus) bind(context.Context, *channelStore) error { return nil }

func (b *pgNotifyBus) unbind(*channelStore) {}

// beforeCommit emits the NOTIFY inside the publish transaction: PG
// buffers the payload until commit, so listeners only see it if the
// publish lands. The LISTEN goroutine on every node (including this
// one) routes the cm to the channel's appender (DESIGN.md §7.2).
func (b *pgNotifyBus) beforeCommit(ctx context.Context, tx pgx.Tx, channel, channelSerial string) error {
	body, err := json.Marshal(notifyPayload{Channel: channel, Serial: channelSerial})
	if err != nil {
		return fmt.Errorf("storage/postgres: encode notify: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannelName, string(body)); err != nil {
		return fmt.Errorf("storage/postgres: notify: %w", err)
	}
	return nil
}

// afterCommit is a no-op: delivery, including to the publisher's own
// subscribers, arrives via the NOTIFY round-trip (DESIGN.md §7.2).
func (b *pgNotifyBus) afterCommit(*channelStore, *protocol.ChannelMessage, string) {}

func (b *pgNotifyBus) close() {}
