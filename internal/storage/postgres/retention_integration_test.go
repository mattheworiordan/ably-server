//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

func persistedNamespace(name string) bool { return strings.HasPrefix(name, "persisted:") }

// leafOf returns the leaf partition that holds the given channel's row
// for channelSerial, via tableoid.
func leafOf(t *testing.T, dsn, table, channel, keyCol, key string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var leaf string
	if err := conn.QueryRow(ctx,
		`SELECT tableoid::regclass::text FROM `+table+` WHERE channel = $1 AND `+keyCol+` = $2 LIMIT 1`,
		channel, key).Scan(&leaf); err != nil {
		t.Fatalf("locate leaf of %s %s=%s: %v", table, keyCol, key, err)
	}
	return leaf
}

func relationExists(t *testing.T, dsn, name string) bool {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok); err != nil {
		t.Fatalf("to_regclass: %v", err)
	}
	return ok
}

// TestRetentionDropsExpiredPartitionsAndKeepsPersisted publishes on a
// channel outside any persisted namespace and on one inside, ages the
// log past the continuity window, and checks that the live channel's
// rows are gone because their partitions were dropped (not deleted row
// by row), while the persisted channel keeps its log, its projection and
// its REST-visible history (DESIGN.md §6.3).
func TestRetentionDropsExpiredPartitionsAndKeepsPersisted(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn, Persisted: persistedNamespace})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	live, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel room: %v", err)
	}
	kept, err := s.Channel(ctx, "persisted:room", nil)
	if err != nil {
		t.Fatalf("Channel persisted:room: %v", err)
	}
	liveCM, _, err := live.Store(ctx, []*protocol.Message{{ID: "l1", Data: "live"}})
	if err != nil {
		t.Fatalf("Store live: %v", err)
	}
	keptCM, _, err := kept.Store(ctx, []*protocol.Message{{ID: "p1", Data: "kept"}})
	if err != nil {
		t.Fatalf("Store persisted: %v", err)
	}

	// Each class reports its own retention floor for resume (§4.3).
	now := time.Now()
	for _, tc := range []struct {
		ch   storage.ChannelStore
		want time.Duration
	}{{live, 2 * time.Minute}, {kept, 24 * time.Hour}} {
		rb, ok := tc.ch.(storage.RetentionBounded)
		if !ok {
			t.Fatal("postgres ChannelStore does not implement storage.RetentionBounded")
		}
		// now - retention, shifted later (never earlier) by the clock
		// margin and the database clock offset, both small here.
		lo := fmt.Sprintf("%014d", now.Add(-tc.want).UnixMilli())
		hi := fmt.Sprintf("%014d", now.Add(-tc.want+5*time.Second).UnixMilli())
		if got := rb.RetainedSince(now); got < lo || got > hi {
			t.Errorf("RetainedSince = %s, want in [%s, %s] (now - %v, plus margin)", got, lo, hi, tc.want)
		}
	}

	liveLeaf := leafOf(t, dsn, "channel_messages", "room", "channel_serial", liveCM.ChannelSerial)
	liveProjLeaf := leafOf(t, dsn, "messages", "room", "message_serial", liveCM.Messages[0].Serial)
	keptLeaf := leafOf(t, dsn, "channel_messages", "persisted:room", "channel_serial", keptCM.ChannelSerial)
	if !strings.HasPrefix(liveLeaf, "channel_messages_live_") {
		t.Errorf("live row in %s, want a channel_messages_live_* leaf", liveLeaf)
	}
	if !strings.HasPrefix(keptLeaf, "channel_messages_persisted_") {
		t.Errorf("persisted row in %s, want a channel_messages_persisted_* leaf", keptLeaf)
	}

	// Inside the window nothing is dropped.
	if err := s.MaintainPartitionsAt(ctx, time.Minute); err != nil {
		t.Fatalf("sweep at +1m: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'room'`); n != 1 {
		t.Fatalf("live rows after +1m sweep = %d, want 1 (still inside the 2m window)", n)
	}

	// Past the window (2m retention + 1m leaf width) the live leaves go.
	if err := s.MaintainPartitionsAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("sweep at +10m: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'room'`); n != 0 {
		t.Errorf("live log rows after the window = %d, want 0", n)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM messages WHERE channel = 'room'`); n != 0 {
		t.Errorf("live projection rows after the window = %d, want 0", n)
	}
	if relationExists(t, dsn, liveLeaf) || relationExists(t, dsn, liveProjLeaf) {
		t.Errorf("leaves %s / %s still exist; retention must drop partitions, not delete rows", liveLeaf, liveProjLeaf)
	}
	if got := s.PartitionsDropped("channel_messages"); got < 1 {
		t.Errorf("ably_storage_partitions_dropped_total{table=channel_messages} = %v, want >= 1", got)
	}

	// The persisted channel keeps everything.
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'persisted:room'`); n != 1 {
		t.Errorf("persisted log rows after the window = %d, want 1", n)
	}
	page, err := kept.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Collapse: true})
	if err != nil {
		t.Fatalf("persisted History: %v", err)
	}
	if len(page.ChannelMessages) != 1 || page.ChannelMessages[0].Messages[0].Data != "kept" {
		t.Errorf("persisted collapsed history = %+v, want the one message", page.ChannelMessages)
	}
	if !relationExists(t, dsn, keptLeaf) {
		t.Errorf("persisted leaf %s was dropped inside its 24h retention", keptLeaf)
	}

	// And a persisted channel ages out on its own, longer, retention.
	if err := s.MaintainPartitionsAt(ctx, 26*time.Hour); err != nil {
		t.Fatalf("sweep at +26h: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'persisted:room'`); n != 0 {
		t.Errorf("persisted log rows after 26h = %d, want 0", n)
	}
}

// TestRetentionIdempotencyWithinWindow checks that a republished id is
// detected in both retention classes (the lookup is confined to the
// channel's class).
func TestRetentionIdempotencyWithinWindow(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn, Persisted: persistedNamespace})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for _, name := range []string{"room", "persisted:room"} {
		ch, err := s.Channel(ctx, name, nil)
		if err != nil {
			t.Fatalf("Channel %s: %v", name, err)
		}
		first, _, err := ch.Store(ctx, []*protocol.Message{{ID: "same", Data: "a"}})
		if err != nil {
			t.Fatalf("Store %s: %v", name, err)
		}
		again, idem, err := ch.Store(ctx, []*protocol.Message{{ID: "same", Data: "b"}})
		if err != nil {
			t.Fatalf("Store %s again: %v", name, err)
		}
		if !idem || again.ChannelSerial != first.ChannelSerial {
			t.Errorf("%s: republish of id=same: idempotent=%v serial=%s, want true %s", name, idem, again.ChannelSerial, first.ChannelSerial)
		}
	}
}

// TestRetentionMigratesExistingLog upgrades a schema that holds rows
// written under migration 0001 only: the old table becomes a leaf of the
// live class, its rows stay readable, new publishes land in new leaves,
// and the old leaf ages out on the continuity window.
func TestRetentionMigratesExistingLog(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	initial, err := os.ReadFile("migrations/0001_initial.sql")
	if err != nil {
		t.Fatalf("read 0001: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
		string(initial),
		`INSERT INTO schema_migrations (version) VALUES ('0001_initial')`,
		`SELECT advance_channel_serial('old', 'legacyseries')`,
		`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
		 SELECT 'old', channel_serial, 0, 'old-id', 'message', '\x80'::bytea, channel_serial || ':0' FROM channels WHERE name = 'old'`,
		`INSERT INTO messages (channel, message_serial, payload)
		 SELECT 'old', channel_serial || ':0', '\x80'::bytea FROM channels WHERE name = 'old'`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed 0001 schema: %v\n%s", err, stmt)
		}
	}
	_ = conn.Close(ctx)

	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn, Persisted: persistedNamespace})
	if err != nil {
		t.Fatalf("Open (migrating): %v", err)
	}
	// Pre-migration rows of a persisted channel sit in the live class and
	// age out with it, so a persisted channel's resume floor is at least
	// the legacy leaf's bound, not now minus 24h (DESIGN.md §4.3, §6.3).
	pch, err := s.Channel(ctx, "persisted:legacy", nil)
	if err != nil {
		t.Fatalf("Channel persisted:legacy: %v", err)
	}
	if floor := pch.(storage.RetentionBounded).RetainedSince(time.Now()); floor < fmt.Sprintf("%014d", time.Now().UnixMilli()) {
		t.Errorf("persisted channel floor after migrating a non-empty log = %s, want at least the legacy bound (now + 1m)", floor)
	}
	t.Cleanup(func() { _ = s.Close() })

	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'old'`); n != 1 {
		t.Fatalf("old rows visible after migration = %d, want 1", n)
	}
	if !relationExists(t, dsn, "channel_messages_legacy") || !relationExists(t, dsn, "messages_legacy") {
		t.Fatal("non-empty old tables should be attached as legacy leaves")
	}
	// The id of an old row still dedupes a republish.
	old, err := s.Channel(ctx, "old", nil)
	if err != nil {
		t.Fatalf("Channel old: %v", err)
	}
	if _, idem, err := old.Store(ctx, []*protocol.Message{{ID: "old-id"}}); err != nil || !idem {
		t.Errorf("republish of a pre-migration id: idempotent=%v err=%v, want true nil", idem, err)
	}
	fresh, _, err := old.Store(ctx, []*protocol.Message{{ID: "new-id", Data: "new"}})
	if err != nil {
		t.Fatalf("Store after migration: %v", err)
	}
	if leaf := leafOf(t, dsn, "channel_messages", "old", "channel_serial", fresh.ChannelSerial); leaf == "channel_messages_legacy" {
		// The legacy leaf covers up to a minute past the migration, so a
		// publish inside that minute may land in it. Either is correct.
		t.Logf("fresh publish landed in the legacy leaf (inside its bound)")
	}

	if err := s.MaintainPartitionsAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("sweep at +10m: %v", err)
	}
	if relationExists(t, dsn, "channel_messages_legacy") || relationExists(t, dsn, "messages_legacy") {
		t.Error("legacy leaves should age out on the continuity window")
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channel_messages WHERE channel = 'old'`); n != 0 {
		t.Errorf("old rows after the window = %d, want 0", n)
	}
}

// TestRetentionFreshInstallHasNoLegacyLeaf checks the empty-schema path:
// the migration drops the empty old tables instead of attaching them.
func TestRetentionFreshInstallHasNoLegacyLeaf(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	s, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if relationExists(t, dsn, "channel_messages_legacy") || relationExists(t, dsn, "messages_legacy") {
		t.Error("fresh install should not keep empty legacy tables")
	}
	// Leaves exist ahead of now in both classes of both tables.
	for _, parent := range []string{"channel_messages_live", "channel_messages_persisted", "messages_live", "messages_persisted"} {
		if n := countRows(t, dsn, `SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass($1)`, parent); n < 2 {
			t.Errorf("%s has %d leaves after Open, want at least 2 (current and ahead)", parent, n)
		}
	}
}

// TestRetentionStuckDetachDoesNotBlockCreation: a transaction holding a
// snapshot on the log makes DETACH ... CONCURRENTLY wait. The wait must
// be bounded, must not stop leaves being created, and the pending detach
// must be finished on a later sweep once the transaction ends.
func TestRetentionStuckDetachDoesNotBlockCreation(t *testing.T) {
	restore := postgres.SetDetachTimeout(500 * time.Millisecond)
	defer restore()
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ch, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	cm, _, err := ch.Store(ctx, []*protocol.Message{{Data: "x"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	leaf := leafOf(t, dsn, "channel_messages", "room", "channel_serial", cm.ChannelSerial)

	// A reader that keeps a snapshot open on the log.
	reader, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect reader: %v", err)
	}
	defer reader.Close(ctx)
	tx, err := reader.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatalf("begin reader: %v", err)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM channel_messages`).Scan(&n); err != nil {
		t.Fatalf("reader query: %v", err)
	}

	start := time.Now()
	if err := s.MaintainPartitionsAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("sweep with a stuck detach returned %v; drops are best effort", err)
	}
	if took := time.Since(start); took > 8*time.Second {
		t.Errorf("sweep took %v with a stuck detach, want it bounded by the detach timeout", took)
	}
	// Creation still happened: leaves exist through +10m plus the lookahead.
	future := fmt.Sprintf("%014d", time.Now().Add(10*time.Minute+30*time.Minute).UnixMilli())
	if got := countRows(t, dsn, `SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'channel_messages_live'::regclass AND c.relname > 'channel_messages_live_'||$1`, future); got == 0 {
		t.Error("no live leaves created ahead while a detach was stuck")
	}
	if !relationExists(t, dsn, leaf) {
		t.Fatalf("leaf %s vanished while a reader held a snapshot on it", leaf)
	}

	_ = tx.Rollback(ctx)
	if err := s.MaintainPartitionsAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if relationExists(t, dsn, leaf) {
		t.Errorf("leaf %s still exists after the reader finished; the pending detach should be finalised and dropped", leaf)
	}
}

// TestRetentionDropsOrphanedDetachedLeaf: a leaf whose detach succeeded
// but whose DROP failed is no longer a partition; a later sweep must
// still drop it.
func TestRetentionDropsOrphanedDetachedLeaf(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE channel_messages_live_00000000060000 (LIKE channel_messages_live)`); err != nil {
		t.Fatalf("create orphan: %v", err)
	}
	if err := s.MaintainPartitionsAt(ctx, 0); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if relationExists(t, dsn, "channel_messages_live_00000000060000") {
		t.Error("orphaned detached leaf was not dropped")
	}
}

// TestRetentionDropTimesOutOnAHeldLeafAndRetries: a reader holding a lock
// on a detached leaf must not stall the DROP (and every later reader of
// the leaf queued behind it). The DROP gives up after the lock timeout,
// counts it and leaves the leaf as an orphan; the next sweep drops it by
// name once the reader is gone (DESIGN.md §6.3). The leaf here is a
// detached orphan, the state a leaf is in after its detach succeeded and
// its DROP failed.
func TestRetentionDropTimesOutOnAHeldLeafAndRetries(t *testing.T) {
	defer postgres.SetDropLockTimeout(300 * time.Millisecond)()
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const leaf = "channel_messages_live_00000000060000"
	reader, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect reader: %v", err)
	}
	defer reader.Close(ctx)
	if _, err := reader.Exec(ctx, `CREATE TABLE `+leaf+` (LIKE channel_messages_live)`); err != nil {
		t.Fatalf("create orphan: %v", err)
	}
	tx, err := reader.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reader: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE `+leaf+` IN ACCESS SHARE MODE`); err != nil {
		t.Fatalf("lock leaf: %v", err)
	}

	start := time.Now()
	if err := s.MaintainDropAt(ctx, 0); err != nil {
		t.Fatalf("drop sweep with a held leaf returned %v; drops are best effort", err)
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("drop sweep took %v with a held leaf, want it bounded by the lock timeout", took)
	}
	if got := s.DropLockTimeouts(); got != 1 {
		t.Errorf("ably_storage_partition_drop_lock_timeouts_total = %v, want 1 (one attempt, not retried in the same sweep)", got)
	}
	if !relationExists(t, dsn, leaf) {
		t.Fatalf("leaf %s was dropped while a reader held it", leaf)
	}

	_ = tx.Rollback(ctx)
	if err := s.MaintainDropAt(ctx, 0); err != nil {
		t.Fatalf("second drop sweep: %v", err)
	}
	if relationExists(t, dsn, leaf) {
		t.Errorf("leaf %s still exists after the reader finished; the next sweep should drop the orphan", leaf)
	}
}

// TestRetentionRangeReadsDoNotLockExpiredLeaves: the chain's
// single-channel range read (sqlLoadRange, LoadRangeAfter) is bounded
// below by its mark, so from a recent mark it prunes (and never locks) a
// leaf entirely below the mark. An ACCESS EXCLUSIVE lock held on such a
// leaf, as a drop waiting its turn would be, must not stall it. Eight
// reads, past the point where a cached statement would switch to a
// generic plan, which locks every leaf.
//
// The shape changed with 2e03e05: the read used to start at mark "" and
// rely on a retention floor applied to the range to prune the old leaf.
// The floor is gone (it hid cms the log still held, DESIGN.md §6.3), so
// the read now starts from a mark ten seconds old; a mark older than the
// leaf asks for it, and must. Only the single-channel read is covered:
// the batched read (sqlLoadRangeMany: reconcile, sweep catch-up, checked
// reads) takes its bounds from unnest and opens every leaf whatever the
// marks, a cost DESIGN.md §6.3 states.
func TestRetentionRangeReadsDoNotLockExpiredLeaves(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ch, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// An old leaf, an hour before now (retention is 2 minutes), locked.
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)
	lo := time.Now().Add(-time.Hour).UnixMilli()
	hi := lo + int64(time.Minute/time.Millisecond)
	const old = "channel_messages_live_old_test"
	for _, stmt := range []string{
		`CREATE TABLE ` + old + ` (LIKE channel_messages_live INCLUDING DEFAULTS)`,
		fmt.Sprintf(`ALTER TABLE channel_messages_live ATTACH PARTITION %s FOR VALUES FROM ('%014d') TO ('%014d')`, old, lo, hi),
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	locker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect locker: %v", err)
	}
	defer locker.Close(ctx)
	tx, err := locker.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE `+old+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock old leaf: %v", err)
	}

	// A mark ten seconds old: the leaves entirely below it are pruned by
	// the planner, so the read never asks for the locked hour-old leaf. (A
	// mark older than the leaf would, and must: every cm after a mark is
	// wanted whatever its age, DESIGN.md §6.3.)
	mark := fmt.Sprintf("%014d", time.Now().Add(-10*time.Second).UnixMilli())
	for i := range 8 {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := s.LoadRangeAfter(rctx, "room", mark)
		cancel()
		if err != nil {
			t.Fatalf("range read %d blocked or failed with an expired leaf locked: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("range read %d returned %d cms, want the 1 published", i, n)
		}
	}
}

// TestRetentionPrunesIdleChannelRows: a channels row idle past the
// longest retention and with no presence member is deleted by the sweep;
// a channel with a presence row keeps its row; a later publish on a
// pruned channel works and mints above every serial the channel ever had
// (DESIGN.md §6.3).
func TestRetentionPrunesIdleChannelRows(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn, Retention: postgres.Retention{Persisted: 3 * time.Minute}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	idle, err := s.Channel(ctx, "idle", nil)
	if err != nil {
		t.Fatalf("Channel idle: %v", err)
	}
	held, err := s.Channel(ctx, "held", nil)
	if err != nil {
		t.Fatalf("Channel held: %v", err)
	}
	oldCM, _, err := idle.Store(ctx, []*protocol.Message{{ID: "i1", Data: "old"}})
	if err != nil {
		t.Fatalf("Store idle: %v", err)
	}
	if _, _, err := held.Store(ctx, []*protocol.Message{{Data: "m"}}); err != nil {
		t.Fatalf("Store held: %v", err)
	}
	if _, _, err := held.StorePresence(ctx, []*protocol.PresenceMessage{{
		Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "conn1",
	}}); err != nil {
		t.Fatalf("enter presence: %v", err)
	}
	rowExists := func(name string) bool {
		return countRows(t, dsn, `SELECT count(*) FROM channels WHERE name = $1`, name) == 1
	}

	// Inside the longest retention (3m) nothing is pruned.
	if err := s.MaintainDropAt(ctx, time.Minute); err != nil {
		t.Fatalf("sweep at +1m: %v", err)
	}
	if !rowExists("idle") || !rowExists("held") {
		t.Fatal("a channels row was pruned inside the retention window")
	}

	// Past it: the idle channel's row goes, the one with a presence row stays.
	if err := s.MaintainDropAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("sweep at +10m: %v", err)
	}
	if rowExists("idle") {
		t.Error("idle channel's row was not pruned")
	}
	if !rowExists("held") {
		t.Error("a channel with a presence row lost its channels row")
	}
	if got := s.ChannelRowsDropped(); got != 1 {
		t.Errorf("ably_storage_channel_rows_dropped_total = %v, want 1", got)
	}

	// The skewed sweep dropped the leaves around real now too; the next
	// create tick of a real deployment makes them again.
	if err := s.MaintainCreateAt(ctx, 0); err != nil {
		t.Fatalf("create tick: %v", err)
	}

	// A later publish recreates the row and mints above the old serials.
	idle2, err := s.Channel(ctx, "idle", nil)
	if err != nil {
		t.Fatalf("Channel idle again: %v", err)
	}
	newCM, _, err := idle2.Store(ctx, []*protocol.Message{{ID: "i2", Data: "new"}})
	if err != nil {
		t.Fatalf("Store after prune: %v", err)
	}
	if newCM.ChannelSerial <= oldCM.ChannelSerial {
		t.Errorf("serial after prune = %s, want above the old %s", newCM.ChannelSerial, oldCM.ChannelSerial)
	}
	if !rowExists("idle") {
		t.Error("publish after prune did not recreate the channels row")
	}
}

// A fresh storage whose node cached a channels row it then lost still
// binds and publishes: the known-row cache falls back to ensure_channel.
func TestRetentionPruneThenBindWithAKnownRowCache(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn, Retention: postgres.Retention{Persisted: 3 * time.Minute}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ch, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "a"}}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s.MaintainDropAt(ctx, 10*time.Minute); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channels WHERE name = 'room'`); n != 0 {
		t.Fatalf("channels rows after prune = %d, want 0", n)
	}
	if err := s.MaintainCreateAt(ctx, 0); err != nil { // the skewed drop took the current leaf too
		t.Fatalf("create tick: %v", err)
	}
	// The same node publishes on its still-bound store: the row is made again.
	if _, _, err := ch.Store(ctx, []*protocol.Message{{Data: "b"}}); err != nil {
		t.Fatalf("Store on a pruned channel's existing store: %v", err)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM channels WHERE name = 'room'`); n != 1 {
		t.Errorf("channels rows after republish = %d, want 1", n)
	}
}
