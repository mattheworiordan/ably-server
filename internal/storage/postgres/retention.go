package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Retention defaults (DESIGN.md §6.3). The continuity window matches
// Ably's default for channels without persisted history; the persisted
// default matches Ably's shortest persisted-history period.
const (
	DefaultMessageRetention   = 2 * time.Minute
	DefaultPersistedRetention = 24 * time.Hour
)

// minPostgresVersion is the oldest server the partitioned log supports:
// the retention sweep detaches expired partitions with DETACH PARTITION
// ... CONCURRENTLY (PostgreSQL 14) so it never blocks publishes.
const minPostgresVersion = 140000

// createLockKey and dropLockKey namespace the advisory locks that
// serialise partition creation and partition dropping across nodes
// sharing a schema. Each is combined with hashtext(current_schema()) in
// the two-key advisory lock form, so independent schemas in one database
// do not contend. They are separate so a slow drop can never hold up
// creation: a missing partition fails publishes, a late drop does not.
const (
	createLockKey int32 = 0x1ab1e5e7
	dropLockKey   int32 = 0x1ab1e5e8
)

// detachTimeout bounds one DETACH ... CONCURRENTLY, which waits for every
// transaction that might still see the partition. A detach cut short is
// left "detach pending" and finished with FINALIZE on a later sweep.
// It is a var so tests can shrink it.
var detachTimeout = 30 * time.Second

// dropLockTimeout bounds how long one DROP TABLE of an expired leaf waits
// for its ACCESS EXCLUSIVE lock. A reader still holding the leaf (or a
// statement queued behind one) would otherwise stall the drop, and every
// new reader of the leaf behind it. A drop that times out leaves the
// detached leaf as an orphan the next drop sweep removes by name. It is
// a var so tests can shrink it.
var dropLockTimeout = time.Second

// clockMargin is added to the retention floor a resume is checked
// against (RetainedSince), on top of the measured offset between this
// node's clock and the database's, so small errors in that offset err
// towards refusing a resume rather than wrongly claiming continuity.
const clockMargin = time.Second

// leafNameRe matches the leaves ensurePartitions creates. A leaf whose
// detach succeeded but whose DROP failed is no longer a partition, so it
// is found again by name.
var leafNameRe = `^(channel_messages|messages)_(live|persisted)_[0-9]{14}$`

// partitionedTables are the tables that grow with every publish and are
// partitioned by retention class and then by serial range (migration
// 0002).
var partitionedTables = []string{"channel_messages", "messages"}

// Retention configures the message log's retention (DESIGN.md §6.3).
// Every node sharing a database must use the same values: whichever node
// runs the sweep applies its own.
type Retention struct {
	// Message is how long a channel outside any persisted namespace keeps
	// its log and projection rows: the continuity window. Zero means
	// DefaultMessageRetention.
	Message time.Duration

	// Persisted is how long a channel in a persisted namespace keeps its
	// log and projection rows. Zero means DefaultPersistedRetention.
	Persisted time.Duration

	// LivePartition and PersistedPartition are the width of one leaf
	// partition in each class. A row lives for at most retention plus one
	// width. Zero derives half the class's retention, clamped to
	// [1m, 1h]. Tests shrink them; operators should not need to.
	LivePartition      time.Duration
	PersistedPartition time.Duration

	// MaintenanceInterval is how often each node runs a retention
	// maintenance tick (alternately creating and dropping leaves, see
	// retentionLoop). It is not the bus watermark sweep (Options.SweepInterval).
	// Zero derives half the narrower partition width, clamped to
	// [1s, 1m].
	MaintenanceInterval time.Duration
}

// retentionClass is one level-1 partition: the live class (persisted =
// FALSE) or the persisted class (persisted = TRUE).
type retentionClass struct {
	persisted bool
	retention time.Duration
	width     time.Duration
}

// suffix names the class's level-1 partition of a table.
func (c retentionClass) suffix() string {
	if c.persisted {
		return "_persisted"
	}
	return "_live"
}

// lookahead is how far past now the class keeps leaf partitions created,
// so sweeps can fail for an hour before a publish finds no partition to
// land in. Empty leaves cost almost nothing, and the lookups that cannot
// prune by an exact serial bound their serial range above by the serial
// being written, so future leaves are pruned from them.
func (c retentionClass) lookahead() time.Duration {
	return max(2*c.width, time.Hour)
}

// resolve fills zero fields with their defaults.
func (r Retention) resolve() Retention {
	if r.Message <= 0 {
		r.Message = DefaultMessageRetention
	}
	if r.Persisted <= 0 {
		r.Persisted = DefaultPersistedRetention
	}
	if r.LivePartition <= 0 {
		r.LivePartition = derivePartitionWidth(r.Message)
	}
	if r.PersistedPartition <= 0 {
		r.PersistedPartition = derivePartitionWidth(r.Persisted)
	}
	if r.MaintenanceInterval <= 0 {
		r.MaintenanceInterval = min(max(min(r.LivePartition, r.PersistedPartition)/2, time.Second), time.Minute)
	}
	return r
}

// setRetention gives a new channelStore its retention class and the
// storage's publish lanes (DESIGN.md §6.3).
func (s *Storage) setRetention(cs *channelStore) {
	cs.persisted = s.persisted(cs.name)
	cs.retention = s.retentionOf(cs.persisted)
	cs.clock = &s.clockOffset
	cs.lanes = s.lanes
	if cs.persisted {
		cs.floorMin = s.legacyBound
	}
}

// preparePartitions runs at Open: it reads the legacy bound a migration
// recorded and creates the leaves ahead of now, without dropping any.
func (s *Storage) preparePartitions(ctx context.Context) error {
	err := s.pool.QueryRow(ctx, `SELECT value FROM retention_state WHERE key = 'legacy_live_bound'`).Scan(&s.legacyBound)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read retention_state: %w", err)
	}
	nowMs, err := s.databaseNow(ctx)
	if err != nil {
		return err
	}
	return s.ensurePartitions(ctx, nowMs)
}

// retentionOf returns the retention of a class.
func (s *Storage) retentionOf(persisted bool) time.Duration {
	if persisted {
		return s.retention.Persisted
	}
	return s.retention.Message
}

func derivePartitionWidth(retention time.Duration) time.Duration {
	return min(max(retention/2, time.Minute), time.Hour)
}

func (r Retention) classes() []retentionClass {
	return []retentionClass{
		{persisted: false, retention: r.Message, width: r.LivePartition},
		{persisted: true, retention: r.Persisted, width: r.PersistedPartition},
	}
}

// partition is one leaf partition of a class, with its serial range
// decoded to mint times in ms. lo is math.MinInt64 for a MINVALUE bound
// (the legacy leaf a migrated table becomes).
type partition struct {
	name          string
	lo, hi        int64
	detachPending bool
}

// retentionMetrics are the ably_storage_* series the sweep maintains.
type retentionMetrics struct {
	created *prometheus.CounterVec
	dropped *prometheus.CounterVec
	errors  prometheus.Counter
	// dropTimeouts counts DROP TABLE of a leaf given up after
	// dropLockTimeout (retried next drop sweep).
	dropTimeouts prometheus.Counter
	// channelRowsDropped counts channels rows deleted by the prune step.
	channelRowsDropped prometheus.Counter
	logBytes           *prometheus.GaugeVec
	leaves             *prometheus.GaugeVec
}

func newRetentionMetrics() *retentionMetrics {
	return &retentionMetrics{
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_storage_partitions_created_total",
			Help: "Leaf partitions of the message log created ahead of time by this node, by table.",
		}, []string{"table"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_storage_partitions_dropped_total",
			Help: "Leaf partitions of the message log dropped by this node's retention sweep, by table.",
		}, []string{"table"}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_storage_retention_errors_total",
			Help: "Retention sweep steps that failed on this node (retried on the next sweep).",
		}),
		dropTimeouts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_storage_partition_drop_lock_timeouts_total",
			Help: "Drops of an expired leaf partition abandoned after waiting the lock timeout for the leaf (a reader held it); the leaf is dropped by a later sweep.",
		}),
		channelRowsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_storage_channel_rows_dropped_total",
			Help: "channels rows deleted by this node's retention sweep: idle for longer than the longest retention and with no presence member.",
		}),
		logBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ably_storage_log_bytes",
			Help: "Total on-disk size of the partitioned tables (heap, indexes, TOAST), by table, as of this node's last sweep.",
		}, []string{"table"}),
		leaves: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ably_storage_log_partitions",
			Help: "Leaf partitions of the partitioned tables, by table, as of this node's last sweep.",
		}, []string{"table"}),
	}
}

func (m *retentionMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.created, m.dropped, m.errors, m.dropTimeouts, m.channelRowsDropped, m.logBytes, m.leaves}
}

// retentionLoop runs partition maintenance every MaintenanceInterval until the
// storage is closed. Ticks alternate between creating leaves ahead and
// dropping expired ones, never both in one tick: a drop's detach can wait
// up to detachTimeout for old snapshots and holds the parent's
// SHARE UPDATE EXCLUSIVE lock while it does, and creation (ATTACH) needs
// the same lock under a shorter lock timeout, so running them back to
// back lets a slow detach fail the creation that publishes depend on. The
// lookahead (at least an hour) is many ticks long, so creating on every
// second tick loses nothing (DESIGN.md §6.3).
func (s *Storage) retentionLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.retention.MaintenanceInterval)
	defer t.Stop()
	for tick := 0; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var err error
			if tick%2 == 0 {
				err = s.maintainCreate(ctx, 0)
			} else {
				err = s.maintainDrop(ctx, 0)
			}
			if err != nil && ctx.Err() == nil {
				s.logger.Warn("storage/postgres: retention sweep failed", "err", err)
			}
		}
	}
}

// maintainPartitions runs a full sweep, creation then drop. The loop never
// does this (see retentionLoop); tests use it to age the log in one call.
func (s *Storage) maintainPartitions(ctx context.Context, skew time.Duration) error {
	if err := s.maintainCreate(ctx, skew); err != nil {
		return err
	}
	return s.maintainDrop(ctx, skew)
}

// maintainCreate creates the leaf partitions each class needs from now to
// now plus its lookahead (DESIGN.md §6.3). Time is the database's clock,
// the same clock that mints serials, so a node with a skewed clock cannot
// create the wrong leaves. skew shifts that clock and exists for tests. A
// failure is returned: without partitions ahead, publishes fail.
func (s *Storage) maintainCreate(ctx context.Context, skew time.Duration) error {
	nowMs, err := s.databaseNow(ctx)
	if err != nil {
		s.metrics.errors.Inc()
		return err
	}
	nowMs += skew.Milliseconds()
	if err := s.ensurePartitions(ctx, nowMs); err != nil {
		s.metrics.errors.Inc()
		return err
	}
	s.observeLogSize(ctx)
	return nil
}

// maintainDrop detaches and drops every leaf whose whole range is older
// than the class's retention, then prunes the channels table (DESIGN.md
// §6.3). A drop failure is logged and counted and retried on the next
// drop sweep; it is not returned.
func (s *Storage) maintainDrop(ctx context.Context, skew time.Duration) error {
	nowMs, err := s.databaseNow(ctx)
	if err != nil {
		s.metrics.errors.Inc()
		return err
	}
	nowMs += skew.Milliseconds()
	if err := s.dropExpired(ctx, nowMs); err != nil {
		s.metrics.errors.Inc()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.Warn("storage/postgres: retention drop failed; retrying next sweep", "err", err)
	}
	return nil
}

// databaseNow reads the database clock in epoch ms and records its
// offset from this node's clock, which RetainedSince applies.
func (s *Storage) databaseNow(ctx context.Context) (int64, error) {
	before := time.Now()
	var nowMs int64
	if err := s.pool.QueryRow(ctx, `SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint`).Scan(&nowMs); err != nil {
		return 0, fmt.Errorf("read database clock: %w", err)
	}
	mid := before.Add(time.Since(before) / 2)
	s.clockOffset.Store(nowMs - mid.UnixMilli())
	return nowMs, nil
}

// ensurePartitions creates, in one transaction under the maintenance
// advisory lock, every leaf missing between the current slot and now
// plus the lookahead, for every table and class (see planPartitions).
//
// Each leaf is created standalone and then ATTACHed, which takes only a
// SHARE UPDATE EXCLUSIVE lock on the parent, so creating a partition
// does not block publishes the way CREATE TABLE ... PARTITION OF would.
func (s *Storage) ensurePartitions(ctx context.Context, nowMs int64) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("retention: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '10s'`); err != nil {
		return fmt.Errorf("retention: set lock_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))`, createLockKey); err != nil {
		return fmt.Errorf("retention: advisory lock: %w", err)
	}

	created := map[string]int{}
	for _, table := range partitionedTables {
		for _, c := range s.retention.classes() {
			parent := table + c.suffix()
			parts, err := listPartitions(ctx, tx, parent)
			if err != nil {
				return err
			}
			end := nowMs + c.lookahead().Milliseconds()
			for _, r := range planPartitions(parts, nowMs, end, c.width.Milliseconds()) {
				name := fmt.Sprintf("%s_%014d", parent, r[0])
				ident := pgx.Identifier{name}.Sanitize()
				if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS)`,
					ident, pgx.Identifier{parent}.Sanitize())); err != nil {
					return fmt.Errorf("retention: create %s: %w", name, err)
				}
				if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%014d') TO ('%014d')`,
					pgx.Identifier{parent}.Sanitize(), ident, r[0], r[1])); err != nil {
					return fmt.Errorf("retention: attach %s: %w", name, err)
				}
				created[table]++
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("retention: commit partition creation: %w", err)
	}
	for table, n := range created {
		s.metrics.created.WithLabelValues(table).Add(float64(n))
		s.logger.Debug("storage/postgres: created log partitions", "table", table, "count", n)
	}
	return nil
}

// dropExpired detaches and drops every leaf whose range ends at or
// before now minus its class's retention, in both classes of both
// tables, then drops any leaf left behind by an earlier detach whose DROP
// failed. It runs on one connection holding a session advisory lock
// (tried, not waited for: another node is already dropping).
//
// DETACH PARTITION ... CONCURRENTLY takes only a SHARE UPDATE EXCLUSIVE
// lock on the parent and waits for older snapshots rather than blocking
// new ones, so a drop never stalls publishes; the detached table is then
// dropped on its own. Each detach is bounded by detachTimeout; one cut
// short is left detach pending and finished with FINALIZE next time.
func (s *Storage) dropExpired(ctx context.Context, nowMs int64) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, hashtext(current_schema()))`, dropLockKey).Scan(&locked); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1, hashtext(current_schema()))`, dropLockKey)
	}()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET statement_timeout = %d`, detachTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set statement_timeout: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `RESET statement_timeout`)
	}()

	var errs []error
	// timedOut records leaves whose DROP gave up on its lock this sweep, so
	// the by-name pass below does not wait on them a second time.
	timedOut := map[string]bool{}
	for _, table := range partitionedTables {
		for _, c := range s.retention.classes() {
			parent := table + c.suffix()
			parts, err := listPartitions(ctx, conn.Conn(), parent)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			cutoff := nowMs - c.retention.Milliseconds()
			for _, p := range parts {
				if p.hi > cutoff {
					continue
				}
				mode := "CONCURRENTLY"
				if p.detachPending {
					mode = "FINALIZE"
				}
				if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s %s`,
					pgx.Identifier{parent}.Sanitize(), pgx.Identifier{p.name}.Sanitize(), mode)); err != nil {
					errs = append(errs, fmt.Errorf("detach %s: %w", p.name, err))
					continue
				}
				if err := s.dropLeaf(ctx, conn, p.name); err != nil {
					if errors.Is(err, errDropLockTimeout) {
						timedOut[p.name] = true
						continue
					}
					errs = append(errs, fmt.Errorf("drop %s: %w", p.name, err))
					continue
				}
				s.metrics.dropped.WithLabelValues(table).Inc()
				s.logger.Debug("storage/postgres: dropped expired log partition", "partition", p.name)
			}
		}
	}

	if err := s.pruneChannels(ctx, conn, nowMs); err != nil {
		errs = append(errs, err)
	}

	// Leaves detached on an earlier sweep whose DROP failed.
	rows, err := conn.Query(ctx, `
		SELECT relname FROM pg_class
		WHERE relnamespace = current_schema()::regnamespace
		  AND relkind = 'r' AND NOT relispartition AND relname ~ $1`, leafNameRe)
	if err != nil {
		errs = append(errs, fmt.Errorf("list detached leaves: %w", err))
		return errors.Join(errs...)
	}
	orphans, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		errs = append(errs, fmt.Errorf("list detached leaves: %w", err))
		return errors.Join(errs...)
	}
	for _, name := range orphans {
		if timedOut[name] {
			continue
		}
		if err := s.dropLeaf(ctx, conn, name); err != nil {
			if !errors.Is(err, errDropLockTimeout) {
				errs = append(errs, fmt.Errorf("drop detached %s: %w", name, err))
			}
			continue
		}
		s.metrics.dropped.WithLabelValues(tableOfLeaf(name)).Inc()
	}
	return errors.Join(errs...)
}

// channelPruneChunk is the most channels rows one prune step deletes.
const channelPruneChunk = 1000

// sqlPruneChannels deletes up to $2 channels rows whose channel_serial
// (the serial of the channel's last publish, or the seed of a row never
// published on) was minted before the floor $1, a 14-digit mint-time
// prefix, and that have no presence row. FOR UPDATE SKIP LOCKED leaves a
// row another transaction holds (a publish in flight) alone, and a row
// advanced after the scan reads fails the predicate when re-checked under
// the lock. There is no index on channel_serial (adding one is a blocking
// build on a hot table), so a step that finds nothing scans the table.
const sqlPruneChannels = `
WITH victims AS (
	SELECT c.name FROM channels c
	WHERE c.channel_serial < $1
	  AND NOT EXISTS (SELECT 1 FROM presence p WHERE p.channel = c.name)
	LIMIT $2
	FOR UPDATE OF c SKIP LOCKED
)
DELETE FROM channels c USING victims v WHERE c.name = v.name`

// pruneChannels runs one chunk of the channels-table prune on the
// sweep's connection, so it runs under the retention drop lock
// (DESIGN.md §6.3). A channels row costs a few hundred bytes for every
// channel name ever used; once a channel has been idle for longer than
// the longest retention no cm of it survives, and the row, which only
// holds the serial the next publish continues from, is deleted. It runs
// at most one chunk per drop tick per node, bounding the load it adds.
func (s *Storage) pruneChannels(ctx context.Context, conn *pgxpool.Conn, nowMs int64) error {
	floor := fmt.Sprintf("%014d", nowMs-max(s.retention.Message, s.retention.Persisted).Milliseconds())
	tag, err := conn.Exec(ctx, sqlPruneChannels, floor, channelPruneChunk)
	if err != nil {
		return fmt.Errorf("prune channels: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		s.metrics.channelRowsDropped.Add(float64(n))
		s.logger.Debug("storage/postgres: pruned idle channels rows", "count", n)
	}
	return nil
}

// errDropLockTimeout reports a DROP TABLE abandoned on dropLockTimeout.
var errDropLockTimeout = errors.New("drop lock timeout")

// dropLeaf drops one detached leaf under lock_timeout = dropLockTimeout.
// The DROP needs ACCESS EXCLUSIVE, which queues behind any reader of the
// leaf and makes every later reader queue behind it; bounding the wait
// keeps a stuck reader from stalling the leaf's other readers. On timeout
// it returns errDropLockTimeout and leaves the leaf, now a detached
// orphan, for the next sweep's by-name pass.
func (s *Storage) dropLeaf(ctx context.Context, conn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}, name string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = %d`, dropLockTimeout.Milliseconds())); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DROP TABLE `+pgx.Identifier{name}.Sanitize()); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			s.metrics.dropTimeouts.Inc()
			s.logger.Debug("storage/postgres: drop of expired leaf timed out on its lock; leaving it for the next sweep", "partition", name)
			return errDropLockTimeout
		}
		return err
	}
	return tx.Commit(ctx)
}

// tableOfLeaf returns the partitioned table a leaf name belongs to.
func tableOfLeaf(name string) string {
	for _, t := range partitionedTables {
		if strings.HasPrefix(name, t+"_") {
			return t
		}
	}
	return ""
}

// observeLogSize refreshes the log size and leaf count gauges.
func (s *Storage) observeLogSize(ctx context.Context) {
	for _, table := range partitionedTables {
		var bytes, leaves int64
		if err := s.pool.QueryRow(ctx,
			`SELECT coalesce(sum(pg_total_relation_size(relid)), 0)::bigint, count(*)
			 FROM pg_partition_tree(to_regclass($1)) WHERE isleaf`, table,
		).Scan(&bytes, &leaves); err != nil {
			if ctx.Err() == nil {
				s.logger.Debug("storage/postgres: log size query failed", "table", table, "err", err)
			}
			continue
		}
		s.metrics.logBytes.WithLabelValues(table).Set(float64(bytes))
		s.metrics.leaves.WithLabelValues(table).Set(float64(leaves))
	}
}

// querier is the subset of pgx.Tx / *pgx.Conn that listPartitions needs.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// listPartitions returns the leaf partitions of parent with their bounds.
func listPartitions(ctx context.Context, q querier, parent string) ([]partition, error) {
	rows, err := q.Query(ctx, `
		SELECT c.relname, pg_get_expr(c.relpartbound, c.oid), i.inhdetachpending
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass($1)
		ORDER BY c.relname`, parent)
	if err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", parent, err)
	}
	defer rows.Close()
	var out []partition
	for rows.Next() {
		var (
			name    string
			bound   *string // NULL for a relation with no partition bound
			pending bool
		)
		if err := rows.Scan(&name, &bound, &pending); err != nil {
			return nil, fmt.Errorf("scan partition of %s: %w", parent, err)
		}
		if bound == nil {
			// pg_get_expr is NULL for a relation that has no bound
			// expression (for example one caught mid-attach or mid-detach).
			// It has no range to plan against or to drop, so skip it.
			continue
		}
		lo, hi, err := parseRangeBound(*bound)
		if err != nil {
			return nil, fmt.Errorf("partition %s: %w", name, err)
		}
		out = append(out, partition{name: name, lo: lo, hi: hi, detachPending: pending})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", parent, err)
	}
	return out, nil
}

var rangeBoundRe = regexp.MustCompile(`^FOR VALUES FROM \((.+)\) TO \((.+)\)$`)

// parseRangeBound decodes a leaf's pg_get_expr(relpartbound) text, e.g.
// FOR VALUES FROM ('01727700000000') TO ('01727700060000'), into the
// mint-time range in ms the serial bounds encode.
func parseRangeBound(bound string) (lo, hi int64, err error) {
	m := rangeBoundRe.FindStringSubmatch(bound)
	if m == nil {
		return 0, 0, fmt.Errorf("unrecognised range bound %q", bound)
	}
	if lo, err = parseBoundValue(m[1]); err != nil {
		return 0, 0, err
	}
	if hi, err = parseBoundValue(m[2]); err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

// parseBoundValue decodes one range bound: MINVALUE, MAXVALUE, or a
// quoted serial prefix whose first 14 characters are the mint time in ms.
func parseBoundValue(v string) (int64, error) {
	switch v {
	case "MINVALUE":
		return math.MinInt64, nil
	case "MAXVALUE":
		return math.MaxInt64, nil
	}
	if len(v) < 16 || v[0] != '\'' || v[len(v)-1] != '\'' {
		return 0, fmt.Errorf("unrecognised bound value %q", v)
	}
	ms, err := strconv.ParseInt(v[1:15], 10, 64)
	if err != nil {
		return 0, errors.Join(fmt.Errorf("bound value %q is not a serial time prefix", v), err)
	}
	return ms, nil
}

// planPartitions returns the [lo, hi) ms ranges of the leaves to create
// so that [floor(now), end) is fully covered, given the existing leaves.
// Coverage is extended to the next multiple of width at or after end.
// New leaves are aligned to multiples of width in epoch ms and never
// overlap an existing leaf: a slot partly covered (by a leaf of another
// width, or the unaligned legacy leaf a migrated table became) is clipped
// to the uncovered part. Gaps between existing leaves are filled too, so
// coverage does not depend on leaves having been created in order.
func planPartitions(existing []partition, nowMs, end, width int64) [][2]int64 {
	parts := slices.Clone(existing)
	slices.SortFunc(parts, func(a, b partition) int { return cmp.Compare(a.lo, b.lo) })
	var out [][2]int64
	fill := func(from, to int64) {
		for from < to {
			hi := min(floorTo(from, width)+width, to)
			out = append(out, [2]int64{from, hi})
			from = hi
		}
	}
	// Cover through the end of end's slot, so steady-state sweeps add
	// whole aligned leaves rather than slivers up to the lookahead.
	end = floorTo(end-1, width) + width
	cursor := floorTo(nowMs, width)
	for _, p := range parts {
		if cursor >= end {
			break
		}
		if p.hi <= cursor {
			continue
		}
		if p.lo > cursor {
			fill(cursor, min(p.lo, end))
		}
		cursor = max(cursor, p.hi)
	}
	fill(cursor, end)
	return out
}

// floorTo rounds ms down to a multiple of width.
func floorTo(ms, width int64) int64 {
	if ms >= 0 {
		return ms - ms%width
	}
	return ms - (width+ms%width)%width
}

// RetainedSince implements storage.RetentionBounded (DESIGN.md §4.3,
// §6.3). A leaf is dropped only once its whole range is older than the
// database's now minus the class retention, so every cm minted at or
// after that instant is still held. now is this node's clock, corrected
// by the offset measured at the last sweep plus a margin that errs
// towards refusing a resume. For a persisted channel the floor is at
// least the bound of any legacy live-class leaf (see Storage.legacyBound).
func (cs *channelStore) RetainedSince(now time.Time) string {
	floor := fmt.Sprintf("%014d", now.UnixMilli()+cs.clock.Load()-cs.retention.Milliseconds()+clockMargin.Milliseconds())
	if cs.persisted && cs.floorMin > floor {
		return cs.floorMin
	}
	return floor
}

// rangeFloor is the lower serial bound of the chain's log range reads (gap
// fill, catch-up, reconcile): the channel's retention floor, as
// RetainedSince reports it. Serials older than it are outside the
// retention the channel promises, so a read need not see them, and the
// bound lets the planner prune the leaves holding them: a read that
// scanned every leaf would hold ACCESS SHARE on a leaf a drop is waiting
// to take ACCESS EXCLUSIVE on, and every later reader would queue behind
// the drop (DESIGN.md §6.3). A channel store with no clock (a unit-test
// stub) reads from the beginning.
func (cs *channelStore) rangeFloor() string {
	if cs.clock == nil {
		return ""
	}
	return cs.RetainedSince(time.Now())
}

// idempotencyFloor is the lower serial bound of the idempotency lookup:
// the channel's retention window on the database clock, widened by the
// clock margin so the lookup never misses a row it should see.
func (cs *channelStore) idempotencyFloor() string {
	return fmt.Sprintf("%014d", time.Now().UnixMilli()+cs.clock.Load()-cs.retention.Milliseconds()-clockMargin.Milliseconds())
}
