package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

// Sharded is the cluster-mode storage.Storage over a fixed list of
// Postgres databases (DESIGN.md §6.4). Each shard is a full Storage, with
// its own pool, migrations, retention sweep, presence lease and reaper,
// publish lanes and bus connection, so everything a Storage does per
// database runs once per shard. A channel belongs to shard
// ShardFor(name, n): Channel and Release route to it, and every per-
// channel operation then runs on that shard's ChannelStore. The
// account-wide pieces (readiness, bus counters, metrics, Close) fan out
// over every shard.
//
// There is no resharding, no data migration between shards and no
// rebalancing: the list is fixed for the life of the data, and
// OpenSharded refuses a list whose order or length differs from the one
// each database was first opened with.
type Sharded struct {
	shards []*Storage
	gauge  prometheus.Collector // ably_storage_shards

	// ready is each shard's result in the last Ping (1 reachable, 0 not),
	// exported as ably_storage_shard_ready{shard}; every shard starts
	// ready, since OpenSharded only returns once all of them opened.
	ready      []atomic.Bool
	readyGauge prometheus.Collector
	lapse      *lapseNotifier // shared by every shard: one re-entry per outage (§12.5)
}

var (
	_ storage.Storage          = (*Sharded)(nil)
	_ storage.UnboundPublisher = (*Sharded)(nil)
	_ storage.UnboundPublisher = (*Storage)(nil)
)

// OpenSharded opens one Storage per DSN, in list order, with opts
// applied to each (opts.DSN is ignored). Shard 0 opens first, then the
// others in parallel. If any fails, the ones that opened are closed and
// every shard's error is returned, joined.
//
// Each shard's database records its place in the list on first open
// (checkShardIdentity), and shard 0 records which database is at every
// position once all of them have opened (checkShardMembers). A later open
// with the list in another order, of another length, with one database
// twice, or with another database at some position is refused. A shard
// that opened during a failed attempt keeps what it recorded (DESIGN.md
// §6.4 says how to reset it).
func OpenSharded(ctx context.Context, opts Options, dsns []string) (*Sharded, error) {
	n := len(dsns)
	if n < 2 {
		return nil, fmt.Errorf("storage/postgres: OpenSharded needs at least 2 DSNs, got %d (open one with Open)", n)
	}
	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}
	if opts.nodeID == "" {
		// One node id on every shard: the node's presence lease row lives
		// in each shard's presence_nodes, beside the members it owns there
		// (DESIGN.md §6.4, §12.5).
		opts.nodeID = serial.NewSeriesID()
	}
	if opts.catchUpSlots == nil {
		// One bound on the node's batched catch-up queries, shared by
		// every shard (chain.go, DESIGN.md §7.2).
		opts.catchUpSlots = make(chan struct{}, catchUpConcurrency)
	}
	// One lapse notifier for every shard: an outage that lapses the
	// node's lease on several shards re-enters its members once
	// (DESIGN.md §12.5).
	lapse := newLapseNotifier(opts.OnPresenceLeaseLapse, logger)
	opts.lapse = lapse
	open := func(i int, listID, deploymentID string) (*Storage, error) {
		o := opts
		o.DSN = dsns[i]
		o.shard = shardSlot{index: i, count: n, listID: listID, deploymentID: deploymentID}
		o.Logger = logger.With("shard", i)
		s, err := Open(ctx, o)
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		return s, nil
	}

	shards := make([]*Storage, n)
	closeAll := func() {
		lapse.stop()
		for _, s := range shards {
			if s != nil {
				_ = s.Close()
			}
		}
	}
	first, err := open(0, "", "")
	if err != nil {
		lapse.stop()
		return nil, err
	}
	shards[0] = first
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shards[i], errs[i] = open(i, first.ident.listID, first.deploymentID)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		closeAll()
		return nil, err
	}
	members := make([]string, n)
	for i, sh := range shards {
		members[i] = sh.ident.memberID
	}
	if err := checkShardMembers(ctx, first.pool, members); err != nil {
		closeAll()
		return nil, fmt.Errorf("storage/postgres: %w", err)
	}
	sh := &Sharded{shards: shards, gauge: shardsGauge(n), ready: make([]atomic.Bool, n), lapse: lapse}
	for i := range sh.ready {
		sh.ready[i].Store(true)
	}
	sh.readyGauge = newShardReadyCollector(sh.ready)
	return sh, nil
}

// Shards is the number of shards (the DSN list's length).
func (s *Sharded) Shards() int { return len(s.shards) }

// Shard returns shard i, for tests and diagnostics.
func (s *Sharded) Shard(i int) *Storage { return s.shards[i] }

// shardOf returns the shard that owns channel name.
func (s *Sharded) shardOf(name string) *Storage {
	return s.shards[ShardFor(name, len(s.shards))]
}

// Channel returns the channel's ChannelStore from the shard that owns it
// (storage.Storage.Channel).
func (s *Sharded) Channel(ctx context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	return s.shardOf(name).Channel(ctx, name, appender)
}

// UnboundChannel returns an unbound store for the channel from the shard
// that owns it (storage.UnboundPublisher), so a write-only publish routes
// by channel hash like everything else.
func (s *Sharded) UnboundChannel(name string) storage.ChannelStore {
	return s.shardOf(name).UnboundChannel(name)
}

// Release drops this node's binding of the channel on the shard that
// owns it (storage.Storage.Release).
func (s *Sharded) Release(ctx context.Context, name string) error {
	return s.shardOf(name).Release(ctx, name)
}

// Close closes every shard.
func (s *Sharded) Close() error {
	s.lapse.stop()
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sh.Close(); err != nil {
				errs[i] = fmt.Errorf("shard %d: %w", i, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Ping pings every shard (its pool and its bus, Storage.Ping) at once and
// reports ready while shard 0 and a majority of the shards are reachable
// (storage.Pinger, /readyz; DESIGN.md §6.4). One shard down does not take
// the node out of rotation: if it did, every node would leave at once
// and the healthy shards' channels would become unreachable too. The
// down shard's channels fail fast instead (ErrUnavailable, 50003) while
// the rest serve. A node that reaches no more than half the shards, or
// not shard 0, which is where it checks the list it was given, leaves
// rotation. Each shard's result is ably_storage_shard_ready{shard}.
func (s *Sharded) Ping(ctx context.Context) error {
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sh.Ping(ctx)
		}()
	}
	wg.Wait()
	up := 0
	var down []error
	for i, err := range errs {
		s.ready[i].Store(err == nil)
		if err == nil {
			up++
			continue
		}
		down = append(down, fmt.Errorf("shard %d: %w", i, err))
	}
	switch {
	case errs[0] != nil:
		return fmt.Errorf("storage/postgres: shard 0 unreachable (%d of %d shards up): %w", up, len(s.shards), errors.Join(down...))
	case 2*up <= len(s.shards):
		return fmt.Errorf("storage/postgres: only %d of %d shards reachable, not a majority: %w", up, len(s.shards), errors.Join(down...))
	}
	return nil
}

// newShardReadyCollector is ably_storage_shard_ready{shard}: 1 while the
// shard was reachable in the last readiness check, else 0 (DESIGN.md
// §6.4, §10).
func newShardReadyCollector(ready []atomic.Bool) prometheus.Collector {
	vec := &shardReadyCollector{
		desc: prometheus.NewDesc("ably_storage_shard_ready",
			"1 while this Postgres shard (and its bus connection) answered the node's last readiness check (/readyz), else 0 (DESIGN.md §6.4).",
			[]string{"shard"}, nil),
		ready: ready,
	}
	return vec
}

type shardReadyCollector struct {
	desc  *prometheus.Desc
	ready []atomic.Bool
}

func (c *shardReadyCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *shardReadyCollector) Collect(ch chan<- prometheus.Metric) {
	for i := range c.ready {
		v := 0.0
		if c.ready[i].Load() {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, v, strconv.Itoa(i))
	}
}

// Collectors returns every shard's ably_storage_* and ably_publish_*
// series, each labelled shard="<index>", plus ably_storage_shards and
// ably_storage_shard_ready.
func (s *Sharded) Collectors() []prometheus.Collector {
	out := []prometheus.Collector{s.gauge, s.readyGauge}
	for i, sh := range s.shards {
		labels := prometheus.Labels{"shard": strconv.Itoa(i)}
		for _, c := range sh.Collectors() {
			out = append(out, prometheus.WrapCollectorWith(labels, c))
		}
	}
	return out
}

// BusStats sums the shards' bus counters (storage.BusStatser): every
// shard has its own bus connection, and the ably_bus_* series report the
// node as a whole. Connected is true only while every shard's bus is up.
func (s *Sharded) BusStats() storage.BusStats {
	out := s.shards[0].BusStats()
	for _, sh := range s.shards[1:] {
		out = addBusStats(out, sh.BusStats())
	}
	return out
}

// addBusStats returns a with every counter and gauge of b added to it.
func addBusStats(a, b storage.BusStats) storage.BusStats {
	a.Connected = a.Connected && b.Connected
	a.BoundChannels += b.BoundChannels
	a.ReceiveQueueDepth += b.ReceiveQueueDepth
	a.Published += b.Published
	a.PublishErrors += b.PublishErrors
	a.Pointers += b.Pointers
	a.Received += b.Received
	a.Unrouted += b.Unrouted
	a.Malformed += b.Malformed
	a.UnroutedForeign += b.UnroutedForeign
	a.MalformedSerial += b.MalformedSerial
	a.MalformedFuture += b.MalformedFuture
	a.Inline += b.Inline
	a.Fetched += b.Fetched
	a.FastPath += b.FastPath
	a.Filled += b.Filled
	a.Duplicates += b.Duplicates
	a.Held += b.Held
	a.GapFills += b.GapFills
	a.FetchErrors += b.FetchErrors
	a.ReconcileRuns += b.ReconcileRuns
	a.Reconciles += b.Reconciles
	a.ReconcileSeconds += b.ReconcileSeconds
	a.Sweeps += b.Sweeps
	a.SweepChannels += b.SweepChannels
	a.SweepCatchUps += b.SweepCatchUps
	a.SweepSeconds += b.SweepSeconds
	a.Drops += b.Drops
	a.Listens += b.Listens
	a.Unlistens += b.Unlistens
	a.WakeupsSent += b.WakeupsSent
	a.WakeupsReceived += b.WakeupsReceived
	a.Flushes += b.Flushes
	a.FlushErrors += b.FlushErrors
	a.Overflow += b.Overflow
	a.FlushSeconds += b.FlushSeconds
	if len(b.DeliveryLag) > 0 {
		merged := make(map[string]storage.LagHistogram, len(a.DeliveryLag)+len(b.DeliveryLag))
		for path, h := range a.DeliveryLag {
			merged[path] = h
		}
		for path, h := range b.DeliveryLag {
			merged[path] = addLag(merged[path], h)
		}
		a.DeliveryLag = merged
	}
	if len(b.Stages) > 0 {
		merged := make(map[string]storage.LagHistogram, len(a.Stages)+len(b.Stages))
		for stage, h := range a.Stages {
			merged[stage] = h
		}
		for stage, h := range b.Stages {
			merged[stage] = addLag(merged[stage], h)
		}
		a.Stages = merged
	}
	return a
}

// addLag sums two snapshots of one lag histogram (same buckets).
func addLag(a, b storage.LagHistogram) storage.LagHistogram {
	counts := make([]uint64, max(len(a.Counts), len(b.Counts)))
	for i := range counts {
		if i < len(a.Counts) {
			counts[i] += a.Counts[i]
		}
		if i < len(b.Counts) {
			counts[i] += b.Counts[i]
		}
	}
	return storage.LagHistogram{Counts: counts, Count: a.Count + b.Count, Sum: a.Sum + b.Sum}
}

// shardSlot is a Storage's place in a shard list: index in [0, count).
// listID is shard 0's list id, which every other shard of the list must
// carry; it is empty for shard 0 and for a lone Storage. The zero value
// is a lone Storage (index 0 of 1).
type shardSlot struct {
	index, count int
	listID       string
	// deploymentID is shard 0's cluster deployment id (DESIGN.md §11),
	// which every other shard records; empty for shard 0 and a lone
	// Storage, which mint or read their own.
	deploymentID string
}

func (sl shardSlot) resolve() shardSlot {
	if sl.count < 1 {
		return shardSlot{index: 0, count: 1}
	}
	return sl
}

// shardIdentity is what a shard's database recorded about it: the id of
// the list it belongs to (minted by shard 0) and its own random id.
type shardIdentity struct {
	listID, memberID string
}

// shardsGauge is ably_storage_shards, the node's shard count.
func shardsGauge(n int) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "ably_storage_shards",
		Help: "Postgres shards this node stores channels in: the length of the --postgres-dsn list (DESIGN.md §6.4).",
	}, func() float64 { return float64(n) })
}

// shardIdentityLockKey namespaces the transaction advisory lock that
// serialises writes of a database's shard identity; like the partition
// locks it is combined with hashtext(current_schema()).
const shardIdentityLockKey int32 = 0x1ab1e5e9

// shardIdentityTable returns the schema-qualified shard_identity name in
// the connection's current schema, where the multi-shard path creates it,
// so a lone Storage never reads another schema's table on its
// search_path.
func shardIdentityTable(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (string, error) {
	var schema string
	if err := q.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return "", fmt.Errorf("current_schema: %w", err)
	}
	return pgx.Identifier{schema, "shard_identity"}.Sanitize(), nil
}

// reshardHint ends every identity refusal.
const reshardHint = "every node must list the same DSNs in the same order, and the list is fixed for the life of the data (resharding is not supported, DESIGN.md §6.4)"

// checkShardIdentity makes sure the database behind pool is shard
// slot.index of a list of slot.count (DESIGN.md §6.4), and returns the
// identity it holds.
//
// A lone Storage (count 1) only reads: it refuses a database recorded as
// a shard of a list of two or more, and otherwise does nothing, so a
// single-DSN deployment creates no table and runs no write.
//
// A shard of a list records its index, the list length, the list id and
// a random member id in shard_identity on its first open, and compares
// on every later one. Shard 0 mints the list id; every other shard must
// be given it (slot.listID), so a database that already belongs to
// another list is refused. A database that already holds channels but
// has no identity (it served a single-DSN deployment) is refused:
// adopting it would strand the channels that now hash elsewhere.
func checkShardIdentity(ctx context.Context, pool *pgxpool.Pool, slot shardSlot) (shardIdentity, error) {
	if slot.count == 1 {
		tbl, err := shardIdentityTable(ctx, pool)
		if err != nil {
			return shardIdentity{}, fmt.Errorf("read shard identity: %w", err)
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&exists); err != nil {
			return shardIdentity{}, fmt.Errorf("read shard identity: %w", err)
		}
		if !exists {
			return shardIdentity{}, nil
		}
		var index, count int
		err = pool.QueryRow(ctx, `SELECT shard_index, shard_count FROM `+tbl).Scan(&index, &count)
		if errors.Is(err, pgx.ErrNoRows) {
			return shardIdentity{}, nil
		}
		if err != nil {
			return shardIdentity{}, fmt.Errorf("read shard identity: %w", err)
		}
		return shardIdentity{}, fmt.Errorf("this database is shard %d of %d; list all %d DSNs in --postgres-dsn; %s", index, count, count, reshardHint)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return shardIdentity{}, fmt.Errorf("shard identity: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))`, shardIdentityLockKey); err != nil {
		return shardIdentity{}, fmt.Errorf("shard identity lock: %w", err)
	}
	tbl, err := shardIdentityTable(ctx, tx)
	if err != nil {
		return shardIdentity{}, fmt.Errorf("shard identity: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+tbl+` (
		one         BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
		shard_index INT NOT NULL,
		shard_count INT NOT NULL,
		list_id     TEXT NOT NULL,
		member_id   TEXT NOT NULL,
		members     TEXT[],
		created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return shardIdentity{}, fmt.Errorf("create shard identity: %w", err)
	}
	var (
		index, count int
		id           shardIdentity
	)
	err = tx.QueryRow(ctx, `SELECT shard_index, shard_count, list_id, member_id FROM `+tbl).Scan(&index, &count, &id.listID, &id.memberID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		var used bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM channels)`).Scan(&used); err != nil {
			return shardIdentity{}, fmt.Errorf("shard identity: %w", err)
		}
		if used {
			return shardIdentity{}, fmt.Errorf("this database already holds channels but is not recorded as a shard, so it cannot join a list of %d; %s", slot.count, reshardHint)
		}
		id = shardIdentity{listID: slot.listID, memberID: rand.Text()}
		if slot.index == 0 {
			id.listID = rand.Text()
		} else if id.listID == "" {
			return shardIdentity{}, errors.New("shard identity: no list id for a shard after the first")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+tbl+` (shard_index, shard_count, list_id, member_id) VALUES ($1, $2, $3, $4)`,
			slot.index, slot.count, id.listID, id.memberID); err != nil {
			return shardIdentity{}, fmt.Errorf("record shard identity: %w", err)
		}
	case err != nil:
		return shardIdentity{}, fmt.Errorf("read shard identity: %w", err)
	case index != slot.index || count != slot.count:
		return shardIdentity{}, fmt.Errorf("this database is shard %d of %d, but --postgres-dsn lists it as shard %d of %d (or lists it twice); %s", index, count, slot.index, slot.count, reshardHint)
	case slot.index > 0 && id.listID != slot.listID:
		return shardIdentity{}, fmt.Errorf("this database is shard %d of %d of another list (whose shard 0 is not the database listed first); %s", index, count, reshardHint)
	}
	if err := tx.Commit(ctx); err != nil {
		return shardIdentity{}, fmt.Errorf("record shard identity: %w", err)
	}
	return id, nil
}

// checkShardMembers records on shard 0 which database is at every
// position of the list (each shard's member id, in list order) the first
// time every shard has opened, and compares on every later open. It
// catches what the per-shard check cannot: a database with no identity
// yet (an empty one, say) listed in place of one that has served the
// list, which would otherwise record itself and split that position's
// channels between two databases.
func checkShardMembers(ctx context.Context, pool *pgxpool.Pool, members []string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shard members: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))`, shardIdentityLockKey); err != nil {
		return fmt.Errorf("shard members lock: %w", err)
	}
	tbl, err := shardIdentityTable(ctx, tx)
	if err != nil {
		return fmt.Errorf("shard members: %w", err)
	}
	var recorded []string
	if err := tx.QueryRow(ctx, `SELECT members FROM `+tbl).Scan(&recorded); err != nil {
		return fmt.Errorf("read shard members: %w", err)
	}
	if recorded == nil {
		if _, err := tx.Exec(ctx, `UPDATE `+tbl+` SET members = $1`, members); err != nil {
			return fmt.Errorf("record shard members: %w", err)
		}
		return tx.Commit(ctx)
	}
	if len(recorded) != len(members) {
		return fmt.Errorf("shard 0 records a list of %d databases, not %d; %s", len(recorded), len(members), reshardHint)
	}
	for i := range members {
		if recorded[i] != members[i] {
			return fmt.Errorf("shard %d is not the database this list was first opened with at that position; %s", i, reshardHint)
		}
	}
	return nil
}
