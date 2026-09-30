package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

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
}

var _ storage.Storage = (*Sharded)(nil)

// OpenSharded opens one Storage per DSN, in list order, with opts
// applied to each (opts.DSN is ignored). The shards open in parallel; if
// any fails, the ones that opened are closed and the first error is
// returned. Each shard's database records its place in the list on first
// open, and a later open with that database at another index, in a list
// of another length, is refused.
func OpenSharded(ctx context.Context, opts Options, dsns []string) (*Sharded, error) {
	n := len(dsns)
	if n < 2 {
		return nil, fmt.Errorf("storage/postgres: OpenSharded needs at least 2 DSNs, got %d (open one with Open)", n)
	}
	shards := make([]*Storage, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, dsn := range dsns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := opts
			o.DSN = dsn
			o.shard = shardSlot{index: i, count: n}
			if o.Logger != nil {
				o.Logger = o.Logger.With("shard", i)
			}
			s, err := Open(ctx, o)
			if err != nil {
				errs[i] = fmt.Errorf("shard %d: %w", i, err)
				return
			}
			shards[i] = s
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		for _, s := range shards {
			if s != nil {
				_ = s.Close()
			}
		}
		return nil, err
	}
	return &Sharded{shards: shards, gauge: shardsGauge(n)}, nil
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

// Release drops this node's binding of the channel on the shard that
// owns it (storage.Storage.Release).
func (s *Sharded) Release(ctx context.Context, name string) error {
	return s.shardOf(name).Release(ctx, name)
}

// Close closes every shard.
func (s *Sharded) Close() error {
	var wg sync.WaitGroup
	for _, sh := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sh.Close()
		}()
	}
	wg.Wait()
	return nil
}

// Ping reports ready only while every shard is: a node that cannot reach
// one shard cannot serve that shard's channels (storage.Pinger, /readyz).
func (s *Sharded) Ping(ctx context.Context) error {
	for i, sh := range s.shards {
		if err := sh.Ping(ctx); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

// Collectors returns every shard's ably_storage_* and ably_publish_*
// series, each labelled shard="<index>", plus ably_storage_shards.
func (s *Sharded) Collectors() []prometheus.Collector {
	out := []prometheus.Collector{s.gauge}
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
// The zero value is a lone Storage (index 0 of 1).
type shardSlot struct {
	index, count int
}

func (sl shardSlot) resolve() shardSlot {
	if sl.count < 1 {
		return shardSlot{index: 0, count: 1}
	}
	return sl
}

// shardsGauge is ably_storage_shards, the node's shard count.
func shardsGauge(n int) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "ably_storage_shards",
		Help: "Postgres shards this node stores channels in: the length of the --postgres-dsn list (DESIGN.md §6.4).",
	}, func() float64 { return float64(n) })
}

// shardIdentityLockKey namespaces the transaction advisory lock that
// serialises the first write of a database's shard identity; like the
// partition locks it is combined with hashtext(current_schema()).
const shardIdentityLockKey int32 = 0x1ab1e5e9

// checkShardIdentity makes sure the database behind pool is shard
// slot.index of a list of slot.count (DESIGN.md §6.4).
//
// A lone Storage (count 1) only reads: it refuses a database recorded as
// a shard of a list of two or more, and otherwise does nothing, so a
// single-DSN deployment creates no table and runs no write.
//
// A shard of a list records (index, count) in shard_identity on its
// first open and compares on every later one, so a node given the DSNs
// in another order, a list of another length, or one database twice
// fails to open instead of routing channels to the wrong database. A
// database that already holds channels but has no identity (it served a
// single-DSN deployment, or a list before identities were recorded) is
// refused: adopting it would strand the channels that now hash
// elsewhere, and resharding is not supported.
func checkShardIdentity(ctx context.Context, pool *pgxpool.Pool, slot shardSlot) error {
	if slot.count == 1 {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('shard_identity') IS NOT NULL`).Scan(&exists); err != nil {
			return fmt.Errorf("read shard identity: %w", err)
		}
		if !exists {
			return nil
		}
		var index, count int
		err := pool.QueryRow(ctx, `SELECT shard_index, shard_count FROM shard_identity`).Scan(&index, &count)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read shard identity: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("this database is shard %d of %d; list all %d DSNs in --postgres-dsn, in the original order (resharding is not supported, DESIGN.md §6.4)", index, count, count)
		}
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shard identity: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))`, shardIdentityLockKey); err != nil {
		return fmt.Errorf("shard identity lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS shard_identity (
		one         BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
		shard_index INT NOT NULL,
		shard_count INT NOT NULL,
		created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create shard identity: %w", err)
	}
	var index, count int
	err = tx.QueryRow(ctx, `SELECT shard_index, shard_count FROM shard_identity`).Scan(&index, &count)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		var used bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM channels)`).Scan(&used); err != nil {
			return fmt.Errorf("shard identity: %w", err)
		}
		if used {
			return fmt.Errorf("this database already holds channels but is not recorded as a shard; it cannot join a list of %d (resharding is not supported, DESIGN.md §6.4)", slot.count)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO shard_identity (shard_index, shard_count) VALUES ($1, $2)`, slot.index, slot.count); err != nil {
			return fmt.Errorf("record shard identity: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read shard identity: %w", err)
	case index != slot.index || count != slot.count:
		return fmt.Errorf("this database is shard %d of %d, but --postgres-dsn lists it as shard %d of %d; the list must keep its original order and length (resharding is not supported, DESIGN.md §6.4)", index, count, slot.index, slot.count)
	}
	return tx.Commit(ctx)
}
