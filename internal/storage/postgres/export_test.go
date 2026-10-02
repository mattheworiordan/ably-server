package postgres

import (
	"context"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// MaintainPartitionsAt runs one retention sweep as if the database clock
// read skew later than it does, so tests can age partitions out without
// waiting.
func (s *Storage) MaintainPartitionsAt(ctx context.Context, skew time.Duration) error {
	return s.maintainPartitions(ctx, skew)
}

// SetClockSkew makes this node read the database clock skew later than
// it does from now on (the retention floor, RetainedSince, moves with
// it), to pair with MaintainPartitionsAt: a test that ages partitions out
// with a skew also moves the floor the catch-up checks against.
func (s *Storage) SetClockSkew(d time.Duration) {
	old := s.clockSkew.Swap(d.Milliseconds())
	s.clockOffset.Add(d.Milliseconds() - old)
}

// MaintainCreateAt and MaintainDropAt run the two halves of a sweep on
// their own, as the retention loop's alternating ticks do.
func (s *Storage) MaintainCreateAt(ctx context.Context, skew time.Duration) error {
	return s.maintainCreate(ctx, skew)
}

func (s *Storage) MaintainDropAt(ctx context.Context, skew time.Duration) error {
	return s.maintainDrop(ctx, skew)
}

// DropLockTimeouts returns this node's
// ably_storage_partition_drop_lock_timeouts_total.
func (s *Storage) DropLockTimeouts() float64 {
	var m dto.Metric
	if err := s.metrics.dropTimeouts.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// SetDropLockTimeout overrides the lock_timeout of one leaf DROP and
// returns a restore func.
func SetDropLockTimeout(d time.Duration) func() {
	orig := dropLockTimeout
	dropLockTimeout = d
	return func() { dropLockTimeout = orig }
}

// LoadRangeAfter runs the chain's single-channel log range read from
// after.
func (s *Storage) LoadRangeAfter(ctx context.Context, channel, after string) (int, error) {
	cms, err := loadChannelMessagesAfter(ctx, s.pool, channel, after, "", rangePageSize)
	return len(cms), err
}

// ApplyMigrationForTest applies sql as migration version through the
// retrying runner, on one pooled connection.
func (s *Storage) ApplyMigrationForTest(ctx context.Context, version, sql string) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return applyMigrationWithRetry(ctx, conn, migration{version: version, sql: sql})
}

// MigrationLockTimeouts returns how many migration attempts lost a lock
// wait (55P03) in this process.
func MigrationLockTimeouts() int64 { return migrationLockTimeouts.Load() }

// SetMigrationTimings overrides the migration lock timeout and the pause
// between attempts and returns a restore func.
func SetMigrationTimings(lock, delay time.Duration) func() {
	origLock, origDelay := migrationLockTimeout, migrationRetryDelay
	migrationLockTimeout, migrationRetryDelay = lock, delay
	return func() { migrationLockTimeout, migrationRetryDelay = origLock, origDelay }
}

// MigrationAttempts is the number of attempts the runner makes.
const MigrationAttempts = migrationAttempts

// ChannelRowsDropped returns this node's
// ably_storage_channel_rows_dropped_total.
func (s *Storage) ChannelRowsDropped() float64 {
	var m dto.Metric
	if err := s.metrics.channelRowsDropped.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// PartitionsDropped returns this node's ably_storage_partitions_dropped_total
// for table.
func (s *Storage) PartitionsDropped(table string) float64 {
	var m dto.Metric
	if err := s.metrics.dropped.WithLabelValues(table).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// SetDetachTimeout overrides the bound on one DETACH ... CONCURRENTLY and
// returns a restore func.
func SetDetachTimeout(d time.Duration) func() {
	orig := detachTimeout
	detachTimeout = d
	return func() { detachTimeout = orig }
}

// SetCommitBatchHook installs a hook run at the start of every batch
// commit attempt (nil removes it).
func SetCommitBatchHook(h func() error) {
	if h == nil {
		commitBatchHook.Store(nil)
		return
	}
	commitBatchHook.Store(&h)
}

// SetCommitBatchAfterHook installs a hook run after a batch's COMMIT
// succeeded; an error makes the attempt fail as if the reply were lost
// (nil removes it).
func SetCommitBatchAfterHook(h func() error) {
	if h == nil {
		commitBatchAfterHook.Store(nil)
		return
	}
	commitBatchAfterHook.Store(&h)
}

// BatchCounters returns this node's batch commits, retries and
// deferrals.
func (s *Storage) BatchCounters() (commits, retries, deferred float64) {
	read := func(c interface{ Write(*dto.Metric) error }) float64 {
		var m dto.Metric
		if err := c.Write(&m); err != nil {
			return -1
		}
		return m.GetCounter().GetValue()
	}
	return read(s.wmetrics.commits), read(s.wmetrics.retries), read(s.wmetrics.deferred)
}

// BatchSizeSum returns the number of batches observed and the publishes
// they committed.
func (s *Storage) BatchSizeSum() (count uint64, sum float64) {
	var m dto.Metric
	if err := s.wmetrics.batchSize.Write(&m); err != nil {
		return 0, 0
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}
