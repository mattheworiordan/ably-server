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
