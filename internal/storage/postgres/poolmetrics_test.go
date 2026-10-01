package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// TestPoolCollector: the pool's size and counters are exported as
// ably_storage_pool_* series (DESIGN.md §10). The pool connects lazily,
// so no server is needed to read its stats.
func TestPoolCollector(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://ably@127.0.0.1:1/ably?pool_max_conns=7")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(newPoolCollector(pool))
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 8 {
		t.Errorf("series = %d, want 8", len(families))
	}
	var maxConns float64 = -1
	for _, f := range families {
		if f.GetName() == "ably_storage_pool_max_conns" {
			maxConns = f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	if maxConns != 7 {
		t.Errorf("ably_storage_pool_max_conns = %v, want 7", maxConns)
	}
}
