package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// poolCollector exports the connection pool's counters as
// ably_storage_pool_* series (DESIGN.md §10), read from pgxpool.Stat on
// every scrape. A pool whose connections are all in use makes every
// storage call queue for one, including the presence seed read an
// attach's SYNC waits on (§12.4), so the empty-acquire wait shows a
// starved pool directly.
type poolCollector struct {
	pool *pgxpool.Pool

	maxConns, acquired, idle *prometheus.Desc
	acquires, emptyAcquires  *prometheus.Desc
	canceledAcquires         *prometheus.Desc
	acquireWait, emptyWait   *prometheus.Desc
}

func newPoolCollector(pool *pgxpool.Pool) *poolCollector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("ably_storage_pool_"+name, help, nil, nil)
	}
	return &poolCollector{
		pool:             pool,
		maxConns:         d("max_conns", "The Postgres connection pool's size."),
		acquired:         d("acquired_conns", "Pool connections currently in use."),
		idle:             d("idle_conns", "Pool connections currently idle."),
		acquires:         d("acquires_total", "Connections acquired from the pool."),
		emptyAcquires:    d("empty_acquires_total", "Acquires that found no idle connection and had to wait for one or for a new connection."),
		canceledAcquires: d("canceled_acquires_total", "Acquires abandoned because their context ended while waiting."),
		acquireWait:      d("acquire_seconds_total", "Total time spent acquiring connections, successful acquires only."),
		emptyWait:        d("empty_acquire_wait_seconds_total", "Total time acquires spent waiting because the pool had no idle connection."),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.maxConns, c.acquired, c.idle, c.acquires, c.emptyAcquires, c.canceledAcquires, c.acquireWait, c.emptyWait} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(st.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(st.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(st.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(st.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquires, prometheus.CounterValue, float64(st.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquires, prometheus.CounterValue, float64(st.CanceledAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireWait, prometheus.CounterValue, st.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(c.emptyWait, prometheus.CounterValue, st.EmptyAcquireWaitTime().Seconds())
}
