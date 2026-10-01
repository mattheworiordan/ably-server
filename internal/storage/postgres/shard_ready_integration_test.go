//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// shardReadyGauge reads ably_storage_shard_ready by shard index.
func shardReadyGauge(t *testing.T, s *Sharded) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(s.readyGauge)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "ably_storage_shard_ready" {
			continue
		}
		for _, m := range f.GetMetric() {
			out[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
		}
	}
	return out
}

// TestShardedReadinessSurvivesOneShardDown (DESIGN.md §6.4): with one of
// three shards unreachable the node stays ready, the down shard's gauge
// reads 0, and a publish, an attach or a history read on a channel of
// that shard fails fast with storage.ErrUnavailable (50003), batched or
// not, while the other shards' channels serve. With two of three down
// the node is not ready. Before, Ping failed on the first failing shard,
// so one shard down took every node out of rotation.
func TestShardedReadinessSurvivesOneShardDown(t *testing.T) {
	origConnect, origPing, origBind := poolConnectTimeout, poolPingTimeout, pgBindTimeout
	poolConnectTimeout, poolPingTimeout, pgBindTimeout = 300*time.Millisecond, 300*time.Millisecond, time.Second
	t.Cleanup(func() { poolConnectTimeout, poolPingTimeout, pgBindTimeout = origConnect, origPing, origBind })

	const n = 3
	schema := pgtest.NewSchemaName()
	proxies := make([]*pgtest.Proxy, n)
	dsns := make([]string, n)
	for i := range n {
		proxies[i], dsns[i] = pgtest.NewProxy(t, pgtest.StartShard(t, i).SchemaDSN(t, schema))
	}
	ctx := context.Background()
	nodes := map[string]*Sharded{
		"unbatched": openShardedT(t, Options{}, dsns),
		"batched":   openShardedT(t, Options{Batching: Batching{Lanes: 2}}, dsns),
	}
	// The postgres bus binds with a LISTEN on the shard's own LISTEN
	// connection, which is down with the shard. It is another cluster (a
	// cluster runs one bus), so it gets a schema of its own.
	pgSchema := pgtest.NewSchemaName()
	pgProxies := make([]*pgtest.Proxy, n)
	pgDSNs := make([]string, n)
	for i := range n {
		pgProxies[i], pgDSNs[i] = pgtest.NewProxy(t, pgtest.StartShard(t, i).SchemaDSN(t, pgSchema))
	}
	nodes["postgres-bus"] = openShardedT(t, Options{Bus: BusPostgres, NotifyMode: NotifyTransactional}, pgDSNs)
	names := namesOnEveryShard(t, "ready", 32, n)
	onShard := func(shard int) []string {
		var out []string
		for _, name := range names {
			if ShardFor(name, n) == shard {
				out = append(out, name)
			}
		}
		return out
	}
	stores := map[string]map[string]storage.ChannelStore{}
	for label, s := range nodes {
		stores[label] = map[string]storage.ChannelStore{}
		for _, shard := range []int{0, 1, 2} {
			name := onShard(shard)[0]
			cs, err := s.Channel(ctx, name, &cmRecorder{})
			if err != nil {
				t.Fatalf("%s: bind %s: %v", label, name, err)
			}
			if _, _, err := cs.Store(ctx, []*protocol.Message{{Data: "up"}}); err != nil {
				t.Fatalf("%s: publish on shard %d: %v", label, shard, err)
			}
			stores[label][name] = cs
		}
		if err := s.Ping(ctx); err != nil {
			t.Fatalf("%s: Ping with every shard up: %v", label, err)
		}
	}

	// Shard 2 becomes unreachable: connections hang rather than fail.
	proxies[2].Blackhole()
	pgProxies[2].Blackhole()
	// Let the pool's connections go idle long enough to be pinged on
	// acquire, as they would be on a real node between requests.
	time.Sleep(1500 * time.Millisecond)

	const bound = 3 * time.Second
	timed := func(what string, f func() error) {
		t.Helper()
		start := time.Now()
		err := f()
		if took := time.Since(start); took > bound {
			t.Errorf("%s took %s, want under %s", what, took, bound)
		}
		if !errors.Is(err, storage.ErrUnavailable) {
			t.Errorf("%s: err = %v, want storage.ErrUnavailable", what, err)
		}
	}
	for label, s := range nodes {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second) // the /readyz bound
		err := s.Ping(pctx)
		cancel()
		if err != nil {
			t.Errorf("%s: Ping with one of three shards down: %v", label, err)
		}
		if g := shardReadyGauge(t, s); g["0"] != 1 || g["1"] != 1 || g["2"] != 0 {
			t.Errorf("%s: ably_storage_shard_ready = %v, want 1, 1, 0", label, g)
		}
		down := onShard(2)
		timed(label+": publish on the down shard", func() error {
			_, _, err := stores[label][down[0]].Store(ctx, []*protocol.Message{{Data: "down"}})
			return err
		})
		timed(label+": attach on the down shard", func() error {
			_, err := s.Channel(ctx, down[1], &cmRecorder{})
			return err
		})
		timed(label+": history on the down shard", func() error {
			_, err := stores[label][down[0]].History(ctx, storage.HistoryQuery{Limit: 10})
			return err
		})
		for _, shard := range []int{0, 1} {
			if _, _, err := stores[label][onShard(shard)[0]].Store(ctx, []*protocol.Message{{Data: "still up"}}); err != nil {
				t.Errorf("%s: publish on healthy shard %d: %v", label, shard, err)
			}
		}
	}

	// Two of three down: no majority.
	proxies[1].Cut()
	pgProxies[1].Cut()
	for label, s := range nodes {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.Ping(pctx)
		cancel()
		if err == nil {
			t.Errorf("%s: Ping ready with two of three shards down", label)
		}
	}

	// Back up: ready again, every gauge 1, every shard serves.
	for _, p := range [][]*pgtest.Proxy{proxies, pgProxies} {
		p[1].Restore()
		p[2].Restore()
	}
	for label, s := range nodes {
		// Ping turns ready with two of three back; a shard's LISTEN
		// connection redials with backoff, so wait for every shard.
		var (
			err error
			g   map[string]float64
		)
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			err = s.Ping(ctx)
			if g = shardReadyGauge(t, s); err == nil && g["0"] == 1 && g["1"] == 1 && g["2"] == 1 {
				break
			}
		}
		if err != nil {
			t.Fatalf("%s: Ping after the shards came back: %v", label, err)
		}
		if g["0"] != 1 || g["1"] != 1 || g["2"] != 1 {
			t.Errorf("%s: ably_storage_shard_ready after recovery = %v, want all 1", label, g)
		}
		for name, cs := range stores[label] {
			if _, _, err := cs.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("back %s", name)}}); err != nil {
				t.Errorf("%s: publish on %s after recovery: %v", label, name, err)
			}
		}
	}
}

// TestShardedPingNeedsShardZero: shard 0 down takes the node out of
// rotation even though the other shards are a majority.
func TestShardedPingNeedsShardZero(t *testing.T) {
	const n = 3
	schema := pgtest.NewSchemaName()
	proxies := make([]*pgtest.Proxy, n)
	dsns := make([]string, n)
	for i := range n {
		proxies[i], dsns[i] = pgtest.NewProxy(t, pgtest.StartShard(t, i).SchemaDSN(t, schema))
	}
	s := openShardedT(t, Options{}, dsns)
	proxies[0].Cut()
	if err := s.Ping(context.Background()); err == nil {
		t.Error("Ping ready with shard 0 down")
	}
	if g := shardReadyGauge(t, s); g["0"] != 0 || g["1"] != 1 || g["2"] != 1 {
		t.Errorf("ably_storage_shard_ready = %v, want 0, 1, 1", g)
	}
	proxies[0].Restore()
}
