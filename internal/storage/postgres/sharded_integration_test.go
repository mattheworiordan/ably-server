//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// Channel sharding tests (DESIGN.md §6.4). Every shard is its own
// Postgres container, and every shard's database uses the same schema
// name, as a real deployment's databases would ("public" on each).

// shardDSNs returns one DSN per shard for n shards: a fresh schema of
// one name on each of n containers.
func shardDSNs(t *testing.T, n int) []string {
	t.Helper()
	schema := pgtest.NewSchemaName()
	out := make([]string, n)
	for i := range out {
		out[i] = pgtest.StartShard(t, i).SchemaDSN(t, schema)
	}
	return out
}

func openShardedT(t *testing.T, o Options, dsns []string) *Sharded {
	t.Helper()
	s, err := OpenSharded(context.Background(), o, dsns)
	if err != nil {
		t.Fatalf("OpenSharded (%s): %v", o.Bus, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// shardedBuses returns, per bus, the Options a sharded node opens with
// (no DSN: OpenSharded sets one per shard).
func shardedBuses(t *testing.T) map[string]Options {
	t.Helper()
	return map[string]Options{
		"pgnotify": {},
		"postgres-coalesced": {
			Bus: BusPostgres, NotifyMode: NotifyCoalesced,
			NotifyWindow: 5 * time.Millisecond, SweepInterval: 200 * time.Millisecond,
		},
		"nats": {Bus: BusNATS, NATSURL: natstest.Start(t).URL},
	}
}

// namesOnEveryShard returns count channel names, prefix-numbered, that
// between them land on every one of n shards.
func namesOnEveryShard(t *testing.T, prefix string, count, n int) []string {
	t.Helper()
	names := make([]string, count)
	seen := make(map[int]bool)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", prefix, i)
		seen[ShardFor(names[i], n)] = true
	}
	if len(seen) != n {
		t.Fatalf("%d names with prefix %q cover shards %v of %d; pick more", count, prefix, seen, n)
	}
	return names
}

// TestShardedChannelStoreContractEveryBus runs the storage contract suite
// against a two-shard store on each bus: sharding changes no storage
// semantics.
func TestShardedChannelStoreContractEveryBus(t *testing.T) {
	for name, o := range shardedBuses(t) {
		t.Run(name, func(t *testing.T) {
			storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
				return openShardedT(t, o, shardDSNs(t, 2))
			})
		})
	}
}

// TestShardedCrossNodeDeliveryEveryBus: two nodes, each with two shards
// and publish batching on, publish concurrently to channels spread over
// both shards. Every node's appender for every channel must receive the
// channel's cms exactly once, in log order.
func TestShardedCrossNodeDeliveryEveryBus(t *testing.T) {
	ctx := context.Background()
	for name, o := range shardedBuses(t) {
		t.Run(name, func(t *testing.T) {
			o.Batching = Batching{Lanes: 4}
			dsns := shardDSNs(t, 2)
			nodes := []*Sharded{openShardedT(t, o, dsns), openShardedT(t, o, dsns)}
			channels := namesOnEveryShard(t, "xnode", 8, 2)
			apps := map[string][]*orderAppender{}
			stores := map[string][]storage.ChannelStore{}
			for _, ch := range channels {
				for _, n := range nodes {
					app := &orderAppender{}
					cs, err := n.Channel(ctx, ch, app)
					if err != nil {
						t.Fatalf("Channel %s: %v", ch, err)
					}
					apps[ch] = append(apps[ch], app)
					stores[ch] = append(stores[ch], cs)
				}
			}

			const writers, each = 8, 25
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range each {
						ch := channels[(w+i)%len(channels)]
						if _, _, err := stores[ch][w%2].Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("%d-%d", w, i)}}); err != nil {
							t.Errorf("Store %s: %v", ch, err)
							return
						}
					}
				}()
			}
			wg.Wait()

			total := 0
			for _, ch := range channels {
				page, err := stores[ch][0].History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 1000})
				if err != nil {
					t.Fatalf("History %s: %v", ch, err)
				}
				var want []string
				for _, cm := range page.ChannelMessages {
					want = append(want, cm.ChannelSerial)
				}
				total += len(want)
				for i, app := range apps[ch] {
					deadline := time.Now().Add(10 * time.Second)
					for {
						got, _ := app.snapshot()
						if len(got) >= len(want) || time.Now().After(deadline) {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					got, bad := app.snapshot()
					if len(bad) > 0 {
						t.Errorf("%s node %d: out of order or duplicate: %v", ch, i, bad)
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s node %d: delivered %d cms, log has %d (delivered %v, log %v)", ch, i, len(got), len(want), got, want)
					}
				}
			}
			if total != writers*each {
				t.Errorf("logs hold %d cms across channels, want %d", total, writers*each)
			}
			assertOnOwningShard(t, dsns, channels)
			for i, n := range nodes {
				if st := n.BusStats(); st.BoundChannels != len(channels) || !st.Connected {
					t.Errorf("node %d BusStats: bound %d (want %d), connected %v", i, st.BoundChannels, len(channels), st.Connected)
				}
			}
		})
	}
}

// shardTables are the per-channel tables a channel's data can land in,
// with the column that names the channel.
var shardTables = map[string]string{"channels": "name", "channel_messages": "channel", "messages": "channel", "presence": "channel"}

// assertOnOwningShard checks that every table of every shard holds rows
// only for channels that hash to that shard, and that each channel in
// channels has its channels row on its own shard.
func assertOnOwningShard(t *testing.T, dsns []string, channels []string) {
	t.Helper()
	ctx := context.Background()
	for i, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		for table, col := range shardTables {
			rows, err := conn.Query(ctx, `SELECT DISTINCT `+col+` FROM `+table)
			if err != nil {
				t.Fatalf("shard %d %s: %v", i, table, err)
			}
			names, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				t.Fatalf("shard %d %s: %v", i, table, err)
			}
			for _, name := range names {
				if owner := ShardFor(name, len(dsns)); owner != i {
					t.Errorf("shard %d table %s holds channel %q, which belongs to shard %d", i, table, name, owner)
				}
			}
		}
		for _, ch := range channels {
			var n int
			if err := conn.QueryRow(ctx, `SELECT count(*) FROM channels WHERE name = $1`, ch).Scan(&n); err != nil {
				t.Fatalf("shard %d channels row %s: %v", i, ch, err)
			}
			if want := ShardFor(ch, len(dsns)) == i; want != (n == 1) {
				t.Errorf("shard %d has %d channels rows for %q (owner shard %d)", i, n, ch, ShardFor(ch, len(dsns)))
			}
		}
		conn.Close(ctx)
	}
}

// TestShardedDataLandsOnItsShard writes every kind of per-channel state
// (a message, an update, an annotation, a presence member) on channels
// across two shards, and checks each channel's rows exist only in its
// shard's database while reads through the store see all of it.
func TestShardedDataLandsOnItsShard(t *testing.T) {
	ctx := context.Background()
	dsns := shardDSNs(t, 2)
	s := openShardedT(t, Options{}, dsns)
	channels := namesOnEveryShard(t, "land", 24, 2)
	for _, ch := range channels {
		cs, err := s.Channel(ctx, ch, nil)
		if err != nil {
			t.Fatalf("Channel %s: %v", ch, err)
		}
		cm, _, err := cs.Store(ctx, []*protocol.Message{{Name: "m", Data: "v1"}})
		if err != nil {
			t.Fatalf("Store %s: %v", ch, err)
		}
		serial := cm.Messages[0].Serial
		if _, _, err := cs.Mutate(ctx, &protocol.Message{Action: protocol.MessageUpdate, Serial: serial, Data: "v2", ClientID: "alice"}); err != nil {
			t.Fatalf("Mutate %s: %v", ch, err)
		}
		if _, _, err := cs.StoreAnnotation(ctx, []*protocol.Annotation{
			{Action: protocol.AnnotationCreate, ClientID: "bob", Type: "reaction:distinct.v1", Name: "+1", MessageSerial: serial},
		}); err != nil {
			t.Fatalf("StoreAnnotation %s: %v", ch, err)
		}
		if _, _, err := cs.StorePresence(ctx, []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "c1", ConnectionID: "conn1"}}); err != nil {
			t.Fatalf("StorePresence %s: %v", ch, err)
		}
		latest, err := cs.LatestVersion(ctx, serial)
		if err != nil || latest.Data != "v2" {
			t.Fatalf("LatestVersion %s = %+v, %v; want v2", ch, latest, err)
		}
		members, _, err := cs.Members(ctx)
		if err != nil || len(members) != 1 {
			t.Fatalf("Members %s = %d, %v; want 1", ch, len(members), err)
		}
	}
	assertOnOwningShard(t, dsns, channels)

	// Each shard holds a share, not all of it.
	for i, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(DISTINCT channel) FROM channel_messages WHERE kind = 'annotation'`).Scan(&n); err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		conn.Close(ctx)
		if n == 0 || n == len(channels) {
			t.Errorf("shard %d holds annotations for %d of %d channels, want a share", i, n, len(channels))
		}
	}
}

// TestShardedRetentionRunsOnEveryShard checks the retention sweep is per
// shard: each shard's own loop recreates a leaf partition removed from
// its database, and aged-out leaves are dropped on each shard.
func TestShardedRetentionRunsOnEveryShard(t *testing.T) {
	ctx := context.Background()
	dsns := shardDSNs(t, 2)
	s := openShardedT(t, Options{Retention: Retention{SweepInterval: 100 * time.Millisecond}}, dsns)

	// Detach and drop the newest live leaf of the log on every shard; each
	// shard's retention loop must create it again.
	removed := make([]string, len(dsns))
	for i, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		if err := conn.QueryRow(ctx, `SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = 'channel_messages_live' AND p.relnamespace = current_schema()::regnamespace
			ORDER BY c.relname DESC LIMIT 1`).Scan(&removed[i]); err != nil {
			t.Fatalf("shard %d newest leaf: %v", i, err)
		}
		if _, err := conn.Exec(ctx, `ALTER TABLE channel_messages_live DETACH PARTITION `+removed[i]); err != nil {
			t.Fatalf("shard %d detach %s: %v", i, removed[i], err)
		}
		if _, err := conn.Exec(ctx, `DROP TABLE `+removed[i]); err != nil {
			t.Fatalf("shard %d drop %s: %v", i, removed[i], err)
		}
		conn.Close(ctx)
	}
	for i, dsn := range dsns {
		deadline := time.Now().Add(10 * time.Second)
		for !relationPresent(t, dsn, removed[i]) {
			if time.Now().After(deadline) {
				t.Fatalf("shard %d: leaf %s not recreated by the retention loop within 10s", i, removed[i])
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Aged out: publish on channels of both shards, sweep every shard as if
	// ten minutes later, and every shard's live rows are gone.
	channels := namesOnEveryShard(t, "ret", 8, 2)
	for _, ch := range channels {
		cs, err := s.Channel(ctx, ch, nil)
		if err != nil {
			t.Fatalf("Channel %s: %v", ch, err)
		}
		if _, _, err := cs.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
			t.Fatalf("Store %s: %v", ch, err)
		}
	}
	for i := range dsns {
		if err := s.Shard(i).MaintainPartitionsAt(ctx, 10*time.Minute); err != nil {
			t.Fatalf("shard %d sweep: %v", i, err)
		}
		if got := s.Shard(i).PartitionsDropped("channel_messages"); got < 1 {
			t.Errorf("shard %d dropped %v channel_messages leaves, want >= 1", i, got)
		}
	}
	for i, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_messages`).Scan(&n); err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		conn.Close(ctx)
		if n != 0 {
			t.Errorf("shard %d holds %d log rows after the window, want 0", i, n)
		}
	}
}

func relationPresent(t *testing.T, dsn, name string) bool {
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

// TestShardedIdentityGuard checks a shard list is fixed once used: the
// same list opens again, but another order, another length, a lone
// Storage on one of the shards, or a database that already holds
// channels without being recorded as a shard are refused (DESIGN.md
// §6.4).
func TestShardedIdentityGuard(t *testing.T) {
	ctx := context.Background()
	pair := shardDSNs(t, 2)
	a, b := pair[0], pair[1]
	fresh := func() string { return shardDSNs(t, 1)[0] }

	s, err := OpenSharded(ctx, Options{}, []string{a, b})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_ = s.Close()
	s, err = OpenSharded(ctx, Options{}, []string{a, b})
	if err != nil {
		t.Fatalf("reopen with the same list: %v", err)
	}
	_ = s.Close()

	refused := func(what string, err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want one containing %q", what, err, want)
		}
	}
	_, err = OpenSharded(ctx, Options{}, []string{b, a})
	refused("swapped order", err, "this database is shard 1 of 2, but --postgres-dsn lists it as shard 0 of 2")
	_, err = OpenSharded(ctx, Options{}, []string{a, b, fresh()})
	refused("grown list", err, "lists it as shard 0 of 3")
	_, err = Open(ctx, Options{DSN: b})
	refused("lone Storage on shard 1", err, "this database is shard 1 of 2")

	// A database a single-DSN deployment used cannot join a list, and the
	// lone Storage wrote no identity.
	used := fresh()
	lone, err := Open(ctx, Options{DSN: used})
	if err != nil {
		t.Fatalf("open lone: %v", err)
	}
	usedCS, err := lone.Channel(ctx, "used", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, _, err := usedCS.Store(ctx, []*protocol.Message{{Data: "x"}}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	_ = lone.Close()
	if !relationPresent(t, used, "channels") || relationPresent(t, used, "shard_identity") {
		t.Error("a lone Storage must not create shard_identity")
	}
	_, err = OpenSharded(ctx, Options{}, []string{used, fresh()})
	refused("used database joins a list", err, "already holds channels but is not recorded as a shard")

	// One database listed twice (two spellings of one DSN) is refused.
	dup := fresh()
	_, err = OpenSharded(ctx, Options{}, []string{dup, dup + "&application_name=twice"})
	refused("one database twice", err, "(or lists it twice)")

	// An empty database in place of one that has served the list records
	// itself as that shard, but shard 0 knows which database was there.
	_, err = OpenSharded(ctx, Options{}, []string{a, fresh()})
	refused("substituted empty database", err, "shard 1 is not the database this list was first opened with")

	// A shard of another list is refused by its list id.
	other := shardDSNs(t, 2)
	s, err = OpenSharded(ctx, Options{}, other)
	if err != nil {
		t.Fatalf("open other list: %v", err)
	}
	_ = s.Close()
	_, err = OpenSharded(ctx, Options{}, []string{a, other[1]})
	refused("shard of another list", err, "of another list")
}

// TestShardedConcurrentFirstOpen opens one fresh list from several nodes
// at once: every open succeeds, and each database records its own index
// once.
func TestShardedConcurrentFirstOpen(t *testing.T) {
	ctx := context.Background()
	dsns := shardDSNs(t, 2)
	const nodes = 6
	errs := make([]error, nodes)
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := OpenSharded(ctx, Options{}, dsns)
			if err == nil {
				defer s.Close()
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", i, err)
		}
	}
	for i, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		var rows, index, count int
		if err := conn.QueryRow(ctx, `SELECT count(*), min(shard_index), min(shard_count) FROM shard_identity`).Scan(&rows, &index, &count); err != nil {
			t.Fatalf("shard %d identity: %v", i, err)
		}
		conn.Close(ctx)
		if rows != 1 || index != i || count != 2 {
			t.Errorf("shard %d identity: %d rows, index %d, count %d; want 1 row, %d of 2", i, rows, index, count, i)
		}
	}
}

// TestShardedMetrics registers a two-shard store's collectors on one
// registry: ably_storage_shards reports 2 and the per-shard series carry
// a shard label for each shard.
func TestShardedMetrics(t *testing.T) {
	s := openShardedT(t, Options{Batching: Batching{Lanes: 2}}, shardDSNs(t, 2))
	reg := prometheus.NewPedanticRegistry()
	for _, c := range s.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	shardsSeen := map[string]map[string]bool{}
	var shards float64
	for _, f := range families {
		if f.GetName() == "ably_storage_shards" {
			shards = f.GetMetric()[0].GetGauge().GetValue()
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard" {
					if shardsSeen[f.GetName()] == nil {
						shardsSeen[f.GetName()] = map[string]bool{}
					}
					shardsSeen[f.GetName()][l.GetValue()] = true
				}
			}
		}
	}
	if shards != 2 {
		t.Errorf("ably_storage_shards = %v, want 2", shards)
	}
	for _, name := range []string{"ably_storage_partitions_created_total", "ably_publish_lane_queue_depth"} {
		if got := shardsSeen[name]; !got["0"] || !got["1"] {
			t.Errorf("%s shard labels = %v, want 0 and 1", name, got)
		}
	}

	// A lone Storage reports one shard and no shard label.
	lone := openOpts(t, Options{DSN: pgtest.Start(t).FreshSchemaDSN(t)})
	reg = prometheus.NewRegistry()
	reg.MustRegister(lone.Collectors()...)
	families, err = reg.Gather()
	if err != nil {
		t.Fatalf("gather lone: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "ably_storage_shards" && f.GetMetric()[0].GetGauge().GetValue() != 1 {
			t.Errorf("lone ably_storage_shards = %v, want 1", f.GetMetric()[0].GetGauge().GetValue())
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard" {
					t.Errorf("lone Storage series %s has a shard label", f.GetName())
				}
			}
		}
	}
}
