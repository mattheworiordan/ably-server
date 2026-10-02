//go:build integration

package postgres

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// Cluster identity tests (DESIGN.md §11): the first node to open a
// schema records the bus, retention settings and persisted namespaces,
// and a later node that differs is refused with the statement that
// changes the record.

// openErr opens a Storage and returns the error; a Storage that opens is
// closed on t.Cleanup.
func openErr(t *testing.T, o Options) error {
	t.Helper()
	s, err := Open(context.Background(), o)
	if err == nil {
		t.Cleanup(func() { _ = s.Close() })
	}
	return err
}

// recordedIdentity reads the schema's cluster_identity row.
func recordedIdentity(t *testing.T, dsn string) (deployment, bus string, namespaces []string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var rows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM cluster_identity`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("cluster_identity rows = %d, err %v; want 1", rows, err)
	}
	if err := conn.QueryRow(ctx, `SELECT deployment_id, bus, persisted_namespaces FROM cluster_identity`).Scan(&deployment, &bus, &namespaces); err != nil {
		t.Fatalf("read cluster_identity: %v", err)
	}
	return deployment, bus, namespaces
}

// updateStatement extracts the UPDATE a refusal tells the operator to
// run.
var updateStatement = regexp.MustCompile(`run ("UPDATE [^"\\]*(?:\\.[^"\\]*)*") on this database`)

func refusalUpdate(t *testing.T, err error) string {
	t.Helper()
	m := updateStatement.FindStringSubmatch(err.Error())
	if m == nil {
		t.Fatalf("refusal names no UPDATE statement: %v", err)
	}
	stmt, uerr := strconv.Unquote(m[1])
	if uerr != nil {
		t.Fatalf("unquote %s: %v", m[1], uerr)
	}
	return stmt
}

// TestClusterIdentityRefusesAnotherBus: a schema first opened by a nats
// node refuses a pgnotify node, naming both buses and the statement
// that changes the record; following the procedure (stop every node,
// run the statement, start the new nodes) lets the pgnotify node in and
// keeps the deployment id.
func TestClusterIdentityRefusesAnotherBus(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	ctx := context.Background()
	dsn := c.FreshSchemaDSN(t)

	s1, err := Open(ctx, Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL})
	if err != nil {
		t.Fatalf("open nats node: %v", err)
	}
	dep, bus, _ := recordedIdentity(t, dsn)
	if bus != BusNATS || dep == "" || s1.deploymentID != dep {
		t.Fatalf("recorded bus %q deployment %q (node has %q); want nats and the node's id", bus, dep, s1.deploymentID)
	}
	// A second nats node joins.
	if err := openErr(t, Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL}); err != nil {
		t.Fatalf("second nats node: %v", err)
	}

	err = openErr(t, Options{DSN: dsn})
	if err == nil {
		t.Fatal("a pgnotify node opened a schema whose cluster runs nats")
	}
	for _, want := range []string{"runs --bus=nats", "this node --bus=pgnotify", "a cluster runs one bus", "stop every node"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
	stmt := refusalUpdate(t, err)

	// The procedure: stop every node, run the statement, start the nodes
	// with the new setting.
	_ = s1.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("run %q: %v", stmt, err)
	}
	conn.Close(ctx)
	if err := openErr(t, Options{DSN: dsn}); err != nil {
		t.Fatalf("pgnotify node after the change: %v", err)
	}
	dep2, bus2, _ := recordedIdentity(t, dsn)
	if bus2 != BusPGNotify || dep2 != dep {
		t.Errorf("after the change: bus %q deployment %q; want pgnotify and the unchanged %q", bus2, dep2, dep)
	}
	if err := openErr(t, Options{DSN: dsn, Bus: BusNATS, NATSURL: n.URL}); err == nil {
		t.Error("a nats node opened the schema after it was changed to pgnotify")
	}
}

// TestClusterIdentityRefusesOtherRetention: every recorded retention
// setting is compared, the persisted namespaces as a set, and one
// refusal names every difference with one UPDATE that fixes them all.
func TestClusterIdentityRefusesOtherRetention(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	dsn := c.FreshSchemaDSN(t)
	base := Options{
		DSN:                 dsn,
		Retention:           Retention{Message: 2 * time.Minute, Persisted: time.Hour},
		PersistedNamespaces: []string{"b", "a"},
	}
	if err := openErr(t, base); err != nil {
		t.Fatalf("first node: %v", err)
	}
	if _, _, ns := recordedIdentity(t, dsn); strings.Join(ns, ",") != "a,b" {
		t.Errorf("recorded namespaces %v, want [a b] (sorted)", ns)
	}
	same := base
	same.PersistedNamespaces = []string{"a", "b", "a"}
	if err := openErr(t, same); err != nil {
		t.Errorf("a node listing the same namespaces in another order: %v", err)
	}

	for name, tc := range map[string]struct {
		edit func(*Options)
		want string
	}{
		"message":    {func(o *Options) { o.Retention.Message = 5 * time.Minute }, "--message-retention=2m0s, and this node 5m0s"},
		"persisted":  {func(o *Options) { o.Retention.Persisted = 2 * time.Hour }, "--persisted-retention=1h0m0s, and this node 2h0m0s"},
		"namespaces": {func(o *Options) { o.PersistedNamespaces = []string{"a"} }, "persists the namespaces [a, b], and this node [a]"},
		"default":    {func(o *Options) { o.Retention = Retention{} }, "--persisted-retention=1h0m0s, and this node 24h0m0s"},
	} {
		t.Run(name, func(t *testing.T) {
			o := base
			tc.edit(&o)
			err := openErr(t, o)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want a refusal containing %q", err, tc.want)
			}
		})
	}

	// Two differences, one refusal, one statement that fixes both.
	o := base
	o.Retention.Message = 5 * time.Minute
	o.PersistedNamespaces = []string{"a", "c"}
	err := openErr(t, o)
	if err == nil {
		t.Fatal("a node with two differences opened")
	}
	stmt := refusalUpdate(t, err)
	if !strings.Contains(stmt, "message_retention") || !strings.Contains(stmt, "persisted_namespaces") {
		t.Fatalf("statement %q does not fix both differences", stmt)
	}
	conn, cerr := pgx.Connect(ctx, dsn)
	if cerr != nil {
		t.Fatalf("connect: %v", cerr)
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("run %q: %v", stmt, err)
	}
	conn.Close(ctx)
	if err := openErr(t, o); err != nil {
		t.Errorf("node after running the statement: %v", err)
	}
	if err := openErr(t, base); err == nil {
		t.Error("a node with the old settings opened after the change")
	}
}

// TestClusterIdentityRefusesNewerCluster: a cluster recorded by a newer
// server version that older nodes cannot join refuses this one.
func TestClusterIdentityRefusesNewerCluster(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()
	dsn := c.FreshSchemaDSN(t)
	if err := openErr(t, Options{DSN: dsn}); err != nil {
		t.Fatalf("first node: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE cluster_identity SET min_server_version = $1`, strconv.Itoa(clusterIdentityVersion+1)); err != nil {
		t.Fatalf("raise min_server_version: %v", err)
	}
	conn.Close(ctx)
	if err := openErr(t, Options{DSN: dsn}); err == nil || !strings.Contains(err.Error(), "upgrade this node") {
		t.Errorf("err = %v, want a refusal asking for an upgrade", err)
	}
}

// TestShardedClusterIdentitySharesDeploymentID: every shard of a list
// records shard 0's deployment id, and a shard whose row names another
// cluster is refused.
func TestShardedClusterIdentitySharesDeploymentID(t *testing.T) {
	ctx := context.Background()
	dsns := shardDSNs(t, 3)
	s := openShardedT(t, Options{}, dsns)
	want := s.Shard(0).deploymentID
	if want == "" {
		t.Fatal("shard 0 has no deployment id")
	}
	for i, dsn := range dsns {
		dep, _, _ := recordedIdentity(t, dsn)
		if dep != want || s.Shard(i).deploymentID != want {
			t.Errorf("shard %d: recorded %q, open %q; want shard 0's %q", i, dep, s.Shard(i).deploymentID, want)
		}
	}
	_ = s.Close()

	conn, err := pgx.Connect(ctx, dsns[2])
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE cluster_identity SET deployment_id = 'another'`); err != nil {
		t.Fatalf("update: %v", err)
	}
	conn.Close(ctx)
	if _, err := OpenSharded(ctx, Options{}, dsns); err == nil || !strings.Contains(err.Error(), "belongs to another cluster") {
		t.Errorf("err = %v, want a refusal of a shard of another cluster", err)
	}
}

// TestNATSBusClustersOnOneNATSDoNotCrossTalk: two clusters, each on its
// own database but with the same schema name, share one NATS server. A
// publish on one never reaches the other's appender for the same
// channel: their deployment ids put them on different subjects.
func TestNATSBusClustersOnOneNATSDoNotCrossTalk(t *testing.T) {
	n := natstest.Start(t)
	ctx := context.Background()
	schema := pgtest.NewSchemaName()
	dsnA := pgtest.StartShard(t, 0).SchemaDSN(t, schema)
	dsnB := pgtest.StartShard(t, 1).SchemaDSN(t, schema)
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	a := openNATSNode(t, dsnA, n.URL)
	b := openNATSNode(t, dsnB, n.URL)
	if a.deploymentID == b.deploymentID {
		t.Fatalf("two clusters share deployment id %q", a.deploymentID)
	}
	recA, recB := &cmRecorder{}, &cmRecorder{}
	chA := bindChannel(t, a, "room", recA)
	bindChannel(t, b, "room", recB)

	for i := range 5 {
		if _, _, err := chA.Store(ctx, []*protocol.Message{{Data: strconv.Itoa(i)}}); err != nil {
			t.Fatalf("Store %d: %v", i, err)
		}
	}
	recA.waitFor(t, 5, 5*time.Second)
	// Long enough for a cross-talking bus message to arrive, be held for
	// its missing predecessor and be delivered from the bus copy.
	time.Sleep(time.Second)
	if got := recB.count(); got != 0 {
		t.Errorf("cluster B's appender received %d of cluster A's cms", got)
	}
	if got := b.BusStats().Received; got != 0 {
		t.Errorf("cluster B received %d bus messages for cluster A's channel", got)
	}
}

// TestClusterIdentityConcurrentFirstOpen: nodes opening an empty schema
// at once all open, and the schema records one row with one deployment
// id, which every node runs with.
func TestClusterIdentityConcurrentFirstOpen(t *testing.T) {
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	const nodes = 6
	stores := make([]*Storage, nodes)
	errs := make([]error, nodes)
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stores[i], errs[i] = Open(context.Background(), Options{DSN: dsn})
		}()
	}
	wg.Wait()
	for _, s := range stores {
		if s != nil {
			t.Cleanup(func() { _ = s.Close() })
		}
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
	}
	dep, _, _ := recordedIdentity(t, dsn) // fails unless exactly one row
	for i, s := range stores {
		if s.deploymentID != dep {
			t.Errorf("node %d runs deployment %q, the schema records %q", i, s.deploymentID, dep)
		}
	}
}

// TestClusterIdentityRefusesAnInferredBusOnAnOlderDatabase: a database
// that served a version from before the cluster identity record holds
// channels and no record, and its nodes ran pgnotify, the default when
// --bus was unset. A node that inferred its bus would record postgres
// (or nats) and never deliver to them, so it is refused with the way
// out; an explicit --bus=pgnotify joins them, and an explicit other bus
// is the operator's stop-every-node decision and is recorded. A database
// with no channels is new and records the inferred bus (DESIGN.md §11).
func TestClusterIdentityRefusesAnInferredBusOnAnOlderDatabase(t *testing.T) {
	c := pgtest.Start(t)
	ctx := context.Background()

	// An older node: pgnotify, no identity record, a channel in use.
	older := func(t *testing.T) string {
		t.Helper()
		dsn := c.FreshSchemaDSN(t)
		s, err := Open(ctx, Options{DSN: dsn, Bus: BusPGNotify, skipClusterIdentity: true})
		if err != nil {
			t.Fatalf("open the older node: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if _, err := s.Channel(ctx, "room", &recorder{}); err != nil {
			t.Fatalf("bind room: %v", err)
		}
		return dsn
	}

	t.Run("inferred", func(t *testing.T) {
		dsn := older(t)
		for _, bus := range []string{BusPostgres, BusNATS} {
			// The refusal comes before the bus connects, so the NATS URL
			// need not answer.
			err := openErr(t, Options{DSN: dsn, Bus: bus, NATSURL: "nats://127.0.0.1:1", BusInferred: true})
			if err == nil {
				t.Fatalf("a node that inferred --bus=%s opened a database that served an earlier version", bus)
			}
			for _, want := range []string{"predates the --bus setting", "ran --bus=pgnotify, the only default then", "inferred --bus=" + bus, "with --bus=pgnotify", "stop every node first"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q lacks %q", err, want)
				}
			}
		}
	})
	t.Run("explicit pgnotify", func(t *testing.T) {
		dsn := older(t)
		if err := openErr(t, Options{DSN: dsn, Bus: BusPGNotify}); err != nil {
			t.Fatalf("explicit --bus=pgnotify: %v", err)
		}
		if _, bus, _ := recordedIdentity(t, dsn); bus != BusPGNotify {
			t.Errorf("recorded bus %q, want pgnotify", bus)
		}
	})
	t.Run("explicit postgres", func(t *testing.T) {
		dsn := older(t)
		if err := openErr(t, Options{DSN: dsn, Bus: BusPostgres}); err != nil {
			t.Fatalf("explicit --bus=postgres: %v", err)
		}
		if _, bus, _ := recordedIdentity(t, dsn); bus != BusPostgres {
			t.Errorf("recorded bus %q, want postgres", bus)
		}
	})
	t.Run("new database", func(t *testing.T) {
		dsn := c.FreshSchemaDSN(t)
		if err := openErr(t, Options{DSN: dsn, Bus: BusPostgres, BusInferred: true}); err != nil {
			t.Fatalf("inferred bus on a new database: %v", err)
		}
		if _, bus, _ := recordedIdentity(t, dsn); bus != BusPostgres {
			t.Errorf("recorded bus %q, want postgres", bus)
		}
		// Once recorded, later nodes that infer the same bus join.
		if err := openErr(t, Options{DSN: dsn, Bus: BusPostgres, BusInferred: true}); err != nil {
			t.Fatalf("second node: %v", err)
		}
	})
}
