package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Cluster identity (DESIGN.md §11). Every node of a cluster shares one
// schema (one per shard, §6.4), and the schema's cluster_identity row
// (migration 0005) records what every node must agree on: the bus, the
// retention settings and the persisted namespaces. Nodes that disagree
// do not fail loudly on their own: a pgnotify node and a nats node never
// deliver to each other, and the retention sweep of whichever node runs
// it applies that node's settings to everybody's log. So the first node
// to open the schema records its settings, and every later open compares
// and refuses a mismatch with the statement that changes the record.
//
// The row also carries the deployment id, a random id minted once per
// cluster that scopes the nats bus's subjects and envelopes
// (natsNamespacePrefix), so two clusters sharing a NATS cluster, even
// with the same schema name, never hear each other.

// clusterIdentityVersion is this server's cluster protocol version. The
// row's min_server_version is the lowest version that may join the
// cluster; a later release that changes something older nodes cannot
// interoperate with raises it, and an older node is then refused.
const clusterIdentityVersion = 1

// clusterSettings are the settings a node brings to the comparison.
type clusterSettings struct {
	bus                 string
	message, persisted  time.Duration
	persistedNamespaces []string // sorted, no duplicates
}

// clusterIdentity is the recorded row.
type clusterIdentity struct {
	deploymentID string
	clusterSettings
	minVersion string
}

// normalizeNamespaces returns the persisted namespace ids sorted and
// de-duplicated, never nil, so two nodes that list the same set in
// another order compare equal.
func normalizeNamespaces(ns []string) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		if n != "" {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// checkClusterIdentity records the cluster's settings in the schema's
// cluster_identity row on its first open and compares them on every
// later one (DESIGN.md §11), under the shard-identity advisory lock, so
// two nodes starting at once against an empty schema never both write.
// It returns the deployment id.
//
// A lone Storage, and shard 0 of a list, mint the deployment id when the
// row is absent; every other shard must be given shard 0's (slot.
// deploymentID) and refuses a row that records another.
func checkClusterIdentity(ctx context.Context, pool *pgxpool.Pool, slot shardSlot, want clusterSettings) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("cluster identity: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext(current_schema()))`, shardIdentityLockKey); err != nil {
		return "", fmt.Errorf("cluster identity lock: %w", err)
	}
	var schema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return "", fmt.Errorf("cluster identity: current_schema: %w", err)
	}
	tbl := pgx.Identifier{schema, "cluster_identity"}.Sanitize()

	var (
		got        clusterIdentity
		msgMs, pMs int64
	)
	err = tx.QueryRow(ctx, `SELECT deployment_id, bus,
			(extract(epoch FROM message_retention) * 1000)::bigint,
			(extract(epoch FROM persisted_retention) * 1000)::bigint,
			persisted_namespaces, min_server_version
		FROM `+tbl).Scan(&got.deploymentID, &got.bus, &msgMs, &pMs, &got.persistedNamespaces, &got.minVersion)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		id := slot.deploymentID
		if id == "" {
			if slot.index > 0 {
				return "", errors.New("cluster identity: no deployment id for a shard after the first")
			}
			id = rand.Text()
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+tbl+` (deployment_id, bus, message_retention, persisted_retention, persisted_namespaces, min_server_version)
			VALUES ($1, $2, $3::bigint * interval '1 millisecond', $4::bigint * interval '1 millisecond', $5, $6)`,
			id, want.bus, want.message.Milliseconds(), want.persisted.Milliseconds(), want.persistedNamespaces, strconv.Itoa(clusterIdentityVersion)); err != nil {
			return "", fmt.Errorf("record cluster identity: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("record cluster identity: %w", err)
		}
		return id, nil
	case err != nil:
		return "", fmt.Errorf("read cluster identity: %w", err)
	}
	got.message = time.Duration(msgMs) * time.Millisecond
	got.persisted = time.Duration(pMs) * time.Millisecond
	got.persistedNamespaces = normalizeNamespaces(got.persistedNamespaces)

	if v, err := strconv.Atoi(got.minVersion); err != nil || v > clusterIdentityVersion {
		return "", fmt.Errorf("this database's cluster requires server cluster version %s or later, and this server is version %d; upgrade this node (DESIGN.md §11)", got.minVersion, clusterIdentityVersion)
	}
	if slot.index > 0 && got.deploymentID != slot.deploymentID {
		return "", fmt.Errorf("this database belongs to another cluster than shard 0 of this list (its deployment id is %s, shard 0's is %s); %s", got.deploymentID, slot.deploymentID, reshardHint)
	}
	if err := compareCluster(tbl, got.clusterSettings, want); err != nil {
		return "", err
	}
	return got.deploymentID, nil
}

// compareCluster refuses a node whose settings differ from the recorded
// ones, naming each difference and the UPDATE that changes the record.
func compareCluster(tbl string, got, want clusterSettings) error {
	var diffs, sets []string
	if got.bus != want.bus {
		diffs = append(diffs, fmt.Sprintf("this database's cluster runs --bus=%s, and this node --bus=%s; a cluster runs one bus, because nodes on different buses do not deliver to each other", got.bus, want.bus))
		sets = append(sets, "bus = "+sqlQuote(want.bus))
	}
	if got.message != want.message {
		diffs = append(diffs, fmt.Sprintf("this database's cluster runs --message-retention=%s, and this node %s", got.message, want.message))
		sets = append(sets, fmt.Sprintf("message_retention = interval '%d milliseconds'", want.message.Milliseconds()))
	}
	if got.persisted != want.persisted {
		diffs = append(diffs, fmt.Sprintf("this database's cluster runs --persisted-retention=%s, and this node %s", got.persisted, want.persisted))
		sets = append(sets, fmt.Sprintf("persisted_retention = interval '%d milliseconds'", want.persisted.Milliseconds()))
	}
	if !slices.Equal(got.persistedNamespaces, want.persistedNamespaces) {
		diffs = append(diffs, fmt.Sprintf("this database's cluster persists the namespaces [%s], and this node [%s]", strings.Join(got.persistedNamespaces, ", "), strings.Join(want.persistedNamespaces, ", ")))
		sets = append(sets, "persisted_namespaces = "+sqlTextArray(want.persistedNamespaces))
	}
	if len(diffs) == 0 {
		return nil
	}
	return fmt.Errorf("%s. Every node of a cluster must run the same bus, retention settings and persisted namespaces; "+
		"start this node with the cluster's settings, or, to change them, stop every node, run %q on this database "+
		"(on every shard's database, for a DSN list), then start the nodes with the new settings (DESIGN.md §11)",
		strings.Join(diffs, "; "), "UPDATE "+tbl+" SET "+strings.Join(sets, ", "))
}

// sqlQuote quotes s as an SQL string literal.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sqlTextArray renders ss as an SQL text[] literal.
func sqlTextArray(ss []string) string {
	if len(ss) == 0 {
		return "'{}'::text[]"
	}
	q := make([]string, len(ss))
	for i, s := range ss {
		q[i] = sqlQuote(s)
	}
	return "ARRAY[" + strings.Join(q, ", ") + "]"
}
