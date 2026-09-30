package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// clusterChildren configures cluster-mode children (DESIGN.md §15): each
// provisioned app gets its own schema in one Postgres database, and its
// child runs --mode=cluster on that schema with the chosen bus. A schema
// per app keeps apps apart in Postgres, and namespaces their bus
// channels (LISTEN names and NATS subjects both hash the schema, §7.2),
// so apps sharing one Postgres and one NATS never hear each other.
type clusterChildren struct {
	dsn     string // base DSN; each app's schema is created in its database
	bus     string // --bus for every child: pgnotify, postgres or nats
	natsURL string // --nats-url for --bus=nats
}

// schemaSeq makes schema names unique within this provisioner process.
var schemaSeq atomic.Uint64

// schemaFor derives an app's schema name: a fixed prefix, the app id
// reduced to lower-case letters and digits, and a sequence number, so
// the name needs no quoting and never collides.
func schemaFor(appID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(appID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
		if b.Len() >= 24 {
			break
		}
	}
	return fmt.Sprintf("sandbox_%s_%d", b.String(), schemaSeq.Add(1))
}

// withSearchPath returns dsn with search_path set to schema, for both
// URL-form and key=value DSNs.
func withSearchPath(dsn, schema string) (string, error) {
	opt := "-c search_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse child DSN: %w", err)
		}
		q := u.Query()
		q.Set("options", opt)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return dsn + " options='" + opt + "'", nil
}

// args returns the child's cluster-mode flags for schema.
func (c *clusterChildren) args(schema string) ([]string, error) {
	dsn, err := withSearchPath(c.dsn, schema)
	if err != nil {
		return nil, err
	}
	out := []string{"--mode", "cluster", "--postgres-dsn", dsn, "--bus", c.bus}
	if c.natsURL != "" {
		out = append(out, "--nats-url", c.natsURL)
	}
	return out, nil
}

// exec runs one DDL statement on the base database.
func (c *clusterChildren) exec(sql string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return fmt.Errorf("connect to child database: %w", err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, sql)
	return err
}

func (c *clusterChildren) createSchema(schema string) error {
	if err := c.exec("CREATE SCHEMA " + pgx.Identifier{schema}.Sanitize()); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}
	return nil
}

func (c *clusterChildren) dropSchema(schema string) error {
	if err := c.exec("DROP SCHEMA IF EXISTS " + pgx.Identifier{schema}.Sanitize() + " CASCADE"); err != nil {
		return fmt.Errorf("drop schema %s: %w", schema, err)
	}
	return nil
}
