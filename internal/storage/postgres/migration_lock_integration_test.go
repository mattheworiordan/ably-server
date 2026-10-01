//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

const lockedMigrationSQL = `INSERT INTO retention_state (key, value) VALUES ('migration-lock-test', 'v')`

// holdTableLock opens a transaction that holds ACCESS EXCLUSIVE on
// retention_state, standing in for a node still serving traffic on the
// tables a migration needs. The returned func releases it.
func holdTableLock(t *testing.T, dsn string) (release func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect holder: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE retention_state IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock: %v", err)
	}
	var done bool
	return func() {
		if done {
			return
		}
		done = true
		_ = tx.Rollback(ctx)
		_ = conn.Close(ctx)
	}
}

// A migration that finds its tables locked by a running node gives up
// after the lock timeout instead of waiting (and queuing every other
// statement behind itself), retries, and succeeds once the lock is
// released (DESIGN.md §6.3).
func TestMigrationLockTimeoutRetriesAndSucceeds(t *testing.T) {
	defer postgres.SetMigrationTimings(200*time.Millisecond, 100*time.Millisecond)()
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	release := holdTableLock(t, dsn)
	defer release()
	time.AfterFunc(900*time.Millisecond, release)

	before := postgres.MigrationLockTimeouts()
	if err := s.ApplyMigrationForTest(ctx, "9998_lock_test", lockedMigrationSQL); err != nil {
		t.Fatalf("migration did not succeed once the lock was released: %v", err)
	}
	if got := postgres.MigrationLockTimeouts() - before; got < 2 {
		t.Errorf("lock timeouts = %d, want at least 2 (the lock was held for several timeouts)", got)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM retention_state WHERE key = 'migration-lock-test'`); n != 1 {
		t.Errorf("migration rows = %d, want 1", n)
	}
}

// A lock that never frees makes the migration fail with 55P03 after the
// bounded number of attempts: Open fails fast rather than hanging.
func TestMigrationLockTimeoutGivesUp(t *testing.T) {
	defer postgres.SetMigrationTimings(150*time.Millisecond, 50*time.Millisecond)()
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	release := holdTableLock(t, dsn)
	defer release()

	before := postgres.MigrationLockTimeouts()
	start := time.Now()
	err = s.ApplyMigrationForTest(ctx, "9997_lock_test", lockedMigrationSQL)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("err = %v, want a 55P03 lock timeout", err)
	}
	if got := postgres.MigrationLockTimeouts() - before; got != postgres.MigrationAttempts {
		t.Errorf("lock timeouts = %d, want %d (one per attempt)", got, postgres.MigrationAttempts)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("gave up after %v, want it bounded by attempts x (timeout + delay)", took)
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM schema_migrations WHERE version = '9997_lock_test'`); n != 0 {
		t.Errorf("a failed migration was recorded as applied")
	}
}
