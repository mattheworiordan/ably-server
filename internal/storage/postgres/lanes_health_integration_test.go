//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestPingReportsStuckLane: a node whose publish lane has stopped
// completing commits reports not ready (Storage.Ping, /readyz) with an
// error naming the lane, and ready again once the lane recovers.
func TestPingReportsStuckLane(t *testing.T) {
	old := commitAttemptTimeout
	commitAttemptTimeout = 300 * time.Millisecond
	defer func() { commitAttemptTimeout = old }()
	ctx := context.Background()
	dsn := pgtest.Start(t).FreshSchemaDSN(t)
	s, err := Open(ctx, Options{DSN: dsn, Batching: Batching{Lanes: 2}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }() // before the timeout restore
	ch, err := s.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping on a healthy node: %v", err)
	}

	release := stallCommits()
	defer release()
	done := make(chan error, 2)
	for i := range 2 {
		go func() {
			_, _, err := ch.Store(ctx, []*protocol.Message{{Name: fmt.Sprintf("m%d", i), Data: "x"}})
			done <- err
		}()
	}
	waitFor(t, 5*time.Second, "Ping to report the stuck lane", func() bool {
		err := s.Ping(ctx)
		return err != nil && strings.Contains(err.Error(), "publish lane")
	})
	t.Logf("Ping with the lane stuck: %v", s.Ping(ctx))

	release()
	for range 2 {
		<-done // either outcome: the attempt may have timed out
	}
	waitFor(t, 5*time.Second, "Ping to recover", func() bool { return s.Ping(ctx) == nil })
}
