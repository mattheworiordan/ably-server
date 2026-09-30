//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

type nopAppender struct{}

func (nopAppender) Initialize(string, string)       {}
func (nopAppender) Append(*protocol.ChannelMessage) {}

// BenchmarkPublishThroughput measures the storage write path alone:
// 16 x GOMAXPROCS concurrent publishers, each publishing one 500-byte
// message at a time to a random one of 1,000 channels, with the shipped
// LISTEN bus running. It reports publishes/s and Postgres commits/s,
// unbatched and batched, on the pgnotify bus or, with BENCH_BUS=postgres,
// the coalesced postgres bus. A laptop number, not a cloud one:
//
//	go test -tags=integration -run '^$' -bench PublishThroughput -benchtime 10s ./internal/storage/postgres/
func BenchmarkPublishThroughput(b *testing.B) {
	c := pgtest.Start(b)
	payload := string(make([]byte, 500))
	for _, mode := range []struct {
		name     string
		batching postgres.Batching
	}{
		{"unbatched", postgres.Batching{}},
		{"lanes=4", postgres.Batching{Lanes: 4}},
	} {
		b.Run(mode.name, func(b *testing.B) {
			ctx := context.Background()
			dsn := c.FreshSchemaDSN(b)
			o := postgres.Options{DSN: dsn, Batching: mode.batching}
			switch os.Getenv("BENCH_BUS") {
			case "postgres":
				o.Bus, o.NotifyMode, o.NotifyWindow = postgres.BusPostgres, postgres.NotifyCoalesced, 0
			}
			s, err := postgres.Open(ctx, o)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer s.Close()
			const channels = 1000
			stores := make([]storage.ChannelStore, channels)
			for i := range stores {
				if stores[i], err = s.Channel(ctx, fmt.Sprintf("bench-%d", i), nopAppender{}); err != nil {
					b.Fatalf("Channel: %v", err)
				}
			}
			commitsBefore := xactCommits(b, dsn)
			b.SetParallelism(16)
			b.ResetTimer()
			start := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ch := stores[rand.IntN(channels)]
					if _, _, err := ch.Store(ctx, []*protocol.Message{{Name: "b", Data: payload}}); err != nil {
						b.Errorf("Store: %v", err)
						return
					}
				}
			})
			elapsed := time.Since(start).Seconds()
			b.StopTimer()
			commits := xactCommits(b, dsn) - commitsBefore
			b.ReportMetric(float64(b.N)/elapsed, "publishes/s")
			b.ReportMetric(float64(commits)/elapsed, "commits/s")
		})
	}
}

// xactCommits reads the database's committed transaction count.
func xactCommits(b *testing.B, dsn string) int64 {
	b.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var n int64
	if err := conn.QueryRow(ctx, `SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()`).Scan(&n); err != nil {
		b.Fatalf("pg_stat_database: %v", err)
	}
	return n
}
