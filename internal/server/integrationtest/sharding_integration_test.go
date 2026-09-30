//go:build integration

package integrationtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"
	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestIntegrationClusterShardedOrderAndExactlyOnce boots two nodes on a
// two-shard --postgres-dsn list (two Postgres servers, one schema name on
// each; DESIGN.md §6.4) on the bus under test. For channels spread over
// both shards, publishes alternate between the nodes' REST endpoints, and
// an SDK subscriber on each node must see every channel's messages
// exactly once and in publish order. Afterwards each channel's log is
// only in its own shard's database.
func TestIntegrationClusterShardedOrderAndExactlyOnce(t *testing.T) {
	schema := pgtest.NewSchemaName()
	dsns := []string{
		pgtest.StartShard(t, 0).SchemaDSN(t, schema),
		pgtest.StartShard(t, 1).SchemaDSN(t, schema),
	}
	list := strings.Join(dsns, ",")
	addrs := []string{startServerOnDSN(t, list), startServerOnDSN(t, list)}

	ctx, cancel := testCtx(t)
	defer cancel()

	const perChannel = 20
	var channels []string
	onShard := map[int]int{}
	for i := 0; len(channels) < 6; i++ {
		name := "sharded-" + strconv.Itoa(i)
		channels = append(channels, name)
		onShard[postgres.ShardFor(name, 2)]++
	}
	if onShard[0] == 0 || onShard[1] == 0 {
		t.Fatalf("channels %v do not cover both shards: %v", channels, onShard)
	}

	// One subscriber per node, attached to every channel before any publish.
	type key struct {
		node    int
		channel string
	}
	var mu sync.Mutex
	got := map[key][]string{}
	for n, addr := range addrs {
		client := newClient(t, addr)
		connect(t, client)
		for _, name := range channels {
			unsub, err := client.Channels.Get(name).SubscribeAll(ctx, func(m *ably.Message) {
				d, _ := m.Data.(string)
				mu.Lock()
				got[key{n, name}] = append(got[key{n, name}], d)
				mu.Unlock()
			})
			if err != nil {
				t.Fatalf("node %d subscribe %s: %v", n, name, err)
			}
			defer unsub()
		}
	}

	// Each channel's publishes run in order, alternating nodes; channels
	// publish concurrently.
	var wg sync.WaitGroup
	for _, name := range channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perChannel {
				if err := restPost(ctx, addrs[i%2], name, fmt.Sprintf(`{"name":"m","data":"%d"}`, i)); err != nil {
					t.Errorf("publish %d to %s via node %d: %v", i, name, i%2, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	want := make([]string, perChannel)
	for i := range want {
		want[i] = strconv.Itoa(i)
	}
	for n := range addrs {
		for _, name := range channels {
			k := key{n, name}
			for {
				mu.Lock()
				have := len(got[k])
				mu.Unlock()
				if have >= perChannel || ctx.Err() != nil {
					break
				}
				waitABit(ctx)
			}
			mu.Lock()
			seq := append([]string(nil), got[k]...)
			mu.Unlock()
			if strings.Join(seq, ",") != strings.Join(want, ",") {
				t.Errorf("node %d channel %s (shard %d) received %v, want %v exactly once in order", n, name, postgres.ShardFor(name, 2), seq, want)
			}
		}
	}

	// Each channel's rows are on its shard only.
	for i, dsn := range dsns {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatalf("connect shard %d: %v", i, err)
		}
		for _, name := range channels {
			var rows int
			if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM channel_messages WHERE channel = $1`, name).Scan(&rows); err != nil {
				t.Fatalf("shard %d count %s: %v", i, name, err)
			}
			wantRows := 0
			if postgres.ShardFor(name, 2) == i {
				wantRows = perChannel
			}
			if rows != wantRows {
				t.Errorf("shard %d holds %d rows of %s, want %d", i, rows, name, wantRows)
			}
		}
		conn.Close(context.Background())
	}
}

// restPost is postPublish for use off the test goroutine: it returns the
// failure instead of failing the test.
func restPost(ctx context.Context, addr, channel, jsonBody string) error {
	u := &url.URL{Scheme: "http", Host: addr, Path: "/channels/" + channel + "/messages"}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}

// waitABit pauses briefly, or until ctx ends.
func waitABit(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(20 * time.Millisecond):
	}
}
