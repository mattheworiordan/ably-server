//go:build integration

package natsjs_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/natsjs"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

// natsCluster is a three-server JetStream cluster on a private Docker
// network, each server's client port mapped to a random host port.
type natsCluster struct {
	names      []string
	urls       []string // host-reachable client URL per server
	containers []*testcontainers.DockerContainer
}

var (
	clusterOnce sync.Once
	clusterInst *natsCluster
	clusterErr  error
)

// startCluster brings up the cluster once per test binary and waits until
// JetStream has a meta leader.
func startCluster(t *testing.T) *natsCluster {
	t.Helper()
	clusterOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		nw, err := network.New(ctx)
		if err != nil {
			clusterErr = fmt.Errorf("create network: %w", err)
			return
		}
		c := &natsCluster{names: []string{"n1", "n2", "n3"}}
		routes := "nats://n1:6222,nats://n2:6222,nats://n3:6222"
		for _, name := range c.names {
			ctr, err := testcontainers.Run(ctx, "nats:2.11-alpine",
				testcontainers.WithCmd("-js", "-sd", "/data", "--server_name", name,
					"--cluster_name", "ably", "--cluster", "nats://0.0.0.0:6222", "--routes", routes),
				testcontainers.WithExposedPorts("4222/tcp"),
				network.WithNetwork([]string{name}, nw),
				testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
			)
			if err != nil {
				clusterErr = fmt.Errorf("start %s: %w", name, err)
				return
			}
			ep, err := ctr.PortEndpoint(ctx, "4222/tcp", "nats")
			if err != nil {
				clusterErr = fmt.Errorf("endpoint %s: %w", name, err)
				return
			}
			c.containers = append(c.containers, ctr)
			c.urls = append(c.urls, ep)
		}
		// Ready means an R3 stream can be placed: the meta group has a
		// leader and all three peers are known and current.
		nc, err := nats.Connect(c.urls[0])
		if err != nil {
			clusterErr = err
			return
		}
		defer nc.Close()
		js, _ := jetstream.New(nc)
		for {
			actx, acancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := js.CreateStream(actx, jetstream.StreamConfig{Name: "READY", Subjects: []string{"ready.>"}, Replicas: 3})
			if err == nil {
				_ = js.DeleteStream(actx, "READY")
			}
			acancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				clusterErr = fmt.Errorf("jetstream cluster not ready: %w", err)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		clusterInst = c
	})
	if clusterErr != nil {
		t.Fatalf("nats cluster: %v", clusterErr)
	}
	return clusterInst
}

// connect dials the cluster preferring server `prefer`, with the other
// servers as failover targets. Discovered (container-internal) addresses
// are ignored because the host cannot reach them.
func (c *natsCluster) connect(t *testing.T, prefer int) *nats.Conn {
	t.Helper()
	urls := []string{c.urls[prefer%len(c.urls)]}
	for i, u := range c.urls {
		if i != prefer%len(c.urls) {
			urls = append(urls, u)
		}
	}
	nc, err := nats.Connect(strings.Join(urls, ","),
		nats.DontRandomize(),
		nats.IgnoreDiscoveredServers(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.Timeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func openClusterStorage(t *testing.T, nc *nats.Conn, prefix string, shards int) *natsjs.Storage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := natsjs.Open(ctx, natsjs.Options{Conn: nc, Prefix: prefix, Shards: shards, Replicas: 3})
	if err != nil {
		t.Fatalf("Open (R3): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestNATSJetStreamClusterR3Contract runs the shared contract against
// three-way replicated streams and buckets on a three-server cluster,
// rotating which server each subtest connects to.
func TestNATSJetStreamClusterR3Contract(t *testing.T) {
	c := startCluster(t)
	n := 0
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		n++
		nc := c.connect(t, n)
		prefix := freshPrefix(t, strings.Join(c.urls, ","))
		return openClusterStorage(t, nc, prefix, 2)
	})
}

// TestNATSJetStreamClusterContendedIdempotentPublish has two nodes, on
// different servers, publish to one channel concurrently with
// client-supplied message ids on an R3 stream. That combines the
// per-channel compare-and-set with Nats-Msg-Id de-duplication on the
// clustered publish path.
func TestNATSJetStreamClusterContendedIdempotentPublish(t *testing.T) {
	c := startCluster(t)
	prefix := freshPrefix(t, strings.Join(c.urls, ","))
	nodeA := openClusterStorage(t, c.connect(t, 0), prefix, 1)
	nodeB := openClusterStorage(t, c.connect(t, 1), prefix, 1)
	ctx := context.Background()

	chA, err := nodeA.Channel(ctx, "contended", nil)
	if err != nil {
		t.Fatalf("nodeA Channel: %v", err)
	}
	chB, err := nodeB.Channel(ctx, "contended", nil)
	if err != nil {
		t.Fatalf("nodeB Channel: %v", err)
	}

	const perNode = 50
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []error
		ok       int
	)
	for n, ch := range []storage.ChannelStore{chA, chB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perNode {
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, _, err := ch.Store(pctx, []*protocol.Message{{ID: fmt.Sprintf("node%d-msg%d", n, i), Name: "x"}})
				cancel()
				mu.Lock()
				if err != nil {
					failures = append(failures, err)
				} else {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("compare-and-set conflicts: node A %d, node B %d; publishes ok %d, failed %d",
		nodeA.WriteConflicts(), nodeB.WriteConflicts(), ok, len(failures))
	if len(failures) > 0 {
		var apiErr *jetstream.APIError
		if errors.As(failures[0], &apiErr) {
			t.Logf("first failure: JetStream error code %d: %v", apiErr.ErrorCode, failures[0])
		}
		t.Errorf("%d of %d contended publishes with client ids failed; first: %v", len(failures), 2*perNode, failures[0])
	}
}

// TestNATSJetStreamClusterLeaderFailover stops the JetStream server that
// leads the channel's stream in the middle of a publish run. The stream
// must elect a new leader from its replicas; ably-server keeps publishing
// (retrying the calls that fail during the election, with the same message
// id so a retry of a publish that did land is de-duplicated), and both
// nodes' appenders must still receive every message exactly once, in order.
func TestNATSJetStreamClusterLeaderFailover(t *testing.T) {
	c := startCluster(t)
	prefix := freshPrefix(t, strings.Join(c.urls, ","))
	ncA, ncB := c.connect(t, 0), c.connect(t, 1)
	nodeA := openClusterStorage(t, ncA, prefix, 1)
	nodeB := openClusterStorage(t, ncB, prefix, 1)
	ctx := context.Background()

	appA, appB := &recordingAppender{}, &recordingAppender{}
	chA, err := nodeA.Channel(ctx, "failover", appA)
	if err != nil {
		t.Fatalf("nodeA Channel: %v", err)
	}
	if _, err := nodeB.Channel(ctx, "failover", appB); err != nil {
		t.Fatalf("nodeB Channel: %v", err)
	}

	js, _ := jetstream.New(ncA)
	stream, err := js.Stream(ctx, prefix+"_LOG_0")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("stream info: %v", err)
	}
	leader := info.Cluster.Leader
	t.Logf("stream leader before failover: %s (replicas %d)", leader, len(info.Cluster.Replicas)+1)

	const total, killAt = 200, 60
	var (
		published []string
		retries   int
		maxGap    time.Duration
		lastOK    = time.Now()
		stopped   *testcontainers.DockerContainer
	)
	for i := range total {
		if i == killAt {
			idx := slices.Index(c.names, leader)
			if idx < 0 {
				t.Fatalf("leader %q is not a cluster member", leader)
			}
			stopped = c.containers[idx]
			stopTimeout := 5 * time.Second
			if err := stopped.Stop(ctx, &stopTimeout); err != nil {
				t.Fatalf("stop leader %s: %v", leader, err)
			}
			t.Logf("stopped leader %s after %d publishes", leader, i)
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			cm, _, err := chA.Store(pctx, []*protocol.Message{{ID: fmt.Sprintf("msg-%d", i), Name: "m", Data: i}})
			cancel()
			if err == nil {
				published = append(published, cm.ChannelSerial)
				if gap := time.Since(lastOK); gap > maxGap {
					maxGap = gap
				}
				lastOK = time.Now()
				break
			}
			retries++
			if time.Now().After(deadline) {
				t.Fatalf("publish %d still failing after 60s: %v", i, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	t.Logf("longest gap between acknowledged publishes: %v; failed attempts retried: %d", maxGap.Round(time.Millisecond), retries)

	if !strictlyIncreasing(published) {
		t.Errorf("acknowledged serials not strictly increasing across the failover")
	}
	if got := len(slices.Compact(slices.Clone(published))); got != total {
		t.Errorf("distinct acknowledged serials = %d, want %d", got, total)
	}
	if info, err := stream.Info(ctx); err == nil && info.Cluster != nil {
		t.Logf("stream leader after failover: %s", info.Cluster.Leader)
	}

	page, err := chA.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
	if err != nil {
		t.Fatalf("History after failover: %v", err)
	}
	var hist []string
	for _, cm := range page.ChannelMessages {
		hist = append(hist, cm.ChannelSerial)
	}
	if !slices.Equal(hist, published) {
		t.Errorf("history holds %d cms, want exactly the %d acknowledged publishes in order (no retry duplicated a message)", len(hist), total)
	}
	if got := appB.waitFor(t, total); !slices.Equal(got, published) {
		t.Errorf("node B received %d cms, want the %d acknowledged publishes exactly once, in order", len(got), total)
	}
	if got := appA.waitFor(t, total); !slices.Equal(got, published) {
		t.Errorf("node A received %d cms, want the %d acknowledged publishes exactly once, in order", len(got), total)
	}

	// Restart the stopped server so any later test sees a full cluster.
	// Docker maps a new host port on restart.
	if stopped != nil {
		if err := stopped.Start(ctx); err != nil {
			t.Logf("restart %s: %v", leader, err)
			return
		}
		if ep, err := stopped.PortEndpoint(ctx, "4222/tcp", "nats"); err == nil {
			c.urls[slices.Index(c.names, leader)] = ep
		}
	}
}
