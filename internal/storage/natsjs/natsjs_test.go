//go:build integration

package natsjs_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/natsjs"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

var (
	natsOnce sync.Once
	natsURL  string
	natsErr  error

	// prefixCounter mints a fresh stream/bucket namespace per test, so
	// subtests sharing one JetStream never see each other's channels.
	prefixCounter atomic.Int64
)

// startNATS brings up nats:2.11-alpine with JetStream enabled, once per
// test binary, on a random host port. The testcontainers reaper removes it
// when the binary exits.
func startNATS(t *testing.T) string {
	t.Helper()
	natsOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		c, err := testcontainers.Run(ctx, "nats:2.11-alpine",
			testcontainers.WithCmd("-js"),
			testcontainers.WithExposedPorts("4222/tcp"),
			testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
		)
		if err != nil {
			natsErr = fmt.Errorf("start nats container: %w", err)
			return
		}
		ep, err := c.PortEndpoint(ctx, "4222/tcp", "nats")
		if err != nil {
			natsErr = fmt.Errorf("nats endpoint: %w", err)
			return
		}
		natsURL = ep
	})
	if natsErr != nil {
		t.Fatalf("nats container: %v", natsErr)
	}
	return natsURL
}

// freshPrefix returns an unused namespace and deletes its streams and
// buckets when the test ends.
func freshPrefix(t *testing.T, url string) string {
	t.Helper()
	prefix := fmt.Sprintf("T%d", prefixCounter.Add(1))
	t.Cleanup(func() { deleteNamespace(t, url, prefix) })
	return prefix
}

func deleteNamespace(t *testing.T, url, prefix string) {
	nc, err := nats.Connect(url)
	if err != nil {
		t.Logf("cleanup connect: %v", err)
		return
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	names := js.StreamNames(ctx)
	var doomed []string
	for name := range names.Name() {
		if strings.HasPrefix(name, prefix+"_") || strings.HasPrefix(name, "KV_"+prefix+"_") {
			doomed = append(doomed, name)
		}
	}
	for _, name := range doomed {
		_ = js.DeleteStream(ctx, name)
	}
}

func openStorage(t *testing.T, url, prefix string, shards int) *natsjs.Storage {
	t.Helper()
	s, err := natsjs.Open(context.Background(), natsjs.Options{URL: url, Prefix: prefix, Shards: shards})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestNATSJetStreamChannelStoreContract runs the shared ChannelStore
// contract. Two shards, so the suite's channels spread across two streams.
func TestNATSJetStreamChannelStoreContract(t *testing.T) {
	url := startNATS(t)
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		return openStorage(t, url, freshPrefix(t, url), 2)
	})
}

// recordingAppender records Initialize and every Append so the cross-node
// tests can assert on delivery order and exactly-once receipt.
type recordingAppender struct {
	mu      sync.Mutex
	current string
	initial string
	cms     []*protocol.ChannelMessage
}

func (a *recordingAppender) Initialize(current, initial string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current, a.initial = current, initial
}

func (a *recordingAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cms = append(a.cms, cm)
}

func (a *recordingAppender) serials() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.cms))
	for i, cm := range a.cms {
		out[i] = cm.ChannelSerial
	}
	return out
}

// waitFor polls until the appender holds at least n cms, then waits a
// short grace period so a duplicate delivery would be observed too.
func (a *recordingAppender) waitFor(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.serials()) >= n {
			time.Sleep(300 * time.Millisecond)
			return a.serials()
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := a.serials()
	t.Fatalf("appender received %d cms, want %d", len(got), n)
	return got
}

func strictlyIncreasing(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] <= s[i-1] {
			return false
		}
	}
	return true
}

// TestNATSJetStreamCrossNodeDelivery brings up two Storage instances (two
// "nodes") on one JetStream, publishes through one, and asserts the other
// node's appender receives every cm exactly once and in publish order. The
// publisher's own appender receives them through the same consumer path.
func TestNATSJetStreamCrossNodeDelivery(t *testing.T) {
	url := startNATS(t)
	prefix := freshPrefix(t, url)
	nodeA := openStorage(t, url, prefix, 2)
	nodeB := openStorage(t, url, prefix, 2)
	ctx := context.Background()

	appA, appB := &recordingAppender{}, &recordingAppender{}
	chA, err := nodeA.Channel(ctx, "room", appA)
	if err != nil {
		t.Fatalf("nodeA Channel: %v", err)
	}
	if _, err := nodeB.Channel(ctx, "room", appB); err != nil {
		t.Fatalf("nodeB Channel: %v", err)
	}
	if appA.initial == "" || appA.initial != appB.initial {
		t.Fatalf("initial serials differ across nodes: %q vs %q", appA.initial, appB.initial)
	}

	const n = 50
	var published []string
	for i := range n {
		cm, _, err := chA.Store(ctx, []*protocol.Message{{Name: "m", Data: fmt.Sprint(i)}})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		published = append(published, cm.ChannelSerial)
	}
	if !strictlyIncreasing(published) {
		t.Fatalf("published serials not strictly increasing: %v", published)
	}

	if got := appB.waitFor(t, n); !slices.Equal(got, published) {
		t.Errorf("remote node received %d cms %v, want exactly the %d published, in order", len(got), got, n)
	}
	if got := appA.waitFor(t, n); !slices.Equal(got, published) {
		t.Errorf("publishing node received %d cms, want exactly the %d published, in order", len(got), n)
	}
	for _, s := range published {
		if s <= appB.initial || s <= appB.current {
			t.Errorf("delivered serial %q not after the Initialize watermark (%q, %q)", s, appB.current, appB.initial)
		}
	}
}

// TestNATSJetStreamCrossNodeConcurrentPublishers has both nodes publish to
// one channel concurrently. The per-channel compare-and-set must give one
// total order: every publish succeeds, serials are unique, both nodes'
// appenders see the identical strictly-increasing sequence, and history
// agrees with it.
func TestNATSJetStreamCrossNodeConcurrentPublishers(t *testing.T) {
	url := startNATS(t)
	prefix := freshPrefix(t, url)
	nodeA := openStorage(t, url, prefix, 1)
	nodeB := openStorage(t, url, prefix, 1)
	ctx := context.Background()

	appA, appB := &recordingAppender{}, &recordingAppender{}
	chA, err := nodeA.Channel(ctx, "race", appA)
	if err != nil {
		t.Fatalf("nodeA Channel: %v", err)
	}
	chB, err := nodeB.Channel(ctx, "race", appB)
	if err != nil {
		t.Fatalf("nodeB Channel: %v", err)
	}

	const workersPerNode, perWorker = 4, 25
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		all []string
	)
	for _, ch := range []storage.ChannelStore{chA, chB} {
		for range workersPerNode {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range perWorker {
					cm, _, err := ch.Store(ctx, []*protocol.Message{{Name: "x"}})
					if err != nil {
						t.Errorf("publish: %v", err)
						return
					}
					mu.Lock()
					all = append(all, cm.ChannelSerial)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	t.Logf("compare-and-set conflicts resolved: node A %d, node B %d", nodeA.WriteConflicts(), nodeB.WriteConflicts())

	total := 2 * workersPerNode * perWorker
	if len(all) != total {
		t.Fatalf("successful publishes = %d, want %d", len(all), total)
	}
	gotA := appA.waitFor(t, total)
	gotB := appB.waitFor(t, total)
	if !slices.Equal(gotA, gotB) {
		t.Errorf("nodes observed different orders (A %d cms, B %d cms)", len(gotA), len(gotB))
	}
	if !strictlyIncreasing(gotA) {
		t.Errorf("delivered serials not strictly increasing")
	}
	sorted := slices.Clone(all)
	slices.Sort(sorted)
	if !slices.Equal(sorted, gotA) {
		t.Errorf("delivered set differs from the published set")
	}

	page, err := chB.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var hist []string
	for _, cm := range page.ChannelMessages {
		hist = append(hist, cm.ChannelSerial)
	}
	if !slices.Equal(hist, gotA) {
		t.Errorf("history order differs from delivery order")
	}
}

// TestNATSJetStreamCrossNodeState checks that the derived and KV state is
// shared: a message created on one node is mutated on the other, and a
// presence member entered on one node is visible from the other.
func TestNATSJetStreamCrossNodeState(t *testing.T) {
	url := startNATS(t)
	prefix := freshPrefix(t, url)
	nodeA := openStorage(t, url, prefix, 1)
	nodeB := openStorage(t, url, prefix, 1)
	ctx := context.Background()

	chA, err := nodeA.Channel(ctx, "shared", nil)
	if err != nil {
		t.Fatalf("nodeA Channel: %v", err)
	}
	chB, err := nodeB.Channel(ctx, "shared", nil)
	if err != nil {
		t.Fatalf("nodeB Channel: %v", err)
	}

	cm, _, err := chA.Store(ctx, []*protocol.Message{{Data: "v1", ClientID: "alice"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	target := cm.Messages[0].Serial
	if _, _, err := chB.Mutate(ctx, &protocol.Message{Action: protocol.MessageUpdate, Serial: target, Data: "v2", ClientID: "bob"}); err != nil {
		t.Fatalf("Mutate on other node: %v", err)
	}
	latest, err := chA.LatestVersion(ctx, target)
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if latest.Data != "v2" {
		t.Errorf("latest on node A = %v, want v2 (mutated on node B)", latest.Data)
	}

	if _, _, err := chA.StorePresence(ctx, []*protocol.PresenceMessage{
		{Action: protocol.PresenceEnter, ConnectionID: "conn-1", ClientID: "alice", Data: "hi"},
	}); err != nil {
		t.Fatalf("StorePresence: %v", err)
	}
	members, _, err := chB.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != 1 || members[0].ClientID != "alice" {
		t.Errorf("members seen from node B = %+v, want alice", members)
	}
}
