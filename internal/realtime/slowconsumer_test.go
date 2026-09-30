package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// A subscriber that stops reading is disconnected as a slow consumer
// within about the write timeout of its buffers filling, and a
// subscriber on the same channel that keeps reading receives every
// message, in order (DESIGN.md §5.2).
func TestSlowConsumerDisconnectedOthersUnaffected(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), m, nil)
	const writeTimeout = 300 * time.Millisecond
	rt.SetConnLimits(ConnLimits{OutboundMaxBytes: 64 << 10, WriteTimeout: writeTimeout})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	slow := dial(t, srv, "")
	drainConnected(t, slow)
	attach(t, slow, "firehose", protocol.FlagSubscribe)
	fast := dial(t, srv, "")
	drainConnected(t, fast)
	attach(t, fast, "firehose", protocol.FlagSubscribe)

	// The fast subscriber reads everything on its own goroutine.
	const n = 1500
	payload := strings.Repeat("x", 16<<10) // 16 KiB: ~24 MiB in all, past any socket buffers
	got := make(chan int, 1)
	go func() {
		count := 0
		for count < n {
			_ = fast.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, data, err := fast.ReadMessage()
			if err != nil {
				break
			}
			var f protocol.ProtocolMessage
			if protocol.Unmarshal(data, protocol.FormatJSON, &f) == nil && f.Action == protocol.ActionMessage {
				count++
			}
		}
		got <- count
	}()

	ctx := context.Background()
	ch, err := manager.GetChannel(ctx, "firehose")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	for range n {
		if _, _, err := ch.Publish(ctx, []*protocol.Message{{Data: payload}}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	published := time.Now()

	select {
	case c := <-got:
		if c != n {
			t.Fatalf("fast subscriber received %d of %d messages", c, n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("fast subscriber did not receive every message")
	}

	// The slow subscriber, which never read, must be gone within a few
	// write timeouts of the publishing ending.
	deadline := time.Now().Add(5 * time.Second)
	for rt.connCount() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("connections = %d %v after publishing ended, want 1 (slow consumer disconnected)", rt.connCount(), time.Since(published))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(scrape(t, m), "ably_slow_consumer_disconnects_total") {
		t.Fatal("no ably_slow_consumer_disconnects_total series after a slow-consumer disconnect")
	}

	// Draining the slow client's socket ends in a DISCONNECTED (80003) if
	// the queue-full path fired first, or a closed socket if the write
	// deadline did; either way the connection is closed.
	var sawDisconnected bool
	for {
		_ = slow.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := slow.ReadMessage()
		if err != nil {
			break
		}
		var f protocol.ProtocolMessage
		if protocol.Unmarshal(data, protocol.FormatJSON, &f) == nil && f.Action == protocol.ActionDisconnected {
			if f.Error == nil || f.Error.Code != 80003 {
				t.Fatalf("DISCONNECTED error = %+v, want code 80003", f.Error)
			}
			sawDisconnected = true
		}
	}
	t.Logf("slow consumer saw DISCONNECTED frame: %v", sawDisconnected)
}

func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}
