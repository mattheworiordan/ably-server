package realtime

import (
	"context"
	"fmt"
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
//
// "Keeps reading" is enforced, not assumed. The publisher holds at most
// fastWindow messages ahead of what the fast subscriber has read (a
// credit per message read), so the fast subscriber never has more than
// fastWindow frames (about 33 KiB) unread: they fit in its outbound
// queue (64 KiB) and the loopback socket buffers, so no push waits and
// no write blocks however long its reader is descheduled. Without the
// window the publisher appended all 24 MiB in a few milliseconds and the
// fast subscriber read flat out against a full socket, so one reader
// stall longer than the 300 ms write timeout on a loaded -race runner
// tripped the write deadline and the server disconnected it as a slow
// consumer too, by the rule this test exists to prove ("fast subscriber
// received 1153 of 1500" in CI). The slow subscriber still never reads
// and is sent all 24 MiB, far past its buffers.
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

	const n = 1500
	const fastWindow = 2
	pad := strings.Repeat("x", 16<<10) // 16 KiB: ~24 MiB in all, past any socket buffers

	// The fast subscriber reads everything on its own goroutine, checks
	// the order, and returns a publish credit per message.
	credits := make(chan struct{}, fastWindow)
	for range fastWindow {
		credits <- struct{}{}
	}
	type result struct {
		count int
		err   error
	}
	got := make(chan result, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		count := 0
		for count < n {
			_ = fast.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, data, err := fast.ReadMessage()
			if err != nil {
				got <- result{count, err}
				return
			}
			var f protocol.ProtocolMessage
			if protocol.Unmarshal(data, protocol.FormatJSON, &f) != nil || f.Action != protocol.ActionMessage {
				continue
			}
			for _, msg := range f.Messages {
				if want := fmt.Sprintf("%05d", count); !strings.HasPrefix(fmt.Sprint(msg.Data), want) {
					got <- result{count, fmt.Errorf("message %d out of order (data prefix %.5q)", count, fmt.Sprint(msg.Data))}
					return
				}
				count++
				credits <- struct{}{}
			}
		}
		got <- result{count, nil}
	}()

	ctx := context.Background()
	ch, err := manager.GetChannel(ctx, "firehose")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	for i := range n {
		select {
		case <-credits:
		case <-readerDone:
			r := <-got
			t.Fatalf("fast subscriber stopped after %d of %d messages (publishing %d): %v", r.count, n, i, r.err)
		case <-time.After(30 * time.Second):
			t.Fatalf("fast subscriber made no progress for 30s at message %d", i)
		}
		if _, _, err := ch.Publish(ctx, []*protocol.Message{{Data: fmt.Sprintf("%05d", i) + pad}}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	published := time.Now()

	select {
	case r := <-got:
		if r.count != n || r.err != nil {
			t.Fatalf("fast subscriber received %d of %d messages: %v", r.count, n, r.err)
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
	// Exactly one connection, the slow one, was disconnected as a slow
	// consumer, whichever path (queue full or write timeout) fired.
	var disconnects float64
	for line := range strings.SplitSeq(scrape(t, m), "\n") {
		if strings.HasPrefix(line, "ably_slow_consumer_disconnects_total{") {
			var v float64
			if _, err := fmt.Sscan(line[strings.LastIndexByte(line, ' ')+1:], &v); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			disconnects += v
		}
	}
	if disconnects != 1 {
		t.Fatalf("ably_slow_consumer_disconnects_total = %v across reasons, want 1 (the slow subscriber only)", disconnects)
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
