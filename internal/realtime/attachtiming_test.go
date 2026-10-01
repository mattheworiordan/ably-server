package realtime

import (
	"context"
	"errors"
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

// TestAttachAndSyncTimed checks the attach timing series over real
// connections (DESIGN.md §10, §12.4): every connection records its
// CONNECTED write and its client's turnaround to the first ATTACH, every
// attach its ATTACHED write, and an attach that receives the presence
// set its snapshot, queue and write stages, the SYNC frame's size and
// the time to its SYNC being written.
func TestAttachAndSyncTimed(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, core.NewManager(memory.New(memory.Options{})), time.Hour, logging.New(slog.DiscardHandler), m, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence) // no PRESENCE_SUBSCRIBE: no SYNC
	enter(t, pub, "room", 1)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	sendAttach(t, sub, "room", 0) // full modes incl. PRESENCE_SUBSCRIBE
	if sync := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); sync.Action != protocol.ActionSync {
		t.Fatalf("got %v, want SYNC", sync.Action)
	}

	want := []string{
		"ably_connect_seconds_count 2",
		`ably_attach_seconds_count{until="attached"} 2`,
		`ably_attach_seconds_count{until="synced"} 1`,
		`ably_presence_sync_stage_seconds_count{stage="snapshot"} 1`,
		`ably_presence_sync_stage_seconds_count{stage="queue"} 1`,
		`ably_presence_sync_stage_seconds_count{stage="write"} 1`,
		"ably_presence_sync_frame_bytes_count 1",
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		body := scrape(t, m)
		var missing []string
		for _, w := range want {
			if !strings.Contains(body, w) {
				missing = append(missing, w)
			}
		}
		// A connection's turnaround is not sampled when its ATTACH is read
		// before the write loop notes the CONNECTED write, so at least one.
		if !strings.Contains(body, "ably_client_attach_delay_seconds_count 1") &&
			!strings.Contains(body, "ably_client_attach_delay_seconds_count 2") {
			missing = append(missing, "ably_client_attach_delay_seconds_count 1 or 2")
		}
		if len(missing) == 0 {
			if strings.Contains(body, "ably_presence_sync_frame_bytes_sum 0\n") {
				t.Error("SYNC frame size recorded as 0 bytes")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing series %q", missing)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSyncSkippedReason: a SYNC whose snapshot fails because the
// attachment has gone is counted as closed, not reported as a store
// failure; any other failure is counted as an error (DESIGN.md §12.4).
func TestSyncSkippedReason(t *testing.T) {
	m := metrics.New()
	a := &attachment{metrics: m, logger: logging.New(slog.DiscardHandler)}

	closed, cancel := context.WithCancel(context.Background())
	cancel()
	a.syncSkipped(closed, context.Canceled)
	a.syncSkipped(context.Background(), errors.New("storage: members read failed"))

	body := scrape(t, m)
	for _, w := range []string{
		`ably_presence_syncs_skipped_total{reason="closed"} 1`,
		`ably_presence_syncs_skipped_total{reason="error"} 1`,
	} {
		if !strings.Contains(body, w) {
			t.Errorf("missing %q", w)
		}
	}
}

// TestWriteTimeoutCountedByFrame: a client that stops reading, with an
// outbound queue too large to fill, is closed by the write deadline, and
// the timeout is counted under the action of the frame whose write
// missed it (DESIGN.md §5.2).
func TestWriteTimeoutCountedByFrame(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), m, nil)
	rt.SetConnLimits(ConnLimits{OutboundMaxBytes: 1 << 30, WriteTimeout: 200 * time.Millisecond})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ws := dial(t, srv, "")
	drainConnected(t, ws)
	attach(t, ws, "firehose", protocol.FlagSubscribe)
	ch, err := manager.GetChannel(context.Background(), "firehose")
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 64<<10)
	const want = `ably_write_timeouts_total{frame="message"} 1`
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; ; i++ {
		if i < 2000 {
			if _, _, err := ch.Publish(context.Background(), []*protocol.Message{{Name: "m", Data: payload}}); err != nil {
				t.Fatal(err)
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		if i%50 == 0 && strings.Contains(scrape(t, m), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q after the client stopped reading", want)
		}
	}
}
