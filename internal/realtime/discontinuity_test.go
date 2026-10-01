package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// newMeteredTestServer is newTestServer with a Metrics the Manager and
// the realtime server report to.
func newMeteredTestServer(t *testing.T) (*httptest.Server, *testHarness, *metrics.Metrics) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	m := metrics.New()
	manager := core.NewManagerWithOptions(memory.New(memory.Options{}), core.Options{Metrics: m})
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), m, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &testHarness{manager: manager}, m
}

func signalDiscontinuity(t *testing.T, h *testHarness, channel string) {
	t.Helper()
	ch, err := h.manager.GetChannel(context.Background(), channel)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	ch.Discontinuity(storage.DiscontinuityRetention)
}

// TestDiscontinuitySendsChannelUpdate: when the backend signals that it
// could not prove continuity on a channel (storage.Discontinuous,
// DESIGN.md §7.2), every attachment gets a server-initiated channel
// update in stream order (RTL12): ATTACHED at the current channelSerial,
// RESUMED clear, error 80016, then the presence set again for a
// PRESENCE_SUBSCRIBE attachment. Nothing is replayed, the connection
// stays up and live delivery carries on.
func TestDiscontinuitySendsChannelUpdate(t *testing.T) {
	srv, h, m := newMeteredTestServer(t)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	if attached := sendAttach(t, sub, "room", 0); attached.Flags&protocol.FlagHasPresence == 0 {
		t.Fatalf("ATTACHED flags = %d, want HAS_PRESENCE", attached.Flags)
	}
	if sync := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); sync.Action != protocol.ActionSync {
		t.Fatalf("frame = %v, want the attach SYNC", sync.Action)
	}
	plain := dial(t, srv, "")
	drainConnected(t, plain)
	attach(t, plain, "room", protocol.FlagSubscribe)

	h.publish(t, "room", &protocol.Message{Name: "before"})
	before := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if before.Action != protocol.ActionMessage {
		t.Fatalf("frame = %v, want MESSAGE before", before.Action)
	}
	if f := readFrame(t, plain, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionMessage {
		t.Fatalf("plain frame = %v, want MESSAGE before", f.Action)
	}

	signalDiscontinuity(t, h, "room")
	h.publish(t, "room", &protocol.Message{Name: "after"})

	for _, tc := range []struct {
		name     string
		ws       *websocket.Conn
		presence bool
	}{{"presence subscriber", sub, true}, {"message subscriber", plain, false}} {
		ws := tc.ws
		update := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if update.Action != protocol.ActionAttached {
			t.Fatalf("%s: frame = %v, want ATTACHED (the channel update)", tc.name, update.Action)
		}
		if update.Flags&protocol.FlagResumed != 0 {
			t.Errorf("%s: flags = %d, want RESUMED clear", tc.name, update.Flags)
		}
		if update.Error == nil || update.Error.Code != 80016 {
			t.Errorf("%s: error = %+v, want 80016", tc.name, update.Error)
		}
		if update.ChannelSerial != before.ChannelSerial {
			t.Errorf("%s: channelSerial = %q, want the current %q", tc.name, update.ChannelSerial, before.ChannelSerial)
		}
		if tc.presence {
			if update.Flags&protocol.FlagHasPresence == 0 || update.Flags&protocol.FlagPresenceSubscribe == 0 {
				t.Errorf("%s: flags = %d, want the modes and HAS_PRESENCE", tc.name, update.Flags)
			}
			sync := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
			if sync.Action != protocol.ActionSync || len(sync.Presence) != 1 || sync.Presence[0].ClientID != "alice" {
				t.Fatalf("%s: frame = %v %+v, want the presence set again (alice)", tc.name, sync.Action, sync.Presence)
			}
		} else if update.Flags&protocol.FlagHasPresence != 0 {
			t.Errorf("%s: flags = %d, want no HAS_PRESENCE without PRESENCE_SUBSCRIBE", tc.name, update.Flags)
		}
		after := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if after.Action != protocol.ActionMessage || after.Messages[0].Name != "after" {
			t.Fatalf("%s: frame = %v, want the live MESSAGE after", tc.name, after.Action)
		}
	}

	if got := scrape(t, m); !strings.Contains(got, `ably_channel_discontinuities_total{reason="retention"} 1`) {
		t.Errorf("metrics lack ably_channel_discontinuities_total{reason=\"retention\"} 1")
	}
}
