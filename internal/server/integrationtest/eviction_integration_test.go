//go:build integration

package integrationtest

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestIntegrationClusterEvictionRebindAcrossNodes is idle-channel
// eviction end to end in cluster mode (DESIGN.md §5.1): a subscriber on
// node A detaches, A evicts the channel (its bound-channel gauge returns
// to zero), node B publishes while A has the channel unbound, then the
// subscriber resumes on A from the last serial it saw. It must receive
// every message committed while A was unbound (re-seeded from the
// store's watermark, gap-filled from the log) and then B's live
// publishes over the bus to A's rebound channel.
func TestIntegrationClusterEvictionRebindAcrossNodes(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	nodeA, debugA := startNode(t, dsn, "--channel-idle-timeout=150ms")
	nodeB, _ := startNode(t, dsn, "--channel-idle-timeout=150ms")

	ws := dialRaw(t, nodeA)
	rawSend(t, ws, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: new("evict-room")})
	rawExpect(t, ws, protocol.ActionAttached)
	if got := boundChannels(t, debugA); got != 1 {
		t.Fatalf("node A bound channels after attach = %d, want 1", got)
	}

	m1 := restPublish(t, nodeB, "evict-room", "m1")
	if got := rawNextMessage(t, ws); got != m1 {
		t.Fatalf("live message on A = %q, want %q", got, m1)
	}

	rawSend(t, ws, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: new("evict-room")})
	rawExpect(t, ws, protocol.ActionDetached)
	waitBoundChannels(t, debugA, 0, 5*time.Second)

	// Committed on B while A holds no binding for the channel.
	m2 := restPublish(t, nodeB, "evict-room", "m2")
	m3 := restPublish(t, nodeB, "evict-room", "m3")

	rawSend(t, ws, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       new("evict-room"),
		ChannelSerial: strings.TrimSuffix(m1, ":000"),
	})
	attached := rawExpect(t, ws, protocol.ActionAttached)
	if attached.Flags&protocol.FlagResumed == 0 {
		t.Fatalf("resume ATTACHED flags = %b, want RESUMED", attached.Flags)
	}
	for _, want := range []string{m2, m3} {
		if got := rawNextMessage(t, ws); got != want {
			t.Fatalf("replayed message on A = %q, want %q", got, want)
		}
	}
	m4 := restPublish(t, nodeB, "evict-room", "m4")
	if got := rawNextMessage(t, ws); got != m4 {
		t.Fatalf("live message on A after rebind = %q, want %q", got, m4)
	}
}

func dialRaw(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	u := url.URL{Scheme: "ws", Host: addr, Path: "/", RawQuery: "key=" + url.QueryEscape(integrationAPIKey) + "&format=json"}
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	rawExpect(t, ws, protocol.ActionConnected)
	return ws
}

func rawSend(t *testing.T, ws *websocket.Conn, msg *protocol.ProtocolMessage) {
	t.Helper()
	data, err := protocol.Marshal(msg, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// rawExpect reads frames until one with the wanted action arrives,
// skipping heartbeats; any other action fails the test.
func rawExpect(t *testing.T, ws *websocket.Conn, want protocol.Action) *protocol.ProtocolMessage {
	t.Helper()
	for {
		m := rawRead(t, ws)
		if m.Action == want {
			return m
		}
		if m.Action != protocol.ActionHeartbeat {
			t.Fatalf("frame action = %v (%+v), want %v", m.Action, m.Error, want)
		}
	}
}

func rawRead(t *testing.T, ws *websocket.Conn) *protocol.ProtocolMessage {
	t.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m protocol.ProtocolMessage
	if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &m
}

// rawNextMessage returns the serial of the next MESSAGE frame's first
// message.
func rawNextMessage(t *testing.T, ws *websocket.Conn) string {
	t.Helper()
	m := rawExpect(t, ws, protocol.ActionMessage)
	return m.Messages[0].Serial
}

// restPublish publishes one message over REST and returns its serial.
func restPublish(t *testing.T, addr, channel, data string) string {
	t.Helper()
	u := url.URL{Scheme: "http", Host: addr, Path: "/channels/" + channel + "/messages"}
	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(`{"data":"`+data+`"}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST publish: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("REST publish status = %d; body=%s", resp.StatusCode, body)
	}
	var out struct {
		Serials []string `json:"serials"`
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Serials) != 1 {
		t.Fatalf("REST publish body %s: %v", body, err)
	}
	return out.Serials[0]
}

// boundChannels scrapes ably_channels_bound from a node's /metrics.
func boundChannels(t *testing.T, debugAddr string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+debugAddr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "ably_channels_bound "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return int(f)
		}
	}
	t.Fatal("ably_channels_bound not found in /metrics")
	return 0
}

func waitBoundChannels(t *testing.T, debugAddr string, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got := boundChannels(t, debugAddr)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bound channels = %d after %v, want %d", got, within, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
