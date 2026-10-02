//go:build integration

package integrationtest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
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

// TestIntegrationClusterWriteOnlyRESTPublish is the write-only REST
// publish path end to end (DESIGN.md §5.1, §6.3), on the bus
// ABLY_INTEGRATION_BUS selects. Node A takes REST publishes on a channel
// only node B has a subscriber for: B's subscriber receives each exactly
// once and in order, and A binds nothing. A later attach on A starts at
// the channel's watermark, A's REST history holds every publish, and A's
// attachment then receives B's next publish live.
func TestIntegrationClusterWriteOnlyRESTPublish(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	nodeA, debugA := startNode(t, dsn)
	nodeB, _ := startNode(t, dsn)

	wsB := dialRaw(t, nodeB)
	rawSend(t, wsB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: new("wo-room")})
	rawExpect(t, wsB, protocol.ActionAttached)

	var want []string
	for i := range 20 {
		want = append(want, restPublish(t, nodeA, "wo-room", "m"+strconv.Itoa(i)))
	}
	for i, w := range want {
		if got := rawNextMessage(t, wsB); got != w {
			t.Fatalf("B message %d = %q, want %q (exactly once, in order)", i, got, w)
		}
	}
	rawNoMessage(t, wsB, 300*time.Millisecond)

	if got := boundChannels(t, debugA); got != 0 {
		t.Fatalf("node A bound channels after write-only publishes = %d, want 0", got)
	}
	if got := metricValue(t, debugA, "ably_channel_unbound_publishes_total"); got != float64(len(want)) {
		t.Fatalf("node A ably_channel_unbound_publishes_total = %v, want %d", got, len(want))
	}

	wsA := dialRaw(t, nodeA)
	rawSend(t, wsA, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: new("wo-room")})
	attached := rawExpect(t, wsA, protocol.ActionAttached)
	last := strings.TrimSuffix(want[len(want)-1], ":000")
	if got := attached.ChannelSerial; got != last {
		t.Fatalf("A ATTACHED channelSerial = %q, want the watermark %q", got, last)
	}
	if got := restHistorySerials(t, nodeA, "wo-room"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("A history = %v, want %v", got, want)
	}

	live := restPublish(t, nodeB, "wo-room", "after-attach")
	if got := rawNextMessage(t, wsA); got != live {
		t.Fatalf("A live message after attach = %q, want %q", got, live)
	}
}

// rawNoMessage fails if a MESSAGE frame arrives within d.
func rawNoMessage(t *testing.T, ws *websocket.Conn, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if err := ws.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := ws.ReadMessage()
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if m.Action == protocol.ActionMessage {
			t.Fatalf("unexpected MESSAGE %+v (a duplicate?)", m.Messages)
		}
	}
}

// restHistorySerials returns a channel's message serials, oldest first,
// from REST history.
func restHistorySerials(t *testing.T, addr, channel string) []string {
	t.Helper()
	u := url.URL{Scheme: "http", Host: addr, Path: "/channels/" + channel + "/messages", RawQuery: "direction=forwards&limit=100"}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST history: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("REST history status = %d; body=%s", resp.StatusCode, body)
	}
	var msgs []struct {
		Serial string `json:"serial"`
	}
	if err := json.Unmarshal(body, &msgs); err != nil {
		t.Fatalf("REST history body %s: %v", body, err)
	}
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Serial
	}
	return out
}

// metricValue scrapes one unlabelled series from a node's /metrics.
func metricValue(t *testing.T, debugAddr, name string) float64 {
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
		if v, ok := strings.CutPrefix(sc.Text(), name+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("parse %s %q: %v", name, v, err)
			}
			return f
		}
	}
	t.Fatalf("%s not found in /metrics", name)
	return 0
}

// TestIntegrationPublishBatchingFlagsReachMetrics: --publish-lanes and
// --publish-linger-max are honoured end to end
// and reported on /metrics, and a publish is counted in the batch-size
// histogram (DESIGN.md §6.3, §10), so a scale run can confirm the
// settings it ran with.
func TestIntegrationPublishBatchingFlagsReachMetrics(t *testing.T) {
	pgc := pgtest.Start(t)
	node, debug := startNode(t, pgc.FreshSchemaDSN(t), "--publish-lanes=1", "--publish-linger-max=10ms")
	restPublish(t, node, "lanes-room", "x")
	for name, want := range map[string]float64{
		"ably_publish_lanes":              1,
		"ably_publish_linger_max_seconds": 0.01,
		"ably_publish_batch_size_count":   1,
		"ably_publish_commits_total":      1,
	} {
		if got := metricValue(t, debug, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}
