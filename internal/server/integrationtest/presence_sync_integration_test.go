//go:build integration

package integrationtest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestIntegrationClusterPresenceSyncMatchesStore: a client attaching on
// node B after members entered on node A receives a SYNC equal to the
// store's member set (DESIGN.md §12.4), and a member that leaves on A is
// gone from B's next SYNC once its LEAVE has reached B. In local mode
// (the default) B serves SYNC from its own member set, seeded once when
// the first presence subscriber attached (before any member entered)
// and then kept current from the presence events B delivered over the
// bus; in store mode B reads the store per attach. Run on every bus via
// ABLY_INTEGRATION_BUS.
func TestIntegrationClusterPresenceSyncMatchesStore(t *testing.T) {
	pgc := pgtest.Start(t)
	for _, source := range []string{"local", "store"} {
		t.Run(source, func(t *testing.T) {
			dsn := pgc.FreshSchemaDSN(t)
			flag := "--presence-sync-source=" + source
			addrA, _ := startNode(t, dsn, flag)
			addrB, debugB := startNode(t, dsn, flag)
			const room = "sync-room"

			// An observer on B binds the room there and, in local mode,
			// seeds B's member set while it is still empty.
			observer := dialRawAs(t, addrB, "")
			if got := rawAttach(t, observer, room, protocol.FlagPresenceSubscribe); len(got) != 0 {
				t.Fatalf("observer SYNC = %v, want none on an empty room", got)
			}

			// Twelve members enter on A.
			const n = 12
			members := make([]*websocket.Conn, n)
			for i := range members {
				members[i] = dialRawAs(t, addrA, fmt.Sprintf("m%02d", i))
				rawAttach(t, members[i], room, protocol.FlagPresence)
				rawPresence(t, members[i], room, protocol.PresenceEnter, fmt.Sprintf("d%02d", i))
			}
			rawAwaitPresence(t, observer, n) // B has delivered every ENTER

			assertSyncMatchesStore(t, addrB, addrA, room, n)

			// m03 leaves on A; once B has delivered the LEAVE, B's SYNC no
			// longer holds it.
			rawPresence(t, members[3], room, protocol.PresenceLeave, "")
			rawAwaitPresence(t, observer, 1)
			assertSyncMatchesStore(t, addrB, addrA, room, n-1)

			syncs := scrapeCounters(t, debugB, "ably_presence_syncs_total")
			seeds := scrapeCounters(t, debugB, "ably_presence_sync_seeds_total")[""]
			t.Logf("node B: syncs %v, seeds %v", syncs, seeds)
			switch source {
			case "local":
				if seeds != 1 {
					t.Errorf("node B seeded its member set %v times, want once for the one bind", seeds)
				}
				if syncs[`snapshot="store"`] != 0 || syncs[`snapshot="fallback"`] != 0 {
					t.Errorf("local mode read the store for SYNC: %v", syncs)
				}
			case "store":
				if seeds != 0 {
					t.Errorf("store mode seeded a local member set (%v)", seeds)
				}
				if syncs[`snapshot="store"`] < 3 {
					t.Errorf("store mode served %v store SYNCs, want one per attach (3)", syncs[`snapshot="store"`])
				}
			}
		})
	}
}

// assertSyncMatchesStore attaches a fresh presence subscriber on addr
// and checks its SYNC against the store's set read over REST on
// restAddr. The SYNC alone must be complete: a client takes the set as
// final once the SYNC ends (presence.get returns it).
func assertSyncMatchesStore(t *testing.T, addr, restAddr, room string, want int) {
	t.Helper()
	ws := dialRawAs(t, addr, "")
	got := rawAttach(t, ws, room, protocol.FlagPresenceSubscribe)
	store := restPresence(t, restAddr, room)
	if len(got) != want || fmt.Sprint(got) != fmt.Sprint(store) {
		t.Fatalf("SYNC on %s = %v, store set = %v (want %d members)", addr, got, store, want)
	}
	_ = ws.Close()
}

// dialRawAs opens a raw JSON WebSocket connection, with clientID as the
// connection's clientId when non-empty.
func dialRawAs(t *testing.T, addr, clientID string) *websocket.Conn {
	t.Helper()
	q := "key=" + url.QueryEscape(integrationAPIKey) + "&format=json"
	if clientID != "" {
		q += "&clientId=" + url.QueryEscape(clientID)
	}
	u := url.URL{Scheme: "ws", Host: addr, Path: "/", RawQuery: q}
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	rawExpect(t, ws, protocol.ActionConnected)
	return ws
}

// rawAttach attaches ws to room with the given modes and returns the
// SYNC it delivers (sorted "clientId=data" entries), or nil when
// ATTACHED carries no HAS_PRESENCE.
func rawAttach(t *testing.T, ws *websocket.Conn, room string, modes int64) []string {
	t.Helper()
	rawSend(t, ws, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: &room, Flags: modes})
	attached := rawExpect(t, ws, protocol.ActionAttached)
	if attached.Flags&protocol.FlagHasPresence == 0 {
		return nil
	}
	var set []string
	for {
		m := rawExpect(t, ws, protocol.ActionSync)
		for _, p := range m.Presence {
			if p.Action != protocol.PresencePresent {
				t.Errorf("SYNC member %s action = %v, want PRESENT", p.ClientID, p.Action)
			}
			set = append(set, fmt.Sprintf("%s=%v", p.ClientID, p.Data))
		}
		if strings.HasSuffix(m.ChannelSerial, ":") {
			break
		}
	}
	sort.Strings(set)
	return set
}

// rawPresence publishes one presence operation on ws and waits for its
// ACK.
func rawPresence(t *testing.T, ws *websocket.Conn, room string, action protocol.PresenceAction, data string) {
	t.Helper()
	serial := int64(0)
	if action != protocol.PresenceEnter {
		serial = 1
	}
	p := &protocol.PresenceMessage{Action: action}
	if data != "" {
		p.Data = data
	}
	rawSend(t, ws, &protocol.ProtocolMessage{Action: protocol.ActionPresence, Channel: &room, MsgSerial: &serial, Presence: []*protocol.PresenceMessage{p}})
	rawExpect(t, ws, protocol.ActionAck)
}

// rawAwaitPresence reads PRESENCE frames on ws until n presence
// operations have arrived.
func rawAwaitPresence(t *testing.T, ws *websocket.Conn, n int) {
	t.Helper()
	for n > 0 {
		n -= len(rawExpect(t, ws, protocol.ActionPresence).Presence)
	}
}

// restPresence reads the store's member set over REST
// (GET /channels/{room}/presence, served from the store, DESIGN.md
// §12.6), as sorted "clientId=data" entries.
func restPresence(t *testing.T, addr, room string) []string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/channels/"+room+"/presence", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST presence: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("REST presence status = %d; body=%s", resp.StatusCode, body)
	}
	var members []*protocol.PresenceMessage
	if err := json.Unmarshal(body, &members); err != nil {
		t.Fatalf("REST presence body %s: %v", body, err)
	}
	set := make([]string, 0, len(members))
	for _, p := range members {
		set = append(set, fmt.Sprintf("%s=%v", p.ClientID, p.Data))
	}
	sort.Strings(set)
	return set
}

// scrapeCounters returns every sample of a counter family from a node's
// /metrics, keyed by its label set ("" for an unlabelled counter).
func scrapeCounters(t *testing.T, debugAddr, name string) map[string]float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+debugAddr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		rest, ok := strings.CutPrefix(line, name)
		if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '{') {
			continue
		}
		labels := ""
		if strings.HasPrefix(rest, "{") {
			end := strings.Index(rest, "}")
			labels, rest = rest[1:end], rest[end+1:]
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		out[labels] = v
	}
	return out
}
