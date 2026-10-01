//go:build integration

package integrationtest

import (
	"context"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"
	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestIntegrationClusterBusOutageLongerThanRetention: node B is cut off
// the NATS bus while node A takes publishes and presence changes, and the
// log loses them (retention, simulated by deleting the rows: B's
// one-second retention puts its marks below the floor at once). When B
// reconnects it cannot prove continuity on the channel, so (DESIGN.md
// §7.2) its subscriber gets a server-initiated channel update, ATTACHED
// with RESUMED clear and error 80016, then the re-seeded presence set
// (bob, who entered in the gap; not alice, who left in it), and live
// delivery carries on. An ably-go client on B sees the update event with
// resumed false (RTL12). Nothing from the gap is replayed.
func TestIntegrationClusterBusOutageLongerThanRetention(t *testing.T) {
	if bus := os.Getenv("ABLY_INTEGRATION_BUS"); bus != "" && bus != "nats" {
		t.Skip("runs its own NATS bus; once, in the default and nats cells")
	}
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	n := natstest.Start(t)
	proxy := natstest.NewProxy(t, n.Addr)
	const retention = "--message-retention=1s"
	addrA, _ := startNodeOnBus(t, dsn, []string{"--bus=nats", "--nats-url=" + n.URL}, retention)
	addrB, debugB := startNodeOnBus(t, dsn, []string{"--bus=nats", "--nats-url=" + proxy.URL()}, retention)
	const room = "continuity-room"

	alice := dialRawAs(t, addrA, "alice")
	rawAttach(t, alice, room, protocol.FlagPresence)
	rawPresence(t, alice, room, protocol.PresenceEnter, "a")

	sub := dialRawAs(t, addrB, "")
	if got := rawAttach(t, sub, room, 0); !slices.Equal(got, []string{"alice=a"}) {
		t.Fatalf("attach SYNC = %v, want [alice=a]", got)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	client := newClient(t, addrB)
	connect(t, client)
	sdkCh := client.Channels.Get(room)
	updates := make(chan ably.ChannelStateChange, 4)
	sdkCh.On(ably.ChannelEventUpdate, func(c ably.ChannelStateChange) { updates <- c })
	sdkMsgs := make(chan *ably.Message, 16)
	if _, err := sdkCh.SubscribeAll(ctx, func(m *ably.Message) { sdkMsgs <- m }); err != nil {
		t.Fatalf("SDK subscribe: %v", err)
	}

	before := restPublish(t, addrA, room, "before")
	if got := rawNextMessage(t, sub); got != before {
		t.Fatalf("message %s, want %s", got, before)
	}

	proxy.Cut()
	waitNotReady(t, addrB)
	bob := dialRawAs(t, addrA, "bob")
	rawAttach(t, bob, room, protocol.FlagPresence)
	rawPresence(t, bob, room, protocol.PresenceEnter, "b")
	rawPresence(t, alice, room, protocol.PresenceLeave, "")
	gap := restPublish(t, addrA, room, "gap")
	deleteLog(t, dsn, room)
	proxy.Restore()

	// The channel update, at the position B had reached.
	update := rawExpect(t, sub, protocol.ActionAttached)
	if update.Flags&protocol.FlagResumed != 0 {
		t.Errorf("update flags = %d, want RESUMED clear", update.Flags)
	}
	if update.Error == nil || update.Error.Code != 80016 {
		t.Errorf("update error = %+v, want 80016", update.Error)
	}
	if update.Flags&protocol.FlagHasPresence == 0 {
		t.Fatalf("update flags = %d, want HAS_PRESENCE (bob is present)", update.Flags)
	}
	sync := rawExpect(t, sub, protocol.ActionSync)
	if len(sync.Presence) != 1 || sync.Presence[0].ClientID != "bob" {
		t.Errorf("re-seeded SYNC = %+v, want only bob", sync.Presence)
	}

	after := restPublish(t, addrA, room, "after")
	if got := rawNextMessage(t, sub); got != after {
		t.Errorf("next message %s, want the live %s (the gap's %s is not replayed)", got, after, gap)
	}

	select {
	case c := <-updates:
		if c.Resumed || c.Reason == nil || c.Reason.Code != 80016 {
			t.Errorf("SDK update resumed=%v reason=%v, want resumed false with 80016", c.Resumed, c.Reason)
		}
	case <-ctx.Done():
		t.Fatal("the SDK saw no channel update")
	}
	var names []string
	for len(names) < 2 {
		select {
		case m := <-sdkMsgs:
			names = append(names, m.Data.(string))
		case <-ctx.Done():
			t.Fatalf("SDK messages = %v, want [before after]", names)
		}
	}
	if !slices.Equal(names, []string{"before", "after"}) {
		t.Errorf("SDK messages = %v, want [before after]", names)
	}

	if got := scrapeCounters(t, debugB, "ably_channel_discontinuities_total")[`reason="retention"`]; got != 1 {
		t.Errorf("node B ably_channel_discontinuities_total{reason=retention} = %v, want 1", got)
	}
}

// waitNotReady waits until a node's /readyz reports it out of rotation
// (in nats mode: no NATS connection).
func waitNotReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("node did not leave rotation")
}

// deleteLog removes a channel's rows from the message log, as retention
// would once they aged out.
func deleteLog(t *testing.T, dsn, channel string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DELETE FROM channel_messages WHERE channel = $1`, channel); err != nil {
		t.Fatalf("delete log rows: %v", err)
	}
}
