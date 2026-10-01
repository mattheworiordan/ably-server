package realtime

import (
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
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// TestSeenSetCapHolds: however many distinct serials are added, the set
// never holds more than its cap, and the most recently added serial is
// always held (DESIGN.md §13.3).
func TestSeenSetCapHolds(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 7, 4096} {
		s := newSeenSet(limit)
		for i := range 10 * limit {
			serial := fmt.Sprintf("s:%d", i)
			s.add(serial)
			if s.len() > limit {
				t.Fatalf("limit %d: len %d after %d adds", limit, s.len(), i+1)
			}
			if !s.has(serial) {
				t.Fatalf("limit %d: just-added %q not held", limit, serial)
			}
		}
		if s.has("s:0") {
			t.Errorf("limit %d: oldest serial still held after %d adds", limit, 10*limit)
		}
	}
}

// TestSeenSetKeepsActiveSerial: a serial touched again before half the
// cap of other serials has been added since its last touch is never
// evicted, so a message still being appended to keeps its deltas.
func TestSeenSetKeepsActiveSerial(t *testing.T) {
	const limit = 8
	s := newSeenSet(limit)
	s.add("active")
	for i := range 1000 {
		s.add(fmt.Sprintf("other:%d", i))
		if i%(limit/2-1) == 0 {
			s.add("active")
		}
		if !s.has("active") {
			t.Fatalf("active serial evicted after %d other adds", i+1)
		}
	}
}

// TestSeenSetAllocatesLazily: an empty set holds no maps.
func TestSeenSetAllocatesLazily(t *testing.T) {
	s := newSeenSet(DefaultAttachmentSeenMax)
	if s.has("x") || s.len() != 0 || s.cur != nil || s.prev != nil {
		t.Fatalf("new set = %+v, want empty with no maps", s)
	}
}

func createMsg(serial, data string) *protocol.Message {
	return &protocol.Message{Action: protocol.MessageCreate, Serial: serial, Data: data}
}

// appendMsg is an append as the store fans it out (DESIGN.md §13.3): the
// full aggregate as action=update, with the delta in Alt.
func appendMsg(serial, full, delta string) *protocol.Message {
	return &protocol.Message{
		Action: protocol.MessageUpdate, Serial: serial, Data: full,
		Alt: map[string]*protocol.Message{
			protocol.DeltaAppend: {Action: protocol.MessageAppend, Serial: serial, Data: delta},
		},
	}
}

func seenAttachment(limit int, trackCreates bool) *attachment {
	return &attachment{seen: newSeenSet(limit), trackCreates: trackCreates}
}

// TestResolveAppendsOrdinaryMessagesRecordNothing: on a channel outside
// a mutable-messages namespace, delivering any number of ordinary
// messages leaves the seen set empty, so a long-lived attachment on a
// busy channel does not grow (DESIGN.md §13.3).
func TestResolveAppendsOrdinaryMessagesRecordNothing(t *testing.T) {
	a := seenAttachment(DefaultAttachmentSeenMax, false)
	for i := range 10_000 {
		a.resolveAppends([]*protocol.Message{createMsg(fmt.Sprintf("s%d:0", i), "x")}, false)
	}
	if n := a.seen.len(); n != 0 {
		t.Fatalf("seen holds %d serials after ordinary messages, want 0", n)
	}
	if a.seen.cur != nil || a.seen.prev != nil {
		t.Errorf("seen allocated maps for ordinary messages")
	}
}

// TestResolveAppendsNonMutableAppendsBecomeDeltas: outside a mutable
// namespace a create is not recorded, so the first append is the full
// version; that append is recorded, so later ones are deltas.
func TestResolveAppendsNonMutableAppendsBecomeDeltas(t *testing.T) {
	a := seenAttachment(DefaultAttachmentSeenMax, false)
	a.resolveAppends([]*protocol.Message{createMsg("t:0", "Hello")}, false)

	first := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world", ", world")}, false)[0]
	if first.Action != protocol.MessageUpdate || first.Data != "Hello, world" || first.Alt != nil {
		t.Errorf("first append = %+v, want full update without Alt", first)
	}
	second := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world!", "!")}, false)[0]
	if second.Action != protocol.MessageAppend || second.Data != "!" {
		t.Errorf("second append = %+v, want delta '!'", second)
	}
	if n := a.seen.len(); n != 1 {
		t.Errorf("seen holds %d serials, want 1", n)
	}
}

// TestResolveAppendsMutableCreateThenDelta: in a mutable namespace the
// create is recorded, so its first append is a delta.
func TestResolveAppendsMutableCreateThenDelta(t *testing.T) {
	a := seenAttachment(DefaultAttachmentSeenMax, true)
	a.resolveAppends([]*protocol.Message{createMsg("t:0", "Hello")}, false)
	got := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world", ", world")}, false)[0]
	if got.Action != protocol.MessageAppend || got.Data != ", world" {
		t.Errorf("append after create = %+v, want delta ', world'", got)
	}
}

// TestResolveAppendsDeltaAfterEvictionIsFull: the cap holds while
// messages are delivered, and an append for a serial the set has
// evicted is delivered as the full version; the next one is a delta
// again (DESIGN.md §13.3).
func TestResolveAppendsDeltaAfterEvictionIsFull(t *testing.T) {
	const limit = 4
	a := seenAttachment(limit, true)
	a.resolveAppends([]*protocol.Message{createMsg("t:0", "Hello")}, false)
	for i := range limit {
		a.resolveAppends([]*protocol.Message{createMsg(fmt.Sprintf("o%d:0", i), "x")}, false)
		if n := a.seen.len(); n > limit {
			t.Fatalf("seen holds %d serials, cap %d", n, limit)
		}
	}
	if a.seen.has("t:0") {
		t.Fatalf("t:0 still held after %d other serials (cap %d)", limit, limit)
	}

	full := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world", ", world")}, false)[0]
	if full.Action != protocol.MessageUpdate || full.Data != "Hello, world" || full.Alt != nil {
		t.Errorf("append after eviction = %+v, want full update without Alt", full)
	}
	delta := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world!", "!")}, false)[0]
	if delta.Action != protocol.MessageAppend || delta.Data != "!" {
		t.Errorf("next append = %+v, want delta '!'", delta)
	}
	if n := a.seen.len(); n > limit {
		t.Errorf("seen holds %d serials, cap %d", n, limit)
	}
}

// TestResolveAppendsAppendModeFullRecordsNothing: a subscriber that opted
// into full versions never receives a delta, so nothing is recorded.
func TestResolveAppendsAppendModeFullRecordsNothing(t *testing.T) {
	a := seenAttachment(DefaultAttachmentSeenMax, true)
	a.appendModeFull = true
	a.resolveAppends([]*protocol.Message{createMsg("t:0", "Hello")}, false)
	got := a.resolveAppends([]*protocol.Message{appendMsg("t:0", "Hello, world", ", world")}, false)[0]
	if got.Action != protocol.MessageUpdate || got.Alt != nil {
		t.Errorf("append under appendMode=full = %+v, want full update", got)
	}
	if n := a.seen.len(); n != 0 {
		t.Errorf("seen holds %d serials under appendMode=full, want 0", n)
	}
}

// TestAppendTrackingByNamespace: through a real connection, a server
// whose Mutable resolver covers only the "mutable:" namespace delivers a
// create's first append as a delta there, and as the full version on a
// channel outside it; later appends are deltas on both (DESIGN.md §13.3).
func TestAppendTrackingByNamespace(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	rt.SetAppendTracking(AppendTracking{Mutable: func(ch string) bool { return strings.HasPrefix(ch, "mutable:") }})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	for _, tc := range []struct {
		channel    string
		firstDelta bool
	}{
		{"mutable:room", true},
		{"room", false},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			sub := dialClient(t, srv, "")
			drainConnected(t, sub)
			attach(t, sub, tc.channel, protocol.FlagSubscribe)

			pub := dialClient(t, srv, "alice")
			drainConnected(t, pub)
			attach(t, pub, tc.channel, protocol.FlagPublish|protocol.FlagSubscribe)

			target := publishCreate(t, pub, tc.channel, 1, "Hello")
			if create := readMessage(t, sub).Messages[0]; create.Serial != target {
				t.Fatalf("create = %+v, want serial %q", create, target)
			}

			sendMutation(t, pub, tc.channel, 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: ", world"})
			first := readMessage(t, sub).Messages[0]
			if tc.firstDelta {
				if first.Action != protocol.MessageAppend || first.Data != ", world" {
					t.Errorf("first append = %+v, want delta ', world'", first)
				}
			} else if first.Action != protocol.MessageUpdate || first.Data != "Hello, world" {
				t.Errorf("first append = %+v, want full update 'Hello, world'", first)
			}

			sendMutation(t, pub, tc.channel, 3, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: "!"})
			second := readMessage(t, sub).Messages[0]
			if second.Action != protocol.MessageAppend || second.Data != "!" {
				t.Errorf("second append = %+v, want delta '!'", second)
			}
		})
	}
}
