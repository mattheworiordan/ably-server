package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// boundedStorage wraps a Storage so its channel stores report a fixed
// retention floor, standing in for a backend that drops old history
// (the Postgres backend, DESIGN.md §6.3).
type boundedStorage struct {
	storage.Storage
	floor string
}

func (b boundedStorage) Channel(ctx context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	cs, err := b.Storage.Channel(ctx, name, appender)
	if err != nil {
		return nil, err
	}
	return boundedStore{ChannelStore: cs, floor: b.floor}, nil
}

type boundedStore struct {
	storage.ChannelStore
	floor string
}

func (b boundedStore) RetainedSince(time.Time) string { return b.floor }

func newBoundedTestServer(t *testing.T, floor string) (*httptest.Server, *testHarness) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(boundedStorage{Storage: memory.New(memory.Options{}), floor: floor})
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &testHarness{manager: manager}
}

// TestResumeOlderThanRetentionIsADiscontinuity: a client cursor older than
// the channel's retained history cannot be proven continuous, because
// messages between it and the oldest retained cm may have aged out. The
// attach must succeed at the live head with RESUMED clear, an ErrorInfo,
// and no replay (DESIGN.md §4.3).
func TestResumeOlderThanRetentionIsADiscontinuity(t *testing.T) {
	const ancientCursor = "00000000000001-000@aaaaaaaaaa:000"
	srv, h := newBoundedTestServer(t, "00000000000500")
	for _, id := range []string{"m1", "m2"} {
		h.publish(t, "expired", &protocol.Message{ID: id})
	}

	attached, replayed := resumeAttachAndDrain(t, srv, "expired", ancientCursor)
	if attached.Flags&protocol.FlagResumed != 0 {
		t.Errorf("Flags = %v, want RESUMED clear (cursor predates retained history)", attached.Flags)
	}
	if attached.Error == nil || attached.Error.Code != 80016 {
		t.Errorf("Error = %+v, want code 80016 (messages expired)", attached.Error)
	}
	if len(replayed) != 0 {
		t.Errorf("replayed %d cms, want none", len(replayed))
	}
	if attached.ChannelSerial == ancientCursor || attached.ChannelSerial == "" {
		t.Errorf("ATTACHED.ChannelSerial = %q, want the live head", attached.ChannelSerial)
	}
}

// TestResumeInsideRetentionStillResumes: with the cursor at or after the
// retention floor, an exhausted backwards walk still means the client has
// everything, as before.
func TestResumeInsideRetentionStillResumes(t *testing.T) {
	const cursor = "00000000000900-000@aaaaaaaaaa:000"
	srv, h := newBoundedTestServer(t, "00000000000500")
	h.publish(t, "kept", &protocol.Message{ID: "m1"})

	attached, replayed := resumeAttachAndDrain(t, srv, "kept", cursor)
	if attached.Flags&protocol.FlagResumed == 0 || attached.Error != nil {
		t.Errorf("Flags = %v Error = %+v, want RESUMED and no error", attached.Flags, attached.Error)
	}
	if len(replayed) != 1 {
		t.Errorf("replayed %d cms, want 1", len(replayed))
	}
}

// TestRewindWithNothingRetainedAttachesAtHead: a rewind whose window
// holds nothing used to advertise the channel's initial serial as the
// attach point. Under retention that serial is older than the floor, so
// the client's next resume from it was refused as a discontinuity
// although nothing had been lost. It must attach at the live head.
func TestRewindWithNothingRetainedAttachesAtHead(t *testing.T) {
	srv, h := newBoundedTestServer(t, "00000000000500")
	ch, err := h.manager.GetChannel(context.Background(), "quiet")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	cm, _, err := ch.Publish(context.Background(), []*protocol.Message{{ID: "old"}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	head := cm.ChannelSerial
	time.Sleep(1100 * time.Millisecond) // "old" falls outside a 1s rewind window

	attached, replayed := rewindAttachAndDrain(t, srv, "quiet", "1s")
	if len(replayed) != 0 {
		t.Fatalf("replayed %d cms, want none", len(replayed))
	}
	if attached.ChannelSerial != head {
		t.Fatalf("ATTACHED.ChannelSerial = %q, want the head %q", attached.ChannelSerial, head)
	}
	resumed, _ := resumeAttachAndDrain(t, srv, "quiet", attached.ChannelSerial)
	if resumed.Flags&protocol.FlagResumed == 0 || resumed.Error != nil {
		t.Errorf("resume from the rewind attach point: Flags = %v Error = %+v, want RESUMED and no error", resumed.Flags, resumed.Error)
	}
}

// failingStorage wraps a Storage so every Store fails with err, standing
// in for the Postgres backend's batching failures (DESIGN.md §6.3).
type failingStorage struct {
	storage.Storage
	err error
}

func (f failingStorage) Channel(ctx context.Context, name string, a storage.Appender) (storage.ChannelStore, error) {
	cs, err := f.Storage.Channel(ctx, name, a)
	if err != nil {
		return nil, err
	}
	return failingStore{ChannelStore: cs, err: f.err}, nil
}

type failingStore struct {
	storage.ChannelStore
	err error
}

func (f failingStore) Store(context.Context, []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	return nil, false, f.err
}

// TestPublishBatchingFailuresNACKWithRetriableCodes: a publish refused
// by the backend's batching layer NACKs with the Ably code a client can
// act on (DESIGN.md §6.3), not a bare NACK.
func TestPublishBatchingFailuresNACKWithRetriableCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{storage.ErrOverloaded, 42910}, {storage.ErrUnavailable, 50003}} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			parsed, err := auth.ParseAPIKey(testKey)
			if err != nil {
				t.Fatalf("parse api key: %v", err)
			}
			manager := core.NewManager(failingStorage{Storage: memory.New(memory.Options{}), err: tc.err})
			rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
			mux := http.NewServeMux()
			mux.HandleFunc("GET /", rt.HandleWebSocket)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			ws := dial(t, srv, "")
			drainConnected(t, ws)
			sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
				Action: protocol.ActionMessage, Channel: new("room"), MsgSerial: msgSerialPtr(0),
				Messages: []*protocol.Message{{Data: "x"}},
			})
			acks := collectAcks(t, ws, 1)
			if acks[0].Action != protocol.ActionNack || acks[0].Error == nil || acks[0].Error.Code != tc.code {
				t.Errorf("reply = %+v, want a NACK with code %d", acks[0], tc.code)
			}
		})
	}
}
