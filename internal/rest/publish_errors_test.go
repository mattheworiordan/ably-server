package rest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

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

func TestPublishBatchingFailuresMapToRetriableCodes(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	for _, tc := range []struct {
		err    error
		status int
		code   int
	}{
		{storage.ErrOverloaded, http.StatusTooManyRequests, 42910},
		{storage.ErrUnavailable, http.StatusServiceUnavailable, 50003},
		{storage.ErrInvalidChannelName, http.StatusBadRequest, 40010},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			manager := core.NewManager(failingStorage{Storage: memory.New(memory.Options{}), err: tc.err})
			rs := NewServer([]auth.APIKey{parsed}, manager, logging.New(slog.DiscardHandler), nil, nil, nil, nil)
			mux := http.NewServeMux()
			mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			body, _ := json.Marshal(&protocol.Message{Name: "n", Data: "d"})
			resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
			if resp.StatusCode != tc.status {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			raw, _ := io.ReadAll(resp.Body)
			var er errorResponse
			if err := json.Unmarshal(raw, &er); err != nil || er.Error == nil || er.Error.Code != tc.code {
				t.Errorf("body = %s, want an ErrorInfo with code %d", raw, tc.code)
			}
		})
	}
}
