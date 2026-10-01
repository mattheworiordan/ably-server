package rest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

// errorCodeOf decodes an Ably error envelope's code.
func errorCodeOf(t *testing.T, resp *http.Response) int {
	t.Helper()
	raw, _ := io.ReadAll(resp.Body)
	var er errorResponse
	if err := json.Unmarshal(raw, &er); err != nil || er.Error == nil {
		t.Fatalf("body = %s, want an Ably error envelope", raw)
	}
	return er.Error.Code
}

func TestPublishOverMaxMessageSizeIsRejected40009(t *testing.T) {
	srv, manager := newTestServer(t)

	over, _ := json.Marshal(&protocol.Message{Name: "n", Data: strings.Repeat("x", int(protocol.MaxMessageSize)+1)})
	resp := request(t, srv, http.MethodPost, "/channels/big/messages", "application/json", over, true)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if code := errorCodeOf(t, resp); code != 40009 {
		t.Fatalf("code = %d, want 40009", code)
	}

	// A batch whose messages sum past the limit is rejected as one publish.
	half := strings.Repeat("y", int(protocol.MaxMessageSize)/2+1)
	batch, _ := json.Marshal([]*protocol.Message{{Data: half}, {Data: half}})
	resp = request(t, srv, http.MethodPost, "/channels/big/messages", "application/json", batch, true)
	if resp.StatusCode != http.StatusBadRequest || errorCodeOf(t, resp) != 40009 {
		t.Fatalf("batch over the limit: status %d, want 400/40009", resp.StatusCode)
	}
	_ = manager

	// Exactly at the limit is accepted.
	atLimit, _ := json.Marshal(&protocol.Message{Data: strings.Repeat("x", int(protocol.MaxMessageSize))})
	resp = request(t, srv, http.MethodPost, "/channels/big/messages", "application/json", atLimit, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish at the limit: status = %d, want 201", resp.StatusCode)
	}
}

func TestMutationOverMaxMessageSizeIsRejected40009(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(&protocol.Message{Action: protocol.MessageUpdate, Data: strings.Repeat("x", int(protocol.MaxMessageSize)+1)})
	resp := request(t, srv, http.MethodPatch, "/channels/big/messages/some-serial", "application/json", body, true)
	if resp.StatusCode != http.StatusBadRequest || errorCodeOf(t, resp) != 40009 {
		t.Fatalf("status %d, want 400/40009", resp.StatusCode)
	}
}

func TestRequestBodyOverCapIsRejected(t *testing.T) {
	srv, _ := newTestServer(t)
	huge := bytes.Repeat([]byte("a"), int(protocol.MaxRequestBodyBytes)+1)

	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/channels/c/messages"},
		{http.MethodPatch, "/channels/c/messages/s"},
		{http.MethodPost, "/channels/c/messages/s/annotations"},
		{http.MethodPost, "/keys/app.key/requestToken"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			// Declared Content-Length over the cap.
			resp := request(t, srv, tc.method, tc.path, "application/json", huge, true)
			if resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("declared length: status = %d, want 413", resp.StatusCode)
			}
			if code := errorCodeOf(t, resp); code != 40009 {
				t.Fatalf("code = %d, want 40009", code)
			}

			// Chunked (undeclared length) body over the cap: the reader, not
			// the header, enforces it.
			req, _ := http.NewRequest(tc.method, srv.URL+tc.path, io.NopCloser(bytes.NewReader(huge)))
			req.ContentLength = -1
			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth("app.key", "secret")
			resp2, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("chunked do: %v", err)
			}
			defer resp2.Body.Close()
			if resp2.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("chunked: status = %d, want 413", resp2.StatusCode)
			}
		})
	}
}
