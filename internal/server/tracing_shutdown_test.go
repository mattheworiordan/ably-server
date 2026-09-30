package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// TestShutdownWithTracingDisconnectsWebSockets is the regression test for
// the claims audit's "enabling tracing bypasses the shutdown connection
// tracker": on the assessed revision the otelhttp wrapper replaced the
// tracked handler, so graceful shutdown had nothing to cancel. On this
// tree shutdown reaches WebSockets through the realtime server's own
// registry (rt.Shutdown), whatever wraps the mux. With tracing on, a live
// WebSocket must still get DISCONNECTED and Run must return inside the
// grace window.
func TestShutdownWithTracingDisconnectsWebSockets(t *testing.T) {
	// A stand-in OTLP/HTTP collector so the exporter has somewhere to send.
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	env := envWith(map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": collector.URL})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, Opts{
			Args:   []string{"--keys=app.key:secret", "--mode=memory", "--listen=127.0.0.1:0", "--shutdown-grace=2s", "--log-level=error"},
			Getenv: env,
			Out:    io.Discard,
			Ready:  ready,
		})
	}()
	var addr net.Addr
	select {
	case addr = <-ready:
	case code := <-done:
		t.Fatalf("server exited before ready (code=%d)", code)
	case <-time.After(10 * time.Second):
		t.Fatal("server not ready within 10s")
	}

	wsURL := (&url.URL{Scheme: "ws", Host: addr.String(), Path: "/"}).String() + "?key=app.key:secret&v=2&format=json"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial through the traced handler: %v", err)
	}
	defer ws.Close()
	if f := readProto(t, ws); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}

	start := time.Now()
	cancel()

	disconnected := make(chan protocol.Action, 1)
	go func() {
		_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				disconnected <- 0
				return
			}
			var f protocol.ProtocolMessage
			if protocol.Unmarshal(data, protocol.FormatJSON, &f) == nil && f.Action == protocol.ActionDisconnected {
				disconnected <- f.Action
				return
			}
		}
	}()
	select {
	case a := <-disconnected:
		if a != protocol.ActionDisconnected {
			t.Error("WebSocket closed without a DISCONNECTED frame")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket not disconnected within 5s of shutdown")
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("Run exit code = %d, want 0", code)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
	if elapsed := time.Since(start); elapsed > 7*time.Second {
		t.Errorf("shutdown took %v, want well inside grace plus tracer flush", elapsed)
	}
}
