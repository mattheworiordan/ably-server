package realtime

import (
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// A publish over the advertised maxMessageSize is NACKed 40009 before
// anything is queued; a publish at the limit is ACKed (DESIGN.md §2.2).
func TestPublishOverMaxMessageSizeIsNacked40009(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	send := func(serial int64, size int) *protocol.ProtocolMessage {
		sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action:    protocol.ActionMessage,
			Channel:   new("big"),
			MsgSerial: msgSerialPtr(serial),
			Messages:  []*protocol.Message{{Name: "n", Data: strings.Repeat("x", size)}},
		})
		return readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	}

	over := send(1, int(defaultMaxMessageSize))
	if over.Action != protocol.ActionNack {
		t.Fatalf("over-limit publish: Action = %v, want NACK", over.Action)
	}
	if over.Error == nil || over.Error.Code != 40009 {
		t.Fatalf("over-limit publish: error = %+v, want code 40009", over.Error)
	}
	if over.GetMsgSerial() != 1 {
		t.Errorf("MsgSerial = %d, want 1", over.GetMsgSerial())
	}

	ok := send(2, int(defaultMaxMessageSize)-1) // plus the 1-byte name
	if ok.Action != protocol.ActionAck {
		t.Fatalf("at-limit publish: Action = %v, want ACK", ok.Action)
	}
}

// An inbound frame past protocol.MaxRequestBodyBytes closes the connection
// (1009) instead of being buffered.
func TestOversizeInboundFrameClosesConnection(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	huge := []byte(strings.Repeat("a", int(protocol.MaxRequestBodyBytes)+1))
	_ = ws.WriteMessage(websocket.TextMessage, huge)

	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) && !websocket.IsUnexpectedCloseError(err) {
				t.Fatalf("read error = %v, want a close (1009 message too big)", err)
			}
			return
		}
	}
}
