package protocol

import "encoding/json"

// Request limits shared by the REST and realtime endpoints (DESIGN.md §2.2).
const (
	// MaxMessageSize is Ably's 64 KiB cap on the size of one publish, the
	// value advertised as maxMessageSize in CONNECTED and enforced on both
	// REST and realtime publishes (error 40009).
	MaxMessageSize int64 = 65536

	// MaxRequestBodyBytes caps every REST request body and every inbound
	// WebSocket frame. It is a transport guard, well above MaxMessageSize
	// (a batch of messages, msgpack/JSON framing and base64 inflation all
	// fit); the per-publish limit is MaxMessageSize.
	MaxRequestBodyBytes int64 = 2 << 20
)

// PublishSize returns the size of a publish for the MaxMessageSize check:
// the sum, over its messages, of the name, clientId, data and extras
// lengths in bytes (Ably's TM6 message size). Data is counted as held:
// the decoded payload (a string or []byte after ingress normalisation),
// not the base64 or JSON text it travelled as.
func PublishSize(msgs []*Message) int64 {
	var n int64
	for _, m := range msgs {
		if m == nil {
			continue
		}
		n += messageSize(m)
	}
	return n
}

func messageSize(m *Message) int64 {
	n := int64(len(m.Name) + len(m.ClientID))
	switch d := m.Data.(type) {
	case nil:
	case string:
		n += int64(len(d))
	case []byte:
		n += int64(len(d))
	default:
		if b, err := json.Marshal(d); err == nil {
			n += int64(len(b))
		}
	}
	if len(m.Extras) > 0 {
		if b, err := json.Marshal(m.Extras); err == nil {
			n += int64(len(b))
		}
	}
	return n
}
