package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Format is the wire encoding of a ProtocolMessage frame.
type Format int

const (
	FormatJSON Format = iota
	FormatMsgpack
)

// FormatFromQuery returns the format selected by the `format` query
// parameter on a WebSocket upgrade request. Empty defaults to JSON.
func FormatFromQuery(v string) (Format, error) {
	switch v {
	case "", "json":
		return FormatJSON, nil
	case "msgpack":
		return FormatMsgpack, nil
	default:
		return 0, fmt.Errorf("unsupported format %q", v)
	}
}

// String returns the format name for diagnostics.
func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatMsgpack:
		return "msgpack"
	default:
		return "unknown"
	}
}

// Marshal encodes a ProtocolMessage in the given format. A SYNC with no
// members is encoded as emptySync.
func Marshal(m *ProtocolMessage, f Format) ([]byte, error) {
	var v any = m
	if m.Action == ActionSync && len(m.Presence) == 0 {
		v = &emptySync{Action: m.Action, ID: m.ID, Channel: m.Channel, ChannelSerial: m.ChannelSerial, Presence: []*PresenceMessage{}}
	}
	switch f {
	case FormatJSON:
		return json.Marshal(v)
	case FormatMsgpack:
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		enc.UseCompactInts(true)
		if err := enc.Encode(v); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported format %d", f)
	}
}

// emptySync is the wire form of a SYNC with no members: its presence
// field is an empty array rather than absent. ProtocolMessage omits an
// empty presence field, and an SDK skips a SYNC without one (ably-js
// stops at a SYNC whose presence is missing), so a sync it is waiting
// on would never complete; with the empty array the SDK ends the sync
// and leaves the members it still holds (RTP18, RTP19). A server SYNC
// carries no other fields.
type emptySync struct {
	Action        Action             `json:"action"                  msgpack:"action"`
	ID            string             `json:"id,omitempty"            msgpack:"id,omitempty"`
	Channel       *string            `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string             `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	Presence      []*PresenceMessage `json:"presence"                msgpack:"presence"`
}

// Unmarshal decodes data into m using the given format.
func Unmarshal(data []byte, f Format, m *ProtocolMessage) error {
	switch f {
	case FormatJSON:
		return json.Unmarshal(data, m)
	case FormatMsgpack:
		return msgpack.Unmarshal(data, m)
	default:
		return fmt.Errorf("unsupported format %d", f)
	}
}
