package loadgen

import (
	"strconv"
	"strings"
)

// Payload is what every generated message carries in its data field. The
// subscriber side needs the publisher identity and sequence number to
// check a stream for loss, duplication and reordering, the send time for
// one-way latency, and the node the publisher sent to so latency can be
// split into same-node and cross-node deliveries.
//
// Wire form (a plain string, so it survives both the JSON and msgpack
// encodings unchanged, DESIGN.md §8):
//
//	lg1|<pubID>|<seq>|<sentAtUnixMicro>|<node>|<padding>
//
// pubID names one sequential publish stream on a channel: the pair
// (channel, pubID) is the unit whose seq increases by exactly one per
// message. node is the index of the server node the publish was sent to,
// or -1 when unknown.
type Payload struct {
	PubID    string
	Seq      int64
	SentAtUS int64
	Node     int
}

const payloadPrefix = "lg1|"

// EncodePayload renders p padded with 'x' up to size bytes. A size smaller
// than the header yields the header alone.
func EncodePayload(p Payload, size int) string {
	var b strings.Builder
	b.Grow(max(size, 48))
	b.WriteString(payloadPrefix)
	b.WriteString(p.PubID)
	b.WriteByte('|')
	b.WriteString(strconv.FormatInt(p.Seq, 10))
	b.WriteByte('|')
	b.WriteString(strconv.FormatInt(p.SentAtUS, 10))
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(p.Node))
	b.WriteByte('|')
	if pad := size - b.Len(); pad > 0 {
		for range pad {
			b.WriteByte('x')
		}
	}
	return b.String()
}

// DecodePayload parses the header of a payload produced by EncodePayload.
// It reports false for anything else, including data from other clients,
// so a subscriber can ignore foreign traffic on a shared server.
func DecodePayload(s string) (Payload, bool) {
	if !strings.HasPrefix(s, payloadPrefix) {
		return Payload{}, false
	}
	rest := s[len(payloadPrefix):]
	var fields [4]string
	for i := range fields {
		j := strings.IndexByte(rest, '|')
		if j < 0 {
			return Payload{}, false
		}
		fields[i] = rest[:j]
		rest = rest[j+1:]
	}
	if fields[0] == "" {
		return Payload{}, false
	}
	seq, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || seq < 0 {
		return Payload{}, false
	}
	sent, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return Payload{}, false
	}
	node, err := strconv.Atoi(fields[3])
	if err != nil {
		return Payload{}, false
	}
	return Payload{PubID: fields[0], Seq: seq, SentAtUS: sent, Node: node}, true
}

// PayloadFromData extracts a Payload from a decoded Message.Data, which is
// a string for a string payload and may arrive as []byte from some
// encoders.
func PayloadFromData(data any) (Payload, bool) {
	switch v := data.(type) {
	case string:
		return DecodePayload(v)
	case []byte:
		return DecodePayload(string(v))
	default:
		return Payload{}, false
	}
}

// MessageID is the idempotency id a publisher puts on each message
// (DESIGN.md §8). Retrying a publish with the same id must not produce a
// second delivery, which the checker would report as a duplicate.
func MessageID(pubID string, seq int64) string {
	return "lg-" + pubID + "-" + strconv.FormatInt(seq, 10)
}
