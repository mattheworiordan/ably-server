package postgres

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// TestEncodeNotificationInlineThreshold checks the NOTIFY payload codec
// (DESIGN.md §7.2): a cm whose payload fits under inlinePayloadLimit is
// carried inline and decodes back to the stored rows; one that does not
// falls back to the pointer form, which always stays far below
// pg_notify's 8000-byte limit.
func TestEncodeNotificationInlineThreshold(t *testing.T) {
	row := func(data string) []byte {
		b, err := msgpack.Marshal(&protocol.Message{Serial: "00000000000001-000@s:000", Data: data})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	// Find the largest data size that still inlines, then step over it.
	size := 0
	for n := 0; n < inlinePayloadLimit; n++ {
		if _, inline, _ := encodeNotification("room", "00000000000001-000@s", storage.KindMessage, [][]byte{row(strings.Repeat("y", n))}, nil); !inline {
			size = n
			break
		}
	}
	if size == 0 {
		t.Fatal("no data size fell back to a pointer")
	}

	under, inline, err := encodeNotification("room", "00000000000001-000@s", storage.KindMessage, [][]byte{row(strings.Repeat("y", size-1))}, nil)
	if err != nil || !inline || len(under) >= inlinePayloadLimit {
		t.Fatalf("just under the limit: inline=%v len=%d err=%v, want inline under %d", inline, len(under), err, inlinePayloadLimit)
	}
	var n busNotification
	if err := json.Unmarshal([]byte(under), &n); err != nil {
		t.Fatal(err)
	}
	cm, err := n.inlineCM()
	if err != nil || cm == nil || len(cm.Messages) != 1 || cm.Messages[0].Data != strings.Repeat("y", size-1) {
		t.Fatalf("inline decode = %+v, %v; want the stored message back", cm, err)
	}

	over, inline, err := encodeNotification("room", "00000000000001-000@s", storage.KindMessage, [][]byte{row(strings.Repeat("y", size))}, nil)
	if err != nil || inline || len(over) > 200 {
		t.Fatalf("just over the limit: inline=%v len=%d err=%v, want a short pointer", inline, len(over), err)
	}
	n = busNotification{}
	if err := json.Unmarshal([]byte(over), &n); err != nil {
		t.Fatal(err)
	}
	if cm, err := n.inlineCM(); cm != nil || err != nil || n.Serial != "00000000000001-000@s" || n.Channel != "room" {
		t.Fatalf("pointer decode = %+v (%v), want the (channel, serial) pointer only", n, err)
	}
}

// TestEncodeNotificationCarriesAnnotationSummaries checks that an inline
// annotation cm keeps its per-row summary snapshot (DESIGN.md §14.2).
func TestEncodeNotificationCarriesAnnotationSummaries(t *testing.T) {
	a := &protocol.Annotation{Serial: "00000000000001-000@s:000", Type: "reaction:distinct.v1", Name: "x", MessageSerial: "m"}
	payload, err := msgpack.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	summary := protocol.Summary(nil).Apply(&protocol.Annotation{Action: protocol.AnnotationCreate, ClientID: "c", Type: "reaction:distinct.v1", Name: "x"})
	blob, err := msgpack.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	enc, inline, err := encodeNotification("room", "00000000000001-000@s", storage.KindAnnotation, [][]byte{payload}, [][]byte{blob})
	if err != nil || !inline {
		t.Fatalf("encode: inline=%v err=%v", inline, err)
	}
	var n busNotification
	if err := json.Unmarshal([]byte(enc), &n); err != nil {
		t.Fatal(err)
	}
	cm, err := n.inlineCM()
	if err != nil || len(cm.Annotations) != 1 || cm.Annotations[0].Summary["reaction:distinct.v1"] == nil {
		t.Fatalf("inline annotation decode = %+v, %v; want the summary snapshot", cm, err)
	}
}
