package loadgen

import (
	"strings"
	"testing"
)

func TestPayloadRoundTrip(t *testing.T) {
	cases := []Payload{
		{PubID: "abc.0", Seq: 0, SentAtUS: 1, Node: 0},
		{PubID: "t1.17", Seq: 123456789, SentAtUS: 1759230000123456, Node: 9, LagUS: 250000},
		{PubID: "x", Seq: 5, SentAtUS: 7, Node: -1, LagUS: 3},
	}
	for _, p := range cases {
		for _, size := range []int{0, 10, 470, 1300} {
			s := EncodePayload(p, size)
			if size > 60 && len(s) != size {
				t.Errorf("size %d: got length %d", size, len(s))
			}
			got, ok := DecodePayload(s)
			if !ok || got != p {
				t.Errorf("round trip %+v size %d: got %+v ok=%v", p, size, got, ok)
			}
			if got2, ok := PayloadFromData([]byte(s)); !ok || got2 != p {
				t.Errorf("[]byte round trip %+v: got %+v", p, got2)
			}
		}
	}
}

func TestPayloadRejectsForeignData(t *testing.T) {
	bad := []string{
		"", "hello", "lg2|", "lg2|a|1|2|", "lg2||1|2|3|0|", "lg2|a|x|2|3|0|", "lg2|a|-1|2|3|0|",
		"lg2|a|1|y|3|0|", "lg2|a|1|2|z|0|", "lg2|a|1|2|3|w|", "lg2|a|1|2|3|", "lg1|a|1|2|3|", "0:1:2:padding", strings.Repeat("x", 100),
	}
	for _, s := range bad {
		if p, ok := DecodePayload(s); ok {
			t.Errorf("DecodePayload(%q) = %+v, want rejection", s, p)
		}
	}
	if _, ok := PayloadFromData(42); ok {
		t.Error("PayloadFromData(int) accepted")
	}
	if _, ok := PayloadFromData(nil); ok {
		t.Error("PayloadFromData(nil) accepted")
	}
}

func TestPayloadActualSendTime(t *testing.T) {
	p := Payload{PubID: "a", Seq: 1, SentAtUS: 1_000_000, LagUS: 40_000}
	got, ok := DecodePayload(EncodePayload(p, 0))
	if !ok || got.SentAtUS != 1_000_000 || got.ActualSendUS() != 1_040_000 {
		t.Fatalf("got %+v", got)
	}
}

func TestMessageIDUniquePerStreamAndSeq(t *testing.T) {
	seen := map[string]bool{}
	for _, pub := range []string{"a.0", "a.1", "b.0"} {
		for seq := int64(0); seq < 20; seq++ {
			id := MessageID(pub, seq)
			if seen[id] {
				t.Fatalf("duplicate id %s", id)
			}
			seen[id] = true
		}
	}
}
