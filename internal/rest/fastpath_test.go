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

// The hand-written publish response must be byte-identical to
// encoding/json's (DESIGN.md §2.2).
func TestEncodePublishResponseMatchesEncodingJSON(t *testing.T) {
	cases := []publishResponse{
		{Channel: "room", MessageID: "abc:0", Serials: []string{"01700000000000-000@s:0"}},
		{Channel: "a:b:c", MessageID: "x:0", Serials: []string{"1:0", "1:1", "1:2"}},
		{Channel: "room", MessageID: "abc:0"},
		{Channel: "r<o>&m", MessageID: "abc:0", Serials: []string{"s"}},
		{Channel: "r\"q", MessageID: "a\\b", Serials: []string{"s"}},
		{Channel: "héllo", MessageID: "abc:0", Serials: []string{"s"}},
		{Channel: "room", MessageID: "abc:0", Serials: []string{"with\u2028sep"}},
		{Channel: "tab\there", MessageID: "abc:0", Serials: []string{}},
	}
	for _, v := range cases {
		got, err := encodePublishResponse(v, protocol.FormatJSON)
		if err != nil {
			t.Fatalf("%+v: %v", v, err)
		}
		want, _ := json.Marshal(v)
		if !bytes.Equal(got, want) {
			t.Errorf("%+v:\n got %s\nwant %s", v, got, want)
		}
	}
}

func TestReadBody(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		length int64
	}{
		{"exact length", `{"data":"x"}`, 12},
		{"unknown length", `{"data":"x"}`, -1},
		{"length under the body", `{"data":"xyz"}`, 5},
		{"empty", ``, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader(tc.body)))
			r.ContentLength = tc.length
			got, err := readBody(r)
			if err != nil {
				t.Fatalf("readBody: %v", err)
			}
			if string(got) != tc.body {
				t.Fatalf("readBody = %q, want %q", got, tc.body)
			}
		})
	}
	r, _ := http.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader("abc")))
	r.ContentLength = 10
	if _, err := readBody(r); err == nil {
		t.Fatal("readBody of a body shorter than its Content-Length returned no error")
	}
}
