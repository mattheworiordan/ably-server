package protocol

import "testing"

func TestPublishSize(t *testing.T) {
	msgs := []*Message{
		{Name: "ab", ClientID: "c", Data: "hello"},
		{Data: []byte{1, 2, 3}, Extras: map[string]any{"k": "v"}},
		nil,
		{Data: map[string]any{"a": 1}},
	}
	// 2+1+5 ; 3 + len(`{"k":"v"}`)=9 ; 0 ; len(`{"a":1}`)=7
	if got, want := PublishSize(msgs), int64(8+12+7); got != want {
		t.Fatalf("PublishSize = %d, want %d", got, want)
	}
}
