package protocol

import (
	"encoding/json"
	"reflect"
	"testing"
)

// decodeSlow is the reference: encoding/json into a Message.
func decodeSlow(b []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

var simpleMessageCorpus = []string{
	`{"name":"greeting","data":"hello"}`,
	`{"data":"hello"}`,
	`{}`,
	` { "name" : "x" , "data" : "y" , "id" : "abc:0" } `,
	`{"id":"a","clientId":"c","connectionKey":"k","encoding":"utf-8","data":"z"}`,
	`{"data":"aGVsbG8=","encoding":"base64"}`,
	`{"data":"aGVsbG8=","encoding":"json/base64"}`,
	`{"data":"not base64!","encoding":"base64"}`,
	`{"name":"héllo ✓","data":"日本"}`,
	`{"name":"a","name":"b"}`,
	// Everything below must fall back to encoding/json.
	`[{"name":"x"}]`,
	`{"Name":"x"}`,
	`{"name":"x","extras":{"headers":{}}}`,
	`{"data":{"a":1}}`,
	`{"data":1}`,
	`{"data":null}`,
	`{"name":"a\"b"}`,
	`{"name":"aé"}`,
	"{\"name\":\"a\tb\"}",
	"{\"name\":\"\xff\"}",
	`{"name":"x"} trailing`,
	`{"name":"x",}`,
	`{"name":"x"`,
	`{"action":1}`,
	``,
	`"x"`,
}

func TestDecodeSimpleMessageJSONMatchesEncodingJSON(t *testing.T) {
	for _, body := range simpleMessageCorpus {
		checkSimpleAgainstSlow(t, []byte(body))
	}
}

func checkSimpleAgainstSlow(t *testing.T, body []byte) {
	t.Helper()
	fast, ok, ferr := DecodeSimpleMessageJSON(body)
	if !ok {
		return // falls back to encoding/json: nothing to compare
	}
	slow, serr := decodeSlow(body)
	if (ferr != nil) != (serr != nil) {
		t.Fatalf("%q: fast err %v, encoding/json err %v", body, ferr, serr)
	}
	if ferr != nil {
		return
	}
	if !reflect.DeepEqual(fast, slow) {
		t.Fatalf("%q: fast path decoded %+v, encoding/json %+v", body, fast, slow)
	}
}

func TestDecodeSimpleMessageJSONTakesCommonShape(t *testing.T) {
	for _, body := range []string{`{"name":"greeting","data":"hello"}`, `{"data":"x","id":"b:0"}`} {
		if _, ok, _ := DecodeSimpleMessageJSON([]byte(body)); !ok {
			t.Errorf("%s: not taken by the fast path", body)
		}
	}
}

func FuzzDecodeSimpleMessageJSON(f *testing.F) {
	for _, body := range simpleMessageCorpus {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		checkSimpleAgainstSlow(t, body)
	})
}
