package loadgen

import "testing"

func TestSerialFromRESTBody(t *testing.T) {
	body := `{"channel":"c","messageId":"lg-x:0","serials":["00000000001000-000@abc:000"]}`
	if got := serialFromRESTBody([]byte(body)); got != "00000000001000-000@abc" {
		t.Errorf("got %q", got)
	}
	for _, bad := range []string{``, `{}`, `{"serials":[]}`, `{"channel":"c","serials":["00000000001000-0`} {
		if got := serialFromRESTBody([]byte(bad)); got != "" {
			t.Errorf("%q: got %q, want none", bad, got)
		}
	}
}

func TestChannelSerialOf(t *testing.T) {
	if got := channelSerialOf("00000000001000-000@abc:002"); got != "00000000001000-000@abc" {
		t.Errorf("got %q", got)
	}
	if got := channelSerialOf("00000000001000-000@abc"); got != "00000000001000-000@abc" {
		t.Errorf("a bare channelSerial must be unchanged, got %q", got)
	}
}

func TestRecordStreamKeepsSerialsBySeq(t *testing.T) {
	j := &Job{streams: map[string]map[string]StreamRecord{}}
	j.recordStream("ch", "p", 0, 10, "s0")
	j.recordStream("ch", "p", 1, 20, "")
	j.recordStream("ch", "p", 2, 30, "s2")
	r := j.streams["ch"]["p"]
	if r.LastAckedSeq != 2 || r.LastAckedUS != 30 {
		t.Fatalf("record %+v", r)
	}
	if len(r.Serials) != 3 || r.Serials[0] != "s0" || r.Serials[1] != "" || r.Serials[2] != "s2" {
		t.Fatalf("serials %q: an ACK without a serial must leave a hole at its seq", r.Serials)
	}
}

func TestRecordStreamSerialLogIsCapped(t *testing.T) {
	j := &Job{streams: map[string]map[string]StreamRecord{}}
	j.serialsLogged.Store(MaxSerialLogEntries)
	j.recordStream("ch", "p", 0, 10, "s0")
	if r := j.streams["ch"]["p"]; len(r.Serials) != 0 || j.c.serialsDropped.Load() != 1 {
		t.Fatalf("record %+v dropped %d", r, j.c.serialsDropped.Load())
	}
}
