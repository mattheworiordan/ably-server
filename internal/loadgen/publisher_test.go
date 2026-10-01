package loadgen

import (
	"context"
	"testing"
	"time"
)

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

type captureSender struct{ got []*pubReq }

func (c *captureSender) send(_ context.Context, r *pubReq, _ func(error)) { c.got = append(c.got, r) }

func TestDispatchStampsTheScheduledTimeAndRecordsTheLag(t *testing.T) {
	sc, err := ParseScenario([]byte(testScenario))
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewJob(JobSpec{ID: "p", RunTag: "t1", Scenario: *sc, Role: RoleREST, Count: 1,
		Endpoints: []string{"127.0.0.1:1"}, Key: "app.key:secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snd := &captureSender{}
	s := &scheduler{j: j, transport: "rest", snd: snd, sendCtx: context.Background(), wake: make(chan struct{}, 1)}
	// The schedule called for this publish 750 ms ago: a stream held back
	// by a slow server. Its stamp must be the schedule, not now.
	intended := time.Now().Add(-750 * time.Millisecond)
	s.dispatch(&pubReq{s: &pubStream{plan: StreamPlan{Channel: "c", PubID: "p.0"}, msgBytes: 100}, seq: 4, intended: intended})
	p, ok := DecodePayload(snd.got[0].data)
	if !ok {
		t.Fatal("undecodable payload")
	}
	if p.SentAtUS != intended.UnixMicro() {
		t.Fatalf("SentAtUS %d, want the scheduled time %d", p.SentAtUS, intended.UnixMicro())
	}
	if p.LagUS < 700_000 || p.LagUS > 2_000_000 || p.ActualSendUS() < p.SentAtUS+700_000 {
		t.Fatalf("lag %d us: the actual send time must record the generator-side wait", p.LagUS)
	}
}
