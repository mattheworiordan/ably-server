package loadgen

import (
	"fmt"
	"testing"
	"time"
)

func serialAt(i int) string { return fmt.Sprintf("%014d-000@s", 1000+i) }

type fixture struct {
	t *testing.T
	c *Checker
	a *AttachmentCheck
	n int
}

func newFixture(t *testing.T) *fixture {
	c := NewChecker()
	a := c.NewAttachment("ch", "conn1")
	a.Attached(serialAt(0), false, false)
	return &fixture{t: t, c: c, a: a}
}

// deliver sends one frame with a fresh, increasing serial.
func (f *fixture) deliver(pub string, seqs ...int64) {
	for _, s := range seqs {
		f.n++
		f.a.Frame(serialAt(f.n), []Payload{{PubID: pub, Seq: s}})
	}
}

func (f *fixture) want(kind ViolationKind, n int64) {
	f.t.Helper()
	if got := f.c.Count(kind); got != n {
		f.t.Errorf("%s = %d, want %d (summary %+v)", kind, got, n, f.c.Summary().Violations)
	}
}

func (f *fixture) wantClean() {
	f.t.Helper()
	for _, k := range ViolationKinds() {
		f.want(k, 0)
	}
}

func TestCheckerCleanStream(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 5, 6, 7, 8, 9) // a mid-stream attach starts at any seq
	f.deliver("q", 0, 1, 2)
	f.a.Finish(true)
	f.wantClean() // the attachment alone cannot tell what came before seq 5
	if f.c.Checked() != 8 {
		t.Errorf("checked = %d", f.c.Checked())
	}
	// What it can do is hand the first seq of each stream, with the attach
	// point, to the conductor (AttachCheck).
	claims := f.c.Summary().AttachClaims
	if len(claims) != 2 {
		t.Fatalf("claims = %+v, want one per stream", claims)
	}
	got := map[string]AttachClaim{}
	for _, cl := range claims {
		got[cl.PubID] = cl
	}
	if got["p"].FirstSeq != 5 || got["q"].FirstSeq != 0 || got["p"].Attach != serialAt(0) || got["p"].Channel != "ch" {
		t.Errorf("claims = %+v", got)
	}
}

// publishedLog builds a publisher record whose seq i has serial serialAt(at[i]).
func publishedLog(ch, pub string, at ...int) map[string]map[string]StreamRecord {
	rec := StreamRecord{LastAckedSeq: int64(len(at) - 1)}
	for _, a := range at {
		if a < 0 {
			rec.Serials = append(rec.Serials, "")
		} else {
			rec.Serials = append(rec.Serials, serialAt(a))
		}
	}
	return map[string]map[string]StreamRecord{ch: {pub: rec}}
}

func TestAttachCheckFindsMessagesBetweenAttachPointAndFirstSeq(t *testing.T) {
	// Seqs 0-2 were committed at or before the attach point (serialAt(0)),
	// 3 and 4 after it: the attachment started at 5, so 3 and 4 were
	// published after attaching and never delivered.
	claims := []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(0), FirstSeq: 5}}
	log := publishedLog("ch", "p", 0, 0, 0, 3, 4, 5, 6, 7)
	res := AttachCheck(log, claims)
	if res.Missed != 2 || res.Checked != 1 || res.Unverifiable != 0 {
		t.Fatalf("res = %+v, want 2 missed (seqs 3 and 4)", res)
	}
}

func TestAttachCheckCleanAndReplay(t *testing.T) {
	// Attach point 10: seqs 0-2 at or before it, seq 3 first after it.
	log := publishedLog("ch", "p", 7, 8, 10, 11, 12, 13)
	for _, c := range []struct {
		first int64
		want  int64
	}{
		{3, 0}, // exactly the first message after the attach point
		{1, 0}, // a replay from before the attach point is not a violation
		{4, 1}, // seq 3 was published after the attach point and lost
		{6, 3}, // 3, 4 and 5; seq 6 is past the log's end but 5 is logged
	} {
		res := AttachCheck(log, []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: c.first}})
		if res.Missed != c.want {
			t.Errorf("first seq %d: missed %d, want %d (%+v)", c.first, res.Missed, c.want, res)
		}
	}
}

func TestAttachCheckStreamStartedAfterAttach(t *testing.T) {
	// The stream's seq 0 was published after the attach point and the
	// attachment's first delivery was seq 2: the pre-fix checker saw a
	// clean stream here (the first seq seeds the expectation).
	log := publishedLog("ch", "p", 11, 12, 13)
	res := AttachCheck(log, []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: 2}})
	if res.Missed != 2 || len(res.Examples) != 1 || res.Examples[0].Kind != "attach_gap" || res.Examples[0].Count != 2 {
		t.Fatalf("res = %+v", res)
	}
}

func TestAttachCheckUnverifiable(t *testing.T) {
	log := publishedLog("ch", "p", 7, 8, 9)
	res := AttachCheck(log, []AttachClaim{
		{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: 9}, // attach after the log ends, first seq beyond it
		{Channel: "ch", PubID: "other", Attach: serialAt(10), FirstSeq: 0},
		{Channel: "nochan", PubID: "p", Attach: serialAt(10), FirstSeq: 0},
	})
	if res.Unverifiable != 3 || res.Checked != 0 || res.Missed != 0 {
		t.Fatalf("res = %+v, want all three unverifiable and none passed", res)
	}
	// Attach after the log's end, first seq just past it: consistent.
	res = AttachCheck(log, []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: 3}})
	if res.Checked != 1 || res.Missed != 0 {
		t.Fatalf("res = %+v", res)
	}
}

func TestAttachCheckHoleInTheLogIsUnknownNotLoss(t *testing.T) {
	// Seq 1's ACK carried no serial: whether it was after the attach point
	// is unknown, so a claim that needs it cannot be settled.
	log := publishedLog("ch", "p", 7, -1, 12, 13)
	res := AttachCheck(log, []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: 3}})
	if res.Unverifiable != 1 || res.Missed != 0 {
		t.Fatalf("res = %+v", res)
	}
	// A hole after the first post-attach serial does not matter.
	log = publishedLog("ch", "p", 7, 12, -1, 13)
	res = AttachCheck(log, []AttachClaim{{Channel: "ch", PubID: "p", Attach: serialAt(10), FirstSeq: 3}})
	if res.Checked != 1 || res.Missed != 2 {
		t.Fatalf("res = %+v, want seqs 1 and 2 missed (first after the attach point is 1, attachment started at 3)", res)
	}
}

func TestCheckerSeedFollowsDiscontinuityNotResume(t *testing.T) {
	c := NewChecker()
	a := c.NewAttachment("ch", "c")
	a.Attached(serialAt(0), false, false)
	a.Frame(serialAt(1), []Payload{{PubID: "p", Seq: 0}})
	// An honoured resume keeps the original attach point for streams first
	// seen afterwards.
	a.Attached(serialAt(1), true, true)
	a.Frame(serialAt(2), []Payload{{PubID: "q", Seq: 0}})
	// A declined resume re-seeds from the new attach point.
	a.Attached(serialAt(50), true, false)
	a.Frame(serialAt(51), []Payload{{PubID: "p", Seq: 40}})
	a.Finish(true)
	var attach []string
	for _, cl := range c.Summary().AttachClaims {
		attach = append(attach, cl.PubID+"@"+cl.Attach)
	}
	want := []string{"p@" + serialAt(0), "p@" + serialAt(50), "q@" + serialAt(0)} // claims come out sorted
	if fmt.Sprint(attach) != fmt.Sprint(want) {
		t.Fatalf("claims %v, want %v", attach, want)
	}
}

func TestCheckerAttachClaimsAreCapped(t *testing.T) {
	c := NewChecker()
	for i := range MaxAttachClaims + 3 {
		c.addClaim(AttachClaim{Channel: "ch", FirstSeq: int64(i)})
	}
	c.addClaim(AttachClaim{Channel: "ch", FirstSeq: 0}) // a repeat of one kept: not dropped
	s := c.Summary()
	if len(s.AttachClaims) != MaxAttachClaims || s.AttachClaimsDropped != 3 {
		t.Fatalf("%d kept, %d dropped", len(s.AttachClaims), s.AttachClaimsDropped)
	}
}

func TestCheckerIdenticalClaimsAreCountedOnce(t *testing.T) {
	// 1000 attachments of one hot channel at the same attach point, all
	// starting at seq 7, are one claim with N=1000, and weigh 1000 in the
	// check.
	c := NewChecker()
	for range 1000 {
		c.addClaim(AttachClaim{Channel: "hot", PubID: "p", Attach: serialAt(10), FirstSeq: 7})
	}
	c.addClaim(AttachClaim{Channel: "hot", PubID: "p", Attach: serialAt(11), FirstSeq: 7})
	claims := c.Summary().AttachClaims
	if len(claims) != 2 || claims[0].N != 1000 || claims[1].N != 1 {
		t.Fatalf("claims %+v", claims)
	}
	log := publishedLog("hot", "p", 7, 8, 11, 11, 11, 11, 11, 11, 11, 11)
	res := AttachCheck(log, claims)
	// Attach point 10: first seq after it is 2, claimed 7: five missed by
	// each of 1000 attachments; attach point 11 (serials equal it at 2..):
	// every later serial is 11 which is not after 11, so none is after it.
	if res.Claims != 1001 || res.Checked != 1001 || res.Missed != 5000 {
		t.Fatalf("res %+v", res)
	}
}

func TestCheckerDetectsDuplicate(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 2, 2, 3)
	f.a.Finish(true)
	f.want(Duplicate, 1)
	f.want(Gap, 0)
	f.want(Reorder, 0)
	s := f.c.Summary()
	if len(s.FirstViolations) != 1 || s.FirstViolations[0].Kind != "duplicate" || s.FirstViolations[0].Seq != 2 {
		t.Errorf("logged = %+v", s.FirstViolations)
	}
}

func TestCheckerDetectsDuplicateFrame(t *testing.T) {
	// The same frame delivered twice: same serial and same seq.
	f := newFixture(t)
	f.deliver("p", 0)
	f.a.Frame(serialAt(f.n), []Payload{{PubID: "p", Seq: 0}})
	f.deliver("p", 1)
	f.a.Finish(true)
	f.want(Duplicate, 1)
	f.want(SerialRegression, 1)
}

func TestCheckerDetectsGap(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 4, 5)
	if f.c.OpenGaps() != 2 {
		t.Errorf("open gaps = %d, want 2", f.c.OpenGaps())
	}
	f.want(Gap, 0) // provisional until the attachment ends
	f.a.Finish(true)
	f.want(Gap, 2)
	f.want(Reorder, 0)
	if f.c.OpenGaps() != 0 {
		t.Errorf("open gaps after finish = %d", f.c.OpenGaps())
	}
}

func TestCheckerDetectsReorder(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 3, 2, 4)
	f.a.Finish(true)
	f.want(Reorder, 1)
	f.want(Gap, 0)
	f.want(Duplicate, 0)
}

func TestCheckerReorderThenDuplicateOfFilled(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 3, 1, 1, 2)
	f.a.Finish(true)
	f.want(Reorder, 2)   // 1 and 2 arrived after 3
	f.want(Duplicate, 1) // the second 1
	f.want(Gap, 0)
}

func TestCheckerFillSplitsRanges(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 10) // missing 1..9
	f.deliver("p", 5, 1, 9)
	f.a.Finish(true)
	f.want(Reorder, 3)
	f.want(Gap, 6) // 2,3,4,6,7,8
}

func TestCheckerStreamsAreIndependent(t *testing.T) {
	f := newFixture(t)
	f.deliver("a", 0)
	f.deliver("b", 7)
	f.deliver("a", 1)
	f.deliver("b", 8)
	f.deliver("a", 2)
	f.a.Finish(true)
	f.wantClean()
}

func TestCheckerDetectsSerialRegression(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0)
	f.a.Frame(serialAt(0), []Payload{{PubID: "p", Seq: 1}}) // not after the attach point
	f.a.Finish(true)
	f.want(SerialRegression, 1)
	f.want(Duplicate, 0)
}

func TestCheckerFirstMessageBeforeAttachPoint(t *testing.T) {
	c := NewChecker()
	a := c.NewAttachment("ch", "c")
	a.Attached(serialAt(10), false, false)
	a.Frame(serialAt(5), []Payload{{PubID: "p", Seq: 0}})
	a.Finish(true)
	if c.Count(SerialRegression) != 1 {
		t.Fatalf("a delivery at or before the attach point must be flagged")
	}
}

func TestCheckerHonouredResumeMustBeContinuous(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 2)
	// Reconnect: re-attach from the last serial, server says RESUMED,
	// but seq 3 never arrives.
	f.a.Attached(serialAt(f.n), true, true)
	f.deliver("p", 4, 5)
	f.a.Finish(true)
	f.want(ResumeGap, 1)
	f.want(Gap, 0)
}

func TestCheckerHonouredResumeClean(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 2)
	f.a.Attached(serialAt(f.n), true, true)
	f.deliver("p", 3, 4)
	f.a.Finish(true)
	f.wantClean()
	if s := f.c.Summary(); s.Resumes != 1 || s.Discontinuities != 0 {
		t.Errorf("resumes=%d discontinuities=%d", s.Resumes, s.Discontinuities)
	}
}

func TestCheckerDuplicateAcrossResume(t *testing.T) {
	// The replay repeats a message the attachment already had.
	f := newFixture(t)
	f.deliver("p", 0, 1, 2)
	f.a.Attached(serialAt(f.n), true, true)
	f.deliver("p", 2, 3)
	f.a.Finish(true)
	f.want(Duplicate, 1)
}

func TestCheckerSignalledDiscontinuityIsNotAViolation(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 1, 2)
	f.a.Attached(serialAt(f.n+50), true, false) // resume refused: attach at head
	f.n += 50
	f.deliver("p", 40, 41)
	f.a.Finish(true)
	f.wantClean()
	s := f.c.Summary()
	if s.Discontinuities != 1 {
		t.Errorf("discontinuities = %d", s.Discontinuities)
	}
	if len(s.Channels) != 0 {
		t.Errorf("a discontinuous attachment must not feed the tail check: %+v", s.Channels)
	}
}

func TestCheckerGapBeforeDiscontinuityStillCounts(t *testing.T) {
	f := newFixture(t)
	f.deliver("p", 0, 2) // 1 lost on the live path
	f.a.Attached(serialAt(f.n+5), true, false)
	f.a.Finish(true)
	f.want(Gap, 1)
}

func TestCheckerLogCap(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 30; i++ {
		f.deliver("p", 0)
	}
	f.want(Duplicate, 29)
	if n := len(f.c.Summary().FirstViolations); n != MaxLoggedViolations {
		t.Errorf("logged %d, want %d", n, MaxLoggedViolations)
	}
}

func TestCheckerOnViolationHook(t *testing.T) {
	c := NewChecker()
	var got [numViolationKinds]int64
	c.OnViolation = func(k ViolationKind, n int64) { got[k] += n }
	a := c.NewAttachment("ch", "")
	a.Attached(serialAt(0), false, false)
	for i, s := range []int64{0, 0, 3} {
		a.Frame(serialAt(i+1), []Payload{{PubID: "p", Seq: s}})
	}
	a.Finish(true)
	if got[Duplicate] != 1 || got[Gap] != 2 {
		t.Errorf("hook saw %v", got)
	}
}

func TestTailCheck(t *testing.T) {
	c := NewChecker()
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	// Two continuous attachments on ch; one saw up to 9, one up to 7.
	a1 := c.NewAttachment("ch", "c1")
	a1.Attached(serialAt(0), false, false)
	for i := int64(0); i <= 9; i++ {
		a1.Frame(serialAt(int(i)+1), []Payload{{PubID: "p", Seq: i}})
	}
	a1.Finish(true)
	a2 := c.NewAttachment("ch", "c2")
	a2.Attached(serialAt(0), false, false)
	for i := int64(0); i <= 7; i++ {
		a2.Frame(serialAt(int(i)+1), []Payload{{PubID: "p", Seq: i}})
	}
	a2.Finish(true)
	// A non-continuous attachment is ignored.
	a3 := c.NewAttachment("ch", "c3")
	a3.Attached(serialAt(0), false, false)
	a3.Finish(false)

	seen := c.Summary().Channels
	if cs := seen["ch"]; cs == nil || cs.Attachments != 2 || cs.MinMaxSeq["p"] != 7 {
		t.Fatalf("seen = %+v", seen["ch"])
	}
	late := now.Add(5 * time.Second).UnixMicro()
	pub := map[string]map[string]StreamRecord{"ch": {"p": {LastAckedSeq: 9, LastAckedUS: late}}}
	res := TailCheck(pub, []map[string]*ChannelSeen{seen}, time.Second)
	if res.Lost != 2 || res.Checked != 1 || len(res.Examples) != 1 || res.Examples[0].Kind != "tail_loss" {
		t.Fatalf("res=%+v", res)
	}
	// Acked only up to 7: nothing lost.
	pub["ch"]["p"] = StreamRecord{LastAckedSeq: 7, LastAckedUS: late}
	if res := TailCheck(pub, []map[string]*ChannelSeen{seen}, time.Second); res.Lost != 0 || res.Checked != 1 {
		t.Fatalf("res=%+v, want 0 lost of 1 checked", res)
	}
	// The last ack came before the latest attach (+ margin): not checkable.
	pub["ch"]["p"] = StreamRecord{LastAckedSeq: 100, LastAckedUS: now.UnixMicro()}
	if res := TailCheck(pub, []map[string]*ChannelSeen{seen}, time.Second); res.Lost != 0 || res.Checked != 0 {
		t.Fatalf("res=%+v, want nothing checkable", res)
	}
	// A stream no continuous attachment ever saw, acked well after they
	// attached: loss.
	pub["ch"]["q"] = StreamRecord{LastAckedSeq: 3, LastAckedUS: late}
	if res := TailCheck(pub, []map[string]*ChannelSeen{seen}, time.Second); res.Lost != 1 {
		t.Fatalf("res=%+v, want 1 lost for an unseen stream", res)
	}
}

func TestTailCheckCountsStreamsSkippedForTheMargin(t *testing.T) {
	// Two subscriber processes hold ch. Stream p is skipped for the margin
	// on the one that attached late and checked on the other: it is
	// checked, not skipped. Stream q is skipped on both: one stream never
	// checked, two pairs skipped.
	early := map[string]*ChannelSeen{"ch": {LatestAttachUS: 1_000_000, Attachments: 1, MinMaxSeq: map[string]int64{"p": 9, "q": 3}}}
	late := map[string]*ChannelSeen{"ch": {LatestAttachUS: 4_000_000, Attachments: 1, MinMaxSeq: map[string]int64{"p": 9, "q": 3}}}
	pub := map[string]map[string]StreamRecord{"ch": {
		"p": {LastAckedSeq: 9, LastAckedUS: 3_000_000},
		"q": {LastAckedSeq: 3, LastAckedUS: 1_500_000},
	}}
	res := TailCheck(pub, []map[string]*ChannelSeen{early, late}, time.Second)
	if res.StreamsChecked != 1 || res.StreamsSkippedMargin != 1 || res.SkippedMargin != 3 || res.Lost != 0 {
		t.Fatalf("res=%+v, want 1 stream checked, 1 stream and 3 pairs skipped for the margin", res)
	}
}

func TestTailCheckMinAcrossAttachmentsWithUnseenStream(t *testing.T) {
	c := NewChecker()
	a1 := c.NewAttachment("ch", "")
	a1.Attached("", false, false)
	a1.Frame(serialAt(1), []Payload{{PubID: "p", Seq: 4}})
	a1.Finish(true)
	a2 := c.NewAttachment("ch", "")
	a2.Attached("", false, false)
	a2.Frame(serialAt(1), []Payload{{PubID: "q", Seq: 1}})
	a2.Finish(true)
	cs := c.Summary().Channels["ch"]
	if cs.MinMaxSeq["p"] != -1 || cs.MinMaxSeq["q"] != -1 {
		t.Fatalf("each stream was unseen by one attachment: %+v", cs.MinMaxSeq)
	}
}
