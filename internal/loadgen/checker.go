package loadgen

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ViolationKind names one way delivery on a checked channel can be wrong.
// One fault can raise more than one kind (a duplicated frame is both a
// duplicate seq and a channelSerial that did not advance); a passing run
// has zero of every kind.
type ViolationKind int

const (
	// Duplicate: a (channel, pubID, seq) delivered more than once on one
	// attachment.
	Duplicate ViolationKind = iota
	// Gap: a seq never delivered on an attachment that saw later seqs of
	// the same stream (counted per missing message when the attachment
	// ends).
	Gap
	// Reorder: a seq delivered after a later seq of the same stream.
	Reorder
	// SerialRegression: a MESSAGE whose channelSerial is not strictly
	// greater than the previous one on the attachment (or than the
	// ATTACHED attach point), DESIGN.md §8.
	SerialRegression
	// ResumeGap: a Gap whose missing messages span a re-attach that the
	// server reported as RESUMED, i.e. continuity was promised and not
	// kept (DESIGN.md §4.3).
	ResumeGap
	// TailLoss: a message the publisher saw acknowledged that a
	// subscriber attached throughout never received. Computed by the
	// conductor from publisher and subscriber stream records.
	TailLoss
	// AttachGap: a message published after an attachment's attach point
	// (its ATTACHED channelSerial) that the attachment never received,
	// between the attach point and the first message it did receive from
	// that stream. Computed by the conductor from the subscribers' attach
	// claims and the publishers' serial logs (AttachCheck).
	AttachGap
	numViolationKinds
)

var violationNames = [numViolationKinds]string{"duplicate", "gap", "reorder", "serial_regression", "resume_gap", "tail_loss", "attach_gap"}

func (k ViolationKind) String() string {
	if k >= 0 && k < numViolationKinds {
		return violationNames[k]
	}
	return "unknown"
}

// ViolationKinds lists every kind, in a stable order.
func ViolationKinds() []ViolationKind {
	out := make([]ViolationKind, numViolationKinds)
	for i := range out {
		out[i] = ViolationKind(i)
	}
	return out
}

// Violation is one logged instance, kept verbatim for the first few so a
// failing run can be diagnosed from its summary alone.
type Violation struct {
	Kind       string `json:"kind"`
	Channel    string `json:"channel"`
	PubID      string `json:"pub_id,omitempty"`
	Seq        int64  `json:"seq"`
	Expected   int64  `json:"expected,omitempty"`
	Count      int64  `json:"count,omitempty"`
	Serial     string `json:"serial,omitempty"`
	LastSerial string `json:"last_serial,omitempty"`
	Conn       string `json:"conn,omitempty"`
	AtUS       int64  `json:"at_us"`
}

// MaxLoggedViolations is how many violations are kept verbatim.
const MaxLoggedViolations = 10

// Checker aggregates serial-continuity checks for one generator process.
// Each attachment on a sampled channel gets an AttachmentCheck, driven
// only by the goroutine that reads that connection, so per-attachment
// state needs no lock; the counts are atomic and the verbatim log and
// the stream records are behind a mutex touched only on a violation or
// when an attachment finishes.
type Checker struct {
	counts  [numViolationKinds]atomic.Int64
	checked atomic.Int64
	// openGaps is the number of currently missing messages across live
	// attachments: provisional, because a late arrival turns a missing
	// message into a reorder.
	openGaps        atomic.Int64
	discontinuities atomic.Int64
	resumes         atomic.Int64

	mu     sync.Mutex
	logged []Violation
	// claims are the attach-point claims of every attachment (see
	// AttachClaim), capped at MaxAttachClaims.
	claims        []AttachClaim
	claimsDropped int64
	// seen is per channel: the latest attach time of any attachment that
	// finished continuous, and per stream the lowest maxSeen across those
	// attachments (-1 for one that saw nothing from the stream).
	seen map[string]*ChannelSeen

	// OnViolation, if set, is called for every counted violation (the
	// Prometheus hook). Set it before any attachment is created.
	OnViolation func(kind ViolationKind, n int64)

	now func() time.Time
}

// MaxAttachClaims caps the attach claims one process keeps. Past it
// claims are counted, not kept, and the coverage gate reports them.
const MaxAttachClaims = 500_000

// AttachClaim is one stream's first observation on one attachment: the
// attach point the server gave (ATTACHED.channelSerial) and the first seq
// of the stream the attachment received. The conductor compares it with
// the publisher's serial log: the first seq whose serial is after the
// attach point must be FirstSeq, or the messages in between were lost.
type AttachClaim struct {
	Channel  string `json:"c"`
	PubID    string `json:"p"`
	Attach   string `json:"a"`
	FirstSeq int64  `json:"s"`
}

func (c *Checker) addClaim(cl AttachClaim) {
	c.mu.Lock()
	if len(c.claims) < MaxAttachClaims {
		c.claims = append(c.claims, cl)
	} else {
		c.claimsDropped++
	}
	c.mu.Unlock()
}

// ChannelSeen is a subscriber process's record of one sampled channel,
// used for the tail-loss check.
type ChannelSeen struct {
	// LatestAttachUS is the latest local time (unix µs) at which any
	// continuous attachment in this process became attached.
	LatestAttachUS int64 `json:"latest_attach_us"`
	// Attachments counts the continuous attachments folded in.
	Attachments int `json:"attachments"`
	// MinMaxSeq maps pubID to the lowest, across continuous attachments,
	// of the highest seq each attachment received (-1: some attachment
	// received nothing from that stream).
	MinMaxSeq map[string]int64 `json:"min_max_seq"`
}

// NewChecker returns an empty Checker.
func NewChecker() *Checker {
	return &Checker{seen: make(map[string]*ChannelSeen), now: time.Now}
}

func (c *Checker) count(kind ViolationKind, n int64, v Violation) {
	if n <= 0 {
		return
	}
	c.counts[kind].Add(n)
	if c.OnViolation != nil {
		c.OnViolation(kind, n)
	}
	v.Kind = kind.String()
	if n > 1 {
		v.Count = n
	}
	v.AtUS = c.now().UnixMicro()
	c.mu.Lock()
	if len(c.logged) < MaxLoggedViolations {
		c.logged = append(c.logged, v)
	}
	c.mu.Unlock()
}

// Count returns the number of violations of kind so far.
func (c *Checker) Count(kind ViolationKind) int64 { return c.counts[kind].Load() }

// Checked returns the number of messages checked.
func (c *Checker) Checked() int64 { return c.checked.Load() }

// OpenGaps returns the number of currently missing (provisional) messages.
func (c *Checker) OpenGaps() int64 { return c.openGaps.Load() }

// CorrectnessSummary is the checker's part of a run summary.
type CorrectnessSummary struct {
	CheckedMessages int64                   `json:"checked_messages"`
	Violations      map[string]int64        `json:"violations"`
	Discontinuities int64                   `json:"discontinuities"`
	Resumes         int64                   `json:"resumes"`
	FirstViolations []Violation             `json:"first_violations,omitempty"`
	Channels        map[string]*ChannelSeen `json:"channels,omitempty"`
	// AttachClaims are the attach-point claims (see AttachClaim);
	// AttachClaimsDropped counts those past MaxAttachClaims.
	AttachClaims        []AttachClaim `json:"attach_claims,omitempty"`
	AttachClaimsDropped int64         `json:"attach_claims_dropped,omitempty"`
}

// Summary snapshots the checker. Call it after every attachment has
// finished for complete stream records.
func (c *Checker) Summary() CorrectnessSummary {
	s := CorrectnessSummary{
		CheckedMessages: c.checked.Load(),
		Violations:      make(map[string]int64, numViolationKinds),
		Discontinuities: c.discontinuities.Load(),
		Resumes:         c.resumes.Load(),
	}
	for _, k := range ViolationKinds() {
		s.Violations[k.String()] = c.counts[k].Load()
	}
	c.mu.Lock()
	s.FirstViolations = append([]Violation(nil), c.logged...)
	s.AttachClaims = append([]AttachClaim(nil), c.claims...)
	s.AttachClaimsDropped = c.claimsDropped
	s.Channels = make(map[string]*ChannelSeen, len(c.seen))
	for ch, cs := range c.seen {
		cp := *cs
		cp.MinMaxSeq = make(map[string]int64, len(cs.MinMaxSeq))
		for k, v := range cs.MinMaxSeq {
			cp.MinMaxSeq[k] = v
		}
		s.Channels[ch] = &cp
	}
	c.mu.Unlock()
	return s
}

// seqRange is an inclusive range of missing seqs.
type seqRange struct {
	lo, hi       int64
	acrossResume bool
}

type streamState struct {
	next         int64 // next expected seq
	max          int64
	missing      []seqRange // sorted, disjoint
	acrossResume bool       // a RESUMED re-attach happened since this stream's last message
}

func (s *streamState) missingCount() int64 {
	var n int64
	for _, r := range s.missing {
		n += r.hi - r.lo + 1
	}
	return n
}

// fill removes seq from the missing ranges, reporting whether it was
// missing.
func (s *streamState) fill(seq int64) bool {
	for i, r := range s.missing {
		if seq < r.lo {
			return false
		}
		if seq > r.hi {
			continue
		}
		switch {
		case r.lo == r.hi:
			s.missing = append(s.missing[:i], s.missing[i+1:]...)
		case seq == r.lo:
			s.missing[i].lo++
		case seq == r.hi:
			s.missing[i].hi--
		default:
			tail := seqRange{lo: seq + 1, hi: r.hi, acrossResume: r.acrossResume}
			s.missing[i].hi = seq - 1
			s.missing = append(s.missing, seqRange{})
			copy(s.missing[i+2:], s.missing[i+1:])
			s.missing[i+1] = tail
		}
		return true
	}
	return false
}

// AttachmentCheck checks one attachment (connection, channel) across its
// whole life, including re-attaches after a reconnect. Not safe for
// concurrent use: drive it from the connection's reader goroutine.
type AttachmentCheck struct {
	c          *Checker
	channel    string
	conn       string
	lastSerial string
	// seed is the attach point from which this attachment's continuity
	// began: ATTACHED.channelSerial of the last attach that was not a
	// resume (a resume keeps the earlier seed, a declined resume starts a
	// new one). A stream's first message is claimed against it.
	seed       string
	streams    map[string]*streamState
	attachedUS int64
	continuous bool
	finished   bool
}

// NewAttachment starts checking channel on connection conn (conn is only
// used in logged violations).
func (c *Checker) NewAttachment(channel, conn string) *AttachmentCheck {
	return &AttachmentCheck{c: c, channel: channel, conn: conn, streams: make(map[string]*streamState), continuous: true}
}

// SetConn updates the connection label (after a reconnect).
func (a *AttachmentCheck) SetConn(conn string) { a.conn = conn }

// Attached records an ATTACHED for this attachment. attachSerial is
// ATTACHED.channelSerial; resumeRequested is whether the ATTACH carried a
// channelSerial cursor; resumed is the RESUMED flag. A resume the server
// did not honour is a signalled discontinuity (DESIGN.md §4.3): messages
// missing across it are not violations, so stream expectations restart,
// and the attachment no longer counts as continuous for the tail check.
func (a *AttachmentCheck) Attached(attachSerial string, resumeRequested, resumed bool) {
	now := a.c.now().UnixMicro()
	if !resumeRequested {
		a.attachedUS = now
		a.lastSerial = attachSerial
		a.seed = attachSerial
		return
	}
	a.c.resumes.Add(1)
	if resumed {
		for _, s := range a.streams {
			s.acrossResume = true
		}
		// The replay starts strictly after the cursor we sent, which is
		// what ATTACHED echoes as the attach point; keep lastSerial as the
		// last one we actually received.
		if attachSerial > a.lastSerial {
			a.lastSerial = attachSerial
		}
		return
	}
	a.c.discontinuities.Add(1)
	a.continuous = false
	a.finalizeGaps()
	a.streams = make(map[string]*streamState)
	a.lastSerial = attachSerial
	a.seed = attachSerial
}

// Frame checks one delivered MESSAGE frame: its channelSerial and the
// payloads it carries (non-generator messages are skipped by the caller).
func (a *AttachmentCheck) Frame(serial string, payloads []Payload) {
	if serial != "" {
		if a.lastSerial != "" && serial <= a.lastSerial {
			a.c.count(SerialRegression, 1, Violation{Channel: a.channel, Serial: serial, LastSerial: a.lastSerial, Conn: a.conn})
		} else {
			a.lastSerial = serial
		}
	}
	for _, p := range payloads {
		a.observe(p, serial)
	}
}

func (a *AttachmentCheck) observe(p Payload, serial string) {
	a.c.checked.Add(1)
	s := a.streams[p.PubID]
	if s == nil {
		a.streams[p.PubID] = &streamState{next: p.Seq + 1, max: p.Seq}
		// Nothing in this attachment can tell whether seqs before p.Seq
		// were published after the attach point: that is the attach
		// claim's job (AttachCheck).
		if a.seed != "" {
			a.c.addClaim(AttachClaim{Channel: a.channel, PubID: p.PubID, Attach: a.seed, FirstSeq: p.Seq})
		}
		return
	}
	across := s.acrossResume
	s.acrossResume = false
	switch {
	case p.Seq == s.next:
		s.next++
		s.max = p.Seq
	case p.Seq > s.next:
		n := p.Seq - s.next
		s.missing = append(s.missing, seqRange{lo: s.next, hi: p.Seq - 1, acrossResume: across})
		a.c.openGaps.Add(n)
		s.next = p.Seq + 1
		s.max = p.Seq
	default:
		if s.fill(p.Seq) {
			a.c.openGaps.Add(-1)
			a.c.count(Reorder, 1, Violation{Channel: a.channel, PubID: p.PubID, Seq: p.Seq, Expected: s.next, Serial: serial, Conn: a.conn})
		} else {
			a.c.count(Duplicate, 1, Violation{Channel: a.channel, PubID: p.PubID, Seq: p.Seq, Expected: s.next, Serial: serial, Conn: a.conn})
		}
	}
}

// finalizeGaps turns every still-missing message into a Gap (or a
// ResumeGap when the range spans an honoured resume).
func (a *AttachmentCheck) finalizeGaps() {
	for pub, s := range a.streams {
		for _, r := range s.missing {
			n := r.hi - r.lo + 1
			a.c.openGaps.Add(-n)
			kind := Gap
			if r.acrossResume {
				kind = ResumeGap
			}
			a.c.count(kind, n, Violation{Channel: a.channel, PubID: pub, Seq: r.lo, Expected: r.hi, Conn: a.conn})
		}
		s.missing = nil
	}
}

// Finish ends the attachment. endedContinuous says whether it stayed
// attached (possibly across honoured resumes) until the end of the run;
// only such attachments feed the tail-loss record. Idempotent.
func (a *AttachmentCheck) Finish(endedContinuous bool) {
	if a.finished {
		return
	}
	a.finished = true
	a.finalizeGaps()
	if !endedContinuous || !a.continuous || a.attachedUS == 0 {
		return
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	cs := a.c.seen[a.channel]
	if cs == nil {
		cs = &ChannelSeen{MinMaxSeq: make(map[string]int64)}
		a.c.seen[a.channel] = cs
	}
	// A stream this attachment never saw lowers the channel's minimum to
	// -1; a stream earlier attachments never saw is lowered the same way.
	if cs.Attachments > 0 {
		for pub := range a.streams {
			if _, ok := cs.MinMaxSeq[pub]; !ok {
				cs.MinMaxSeq[pub] = -1
			}
		}
	}
	for pub, v := range cs.MinMaxSeq {
		if s, ok := a.streams[pub]; !ok {
			cs.MinMaxSeq[pub] = -1
		} else if s.max < v {
			cs.MinMaxSeq[pub] = s.max
		}
	}
	if cs.Attachments == 0 {
		for pub, s := range a.streams {
			cs.MinMaxSeq[pub] = s.max
		}
	}
	cs.Attachments++
	cs.LatestAttachUS = max(cs.LatestAttachUS, a.attachedUS)
}

// StreamRecord is a publisher's record of one stream on a sampled
// channel: the highest seq it saw acknowledged and when, and the
// channelSerial the server assigned to each acknowledged seq.
type StreamRecord struct {
	LastAckedSeq int64 `json:"last_acked_seq"`
	LastAckedUS  int64 `json:"last_acked_us"`
	// Serials[seq] is the channelSerial of that seq's message ("" when
	// the ACK carried none). Streams never skip a seq, so the index is
	// the seq. It ends at the last ACK before the end of the hold.
	Serials []string `json:"serials,omitempty"`
}

// TailResult is the outcome of TailCheck.
type TailResult struct {
	// Checked counts (subscriber process, channel, stream) triples that
	// were checkable.
	Checked int64 `json:"checked"`
	// Lost counts acknowledged messages some continuous subscriber never
	// received (per subscriber process).
	Lost int64 `json:"lost"`
	// StreamsChecked counts distinct streams checked on at least one
	// subscriber process.
	StreamsChecked int64 `json:"streams_checked"`
	// SkippedMargin counts (subscriber process, channel, stream) triples
	// not checked because the stream's last acknowledgement came less
	// than the margin after the latest continuous attach, so a subscriber
	// attached then could not be held to it.
	SkippedMargin int64 `json:"skipped_margin"`
	// Margin is the margin used.
	Margin   time.Duration `json:"margin_ns"`
	Examples []Violation   `json:"examples,omitempty"`
}

// TailCheck compares publisher stream records with subscriber records
// for the sampled channels and returns how many acknowledged messages
// some continuous subscriber never received, plus up to
// MaxLoggedViolations examples. A stream is checked on a channel only if
// its last acknowledgement came at least margin after the latest attach
// of any continuous attachment on that channel (so every such attachment
// was live for it); publishers and subscribers must share a clock to
// within margin (chrony on every box).
func TailCheck(published map[string]map[string]StreamRecord, seen []map[string]*ChannelSeen, margin time.Duration) TailResult {
	res := TailResult{Margin: margin}
	type streamKey struct{ ch, pub string }
	checkedStreams := map[streamKey]bool{}
	for _, bySub := range seen {
		for ch, cs := range bySub {
			streams := published[ch]
			for pub, rec := range streams {
				if rec.LastAckedUS < cs.LatestAttachUS+margin.Microseconds() {
					res.SkippedMargin++
					continue
				}
				res.Checked++
				checkedStreams[streamKey{ch, pub}] = true
				got, ok := cs.MinMaxSeq[pub]
				if !ok {
					got = -1
				}
				if got >= rec.LastAckedSeq {
					continue
				}
				// got == -1 means some attachment saw nothing from the
				// stream; it is only known to have missed messages acked
				// after it attached, so count the stream's tail from its
				// highest record conservatively as one message.
				n := rec.LastAckedSeq - got
				if got < 0 {
					n = 1
				}
				res.Lost += n
				if len(res.Examples) < MaxLoggedViolations {
					res.Examples = append(res.Examples, Violation{
						Kind: TailLoss.String(), Channel: ch, PubID: pub,
						Seq: got + 1, Expected: rec.LastAckedSeq, Count: n,
						AtUS: rec.LastAckedUS,
					})
				}
			}
		}
	}
	res.StreamsChecked = int64(len(checkedStreams))
	return res
}

// AttachResult is the outcome of AttachCheck.
type AttachResult struct {
	// Claims counts the attach claims examined.
	Claims int64 `json:"claims"`
	// Checked counts the claims the publisher's serial log could settle.
	Checked int64 `json:"checked"`
	// Unverifiable counts claims it could not settle: no publisher log
	// for the stream, a log that ends before the attach point is passed,
	// or an unknown serial in the way.
	Unverifiable int64 `json:"unverifiable"`
	// Dropped counts claims the subscribers did not keep (cap).
	Dropped int64 `json:"dropped,omitempty"`
	// Missed counts messages published after an attach point and never
	// delivered before the stream's first delivered message.
	Missed   int64       `json:"missed"`
	Examples []Violation `json:"examples,omitempty"`
}

// AttachCheck closes the first-message blind spot of the per-attachment
// checks. An attachment learns a stream's seq only from its first
// delivery, so anything between the attach point (ATTACHED.channelSerial)
// and that first delivery is invisible to it. The publisher knows every
// message's channelSerial (from the REST response or the realtime ACK),
// so for each claim the first seq with a serial after the attach point is
// the seq the attachment must have started at; a later FirstSeq means the
// messages in between, published after the attach point, were lost.
// A FirstSeq at or before it (a replay from before the attach point) is
// not a violation.
//
// published is keyed channel then pubID. The check is exact where the log
// covers the claim and silent about the rest (Unverifiable): serials are
// compared as strings, the order the server itself uses (DESIGN.md §8),
// so no clock is involved.
func AttachCheck(published map[string]map[string]StreamRecord, claims []AttachClaim) AttachResult {
	var res AttachResult
	type streamKey struct{ ch, pub string }
	hasHole := map[streamKey]bool{}
	for _, cl := range claims {
		res.Claims++
		rec, ok := published[cl.Channel][cl.PubID]
		n := len(rec.Serials)
		if !ok || n == 0 {
			res.Unverifiable++
			continue
		}
		// First index with a serial after the attach point.
		first := -1
		sk := streamKey{cl.Channel, cl.PubID}
		holes, seen := hasHole[sk]
		if !seen {
			holes = slices.Contains(rec.Serials, "")
			hasHole[sk] = holes
		}
		if !holes {
			if i := sort.Search(n, func(i int) bool { return rec.Serials[i] > cl.Attach }); i < n {
				first = i
			}
		} else {
			for i, sr := range rec.Serials {
				if sr == "" {
					first = -2 // an unknown serial before any later one
					break
				}
				if sr > cl.Attach {
					first = i
					break
				}
			}
		}
		switch {
		case first == -2:
			res.Unverifiable++
		case first < 0:
			// Every logged serial is at or before the attach point: the
			// attachment started at or after the log's end. FirstSeq <= n
			// is consistent with that; beyond it the log cannot say.
			if cl.FirstSeq <= int64(n) {
				res.Checked++
			} else {
				res.Unverifiable++
			}
		default:
			res.Checked++
			if cl.FirstSeq > int64(first) {
				lost := cl.FirstSeq - int64(first)
				res.Missed += lost
				if len(res.Examples) < MaxLoggedViolations {
					res.Examples = append(res.Examples, Violation{
						Kind: AttachGap.String(), Channel: cl.Channel, PubID: cl.PubID,
						Seq: int64(first), Expected: cl.FirstSeq - 1, Count: lost, Serial: cl.Attach,
					})
				}
			}
		}
	}
	return res
}

// String renders a violation for logs.
func (v Violation) String() string {
	return fmt.Sprintf("%s channel=%s pub=%s seq=%d expected=%d count=%d serial=%s last=%s conn=%s",
		v.Kind, v.Channel, v.PubID, v.Seq, v.Expected, v.Count, v.Serial, v.LastSerial, v.Conn)
}
