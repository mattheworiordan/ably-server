package postgres

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Unit tests for the chained delivery point (DESIGN.md §7.2). Every
// event here carries its body inline, so no database is involved; the
// gap-fill timer is parked an hour out so only the chain itself acts.

type chainRecorder struct{ serials []string }

func (r *chainRecorder) Initialize(current, initial string) {}
func (r *chainRecorder) Append(cm *protocol.ChannelMessage) {
	r.serials = append(r.serials, cm.ChannelSerial)
}

func newTestChain(t *testing.T) (*channelStore, *chainRecorder) {
	t.Helper()
	rec := &chainRecorder{}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	cs := &channelStore{
		name:     "room",
		appender: rec,
		logger:   logging.Default(),
		done:     done,
		timing:   chainTiming{gapDelay: time.Hour, gapMaxDelay: time.Hour, fetchTimeout: time.Second, overflowDelay: time.Hour},
	}
	t.Cleanup(func() {
		cs.hwmMu.Lock()
		cs.stopGapFillLocked()
		cs.hwmMu.Unlock()
	})
	return cs, rec
}

func ev(serial, prev string) busEvent {
	return busEvent{serial: serial, prev: prev, cm: &protocol.ChannelMessage{
		ChannelSerial: serial,
		Messages:      []*protocol.Message{{Data: serial}},
	}}
}

func TestChainDeliversInOrder(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s1", "s0"))
	cs.deliverChained(ev("s2", "s1"))
	if want := []string{"s1", "s2"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
}

func TestChainHoldsAnEarlyArrivalUntilItsPredecessor(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s3", "s2"))
	cs.deliverChained(ev("s2", "s1"))
	if len(rec.serials) != 0 {
		t.Fatalf("delivered %v before s1 arrived, want nothing", rec.serials)
	}
	cs.deliverChained(ev("s1", "s0"))
	if want := []string{"s1", "s2", "s3"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	if cs.held != 2 || len(cs.pending) != 0 || cs.gapTimer != nil {
		t.Errorf("held=%d pending=%d timer=%v, want 2, 0, nil", cs.held, len(cs.pending), cs.gapTimer != nil)
	}
}

func TestChainDropsDuplicates(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s1", "s0")) // fast path
	cs.deliverChained(ev("s1", "s0")) // bus echo
	cs.deliverChained(ev("s2", "s1"))
	cs.deliverChained(ev("s1", "s0")) // a late echo
	if want := []string{"s1", "s2"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	if cs.duplicates != 2 {
		t.Errorf("duplicates = %d, want 2", cs.duplicates)
	}
}

func TestChainHoldsUntilSeededThenDropsHistory(t *testing.T) {
	cs, rec := newTestChain(t)
	// Arrivals while the bind is still reading the watermark.
	cs.deliverChained(ev("s1", "s0"))
	cs.deliverChained(ev("s2", "s1"))
	cs.deliverChained(ev("s3", "s2"))
	if len(rec.serials) != 0 {
		t.Fatalf("delivered %v before seeding, want nothing", rec.serials)
	}
	// The watermark read saw s1 committed: s1 is history, s2 onwards is live.
	cs.seed("s1")
	if want := []string{"s2", "s3"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
}

func TestChainArmsGapFillForAMissingPredecessor(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1")) // s1 never arrives on the bus
	if len(rec.serials) != 0 || cs.gapTimer == nil {
		t.Fatalf("delivered=%v timer armed=%v, want nothing delivered and the gap fill armed", rec.serials, cs.gapTimer != nil)
	}
	// The gap fill's log read returns s1 and s2: both are delivered once
	// and the held copy of s2 is discarded.
	cs.hwmMu.Lock()
	cs.applyRangeLocked(rangeRead{cms: []*protocol.ChannelMessage{ev("s1", "s0").cm, ev("s2", "s1").cm}}, "s2")
	cs.hwmMu.Unlock()
	if want := []string{"s1", "s2"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	if len(cs.pending) != 0 || cs.gapTimer != nil {
		t.Errorf("pending=%d timer=%v after the fill, want 0, nil", len(cs.pending), cs.gapTimer != nil)
	}
}

func TestChainSkipsAGapTheLogNoLongerHolds(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1"))
	cs.deliverChained(ev("s3", "s2"))
	// The log read comes back empty (the rows are gone): the held bus
	// copies are delivered in order rather than wedging the chain.
	cs.hwmMu.Lock()
	cs.applyRangeLocked(rangeRead{}, "s3")
	cs.hwmMu.Unlock()
	if want := []string{"s2", "s3"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	cs.deliverChained(ev("s4", "s3"))
	if want := []string{"s2", "s3", "s4"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
}

// discontinuityRecorder is a chainRecorder that also counts
// Discontinuity calls (storage.Discontinuous).
type discontinuityRecorder struct {
	chainRecorder
	discontinuities int
	reasons         []storage.DiscontinuityReason
}

func (r *discontinuityRecorder) Discontinuity(reason storage.DiscontinuityReason) {
	r.discontinuities++
	r.reasons = append(r.reasons, reason)
	r.serials = append(r.serials, "|") // where in the delivery order it came
}

// TestChainSkippedGapSignalsDiscontinuity: a gap the log no longer holds
// is skipped, and the appender is told, so a node's local presence
// member set (DESIGN.md §12.4) re-seeds rather than miss the skipped
// cms for good. A gap that is filled is not a discontinuity.
func TestChainSkippedGapSignalsDiscontinuity(t *testing.T) {
	cs, _ := newTestChain(t)
	rec := &discontinuityRecorder{}
	cs.appender = rec
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1"))
	cs.hwmMu.Lock()
	cs.applyRangeLocked(rangeRead{cms: []*protocol.ChannelMessage{{ChannelSerial: "s1"}, {ChannelSerial: "s2"}}}, "s2")
	cs.hwmMu.Unlock()
	if rec.discontinuities != 0 {
		t.Fatalf("a filled gap signalled %d discontinuities", rec.discontinuities)
	}
	cs.deliverChained(ev("s4", "s3"))
	cs.hwmMu.Lock()
	cs.applyRangeLocked(rangeRead{}, "s4")
	cs.hwmMu.Unlock()
	if rec.discontinuities != 1 {
		t.Errorf("a skipped gap signalled %d discontinuities, want 1", rec.discontinuities)
	}
	// The discontinuity ("|") comes where s3 was skipped, before s4.
	if want := []string{"s1", "s2", "|", "s4"}; !slices.Equal(rec.serials, want) {
		t.Errorf("delivered %v, want %v", rec.serials, want)
	}
	if want := []storage.DiscontinuityReason{storage.DiscontinuityLogGap}; !slices.Equal(rec.reasons, want) {
		t.Errorf("reasons %v, want %v", rec.reasons, want)
	}
}

func TestNATSEnvelopeRoundTrip(t *testing.T) {
	cm := &protocol.ChannelMessage{
		ChannelSerial: "00000000000001-000@abc",
		Annotations: []*protocol.Annotation{{
			Serial: "00000000000001-000@abc:0", Type: "reaction:distinct.v1", Name: "x", ClientID: "alice",
			MessageSerial: "00000000000000-000@abc:0",
		}},
	}
	cm.Annotations[0].Summary = protocol.Summary(nil).Apply(cm.Annotations[0])

	data, pointer, err := encodeNATSEnvelope("dep", "room", cm, "00000000000000-000@abc", DefaultNATSInlineMaxBytes)
	if err != nil || pointer {
		t.Fatalf("encode: pointer=%v err=%v", pointer, err)
	}
	env, err := decodeNATSEnvelope(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, channel := env.ev, env.channel
	if env.deployment != "dep" || env.mintedMs != 1 {
		t.Fatalf("decoded deployment %q minted %d, want dep and 1", env.deployment, env.mintedMs)
	}
	if channel != "room" || got.serial != cm.ChannelSerial || got.prev != "00000000000000-000@abc" || got.cm == nil {
		t.Fatalf("decoded %q %+v", channel, got)
	}
	agg := got.cm.Annotations[0].Summary["reaction:distinct.v1"]
	if agg == nil || agg.Values["x"] == nil || !slices.Equal(agg.Values["x"].ClientIDs, []string{"alice"}) {
		t.Errorf("summary did not survive the envelope: %#v", got.cm.Annotations[0].Summary)
	}

	// Over the threshold: a pointer with no body.
	data, pointer, err = encodeNATSEnvelope("dep", "room", cm, "00000000000000-001@abc", 8)
	if err != nil || !pointer {
		t.Fatalf("encode over threshold: pointer=%v err=%v", pointer, err)
	}
	env, err = decodeNATSEnvelope(data)
	got = env.ev
	if err != nil || got.cm != nil || got.serial != cm.ChannelSerial || got.prev != "00000000000000-001@abc" {
		t.Fatalf("pointer decoded as %+v, err=%v", got, err)
	}
}

// TestChainRecordsReceiveStages checks the receive-side stage histograms
// (DESIGN.md §10): every append is timed (ably_bus_append_seconds), and a
// cm held for its predecessor records the hold (ably_bus_hold_seconds),
// which ends the moment the predecessor arrives, not on the gap-fill
// timer (parked an hour out here).
func TestChainRecordsReceiveStages(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.stats = &busStats{}
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1"))
	time.Sleep(20 * time.Millisecond)
	cs.deliverChained(ev("s1", "s0"))
	if want := []string{"s1", "s2"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	st := cs.stats.stageSnapshot()
	if n := st["append"].Count; n != 2 {
		t.Errorf("append observations = %d, want 2", n)
	}
	hold := st["hold"]
	if hold.Count != 1 {
		t.Fatalf("hold observations = %d, want 1 (only s2 was held)", hold.Count)
	}
	if hold.Sum < 0.02 || hold.Sum > 5 {
		t.Errorf("hold time = %vs, want about the 20 ms s2 waited", hold.Sum)
	}
}

// TestChainHoldTimeSpansAReplacedOffer: when a held pointer (no body) is
// replaced by a later offer of the same cm that carries one, the hold
// time still runs from the first offer.
func TestChainHoldTimeSpansAReplacedOffer(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.stats = &busStats{}
	cs.seed("s0")
	pointer := ev("s2", "s1")
	pointer.cm = nil
	cs.deliverChained(pointer)
	time.Sleep(30 * time.Millisecond)
	cs.deliverChained(ev("s2", "s1"))
	cs.deliverChained(ev("s1", "s0"))
	if want := []string{"s1", "s2"}; !slices.Equal(rec.serials, want) {
		t.Fatalf("delivered %v, want %v", rec.serials, want)
	}
	if hold := cs.stats.stageSnapshot()["hold"]; hold.Count != 1 || hold.Sum < 0.03 {
		t.Errorf("hold = %d observations, %vs; want 1 of at least the 30 ms since the pointer", hold.Count, hold.Sum)
	}
}

// retentionChain is newTestChain with a retention window, so its
// retention floor (RetainedSince) is a minute before now, and with a
// discontinuityRecorder appender.
func retentionChain(t *testing.T) (*channelStore, *discontinuityRecorder) {
	t.Helper()
	cs, _ := newTestChain(t)
	rec := &discontinuityRecorder{}
	cs.appender = rec
	cs.retention = time.Minute
	cs.clock = new(atomic.Int64)
	return cs, rec
}

// serialAt is a channelSerial minted d from now.
func serialAt(d time.Duration, ctr int) string {
	return fmt.Sprintf("%014d-%03d@test", time.Now().Add(d).UnixMilli(), ctr)
}

// fakeLog installs a readRangeHook standing in for the database: it
// returns cms past the mark, and, for a checked read, reports the mark's
// cm gone from the log and the channel moved on to current (what the
// database finds after the rows between the mark and cms aged out). It
// records whether each read was checked.
func fakeLog(t *testing.T, current string, cms ...*protocol.ChannelMessage) *[]bool {
	t.Helper()
	var checks []bool
	hook := func(_ *channelStore, after, upTo string, check bool) (rangeRead, error) {
		checks = append(checks, check)
		r := rangeRead{after: after, upTo: upTo}
		for _, cm := range cms {
			if cm.ChannelSerial > after && (upTo == "" || cm.ChannelSerial <= upTo) {
				r.cms = append(r.cms, cm)
			}
		}
		if check {
			r.unproven, r.current = true, current
		}
		return r, nil
	}
	readRangeHook.Store(&hook)
	t.Cleanup(func() { readRangeHook.Store(nil) })
	return &checks
}

// TestCatchUpBelowRetentionFloorSignalsDiscontinuity: a catch-up whose
// delivery mark is older than the retention floor cannot prove that no
// cm after the mark aged out of the log (DESIGN.md §7.2), so the
// appender is told before the cms that survived, once, and the mark
// moves to the channel's serial so the expired cms are not looked for
// again. A mark inside the window needs no proof and signals nothing.
func TestCatchUpBelowRetentionFloorSignalsDiscontinuity(t *testing.T) {
	t.Run("below the floor", func(t *testing.T) {
		cs, rec := retentionChain(t)
		mark := serialAt(-10*time.Minute, 0)
		survivor := serialAt(-30*time.Second, 0)
		current := serialAt(-30*time.Second, 1)
		checks := fakeLog(t, current, &protocol.ChannelMessage{ChannelSerial: survivor})
		cs.seed(mark)
		if err := cs.catchUp(context.Background()); err != nil {
			t.Fatal(err)
		}
		if want := []string{"|", survivor}; !slices.Equal(rec.serials, want) {
			t.Errorf("delivered %v, want the discontinuity before the survivor: %v", rec.serials, want)
		}
		if want := []storage.DiscontinuityReason{storage.DiscontinuityRetention}; !slices.Equal(rec.reasons, want) {
			t.Errorf("reasons %v, want %v", rec.reasons, want)
		}
		if !slices.Equal(*checks, []bool{true}) {
			t.Errorf("reads checked %v, want one checked read", *checks)
		}
		if cs.watermark() != current {
			t.Errorf("mark = %s, want the channel's serial %s", cs.watermark(), current)
		}
		// A second catch-up starts inside the window: nothing more.
		if err := cs.catchUp(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rec.discontinuities != 1 {
			t.Errorf("discontinuities = %d after a second catch-up, want 1", rec.discontinuities)
		}
	})
	t.Run("inside the window", func(t *testing.T) {
		cs, rec := retentionChain(t)
		mark := serialAt(-10*time.Second, 0)
		next := serialAt(-5*time.Second, 0)
		checks := fakeLog(t, next, &protocol.ChannelMessage{ChannelSerial: next})
		cs.seed(mark)
		if err := cs.catchUp(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rec.discontinuities != 0 || !slices.Equal(rec.serials, []string{next}) {
			t.Errorf("delivered %v with %d discontinuities, want %s and none", rec.serials, rec.discontinuities, next)
		}
		if !slices.Equal(*checks, []bool{false}) {
			t.Errorf("reads checked %v, want one unchecked read", *checks)
		}
	})
}

// TestGapFillBelowRetentionFloorSignalsDiscontinuity: the gap fill reads
// the log from the same mark, so when the node was off the bus long
// enough for the cms after its mark to age out, the first cm the bus
// brings back is held, and the fill that releases it must say so too.
func TestGapFillBelowRetentionFloorSignalsDiscontinuity(t *testing.T) {
	cs, rec := retentionChain(t)
	mark := serialAt(-10*time.Minute, 0)
	survivor := serialAt(-30*time.Second, 0)
	live := serialAt(-time.Second, 0)
	fakeLog(t, live, &protocol.ChannelMessage{ChannelSerial: survivor}, ev(live, survivor).cm)
	cs.seed(mark)
	cs.deliverChained(ev(live, survivor)) // held: survivor never came on the bus
	cs.fillGap()
	if want := []string{"|", survivor, live}; !slices.Equal(rec.serials, want) {
		t.Errorf("delivered %v, want %v", rec.serials, want)
	}
	if rec.discontinuities != 1 {
		t.Errorf("discontinuities = %d, want 1", rec.discontinuities)
	}
}

// TestGapFillBelowFloorKeepsCmsPastItsBound: the gap fill's read is
// bounded by the highest held cm, so after a discontinuity it moves the
// mark only to that bound, never to the channel's serial: a cm committed
// after the bound may still be on the bus, and must not be dropped as a
// duplicate when it arrives.
func TestGapFillBelowFloorKeepsCmsPastItsBound(t *testing.T) {
	cs, rec := retentionChain(t)
	mark := serialAt(-10*time.Minute, 0)
	survivor := serialAt(-30*time.Second, 0)
	live := serialAt(-time.Second, 0)
	next := serialAt(-time.Second, 1) // committed, its bus message in flight
	fakeLog(t, next, &protocol.ChannelMessage{ChannelSerial: survivor}, ev(live, survivor).cm, ev(next, live).cm)
	cs.seed(mark)
	cs.deliverChained(ev(live, survivor))
	cs.fillGap()
	if cs.watermark() != live {
		t.Fatalf("mark = %s after the fill, want its bound %s", cs.watermark(), live)
	}
	cs.deliverChained(ev(next, live))
	if want := []string{"|", survivor, live, next}; !slices.Equal(rec.serials, want) {
		t.Errorf("delivered %v, want %v", rec.serials, want)
	}
}

// TestUnprovenReadsFromOneMarkSignalOnce: two reads taken from the same
// mark (a gap fill and a catch-up racing) both fail to prove continuity;
// the first signals and moves the mark, the second finds the mark moved
// and signals nothing.
func TestUnprovenReadsFromOneMarkSignalOnce(t *testing.T) {
	cs, rec := retentionChain(t)
	mark := serialAt(-10*time.Minute, 0)
	current := serialAt(-30*time.Second, 0)
	cs.seed(mark)
	read := rangeRead{after: mark, unproven: true, current: current}
	cs.hwmMu.Lock()
	cs.applyRangeLocked(read, "")
	cs.applyRangeLocked(read, "")
	cs.hwmMu.Unlock()
	if rec.discontinuities != 1 {
		t.Errorf("discontinuities = %d, want 1", rec.discontinuities)
	}
	if cs.watermark() != current {
		t.Errorf("mark = %s, want %s", cs.watermark(), current)
	}
}

// TestCatchUpFromAMarkProvenRecentlyIsNotChecked: a mark below the
// retention floor needs no proof when the node knew, inside the window,
// that the channel had nothing past it (a bind's watermark read, or a
// sweep that found it caught up): every cm after the mark was minted
// after that, so it is still held. A quiet channel caught up after a
// short outage is not signalled.
func TestCatchUpFromAMarkProvenRecentlyIsNotChecked(t *testing.T) {
	cs, rec := retentionChain(t)
	mark := serialAt(-10*time.Minute, 0)
	next := serialAt(-time.Second, 0)
	checks := fakeLog(t, next, &protocol.ChannelMessage{ChannelSerial: next})
	cs.seed(mark)
	cs.hwmMu.Lock()
	cs.markProvenLocked(serialAt(-5*time.Second, 0)[:14])
	cs.hwmMu.Unlock()
	if err := cs.catchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.discontinuities != 0 || !slices.Equal(rec.serials, []string{next}) {
		t.Errorf("delivered %v with %d discontinuities, want %s and none", rec.serials, rec.discontinuities, next)
	}
	if !slices.Equal(*checks, []bool{false}) {
		t.Errorf("reads checked %v, want one unchecked read", *checks)
	}
}

// A channel's hold map is capped at maxPendingHold bodies: past it a cm
// keeps its serial and loses its body, and a gap fill is forced, which
// delivers every cm from the log in order (DESIGN.md §7.2).
func TestChainHoldMapIsCappedAndOverflowForcesAGapFill(t *testing.T) {
	cs, rec := newTestChain(t)
	cs.seed("s00000")
	name := func(i int) string { return fmt.Sprintf("s%05d", i) }

	// s00001 never arrives; everything after it is held.
	total := maxPendingHold + 50
	for i := 2; i <= total+1; i++ {
		cs.deliverChained(ev(name(i), name(i-1)))
	}
	if len(rec.serials) != 0 {
		t.Fatalf("delivered %d cms ahead of the missing predecessor", len(rec.serials))
	}

	cs.hwmMu.Lock()
	bodies, serialsOnly := 0, 0
	for _, e := range cs.pending {
		if e.cm != nil {
			bodies++
		} else {
			serialsOnly++
		}
	}
	forced, overflows := cs.overflowArmed, cs.holdOverflows
	cs.hwmMu.Unlock()
	if bodies != maxPendingHold {
		t.Errorf("held bodies = %d, want the cap %d", bodies, maxPendingHold)
	}
	if serialsOnly != 50 || overflows != 50 {
		t.Errorf("serial-only holds = %d, overflows = %d, want 50 each", serialsOnly, overflows)
	}
	if !forced || cs.gapTimer == nil {
		t.Errorf("overflowArmed=%v timer=%v, want a forced gap fill armed", forced, cs.gapTimer != nil)
	}

	// The forced fill reads the range from the log (every serial, bodies
	// included): each cm is delivered exactly once, in order.
	var log []*protocol.ChannelMessage
	for i := 1; i <= total+1; i++ {
		log = append(log, ev(name(i), name(i-1)).cm)
	}
	cs.hwmMu.Lock()
	cs.applyRangeLocked(rangeRead{after: name(0), upTo: name(total + 1), cms: log}, name(total+1))
	left := len(cs.pending)
	cs.hwmMu.Unlock()
	if len(rec.serials) != total+1 {
		t.Fatalf("delivered %d cms, want %d", len(rec.serials), total+1)
	}
	for i, got := range rec.serials {
		if got != name(i+1) {
			t.Fatalf("delivery %d = %s, want %s", i, got, name(i+1))
		}
	}
	if left != 0 {
		t.Errorf("pending after the fill = %d, want 0", left)
	}
}

// Closing the Storage stops every bound channel's gap-fill timer, and a
// closed Storage arms none, so no fill starts against a closing pool.
func TestStorageCloseStopsGapTimers(t *testing.T) {
	cs, _ := newTestChain(t)
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1")) // s1 never arrives: arms the gap fill
	if cs.gapTimer == nil {
		t.Fatal("gap fill not armed")
	}
	s := &Storage{channels: map[string]*channelStore{"room": cs}}
	s.stopGapTimers()
	if cs.gapTimer != nil {
		t.Fatal("gap timer still armed after stopGapTimers")
	}
}

func TestClosedStorageArmsNoGapFill(t *testing.T) {
	rec := &chainRecorder{}
	done := make(chan struct{})
	close(done)
	cs := &channelStore{
		name: "room", appender: rec, logger: logging.Default(), done: done,
		timing: chainTiming{gapDelay: time.Millisecond, gapMaxDelay: time.Millisecond, fetchTimeout: time.Second},
	}
	cs.seed("s0")
	cs.deliverChained(ev("s2", "s1"))
	if cs.gapTimer != nil {
		t.Fatal("a closed Storage armed a gap fill")
	}
}

// Log reads of the delivery point run on the Storage's loop context, so
// closing the Storage cancels one in flight.
func TestFetchContextFollowsTheStorageLoopContext(t *testing.T) {
	loop, cancel := context.WithCancel(context.Background())
	cs := &channelStore{ctx: loop, timing: chainTiming{fetchTimeout: time.Hour}}
	ctx, done := cs.fetchContext()
	defer done()
	if dl, ok := ctx.Deadline(); !ok || time.Until(dl) < 59*time.Minute {
		t.Fatalf("fetch context deadline = %v (ok=%v), want the fetch timeout", dl, ok)
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("fetch context not cancelled when the loop context was")
	}
}
