package postgres

import (
	"slices"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
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
		timing:   chainTiming{gapDelay: time.Hour, gapMaxDelay: time.Hour, fetchTimeout: time.Second},
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
	cs.applyRangeLocked([]*protocol.ChannelMessage{ev("s1", "s0").cm, ev("s2", "s1").cm}, "s2")
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
	cs.applyRangeLocked(nil, "s3")
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
}

func (r *discontinuityRecorder) Discontinuity() { r.discontinuities++ }

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
	cs.applyRangeLocked([]*protocol.ChannelMessage{{ChannelSerial: "s1"}, {ChannelSerial: "s2"}}, "s2")
	cs.hwmMu.Unlock()
	if rec.discontinuities != 0 {
		t.Fatalf("a filled gap signalled %d discontinuities", rec.discontinuities)
	}
	cs.deliverChained(ev("s4", "s3"))
	cs.hwmMu.Lock()
	cs.applyRangeLocked(nil, "s4")
	cs.hwmMu.Unlock()
	if rec.discontinuities != 1 {
		t.Errorf("a skipped gap signalled %d discontinuities, want 1", rec.discontinuities)
	}
	if want := []string{"s1", "s2", "s4"}; !slices.Equal(rec.serials, want) {
		t.Errorf("delivered %v, want %v", rec.serials, want)
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

	data, pointer, err := encodeNATSEnvelope("room", cm, "00000000000000-000@abc", DefaultNATSInlineMaxBytes)
	if err != nil || pointer {
		t.Fatalf("encode: pointer=%v err=%v", pointer, err)
	}
	got, channel, err := decodeNATSEnvelope(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if channel != "room" || got.serial != cm.ChannelSerial || got.prev != "00000000000000-000@abc" || got.cm == nil {
		t.Fatalf("decoded %q %+v", channel, got)
	}
	agg := got.cm.Annotations[0].Summary["reaction:distinct.v1"]
	if agg == nil || agg.Values["x"] == nil || !slices.Equal(agg.Values["x"].ClientIDs, []string{"alice"}) {
		t.Errorf("summary did not survive the envelope: %#v", got.cm.Annotations[0].Summary)
	}

	// Over the threshold: a pointer with no body.
	data, pointer, err = encodeNATSEnvelope("room", cm, "p", 8)
	if err != nil || !pointer {
		t.Fatalf("encode over threshold: pointer=%v err=%v", pointer, err)
	}
	got, _, err = decodeNATSEnvelope(data)
	if err != nil || got.cm != nil || got.serial != cm.ChannelSerial || got.prev != "p" {
		t.Fatalf("pointer decoded as %+v, err=%v", got, err)
	}
}
