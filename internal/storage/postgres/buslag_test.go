package postgres

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// lagBucketCount returns the cumulative count at bound le, or the total
// for a bound not in BusLagBuckets (+Inf).
func lagBucketCount(h storage.LagHistogram, le float64) uint64 {
	for i, b := range storage.BusLagBuckets {
		if b == le {
			return h.Counts[i]
		}
	}
	return h.Count
}

// TestLagHistogramBuckets: observations land in the first bucket whose
// bound is at or above them, counts are cumulative, a negative lag (clock
// skew) counts as zero, and the sum is in seconds.
func TestLagHistogramBuckets(t *testing.T) {
	var h lagHistogram
	h.observe(500 * time.Microsecond)
	h.observe(time.Millisecond) // on the bound: le="0.001"
	h.observe(3 * time.Millisecond)
	h.observe(-time.Second)
	h.observe(time.Minute)
	s := h.snapshot()
	if got := lagBucketCount(s, 0.001); got != 3 {
		t.Errorf("le=0.001 count = %d, want 3 (0.5 ms, 1 ms, negative)", got)
	}
	if got := lagBucketCount(s, 0.005); got != 4 {
		t.Errorf("le=0.005 count = %d, want 4", got)
	}
	if got := lagBucketCount(s, 30); got != 4 {
		t.Errorf("le=30 count = %d, want 4 (a minute is only in +Inf)", got)
	}
	if s.Count != 5 {
		t.Errorf("count = %d, want 5", s.Count)
	}
	if want := 0.0005 + 0.001 + 0.003 + 60; s.Sum < want-1e-9 || s.Sum > want+1e-9 {
		t.Errorf("sum = %v, want %v", s.Sum, want)
	}
}

// TestCMTimestampPrefersTheVersionTime: a mutation's lag runs from its
// own version time, not the original create's timestamp it carries.
func TestCMTimestampPrefersTheVersionTime(t *testing.T) {
	for _, tc := range []struct {
		name string
		cm   *protocol.ChannelMessage
		want int64
	}{
		{"nil", nil, 0},
		{"empty", &protocol.ChannelMessage{}, 0},
		{"create", &protocol.ChannelMessage{Messages: []*protocol.Message{{Timestamp: 100, Version: &protocol.MessageVersion{Timestamp: 100}}}}, 100},
		{"mutation", &protocol.ChannelMessage{Messages: []*protocol.Message{{Timestamp: 100, Version: &protocol.MessageVersion{Timestamp: 250}}}}, 250},
		{"no version", &protocol.ChannelMessage{Messages: []*protocol.Message{{Timestamp: 100}}}, 100},
		{"presence", &protocol.ChannelMessage{Presence: []*protocol.PresenceMessage{{Timestamp: 300}}}, 300},
		{"annotation", &protocol.ChannelMessage{Annotations: []*protocol.Annotation{{Timestamp: 400}}}, 400},
	} {
		if got := cmTimestamp(tc.cm); got != tc.want {
			t.Errorf("%s: cmTimestamp = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestChainRecordsDeliveryLagByPath: a cm delivered from a bus message
// records its lag on the inline path, measured from the envelope's send
// time; a cm read from the log records on the filled path from its
// stored timestamp; the publisher fast path records nothing.
func TestChainRecordsDeliveryLagByPath(t *testing.T) {
	cs, _ := newTestChain(t)
	st := &busStats{}
	cs.stats = st
	cs.seed("s0")

	sent := time.Now().Add(-40 * time.Millisecond).UnixNano()
	e := ev("s1", "s0")
	e.sentAt = sent
	cs.deliverChained(e)

	fp := ev("s2", "s1")
	fp.src = srcFastPath
	cs.deliverChained(fp)

	old := time.Now().Add(-3 * time.Second).UnixMilli()
	cs.hwmMu.Lock()
	cs.applyRangeLocked([]*protocol.ChannelMessage{{ChannelSerial: "s3", Messages: []*protocol.Message{{Data: "x", Timestamp: old}}}}, "")
	cs.hwmMu.Unlock()

	snap := st.lagSnapshot()
	inline, filled, fetched := snap["inline"], snap["filled"], snap["fetched"]
	// 40 ms lands in le=0.05, later buckets on a slow runner; never below 25 ms.
	if inline.Count != 1 || lagBucketCount(inline, 0.025) != 0 || lagBucketCount(inline, 1) != 1 {
		t.Errorf("inline lag %+v, want one observation of about 40 ms", inline)
	}
	if filled.Count != 1 || lagBucketCount(filled, 2.5) != 0 || lagBucketCount(filled, 5) != 1 {
		t.Errorf("filled lag %+v, want one observation of about 3 s", filled)
	}
	if fetched.Count != 0 {
		t.Errorf("fetched lag count = %d, want 0", fetched.Count)
	}
	if total := inline.Count + filled.Count + fetched.Count; total != 2 {
		t.Errorf("%d lag observations, want 2 (the fast path is not a bus delivery)", total)
	}
}

// TestNATSEnvelopeCarriesSendTime: the envelope's send time survives
// encoding, inline and as a pointer, and reaches the bus event.
func TestNATSEnvelopeCarriesSendTime(t *testing.T) {
	cm := &protocol.ChannelMessage{ChannelSerial: "s1", Messages: []*protocol.Message{{Data: "hi"}}}
	before := time.Now().UnixNano()
	for _, inlineMax := range []int{DefaultNATSInlineMaxBytes, 1} {
		data, pointer, err := encodeNATSEnvelope("room", cm, "s0", inlineMax)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		ev, ch, err := decodeNATSEnvelope(data)
		if err != nil || ch != "room" {
			t.Fatalf("decode: channel %q err %v", ch, err)
		}
		if ev.sentAt < before || ev.sentAt > time.Now().UnixNano() {
			t.Errorf("pointer=%v: sentAt %d not between %d and now", pointer, ev.sentAt, before)
		}
	}
}
