package postgres

import (
	"context"
	"sort"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// Chained delivery (DESIGN.md §7.3). A bus that does not deliver each
// channel's cms in commit order (NATS orders per publishing connection,
// not across nodes) announces every cm with its predecessor serial, and
// the per-channel delivery point orders on that link: a cm whose
// predecessor is the last delivered serial is appended at once; a cm
// that arrives ahead of its predecessor is held until the predecessor
// arrives, or, if it has not arrived within natsGapFillDelay, until the
// missing range has been read back from the log. The high-water mark
// (lastSeen) is the same one the LISTEN path uses, so a cm offered twice
// (the publisher fast path and the bus echo, or a reconcile and a late
// bus delivery) reaches the appender exactly once.

// Gap-fill tuning. natsGapFillDelay is how long a held cm waits for its
// predecessor before the gap is read from the log; natsGapFillMaxDelay
// caps the retry backoff when that read fails; natsFetchTimeout bounds
// each log read. Package vars so integration tests can shrink them;
// Open snapshots them into chainTiming so timer goroutines never read
// the vars themselves.
var (
	natsGapFillDelay    = 100 * time.Millisecond
	natsGapFillMaxDelay = 5 * time.Second
	natsFetchTimeout    = 10 * time.Second
)

// chainTiming is the snapshot of the gap-fill tuning a Storage (and each
// of its channelStores) runs with.
type chainTiming struct {
	gapDelay, gapMaxDelay, fetchTimeout time.Duration
}

func currentChainTiming() chainTiming {
	return chainTiming{gapDelay: natsGapFillDelay, gapMaxDelay: natsGapFillMaxDelay, fetchTimeout: natsFetchTimeout}
}

// busEvent is one committed cm as a chaining bus announces it.
type busEvent struct {
	serial string                   // the cm's channelSerial
	prev   string                   // the channel's serial before this cm ("" if unknown)
	cm     *protocol.ChannelMessage // nil for a pointer: the body is fetched by serial
}

// deliverChained offers ev to the delivery point. Safe for concurrent
// use: the channel's bus subscription, the publisher fast path, gap fills
// and reconciles all call it, serialised by hwmMu, which is held across
// the appender call so appends stay in serial order.
func (cs *channelStore) deliverChained(ev busEvent) {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if !cs.seeded {
		// Bound but not yet initialised: the watermark is not known, so
		// hold everything; seed sorts it out.
		cs.holdLocked(ev)
		return
	}
	cs.offerLocked(ev)
}

// seed sets the delivery point's starting mark to the channel's
// watermark (read by Storage.Channel after the bus subscription is in
// place) and releases anything held while the bind was in flight: cms
// at or below the watermark are history, the rest chain on from it.
func (cs *channelStore) seed(current string) {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	cs.lastSeen = current
	cs.seeded = true
	cs.settleLocked()
}

// holdLocked parks ev until its predecessor has been delivered. Held
// cms are keyed by predecessor; two offers of the same cm keep the one
// that carries a body.
func (cs *channelStore) holdLocked(ev busEvent) {
	if cs.pending == nil {
		cs.pending = make(map[string]busEvent)
	}
	if old, ok := cs.pending[ev.prev]; ok && old.cm != nil && ev.cm == nil {
		return
	}
	cs.pending[ev.prev] = ev
}

func (cs *channelStore) offerLocked(ev busEvent) {
	switch {
	case ev.serial <= cs.lastSeen:
		cs.duplicates++
		return
	case ev.prev > cs.lastSeen:
		// The predecessor has not been delivered yet: hold, and read the
		// gap from the log if it does not turn up.
		cs.held++
		cs.holdLocked(ev)
		cs.armGapFillLocked(cs.timing.gapDelay)
		return
	}
	// ev.prev == lastSeen: next in line. (A prev below lastSeen with a
	// serial above it cannot happen on a linear chain; deliver it rather
	// than wedge.)
	if !cs.appendEventLocked(ev) {
		cs.holdLocked(ev)
		cs.armGapFillLocked(cs.timing.gapDelay)
		return
	}
	cs.settleLocked()
}

// settleLocked appends held cms that now chain on lastSeen, drops any
// the mark has overtaken, and arms or stops the gap-fill timer.
func (cs *channelStore) settleLocked() {
	for {
		ev, ok := cs.pending[cs.lastSeen]
		if !ok {
			break
		}
		delete(cs.pending, cs.lastSeen)
		if !cs.appendEventLocked(ev) {
			cs.pending[ev.prev] = ev
			break
		}
	}
	for prev, ev := range cs.pending {
		if ev.serial <= cs.lastSeen {
			delete(cs.pending, prev)
		}
	}
	if len(cs.pending) == 0 {
		cs.stopGapFillLocked()
		return
	}
	cs.armGapFillLocked(cs.timing.gapDelay)
}

// appendEventLocked hands ev's cm to the appender and advances the
// mark. A pointer is resolved through the fetch-by-serial path the
// LISTEN broker uses; if that read fails the cm stays undelivered and
// false is returned.
func (cs *channelStore) appendEventLocked(ev busEvent) bool {
	cm := ev.cm
	if cm == nil {
		ctx, cancel := context.WithTimeout(context.Background(), cs.timing.fetchTimeout)
		var err error
		cm, err = loadChannelMessagePool(ctx, cs.pool, cs.name, ev.serial)
		cancel()
		if err != nil {
			cs.logger.Warn("storage/postgres: fetch of bus pointer failed; will retry from the log", "channel", cs.name, "serial", ev.serial, "err", err)
			return false
		}
	}
	cs.lastSeen = ev.serial
	cs.delivered++
	cs.appender.Append(cm)
	return true
}

func (cs *channelStore) armGapFillLocked(d time.Duration) {
	if cs.gapTimer != nil {
		return
	}
	cs.gapTimer = time.AfterFunc(d, cs.fillGap)
}

func (cs *channelStore) stopGapFillLocked() {
	if cs.gapTimer != nil {
		cs.gapTimer.Stop()
		cs.gapTimer = nil
	}
}

// fillGap runs when a held cm's predecessor has not arrived in time. It
// reads every cm in (lastSeen, highest held serial] from the log, which
// is authoritative because every held cm has committed, and delivers the
// range through the delivery point. The read happens outside hwmMu so
// the bus subscription keeps flowing; anything it delivers meanwhile is
// deduplicated by the mark.
func (cs *channelStore) fillGap() {
	cs.hwmMu.Lock()
	cs.gapTimer = nil
	if cs.closed() || !cs.seeded || len(cs.pending) == 0 {
		cs.hwmMu.Unlock()
		return
	}
	after, upTo := cs.lastSeen, ""
	for _, ev := range cs.pending {
		if ev.serial > upTo {
			upTo = ev.serial
		}
	}
	cs.hwmMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), cs.timing.fetchTimeout)
	cms, err := loadChannelMessagesAfter(ctx, cs.pool, cs.name, after, upTo)
	cancel()

	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if err != nil {
		if cs.closed() {
			return
		}
		cs.gapBackoff = min(max(2*cs.gapBackoff, cs.timing.gapDelay), cs.timing.gapMaxDelay)
		cs.logger.Warn("storage/postgres: bus gap fill failed; retrying", "channel", cs.name, "after", after, "err", err, "delay", cs.gapBackoff)
		cs.armGapFillLocked(cs.gapBackoff)
		return
	}
	cs.gapBackoff = 0
	cs.gapFills++
	cs.applyRangeLocked(cms, upTo)
}

// catchUp replays every cm past the delivery point's mark from the log
// (all kinds, in serial order) through the delivery point. It is the
// chained bus's reconcile: run for every bound channel after the bus
// reconnects, and by the watermark sweep for a channel that has fallen
// behind (DESIGN.md §7.3). A channel that is still binding is skipped;
// its seed reads a watermark newer than anything this would replay.
func (cs *channelStore) catchUp(ctx context.Context) error {
	cs.hwmMu.Lock()
	if !cs.seeded {
		cs.hwmMu.Unlock()
		return nil
	}
	after := cs.lastSeen
	cs.hwmMu.Unlock()

	cms, err := loadChannelMessagesAfter(ctx, cs.pool, cs.name, after, "")
	if err != nil {
		return err
	}

	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	cs.applyRangeLocked(cms, "")
	return nil
}

// applyRangeLocked delivers a serial-ascending range read from the log.
// When upTo is set the log is authoritative up to it: a held cm at or
// below upTo that the read did not return (possible only if its rows
// were removed) is delivered from the bus copy, and the mark moves to
// upTo, so the chain can never wedge on a cm the log no longer has.
func (cs *channelStore) applyRangeLocked(cms []*protocol.ChannelMessage, upTo string) {
	for _, cm := range cms {
		if cm.ChannelSerial <= cs.lastSeen {
			continue
		}
		cs.lastSeen = cm.ChannelSerial
		cs.delivered++
		cs.appender.Append(cm)
	}
	if upTo != "" && upTo > cs.lastSeen {
		var stale []busEvent
		for _, ev := range cs.pending {
			if ev.serial > cs.lastSeen && ev.serial <= upTo {
				stale = append(stale, ev)
			}
		}
		sort.Slice(stale, func(i, j int) bool { return stale[i].serial < stale[j].serial })
		for _, ev := range stale {
			delete(cs.pending, ev.prev)
			if ev.cm != nil && ev.serial > cs.lastSeen {
				cs.lastSeen = ev.serial
				cs.delivered++
				cs.appender.Append(ev.cm)
			}
		}
		cs.logger.Warn("storage/postgres: bus gap not found in the log; skipped past it", "channel", cs.name, "upTo", upTo)
		cs.lastSeen = upTo
	}
	cs.settleLocked()
}

// closed reports whether the owning Storage has been closed.
func (cs *channelStore) closed() bool {
	select {
	case <-cs.done:
		return true
	default:
		return false
	}
}
