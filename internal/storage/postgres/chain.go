package postgres

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Chained delivery (DESIGN.md §7.2). The two buses that deliver a
// committed cm outside the publish transaction, postgres (per-channel
// LISTEN) and nats, share this delivery point. Every cm is announced
// with its predecessor serial, read under the channels-row lock in the
// publish transaction (channelStore.advanceSerial), and the per-channel
// delivery point orders on that link: a cm whose predecessor is the last
// delivered serial is appended at once; a cm that arrives ahead of its
// predecessor is held until the predecessor arrives, or, if it has not
// arrived within gapFillDelay, until the missing range has been read
// back from the log. The high-water mark (lastSeen) is the same one the
// pgnotify path uses, so a cm offered twice (the publisher fast path and
// the bus echo, or a reconcile and a late bus delivery) reaches the
// appender exactly once.
//
// Three paths recover a cm whose bus message never arrived: the next cm
// on the channel reveals the gap through its predecessor; a bus
// reconnect catches every bound channel up from the log
// (reconcileBound); and the watermark sweep (sweepWatermarks) catches a
// lost tail with no later publish. A missed bus message makes a cm
// late, never lost: it is committed before it is announced.

// Gap-fill tuning. gapFillDelay is how long a held cm waits for its
// predecessor before the gap is read from the log; gapFillMaxDelay caps
// the retry backoff when that read fails; busFetchTimeout bounds each
// log read. Package vars so integration tests can shrink them; Open
// snapshots them into chainTiming so timer goroutines never read the
// vars themselves.
var (
	gapFillDelay    = 100 * time.Millisecond
	gapFillMaxDelay = 5 * time.Second
	busFetchTimeout = 10 * time.Second
)

// rangePageSize caps the cms one log range read returns (gap fill,
// catch-up, reconcile); a read that fills the page loops until it has
// caught up. A package var so tests can force paging.
var rangePageSize = 500

// reconcileChunk caps the channels one batched reconcile query covers.
const reconcileChunk = 500

// sweepChunk caps the channel names in one watermark sweep query.
const sweepChunk = 1000

// Watermark sweep scopes accepted by Options.SweepScope (DESIGN.md
// §7.2).
const (
	// SweepSubscribed sweeps only the bound channels with a subscriber on
	// this node: an attachment or a tracked presence member
	// (storage.SubscriberReporter). The default.
	SweepSubscribed = "subscribed"
	// SweepBound sweeps every bound channel, the behaviour before sweep
	// scopes existed.
	SweepBound = "bound"
)

// ParseSweepScope validates a sweep scope. The empty string is the
// default, SweepSubscribed.
func ParseSweepScope(s string) (string, error) {
	switch s {
	case "", SweepSubscribed:
		return SweepSubscribed, nil
	case SweepBound:
		return SweepBound, nil
	}
	return "", fmt.Errorf("unknown bus sweep scope %q (valid: %s, %s)", s, SweepSubscribed, SweepBound)
}

// sweepNamesHook, when set, receives the channel names of each watermark
// sweep query (tests count what the sweep reads). Nil in production.
var sweepNamesHook atomic.Pointer[func(names []string)]

// chainTiming is the snapshot of the gap-fill tuning a Storage (and each
// of its channelStores) runs with.
type chainTiming struct {
	gapDelay, gapMaxDelay, fetchTimeout time.Duration
}

func currentChainTiming() chainTiming {
	return chainTiming{gapDelay: gapFillDelay, gapMaxDelay: gapFillMaxDelay, fetchTimeout: busFetchTimeout}
}

// eventSource records which path offered a cm to the delivery point, so
// the delivery counters say how each cm arrived.
type eventSource uint8

const (
	srcBus      eventSource = iota // a bus message (inline body, or a pointer when cm is nil)
	srcFastPath                    // the publishing node's own commit
)

// busEvent is one committed cm as a chaining bus announces it.
type busEvent struct {
	serial  string                   // the cm's channelSerial
	prev    string                   // the channel's serial before this cm ("" if unknown)
	cm      *protocol.ChannelMessage // nil for a pointer whose body is not yet read
	src     eventSource
	fetched bool      // cm was read from the log by serial (a pointer), not carried by the bus
	sentAt  int64     // when the publisher sent the bus message (Unix ns; 0 if the bus does not say)
	heldAt  time.Time // when the delivery point first held it (zero if never held)
}

// resolvePointer reads a pointer event's body from the log, outside
// hwmMu, so a slow read never blocks the channel's other deliveries (the
// publisher fast path among them). An event the delivery point would
// drop as a duplicate is not read. If the read fails the event keeps a
// nil body: the delivery point holds it and the gap fill reads it from
// the log.
func (cs *channelStore) resolvePointer(ev busEvent) busEvent {
	if ev.cm != nil || ev.serial <= cs.watermark() {
		return ev
	}
	ctx, cancel := context.WithTimeout(context.Background(), cs.timing.fetchTimeout)
	defer cancel()
	cm, err := loadChannelMessagePool(ctx, cs.pool, cs.name, ev.serial)
	if err != nil {
		cs.st().fetchErrors.Add(1)
		cs.logger.Warn("storage/postgres: fetch of bus pointer failed; the gap fill will read it", "channel", cs.name, "serial", ev.serial, "err", err)
		return ev
	}
	ev.cm, ev.fetched = cm, true
	return ev
}

// deliverChained offers ev to the delivery point. Safe for concurrent
// use: the channel's bus subscription or worker, the publisher fast
// path, gap fills and reconciles all call it, serialised by hwmMu, which
// is held across the appender call so appends stay in serial order.
func (cs *channelStore) deliverChained(ev busEvent) {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if cs.released {
		return
	}
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
// at or below the watermark are history, the rest chain on from it. The
// mark only moves forward, so a delivery that raced the bind is never
// replayed.
func (cs *channelStore) seed(current string) {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	if current > cs.lastSeen {
		cs.lastSeen = current
	}
	cs.seeded = true
	cs.closeReadyLocked()
	if cs.released {
		return
	}
	cs.settleLocked()
}

// seedChain finishes a chaining bus's bind: Initialize the appender and
// seed the delivery point, under hwmMu so no append can come first, and
// not at all for a store released while it was binding.
func (cs *channelStore) seedChain(current, initial string) {
	cs.hwmMu.Lock()
	if cs.released {
		cs.closeReadyLocked()
		cs.hwmMu.Unlock()
		return
	}
	cs.appender.Initialize(current, initial)
	cs.hwmMu.Unlock()
	cs.seed(current)
}

// closeReadyLocked closes the ready channel (once), releasing a pgBus
// worker waiting for the bind to finish.
func (cs *channelStore) closeReadyLocked() {
	if cs.ready != nil && !cs.readyClosed {
		cs.readyClosed = true
		close(cs.ready)
	}
}

// release stops all further deliveries to the channel's appender
// (Storage.Release, or a bind that failed). Held cms are discarded; the
// log still has them.
func (cs *channelStore) release() {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	cs.released = true
	cs.pending = nil
	cs.stopGapFillLocked()
	cs.closeReadyLocked()
}

// isReleased reports whether release has cut the channel off from its
// appender.
func (cs *channelStore) isReleased() bool {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return cs.released
}

// watermark returns the channel's high-water mark: the highest serial
// delivered to the appender (seeded with the bind-time watermark).
func (cs *channelStore) watermark() string {
	cs.hwmMu.Lock()
	defer cs.hwmMu.Unlock()
	return cs.lastSeen
}

// holdLocked parks ev until its predecessor has been delivered. Held
// cms are keyed by predecessor; two offers of the same cm keep the one
// that carries a body.
func (cs *channelStore) holdLocked(ev busEvent) {
	if ev.heldAt.IsZero() {
		ev.heldAt = time.Now()
	}
	if cs.pending == nil {
		cs.pending = make(map[string]busEvent)
	}
	if old, ok := cs.pending[ev.prev]; ok {
		if old.cm != nil && ev.cm == nil {
			return
		}
		// The hold started with the first offer of this cm.
		if !old.heldAt.IsZero() && old.heldAt.Before(ev.heldAt) {
			ev.heldAt = old.heldAt
		}
	}
	cs.pending[ev.prev] = ev
}

func (cs *channelStore) offerLocked(ev busEvent) {
	switch {
	case ev.serial <= cs.lastSeen:
		cs.duplicates++
		cs.st().duplicates.Add(1)
		return
	case ev.prev > cs.lastSeen:
		// The predecessor has not been delivered yet: hold, and read the
		// gap from the log if it does not turn up.
		cs.held++
		cs.st().held.Add(1)
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
// mark. It does no I/O under the lock: an event without a body (a
// pointer whose read failed, see resolvePointer) is not appended and
// false is returned, so the caller holds it for the gap fill.
func (cs *channelStore) appendEventLocked(ev busEvent) bool {
	if ev.cm == nil {
		return false
	}
	cs.lastSeen = ev.serial
	cs.delivered++
	switch {
	case ev.src == srcFastPath:
		cs.st().fastPath.Add(1)
	case ev.fetched:
		cs.st().fetched.Add(1)
	default:
		cs.st().inline.Add(1)
	}
	if !ev.heldAt.IsZero() {
		cs.st().observeStage(stageHold, time.Since(ev.heldAt))
	}
	cs.appendTimed(ev.cm)
	switch {
	case ev.src == srcFastPath:
	case ev.fetched:
		cs.st().observeLag(lagFetched, ev.sentAt, ev.cm)
	default:
		cs.st().observeLag(lagInline, ev.sentAt, ev.cm)
	}
	return true
}

// appendTimed hands cm to the appender, recording the time the Append
// took (ably_bus_append_seconds): Append wakes every attachment parked on
// the channel, so on a channel with many subscribers it is where a bus
// worker spends its time.
func (cs *channelStore) appendTimed(cm *protocol.ChannelMessage) {
	start := time.Now()
	cs.appender.Append(cm)
	cs.st().observeStage(stageAppend, time.Since(start))
}

// armGapFillLocked arms the gap-fill timer, unless it is armed already
// or a fill is in flight (the fill re-arms when it finishes if a gap is
// left), so fills never overlap.
func (cs *channelStore) armGapFillLocked(d time.Duration) {
	if cs.gapTimer != nil || cs.filling {
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
// reads every cm in (lastSeen, highest held serial] from the log, a page
// at a time, which is authoritative because every held cm has
// committed, and delivers the range through the delivery point. The
// reads happen outside hwmMu so the bus keeps flowing; anything it
// delivers meanwhile is deduplicated by the mark. filling keeps a second
// fill from starting while this one pages.
func (cs *channelStore) fillGap() {
	cs.hwmMu.Lock()
	cs.gapTimer = nil
	if cs.closed() || cs.released || !cs.seeded || len(cs.pending) == 0 || cs.filling {
		cs.hwmMu.Unlock()
		return
	}
	after, upTo := cs.lastSeen, ""
	for _, ev := range cs.pending {
		if ev.serial > upTo {
			upTo = ev.serial
		}
	}
	cs.filling = true
	cs.hwmMu.Unlock()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), cs.timing.fetchTimeout)
		cms, err := loadChannelMessagesAfter(ctx, cs.pool, cs.name, after, upTo, rangePageSize)
		cancel()

		cs.hwmMu.Lock()
		if err == nil && !cs.released && len(cms) == rangePageSize {
			// A full page: deliver it and read on from its end. The log is
			// authoritative up to upTo only once the last page is in.
			cs.applyRangeLocked(cms, "")
			after = cms[len(cms)-1].ChannelSerial
			cs.hwmMu.Unlock()
			continue
		}
		cs.filling = false
		switch {
		case err != nil:
			cs.st().fetchErrors.Add(1)
			if !cs.closed() && !cs.released {
				cs.gapBackoff = min(max(2*cs.gapBackoff, cs.timing.gapDelay), cs.timing.gapMaxDelay)
				cs.logger.Warn("storage/postgres: bus gap fill failed; retrying", "channel", cs.name, "after", after, "err", err, "delay", cs.gapBackoff)
				cs.armGapFillLocked(cs.gapBackoff)
			}
		case cs.released:
		default:
			cs.gapBackoff = 0
			cs.gapFills++
			cs.st().gapFills.Add(1)
			cs.applyRangeLocked(cms, upTo) // settles, re-arming if a gap is left
		}
		cs.hwmMu.Unlock()
		return
	}
}

// catchUp replays every cm past the delivery point's mark from the log
// (all kinds, in serial order, a page at a time) through the delivery
// point. A channel that is still binding is skipped; its seed reads a
// watermark newer than anything this would replay.
func (cs *channelStore) catchUp(ctx context.Context) error {
	for {
		cs.hwmMu.Lock()
		if !cs.seeded || cs.released {
			cs.hwmMu.Unlock()
			return nil
		}
		after := cs.lastSeen
		cs.hwmMu.Unlock()

		cms, err := loadChannelMessagesAfter(ctx, cs.pool, cs.name, after, "", rangePageSize)
		if err != nil {
			cs.st().fetchErrors.Add(1)
			return err
		}

		cs.hwmMu.Lock()
		if !cs.released {
			cs.applyRangeLocked(cms, "")
		}
		cs.hwmMu.Unlock()
		if len(cms) < rangePageSize {
			return nil
		}
	}
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
		cs.st().filled.Add(1)
		cs.appendTimed(cm)
		cs.st().observeLag(lagFilled, 0, cm)
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
				cs.st().filled.Add(1)
				cs.appendTimed(ev.cm)
				cs.st().observeLag(lagFilled, ev.sentAt, ev.cm)
			}
		}
		cs.logger.Warn("storage/postgres: bus gap not found in the log; skipped past it", "channel", cs.name, "upTo", upTo)
		cs.lastSeen = upTo
		if d, ok := cs.appender.(storage.Discontinuous); ok {
			d.Discontinuity()
		}
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

// fastPath is the publisher fast path shared by the chaining buses
// (DESIGN.md §7.2): straight after its own transaction commits, the
// publishing node offers the cm to its own delivery point for the
// channel, without waiting for the bus; the bus echo is then a duplicate
// the mark drops. It targets the channel's bound store, not the
// publishing store, so a publish through a transient store (the presence
// reaper's) still reaches local subscribers. If another node's earlier
// cm has not reached this node yet, the delivery point holds this one
// behind it.
func (s *Storage) fastPath(channel string, cm *protocol.ChannelMessage, prev string) {
	if bound := s.boundStore(channel); bound != nil {
		bound.deliverChained(busEvent{serial: cm.ChannelSerial, prev: prev, cm: cm, src: srcFastPath})
	}
}

// requestReconcile queues a reconcile of every bound channel on the
// chain loop; requests coalesce.
func (s *Storage) requestReconcile() {
	select {
	case s.reconcileCh <- struct{}{}:
	default:
	}
}

// startChainLoop launches chainLoop on the Storage WaitGroup.
func (s *Storage) startChainLoop(ctx context.Context, retryWait time.Duration, beforeReconcile func(context.Context) error) {
	s.wg.Add(1)
	go s.chainLoop(ctx, s.sweepInterval, retryWait, beforeReconcile)
}

// chainLoop runs the reconnect reconcile and the periodic watermark
// sweep for a chaining bus, off the bus's own goroutines. beforeReconcile
// (may be nil) runs first on each reconcile request and must confirm the
// bus has re-registered every subscription; if it fails the reconcile is
// retried after retryWait.
func (s *Storage) chainLoop(ctx context.Context, sweepInterval, retryWait time.Duration, beforeReconcile func(context.Context) error) {
	defer s.wg.Done()
	sweep := time.NewTicker(sweepInterval)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweep.C:
			start := time.Now()
			if err := s.sweepWatermarks(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("storage/postgres: bus watermark sweep failed", "err", err)
			}
			s.stats.sweeps.Add(1)
			s.stats.sweepNanos.Add(uint64(time.Since(start)))
			continue
		case <-s.reconcileCh:
		}

		if beforeReconcile != nil {
			if err := beforeReconcile(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Warn("storage/postgres: bus not ready to reconcile; retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryWait):
				}
				s.requestReconcile()
				continue
			}
		}
		start := time.Now()
		n, err := s.reconcileBound(ctx)
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("storage/postgres: bus reconcile failed", "err", err)
		}
		s.stats.reconcileRuns.Add(1)
		s.stats.reconcileNanos.Add(uint64(time.Since(start)))
		s.logger.Info("storage/postgres: bus reconciled", "channels", n)
	}
}

// reconcileBound catches every bound channel up from the log after a bus
// reconnect (DESIGN.md §7.2), in batches of reconcileChunk channels per
// query. It returns the number of channels reconciled.
func (s *Storage) reconcileBound(ctx context.Context) (int, error) {
	stores := s.boundStores()
	var firstErr error
	for start := 0; start < len(stores); start += reconcileChunk {
		end := min(start+reconcileChunk, len(stores))
		if err := s.catchUpMany(ctx, stores[start:end]); err != nil && firstErr == nil {
			firstErr = err
		}
		s.stats.reconciles.Add(uint64(end - start))
	}
	return len(stores), firstErr
}

// sqlLoadRangeMany reads, for each (channel, after) pair, up to $3 cms
// past after, every kind, ascending: one round trip for many channels.
const sqlLoadRangeMany = `
SELECT t.name, m.channel_serial, m.idx, m.kind, m.payload, m.summary
FROM unnest($1::text[], $2::text[]) AS t(name, after)
CROSS JOIN LATERAL (
	SELECT cm.channel_serial, cm.idx, cm.kind, cm.payload, cm.summary
	FROM channel_messages cm
	WHERE cm.channel = t.name AND cm.channel_serial IN (
		SELECT DISTINCT channel_serial FROM channel_messages
		WHERE channel = t.name AND channel_serial > t.after
		ORDER BY channel_serial
		LIMIT $3)
) m
ORDER BY t.name, m.channel_serial, m.idx
`

// catchUpMany is catchUp for many channels in one query: each channel's
// first page past its mark is read together, and a channel whose page
// came back full is then caught up on its own.
func (s *Storage) catchUpMany(ctx context.Context, stores []*channelStore) error {
	byName := make(map[string]*channelStore, len(stores))
	names := make([]string, 0, len(stores))
	afters := make([]string, 0, len(stores))
	for _, cs := range stores {
		cs.hwmMu.Lock()
		ok := cs.seeded && !cs.released
		after := cs.lastSeen
		cs.hwmMu.Unlock()
		if !ok {
			continue
		}
		byName[cs.name] = cs
		names = append(names, cs.name)
		afters = append(afters, after)
	}
	if len(names) == 0 {
		return nil
	}

	rows, err := s.pool.Query(ctx, sqlLoadRangeMany, names, afters, rangePageSize)
	if err != nil {
		s.stats.fetchErrors.Add(1)
		return fmt.Errorf("storage/postgres: batched range read: %w", err)
	}
	ranges := make(map[string][]*protocol.ChannelMessage, len(names))
	for rows.Next() {
		var (
			name, channelSerial, kind string
			idx                       int
			payload, summary          []byte
		)
		if err := rows.Scan(&name, &channelSerial, &idx, &kind, &payload, &summary); err != nil {
			rows.Close()
			return fmt.Errorf("storage/postgres: scan batched range: %w", err)
		}
		out := ranges[name]
		if n := len(out); n == 0 || out[n-1].ChannelSerial != channelSerial {
			out = append(out, &protocol.ChannelMessage{ChannelSerial: channelSerial})
		}
		if err := decodeRowInto(out[len(out)-1], name, idx, kind, payload, summary); err != nil {
			rows.Close()
			return err
		}
		ranges[name] = out
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		s.stats.fetchErrors.Add(1)
		return fmt.Errorf("storage/postgres: batched range rows: %w", err)
	}

	var firstErr error
	for name, cms := range ranges {
		cs := byName[name]
		cs.hwmMu.Lock()
		if !cs.released {
			cs.applyRangeLocked(cms, "")
		}
		cs.hwmMu.Unlock()
		if len(cms) == rangePageSize {
			if err := cs.catchUp(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// sweepStores returns the bound channels the watermark sweep reads: every
// one under SweepBound; under SweepSubscribed only those whose appender
// has a subscriber on this node (an appender that cannot say counts as
// subscribed). A channel with no local subscriber has nobody a lost cm
// could be late for: its binding goes on receiving the bus, a later
// subscriber sees whatever the delivery point appends from then on, and
// a lost tail is caught up within two sweeps of the channel gaining a
// subscriber (DESIGN.md §7.2).
func (s *Storage) sweepStores() []*channelStore {
	stores := s.boundStores()
	if s.sweepAll {
		return stores
	}
	kept := stores[:0]
	for _, cs := range stores {
		if r, ok := cs.appender.(storage.SubscriberReporter); ok && !r.HasSubscribers() {
			continue
		}
		kept = append(kept, cs)
	}
	clear(stores[len(kept):])
	return kept
}

// sweepWatermarks is the chained buses' safety net (DESIGN.md §7.2). It
// reads the current serial of the channels sweepStores selects,
// sweepChunk names per query, and catches up each channel whose delivery
// mark is still behind the watermark this node read on the previous
// sweep: a cm that old whose bus message has not arrived is treated as
// lost, not late. Each shard of a shard list is its own Storage, so the
// queries go to the shard that holds the channels.
func (s *Storage) sweepWatermarks(ctx context.Context) error {
	stores := s.sweepStores()
	s.stats.sweepChannels.Add(uint64(len(stores)))
	var behind []*channelStore
	for start := 0; start < len(stores); start += sweepChunk {
		chunk := stores[start:min(start+sweepChunk, len(stores))]
		byName := make(map[string]*channelStore, len(chunk))
		names := make([]string, 0, len(chunk))
		for _, cs := range chunk {
			byName[cs.name] = cs
			names = append(names, cs.name)
		}
		if hook := sweepNamesHook.Load(); hook != nil {
			(*hook)(names)
		}
		rows, err := s.pool.Query(ctx, `SELECT name, channel_serial FROM channels WHERE name = ANY($1)`, names)
		if err != nil {
			return fmt.Errorf("storage/postgres: read watermarks: %w", err)
		}
		for rows.Next() {
			var name, watermark string
			if err := rows.Scan(&name, &watermark); err != nil {
				rows.Close()
				return fmt.Errorf("storage/postgres: scan watermark: %w", err)
			}
			cs := byName[name]
			cs.hwmMu.Lock()
			if cs.seeded && !cs.released && cs.sweptWatermark > cs.lastSeen {
				behind = append(behind, cs)
			}
			cs.sweptWatermark = watermark
			cs.hwmMu.Unlock()
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("storage/postgres: watermark rows: %w", err)
		}
	}
	for start := 0; start < len(behind); start += reconcileChunk {
		chunk := behind[start:min(start+reconcileChunk, len(behind))]
		if err := s.catchUpMany(ctx, chunk); err != nil {
			return fmt.Errorf("storage/postgres: sweep catch-up: %w", err)
		}
		for _, cs := range chunk {
			cs.hwmMu.Lock()
			cs.sweepCatchUps++
			cs.hwmMu.Unlock()
		}
		s.stats.sweepCatchUps.Add(uint64(len(chunk)))
	}
	return nil
}

// sqlLoadRange reads the cms on one channel in (after, upTo] ($3 = ""
// means unbounded), up to $4 of them, every kind, ascending.
const sqlLoadRange = `
SELECT channel_serial, idx, kind, payload, summary FROM channel_messages
WHERE channel = $1 AND channel_serial IN (
	SELECT DISTINCT channel_serial FROM channel_messages
	WHERE channel = $1 AND channel_serial > $2 AND ($3 = '' OR channel_serial <= $3)
	ORDER BY channel_serial
	LIMIT $4)
ORDER BY channel_serial, idx
`

// loadChannelMessagesAfter reads up to limit cms on channel with a
// serial in (after, upTo] (upTo "" means unbounded), of every kind,
// ascending, with annotation summary snapshots. The chained buses' gap
// fill and catch-up read the log through it; unlike the History-based
// reconcile of the pgnotify bus it includes annotation cms, which a
// chain must see to stay unbroken.
func loadChannelMessagesAfter(ctx context.Context, pool *pgxpool.Pool, channel, after, upTo string, limit int) ([]*protocol.ChannelMessage, error) {
	rows, err := pool.Query(ctx, sqlLoadRange, channel, after, upTo, limit)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: load range %s (%s, %s]: %w", channel, after, upTo, err)
	}
	defer rows.Close()

	var out []*protocol.ChannelMessage
	for rows.Next() {
		var (
			channelSerial string
			idx           int
			kind          string
			payload       []byte
			summary       []byte
		)
		if err := rows.Scan(&channelSerial, &idx, &kind, &payload, &summary); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan range %s: %w", channel, err)
		}
		if n := len(out); n == 0 || out[n-1].ChannelSerial != channelSerial {
			out = append(out, &protocol.ChannelMessage{ChannelSerial: channelSerial})
		}
		if err := decodeRowInto(out[len(out)-1], channel, idx, kind, payload, summary); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: range rows %s: %w", channel, err)
	}
	return out, nil
}
