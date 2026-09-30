package postgres

// Coalesced notify mode (DESIGN.md §7.2). Postgres serialises every
// transaction that issued NOTIFY on one cluster-wide lock, taken at
// commit and held through the commit's WAL flush, so NOTIFYing
// transactions cannot group-commit. In coalesced mode the write
// transaction commits without pg_notify. A per-node notifier then sends
// at most one wake-up per channel per window, outside any transaction,
// and receivers answer a wake-up with one range read of everything after
// their last delivered serial. A slow poll of the channels table is the
// safety net, so a lost wake-up only adds latency.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// NotifyMode selects how a committed write reaches the other nodes that
// hold its channel (DESIGN.md §7.2).
type NotifyMode string

const (
	// NotifyPerPublish sends one NOTIFY per write, inside the write's
	// transaction, carrying the cm inline when it fits. It is the default.
	NotifyPerPublish NotifyMode = "publish"
	// NotifyCoalesced commits writes without NOTIFY; a per-node notifier
	// sends at most one wake-up per channel per NotifyWindow, and
	// receivers read everything after their last delivered serial.
	NotifyCoalesced NotifyMode = "coalesced"
)

// ParseNotifyMode validates a notify mode name. The empty string is the
// default, NotifyPerPublish.
func ParseNotifyMode(s string) (NotifyMode, error) {
	switch NotifyMode(s) {
	case "", NotifyPerPublish:
		return NotifyPerPublish, nil
	case NotifyCoalesced:
		return NotifyCoalesced, nil
	}
	return "", fmt.Errorf("unknown postgres notify mode %q (valid: %s, %s)", s, NotifyPerPublish, NotifyCoalesced)
}

const (
	defaultNotifyWindow = 50 * time.Millisecond
	defaultPollInterval = 2 * time.Second

	// pollBatchSize caps the channel names in one safety-net poll query.
	pollBatchSize = 1000
)

// wakeNotifier is the per-node coalescing notifier. mark records that a
// channel committed a write; the first mark in a quiet period starts a
// window, and at its end one statement sends a wake-up for every channel
// marked in it.
type wakeNotifier struct {
	s      *Storage
	window time.Duration
	kick   chan struct{}

	mu    sync.Mutex
	dirty map[string]wakeEntry // by Postgres channel name
}

type wakeEntry struct{ channel, serial string }

func newWakeNotifier(s *Storage, window time.Duration) *wakeNotifier {
	return &wakeNotifier{s: s, window: window, kick: make(chan struct{}, 1), dirty: make(map[string]wakeEntry)}
}

// mark records a committed write on a channel for the current window.
func (w *wakeNotifier) mark(pgChan, channel, serial string) {
	w.mu.Lock()
	if cur, ok := w.dirty[pgChan]; !ok || serial > cur.serial {
		w.dirty[pgChan] = wakeEntry{channel: channel, serial: serial}
	}
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// run waits for a mark, lets the window fill, flushes, and repeats until
// the storage closes; it flushes once more on the way out.
func (w *wakeNotifier) run(ctx context.Context) {
	defer w.s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			w.finalFlush()
			return
		case <-w.kick:
		}
		select {
		case <-ctx.Done():
			w.finalFlush()
			return
		case <-time.After(w.window):
		}
		w.flush(ctx)
	}
}

func (w *wakeNotifier) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.flush(ctx)
}

// flush sends one wake-up per channel marked since the last flush, in a
// single statement outside any explicit transaction. The payload names
// the latest serial this node committed on the channel in the window, so
// a receiver that is already past it skips the read.
func (w *wakeNotifier) flush(ctx context.Context) {
	w.mu.Lock()
	batch := w.dirty
	w.dirty = make(map[string]wakeEntry, len(batch))
	w.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	chans := make([]string, 0, len(batch))
	payloads := make([]string, 0, len(batch))
	for pgChan, e := range batch {
		b, err := json.Marshal(busNotification{Channel: e.channel, Serial: e.serial, Wake: true})
		if err != nil {
			continue
		}
		chans = append(chans, pgChan)
		payloads = append(payloads, string(b))
	}
	if _, err := w.s.pool.Exec(ctx,
		`SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)`,
		chans, payloads,
	); err != nil {
		// Receivers' safety-net poll delivers these writes instead.
		w.s.logger.Warn("storage/postgres: coalesced wake-up failed", "channels", len(chans), "err", err)
		return
	}
	w.s.stats.wakeupsSent.Add(uint64(len(chans)))
}

// pollLoop is the coalesced mode's safety net: every interval it compares
// each bound channel's committed serial (the channels row) with the
// node's high-water mark and queues a range read for any channel that is
// behind, so a lost wake-up costs at most one interval of latency.
func (s *Storage) pollLoop(ctx context.Context, interval time.Duration) {
	defer s.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.pollOnce(ctx)
		}
	}
}

func (s *Storage) pollOnce(ctx context.Context) {
	s.mu.RLock()
	stores := make(map[string]*channelStore, len(s.bound))
	for _, cs := range s.bound {
		if cs.isReady() {
			stores[cs.name] = cs
		}
	}
	s.mu.RUnlock()
	s.stats.polls.Add(1)

	names := make([]string, 0, len(stores))
	for name := range stores {
		names = append(names, name)
	}
	for start := 0; start < len(names); start += pollBatchSize {
		end := min(start+pollBatchSize, len(names))
		rows, err := s.pool.Query(ctx, `SELECT name, channel_serial FROM channels WHERE name = ANY($1)`, names[start:end])
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("storage/postgres: safety-net poll failed", "err", err)
			}
			return
		}
		var behind []*channelStore
		for rows.Next() {
			var name, committed string
			if err := rows.Scan(&name, &committed); err != nil {
				rows.Close()
				return
			}
			if cs := stores[name]; cs != nil && committed > cs.watermark() {
				behind = append(behind, cs)
			}
		}
		rows.Close()
		for _, cs := range behind {
			if cs.requestPull(ctx) {
				s.stats.pollPulls.Add(1)
			}
		}
	}
}

// requestPull queues one range read on the channel's worker unless one is
// already queued. It reports whether it queued one.
func (cs *channelStore) requestPull(ctx context.Context) bool {
	if !cs.pullQueued.CompareAndSwap(false, true) {
		return false
	}
	if !cs.enqueue(ctx, busItem{pull: true}) {
		cs.pullQueued.Store(false)
		return false
	}
	return true
}
