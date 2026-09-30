package postgres

// Coalesced notify mode of the postgres bus (DESIGN.md §7.2). Postgres
// serialises every transaction that issued NOTIFY on one cluster-wide
// lock, taken at commit and held through the commit's WAL flush, so
// NOTIFYing transactions cannot group-commit. In coalesced mode the
// write transaction commits without pg_notify. A per-node notifier then
// sends at most one wake-up per channel per window, outside any
// transaction, and receivers answer a wake-up with one range read of
// everything after their last delivered serial. The shared watermark
// sweep (chain.go) is the safety net, so a lost wake-up only adds
// latency.
//
// Overflow policy. A wake-up is small and bounded: the Postgres channel
// name plus {"serial": ..., "wake": true}, whatever the Ably channel
// name, so it can never exceed pg_notify's payload limit. The pending
// set holds one entry per channel written in the window, so a hot
// channel costs one entry however often it is written. It is capped at
// maxPending channels: a write to a channel not already pending when the
// set is full is not marked, and is counted as overflow; receivers
// deliver it from the log on their next sweep (at most about two sweep
// intervals late). A flush sends wakeFlushChunk wake-ups per statement;
// a chunk whose statement fails (a Postgres error, or a full NOTIFY
// queue) goes back into the pending set for the next window, within the
// same cap. The notifier flushes one window at a time: if a flush takes
// longer than the window, marks arriving meanwhile coalesce into the
// next flush, so falling behind costs latency and never grows the set
// beyond one entry per channel.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// NotifyMode selects how a committed write on the postgres bus reaches
// the other nodes that hold its channel (DESIGN.md §7.2).
type NotifyMode string

const (
	// NotifyTransactional sends one NOTIFY per write, inside the write's
	// transaction, carrying the cm inline when it fits.
	NotifyTransactional NotifyMode = "transactional"
	// NotifyCoalesced commits writes without NOTIFY; a per-node notifier
	// sends at most one wake-up per channel per NotifyWindow, and
	// receivers read everything after their last delivered serial. It is
	// the default.
	NotifyCoalesced NotifyMode = "coalesced"
)

// ParseNotifyMode validates a notify mode name. The empty string is the
// default, NotifyCoalesced.
func ParseNotifyMode(s string) (NotifyMode, error) {
	switch NotifyMode(s) {
	case "", NotifyCoalesced:
		return NotifyCoalesced, nil
	case NotifyTransactional:
		return NotifyTransactional, nil
	}
	return "", fmt.Errorf("unknown postgres notify mode %q (valid: %s, %s)", s, NotifyTransactional, NotifyCoalesced)
}

const (
	// DefaultNotifyWindow is the default coalescing window.
	DefaultNotifyWindow = 50 * time.Millisecond
	// DefaultNotifyMaxPending is the default cap on channels pending a
	// wake-up on one node.
	DefaultNotifyMaxPending = 65536

	// wakeFlushChunk caps the wake-ups sent in one statement.
	wakeFlushChunk = 1000
)

// wakeNotifier is the per-node coalescing notifier. mark records that a
// channel committed a write; the first mark in a quiet period starts a
// window, and at its end the notifier sends one wake-up for every
// channel marked in it.
type wakeNotifier struct {
	s          *Storage
	window     time.Duration
	maxPending int
	kick       chan struct{}

	mu      sync.Mutex
	pending map[string]string // Postgres channel name -> latest serial committed in the window
}

func newWakeNotifier(s *Storage, window time.Duration, maxPending int) *wakeNotifier {
	if window <= 0 {
		window = DefaultNotifyWindow
	}
	if maxPending <= 0 {
		maxPending = DefaultNotifyMaxPending
	}
	return &wakeNotifier{s: s, window: window, maxPending: maxPending, kick: make(chan struct{}, 1), pending: make(map[string]string)}
}

// mark records a committed write on a channel for the current window.
// It reports false when the overflow policy dropped the mark.
func (w *wakeNotifier) mark(pgChan, serial string) bool {
	w.mu.Lock()
	ok := w.addLocked(pgChan, serial)
	w.mu.Unlock()
	if !ok {
		w.s.stats.overflow.Add(1)
		return false
	}
	select {
	case w.kick <- struct{}{}:
	default:
	}
	return true
}

// addLocked adds or raises a pending entry, within the cap.
func (w *wakeNotifier) addLocked(pgChan, serial string) bool {
	cur, ok := w.pending[pgChan]
	if !ok && len(w.pending) >= w.maxPending {
		return false
	}
	if serial > cur {
		w.pending[pgChan] = serial
	}
	return true
}

// take swaps out the pending set for a flush.
func (w *wakeNotifier) take() map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	batch := w.pending
	w.pending = make(map[string]string, len(batch))
	return batch
}

// retain puts a chunk whose flush failed back into the pending set,
// within the cap, and kicks the next window.
func (w *wakeNotifier) retain(chans, serials []string) {
	w.mu.Lock()
	dropped := 0
	for i := range chans {
		if !w.addLocked(chans[i], serials[i]) {
			dropped++
		}
	}
	w.mu.Unlock()
	w.s.stats.overflow.Add(uint64(dropped))
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
		if !sleepCtx(ctx, w.window) {
			w.finalFlush()
			return
		}
		w.flush(ctx)
	}
}

func (w *wakeNotifier) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.flush(ctx)
}

// flush sends one wake-up per channel marked since the last flush,
// wakeFlushChunk per statement, each outside any explicit transaction.
// The payload names the latest serial this node committed on the channel
// in the window, so a receiver that is already past it skips the read.
func (w *wakeNotifier) flush(ctx context.Context) {
	batch := w.take()
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	chans := make([]string, 0, len(batch))
	serials := make([]string, 0, len(batch))
	for pgChan, serial := range batch {
		chans = append(chans, pgChan)
		serials = append(serials, serial)
	}
	for lo := 0; lo < len(chans); lo += wakeFlushChunk {
		hi := min(lo+wakeFlushChunk, len(chans))
		payloads := make([]string, 0, hi-lo)
		for _, serial := range serials[lo:hi] {
			b, _ := json.Marshal(busNotification{Serial: serial, Wake: true})
			payloads = append(payloads, string(b))
		}
		w.s.stats.flushes.Add(1)
		if _, err := w.s.pool.Exec(ctx,
			`SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)`,
			chans[lo:hi], payloads,
		); err != nil {
			w.s.stats.flushErrors.Add(1)
			if ctx.Err() == nil {
				w.s.logger.Warn("storage/postgres: coalesced wake-up flush failed; retrying next window", "channels", hi-lo, "err", err)
				w.retain(chans[lo:hi], serials[lo:hi])
			}
			continue
		}
		w.s.stats.wakeupsSent.Add(uint64(hi - lo))
	}
	w.s.stats.flushNanos.Add(uint64(time.Since(start)))
}
