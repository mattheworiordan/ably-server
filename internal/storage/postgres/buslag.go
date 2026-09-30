package postgres

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Bus delivery lag (DESIGN.md §10, ably_bus_delivery_lag_seconds): for
// every cm a node appends that came from another node, the time from
// the cm's commit to the append, so a run can tell time spent on the
// bus from time spent in the server. The start is the bus message's
// send time when the bus carries one (the nats envelope's SentAt, taken
// straight after the commit); otherwise the cm's own timestamp as
// stored (set by the publishing node just before its transaction, in
// milliseconds), which is all a cm read from the log carries. The
// publisher fast path is not recorded: it is the publishing node's own
// commit, with no bus in between. Clocks are the two nodes'; a negative
// lag (clock skew) counts as zero.

// Lag paths, matching the ably_bus_*_deliveries_total counters.
const (
	lagInline  = iota // body carried by the bus message
	lagFetched        // read back by serial: a pointer, or a pgnotify delivery
	lagFilled         // a log range read: gap fill, catch-up, reconcile, sweep, coalesced wake-up
	lagPaths
)

var lagPathNames = [lagPaths]string{"inline", "fetched", "filled"}

// lagHistogram counts observations per storage.BusLagBuckets bucket
// (not cumulative; the last slot is +Inf) and their sum.
type lagHistogram struct {
	counts   [16]atomic.Uint64 // len(storage.BusLagBuckets)+1 slots used
	sumNanos atomic.Uint64
}

func init() {
	if len(storage.BusLagBuckets)+1 > len(lagHistogram{}.counts) {
		panic("storage/postgres: lagHistogram has fewer slots than storage.BusLagBuckets")
	}
}

func (h *lagHistogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := sort.SearchFloat64s(storage.BusLagBuckets, d.Seconds())
	h.counts[i].Add(1)
	h.sumNanos.Add(uint64(d))
}

// snapshot returns the histogram with cumulative bucket counts.
func (h *lagHistogram) snapshot() storage.LagHistogram {
	out := storage.LagHistogram{Counts: make([]uint64, len(storage.BusLagBuckets))}
	var cum uint64
	for i := range storage.BusLagBuckets {
		cum += h.counts[i].Load()
		out.Counts[i] = cum
	}
	out.Count = cum + h.counts[len(storage.BusLagBuckets)].Load()
	out.Sum = time.Duration(h.sumNanos.Load()).Seconds()
	return out
}

// observeLag records one cross-node delivery on path. sentAt is the bus
// message's send time (Unix ns), or 0 to use the cm's stored timestamp;
// a cm with neither is not recorded.
func (c *busStats) observeLag(path int, sentAt int64, cm *protocol.ChannelMessage) {
	var start time.Time
	switch {
	case sentAt > 0:
		start = time.Unix(0, sentAt)
	default:
		ms := cmTimestamp(cm)
		if ms <= 0 {
			return
		}
		start = time.UnixMilli(ms)
	}
	c.lag[path].observe(time.Since(start))
}

// lagSnapshot returns the delivery-lag histograms by path name.
func (c *busStats) lagSnapshot() map[string]storage.LagHistogram {
	out := make(map[string]storage.LagHistogram, lagPaths)
	for i := range c.lag {
		out[lagPathNames[i]] = c.lag[i].snapshot()
	}
	return out
}

// cmTimestamp is the time the publishing node stamped on the cm, in
// Unix milliseconds: a message's version timestamp (which a mutation
// sets to its own time, and a create to the create's), else its
// timestamp, else a presence or annotation item's. 0 if none.
func cmTimestamp(cm *protocol.ChannelMessage) int64 {
	if cm == nil {
		return 0
	}
	for _, m := range cm.Messages {
		if m.Version != nil && m.Version.Timestamp > 0 {
			return m.Version.Timestamp
		}
		if m.Timestamp > 0 {
			return m.Timestamp
		}
	}
	for _, p := range cm.Presence {
		if p.Timestamp > 0 {
			return p.Timestamp
		}
	}
	for _, a := range cm.Annotations {
		if a.Timestamp > 0 {
			return a.Timestamp
		}
	}
	return 0
}
