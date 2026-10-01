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

// lagHistogram counts observations per bucket of one bounds list
// (storage.BusLagBuckets for the delivery lag, storage.StageBuckets for
// the stages), not cumulative, with the last used slot for +Inf, and
// their sum.
type lagHistogram struct {
	counts   [16]atomic.Uint64 // len(bounds)+1 slots used
	sumNanos atomic.Uint64
}

func init() {
	for _, b := range [][]float64{storage.BusLagBuckets, storage.StageBuckets} {
		if len(b)+1 > len(lagHistogram{}.counts) {
			panic("storage/postgres: lagHistogram has fewer slots than a bucket list")
		}
	}
}

func (h *lagHistogram) observe(d time.Duration) {
	h.observeOn(storage.BusLagBuckets, d)
}

func (h *lagHistogram) observeOn(bounds []float64, d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := sort.SearchFloat64s(bounds, d.Seconds())
	h.counts[i].Add(1)
	h.sumNanos.Add(uint64(d))
}

// snapshot returns the histogram with cumulative bucket counts.
func (h *lagHistogram) snapshot() storage.LagHistogram {
	return h.snapshotOn(storage.BusLagBuckets)
}

func (h *lagHistogram) snapshotOn(bounds []float64) storage.LagHistogram {
	out := storage.LagHistogram{Counts: make([]uint64, len(bounds))}
	var cum uint64
	for i := range bounds {
		cum += h.counts[i].Load()
		out.Counts[i] = cum
	}
	out.Count = cum + h.counts[len(bounds)].Load()
	out.Sum = time.Duration(h.sumNanos.Load()).Seconds()
	return out
}

// Receive-side delivery stages (DESIGN.md §10, storage.BusStats.Stages),
// on the storage.StageBuckets bounds.
const (
	stageQueueWait = iota // nats: publisher send to dispatch-worker pickup
	stageHold             // a held cm: hold to append
	stageAppend           // inside the appender's Append
	stages
)

var stageNames = [stages]string{"receive_queue_wait", "hold", "append"}

// observeStage records one observation of a receive-side stage.
func (c *busStats) observeStage(stage int, d time.Duration) {
	c.stage[stage].observeOn(storage.StageBuckets, d)
}

// stageSnapshot returns the stage histograms by stage name.
func (c *busStats) stageSnapshot() map[string]storage.LagHistogram {
	out := make(map[string]storage.LagHistogram, stages)
	for i := range c.stage {
		out[stageNames[i]] = c.stage[i].snapshotOn(storage.StageBuckets)
	}
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
