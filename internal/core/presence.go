package core

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

// Presence sync sources (DESIGN.md §12.4): where an attach's SYNC
// snapshot comes from.
const (
	// PresenceSyncLocal serves SYNC from the node's own member set:
	// seeded from the store once per channel bind, then folded from
	// every presence cm the node delivers on the channel. The default.
	PresenceSyncLocal = "local"
	// PresenceSyncStore reads the store's member set on every SYNC, as
	// before the local set existed.
	PresenceSyncStore = "store"
)

// ParsePresenceSyncSource validates a --presence-sync-source value. The
// empty string means PresenceSyncLocal.
func ParsePresenceSyncSource(s string) (string, error) {
	switch s {
	case "", PresenceSyncLocal:
		return PresenceSyncLocal, nil
	case PresenceSyncStore:
		return PresenceSyncStore, nil
	}
	return "", errors.New(`presence sync source must be "local" or "store"`)
}

// DefaultPresenceSyncRefresh bounds how often a channel's SYNC snapshot
// is rebuilt while its members keep changing: a snapshot younger than
// this is served as it is, with the presence cms after it (DESIGN.md
// §12.4).
const DefaultPresenceSyncRefresh = 50 * time.Millisecond

// maxSnapshotGap caps the presence cms a stale snapshot is served with;
// past it the snapshot is dropped and the next SYNC rebuilds.
const maxSnapshotGap = 256

// PresenceSnapshot is a channel's member set as of one serial, shared by
// every attach it is served to. Members and AsOf are read-only.
type PresenceSnapshot struct {
	// Members are the set's members, each a copy with action PRESENT, in
	// serial order.
	Members []*protocol.PresenceMessage
	// AsOf is the channelSerial the set is current as of.
	AsOf string

	memo sync.Map // key -> *memoEntry
}

type memoEntry struct {
	once sync.Once
	v    any
}

// Memo returns the value build makes for key, calling build at most once
// per snapshot and key. The realtime layer keeps the snapshot's encoded
// SYNC frame here, one per wire format, so a snapshot served to many
// attaches is encoded once.
func (s *PresenceSnapshot) Memo(key any, build func() any) any {
	e, _ := s.memo.LoadOrStore(key, &memoEntry{})
	me := e.(*memoEntry)
	me.once.Do(func() { me.v = build() })
	return me.v
}

// newSnapshot copies members into a snapshot, stamping each PRESENT.
func newSnapshot(members []*protocol.PresenceMessage, asOf string) *PresenceSnapshot {
	out := make([]*protocol.PresenceMessage, len(members))
	for i, m := range members {
		cp := *m
		cp.Action = protocol.PresencePresent
		out[i] = &cp
	}
	return &PresenceSnapshot{Members: out, AsOf: asOf}
}

// memberView is a Channel's local member set (DESIGN.md §12.4, §12.5):
// a cache of the store's set as of a serial. It is seeded from
// store.Members the first time a SYNC needs it, then kept current by
// Append, which delivers the channel's cms in serial order, including
// other nodes' presence events and the reaper's synthesised LEAVEs.
// Guarded by Channel.mu.
type memberView struct {
	seeded  bool
	seeding chan struct{} // non-nil while a seed read is in flight; closed when it ends
	buffer  []*protocol.ChannelMessage

	members map[string]*protocol.PresenceMessage // by storage.MemberKey
	asOf    string                               // the set is the fold of every cm up to here

	// snap is the cached snapshot, built at snapBuilt (Channel clock).
	// since holds the presence cms folded after it was built, which it
	// does not reflect (stale when non-empty).
	snap      *PresenceSnapshot
	snapBuilt int64
	since     []*protocol.ChannelMessage
}

// observe feeds one delivered presence cm to the view: buffered while a
// seed read is in flight, folded once seeded, ignored before the first
// SYNC needs the view. Called with Channel.mu held.
func (v *memberView) observe(cm *protocol.ChannelMessage, now int64, refresh time.Duration) {
	switch {
	case v.seeding != nil:
		v.buffer = append(v.buffer, cm)
	case v.seeded:
		if !v.fold(cm) || v.snap == nil {
			return
		}
		// The cached snapshot no longer reflects the set. Keep it for the
		// refresh window, with the cms since, unless it is already older
		// than that or the gap has grown too long: then drop it, so it
		// pins nothing.
		if now-v.snapBuilt >= int64(refresh) || len(v.since) >= maxSnapshotGap {
			v.snap, v.since = nil, nil
			return
		}
		v.since = append(v.since, cm)
	}
}

// fold applies a presence cm to the set if it sorts after the set's
// as-of serial, and reports whether it did. Within it, an operation
// older than the member's current state (by member serial) is skipped:
// last writer wins, the rule the SDK merge also applies (RTP2).
func (v *memberView) fold(cm *protocol.ChannelMessage) bool {
	if v.asOf != "" && cm.ChannelSerial <= v.asOf {
		return false // already reflected: in the seed, or delivered twice
	}
	for _, p := range cm.Presence {
		key := storage.MemberKey(p.ConnectionID, p.ClientID)
		if cur, ok := v.members[key]; ok && !newerPresence(p, cur) {
			continue
		}
		switch p.Action {
		case protocol.PresenceEnter, protocol.PresenceUpdate, protocol.PresencePresent:
			v.members[key] = p
		case protocol.PresenceLeave, protocol.PresenceAbsent:
			delete(v.members, key)
		}
	}
	v.asOf = cm.ChannelSerial
	return true
}

// newerPresence reports whether p supersedes cur: p's serial sorts after
// cur's, comparing the channelSerial and then the index numerically. An
// unstamped serial is treated as newest.
func newerPresence(p, cur *protocol.PresenceMessage) bool {
	if p.Serial == "" || cur.Serial == "" {
		return true
	}
	return compareMemberSerial(p.Serial, cur.Serial) > 0
}

func compareMemberSerial(a, b string) int {
	ac, ai, aerr := serial.ParseMessageSerial(a)
	bc, bi, berr := serial.ParseMessageSerial(b)
	if aerr != nil || berr != nil {
		return strings.Compare(a, b)
	}
	return cmp.Or(strings.Compare(ac, bc), cmp.Compare(ai, bi))
}

// build returns a snapshot of the current set as of asOf.
func (v *memberView) build(asOf string) *PresenceSnapshot {
	members := make([]*protocol.PresenceMessage, 0, len(v.members))
	for _, p := range v.members {
		members = append(members, p)
	}
	slices.SortFunc(members, func(a, b *protocol.PresenceMessage) int {
		return cmp.Or(compareMemberSerial(a.Serial, b.Serial), strings.Compare(a.ClientID, b.ClientID), strings.Compare(a.ConnectionID, b.ConnectionID))
	})
	return newSnapshot(members, asOf)
}

// PresenceSync returns the snapshot an attach's SYNC delivers
// (DESIGN.md §12.4) and the presence operations the snapshot does not
// reflect that the attach's stream will not deliver either.
//
// at is the attaching Stream's position (Stream.ChannelSerial before its
// first Next). Every cm after at reaches the attach on its stream, so a
// snapshot is complete for it if it reflects every presence cm up to at.
// A snapshot built at or after at is; one built before it (served within
// the refresh window so a busy room is not re-encoded for every attach)
// is returned with gap, the operations of its missing cms up to at, for
// the caller to deliver after it. An empty at asks for a current
// snapshot (a client-initiated SYNC).
//
// In store mode, or when the local set cannot be seeded, the snapshot is
// read from the store and gap is nil.
func (c *Channel) PresenceSync(ctx context.Context, at string) (snap *PresenceSnapshot, gap []*protocol.PresenceMessage, err error) {
	if c.syncSource == PresenceSyncStore {
		c.metrics.PresenceSync("store")
		return c.storeSnapshot(ctx)
	}
	if err := c.seedMembers(ctx); err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		c.logger().Warn("presence sync: seeding the local member set failed; reading the store", "channel", c.name, "err", err)
		c.metrics.PresenceSync("fallback")
		return c.storeSnapshot(ctx)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	v := &c.pv
	switch {
	case v.snap != nil && len(v.since) == 0:
		c.metrics.PresenceSync("cached")
		return v.snap, nil, nil
	case v.snap != nil && at != "" && c.now()-v.snapBuilt < int64(c.syncRefresh):
		for _, cm := range v.since {
			if cm.ChannelSerial > at {
				break
			}
			gap = append(gap, cm.Presence...)
		}
		c.metrics.PresenceSync("stale")
		return v.snap, gap, nil
	}
	// The set covers every delivered cm, and cms that change no member do
	// not change it, so it is current as of the tail too.
	asOf := v.asOf
	if tail := c.tail.cm; tail != nil && tail.ChannelSerial > asOf {
		asOf = tail.ChannelSerial
	}
	v.snap, v.snapBuilt, v.since = v.build(asOf), c.now(), nil
	c.metrics.PresenceSync("built")
	return v.snap, nil, nil
}

// storeSnapshot reads the store's member set into a snapshot.
func (c *Channel) storeSnapshot(ctx context.Context) (*PresenceSnapshot, []*protocol.PresenceMessage, error) {
	members, asOf, err := c.Members(ctx)
	if err != nil {
		return nil, nil, err
	}
	return newSnapshot(members, asOf), nil, nil
}

// seedMembers seeds the local member set from the store, once per bind;
// concurrent callers wait for the one read. The store's set and as-of
// serial come from one snapshot, so the cms delivered meanwhile, which
// the view buffers, fold on top of it exactly: those at or below the
// as-of serial are already in it, those after are not.
func (c *Channel) seedMembers(ctx context.Context) error {
	c.mu.Lock()
	for !c.pv.seeded {
		if wait := c.pv.seeding; wait != nil {
			c.mu.Unlock()
			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
			c.mu.Lock()
			continue
		}
		done := make(chan struct{})
		c.pv.seeding, c.pv.buffer = done, nil
		c.mu.Unlock()

		members, asOf, err := c.store.Members(ctx)

		c.mu.Lock()
		c.pv.seeding = nil
		close(done)
		if err != nil {
			c.pv.buffer = nil
			c.mu.Unlock()
			return err
		}
		c.pv.members = make(map[string]*protocol.PresenceMessage, len(members))
		for _, p := range members {
			c.pv.members[storage.MemberKey(p.ConnectionID, p.ClientID)] = p
		}
		c.pv.asOf = asOf
		for _, cm := range c.pv.buffer {
			c.pv.fold(cm)
		}
		c.pv.buffer, c.pv.seeded = nil, true
		c.metrics.PresenceSeed()
	}
	c.mu.Unlock()
	return nil
}
