// Package core implements the per-channel state and per-process
// channel manager — DESIGN.md §5.1.
package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// entry is a node in a Channel's linked list of ChannelMessages. Each
// entry is one atomic publish carrying one or more Messages. Streams
// tail the list at their own pace; entry.notify is closed once
// entry.next has been set, which wakes all parked streams. The list is
// grow-only — older entries become eligible for GC once no stream
// retains a reference.
//
// The sentinel head (the entry installed at construction, before any
// Append) carries a serial-only ChannelMessage once Initialize has
// run: the channel's initial watermark. That ChannelMessage has no
// Messages, so Stream.Next never returns it as a delivered cm — it
// is only inspected via Stream.ChannelSerial.
type entry struct {
	cm     *protocol.ChannelMessage
	notify chan struct{}
	next   *entry
}

// Channel holds the live ChannelMessage list for one channel name and
// the storage facet that backs it. It owns no goroutine; concurrency
// is serialised by mu around Append.
//
// Publish() is the orchestration entry point: it calls Store on the
// underlying storage and lets the storage backend drive the local
// Append via the Appender callback we registered at construction
// time. The same Append path is used for foreign publishes arriving
// via the Postgres broker in cluster mode (DESIGN.md §7).
//
// A Channel is created in a not-ready state; storage.Storage.Channel
// calls Initialize on it before returning to seed the sentinel's
// watermark serial, record the channel's immutable initial serial,
// and close ready. Attach blocks on ready, so a caller cannot observe
// an empty channelSerial.
type Channel struct {
	name          string
	store         storage.ChannelStore
	ready         chan struct{}
	initialSerial string // immutable after Initialize; sorts strictly less than every cm in this channel

	mu   sync.Mutex
	tail *entry // never nil: a sentinel is installed at construction
	// members is the set of presence members this Channel has seen
	// enter and not yet leave, keyed by storage.MemberKey. It is fed by
	// Append, so it counts every member whose ENTER reached this node
	// since the channel was bound, whichever node owns the member. A
	// channel with members is not evicted (DESIGN.md §5.1). Nil when
	// empty. Guarded by mu.
	members map[string]struct{}
	// pv is the local member set SYNC is served from (presence.go,
	// DESIGN.md §12.4). Unlike members it holds the whole set, seeded from
	// the store on first use. Guarded by mu.
	pv memberView

	// syncSource and syncRefresh configure SYNC (PresenceSync); metrics
	// receives its series (nil-safe). Set before the Channel is shared.
	syncSource  string
	syncRefresh time.Duration
	metrics     *metrics.Metrics

	// Lifecycle state for idle-channel eviction (DESIGN.md §5.1), guarded
	// by life. Kept apart from mu so that pinning a channel for an
	// operation never contends with the append path.
	mgr      *Manager // nil for channels built outside a Manager (tests)
	life     sync.Mutex
	evicted  bool  // set once, by the Manager's sweeper; the Channel is then dropped
	refs     int   // open Streams (attachments)
	inflight int   // storage operations in progress
	lastUsed int64 // Manager clock reading when refs or inflight last fell, or at bind

	// bound is closed by the Manager once the storage binding is done
	// (store set, or bindErr recorded); released is closed once an
	// evicted channel's storage Release has returned. GetChannel waits
	// on them so that a caller never sees a half-bound channel, and a
	// rebind never overlaps the Release of the channel it replaces.
	bound    chan struct{}
	bindErr  error
	released chan struct{}
}

// newChannel constructs a Channel in the not-ready state. The list
// starts with a sentinel entry (cm = nil) that Initialize will then
// populate with the watermark serial.
func newChannel(name string) *Channel {
	return &Channel{
		name:        name,
		ready:       make(chan struct{}),
		tail:        &entry{notify: make(chan struct{})},
		bound:       make(chan struct{}),
		released:    make(chan struct{}),
		syncSource:  PresenceSyncLocal,
		syncRefresh: DefaultPresenceSyncRefresh,
	}
}

// now reads the clock the Channel's SYNC snapshots age by: the
// Manager's, or the wall clock for a Channel built outside one.
func (c *Channel) now() int64 {
	if c.mgr != nil {
		return c.mgr.now()
	}
	return time.Now().UnixNano()
}

// logger returns the Manager's logger, or the default.
func (c *Channel) logger() *logging.Logger {
	if c.mgr != nil {
		return c.mgr.logger
	}
	return logging.Default()
}

// errNoManager is returned when an evicted Channel that was built
// outside a Manager is asked to redirect; only the Manager evicts, so
// this indicates a programming error.
var errNoManager = errors.New("core: evicted channel has no manager to rebind through")

// pin reserves the live Channel for name for one operation (attach
// false) or one Stream (attach true), and returns it. It is c itself
// unless c has been evicted since the caller obtained it; then pin
// rebinds through the Manager and pins the fresh Channel instead, so a
// caller holding a stale *Channel never operates on released storage
// (DESIGN.md §5.1). A pinned Channel is not evicted until unpin.
func (c *Channel) pin(ctx context.Context, attach bool) (*Channel, error) {
	ch := c
	for {
		ch.life.Lock()
		if !ch.evicted {
			if attach {
				ch.refs++
			} else {
				ch.inflight++
			}
			ch.life.Unlock()
			return ch, nil
		}
		ch.life.Unlock()
		if ch.mgr == nil {
			return nil, errNoManager
		}
		next, err := ch.mgr.GetChannel(ctx, ch.name)
		if err != nil {
			return nil, err
		}
		ch = next
	}
}

// unpin releases a reservation taken by pin, restarting the idle clock.
func (c *Channel) unpin(attach bool) {
	c.life.Lock()
	if attach {
		c.refs--
	} else {
		c.inflight--
	}
	if c.mgr != nil {
		c.lastUsed = c.mgr.now()
	}
	c.life.Unlock()
}

// begin pins the live Channel for one storage operation; the caller
// must defer end on the returned Channel.
func (c *Channel) begin(ctx context.Context) (*Channel, error) {
	return c.pin(ctx, false)
}

// end releases a reservation taken by begin.
func (c *Channel) end() {
	c.unpin(false)
}

// idle reports whether the channel may be evicted at time now: bound,
// not already evicted, no open Streams, no operation in flight, no
// presence members, and unused for at least timeout. Called with life
// held.
func (c *Channel) idle(now int64, timeout time.Duration) bool {
	select {
	case <-c.bound:
	default:
		return false // still binding
	}
	if c.evicted || c.bindErr != nil || c.refs > 0 || c.inflight > 0 {
		return false
	}
	if now-c.lastUsed < int64(timeout) {
		return false
	}
	c.mu.Lock()
	hasMembers := len(c.members) > 0
	c.mu.Unlock()
	return !hasMembers
}

// Name returns the channel name.
func (c *Channel) Name() string {
	return c.name
}

// Publish runs the full publish-and-link sequence: hand msgs to the
// underlying storage backend, which mints the channelSerial, persists,
// and (in cluster mode) emits a NOTIFY. The link onto the live list
// always arrives via the Appender callback the Channel registered at
// construction — synchronously in single-process backends,
// asynchronously via the LISTEN goroutine in cluster mode. The
// (cm, idempotent, err) tuple is forwarded verbatim from storage.
func (c *Channel) Publish(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer ch.end()
	return ch.store.Store(ctx, msgs)
}

// PublishPresence runs the presence-publish sequence: hand the presence
// messages to the storage backend (which mints the channelSerial, stamps
// each Serial, folds the membership set, and persists), then the link
// onto the live list arrives via the Appender callback exactly as for a
// message publish (DESIGN.md §12.2). The (cm, idempotent, err) tuple is
// forwarded verbatim from storage.
func (c *Channel) PublishPresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer ch.end()
	return ch.store.StorePresence(ctx, presence)
}

// PublishAnnotation runs the annotation-publish sequence: hand the
// annotations to the storage backend (which validates each target,
// mints the channelSerial, stamps each Serial, persists the annotation cm
// on the shared stream, and — the seam for the summary fold),
// then the link onto the live list arrives via the Appender callback
// exactly as for a message publish (DESIGN.md §14.1). The
// (cm, idempotent, err) tuple is forwarded verbatim — notably
// storage.ErrTargetNotFound when a target message does not exist.
func (c *Channel) PublishAnnotation(ctx context.Context, annotations []*protocol.Annotation) (*protocol.ChannelMessage, bool, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer ch.end()
	return ch.store.StoreAnnotation(ctx, annotations)
}

// Annotations returns the annotations attached to the message identified
// by messageSerial, paginated per q (DESIGN.md §14.4).
func (c *Channel) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	defer ch.end()
	return ch.store.Annotations(ctx, messageSerial, q)
}

// Mutate applies an update/delete/append to an existing message,
// delegating to the storage backend (which validates the target, merges,
// mints the new version, persists, and updates the projection/versions
// index). The link onto the live list arrives via the Appender callback
// exactly as for a publish, so subscribers see the new version in stream
// order (DESIGN.md §13.2). The (cm, idempotent, err) tuple is forwarded
// verbatim — notably storage.ErrTargetNotFound for an unknown target.
func (c *Channel) Mutate(ctx context.Context, mut *protocol.Message) (*protocol.ChannelMessage, bool, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer ch.end()
	return ch.store.Mutate(ctx, mut)
}

// LatestVersion returns the current latest version of the message
// identified by serial, or storage.ErrTargetNotFound (DESIGN.md §13.4).
func (c *Channel) LatestVersion(ctx context.Context, serial string) (*protocol.Message, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer ch.end()
	return ch.store.LatestVersion(ctx, serial)
}

// Versions returns every version of the message identified by serial,
// paginated per q (DESIGN.md §13.4).
func (c *Channel) Versions(ctx context.Context, serial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	defer ch.end()
	return ch.store.Versions(ctx, serial, q)
}

// History delegates to the underlying ChannelStore. Backends return
// ChannelMessages in the order requested by q.Direction (see
// storage.HistoryQuery); the REST and resume paths flatten the page
// without further reordering.
func (c *Channel) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return storage.HistoryPage{}, err
	}
	defer ch.end()
	return ch.store.History(ctx, q)
}

// RetainedSince returns the channel's retention floor as of now: the
// serial prefix at or after which every persisted cm is still held, or ""
// when the backend keeps everything (storage.RetentionBounded, DESIGN.md
// §4.3, §6.3).
func (c *Channel) RetainedSince(now time.Time) string {
	if rb, ok := c.store.(storage.RetentionBounded); ok {
		return rb.RetainedSince(now)
	}
	return ""
}

// Members returns the channel's current presence set plus the
// channelSerial the set is current as-of, delegating to the storage
// backend. Backs presence sync on attach (DESIGN.md §12.4).
func (c *Channel) Members(ctx context.Context) ([]*protocol.PresenceMessage, string, error) {
	ch, err := c.begin(ctx)
	if err != nil {
		return nil, "", err
	}
	defer ch.end()
	return ch.store.Members(ctx)
}

// Initialize seeds the sentinel with the channel's current watermark
// serial, records the channel's immutable initial serial, and marks
// the channel ready. The storage backend calls this exactly once
// before any Append. Subsequent calls are no-ops.
//
// current is the cursor fresh attachments use as their attach point
// (== latest persisted cm's serial, or the freshly-minted seed for an
// empty channel). initial is the channel's immutable seed — strictly
// less than every cm ever persisted on this channel — used as the
// attach point for rewinds that cover the entire channel history.
//
// Implements storage.Appender.
func (c *Channel) Initialize(current, initial string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		// Double-init: the contract says backends call this once. Honour
		// idempotency defensively so a re-Channel call doesn't panic.
		return
	default:
	}
	c.tail.cm = &protocol.ChannelMessage{ChannelSerial: current}
	c.initialSerial = initial
	close(c.ready)
}

// InitialChannelSerial returns the channel's immutable initial serial,
// recorded at Initialize time. Used as the ATTACHED.channelSerial for
// rewind ATTACHes that cover the entire channel history (no
// predecessor cm exists in storage to use instead).
//
// Safe to call only after the channel is ready (Attach has unblocked).
func (c *Channel) InitialChannelSerial() string {
	return c.initialSerial
}

// Append links an already-minted ChannelMessage at the tail as a
// single entry, waking any parked streams. It satisfies the
// storage.Appender interface — the storage backend calls this to
// deliver a persisted cm to subscribers (the publisher's own publish
// in memory/bbolt; every node's publish in cluster mode).
//
// A no-op when cm is nil or carries no items — a presence cm or an
// annotation cm (DESIGN.md §12.2, §14.1) links onto the list exactly like
// a message cm.
func (c *Channel) Append(cm *protocol.ChannelMessage) {
	if cm == nil || (len(cm.Messages) == 0 && len(cm.Presence) == 0 && len(cm.Annotations) == 0) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(cm.Presence) > 0 {
		c.trackMembers(cm.Presence)
		if c.pv.seeded || c.pv.seeding != nil {
			c.pv.observe(cm, c.now(), c.syncRefresh)
		}
	}
	e := &entry{cm: cm, notify: make(chan struct{})}
	c.tail.next = e
	close(c.tail.notify)
	c.tail = e
}

// trackMembers folds a presence cm into the local member set that
// holds off eviction (DESIGN.md §5.1): ENTER/UPDATE/PRESENT add the
// member, LEAVE/ABSENT remove it. Called with mu held.
func (c *Channel) trackMembers(presence []*protocol.PresenceMessage) {
	for _, p := range presence {
		key := storage.MemberKey(p.ConnectionID, p.ClientID)
		switch p.Action {
		case protocol.PresenceEnter, protocol.PresenceUpdate, protocol.PresencePresent:
			if c.members == nil {
				c.members = make(map[string]struct{})
			}
			c.members[key] = struct{}{}
		case protocol.PresenceLeave, protocol.PresenceAbsent:
			delete(c.members, key)
		}
	}
	if len(c.members) == 0 {
		c.members = nil
	}
}

// Attach blocks until the channel is ready (Initialize has run), then
// returns a Stream positioned at the current tail. The Stream's first
// Next call blocks until the next ChannelMessage is appended.
//
// The Stream holds the channel against eviction until Close. If c was
// evicted after the caller obtained it, the Stream is opened on the
// freshly bound Channel for the same name (see Stream.Channel).
//
// Returns ctx.Err() if the context cancels before the channel readies.
func (c *Channel) Attach(ctx context.Context) (*Stream, error) {
	ch, err := c.pin(ctx, true)
	if err != nil {
		return nil, err
	}
	select {
	case <-ch.ready:
	case <-ctx.Done():
		ch.unpin(true)
		return nil, ctx.Err()
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return &Stream{cursor: ch.tail, ch: ch}, nil
}

// Stream is an attachment's per-channel view of the linked list. Its
// methods are not safe for concurrent use; an attachment is expected
// to drive a single Stream from one goroutine.
type Stream struct {
	cursor *entry
	ch     *Channel
	closed atomic.Bool
}

// Channel returns the Channel this Stream is attached to. It differs
// from the Channel Attach was called on only when that one had been
// evicted in the meantime.
func (s *Stream) Channel() *Channel {
	return s.ch
}

// Close releases the Stream's hold on its Channel, so the channel can
// be evicted once idle (DESIGN.md §5.1). Idempotent.
func (s *Stream) Close() {
	if s.closed.CompareAndSwap(false, true) {
		s.ch.unpin(true)
	}
}

// ChannelSerial returns the cursor's current position — the
// channelSerial at the cursor (the sentinel's watermark before any
// cm has been delivered, or the last delivered cm's serial after).
// Never empty for a Stream returned by a ready Channel.
func (s *Stream) ChannelSerial() string {
	return s.cursor.cm.ChannelSerial
}

// Next blocks until the next ChannelMessage is available, advances
// the cursor to that entry, and returns the ChannelMessage. Returns
// ctx.Err() if the context is cancelled.
func (s *Stream) Next(ctx context.Context) (*protocol.ChannelMessage, error) {
	select {
	case <-s.cursor.notify:
		s.cursor = s.cursor.next
		return s.cursor.cm, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
