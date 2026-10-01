package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/storage"
)

// DefaultChannelIdleTimeout is how long a channel with no attachments,
// no operation in flight and no presence members stays bound before the
// Manager evicts it (DESIGN.md §5.1, §9).
const DefaultChannelIdleTimeout = 60 * time.Second

// shardCount is the number of independently locked channel maps. A
// power of two, so the shard index is a mask of the name hash. Sharding
// keeps GetChannel (called once per publish, attach and REST request)
// from contending on one lock at hundreds of thousands of channels, and
// bounds how long the sweeper holds any one lock.
const shardCount = 64

// releaseTimeout bounds one storage Release call made by the sweeper.
const releaseTimeout = 5 * time.Second

// Options configures a Manager. The zero value disables eviction.
type Options struct {
	// IdleTimeout is how long an idle channel stays bound before it is
	// evicted. Zero disables eviction: channels are then held for the
	// life of the process, the behaviour before eviction existed.
	IdleTimeout time.Duration

	// SweepInterval is how often the sweeper looks for idle channels.
	// Zero picks IdleTimeout/10, clamped to [5ms, 5s], so a channel is
	// evicted between IdleTimeout and about 1.1 × IdleTimeout after it
	// fell idle.
	SweepInterval time.Duration

	// Metrics receives the channel lifecycle series. Nil disables them.
	Metrics *metrics.Metrics

	// Logger receives Release failures. Nil means logging.Default().
	Logger *logging.Logger

	// WriteOnlyPublish enables the write-only publish path (DESIGN.md
	// §5.1): when the storage implements storage.UnboundPublisher,
	// WriteOnlyStore hands out an unbound store for a channel with no
	// Channel on this node, so a publish to it does not bind it. False
	// keeps every publish on GetChannel (--publish-bind-on-write).
	WriteOnlyPublish bool

	// PresenceSyncSource is where an attach's SYNC snapshot comes from:
	// PresenceSyncLocal (the empty default) or PresenceSyncStore
	// (DESIGN.md §12.4).
	PresenceSyncSource string

	// PresenceSyncRefresh bounds how often a busy channel's SYNC snapshot
	// is rebuilt. Zero means DefaultPresenceSyncRefresh.
	PresenceSyncRefresh time.Duration
}

// Manager owns the set of active Channels in this process. It pairs
// each Channel with its storage.ChannelStore at creation time — the
// Channel is passed to the storage as the Appender, so persisted cms
// can flow back into the live list — and, when an idle timeout is
// configured, evicts channels that have been idle for that long and
// releases their storage binding (DESIGN.md §5.1). Persistence and
// pub/sub mechanics live in storage.
type Manager struct {
	store       storage.Storage
	unbound     storage.UnboundPublisher // nil: the write-only path is off
	idleTimeout time.Duration
	sweepEvery  time.Duration
	metrics     *metrics.Metrics
	logger      *logging.Logger
	syncSource  string
	syncRefresh time.Duration

	// now reads the Manager's monotonic clock in nanoseconds. A field so
	// tests can drive eviction without sleeping.
	now func() int64
	// sleep, when set, replaces the wall-clock wait a SYNC does for the
	// refresh window (Channel.sleep), so tests can drive it with now.
	sleep func(ctx context.Context, d time.Duration) error

	shards [shardCount]shard
	bound  atomic.Int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// shard is one lock-protected slice of the channel map, padded to its
// own cache line.
type shard struct {
	mu       sync.Mutex
	channels map[string]*Channel
	_        [48]byte
}

// NewManager constructs a Manager backed by store, with eviction
// disabled.
func NewManager(store storage.Storage) *Manager {
	return NewManagerWithOptions(store, Options{})
}

// NewManagerWithOptions constructs a Manager backed by store. When
// opts.IdleTimeout is positive it starts the eviction sweeper; call
// Close to stop it.
func NewManagerWithOptions(store storage.Storage, opts Options) *Manager {
	epoch := time.Now()
	return newManager(store, opts, func() int64 { return int64(time.Since(epoch)) })
}

// newManager is NewManagerWithOptions with an injectable clock, so tests
// can age channels past the idle timeout without sleeping.
func newManager(store storage.Storage, opts Options, now func() int64) *Manager {
	m := &Manager{
		store:       store,
		idleTimeout: opts.IdleTimeout,
		sweepEvery:  opts.SweepInterval,
		metrics:     opts.Metrics,
		logger:      opts.Logger,
		syncSource:  opts.PresenceSyncSource,
		syncRefresh: opts.PresenceSyncRefresh,
		now:         now,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	if m.logger == nil {
		m.logger = logging.Default()
	}
	if up, ok := store.(storage.UnboundPublisher); ok && opts.WriteOnlyPublish {
		m.unbound = up
	}
	if m.syncSource == "" {
		m.syncSource = PresenceSyncLocal
	}
	if m.syncRefresh <= 0 {
		m.syncRefresh = DefaultPresenceSyncRefresh
	}
	for i := range m.shards {
		m.shards[i].channels = make(map[string]*Channel)
	}
	if m.idleTimeout <= 0 {
		close(m.done)
		return m
	}
	if m.sweepEvery <= 0 {
		m.sweepEvery = min(max(m.idleTimeout/10, 5*time.Millisecond), 5*time.Second)
	}
	go m.sweepLoop()
	return m
}

// Close stops the eviction sweeper and waits for it to exit. Channels
// stay bound; the storage is closed separately by its owner.
// Idempotent.
func (m *Manager) Close() {
	m.closeOnce.Do(func() { close(m.stop) })
	<-m.done
}

// BoundChannels returns the number of channels currently bound in this
// process (the ably_channels_bound gauge).
func (m *Manager) BoundChannels() int {
	return int(m.bound.Load())
}

// shardFor returns the shard owning name (FNV-1a, no allocation).
func (m *Manager) shardFor(name string) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(name); i++ {
		h ^= uint32(name[i])
		h *= 16777619
	}
	return &m.shards[h&(shardCount-1)]
}

// GetChannel returns the Channel for name, creating it if necessary
// and registering it with the storage backend as the Appender for
// that channel's stream of persisted cms. Concurrent calls for the
// same name observe the same instance.
//
// On first creation the storage backend calls Channel.Initialize
// with the channel's watermark serial before this returns, so the
// returned Channel is ready for Attach. A concurrent caller for a name
// that is still binding waits for the binding to finish; a caller for a
// name that is being evicted waits for the storage Release to finish
// and then binds afresh. The shard lock is released across the storage
// call to avoid holding it through any I/O.
//
// The returned Channel may be evicted once it is idle; its methods
// (Publish, Attach, History, …) then transparently rebind, so callers
// need not re-fetch it.
func (m *Manager) GetChannel(ctx context.Context, name string) (*Channel, error) {
	sh := m.shardFor(name)
	for {
		sh.mu.Lock()
		if ch, ok := sh.channels[name]; ok {
			sh.mu.Unlock()
			select {
			case <-ch.bound:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if ch.bindErr != nil {
				// The bind ran under the first caller's context. If that
				// caller went away, this caller's own bind attempt may
				// still succeed: retry rather than inherit its
				// cancellation (the failed entry is already gone).
				if isContextErr(ch.bindErr) && ctx.Err() == nil {
					continue
				}
				return nil, ch.bindErr
			}
			ch.life.Lock()
			evicted := ch.evicted
			ch.life.Unlock()
			if !evicted {
				return ch, nil
			}
			select {
			case <-ch.released:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		ch := newChannel(name)
		ch.mgr = m
		ch.syncSource, ch.syncRefresh, ch.metrics = m.syncSource, m.syncRefresh, m.metrics
		ch.lastUsed = m.now()
		sh.channels[name] = ch
		sh.mu.Unlock()

		store, err := m.store.Channel(ctx, name, ch)
		if err != nil {
			// Roll back: the half-constructed channel never finished init.
			// Drop it so a later GetChannel can retry from scratch.
			ch.bindErr = err
			sh.mu.Lock()
			if cur, ok := sh.channels[name]; ok && cur == ch {
				delete(sh.channels, name)
			}
			sh.mu.Unlock()
			close(ch.bound)
			return nil, err
		}
		ch.store = store
		ch.life.Lock()
		ch.lastUsed = m.now()
		ch.life.Unlock()
		close(ch.bound)
		m.bound.Add(1)
		m.metrics.ChannelBound()
		return ch, nil
	}
}

// WriteOnlyStore returns a store through which a message publish on name
// is stored without binding the channel (DESIGN.md §5.1), or nil when the
// publish should go through GetChannel: the write-only path is off or the
// storage has none, or this node has a Channel for name (bound, or still
// binding) that is not being evicted. A name with no Channel here has no
// attachment and no presence member on this node, so the publish has no
// local subscriber to reach; the storage still announces it to the other
// nodes, and if the channel is bound here while the publish is in flight
// the storage delivers it to that binding. Nothing is created, so there
// is nothing to evict afterwards.
func (m *Manager) WriteOnlyStore(name string) storage.ChannelStore {
	if m.unbound == nil {
		return nil
	}
	sh := m.shardFor(name)
	sh.mu.Lock()
	ch, ok := sh.channels[name]
	sh.mu.Unlock()
	if ok {
		ch.life.Lock()
		evicted := ch.evicted
		ch.life.Unlock()
		if !evicted {
			return nil
		}
	}
	m.metrics.UnboundPublish()
	return m.unbound.UnboundChannel(name)
}

// sweepLoop runs sweep on the configured cadence until Close.
func (m *Manager) sweepLoop() {
	defer close(m.done)
	t := time.NewTicker(m.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.sweep()
		}
	}
}

// sweep evicts every channel that has been idle for at least the idle
// timeout (DESIGN.md §5.1) and returns how many it evicted. For each
// shard it marks the idle channels evicted under the shard lock (from
// then on, pin redirects to a fresh binding and GetChannel waits), then
// releases their storage binding outside the lock, then removes them
// from the map and wakes any waiter.
func (m *Manager) sweep() int {
	if m.idleTimeout <= 0 {
		return 0
	}
	now := m.now()
	evicted := 0
	var victims []*Channel
	for i := range m.shards {
		select {
		case <-m.stop:
			return evicted // Close: stop between shards
		default:
		}
		sh := &m.shards[i]
		sh.mu.Lock()
		for _, ch := range sh.channels {
			ch.life.Lock()
			if ch.idle(now, m.idleTimeout) {
				ch.evicted = true
				victims = append(victims, ch)
			}
			ch.life.Unlock()
		}
		sh.mu.Unlock()
		for _, ch := range victims {
			m.release(sh, ch)
		}
		evicted += len(victims)
		clear(victims)
		victims = victims[:0]
	}
	return evicted
}

// release finishes evicting ch: storage Release, removal from the map,
// then the wake-up for any GetChannel waiting to rebind the name.
func (m *Manager) release(sh *shard, ch *Channel) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	err := m.store.Release(ctx, ch.name)
	cancel()
	if err != nil {
		m.logger.Warn("core: storage release failed", "channel", ch.name, "err", err)
		m.metrics.ChannelReleaseError()
	}
	sh.mu.Lock()
	if cur, ok := sh.channels[ch.name]; ok && cur == ch {
		delete(sh.channels, ch.name)
	}
	sh.mu.Unlock()
	close(ch.released)
	m.bound.Add(-1)
	m.metrics.ChannelEvicted()
}

// isContextErr reports whether err is a context cancellation or deadline.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
