// Package postgres is the database-backed storage backend used by
// ably-server's cluster mode (DESIGN.md §6.3, §7.2).
//
// State is held in a single `messages` table (one row per Message,
// grouped under a shared channelSerial PK), provisioned at Open() via
// an embedded migration sweep guarded by a session-scoped
// pg_advisory_lock — so N nodes booting simultaneously against an
// empty database serialise on the lock and only one applies the
// pending migrations.
//
// The publish path: ChannelStore.Store (and Mutate, StorePresence,
// StoreAnnotation) persists the cm in one transaction. Concurrent
// writers serialise per channel on the channels-row lock taken by
// advance_channel_serial, so each channel's serials are minted in
// commit order, cluster-wide.
//
// Cross-node delivery sits behind a Bus (bus.go, DESIGN.md §7.2),
// selected by Options.Bus:
//
//   - pgnotify (the default, pgnotify.go): the publish transaction
//     NOTIFYs the one global channel "ably_channel"; a LISTEN goroutine
//     on every node reads each cm back by (channel, serial) and delivers
//     it to the channel's appender.
//   - postgres (pgbus.go): per-channel LISTEN, cms inline in the
//     payload, one ordered worker per channel; the NOTIFY is either in
//     the transaction (transactional) or replaced by a per-channel
//     wake-up sent outside it once per window (coalesced, the default).
//   - nats (natsbus.go): after commit the publishing node sends the cm to
//     the channel's NATS subject; nodes subscribe per bound channel.
//
// The postgres and nats buses share the chained delivery point in
// chain.go: each cm carries its predecessor serial, the publishing node
// delivers its own cm straight after commit, and gaps are filled from
// the log.
//
// The log and the latest-version projection are partitioned by
// retention class and then by serial range; retention.go creates leaf
// partitions ahead of time and drops expired ones (DESIGN.md §6.3).
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockKey is the int8 used with pg_advisory_lock to serialise
// the migration sweep across all processes sharing this database. The
// value is arbitrary — pg_advisory_lock's keyspace is per-database and
// opt-in, so collisions with other applications would only matter if
// someone else's code also picked this exact key on the same DB.
const migrationLockKey int64 = 0x1ab1_e5e7_2e0a_17a3

// listenReconnectBaseDelay / listenReconnectMaxDelay bound the capped
// exponential backoff a LISTEN goroutine applies between re-dial
// attempts after its connection drops (DESIGN.md §7.2). They are
// package vars, not consts, so integration tests can shrink them; in
// production they are effectively constant. Open copies them into the
// Storage, so a test that restores them never races a reconnect still
// in flight.
var (
	listenReconnectBaseDelay = 200 * time.Millisecond
	listenReconnectMaxDelay  = 5 * time.Second
)

// Presence liveness lease + dead-node reaper timings (DESIGN.md
// §12.5). presenceLeaseWindow is how long a lease (a node's, or in
// member lease mode a member row's) is valid without a refresh;
// presenceLeaseBumpInterval is the cadence at which a live node renews
// it (comfortably shorter than the window so a live node's members
// never expire); and presenceReaperInterval is how often each node
// sweeps for lapsed leases. Making these operator-configurable is a
// follow-up: they are effectively constant in production, package vars
// only so integration tests can shrink them.
var (
	presenceLeaseWindow       = 30 * time.Second
	presenceLeaseBumpInterval = 10 * time.Second
	presenceReaperInterval    = 5 * time.Second
)

// Test hooks, nil in production. channelBindHook runs inside
// Storage.Channel at the named phase ("registered": the store is in the
// dispatch map and the bus subscription is in place, but the watermark
// is unread; "ensured": the watermark is read but the appender is not
// yet initialized). loadCMHook runs before a notified cm is read back
// and can fail the read.
var (
	channelBindHook atomic.Pointer[func(phase string)]
	loadCMHook      atomic.Pointer[func(channel, serial string) error]
	// commitBatchHook runs at the start of each batch commit attempt;
	// commitBatchAfterHook runs after a batch's COMMIT succeeded, as if
	// its reply were lost. Either can fail the attempt.
	commitBatchHook      atomic.Pointer[func() error]
	commitBatchAfterHook atomic.Pointer[func() error]
)

// fixtureNodeID is the sentinel owner recorded on static fixture presence
// rows (DESIGN.md §9, §12.5). It is not a real node id, so no live node's
// member-mode lease bump (WHERE node_id = $node) ever touches these rows,
// and the reapers of both lease modes leave rows with this owner and an
// 'infinity' lease alone.
const fixtureNodeID = "__fixtures__"

// DefaultPostgresSweepInterval is the postgres bus's default watermark
// sweep interval: the safety net that delivers a write whose coalesced
// wake-up was lost (DESIGN.md §7.2).
const DefaultPostgresSweepInterval = 30 * time.Second

// postgresSweepDefault and natsSweepDefault are the sweep intervals Open
// uses when Options.SweepInterval is zero. Package vars so integration
// tests can shrink them; in production they are the exported defaults.
var (
	postgresSweepDefault = DefaultPostgresSweepInterval
	natsSweepDefault     = DefaultNATSSweepInterval
)

// Options configures the Postgres backend.
type Options struct {
	// DSN is the libpq-style connection string (e.g.
	// "postgres://user:pw@host:5432/db?sslmode=disable"). Required.
	DSN string

	// Now is the clock used by the serial generator. Nil means
	// time.Now().UnixMilli — overridden by tests for determinism.
	Now func() int64

	// Logger receives operational events — the bus's reconnect and
	// reconcile lifecycle (DESIGN.md §7.2) and the presence reaper
	// (§12.5). Nil means logging.Default().
	Logger *logging.Logger

	// Bus selects the cross-node delivery mechanism (DESIGN.md §7.2):
	// BusPGNotify (the default; the shipped LISTEN/NOTIFY broker),
	// BusPostgres (per-channel LISTEN, transactional or coalesced) or
	// BusNATS (NATS core pub/sub). Empty means BusPGNotify. Postgres is
	// the store whichever bus is chosen.
	Bus string

	// NATSURL is the NATS server URL for BusNATS; a comma-separated list
	// of the servers of one NATS cluster is accepted. Required when Bus
	// is BusNATS.
	NATSURL string

	// NATSCredsFile, NATSTLSCA, NATSTLSCert and NATSTLSKey authenticate
	// and encrypt the NATS bus connection (DESIGN.md §7.2, §9): a NATS
	// credentials file (JWT and NKey seed, nats.UserCredentials), a PEM
	// CA bundle the server's certificate must chain to, and a client
	// certificate and its key for a server that verifies clients (both
	// or neither). Empty means unset. Credentials and TLS can also come
	// from the URL (nats://user:pass@host, tls://host).
	NATSCredsFile, NATSTLSCA, NATSTLSCert, NATSTLSKey string

	// NATSInlineMaxBytes caps the encoded size of a cm carried inline on
	// the NATS bus; a larger cm travels as a (channel, serial) pointer
	// that receivers fetch from the log. Zero means
	// DefaultNATSInlineMaxBytes.
	NATSInlineMaxBytes int

	// NotifyMode is the BusPostgres notify mode. Empty means
	// NotifyCoalesced.
	NotifyMode NotifyMode

	// NotifyWindow is the coalescing window in NotifyCoalesced mode: a
	// node sends at most one wake-up per channel per window. Zero means
	// DefaultNotifyWindow. It is the extra latency a remote subscriber
	// pays in that mode.
	NotifyWindow time.Duration

	// NotifyMaxPending caps the channels pending a wake-up on one node in
	// NotifyCoalesced mode (the overflow policy, pgbus_coalesced.go).
	// Zero means DefaultNotifyMaxPending.
	NotifyMaxPending int

	// SweepInterval is how often a chaining bus (BusPostgres, BusNATS)
	// compares every bound channel's delivery mark with its committed
	// serial and catches up a channel that has fallen behind. Zero means
	// the bus's default: DefaultPostgresSweepInterval or
	// DefaultNATSSweepInterval.
	SweepInterval time.Duration

	// SweepScope selects the bound channels the watermark sweep reads
	// (DESIGN.md §7.2): SweepSubscribed, only those whose appender reports
	// a subscriber on this node (storage.SubscriberReporter), or
	// SweepBound, every bound channel. Empty means SweepSubscribed.
	SweepScope string

	// Retention configures how long the message log keeps each class of
	// channel (DESIGN.md §6.3). The zero value applies the defaults.
	Retention Retention

	// Persisted reports whether a channel belongs to a persisted
	// namespace, and so keeps Retention.Persisted rather than the
	// continuity window. Nil means no channel is persisted.
	Persisted func(channel string) bool

	// PersistedNamespaces are the namespace ids Persisted reports as
	// persisted. Open records them, sorted, in the cluster identity
	// (DESIGN.md §11) and refuses a node that lists another set. It does
	// not change what Persisted reports.
	PersistedNamespaces []string

	// Batching configures leading-edge publish batching (DESIGN.md
	// §6.3). The zero value (Lanes 0) commits every publish in its own
	// transaction; the server enables 4 lanes by default.
	Batching Batching

	// PresenceMaxInflight bounds the presence writes this Storage runs
	// in their own transaction at once (those not batched, DESIGN.md
	// §12.5), so a convoy on one room's row lock cannot hold the whole
	// pool; a presence write beyond it fails at once with
	// storage.ErrOverloaded. Zero means DefaultPresenceInflightPerLane x
	// Batching.Lanes (x DefaultPublishLanes when batching is off);
	// negative means no bound.
	PresenceMaxInflight int

	// PresenceLeaseMode selects how presence liveness is leased
	// (DESIGN.md §12.5): PresenceLeaseNode, one lease row per node that
	// the node renews, or PresenceLeaseMember, a lease on every member
	// row that the node renews with one UPDATE of all its rows. Empty
	// means PresenceLeaseNode. Every node sharing a database should run
	// the same mode; each mode's reaper removes the other's rows of dead
	// nodes and leaves the other's live ones alone, so a rolling switch
	// is safe.
	PresenceLeaseMode string

	// OnPresenceLeaseLapse, when set, is called after this node finds its
	// presence lease had lapsed (DESIGN.md §12.5): another node may have
	// reaped its members and published their LEAVEs while it could not
	// renew, so the hook re-enters them (the server wires it to
	// realtime.Server.ReenterPresence). It runs on its own goroutine a
	// bump interval after the lapse, with a context Close cancels; lapses
	// reported meanwhile (a sharded node's other shards) join that one
	// call.
	OnPresenceLeaseLapse func(ctx context.Context)

	// lapse is the notifier OpenSharded shares between its shards, so one
	// outage seen by every shard re-enters the members once. Nil means
	// Open makes its own from OnPresenceLeaseLapse.
	lapse *lapseNotifier

	// nodeID overrides the per-process node id that owns this Storage's
	// presence rows (tests; OpenSharded gives every shard the same one).
	// Empty means a fresh random id.
	nodeID string

	// shard is this Storage's place in a shard list, set by OpenSharded
	// (DESIGN.md §6.4). The zero value is a lone Storage.
	shard shardSlot

	// catchUpSlots bounds the batched catch-up queries (reconcile and
	// sweep, chain.go) in flight on this node; OpenSharded shares one
	// across its shards. Nil means a fresh one of catchUpConcurrency.
	catchUpSlots chan struct{}

	// skipClusterIdentity skips the cluster identity check (DESIGN.md
	// §11). Only tests set it: those that open a pgnotify node beside a
	// nats or postgres one on purpose, to stand in for a bus message lost
	// after commit.
	skipClusterIdentity bool
}

// Pool timeouts applied when the DSN does not set its own (DESIGN.md
// §6.4): a connection attempt to an unreachable database gives up after
// poolConnectTimeout, and the liveness ping of an idle connection on
// acquire after poolPingTimeout, so a call on a channel whose shard is
// down fails within about that long instead of waiting on the dial.
// Package vars so tests can shrink them.
var (
	poolConnectTimeout = 5 * time.Second
	poolPingTimeout    = 2 * time.Second
)

// Storage is the pgx/pgxpool-backed storage.Storage.
type Storage struct {
	pool      *pgxpool.Pool
	dsn       string         // retained so a LISTEN goroutine can re-dial on drop
	series    string         // per-process seriesId, embedded in every minted channelSerial
	node      string         // per-process node id, owning presence rows for the liveness lease (§12.5)
	leaseNode bool           // PresenceLeaseNode: liveness is the node's presence_nodes row (§12.5)
	leaseRun  leaseRun       // this node's own lease renewals, for the reaper guard (§12.5)
	lmetrics  *leaseMetrics  // ably_presence_* liveness series
	lapse     *lapseNotifier // calls Options.OnPresenceLeaseLapse; nil when unset
	ownLapse  bool           // lapse is this Storage's to stop (not a shared shard notifier)
	namespace string         // current_schema(), mixed into every bus channel name (LISTEN names, NATS subjects)
	// deploymentID is the cluster's id from its cluster identity row
	// (DESIGN.md §11), mixed into the NATS subjects and carried in every
	// NATS envelope; "" only when a test skipped the check.
	deploymentID string
	logger       *logging.Logger

	retention Retention                 // resolved retention settings (§6.3)
	persisted func(channel string) bool // retention class of a channel
	metrics   *retentionMetrics
	lanes     *laneSet // publish batching; nil when off (§6.3)
	wmetrics  *writeMetrics
	// rows remembers channels known to have a row. ensureCalls and
	// rowReads count the binds that ran ensure_channel and those that read
	// a known row (ably_storage_channel_binds_total{source}).
	rows                  *rowCache
	ensureCalls, rowReads atomic.Uint64

	// presenceLanes is lanes when presence is batched too, else nil;
	// presenceSlots bounds unbatched presence writes (nil: unbounded).
	presenceLanes *laneSet
	presenceSlots chan struct{}
	// clockOffset is the database clock minus this node's, in ms, as of
	// the last sweep; RetainedSince applies it (§4.3).
	clockOffset atomic.Int64
	// clockSkew (ms) is added to clockOffset whenever it is measured, so
	// tests can move the retention floor the way MaintainPartitionsAt
	// moves the drop cutoff. Zero in production.
	clockSkew atomic.Int64
	// legacyBound is the upper bound of the live-class leaf a migrated
	// pre-partitioning log became (retention_state), or "" when there was
	// none. Persisted channels' rows from before it sit in the live class.
	legacyBound string

	shard      shardSlot     // place in the shard list (§6.4); {0, 1} when alone
	ident      shardIdentity // this shard's recorded identity; zero when alone
	busKind    string        // BusPGNotify, BusPostgres or BusNATS
	notifyMode string        // the BusPostgres notify mode, "" for the other buses

	reconnectBase, reconnectMax time.Duration // LISTEN re-dial backoff, copied at Open
	sweepInterval               time.Duration // chaining buses' watermark sweep
	sweepAll                    bool          // sweep every bound channel, not only subscribed ones
	timing                      chainTiming   // gap-fill tuning, snapshotted by Open
	catchUpSlots                chan struct{} // bounds batched catch-up queries in flight on the node (chain.go)
	reconcileJitter             time.Duration // cap on the wait before a reconnect reconcile, copied at Open

	mu       sync.RWMutex
	channels map[string]*channelStore // every store handed out, by Ably channel name

	bus         Bus // cross-node delivery (bus.go); set by Open
	stats       busStats
	reconcileCh chan struct{} // chaining buses: reconcile requests (chain.go)

	loopCtx   context.Context
	cancel    context.CancelFunc
	done      <-chan struct{} // closed when Close cancels the background goroutines
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Open dials Postgres at opts.DSN, applies any pending migrations
// (under a session-scoped advisory lock so concurrent Opens
// serialise), connects the cross-node bus, and returns a Storage ready
// for use. The seriesId is freshly generated per process — multi-node
// deployments rely on distinct per-node seriesIds to disambiguate
// concurrent mints (DESIGN.md §8).
func Open(ctx context.Context, opts Options) (*Storage, error) {
	if opts.DSN == "" {
		return nil, errors.New("storage/postgres: Open requires a DSN")
	}
	busKind, err := ParseBus(opts.Bus)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: %w", err)
	}
	var mode NotifyMode
	if busKind == BusPostgres {
		if mode, err = ParseNotifyMode(string(opts.NotifyMode)); err != nil {
			return nil, fmt.Errorf("storage/postgres: %w", err)
		}
	}
	sweepScope, err := ParseSweepScope(opts.SweepScope)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: %w", err)
	}
	leaseMode, err := ParsePresenceLeaseMode(opts.PresenceLeaseMode)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: %w", err)
	}
	node := opts.nodeID
	if node == "" {
		node = serial.NewSeriesID()
	}
	if busKind == BusNATS && opts.NATSURL == "" {
		return nil, errors.New("storage/postgres: the NATS bus requires a NATS URL")
	}
	poolCfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: parse DSN: %w", err)
	}
	batching := opts.Batching
	if batching.enabled() {
		batching = batching.resolve()
		// Each lane can hold maxInflightPerLane connections for its
		// batches; leave room for reads, presence and the sweep unless
		// the DSN sets pool_max_conns itself.
		if !strings.Contains(opts.DSN, "pool_max_conns") {
			poolCfg.MaxConns = max(poolCfg.MaxConns, int32(batching.Lanes*maxInflightPerLane+8))
		}
	}
	if poolCfg.ConnConfig.ConnectTimeout == 0 { // no connect_timeout in the DSN or PGCONNECT_TIMEOUT
		poolCfg.ConnConfig.ConnectTimeout = poolConnectTimeout
	}
	if poolCfg.PingTimeout == 0 {
		poolCfg.PingTimeout = poolPingTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: ping: %w", err)
	}
	var version int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: read server version: %w", err)
	}
	if version < minPostgresVersion {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: PostgreSQL %d is too old: the partitioned message log needs 14 or later", version)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: migrate: %w", err)
	}
	slot := opts.shard.resolve()
	ident, err := checkShardIdentity(ctx, pool, slot)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage/postgres: %w", err)
	}
	retention := opts.Retention.resolve()
	var deploymentID string
	if !opts.skipClusterIdentity {
		deploymentID, err = checkClusterIdentity(ctx, pool, slot, clusterSettings{
			bus:                 busKind,
			message:             retention.Message,
			persisted:           retention.Persisted,
			persistedNamespaces: normalizeNamespaces(opts.PersistedNamespaces),
		})
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("storage/postgres: %w", err)
		}
	}
	catchUpSlots := opts.catchUpSlots
	if catchUpSlots == nil {
		catchUpSlots = make(chan struct{}, catchUpConcurrency)
	}

	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	persisted := opts.Persisted
	if persisted == nil {
		persisted = func(string) bool { return false }
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		pool:            pool,
		dsn:             opts.DSN,
		series:          serial.NewSeriesID(),
		node:            node,
		leaseNode:       leaseMode == PresenceLeaseNode,
		logger:          logger,
		shard:           slot,
		ident:           ident,
		busKind:         busKind,
		notifyMode:      string(mode),
		reconnectBase:   listenReconnectBaseDelay,
		reconnectMax:    listenReconnectMaxDelay,
		sweepInterval:   opts.SweepInterval,
		sweepAll:        sweepScope == SweepBound,
		timing:          currentChainTiming(),
		reconcileJitter: reconcileJitterMax,
		retention:       retention,
		deploymentID:    deploymentID,
		catchUpSlots:    catchUpSlots,
		persisted:       persisted,
		metrics:         newRetentionMetrics(),
		wmetrics:        newWriteMetrics(),
		lmetrics:        newLeaseMetrics(),
		rows:            newRowCache(rowCacheSize),
		channels:        make(map[string]*channelStore),
		reconcileCh:     make(chan struct{}, 1),
		loopCtx:         loopCtx,
		cancel:          cancel,
		done:            loopCtx.Done(),
	}
	if opts.lapse != nil {
		s.lapse = opts.lapse
	} else {
		s.lapse, s.ownLapse = newLapseNotifier(opts.OnPresenceLeaseLapse, logger), true
	}
	fail := func(err error) (*Storage, error) {
		cancel()
		if s.ownLapse {
			s.lapse.stop()
		}
		pool.Close()
		return nil, err
	}

	// Create the leaf partitions publishes will land in before any
	// publish can run (DESIGN.md §6.3); the retention loop keeps them
	// ahead and drops expired ones. Open does not drop, so a slow drop
	// never delays startup.
	if err := s.preparePartitions(ctx); err != nil {
		return fail(fmt.Errorf("storage/postgres: prepare log partitions: %w", err))
	}

	leaseStart := time.Now()
	if s.leaseNode {
		// Take the node's lease before any presence write can run: the
		// node-mode reaper treats the members of a node without a lease
		// as orphans (DESIGN.md §12.5).
		if _, err := s.takeNodeLease(ctx); err != nil {
			return fail(fmt.Errorf("storage/postgres: take presence lease: %w", err))
		}
	}
	// The reaper guard counts this node's unbroken lease from here: it
	// reaps nothing for one lease window after Open (§12.5).
	s.leaseRun.begin(leaseStart)

	if busKind != BusPGNotify {
		// The schema this Storage works in namespaces its bus channels
		// (LISTEN names, NATS subjects): NOTIFY is per database, not per
		// schema, and a NATS cluster may be shared by several deployments.
		if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&s.namespace); err != nil {
			return fail(fmt.Errorf("storage/postgres: current_schema: %w", err))
		}
	}

	switch busKind {
	case BusNATS:
		if s.sweepInterval <= 0 {
			s.sweepInterval = natsSweepDefault
		}
		b, err := dialNATSBus(s, opts)
		if err != nil {
			return fail(err)
		}
		s.bus = b
	case BusPostgres:
		if s.sweepInterval <= 0 {
			s.sweepInterval = postgresSweepDefault
		}
		// Dedicated LISTEN connection. It LISTENs on nothing yet: each
		// channel is LISTENed when this node first binds it (Channel).
		conn, err := dialListenConn(ctx, opts.DSN)
		if err != nil {
			return fail(err)
		}
		s.bus = newPGBus(s, mode, conn, opts.NotifyWindow, opts.NotifyMaxPending)
	default:
		// Dedicated LISTEN connection. pgxpool doesn't expose the long-
		// lived single-conn semantics LISTEN needs, so we acquire a
		// separate raw conn for the broker goroutine. Dialing it here lets
		// Open fail fast on a bad DSN; the goroutine re-dials fresh conns
		// itself when this one drops.
		conn, err := dialAndListen(ctx, opts.DSN)
		if err != nil {
			return fail(err)
		}
		b := &pgNotifyBus{s: s, initialConn: conn}
		b.connected.Store(true)
		s.bus = b
	}

	if batching.enabled() {
		s.lanes = newLaneSet(batching, s, s.wmetrics)
		if !batching.PresenceUnbatched {
			s.presenceLanes = s.lanes
		}
	}
	if n := presenceMaxInflight(opts.PresenceMaxInflight, batching); n > 0 {
		s.presenceSlots = make(chan struct{}, n)
	}

	s.wg.Add(3)
	go s.presenceLeaseBumpLoop(loopCtx)
	go s.presenceReaperLoop(loopCtx)
	go s.retentionLoop(loopCtx)
	s.bus.start(loopCtx)
	return s, nil
}

// Channel returns the ChannelStore for name, binding it to appender on
// first access. Subsequent calls with the same name return the same
// instance and ignore the new appender (until Release drops the
// binding).
//
// Binding with a non-nil appender first puts the bus subscription in
// place (Bus.bind: a no-op for pgnotify, whose one LISTEN covers every
// channel; a LISTEN or a NATS subscription otherwise), then reads the
// channels row (channelRow: a plain read of a row this node knows
// exists, else ensure_channel, which creates it with a fresh seed serial
// if absent) and hands the resulting channelSerial — the watermark — to
// appender.Initialize before this call returns. Because the
// subscription is in effect before the read, a cm committed after it
// always reaches this node, and one committed before it sorts at or
// below the watermark, which seeds the delivery point's de-dup mark.
func (s *Storage) Channel(ctx context.Context, name string, appender storage.Appender) (storage.ChannelStore, error) {
	s.mu.Lock()
	if cs, ok := s.channels[name]; ok {
		s.mu.Unlock()
		return cs, nil
	}
	cs := s.newChannelStore(name, appender)
	s.channels[name] = cs
	s.mu.Unlock()

	if appender == nil {
		return cs, nil
	}

	// fail unwinds a bind that never finished: drop the store so a retry
	// can start again, and release anything waiting on it.
	fail := func(err error) (storage.ChannelStore, error) {
		s.mu.Lock()
		if s.channels[name] == cs {
			delete(s.channels, name)
		}
		s.mu.Unlock()
		cs.release()
		s.bus.unbind(cs)
		return nil, err
	}

	if err := s.bus.bind(ctx, cs); err != nil {
		return fail(unavailable(err))
	}
	if hook := channelBindHook.Load(); hook != nil {
		(*hook)("registered")
	}
	// The watermark read's snapshot is taken after now, so nothing past
	// the watermark had committed at now (DESIGN.md §7.2).
	cs.hwmMu.Lock()
	cs.markProvenLocked(s.clockSerial(time.Now()))
	cs.hwmMu.Unlock()
	current, initial, err := s.channelRow(ctx, name)
	if err != nil {
		return fail(unavailable(err))
	}
	if hook := channelBindHook.Load(); hook != nil {
		(*hook)("ensured")
	}
	// Initialize the appender and let deliveries through. pgnotify also
	// runs here a reconcile a LISTEN reconnect requested during the bind
	// (pgnotify.go initialize); the chaining buses seed their delivery
	// point (chain.go seedChain).
	if s.bus.chains() {
		cs.seedChain(current, initial)
	} else if err := cs.initialize(ctx, current, initial); err != nil {
		s.logger.Warn("storage/postgres: reconcile during bind failed", "channel", name, "err", err)
	}
	if cs.isReleased() {
		// Release ran while this bind was in flight and may have missed
		// the subscription bind put in place; drop it.
		s.bus.unbind(cs)
	}
	return cs, nil
}

// channelRow returns the channel's current and initial serial for a
// bind, creating its row if absent. A row this node knows exists
// (rowCache) is read with a plain SELECT: no row version is written and
// no row lock is waited for, which matters for a channel another
// transaction is publishing on. Otherwise ensure_channel creates or
// touches the row. Either read happens after the bus subscription is in
// place, and a publish that commits after it is announced on the bus, so
// the bind misses nothing whichever read it uses (DESIGN.md §7.2).
func (s *Storage) channelRow(ctx context.Context, name string) (current, initial string, err error) {
	if s.rows != nil && s.rows.has(name) {
		err := s.pool.QueryRow(ctx, `SELECT channel_serial, initial_channel_serial FROM channels WHERE name = $1`, name).Scan(&current, &initial)
		if err == nil {
			s.rowReads.Add(1)
			return current, initial, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", "", fmt.Errorf("storage/postgres: read channel row: %w", err)
		}
		// Not there after all (a test database reset under a live node):
		// create it.
	}
	if err := s.pool.QueryRow(ctx, `SELECT current_serial, initial_serial FROM ensure_channel($1, $2)`, name, s.series).Scan(&current, &initial); err != nil {
		return "", "", fmt.Errorf("storage/postgres: ensure_channel: %w", err)
	}
	s.ensureCalls.Add(1)
	s.rememberRow(name)
	return current, initial, nil
}

// rememberRow records that name has a channels row.
func (s *Storage) rememberRow(name string) {
	if s.rows != nil {
		s.rows.add(name)
	}
}

// UnboundChannel returns a store for publishing on name without binding
// it (storage.UnboundPublisher, DESIGN.md §6.3): no appender, no bus
// subscription, no entry in the channel map, so nothing for the sweep to
// read or for eviction to release. Its publishes go through the same
// lanes and bus hooks as a bound store's: the bus announces each one to
// the other nodes, and the publisher fast path delivers it to this
// node's binding of the channel if one exists by then. A publish on a
// channel with no row yet creates the row inside its batch transaction
// (publish_batch_lock), so a cold channel costs no extra round trip.
func (s *Storage) UnboundChannel(name string) storage.ChannelStore {
	return s.newChannelStore(name, nil)
}

// Channel and Release for the same name are not meant to run
// concurrently (the caller, core.Manager, serialises them per channel).
// If they do, the outcome is as if Release ran after Channel: the store
// Channel returns is released and delivers nothing, and the next Channel
// call binds a new one.

// Release drops this node's binding of the channel
// (storage.Storage.Release, DESIGN.md §7.2): no further cm reaches the
// appender, the bus subscription is removed (UNLISTEN, or a NATS
// unsubscribe), and the channelStore is forgotten. A later Channel call
// binds afresh; its watermark read seeds the new delivery point, so it
// sees every cm committed while the channel was released that sorts
// after the new watermark, and none twice.
func (s *Storage) Release(_ context.Context, name string) error {
	s.mu.Lock()
	cs, ok := s.channels[name]
	if ok {
		delete(s.channels, name)
	}
	s.mu.Unlock()
	if !ok || cs.appender == nil {
		return nil
	}
	cs.release()
	s.bus.unbind(cs)
	return nil
}

// newChannelStore constructs a channelStore bound to appender (nil for a
// storage-only or transient store).
func (s *Storage) newChannelStore(name string, appender storage.Appender) *channelStore {
	cs := &channelStore{
		pool:      s.pool,
		series:    s.series,
		node:      s.node,
		leaseNode: s.leaseNode,
		name:      name,
		appender:  appender,
		bus:       s.bus,
		logger:    s.logger,
		done:      s.done,
		ctx:       s.loopCtx,
		timing:    s.timing,
		stats:     &s.stats,
	}
	cs.rows = s.rows
	if s.busKind == BusPostgres {
		cs.pgChan = pgChannelName(s.namespace, name)
	}
	if appender != nil {
		cs.ready = make(chan struct{})
	}
	s.setRetention(cs)
	cs.presenceLanes, cs.presenceSlots, cs.wmetrics = s.presenceLanes, s.presenceSlots, s.wmetrics
	return cs
}

// DefaultPresenceInflightPerLane is the default bound on unbatched
// presence writes per publish lane (Options.PresenceMaxInflight).
const DefaultPresenceInflightPerLane = 4

// presenceMaxInflight resolves Options.PresenceMaxInflight: the bound
// on unbatched presence writes, or 0 for none.
func presenceMaxInflight(n int, b Batching) int {
	switch {
	case n < 0:
		return 0
	case n > 0:
		return n
	case b.enabled():
		return DefaultPresenceInflightPerLane * b.Lanes
	default:
		return DefaultPresenceInflightPerLane * DefaultPublishLanes
	}
}

// boundStore returns the channelStore bound to name with a live
// appender, or nil if this node holds no such channel.
func (s *Storage) boundStore(name string) *channelStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cs, ok := s.channels[name]; ok && cs.appender != nil {
		return cs
	}
	return nil
}

// boundStores snapshots every channelStore bound with a live appender.
func (s *Storage) boundStores() []*channelStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stores := make([]*channelStore, 0, len(s.channels))
	for _, cs := range s.channels {
		if cs.appender != nil {
			stores = append(stores, cs)
		}
	}
	return stores
}

// Close stops the background goroutines (the bus and the presence
// lease-bump and reaper loops), in node lease mode deletes the node's
// presence lease row (DESIGN.md §12.5), closes the bus and releases the
// pool. A LISTEN goroutine owns closing its own conn, so Close only
// cancels and waits.
func (s *Storage) Close() error { return s.close(true) }

// close is Close; graceful false skips the lease release, leaving the
// node's lease to expire as a crashed node's would (tests).
func (s *Storage) close(graceful bool) error {
	s.closeOnce.Do(func() {
		if s.ownLapse {
			s.lapse.stop() // no re-entry runs against a closing store
		}
		if s.lanes != nil {
			s.lanes.close() // finish in-flight batches before the bus stops
		}
		s.cancel()
		s.stopGapTimers()
		s.wg.Wait()
		if graceful && s.leaseNode {
			s.releaseNodeLease()
		}
		s.bus.close()
		s.pool.Close()
	})
	return nil
}

// stopGapTimers stops every bound channel's pending gap-fill timer, so no
// fill starts against the pool once Close is releasing it. A fill already
// reading the log runs on the cancelled loop context and ends at once.
func (s *Storage) stopGapTimers() {
	s.mu.Lock()
	stores := make([]*channelStore, 0, len(s.channels))
	for _, cs := range s.channels {
		stores = append(stores, cs)
	}
	s.mu.Unlock()
	for _, cs := range stores {
		cs.hwmMu.Lock()
		cs.stopGapFillLocked()
		cs.hwmMu.Unlock()
	}
}

// Collectors returns the backend's Prometheus collectors (the
// ably_storage_* retention series and the ably_publish_* batching series,
// DESIGN.md §10), for registration on the process registry. A lone
// Storage also reports ably_storage_shards (1); a shard of a list leaves
// that to Sharded.
func (s *Storage) Collectors() []prometheus.Collector {
	out := append(s.metrics.collectors(), s.wmetrics.collectors()...)
	out = append(out, s.lmetrics.collectors()...)
	for _, src := range []struct {
		label string
		n     *atomic.Uint64
	}{{"ensure", &s.ensureCalls}, {"read", &s.rowReads}} {
		out = append(out, prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "ably_storage_channel_binds_total",
			Help:        "Channel binds on this node by how the channels row was read: ensure (ensure_channel, which writes) or read (a plain read of a row the node knows exists).",
			ConstLabels: prometheus.Labels{"source": src.label},
		}, func() float64 { return float64(src.n.Load()) }))
	}
	if s.shard.count == 1 {
		out = append(out, shardsGauge(1))
	}
	return out
}

// Ping reports whether the node can serve cluster traffic: the Postgres
// pool is reachable, the bus is ready (the nats bus is not ready while
// disconnected from NATS, the postgres bus while its LISTEN connection
// is down), and every publish lane is completing its batches
// (laneSet.health). It satisfies storage.Pinger, backing the /readyz
// check in cluster mode (DESIGN.md §2.2, §7.2, §11).
func (s *Storage) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return err
	}
	if err := s.bus.ready(); err != nil {
		return err
	}
	if s.lanes != nil {
		return s.lanes.health(time.Now())
	}
	return nil
}

// unavailable wraps err in storage.ErrUnavailable when it says the
// database could not be reached: a failed connection attempt (bounded by
// poolConnectTimeout), a deadline that passed, or an error pgconn reports as safe to
// retry because nothing reached the server. Callers map ErrUnavailable
// to 50003 (DESIGN.md §6.4), which is what a channel on a shard that is
// down answers. Other errors are returned as they are.
func unavailable(err error) error {
	if err == nil || errors.Is(err, storage.ErrUnavailable) {
		return err
	}
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) || pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded) || pgconn.SafeToRetry(err) {
		return fmt.Errorf("%w: %w", storage.ErrUnavailable, err)
	}
	return err
}

// sleepCtx waits for d or until ctx is done, reporting whether the full
// wait elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// loadChannelMessage fetches the canonical ChannelMessage at
// (channel, channelSerial) via the pool. Used by the LISTEN loop
// after each NOTIFY.
func (s *Storage) loadChannelMessage(ctx context.Context, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	if hook := loadCMHook.Load(); hook != nil {
		if err := (*hook)(channel, channelSerial); err != nil {
			return nil, err
		}
	}
	return loadChannelMessagePool(ctx, s.pool, channel, channelSerial)
}

// loadChannelMessagePool is loadChannelMessage against an explicit
// pool: the fetch-by-serial path a NATS bus pointer resolves through.
func loadChannelMessagePool(ctx context.Context, pool *pgxpool.Pool, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := pool.Query(ctx, sqlLoadCM, channel, channelSerial)
	return decodeChannelMessageRows(rows, err, channel, channelSerial)
}

const sqlLoadCM = `
SELECT idx, kind, payload, summary FROM channel_messages
WHERE channel = $1 AND channel_serial = $2
ORDER BY idx
`

// decodeChannelMessageRows materialises a ChannelMessage from a rows
// result of (idx, kind, payload, summary). All rows of a cm share a kind; a
// presence cm decodes into Presence, a message cm into Messages, an
// annotation cm into Annotations — the latter with its post-fold summary
// snapshot reconstructed from the summary column so the delivery path has it
// (DESIGN.md §14.2). Closes rows on exit.
func decodeChannelMessageRows(rows pgx.Rows, queryErr error, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	if queryErr != nil {
		return nil, fmt.Errorf("storage/postgres: load %s:%s: %w", channel, channelSerial, queryErr)
	}
	defer rows.Close()

	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial}
	for rows.Next() {
		var (
			idx     int
			kind    string
			payload []byte
			summary []byte
		)
		if err := rows.Scan(&idx, &kind, &payload, &summary); err != nil {
			return nil, fmt.Errorf("storage/postgres: scan %s:%s: %w", channel, channelSerial, err)
		}
		if err := decodeRowInto(cm, channel, idx, kind, payload, summary); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: rows %s:%s: %w", channel, channelSerial, err)
	}
	if len(cm.Messages) == 0 && len(cm.Presence) == 0 && len(cm.Annotations) == 0 {
		return nil, fmt.Errorf("storage/postgres: ChannelMessage not found: %s:%s", channel, channelSerial)
	}
	return cm, nil
}

// decodeRowInto decodes one channel_messages row of (idx, kind,
// payload, summary) and appends the item to cm: a presence row into
// Presence, an annotation row into Annotations with its post-fold
// summary snapshot reconstructed from the summary column (DESIGN.md
// §14.2), a message row into Messages.
func decodeRowInto(cm *protocol.ChannelMessage, channel string, idx int, kind string, payload, summary []byte) error {
	channelSerial := cm.ChannelSerial
	switch kind {
	case string(storage.KindPresence):
		var p protocol.PresenceMessage
		if err := msgpack.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("storage/postgres: decode presence payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
		}
		cm.Presence = append(cm.Presence, &p)
	case string(storage.KindAnnotation):
		var a protocol.Annotation
		if err := msgpack.Unmarshal(payload, &a); err != nil {
			return fmt.Errorf("storage/postgres: decode annotation payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
		}
		if len(summary) > 0 {
			if err := msgpack.Unmarshal(summary, &a.Summary); err != nil {
				return fmt.Errorf("storage/postgres: decode annotation summary %s:%s idx=%d: %w", channel, channelSerial, idx, err)
			}
		}
		cm.Annotations = append(cm.Annotations, &a)
	default:
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return fmt.Errorf("storage/postgres: decode payload %s:%s idx=%d: %w", channel, channelSerial, idx, err)
		}
		cm.Messages = append(cm.Messages, &m)
	}
	return nil
}

// migrate applies any pending embedded migrations under a session-
// scoped pg_advisory_lock. Other processes calling Open against the
// same database block on the lock acquire, then observe an
// up-to-date schema_migrations table and apply nothing.
//
// Migrations live in the embedded migrations/ tree and are applied
// in lex order of filename (the convention is "<4-digit>_<name>.sql",
// e.g. 0001_initial.sql). Each migration runs in its own transaction
// alongside the schema_migrations INSERT, so a crash mid-sweep
// leaves the DB consistent (either fully applied or not), and the
// next Open picks up where the previous one left off.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	// Session-scoped lock: held until we explicitly release it (or
	// the conn returns to the pool, since pgxpool resets the
	// session). We unlock explicitly for symmetry / defensiveness.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		// Best-effort release; the conn's session-end would do it too.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
		  version    TEXT        PRIMARY KEY,
		  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := make(map[string]struct{})
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied version: %w", err)
		}
		applied[v] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	pending, err := listMigrations()
	if err != nil {
		return err
	}
	for _, m := range pending {
		if _, ok := applied[m.version]; ok {
			continue
		}
		if err := applyMigrationWithRetry(ctx, conn, m); err != nil {
			return fmt.Errorf("apply %s: %w", m.version, err)
		}
	}
	return nil
}

type migration struct {
	version string // filename minus ".sql", e.g. "0001_initial"
	sql     string
}

// listMigrations reads the embedded migrations/ tree and returns the
// migrations in lex order (which by convention matches numeric order
// of the leading digit prefix).
func listMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]migration, 0, len(names))
	for _, name := range names {
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		out = append(out, migration{
			version: strings.TrimSuffix(name, ".sql"),
			sql:     string(body),
		})
	}
	return out, nil
}

// Migration lock handling (DESIGN.md §6.3, §11). Every migration runs in
// one transaction under lock_timeout = migrationLockTimeout, so a lock
// held by a node still serving traffic makes the attempt fail with 55P03
// after that long instead of waiting indefinitely (and, behind it, queuing
// every other statement on the table). A failed attempt rolls back whole;
// applyMigrationWithRetry makes up to migrationAttempts attempts,
// migrationRetryDelay apart, so Open fails after about
// attempts x (lock timeout + delay) = 5 x (5 s + 250 ms), roughly 26 s,
// when the lock never frees. The vars exist so tests can shorten them.
const migrationAttempts = 5

var (
	migrationLockTimeout = 5 * time.Second
	migrationRetryDelay  = 250 * time.Millisecond
)

// migrationLockTimeouts counts attempts that lost a lock wait (55P03),
// for tests.
var migrationLockTimeouts atomic.Int64

// applyMigrationWithRetry retries a migration that failed on a deadlock
// or a lock timeout: a migration that restructures tables in use by
// nodes still running the old version can lose the deadlock detector's
// choice or wait out the lock timeout, and the whole transaction rolls
// back, so a retry is safe.
func applyMigrationWithRetry(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	var err error
	for i := range migrationAttempts {
		if err = applyMigration(ctx, conn, m); err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "40P01" && pgErr.Code != "55P03") {
			return err
		}
		if pgErr.Code == "55P03" {
			migrationLockTimeouts.Add(1)
		}
		if i < migrationAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(migrationRetryDelay):
			}
		}
	}
	return err
}

// applyMigration runs a single migration's SQL and records the
// schema_migrations row in the same transaction.
func applyMigration(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// SET LOCAL does not take a bind parameter; the value is a constant
	// duration rendered as milliseconds.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = %d`, migrationLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set lock_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("exec migration sql: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`,
		m.version,
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// channelStore is the per-channel facet. Concurrency is controlled by
// the channels-row UPDATE inside advance_channel_serial: every Store
// call serialises against other writers to the same channel via that
// row's lock.
type channelStore struct {
	pool     *pgxpool.Pool
	series   string
	node     string
	name     string
	appender storage.Appender
	// leaseNode is the owning Storage's leaseNode: member rows get an
	// 'infinity' lease, their node's lease being the one that counts.
	leaseNode bool
	bus       Bus
	logger    *logging.Logger
	done      <-chan struct{} // the owning Storage's shutdown signal
	ctx       context.Context // the owning Storage's loop context (nil in unit-test stubs)
	timing    chainTiming
	stats     *busStats // the owning Storage's bus counters (nil in unit tests)
	pgChan    string    // the postgres bus's notification channel (pgChannelName)

	// persisted selects the channel's retention class: the value of the
	// persisted partition key on every row it writes (DESIGN.md §6.3).
	// retention is that class's retention; clock is the storage's
	// database clock offset; floorMin raises a persisted channel's
	// retention floor over rows a migration left in the live class. All
	// feed RetainedSince and the idempotency window.
	persisted bool
	// lanes is the storage's publish batcher, nil when batching is off
	// (DESIGN.md §6.3); a batched publish on a channel with no row yet
	// creates it inside its batch (publish_batch_lock). rows is the
	// storage's known-row cache.
	lanes     *laneSet
	rows      *rowCache
	retention time.Duration
	clock     *atomic.Int64
	floorMin  string

	// presenceLanes is lanes when presence operations are batched too
	// (nil otherwise); presenceSlots bounds the presence operations this
	// Storage runs in their own transaction at once (nil: no bound). Both
	// DESIGN.md §12.5.
	presenceLanes *laneSet
	presenceSlots chan struct{}
	wmetrics      *writeMetrics

	// hwmMu guards lastSeen, the highest channel_serial delivered to
	// appender (seeded with the bind-time watermark). It is the
	// per-channel de-dup high-water mark that makes the publisher fast
	// path, gap fills and reconciles idempotent against bus delivery
	// (DESIGN.md §7.2). On the pgnotify bus the LISTEN goroutine writes
	// it via deliver (pgnotify.go); on the chaining buses every writer
	// goes through deliverChained (chain.go). Both hold it across
	// appender.Append, so appends on one channel stay in serial order and
	// Release cannot return while an append to the old appender is in
	// flight. It also guards the fields below.
	hwmMu    sync.Mutex
	lastSeen string
	// provenAt is the serial prefix of the latest database time at which
	// the channel was known to have nothing committed past lastSeen: the
	// bind's watermark read, or a bus sweep that found the channel caught
	// up. A log read from the mark needs no continuity proof while it is
	// inside the retention window (chain.go mustProveLocked).
	provenAt string

	// released is set by Storage.Release (or a failed bind): nothing is
	// delivered to the appender after it. ready is closed once the bind
	// has seeded (or failed); a postgres-bus worker waits on it.
	released    bool
	ready       chan struct{}
	readyClosed bool
	// pgnotify only (pgnotify.go): deliveries received before the bind
	// seeded, a reconcile a LISTEN reconnect requested meanwhile, a
	// failed read-back that must be repaired from the mark (dirty), and a
	// log-read stub for unit tests.
	preSeed        []*protocol.ChannelMessage
	needsReconcile bool
	dirty          bool
	loadAfterFn    func(ctx context.Context, after string) ([]*protocol.ChannelMessage, error)

	// Chained-delivery state, used only by a bus whose chains() is true
	// (DESIGN.md §7.2). seeded is set once Storage.Channel has read the
	// channel's watermark into lastSeen; pending holds cms that arrived
	// ahead of their predecessor, keyed by predecessor serial; gapTimer
	// fires the log read that fills a gap the bus did not.
	seeded         bool
	pending        map[string]busEvent
	gapTimer       *time.Timer
	filling        bool // a gap fill is reading the log
	gapBackoff     time.Duration
	overflowArmed  bool   // a forced fill is armed after a hold overflow
	sweptWatermark string // the channel's watermark at the previous sweep

	// Delivery counters for tests (guarded by hwmMu).
	delivered, duplicates, held, gapFills, sweepCatchUps, holdOverflows int

	// Bus-specific state: the nats bus's subscription (guarded by subMu)
	// and the postgres bus's delivery queue.
	subMu sync.Mutex
	sub   *nats.Subscription
	q     busQueue
}

// commitWrite finishes a publish transaction: the bus's in-transaction
// hook (the pgnotify and transactional postgres buses NOTIFY here, so
// listeners see the cm only if it commits), COMMIT, then the bus's
// post-commit hook (the publisher fast path, the NATS publish, the
// coalesced wake-up mark). w carries the cm's serial, predecessor and
// stored rows.
func (cs *channelStore) commitWrite(ctx context.Context, tx pgx.Tx, cm *protocol.ChannelMessage, w *busWrite) error {
	if err := cs.bus.beforeCommit(ctx, tx, cs, w); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage/postgres: commit: %w", err)
	}
	if w.notified {
		cs.st().published.Add(1)
	}
	if w.pointer {
		cs.st().pointers.Add(1)
	}
	cs.bus.afterCommit(cs, cm, w.prev)
	return nil
}

// advanceSerial mints the publish's channelSerial inside tx via
// advance_channel_serial, whose channels-row lock serialises writers to
// the channel cluster-wide. For a bus that chains deliveries it also
// returns the serial it replaced: the row is locked and read first, in
// the same round trip, so prev is exactly the cm before this one ("" if
// the channel had no row yet).
func (cs *channelStore) advanceSerial(ctx context.Context, tx pgx.Tx) (channelSerial, prev string, err error) {
	if !cs.bus.chains() {
		if err := tx.QueryRow(ctx,
			`SELECT advance_channel_serial($1, $2)`,
			cs.name, cs.series,
		).Scan(&channelSerial); err != nil {
			return "", "", fmt.Errorf("storage/postgres: advance channel serial: %w", err)
		}
		return channelSerial, "", nil
	}

	batch := &pgx.Batch{}
	batch.Queue(`SELECT channel_serial FROM channels WHERE name = $1 FOR UPDATE`, cs.name)
	batch.Queue(`SELECT advance_channel_serial($1, $2)`, cs.name, cs.series)
	br := tx.SendBatch(ctx, batch)
	if err := br.QueryRow().Scan(&prev); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		_ = br.Close()
		return "", "", fmt.Errorf("storage/postgres: lock channel row: %w", err)
	}
	if err := br.QueryRow().Scan(&channelSerial); err != nil {
		_ = br.Close()
		return "", "", fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	if err := br.Close(); err != nil {
		return "", "", fmt.Errorf("storage/postgres: advance channel serial: %w", err)
	}
	return channelSerial, prev, nil
}

// Store persists one publish atomically. With publish lanes (the server's
// default, Options.Batching), it queues the publish on the channel's lane
// and a batch commits it with the others queued (storeBatched,
// committer.go, DESIGN.md §6.3). Without lanes it runs the single-publish
// transaction below: advance the channel's serial via
// advance_channel_serial (which takes a row-level lock on the channels row
// and is the per-channel write serialiser), then look up any contained
// Message.IDs for prior matches (idempotent return on a hit, rolling the
// advance back), otherwise stamp each Message.Serial, insert one row per
// Message and run the bus's in-transaction hook. Either way the commit is
// announced through the Bus (bus.go, DESIGN.md §7.2): the publishing node
// delivers its own cm to the channel's appender straight after commit, and
// the other nodes receive it over the bus (a NOTIFY and the LISTEN
// round-trip on pgnotify, inline over the per-channel postgres or nats
// subscription otherwise).
func (cs *channelStore) Store(ctx context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if len(msgs) == 0 {
		return nil, false, errors.New("storage/postgres: Store with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	if cs.lanes != nil {
		return cs.storeBatched(ctx, cs.lanes, msgs)
	}

	// Resolve the batch id and stamp each Message.ID = "<batchID>:<idx>"
	// (DESIGN.md §8) before the tx, so the partial UNIQUE id index keys on
	// the batch-derived ids.
	batchID, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", unavailable(err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Take the channels-row lock first by advancing the serial, THEN
	// look for a prior publish of any contained id. The order matters:
	// a lookup before the lock let two concurrent publishes with the
	// same id both miss it (the claims audit's idempotency race). With
	// the row lock held, a concurrent publish of the same id has either
	// committed, and this statement's fresh READ COMMITTED snapshot sees
	// it, or is queued behind us. On a hit the deferred Rollback undoes
	// the advance, so a duplicate does not burn a serial.
	channelSerial, prev, err := cs.advanceSerial(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	if original, err := cs.findIdempotent(ctx, tx, nonEmptyIDs(msgs), channelSerial); err != nil || original != nil {
		return original, original != nil, err
	}

	// Fresh publish: stamp Message.Serials, persist.
	for i, m := range msgs {
		m.Serial = serial.MessageSerial(channelSerial, i)
		m.Action = protocol.MessageCreate
		storage.StampCreateVersion(m)
	}
	cm := &protocol.ChannelMessage{ID: batchID, ChannelSerial: channelSerial, Messages: msgs}

	rows := make([][]byte, 0, len(msgs))
	for i, m := range msgs {
		payload, err := msgpack.Marshal(m)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode message %d: %w", i, err)
		}
		rows = append(rows, payload)
		// id is stored as NULL when empty so the partial UNIQUE
		// idempotency index never matches a no-id publish. message_serial
		// is the message identity (its own serial for a create) — the
		// versions index and the projection key off it (DESIGN.md §13.4).
		var idArg any
		if m.ID != "" {
			idArg = m.ID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial, persisted)
			 VALUES ($1, $2, $3, $4, 'message', $5, $6, $7)`,
			cs.name, channelSerial, i, idArg, payload, m.Serial, cs.persisted,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert message %d: %w", i, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO messages (channel, message_serial, payload, deleted, persisted)
			 VALUES ($1, $2, $3, FALSE, $4)`,
			cs.name, m.Serial, payload, cs.persisted,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert projection %d: %w", i, err)
		}
	}

	if err := cs.commitWrite(ctx, tx, cm, &busWrite{serial: channelSerial, prev: prev, kind: storage.KindMessage, rows: rows}); err != nil {
		return nil, false, err
	}
	if cs.rows != nil {
		cs.rows.add(cs.name) // advance_channel_serial made or found the row
	}
	return cm, false, nil
}

// Mutate applies an update/delete/append to an existing message
// (DESIGN.md §13.2) in one transaction: resolve the target's current
// latest version from the projection (ErrTargetNotFound if absent),
// apply the shallow-mixin merge, mint a fresh version serial, insert the
// merged version row on channel_messages (message_serial = identity),
// upsert the projection (deleted = TRUE for a delete), and announce the
// cm on the bus. It reaches every node's appender the way Store's does.
func (cs *channelStore) Mutate(ctx context.Context, mut *protocol.Message) (*protocol.ChannelMessage, bool, error) {
	if mut == nil || !mut.Action.IsMutation() {
		return nil, false, errors.New("storage/postgres: Mutate requires a mutation action")
	}
	if mut.Serial == "" {
		return nil, false, errors.New("storage/postgres: Mutate requires a target serial")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", unavailable(err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the channels row (advance the serial) before the idempotency
	// lookup, for the same reason as Store; an early return rolls the
	// advance back.
	var mutIDs []string
	if mut.ID != "" {
		mutIDs = []string{mut.ID}
	}
	channelSerial, prev, err := cs.advanceSerial(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	if original, err := cs.findIdempotent(ctx, tx, mutIDs, channelSerial); err != nil || original != nil {
		return original, original != nil, err
	}

	// Resolve the target's current latest version from the projection.
	var curPayload []byte
	switch err := tx.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, mut.Serial).Scan(&curPayload); {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, storage.ErrTargetNotFound
	case err != nil:
		return nil, false, fmt.Errorf("storage/postgres: load target version: %w", err)
	}
	var current protocol.Message
	if err := msgpack.Unmarshal(curPayload, &current); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: decode target version: %w", err)
	}

	version, err := storage.MergeVersion(&current, mut, serial.MessageSerial(channelSerial, 0))
	if err != nil {
		return nil, false, err
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Messages: []*protocol.Message{version}}

	payload, err := msgpack.Marshal(version)
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: encode version: %w", err)
	}
	var idArg any
	if mut.ID != "" {
		idArg = mut.ID
	}
	// is_append marks appends so the versions read collapses their runs
	// to the aggregate (DESIGN.md §13.3); the log row itself is retained.
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial, is_append, persisted)
		 VALUES ($1, $2, 0, $3, 'message', $4, $5, $6, $7)`,
		cs.name, channelSerial, idArg, payload, mut.Serial, mut.Action == protocol.MessageAppend, cs.persisted,
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE messages SET payload = $3, deleted = $4 WHERE channel = $1 AND message_serial = $2`,
		cs.name, mut.Serial, payload, version.Action == protocol.MessageDelete,
	); err != nil {
		return nil, false, fmt.Errorf("storage/postgres: update projection: %w", err)
	}

	if err := cs.commitWrite(ctx, tx, cm, &busWrite{serial: channelSerial, prev: prev, kind: storage.KindMessage, rows: [][]byte{payload}}); err != nil {
		return nil, false, err
	}
	return cm, false, nil
}

// LatestVersion returns the projection entry for serial, or
// ErrTargetNotFound (DESIGN.md §13.4).
func (cs *channelStore) LatestVersion(ctx context.Context, serial string) (*protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var payload []byte
	switch err := cs.pool.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, serial).Scan(&payload); {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, storage.ErrTargetNotFound
	case err != nil:
		return nil, fmt.Errorf("storage/postgres: latest version: %w", err)
	}
	var m protocol.Message
	if err := msgpack.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("storage/postgres: decode latest version: %w", err)
	}
	return &m, nil
}

// Versions returns every version of serial ordered by version (the
// versions index), paginated at version granularity (DESIGN.md §13.4).
// The cursor is a version serial decomposed to (channel_serial, idx);
// Limit+1 detects HasMore. ErrTargetNotFound if the message has no rows.
func (cs *channelStore) Versions(ctx context.Context, serial2 string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}
	var (
		cursorCS  string
		cursorIdx int
	)
	if q.Cursor != "" {
		var err error
		cursorCS, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse versions cursor: %w", err)
		}
	}

	// Collapse runs of appends to the aggregate (DESIGN.md §13.3, §13.4):
	// the inner LEAD window (always in forward version order) keeps an
	// append row only when the next version is not itself an append —
	// i.e. the last of its run — while creates/updates/deletes are kept
	// verbatim. Pagination (cursor + Limit) then applies over the
	// collapsed set, so the read reflects the aggregate, not each delta.
	query := fmt.Sprintf(`
		WITH ordered AS (
			SELECT channel_serial, idx, payload, is_append,
			       LEAD(is_append) OVER (ORDER BY channel_serial, idx) AS next_is_append
			FROM channel_messages
			WHERE channel = $1 AND message_serial = $2 AND kind = 'message'
		),
		collapsed AS (
			SELECT channel_serial, idx, payload FROM ordered
			WHERE is_append = FALSE OR next_is_append IS DISTINCT FROM TRUE
		)
		SELECT channel_serial, payload FROM collapsed
		WHERE ($3 = '' OR (channel_serial, idx) %s ($3, $4))
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, serial2, cursorCS, cursorIdx, q.Limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: versions query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			cs2     string
			payload []byte
		)
		if err := rows.Scan(&cs2, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan version: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode version %s: %w", cs2, err)
		}
		page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
			ChannelSerial: cs2,
			Messages:      []*protocol.Message{&m},
		})
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: versions rows: %w", err)
	}
	if len(page.ChannelMessages) == 0 {
		return storage.HistoryPage{}, storage.ErrTargetNotFound
	}
	if q.Limit > 0 && len(page.ChannelMessages) > q.Limit {
		page.ChannelMessages = page.ChannelMessages[:q.Limit]
		page.HasMore = true
	}
	return page, nil
}

// StorePresence persists a presence publish on channel_messages (kind =
// presence) and folds it into the presence projection table, all in one
// transaction, then announces the cm on the bus. It reaches the channel's
// appender exactly like a message publish (DESIGN.md §12.2, §12.5); the membership table is authoritative across
// nodes the moment the tx commits.
func (cs *channelStore) StorePresence(ctx context.Context, presence []*protocol.PresenceMessage) (*protocol.ChannelMessage, bool, error) {
	if len(presence) == 0 {
		return nil, false, errors.New("storage/postgres: StorePresence with no messages")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	if cs.presenceLanes != nil {
		return cs.storePresenceBatched(ctx, cs.presenceLanes, presence)
	}
	if storage.IsPresenceReentry(ctx) {
		// A lease-lapse re-entry skips members still present (§12.5).
		return cs.storePresenceTx(ctx, presence, true)
	}
	// Unbatched, each operation holds a pool connection for its whole
	// transaction, including any wait on the room's row lock. Past the
	// bound it is refused at once rather than queued, so SYNC reads and
	// publishes always find a connection (DESIGN.md §12.5).
	// Server-synthesised presence (a LEAVE nothing would retry) is exempt.
	if cs.presenceSlots != nil && !storage.IsServerPresence(ctx) {
		select {
		case cs.presenceSlots <- struct{}{}:
			defer func() { <-cs.presenceSlots }()
		default:
			if cs.wmetrics != nil {
				cs.wmetrics.nacks.WithLabelValues("presence_inflight").Inc()
			}
			return nil, false, fmt.Errorf("%w: too many presence operations in flight", storage.ErrOverloaded)
		}
	}
	return cs.storePresenceTx(ctx, presence, false)
}

// storePresenceTx is StorePresence in a transaction of its own: the
// unbatched path, a reaper's LEAVEs, and server-synthesised presence a
// full lane could not queue (DESIGN.md §6.3, §12.5).
//
// onlyAbsent (the reaper's LEAVEs) leaves out every operation whose
// member is in the presence table once the channel's row lock is held,
// and stores nothing (a nil cm) if that leaves none. Every writer of a
// room's presence rows holds that lock first, so the check sees every
// ENTER committed before this transaction, and one committed after it
// sorts after the LEAVE.
func (cs *channelStore) storePresenceTx(ctx context.Context, presence []*protocol.PresenceMessage, onlyAbsent bool) (*protocol.ChannelMessage, bool, error) {
	static := storage.IsStaticPresence(ctx)

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", unavailable(err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Row lock first, then the idempotency lookup (see Store).
	channelSerial, prev, err := cs.advanceSerial(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	if onlyAbsent {
		if presence, err = cs.absentMembers(ctx, tx, presence); err != nil || len(presence) == 0 {
			return nil, false, err // the rollback undoes the serial advance
		}
	}
	if original, err := cs.findIdempotent(ctx, tx, nonEmptyPresenceIDs(presence), channelSerial); err != nil || original != nil {
		return original, original != nil, err
	}

	for i, p := range presence {
		storage.StampPresenceMember(p, channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Presence: presence}

	rows := make([][]byte, 0, len(presence))
	for i, p := range presence {
		payload, err := msgpack.Marshal(p)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode presence %d: %w", i, err)
		}
		rows = append(rows, payload)
		var idArg any
		if p.ID != "" {
			idArg = p.ID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, persisted)
			 VALUES ($1, $2, $3, $4, 'presence', $5, $6)`,
			cs.name, channelSerial, i, idArg, payload, cs.persisted,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert presence %d: %w", i, err)
		}

		// Fold into the membership projection in the same tx.
		switch p.Action {
		case protocol.PresenceLeave, protocol.PresenceAbsent:
			if _, err := tx.Exec(ctx,
				`DELETE FROM presence WHERE channel = $1 AND connection_id = $2 AND client_id = $3`,
				cs.name, p.ConnectionID, p.ClientID,
			); err != nil {
				return nil, false, fmt.Errorf("storage/postgres: presence leave: %w", err)
			}
		default: // Enter, Update, Present
			if static {
				// Static fixture member (DESIGN.md §9, §12.5): a sentinel
				// owner and an 'infinity' lease so no lease-bump loop claims
				// it and neither lease mode's reaper deletes it (both skip
				// the sentinel owner). It belongs to no connection, so nothing ever
				// synthesises a LEAVE for it.
				if _, err := tx.Exec(ctx,
					`INSERT INTO presence (channel, connection_id, client_id, channel_serial, payload, node_id, expires_at)
					 VALUES ($1, $2, $3, $4, $5, $6, 'infinity')
					 ON CONFLICT (channel, connection_id, client_id)
					 DO UPDATE SET channel_serial = EXCLUDED.channel_serial,
					               payload = EXCLUDED.payload,
					               node_id = EXCLUDED.node_id,
					               expires_at = EXCLUDED.expires_at`,
					cs.name, p.ConnectionID, p.ClientID, channelSerial, payload, fixtureNodeID,
				); err != nil {
					return nil, false, fmt.Errorf("storage/postgres: presence fixture upsert: %w", err)
				}
				break
			}
			// Stamp the owning node (§12.5) and, in member lease mode,
			// a fresh lease that this node's bump loop keeps ahead while
			// it lives. In node lease mode the row's liveness is its
			// node's lease, so the row's own is 'infinity'. If the node
			// dies, the reaper on another node deletes the row once the
			// lease lapses and emits a synthetic LEAVE.
			if _, err := tx.Exec(ctx,
				`INSERT INTO presence (channel, connection_id, client_id, channel_serial, payload, node_id, expires_at)
				 VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $8 THEN 'infinity'::timestamptz ELSE now() + make_interval(secs => $7) END)
				 ON CONFLICT (channel, connection_id, client_id)
				 DO UPDATE SET channel_serial = EXCLUDED.channel_serial,
				               payload = EXCLUDED.payload,
				               node_id = EXCLUDED.node_id,
				               expires_at = EXCLUDED.expires_at`,
				cs.name, p.ConnectionID, p.ClientID, channelSerial, payload, cs.node, presenceLeaseWindow.Seconds(), cs.leaseNode,
			); err != nil {
				return nil, false, fmt.Errorf("storage/postgres: presence upsert: %w", err)
			}
		}
	}

	if err := cs.commitWrite(ctx, tx, cm, &busWrite{serial: channelSerial, prev: prev, kind: storage.KindPresence, rows: rows}); err != nil {
		return nil, false, err
	}
	if cs.rows != nil {
		cs.rows.add(cs.name) // advance_channel_serial made or found the row
	}
	return cm, false, nil
}

// absentMembers returns the operations of presence whose member has no
// row in the presence table, read inside tx (storePresenceTx). FOR KEY
// SHARE makes the read wait for a reaper DELETE of one of the rows that
// has not committed yet, so a re-entry does not skip a member that is
// about to be gone.
func (cs *channelStore) absentMembers(ctx context.Context, tx pgx.Tx, presence []*protocol.PresenceMessage) ([]*protocol.PresenceMessage, error) {
	conns := make([]string, len(presence))
	clients := make([]string, len(presence))
	for i, p := range presence {
		conns[i], clients[i] = p.ConnectionID, p.ClientID
	}
	rows, err := tx.Query(ctx, `
		SELECT connection_id, client_id FROM presence
		WHERE channel = $1 AND (connection_id, client_id) IN (SELECT * FROM unnest($2::text[], $3::text[]))
		FOR KEY SHARE`,
		cs.name, conns, clients)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: presence lookup: %w", err)
	}
	present := make(map[[2]string]bool)
	for rows.Next() {
		var conn, client string
		if err := rows.Scan(&conn, &client); err != nil {
			rows.Close()
			return nil, fmt.Errorf("storage/postgres: presence lookup: %w", err)
		}
		present[[2]string{conn, client}] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage/postgres: presence lookup: %w", err)
	}
	out := presence[:0:0]
	for _, p := range presence {
		if !present[[2]string{p.ConnectionID, p.ClientID}] {
			out = append(out, p)
		}
	}
	return out, nil
}

// StoreAnnotation persists an annotation publish on channel_messages
// (kind = annotation, message_serial = the TARGET message serial so the
// channel_messages serial index serves annotations-for-message scans,
// DESIGN.md §14.1, §14.4), then announces the cm on the bus so it reaches
// every node's appender exactly like a message publish. Every target must resolve in the messages projection
// (ErrTargetNotFound otherwise, like a mutation). The returned cm is the
// annotation summary-fold seam.
func (cs *channelStore) StoreAnnotation(ctx context.Context, annotations []*protocol.Annotation) (*protocol.ChannelMessage, bool, error) {
	if len(annotations) == 0 {
		return nil, false, errors.New("storage/postgres: StoreAnnotation with no annotations")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tx, err := cs.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("storage/postgres: begin tx: %w", unavailable(err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Row lock first, then the idempotency lookup (see Store).
	channelSerial, prev, err := cs.advanceSerial(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	if original, err := cs.findIdempotent(ctx, tx, nonEmptyAnnotationIDs(annotations), channelSerial); err != nil || original != nil {
		return original, original != nil, err
	}

	// Target existence: every annotation must reference a message in the
	// projection (DESIGN.md §14.1). A bad target returns early, and the
	// deferred Rollback undoes the serial advance.
	for _, a := range annotations {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM messages WHERE channel = $1 AND message_serial = $2)`,
			cs.name, a.MessageSerial,
		).Scan(&exists); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: annotation target lookup: %w", err)
		}
		if !exists {
			return nil, false, storage.ErrTargetNotFound
		}
	}

	for i, a := range annotations {
		a.Serial = serial.MessageSerial(channelSerial, i)
	}
	cm := &protocol.ChannelMessage{ChannelSerial: channelSerial, Annotations: annotations}

	rows := make([][]byte, 0, len(annotations))
	sums := make([][]byte, 0, len(annotations))
	for i, a := range annotations {
		payload, err := msgpack.Marshal(a)
		if err != nil {
			return nil, false, fmt.Errorf("storage/postgres: encode annotation %d: %w", i, err)
		}
		// Fold the annotation into its target's summary projection and encode
		// the post-fold snapshot, atomically within this tx (DESIGN.md §14.2).
		// The snapshot rides the summary column (never the annotation payload,
		// which stays a raw annotation) so a remote node reconstructs the
		// exact delivery summary from the cm on its LISTEN load.
		summaryBlob, err := cs.foldSummaryTx(ctx, tx, a)
		if err != nil {
			return nil, false, err
		}
		rows = append(rows, payload)
		sums = append(sums, summaryBlob)
		var idArg any
		if a.ID != "" {
			idArg = a.ID
		}
		// message_serial holds the TARGET message serial so the existing
		// channel_messages_serial_idx serves the annotations-for-message
		// scan (DESIGN.md §14.4).
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial, summary, persisted)
			 VALUES ($1, $2, $3, $4, 'annotation', $5, $6, $7, $8)`,
			cs.name, channelSerial, i, idArg, payload, a.MessageSerial, summaryBlob, cs.persisted,
		); err != nil {
			return nil, false, fmt.Errorf("storage/postgres: insert annotation %d: %w", i, err)
		}
	}

	if err := cs.commitWrite(ctx, tx, cm, &busWrite{serial: channelSerial, prev: prev, kind: storage.KindAnnotation, rows: rows, sums: sums}); err != nil {
		return nil, false, err
	}
	return cm, false, nil
}

// foldSummaryTx folds one annotation into its target message's summary on
// the messages projection and returns the msgpack-encoded post-fold snapshot
// for the annotation's summary column (DESIGN.md §14.2). It runs inside the
// StoreAnnotation tx with the target already validated to exist: it reads the
// projection payload, folds, writes the merged Message back (so message reads
// carry the current summary), and also stamps the snapshot onto the in-memory
// annotation for the publisher-node delivery path.
func (cs *channelStore) foldSummaryTx(ctx context.Context, tx pgx.Tx, a *protocol.Annotation) ([]byte, error) {
	var payload []byte
	if err := tx.QueryRow(ctx,
		`SELECT payload FROM messages WHERE channel = $1 AND message_serial = $2`,
		cs.name, a.MessageSerial,
	).Scan(&payload); err != nil {
		return nil, fmt.Errorf("storage/postgres: load projection for summary fold: %w", err)
	}
	var m protocol.Message
	if err := msgpack.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("storage/postgres: decode projection for summary fold: %w", err)
	}
	m.Summary = m.Summary.Apply(a)
	merged, err := msgpack.Marshal(&m)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: encode projection after summary fold: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE messages SET payload = $3 WHERE channel = $1 AND message_serial = $2`,
		cs.name, a.MessageSerial, merged,
	); err != nil {
		return nil, fmt.Errorf("storage/postgres: update projection after summary fold: %w", err)
	}
	a.Summary = m.Summary.Clone()
	if m.Summary == nil {
		return nil, nil
	}
	summaryBlob, err := msgpack.Marshal(m.Summary)
	if err != nil {
		return nil, fmt.Errorf("storage/postgres: encode summary snapshot: %w", err)
	}
	return summaryBlob, nil
}

// Annotations returns the annotations attached to messageSerial in stream
// order, paginated at annotation-serial granularity (DESIGN.md §14.4). The
// scan filters channel_messages by kind = 'annotation' and message_serial
// = the target, served by channel_messages_serial_idx. An unknown target
// yields an empty page.
func (cs *channelStore) Annotations(ctx context.Context, messageSerial string, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}

	var (
		cursorChannelSerial string
		cursorIdx           int
	)
	if q.Cursor != "" {
		var err error
		cursorChannelSerial, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse annotation cursor: %w", err)
		}
	}

	query := fmt.Sprintf(`
		SELECT channel_serial, idx, payload
		FROM channel_messages
		WHERE channel = $1
		  AND kind = 'annotation'
		  AND message_serial = $2
		  AND ($3 = '' OR (channel_serial, idx) %s ($3, $4))
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, messageSerial, cursorChannelSerial, cursorIdx, q.Limit)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: annotations query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			cs2     string
			idx     int
			payload []byte
		)
		if err := rows.Scan(&cs2, &idx, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan annotation row: %w", err)
		}
		var a protocol.Annotation
		if err := msgpack.Unmarshal(payload, &a); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode annotation payload %s:%d: %w", cs2, idx, err)
		}
		appendAnnotation(&page, cs2, &a)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: annotation rows: %w", err)
	}

	if q.Limit > 0 && itemCount(page) > q.Limit {
		trimToLimit(&page, q.Limit)
		page.HasMore = true
	}
	return page, nil
}

// Members returns the channel's presence projection plus the channel's
// current watermark serial as the as-of point. Both are read in one
// REPEATABLE READ snapshot, pipelined in one round trip, so the set is
// exactly the fold of every cm up to and including the as-of serial and
// of none after it: presence writes fold the set and advance the
// watermark in the same transaction, under the channel's row lock. A
// node seeding its local member set from this (DESIGN.md §12.4) can then
// fold exactly the cms after the as-of serial.
func (cs *channelStore) Members(ctx context.Context) (members []*protocol.PresenceMessage, asOfSerial string, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	b := &pgx.Batch{}
	b.Queue(`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`)
	b.Queue(`SELECT payload FROM presence WHERE channel = $1 ORDER BY channel_serial, client_id`, cs.name)
	b.Queue(`SELECT channel_serial FROM channels WHERE name = $1`, cs.name)
	b.Queue(`COMMIT`)
	br := cs.pool.SendBatch(ctx, b)
	defer func() {
		// Every result is read below, so Close finds nothing unread; what
		// it reports is the only sign of a statement that did not complete.
		if cerr := br.Close(); cerr != nil && retErr == nil {
			members, asOfSerial, retErr = nil, "", fmt.Errorf("storage/postgres: members close: %w", cerr)
		}
	}()
	if _, err := br.Exec(); err != nil {
		return nil, "", fmt.Errorf("storage/postgres: members begin: %w", err)
	}

	out, err := func() ([]*protocol.PresenceMessage, error) {
		rows, err := br.Query()
		if err != nil {
			return nil, fmt.Errorf("storage/postgres: members query: %w", err)
		}
		defer rows.Close()
		var out []*protocol.PresenceMessage
		for rows.Next() {
			var payload []byte
			if err := rows.Scan(&payload); err != nil {
				return nil, fmt.Errorf("storage/postgres: scan member: %w", err)
			}
			var p protocol.PresenceMessage
			if err := msgpack.Unmarshal(payload, &p); err != nil {
				return nil, fmt.Errorf("storage/postgres: decode member: %w", err)
			}
			out = append(out, &p)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("storage/postgres: members rows: %w", err)
		}
		return out, nil
	}()
	if err != nil {
		return nil, "", err
	}

	var asOf string
	if err := br.QueryRow().Scan(&asOf); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, "", fmt.Errorf("storage/postgres: members watermark: %w", err)
	}
	if _, err := br.Exec(); err != nil {
		return nil, "", fmt.Errorf("storage/postgres: members commit: %w", err)
	}
	return out, asOf, nil
}

// History runs a direction-aware range scan over channel_messages at item
// granularity. Time bounds (q.Start / q.End) are applied as predicates
// on channel_serial — the serial's leading timestamp prefix makes the
// lex compare correct (DESIGN.md §8 + internal/serial.TimestampBounds),
// so no separate timestamp column or index is needed.
//
// q.Cursor is a Message.Serial (`<channelSerial>:<idx>`), decomposed
// into a (channel_serial, idx) tuple and applied as a strict (>/<)
// predicate over the lex-ordered pair. q.Limit caps Messages (not
// ChannelMessages); we fetch Limit+1 rows to detect HasMore. Rows are
// grouped into ChannelMessages in Go — a multi-message batch may be
// split across pages, so the head/tail ChannelMessage in a page can
// be partial.
func (cs *channelStore) History(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	page, err := cs.history(ctx, q)
	return page, unavailable(err)
}

func (cs *channelStore) history(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.HistoryPage{}, err
	}

	if q.Collapse && q.Kind.Normalize() == storage.KindMessage {
		return cs.collapsedHistory(ctx, q)
	}

	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	forwards := q.Direction == storage.DirectionForwards

	var (
		cursorChannelSerial string
		cursorIdx           int
	)
	if q.Cursor != "" {
		var err error
		cursorChannelSerial, cursorIdx, err = serial.ParseMessageSerial(q.Cursor)
		if err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: parse cursor: %w", err)
		}
	}

	limit := q.Limit
	overLimit := q.Limit > 0
	order := "ASC"
	cursorOp := ">"
	if !forwards {
		order = "DESC"
		cursorOp = "<"
	}

	// The cursor predicate is the lex compare over the (channel_serial,
	// idx) tuple. $4 holds the cursor's channelSerial and $5 its idx;
	// empty $4 means "no cursor". $7 is the optional inclusive
	// channelSerial upper bound (EndChannelSerial).
	wantKind := q.Kind.Normalize()

	query := fmt.Sprintf(`
		SELECT channel_serial, idx, payload
		FROM channel_messages
		WHERE channel = $1
		  AND kind = $8
		  AND ($2 = '' OR channel_serial >= $2)
		  AND ($3 = '' OR channel_serial <  $3)
		  AND ($4 = '' OR (channel_serial, idx) %s ($4, $5))
		  AND ($7 = '' OR channel_serial <= $7)
		  AND ($9 = '' OR channel_serial > $9)
		ORDER BY channel_serial %s, idx %s
		LIMIT CASE WHEN $6 > 0 THEN $6 + 1 ELSE NULL END
	`, cursorOp, order, order)

	rows, err := cs.pool.Query(ctx, query,
		cs.name, timeLower, timeUpper,
		cursorChannelSerial, cursorIdx,
		limit,
		q.EndChannelSerial,
		string(wantKind),
		q.AfterChannelSerial,
	)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			cs2     string
			idx     int
			payload []byte
		)
		if err := rows.Scan(&cs2, &idx, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan row: %w", err)
		}
		if wantKind == storage.KindPresence {
			var p protocol.PresenceMessage
			if err := msgpack.Unmarshal(payload, &p); err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode presence payload %s:%d: %w", cs2, idx, err)
			}
			appendPresence(&page, cs2, &p)
			continue
		}
		if wantKind == storage.KindAnnotation {
			var a protocol.Annotation
			if err := msgpack.Unmarshal(payload, &a); err != nil {
				return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode annotation payload %s:%d: %w", cs2, idx, err)
			}
			appendAnnotation(&page, cs2, &a)
			continue
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode payload %s:%d: %w", cs2, idx, err)
		}
		appendMessage(&page, cs2, &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: history rows: %w", err)
	}

	if overLimit && itemCount(page) > limit {
		trimToLimit(&page, limit)
		page.HasMore = true
	}
	return page, nil
}

// collapsedHistory returns the latest version of each message positioned
// at its create serial (DESIGN.md §13.4), backing the default REST
// message history. It scans the latest-version projection ordered by
// message_serial (== create serial, so create-timeline order), applies
// the time bounds / cursor / limit against that identity, and regroups
// rows under their create channelSerial (DESC within a batch for
// backwards, matching the raw scan). q.EndChannelSerial (the
// fromSerial/untilAttached bound) caps rows to their CREATE
// channelSerial <= the bound: split_part(message_serial, ':', 1)
// strips the ":idx" suffix (message_serial never has more than one
// ':' — serial.ParseMessageSerial's channelSerial half never contains
// one) to get the pure channelSerial for the compare.
func (cs *channelStore) collapsedHistory(ctx context.Context, q storage.HistoryQuery) (storage.HistoryPage, error) {
	timeLower, timeUpper := serial.TimestampBounds(q.Start, q.End)
	forwards := q.Direction == storage.DirectionForwards
	order, cursorOp := "ASC", ">"
	if !forwards {
		order, cursorOp = "DESC", "<"
	}

	query := fmt.Sprintf(`
		SELECT message_serial, payload FROM messages
		WHERE channel = $1
		  AND ($2 = '' OR message_serial >= $2)
		  AND ($3 = '' OR message_serial <  $3)
		  AND ($4 = '' OR message_serial %s $4)
		  AND ($6 = '' OR split_part(message_serial, ':', 1) <= $6)
		ORDER BY message_serial %s
		LIMIT CASE WHEN $5 > 0 THEN $5 + 1 ELSE NULL END
	`, cursorOp, order)

	rows, err := cs.pool.Query(ctx, query, cs.name, timeLower, timeUpper, q.Cursor, q.Limit, q.EndChannelSerial)
	if err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: collapsed history query: %w", err)
	}
	defer rows.Close()

	var page storage.HistoryPage
	for rows.Next() {
		var (
			identity string
			payload  []byte
		)
		if err := rows.Scan(&identity, &payload); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: scan collapsed row: %w", err)
		}
		var m protocol.Message
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return storage.HistoryPage{}, fmt.Errorf("storage/postgres: decode projection %s: %w", identity, err)
		}
		appendMessage(&page, storage.CreateChannelSerial(identity), &m)
	}
	if err := rows.Err(); err != nil {
		return storage.HistoryPage{}, fmt.Errorf("storage/postgres: collapsed history rows: %w", err)
	}

	if q.Limit > 0 && itemCount(page) > q.Limit {
		trimToLimit(&page, q.Limit)
		page.HasMore = true
	}
	return page, nil
}

// appendMessage tacks m onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. Used by the
// row-scanning loop above where consecutive rows from the same batch
// arrive contiguously.
func appendMessage(page *storage.HistoryPage, channelSerial string, m *protocol.Message) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Messages = append(page.ChannelMessages[n-1].Messages, m)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Messages:      []*protocol.Message{m},
	})
}

// appendPresence tacks p onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. The presence
// analogue of appendMessage.
func appendPresence(page *storage.HistoryPage, channelSerial string, p *protocol.PresenceMessage) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Presence = append(page.ChannelMessages[n-1].Presence, p)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Presence:      []*protocol.PresenceMessage{p},
	})
}

// appendAnnotation tacks a onto the trailing ChannelMessage when its
// channelSerial matches; otherwise starts a fresh entry. The annotation
// analogue of appendMessage.
func appendAnnotation(page *storage.HistoryPage, channelSerial string, a *protocol.Annotation) {
	if n := len(page.ChannelMessages); n > 0 && page.ChannelMessages[n-1].ChannelSerial == channelSerial {
		page.ChannelMessages[n-1].Annotations = append(page.ChannelMessages[n-1].Annotations, a)
		return
	}
	page.ChannelMessages = append(page.ChannelMessages, &protocol.ChannelMessage{
		ChannelSerial: channelSerial,
		Annotations:   []*protocol.Annotation{a},
	})
}

// cmLen is the item count of a cm — Messages, Presence or Annotations,
// whichever the (single-kind) cm carries.
func cmLen(cm *protocol.ChannelMessage) int {
	return len(cm.Messages) + len(cm.Presence) + len(cm.Annotations)
}

// itemCount totals the items (Messages or Presence) across all
// ChannelMessages in page.
func itemCount(page storage.HistoryPage) int {
	n := 0
	for _, cm := range page.ChannelMessages {
		n += cmLen(cm)
	}
	return n
}

// trimToLimit drops items past limit, in order, possibly leaving the
// last surviving ChannelMessage partial. Trims whichever slice the cm
// carries (a page is single-kind).
func trimToLimit(page *storage.HistoryPage, limit int) {
	left := limit
	for i, cm := range page.ChannelMessages {
		if left == 0 {
			page.ChannelMessages = page.ChannelMessages[:i]
			return
		}
		if left >= cmLen(cm) {
			left -= cmLen(cm)
			continue
		}
		switch {
		case len(cm.Presence) > 0:
			cm.Presence = cm.Presence[:left]
		case len(cm.Annotations) > 0:
			cm.Annotations = cm.Annotations[:left]
		default:
			cm.Messages = cm.Messages[:left]
		}
		page.ChannelMessages = page.ChannelMessages[:i+1]
		return
	}
}

// nonEmptyIDs returns the subset of m.ID values that are non-empty.
// Used for the idempotency pre-check.
func nonEmptyIDs(msgs []*protocol.Message) []string {
	var ids []string
	for _, m := range msgs {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// nonEmptyPresenceIDs is the presence analogue of nonEmptyIDs.
func nonEmptyPresenceIDs(presence []*protocol.PresenceMessage) []string {
	var ids []string
	for _, p := range presence {
		if p.ID != "" {
			ids = append(ids, p.ID)
		}
	}
	return ids
}

// nonEmptyAnnotationIDs is the annotation analogue of nonEmptyIDs.
func nonEmptyAnnotationIDs(annotations []*protocol.Annotation) []string {
	var ids []string
	for _, a := range annotations {
		if a.ID != "" {
			ids = append(ids, a.ID)
		}
	}
	return ids
}

// findIdempotent returns the originally persisted cm when any of ids was
// already stored on this channel, or nil on a miss (DESIGN.md §6, §8).
// It must run after the caller has locked the channels row (via
// advanceSerial) in tx: that lock, not an index, is what makes the check
// race-free, because every writer of this channel queues on it.
// The lookup is bounded to serials from the channel's retention floor up
// to before (the serial this publish was just given; every stored cm of
// the channel sorts below it), so it touches only the leaves in that
// range: a channel is idempotent within its own retention (§6.3). It
// spans both retention classes, so rows left in the other class by a
// migration or a namespace change still dedupe.
func (cs *channelStore) findIdempotent(ctx context.Context, tx pgx.Tx, ids []string, before string) (*protocol.ChannelMessage, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var existingCS string
	err := tx.QueryRow(ctx,
		`SELECT channel_serial FROM channel_messages
		 WHERE channel = $1 AND id = ANY($2) AND channel_serial >= $3 AND channel_serial < $4 LIMIT 1`,
		cs.name, ids, cs.idempotencyFloor(), before).Scan(&existingCS)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("storage/postgres: idempotency lookup: %w", err)
	}
	return loadChannelMessageTx(ctx, tx, cs.name, existingCS)
}

// loadChannelMessageTx fetches a previously-persisted ChannelMessage
// inside the caller's transaction — used by the idempotency pre-check
// in Store to return the original cm on a hit.
func loadChannelMessageTx(ctx context.Context, tx pgx.Tx, channel, channelSerial string) (*protocol.ChannelMessage, error) {
	rows, err := tx.Query(ctx, sqlLoadCM, channel, channelSerial)
	return decodeChannelMessageRows(rows, err, channel, channelSerial)
}
