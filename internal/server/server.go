// Package server assembles the ably-server process: flag/env/config
// parsing, storage/realtime/REST wiring, HTTP routing, and graceful
// shutdown. cmd/ably-server is a thin wrapper that calls Run; tests
// (including the //go:build integration SDK suites in
// internal/server/integrationtest) call Run directly so they can
// discover ephemeral listener addresses via Opts.Ready/DebugReady
// without shelling out to a built binary.
package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/fixtures"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/tracing"
)

const (
	keysEnv            = "ABLY_SERVER_KEYS"
	postgresDSNEnv     = "ABLY_SERVER_POSTGRES_DSN"
	logFormatEnv       = "ABLY_SERVER_LOG_FORMAT"
	debugListenEnv     = "ABLY_SERVER_DEBUG_LISTEN"
	modeEnv            = "ABLY_SERVER_MODE"
	listenEnv          = "ABLY_SERVER_LISTEN"
	dataDirEnv         = "ABLY_SERVER_DATA_DIR"
	shutdownGraceEnv   = "ABLY_SERVER_SHUTDOWN_GRACE"
	logLevelEnv        = "ABLY_SERVER_LOG_LEVEL"
	configPathEnv      = "ABLY_SERVER_CONFIG"
	addrFileEnv        = "ABLY_SERVER_ADDR_FILE"
	enableStatsStubEnv = "ABLY_SERVER_ENABLE_STATS_STUB"
	busEnv             = "ABLY_SERVER_BUS"
	natsURLEnv         = "ABLY_SERVER_NATS_URL"
	natsInlineMaxEnv   = "ABLY_SERVER_NATS_INLINE_MAX_BYTES"
	pgNotifyModeEnv    = "ABLY_SERVER_POSTGRES_NOTIFY_MODE"
	pgNotifyWindowEnv  = "ABLY_SERVER_POSTGRES_NOTIFY_WINDOW"
	pgNotifyMaxPendEnv = "ABLY_SERVER_POSTGRES_NOTIFY_MAX_PENDING"
	busSweepEnv        = "ABLY_SERVER_BUS_SWEEP_INTERVAL"
	busSweepScopeEnv   = "ABLY_SERVER_BUS_SWEEP_SCOPE"
	channelIdleEnv     = "ABLY_SERVER_CHANNEL_IDLE_TIMEOUT"
	connOutboundEnv    = "ABLY_SERVER_CONN_OUTBOUND_MAX_BYTES"
	connWriteTOEnv     = "ABLY_SERVER_CONN_WRITE_TIMEOUT"
	wsReadBufEnv       = "ABLY_SERVER_WS_READ_BUFFER_SIZE"
	wsWriteBufEnv      = "ABLY_SERVER_WS_WRITE_BUFFER_SIZE"
	httpIdleEnv        = "ABLY_SERVER_HTTP_IDLE_TIMEOUT"

	messageRetentionEnv   = "ABLY_SERVER_MESSAGE_RETENTION"
	persistedRetentionEnv = "ABLY_SERVER_PERSISTED_RETENTION"

	publishLanesEnv     = "ABLY_SERVER_PUBLISH_LANES"
	publishBatchMaxEnv  = "ABLY_SERVER_PUBLISH_BATCH_MAX"
	publishLingerMaxEnv = "ABLY_SERVER_PUBLISH_LINGER_MAX"
	publishLingerMinEnv = "ABLY_SERVER_PUBLISH_LINGER_MIN"
	publishQueueMaxEnv  = "ABLY_SERVER_PUBLISH_QUEUE_MAX"
	publishBindEnv      = "ABLY_SERVER_PUBLISH_BIND_ON_WRITE"
)

// DefaultHTTPIdleTimeout is how long the HTTP server keeps an idle
// keep-alive connection open waiting for the next request (DESIGN.md
// §2.2). Without it an idle keep-alive connection is held until the
// client closes it.
const DefaultHTTPIdleTimeout = 120 * time.Second

// Opts bundles Run's inputs so the production main() and tests
// share one entry point. Args/Getenv/Out are required; Ready is an
// optional testing hook (see field doc).
type Opts struct {
	// Args is the slice of CLI args (excluding os.Args[0]).
	Args []string

	// Getenv resolves an environment variable; tests pass a stub.
	Getenv func(string) string

	// Out is the writer used for logs and flag-parsing errors.
	Out io.Writer

	// Ready, when non-nil, receives the bound listener's address once
	// net.Listen returns — used by tests that pass --listen=:0 to
	// discover the ephemeral port. The send is bounded by ctx so a
	// missing receiver does not deadlock startup.
	Ready chan<- net.Addr

	// DebugReady, when non-nil, receives the bound debug listener's
	// address once it starts — used by tests that pass
	// --debug-listen=:0 to discover the ephemeral port. Only sent to
	// when --debug-listen is set; the send is bounded by ctx.
	DebugReady chan<- net.Addr
}

// Run executes the server and returns the process exit code. All
// inputs are passed via Opts so the function is testable without
// touching package-level state.
func Run(ctx context.Context, opts Opts) int {
	// The config file's path must be known before the flags it seeds
	// are defined below, so it's resolved by hand (flag > env) ahead
	// of the real flag.Parse pass. --config is still registered as a
	// flag further down purely so fs.Parse recognises it and --help
	// lists it; its value there is unused.
	configPath := opts.Getenv(configPathEnv)
	if p := config.PathFromArgs(opts.Args); p != "" {
		configPath = p
	}
	var file config.File
	if configPath != "" {
		f, err := config.Load(configPath)
		if err != nil {
			fmt.Fprintln(opts.Out, err)
			return 1
		}
		file = *f
	}

	shutdownGraceDefault, err := config.DefaultDuration(opts.Getenv(shutdownGraceEnv), file.ShutdownGrace, 10*time.Second)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	channelIdleDefault, err := config.DefaultDuration(opts.Getenv(channelIdleEnv), file.ChannelIdleTimeout, core.DefaultChannelIdleTimeout)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	connWriteTimeoutDefault, err := config.DefaultDuration(opts.Getenv(connWriteTOEnv), file.ConnWriteTimeout, realtime.DefaultConnWriteTimeout)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	connOutboundDefault, err := config.DefaultInt64(opts.Getenv(connOutboundEnv), file.ConnOutboundMaxBytes, realtime.DefaultConnOutboundMaxBytes)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	wsReadBufDefault, err := config.DefaultInt64(opts.Getenv(wsReadBufEnv), file.WSReadBufferSize, realtime.DefaultWSReadBufferSize)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	wsWriteBufDefault, err := config.DefaultInt64(opts.Getenv(wsWriteBufEnv), file.WSWriteBufferSize, realtime.DefaultWSWriteBufferSize)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	httpIdleDefault, err := config.DefaultDuration(opts.Getenv(httpIdleEnv), file.HTTPIdleTimeout, DefaultHTTPIdleTimeout)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	messageRetentionDefault, err := config.DefaultDuration(opts.Getenv(messageRetentionEnv), file.MessageRetention, postgres.DefaultMessageRetention)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	persistedRetentionDefault, err := config.DefaultDuration(opts.Getenv(persistedRetentionEnv), file.PersistedRetention, postgres.DefaultPersistedRetention)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishLanesDefault, err := config.DefaultInt(opts.Getenv(publishLanesEnv), file.PublishLanes, postgres.DefaultPublishLanes)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishBatchMaxDefault, err := config.DefaultInt(opts.Getenv(publishBatchMaxEnv), file.PublishBatchMax, postgres.DefaultPublishBatchMax)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishLingerMaxDefault, err := config.DefaultDuration(opts.Getenv(publishLingerMaxEnv), file.PublishLingerMax, postgres.DefaultPublishLingerMax)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishLingerMinDefault, err := config.DefaultDuration(opts.Getenv(publishLingerMinEnv), file.PublishLingerMin, 0)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishQueueMaxDefault, err := config.DefaultInt(opts.Getenv(publishQueueMaxEnv), file.PublishQueueMax, postgres.DefaultPublishQueueMax)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	publishBindDefault, err := config.DefaultBool(opts.Getenv(publishBindEnv), file.PublishBindOnWrite, false)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	enableStatsStubDefault, err := config.DefaultBool(opts.Getenv(enableStatsStubEnv), file.EnableStatsStub, false)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	natsInlineMaxDefault, err := config.DefaultInt(opts.Getenv(natsInlineMaxEnv), file.NATSInlineMaxBytes, postgres.DefaultNATSInlineMaxBytes)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	pgNotifyWindowDefault, err := config.DefaultDuration(opts.Getenv(pgNotifyWindowEnv), file.PostgresNotifyWindow, postgres.DefaultNotifyWindow)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	pgNotifyMaxPendingDefault, err := config.DefaultInt(opts.Getenv(pgNotifyMaxPendEnv), file.PostgresNotifyMaxPending, postgres.DefaultNotifyMaxPending)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	busSweepDefault, err := config.DefaultDuration(opts.Getenv(busSweepEnv), file.BusSweepInterval, 0)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	fs := flag.NewFlagSet("ably-server", flag.ContinueOnError)
	fs.SetOutput(opts.Out)
	fs.String("config", configPath, "path to an optional TOML config file (env: "+configPathEnv+")")
	listen := fs.String("listen", config.Default(opts.Getenv(listenEnv), file.Listen, ":8080"), "address for HTTP/WS listener (env: "+listenEnv+")")
	var keysFlags multiFlag
	fs.Var(&keysFlags, "keys", "API key in appId.keyId:keySecret format; repeatable (env: "+keysEnv+", comma-separated)")
	mode := fs.String("mode", config.Default(opts.Getenv(modeEnv), file.Mode, "memory"), "storage backend: memory, disk, or cluster (env: "+modeEnv+")")
	dataDir := fs.String("data-dir", config.Default(opts.Getenv(dataDirEnv), file.DataDir, "./data"), "data directory for disk mode (holds the bbolt file) (env: "+dataDirEnv+")")
	postgresDSN := fs.String("postgres-dsn", config.Default(opts.Getenv(postgresDSNEnv), file.PostgresDSN, ""), "libpq DSN for cluster mode, e.g. postgres://user:pw@host:5432/db?sslmode=disable; a comma-separated list of URL-form DSNs shards channels across the databases by channel-name hash, fixed for the life of the data (DESIGN.md §6.4) (env: "+postgresDSNEnv+")")
	bus := fs.String("bus", config.Default(opts.Getenv(busEnv), file.Bus, postgres.BusPGNotify), "cluster-mode cross-node bus: pgnotify (the shipped LISTEN/NOTIFY broker), postgres (per-channel LISTEN, see --postgres-notify-mode) or nats; Postgres stays the store in every case (DESIGN.md §7.2) (env: "+busEnv+")")
	natsURL := fs.String("nats-url", config.Default(opts.Getenv(natsURLEnv), file.NATSURL, ""), "NATS server URL for --bus=nats, e.g. nats://host:4222; a comma-separated list of one NATS cluster's servers is accepted (env: "+natsURLEnv+")")
	natsInlineMax := fs.Int("nats-inline-max-bytes", natsInlineMaxDefault, "largest encoded message the NATS bus carries inline; larger ones travel as a pointer read back from Postgres (env: "+natsInlineMaxEnv+")")
	pgNotifyMode := fs.String("postgres-notify-mode", config.Default(opts.Getenv(pgNotifyModeEnv), file.PostgresNotifyMode, string(postgres.NotifyCoalesced)), "--bus=postgres notify mode: coalesced (writes commit without NOTIFY; at most one wake-up per channel per window) or transactional (one NOTIFY per write, inside its transaction) (env: "+pgNotifyModeEnv+")")
	pgNotifyWindow := fs.Duration("postgres-notify-window", pgNotifyWindowDefault, "coalescing window for --postgres-notify-mode=coalesced; under 20ms logs a warning (DESIGN.md §7.2) (env: "+pgNotifyWindowEnv+")")
	pgNotifyMaxPending := fs.Int("postgres-notify-max-pending", pgNotifyMaxPendingDefault, "cap on channels pending a coalesced wake-up on this node; writes beyond it are delivered by the sweep instead (env: "+pgNotifyMaxPendEnv+")")
	busSweep := fs.Duration("bus-sweep-interval", busSweepDefault, "how often --bus=postgres or --bus=nats checks the channels in --bus-sweep-scope against their committed serial and catches up one that fell behind; 0 means the bus default (30s) (env: "+busSweepEnv+")")
	busSweepScope := fs.String("bus-sweep-scope", config.Default(opts.Getenv(busSweepScopeEnv), file.BusSweepScope, postgres.SweepSubscribed), "channels the --bus-sweep-interval sweep reads: subscribed (bound channels with an attachment or presence member on this node) or bound (every bound channel) (DESIGN.md §7.2) (env: "+busSweepScopeEnv+")")
	messageRetention := fs.Duration("message-retention", messageRetentionDefault, "cluster mode: how long a channel outside any persisted namespace keeps its message log, the continuity window (DESIGN.md §6.3) (env: "+messageRetentionEnv+")")
	persistedRetention := fs.Duration("persisted-retention", persistedRetentionDefault, "cluster mode: how long a channel in a persisted namespace keeps its message log (DESIGN.md §6.3) (env: "+persistedRetentionEnv+")")
	publishLanes := fs.Int("publish-lanes", publishLanesDefault, "cluster mode: publish lanes for leading-edge batching; a channel always uses the same lane; 0 commits every publish in its own transaction (DESIGN.md §6.3) (env: "+publishLanesEnv+")")
	publishBatchMax := fs.Int("publish-batch-max", publishBatchMaxDefault, "cluster mode: most publishes committed in one batch transaction (env: "+publishBatchMaxEnv+")")
	publishLingerMax := fs.Duration("publish-linger-max", publishLingerMaxDefault, "cluster mode: once a lane's batch has been in flight this long, queued publishes of other channels start a second batch (env: "+publishLingerMaxEnv+")")
	publishLingerMin := fs.Duration("publish-linger-min", publishLingerMinDefault, "cluster mode: how long an idle lane holds its first publish so others can join its commit; 0 commits at once (DESIGN.md §6.3) (env: "+publishLingerMinEnv+")")
	publishQueueMax := fs.Int("publish-queue-max", publishQueueMaxDefault, "cluster mode: publishes queued per lane before new ones are refused with 42910 (env: "+publishQueueMaxEnv+")")
	publishBindOnWrite := fs.Bool("publish-bind-on-write", publishBindDefault, "cluster mode: bind a channel on every REST publish, as before the write-only path; false (the default) stores a publish to a channel with no attachment or presence member on this node without binding it, and creates a missing channel row inside the batch (DESIGN.md §5.1, §6.3) (env: "+publishBindEnv+")")
	hbInterval := fs.Duration("heartbeat-interval", realtime.DefaultHeartbeatInterval, "server-driven HEARTBEAT cadence")
	remainPresentFor := fs.Duration("presence-remain-for", realtime.DefaultRemainPresentFor, "how long a presence member survives an abrupt disconnect before its LEAVE is synthesised, so a resume+re-enter avoids a flicker (DESIGN.md §12.5)")
	shutdownGrace := fs.Duration("shutdown-grace", shutdownGraceDefault, "window to disconnect existing connections on SIGTERM (env: "+shutdownGraceEnv+")")
	logLevel := fs.String("log-level", config.Default(opts.Getenv(logLevelEnv), file.LogLevel, "info"), "log level: "+logging.LevelNames+" (env: "+logLevelEnv+")")
	logFormat := fs.String("log-format", config.Default(opts.Getenv(logFormatEnv), file.LogFormat, "text"), "log format: text or json (env: "+logFormatEnv+")")
	debugListen := fs.String("debug-listen", config.Default(opts.Getenv(debugListenEnv), file.DebugListen, ""), "address for the pprof debug listener; disabled if empty (env: "+debugListenEnv+")")
	addrFile := fs.String("addr-file", opts.Getenv(addrFileEnv), "path to write the bound listener address to once listening; used by a parent process to discover an ephemeral (--listen :0) port (env: "+addrFileEnv+")")
	enableStatsStub := fs.Bool("enable-stats-stub", enableStatsStubDefault, "register the GET/POST /stats compatibility stub used by SDK test flows; unregistered (404) by default (env: "+enableStatsStubEnv+")")
	channelIdleTimeout := fs.Duration("channel-idle-timeout", channelIdleDefault, "evict a channel with no attachments, no operation in flight and no presence members after this long idle, releasing its storage binding; 0 disables eviction (DESIGN.md §5.1) (env: "+channelIdleEnv+")")
	connOutboundMaxBytes := fs.Int64("conn-outbound-max-bytes", connOutboundDefault, "bytes of encoded frames queued for one connection before publishers to it wait; a queue that stays full for --conn-write-timeout disconnects the client as a slow consumer (DESIGN.md §5.2) (env: "+connOutboundEnv+")")
	connWriteTimeout := fs.Duration("conn-write-timeout", connWriteTimeoutDefault, "deadline for one frame write, and the longest wait for room in a full outbound queue, before the connection is closed as a slow consumer (DESIGN.md §5.2) (env: "+connWriteTOEnv+")")
	wsReadBufferSize := fs.Int64("ws-read-buffer-size", wsReadBufDefault, "per-connection WebSocket read buffer in bytes (DESIGN.md §5.2) (env: "+wsReadBufEnv+")")
	wsWriteBufferSize := fs.Int64("ws-write-buffer-size", wsWriteBufDefault, "WebSocket write buffer in bytes, pooled and held only during a write (DESIGN.md §5.2) (env: "+wsWriteBufEnv+")")
	httpIdleTimeout := fs.Duration("http-idle-timeout", httpIdleDefault, "how long an idle HTTP keep-alive connection is kept open for the next request (DESIGN.md §2.2) (env: "+httpIdleEnv+")")
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}
	if *httpIdleTimeout <= 0 {
		fmt.Fprintln(opts.Out, "--http-idle-timeout must be positive")
		return 2
	}
	if *publishLingerMin < 0 {
		fmt.Fprintln(opts.Out, "--publish-linger-min must not be negative")
		return 2
	}
	if *channelIdleTimeout < 0 {
		fmt.Fprintln(opts.Out, "--channel-idle-timeout must not be negative")
		return 2
	}
	if *connOutboundMaxBytes <= 0 || *connWriteTimeout <= 0 || *wsReadBufferSize <= 0 || *wsWriteBufferSize <= 0 {
		fmt.Fprintln(opts.Out, "--conn-outbound-max-bytes, --conn-write-timeout, --ws-read-buffer-size and --ws-write-buffer-size must be positive")
		return 2
	}

	logger, err := newLogger(*logLevel, *logFormat, opts.Out)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	keySpecs := resolveAPIKeys([]string(keysFlags), opts.Getenv(keysEnv), file)
	if len(keySpecs) == 0 {
		logger.Error("at least one api key is required", "flag", "--keys", "env", keysEnv)
		return 1
	}
	parsedKeys := make([]auth.APIKey, 0, len(keySpecs))
	for _, spec := range keySpecs {
		k, err := auth.ParseAPIKeyWithCapability(spec.key, spec.capability)
		if err != nil {
			logger.Error("invalid api key", "err", err)
			return 1
		}
		parsedKeys = append(parsedKeys, k)
	}
	// All keys must belong to the same app: the server owns one channel
	// namespace, so keys spanning multiple appIds are a misconfiguration
	// (DESIGN.md §3).
	appID := parsedKeys[0].AppID
	for _, k := range parsedKeys[1:] {
		if k.AppID != appID {
			logger.Error("all api keys must share the same appId", "appId", appID, "conflicting", k.AppID)
			return 1
		}
	}

	// OpenTelemetry tracing is off unless the standard OTEL_* env asks for
	// it (DESIGN.md §10). Setup uses a background context so a SIGTERM
	// cancelling ctx does not tear the exporter down before graceful
	// shutdown flushes it. When disabled this installs no exporter and
	// starts no goroutine.
	tp, err := tracing.Setup(context.Background(), opts.Getenv)
	if err != nil {
		logger.Error("setup tracing", "err", err)
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "err", err)
		}
	}()
	// Only hand the servers a tracer when export is on, so the disabled
	// path skips span creation (and its context allocations) entirely.
	var tracer trace.Tracer
	if tp.Enabled {
		tracer = tp.Tracer
		logger.Info("tracing enabled")
	}

	store, err := openStorage(ctx, *mode, *dataDir, clusterOptions{
		dsn:              *postgresDSN,
		bus:              *bus,
		natsURL:          *natsURL,
		natsInlineMax:    *natsInlineMax,
		notifyMode:       *pgNotifyMode,
		notifyWindow:     *pgNotifyWindow,
		notifyMaxPending: *pgNotifyMaxPending,
		sweepInterval:    *busSweep,
		sweepScope:       *busSweepScope,
		logger:           logger,
		retention: postgres.Retention{
			Message:   *messageRetention,
			Persisted: *persistedRetention,
		},
		persisted:   persistedNamespaces(file.Namespaces),
		bindOnWrite: *publishBindOnWrite,
		batching: postgres.Batching{
			Lanes:     *publishLanes,
			BatchMax:  *publishBatchMax,
			LingerMax: *publishLingerMax,
			LingerMin: *publishLingerMin,
			QueueMax:  *publishQueueMax,
		},
	})
	if err != nil {
		logger.Error("open storage", "mode", *mode, "err", err)
		return 1
	}
	// Deferred so it fires after the graceful-shutdown block below
	// (srv.Shutdown drains in-flight HTTP requests, then this defer
	// closes the LISTEN goroutine + the pool via
	// postgres.Storage.Close).
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close storage", "err", err)
		}
	}()
	m := metrics.New()
	if bs, ok := store.(storage.BusStatser); ok {
		st := bs.BusStats()
		shards := 1
		if sh, ok := store.(interface{ Shards() int }); ok {
			shards = sh.Shards()
		}
		logger.Info("storage ready", "mode", *mode, "bus", st.Bus, "postgresNotifyMode", st.Mode, "shards", shards)
		m.RegisterBus(bs) // ably_bus_* series (DESIGN.md §7.2, §10)
	} else {
		logger.Info("storage ready", "mode", *mode)
	}
	// Backends with their own series (the Postgres retention sweep,
	// DESIGN.md §10) register them on the process registry.
	if c, ok := store.(interface{ Collectors() []prometheus.Collector }); ok {
		m.Register(c.Collectors()...)
	}

	manager := core.NewManagerWithOptions(store, core.Options{
		IdleTimeout:      *channelIdleTimeout,
		Metrics:          m,
		Logger:           logger,
		WriteOnlyPublish: !*publishBindOnWrite,
	})
	// Deferred after the storage close, so it runs first: the eviction
	// sweeper stops before the storage it releases into is closed.
	defer manager.Close()

	// Pre-seed presence fixtures declared in the config file before
	// serving traffic (DESIGN.md §9, §12.5). Malformed sections are a
	// startup error.
	spec, err := fixtureSpec(file)
	if err != nil {
		logger.Error("invalid config fixtures", "err", err)
		return 1
	}
	if spec != nil {
		if err := fixtures.Seed(ctx, manager, spec, logger); err != nil {
			logger.Error("seed fixtures", "err", err)
			return 1
		}
	}

	rt := realtime.NewServer(parsedKeys, manager, *hbInterval, logger, m, tracer)
	rt.SetRemainPresentFor(*remainPresentFor)
	rt.SetConnLimits(realtime.ConnLimits{
		OutboundMaxBytes: *connOutboundMaxBytes,
		WriteTimeout:     *connWriteTimeout,
		ReadBufferSize:   int(*wsReadBufferSize),
		WriteBufferSize:  int(*wsWriteBufferSize),
	})
	// ready is non-nil only for backends with an external dependency
	// worth probing (currently postgres.Storage); memory/disk leave it
	// nil and /readyz reports 200 unconditionally.
	ready, _ := store.(storage.Pinger)
	// rt resolves a REST publish's connectionKey to a live connection for
	// publish-on-behalf (DESIGN.md §13); a single-node, in-process registry.
	rs := rest.NewServer(parsedKeys, manager, logger, ready, m, tracer, rt)

	mux := newMux(rt, rs, m, *enableStatsStub)

	// When tracing is enabled, otelhttp wraps the whole mux so every HTTP
	// request (including the REST handlers) gets a server span; the WS
	// upgrade request's span then spans the connection handler too. When
	// disabled the mux is served directly with no wrapping overhead.
	var handler http.Handler = mux
	if tp.Enabled {
		handler = otelhttp.NewHandler(mux, "http.server")
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// Keep-alive (DESIGN.md §2.2): REST publishers reuse connections;
		// an idle one is closed after this long.
		IdleTimeout: *httpIdleTimeout,
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("failed to listen", "addr", *listen, "err", err)
		return 1
	}

	// Publish the bound address so a parent process (the sandbox
	// provisioner, DESIGN.md §15) can discover the port a `--listen
	// 127.0.0.1:0` bind resolved to. Written atomically so a reader
	// polling the path never observes a partial address.
	if *addrFile != "" {
		if err := writeAddrFile(*addrFile, listener.Addr().String()); err != nil {
			logger.Error("failed to write addr-file", "path", *addrFile, "err", err)
			_ = listener.Close()
			return 1
		}
	}

	if opts.Ready != nil {
		select {
		case opts.Ready <- listener.Addr():
		case <-ctx.Done():
			_ = listener.Close()
			return 1
		}
	}

	go func() {
		logger.Info("listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve error", "err", err)
		}
	}()

	// debugSrv is non-nil only when --debug-listen is set; it serves
	// net/http/pprof's handlers plus /metrics (newDebugMux) on a separate
	// address so neither is ever reachable via the main listener.
	var debugSrv *http.Server
	if *debugListen != "" {
		debugListener, err := net.Listen("tcp", *debugListen)
		if err != nil {
			logger.Error("failed to listen on debug address", "addr", *debugListen, "err", err)
			return 1
		}
		if opts.DebugReady != nil {
			select {
			case opts.DebugReady <- debugListener.Addr():
			case <-ctx.Done():
				_ = debugListener.Close()
				return 1
			}
		}
		debugSrv = &http.Server{Handler: newDebugMux(m), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			logger.Info("debug listening", "addr", debugListener.Addr().String())
			if err := debugSrv.Serve(debugListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("debug serve error", "err", err)
			}
		}()
	}

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
	if debugSrv != nil {
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("debug shutdown error", "err", err)
		}
	}
	// srv.Shutdown stops accepting new connections and drains in-flight
	// HTTP handlers — but a WebSocket handler blocks in the connection
	// loop until its socket closes, so it would otherwise hang until the
	// deadline. Run it concurrently with rt.Shutdown, which sends
	// DISCONNECTED to every live WebSocket and paces the closures across
	// the grace window (DESIGN.md §11), unblocking those handlers.
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.Shutdown(shutdownCtx) }()
	rt.Shutdown(shutdownCtx)
	if err := <-srvErr; err != nil {
		logger.Error("shutdown error", "err", err)
		return 1
	}
	return 0
}

// multiFlag collects a repeatable string flag (each --keys occurrence
// appends one value), so multiple keys can be configured on the command
// line (DESIGN.md §3, §9).
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// keySpec is one resolved API key: its Ably-format spec plus an optional
// per-key capability (an x-ably-capability-format JSON string, empty for
// full capability) — DESIGN.md §3.1, §9.
type keySpec struct {
	key        string
	capability string
}

// resolveAPIKeys resolves the configured API keys with precedence
// flag > env > file, applied as whole sets (DESIGN.md §3, §9): repeated
// --keys flags win outright; otherwise a comma-separated ABLY_SERVER_KEYS;
// otherwise the config file's structured [[keys]] entries. Flag/env keys
// are always full-capability; only [[keys]] entries may carry a narrowing
// capability. Whitespace around each spec is trimmed and empty entries
// dropped.
func resolveAPIKeys(flagKeys []string, env string, file config.File) []keySpec {
	if len(flagKeys) > 0 {
		return fullCapSpecs(splitTrim(flagKeys))
	}
	if env != "" {
		return fullCapSpecs(splitTrim(strings.Split(env, ",")))
	}
	specs := make([]keySpec, 0, len(file.Keys))
	for _, e := range file.Keys {
		if s := strings.TrimSpace(e.Key); s != "" {
			specs = append(specs, keySpec{key: s, capability: e.Capability})
		}
	}
	return specs
}

// fullCapSpecs wraps bare key strings as full-capability keySpecs.
func fullCapSpecs(keys []string) []keySpec {
	out := make([]keySpec, 0, len(keys))
	for _, k := range keys {
		out = append(out, keySpec{key: k})
	}
	return out
}

// splitTrim trims whitespace from each entry and drops empties.
func splitTrim(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// persistedNamespaces returns the retention-class resolver for the
// Postgres backend (DESIGN.md §6.3, §9): a channel is persisted when the
// namespace its name starts with (the part before the first ':') is a
// [[namespaces]] entry with persisted = true.
func persistedNamespaces(namespaces []config.Namespace) func(string) bool {
	set := make(map[string]bool)
	for _, ns := range namespaces {
		if ns.Persisted && ns.ID != "" {
			set[ns.ID] = true
		}
	}
	return func(channel string) bool {
		ns, _, ok := strings.Cut(channel, ":")
		return ok && set[ns]
	}
}

// fixtureSpec validates the config file's [[namespaces]] and [[channels]]
// sections and builds the presence-fixture spec to seed at startup
// (DESIGN.md §9, §12.5). Namespaces are only validated here; their
// persisted flag is applied by persistedNamespaces. A namespace with no id, a
// channel with no name, or a presence member with no clientId is a
// malformed section and returns an error. Returns a nil spec when no
// channels are declared.
func fixtureSpec(file config.File) (*fixtures.Spec, error) {
	for i, ns := range file.Namespaces {
		if ns.ID == "" {
			return nil, fmt.Errorf("namespace #%d has no id", i)
		}
	}
	if len(file.Channels) == 0 {
		return nil, nil
	}
	spec := &fixtures.Spec{Channels: make([]fixtures.Channel, 0, len(file.Channels))}
	for i, ch := range file.Channels {
		if ch.Name == "" {
			return nil, fmt.Errorf("channel #%d has no name", i)
		}
		members := make([]fixtures.Member, 0, len(ch.Presence))
		for mi, m := range ch.Presence {
			if m.ClientID == "" {
				return nil, fmt.Errorf("channel %q presence member #%d has no clientId", ch.Name, mi)
			}
			members = append(members, fixtures.Member{
				ClientID: m.ClientID,
				Data:     m.Data,
				Encoding: m.Encoding,
			})
		}
		spec.Channels = append(spec.Channels, fixtures.Channel{Name: ch.Name, Presence: members})
	}
	return spec, nil
}

// writeAddrFile atomically writes addr to path. It writes to a temp file
// in the same directory and renames it into place, so a parent process
// polling the path reads a complete address rather than a truncated one.
func writeAddrFile(path, addr string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".addr-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.WriteString(addr); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

// openStorage constructs the storage.Storage selected by mode:
//
//   - memory: in-process, no persistence.
//   - disk:   bbolt at <dataDir>/ably.db (dataDir created if absent).
//   - cluster: postgres at cluster.dsn (auto-migrates schema on Open),
//     with cluster.bus as the cross-node bus: pgnotify (the shipped
//     LISTEN/NOTIFY broker, the default), postgres or nats (DESIGN.md
//     §7.2).
//
// ctx bounds the cluster-mode dial + ping + migrate; it's ignored by
// the in-process modes.
// newMux builds the HTTP routing table. The WebSocket endpoint is bound to
// the exact root with the `{$}` anchor: a bare `GET /` is a catch-all in
// Go 1.22's ServeMux and would feed every unmatched GET path to the
// upgrader (returning a confusing 400 with WebSocket headers). With `{$}`,
// only `/` upgrades and unknown paths fall through to a clean 404.
func newMux(rt *realtime.Server, rs *rest.Server, m *metrics.Metrics, enableStatsStub bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", rt.HandleWebSocket)

	// REST routes are wrapped so each records ably_http_requests_total by
	// route pattern / method / status (DESIGN.md §10). The WebSocket route
	// is excluded — its handler blocks for the connection's whole lifetime,
	// which the connection metrics already cover. /metrics itself is
	// unwrapped so scrapes don't inflate the counters.
	rest := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, instrumentHTTP(m, pattern, h))
	}
	rest("POST /channels/{name}/messages", rs.HandlePublish)
	rest("GET /channels/{name}/messages", rs.HandleHistory)
	// Ably SDKs' REST history reads request /history; serve it as
	// an alias so those reads work.
	rest("GET /channels/{name}/history", rs.HandleHistory)
	rest("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	rest("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	rest("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	rest("POST /channels/{name}/messages/{serial}/annotations", rs.HandlePublishAnnotation)
	rest("GET /channels/{name}/messages/{serial}/annotations", rs.HandleListAnnotations)
	rest("GET /channels/{name}/presence", rs.HandlePresence)
	rest("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	rest("POST /keys/{keyName}/requestToken", rs.HandleRequestToken)
	// GET/POST /stats are a compatibility stub (DESIGN.md §1) only needed by
	// SDK test flows — the sandbox provisioner boots its children with
	// --enable-stats-stub (§15). Unregistered by default, they fall
	// through to the catch-all Ably-shaped 40400 below rather than the
	// stub always answering an app that never asked for it.
	if enableStatsStub {
		rest("GET /stats", rs.HandleStats)
		// POST /stats is a no-op stats-injection stub: SDK test flows write
		// stats before reading them, and a 404 would leave the SDK blocked
		// reading the error body of a request it never fully sent.
		rest("POST /stats", rs.HandlePostStats)
	}
	rest("GET /time", rs.HandleTime)
	rest("GET /healthz", rs.HandleHealthz)
	rest("GET /readyz", rs.HandleReadyz)

	// Catch-all fallback: any path/method not matched above gets an
	// Ably-shaped 404 (code 40400) instead of ServeMux's bare 404, and —
	// since a subtree "/" pattern also matches paths whose only registered
	// method differs — the 405 Go would otherwise return (e.g. GET on the
	// POST-only requestToken) becomes the 404 SDKs expect (DESIGN.md §2.2).
	// "GET /{$}" stays more specific, so the WebSocket root is unaffected.
	rest("/", rs.HandleNotFound)

	// /metrics is deliberately not registered here: it moved to the debug
	// listener (newDebugMux, --debug-listen) so a publicly reachable main
	// listener doesn't leak operational detail to anyone
	// (DESIGN.md §10). An unmatched GET /metrics on the main mux falls
	// through to the catch-all above and gets the Ably-shaped 404.
	return mux
}

// newDebugMux builds the routing table served on the --debug-listen
// address: net/http/pprof's handlers (registered on http.DefaultServeMux
// by this file's blank import) via a catch-all delegate, plus /metrics
// registered directly and unwrapped — no instrumentHTTP — so scraping it
// doesn't inflate ably_http_requests_total (DESIGN.md §10). m is nil only
// in tests that pass no Metrics; production always wires one.
func newDebugMux(m *metrics.Metrics) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", http.DefaultServeMux)
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}
	return mux
}

// instrumentHTTP wraps a REST handler so it records the served request
// against ably_http_requests_total, labelled by the route pattern (kept
// low-cardinality by using the pattern, not the concrete path), method,
// and response status.
func instrumentHTTP(m *metrics.Metrics, pattern string, h http.HandlerFunc) http.HandlerFunc {
	route := routePath(pattern)
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		m.HTTPRequest(route, r.Method, rec.status)
	}
}

// routePath strips the leading "METHOD " from a ServeMux pattern, leaving
// the path template used as the metric's route label.
func routePath(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

// statusRecorder captures the response status code written by a handler
// so the HTTP metrics middleware can label by it. A handler that never
// calls WriteHeader leaves the default 200.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

// clusterOptions carries the cluster-mode storage settings: the Postgres
// DSN and the cross-node bus (DESIGN.md §7.2).
type clusterOptions struct {
	dsn              string
	bus              string
	natsURL          string
	natsInlineMax    int
	notifyMode       string
	notifyWindow     time.Duration
	notifyMaxPending int
	sweepInterval    time.Duration
	sweepScope       string
	logger           *logging.Logger
	retention        postgres.Retention        // log retention classes (DESIGN.md §6.3)
	batching         postgres.Batching         // publish batching (DESIGN.md §6.3)
	persisted        func(channel string) bool // persisted-namespace resolver
	bindOnWrite      bool                      // --publish-bind-on-write (DESIGN.md §6.3)
}

// options returns the postgres.Options every bus shares.
func (c clusterOptions) options() postgres.Options {
	return postgres.Options{
		DSN:         c.dsn,
		Logger:      c.logger,
		Retention:   c.retention,
		Persisted:   c.persisted,
		Batching:    c.batching,
		BindOnWrite: c.bindOnWrite,
	}
}

func openStorage(ctx context.Context, mode, dataDir string, cluster clusterOptions) (storage.Storage, error) {
	switch mode {
	case "memory":
		return memory.New(memory.Options{}), nil
	case "disk":
		if dataDir == "" {
			return nil, errors.New("--data-dir is required when --mode=disk")
		}
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("create data-dir %q: %w", dataDir, err)
		}
		return bbolt.Open(bbolt.Options{Path: filepath.Join(dataDir, "ably.db")})
	case "cluster":
		if cluster.dsn == "" {
			return nil, fmt.Errorf("--postgres-dsn is required when --mode=cluster (env: %s)", postgresDSNEnv)
		}
		if _, err := postgres.ParseSweepScope(cluster.sweepScope); err != nil {
			return nil, fmt.Errorf("invalid --bus-sweep-scope: %w", err)
		}
		switch cluster.bus {
		case postgres.BusPGNotify:
			// The default, opened exactly as before the bus seam: no bus
			// settings apply.
			return openPostgres(ctx, cluster.options())
		case postgres.BusPostgres:
			mode, err := postgres.ParseNotifyMode(cluster.notifyMode)
			if err != nil {
				return nil, fmt.Errorf("invalid --postgres-notify-mode: %w", err)
			}
			opts := cluster.options()
			opts.Bus = postgres.BusPostgres
			opts.NotifyMode = mode
			opts.NotifyWindow = cluster.notifyWindow
			opts.NotifyMaxPending = cluster.notifyMaxPending
			if msg := postgres.NotifyWindowWarning(cluster.notifyWindow); msg != "" && mode == postgres.NotifyCoalesced && cluster.logger != nil {
				cluster.logger.Warn(msg)
			}
			opts.SweepInterval = cluster.sweepInterval
			opts.SweepScope = cluster.sweepScope
			return openPostgres(ctx, opts)
		case postgres.BusNATS:
			if cluster.natsURL == "" {
				return nil, fmt.Errorf("--nats-url is required when --bus=nats (env: %s)", natsURLEnv)
			}
			opts := cluster.options()
			opts.Bus = postgres.BusNATS
			opts.NATSURL = cluster.natsURL
			opts.NATSInlineMaxBytes = cluster.natsInlineMax
			opts.SweepInterval = cluster.sweepInterval
			opts.SweepScope = cluster.sweepScope
			return openPostgres(ctx, opts)
		default:
			return nil, fmt.Errorf("unknown --bus %q (valid: %s, %s, %s)", cluster.bus, postgres.BusPGNotify, postgres.BusPostgres, postgres.BusNATS)
		}
	default:
		return nil, fmt.Errorf("unknown --mode %q (valid: memory, disk, cluster)", mode)
	}
}

// openPostgres opens the cluster-mode store for opts.DSN, the
// --postgres-dsn value. One DSN opens a plain postgres.Storage, exactly
// as before sharding; a list of two or more opens one Storage per DSN
// behind postgres.Sharded, which routes each channel to its shard by
// hash (DESIGN.md §6.4).
func openPostgres(ctx context.Context, opts postgres.Options) (storage.Storage, error) {
	dsns, err := postgres.SplitDSNs(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid --postgres-dsn: %w", err)
	}
	if len(dsns) == 1 {
		return postgres.Open(ctx, opts)
	}
	return postgres.OpenSharded(ctx, opts, dsns)
}

// newLogger builds the process logger. level is parsed by
// logging.ParseLevel (trace..error); format selects the slog handler:
// "text" (the default) or "json". Either being unrecognised is a
// startup error.
func newLogger(level, format string, w io.Writer) (*logging.Logger, error) {
	lvl, err := logging.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: logging.ReplaceAttr}
	switch format {
	case "text":
		return logging.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return logging.New(slog.NewJSONHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("unknown --log-format %q (valid: text, json)", format)
	}
}
