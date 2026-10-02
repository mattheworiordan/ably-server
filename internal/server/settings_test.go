package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/storage/postgres"
)

// settingsBase are the flags every resolution test starts from: one key
// and cluster mode on a DSN nothing dials (resolveSettings and
// postgresOptions open nothing).
var settingsBase = []string{"--keys=app.key:secret", "--mode=cluster"}

const settingsDSN = "postgres://u:p@127.0.0.1:1/db?sslmode=disable"

// resolveFor resolves settings from args, env and a config file body
// (empty for none), then the cluster storage options, failing the test
// if settings do not resolve. It returns the settings, the options,
// everything logged, and postgresOptions' error.
func resolveFor(t *testing.T, args []string, env map[string]string, toml string) (*settings, postgres.Options, string, error) {
	t.Helper()
	args = append(append([]string{}, settingsBase...), args...)
	if toml != "" {
		args = append(args, "--config="+writeConfigFile(t, toml))
	}
	var out bytes.Buffer
	s, code := resolveSettings(Opts{Args: args, Getenv: envWith(env), Out: &out})
	if s == nil {
		t.Fatalf("resolveSettings(%q, env %v, file %q) failed with exit %d: %s", args, env, toml, code, out.String())
	}
	opts, err := s.cluster.postgresOptions()
	return s, opts, out.String(), err
}

// TestBusResolution (DESIGN.md §7.2, §9): "start with one Postgres; add
// NATS when you need it". For every way of giving --bus (unset, flag,
// env, config file) and value, with and without a NATS URL, the bus
// resolves as the rule says: unset is nats with a NATS URL and postgres
// without one; an explicit bus wins; pgnotify is never inferred and
// logs its ceiling when asked for; nats without a NATS URL is a startup
// error naming the trade-off; a NATS URL the chosen bus does not use is
// named in a warning.
func TestBusResolution(t *testing.T) {
	const url = "nats://127.0.0.1:4222"
	type source struct {
		name string
		set  func(key, val string) (args []string, env map[string]string, toml string)
	}
	sources := []source{
		{"flag", func(key, val string) ([]string, map[string]string, string) {
			return []string{"--" + key + "=" + val}, nil, ""
		}},
		{"env", func(key, val string) ([]string, map[string]string, string) {
			name := map[string]string{"bus": busEnv, "nats-url": natsURLEnv}[key]
			return nil, map[string]string{name: val}, ""
		}},
		{"file", func(key, val string) ([]string, map[string]string, string) {
			return nil, nil, fmt.Sprintf("%s = %q\n", key, val)
		}},
	}
	for _, withURL := range []bool{false, true} {
		for _, busSrc := range append([]source{{name: "unset"}}, sources...) {
			buses := []string{"pgnotify", "postgres", "nats"}
			if busSrc.set == nil {
				buses = []string{""}
			}
			for _, bus := range buses {
				name := fmt.Sprintf("bus=%s(%s)/natsURL=%v", bus, busSrc.name, withURL)
				t.Run(name, func(t *testing.T) {
					args := []string{"--postgres-dsn=" + settingsDSN}
					env := map[string]string{}
					var toml string
					if busSrc.set != nil {
						a, e, f := busSrc.set("bus", bus)
						args, toml = append(args, a...), toml+f
						for k, v := range e {
							env[k] = v
						}
					}
					if withURL {
						// The NATS URL from a different source from the
						// bus, so the sources mix as they would in use.
						urlSrc := sources[(len(name))%len(sources)]
						a, e, f := urlSrc.set("nats-url", url)
						args, toml = append(args, a...), toml+f
						for k, v := range e {
							env[k] = v
						}
					}
					_, opts, logged, err := resolveFor(t, args, env, toml)

					wantBus := bus
					switch {
					case bus == "" && withURL:
						wantBus = postgres.BusNATS
					case bus == "":
						wantBus = postgres.BusPostgres
					}
					if bus == postgres.BusNATS && !withURL {
						if err == nil || !strings.Contains(err.Error(), "--bus=nats needs --nats-url") || !strings.Contains(err.Error(), "--bus=postgres") {
							t.Fatalf("err = %v, want the nats-needs-a-URL error naming the trade-off", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("postgresOptions: %v", err)
					}
					if opts.Bus != wantBus {
						t.Errorf("bus = %q, want %q", opts.Bus, wantBus)
					}
					// An inferred bus is flagged, so a database that served
					// an earlier version refuses it (DESIGN.md §11).
					if opts.BusInferred != (bus == "") {
						t.Errorf("BusInferred = %v, want %v", opts.BusInferred, bus == "")
					}
					if withURL && wantBus == postgres.BusNATS && opts.NATSURL != url {
						t.Errorf("NATSURL = %q, want %q", opts.NATSURL, url)
					}
					ceiling := strings.Contains(logged, "--bus=pgnotify has a measured ceiling")
					if ceiling != (wantBus == postgres.BusPGNotify) {
						t.Errorf("pgnotify ceiling warning logged = %v, want %v: %s", ceiling, wantBus == postgres.BusPGNotify, logged)
					}
					unused := strings.Contains(logged, "bus setting ignored") && strings.Contains(logged, "flag=--nats-url")
					if want := withURL && wantBus != postgres.BusNATS; unused != want {
						t.Errorf("NATS-configured-but-unused warning logged = %v, want %v: %s", unused, want, logged)
					}
				})
			}
		}
	}
}

// TestSettingsReachOptions: every setting, given a
// valid non-default value by flag, by env var and by config file key,
// reaches the options the server builds its storage, core and realtime
// layers from (postgres.Options, core.Options, realtime limits), through
// the same assembly Run uses. A setting that parsed but never reached
// the code would fail here.
func TestSettingsReachOptions(t *testing.T) {
	type tc struct {
		key  string // flag name and TOML key
		env  string // env var
		val  string // flag and env value
		toml string // TOML value literal
		args []string
		got  func(s *settings, o postgres.Options) any
		want any
	}
	nats := []string{"--nats-url=nats://127.0.0.1:4222"}
	cases := []tc{
		{key: "postgres-dsn", env: postgresDSNEnv, val: "postgres://a@h1/d,postgres://a@h2/d", toml: `"postgres://a@h1/d,postgres://a@h2/d"`,
			got: func(_ *settings, o postgres.Options) any { return o.DSN }, want: "postgres://a@h1/d,postgres://a@h2/d"},
		{key: "bus", env: busEnv, val: "pgnotify", toml: `"pgnotify"`,
			got: func(_ *settings, o postgres.Options) any { return o.Bus }, want: postgres.BusPGNotify},
		{key: "nats-url", env: natsURLEnv, val: "nats://n1:4222,nats://n2:4222", toml: `"nats://n1:4222,nats://n2:4222"`,
			got: func(_ *settings, o postgres.Options) any { return o.NATSURL }, want: "nats://n1:4222,nats://n2:4222"},
		{key: "nats-inline-max-bytes", env: natsInlineMaxEnv, val: "1234", toml: `1234`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.NATSInlineMaxBytes }, want: 1234},
		{key: "nats-creds", env: natsCredsEnv, val: "/etc/nats/user.creds", toml: `"/etc/nats/user.creds"`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.NATSCredsFile }, want: "/etc/nats/user.creds"},
		{key: "nats-tls-ca", env: natsTLSCAEnv, val: "/etc/nats/ca.pem", toml: `"/etc/nats/ca.pem"`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.NATSTLSCA }, want: "/etc/nats/ca.pem"},
		{key: "nats-tls-cert", env: natsTLSCertEnv, val: "/etc/nats/c.pem", toml: `"/etc/nats/c.pem"`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.NATSTLSCert }, want: "/etc/nats/c.pem"},
		{key: "nats-tls-key", env: natsTLSKeyEnv, val: "/etc/nats/k.pem", toml: `"/etc/nats/k.pem"`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.NATSTLSKey }, want: "/etc/nats/k.pem"},
		{key: "postgres-notify-mode", env: pgNotifyModeEnv, val: "transactional", toml: `"transactional"`,
			got: func(_ *settings, o postgres.Options) any { return o.NotifyMode }, want: postgres.NotifyTransactional},
		{key: "postgres-notify-window", env: pgNotifyWindowEnv, val: "75ms", toml: `"75ms"`,
			got: func(_ *settings, o postgres.Options) any { return o.NotifyWindow }, want: 75 * time.Millisecond},
		{key: "postgres-notify-max-pending", env: pgNotifyMaxPendEnv, val: "99", toml: `99`,
			got: func(_ *settings, o postgres.Options) any { return o.NotifyMaxPending }, want: 99},
		{key: "bus-sweep-interval", env: busSweepEnv, val: "7s", toml: `"7s"`,
			got: func(_ *settings, o postgres.Options) any { return o.SweepInterval }, want: 7 * time.Second},
		{key: "bus-sweep-interval", env: busSweepEnv, val: "8s", toml: `"8s"`, args: nats,
			got: func(_ *settings, o postgres.Options) any { return o.SweepInterval }, want: 8 * time.Second},
		{key: "message-retention", env: messageRetentionEnv, val: "3m", toml: `"3m"`,
			got: func(_ *settings, o postgres.Options) any { return o.Retention.Message }, want: 3 * time.Minute},
		{key: "persisted-retention", env: persistedRetentionEnv, val: "48h", toml: `"48h"`,
			got: func(_ *settings, o postgres.Options) any { return o.Retention.Persisted }, want: 48 * time.Hour},
		{key: "publish-lanes", env: publishLanesEnv, val: "3", toml: `3`,
			got: func(_ *settings, o postgres.Options) any { return o.Batching.Lanes }, want: 3},
		{key: "publish-batch-max", env: publishBatchMaxEnv, val: "50", toml: `50`,
			got: func(_ *settings, o postgres.Options) any { return o.Batching.BatchMax }, want: 50},
		{key: "publish-linger-max", env: publishLingerMaxEnv, val: "9ms", toml: `"9ms"`,
			got: func(_ *settings, o postgres.Options) any { return o.Batching.LingerMax }, want: 9 * time.Millisecond},
		{key: "publish-queue-max", env: publishQueueMaxEnv, val: "77", toml: `77`,
			got: func(_ *settings, o postgres.Options) any { return o.Batching.QueueMax }, want: 77},
		{key: "presence-max-inflight", env: presenceMaxInflightEnv, val: "5", toml: `5`,
			got: func(_ *settings, o postgres.Options) any { return o.PresenceMaxInflight }, want: 5},
		{key: "channel-idle-timeout", env: channelIdleEnv, val: "90s", toml: `"90s"`,
			got: func(s *settings, _ postgres.Options) any { return s.core.IdleTimeout }, want: 90 * time.Second},
		{key: "conn-outbound-max-bytes", env: connOutboundEnv, val: "2048", toml: `2048`,
			got: func(s *settings, _ postgres.Options) any { return s.connLimits.OutboundMaxBytes }, want: int64(2048)},
		{key: "conn-write-timeout", env: connWriteTOEnv, val: "3s", toml: `"3s"`,
			got: func(s *settings, _ postgres.Options) any { return s.connLimits.WriteTimeout }, want: 3 * time.Second},
		{key: "ws-read-buffer-size", env: wsReadBufEnv, val: "2048", toml: `2048`,
			got: func(s *settings, _ postgres.Options) any { return s.connLimits.ReadBufferSize }, want: 2048},
		{key: "ws-write-buffer-size", env: wsWriteBufEnv, val: "8192", toml: `8192`,
			got: func(s *settings, _ postgres.Options) any { return s.connLimits.WriteBufferSize }, want: 8192},
		{key: "attachment-seen-max", env: attachSeenMaxEnv, val: "100", toml: `100`,
			got: func(s *settings, _ postgres.Options) any { return s.appendTracking.SeenMax }, want: 100},
		{key: "delivery-fanout-pool", env: fanoutPoolEnv, val: "3", toml: `3`,
			got: func(s *settings, _ postgres.Options) any { return s.fanoutWorkers }, want: 3},
		{key: "delivery-fanout-pool", env: fanoutPoolEnv, val: "0", toml: `0`,
			got: func(s *settings, _ postgres.Options) any { return s.fanoutWorkers }, want: 0},
		{key: "delivery-fanout-threshold", env: fanoutThresholdEnv, val: "500", toml: `500`,
			got: func(s *settings, _ postgres.Options) any { return s.fanoutThreshold }, want: 500},
		{key: "http-idle-timeout", env: httpIdleEnv, val: "30s", toml: `"30s"`,
			got: func(s *settings, _ postgres.Options) any { return s.httpIdleTimeout }, want: 30 * time.Second},
		{key: "shutdown-grace", env: shutdownGraceEnv, val: "3s", toml: `"3s"`,
			got: func(s *settings, _ postgres.Options) any { return s.shutdownGrace }, want: 3 * time.Second},
		{key: "enable-stats-stub", env: enableStatsStubEnv, val: "true", toml: `true`,
			got: func(s *settings, _ postgres.Options) any { return s.enableStatsStub }, want: true},
		{key: "listen", env: listenEnv, val: "127.0.0.1:9999", toml: `"127.0.0.1:9999"`,
			got: func(s *settings, _ postgres.Options) any { return s.listen }, want: "127.0.0.1:9999"},
		{key: "debug-listen", env: debugListenEnv, val: "127.0.0.1:6061", toml: `"127.0.0.1:6061"`,
			got: func(s *settings, _ postgres.Options) any { return s.debugListen }, want: "127.0.0.1:6061"},
		{key: "log-level", env: logLevelEnv, val: "debug", toml: `"debug"`,
			got: func(s *settings, _ postgres.Options) any { return s.logLevel }, want: "debug"},
		{key: "log-format", env: logFormatEnv, val: "json", toml: `"json"`,
			got: func(s *settings, _ postgres.Options) any { return s.logFormat }, want: "json"},
		{key: "data-dir", env: dataDirEnv, val: "/var/lib/x", toml: `"/var/lib/x"`,
			got: func(s *settings, _ postgres.Options) any { return s.dataDir }, want: "/var/lib/x"},
	}
	for _, c := range cases {
		for _, src := range []string{"flag", "env", "file"} {
			t.Run(c.key+"/"+src+"/"+c.val, func(t *testing.T) {
				args := append([]string{}, c.args...)
				if c.key != "postgres-dsn" {
					args = append(args, "--postgres-dsn="+settingsDSN)
				}
				var env map[string]string
				var toml string
				switch src {
				case "flag":
					args = append(args, "--"+c.key+"="+c.val)
				case "env":
					env = map[string]string{c.env: c.val}
				case "file":
					toml = c.key + " = " + c.toml + "\n"
				}
				s, opts, logged, err := resolveFor(t, args, env, toml)
				if err != nil {
					t.Fatalf("postgresOptions: %v (%s)", err, logged)
				}
				if got := c.got(s, opts); got != c.want {
					t.Errorf("%s via %s = %v (%T), want %v (%T)", c.key, src, got, got, c.want, c.want)
				}
			})
		}
	}
}

// TestSettingsDefaults: with nothing set, cluster mode resolves to the
// documented defaults (DESIGN.md §9), so a changed default is a
// deliberate edit here too.
func TestSettingsDefaults(t *testing.T) {
	s, o, _, err := resolveFor(t, []string{"--postgres-dsn=" + settingsDSN}, nil, "")
	if err != nil {
		t.Fatalf("postgresOptions: %v", err)
	}
	for name, c := range map[string]struct{ got, want any }{
		"bus":              {o.Bus, postgres.BusPostgres},
		"notify mode":      {o.NotifyMode, postgres.NotifyCoalesced},
		"publish lanes":    {o.Batching.Lanes, postgres.DefaultPublishLanes},
		"batch max":        {o.Batching.BatchMax, postgres.DefaultPublishBatchMax},
		"linger max":       {o.Batching.LingerMax, postgres.DefaultPublishLingerMax},
		"queue max":        {o.Batching.QueueMax, postgres.DefaultPublishQueueMax},
		"retention":        {o.Retention.Message, postgres.DefaultMessageRetention},
		"presence bound":   {o.PresenceMaxInflight, 0},
		"sweep":            {o.SweepInterval, time.Duration(0)},
		"idle timeout":     {s.core.IdleTimeout, core.DefaultChannelIdleTimeout},
		"http idle":        {s.httpIdleTimeout, DefaultHTTPIdleTimeout},
		"fanout pool":      {s.fanoutWorkers, core.DefaultFanoutWorkers()},
		"fanout threshold": {s.fanoutThreshold, core.DefaultFanoutThreshold},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", name, c.got, c.want)
		}
	}
}

// TestSettingsPersistedNamespaces: the config file's persisted
// namespaces reach the storage options, as the retention-class resolver
// and the ids recorded in the cluster identity (DESIGN.md §6.3, §11).
func TestSettingsPersistedNamespaces(t *testing.T) {
	_, o, _, err := resolveFor(t, []string{"--postgres-dsn=" + settingsDSN}, nil, `
[[namespaces]]
id = "keep"
persisted = true

[[namespaces]]
id = "drop"
`)
	if err != nil {
		t.Fatalf("postgresOptions: %v", err)
	}
	if fmt.Sprint(o.PersistedNamespaces) != "[keep]" {
		t.Errorf("PersistedNamespaces = %v, want [keep]", o.PersistedNamespaces)
	}
	if o.Persisted == nil || !o.Persisted("keep:room") || o.Persisted("drop:room") {
		t.Error("Persisted does not resolve keep:room as persisted and drop:room as not")
	}
}

// TestSettingsMode: --mode reaches the settings by flag, env var and
// file key. Apart from TestSettingsReachOptions, whose cases all run in
// cluster mode by flag, which would win.
func TestSettingsMode(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		env  map[string]string
		toml string
	}{
		{name: "flag", args: []string{"--mode=disk"}},
		{name: "env", env: map[string]string{modeEnv: "disk"}},
		{name: "file", toml: `mode = "disk"`},
	} {
		args := append([]string{"--keys=app.key:secret"}, c.args...)
		if c.toml != "" {
			args = append(args, "--config="+writeConfigFile(t, c.toml))
		}
		var out bytes.Buffer
		s, code := resolveSettings(Opts{Args: args, Getenv: envWith(c.env), Out: &out})
		if s == nil {
			t.Fatalf("%s: resolveSettings failed with exit %d: %s", c.name, code, out.String())
		}
		if s.mode != "disk" {
			t.Errorf("mode via %s = %q, want disk", c.name, s.mode)
		}
	}
}
