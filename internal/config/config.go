// Package config supports ably-server's optional TOML config file
// (--config, DESIGN.md §9). Resolution order across every source is
// flag > env > config file > hardcoded default; File and Default
// exist to let cmd/ably-server seed each flag.String/flag.Duration
// call with the env-then-file-then-default value, so flag.Parse's own
// explicit-flag-wins behaviour produces the full precedence chain
// without extra bookkeeping.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// File is the shape of the optional TOML config file. It covers the
// same keys as the CLI flags that exist today (DESIGN.md §9); every
// field is optional — a zero value means "absent from the file" and
// resolution falls through to the flag's environment variable or
// hardcoded default. ShutdownGrace is kept as the raw string (e.g.
// "10s") since TOML has no duration type; callers parse it with
// DefaultDuration the same way the --shutdown-grace flag's value is
// parsed.
type File struct {
	Mode   string `toml:"mode"`
	Listen string `toml:"listen"`
	// Keys are structured [[keys]] entries: each a key spec plus an
	// optional per-key capability (DESIGN.md §3.1, §9). This is the only
	// file-tier source of API keys; at least one key must be configured
	// across all sources (flag, env, or this).
	Keys          []KeyEntry `toml:"keys"`
	DataDir       string     `toml:"data-dir"`
	PostgresDSN   string     `toml:"postgres-dsn"`
	ShutdownGrace string     `toml:"shutdown-grace"`
	LogLevel      string     `toml:"log-level"`
	LogFormat     string     `toml:"log-format"`
	DebugListen   string     `toml:"debug-listen"`
	// Bus selects cluster mode's cross-node bus, "pgnotify", "postgres"
	// or "nats" (DESIGN.md §7.2); NATSURL and NATSInlineMaxBytes
	// configure the nats bus; the PostgresNotify* keys configure the
	// postgres bus; BusSweepInterval the chaining buses' safety-net
	// sweep. Durations are strings, like ShutdownGrace;
	// a zero int means absent.
	Bus                string `toml:"bus"`
	NATSURL            string `toml:"nats-url"`
	NATSInlineMaxBytes int    `toml:"nats-inline-max-bytes"`
	// NATSCreds, NATSTLSCA, NATSTLSCert and NATSTLSKey authenticate and
	// encrypt the nats bus connection (DESIGN.md §7.2, §9): file paths.
	NATSCreds                string `toml:"nats-creds"`
	NATSTLSCA                string `toml:"nats-tls-ca"`
	NATSTLSCert              string `toml:"nats-tls-cert"`
	NATSTLSKey               string `toml:"nats-tls-key"`
	PostgresNotifyMode       string `toml:"postgres-notify-mode"`
	PostgresNotifyWindow     string `toml:"postgres-notify-window"`
	PostgresNotifyMaxPending int    `toml:"postgres-notify-max-pending"`
	BusSweepInterval         string `toml:"bus-sweep-interval"`
	// MessageRetention and PersistedRetention are duration strings (e.g.
	// "2m", "24h") for the cluster-mode message log's retention classes
	// (DESIGN.md §6.3, §9).
	MessageRetention   string `toml:"message-retention"`
	PersistedRetention string `toml:"persisted-retention"`
	// Publish batching for the cluster-mode write path (DESIGN.md §6.3,
	// §9). Zero means absent; publish-linger-max and publish-linger-min
	// are duration strings.
	PublishLanes     int    `toml:"publish-lanes"`
	PublishBatchMax  int    `toml:"publish-batch-max"`
	PublishLingerMax string `toml:"publish-linger-max"`
	PublishLingerMin string `toml:"publish-linger-min"`
	PublishQueueMax  int    `toml:"publish-queue-max"`
	// The presence path (DESIGN.md §12.4, §12.5, §9): the SYNC source
	// ("local" or "store"), whether presence writes join the publish
	// batches (a pointer, since its default is true and a file must be
	// able to turn it off), the bound on unbatched presence writes
	// in flight (zero means absent), and the liveness lease mode ("node"
	// or "member").
	PresenceSyncSource  string `toml:"presence-sync-source"`
	PresenceBatching    *bool  `toml:"presence-batching"`
	PresenceMaxInflight int    `toml:"presence-max-inflight"`
	PresenceLeaseMode   string `toml:"presence-lease-mode"`
	// EnableStatsStub registers the GET/POST /stats compatibility stub
	// (DESIGN.md §1); absent/false — the zero value — keeps it
	// unregistered, matching the fallback default, so the usual
	// "zero value means absent" convention costs nothing here.
	EnableStatsStub bool `toml:"enable-stats-stub"`
	// ChannelIdleTimeout is the raw duration string for
	// --channel-idle-timeout (DESIGN.md §5.1, §9), parsed like
	// ShutdownGrace.
	ChannelIdleTimeout string `toml:"channel-idle-timeout"`
	// Connection-layer limits (DESIGN.md §5.2, §9). ConnWriteTimeout is a
	// raw duration string; the sizes are byte counts, zero meaning absent.
	ConnOutboundMaxBytes int64  `toml:"conn-outbound-max-bytes"`
	ConnWriteTimeout     string `toml:"conn-write-timeout"`
	WSReadBufferSize     int64  `toml:"ws-read-buffer-size"`
	WSWriteBufferSize    int64  `toml:"ws-write-buffer-size"`
	// AttachmentSeenMax caps the message serials one attachment remembers
	// for append delivery (DESIGN.md §13.3, §9); zero means absent.
	AttachmentSeenMax int `toml:"attachment-seen-max"`
	// HTTPIdleTimeout is the raw duration string for --http-idle-timeout
	// (DESIGN.md §2.2, §9).
	HTTPIdleTimeout string `toml:"http-idle-timeout"`
	// Namespaces are [[namespaces]] entries mirroring the test-app-setup
	// post_apps shape (DESIGN.md §9, §12.5). persisted selects the
	// cluster-mode retention class of the namespace's channels (§6.3);
	// the other flags are recorded but inert.
	Namespaces []Namespace `toml:"namespaces"`
	// Channels are [[channels]] entries whose nested presence members are
	// seeded at startup as static fixtures (DESIGN.md §9, §12.5),
	// replacing the retired --fixtures JSON path.
	Channels []Channel `toml:"channels"`

	// Unknown lists the keys in the file that File does not define, such
	// as a setting that has been removed (DESIGN.md §9 "Removed
	// settings"), so the server can name each in a startup warning
	// rather than ignore it silently. Set by Load, never by the file.
	Unknown []string `toml:"-"`
}

// KeyEntry is one structured [[keys]] entry (DESIGN.md §3.1, §9): an
// Ably-format key spec plus an optional capability. Capability is an
// `x-ably-capability`-format JSON object string; empty means the key
// grants the full capability, matching a --keys flag or env entry.
type KeyEntry struct {
	Key        string `toml:"key"`
	Capability string `toml:"capability"`
}

// Namespace is one [[namespaces]] entry (DESIGN.md §9, §12.5): a
// namespace id plus feature flags mirroring test-app-setup's post_apps
// shape. Persisted selects the retention class in cluster mode (§6.3);
// MutableMessages makes attachments track every delivered message so a
// create's first append is a delta (DESIGN.md §13.3); PushEnabled is
// recorded but inert.
type Namespace struct {
	ID              string `toml:"id"`
	Persisted       bool   `toml:"persisted"`
	MutableMessages bool   `toml:"mutableMessages"`
	PushEnabled     bool   `toml:"pushEnabled"`
}

// Channel is one [[channels]] entry: a channel name plus the presence
// members to seed at startup (DESIGN.md §9, §12.5).
type Channel struct {
	Name     string           `toml:"name"`
	Presence []PresenceMember `toml:"presence"`
}

// PresenceMember is one nested presence entry under a [[channels]] entry.
// Data and Encoding round-trip verbatim — the server treats Encoding as
// opaque and never decodes Data (DESIGN.md §9), so a cipher payload is
// seeded exactly as given.
type PresenceMember struct {
	ClientID string `toml:"clientId"`
	Data     string `toml:"data"`
	Encoding string `toml:"encoding"`
}

// Load parses the TOML file at path into a File. Keys File does not
// define are not an error; they are listed in File.Unknown.
func Load(path string) (*File, error) {
	var f File
	md, err := toml.DecodeFile(path, &f)
	if err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	for _, k := range md.Undecoded() {
		f.Unknown = append(f.Unknown, k.String())
	}
	return &f, nil
}

// PathFromArgs scans args for --config/-config's value, recognising
// both the "=value" and the "next argument" forms the stdlib flag
// package accepts. main needs the config file's path before it can
// define its other flags with config-seeded defaults, and the flag
// package has no way to parse a single flag ahead of the rest, so
// this walks args by hand. It stops at a bare "--", matching flag's
// own terminator convention.
func PathFromArgs(args []string) string {
	for i, a := range args {
		switch {
		case a == "--":
			return ""
		case a == "--config" || a == "-config":
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		case strings.HasPrefix(a, "--config="):
			return strings.TrimPrefix(a, "--config=")
		case strings.HasPrefix(a, "-config="):
			return strings.TrimPrefix(a, "-config=")
		}
	}
	return ""
}

// Default resolves a flag's default value by precedence env > file >
// fallback; whichever of env/file is non-empty and comes first wins.
// The flag itself, if passed explicitly on the command line, is
// applied on top of this by flag.Parse — giving the full flag > env >
// file > default chain.
func Default(env, file, fallback string) string {
	if env != "" {
		return env
	}
	if file != "" {
		return file
	}
	return fallback
}

// DefaultDuration is Default for a time.Duration-valued flag: it
// resolves the env/file/fallback string precedence and then parses
// the winning string. A malformed env or file value is reported as an
// error rather than silently falling back, since that's very likely a
// typo the operator wants to know about at startup.
func DefaultDuration(env, file string, fallback time.Duration) (time.Duration, error) {
	v := Default(env, file, "")
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid duration %q: %w", v, err)
	}
	return d, nil
}

// DefaultInt is Default for an int-valued flag: env, if set, is parsed
// with strconv.Atoi (a malformed value is reported as an error, like
// DefaultDuration); otherwise a non-zero file value wins; otherwise
// fallback. file is a plain int under the File struct's "zero value
// means absent" convention.
func DefaultInt(env string, file int, fallback int) (int, error) {
	if env != "" {
		n, err := strconv.Atoi(env)
		if err != nil {
			return 0, fmt.Errorf("config: invalid integer %q: %w", env, err)
		}
		return n, nil
	}
	if file != 0 {
		return file, nil
	}
	return fallback, nil
}

// DefaultBool is Default for a bool-valued flag: env, if set, is parsed
// with strconv.ParseBool (accepting "true"/"false"/"1"/"0"/etc, reported
// as an error on a malformed value — likely a typo the operator wants to
// know about at startup); otherwise file wins when true; otherwise
// fallback. file is a plain bool rather than Default's string precedence
// chain because the File struct's fields already use "zero value means
// absent from the file" as their convention — which only loses
// information when an option's fallback is true and the file wants to
// override it to false, a case no current option needs.
func DefaultBool(env string, file bool, fallback bool) (bool, error) {
	if env != "" {
		b, err := strconv.ParseBool(env)
		if err != nil {
			return false, fmt.Errorf("config: invalid bool %q: %w", env, err)
		}
		return b, nil
	}
	if file {
		return true, nil
	}
	return fallback, nil
}

// DefaultBoolPtr is DefaultBool for an option whose file value may be
// set to false over a true fallback: file is nil when absent.
func DefaultBoolPtr(env string, file *bool, fallback bool) (bool, error) {
	if env == "" && file != nil {
		return *file, nil
	}
	return DefaultBool(env, false, fallback)
}

// DefaultInt64 is Default for an integer-valued flag: env, if set, is
// parsed as a base-10 int64 (a malformed value is an error); otherwise a
// non-zero file value wins; otherwise fallback. As with DefaultBool, the
// File convention "zero means absent" applies to file.
func DefaultInt64(env string, file int64, fallback int64) (int64, error) {
	if env != "" {
		v, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("config: invalid integer %q: %w", env, err)
		}
		return v, nil
	}
	if file != 0 {
		return file, nil
	}
	return fallback, nil
}
