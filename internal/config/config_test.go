package config

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func writeTOML(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ably-server.toml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func TestLoadParsesAllKeys(t *testing.T) {
	path := writeTOML(t, `
mode = "cluster"
listen = ":9090"
data-dir = "/var/lib/ably"
postgres-dsn = "postgres://user:pw@host:5432/db"
shutdown-grace = "30s"
log-level = "debug"
log-format = "json"
debug-listen = "127.0.0.1:6060"
enable-stats-stub = true

[[keys]]
key = "app.key:secret"
`)

	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := File{
		Mode:            "cluster",
		Listen:          ":9090",
		Keys:            []KeyEntry{{Key: "app.key:secret"}},
		DataDir:         "/var/lib/ably",
		PostgresDSN:     "postgres://user:pw@host:5432/db",
		ShutdownGrace:   "30s",
		LogLevel:        "debug",
		LogFormat:       "json",
		DebugListen:     "127.0.0.1:6060",
		EnableStatsStub: true,
	}
	if !reflect.DeepEqual(*f, want) {
		t.Errorf("Load() = %+v, want %+v", *f, want)
	}
}

func TestLoadParsesStructuredKeys(t *testing.T) {
	path := writeTOML(t, `
[[keys]]
key = "app.sub:secret1"
capability = '{"chat:*":["subscribe"]}'

[[keys]]
key = "app.full:secret2"
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []KeyEntry{
		{Key: "app.sub:secret1", Capability: `{"chat:*":["subscribe"]}`},
		{Key: "app.full:secret2"},
	}
	if !reflect.DeepEqual(f.Keys, want) {
		t.Errorf("Keys = %+v, want %+v", f.Keys, want)
	}
}

func TestLoadParsesNamespacesAndChannels(t *testing.T) {
	path := writeTOML(t, `
[[namespaces]]
id = "persisted"
persisted = true

[[namespaces]]
id = "mutable"
mutableMessages = true

[[channels]]
name = "persisted:presence_fixtures"

  [[channels.presence]]
  clientId = "client_string"
  data = "hello"

  [[channels.presence]]
  clientId = "client_encoded"
  data = "AAAA"
  encoding = "json/utf-8/cipher+aes-128-cbc/base64"
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantNS := []Namespace{
		{ID: "persisted", Persisted: true},
		{ID: "mutable", MutableMessages: true},
	}
	if !reflect.DeepEqual(f.Namespaces, wantNS) {
		t.Errorf("Namespaces = %+v, want %+v", f.Namespaces, wantNS)
	}
	wantCh := []Channel{{
		Name: "persisted:presence_fixtures",
		Presence: []PresenceMember{
			{ClientID: "client_string", Data: "hello"},
			{ClientID: "client_encoded", Data: "AAAA", Encoding: "json/utf-8/cipher+aes-128-cbc/base64"},
		},
	}}
	if !reflect.DeepEqual(f.Channels, wantCh) {
		t.Errorf("Channels = %+v, want %+v", f.Channels, wantCh)
	}
}

func TestLoadPartialFileLeavesOtherFieldsZero(t *testing.T) {
	path := writeTOML(t, `log-format = "json"`)

	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want %q", f.LogFormat, "json")
	}
	if f.Mode != "" || f.Listen != "" || len(f.Keys) != 0 {
		t.Errorf("unset fields should be zero, got %+v", *f)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.toml")); err == nil {
		t.Error("Load(missing file) error = nil, want an error")
	}
}

func TestLoadRejectsMalformedTOML(t *testing.T) {
	path := writeTOML(t, `this is not = = toml`)
	if _, err := Load(path); err == nil {
		t.Error("Load(malformed TOML) error = nil, want a parse error")
	}
}

func TestPathFromArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"absent", []string{"--keys=x"}, ""},
		{"equals form", []string{"--config=/etc/ably-server.toml"}, "/etc/ably-server.toml"},
		{"single dash equals", []string{"-config=/etc/ably.toml"}, "/etc/ably.toml"},
		{"next-arg form", []string{"--config", "/etc/ably.toml", "--keys=x"}, "/etc/ably.toml"},
		{"single dash next-arg", []string{"-config", "/etc/ably.toml"}, "/etc/ably.toml"},
		{"trailing with no value", []string{"--keys=x", "--config"}, ""},
		{"stops at terminator", []string{"--", "--config=/etc/ably.toml"}, ""},
		{"mixed with other flags", []string{"--log-level=debug", "--config=/x.toml", "--mode=cluster"}, "/x.toml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PathFromArgs(tc.args); got != tc.want {
				t.Errorf("PathFromArgs(%v) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestDefault(t *testing.T) {
	cases := []struct {
		name            string
		env, file, back string
		want            string
	}{
		{"env wins", "from-env", "from-file", "fallback", "from-env"},
		{"file wins over fallback", "", "from-file", "fallback", "from-file"},
		{"fallback when both empty", "", "", "fallback", "fallback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Default(tc.env, tc.file, tc.back); got != tc.want {
				t.Errorf("Default(%q, %q, %q) = %q, want %q", tc.env, tc.file, tc.back, got, tc.want)
			}
		})
	}
}

func TestDefaultDuration(t *testing.T) {
	d, err := DefaultDuration("", "", 10*time.Second)
	if err != nil || d != 10*time.Second {
		t.Errorf("DefaultDuration(empty, empty, 10s) = %v, %v; want 10s, nil", d, err)
	}

	d, err = DefaultDuration("", "30s", 10*time.Second)
	if err != nil || d != 30*time.Second {
		t.Errorf("DefaultDuration(empty, 30s, 10s) = %v, %v; want 30s, nil", d, err)
	}

	d, err = DefaultDuration("5s", "30s", 10*time.Second)
	if err != nil || d != 5*time.Second {
		t.Errorf("DefaultDuration(5s, 30s, 10s) = %v, %v; want 5s, nil (env wins)", d, err)
	}

	if _, err := DefaultDuration("", "not-a-duration", 10*time.Second); err == nil {
		t.Error("DefaultDuration with malformed value error = nil, want an error")
	}
}

func TestDefaultBool(t *testing.T) {
	b, err := DefaultBool("", false, false)
	if err != nil || b != false {
		t.Errorf("DefaultBool(empty, false, false) = %v, %v; want false, nil", b, err)
	}

	b, err = DefaultBool("", true, false)
	if err != nil || b != true {
		t.Errorf("DefaultBool(empty, true, false) = %v, %v; want true, nil (file wins over fallback)", b, err)
	}

	b, err = DefaultBool("false", true, false)
	if err != nil || b != false {
		t.Errorf("DefaultBool(false, true, false) = %v, %v; want false, nil (env wins over file)", b, err)
	}

	b, err = DefaultBool("1", false, false)
	if err != nil || b != true {
		t.Errorf("DefaultBool(1, false, false) = %v, %v; want true, nil (env parses 1 as true)", b, err)
	}

	if _, err := DefaultBool("not-a-bool", false, false); err == nil {
		t.Error("DefaultBool with malformed env value error = nil, want an error")
	}
}

func TestDefaultInt(t *testing.T) {
	n, err := DefaultInt("", 0, 7)
	if err != nil || n != 7 {
		t.Errorf("DefaultInt(empty, 0, 7) = %v, %v; want 7, nil (fallback)", n, err)
	}
	n, err = DefaultInt("", 42, 7)
	if err != nil || n != 42 {
		t.Errorf("DefaultInt(empty, 42, 7) = %v, %v; want 42, nil (file wins over fallback)", n, err)
	}
	n, err = DefaultInt("9", 42, 7)
	if err != nil || n != 9 {
		t.Errorf("DefaultInt(9, 42, 7) = %v, %v; want 9, nil (env wins over file)", n, err)
	}
	if _, err := DefaultInt("not-a-number", 0, 7); err == nil {
		t.Error("DefaultInt with malformed env value error = nil, want an error")
	}
}

func TestDefaultIntPtr(t *testing.T) {
	zero, five := 0, 5
	for _, tc := range []struct {
		env  string
		file *int
		want int
	}{
		{"", nil, 7},      // fallback
		{"", &zero, 0},    // a file zero wins over the fallback
		{"", &five, 5},    // file
		{"3", &five, 3},   // env wins over file
		{"0", nil, 0},     // env zero
		{"-1", &zero, -1}, // env, validated by the caller
	} {
		got, err := DefaultIntPtr(tc.env, tc.file, 7)
		if err != nil || got != tc.want {
			t.Errorf("DefaultIntPtr(%q, %v, 7) = %v, %v; want %v", tc.env, tc.file, got, err, tc.want)
		}
	}
	if _, err := DefaultIntPtr("x", nil, 7); err == nil {
		t.Error("DefaultIntPtr with malformed env value error = nil, want an error")
	}
}

func TestLoadParsesBusKeys(t *testing.T) {
	path := writeTOML(t, `
mode = "cluster"
bus = "nats"
nats-url = "nats://nats:4222"
nats-inline-max-bytes = 65536
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.Bus != "nats" || f.NATSURL != "nats://nats:4222" || f.NATSInlineMaxBytes != 65536 {
		t.Errorf("Load() bus keys = %q, %q, %d; want nats, nats://nats:4222, 65536", f.Bus, f.NATSURL, f.NATSInlineMaxBytes)
	}
}

// exampleConfigPath locates config.example.toml at the repo root,
// relative to this test file's own location, so the test works
// regardless of the working directory `go test` is invoked from.
func exampleConfigPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	// this file is internal/config/config_test.go; the repo root is two
	// directories up.
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "config.example.toml")
}

// TestExampleConfigFileParses guards against config.example.toml
// bit-rotting into something config.Load can't parse. Every entry in
// the shipped file is commented out, so a successful parse
// should yield the zero-value File — if it doesn't, either the file
// has an uncommented stray entry or config.Load's zero-value
// convention has changed underneath it.
func TestExampleConfigFileParses(t *testing.T) {
	path := exampleConfigPath(t)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}
	if !reflect.DeepEqual(*f, File{}) {
		t.Errorf("Load(%q) = %+v, want the zero-value File (every entry in the example is commented out)", path, *f)
	}
}

// TestExampleConfigFileDocumentsEveryField guards against drift
// between the File struct and config.example.toml: every `toml`
// struct tag on File must appear somewhere in the example file's text
// (as a key, e.g. `data-dir =`, or a table header, e.g. `[[keys]]`),
// so adding a new config field without documenting it in the example
// fails this test.
func TestExampleConfigFileDocumentsEveryField(t *testing.T) {
	path := exampleConfigPath(t)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	text := string(contents)

	typ := reflect.TypeOf(File{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("toml")
		if tag == "" {
			t.Fatalf("File.%s has no toml tag; give it one or exempt it here", field.Name)
		}
		if !strings.Contains(text, tag) {
			t.Errorf("config.example.toml does not mention %q (File.%s) — document the new/renamed field", tag, field.Name)
		}
	}
}

func TestDefaultInt64(t *testing.T) {
	cases := []struct {
		env      string
		file     int64
		fallback int64
		want     int64
		wantErr  bool
	}{
		{"", 0, 7, 7, false},
		{"", 5, 7, 5, false},
		{"9", 5, 7, 9, false},
		{"x", 5, 7, 0, true},
	}
	for _, tc := range cases {
		got, err := DefaultInt64(tc.env, tc.file, tc.fallback)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("DefaultInt64(%q, %d, %d) = %d, %v; want %d, err=%v", tc.env, tc.file, tc.fallback, got, err, tc.want, tc.wantErr)
		}
	}
}
