package server

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/config"
)

// writeConfigFile writes contents to a fresh ably-server.toml under a
// t.TempDir() and returns its path.
func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ably-server.toml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

// emptyEnv is a getenv stub that returns "" for every key.
func emptyEnv(string) string { return "" }

// envWith returns a getenv stub that returns vals[k] for known keys
// and "" otherwise.
func envWith(vals map[string]string) func(string) string {
	return func(k string) string { return vals[k] }
}

func TestRunRejectsMissingKey(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   nil,
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "api key is required") {
		t.Errorf("output = %q, want substring %q", out.String(), "api key is required")
	}
}

func TestRunRejectsMalformedKeyFromFlag(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--keys=bogus"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunRejectsKeysSpanningAppIds(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--keys=app1.k1:s1", "--keys=app2.k2:s2"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "share the same appId") {
		t.Errorf("output = %q, want substring %q", out.String(), "share the same appId")
	}
}

func TestRunAcceptsCommaSeparatedEnvKeys(t *testing.T) {
	// A comma-separated env value contributes multiple keys; a malformed
	// second entry proves the whole set is parsed (without starting the
	// server).
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   nil,
		Getenv: envWith(map[string]string{keysEnv: "app.k1:s1, bogus"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunFallsBackToEnv(t *testing.T) {
	// Flag is absent; the env value must be picked up. We supply a
	// malformed env value so the parse error proves the env was read
	// — without starting the server.
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   nil,
		Getenv: envWith(map[string]string{keysEnv: "bogus"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunRejectsUnknownLogFormat(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--log-format=xml"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestRunLogFormatEnvFallback(t *testing.T) {
	// No --log-format flag; the env value must be picked up. An
	// invalid env value surfaces the same startup error as an invalid
	// flag value would, proving the env was read.
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   nil,
		Getenv: envWith(map[string]string{logFormatEnv: "xml"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestNewLoggerSelectsHandler(t *testing.T) {
	var out bytes.Buffer
	logger, err := newLogger("info", "json", &out)
	if err != nil {
		t.Fatalf("newLogger(json) error: %v", err)
	}
	logger.Info("hello")
	if !strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
		t.Errorf("json output = %q, want a JSON object", out.String())
	}

	out.Reset()
	logger, err = newLogger("info", "text", &out)
	if err != nil {
		t.Fatalf("newLogger(text) error: %v", err)
	}
	logger.Info("hello")
	if strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
		t.Errorf("text output = %q, want slog's key=value form", out.String())
	}

	if _, err := newLogger("info", "yaml", &out); err == nil {
		t.Error("newLogger(yaml) error = nil, want an error for an unrecognised format")
	}
}

func TestRunConfigFileSuppliesDefaults(t *testing.T) {
	// Nothing on the command line or in the environment; an invalid
	// log-format in the file surfaces the same startup error a flag or
	// env value would, proving the file was read for a flag with
	// neither set.
	path := writeConfigFile(t, `log-format = "xml"`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestRunEnvOverridesConfigFile(t *testing.T) {
	// The file's log-format is valid; the env value is not. Env must
	// win, so the invalid env value is what surfaces.
	path := writeConfigFile(t, `log-format = "json"`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path},
		Getenv: envWith(map[string]string{logFormatEnv: "xml"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestRunFlagOverridesEnvAndConfigFile(t *testing.T) {
	// Both the file and the env supply an invalid log-format; an
	// explicit --log-format flag must still win. With a valid format
	// resolved, the run proceeds past logger construction and fails
	// later for the (deliberately) missing api key — proving the flag,
	// not the file or env, decided the format.
	path := writeConfigFile(t, `log-format = "xml"`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path, "--log-format=text"},
		Getenv: envWith(map[string]string{logFormatEnv: "xml"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if strings.Contains(out.String(), "log-format") {
		t.Errorf("output = %q, should not mention log-format (the flag should have resolved it)", out.String())
	}
	if !strings.Contains(out.String(), "api key is required") {
		t.Errorf("output = %q, want substring %q", out.String(), "api key is required")
	}
}

func TestRunConfigFileShutdownGraceMalformed(t *testing.T) {
	path := writeConfigFile(t, `shutdown-grace = "not-a-duration"`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid duration") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid duration")
	}
}

func TestFixtureSpec(t *testing.T) {
	// Valid namespaces + channels build a spec with members preserved
	// verbatim; namespaces are validated but do not appear in the spec.
	spec, err := fixtureSpec(config.File{
		Namespaces: []config.Namespace{{ID: "persisted", Persisted: true}},
		Channels: []config.Channel{{
			Name: "persisted:presence_fixtures",
			Presence: []config.PresenceMember{
				{ClientID: "a", Data: "1"},
				{ClientID: "b", Data: "x", Encoding: "json"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("fixtureSpec: %v", err)
	}
	if len(spec.Channels) != 1 || spec.Channels[0].Name != "persisted:presence_fixtures" {
		t.Fatalf("channels = %+v", spec.Channels)
	}
	if len(spec.Channels[0].Presence) != 2 || spec.Channels[0].Presence[1].Encoding != "json" {
		t.Fatalf("presence = %+v", spec.Channels[0].Presence)
	}

	// No channels → nil spec (nothing to seed).
	if s, err := fixtureSpec(config.File{}); err != nil || s != nil {
		t.Errorf("fixtureSpec(empty) = %v, %v; want nil, nil", s, err)
	}

	// Malformed sections are errors.
	malformed := map[string]config.File{
		"namespace no id": {Namespaces: []config.Namespace{{Persisted: true}}},
		"channel no name": {Channels: []config.Channel{{Presence: nil}}},
		"member no clientId": {Channels: []config.Channel{{
			Name:     "c1",
			Presence: []config.PresenceMember{{Data: "x"}},
		}}},
	}
	for name, f := range malformed {
		t.Run(name, func(t *testing.T) {
			if _, err := fixtureSpec(f); err == nil {
				t.Errorf("fixtureSpec(%s) = nil error, want error", name)
			}
		})
	}
}

// TestRunConfigChannelMalformedIsStartupError proves a malformed
// [[channels]] section (a member without a clientId) fails startup.
func TestRunConfigChannelMalformedIsStartupError(t *testing.T) {
	path := writeConfigFile(t, `
[[keys]]
key = "app.key:secret"

[[channels]]
name = "c1"

  [[channels.presence]]
  data = "x"
`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "no clientId") {
		t.Errorf("output = %q, want substring %q", out.String(), "no clientId")
	}
}

func TestRunConfigFileStructuredKeyMalformedCapability(t *testing.T) {
	// A [[keys]] entry with a malformed capability is a startup error,
	// proving the structured-key path is parsed and its capability
	// validated.
	path := writeConfigFile(t, `
[[keys]]
key = "app.key:secret"
capability = "not json"
`)
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=" + path},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunConfigFileMissingPathIsAnError(t *testing.T) {
	var out bytes.Buffer
	code := Run(context.Background(), Opts{
		Args:   []string{"--config=/nonexistent/ably-server.toml"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "/nonexistent/ably-server.toml") {
		t.Errorf("output = %q, want it to name the missing config path", out.String())
	}
}

func TestRunConfigFileListenAndAPIKeyEndToEnd(t *testing.T) {
	// The file alone (no flags, no env) supplies both --listen and the
	// [[keys]] entry; the server must actually start and bind, proving
	// the file's values reached the real flags rather than just being
	// parsed and discarded.
	path := writeConfigFile(t, `
listen = "127.0.0.1:0"

[[keys]]
key = "app.key:secret"
`)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	var out bytes.Buffer
	go func() {
		done <- Run(ctx, Opts{
			Args:   []string{"--config=" + path},
			Getenv: emptyEnv,
			Out:    &out,
			Ready:  ready,
		})
	}()

	select {
	case addr := <-ready:
		if addr == nil {
			t.Error("Ready sent a nil address")
		}
	case code := <-done:
		t.Fatalf("server exited before ready (code=%d), output: %s", code, out.String())
	case <-time.After(5 * time.Second):
		t.Fatal("server did not become ready within 5s")
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code = %d, want 0; output: %s", code, out.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down within 5s")
	}
}

func TestRunLifecycleAndConnFlagValidation(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		env      map[string]string
		file     string
		wantCode int
		wantOut  string
	}{
		{"negative flag", []string{"--keys=app.key:secret", "--channel-idle-timeout=-1s"}, nil, "", 2, "must not be negative"},
		{"malformed env", []string{"--keys=app.key:secret"}, map[string]string{channelIdleEnv: "soon"}, "", 1, "invalid duration"},
		{"malformed file", []string{"--keys=app.key:secret"}, nil, `channel-idle-timeout = "soon"`, 1, "invalid duration"},
		{"zero outbound bytes", []string{"--keys=app.key:secret", "--conn-outbound-max-bytes=0"}, nil, "", 2, "must be positive"},
		{"negative write timeout", []string{"--keys=app.key:secret", "--conn-write-timeout=-1s"}, nil, "", 2, "must be positive"},
		{"malformed read buffer env", []string{"--keys=app.key:secret"}, map[string]string{wsReadBufEnv: "big"}, "", 1, "invalid integer"},
		{"zero http idle timeout", []string{"--keys=app.key:secret", "--http-idle-timeout=0s"}, nil, "", 2, "must be positive"},
		{"malformed write timeout file", []string{"--keys=app.key:secret"}, nil, `conn-write-timeout = "soon"`, 1, "invalid duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.args
			if tc.file != "" {
				args = append(args, "--config="+writeConfigFile(t, tc.file))
			}
			var out bytes.Buffer
			code := Run(context.Background(), Opts{Args: args, Getenv: envWith(tc.env), Out: &out})
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d (out %q)", code, tc.wantCode, out.String())
			}
			if !strings.Contains(out.String(), tc.wantOut) {
				t.Errorf("output = %q, want substring %q", out.String(), tc.wantOut)
			}
		})
	}
}

func TestPersistedNamespaces(t *testing.T) {
	persisted := persistedNamespaces([]config.Namespace{
		{ID: "persisted", Persisted: true},
		{ID: "ephemeral"},
	})
	for name, want := range map[string]bool{
		"persisted:room":       true,
		"persisted:a:b":        true,
		"ephemeral:room":       false,
		"persisted":            false, // no namespace separator: the default namespace
		"other:room":           false,
		"persistedx:room":      false,
		"persisted:presence_x": true,
	} {
		if got := persisted(name); got != want {
			t.Errorf("persisted(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestRunRetentionMalformedIsStartupError(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		file string
	}{
		{name: "env message", env: map[string]string{messageRetentionEnv: "soon"}},
		{name: "file persisted", file: `persisted-retention = "a day"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--keys=app.key:secret"}
			if tc.file != "" {
				args = append(args, "--config="+writeConfigFile(t, tc.file))
			}
			var out bytes.Buffer
			code := Run(context.Background(), Opts{Args: args, Getenv: envWith(tc.env), Out: &out})
			if code != 1 || !strings.Contains(out.String(), "invalid duration") {
				t.Errorf("exit = %d, output %q; want 1 and an invalid duration error", code, out.String())
			}
		})
	}
}

func TestRunPublishBatchingMalformedIsStartupError(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		env        map[string]string
		file       string
	}{
		{name: "env lanes", env: map[string]string{publishLanesEnv: "four"}, want: "invalid integer"},
		{name: "file linger", file: `publish-linger-max = "soon"`, want: "invalid duration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--keys=app.key:secret"}
			if tc.file != "" {
				args = append(args, "--config="+writeConfigFile(t, tc.file))
			}
			var out bytes.Buffer
			code := Run(context.Background(), Opts{Args: args, Getenv: envWith(tc.env), Out: &out})
			if code != 1 || !strings.Contains(out.String(), tc.want) {
				t.Errorf("exit = %d, output %q; want 1 and %q", code, out.String(), tc.want)
			}
		})
	}
}

// TestRunPresenceLeaseModeInvalidIsStartupError: an unknown
// --presence-lease-mode, from the flag, the env or the config file, stops
// a cluster-mode node before it dials Postgres (DESIGN.md §12.5).
func TestRunPresenceLeaseModeInvalidIsStartupError(t *testing.T) {
	base := []string{"--keys=app.key:secret", "--mode=cluster", "--postgres-dsn=postgres://u:p@127.0.0.1:1/db?sslmode=disable"}
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		file string
	}{
		{name: "flag", args: []string{"--presence-lease-mode=row"}},
		{name: "env", env: map[string]string{presenceLeaseModeEnv: "row"}},
		{name: "file", file: `presence-lease-mode = "row"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{}, base...), tc.args...)
			if tc.file != "" {
				args = append(args, "--config="+writeConfigFile(t, tc.file))
			}
			var out bytes.Buffer
			code := Run(context.Background(), Opts{Args: args, Getenv: envWith(tc.env), Out: &out})
			if code != 1 || !strings.Contains(out.String(), "invalid --presence-lease-mode") {
				t.Errorf("exit = %d, output %q; want 1 and an invalid --presence-lease-mode error", code, out.String())
			}
		})
	}
}
