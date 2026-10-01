package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/storage"
)

// TestShardIndexIsStableAndSpreads: a channel always maps to the same
// dispatch shard (which keeps its bus messages in order), and many
// channels spread over every shard.
func TestShardIndexIsStableAndSpreads(t *testing.T) {
	const shards = 16
	counts := make([]int, shards)
	for i := range 16000 {
		name := fmt.Sprintf("room-%d", i)
		a, b := shardIndex(name, shards), shardIndex(name, shards)
		if a != b {
			t.Fatalf("shardIndex(%q) = %d then %d", name, a, b)
		}
		if a < 0 || a >= shards {
			t.Fatalf("shardIndex(%q) = %d, out of [0,%d)", name, a, shards)
		}
		counts[a]++
	}
	for i, n := range counts {
		if n < 500 || n > 1500 {
			t.Errorf("shard %d got %d of 16000 channels; want roughly 1000", i, n)
		}
	}
	if got := shardIndex("x", 1); got != 0 {
		t.Errorf("shardIndex with one shard = %d, want 0", got)
	}
}

// TestDecodeNATSEnvelopeValidatesSerials (DESIGN.md §7.2): an envelope
// is refused, as malformed for its serials, when its serial or its
// predecessor is not a well-formed channelSerial, or its predecessor
// does not sort below its serial; a missing predecessor is allowed.
func TestDecodeNATSEnvelopeValidatesSerials(t *testing.T) {
	const (
		s0 = "01726585978590-000@abcdefghij"
		s1 = "01726585978590-001@abcdefghij"
	)
	for _, tc := range []struct {
		serial, prev string
		ok           bool
	}{
		{s1, s0, true},
		{s1, "", true},
		{"", s0, false},
		{"zzz", s0, false},
		{"99999999999999", s0, false},
		{s1 + ":000", s0, false},
		{s1, "p", false},
		{s1, s1, false},
		{s0, s1, false},
	} {
		data, err := msgpack.Marshal(&natsEnvelope{Channel: "room", Serial: tc.serial, Prev: tc.prev, Deployment: "d"})
		if err != nil {
			t.Fatal(err)
		}
		env, err := decodeNATSEnvelope(data)
		if tc.ok {
			if err != nil || env.ev.serial != tc.serial || env.ev.prev != tc.prev || env.mintedMs != 1726585978590 {
				t.Errorf("serial %q prev %q: %+v, %v; want accepted", tc.serial, tc.prev, env, err)
			}
			continue
		}
		var ee *natsEnvelopeError
		if !errors.As(err, &ee) || ee.reason != "serial" {
			t.Errorf("serial %q prev %q: err = %v, want a serial refusal", tc.serial, tc.prev, err)
		}
	}
	var ee *natsEnvelopeError
	if _, err := decodeNATSEnvelope([]byte{0xc1}); !errors.As(err, &ee) || ee.reason != "decode" {
		t.Errorf("undecodable bytes: err = %v, want a decode refusal", err)
	}
}

// TestNATSSecurityOptions: each credentials and TLS setting adds its
// connect option, a client certificate needs its key and the other way
// round, and a missing credentials file is refused before dialling.
func TestNATSSecurityOptions(t *testing.T) {
	if got, err := natsSecurityOptions(Options{}); err != nil || len(got) != 0 {
		t.Errorf("no settings: %d options, %v; want none", len(got), err)
	}
	creds := filepath.Join(t.TempDir(), "bus.creds")
	if err := os.WriteFile(creds, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := natsSecurityOptions(Options{NATSCredsFile: creds, NATSTLSCA: "ca.pem", NATSTLSCert: "c.pem", NATSTLSKey: "k.pem"}); err != nil || len(got) != 3 {
		t.Errorf("every setting: %d options, %v; want 3", len(got), err)
	}
	if _, err := natsSecurityOptions(Options{NATSCredsFile: creds + ".missing"}); err == nil {
		t.Error("a missing credentials file was accepted")
	}
	for _, o := range []Options{{NATSTLSCert: "c.pem"}, {NATSTLSKey: "k.pem"}} {
		if _, err := natsSecurityOptions(o); err == nil {
			t.Errorf("%+v: half a client certificate was accepted", o)
		}
	}
}

// TestUnavailableClassifiesUnreachable: a failed connection attempt or a
// timeout becomes storage.ErrUnavailable (50003, DESIGN.md §6.4); any
// other error is returned as it is.
func TestUnavailableClassifiesUnreachable(t *testing.T) {
	if unavailable(nil) != nil {
		t.Error("unavailable(nil) != nil")
	}
	other := errors.New("syntax error")
	if got := unavailable(other); got != other {
		t.Errorf("unavailable(%v) = %v, want it unchanged", other, got)
	}
	cfg, err := pgconn.ParseConfig("postgres://u@127.0.0.1:1/db?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	_, connErr := pgconn.ConnectConfig(context.Background(), cfg)
	if connErr == nil {
		t.Fatal("connected to port 1")
	}
	if got := unavailable(connErr); !errors.Is(got, storage.ErrUnavailable) {
		t.Errorf("unavailable(%v) is not ErrUnavailable", connErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if got := unavailable(fmt.Errorf("query: %w", ctx.Err())); !errors.Is(got, storage.ErrUnavailable) {
		t.Errorf("a timeout is not ErrUnavailable: %v", got)
	}
}
