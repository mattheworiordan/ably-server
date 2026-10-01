//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// deliversAcross opens two nodes on dsn with o and checks one publish
// crosses between them over the bus.
func deliversAcross(t *testing.T, dsn string, o Options) {
	t.Helper()
	ctx := context.Background()
	o.DSN = dsn
	open := func() *Storage {
		s, err := Open(ctx, o)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	s1, s2 := open(), open()
	rec := &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", &cmRecorder{})
	bindChannel(t, s2, "room", rec)
	cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: "hi"}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	rec.waitFor(t, 1, 5*time.Second)
	assertSerials(t, "node2", rec.serials(), []string{cm.ChannelSerial})
	if got := s2.BusStats().Inline; got != 1 {
		t.Errorf("node2 inline deliveries = %d, want 1 (over the bus)", got)
	}
}

// TestNATSBusUserPassword (DESIGN.md §7.2, §9): against a NATS server
// that requires a user and password, the bus does not connect without
// them or with the wrong ones, and delivers with nats://user:pass@host.
func TestNATSBusUserPassword(t *testing.T) {
	c := pgtest.Start(t)
	srv := natstest.StartSecure(t, natstest.Security{User: "ably", Pass: "s3cret"})
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	for what, url := range map[string]string{
		"no credentials": srv.URL,
		"wrong password": "nats://ably:wrong@" + srv.Addr,
	} {
		if s, err := Open(ctx, Options{DSN: dsn, Bus: BusNATS, NATSURL: url}); err == nil {
			_ = s.Close()
			t.Errorf("%s: the bus connected to a server that requires a password", what)
		} else if !strings.Contains(strings.ToLower(err.Error()), "authorization") {
			t.Errorf("%s: err = %v, want an authorization failure", what, err)
		}
	}
	deliversAcross(t, dsn, Options{Bus: BusNATS, NATSURL: "nats://ably:s3cret@" + srv.Addr})
}

// TestNATSBusTLS (DESIGN.md §7.2, §9): against a NATS server that
// requires TLS and verifies client certificates, the bus does not
// connect without --nats-tls-ca (the certificate does not chain to the
// system roots) or without a client certificate, and delivers with
// --nats-tls-ca, --nats-tls-cert and --nats-tls-key.
func TestNATSBusTLS(t *testing.T) {
	c := pgtest.Start(t)
	files := natstest.NewTLS(t)
	srv := natstest.StartSecure(t, natstest.Security{TLS: files, VerifyClients: true})
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	for what, o := range map[string]Options{
		"no CA":                 {NATSTLSCert: files.ClientCert, NATSTLSKey: files.ClientKey},
		"no client certificate": {NATSTLSCA: files.CA},
	} {
		o.DSN, o.Bus, o.NATSURL = dsn, BusNATS, srv.URL
		if s, err := Open(ctx, o); err == nil {
			_ = s.Close()
			t.Errorf("%s: the bus connected", what)
		}
	}
	if _, err := Open(ctx, Options{DSN: dsn, Bus: BusNATS, NATSURL: srv.URL, NATSTLSCA: files.CA, NATSTLSCert: files.ClientCert}); err == nil || !strings.Contains(err.Error(), "needs both") {
		t.Errorf("a client certificate without its key: err = %v, want a refusal", err)
	}
	if _, err := Open(ctx, Options{DSN: dsn, Bus: BusNATS, NATSURL: srv.URL, NATSCredsFile: t.TempDir() + "/missing.creds"}); err == nil || !strings.Contains(err.Error(), "credentials file") {
		t.Errorf("a missing credentials file: err = %v, want a refusal naming it", err)
	}
	for _, url := range []string{srv.URL, "tls://" + srv.Addr} {
		t.Run(fmt.Sprintf("url=%s", strings.SplitN(url, ":", 2)[0]), func(t *testing.T) {
			deliversAcross(t, c.FreshSchemaDSN(t), Options{
				Bus: BusNATS, NATSURL: url,
				NATSTLSCA: files.CA, NATSTLSCert: files.ClientCert, NATSTLSKey: files.ClientKey,
			})
		})
	}
}
