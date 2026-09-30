//go:build integration

package natstest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Restartable is a single NATS server a test can kill (SIGKILL, the
// container removed) and later replace with a fresh one, behind a Proxy
// so clients keep one URL across the replacement (a new container
// comes up on a new host port). Unlike Start it is per test.
type Restartable struct {
	proxy *Proxy
	ctr   testcontainers.Container
}

// StartRestartable starts a NATS server behind a proxy. The server is
// removed on t.Cleanup.
func StartRestartable(t *testing.T) *Restartable {
	t.Helper()
	ctr, addr := startServer(t)
	r := &Restartable{ctr: ctr}
	r.proxy = NewProxy(t, addr)
	t.Cleanup(func() {
		if r.ctr != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = r.ctr.Terminate(ctx)
		}
	})
	return r
}

// URL is the client URL, stable across Kill and Replace.
func (r *Restartable) URL() string { return r.proxy.URL() }

// Kill removes the server at once, as a crashed machine would
// disappear, and refuses new connections until Replace.
func (r *Restartable) Kill(t *testing.T) {
	t.Helper()
	r.proxy.Cut()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.ctr.Terminate(ctx); err != nil {
		t.Fatalf("kill nats: %v", err)
	}
	r.ctr = nil
}

// Replace starts a fresh server (no state: NATS core keeps none) and
// lets clients reach it through the proxy.
func (r *Restartable) Replace(t *testing.T) {
	t.Helper()
	ctr, addr := startServer(t)
	r.ctr = ctr
	r.proxy.Retarget(addr)
	r.proxy.Restore()
}

// startServer runs one nats:2.11-alpine container and returns it with
// its host address.
func startServer(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, "nats:2.11-alpine",
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
	)
	if err != nil {
		t.Fatalf("start nats container: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("nats container host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatalf("nats container port: %v", err)
	}
	return ctr, net.JoinHostPort(host, port.Port())
}
