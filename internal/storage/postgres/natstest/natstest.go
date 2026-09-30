//go:build integration

// Package natstest is the shared testcontainer harness for tests that
// need a real NATS server, the NATS counterpart of pgtest. Start brings
// up nats:2.11-alpine once per test binary (sync.Once-guarded); the
// container is reaped when the binary exits via the testcontainers ryuk
// sidecar. Proxy puts a cuttable TCP link in front of it, so a test can
// partition one node from the bus without restarting the server (a
// restarted container would come back on a new host port).
package natstest

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Server is the handle returned by Start.
type Server struct {
	// URL is the client URL for the container, e.g. nats://127.0.0.1:32768.
	URL string
	// Addr is the host:port part of URL.
	Addr string
}

var (
	once     sync.Once
	inst     *Server
	startErr error
)

// Start brings up nats:2.11-alpine (lazily, once per test binary) and
// returns its handle. Startup failures are reported via t.Fatalf.
func Start(t *testing.T) *Server {
	t.Helper()
	once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		ctr, err := testcontainers.Run(ctx, "nats:2.11-alpine",
			testcontainers.WithExposedPorts("4222/tcp"),
			testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
		)
		if err != nil {
			startErr = fmt.Errorf("start nats container: %w", err)
			return
		}
		host, err := ctr.Host(ctx)
		if err != nil {
			startErr = fmt.Errorf("nats container host: %w", err)
			return
		}
		port, err := ctr.MappedPort(ctx, "4222/tcp")
		if err != nil {
			startErr = fmt.Errorf("nats container port: %w", err)
			return
		}
		addr := net.JoinHostPort(host, port.Port())
		inst = &Server{URL: "nats://" + addr, Addr: addr}
	})
	if startErr != nil {
		t.Fatalf("nats container: %v", startErr)
	}
	return inst
}

// Proxy is a TCP proxy between one client and the NATS server that a
// test can Cut (every connection through it is closed and new ones are
// refused) and Restore, simulating a network partition of that client.
type Proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	up    bool
	conns map[net.Conn]struct{}
}

// NewProxy starts a proxy to target (host:port) on an ephemeral local
// port. It is closed on t.Cleanup.
func NewProxy(t *testing.T, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{ln: ln, target: target, up: true, conns: make(map[net.Conn]struct{})}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Cut()
	})
	return p
}

// URL is the client URL that routes through the proxy.
func (p *Proxy) URL() string { return "nats://" + p.ln.Addr().String() }

// Cut closes every connection through the proxy and refuses new ones
// until Restore.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up = false
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// Retarget points new connections at another upstream (host:port).
// Connections already open are left alone.
func (p *Proxy) Retarget(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
}

// Restore lets connections through again.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up = true
}

func (p *Proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if !p.up {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c] = struct{}{}
		p.conns[upstream] = struct{}{}
		p.mu.Unlock()
		go p.pipe(c, upstream)
		go p.pipe(upstream, c)
	}
}

func (p *Proxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}
