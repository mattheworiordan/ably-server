//go:build integration

package pgtest

import (
	"net"
	"net/url"
	"sync"
	"testing"
)

// Proxy is a TCP proxy in front of a Postgres container that a test can
// break, standing in for a database that goes down (DESIGN.md §6.4)
// without stopping the container the rest of the test binary shares.
// Cut closes every connection and refuses new ones, as a stopped server
// would. Blackhole keeps every connection open but drops the bytes both
// ways and accepts new connections without answering, as an unreachable
// host would. Restore lets traffic through again (connections that
// were blackholed are closed, since they lost bytes).
type Proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	mode  proxyMode
	conns map[net.Conn]struct{}
}

type proxyMode int

const (
	proxyUp proxyMode = iota
	proxyCut
	proxyBlackhole
)

// NewProxy starts a proxy on an ephemeral local port in front of the
// host:port of dsn (a URL-form DSN), and returns it with dsn rewritten
// to go through it. The proxy is closed on t.Cleanup.
func NewProxy(t testing.TB, dsn string) (*Proxy, string) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("proxy: parse DSN: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{ln: ln, target: u.Host, conns: make(map[net.Conn]struct{})}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Cut()
	})
	u.Host = ln.Addr().String()
	return p, u.String()
}

// Cut closes every connection through the proxy and refuses new ones
// until Restore.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = proxyCut
	p.closeAllLocked()
}

// Blackhole stops every byte through the proxy, both ways, without
// closing anything, and accepts new connections without answering,
// until Restore.
func (p *Proxy) Blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = proxyBlackhole
}

// Restore lets connections through again, closing any that were
// blackholed.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == proxyBlackhole {
		p.closeAllLocked()
	}
	p.mode = proxyUp
}

func (p *Proxy) closeAllLocked() {
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

func (p *Proxy) currentMode() proxyMode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

func (p *Proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		switch p.mode {
		case proxyCut:
			p.mu.Unlock()
			_ = c.Close()
			continue
		case proxyBlackhole:
			// Hold the connection open and answer nothing.
			p.conns[c] = struct{}{}
			p.mu.Unlock()
			go p.pipe(nil, c)
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

// pipe copies src to dst (dst nil: discards), dropping what it reads
// while the proxy is blackholed.
func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && dst != nil && p.currentMode() == proxyUp {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				err = werr
			}
		}
		if err != nil {
			break
		}
	}
	if dst != nil {
		_ = dst.Close()
	}
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, src)
	if dst != nil {
		delete(p.conns, dst)
	}
	p.mu.Unlock()
}
