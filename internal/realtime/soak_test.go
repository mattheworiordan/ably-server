//go:build soak

package realtime

// Connection-layer soak (DESIGN.md §5.2): N WebSocket connections held
// against one realtime server, to measure per-connection heap, stack
// and goroutines, and to check nothing grows after the ramp.
//
// The server runs in the test process and the clients in a child
// process (this test binary re-executed), so the server's runtime
// numbers are not polluted by client state. The server listens on
// several ports because one loopback address and one server port allow
// only ~16k client ports on macOS.
//
//	bench/soak/run.sh     # runs the test below inside a golang:1.26 container
//
// It is never run on the host: its sockets would exhaust the host's
// ephemeral ports for everything else on the machine. The test skips
// unless SOAK_IN_CONTAINER=1, which the script sets.
//
// Environment: SOAK_CONNS (default 50000), SOAK_CHANNELS (1000),
// SOAK_HOLD (30s), SOAK_PUBLISH_EVERY (5s: one publish per channel per
// interval during the hold), SOAK_OUT (optional path for a JSON summary),
// SOAK_HEAPPROFILE (optional path for a heap profile after the ramp).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// soakListeners is how many ports the server listens on; each client
// local port is reused once per listener.
const soakListeners = 8

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

type soakSample struct {
	Label        string  `json:"label"`
	Conns        int     `json:"conns"`
	Goroutines   int     `json:"goroutines"`
	HeapInuseMiB float64 `json:"heap_inuse_mib"`
	StackMiB     float64 `json:"stack_inuse_mib"`
	SysMiB       float64 `json:"sys_mib"`
}

func sample(label string, conns int) soakSample {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return soakSample{
		Label:        label,
		Conns:        conns,
		Goroutines:   runtime.NumGoroutine(),
		HeapInuseMiB: float64(ms.HeapInuse) / (1 << 20),
		StackMiB:     float64(ms.StackInuse) / (1 << 20),
		SysMiB:       float64(ms.Sys) / (1 << 20),
	}
}

func TestSoakConnections(t *testing.T) {
	if os.Getenv("SOAK_CLIENT") == "1" {
		t.Skip("client process")
	}
	// Tens of thousands of loopback sockets exhaust the host's ephemeral
	// ports and break other loopback tests on the machine, so the soak
	// runs only inside its own container (bench/soak/run.sh sets this).
	if os.Getenv("SOAK_IN_CONTAINER") != "1" {
		t.Skip("run the soak with bench/soak/run.sh (inside a Linux container), not on the host")
	}
	nConns := envInt("SOAK_CONNS", 50000)
	nChannels := envInt("SOAK_CHANNELS", 1000)
	hold := envDuration("SOAK_HOLD", 30*time.Second)
	publishEvery := envDuration("SOAK_PUBLISH_EVERY", 5*time.Second)

	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	manager := core.NewManagerWithOptions(memory.New(memory.Options{}), core.Options{IdleTimeout: core.DefaultChannelIdleTimeout})
	defer manager.Close()
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, manager, DefaultHeartbeatInterval, logging.New(slog.DiscardHandler), m, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	var addrs []string
	for range soakListeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, ln.Addr().String())
		go func() { _ = hs.Serve(ln) }()
	}
	defer hs.Close()

	base := sample("baseline", 0)
	t.Logf("%+v", base)

	cmd := exec.Command(os.Args[0], "-test.run=^TestSoakClientProcess$", "-test.timeout=0")
	cmd.Env = append(os.Environ(),
		"SOAK_CLIENT=1",
		"SOAK_ADDRS="+strings.Join(addrs, ","),
		"SOAK_CONNS="+strconv.Itoa(nConns),
		"SOAK_CHANNELS="+strconv.Itoa(nChannels),
	)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	rampStart := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	lines := bufio.NewScanner(stdout)
	ready := false
	for lines.Scan() {
		line := lines.Text()
		if strings.HasPrefix(line, "SOAK-READY") {
			ready = true
			break
		}
		if strings.HasPrefix(line, "SOAK-FAIL") {
			t.Fatalf("client: %s", line)
		}
	}
	if !ready {
		t.Fatalf("client process ended before it was ready: %v", lines.Err())
	}
	rampTime := time.Since(rampStart)
	go func() { _, _ = io.Copy(io.Discard, stdout) }()

	// A client dial that timed out after the server accepted it leaves a
	// server connection that closes once the client drops the socket:
	// let the count settle before measuring.
	settle := time.Now().Add(30 * time.Second)
	for rt.connCount() != nConns && time.Now().Before(settle) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := rt.connCount(); got < nConns || got > nConns+nConns/1000 {
		t.Fatalf("server holds %d connections, want %d", got, nConns)
	}
	ramped := sample("after-ramp", nConns)
	t.Logf("%+v (ramp %v)", ramped, rampTime)
	if path := os.Getenv("SOAK_HEAPPROFILE"); path != "" {
		if f, err := os.Create(path); err == nil {
			_ = pprof.WriteHeapProfile(f)
			_ = f.Close()
		}
	}

	// Hold with light traffic: one publish per channel every
	// SOAK_PUBLISH_EVERY, spread evenly. (Publishing every channel in one
	// burst, 50k deliveries at once, made the laptop running both processes
	// stall in loopback socket writes until the Go runtime hit its
	// 10,000-thread limit; see .working/STATUS.md.)
	ctx := context.Background()
	chans := make([]*core.Channel, nChannels)
	for i := range chans {
		if chans[i], err = manager.GetChannel(ctx, "soak-"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	samples := []soakSample{base, ramped}
	var published atomic.Int64
	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		// Spread the publishes over the interval, one channel per tick,
		// rather than one burst per interval: a burst wakes every
		// subscriber's writer at once (see the comment above).
		tick := time.NewTicker(max(publishEvery/time.Duration(nChannels), time.Millisecond))
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-tick.C:
				ch := chans[i%len(chans)]
				if _, _, err := ch.Publish(ctx, []*protocol.Message{{Name: "tick", Data: "0123456789abcdef0123456789abcdef"}}); err == nil {
					published.Add(1)
				}
			}
		}
	}()
	end := time.Now().Add(hold)
	for time.Now().Before(end) {
		time.Sleep(min(10*time.Second, time.Until(end)))
		s := sample(fmt.Sprintf("hold+%ds", int(time.Since(end.Add(-hold)).Seconds())), rt.connCount())
		samples = append(samples, s)
		t.Logf("%+v", s)
	}
	close(stop)
	pubWG.Wait()
	held := sample("after-hold", rt.connCount())
	samples = append(samples, held)
	t.Logf("%+v published=%d", held, published.Load())

	perConnHeap := (ramped.HeapInuseMiB - base.HeapInuseMiB) * (1 << 20) / float64(nConns)
	perConnStack := (ramped.StackMiB - base.StackMiB) * (1 << 20) / float64(nConns)
	perConnG := float64(ramped.Goroutines-base.Goroutines) / float64(nConns)
	t.Logf("per connection: heap %.0f B, stack %.0f B, goroutines %.2f (1 attachment each)", perConnHeap, perConnStack, perConnG)

	for _, line := range strings.Split(scrape(t, m), "\n") {
		if strings.HasPrefix(line, "ably_slow_consumer_disconnects_total{") {
			t.Logf("%s", line)
		}
	}
	if held.Conns < nConns {
		t.Errorf("connections dropped during the hold: %d of %d", held.Conns, nConns)
	}
	if held.Goroutines > ramped.Goroutines+nChannels/10+50 {
		t.Errorf("goroutines grew during the hold: %d -> %d", ramped.Goroutines, held.Goroutines)
	}
	if held.HeapInuseMiB > ramped.HeapInuseMiB*1.25+16 {
		t.Errorf("heap grew during the hold: %.1f -> %.1f MiB", ramped.HeapInuseMiB, held.HeapInuseMiB)
	}

	// Close the clients; the server must return to its baseline.
	_ = stdin.Close()
	deadline := time.Now().Add(2 * time.Minute)
	for rt.connCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Second)
	after := sample("after-close", rt.connCount())
	samples = append(samples, after)
	t.Logf("%+v", after)
	if after.Conns != 0 {
		t.Errorf("%d connections still open after the clients closed", after.Conns)
	}
	if after.Goroutines > base.Goroutines+nChannels/10+50 {
		t.Errorf("goroutines leaked: baseline %d, after close %d", base.Goroutines, after.Goroutines)
	}

	if out := os.Getenv("SOAK_OUT"); out != "" {
		summary := map[string]any{
			"conns": nConns, "channels": nChannels, "hold": hold.String(), "ramp": rampTime.String(),
			"per_conn_heap_bytes": perConnHeap, "per_conn_stack_bytes": perConnStack, "per_conn_goroutines": perConnG,
			"published": published.Load(), "samples": samples, "gomaxprocs": runtime.GOMAXPROCS(0),
		}
		b, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(out, b, 0o644)
	}
}

// TestSoakClientProcess is the client half of TestSoakConnections, run
// in a child process. It dials SOAK_CONNS connections spread over
// SOAK_ADDRS, attaches each to one of SOAK_CHANNELS channels, reads and
// discards every frame, prints SOAK-READY once all are attached, and
// exits when its stdin closes.
func TestSoakClientProcess(t *testing.T) {
	if os.Getenv("SOAK_CLIENT") != "1" {
		t.Skip("run by TestSoakConnections")
	}
	addrs := strings.Split(os.Getenv("SOAK_ADDRS"), ",")
	nConns := envInt("SOAK_CONNS", 50000)
	nChannels := envInt("SOAK_CHANNELS", 1000)
	newDialer := func(localPort int) *websocket.Dialer {
		// macOS allocates ephemeral ports from one global ~16k range, so
		// each client binds an explicit local port (SO_REUSEADDR +
		// SO_REUSEPORT) and reuses it once per server listener: the
		// 4-tuples still differ.
		nd := &net.Dialer{
			LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: localPort},
			Control:   reusePort,
			Timeout:   30 * time.Second,
		}
		return &websocket.Dialer{
			NetDialContext:   nd.DialContext,
			ReadBufferSize:   512,
			WriteBufferSize:  512,
			WriteBufferPool:  soakWritePool,
			HandshakeTimeout: 30 * time.Second,
		}
	}

	ports := make([]atomic.Int64, len(addrs))
	for d := range ports {
		ports[d].Store(soakLocalPortBase - 1)
	}
	var failed atomic.Int64
	conns := make([]*websocket.Conn, nConns)
	sem := make(chan struct{}, 96) // below the listeners' accept backlog, so SYNs are not dropped
	var wg sync.WaitGroup
	for i := range nConns {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d := i % len(addrs)
			var ws *websocket.Conn
			var err error
			// A local port another process holds is skipped: each
			// listener has its own port cursor, advanced past failures.
			for range 50 {
				port := int(ports[d].Add(1))
				if ws, err = soakDial(newDialer(port), addrs[d], "soak-"+strconv.Itoa(i%nChannels)); err == nil {
					break
				}
			}
			if err != nil {
				if failed.Add(1) <= 5 {
					fmt.Fprintf(os.Stderr, "dial %d: %v\n", i, err)
				}
				return
			}
			conns[i] = ws
		}()
	}
	wg.Wait()
	if n := failed.Load(); n > 0 {
		fmt.Printf("SOAK-FAIL %d of %d connections failed\n", n, nConns)
		return
	}
	for _, ws := range conns {
		go func() {
			for {
				if _, _, err := ws.NextReader(); err != nil {
					return
				}
			}
		}()
	}
	fmt.Println("SOAK-READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
	for _, ws := range conns {
		_ = ws.Close()
	}
}

// soakLocalPortBase is the first local port clients bind; below the
// macOS ephemeral range (49152+) so it never collides with it.
const soakLocalPortBase = 20000

var soakWritePool = &sync.Pool{}

func reusePort(_, _ string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		if serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); serr != nil {
			return
		}
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
	})
	if err != nil {
		return err
	}
	return serr
}

func soakDial(d *websocket.Dialer, addr, channel string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: addr, Path: "/", RawQuery: "key=" + url.QueryEscape(testKey) + "&format=json"}
	ws, _, err := d.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	read := func(want protocol.Action) error {
		_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
		defer ws.SetReadDeadline(time.Time{})
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				return err
			}
			var m protocol.ProtocolMessage
			if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
				return err
			}
			if m.Action == want {
				return nil
			}
		}
	}
	if err := read(protocol.ActionConnected); err != nil {
		ws.Close()
		return nil, err
	}
	data, _ := protocol.Marshal(&protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: &channel}, protocol.FormatJSON)
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		ws.Close()
		return nil, err
	}
	if err := read(protocol.ActionAttached); err != nil {
		ws.Close()
		return nil, err
	}
	return ws, nil
}
