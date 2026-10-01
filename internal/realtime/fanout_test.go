package realtime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// fanoutRig is a channel with n subscriber attachments, each on its own
// connection with no socket: frames stop in the connection's outbound
// queue, which the test reads. Each queued frame calls queued.Done.
type fanoutRig struct {
	ch     *core.Channel
	conns  []*connection
	queued sync.WaitGroup
	cancel context.CancelFunc
	atts   []*attachment
	pool   *core.FanoutPool
	// pooled counts the frames the fan-out pool queued, and full the
	// frames it could not queue (no room in the connection's queue).
	pooled atomic.Int64
	full   atomic.Int64
}

// rigOptions configures newFanoutRigWith. With pool set, the channel's
// Manager has that fan-out pool and every attachment may be pooled
// (DESIGN.md §5.1); setup, when set, adjusts attachment i before it
// runs.
type rigOptions struct {
	shared bool
	pool   *core.FanoutPool
	setup  func(i int, a *attachment)
}

// Connection i uses formats[i%len(formats)]; every attachment has modes.
func newFanoutRig(tb testing.TB, n int, formats []protocol.Format, modes int64, shared bool) *fanoutRig {
	return newFanoutRigWith(tb, n, formats, modes, rigOptions{shared: shared})
}

func newFanoutRigWith(tb testing.TB, n int, formats []protocol.Format, modes int64, opts rigOptions) *fanoutRig {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	manager := core.NewManagerWithOptions(memory.New(memory.Options{}), core.Options{FanoutPool: opts.pool})
	ch, err := manager.GetChannel(ctx, "fanout")
	if err != nil {
		tb.Fatal(err)
	}
	r := &fanoutRig{ch: ch, cancel: cancel, pool: opts.pool}
	logger := logging.New(slog.DiscardHandler)
	r.queued.Add(n) // the ATTACHED frames
	for i := range n {
		c := &connection{format: formats[i%len(formats)], out: newOutQueue(1<<20, 10*time.Second), logger: logger}
		stream, err := ch.Attach(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		out := func(ctx context.Context, msg *protocol.ProtocolMessage) bool {
			ok := c.queue(ctx, msg)
			r.queued.Done()
			return ok
		}
		a := newAttachment(ctx, "fanout", stream.Channel(), stream, "", false, modes, modes, nil, out, fmt.Sprintf("conn-%d", i), true, nil, logger)
		if opts.shared || opts.pool != nil {
			a.outShared = func(ctx context.Context, msg *protocol.ProtocolMessage, memo memoizer) bool {
				ok := c.queueShared(ctx, msg, memo)
				r.queued.Done()
				return ok
			}
		}
		if opts.pool != nil {
			a.enablePool(c.encode, func(action protocol.Action, memo memoizer, build func() any) bool {
				if !c.tryQueueShared(action, memo, build) {
					r.full.Add(1)
					return false
				}
				r.pooled.Add(1)
				r.queued.Done()
				return true
			})
		}
		if opts.setup != nil {
			opts.setup(i, a)
		}
		r.conns = append(r.conns, c)
		r.atts = append(r.atts, a)
		go a.run()
	}
	r.queued.Wait()
	r.drain()
	return r
}

// drain empties every connection's outbound queue and returns the frames.
func (r *fanoutRig) drain() [][]outFrame {
	out := make([][]outFrame, len(r.conns))
	for i, c := range r.conns {
		for {
			f, ok := c.out.pop()
			if !ok {
				break
			}
			out[i] = append(out[i], f)
		}
	}
	return out
}

func (r *fanoutRig) close() {
	r.cancel()
	for _, a := range r.atts {
		<-a.done
	}
	r.pool.Close()
}

// publish publishes one message and waits until every attachment has
// queued its frame.
func (r *fanoutRig) publish(tb testing.TB, msg *protocol.Message) {
	tb.Helper()
	r.queued.Add(len(r.conns))
	if _, _, err := r.ch.Publish(context.Background(), []*protocol.Message{msg}); err != nil {
		tb.Fatal(err)
	}
	r.queued.Wait()
}

// TestFanoutSharesEncodedFrame checks encode-once delivery (DESIGN.md
// §5.1): a message cm, and a presence cm, reach every attachment on the
// channel as one encoding per wire format, with JSON and msgpack
// connections on the same channel, byte-identical to the frame the
// per-attachment path sends for the same cm.
func TestFanoutSharesEncodedFrame(t *testing.T) {
	formats := []protocol.Format{protocol.FormatJSON, protocol.FormatMsgpack}
	modes := protocol.FlagSubscribe | protocol.FlagPresenceSubscribe
	publish := []struct {
		name string
		do   func(*fanoutRig)
	}{
		{"message", func(r *fanoutRig) { r.publish(t, &protocol.Message{Name: "m", Data: "hello", ID: "id1"}) }},
		{"presence", func(r *fanoutRig) {
			r.queued.Add(len(r.conns))
			if _, _, err := r.ch.PublishPresence(context.Background(), []*protocol.PresenceMessage{{
				Action: protocol.PresenceEnter, ClientID: "c1", ConnectionID: "conn-x", Data: "hi",
			}}); err != nil {
				t.Fatal(err)
			}
			r.queued.Wait()
		}},
	}
	for _, p := range publish {
		const n = 20
		shared := newFanoutRig(t, n, formats, modes, true)
		p.do(shared)
		got := shared.drain()
		shared.close()
		own := newFanoutRig(t, n, formats, modes, false)
		p.do(own)
		want := own.drain()
		own.close()

		first := map[protocol.Format]*byte{}
		for i, fs := range got {
			format := formats[i%len(formats)]
			if len(fs) != 1 || len(want[i]) != 1 {
				t.Fatalf("%s: attachment %d queued %d shared / %d own frames, want 1", p.name, i, len(fs), len(want[i]))
			}
			var a, b protocol.ProtocolMessage
			if err := protocol.Unmarshal(fs[0].data, format, &a); err != nil {
				t.Fatal(err)
			}
			if err := protocol.Unmarshal(want[i][0].data, format, &b); err != nil {
				t.Fatal(err)
			}
			// Serials and timestamps differ between the two rigs' stores;
			// everything else in the frame must match. Within one rig the
			// bytes are compared exactly below.
			a.ChannelSerial, b.ChannelSerial = "", ""
			for _, m := range append(a.Messages, b.Messages...) {
				m.Serial, m.Timestamp, m.Version = "", 0, nil
			}
			for _, m := range append(a.Presence, b.Presence...) {
				m.Serial, m.Timestamp, m.ID = "", 0, ""
			}
			ab, _ := protocol.Marshal(&a, protocol.FormatJSON)
			bb, _ := protocol.Marshal(&b, protocol.FormatJSON)
			if !bytes.Equal(ab, bb) {
				t.Errorf("%s: attachment %d (%v): shared frame %s, per-attachment frame %s", p.name, i, format, ab, bb)
			}
			if f, ok := first[format]; !ok {
				first[format] = &fs[0].data[0]
				if i >= len(formats) {
					t.Fatalf("%s: first %v frame at attachment %d", p.name, format, i)
				}
			} else if f != &fs[0].data[0] {
				t.Errorf("%s: attachment %d (%v) has its own encoding; want the shared one", p.name, i, format)
			}
			if !bytes.Equal(fs[0].data, got[i%len(formats)][0].data) {
				t.Errorf("%s: attachment %d (%v) bytes differ from the format's first frame", p.name, i, format)
			}
		}
		if first[protocol.FormatJSON] == first[protocol.FormatMsgpack] {
			t.Errorf("%s: JSON and msgpack connections share one encoding", p.name)
		}
	}
}

// TestFanoutAppendDeltaNotShared checks that a frame that needs a
// per-attachment transform keeps its own encoding: an append is a delta
// for an attachment that has seen the message and the full version for
// one that has not (DESIGN.md §13.3).
func TestFanoutAppendDeltaNotShared(t *testing.T) {
	r := newFanoutRig(t, 3, []protocol.Format{protocol.FormatJSON}, protocol.FlagSubscribe, true)
	defer r.close()
	r.atts[2].appendModeFull = true
	delta := &protocol.Message{Action: protocol.MessageAppend, Serial: "s1", Data: "+more"}
	full := &protocol.Message{
		Action: protocol.MessageUpdate, Serial: "s1", Data: "start+more",
		Alt: map[string]*protocol.Message{protocol.DeltaAppend: delta},
	}
	// Mark the message seen on attachments 0 and 2, as if they had
	// received the create before attachment 1 attached; attachment 2
	// asked for appendMode=full.
	r.atts[0].seen.add("s1")
	r.atts[2].seen.add("s1")

	r.queued.Add(3)
	r.ch.Append(&protocol.ChannelMessage{ChannelSerial: "zzz", Messages: []*protocol.Message{full}})
	r.queued.Wait()
	frames := r.drain()

	var got [3]protocol.ProtocolMessage
	for i := range got {
		if len(frames[i]) != 1 {
			t.Fatalf("attachment %d queued %d frames, want 1", i, len(frames[i]))
		}
		if err := protocol.Unmarshal(frames[i][0].data, protocol.FormatJSON, &got[i]); err != nil {
			t.Fatal(err)
		}
	}
	if a := got[0].Messages[0]; a.Action != protocol.MessageAppend || a.Data != "+more" {
		t.Errorf("attachment that saw the message got %v %v, want the append delta", a.Action, a.Data)
	}
	if a := got[1].Messages[0]; a.Action != protocol.MessageUpdate || a.Data != "start+more" {
		t.Errorf("attachment that did not see the message got %v %v, want the full version", a.Action, a.Data)
	}
	if a := got[2].Messages[0]; a.Action != protocol.MessageUpdate || a.Data != "start+more" {
		t.Errorf("appendMode=full attachment got %v %v, want the full version", a.Action, a.Data)
	}
}

// BenchmarkFanoutEnqueue measures shape M's hot path on one node: one
// publish on a channel with 20,000 subscriber attachments, timed from
// the publish to every attachment's frame being queued on its
// connection, with each attachment encoding its own frame (per-attachment),
// with the frame encoded once per format and shared (shared), and with
// the shared frame queued by a fan-out pool of GOMAXPROCS workers while
// the attachment goroutines stay parked (pool, DESIGN.md §5.1).
func BenchmarkFanoutEnqueue(b *testing.B) {
	const n = 20000
	payload := strings.Repeat("x", 470)
	for _, format := range []protocol.Format{protocol.FormatMsgpack, protocol.FormatJSON} {
		for _, mode := range []string{"per-attachment", "shared", "pool"} {
			b.Run(fmt.Sprintf("format=%v/%s", format, mode), func(b *testing.B) {
				opts := rigOptions{shared: mode == "shared"}
				if mode == "pool" {
					opts.pool = core.NewFanoutPool(core.DefaultFanoutWorkers(), core.DefaultFanoutThreshold)
				}
				r := newFanoutRigWith(b, n, []protocol.Format{format}, protocol.FlagSubscribe, opts)
				defer r.close()
				if mode == "pool" {
					// Let every attachment join the pool: a first publish is
					// delivered by the goroutines, which then join.
					r.publish(b, &protocol.Message{Name: "warm", Data: payload})
					r.drain()
					time.Sleep(100 * time.Millisecond)
					r.pooled.Store(0)
				}
				b.ResetTimer()
				for b.Loop() {
					r.publish(b, &protocol.Message{Name: "tick", Data: payload, ClientID: "publisher"})
					b.StopTimer()
					r.drain()
					b.StartTimer()
				}
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/fanout")
				if mode == "pool" {
					// The share of frames the pool queued (1: every one).
					b.ReportMetric(float64(r.pooled.Load())/float64(n*b.N), "pooled")
				}
			})
		}
	}
}

// TestDeliveryStagesSampled checks the sampled delivery-stage series over
// a real connection (DESIGN.md §10): a sampled connection's attachment
// records the fan-out time of a live message, and its write loop the
// frame's wait in the outbound queue.
func TestDeliveryStagesSampled(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	m := metrics.New()
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), m, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// The next connection is the one in DeliverySampleEvery that samples.
	for deliverySampleSeq.Load()%metrics.DeliverySampleEvery != metrics.DeliverySampleEvery-1 {
		deliverySampleSeq.Add(1)
	}
	ws := dial(t, srv, "")
	drainConnected(t, ws)
	attach(t, ws, "stages", protocol.FlagSubscribe)
	ch, err := manager.GetChannel(context.Background(), "stages")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ch.Publish(context.Background(), []*protocol.Message{{Name: "m", Data: "x"}}); err != nil {
		t.Fatal(err)
	}
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionMessage {
		t.Fatalf("got %v, want MESSAGE", msg.Action)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		rec := httptest.NewRecorder()
		m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		body := rec.Body.String()
		fanout := strings.Contains(body, "ably_delivery_fanout_seconds_count 1")
		// CONNECTED, ATTACHED and MESSAGE were all queued and written.
		wait := strings.Contains(body, "ably_conn_write_wait_seconds_count 3")
		if fanout && wait {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after a delivery on a sampled connection: fan-out recorded %v, write wait recorded %v", fanout, wait)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// BenchmarkFanoutAppendDelta measures the fan-out of a channel streaming
// appends (DESIGN.md §5.1, §13.3): 20,000 subscribers that have all seen
// the message, then one append after another, each delivered as a
// delta, timed from the append to every frame queued. Appends take the
// per-attachment path, so with the fan-out pool on (pool) the attachment
// goroutines deliver them; it must cost no more than without the pool
// (shared).
func BenchmarkFanoutAppendDelta(b *testing.B) {
	const n = 20000
	for _, mode := range []string{"shared", "pool"} {
		b.Run(mode, func(b *testing.B) {
			opts := rigOptions{shared: mode == "shared"}
			if mode == "pool" {
				opts.pool = core.NewFanoutPool(core.DefaultFanoutWorkers(), core.DefaultFanoutThreshold)
			}
			r := newFanoutRigWith(b, n, []protocol.Format{protocol.FormatJSON}, protocol.FlagSubscribe, opts)
			defer r.close()
			r.queued.Add(n)
			r.ch.Append(&protocol.ChannelMessage{ChannelSerial: "s000000", Messages: []*protocol.Message{{Serial: "s000000:000", Data: "start"}}})
			r.queued.Wait()
			r.drain()
			time.Sleep(100 * time.Millisecond)
			delta := &protocol.Message{Action: protocol.MessageAppend, Serial: "s000000:000", Data: "+tok"}
			i := 0
			b.ResetTimer()
			for b.Loop() {
				i++
				r.queued.Add(n)
				r.ch.Append(&protocol.ChannelMessage{ChannelSerial: fmt.Sprintf("s%06d", i), Messages: []*protocol.Message{{
					Action: protocol.MessageUpdate, Serial: "s000000:000", Data: "start+tok",
					Alt: map[string]*protocol.Message{protocol.DeltaAppend: delta},
				}}})
				r.queued.Wait()
				b.StopTimer()
				r.drain()
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/fanout")
		})
	}
}
