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
}

func newFanoutRig(tb testing.TB, n int, format protocol.Format, shared bool) *fanoutRig {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	manager := core.NewManager(memory.New(memory.Options{}))
	ch, err := manager.GetChannel(ctx, "fanout")
	if err != nil {
		tb.Fatal(err)
	}
	r := &fanoutRig{ch: ch, cancel: cancel}
	logger := logging.New(slog.DiscardHandler)
	r.queued.Add(n) // the ATTACHED frames
	for i := range n {
		c := &connection{format: format, out: newOutQueue(1<<20, 10*time.Second), logger: logger}
		stream, err := ch.Attach(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		out := func(ctx context.Context, msg *protocol.ProtocolMessage) bool {
			ok := c.queue(ctx, msg)
			r.queued.Done()
			return ok
		}
		a := newAttachment(ctx, "fanout", stream.Channel(), stream, "", false, protocol.FlagSubscribe, protocol.FlagSubscribe, nil, out, fmt.Sprintf("conn-%d", i), true, nil, logger)
		if shared {
			a.outShared = func(ctx context.Context, msg *protocol.ProtocolMessage, memo memoizer) bool {
				ok := c.queueShared(ctx, msg, memo)
				r.queued.Done()
				return ok
			}
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
// §5.1): a message cm reaches every attachment on the channel as one
// encoding per wire format, byte-identical to what each attachment would
// have encoded for itself.
func TestFanoutSharesEncodedFrame(t *testing.T) {
	for _, format := range []protocol.Format{protocol.FormatJSON, protocol.FormatMsgpack} {
		r := newFanoutRig(t, 20, format, true)
		r.publish(t, &protocol.Message{Name: "m", Data: "hello"})
		frames := r.drain()
		r.close()

		first := frames[0]
		if len(first) != 1 {
			t.Fatalf("format %v: attachment 0 queued %d frames, want 1", format, len(first))
		}
		var want protocol.ProtocolMessage
		if err := protocol.Unmarshal(first[0].data, format, &want); err != nil {
			t.Fatal(err)
		}
		if want.Action != protocol.ActionMessage || len(want.Messages) != 1 || want.Messages[0].Data != "hello" {
			t.Fatalf("format %v: frame = %+v", format, want)
		}
		ownEncoding, err := protocol.Marshal(&want, format)
		if err != nil {
			t.Fatal(err)
		}
		for i, fs := range frames {
			if len(fs) != 1 {
				t.Fatalf("format %v: attachment %d queued %d frames, want 1", format, i, len(fs))
			}
			if &fs[0].data[0] != &first[0].data[0] {
				t.Errorf("format %v: attachment %d has its own encoding; want the shared one", format, i)
			}
		}
		if !bytes.Equal(first[0].data, ownEncoding) {
			t.Errorf("format %v: shared frame differs from a per-attachment encoding", format)
		}
	}
}

// TestFanoutAppendDeltaNotShared checks that a frame that needs a
// per-attachment transform keeps its own encoding: an append is a delta
// for an attachment that has seen the message and the full version for
// one that has not (DESIGN.md §13.3).
func TestFanoutAppendDeltaNotShared(t *testing.T) {
	r := newFanoutRig(t, 2, protocol.FormatJSON, true)
	defer r.close()
	delta := &protocol.Message{Action: protocol.MessageAppend, Serial: "s1", Data: "+more"}
	full := &protocol.Message{
		Action: protocol.MessageUpdate, Serial: "s1", Data: "start+more",
		Alt: map[string]*protocol.Message{protocol.DeltaAppend: delta},
	}
	// Mark the message seen on attachment 0 only, as if it had received
	// the create before attachment 1 attached.
	r.atts[0].seen["s1"] = struct{}{}

	r.queued.Add(2)
	r.ch.Append(&protocol.ChannelMessage{ChannelSerial: "zzz", Messages: []*protocol.Message{full}})
	r.queued.Wait()
	frames := r.drain()

	var got [2]protocol.ProtocolMessage
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
}

// BenchmarkFanoutEnqueue measures shape M's hot path on one node: one
// publish on a channel with 20,000 subscriber attachments, timed from
// the publish to every attachment's frame being queued on its
// connection, with each attachment encoding its own frame (per-attachment)
// and with the frame encoded once per format and shared (shared).
func BenchmarkFanoutEnqueue(b *testing.B) {
	const n = 20000
	payload := strings.Repeat("x", 470)
	for _, format := range []protocol.Format{protocol.FormatMsgpack, protocol.FormatJSON} {
		for _, shared := range []bool{false, true} {
			name := fmt.Sprintf("format=%v/per-attachment", format)
			if shared {
				name = fmt.Sprintf("format=%v/shared", format)
			}
			b.Run(name, func(b *testing.B) {
				r := newFanoutRig(b, n, format, shared)
				defer r.close()
				b.ResetTimer()
				for b.Loop() {
					r.publish(b, &protocol.Message{Name: "tick", Data: payload, ClientID: "publisher"})
					b.StopTimer()
					r.drain()
					b.StartTimer()
				}
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/fanout")
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
