//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// strictAppender records deliveries and every violation of the Appender
// contract (storage.Appender): Append before Initialize, or a cm at or
// below the Initialize watermark, or out of order.
type strictAppender struct {
	mu         sync.Mutex
	watermark  string
	init       bool
	serials    []string
	violations []string
}

func (a *strictAppender) Initialize(current, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.init, a.watermark = true, current
}

func (a *strictAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case !a.init:
		a.violations = append(a.violations, "Append("+cm.ChannelSerial+") before Initialize")
	case cm.ChannelSerial <= a.watermark:
		a.violations = append(a.violations, fmt.Sprintf("Append(%s) at or below watermark %s", cm.ChannelSerial, a.watermark))
	case len(a.serials) > 0 && cm.ChannelSerial <= a.serials[len(a.serials)-1]:
		a.violations = append(a.violations, fmt.Sprintf("Append(%s) out of order after %s", cm.ChannelSerial, a.serials[len(a.serials)-1]))
	}
	a.serials = append(a.serials, cm.ChannelSerial)
}

func (a *strictAppender) snapshot() (serials, violations []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.serials...), append([]string(nil), a.violations...)
}

func (a *strictAppender) waitFor(t *testing.T, n int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s, _ := a.snapshot(); len(s) >= n {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	s, _ := a.snapshot()
	t.Fatalf("timed out waiting for %d deliveries; have %d: %v", n, len(s), s)
	return nil
}

func openPGNotifyNode(t *testing.T, dsn string) *Storage {
	t.Helper()
	s, err := Open(context.Background(), Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestListenReconnectDoesNotReplayPreBindHistory: a channel bound on a
// node that has never had a live delivery used to have an empty
// high-water mark, so a LISTEN reconnect replayed its whole retained
// history into the live list (Spike A report, LISTEN-bus issue 1). The
// mark must start at the watermark handed to Initialize.
func TestListenReconnectDoesNotReplayPreBindHistory(t *testing.T) {
	defer swapReconnectDelays(20*time.Millisecond, 100*time.Millisecond)()
	c := pgtest.Start(t)
	appName := fmt.Sprintf("prebind_%d", time.Now().UnixNano())
	base := c.FreshSchemaDSN(t)
	ctx := context.Background()

	writer := openPGNotifyNode(t, base)
	wch, err := writer.Channel(ctx, "hist", nil)
	if err != nil {
		t.Fatalf("writer Channel: %v", err)
	}
	for i := range 3 {
		publish(t, ctx, wch, fmt.Sprintf("before-%d", i))
	}

	node := openPGNotifyNode(t, withApplicationName(t, base, appName))
	app := &strictAppender{}
	if _, err := node.Channel(ctx, "hist", app); err != nil {
		t.Fatalf("node Channel: %v", err)
	}

	terminateListenBackend(t, c.BaseDSN(), appName)
	after := publish(t, ctx, wch, "after")

	got := app.waitFor(t, 1, 10*time.Second)
	time.Sleep(300 * time.Millisecond) // let any stray replay land
	got, violations := app.snapshot()
	for _, v := range violations {
		t.Error(v)
	}
	if len(got) != 1 || got[0] != after {
		t.Errorf("deliveries = %v, want only the post-bind publish %s", got, after)
	}
}

// TestListenReconnectReplaysAnnotations: the reconnect reconcile read
// only message and presence history, so an annotation whose NOTIFY was
// lost during a LISTEN drop never reached the node (Spike A report,
// LISTEN-bus issue 2).
func TestListenReconnectReplaysAnnotations(t *testing.T) {
	// A long first backoff so the annotation commits while the node has
	// no LISTEN connection, and its NOTIFY is certainly lost.
	defer swapReconnectDelays(700*time.Millisecond, time.Second)()
	c := pgtest.Start(t)
	appName := fmt.Sprintf("annrec_%d", time.Now().UnixNano())
	base := c.FreshSchemaDSN(t)
	ctx := context.Background()

	writer := openPGNotifyNode(t, base)
	wch, err := writer.Channel(ctx, "ann", nil)
	if err != nil {
		t.Fatalf("writer Channel: %v", err)
	}
	node := openPGNotifyNode(t, withApplicationName(t, base, appName))
	app := &strictAppender{}
	if _, err := node.Channel(ctx, "ann", app); err != nil {
		t.Fatalf("node Channel: %v", err)
	}

	target, _, err := wch.Store(ctx, []*protocol.Message{{Name: "target"}})
	if err != nil {
		t.Fatalf("Store target: %v", err)
	}
	app.waitFor(t, 1, 10*time.Second)

	terminateListenBackend(t, c.BaseDSN(), appName)
	time.Sleep(100 * time.Millisecond)
	acm, _, err := wch.StoreAnnotation(ctx, []*protocol.Annotation{{
		Action: protocol.AnnotationCreate, ClientID: "bob", Type: "reaction:distinct.v1", Name: "+1",
		MessageSerial: target.Messages[0].Serial,
	}})
	if err != nil {
		t.Fatalf("StoreAnnotation: %v", err)
	}

	got := app.waitFor(t, 2, 10*time.Second)
	if got[1] != acm.ChannelSerial {
		t.Errorf("second delivery = %s, want the annotation %s", got[1], acm.ChannelSerial)
	}
	if _, v := app.snapshot(); len(v) > 0 {
		t.Errorf("appender contract violations: %v", v)
	}
}

// TestChannelBindHoldsDeliveriesUntilInitialized: Channel() registers the
// store for NOTIFY dispatch before it reads the watermark, so a NOTIFY
// could reach the appender before Initialize (Spike A report, LISTEN-bus
// issue 3). A cm committed before the watermark read must not be
// delivered at all; one committed after it must be delivered exactly
// once, after Initialize.
func TestChannelBindHoldsDeliveriesUntilInitialized(t *testing.T) {
	c := pgtest.Start(t)
	base := c.FreshSchemaDSN(t)
	ctx := context.Background()

	writer := openPGNotifyNode(t, base)
	wch, err := writer.Channel(ctx, "bind", nil)
	if err != nil {
		t.Fatalf("writer Channel: %v", err)
	}
	node := openPGNotifyNode(t, base)

	var beforeWatermark, afterWatermark string
	bindHook := func(phase string) {
		switch phase {
		case "registered": // in the map, watermark not yet read
			beforeWatermark = publish(t, ctx, wch, "before-watermark")
		case "ensured": // watermark read, Initialize not yet called
			afterWatermark = publish(t, ctx, wch, "after-watermark")
		}
		time.Sleep(300 * time.Millisecond) // the NOTIFY reaches the node meanwhile
	}
	channelBindHook.Store(&bindHook)
	defer channelBindHook.Store(nil)

	app := &strictAppender{}
	if _, err := node.Channel(ctx, "bind", app); err != nil {
		t.Fatalf("node Channel: %v", err)
	}
	channelBindHook.Store(nil)

	got := app.waitFor(t, 1, 10*time.Second)
	time.Sleep(200 * time.Millisecond)
	got, violations := app.snapshot()
	for _, v := range violations {
		t.Error(v)
	}
	if len(got) != 1 || got[0] != afterWatermark {
		t.Errorf("deliveries = %v, want only %s (not %s, which precedes the watermark)", got, afterWatermark, beforeWatermark)
	}
}

// TestFailedReadBackIsRecovered: when the LISTEN consumer failed to read
// a notified cm back, it skipped it, and the next cm's delivery advanced
// the high-water mark past it, so it was lost for good (claims audit,
// "a failed steady-state read-back can silently lose a delivery").
func TestFailedReadBackIsRecovered(t *testing.T) {
	c := pgtest.Start(t)
	base := c.FreshSchemaDSN(t)
	ctx := context.Background()
	node := openPGNotifyNode(t, base)

	app := &strictAppender{}
	ch, err := node.Channel(ctx, "readback", app)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	var loads atomic.Int32
	failSecond := func(channel, serial string) error {
		if channel == "readback" && loads.Add(1) == 2 {
			return errors.New("injected read-back failure")
		}
		return nil
	}
	loadCMHook.Store(&failSecond)
	defer loadCMHook.Store(nil)

	var want []string
	for i := range 3 {
		want = append(want, publish(t, ctx, ch, fmt.Sprintf("m%d", i)))
	}
	got := app.waitFor(t, 3, 10*time.Second)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deliveries = %v, want %v", got, want)
		}
	}
	if _, v := app.snapshot(); len(v) > 0 {
		t.Errorf("appender contract violations: %v", v)
	}
}
