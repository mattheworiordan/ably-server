package postgres

import (
	"testing"
	"time"
)

// TestWakeNotifierOverflowPolicy checks the coalesced notifier's pending
// set (DESIGN.md §7.2): one entry per channel however often it is
// written, capped at maxPending; a mark for a new channel beyond the cap
// is dropped and counted as overflow, and a failed flush's wake-ups go
// back within the same cap.
func TestWakeNotifierOverflowPolicy(t *testing.T) {
	s := &Storage{}
	w := newWakeNotifier(s, time.Hour, 2)

	if !w.mark("c1", "s1") || !w.mark("c2", "s1") {
		t.Fatal("marks within the cap were dropped")
	}
	if !w.mark("c1", "s3") || !w.mark("c1", "s2") {
		t.Fatal("a mark for a channel already pending was dropped")
	}
	if w.mark("c3", "s1") {
		t.Fatal("a mark for a third channel with the cap at 2 was accepted")
	}
	if got := s.stats.overflow.Load(); got != 1 {
		t.Errorf("overflow = %d, want 1", got)
	}

	batch := w.take()
	if len(batch) != 2 || batch["c1"] != "s3" || batch["c2"] != "s1" {
		t.Fatalf("pending = %v, want c1:s3 (the latest serial) and c2:s1", batch)
	}
	if len(w.take()) != 0 {
		t.Fatal("take did not empty the pending set")
	}

	// A failed flush of three channels puts back only what fits.
	if !w.mark("c9", "s9") {
		t.Fatal("mark after take was dropped")
	}
	w.retain([]string{"c1", "c2", "c9"}, []string{"s3", "s1", "s8"})
	batch = w.take()
	if len(batch) != 2 || batch["c9"] != "s9" {
		t.Fatalf("pending after retain = %v, want 2 entries keeping c9's newer serial s9", batch)
	}
	if got := s.stats.overflow.Load(); got != 2 {
		t.Errorf("overflow after retain = %d, want 2", got)
	}
}

// TestParseNotifyModeDefaultsToCoalesced pins the default notify mode
// (plan §2: the small deployment runs coalesced).
func TestParseNotifyModeDefaultsToCoalesced(t *testing.T) {
	for in, want := range map[string]NotifyMode{"": NotifyCoalesced, "coalesced": NotifyCoalesced, "transactional": NotifyTransactional} {
		got, err := ParseNotifyMode(in)
		if err != nil || got != want {
			t.Errorf("ParseNotifyMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseNotifyMode("publish"); err == nil {
		t.Error(`ParseNotifyMode("publish") = nil error, want an error`)
	}
}

// TestParseBusDefaultsToPGNotify pins the default bus: the shipped one.
func TestParseBusDefaultsToPGNotify(t *testing.T) {
	for in, want := range map[string]string{"": BusPGNotify, "pgnotify": BusPGNotify, "postgres": BusPostgres, "nats": BusNATS} {
		got, err := ParseBus(in)
		if err != nil || got != want {
			t.Errorf("ParseBus(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseBus("redis"); err == nil {
		t.Error(`ParseBus("redis") = nil error, want an error`)
	}
}
