package main

import (
	"testing"
	"time"
)

func TestStaggerOffsetSpreadsPublishersAcrossOneInterval(t *testing.T) {
	const n = 4
	perPub := 10.0 // one publish every 100ms per publisher
	var prev time.Duration = -1
	for i := 0; i < n; i++ {
		off := staggerOffset(i, n, perPub)
		if off <= prev {
			t.Fatalf("offset %d (%s) not after offset %d (%s)", i, off, i-1, prev)
		}
		if off >= 100*time.Millisecond {
			t.Fatalf("offset %d = %s, beyond one interval", i, off)
		}
		prev = off
	}
	if got := staggerOffset(2, 4, 10); got != 50*time.Millisecond {
		t.Fatalf("offset(2,4,10) = %s, want 50ms", got)
	}
	if staggerOffset(1, 0, 10) != 0 || staggerOffset(1, 4, 0) != 0 {
		t.Fatal("degenerate inputs must give no offset")
	}
}

func TestWindowOpensOnlyAfterSetup(t *testing.T) {
	var w window
	now := time.Now()
	if w.contains(now.UnixNano()) {
		t.Fatal("window open before set")
	}
	w.set(now, 2*time.Second, 10*time.Second)
	cases := []struct {
		at   time.Duration
		want bool
	}{
		{0, false},
		{time.Second, false},
		{2 * time.Second, true},
		{7 * time.Second, true},
		{12 * time.Second, true},
		{12*time.Second + time.Millisecond, false},
	}
	for _, c := range cases {
		if got := w.contains(now.Add(c.at).UnixNano()); got != c.want {
			t.Errorf("contains(setup+%s) = %v, want %v", c.at, got, c.want)
		}
	}
}
