package postgres

import (
	"fmt"
	"testing"
)

// TestRowCacheBounded: the known-row cache remembers names up to its
// bound and then forgets the oldest first, never growing past it.
func TestRowCacheBounded(t *testing.T) {
	c := newRowCache(3)
	for i := range 3 {
		c.add(fmt.Sprintf("c%d", i))
	}
	c.add("c1") // already known: no change
	for i := range 3 {
		if !c.has(fmt.Sprintf("c%d", i)) {
			t.Fatalf("c%d forgotten before the cache was full", i)
		}
	}
	c.add("c3")
	c.add("c4")
	if c.has("c0") || c.has("c1") {
		t.Fatal("the oldest names were not forgotten first")
	}
	for _, n := range []string{"c2", "c3", "c4"} {
		if !c.has(n) {
			t.Fatalf("%s forgotten out of order", n)
		}
	}
	if got := c.len(); got != 3 {
		t.Fatalf("len = %d, want the bound 3", got)
	}
}
