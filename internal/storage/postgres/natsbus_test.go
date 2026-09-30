package postgres

import (
	"fmt"
	"testing"
)

// TestShardIndexIsStableAndSpreads: a channel always maps to the same
// dispatch shard (which keeps its bus messages in order), and many
// channels spread over every shard.
func TestShardIndexIsStableAndSpreads(t *testing.T) {
	const shards = 16
	counts := make([]int, shards)
	for i := range 16000 {
		name := fmt.Sprintf("room-%d", i)
		a, b := shardIndex(name, shards), shardIndex(name, shards)
		if a != b {
			t.Fatalf("shardIndex(%q) = %d then %d", name, a, b)
		}
		if a < 0 || a >= shards {
			t.Fatalf("shardIndex(%q) = %d, out of [0,%d)", name, a, shards)
		}
		counts[a]++
	}
	for i, n := range counts {
		if n < 500 || n > 1500 {
			t.Errorf("shard %d got %d of 16000 channels; want roughly 1000", i, n)
		}
	}
	if got := shardIndex("x", 1); got != 0 {
		t.Errorf("shardIndex with one shard = %d, want 0", got)
	}
}
