package postgres

import (
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/storage"
)

// TestShardForIsStable pins the channel-to-shard mapping. Every node of a
// deployment, and every version of the server, must route a channel to
// the same database (DESIGN.md §6.4); a change here strands data, so it
// must fail a test rather than ship.
func TestShardForIsStable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key        uint64
		n2, n3, n4 int
	}{
		{"", 0xf52a15e9a9b5e89b, 0, 0, 0},
		{"a", 0x02c0bdbf481420f8, 1, 1, 1},
		{"room", 0x96d73e815314c118, 0, 0, 3},
		{"chat:lobby", 0xa43892c217aaff4b, 0, 2, 2},
		{"persisted:orders", 0xc146f1ac546fe043, 1, 1, 3},
		{"[meta]log", 0xb31f0c58d1f0dde6, 1, 1, 1},
		{"loadgen-000001", 0xcaebd3569b10db51, 1, 1, 1},
		{"ünïcødé", 0xdb4bbf3e7e951878, 1, 1, 1},
	} {
		if got := channelKey(tc.name); got != tc.key {
			t.Errorf("channelKey(%q) = %#016x, want %#016x", tc.name, got, tc.key)
		}
		for n, want := range map[int]int{2: tc.n2, 3: tc.n3, 4: tc.n4} {
			if got := ShardFor(tc.name, n); got != want {
				t.Errorf("ShardFor(%q, %d) = %d, want %d", tc.name, n, got, want)
			}
		}
	}
}

// TestShardForSingleShardIdentity: with one shard (or a nonsensical
// count) every channel is on shard 0, so a single DSN routes nowhere.
func TestShardForSingleShardIdentity(t *testing.T) {
	for i := range 10_000 {
		name := fmt.Sprintf("channel-%d", i)
		for _, n := range []int{-1, 0, 1} {
			if got := ShardFor(name, n); got != 0 {
				t.Fatalf("ShardFor(%q, %d) = %d, want 0", name, n, got)
			}
		}
	}
}

// channelNames returns count distinct channel names shaped like the
// load generator's and real applications' names.
func channelNames(count int) []string {
	out := make([]string, count)
	for i := range out {
		switch i % 4 {
		case 0:
			out[i] = fmt.Sprintf("loadgen-%06d", i)
		case 1:
			out[i] = fmt.Sprintf("room:%d", i)
		case 2:
			out[i] = fmt.Sprintf("persisted:orders:%x", i*7919)
		default:
			out[i] = fmt.Sprintf("user/%d/notifications", i)
		}
	}
	return out
}

// TestShardForDistribution hashes 100,000 channel names onto 2, 3, 4 and
// 8 shards and checks every shard gets its share within 2% (the standard
// deviation of a fair split is well under 0.5% at this size).
func TestShardForDistribution(t *testing.T) {
	names := channelNames(100_000)
	for _, n := range []int{2, 3, 4, 8} {
		counts := make([]int, n)
		for _, name := range names {
			s := ShardFor(name, n)
			if s < 0 || s >= n {
				t.Fatalf("ShardFor(%q, %d) = %d, out of range", name, n, s)
			}
			counts[s]++
		}
		want := float64(len(names)) / float64(n)
		for i, c := range counts {
			if dev := math.Abs(float64(c)-want) / want; dev > 0.02 {
				t.Errorf("n=%d: shard %d holds %d channels, want %.0f within 2%% (off by %.1f%%)", n, i, c, want, 100*dev)
			}
		}
	}
}

// TestShardForMonotonic checks the jump-hash property: going from n to
// n+1 shards moves a channel only onto the new shard, never between old
// ones, and moves about 1/(n+1) of them.
func TestShardForMonotonic(t *testing.T) {
	names := channelNames(50_000)
	for n := 1; n < 8; n++ {
		moved := 0
		for _, name := range names {
			a, b := ShardFor(name, n), ShardFor(name, n+1)
			if a != b {
				if b != n {
					t.Fatalf("%q moved from shard %d to %d going from %d to %d shards; want only moves to the new shard", name, a, b, n, n+1)
				}
				moved++
			}
		}
		want := float64(len(names)) / float64(n+1)
		if dev := math.Abs(float64(moved)-want) / want; dev > 0.05 {
			t.Errorf("%d -> %d shards moved %d channels, want about %.0f", n, n+1, moved, want)
		}
	}
}

// TestShardForIndependentOfLanes checks that the channels of one shard
// still spread evenly over the publish lanes (fnv32a mod lanes, lanes.go),
// so sharding does not leave a node's lanes idle on some shard.
func TestShardForIndependentOfLanes(t *testing.T) {
	const shards, lanes = 2, 4
	names := channelNames(100_000)
	var counts [shards][lanes]int
	for _, name := range names {
		h := fnv.New32a()
		_, _ = h.Write([]byte(name))
		counts[ShardFor(name, shards)][h.Sum32()%lanes]++
	}
	for s := range shards {
		total := 0
		for _, c := range counts[s] {
			total += c
		}
		want := float64(total) / lanes
		for l, c := range counts[s] {
			if dev := math.Abs(float64(c)-want) / want; dev > 0.03 {
				t.Errorf("shard %d lane %d holds %d channels, want %.0f within 3%%", s, l, c, want)
			}
		}
	}
}

func TestSplitDSNs(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
		err  string
	}{
		{in: "postgres://u:p@h:5432/db?sslmode=disable", want: []string{"postgres://u:p@h:5432/db?sslmode=disable"}},
		// A multi-host URL is one DSN.
		{in: "postgres://u:p@h1:5432,h2:5432/db", want: []string{"postgres://u:p@h1:5432,h2:5432/db"}},
		// A key=value DSN with commas of its own is one DSN.
		{in: "host=h1,h2 dbname=db options='-c search_path=a,b'", want: []string{"host=h1,h2 dbname=db options='-c search_path=a,b'"}},
		// The format bench/aws/lib.sh postgres_dsns writes.
		{
			in:   "postgres://ably:pw@10.0.0.1:5432/ably?sslmode=disable,postgres://ably:pw@10.0.0.2:5432/ably?sslmode=disable",
			want: []string{"postgres://ably:pw@10.0.0.1:5432/ably?sslmode=disable", "postgres://ably:pw@10.0.0.2:5432/ably?sslmode=disable"},
		},
		{
			in:   " postgres://a/db , postgresql://b/db,postgres://c:1,d:2/db ",
			want: []string{"postgres://a/db", "postgresql://b/db", "postgres://c:1,d:2/db"},
		},
		{in: "", err: "empty DSN"},
		{in: "postgres://a/db,", err: "entry 2 is empty"},
		{in: ",postgres://a/db", err: "entry 1 is empty"},
		{in: "postgres://a/db,postgres://b/db,postgres://a/db", err: "entries 1 and 3 are the same"},
	} {
		got, err := SplitDSNs(tc.in)
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("SplitDSNs(%q) = %q, %v; want error containing %q", tc.in, got, err, tc.err)
			}
			continue
		}
		if err != nil || strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("SplitDSNs(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

// TestAddBusStats checks that the sharded bus counters are the sum of the
// shards', and connected only while every shard's bus is.
func TestAddBusStats(t *testing.T) {
	a := storage.BusStats{
		Bus: "nats", Connected: true, BoundChannels: 3, Published: 10, Sweeps: 2, SweepSeconds: 0.5,
		DeliveryLag: map[string]storage.LagHistogram{"inline": {Counts: []uint64{1, 2}, Count: 2, Sum: 0.1}},
	}
	b := storage.BusStats{
		Bus: "nats", Connected: false, BoundChannels: 4, Published: 5, Sweeps: 1, SweepSeconds: 0.25,
		DeliveryLag: map[string]storage.LagHistogram{
			"inline": {Counts: []uint64{0, 3}, Count: 3, Sum: 0.2},
			"filled": {Counts: []uint64{1, 1}, Count: 1, Sum: 0.3},
		},
	}
	got := addBusStats(a, b)
	if got.Bus != "nats" || got.Connected || got.BoundChannels != 7 || got.Published != 15 || got.Sweeps != 3 || got.SweepSeconds != 0.75 {
		t.Errorf("addBusStats = %+v", got)
	}
	if in := got.DeliveryLag["inline"]; in.Count != 5 || in.Counts[0] != 1 || in.Counts[1] != 5 || math.Abs(in.Sum-0.3) > 1e-9 {
		t.Errorf("inline lag = %+v, want counts [1 5], count 5, sum 0.3", in)
	}
	if f := got.DeliveryLag["filled"]; f.Count != 1 {
		t.Errorf("filled lag = %+v, want count 1", f)
	}
	if a.DeliveryLag["inline"].Count != 2 {
		t.Error("addBusStats modified its first argument's lag map")
	}
	if both := addBusStats(storage.BusStats{Connected: true}, storage.BusStats{Connected: true}); !both.Connected {
		t.Error("two connected shards should report connected")
	}
}
