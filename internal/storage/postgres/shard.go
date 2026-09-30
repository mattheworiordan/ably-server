package postgres

import (
	"errors"
	"fmt"
	"strings"
)

// Channel sharding (DESIGN.md §6.4): a fixed list of databases, and a
// stable hash of the channel name to pick one. Everything a channel owns
// (its serials, log, idempotency keys, presence set, projections and
// annotations) lives on that one shard, so a publish never spans two
// databases and there is no cross-shard invariant to keep.

// ShardFor returns the shard, in [0, n), that owns channel among n
// shards. It is Lamping and Veach's jump consistent hash of a 64-bit key
// taken from the channel name (FNV-1a, then the SplitMix64 finalizer so
// every key bit depends on every name byte). The result depends only on
// the name and n, never on the process, the platform or the Go version,
// so every node that is given the same DSN list routes a channel to the
// same database. n <= 1 always returns 0.
func ShardFor(channel string, n int) int {
	if n <= 1 {
		return 0
	}
	return jumpHash(channelKey(channel), n)
}

// channelKey is the 64-bit key jumpHash consumes: FNV-1a over the name's
// bytes, mixed by the SplitMix64 finalizer. Jump hash draws its buckets
// from the key through a linear congruential step, so the key must be
// well mixed; raw FNV-1a is weak in its low bits, and those low bits are
// what the publish lanes use (fnv32a mod lanes), so the mix also keeps
// the shard choice independent of the lane choice.
func channelKey(name string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= prime64
	}
	// SplitMix64 finalizer.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// jumpHash is "A Fast, Minimal Memory, Consistent Hash Algorithm"
// (Lamping, Veach, 2014): it maps key onto [0, n) evenly, and growing n
// to n+1 moves only the keys that land in the new bucket. The server does
// not reshard (DESIGN.md §6.4), but the property means a future
// resharding tool would move about 1/(n+1) of the channels, not most of
// them.
func jumpHash(key uint64, n int) int {
	var b, j int64 = -1, 0
	for j < int64(n) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// SplitDSNs splits a --postgres-dsn value into its shard DSNs, in order
// (DESIGN.md §6.4). The list is comma-separated, but a comma only
// separates two DSNs when a postgres:// or postgresql:// URL starts right
// after it (spaces allowed), so a DSN that contains commas of its own,
// such as a multi-host URL (postgres://h1:5432,h2:5432/db) or a
// key=value DSN, stays one DSN. A list of more than one DSN therefore
// needs URL-form DSNs. An empty entry or the same DSN twice is an error:
// two shards on one schema would both claim it (the shard identity check
// in OpenSharded refuses that too).
func SplitDSNs(v string) ([]string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, errors.New("storage/postgres: empty DSN")
	}
	var out []string
	start := 0
	for i := 0; i < len(v); i++ {
		if v[i] != ',' {
			continue
		}
		rest := strings.TrimLeft(v[i+1:], " \t")
		if !hasURLScheme(rest) && rest != "" {
			continue
		}
		out = append(out, strings.TrimSpace(v[start:i]))
		start = i + 1
	}
	out = append(out, strings.TrimSpace(v[start:]))
	seen := make(map[string]int, len(out))
	for i, dsn := range out {
		if dsn == "" {
			return nil, fmt.Errorf("storage/postgres: DSN list entry %d is empty", i+1)
		}
		if j, dup := seen[dsn]; dup {
			return nil, fmt.Errorf("storage/postgres: DSN list entries %d and %d are the same database", j+1, i+1)
		}
		seen[dsn] = i
	}
	return out, nil
}

func hasURLScheme(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}
