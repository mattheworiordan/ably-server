package postgres

import "sync"

// rowCacheSize bounds the names one Storage remembers as having a
// channels row. A var so tests can shrink it.
var rowCacheSize = 1 << 16

// rowCache is a node-local, bounded set of channel names known to have a
// channels row (DESIGN.md §6.3). Rows are never deleted, so a name in
// the set stays true; the set only forgets names, oldest first, to stay
// within its bound. A bind of a remembered name reads the row instead of
// running ensure_channel, which writes a new row version and waits for
// the row lock of a channel another transaction is publishing on.
type rowCache struct {
	mu   sync.Mutex
	set  map[string]struct{}
	ring []string // insertion order, for eviction
	next int      // the ring slot the next insert overwrites once full
}

func newRowCache(n int) *rowCache {
	return &rowCache{set: make(map[string]struct{}, min(n, 1024)), ring: make([]string, 0, min(n, 1024))}
}

// has reports whether name is remembered.
func (c *rowCache) has(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.set[name]
	return ok
}

// add remembers name, forgetting the oldest remembered name if the cache
// is full.
func (c *rowCache) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.set[name]; ok {
		return
	}
	if len(c.ring) < rowCacheSize {
		c.ring = append(c.ring, name)
	} else {
		delete(c.set, c.ring[c.next])
		c.ring[c.next] = name
		c.next = (c.next + 1) % len(c.ring)
	}
	c.set[name] = struct{}{}
}

// len is the number of names remembered.
func (c *rowCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.set)
}
