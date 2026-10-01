package realtime

// DefaultAttachmentSeenMax is the default cap on the message serials one
// attachment remembers for append delivery (DESIGN.md §13.3).
const DefaultAttachmentSeenMax = 4096

// seenSet is the bounded set of message serials an attachment has
// delivered, used to decide whether an append may go out as a delta
// (DESIGN.md §13.3). It is a generational pair of maps: serials are added
// to cur; when cur reaches half the cap it becomes prev and a fresh cur
// starts, dropping the old prev. Re-adding a serial found in prev moves it
// to cur, so a serial is evicted only when half the cap of other serials
// have been recorded since it was last touched: a message that keeps
// receiving appends stays in the set. The set never holds more than the
// cap.
//
// Eviction is safe by construction: a serial the set no longer holds is
// treated as not yet seen, so its next append is delivered as the full
// rolled-up version, which every client accepts (§13.3 allows the server
// to deliver an append as a full update at any time).
//
// The maps are allocated on first add, so an attachment that never
// records a serial costs nothing. Not safe for concurrent use; the
// attachment's run goroutine owns it.
type seenSet struct {
	half int
	cur  map[string]struct{}
	prev map[string]struct{}
}

// newSeenSet returns a set holding at most limit serials (at least one).
// Each generation holds up to ceil(limit/2), and a full current
// generation rotates at once, so the two together never exceed limit.
func newSeenSet(limit int) seenSet {
	return seenSet{half: max((limit+1)/2, 1)}
}

// has reports whether serial is in the set.
func (s *seenSet) has(serial string) bool {
	if _, ok := s.cur[serial]; ok {
		return true
	}
	_, ok := s.prev[serial]
	return ok
}

// add records serial, moving it to the current generation if it is in
// the previous one, and rotates the generations when the current one is
// full.
func (s *seenSet) add(serial string) {
	if _, ok := s.cur[serial]; ok {
		return
	}
	delete(s.prev, serial)
	if s.cur == nil {
		s.cur = make(map[string]struct{})
	}
	s.cur[serial] = struct{}{}
	if len(s.cur) >= s.half {
		s.prev = s.cur
		s.cur = nil
	}
}

// len returns the number of serials held.
func (s *seenSet) len() int {
	return len(s.cur) + len(s.prev)
}
