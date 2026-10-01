package postgres

import (
	"sort"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

// subAppender is an appender that reports a fixed subscriber state
// (storage.SubscriberReporter).
type subAppender struct{ subscribed bool }

func (a *subAppender) Initialize(string, string)       {}
func (a *subAppender) Append(*protocol.ChannelMessage) {}
func (a *subAppender) HasSubscribers() bool            { return a.subscribed }

// plainAppender cannot say whether it has subscribers.
type plainAppender struct{}

func (plainAppender) Initialize(string, string)       {}
func (plainAppender) Append(*protocol.ChannelMessage) {}

func sweepNames(s *Storage) []string {
	var names []string
	for _, cs := range s.sweepStores() {
		names = append(names, cs.name)
	}
	sort.Strings(names)
	return names
}

// TestSweepStoresSelection: the sweep reads a bound channel only while
// its appender has a local subscriber, or when the appender cannot say;
// a store with no appender (storage-only or transient) is never swept.
func TestSweepStoresSelection(t *testing.T) {
	sub, rest := &subAppender{subscribed: true}, &subAppender{}
	s := &Storage{channels: map[string]*channelStore{
		"attached":  {name: "attached", appender: sub},
		"rest-only": {name: "rest-only", appender: rest},
		"unknown":   {name: "unknown", appender: plainAppender{}},
		"no-app":    {name: "no-app"},
	}}

	if got, want := sweepNames(s), []string{"attached", "unknown"}; !equal(got, want) {
		t.Errorf("sweeps %v, want %v", got, want)
	}

	// The set follows the subscriber state at each sweep.
	rest.subscribed, sub.subscribed = true, false
	if got, want := sweepNames(s), []string{"rest-only", "unknown"}; !equal(got, want) {
		t.Errorf("after the subscriber moved: sweeps %v, want %v", got, want)
	}
}
