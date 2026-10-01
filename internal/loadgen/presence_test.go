package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const presenceTestScenario = `
name = "P"
shape = "M"
message_bytes = 64
sample_percent = 100
[timing]
ramp = "1s"
hold = "1s"
drain = "1s"
[presence]
enabled = true
channels = 3
members_per_channel = 4
events_per_sec = 1
`

// presenceFixture is a presence job with every member settled and
// entered, and a fake REST endpoint whose sets the test controls.
type presenceFixture struct {
	t   *testing.T
	j   *Job
	ms  []*presenceMember
	srv *httptest.Server

	mu    sync.Mutex
	sets  map[string][]string // channel -> clientIds in the REST set
	page  int                 // members per page (0: one page)
	fail  map[string]bool
	calls int
}

func newPresenceFixture(t *testing.T) *presenceFixture {
	t.Helper()
	sc, err := ParseScenario([]byte(presenceTestScenario))
	if err != nil {
		t.Fatal(err)
	}
	f := &presenceFixture{t: t, sets: map[string][]string{}, fail: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	j, err := NewJob(JobSpec{ID: "p", RunTag: "t1", Scenario: *sc, Role: RolePresence, Count: 1,
		Endpoints: []string{strings.TrimPrefix(f.srv.URL, "http://")}, Key: "app.key:secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.j = j
	for _, m := range j.Plan.PresenceSlice(0, 1) {
		pm := &presenceMember{j: j, pm: m, conn: &Conn{}, attached: true, entered: true}
		f.ms = append(f.ms, pm)
		f.sets[m.Channel] = append(f.sets[m.Channel], m.ClientID)
	}
	return f
}

func (f *presenceFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/channels/"), "/presence")
	if f.fail[name] {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	if r.Header.Get("Authorization") == "" {
		http.Error(w, "no auth", http.StatusUnauthorized)
		return
	}
	ids := f.sets[name]
	start := 0
	if c := r.URL.Query().Get("_cursor"); c != "" {
		fmt.Sscanf(c, "%d", &start)
	}
	end := len(ids)
	if f.page > 0 && start+f.page < end {
		end = start + f.page
		w.Header().Add("Link", fmt.Sprintf(`<./presence?limit=1000&_cursor=%d>; rel="next"`, end))
	}
	type member struct {
		Action   int    `json:"action"`
		ClientID string `json:"clientId"`
	}
	var out []member
	for _, id := range ids[start:end] {
		out = append(out, member{Action: 1, ClientID: id})
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (f *presenceFixture) run() {
	f.t.Helper()
	f.j.presenceCheck(context.Background(), f.ms)
}

func (f *presenceFixture) mismatches() int64 { return f.j.checker.Count(PresenceSetMismatch) }

func TestPresenceCheckCleanSet(t *testing.T) {
	f := newPresenceFixture(t)
	f.page = 5 // more than the 4 members per channel: one page
	f.run()
	if f.mismatches() != 0 || f.j.c.presChecksDone.Load() != 3 || f.j.c.presCompared.Load() != 12 || f.j.c.presChecksFailed.Load() != 0 {
		t.Fatalf("mismatches %d done %d compared %d failed %d", f.mismatches(), f.j.c.presChecksDone.Load(), f.j.c.presCompared.Load(), f.j.c.presChecksFailed.Load())
	}
}

func TestPresenceCheckFollowsPagination(t *testing.T) {
	f := newPresenceFixture(t)
	f.page = 1 // four pages per channel
	f.run()
	if f.calls != 12 {
		t.Fatalf("%d REST calls, want 4 pages x 3 channels", f.calls)
	}
	if f.mismatches() != 0 {
		t.Fatalf("a member on a later page must be seen: %d mismatches, first %v", f.mismatches(), f.j.checker.Summary().FirstViolations)
	}
}

func TestPresenceCheckDetectsAMissingMember(t *testing.T) {
	f := newPresenceFixture(t)
	ch := f.ms[0].pm.Channel
	f.sets[ch] = f.sets[ch][1:] // the first member is entered but the server lost it
	f.run()
	v := f.j.checker.Summary().FirstViolations
	if f.mismatches() != 1 || len(v) != 1 || v[0].Kind != "presence_set_mismatch" || v[0].PubID != f.ms[0].pm.ClientID || !strings.Contains(v[0].Detail, "missing") {
		t.Fatalf("mismatches %d violations %+v", f.mismatches(), v)
	}
}

func TestPresenceCheckDetectsAStaleMember(t *testing.T) {
	f := newPresenceFixture(t)
	f.ms[1].entered = false // left, but the server still lists it
	f.run()
	if f.mismatches() != 1 || !strings.Contains(f.j.checker.Summary().FirstViolations[0].Detail, "not entered") {
		t.Fatalf("mismatches %d %+v", f.mismatches(), f.j.checker.Summary().FirstViolations)
	}
}

func TestPresenceCheckDetectsStrayClients(t *testing.T) {
	f := newPresenceFixture(t)
	ch := f.ms[0].pm.Channel
	f.sets[ch] = append(f.sets[ch], "nobody", f.ms[len(f.ms)-1].pm.ClientID) // unknown, and a member of another channel
	f.run()
	if f.mismatches() != 2 {
		t.Fatalf("mismatches %d %+v", f.mismatches(), f.j.checker.Summary().FirstViolations)
	}
}

func TestPresenceCheckSkipsUnsettledMembers(t *testing.T) {
	f := newPresenceFixture(t)
	f.ms[0].inflight.Store(1) // an operation in flight
	f.ms[1].mu.Lock()
	f.ms[1].conn = nil // reconnecting
	f.ms[1].mu.Unlock()
	ch := f.ms[0].pm.Channel
	f.sets[ch] = nil // nothing in the set: only the settled members of that channel count as missing
	f.run()
	if f.j.c.presIndeterminate.Load() != 2 || f.j.c.presCompared.Load() != 10 {
		t.Fatalf("indeterminate %d compared %d", f.j.c.presIndeterminate.Load(), f.j.c.presCompared.Load())
	}
	if f.mismatches() != 2 {
		t.Fatalf("the two settled members of the emptied channel are missing: %d", f.mismatches())
	}
}

func TestPresenceCheckFetchFailureIsACheckFailureNotAPass(t *testing.T) {
	f := newPresenceFixture(t)
	f.fail[f.ms[0].pm.Channel] = true
	f.run()
	if f.j.c.presChecksFailed.Load() != 1 || f.j.c.presChecksDone.Load() != 2 || f.j.c.presChecksPlanned.Load() != 3 {
		t.Fatalf("failed %d done %d planned %d", f.j.c.presChecksFailed.Load(), f.j.c.presChecksDone.Load(), f.j.c.presChecksPlanned.Load())
	}
}

func TestPresenceSampledAlwaysIncludesTheFirstChannelAndIgnoresTheRunTag(t *testing.T) {
	sc, err := ParseScenario([]byte(strings.Replace(presenceTestScenario, "sample_percent = 100", "sample_percent = 0", 1)))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := sc.Resolve(1, 1, "ra")
	b, _ := sc.Resolve(1, 1, "rb")
	for c := 0; c < 3; c++ {
		if a.PresenceSampled(c) != b.PresenceSampled(c) {
			t.Fatalf("channel %d: sample moved with the run tag", c)
		}
	}
	if !a.PresenceSampled(0) || a.PresenceSampled(1) || a.PresenceSampled(2) {
		t.Fatal("at sample_percent 0 only the first presence channel is checked")
	}
	if tot, _ := a.Totals(); tot.PresenceSampled != 1 {
		t.Fatalf("totals count %d sampled presence channels, want 1", tot.PresenceSampled)
	}
}

func TestPresenceMemberChannel(t *testing.T) {
	sc, _ := ParseScenario([]byte(presenceTestScenario))
	p, _ := sc.Resolve(1, 1, "t1")
	for g := 0; g < 12; g++ {
		if c, ok := p.PresenceMemberChannel(p.PresenceClientID(g)); !ok || c != g/4 {
			t.Errorf("member %d: channel %d ok=%v", g, c, ok)
		}
	}
	for _, id := range []string{"", "nobody", "t1m", "t1mx", "t1m12", "t1m-1", "other3"} {
		if _, ok := p.PresenceMemberChannel(id); ok {
			t.Errorf("%q must not be a member", id)
		}
	}
}
