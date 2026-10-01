package loadgen_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/loadgen"
)

// testdata/legacy-shape-m/summary.json is a real full-scale run record (28
// nodes, shape M at 5x) written before the attach-point, node-coverage,
// harness-CPU, clock-offset, server-flag and fault-kind data existed, with
// its node samples dropped and every address, host name and account
// removed. Re-evaluating it must keep its verdict.
const legacyDir = "testdata/legacy-shape-m"

// newRows are the rows a record of that vintage cannot have: they print
// "not recorded in this run" and never fail.
var newRows = []string{
	"sample coverage (attach)", "tail check coverage", "negative latency (clock skew)", "generator clock offset",
	"node metrics coverage", "generator CPU", "publisher CPU", "server configuration", "publish retries", "unresolved publishes",
}

func loadRecorded(t *testing.T) *loadgen.RunRecord {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(legacyDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec loadgen.RunRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return &rec
}

func TestEvaluateKeepsTheVerdictOfALegacyRecord(t *testing.T) {
	recorded := loadRecorded(t)
	if recorded.Version != 1 || recorded.Verdict != "FAIL" {
		t.Fatalf("fixture is version %d verdict %s", recorded.Version, recorded.Verdict)
	}
	rec, err := loadgen.EvaluateRunDir(legacyDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Verdict != recorded.Verdict || rec.Pass != recorded.Pass {
		t.Fatalf("verdict %s (pass %v), recorded %s", rec.Verdict, rec.Pass, recorded.Verdict)
	}
	// Every row of the original keeps its outcome and whether it gated.
	got := map[string]loadgen.Check{}
	for _, c := range rec.Checks {
		got[c.Name] = c
	}
	for _, old := range recorded.Checks {
		c, ok := got[old.Name]
		if !ok {
			t.Errorf("row %q disappeared", old.Name)
			continue
		}
		if c.Pass != old.Pass || c.Gating != old.Gating {
			t.Errorf("row %q: pass=%v gating=%v, recorded pass=%v gating=%v", old.Name, c.Pass, c.Gating, old.Pass, old.Gating)
		}
	}
	// The new rows are there, say so, and gate nothing.
	for _, name := range newRows {
		c, ok := got[name]
		if !ok {
			t.Errorf("no %q row", name)
			continue
		}
		if c.Value != "not recorded in this run" || c.Gating || !c.Pass {
			t.Errorf("row %q = %+v, want not recorded, not gating, not failing", name, c)
		}
	}
	for _, c := range rec.Checks {
		if c.Gating && !c.Pass {
			if _, was := map[string]bool{"delivery p50 (cross-node)": true, "delivery p99 (cross-node)": true, "REST publish ACK p99": true, "node memory growth over hold": true}[c.Name]; !was {
				t.Errorf("%q fails a legacy record that did not fail it before", c.Name)
			}
		}
	}
	md := rec.Markdown()
	if strings.Count(md, "not recorded in this run") < len(newRows) {
		t.Errorf("summary.md must print the rows as not recorded:\n%s", md)
	}
	if !strings.Contains(md, "FAIL") || strings.Contains(md, "INVALID") {
		t.Errorf("title: %s", strings.SplitN(md, "\n", 2)[0])
	}
}

func TestLegacyRecordKeepsItsOwnCriteria(t *testing.T) {
	// The record's deliveries were at 100% of the plan; take it to 94%,
	// which the version-1 gate (90%) accepted and the current one (99%)
	// does not. A version-1 record is judged by the first.
	recorded := loadRecorded(t)
	recorded.Result.Deliveries.Rate = 0.94 * recorded.Plan.DeliveriesPerSec
	loadgen.Evaluate(recorded, loadgen.PassSpec{})
	for _, c := range recorded.Checks {
		if c.Name == "deliveries vs plan" && (!c.Pass || !strings.Contains(c.Limit, "90%")) {
			t.Fatalf("legacy record: %+v", c)
		}
	}
	recorded.Version = loadgen.RunRecordVersion
	loadgen.Evaluate(recorded, loadgen.PassSpec{})
	for _, c := range recorded.Checks {
		if c.Name == "deliveries vs plan" && c.Pass {
			t.Fatalf("a current record is held to 99%%: %+v", c)
		}
	}
}

func TestLegacyFaultRecordKeepsTheOldRelaxation(t *testing.T) {
	// Before fault kinds a fault relaxed growth and steadiness, whatever the
	// hook's exit code: old records keep that, and are never INVALID.
	rec := loadRecorded(t)
	rec.Fault = &loadgen.FaultRecord{Command: "kill node2", ExitCode: 1}
	rec.NodeStats.MemoryGrowth = 0.5
	loadgen.Evaluate(rec, loadgen.PassSpec{})
	for _, c := range rec.Checks {
		if c.Name == "node memory growth over hold" && c.Gating {
			t.Fatalf("legacy fault relaxation lost: %+v", c)
		}
	}
	if strings.HasPrefix(rec.Verdict, "INVALID") {
		t.Fatalf("verdict %q", rec.Verdict)
	}
}
