package loadgen_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ably/ably-server/internal/loadgen"
)

// testdata/legacy-shape-m/summary.json is a real full-scale run record
// (28 nodes, shape M at 5x) written by a version 1 conductor, before the
// attach-point, node-coverage, harness-CPU, clock-offset, server-flag and
// fault-kind data existed. Its node samples, error lines and job host
// names are dropped and the image reference carries no registry, so it
// holds no address, host name or account. Re-evaluating it must keep its
// verdict.
const legacyDir = "testdata/legacy-shape-m"

// newRows are the rows a record of that vintage cannot have: they print
// "not recorded in this run" and never fail.
var newRows = []string{
	"sample coverage (attach)", "tail check coverage", "generator clock offset",
	"node metrics coverage", "generator CPU", "publisher CPU", "server configuration",
}

func readLegacy(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(legacyDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodeRecord(t *testing.T, b []byte) *loadgen.RunRecord {
	t.Helper()
	var rec loadgen.RunRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return &rec
}

func checksByName(rec *loadgen.RunRecord) map[string]loadgen.Check {
	out := map[string]loadgen.Check{}
	for _, c := range rec.Checks {
		out[c.Name] = c
	}
	return out
}

func TestLegacyRecordNamesWhatItLacks(t *testing.T) {
	rec := decodeRecord(t, readLegacy(t))
	want := []string{loadgen.RecordedAgentCPU, loadgen.RecordedClocks, loadgen.RecordedNodeCoverage, loadgen.RecordedPresenceChecks,
		loadgen.RecordedAttach, loadgen.RecordedSampledStreams, loadgen.RecordedSampledSubscribed, loadgen.RecordedServerConfig}
	slices.Sort(want)
	if !slices.Equal(rec.NotRecorded, want) {
		t.Fatalf("not recorded %q, want %q", rec.NotRecorded, want)
	}
	// A current record is never second-guessed: a key it omits is a zero.
	if cur := decodeRecord(t, []byte(`{"version":2,"plan":{}}`)); len(cur.NotRecorded) != 0 {
		t.Fatalf("version 2: %q", cur.NotRecorded)
	}
}

func TestEvaluateKeepsTheVerdictOfALegacyRecord(t *testing.T) {
	recorded := decodeRecord(t, readLegacy(t))
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
	got := checksByName(rec)
	// Every row of the original keeps its outcome and whether it gated.
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
	// The rows it cannot have are there, say so, and gate nothing.
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
	// Nothing fails that did not fail before.
	before := map[string]bool{}
	for _, c := range recorded.Checks {
		if c.Gating && !c.Pass {
			before[c.Name] = true
		}
	}
	for _, c := range rec.Checks {
		if c.Gating && !c.Pass && !before[c.Name] {
			t.Errorf("%q fails a legacy record that did not fail it before: %+v", c.Name, c)
		}
	}
	md := rec.Markdown()
	if strings.Count(md, "| not recorded in this run") != len(newRows) || !strings.Contains(md, "| n/a | n/a |") ||
		!strings.Contains(md, "This record is version 1") {
		t.Errorf("summary.md must print the rows as not recorded:\n%s", md)
	}
	if !strings.HasPrefix(md, "# Run run-20261001T141856Z-shape-m: shape M at 5x, scale 1: FAIL\n") {
		t.Errorf("title: %s", strings.SplitN(md, "\n", 2)[0])
	}
	// The footprint survives without an inventory.
	if rec.Footprint.VCPU != recorded.Footprint.VCPU || rec.Footprint.VCPU == 0 {
		t.Errorf("footprint vCPU %v, recorded %v", rec.Footprint.VCPU, recorded.Footprint.VCPU)
	}
}

func TestLegacyRecordRewrittenKeepsItsRows(t *testing.T) {
	// evaluate --write rewrites summary.json with zero values where the
	// record had nothing; read back, those still count as not recorded.
	rec, err := loadgen.EvaluateRunDir(legacyDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	again := decodeRecord(t, b)
	loadgen.Evaluate(again, loadgen.PassSpec{})
	if again.Verdict != rec.Verdict {
		t.Fatalf("verdict %s after a rewrite, %s before", again.Verdict, rec.Verdict)
	}
	for _, name := range newRows {
		if c := checksByName(again)[name]; c.Value != "not recorded in this run" || c.Gating {
			t.Errorf("after a rewrite %q = %+v", name, c)
		}
	}
}

func TestLegacyRecordWithTheFieldIsJudgedOnIt(t *testing.T) {
	// A version 1 record that does carry an input (a later version 1
	// conductor wrote it) is judged on it: zero attach claims fail.
	var m map[string]any
	if err := json.Unmarshal(readLegacy(t), &m); err != nil {
		t.Fatal(err)
	}
	m["result"].(map[string]any)["attach"] = map[string]any{}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	rec := decodeRecord(t, b)
	loadgen.Evaluate(rec, loadgen.PassSpec{})
	if c := checksByName(rec)["sample coverage (attach)"]; c.Pass || !c.Gating || !strings.HasPrefix(c.Value, "0 of 0 claims settled") {
		t.Fatalf("recorded zero claims must fail: %+v", c)
	}
}

func TestLegacyFaultWithoutAKindKeepsTheOldRelaxation(t *testing.T) {
	// A record that does not say what its fault was is relaxed as version 1
	// relaxed it: steady-state gates yes, latency gates no; a hook that
	// failed is a plain FAIL, as it was then, not INVALID.
	withFault := func(exit int) *loadgen.RunRecord {
		var m map[string]any
		if err := json.Unmarshal(readLegacy(t), &m); err != nil {
			t.Fatal(err)
		}
		m["fault"] = map[string]any{"command": "kill node2", "at_us": 1, "exit_code": exit}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		rec := decodeRecord(t, b)
		loadgen.Evaluate(rec, loadgen.PassSpec{})
		return rec
	}
	rec := withFault(0)
	got := checksByName(rec)
	if c := got["node memory growth over hold"]; c.Gating {
		t.Fatalf("legacy fault relaxation lost: %+v", c)
	}
	for _, name := range []string{"connect+attach p99", "delivery p99 (cross-node)", "REST publish ACK p99"} {
		if c := got[name]; !c.Gating {
			t.Errorf("%q: version 1 never relaxed it: %+v", name, c)
		}
	}
	if c := got["fault injection"]; !strings.Contains(c.Value, "kind not recorded in this run") || !c.Pass {
		t.Errorf("fault row %+v", c)
	}
	if rec.Verdict != "FAIL" {
		t.Errorf("verdict %q", rec.Verdict)
	}
	failed := withFault(1)
	if failed.Verdict != "FAIL" {
		t.Errorf("a failed hook on a version 1 record: verdict %q, want FAIL as it was", failed.Verdict)
	}
	if c := checksByName(failed)["node memory growth over hold"]; !c.Gating {
		t.Errorf("a failed hook relaxes nothing: %+v", c)
	}
}

// testdata/legacy-fault-all-inputs/summary.json is a version 1 record with
// every input (attach results, node coverage, harness CPU, clocks, server
// flags) and a fault without a kind that ran, written and judged by the
// version 1 conductor (987d4ce). The gates that version relaxed for any
// successful fault fail in it: growth, steadiness, zero attach claims, the
// tail floor, retries, unresolved, generator CPU, one node without an
// end-of-hold sample, deliveries at 94% of the plan. It passed.
const legacyFaultDir = "testdata/legacy-fault-all-inputs"

// renamed maps a version 1 row name to today's.
var renamed = map[string]string{"attach-point check coverage": "sample coverage (attach)"}

func TestLegacyFaultRecordWithEveryInputKeepsItsVerdict(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(legacyFaultDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	recorded := decodeRecord(t, b)
	if recorded.Version != 1 || recorded.Verdict != "PASS" || recorded.Fault == nil || recorded.Fault.Kind != "" {
		t.Fatalf("fixture: version %d verdict %s fault %+v", recorded.Version, recorded.Verdict, recorded.Fault)
	}
	if !slices.Equal(recorded.NotRecorded, []string{loadgen.RecordedFaultKind, loadgen.RecordedPresenceChecks}) {
		t.Fatalf("the fixture must carry every input but the fault kind (and presence, which it has none of): %q", recorded.NotRecorded)
	}
	rec, err := loadgen.EvaluateRunDir(legacyFaultDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Verdict != "PASS" || !rec.Pass {
		var failing []string
		for _, c := range rec.Checks {
			if c.Gating && !c.Pass {
				failing = append(failing, c.Name)
			}
		}
		t.Fatalf("verdict %s, recorded PASS; failing %v\n%s", rec.Verdict, failing, rec.Markdown())
	}
	got := checksByName(rec)
	for _, old := range recorded.Checks {
		name := old.Name
		if n, ok := renamed[name]; ok {
			name = n
		}
		c, ok := got[name]
		if !ok {
			t.Errorf("row %q disappeared", name)
			continue
		}
		if c.Pass != old.Pass || c.Gating != old.Gating {
			t.Errorf("row %q: pass=%v gating=%v, recorded pass=%v gating=%v", name, c.Pass, c.Gating, old.Pass, old.Gating)
		}
	}
	// The same record with the hook failed: a plain FAIL, as version 1 had it.
	recorded.Fault.ExitCode = 2
	loadgen.Evaluate(recorded, loadgen.PassSpec{})
	if recorded.Verdict != "FAIL" {
		t.Fatalf("failed hook: verdict %q", recorded.Verdict)
	}
}
