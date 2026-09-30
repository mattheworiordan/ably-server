package loadgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The shape bench/aws/60-run.sh renders (jq over STATE).
const fleetInventory = `{
  "run_id": "run-20261003T100000Z-smoke-1pct",
  "bus": "nats",
  "server_image": "repo/ably-server:abc123",
  "api_key": "app.key:secret",
  "nodes": [
    {"http": "http://10.0.1.11:8080", "ws": "ws://10.0.1.11:8080", "metrics": "http://10.0.1.11:6060/metrics"},
    {"http": "http://10.0.1.12:8080", "ws": "ws://10.0.1.12:8080", "metrics": "http://10.0.1.12:6060/metrics"}
  ],
  "nats": "nats://10.0.1.21:4222,nats://10.0.1.22:4222,nats://10.0.1.23:4222",
  "generators": [{"agent": "10.0.2.31:9200", "metrics": "http://10.0.2.31:9101/metrics"}],
  "publishers": [{"agent": "10.0.3.41:9200", "metrics": "http://10.0.3.41:9101/metrics"}],
  "postgres": {"shards": [{"id": "db1", "storage": "io2", "shard": 0, "endpoint": "db1.example:5432"}]},
  "prometheus": "http://127.0.0.1:9090"
}`

func TestLoadFleetInventory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, []byte(fleetInventory), 0o644); err != nil {
		t.Fatal(err)
	}
	inv, err := LoadInventory(path)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Key != "app.key:secret" || len(inv.Nodes) != 2 || inv.Nodes[1].Endpoint != "10.0.1.12:8080" || inv.Nodes[0].Metrics != "http://10.0.1.11:6060/metrics" {
		t.Fatalf("nodes/key: %+v", inv)
	}
	env := inv.Environment
	if env["bus"] != "nats" || env["nats_servers"] != "3" || env["postgres_shards"] != "1" || env["storage"] != "io2" {
		t.Errorf("environment %v", env)
	}
	if got := inv.agentsFor(RoleSubscriber); len(got) != 1 || got[0].URL != "http://10.0.2.31:9200" {
		t.Errorf("subscribers %+v", got)
	}
	if got := inv.agentsFor(RoleREST); len(got) != 1 || got[0].URL != "http://10.0.3.41:9200" {
		t.Errorf("rest publishers %+v", got)
	}
	if got := inv.agentsFor(RoleRealtime); len(got) != 1 || got[0].Name != "gen-1" {
		t.Errorf("realtime publishers %+v", got)
	}
	// With no publisher boxes the generators also publish over REST.
	noPub := strings.Replace(fleetInventory, `"publishers": [{"agent": "10.0.3.41:9200", "metrics": "http://10.0.3.41:9101/metrics"}]`, `"publishers": []`, 1)
	if err := os.WriteFile(path, []byte(noPub), 0o644); err != nil {
		t.Fatal(err)
	}
	inv, err = LoadInventory(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := inv.agentsFor(RoleREST); len(got) != 1 || got[0].Name != "gen-1" {
		t.Errorf("rest publishers without publisher boxes: %+v", got)
	}
}

func TestLoadNativeInventoryRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, []byte(`{"nodes":[{"endpoint":"a:1"}],"agents":[{"url":"http://x","roles":["subscriber"]}],"bogus":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInventory(path); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestAgentRoleRestriction(t *testing.T) {
	a := NewAgent(t.Context(), nil)
	a.Roles = []string{RoleREST}
	sc, err := ParseScenario([]byte(testScenario))
	if err != nil {
		t.Fatal(err)
	}
	spec := JobSpec{ID: "x", RunTag: "t", Scenario: *sc, Role: RoleSubscriber, Count: 1, Endpoints: []string{"127.0.0.1:1"}, Key: "a.b:c"}
	if _, err := a.Start(spec); err == nil || !strings.Contains(err.Error(), "takes roles") {
		t.Fatalf("subscriber job on a publisher agent: %v", err)
	}
}
