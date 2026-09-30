package loadgen

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Inventory is the fleet a run uses: the server nodes and the generator
// agents. WS5's scripts write it (bench/aws/README.md); locally the
// conductor builds one for the processes it spawns.
type Inventory struct {
	// Key is the API key (default: the ABLY_SERVER_KEYS environment
	// variable, first entry).
	Key string `json:"key,omitempty"`
	// Environment describes the fleet for the run record (bus mode,
	// storage, region); free-form, never customer-identifying.
	Environment map[string]string `json:"environment,omitempty"`
	Nodes       []InventoryNode   `json:"nodes"`
	Agents      []InventoryAgent  `json:"agents"`
}

// InventoryNode is one ably-server node.
type InventoryNode struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`          // host:port for WebSocket and REST
	Metrics  string `json:"metrics,omitempty"` // URL of its /metrics (debug listener)
	// Instance type, vCPU and memory, for the footprint figures.
	InstanceType string  `json:"instance_type,omitempty"`
	VCPU         float64 `json:"vcpu,omitempty"`
	MemoryGB     float64 `json:"memory_gb,omitempty"`
}

// InventoryAgent is one ably-loadgen agent and the roles it takes.
type InventoryAgent struct {
	Name  string   `json:"name"`
	URL   string   `json:"url"`
	Roles []string `json:"roles"`
	// Workers and RealtimeConns override the job defaults for this agent.
	Workers       int `json:"workers,omitempty"`
	RealtimeConns int `json:"realtime_conns,omitempty"`
}

// LoadInventory reads an inventory JSON file: this package's format, or
// the one bench/aws/60-run.sh renders from STATE (recognised by its
// "generators" or "api_key" keys, see FleetInventory).
func LoadInventory(path string) (*Inventory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("inventory %s: %w", path, err)
	}
	_, fleet := probe["generators"]
	if _, ok := probe["api_key"]; ok {
		fleet = true
	}
	if fleet {
		var fi FleetInventory
		if err := json.Unmarshal(b, &fi); err != nil {
			return nil, fmt.Errorf("inventory %s: %w", path, err)
		}
		inv, err := fi.Inventory()
		if err != nil {
			return nil, fmt.Errorf("inventory %s: %w", path, err)
		}
		return inv, inv.Validate()
	}
	var inv Inventory
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&inv); err != nil {
		return nil, fmt.Errorf("inventory %s: %w", path, err)
	}
	return &inv, inv.Validate()
}

// FleetInventory is the inventory bench/aws/60-run.sh writes for a cloud
// run. Generators take the subscriber, realtime-publisher and presence
// roles, publishers the rest-publisher role (generators take it too when
// there are no publishers).
type FleetInventory struct {
	RunID       string `json:"run_id"`
	Bus         string `json:"bus"`
	ServerImage string `json:"server_image"`
	APIKey      string `json:"api_key"`
	Nodes       []struct {
		HTTP    string `json:"http"`
		WS      string `json:"ws"`
		Metrics string `json:"metrics"`
		// Optional, for the footprint.
		InstanceType string  `json:"instance_type"`
		VCPU         float64 `json:"vcpu"`
		MemoryGB     float64 `json:"memory_gb"`
	} `json:"nodes"`
	NATS       string       `json:"nats"`
	Generators []FleetAgent `json:"generators"`
	Publishers []FleetAgent `json:"publishers"`
	Postgres   struct {
		Shards []struct {
			ID       string `json:"id"`
			Storage  string `json:"storage"`
			Shard    any    `json:"shard"`
			Endpoint string `json:"endpoint"`
		} `json:"shards"`
	} `json:"postgres"`
	Prometheus string `json:"prometheus"`
}

// FleetAgent is one agent in a FleetInventory: agent is host:port of its
// control endpoint.
type FleetAgent struct {
	Agent   string `json:"agent"`
	Metrics string `json:"metrics"`
}

// Inventory converts to the conductor's form.
func (fi *FleetInventory) Inventory() (*Inventory, error) {
	inv := &Inventory{Key: fi.APIKey, Environment: map[string]string{}}
	set := func(k, v string) {
		if v != "" {
			inv.Environment[k] = v
		}
	}
	set("bus", fi.Bus)
	set("server_image", fi.ServerImage)
	if fi.NATS != "" {
		set("nats_servers", fmt.Sprint(len(strings.Split(fi.NATS, ","))))
	}
	if n := len(fi.Postgres.Shards); n > 0 {
		set("postgres_shards", fmt.Sprint(n))
		set("storage", fi.Postgres.Shards[0].Storage)
	}
	for i, n := range fi.Nodes {
		ep := n.HTTP
		if ep == "" {
			ep = n.WS
		}
		for _, p := range []string{"http://", "https://", "ws://", "wss://"} {
			ep = strings.TrimPrefix(ep, p)
		}
		ep = strings.TrimRight(ep, "/")
		if ep == "" {
			return nil, fmt.Errorf("node %d has no http or ws address", i)
		}
		inv.Nodes = append(inv.Nodes, InventoryNode{
			Name: fmt.Sprintf("node-%d", i+1), Endpoint: ep, Metrics: n.Metrics,
			InstanceType: n.InstanceType, VCPU: n.VCPU, MemoryGB: n.MemoryGB,
		})
	}
	url := func(a string) string {
		if strings.HasPrefix(a, "http://") || strings.HasPrefix(a, "https://") {
			return a
		}
		return "http://" + a
	}
	genRoles := []string{RoleSubscriber, RoleRealtime, RolePresence}
	if len(fi.Publishers) == 0 {
		genRoles = append(genRoles, RoleREST)
	}
	for i, g := range fi.Generators {
		inv.Agents = append(inv.Agents, InventoryAgent{Name: fmt.Sprintf("gen-%d", i+1), URL: url(g.Agent), Roles: genRoles})
	}
	for i, p := range fi.Publishers {
		inv.Agents = append(inv.Agents, InventoryAgent{Name: fmt.Sprintf("pub-%d", i+1), URL: url(p.Agent), Roles: []string{RoleREST}})
	}
	return inv, nil
}

// Validate checks the inventory.
func (inv *Inventory) Validate() error {
	if len(inv.Nodes) == 0 {
		return fmt.Errorf("inventory: no nodes")
	}
	for i, n := range inv.Nodes {
		if n.Endpoint == "" {
			return fmt.Errorf("inventory: node %d has no endpoint", i)
		}
	}
	if len(inv.Agents) == 0 {
		return fmt.Errorf("inventory: no agents")
	}
	for i, a := range inv.Agents {
		if a.URL == "" {
			return fmt.Errorf("inventory: agent %d has no url", i)
		}
		for _, r := range a.Roles {
			switch r {
			case RoleSubscriber, RoleREST, RoleRealtime, RolePresence:
			default:
				return fmt.Errorf("inventory: agent %s: unknown role %q", a.Name, r)
			}
		}
	}
	return nil
}

// Endpoints returns the node endpoints in inventory order.
func (inv *Inventory) Endpoints() []string {
	out := make([]string, len(inv.Nodes))
	for i, n := range inv.Nodes {
		out[i] = n.Endpoint
	}
	return out
}

// agentsFor returns the agents taking role, in inventory order.
func (inv *Inventory) agentsFor(role string) []InventoryAgent {
	var out []InventoryAgent
	for _, a := range inv.Agents {
		for _, r := range a.Roles {
			if r == role {
				out = append(out, a)
				break
			}
		}
	}
	return out
}
