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

// LoadInventory reads an inventory JSON file.
func LoadInventory(path string) (*Inventory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var inv Inventory
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&inv); err != nil {
		return nil, fmt.Errorf("inventory %s: %w", path, err)
	}
	return &inv, inv.Validate()
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
