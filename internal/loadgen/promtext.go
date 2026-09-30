package loadgen

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseMetricSums reads the Prometheus text exposition format and returns,
// for each wanted metric name, the sum of its samples across label sets
// (for a gauge or counter without labels, just its value). Names not
// present are absent from the map.
func ParseMetricSums(r io.Reader, wanted ...string) (map[string]float64, error) {
	want := make(map[string]bool, len(wanted))
	for _, w := range wanted {
		want[w] = true
	}
	out := make(map[string]float64)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd <= 0 {
			continue
		}
		name := line[:nameEnd]
		if !want[name] {
			continue
		}
		rest := line[nameEnd:]
		if rest[0] == '{' {
			close := strings.LastIndexByte(rest, '}')
			if close < 0 {
				continue
			}
			rest = rest[close+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out, sc.Err()
}

// Node metric names the conductor samples from each ably-server node's
// debug listener (DESIGN.md §10).
var nodeMetricNames = []string{
	"process_resident_memory_bytes",
	"process_cpu_seconds_total",
	"go_goroutines",
	"go_memstats_heap_inuse_bytes",
	"ably_connections_open",
	"ably_channels_bound",
	"ably_messages_published_total",
	"ably_messages_delivered_total",
}

// NodeSample is one scrape of one node.
type NodeSample struct {
	Node   string             `json:"node"`
	AtUS   int64              `json:"at_us"`
	Phase  string             `json:"phase"`
	Values map[string]float64 `json:"values,omitempty"`
	Error  string             `json:"error,omitempty"`
}

func scrapeNode(ctx context.Context, client *http.Client, node, url, phase string) NodeSample {
	s := NodeSample{Node: node, AtUS: time.Now().UnixMicro(), Phase: phase}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		s.Error = err.Error()
		return s
	}
	resp, err := client.Do(req)
	if err != nil {
		s.Error = err.Error()
		return s
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		s.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return s
	}
	s.Values, err = ParseMetricSums(resp.Body, nodeMetricNames...)
	if err != nil {
		s.Error = err.Error()
	}
	return s
}
