package loadgen

import (
	"bufio"
	"bytes"
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

// ParseMetricLabels reads the Prometheus text exposition format and
// returns the label sets of every sample of the named metric.
func ParseMetricLabels(r io.Reader, name string) ([]map[string]string, error) {
	var out []map[string]string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	prefix := name + "{"
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		end := strings.LastIndexByte(line, '}')
		if end < 0 {
			continue
		}
		labels := map[string]string{}
		rest := line[len(prefix):end]
		for rest != "" {
			eq := strings.IndexByte(rest, '=')
			if eq < 0 || eq+1 >= len(rest) || rest[eq+1] != '"' {
				break
			}
			key := strings.TrimSpace(rest[:eq])
			i := eq + 2
			var val strings.Builder
			for i < len(rest) && rest[i] != '"' {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
				}
				val.WriteByte(rest[i])
				i++
			}
			labels[key] = val.String()
			rest = strings.TrimPrefix(strings.TrimSpace(rest[min(i+1, len(rest)):]), ",")
			rest = strings.TrimSpace(rest)
		}
		out = append(out, labels)
	}
	return out, sc.Err()
}

// Server configuration gauges the nodes export (DESIGN.md §10), read so a
// run record shows the flags the nodes actually ran with.
const (
	metricPublishLanes     = "ably_publish_lanes"
	metricLingerMax        = "ably_publish_linger_max_seconds"
	metricStorageShards    = "ably_storage_shards"
	metricBusInfo          = "ably_bus_info"
	metricBusSweepInterval = "ably_bus_sweep_interval_seconds"
)

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
	metricPublishLanes,
	metricLingerMax,
	metricStorageShards,
	metricBusSweepInterval,
}

// NodeSample is one scrape of one node.
type NodeSample struct {
	Node   string             `json:"node"`
	AtUS   int64              `json:"at_us"`
	Phase  string             `json:"phase"`
	Values map[string]float64 `json:"values,omitempty"`
	// Info is the label set of ably_bus_info (bus, mode) when exported.
	Info  map[string]string `json:"info,omitempty"`
	Error string            `json:"error,omitempty"`
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		s.Error = err.Error()
		return s
	}
	s.Values, err = ParseMetricSums(bytes.NewReader(body), nodeMetricNames...)
	if err != nil {
		s.Error = err.Error()
		return s
	}
	if info, _ := ParseMetricLabels(bytes.NewReader(body), metricBusInfo); len(info) > 0 {
		s.Info = info[0]
	}
	return s
}
