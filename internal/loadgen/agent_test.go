package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAgentClockEndpoint(t *testing.T) {
	a := NewAgent(context.Background(), nil)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/clock")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("no --ntp-server: HTTP %d, want 501", resp.StatusCode)
	}
	a.NTPServer = fakeNTP(t, 12*time.Millisecond, nil)
	resp, err = http.Get(srv.URL + "/v1/clock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var off ClockOffset
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&off) != nil {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}
	if off.OffsetUS > -8_000 || off.OffsetUS < -16_000 || off.Samples != 5 {
		t.Fatalf("offset %+v, want about -12000 us from 5 samples", off)
	}
}

// TestAgentHostEndpoint: /v1/host serves this box's CPU reading, taken
// while the request is served, so it lies between a reading taken before
// the request and one taken after. (The test used to read its reference
// after the request and require the served reading to be at least it,
// which fails whenever a CPU tick passes between the two reads.)
func TestAgentHostEndpoint(t *testing.T) {
	a := NewAgent(context.Background(), nil)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	before, berr := ReadHostCPU()
	resp, err := http.Get(srv.URL + "/v1/host")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if berr != nil {
		if resp.StatusCode != http.StatusNotImplemented {
			t.Fatalf("no /proc/stat: HTTP %d, want 501", resp.StatusCode)
		}
		return
	}
	var h HostCPU
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&h) != nil {
		t.Fatalf("HTTP %d, want 200 and a host reading", resp.StatusCode)
	}
	after, err := ReadHostCPU()
	if err != nil {
		t.Fatalf("read /proc/stat after the request: %v", err)
	}
	if h.CPUs != before.CPUs || h.Total < before.Total || h.Total > after.Total || h.Busy < before.Busy || h.Busy > after.Busy {
		t.Fatalf("host %+v, want a reading between %+v and %+v", h, before, after)
	}
}
