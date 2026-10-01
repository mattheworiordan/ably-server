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

func TestAgentHostEndpoint(t *testing.T) {
	a := NewAgent(context.Background(), nil)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/host")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	want, werr := ReadHostCPU()
	if werr != nil {
		if resp.StatusCode != http.StatusNotImplemented {
			t.Fatalf("no /proc/stat: HTTP %d, want 501", resp.StatusCode)
		}
		return
	}
	var h HostCPU
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&h) != nil || h.CPUs != want.CPUs || h.Total < want.Total {
		t.Fatalf("HTTP %d host %+v, want about %+v", resp.StatusCode, h, want)
	}
}
