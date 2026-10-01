package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Agent is the HTTP control endpoint a generator box runs
// (ably-loadgen agent). The conductor posts JobSpecs to it, polls status,
// and fetches summaries; Prometheus scrapes /metrics. One agent can run
// several jobs at once (for example a subscriber job and a small realtime
// publisher job on the same box); they share one metrics registry.
//
//	POST /v1/jobs              start a job (body: JobSpec)
//	GET  /v1/jobs              list job statuses
//	GET  /v1/jobs/{id}         one job's status
//	GET  /v1/jobs/{id}/summary its summary (202 while running)
//	POST /v1/stop              stop every running job
//	GET  /v1/clock             this box's clock offset from NTP (501 without NTPServer)
//	DELETE /v1/jobs            forget finished jobs
//	GET  /metrics, /healthz
type Agent struct {
	M *Metrics
	// SummaryDir, if set, receives <job id>.json for each finished job.
	SummaryDir string
	// Roles, if non-empty, limits the job roles this agent accepts.
	Roles []string
	// NTPServer (host:port, UDP) is what GET /v1/clock measures this box's
	// clock against: on AWS the Amazon Time Sync address. Empty disables.
	NTPServer string

	ctx  context.Context
	mu   sync.Mutex
	jobs map[string]*Job
}

// NewAgent returns an agent whose jobs run under ctx.
func NewAgent(ctx context.Context, m *Metrics) *Agent {
	if m == nil {
		m = NewMetrics()
	}
	return &Agent{M: m, ctx: ctx, jobs: make(map[string]*Job)}
}

// Handler returns the agent's HTTP routes.
func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", a.M.Handler())
	mux.HandleFunc("GET /v1/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"roles": a.Roles})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("POST /v1/jobs", a.handleStart)
	mux.HandleFunc("GET /v1/jobs", a.handleList)
	mux.HandleFunc("DELETE /v1/jobs", a.handleForget)
	mux.HandleFunc("GET /v1/jobs/{id}", a.handleStatus)
	mux.HandleFunc("GET /v1/jobs/{id}/summary", a.handleSummary)
	mux.HandleFunc("POST /v1/stop", a.handleStop)
	mux.HandleFunc("GET /v1/clock", a.handleClock)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// Start begins a job and returns it.
func (a *Agent) Start(spec JobSpec) (*Job, error) {
	if spec.ID == "" {
		spec.ID = fmt.Sprintf("%s-%d", spec.Role, spec.Index)
	}
	if len(a.Roles) > 0 {
		ok := false
		for _, r := range a.Roles {
			ok = ok || r == spec.Role
		}
		if !ok {
			return nil, fmt.Errorf("this agent takes roles %v, not %s", a.Roles, spec.Role)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old, ok := a.jobs[spec.ID]; ok {
		select {
		case <-old.Done():
		default:
			return nil, fmt.Errorf("job %s is already running", spec.ID)
		}
	}
	job, err := NewJob(spec, a.M)
	if err != nil {
		return nil, err
	}
	a.jobs[spec.ID] = job
	go func() {
		s := job.Run(a.ctx)
		if a.SummaryDir != "" {
			if err := WriteJSONFile(filepath.Join(a.SummaryDir, spec.ID+".json"), s); err != nil {
				job.logErr("write summary: %v", err)
			}
		}
	}()
	return job, nil
}

func (a *Agent) handleStart(w http.ResponseWriter, r *http.Request) {
	var spec JobSpec
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	job, err := a.Start(spec)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, job.Status())
}

func (a *Agent) job(id string) *Job {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.jobs[id]
}

func (a *Agent) handleList(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	out := make([]JobStatus, 0, len(a.jobs))
	for _, j := range a.jobs {
		out = append(out, j.Status())
	}
	a.mu.Unlock()
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	writeJSON(w, http.StatusOK, out)
}

func (a *Agent) handleStatus(w http.ResponseWriter, r *http.Request) {
	j := a.job(r.PathValue("id"))
	if j == nil {
		writeErr(w, http.StatusNotFound, fmt.Errorf("no job %s", r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, j.Status())
}

func (a *Agent) handleSummary(w http.ResponseWriter, r *http.Request) {
	j := a.job(r.PathValue("id"))
	if j == nil {
		writeErr(w, http.StatusNotFound, fmt.Errorf("no job %s", r.PathValue("id")))
		return
	}
	s := j.Summary()
	if s == nil {
		writeJSON(w, http.StatusAccepted, j.Status())
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (a *Agent) handleClock(w http.ResponseWriter, r *http.Request) {
	if a.NTPServer == "" {
		writeErr(w, http.StatusNotImplemented, fmt.Errorf("no NTP server configured (--ntp-server)"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	off, err := MeasureClockOffset(ctx, a.NTPServer, 5, time.Second)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, off)
}

func (a *Agent) handleStop(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	for _, j := range a.jobs {
		j.Stop()
	}
	a.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopping"})
}

func (a *Agent) handleForget(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	for id, j := range a.jobs {
		select {
		case <-j.Done():
			delete(a.jobs, id)
		default:
		}
	}
	a.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// WriteJSONFile writes v as indented JSON, atomically.
func WriteJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
