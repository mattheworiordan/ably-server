package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
)

// REST publish benchmarks (DESIGN.md §2.2): the cost of the HTTP layer,
// routing, auth, decode, encode and metrics for the common publish — one
// JSON message, Basic key auth — with a storage stub that only stamps
// serials, so the numbers are the REST path's own.
//
//	go test -run '^$' -bench RESTPublish -benchmem ./internal/server/

const benchKey = "app.key:secret"

var benchBody = []byte(`{"name":"greeting","data":"hello world, a typical short message payload of about 60 bytes"}`)

// stampStorage is a storage.Storage whose Store only stamps ids and
// serials; it keeps nothing.
type stampStorage struct{ n atomic.Int64 }

func (s *stampStorage) Channel(_ context.Context, _ string, a storage.Appender) (storage.ChannelStore, error) {
	if a != nil {
		a.Initialize("00000000000000-000@bench", "00000000000000-000@bench")
	}
	return &stampStore{parent: s}, nil
}
func (s *stampStorage) Release(context.Context, string) error { return nil }
func (s *stampStorage) Close() error                          { return nil }

type stampStore struct {
	storage.ChannelStore
	parent *stampStorage
}

func (c *stampStore) Store(_ context.Context, msgs []*protocol.Message) (*protocol.ChannelMessage, bool, error) {
	batch, err := storage.StampMessageIDs(msgs)
	if err != nil {
		return nil, false, err
	}
	serial := "01700000000000-000@bench"
	for i, m := range msgs {
		m.Serial = serial + ":" + string(rune('0'+i%10))
	}
	c.parent.n.Add(1)
	return &protocol.ChannelMessage{ID: batch, ChannelSerial: serial, Messages: msgs}, false, nil
}

func benchMux(b *testing.B) http.Handler {
	b.Helper()
	key, err := auth.ParseAPIKey(benchKey)
	if err != nil {
		b.Fatal(err)
	}
	logger := logging.New(slog.DiscardHandler)
	manager := core.NewManager(&stampStorage{})
	m := metrics.New()
	rt := realtime.NewServer([]auth.APIKey{key}, manager, realtime.DefaultHeartbeatInterval, logger, m, nil)
	rs := rest.NewServer([]auth.APIKey{key}, manager, logger, nil, m, nil, rt)
	return newMux(rt, rs, m, false)
}

// discardWriter is a reusable http.ResponseWriter that keeps nothing.
type discardWriter struct {
	h      http.Header
	status int
}

func (w *discardWriter) Header() http.Header         { return w.h }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardWriter) WriteHeader(code int)        { w.status = code }

// BenchmarkRESTPublishHandler drives the full mux (routing, metrics
// middleware, auth, decode, publish, encode) in-process, without the
// network.
func BenchmarkRESTPublishHandler(b *testing.B) {
	mux := benchMux(b)
	authz := "Basic " + base64.StdEncoding.EncodeToString([]byte(benchKey))
	body := bytes.NewReader(benchBody)
	w := &discardWriter{h: make(http.Header)}
	b.ReportAllocs()
	for b.Loop() {
		body.Reset(benchBody)
		req, _ := http.NewRequest(http.MethodPost, "http://bench/channels/bench/messages", io.NopCloser(body))
		req.ContentLength = int64(len(benchBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authz)
		clear(w.h)
		mux.ServeHTTP(w, req)
		if w.status != http.StatusCreated {
			b.Fatalf("status %d", w.status)
		}
	}
}

// BenchmarkRESTPublishHTTP is the same publish over real keep-alive
// HTTP/1.1 connections to a local listener, from parallel clients.
func BenchmarkRESTPublishHTTP(b *testing.B) {
	srv := httptest.NewUnstartedServer(benchMux(b))
	srv.Config.IdleTimeout = DefaultHTTPIdleTimeout
	srv.Start()
	defer srv.Close()
	tr := &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	authz := "Basic " + base64.StdEncoding.EncodeToString([]byte(benchKey))
	url := srv.URL + "/channels/bench/messages"
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(benchBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", authz)
			resp, err := client.Do(req)
			if err != nil {
				b.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				b.Errorf("status %d", resp.StatusCode)
				return
			}
		}
	})
}
