package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func metricText(t *testing.T, mux http.Handler) string {
	t.Helper()
	r := httptest.NewRequest("GET", "/metrics", nil)
	r.Header.Set("Authorization", "Bearer client-secret")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatal("metrics response invalid")
	}
	return w.Body.String()
}

func TestTelemetryFallbackUsageAndPrivacy(t *testing.T) {
	var logs bytes.Buffer
	obs := newTelemetry(&logs)
	t.Cleanup(obs.close)
	p, _ := newProvider("http://localhost", "provider-secret")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}`))}, nil
	})
	p.configureFallback("http://localhost", "fallback-secret", "fallback-model")
	p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"private answer"}}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))}, nil
	})
	limiter := &stubLimiter{decision: rateDecision{allowed: true, reservation: "quota-secret"}}
	mux := newMuxWithTelemetry(p, "client-secret", limiter, obs)
	r := httptest.NewRequest("POST", "/v1/chat/completions?private=query", strings.NewReader(`{"model":"private-model","messages":[{"role":"user","content":"private prompt"}]}`))
	r.Header.Set("Authorization", "Bearer client-secret")
	r.Header.Set("X-Request-ID", "untrusted-client-id")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	obs.close()
	var event requestEvent
	if json.Unmarshal(logs.Bytes(), &event) != nil || event.Status != 200 || event.Route != "fallback" || event.Outcome != "success" || len(event.Attempts) != 2 {
		t.Fatal("request log incorrect")
	}
	if event.RequestID == "untrusted-client-id" || event.RequestID != w.Header().Get("X-Request-ID") {
		t.Fatal("request ID correlation incorrect")
	}
	if *event.Attempts[0].Usage.Total != 50 || *event.Attempts[1].Usage.Total != 7 {
		t.Fatal("lost usage from an attempt")
	}
	metrics := metricText(t, mux)
	// Primary and fallback each reserve/reconcile; metrics must include both attempts.
	for _, required := range []string{"janus_quota_admission_duration_seconds_count 2", "janus_quota_settlement_duration_seconds_count 2", "janus_usage_enqueue_duration_seconds_count 0"} {
		if !strings.Contains(metrics, required) {
			t.Fatalf("missing stage metric %s", required)
		}
	}
	for _, required := range []string{`janus_reported_tokens_total{route="primary",kind="total"} 50`, `janus_reported_tokens_total{route="fallback",kind="total"} 7`, `janus_fallback_selections_total 1`, `janus_request_duration_seconds_count 1`, `janus_chat_inflight 0`, `janus_ttft_seconds_count 0`} {
		if !strings.Contains(metrics, required) {
			t.Fatalf("missing metric %s", required)
		}
	}
	for _, secret := range []string{"client-secret", "provider-secret", "fallback-secret", "quota-secret", "private prompt", "private answer", "private-model", "private=query"} {
		if strings.Contains(logs.String()+metrics, secret) {
			t.Fatal("sensitive data leaked into telemetry")
		}
	}
	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest("GET", "/metrics", nil))
	if unauthorized.Code != 401 || limiter.calls != 1 {
		t.Fatal("metrics bypassed auth or consumed quotas")
	}
}

func TestTelemetryStreamingTTFTAndInterruptions(t *testing.T) {
	for _, test := range []struct {
		name, stream   string
		text, complete bool
	}{
		{"role only", "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n", false, true},
		{"text", "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n", true, true},
		{"incomplete", "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			obs := newTelemetry(&logs)
			t.Cleanup(obs.close)
			p, _ := newProvider("http://localhost", "test")
			p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(test.stream))}, nil
			})
			mux := newMuxWithTelemetry(p, "client-secret", &stubLimiter{decision: rateDecision{allowed: true}}, obs)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(validStreamBody))
			r.Header.Set("Authorization", "Bearer client-secret")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			obs.close()
			var event requestEvent
			json.Unmarshal(logs.Bytes(), &event)
			if w.Body.String() != test.stream || event.Status != 200 || (event.TTFT != nil) != test.text {
				t.Fatal("stream or TTFT changed")
			}
			if (event.Outcome == "success") != test.complete {
				t.Fatal("incomplete 200 stream counted as success")
			}
			if event.TTFT != nil && (*event.TTFT < 0 || *event.TTFT > event.Duration) {
				t.Fatal("invalid latency measurement")
			}
			if !strings.Contains(metricText(t, mux), `janus_usage_unknown_total{route="primary"} 1`) {
				t.Fatal("missing usage treated as zero")
			}
		})
	}
}

func TestTelemetryPreservesEarlyStreaming(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			io.WriteString(w, "data: [DONE]\n\n")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	p, _ := newProvider(upstream.URL, "test")
	obs := newTelemetry(nil)
	gateway := httptest.NewServer(newMuxWithTelemetry(p, "client-secret", &stubLimiter{decision: rateDecision{allowed: true}}, obs))
	defer gateway.Close()
	r, _ := http.NewRequest("POST", gateway.URL+"/v1/chat/completions", strings.NewReader(validStreamBody))
	r.Header.Set("Authorization", "Bearer client-secret")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	first := make([]byte, 5)
	if _, err := io.ReadFull(response.Body, first); err != nil || string(first) != "data:" {
		t.Fatal("telemetry buffered early stream")
	}
	release <- struct{}{}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
}

func TestTelemetryAbortAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		var logs bytes.Buffer
		obs := newTelemetry(&logs)
		t.Cleanup(obs.close)
		p, _ := newProvider("http://localhost", "test")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
			if canceled {
				cancel()
				return nil, context.Canceled
			}
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: breakerReadFailure{}}, nil
		})
		mux := newMuxWithTelemetry(p, "client-secret", &stubLimiter{decision: rateDecision{allowed: true}}, obs)
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(validStreamBody)).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer client-secret")
		func() {
			defer func() {
				recovered := recover()
				if !canceled && recovered != http.ErrAbortHandler {
					t.Fatal("stream abort was swallowed")
				}
				if canceled && recovered != nil {
					t.Fatal("unexpected cancellation panic")
				}
			}()
			mux.ServeHTTP(httptest.NewRecorder(), r)
		}()
		obs.close()
		var event requestEvent
		if json.Unmarshal(logs.Bytes(), &event) != nil {
			t.Fatal("missing termination log")
		}
		if canceled {
			if event.Outcome != "canceled" || event.Status != 0 {
				t.Fatal("canceled unsent response reported as success")
			}
		} else if event.Outcome != "interrupted" || event.Status != 200 {
			t.Fatal("stream abort reported as success")
		}
	}
}

type blockedLogWriter struct {
	started, release chan struct{}
	once             sync.Once
}

func (w *blockedLogWriter) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(data), nil
}

func TestTelemetrySlowLogsAndConcurrentMetrics(t *testing.T) {
	writer := &blockedLogWriter{started: make(chan struct{}), release: make(chan struct{})}
	obs := newTelemetry(writer)
	handler := obs.observe(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
	<-writer.started
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var wg sync.WaitGroup
		for i := 0; i < 400; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
				obs.serveMetrics(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
			}()
		}
		wg.Wait()
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		close(writer.release)
		t.Fatal("slow logging blocked handlers")
	}
	close(writer.release)
	obs.close()
	if obs.dropped.Load() == 0 || obs.duration.count != 401 || obs.inflight != 0 {
		t.Fatal("queue overflow or concurrent counts incorrect")
	}
	var histogram strings.Builder
	writeHistogram(&histogram, "test_seconds", "test", obs.duration)
	if !strings.Contains(histogram.String(), `test_seconds_bucket{le="+Inf"} 401`) {
		t.Fatal("histogram infinity bucket wrong")
	}
}
