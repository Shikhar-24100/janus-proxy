package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type traceKey struct{}

type attemptObservation struct {
	Route    string      `json:"route"`
	Outcome  string      `json:"outcome"`
	Status   int         `json:"upstream_status"`
	Duration float64     `json:"duration_ms"`
	Usage    *tokenUsage `json:"usage"`
}

type requestTrace struct {
	metrics         *telemetry
	tenantID        string
	started         time.Time
	route           string
	stream          bool
	streamStarted   bool
	streamComplete  bool
	fallback        bool
	ttft            *float64
	attempts        []attemptObservation
	cache           string
	cacheWriteError bool
	overloaded      bool
	release         func()
}

func traceFrom(r *http.Request) *requestTrace {
	trace, _ := r.Context().Value(traceKey{}).(*requestTrace)
	return trace
}

type requestEvent struct {
	TenantID        string               `json:"tenant_id,omitempty"`
	Time            time.Time            `json:"time"`
	Event           string               `json:"event"`
	RequestID       string               `json:"request_id"`
	Route           string               `json:"route"`
	Status          int                  `json:"status"`
	Outcome         string               `json:"outcome"`
	Stream          bool                 `json:"stream"`
	Duration        float64              `json:"duration_ms"`
	TTFT            *float64             `json:"ttft_ms"`
	Attempts        []attemptObservation `json:"attempts"`
	Cache           string               `json:"cache"`
	CacheWriteError bool                 `json:"cache_write_error"`
	Overloaded      bool                 `json:"-"`
}

var latencyBounds = [...]float64{0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

type latencyHistogram struct {
	buckets [len(latencyBounds)]uint64
	count   uint64
	sum     float64
}

func (h *latencyHistogram) observe(seconds float64) {
	h.count++
	h.sum += seconds
	for i, bound := range latencyBounds {
		if seconds <= bound {
			h.buckets[i]++
		}
	}
}

type requestMetric struct{ route, status, outcome string }
type attemptMetric struct{ route, outcome string }

// One registry per gateway. Labels come only from fixed internal categories.
type telemetry struct {
	mu           sync.Mutex
	requests     map[requestMetric]uint64
	attempts     map[attemptMetric]uint64
	tokens       [2][3]uint64
	unknown      [2]uint64
	fallbacks    uint64
	cacheResults [4]uint64
	cacheErrors  [2]uint64
	inflight     int64
	duration     latencyHistogram
	ttft         latencyHistogram
	stages       [len(stageNames)]latencyHistogram
	queue        chan requestEvent
	done         chan struct{}
	closeOnce    sync.Once
	dropped      atomic.Uint64
	logErrors    atomic.Uint64
	sequence     atomic.Uint64
	idPrefix     string
	breakers     map[string]*circuitBreaker
	usage        *usagePipeline
	admission    *admissionGate
}

func newTelemetry(writer io.Writer) *telemetry {
	t := &telemetry{requests: make(map[requestMetric]uint64), attempts: make(map[attemptMetric]uint64), idPrefix: rand.Text(), breakers: make(map[string]*circuitBreaker)}
	t.admission, _ = newAdmissionGate("")
	if writer != nil {
		t.queue, t.done = make(chan requestEvent, 256), make(chan struct{})
		go func() {
			defer close(t.done)
			encoder := json.NewEncoder(writer)
			for event := range t.queue {
				if encoder.Encode(event) != nil {
					t.logErrors.Add(1)
				}
			}
		}()
	}
	return t
}

// Drain only after all handlers have stopped; production shutdown is a later step.
func (t *telemetry) close() {
	t.closeOnce.Do(func() {
		if t.queue != nil {
			close(t.queue)
			<-t.done
		}
	})
}

type observedWriter struct {
	http.ResponseWriter
	status int
}

func (w *observedWriter) WriteHeader(status int) {
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *observedWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(data)
}

func (w *observedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type flushingObservedWriter struct{ *observedWriter }

func (w *flushingObservedWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	w.ResponseWriter.(http.Flusher).Flush()
}

func (t *telemetry) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace := &requestTrace{metrics: t, started: time.Now(), route: "none", cache: "BYPASS", attempts: make([]attemptObservation, 0, 2)}
		id := t.idPrefix + "-" + strconv.FormatUint(t.sequence.Add(1), 16)
		w.Header().Set("X-Request-ID", id)
		captured := &observedWriter{ResponseWriter: w}
		var wrapped http.ResponseWriter = captured
		if _, ok := w.(http.Flusher); ok {
			wrapped = &flushingObservedWriter{captured}
		}
		t.mu.Lock()
		t.inflight++
		t.mu.Unlock()
		defer func() {
			panicked := recover()
			status := captured.status
			if status == 0 {
				status = 200
				if r.Context().Err() != nil {
					status = 0
				}
			}
			outcome := "success"
			switch {
			case r.Context().Err() != nil:
				outcome = "canceled"
			case panicked != nil:
				outcome = "interrupted"
				if captured.status == 0 {
					status = 500
				}
			case trace.streamStarted && !trace.streamComplete:
				outcome = "interrupted"
			case status >= 400:
				outcome = "error"
			}
			event := requestEvent{TenantID: trace.tenantID, Time: time.Now().UTC(), Event: "chat_request", RequestID: id, Route: trace.route, Status: status, Outcome: outcome, Stream: trace.stream, Duration: float64(time.Since(trace.started)) / float64(time.Millisecond), TTFT: trace.ttft, Attempts: trace.attempts, Cache: trace.cache, CacheWriteError: trace.cacheWriteError}
			event.Overloaded = trace.overloaded
			t.recordWithRelease(event, trace.fallback, trace.release)
			if panicked != nil {
				panic(panicked)
			}
		}()
		next.ServeHTTP(wrapped, r.WithContext(context.WithValue(r.Context(), traceKey{}, trace)))
	})
}

func (t *telemetry) record(event requestEvent, fallback bool) {
	t.recordWithRelease(event, fallback, nil)
}

func (t *telemetry) recordWithRelease(event requestEvent, fallback bool, release func()) {
	status := "other"
	if event.Status >= 100 && event.Status < 600 {
		status = strconv.Itoa(event.Status/100) + "xx"
	}
	t.mu.Lock()
	t.inflight--
	t.requests[requestMetric{event.Route, status, event.Outcome}]++
	cacheIndex := 2
	switch event.Cache {
	case "HIT":
		cacheIndex = 0
	case "MISS":
		cacheIndex = 1
	case "ERROR":
		cacheIndex = 3
		t.cacheErrors[0]++
	}
	t.cacheResults[cacheIndex]++
	if event.CacheWriteError {
		t.cacheErrors[1]++
	}
	t.duration.observe(event.Duration / 1000)
	if event.TTFT != nil {
		t.ttft.observe(*event.TTFT / 1000)
	}
	if fallback {
		t.fallbacks++
	}
	for _, attempt := range event.Attempts {
		t.attempts[attemptMetric{attempt.Route, attempt.Outcome}]++
		index := 0
		if attempt.Route == "fallback" {
			index = 1
		}
		if attempt.Outcome != "skipped" {
			if attempt.Usage == nil {
				t.unknown[index]++
			} else {
				t.tokens[index][0] += uint64(*attempt.Usage.Prompt)
				t.tokens[index][1] += uint64(*attempt.Usage.Completion)
				t.tokens[index][2] += uint64(*attempt.Usage.Total)
			}
		}
	}
	t.mu.Unlock()
	if t.queue != nil {
		select {
		case t.queue <- event:
		default:
			t.dropped.Add(1)
		}
	}
	if t.usage != nil && event.TenantID != "" && !event.Overloaded {
		started := time.Now()
		t.usage.publish(event, release)
		t.observeStage(2, started)
	} else if release != nil {
		release()
	}
}

func writeHistogram(out *strings.Builder, name, help string, h latencyHistogram) {
	fmt.Fprintf(out, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for i, bound := range latencyBounds {
		fmt.Fprintf(out, "%s_bucket{le=%q} %d\n", name, strconv.FormatFloat(bound, 'g', -1, 64), h.buckets[i])
	}
	fmt.Fprintf(out, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", name, h.count, name, h.sum, name, h.count)
}

func (t *telemetry) serveMetrics(w http.ResponseWriter, r *http.Request) {
	// Seats include completed requests still waiting for usage confirmation.
	var out strings.Builder
	t.mu.Lock()
	fmt.Fprintf(&out, "# HELP janus_admission_active Held concurrency seats, including pending handoffs.\n# TYPE janus_admission_active gauge\njanus_admission_active %d\n# TYPE janus_admission_limit gauge\njanus_admission_limit %d\n# TYPE janus_overload_rejections_total counter\njanus_overload_rejections_total %d\n", t.admission.active.Load(), cap(t.admission.slots), t.admission.rejected.Load())
	fmt.Fprintln(&out, "# HELP janus_chat_requests_total Finished chat requests, including rejected calls.\n# TYPE janus_chat_requests_total counter")
	for key, count := range t.requests {
		fmt.Fprintf(&out, "janus_chat_requests_total{route=%q,status_class=%q,outcome=%q} %d\n", key.route, key.status, key.outcome, count)
	}
	fmt.Fprintln(&out, "# HELP janus_provider_attempts_total Provider route attempts, including skipped circuits.\n# TYPE janus_provider_attempts_total counter")
	for key, count := range t.attempts {
		fmt.Fprintf(&out, "janus_provider_attempts_total{route=%q,outcome=%q} %d\n", key.route, key.outcome, count)
	}
	fmt.Fprintln(&out, "# HELP janus_cache_requests_total Chat requests by cache decision.\n# TYPE janus_cache_requests_total counter")
	for index, result := range []string{"hit", "miss", "bypass", "error"} {
		fmt.Fprintf(&out, "janus_cache_requests_total{result=%q} %d\n", result, t.cacheResults[index])
	}
	fmt.Fprintln(&out, "# HELP janus_cache_errors_total Cache read or write failures.\n# TYPE janus_cache_errors_total counter")
	for index, operation := range []string{"read", "write"} {
		fmt.Fprintf(&out, "janus_cache_errors_total{operation=%q} %d\n", operation, t.cacheErrors[index])
	}
	fmt.Fprintln(&out, "# HELP janus_reported_tokens_total Valid provider-reported tokens; excludes unknown usage.\n# TYPE janus_reported_tokens_total counter")
	// Keep each metric family contiguous for Prometheus text parsing.
	for index, route := range []string{"primary", "fallback"} {
		for kind, name := range []string{"prompt", "completion", "total"} {
			fmt.Fprintf(&out, "janus_reported_tokens_total{route=%q,kind=%q} %d\n", route, name, t.tokens[index][kind])
		}
	}
	fmt.Fprintln(&out, "# HELP janus_usage_unknown_total Contacted provider attempts without valid final usage.\n# TYPE janus_usage_unknown_total counter")
	for index, route := range []string{"primary", "fallback"} {
		fmt.Fprintf(&out, "janus_usage_unknown_total{route=%q} %d\n", route, t.unknown[index])
	}
	fmt.Fprintf(&out, "# HELP janus_fallback_selections_total Requests selecting the fallback route.\n# TYPE janus_fallback_selections_total counter\njanus_fallback_selections_total %d\n# HELP janus_chat_inflight Current chat handlers.\n# TYPE janus_chat_inflight gauge\njanus_chat_inflight %d\n", t.fallbacks, t.inflight)
	writeHistogram(&out, "janus_request_duration_seconds", "Handler duration including quota settlement and client writes.", t.duration)
	writeHistogram(&out, "janus_ttft_seconds", "Time from handler entry to flushing the first recognized text delta.", t.ttft)
	for index, stage := range stageNames {
		writeHistogram(&out, "janus_"+stage+"_duration_seconds", "Operation duration including connection acquisition and failures.", t.stages[index])
	}
	t.mu.Unlock()
	fmt.Fprintf(&out, "# HELP janus_log_dropped_total Request logs dropped when the queue is full.\n# TYPE janus_log_dropped_total counter\njanus_log_dropped_total %d\n# HELP janus_log_write_errors_total Failed JSON log writes.\n# TYPE janus_log_write_errors_total counter\njanus_log_write_errors_total %d\n", t.dropped.Load(), t.logErrors.Load())
	if p := t.usage; p != nil {
		fmt.Fprintf(&out, "# HELP janus_usage_pending_handoffs Retained jobs awaiting Redis confirmation.\n# TYPE janus_usage_pending_handoffs gauge\njanus_usage_pending_handoffs %d\n# HELP janus_usage_enqueue_retries_total Failures triggering another handoff attempt.\n# TYPE janus_usage_enqueue_retries_total counter\njanus_usage_enqueue_retries_total %d\n", p.pending.Load(), p.retriesTotal.Load())
		fmt.Fprintf(&out, "# HELP janus_usage_enqueued_total Confirmed usage queue handoffs.\n# TYPE janus_usage_enqueued_total counter\njanus_usage_enqueued_total %d\n", p.queued.Load())
		fmt.Fprintf(&out, "# HELP janus_usage_enqueue_errors_total Terminal invalid events or handoffs unconfirmed at shutdown.\n# TYPE janus_usage_enqueue_errors_total counter\njanus_usage_enqueue_errors_total %d\n", p.enqueueErr.Load())
		fmt.Fprintf(&out, "# HELP janus_usage_persisted_total Saved and acknowledged deliveries, including deduplicated retries.\n# TYPE janus_usage_persisted_total counter\njanus_usage_persisted_total %d\n", p.persisted.Load())
		fmt.Fprintf(&out, "# HELP janus_usage_worker_errors_total Worker read, save or acknowledgement failures.\n# TYPE janus_usage_worker_errors_total counter\njanus_usage_worker_errors_total %d\n", p.workerErr.Load())
		fmt.Fprintf(&out, "# HELP janus_usage_invalid_events_total Invalid entries encountered, including repeat encounters.\n# TYPE janus_usage_invalid_events_total counter\njanus_usage_invalid_events_total %d\n", p.invalid.Load())
	}
	fmt.Fprintln(&out, "# HELP janus_circuit_state Provider circuit state: closed=0, open=1, half-open=2.\n# TYPE janus_circuit_state gauge")
	for route, breaker := range t.breakers {
		breaker.mu.Lock()
		state := breaker.state
		breaker.mu.Unlock()
		fmt.Fprintf(&out, "janus_circuit_state{route=%q} %d\n", route, state)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(out.String()))
}
