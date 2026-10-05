package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Core mode measures HTTP/auth/validation/routing/telemetry/SSE with quota I/O
// replaced by a no-op. It is explicitly NOT a measurement of Redis overhead.
type perfCoreLimiter struct{}

func (perfCoreLimiter) Reserve(context.Context, int64) (rateDecision, error) {
	return rateDecision{allowed: true, remaining: 1000000, tokenRemaining: 100000000}, nil
}
func (l perfCoreLimiter) ReserveTokens(ctx context.Context, n int64) (rateDecision, error) {
	return l.Reserve(ctx, n)
}
func (perfCoreLimiter) Settle(context.Context, string, int64) error { return nil }
func (perfCoreLimiter) Limit() int                                  { return 1000000 }
func (perfCoreLimiter) TokenLimit() int                             { return 100000000 }
func (perfCoreLimiter) OutputLimit() int                            { return 64 }

type perfSample struct {
	duration time.Duration
	ttft     time.Duration
	err      error
}

type perfSummary struct {
	Scenario                        string   `json:"scenario"`
	Concurrency                     int      `json:"concurrency"`
	Requests                        int      `json:"requests"`
	Errors                          int      `json:"errors"`
	P50MS                           float64  `json:"p50_ms"`
	P99MS                           float64  `json:"p99_ms"`
	TTFTP50MS                       *float64 `json:"ttft_p50_ms,omitempty"`
	TTFTP99MS                       *float64 `json:"ttft_p99_ms,omitempty"`
	RequestsPerSecond               float64  `json:"requests_per_second"`
	HarnessAllocatedBytesPerRequest uint64   `json:"harness_allocated_bytes_per_request"`
	QueueAfterLoad                  int64    `json:"queue_after_load"`
	QueueAfterDrain                 int64    `json:"queue_after_drain"`
	EnqueueErrors                   uint64   `json:"enqueue_errors"`
	WorkerErrors                    uint64   `json:"worker_errors"`
}

func perfPercentile(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := max(0, min(len(ordered)-1, int(math.Ceil(percentile*float64(len(ordered))))-1))
	return float64(ordered[index]) / float64(time.Millisecond)
}

const perfJSON = `{"model":"fake-model","choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`

func perfProvider(w http.ResponseWriter, r *http.Request) {
	var input ChatRequest
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		w.WriteHeader(400)
		return
	}
	if input.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if !usagePause(r.Context(), 3*time.Millisecond) {
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if !usagePause(r.Context(), 7*time.Millisecond) {
			return
		}
		io.WriteString(w, "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\ndata: [DONE]\n\n")
		w.(http.Flusher).Flush()
		return
	}
	if !usagePause(r.Context(), 10*time.Millisecond) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, perfJSON)
}

func perfRequest(client *http.Client, url string, stream, cache bool) (sample perfSample) {
	body := fmt.Sprintf(`{"model":"fake-model","messages":[{"role":"user","content":"hello"}],"stream":%t,"max_completion_tokens":64}`, stream)
	r, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		sample.err = err
		return
	}
	r.Header.Set("Authorization", "Bearer perf-client-key")
	r.Header.Set("Content-Type", "application/json")
	if cache {
		r.Header.Set("X-Janus-Cache", "true")
	}
	started := time.Now()
	defer func() { sample.duration = time.Since(started) }()
	response, err := client.Do(r)
	if err != nil {
		sample.err = err
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		io.Copy(io.Discard, response.Body)
		sample.err = fmt.Errorf("HTTP %d", response.StatusCode)
		return
	}
	if cache && response.Header.Get("X-Janus-Cache") != "HIT" {
		sample.err = fmt.Errorf("expected cache hit")
		return
	}
	if !stream {
		data, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		if err != nil || string(data) != perfJSON {
			sample.err = fmt.Errorf("invalid completion")
		}
		return
	}
	// First role/header arrival is NOT TTFT. Measure a complete visible text event.
	scanner := bufio.NewScanner(response.Body)
	done := false
	var dataLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			continue
		}
		if line != "" || len(dataLines) == 0 {
			continue
		}
		data := strings.Join(dataLines, "\n")
		dataLines = nil
		if data == "[DONE]" {
			done = true
			continue
		}
		{
			var event struct {
				Choices []struct {
					Delta struct {
						Content string `json:"content"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if json.Unmarshal([]byte(data), &event) != nil {
				sample.err = fmt.Errorf("invalid SSE JSON")
				return
			}
			for _, choice := range event.Choices {
				if choice.Delta.Content != "" && sample.ttft == 0 {
					sample.ttft = time.Since(started)
				}
			}
		}
	}
	if scanner.Err() != nil || !done || sample.ttft == 0 || len(dataLines) != 0 {
		sample.err = fmt.Errorf("incomplete SSE response")
	}
	return
}

func perfLoad(client *http.Client, url string, stream, cache bool, count, concurrency int) ([]perfSample, time.Duration) {
	samples := make([]perfSample, count)
	var next atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				samples[index] = perfRequest(client, url, stream, cache)
			}
		}()
	}
	wg.Wait()
	return samples, time.Since(started)
}

func TestGatewayPerformance(t *testing.T) {
	if os.Getenv("JANUS_PERF") != "1" {
		t.Skip("run benchmark.ps1 for an explicit local performance run")
	}
	mode := os.Getenv("JANUS_PERF_MODE")
	if mode != "core" && mode != "full" {
		t.Fatal("performance mode must be core or full")
	}
	count, err := strconv.Atoi(os.Getenv("JANUS_PERF_REQUESTS"))
	if err != nil || count < 100 || count > 5000 {
		t.Fatal("request count must be between 100 and 5000")
	}
	var levels []int
	for _, item := range strings.Split(os.Getenv("JANUS_PERF_CONCURRENCY"), ",") {
		level, err := strconv.Atoi(item)
		if err != nil || level < 1 || level > 128 {
			t.Fatal("concurrency must be between 1 and 128")
		}
		levels = append(levels, level)
	}
	providerServer := httptest.NewServer(http.HandlerFunc(perfProvider))
	defer providerServer.Close()
	provider, err := newProvider(providerServer.URL, "fake-provider-key")
	if err != nil {
		t.Fatal(err)
	}
	obs := newTelemetry(io.Discard)
	var limiter requestLimiter = perfCoreLimiter{}
	var quota *RateLimiter
	var pipeline *usagePipeline
	var store *postgresUsageStore
	tenantID := "perf-" + rand.Text()
	if mode == "full" {
		quota, err = newRateLimiter(os.Getenv("JANUS_PERF_REDIS_URL"), "1000000", tenantID)
		if err != nil {
			t.Fatal(err)
		}
		quota.tpm, quota.outputLimit = 100000000, 64
		defer quota.client.Close()
		defer quota.client.Del(context.Background(), quota.quotaKeys()...)
		if err := quota.client.Ping(context.Background()).Err(); err != nil {
			t.Fatal("quota Redis unavailable; start services or use -Mode core")
		}
		limiter = quota
		provider.cache, err = newResponseCache(quota.client, "300", tenantID, provider)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			var cursor uint64
			for {
				keys, next, err := quota.client.Scan(context.Background(), cursor, provider.cache.scope+":*", 100).Result()
				if err != nil {
					break
				}
				if len(keys) > 0 {
					quota.client.Del(context.Background(), keys...)
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err = newPostgresUsageStore(ctx, os.Getenv("JANUS_PERF_DATABASE_URL"))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer store.pool.Close()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := store.pool.Exec(ctx, `DELETE FROM janus_usage_attempts WHERE request_id IN (SELECT request_id FROM janus_usage_requests WHERE tenant_id=$1)`, tenantID)
			if err == nil {
				_, err = store.pool.Exec(ctx, `DELETE FROM janus_usage_requests WHERE tenant_id=$1`, tenantID)
			}
			if err != nil {
				t.Error("benchmark database cleanup failed")
			}
		}()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		pipeline, err = newUsagePipelineWithStream(ctx, os.Getenv("JANUS_PERF_USAGE_REDIS_URL"), store, "janus:usage:perf:"+rand.Text())
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			pipeline.cancel()
			<-pipeline.done
			pipeline.client.Del(context.Background(), pipeline.stream)
			pipeline.client.Close()
		}()
		pipeline.start()
	}
	// Set trusted tenant context through the real registry middleware in both modes.
	perfTenant := &tenant{id: tenantID, enabled: true, handler: limitRequests(limiter, http.HandlerFunc(provider.chatHandler), provider.cache)}
	registry := &tenantRegistry{byKey: map[[32]byte]*tenant{}}
	// Obtain the fingerprint through the same parser used for client authentication.
	authRequest := httptest.NewRequest("POST", "/", nil)
	authRequest.Header.Set("Authorization", "Bearer perf-client-key")
	fingerprint, _ := bearerFingerprint(authRequest)
	registry.byKey[fingerprint] = perfTenant
	gateway := httptest.NewServer(newTenantMux(provider, "perf-admin-key", registry, obs))
	defer obs.close()
	defer gateway.Close()
	var fullGateway *httptest.Server
	if pipeline != nil {
		fullObs := newTelemetry(io.Discard)
		fullObs.usage = pipeline
		fullGateway = httptest.NewServer(newTenantMux(provider, "perf-admin-key", registry, fullObs))
		defer fullObs.close()
		defer fullGateway.Close()
	}
	type scenario struct {
		name, url            string
		stream, cache, usage bool
	}
	scenarios := []scenario{{name: "direct-json", url: providerServer.URL}, {name: "janus-core-json", url: gateway.URL + "/v1/chat/completions"}, {name: "direct-sse", url: providerServer.URL, stream: true}, {name: "janus-core-sse", url: gateway.URL + "/v1/chat/completions", stream: true}}
	if mode == "full" {
		scenarios = []scenario{{name: "direct-json", url: providerServer.URL}, {name: "janus-redis-json", url: gateway.URL + "/v1/chat/completions"}, {name: "janus-full-json", url: fullGateway.URL + "/v1/chat/completions", usage: true}, {name: "janus-cache-hit", url: fullGateway.URL + "/v1/chat/completions", cache: true, usage: true}, {name: "direct-sse", url: providerServer.URL, stream: true}, {name: "janus-redis-sse", url: gateway.URL + "/v1/chat/completions", stream: true}, {name: "janus-full-sse", url: fullGateway.URL + "/v1/chat/completions", stream: true, usage: true}}
	}
	var summaries []perfSummary
	var csvRows [][]string
	csvRows = append(csvRows, []string{"scenario", "concurrency", "sample", "duration_ms", "ttft_ms", "error"})
	for _, level := range levels {
		for _, scenario := range scenarios {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.MaxIdleConnsPerHost = 2 // Same idle-pool default as the upstream client.
			client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
			if scenario.cache { // Prime without requiring the first request to be a hit.
				r := httptest.NewRequest("POST", "/", strings.NewReader(`{"model":"fake-model","messages":[{"role":"user","content":"hello"}],"stream":false,"max_completion_tokens":64}`))
				input, ok := decodeChatRequest(httptest.NewRecorder(), r, 64)
				if !ok || provider.cache.put(context.Background(), provider.cache.key(input), []byte(perfJSON)) != nil {
					t.Fatal("cannot prime benchmark cache")
				}
			}
			warm, _ := perfLoad(client, scenario.url, scenario.stream, scenario.cache, max(20, level), level)
			for _, sample := range warm {
				if sample.err != nil {
					t.Fatal("warm-up failed: ", sample.err)
				}
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			samples, elapsed := perfLoad(client, scenario.url, scenario.stream, scenario.cache, count, level)
			runtime.ReadMemStats(&after)
			client.CloseIdleConnections()
			result := perfSummary{Scenario: scenario.name, Concurrency: level, Requests: count, HarnessAllocatedBytesPerRequest: (after.TotalAlloc - before.TotalAlloc) / uint64(count)}
			var durations, ttfts []time.Duration
			for index, sample := range samples {
				errorText := ""
				if sample.err != nil {
					result.Errors++
					errorText = sample.err.Error()
				} else {
					durations = append(durations, sample.duration)
					if sample.ttft > 0 {
						ttfts = append(ttfts, sample.ttft)
					}
				}
				csvRows = append(csvRows, []string{scenario.name, strconv.Itoa(level), strconv.Itoa(index), fmt.Sprintf("%.6f", float64(sample.duration)/float64(time.Millisecond)), fmt.Sprintf("%.6f", float64(sample.ttft)/float64(time.Millisecond)), errorText})
			}
			result.P50MS, result.P99MS = perfPercentile(durations, .50), perfPercentile(durations, .99)
			result.RequestsPerSecond = float64(count-result.Errors) / elapsed.Seconds()
			if len(ttfts) > 0 {
				p50, p99 := perfPercentile(ttfts, .50), perfPercentile(ttfts, .99)
				result.TTFTP50MS, result.TTFTP99MS = &p50, &p99
			}
			if scenario.usage {
				result.QueueAfterLoad, _ = pipeline.client.XLen(context.Background(), pipeline.stream).Result()
				deadline := time.Now().Add(30 * time.Second)
				for {
					length, err := pipeline.client.XLen(context.Background(), pipeline.stream).Result()
					if err != nil {
						t.Fatal("cannot inspect benchmark queue")
					}
					result.QueueAfterDrain = length
					if length == 0 || time.Now().After(deadline) {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				result.EnqueueErrors, result.WorkerErrors = pipeline.enqueueErr.Load(), pipeline.workerErr.Load()
				if result.QueueAfterDrain != 0 || result.EnqueueErrors != 0 || result.WorkerErrors != 0 {
					t.Error("usage queue failed or did not drain")
				}
			}
			if result.Errors > 0 {
				t.Errorf("%s: %d request failures", scenario.name, result.Errors)
			}
			summaries = append(summaries, result)
			t.Logf("%s c=%d: p50 %.2f ms, p99 %.2f ms, errors %d", scenario.name, level, result.P50MS, result.P99MS, result.Errors)
		}
	}
	writePerfReport(t, mode, summaries, csvRows)
}

func writePerfReport(t *testing.T, mode string, results []perfSummary, rows [][]string) {
	t.Helper()
	dir := filepath.Join(".cache", "perf")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	metadata := struct {
		Mode       string        `json:"mode"`
		MeasuredAt string        `json:"measured_at"`
		Go         string        `json:"go"`
		OS         string        `json:"os"`
		GOMAXPROCS int           `json:"gomaxprocs"`
		Profiled   bool          `json:"profiled"`
		Results    []perfSummary `json:"results"`
	}{mode, time.Now().UTC().Format(time.RFC3339), runtime.Version(), runtime.GOOS, runtime.GOMAXPROCS(0), os.Getenv("JANUS_PERF_PROFILE") == "1", results}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "latest-"+mode)
	if metadata.Profiled {
		base += "-profiled"
	}
	if err := os.WriteFile(base+".json", data, 0644); err != nil {
		t.Fatal(err)
	}
	var csvData bytes.Buffer
	writer := csv.NewWriter(&csvData)
	writer.WriteAll(rows)
	if writer.Error() != nil {
		t.Fatal(writer.Error())
	}
	if err := os.WriteFile(base+".csv", csvData.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	var markdown strings.Builder
	fmt.Fprintf(&markdown, "# Local Janus performance (%s mode)\n\nMeasured %s, %s, %s, GOMAXPROCS=%d.\n\n", mode, metadata.MeasuredAt, metadata.Go, metadata.OS, metadata.GOMAXPROCS)
	if metadata.Profiled {
		markdown.WriteString("**Profiling enabled: timing includes profiler overhead.**\n\n")
	}
	markdown.WriteString("Fake provider: JSON waits 10 ms; SSE emits a role, waits 3 ms for text, then 7 ms for usage/DONE. Actual timers depend on OS scheduling. Closed-loop clients, 20 or concurrency warm-ups per scenario, successful samples only; all failures reported. Cache hits bypass generation.\n\n")
	if mode == "core" {
		markdown.WriteString("**Core mode excludes real Redis, caching and durable usage. These results do not validate full production overhead.**\n\n")
	}
	markdown.WriteString("| Scenario | Concurrency | Requests | Errors | p50 ms | p99 ms | TTFT p50 ms | TTFT p99 ms | Requests/s |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, result := range results {
		tt50, tt99 := "-", "-"
		if result.TTFTP50MS != nil {
			tt50, tt99 = fmt.Sprintf("%.2f", *result.TTFTP50MS), fmt.Sprintf("%.2f", *result.TTFTP99MS)
		}
		fmt.Fprintf(&markdown, "| %s | %d | %d | %d | %.2f | %.2f | %s | %s | %.1f |\n", result.Scenario, result.Concurrency, result.Requests, result.Errors, result.P50MS, result.P99MS, tt50, tt99, result.RequestsPerSecond)
	}
	markdown.WriteString("\nCompare direct and Janus distributions at the same concurrency. Differences between p99s are not the p99 of per-request overhead. Repeat runs; a few hundred samples give a noisy tail. Harness allocation figures in JSON include client, fake provider, gateway and worker in one process, not isolated Janus RSS/CPU. Optional CPU/heap profiles add profiling overhead. Full mode uses its own Redis stream, quota/cache namespace and temporary tenant rows, cleaned after the run.\n")
	if err := os.WriteFile(base+".md", []byte(markdown.String()), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("reports: %s.{md,json,csv}", base)
}

func TestPerfPercentileNearestRank(t *testing.T) {
	values := []time.Duration{100 * time.Millisecond, time.Millisecond, 4 * time.Millisecond, 2 * time.Millisecond}
	if perfPercentile(values, .5) != 2 || perfPercentile(values, .99) != 100 || values[0] != 100*time.Millisecond {
		t.Fatal("percentile rank or input preservation incorrect")
	}
}

func TestPerfRequestChecksStreamCompletion(t *testing.T) {
	for _, body := range []string{
		"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n",
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, body)
		}))
		result := perfRequest(server.Client(), server.URL, true, false)
		server.Close()
		if result.err == nil {
			t.Fatal("role-only or incomplete SSE became a successful benchmark sample")
		}
	}
}
