package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const cacheAnswer = `{"id":"cached-id","model":"demo-model","choices":[{"message":{"role":"assistant","content":"Hello back"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}`

func cacheFixture(t *testing.T) (*Provider, *RateLimiter, *responseCache, *telemetry, http.Handler, *int) {
	t.Helper()
	limiter := testQuota(t, "60", 60000)
	calls := new(int)
	p, _ := newProvider("http://localhost", "provider-key")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		*calls++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	cache, err := newResponseCache(limiter.client, "300", limiter.key, p)
	if err != nil {
		t.Fatal(err)
	}
	p.cache = cache
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := limiter.client.Scan(context.Background(), cursor, cache.scope+":*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				limiter.client.Del(context.Background(), keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
	obs := newTelemetry(nil)
	return p, limiter, cache, obs, newMuxWithTelemetry(p, "client-key", limiter, obs), calls
}

func ioBody(text string) io.ReadCloser { return io.NopCloser(strings.NewReader(text)) }

func cacheCall(mux http.Handler, body string, optIn bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer client-key")
	if optIn {
		r.Header.Set("X-Janus-Cache", "true")
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestRedisCacheHitAndExpiry(t *testing.T) {
	_, limiter, cache, obs, mux, calls := cacheFixture(t)
	first := cacheCall(mux, validChatBody, true)
	second := cacheCall(mux, "  \n"+validChatBody+"\n", true)
	if first.Code != 200 || first.Header().Get("X-Janus-Cache") != "MISS" || second.Header().Get("X-Janus-Cache") != "HIT" || first.Body.String() != second.Body.String() || *calls != 1 {
		t.Fatal("miss/hit did not preserve response or avoid provider")
	}
	if second.Header().Get("X-TokenLimit-Reserved") != "0" || second.Header().Get("X-Janus-Route") != "cache" {
		t.Fatal("cache hit charged TPM or claimed provider route")
	}
	total, _ := limiter.client.Get(context.Background(), limiter.quotaKeys()[3]).Int64()
	if total != 50 || obs.tokens[0][2] != 50 || obs.cacheResults[0] != 1 || obs.cacheResults[1] != 1 {
		t.Fatal("cached historical usage was charged again")
	}
	if first.Header().Get("X-RateLimit-Remaining") != "59" || second.Header().Get("X-RateLimit-Remaining") != "58" {
		t.Fatal("hit bypassed RPM")
	}
	input, _ := decodeChatRequest(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)), 1024)
	key := cache.key(input)
	if ttl, err := cache.client.TTL(context.Background(), key).Result(); err != nil || ttl <= 0 || ttl > 300*time.Second {
		t.Fatal("cache TTL missing")
	}
	cache.client.PExpire(context.Background(), key, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	third := cacheCall(mux, validChatBody, true)
	if third.Header().Get("X-Janus-Cache") != "MISS" || *calls != 2 {
		t.Fatal("expired response reused")
	}
}

func TestRedisCacheHitDespiteTPMDebt(t *testing.T) {
	_, limiter, _, _, mux, calls := cacheFixture(t)
	cacheCall(mux, validChatBody, true)
	charge, err := limiter.ReserveTokens(context.Background(), 59950)
	if err != nil || !charge.allowed {
		t.Fatal(err)
	}
	limiter.Settle(context.Background(), charge.reservation, 60100)
	hit := cacheCall(mux, validChatBody, true)
	miss := cacheCall(mux, strings.Replace(validChatBody, "Hello", "different", 1), true)
	if hit.Code != 200 || hit.Header().Get("X-Janus-Cache") != "HIT" || miss.Code != 429 || *calls != 1 {
		t.Fatal("hit checked TPM debt or miss bypassed quota")
	}
}

func TestRedisCacheIsolationAndCorruption(t *testing.T) {
	p, limiter, cache, obs, mux, calls := cacheFixture(t)
	input, _ := decodeChatRequest(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)), 1024)
	original := cache.key(input)
	for _, body := range []string{strings.Replace(validChatBody, "Hello", "other", 1), strings.Replace(validChatBody, "demo-model", "other-model", 1), strings.Replace(validChatBody, "user", "system", 1), strings.TrimSuffix(validChatBody, "}") + `,"max_completion_tokens":32}`} {
		changed, ok := decodeChatRequest(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(body)), 1024)
		if !ok {
			t.Fatal("invalid key-isolation test request")
		}
		if cache.key(changed) == original {
			t.Fatal("answer-affecting request fields missing from key")
		}
	}
	other, _ := newResponseCache(cache.client, "300", "other-client", p)
	if other.key(input) == original {
		t.Fatal("client caches not isolated")
	}
	p.apiKey = "rotated-key"
	rotated, _ := newResponseCache(cache.client, "300", limiter.key, p)
	if rotated.key(input) == original {
		t.Fatal("credential changes did not isolate cache")
	}
	cache.client.Set(context.Background(), original, strings.Repeat("x", maxCacheBytes+10), time.Minute)
	w := cacheCall(mux, validChatBody, true)
	if w.Code != 200 || w.Header().Get("X-Janus-Cache") != "ERROR" || *calls != 1 || obs.cacheErrors[0] != 1 {
		t.Fatal("corrupt cache blocked fresh generation")
	}
}

func TestCacheAnswerEligibility(t *testing.T) {
	if !cacheableAnswer([]byte(cacheAnswer)) {
		t.Fatal("complete answer rejected")
	}
	for _, data := range []string{`{}`, `{"error":"bad"}`, strings.Replace(cacheAnswer, `"stop"`, `"length"`, 1), strings.Replace(cacheAnswer, `"Hello back"`, `""`, 1), strings.Replace(cacheAnswer, `"role":"assistant"`, `"role":"user"`, 1), strings.Replace(cacheAnswer, `"content":"Hello back"`, `"content":"Hello back","refusal":"no"`, 1), strings.Replace(cacheAnswer, `"content":"Hello back"`, `"content":"Hello back","tool_calls":[{}]`, 1), strings.Repeat("x", maxCacheBytes+1)} {
		if cacheableAnswer([]byte(data)) {
			t.Fatal("ineligible answer cached")
		}
	}
	for _, ttl := range []string{"-1", "invalid", "86401"} {
		if _, err := newResponseCache(nil, ttl, "key", &Provider{}); err == nil {
			t.Fatal("invalid TTL accepted")
		}
	}
	if disabled, err := newResponseCache(nil, "0", "key", &Provider{}); err != nil || disabled != nil {
		t.Fatal("cache disable failed")
	}
}

func TestCacheFailureAndStreamingBypass(t *testing.T) {
	p, _ := newProvider("http://localhost", "key")
	// A failed cache need not mean the independent quota implementation failed.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Millisecond})
	defer client.Close()
	cache, _ := newResponseCache(client, "300", "key", p)
	p.cache = cache
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	obs := newTelemetry(nil)
	mux := newMuxWithTelemetry(p, "client-key", &stubLimiter{decision: rateDecision{allowed: true}}, obs)
	w := cacheCall(mux, validChatBody, true)
	if w.Code != 200 || w.Header().Get("X-Janus-Cache") != "ERROR" || obs.cacheErrors[0] != 1 || obs.cacheErrors[1] != 1 {
		t.Fatal("cache errors changed successful response")
	}
	input := ChatRequest{Stream: true}
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("X-Janus-Cache", "true")
	if wantsCache(r, input, cache) {
		t.Fatal("stream eligible for cache")
	}
}

func TestRedisCacheDoesNotStoreFallback(t *testing.T) {
	p, _, cache, _, mux, calls := cacheFixture(t)
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		*calls++
		return &http.Response{StatusCode: 503, Header: make(http.Header), Body: ioBody(`{}`)}, nil
	})
	p.configureFallback("http://localhost", "fallback", "demo-model")
	p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	first, second := cacheCall(mux, validChatBody, true), cacheCall(mux, validChatBody, true)
	if first.Code != 200 || second.Header().Get("X-Janus-Cache") != "MISS" || *calls != 2 {
		t.Fatal("fallback response reused")
	}
	input, _ := decodeChatRequest(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)), 1024)
	if exists, _ := cache.client.Exists(context.Background(), cache.key(input)).Result(); exists != 0 {
		t.Fatal("fallback answer stored")
	}
}
