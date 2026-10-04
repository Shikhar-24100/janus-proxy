package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubLimiter struct {
	decision      rateDecision
	err           error
	calls         int
	reserved      int64
	settled       []int64
	tokenCalls    int
	tokenDecision *rateDecision
	tokenError    error
}

func (s *stubLimiter) Limit() int { return 60 }
func (s *stubLimiter) Reserve(_ context.Context, amount int64) (rateDecision, error) {
	s.calls++
	s.reserved = amount
	return s.decision, s.err
}

func (s *stubLimiter) TokenLimit() int { return 60000 }
func (s *stubLimiter) ReserveTokens(_ context.Context, _ int64) (rateDecision, error) {
	s.tokenCalls++
	if s.tokenDecision != nil {
		return *s.tokenDecision, s.tokenError
	}
	return rateDecision{allowed: true, reservation: "fallback"}, s.tokenError
}
func (s *stubLimiter) OutputLimit() int { return 1024 }
func (s *stubLimiter) Settle(_ context.Context, _ string, actual int64) error {
	s.settled = append(s.settled, actual)
	return nil
}

func TestRateLimitMiddleware(t *testing.T) {
	for _, test := range []struct {
		name     string
		decision rateDecision
		err      error
		status   int
	}{
		{"allowed", rateDecision{allowed: true, remaining: 59}, nil, 204},
		{"exhausted", rateDecision{retryAfter: 1500 * time.Millisecond}, nil, 429},
		{"Redis unavailable", rateDecision{}, errors.New("unavailable"), 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			limiter := &stubLimiter{decision: test.decision, err: test.err}
			called := false
			handler := limitRequests(limiter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(204)
			}))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody)))
			if response.Code != test.status || called != test.decision.allowed {
				t.Fatalf("status=%d, next called=%v", response.Code, called)
			}
			if test.status == 429 && response.Header().Get("Retry-After") != "2" {
				t.Fatal("Retry-After must round up to whole seconds")
			}
		})
	}
}

func TestAuthenticationRunsBeforeRateLimit(t *testing.T) {
	limiter := &stubLimiter{decision: rateDecision{allowed: true}}
	mux := newMux(nil, "test-key", limiter)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if response.Code != 401 || limiter.calls != 0 {
		t.Fatal("unauthorized request consumed quota")
	}
	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != 200 || limiter.calls != 0 {
		t.Fatal("health request consumed quota")
	}
}

func TestRateLimitConfig(t *testing.T) {
	for _, rpm := range []string{"0", "-1", "no", "1000001"} {
		if _, err := newRateLimiter("", rpm, "test-key"); err == nil {
			t.Errorf("accepted invalid RPM_LIMIT %q", rpm)
		}
	}
	if _, err := newRateLimiter("not-a-url", "60", "test-key"); err == nil {
		t.Fatal("accepted invalid Redis URL")
	}
	limiter, err := newRateLimiter("", "", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	defer limiter.client.Close()
	if limiter.rpm != 60 || strings.Contains(limiter.key, "test-key") {
		t.Fatal("wrong default limit or raw key exposed in Redis key")
	}
}

func TestRedisSharedTokenBucket(t *testing.T) {
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("set REDIS_TEST_URL to run Redis integration checks")
	}
	key := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	first, err := newRateLimiter(redisURL, "5", key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRateLimiter(redisURL, "5", key)
	if err != nil {
		t.Fatal(err)
	}
	defer first.client.Close()
	defer second.client.Close()
	t.Cleanup(func() {
		// Delete only the bucket uniquely created by this test.
		client, _ := newRateLimiter(redisURL, "5", key)
		defer client.client.Close()
		client.client.Del(context.Background(), first.key)
	})
	var allowed atomic.Int32
	var failures atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < 40; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			limiter := first
			if i%2 == 0 {
				limiter = second
			}
			decision, err := limiter.Allow(context.Background())
			if err != nil {
				failures.Add(1)
				return
			}
			if decision.allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wait.Wait()
	if failures.Load() != 0 || allowed.Load() != 5 {
		t.Fatalf("40 concurrent calls across two clients: admitted=%d errors=%d", allowed.Load(), failures.Load())
	}
	decision, err := first.Allow(context.Background())
	if err != nil || decision.allowed || decision.retryAfter <= 0 {
		t.Fatalf("exhausted bucket: %+v, %v", decision, err)
	}
	// Simulate a bucket last updated long ago, without sleeping for a minute.
	if err := first.client.HSet(context.Background(), first.key, "updated_ms", 0, "tokens", 0).Err(); err != nil {
		t.Fatal(err)
	}
	decision, err = second.Allow(context.Background())
	if err != nil || !decision.allowed || decision.remaining != 4 {
		t.Fatalf("refilled bucket: %+v, %v", decision, err)
	}
	ttl, err := first.client.PTTL(context.Background(), first.key).Result()
	if err != nil || ttl <= 0 || ttl > 2*time.Minute {
		t.Fatalf("bucket expiration = %v, %v", ttl, err)
	}
}

func TestRedisHTTPAdmission(t *testing.T) {
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("set REDIS_TEST_URL to run Redis integration checks")
	}
	key := fmt.Sprintf("http-integration-%d", time.Now().UnixNano())
	limiter, err := newRateLimiter(redisURL, "2", key)
	if err != nil {
		t.Fatal(err)
	}
	defer limiter.client.Close()
	defer limiter.client.Del(context.Background(), limiter.key)
	provider, _ := newProvider("", "")
	mux := newMux(provider, key, limiter)
	// Valid admission consumes RPM; a missing provider key refunds TPM only.
	for _, want := range []int{503, 503, 429} {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody))
		request.Header.Set("Authorization", "Bearer "+key)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("HTTP status = %d, want %d", response.Code, want)
		}
		if want == 429 && response.Header().Get("Retry-After") == "" {
			t.Fatal("429 must include Retry-After")
		}
	}
}
