package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestFallbackRouting(t *testing.T) {
	const answer = `{"model":"fallback-model","choices":[{"message":{"content":"answer"}}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`
	for _, test := range []struct {
		name     string
		status   int
		body     string
		fallback bool
	}{
		{"primary healthy", 200, answer, false},
		{"rate limited", 429, `{}`, true},
		{"server error", 503, `{}`, true},
		{"known failed usage", 500, `{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}`, true},
		{"bad request", 400, `{}`, false},
		{"invalid credentials", 401, `{}`, false},
		{"invalid response", 200, "invalid", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer primary-key" {
					t.Error("wrong primary credential")
				}
				w.Header().Set("Retry-After", "90")
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			}))
			defer primary.Close()
			var fallbackCalls atomic.Int32
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer fallback-key" {
					t.Error("wrong fallback credential")
				}
				var input ChatRequest
				json.NewDecoder(r.Body).Decode(&input)
				if input.Model != "fallback-model" || input.Messages[0].Content != "Hello" || *input.MaxCompletionTokens != 1024 {
					t.Error("fallback payload changed unexpectedly")
				}
				io.WriteString(w, answer)
			}))
			defer fallback.Close()
			p, _ := newProvider(primary.URL, "primary-key")
			p.configureFallback(fallback.URL, "fallback-key", "fallback-model")
			limiter := &stubLimiter{decision: rateDecision{allowed: true, reservation: "primary"}}
			w := httptest.NewRecorder()
			limitRequests(limiter, http.HandlerFunc(p.chatHandler)).ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
			if test.fallback {
				if w.Code != 200 || w.Body.String() != answer || fallbackCalls.Load() != 1 || w.Header().Get("X-Janus-Route") != "fallback" || w.Header().Get("Retry-After") != "" {
					t.Fatal("fallback response or route incorrect")
				}
				if limiter.calls != 1 || limiter.tokenCalls != 1 {
					t.Fatal("fallback charged another RPM or failed to reserve TPM")
				}
				if test.name == "known failed usage" {
					if len(limiter.settled) != 2 || limiter.settled[0] != 50 || limiter.settled[1] != 7 {
						t.Fatalf("both attempts not charged: %v", limiter.settled)
					}
				} else if len(limiter.settled) != 1 || limiter.settled[0] != 7 {
					t.Fatal("unknown primary usage was refunded")
				}
			} else if fallbackCalls.Load() != 0 || limiter.tokenCalls != 0 || w.Code != test.status {
				t.Fatal("non-retryable response used fallback")
			}
		})
	}
}

func TestFallbackOpenCircuitAndQuota(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		p, _ := newProvider("http://localhost", "primary")
		var calls atomic.Int32
		p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) { return nil, errors.New("network failure") })
		p.configureFallback("http://localhost", "fallback", "fallback-model")
		p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))}, nil
		})
		limiter := &stubLimiter{decision: rateDecision{allowed: true, reservation: "original"}, tokenDecision: &rateDecision{allowed: false}}
		if blocked {
			failBreaker(t, p.breaker)
		}
		w := httptest.NewRecorder()
		limitRequests(limiter, http.HandlerFunc(p.chatHandler)).ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
		if blocked {
			if w.Code != 200 || calls.Load() != 1 || limiter.tokenCalls != 0 || len(limiter.settled) != 1 || limiter.settled[0] != 7 {
				t.Fatal("open primary did not reuse unused reservation")
			}
		} else if w.Code != 429 || calls.Load() != 0 || limiter.tokenCalls != 1 || len(limiter.settled) != 0 {
			t.Fatal("fallback bypassed quota or refunded unknown primary")
		}
	}
}

func TestFallbackExhaustedAndCanceled(t *testing.T) {
	p, _ := newProvider("http://localhost", "primary")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) { return nil, errors.New("network failure") })
	p.configureFallback("http://localhost", "fallback", "fallback-model")
	var calls atomic.Int32
	p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"15"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"fallback busy"}}`))}, nil
	})
	w := httptest.NewRecorder()
	p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
	if w.Code != 429 || w.Header().Get("Retry-After") != "15" || !strings.Contains(w.Body.String(), "fallback busy") || calls.Load() != 1 {
		t.Fatal("last provider error not preserved")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.chatHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)).WithContext(ctx))
	if calls.Load() != 1 {
		t.Fatal("cancellation triggered paid fallback")
	}
}

func TestFallbackStreamingBoundary(t *testing.T) {
	for _, primaryStarted := range []bool{false, true} {
		p, _ := newProvider("http://localhost", "primary")
		p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
			contentType := "application/json"
			var body io.ReadCloser = io.NopCloser(strings.NewReader(`{}`))
			if primaryStarted {
				contentType = "text/event-stream"
				body = breakerReadFailure{}
			}
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: body}, nil
		})
		p.configureFallback("http://localhost", "fallback", "fallback-model")
		calls := 0
		const stream = "data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7}}\n\ndata: [DONE]\n\n"
		p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream))}, nil
		})
		w := httptest.NewRecorder()
		func() {
			defer func() {
				if recovered := recover(); recovered != nil && (!primaryStarted || recovered != http.ErrAbortHandler) {
					t.Fatalf("unexpected panic: %v", recovered)
				}
			}()
			p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validStreamBody)))
		}()
		if primaryStarted {
			if calls != 0 {
				t.Fatal("switched after streaming headers")
			}
		} else if calls != 1 || w.Body.String() != stream {
			t.Fatal("fallback stream was modified")
		}
	}
}

func TestFallbackConfiguration(t *testing.T) {
	p, _ := newProvider("http://localhost", "primary")
	if err := p.configureFallback("", "", ""); err != nil || p.fallback != nil {
		t.Fatal("optional fallback enabled without key")
	}
	if err := p.configureFallback("", "fallback", ""); err != nil || p.fallbackModel != "gpt-4o-mini" || p.fallback.endpoint != "https://api.openai.com/v1/chat/completions" || p.fallback.breaker == p.breaker {
		t.Fatal("fallback defaults or isolation incorrect")
	}
	if p.configureFallback("http://untrusted.example", "fallback", "model") == nil {
		t.Fatal("accepted remote HTTP endpoint")
	}
}
