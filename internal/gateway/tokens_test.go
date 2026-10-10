package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTokenEstimate(t *testing.T) {
	input := ChatRequest{Messages: []Message{{Role: "user", Content: "Hello"}}}
	if got := estimateInputTokens(input); got != 57 {
		t.Fatalf("estimate = %d, want 32 + 5 + 4 + 16 = 57", got)
	}
}

func TestUsageObserver(t *testing.T) {
	const usage = `{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}`
	stream := "data: " + usage + "\r\n\r\ndata: [DONE]\r\n\r\n"
	observer := usageObserver{}
	for _, b := range []byte(stream) {
		observer.Feed([]byte{b})
	}
	if !observer.done || observer.actual == nil || *observer.actual != 50 {
		t.Fatalf("split usage event was not assembled: %+v", observer)
	}
	groq := reportedTokens([]byte(`{"x_groq":{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}}`))
	if groq == nil || *groq != 50 {
		t.Fatal("Groq usage not parsed")
	}
	for _, invalid := range []string{`{}`, `{"usage":{"total_tokens":50}}`, `{"usage":{"prompt_tokens":-1,"completion_tokens":51,"total_tokens":50}}`, `{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":99}}`} {
		if reportedTokens([]byte(invalid)) != nil {
			t.Fatalf("accepted invalid usage: %s", invalid)
		}
	}
	observer = usageObserver{}
	observer.Feed([]byte("data: " + strings.Repeat("x", 65537)))
	if !observer.disabled || observer.actual != nil {
		t.Fatal("oversized observer was not bounded")
	}
}

func TestQuotaUsageSettlement(t *testing.T) {
	const usageJSON = `{"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50}}`
	for _, test := range []struct {
		name, response string
		stream, settle bool
	}{
		{"normal usage", usageJSON, false, true},
		{"stream usage", "data: " + usageJSON + "\n\ndata: [DONE]\n\n", true, true},
		{"stream missing DONE", "data: " + usageJSON + "\n\n", true, false},
		{"stream missing usage", "data: {}\n\ndata: [DONE]\n\n", true, false},
		{"normal missing usage", `{ "id": "test" }`, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.stream {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				w.Write([]byte(test.response))
			}))
			defer upstream.Close()
			provider, err := newProvider(upstream.URL, "test-provider-key")
			if err != nil {
				t.Fatal(err)
			}
			limiter := &stubLimiter{decision: rateDecision{allowed: true, reservation: "test"}}
			body := validChatBody
			if test.stream {
				body = validStreamBody
			}
			response := httptest.NewRecorder()
			limitRequests(limiter, http.HandlerFunc(provider.chatHandler)).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
			if response.Body.String() != test.response {
				t.Fatal("provider response bytes changed")
			}
			if limiter.reserved != 1081 {
				t.Fatalf("reservation=%d, want 57+1024", limiter.reserved)
			}
			if test.settle {
				if len(limiter.settled) != 1 || limiter.settled[0] != 50 {
					t.Fatalf("settlement=%v", limiter.settled)
				}
			} else if len(limiter.settled) != 0 {
				t.Fatal("unknown usage was refunded")
			}
		})
	}
}

func TestOutputLimitValidation(t *testing.T) {
	for _, value := range []string{"0", "-1", "1025"} {
		body := `{"model":"demo","messages":[{"role":"user","content":"Hi"}],"max_completion_tokens":` + value + `}`
		response := httptest.NewRecorder()
		if _, ok := decodeChatRequest(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), 1024); ok || response.Code != 400 {
			t.Fatal("invalid output limit accepted")
		}
	}
}
