package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const validChatBody = `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}]}`

func TestForwardChat(t *testing.T) {
	const completion = `{"id":"chatcmpl-test","choices":[{"message":{"role":"assistant","content":"Hello back"}}],"usage":{"total_tokens":7}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing provider headers")
		}
		var input ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if input.Model != "demo-model" || len(input.Messages) != 1 || input.Messages[0].Content != "Hello" || input.Stream {
			t.Errorf("unexpected payload: %+v", input)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(completion))
	}))
	defer upstream.Close()
	provider, err := newProvider(upstream.URL+"/v1/", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody))
	request.Header.Set("Authorization", "Bearer client-key")
	response := httptest.NewRecorder()
	provider.chatHandler(response, request)
	if response.Code != 200 || response.Body.String() != completion {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestProviderFailures(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantStatus int
	}{
		{"rate limited", 429, `{"error":{"message":"Rate limited"}}`, 429},
		{"upstream failure", 500, `{"error":{"message":"Unavailable"}}`, 500},
		{"invalid JSON", 200, `<html>Bad response</html>`, 502},
		{"oversized response", 200, `"` + strings.Repeat("a", 4<<20) + `"`, 502},
		{"redirect", 307, `{}`, 502},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "10")
				w.Header().Set("Location", "/other")
				w.WriteHeader(test.status)
				w.Write([]byte(test.body))
			}))
			defer upstream.Close()
			provider, err := newProvider(upstream.URL, "test-key")
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			provider.chatHandler(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody)))
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if test.wantStatus == test.status && response.Body.String() != test.body {
				t.Fatal("provider error body was changed")
			}
			if test.status == 429 && response.Header().Get("Retry-After") != "10" {
				t.Fatal("Retry-After was not forwarded")
			}
		})
	}
}

func TestProviderTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer upstream.Close()
	provider, err := newProvider(upstream.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	provider.client.Timeout = 100 * time.Millisecond
	response := httptest.NewRecorder()
	provider.chatHandler(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody)))
	if response.Code != 504 {
		t.Fatalf("status = %d, want 504", response.Code)
	}
}

func TestMissingProviderKey(t *testing.T) {
	provider, err := newProvider("", "")
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	provider.chatHandler(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(validChatBody)))
	if response.Code != 503 {
		t.Fatalf("status = %d, want 503", response.Code)
	}
}

func TestProviderURL(t *testing.T) {
	for _, baseURL := range []string{"not-a-url", "http://example.com/v1", "https://user:secret@example.com/v1", "https://example.com/v1?key=secret", "https://example.com/v1#fragment"} {
		if _, err := newProvider(baseURL, "test-key"); err == nil {
			t.Errorf("accepted invalid URL: %s", baseURL)
		}
	}
	provider, err := newProvider("", "test-key")
	if err != nil || provider.endpoint != "https://api.openai.com/v1/chat/completions" {
		t.Fatalf("unexpected default provider: %+v, %v", provider, err)
	}
}
