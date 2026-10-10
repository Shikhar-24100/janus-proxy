package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChatHandler(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"id": "test-completion"})
	}))
	defer upstream.Close()
	provider, err := newProvider(upstream.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"valid", `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}]}`, 200},
		{"empty body", ``, 400},
		{"malformed JSON", `{`, 400},
		{"missing model", `{"messages":[{"role":"user","content":"Hello"}]}`, 400},
		{"empty messages", `{"model":"demo-model","messages":[]}`, 400},
		{"invalid role", `{"model":"demo-model","messages":[{"role":"invalid","content":"Hello"}]}`, 400},
		{"blank content", `{"model":"demo-model","messages":[{"role":"user","content":" "}]}`, 400},
		{"wrong field type", `{"model":123,"messages":[]}`, 400},
		{"unsupported field", `{"model":"demo-model","temperature":0}`, 400},
		{"wrong stream type", `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}],"stream":"yes"}`, 400},
		{"multiple JSON values", `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}]} {}`, 400},
		{"trailing garbage", `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}]} garbage`, 400},
		{"oversized body", `{"model":"demo-model","messages":[{"role":"user","content":"` + strings.Repeat("a", 1<<20) + `"}]}`, 413},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(test.body))
			response := httptest.NewRecorder()
			provider.chatHandler(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.status, response.Body.String())
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatal("response must be JSON")
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("invalid JSON response: %v", err)
			}
			if test.status == 200 {
				if body["id"] != "test-completion" {
					t.Fatalf("unexpected provider response: %v", body)
				}
			} else if body["error"] == nil {
				t.Fatal("error response must contain error details")
			}
		})
	}
}
