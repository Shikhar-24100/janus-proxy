package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRequireAPIKey(t *testing.T) {
	const key = "test-janus-key"
	tests := []struct {
		name    string
		headers []string
		allowed bool
	}{
		{"missing", nil, false},
		{"empty", []string{""}, false},
		{"missing token", []string{"Bearer"}, false},
		{"wrong scheme", []string{"Basic " + key}, false},
		{"wrong key", []string{"Bearer wrong-key"}, false},
		{"extra token", []string{"Bearer " + key + " extra"}, false},
		{"duplicate headers", []string{"Bearer " + key, "Bearer " + key}, false},
		{"valid", []string{"Bearer " + key}, true},
		{"case insensitive scheme", []string{"bearer " + key}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := requireAPIKey(key, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("not JSON"))
			for _, header := range test.headers {
				request.Header.Add("Authorization", header)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if called != test.allowed {
				t.Fatalf("handler called = %v, want %v", called, test.allowed)
			}
			if !test.allowed {
				if response.Code != 401 || response.Header().Get("WWW-Authenticate") == "" || !json.Valid(response.Body.Bytes()) {
					t.Fatalf("unexpected auth error: %d %s", response.Code, response.Body.String())
				}
			} else if response.Code != 204 {
				t.Fatalf("status = %d, want 204", response.Code)
			}
		})
	}
}

func TestEmptyConfiguredKeyDeniesAccess(t *testing.T) {
	response := httptest.NewRecorder()
	handler := requireAPIKey("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("empty configured key allowed a request")
	}))
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if response.Code != 401 {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}

func TestAuthenticatedRoutes(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer provider-test-key" {
			t.Error("Janus client key was forwarded instead of the provider key")
		}
		var input ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if input.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {}\n\ndata: [DONE]\n\n")
			w.(http.Flusher).Flush()
		} else {
			writeJSON(w, 200, map[string]string{"id": "test-completion"})
		}
	}))
	defer upstream.Close()
	provider, err := newProvider(upstream.URL, "provider-test-key")
	if err != nil {
		t.Fatal(err)
	}
	mux := newMux(provider, "janus-test-key")
	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != 200 || calls.Load() != 0 {
		t.Fatal("health endpoint should stay public and not call provider")
	}
	for _, body := range []string{validChatBody, validStreamBody} {
		for _, authorized := range []bool{false, true} {
			before := calls.Load()
			request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			if authorized {
				request.Header.Set("Authorization", "Bearer janus-test-key")
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if authorized {
				if response.Code != 200 || calls.Load() != before+1 {
					t.Fatalf("authenticated request failed: %d %s", response.Code, response.Body.String())
				}
				if body == validStreamBody && !strings.Contains(response.Body.String(), "data: [DONE]") {
					t.Fatal("authenticated stream lost its completion marker")
				}
			} else if response.Code != 401 || calls.Load() != before {
				t.Fatal("unauthorized request reached provider or returned wrong status")
			}
		}
	}
}
