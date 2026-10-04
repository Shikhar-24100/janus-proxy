package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Explicit opt-in only: this test sends two small requests to the configured fallback.
// The normal suite uses fake providers and never reads local credentials.
func TestLiveConfiguredFallback(t *testing.T) {
	if os.Getenv("JANUS_LIVE_FALLBACK") != "1" {
		t.Skip("opt in to two potentially billed fallback requests")
	}
	if strings.TrimSpace(os.Getenv("FALLBACK_API_KEY")) == "" {
		t.Fatal("fallback key is required")
	}
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{}`)
	}))
	defer primary.Close()
	p, _ := newProvider(primary.URL, "fake-primary-key")
	if err := p.configureFallback(os.Getenv("FALLBACK_BASE_URL"), os.Getenv("FALLBACK_API_KEY"), os.Getenv("FALLBACK_MODEL")); err != nil {
		t.Fatal(err)
	}
	limiter := testQuota(t, "60", 60000)
	gateway := httptest.NewServer(newMux(p, "live-test-client", limiter))
	defer gateway.Close()
	for _, stream := range []bool{false, true} {
		streamValue := "false"
		if stream {
			streamValue = "true"
		}
		body := `{"model":"fake-primary-model","messages":[{"role":"user","content":"Reply with exactly: fallback works."}],"max_completion_tokens":256,"stream":` + streamValue + `}`
		r, _ := http.NewRequest("POST", gateway.URL+"/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer live-test-client")
		r.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal("live gateway request failed")
		}
		data, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != 200 || response.Header.Get("X-Janus-Route") != "fallback" {
			t.Fatalf("live fallback failed: HTTP %d; credentials and response body omitted", response.StatusCode)
		}
		var usage *int64
		if stream {
			observer := usageObserver{}
			observer.Feed(data)
			if !observer.done {
				t.Fatal("live stream missing DONE")
			}
			usage = observer.actual
		} else {
			usage = reportedTokens(data)
		}
		if usage == nil || !strings.Contains(strings.ToLower(string(data)), "fallback") {
			t.Fatal("live fallback missing usage or visible answer")
		}
		t.Logf("Configured fallback stream=%v actual total tokens=%d", stream, *usage)
	}
}
