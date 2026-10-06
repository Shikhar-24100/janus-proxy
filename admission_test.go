package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionKeepsStreamAndRejectsBeforeHandler(t *testing.T) {
	metrics := newTelemetry(nil)
	metrics.admission, _ = newAdmissionGate("1")
	finish := make(chan struct{})
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: hello\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-finish:
			io.WriteString(w, "data: [DONE]\n\n")
		case <-r.Context().Done():
		}
	})
	server := httptest.NewServer(metrics.observe(requireAPIKey("key", metrics.admit(handler))))
	defer server.Close()
	send := func(key string) *http.Response {
		req, _ := http.NewRequest("POST", server.URL, strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := send("key")
	defer first.Body.Close()
	second := send("key")
	body, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if second.StatusCode != 503 || second.Header.Get("Retry-After") != "1" || !strings.Contains(string(body), "overloaded_error") || calls.Load() != 1 || metrics.admission.active.Load() != 1 {
		t.Fatalf("overload harmed admitted stream: %d %s", second.StatusCode, body)
	}
	unauth := send("wrong")
	unauth.Body.Close()
	if unauth.StatusCode != 401 || metrics.admission.rejected.Load() != 1 {
		t.Fatal("authentication did not precede admission")
	}
	close(finish)
	body, _ = io.ReadAll(first.Body)
	if !strings.Contains(string(body), "[DONE]") {
		t.Fatal("admitted stream was truncated")
	}
	waitFor(t, func() bool { return metrics.admission.active.Load() == 0 })
	third := send("key")
	io.Copy(io.Discard, third.Body)
	third.Body.Close()
	if third.StatusCode != 200 || calls.Load() != 2 {
		t.Fatal("seat not reusable")
	}
}

func TestAdmissionReleasesCancelledStream(t *testing.T) {
	metrics := newTelemetry(nil)
	metrics.admission, _ = newAdmissionGate("1")
	server := httptest.NewServer(metrics.observe(metrics.admit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL, nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	resp.Body.Close()
	waitFor(t, func() bool { return metrics.admission.active.Load() == 0 })
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not recover")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
