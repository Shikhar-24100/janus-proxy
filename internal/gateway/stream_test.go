package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const validStreamBody = `{"model":"demo-model","messages":[{"role":"user","content":"Hello"}],"stream":true}`

func streamingGateway(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)
	provider, err := newProvider(upstream.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(http.HandlerFunc(provider.chatHandler))
	t.Cleanup(gateway.Close)
	return gateway
}

func openTestStream(t *testing.T, gateway *httptest.Server) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post(gateway.URL, "application/json", strings.NewReader(validStreamBody))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestStreamArrivesBeforeProviderFinishes(t *testing.T) {
	const stream = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello 🌍\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	// Split in the middle of a UTF-8 character and an SSE event.
	split := bytes.Index([]byte(stream), []byte("🌍")) + 2
	release := make(chan struct{})
	defer close(release)
	gateway := streamingGateway(t, func(w http.ResponseWriter, r *http.Request) {
		var input ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || !input.Stream {
			t.Error("stream=true was not sent upstream")
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		io.WriteString(w, stream[:split])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			io.WriteString(w, stream[split:])
		case <-r.Context().Done():
		}
	})
	response := openTestStream(t, gateway)
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("unexpected stream response: %d %v", response.StatusCode, response.Header)
	}
	first := make([]byte, split)
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatalf("first bytes did not arrive before provider finished: %v", err)
	}
	if string(first) != stream[:split] {
		t.Fatal("first bytes were modified")
	}
	// Only allow the provider to finish AFTER the client received the first bytes.
	release <- struct{}{}
	rest, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(first)+string(rest) != stream {
		t.Fatal("stream framing, UTF-8, or completion marker was modified")
	}
}

func TestStreamClientDisconnectCancelsProvider(t *testing.T) {
	canceled := make(chan struct{})
	gateway := streamingGateway(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	})
	response := openTestStream(t, gateway)
	response.Body.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("provider was not canceled after client disconnected")
	}
}

func TestStreamInterruptedAfterHeaders(t *testing.T) {
	const first = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n"
	gateway := streamingGateway(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, first)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	response := openTestStream(t, gateway)
	body, err := io.ReadAll(response.Body)
	if err == nil {
		t.Fatal("interrupted stream appeared to complete successfully")
	}
	if string(body) != first {
		t.Fatalf("partial stream was modified: %q", body)
	}
}

func TestStreamingRequestErrorsRemainJSON(t *testing.T) {
	for _, status := range []int{200, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			gateway := streamingGateway(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(status)
				io.WriteString(w, `{"error":{"message":"Provider error"}}`)
			})
			response := openTestStream(t, gateway)
			want := status
			if status == 200 {
				want = 502 // A successful streaming request must return SSE.
			}
			body, err := io.ReadAll(response.Body)
			if err != nil || !json.Valid(body) || response.StatusCode != want || response.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("unexpected error response: %d %q, %v", response.StatusCode, body, err)
			}
			if status == 429 && response.Header.Get("Retry-After") != "5" {
				t.Fatal("Retry-After was lost")
			}
		})
	}
}
