package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func failBreaker(t *testing.T, b *circuitBreaker) {
	t.Helper()
	for i := 0; i < b.threshold; i++ {
		generation, ok, _ := b.acquire()
		if !ok {
			t.Fatal("opened before threshold")
		}
		b.finish(generation, breakerFailure)
	}
}

func TestBreakerTransitions(t *testing.T) {
	now := time.Unix(1000, 0)
	b := newCircuitBreaker()
	b.now = func() time.Time { return now }
	for i := 0; i < 4; i++ {
		generation, _, _ := b.acquire()
		b.finish(generation, breakerFailure)
	}
	generation, _, _ := b.acquire()
	b.finish(generation, breakerSuccess)
	if b.failures != 0 {
		t.Fatal("success did not reset consecutive failures")
	}
	oldGeneration, _, _ := b.acquire()
	failBreaker(t, b)
	b.finish(oldGeneration, breakerSuccess)
	if _, ok, retry := b.acquire(); ok || retry != 30*time.Second {
		t.Fatal("open circuit admitted a call or stale success changed state")
	}
	now = now.Add(30 * time.Second)
	probe, ok, _ := b.acquire()
	if !ok || b.state != breakerHalfOpen {
		t.Fatal("cooldown did not admit probe")
	}
	if _, ok, _ := b.acquire(); ok {
		t.Fatal("admitted second probe")
	}
	b.finish(probe, breakerFailure)
	if _, ok, _ := b.acquire(); ok {
		t.Fatal("failed probe did not reopen")
	}
	now = now.Add(30 * time.Second)
	probe, _, _ = b.acquire()
	b.finish(probe, breakerSuccess)
	if _, ok, _ := b.acquire(); !ok || b.state != breakerClosed {
		t.Fatal("successful probe did not recover")
	}
}

func TestBreakerOneConcurrentProbe(t *testing.T) {
	b := newCircuitBreaker()
	failBreaker(t, b)
	b.now = func() time.Time { return b.openUntil }
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, _ := b.acquire(); ok {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("admitted %d probes", admitted.Load())
	}
}

func TestBreakerCanceledProbeReleasesSlot(t *testing.T) {
	b := newCircuitBreaker()
	now := time.Unix(1000, 0)
	b.now = func() time.Time { return now }
	failBreaker(t, b)
	now = now.Add(30 * time.Second)
	probe, _, _ := b.acquire()
	b.finish(probe, breakerNeutral)
	if b.state != breakerOpen || b.openUntil != now.Add(30*time.Second) {
		t.Fatal("canceled probe left half-open state stuck")
	}
	now = now.Add(30 * time.Second)
	if _, ok, _ := b.acquire(); !ok {
		t.Fatal("could not retry canceled probe")
	}
}

func TestProviderBreakerStatusAndQuota(t *testing.T) {
	for _, status := range []int{429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls, currentStatus atomic.Int32
			currentStatus.Store(int32(status))
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(int(currentStatus.Load()))
				io.WriteString(w, `{}`)
			}))
			defer upstream.Close()
			p, _ := newProvider(upstream.URL, "test")
			for i := 0; i < 5; i++ {
				w := httptest.NewRecorder()
				p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
				if w.Code != status {
					t.Fatalf("provider status changed: %d", w.Code)
				}
			}
			limiter := &stubLimiter{decision: rateDecision{allowed: true, reservation: "blocked"}}
			w := httptest.NewRecorder()
			limitRequests(limiter, http.HandlerFunc(p.chatHandler)).ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
			if w.Code != 503 || w.Header().Get("Retry-After") != "30" || calls.Load() != 5 {
				t.Fatalf("blocked response=%d retry=%s upstream calls=%d", w.Code, w.Header().Get("Retry-After"), calls.Load())
			}
			if limiter.calls != 1 || len(limiter.settled) != 1 || limiter.settled[0] != 0 {
				t.Fatal("blocked request did not release TPM reservation")
			}
			p.breaker.now = func() time.Time { return p.breaker.openUntil }
			currentStatus.Store(200)
			w = httptest.NewRecorder()
			p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
			if w.Code != 200 || p.breaker.state != breakerClosed {
				t.Fatal("provider did not recover")
			}
		})
	}
}

type breakerTransport func(*http.Request) (*http.Response, error)

func (f breakerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderBreakerClassification(t *testing.T) {
	for _, test := range []struct {
		name         string
		status       int
		body         string
		err          error
		cancel       bool
		wantFailures int
	}{
		{"bad request", 400, `{}`, nil, false, 0},
		{"invalid credentials", 401, `{}`, nil, false, 0},
		{"network error", 0, "", errors.New("connection refused"), false, 1},
		{"timeout", 0, "", context.DeadlineExceeded, false, 1},
		{"client canceled", 0, "", context.Canceled, true, 0},
		{"invalid JSON", 200, "broken", nil, false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, _ := newProvider("http://localhost", "test")
			p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
				if test.err != nil {
					return nil, test.err
				}
				return &http.Response{StatusCode: test.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})
			r := httptest.NewRequest("POST", "/", strings.NewReader(validChatBody))
			if test.cancel {
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			}
			p.chatHandler(httptest.NewRecorder(), r)
			if p.breaker.failures != test.wantFailures {
				t.Fatalf("failures=%d want=%d", p.breaker.failures, test.wantFailures)
			}
		})
	}
}

func TestBreakerStreamCompletion(t *testing.T) {
	for _, test := range []struct {
		name, body string
		want       int
	}{
		{"complete without usage", "data: {}\n\ndata: [DONE]\n\n", 0},
		{"incomplete", "data: {}\n\n", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, _ := newProvider("http://localhost", "test")
			p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})
			w := httptest.NewRecorder()
			p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validStreamBody)))
			if p.breaker.failures != test.want || w.Body.String() != test.body {
				t.Fatal("stream classification or forwarding failed")
			}
		})
	}
}

type breakerReadFailure struct{}

func (breakerReadFailure) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (breakerReadFailure) Close() error             { return nil }

type breakerWriteFailure struct{ *httptest.ResponseRecorder }

func (breakerWriteFailure) Write([]byte) (int, error) { return 0, errors.New("client disconnected") }

func TestBreakerStreamAbortOutcome(t *testing.T) {
	for _, upstreamFailure := range []bool{true, false} {
		p, _ := newProvider("http://localhost", "test")
		p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
			var body io.ReadCloser = io.NopCloser(strings.NewReader("data: {}\n\n"))
			if upstreamFailure {
				body = breakerReadFailure{}
			}
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
		})
		var w http.ResponseWriter = httptest.NewRecorder()
		if !upstreamFailure {
			w = breakerWriteFailure{httptest.NewRecorder()}
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != http.ErrAbortHandler {
					t.Fatalf("expected stream abort, got %v", recovered)
				}
			}()
			p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validStreamBody)))
		}()
		want := 0
		if upstreamFailure {
			want = 1
		}
		if p.breaker.failures != want {
			t.Fatalf("upstreamFailure=%v failures=%d want=%d", upstreamFailure, p.breaker.failures, want)
		}
	}
}

func TestBreakerStreamingProbeWaitsForCompletion(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	started := make(chan struct{})
	p, _ := newProvider("http://localhost", "test")
	failBreaker(t, p.breaker)
	now := p.breaker.openUntil
	p.breaker.now = func() time.Time { return now }
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		close(started)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
	})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p.chatHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(validStreamBody)))
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not start")
	}
	w := httptest.NewRecorder()
	p.chatHandler(w, httptest.NewRequest("POST", "/", strings.NewReader(validChatBody)))
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("second call admitted before probe stream finished")
	}
	io.WriteString(writer, "data: [DONE]\n\n")
	writer.Close()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not finish")
	}
	if p.breaker.state != breakerClosed {
		t.Fatal("completed probe did not close circuit")
	}
}
