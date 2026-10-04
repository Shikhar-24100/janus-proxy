package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Provider holds the configuration and reusable HTTP client for our upstream.
type Provider struct {
	endpoint      string
	apiKey        string
	client        *http.Client
	breaker       *circuitBreaker
	fallback      *Provider
	fallbackModel string
}

func newProvider(baseURL, apiKey string) (*Provider, error) {
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("OPENAI_BASE_URL must be an absolute URL without credentials, query, or fragment")
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, errors.New("OPENAI_BASE_URL must use HTTPS, except for local development")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/chat/completions"
	return &Provider{
		endpoint: u.String(),
		apiKey:   strings.TrimSpace(apiKey),
		breaker:  newCircuitBreaker(),
		client: &http.Client{
			Timeout: 60 * time.Second,
			// Do not follow redirects with provider credentials.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (p *Provider) forwardChat(w http.ResponseWriter, r *http.Request, input ChatRequest) {
	p.routeChat(w, r, input)
}

// No response is committed on an attempt failure; the router can decide to
// try another provider. Once streaming starts, this function never offers retry.
func (p *Provider) tryChat(w http.ResponseWriter, r *http.Request, input ChatRequest) *attemptFailure {
	if p.apiKey == "" {
		return gatewayFailure(503, "Provider API key is not configured.", true)
	}

	body, err := json.Marshal(input)
	if err != nil {
		return gatewayFailure(500, "Could not encode provider request.", false)
	}
	// Janus acts as a client here. The context cancels this call if our client disconnects.
	upstreamRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return gatewayFailure(500, "Could not create provider request.", false)
	}
	upstreamRequest.Header.Set("Content-Type", "application/json")
	upstreamRequest.Header.Set("Authorization", "Bearer "+p.apiKey)

	generation, allowed, retry := p.breaker.acquire()
	if !allowed {
		seconds := (retry + time.Second - 1) / time.Second
		failure := gatewayFailure(503, "Provider circuit is open; try again later.", true)
		failure.retryAfter = strconv.FormatInt(int64(seconds), 10)
		return failure
	}
	outcome := breakerNeutral
	defer func() {
		if r.Context().Err() != nil {
			outcome = breakerNeutral
		}
		p.breaker.finish(generation, outcome)
	}()

	if state := accountingFrom(r); state != nil {
		state.attempted = true
	}
	response, err := p.client.Do(upstreamRequest)
	if err != nil {
		outcome = breakerFailure
		return upstreamFailure(r, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		outcome = breakerFailure
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		outcome = breakerFailure
		return gatewayFailure(502, "Provider returned an unexpected redirect.", true)
	}
	if input.Stream && response.StatusCode == http.StatusOK {
		mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if mediaErr != nil || mediaType != "text/event-stream" {
			outcome = breakerFailure
			return gatewayFailure(502, "Provider did not return an SSE stream.", true)
		}
		p.forwardStream(w, r, response, &outcome)
		return nil
	}

	// Complete JSON responses, including provider errors, use the buffered path.
	const maxResponseBytes = 4 << 20
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		outcome = breakerFailure
		return upstreamFailure(r, err)
	}
	if len(responseBody) > maxResponseBytes || !json.Valid(responseBody) {
		outcome = breakerFailure
		return gatewayFailure(502, "Provider returned an oversized or invalid JSON response.", true)
	}
	if outcome != breakerFailure {
		// Non-429 client errors demonstrate reachability, not an outage.
		outcome = breakerSuccess
	}

	// Keep the provider's JSON and status, including errors such as 429.
	if state := accountingFrom(r); state != nil {
		state.actual = reportedTokens(responseBody)
	}
	if response.StatusCode >= 400 {
		return &attemptFailure{status: response.StatusCode, body: responseBody, retryAfter: response.Header.Get("Retry-After"), retryable: response.StatusCode == 429 || response.StatusCode >= 500}
	}
	w.Header().Set("Content-Type", "application/json")
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(response.StatusCode)
	if _, err := w.Write(responseBody); err != nil {
		log.Println("Could not write provider response to client")
	}
	return nil
}

func writeGatewayError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "gateway_error",
		},
	})
}
