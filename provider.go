package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Provider holds the configuration and reusable HTTP client for our upstream.
type Provider struct {
	endpoint string
	apiKey   string
	client   *http.Client
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
	if p.apiKey == "" {
		writeGatewayError(w, http.StatusServiceUnavailable, "Provider API key is not configured.")
		return
	}

	body, err := json.Marshal(input)
	if err != nil {
		writeGatewayError(w, http.StatusInternalServerError, "Could not encode provider request.")
		return
	}
	// Janus acts as a client here. The context cancels this call if our client disconnects.
	upstreamRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		writeGatewayError(w, http.StatusInternalServerError, "Could not create provider request.")
		return
	}
	upstreamRequest.Header.Set("Content-Type", "application/json")
	upstreamRequest.Header.Set("Authorization", "Bearer "+p.apiKey)

	response, err := p.client.Do(upstreamRequest)
	if err != nil {
		p.writeUpstreamError(w, r, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		writeGatewayError(w, http.StatusBadGateway, "Provider returned an unexpected redirect.")
		return
	}
	if input.Stream && response.StatusCode == http.StatusOK {
		p.forwardStream(w, r, response)
		return
	}

	// Complete JSON responses, including provider errors, use the buffered path.
	const maxResponseBytes = 4 << 20
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		p.writeUpstreamError(w, r, err)
		return
	}
	if len(responseBody) > maxResponseBytes || !json.Valid(responseBody) {
		writeGatewayError(w, http.StatusBadGateway, "Provider returned an oversized or invalid JSON response.")
		return
	}

	// Keep the provider's JSON and status, including errors such as 429.
	w.Header().Set("Content-Type", "application/json")
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(response.StatusCode)
	if _, err := w.Write(responseBody); err != nil {
		log.Println("Could not write provider response to client")
	}
}

func (p *Provider) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		writeGatewayError(w, http.StatusGatewayTimeout, "Provider request timed out.")
		return
	}
	writeGatewayError(w, http.StatusBadGateway, "Could not complete provider request.")
}

func writeGatewayError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "gateway_error",
		},
	})
}
