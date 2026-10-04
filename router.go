package main

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (p *Provider) configureFallback(baseURL, apiKey, model string) error {
	if strings.TrimSpace(apiKey) == "" {
		return nil
	}
	model = strings.TrimSpace(model)
	if model == "" {
		model = "gpt-4o-mini"
	}
	if model == "your-openai-model-id" {
		return errors.New("FALLBACK_MODEL must be a real model ID")
	}
	fallback, err := newProvider(baseURL, apiKey)
	if err != nil {
		return errors.New("FALLBACK_BASE_URL must be a valid trusted HTTPS API base URL")
	}
	p.fallback, p.fallbackModel = fallback, model
	return nil
}

type attemptFailure struct {
	status     int
	message    string
	body       []byte
	retryAfter string
	retryable  bool
}

func gatewayFailure(status int, message string, retryable bool) *attemptFailure {
	return &attemptFailure{status: status, message: message, retryable: retryable}
}

func upstreamFailure(r *http.Request, err error) *attemptFailure {
	if r.Context().Err() != nil {
		return gatewayFailure(502, "Request canceled.", false)
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return gatewayFailure(504, "Provider request timed out.", true)
	}
	return gatewayFailure(502, "Could not complete provider request.", true)
}

func (f *attemptFailure) write(w http.ResponseWriter) {
	if f.retryAfter != "" {
		w.Header().Set("Retry-After", f.retryAfter)
	}
	if f.body == nil {
		writeGatewayError(w, f.status, f.message)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	w.Write(f.body)
}

func (p *Provider) routeChat(w http.ResponseWriter, r *http.Request, input ChatRequest) {
	w.Header().Set("X-Janus-Route", "primary")
	if trace := traceFrom(r); trace != nil {
		trace.route = "primary"
	}
	failure := p.tryChat(w, r, input)
	if failure == nil || r.Context().Err() != nil {
		return
	}
	if failure.retryable && p.fallback != nil {
		// If primary never contacted upstream (e.g. open circuit), reuse the
		// original allocation. Otherwise account for both provider attempts.
		if state := accountingFrom(r); state != nil && state.attempted {
			state.settle()
			decision, err := state.limiter.ReserveTokens(r.Context(), state.reserved)
			if err != nil {
				writeGatewayError(w, 503, "Could not reserve fallback token quota.")
				return
			}
			if !decision.allowed {
				seconds := (decision.retryAfter + time.Second - 1) / time.Second
				if seconds < 1 {
					seconds = 1
				}
				w.Header().Set("Retry-After", strconv.FormatInt(int64(seconds), 10))
				writeJSON(w, 429, map[string]any{"error": map[string]string{"message": "Janus fallback token quota exceeded.", "type": "rate_limit_error"}})
				return
			}
			state.reservation = decision.reservation
			state.attempted, state.settled, state.actual = false, false, nil
			state.usage = nil
		}
		input.Model = p.fallbackModel
		w.Header().Set("X-Janus-Route", "fallback")
		if trace := traceFrom(r); trace != nil {
			trace.route, trace.fallback = "fallback", true
		}
		failure = p.fallback.tryChat(w, r, input)
		if failure == nil || r.Context().Err() != nil {
			return
		}
	}
	failure.write(w)
}
