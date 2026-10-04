package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type rateDecision struct {
	allowed        bool
	remaining      int64
	retryAfter     time.Duration
	tokenRemaining int64
	reservation    string
	tokenDenied    bool
}

type requestLimiter interface {
	Reserve(context.Context, int64) (rateDecision, error)
	ReserveTokens(context.Context, int64) (rateDecision, error)
	Settle(context.Context, string, int64) error
	Limit() int
	TokenLimit() int
	OutputLimit() int
}

type RateLimiter struct {
	client      *redis.Client
	key         string
	rpm         int
	period      time.Duration
	tpm         int
	outputLimit int
}

func newRateLimiter(redisURL, rpmSetting, janusKey string) (*RateLimiter, error) {
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379/0"
	}
	if rpmSetting == "" {
		rpmSetting = "60"
	}
	rpm, err := strconv.Atoi(rpmSetting)
	if err != nil || rpm < 1 || rpm > 1_000_000 {
		return nil, fmt.Errorf("RPM_LIMIT must be an integer between 1 and 1000000")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL must be a valid Redis connection URL")
	}
	options.DialTimeout = 500 * time.Millisecond
	options.ReadTimeout = 500 * time.Millisecond
	options.WriteTimeout = 500 * time.Millisecond
	options.PoolTimeout = 500 * time.Millisecond
	options.MaxRetries = -1 // Avoid retrying an admission decision that may have executed.
	options.ContextTimeoutEnabled = true
	fingerprint := sha256.Sum256([]byte(janusKey))
	return &RateLimiter{
		client:      redis.NewClient(options),
		key:         fmt.Sprintf("janus:quota:{%x}", fingerprint),
		rpm:         rpm,
		period:      time.Minute,
		tpm:         60000,
		outputLimit: 1024,
	}, nil
}

func (l *RateLimiter) Limit() int { return l.rpm }

func (l *RateLimiter) Allow(ctx context.Context) (rateDecision, error) {
	return l.Reserve(ctx, 0)
}

func limitRequests(limiter requestLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		input, ok := decodeChatRequest(w, r, limiter.OutputLimit())
		if !ok {
			return
		}
		reserved := estimateInputTokens(input) + int64(*input.MaxCompletionTokens)
		if reserved > int64(limiter.TokenLimit()) {
			writeRequestError(w, 400, "Prompt estimate plus output allowance exceeds TPM_LIMIT; reduce the request.")
			return
		}
		decision, err := limiter.Reserve(r.Context(), reserved)
		if err != nil {
			writeGatewayError(w, http.StatusServiceUnavailable, "Rate limiter is unavailable.")
			return
		}
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limiter.Limit()))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(decision.remaining, 10))
		w.Header().Set("X-TokenLimit-Limit", strconv.Itoa(limiter.TokenLimit()))
		w.Header().Set("X-TokenLimit-Remaining", strconv.FormatInt(decision.tokenRemaining, 10))
		w.Header().Set("X-TokenLimit-Reserved", strconv.FormatInt(reserved, 10))
		if !decision.allowed {
			seconds := (decision.retryAfter + time.Second - 1) / time.Second
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.FormatInt(int64(seconds), 10))
			message := "Janus request rate limit exceeded."
			if decision.tokenDenied {
				message = "Janus token quota exceeded."
			}
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": map[string]string{"message": message, "type": "rate_limit_error"},
			})
			return
		}
		state := &requestAccounting{input: input, limiter: limiter, reservation: decision.reservation, reserved: reserved}
		defer state.settle()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), accountingKey{}, state)))
	})
}
