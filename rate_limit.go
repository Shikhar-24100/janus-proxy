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

// Redis runs the whole read/refill/check/update operation atomically.
// Redis time avoids clock differences between gateway instances.
var requestBucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local period_ms = tonumber(ARGV[2])
local clock = redis.call('TIME')
local now_ms = tonumber(clock[1]) * 1000 + tonumber(clock[2]) / 1000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_ms')
local tokens = tonumber(state[1]) or capacity
local updated_ms = tonumber(state[2]) or now_ms
local elapsed = math.max(0, now_ms - updated_ms)
tokens = math.min(capacity, tokens + elapsed * capacity / period_ms)
local allowed = 0
local retry_ms = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_ms = math.ceil((1 - tokens) * period_ms / capacity)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_ms', math.max(now_ms, updated_ms))
redis.call('PEXPIRE', KEYS[1], math.ceil(period_ms * 2))
return {allowed, math.floor(tokens), retry_ms}
`)

type rateDecision struct {
	allowed    bool
	remaining  int64
	retryAfter time.Duration
}

type requestLimiter interface {
	Allow(context.Context) (rateDecision, error)
	Limit() int
}

type RateLimiter struct {
	client *redis.Client
	key    string
	rpm    int
	period time.Duration
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
		client: redis.NewClient(options),
		key:    fmt.Sprintf("janus:rpm:%x", fingerprint),
		rpm:    rpm,
		period: time.Minute,
	}, nil
}

func (l *RateLimiter) Limit() int { return l.rpm }

func (l *RateLimiter) Allow(ctx context.Context) (rateDecision, error) {
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	result, err := requestBucketScript.Run(ctx, l.client, []string{l.key}, l.rpm, l.period.Milliseconds()).Int64Slice()
	if err != nil {
		return rateDecision{}, err
	}
	if len(result) != 3 {
		return rateDecision{}, fmt.Errorf("unexpected Redis rate-limit result")
	}
	return rateDecision{result[0] == 1, result[1], time.Duration(result[2]) * time.Millisecond}, nil
}

func limitRequests(limiter requestLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decision, err := limiter.Allow(r.Context())
		if err != nil {
			writeGatewayError(w, http.StatusServiceUnavailable, "Rate limiter is unavailable.")
			return
		}
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limiter.Limit()))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(decision.remaining, 10))
		if !decision.allowed {
			seconds := (decision.retryAfter + time.Second - 1) / time.Second
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.FormatInt(int64(seconds), 10))
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": map[string]string{"message": "Janus request rate limit exceeded.", "type": "rate_limit_error"},
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
