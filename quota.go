package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RPM uses a refillable bucket. TPM uses a rolling ledger of reservations.
// Both admissions happen in one script: rejection consumes neither quota.
var reserveQuotaScript = redis.NewScript(`
local rpm, window, tpm, amount = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local clock = redis.call('TIME')
local now = math.floor(tonumber(clock[1])*1000 + tonumber(clock[2])/1000)
local total = tonumber(redis.call('GET', KEYS[4])) or 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now-window)
for _, id in ipairs(expired) do
    total = total - (tonumber(redis.call('HGET', KEYS[3], id)) or 0)
    redis.call('HDEL', KEYS[3], id, id..':settled')
    redis.call('ZREM', KEYS[2], id)
end
total = math.max(0, total)
redis.call('SET', KEYS[4], total, 'PX', window*2)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_ms')
local tokens = tonumber(state[1]) or rpm
local updated = tonumber(state[2]) or now
tokens = math.min(rpm, tokens + math.max(0, now-updated)*rpm/window)
if amount > tpm then return {0, math.floor(tokens), tpm-total, 0, 2} end
local retry = 0
local reason = 0
if tokens < 1 then retry = math.ceil((1-tokens)*window/rpm); reason = 1 end
if total+amount > tpm then
    local freed = 0
    local events = redis.call('ZRANGE', KEYS[2], 0, -1, 'WITHSCORES')
    for i=1,#events,2 do
        freed = freed + (tonumber(redis.call('HGET', KEYS[3], events[i])) or 0)
        if total-freed+amount <= tpm then
            retry = math.max(retry, tonumber(events[i+1])+window-now)
            break
        end
    end
    reason = 2
end
if reason ~= 0 then return {0, math.floor(tokens), math.max(0,tpm-total), math.max(1,retry), reason} end
tokens = tokens-1
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_ms', math.max(now,updated))
if amount > 0 then
    redis.call('ZADD', KEYS[2], now, ARGV[5])
    redis.call('HSET', KEYS[3], ARGV[5], amount)
    total = total+amount
end
redis.call('SET', KEYS[4], total, 'PX', window*2)
for i=1,3 do redis.call('PEXPIRE', KEYS[i], window*2) end
return {1, math.floor(tokens), tpm-total, 0, 0}
`)

// Settlement changes the charge at its ORIGINAL admission time, once only.
// A late settlement never grants credits in a later rolling window.
var settleQuotaScript = redis.NewScript(`
local id, actual, window = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local reserved = redis.call('HGET', KEYS[3], id)
if not reserved or redis.call('HEXISTS', KEYS[3], id..':settled') == 1 then return 0 end
local timestamp = redis.call('ZSCORE', KEYS[2], id)
local clock = redis.call('TIME')
local now = math.floor(tonumber(clock[1])*1000 + tonumber(clock[2])/1000)
if not timestamp or tonumber(timestamp) <= now-window then return 0 end
local total = tonumber(redis.call('GET', KEYS[4])) or 0
redis.call('HSET', KEYS[3], id, actual, id..':settled', 1)
redis.call('SET', KEYS[4], math.max(0,total-tonumber(reserved)+actual), 'PX', window*2)
for i=2,3 do redis.call('PEXPIRE', KEYS[i], window*2) end
return 1
`)

func (l *RateLimiter) quotaKeys() []string {
	return []string{l.key, l.key + ":events", l.key + ":charges", l.key + ":total"}
}

func (l *RateLimiter) TokenLimit() int  { return l.tpm }
func (l *RateLimiter) OutputLimit() int { return l.outputLimit }

func (l *RateLimiter) Reserve(ctx context.Context, amount int64) (rateDecision, error) {
	if amount < 0 {
		return rateDecision{}, fmt.Errorf("negative reservation")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return rateDecision{}, err
	}
	id := hex.EncodeToString(idBytes)
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	result, err := reserveQuotaScript.Run(ctx, l.client, l.quotaKeys(), l.rpm, l.period.Milliseconds(), l.tpm, amount, id).Int64Slice()
	if err != nil {
		return rateDecision{}, err
	}
	if len(result) != 5 {
		return rateDecision{}, fmt.Errorf("unexpected quota result")
	}
	return rateDecision{allowed: result[0] == 1, remaining: result[1], tokenRemaining: result[2], retryAfter: time.Duration(result[3]) * time.Millisecond, reservation: id, tokenDenied: result[4] == 2}, nil
}

func (l *RateLimiter) Settle(ctx context.Context, id string, actual int64) error {
	if actual < 0 {
		return fmt.Errorf("negative usage")
	}
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	return settleQuotaScript.Run(ctx, l.client, l.quotaKeys(), id, actual, l.period.Milliseconds()).Err()
}
