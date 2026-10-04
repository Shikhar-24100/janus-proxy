package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxCacheBytes = 256 << 10

type responseCache struct {
	client *redis.Client
	ttl    time.Duration
	scope  string
}

type cacheCandidate struct {
	cache *responseCache
	key   string
}

func newResponseCache(client *redis.Client, ttlSetting, janusKey string, provider *Provider) (*responseCache, error) {
	if ttlSetting == "" {
		ttlSetting = "300"
	}
	seconds, err := strconv.Atoi(ttlSetting)
	if err != nil || seconds < 0 || seconds > 86400 {
		return nil, errors.New("CACHE_TTL_SECONDS must be an integer between 0 and 86400")
	}
	if seconds == 0 {
		return nil, nil
	}
	// Configuration and credential changes isolate entries without exposing secrets.
	identity := janusKey + "\x00" + provider.endpoint + "\x00" + provider.apiKey
	if provider.fallback != nil {
		identity += "\x00" + provider.fallback.endpoint + "\x00" + provider.fallback.apiKey + "\x00" + provider.fallbackModel
	}
	fingerprint := sha256.Sum256([]byte(identity))
	return &responseCache{client: client, ttl: time.Duration(seconds) * time.Second, scope: "janus:cache:v1:" + fmtHash(fingerprint)}, nil
}

func fmtHash(hash [32]byte) string { return hex.EncodeToString(hash[:]) }

func (c *responseCache) key(input ChatRequest) string {
	data, _ := json.Marshal(input) // Supported fields cannot fail JSON encoding.
	return c.scope + ":" + fmtHash(sha256.Sum256(data))
}

func wantsCache(r *http.Request, input ChatRequest, cache *responseCache) bool {
	values := r.Header.Values("X-Janus-Cache")
	return cache != nil && !input.Stream && len(values) == 1 && strings.EqualFold(strings.TrimSpace(values[0]), "true")
}

// Only complete text answers are reusable; errors, refusals, truncation, tools,
// and empty or oversized answers are excluded from this first version.
func cacheableAnswer(data []byte) bool {
	if len(data) == 0 || len(data) > maxCacheBytes {
		return false
	}
	var answer struct {
		Error   json.RawMessage `json:"error"`
		Model   string          `json:"model"`
		Choices []struct {
			Finish  string `json:"finish_reason"`
			Message struct {
				Role    string          `json:"role"`
				Content string          `json:"content"`
				Refusal *string         `json:"refusal"`
				Tools   json.RawMessage `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &answer) != nil || len(answer.Choices) == 0 || answer.Model == "" || (len(answer.Error) != 0 && string(answer.Error) != "null") {
		return false
	}
	for _, choice := range answer.Choices {
		if choice.Finish != "stop" || choice.Message.Role != "assistant" || strings.TrimSpace(choice.Message.Content) == "" || choice.Message.Refusal != nil {
			return false
		}
		if tools := string(choice.Message.Tools); tools != "" && tools != "null" && tools != "[]" {
			return false
		}
	}
	return true
}

func (c *responseCache) get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	// Bounded read even if a damaged/external entry is unexpectedly huge.
	data, err := c.client.GetRange(ctx, key, 0, maxCacheBytes).Bytes()
	if err != nil || len(data) == 0 {
		return nil, err
	}
	if !cacheableAnswer(data) {
		return nil, errors.New("invalid cache entry")
	}
	return data, nil
}

func (c *responseCache) put(ctx context.Context, key string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	return c.client.Set(ctx, key, data, c.ttl).Err()
}

func setCacheResult(w http.ResponseWriter, r *http.Request, result string) {
	w.Header().Set("X-Janus-Cache", result)
	if trace := traceFrom(r); trace != nil {
		trace.cache = result
	}
}
