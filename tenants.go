package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
)

// Configuration references environment variables; it never contains API keys.
type tenantConfig struct {
	ID              string `json:"id"`
	APIKeyEnv       string `json:"api_key_env"`
	RPM             int    `json:"rpm"`
	TPM             int    `json:"tpm"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	Enabled         *bool  `json:"enabled"`
}

type tenant struct {
	id      string
	enabled bool
	limiter *RateLimiter
	cache   *responseCache
	handler http.Handler
}

// Immutable after startup, so requests can look up tenants without a lock.
type tenantRegistry struct {
	byKey map[[32]byte]*tenant
}

type tenantContextKey struct{}

func tenantFrom(r *http.Request) *tenant {
	t, _ := r.Context().Value(tenantContextKey{}).(*tenant)
	return t
}

var tenantIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
var tenantEnvPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func readTenantConfigs(path string) ([]tenantConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open TENANTS_CONFIG file")
	}
	defer f.Close()
	return decodeTenantConfigs(f)
}

func decodeTenantConfigs(reader io.Reader) ([]tenantConfig, error) {
	data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("tenant configuration must be readable and at most 1 MiB")
	}
	var config struct {
		Tenants []tenantConfig `json:"tenants"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("tenant configuration must contain one JSON object with supported fields")
	}
	if len(config.Tenants) == 0 || len(config.Tenants) > 1000 {
		return nil, errors.New("tenant configuration requires between 1 and 1000 tenants")
	}
	return config.Tenants, nil
}

func buildTenantRegistry(configs []tenantConfig, getenv func(string) string, base *RateLimiter, provider *Provider, ttl string) (*tenantRegistry, error) {
	registry := &tenantRegistry{byKey: make(map[[32]byte]*tenant)}
	ids := make(map[string]bool)
	for index, config := range configs {
		// Report positions rather than echoing potentially secret configuration.
		invalid := func(reason string) (*tenantRegistry, error) {
			return nil, fmt.Errorf("tenant %d: %s", index+1, reason)
		}
		if !tenantIDPattern.MatchString(config.ID) || ids[config.ID] {
			return invalid("id must be unique and contain 1-64 letters, digits, underscores or hyphens, starting with a letter or digit")
		}
		ids[config.ID] = true
		if !tenantEnvPattern.MatchString(config.APIKeyEnv) || config.Enabled == nil {
			return invalid("api_key_env must be an uppercase environment variable name and enabled must be specified")
		}
		if config.RPM < 1 || config.RPM > 1000000 || config.TPM < 1 || config.TPM > 100000000 || config.MaxOutputTokens < 1 || config.MaxOutputTokens >= config.TPM {
			return invalid("invalid RPM, TPM or output limit; output limit must be smaller than TPM")
		}
		key := getenv(config.APIKeyEnv)
		if len(key) == 0 || strings.ContainsAny(key, " \t\r\n") {
			return invalid("API key must be present and contain no whitespace")
		}
		fingerprint := sha256.Sum256([]byte(key))
		if _, exists := registry.byKey[fingerprint]; exists {
			return invalid("API key must be unique across tenants")
		}
		// Stable IDs preserve quota and cache state when client credentials rotate.
		identity := "tenant:" + config.ID
		scope := sha256.Sum256([]byte(identity))
		limiter := &RateLimiter{client: base.client, key: "janus:quota:tenant:{" + fmtHash(scope) + "}", rpm: config.RPM, period: base.period, tpm: config.TPM, outputLimit: config.MaxOutputTokens}
		cache, err := newResponseCache(base.client, ttl, identity, provider)
		if err != nil {
			return nil, err
		}
		t := &tenant{id: config.ID, enabled: *config.Enabled, limiter: limiter, cache: cache}
		t.handler = limitRequests(limiter, http.HandlerFunc(provider.chatHandler), cache)
		registry.byKey[fingerprint] = t
	}
	if len(registry.byKey) == 0 || len(registry.byKey) > 1000 {
		return nil, errors.New("between 1 and 1000 tenants are required")
	}
	return registry, nil
}

func (registry *tenantRegistry) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fingerprint, ok := bearerFingerprint(r)
		t := registry.byKey[fingerprint]
		if !ok || t == nil || !t.enabled {
			writeAuthError(w)
			return
		}
		if trace := traceFrom(r); trace != nil {
			trace.tenantID = t.id
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantContextKey{}, t)))
	})
}

func newTenantMux(provider *Provider, adminKey string, registry *tenantRegistry, observability *telemetry) *http.ServeMux {
	mux := newOperationalMux(provider, adminKey, observability)
	chat := registry.authenticate(observability.admit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantFrom(r).handler.ServeHTTP(w, r)
	})))
	mux.Handle("POST /v1/chat/completions", observability.observe(chat))
	return mux
}
