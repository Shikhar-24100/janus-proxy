package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func tenantTestConfig(id, env string, rpm int) tenantConfig {
	enabled := true
	return tenantConfig{ID: id, APIKeyEnv: env, RPM: rpm, TPM: 1000, MaxOutputTokens: 128, Enabled: &enabled}
}

func TestTenantConfigurationValidation(t *testing.T) {
	valid := `{"tenants":[{"id":"alice","api_key_env":"ALICE_JANUS_API_KEY","rpm":30,"tpm":20000,"max_output_tokens":512,"enabled":true}]}`
	for _, bad := range []string{`null`, `{}`, `{"tenants":[]}`, valid + `{}`, strings.Replace(valid, `"enabled":true`, `"enabled":true,"api_key":"secret"`, 1), strings.Repeat(" ", (1<<20)+1)} {
		if _, err := decodeTenantConfigs(strings.NewReader(bad)); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	configs, err := decodeTenantConfigs(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	base := &RateLimiter{period: time.Minute}
	p, _ := newProvider("http://localhost", "provider-key")
	getenv := func(string) string { return "test-client-key" }
	if _, err := buildTenantRegistry(configs, getenv, base, p, "300"); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*tenantConfig){
		func(c *tenantConfig) { c.ID = "bad/id" },
		func(c *tenantConfig) { c.APIKeyEnv = "invalid-name" },
		func(c *tenantConfig) { c.Enabled = nil },
		func(c *tenantConfig) { c.RPM = 0 },
		func(c *tenantConfig) { c.RPM = 1000001 },
		func(c *tenantConfig) { c.TPM = 100000001 },
		func(c *tenantConfig) { c.MaxOutputTokens = c.TPM },
	} {
		config := configs[0]
		mutate(&config)
		if _, err := buildTenantRegistry([]tenantConfig{config}, getenv, base, p, "300"); err == nil {
			t.Fatal("invalid tenant settings accepted")
		}
	}
	for _, key := range []string{"", "contains space", "trailing\n"} {
		if _, err := buildTenantRegistry(configs, func(string) string { return key }, base, p, "300"); err == nil {
			t.Fatal("missing or malformed credential accepted")
		}
	}
	duplicate := append([]tenantConfig{}, configs[0], configs[0])
	if _, err := buildTenantRegistry(duplicate, getenv, base, p, "300"); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	duplicate[1].ID = "bob"
	if _, err := buildTenantRegistry(duplicate, getenv, base, p, "300"); err == nil {
		t.Fatal("duplicate credential accepted")
	}
}

func tenantCall(mux http.Handler, key, body string, optIn bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+key)
	if optIn {
		r.Header.Set("X-Janus-Cache", "true")
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func cleanTenantState(t *testing.T, registry *tenantRegistry) {
	t.Helper()
	t.Cleanup(func() {
		for _, tenant := range registry.byKey {
			tenant.limiter.client.Del(context.Background(), tenant.limiter.quotaKeys()...)
			if tenant.cache == nil {
				continue
			}
			var cursor uint64
			for {
				keys, next, err := tenant.cache.client.Scan(context.Background(), cursor, tenant.cache.scope+":*", 100).Result()
				if err != nil {
					t.Error(err)
					break
				}
				if len(keys) > 0 {
					tenant.cache.client.Del(context.Background(), keys...)
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
	})
}

func TestRedisTenantIsolationRotationAndLogs(t *testing.T) {
	base := testQuota(t, "60", 60000)
	id := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano())
	configs := []tenantConfig{tenantTestConfig(id+"-alice", "ALICE_JANUS_API_KEY", 2), tenantTestConfig(id+"-bob", "BOB_JANUS_API_KEY", 5)}
	keys := map[string]string{"ALICE_JANUS_API_KEY": "alice-test-key", "BOB_JANUS_API_KEY": "bob-test-key"}
	var calls atomic.Int32
	p, _ := newProvider("http://localhost", "provider-test-key")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer provider-test-key" {
			t.Error("tenant credential reached provider")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	registry, err := buildTenantRegistry(configs, func(name string) string { return keys[name] }, base, p, "300")
	if err != nil {
		t.Fatal(err)
	}
	cleanTenantState(t, registry)
	var logs bytes.Buffer
	obs := newTelemetry(&logs)
	mux := newTenantMux(p, "admin-test-key", registry, obs)
	first := tenantCall(mux, "alice-test-key", validChatBody, true)
	hit := tenantCall(mux, "alice-test-key", validChatBody, true)
	blocked := tenantCall(mux, "alice-test-key", validChatBody, true)
	bob := tenantCall(mux, "bob-test-key", validChatBody, true)
	if first.Code != 200 || hit.Header().Get("X-Janus-Cache") != "HIT" || blocked.Code != 429 || bob.Header().Get("X-Janus-Cache") != "MISS" || calls.Load() != 2 {
		t.Fatalf("tenant cache/RPM isolation failed: %d %d %d %d, calls=%d", first.Code, hit.Code, blocked.Code, bob.Code, calls.Load())
	}
	alice := registry.byKey[sha256.Sum256([]byte("alice-test-key"))]
	// Rotation changes only authentication, preserving tenant state.
	keys["ALICE_JANUS_API_KEY"] = "rotated-alice-key"
	rotated, err := buildTenantRegistry(configs, func(name string) string { return keys[name] }, base, p, "300")
	if err != nil {
		t.Fatal(err)
	}
	newAlice := rotated.byKey[sha256.Sum256([]byte("rotated-alice-key"))]
	if newAlice.limiter.key != alice.limiter.key || newAlice.cache.scope != alice.cache.scope {
		t.Fatal("rotation reset quota/cache identity")
	}
	rotatedMux := newTenantMux(p, "admin-test-key", rotated, obs)
	if tenantCall(rotatedMux, "alice-test-key", validChatBody, false).Code != 401 || tenantCall(rotatedMux, "rotated-alice-key", validChatBody, true).Code != 429 {
		t.Fatal("old key accepted or rotated key reset exhausted RPM")
	}
	// Alice's TPM exhaustion cannot block Bob's independent ledger.
	charge, err := alice.limiter.ReserveTokens(context.Background(), 950)
	if err != nil || !charge.allowed {
		t.Fatal(err)
	}
	if err := alice.limiter.Settle(context.Background(), charge.reservation, 1200); err != nil {
		t.Fatal(err)
	}
	bobFresh := tenantCall(mux, "bob-test-key", strings.Replace(validChatBody, "Hello", "different", 1), false)
	if bobFresh.Code != 200 {
		t.Fatal("Alice TPM debt blocked Bob")
	}
	obs.close()
	decoder := json.NewDecoder(&logs)
	seen := make(map[string]bool)
	for {
		var event requestEvent
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if event.Status == 401 && event.TenantID != "" {
			t.Fatal("untrusted identity logged on rejected credentials")
		}
		seen[event.TenantID] = true
	}
	if !seen[configs[0].ID] || !seen[configs[1].ID] {
		t.Fatal("authenticated tenant missing from request logs")
	}
}

func TestRedisTenantAuthenticationAndOutputLimits(t *testing.T) {
	base := testQuota(t, "60", 60000)
	id := fmt.Sprintf("auth-test-%d", time.Now().UnixNano())
	configs := []tenantConfig{tenantTestConfig(id, "TEST_JANUS_API_KEY", 60)}
	p, _ := newProvider("http://localhost", "provider-key")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	registry, err := buildTenantRegistry(configs, func(string) string { return "tenant-key" }, base, p, "0")
	if err != nil {
		t.Fatal(err)
	}
	cleanTenantState(t, registry)
	obs := newTelemetry(nil)
	mux := newTenantMux(p, "admin-key", registry, obs)
	tooLarge := strings.TrimSuffix(validChatBody, "}") + `,"max_completion_tokens":129}`
	if tenantCall(mux, "tenant-key", tooLarge, false).Code != 400 {
		t.Fatal("tenant output cap ignored")
	}
	for _, key := range []string{"wrong", "admin-key", "tenant-key extra"} {
		if tenantCall(mux, key, validChatBody, false).Code != 401 {
			t.Fatal("invalid or admin-only chat key accepted")
		}
	}
	for _, key := range []string{"tenant-key", "admin-key"} {
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		want := 401
		if key == "admin-key" {
			want = 200
		}
		if w.Code != want {
			t.Fatal("metrics not restricted to admin key")
		}
	}
	// The first valid request still has every RPM permit except its own.
	valid := tenantCall(mux, "tenant-key", validChatBody, false)
	if valid.Code != 200 || valid.Header().Get("X-RateLimit-Remaining") != "59" {
		t.Fatal("rejected authentication or validation spent quota")
	}
	*configs[0].Enabled = false
	disabled, err := buildTenantRegistry(configs, func(string) string { return "tenant-key" }, base, p, "0")
	if err != nil {
		t.Fatal(err)
	}
	if tenantCall(newTenantMux(p, "admin-key", disabled, obs), "tenant-key", validChatBody, false).Code != 401 {
		t.Fatal("disabled tenant allowed access")
	}
}

func TestRedisTenantStreamingAndFallbackAccounting(t *testing.T) {
	base := testQuota(t, "60", 60000)
	id := fmt.Sprintf("stream-test-%d", time.Now().UnixNano())
	configs := []tenantConfig{tenantTestConfig(id+"-alice", "ALICE_JANUS_API_KEY", 60), tenantTestConfig(id+"-bob", "BOB_JANUS_API_KEY", 60)}
	keys := map[string]string{"ALICE_JANUS_API_KEY": "alice-key", "BOB_JANUS_API_KEY": "bob-key"}
	var fail atomic.Bool
	p, _ := newProvider("http://localhost", "provider-key")
	p.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		if fail.Load() {
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: ioBody(`{}`)}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: ioBody("data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":30,\"total_tokens\":50}}\n\ndata: [DONE]\n\n")}, nil
	})
	if err := p.configureFallback("http://localhost", "fallback-key", "demo-model"); err != nil {
		t.Fatal(err)
	}
	p.fallback.client.Transport = breakerTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: ioBody(cacheAnswer)}, nil
	})
	registry, err := buildTenantRegistry(configs, func(name string) string { return keys[name] }, base, p, "300")
	if err != nil {
		t.Fatal(err)
	}
	cleanTenantState(t, registry)
	mux := newTenantMux(p, "admin-key", registry, newTelemetry(nil))
	stream := tenantCall(mux, "alice-key", validStreamBody, true)
	if stream.Code != 200 || stream.Header().Get("X-Janus-Cache") != "BYPASS" || !strings.Contains(stream.Body.String(), "[DONE]") || !stream.Flushed {
		t.Fatal("tenant stream was not forwarded and flushed")
	}
	alice := registry.byKey[sha256.Sum256([]byte("alice-key"))]
	bob := registry.byKey[sha256.Sum256([]byte("bob-key"))]
	aliceTokens, err := base.client.Get(context.Background(), alice.limiter.quotaKeys()[3]).Int64()
	if err != nil || aliceTokens != 50 {
		t.Fatal("stream usage not settled against Alice")
	}
	fail.Store(true)
	fallback := tenantCall(mux, "bob-key", validChatBody, true)
	bobTokens, err := base.client.Get(context.Background(), bob.limiter.quotaKeys()[3]).Int64()
	if fallback.Code != 200 || fallback.Header().Get("X-Janus-Route") != "fallback" || err != nil || bobTokens <= 50 {
		t.Fatal("fallback did not account both attempts against Bob")
	}
	unchanged, err := base.client.Get(context.Background(), alice.limiter.quotaKeys()[3]).Int64()
	if err != nil || unchanged != aliceTokens {
		t.Fatal("Bob fallback changed Alice usage")
	}
}
