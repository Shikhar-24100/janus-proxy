package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	janusKey := os.Getenv("JANUS_API_KEY")
	if janusKey == "" || strings.ContainsAny(janusKey, " \t\r\n") {
		log.Fatal("JANUS_API_KEY must be configured and contain no whitespace")
	}

	provider, err := newProvider(os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	if provider.apiKey == "" {
		log.Println("OPENAI_API_KEY is unset; primary provider is unavailable")
	}
	if err := provider.configureFallback(os.Getenv("FALLBACK_BASE_URL"), os.Getenv("FALLBACK_API_KEY"), os.Getenv("FALLBACK_MODEL")); err != nil {
		log.Fatal(err)
	}

	limiter, err := newRateLimiter(os.Getenv("REDIS_URL"), os.Getenv("RPM_LIMIT"), janusKey)
	if err != nil {
		log.Fatal(err)
	}
	defer limiter.client.Close()
	for name, target := range map[string]*int{"TPM_LIMIT": &limiter.tpm, "MAX_OUTPUT_TOKENS": &limiter.outputLimit} {
		if setting := os.Getenv(name); setting != "" {
			value, err := strconv.Atoi(setting)
			if err != nil || value < 1 || value > 100000000 {
				log.Fatalf("%s must be a positive integer up to 100000000", name)
			}
			*target = value
		}
	}
	if limiter.outputLimit >= limiter.tpm {
		log.Fatal("MAX_OUTPUT_TOKENS must be less than TPM_LIMIT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = limiter.client.Ping(ctx).Err()
	cancel()
	if err != nil {
		log.Fatal("Cannot connect to Redis; check REDIS_URL and start Redis")
	}
	enabled := true
	configs := []tenantConfig{{ID: "default", APIKeyEnv: "JANUS_API_KEY", RPM: limiter.rpm, TPM: limiter.tpm, MaxOutputTokens: limiter.outputLimit, Enabled: &enabled}}
	if path := os.Getenv("TENANTS_CONFIG"); path != "" {
		configs, err = readTenantConfigs(path)
		if err != nil {
			log.Fatal(err)
		}
	}
	registry, err := buildTenantRegistry(configs, os.Getenv, limiter, provider, os.Getenv("CACHE_TTL_SECONDS"))
	if err != nil {
		log.Fatal(err)
	}
	observability := newTelemetry(os.Stderr)
	observability.admission, err = newAdmissionGate(os.Getenv("MAX_INFLIGHT"))
	if err != nil {
		log.Fatal(err)
	}
	databaseURL, queueURL := os.Getenv("DATABASE_URL"), os.Getenv("USAGE_REDIS_URL")
	if (databaseURL == "") != (queueURL == "") {
		log.Fatal("Set both DATABASE_URL and USAGE_REDIS_URL, or leave both empty to disable usage storage")
	}
	if databaseURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err := newPostgresUsageStore(ctx, databaseURL)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		defer store.pool.Close()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		pipeline, err := newUsagePipeline(ctx, queueURL, store)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		observability.usage = pipeline
		pipeline.retries = make(chan usageRetry, cap(observability.admission.slots))
		pipeline.start()
		defer pipeline.close()
	}
	mux := newTenantMux(provider, janusKey, registry, observability)

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-stopCtx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if server.Shutdown(ctx) != nil {
			_ = server.Close()
		}
	}()
	log.Printf("Janus proxy listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	stop()
	<-shutdownDone
	// Accepted usage events remain in Redis even if the worker stops before draining.
}

func newMux(provider *Provider, janusKey string, limiter requestLimiter) *http.ServeMux {
	return newMuxWithTelemetry(provider, janusKey, limiter, newTelemetry(nil))
}

func newMuxWithTelemetry(provider *Provider, janusKey string, limiter requestLimiter, observability *telemetry) *http.ServeMux {
	mux := newOperationalMux(provider, janusKey, observability)
	var cache *responseCache
	if provider != nil {
		cache = provider.cache
	}
	chat := limitRequests(limiter, http.HandlerFunc(provider.chatHandler), cache)
	mux.Handle("POST /v1/chat/completions", observability.observe(requireAPIKey(janusKey, observability.admit(chat))))
	return mux
}

func newOperationalMux(provider *Provider, janusKey string, observability *telemetry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if provider != nil {
		observability.breakers["primary"] = provider.breaker
		if provider.fallback != nil {
			observability.breakers["fallback"] = provider.fallback.breaker
		}
	}
	mux.Handle("GET /metrics", requireAPIKey(janusKey, http.HandlerFunc(observability.serveMetrics)))
	return mux
}

// r->info the client sent
// w->way tio send the response back from server to the client
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
