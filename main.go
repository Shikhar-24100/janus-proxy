package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	janusKey := os.Getenv("JANUS_API_KEY")
	if strings.TrimSpace(janusKey) == "" {
		log.Fatal("JANUS_API_KEY must be configured before starting Janus")
	}

	provider, err := newProvider(os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	if provider.apiKey == "" {
		log.Println("OPENAI_API_KEY is unset; chat requests will return 503")
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
	mux := newMux(provider, janusKey, limiter)

	log.Println("Janus proxy listening on http://localhost:8080")
	if err := http.ListenAndServe("127.0.0.1:8080", mux); err != nil {
		log.Fatal(err)
	}
}

func newMux(provider *Provider, janusKey string, limiter requestLimiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	chat := limitRequests(limiter, http.HandlerFunc(provider.chatHandler))
	mux.Handle("POST /v1/chat/completions", requireAPIKey(janusKey, chat))
	return mux
}

// r->info the client sent
// w->way tio send the response back from server to the client
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
