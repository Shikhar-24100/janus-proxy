package main

import (
	"log"
	"net/http"
	"os"
	"strings"
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

	mux := newMux(provider, janusKey)

	log.Println("Janus proxy listening on http://localhost:8080")
	if err := http.ListenAndServe("127.0.0.1:8080", mux); err != nil {
		log.Fatal(err)
	}
}

func newMux(provider *Provider, janusKey string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.Handle("POST /v1/chat/completions", requireAPIKey(janusKey, http.HandlerFunc(provider.chatHandler)))
	return mux
}

// r->info the client sent
// w->way tio send the response back from server to the client
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
