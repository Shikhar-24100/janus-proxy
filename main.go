package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	provider, err := newProvider(os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	if provider.apiKey == "" {
		log.Println("OPENAI_API_KEY is unset; chat requests will return 503")
	}

	// A mux routes incoming requests to the handler for their path and method.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /v1/chat/completions", provider.chatHandler)

	log.Println("Janus proxy listening on http://localhost:8080")
	if err := http.ListenAndServe("127.0.0.1:8080", mux); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
