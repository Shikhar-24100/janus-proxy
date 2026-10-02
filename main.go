package main

import (
	"log"
	"net/http"
)

func main() {
	// A mux routes incoming requests to the handler for their path and method.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /v1/chat/completions", chatHandler)

	log.Println("Janus proxy listening on http://localhost:8080")
	if err := http.ListenAndServe("127.0.0.1:8080", mux); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
