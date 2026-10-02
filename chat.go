package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
)

// Structs describe the JSON fields our first version supports.
// JSON tags connect Go field names to names in the request body.
type ChatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (p *Provider) chatHandler(w http.ResponseWriter, r *http.Request) {
	// Limit the body to 1 MiB before parsing client-controlled JSON.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	// Reject unsupported fields instead of silently ignoring them.
	decoder.DisallowUnknownFields()

	var request ChatRequest
	if err := decoder.Decode(&request); err != nil {
		writeDecodeError(w, err)
		return
	}

	// A request must contain exactly one JSON value.
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			writeRequestError(w, http.StatusBadRequest, "Body must contain one JSON object.")
		} else {
			writeDecodeError(w, err)
		}
		return
	}

	if strings.TrimSpace(request.Model) == "" {
		writeRequestError(w, http.StatusBadRequest, "model is required.")
		return
	}
	if len(request.Messages) == 0 {
		writeRequestError(w, http.StatusBadRequest, "messages must contain at least one message.")
		return
	}
	for _, message := range request.Messages {
		if message.Role != "system" && message.Role != "user" && message.Role != "assistant" {
			writeRequestError(w, http.StatusBadRequest, "Each role must be system, user, or assistant.")
			return
		}
		if strings.TrimSpace(message.Content) == "" {
			writeRequestError(w, http.StatusBadRequest, "Each message must have non-empty string content.")
			return
		}
	}
	if request.Stream {
		writeRequestError(w, http.StatusBadRequest, "Streaming is not implemented yet.")
		return
	}

	p.forwardChat(w, r, request)
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var sizeError *http.MaxBytesError
	if errors.As(err, &sizeError) {
		writeRequestError(w, http.StatusRequestEntityTooLarge, "Request body exceeds 1 MiB.")
		return
	}
	writeRequestError(w, http.StatusBadRequest, "Invalid JSON or unsupported request fields or types.")
}

func writeRequestError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "invalid_request_error",
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("Could not write JSON response: %v", err)
	}
}
