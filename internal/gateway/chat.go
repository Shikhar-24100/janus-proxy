package gateway

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
	Model               string         `json:"model"`
	Messages            []Message      `json:"messages"`
	Stream              bool           `json:"stream"`
	MaxCompletionTokens *int           `json:"max_completion_tokens,omitempty"`
	StreamOptions       *StreamOptions `json:"stream_options,omitempty"`
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (p *Provider) chatHandler(w http.ResponseWriter, r *http.Request) {
	if state := accountingFrom(r); state != nil {
		p.forwardChat(w, r, state.input)
		return
	}
	input, ok := decodeChatRequest(w, r, 1024)
	if ok {
		p.forwardChat(w, r, input)
	}
}

func decodeChatRequest(w http.ResponseWriter, r *http.Request, outputLimit int) (ChatRequest, bool) {
	// Limit the body to 1 MiB before parsing client-controlled JSON.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	// Reject unsupported fields instead of silently ignoring them.
	decoder.DisallowUnknownFields()

	var request ChatRequest
	if err := decoder.Decode(&request); err != nil {
		writeDecodeError(w, err)
		return ChatRequest{}, false
	}

	// A request must contain exactly one JSON value.
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			writeRequestError(w, http.StatusBadRequest, "Body must contain one JSON object.")
		} else {
			writeDecodeError(w, err)
		}
		return ChatRequest{}, false
	}

	if strings.TrimSpace(request.Model) == "" {
		writeRequestError(w, http.StatusBadRequest, "model is required.")
		return ChatRequest{}, false
	}
	if len(request.Messages) == 0 {
		writeRequestError(w, http.StatusBadRequest, "messages must contain at least one message.")
		return ChatRequest{}, false
	}
	for _, message := range request.Messages {
		if message.Role != "system" && message.Role != "user" && message.Role != "assistant" {
			writeRequestError(w, http.StatusBadRequest, "Each role must be system, user, or assistant.")
			return ChatRequest{}, false
		}
		if strings.TrimSpace(message.Content) == "" {
			writeRequestError(w, http.StatusBadRequest, "Each message must have non-empty string content.")
			return ChatRequest{}, false
		}
	}
	if request.MaxCompletionTokens == nil {
		request.MaxCompletionTokens = &outputLimit
	}
	if *request.MaxCompletionTokens < 1 || *request.MaxCompletionTokens > outputLimit {
		writeRequestError(w, 400, "max_completion_tokens must be positive and no greater than MAX_OUTPUT_TOKENS.")
		return ChatRequest{}, false
	}
	if request.Stream {
		request.StreamOptions = &StreamOptions{IncludeUsage: true}
	} else if request.StreamOptions != nil {
		writeRequestError(w, 400, "stream_options is only supported with stream=true.")
		return ChatRequest{}, false
	}
	return request, true
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
