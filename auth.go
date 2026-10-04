package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// Middleware wraps a handler and runs a check before allowing it to execute.
func requireAPIKey(apiKey string, next http.Handler) http.Handler {
	expectedHash := sha256.Sum256([]byte(apiKey))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedHash, ok := bearerFingerprint(r)
		// Compare fixed-size hashes without revealing where two keys differ.
		if !ok || strings.TrimSpace(apiKey) == "" || subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) != 1 {
			writeAuthError(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerFingerprint(r *http.Request) ([32]byte, bool) {
	headers := r.Header.Values("Authorization")
	if len(headers) != 1 {
		return [32]byte{}, false
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return [32]byte{}, false
	}
	return sha256.Sum256([]byte(parts[1])), true
}

func writeAuthError(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="janus"`)
	writeJSON(w, http.StatusUnauthorized, map[string]any{
		"error": map[string]string{
			"message": "A valid Janus API key is required.",
			"type":    "authentication_error",
		},
	})
}
