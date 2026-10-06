package main

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
)

const defaultMaxInflight = 32

type admissionGate struct {
	slots    chan struct{}
	active   atomic.Int64
	rejected atomic.Uint64
}

func newAdmissionGate(setting string) (*admissionGate, error) {
	limit := defaultMaxInflight
	if setting != "" {
		n, err := strconv.Atoi(setting)
		if err != nil || n < 1 || n > 4096 {
			return nil, errors.New("MAX_INFLIGHT must be between 1 and 4096")
		}
		limit = n
	}
	return &admissionGate{slots: make(chan struct{}, limit)}, nil
}

func (t *telemetry) admit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate := t.admission
		reject := func() {
			gate.rejected.Add(1)
			if trace := traceFrom(r); trace != nil {
				trace.overloaded = true
			}
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]string{"type": "overloaded_error", "message": "Janus is busy or usage delivery is recovering. Retry later."}})
		}
		if t.usage != nil && t.usage.pending.Load() > 0 {
			reject()
			return
		}
		select {
		case gate.slots <- struct{}{}:
		default:
			reject()
			return
		}
		gate.active.Add(1)
		var once sync.Once
		release := func() { once.Do(func() { gate.active.Add(-1); <-gate.slots }) }
		if trace := traceFrom(r); trace != nil {
			// The outer observer releases after usage confirmation, including retries.
			trace.release = release
		} else {
			defer release()
		}
		// A concurrent failed handoff may have closed admission during acquisition.
		if t.usage != nil && t.usage.pending.Load() > 0 {
			release()
			reject()
			return
		}
		next.ServeHTTP(w, r)
	})
}
