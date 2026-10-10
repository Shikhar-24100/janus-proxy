package gateway

import (
	"io"
	"log"
	"mime"
	"net/http"
	"sync"
	"time"
)

var streamBuffers = sync.Pool{New: func() any { return new([32 * 1024]byte) }}

func (p *Provider) forwardStream(w http.ResponseWriter, r *http.Request, response *http.Response, outcome *breakerOutcome) {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		*outcome = breakerFailure
		writeGatewayError(w, http.StatusBadGateway, "Provider did not return an SSE stream.")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeGatewayError(w, http.StatusInternalServerError, "Response writer does not support streaming.")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if trace := traceFrom(r); trace != nil {
		trace.streamStarted = true
	}
	flusher.Flush()

	// A network read can contain part of an SSE event or several events.
	// Forward the bytes unchanged; the client assembles complete events.
	storage := streamBuffers.Get().(*[32 * 1024]byte)
	defer func() {
		// Return on completion, cancellation or abort; erase the previous answer.
		clear(storage[:])
		streamBuffers.Put(storage)
	}()
	buffer := storage[:]
	observer := usageObserver{}
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			observer.Feed(buffer[:n])
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				abortStream(r, "Could not write stream to client")
			}
			flusher.Flush()
			if trace := traceFrom(r); trace != nil && trace.ttft == nil && observer.hasText {
				elapsed := float64(time.Since(trace.started)) / float64(time.Millisecond)
				trace.ttft = &elapsed
			}
		}
		if readErr == io.EOF {
			if observer.done && !observer.disabled {
				*outcome = breakerSuccess
				if trace := traceFrom(r); trace != nil {
					trace.streamComplete = true
				}
			} else {
				*outcome = breakerFailure
			}
			if state := accountingFrom(r); state != nil && observer.done && !observer.disabled {
				state.actual = observer.actual
				state.usage = observer.usage
			}
			return
		}
		if readErr != nil {
			*outcome = breakerFailure
			abortStream(r, "Provider stream interrupted")
		}
	}
}

func abortStream(r *http.Request, message string) {
	if r.Context().Err() == nil {
		log.Println(message)
	}
	// Headers are already sent: abort the connection instead of appending JSON
	// or a fake [DONE] event. net/http handles this sentinel without a stack trace.
	panic(http.ErrAbortHandler)
}
