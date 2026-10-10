package gateway

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

// Deliberately conservative heuristic: one token per UTF-8 byte, plus message
// overhead. This is NOT a model tokenizer and can still underestimate framing.
func estimateInputTokens(input ChatRequest) int64 {
	total := int64(32)
	for _, message := range input.Messages {
		total += int64(len(message.Content) + len(message.Role) + 16)
	}
	return total
}

type accountingKey struct{}
type requestAccounting struct {
	input          ChatRequest
	attempted      bool
	actual         *int64
	usage          *tokenUsage
	limiter        requestLimiter
	reservation    string
	reserved       int64
	settled        bool
	cacheCandidate *cacheCandidate
}

func (s *requestAccounting) settle() {
	if s.settled {
		return
	}
	s.settled = true
	actual := s.actual
	if !s.attempted {
		zero := int64(0)
		actual = &zero
	}
	if actual == nil {
		log.Println("Provider usage unavailable; retaining token reservation")
		return
	}
	if err := s.limiter.Settle(context.Background(), s.reservation, *actual); err != nil {
		log.Println("Token reconciliation failed; reservation remains conservative")
	}
}

func accountingFrom(r *http.Request) *requestAccounting {
	state, _ := r.Context().Value(accountingKey{}).(*requestAccounting)
	return state
}

type tokenUsage struct {
	Prompt     *int64 `json:"prompt_tokens"`
	Completion *int64 `json:"completion_tokens"`
	Total      *int64 `json:"total_tokens"`
}

func reportedTokens(data []byte) *int64 {
	usage := reportedUsage(data)
	if usage == nil {
		return nil
	}
	return usage.Total
}

type providerEnvelope struct {
	Usage *tokenUsage `json:"usage"`
	Groq  struct {
		Usage *tokenUsage `json:"usage"`
	} `json:"x_groq"`
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

func reportedUsage(data []byte) *tokenUsage {
	var envelope providerEnvelope
	if json.Unmarshal(data, &envelope) != nil {
		return nil
	}
	return validUsage(envelope)
}

func validUsage(envelope providerEnvelope) *tokenUsage {
	usage := envelope.Usage
	if usage == nil {
		usage = envelope.Groq.Usage
	}
	if usage == nil || usage.Prompt == nil || usage.Completion == nil || usage.Total == nil {
		return nil
	}
	if *usage.Prompt < 0 || *usage.Completion < 0 || *usage.Total < 0 || *usage.Total > 1_000_000_000 {
		return nil
	}
	if *usage.Prompt > *usage.Total || *usage.Completion > *usage.Total || *usage.Prompt+*usage.Completion != *usage.Total {
		return nil
	}
	return usage
}

// Observe SSE alongside forwarding. Parsing never changes the bytes sent to
// the client. Bound memory, support CR/LF/CRLF and multi-line data fields.
type usageObserver struct {
	line     []byte
	data     []string
	size     int
	lastCR   bool
	disabled bool
	done     bool
	actual   *int64
	usage    *tokenUsage
	hasText  bool
}

func (o *usageObserver) Feed(chunk []byte) {
	for _, b := range chunk {
		if o.disabled {
			return
		}
		if b == '\n' && o.lastCR {
			o.lastCR = false
			continue
		}
		o.lastCR = b == '\r'
		if b == '\n' || b == '\r' {
			o.endLine()
			continue
		}
		o.line = append(o.line, b)
		o.size++
		if o.size > 64*1024 {
			o.disabled = true
			o.actual = nil
			o.usage = nil
			o.line = nil
			o.data = nil
		}
	}
}

func (o *usageObserver) endLine() {
	if len(o.line) == 0 {
		data := strings.Join(o.data, "\n")
		if data == "[DONE]" {
			o.done = true
		}
		if !o.done {
			var envelope providerEnvelope
			if json.Unmarshal([]byte(data), &envelope) == nil {
				if usage := validUsage(envelope); usage != nil {
					o.usage, o.actual = usage, usage.Total
				}
				for _, choice := range envelope.Choices {
					if choice.Delta.Content != "" {
						o.hasText = true
					}
				}
			}
		}
		o.data = nil
		o.size = 0
	} else if strings.HasPrefix(string(o.line), "data:") {
		o.data = append(o.data, strings.TrimPrefix(string(o.line[5:]), " "))
	}
	o.line = o.line[:0]
}
