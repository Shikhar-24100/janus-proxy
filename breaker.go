package main

import (
	"sync"
	"time"
)

type breakerState uint8

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type breakerOutcome uint8

const (
	breakerNeutral breakerOutcome = iota // Client cancellation or local failure.
	breakerSuccess
	breakerFailure
)

// Each Provider owns one breaker. The lock protects transitions, never HTTP I/O.
type circuitBreaker struct {
	mu         sync.Mutex
	state      breakerState
	failures   int
	threshold  int
	cooldown   time.Duration
	openUntil  time.Time
	generation uint64
	now        func() time.Time
}

func newCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{threshold: 5, cooldown: 30 * time.Second, now: time.Now}
}

// A generation identifies the state in which a call was admitted. Results from
// older calls cannot close a breaker that has since opened or started probing.
func (b *circuitBreaker) acquire() (generation uint64, allowed bool, retry time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerOpen {
		if remaining := b.openUntil.Sub(b.now()); remaining > 0 {
			return 0, false, remaining
		}
		b.state = breakerHalfOpen
		b.generation++
		return b.generation, true, 0
	}
	if b.state == breakerHalfOpen {
		return 0, false, time.Second // Probe still running; recovery time is unknown.
	}
	return b.generation, true, 0
}

func (b *circuitBreaker) finish(generation uint64, outcome breakerOutcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.generation {
		return
	}
	if b.state == breakerHalfOpen {
		if outcome == breakerSuccess {
			b.state = breakerClosed
			b.failures = 0
			b.generation++
		} else {
			// A canceled probe cannot prove recovery. Release its slot and wait
			// another cooldown without counting cancellation as a provider failure.
			b.open()
		}
		return
	}
	if b.state != breakerClosed {
		return
	}
	switch outcome {
	case breakerSuccess:
		b.failures = 0
	case breakerFailure:
		b.failures++
		if b.failures >= b.threshold {
			b.open()
		}
	}
}

// Caller holds mu.
func (b *circuitBreaker) open() {
	b.state = breakerOpen
	b.openUntil = b.now().Add(b.cooldown)
	b.generation++
}
