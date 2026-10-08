package main

import (
	"context"
	"time"
)

// Fixed stages avoid tenant/model labels and include Redis connection-pool waits.
var stageNames = [...]string{"quota_admission", "quota_settlement", "usage_enqueue", "outbox_write", "usage_claim", "usage_read", "usage_store", "usage_ack"}

func (t *telemetry) observeStage(stage int, started time.Time) {
	if t == nil {
		return
	}
	elapsed := time.Since(started).Seconds()
	t.mu.Lock()
	t.stages[stage].observe(elapsed)
	t.mu.Unlock()
}

type timedLimiter struct {
	requestLimiter
	metrics *telemetry
}

func (l timedLimiter) Reserve(ctx context.Context, amount int64) (rateDecision, error) {
	started := time.Now()
	defer l.metrics.observeStage(0, started)
	return l.requestLimiter.Reserve(ctx, amount)
}

func (l timedLimiter) ReserveTokens(ctx context.Context, amount int64) (rateDecision, error) {
	started := time.Now()
	defer l.metrics.observeStage(0, started)
	return l.requestLimiter.ReserveTokens(ctx, amount)
}

func (l timedLimiter) Settle(ctx context.Context, id string, actual int64) error {
	started := time.Now()
	defer l.metrics.observeStage(1, started)
	return l.requestLimiter.Settle(ctx, id, actual)
}
