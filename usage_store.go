package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_usage.sql
var usageSchema string

type usageStore interface {
	Save(context.Context, requestEvent) error
}

type postgresUsageStore struct{ pool *pgxpool.Pool }

func newPostgresUsageStore(ctx context.Context, url string) (*postgresUsageStore, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("DATABASE_URL is invalid")
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("cannot configure usage database")
	}
	// One idempotent, transactional initial schema. Later versions need a migration runner.
	if _, err = pool.Exec(ctx, "BEGIN;\n"+usageSchema+"\nCOMMIT;"); err != nil {
		pool.Close()
		return nil, errors.New("cannot initialize usage database")
	}
	return &postgresUsageStore{pool: pool}, nil
}

func (store *postgresUsageStore) Save(ctx context.Context, event requestEvent) error {
	if err := validateUsageEvent(event); err != nil {
		return err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	result, err := tx.Exec(ctx, `INSERT INTO janus_usage_requests
		(request_id, tenant_id, finished_at, route, status, outcome, streaming, cache_result, duration_ms, ttft_ms)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (request_id) DO NOTHING`,
		event.RequestID, event.TenantID, event.Time, event.Route, event.Status, event.Outcome, event.Stream, event.Cache, event.Duration, event.TTFT)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		// A previous delivery committed the entire request and its attempts.
		return tx.Commit(ctx)
	}
	for index, attempt := range event.Attempts {
		var prompt, completion, total *int64
		if attempt.Usage != nil {
			prompt, completion, total = attempt.Usage.Prompt, attempt.Usage.Completion, attempt.Usage.Total
		}
		_, err = tx.Exec(ctx, `INSERT INTO janus_usage_attempts
			(request_id, attempt_index, route, outcome, upstream_status, duration_ms, prompt_tokens, completion_tokens, total_tokens)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, event.RequestID, index, attempt.Route, attempt.Outcome, attempt.Status, attempt.Duration, prompt, completion, total)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func validateUsageEvent(event requestEvent) error {
	finite := func(value float64) bool { return value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value) }
	if !tenantIDPattern.MatchString(event.TenantID) || event.RequestID == "" || len(event.RequestID) > 128 || event.Time.IsZero() || len(event.Attempts) > 2 || !finite(event.Duration) || (event.TTFT != nil && !finite(*event.TTFT)) {
		return errors.New("invalid usage event")
	}
	if event.Event != "chat_request" || !oneOf(event.Route, "none", "cache", "primary", "fallback") || !oneOf(event.Outcome, "success", "error", "interrupted", "canceled") || !oneOf(event.Cache, "HIT", "MISS", "BYPASS", "ERROR") || (event.Status != 0 && (event.Status < 100 || event.Status > 599)) {
		return errors.New("invalid usage event fields")
	}
	for _, attempt := range event.Attempts {
		if !oneOf(attempt.Route, "primary", "fallback") || !oneOf(attempt.Outcome, "success", "failure", "client_error", "neutral", "canceled", "skipped") || !finite(attempt.Duration) || attempt.Status < 0 || attempt.Status > 599 {
			return errors.New("invalid usage attempt")
		}
		if usage := attempt.Usage; usage != nil {
			if usage.Prompt == nil || usage.Completion == nil || usage.Total == nil || *usage.Prompt < 0 || *usage.Completion < 0 || *usage.Total < 0 || *usage.Prompt > 1_000_000_000 || *usage.Completion > 1_000_000_000 || *usage.Total > 1_000_000_000 || *usage.Total != *usage.Prompt+*usage.Completion {
				return errors.New("invalid reported usage")
			}
		}
	}
	return nil
}

func oneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func decodeUsageEvent(data string) (requestEvent, error) {
	var event requestEvent
	if len(data) > 16<<10 || json.Unmarshal([]byte(data), &event) != nil {
		return event, errors.New("invalid queued usage JSON")
	}
	return event, validateUsageEvent(event)
}
