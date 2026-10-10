CREATE TABLE IF NOT EXISTS janus_usage_requests (
    request_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    finished_at timestamptz NOT NULL,
    route text NOT NULL,
    status integer NOT NULL,
    outcome text NOT NULL,
    streaming boolean NOT NULL,
    cache_result text NOT NULL,
    duration_ms double precision NOT NULL CHECK (duration_ms >= 0),
    ttft_ms double precision CHECK (ttft_ms >= 0)
);
CREATE INDEX IF NOT EXISTS janus_usage_tenant_time
    ON janus_usage_requests (tenant_id, finished_at);

CREATE TABLE IF NOT EXISTS janus_usage_attempts (
    request_id text NOT NULL REFERENCES janus_usage_requests(request_id),
    attempt_index integer NOT NULL,
    route text NOT NULL,
    outcome text NOT NULL,
    upstream_status integer NOT NULL,
    duration_ms double precision NOT NULL CHECK (duration_ms >= 0),
    prompt_tokens bigint CHECK (prompt_tokens >= 0),
    completion_tokens bigint CHECK (completion_tokens >= 0),
    total_tokens bigint CHECK (total_tokens >= 0),
    PRIMARY KEY (request_id, attempt_index),
    CHECK ((prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL)
        OR (prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL AND total_tokens IS NOT NULL
            AND total_tokens = prompt_tokens + completion_tokens))
);
