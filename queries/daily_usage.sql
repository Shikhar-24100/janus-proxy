-- UTC reporting. Preaggregate attempts to avoid counting a fallback request twice.
WITH per_request AS (
    SELECT request_id,
        SUM(prompt_tokens) AS prompt_tokens,
        SUM(completion_tokens) AS completion_tokens,
        SUM(total_tokens) AS total_tokens,
        COUNT(*) FILTER (WHERE total_tokens IS NULL AND outcome <> 'skipped') AS unknown_attempts
    FROM janus_usage_attempts
    GROUP BY request_id
)
SELECT (r.finished_at AT TIME ZONE 'UTC')::date AS day_utc,
    r.tenant_id,
    COUNT(*) AS requests,
    COUNT(*) FILTER (WHERE r.cache_result = 'HIT') AS cache_hits,
    COALESCE(SUM(a.prompt_tokens), 0) AS known_prompt_tokens,
    COALESCE(SUM(a.completion_tokens), 0) AS known_completion_tokens,
    COALESCE(SUM(a.total_tokens), 0) AS known_total_tokens,
    COALESCE(SUM(a.unknown_attempts), 0) AS unknown_provider_attempts
FROM janus_usage_requests r
LEFT JOIN per_request a USING (request_id)
WHERE r.finished_at >= now() - interval '7 days'
GROUP BY day_utc, r.tenant_id
ORDER BY day_utc DESC, r.tenant_id;
