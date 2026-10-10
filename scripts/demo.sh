#!/bin/sh
set -eu
# These scenarios use local fake providers and isolated Redis/PostgreSQL records.
# Missing services must fail the demo rather than silently skip integrations.
: "${REDIS_TEST_URL:?Set REDIS_TEST_URL for the demo}"
: "${POSTGRES_TEST_URL:?Set POSTGRES_TEST_URL for the demo}"
go test -p 1 -count=1 -timeout 2m -v ./internal/gateway \
  -run '^(TestAuthenticationRunsBeforeRateLimit|TestStreamArrivesBeforeProviderFinishes|TestRedisHTTPAdmission|TestRedisCacheHitAndExpiry|TestFallbackRouting|TestTelemetryFallbackUsageAndPrivacy|TestRedisTenantIsolationRotationAndLogs|TestPostgresUsageDeduplicationAndDailyReport|TestOutboxHardCrashRecovery)$'
