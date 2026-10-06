#!/bin/sh
set -eu
cd "$1"
duration="$2"
rates="$3"
inflight="$4"
overload="$5"
mkdir -p .cache/loadtest
compose() { docker compose -p janus-loadtest -f compose.loadtest.yaml "$@"; }
# Reset only this fixture stack's processes. Its private database can retain rows.
compose down --timeout 15
compose build gateway fake-provider load cert
compose up -d --wait --wait-timeout 120 gateway
: > .cache/loadtest/resources.jsonl
sample() {
  while true; do
    stats=$(docker stats --no-stream --format '{{json .}}' janus-loadtest-gateway-1) || return
    backlog=$(docker exec janus-loadtest-usage-redis-1 redis-cli XLEN janus:usage:v1) || return
    printf '{"at":"%s","backlog":%s,"stats":%s}\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$backlog" "$stats" >> .cache/loadtest/resources.jsonl
    sleep 1
  done
}
sample &
sampler=$!
cleanup() { kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
status=0
compose run --rm load run --duration "$duration" --rates "$rates" --inflight "$inflight" --allow-overload="$overload" || status=$?
cleanup
trap - EXIT INT TERM
compose run --rm load summarize || status=$?
compose logs --no-color --tail 200 gateway postgres quota-redis usage-redis > .cache/loadtest/fixture.log 2>&1
docker inspect --format '{{json .State}}' janus-loadtest-gateway-1 > .cache/loadtest/gateway-state.json
compose down --timeout 15
exit "$status"
