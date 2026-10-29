#!/bin/sh
set -eu

POSTGRES_TEST_PROJECT="ai-ops-m9-test-$$"
export POSTGRES_TEST_PROJECT
REPORT_DIR="${M9_REPORT_DIR:-/tmp/ai-ops-m9-report-$$}"
mkdir -p "${REPORT_DIR}"

compose() {
  docker compose -p "${POSTGRES_TEST_PROJECT}" \
    -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.postgres.yml -f docker-compose.postgres-test.yml \
    -f docker-compose.staging-test.yml "$@"
}

cleanup() {
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    compose down --volumes --remove-orphans
  fi
}
trap cleanup EXIT INT TERM

verify() {
  phase="$1"
  minimum="${2:-2}"
  temporary="${REPORT_DIR}/${phase}.tmp"
  rm -f "${temporary}"
  compose exec -T -e "M9_PHASE=${phase}" -e "M9_MIN_REPLICAS=${minimum}" \
    m9-runner python < scripts/verify_m9_staging.py > "${temporary}"
  mv "${temporary}" "${REPORT_DIR}/${phase}.json"
  sed -n '1,20p' "${REPORT_DIR}/${phase}.json"
}

compose config --quiet
compose build gateway browser mock-llm
compose up --detach --wait --scale gateway=2 --scale worker=2 --scale scheduler=2 \
  --scale postgres-projector=2 \
  redis postgres mock-llm browser gateway worker scheduler postgres-projector m9-runner

verify load
docker stats --no-stream --format '{{json .}}' \
  $(compose ps -q gateway worker scheduler postgres-projector redis postgres) \
  > "${REPORT_DIR}/docker-stats.jsonl"

# Kill exactly one disposable Gateway replica; the second must continue serving.
victim="$(compose ps -q gateway | sed -n '1p')"
test -n "${victim}"
docker kill --signal KILL "${victim}" >/dev/null
verify gateway-sigkill 1
compose up --detach --wait --scale gateway=2 gateway
verify gateway-restored

# Redis remains execution authority; readiness must fail closed during outage.
compose stop redis
verify redis-down
redis_started="$(date +%s)"
compose up --detach --wait redis
verify redis-recovered
redis_finished="$(date +%s)"
printf '{"redis_recovery_wall_seconds":%s}\n' "$((redis_finished - redis_started))" \
  > "${REPORT_DIR}/redis-recovery-wall.json"

# PostgreSQL is the history plane in shadow mode: execution continues, outbox
# retains writes, and both projectors must converge after recovery.
compose stop postgres
verify postgres-down
postgres_started="$(date +%s)"
compose up --detach --wait postgres
compose up --detach --wait --scale postgres-projector=2 postgres-projector
verify projector-recovered
postgres_finished="$(date +%s)"
printf '{"postgres_recovery_wall_seconds":%s}\n' "$((postgres_finished - postgres_started))" \
  > "${REPORT_DIR}/postgres-recovery-wall.json"

echo "M9 multi-replica load, exact SIGKILL, Redis fail-closed and PostgreSQL catch-up passed"
echo "M9 report directory: ${REPORT_DIR}"
