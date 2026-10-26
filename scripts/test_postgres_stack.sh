#!/bin/sh
set -eu

# A private project, dedicated images/volumes and loopback-only API port. All
# credentials below are public, disposable test values, not production secrets.
POSTGRES_TEST_PROJECT="${POSTGRES_TEST_PROJECT:-ai-ops-postgres-test}"
export POSTGRES_TEST_PROJECT
BACKUP_FILE="$(mktemp)"
SOURCE_DIGEST="$(mktemp)"
RESTORE_DIGEST="$(mktemp)"

compose() {
  docker compose -p "${POSTGRES_TEST_PROJECT}" \
    -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.postgres.yml -f docker-compose.postgres-test.yml "$@"
}

cleanup() {
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    compose down --volumes --remove-orphans
  fi
  rm -f "${BACKUP_FILE}" "${SOURCE_DIGEST}" "${RESTORE_DIGEST}"
}
trap cleanup EXIT INT TERM

compose config --quiet
compose build gateway browser
compose up --detach --wait --scale postgres-projector=2 \
  redis postgres mock-llm browser gateway worker scheduler postgres-projector
compose exec -T -e POSTGRES_TEST_PHASE=prepare gateway python < scripts/verify_postgres.py
compose stop postgres
compose exec -T -e POSTGRES_TEST_PHASE=outage-write gateway python < scripts/verify_postgres.py
compose up --detach --wait postgres

# These commands fence participating Redis writers while collecting a stable
# migration/reconciliation boundary; the projector can continue safely.
compose exec -T gateway python -m app.postgres_worker backfill
compose exec -T gateway python -m app.postgres_worker reconcile

POSTGRES_TEST_HISTORY_MODE=postgres compose up --detach --force-recreate --wait gateway
compose exec -T -e POSTGRES_TEST_PHASE=verify gateway python < scripts/verify_postgres.py
compose stop postgres
compose exec -T -e POSTGRES_TEST_PHASE=outage-read gateway python < scripts/verify_postgres.py
compose up --detach --wait postgres
compose exec -T -e POSTGRES_TEST_PHASE=verify gateway python < scripts/verify_postgres.py

compose stop redis
compose exec -T -e POSTGRES_TEST_PHASE=redis-outage gateway python < scripts/verify_postgres.py
compose up --detach --wait redis

# Stop this disposable stack's writers so dump/restore digests compare a stable
# database. Redis is deliberately kept: this tests restore, not migration from an
# empty execution store or PostgreSQL becoming execution authority.
compose stop gateway worker scheduler postgres-projector
compose exec -T postgres pg_dump -U gateway -d gateway --format=custom > "${BACKUP_FILE}"
test -s "${BACKUP_FILE}"
compose exec -T postgres createdb -U gateway gateway_restore
compose exec -T postgres pg_restore --exit-on-error -U gateway -d gateway_restore < "${BACKUP_FILE}"

digest() {
  compose exec -T postgres psql -X -q -A -t -v ON_ERROR_STOP=1 -U gateway -d "$1" <<'SQL'
SELECT format(
  'SELECT %L, count(*), md5(coalesce(string_agg(md5(to_jsonb(t)::text), %L ORDER BY md5(to_jsonb(t)::text)), %L)) FROM %I.%I t;',
  tablename, '', '', schemaname, tablename
)
FROM pg_tables
WHERE schemaname = 'public' AND left(tablename, 8) = 'gateway_'
ORDER BY tablename
\gexec
SQL
}

digest gateway > "${SOURCE_DIGEST}"
digest gateway_restore > "${RESTORE_DIGEST}"
test -s "${SOURCE_DIGEST}"
diff -u "${SOURCE_DIGEST}" "${RESTORE_DIGEST}"
echo "PostgreSQL HTTP history, two projectors, backfill/reconcile and real pg_dump/pg_restore passed"
