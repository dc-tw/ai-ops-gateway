#!/bin/sh
set -eu

# Explicit disposable project; never point this script at a production stack.
POSTGRES_TEST_PROJECT=ai-ops-acl-test
POSTGRES_TEST_PORT=18084
POSTGRES_TEST_HISTORY_MODE=postgres
export POSTGRES_TEST_PROJECT POSTGRES_TEST_PORT POSTGRES_TEST_HISTORY_MODE
compose() {
  docker compose -p ai-ops-acl-test -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.postgres.yml -f docker-compose.postgres-test.yml \
    -f docker-compose.acl-test.yml "$@"
}
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT INT TERM
compose config --quiet
compose build gateway browser
compose up --detach --wait --scale postgres-projector=2 \
  redis postgres mock-llm browser gateway worker scheduler postgres-projector
compose exec -T -e ACL_TEST_PHASE=prepare gateway python < scripts/verify_resource_access.py
compose exec -T gateway python -m app.postgres_worker backfill
compose exec -T gateway python -m app.postgres_worker reconcile
compose exec -T -e ACL_TEST_PHASE=verify gateway python < scripts/verify_resource_access.py
compose stop redis
compose exec -T -e ACL_TEST_PHASE=redis-outage gateway python < scripts/verify_resource_access.py
echo "Strict ACL authenticated HTTP, real PostgreSQL history, revoke and Redis outage passed"
