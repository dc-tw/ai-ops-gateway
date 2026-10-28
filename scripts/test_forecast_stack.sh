#!/bin/sh
set -eu

compose() {
  docker compose -p ai-ops-forecast-test -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.acl-test.yml -f docker-compose.skills-v2-test.yml \
    -f docker-compose.forecast-test.yml "$@"
}
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT INT TERM
compose config --quiet
compose build gateway browser mock-llm
compose up --detach --wait redis mock-llm browser gateway worker
compose exec -T gateway python < scripts/verify_forecast.py
echo "M5 shared model publication, per-call telemetry and plan-only forecast passed"
