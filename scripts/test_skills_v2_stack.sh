#!/bin/sh
set -eu

POSTGRES_TEST_PROJECT=ai-ops-skills-v2-test
POSTGRES_TEST_PORT=18085
export POSTGRES_TEST_PROJECT POSTGRES_TEST_PORT
compose() {
  docker compose -p ai-ops-skills-v2-test -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.postgres.yml -f docker-compose.postgres-test.yml \
    -f docker-compose.acl-test.yml -f docker-compose.skills-v2-test.yml "$@"
}
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT INT TERM
compose config --quiet
compose build gateway browser
compose up --detach --wait redis postgres mock-llm browser gateway worker scheduler
compose exec -T gateway python < scripts/verify_skills_v2.py
echo "Skill v2 modes, complete resource context and pinned automatic selections passed"
