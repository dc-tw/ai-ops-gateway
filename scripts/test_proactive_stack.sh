#!/bin/sh
set -eu

PROJECT_NAME=ai-ops-proactive-test
COMPOSE="docker compose -p ${PROJECT_NAME} -f docker-compose.yml -f docker-compose.test.yml"
cleanup() {
  if [ "${KEEP_STACK:-0}" != 1 ]; then
    ${COMPOSE} down --volumes --remove-orphans
  fi
}
trap cleanup EXIT INT TERM

${COMPOSE} up --detach --build --wait --scale worker=2 --scale scheduler=2
AUTOMATION_TEST_JOB_IDS_JSON="$(${COMPOSE} exec -T -e AUTOMATION_TEST_PHASE=prepare \
  gateway python < scripts/verify_automations.py)"
export AUTOMATION_TEST_JOB_IDS_JSON
AUTOMATION_WEBHOOK_SECRETS_JSON="$(python3 -c \
  'import json,os; ids=json.loads(os.environ["AUTOMATION_TEST_JOB_IDS_JSON"]); print(json.dumps({ids["hook"]:"automation-e2e-public-test-secret-20261025"}))')"
export AUTOMATION_WEBHOOK_SECRETS_JSON
${COMPOSE} up --detach --force-recreate --wait gateway
${COMPOSE} exec -T -e "AUTOMATION_TEST_JOB_IDS_JSON=${AUTOMATION_TEST_JOB_IDS_JSON}" \
  gateway python < scripts/verify_automations.py
AUTOMATION_TEST_CRASH_GOAL_ID="$(${COMPOSE} exec -T -e AUTOMATION_TEST_PHASE=crash-prepare \
  gateway python < scripts/verify_automations.py)"
${COMPOSE} kill -s SIGKILL worker
${COMPOSE} up --detach --wait --scale worker=2 worker
${COMPOSE} exec -T -e AUTOMATION_TEST_PHASE=crash-verify \
  -e "AUTOMATION_TEST_CRASH_GOAL_ID=${AUTOMATION_TEST_CRASH_GOAL_ID}" \
  gateway python < scripts/verify_automations.py
