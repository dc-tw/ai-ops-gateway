#!/bin/sh
set -eu

POSTGRES_TEST_PROJECT=ai-ops-workflow-v1-test
POSTGRES_TEST_PORT=18086
POSTGRES_TEST_HISTORY_MODE=postgres
if [ -z "${WORKFLOW_LEGACY_IMAGE:-}" ]; then
  if ! docker image inspect ai-ops-workflow-v1-legacy:7c6ebe3 >/dev/null 2>&1; then
    sh scripts/build_workflow_legacy_image.sh
  fi
  WORKFLOW_LEGACY_IMAGE=$(docker image inspect ai-ops-workflow-v1-legacy:7c6ebe3 --format '{{.Id}}')
fi
export POSTGRES_TEST_PROJECT POSTGRES_TEST_PORT POSTGRES_TEST_HISTORY_MODE WORKFLOW_LEGACY_IMAGE
compose() {
  docker compose -p ai-ops-workflow-v1-test -f docker-compose.yml -f docker-compose.test.yml \
    -f docker-compose.postgres.yml -f docker-compose.postgres-test.yml \
    -f docker-compose.acl-test.yml -f docker-compose.workflow-test.yml "$@"
}
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT INT TERM
compose config --quiet
# Fail if the selected legacy reference already contains the new protocol.
docker run --rm --network none --entrypoint python "$WORKFLOW_LEGACY_IMAGE" \
  -c 'import importlib.util; assert importlib.util.find_spec("app.core.goal_protocol") is None'
compose build gateway browser mock-llm
compose up --detach --wait redis postgres mock-llm browser gateway scheduler postgres-projector legacy-gateway legacy-worker legacy-scheduler
compose exec -T -e WORKFLOW_TEST_PHASE=prepare gateway python < scripts/verify_workflow_runtime.py
compose up --detach --wait --scale worker=2 worker
compose exec -T -e WORKFLOW_TEST_PHASE=parallel gateway python < scripts/verify_workflow_runtime.py
WORKFLOW_TEST_CONSUMER=$(compose exec -T redis redis-cli --raw GET workflow-e2e:kill-consumer)
WORKFLOW_TEST_VICTIM=
for WORKFLOW_TEST_CONTAINER in $(compose ps -q worker); do
  WORKFLOW_TEST_HOSTNAME=$(docker inspect "$WORKFLOW_TEST_CONTAINER" --format '{{.Config.Hostname}}')
  if [ "$WORKFLOW_TEST_HOSTNAME" = "$WORKFLOW_TEST_CONSUMER" ]; then
    WORKFLOW_TEST_VICTIM=$WORKFLOW_TEST_CONTAINER
  fi
done
test -n "$WORKFLOW_TEST_VICTIM"
docker kill --signal KILL "$WORKFLOW_TEST_VICTIM"
compose up --detach --wait --scale worker=2 worker
compose exec -T -e WORKFLOW_TEST_PHASE=recover gateway python < scripts/verify_workflow_runtime.py
compose exec -T gateway python -m app.postgres_worker backfill
compose exec -T gateway python -m app.postgres_worker reconcile
compose exec -T -e WORKFLOW_TEST_PHASE=history gateway python < scripts/verify_workflow_runtime.py
echo "Workflow v1 mixed-legacy isolation, parallelism, SIGKILL, duplicate delivery, ACL posture and history passed"
