#!/bin/sh
set -eu

# A dedicated project, image tags, loopback port and Redis volume avoid touching
# the normal smoke stack or its persisted records. KEEP_STACK=1 retains evidence.
ATR_TOOL_PROJECT="${ATR_TOOL_PROJECT:-ai-ops-atr-tool-test}"
export ATR_TOOL_PROJECT

compose() {
  docker compose -p "${ATR_TOOL_PROJECT}" \
    -f docker-compose.yml -f docker-compose.test.yml -f docker-compose.atr-tool-test.yml "$@"
}

cleanup() {
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    compose down --volumes --remove-orphans
  fi
}
trap cleanup EXIT INT TERM

compose up --detach --build --wait redis mock-llm browser gateway
compose exec -T gateway python < scripts/verify_atr_remote_tool.py
