#!/bin/sh
# Opt-in real-model smoke test. It downloads qwen2.5:7b-instruct on first run.
set -eu

PROJECT_NAME="ai-ops-gateway-ollama-test"
export OLLAMA_MODEL="${OLLAMA_MODEL:-qwen2.5:0.5b-instruct}"
export LLM_MODEL="${LLM_MODEL:-${OLLAMA_MODEL}}"
COMPOSE="docker compose -p ${PROJECT_NAME} --profile ollama -f docker-compose.yml -f docker-compose.ollama-test.yml"
RESULT_FILE="$(mktemp)"

cleanup() {
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    ${COMPOSE} down --remove-orphans
  fi
  rm -f "${RESULT_FILE}"
}
trap cleanup EXIT INT TERM

${COMPOSE} up --detach --build
${COMPOSE} wait ollama-init

attempt=0
until curl --fail --silent http://localhost:8000/healthz >/dev/null; do
  attempt=$((attempt + 1))
  if [ "${attempt}" -ge 60 ]; then
    echo "Gateway did not become healthy" >&2
    exit 1
  fi
  sleep 2
done

curl --fail --no-buffer --max-time 300 \
  -H 'content-type: application/json' \
  -d '{"query":"請只回答：Ollama internal network is ready","max_retries":0}' \
  http://localhost:8000/v1/agent/stream >"${RESULT_FILE}"

grep -q 'event: token' "${RESULT_FILE}"
grep -q 'event: completed' "${RESULT_FILE}"
echo "Docker gateway -> Ollama (${OLLAMA_MODEL}) integration test passed"
