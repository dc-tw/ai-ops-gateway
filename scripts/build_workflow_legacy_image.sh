#!/bin/sh
set -eu

# Reproducible source for the pre-Workflow compatibility fixture. This does not
# checkout/reset the user's worktree and never copies ignored .env credentials.
WORKFLOW_LEGACY_REF=7c6ebe351ab02726dc0853fd6f237bdabb636463
WORKFLOW_LEGACY_CONTEXT=$(mktemp -d "${TMPDIR:-/tmp}/gateway-workflow-legacy.XXXXXX")
cleanup() {
  # Only the fresh directory created above; every file is recoverable from Git.
  case "$WORKFLOW_LEGACY_CONTEXT" in
    */gateway-workflow-legacy.*)
      if [ -d "$WORKFLOW_LEGACY_CONTEXT" ]; then rm -r -- "$WORKFLOW_LEGACY_CONTEXT"; fi ;;
    *) echo "Refusing unexpected temporary context path" >&2; exit 1 ;;
  esac
}
trap cleanup EXIT INT TERM
git cat-file -e "$WORKFLOW_LEGACY_REF^{commit}"
git archive "$WORKFLOW_LEGACY_REF" | tar -x -C "$WORKFLOW_LEGACY_CONTEXT"
docker build --build-arg INSTALL_BROWSER=false \
  --label "org.opencontainers.image.revision=$WORKFLOW_LEGACY_REF" \
  --tag ai-ops-workflow-v1-legacy:7c6ebe3 "$WORKFLOW_LEGACY_CONTEXT"
