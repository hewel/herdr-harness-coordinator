#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <omp|codex> <worker-id> <profile-id>" >&2
  exit 2
fi

supervisor_kind=$1
worker_id=$2
profile_id=$3
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
workspace_id=${HERDR_WORKSPACE_ID:?run inside Herdr or export HERDR_WORKSPACE_ID}
session_socket=${HERDR_SOCKET_PATH:?run inside Herdr or export HERDR_SOCKET_PATH}
state_root=$(mktemp -d "${TMPDIR:-/tmp}/herdr-harness-live.XXXXXX")

case "$supervisor_kind" in
  omp) supervisor_model=${OMP_SUPERVISOR_MODEL:-kimi-code/k3:high} ;;
  codex) supervisor_model=${CODEX_SUPERVISOR_MODEL:-gpt-5.6-sol} ;;
  *) echo "unsupported Supervisor kind: $supervisor_kind" >&2; exit 2 ;;
esac

herdr-harness-coordinator workspace --state-dir "$state_root" set on \
  --workspace "$workspace_id" \
  --root "$repo_root" \
  --session-socket "$session_socket" \
  --supervisor-kind "$supervisor_kind" \
  --supervisor-model "$supervisor_model" \
  --worker "$worker_id=$profile_id" \
  --json

printf 'LIVE_STATE_ROOT=%s\n' "$state_root"
herdr pane list --workspace "$workspace_id"
