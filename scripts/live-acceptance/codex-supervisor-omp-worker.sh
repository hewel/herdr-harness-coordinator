#!/bin/sh
set -eu
exec "$(dirname -- "$0")/start-flow.sh" codex omp-worker omp-kimi
