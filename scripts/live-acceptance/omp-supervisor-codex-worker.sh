#!/bin/sh
set -eu
exec "$(dirname -- "$0")/start-flow.sh" omp codex-worker codex-sol
