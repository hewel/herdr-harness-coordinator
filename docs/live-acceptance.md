# Live provider acceptance

## Install and activate

The verified development installation uses a release binary symlink and a linked
Herdr plugin:

```bash
cargo build --release --locked
ln -sfn "$PWD/target/release/herdr-harness-coordinator" \
  "$HOME/.local/bin/herdr-harness-coordinator"
herdr plugin unlink herdr-harness-coordinator || true
herdr plugin link "$PWD/plugin/herdr-harness-coordinator"
cp scripts/live-acceptance/profiles/*.toml \
  "$(herdr plugin config-dir herdr-harness-coordinator)/profiles/"
herdr server reload-config
```

Verify the installed entrypoints with:

```bash
herdr plugin action invoke workspace --plugin herdr-harness-coordinator
herdr plugin pane open --plugin herdr-harness-coordinator --entrypoint harness-network
```

The `supervisor` and `worker` pane entrypoints are opened by `workspace set on`.
Herdr owns `HERDR_PLUGIN_STATE_DIR`; spawned panes receive the per-workspace
Coordinator root as `HERDR_COORDINATOR_STATE_DIR`. They also require
`HERDR_SOCKET_PATH` and their identity capability. Worker panes additionally receive
`HERDR_HARNESS_SESSION_ID` and `HERDR_HARNESS_CWD`.

## Evidence requirements

For every live flow retain:

- the isolated state root and `coordinator.sqlite3`;
- Coordinator Task and Harness Session IDs;
- provider session/thread/turn IDs;
- Supervisor Event and attempt state transitions;
- Result revisions and immutable Attachment IDs;
- Herdr workspace, tab, pane, and terminal IDs;
- the provider's native JSONL transcript when available.

Use `scripts/live-acceptance/capture-evidence.sh` for the durable projection. Codex
turn IDs are also present in its rollout JSONL. Herdr pane IDs come from
`herdr pane list --workspace "$HERDR_WORKSPACE_ID"`.

## Current verified behavior

The 2026-07-18 run proved both directions and Correction reuse against Herdr 0.7.4,
OMP 17.0.4, and Codex 0.144.5. Exact run IDs and limitations are in the completion
report for the change that introduced this document. Normal CI remains fixture-only.

Codex App Server does not accept the CLI `--profile` option. Profile schema v3 pins
the supported `approval_policy` and `sandbox_mode` values and sends them through
`thread/start`. The live profile uses `danger-full-access`: `workspace-write` blocks
the Unix-socket MCP bridge with `EPERM`.

OMP may report a provider-local model alias such as `k3` after accepting an explicit
`kimi-code/k3:high` launch. The adapter retains the immutable selected model for
Session compatibility. A never-used incompatible Session now becomes a durable
blocker instead of causing unbounded same-profile rotation.

## Release boundary

Do not call a build production-ready until both provider directions, idle and busy
follow-up delivery, Correction reuse and rejection, DAG behavior, controlled restart,
SIGKILL recovery, and stale-presence expiry have all passed in the target Herdr
environment. Unknown delivery is never replayed without Supervisor reconciliation.
