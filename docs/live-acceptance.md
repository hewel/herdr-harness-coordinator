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
Codex Supervisors additionally require explicit
`--supervisor-codex-approval-policy` and `--supervisor-codex-sandbox-mode`
arguments; the live script visibly opts into `never` and `danger-full-access`.
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

## Crash and restart recovery

`scripts/live-acceptance/crash-recovery.sh` provides three opt-in phases:
`controlled-restart`, `supervisor-sigkill`, and `worker-sigkill`. Prepare the
durable state required by each phase first, then follow the environment contract in
`scripts/live-acceptance/README.md`. Prefer separate disposable state roots so a
deliberate crash in one scenario cannot invalidate another scenario's preconditions.
Run the controlled restart on each root before a SIGKILL phase; the script refuses
to crash a Host unless the live daemon executable is the selected tested candidate.

The controlled phase compares immutable Task, dependency, and Result evidence,
requires every pre-existing Session binding to survive, and rejects a new attempt
for any previously unsettled Supervisor event across a verified daemon handoff. The
Supervisor phase requires an event already in `dispatching` or
`accepted`, proves the exact Host process before SIGKILL, rebinds a managed Host,
and requires the unsettled event to become `unknown` without a new attempt. The
Worker phase requires an active mutating Task, then requires explicit safe Task
state, a Worktree Hold, Supervisor attention, blocked downstream work when supplied,
and no additional dispatch transition before starting a replacement Worker.

The script intentionally stops on the first missing invariant. An `unknown` event
remains for explicit Supervisor inspection and reconciliation; the acceptance driver
never chooses `retry`, `processed`, or `cancel` on the Supervisor's behalf.

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
