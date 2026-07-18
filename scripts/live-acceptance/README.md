# Live acceptance

These scripts intentionally keep paid-provider runs outside normal CI. They require
Herdr, OMP, Codex, SQLite, and authenticated provider installations.

```bash
./scripts/live-acceptance/setup.sh
./scripts/live-acceptance/omp-supervisor-codex-worker.sh
./scripts/live-acceptance/codex-supervisor-omp-worker.sh
./scripts/live-acceptance/capture-evidence.sh "$WORKSPACE_STATE_DIR"
```

Crash and restart acceptance is deliberately destructive and runs only against an
already prepared disposable live state root. Each phase has explicit process and
durable-state prerequisites:

```bash
export LIVE_CRASH_ACCEPT=I_UNDERSTAND_THIS_SENDS_SIGNALS
export LIVE_CANDIDATE_BIN=/absolute/path/to/tested/herdr-harness-coordinator
export HERDR_WORKSPACE_ID=<workspace>
export HERDR_SOCKET_PATH=<Herdr-session-socket>

./scripts/live-acceptance/crash-recovery.sh "$WORKSPACE_STATE_DIR" controlled-restart

export LIVE_SUPERVISOR_EVENT_ID=<dispatching-or-accepted-event>
./scripts/live-acceptance/crash-recovery.sh "$WORKSPACE_STATE_DIR" supervisor-sigkill

export LIVE_WORKER_ID=<worker>
export LIVE_WORKER_SESSION_ID=<session-uuid>
export LIVE_MUTATING_TASK_ID=<active-mutating-task-uuid>
export LIVE_DOWNSTREAM_TASK_ID=<optional-dependent-task-uuid>
./scripts/live-acceptance/crash-recovery.sh "$WORKSPACE_STATE_DIR" worker-sigkill
```

Run the phases on separate prepared state roots when their preconditions conflict.
On every state root, run `controlled-restart` first so the later crash phases can
prove that the live daemon is the tested candidate.
The driver validates `/proc` executable, command-line, workspace, and Session
identity before signaling; PID-file existence alone is never liveness authority.
It captures durable and pane evidence at every boundary. It never reconciles an
Unknown event, clears a Hold, approves a Task, or blindly replays ambiguous work.

Use the identity-bound control helper during a live run instead of manually
constructing MCP JSON-RPC frames. It discovers the immutable binary, Supervisor
capability, and short broker socket from the workspace state directory:

```bash
./scripts/live-acceptance/control.sh "$WORKSPACE_STATE_DIR" status
./scripts/live-acceptance/control.sh "$WORKSPACE_STATE_DIR" start implementer
./scripts/live-acceptance/control.sh "$WORKSPACE_STATE_DIR" handoff /path/to/tested-candidate
./scripts/live-acceptance/control.sh "$WORKSPACE_STATE_DIR" \
  call harness_task_approve '{"task_id":"...","result_revision":1,"observation_digest":"..."}'
./scripts/live-acceptance/control.sh "$WORKSPACE_STATE_DIR" evidence evidence.json
```

The helper never prints the durable capability. Before a daemon handoff it
checks the PID's live command line for both the daemon subcommand and exact
workspace state directory, verifies the replacement executable, and runs an
authenticated broker query; the PID file or socket alone is not treated as
liveness authority. `HERDR_COORDINATOR_BIN` and `HERDR_COORDINATOR_SOCKET` can
be supplied when process-based discovery is not appropriate.

`start-flow.sh` creates a unique `/tmp/herdr-harness-live.*` state root and prints
the resolved workspace state directory plus Herdr pane identities. Keep that state
directory until the evidence report has been captured. The Codex live profile uses
`danger-full-access` explicitly because its `workspace-write` sandbox rejects the
Coordinator Unix socket. This is suitable only for the MVP's documented same-user,
cooperative threat model.

The scenario prompts and evidence fields are recorded in
`docs/live-acceptance.md`. Never interpret a pane launch or provider response alone
as acceptance: require the durable Task, Result, Supervisor Event, delivery attempt,
and native turn evidence described there.
