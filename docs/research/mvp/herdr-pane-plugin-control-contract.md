# Herdr pane and plugin control contract

Status: resolved for the Managed Runtime MVP.

This note defines the boundary between Herdr and Herdr Agent Orchestrator for
provider launch, pane identity, focus, cancellation, status, popups, and restart
reconciliation. It targets the locally verified `herdr 0.7.4` socket protocol
`16` and deliberately separates Herdr guarantees from project decisions.

## Confirmed Herdr facts

### Plugins and panes

- A plugin is an out-of-process package whose `herdr-plugin.toml` declares its
  actions and terminal-pane entrypoints. Runtime action registration and
  arbitrary runtime pane commands are not part of the v1 plugin API. Linked
  plugins persist across restarts, but plugins own the files and migrations
  beneath their configuration and state directories. See the official
  [plugin API reference](https://herdr.dev/docs/socket-api/#plugin-apis).
- `plugin.pane.open` launches a manifest-declared pane entrypoint. A `tab`
  placement is a normal managed Herdr pane; `plugin.pane.focus` and
  `plugin.pane.close` can later focus or close it. Herdr injects plugin,
  socket, workspace, tab, and pane context through `HERDR_*` variables. See
  [plugin pane launch and environment](https://herdr.dev/docs/socket-api/#plugin-apis).
- A `popup` is session-modal, does not alter tab layout, has no pane ID, is
  absent from pane and agent APIs, emits no pane lifecycle events, and does not
  receive `HERDR_PANE_ID`. `popup.close` closes only the popup. See
  [popup behavior](https://herdr.dev/docs/socket-api/#plugin-apis).

### Identity and observation

- `session.snapshot` is the bootstrap operation for a client-maintained cache.
  It returns protocol metadata, focused resource IDs, workspace, tab, pane and
  agent records, and layout snapshots. It is not a subscription; clients must
  subscribe to resource events afterward and fetch another snapshot after a
  reconnect or suspected stale cache. See
  [`session.snapshot`](https://herdr.dev/docs/socket-api/#raw-methods) and
  [event subscriptions](https://herdr.dev/docs/socket-api/#event-subscriptions).
- Pane records contain both a public `pane_id` and an internal `terminal_id`,
  plus the current workspace and tab IDs. Moving a pane across workspaces keeps
  its terminal alive but assigns a new public pane ID and emits `pane.moved`
  rather than synthetic close/create events. See
  [`pane.move`](https://herdr.dev/docs/socket-api/#raw-methods).
- The CLI wrappers and raw socket expose the same control surface. Herdr
  recommends CLI wrappers for simple automation and debugging, and the raw
  socket for protocol clients and long-lived subscriptions. See
  [choosing an integration layer](https://herdr.dev/docs/socket-api/#choose-an-integration-layer).

### Agent state and restoration

- `pane.report_agent` supplies semantic state used by waits, notifications, and
  rollups. `pane.report_metadata` changes presentation such as titles, visible
  state labels, and tokens without taking lifecycle authority. Metadata tokens
  are not restored after a server restart. See
  [agent state reporting](https://herdr.dev/docs/socket-api/#agent-state-reporting).
- Herdr maintains one status authority for a pane. OMP's official integration
  reports lifecycle state and native session identity; Codex's official
  integration reports session identity while screen detection supplies its
  state. See [status authority](https://herdr.dev/docs/agents/#status-authority)
  and the [integration reference](https://herdr.dev/docs/integrations/#how-herdr-uses-integrations).
- Detach/reattach preserves live processes, but a cold Herdr server restart
  does not. Snapshot restore recreates workspaces, tabs, panes, cwd, layout,
  and focus, while original shells and processes are gone. Herdr can relaunch
  supported agents only from native session references reported by current
  official integrations. See
  [session state and restore](https://herdr.dev/docs/session-state/).

## Managed Runtime MVP decisions

### Ownership

Herdr owns terminal topology: opening, placing, moving, focusing, and forcibly
closing panes. Herdr Agent Orchestrator remains the top-level authority for the
run, including the provider subprocess, native provider protocol, cancellation
handshake, normalized events, repository policy, verification, and durable
state.

Each managed run starts through a manifest-declared `worker` pane entrypoint:

- `placement = "tab"`
- `focus = false`
- the worker pane is a normal Herdr pane
- the plugin passes only the orchestrator `run_id` as run-specific input
- the worker launches and supervises `omp --mode rpc` or `codex app-server`

The plugin manifest must require `min_herdr_version = "0.7.4"`. The runtime
must check socket protocol `16` before depending on this contract.

### Identity contract

| Identity | Meaning | Persistence rule |
| --- | --- | --- |
| `run_id` | Orchestrator run identity | The only durable identity; stored by the orchestrator. |
| Provider session ID | Native OMP or Codex identity | Stored only in orchestrator state, never published as a Herdr native session reference. |
| `terminal_id` | Binding to one live Herdr terminal | Stable across a live pane move; not assumed to survive a cold server restart. |
| `pane_id` | Current public pane location | Mutable and refreshed after moves or reconciliation. |
| `tab_id`, `workspace_id` | Current UI location | Mutable and never used as run identity. |

The runtime bootstraps from `session.snapshot`, then tracks pane lifecycle
events. It matches active runs using the persisted live `terminal_id`, refreshes
the mutable pane, tab, and workspace IDs, and resnapshots after reconnect. A
`pane.moved` event updates location without changing run state.

### Managed-provider integration boundary

The worker is the sole semantic status authority for a managed run. To prevent
an official provider integration from becoming a competing authority or
publishing a native restore reference that bypasses the worker:

1. The worker retains the `HERDR_*` environment it needs.
2. It removes **all** `HERDR_*` variables from the managed provider child's
   environment before spawning OMP or Codex.
3. It never reports `agent_session_id` or `agent_session_path` to Herdr.
4. It stores provider-native identity only in orchestrator durable state.
5. It derives Herdr semantic state from normalized provider events.

This rule applies only to orchestrator-managed provider subprocesses. OMP and
Codex launched directly by users remain governed by official Herdr integrations.

Environment sanitization is an architectural requirement, not yet a proven
provider guarantee. The OMP and Codex provider-contract tickets must verify in
real panes that removing `HERDR_*` prevents their current integrations from
reporting state or a native restore reference.

### Status and presentation

The worker publishes semantic state through `pane.report_agent` with a
monotonic sequence and uses `pane.report_metadata` only for display fields such
as title, provider, role, current phase, and terminal outcome.

| Orchestrator state | Herdr semantic state |
| --- | --- |
| Preparing, starting, working, editing, verifying, collecting artifacts, repository checking | `working` |
| Waiting for input or approval | `blocked` |
| Completed, invalid, failed, or cancelled | `idle` |
| Live state cannot be reconciled | `unknown` |

SQLite and artifact files remain authoritative. Herdr metadata is a projection,
not a source for workflow state or restart recovery. The exact terminal outcome
is retained in durable state and displayed through metadata rather than encoded
as a new Herdr lifecycle state.

### Focus and cancellation

- Focus resolves the persisted `terminal_id` against the current snapshot,
  updates stale location IDs, and calls `plugin.pane.focus` for the current
  plugin-owned worker pane.
- Cancellation first persists cancellation intent, then asks the provider
  adapter to cancel cooperatively through its native protocol.
- If the provider does not stop within the lifecycle-policy grace period, the
  runtime escalates with `plugin.pane.close`. Forced termination is recorded
  separately from cooperative cancellation.
- Closing the details popup calls only `popup.close`; it never cancels the run
  or closes the worker pane.

The grace duration and cancellation race precedence belong to the lifecycle
contract. This note fixes only the cooperative-then-forced ordering.

### Popup contract

The manifest declares a `run-details` entrypoint with `placement = "popup"`.
It is an observer/controller over durable orchestrator state and may request
focus or cancellation through orchestrator commands. It does not own or
supervise either the worker or provider process, cannot infer a run from
`HERDR_PANE_ID`, and must receive the selected `run_id` through plugin context
or an explicit plugin-owned environment value.

### Reconciliation

On a socket reconnect or control-client restart, the runtime:

1. verifies the Herdr protocol version;
2. reads `session.snapshot`;
3. matches active bindings by `terminal_id`;
4. refreshes pane, tab, and workspace IDs;
5. subscribes to pane lifecycle and status events; and
6. republishes semantic state and metadata from durable state.

If the terminal still exists, the run continues without a lifecycle transition.
If an active worker pane exits or closes, the runtime completes an already
requested cancellation or otherwise fails the run with a host-pane-loss reason.
In both cases it performs repository post-checks before releasing an edit lease.

After a cold Herdr server restart, the original worker and provider processes
are gone. The MVP therefore:

1. observes that the persisted terminal binding is absent;
2. fails the active run with a cold-host-restart reason;
3. clears the live Herdr binding;
4. performs repository reconciliation and preserves all changes for parent
   inspection; and
5. requires the parent to submit a new run.

It never automatically adopts, resumes, or retries the old provider session.

## Downstream verification obligations

- **OMP provider contract:** prove that a managed `omp --mode rpc` child with
  all `HERDR_*` variables removed emits neither official Herdr lifecycle reports
  nor a native restore reference.
- **Codex provider contract:** prove the same for `codex app-server`, including
  the installed Codex hook behavior.
- **Lifecycle and recovery contract:** define the cooperative-cancel grace
  duration, event ordering, repeated cancellation behavior, and terminal-state
  precedence for cancel/exit/verification races.
- **Repository safety contract:** define the post-check performed after worker
  loss or cold restart and the point at which the edit lease may be released.
- **Popup prototype:** validate focus, cancel, and close behavior through a real
  Herdr popup without transferring process ownership to the popup.
- **Herdr transport implementation:** generate or pin request/event types from
  the installed protocol schema, reject incompatible protocols clearly, and
  test `terminal_id` continuity across `pane.moved` in a real session.

## Acceptance scenarios

- Opening a run creates an unfocused tab containing the manifest worker and
  persists its run and live Herdr bindings.
- Moving the worker across tabs or workspaces changes public location IDs
  without losing the run binding.
- Focus succeeds after resolving a stale pane ID through the terminal binding.
- Cooperative cancellation uses the provider protocol; timeout escalation
  closes the worker pane.
- Closing the popup leaves the worker and provider running.
- Presentation metadata never changes semantic waits or durable workflow state.
- Socket reconnect reconstructs live mappings from a fresh snapshot and events.
- Worker loss and cold restart fail the run, reconcile repository state, and do
  not automatically retry, resume, or adopt a provider session.
