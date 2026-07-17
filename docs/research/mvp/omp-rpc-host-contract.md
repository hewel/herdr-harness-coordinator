# OMP RPC host contract

Status: resolved for the Managed Runtime MVP.

This note defines how Herdr Agent Orchestrator starts and controls OMP, maps
OMP RPC frames into an `AgentRun`, obtains a structured artifact, cancels work,
and retains native evidence. It targets the locally verified OMP `17.0.2`
protocol and deliberately keeps OMP's native orchestration outside the MVP.

## Confirmed OMP facts

### Process and wire protocol

- `omp --mode rpc` is a long-lived, headless process that reads newline-delimited
  JSON from stdin and writes newline-delimited JSON to stdout. It emits
  `{"type":"ready"}` when the RPC command loop can accept input. Commands,
  responses, session events, host-tool requests, and extension UI requests
  share the same stream. See the versioned
  [RPC reference](https://github.com/can1357/oh-my-pi/blob/v17.0.2/docs/rpc.md)
  and
  [`runRpcMode`](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-mode.ts).
- Normal commands accept an optional `id` and response frames echo it. Event
  frames and responses may interleave, and background bash responses are not
  ordered with serial commands. Malformed input produces a recoverable parse
  error rather than terminating the process. The host must therefore frame by
  line and correlate by ID rather than by arrival order. See the
  [RPC command and response types](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-types.ts).
- `prompt` acknowledges scheduling; it does not mean the agent finished. A
  prompt may be handled locally and report `agentInvoked: false`, while an
  invoked turn produces agent lifecycle events and settles at `agent_end`.
  `get_state` exposes `isStreaming`, session identity, queue state, the system
  prompt, and the current tool definitions through `dumpTools`. See
  [RPC prompt dispatch and state](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-mode.ts).
- Closing stdin rejects pending extension UI, host-tool, and host-URI requests,
  drains accepted commands, disposes the session, and exits with status `0`.
  OMP may also request a deferred shutdown, which drains tracked work before
  disposing the session. See
  [RPC shutdown coordination](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-mode.ts).

### Host tools and observation

- `set_host_tools` replaces the host-owned tools exposed to the session. OMP
  emits `host_tool_call` with both a host request ID and model tool-call ID; the
  host completes that request with `host_tool_result` and may stream
  `host_tool_update` frames. If OMP aborts a pending call, it emits
  `host_tool_cancel` whose `targetId` is the original host request ID. See the
  [host-tool wire types](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-types.ts)
  and
  [host-tool bridge](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/host-tools.ts).
- `set_subagent_subscription` controls optional `off`, `progress`, or `events`
  frames. `get_subagents` and `get_subagent_messages` can inspect native child
  state, but observation does not prevent child creation. Native delegation is
  instead prevented by withholding its tools. See the
  [subagent RPC adapter](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/modes/rpc/rpc-subagents.ts).
- `get_messages` returns the session's normalized messages and `get_state`
  returns both `sessionId` and `sessionFile`. OMP persists the native JSONL
  session unless launched with `--no-session`.

### Startup discovery limitation

OMP constructs the agent session before entering `runRpcMode` and before
emitting `ready`. In that construction path it:

- creates the requested built-in tools;
- discovers and connects MCP servers in headless mode;
- discovers custom tools from provider, project, user, and plugin locations;
  and
- imports custom-tool modules and invokes their factories.

The `--no-extensions` switch disables extension discovery, but it does not
disable MCP or custom-tool discovery. An explicit `--tools` list constrains
built-ins, but custom and MCP tools can still enter the registry. Project MCP
configuration can be disabled with `mcp.enableProjectConfig = false`; OMP
`17.0.2` does not expose one CLI switch that disables all MCP and custom-tool
discovery. These behaviors are visible in
[`createAgentSession`](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/sdk.ts),
the
[custom-tool loader](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/extensibility/custom-tools/loader.ts),
and the
[settings schema](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/config/settings-schema.ts).

Consequently, validating `dumpTools` after `ready` detects an unexpected tool
set but cannot contain side effects already performed during discovery. The
Managed OMP path is blocked until the
[Managed repository safety contract](repository-safety-contract.md) is
implemented and proves process isolation before OMP starts, or a verified OMP
version supplies a complete discovery-disable mechanism.

## Managed Runtime MVP decisions

### Compatibility and launch

The adapter supports OMP `17.0.2` exactly. It must execute `omp --version`
before accepting a run and reject another version until its launch, frames,
events, and shutdown behavior have been compatibility-tested.

One worker starts one run-owned OMP process with the equivalent of:

```text
omp
  --mode rpc
  --cwd <validated-worktree-path-projected-from-run-overlay>
  --session-dir <run-state-dir>/provider-session
  --append-system-prompt <run-state-dir>/resolved-role-prompt.md
  --tools <comma-separated-effective-builtins>
  --config <run-state-dir>/omp-managed.yml
  --no-extensions
  --no-skills
  --no-rules
  --no-title
  --approval-mode <effective-policy-mode>
```

The `--cwd` value is the validated absolute worktree path inside the sandbox;
it resolves to the Repository Snapshot and Run Overlay projection, never the
live writable worktree. The worker passes the task through the RPC `prompt`
command rather than as a CLI argument. It does not use `--no-session`,
`--continue`, `--resume`, or an
initial message. The system-prompt append file contains only the resolved role,
policy summary, artifact protocol, and bounded task behavior; secrets are not
placed in argv or the prompt file.

The run-owned configuration overlay fixes values that would otherwise widen or
change execution independently of the orchestrator:

```yaml
advisor.enabled: false
prewalk.enabled: false
retry.enabled: false
retry.modelFallback: false
memory.backend: off
autolearn.enabled: false
mcp.enableProjectConfig: false
tools.xdev: false
astGrep.enabled: false
astEdit.enabled: false
```

The exact built-in list comes from the resolved role and effective policy. It
must omit `task`, `ask`, `todo`, and every unapproved editing or execution tool;
it must never contain `hub`. `task` is the native subagent entrypoint. `hub`
must also be absent even though it is not listed as a default built-in, because
it can communicate with native peers. No `join`, handoff, branch, session
switch, or subagent RPC command is part of Managed execution.

Approval mode is resolved by execution policy, not chosen by the adapter. The
adapter must not translate `WaitingForInput` into approval or use
`--auto-approve` unless the effective policy explicitly represents equivalent
authorization. Process isolation remains mandatory regardless of approval
mode because provider-level approval is not the repository security boundary.

### Environment boundary

The worker removes every `HERDR_*` variable from the OMP child's environment.
It passes only the run-scoped model-broker endpoint and capability plus runtime
variables explicitly allowed by effective policy. The real provider credential
stays in the host-side broker. The child does not inherit arbitrary tool,
plugin, collaboration, credential, or provider-routing overrides. OMP native
session identity is stored in orchestrator state and is never reported to
Herdr as a native restore reference.

This preserves the worker as the sole semantic status authority under the
[Herdr pane and plugin control contract](herdr-pane-plugin-control-contract.md).
The implementation must still prove in a real Herdr pane that current OMP code
with the sanitized environment emits no official Herdr lifecycle or native
session report.

### Startup handshake

After spawning the isolated process, the adapter:

1. reads protocol stdout by complete newline and requires exactly one valid
   `ready` frame within 30 seconds;
2. assigns a unique nonempty ID to every command and keeps a pending-command
   map independent of event ordering;
3. sends `set_host_tools` for `report_progress`, `report_blocked`, and the
   role-specific `complete_task` schema;
4. sends `set_subagent_subscription` with `level = "off"`;
5. sends `set_auto_retry` with `enabled = false` as a runtime confirmation;
6. calls `get_state`, persists `sessionId` and `sessionFile`, and compares the
   names in `dumpTools` with the exact effective built-ins plus the three
   registered host tools; and
7. sends the bounded task packet as a plain `prompt` only after every response
   above succeeds.

Any missing, duplicated, malformed, or premature frame fails startup. A tool
set mismatch fails closed before the model is prompted. In particular, the
presence of `task`, `hub`, an `mcp__*` tool, or any discovered custom tool is a
contract violation. Because the post-ready check is detection rather than
containment, the process must already be running inside the startup sandbox.

### Turn and event lifecycle

The prompt response is scheduling evidence only. For an AgentRun, the adapter
requires either `data.agentInvoked: true` or lifecycle evidence that an agent
turn started. `agentInvoked: false`, or a later `prompt_result` reporting
`false`, is invalid because a task packet must not resolve as a local slash
command.

The adapter maps OMP events as follows:

| OMP evidence | Normalized meaning |
| --- | --- |
| `agent_start`, `turn_start` | Working or thinking |
| `message_start`, `message_update`, `message_end` | Transcript/progress evidence |
| `tool_execution_start` for `bash` | Running command |
| `tool_execution_start` for `edit` or `write` | Editing the reported path |
| verification command executed by the orchestrator | Verifying |
| `extension_ui_request` or `report_blocked` | Waiting for input |
| `agent_end` | Invoked turn is idle; begin artifact and repository checks |
| provider error or unexpected EOF/exit | Provider failure |

The worker records every valid frame in order, but state transitions use
correlated commands and semantic event types rather than assumptions about
adjacency. `agent_end` is not successful run completion: it only permits the
orchestrator to collect and validate the candidate artifact.

### Structured host tools

`report_progress` accepts a concise progress message. `report_blocked` accepts
the question or blocking condition and structured context needed by the parent.
Neither tool changes terminal outcome.

`complete_task` uses the output schema required by the resolved role. Its
common envelope includes the task ID, proposed status, summary, changed files,
verification evidence, deviations, and risks. The host:

1. validates the request against the resolved schema and task identity;
2. durably stores it as a candidate artifact;
3. returns a structured success or schema error through `host_tool_result`;
4. rejects a second successful completion for the same turn; and
5. honors `host_tool_cancel` by abandoning work keyed by the original host
   request ID.

A successful host-tool result does not complete the run. Success still
requires `agent_end`, artifact validation, Run Overlay and Publish Delta
validation, and the orchestrator-owned verification and publication boundaries.
Natural-language assistant output is never parsed as the authoritative artifact.

### UI requests

An `extension_ui_request` is correlated and durably mapped to
`WaitingForInput`. The host forwards the request to the parent control path and
waits for an explicit response. It never invents a choice or confirms an
approval. Cancellation returns a cancelled `extension_ui_response`; host loss
rejects the pending request through stdin shutdown.

Tasks must contain resolved requirements, so an unexpected architecture or
product-choice request remains blocked for the parent rather than being
answered by the worker.

### Session and transcript

The adapter persists `sessionId` and `sessionFile` immediately after the
startup state check. At terminal collection it requests `get_messages`, stores
the normalized result as the orchestrator transcript, and retains the native
session JSONL as an immutable provider evidence reference. It may request
`get_last_assistant_text` for display, but not for structured completion.

The MVP never invokes `new_session`, `switch_session`, `branch`, `handoff`,
`get_subagents`, or `get_subagent_messages`. It never automatically resumes or
adopts the recorded session after worker loss or a cold Herdr restart. A parent
may submit a new run with an explicit handoff artifact; that is a new run, not
RPC session continuation.

### Cancellation and shutdown

Cancellation uses the following cooperative order:

1. persist cancellation intent before writing to the provider;
2. send one ID-correlated `abort` command;
3. accept either the abort response or `agent_end` first;
4. cancel host-side work when `host_tool_cancel` arrives and respond cancelled
   to outstanding UI requests;
5. after the abort response, call `get_state` until `isStreaming` is false or
   the lifecycle-policy grace period expires;
6. capture messages and repository evidence that remain available; and
7. close stdin, wait for OMP's normal status-`0` exit, then let the worker
   publish the terminal result.

Repeated cancellation is idempotent at the orchestrator boundary and must not
send concurrent abort commands. EOF is used only after pending host/UI
interactions have been settled or deliberately cancelled. If cooperative
shutdown exceeds the later lifecycle-policy grace period, the Herdr contract
allows the worker pane to be forcibly closed; forced termination is recorded
separately from cooperative cancellation.

The lifecycle contract, not this provider contract, decides precedence when
completion, cancellation, process exit, or verification race. This note fixes
only the provider observations and cooperative ordering.

### Failure handling

The provider run fails without automatic retry when any of these occurs:

- OMP version mismatch, spawn failure, startup timeout, EOF, or exit before
  `ready`;
- non-JSON stdout, a malformed frame, duplicate readiness, an unknown response
  ID, or a command response whose type does not match its pending request;
- host-tool registration, subagent-disable, state, prompt, abort, or transcript
  command failure, or failure to complete the stdin-driven shutdown handshake;
- active-tool mismatch, especially native delegation, MCP, extension, or
  custom tools;
- a local-only prompt result, `agent_end` without one valid candidate artifact,
  or more than one accepted completion artifact;
- unexpected process exit, stdout closure before terminal collection, or a
  nonzero normal-shutdown status; or
- provider behavior that changes repository state outside the effective
  sandbox or write scope.

OMP command errors are normally recoverable at the process level, but an error
for a command required by this contract fails the AgentRun. The runtime keeps
stderr as diagnostic output and never parses it as RPC. It preserves the Run
Overlay and, when applicable, Publish Journal and Repository Quarantine
evidence for inspection. It never automatically reverts or retries the run.

## Required startup safety dependency

The Managed OMP vertical slice must not be enabled merely because its adapter
implements this handshake. Before OMP is spawned, the
[Managed repository safety contract](repository-safety-contract.md) must pass
its fail-closed Linux capability probes and provide an OS-enforced process
boundary that:

- limits filesystem writes to the effective write scope and run-owned state;
- prevents user/project tool factories and MCP servers from escaping that
  boundary during startup;
- applies to OMP and every subprocess it creates; and
- remains in force until the provider process is reaped.

That boundary uses the contract's private Repository Snapshot and Run Overlay,
Bubblewrap namespaces, Landlock, seccomp, cgroup v2 containment, nested command
sandbox, and broker-only model network. Until it is implemented and proved,
OMP Managed runs must return a clear unsupported or safety-prerequisite error.
A future OMP version with a verified, complete disable-discovery capability may
remove only the corresponding masking requirement after its startup path is
re-audited; it does not remove the repository boundary.

## Downstream implementation obligations

- **Repository safety contract:** implement and prove the linked startup-time
  filesystem, network, credential, and descendant-process boundary, including
  pre-`ready` imports and MCP subprocesses.
- **Provider adapter:** model every accepted RPC frame as version-pinned Rust
  protocol types, maintain command and host-call correlation maps, and keep
  native types inside the OMP module.
- **Role and policy resolution:** produce the exact built-in tool list,
  approval mode, system-prompt fragment, complete-task schema, and environment
  allowlist for each run.
- **Artifact protocol:** define the role-specific payloads and persist candidate
  artifacts atomically before acknowledging `complete_task`.
- **Lifecycle and recovery contract:** define grace periods, terminal-state
  race precedence, repeated cancel behavior, and post-loss reconciliation.
- **Herdr integration test:** prove that deleting all `HERDR_*` variables from
  the child prevents official OMP lifecycle and session-reference reporting.
- **Compatibility suite:** retain captured 17.0.2 startup, prompt, host-tool,
  abort, transcript, EOF, and error fixtures and require them before accepting
  another OMP version.

## Acceptance scenarios

- A valid isolated 17.0.2 process becomes ready, registers the three host tools,
  reports subagent subscription `off`, exposes exactly the effective tools, and
  persists its native session identity before the task prompt.
- Interleaved events and out-of-order command responses resolve through IDs
  without corrupting run state.
- A prompt acknowledgement cannot complete a run; one schema-valid
  `complete_task`, `agent_end`, repository checks, and verification are all
  required.
- `task`, `hub`, any `mcp__*` entry, or a discovered custom tool in `dumpTools`
  rejects startup before prompting the model.
- A fixture custom-tool factory that attempts a side effect before `ready` is
  contained by the process sandbox, proving the post-ready check is not the
  security boundary.
- Host-tool progress, cancellation, schema error, duplicate completion, and
  `host_tool_cancel` use the correct request IDs and durable outcomes.
- An extension UI request blocks without an implicit answer and can be
  explicitly answered or cancelled through the parent path.
- Cooperative cancellation persists intent, sends one abort, tolerates either
  response/event ordering, confirms idle state, captures evidence, closes
  stdin, and observes a clean exit.
- Closing stdin with a pending host call rejects that call and disposes OMP;
  unexpected EOF or process loss instead fails the run and triggers repository
  reconciliation.
- Transcript collection stores `get_messages` plus the native JSONL reference,
  while cold recovery never resumes or adopts that session.
- A real Herdr worker pane confirms that the sanitized OMP child emits no
  official Herdr status or native restore reference.
