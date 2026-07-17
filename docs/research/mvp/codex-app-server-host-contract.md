# Codex App Server host contract

Status: resolved for the Managed Runtime MVP.

This note defines how Herdr Agent Orchestrator starts and controls Codex App
Server, maps App Server messages into an `AgentRun`, obtains a structured
artifact, cancels work, and retains native evidence. It targets the locally
verified Codex CLI `0.144.5` protocol and treats Codex as a single managed
provider. Codex-native multi-agent behavior is outside the MVP.

## Confirmed Codex facts

### Process and wire protocol

- `codex app-server --listen stdio://` is a long-lived process that reads and
  writes newline-delimited JSON on stdin and stdout. The protocol follows
  JSON-RPC 2.0 semantics but deliberately omits the `"jsonrpc":"2.0"` member.
  Requests, responses, server requests, and notifications share the same
  stream and may interleave. See the versioned
  [App Server protocol](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#protocol).
- App Server exposes version-specific TypeScript and JSON Schema generation.
  The generated definitions are authoritative for the installed CLI version;
  the adapter must not assume that another Codex version has an identical
  message set. See the versioned
  [message-schema guidance](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#message-schema).
- Stdio has no independent readiness frame. The client first sends exactly one
  `initialize` request, waits for its correlated response, and then sends one
  `initialized` notification. Requests before the handshake are rejected, as
  are repeated `initialize` requests. The initialize response returns the
  effective `codexHome`, upstream `userAgent`, `platformFamily`, and
  `platformOs`. See
  [initialization](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#initialization).
- `thread/start` creates and subscribes the connection to a new native thread;
  `turn/start` schedules one turn within it. The immediate response is creation
  evidence, not terminal completion. Turn and item notifications are the live
  execution stream, and `turn/completed` is the terminal provider event. See
  the
  [lifecycle overview](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#lifecycle-overview).
- Closing stdin after the initialization handshake and receiving EOF caused the
  locally probed `0.144.5` App Server to exit with status `0`. The adapter still
  treats this as a version-pinned observation rather than a protocol guarantee.

### Thread, turn, and item behavior

- A fresh `thread/start` response includes the thread ID, persisted session ID,
  path when persisted, model/provider selection, working directory, sandbox
  projection, approval settings, parent relation, and ephemeral status. Starting
  a thread with a write-enabled sandbox and a working directory may mark that
  project trusted in Codex configuration. The Managed adapter must therefore
  start the thread read-only and apply the resolved role sandbox at
  `turn/start`. See the
  [`thread/start` behavior](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#api-overview).
- `turn/start` accepts an optional JSON `outputSchema`. App Server forwards that
  schema as the required final-answer shape for the turn, while the completed
  assistant message still arrives through ordinary item notifications. The
  schema belongs to a turn, not a persistent thread. See
  [`turn/start`](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#api-overview).
- `item/started` and `item/completed` identify semantic items such as agent
  messages, reasoning, command executions, file changes, MCP calls, and
  collaboration calls. Delta notifications are useful for display and logs,
  but the completed item is the authoritative item result. A
  `turn/completed` payload may not reproduce every streamed item, so the client
  must retain the live item stream. See the versioned
  [event model](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#events).
- `thread/read` can return the stored thread and optionally its turns without
  resuming it. `thread/unsubscribe` removes the current connection's event
  subscription but does not immediately unload the native thread. See the
  [thread API](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#api-overview).

### Approvals and cancellation

- Command, file-change, and supported tool operations can arrive as
  server-to-client approval or input requests. The client must return a
  correlated response. App Server supports session-wide acceptance and policy
  amendments, but those decisions widen authority beyond one bounded task. See
  [approvals](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#approvals).
- `turn/interrupt` addresses an in-flight turn by thread and turn ID. Its empty
  response acknowledges the request; terminal evidence is a later
  `turn/completed` with `status: "interrupted"`. Interruption does not terminate
  background terminal processes. Stable cleanup therefore cannot depend on
  turn interruption alone. The available background-terminal cleanup API is
  experimental. See
  [`turn/interrupt` and background terminals](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#api-overview).

### Configuration and discovery limitations

- Codex reads configuration, authentication, sessions, hooks, plugins, skills,
  and other provider state relative to `CODEX_HOME`. A run that inherits the
  user's normal Codex home can therefore import authority and behavior that the
  orchestrator did not resolve.
- Codex `0.144.5` enables the stable `multi_agent` feature by default, while
  `multi_agent_v2` is an under-development feature. Other optional systems such
  as plugins, hooks, goals, memories, browser/computer use, and permission
  request tools are independently feature-gated. The Managed launch must
  disable every feature that can add orchestration, stateful behavior, or an
  unhandled control channel. See the versioned
  [feature registry](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/features/src/lib.rs)
  and
  [configuration schema](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/core/config.schema.json).
- An empty managed `CODEX_HOME`, strict configuration, and disabled plugin and
  multi-agent features do **not** suppress repository skill discovery. A local
  `0.144.5` probe confirmed that untrusted project configuration, hooks, and
  execution policies were disabled while App Server emitted a `configWarning`
  explaining that project skills still load. Discovery is read-only, but it
  happens inside the outer startup sandbox. Skill instructions and any tools
  or scripts they cause the model to invoke must remain contained by the
  effective turn sandbox and outer process boundary even after the adapter
  disables unapproved skills.
- The stable `skills/list` request can force a refresh for specified working
  directories, and `skills/config/write` can enable or disable a discovered
  skill by absolute path or name in user-level managed-home configuration. The
  host can therefore enforce a resolved enabled-skill set before creating a
  thread, although it cannot prevent discovery itself. See the versioned
  [skills API](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/app-server/README.md#skills).
- Codex `0.144.5` does not expose a complete stable allowlist for every
  model-facing tool through App Server or `config.toml`. Provider sandbox and
  approval settings can constrain execution, but they cannot by themselves
  enforce an arbitrary role-level `allowed_tools` list. The generated
  version-specific schema must be used to verify this limitation before any
  future version is allowed to replace `0.144.5`.

## Managed Runtime MVP decisions

### Compatibility and generated protocol

The adapter supports Codex CLI `0.144.5` exactly. It must execute
`codex --version` before accepting a run and reject another version until its
generated schemas, startup configuration, events, approvals, cancellation,
and shutdown behavior have been compatibility-tested.

The repository stores the JSON Schema generated by that exact CLI as a test
fixture. Rust protocol types cover only the stable methods and notifications
used by this contract. Unknown experimental messages do not silently become
normal progress: any message that requests authority, creates a child, or
invokes an unmodeled tool fails closed, while harmless unknown notifications
are retained as evidence and handled according to the compatibility policy.

### Managed home, authentication, and launch

Each worker starts one run-owned App Server process. It uses a run-owned,
persistent home beneath:

```text
$HERDR_PLUGIN_STATE_DIR/provider-state/codex/runs/<run-id>
```

`CODEX_HOME` and `CODEX_SQLITE_HOME` point at that managed location. The worker
creates only the minimum orchestrator-controlled configuration needed for the
provider. It does not copy or bind-mount the user's `~/.codex`, because doing so
would import user hooks, plugins, MCP servers, sessions, rules, memories, and
other unresolved state. A run-owned home also prevents `skills/config/write`
from changing another concurrently running role's enabled-skill set.

The host-side model broker owns and injects the real provider credential. App
Server receives only the broker endpoint and a run-scoped capability; no
reusable provider credential is written into its environment, managed home,
argv, task packet, transcript, or persisted role prompt. The orchestrator never
copies credentials from the user's Codex home. The managed directory is
created with owner-only permissions. A profile that cannot use the broker is
unsupported, and missing or invalid broker authorization is not permission to
fall back to the user's Codex home. See the official
[authentication guidance](https://developers.openai.com/codex/auth).

The process starts inside the outer Repository Guard sandbox and run-owned
process boundary. Its validated absolute worktree path projects the private
Repository Snapshot and Run Overlay, never the live writable worktree. It uses
the equivalent of:

```text
codex app-server
  --listen stdio://
  --strict-config
  --disable apps
  --disable auth_elicitation
  --disable browser_use
  --disable browser_use_external
  --disable browser_use_full_cdp_access
  --disable code_mode_host
  --disable computer_use
  --disable goals
  --disable guardian_approval
  --disable hooks
  --disable image_generation
  --disable in_app_browser
  --disable memories
  --disable multi_agent
  --disable multi_agent_v2
  --disable plugins
  --disable plugin_sharing
  --disable remote_plugin
  --disable request_permissions_tool
  --disable shell_snapshot
  --disable skill_mcp_dependency_install
  --disable tool_call_mcp_elicitation
  --disable tool_suggest
  --disable workspace_dependencies
  -c 'allow_login_shell=false'
  -c 'web_search="disabled"'
  -c 'approvals_reviewer="user"'
```

The maintained launch builder, not a shell-expanded command string, supplies
these arguments. The compatibility fixture must prove that each referenced
feature exists in `0.144.5` and is disabled. `experimentalApi` remains false,
Ultra reasoning is forbidden, and deprecated `multiAgentMode` is never used.
The adapter also never selects `approvals_reviewer = "auto_review"`, because
automated review can introduce provider-owned agent behavior.

The managed configuration sets `shell_environment_policy.inherit = "none"`,
keeps Codex's default secret-name exclusions enabled, and supplies only the
minimal command environment resolved by policy. App Server receives the model
broker endpoint and run-scoped capability, but neither that capability nor an
underlying provider credential is forwarded to model-generated commands. The
outer process sandbox must additionally prevent descendants from recovering
the provider environment through host process-inspection APIs.

### Environment boundary

The worker removes every `HERDR_*` variable from the Codex child's environment.
This is required because the official Herdr Codex integration uses a global
`SessionStart` hook and the Herdr environment/socket/pane identity to publish
native lifecycle state. Hooks are disabled as a second independent measure.

Only run-owned home paths, the run-scoped broker endpoint and capability,
locale/runtime essentials, and effective policy variables are passed.
Arbitrary credentials, MCP, plugin, hook, browser, collaboration,
provider-routing, and model overrides are not inherited. The implementation
must prove in a real Herdr pane that this sanitized child emits no official
Herdr lifecycle or native restore reference.

### Startup handshake

After spawning the isolated process, the adapter writes one compact JSON object
per line and requires this exact stable handshake:

```json
{"method":"initialize","id":"run-0194:initialize","params":{"clientInfo":{"name":"herdr_agent_orchestrator","title":"Herdr Agent Orchestrator","version":"<plugin-version>"},"capabilities":{"experimentalApi":false}}}
{"method":"initialized"}
```

The second line is sent only after the correlated initialize response succeeds.
The adapter requires that response within 30 seconds, compares the returned
`codexHome` with the canonical managed home, and persists `userAgent`,
`platformFamily`, and `platformOs`. A missing, duplicated, malformed,
premature, or mismatched response fails startup. App Server messages never
contain an added `jsonrpc` member.

Every later client request uses a unique nonempty ID namespaced by the run and
method. A pending-request map correlates responses independently of
notifications and server requests. Stderr is retained as diagnostics but is
never parsed as protocol stdout.

The worker records any startup `configWarning`, including the expected warning
that untrusted project skills remain discoverable. A new warning, or absence of
the expected skill-discovery warning in a fixture designed to trigger it, is
compatibility evidence requiring review rather than proof of stronger safety.

### Pre-thread skill audit

Before `thread/start`, the adapter sends `skills/list` with
`forceReload: true` and exactly the validated sandbox-projected worktree path in
`cwds`. It canonicalizes each discovered skill path inside that projection,
retains discovery errors and warnings, and compares each skill with the exact
allowed-skill set produced by role and policy resolution. The three built-in
MVP roles resolve to an empty allowed set by default.

For every discovered skill not explicitly allowed, the adapter calls
`skills/config/write` with its absolute path and `enabled: false`. If an allowed
skill is discovered but disabled by prior managed-home state, the adapter writes
`enabled: true`. Name-based writes are permitted only when the generated
response does not supply a path and the discovered name is unique. Missing,
duplicate, ambiguous, or out-of-worktree role-requested skills fail closed.

After all writes succeed, the adapter force-reloads `skills/list` again and
requires that the enabled skills are exactly the resolved set, identified by
canonical path and name. It persists both listings and every correlated write
as startup evidence. A `skills/changed` notification before the turn causes the
audit to repeat; one during an active turn invalidates the run because the
model-visible instruction set may have changed after policy validation.

This audit controls whether discovered skills are enabled. It does not make
skill discovery a security boundary or prove that an allowed skill's scripts
are safe. Discovery and all later skill-directed reads and execution stay inside
the startup/process sandbox and effective runtime policy.

### Read-only thread creation

The adapter sends one `thread/start` request with:

- the validated absolute worktree path as `cwd`, projected from the Repository
  Snapshot and Run Overlay rather than the live worktree;
- an explicit model and provider only when already resolved by the selected
  provider profile;
- `sandbox: "read-only"` regardless of the eventual editing role;
- the effective approval policy and `approvalsReviewer: "user"`;
- `personality: "none"` and `ephemeral: false`;
- resolved role, policy summary, artifact instructions, and task behavior in
  `developerInstructions`; and
- no `baseInstructions`, experimental permissions, runtime workspace roots,
  selected capability roots, environments, dynamic tools, or collaboration
  mode.

The read-only start prevents App Server's documented write-enabled
`thread/start` trust mutation. The outer sandbox is already active because
thread creation can read project instructions and discover project skills.

The adapter persists `thread.id` and `thread.sessionId` from the correlated
response and verifies that `parentThreadId` is null, `ephemeral` is false, and
the returned working directory, model/provider, sandbox projection, and
approval settings match the request. A `thread/started` notification is
expected but cannot substitute for the response. Any child relation or
unrequested effective setting fails startup.

`ThreadStartResponse.instructionSources` is a separate instruction channel from
skills. Before provider launch, the repository guard resolves the expected
canonical AGENTS instruction files in the sandbox projection. The adapter
preserves the returned `instructionSources` independently of the skill audit and
requires its canonical projected paths to match that expected set exactly. An
unexpected, missing, remote, or noncanonical instruction source fails startup.
The contents and precedence of approved AGENTS files remain governed by the
parent task and repository instructions; they are never disabled through
`skills/config/write`.

### Bounded turn and role sandbox

Only after thread validation does the adapter send `turn/start`. The request
contains:

- the persisted `threadId`;
- a deterministic `clientUserMessageId` derived from run and task identity;
- one bounded text input containing resolved requirements, context paths,
  acceptance criteria, and the artifact instruction;
- the role's effective `sandboxPolicy`: read-only for reviewer/verifier work,
  or workspace-write with validated writable roots and network disabled for an
  implementer;
- the effective approval policy and `approvalsReviewer: "user"`;
- a supported non-Ultra reasoning effort; and
- the resolved role's JSON `outputSchema`.

Codex workspace-write semantics may permit writes to the working directory in
addition to listed roots. The App Server sandbox therefore cannot express the
orchestrator's exact path-level write scope. The outer Repository Guard remains
authoritative: all writes stay in the Run Overlay and only a validated Publish
Delta can reach the live worktree.

The adapter persists the turn ID from the response before treating any later
event as turn evidence. Because notifications may arrive before the correlated
response, it buffers early turn notifications and replays them only after the
response establishes the expected turn ID. It requires `turn/started` for that
same thread and turn before mapping work, and it rejects unexpected overlapping
turns.

### Structured artifact

The role schema is supplied through `turn/start.outputSchema`; dynamic tools
and experimental host callbacks are not used. During the turn, the adapter
retains every completed `agentMessage` item. It prefers the completed message
whose `phase` is `final_answer`; because phase metadata is not guaranteed for
all compatible model paths, it otherwise uses the last completed agent message
for the turn.

At `turn/completed` with provider status `completed`, exactly one candidate
must parse as JSON and validate against the resolved role schema and task
identity. Missing, ambiguous, non-JSON, or schema-invalid candidates fail the
run. Delta text is never parsed as the authoritative artifact.

A valid candidate and `turn/completed` do not make the AgentRun successful.
They only permit the orchestrator to persist the candidate, collect transcript
and repository evidence, run the final scope check, and execute the
orchestrator-owned verification commands.

### Turn and event lifecycle

The adapter maps stable App Server evidence as follows:

| App Server evidence | Normalized meaning |
| --- | --- |
| `thread/started`, `turn/started` | Starting, then working or thinking |
| `item/started` for `commandExecution` | Running the reported command |
| `item/started` or `item/completed` for `fileChange` | Editing each reported path |
| `item/agentMessage/delta` and completed `agentMessage` | Transcript/progress evidence |
| orchestrator-owned verification command | Verifying |
| command/file/tool approval or `requestUserInput` | Waiting for input |
| `error` with `willRetry: true` | Nonterminal provider retry evidence |
| `turn/completed` with `completed` | Begin artifact and repository checks |
| `turn/completed` with `interrupted` | Provider cancellation evidence |
| `turn/completed` with `failed`, or terminal error | Provider failure |

The worker records every valid protocol frame in arrival order. Completed
items, not deltas, are authoritative for command exit status, file-change
contents, and final agent messages. `turn/diff` is retained as the latest
provider display diff but never replaces the Git baseline comparison.

Codex may internally retry a transient model call and report
`willRetry: true`; the orchestrator records that provider behavior but never
starts its own retry or a second turn. A terminal provider failure remains one
failed AgentRun.

The following evidence is a Managed-mode contract violation: a
`collabAgentToolCall`, `subAgentActivity`, child `thread/started`, MCP call or
elicitation, dynamic tool request, selected capability-root interaction, or
unmodeled permission request. Native children and provider-owned workflow
steps are not normalized as top-level progress.

### Approval and input requests

The adapter supports these stable server requests as durable blocked state:

- `item/commandExecution/requestApproval`;
- `item/fileChange/requestApproval`; and
- `item/tool/requestUserInput`.

Each request is correlated, persisted, and surfaced to the parent control path.
Only an explicit parent decision may produce `accept`, `decline`, or `cancel`.
The Managed MVP never sends `acceptForSession`, execution-policy amendments,
network-policy amendments, or session permission grants. A task that asks for
an unresolved product or architecture choice remains blocked rather than being
answered by the worker.

Unexpected MCP elicitation, dynamic-tool invocation, attestation, token refresh,
or permission-elevation requests fail closed as an authentication, input, or
unsupported-capability blocker. Cancellation responds to every pending
server request with the narrowest supported cancellation/decline outcome before
provider shutdown.

### Session, transcript, and native identity

The adapter persists the native thread ID and session ID immediately after
`thread/start`. If App Server supplies a persisted path, it is retained as
diagnostic evidence rather than treated as a stable public API. The stored
identity also records Codex CLI version and executable path.

After terminal turn evidence, the adapter calls `thread/read` with
`includeTurns: true` and stores that response as the native terminal snapshot.
The raw inbound/outbound JSONL log remains the canonical full evidence because
the stored thread may omit transient deltas or live items. Provider stderr is
stored separately.

The MVP never invokes `thread/resume`, `thread/fork`, `turn/steer`, review mode,
compaction, rollback, realtime APIs, shell-command APIs, process APIs, or
filesystem APIs. It never adopts a recorded thread after worker loss or cold
Herdr restart. A parent may create a new run from a structured handoff artifact;
that is not native thread continuation.

At normal shutdown, the adapter calls `thread/unsubscribe`, closes App Server
stdin, and requires the process to exit within the lifecycle-policy grace
period. The locally verified `0.144.5` initialize, initialized, and EOF sequence
exited with status `0`; a different result is preserved as compatibility
evidence and fails the run.

### Cancellation and process cleanup

Cancellation uses this cooperative order:

1. persist cancellation intent before writing to App Server;
2. if `turn/start` is pending, remember the intent and obtain the turn ID from
   either its response or matching notification;
3. send one ID-correlated `turn/interrupt` with the thread and turn IDs;
4. treat its empty response only as acknowledgement;
5. wait for matching `turn/completed` with `status: "interrupted"` or until the
   lifecycle-policy grace period expires;
6. decline/cancel outstanding server requests and capture transcript and
   repository evidence that remain available;
7. unsubscribe, close stdin, and wait for App Server; and
8. terminate and reap the run-owned process group if any provider or descendant
   process remains.

Repeated cancellation is idempotent at the orchestrator boundary and must not
send concurrent interrupts. If normal completion races cancellation, the
lifecycle contract decides terminal precedence while retaining both pieces of
evidence. Because turn interruption does not kill background terminals, the
outer process group and sandbox remain mandatory through final reap; the MVP
does not enable experimental background-terminal cleanup methods.

### Failure handling

The provider run fails without orchestrator retry when any of these occurs:

- Codex version mismatch, managed-home/authentication failure, spawn failure,
  startup timeout, or initialize-home mismatch;
- non-JSON stdout, a malformed message, duplicate initialization, an unknown
  response ID, or a response whose result does not match its pending request;
- thread or turn response/notification identity mismatch, non-null parent
  thread, unexpected overlap, or unrequested effective settings;
- an unexpected native child, collaboration activity, MCP/dynamic tool,
  experimental API, permission elevation, or provider-owned workflow action;
- terminal provider completion without exactly one schema-valid artifact;
- EOF, stdout closure, or process exit before terminal collection and
  repository reconciliation;
- failure to interrupt, unsubscribe, close, terminate descendants, or reap the
  provider within the lifecycle contract; or
- provider behavior that changes repository state outside the effective
  sandbox or declared write scope.

App Server overload and model errors may be retryable at the protocol/provider
level, but the Managed MVP does not create a second App Server, thread, or turn
automatically. It preserves all logs, transcript fragments, provider state,
Run Overlay state, publication or quarantine evidence, and correlation evidence
for parent inspection. It never automatically reverts provider changes.

## Required repository-safety dependency

The Managed Codex vertical slice must not be enabled merely because the adapter
implements this handshake. Before App Server starts, the
[Managed repository safety contract](repository-safety-contract.md) must pass
its fail-closed Linux capability probes and provide an OS-enforced process
boundary that:

- contains App Server and every command, skill-invoked script, terminal, and
  descendant process it creates;
- prevents writes outside the exact effective write scope and run-owned state,
  including writes permitted more broadly by Codex workspace-write semantics;
- blocks unapproved network access and host resources independently of model
  approval behavior;
- remains active during initialization, project instruction and skill
  discovery, turn execution, transcript collection, and process-group reap;
  and
- preserves the user's pre-existing Git state for final baseline comparison.

Codex sandboxing and managed-home configuration are defense-in-depth, not the
repository security boundary. In particular, no complete stable provider tool
allowlist exists in `0.144.5`, and repository skills remain discoverable. Until
the linked contract is implemented and proved, Managed Codex runs must return a
clear unsupported or safety-prerequisite error.

## Downstream implementation obligations

- **Repository safety contract:** implement and prove the linked startup-time
  filesystem, network, credential, descendant-process, and publication
  boundary.
- **Provider adapter:** generate and pin the `0.144.5` schemas, model the stable
  handshake and accepted frames as Rust types, maintain request/server-request
  correlation, and keep native types inside the Codex module.
- **Role and policy resolution:** produce the effective developer instructions,
  turn sandbox, approval policy, reasoning effort, model profile, environment
  allowlist, exact enabled-skill set, expected AGENTS instruction sources, and
  role artifact schema.
- **Artifact protocol:** define role payloads, validate the final completed agent
  message, and atomically persist the candidate before repository checks.
- **Lifecycle and recovery contract:** define grace periods, completion/cancel
  race precedence, worker-loss reconciliation, and process-group reap behavior.
- **Public status contract:** distinguish provider-internal retry, durable
  blocked approvals, interrupted turns, invalid scope, and failed provider
  sessions without exposing native protocol details.
- **Herdr integration test:** prove that sanitized environment plus disabled
  hooks prevents official Codex lifecycle and session-reference reporting.
- **Compatibility suite:** retain captured `0.144.5` initialization, thread,
  turn, item, approval, structured-output, interrupt, transcript, EOF,
  config-warning, and failure fixtures before accepting another Codex version.

## Acceptance scenarios

- An isolated `0.144.5` App Server accepts the exact initialize/initialized
  handshake, returns the canonical managed `codexHome`, and persists runtime
  identity before `thread/start`.
- A managed-home probe confirms that untrusted project config, hooks, and exec
  policy are disabled while the repository-skill `configWarning` is retained
  and skill-triggered execution remains inside the outer sandbox.
- Before thread creation, force-reloaded skill discovery disables every
  unapproved skill in the run-owned home, re-enables only explicitly allowed
  skills, and a second listing matches the resolved set exactly; built-in roles
  expose no enabled skills by default.
- A thread starts read-only, has no parent, remains persisted, and does not mark
  the worktree trusted before the role-specific turn sandbox is supplied; its
  AGENTS `instructionSources` separately match the repository guard's expected
  canonical set.
- Interleaved responses, notifications, and server requests resolve through IDs
  without corrupting thread, turn, or blocked-state identity.
- A turn with the role `outputSchema` cannot complete the AgentRun until one
  completed final agent message validates and repository and verification
  checks also succeed.
- Command, file-change, message, retry, diff, and terminal notifications map to
  normalized evidence without treating deltas as authoritative results.
- A command, file, or user-input request blocks without implicit approval;
  session-wide acceptance and policy amendments are rejected.
- Any collaboration item, child thread, MCP call, dynamic tool request, Ultra
  behavior, or experimental capability fails closed in Managed mode.
- Cooperative cancellation persists intent, sends one interrupt after obtaining
  the turn ID, waits for interrupted completion, settles pending requests,
  captures evidence, and reaps the entire process group.
- A background command that survives `turn/interrupt` is terminated by the
  run-owned process group, proving provider interruption is not the cleanup
  boundary.
- Terminal collection stores `thread/read(includeTurns = true)` plus raw JSONL
  and stderr, while cold recovery never resumes or adopts the native thread.
- Initialize followed by `initialized` and EOF exits cleanly; unexpected EOF or
  process loss fails the run and still triggers repository reconciliation.
- A write attempted outside the declared path scope is contained when possible,
  detected by the final Git comparison, preserved for inspection, and marks the
  run invalid without automatic reversion.
- A real Herdr worker pane confirms that the sanitized Codex child emits no
  official Herdr status or native restore reference.
