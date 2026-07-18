# Codex App Server Harness Adapter contract

Status: resolved for the Harness Coordinator MVP.

This contract defines how a pane-resident Harness Host starts and controls one autonomous Codex Worker Harness, maintains its thread across top-level Tasks, delivers FollowUps and Steers, captures Result evidence, and leaves Codex-native collaboration private. Compatibility is established by App Server behavior, not a Codex CLI release pin.

## Confirmed App Server facts

- `codex app-server --listen stdio://` uses newline-delimited JSON messages without a `jsonrpc` member.
- Requests, responses, notifications, and server requests share the stream and may interleave.
- The client sends one `initialize` request, waits for its response, then sends `initialized` before other methods.
- `thread/start` creates a native thread. `turn/start` creates one turn, but its response is not completion.
- `turn/steer` appends input to an in-flight turn. `turn/interrupt` requests cancellation.
- Item and turn notifications provide the live transcript, command, file-change, MCP, collaboration, and completion evidence.
- `thread/read` returns a durable thread snapshot, while live events remain necessary for complete evidence.
- App Server can generate version-specific JSON Schemas; another CLI version may change the protocol.
- Codex `0.144.5` supports native multi-agent activity. The Coordinator does not require or model its child topology.

The current upstream overview is the [Codex App Server manual](https://learn.chatgpt.com/docs/app-server.md).

## Launch

The Harness Host requires bounded nonempty `codex --version` output, records it, and launches:

```text
codex app-server --listen stdio:// --strict-config
```

The selected Worker launch profile owns model, reasoning, sandbox, approvals, tools, skills, plugins, MCP servers, hooks, and native multi-agent configuration. The Coordinator records effective values reported by App Server but does not rewrite them into a cross-provider policy.

A compatible newer release proceeds when initialization, thread creation, correlation, and required Coordinator MCP-tool visibility succeed. Missing behavior fails closed regardless of the version string.

The process receives normal Herdr context so the official Codex integration may remain the pane's semantic status authority. The Coordinator's same-user cooperative threat model applies.

## Initialization and thread

The adapter:

1. starts the process and captures stderr separately;
2. sends one ID-correlated `initialize` with client name `herdr_harness_coordinator`;
3. validates the response and sends `initialized`;
4. starts one non-ephemeral thread in the registered live worktree;
5. records thread ID, session ID, effective cwd, model, sandbox, and approval settings; and
6. reports the Harness Session online and idle.

Every request has a unique nonempty ID and a pending-request entry. Early notifications are buffered until the correlated response establishes their thread or turn identity.

The Worker profile configures the local Coordinator MCP bridge. Its tools are identity-bound to this Harness Session. Missing required Coordinator tools fails Harness startup, while additional native tools and collaboration capabilities are allowed and recorded.

After `thread/start` or `thread/resume`, the adapter calls
`mcpServerStatus/list` when the installed App Server supports it and requires the
tier-specific Herdr Coordinator tools. An explicit JSON-RPC method-not-found
response is retained as older-server compatibility evidence; a supported method
that omits required tools fails closed. Shell fallback is not production success.

## Delivery

| Coordinator condition | App Server operation |
| --- | --- |
| New eligible Task while idle | `turn/start` in the existing thread |
| FollowUp after the active turn settles | next `turn/start` |
| FollowUp submitted during an active turn | retain in Coordinator queue until completion |
| Explicit Supervisor Steer for active Task | `turn/steer` |

The Coordinator never schedules overlapping top-level turns. Provider-native child activity inside one turn is opaque and permitted.

The adapter persists the outbound request before writing it. A successful response establishes native acceptance, but Task completion requires matching `turn/completed`. Transport loss after bytes may have been accepted becomes delivery `unknown`.

## Coordinator tools and Result

The configured MCP bridge exposes the Worker route:

```text
harness_list
harness_status
harness_inbox
harness_request
harness_send
harness_complete
```

Calls are attributed to the containing Worker Harness, including calls made by a native child. The top-level turn must produce one valid `ResultManifestV1` through `harness_complete`.

The shared MCP surface may advertise the Supervisor-only `harness_task_graph` query, but a Worker capability cannot invoke it. Dependency inputs arrive only as immutable Attachment references on the assigned Task; dependency edges never open a Worker-to-Worker route.

The broker stores the Result candidate immediately. The Task moves to `reviewing` only after matching `turn/completed` with completed status and a successful Result Repository Observation. A terminal turn without one valid Result fails the Task.

## Events

The adapter retains every valid frame in arrival order and normalizes top-level evidence:

| App Server evidence | Coordinator meaning |
| --- | --- |
| `thread/started`, `turn/started` | Harness or Task started |
| command and file-change items | display-only activity and evidence |
| agent-message items | transcript and final natural-language output |
| MCP items | Coordinator or native tool evidence |
| collaboration or subagent items | opaque native evidence |
| approval or input request | Worker waiting for Supervisor or native handling |
| `turn/completed` | top-level turn settled |
| terminal error or process exit | adapter failure |

The Coordinator does not register child threads, expose native subagent trees, route child messages, or make native collaboration a top-level workflow.

After completion, the adapter calls `thread/read` with turns included and stores the snapshot alongside raw JSONL and stderr logs.

## Questions, corrections, and reuse

A blocking Question persists through the MCP bridge and moves the Task to `waiting`. The Supervisor Reply becomes a new turn in the same thread after the current turn settles. A Correction from `reviewing` also creates a new turn and Result revision.

After Approval, another Task may use the same thread sequentially only when the Task's Session reuse policy and candidate checks admit it; the coordination contract's Session reuse rules are authoritative. The Task attachment and message remain the authoritative bounded assignment even though the native thread retains history.

The Coordinator does not automatically resume or adopt a thread after Worker Host loss. A deliberate Worker restart creates a new Harness Session and thread under the same durable Harness identity.

A managed Codex Supervisor reconnect is narrower: it sends `thread/resume` with
the exact durable thread ID and the selected cwd, model, approval policy, and
sandbox policy. The returned thread must match exactly before event injection can
resume. Unsettled Supervisor delivery remains `unknown` and is never replayed by
the resumed thread without explicit reconciliation.

## Cancellation and shutdown

Cancellation:

1. persists intent and current thread/turn identity;
2. sends one correlated `turn/interrupt`;
3. waits for matching `turn/completed` with interrupted status;
4. captures pending server requests, transcript, thread snapshot, and cancellation Repository Observation;
5. closes App Server stdin and waits for process exit; and
6. escalates through Herdr pane closure when the grace period expires.

Repeated cancellation is idempotent. Background terminals and native descendants must be reaped with the Harness Host process; an uncertain stop creates Worktree Hold.

## Compatibility tests

Real-process fixtures must generate or compare the installed App Server schemas and prove initialization, persistent thread creation, consecutive turns, FollowUp queuing, `turn/steer`, MCP tool calls, native collaboration tolerance, structured completion, thread snapshot, interruption, EOF, and process cleanup. Version text alone never grants or denies compatibility.
