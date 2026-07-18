# OMP RPC Harness Adapter contract

Status: resolved for the Harness Coordinator MVP.

This contract defines how a pane-resident Harness Host starts and controls one autonomous OMP Worker Harness, delivers Coordinator messages, captures top-level lifecycle and Result evidence, and leaves OMP-native multi-agent behavior private. Compatibility is established by runtime protocol behavior, not an OMP release pin.

## Confirmed OMP facts

- `omp --mode rpc` is a long-lived JSONL process over stdin and stdout and emits `{"type":"ready"}` before accepting commands.
- Commands accept optional IDs and responses echo them. Responses, events, host-tool calls, and extension UI requests may interleave, so the adapter must frame by line and correlate by ID.
- `prompt` acknowledgement proves scheduling, not terminal completion. An invoked turn settles at `agent_end`.
- When streaming, OMP exposes distinct `steer` and `follow_up` commands. `abort` cooperatively stops active work.
- `get_state` reports streaming and queue state plus native session identity; `get_messages` returns normalized conversation history.
- `set_host_tools` installs host-owned tools and correlates calls, results, updates, and cancellation.
- OMP can create native children through its own tool surface and can optionally emit subagent progress or event streams.
- OMP constructs its session and discovers configured extensions, MCP servers, custom tools, skills, and rules before the RPC `ready` frame.
- Closing stdin drains accepted commands, disposes the session, and normally exits with status `0`.

The versioned upstream reference is [OMP RPC v17.0.2](https://github.com/can1357/oh-my-pi/blob/v17.0.2/docs/rpc.md).

## Launch

The Harness Host requires bounded nonempty `omp --version` output, records it, and launches one process with:

```text
omp
  --model <explicit-selected-model>
  --mode rpc
  --cwd <registered-live-worktree>
  --session-dir <harness-session-state>/provider-session
  --config <optional-selected-overlay>
```

`--profile` is included only when the launch profile explicitly selects a native profile. Omitting it deliberately uses the user's existing OMP default profile. A compatible newer OMP release proceeds; missing `ready`, `set_host_tools`, correlation, or `get_state` behavior fails closed.

The launch profile, not the Coordinator, chooses model, approval behavior, native tools, extensions, skills, rules, MCP configuration, and multi-agent behavior. The selected configuration is recorded with the Harness Session.

The process receives normal Herdr context so the official OMP integration may remain the pane's semantic status authority. The Coordinator never claims that this environment isolates the Worker from the live worktree, user credentials, other same-user processes, or network access.

## Startup

The adapter:

1. requires one valid `ready` frame within 30 seconds;
2. assigns a unique nonempty ID to every command;
3. installs Coordinator host tools or verifies the configured MCP bridge;
4. calls `get_state` and records native session ID, session file, active tool names, queue modes, and model; and
5. reports the Harness Session online and idle only after every required response succeeds.

Unexpected tool names are recorded but are not a contract violation: OMP is an autonomous Harness and may expose `task`, `hub`, custom tools, and MCP tools. The adapter does not call `set_subagent_subscription` for Coordinator presentation in the MVP.

## Delivery

The Coordinator resolves delivery intent before calling the adapter.

| Coordinator condition | OMP command |
| --- | --- |
| New eligible Task while idle | `prompt` |
| FollowUp after the active turn settles | `prompt` |
| FollowUp intentionally queued during the same active Task | `follow_up` |
| Explicit Supervisor Steer for the active Task | `steer` |

The adapter persists the outgoing command and correlation before writing stdin. A successful command response becomes native acceptance evidence. EOF, timeout, or connection loss after bytes may have been written becomes delivery `unknown`, not an automatic retry.

OMP native queues never schedule a second top-level Task. The Coordinator retains later Tasks until the Worker is idle and repository authority is eligible.

## Coordinator tools

The host-tool or MCP bridge exposes the identity-bound Coordinator operations allowed to a Worker:

```text
harness_list
harness_status
harness_inbox
harness_request
harness_send
harness_complete
```

Worker routes are limited to the Supervisor. Native descendants share the containing Harness Session capability and are therefore attributed to the Worker Harness. The top-level OMP prompt requires one consolidated `harness_complete` call, but this is a cooperative contract rather than child-process isolation.

The shared MCP surface may advertise the Supervisor-only `harness_task_graph` query, but a Worker capability cannot invoke it. Dependency inputs arrive only as immutable Attachment references on the assigned Task; dependency edges never open a Worker-to-Worker route.

A blocking `harness_request` persists a Question and moves the Task to `waiting`. `harness_complete` validates `ResultManifestV1` and stores a candidate Result. Neither tool alone completes the native turn.

## Events and completion

The adapter normalizes only top-level evidence:

| OMP evidence | Coordinator meaning |
| --- | --- |
| `agent_start`, `turn_start` | active Task working |
| message events | transcript or progress |
| tool execution events | display-only activity |
| extension UI request | native input request requiring Supervisor handling or failure |
| `agent_end` | top-level turn settled |
| provider error, EOF, exit | adapter failure |

Native child events, hub traffic, and child transcripts stay in raw provider logs. They never create Coordinator Tasks, Sessions, or messages automatically.

At `agent_end`, exactly one accepted Result for the current Task revision is required. The adapter then calls `get_messages`, stores the top-level transcript and latest state, and reports terminal turn evidence. The Coordinator captures the Result Repository Observation before transitioning the Task to `reviewing`.

## Questions, corrections, and reuse

A blocking Question normally ends or pauses the active OMP turn. When the Supervisor Reply becomes eligible, the adapter delivers it in the same native session and Task conversation. A Correction after `reviewing` starts another native turn in that session and increments the Result revision.

After Approval the Worker may remain idle and receive another top-level Task in the same OMP session only when the Task's Session reuse policy and candidate checks admit it; the coordination contract's Session reuse rules are authoritative. A failed, forcibly cancelled, or ambiguous session is stopped and cannot receive another Task.

For a managed Supervisor Host reconnect, the adapter launches OMP with
`--resume <durable-native-session-id>` and requires the first correlated
`get_state` response to report that exact Session. Identity drift fails closed.
This recovery resumes only the visible managed Supervisor conversation; a lost
Worker Host does not automatically replay native work.

## Cancellation and shutdown

Cancellation:

1. persists intent;
2. sends one correlated `abort`;
3. accepts the abort response or `agent_end` in either order;
4. polls `get_state` until `isStreaming` is false within the configured grace period;
5. captures messages and a cancellation Repository Observation; and
6. closes stdin and waits for normal exit.

Repeated cancellation is idempotent. If cooperative cancellation fails, the Herdr integration closes the Worker pane. Any possibly mutating Task then enters Worktree Hold.

## Compatibility tests

Real-process fixtures must prove ready framing, correlation under interleaved events, prompt acceptance versus completion, FollowUp, Steer, host-tool execution and correlation, native child tolerance, Result completion, transcript collection, abort, and normal shutdown. Version text alone never grants or denies compatibility.
