# Live acceptance evidence: 2026-07-19

Release classification: `live-acceptance-partial`.

## Environment

- Candidate commit: `ffe2a2d`.
- Release binary SHA-256: `67c289b35c4b83c896d3c66c14b100a94bc9a8222179720d46da0414756df380`.
- Herdr: `0.7.4`.
- OMP: `17.0.4` using `kimi-code/k3:high`.
- Codex: `0.144.5` using `gpt-5.6-sol`.
- Installed binary: `/home/hewel/.local/bin/herdr-harness-coordinator`, linked to the repository release build.
- Plugin: `/home/hewel/Codes/herdr-harness-coordinator/plugin/herdr-harness-coordinator`.

## Flow A: OMP Supervisor to Codex Worker

- State directory: `/tmp/herdr-harness-live.ZRQ8Cu/workspaces/1a68d2596446eed89fc08c0a9094322ab7501e89101499d539b260d9a17550d2`.
- Durable projection: `/tmp/herdr-harness-live.ZRQ8Cu/flow-a-evidence.json`.
- Herdr workspace `wG`; Supervisor pane `wG:pC`, terminal `term_656f0403398c347`; Worker pane `wG:pD`, terminal `term_656f04585a92848`.
- Coordinator Supervisor Session `019f76fe-640a-7201-b31a-51a2f801d481`; native OMP Session `019f76fe-68e3-7000-8893-406bf13b1e95`.
- Task `019f78e7-94d4-7231-8ae1-6a8de4d816f8`; Coordinator Worker Session `019f78e6-ca21-7361-9fca-12aac544ad9c`; native Codex thread `019f78e6-cbcb-7fe3-abf6-a143b7c9bcf5`.
- Revision 0 native turn `019f78e7-9690-7b13-9f95-5f3a683303ec`; ResultReady Event `019f78e9-4957-70e1-b4c5-880d20197f2f`; attempt `019f78e9-49ed-7952-a7fe-01f1359f3ad4`, accepted and then processed by Correction.
- Correction retained the same Task, Coordinator Worker Session, and native Codex thread. Revision 1 used native turn `019f78ec-0eb4-7df1-bc25-0dbd851cdc3a`; ResultReady Event `019f78ec-a782-7b13-802c-1529720fa267`; attempt `019f78ec-a83b-7b22-8c08-858a514adf4c`, accepted and then processed by Approval.
- Final state: `approved`, revision 1. Repository authority was released only after Approval.

The first live attempt exposed two defects that now have regression coverage: Codex
MCP submissions supplied a thread/candidate correlation that did not equal the App
Server terminal turn, and the generated prompt required a turn ID unavailable to a
long-lived MCP child. The Worker Host now binds the candidate to terminal evidence,
and the prompt explicitly tells providers not to invent or search for a turn ID.

## Flow B: Codex Supervisor to OMP Worker

- State directory: `/tmp/herdr-harness-live.knLkOW/workspaces/d36da6d12ffa3eee8b2ff7a59b09a050ccbd5512120e7c0b3c70f02b600dc6e2`.
- Durable projection: `/tmp/herdr-harness-live.knLkOW/flow-b-evidence.json`.
- Herdr workspace `wH`; Supervisor pane `wH:pT`, terminal `term_656f0a1387c2588`; current OMP Worker pane `wH:pV`, terminal `term_656f0a51766ea8b`.
- Coordinator Supervisor Session `019f78f1-af16-7c31-90da-b9e41077a036`; native Codex thread `019f78fe-44e3-7bf3-b3e5-5ab74dec0164`.
- Supervisor-created Task `019f7900-392b-7bb0-99c4-5b9d699134d5`; Coordinator Worker Session `019f78ff-415c-7253-b07f-b71e40509574`; native OMP Session `019f78ff-45dd-7000-8aba-81f02deca15d`; final state `approved`, revision 0.
- The Result arrived while the original Supervisor turn was busy. It stayed durable and queued; no native injection interrupted the turn. The Supervisor read the Inbox, acknowledged the queued events, reviewed the Result, and asked Approval or Correction.
- Idle-injection Task `019f7903-8f76-7041-9f16-f17208498dd6`; Coordinator Worker Session `019f7903-9171-7b22-ba9b-5641e4c540e8`; native OMP Session `019f7903-95d6-7000-bb0b-8441e33191ad`.
- ResultReady Event `019f7904-8319-7793-966a-ed7417bf355a` moved pending to accepted on attempt `019f7904-83c1-7f73-8162-1354c67bf379`. Codex accepted new turn `019f7904-83c3-7b82-9ea3-80809fc3d0a0` in the existing thread.
- Durable observations prove `turn_started`, exact Event-ID `presented`, and `turn_completed` in that same native thread. The new review turn acknowledged the Event, asked Approval or Correction, and the later explicit Approval released repository authority.

Flow B exposed a managed-Supervisor startup defect. Workspace activation persisted
Codex `approval_policy` and `sandbox_mode`, but the Supervisor Host filtered those
values before adapter binding. The Host now loads the allowlisted policy from the
0600 durable `managed-supervisor.json`, so reconnect does not depend on pane
environment propagation.

## Recovery and reuse rejection observed

In Flow A, an idle managed OMP Supervisor Host was sent SIGKILL. Its presence lease
expired, the Session became disconnected/reconnecting, the pending accepted event
became `unknown`, and no second injection attempt was created. After visible native
conversation inspection, OMP explicitly reconciled it as processed. The exact native
OMP Session was resumed.

The Worker Host loss ended Coordinator Worker Session
`019f76fe-6476-7570-99b2-afaaac3af2dd`, rejected reuse of native Codex thread
`019f76fe-65db-7923-a54b-4bc31aeb95a4`, created a Worktree Hold, and required a
fresh compatible Session. This is the live unsafe-reuse rejection case.

## Remaining release blockers

- The full real-provider DAG matrix (fan-out, fan-in, FIFO/concurrency, both failure policies, revision revocation, and cycle rejection) is not yet captured live in one reproducible run.
- Worker Host SIGKILL with an active mutating Task and blocked downstream Task has local regression coverage but no completed live acceptance artifact from this run.
- Pane and terminal IDs are captured from Herdr evidence but remain null in `harness_sessions`; durable pane-location recording is still missing.
- The controlled-handoff helper rejects a live process whose on-disk binary was rebuilt in place because `/proc/<pid>/exe` resolves with a deleted-image suffix. This is safe failure, but release handoff should install immutable versioned binaries instead of replacing a running target.
- The managed Codex pane does not receive `HERDR_ENV=1`, so the general Herdr skill declines pane-control operations. Identity-bound Coordinator MCP tools still work; the mismatch is a Supervisor UX limitation.

The candidate is not production-ready until these blockers and the complete live DAG
and crash matrix are accepted.
