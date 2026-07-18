# Live acceptance evidence: 2026-07-18

Environment: Herdr 0.7.4, OMP 17.0.4, Codex 0.144.5, Rust 1.96.1.
Repository baseline: `48221a6f34ea44b4f33c617d4a891cde1565dcbd`.

## OMP Supervisor to Codex Worker

State root: `/tmp/herdr-harness-flow-a7.VhZ8sx`.

- Herdr Supervisor: tab `wF:t0`, pane `wF:pT`, terminal `term_656e5702b58102a`.
- Herdr Worker: tab `wF:t11`, pane `wF:pV`, terminal `term_656e5702c36572b`.
- Task: `019f7621-baea-7ff3-bb46-76a7c1f6d93e`, approved revision 0.
- Coordinator Supervisor Session: `019f7620-cf7a-7c63-8dcc-ad8d03fed120`.
- Native OMP Supervisor Session: `019f7620-d44d-7000-bb02-547e58ffc9e0`.
- Coordinator Worker Session: `019f7620-cfe9-7511-bb56-1ae6f7c98e2b`.
- Native Codex thread/session: `019f7620-d134-72c0-a910-e50adb3fcea5`.
- Native Codex turn: `019f7621-bca4-7880-b83d-e24fd5664452`.
- Supervisor Event: `019f7622-8c44-7c02-b4ef-80f714e39a4e`.
- Delivery: pending to accepted on attempt 1, native evidence `OMP accepted follow_up: null`; event later processed with Approval.
- Attachment: `019f7622-59fd-7833-8405-71de6750f7b6`.

## Codex Supervisor to OMP Worker

State root: `/tmp/herdr-harness-flow-b2.4Fzvef`.

- Herdr Supervisor: tab `wF:t14`, pane `wF:pY`, terminal `term_656e5a5b30a072e`.
- Herdr Worker: tab `wF:t15`, pane `wF:pZ`, terminal `term_656e5a5b3e8802f`.
- Task: `019f7633-3448-7252-a67e-a7d2ddd5ac0e`, approved revision 0.
- Coordinator Supervisor Session: `019f762e-839f-7450-86b8-5002e9265e14`.
- Native Codex Supervisor thread/session: `019f762e-8508-7401-a9bc-0bda819a04ba`.
- Task-creation Codex turn: `019f7632-b66f-7ad3-ace8-2611eac4954d`.
- Automatic review Codex turn in the same thread: `019f7634-3e88-7711-a6f8-f5b6264739d3`.
- Coordinator Worker Session: `019f762e-840e-7081-a805-385e4b768035`.
- Native OMP Worker Session: `019f762e-8966-7000-8bdc-a4afe103393c`.
- Supervisor Event: `019f7634-3e4b-7ba1-910c-cf630272a38a`.
- Delivery: pending to accepted on attempt 1 with `Codex accepted turn/start`; event later processed with Approval.
- Attachment: `019f7633-79d0-7d01-aca8-a2d85f4235da`.

The first Codex Supervisor prompt incorrectly routed itself through the pane-control
skill because `HERDR_ENV` was absent. A follow-up explicitly selected the already
available identity-bound MCP stdio bridge. This is a live UX/instruction limitation,
not a durable delivery failure.

## Correction reuse

State root: `/tmp/herdr-harness-flow-b2.4Fzvef`.

- Task: `019f7638-281c-76e2-9e96-0e5c27f848b9`.
- Coordinator Worker Session before and after: `019f7638-28a9-7e73-a01b-f1580d6f464f`.
- Native OMP Session before and after: `019f7638-2d40-7000-a9ef-60a7e8232d10`.
- Revision 0 ResultReady Event: `019f763a-9e3b-7c73-a65c-d55a8a51ae16`.
- Correction Message: `019f763b-2d10-7543-90a7-c71a14d54294`.
- Revision 1 ResultReady Event: `019f763b-dbc5-7b03-b27a-dcfbc1eda129`.
- Task transitioned reviewing revision 0 to working revision 1, then reviewing and approved revision 1.
- Repository authority remained on the same Task through final Approval.
- Final provider evidence file: `scripts/live-acceptance/provider-proof.txt`, containing `revision two` plus newline.

## Live failures that produced fixes

- Cached Herdr plugin metadata omitted `supervisor` until unlink/link and config reload.
- Supervisor pane cwd overrode plugin-root entrypoint resolution.
- Herdr reserved `HERDR_PLUGIN_STATE_DIR`, so per-workspace state was moved to `HERDR_COORDINATOR_STATE_DIR`.
- Activation-spawned daemon inherited caller lifecycle and died; it now receives a process group and durable stderr log.
- Codex App Server rejected `--profile`; v3 launch policy now uses `thread/start`.
- Codex `workspace-write` rejected Coordinator Unix-socket IPC with `EPERM`; the cooperative live profile explicitly selects `danger-full-access`.
- OMP reported model alias `k3`, causing 34 same-profile Session rotations for one queued Task in failed state root `/tmp/herdr-harness-flow-b1.8r9UEg`. The adapter now retains the explicit selected model and fresh incompatibility cannot request unbounded rotation.

## Not yet accepted

- busy Supervisor queuing was observed for OMP follow-up acceptance, but a dedicated progress-only non-wake proof is not recorded;
- unsafe reuse rejection has unit coverage but no completed live rejection scenario;
- the full DAG matrix, restart matrix, Worker SIGKILL semantics, and stale Supervisor presence expiry remain unproven live;
- managed presence still has no expiring heartbeat lease, so Supervisor SIGKILL can leave `online` state stale;
- terminal and pane IDs are captured from Herdr but are not yet persisted into `harness_sessions`;
- Codex Coordinator tools are reachable through its orchestration/MCP bridge, but direct discoverability was inconsistent and caused CLI fallback exploration.

Release classification: `live-acceptance-partial`.
