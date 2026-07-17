# Herdr Agent Orchestrator

## Project direction

- Build a Rust Herdr plugin that runs, supervises, and coordinates coding agents in normal Herdr-managed terminal panes.
- The parent agent retains ownership of intent, architecture, decomposition, acceptance criteria, verification design, final diff review, and the user-facing response.
- Child agents receive bounded tasks with resolved requirements, declared write scopes, and objectively verifiable completion criteria.
- Treat `docs/ARCHITECTURE.md` as the product source of truth and `CONTEXT.md` as the canonical domain vocabulary. Keep this guide concise; do not duplicate the full design here.

## Architectural boundaries

- Herdr Agent Orchestrator is the single top-level workflow authority. Provider-native orchestration may only appear later as a controlled child capability.
- Keep provider, role, task, and policy independent. OMP and Codex are MVP providers; Pi and OpenCode are later providers, not hard-coded responsibilities.
- Hide provider-specific protocols behind a shared Rust adapter interface. Shared runtime code consumes normalized events and structured artifacts, not native protocol messages.
- Prompts describe role behavior; execution policies enforce permissions. Read-only and write-scope rules must be runtime constraints, not prompt-only requests.
- Exchange structured run specifications, reports, and handoff artifacts instead of relying on cross-provider conversation history.
- The child-agent pane owns the real process. Popups display and control runs but must not own agent lifecycles.
- Official Herdr integrations may remain authoritative for native provider identity and protocol lifecycle, but this plugin owns top-level workflow, task, role, policy, repository safety, and verification state.

## Repository safety

- Treat `docs/research/mvp/repository-safety-contract.md` as the normative Managed-runtime safety boundary.
- Inspect and preserve the user's existing repository state before every editing run.
- Capture a Repository Snapshot, validate declared scopes, and acquire a Worktree Lease before execution.
- Run providers against a private Run Overlay; only a sealed, validated Publish Delta may reach the live worktree.
- Permit only one editing workflow per worktree. Read-only verification may run concurrently when it cannot interfere with edits.
- Quarantine uncertain publication and never automatically revert, merge, or discard unexpected modifications.
- Reject unresolved architecture choices in child tasks; return them to the parent agent for a decision.

## MVP boundaries

- Require explicit provider and role selection. Initial providers are OMP and Codex; built-in roles are implementer, reviewer, and verifier.
- Use Managed delegation mode for the MVP. Include structured run specs and artifacts, repository guards, verification commands, normalized lifecycle events, persistent SQLite state, Herdr metadata, a Ratatui popup, and cancel/focus/inspect controls.
- Keep role and policy concepts in the domain model from the start, even when initial role definitions are built in.
- Defer automatic routing, provider-native subagents, user-defined roles, inheritance, general workflow DAG execution, multiple editing agents, automatic merge/rollback, OpenCode, graphical or web UIs, distributed workers, and deep recursive delegation. Pi is the first provider after the MVP.

## Implementation order

1. Build the Managed runtime with OMP, Codex, built-in roles, repository guards, SQLite state, artifacts, Herdr metadata, and popup controls.
2. Add provider capability negotiation, persistent sessions, resume, steering, transcripts, handoffs, and isolated worktrees.
3. Add Pi as a managed provider.
4. Add policy-bounded OMP Hybrid mode with read-only native helpers first.
5. Add OpenCode, then custom roles and general DAG workflows.

The first milestone must prove the full Managed OMP path from parent submission through a bounded edit, verification, scope validation, structured artifact, popup result, and parent review while keeping provider-native subagents disabled.

## Rust and persistence conventions

- Use async Rust with Tokio and provider traits with `async-trait`; use Serde-backed versioned JSON/TOML types at process and disk boundaries.
- Use the Git CLI initially rather than `git2`.
- Store indexed runtime state in SQLite beneath `HERDR_PLUGIN_STATE_DIR`; store large artifacts, transcripts, logs, diffs, and patches as files.
- Keep provider protocol types inside provider modules and keep Herdr transport details behind the Herdr integration boundary.
- Prefer focused tests around domain validation, state transitions, repository guards, protocol translation, cancellation, and artifact construction.

## Change discipline

- Implement vertical slices in the recommended order; do not scaffold deferred subsystems speculatively.
- Preserve public and persisted schemas deliberately with explicit `schema_version` fields.
- Keep verification commands declared in the task packet and report their command, exit status, pass/fail result, and concise evidence.
- When runtime behavior matters, validate through the real process or Herdr boundary rather than treating compilation alone as completion.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues, and external pull requests are also a triage request surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The repository uses the five default triage labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repository. See `docs/agents/domain.md`.
