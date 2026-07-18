# Live acceptance

These scripts intentionally keep paid-provider runs outside normal CI. They require
Herdr, OMP, Codex, SQLite, and authenticated provider installations.

```bash
./scripts/live-acceptance/setup.sh
./scripts/live-acceptance/omp-supervisor-codex-worker.sh
./scripts/live-acceptance/codex-supervisor-omp-worker.sh
./scripts/live-acceptance/capture-evidence.sh "$WORKSPACE_STATE_DIR"
```

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
