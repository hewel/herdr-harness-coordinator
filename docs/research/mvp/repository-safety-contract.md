# Managed repository safety contract

## Decision

Managed runs use a fail-closed Linux safety backend. Providers never receive a
live writable mount of the target worktree. The Repository Guard captures a
full immutable Repository Snapshot, executes the provider against a private Run
Overlay, validates the resulting Publish Delta, and publishes only after every
required review and verification boundary has succeeded.

The contract is authoritative for Managed OMP and Managed Codex. Provider
approval modes, prompts, native sandboxes, and final Git checks are
defense-in-depth; none replaces this boundary. The architectural reason for
staging writes before publication is recorded in
[ADR 0002](../../adr/0002-stage-managed-writes-before-publication.md).

## Guarantee boundary

The MVP guarantees that:

- provider startup, execution, tools, and descendants cannot write the live
  worktree or live Git metadata;
- provider writes cannot escape the private Run Overlay and run-owned state;
- the kernel enforces the narrowest available scope inside that overlay, and
  the Repository Guard rejects every exact-scope violation before publication;
- the host worktree is unchanged until publication begins;
- a candidate that violates scope, file-type, dirty-state, ignored-file, or
  destructive-change policy is invalid and is never published;
- a host change detected before the first publication mutation invalidates the
  run and publishes nothing; and
- a conflict or crash after publication begins stops publication, preserves
  evidence, and places the worktree in Repository Quarantine.

Linux does not provide an unprivileged transaction spanning multiple paths.
Consequently, the MVP does not claim that a multi-path publication is
all-or-nothing after its first host mutation. It uses a durable Publish Journal
and per-path compare-and-swap operations, never automatically rolls back a
partial publication, and makes uncertainty explicit through quarantine.

The enforcement split is deliberate:

| Boundary | MVP treatment |
| --- | --- |
| Live worktree, live Git metadata, host home, sockets, credentials, direct network, and orchestrator state | Prevent access through namespace, mount, credential-broker, and network construction before provider startup. |
| Writes outside the overlay, run-owned state, Scratch Scopes, and the kernel-expressible portion of write scope | Prevent through Bubblewrap, read-only mounts, Landlock, seccomp, and nested command policy. |
| Exact candidate scope, missing-target sibling creation, ignored output, dirty acknowledgement, destructive changes, file type, and metadata | Detect against the sealed Publish Delta and invalidate before publication. |
| Any host or real Git change before publication | Detect by complete baseline reconciliation, invalidate, and publish nothing. |
| Conflict, crash, or uncertainty after publication begins | Stop, preserve evidence, and enter Repository Quarantine. |

The contract protects repository integrity and host credentials, not the
confidentiality of repository contents from the selected provider. Tracked,
untracked, and ignored snapshot content is readable to the provider except for
paths masked to prevent executable provider discovery, and may be included in
model requests under the resolved task policy.

## Supported host

The MVP supports Linux only. Before starting a provider, the worker MUST prove
all of the following in its actual Herdr execution context:

- Bubblewrap can create user, mount, PID, IPC, UTS, cgroup, and network
  namespaces, mount the private OverlayFS view from sources resolved
  descriptor-relatively by the trusted launcher, apply a seccomp filter, and
  terminate with its parent;
- the chosen upper and work directories are on the same compatible filesystem
  and a representative overlay can be created;
- same-filesystem publication staging and journal-owned recovery storage can be
  created for the worktree and exercised with the required rename operations;
- the host exposes Landlock ABI 3 or newer;
- a delegated cgroup v2 subtree can contain the complete run process tree and
  supports authoritative termination through `cgroup.kill`;
- the trusted nested command launcher can create its stricter command sandbox;
- the synthetic Git view can be built without fetching objects; and
- the selected model profile is supported by the host-side credential broker.

Every probe is mandatory. A failed or unavailable probe returns a typed safety
prerequisite error before provider process creation. There is no degraded
backend, provider-only fallback, privileged helper, OCI fallback, or non-Linux
mode in the MVP.

## Repository identity and lease

The Repository Guard resolves repository identity using sanitized Git commands
and descriptor-based filesystem inspection. It records:

- the canonical worktree root;
- the per-worktree Git directory and Git common directory;
- canonical paths plus mount, device, and inode identity for each root;
- the captured HEAD form and object ID;
- a cryptographic manifest of every entry in the per-worktree and common Git
  directories, including refs, reflogs, objects, index, and configuration; and
- whether the repository is a main or linked worktree.

Linked worktrees are supported. Bare repositories, unborn repositories,
external object alternates, unavailable partial-clone objects, nested mounts,
and a write scope entering a submodule or nested Git repository are rejected.
Submodules and nested repositories may be copied for read-only context, but
their Git metadata is not exposed and their paths cannot be published. LFS
pointer files are ordinary files; the guard never hydrates them over the
network.

Before baseline capture, the workflow acquires one kernel-owned Worktree Lease
keyed by the resolved worktree and Git common-directory identities. The lease
is held from baseline capture through terminal artifact persistence and, when
publication becomes uncertain, through Repository Quarantine. Child Agent Runs
in the fixed workflow share it.

The operating-system file lock is authoritative. Durable lease rows are
diagnostic and recovery state, never authority to break a live lock. If the
file lock is free but durable state describes an unfinished run, the worker
reconciles the recorded phase before admitting another editing run.

## Repository Snapshot

The Repository Guard builds a run-owned immutable snapshot through a
descriptor-relative walker. It reflinks regular files when supported and falls
back to byte copies. The walker re-stats each source around copying and rejects
an unstable source. It rejects special files, cross-mount traversal, and unsafe
hard-link relationships. Existing symlink text may be copied for read-only
visibility, but symlinks never grant publication authority.

The snapshot contains the complete worktree state at baseline time:

- tracked files at their actual worktree contents;
- staged and unstaged changes;
- untracked files;
- ignored files; and
- file type, executable bit, and symlink-target evidence.

Git porcelain-v2 status is semantic evidence, but the security baseline is a
cryptographic Merkle manifest over the raw filesystem state. The guard stores
the manifest and required baseline blobs outside the provider sandbox.

The provider does not receive the live `.git`, linked-worktree Git directory,
or Git common directory. The guard constructs an immutable synthetic Git view
containing the captured HEAD, refs, locally available objects, shallow metadata
when applicable, and an index representing the actual baseline index. The
index is normalized to remove split-index, FSMonitor, and untracked-cache
dependencies. Generated Git configuration contains no remotes, credentials,
aliases, hooks, filters, FSMonitor, alternates, or executable configuration.
Provider Git commands therefore describe the dirty baseline without being able
to change real refs or the real index.

## Scope and change policy

The effective repository policy distinguishes these concepts independently:

- an exact-file write scope;
- a subtree write scope;
- Scratch Scopes that are writable but never publishable;
- explicit ignored-path publication authorization;
- dirty-path acknowledgements bound to the Repository Snapshot digest; and
- explicit destructive-change authorization for deletions and renames.

Serialized field names belong to the public run contract, but it MUST preserve
these distinctions. In particular, a trailing slash or string-prefix test is
not a scope kind.

Scope paths are relative UTF-8 paths beneath the canonical worktree root. They
are compared as exact Linux path bytes without Unicode normalization. Empty,
absolute, NUL-containing, `.` or `..`-traversing paths are rejected. Existing
components are opened from an `O_PATH` root file descriptor with
`openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS |
RESOLVE_NO_XDEV)`. A missing target is permitted only when its nearest existing
parent resolves under those same constraints.

Any symlink in a scoped path or its existing ancestors rejects the scope.
Creating, deleting, replacing, retargeting, or publishing a symlink is always
invalid. This applies even when both its lexical path and target appear to be
inside scope.

A dirty writable path is any path intersecting the proposed publish scope that
was staged, unstaged, untracked, or ignored at baseline. The parent MUST
explicitly acknowledge every such path and the exact Repository Snapshot
digest before the provider starts. A missing acknowledgement or digest mismatch
rejects the run. Dirty paths outside publish scope remain readable and are
protected by whole-repository baseline reconciliation.

An ignored path is publishable only when it is both inside ordinary write scope
and covered by a separate ignored-path authorization. The guard evaluates
ignored status against both the immutable baseline rules and candidate rules;
changing ignore configuration cannot self-authorize an output. Declared
Scratch Scopes use run-private storage and are discarded after evidence
collection. Discarding scratch is policy fulfillment, not rollback.

Deletes and renames require destructive-change authorization that cannot be
inferred from a directory scope. A deletion source and both rename endpoints
must independently be in publish scope. The final Publish Delta supports only
regular files, directories, and executable-bit changes. Ownership, ordinary
permission changes other than the executable bit, setuid/setgid bits, ACLs,
capabilities, arbitrary extended attributes, devices, sockets, FIFOs, and other
special files invalidate the candidate.

## Provider and descendant containment

Bubblewrap constructs the primary sandbox from an empty root. The provider sees
only:

- immutable, allowlisted runtime and provider installations;
- the merged workspace projected at the validated worktree path;
- the synthetic Git view;
- private `HOME`, XDG, temporary, `/proc`, and minimal `/dev` trees; and
- the run-scoped model-broker endpoint required by the selected profile.

The trusted launcher constructs the merged workspace from the Repository
Snapshot lower layer and run-private upper and work directories. Those raw
layers remain outside the provider namespace; exposing them would bypass
merged-path policy and invalidate the safety backend.

The sandbox exposes no live worktree, live Git metadata, host home, arbitrary
`/run` content, Herdr socket, SSH agent, container socket, credential store,
orchestrator database, artifact store, or host cgroup control files. All Linux
capabilities are dropped. Nested user namespaces are disabled after setup.

After the merged workspace is mounted, a trusted launcher applies Landlock to
the provider and descendants. Existing exact-file scopes receive only the file
write and truncate rights they need. Subtree scopes receive write and truncate
rights for existing descendants plus only the creation rights required by the
resolved policy. Exact missing-file scopes necessarily grant creation at their
validated parent, so the final delta check rejects any sibling creation.
`MAKE_SYM` is never granted; `REFER` and removal rights are granted only when
destructive changes are authorized. Provider-native state and Scratch Scopes
are separate rules.

A versioned seccomp profile blocks namespace, mount, ptrace, keyring, BPF,
device, credential, and other privilege-escalation syscall classes not required
by the pinned provider. A private PID namespace and `--die-with-parent` provide
local reaping. A run-owned cgroup v2 subtree is the authoritative process and
resource boundary: the provider, broker bridge, MCP processes, tool processes,
and every descendant belong to it. Process groups are used only for cooperative
signals. Validation and publication cannot begin until the cgroup is killed or
observed empty.

The provider network namespace has no direct host or Internet route. A trusted
bridge reaches only a host-side, provider-specific model proxy. The proxy owns
the real credential, injects authentication upstream, restricts destination and
port to the selected model endpoint, rejects loopback/private/link-local and
metadata destinations, and records destination and byte-count evidence. The
provider receives only a run-scoped broker capability. Profiles that cannot use
this broker are unsupported.

Nested command sandboxes receive the merged workspace under the same Landlock
policy but receive no model capability, network bridge, Herdr identity, or host
credential. They use a clean environment, private process namespace, and
private `/proc`. Build and test caches must be resolved as Scratch Scopes or
other run-owned state.

## Provider-specific startup requirements

### OMP 17.0.2

OMP imports discovered custom-tool modules and invokes their factories before
the RPC `ready` frame. Containment and discovery masking therefore exist before
`omp --mode rpc` is spawned; post-ready tool inspection is not the security
boundary.

The run-owned OMP configuration MUST disable native subagents, project MCP,
LSP, browser/eval facilities, and every process launcher not routed through the
trusted command wrapper. The snapshot retains all baseline evidence, but the
OMP view masks every executable discovery path identified by the pinned source
audit, including project custom-tool and MCP configuration paths, explicit
configured-tool paths, and provider plugin caches. The model bridge remains
closed until RPC readiness, exact tool-dump validation, discovery audit, and
process-tree validation succeed.

OMP shell execution MUST use the trusted nested command launcher. If the pinned
version cannot prove that every command-launch path uses that wrapper, the
adapter MUST replace shell execution with a host-owned command tool. If neither
path is proved, Managed OMP remains unsupported.

### Codex 0.144.5

Codex uses its run-owned managed home and native command sandbox as additional
defense-in-depth. Repository skills and instructions are audited as required by
the [Codex App Server host contract](codex-app-server-host-contract.md), but the
outer Repository Guard remains authoritative. App Server receives the broker
endpoint and run-scoped capability, never a reusable provider credential.

## Candidate sealing and verification

When the editing provider finishes, the worker first reaps its complete process
tree and validates the Run Overlay against the Repository Snapshot. It rejects
out-of-scope paths, undeclared ignored output, scratch publication, missing
dirty acknowledgement, unauthorized destructive changes, symlink changes,
unsupported file types or metadata, and an artifact that does not describe the
same candidate.

A valid implementation candidate is sealed with its own digest. Verification
commands and read-only role runs execute against immutable projections of that
candidate with disposable Scratch Scopes; they cannot mutate the sealed
candidate. Verification-generated caches and temporary output are discarded
after evidence collection.

For an individual editing Agent Run, publication may begin only after its
required repository checks and verification pass. In the built-in implementer
to reviewer to verifier Workflow Run, the sealed candidate remains private
through review and verification. Blocking review findings or failed
verification preserve the candidate, patch, logs, and artifacts but leave the
host worktree unchanged. Successful verification permits one workflow-owned
publication. The parent reviews the resulting real-worktree diff; parent
rejection never triggers automatic rollback.

## Publication

Publication begins only after all provider and verification process trees are
reaped and the sealed candidate digest is unchanged. The publisher then:

1. recomputes the complete live worktree Merkle manifest, repository identity,
   and complete per-worktree and common Git-directory manifest;
2. compares them with the captured baseline and invalidates the run without
   mutation on any mismatch;
3. derives an ordered per-path publication plan and writes and fsyncs the
   Publish Journal before the first host mutation;
4. closes the cancellation gate;
5. immediately before every path operation, proves that the live repository
   matches the journal-derived expected intermediate state, including the
   target path's expected baseline preimage when it has not yet been published;
6. applies additions with no-replace semantics, modifications through
   same-filesystem staged files and atomic per-path exchange, and deletions by
   moving the expected baseline entry into same-filesystem journal-owned
   recovery storage;
7. fsyncs changed files, affected directories, and journal phase transitions;
   and
8. recomputes the expected final host and Git evidence before recording a
   successful publication.

The publisher uses validated directory file descriptors and compare-and-swap
preconditions throughout. It never follows submitted paths again by name and
never changes the real Git index, refs, configuration, or object database.

A cancel request before the journal's point of no return cancels the run and
publishes nothing. Once the first host mutation may occur, cancellation returns
`too late`; the publisher continues until it reaches a valid publication or a
quarantined failure.

## Failure, restart, and Repository Quarantine

A worker or host loss before publication leaves the host untouched. Recovery
marks the run failed, preserves its Repository Snapshot, Run Overlay, candidate,
logs, and artifacts, and may release the Worktree Lease after proving that no
publication began and the host still matches the baseline.

A conflict, process loss, corrupt journal, or restart with an uncertain phase
after publication begins immediately places the worktree in Repository
Quarantine. Recovery stops publication, preserves the journal, overlay,
baseline blobs, expected candidate, observed host state, and correlation logs,
and never overwrites an external writer or attempts automatic rollback.

Quarantine retains exclusive editing ownership for that worktree. Read-only
inspection is allowed from independent immutable snapshots, but new editing
runs are rejected. Only an explicit Parent Agent acknowledgement that the
repository has been manually reconciled may clear quarantine and its durable
lease metadata. A stale database row or absent process is not sufficient.

## Required downstream semantics

The public run, lifecycle, state, and artifact contracts MUST be able to carry,
without relying on provider-native fields:

- resolved repository and worktree identity;
- safety-backend capability evidence;
- Repository Snapshot and sealed candidate digests;
- exact-file and subtree scopes, Scratch Scopes, dirty acknowledgements,
  ignored-path authorization, and destructive-change authorization;
- Publish Delta and verification evidence;
- publication phase and cancellation-gate result;
- a durable Publish Journal reference; and
- Repository Quarantine state and reconciliation acknowledgement.

Exact serialized names, error envelopes, and general terminal-state precedence
remain downstream decisions. Semantically, an unavailable prerequisite rejects
startup, a candidate or pre-publication baseline violation is invalid and
publishes nothing, a pre-publication provider loss fails with the overlay
preserved, and any uncertain or partial publication is invalid and quarantined.

## Required proof scenarios

Implementation is not complete until real-boundary tests prove:

- every required host probe succeeds in a Herdr worker and each missing feature
  fails before process creation;
- clean, staged, unstaged, untracked, ignored, linked-worktree, missing-target,
  rename, and deletion fixtures produce the specified policy decisions;
- traversal, symlink-swap, hard-link, nested-repository, mount-boundary, and
  unsupported-metadata attacks cannot publish;
- an OMP pre-ready custom-tool factory and MCP process cannot touch the live
  worktree, host home, Herdr socket, direct network, or real credential;
- forked, daemonized, and nested command descendants remain in the cgroup and
  are absent before validation;
- external mutation before publication publishes nothing;
- injected conflicts and crashes at every durable journal boundary either
  finish validly or produce preserved quarantine evidence without rollback;
- cancellation immediately before and after the publication gate has the
  specified result;
- worker loss before publication preserves the candidate and leaves the host
  byte-for-byte at baseline;
- reviewer and verifier see the sealed workflow candidate while the real
  worktree remains unchanged, and only successful verification publishes it;
  and
- successful publication changes only authorized content, leaves real Git
  metadata untouched, empties the run cgroup, and records complete evidence.

## Primary references

- [Bubblewrap upstream manual](https://github.com/containers/bubblewrap/blob/main/bwrap.xml)
- [Linux OverlayFS](https://docs.kernel.org/filesystems/overlayfs.html)
- [Linux Landlock](https://docs.kernel.org/userspace-api/landlock.html)
- [Linux cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [`openat2(2)`](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [`renameat2(2)`](https://man7.org/linux/man-pages/man2/rename.2.html)
- [`fanotify_init(2)`](https://man7.org/linux/man-pages/man2/fanotify_init.2.html)
- [Git status](https://git-scm.com/docs/git-status)
- [Git worktree](https://git-scm.com/docs/git-worktree)
- [OMP 17.0.2 custom-tool loader](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/extensibility/custom-tools/loader.ts)
- [OMP 17.0.2 noninteractive environment](https://github.com/can1357/oh-my-pi/blob/v17.0.2/packages/coding-agent/src/exec/non-interactive-env.ts)
- [Codex 0.144.5 Linux sandbox](https://github.com/openai/codex/blob/rust-v0.144.5/codex-rs/linux-sandbox/README.md)
