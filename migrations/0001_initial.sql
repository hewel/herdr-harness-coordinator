PRAGMA foreign_keys = ON;

CREATE TABLE harnesses (
    id TEXT PRIMARY KEY,
    definition_json TEXT NOT NULL,
    kind TEXT NOT NULL,
    tier TEXT NOT NULL,
    cwd TEXT NOT NULL,
    launch_profile TEXT,
    model TEXT,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE global_sequences (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose TEXT NOT NULL
) STRICT;

CREATE TABLE harness_sessions (
    id TEXT PRIMARY KEY,
    harness_id TEXT NOT NULL REFERENCES harnesses(id),
    harness_tier TEXT NOT NULL,
    capability_hash TEXT NOT NULL UNIQUE,
    connection_generation INTEGER NOT NULL,
    native_session_id TEXT,
    terminal_id TEXT,
    pane_id TEXT,
    presence TEXT NOT NULL,
    activity TEXT NOT NULL,
    event_sequence INTEGER NOT NULL DEFAULT 0,
    profile_snapshot_json TEXT,
    profile_digest TEXT,
    started_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    ended_at TEXT
) STRICT;

CREATE UNIQUE INDEX one_live_supervisor
ON harness_sessions((1))
WHERE ended_at IS NULL
  AND harness_tier = 'supervisor';

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES harnesses(id),
    related_task_id TEXT REFERENCES tasks(id),
    submission_json TEXT NOT NULL,
    state TEXT NOT NULL,
    result_revision INTEGER NOT NULL DEFAULT 0,
    active_session_id TEXT REFERENCES harness_sessions(id),
    baseline_observation_id TEXT,
    created_sequence INTEGER NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE task_transitions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    task_id TEXT REFERENCES tasks(id),
    sender_id TEXT NOT NULL REFERENCES harnesses(id),
    recipient_id TEXT NOT NULL REFERENCES harnesses(id),
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    reply_to TEXT REFERENCES messages(id),
    delivery_intent TEXT NOT NULL,
    created_sequence INTEGER NOT NULL UNIQUE,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE inbox_reads (
    harness_id TEXT NOT NULL REFERENCES harnesses(id),
    message_id TEXT NOT NULL REFERENCES messages(id),
    read_at TEXT NOT NULL,
    PRIMARY KEY (harness_id, message_id)
) STRICT;

CREATE TABLE delivery_attempts (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES messages(id),
    attempt_number INTEGER NOT NULL,
    target_session_id TEXT REFERENCES harness_sessions(id),
    state TEXT NOT NULL,
    provider_bytes_may_have_been_written INTEGER NOT NULL,
    native_correlation TEXT,
    evidence_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(message_id, attempt_number)
) STRICT;

CREATE TABLE results (
    task_id TEXT NOT NULL REFERENCES tasks(id),
    revision INTEGER NOT NULL,
    native_turn_id TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    terminal_at TEXT,
    PRIMARY KEY(task_id, revision)
) STRICT;

CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    original_name TEXT NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE repository_observations (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    checkpoint TEXT NOT NULL,
    digest TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE worktree_leases (
    repository_key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
    acquired_at TEXT NOT NULL,
    released_at TEXT
) STRICT;

CREATE TABLE worktree_holds (
    id TEXT PRIMARY KEY,
    repository_key TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    reason TEXT NOT NULL,
    observation_id TEXT REFERENCES repository_observations(id),
    created_at TEXT NOT NULL,
    cleared_at TEXT,
    audit_note TEXT
) STRICT;

CREATE UNIQUE INDEX one_active_hold_per_repository
ON worktree_holds(repository_key)
WHERE cleared_at IS NULL;

CREATE TABLE idempotency (
    actor_id TEXT NOT NULL,
    command_kind TEXT NOT NULL,
    request_key TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(actor_id, command_kind, request_key)
) STRICT;

CREATE TABLE host_events (
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    sequence INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY(session_id, sequence)
) STRICT;
