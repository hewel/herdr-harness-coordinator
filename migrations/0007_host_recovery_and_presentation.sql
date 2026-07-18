CREATE TABLE host_connections (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    generation INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    capability_hash TEXT NOT NULL UNIQUE,
    lease_seconds INTEGER NOT NULL CHECK (lease_seconds BETWEEN 1 AND 300),
    status TEXT NOT NULL CHECK (status IN ('active', 'disconnected', 'expired')),
    bound_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    disconnected_at TEXT,
    disconnect_reason TEXT,
    UNIQUE(session_id, generation)
) STRICT;

CREATE UNIQUE INDEX one_active_host_connection_per_session
ON host_connections(session_id)
WHERE status = 'active';

CREATE INDEX host_connections_expiry
ON host_connections(status, expires_at);

ALTER TABLE supervisor_event_attempts
ADD COLUMN target_host_connection_id TEXT REFERENCES host_connections(id);

CREATE TABLE supervisor_event_observations (
    id TEXT PRIMARY KEY,
    observation_key TEXT NOT NULL UNIQUE,
    event_id TEXT NOT NULL REFERENCES supervisor_events(id),
    attempt_id TEXT REFERENCES supervisor_event_attempts(id),
    observation_kind TEXT NOT NULL CHECK (observation_kind IN (
        'turn_started', 'presented', 'turn_completed', 'presentation_timeout'
    )),
    native_session_id TEXT,
    native_thread_id TEXT,
    native_turn_id TEXT,
    evidence_json TEXT NOT NULL,
    observed_at TEXT NOT NULL
) STRICT;

CREATE INDEX supervisor_event_observations_event
ON supervisor_event_observations(event_id, observed_at);

CREATE TABLE supervisor_event_reconciliations (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES supervisor_events(id),
    attempt_id TEXT REFERENCES supervisor_event_attempts(id),
    resolution TEXT NOT NULL CHECK (resolution IN ('retry', 'processed', 'cancel')),
    audit_note TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE INDEX supervisor_event_reconciliations_event
ON supervisor_event_reconciliations(event_id, created_at);
