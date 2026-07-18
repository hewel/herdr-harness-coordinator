//! Per-Herdr-workspace Coordinator activation and saved Harness selection.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;

use crate::contract::{CodexApprovalPolicy, CodexSandboxMode};
use crate::contract::{HarnessId, HarnessKind};

const SCHEMA_VERSION: u32 = 1;

/// Persistent user intent for one workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredActivation {
    /// Coordinator commands are rejected for this workspace.
    Off,
    /// Coordinator configuration is enabled for this workspace.
    On,
}

impl DesiredActivation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
        }
    }
}

/// Runtime status is deliberately separate from durable user intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationRuntime {
    /// No Coordinator process is expected to be live.
    Offline,
    /// Desired state survived a cold restart and must be explicitly reactivated.
    ReactivationRequired,
    /// The workspace Coordinator and selected Harnesses passed readiness.
    Online,
    /// Startup was incomplete and requires operator reconciliation.
    RecoveryRequired,
}

/// Exact Supervisor declaration saved for a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorSelection {
    /// Native Supervisor Harness Kind.
    pub kind: HarnessKind,
    /// Explicit model identifier; the Coordinator never chooses one.
    pub model: String,
    /// Optional provider-native reasoning effort.
    pub reasoning_effort: Option<String>,
    /// Explicit Codex App Server approval policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_approval_policy: Option<CodexApprovalPolicy>,
    /// Explicit Codex App Server sandbox mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_sandbox_mode: Option<CodexSandboxMode>,
}

/// Exact durable Worker identity and launch profile selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSelection {
    /// Worker address used by Tasks.
    pub worker_id: HarnessId,
    /// Coordinator-owned launch profile ID.
    pub profile_id: HarnessId,
}

/// Explicit Harness selection retained while the workspace is off.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSelection {
    /// Persisted selection contract version.
    #[serde(default = "selection_schema_version")]
    pub schema_version: u32,
    /// Sole planning and verification authority.
    pub supervisor: SupervisorSelection,
    /// Explicitly addressable execution Harnesses.
    pub workers: Vec<WorkerSelection>,
}

/// Stable identity resolved from Herdr action context and repository state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceIdentity {
    session_socket: PathBuf,
    workspace_id: String,
    repository_root: PathBuf,
}

impl WorkspaceIdentity {
    /// Constructs an identity that will be canonicalized and validated on use.
    #[must_use]
    pub fn new(session_socket: PathBuf, workspace_id: String, repository_root: PathBuf) -> Self {
        Self {
            session_socket,
            workspace_id,
            repository_root,
        }
    }

    /// Returns the Herdr session socket that scopes the workspace identifier.
    #[must_use]
    pub fn session_socket(&self) -> &Path {
        &self.session_socket
    }
}

/// Idempotent desired-state request with optional compare-and-set protection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetActivationRequest {
    /// Requested durable state.
    pub desired: DesiredActivation,
    /// Revision observed by the caller.
    pub expected_revision: Option<u64>,
    /// Replacement selection; required on first enable.
    pub selection: Option<WorkspaceSelection>,
}

/// Public activation projection used by the CLI, popup, and MCP router.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceActivationView {
    /// Persisted contract version.
    pub schema_version: u32,
    /// Opaque Herdr workspace ID.
    pub workspace_id: String,
    /// Canonical repository worktree root.
    pub repository_root: PathBuf,
    /// Monotonic compare-and-set revision.
    pub revision: u64,
    /// Durable user intent.
    pub desired: DesiredActivation,
    /// Best-known live state.
    pub runtime: ActivationRuntime,
    /// Saved exact Supervisor and Worker selection.
    pub selection: Option<WorkspaceSelection>,
    /// Actionable recovery evidence, when present.
    pub diagnostic: Option<String>,
    /// Private per-workspace Coordinator state directory.
    pub state_dir: PathBuf,
}

/// Workspace activation failure with stable operator-facing meaning.
#[derive(Debug, Error)]
pub enum ActivationError {
    /// Herdr context did not provide a usable workspace identity.
    #[error("invalid workspace identity: {0}")]
    InvalidIdentity(String),
    /// The same Herdr identity was previously bound to a different root.
    #[error("workspace `{workspace_id}` is already bound to a different repository root")]
    IdentityMismatch { workspace_id: String },
    /// First activation did not explicitly name the Supervisor and Workers.
    #[error("an explicit Supervisor and Worker selection is required on first enable")]
    SelectionRequired,
    /// Harness selection violates a public invariant.
    #[error("invalid Harness selection: {0}")]
    InvalidSelection(String),
    /// Caller attempted a competing write based on stale state.
    #[error("workspace activation revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    /// Another workspace already owns this canonical worktree activation.
    #[error("repository `{root}` is already enabled by workspace `{workspace_id}`")]
    RepositoryAlreadyActive { root: String, workspace_id: String },
    /// Durable work makes shutdown unsafe.
    #[error("workspace cannot be disabled while {0}")]
    WorkspaceBusy(String),
    /// Filesystem or `SQLite` failure.
    #[error("workspace activation storage failure: {0}")]
    Storage(String),
}

/// Deep command/query module for all per-workspace activation state.
#[derive(Debug, Clone)]
pub struct ActivationRegistry {
    plugin_state_dir: PathBuf,
    pool: SqlitePool,
}

impl ActivationRegistry {
    /// Opens the plugin-root activation index and creates its private schema.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] when the directory or `SQLite` index cannot be opened.
    pub async fn open(plugin_state_dir: impl AsRef<Path>) -> Result<Self, ActivationError> {
        let plugin_state_dir = plugin_state_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&plugin_state_dir)
            .await
            .map_err(storage)?;
        let database = plugin_state_dir.join("activation.sqlite3");
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", database.display()))
            .map_err(storage)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(storage)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS workspace_activations (\
                identity_key TEXT PRIMARY KEY,\
                session_socket TEXT NOT NULL,\
                workspace_id TEXT NOT NULL,\
                canonical_root TEXT NOT NULL,\
                revision INTEGER NOT NULL,\
                desired TEXT NOT NULL CHECK(desired IN ('on','off')),\
                runtime TEXT NOT NULL,\
                selection_json TEXT,\
                diagnostic TEXT,\
                state_dir TEXT NOT NULL,\
                updated_at TEXT NOT NULL,\
                UNIQUE(session_socket, workspace_id)\
            ) STRICT",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_enabled_workspace_per_root \
             ON workspace_activations(canonical_root) WHERE desired = 'on'",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;
        Ok(Self {
            plugin_state_dir,
            pool,
        })
    }

    /// Returns one workspace view; an unknown identity is off by default.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] for invalid identity, recycled IDs, or storage failures.
    pub async fn get(
        &self,
        identity: &WorkspaceIdentity,
    ) -> Result<WorkspaceActivationView, ActivationError> {
        let resolved = self.resolve_identity(identity)?;
        let row = sqlx::query(
            "SELECT * FROM workspace_activations WHERE session_socket = ? AND workspace_id = ?",
        )
        .bind(&resolved.session_socket)
        .bind(&resolved.workspace_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        let Some(row) = row else {
            return Ok(resolved.default_view());
        };
        if row.get::<&str, _>("canonical_root") != resolved.canonical_root {
            return Err(ActivationError::IdentityMismatch {
                workspace_id: resolved.workspace_id,
            });
        }
        decode_view(&row)
    }

    /// Lists all known workspace activation records.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] when persisted data cannot be decoded.
    pub async fn list(&self) -> Result<Vec<WorkspaceActivationView>, ActivationError> {
        let rows = sqlx::query(
            "SELECT * FROM workspace_activations ORDER BY session_socket, workspace_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.iter().map(decode_view).collect()
    }

    /// Resolves the activation routed to one private workspace state directory.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] when storage fails or multiple rows name the directory.
    pub async fn find_by_state_dir(
        &self,
        state_dir: &Path,
    ) -> Result<Option<WorkspaceActivationView>, ActivationError> {
        let rows = sqlx::query("SELECT * FROM workspace_activations WHERE state_dir = ?")
            .bind(state_dir.to_string_lossy().as_ref())
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        if rows.len() > 1 {
            return Err(ActivationError::Storage(
                "multiple workspaces route to one state directory".to_owned(),
            ));
        }
        rows.first().map(decode_view).transpose()
    }

    /// Records the outcome of live activation side effects with revision protection.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] when identity changed, evidence is oversized, or CAS loses.
    pub async fn record_runtime(
        &self,
        identity: &WorkspaceIdentity,
        runtime: ActivationRuntime,
        diagnostic: Option<String>,
    ) -> Result<WorkspaceActivationView, ActivationError> {
        let current = self.get(identity).await?;
        if diagnostic
            .as_ref()
            .is_some_and(|value| value.len() > 16_384)
        {
            return Err(ActivationError::Storage(
                "activation diagnostic exceeds 16 KiB".to_owned(),
            ));
        }
        let runtime = match runtime {
            ActivationRuntime::Offline => "offline",
            ActivationRuntime::ReactivationRequired => "reactivation_required",
            ActivationRuntime::Online => "online",
            ActivationRuntime::RecoveryRequired => "recovery_required",
        };
        let next = current.revision.saturating_add(1);
        let changed = sqlx::query(
            "UPDATE workspace_activations SET revision = ?, runtime = ?, diagnostic = ?, updated_at = ? WHERE state_dir = ? AND revision = ?",
        )
        .bind(i64::try_from(next).map_err(storage)?)
        .bind(runtime)
        .bind(diagnostic)
        .bind(Utc::now().to_rfc3339())
        .bind(current.state_dir.to_string_lossy().as_ref())
        .bind(i64::try_from(current.revision).map_err(storage)?)
        .execute(&self.pool)
        .await
        .map_err(storage)?
        .rows_affected();
        if changed != 1 {
            return Err(ActivationError::RevisionConflict {
                expected: current.revision,
                actual: self.get(identity).await?.revision,
            });
        }
        self.get(identity).await
    }

    /// Applies an idempotent desired state using optional revision protection.
    ///
    /// # Errors
    ///
    /// Returns [`ActivationError`] for invalid selection, conflicts, unsafe disable, or storage failures.
    pub async fn set(
        &self,
        identity: &WorkspaceIdentity,
        request: SetActivationRequest,
    ) -> Result<WorkspaceActivationView, ActivationError> {
        let resolved = self.resolve_identity(identity)?;
        let current = self.get(identity).await?;
        if let Some(expected) = request.expected_revision
            && expected != current.revision
        {
            return Err(ActivationError::RevisionConflict {
                expected,
                actual: current.revision,
            });
        }
        if current.desired == request.desired
            && (request.selection.is_none() || request.selection == current.selection)
        {
            return Ok(current);
        }
        let selection = request.selection.or(current.selection);
        if request.desired == DesiredActivation::On {
            let selection = selection
                .as_ref()
                .ok_or(ActivationError::SelectionRequired)?;
            validate_selection(selection)?;
            if let Some(row) = sqlx::query(
                "SELECT workspace_id FROM workspace_activations WHERE canonical_root = ? AND desired = 'on' AND identity_key != ?",
            ).bind(&resolved.canonical_root).bind(&resolved.identity_key).fetch_optional(&self.pool).await.map_err(storage)? {
                return Err(ActivationError::RepositoryAlreadyActive {
                    root: resolved.canonical_root,
                    workspace_id: row.get::<&str, _>("workspace_id").to_owned(),
                });
            }
        } else {
            self.ensure_safe_to_disable(&current.state_dir).await?;
        }
        tokio::fs::create_dir_all(&resolved.state_dir)
            .await
            .map_err(storage)?;
        let next_revision = current.revision.saturating_add(1);
        let runtime = if request.desired == DesiredActivation::On {
            "reactivation_required"
        } else {
            "offline"
        };
        let selection_json = selection
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(storage)?;
        let revision = i64::try_from(next_revision).map_err(storage)?;
        let updated_at = Utc::now().to_rfc3339();
        if current.revision == 0 {
            sqlx::query(
                "INSERT INTO workspace_activations (identity_key, session_socket, workspace_id, canonical_root, revision, desired, runtime, selection_json, diagnostic, state_dir, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            )
            .bind(&resolved.identity_key).bind(&resolved.session_socket).bind(&resolved.workspace_id)
            .bind(&resolved.canonical_root).bind(revision).bind(request.desired.as_str())
            .bind(runtime).bind(selection_json).bind(resolved.state_dir.to_string_lossy().as_ref())
            .bind(&updated_at).execute(&self.pool).await.map_err(storage)?;
        } else {
            let changed = sqlx::query(
                "UPDATE workspace_activations SET revision = ?, desired = ?, runtime = ?, selection_json = ?, diagnostic = NULL, updated_at = ? WHERE identity_key = ? AND revision = ?",
            )
            .bind(revision).bind(request.desired.as_str()).bind(runtime).bind(selection_json)
            .bind(&updated_at).bind(&resolved.identity_key)
            .bind(i64::try_from(current.revision).map_err(storage)?)
            .execute(&self.pool).await.map_err(storage)?.rows_affected();
            if changed != 1 {
                let actual: i64 = sqlx::query_scalar(
                    "SELECT revision FROM workspace_activations WHERE identity_key = ?",
                )
                .bind(&resolved.identity_key)
                .fetch_one(&self.pool)
                .await
                .map_err(storage)?;
                return Err(ActivationError::RevisionConflict {
                    expected: current.revision,
                    actual: u64::try_from(actual).map_err(storage)?,
                });
            }
        }
        self.get(identity).await
    }

    fn resolve_identity(
        &self,
        identity: &WorkspaceIdentity,
    ) -> Result<ResolvedIdentity, ActivationError> {
        if identity.workspace_id.trim().is_empty() || identity.workspace_id.len() > 256 {
            return Err(ActivationError::InvalidIdentity(
                "workspace ID is missing or too long".to_owned(),
            ));
        }
        let root = std::fs::canonicalize(&identity.repository_root).map_err(storage)?;
        if !root.is_dir() {
            return Err(ActivationError::InvalidIdentity(
                "repository root is not a directory".to_owned(),
            ));
        }
        let session_socket = identity.session_socket.to_string_lossy().into_owned();
        if session_socket.trim().is_empty() {
            return Err(ActivationError::InvalidIdentity(
                "Herdr session socket is missing".to_owned(),
            ));
        }
        let canonical_root = root.to_string_lossy().into_owned();
        let identity_key = hex::encode(Sha256::digest(
            format!("{session_socket}\0{}", identity.workspace_id).as_bytes(),
        ));
        let state_dir = self.plugin_state_dir.join("workspaces").join(&identity_key);
        Ok(ResolvedIdentity {
            identity_key,
            session_socket,
            workspace_id: identity.workspace_id.clone(),
            canonical_root,
            state_dir,
        })
    }

    async fn ensure_safe_to_disable(&self, state_dir: &Path) -> Result<(), ActivationError> {
        let database = state_dir.join("coordinator.sqlite3");
        if !database.exists() {
            return Ok(());
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", database.display()))
            .map_err(storage)?
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(storage)?;
        let active_tasks: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE state IN ('queued','dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
        ).fetch_one(&pool).await.map_err(storage)?;
        if active_tasks > 0 {
            return Err(ActivationError::WorkspaceBusy(
                "Tasks are active or awaiting review".to_owned(),
            ));
        }
        let holds: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worktree_holds WHERE cleared_at IS NULL")
                .fetch_one(&pool)
                .await
                .map_err(storage)?;
        if holds > 0 {
            return Err(ActivationError::WorkspaceBusy(
                "a Worktree Hold is unresolved".to_owned(),
            ));
        }
        let busy_workers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_sessions WHERE harness_tier = 'worker' AND ended_at IS NULL AND activity NOT IN ('idle','stopped')",
        ).fetch_one(&pool).await.map_err(storage)?;
        if busy_workers > 0 {
            return Err(ActivationError::WorkspaceBusy(
                "a Worker Harness is busy".to_owned(),
            ));
        }
        Ok(())
    }
}

struct ResolvedIdentity {
    identity_key: String,
    session_socket: String,
    workspace_id: String,
    canonical_root: String,
    state_dir: PathBuf,
}

impl ResolvedIdentity {
    fn default_view(&self) -> WorkspaceActivationView {
        WorkspaceActivationView {
            schema_version: SCHEMA_VERSION,
            workspace_id: self.workspace_id.clone(),
            repository_root: PathBuf::from(&self.canonical_root),
            revision: 0,
            desired: DesiredActivation::Off,
            runtime: ActivationRuntime::Offline,
            selection: None,
            diagnostic: None,
            state_dir: self.state_dir.clone(),
        }
    }
}

fn validate_selection(selection: &WorkspaceSelection) -> Result<(), ActivationError> {
    if selection.schema_version != SCHEMA_VERSION {
        return Err(ActivationError::InvalidSelection(
            "selection schema_version must equal 1".to_owned(),
        ));
    }
    if selection.supervisor.model.trim().is_empty() {
        return Err(ActivationError::InvalidSelection(
            "Supervisor model is empty".to_owned(),
        ));
    }
    let codex_policy_complete = selection.supervisor.codex_approval_policy.is_some()
        && selection.supervisor.codex_sandbox_mode.is_some();
    if selection.supervisor.kind == HarnessKind::Codex && !codex_policy_complete {
        return Err(ActivationError::InvalidSelection(
            "Codex Supervisor requires explicit approval and sandbox policies".to_owned(),
        ));
    }
    if selection.supervisor.kind != HarnessKind::Codex
        && (selection.supervisor.codex_approval_policy.is_some()
            || selection.supervisor.codex_sandbox_mode.is_some())
    {
        return Err(ActivationError::InvalidSelection(
            "Codex Supervisor policies require a Codex Supervisor".to_owned(),
        ));
    }
    if selection.workers.is_empty() {
        return Err(ActivationError::InvalidSelection(
            "at least one Worker is required".to_owned(),
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    if selection
        .workers
        .iter()
        .any(|worker| !ids.insert(&worker.worker_id))
    {
        return Err(ActivationError::InvalidSelection(
            "Worker IDs must be unique".to_owned(),
        ));
    }
    Ok(())
}

const fn selection_schema_version() -> u32 {
    SCHEMA_VERSION
}

fn decode_view(row: &sqlx::sqlite::SqliteRow) -> Result<WorkspaceActivationView, ActivationError> {
    let desired = match row.get::<&str, _>("desired") {
        "on" => DesiredActivation::On,
        "off" => DesiredActivation::Off,
        value => return Err(storage(format!("unknown desired state `{value}`"))),
    };
    let runtime = match row.get::<&str, _>("runtime") {
        "offline" => ActivationRuntime::Offline,
        "reactivation_required" => ActivationRuntime::ReactivationRequired,
        "online" => ActivationRuntime::Online,
        "recovery_required" => ActivationRuntime::RecoveryRequired,
        value => return Err(storage(format!("unknown runtime state `{value}`"))),
    };
    let selection = row
        .get::<Option<&str>, _>("selection_json")
        .map(serde_json::from_str)
        .transpose()
        .map_err(storage)?;
    Ok(WorkspaceActivationView {
        schema_version: SCHEMA_VERSION,
        workspace_id: row.get::<&str, _>("workspace_id").to_owned(),
        repository_root: PathBuf::from(row.get::<&str, _>("canonical_root")),
        revision: u64::try_from(row.get::<i64, _>("revision")).map_err(storage)?,
        desired,
        runtime,
        selection,
        diagnostic: row.get::<Option<&str>, _>("diagnostic").map(str::to_owned),
        state_dir: PathBuf::from(row.get::<&str, _>("state_dir")),
    })
}

fn storage(error: impl std::fmt::Display) -> ActivationError {
    ActivationError::Storage(error.to_string())
}
