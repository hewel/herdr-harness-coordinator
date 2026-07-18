//! Transactional command/query boundary for durable Coordinator state.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{SecondsFormat, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use uuid::Uuid;

use crate::contract::{
    DeliveryAttemptId, DeliveryIntent, HarnessDefinitionV1, HarnessId, HarnessSessionId,
    HarnessTier, MessageId, MessageKind, MessageSubmissionV1, ResultManifestV1, TaskId,
    TaskSubmissionV1, Validate,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Stable public error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Input failed shape or semantic validation.
    InvalidInput,
    /// Session capability is absent or invalid.
    Unauthenticated,
    /// Authenticated actor lacks authority.
    Forbidden,
    /// Requested durable value does not exist.
    NotFound,
    /// Durable identity or idempotency conflict.
    Conflict,
    /// Command is invalid for the current lifecycle state.
    InvalidState,
    /// Selected Harness is offline.
    TargetOffline,
    /// Public or native version is unsupported.
    UnsupportedVersion,
    /// Repository is blocked by a Hold or lease.
    RepositoryBlocked,
    /// Native acceptance is ambiguous.
    DeliveryUnknown,
    /// Durable storage failed.
    StorageFailure,
    /// Harness Adapter failed.
    AdapterFailure,
    /// Herdr control failed.
    HerdrFailure,
}

/// Stable error returned by the Coordinator boundary.
#[derive(Debug, Error)]
#[error("{category:?}: {message}")]
pub struct CoordinatorError {
    /// Stable machine-readable category.
    pub category: ErrorCategory,
    /// Concise diagnostic message.
    pub message: String,
    /// Optional immutable evidence Attachment.
    pub evidence: Option<crate::contract::AttachmentId>,
}

impl CoordinatorError {
    fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            evidence: None,
        }
    }

    fn storage(error: impl std::fmt::Display) -> Self {
        Self::new(ErrorCategory::StorageFailure, error.to_string())
    }
}

/// Opaque bearer value issued to one live Harness Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionCapability(String);

impl SessionCapability {
    fn generate() -> Self {
        let mut bytes = [0_u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        Self(hex::encode(bytes))
    }

    fn digest(&self) -> String {
        hex::encode(Sha256::digest(self.0.as_bytes()))
    }
}

/// Authenticated command/query actor.
#[derive(Debug, Clone)]
pub enum ActorContext {
    /// Initial sole-Supervisor registration only.
    Bootstrap,
    /// Live Session authenticated by capability.
    Session { capability: SessionCapability },
}

/// State-changing operations accepted by [`Coordinator::execute`].
#[derive(Debug, Clone)]
pub enum CoordinatorCommand {
    /// Register the sole Supervisor and create its live Session.
    RegisterSupervisor { definition: HarnessDefinitionV1 },
    /// Create or reactivate an explicit Worker Harness.
    StartWorker {
        /// Immutable durable Worker definition.
        definition: HarnessDefinitionV1,
        /// Exact resolved profile contents.
        profile_snapshot: String,
        /// SHA-256 of the resolved profile.
        profile_digest: String,
    },
    /// Create a bounded Task and its root Task message atomically.
    CreateTask {
        /// Validated Supervisor intent.
        submission: TaskSubmissionV1,
    },
    /// Begin delivery of the queued root Task message.
    DispatchTask { task_id: TaskId },
    /// Admit a routed Question, Reply, Correction, or Notification.
    SendMessage { submission: MessageSubmissionV1 },
    /// Record native acceptance reported by the destination Host.
    AcceptDelivery {
        message_id: MessageId,
        native_correlation: String,
    },
    /// Admit the assigned Worker's Result for its current native turn.
    CompleteTask {
        manifest: ResultManifestV1,
        native_turn_id: String,
    },
    /// Record terminal native-turn evidence after Result admission.
    RecordTurnCompleted {
        task_id: TaskId,
        native_turn_id: String,
        succeeded: bool,
    },
}

/// Read-only operations accepted by [`Coordinator::query`].
#[derive(Debug, Clone)]
pub enum CoordinatorQuery {
    /// Return durable Harness identities in creation order.
    ListHarnesses,
    /// Return one Task projection.
    GetTask {
        /// Task to retrieve.
        task_id: TaskId,
    },
}

/// Durable Task lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Accepted and waiting for eligibility.
    Queued,
    /// Delivery attempt is in progress.
    Dispatching,
    /// Worker is executing.
    Working,
    /// Worker asked a blocking Question.
    Waiting,
    /// Result awaits Supervisor decision.
    Reviewing,
    /// Cancellation is in progress.
    Cancelling,
    /// Provider acceptance is ambiguous.
    DeliveryUnknown,
    /// Supervisor accepted the Result and repository state.
    Approved,
    /// Task was cancelled.
    Cancelled,
    /// Task failed.
    Failed,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatching => "dispatching",
            Self::Working => "working",
            Self::Waiting => "waiting",
            Self::Reviewing => "reviewing",
            Self::Cancelling => "cancelling",
            Self::DeliveryUnknown => "delivery_unknown",
            Self::Approved => "approved",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for TaskState {
    type Err = CoordinatorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "dispatching" => Ok(Self::Dispatching),
            "working" => Ok(Self::Working),
            "waiting" => Ok(Self::Waiting),
            "reviewing" => Ok(Self::Reviewing),
            "cancelling" => Ok(Self::Cancelling),
            "delivery_unknown" => Ok(Self::DeliveryUnknown),
            "approved" => Ok(Self::Approved),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(CoordinatorError::new(
                ErrorCategory::StorageFailure,
                format!("unknown Task state `{value}`"),
            )),
        }
    }
}

/// Read-only Task projection returned by the Core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskView {
    /// Task identity.
    pub id: TaskId,
    /// Assigned Worker.
    pub worker_id: HarnessId,
    /// Current durable lifecycle state.
    pub state: TaskState,
    /// Current Result revision.
    pub result_revision: u32,
}

/// Successful command outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutcome {
    /// Supervisor registration and its raw one-time capability.
    SupervisorRegistered {
        /// New live Session identity.
        session_id: HarnessSessionId,
        /// Capability to retain in the calling process.
        capability: SessionCapability,
    },
    /// Worker is online with a Host capability.
    WorkerStarted {
        /// Worker Session identity.
        session_id: HarnessSessionId,
        /// Capability passed only to the Worker Host.
        capability: SessionCapability,
    },
    /// Task and root Task message were durably created.
    TaskCreated {
        /// New Task identity.
        task_id: TaskId,
        /// New root Bus Message identity.
        message_id: MessageId,
    },
    /// Root Task delivery began.
    TaskDispatching {
        task_id: TaskId,
        message_id: MessageId,
    },
    /// Public Bus Message was durably admitted.
    MessageCreated { message_id: MessageId },
    /// A native delivery was accepted and its effects applied.
    DeliveryAccepted { message_id: MessageId },
    /// A Result candidate was durably admitted.
    ResultRecorded { task_id: TaskId, revision: u32 },
    /// A native turn ended and the Task state was updated.
    TurnCompleted { task_id: TaskId, state: TaskState },
}

/// Successful query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryResult {
    /// Durable Harness identities.
    Harnesses(Vec<HarnessId>),
    /// One durable Task.
    Task(TaskView),
}

/// One Coordinator daemon's deep transactional state module.
#[derive(Debug, Clone)]
pub struct Coordinator {
    pool: SqlitePool,
    state_dir: PathBuf,
}

impl Coordinator {
    /// Opens or initializes Coordinator state beneath `state_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when directories, SQLite, or migrations fail.
    pub async fn open(state_dir: impl AsRef<Path>) -> Result<Self, CoordinatorError> {
        let state_dir = state_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&state_dir)
            .await
            .map_err(CoordinatorError::storage)?;
        let database = state_dir.join("coordinator.sqlite3");
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", database.display()))
            .map_err(CoordinatorError::storage)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(CoordinatorError::storage)?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(Self { pool, state_dir })
    }

    /// Executes one authenticated command atomically.
    ///
    /// # Errors
    ///
    /// Returns a stable [`CoordinatorError`] for validation, authorization, conflict, or storage failure.
    pub async fn execute(
        &self,
        actor: ActorContext,
        command: CoordinatorCommand,
    ) -> Result<CommandOutcome, CoordinatorError> {
        match (actor, command) {
            (ActorContext::Bootstrap, CoordinatorCommand::RegisterSupervisor { definition }) => {
                self.register_supervisor(definition).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::StartWorker {
                    definition,
                    profile_snapshot,
                    profile_digest,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.start_worker(definition, profile_snapshot, profile_digest)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::CreateTask { submission },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.create_task(&actor, submission).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::DispatchTask { task_id },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.dispatch_task(task_id).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::SendMessage { submission },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.send_message(&actor, submission).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::AcceptDelivery {
                    message_id,
                    native_correlation,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.accept_delivery(&actor, message_id, native_correlation)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::CompleteTask {
                    manifest,
                    native_turn_id,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.complete_task(&actor, manifest, native_turn_id).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordTurnCompleted {
                    task_id,
                    native_turn_id,
                    succeeded,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_turn_completed(&actor, task_id, native_turn_id, succeeded)
                    .await
            }
            _ => Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "command is not permitted for this actor",
            )),
        }
    }

    /// Executes one authenticated query without exposing SQLite internals.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when authentication or storage fails.
    pub async fn query(
        &self,
        actor: ActorContext,
        query: CoordinatorQuery,
    ) -> Result<QueryResult, CoordinatorError> {
        let ActorContext::Session { capability } = actor else {
            return Err(CoordinatorError::new(
                ErrorCategory::Unauthenticated,
                "a live Session capability is required",
            ));
        };
        self.authenticate(&capability).await?;
        match query {
            CoordinatorQuery::ListHarnesses => {
                let rows = sqlx::query("SELECT id FROM harnesses ORDER BY created_at, id")
                    .fetch_all(&self.pool)
                    .await
                    .map_err(CoordinatorError::storage)?;
                let harnesses = rows
                    .iter()
                    .map(|row| HarnessId::from_str(row.get::<&str, _>("id")))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        CoordinatorError::new(ErrorCategory::StorageFailure, error.to_string())
                    })?;
                Ok(QueryResult::Harnesses(harnesses))
            }
            CoordinatorQuery::GetTask { task_id } => {
                let row =
                    sqlx::query("SELECT worker_id, state, result_revision FROM tasks WHERE id = ?")
                        .bind(task_id.to_string())
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(CoordinatorError::storage)?
                        .ok_or_else(|| {
                            CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
                        })?;
                let worker_id =
                    HarnessId::from_str(row.get::<&str, _>("worker_id")).map_err(|error| {
                        CoordinatorError::new(ErrorCategory::StorageFailure, error.to_string())
                    })?;
                let state = TaskState::from_str(row.get::<&str, _>("state"))?;
                let revision: i64 = row.get("result_revision");
                Ok(QueryResult::Task(TaskView {
                    id: task_id,
                    worker_id,
                    state,
                    result_revision: u32::try_from(revision).map_err(CoordinatorError::storage)?,
                }))
            }
        }
    }

    /// Returns the state directory used by this Coordinator.
    #[must_use]
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    async fn register_supervisor(
        &self,
        definition: HarnessDefinitionV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        definition.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        if definition.tier != HarnessTier::Supervisor {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "bootstrap registration requires Supervisor tier",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_sessions s JOIN harnesses h ON h.id = s.harness_id WHERE h.tier = 'supervisor' AND s.ended_at IS NULL",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if active != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "an active Supervisor already exists",
            ));
        }

        let now = timestamp();
        let definition_json =
            serde_json::to_string(&definition).map_err(CoordinatorError::storage)?;
        sqlx::query(
            "INSERT INTO harnesses (id, definition_json, kind, tier, cwd, launch_profile, model, created_at) VALUES (?, ?, ?, 'supervisor', ?, ?, ?, ?)",
        )
        .bind(definition.id.as_str())
        .bind(definition_json)
        .bind(kind_name(definition.kind))
        .bind(definition.cwd.to_string_lossy().as_ref())
        .bind(definition.launch_profile.as_deref())
        .bind(definition.model.as_deref())
        .bind(&now)
        .execute(&mut *transaction)
        .await
        .map_err(|error| CoordinatorError::new(ErrorCategory::Conflict, error.to_string()))?;

        let session_id = HarnessSessionId::new();
        let capability = SessionCapability::generate();
        sqlx::query(
            "INSERT INTO harness_sessions (id, harness_id, harness_tier, capability_hash, connection_generation, presence, activity, started_at, last_seen_at) VALUES (?, ?, 'supervisor', ?, 1, 'online', 'idle', ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(definition.id.as_str())
        .bind(capability.digest())
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorRegistered {
            session_id,
            capability,
        })
    }

    async fn start_worker(
        &self,
        definition: HarnessDefinitionV1,
        profile_snapshot: String,
        profile_digest: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        definition.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        if definition.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Worker start requires Worker tier",
            ));
        }
        validate_digest(&profile_digest)?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let definition_json =
            serde_json::to_string(&definition).map_err(CoordinatorError::storage)?;
        if let Some(existing) =
            sqlx::query_scalar::<_, String>("SELECT definition_json FROM harnesses WHERE id = ?")
                .bind(definition.id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
        {
            if existing != definition_json {
                return Err(CoordinatorError::new(
                    ErrorCategory::Conflict,
                    "Harness ID is already bound to another definition",
                ));
            }
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL",
            )
            .bind(definition.id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if active != 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::Conflict,
                    "Worker already has an active Session",
                ));
            }
        } else {
            sqlx::query("INSERT INTO harnesses (id, definition_json, kind, tier, cwd, launch_profile, model, created_at) VALUES (?, ?, ?, 'worker', ?, ?, ?, ?)")
                .bind(definition.id.as_str())
                .bind(&definition_json)
                .bind(kind_name(definition.kind))
                .bind(definition.cwd.to_string_lossy().as_ref())
                .bind(definition.launch_profile.as_deref())
                .bind(definition.model.as_deref())
                .bind(timestamp())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        let now = timestamp();
        let session_id = HarnessSessionId::new();
        let capability = SessionCapability::generate();
        sqlx::query("INSERT INTO harness_sessions (id, harness_id, harness_tier, capability_hash, connection_generation, presence, activity, profile_snapshot_json, profile_digest, started_at, last_seen_at) VALUES (?, ?, 'worker', ?, 1, 'online', 'idle', ?, ?, ?, ?)")
            .bind(session_id.to_string())
            .bind(definition.id.as_str())
            .bind(capability.digest())
            .bind(profile_snapshot)
            .bind(profile_digest)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::WorkerStarted {
            session_id,
            capability,
        })
    }

    async fn create_task(
        &self,
        actor: &AuthenticatedActor,
        submission: TaskSubmissionV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        submission.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let worker = sqlx::query("SELECT tier, cwd FROM harnesses WHERE id = ?")
            .bind(submission.worker_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::NotFound, "Worker Harness does not exist")
            })?;
        if worker.get::<&str, _>("tier") != "worker" {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Task target is not a Worker",
            ));
        }
        if worker.get::<&str, _>("cwd") != submission.repository.root.to_string_lossy() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Task repository does not match Worker registration",
            ));
        }
        for attachment in &submission.attachments {
            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attachments WHERE id = ?")
                .bind(attachment.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            if exists == 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::NotFound,
                    format!("Attachment {attachment} does not exist"),
                ));
            }
        }
        let sequence = next_sequence(&mut transaction, "task_create").await?;
        let task_id = TaskId::new();
        let message_id = MessageId::new();
        let now = timestamp();
        let submission_json =
            serde_json::to_string(&submission).map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO tasks (id, worker_id, related_task_id, submission_json, state, created_sequence, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(task_id.to_string())
            .bind(submission.worker_id.as_str())
            .bind(submission.related_task_id.map(|id| id.to_string()))
            .bind(&submission_json)
            .bind(TaskState::Queued.as_str())
            .bind(sequence)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO task_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, NULL, 'queued', '{}', ?)")
            .bind(task_id.to_string())
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let message_sequence = next_sequence(&mut transaction, "task_message").await?;
        sqlx::query("INSERT INTO messages (id, task_id, sender_id, recipient_id, kind, body_json, delivery_intent, created_sequence, created_at) VALUES (?, ?, ?, ?, 'task', ?, 'follow_up', ?, ?)")
            .bind(message_id.to_string())
            .bind(task_id.to_string())
            .bind(actor.id.as_str())
            .bind(submission.worker_id.as_str())
            .bind(submission_json)
            .bind(message_sequence)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TaskCreated {
            task_id,
            message_id,
        })
    }

    async fn dispatch_task(&self, task_id: TaskId) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let row = sqlx::query("SELECT worker_id, state FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(row.get("state"), TaskState::Queued)?;
        let worker_id: &str = row.get("worker_id");
        let session_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL AND presence = 'online' ORDER BY started_at DESC LIMIT 1",
        )
        .bind(worker_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let session_id = session_id.ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::TargetOffline, "assigned Worker is offline")
        })?;
        let busy: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND id <> ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
        )
        .bind(worker_id)
        .bind(task_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if busy != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "Worker already owns an active Task",
            ));
        }
        let message = sqlx::query("SELECT id FROM messages WHERE task_id = ? AND kind = 'task'")
            .bind(task_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let message_id = parse_uuid_id::<MessageId>(message.get("id"))?;
        let now = timestamp();
        sqlx::query("UPDATE tasks SET state = 'dispatching', active_session_id = ?, updated_at = ? WHERE id = ?")
            .bind(&session_id)
            .bind(&now)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        record_transition(
            &mut transaction,
            task_id,
            TaskState::Queued,
            TaskState::Dispatching,
            "{}",
            &now,
        )
        .await?;
        create_delivery_attempt(
            &mut transaction,
            message_id,
            &session_id,
            "dispatching",
            false,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TaskDispatching {
            task_id,
            message_id,
        })
    }

    async fn send_message(
        &self,
        actor: &AuthenticatedActor,
        submission: MessageSubmissionV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        submission.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        let recipient_tier: Option<String> =
            sqlx::query_scalar("SELECT tier FROM harnesses WHERE id = ?")
                .bind(submission.to.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        let recipient_tier = recipient_tier.ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::NotFound, "recipient Harness does not exist")
        })?;
        if let Some(task_id) = submission.task_id {
            let task = sqlx::query("SELECT worker_id, state FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .ok_or_else(|| {
                    CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
                })?;
            let worker_id = HarnessId::from_str(task.get::<&str, _>("worker_id"))
                .map_err(CoordinatorError::storage)?;
            let state = TaskState::from_str(task.get("state"))?;
            validate_message_route(actor, &submission, &worker_id, state, &recipient_tier)?;
            if submission.kind == MessageKind::Reply {
                let question_id = submission.reply_to.expect("validated Reply correlation");
                let question: Option<String> = sqlx::query_scalar(
                    "SELECT id FROM messages WHERE id = ? AND task_id = ? AND kind = 'question' AND sender_id = ? AND recipient_id = ?",
                )
                .bind(question_id.to_string())
                .bind(task_id.to_string())
                .bind(worker_id.as_str())
                .bind(actor.id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
                if question.is_none() {
                    return Err(CoordinatorError::new(
                        ErrorCategory::InvalidInput,
                        "Reply does not reference an unanswered Question in this Task",
                    ));
                }
                let replies: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE reply_to = ?")
                        .bind(question_id.to_string())
                        .fetch_one(&mut *transaction)
                        .await
                        .map_err(CoordinatorError::storage)?;
                if replies != 0 {
                    return Err(CoordinatorError::new(
                        ErrorCategory::Conflict,
                        "Question already has a Reply",
                    ));
                }
            }
        } else if submission.kind != MessageKind::Notification {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "only Notification may omit task_id",
            ));
        } else if actor.id == submission.to {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "sender and recipient must differ",
            ));
        }
        let recipient_session: Option<String> = sqlx::query_scalar(
            "SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1",
        )
        .bind(submission.to.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if recipient_session.is_none() {
            return Err(CoordinatorError::new(
                ErrorCategory::TargetOffline,
                "recipient Harness is offline",
            ));
        }
        let sequence = next_sequence(&mut transaction, "message").await?;
        let message_id = MessageId::new();
        let body = serde_json::to_string(&submission).map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO messages (id, task_id, sender_id, recipient_id, kind, body_json, reply_to, delivery_intent, created_sequence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(message_id.to_string())
            .bind(submission.task_id.map(|id| id.to_string()))
            .bind(actor.id.as_str())
            .bind(submission.to.as_str())
            .bind(message_kind_name(submission.kind))
            .bind(body)
            .bind(submission.reply_to.map(|id| id.to_string()))
            .bind(delivery_intent_name(submission.delivery))
            .bind(sequence)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        create_delivery_attempt(
            &mut transaction,
            message_id,
            recipient_session.as_deref().expect("checked Session"),
            "pending",
            false,
            &now,
        )
        .await?;
        if submission.kind == MessageKind::Question {
            let task_id = submission.task_id.expect("validated Task correlation");
            sqlx::query("UPDATE tasks SET state = 'waiting', updated_at = ? WHERE id = ?")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            record_transition(
                &mut transaction,
                task_id,
                TaskState::Working,
                TaskState::Waiting,
                "{}",
                &now,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::MessageCreated { message_id })
    }

    async fn accept_delivery(
        &self,
        actor: &AuthenticatedActor,
        message_id: MessageId,
        native_correlation: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if native_correlation.is_empty() || native_correlation.len() > 512 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "native correlation must contain 1 to 512 bytes",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let message = sqlx::query("SELECT task_id, recipient_id, kind FROM messages WHERE id = ?")
            .bind(message_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::NotFound, "Message does not exist")
            })?;
        if message.get::<&str, _>("recipient_id") != actor.id.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the destination Host may accept delivery",
            ));
        }
        let changed = sqlx::query("UPDATE delivery_attempts SET state = 'accepted', native_correlation = ?, provider_bytes_may_have_been_written = 1, updated_at = ? WHERE message_id = ? AND attempt_number = 1 AND state IN ('pending','dispatching')")
            .bind(native_correlation)
            .bind(timestamp())
            .bind(message_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Message is not awaiting native acceptance",
            ));
        }
        if let Some(task_text) = message.get::<Option<&str>, _>("task_id") {
            let task_id = parse_uuid_id::<TaskId>(task_text)?;
            let now = timestamp();
            match message.get::<&str, _>("kind") {
                "task" => {
                    transition_exact(
                        &mut transaction,
                        task_id,
                        TaskState::Dispatching,
                        TaskState::Working,
                        false,
                        &now,
                    )
                    .await?
                }
                "reply" => {
                    transition_exact(
                        &mut transaction,
                        task_id,
                        TaskState::Waiting,
                        TaskState::Working,
                        false,
                        &now,
                    )
                    .await?
                }
                "correction" => {
                    transition_exact(
                        &mut transaction,
                        task_id,
                        TaskState::Reviewing,
                        TaskState::Working,
                        true,
                        &now,
                    )
                    .await?
                }
                _ => {}
            }
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::DeliveryAccepted { message_id })
    }

    async fn complete_task(
        &self,
        actor: &AuthenticatedActor,
        manifest: ResultManifestV1,
        native_turn_id: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        manifest.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        if native_turn_id.is_empty() || native_turn_id.len() > 512 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "native turn ID must contain 1 to 512 bytes",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT worker_id, state, result_revision FROM tasks WHERE id = ?")
            .bind(manifest.task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        if task.get::<&str, _>("worker_id") != actor.id.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the assigned Worker may complete this Task",
            ));
        }
        require_state(task.get("state"), TaskState::Working)?;
        let revision_i64: i64 = task.get("result_revision");
        let revision = u32::try_from(revision_i64).map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO results (task_id, revision, native_turn_id, manifest_json, accepted_at) VALUES (?, ?, ?, ?, ?)")
            .bind(manifest.task_id.to_string())
            .bind(revision_i64)
            .bind(native_turn_id)
            .bind(serde_json::to_string(&manifest).map_err(CoordinatorError::storage)?)
            .bind(timestamp())
            .execute(&mut *transaction)
            .await
            .map_err(|error| CoordinatorError::new(ErrorCategory::Conflict, error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::ResultRecorded {
            task_id: manifest.task_id,
            revision,
        })
    }

    async fn record_turn_completed(
        &self,
        actor: &AuthenticatedActor,
        task_id: TaskId,
        native_turn_id: String,
        succeeded: bool,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT worker_id, state, result_revision FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        if task.get::<&str, _>("worker_id") != actor.id.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the assigned Worker Host may report turn completion",
            ));
        }
        require_state(task.get("state"), TaskState::Working)?;
        let revision: i64 = task.get("result_revision");
        let matching_result: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM results WHERE task_id = ? AND revision = ? AND native_turn_id = ?")
            .bind(task_id.to_string())
            .bind(revision)
            .bind(&native_turn_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let next = if succeeded && matching_result == 1 {
            sqlx::query("UPDATE results SET terminal_at = ? WHERE task_id = ? AND revision = ?")
                .bind(timestamp())
                .bind(task_id.to_string())
                .bind(revision)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            TaskState::Reviewing
        } else {
            TaskState::Failed
        };
        let now = timestamp();
        transition_exact(
            &mut transaction,
            task_id,
            TaskState::Working,
            next,
            false,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TurnCompleted {
            task_id,
            state: next,
        })
    }

    async fn authenticate(
        &self,
        capability: &SessionCapability,
    ) -> Result<AuthenticatedActor, CoordinatorError> {
        let row = sqlx::query(
            "SELECT h.id, h.tier, s.id AS session_id FROM harness_sessions s JOIN harnesses h ON h.id = s.harness_id WHERE s.capability_hash = ? AND s.ended_at IS NULL AND s.presence IN ('starting', 'online', 'disconnected')",
        )
        .bind(capability.digest())
        .fetch_optional(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?
        .ok_or_else(|| CoordinatorError::new(ErrorCategory::Unauthenticated, "Session capability is invalid or expired"))?;
        let id = HarnessId::from_str(row.get::<&str, _>("id")).map_err(|error| {
            CoordinatorError::new(ErrorCategory::StorageFailure, error.to_string())
        })?;
        let tier = match row.get::<&str, _>("tier") {
            "supervisor" => HarnessTier::Supervisor,
            "worker" => HarnessTier::Worker,
            value => {
                return Err(CoordinatorError::new(
                    ErrorCategory::StorageFailure,
                    format!("unknown Harness tier `{value}`"),
                ));
            }
        };
        let session_id = HarnessSessionId(
            Uuid::parse_str(row.get::<&str, _>("session_id")).map_err(CoordinatorError::storage)?,
        );
        Ok(AuthenticatedActor {
            id,
            tier,
            session_id,
        })
    }

    fn require_supervisor(&self, actor: &AuthenticatedActor) -> Result<(), CoordinatorError> {
        if actor.tier == HarnessTier::Supervisor {
            Ok(())
        } else {
            Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "Supervisor authority is required",
            ))
        }
    }
}

#[derive(Debug)]
struct AuthenticatedActor {
    id: HarnessId,
    tier: HarnessTier,
    #[expect(dead_code, reason = "retained for Session-bound command authorization")]
    session_id: HarnessSessionId,
}

fn validate_message_route(
    actor: &AuthenticatedActor,
    submission: &MessageSubmissionV1,
    worker_id: &HarnessId,
    state: TaskState,
    recipient_tier: &str,
) -> Result<(), CoordinatorError> {
    let valid = match (actor.tier, submission.kind) {
        (HarnessTier::Worker, MessageKind::Question) => {
            actor.id == *worker_id
                && recipient_tier == "supervisor"
                && state == TaskState::Working
                && submission.delivery == DeliveryIntent::FollowUp
        }
        (HarnessTier::Worker, MessageKind::Notification) => {
            actor.id == *worker_id && recipient_tier == "supervisor"
        }
        (HarnessTier::Supervisor, MessageKind::Reply) => {
            submission.to == *worker_id
                && state == TaskState::Waiting
                && submission.delivery == DeliveryIntent::FollowUp
        }
        (HarnessTier::Supervisor, MessageKind::Correction) => {
            submission.to == *worker_id
                && state == TaskState::Reviewing
                && submission.delivery == DeliveryIntent::FollowUp
        }
        (HarnessTier::Supervisor, MessageKind::Notification) => {
            submission.to == *worker_id
                && (submission.delivery == DeliveryIntent::FollowUp || state == TaskState::Working)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(CoordinatorError::new(
            ErrorCategory::Forbidden,
            "Message route, lifecycle state, or delivery intent is not permitted",
        ))
    }
}

async fn create_delivery_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message_id: MessageId,
    session_id: &str,
    state: &str,
    provider_bytes_may_have_been_written: bool,
    now: &str,
) -> Result<(), CoordinatorError> {
    sqlx::query("INSERT INTO delivery_attempts (id, message_id, attempt_number, target_session_id, state, provider_bytes_may_have_been_written, evidence_json, created_at, updated_at) VALUES (?, ?, 1, ?, ?, ?, '{}', ?, ?)")
        .bind(DeliveryAttemptId::new().to_string())
        .bind(message_id.to_string())
        .bind(session_id)
        .bind(state)
        .bind(provider_bytes_may_have_been_written)
        .bind(now)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(())
}

async fn transition_exact(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: TaskId,
    from: TaskState,
    to: TaskState,
    increment_revision: bool,
    now: &str,
) -> Result<(), CoordinatorError> {
    let changed = if increment_revision {
        sqlx::query("UPDATE tasks SET state = ?, updated_at = ?, result_revision = result_revision + 1 WHERE id = ? AND state = ?")
            .bind(to.as_str())
            .bind(now)
            .bind(task_id.to_string())
            .bind(from.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected()
    } else {
        sqlx::query("UPDATE tasks SET state = ?, updated_at = ? WHERE id = ? AND state = ?")
            .bind(to.as_str())
            .bind(now)
            .bind(task_id.to_string())
            .bind(from.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected()
    };
    if changed != 1 {
        return Err(CoordinatorError::new(
            ErrorCategory::InvalidState,
            format!("Task must be {}", from.as_str()),
        ));
    }
    record_transition(transaction, task_id, from, to, "{}", now).await
}

async fn record_transition(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: TaskId,
    from: TaskState,
    to: TaskState,
    evidence: &str,
    now: &str,
) -> Result<(), CoordinatorError> {
    sqlx::query("INSERT INTO task_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, ?, ?, ?, ?)")
        .bind(task_id.to_string())
        .bind(from.as_str())
        .bind(to.as_str())
        .bind(evidence)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(())
}

fn require_state(actual: &str, expected: TaskState) -> Result<(), CoordinatorError> {
    if actual == expected.as_str() {
        Ok(())
    } else {
        Err(CoordinatorError::new(
            ErrorCategory::InvalidState,
            format!("Task must be {}", expected.as_str()),
        ))
    }
}

trait UuidIdentity: Sized {
    fn from_uuid(uuid: Uuid) -> Self;
}

impl UuidIdentity for MessageId {
    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl UuidIdentity for TaskId {
    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

fn parse_uuid_id<T: UuidIdentity>(value: &str) -> Result<T, CoordinatorError> {
    Uuid::parse_str(value)
        .map(T::from_uuid)
        .map_err(CoordinatorError::storage)
}

fn message_kind_name(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Question => "question",
        MessageKind::Reply => "reply",
        MessageKind::Correction => "correction",
        MessageKind::Notification => "notification",
    }
}

fn delivery_intent_name(intent: DeliveryIntent) -> &'static str {
    match intent {
        DeliveryIntent::FollowUp => "follow_up",
        DeliveryIntent::Steer => "steer",
    }
}

async fn next_sequence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    purpose: &str,
) -> Result<i64, CoordinatorError> {
    let result = sqlx::query("INSERT INTO global_sequences (purpose) VALUES (?)")
        .bind(purpose)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(result.last_insert_rowid())
}

fn validate_digest(value: &str) -> Result<(), CoordinatorError> {
    let valid = value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if valid {
        Ok(())
    } else {
        Err(CoordinatorError::new(
            ErrorCategory::InvalidInput,
            "profile digest must be lowercase SHA-256",
        ))
    }
}

fn kind_name(kind: crate::contract::HarnessKind) -> &'static str {
    match kind {
        crate::contract::HarnessKind::Omp => "omp",
        crate::contract::HarnessKind::Codex => "codex",
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}
