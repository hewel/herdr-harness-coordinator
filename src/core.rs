//! Transactional command/query boundary for durable Coordinator state.

use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use chrono::{SecondsFormat, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use uuid::Uuid;

use crate::attachment::{AttachmentMetadata, AttachmentStore};
use crate::contract::{
    CommandEvidenceV1, DeliveryAttemptId, DeliveryIntent, HarnessDefinitionV1, HarnessId,
    HarnessLaunchProfileV1, HarnessSessionId, HarnessTier, MessageId, MessageKind,
    MessageSubmissionV1, ObservationCheckpoint, RepositoryAccess, RepositoryObservationId,
    RepositoryObservationV1, ResultManifestV1, SCHEMA_VERSION, ScopeClassification, TaskId,
    TaskSubmissionV1, Validate, WorktreeHoldId, WriteScopeV1,
};
use crate::repository::{GitRepository, RepositorySnapshot};

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

    pub(crate) fn storage(error: impl std::fmt::Display) -> Self {
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

    /// Parses the opaque bearer passed to a pane-resident Host.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when the bearer does not have the generated v1 shape.
    pub fn from_bearer(value: impl Into<String>) -> Result<Self, CoordinatorError> {
        let value = value.into();
        let valid = value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        if valid {
            Ok(Self(value))
        } else {
            Err(CoordinatorError::new(
                ErrorCategory::Unauthenticated,
                "Session capability has an invalid shape",
            ))
        }
    }
}

/// Authenticated command/query actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ActorContext {
    /// Initial sole-Supervisor registration only.
    Bootstrap,
    /// Live Session authenticated by capability.
    Session { capability: SessionCapability },
}

/// State-changing operations accepted by [`Coordinator::execute`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CoordinatorCommand {
    /// Copy a regular file into immutable Coordinator-owned storage.
    AdmitAttachment {
        source: PathBuf,
        media_type: String,
        original_name: String,
    },
    /// Mark admitted inbox Messages as observed by their recipient.
    MarkInboxRead { message_ids: Vec<MessageId> },
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
    /// Fail a Worker Session whose Herdr pane could not be opened.
    AbortWorkerStart {
        worker_id: HarnessId,
        diagnostic: String,
    },
    /// Create a bounded Task and its root Task message atomically.
    CreateTask {
        /// Validated Supervisor intent.
        submission: TaskSubmissionV1,
    },
    /// Begin delivery of the queued root Task message.
    DispatchTask { task_id: TaskId },
    /// Atomically claim the assigned Worker's oldest eligible queued Task.
    ClaimNextTask,
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
    /// Capture and persist immutable Git evidence for one Task checkpoint.
    CaptureRepositoryObservation {
        task_id: TaskId,
        checkpoint: ObservationCheckpoint,
    },
    /// Accept the current reviewable Result against exact repository evidence.
    ApproveTask {
        task_id: TaskId,
        result_revision: u32,
        observation_digest: String,
    },
    /// Request cancellation, or cancel immediately while still queued.
    CancelTask { task_id: TaskId },
    /// Confirm that the assigned provider settled a cancellation request.
    RecordCancellationCompleted { task_id: TaskId, succeeded: bool },
    /// Record that provider acceptance cannot be proven either way.
    MarkDeliveryUnknown {
        message_id: MessageId,
        diagnostic: String,
    },
    /// Reconcile an uncertain dispatch against current repository evidence.
    ResolveDeliveryUnknown {
        task_id: TaskId,
        resolution: DeliveryUnknownResolution,
        observation_digest: String,
        audit_note: String,
    },
    /// Clear a reconciled Worktree Hold without modifying repository files.
    ClearWorktreeHold {
        task_id: TaskId,
        observation_digest: String,
        audit_note: String,
    },
    /// Request a Worker Host and provider process to stop.
    StopWorker { worker_id: HarnessId },
    /// Confirm Worker Host process shutdown.
    RecordHostStopped { clean: bool },
    /// Record Worker Host failure and conservatively settle active work.
    RecordHostFailed { diagnostic: String },
    /// Mark a Worker online only after its pane Host and native Adapter are ready.
    RecordHostReady,
    /// Persist one monotonic pane-resident Host event for reconnect replay.
    RecordHostEvent { sequence: u64, event: Value },
}

/// Explicit Supervisor resolution after ambiguous native acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryUnknownResolution {
    /// Create a new audited delivery attempt after proving repository state.
    Requeue,
    /// Terminate the Task without replaying the uncertain native request.
    Cancel,
}

/// Read-only operations accepted by [`Coordinator::query`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CoordinatorQuery {
    /// Return durable Harness identities in creation order.
    ListHarnesses,
    /// Return one Task projection.
    GetTask {
        /// Task to retrieve.
        task_id: TaskId,
    },
    /// Return all Tasks in durable FIFO order.
    ListTasks,
    /// Return unread Messages addressed to the authenticated Harness.
    Inbox,
    /// Return one row per durable Harness for popup presentation.
    HarnessStatus,
    /// Return unresolved Worktree Holds (Supervisor only).
    ActiveHolds,
    /// Return launch context for the authenticated Harness Host.
    SessionSelf,
    /// Return immutable Attachment metadata for authorized local delivery.
    GetAttachment {
        attachment_id: crate::contract::AttachmentId,
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

/// Compact durable Harness and live Session projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessStatusView {
    /// Durable Harness identity.
    pub id: HarnessId,
    /// Supervisor or Worker.
    pub tier: HarnessTier,
    /// Latest Session presence, or `offline` when no live Session exists.
    pub presence: String,
    /// Latest Session activity.
    pub activity: String,
    /// Unread durable Message count.
    pub unread_messages: u32,
    /// Current active Task, when one exists.
    pub active_task_id: Option<TaskId>,
}

/// Durable inbox Message projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxMessageView {
    /// Message identity.
    pub id: MessageId,
    /// Related Task, when present.
    pub task_id: Option<TaskId>,
    /// Authenticated sender identity.
    pub sender_id: HarnessId,
    /// Public or reserved Message kind.
    pub kind: String,
    /// Original versioned Message JSON.
    pub body: Value,
    /// Latest native delivery state, when delivery applies.
    pub delivery_state: Option<String>,
}

/// Unresolved advisory Worktree Hold projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldView {
    /// Hold identity.
    pub id: WorktreeHoldId,
    /// Canonical repository scheduling key.
    pub repository_key: String,
    /// Task responsible for the Hold.
    pub task_id: TaskId,
    /// Stable reason.
    pub reason: String,
}

/// Session-bound launch context returned only to its capability holder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSelfView {
    /// Live Coordinator Session identity.
    pub session_id: HarnessSessionId,
    /// Durable Harness definition.
    pub definition: HarnessDefinitionV1,
    /// Exact selected launch profile source for Workers.
    pub profile_snapshot: Option<String>,
    /// SHA-256 of the selected profile source.
    pub profile_digest: Option<String>,
    /// Current Coordinator presence projection.
    pub presence: String,
    /// Current Coordinator activity projection.
    pub activity: String,
    /// Latest durable pane-resident Host event sequence.
    pub event_sequence: u64,
}

/// Successful command outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutcome {
    /// File was copied, hashed, and indexed.
    AttachmentAdmitted { attachment: AttachmentMetadata },
    /// Inbox read markers were persisted.
    InboxMarkedRead { count: u32 },
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
    /// Worker currently has no eligible queued Task.
    NoTaskAvailable,
    /// Public Bus Message was durably admitted.
    MessageCreated { message_id: MessageId },
    /// A native delivery was accepted and its effects applied.
    DeliveryAccepted { message_id: MessageId },
    /// A Result candidate was durably admitted.
    ResultRecorded { task_id: TaskId, revision: u32 },
    /// A native turn ended and the Task state was updated.
    TurnCompleted { task_id: TaskId, state: TaskState },
    /// Repository evidence was durably indexed.
    ObservationRecorded { task_id: TaskId, digest: String },
    /// Supervisor approved the current Result revision.
    TaskApproved { task_id: TaskId },
    /// Cancellation state was durably updated.
    TaskCancellationUpdated { task_id: TaskId, state: TaskState },
    /// Ambiguous delivery was recorded or explicitly reconciled.
    DeliveryUnknownUpdated { task_id: TaskId, state: TaskState },
    /// A digest-confirmed Worktree Hold was cleared.
    HoldCleared { task_id: TaskId },
    /// Worker Session was asked to stop.
    WorkerStopping { worker_id: HarnessId },
    /// Worker Host shutdown was durably settled.
    HostStopped { clean: bool },
    /// Worker Host and native Adapter are ready for dispatch.
    HostReady,
    /// Monotonic Host event was persisted or replayed idempotently.
    HostEventRecorded { sequence: u64 },
}

/// Successful query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryResult {
    /// Durable Harness identities.
    Harnesses(Vec<HarnessId>),
    /// One durable Task.
    Task(TaskView),
    /// Durable Tasks in FIFO order.
    Tasks(Vec<TaskView>),
    /// Unread inbox Messages.
    Inbox(Vec<InboxMessageView>),
    /// Popup-oriented Harness rows.
    HarnessStatus(Vec<HarnessStatusView>),
    /// Active advisory Holds.
    Holds(Vec<HoldView>),
    /// Authenticated Session launch context.
    Session(SessionSelfView),
    /// Immutable Attachment metadata.
    Attachment(AttachmentMetadata),
}

/// One Coordinator daemon's deep transactional state module.
#[derive(Debug, Clone)]
pub struct Coordinator {
    pool: SqlitePool,
    state_dir: PathBuf,
    lease_files: Arc<tokio::sync::Mutex<BTreeMap<TaskId, File>>>,
    issued_capabilities: Arc<tokio::sync::Mutex<BTreeMap<HarnessSessionId, SessionCapability>>>,
}

impl Coordinator {
    /// Opens or initializes Coordinator state beneath `state_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when directories, `SQLite`, or migrations fail.
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
        let coordinator = Self {
            pool,
            state_dir,
            lease_files: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            issued_capabilities: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        };
        let leases = sqlx::query(
            "SELECT repository_key, task_id FROM worktree_leases WHERE released_at IS NULL",
        )
        .fetch_all(&coordinator.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        for lease in leases {
            let task_id = parse_uuid_id::<TaskId>(lease.get("task_id"))?;
            let file = coordinator.try_lock_worktree(lease.get("repository_key"))?;
            coordinator.lease_files.lock().await.insert(task_id, file);
        }
        Ok(coordinator)
    }

    /// Executes one authenticated command atomically.
    ///
    /// # Errors
    ///
    /// Returns a stable [`CoordinatorError`] for validation, authorization, conflict, or storage failure.
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive command authorization boundary"
    )]
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
                CoordinatorCommand::AdmitAttachment {
                    source,
                    media_type,
                    original_name,
                },
            ) => {
                self.authenticate(&capability).await?;
                self.admit_attachment(source, media_type, original_name)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::MarkInboxRead { message_ids },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.mark_inbox_read(&actor, message_ids).await
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
                CoordinatorCommand::AbortWorkerStart {
                    worker_id,
                    diagnostic,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.abort_worker_start(worker_id, diagnostic).await
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
            (ActorContext::Session { capability }, CoordinatorCommand::ClaimNextTask) => {
                let actor = self.authenticate(&capability).await?;
                if actor.tier != HarnessTier::Worker {
                    return Err(CoordinatorError::new(
                        ErrorCategory::Forbidden,
                        "only a Worker Host may claim queued work",
                    ));
                }
                self.claim_next_task(&actor).await
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
            (
                ActorContext::Session { capability },
                CoordinatorCommand::CaptureRepositoryObservation {
                    task_id,
                    checkpoint,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.capture_repository_observation(&actor, task_id, checkpoint)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ApproveTask {
                    task_id,
                    result_revision,
                    observation_digest,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.approve_task(task_id, result_revision, observation_digest)
                    .await
            }
            (ActorContext::Session { capability }, CoordinatorCommand::CancelTask { task_id }) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.cancel_task(task_id).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordCancellationCompleted { task_id, succeeded },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_cancellation_completed(&actor, task_id, succeeded)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::MarkDeliveryUnknown {
                    message_id,
                    diagnostic,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.mark_delivery_unknown(&actor, message_id, diagnostic)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ResolveDeliveryUnknown {
                    task_id,
                    resolution,
                    observation_digest,
                    audit_note,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.resolve_delivery_unknown(task_id, resolution, observation_digest, audit_note)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ClearWorktreeHold {
                    task_id,
                    observation_digest,
                    audit_note,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.clear_worktree_hold(task_id, observation_digest, audit_note)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::StopWorker { worker_id },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.stop_worker(worker_id).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordHostStopped { clean },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_stopped(&actor, clean).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordHostFailed { diagnostic },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_failed(&actor, diagnostic).await
            }
            (ActorContext::Session { capability }, CoordinatorCommand::RecordHostReady) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_ready(&actor).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordHostEvent { sequence, event },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_event(&actor, sequence, event).await
            }
            _ => Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "command is not permitted for this actor",
            )),
        }
    }

    /// Executes one authenticated query without exposing `SQLite` internals.
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
        let actor = self.authenticate(&capability).await?;
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
            CoordinatorQuery::ListTasks => self.list_tasks().await,
            CoordinatorQuery::Inbox => self.inbox(&actor).await,
            CoordinatorQuery::HarnessStatus => self.harness_status().await,
            CoordinatorQuery::ActiveHolds => {
                self.require_supervisor(&actor)?;
                self.active_holds().await
            }
            CoordinatorQuery::SessionSelf => self.session_self(&actor).await,
            CoordinatorQuery::GetAttachment { attachment_id } => {
                self.get_attachment(attachment_id).await
            }
        }
    }

    /// Returns the state directory used by this Coordinator.
    #[must_use]
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    fn try_lock_worktree(&self, repository_key: &str) -> Result<File, CoordinatorError> {
        let directory = self.state_dir.join("worktree-locks");
        std::fs::create_dir_all(&directory).map_err(CoordinatorError::storage)?;
        let name = hex::encode(Sha256::digest(repository_key.as_bytes()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(format!("{name}.lock")))
            .map_err(CoordinatorError::storage)?;
        file.try_lock().map_err(|error| {
            CoordinatorError::new(
                ErrorCategory::RepositoryBlocked,
                format!("worktree OS lease lock is held: {error}"),
            )
        })?;
        Ok(file)
    }

    async fn release_lease_file(&self, task_id: TaskId) {
        self.lease_files.lock().await.remove(&task_id);
    }

    async fn admit_attachment(
        &self,
        source: PathBuf,
        media_type: String,
        original_name: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if media_type.is_empty() || media_type.len() > 255 || media_type.contains(['\r', '\n']) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "media type must contain 1 to 255 bytes without line breaks",
            ));
        }
        if original_name.is_empty()
            || original_name.len() > 255
            || Path::new(&original_name)
                .file_name()
                .and_then(|name| name.to_str())
                != Some(original_name.as_str())
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "original name must be a basename containing 1 to 255 bytes",
            ));
        }
        let attachment = AttachmentStore::new(&self.state_dir)
            .admit(&source, &media_type, &original_name)
            .await
            .map_err(|error| {
                CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
            })?;
        let size = i64::try_from(attachment.size_bytes).map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO attachments (id, digest, byte_size, media_type, original_name, storage_path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(attachment.id.to_string())
            .bind(&attachment.digest)
            .bind(size)
            .bind(&attachment.media_type)
            .bind(&attachment.original_name)
            .bind(attachment.storage_path.to_string_lossy().as_ref())
            .bind(timestamp())
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::AttachmentAdmitted { attachment })
    }

    async fn mark_inbox_read(
        &self,
        actor: &AuthenticatedActor,
        message_ids: Vec<MessageId>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if message_ids.len() > 256 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "at most 256 inbox Messages may be marked at once",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let mut count = 0_u32;
        for message_id in message_ids {
            let changed = sqlx::query("INSERT OR IGNORE INTO inbox_reads (harness_id, message_id, read_at) SELECT ?, id, ? FROM messages WHERE id = ? AND recipient_id = ?")
                .bind(actor.id.as_str())
                .bind(timestamp())
                .bind(message_id.to_string())
                .bind(actor.id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .rows_affected();
            count = count.saturating_add(u32::try_from(changed).unwrap_or(u32::MAX));
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::InboxMarkedRead { count })
    }

    async fn list_tasks(&self) -> Result<QueryResult, CoordinatorError> {
        let rows = sqlx::query(
            "SELECT id, worker_id, state, result_revision FROM tasks ORDER BY created_sequence",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        let tasks = rows
            .iter()
            .map(task_view_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(QueryResult::Tasks(tasks))
    }

    async fn inbox(&self, actor: &AuthenticatedActor) -> Result<QueryResult, CoordinatorError> {
        let rows = sqlx::query(
            "SELECT m.id, m.task_id, m.sender_id, m.kind, m.body_json, (SELECT state FROM delivery_attempts d WHERE d.message_id = m.id ORDER BY d.attempt_number DESC LIMIT 1) AS delivery_state FROM messages m LEFT JOIN inbox_reads r ON r.harness_id = ? AND r.message_id = m.id WHERE m.recipient_id = ? AND r.message_id IS NULL AND (m.kind <> 'task' OR EXISTS (SELECT 1 FROM delivery_attempts d WHERE d.message_id = m.id)) ORDER BY m.created_sequence",
        )
        .bind(actor.id.as_str())
        .bind(actor.id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        let messages = rows
            .iter()
            .map(|row| {
                Ok(InboxMessageView {
                    id: parse_uuid_id(row.get("id"))?,
                    task_id: row
                        .get::<Option<&str>, _>("task_id")
                        .map(parse_uuid_id)
                        .transpose()?,
                    sender_id: HarnessId::from_str(row.get("sender_id"))
                        .map_err(CoordinatorError::storage)?,
                    kind: row.get::<&str, _>("kind").to_owned(),
                    body: serde_json::from_str(row.get("body_json"))
                        .map_err(CoordinatorError::storage)?,
                    delivery_state: row
                        .get::<Option<&str>, _>("delivery_state")
                        .map(str::to_owned),
                })
            })
            .collect::<Result<Vec<_>, CoordinatorError>>()?;
        Ok(QueryResult::Inbox(messages))
    }

    async fn harness_status(&self) -> Result<QueryResult, CoordinatorError> {
        let rows = sqlx::query(
            "SELECT h.id, h.tier, COALESCE((SELECT presence FROM harness_sessions s WHERE s.harness_id = h.id AND s.ended_at IS NULL ORDER BY s.started_at DESC LIMIT 1), 'offline') AS presence, COALESCE((SELECT activity FROM harness_sessions s WHERE s.harness_id = h.id AND s.ended_at IS NULL ORDER BY s.started_at DESC LIMIT 1), 'idle') AS activity, (SELECT COUNT(*) FROM messages m LEFT JOIN inbox_reads r ON r.harness_id = h.id AND r.message_id = m.id WHERE m.recipient_id = h.id AND r.message_id IS NULL) AS unread_messages, (SELECT id FROM tasks t WHERE t.worker_id = h.id AND t.state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown') ORDER BY t.created_sequence LIMIT 1) AS active_task_id FROM harnesses h ORDER BY h.created_at, h.id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        let status = rows
            .iter()
            .map(|row| {
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
                let unread: i64 = row.get("unread_messages");
                Ok(HarnessStatusView {
                    id: HarnessId::from_str(row.get("id")).map_err(CoordinatorError::storage)?,
                    tier,
                    presence: row.get::<&str, _>("presence").to_owned(),
                    activity: row.get::<&str, _>("activity").to_owned(),
                    unread_messages: u32::try_from(unread).map_err(CoordinatorError::storage)?,
                    active_task_id: row
                        .get::<Option<&str>, _>("active_task_id")
                        .map(parse_uuid_id)
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, CoordinatorError>>()?;
        Ok(QueryResult::HarnessStatus(status))
    }

    async fn active_holds(&self) -> Result<QueryResult, CoordinatorError> {
        let rows = sqlx::query("SELECT id, repository_key, task_id, reason FROM worktree_holds WHERE cleared_at IS NULL ORDER BY created_at")
            .fetch_all(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        let holds = rows
            .iter()
            .map(|row| {
                Ok(HoldView {
                    id: parse_uuid_id(row.get("id"))?,
                    repository_key: row.get::<&str, _>("repository_key").to_owned(),
                    task_id: parse_uuid_id(row.get("task_id"))?,
                    reason: row.get::<&str, _>("reason").to_owned(),
                })
            })
            .collect::<Result<Vec<_>, CoordinatorError>>()?;
        Ok(QueryResult::Holds(holds))
    }

    async fn session_self(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<QueryResult, CoordinatorError> {
        let row = sqlx::query("SELECT h.definition_json, s.profile_snapshot_json, s.profile_digest, s.presence, s.activity, s.event_sequence FROM harness_sessions s JOIN harnesses h ON h.id = s.harness_id WHERE s.id = ? AND s.ended_at IS NULL")
            .bind(actor.session_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Session is no longer active"))?;
        Ok(QueryResult::Session(SessionSelfView {
            session_id: actor.session_id,
            definition: serde_json::from_str(row.get("definition_json"))
                .map_err(CoordinatorError::storage)?,
            profile_snapshot: row
                .get::<Option<&str>, _>("profile_snapshot_json")
                .map(str::to_owned),
            profile_digest: row
                .get::<Option<&str>, _>("profile_digest")
                .map(str::to_owned),
            presence: row.get::<&str, _>("presence").to_owned(),
            activity: row.get::<&str, _>("activity").to_owned(),
            event_sequence: u64::try_from(row.get::<i64, _>("event_sequence"))
                .map_err(CoordinatorError::storage)?,
        }))
    }

    async fn get_attachment(
        &self,
        attachment_id: crate::contract::AttachmentId,
    ) -> Result<QueryResult, CoordinatorError> {
        let row = sqlx::query("SELECT digest, byte_size, media_type, original_name, storage_path FROM attachments WHERE id = ?")
            .bind(attachment_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Attachment does not exist"))?;
        let size: i64 = row.get("byte_size");
        Ok(QueryResult::Attachment(AttachmentMetadata {
            id: attachment_id,
            digest: row.get::<&str, _>("digest").to_owned(),
            size_bytes: u64::try_from(size).map_err(CoordinatorError::storage)?,
            media_type: row.get::<&str, _>("media_type").to_owned(),
            original_name: row.get::<&str, _>("original_name").to_owned(),
            storage_path: PathBuf::from(row.get::<&str, _>("storage_path")),
        }))
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

    #[expect(
        clippy::too_many_lines,
        reason = "atomic durable Worker registration transaction"
    )]
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
        let actual_digest = hex::encode(Sha256::digest(profile_snapshot.as_bytes()));
        if actual_digest != profile_digest {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "launch profile snapshot digest does not match",
            ));
        }
        let profile: HarnessLaunchProfileV1 =
            toml::from_str(&profile_snapshot).map_err(|error| {
                CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    format!("launch profile snapshot is invalid TOML: {error}"),
                )
            })?;
        profile.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        if profile.kind != definition.kind
            || definition.launch_profile.as_deref() != Some(profile.id.as_str())
            || definition.model != profile.model
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Worker definition does not match the resolved launch profile",
            ));
        }
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
            let active = sqlx::query("SELECT id, presence, profile_digest FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1")
            .bind(definition.id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if let Some(active) = active {
                let session_id = parse_uuid_id::<HarnessSessionId>(active.get("id"))?;
                if active.get::<&str, _>("presence") == "starting"
                    && active.get::<Option<&str>, _>("profile_digest")
                        == Some(profile_digest.as_str())
                    && let Some(capability) = self
                        .issued_capabilities
                        .lock()
                        .await
                        .get(&session_id)
                        .cloned()
                {
                    return Ok(CommandOutcome::WorkerStarted {
                        session_id,
                        capability,
                    });
                }
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
        sqlx::query("INSERT INTO harness_sessions (id, harness_id, harness_tier, capability_hash, connection_generation, presence, activity, profile_snapshot_json, profile_digest, started_at, last_seen_at) VALUES (?, ?, 'worker', ?, 1, 'starting', 'starting', ?, ?, ?, ?)")
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
        self.issued_capabilities
            .lock()
            .await
            .insert(session_id, capability.clone());
        Ok(CommandOutcome::WorkerStarted {
            session_id,
            capability,
        })
    }

    async fn abort_worker_start(
        &self,
        worker_id: HarnessId,
        diagnostic: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if diagnostic.trim().is_empty() || diagnostic.len() > 4096 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Worker start diagnostic must contain 1 to 4096 bytes",
            ));
        }
        let now = timestamp();
        let changed = sqlx::query("UPDATE harness_sessions SET presence = 'failed', activity = 'failed', ended_at = ?, last_seen_at = ? WHERE harness_id = ? AND ended_at IS NULL AND presence = 'starting'")
            .bind(&now)
            .bind(&now)
            .bind(worker_id.as_str())
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Worker has no starting Session",
            ));
        }
        Ok(CommandOutcome::HostStopped { clean: false })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "atomic Task, lease, and root Message transaction"
    )]
    async fn create_task(
        &self,
        actor: &AuthenticatedActor,
        submission: TaskSubmissionV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        submission.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let repository = GitRepository::open(&submission.repository.root).map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let canonical_root = &repository.identity().worktree_root;
        validate_scope_paths(canonical_root, &submission.repository.write_scopes)?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let payload_digest = canonical_digest(&submission)?;
        if let Some(outcome) = find_idempotent_outcome(
            &mut transaction,
            actor,
            "create_task",
            submission.request_key.as_deref(),
            &payload_digest,
        )
        .await?
        {
            return Ok(outcome);
        }
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
        if worker.get::<&str, _>("cwd") != canonical_root.to_string_lossy() {
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
        let outcome = CommandOutcome::TaskCreated {
            task_id,
            message_id,
        };
        store_idempotent_outcome(
            &mut transaction,
            actor,
            "create_task",
            submission.request_key.as_deref(),
            &payload_digest,
            &outcome,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(outcome)
    }

    async fn dispatch_task(&self, task_id: TaskId) -> Result<CommandOutcome, CoordinatorError> {
        self.preflight_dispatch(task_id).await?;
        self.capture_repository_checkpoint(task_id, ObservationCheckpoint::BeforeDispatch)
            .await?;
        let outcome = self.commit_dispatch(task_id).await;
        if outcome.as_ref().is_err_and(|error| {
            matches!(
                error.category,
                ErrorCategory::RepositoryBlocked
                    | ErrorCategory::Conflict
                    | ErrorCategory::TargetOffline
                    | ErrorCategory::InvalidState
            )
        }) {
            self.cleanup_failed_dispatch_baseline(task_id).await?;
        }
        outcome
    }

    #[expect(
        clippy::too_many_lines,
        reason = "dispatch revalidates the durable Task, Session, lease, and root delivery atomically"
    )]
    async fn commit_dispatch(&self, task_id: TaskId) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let row = sqlx::query("SELECT worker_id, state, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(row.get("state"), TaskState::Queued)?;
        let worker_id: &str = row.get("worker_id");
        let submission: TaskSubmissionV1 =
            serde_json::from_str(row.get("submission_json")).map_err(CoordinatorError::storage)?;
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
        let mut acquired_lease = None;
        if submission.repository.access == RepositoryAccess::Mutating {
            let repository_key = submission.repository.root.to_string_lossy().into_owned();
            let held: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL",
            )
            .bind(&repository_key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if held != 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "worktree has an unresolved Hold",
                ));
            }
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT task_id FROM worktree_leases WHERE repository_key = ? AND released_at IS NULL",
            )
            .bind(&repository_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if existing
                .as_deref()
                .is_some_and(|id| id != task_id.to_string())
            {
                return Err(CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "worktree already has an active mutating lease",
                ));
            }
            if existing.is_none() {
                acquired_lease = Some(self.try_lock_worktree(&repository_key)?);
                let lease_statement = "INSERT INTO worktree_leases (repository_key, task_id, acquired_at) VALUES (?, ?, ?) ON CONFLICT(repository_key) DO UPDATE SET task_id = excluded.task_id, acquired_at = excluded.acquired_at, released_at = NULL";
                sqlx::query(lease_statement)
                    .bind(&repository_key)
                    .bind(task_id.to_string())
                    .bind(timestamp())
                    .execute(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            }
        } else {
            let repository_key = submission.repository.root.to_string_lossy().into_owned();
            let held: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL",
            )
            .bind(&repository_key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if held != 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "read-only Task cannot dispatch while the worktree has an unresolved Hold",
                ));
            }
            let leased_parent = sqlx::query("SELECT l.task_id, t.state FROM worktree_leases l JOIN tasks t ON t.id = l.task_id WHERE l.repository_key = ? AND l.released_at IS NULL")
                .bind(&repository_key)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            if let Some(parent) = leased_parent {
                let parent_id = parse_uuid_id::<TaskId>(parent.get("task_id"))?;
                if submission.related_task_id != Some(parent_id)
                    || parent.get::<&str, _>("state") != TaskState::Reviewing.as_str()
                {
                    return Err(CoordinatorError::new(
                        ErrorCategory::RepositoryBlocked,
                        "read-only Task must explicitly relate to the stably reviewing lease owner",
                    ));
                }
            }
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
            Some(&session_id),
            "dispatching",
            false,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        if let Some(lease) = acquired_lease {
            self.lease_files.lock().await.insert(task_id, lease);
        }
        Ok(CommandOutcome::TaskDispatching {
            task_id,
            message_id,
        })
    }

    async fn cleanup_failed_dispatch_baseline(
        &self,
        task_id: TaskId,
    ) -> Result<(), CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let queued: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE id = ? AND state = 'queued'")
                .bind(task_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        if queued == 1 {
            sqlx::query("DELETE FROM repository_observations WHERE task_id = ? AND checkpoint = 'before_dispatch'")
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            sqlx::query("DELETE FROM repository_snapshots WHERE task_id = ? AND checkpoint = 'before_dispatch'")
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)
    }

    async fn preflight_dispatch(&self, task_id: TaskId) -> Result<(), CoordinatorError> {
        let row = sqlx::query("SELECT worker_id, state, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(row.get("state"), TaskState::Queued)?;
        let worker_id: &str = row.get("worker_id");
        let online: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL AND presence = 'online'")
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        if online != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::TargetOffline,
                "assigned Worker is not ready",
            ));
        }
        let busy: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND id <> ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')")
            .bind(worker_id)
            .bind(task_id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        if busy != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "Worker already owns an active Task",
            ));
        }
        let submission: TaskSubmissionV1 =
            serde_json::from_str(row.get("submission_json")).map_err(CoordinatorError::storage)?;
        let repository_key = submission.repository.root.to_string_lossy().into_owned();
        let held: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL",
        )
        .bind(&repository_key)
        .fetch_one(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        if held != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::RepositoryBlocked,
                "worktree has an unresolved Hold",
            ));
        }
        let leased_parent = sqlx::query("SELECT l.task_id, t.state FROM worktree_leases l JOIN tasks t ON t.id = l.task_id WHERE l.repository_key = ? AND l.released_at IS NULL")
            .bind(&repository_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        match (submission.repository.access, leased_parent) {
            (RepositoryAccess::Mutating, Some(parent))
                if parent.get::<&str, _>("task_id") != task_id.to_string() =>
            {
                Err(CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "worktree already has an active mutating lease",
                ))
            }
            (RepositoryAccess::ReadOnly, Some(parent)) => {
                let parent_id = parse_uuid_id::<TaskId>(parent.get("task_id"))?;
                if submission.related_task_id == Some(parent_id)
                    && parent.get::<&str, _>("state") == TaskState::Reviewing.as_str()
                {
                    Ok(())
                } else {
                    Err(CoordinatorError::new(
                        ErrorCategory::RepositoryBlocked,
                        "read-only Task must explicitly relate to the stably reviewing lease owner",
                    ))
                }
            }
            _ => Ok(()),
        }
    }

    async fn claim_next_task(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
        )
        .bind(actor.id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        if active != 0 {
            return Ok(CommandOutcome::NoTaskAvailable);
        }
        let task_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM tasks WHERE worker_id = ? AND state = 'queued' ORDER BY created_sequence LIMIT 1",
        )
        .bind(actor.id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        let Some(task_id) = task_id else {
            return Ok(CommandOutcome::NoTaskAvailable);
        };
        match self.dispatch_task(parse_uuid_id(&task_id)?).await {
            Err(error)
                if matches!(
                    error.category,
                    ErrorCategory::RepositoryBlocked | ErrorCategory::TargetOffline
                ) =>
            {
                Ok(CommandOutcome::NoTaskAvailable)
            }
            outcome => outcome,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "route authorization and persistence are one transaction"
    )]
    async fn send_message(
        &self,
        actor: &AuthenticatedActor,
        submission: MessageSubmissionV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        submission.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let payload_digest = canonical_digest(&submission)?;
        if let Some(outcome) = find_idempotent_outcome(
            &mut transaction,
            actor,
            "send_message",
            submission.request_key.as_deref(),
            &payload_digest,
        )
        .await?
        {
            return Ok(outcome);
        }
        let now = timestamp();
        require_attachments(&mut transaction, &submission.attachments).await?;
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
        } else if !matches!(
            (actor.tier, recipient_tier.as_str()),
            (HarnessTier::Supervisor, "worker") | (HarnessTier::Worker, "supervisor")
        ) {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "taskless Notifications must follow the Supervisor-Worker topology",
            ));
        }
        let recipient_session: Option<String> = sqlx::query_scalar(
            "SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1",
        )
        .bind(submission.to.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
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
            recipient_session.as_deref(),
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
        let outcome = CommandOutcome::MessageCreated { message_id };
        store_idempotent_outcome(
            &mut transaction,
            actor,
            "send_message",
            submission.request_key.as_deref(),
            &payload_digest,
            &outcome,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(outcome)
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
        let changed = sqlx::query("UPDATE delivery_attempts SET state = 'accepted', native_correlation = ?, provider_bytes_may_have_been_written = 1, updated_at = ? WHERE id = (SELECT id FROM delivery_attempts WHERE message_id = ? ORDER BY attempt_number DESC LIMIT 1) AND state IN ('pending','dispatching')")
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
                    .await?;
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
                    .await?;
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
                    .await?;
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
        let mut attachment_ids = manifest.attachments.clone();
        attachment_ids.extend(manifest.verification.iter().map(|entry| entry.evidence));
        require_attachments(&mut transaction, &attachment_ids).await?;
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

    #[expect(
        clippy::too_many_lines,
        reason = "terminal Result, repository safety, notification, and state transition are atomic"
    )]
    async fn record_turn_completed(
        &self,
        actor: &AuthenticatedActor,
        task_id: TaskId,
        native_turn_id: String,
        succeeded: bool,
    ) -> Result<CommandOutcome, CoordinatorError> {
        self.require_assigned_worker(actor, task_id).await?;
        let checkpoint = if succeeded {
            ObservationCheckpoint::Result
        } else {
            ObservationCheckpoint::Failure
        };
        if let Err(error) = self
            .capture_repository_checkpoint(task_id, checkpoint)
            .await
        {
            self.fail_task_for_missing_evidence(
                task_id,
                &[TaskState::Working, TaskState::Waiting],
                &error.to_string(),
            )
            .await?;
            return Ok(CommandOutcome::TurnCompleted {
                task_id,
                state: TaskState::Failed,
            });
        }
        let out_of_scope = succeeded
            && self
                .checkpoint_has_out_of_scope(task_id, ObservationCheckpoint::Result)
                .await?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query(
            "SELECT worker_id, state, result_revision, submission_json FROM tasks WHERE id = ?",
        )
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
        let current = TaskState::from_str(task.get("state"))?;
        if !matches!(current, TaskState::Working | TaskState::Waiting) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task must be working or waiting",
            ));
        }
        let revision: i64 = task.get("result_revision");
        let result_manifest: Option<String> = sqlx::query_scalar("SELECT manifest_json FROM results WHERE task_id = ? AND revision = ? AND native_turn_id = ?")
            .bind(task_id.to_string())
            .bind(revision)
            .bind(&native_turn_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let next = if succeeded && result_manifest.is_some() {
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
        transition_exact(&mut transaction, task_id, current, next, false, &now).await?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        if next == TaskState::Failed || out_of_scope {
            let hold_task_id = if submission.repository.access == RepositoryAccess::Mutating {
                Some(task_id)
            } else if out_of_scope {
                submission.related_task_id
            } else {
                None
            };
            if let Some(hold_task_id) = hold_task_id {
                sqlx::query("INSERT OR IGNORE INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, ?, ?)")
                .bind(WorktreeHoldId::new().to_string())
                .bind(submission.repository.root.to_string_lossy().as_ref())
                    .bind(hold_task_id.to_string())
                .bind(if out_of_scope { "out_of_scope_mutation" } else { "native_turn_failed" })
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            }
        }
        if next == TaskState::Reviewing {
            let supervisor_id: String =
                sqlx::query_scalar("SELECT id FROM harnesses WHERE tier = 'supervisor' LIMIT 1")
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            let supervisor_session: Option<String> = sqlx::query_scalar("SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1")
                .bind(&supervisor_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            let message_id = MessageId::new();
            let sequence = next_sequence(&mut transaction, "result_message").await?;
            sqlx::query("INSERT INTO messages (id, task_id, sender_id, recipient_id, kind, body_json, delivery_intent, created_sequence, created_at) VALUES (?, ?, ?, ?, 'result', ?, 'follow_up', ?, ?)")
                .bind(message_id.to_string())
                .bind(task_id.to_string())
                .bind(actor.id.as_str())
                .bind(&supervisor_id)
                .bind(result_manifest.expect("reviewing requires a Result"))
                .bind(sequence)
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            create_delivery_attempt(
                &mut transaction,
                message_id,
                supervisor_session.as_deref(),
                "pending",
                false,
                &now,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TurnCompleted {
            task_id,
            state: next,
        })
    }

    async fn capture_repository_observation(
        &self,
        actor: &AuthenticatedActor,
        task_id: TaskId,
        checkpoint: ObservationCheckpoint,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let worker_id: Option<String> =
            sqlx::query_scalar("SELECT worker_id FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
        let worker_id = worker_id
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        if actor.tier != HarnessTier::Supervisor && actor.id.as_str() != worker_id {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the assigned Worker or Supervisor may record Task repository evidence",
            ));
        }
        let checkpoint_allowed = match actor.tier {
            HarnessTier::Supervisor => matches!(
                checkpoint,
                ObservationCheckpoint::Approval | ObservationCheckpoint::HoldClear
            ),
            HarnessTier::Worker => matches!(
                checkpoint,
                ObservationCheckpoint::Result
                    | ObservationCheckpoint::Cancel
                    | ObservationCheckpoint::Failure
            ),
        };
        if !checkpoint_allowed {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "repository checkpoint is not authorized for this Harness tier",
            ));
        }
        match self
            .capture_repository_checkpoint(task_id, checkpoint)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(error)
                if matches!(
                    checkpoint,
                    ObservationCheckpoint::Approval | ObservationCheckpoint::HoldClear
                ) =>
            {
                self.create_repository_hold(task_id, "repository_checkpoint_failed")
                    .await?;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "trusted Git capture, immutable diff admission, and indexing form one boundary"
    )]
    async fn capture_repository_checkpoint(
        &self,
        task_id: TaskId,
        checkpoint: ObservationCheckpoint,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let submission_json: Option<String> =
            sqlx::query_scalar("SELECT submission_json FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(&submission_json.ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
            })?)
            .map_err(CoordinatorError::storage)?;
        let repository = GitRepository::open(&submission.repository.root).map_err(|error| {
            CoordinatorError::new(ErrorCategory::RepositoryBlocked, error.to_string())
        })?;
        let snapshot = repository.observe().map_err(|error| {
            CoordinatorError::new(ErrorCategory::RepositoryBlocked, error.to_string())
        })?;
        let baseline_json: Option<String> = sqlx::query_scalar(
            "SELECT snapshot_json FROM repository_snapshots WHERE task_id = ? AND checkpoint = 'before_dispatch'",
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        let (changed_paths, scope_classifications) = if checkpoint
            == ObservationCheckpoint::BeforeDispatch
        {
            (Vec::new(), Vec::new())
        } else {
            let baseline: RepositorySnapshot =
                serde_json::from_str(&baseline_json.ok_or_else(|| {
                    CoordinatorError::new(
                        ErrorCategory::InvalidState,
                        "Task has no trusted before-dispatch repository baseline",
                    )
                })?)
                .map_err(CoordinatorError::storage)?;
            if snapshot.identity != baseline.identity {
                return Err(CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "repository identity changed after Task dispatch",
                ));
            }
            let comparison = snapshot.compare_to(&baseline, &submission.repository.write_scopes);
            (comparison.changed_paths, comparison.scope_classifications)
        };
        let staged_diff = self
            .admit_observation_diff(&snapshot.staged_diff, "staged.diff")
            .await?;
        let unstaged_diff = self
            .admit_observation_diff(&snapshot.unstaged_diff, "unstaged.diff")
            .await?;
        let version = git_version()?;
        let command_evidence = [
            "git status --porcelain=v2 -z --branch --untracked-files=all --ignored=matching --no-renames",
            "git diff --binary --cached --no-ext-diff --no-textconv --",
            "git diff --binary --no-ext-diff --no-textconv --",
            "git rev-parse",
        ]
        .into_iter()
        .map(|command| CommandEvidenceV1 {
            command: command.to_owned(),
            version: version.clone(),
            exit_code: 0,
            diagnostics: String::new(),
        })
        .collect();
        let mut observation = RepositoryObservationV1 {
            schema_version: SCHEMA_VERSION,
            id: RepositoryObservationId::new(),
            task_id,
            checkpoint,
            worktree_root: snapshot.identity.worktree_root.clone(),
            git_common_dir: snapshot.identity.git_common_dir.clone(),
            head: snapshot.head.clone(),
            branch: snapshot.branch.clone(),
            index_digest: snapshot.index_digest.clone(),
            staged_diff,
            unstaged_diff,
            untracked: snapshot.untracked.clone(),
            ignored_paths: snapshot.ignored_paths.clone(),
            status_entries: snapshot.status_entries.clone(),
            changed_paths,
            scope_classifications,
            command_evidence,
            captured_at: Utc::now(),
            digest: "0".repeat(64),
        };
        observation.digest = canonical_digest(&observation)?;
        observation.validate().map_err(|error| {
            CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
        })?;
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO repository_observations (id, task_id, checkpoint, digest, observation_json, created_at) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(observation.id.to_string())
            .bind(task_id.to_string())
            .bind(observation_checkpoint_name(observation.checkpoint))
            .bind(&observation.digest)
            .bind(serde_json::to_string(&observation).map_err(CoordinatorError::storage)?)
            .bind(timestamp())
            .execute(&mut *transaction)
            .await
            .map_err(|error| CoordinatorError::new(ErrorCategory::Conflict, error.to_string()))?;
        let snapshot_sql = if checkpoint == ObservationCheckpoint::BeforeDispatch {
            "INSERT OR IGNORE INTO repository_snapshots (task_id, checkpoint, snapshot_json, created_at) VALUES (?, ?, ?, ?)"
        } else {
            "INSERT INTO repository_snapshots (task_id, checkpoint, snapshot_json, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(task_id, checkpoint) DO UPDATE SET snapshot_json = excluded.snapshot_json, created_at = excluded.created_at"
        };
        sqlx::query(snapshot_sql)
            .bind(task_id.to_string())
            .bind(observation_checkpoint_name(checkpoint))
            .bind(serde_json::to_string(&snapshot).map_err(CoordinatorError::storage)?)
            .bind(timestamp())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::ObservationRecorded {
            task_id,
            digest: observation.digest,
        })
    }

    async fn admit_observation_diff(
        &self,
        bytes: &[u8],
        original_name: &str,
    ) -> Result<Option<crate::contract::AttachmentId>, CoordinatorError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let temporary_dir = self.state_dir.join("tmp");
        tokio::fs::create_dir_all(&temporary_dir)
            .await
            .map_err(CoordinatorError::storage)?;
        let temporary = temporary_dir.join(format!("repository-diff-{}.tmp", Uuid::now_v7()));
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(CoordinatorError::storage)?;
        let outcome = self
            .admit_attachment(
                temporary.clone(),
                "application/octet-stream".to_owned(),
                original_name.to_owned(),
            )
            .await;
        let _ = tokio::fs::remove_file(temporary).await;
        let CommandOutcome::AttachmentAdmitted { attachment } = outcome? else {
            unreachable!("attachment admission has one success outcome")
        };
        Ok(Some(attachment.id))
    }

    async fn verify_repository_checkpoint_current(
        &self,
        task_id: TaskId,
        checkpoint: ObservationCheckpoint,
    ) -> Result<(), CoordinatorError> {
        let row = sqlx::query("SELECT t.submission_json, s.snapshot_json FROM tasks t JOIN repository_snapshots s ON s.task_id = t.id WHERE t.id = ? AND s.checkpoint = ?")
            .bind(task_id.to_string())
            .bind(observation_checkpoint_name(checkpoint))
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| {
                CoordinatorError::new(
                    ErrorCategory::Conflict,
                    "required trusted repository checkpoint is missing",
                )
            })?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(row.get("submission_json")).map_err(CoordinatorError::storage)?;
        let expected: RepositorySnapshot =
            serde_json::from_str(row.get("snapshot_json")).map_err(CoordinatorError::storage)?;
        let current = GitRepository::open(&submission.repository.root)
            .and_then(|repository| repository.observe())
            .map_err(|error| {
                CoordinatorError::new(ErrorCategory::RepositoryBlocked, error.to_string())
            })?;
        if current == expected {
            Ok(())
        } else {
            Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "repository changed after the trusted checkpoint was captured",
            ))
        }
    }

    async fn checkpoint_has_out_of_scope(
        &self,
        task_id: TaskId,
        checkpoint: ObservationCheckpoint,
    ) -> Result<bool, CoordinatorError> {
        let observation_json: Option<String> = sqlx::query_scalar("SELECT observation_json FROM repository_observations WHERE task_id = ? AND checkpoint = ? ORDER BY created_at DESC LIMIT 1")
            .bind(task_id.to_string())
            .bind(observation_checkpoint_name(checkpoint))
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        let observation: RepositoryObservationV1 =
            serde_json::from_str(&observation_json.ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::Conflict, "repository checkpoint is missing")
            })?)
            .map_err(CoordinatorError::storage)?;
        Ok(observation
            .scope_classifications
            .iter()
            .any(|entry| entry.classification == ScopeClassification::OutOfScope))
    }

    async fn create_repository_hold(
        &self,
        task_id: TaskId,
        reason: &str,
    ) -> Result<(), CoordinatorError> {
        let submission_json: Option<String> =
            sqlx::query_scalar("SELECT submission_json FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(&submission_json.ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
            })?)
            .map_err(CoordinatorError::storage)?;
        let hold_task_id = if submission.repository.access == RepositoryAccess::Mutating {
            task_id
        } else {
            submission.related_task_id.ok_or_else(|| {
                CoordinatorError::new(
                    ErrorCategory::RepositoryBlocked,
                    "read-only repository drift has no mutating parent to hold",
                )
            })?
        };
        sqlx::query("INSERT OR IGNORE INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(WorktreeHoldId::new().to_string())
            .bind(submission.repository.root.to_string_lossy().as_ref())
            .bind(hold_task_id.to_string())
            .bind(reason)
            .bind(timestamp())
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(())
    }

    async fn fail_task_for_missing_evidence(
        &self,
        task_id: TaskId,
        expected_states: &[TaskState],
        diagnostic: &str,
    ) -> Result<(), CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let row = sqlx::query("SELECT state, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        let current = TaskState::from_str(row.get("state"))?;
        if !expected_states.contains(&current) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task state changed before evidence failure could be settled",
            ));
        }
        let now = timestamp();
        let evidence = serde_json::to_string(&serde_json::json!({
            "repository_observation_failure": diagnostic,
        }))
        .map_err(CoordinatorError::storage)?;
        let changed = sqlx::query(
            "UPDATE tasks SET state = 'failed', updated_at = ? WHERE id = ? AND state = ?",
        )
        .bind(&now)
        .bind(task_id.to_string())
        .bind(current.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?
        .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task state changed before evidence failure could be settled",
            ));
        }
        record_transition(
            &mut transaction,
            task_id,
            current,
            TaskState::Failed,
            &evidence,
            &now,
        )
        .await?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(row.get("submission_json")).map_err(CoordinatorError::storage)?;
        if submission.repository.access == RepositoryAccess::Mutating {
            sqlx::query("INSERT OR IGNORE INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, 'repository_observation_failed', ?)")
                .bind(WorktreeHoldId::new().to_string())
                .bind(submission.repository.root.to_string_lossy().as_ref())
                .bind(task_id.to_string())
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        } else {
            sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        if submission.repository.access == RepositoryAccess::ReadOnly {
            self.release_lease_file(task_id).await;
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Approval preconditions and release are one transaction"
    )]
    async fn approve_task(
        &self,
        task_id: TaskId,
        result_revision: u32,
        observation_digest: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        validate_digest(&observation_digest)?;
        self.verify_repository_checkpoint_current(task_id, ObservationCheckpoint::Approval)
            .await?;
        if self
            .checkpoint_has_out_of_scope(task_id, ObservationCheckpoint::Approval)
            .await?
        {
            self.create_repository_hold(task_id, "out_of_scope_approval_drift")
                .await?;
            return Err(CoordinatorError::new(
                ErrorCategory::RepositoryBlocked,
                "Approval Observation contains out-of-scope repository drift",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task =
            sqlx::query("SELECT state, result_revision, submission_json FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .ok_or_else(|| {
                    CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
                })?;
        require_state(task.get("state"), TaskState::Reviewing)?;
        let current_revision: i64 = task.get("result_revision");
        if current_revision != i64::from(result_revision) {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "Result revision is stale",
            ));
        }
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        let repository_key = submission.repository.root.to_string_lossy().into_owned();
        let held: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL",
        )
        .bind(&repository_key)
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if held != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::RepositoryBlocked,
                "worktree has an unresolved Hold",
            ));
        }
        let observation_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM repository_observations WHERE task_id = ? AND checkpoint = 'approval' AND digest = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id.to_string())
        .bind(&observation_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let observation_id = observation_id.ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::Conflict,
                "current Repository Observation digest does not match",
            )
        })?;
        let active_reviews: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE related_task_id = ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
        )
        .bind(task_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if active_reviews != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "related review Task is still active",
            ));
        }
        let now = timestamp();
        let changed = sqlx::query("UPDATE tasks SET state = 'approved', approved_result_revision = ?, approval_observation_id = ?, updated_at = ? WHERE id = ? AND state = 'reviewing'")
            .bind(current_revision)
            .bind(observation_id)
            .bind(&now)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task is no longer reviewing",
            ));
        }
        record_transition(
            &mut transaction,
            task_id,
            TaskState::Reviewing,
            TaskState::Approved,
            "{}",
            &now,
        )
        .await?;
        sqlx::query(
            "UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL",
        )
        .bind(&now)
        .bind(task_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        self.release_lease_file(task_id).await;
        Ok(CommandOutcome::TaskApproved { task_id })
    }

    async fn cancel_task(&self, task_id: TaskId) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let state_text: Option<String> = sqlx::query_scalar("SELECT state FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let current = TaskState::from_str(&state_text.ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
        })?)?;
        let next = if current == TaskState::Queued {
            TaskState::Cancelled
        } else if matches!(
            current,
            TaskState::Dispatching
                | TaskState::Working
                | TaskState::Waiting
                | TaskState::Reviewing
                | TaskState::DeliveryUnknown
        ) {
            TaskState::Cancelling
        } else {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task cannot be cancelled from its current state",
            ));
        };
        let now = timestamp();
        transition_exact(&mut transaction, task_id, current, next, false, &now).await?;
        if next == TaskState::Cancelled {
            sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        if next == TaskState::Cancelled {
            self.release_lease_file(task_id).await;
        }
        Ok(CommandOutcome::TaskCancellationUpdated {
            task_id,
            state: next,
        })
    }

    async fn record_cancellation_completed(
        &self,
        actor: &AuthenticatedActor,
        task_id: TaskId,
        succeeded: bool,
    ) -> Result<CommandOutcome, CoordinatorError> {
        self.require_assigned_worker(actor, task_id).await?;
        if let Err(error) = self
            .capture_repository_checkpoint(
                task_id,
                if succeeded {
                    ObservationCheckpoint::Cancel
                } else {
                    ObservationCheckpoint::Failure
                },
            )
            .await
        {
            self.fail_task_for_missing_evidence(
                task_id,
                &[TaskState::Cancelling],
                &error.to_string(),
            )
            .await?;
            return Ok(CommandOutcome::TaskCancellationUpdated {
                task_id,
                state: TaskState::Failed,
            });
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT worker_id, state, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        if actor.id.as_str() != task.get::<&str, _>("worker_id") {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the assigned Worker Host may settle cancellation",
            ));
        }
        require_state(task.get("state"), TaskState::Cancelling)?;
        let next = if succeeded {
            TaskState::Cancelled
        } else {
            TaskState::Failed
        };
        let now = timestamp();
        transition_exact(
            &mut transaction,
            task_id,
            TaskState::Cancelling,
            next,
            false,
            &now,
        )
        .await?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        if submission.repository.access == RepositoryAccess::Mutating {
            let repository_key = submission.repository.root.to_string_lossy().into_owned();
            sqlx::query("INSERT INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, ?, ?)")
                .bind(WorktreeHoldId::new().to_string())
                .bind(repository_key)
                .bind(task_id.to_string())
                .bind(if succeeded { "cancelled_after_dispatch" } else { "cancellation_failed" })
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        } else {
            sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TaskCancellationUpdated {
            task_id,
            state: next,
        })
    }

    async fn mark_delivery_unknown(
        &self,
        actor: &AuthenticatedActor,
        message_id: MessageId,
        diagnostic: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if diagnostic.is_empty() || diagnostic.len() > 4096 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "delivery diagnostic must contain 1 to 4096 bytes",
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
                "only the destination Host may report ambiguous acceptance",
            ));
        }
        let task_id = parse_uuid_id::<TaskId>(
            message.get::<Option<&str>, _>("task_id").ok_or_else(|| {
                CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    "network Notification cannot make a Task delivery unknown",
                )
            })?,
        )?;
        let now = timestamp();
        let attempt = sqlx::query("SELECT id, target_session_id FROM delivery_attempts WHERE message_id = ? ORDER BY attempt_number DESC LIMIT 1")
            .bind(message_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        sqlx::query("UPDATE delivery_attempts SET state = 'unknown', provider_bytes_may_have_been_written = 1, evidence_json = ?, updated_at = ? WHERE id = ?")
            .bind(serde_json::json!({"diagnostic": diagnostic}).to_string())
            .bind(&now)
            .bind(attempt.get::<&str, _>("id"))
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if let Some(session_id) = attempt.get::<Option<&str>, _>("target_session_id") {
            sqlx::query("UPDATE harness_sessions SET presence = 'failed', activity = 'failed', ended_at = ? WHERE id = ? AND ended_at IS NULL")
                .bind(&now)
                .bind(session_id)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        let current_text: String = sqlx::query_scalar("SELECT state FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let current = TaskState::from_str(&current_text)?;
        if current != TaskState::Dispatching {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "only initial Task dispatch may enter delivery_unknown",
            ));
        }
        transition_exact(
            &mut transaction,
            task_id,
            current,
            TaskState::DeliveryUnknown,
            false,
            &now,
        )
        .await?;
        let submission_json: String =
            sqlx::query_scalar("SELECT submission_json FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(&submission_json).map_err(CoordinatorError::storage)?;
        if submission.repository.access == RepositoryAccess::Mutating {
            sqlx::query("INSERT INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, 'delivery_unknown', ?)")
                .bind(WorktreeHoldId::new().to_string())
                .bind(submission.repository.root.to_string_lossy().as_ref())
                .bind(task_id.to_string())
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::DeliveryUnknownUpdated {
            task_id,
            state: TaskState::DeliveryUnknown,
        })
    }

    async fn resolve_delivery_unknown(
        &self,
        task_id: TaskId,
        resolution: DeliveryUnknownResolution,
        observation_digest: String,
        audit_note: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        validate_digest(&observation_digest)?;
        self.verify_repository_checkpoint_current(task_id, ObservationCheckpoint::HoldClear)
            .await?;
        if audit_note.trim().is_empty() || audit_note.len() > 4096 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "audit note must contain 1 to 4096 bytes",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT state, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(task.get("state"), TaskState::DeliveryUnknown)?;
        let observation_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM repository_observations WHERE task_id = ? AND checkpoint = 'hold_clear' AND digest = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id.to_string())
        .bind(&observation_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let observation_id = observation_id.ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::Conflict,
                "reconciliation Observation digest does not match",
            )
        })?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        let next = match resolution {
            DeliveryUnknownResolution::Requeue => TaskState::Queued,
            DeliveryUnknownResolution::Cancel => TaskState::Cancelled,
        };
        let now = timestamp();
        transition_exact(
            &mut transaction,
            task_id,
            TaskState::DeliveryUnknown,
            next,
            false,
            &now,
        )
        .await?;
        sqlx::query("UPDATE worktree_holds SET cleared_at = ?, observation_id = ?, audit_note = ? WHERE task_id = ? AND cleared_at IS NULL")
            .bind(&now)
            .bind(observation_id)
            .bind(audit_note)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if next == TaskState::Cancelled
            || submission.repository.access == RepositoryAccess::ReadOnly
        {
            sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        if next == TaskState::Cancelled {
            self.release_lease_file(task_id).await;
        }
        Ok(CommandOutcome::DeliveryUnknownUpdated {
            task_id,
            state: next,
        })
    }

    async fn clear_worktree_hold(
        &self,
        task_id: TaskId,
        observation_digest: String,
        audit_note: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        validate_digest(&observation_digest)?;
        self.verify_repository_checkpoint_current(task_id, ObservationCheckpoint::HoldClear)
            .await?;
        if audit_note.trim().is_empty() || audit_note.len() > 4096 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "audit note must contain 1 to 4096 bytes",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let state = TaskState::from_str(&state.ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
        })?)?;
        if !matches!(
            state,
            TaskState::Reviewing | TaskState::Cancelled | TaskState::Failed
        ) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Hold clearance requires a reviewing, cancelled, or failed Task",
            ));
        }
        let observation_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM repository_observations WHERE task_id = ? AND checkpoint = 'hold_clear' AND digest = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id.to_string())
        .bind(&observation_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let observation_id = observation_id.ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::Conflict,
                "Hold reconciliation Observation digest does not match",
            )
        })?;
        let now = timestamp();
        let changed = sqlx::query("UPDATE worktree_holds SET cleared_at = ?, observation_id = ?, audit_note = ? WHERE task_id = ? AND cleared_at IS NULL")
            .bind(&now)
            .bind(observation_id)
            .bind(audit_note)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::NotFound,
                "Task has no active Worktree Hold",
            ));
        }
        if matches!(state, TaskState::Cancelled | TaskState::Failed) {
            sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                .bind(&now)
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        if matches!(state, TaskState::Cancelled | TaskState::Failed) {
            self.release_lease_file(task_id).await;
        }
        Ok(CommandOutcome::HoldCleared { task_id })
    }

    async fn stop_worker(&self, worker_id: HarnessId) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let session_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM harness_sessions WHERE harness_id = ? AND harness_tier = 'worker' AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1",
        )
        .bind(worker_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let session_id = session_id.ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::TargetOffline, "Worker has no live Session")
        })?;
        if let Some(row) = sqlx::query("SELECT id, state FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','reviewing') ORDER BY created_sequence LIMIT 1")
            .bind(worker_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
        {
            let task_id = parse_uuid_id(row.get("id"))?;
            let state = TaskState::from_str(row.get("state"))?;
            transition_exact(&mut transaction, task_id, state, TaskState::Cancelling, false, &timestamp()).await?;
        }
        sqlx::query(
            "UPDATE harness_sessions SET activity = 'stopping', last_seen_at = ? WHERE id = ?",
        )
        .bind(timestamp())
        .bind(session_id)
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::WorkerStopping { worker_id })
    }

    async fn record_host_stopped(
        &self,
        actor: &AuthenticatedActor,
        clean: bool,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only a Worker Host may report its shutdown",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        if clean {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
            )
            .bind(actor.id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if active != 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidState,
                    "clean Host stop requires active Task cancellation to settle first",
                ));
            }
        }
        let now = timestamp();
        sqlx::query("UPDATE harness_sessions SET presence = ?, activity = ?, ended_at = ?, last_seen_at = ? WHERE id = ? AND ended_at IS NULL")
            .bind(if clean { "stopped" } else { "failed" })
            .bind(if clean { "idle" } else { "failed" })
            .bind(&now)
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::HostStopped { clean })
    }

    async fn record_host_ready(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only a Worker Host may report readiness",
            ));
        }
        let changed = sqlx::query("UPDATE harness_sessions SET presence = 'online', activity = 'idle', last_seen_at = ? WHERE id = ? AND ended_at IS NULL AND presence = 'starting'")
            .bind(timestamp())
            .bind(actor.session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Worker Host readiness requires a starting Session",
            ));
        }
        self.issued_capabilities
            .lock()
            .await
            .remove(&actor.session_id);
        Ok(CommandOutcome::HostReady)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Host failure settles Session, Task, repository evidence, lease, and Hold together"
    )]
    async fn record_host_failed(
        &self,
        actor: &AuthenticatedActor,
        diagnostic: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only a Worker Host may report its failure",
            ));
        }
        if diagnostic.trim().is_empty() || diagnostic.len() > 16_384 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Host failure diagnostic must contain 1 to 16,384 bytes",
            ));
        }
        let active = sqlx::query("SELECT id, state, submission_json FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','cancelling','delivery_unknown') ORDER BY created_sequence LIMIT 1")
            .bind(actor.id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        let active = active
            .map(|row| {
                Ok::<_, CoordinatorError>((
                    parse_uuid_id::<TaskId>(row.get("id"))?,
                    TaskState::from_str(row.get("state"))?,
                    serde_json::from_str::<TaskSubmissionV1>(row.get("submission_json"))
                        .map_err(CoordinatorError::storage)?,
                ))
            })
            .transpose()?;
        if let Some((task_id, _, _)) = &active
            && let Err(error) = self
                .capture_repository_checkpoint(*task_id, ObservationCheckpoint::Failure)
                .await
        {
            let expected = active
                .as_ref()
                .map(|(_, state, _)| *state)
                .expect("active Task exists");
            self.fail_task_for_missing_evidence(*task_id, &[expected], &error.to_string())
                .await?;
            return self.record_host_stopped(actor, false).await;
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        if let Some((task_id, state, submission)) = active {
            if state != TaskState::DeliveryUnknown {
                let evidence = serde_json::to_string(&serde_json::json!({
                    "host_failure": diagnostic,
                }))
                .map_err(CoordinatorError::storage)?;
                let changed = sqlx::query(
                    "UPDATE tasks SET state = 'failed', updated_at = ? WHERE id = ? AND state = ?",
                )
                .bind(&now)
                .bind(task_id.to_string())
                .bind(state.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .rows_affected();
                if changed == 1 {
                    record_transition(
                        &mut transaction,
                        task_id,
                        state,
                        TaskState::Failed,
                        &evidence,
                        &now,
                    )
                    .await?;
                }
            }
            if submission.repository.access == RepositoryAccess::Mutating {
                sqlx::query("INSERT OR IGNORE INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, 'worker_host_failed', ?)")
                    .bind(WorktreeHoldId::new().to_string())
                    .bind(submission.repository.root.to_string_lossy().as_ref())
                    .bind(task_id.to_string())
                    .bind(&now)
                    .execute(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            } else {
                sqlx::query("UPDATE worktree_leases SET released_at = ? WHERE task_id = ? AND released_at IS NULL")
                    .bind(&now)
                    .bind(task_id.to_string())
                    .execute(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            }
        }
        sqlx::query("UPDATE harness_sessions SET presence = 'failed', activity = 'failed', ended_at = ?, last_seen_at = ? WHERE id = ? AND ended_at IS NULL")
            .bind(&now)
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::HostStopped { clean: false })
    }

    async fn record_host_event(
        &self,
        actor: &AuthenticatedActor,
        sequence: u64,
        event: Value,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker || sequence == 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Worker Host event sequence must start at one",
            ));
        }
        let sequence = i64::try_from(sequence).map_err(CoordinatorError::storage)?;
        let event_json = serde_json::to_string(&event).map_err(CoordinatorError::storage)?;
        if event_json.len() > 65_536 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Host event exceeds 65,536 bytes",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let current: i64 = sqlx::query_scalar(
            "SELECT event_sequence FROM harness_sessions WHERE id = ? AND ended_at IS NULL",
        )
        .bind(actor.session_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if sequence <= current {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT event_json FROM host_events WHERE session_id = ? AND sequence = ?",
            )
            .bind(actor.session_id.to_string())
            .bind(sequence)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            if existing.as_deref() != Some(&event_json) {
                return Err(CoordinatorError::new(
                    ErrorCategory::Conflict,
                    "replayed Host event differs from durable event",
                ));
            }
        } else if sequence == current + 1 {
            sqlx::query("INSERT INTO host_events (session_id, sequence, event_json, received_at) VALUES (?, ?, ?, ?)")
                .bind(actor.session_id.to_string())
                .bind(sequence)
                .bind(event_json)
                .bind(timestamp())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            sqlx::query(
                "UPDATE harness_sessions SET event_sequence = ?, last_seen_at = ? WHERE id = ?",
            )
            .bind(sequence)
            .bind(timestamp())
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        } else {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "Host event sequence has a gap",
            ));
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::HostEventRecorded {
            sequence: u64::try_from(sequence).map_err(CoordinatorError::storage)?,
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

    #[expect(
        clippy::unused_self,
        reason = "kept on the deep Coordinator authorization boundary"
    )]
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

    async fn require_assigned_worker(
        &self,
        actor: &AuthenticatedActor,
        task_id: TaskId,
    ) -> Result<(), CoordinatorError> {
        let worker_id: Option<String> =
            sqlx::query_scalar("SELECT worker_id FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
        let worker_id = worker_id
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        if actor.tier == HarnessTier::Worker && actor.id.as_str() == worker_id {
            Ok(())
        } else {
            Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the assigned Worker Host may report this Task event",
            ))
        }
    }
}

#[derive(Debug)]
struct AuthenticatedActor {
    id: HarnessId,
    tier: HarnessTier,
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
    session_id: Option<&str>,
    state: &str,
    provider_bytes_may_have_been_written: bool,
    now: &str,
) -> Result<(), CoordinatorError> {
    let attempt_number: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM delivery_attempts WHERE message_id = ?",
    )
    .bind(message_id.to_string())
    .fetch_one(&mut **transaction)
    .await
    .map_err(CoordinatorError::storage)?;
    sqlx::query("INSERT INTO delivery_attempts (id, message_id, attempt_number, target_session_id, state, provider_bytes_may_have_been_written, evidence_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, '{}', ?, ?)")
        .bind(DeliveryAttemptId::new().to_string())
        .bind(message_id.to_string())
        .bind(attempt_number)
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

fn canonical_digest(value: &impl Serialize) -> Result<String, CoordinatorError> {
    let bytes = serde_json::to_vec(value).map_err(CoordinatorError::storage)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn git_version() -> Result<String, CoordinatorError> {
    let output = std::process::Command::new("git")
        .arg("--version")
        .output()
        .map_err(CoordinatorError::storage)?;
    if !output.status.success() {
        return Err(CoordinatorError::new(
            ErrorCategory::RepositoryBlocked,
            "git --version failed while capturing repository evidence",
        ));
    }
    String::from_utf8(output.stdout)
        .map(|version| version.trim().to_owned())
        .map_err(CoordinatorError::storage)
}

fn validate_scope_paths(root: &Path, scopes: &[WriteScopeV1]) -> Result<(), CoordinatorError> {
    for scope in scopes {
        let relative = match scope {
            WriteScopeV1::ExactFile { path } | WriteScopeV1::Subtree { path } => path,
        };
        if relative
            .components()
            .any(|component| component.as_os_str() == ".git")
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "write scope may not target Git administrative data",
            ));
        }
        let mut candidate = root.join(relative);
        if matches!(scope, WriteScopeV1::ExactFile { .. }) && !candidate.exists() {
            candidate.pop();
        }
        while !candidate.exists() && candidate != root {
            candidate.pop();
        }
        let resolved = std::fs::canonicalize(&candidate).map_err(CoordinatorError::storage)?;
        if !resolved.starts_with(root) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "write scope resolves outside the canonical worktree",
            ));
        }
        let mut nested = resolved.as_path();
        while nested != root {
            if nested.join(".git").exists() {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    "write scope crosses a nested repository or submodule boundary",
                ));
            }
            nested = nested.parent().ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::InvalidInput, "write scope is invalid")
            })?;
        }
        if matches!(scope, WriteScopeV1::Subtree { .. }) && resolved.is_dir() {
            validate_scope_subtree(root, &resolved)?;
        }
    }
    Ok(())
}

fn validate_scope_subtree(root: &Path, subtree: &Path) -> Result<(), CoordinatorError> {
    let mut pending = vec![subtree.to_path_buf()];
    let mut visited = 0_usize;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).map_err(CoordinatorError::storage)? {
            let entry = entry.map_err(CoordinatorError::storage)?;
            visited = visited.saturating_add(1);
            if visited > 100_000 {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    "write scope subtree is too large to validate safely",
                ));
            }
            if entry.file_name() == ".git" {
                if directory != root {
                    return Err(CoordinatorError::new(
                        ErrorCategory::InvalidInput,
                        "write scope contains a nested repository or submodule",
                    ));
                }
                continue;
            }
            let file_type = entry.file_type().map_err(CoordinatorError::storage)?;
            if file_type.is_symlink() {
                let target =
                    std::fs::canonicalize(entry.path()).map_err(CoordinatorError::storage)?;
                if !target.starts_with(root) {
                    return Err(CoordinatorError::new(
                        ErrorCategory::InvalidInput,
                        "write scope contains a symlink escaping the canonical worktree",
                    ));
                }
            } else if file_type.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

async fn find_idempotent_outcome(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    actor: &AuthenticatedActor,
    command_kind: &str,
    request_key: Option<&str>,
    payload_digest: &str,
) -> Result<Option<CommandOutcome>, CoordinatorError> {
    let Some(request_key) = request_key else {
        return Ok(None);
    };
    let row = sqlx::query(
        "SELECT payload_digest, outcome_json FROM idempotency WHERE actor_id = ? AND command_kind = ? AND request_key = ?",
    )
    .bind(actor.id.as_str())
    .bind(command_kind)
    .bind(request_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(CoordinatorError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.get::<&str, _>("payload_digest") != payload_digest {
        return Err(CoordinatorError::new(
            ErrorCategory::Conflict,
            "request_key was already used with a different payload",
        ));
    }
    serde_json::from_str(row.get("outcome_json"))
        .map(Some)
        .map_err(CoordinatorError::storage)
}

async fn store_idempotent_outcome(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    actor: &AuthenticatedActor,
    command_kind: &str,
    request_key: Option<&str>,
    payload_digest: &str,
    outcome: &CommandOutcome,
    now: &str,
) -> Result<(), CoordinatorError> {
    let Some(request_key) = request_key else {
        return Ok(());
    };
    let outcome_json = serde_json::to_string(outcome).map_err(CoordinatorError::storage)?;
    sqlx::query("INSERT INTO idempotency (actor_id, command_kind, request_key, payload_digest, outcome_json, created_at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(actor.id.as_str())
        .bind(command_kind)
        .bind(request_key)
        .bind(payload_digest)
        .bind(outcome_json)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(())
}

async fn require_attachments(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attachments: &[crate::contract::AttachmentId],
) -> Result<(), CoordinatorError> {
    for attachment in attachments {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attachments WHERE id = ?")
            .bind(attachment.to_string())
            .fetch_one(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if exists == 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::NotFound,
                format!("Attachment {attachment} does not exist"),
            ));
        }
    }
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

impl UuidIdentity for WorktreeHoldId {
    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl UuidIdentity for HarnessSessionId {
    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

fn parse_uuid_id<T: UuidIdentity>(value: &str) -> Result<T, CoordinatorError> {
    Uuid::parse_str(value)
        .map(T::from_uuid)
        .map_err(CoordinatorError::storage)
}

fn task_view_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TaskView, CoordinatorError> {
    let revision: i64 = row.get("result_revision");
    Ok(TaskView {
        id: parse_uuid_id(row.get("id"))?,
        worker_id: HarnessId::from_str(row.get("worker_id")).map_err(CoordinatorError::storage)?,
        state: TaskState::from_str(row.get("state"))?,
        result_revision: u32::try_from(revision).map_err(CoordinatorError::storage)?,
    })
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

fn observation_checkpoint_name(checkpoint: crate::contract::ObservationCheckpoint) -> &'static str {
    match checkpoint {
        crate::contract::ObservationCheckpoint::BeforeDispatch => "before_dispatch",
        crate::contract::ObservationCheckpoint::Result => "result",
        crate::contract::ObservationCheckpoint::Cancel => "cancel",
        crate::contract::ObservationCheckpoint::Failure => "failure",
        crate::contract::ObservationCheckpoint::Approval => "approval",
        crate::contract::ObservationCheckpoint::HoldClear => "hold_clear",
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
