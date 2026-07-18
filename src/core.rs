//! Transactional command/query boundary for durable Coordinator state.

use std::{
    collections::{BTreeMap, VecDeque},
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
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;
use uuid::Uuid;

use crate::adapter::{AdapterCapabilities, AdapterLifecycle, AdapterSnapshot};
use crate::attachment::{AttachmentMetadata, AttachmentStore};
use crate::contract::{
    CommandEvidenceV1, DeliveryAttemptId, DeliveryIntent, DependencyCondition,
    DependencyFailurePolicy, HarnessDefinitionV1, HarnessId, HarnessSessionId, HarnessTier,
    MessageId, MessageKind, MessageSubmissionV1, ObservationCheckpoint, RepositoryAccess,
    RepositoryObservationId, RepositoryObservationV1, ResultManifestV1, SCHEMA_VERSION,
    ScopeClassification, SessionReusePolicy, SupervisorEventDeliveryState, SupervisorEventId,
    SupervisorEventKind, TaskGraphWatchId, TaskId, TaskRole, TaskSubmissionV1, Validate,
    WorktreeHoldId, WriteScopeV1,
};
use crate::profile::parse_launch_profile_snapshot;
use crate::repository::{GitRepository, RepositorySnapshot};
use crate::session_reuse::{SessionReuseCandidate, effective_policy, evaluate_session_reuse};

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

/// Opaque bearer scoped to one live pane-resident Host connection generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostConnectionCapability(String);

impl HostConnectionCapability {
    fn generate() -> Self {
        let mut bytes = [0_u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        Self(hex::encode(bytes))
    }

    fn digest(&self) -> String {
        hex::encode(Sha256::digest(self.0.as_bytes()))
    }

    /// Parses the opaque bearer passed to a pane-resident provider bridge.
    ///
    /// # Errors
    ///
    /// Returns an error when the bearer does not match the v1 Host capability shape.
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
                "Host connection capability has an invalid shape",
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
    /// Current pane-resident Host generation authenticated by its expiring lease.
    Host {
        capability: HostConnectionCapability,
    },
}

impl From<SessionCapability> for ActorContext {
    fn from(capability: SessionCapability) -> Self {
        Self::Session { capability }
    }
}

impl From<HostConnectionCapability> for ActorContext {
    fn from(capability: HostConnectionCapability) -> Self {
        Self::Host { capability }
    }
}

/// State-changing operations accepted by [`Coordinator::execute`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CoordinatorCommand {
    /// Bind a new expiring Host connection and fence the previous generation.
    BindHostConnection {
        instance_id: String,
        lease_seconds: u32,
    },
    /// Extend the current Host connection's presence lease.
    RenewHostConnection,
    /// Close the current Host connection without ending its durable Session.
    DisconnectHostConnection { diagnostic: Option<String> },
    /// Expire stale Host connections and conservatively settle ambiguous work.
    ReapStaleHostConnections,
    /// Atomically reserve one managed Supervisor pane reopen attempt.
    PrepareSupervisorReconnect,
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
    /// Rotate an idle Worker to a fresh Coordinator/native Session in the same pane.
    RotateWorkerSession,
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
    /// End the idle Supervisor after all Workers and repository guards settle.
    DeactivateWorkspace,
    /// Confirm Worker Host process shutdown.
    RecordHostStopped { clean: bool },
    /// Record Worker Host failure and conservatively settle active work.
    RecordHostFailed { diagnostic: String },
    /// Mark a Worker online only after its pane Host and native Adapter are ready.
    RecordHostReady,
    /// Retain executable, version, native identity, model, and handshake evidence.
    RecordHostCompatibility {
        resolved_executable: PathBuf,
        observed_version: String,
        native_session_id: Option<String>,
        native_thread_id: Option<String>,
        effective_model: Option<String>,
        safe_compaction: bool,
        evidence: HarnessCompatibilityEvidenceV1,
    },
    /// Register the native conversation bound by the managed Supervisor Host.
    RecordSupervisorBinding {
        native_session_id: Option<String>,
        native_thread_id: Option<String>,
    },
    /// Mark the managed Supervisor native transport offline without ending durable state.
    RecordSupervisorDisconnected { diagnostic: Option<String> },
    /// Persist the latest provider-neutral native health evidence.
    RecordAdapterSnapshot { snapshot: AdapterSnapshot },
    /// Claim the oldest retry-safe durable Supervisor-attention event.
    ClaimNextSupervisorEvent,
    /// Record provider-native acceptance separately from model processing.
    AcceptSupervisorEvent {
        event_id: SupervisorEventId,
        native_correlation: String,
        native_turn_id: Option<String>,
        evidence: String,
    },
    /// Append provider-native evidence that an accepted event entered its visible turn.
    RecordSupervisorEventPresentation {
        event_id: SupervisorEventId,
        phase: SupervisorPresentationPhase,
        native_turn_id: Option<String>,
        evidence: String,
    },
    /// Mark an attempted native Supervisor injection as ambiguous.
    MarkSupervisorEventUnknown {
        event_id: SupervisorEventId,
        diagnostic: String,
    },
    /// Return a definitively unwritten Supervisor injection to the FIFO.
    ReleaseSupervisorEvent {
        event_id: SupervisorEventId,
        diagnostic: String,
    },
    /// Explicitly acknowledge one or more Inbox-backed events as processed.
    AcknowledgeSupervisorEvents { event_ids: Vec<SupervisorEventId> },
    /// Reconcile an event whose native injection is ambiguous.
    ReconcileSupervisorEvent {
        event_id: SupervisorEventId,
        resolution: SupervisorEventResolution,
        audit_note: String,
    },
    /// Register a durable completion watch for an explicit Task-root set.
    WatchTaskGraph {
        root_task_ids: Vec<TaskId>,
        request_key: Option<String>,
    },
    /// Persist one monotonic pane-resident Host event for reconnect replay.
    RecordHostEvent { sequence: u64, event: Value },
}

/// Versioned native startup evidence retained with one Harness Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessCompatibilityEvidenceV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Native Harness whose handshake was observed.
    pub kind: crate::contract::HarnessKind,
    /// Adapter operations supported after startup.
    pub capabilities: AdapterCapabilities,
    /// Ordered native checks that succeeded before readiness.
    pub successful_checks: Vec<String>,
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

/// Explicit resolution for an ambiguous Supervisor native injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorEventResolution {
    Retry,
    Processed,
    Cancel,
}

/// Provider-native presentation evidence for an accepted Supervisor event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorPresentationPhase {
    Presented,
    TurnStarted,
    TurnCompleted,
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
    /// Return dependency, readiness, and admission details for all Tasks.
    TaskGraph,
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
    /// Return explicit and frozen dependency inputs for one Task delivery.
    ResolvedTaskInput { task_id: TaskId },
    /// Return durable Supervisor events ordered by attention FIFO.
    SupervisorEvents,
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

/// Logical dependency readiness, independent of native Task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskSchedulingState {
    /// At least one declared dependency is not satisfied.
    Blocked,
    /// Every declared dependency is satisfied; capacity admission may still wait.
    Ready,
}

impl TaskSchedulingState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::Ready => "ready",
        }
    }
}

impl FromStr for TaskSchedulingState {
    type Err = CoordinatorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "blocked" => Ok(Self::Blocked),
            "ready" => Ok(Self::Ready),
            _ => Err(CoordinatorError::new(
                ErrorCategory::StorageFailure,
                format!("unknown Task scheduling state `{value}`"),
            )),
        }
    }
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
    pub task_role: TaskRole,
    pub requested_session_policy: SessionReusePolicy,
    pub effective_session_policy: Option<SessionReusePolicy>,
    pub harness_session_id: Option<HarnessSessionId>,
    pub session_reused: Option<bool>,
    pub session_decision_reason: Option<String>,
    pub context_percent: Option<String>,
}

/// One persisted dependency edge in a Task graph projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDependencyView {
    pub task_id: TaskId,
    pub condition: crate::contract::DependencyCondition,
    pub failure_policy: crate::contract::DependencyFailurePolicy,
    pub satisfied_by_result_revision: Option<u32>,
}

/// Scheduling projection used by the Supervisor and popup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGraphView {
    pub task: TaskView,
    pub scheduling_state: TaskSchedulingState,
    pub dependencies: Vec<TaskDependencyView>,
    pub dependents: Vec<TaskId>,
    pub worker_queue_position: Option<u32>,
    pub waiting_for_worker: bool,
    pub waiting_for_session: bool,
    pub waiting_for_repository: bool,
}

/// Immutable upstream Result reference delivered to a dependent Worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyResultRef {
    pub task_id: TaskId,
    pub result_revision: u32,
    pub attachment_id: crate::contract::AttachmentId,
}

/// Attachment-only resolved input; Result bodies are never inlined into Task text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTaskInputView {
    pub explicit_attachments: Vec<crate::contract::AttachmentId>,
    pub dependency_results: Vec<DependencyResultRef>,
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

/// Durable event delivery projection used by the Supervisor Host and popup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorEventView {
    pub id: SupervisorEventId,
    pub kind: SupervisorEventKind,
    pub task_id: Option<TaskId>,
    pub result_revision: Option<u32>,
    pub source_message_id: Option<MessageId>,
    pub summary: String,
    pub attachments: Vec<crate::contract::AttachmentId>,
    pub delivery_intent: DeliveryIntent,
    pub state: SupervisorEventDeliveryState,
    pub created_at: String,
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
    /// Previously bound native OMP Session identity, when present.
    pub native_session_id: Option<String>,
    /// Previously bound native Codex thread identity, when present.
    pub native_thread_id: Option<String>,
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
    /// A new generation-fenced Host connection was bound.
    HostConnectionBound {
        connection_id: String,
        generation: u64,
        capability: HostConnectionCapability,
        expires_at: String,
    },
    /// The current Host connection lease was extended.
    HostConnectionRenewed { expires_at: String },
    /// The Host connection was disconnected or expired.
    HostConnectionDisconnected,
    /// Number of stale Host connections expired by the daemon.
    StaleHostConnectionsReaped { count: u32 },
    /// Whether this daemon acquired the durable Supervisor reopen claim.
    SupervisorReconnectPrepared { claimed: bool },
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
    /// The Worker Host must rotate before retrying this Task.
    SessionRotationRequired { task_id: TaskId },
    /// Required reuse needs one provider-native safe compaction before retry.
    SessionCompactionRequired { task_id: TaskId },
    /// New capability created for same-pane Worker Session rotation.
    WorkerSessionRotated {
        session_id: HarnessSessionId,
        capability: SessionCapability,
    },
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
    /// Idle workspace Sessions ended without discarding durable state.
    WorkspaceDeactivated,
    /// Worker Host shutdown was durably settled.
    HostStopped { clean: bool },
    /// Worker Host and native Adapter are ready for dispatch.
    HostReady,
    /// Native compatibility evidence was retained before readiness.
    HostCompatibilityRecorded,
    /// Managed Supervisor native identity was durably bound.
    SupervisorBound,
    /// Managed Supervisor transport is offline; durable attention remains pending.
    SupervisorDisconnected,
    /// Latest native health and context evidence was retained.
    AdapterSnapshotRecorded,
    /// Oldest retry-safe event was reserved for native injection.
    SupervisorEventClaimed { event: Option<SupervisorEventView> },
    /// Supervisor event delivery state was updated.
    SupervisorEventUpdated {
        event_id: SupervisorEventId,
        state: SupervisorEventDeliveryState,
    },
    /// Durable root Task watch was registered.
    TaskGraphWatchRegistered { watch_id: TaskGraphWatchId },
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
    /// Global dependency-aware scheduling projection.
    TaskGraph(Vec<TaskGraphView>),
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
    /// Frozen delivery input for a Task.
    ResolvedTaskInput(ResolvedTaskInputView),
    /// Durable Supervisor-attention events.
    SupervisorEvents(Vec<SupervisorEventView>),
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
        coordinator.recover_task_scheduling().await?;
        Ok(coordinator)
    }

    async fn recover_task_scheduling(&self) -> Result<(), CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM tasks WHERE state = 'queued' AND scheduling_state = 'blocked'",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let now = timestamp();
        for task_id in task_ids {
            reevaluate_new_task_dependencies(&mut transaction, parse_uuid_id(&task_id)?, &now)
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)
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
            (ActorContext::Bootstrap, CoordinatorCommand::ReapStaleHostConnections) => {
                self.reap_stale_host_connections().await
            }
            (ActorContext::Bootstrap, CoordinatorCommand::PrepareSupervisorReconnect) => {
                self.prepare_supervisor_reconnect().await
            }
            (ActorContext::Bootstrap, CoordinatorCommand::RegisterSupervisor { definition }) => {
                self.register_supervisor(definition).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::BindHostConnection {
                    instance_id,
                    lease_seconds,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.bind_host_connection(&actor, instance_id, lease_seconds)
                    .await
            }
            (ActorContext::Host { capability }, CoordinatorCommand::RenewHostConnection) => {
                let actor = self.authenticate_host(&capability).await?;
                self.renew_host_connection(&actor).await
            }
            (
                ActorContext::Host { capability },
                CoordinatorCommand::DisconnectHostConnection { diagnostic },
            ) => {
                let actor = self.authenticate_host(&capability).await?;
                self.disconnect_host_connection(&actor, diagnostic).await
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
            (ActorContext::Session { capability }, CoordinatorCommand::RotateWorkerSession) => {
                let actor = self.authenticate(&capability).await?;
                self.rotate_worker_session(&actor).await
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
            (ActorContext::Session { capability }, CoordinatorCommand::DeactivateWorkspace) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.deactivate_workspace(&actor).await
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
                CoordinatorCommand::RecordHostCompatibility {
                    resolved_executable,
                    observed_version,
                    native_session_id,
                    native_thread_id,
                    effective_model,
                    safe_compaction,
                    evidence,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_compatibility(
                    &actor,
                    resolved_executable,
                    observed_version,
                    native_session_id,
                    native_thread_id,
                    effective_model,
                    safe_compaction,
                    evidence,
                )
                .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordAdapterSnapshot { snapshot },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_adapter_snapshot(&actor, snapshot).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordSupervisorBinding {
                    native_session_id,
                    native_thread_id,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_supervisor_binding(&actor, native_session_id, native_thread_id)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordSupervisorDisconnected { diagnostic },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_supervisor_disconnected(&actor, diagnostic)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ClaimNextSupervisorEvent,
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.claim_next_supervisor_event(&actor).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::AcceptSupervisorEvent {
                    event_id,
                    native_correlation,
                    native_turn_id,
                    evidence,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.accept_supervisor_event(
                    &actor,
                    event_id,
                    native_correlation,
                    native_turn_id,
                    evidence,
                )
                .await
            }
            (
                ActorContext::Host { capability },
                CoordinatorCommand::RecordSupervisorEventPresentation {
                    event_id,
                    phase,
                    native_turn_id,
                    evidence,
                },
            ) => {
                let actor = self.authenticate_host(&capability).await?;
                self.require_supervisor(&actor)?;
                self.record_supervisor_event_presentation(
                    &actor,
                    event_id,
                    phase,
                    native_turn_id,
                    evidence,
                )
                .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::MarkSupervisorEventUnknown {
                    event_id,
                    diagnostic,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.mark_supervisor_event_unknown(event_id, diagnostic)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ReleaseSupervisorEvent {
                    event_id,
                    diagnostic,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.release_supervisor_event(event_id, diagnostic).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::AcknowledgeSupervisorEvents { event_ids },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.acknowledge_supervisor_events(&actor, event_ids).await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::ReconcileSupervisorEvent {
                    event_id,
                    resolution,
                    audit_note,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.reconcile_supervisor_event(event_id, resolution, audit_note)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::WatchTaskGraph {
                    root_task_ids,
                    request_key,
                },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.require_supervisor(&actor)?;
                self.watch_task_graph(&actor, root_task_ids, request_key)
                    .await
            }
            (
                ActorContext::Session { capability },
                CoordinatorCommand::RecordHostEvent { sequence, event },
            ) => {
                let actor = self.authenticate(&capability).await?;
                self.record_host_event(&actor, sequence, event).await
            }
            (ActorContext::Host { capability }, command) => {
                let actor = self.authenticate_host(&capability).await?;
                self.execute_host_command(&actor, command).await
            }
            _ => Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "command is not permitted for this actor",
            )),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive generation-fenced Host authorization boundary"
    )]
    async fn execute_host_command(
        &self,
        actor: &AuthenticatedActor,
        command: CoordinatorCommand,
    ) -> Result<CommandOutcome, CoordinatorError> {
        match command {
            CoordinatorCommand::AdmitAttachment {
                source,
                media_type,
                original_name,
            } => {
                self.admit_attachment(source, media_type, original_name)
                    .await
            }
            CoordinatorCommand::MarkInboxRead { message_ids } => {
                self.mark_inbox_read(actor, message_ids).await
            }
            CoordinatorCommand::StartWorker {
                definition,
                profile_snapshot,
                profile_digest,
            } => {
                self.require_supervisor(actor)?;
                self.start_worker(definition, profile_snapshot, profile_digest)
                    .await
            }
            CoordinatorCommand::CreateTask { submission } => {
                self.require_supervisor(actor)?;
                self.create_task(actor, submission).await
            }
            CoordinatorCommand::DispatchTask { task_id } => {
                self.require_supervisor(actor)?;
                self.dispatch_task(task_id).await
            }
            CoordinatorCommand::ClaimNextTask => self.claim_next_task(actor).await,
            CoordinatorCommand::RotateWorkerSession => self.rotate_worker_session(actor).await,
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation,
            } => {
                self.accept_delivery(actor, message_id, native_correlation)
                    .await
            }
            CoordinatorCommand::SendMessage { submission } => {
                self.send_message(actor, submission).await
            }
            CoordinatorCommand::CompleteTask {
                manifest,
                native_turn_id,
            } => self.complete_task(actor, manifest, native_turn_id).await,
            CoordinatorCommand::RecordTurnCompleted {
                task_id,
                native_turn_id,
                succeeded,
            } => {
                self.record_turn_completed(actor, task_id, native_turn_id, succeeded)
                    .await
            }
            CoordinatorCommand::RecordCancellationCompleted { task_id, succeeded } => {
                self.record_cancellation_completed(actor, task_id, succeeded)
                    .await
            }
            CoordinatorCommand::CaptureRepositoryObservation {
                task_id,
                checkpoint,
            } => {
                self.capture_repository_observation(actor, task_id, checkpoint)
                    .await
            }
            CoordinatorCommand::ApproveTask {
                task_id,
                result_revision,
                observation_digest,
            } => {
                self.require_supervisor(actor)?;
                self.approve_task(task_id, result_revision, observation_digest)
                    .await
            }
            CoordinatorCommand::CancelTask { task_id } => {
                self.require_supervisor(actor)?;
                self.cancel_task(task_id).await
            }
            CoordinatorCommand::MarkDeliveryUnknown {
                message_id,
                diagnostic,
            } => {
                self.mark_delivery_unknown(actor, message_id, diagnostic)
                    .await
            }
            CoordinatorCommand::ResolveDeliveryUnknown {
                task_id,
                resolution,
                observation_digest,
                audit_note,
            } => {
                self.require_supervisor(actor)?;
                self.resolve_delivery_unknown(task_id, resolution, observation_digest, audit_note)
                    .await
            }
            CoordinatorCommand::ClearWorktreeHold {
                task_id,
                observation_digest,
                audit_note,
            } => {
                self.require_supervisor(actor)?;
                self.clear_worktree_hold(task_id, observation_digest, audit_note)
                    .await
            }
            CoordinatorCommand::StopWorker { worker_id } => {
                self.require_supervisor(actor)?;
                self.stop_worker(worker_id).await
            }
            CoordinatorCommand::RecordHostStopped { clean } => {
                self.record_host_stopped(actor, clean).await
            }
            CoordinatorCommand::RecordHostFailed { diagnostic } => {
                self.record_host_failed(actor, diagnostic).await
            }
            CoordinatorCommand::RecordHostReady => self.record_host_ready(actor).await,
            CoordinatorCommand::RecordHostCompatibility {
                resolved_executable,
                observed_version,
                native_session_id,
                native_thread_id,
                effective_model,
                safe_compaction,
                evidence,
            } => {
                self.record_host_compatibility(
                    actor,
                    resolved_executable,
                    observed_version,
                    native_session_id,
                    native_thread_id,
                    effective_model,
                    safe_compaction,
                    evidence,
                )
                .await
            }
            CoordinatorCommand::RecordSupervisorBinding {
                native_session_id,
                native_thread_id,
            } => {
                self.record_supervisor_binding(actor, native_session_id, native_thread_id)
                    .await
            }
            CoordinatorCommand::RecordSupervisorDisconnected { diagnostic } => {
                self.record_supervisor_disconnected(actor, diagnostic).await
            }
            CoordinatorCommand::RecordAdapterSnapshot { snapshot } => {
                self.record_adapter_snapshot(actor, snapshot).await
            }
            CoordinatorCommand::ClaimNextSupervisorEvent => {
                self.require_supervisor(actor)?;
                self.claim_next_supervisor_event(actor).await
            }
            CoordinatorCommand::AcceptSupervisorEvent {
                event_id,
                native_correlation,
                native_turn_id,
                evidence,
            } => {
                self.require_supervisor(actor)?;
                self.accept_supervisor_event(
                    actor,
                    event_id,
                    native_correlation,
                    native_turn_id,
                    evidence,
                )
                .await
            }
            CoordinatorCommand::MarkSupervisorEventUnknown {
                event_id,
                diagnostic,
            } => {
                self.require_supervisor(actor)?;
                self.mark_supervisor_event_unknown(event_id, diagnostic)
                    .await
            }
            CoordinatorCommand::ReleaseSupervisorEvent {
                event_id,
                diagnostic,
            } => {
                self.require_supervisor(actor)?;
                self.release_supervisor_event(event_id, diagnostic).await
            }
            CoordinatorCommand::AcknowledgeSupervisorEvents { event_ids } => {
                self.require_supervisor(actor)?;
                self.acknowledge_supervisor_events(actor, event_ids).await
            }
            CoordinatorCommand::ReconcileSupervisorEvent {
                event_id,
                resolution,
                audit_note,
            } => {
                self.require_supervisor(actor)?;
                self.reconcile_supervisor_event(event_id, resolution, audit_note)
                    .await
            }
            CoordinatorCommand::WatchTaskGraph {
                root_task_ids,
                request_key,
            } => {
                self.require_supervisor(actor)?;
                self.watch_task_graph(actor, root_task_ids, request_key)
                    .await
            }
            CoordinatorCommand::RecordSupervisorEventPresentation {
                event_id,
                phase,
                native_turn_id,
                evidence,
            } => {
                self.require_supervisor(actor)?;
                self.record_supervisor_event_presentation(
                    actor,
                    event_id,
                    phase,
                    native_turn_id,
                    evidence,
                )
                .await
            }
            CoordinatorCommand::RecordHostEvent { sequence, event } => {
                self.record_host_event(actor, sequence, event).await
            }
            _ => Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "command is not permitted for a managed Host connection",
            )),
        }
    }

    /// Executes one authenticated query without exposing `SQLite` internals.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when authentication or storage fails.
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive authenticated query authorization boundary"
    )]
    pub async fn query(
        &self,
        actor: ActorContext,
        query: CoordinatorQuery,
    ) -> Result<QueryResult, CoordinatorError> {
        let actor = match actor {
            ActorContext::Session { capability } => self.authenticate(&capability).await?,
            ActorContext::Host { capability } => self.authenticate_host(&capability).await?,
            ActorContext::Bootstrap => {
                return Err(CoordinatorError::new(
                    ErrorCategory::Unauthenticated,
                    "a live Session or Host capability is required",
                ));
            }
        };
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
                    sqlx::query("SELECT t.id, t.worker_id, t.state, t.result_revision, t.task_role, t.session_reuse_policy, b.effective_policy, b.harness_session_id, b.reused, b.decision_reason, b.context_percent FROM tasks t LEFT JOIN task_session_bindings b ON b.task_id = t.id AND b.superseded_at IS NULL WHERE t.id = ?")
                        .bind(task_id.to_string())
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(CoordinatorError::storage)?
                        .ok_or_else(|| {
                            CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
                        })?;
                Ok(QueryResult::Task(task_view_from_row(&row)?))
            }
            CoordinatorQuery::ListTasks => self.list_tasks().await,
            CoordinatorQuery::TaskGraph => {
                self.require_supervisor(&actor)?;
                self.task_graph().await
            }
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
            CoordinatorQuery::ResolvedTaskInput { task_id } => {
                let row = sqlx::query("SELECT worker_id, submission_json FROM tasks WHERE id = ?")
                    .bind(task_id.to_string())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(CoordinatorError::storage)?
                    .ok_or_else(|| {
                        CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist")
                    })?;
                if actor.tier != HarnessTier::Supervisor
                    && row.get::<&str, _>("worker_id") != actor.id.as_str()
                {
                    return Err(CoordinatorError::new(
                        ErrorCategory::Forbidden,
                        "only the Supervisor or assigned Worker may resolve Task input",
                    ));
                }
                let submission: TaskSubmissionV1 = serde_json::from_str(row.get("submission_json"))
                    .map_err(CoordinatorError::storage)?;
                let dependencies = sqlx::query("SELECT dependency_task_id, satisfied_by_result_revision, result_snapshot_attachment_id FROM task_dependencies WHERE task_id = ? AND bound_at IS NOT NULL ORDER BY dependency_task_id")
                    .bind(task_id.to_string())
                    .fetch_all(&self.pool)
                    .await
                    .map_err(CoordinatorError::storage)?
                    .iter()
                    .map(|dependency| {
                        let revision: i64 = dependency.get("satisfied_by_result_revision");
                        Ok(DependencyResultRef {
                            task_id: parse_uuid_id(dependency.get("dependency_task_id"))?,
                            result_revision: u32::try_from(revision)
                                .map_err(CoordinatorError::storage)?,
                            attachment_id: parse_uuid_id(
                                dependency.get("result_snapshot_attachment_id"),
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoordinatorError>>()?;
                Ok(QueryResult::ResolvedTaskInput(ResolvedTaskInputView {
                    explicit_attachments: submission.attachments,
                    dependency_results: dependencies,
                }))
            }
            CoordinatorQuery::SupervisorEvents => {
                self.require_supervisor(&actor)?;
                Ok(QueryResult::SupervisorEvents(
                    self.supervisor_events().await?,
                ))
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
            "SELECT t.id, t.worker_id, t.state, t.result_revision, t.task_role, t.session_reuse_policy, b.effective_policy, b.harness_session_id, b.reused, b.decision_reason, b.context_percent FROM tasks t LEFT JOIN task_session_bindings b ON b.task_id = t.id AND b.superseded_at IS NULL ORDER BY t.created_sequence",
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

    async fn task_graph(&self) -> Result<QueryResult, CoordinatorError> {
        let rows = sqlx::query("SELECT t.id, t.worker_id, t.state, t.scheduling_state, t.result_revision, t.submission_json, t.created_sequence, t.task_role, t.session_reuse_policy, b.effective_policy, b.harness_session_id, b.reused, b.decision_reason, b.context_percent FROM tasks t LEFT JOIN task_session_bindings b ON b.task_id = t.id AND b.superseded_at IS NULL ORDER BY t.created_sequence")
            .fetch_all(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        let mut graph = Vec::with_capacity(rows.len());
        for row in rows {
            let task = task_view_from_row(&row)?;
            let dependencies = sqlx::query("SELECT dependency_task_id, condition, failure_policy, satisfied_by_result_revision FROM task_dependencies WHERE task_id = ? ORDER BY dependency_task_id")
                .bind(task.id.to_string())
                .fetch_all(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?
                .iter()
                .map(task_dependency_view_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            let dependents = sqlx::query_scalar::<_, String>("SELECT task_id FROM task_dependencies WHERE dependency_task_id = ? ORDER BY task_id")
                .bind(task.id.to_string())
                .fetch_all(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?
                .iter()
                .map(|id| parse_uuid_id(id))
                .collect::<Result<Vec<_>, _>>()?;
            let scheduling_state = TaskSchedulingState::from_str(row.get("scheduling_state"))?;
            let position: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND scheduling_state = 'ready' AND state = 'queued' AND created_sequence <= ?")
                .bind(task.worker_id.as_str())
                .bind(row.get::<i64, _>("created_sequence"))
                .fetch_one(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
            let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND id <> ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')")
                .bind(task.worker_id.as_str())
                .bind(task.id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
            let submission: TaskSubmissionV1 = serde_json::from_str(row.get("submission_json"))
                .map_err(CoordinatorError::storage)?;
            let repository_key = submission.repository.root.to_string_lossy();
            let repository_blockers: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL) + (SELECT COUNT(*) FROM worktree_leases WHERE repository_key = ? AND released_at IS NULL AND task_id <> ?)")
                .bind(repository_key.as_ref())
                .bind(repository_key.as_ref())
                .bind(task.id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
            let is_waiting =
                scheduling_state == TaskSchedulingState::Ready && task.state == TaskState::Queued;
            let waiting_for_session =
                is_waiting && task.harness_session_id.is_none() && active == 0;
            graph.push(TaskGraphView {
                task,
                scheduling_state,
                dependencies,
                dependents,
                worker_queue_position: is_waiting
                    .then(|| u32::try_from(position).unwrap_or(u32::MAX)),
                waiting_for_worker: is_waiting && active != 0,
                waiting_for_session,
                waiting_for_repository: is_waiting && repository_blockers != 0,
            });
        }
        Ok(QueryResult::TaskGraph(graph))
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
        let row = sqlx::query("SELECT h.definition_json, s.native_session_id, s.native_thread_id, s.profile_snapshot_json, s.profile_digest, s.presence, s.activity, s.event_sequence FROM harness_sessions s JOIN harnesses h ON h.id = s.harness_id WHERE s.id = ? AND s.ended_at IS NULL")
            .bind(actor.session_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Session is no longer active"))?;
        Ok(QueryResult::Session(SessionSelfView {
            session_id: actor.session_id,
            native_session_id: row
                .get::<Option<&str>, _>("native_session_id")
                .map(str::to_owned),
            native_thread_id: row
                .get::<Option<&str>, _>("native_thread_id")
                .map(str::to_owned),
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
        let profile = parse_launch_profile_snapshot(&profile_snapshot).map_err(|error| {
            CoordinatorError::new(
                ErrorCategory::InvalidInput,
                format!("launch profile snapshot is invalid: {error}"),
            )
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
        let worker = sqlx::query("SELECT tier, cwd, model FROM harnesses WHERE id = ?")
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
        let profile_digest: Option<String> = sqlx::query_scalar(
            "SELECT profile_digest FROM harness_sessions WHERE harness_id = ? ORDER BY started_at DESC LIMIT 1",
        )
        .bind(submission.worker_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?
        .flatten();
        if let Some(preferred_session_id) = submission.preferred_session_id {
            let preferred_worker: Option<String> =
                sqlx::query_scalar("SELECT harness_id FROM harness_sessions WHERE id = ?")
                    .bind(preferred_session_id.to_string())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            if preferred_worker.as_deref() != Some(submission.worker_id.as_str()) {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    "preferred Session does not belong to the selected Worker",
                ));
            }
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
        for dependency in &submission.depends_on {
            let upstream = sqlx::query("SELECT submission_json FROM tasks WHERE id = ?")
                .bind(dependency.task_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .ok_or_else(|| {
                    CoordinatorError::new(
                        ErrorCategory::NotFound,
                        format!("dependency Task {} does not exist", dependency.task_id),
                    )
                })?;
            let upstream_submission: TaskSubmissionV1 =
                serde_json::from_str(upstream.get("submission_json"))
                    .map_err(CoordinatorError::storage)?;
            let upstream_repository = GitRepository::open(&upstream_submission.repository.root)
                .map_err(|error| {
                    CoordinatorError::new(ErrorCategory::InvalidInput, error.to_string())
                })?;
            if upstream_repository.identity().worktree_root != *canonical_root {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidInput,
                    "dependency Task belongs to a different canonical worktree",
                ));
            }
        }
        let sequence = next_sequence(&mut transaction, "task_create").await?;
        let task_id = TaskId::new();
        let message_id = MessageId::new();
        let now = timestamp();
        let submission_json =
            serde_json::to_string(&submission).map_err(CoordinatorError::storage)?;
        let scheduling_state = if submission.depends_on.is_empty() {
            TaskSchedulingState::Ready
        } else {
            TaskSchedulingState::Blocked
        };
        sqlx::query("INSERT INTO tasks (id, worker_id, related_task_id, submission_json, state, scheduling_state, task_role, session_reuse_policy, preferred_session_id, expected_profile_digest, expected_model, expected_tool_policy_digest, created_sequence, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(task_id.to_string())
            .bind(submission.worker_id.as_str())
            .bind(submission.related_task_id.map(|id| id.to_string()))
            .bind(&submission_json)
            .bind(TaskState::Queued.as_str())
            .bind(scheduling_state.as_str())
            .bind(task_role_as_str(submission.task_role))
            .bind(session_reuse_policy_as_str(submission.session_reuse))
            .bind(submission.preferred_session_id.map(|id| id.to_string()))
            .bind(profile_digest.as_deref())
            .bind(worker.get::<Option<&str>, _>("model"))
            .bind(profile_digest.as_deref())
            .bind(sequence)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO task_scheduling_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, NULL, ?, '{}', ?)")
            .bind(task_id.to_string())
            .bind(scheduling_state.as_str())
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        for dependency in &submission.depends_on {
            sqlx::query("INSERT INTO task_dependencies (task_id, dependency_task_id, condition, failure_policy) VALUES (?, ?, ?, ?)")
                .bind(task_id.to_string())
                .bind(dependency.task_id.to_string())
                .bind(dependency_condition_as_str(dependency.condition))
                .bind(dependency_failure_policy_as_str(dependency.failure_policy))
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        validate_acyclic_task_graph(&mut transaction).await?;
        reevaluate_new_task_dependencies(&mut transaction, task_id, &now).await?;
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

    #[expect(
        clippy::too_many_lines,
        reason = "Session identity, safety, and audit evidence are selected atomically"
    )]
    async fn prepare_task_session_binding(
        &self,
        task_id: TaskId,
    ) -> Result<HarnessSessionId, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let existing_binding = sqlx::query_scalar::<_, String>(
            "SELECT harness_session_id FROM task_session_bindings WHERE task_id = ? AND superseded_at IS NULL",
        )
        .bind(task_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT worker_id, state, scheduling_state, submission_json, expected_profile_digest, expected_model, expected_tool_policy_digest FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(task.get("state"), TaskState::Queued)?;
        if task.get::<&str, _>("scheduling_state") != TaskSchedulingState::Ready.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task dependencies are not satisfied",
            ));
        }
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        let policy = effective_policy(&submission);
        let worker_id: &str = task.get("worker_id");
        let related_session: Option<String> = if let Some(related_task_id) =
            submission.related_task_id
        {
            sqlx::query_scalar("SELECT harness_session_id FROM task_session_bindings WHERE task_id = ? ORDER BY sequence DESC LIMIT 1")
                .bind(related_task_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
        } else {
            None
        };
        let candidate_id = if existing_binding.is_some() {
            existing_binding.clone()
        } else if let Some(preferred) = submission.preferred_session_id {
            Some(preferred.to_string())
        } else if policy != SessionReusePolicy::Fresh && related_session.is_some() {
            related_session
        } else {
            sqlx::query_scalar("SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1")
                .bind(worker_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
        };
        let candidate_id = candidate_id.ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::TargetOffline,
                "assigned Worker has no Session",
            )
        })?;
        let session = sqlx::query("SELECT id, harness_id, presence, activity, profile_digest, effective_model, tool_policy_digest, native_health, context_tokens, context_window, context_percent, compaction_count, adapter_snapshot_json FROM harness_sessions WHERE id = ? AND ended_at IS NULL")
            .bind(&candidate_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::InvalidState, "required Session is unavailable"))?;
        let candidate_session_id = parse_uuid_id::<HarnessSessionId>(session.get("id"))?;
        let busy: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE active_session_id = ? AND id <> ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')")
            .bind(&candidate_id)
            .bind(task_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE active_session_id = ? AND state = 'waiting'",
        )
        .bind(&candidate_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let unknown: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE active_session_id = ? AND state = 'delivery_unknown'",
        )
        .bind(&candidate_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let cancelling: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE active_session_id = ? AND state = 'cancelling'",
        )
        .bind(&candidate_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let holds: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worktree_holds h JOIN tasks t ON t.id = h.task_id WHERE t.active_session_id = ? AND h.cleared_at IS NULL")
            .bind(&candidate_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let prior_bindings: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_session_bindings WHERE harness_session_id = ?",
        )
        .bind(&candidate_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let native_health = native_session_health_from_str(session.get("native_health"))?;
        let lifecycle = if session.get::<&str, _>("presence") == "online"
            && session.get::<&str, _>("activity") == "idle"
        {
            AdapterLifecycle::Idle
        } else if session.get::<&str, _>("presence") == "online" {
            AdapterLifecycle::Working
        } else {
            AdapterLifecycle::Failed
        };
        let adapter = session
            .get::<Option<&str>, _>("adapter_snapshot_json")
            .and_then(|json| serde_json::from_str::<AdapterSnapshot>(json).ok())
            .unwrap_or(AdapterSnapshot {
                lifecycle,
                session_id: None,
                thread_id: None,
                active_turn_id: None,
                steerable: false,
                queued_input_count: None,
                model: session
                    .get::<Option<&str>, _>("effective_model")
                    .map(str::to_owned),
                native_health,
                context_tokens: session.get("context_tokens"),
                context_window: session.get("context_window"),
                context_percent: session.get("context_percent"),
                compaction_count: session.get("compaction_count"),
            });
        let expected_profile = task.get::<Option<&str>, _>("expected_profile_digest");
        let expected_model = task.get::<Option<&str>, _>("expected_model");
        let expected_tool_policy = task.get::<Option<&str>, _>("expected_tool_policy_digest");
        let candidate = SessionReuseCandidate {
            session_id: candidate_session_id,
            same_worker: session.get::<&str, _>("harness_id") == worker_id,
            same_harness_kind: true,
            same_launch_profile: expected_profile
                == session.get::<Option<&str>, _>("profile_digest"),
            same_repository: true,
            same_tool_policy: expected_tool_policy
                == session
                    .get::<Option<&str>, _>("tool_policy_digest")
                    .or(session.get::<Option<&str>, _>("profile_digest")),
            compatible_model: expected_model == session.get::<Option<&str>, _>("effective_model"),
            has_active_task: busy != 0,
            has_unresolved_question: waiting != 0,
            has_delivery_unknown: unknown != 0,
            has_unresolved_cancellation: cancelling != 0,
            has_session_worktree_hold: holds != 0,
            native_protocol_unambiguous: native_health
                != crate::contract::NativeSessionHealth::Ambiguous,
            previously_bound: prior_bindings != 0,
            adapter,
        };
        let decision = if existing_binding.is_some()
            || (policy == SessionReusePolicy::Fresh && prior_bindings == 0)
        {
            let mut compatibility_task = submission.clone();
            compatibility_task.session_reuse = SessionReusePolicy::Prefer;
            let compatibility = evaluate_session_reuse(&compatibility_task, &candidate);
            if compatibility.reusable {
                crate::session_reuse::SessionReuseDecision {
                    reusable: true,
                    effective_policy: policy,
                    reason_code: if existing_binding.is_some() {
                        "binding_revalidated".to_owned()
                    } else {
                        "fresh_unbound_session".to_owned()
                    },
                    reason: if existing_binding.is_some() {
                        "existing Task binding remains compatible and healthy".to_owned()
                    } else {
                        "fresh compatible native Session has not executed another Task".to_owned()
                    },
                }
            } else {
                compatibility
            }
        } else {
            evaluate_session_reuse(&submission, &candidate)
        };
        if !decision.reusable {
            if existing_binding.is_some()
                && submission.session_reuse != SessionReusePolicy::Required
            {
                let now = timestamp();
                sqlx::query("UPDATE task_session_bindings SET superseded_at = ? WHERE task_id = ? AND superseded_at IS NULL")
                    .bind(&now)
                    .bind(task_id.to_string())
                    .execute(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
                transaction
                    .commit()
                    .await
                    .map_err(CoordinatorError::storage)?;
            }
            // A never-used Session that cannot satisfy its own immutable launch
            // selection will not become compatible by rotating the same profile.
            // Surface the blocker instead of creating an unbounded launch loop.
            let category = if submission.session_reuse == SessionReusePolicy::Required
                || prior_bindings == 0
            {
                ErrorCategory::InvalidState
            } else {
                ErrorCategory::TargetOffline
            };
            return Err(CoordinatorError::new(
                category,
                format!(
                    "Session selection blocked: {} ({})",
                    decision.reason, decision.reason_code
                ),
            ));
        }
        if existing_binding.is_some() {
            transaction
                .commit()
                .await
                .map_err(CoordinatorError::storage)?;
            return Ok(candidate_session_id);
        }
        let now = timestamp();
        let context_tokens = candidate
            .adapter
            .context_tokens
            .map(i64::try_from)
            .transpose()
            .map_err(CoordinatorError::storage)?;
        let context_window = candidate
            .adapter
            .context_window
            .map(i64::try_from)
            .transpose()
            .map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO task_session_bindings (task_id, harness_session_id, requested_policy, effective_policy, reused, reason_code, decision_reason, adapter_snapshot_json, context_tokens, context_window, context_percent, bound_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(task_id.to_string())
            .bind(&candidate_id)
            .bind(session_reuse_policy_as_str(submission.session_reuse))
            .bind(session_reuse_policy_as_str(decision.effective_policy))
            .bind(i64::from(prior_bindings != 0))
            .bind(&decision.reason_code)
            .bind(&decision.reason)
            .bind(serde_json::to_string(&candidate.adapter).map_err(CoordinatorError::storage)?)
            .bind(context_tokens)
            .bind(context_window)
            .bind(candidate.adapter.context_percent)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(candidate_session_id)
    }

    async fn dispatch_task(&self, task_id: TaskId) -> Result<CommandOutcome, CoordinatorError> {
        self.preflight_dispatch(task_id).await?;
        self.prepare_task_session_binding(task_id).await?;
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
        let row = sqlx::query("SELECT worker_id, state, scheduling_state, created_sequence, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(row.get("state"), TaskState::Queued)?;
        if row.get::<&str, _>("scheduling_state") != TaskSchedulingState::Ready.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task dependencies are not satisfied",
            ));
        }
        let worker_id: &str = row.get("worker_id");
        let earlier: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND scheduling_state = 'ready' AND state = 'queued' AND created_sequence < ?")
            .bind(worker_id)
            .bind(row.get::<i64, _>("created_sequence"))
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if earlier != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "an earlier Ready Task owns the Worker FIFO head",
            ));
        }
        let submission: TaskSubmissionV1 =
            serde_json::from_str(row.get("submission_json")).map_err(CoordinatorError::storage)?;
        let session_id: Option<String> = sqlx::query_scalar(
            "SELECT b.harness_session_id FROM task_session_bindings b JOIN harness_sessions s ON s.id = b.harness_session_id WHERE b.task_id = ? AND b.superseded_at IS NULL AND s.harness_id = ? AND s.ended_at IS NULL AND s.presence = 'online' AND s.activity = 'idle'",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let session_id = session_id.ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::TargetOffline,
                "bound Worker Session is unavailable",
            )
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
        let unmet_dependencies: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_dependencies WHERE task_id = ? AND satisfied_at IS NULL",
        )
        .bind(task_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if unmet_dependencies != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task dependencies changed before dispatch",
            ));
        }
        sqlx::query(
            "UPDATE task_dependencies SET bound_at = ? WHERE task_id = ? AND bound_at IS NULL",
        )
        .bind(&now)
        .bind(task_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
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
        let row = sqlx::query("SELECT worker_id, state, scheduling_state, created_sequence, submission_json FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        require_state(row.get("state"), TaskState::Queued)?;
        if row.get::<&str, _>("scheduling_state") != TaskSchedulingState::Ready.as_str() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Task dependencies are not satisfied",
            ));
        }
        let worker_id: &str = row.get("worker_id");
        let earlier: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND scheduling_state = 'ready' AND state = 'queued' AND created_sequence < ?")
            .bind(worker_id)
            .bind(row.get::<i64, _>("created_sequence"))
            .fetch_one(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        if earlier != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "an earlier Ready Task owns the Worker FIFO head",
            ));
        }
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
            "SELECT id FROM tasks WHERE worker_id = ? AND state = 'queued' AND scheduling_state = 'ready' ORDER BY created_sequence LIMIT 1",
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
                if error.category == ErrorCategory::TargetOffline
                    && error.message.starts_with("Session selection blocked:") =>
            {
                Ok(CommandOutcome::SessionRotationRequired {
                    task_id: parse_uuid_id(&task_id)?,
                })
            }
            Err(error)
                if error.category == ErrorCategory::InvalidState
                    && error.message.starts_with("Session selection blocked:") =>
            {
                let task_id = parse_uuid_id(&task_id)?;
                if error.message.contains("(context_pressure)")
                    && self.required_compaction_available(actor).await?
                {
                    return Ok(CommandOutcome::SessionCompactionRequired { task_id });
                }
                self.record_session_blocker_event(task_id, &error.message)
                    .await?;
                Ok(CommandOutcome::NoTaskAvailable)
            }
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

    async fn required_compaction_available(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<bool, CoordinatorError> {
        let row = sqlx::query("SELECT safe_compaction, compaction_count FROM harness_sessions WHERE id = ? AND ended_at IS NULL")
            .bind(actor.session_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(row.is_some_and(|row| {
            row.get::<i64, _>("safe_compaction") != 0
                && row
                    .get::<Option<i64>, _>("compaction_count")
                    .unwrap_or_default()
                    == 0
        }))
    }

    async fn record_session_blocker_event(
        &self,
        task_id: TaskId,
        diagnostic: &str,
    ) -> Result<(), CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        insert_supervisor_event(
            &mut transaction,
            SupervisorEventKind::Notification,
            Some(task_id),
            None,
            None,
            &format!("task:{task_id}:session_blocked"),
            diagnostic,
            &[],
            DeliveryIntent::FollowUp,
            &now,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)
    }

    async fn rotate_worker_session(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only a Worker Host may rotate its Session",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown')")
            .bind(actor.id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if active != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Worker Session rotation requires no active Task",
            ));
        }
        let old = sqlx::query("SELECT profile_snapshot_json, profile_digest, connection_generation FROM harness_sessions WHERE id = ? AND ended_at IS NULL AND presence = 'online' AND activity = 'idle'")
            .bind(actor.session_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::InvalidState, "Worker Session is not idle and online"))?;
        let now = timestamp();
        sqlx::query("UPDATE harness_sessions SET presence = 'stopped', activity = 'idle', ended_at = ?, last_seen_at = ? WHERE id = ?")
            .bind(&now)
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let session_id = HarnessSessionId::new();
        let capability = SessionCapability::generate();
        let generation: i64 = old.get("connection_generation");
        sqlx::query("INSERT INTO harness_sessions (id, harness_id, harness_tier, capability_hash, connection_generation, presence, activity, profile_snapshot_json, profile_digest, native_health, started_at, last_seen_at) VALUES (?, ?, 'worker', ?, ?, 'starting', 'starting', ?, ?, 'ambiguous', ?, ?)")
            .bind(session_id.to_string())
            .bind(actor.id.as_str())
            .bind(capability.digest())
            .bind(generation + 1)
            .bind(old.get::<Option<&str>, _>("profile_snapshot_json"))
            .bind(old.get::<Option<&str>, _>("profile_digest"))
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
        Ok(CommandOutcome::WorkerSessionRotated {
            session_id,
            capability,
        })
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
            "SELECT CASE WHEN ? = 'worker' AND ? IS NOT NULL THEN (SELECT b.harness_session_id FROM task_session_bindings b JOIN harness_sessions s ON s.id = b.harness_session_id WHERE b.task_id = ? AND b.superseded_at IS NULL AND s.ended_at IS NULL) ELSE (SELECT id FROM harness_sessions WHERE harness_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1) END",
        )
        .bind(&recipient_tier)
        .bind(submission.task_id.map(|id| id.to_string()))
        .bind(submission.task_id.map(|id| id.to_string()))
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
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::BlockingQuestion,
                Some(task_id),
                None,
                Some(message_id),
                &format!("question:{message_id}"),
                &submission.text,
                &submission.attachments,
                submission.delivery,
                &now,
            )
            .await?;
        } else if let Some(task_id) = submission.task_id {
            match submission.kind {
                MessageKind::Reply => {
                    process_matching_supervisor_events(
                        &mut transaction,
                        SupervisorEventKind::BlockingQuestion,
                        task_id,
                        None,
                        &now,
                    )
                    .await?;
                }
                MessageKind::Correction => {
                    process_matching_supervisor_events(
                        &mut transaction,
                        SupervisorEventKind::ResultReady,
                        task_id,
                        None,
                        &now,
                    )
                    .await?;
                }
                MessageKind::Question | MessageKind::Notification => {}
            }
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
        let message = sqlx::query("SELECT m.task_id, m.recipient_id, m.kind, (SELECT target_session_id FROM delivery_attempts d WHERE d.message_id = m.id ORDER BY d.attempt_number DESC LIMIT 1) AS target_session_id FROM messages m WHERE m.id = ?")
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
        if message.get::<Option<&str>, _>("target_session_id")
            != Some(actor.session_id.to_string().as_str())
        {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "native delivery was not addressed to this exact Harness Session",
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
                    revoke_unbound_result_dependencies(&mut transaction, task_id, &now).await?;
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
        let task = sqlx::query("SELECT worker_id, state FROM tasks WHERE id = ?")
            .bind(manifest.task_id.to_string())
            .fetch_optional(&self.pool)
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
        let mut attachment_ids = manifest.attachments.clone();
        attachment_ids.extend(manifest.verification.iter().map(|entry| entry.evidence));
        for attachment_id in attachment_ids {
            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attachments WHERE id = ?")
                .bind(attachment_id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
            if exists == 0 {
                return Err(CoordinatorError::new(
                    ErrorCategory::NotFound,
                    "Result references an unknown Attachment",
                ));
            }
        }
        let snapshot_attachment = self.admit_dependency_result_snapshot(&manifest).await?;
        // Reserve the write lock before attachment and Task reads. A deferred SQLite
        // transaction can lose its later read-to-write upgrade to concurrent Host events.
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(CoordinatorError::storage)?;
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
        sqlx::query("INSERT INTO result_dependency_snapshots (task_id, result_revision, attachment_id) VALUES (?, ?, ?)")
            .bind(manifest.task_id.to_string())
            .bind(revision_i64)
            .bind(snapshot_attachment.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
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
        if next == TaskState::Failed {
            cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
        }
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
                insert_supervisor_event(
                    &mut transaction,
                    SupervisorEventKind::WorktreeHoldCreated,
                    Some(hold_task_id),
                    None,
                    None,
                    &format!("hold:{hold_task_id}:turn_settlement"),
                    "Repository Worktree Hold created while settling the Worker turn",
                    &[],
                    DeliveryIntent::FollowUp,
                    &now,
                )
                .await?;
            }
        }
        if next == TaskState::Reviewing {
            if !out_of_scope {
                satisfy_downstream_dependencies(
                    &mut transaction,
                    task_id,
                    DependencyCondition::ResultReady,
                    u32::try_from(revision).map_err(CoordinatorError::storage)?,
                    &now,
                )
                .await?;
            }
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
            let manifest_json = result_manifest.expect("reviewing requires a Result");
            let manifest: ResultManifestV1 =
                serde_json::from_str(&manifest_json).map_err(CoordinatorError::storage)?;
            sqlx::query("INSERT INTO messages (id, task_id, sender_id, recipient_id, kind, body_json, delivery_intent, created_sequence, created_at) VALUES (?, ?, ?, ?, 'result', ?, 'follow_up', ?, ?)")
                .bind(message_id.to_string())
                .bind(task_id.to_string())
                .bind(actor.id.as_str())
                .bind(&supervisor_id)
                .bind(&manifest_json)
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
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::ResultReady,
                Some(task_id),
                Some(u32::try_from(revision).map_err(CoordinatorError::storage)?),
                Some(message_id),
                &format!("result:{task_id}:{revision}"),
                &manifest.summary,
                &manifest.attachments,
                DeliveryIntent::FollowUp,
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

    async fn admit_dependency_result_snapshot(
        &self,
        manifest: &ResultManifestV1,
    ) -> Result<crate::contract::AttachmentId, CoordinatorError> {
        let temporary_dir = self.state_dir.join("tmp");
        tokio::fs::create_dir_all(&temporary_dir)
            .await
            .map_err(CoordinatorError::storage)?;
        let temporary = temporary_dir.join(format!(
            "dependency-result-{}-{}.tmp",
            manifest.task_id,
            Uuid::now_v7()
        ));
        let snapshot = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "dependency_task_id": manifest.task_id,
            "result": manifest,
        }))
        .map_err(CoordinatorError::storage)?;
        tokio::fs::write(&temporary, snapshot)
            .await
            .map_err(CoordinatorError::storage)?;
        let outcome = self
            .admit_attachment(
                temporary.clone(),
                "application/json".to_owned(),
                format!("task-{}-result.json", manifest.task_id),
            )
            .await;
        let _ = tokio::fs::remove_file(temporary).await;
        match outcome? {
            CommandOutcome::AttachmentAdmitted { attachment } => Ok(attachment.id),
            _ => Err(CoordinatorError::new(
                ErrorCategory::StorageFailure,
                "generated Result snapshot admission returned an unexpected outcome",
            )),
        }
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
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let task = sqlx::query("SELECT submission_json, state FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::NotFound, "Task does not exist"))?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
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
        let now = timestamp();
        let created = sqlx::query("INSERT OR IGNORE INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(WorktreeHoldId::new().to_string())
            .bind(submission.repository.root.to_string_lossy().as_ref())
            .bind(hold_task_id.to_string())
            .bind(reason)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if created == 1 {
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::WorktreeHoldCreated,
                Some(hold_task_id),
                None,
                None,
                &format!("hold:{hold_task_id}:{reason}"),
                &format!("Repository Worktree Hold created: {reason}"),
                &[],
                DeliveryIntent::FollowUp,
                &now,
            )
            .await?;
        }
        if task.get::<&str, _>("state") == TaskState::Reviewing.as_str() {
            revoke_unbound_result_dependencies(&mut transaction, task_id, &now).await?;
        }
        transaction
            .commit()
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
        cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
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
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::WorktreeHoldCreated,
                Some(task_id),
                None,
                None,
                &format!("hold:{task_id}:repository_observation_failed"),
                "Repository Worktree Hold created after observation failure",
                &[],
                DeliveryIntent::FollowUp,
                &now,
            )
            .await?;
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
        satisfy_downstream_dependencies(
            &mut transaction,
            task_id,
            DependencyCondition::Approved,
            result_revision,
            &now,
        )
        .await?;
        process_matching_supervisor_events(
            &mut transaction,
            SupervisorEventKind::ResultReady,
            task_id,
            Some(result_revision),
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
            cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
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

    #[expect(
        clippy::too_many_lines,
        reason = "cancellation settlement atomically records Task, lease, Hold, and Supervisor event state"
    )]
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
        cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
        let submission: TaskSubmissionV1 =
            serde_json::from_str(task.get("submission_json")).map_err(CoordinatorError::storage)?;
        if submission.repository.access == RepositoryAccess::Mutating {
            let reason = if succeeded {
                "cancelled_after_dispatch"
            } else {
                "cancellation_failed"
            };
            let repository_key = submission.repository.root.to_string_lossy().into_owned();
            sqlx::query("INSERT INTO worktree_holds (id, repository_key, task_id, reason, created_at) VALUES (?, ?, ?, ?, ?)")
                .bind(WorktreeHoldId::new().to_string())
                .bind(repository_key)
                .bind(task_id.to_string())
                .bind(reason)
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::WorktreeHoldCreated,
                Some(task_id),
                None,
                None,
                &format!("hold:{task_id}:{reason}"),
                &format!("Repository Worktree Hold created: {reason}"),
                &[],
                DeliveryIntent::FollowUp,
                &now,
            )
            .await?;
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

    #[expect(
        clippy::too_many_lines,
        reason = "ambiguous delivery, Session failure, Hold, and attention event settle atomically"
    )]
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
            .bind(serde_json::json!({"diagnostic": &diagnostic}).to_string())
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
        insert_supervisor_event(
            &mut transaction,
            SupervisorEventKind::DeliveryUnknown,
            Some(task_id),
            None,
            Some(message_id),
            &format!("delivery:{message_id}:unknown"),
            &diagnostic,
            &[],
            DeliveryIntent::FollowUp,
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
            insert_supervisor_event(
                &mut transaction,
                SupervisorEventKind::WorktreeHoldCreated,
                Some(task_id),
                None,
                None,
                &format!("hold:{task_id}:delivery_unknown"),
                "Repository Worktree Hold created after ambiguous native delivery",
                &[],
                DeliveryIntent::FollowUp,
                &now,
            )
            .await?;
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
        if next == TaskState::Cancelled {
            cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
        }
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
        } else {
            let revision: i64 =
                sqlx::query_scalar("SELECT result_revision FROM tasks WHERE id = ?")
                    .bind(task_id.to_string())
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            satisfy_downstream_dependencies(
                &mut transaction,
                task_id,
                DependencyCondition::ResultReady,
                u32::try_from(revision).map_err(CoordinatorError::storage)?,
                &now,
            )
            .await?;
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

    async fn deactivate_workspace(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let active_tasks: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE state IN ('queued','dispatching','working','waiting','reviewing','cancelling','delivery_unknown')",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        let holds: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worktree_holds WHERE cleared_at IS NULL")
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        let leases: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worktree_leases WHERE released_at IS NULL")
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        let workers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_sessions WHERE harness_tier = 'worker' AND ended_at IS NULL",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if active_tasks != 0 || holds != 0 || leases != 0 || workers != 0 {
            return Err(CoordinatorError::new(
                ErrorCategory::Conflict,
                "workspace deactivation requires no active Task, Hold, lease, or Worker Session",
            ));
        }
        let now = timestamp();
        let changed = sqlx::query(
            "UPDATE harness_sessions SET presence = 'stopped', activity = 'idle', ended_at = ?, last_seen_at = ? WHERE id = ? AND harness_tier = 'supervisor' AND ended_at IS NULL",
        )
        .bind(&now)
        .bind(&now)
        .bind(actor.session_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?
        .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Supervisor Session is not active",
            ));
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::WorkspaceDeactivated)
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
        if let Some(connection_id) = &actor.host_connection_id {
            sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = ? WHERE id = ? AND status = 'active'")
                .bind(&now)
                .bind(if clean { "Worker Host stopped cleanly" } else { "Worker Host stopped without clean provider shutdown" })
                .bind(connection_id)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
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
        let changed = sqlx::query("UPDATE harness_sessions SET presence = 'online', activity = 'idle', native_health = CASE WHEN native_health = 'ambiguous' THEN 'healthy' ELSE native_health END, tool_policy_digest = COALESCE(tool_policy_digest, profile_digest), effective_model = COALESCE(effective_model, (SELECT model FROM harnesses WHERE id = harness_sessions.harness_id)), last_seen_at = ? WHERE id = ? AND ended_at IS NULL AND presence = 'starting'")
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
        clippy::too_many_arguments,
        reason = "one immutable native compatibility evidence record"
    )]
    async fn record_host_compatibility(
        &self,
        actor: &AuthenticatedActor,
        resolved_executable: PathBuf,
        observed_version: String,
        native_session_id: Option<String>,
        native_thread_id: Option<String>,
        effective_model: Option<String>,
        safe_compaction: bool,
        evidence: HarnessCompatibilityEvidenceV1,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Worker {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only a Worker Host may record compatibility evidence",
            ));
        }
        if !resolved_executable.is_absolute()
            || observed_version.trim().is_empty()
            || observed_version.len() > 4096
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "compatibility evidence requires an absolute executable and bounded version",
            ));
        }
        if evidence.schema_version != SCHEMA_VERSION || evidence.successful_checks.is_empty() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "compatibility evidence requires schema version 1 and successful checks",
            ));
        }
        let evidence = serde_json::to_string(&evidence).map_err(CoordinatorError::storage)?;
        if evidence.len() > 65_536 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "compatibility evidence exceeds 64 KiB",
            ));
        }
        let changed = sqlx::query(
            "UPDATE harness_sessions SET resolved_executable = ?, observed_version = ?, native_session_id = ?, native_thread_id = ?, effective_model = ?, safe_compaction = ?, compatibility_evidence_json = ?, native_health = 'healthy', tool_policy_digest = profile_digest, last_seen_at = ? WHERE id = ? AND ended_at IS NULL AND presence = 'starting'",
        )
        .bind(resolved_executable.to_string_lossy().as_ref())
        .bind(observed_version)
        .bind(native_session_id)
        .bind(native_thread_id)
        .bind(effective_model)
        .bind(i64::from(safe_compaction))
        .bind(evidence)
        .bind(timestamp())
        .bind(actor.session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?
        .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "compatibility evidence requires a starting Worker Session",
            ));
        }
        Ok(CommandOutcome::HostCompatibilityRecorded)
    }

    async fn record_adapter_snapshot(
        &self,
        actor: &AuthenticatedActor,
        snapshot: AdapterSnapshot,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if snapshot
            .context_percent
            .is_some_and(|percent| !(0.0..=100.0).contains(&percent))
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "context_percent must be between zero and one hundred",
            ));
        }
        let now = timestamp();
        let context_tokens = snapshot
            .context_tokens
            .map(i64::try_from)
            .transpose()
            .map_err(CoordinatorError::storage)?;
        let context_window = snapshot
            .context_window
            .map(i64::try_from)
            .transpose()
            .map_err(CoordinatorError::storage)?;
        let changed = sqlx::query("UPDATE harness_sessions SET native_health = ?, context_tokens = ?, context_window = ?, context_percent = ?, compaction_count = ?, adapter_snapshot_json = ?, adapter_snapshot_at = ?, last_seen_at = ? WHERE id = ? AND ended_at IS NULL")
            .bind(native_session_health_as_str(snapshot.native_health))
            .bind(context_tokens)
            .bind(context_window)
            .bind(snapshot.context_percent)
            .bind(snapshot.compaction_count.map(i64::from))
            .bind(serde_json::to_string(&snapshot).map_err(CoordinatorError::storage)?)
            .bind(&now)
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Adapter snapshot requires a live Session",
            ));
        }
        Ok(CommandOutcome::AdapterSnapshotRecorded)
    }

    async fn record_supervisor_binding(
        &self,
        actor: &AuthenticatedActor,
        native_session_id: Option<String>,
        native_thread_id: Option<String>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Supervisor {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the managed Supervisor Host may bind a native Supervisor Session",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        mark_unsettled_supervisor_delivery_unknown(
            &mut transaction,
            "managed Supervisor reconnected before native acceptance was recorded",
            &now,
        )
        .await?;
        let changed = sqlx::query("UPDATE harness_sessions SET native_session_id = ?, native_thread_id = ?, presence = 'online', activity = 'idle', native_health = 'healthy', last_seen_at = ? WHERE id = ? AND ended_at IS NULL")
            .bind(native_session_id)
            .bind(native_thread_id)
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Supervisor binding requires a live Coordinator Session",
            ));
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorBound)
    }

    async fn record_supervisor_disconnected(
        &self,
        actor: &AuthenticatedActor,
        diagnostic: Option<String>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if actor.tier != HarnessTier::Supervisor {
            return Err(CoordinatorError::new(
                ErrorCategory::Forbidden,
                "only the managed Supervisor Host may report Supervisor disconnection",
            ));
        }
        if diagnostic
            .as_ref()
            .is_some_and(|value| value.len() > 16_384)
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Supervisor disconnection diagnostic exceeds 16 KiB",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        if let Some(connection_id) = &actor.host_connection_id {
            sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = ? WHERE id = ? AND status = 'active'")
                .bind(&now)
                .bind(diagnostic.as_deref().unwrap_or("managed Supervisor disconnected"))
                .bind(connection_id)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        mark_unsettled_supervisor_delivery_unknown(
            &mut transaction,
            diagnostic
                .as_deref()
                .unwrap_or("managed Supervisor disconnected during native dispatch"),
            &now,
        )
        .await?;
        let changed = sqlx::query("UPDATE harness_sessions SET presence = 'disconnected', activity = 'idle', native_health = 'ambiguous', adapter_snapshot_json = NULL, adapter_snapshot_at = NULL, last_seen_at = ? WHERE id = ? AND ended_at IS NULL")
            .bind(&now)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Supervisor disconnection requires a live Coordinator Session",
            ));
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorDisconnected)
    }

    async fn supervisor_events(&self) -> Result<Vec<SupervisorEventView>, CoordinatorError> {
        let rows = sqlx::query("SELECT id, kind, task_id, result_revision, source_message_id, summary, attachments_json, delivery_intent, state, created_at FROM supervisor_events ORDER BY created_sequence")
            .fetch_all(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        rows.iter().map(supervisor_event_view).collect()
    }

    async fn claim_next_supervisor_event(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let in_flight: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM supervisor_events WHERE state IN ('dispatching','accepted','unknown')",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if in_flight != 0 {
            return Ok(CommandOutcome::SupervisorEventClaimed { event: None });
        }
        let row = sqlx::query("SELECT id, kind, task_id, result_revision, source_message_id, summary, attachments_json, delivery_intent, state, created_at FROM supervisor_events WHERE state = 'pending' ORDER BY created_sequence LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let Some(row) = row else {
            return Ok(CommandOutcome::SupervisorEventClaimed { event: None });
        };
        let event = supervisor_event_view(&row)?;
        let now = timestamp();
        sqlx::query("UPDATE supervisor_events SET state = 'dispatching', updated_at = ? WHERE id = ? AND state = 'pending'")
            .bind(&now)
            .bind(event.id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let attempt_number: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM supervisor_event_attempts WHERE event_id = ?")
            .bind(event.id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT INTO supervisor_event_attempts (id, event_id, attempt_number, target_session_id, target_host_connection_id, state, provider_bytes_may_have_been_written, acceptance_evidence_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'dispatching', 0, '{}', ?, ?)")
            .bind(DeliveryAttemptId::new().to_string())
            .bind(event.id.to_string())
            .bind(attempt_number)
            .bind(actor.session_id.to_string())
            .bind(actor.host_connection_id.as_deref())
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventClaimed {
            event: Some(SupervisorEventView {
                state: SupervisorEventDeliveryState::Dispatching,
                ..event
            }),
        })
    }

    async fn accept_supervisor_event(
        &self,
        actor: &AuthenticatedActor,
        event_id: SupervisorEventId,
        native_correlation: String,
        native_turn_id: Option<String>,
        evidence: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if native_correlation.trim().is_empty() || evidence.trim().is_empty() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "native acceptance requires correlation and evidence",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        let changed = sqlx::query("UPDATE supervisor_events SET state = 'accepted', updated_at = ? WHERE id = ? AND state = 'dispatching'")
            .bind(&now)
            .bind(event_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Supervisor event must be dispatching",
            ));
        }
        sqlx::query("UPDATE supervisor_event_attempts SET state = 'accepted', native_correlation = ?, acceptance_evidence_json = ?, updated_at = ? WHERE event_id = ? AND target_session_id = ? AND state = 'dispatching'")
            .bind(native_correlation)
            .bind(serde_json::to_string(&serde_json::json!({"acceptance": evidence, "native_turn_id": native_turn_id})).map_err(CoordinatorError::storage)?)
            .bind(&now)
            .bind(event_id.to_string())
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: SupervisorEventDeliveryState::Accepted,
        })
    }

    async fn record_supervisor_event_presentation(
        &self,
        actor: &AuthenticatedActor,
        event_id: SupervisorEventId,
        phase: SupervisorPresentationPhase,
        native_turn_id: Option<String>,
        evidence: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if evidence.trim().is_empty() || evidence.len() > 16_384 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Supervisor presentation evidence must contain 1 to 16,384 bytes",
            ));
        }
        let phase_text = match phase {
            SupervisorPresentationPhase::Presented => "presented",
            SupervisorPresentationPhase::TurnStarted => "turn_started",
            SupervisorPresentationPhase::TurnCompleted => "turn_completed",
        };
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let attempt = sqlx::query("SELECT id FROM supervisor_event_attempts WHERE event_id = ? AND target_session_id = ? AND state = 'accepted' ORDER BY attempt_number DESC LIMIT 1")
            .bind(event_id.to_string())
            .bind(actor.session_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::InvalidState, "presentation evidence requires an accepted event attempt"))?;
        let attempt_id = attempt.get::<&str, _>("id");
        let observation_key = format!(
            "{}:{attempt_id}:{phase_text}:{}",
            event_id,
            native_turn_id.as_deref().unwrap_or("none")
        );
        let native = sqlx::query("SELECT native_session_id, native_thread_id FROM harness_sessions WHERE id = ? AND ended_at IS NULL")
            .bind(actor.session_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        sqlx::query("INSERT OR IGNORE INTO supervisor_event_observations (id, observation_key, event_id, attempt_id, observation_kind, native_session_id, native_thread_id, native_turn_id, evidence_json, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(Uuid::now_v7().to_string())
            .bind(observation_key)
            .bind(event_id.to_string())
            .bind(attempt_id)
            .bind(phase_text)
            .bind(native.get::<Option<&str>, _>("native_session_id"))
            .bind(native.get::<Option<&str>, _>("native_thread_id"))
            .bind(native_turn_id)
            .bind(serde_json::to_string(&json_evidence(&evidence)).map_err(CoordinatorError::storage)?)
            .bind(timestamp())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: SupervisorEventDeliveryState::Accepted,
        })
    }

    async fn mark_supervisor_event_unknown(
        &self,
        event_id: SupervisorEventId,
        diagnostic: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if diagnostic.trim().is_empty() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "ambiguous injection requires a diagnostic",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        let attempt = sqlx::query("SELECT id, target_session_id, state FROM supervisor_event_attempts WHERE event_id = ? AND state IN ('dispatching','accepted') ORDER BY attempt_number DESC LIMIT 1")
            .bind(event_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::InvalidState, "event has no unsettled delivery attempt"))?;
        let attempt_id = attempt.get::<&str, _>("id").to_owned();
        let attempt_was_accepted = attempt.get::<&str, _>("state") == "accepted";
        let diagnostic_json = serde_json::to_string(&json_evidence(&diagnostic))
            .map_err(CoordinatorError::storage)?;
        let changed = sqlx::query("UPDATE supervisor_events SET state = 'unknown', updated_at = ? WHERE id = ? AND state IN ('dispatching','accepted')")
            .bind(&now)
            .bind(event_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "only a dispatching or accepted Supervisor event may become Unknown",
            ));
        }
        sqlx::query("UPDATE supervisor_event_attempts SET state = 'unknown', provider_bytes_may_have_been_written = 1, ambiguity_evidence_json = ?, updated_at = ? WHERE id = ? AND state IN ('dispatching','accepted')")
            .bind(&diagnostic_json)
            .bind(&now)
            .bind(&attempt_id)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if attempt_was_accepted {
            let target_session_id = attempt.get::<&str, _>("target_session_id");
            let native = sqlx::query(
                "SELECT native_session_id, native_thread_id FROM harness_sessions WHERE id = ?",
            )
            .bind(target_session_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
            sqlx::query("INSERT OR IGNORE INTO supervisor_event_observations (id, observation_key, event_id, attempt_id, observation_kind, native_session_id, native_thread_id, native_turn_id, evidence_json, observed_at) SELECT ?, ?, ?, ?, 'presentation_timeout', ?, ?, NULL, ?, ? WHERE NOT EXISTS (SELECT 1 FROM supervisor_event_observations WHERE attempt_id = ? AND observation_kind = 'presented')")
                .bind(Uuid::now_v7().to_string())
                .bind(format!("{event_id}:{attempt_id}:presentation_timeout:none"))
                .bind(event_id.to_string())
                .bind(&attempt_id)
                .bind(native.get::<Option<&str>, _>("native_session_id"))
                .bind(native.get::<Option<&str>, _>("native_thread_id"))
                .bind(&diagnostic_json)
                .bind(&now)
                .bind(&attempt_id)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: SupervisorEventDeliveryState::Unknown,
        })
    }

    async fn release_supervisor_event(
        &self,
        event_id: SupervisorEventId,
        diagnostic: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if diagnostic.trim().is_empty() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "retry-safe release requires a diagnostic",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        let changed = sqlx::query("UPDATE supervisor_events SET state = 'pending', updated_at = ? WHERE id = ? AND state = 'dispatching'")
            .bind(&now)
            .bind(event_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "only a dispatching event may be released",
            ));
        }
        sqlx::query("UPDATE supervisor_event_attempts SET state = 'cancelled', acceptance_evidence_json = ?, updated_at = ? WHERE event_id = ? AND state = 'dispatching' AND provider_bytes_may_have_been_written = 0")
            .bind(serde_json::to_string(&json_evidence(&diagnostic)).map_err(CoordinatorError::storage)?)
            .bind(&now)
            .bind(event_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: SupervisorEventDeliveryState::Pending,
        })
    }

    async fn acknowledge_supervisor_events(
        &self,
        actor: &AuthenticatedActor,
        event_ids: Vec<SupervisorEventId>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if event_ids.is_empty() || event_ids.len() > 32 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "event acknowledgement requires 1 to 32 IDs",
            ));
        }
        let unique = event_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if unique.len() != event_ids.len() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "event acknowledgement contains duplicate IDs",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let now = timestamp();
        for event_id in &event_ids {
            let source_message_id: Option<String> = sqlx::query_scalar("SELECT source_message_id FROM supervisor_events WHERE id = ? AND state IN ('pending','accepted')")
                .bind(event_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .flatten();
            let changed = sqlx::query("UPDATE supervisor_events SET state = 'processed', processed_at = ?, updated_at = ? WHERE id = ? AND state IN ('pending','accepted')")
                .bind(&now)
                .bind(&now)
                .bind(event_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .rows_affected();
            if changed != 1 {
                return Err(CoordinatorError::new(
                    ErrorCategory::InvalidState,
                    "event is not safely acknowledgeable",
                ));
            }
            if let Some(message_id) = source_message_id {
                sqlx::query("INSERT OR IGNORE INTO inbox_reads (harness_id, message_id, read_at) VALUES (?, ?, ?)")
                    .bind(actor.id.as_str())
                    .bind(message_id)
                    .bind(&now)
                    .execute(&mut *transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        let event_id = event_ids[0];
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: SupervisorEventDeliveryState::Processed,
        })
    }

    async fn reconcile_supervisor_event(
        &self,
        event_id: SupervisorEventId,
        resolution: SupervisorEventResolution,
        audit_note: String,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if audit_note.trim().is_empty() || audit_note.len() > 4096 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "reconciliation audit note must contain 1 to 4096 bytes",
            ));
        }
        let (state, processed_at, resolution_text) = match resolution {
            SupervisorEventResolution::Retry => ("pending", None, "retry"),
            SupervisorEventResolution::Processed => ("processed", Some(timestamp()), "processed"),
            SupervisorEventResolution::Cancel => ("cancelled", None, "cancel"),
        };
        let now = timestamp();
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        let attempt_id: Option<String> = sqlx::query_scalar("SELECT id FROM supervisor_event_attempts WHERE event_id = ? ORDER BY attempt_number DESC LIMIT 1")
            .bind(event_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let changed = sqlx::query("UPDATE supervisor_events SET state = ?, processed_at = ?, updated_at = ? WHERE id = ? AND state = 'unknown'")
            .bind(state)
            .bind(processed_at)
            .bind(&now)
            .bind(event_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidState,
                "only an Unknown Supervisor event may be reconciled",
            ));
        }
        sqlx::query("INSERT INTO supervisor_event_reconciliations (id, event_id, attempt_id, resolution, audit_note, created_at) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(Uuid::now_v7().to_string())
            .bind(event_id.to_string())
            .bind(attempt_id)
            .bind(resolution_text)
            .bind(audit_note)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::SupervisorEventUpdated {
            event_id,
            state: supervisor_event_state_from_str(state)?,
        })
    }

    async fn watch_task_graph(
        &self,
        actor: &AuthenticatedActor,
        root_task_ids: Vec<TaskId>,
        request_key: Option<String>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if root_task_ids.is_empty() || root_task_ids.len() > 32 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Task graph watch requires 1 to 32 root Tasks",
            ));
        }
        let unique = root_task_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if unique.len() != root_task_ids.len() {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Task graph watch contains duplicate roots",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        for task_id in &root_task_ids {
            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE id = ?")
                .bind(task_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            if exists != 1 {
                return Err(CoordinatorError::new(
                    ErrorCategory::NotFound,
                    format!("root Task {task_id} does not exist"),
                ));
            }
        }
        let watch_id = TaskGraphWatchId::new();
        let now = timestamp();
        sqlx::query("INSERT INTO task_graph_watches (id, supervisor_id, request_key, created_at) VALUES (?, ?, ?, ?)")
            .bind(watch_id.to_string())
            .bind(actor.id.as_str())
            .bind(request_key)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(|error| CoordinatorError::new(ErrorCategory::Conflict, error.to_string()))?;
        for task_id in root_task_ids {
            sqlx::query("INSERT INTO task_graph_watch_roots (watch_id, task_id) VALUES (?, ?)")
                .bind(watch_id.to_string())
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        complete_task_graph_watches(&mut transaction, &now).await?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::TaskGraphWatchRegistered { watch_id })
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
        let active = sqlx::query("SELECT id, state, submission_json FROM tasks WHERE worker_id = ? AND state IN ('dispatching','working','waiting','reviewing','cancelling','delivery_unknown') ORDER BY created_sequence LIMIT 1")
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
        if let Some(connection_id) = &actor.host_connection_id {
            sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = ? WHERE id = ? AND status = 'active'")
                .bind(&now)
                .bind(&diagnostic)
                .bind(connection_id)
                .execute(&mut *transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        if let Some((task_id, state, submission)) = active {
            if !matches!(state, TaskState::DeliveryUnknown | TaskState::Reviewing) {
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
                    cascade_failed_dependencies(&mut transaction, task_id, &now).await?;
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
                insert_supervisor_event(
                    &mut transaction,
                    SupervisorEventKind::WorktreeHoldCreated,
                    Some(task_id),
                    None,
                    None,
                    &format!("hold:{task_id}:worker_host_failed"),
                    "Repository Worktree Hold created after Worker Host failure",
                    &[],
                    DeliveryIntent::FollowUp,
                    &now,
                )
                .await?;
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

    async fn bind_host_connection(
        &self,
        actor: &AuthenticatedActor,
        instance_id: String,
        lease_seconds: u32,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if instance_id.trim().is_empty() || instance_id.len() > 256 {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Host instance ID must contain 1 to 256 bytes",
            ));
        }
        if !(1..=300).contains(&lease_seconds) {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Host lease must be between 1 and 300 seconds",
            ));
        }
        let connection_id = Uuid::now_v7().to_string();
        let capability = HostConnectionCapability::generate();
        let now = Utc::now();
        let now_text = now.to_rfc3339_opts(SecondsFormat::Micros, true);
        let expires_at = (now + chrono::Duration::seconds(i64::from(lease_seconds)))
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        let mut transaction = self.pool.begin().await.map_err(CoordinatorError::storage)?;
        sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = 'superseded by a newer Host generation' WHERE session_id = ? AND status = 'active'")
            .bind(&now_text)
            .bind(actor.session_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        let generation: i64 = sqlx::query_scalar(
            "UPDATE harness_sessions SET connection_generation = connection_generation + 1, last_seen_at = ? WHERE id = ? AND ended_at IS NULL RETURNING connection_generation",
        )
        .bind(&now_text)
        .bind(actor.session_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(CoordinatorError::storage)?
        .ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::InvalidState,
                "Host connection requires a live Coordinator Session",
            )
        })?;
        sqlx::query("INSERT INTO host_connections (id, session_id, generation, instance_id, capability_hash, lease_seconds, status, bound_at, last_heartbeat_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)")
            .bind(&connection_id)
            .bind(actor.session_id.to_string())
            .bind(generation)
            .bind(instance_id)
            .bind(capability.digest())
            .bind(i64::from(lease_seconds))
            .bind(&now_text)
            .bind(&now_text)
            .bind(&expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        transaction
            .commit()
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::HostConnectionBound {
            connection_id,
            generation: u64::try_from(generation).map_err(CoordinatorError::storage)?,
            capability,
            expires_at,
        })
    }

    async fn renew_host_connection(
        &self,
        actor: &AuthenticatedActor,
    ) -> Result<CommandOutcome, CoordinatorError> {
        let connection_id = actor.host_connection_id.as_deref().ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::Forbidden, "a Host connection is required")
        })?;
        let lease_seconds: i64 = sqlx::query_scalar(
            "SELECT lease_seconds FROM host_connections WHERE id = ? AND status = 'active'",
        )
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?
        .ok_or_else(|| {
            CoordinatorError::new(
                ErrorCategory::Unauthenticated,
                "Host connection has expired",
            )
        })?;
        let now = Utc::now();
        let now_text = now.to_rfc3339_opts(SecondsFormat::Micros, true);
        let expires_at = (now + chrono::Duration::seconds(lease_seconds))
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        let changed = sqlx::query("UPDATE host_connections SET last_heartbeat_at = ?, expires_at = ? WHERE id = ? AND status = 'active'")
            .bind(&now_text)
            .bind(&expires_at)
            .bind(connection_id)
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::Unauthenticated,
                "Host connection has expired",
            ));
        }
        sqlx::query(
            "UPDATE harness_sessions SET last_seen_at = ? WHERE id = ? AND ended_at IS NULL",
        )
        .bind(&now_text)
        .bind(actor.session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::HostConnectionRenewed { expires_at })
    }

    async fn disconnect_host_connection(
        &self,
        actor: &AuthenticatedActor,
        diagnostic: Option<String>,
    ) -> Result<CommandOutcome, CoordinatorError> {
        if diagnostic
            .as_ref()
            .is_some_and(|value| value.len() > 16_384)
        {
            return Err(CoordinatorError::new(
                ErrorCategory::InvalidInput,
                "Host disconnect diagnostic exceeds 16 KiB",
            ));
        }
        let connection_id = actor.host_connection_id.as_deref().ok_or_else(|| {
            CoordinatorError::new(ErrorCategory::Forbidden, "a Host connection is required")
        })?;
        let now = timestamp();
        let changed = sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = ? WHERE id = ? AND status = 'active'")
            .bind(&now)
            .bind(diagnostic)
            .bind(connection_id)
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed != 1 {
            return Err(CoordinatorError::new(
                ErrorCategory::Unauthenticated,
                "Host connection is no longer active",
            ));
        }
        Ok(CommandOutcome::HostConnectionDisconnected)
    }

    async fn reap_stale_host_connections(&self) -> Result<CommandOutcome, CoordinatorError> {
        let now = timestamp();
        let stale = sqlx::query("SELECT c.id AS connection_id, c.status, s.id AS session_id, h.id AS harness_id, h.tier FROM host_connections c JOIN harness_sessions s ON s.id = c.session_id JOIN harnesses h ON h.id = s.harness_id WHERE ((c.status = 'active' AND c.expires_at <= ?) OR (c.status = 'disconnected' AND c.disconnect_reason = 'presence lease expired; settlement pending')) AND c.generation = s.connection_generation AND s.ended_at IS NULL ORDER BY c.expires_at")
            .bind(&now)
            .fetch_all(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        let mut changed = 0_u32;
        for row in stale {
            let connection_id = row.get::<&str, _>("connection_id");
            if row.get::<&str, _>("status") == "active" {
                let claimed = sqlx::query("UPDATE host_connections SET status = 'disconnected', disconnected_at = ?, disconnect_reason = 'presence lease expired; settlement pending' WHERE id = ? AND status = 'active' AND expires_at <= ?")
                    .bind(&now)
                    .bind(connection_id)
                    .bind(&now)
                    .execute(&self.pool)
                    .await
                    .map_err(CoordinatorError::storage)?
                    .rows_affected();
                if claimed != 1 {
                    continue;
                }
            }
            changed = changed.saturating_add(1);
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
            let actor = AuthenticatedActor {
                id: HarnessId::from_str(row.get("harness_id"))
                    .map_err(CoordinatorError::storage)?,
                tier,
                session_id: parse_uuid_id(row.get("session_id"))?,
                host_connection_id: Some(connection_id.to_owned()),
            };
            if tier == HarnessTier::Supervisor {
                self.record_supervisor_disconnected(
                    &actor,
                    Some("managed Supervisor presence lease expired".to_owned()),
                )
                .await?;
            } else {
                let dispatching_message: Option<String> = sqlx::query_scalar("SELECT m.id FROM tasks t JOIN messages m ON m.task_id = t.id AND m.kind = 'task' WHERE t.worker_id = ? AND t.state = 'dispatching' ORDER BY t.created_sequence, m.created_sequence LIMIT 1")
                    .bind(actor.id.as_str())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(CoordinatorError::storage)?;
                if let Some(message_id) = dispatching_message {
                    self.mark_delivery_unknown(
                        &actor,
                        parse_uuid_id(&message_id)?,
                        "Worker Host presence expired during native Task dispatch".to_owned(),
                    )
                    .await?;
                } else {
                    self.record_host_failed(
                        &actor,
                        "Worker Host presence lease expired without a clean disconnect".to_owned(),
                    )
                    .await?;
                }
            }
            sqlx::query("UPDATE host_connections SET status = 'expired', disconnected_at = ?, disconnect_reason = 'presence lease expired; settlement complete' WHERE id = ? AND status = 'disconnected' AND disconnect_reason = 'presence lease expired; settlement pending'")
                .bind(&now)
                .bind(connection_id)
                .execute(&self.pool)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        sqlx::query("UPDATE host_connections SET status = 'expired', disconnected_at = COALESCE(disconnected_at, ?), disconnect_reason = 'presence lease settlement already completed or superseded' WHERE status = 'disconnected' AND disconnect_reason = 'presence lease expired; settlement pending' AND (session_id IN (SELECT id FROM harness_sessions WHERE ended_at IS NOT NULL) OR generation <> (SELECT connection_generation FROM harness_sessions WHERE id = host_connections.session_id))")
            .bind(&now)
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?;
        Ok(CommandOutcome::StaleHostConnectionsReaped { count: changed })
    }

    async fn prepare_supervisor_reconnect(&self) -> Result<CommandOutcome, CoordinatorError> {
        let now = Utc::now();
        let retry_before =
            (now - chrono::Duration::seconds(45)).to_rfc3339_opts(SecondsFormat::Micros, true);
        let now = now.to_rfc3339_opts(SecondsFormat::Micros, true);
        let changed = sqlx::query("UPDATE harness_sessions SET presence = 'reconnecting', last_seen_at = ? WHERE harness_tier = 'supervisor' AND ended_at IS NULL AND (presence = 'disconnected' OR (presence = 'reconnecting' AND last_seen_at <= ?))")
            .bind(&now)
            .bind(retry_before)
            .execute(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        Ok(CommandOutcome::SupervisorReconnectPrepared {
            claimed: changed == 1,
        })
    }

    async fn authenticate(
        &self,
        capability: &SessionCapability,
    ) -> Result<AuthenticatedActor, CoordinatorError> {
        let row = sqlx::query(
            "SELECT h.id, h.tier, s.id AS session_id FROM harness_sessions s JOIN harnesses h ON h.id = s.harness_id WHERE s.capability_hash = ? AND s.ended_at IS NULL AND s.presence IN ('starting', 'online', 'disconnected', 'reconnecting', 'offline')",
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
            host_connection_id: None,
        })
    }

    async fn authenticate_host(
        &self,
        capability: &HostConnectionCapability,
    ) -> Result<AuthenticatedActor, CoordinatorError> {
        let row = sqlx::query("SELECT h.id, h.tier, s.id AS session_id, c.id AS connection_id FROM host_connections c JOIN harness_sessions s ON s.id = c.session_id JOIN harnesses h ON h.id = s.harness_id WHERE c.capability_hash = ? AND c.status = 'active' AND c.expires_at > ? AND c.generation = s.connection_generation AND s.ended_at IS NULL")
            .bind(capability.digest())
            .bind(timestamp())
            .fetch_optional(&self.pool)
            .await
            .map_err(CoordinatorError::storage)?
            .ok_or_else(|| CoordinatorError::new(ErrorCategory::Unauthenticated, "Host connection capability is invalid, expired, or superseded"))?;
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
        Ok(AuthenticatedActor {
            id: HarnessId::from_str(row.get("id")).map_err(CoordinatorError::storage)?,
            tier,
            session_id: parse_uuid_id(row.get("session_id"))?,
            host_connection_id: Some(row.get::<&str, _>("connection_id").to_owned()),
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
    host_connection_id: Option<String>,
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
            actor.id == *worker_id && recipient_tier == "supervisor" && state == TaskState::Working
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
    if to == TaskState::Failed {
        insert_supervisor_event(
            transaction,
            SupervisorEventKind::TaskFailed,
            Some(task_id),
            None,
            None,
            &format!("task:{task_id}:failed"),
            "Worker Task failed and requires Supervisor direction",
            &[],
            DeliveryIntent::FollowUp,
            now,
        )
        .await?;
    }
    complete_task_graph_watches(transaction, now).await
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

impl UuidIdentity for crate::contract::AttachmentId {
    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl UuidIdentity for SupervisorEventId {
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
        task_role: task_role_from_str(row.get("task_role"))?,
        requested_session_policy: session_reuse_policy_from_str(row.get("session_reuse_policy"))?,
        effective_session_policy: row
            .get::<Option<&str>, _>("effective_policy")
            .map(session_reuse_policy_from_str)
            .transpose()?,
        harness_session_id: row
            .get::<Option<&str>, _>("harness_session_id")
            .map(parse_uuid_id)
            .transpose()?,
        session_reused: row.get::<Option<i64>, _>("reused").map(|value| value != 0),
        session_decision_reason: row
            .get::<Option<&str>, _>("decision_reason")
            .map(str::to_owned),
        context_percent: row
            .get::<Option<f64>, _>("context_percent")
            .map(|value| format!("{:.0}", value.clamp(0.0, 100.0))),
    })
}

fn task_dependency_view_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<TaskDependencyView, CoordinatorError> {
    let condition = match row.get::<&str, _>("condition") {
        "result_ready" => DependencyCondition::ResultReady,
        "approved" => DependencyCondition::Approved,
        value => {
            return Err(CoordinatorError::new(
                ErrorCategory::StorageFailure,
                format!("unknown dependency condition `{value}`"),
            ));
        }
    };
    let failure_policy = match row.get::<&str, _>("failure_policy") {
        "cancel" => DependencyFailurePolicy::Cancel,
        "keep_blocked" => DependencyFailurePolicy::KeepBlocked,
        value => {
            return Err(CoordinatorError::new(
                ErrorCategory::StorageFailure,
                format!("unknown dependency failure policy `{value}`"),
            ));
        }
    };
    let revision: Option<i64> = row.get("satisfied_by_result_revision");
    Ok(TaskDependencyView {
        task_id: parse_uuid_id(row.get("dependency_task_id"))?,
        condition,
        failure_policy,
        satisfied_by_result_revision: revision
            .map(u32::try_from)
            .transpose()
            .map_err(CoordinatorError::storage)?,
    })
}

fn dependency_condition_as_str(condition: DependencyCondition) -> &'static str {
    match condition {
        DependencyCondition::ResultReady => "result_ready",
        DependencyCondition::Approved => "approved",
    }
}

fn dependency_failure_policy_as_str(policy: DependencyFailurePolicy) -> &'static str {
    match policy {
        DependencyFailurePolicy::Cancel => "cancel",
        DependencyFailurePolicy::KeepBlocked => "keep_blocked",
    }
}

async fn reevaluate_new_task_dependencies(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: TaskId,
    now: &str,
) -> Result<(), CoordinatorError> {
    let edges = sqlx::query("SELECT d.dependency_task_id, d.condition, t.state, t.result_revision, t.approved_result_revision, t.submission_json FROM task_dependencies d JOIN tasks t ON t.id = d.dependency_task_id WHERE d.task_id = ?")
        .bind(task_id.to_string())
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for edge in edges {
        let condition = edge.get::<&str, _>("condition");
        let state = edge.get::<&str, _>("state");
        let revision = if condition == "approved" && state == "approved" {
            edge.get::<Option<i64>, _>("approved_result_revision")
        } else if condition == "result_ready" && matches!(state, "reviewing" | "approved") {
            let submission: TaskSubmissionV1 = serde_json::from_str(edge.get("submission_json"))
                .map_err(CoordinatorError::storage)?;
            let holds: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worktree_holds WHERE repository_key = ? AND cleared_at IS NULL")
                .bind(submission.repository.root.to_string_lossy().as_ref())
                .fetch_one(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            (holds == 0).then_some(edge.get::<i64, _>("result_revision"))
        } else {
            None
        };
        if let Some(revision) = revision {
            let snapshot_exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM result_dependency_snapshots WHERE task_id = ? AND result_revision = ?")
                .bind(edge.get::<&str, _>("dependency_task_id"))
                .bind(revision)
                .fetch_one(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            if snapshot_exists == 0 {
                continue;
            }
            sqlx::query("UPDATE task_dependencies SET satisfied_at = ?, satisfied_by_result_revision = ?, result_snapshot_attachment_id = (SELECT attachment_id FROM result_dependency_snapshots WHERE task_id = ? AND result_revision = ?) WHERE task_id = ? AND dependency_task_id = ?")
                .bind(now)
                .bind(revision)
                .bind(edge.get::<&str, _>("dependency_task_id"))
                .bind(revision)
                .bind(task_id.to_string())
                .bind(edge.get::<&str, _>("dependency_task_id"))
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
    }
    let unmet: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_dependencies WHERE task_id = ? AND satisfied_at IS NULL",
    )
    .bind(task_id.to_string())
    .fetch_one(&mut **transaction)
    .await
    .map_err(CoordinatorError::storage)?;
    let failed_dependency: Option<(String, String)> = sqlx::query_as(
        "SELECT d.dependency_task_id, d.failure_policy FROM task_dependencies d JOIN tasks upstream ON upstream.id = d.dependency_task_id WHERE d.task_id = ? AND upstream.state IN ('failed', 'cancelled') AND d.satisfied_at IS NULL LIMIT 1",
    )
    .bind(task_id.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(CoordinatorError::storage)?;
    if failed_dependency.is_some_and(|(_, policy)| policy == "cancel") {
        let changed = sqlx::query("UPDATE tasks SET state = 'cancelled', scheduling_state = 'blocked', updated_at = ? WHERE id = ? AND state = 'queued'")
            .bind(now)
            .bind(task_id.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed == 1 {
            sqlx::query("INSERT INTO task_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'queued', 'cancelled', '{\"reason\":\"dependency_already_failed\"}', ?)")
                .bind(task_id.to_string())
                .bind(now)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
        return Ok(());
    }
    if unmet == 0 {
        let changed = sqlx::query("UPDATE tasks SET scheduling_state = 'ready', updated_at = ? WHERE id = ? AND scheduling_state = 'blocked'")
            .bind(now)
            .bind(task_id.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed == 1 {
            sqlx::query("INSERT INTO task_scheduling_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'blocked', 'ready', '{\"reason\":\"already_satisfied\"}', ?)")
                .bind(task_id.to_string())
                .bind(now)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
    }
    Ok(())
}

async fn satisfy_downstream_dependencies(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    upstream_task_id: TaskId,
    condition: DependencyCondition,
    revision: u32,
    now: &str,
) -> Result<(), CoordinatorError> {
    sqlx::query("UPDATE task_dependencies SET satisfied_at = ?, satisfied_by_result_revision = ?, result_snapshot_attachment_id = (SELECT attachment_id FROM result_dependency_snapshots WHERE task_id = ? AND result_revision = ?) WHERE dependency_task_id = ? AND condition = ? AND satisfied_at IS NULL AND EXISTS (SELECT 1 FROM result_dependency_snapshots WHERE task_id = ? AND result_revision = ?)")
        .bind(now)
        .bind(i64::from(revision))
        .bind(upstream_task_id.to_string())
        .bind(i64::from(revision))
        .bind(upstream_task_id.to_string())
        .bind(dependency_condition_as_str(condition))
        .bind(upstream_task_id.to_string())
        .bind(i64::from(revision))
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    let dependent_ids = sqlx::query_scalar::<_, String>("SELECT DISTINCT task_id FROM task_dependencies WHERE dependency_task_id = ? AND condition = ?")
        .bind(upstream_task_id.to_string())
        .bind(dependency_condition_as_str(condition))
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for dependent_id in dependent_ids {
        let unmet: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_dependencies WHERE task_id = ? AND satisfied_at IS NULL",
        )
        .bind(&dependent_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
        if unmet == 0 {
            let changed = sqlx::query("UPDATE tasks SET scheduling_state = 'ready', updated_at = ? WHERE id = ? AND scheduling_state = 'blocked' AND state = 'queued'")
                .bind(now)
                .bind(&dependent_id)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?
                .rows_affected();
            if changed == 1 {
                sqlx::query("INSERT INTO task_scheduling_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'blocked', 'ready', '{}', ?)")
                    .bind(&dependent_id)
                    .bind(now)
                    .execute(&mut **transaction)
                    .await
                    .map_err(CoordinatorError::storage)?;
            }
        }
    }
    Ok(())
}

async fn revoke_unbound_result_dependencies(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    upstream_task_id: TaskId,
    now: &str,
) -> Result<(), CoordinatorError> {
    let dependent_ids = sqlx::query_scalar::<_, String>("SELECT task_id FROM task_dependencies WHERE dependency_task_id = ? AND condition = 'result_ready' AND satisfied_at IS NOT NULL AND bound_at IS NULL")
        .bind(upstream_task_id.to_string())
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    sqlx::query("UPDATE task_dependencies SET satisfied_at = NULL, satisfied_by_result_revision = NULL, result_snapshot_attachment_id = NULL WHERE dependency_task_id = ? AND condition = 'result_ready' AND bound_at IS NULL")
        .bind(upstream_task_id.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for dependent_id in dependent_ids {
        let changed = sqlx::query("UPDATE tasks SET scheduling_state = 'blocked', updated_at = ? WHERE id = ? AND scheduling_state = 'ready' AND state = 'queued'")
            .bind(now)
            .bind(&dependent_id)
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?
            .rows_affected();
        if changed == 1 {
            sqlx::query("INSERT INTO task_scheduling_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'ready', 'blocked', '{\"reason\":\"upstream_correction\"}', ?)")
                .bind(&dependent_id)
                .bind(now)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
    }
    Ok(())
}

async fn cascade_failed_dependencies(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    failed_task_id: TaskId,
    now: &str,
) -> Result<(), CoordinatorError> {
    let mut failed = VecDeque::from([failed_task_id.to_string()]);
    while let Some(upstream_id) = failed.pop_front() {
        let edges = sqlx::query("SELECT task_id, failure_policy FROM task_dependencies WHERE dependency_task_id = ? AND bound_at IS NULL")
            .bind(&upstream_id)
            .fetch_all(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        for edge in edges {
            let dependent_id = edge.get::<String, _>("task_id");
            sqlx::query("UPDATE task_dependencies SET satisfied_at = NULL, satisfied_by_result_revision = NULL, result_snapshot_attachment_id = NULL WHERE task_id = ? AND dependency_task_id = ? AND bound_at IS NULL")
                .bind(&dependent_id)
                .bind(&upstream_id)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
            if edge.get::<&str, _>("failure_policy") == "cancel" {
                let changed = sqlx::query("UPDATE tasks SET state = 'cancelled', scheduling_state = 'blocked', updated_at = ? WHERE id = ? AND state = 'queued'")
                    .bind(now)
                    .bind(&dependent_id)
                    .execute(&mut **transaction)
                    .await
                    .map_err(CoordinatorError::storage)?
                    .rows_affected();
                if changed == 1 {
                    sqlx::query("INSERT INTO task_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'queued', 'cancelled', '{\"reason\":\"dependency_failed\"}', ?)")
                        .bind(&dependent_id)
                        .bind(now)
                        .execute(&mut **transaction)
                        .await
                        .map_err(CoordinatorError::storage)?;
                    failed.push_back(dependent_id);
                }
            } else {
                let changed = sqlx::query("UPDATE tasks SET scheduling_state = 'blocked', updated_at = ? WHERE id = ? AND state = 'queued' AND scheduling_state = 'ready'")
                    .bind(now)
                    .bind(&dependent_id)
                    .execute(&mut **transaction)
                    .await
                    .map_err(CoordinatorError::storage)?
                    .rows_affected();
                if changed == 1 {
                    sqlx::query("INSERT INTO task_scheduling_transitions (task_id, from_state, to_state, evidence_json, created_at) VALUES (?, 'ready', 'blocked', '{\"reason\":\"dependency_failed\"}', ?)")
                        .bind(&dependent_id)
                        .bind(now)
                        .execute(&mut **transaction)
                        .await
                        .map_err(CoordinatorError::storage)?;
                }
            }
        }
    }
    complete_task_graph_watches(transaction, now).await
}

#[expect(
    clippy::too_many_arguments,
    reason = "durable event identity, source, delivery, and payload are one atomic record"
)]
async fn insert_supervisor_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    kind: SupervisorEventKind,
    task_id: Option<TaskId>,
    result_revision: Option<u32>,
    source_message_id: Option<MessageId>,
    source_key: &str,
    summary: &str,
    attachments: &[crate::contract::AttachmentId],
    delivery_intent: DeliveryIntent,
    now: &str,
) -> Result<SupervisorEventId, CoordinatorError> {
    let event_id = SupervisorEventId::new();
    let sequence = next_sequence(transaction, "supervisor_event").await?;
    sqlx::query("INSERT OR IGNORE INTO supervisor_events (id, kind, task_id, result_revision, source_message_id, source_key, summary, attachments_json, delivery_intent, state, created_sequence, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)")
        .bind(event_id.to_string())
        .bind(supervisor_event_kind_as_str(kind))
        .bind(task_id.map(|id| id.to_string()))
        .bind(result_revision.map(i64::from))
        .bind(source_message_id.map(|id| id.to_string()))
        .bind(source_key)
        .bind(summary)
        .bind(serde_json::to_string(attachments).map_err(CoordinatorError::storage)?)
        .bind(delivery_intent_name(delivery_intent))
        .bind(sequence)
        .bind(now)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(event_id)
}

async fn mark_unsettled_supervisor_delivery_unknown(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    diagnostic: &str,
    now: &str,
) -> Result<(), CoordinatorError> {
    let evidence =
        serde_json::to_string(&json_evidence(diagnostic)).map_err(CoordinatorError::storage)?;
    let accepted = sqlx::query("SELECT a.id, a.event_id, a.target_session_id, s.native_session_id, s.native_thread_id FROM supervisor_event_attempts a JOIN harness_sessions s ON s.id = a.target_session_id WHERE a.state = 'accepted'")
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for attempt in accepted {
        let attempt_id = attempt.get::<&str, _>("id");
        let event_id = attempt.get::<&str, _>("event_id");
        sqlx::query("INSERT OR IGNORE INTO supervisor_event_observations (id, observation_key, event_id, attempt_id, observation_kind, native_session_id, native_thread_id, native_turn_id, evidence_json, observed_at) SELECT ?, ?, ?, ?, 'presentation_timeout', ?, ?, NULL, ?, ? WHERE NOT EXISTS (SELECT 1 FROM supervisor_event_observations WHERE attempt_id = ? AND observation_kind = 'presented')")
            .bind(Uuid::now_v7().to_string())
            .bind(format!("{event_id}:{attempt_id}:presentation_timeout:none"))
            .bind(event_id)
            .bind(attempt_id)
            .bind(attempt.get::<Option<&str>, _>("native_session_id"))
            .bind(attempt.get::<Option<&str>, _>("native_thread_id"))
            .bind(&evidence)
            .bind(now)
            .bind(attempt_id)
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?;
    }
    sqlx::query("UPDATE supervisor_event_attempts SET state = 'unknown', provider_bytes_may_have_been_written = 1, ambiguity_evidence_json = ?, updated_at = ? WHERE state IN ('dispatching','accepted')")
        .bind(&evidence)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    sqlx::query("UPDATE supervisor_events SET state = 'unknown', updated_at = ? WHERE state IN ('dispatching','accepted')")
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    Ok(())
}

async fn process_matching_supervisor_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    kind: SupervisorEventKind,
    task_id: TaskId,
    result_revision: Option<u32>,
    now: &str,
) -> Result<(), CoordinatorError> {
    let rows = sqlx::query("SELECT id, source_message_id FROM supervisor_events WHERE kind = ? AND task_id = ? AND (? IS NULL OR result_revision = ?) AND state IN ('pending','accepted')")
        .bind(supervisor_event_kind_as_str(kind))
        .bind(task_id.to_string())
        .bind(result_revision.map(i64::from))
        .bind(result_revision.map(i64::from))
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for row in rows {
        sqlx::query("UPDATE supervisor_events SET state = 'processed', processed_at = ?, updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(now)
            .bind(row.get::<&str, _>("id"))
            .execute(&mut **transaction)
            .await
            .map_err(CoordinatorError::storage)?;
        if let Some(message_id) = row.get::<Option<&str>, _>("source_message_id") {
            sqlx::query("INSERT OR IGNORE INTO inbox_reads (harness_id, message_id, read_at) SELECT recipient_id, id, ? FROM messages WHERE id = ?")
                .bind(now)
                .bind(message_id)
                .execute(&mut **transaction)
                .await
                .map_err(CoordinatorError::storage)?;
        }
    }
    Ok(())
}

async fn complete_task_graph_watches(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    now: &str,
) -> Result<(), CoordinatorError> {
    let watches = sqlx::query_scalar::<_, String>("SELECT w.id FROM task_graph_watches w WHERE w.completed_at IS NULL AND NOT EXISTS (SELECT 1 FROM task_graph_watch_roots r JOIN tasks t ON t.id = r.task_id WHERE r.watch_id = w.id AND t.state NOT IN ('reviewing','approved','cancelled','failed')) ORDER BY w.created_at")
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    for watch_id in watches {
        let changed = sqlx::query(
            "UPDATE task_graph_watches SET completed_at = ? WHERE id = ? AND completed_at IS NULL",
        )
        .bind(now)
        .bind(&watch_id)
        .execute(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?
        .rows_affected();
        if changed == 1 {
            insert_supervisor_event(
                transaction,
                SupervisorEventKind::TaskGraphCompleted,
                None,
                None,
                None,
                &format!("graph:{watch_id}:completed"),
                "All watched root Tasks reached review or a terminal state",
                &[],
                DeliveryIntent::FollowUp,
                now,
            )
            .await?;
        }
    }
    Ok(())
}

fn supervisor_event_view(row: &SqliteRow) -> Result<SupervisorEventView, CoordinatorError> {
    let revision = row
        .get::<Option<i64>, _>("result_revision")
        .map(u32::try_from)
        .transpose()
        .map_err(CoordinatorError::storage)?;
    Ok(SupervisorEventView {
        id: parse_uuid_id(row.get("id"))?,
        kind: supervisor_event_kind_from_str(row.get("kind"))?,
        task_id: row
            .get::<Option<&str>, _>("task_id")
            .map(parse_uuid_id)
            .transpose()?,
        result_revision: revision,
        source_message_id: row
            .get::<Option<&str>, _>("source_message_id")
            .map(parse_uuid_id)
            .transpose()?,
        summary: row.get("summary"),
        attachments: serde_json::from_str(row.get("attachments_json"))
            .map_err(CoordinatorError::storage)?,
        delivery_intent: delivery_intent_from_str(row.get("delivery_intent"))?,
        state: supervisor_event_state_from_str(row.get("state"))?,
        created_at: row.get("created_at"),
    })
}

fn json_evidence(text: &str) -> Value {
    serde_json::json!({"text": text})
}

async fn validate_acyclic_task_graph(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), CoordinatorError> {
    let nodes = sqlx::query_scalar::<_, String>("SELECT id FROM tasks")
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    let edges = sqlx::query("SELECT task_id, dependency_task_id FROM task_dependencies")
        .fetch_all(&mut **transaction)
        .await
        .map_err(CoordinatorError::storage)?;
    let mut incoming = nodes
        .iter()
        .map(|node| (node.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut downstream = BTreeMap::<String, Vec<String>>::new();
    for edge in edges {
        let task_id = edge.get::<String, _>("task_id");
        let dependency_id = edge.get::<String, _>("dependency_task_id");
        *incoming.entry(task_id.clone()).or_default() += 1;
        downstream.entry(dependency_id).or_default().push(task_id);
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(node, count)| (*count == 0).then_some(node.clone()))
        .collect::<VecDeque<_>>();
    let mut visited = 0_usize;
    while let Some(node) = ready.pop_front() {
        visited += 1;
        for dependent in downstream.get(&node).into_iter().flatten() {
            let count = incoming.get_mut(dependent).ok_or_else(|| {
                CoordinatorError::new(ErrorCategory::StorageFailure, "dependency graph is corrupt")
            })?;
            *count -= 1;
            if *count == 0 {
                ready.push_back(dependent.clone());
            }
        }
    }
    if visited == incoming.len() {
        Ok(())
    } else {
        Err(CoordinatorError::new(
            ErrorCategory::InvalidInput,
            "Task dependencies contain a cycle",
        ))
    }
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

fn delivery_intent_from_str(value: &str) -> Result<DeliveryIntent, CoordinatorError> {
    match value {
        "follow_up" => Ok(DeliveryIntent::FollowUp),
        "steer" => Ok(DeliveryIntent::Steer),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown delivery intent `{value}`"),
        )),
    }
}

fn supervisor_event_kind_as_str(kind: SupervisorEventKind) -> &'static str {
    match kind {
        SupervisorEventKind::ResultReady => "result_ready",
        SupervisorEventKind::BlockingQuestion => "blocking_question",
        SupervisorEventKind::TaskFailed => "task_failed",
        SupervisorEventKind::DeliveryUnknown => "delivery_unknown",
        SupervisorEventKind::WorktreeHoldCreated => "worktree_hold_created",
        SupervisorEventKind::TaskGraphCompleted => "task_graph_completed",
        SupervisorEventKind::Notification => "notification",
    }
}

fn supervisor_event_kind_from_str(value: &str) -> Result<SupervisorEventKind, CoordinatorError> {
    match value {
        "result_ready" => Ok(SupervisorEventKind::ResultReady),
        "blocking_question" => Ok(SupervisorEventKind::BlockingQuestion),
        "task_failed" => Ok(SupervisorEventKind::TaskFailed),
        "delivery_unknown" => Ok(SupervisorEventKind::DeliveryUnknown),
        "worktree_hold_created" => Ok(SupervisorEventKind::WorktreeHoldCreated),
        "task_graph_completed" => Ok(SupervisorEventKind::TaskGraphCompleted),
        "notification" => Ok(SupervisorEventKind::Notification),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown Supervisor event kind `{value}`"),
        )),
    }
}

fn supervisor_event_state_from_str(
    value: &str,
) -> Result<SupervisorEventDeliveryState, CoordinatorError> {
    match value {
        "pending" => Ok(SupervisorEventDeliveryState::Pending),
        "dispatching" => Ok(SupervisorEventDeliveryState::Dispatching),
        "accepted" => Ok(SupervisorEventDeliveryState::Accepted),
        "processed" => Ok(SupervisorEventDeliveryState::Processed),
        "unknown" => Ok(SupervisorEventDeliveryState::Unknown),
        "cancelled" => Ok(SupervisorEventDeliveryState::Cancelled),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown Supervisor event state `{value}`"),
        )),
    }
}

fn task_role_as_str(role: TaskRole) -> &'static str {
    match role {
        TaskRole::Implementation => "implementation",
        TaskRole::Investigation => "investigation",
        TaskRole::Review => "review",
        TaskRole::Verification => "verification",
        TaskRole::Other => "other",
    }
}

fn task_role_from_str(value: &str) -> Result<TaskRole, CoordinatorError> {
    match value {
        "implementation" => Ok(TaskRole::Implementation),
        "investigation" => Ok(TaskRole::Investigation),
        "review" => Ok(TaskRole::Review),
        "verification" => Ok(TaskRole::Verification),
        "other" => Ok(TaskRole::Other),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown Task role `{value}`"),
        )),
    }
}

fn session_reuse_policy_as_str(policy: SessionReusePolicy) -> &'static str {
    match policy {
        SessionReusePolicy::Required => "required",
        SessionReusePolicy::Prefer => "prefer",
        SessionReusePolicy::Fresh => "fresh",
        SessionReusePolicy::Auto => "auto",
    }
}

fn session_reuse_policy_from_str(value: &str) -> Result<SessionReusePolicy, CoordinatorError> {
    match value {
        "required" => Ok(SessionReusePolicy::Required),
        "prefer" => Ok(SessionReusePolicy::Prefer),
        "fresh" => Ok(SessionReusePolicy::Fresh),
        "auto" => Ok(SessionReusePolicy::Auto),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown Session reuse policy `{value}`"),
        )),
    }
}

fn native_session_health_as_str(health: crate::contract::NativeSessionHealth) -> &'static str {
    match health {
        crate::contract::NativeSessionHealth::Healthy => "healthy",
        crate::contract::NativeSessionHealth::ContextPressure => "context_pressure",
        crate::contract::NativeSessionHealth::Compacted => "compacted",
        crate::contract::NativeSessionHealth::Ambiguous => "ambiguous",
        crate::contract::NativeSessionHealth::Failed => "failed",
    }
}

fn native_session_health_from_str(
    value: &str,
) -> Result<crate::contract::NativeSessionHealth, CoordinatorError> {
    match value {
        "healthy" => Ok(crate::contract::NativeSessionHealth::Healthy),
        "context_pressure" => Ok(crate::contract::NativeSessionHealth::ContextPressure),
        "compacted" => Ok(crate::contract::NativeSessionHealth::Compacted),
        "ambiguous" => Ok(crate::contract::NativeSessionHealth::Ambiguous),
        "failed" => Ok(crate::contract::NativeSessionHealth::Failed),
        _ => Err(CoordinatorError::new(
            ErrorCategory::StorageFailure,
            format!("unknown native Session health `{value}`"),
        )),
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
