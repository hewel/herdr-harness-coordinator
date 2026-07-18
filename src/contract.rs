//! Versioned public values shared by every Coordinator transport.

use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// The public contract version implemented by this crate.
pub const SCHEMA_VERSION: u32 = 1;

/// An error raised when a public value violates v1 semantics.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// A Harness ID was not a lowercase slug.
    #[error("invalid Harness ID `{0}`")]
    HarnessId(String),
    /// A field failed a contract rule.
    #[error("invalid `{field}`: {message}")]
    Field {
        /// Public field name.
        field: &'static str,
        /// Human-readable rule violation.
        message: String,
    },
}

/// Performs typed validation not expressible by JSON Schema alone.
pub trait Validate {
    /// Validates this value against the v1 contract.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when any field violates v1 semantics.
    fn validate(&self) -> Result<(), ValidationError>;
}

/// A durable, user-selected Harness address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HarnessId(String);

impl HarnessId {
    /// Returns the slug as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HarnessId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for HarnessId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        let valid = (1..=64).contains(&bytes.len())
            && bytes.first().is_some_and(u8::is_ascii_lowercase)
            && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            && !bytes.windows(2).any(|pair| pair == b"--");
        if !valid {
            return Err(ValidationError::HarnessId(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for HarnessId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<HarnessId> for String {
    fn from(value: HarnessId) -> Self {
        value.0
    }
}

macro_rules! uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generates a time-ordered `UUIDv7` identity.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(
    HarnessSessionId,
    "One live activation of a durable Harness."
);
uuid_id!(TaskId, "A bounded assignment to one Worker Harness.");
uuid_id!(MessageId, "A durable Bus Message identity.");
uuid_id!(DeliveryAttemptId, "One native delivery attempt.");
uuid_id!(AttachmentId, "An immutable admitted file identity.");
uuid_id!(
    RepositoryObservationId,
    "An immutable Git checkpoint identity."
);
uuid_id!(
    WorktreeHoldId,
    "A durable repository scheduling block identity."
);
uuid_id!(
    SupervisorEventId,
    "A durable Supervisor-attention event identity."
);
uuid_id!(
    TaskGraphWatchId,
    "A durable set of root Tasks awaiting attention."
);

/// Native Harness implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessKind {
    /// Oh My Pi RPC Harness.
    Omp,
    /// Codex App Server Harness.
    Codex,
}

/// Coordination authority assigned to a Harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessTier {
    /// Sole semantic authority.
    Supervisor,
    /// Bounded Task executor.
    Worker,
}

/// Durable Harness launch identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessDefinitionV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Immutable durable address.
    pub id: HarnessId,
    /// Native Harness implementation.
    pub kind: HarnessKind,
    /// Coordination tier.
    pub tier: HarnessTier,
    /// Canonical working directory.
    pub cwd: PathBuf,
    /// Explicit Worker launch profile.
    pub launch_profile: Option<String>,
    /// Recorded model selection.
    pub model: Option<String>,
}

/// Coordinator-owned, provider-native Worker launch selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessLaunchProfileV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Immutable profile identifier.
    pub id: HarnessId,
    /// Harness Kind accepted by this profile.
    pub kind: HarnessKind,
    /// Absolute provider executable path.
    pub executable: PathBuf,
    /// Provider-native isolated profile name.
    pub provider_profile: String,
    /// Model recorded for the Harness Session.
    pub model: Option<String>,
    /// Environment variable names explicitly inherited by the Worker.
    #[serde(default)]
    pub inherit_env: Vec<String>,
    /// OMP configuration overlays, applied in order.
    #[serde(default)]
    pub config_overlays: Vec<PathBuf>,
}

/// Coordinator-owned launch selection that resolves current executables at Session start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessLaunchProfileV2 {
    /// Must equal two.
    pub schema_version: u32,
    /// Immutable profile identifier.
    pub id: HarnessId,
    /// Harness Kind accepted by this profile.
    pub kind: HarnessKind,
    /// Absolute executable or one bare command resolved through `PATH`.
    pub executable: String,
    /// Optional native profile; absent uses the user's existing default.
    pub provider_profile: Option<String>,
    /// Explicit model selected by the user.
    pub model: String,
    /// Environment variable names explicitly inherited by the Worker.
    #[serde(default)]
    pub inherit_env: Vec<String>,
    /// OMP configuration overlays, applied in order.
    #[serde(default)]
    pub config_overlays: Vec<PathBuf>,
}

/// Coordinator-owned launch selection with explicit Codex App Server policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessLaunchProfileV3 {
    /// Must equal three.
    pub schema_version: u32,
    /// Immutable profile identifier.
    pub id: HarnessId,
    /// Native Harness Kind; v3 is currently Codex-specific.
    pub kind: HarnessKind,
    /// Absolute executable or one bare command resolved through `PATH`.
    pub executable: String,
    /// Explicit model selected by the user.
    pub model: String,
    /// Codex App Server approval policy sent at `thread/start`.
    pub approval_policy: CodexApprovalPolicy,
    /// Codex App Server sandbox mode sent at `thread/start`.
    pub sandbox_mode: CodexSandboxMode,
    /// Environment variable names explicitly inherited by the Worker.
    #[serde(default)]
    pub inherit_env: Vec<String>,
}

/// Stable subset of Codex App Server approval policies accepted by v3 profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexApprovalPolicy {
    /// Ask for commands outside the trusted allowlist.
    Untrusted,
    /// Let the model request escalation.
    OnRequest,
    /// Never pause for interactive approval.
    Never,
}

/// Stable subset of Codex App Server sandbox modes accepted by v3 profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexSandboxMode {
    /// Permit filesystem reads only.
    ReadOnly,
    /// Permit writes inside the configured workspace roots.
    WorkspaceWrite,
    /// Delegate isolation to the same-user cooperative Coordinator boundary.
    DangerFullAccess,
}

impl Validate for HarnessLaunchProfileV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_absolute_path("executable", &self.executable)?;
        validate_text("provider_profile", &self.provider_profile, 128, 512)?;
        validate_optional_text("model", self.model.as_deref(), 256)?;
        validate_unique_limit("inherit_env", &self.inherit_env, 128)?;
        for name in &self.inherit_env {
            let valid = !name.is_empty()
                && name.len() <= 128
                && name.bytes().enumerate().all(|(index, byte)| {
                    byte == b'_'
                        || byte.is_ascii_uppercase()
                        || (index > 0 && byte.is_ascii_digit())
                });
            if !valid {
                return field_error("inherit_env", "contains an invalid environment name");
            }
        }
        for overlay in &self.config_overlays {
            validate_absolute_path("config_overlays", overlay)?;
        }
        if self.kind == HarnessKind::Codex && !self.config_overlays.is_empty() {
            return field_error("config_overlays", "is supported only for OMP profiles");
        }
        Ok(())
    }
}

impl Validate for HarnessLaunchProfileV2 {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.schema_version != 2 {
            return field_error("schema_version", "must equal 2");
        }
        let executable = Path::new(&self.executable);
        let bare = !self.executable.is_empty()
            && executable.components().count() == 1
            && self.executable != "."
            && self.executable != "..";
        if !executable.is_absolute() && !bare {
            return field_error("executable", "must be absolute or a bare command name");
        }
        validate_optional_text("provider_profile", self.provider_profile.as_deref(), 128)?;
        validate_text("model", &self.model, 256, 1024)?;
        validate_unique_limit("inherit_env", &self.inherit_env, 128)?;
        for name in &self.inherit_env {
            let valid = !name.is_empty()
                && name.len() <= 128
                && name.bytes().enumerate().all(|(index, byte)| {
                    byte == b'_'
                        || byte.is_ascii_uppercase()
                        || (index > 0 && byte.is_ascii_digit())
                });
            if !valid {
                return field_error("inherit_env", "contains an invalid environment name");
            }
        }
        for overlay in &self.config_overlays {
            validate_absolute_path("config_overlays", overlay)?;
        }
        if self.kind == HarnessKind::Codex && !self.config_overlays.is_empty() {
            return field_error("config_overlays", "is supported only for OMP profiles");
        }
        Ok(())
    }
}

impl Validate for HarnessLaunchProfileV3 {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.schema_version != 3 {
            return field_error("schema_version", "must equal 3");
        }
        if self.kind != HarnessKind::Codex {
            return field_error("kind", "v3 profiles currently support only codex");
        }
        validate_v2_executable_and_environment(&self.executable, &self.model, &self.inherit_env)
    }
}

fn validate_v2_executable_and_environment(
    executable: &str,
    model: &str,
    inherit_env: &[String],
) -> Result<(), ValidationError> {
    let path = Path::new(executable);
    let bare = !executable.is_empty()
        && path.components().count() == 1
        && executable != "."
        && executable != "..";
    if !path.is_absolute() && !bare {
        return field_error("executable", "must be absolute or a bare command name");
    }
    validate_text("model", model, 256, 1024)?;
    validate_unique_limit("inherit_env", inherit_env, 128)?;
    for name in inherit_env {
        let valid = !name.is_empty()
            && name.len() <= 128
            && name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit())
            });
        if !valid {
            return field_error("inherit_env", "contains an invalid environment name");
        }
    }
    Ok(())
}

impl Validate for HarnessDefinitionV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_absolute_path("cwd", &self.cwd)?;
        validate_optional_text("launch_profile", self.launch_profile.as_deref(), 128)?;
        validate_optional_text("model", self.model.as_deref(), 256)?;
        if self.tier == HarnessTier::Worker && self.launch_profile.is_none() {
            return field_error("launch_profile", "is required for a Worker");
        }
        Ok(())
    }
}

/// Repository access granted to a Task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryAccess {
    /// Observe without authorizing changes.
    ReadOnly,
    /// Authorize changes inside declared scopes.
    Mutating,
}

/// One lexical repository write scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteScopeV1 {
    /// Authorize exactly one path.
    ExactFile { path: PathBuf },
    /// Authorize a directory and descendants.
    Subtree { path: PathBuf },
}

impl WriteScopeV1 {
    /// Returns the normalized repository-relative path.
    #[must_use]
    pub fn path(&self) -> &PathBuf {
        match self {
            Self::ExactFile { path } | Self::Subtree { path } => path,
        }
    }
}

/// Repository authority attached to one Task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRepositoryAuthorityV1 {
    /// Canonical worktree root.
    pub root: PathBuf,
    /// Read-only or mutating authority.
    pub access: RepositoryAccess,
    /// Normalized write scopes.
    pub write_scopes: Vec<WriteScopeV1>,
}

impl Validate for TaskRepositoryAuthorityV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_absolute_path("repository.root", &self.root)?;
        for scope in &self.write_scopes {
            validate_relative_path("repository.write_scopes.path", scope.path())?;
        }
        let unique = self
            .write_scopes
            .iter()
            .collect::<std::collections::HashSet<_>>();
        if unique.len() != self.write_scopes.len() {
            return field_error("repository.write_scopes", "contains duplicates");
        }
        match self.access {
            RepositoryAccess::ReadOnly if !self.write_scopes.is_empty() => {
                field_error("repository.write_scopes", "must be empty for read_only")
            }
            RepositoryAccess::Mutating if self.write_scopes.is_empty() => {
                field_error("repository.write_scopes", "must not be empty for mutating")
            }
            _ => Ok(()),
        }
    }
}

/// Required upstream checkpoint for one scheduling edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyCondition {
    /// The dependency has a valid Result and its native turn entered Reviewing.
    ResultReady,
    /// The Supervisor approved the dependency's Result.
    Approved,
}

/// Behavior when one required dependency fails or is cancelled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyFailurePolicy {
    /// Cancel the undispatched dependent Task.
    #[default]
    Cancel,
    /// Retain the dependent Task as Blocked for Supervisor reconciliation.
    KeepBlocked,
}

/// One immutable scheduling edge from a Task to an upstream Task.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDependencyV1 {
    /// Existing upstream Task.
    pub task_id: TaskId,
    /// State required before the dependent may run.
    pub condition: DependencyCondition,
    /// Failure behavior for this edge.
    #[serde(default)]
    pub failure_policy: DependencyFailurePolicy,
}

/// Semantic purpose used by conservative automatic Session selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRole {
    /// Product or infrastructure implementation work.
    Implementation,
    /// Discovery work whose context may benefit a later implementation.
    Investigation,
    /// Independent assessment of another Task's Result.
    Review,
    /// Independent execution of acceptance checks.
    Verification,
    /// Work without a more specific scheduling role.
    Other,
}

/// Requested relationship between a Task and a native Worker Session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionReusePolicy {
    /// The Task must continue in the preferred existing Session.
    Required,
    /// Reuse compatible healthy context when available.
    Prefer,
    /// Start a new native Session.
    Fresh,
    /// Apply conservative role- and relationship-aware defaults.
    Auto,
}

/// Public bounded Task request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSubmissionV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Optional idempotency key.
    pub request_key: Option<String>,
    /// Selected Worker Harness.
    pub worker_id: HarnessId,
    /// Optional related Task context.
    pub related_task_id: Option<TaskId>,
    /// Immutable scheduling dependencies, distinct from informational relation.
    #[serde(default)]
    pub depends_on: Vec<TaskDependencyV1>,
    /// Semantic purpose used by conservative automatic Session reuse.
    pub task_role: TaskRole,
    /// Explicit native Session reuse policy.
    pub session_reuse: SessionReusePolicy,
    /// Preferred existing Coordinator Session; required by `required` reuse.
    pub preferred_session_id: Option<HarnessSessionId>,
    /// Short Task title.
    pub title: String,
    /// Full bounded instructions.
    pub instructions: String,
    /// Immutable input Attachments.
    pub attachments: Vec<AttachmentId>,
    /// Explicit repository authority.
    pub repository: TaskRepositoryAuthorityV1,
}

impl Validate for TaskSubmissionV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_request_key(self.request_key.as_deref())?;
        validate_text("title", &self.title, 160, usize::MAX)?;
        validate_text("instructions", &self.instructions, 16_384, 65_536)?;
        validate_unique_limit("attachments", &self.attachments, 32)?;
        validate_unique_limit("depends_on", &self.depends_on, 32)?;
        let unique_dependencies = self
            .depends_on
            .iter()
            .map(|dependency| dependency.task_id)
            .collect::<std::collections::HashSet<_>>();
        if unique_dependencies.len() != self.depends_on.len() {
            return field_error("depends_on", "contains repeated upstream Tasks");
        }
        if self.session_reuse == SessionReusePolicy::Required && self.preferred_session_id.is_none()
        {
            return field_error(
                "preferred_session_id",
                "is required when session_reuse is required",
            );
        }
        self.repository.validate()
    }
}

/// Durable native Session selection recorded independently from Task lifecycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSessionBinding {
    pub task_id: TaskId,
    pub harness_session_id: HarnessSessionId,
    pub reuse_policy: SessionReusePolicy,
    pub reused: bool,
    pub decision_reason: String,
    pub bound_at: DateTime<Utc>,
}

/// Native health evidence used only after identity and policy compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeSessionHealth {
    Healthy,
    ContextPressure,
    Compacted,
    Ambiguous,
    Failed,
}

/// Durable event that may wake the visible Supervisor Harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorEvent {
    pub id: SupervisorEventId,
    pub kind: SupervisorEventKind,
    pub task_id: Option<TaskId>,
    pub result_revision: Option<u32>,
    pub summary: String,
    pub attachments: Vec<AttachmentId>,
    pub created_at: DateTime<Utc>,
}

/// Events important enough to require Supervisor attention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorEventKind {
    ResultReady,
    BlockingQuestion,
    TaskFailed,
    DeliveryUnknown,
    WorktreeHoldCreated,
    TaskGraphCompleted,
    Notification,
}

/// At-least-once native delivery state for a durable Supervisor event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorEventDeliveryState {
    Pending,
    Dispatching,
    Accepted,
    Processed,
    Unknown,
    Cancelled,
}

/// Purpose of a public Bus Message submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Blocking Worker request.
    Question,
    /// Correlated Supervisor answer.
    Reply,
    /// Supervisor revision request.
    Correction,
    /// Non-structural information.
    Notification,
}

/// Explicit native delivery behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryIntent {
    /// Deliver in a later eligible turn.
    #[default]
    FollowUp,
    /// Append to a verified active turn.
    Steer,
}

/// Public message intent; sender identity is derived from the Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSubmissionV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Optional idempotency key.
    pub request_key: Option<String>,
    /// Destination Harness.
    pub to: HarnessId,
    /// Associated Task, absent only for network Notifications.
    pub task_id: Option<TaskId>,
    /// Message purpose.
    pub kind: MessageKind,
    /// Human-readable content.
    pub text: String,
    /// Immutable Attachments.
    pub attachments: Vec<AttachmentId>,
    /// Question answered by a Reply.
    pub reply_to: Option<MessageId>,
    /// Explicit delivery intent.
    #[serde(default)]
    pub delivery: DeliveryIntent,
    /// Required justification when native steering would interrupt active work.
    pub steer_reason: Option<String>,
}

impl Validate for MessageSubmissionV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_request_key(self.request_key.as_deref())?;
        validate_text("text", &self.text, 16_384, 65_536)?;
        validate_unique_limit("attachments", &self.attachments, 32)?;
        if self.kind != MessageKind::Notification && self.task_id.is_none() {
            return field_error("task_id", "is required for this Message Kind");
        }
        if (self.kind == MessageKind::Reply) != self.reply_to.is_some() {
            return field_error("reply_to", "is required only for Reply");
        }
        if self.delivery == DeliveryIntent::Steer {
            validate_text(
                "steer_reason",
                self.steer_reason.as_deref().unwrap_or_default(),
                1024,
                usize::MAX,
            )?;
        } else if self.steer_reason.is_some() {
            return field_error("steer_reason", "is valid only when delivery is steer");
        }
        Ok(())
    }
}

/// Verification command evidence supplied by a Worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationResultV1 {
    /// Exact command executed.
    pub command: String,
    /// Process exit status.
    pub exit_code: i32,
    /// Worker assessment of success.
    pub passed: bool,
    /// Immutable output evidence.
    pub evidence: AttachmentId,
}

/// Consolidated Worker completion candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultManifestV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Matching Task identity.
    pub task_id: TaskId,
    /// Human-readable summary.
    pub summary: String,
    /// Worker-reported changed paths.
    pub changed_files: Vec<PathBuf>,
    /// One or more verification results.
    pub verification: Vec<VerificationResultV1>,
    /// Declared deviations.
    pub deviations: Vec<String>,
    /// Declared risks.
    pub risks: Vec<String>,
    /// Additional immutable evidence.
    pub attachments: Vec<AttachmentId>,
}

impl Validate for ResultManifestV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_text("summary", &self.summary, 16_384, 65_536)?;
        validate_unique_limit("changed_files", &self.changed_files, usize::MAX)?;
        for path in &self.changed_files {
            validate_relative_path("changed_files", path)?;
        }
        if self.verification.is_empty() {
            return field_error("verification", "must not be empty");
        }
        for entry in &self.verification {
            validate_text("verification.command", &entry.command, 8192, usize::MAX)?;
        }
        validate_string_items("deviations", &self.deviations, 4096)?;
        validate_string_items("risks", &self.risks, 4096)?;
        validate_unique_limit("attachments", &self.attachments, 32)
    }
}

/// Current delivery state projected from immutable attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    /// Awaiting eligibility or an online target.
    Pending,
    /// Persisted and being written to the provider.
    Dispatching,
    /// Natively accepted.
    Accepted,
    /// Failed before provider bytes and eligible for retry.
    RetryableFailed,
    /// Definitively failed.
    PermanentFailed,
    /// Provider acceptance cannot be proved or disproved.
    Unknown,
    /// Delivery was cancelled.
    Cancelled,
}

/// Current durable native-delivery evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReceiptV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Delivered Bus Message.
    pub message_id: MessageId,
    /// Current receipt state.
    pub state: DeliveryState,
    /// Number of immutable attempts.
    pub attempt_count: u32,
    /// Last state update.
    pub updated_at: DateTime<Utc>,
    /// Native request correlation.
    pub native_correlation: Option<String>,
    /// Stable diagnostic code.
    pub error_code: Option<String>,
    /// Bounded diagnostic message.
    pub error_message: Option<String>,
}

impl Validate for DeliveryReceiptV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_optional_text(
            "native_correlation",
            self.native_correlation.as_deref(),
            512,
        )?;
        validate_optional_text("error_code", self.error_code.as_deref(), 128)?;
        validate_optional_text("error_message", self.error_message.as_deref(), 4096)
    }
}

/// Repository checkpoint kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationCheckpoint {
    /// Immediately before native dispatch.
    BeforeDispatch,
    /// After a candidate Result.
    Result,
    /// During cancellation.
    Cancel,
    /// After failure.
    Failure,
    /// During Supervisor Approval.
    Approval,
    /// During Hold reconciliation.
    HoldClear,
}

/// Observed filesystem kind for an untracked path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedFileType {
    /// Regular file with size and digest.
    Regular,
    /// Symbolic link.
    Symlink,
    /// Any other filesystem object.
    Other,
}

/// Evidence for one untracked path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UntrackedPathV1 {
    /// Repository-relative path.
    pub path: PathBuf,
    /// Filesystem object kind.
    pub file_type: ObservedFileType,
    /// Byte size for regular files.
    pub size: Option<u64>,
    /// SHA-256 for regular files.
    pub digest: Option<String>,
}

/// One normalized Git status entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitStatusEntryV1 {
    /// Current path.
    pub path: PathBuf,
    /// Git index status code.
    pub index_status: String,
    /// Git worktree status code.
    pub worktree_status: String,
    /// Rename or copy source.
    pub original_path: Option<PathBuf>,
}

/// Advisory scope classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeClassification {
    /// Path is authorized by a declared scope.
    InScope,
    /// Path is outside declared scopes.
    OutOfScope,
}

/// Classification evidence for one changed path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeClassificationV1 {
    /// Changed repository-relative path.
    pub path: PathBuf,
    /// Advisory classification.
    pub classification: ScopeClassification,
    /// Scope authorizing the path, when present.
    pub matched_scope: Option<WriteScopeV1>,
}

/// Evidence from one Git CLI command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandEvidenceV1 {
    /// Sanitized command description.
    pub command: String,
    /// Tool version.
    pub version: String,
    /// Process exit status.
    pub exit_code: i32,
    /// Bounded diagnostics.
    pub diagnostics: String,
}

/// Immutable digest-addressed Git checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryObservationV1 {
    /// Must equal one.
    pub schema_version: u32,
    /// Observation identity.
    pub id: RepositoryObservationId,
    /// Associated Task.
    pub task_id: TaskId,
    /// Checkpoint purpose.
    pub checkpoint: ObservationCheckpoint,
    /// Canonical worktree root.
    pub worktree_root: PathBuf,
    /// Canonical Git common directory.
    pub git_common_dir: PathBuf,
    /// HEAD object or unborn state.
    pub head: Option<String>,
    /// Current branch or detached state.
    pub branch: Option<String>,
    /// SHA-256 of index metadata.
    pub index_digest: String,
    /// Binary staged diff Attachment.
    pub staged_diff: Option<AttachmentId>,
    /// Binary unstaged diff Attachment.
    pub unstaged_diff: Option<AttachmentId>,
    /// Untracked path evidence.
    pub untracked: Vec<UntrackedPathV1>,
    /// Visible ignored paths.
    pub ignored_paths: Vec<PathBuf>,
    /// Normalized Git status.
    pub status_entries: Vec<GitStatusEntryV1>,
    /// Paths changed relative to the Task baseline.
    pub changed_paths: Vec<PathBuf>,
    /// Per-path advisory scope evidence.
    pub scope_classifications: Vec<ScopeClassificationV1>,
    /// Commands used to gather evidence.
    pub command_evidence: Vec<CommandEvidenceV1>,
    /// Capture time.
    pub captured_at: DateTime<Utc>,
    /// Canonical SHA-256 digest.
    pub digest: String,
}

impl Validate for RepositoryObservationV1 {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_version(self.schema_version)?;
        validate_absolute_path("worktree_root", &self.worktree_root)?;
        validate_absolute_path("git_common_dir", &self.git_common_dir)?;
        validate_sha256("index_digest", &self.index_digest)?;
        validate_sha256("digest", &self.digest)?;
        if self.command_evidence.is_empty() {
            return field_error("command_evidence", "must not be empty");
        }
        for path in self.ignored_paths.iter().chain(&self.changed_paths) {
            validate_relative_path("repository path", path)?;
        }
        Ok(())
    }
}

fn validate_version(version: u32) -> Result<(), ValidationError> {
    if version == SCHEMA_VERSION {
        Ok(())
    } else {
        field_error("schema_version", "must equal 1")
    }
}

fn validate_text(
    field: &'static str,
    value: &str,
    max_scalars: usize,
    max_bytes: usize,
) -> Result<(), ValidationError> {
    let scalars = value.chars().count();
    if scalars == 0 || scalars > max_scalars || value.len() > max_bytes {
        return field_error(field, "is empty or exceeds the v1 length limit");
    }
    Ok(())
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    max_scalars: usize,
) -> Result<(), ValidationError> {
    value.map_or(Ok(()), |text| {
        validate_text(field, text, max_scalars, usize::MAX)
    })
}

fn validate_request_key(value: Option<&str>) -> Result<(), ValidationError> {
    value.map_or(Ok(()), |key| validate_text("request_key", key, 128, 512))
}

fn validate_absolute_path(field: &'static str, path: &Path) -> Result<(), ValidationError> {
    let Some(text) = path.to_str() else {
        return field_error(field, "must be UTF-8");
    };
    if !path.is_absolute() || text.is_empty() || text.len() > 4096 {
        return field_error(field, "must be an absolute path of at most 4096 bytes");
    }
    Ok(())
}

fn validate_relative_path(field: &'static str, path: &Path) -> Result<(), ValidationError> {
    let Some(text) = path.to_str() else {
        return field_error(field, "must be UTF-8");
    };
    let invalid_component = text.split('/').any(|component| {
        component.is_empty() || component == "." || component == ".." || component == ".git"
    });
    if text.is_empty()
        || text.len() > 4096
        || path.is_absolute()
        || text.ends_with('/')
        || text.contains('\\')
        || text.contains('\0')
        || invalid_component
    {
        return field_error(field, "must be a normalized repository-relative UTF-8 path");
    }
    Ok(())
}

fn validate_unique_limit<T: Eq + std::hash::Hash>(
    field: &'static str,
    values: &[T],
    max: usize,
) -> Result<(), ValidationError> {
    if values.len() > max
        || values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != values.len()
    {
        return field_error(field, "contains duplicates or exceeds the item limit");
    }
    Ok(())
}

fn validate_string_items(
    field: &'static str,
    values: &[String],
    max_scalars: usize,
) -> Result<(), ValidationError> {
    for value in values {
        validate_text(field, value, max_scalars, usize::MAX)?;
    }
    Ok(())
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        field_error(field, "must be a lowercase SHA-256 digest")
    }
}

fn field_error<T>(field: &'static str, message: &str) -> Result<T, ValidationError> {
    Err(ValidationError::Field {
        field,
        message: message.to_owned(),
    })
}
