//! Provider-neutral Harness Adapter seam and pinned protocol frame classifiers.

use std::{collections::BTreeMap, fmt, path::PathBuf, pin::Pin};

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::contract::{AttachmentId, HarnessKind, HarnessSessionId, TaskId};

/// OMP version verified by the MVP compatibility contract.
pub const OMP_VERSION_OUTPUT: &str = "omp/17.0.2";

/// Codex CLI version verified by the MVP compatibility contract.
pub const CODEX_VERSION_OUTPUT: &str = "codex-cli 0.144.5";

/// Provider Adapter failure with enough structure for stable Coordinator mapping.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// A provider emitted malformed JSONL.
    #[error("invalid {kind:?} JSON frame: {source}")]
    InvalidJson {
        /// Provider whose stream contained the frame.
        kind: HarnessKind,
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// A valid JSON value did not match the pinned provider framing contract.
    #[error("invalid {kind:?} protocol frame: {message}")]
    InvalidFrame {
        /// Provider whose protocol was violated.
        kind: HarnessKind,
        /// Concise framing violation.
        message: String,
    },
    /// The installed provider version has not been compatibility-tested.
    #[error("unsupported {kind:?} version output `{actual}`; expected `{expected}`")]
    UnsupportedVersion {
        /// Provider executable being checked.
        kind: HarnessKind,
        /// Exact pinned command output, excluding a line ending.
        expected: &'static str,
        /// Actual output, excluding a single line ending.
        actual: String,
    },
    /// Provider I/O or lifecycle operation failed.
    #[error("{kind:?} adapter operation failed: {message}")]
    Operation {
        /// Provider being operated.
        kind: HarnessKind,
        /// Concise provider or transport diagnostic.
        message: String,
    },
    /// A provider request failed after a write was attempted, so replay is unsafe.
    #[error("{kind:?} delivery acceptance is ambiguous: {message}")]
    DeliveryAmbiguous {
        /// Provider being operated.
        kind: HarnessKind,
        /// Concise provider or transport diagnostic.
        message: String,
    },
}

impl AdapterError {
    /// Whether the failed operation may already have reached the provider.
    #[must_use]
    pub fn provider_bytes_may_have_been_written(&self) -> bool {
        matches!(self, Self::DeliveryAmbiguous { .. })
    }
}

/// Result returned by provider-neutral Adapter operations.
pub type AdapterResult<T> = Result<T, AdapterError>;

/// Dynamically dispatched stream of normalized Adapter events.
pub type AdapterEventStream =
    Pin<Box<dyn Stream<Item = AdapterResult<AdapterEvent>> + Send + 'static>>;

/// Features whose native semantics have been verified for one Adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent provider capabilities form a compact feature matrix"
)]
pub struct AdapterCapabilities {
    /// One native session can execute sequential top-level Tasks.
    pub persistent_session: bool,
    /// Explicit Supervisor input can steer an active top-level turn.
    pub active_turn_steering: bool,
    /// Input can be queued inside the provider while a turn is active.
    pub active_turn_follow_up: bool,
    /// The provider exposes cooperative cancellation before forced shutdown.
    pub cooperative_cancellation: bool,
}

/// Provider-neutral configuration needed to start one Worker Harness process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessStartSpec {
    /// Durable Coordinator Session being hosted.
    pub session_id: HarnessSessionId,
    /// Absolute pinned provider executable.
    pub executable: PathBuf,
    /// Registered live worktree used by the native Harness.
    pub cwd: PathBuf,
    /// Provider-owned files for this Harness Session.
    pub provider_state_dir: PathBuf,
    /// Explicit provider-native profile selection.
    pub provider_profile: String,
    /// Explicitly selected model, when the profile pins one.
    pub model: Option<String>,
    /// OMP configuration overlays; empty for Codex.
    pub config_overlays: Vec<PathBuf>,
    /// Already-filtered environment values inherited by the provider.
    pub environment: BTreeMap<String, String>,
}

/// Native identity established after successful provider startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSession {
    /// Provider session identity, if the provider exposes one.
    pub session_id: Option<String>,
    /// Persistent conversation identity, such as a Codex thread.
    pub thread_id: Option<String>,
    /// Effective provider working directory.
    pub cwd: PathBuf,
    /// Effective provider model, if reported.
    pub model: Option<String>,
}

/// Provider-independent delivery operation selected by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeDeliveryKind {
    /// Begin a new top-level native turn.
    StartTurn,
    /// Queue input for the same active native Task.
    FollowUp,
    /// Append explicit Supervisor input to a verified active turn.
    Steer,
}

/// Immutable Attachment resolved for provider delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedAttachment {
    /// Coordinator Attachment identity.
    pub id: AttachmentId,
    /// Verified immutable file path in Coordinator state.
    pub path: PathBuf,
    /// Recorded media type.
    pub media_type: String,
}

/// Fully authorized native input passed from the Coordinator to an Adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedDelivery {
    /// Unique host-generated provider request correlation.
    pub correlation: String,
    /// Task whose top-level conversation receives the input, absent for network Notifications.
    pub task_id: Option<TaskId>,
    /// Resolved provider operation.
    pub kind: NativeDeliveryKind,
    /// Prompt, Reply, Correction, or Notification text.
    pub text: String,
    /// Verified immutable Attachments available to the provider.
    pub attachments: Vec<ResolvedAttachment>,
}

/// Provider evidence that one delivery was accepted, not completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAcceptance {
    /// Correlation from [`ResolvedDelivery`].
    pub correlation: String,
    /// Provider-native turn identity when acceptance creates or identifies one.
    pub turn_id: Option<String>,
    /// Provider-native acceptance description suitable for durable evidence.
    pub evidence: String,
}

/// Coarse provider lifecycle exposed in snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterLifecycle {
    /// Process startup is not yet complete.
    Starting,
    /// Provider is online without an active top-level turn.
    Idle,
    /// Provider has an active top-level turn.
    Working,
    /// Cooperative or forced shutdown is underway.
    Stopping,
    /// Provider has stopped cleanly.
    Stopped,
    /// Provider can no longer safely receive delivery.
    Failed,
}

/// Provider-neutral live state captured on demand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterSnapshot {
    /// Current coarse lifecycle.
    pub lifecycle: AdapterLifecycle,
    /// Provider-native Session identity, when reported.
    pub session_id: Option<String>,
    /// Persistent provider thread or conversation identity.
    pub thread_id: Option<String>,
    /// Active top-level turn identity, when reported.
    pub active_turn_id: Option<String>,
    /// Whether explicit steering is currently valid.
    pub steerable: bool,
    /// Provider-native queued input count, when available.
    pub queued_input_count: Option<u32>,
    /// Effective model, when reported.
    pub model: Option<String>,
}

/// Terminal status of one top-level native turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeTurnStatus {
    /// Provider reported normal completion.
    Completed,
    /// Provider reported cooperative interruption.
    Interrupted,
    /// Provider reported terminal failure.
    Failed,
}

/// Top-level provider evidence normalized for the Coordinator Core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AdapterEvent {
    /// Provider startup established its native identity.
    SessionStarted(NativeSession),
    /// A correlated top-level turn began.
    TurnStarted {
        /// Provider-native turn identity, if reported.
        turn_id: Option<String>,
    },
    /// Human-readable top-level transcript content.
    Transcript {
        /// Text retained as transcript evidence.
        text: String,
    },
    /// Display-only command, tool, or file-change activity.
    Activity {
        /// Concise provider-independent description.
        summary: String,
    },
    /// Provider requires native user input or approval handling.
    InputRequired {
        /// Provider-native request correlation when one exists.
        correlation: Option<String>,
        /// Concise prompt for Supervisor handling.
        prompt: String,
    },
    /// One top-level native turn settled.
    TurnCompleted {
        /// Settled provider-native turn identity, if reported.
        turn_id: Option<String>,
        /// Terminal provider status.
        status: NativeTurnStatus,
    },
    /// Provider or transport failure invalidated the live Session.
    Failed {
        /// Concise failure diagnostic.
        message: String,
    },
    /// Provider process exited.
    Exited {
        /// Exit status when the host observed one.
        exit_code: Option<i32>,
    },
}

/// Object-safe provider boundary owned by a pane-resident Harness Host.
#[async_trait]
pub trait HarnessAdapter: Send {
    /// Returns the native Harness implementation.
    fn kind(&self) -> HarnessKind;

    /// Returns only capabilities verified by pinned compatibility fixtures.
    fn capabilities(&self) -> AdapterCapabilities;

    /// Starts and initializes one persistent native Worker Session.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when version checks, launch, or initialization fail.
    async fn start(&mut self, spec: &HarnessStartSpec) -> AdapterResult<NativeSession>;

    /// Delivers authorized input and returns only native acceptance evidence.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when acceptance fails or becomes ambiguous.
    async fn dispatch(&mut self, delivery: ResolvedDelivery) -> AdapterResult<NativeAcceptance>;

    /// Requests cooperative cancellation of the active top-level turn.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the request cannot be issued or observed.
    async fn cancel_active(&mut self) -> AdapterResult<()>;

    /// Stops the provider process after draining accepted protocol work.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when clean shutdown cannot be established.
    async fn stop(&mut self) -> AdapterResult<()>;

    /// Captures provider state without exposing provider-native frame types.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the snapshot request fails.
    async fn snapshot(&mut self) -> AdapterResult<AdapterSnapshot>;

    /// Borrows the ordered stream of normalized top-level provider events.
    fn events(&mut self) -> AdapterEventStream;
}

/// Correlation identifier accepted by the pinned provider JSONL protocols.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CorrelationId {
    /// String correlation used by OMP and accepted by Codex.
    String(String),
    /// Integer correlation accepted by Codex App Server.
    Number(i64),
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => formatter.write_str(value),
            Self::Number(value) => value.fmt(formatter),
        }
    }
}

/// Classified OMP RPC output frame from the pinned `17.0.2` protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OmpFrame {
    /// Process completed initialization and is ready for commands.
    Ready,
    /// Correlated command response; success does not mean turn completion.
    Response {
        /// Echoed command correlation.
        id: CorrelationId,
        /// Command being answered.
        command: String,
        /// Successful data or provider error text.
        result: Result<Value, String>,
    },
    /// Provider asks the Host to execute one registered Coordinator tool.
    HostToolCall {
        /// Host request correlation.
        id: CorrelationId,
        /// Provider tool-call identity.
        tool_call_id: String,
        /// Registered host tool name.
        tool_name: String,
        /// Provider-supplied tool arguments.
        arguments: Map<String, Value>,
    },
    /// Provider asks the Host to cancel a pending host tool call.
    HostToolCancel {
        /// Host cancellation request correlation.
        id: CorrelationId,
        /// Target host request identity.
        target_id: String,
    },
    /// Extension UI traffic requiring Host handling or durable evidence.
    ExtensionUiRequest {
        /// Extension request correlation.
        id: CorrelationId,
        /// Requested UI operation.
        method: String,
        /// Complete provider payload.
        payload: Value,
    },
    /// OMP session event, including `agent_end` and opaque native-child events.
    SessionEvent {
        /// Provider event discriminator.
        event_type: String,
        /// Complete provider payload.
        payload: Value,
    },
}

/// Parses and classifies one OMP RPC JSONL frame.
///
/// # Errors
///
/// Returns [`AdapterError::InvalidJson`] for malformed JSON or
/// [`AdapterError::InvalidFrame`] when required framing or correlation is absent.
pub fn classify_omp_frame(line: &str) -> AdapterResult<OmpFrame> {
    let value: Value = serde_json::from_str(line).map_err(|source| AdapterError::InvalidJson {
        kind: HarnessKind::Omp,
        source,
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid_frame(HarnessKind::Omp, "top-level frame must be an object"))?;
    let frame_type = required_nonempty_string(object, "type", HarnessKind::Omp)?;

    match frame_type {
        "ready" => Ok(OmpFrame::Ready),
        "response" => classify_omp_response(object),
        "host_tool_call" => {
            let arguments = object
                .get("arguments")
                .and_then(Value::as_object)
                .cloned()
                .ok_or_else(|| {
                    invalid_frame(HarnessKind::Omp, "host tool arguments must be an object")
                })?;
            Ok(OmpFrame::HostToolCall {
                id: required_omp_correlation(object)?,
                tool_call_id: required_nonempty_string(object, "toolCallId", HarnessKind::Omp)?
                    .to_owned(),
                tool_name: required_nonempty_string(object, "toolName", HarnessKind::Omp)?
                    .to_owned(),
                arguments,
            })
        }
        "host_tool_cancel" => Ok(OmpFrame::HostToolCancel {
            id: required_omp_correlation(object)?,
            target_id: required_nonempty_string(object, "targetId", HarnessKind::Omp)?.to_owned(),
        }),
        "extension_ui_request" => Ok(OmpFrame::ExtensionUiRequest {
            id: required_omp_correlation(object)?,
            method: required_nonempty_string(object, "method", HarnessKind::Omp)?.to_owned(),
            payload: value,
        }),
        _ => Ok(OmpFrame::SessionEvent {
            event_type: frame_type.to_owned(),
            payload: value,
        }),
    }
}

fn classify_omp_response(object: &Map<String, Value>) -> AdapterResult<OmpFrame> {
    let id = required_omp_correlation(object)?;
    let command = required_nonempty_string(object, "command", HarnessKind::Omp)?.to_owned();
    let success = object
        .get("success")
        .and_then(Value::as_bool)
        .ok_or_else(|| invalid_frame(HarnessKind::Omp, "response success must be a boolean"))?;
    let result = if success {
        Ok(object.get("data").cloned().unwrap_or(Value::Null))
    } else {
        Err(required_nonempty_string(object, "error", HarnessKind::Omp)?.to_owned())
    };
    Ok(OmpFrame::Response {
        id,
        command,
        result,
    })
}

/// Classified Codex App Server output frame from the pinned `0.144.5` protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexFrame {
    /// Correlated response to a Host request.
    Response {
        /// Echoed Host request correlation.
        id: CorrelationId,
        /// Successful result or structured provider error.
        result: Result<Value, Value>,
    },
    /// Uncorrelated provider notification such as `turn/completed`.
    Notification {
        /// App Server notification method.
        method: String,
        /// Notification parameters.
        params: Value,
    },
    /// Correlated provider request requiring Host handling.
    ServerRequest {
        /// Server request correlation.
        id: CorrelationId,
        /// App Server request method.
        method: String,
        /// Request parameters.
        params: Value,
    },
}

/// Parses and classifies one Codex App Server JSONL frame.
///
/// # Errors
///
/// Returns [`AdapterError::InvalidJson`] for malformed JSON or
/// [`AdapterError::InvalidFrame`] when response, notification, and request
/// framing is missing or ambiguous.
pub fn classify_codex_frame(line: &str) -> AdapterResult<CodexFrame> {
    let value: Value = serde_json::from_str(line).map_err(|source| AdapterError::InvalidJson {
        kind: HarnessKind::Codex,
        source,
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid_frame(HarnessKind::Codex, "top-level frame must be an object"))?;
    let id = optional_correlation(object, HarnessKind::Codex)?;
    let method = optional_nonempty_string(object, "method", HarnessKind::Codex)?;

    match (id, method, object.get("result"), object.get("error")) {
        (Some(id), None, Some(result), None) => Ok(CodexFrame::Response {
            id,
            result: Ok(result.clone()),
        }),
        (Some(id), None, None, Some(error)) => Ok(CodexFrame::Response {
            id,
            result: Err(error.clone()),
        }),
        (Some(id), Some(method), None, None) => Ok(CodexFrame::ServerRequest {
            id,
            method: method.to_owned(),
            params: object.get("params").cloned().unwrap_or(Value::Null),
        }),
        (None, Some(method), None, None) => Ok(CodexFrame::Notification {
            method: method.to_owned(),
            params: object.get("params").cloned().unwrap_or(Value::Null),
        }),
        _ => Err(invalid_frame(
            HarnessKind::Codex,
            "correlated frame must contain exactly one of result, error, or method",
        )),
    }
}

/// Validates exact `omp --version` output for the pinned MVP protocol.
///
/// A single platform line ending is ignored; all other whitespace is significant.
///
/// # Errors
///
/// Returns [`AdapterError::UnsupportedVersion`] unless output is exactly
/// [`OMP_VERSION_OUTPUT`].
pub fn validate_omp_version_output(output: &str) -> AdapterResult<()> {
    validate_version_output(HarnessKind::Omp, OMP_VERSION_OUTPUT, output)
}

/// Validates exact `codex --version` output for the pinned MVP protocol.
///
/// A single platform line ending is ignored; all other whitespace is significant.
///
/// # Errors
///
/// Returns [`AdapterError::UnsupportedVersion`] unless output is exactly
/// [`CODEX_VERSION_OUTPUT`].
pub fn validate_codex_version_output(output: &str) -> AdapterResult<()> {
    validate_version_output(HarnessKind::Codex, CODEX_VERSION_OUTPUT, output)
}

fn validate_version_output(
    kind: HarnessKind,
    expected: &'static str,
    output: &str,
) -> AdapterResult<()> {
    let actual = output
        .strip_suffix("\r\n")
        .or_else(|| output.strip_suffix('\n'))
        .unwrap_or(output);
    if actual == expected {
        Ok(())
    } else {
        Err(AdapterError::UnsupportedVersion {
            kind,
            expected,
            actual: actual.to_owned(),
        })
    }
}

fn required_omp_correlation(object: &Map<String, Value>) -> AdapterResult<CorrelationId> {
    match object.get("id") {
        Some(Value::String(value)) if !value.is_empty() => Ok(CorrelationId::String(value.clone())),
        Some(_) => Err(invalid_frame(
            HarnessKind::Omp,
            "correlation id must be a nonempty string",
        )),
        None => Err(invalid_frame(
            HarnessKind::Omp,
            "correlation id is required",
        )),
    }
}

fn optional_correlation(
    object: &Map<String, Value>,
    kind: HarnessKind,
) -> AdapterResult<Option<CorrelationId>> {
    match object.get("id") {
        None => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => {
            Ok(Some(CorrelationId::String(value.clone())))
        }
        Some(Value::Number(value)) => value
            .as_i64()
            .map(CorrelationId::Number)
            .map(Some)
            .ok_or_else(|| invalid_frame(kind, "correlation number must be an integer")),
        Some(_) => Err(invalid_frame(
            kind,
            "correlation id must be a nonempty string or integer",
        )),
    }
}

fn required_nonempty_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    kind: HarnessKind,
) -> AdapterResult<&'a str> {
    optional_nonempty_string(object, field, kind)?
        .ok_or_else(|| invalid_frame(kind, format!("{field} is required")))
}

fn optional_nonempty_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    kind: HarnessKind,
) -> AdapterResult<Option<&'a str>> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value)),
        Some(_) => Err(invalid_frame(
            kind,
            format!("{field} must be a nonempty string"),
        )),
    }
}

fn invalid_frame(kind: HarnessKind, message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidFrame {
        kind,
        message: message.into(),
    }
}
