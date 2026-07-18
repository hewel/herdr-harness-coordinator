//! Herdr socket-protocol and plugin boundary.
//!
//! Public pane identifiers are live UI locations. Coordinator sessions bind to
//! [`PaneInfo::terminal_id`] and resolve a fresh [`PaneLocation`] after every
//! snapshot or reconnect.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
};
use uuid::Uuid;

pub const HERDR_VERSION: &str = "0.7.4";
pub const HERDR_PROTOCOL: u32 = 16;
pub const PLUGIN_ID: &str = "herdr-harness-coordinator";
pub const WORKER_ENTRYPOINT: &str = "worker";
pub const SUPERVISOR_ENTRYPOINT: &str = "supervisor";
pub const POPUP_ENTRYPOINT: &str = "harness-network";
pub const METADATA_SOURCE: &str = "herdr-harness-coordinator";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum HerdrError {
    #[error("Herdr socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Herdr JSON frame is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Herdr returned {code}: {message}")]
    Remote { code: String, message: String },
    #[error("Herdr response correlation mismatch: expected {expected}, received {received}")]
    Correlation { expected: String, received: String },
    #[error("Herdr response exceeded the {MAX_RESPONSE_BYTES} byte limit")]
    OversizedResponse,
    #[error("unexpected Herdr response: {0}")]
    UnexpectedResponse(String),
    #[error(
        "Herdr >=0.7.4 with protocol 16 required; received version {version}, protocol {protocol}"
    )]
    Incompatible { version: String, protocol: u32 },
    #[error("terminal {0} is absent from the current Herdr snapshot")]
    TerminalMissing(String),
    #[error("terminal {0} occurs more than once in the current Herdr snapshot")]
    DuplicateTerminal(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaneInfo {
    pub pane_id: String,
    pub terminal_id: String,
    pub workspace_id: String,
    pub tab_id: String,
    pub focused: bool,
    pub agent_status: String,
    pub revision: u64,
    #[serde(default)]
    pub state_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub tokens: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub version: String,
    pub protocol: u32,
    #[serde(default)]
    pub panes: Vec<PaneInfo>,
}

impl SessionSnapshot {
    /// Verifies the minimum product release and exact socket protocol.
    ///
    /// # Errors
    ///
    /// Returns [`HerdrError::Incompatible`] when the protocol differs or the release is too old.
    pub fn validate_compatibility(&self) -> Result<(), HerdrError> {
        if !version_at_least(&self.version, HERDR_VERSION) || self.protocol != HERDR_PROTOCOL {
            return Err(HerdrError::Incompatible {
                version: self.version.clone(),
                protocol: self.protocol,
            });
        }
        Ok(())
    }

    /// Resolves a live pane location from its stable terminal identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the terminal is missing or duplicated.
    pub fn resolve_terminal(&self, terminal_id: &str) -> Result<PaneLocation, HerdrError> {
        let mut matches = self
            .panes
            .iter()
            .filter(|pane| pane.terminal_id == terminal_id);
        let pane = matches
            .next()
            .ok_or_else(|| HerdrError::TerminalMissing(terminal_id.to_owned()))?;
        if matches.next().is_some() {
            return Err(HerdrError::DuplicateTerminal(terminal_id.to_owned()));
        }
        Ok(PaneLocation::from(pane))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaneLocation {
    pub pane_id: String,
    pub terminal_id: String,
    pub workspace_id: String,
    pub tab_id: String,
}

impl From<&PaneInfo> for PaneLocation {
    fn from(pane: &PaneInfo) -> Self {
        Self {
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            workspace_id: pane.workspace_id.clone(),
            tab_id: pane.tab_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PluginPaneOpenParams {
    pub plugin_id: String,
    pub entrypoint: String,
    pub placement: Option<String>,
    pub workspace_id: Option<String>,
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub focus: bool,
}

impl PluginPaneOpenParams {
    #[must_use]
    pub fn supervisor(session_capability: &str, _cwd: &Path, workspace_id: Option<String>) -> Self {
        let mut env = BTreeMap::new();
        env.insert(
            "HERDR_SUPERVISOR_CAPABILITY".to_owned(),
            session_capability.to_owned(),
        );
        Self {
            plugin_id: PLUGIN_ID.to_owned(),
            entrypoint: SUPERVISOR_ENTRYPOINT.to_owned(),
            placement: Some("tab".to_owned()),
            workspace_id,
            cwd: None,
            env,
            focus: true,
        }
    }

    #[must_use]
    pub fn worker(session_capability: &str, cwd: &Path, workspace_id: Option<String>) -> Self {
        let mut env = BTreeMap::new();
        env.insert(
            "HERDR_HARNESS_SESSION_ID".to_owned(),
            session_capability.to_owned(),
        );
        env.insert(
            "HERDR_HARNESS_CWD".to_owned(),
            cwd.to_string_lossy().into_owned(),
        );
        Self {
            plugin_id: PLUGIN_ID.to_owned(),
            entrypoint: WORKER_ENTRYPOINT.to_owned(),
            placement: Some("tab".to_owned()),
            workspace_id,
            cwd: None,
            env,
            focus: false,
        }
    }

    #[must_use]
    pub fn popup(workspace_id: String) -> Self {
        Self {
            plugin_id: PLUGIN_ID.to_owned(),
            entrypoint: POPUP_ENTRYPOINT.to_owned(),
            placement: Some("popup".to_owned()),
            workspace_id: Some(workspace_id),
            cwd: None,
            env: BTreeMap::new(),
            focus: true,
        }
    }
}

fn version_at_least(actual: &str, minimum: &str) -> bool {
    fn components(value: &str) -> Option<[u64; 3]> {
        let mut parts = value.split('.');
        let parsed = [
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
        ];
        parts.next().is_none().then_some(parsed)
    }
    matches!((components(actual), components(minimum)), (Some(actual), Some(minimum)) if actual >= minimum)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataProjection {
    pub title: String,
    pub state: String,
    pub detail: String,
    pub inbox: u64,
}

impl MetadataProjection {
    #[must_use]
    pub fn for_pane(&self, pane_id: &str, sequence: u64) -> PaneReportMetadataParams {
        PaneReportMetadataParams {
            pane_id: pane_id.to_owned(),
            source: METADATA_SOURCE.to_owned(),
            seq: Some(sequence),
            title: Some(self.title.clone()),
            state_labels: BTreeMap::from([
                ("state".to_owned(), self.state.clone()),
                ("detail".to_owned(), self.detail.clone()),
                ("inbox".to_owned(), self.inbox.to_string()),
            ]),
        }
    }
}

/// Presentation-only metadata. It intentionally has no `agent_status` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaneReportMetadataParams {
    pub pane_id: String,
    pub source: String,
    pub seq: Option<u64>,
    pub title: Option<String>,
    pub state_labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PluginPaneInfo {
    pub plugin_id: String,
    pub entrypoint: String,
    pub pane: PaneInfo,
}

#[derive(Debug, Clone)]
pub struct HerdrSocketClient {
    socket_path: PathBuf,
}

impl HerdrSocketClient {
    #[must_use]
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// Takes a fresh snapshot from the configured Herdr socket.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O, malformed frames, or an incompatible Herdr.
    pub async fn snapshot(&self) -> Result<SessionSnapshot, HerdrError> {
        let result = self.call("session.snapshot", json!({})).await?;
        parse_snapshot(&result)
    }

    /// Exchange a snapshot request over an already-connected stream.
    ///
    /// This is useful to retain connection ownership in a broker and to test
    /// the wire contract without binding or mutating a live Herdr socket.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O, malformed frames, or an incompatible Herdr.
    pub async fn snapshot_over<S>(stream: S) -> Result<SessionSnapshot, HerdrError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let result = Self::call_over(stream, "session.snapshot", json!({})).await?;
        parse_snapshot(&result)
    }

    /// Opens an unfocused plugin-owned Worker pane.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr rejects the request or the wire exchange fails.
    pub async fn open_worker(
        &self,
        params: PluginPaneOpenParams,
    ) -> Result<PluginPaneInfo, HerdrError> {
        let result = self.call("plugin.pane.open", params).await?;
        parse_plugin_pane(&result, "plugin_pane_opened")
    }

    /// Focuses the current public location of a plugin-owned pane.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr rejects the pane or the wire exchange fails.
    pub async fn focus(&self, pane_id: &str) -> Result<PluginPaneInfo, HerdrError> {
        let result = self
            .call("plugin.pane.focus", json!({ "pane_id": pane_id }))
            .await?;
        parse_plugin_pane(&result, "plugin_pane_focused")
    }

    /// Closes a plugin-owned Worker pane after lifecycle intent is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr rejects the pane or the wire exchange fails.
    pub async fn close(&self, pane_id: &str) -> Result<(), HerdrError> {
        self.call("plugin.pane.close", json!({ "pane_id": pane_id }))
            .await?;
        Ok(())
    }

    /// Closes only the session-modal popup.
    ///
    /// This command carries no pane or Harness identity and therefore cannot
    /// cancel work or close a Worker pane.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr rejects the request or the exchange fails.
    pub async fn close_popup(&self) -> Result<(), HerdrError> {
        self.call("popup.close", json!({})).await?;
        Ok(())
    }

    /// Publishes Coordinator presentation metadata without claiming agent status.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr rejects the projection or the exchange fails.
    pub async fn report_metadata(
        &self,
        params: PaneReportMetadataParams,
    ) -> Result<(), HerdrError> {
        self.call("pane.report_metadata", params).await?;
        Ok(())
    }

    async fn call<P>(&self, method: &str, params: P) -> Result<Value, HerdrError>
    where
        P: Serialize,
    {
        let stream = UnixStream::connect(&self.socket_path).await?;
        Self::call_over(stream, method, params).await
    }

    async fn call_over<S, P>(mut stream: S, method: &str, params: P) -> Result<Value, HerdrError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        P: Serialize,
    {
        let correlation_id = Uuid::now_v7().to_string();
        let request = json!({
            "id": correlation_id,
            "method": method,
            "params": params,
        });
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;

        let mut response = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            let received = stream.read(&mut chunk).await?;
            if received == 0 {
                return Err(HerdrError::UnexpectedResponse(
                    "socket closed before a complete JSONL response".to_owned(),
                ));
            }
            let end = chunk[..received]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(received, |position| position + 1);
            if response.len() + end > MAX_RESPONSE_BYTES {
                return Err(HerdrError::OversizedResponse);
            }
            response.extend_from_slice(&chunk[..end]);
            if end != received || response.last() == Some(&b'\n') {
                break;
            }
        }
        let response: WireResponse = serde_json::from_slice(&response)?;
        if response.id != correlation_id {
            return Err(HerdrError::Correlation {
                expected: correlation_id,
                received: response.id,
            });
        }
        if let Some(error) = response.error {
            return Err(HerdrError::Remote {
                code: error.code,
                message: error.message,
            });
        }
        response
            .result
            .ok_or_else(|| HerdrError::UnexpectedResponse("missing result".to_owned()))
    }
}

fn parse_snapshot(result: &Value) -> Result<SessionSnapshot, HerdrError> {
    let snapshot = result
        .get("snapshot")
        .cloned()
        .filter(|_| result.get("type") == Some(&Value::String("session_snapshot".to_owned())))
        .ok_or_else(|| HerdrError::UnexpectedResponse(result.to_string()))?;
    let snapshot: SessionSnapshot = serde_json::from_value(snapshot)?;
    snapshot.validate_compatibility()?;
    Ok(snapshot)
}

fn parse_plugin_pane(result: &Value, expected_type: &str) -> Result<PluginPaneInfo, HerdrError> {
    if result.get("type").and_then(Value::as_str) != Some(expected_type) {
        return Err(HerdrError::UnexpectedResponse(result.to_string()));
    }
    let plugin_pane = result
        .get("plugin_pane")
        .cloned()
        .ok_or_else(|| HerdrError::UnexpectedResponse(result.to_string()))?;
    Ok(serde_json::from_value(plugin_pane)?)
}

#[derive(Debug, Deserialize)]
struct WireResponse {
    id: String,
    result: Option<Value>,
    error: Option<WireError>,
}

#[derive(Debug, Deserialize)]
struct WireError {
    code: String,
    message: String,
}
