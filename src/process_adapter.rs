//! Process-backed OMP RPC and Codex App Server Harness Adapters.

use std::{
    collections::HashMap,
    fmt::Write as _,
    path::Path,
    process::Stdio,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};

use crate::{
    adapter::{
        AdapterCapabilities, AdapterError, AdapterEvent, AdapterEventStream, AdapterLifecycle,
        AdapterResult, AdapterSnapshot, CodexFrame, HarnessAdapter, HarnessStartSpec,
        NativeAcceptance, NativeDeliveryKind, NativeSession, NativeSessionResume, NativeTurnStatus,
        OmpFrame, ResolvedDelivery, WorkerCompletionTools, classify_codex_frame,
        classify_omp_frame, validate_codex_version_output, validate_omp_version_output,
    },
    contract::{HarnessKind, NativeSessionHealth},
    mcp::{self, McpServer},
};

const START_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);
const VERSION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_VERSION_BYTES: u64 = 4096;
type Reply = Result<Value, String>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>>;

#[derive(Debug)]
struct State {
    lifecycle: AdapterLifecycle,
    session_id: Option<String>,
    thread_id: Option<String>,
    active_turn_id: Option<String>,
    queued_input_count: Option<u32>,
    model: Option<String>,
    native_health: NativeSessionHealth,
    context_tokens: Option<u64>,
    context_window: Option<u64>,
    context_percent: Option<f64>,
    compaction_count: Option<u32>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            lifecycle: AdapterLifecycle::Starting,
            session_id: None,
            thread_id: None,
            active_turn_id: None,
            queued_input_count: None,
            model: None,
            native_health: NativeSessionHealth::Healthy,
            context_tokens: None,
            context_window: None,
            context_percent: None,
            compaction_count: Some(0),
        }
    }
}

struct Runtime {
    child: Child,
    stdin: Option<Arc<Mutex<ChildStdin>>>,
    pending: Pending,
}

struct ProcessAdapter {
    runtime: Option<Runtime>,
    state: Arc<Mutex<State>>,
    event_tx: mpsc::Sender<AdapterResult<AdapterEvent>>,
    event_rx: Option<mpsc::Receiver<AdapterResult<AdapterEvent>>>,
    start_timeout: Duration,
    request_timeout: Duration,
    stop_timeout: Duration,
    next_id: u64,
}

impl ProcessAdapter {
    fn new() -> Self {
        let (event_tx, event_rx) = mpsc::channel(256);
        Self {
            runtime: None,
            state: Arc::new(Mutex::new(State::default())),
            event_tx,
            event_rx: Some(event_rx),
            start_timeout: START_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            stop_timeout: STOP_TIMEOUT,
            next_id: 1,
        }
    }

    fn with_timeouts(mut self, start: Duration, request: Duration, stop: Duration) -> Self {
        self.start_timeout = start;
        self.request_timeout = request;
        self.stop_timeout = stop;
        self
    }

    fn id(&mut self) -> String {
        let id = format!("host-{}", self.next_id);
        self.next_id += 1;
        id
    }

    async fn request(
        &mut self,
        kind: HarnessKind,
        mut payload: Value,
        id: String,
    ) -> AdapterResult<Value> {
        payload["id"] = Value::String(id.clone());
        let runtime = self
            .runtime
            .as_mut()
            .ok_or_else(|| operation(kind, "provider is not running"))?;
        let (tx, rx) = oneshot::channel();
        runtime.pending.lock().await.insert(id.clone(), tx);
        if let Err(error) = write_line(kind, runtime.stdin.as_ref(), &payload).await {
            runtime.pending.lock().await.remove(&id);
            return Err(delivery_ambiguous(kind, error.to_string()));
        }
        match timeout(self.request_timeout, rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(message))) => Err(delivery_ambiguous(kind, message)),
            Ok(Err(_)) => Err(delivery_ambiguous(kind, "provider response channel closed")),
            Err(_) => {
                runtime.pending.lock().await.remove(&id);
                Err(delivery_ambiguous(
                    kind,
                    format!("request {id} timed out after write; acceptance is unknown"),
                ))
            }
        }
    }

    async fn compatibility_request(
        &mut self,
        kind: HarnessKind,
        payload: Value,
        id: String,
    ) -> AdapterResult<Option<Value>> {
        match self.request(kind, payload, id).await {
            Ok(value) => Ok(Some(value)),
            Err(AdapterError::DeliveryAmbiguous { message, .. })
                if is_method_not_found(&message) =>
            {
                Ok(None)
            }
            Err(AdapterError::DeliveryAmbiguous { message, .. }) => Err(operation(kind, message)),
            Err(error) => Err(error),
        }
    }

    async fn stop(&mut self, kind: HarnessKind) -> AdapterResult<()> {
        let Some(mut runtime) = self.runtime.take() else {
            return Ok(());
        };
        self.state.lock().await.lifecycle = AdapterLifecycle::Stopping;
        runtime.stdin.take();
        match timeout(self.stop_timeout, runtime.child.wait()).await {
            Ok(Ok(status)) if status.success() => {
                self.state.lock().await.lifecycle = AdapterLifecycle::Stopped;
                Ok(())
            }
            Ok(Ok(status)) => {
                self.state.lock().await.lifecycle = AdapterLifecycle::Failed;
                Err(operation(kind, format!("process exited with {status}")))
            }
            Ok(Err(error)) => {
                self.state.lock().await.lifecycle = AdapterLifecycle::Failed;
                Err(operation(kind, format!("wait for process: {error}")))
            }
            Err(_) => {
                let _ = runtime.child.start_kill();
                self.state.lock().await.lifecycle = AdapterLifecycle::Failed;
                Err(operation(kind, "clean shutdown timed out"))
            }
        }
    }

    async fn snapshot(&self) -> AdapterSnapshot {
        let state = self.state.lock().await;
        AdapterSnapshot {
            lifecycle: state.lifecycle,
            session_id: state.session_id.clone(),
            thread_id: state.thread_id.clone(),
            active_turn_id: state.active_turn_id.clone(),
            steerable: state.lifecycle == AdapterLifecycle::Working,
            queued_input_count: state.queued_input_count,
            model: state.model.clone(),
            native_health: state.native_health,
            context_tokens: state.context_tokens,
            context_window: state.context_window,
            context_percent: state.context_percent,
            compaction_count: state.compaction_count,
        }
    }

    fn events(&mut self) -> AdapterEventStream {
        let Some(rx) = self.event_rx.take() else {
            return Box::pin(stream::empty());
        };
        Box::pin(stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        }))
    }
}

/// Process-backed Adapter for runtime-verified OMP RPC releases.
pub struct OmpProcessAdapter(ProcessAdapter);

impl Default for OmpProcessAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl OmpProcessAdapter {
    /// Creates an Adapter with the contract default timeouts.
    #[must_use]
    pub fn new() -> Self {
        Self(ProcessAdapter::new())
    }

    /// Overrides process timeouts, primarily for compatibility fixtures.
    #[must_use]
    pub fn with_timeouts(self, start: Duration, request: Duration, stop: Duration) -> Self {
        Self(self.0.with_timeouts(start, request, stop))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "OMP startup keeps the ordered native handshake and resume identity proof together"
    )]
    async fn start_session(
        &mut self,
        spec: &HarnessStartSpec,
        resume_session_id: Option<&str>,
    ) -> AdapterResult<NativeSession> {
        ensure_fresh(HarnessKind::Omp, self.0.runtime.as_ref())?;
        let observed_version = version(HarnessKind::Omp, &spec.executable).await?;
        tokio::fs::create_dir_all(&spec.provider_state_dir)
            .await
            .map_err(|error| operation(HarnessKind::Omp, error.to_string()))?;
        let session_dir = spec.provider_state_dir.join("provider-session");
        tokio::fs::create_dir_all(&session_dir)
            .await
            .map_err(|error| operation(HarnessKind::Omp, error.to_string()))?;
        let mut command = Command::new(&spec.executable);
        if let Some(profile) = &spec.provider_profile {
            command.arg("--profile").arg(profile);
        }
        if let Some(model) = &spec.model {
            command.arg("--model").arg(model);
        }
        if let Some(session_id) = resume_session_id {
            command.arg("--resume").arg(session_id);
        }
        command
            .args(["--mode", "rpc", "--cwd"])
            .arg(&spec.cwd)
            .arg("--session-dir")
            .arg(session_dir);
        for overlay in &spec.config_overlays {
            command.arg("--config").arg(overlay);
        }
        let (mut child, stdin, stdout) = spawn(&mut command, spec, HarnessKind::Omp)?;
        let mut lines = BufReader::new(stdout).lines();
        let line = timeout(self.0.start_timeout, lines.next_line())
            .await
            .map_err(|_| operation(HarnessKind::Omp, "ready frame timed out"))?
            .map_err(|error| operation(HarnessKind::Omp, error.to_string()))?
            .ok_or_else(|| operation(HarnessKind::Omp, "process exited before ready"))?;
        if classify_omp_frame(&line)? != OmpFrame::Ready {
            let _ = child.start_kill();
            return Err(operation(HarnessKind::Omp, "first frame was not ready"));
        }
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let stdin = Arc::new(Mutex::new(stdin));
        omp_reader(
            lines,
            Arc::clone(&pending),
            Arc::clone(&self.0.state),
            self.0.event_tx.clone(),
            Arc::downgrade(&stdin),
            coordinator_bridge(spec),
        );
        drain_stderr(child.stderr.take());
        self.0.runtime = Some(Runtime {
            child,
            stdin: Some(stdin),
            pending,
        });
        let id = self.0.id();
        self.0
            .request(
                HarnessKind::Omp,
                json!({"type":"set_host_tools","tools":mcp::omp_host_tools(spec.tier)}),
                id,
            )
            .await?;
        let id = self.0.id();
        let value = self
            .0
            .request(HarnessKind::Omp, json!({"type": "get_state"}), id)
            .await?;
        let session_id = field(&value, "sessionId");
        if let Some(expected) = resume_session_id
            && session_id.as_deref() != Some(expected)
        {
            return Err(operation(
                HarnessKind::Omp,
                format!(
                    "resumed Session identity mismatch: expected `{expected}`, observed `{}`",
                    session_id.as_deref().unwrap_or("missing")
                ),
            ));
        }
        let native = NativeSession {
            observed_version,
            session_id,
            thread_id: None,
            cwd: spec.cwd.clone(),
            model: compatible_omp_model(spec.model.as_deref(), model(&value)),
        };
        {
            let mut state = self.0.state.lock().await;
            state.lifecycle = AdapterLifecycle::Idle;
            state.session_id.clone_from(&native.session_id);
            state.model.clone_from(&native.model);
            state.queued_input_count = number(&value, "queuedMessageCount");
        }
        emit(
            &self.0.event_tx,
            AdapterEvent::SessionStarted(native.clone()),
        )
        .await;
        Ok(native)
    }
}

/// Process-backed Adapter for runtime-verified Codex App Server releases.
pub struct CodexProcessAdapter(ProcessAdapter);

impl Default for CodexProcessAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexProcessAdapter {
    /// Creates an Adapter with the contract default timeouts.
    #[must_use]
    pub fn new() -> Self {
        Self(ProcessAdapter::new())
    }

    /// Overrides process timeouts, primarily for compatibility fixtures.
    #[must_use]
    pub fn with_timeouts(self, start: Duration, request: Duration, stop: Duration) -> Self {
        Self(self.0.with_timeouts(start, request, stop))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Codex startup keeps the ordered native handshake and policy evidence together"
    )]
    async fn start_session(
        &mut self,
        spec: &HarnessStartSpec,
        resume_thread_id: Option<&str>,
    ) -> AdapterResult<NativeSession> {
        ensure_fresh(HarnessKind::Codex, self.0.runtime.as_ref())?;
        let observed_version = version(HarnessKind::Codex, &spec.executable).await?;
        tokio::fs::create_dir_all(&spec.provider_state_dir)
            .await
            .map_err(|error| operation(HarnessKind::Codex, error.to_string()))?;
        let mut command = Command::new(&spec.executable);
        if spec.provider_profile.is_some() {
            return Err(operation(
                HarnessKind::Codex,
                "Codex App Server does not accept CLI profiles; use a v3 launch profile with explicit approval_policy and sandbox_mode",
            ));
        }
        let coordinator = std::env::current_exe().map_err(|error| {
            operation(
                HarnessKind::Codex,
                format!("cannot resolve Coordinator MCP executable: {error}"),
            )
        })?;
        let command_value = serde_json::to_string(&coordinator.to_string_lossy())
            .map_err(|error| operation(HarnessKind::Codex, error.to_string()))?;
        command
            .arg("-c")
            .arg(format!("mcp_servers.herdr.command={command_value}"))
            .arg("-c")
            .arg("mcp_servers.herdr.args=[\"mcp\"]");
        command.args(["app-server", "--listen", "stdio://", "--strict-config"]);
        let (mut child, stdin, stdout) = spawn(&mut command, spec, HarnessKind::Codex)?;
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let stdin = Arc::new(Mutex::new(stdin));
        codex_reader(
            BufReader::new(stdout).lines(),
            Arc::clone(&pending),
            Arc::clone(&self.0.state),
            self.0.event_tx.clone(),
        );
        drain_stderr(child.stderr.take());
        self.0.runtime = Some(Runtime {
            child,
            stdin: Some(stdin),
            pending,
        });
        let id = self.0.id();
        timeout(
            self.0.start_timeout,
            self.0.request(
                HarnessKind::Codex,
                json!({"method":"initialize","params":{"clientInfo":{"name":"herdr_harness_coordinator","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}}}),
                id,
            ),
        )
        .await
        .map_err(|_| operation(HarnessKind::Codex, "initialize timed out"))??;
        write_line(
            HarnessKind::Codex,
            self.0
                .runtime
                .as_mut()
                .and_then(|runtime| runtime.stdin.as_ref()),
            &json!({"method":"initialized"}),
        )
        .await?;
        let approval_policy = spec.codex_approval_policy.ok_or_else(|| {
            operation(
                HarnessKind::Codex,
                "Codex App Server requires a v3 launch profile with approval_policy",
            )
        })?;
        let sandbox = spec.codex_sandbox_mode.ok_or_else(|| {
            operation(
                HarnessKind::Codex,
                "Codex App Server requires a v3 launch profile with sandbox_mode",
            )
        })?;
        let (method, params) = resume_thread_id.map_or_else(
            || {
                (
                    "thread/start",
                    json!({
                        "cwd":spec.cwd,
                        "model":spec.model,
                        "ephemeral":false,
                        "approvalPolicy":approval_policy,
                        "sandbox":sandbox
                    }),
                )
            },
            |thread_id| {
                (
                    "thread/resume",
                    json!({
                        "threadId":thread_id,
                        "cwd":spec.cwd,
                        "model":spec.model,
                        "approvalPolicy":approval_policy,
                        "sandbox":sandbox,
                        "excludeTurns":true
                    }),
                )
            },
        );
        let id = self.0.id();
        let value = self
            .0
            .request(
                HarnessKind::Codex,
                json!({"method":method,"params":params}),
                id,
            )
            .await?;
        let thread = value.get("thread").unwrap_or(&value);
        let thread_id = field(thread, "id")
            .ok_or_else(|| operation(HarnessKind::Codex, format!("{method} omitted thread id")))?;
        if let Some(expected) = resume_thread_id
            && thread_id != expected
        {
            return Err(operation(
                HarnessKind::Codex,
                format!(
                    "resumed thread identity mismatch: expected `{expected}`, observed `{thread_id}`"
                ),
            ));
        }
        verify_codex_mcp_readiness(&mut self.0, spec.tier, &thread_id).await?;
        let native = NativeSession {
            observed_version,
            session_id: field(thread, "sessionId"),
            thread_id: Some(thread_id),
            cwd: field(&value, "cwd")
                .or_else(|| field(thread, "cwd"))
                .map_or_else(|| spec.cwd.clone(), Into::into),
            model: field(&value, "model")
                .or_else(|| field(thread, "model"))
                .or_else(|| spec.model.clone()),
        };
        {
            let mut state = self.0.state.lock().await;
            state.lifecycle = AdapterLifecycle::Idle;
            state.session_id.clone_from(&native.session_id);
            state.thread_id.clone_from(&native.thread_id);
            state.model.clone_from(&native.model);
        }
        emit(
            &self.0.event_tx,
            AdapterEvent::SessionStarted(native.clone()),
        )
        .await;
        Ok(native)
    }
}

#[async_trait]
impl HarnessAdapter for OmpProcessAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Omp
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            persistent_session: true,
            active_turn_steering: true,
            active_turn_follow_up: true,
            cooperative_cancellation: true,
            safe_compaction: true,
        }
    }

    fn completion_tools(&self) -> WorkerCompletionTools {
        WorkerCompletionTools {
            attachment_create: "harness_attachment_create",
            complete: "harness_complete",
        }
    }

    async fn start(&mut self, spec: &HarnessStartSpec) -> AdapterResult<NativeSession> {
        self.start_session(spec, None).await
    }

    async fn resume(
        &mut self,
        spec: &HarnessStartSpec,
        target: &NativeSessionResume,
    ) -> AdapterResult<NativeSession> {
        let session_id = target.session_id.as_deref().ok_or_else(|| {
            operation(HarnessKind::Omp, "OMP resume requires a native Session ID")
        })?;
        self.start_session(spec, Some(session_id)).await
    }

    async fn conversation_contains(&mut self, marker: &str) -> AdapterResult<bool> {
        let id = self.0.id();
        let messages = self
            .0
            .request(HarnessKind::Omp, json!({"type":"get_messages"}), id)
            .await?;
        Ok(messages.to_string().contains(marker))
    }

    async fn dispatch(&mut self, delivery: ResolvedDelivery) -> AdapterResult<NativeAcceptance> {
        let command = match delivery.kind {
            NativeDeliveryKind::StartTurn => "prompt",
            NativeDeliveryKind::FollowUp => "follow_up",
            NativeDeliveryKind::Steer => "steer",
        };
        if delivery.kind == NativeDeliveryKind::StartTurn {
            self.0.state.lock().await.lifecycle = AdapterLifecycle::Working;
        }
        let value = self
            .0
            .request(
                HarnessKind::Omp,
                json!({"type": command, "message": delivery_text(&delivery)}),
                delivery.correlation.clone(),
            )
            .await;
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                self.0.state.lock().await.lifecycle = AdapterLifecycle::Failed;
                return Err(error);
            }
        };
        Ok(NativeAcceptance {
            correlation: delivery.correlation,
            turn_id: None,
            evidence: format!("OMP accepted {command}: {value}"),
        })
    }

    async fn cancel_active(&mut self) -> AdapterResult<()> {
        let id = self.0.id();
        self.0
            .request(HarnessKind::Omp, json!({"type": "abort"}), id)
            .await?;
        self.0.state.lock().await.lifecycle = AdapterLifecycle::Stopping;
        Ok(())
    }

    async fn stop(&mut self) -> AdapterResult<()> {
        self.0.stop(HarnessKind::Omp).await?;
        emit(
            &self.0.event_tx,
            AdapterEvent::Exited { exit_code: Some(0) },
        )
        .await;
        Ok(())
    }

    async fn snapshot(&mut self) -> AdapterResult<AdapterSnapshot> {
        let id = self.0.id();
        let value = self
            .0
            .request(HarnessKind::Omp, json!({"type": "get_state"}), id)
            .await?;
        {
            let mut state = self.0.state.lock().await;
            state.lifecycle = if boolean(&value, "isStreaming") {
                AdapterLifecycle::Working
            } else {
                AdapterLifecycle::Idle
            };
            state.session_id = field(&value, "sessionId");
            state.queued_input_count = number(&value, "queuedMessageCount");
            state.model = model(&value);
            let usage = value.get("contextUsage").unwrap_or(&Value::Null);
            state.context_tokens = unsigned(usage, "tokens");
            state.context_window = unsigned(usage, "contextWindow");
            state.context_percent = decimal(usage, "percent");
            state.native_health = match state.context_percent {
                Some(percent) if percent >= 70.0 => NativeSessionHealth::ContextPressure,
                _ => NativeSessionHealth::Healthy,
            };
        }
        Ok(self.0.snapshot().await)
    }

    async fn compact(&mut self) -> AdapterResult<()> {
        let id = self.0.id();
        self.0
            .request(HarnessKind::Omp, json!({"type": "compact"}), id)
            .await?;
        let mut state = self.0.state.lock().await;
        state.compaction_count = Some(state.compaction_count.unwrap_or_default() + 1);
        state.native_health = NativeSessionHealth::Compacted;
        Ok(())
    }

    fn events(&mut self) -> AdapterEventStream {
        self.0.events()
    }
}

fn compatible_omp_model(selected: Option<&str>, observed: Option<String>) -> Option<String> {
    match (selected, observed) {
        (Some(selected), Some(observed))
            if selected == observed || selected_omp_alias(selected) == observed =>
        {
            Some(selected.to_owned())
        }
        (_, Some(observed)) => Some(observed),
        (Some(selected), None) => Some(selected.to_owned()),
        (None, None) => None,
    }
}

fn selected_omp_alias(model: &str) -> &str {
    model
        .rsplit('/')
        .next()
        .unwrap_or(model)
        .split(':')
        .next()
        .unwrap_or(model)
}

async fn verify_codex_mcp_readiness(
    adapter: &mut ProcessAdapter,
    tier: crate::contract::HarnessTier,
    thread_id: &str,
) -> AdapterResult<()> {
    let id = adapter.id();
    let Some(status) = adapter
        .compatibility_request(
            HarnessKind::Codex,
            json!({"method":"mcpServerStatus/list","params":{
                "detail":"toolsAndAuthOnly",
                "threadId":thread_id
            }}),
            id,
        )
        .await?
    else {
        return Ok(());
    };
    let servers = status
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| operation(HarnessKind::Codex, "MCP status omitted data"))?;
    let tools = servers
        .iter()
        .find(|server| field(server, "name").as_deref() == Some("herdr"))
        .and_then(|server| server.get("tools"))
        .and_then(Value::as_object)
        .ok_or_else(|| operation(HarnessKind::Codex, "herdr MCP server is not ready"))?;
    let required: &[&str] = match tier {
        crate::contract::HarnessTier::Worker => &[
            "harness_list",
            "harness_status",
            "harness_inbox",
            "harness_request",
            "harness_send",
            "harness_complete",
            "harness_attachment_create",
        ],
        crate::contract::HarnessTier::Supervisor => &[
            "harness_status",
            "harness_inbox",
            "harness_supervisor_events",
            "harness_start",
            "harness_task_create",
            "harness_task_approve",
            "harness_supervisor_event_ack",
        ],
    };
    let missing = required
        .iter()
        .copied()
        .filter(|name| !tools.contains_key(*name))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(operation(
            HarnessKind::Codex,
            format!(
                "herdr MCP server is not ready; missing tools: {}",
                missing.join(", ")
            ),
        ))
    }
}

fn is_method_not_found(message: &str) -> bool {
    message.contains("\"code\":-32601")
        || message.contains("\"code\": -32601")
        || message.to_ascii_lowercase().contains("method not found")
}

#[async_trait]
impl HarnessAdapter for CodexProcessAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Codex
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            persistent_session: true,
            active_turn_steering: true,
            active_turn_follow_up: false,
            cooperative_cancellation: true,
            safe_compaction: false,
        }
    }

    fn completion_tools(&self) -> WorkerCompletionTools {
        WorkerCompletionTools {
            attachment_create: "tools.mcp__herdr__harness_attachment_create",
            complete: "tools.mcp__herdr__harness_complete",
        }
    }

    async fn start(&mut self, spec: &HarnessStartSpec) -> AdapterResult<NativeSession> {
        self.start_session(spec, None).await
    }

    async fn resume(
        &mut self,
        spec: &HarnessStartSpec,
        target: &NativeSessionResume,
    ) -> AdapterResult<NativeSession> {
        let thread_id = target.thread_id.as_deref().ok_or_else(|| {
            operation(
                HarnessKind::Codex,
                "Codex resume requires a native thread ID",
            )
        })?;
        self.start_session(spec, Some(thread_id)).await
    }

    async fn conversation_contains(&mut self, marker: &str) -> AdapterResult<bool> {
        let thread = self
            .0
            .state
            .lock()
            .await
            .thread_id
            .clone()
            .ok_or_else(|| operation(HarnessKind::Codex, "thread is not established"))?;
        let id = self.0.id();
        let snapshot = self
            .0
            .request(
                HarnessKind::Codex,
                json!({"method":"thread/read","params":{"threadId":thread,"includeTurns":true}}),
                id,
            )
            .await?;
        Ok(snapshot.to_string().contains(marker))
    }

    async fn dispatch(&mut self, delivery: ResolvedDelivery) -> AdapterResult<NativeAcceptance> {
        if delivery.kind == NativeDeliveryKind::FollowUp {
            return Err(operation(
                HarnessKind::Codex,
                "active FollowUp must remain queued",
            ));
        }
        let state = self.0.state.lock().await;
        let thread = state
            .thread_id
            .clone()
            .ok_or_else(|| operation(HarnessKind::Codex, "thread is not established"))?;
        let turn = state.active_turn_id.clone();
        drop(state);
        let input = codex_input(&delivery);
        let payload = match delivery.kind {
            NativeDeliveryKind::StartTurn => {
                json!({"method":"turn/start","params":{"threadId":thread,"input":input}})
            }
            NativeDeliveryKind::Steer => {
                let turn = turn.ok_or_else(|| operation(HarnessKind::Codex, "no active turn"))?;
                json!({"method":"turn/steer","params":{"threadId":thread,"expectedTurnId":turn,"input":input}})
            }
            NativeDeliveryKind::FollowUp => unreachable!("rejected above"),
        };
        let method = payload["method"].as_str().unwrap_or_default().to_owned();
        if delivery.kind == NativeDeliveryKind::StartTurn {
            self.0.state.lock().await.lifecycle = AdapterLifecycle::Working;
        }
        let id = self.0.id();
        let value = self.0.request(HarnessKind::Codex, payload, id).await;
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                self.0.state.lock().await.lifecycle = AdapterLifecycle::Failed;
                return Err(error);
            }
        };
        let turn_id = value
            .get("turn")
            .and_then(|turn| field(turn, "id"))
            .or_else(|| field(&value, "turnId"));
        if delivery.kind == NativeDeliveryKind::StartTurn {
            let mut state = self.0.state.lock().await;
            if state.lifecycle == AdapterLifecycle::Working {
                state.active_turn_id.clone_from(&turn_id);
            }
        }
        Ok(NativeAcceptance {
            correlation: delivery.correlation,
            turn_id,
            evidence: format!("Codex accepted {method}"),
        })
    }

    async fn cancel_active(&mut self) -> AdapterResult<()> {
        let state = self.0.state.lock().await;
        let thread = state
            .thread_id
            .clone()
            .ok_or_else(|| operation(HarnessKind::Codex, "thread is not established"))?;
        let turn = state
            .active_turn_id
            .clone()
            .ok_or_else(|| operation(HarnessKind::Codex, "no active turn"))?;
        drop(state);
        let id = self.0.id();
        self.0
            .request(
                HarnessKind::Codex,
                json!({"method":"turn/interrupt","params":{"threadId":thread,"turnId":turn}}),
                id,
            )
            .await?;
        self.0.state.lock().await.lifecycle = AdapterLifecycle::Stopping;
        Ok(())
    }

    async fn stop(&mut self) -> AdapterResult<()> {
        self.0.stop(HarnessKind::Codex).await?;
        emit(
            &self.0.event_tx,
            AdapterEvent::Exited { exit_code: Some(0) },
        )
        .await;
        Ok(())
    }

    async fn snapshot(&mut self) -> AdapterResult<AdapterSnapshot> {
        let thread = self
            .0
            .state
            .lock()
            .await
            .thread_id
            .clone()
            .ok_or_else(|| operation(HarnessKind::Codex, "thread is not established"))?;
        let id = self.0.id();
        self.0
            .request(
                HarnessKind::Codex,
                json!({"method":"thread/read","params":{"threadId":thread,"includeTurns":false}}),
                id,
            )
            .await?;
        Ok(self.0.snapshot().await)
    }

    fn events(&mut self) -> AdapterEventStream {
        self.0.events()
    }
}

async fn version(kind: HarnessKind, executable: &Path) -> AdapterResult<String> {
    let mut child = Command::new(executable)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| operation(kind, error.to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| operation(kind, "version check has no stdout"))?;
    let mut bytes = Vec::new();
    timeout(
        VERSION_TIMEOUT,
        stdout.take(MAX_VERSION_BYTES + 1).read_to_end(&mut bytes),
    )
    .await
    .map_err(|_| operation(kind, "version check timed out"))?
    .map_err(|error| operation(kind, error.to_string()))?;
    if bytes.len() as u64 > MAX_VERSION_BYTES {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return Err(operation(kind, "version output exceeds 4096 bytes"));
    }
    let status = timeout(VERSION_TIMEOUT, child.wait())
        .await
        .map_err(|_| operation(kind, "version check did not exit"))?
        .map_err(|error| operation(kind, error.to_string()))?;
    if !status.success() {
        return Err(operation(
            kind,
            format!("version check exited with {status}"),
        ));
    }
    let stdout = String::from_utf8(bytes).map_err(|error| operation(kind, error.to_string()))?;
    match kind {
        HarnessKind::Omp => validate_omp_version_output(&stdout),
        HarnessKind::Codex => validate_codex_version_output(&stdout),
    }
}

fn spawn(
    command: &mut Command,
    spec: &HarnessStartSpec,
    kind: HarnessKind,
) -> AdapterResult<(Child, ChildStdin, ChildStdout)> {
    command
        .current_dir(&spec.cwd)
        .env_clear()
        .envs(&spec.environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| operation(kind, error.to_string()))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| operation(kind, "missing stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| operation(kind, "missing stdout"))?;
    Ok((child, stdin, stdout))
}

async fn write_line(
    kind: HarnessKind,
    stdin: Option<&Arc<Mutex<ChildStdin>>>,
    value: &Value,
) -> AdapterResult<()> {
    let stdin = stdin.ok_or_else(|| operation(kind, "provider stdin is closed"))?;
    let mut stdin = stdin.lock().await;
    let mut bytes =
        serde_json::to_vec(value).map_err(|error| operation(kind, error.to_string()))?;
    bytes.push(b'\n');
    stdin
        .write_all(&bytes)
        .await
        .map_err(|error| operation(kind, error.to_string()))?;
    stdin
        .flush()
        .await
        .map_err(|error| operation(kind, error.to_string()))
}

fn omp_reader(
    mut lines: Lines<BufReader<ChildStdout>>,
    pending: Pending,
    state: Arc<Mutex<State>>,
    events: mpsc::Sender<AdapterResult<AdapterEvent>>,
    stdin: Weak<Mutex<ChildStdin>>,
    bridge: Option<McpServer>,
) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            match classify_omp_frame(&line) {
                Ok(OmpFrame::Response { id, result, .. }) => {
                    if let Some(tx) = pending.lock().await.remove(&id.to_string()) {
                        let _ = tx.send(result);
                    }
                }
                Ok(OmpFrame::SessionEvent {
                    event_type,
                    payload,
                }) => {
                    provider_event(HarnessKind::Omp, &event_type, &payload, &state, &events).await;
                }
                Ok(OmpFrame::ExtensionUiRequest { id, method, .. }) => {
                    send(
                        &events,
                        AdapterEvent::InputRequired {
                            correlation: Some(id.to_string()),
                            prompt: format!("OMP extension requested {method}"),
                        },
                    )
                    .await;
                }
                Ok(OmpFrame::HostToolCall {
                    id,
                    tool_name,
                    arguments,
                    ..
                }) => {
                    send(
                        &events,
                        AdapterEvent::Activity {
                            summary: format!("OMP host tool {tool_name} ({id})"),
                        },
                    )
                    .await;
                    let result = execute_host_tool(bridge.as_ref(), &tool_name, arguments).await;
                    let Some(stdin) = stdin.upgrade() else {
                        break;
                    };
                    if let Err(error) = write_line(
                        HarnessKind::Omp,
                        Some(&stdin),
                        &json!({
                            "type": "host_tool_result",
                            "id": id.to_string(),
                            "result": result.value,
                            "isError": result.is_error,
                        }),
                    )
                    .await
                    {
                        let _ = events.send(Err(error)).await;
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    let _ = events.send(Err(error)).await;
                }
            }
        }
        reader_failed(HarnessKind::Omp, pending, state, events).await;
    });
}

fn coordinator_bridge(spec: &HarnessStartSpec) -> Option<McpServer> {
    let socket = spec.environment.get("HERDR_COORDINATOR_SOCKET")?;
    let bearer = spec.environment.get("HERDR_HARNESS_CAPABILITY")?;
    mcp::from_host_bearer(Path::new(socket), bearer.clone())
        .ok()
        .map(|bridge| bridge.with_native_turn_id("provider-turn"))
}

struct HostToolResult {
    value: Value,
    is_error: bool,
}

async fn execute_host_tool(
    bridge: Option<&McpServer>,
    tool_name: &str,
    arguments: serde_json::Map<String, Value>,
) -> HostToolResult {
    let Some(bridge) = bridge else {
        return host_tool_error("Coordinator MCP identity is unavailable");
    };
    let response = bridge
        .handle(json!({
            "jsonrpc": "2.0",
            "id": "omp-host-tool",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }))
        .await;
    let Some(response) = response else {
        return host_tool_error("Coordinator MCP returned no correlated response");
    };
    let Some(result) = response.get("result") else {
        return host_tool_error(
            response
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("Coordinator MCP returned an invalid response"),
        );
    };
    HostToolResult {
        is_error: result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        value: result.clone(),
    }
}

fn host_tool_error(message: &str) -> HostToolResult {
    HostToolResult {
        value: json!({"content":[{"type":"text","text":message}],"details":{}}),
        is_error: true,
    }
}

fn codex_reader(
    mut lines: Lines<BufReader<ChildStdout>>,
    pending: Pending,
    state: Arc<Mutex<State>>,
    events: mpsc::Sender<AdapterResult<AdapterEvent>>,
) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            match classify_codex_frame(&line) {
                Ok(CodexFrame::Response { id, result }) => {
                    if let Some(tx) = pending.lock().await.remove(&id.to_string()) {
                        let _ = tx.send(result.map_err(|error| error.to_string()));
                    }
                }
                Ok(CodexFrame::Notification { method, params }) => {
                    let notification_thread = field(&params, "threadId");
                    let bound_thread = state.lock().await.thread_id.clone();
                    if notification_thread.is_none() || notification_thread == bound_thread {
                        provider_event(HarnessKind::Codex, &method, &params, &state, &events).await;
                    }
                }
                Ok(CodexFrame::ServerRequest { id, method, .. }) => {
                    send(
                        &events,
                        AdapterEvent::InputRequired {
                            correlation: Some(id.to_string()),
                            prompt: format!("Codex requested {method}"),
                        },
                    )
                    .await;
                }
                Err(error) => {
                    let _ = events.send(Err(error)).await;
                }
            }
        }
        reader_failed(HarnessKind::Codex, pending, state, events).await;
    });
}

async fn provider_event(
    kind: HarnessKind,
    event: &str,
    value: &Value,
    state: &Mutex<State>,
    events: &mpsc::Sender<AdapterResult<AdapterEvent>>,
) {
    if event == "agent_start" || event == "turn_start" || event == "turn/started" {
        let turn_id = value.get("turn").and_then(|turn| field(turn, "id"));
        let mut current = state.lock().await;
        current.lifecycle = AdapterLifecycle::Working;
        current.active_turn_id.clone_from(&turn_id);
        drop(current);
        send(events, AdapterEvent::TurnStarted { turn_id }).await;
    } else if event == "agent_end" || event == "turn/completed" {
        let turn = value.get("turn").unwrap_or(value);
        let turn_id = field(turn, "id");
        let status = match field(turn, "status").as_deref() {
            Some("interrupted" | "cancelled") => NativeTurnStatus::Interrupted,
            Some("failed") => NativeTurnStatus::Failed,
            _ => NativeTurnStatus::Completed,
        };
        let mut current = state.lock().await;
        current.lifecycle = AdapterLifecycle::Idle;
        current.active_turn_id = None;
        drop(current);
        send(events, AdapterEvent::TurnCompleted { turn_id, status }).await;
    } else if event.contains("agentMessage") || event == "message_end" {
        if let Some(text) = find_text(value) {
            send(events, AdapterEvent::Transcript { text }).await;
        }
    } else if event.contains("commandExecution") || event.contains("fileChange") {
        send(
            events,
            AdapterEvent::Activity {
                summary: event.to_owned(),
            },
        )
        .await;
    } else if event == "error" {
        state.lock().await.lifecycle = AdapterLifecycle::Failed;
        send(
            events,
            AdapterEvent::Failed {
                message: format!(
                    "{kind:?}: {}",
                    find_text(value).unwrap_or_else(|| value.to_string())
                ),
            },
        )
        .await;
    }
}

async fn reader_failed(
    kind: HarnessKind,
    pending: Pending,
    state: Arc<Mutex<State>>,
    events: mpsc::Sender<AdapterResult<AdapterEvent>>,
) {
    let expected_shutdown = state.lock().await.lifecycle == AdapterLifecycle::Stopping;
    for (_, tx) in pending.lock().await.drain() {
        let _ = tx.send(Err("provider stdout reached EOF".to_owned()));
    }
    if expected_shutdown {
        return;
    }
    state.lock().await.lifecycle = AdapterLifecycle::Failed;
    send(
        &events,
        AdapterEvent::Failed {
            message: format!("{kind:?} provider stdout reached EOF"),
        },
    )
    .await;
}

fn drain_stderr(stderr: Option<tokio::process::ChildStderr>) {
    if let Some(mut stderr) = stderr {
        tokio::spawn(async move {
            let mut bytes = Vec::new();
            let _ = stderr.read_to_end(&mut bytes).await;
        });
    }
}

fn delivery_text(delivery: &ResolvedDelivery) -> String {
    let mut text = delivery.text.clone();
    if !delivery.attachments.is_empty() {
        text.push_str("\n\nImmutable attachments:\n");
        for attachment in &delivery.attachments {
            let _ = writeln!(
                text,
                "- {} ({})",
                attachment.path.display(),
                attachment.media_type
            );
        }
    }
    text
}

fn codex_input(delivery: &ResolvedDelivery) -> Vec<Value> {
    let mut input = vec![json!({"type":"text","text":delivery_text(delivery)})];
    input.extend(
        delivery
            .attachments
            .iter()
            .filter(|item| item.media_type.starts_with("image/"))
            .map(|item| json!({"type":"localImage","path":item.path})),
    );
    input
}

fn field(value: &Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn model(value: &Value) -> Option<String> {
    value.get("model").and_then(|model| {
        model
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| field(model, "id").or_else(|| field(model, "modelId")))
    })
}

fn number(value: &Value, name: &str) -> Option<u32> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn unsigned(value: &Value, name: &str) -> Option<u64> {
    value.get(name).and_then(Value::as_u64)
}

fn decimal(value: &Value, name: &str) -> Option<f64> {
    value.get(name).and_then(Value::as_f64)
}

fn boolean(value: &Value, name: &str) -> bool {
    value.get(name).and_then(Value::as_bool).unwrap_or(false)
}

fn find_text(value: &Value) -> Option<String> {
    ["text", "message", "delta"]
        .into_iter()
        .find_map(|key| field(value, key))
        .or_else(|| {
            value
                .as_object()
                .and_then(|object| object.values().find_map(find_text))
        })
        .or_else(|| {
            value
                .as_array()
                .and_then(|array| array.iter().find_map(find_text))
        })
}

async fn emit(sender: &mpsc::Sender<AdapterResult<AdapterEvent>>, event: AdapterEvent) {
    let _ = sender.send(Ok(event)).await;
}

async fn send(sender: &mpsc::Sender<AdapterResult<AdapterEvent>>, event: AdapterEvent) {
    let _ = sender.send(Ok(event)).await;
}

fn ensure_fresh(kind: HarnessKind, runtime: Option<&Runtime>) -> AdapterResult<()> {
    if runtime.is_none() {
        Ok(())
    } else {
        Err(operation(kind, "Adapter is already started"))
    }
}

fn operation(kind: HarnessKind, message: impl Into<String>) -> AdapterError {
    AdapterError::Operation {
        kind,
        message: message.into(),
    }
}

fn delivery_ambiguous(kind: HarnessKind, message: impl Into<String>) -> AdapterError {
    AdapterError::DeliveryAmbiguous {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod model_tests {
    use super::compatible_omp_model;

    #[test]
    fn verified_omp_alias_retains_the_explicit_selection() {
        assert_eq!(
            compatible_omp_model(Some("kimi-code/k3:high"), Some("k3".to_owned())).as_deref(),
            Some("kimi-code/k3:high")
        );
    }

    #[test]
    fn different_provider_model_is_not_recorded_as_the_selection() {
        assert_eq!(
            compatible_omp_model(
                Some("kimi-code/k3:high"),
                Some("different-model".to_owned())
            )
            .as_deref(),
            Some("different-model")
        );
    }
}
