//! Identity-bound MCP 2025-06-18 stdio bridge for Coordinator tools.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{
    activation::{ActivationRegistry, DesiredActivation},
    broker::{BrokerOperation, BrokerRequest, BrokerResponse, call},
    contract::{
        HarnessDefinitionV1, HarnessId, HarnessTier, MessageSubmissionV1, ObservationCheckpoint,
        ResultManifestV1, SCHEMA_VERSION, SupervisorEventId, TaskId,
    },
    core::{
        ActorContext, CoordinatorCommand, CoordinatorQuery, HostConnectionCapability,
        SessionCapability, SupervisorEventResolution,
    },
    herdr::{HerdrSocketClient, PluginPaneOpenParams},
    profile::ProfileRegistry,
};

/// MCP revision implemented by the stdio bridge.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const REQUIRED_WORKER_TOOLS: [&str; 7] = [
    "harness_list",
    "harness_status",
    "harness_inbox",
    "harness_request",
    "harness_send",
    "harness_complete",
    "harness_attachment_create",
];

/// One identity-bound stdio MCP server.
#[derive(Debug, Clone)]
pub struct McpServer {
    socket: PathBuf,
    actor: ActorContext,
    workspace_state_dir: Option<PathBuf>,
    herdr_socket: Option<PathBuf>,
    native_turn_id: Option<String>,
}

impl McpServer {
    /// Creates a bridge whose every call is attributed to one Harness Session.
    #[must_use]
    pub fn new(socket: PathBuf, capability: SessionCapability) -> Self {
        Self {
            socket,
            actor: ActorContext::Session { capability },
            workspace_state_dir: None,
            herdr_socket: None,
            native_turn_id: None,
        }
    }

    /// Creates a bridge with an explicit durable workspace state directory.
    #[must_use]
    pub fn for_workspace(
        socket: PathBuf,
        capability: SessionCapability,
        workspace_state_dir: PathBuf,
    ) -> Self {
        Self {
            socket,
            actor: ActorContext::Session { capability },
            workspace_state_dir: Some(workspace_state_dir),
            herdr_socket: None,
            native_turn_id: None,
        }
    }

    /// Creates a bridge fenced to one current Host connection generation.
    #[must_use]
    pub fn for_host(
        socket: PathBuf,
        capability: HostConnectionCapability,
        workspace_state_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            socket,
            actor: ActorContext::Host { capability },
            workspace_state_dir,
            herdr_socket: None,
            native_turn_id: None,
        }
    }

    /// Binds Herdr pane operations to the socket that identifies this workspace session.
    #[must_use]
    pub fn with_herdr_socket(mut self, herdr_socket: PathBuf) -> Self {
        self.herdr_socket = Some(herdr_socket);
        self
    }

    /// Correlates provider host-tool completion when its protocol omits turn IDs.
    #[must_use]
    pub fn with_native_turn_id(mut self, native_turn_id: impl Into<String>) -> Self {
        self.native_turn_id = Some(native_turn_id.into());
        self
    }

    /// Serves newline-delimited JSON-RPC messages on stdin/stdout.
    ///
    /// # Errors
    ///
    /// Returns an error for stdio or response encoding failure.
    pub async fn run_stdio(&self) -> Result<()> {
        let mut input = BufReader::new(tokio::io::stdin());
        let mut output = tokio::io::stdout();
        loop {
            let mut frame = Vec::new();
            let read = input
                .read_until(b'\n', &mut frame)
                .await
                .context("reading MCP frame")?;
            if read == 0 {
                return Ok(());
            }
            if frame.len() > crate::broker::MAX_BROKER_FRAME_BYTES {
                write_json(
                    &mut output,
                    &protocol_error(Value::Null, -32600, "MCP frame exceeds 1 MiB"),
                )
                .await?;
                continue;
            }
            let request: Value = match serde_json::from_slice(&frame) {
                Ok(request) => request,
                Err(error) => {
                    write_json(
                        &mut output,
                        &protocol_error(Value::Null, -32700, &error.to_string()),
                    )
                    .await?;
                    continue;
                }
            };
            if let Some(response) = self.handle(request).await {
                write_json(&mut output, &response).await?;
            }
        }
    }

    /// Handles one decoded MCP message. Notifications return `None`.
    pub async fn handle(&self, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        id.as_ref()?;
        let id = id.unwrap_or(Value::Null);
        let result = match method {
            "initialize" => Ok(json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "herdr-harness-coordinator", "version": env!("CARGO_PKG_VERSION")},
                "instructions": "Use these tools only for the current identity-bound Harness Session."
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({"tools": tools()})),
            "tools/call" => {
                self.call_tool(request.get("params").cloned().unwrap_or(Value::Null))
                    .await
            }
            _ => return Some(protocol_error(id, -32601, "method not found")),
        };
        Some(match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(error) => json!({
                "jsonrpc":"2.0",
                "id":id,
                "result": {"content":[{"type":"text","text":format!("{error:#}")}],"isError":true}
            }),
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive MCP name-to-Core-operation authorization map"
    )]
    async fn call_tool(&self, params: Value) -> Result<Value> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .context("tool name is required")?;
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if name == "harness_start" {
            return self.start_worker(arguments).await;
        }
        if name == "harness_attachment_create" {
            return self.create_inline_attachment(arguments).await;
        }
        let operation = match name {
            "harness_list" => query(CoordinatorQuery::HarnessStatus),
            "harness_status" => query(CoordinatorQuery::ListTasks),
            "harness_task_graph" => query(CoordinatorQuery::TaskGraph),
            "harness_inbox" => query(CoordinatorQuery::Inbox),
            "harness_supervisor_events" => query(CoordinatorQuery::SupervisorEvents),
            "harness_task_create" => execute(CoordinatorCommand::CreateTask {
                submission: serde_json::from_value(arguments)
                    .context("invalid TaskSubmissionV1")?,
            }),
            "harness_send" | "harness_request" => execute(CoordinatorCommand::SendMessage {
                submission: serde_json::from_value::<MessageSubmissionV1>(arguments)
                    .context("invalid MessageSubmissionV1")?,
            }),
            "harness_complete" => self.complete_operation(arguments)?,
            "harness_task_approve" => {
                let args: ApproveArgs =
                    serde_json::from_value(arguments).context("invalid Approval arguments")?;
                execute(CoordinatorCommand::ApproveTask {
                    task_id: args.task_id,
                    result_revision: args.result_revision,
                    observation_digest: args.observation_digest,
                })
            }
            "harness_repository_observe" => {
                let args: ObserveArgs = serde_json::from_value(arguments)
                    .context("invalid repository observation arguments")?;
                execute(CoordinatorCommand::CaptureRepositoryObservation {
                    task_id: args.task_id,
                    checkpoint: args.checkpoint,
                })
            }
            "harness_task_cancel" => {
                let args: TaskArgs =
                    serde_json::from_value(arguments).context("invalid cancellation arguments")?;
                execute(CoordinatorCommand::CancelTask {
                    task_id: args.task_id,
                })
            }
            "harness_hold_clear" => {
                let args: HoldClearArgs = serde_json::from_value(arguments)
                    .context("invalid Hold clearance arguments")?;
                execute(CoordinatorCommand::ClearWorktreeHold {
                    task_id: args.task_id,
                    observation_digest: args.observation_digest,
                    audit_note: args.audit_note,
                })
            }
            "harness_supervisor_event_ack" => {
                let args: SupervisorEventAckArgs = serde_json::from_value(arguments)
                    .context("invalid Supervisor event acknowledgement arguments")?;
                execute(CoordinatorCommand::AcknowledgeSupervisorEvents {
                    event_ids: args.event_ids,
                })
            }
            "harness_supervisor_event_reconcile" => {
                let args: SupervisorEventReconcileArgs = serde_json::from_value(arguments)
                    .context("invalid Supervisor event reconciliation arguments")?;
                execute(CoordinatorCommand::ReconcileSupervisorEvent {
                    event_id: args.event_id,
                    resolution: args.resolution,
                    audit_note: args.audit_note,
                })
            }
            "harness_task_graph_watch" => {
                let args: TaskGraphWatchArgs = serde_json::from_value(arguments)
                    .context("invalid Task graph watch arguments")?;
                execute(CoordinatorCommand::WatchTaskGraph {
                    root_task_ids: args.root_task_ids,
                    request_key: args.request_key,
                })
            }
            "harness_stop" => {
                let args: StopArgs =
                    serde_json::from_value(arguments).context("invalid Worker stop arguments")?;
                execute(CoordinatorCommand::StopWorker {
                    worker_id: args.worker_id,
                })
            }
            _ => bail!("unknown Coordinator tool `{name}`"),
        };
        let response = call(
            &self.socket,
            &BrokerRequest {
                schema_version: SCHEMA_VERSION,
                request_id: uuid::Uuid::now_v7().to_string(),
                operation: match operation {
                    ToolOperation::Execute(command) => BrokerOperation::Execute {
                        actor: self.actor.clone(),
                        command,
                    },
                    ToolOperation::Query(query) => BrokerOperation::Query {
                        actor: self.actor.clone(),
                        query,
                    },
                },
            },
        )
        .await?;
        tool_result(response)
    }

    async fn create_inline_attachment(&self, arguments: Value) -> Result<Value> {
        let args: InlineAttachmentArgs =
            serde_json::from_value(arguments).context("invalid inline Attachment arguments")?;
        if args.content.is_empty() || args.content.len() > 512 * 1024 {
            bail!("inline Attachment content must contain 1 to 524288 UTF-8 bytes");
        }
        if args.media_type.is_empty() || args.media_type.len() > 255 {
            bail!("inline Attachment media_type must contain 1 to 255 bytes");
        }
        if args.original_name.is_empty() || args.original_name.len() > 255 {
            bail!("inline Attachment original_name must contain 1 to 255 bytes");
        }
        let state_dir = self
            .workspace_state_dir
            .as_deref()
            .or_else(|| self.socket.parent())
            .context("Coordinator socket has no state directory")?;
        let temporary_dir = state_dir.join("tmp");
        tokio::fs::create_dir_all(&temporary_dir).await?;
        let temporary =
            temporary_dir.join(format!("inline-attachment-{}.tmp", uuid::Uuid::now_v7()));
        tokio::fs::write(&temporary, args.content).await?;
        let response = call(
            &self.socket,
            &BrokerRequest {
                schema_version: SCHEMA_VERSION,
                request_id: uuid::Uuid::now_v7().to_string(),
                operation: BrokerOperation::Execute {
                    actor: self.actor.clone(),
                    command: CoordinatorCommand::AdmitAttachment {
                        source: temporary.clone(),
                        media_type: args.media_type,
                        original_name: args.original_name,
                    },
                },
            },
        )
        .await;
        let _ = tokio::fs::remove_file(temporary).await;
        tool_result(response?)
    }

    fn complete_operation(&self, arguments: Value) -> Result<ToolOperation> {
        let args: CompleteArgs =
            serde_json::from_value(arguments).context("invalid completion arguments")?;
        let native_turn_id = self
            .native_turn_id
            .clone()
            .or(args.native_turn_id)
            .context("native turn ID is required for this provider")?;
        Ok(execute(CoordinatorCommand::CompleteTask {
            manifest: args.manifest,
            native_turn_id,
        }))
    }

    async fn start_worker(&self, arguments: Value) -> Result<Value> {
        let args: StartArgs =
            serde_json::from_value(arguments).context("invalid Worker start arguments")?;
        let (definition, profile_snapshot, profile_digest, workspace_id) =
            self.resolve_worker_start(&args.worker_id).await?;
        let worker_id = definition.id.clone();
        let request = BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: uuid::Uuid::now_v7().to_string(),
            operation: BrokerOperation::Execute {
                actor: self.actor.clone(),
                command: CoordinatorCommand::StartWorker {
                    definition: definition.clone(),
                    profile_snapshot,
                    profile_digest,
                },
            },
        };
        let mut last_error = None;
        let mut response = None;
        for _ in 0..3 {
            match call(&self.socket, &request).await {
                Ok(value) => {
                    response = Some(value);
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        let response = response.ok_or_else(|| {
            last_error.expect("three failed broker attempts retain the final error")
        })?;
        if let Some(error) = response.error {
            bail!("Coordinator {:?}: {}", error.category, error.message);
        }
        let launch = async {
            let structured = response
                .result
                .context("Coordinator response omitted Worker start result")?;
            let outcome: crate::core::CommandOutcome = serde_json::from_value(structured.clone())?;
            let crate::core::CommandOutcome::WorkerStarted { capability, .. } = outcome else {
                bail!("Coordinator returned the wrong Worker start outcome")
            };
            let bearer = serde_json::to_value(capability)?
                .as_str()
                .context("Session capability did not serialize as a bearer")?
                .to_owned();
            self.open_worker_pane(&bearer, &definition, workspace_id)
                .await?;
            let public = json!({"worker_id": worker_id, "presence": "starting"});
            Ok(json!({
                "content": [{"type":"text","text":serde_json::to_string_pretty(&public)?}],
                "structuredContent": public,
                "isError": false
            }))
        }
        .await;
        if let Err(error) = &launch {
            let _ = call(
                &self.socket,
                &BrokerRequest {
                    schema_version: SCHEMA_VERSION,
                    request_id: uuid::Uuid::now_v7().to_string(),
                    operation: BrokerOperation::Execute {
                        actor: self.actor.clone(),
                        command: CoordinatorCommand::AbortWorkerStart {
                            worker_id: worker_id.clone(),
                            diagnostic: error.to_string(),
                        },
                    },
                },
            )
            .await;
        }
        launch
    }

    async fn open_worker_pane(
        &self,
        bearer: &str,
        definition: &HarnessDefinitionV1,
        workspace_id: String,
    ) -> Result<()> {
        let socket_path = self
            .herdr_socket
            .clone()
            .or_else(|| std::env::var_os("HERDR_SOCKET_PATH").map(PathBuf::from))
            .context("Herdr socket is required to open a Worker pane")?;
        let mut pane = PluginPaneOpenParams::worker(bearer, &definition.cwd, Some(workspace_id));
        let state_dir = self
            .workspace_state_dir
            .as_deref()
            .or_else(|| self.socket.parent())
            .context("Coordinator socket has no state directory")?;
        pane.env.insert(
            "HERDR_COORDINATOR_STATE_DIR".to_owned(),
            state_dir.to_string_lossy().into_owned(),
        );
        pane.env.insert(
            "HERDR_COORDINATOR_SOCKET".to_owned(),
            self.socket.to_string_lossy().into_owned(),
        );
        pane.env.insert(
            "HERDR_COORDINATOR_BIN".to_owned(),
            std::env::current_exe()?.to_string_lossy().into_owned(),
        );
        HerdrSocketClient::new(socket_path)
            .open_worker(pane)
            .await
            .context("opening Herdr Worker pane")?;
        Ok(())
    }

    async fn resolve_worker_start(
        &self,
        worker_id: &HarnessId,
    ) -> Result<(HarnessDefinitionV1, String, String, String)> {
        let workspace_state = self.workspace_state_dir.as_deref().unwrap_or_else(|| {
            self.socket
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
        });
        let workspaces = workspace_state
            .parent()
            .context("workspace state has no workspaces directory")?;
        let plugin_state = workspaces
            .parent()
            .context("workspaces directory has no plugin state root")?;
        let activation = ActivationRegistry::open(plugin_state).await?;
        let view = activation
            .find_by_state_dir(workspace_state)
            .await?
            .filter(|view| view.desired == DesiredActivation::On)
            .context("this Coordinator state directory is not enabled for a workspace")?;
        let selection = view
            .selection
            .context("enabled workspace has no Harness selection")?;
        let worker = selection
            .workers
            .iter()
            .find(|worker| &worker.worker_id == worker_id)
            .with_context(|| format!("Worker `{worker_id}` is not selected for this workspace"))?;
        let config_dir = plugin_config_dir(plugin_state);
        let profiles = ProfileRegistry::load(&config_dir.join("profiles"))?;
        let resolved = profiles.resolve_selected(&worker.profile_id, std::env::vars())?;
        let definition = HarnessDefinitionV1 {
            schema_version: SCHEMA_VERSION,
            id: worker.worker_id.clone(),
            kind: resolved.profile.kind,
            tier: HarnessTier::Worker,
            cwd: view.repository_root,
            launch_profile: Some(resolved.profile.id.to_string()),
            model: resolved.profile.model.clone(),
        };
        Ok((
            definition,
            resolved.snapshot,
            resolved.digest,
            view.workspace_id,
        ))
    }
}

fn plugin_config_dir(plugin_state: &Path) -> PathBuf {
    std::env::var_os("HERDR_PLUGIN_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config"))
                })
                .map(|config| {
                    config
                        .join("herdr/plugins/config")
                        .join("herdr-harness-coordinator")
                })
        })
        .unwrap_or_else(|| plugin_state.join("config"))
}

/// Verifies the identity-bound Worker tool surface before a native Harness becomes online.
///
/// # Errors
///
/// Returns an error when the local bridge omits any required Worker operation.
pub async fn verify_required_worker_tools(
    socket: &Path,
    capability: SessionCapability,
) -> Result<()> {
    let server = McpServer::new(socket.to_path_buf(), capability);
    let response = server
        .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await
        .context("tools/list unexpectedly returned no response")?;
    let tools = response
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .context("tools/list omitted its tool array")?;
    for required in REQUIRED_WORKER_TOOLS {
        if !tools
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(required))
        {
            bail!("Coordinator MCP bridge omitted required tool `{required}`");
        }
    }
    Ok(())
}

#[expect(
    clippy::large_enum_variant,
    reason = "short-lived typed routing value avoids heap allocation at every MCP call"
)]
enum ToolOperation {
    Execute(CoordinatorCommand),
    Query(CoordinatorQuery),
}

fn execute(command: CoordinatorCommand) -> ToolOperation {
    ToolOperation::Execute(command)
}

fn query(query: CoordinatorQuery) -> ToolOperation {
    ToolOperation::Query(query)
}

#[derive(Deserialize)]
struct CompleteArgs {
    manifest: ResultManifestV1,
    native_turn_id: Option<String>,
}

#[derive(Deserialize)]
struct StartArgs {
    worker_id: HarnessId,
}

#[derive(Deserialize)]
struct InlineAttachmentArgs {
    content: String,
    media_type: String,
    original_name: String,
}

#[derive(Deserialize)]
struct ApproveArgs {
    task_id: TaskId,
    result_revision: u32,
    observation_digest: String,
}

#[derive(Deserialize)]
struct ObserveArgs {
    task_id: TaskId,
    checkpoint: ObservationCheckpoint,
}

#[derive(Deserialize)]
struct TaskArgs {
    task_id: TaskId,
}

#[derive(Deserialize)]
struct HoldClearArgs {
    task_id: TaskId,
    observation_digest: String,
    audit_note: String,
}

#[derive(Deserialize)]
struct StopArgs {
    worker_id: HarnessId,
}

#[derive(Deserialize)]
struct SupervisorEventAckArgs {
    event_ids: Vec<SupervisorEventId>,
}

#[derive(Deserialize)]
struct SupervisorEventReconcileArgs {
    event_id: SupervisorEventId,
    resolution: SupervisorEventResolution,
    audit_note: String,
}

#[derive(Deserialize)]
struct TaskGraphWatchArgs {
    root_task_ids: Vec<TaskId>,
    request_key: Option<String>,
}

fn tool_result(response: BrokerResponse) -> Result<Value> {
    if let Some(error) = response.error {
        bail!("Coordinator {:?}: {}", error.category, error.message);
    }
    let structured = response
        .result
        .context("Coordinator response omitted result")?;
    Ok(json!({
        "content": [{"type":"text","text":serde_json::to_string_pretty(&structured)?}],
        "structuredContent": structured,
        "isError": false
    }))
}

fn tools() -> Vec<Value> {
    let empty = json!({"type":"object","additionalProperties":false});
    let passthrough = json!({"type":"object","additionalProperties":true});
    vec![
        tool(
            "harness_list",
            "List durable Harnesses and live status.",
            empty.clone(),
        ),
        tool(
            "harness_status",
            "List durable Tasks and lifecycle states.",
            empty.clone(),
        ),
        tool(
            "harness_task_graph",
            "Inspect Task dependencies, blockers, bound Result revisions, and Worker queue positions.",
            empty.clone(),
        ),
        tool(
            "harness_inbox",
            "Read unread Messages for this Harness.",
            empty.clone(),
        ),
        tool(
            "harness_supervisor_events",
            "Inspect durable Supervisor event delivery and processing state.",
            empty,
        ),
        tool(
            "harness_start",
            "Start one explicitly selected Worker by its durable ID.",
            json!({"type":"object","required":["worker_id"],"properties":{"worker_id":{"type":"string"}},"additionalProperties":false}),
        ),
        tool(
            "harness_task_create",
            "Create a bounded Task for an explicit Worker.",
            passthrough.clone(),
        ),
        tool(
            "harness_send",
            "Send a routed Reply, Correction, or Notification.",
            passthrough.clone(),
        ),
        tool(
            "harness_request",
            "Send a blocking Worker Question to the Supervisor.",
            passthrough.clone(),
        ),
        tool(
            "harness_complete",
            "Submit one Result candidate for the current native turn.",
            passthrough.clone(),
        ),
        tool(
            "harness_attachment_create",
            "Create one immutable, Coordinator-owned text evidence Attachment and return its Attachment ID.",
            json!({"type":"object","required":["content","media_type","original_name"],"properties":{"content":{"type":"string","minLength":1,"maxLength":524_288},"media_type":{"type":"string","minLength":1,"maxLength":255},"original_name":{"type":"string","minLength":1,"maxLength":255}},"additionalProperties":false}),
        ),
        tool(
            "harness_repository_observe",
            "Capture trusted Git evidence and return its digest.",
            json!({"type":"object","required":["task_id","checkpoint"],"properties":{"task_id":{"type":"string"},"checkpoint":{"type":"string","enum":["before_dispatch","result","cancel","failure","approval","hold_clear"]}},"additionalProperties":false}),
        ),
        tool(
            "harness_task_approve",
            "Approve the current Result against repository evidence.",
            passthrough.clone(),
        ),
        tool(
            "harness_task_cancel",
            "Cancel a queued or active Task.",
            passthrough.clone(),
        ),
        tool(
            "harness_hold_clear",
            "Clear a digest-confirmed Worktree Hold without editing files.",
            passthrough,
        ),
        tool(
            "harness_supervisor_event_ack",
            "Acknowledge retry-safe durable Supervisor events as processed.",
            json!({"type":"object","required":["event_ids"],"properties":{"event_ids":{"type":"array","minItems":1,"maxItems":32,"uniqueItems":true,"items":{"type":"string","format":"uuid"}}},"additionalProperties":false}),
        ),
        tool(
            "harness_supervisor_event_reconcile",
            "Explicitly reconcile an Unknown native Supervisor injection.",
            json!({"type":"object","required":["event_id","resolution","audit_note"],"properties":{"event_id":{"type":"string","format":"uuid"},"resolution":{"type":"string","enum":["retry","processed","cancel"]},"audit_note":{"type":"string","minLength":1,"maxLength":4096}},"additionalProperties":false}),
        ),
        tool(
            "harness_task_graph_watch",
            "Register the explicit root Tasks whose review or terminal completion should wake the Supervisor.",
            json!({"type":"object","required":["root_task_ids"],"properties":{"root_task_ids":{"type":"array","minItems":1,"maxItems":32,"uniqueItems":true,"items":{"type":"string","format":"uuid"}},"request_key":{"type":["string","null"]}},"additionalProperties":false}),
        ),
        tool(
            "harness_stop",
            "Stop one explicit Worker Host after settling active cancellation.",
            json!({"type":"object","required":["worker_id"],"properties":{"worker_id":{"type":"string"}},"additionalProperties":false}),
        ),
    ]
}

/// OMP RPC declarations for the Worker-safe Coordinator MCP tools.
pub(crate) fn omp_host_tools(tier: HarnessTier) -> Vec<Value> {
    tools()
        .into_iter()
        .filter(|tool| {
            tool.get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| {
                    tier == HarnessTier::Supervisor || REQUIRED_WORKER_TOOLS.contains(&name)
                })
        })
        .map(|tool| {
            json!({
                "name": tool["name"],
                "label": tool["name"],
                "description": tool["description"],
                "parameters": tool["inputSchema"],
            })
        })
        .collect()
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the schema Value is moved directly into the JSON result"
)]
fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({"name":name,"description":description,"inputSchema":input_schema})
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the correlation Value is moved directly into the JSON result"
)]
fn protocol_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

async fn write_json(output: &mut tokio::io::Stdout, value: &Value) -> Result<()> {
    let mut frame = serde_json::to_vec(value)?;
    frame.push(b'\n');
    output
        .write_all(&frame)
        .await
        .context("writing MCP frame")?;
    output.flush().await.context("flushing MCP frame")
}

/// Convenience constructor used by the CLI.
///
/// # Errors
///
/// Returns an error when the Session bearer does not match the v1 capability shape.
pub fn from_bearer(socket: &Path, bearer: String) -> Result<McpServer> {
    Ok(McpServer::new(
        socket.to_path_buf(),
        SessionCapability::from_bearer(bearer)?,
    ))
}

/// Creates a provider bridge fenced to a current Host connection generation.
///
/// # Errors
///
/// Returns an error when the bearer is not a valid Host connection capability.
pub fn from_host_bearer(socket: &Path, bearer: String) -> Result<McpServer> {
    Ok(McpServer::for_host(
        socket.to_path_buf(),
        HostConnectionCapability::from_bearer(bearer)?,
        None,
    ))
}

/// Convenience constructor for short runtime sockets outside the durable state directory.
///
/// # Errors
///
/// Returns an error when the bearer is not a valid Session capability.
pub fn from_bearer_for_workspace(
    socket: &Path,
    bearer: String,
    workspace_state_dir: PathBuf,
) -> Result<McpServer> {
    Ok(McpServer::for_workspace(
        socket.to_path_buf(),
        SessionCapability::from_bearer(bearer)?,
        workspace_state_dir,
    ))
}

/// Creates a workspace-aware provider bridge fenced to one Host generation.
///
/// # Errors
///
/// Returns an error when the bearer is not a valid Host connection capability.
pub fn from_host_bearer_for_workspace(
    socket: &Path,
    bearer: String,
    workspace_state_dir: PathBuf,
) -> Result<McpServer> {
    Ok(McpServer::for_host(
        socket.to_path_buf(),
        HostConnectionCapability::from_bearer(bearer)?,
        Some(workspace_state_dir),
    ))
}
