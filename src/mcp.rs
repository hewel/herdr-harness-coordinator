//! Identity-bound MCP 2025-06-18 stdio bridge for Coordinator tools.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{
    broker::{BrokerOperation, BrokerRequest, BrokerResponse, call},
    contract::{
        HarnessDefinitionV1, HarnessId, MessageSubmissionV1, ObservationCheckpoint,
        ResultManifestV1, SCHEMA_VERSION, TaskId,
    },
    core::{ActorContext, CoordinatorCommand, CoordinatorQuery, SessionCapability},
    herdr::{HerdrSocketClient, PluginPaneOpenParams},
};

/// MCP revision implemented by the stdio bridge.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const REQUIRED_WORKER_TOOLS: [&str; 6] = [
    "harness_list",
    "harness_status",
    "harness_inbox",
    "harness_request",
    "harness_send",
    "harness_complete",
];

/// One identity-bound stdio MCP server.
#[derive(Debug, Clone)]
pub struct McpServer {
    socket: PathBuf,
    capability: SessionCapability,
}

impl McpServer {
    /// Creates a bridge whose every call is attributed to one Harness Session.
    #[must_use]
    pub fn new(socket: PathBuf, capability: SessionCapability) -> Self {
        Self { socket, capability }
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
                "result": {"content":[{"type":"text","text":error.to_string()}],"isError":true}
            }),
        })
    }

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
        let operation = match name {
            "harness_list" => query(CoordinatorQuery::HarnessStatus),
            "harness_status" => query(CoordinatorQuery::ListTasks),
            "harness_inbox" => query(CoordinatorQuery::Inbox),
            "harness_task_create" => execute(CoordinatorCommand::CreateTask {
                submission: serde_json::from_value(arguments)
                    .context("invalid TaskSubmissionV1")?,
            }),
            "harness_send" | "harness_request" => execute(CoordinatorCommand::SendMessage {
                submission: serde_json::from_value::<MessageSubmissionV1>(arguments)
                    .context("invalid MessageSubmissionV1")?,
            }),
            "harness_complete" => {
                let args: CompleteArgs =
                    serde_json::from_value(arguments).context("invalid completion arguments")?;
                execute(CoordinatorCommand::CompleteTask {
                    manifest: args.manifest,
                    native_turn_id: args.native_turn_id,
                })
            }
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
                        actor: ActorContext::Session {
                            capability: self.capability.clone(),
                        },
                        command,
                    },
                    ToolOperation::Query(query) => BrokerOperation::Query {
                        actor: ActorContext::Session {
                            capability: self.capability.clone(),
                        },
                        query,
                    },
                },
            },
        )
        .await?;
        tool_result(response)
    }

    async fn start_worker(&self, arguments: Value) -> Result<Value> {
        let args: StartArgs =
            serde_json::from_value(arguments).context("invalid Worker start arguments")?;
        let definition = args.definition.clone();
        let worker_id = definition.id.clone();
        let request = BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: uuid::Uuid::now_v7().to_string(),
            operation: BrokerOperation::Execute {
                actor: ActorContext::Session {
                    capability: self.capability.clone(),
                },
                command: CoordinatorCommand::StartWorker {
                    definition: args.definition,
                    profile_snapshot: args.profile_snapshot,
                    profile_digest: args.profile_digest,
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
            let socket_path = std::env::var_os("HERDR_SOCKET_PATH")
                .map(PathBuf::from)
                .context("HERDR_SOCKET_PATH is required to open a Worker pane")?;
            HerdrSocketClient::new(socket_path)
                .open_worker(PluginPaneOpenParams::worker(&bearer, &definition.cwd, None))
                .await
                .context("opening Herdr Worker pane")?;
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
                        actor: ActorContext::Session {
                            capability: self.capability.clone(),
                        },
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
    native_turn_id: String,
}

#[derive(Deserialize)]
struct StartArgs {
    definition: HarnessDefinitionV1,
    profile_snapshot: String,
    profile_digest: String,
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
            "harness_inbox",
            "Read unread Messages for this Harness.",
            empty,
        ),
        tool(
            "harness_start",
            "Register an explicit Worker and open its unfocused Herdr pane.",
            passthrough.clone(),
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
            "harness_stop",
            "Stop one explicit Worker Host after settling active cancellation.",
            json!({"type":"object","required":["worker_id"],"properties":{"worker_id":{"type":"string"}},"additionalProperties":false}),
        ),
    ]
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
