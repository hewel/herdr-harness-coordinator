use std::{
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use herdr_harness_coordinator::{
    activation::{
        ActivationRegistry, ActivationRuntime, DesiredActivation, SetActivationRequest,
        SupervisorSelection, WorkerSelection, WorkspaceActivationView, WorkspaceIdentity,
        WorkspaceSelection,
    },
    broker::{BrokerRequest, BrokerServer, call},
    contract::{HarnessDefinitionV1, HarnessId, HarnessKind, HarnessTier, SCHEMA_VERSION},
    core::{
        ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
        HarnessStatusView, QueryResult, SessionCapability,
    },
    herdr::{HerdrSocketClient, PluginPaneOpenParams},
    mcp::McpServer,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Parser)]
#[command(name = "herdr-harness-coordinator", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Configure Coordinator activation independently for Herdr workspaces.
    Workspace {
        /// Plugin-root durable state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    /// Own `SQLite` state and the local JSONL Unix socket.
    Daemon {
        /// Durable state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        /// Unix socket path; defaults beneath the state directory.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Send one `BrokerRequest` JSON value from stdin and print one response.
    Call {
        /// Broker Unix socket.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: PathBuf,
    },
    /// Proxy newline-delimited `BrokerRequest` values between stdio and the broker.
    StdioProxy {
        /// Broker Unix socket.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: PathBuf,
    },
    /// Run one pane-resident Worker Host and its persistent native Harness.
    WorkerHost {
        /// Opaque Session capability passed by the Herdr Worker pane launch.
        #[arg(long)]
        session_id: String,
        /// Durable plugin state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        /// Broker Unix socket; defaults beneath the state directory.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: Option<PathBuf>,
    },
    /// Run the managed visible Supervisor terminal and native Harness.
    SupervisorHost {
        /// Opaque capability for the sole durable Supervisor Session.
        #[arg(long, env = "HERDR_SUPERVISOR_CAPABILITY")]
        supervisor_capability: String,
        /// Durable plugin state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        /// Broker Unix socket; defaults beneath the state directory.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: Option<PathBuf>,
    },
    /// Render the durable Harness Network popup snapshot.
    Popup {
        /// Active Supervisor capability used for authorized popup queries.
        #[arg(long, env = "HERDR_SUPERVISOR_CAPABILITY")]
        supervisor_capability: String,
        /// Durable plugin state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        /// Broker Unix socket; defaults beneath the state directory.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: Option<PathBuf>,
    },
    /// Serve identity-bound Coordinator tools over MCP stdio.
    Mcp {
        /// Harness Session capability retained by this proxy process.
        #[arg(long, env = "HERDR_HARNESS_CAPABILITY")]
        session_capability: String,
        /// Durable plugin state directory.
        #[arg(long, env = "HERDR_PLUGIN_STATE_DIR")]
        state_dir: PathBuf,
        /// Broker Unix socket; defaults beneath the state directory.
        #[arg(long, env = "HERDR_COORDINATOR_SOCKET")]
        socket: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum WorkspaceCommand {
    /// Get one workspace's saved activation state.
    Get(WorkspaceTargetArgs),
    /// Idempotently request an on or off state.
    Set {
        /// Desired state.
        state: WorkspaceStateArg,
        #[command(flatten)]
        target: WorkspaceTargetArgs,
        /// Revision returned by a previous get/set operation.
        #[arg(long)]
        expected_revision: Option<u64>,
        /// Explicit Supervisor Harness Kind for first setup.
        #[arg(long, value_enum, requires = "supervisor_model")]
        supervisor_kind: Option<HarnessKindArg>,
        /// Explicit strong-logic Supervisor model for first setup.
        #[arg(long)]
        supervisor_model: Option<String>,
        /// Provider-native Supervisor reasoning effort.
        #[arg(long)]
        supervisor_reasoning_effort: Option<String>,
        /// Explicit Worker mapping in `worker-id=profile-id` form.
        #[arg(long)]
        worker: Vec<String>,
    },
    /// List every known workspace without changing state.
    List {
        /// Emit the stable JSON projection.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, clap::Args)]
struct WorkspaceTargetArgs {
    /// Opaque current Herdr workspace ID.
    #[arg(long, env = "HERDR_WORKSPACE_ID")]
    workspace: String,
    /// Repository root; defaults to the current directory.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Current Herdr session socket, part of workspace identity.
    #[arg(long, env = "HERDR_SOCKET_PATH")]
    session_socket: PathBuf,
    /// Emit the stable JSON projection.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorkspaceStateArg {
    On,
    Off,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HarnessKindArg {
    Omp,
    Codex,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init()
        .ok();
    match Cli::parse().command {
        Command::Workspace { state_dir, command } => run_workspace(state_dir, command).await,
        Command::Daemon { state_dir, socket } => run_daemon(state_dir, socket).await,
        Command::Call { socket } => run_call(socket).await,
        Command::StdioProxy { socket } => run_proxy(socket).await,
        Command::WorkerHost {
            session_id,
            state_dir,
            socket,
        } => {
            let socket = socket.unwrap_or_else(|| state_dir.join("coordinator.sock"));
            let result =
                herdr_harness_coordinator::host::run_worker_host(&socket, &state_dir, session_id)
                    .await;
            if let Err(error) = &result {
                append_host_diagnostic(&state_dir, "worker-host", error).await;
            }
            result
        }
        Command::SupervisorHost {
            supervisor_capability,
            state_dir,
            socket,
        } => {
            let socket = socket.unwrap_or_else(|| state_dir.join("coordinator.sock"));
            let pid_path = state_dir.join("supervisor-host.pid");
            tokio::fs::write(&pid_path, std::process::id().to_string()).await?;
            let result = herdr_harness_coordinator::supervisor_host::run_managed_supervisor_host(
                &state_dir,
                &socket,
                supervisor_capability,
            )
            .await;
            if let Err(error) = &result {
                append_host_diagnostic(&state_dir, "supervisor-host", error).await;
            }
            remove_file_if_present(&pid_path).await?;
            result
        }
        Command::Popup {
            supervisor_capability,
            state_dir,
            socket,
        } => {
            let socket = socket.unwrap_or_else(|| state_dir.join("coordinator.sock"));
            herdr_harness_coordinator::host::run_popup(&socket, supervisor_capability).await
        }
        Command::Mcp {
            session_capability,
            state_dir,
            socket,
        } => {
            let socket = socket.unwrap_or_else(|| state_dir.join("coordinator.sock"));
            herdr_harness_coordinator::mcp::from_bearer_for_workspace(
                &socket,
                session_capability,
                state_dir,
            )?
            .run_stdio()
            .await
        }
    }
}

async fn append_host_diagnostic(state_dir: &Path, host: &str, error: &anyhow::Error) {
    let path = state_dir.join(format!("{host}.log"));
    let Ok(mut log) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    else {
        return;
    };
    let _ = log
        .write_all(
            format!(
                "{}\t{}\n",
                chrono::Utc::now().to_rfc3339(),
                format_args!("{error:#}")
            )
            .as_bytes(),
        )
        .await;
}

async fn run_workspace(state_dir: PathBuf, command: WorkspaceCommand) -> Result<()> {
    let registry = ActivationRegistry::open(&state_dir).await?;
    match command {
        WorkspaceCommand::Get(target) => {
            let identity = workspace_identity(&target)?;
            print_activation(&registry.get(&identity).await?, target.json)
        }
        WorkspaceCommand::Set {
            state,
            target,
            expected_revision,
            supervisor_kind,
            supervisor_model,
            supervisor_reasoning_effort,
            worker,
        } => {
            let identity = workspace_identity(&target)?;
            let selection = workspace_selection(
                supervisor_kind,
                supervisor_model,
                supervisor_reasoning_effort,
                worker,
            )?;
            let desired = match state {
                WorkspaceStateArg::On => DesiredActivation::On,
                WorkspaceStateArg::Off => DesiredActivation::Off,
            };
            let mut view = registry
                .set(
                    &identity,
                    SetActivationRequest {
                        desired,
                        expected_revision,
                        selection,
                    },
                )
                .await?;
            if desired == DesiredActivation::Off {
                if let Err(error) = deactivate_workspace(&registry, &identity).await {
                    registry
                        .record_runtime(
                            &identity,
                            ActivationRuntime::RecoveryRequired,
                            Some(format!("{error:#}")),
                        )
                        .await?;
                    return Err(error);
                }
                view = registry.get(&identity).await?;
            } else if view.runtime != ActivationRuntime::Online {
                view = match activate_workspace(&registry, &identity, &view).await {
                    Ok(view) => view,
                    Err(error) => {
                        registry
                            .record_runtime(
                                &identity,
                                ActivationRuntime::RecoveryRequired,
                                Some(format!("{error:#}")),
                            )
                            .await?;
                        return Err(error);
                    }
                };
            }
            print_activation(&view, target.json)
        }
        WorkspaceCommand::List { json } => {
            let views = registry.list().await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&views)?);
            } else if views.is_empty() {
                println!("No configured workspaces.");
            } else {
                for view in views {
                    println!(
                        "{}\t{:?}\t{:?}\trevision {}\t{}",
                        view.workspace_id,
                        view.desired,
                        view.runtime,
                        view.revision,
                        view.repository_root.display()
                    );
                }
            }
            Ok(())
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "workspace activation keeps daemon, managed Supervisor, and explicit Worker startup ordered"
)]
async fn activate_workspace(
    registry: &ActivationRegistry,
    identity: &WorkspaceIdentity,
    view: &WorkspaceActivationView,
) -> Result<WorkspaceActivationView> {
    let selection = view
        .selection
        .as_ref()
        .context("enabled workspace has no Harness selection")?;
    let coordinator = Coordinator::open(&view.state_dir).await?;
    let capability_path = view.state_dir.join("supervisor.capability");
    let capability = if capability_path.exists() {
        SessionCapability::from_bearer(
            std::fs::read_to_string(&capability_path)?.trim().to_owned(),
        )?
    } else {
        let outcome = coordinator
            .execute(
                ActorContext::Bootstrap,
                CoordinatorCommand::RegisterSupervisor {
                    definition: HarnessDefinitionV1 {
                        schema_version: SCHEMA_VERSION,
                        id: "supervisor".parse()?,
                        kind: selection.supervisor.kind,
                        tier: HarnessTier::Supervisor,
                        cwd: view.repository_root.clone(),
                        launch_profile: None,
                        model: Some(selection.supervisor.model.clone()),
                    },
                },
            )
            .await?;
        let CommandOutcome::SupervisorRegistered { capability, .. } = outcome else {
            bail!("Coordinator returned the wrong Supervisor registration outcome")
        };
        write_capability(&capability_path, &capability)?;
        capability
    };
    let socket = workspace_socket(&view.state_dir)?;
    if !socket_is_live(&socket).await {
        remove_file_if_present(&socket).await?;
        let executable = std::env::current_exe().context("resolving Coordinator executable")?;
        let daemon_log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(view.state_dir.join("coordinator-daemon.log"))?;
        let child = tokio::process::Command::new(&executable)
            .arg("daemon")
            .arg("--state-dir")
            .arg(&view.state_dir)
            .arg("--socket")
            .arg(&socket)
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(daemon_log))
            .spawn()
            .context("starting workspace Coordinator daemon")?;
        let pid = child.id().context("workspace daemon has no process ID")?;
        tokio::fs::write(view.state_dir.join("coordinator.pid"), pid.to_string()).await?;
        wait_for_path(&socket, Duration::from_secs(10)).await?;
    }
    let supervisor_pid = view.state_dir.join("supervisor-host.pid");
    let supervisor_live = if supervisor_pid.exists() {
        let pid = std::fs::read_to_string(&supervisor_pid)?;
        PathBuf::from(format!("/proc/{}", pid.trim())).exists()
    } else {
        false
    };
    if !supervisor_live {
        remove_file_if_present(&supervisor_pid).await?;
        let bearer = serde_json::to_value(&capability)?
            .as_str()
            .context("Supervisor capability did not serialize as text")?
            .to_owned();
        let mut pane = PluginPaneOpenParams::supervisor(
            &bearer,
            &view.repository_root,
            Some(view.workspace_id.clone()),
        );
        pane.env.insert(
            "HERDR_COORDINATOR_STATE_DIR".to_owned(),
            view.state_dir.to_string_lossy().into_owned(),
        );
        pane.env.insert(
            "HERDR_COORDINATOR_SOCKET".to_owned(),
            socket.to_string_lossy().into_owned(),
        );
        pane.env.insert(
            "HERDR_COORDINATOR_BIN".to_owned(),
            std::env::current_exe()?.to_string_lossy().into_owned(),
        );
        HerdrSocketClient::new(identity.session_socket().to_path_buf())
            .open_worker(pane)
            .await
            .context("opening managed Supervisor pane")?;
        wait_for_path(&supervisor_pid, Duration::from_secs(10)).await?;
    }
    let server = McpServer::for_workspace(socket, capability.clone(), view.state_dir.clone())
        .with_herdr_socket(identity.session_socket().to_path_buf());
    for worker in &selection.workers {
        let response = server
            .handle(serde_json::json!({
                "jsonrpc":"2.0",
                "id":worker.worker_id.as_str(),
                "method":"tools/call",
                "params":{"name":"harness_start","arguments":{"worker_id":worker.worker_id}}
            }))
            .await
            .context("harness_start returned no correlated response")?;
        if response["result"]["isError"].as_bool() == Some(true) {
            bail!(
                "harness_start failed for {}: {}",
                worker.worker_id,
                response["result"]
            );
        }
    }
    let selected_workers = selection
        .workers
        .iter()
        .map(|worker| worker.worker_id.clone())
        .collect::<Vec<_>>();
    wait_for_selected_workers(&coordinator, &capability, &selected_workers).await?;
    registry
        .record_runtime(identity, ActivationRuntime::Online, None)
        .await
        .map_err(anyhow::Error::from)
}

async fn socket_is_live(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::net::UnixStream::connect(path),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

async fn deactivate_workspace(
    registry: &ActivationRegistry,
    identity: &WorkspaceIdentity,
) -> Result<()> {
    let view = registry.get(identity).await?;
    let capability_path = view.state_dir.join("supervisor.capability");
    if !capability_path.exists() {
        stop_daemon(&view.state_dir).await?;
        return Ok(());
    }
    let capability = SessionCapability::from_bearer(
        std::fs::read_to_string(&capability_path)?.trim().to_owned(),
    )?;
    let coordinator = Coordinator::open(&view.state_dir).await?;
    let status = coordinator
        .query(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorQuery::HarnessStatus,
        )
        .await?;
    let QueryResult::HarnessStatus(status) = status else {
        bail!("Coordinator returned the wrong Harness status projection")
    };
    for worker in status
        .iter()
        .filter(|item| item.tier == HarnessTier::Worker && item.presence != "stopped")
    {
        coordinator
            .execute(
                ActorContext::Session {
                    capability: capability.clone(),
                },
                CoordinatorCommand::StopWorker {
                    worker_id: worker.id.clone(),
                },
            )
            .await?;
    }
    wait_for_workers(&coordinator, &capability, 0).await?;
    coordinator
        .execute(
            ActorContext::Session { capability },
            CoordinatorCommand::DeactivateWorkspace,
        )
        .await?;
    stop_daemon(&view.state_dir).await?;
    remove_file_if_present(&capability_path).await?;
    Ok(())
}

fn write_capability(path: &Path, capability: &SessionCapability) -> Result<()> {
    let bearer = serde_json::to_value(capability)?
        .as_str()
        .context("Session capability did not serialize as a bearer")?
        .to_owned();
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    std::io::Write::write_all(&mut file, bearer.as_bytes())?;
    let permissions = file.metadata()?.permissions();
    if permissions.mode() & 0o077 != 0 {
        bail!("Supervisor capability permissions are not private")
    }
    Ok(())
}

async fn wait_for_path(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("timed out waiting for {}", path.display())
}

async fn wait_for_workers(
    coordinator: &Coordinator,
    capability: &SessionCapability,
    expected_online: usize,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while tokio::time::Instant::now() < deadline {
        let result = coordinator
            .query(
                ActorContext::Session {
                    capability: capability.clone(),
                },
                CoordinatorQuery::HarnessStatus,
            )
            .await?;
        let QueryResult::HarnessStatus(status) = result else {
            bail!("Coordinator returned the wrong Harness status projection")
        };
        let live = status
            .iter()
            .filter(|item| item.tier == HarnessTier::Worker && item.presence == "online")
            .count();
        let active = status
            .iter()
            .filter(|item| item.tier == HarnessTier::Worker && item.presence != "stopped")
            .count();
        if live == expected_online && active == expected_online {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("timed out waiting for {expected_online} online Worker Harnesses")
}

async fn wait_for_selected_workers(
    coordinator: &Coordinator,
    capability: &SessionCapability,
    selected_workers: &[HarnessId],
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while tokio::time::Instant::now() < deadline {
        let result = coordinator
            .query(
                ActorContext::Session {
                    capability: capability.clone(),
                },
                CoordinatorQuery::HarnessStatus,
            )
            .await?;
        let QueryResult::HarnessStatus(status) = result else {
            bail!("Coordinator returned the wrong Harness status projection")
        };
        if selected_workers_are_online(&status, selected_workers) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("timed out waiting for selected Worker Harnesses to become online")
}

fn selected_workers_are_online(
    status: &[HarnessStatusView],
    selected_workers: &[HarnessId],
) -> bool {
    selected_workers.iter().all(|selected| {
        status.iter().any(|item| {
            &item.id == selected
                && item.tier == HarnessTier::Worker
                && item.presence == "online"
                && item.activity != "stopping"
        })
    })
}

async fn stop_daemon(state_dir: &Path) -> Result<()> {
    let pid_path = state_dir.join("coordinator.pid");
    if pid_path.exists() {
        let pid = tokio::fs::read_to_string(&pid_path).await?;
        let status = tokio::process::Command::new("kill")
            .arg(pid.trim())
            .status()
            .await?;
        if !status.success() {
            bail!("workspace daemon did not accept shutdown signal")
        }
        remove_file_if_present(&pid_path).await?;
    }
    remove_file_if_present(&workspace_socket(state_dir)?).await
}

fn workspace_socket(state_dir: &Path) -> Result<PathBuf> {
    let digest = state_dir
        .file_name()
        .and_then(|value| value.to_str())
        .context("workspace state directory has no digest name")?;
    let plugin_state = state_dir
        .parent()
        .and_then(Path::parent)
        .context("workspace state directory has no plugin root")?;
    let short = digest.get(..24).unwrap_or(digest);
    let directory = plugin_state.join("s");
    std::fs::create_dir_all(&directory)?;
    Ok(directory.join(format!("{short}.sock")))
}

async fn remove_file_if_present(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn workspace_identity(args: &WorkspaceTargetArgs) -> Result<WorkspaceIdentity> {
    let root = args.root.clone().map_or_else(std::env::current_dir, Ok)?;
    Ok(WorkspaceIdentity::new(
        args.session_socket.clone(),
        args.workspace.clone(),
        root,
    ))
}

fn workspace_selection(
    kind: Option<HarnessKindArg>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    workers: Vec<String>,
) -> Result<Option<WorkspaceSelection>> {
    if kind.is_none() && model.is_none() && workers.is_empty() && reasoning_effort.is_none() {
        return Ok(None);
    }
    let kind = match kind.context("--supervisor-kind is required when changing selection")? {
        HarnessKindArg::Omp => HarnessKind::Omp,
        HarnessKindArg::Codex => HarnessKind::Codex,
    };
    let model = model.context("--supervisor-model is required when changing selection")?;
    let workers = workers
        .into_iter()
        .map(|mapping| {
            let (worker_id, profile_id) = mapping
                .split_once('=')
                .context("--worker must use worker-id=profile-id")?;
            Ok(WorkerSelection {
                worker_id: HarnessId::from_str(worker_id)?,
                profile_id: HarnessId::from_str(profile_id)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(WorkspaceSelection {
        schema_version: 1,
        supervisor: SupervisorSelection {
            kind,
            model,
            reasoning_effort,
        },
        workers,
    }))
}

fn print_activation(
    view: &herdr_harness_coordinator::activation::WorkspaceActivationView,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(view)?);
    } else {
        println!(
            "Workspace {}: {:?} ({:?}), revision {}\nRepository: {}",
            view.workspace_id,
            view.desired,
            view.runtime,
            view.revision,
            view.repository_root.display()
        );
        if view.selection.is_none() {
            println!(
                "First setup: workspace set on --supervisor-kind KIND --supervisor-model MODEL --worker ID=PROFILE"
            );
        }
    }
    Ok(())
}

async fn run_daemon(state_dir: PathBuf, socket: Option<PathBuf>) -> Result<()> {
    let coordinator = Arc::new(Coordinator::open(&state_dir).await?);
    let socket = socket.unwrap_or_else(|| state_dir.join("coordinator.sock"));
    let server = BrokerServer::bind(coordinator, &socket).await?;
    let result = tokio::select! {
        result = server.serve() => result.map_err(anyhow::Error::from),
        signal = tokio::signal::ctrl_c() => signal.context("waiting for shutdown signal"),
    };
    if let Err(error) = tokio::fs::remove_file(&socket).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, path = %socket.display(), "failed to remove owned broker socket");
    }
    result
}

async fn run_call(socket: PathBuf) -> Result<()> {
    let mut input = Vec::new();
    tokio::io::stdin()
        .read_to_end(&mut input)
        .await
        .context("reading BrokerRequest from stdin")?;
    if input.len() > herdr_harness_coordinator::broker::MAX_BROKER_FRAME_BYTES {
        bail!("request exceeds the 1 MiB broker frame limit");
    }
    let request: BrokerRequest =
        serde_json::from_slice(&input).context("decoding BrokerRequest JSON")?;
    let response = call(&socket, &request).await?;
    let mut output = serde_json::to_vec(&response).context("encoding BrokerResponse")?;
    output.push(b'\n');
    tokio::io::stdout()
        .write_all(&output)
        .await
        .context("writing BrokerResponse to stdout")
}

async fn run_proxy(socket: PathBuf) -> Result<()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    loop {
        let mut frame = Vec::new();
        let read = input
            .read_until(b'\n', &mut frame)
            .await
            .context("reading proxy frame")?;
        if read == 0 {
            return Ok(());
        }
        if frame.len() > herdr_harness_coordinator::broker::MAX_BROKER_FRAME_BYTES {
            bail!("request exceeds the 1 MiB broker frame limit");
        }
        let request: BrokerRequest =
            serde_json::from_slice(&frame).context("decoding proxy BrokerRequest")?;
        let response = call(&socket, &request).await?;
        let mut encoded = serde_json::to_vec(&response).context("encoding BrokerResponse")?;
        encoded.push(b'\n');
        output
            .write_all(&encoded)
            .await
            .context("writing proxy response")?;
        output.flush().await.context("flushing proxy response")?;
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, WorkspaceCommand, WorkspaceStateArg, selected_workers_are_online};
    use clap::Parser;
    use herdr_harness_coordinator::{
        contract::{HarnessTier, TaskId},
        core::HarnessStatusView,
    };

    #[test]
    fn workspace_set_parses_the_idempotent_desired_state_surface() {
        let cli = Cli::try_parse_from([
            "coordinator",
            "workspace",
            "--state-dir",
            "/tmp/state",
            "set",
            "on",
            "--workspace",
            "wF",
            "--root",
            "/repo",
            "--session-socket",
            "/tmp/herdr.sock",
            "--expected-revision",
            "4",
        ])
        .unwrap();

        assert!(matches!(
            cli.command,
            Command::Workspace {
                command: WorkspaceCommand::Set {
                    state: WorkspaceStateArg::On,
                    expected_revision: Some(4),
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn activation_readiness_ignores_historical_unselected_workers() {
        let status = [
            harness_status("old-worker", "online", "stopping"),
            harness_status("selected-worker", "online", "idle"),
        ];

        assert!(selected_workers_are_online(
            &status,
            &["selected-worker".parse().unwrap()]
        ));
        assert!(!selected_workers_are_online(
            &status,
            &["old-worker".parse().unwrap()]
        ));
    }

    fn harness_status(id: &str, presence: &str, activity: &str) -> HarnessStatusView {
        HarnessStatusView {
            id: id.parse().unwrap(),
            tier: HarnessTier::Worker,
            presence: presence.to_owned(),
            activity: activity.to_owned(),
            unread_messages: 0,
            active_task_id: None::<TaskId>,
        }
    }
}
