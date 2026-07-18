//! Pane-resident Worker Host and terminal popup entrypoint behavior.

use std::{collections::BTreeMap, fmt::Write as _, path::Path, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use futures::StreamExt;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use sha2::{Digest, Sha256};

use crate::{
    adapter::{
        AdapterEvent, HarnessAdapter, HarnessStartSpec, NativeDeliveryKind, NativeTurnStatus,
        ResolvedDelivery,
    },
    attachment::AttachmentStore,
    broker::{BrokerOperation, BrokerRequest, call},
    contract::{
        DeliveryIntent, HarnessKind, HarnessLaunchProfileV1, MessageSubmissionV1, SCHEMA_VERSION,
        TaskSubmissionV1, Validate,
    },
    core::{
        ActorContext, CommandOutcome, CoordinatorCommand, CoordinatorQuery, InboxMessageView,
        QueryResult, SessionCapability, TaskState,
    },
    process_adapter::{CodexProcessAdapter, OmpProcessAdapter},
};

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const PRE_WRITE_RETRIES: usize = 3;

/// Runs one Worker pane's provider process until it exits or the Host is stopped.
///
/// # Errors
///
/// Returns an error when Session bootstrap, profile validation, broker delivery, or the
/// provider lifecycle fails.
pub async fn run_worker_host(socket: &Path, state_dir: &Path, bearer: String) -> Result<()> {
    let capability = SessionCapability::from_bearer(bearer)?;
    let result = run_worker_host_inner(socket, state_dir, capability.clone()).await;
    if let Err(error) = &result {
        let _ = broker_execute(
            socket,
            capability,
            CoordinatorCommand::RecordHostFailed {
                diagnostic: format!("{error:#}"),
            },
        )
        .await;
    }
    result
}

#[expect(
    clippy::too_many_lines,
    reason = "the Host loop owns one provider lifecycle and event stream"
)]
async fn run_worker_host_inner(
    socket: &Path,
    state_dir: &Path,
    capability: SessionCapability,
) -> Result<()> {
    let session = broker_query(socket, capability.clone(), CoordinatorQuery::SessionSelf).await?;
    let QueryResult::Session(session) = session else {
        bail!("broker returned the wrong Session bootstrap projection");
    };
    let snapshot = session
        .profile_snapshot
        .context("Worker Session has no launch profile snapshot")?;
    let expected_digest = session
        .profile_digest
        .context("Worker Session has no launch profile digest")?;
    let actual_digest = hex::encode(Sha256::digest(snapshot.as_bytes()));
    if actual_digest != expected_digest {
        bail!("Worker launch profile snapshot digest does not match durable Session state");
    }
    let profile: HarnessLaunchProfileV1 =
        toml::from_str(&snapshot).context("decoding durable Worker launch profile snapshot")?;
    profile
        .validate()
        .context("validating durable Worker launch profile")?;
    if profile.kind != session.definition.kind {
        bail!("Worker Harness Kind differs from its durable launch profile");
    }
    let mut environment = profile
        .inherit_env
        .iter()
        .filter_map(|name| std::env::var(name).ok().map(|value| (name.clone(), value)))
        .collect::<BTreeMap<_, _>>();
    environment.insert(
        "HERDR_HARNESS_CAPABILITY".to_owned(),
        serde_json::to_value(&capability)?
            .as_str()
            .context("Session capability did not serialize as a bearer")?
            .to_owned(),
    );
    environment.insert(
        "HERDR_COORDINATOR_SOCKET".to_owned(),
        socket.to_string_lossy().into_owned(),
    );
    environment.insert(
        "HERDR_PLUGIN_STATE_DIR".to_owned(),
        state_dir.to_string_lossy().into_owned(),
    );
    let spec = HarnessStartSpec {
        session_id: session.session_id,
        executable: profile.executable,
        cwd: session.definition.cwd,
        provider_state_dir: state_dir
            .join("sessions")
            .join(session.session_id.to_string()),
        provider_profile: profile.provider_profile,
        model: profile.model,
        config_overlays: profile.config_overlays,
        environment,
    };
    tokio::fs::create_dir_all(&spec.provider_state_dir)
        .await
        .context("creating provider Session state directory")?;
    crate::mcp::verify_required_worker_tools(socket, capability.clone())
        .await
        .context("verifying required Coordinator tools")?;
    let mut adapter: Box<dyn HarnessAdapter> = match profile.kind {
        HarnessKind::Omp => Box::new(OmpProcessAdapter::new()),
        HarnessKind::Codex => Box::new(CodexProcessAdapter::new()),
    };
    adapter
        .start(&spec)
        .await
        .context("starting native Harness")?;
    broker_execute(
        socket,
        capability.clone(),
        CoordinatorCommand::RecordHostReady,
    )
    .await?;
    let mut events = adapter.events();
    let mut current_task = None;
    let mut cancellation_requested = None;
    let mut cancellation_started = None;
    let mut event_sequence = session.event_sequence;
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                broker_execute(
                    socket,
                    capability.clone(),
                    CoordinatorCommand::ClaimNextTask,
                ).await?;
                let inbox = broker_query(socket, capability.clone(), CoordinatorQuery::Inbox).await?;
                let QueryResult::Inbox(messages) = inbox else { bail!("broker returned the wrong inbox projection") };
                if let Some(message) = messages.first() {
                    let Some(delivery) = resolve_delivery(
                        adapter.as_mut(),
                        socket,
                        state_dir,
                        capability.clone(),
                        message,
                        message.task_id,
                    ).await? else {
                        continue;
                    };
                    match dispatch_with_safe_retries(adapter.as_mut(), delivery).await {
                        Ok(acceptance) => {
                            broker_execute(socket, capability.clone(), CoordinatorCommand::AcceptDelivery {
                                message_id: message.id,
                                native_correlation: acceptance.correlation,
                            }).await?;
                            broker_execute(socket, capability.clone(), CoordinatorCommand::MarkInboxRead {
                                message_ids: vec![message.id],
                            }).await?;
                            if let Some(task_id) = message.task_id {
                                current_task = Some(task_id);
                            }
                        }
                        Err(error) if error.provider_bytes_may_have_been_written() => {
                            broker_execute(socket, capability.clone(), CoordinatorCommand::MarkDeliveryUnknown {
                                message_id: message.id,
                                diagnostic: error.to_string(),
                            }).await?;
                            adapter.stop().await.ok();
                            return Err(error).context("native delivery acceptance became ambiguous");
                        }
                        Err(error) => return Err(error).context("native delivery failed before acceptance"),
                    }
                }
                let tasks = broker_query(socket, capability.clone(), CoordinatorQuery::ListTasks).await?;
                if let QueryResult::Tasks(tasks) = tasks
                    && let Some(task) = tasks.iter().find(|task| task.worker_id == session.definition.id && task.state == TaskState::Cancelling)
                    && cancellation_requested != Some(task.id)
                {
                    if current_task == Some(task.id) {
                        adapter.cancel_active().await.context("cooperatively cancelling native turn")?;
                        cancellation_requested = Some(task.id);
                        cancellation_started = Some(tokio::time::Instant::now());
                    } else {
                        broker_execute(socket, capability.clone(), CoordinatorCommand::RecordCancellationCompleted {
                            task_id: task.id,
                            succeeded: true,
                        }).await?;
                    }
                }
                if let (Some(task_id), Some(started)) = (cancellation_requested, cancellation_started)
                    && started.elapsed() >= Duration::from_secs(15)
                {
                    broker_execute(socket, capability.clone(), CoordinatorCommand::RecordCancellationCompleted {
                        task_id,
                        succeeded: false,
                    }).await?;
                    adapter.stop().await.ok();
                    bail!("cooperative cancellation timed out");
                }
                let session_state = broker_query(socket, capability.clone(), CoordinatorQuery::SessionSelf).await?;
                let QueryResult::Session(session_state) = session_state else {
                    bail!("broker returned the wrong Session projection");
                };
                if session_state.activity == "stopping" {
                    let tasks = broker_query(socket, capability.clone(), CoordinatorQuery::ListTasks).await?;
                    let QueryResult::Tasks(tasks) = tasks else {
                        bail!("broker returned the wrong Task projection");
                    };
                    let active = tasks.iter().any(|task| {
                        task.worker_id == session.definition.id
                            && matches!(task.state, TaskState::Dispatching | TaskState::Working | TaskState::Waiting | TaskState::Reviewing | TaskState::Cancelling | TaskState::DeliveryUnknown)
                    });
                    if !active {
                        adapter.stop().await.context("stopping native Harness")?;
                        broker_execute(socket, capability.clone(), CoordinatorCommand::RecordHostStopped { clean: true }).await?;
                        return Ok(());
                    }
                }
            }
            event = events.next() => {
                match event {
                    Some(Ok(event)) => {
                        event_sequence = event_sequence.saturating_add(1);
                        broker_execute(socket, capability.clone(), CoordinatorCommand::RecordHostEvent {
                            sequence: event_sequence,
                            event: serde_json::to_value(&event).context("serializing normalized Host event")?,
                        }).await?;
                        match event {
                            AdapterEvent::TurnCompleted { turn_id, status } => {
                                if let Some(task_id) = current_task.take() {
                                    let task = broker_query(socket, capability.clone(), CoordinatorQuery::GetTask { task_id }).await?;
                                    let QueryResult::Task(task) = task else {
                                        bail!("broker returned the wrong Task projection")
                                    };
                                    if task.state == TaskState::Cancelling {
                                        broker_execute(socket, capability.clone(), CoordinatorCommand::RecordCancellationCompleted {
                                            task_id,
                                            succeeded: matches!(status, NativeTurnStatus::Interrupted | NativeTurnStatus::Completed),
                                        }).await?;
                                        cancellation_requested = None;
                                        cancellation_started = None;
                                    } else {
                                        broker_execute(socket, capability.clone(), CoordinatorCommand::RecordTurnCompleted {
                                            task_id,
                                            native_turn_id: turn_id.unwrap_or_else(|| "provider-turn".to_owned()),
                                            succeeded: status == NativeTurnStatus::Completed,
                                        }).await?;
                                    }
                                }
                            }
                            AdapterEvent::Failed { message } => return Err(anyhow!(message)),
                            AdapterEvent::Exited { exit_code } => {
                                bail!("native Harness exited with status {exit_code:?}");
                            }
                            _ => {}
                        }
                    }
                    Some(Err(error)) => return Err(error).context("reading native Harness event"),
                    None => bail!("native Harness event stream closed"),
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for Worker Host shutdown signal")?;
                adapter.stop().await.context("stopping native Harness")?;
                return Ok(());
            }
        }
    }
}

/// Renders one durable text snapshot for the Herdr popup entrypoint.
///
/// Mutating popup actions use the same broker command frames through the `call` CLI.
///
/// # Errors
///
/// Returns an error when authentication, broker queries, or response decoding fails.
pub async fn render_popup(socket: &Path, bearer: String) -> Result<String> {
    let capability = SessionCapability::from_bearer(bearer)?;
    let status = broker_query(socket, capability.clone(), CoordinatorQuery::HarnessStatus).await?;
    let tasks = broker_query(socket, capability.clone(), CoordinatorQuery::ListTasks).await?;
    let inbox = broker_query(socket, capability.clone(), CoordinatorQuery::Inbox).await?;
    let holds = broker_query(socket, capability, CoordinatorQuery::ActiveHolds).await?;
    let QueryResult::HarnessStatus(status) = status else {
        bail!("invalid Harness status response")
    };
    let QueryResult::Tasks(tasks) = tasks else {
        bail!("invalid Task list response")
    };
    let QueryResult::Inbox(inbox) = inbox else {
        bail!("invalid inbox response")
    };
    let QueryResult::Holds(holds) = holds else {
        bail!("invalid Hold list response")
    };
    let mut output = String::from("Harness Network\n\n");
    for harness in status {
        let _ = writeln!(
            output,
            "{} {} · {:?} · {} · inbox {}",
            if harness.presence == "online" {
                "●"
            } else {
                "○"
            },
            harness.id,
            harness.tier,
            harness.activity,
            harness.unread_messages
        );
    }
    output.push_str("\nTasks\n");
    for task in tasks {
        let _ = writeln!(
            output,
            "{} · {} · {:?} · revision {}",
            task.id, task.worker_id, task.state, task.result_revision
        );
    }
    output.push_str("\nInbox\n");
    for message in inbox {
        let _ = writeln!(
            output,
            "{} · {} · {} · {}",
            message.id,
            message.sender_id,
            message.kind,
            message.delivery_state.as_deref().unwrap_or("pending")
        );
    }
    if !holds.is_empty() {
        output.push_str("\nWorktree Holds\n");
        for hold in holds {
            let _ = writeln!(
                output,
                "{} · {} · {}",
                hold.task_id, hold.repository_key, hold.reason
            );
        }
    }
    Ok(output)
}

/// Runs the interactive Supervisor popup until Escape or `q` is pressed.
///
/// The selected Task follows FIFO display order. Arrow keys change selection; `a` approves a
/// reviewing Task after a fresh trusted Observation, `c` cancels it, `h` clears its Hold after a
/// fresh reconciliation Observation, and `s` stops the first online Worker.
///
/// # Errors
///
/// Returns an error when terminal setup, broker access, or an authorized action fails.
pub async fn run_popup(socket: &Path, bearer: String) -> Result<()> {
    let capability = SessionCapability::from_bearer(bearer.clone())?;
    let mut terminal = ratatui::init();
    let result = run_popup_loop(&mut terminal, socket, &bearer, &capability).await;
    ratatui::restore();
    result
}

#[expect(
    clippy::too_many_lines,
    reason = "the compact popup event loop keeps selection and authorized controls together"
)]
async fn run_popup_loop(
    terminal: &mut ratatui::DefaultTerminal,
    socket: &Path,
    bearer: &str,
    capability: &SessionCapability,
) -> Result<()> {
    let mut selected = 0_usize;
    loop {
        let tasks = broker_query(socket, capability.clone(), CoordinatorQuery::ListTasks).await?;
        let QueryResult::Tasks(tasks) = tasks else {
            bail!("invalid Task list response")
        };
        selected = selected.min(tasks.len().saturating_sub(1));
        let mut content = render_popup(socket, bearer.to_owned()).await?;
        let _ = writeln!(
            content,
            "\nSelected: {}\n[↑/↓] Select  [a] Approve  [c] Cancel  [h] Clear Hold  [s] Stop Worker  [Esc/q] Close",
            tasks
                .get(selected)
                .map_or_else(|| "none".to_owned(), |task| task.id.to_string())
        );
        terminal.draw(|frame| {
            frame.render_widget(
                Paragraph::new(content)
                    .block(Block::default().borders(Borders::ALL).title(" Herdr "))
                    .wrap(Wrap { trim: false }),
                frame.area(),
            );
        })?;
        if !event::poll(Duration::from_millis(500))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => return Ok(()),
            KeyCode::Up => selected = selected.saturating_sub(1),
            KeyCode::Down => selected = (selected + 1).min(tasks.len().saturating_sub(1)),
            KeyCode::Char('c') => {
                if let Some(task) = tasks.get(selected) {
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::CancelTask { task_id: task.id },
                    )
                    .await?;
                }
            }
            KeyCode::Char('a') => {
                if let Some(task) = tasks.get(selected) {
                    let captured = broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::CaptureRepositoryObservation {
                            task_id: task.id,
                            checkpoint: crate::contract::ObservationCheckpoint::Approval,
                        },
                    )
                    .await?;
                    let CommandOutcome::ObservationRecorded { digest, .. } = captured else {
                        bail!("repository capture returned the wrong outcome")
                    };
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::ApproveTask {
                            task_id: task.id,
                            result_revision: task.result_revision,
                            observation_digest: digest,
                        },
                    )
                    .await?;
                }
            }
            KeyCode::Char('h') => {
                if let Some(task) = tasks.get(selected) {
                    let captured = broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::CaptureRepositoryObservation {
                            task_id: task.id,
                            checkpoint: crate::contract::ObservationCheckpoint::HoldClear,
                        },
                    )
                    .await?;
                    let CommandOutcome::ObservationRecorded { digest, .. } = captured else {
                        bail!("repository capture returned the wrong outcome")
                    };
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::ClearWorktreeHold {
                            task_id: task.id,
                            observation_digest: digest,
                            audit_note: "Supervisor reconciled the repository from the popup."
                                .to_owned(),
                        },
                    )
                    .await?;
                }
            }
            KeyCode::Char('s') => {
                let status =
                    broker_query(socket, capability.clone(), CoordinatorQuery::HarnessStatus)
                        .await?;
                let QueryResult::HarnessStatus(status) = status else {
                    bail!("invalid Harness status response")
                };
                if let Some(worker) = status.into_iter().find(|harness| {
                    harness.tier == crate::contract::HarnessTier::Worker
                        && harness.presence == "online"
                }) {
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::StopWorker {
                            worker_id: worker.id,
                        },
                    )
                    .await?;
                }
            }
            _ => {}
        }
    }
}

async fn resolve_delivery(
    adapter: &mut dyn HarnessAdapter,
    socket: &Path,
    state_dir: &Path,
    capability: SessionCapability,
    message: &InboxMessageView,
    task_id: Option<crate::contract::TaskId>,
) -> Result<Option<ResolvedDelivery>> {
    let (text, intent, attachment_ids) = if message.kind == "task" {
        let task: TaskSubmissionV1 =
            serde_json::from_value(message.body.clone()).context("decoding root Task Message")?;
        (
            task.instructions,
            DeliveryIntent::FollowUp,
            task.attachments,
        )
    } else {
        let message: MessageSubmissionV1 =
            serde_json::from_value(message.body.clone()).context("decoding Bus Message")?;
        (message.text, message.delivery, message.attachments)
    };
    let snapshot = adapter
        .snapshot()
        .await
        .context("capturing Adapter state before delivery")?;
    if intent == DeliveryIntent::FollowUp
        && snapshot.lifecycle == crate::adapter::AdapterLifecycle::Working
        && !adapter.capabilities().active_turn_follow_up
    {
        return Ok(None);
    }
    let kind = match intent {
        DeliveryIntent::Steer => NativeDeliveryKind::Steer,
        DeliveryIntent::FollowUp
            if snapshot.lifecycle == crate::adapter::AdapterLifecycle::Working =>
        {
            NativeDeliveryKind::FollowUp
        }
        DeliveryIntent::FollowUp => NativeDeliveryKind::StartTurn,
    };
    let mut attachments = Vec::with_capacity(attachment_ids.len());
    let store = AttachmentStore::new(state_dir);
    for attachment_id in attachment_ids {
        let result = broker_query(
            socket,
            capability.clone(),
            CoordinatorQuery::GetAttachment { attachment_id },
        )
        .await?;
        let QueryResult::Attachment(metadata) = result else {
            bail!("broker returned the wrong Attachment projection");
        };
        store
            .verify(&metadata)
            .await
            .context("verifying immutable Attachment before provider delivery")?;
        attachments.push(crate::adapter::ResolvedAttachment {
            id: metadata.id,
            path: state_dir.join(metadata.storage_path),
            media_type: metadata.media_type,
        });
    }
    Ok(Some(ResolvedDelivery {
        correlation: message.id.to_string(),
        task_id,
        kind,
        text,
        attachments,
    }))
}

async fn dispatch_with_safe_retries(
    adapter: &mut dyn HarnessAdapter,
    delivery: ResolvedDelivery,
) -> crate::adapter::AdapterResult<crate::adapter::NativeAcceptance> {
    let mut last = None;
    for _ in 0..PRE_WRITE_RETRIES {
        match adapter.dispatch(delivery.clone()).await {
            Ok(acceptance) => return Ok(acceptance),
            Err(error) if error.provider_bytes_may_have_been_written() => {
                return Err(error);
            }
            Err(error) => last = Some(error),
        }
    }
    Err(last.expect("at least one retry attempt"))
}

async fn broker_query(
    socket: &Path,
    capability: SessionCapability,
    query: CoordinatorQuery,
) -> Result<QueryResult> {
    let response = call(
        socket,
        &BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: uuid::Uuid::now_v7().to_string(),
            operation: BrokerOperation::Query {
                actor: ActorContext::Session { capability },
                query,
            },
        },
    )
    .await?;
    decode_result(response)
}

async fn broker_execute(
    socket: &Path,
    capability: SessionCapability,
    command: CoordinatorCommand,
) -> Result<CommandOutcome> {
    let response = call(
        socket,
        &BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: uuid::Uuid::now_v7().to_string(),
            operation: BrokerOperation::Execute {
                actor: ActorContext::Session { capability },
                command,
            },
        },
    )
    .await?;
    decode_result(response)
}

fn decode_result<T: serde::de::DeserializeOwned>(
    response: crate::broker::BrokerResponse,
) -> Result<T> {
    if let Some(error) = response.error {
        bail!("broker {:?}: {}", error.category, error.message);
    }
    serde_json::from_value(response.result.context("broker response omitted result")?)
        .context("decoding typed broker result")
}
