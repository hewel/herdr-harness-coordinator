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
        DeliveryIntent, HarnessKind, HarnessTier, MessageSubmissionV1, SCHEMA_VERSION,
        TaskSubmissionV1,
    },
    core::{
        ActorContext, CommandOutcome, CoordinatorCommand, CoordinatorQuery,
        HarnessCompatibilityEvidenceV1, InboxMessageView, QueryResult, SessionCapability,
        TaskState,
    },
    process_adapter::{CodexProcessAdapter, OmpProcessAdapter},
    profile::{parse_launch_profile_snapshot, resolve_executable},
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
    let mut capability = SessionCapability::from_bearer(bearer)?;
    loop {
        match run_worker_host_inner(socket, state_dir, capability.clone()).await {
            Ok(Some(rotated)) => capability = rotated,
            Ok(None) => return Ok(()),
            Err(error) => {
                let _ = broker_execute(
                    socket,
                    capability,
                    CoordinatorCommand::RecordHostFailed {
                        diagnostic: format!("{error:#}"),
                    },
                )
                .await;
                return Err(error);
            }
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the Host loop owns one provider lifecycle and event stream"
)]
async fn run_worker_host_inner(
    socket: &Path,
    state_dir: &Path,
    capability: SessionCapability,
) -> Result<Option<SessionCapability>> {
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
    let profile = parse_launch_profile_snapshot(&snapshot)
        .map_err(anyhow::Error::msg)
        .context("decoding durable Worker launch profile snapshot")?;
    if profile.kind != session.definition.kind {
        bail!("Worker Harness Kind differs from its durable launch profile");
    }
    let process_environment = std::env::vars().collect::<BTreeMap<_, _>>();
    let executable = resolve_executable(&profile, &process_environment)
        .context("resolving durable Worker executable")?;
    let mut environment = profile
        .inherit_env
        .iter()
        .filter_map(|name| {
            process_environment
                .get(name)
                .map(|value| (name.clone(), value.clone()))
        })
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
        tier: HarnessTier::Worker,
        executable,
        cwd: session.definition.cwd,
        provider_state_dir: state_dir
            .join("sessions")
            .join(session.session_id.to_string()),
        provider_profile: profile.provider_profile,
        model: profile.model,
        config_overlays: profile.config_overlays,
        codex_approval_policy: profile.codex_approval_policy,
        codex_sandbox_mode: profile.codex_sandbox_mode,
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
    let capabilities = adapter.capabilities();
    let native = adapter
        .start(&spec)
        .await
        .context("starting native Harness")?;
    broker_execute(
        socket,
        capability.clone(),
        CoordinatorCommand::RecordHostCompatibility {
            resolved_executable: spec.executable.clone(),
            observed_version: native.observed_version,
            native_session_id: native.session_id,
            native_thread_id: native.thread_id,
            effective_model: native.model,
            safe_compaction: capabilities.safe_compaction,
            evidence: HarnessCompatibilityEvidenceV1 {
                schema_version: SCHEMA_VERSION,
                kind: profile.kind,
                capabilities,
                successful_checks: match profile.kind {
                    HarnessKind::Omp => vec![
                        "version".to_owned(),
                        "ready".to_owned(),
                        "set_host_tools".to_owned(),
                        "get_state".to_owned(),
                    ],
                    HarnessKind::Codex => vec![
                        "version".to_owned(),
                        "initialize".to_owned(),
                        "initialized".to_owned(),
                        "thread_start".to_owned(),
                    ],
                },
            },
        },
    )
    .await?;
    broker_execute(
        socket,
        capability.clone(),
        CoordinatorCommand::RecordHostReady,
    )
    .await?;
    let snapshot = adapter
        .snapshot()
        .await
        .context("snapshotting native Harness")?;
    broker_execute(
        socket,
        capability.clone(),
        CoordinatorCommand::RecordAdapterSnapshot { snapshot },
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
                if current_task.is_none() {
                    let snapshot = adapter.snapshot().await.context("refreshing native Harness snapshot")?;
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::RecordAdapterSnapshot { snapshot },
                    ).await?;
                }
                let claim = broker_execute(
                    socket,
                    capability.clone(),
                    CoordinatorCommand::ClaimNextTask,
                ).await?;
                if let CommandOutcome::SessionCompactionRequired { .. } = claim {
                    adapter.compact().await.context("compacting required OMP Session")?;
                    let snapshot = adapter.snapshot().await.context("snapshotting compacted OMP Session")?;
                    broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::RecordAdapterSnapshot { snapshot },
                    ).await?;
                    continue;
                }
                if let CommandOutcome::SessionRotationRequired { .. } = claim {
                    adapter.stop().await.context("stopping Session before same-pane rotation")?;
                    let rotated = broker_execute(
                        socket,
                        capability.clone(),
                        CoordinatorCommand::RotateWorkerSession,
                    ).await?;
                    let CommandOutcome::WorkerSessionRotated { capability, .. } = rotated else {
                        bail!("Coordinator returned the wrong Session rotation outcome")
                    };
                    return Ok(Some(capability));
                }
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
                        return Ok(None);
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
                return Ok(None);
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
#[expect(
    clippy::too_many_lines,
    reason = "popup rendering keeps the compact Supervisor projection together"
)]
pub async fn render_popup(socket: &Path, bearer: String) -> Result<String> {
    let capability = SessionCapability::from_bearer(bearer)?;
    let status = broker_query(socket, capability.clone(), CoordinatorQuery::HarnessStatus).await?;
    let tasks = broker_query(socket, capability.clone(), CoordinatorQuery::ListTasks).await?;
    let graph = broker_query(socket, capability.clone(), CoordinatorQuery::TaskGraph).await?;
    let inbox = broker_query(socket, capability.clone(), CoordinatorQuery::Inbox).await?;
    let supervisor_events = broker_query(
        socket,
        capability.clone(),
        CoordinatorQuery::SupervisorEvents,
    )
    .await?;
    let holds = broker_query(socket, capability, CoordinatorQuery::ActiveHolds).await?;
    let QueryResult::HarnessStatus(status) = status else {
        bail!("invalid Harness status response")
    };
    let QueryResult::Tasks(tasks) = tasks else {
        bail!("invalid Task list response")
    };
    let QueryResult::TaskGraph(graph) = graph else {
        bail!("invalid Task graph response")
    };
    let QueryResult::Inbox(inbox) = inbox else {
        bail!("invalid inbox response")
    };
    let QueryResult::Holds(holds) = holds else {
        bail!("invalid Hold list response")
    };
    let QueryResult::SupervisorEvents(supervisor_events) = supervisor_events else {
        bail!("invalid Supervisor event response")
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
            "{} · {} · {:?} · revision {} · {:?}/{:?}",
            task.id,
            task.worker_id,
            task.state,
            task.result_revision,
            task.task_role,
            task.requested_session_policy,
        );
        if let Some(session_id) = task.harness_session_id {
            let _ = writeln!(
                output,
                "  Session {} · {} · {}{}",
                session_id,
                if task.session_reused == Some(true) {
                    "Reused"
                } else {
                    "Fresh"
                },
                task.session_decision_reason
                    .as_deref()
                    .unwrap_or("decision unavailable"),
                task.context_percent
                    .map_or_else(String::new, |percent| format!(" · context {percent}%")),
            );
        }
    }
    output.push_str("\nScheduling\n");
    for entry in graph {
        let execution = if entry.task.state == TaskState::Queued {
            "Not started".to_owned()
        } else {
            format!("{:?}", entry.task.state)
        };
        let _ = writeln!(
            output,
            "{} · {:?} · {} · queue {}{}{}",
            entry.task.id,
            entry.scheduling_state,
            execution,
            entry
                .worker_queue_position
                .map_or_else(|| "-".to_owned(), |position| position.to_string()),
            if entry.waiting_for_worker {
                " · waiting Worker"
            } else {
                ""
            },
            if entry.waiting_for_repository {
                " · waiting repository"
            } else {
                ""
            },
        );
        if entry.waiting_for_session {
            output.push_str("  waiting Session selection or rotation\n");
        }
        for dependency in entry.dependencies {
            let status = dependency.satisfied_by_result_revision.map_or_else(
                || "awaiting".to_owned(),
                |revision| format!("Result revision {revision}"),
            );
            let _ = writeln!(
                output,
                "  {} · {:?} · {}",
                dependency.task_id, dependency.condition, status
            );
        }
        if !entry.dependents.is_empty() {
            let _ = writeln!(
                output,
                "  dependents · {}",
                entry
                    .dependents
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
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
    output.push_str("\nSupervisor events\n");
    for event in supervisor_events {
        let _ = writeln!(
            output,
            "{} · {:?} · {:?} · {}",
            event.id, event.kind, event.state, event.summary
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
        let task_id = task_id.context("root Task delivery must carry a Task identity")?;
        let resolved = broker_query(
            socket,
            capability.clone(),
            CoordinatorQuery::ResolvedTaskInput { task_id },
        )
        .await?;
        let QueryResult::ResolvedTaskInput(resolved) = resolved else {
            bail!("broker returned the wrong resolved Task input projection");
        };
        let mut attachments = resolved.explicit_attachments;
        attachments.extend(
            resolved
                .dependency_results
                .into_iter()
                .map(|dependency| dependency.attachment_id),
        );
        (
            worker_task_prompt(task_id, &task.instructions),
            DeliveryIntent::FollowUp,
            attachments,
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

/// Formats the native Worker prompt for one Task and preserves structured Result authority.
#[must_use]
pub fn worker_task_prompt(task_id: crate::contract::TaskId, instructions: &str) -> String {
    format!(
        "{instructions}\n\nCoordinator completion contract:\n- This is Task {task_id}.\n- Normal assistant text is not a Result and does not complete the Task.\n- Execute the requested verification command(s).\n- The Coordinator tools are on the `herdr` MCP server. In Codex, invoke them through the orchestration tool as `tools.mcp__herdr__harness_attachment_create(...)` and `tools.mcp__herdr__harness_complete(...)`; provider UIs may display the shorter names.\n- Call `harness_attachment_create` with the exact verification output to create immutable evidence.\n- Then call `harness_complete` exactly once with the current native turn ID and a `manifest` containing schema_version 1, this task_id, summary, changed_files, at least one verification entry referencing the returned Attachment ID, deviations, risks, and attachments.\n- Do not finish the native turn until `harness_complete` reports that the Result was recorded."
    )
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
