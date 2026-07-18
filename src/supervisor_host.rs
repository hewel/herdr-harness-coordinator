//! Provider-specific native injection boundary for the managed visible Supervisor.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{
    adapter::{
        AdapterError, AdapterEvent, AdapterLifecycle, AdapterResult, HarnessAdapter,
        HarnessStartSpec, NativeAcceptance, NativeDeliveryKind, NativeSupervisorSession,
        ResolvedDelivery, SupervisorAdapter, SupervisorAdapterEvent, SupervisorAdapterEventStream,
        SupervisorBindSpec, SupervisorSnapshot,
    },
    contract::{HarnessKind, HarnessTier, ResultManifestV1, SupervisorEvent},
    core::{
        ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
        QueryResult, SessionCapability,
    },
    process_adapter::{CodexProcessAdapter, OmpProcessAdapter},
};

/// Runs the pane-resident terminal frontend and automatic durable-event pump.
///
/// # Errors
///
/// Returns an error when binding, durable event delivery, provider I/O, or terminal input fails.
#[expect(
    clippy::too_many_lines,
    reason = "one select loop coordinates user input, durable FIFO delivery, and provider events"
)]
pub async fn run_managed_supervisor_host(
    state_dir: &Path,
    socket: &Path,
    capability_bearer: String,
) -> Result<()> {
    let capability = SessionCapability::from_bearer(capability_bearer)?;
    let coordinator = Coordinator::open(state_dir).await?;
    let QueryResult::Session(session) = coordinator
        .query(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorQuery::SessionSelf,
        )
        .await?
    else {
        bail!("Coordinator returned the wrong Supervisor Session projection")
    };
    if session.definition.tier != HarnessTier::Supervisor {
        bail!("managed Supervisor Host requires a Supervisor Session")
    }
    let executable = resolve_provider_executable(session.definition.kind)?;
    let mut environment = inherited_supervisor_environment();
    environment.insert(
        "HERDR_HARNESS_CAPABILITY".to_owned(),
        serde_json::to_value(&capability)?
            .as_str()
            .context("Supervisor capability did not serialize as text")?
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
    let mut adapter = match session.definition.kind {
        HarnessKind::Omp => ProcessSupervisorAdapter::omp(),
        HarnessKind::Codex => ProcessSupervisorAdapter::codex(),
    };
    let native = adapter
        .bind(&SupervisorBindSpec {
            coordinator_session_id: session.session_id,
            executable,
            cwd: session.definition.cwd,
            provider_state_dir: state_dir
                .join("sessions")
                .join(session.session_id.to_string()),
            model: session.definition.model.clone(),
            provider_profile: None,
            config_overlays: Vec::new(),
            codex_approval_policy: environment
                .get("HERDR_CODEX_APPROVAL_POLICY")
                .map(|value| parse_codex_policy(value))
                .transpose()?,
            codex_sandbox_mode: environment
                .get("HERDR_CODEX_SANDBOX_MODE")
                .map(|value| parse_codex_sandbox(value))
                .transpose()?,
            environment,
        })
        .await
        .context("binding native Supervisor Session")?;
    coordinator
        .execute(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorCommand::RecordSupervisorBinding {
                native_session_id: native.native_session_id.clone(),
                native_thread_id: native.thread_id.clone(),
            },
        )
        .await?;
    let snapshot = adapter
        .snapshot()
        .await
        .context("snapshotting bound Supervisor")?;
    coordinator
        .execute(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorCommand::RecordAdapterSnapshot {
                snapshot: crate::adapter::AdapterSnapshot {
                    lifecycle: snapshot.lifecycle,
                    session_id: snapshot.native_session_id,
                    thread_id: snapshot.thread_id,
                    active_turn_id: snapshot.active_turn_id,
                    steerable: snapshot.steerable,
                    queued_input_count: None,
                    model: session.definition.model,
                    native_health: snapshot.native_health,
                    context_tokens: None,
                    context_window: None,
                    context_percent: None,
                    compaction_count: None,
                },
            },
        )
        .await?;
    let mut events = adapter.events();
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    println!(
        "Managed {:?} Supervisor ready. Type a request and press Enter.",
        adapter.kind()
    );
    let run_result: Result<()> = async {
        loop {
            tokio::select! {
            line = input.next_line() => {
                let Some(line) = line.context("reading Supervisor input")? else {
                    return Ok(());
                };
                if !line.trim().is_empty()
                    && let Err(error) = adapter.send_user_prompt(line).await
                {
                    eprintln!("Supervisor prompt was not accepted: {error}");
                }
            }
            _ = ticker.tick() => {
                let snapshot = adapter.snapshot().await.context("snapshotting Supervisor")?;
                let may_claim = snapshot.lifecycle == AdapterLifecycle::Idle
                    || (adapter.kind() == HarnessKind::Omp
                        && snapshot.lifecycle == AdapterLifecycle::Working);
                if !may_claim {
                    continue;
                }
                let outcome = coordinator.execute(
                    ActorContext::Session { capability: capability.clone() },
                    CoordinatorCommand::ClaimNextSupervisorEvent,
                ).await?;
                let CommandOutcome::SupervisorEventClaimed { event: Some(event) } = outcome else {
                    continue;
                };
                let summary = enrich_event_summary(&coordinator, &capability, &event).await?;
                let durable = SupervisorEvent {
                    id: event.id,
                    kind: event.kind,
                    task_id: event.task_id,
                    result_revision: event.result_revision,
                    summary,
                    attachments: event.attachments,
                    created_at: DateTime::parse_from_rfc3339(&event.created_at)
                        .map_err(anyhow::Error::from)?
                        .with_timezone(&Utc),
                };
                let acceptance = match event.delivery_intent {
                    crate::contract::DeliveryIntent::FollowUp => {
                        adapter.inject_follow_up(&native, &durable).await
                    }
                    crate::contract::DeliveryIntent::Steer => {
                        adapter.inject_steer(&native, &durable).await
                    }
                };
                match acceptance {
                    Ok(acceptance) => {
                        coordinator.execute(
                            ActorContext::Session { capability: capability.clone() },
                            CoordinatorCommand::AcceptSupervisorEvent {
                                event_id: durable.id,
                                native_correlation: acceptance.correlation,
                                evidence: acceptance.evidence,
                            },
                        ).await?;
                    }
                    Err(error) if error.provider_bytes_may_have_been_written() => {
                        coordinator.execute(
                            ActorContext::Session { capability: capability.clone() },
                            CoordinatorCommand::MarkSupervisorEventUnknown {
                                event_id: durable.id,
                                diagnostic: error.to_string(),
                            },
                        ).await?;
                    }
                    Err(error) => {
                        coordinator.execute(
                            ActorContext::Session { capability: capability.clone() },
                            CoordinatorCommand::ReleaseSupervisorEvent {
                                event_id: durable.id,
                                diagnostic: error.to_string(),
                            },
                        ).await?;
                    }
                }
            }
            event = events.next() => {
                match event {
                    Some(Ok(SupervisorAdapterEvent::Transcript { text })) => println!("{text}"),
                    Some(Ok(SupervisorAdapterEvent::Activity { summary })) => eprintln!("{summary}"),
                    Some(Ok(SupervisorAdapterEvent::Failed { message })) => bail!("native Supervisor failed: {message}"),
                    Some(Ok(SupervisorAdapterEvent::Exited { exit_code })) => bail!("native Supervisor exited: {exit_code:?}"),
                    Some(Err(error)) => return Err(error).context("Supervisor adapter event"),
                    Some(Ok(_)) | None => {}
                }
            }
            }
        }
    }
    .await;
    drop(events);
    adapter.stop().await.ok();
    let diagnostic = run_result.as_ref().err().map(ToString::to_string);
    let disconnect_result = coordinator
        .execute(
            ActorContext::Session { capability },
            CoordinatorCommand::RecordSupervisorDisconnected { diagnostic },
        )
        .await;
    match run_result {
        Ok(()) => {
            disconnect_result?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn enrich_event_summary(
    coordinator: &Coordinator,
    capability: &SessionCapability,
    event: &crate::core::SupervisorEventView,
) -> Result<String> {
    let Some(source_message_id) = event.source_message_id else {
        return Ok(event.summary.clone());
    };
    let QueryResult::Inbox(messages) = coordinator
        .query(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorQuery::Inbox,
        )
        .await?
    else {
        return Ok(event.summary.clone());
    };
    let Some(message) = messages
        .into_iter()
        .find(|message| message.id == source_message_id)
    else {
        return Ok(event.summary.clone());
    };
    let Ok(manifest) = serde_json::from_value::<ResultManifestV1>(message.body) else {
        return Ok(event.summary.clone());
    };
    let changed_files = manifest
        .changed_files
        .iter()
        .map(|path| format!("- {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n");
    let verification = manifest
        .verification
        .iter()
        .map(|entry| {
            format!(
                "- {}: {} (exit {})",
                entry.command,
                if entry.passed { "passed" } else { "failed" },
                entry.exit_code
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let risks = if manifest.risks.is_empty() {
        "- None reported".to_owned()
    } else {
        manifest
            .risks
            .iter()
            .map(|risk| format!("- {risk}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Ok(format!(
        "Worker: {}\n\nSummary:\n{}\n\nChanged files:\n{}\n\nVerification:\n{}\n\nRisks:\n{}",
        message.sender_id,
        manifest.summary,
        if changed_files.is_empty() {
            "- None"
        } else {
            &changed_files
        },
        verification,
        risks,
    ))
}

fn inherited_supervisor_environment() -> BTreeMap<String, String> {
    const SAFE: [&str; 10] = [
        "HOME",
        "PATH",
        "USER",
        "LOGNAME",
        "SHELL",
        "TERM",
        "LANG",
        "LC_ALL",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ];
    std::env::vars()
        .filter(|(name, _)| SAFE.contains(&name.as_str()))
        .collect()
}

fn resolve_provider_executable(kind: HarnessKind) -> Result<PathBuf> {
    let name = match kind {
        HarnessKind::Omp => "omp",
        HarnessKind::Codex => "codex",
    };
    let path = std::env::var_os("PATH").context("PATH is unavailable")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .with_context(|| format!("resolving `{name}` from PATH"))
}

/// Supervisor adapter backed by the same pinned provider protocols as Worker adapters.
pub struct ProcessSupervisorAdapter {
    adapter: Box<dyn HarnessAdapter>,
    session: Option<NativeSupervisorSession>,
}

impl ProcessSupervisorAdapter {
    #[must_use]
    pub fn omp() -> Self {
        Self {
            adapter: Box::new(OmpProcessAdapter::new()),
            session: None,
        }
    }

    #[must_use]
    pub fn codex() -> Self {
        Self {
            adapter: Box::new(CodexProcessAdapter::new()),
            session: None,
        }
    }

    fn require_session(&self, session: &NativeSupervisorSession) -> AdapterResult<()> {
        if self.session.as_ref() == Some(session) {
            Ok(())
        } else {
            Err(AdapterError::Operation {
                kind: self.kind(),
                message: "Supervisor event targeted an unbound native Session".to_owned(),
            })
        }
    }

    /// Starts or queues a user-authored prompt in the bound visible Session.
    ///
    /// # Errors
    ///
    /// Returns an Adapter error when the prompt is empty or native acceptance fails.
    pub async fn send_user_prompt(&mut self, text: String) -> AdapterResult<NativeAcceptance> {
        if text.trim().is_empty() {
            return Err(AdapterError::Operation {
                kind: self.kind(),
                message: "Supervisor prompt is empty".to_owned(),
            });
        }
        let snapshot = self.adapter.snapshot().await?;
        self.adapter
            .dispatch(ResolvedDelivery {
                correlation: uuid::Uuid::now_v7().to_string(),
                task_id: None,
                kind: if snapshot.lifecycle == AdapterLifecycle::Idle {
                    NativeDeliveryKind::StartTurn
                } else {
                    NativeDeliveryKind::FollowUp
                },
                text,
                attachments: Vec::new(),
            })
            .await
    }

    /// Stops the native Supervisor provider process.
    ///
    /// # Errors
    ///
    /// Returns an Adapter error when clean provider shutdown cannot be established.
    pub async fn stop(&mut self) -> AdapterResult<()> {
        self.adapter.stop().await
    }

    async fn inject(
        &mut self,
        session: &NativeSupervisorSession,
        event: &SupervisorEvent,
        steer: bool,
    ) -> AdapterResult<NativeAcceptance> {
        self.require_session(session)?;
        let snapshot = self.adapter.snapshot().await?;
        let kind = if steer {
            if !snapshot.steerable {
                return Err(AdapterError::Operation {
                    kind: self.kind(),
                    message: "native Supervisor turn is not safely steerable".to_owned(),
                });
            }
            NativeDeliveryKind::Steer
        } else if snapshot.lifecycle == AdapterLifecycle::Idle {
            NativeDeliveryKind::StartTurn
        } else {
            NativeDeliveryKind::FollowUp
        };
        self.adapter
            .dispatch(ResolvedDelivery {
                correlation: event.id.to_string(),
                task_id: event.task_id,
                kind,
                text: compact_payload(event),
                attachments: Vec::new(),
            })
            .await
    }
}

#[async_trait]
impl SupervisorAdapter for ProcessSupervisorAdapter {
    fn kind(&self) -> HarnessKind {
        self.adapter.kind()
    }

    async fn bind(&mut self, spec: &SupervisorBindSpec) -> AdapterResult<NativeSupervisorSession> {
        let native = self
            .adapter
            .start(&HarnessStartSpec {
                session_id: spec.coordinator_session_id,
                tier: crate::contract::HarnessTier::Supervisor,
                executable: spec.executable.clone(),
                cwd: spec.cwd.clone(),
                provider_state_dir: spec.provider_state_dir.clone(),
                provider_profile: spec.provider_profile.clone(),
                model: spec.model.clone(),
                config_overlays: spec.config_overlays.clone(),
                codex_approval_policy: spec.codex_approval_policy,
                codex_sandbox_mode: spec.codex_sandbox_mode,
                environment: spec.environment.clone(),
            })
            .await?;
        let session = NativeSupervisorSession {
            coordinator_session_id: spec.coordinator_session_id,
            native_session_id: native.session_id,
            thread_id: native.thread_id,
        };
        self.session = Some(session.clone());
        Ok(session)
    }

    async fn inject_follow_up(
        &mut self,
        session: &NativeSupervisorSession,
        event: &SupervisorEvent,
    ) -> AdapterResult<NativeAcceptance> {
        self.inject(session, event, false).await
    }

    async fn inject_steer(
        &mut self,
        session: &NativeSupervisorSession,
        event: &SupervisorEvent,
    ) -> AdapterResult<NativeAcceptance> {
        self.inject(session, event, true).await
    }

    async fn snapshot(&mut self) -> AdapterResult<SupervisorSnapshot> {
        let snapshot = self.adapter.snapshot().await?;
        Ok(SupervisorSnapshot {
            lifecycle: snapshot.lifecycle,
            native_session_id: snapshot.session_id,
            thread_id: snapshot.thread_id,
            active_turn_id: snapshot.active_turn_id,
            steerable: snapshot.steerable,
            native_health: snapshot.native_health,
        })
    }

    fn events(&mut self) -> SupervisorAdapterEventStream {
        let events = self.adapter.events().filter_map(|event| async move {
            match event {
                Ok(AdapterEvent::Failed { message }) => {
                    Some(Ok(SupervisorAdapterEvent::Failed { message }))
                }
                Ok(AdapterEvent::Transcript { text }) => {
                    Some(Ok(SupervisorAdapterEvent::Transcript { text }))
                }
                Ok(AdapterEvent::Activity { summary }) => {
                    Some(Ok(SupervisorAdapterEvent::Activity { summary }))
                }
                Ok(AdapterEvent::TurnStarted { turn_id }) => {
                    Some(Ok(SupervisorAdapterEvent::TurnStarted { turn_id }))
                }
                Ok(AdapterEvent::TurnCompleted { turn_id, status }) => {
                    Some(Ok(SupervisorAdapterEvent::TurnCompleted {
                        turn_id,
                        status,
                    }))
                }
                Ok(AdapterEvent::Exited { exit_code }) => {
                    Some(Ok(SupervisorAdapterEvent::Exited { exit_code }))
                }
                Err(error) => Some(Err(error)),
                Ok(_) => None,
            }
        });
        Box::pin(stream::select(events, stream::empty()))
    }
}

fn parse_codex_policy(value: &str) -> Result<crate::contract::CodexApprovalPolicy> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .context("invalid explicit Codex Supervisor approval policy")
}

fn parse_codex_sandbox(value: &str) -> Result<crate::contract::CodexSandboxMode> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .context("invalid explicit Codex Supervisor sandbox mode")
}

fn compact_payload(event: &SupervisorEvent) -> String {
    let task = event
        .task_id
        .map_or_else(|| "none".to_owned(), |id| id.to_string());
    let revision = event
        .result_revision
        .map_or_else(|| "none".to_owned(), |value| value.to_string());
    let attachments = event
        .attachments
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Coordinator Supervisor event\n\nEvent ID: {}\nKind: {:?}\nTask: {task}\nResult revision: {revision}\n\nSummary:\n{}\n\nAttachments: {}\n\nRequired action:\nProcess this event using the Coordinator tools, then acknowledge it explicitly or through the matching Reply, Correction, Approval, or reconciliation command.",
        event.id,
        event.kind,
        event.summary,
        if attachments.is_empty() {
            "none"
        } else {
            &attachments
        },
    )
}
