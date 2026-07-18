use std::path::PathBuf;

use chrono::Utc;
use herdr_harness_coordinator::{
    contract::{
        AttachmentId, CommandEvidenceV1, DeliveryIntent, HarnessDefinitionV1, HarnessId,
        HarnessKind, HarnessTier, MessageKind, MessageSubmissionV1, ObservationCheckpoint,
        RepositoryAccess, RepositoryObservationId, RepositoryObservationV1, ResultManifestV1,
        SCHEMA_VERSION, TaskId, TaskRepositoryAuthorityV1, TaskSubmissionV1, VerificationResultV1,
        WriteScopeV1,
    },
    core::{
        ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
        DeliveryUnknownResolution, QueryResult, SessionCapability, TaskState,
    },
};

#[tokio::test]
async fn supervisor_registration_makes_the_harness_queryable() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let coordinator = Coordinator::open(state.path())
        .await
        .expect("Coordinator must open");
    let definition = HarnessDefinitionV1 {
        schema_version: SCHEMA_VERSION,
        id: "supervisor".parse::<HarnessId>().expect("ID must be valid"),
        kind: HarnessKind::Codex,
        tier: HarnessTier::Supervisor,
        cwd: PathBuf::from("/tmp/project"),
        launch_profile: None,
        model: Some("gpt-5.4".to_owned()),
    };

    let outcome = coordinator
        .execute(
            ActorContext::Bootstrap,
            CoordinatorCommand::RegisterSupervisor { definition },
        )
        .await
        .expect("registration must succeed");
    let capability = match outcome {
        CommandOutcome::SupervisorRegistered { capability, .. } => capability,
        _ => panic!("registration returned the wrong outcome"),
    };

    let result = coordinator
        .query(
            ActorContext::Session { capability },
            CoordinatorQuery::ListHarnesses,
        )
        .await
        .expect("authenticated query must succeed");
    let harnesses = match result {
        QueryResult::Harnesses(harnesses) => harnesses,
        _ => panic!("list query returned the wrong result"),
    };

    assert_eq!(
        harnesses,
        vec!["supervisor".parse().expect("ID must be valid")]
    );
}

#[tokio::test]
async fn question_reply_result_and_correction_follow_the_v1_lifecycle() {
    let (state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let evidence_path = state.path().join("verification.txt");
    std::fs::write(&evidence_path, "all focused tests passed\n").expect("evidence fixture");
    let CommandOutcome::AttachmentAdmitted { attachment } = coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AdmitAttachment {
                source: evidence_path,
                media_type: "text/plain".to_owned(),
                original_name: "verification.txt".to_owned(),
            },
        )
        .await
        .expect("Worker may admit immutable Result evidence")
    else {
        panic!("admission must return Attachment metadata")
    };
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("eligible Task must dispatch")
    else {
        panic!("dispatch must identify the root Message")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation: "omp-prompt-1".to_owned(),
            },
        )
        .await
        .expect("native Task acceptance must start work");

    let CommandOutcome::MessageCreated {
        message_id: question_id,
    } = coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::SendMessage {
                submission: message(
                    "supervisor",
                    task_id,
                    MessageKind::Question,
                    "Which compatibility target?",
                    None,
                ),
            },
        )
        .await
        .expect("assigned Worker may ask a blocking Question")
    else {
        panic!("Question must become a Message")
    };
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Waiting, 0).await;

    let mut reply = message(
        "omp-worker",
        task_id,
        MessageKind::Reply,
        "Target v1 only.",
        Some(question_id),
    );
    reply.delivery = DeliveryIntent::FollowUp;
    let CommandOutcome::MessageCreated {
        message_id: reply_id,
    } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::SendMessage { submission: reply },
        )
        .await
        .expect("Supervisor may answer the blocking Question")
    else {
        panic!("Reply must become a Message")
    };
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Waiting, 0).await;
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id: reply_id,
                native_correlation: "omp-follow-up-2".to_owned(),
            },
        )
        .await
        .expect("accepted Reply must resume work");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Working, 0).await;

    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::CompleteTask {
                manifest: result_manifest(task_id, "first result", attachment.id),
                native_turn_id: "turn-2".to_owned(),
            },
        )
        .await
        .expect("valid Result must be admitted before terminal evidence");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Working, 0).await;
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::RecordTurnCompleted {
                task_id,
                native_turn_id: "turn-2".to_owned(),
                succeeded: true,
            },
        )
        .await
        .expect("successful terminal evidence must make Result reviewable");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Reviewing, 0).await;

    let CommandOutcome::MessageCreated {
        message_id: correction_id,
    } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::SendMessage {
                submission: message(
                    "omp-worker",
                    task_id,
                    MessageKind::Correction,
                    "Add the missing boundary test.",
                    None,
                ),
            },
        )
        .await
        .expect("Correction must be queued as a follow-up turn")
    else {
        panic!("Correction must become a Message")
    };
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Reviewing, 0).await;
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id: correction_id,
                native_correlation: "omp-follow-up-3".to_owned(),
            },
        )
        .await
        .expect("accepted Correction must start the next Result revision");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Working, 1).await;

    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::CompleteTask {
                manifest: result_manifest(task_id, "corrected result", attachment.id),
                native_turn_id: "turn-3".to_owned(),
            },
        )
        .await
        .expect("corrected Result must be admitted");
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordTurnCompleted {
                task_id,
                native_turn_id: "turn-3".to_owned(),
                succeeded: true,
            },
        )
        .await
        .expect("corrected turn must become reviewable");
    let digest = "a".repeat(64);
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::RecordRepositoryObservation {
                observation: observation(task_id, &digest),
            },
        )
        .await
        .expect("Supervisor may index current approval evidence");
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::ApproveTask {
                task_id,
                result_revision: 1,
                observation_digest: digest,
            },
        )
        .await
        .expect("matching current Result and repository evidence must approve");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Approved, 1).await;
}

#[tokio::test]
async fn queued_cancellation_is_terminal_without_native_dispatch() {
    let (_state, coordinator, supervisor, _worker, task_id) = seeded_task().await;
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CancelTask { task_id },
        )
        .await
        .expect("queued Task must cancel immediately");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Cancelled, 0).await;
}

#[tokio::test]
async fn ambiguous_dispatch_requires_digest_confirmed_supervisor_reconciliation() {
    let (_state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("Task must enter dispatching")
    else {
        panic!("dispatch must identify its Message")
    };
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::MarkDeliveryUnknown {
                message_id,
                diagnostic: "provider bytes were written before the pipe closed".to_owned(),
            },
        )
        .await
        .expect("destination Host must preserve ambiguous acceptance");
    assert_task_state(
        &coordinator,
        &supervisor,
        task_id,
        TaskState::DeliveryUnknown,
        0,
    )
    .await;
    let digest = "b".repeat(64);
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::RecordRepositoryObservation {
                observation: observation(task_id, &digest),
            },
        )
        .await
        .expect("Supervisor must capture current repository state");
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::ResolveDeliveryUnknown {
                task_id,
                resolution: DeliveryUnknownResolution::Requeue,
                observation_digest: digest,
                audit_note: "No Task changes are present; safe to create a new attempt.".to_owned(),
            },
        )
        .await
        .expect("digest-confirmed reconciliation must allow explicit requeue");
    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Queued, 0).await;
}

#[tokio::test]
async fn inbox_and_popup_projections_follow_durable_read_markers() {
    let (_state, coordinator, supervisor, worker, _task_id) = seeded_task().await;
    let QueryResult::Inbox(messages) = coordinator
        .query(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorQuery::Inbox,
        )
        .await
        .expect("Worker inbox query must succeed")
    else {
        panic!("inbox query must return Messages")
    };
    assert_eq!(messages.len(), 1);
    let CommandOutcome::InboxMarkedRead { count } = coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::MarkInboxRead {
                message_ids: vec![messages[0].id],
            },
        )
        .await
        .expect("recipient must mark its own Message")
    else {
        panic!("read command must report its count")
    };
    assert_eq!(count, 1);
    let QueryResult::HarnessStatus(status) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorQuery::HarnessStatus,
        )
        .await
        .expect("popup status query must succeed")
    else {
        panic!("status query must return Harness rows")
    };
    assert_eq!(status.len(), 2);
    assert_eq!(
        status
            .iter()
            .find(|row| row.id.as_str() == "omp-worker")
            .expect("Worker row")
            .unread_messages,
        0
    );
}

#[tokio::test]
async fn supervisor_can_queue_a_mutating_task_for_an_explicit_worker() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let coordinator = Coordinator::open(state.path())
        .await
        .expect("Coordinator must open");
    let supervisor = supervisor_definition();
    let CommandOutcome::SupervisorRegistered { capability, .. } = coordinator
        .execute(
            ActorContext::Bootstrap,
            CoordinatorCommand::RegisterSupervisor {
                definition: supervisor,
            },
        )
        .await
        .expect("registration must succeed")
    else {
        panic!("registration must return a capability")
    };
    let worker_id: HarnessId = "omp-worker".parse().expect("ID must be valid");
    let worker = HarnessDefinitionV1 {
        schema_version: SCHEMA_VERSION,
        id: worker_id.clone(),
        kind: HarnessKind::Omp,
        tier: HarnessTier::Worker,
        cwd: PathBuf::from("/tmp/project"),
        launch_profile: Some("omp-worker".to_owned()),
        model: Some("anthropic/claude-sonnet-4".to_owned()),
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorCommand::StartWorker {
                definition: worker,
                profile_snapshot: "profile-v1".to_owned(),
                profile_digest: "0".repeat(64),
            },
        )
        .await
        .expect("Worker start must succeed");
    let task = TaskSubmissionV1 {
        schema_version: SCHEMA_VERSION,
        request_key: Some("task-1".to_owned()),
        worker_id,
        related_task_id: None,
        title: "Implement bounded change".to_owned(),
        instructions: "Change only src/lib.rs and report verification.".to_owned(),
        attachments: Vec::new(),
        repository: TaskRepositoryAuthorityV1 {
            root: PathBuf::from("/tmp/project"),
            access: RepositoryAccess::Mutating,
            write_scopes: vec![WriteScopeV1::ExactFile {
                path: PathBuf::from("src/lib.rs"),
            }],
        },
    };
    let CommandOutcome::TaskCreated { task_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorCommand::CreateTask { submission: task },
        )
        .await
        .expect("Task creation must succeed")
    else {
        panic!("Task creation must return identity")
    };

    let QueryResult::Task(task) = coordinator
        .query(
            ActorContext::Session { capability },
            CoordinatorQuery::GetTask { task_id },
        )
        .await
        .expect("Task query must succeed")
    else {
        panic!("Task query must return a Task")
    };

    assert_eq!(task.state, TaskState::Queued);
}

fn supervisor_definition() -> HarnessDefinitionV1 {
    HarnessDefinitionV1 {
        schema_version: SCHEMA_VERSION,
        id: "supervisor".parse::<HarnessId>().expect("ID must be valid"),
        kind: HarnessKind::Codex,
        tier: HarnessTier::Supervisor,
        cwd: PathBuf::from("/tmp/project"),
        launch_profile: None,
        model: Some("gpt-5.4".to_owned()),
    }
}

async fn seeded_task() -> (
    tempfile::TempDir,
    Coordinator,
    SessionCapability,
    SessionCapability,
    TaskId,
) {
    let state = tempfile::tempdir().expect("state directory must exist");
    let coordinator = Coordinator::open(state.path())
        .await
        .expect("Coordinator must open");
    let CommandOutcome::SupervisorRegistered {
        capability: supervisor,
        ..
    } = coordinator
        .execute(
            ActorContext::Bootstrap,
            CoordinatorCommand::RegisterSupervisor {
                definition: supervisor_definition(),
            },
        )
        .await
        .expect("Supervisor registration must succeed")
    else {
        panic!("Supervisor registration must return a capability")
    };
    let worker_id: HarnessId = "omp-worker".parse().expect("ID must be valid");
    let CommandOutcome::WorkerStarted {
        capability: worker, ..
    } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::StartWorker {
                definition: HarnessDefinitionV1 {
                    schema_version: SCHEMA_VERSION,
                    id: worker_id.clone(),
                    kind: HarnessKind::Omp,
                    tier: HarnessTier::Worker,
                    cwd: PathBuf::from("/tmp/project"),
                    launch_profile: Some("omp-worker".to_owned()),
                    model: None,
                },
                profile_snapshot: "profile-v1".to_owned(),
                profile_digest: "0".repeat(64),
            },
        )
        .await
        .expect("Worker start must succeed")
    else {
        panic!("Worker start must return a capability")
    };
    let CommandOutcome::TaskCreated { task_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CreateTask {
                submission: TaskSubmissionV1 {
                    schema_version: SCHEMA_VERSION,
                    request_key: None,
                    worker_id,
                    related_task_id: None,
                    title: "Lifecycle proof".to_owned(),
                    instructions: "Exercise the durable lifecycle.".to_owned(),
                    attachments: Vec::new(),
                    repository: TaskRepositoryAuthorityV1 {
                        root: PathBuf::from("/tmp/project"),
                        access: RepositoryAccess::Mutating,
                        write_scopes: vec![WriteScopeV1::Subtree {
                            path: PathBuf::from("src"),
                        }],
                    },
                },
            },
        )
        .await
        .expect("Task creation must succeed")
    else {
        panic!("Task creation must return an ID")
    };
    (state, coordinator, supervisor, worker, task_id)
}

fn message(
    to: &str,
    task_id: TaskId,
    kind: MessageKind,
    text: &str,
    reply_to: Option<herdr_harness_coordinator::contract::MessageId>,
) -> MessageSubmissionV1 {
    MessageSubmissionV1 {
        schema_version: SCHEMA_VERSION,
        request_key: None,
        to: to.parse().expect("ID must be valid"),
        task_id: Some(task_id),
        kind,
        text: text.to_owned(),
        attachments: Vec::new(),
        reply_to,
        delivery: DeliveryIntent::FollowUp,
    }
}

fn result_manifest(task_id: TaskId, summary: &str, evidence: AttachmentId) -> ResultManifestV1 {
    ResultManifestV1 {
        schema_version: SCHEMA_VERSION,
        task_id,
        summary: summary.to_owned(),
        changed_files: vec![PathBuf::from("src/lib.rs")],
        verification: vec![VerificationResultV1 {
            command: "cargo test".to_owned(),
            exit_code: 0,
            passed: true,
            evidence,
        }],
        deviations: Vec::new(),
        risks: Vec::new(),
        attachments: Vec::new(),
    }
}

fn observation(task_id: TaskId, digest: &str) -> RepositoryObservationV1 {
    RepositoryObservationV1 {
        schema_version: SCHEMA_VERSION,
        id: RepositoryObservationId::new(),
        task_id,
        checkpoint: ObservationCheckpoint::Approval,
        worktree_root: PathBuf::from("/tmp/project"),
        git_common_dir: PathBuf::from("/tmp/project/.git"),
        head: None,
        branch: Some("main".to_owned()),
        index_digest: "0".repeat(64),
        staged_diff: None,
        unstaged_diff: None,
        untracked: Vec::new(),
        ignored_paths: Vec::new(),
        status_entries: Vec::new(),
        changed_paths: vec![PathBuf::from("src/lib.rs")],
        scope_classifications: Vec::new(),
        command_evidence: vec![CommandEvidenceV1 {
            command: "git status --porcelain=v2 -z".to_owned(),
            version: "git version 2".to_owned(),
            exit_code: 0,
            diagnostics: String::new(),
        }],
        captured_at: Utc::now(),
        digest: digest.to_owned(),
    }
}

async fn assert_task_state(
    coordinator: &Coordinator,
    capability: &SessionCapability,
    task_id: TaskId,
    expected_state: TaskState,
    expected_revision: u32,
) {
    let QueryResult::Task(task) = coordinator
        .query(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorQuery::GetTask { task_id },
        )
        .await
        .expect("Task query must succeed")
    else {
        panic!("Task query must return a Task")
    };
    assert_eq!(task.state, expected_state);
    assert_eq!(task.result_revision, expected_revision);
}
