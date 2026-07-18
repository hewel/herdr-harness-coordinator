use std::path::PathBuf;

use herdr_harness_coordinator::{
    contract::{
        AttachmentId, DeliveryIntent, HarnessDefinitionV1, HarnessId, HarnessKind, HarnessTier,
        MessageKind, MessageSubmissionV1, ObservationCheckpoint, RepositoryAccess,
        ResultManifestV1, SCHEMA_VERSION, TaskId, TaskRepositoryAuthorityV1, TaskSubmissionV1,
        VerificationResultV1, WriteScopeV1,
    },
    core::{
        ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
        DeliveryUnknownResolution, QueryResult, SessionCapability, TaskState,
    },
};
use sha2::{Digest, Sha256};

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
    let CommandOutcome::SupervisorRegistered { capability, .. } = outcome else {
        panic!("registration returned the wrong outcome")
    };

    let result = coordinator
        .query(
            ActorContext::Session { capability },
            CoordinatorQuery::ListHarnesses,
        )
        .await
        .expect("authenticated query must succeed");
    let QueryResult::Harnesses(harnesses) = result else {
        panic!("list query returned the wrong result")
    };

    assert_eq!(
        harnesses,
        vec!["supervisor".parse().expect("ID must be valid")]
    );
}

#[tokio::test]
async fn request_keys_replay_original_outcomes_and_reject_changed_payloads() {
    let (state, coordinator, supervisor, _worker, _task_id) = seeded_task().await;
    let worker_id: HarnessId = "omp-worker".parse().expect("ID must be valid");
    let submission = TaskSubmissionV1 {
        schema_version: SCHEMA_VERSION,
        request_key: Some("task-retry-1".to_owned()),
        worker_id: worker_id.clone(),
        related_task_id: None,
        title: "Idempotent task".to_owned(),
        instructions: "Prove that task retries return the original outcome.".to_owned(),
        attachments: Vec::new(),
        repository: TaskRepositoryAuthorityV1 {
            root: state.path().join("project"),
            access: RepositoryAccess::ReadOnly,
            write_scopes: Vec::new(),
        },
    };
    let first = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CreateTask {
                submission: submission.clone(),
            },
        )
        .await
        .expect("first keyed Task must succeed");
    let replay = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CreateTask {
                submission: submission.clone(),
            },
        )
        .await
        .expect("same keyed Task must replay");
    assert_eq!(
        serde_json::to_value(first).expect("outcome serializes"),
        serde_json::to_value(replay).expect("outcome serializes")
    );

    let mut changed = submission;
    changed.title = "Changed payload".to_owned();
    let error = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorCommand::CreateTask {
                submission: changed,
            },
        )
        .await
        .expect_err("changed payload must conflict");
    assert_eq!(
        error.category,
        herdr_harness_coordinator::core::ErrorCategory::Conflict
    );
}

#[tokio::test]
async fn taskless_notifications_remain_on_the_supervisor_worker_star() {
    let (_state, coordinator, supervisor, worker, _task_id) = seeded_task().await;
    let submission = MessageSubmissionV1 {
        schema_version: SCHEMA_VERSION,
        request_key: Some("notification-retry-1".to_owned()),
        to: "omp-worker".parse().expect("ID must be valid"),
        task_id: None,
        kind: MessageKind::Notification,
        text: "Coordinator notice".to_owned(),
        attachments: Vec::new(),
        reply_to: None,
        delivery: DeliveryIntent::FollowUp,
    };
    let first = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::SendMessage {
                submission: submission.clone(),
            },
        )
        .await
        .expect("Supervisor notification must succeed");
    let replay = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorCommand::SendMessage { submission },
        )
        .await
        .expect("notification retry must replay");
    assert_eq!(
        serde_json::to_value(first).expect("outcome serializes"),
        serde_json::to_value(replay).expect("outcome serializes")
    );

    let error = coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::SendMessage {
                submission: MessageSubmissionV1 {
                    schema_version: SCHEMA_VERSION,
                    request_key: None,
                    to: "omp-worker".parse().expect("ID must be valid"),
                    task_id: None,
                    kind: MessageKind::Notification,
                    text: "self route".to_owned(),
                    attachments: Vec::new(),
                    reply_to: None,
                    delivery: DeliveryIntent::FollowUp,
                },
            },
        )
        .await
        .expect_err("Worker self-route must be forbidden");
    assert_eq!(
        error.category,
        herdr_harness_coordinator::core::ErrorCategory::Forbidden
    );
}

#[tokio::test]
async fn active_mutating_lease_is_held_across_coordinator_instances() {
    let (state, coordinator, supervisor, _worker, task_id) = seeded_task().await;
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("first Coordinator must acquire the lease");

    let error = Coordinator::open(state.path())
        .await
        .expect_err("second Coordinator must not share the mutating lease");
    assert_eq!(
        error.category,
        herdr_harness_coordinator::core::ErrorCategory::RepositoryBlocked
    );
}

#[tokio::test]
async fn worker_host_failure_records_repository_evidence_and_a_hold() {
    let (_state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("Task must dispatch")
    else {
        panic!("dispatch must return its Message")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation: "native-turn".to_owned(),
            },
        )
        .await
        .expect("provider must accept the Task");
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordHostFailed {
                diagnostic: "provider process exited unexpectedly".to_owned(),
            },
        )
        .await
        .expect("Host failure must settle durably");

    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Failed, 0).await;
    let QueryResult::Holds(holds) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorQuery::ActiveHolds,
        )
        .await
        .expect("Hold query must succeed")
    else {
        panic!("Hold query must return Holds")
    };
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].task_id, task_id);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "end-to-end proof covers capture, Result delivery, Hold, and approval rejection"
)]
async fn out_of_scope_result_is_delivered_for_review_but_cannot_be_approved() {
    let (state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let evidence_path = state.path().join("verification.txt");
    std::fs::write(&evidence_path, "focused verification passed\n").expect("evidence fixture");
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
        .expect("evidence admission must succeed")
    else {
        panic!("admission must return metadata")
    };
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("Task must dispatch")
    else {
        panic!("dispatch must return its Message")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation: "turn-out-of-scope".to_owned(),
            },
        )
        .await
        .expect("Task delivery must be accepted");
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::CompleteTask {
                manifest: result_manifest(task_id, "unsafe result", attachment.id),
                native_turn_id: "turn-out-of-scope".to_owned(),
            },
        )
        .await
        .expect("Result candidate must be retained");
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordTurnCompleted {
                task_id,
                native_turn_id: "turn-out-of-scope".to_owned(),
                succeeded: true,
            },
        )
        .await
        .expect("terminal evidence must preserve the Result for review");
    std::fs::write(state.path().join("project/outside.txt"), "unauthorized\n")
        .expect("post-Result out-of-scope fixture");

    let QueryResult::Inbox(inbox) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorQuery::Inbox,
        )
        .await
        .expect("Supervisor inbox query")
    else {
        panic!("inbox query must return Messages")
    };
    assert!(inbox.iter().any(|message| message.kind == "result"));
    let CommandOutcome::ObservationRecorded { digest, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CaptureRepositoryObservation {
                task_id,
                checkpoint: ObservationCheckpoint::Approval,
            },
        )
        .await
        .expect("approval checkpoint must capture")
    else {
        panic!("capture must return digest")
    };
    let error = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::ApproveTask {
                task_id,
                result_revision: 0,
                observation_digest: digest,
            },
        )
        .await
        .expect_err("Hold must prevent approval");
    assert_eq!(
        error.category,
        herdr_harness_coordinator::core::ErrorCategory::RepositoryBlocked
    );
    let QueryResult::Holds(holds) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorQuery::ActiveHolds,
        )
        .await
        .expect("Hold query")
    else {
        panic!("Hold query must return Holds")
    };
    assert_eq!(holds.len(), 1);
}

#[tokio::test]
async fn repository_observation_failure_fails_active_task_and_creates_hold() {
    let (state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("Task must dispatch")
    else {
        panic!("dispatch must return its Message")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation: "turn-repository-loss".to_owned(),
            },
        )
        .await
        .expect("Task delivery must be accepted");
    std::fs::rename(
        state.path().join("project/.git"),
        state.path().join("removed-git"),
    )
    .expect("repository fixture must become unavailable");
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordTurnCompleted {
                task_id,
                native_turn_id: "turn-repository-loss".to_owned(),
                succeeded: false,
            },
        )
        .await
        .expect("evidence failure must settle conservatively");

    assert_task_state(&coordinator, &supervisor, task_id, TaskState::Failed, 0).await;
    let QueryResult::Holds(holds) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorQuery::ActiveHolds,
        )
        .await
        .expect("Hold query")
    else {
        panic!("Hold query must return Holds")
    };
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].reason, "repository_observation_failed");
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "single end-to-end lifecycle proof")]
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
    let CommandOutcome::ObservationRecorded { digest, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CaptureRepositoryObservation {
                task_id,
                checkpoint: ObservationCheckpoint::Approval,
            },
        )
        .await
        .expect("Supervisor may capture current approval evidence")
    else {
        panic!("capture must return a digest")
    };
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
async fn mutating_cancellation_hold_clears_only_with_current_digest_and_audit_note() {
    let (_state, coordinator, supervisor, worker, task_id) = seeded_task().await;
    let CommandOutcome::TaskDispatching { message_id, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::DispatchTask { task_id },
        )
        .await
        .expect("dispatch")
    else {
        panic!("dispatch outcome")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::AcceptDelivery {
                message_id,
                native_correlation: "prompt-1".to_owned(),
            },
        )
        .await
        .expect("acceptance");
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CancelTask { task_id },
        )
        .await
        .expect("cancellation intent");
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordCancellationCompleted {
                task_id,
                succeeded: true,
            },
        )
        .await
        .expect("provider cancellation evidence");
    let CommandOutcome::ObservationRecorded { digest, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CaptureRepositoryObservation {
                task_id,
                checkpoint: ObservationCheckpoint::HoldClear,
            },
        )
        .await
        .expect("reconciliation Observation")
    else {
        panic!("capture must return a digest")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::ClearWorktreeHold {
                task_id,
                observation_digest: digest,
                audit_note:
                    "Repository inspected after cancellation; retained changes are expected."
                        .to_owned(),
            },
        )
        .await
        .expect("digest-confirmed Hold clearance");
    let QueryResult::Holds(holds) = coordinator
        .query(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorQuery::ActiveHolds,
        )
        .await
        .expect("Hold query")
    else {
        panic!("Hold query result")
    };
    assert!(holds.is_empty());
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
    let CommandOutcome::ObservationRecorded { digest, .. } = coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor.clone(),
            },
            CoordinatorCommand::CaptureRepositoryObservation {
                task_id,
                checkpoint: ObservationCheckpoint::HoldClear,
            },
        )
        .await
        .expect("Supervisor must capture current repository state")
    else {
        panic!("capture must return a digest")
    };
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
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::ClaimNextTask,
        )
        .await
        .expect("Worker Host must claim its oldest queued Task");
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
async fn host_events_replay_idempotently_and_supervisor_can_stop_an_idle_worker() {
    let (_state, coordinator, supervisor, worker, _task_id) = seeded_task().await;
    let event = serde_json::json!({"kind":"activity","summary":"started"});
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::RecordHostEvent {
                sequence: 1,
                event: event.clone(),
            },
        )
        .await
        .expect("first Host event");
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::RecordHostEvent { sequence: 1, event },
        )
        .await
        .expect("identical replay must be idempotent");
    coordinator
        .execute(
            ActorContext::Session {
                capability: supervisor,
            },
            CoordinatorCommand::StopWorker {
                worker_id: "omp-worker".parse().expect("valid ID"),
            },
        )
        .await
        .expect("Supervisor stop intent");
    let QueryResult::Session(session) = coordinator
        .query(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorQuery::SessionSelf,
        )
        .await
        .expect("Host launch state")
    else {
        panic!("Session projection")
    };
    assert_eq!(session.activity, "stopping");
    assert_eq!(session.event_sequence, 1);
    coordinator
        .execute(
            ActorContext::Session { capability: worker },
            CoordinatorCommand::RecordHostStopped { clean: true },
        )
        .await
        .expect("idle Host stop completion");
}

#[tokio::test]
async fn supervisor_can_queue_a_mutating_task_for_an_explicit_worker() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let repository_root = state.path().join("project");
    std::fs::create_dir_all(&repository_root).expect("repository fixture directory");
    let status = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&repository_root)
        .status()
        .expect("git init must run");
    assert!(status.success(), "git init must succeed");
    let coordinator = Coordinator::open(state.path())
        .await
        .expect("Coordinator must open");
    let supervisor = supervisor_definition(repository_root.clone());
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
        cwd: repository_root.clone(),
        launch_profile: Some("omp-worker".to_owned()),
        model: Some("anthropic/claude-sonnet-4".to_owned()),
    };
    let (profile_snapshot, profile_digest) = worker_profile(Some("anthropic/claude-sonnet-4"));
    coordinator
        .execute(
            ActorContext::Session {
                capability: capability.clone(),
            },
            CoordinatorCommand::StartWorker {
                definition: worker,
                profile_snapshot,
                profile_digest,
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
            root: repository_root,
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

fn supervisor_definition(cwd: PathBuf) -> HarnessDefinitionV1 {
    HarnessDefinitionV1 {
        schema_version: SCHEMA_VERSION,
        id: "supervisor".parse::<HarnessId>().expect("ID must be valid"),
        kind: HarnessKind::Codex,
        tier: HarnessTier::Supervisor,
        cwd,
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
    let repository_root = state.path().join("project");
    std::fs::create_dir_all(repository_root.join("src")).expect("repository fixture directory");
    let status = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&repository_root)
        .status()
        .expect("git init must run");
    assert!(status.success(), "git init must succeed");
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
                definition: supervisor_definition(repository_root.clone()),
            },
        )
        .await
        .expect("Supervisor registration must succeed")
    else {
        panic!("Supervisor registration must return a capability")
    };
    let worker_id: HarnessId = "omp-worker".parse().expect("ID must be valid");
    let (profile_snapshot, profile_digest) = worker_profile(None);
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
                    cwd: repository_root.clone(),
                    launch_profile: Some("omp-worker".to_owned()),
                    model: None,
                },
                profile_snapshot,
                profile_digest,
            },
        )
        .await
        .expect("Worker start must succeed")
    else {
        panic!("Worker start must return a capability")
    };
    coordinator
        .execute(
            ActorContext::Session {
                capability: worker.clone(),
            },
            CoordinatorCommand::RecordHostReady,
        )
        .await
        .expect("test Worker Host must become ready");
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
                        root: repository_root,
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

fn worker_profile(model: Option<&str>) -> (String, String) {
    let executable = std::env::current_exe().expect("test executable path");
    let executable = executable.display();
    let model = model.map_or_else(String::new, |model| format!("model = {model:?}\n"));
    let snapshot = format!(
        "schema_version = 1\nid = \"omp-worker\"\nkind = \"omp\"\nexecutable = \"{executable}\"\nprovider_profile = \"test-worker\"\n{model}"
    );
    let digest = hex::encode(Sha256::digest(snapshot.as_bytes()));
    (snapshot, digest)
}
