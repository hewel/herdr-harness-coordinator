use std::{path::PathBuf, sync::Arc};

use herdr_harness_coordinator::{
    adapter::WorkerCompletionTools,
    broker::BrokerServer,
    contract::{HarnessDefinitionV1, HarnessKind, HarnessTier, SCHEMA_VERSION, TaskId},
    core::{ActorContext, CommandOutcome, Coordinator, CoordinatorCommand},
    host::{render_popup, worker_task_prompt},
};

#[test]
fn worker_task_prompt_requires_a_structured_result_at_the_coordinator_boundary() {
    let task_id =
        TaskId(uuid::Uuid::parse_str("019f7606-a26b-7a41-87dd-95f3a072a226").expect("Task ID"));
    let completion_tools = WorkerCompletionTools {
        attachment_create: "fixture_attachment_create",
        complete: "fixture_complete",
    };

    let prompt = worker_task_prompt(
        task_id,
        "Inspect Cargo.toml without editing files.",
        completion_tools,
    );

    assert_eq!(
        prompt,
        "Inspect Cargo.toml without editing files.\n\nCoordinator completion contract:\n- This is Task 019f7606-a26b-7a41-87dd-95f3a072a226.\n- Normal assistant text is not a Result and does not complete the Task.\n- Execute the requested verification command(s).\n- Call `fixture_attachment_create` with the exact verification output to create immutable evidence.\n- Then call `fixture_complete` exactly once with a `manifest` containing schema_version 1, this task_id, summary, changed_files, at least one verification entry referencing the returned Attachment ID, deviations, risks, and attachments.\n- Do not invent or search for a native turn ID; omit native_turn_id unless the provider explicitly exposes it. The Worker Host binds the Result to terminal provider evidence.\n- Do not finish the native turn until `fixture_complete` reports that the Result was recorded."
    );
}

#[tokio::test]
async fn popup_renders_durable_state_through_the_real_broker_boundary() {
    let state = tempfile::tempdir().expect("state directory");
    let coordinator = Arc::new(
        Coordinator::open(state.path())
            .await
            .expect("Core must open"),
    );
    let CommandOutcome::SupervisorRegistered { capability, .. } = coordinator
        .execute(
            ActorContext::Bootstrap,
            CoordinatorCommand::RegisterSupervisor {
                definition: HarnessDefinitionV1 {
                    schema_version: SCHEMA_VERSION,
                    id: "supervisor".parse().expect("valid ID"),
                    kind: HarnessKind::Codex,
                    tier: HarnessTier::Supervisor,
                    cwd: PathBuf::from("/tmp/project"),
                    launch_profile: None,
                    model: None,
                },
            },
        )
        .await
        .expect("Supervisor registration")
    else {
        panic!("registration must return a capability")
    };
    let bearer = serde_json::to_value(capability)
        .expect("serialize capability")
        .as_str()
        .expect("transparent bearer")
        .to_owned();
    let socket = state.path().join("broker.sock");
    let server = BrokerServer::bind(coordinator, &socket)
        .await
        .expect("broker bind");
    let task = tokio::spawn(server.serve());

    let rendered = render_popup(&socket, bearer)
        .await
        .expect("popup projection");
    assert!(rendered.starts_with("Harness Network\n\n"));
    assert!(rendered.contains("supervisor"));
    assert!(rendered.contains("Tasks"));
    assert!(rendered.contains("Scheduling"));

    task.abort();
}
