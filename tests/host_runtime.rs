use std::{path::PathBuf, sync::Arc};

use herdr_harness_coordinator::{
    broker::BrokerServer,
    contract::{HarnessDefinitionV1, HarnessKind, HarnessTier, SCHEMA_VERSION, TaskId},
    core::{ActorContext, CommandOutcome, Coordinator, CoordinatorCommand},
    host::{render_popup, worker_task_prompt},
};

#[test]
fn worker_task_prompt_requires_a_structured_result_at_the_coordinator_boundary() {
    let task_id =
        TaskId(uuid::Uuid::parse_str("019f7606-a26b-7a41-87dd-95f3a072a226").expect("Task ID"));

    let prompt = worker_task_prompt(task_id, "Inspect Cargo.toml without editing files.");

    assert!(prompt.contains("Inspect Cargo.toml without editing files."));
    assert!(prompt.contains("harness_complete"));
    assert!(prompt.contains("tools.mcp__herdr__harness_complete"));
    assert!(prompt.contains("019f7606-a26b-7a41-87dd-95f3a072a226"));
    assert!(prompt.contains("Normal assistant text is not a Result"));
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
