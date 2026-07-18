use std::{path::PathBuf, sync::Arc};

use herdr_harness_coordinator::{
    broker::{BrokerOperation, BrokerRequest, BrokerServer, call, call_with_connect_retry},
    contract::{HarnessDefinitionV1, HarnessId, HarnessKind, HarnessTier, SCHEMA_VERSION},
    core::{
        ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
        QueryResult,
    },
};

#[tokio::test]
async fn broker_retries_only_prewrite_connect_failures_during_handoff() {
    let state = tempfile::tempdir().expect("state directory");
    let socket = state.path().join("delayed.sock");
    let request = BrokerRequest {
        schema_version: SCHEMA_VERSION,
        request_id: "handoff-connect".to_owned(),
        operation: BrokerOperation::Query {
            actor: ActorContext::Bootstrap,
            query: CoordinatorQuery::ListHarnesses,
        },
    };
    let first = call(&socket, &request)
        .await
        .expect_err("missing socket must fail before write");
    assert!(first.is_retry_safe_connect());

    let coordinator = std::sync::Arc::new(
        Coordinator::open(state.path().join("coordinator"))
            .await
            .expect("Coordinator must open"),
    );
    let delayed_socket = socket.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let server = BrokerServer::bind(coordinator, delayed_socket)
            .await
            .expect("delayed broker must bind");
        server.serve().await.expect("broker must serve");
    });
    let response = call_with_connect_retry(&socket, &request, std::time::Duration::from_secs(2))
        .await
        .expect("pre-write handoff gap must reconnect");
    assert_eq!(response.request_id.as_deref(), Some("handoff-connect"));
    assert!(
        response.error.is_some(),
        "Bootstrap query remains forbidden"
    );
}

#[tokio::test]
async fn unix_jsonl_broker_round_trips_capability_authenticated_core_calls() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let coordinator = Arc::new(
        Coordinator::open(state.path())
            .await
            .expect("Core must open"),
    );
    let socket = state.path().join("broker.sock");
    let server = BrokerServer::bind(Arc::clone(&coordinator), &socket)
        .await
        .expect("broker must bind");
    let server_task = tokio::spawn(server.serve());

    let registration = call(
        &socket,
        &BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: "register-1".to_owned(),
            operation: BrokerOperation::Execute {
                actor: ActorContext::Bootstrap,
                command: CoordinatorCommand::RegisterSupervisor {
                    definition: HarnessDefinitionV1 {
                        schema_version: SCHEMA_VERSION,
                        id: "supervisor".parse::<HarnessId>().expect("valid ID"),
                        kind: HarnessKind::Codex,
                        tier: HarnessTier::Supervisor,
                        cwd: PathBuf::from("/tmp/project"),
                        launch_profile: None,
                        model: None,
                    },
                },
            },
        },
    )
    .await
    .expect("registration frame must round trip");
    assert!(registration.error.is_none());
    let outcome: CommandOutcome = serde_json::from_value(registration.result.expect("result"))
        .expect("command result must retain its type");
    let CommandOutcome::SupervisorRegistered { capability, .. } = outcome else {
        panic!("registration must return a capability")
    };

    let listing = call(
        &socket,
        &BrokerRequest {
            schema_version: SCHEMA_VERSION,
            request_id: "query-1".to_owned(),
            operation: BrokerOperation::Query {
                actor: ActorContext::Session { capability },
                query: CoordinatorQuery::ListHarnesses,
            },
        },
    )
    .await
    .expect("query frame must round trip");
    let result: QueryResult = serde_json::from_value(listing.result.expect("result"))
        .expect("query result must retain its type");
    assert_eq!(
        result,
        QueryResult::Harnesses(vec!["supervisor".parse().expect("valid ID")])
    );

    server_task.abort();
}

#[tokio::test]
async fn broker_rejects_an_unversioned_request_without_closing_the_daemon() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let coordinator = Arc::new(
        Coordinator::open(state.path())
            .await
            .expect("Core must open"),
    );
    let socket = state.path().join("broker.sock");
    let server = BrokerServer::bind(coordinator, &socket)
        .await
        .expect("broker must bind");
    let server_task = tokio::spawn(server.serve());

    let response = call(
        &socket,
        &BrokerRequest {
            schema_version: 2,
            request_id: "future".to_owned(),
            operation: BrokerOperation::Query {
                actor: ActorContext::Bootstrap,
                query: CoordinatorQuery::ListHarnesses,
            },
        },
    )
    .await
    .expect("error response must round trip");
    assert_eq!(
        response.error.expect("version error").category,
        herdr_harness_coordinator::core::ErrorCategory::UnsupportedVersion
    );

    server_task.abort();
}
