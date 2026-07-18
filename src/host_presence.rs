//! Independent Host presence renewal that cannot be starved by provider I/O.

use std::{path::PathBuf, time::Duration};

use anyhow::{Error, anyhow};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    broker::{BrokerOperation, BrokerRequest, call_with_connect_retry},
    contract::SCHEMA_VERSION,
    core::{ActorContext, CoordinatorCommand, HostConnectionCapability},
};

pub(crate) struct HostHeartbeat {
    failures: mpsc::Receiver<Error>,
    task: JoinHandle<()>,
}

impl HostHeartbeat {
    pub(crate) fn spawn(
        socket: PathBuf,
        capability: HostConnectionCapability,
        period: Duration,
    ) -> Self {
        let (failure_tx, failures) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            loop {
                ticker.tick().await;
                let request = BrokerRequest {
                    schema_version: SCHEMA_VERSION,
                    request_id: uuid::Uuid::now_v7().to_string(),
                    operation: BrokerOperation::Execute {
                        actor: ActorContext::Host {
                            capability: capability.clone(),
                        },
                        command: CoordinatorCommand::RenewHostConnection,
                    },
                };
                let result = call_with_connect_retry(&socket, &request, Duration::from_secs(5))
                    .await
                    .map_err(Error::from)
                    .and_then(|response| {
                        if let Some(error) = response.error {
                            Err(anyhow!("Host heartbeat rejected: {}", error.message))
                        } else {
                            Ok(())
                        }
                    });
                if let Err(error) = result {
                    let _ = failure_tx.send(error).await;
                    break;
                }
            }
        });
        Self { failures, task }
    }

    pub(crate) async fn failed(&mut self) -> Error {
        self.failures
            .recv()
            .await
            .unwrap_or_else(|| anyhow!("Host heartbeat task ended unexpectedly"))
    }
}

impl Drop for HostHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        broker::BrokerServer,
        contract::{HarnessDefinitionV1, HarnessId, HarnessKind, HarnessTier, SCHEMA_VERSION},
        core::{
            ActorContext, CommandOutcome, Coordinator, CoordinatorCommand, CoordinatorQuery,
            QueryResult,
        },
    };

    use super::HostHeartbeat;

    #[tokio::test]
    async fn provider_delay_cannot_starve_host_presence_renewal() {
        let state = tempfile::tempdir().expect("state directory");
        let coordinator = Arc::new(
            Coordinator::open(state.path())
                .await
                .expect("Coordinator opens"),
        );
        let CommandOutcome::SupervisorRegistered {
            capability: session,
            ..
        } = coordinator
            .execute(
                ActorContext::Bootstrap,
                CoordinatorCommand::RegisterSupervisor {
                    definition: HarnessDefinitionV1 {
                        schema_version: SCHEMA_VERSION,
                        id: "supervisor".parse::<HarnessId>().expect("Harness ID"),
                        kind: HarnessKind::Codex,
                        tier: HarnessTier::Supervisor,
                        cwd: state.path().to_path_buf(),
                        launch_profile: None,
                        model: None,
                    },
                },
            )
            .await
            .expect("Supervisor registers")
        else {
            panic!("registration returns capability")
        };
        let CommandOutcome::HostConnectionBound {
            capability: host, ..
        } = coordinator
            .execute(
                ActorContext::Session {
                    capability: session,
                },
                CoordinatorCommand::BindHostConnection {
                    instance_id: "delayed-provider".to_owned(),
                    lease_seconds: 1,
                },
            )
            .await
            .expect("Host binds")
        else {
            panic!("Host bind returns capability")
        };
        let socket = state.path().join("heartbeat.sock");
        let server = BrokerServer::bind(Arc::clone(&coordinator), &socket)
            .await
            .expect("broker binds");
        let server_task = tokio::spawn(server.serve());
        let heartbeat =
            HostHeartbeat::spawn(socket, host.clone(), std::time::Duration::from_millis(100));

        tokio::time::sleep(std::time::Duration::from_millis(1_250)).await;
        assert!(matches!(
            coordinator
                .execute(
                    ActorContext::Bootstrap,
                    CoordinatorCommand::ReapStaleHostConnections,
                )
                .await
                .expect("healthy Host must not be reaped"),
            CommandOutcome::StaleHostConnectionsReaped { count: 0 }
        ));
        assert!(matches!(
            coordinator
                .query(
                    ActorContext::Host { capability: host },
                    CoordinatorQuery::SessionSelf,
                )
                .await
                .expect("renewed Host remains authenticated"),
            QueryResult::Session(_)
        ));

        drop(heartbeat);
        server_task.abort();
    }
}
