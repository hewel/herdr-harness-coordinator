use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};

use futures::StreamExt;
use herdr_harness_coordinator::{
    adapter::{
        AdapterEvent, HarnessAdapter, HarnessStartSpec, NativeDeliveryKind, ResolvedDelivery,
    },
    contract::{HarnessSessionId, TaskId},
    process_adapter::{CodexProcessAdapter, OmpProcessAdapter},
};
use tempfile::TempDir;

fn executable(directory: &Path, name: &str, source: &str) -> std::path::PathBuf {
    let path = directory.join(name);
    fs::write(&path, source).expect("write fake provider");
    let mut permissions = fs::metadata(&path)
        .expect("provider metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make provider executable");
    path
}

fn spec(temp: &TempDir, executable: std::path::PathBuf) -> HarnessStartSpec {
    HarnessStartSpec {
        session_id: HarnessSessionId::new(),
        executable,
        cwd: temp.path().to_path_buf(),
        provider_state_dir: temp.path().join("state"),
        provider_profile: "fixture-profile".to_owned(),
        model: Some("fixture-model".to_owned()),
        config_overlays: Vec::new(),
        environment: BTreeMap::new(),
    }
}

fn delivery(kind: NativeDeliveryKind) -> ResolvedDelivery {
    ResolvedDelivery {
        correlation: "delivery-7".to_owned(),
        task_id: Some(TaskId::new()),
        kind,
        text: "do the bounded work".to_owned(),
        attachments: Vec::new(),
    }
}

#[tokio::test]
async fn omp_process_adapter_separates_acceptance_from_agent_end() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(
        temp.path(),
        "fake-omp",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'omp/17.0.2'; exit 0; fi
echo '{"type":"ready"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"get_state"'*)
      id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
      printf '{"type":"response","id":"%s","command":"get_state","success":true,"data":{"sessionId":"omp-session","isStreaming":false,"queuedMessageCount":0,"model":{"id":"fixture-model"}}}\n' "$id"
      ;;
    *'"type":"prompt"'*)
      id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
      printf '{"type":"response","id":"%s","command":"prompt","success":true,"data":{"agentInvoked":true}}\n' "$id"
      echo '{"type":"agent_start"}'
      echo '{"type":"agent_end","messages":[]}'
      ;;
  esac
done
"#,
    );
    let mut adapter = OmpProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );
    let mut events = adapter.events();

    let native = adapter
        .start(&spec(&temp, provider))
        .await
        .expect("start OMP");
    let acceptance = adapter
        .dispatch(delivery(NativeDeliveryKind::StartTurn))
        .await
        .expect("prompt acceptance");

    assert_eq!(native.session_id.as_deref(), Some("omp-session"));
    assert_eq!(acceptance.correlation, "delivery-7");
    let mut completed = false;
    for _ in 0..4 {
        if matches!(
            tokio::time::timeout(Duration::from_secs(1), events.next()).await,
            Ok(Some(Ok(AdapterEvent::TurnCompleted { .. })))
        ) {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "agent_end must become separate terminal evidence"
    );
    adapter.stop().await.expect("clean OMP shutdown");
}

#[tokio::test]
async fn codex_process_adapter_initializes_thread_and_observes_completion() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(
        temp.path(),
        "fake-codex",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'codex-cli 0.144.5'; exit 0; fi
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"serverInfo":{"name":"fixture"}}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"thread/start"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-1","cwd":"%s","model":"fixture-model"}}}\n' "$id" "$PWD" ;;
    *'"method":"turn/start"'*)
      printf '{"id":"%s","result":{"turn":{"id":"turn-1"}}}\n' "$id"
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}'
      echo '{"method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}'
      ;;
  esac
done
"#,
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );
    let mut events = adapter.events();

    let native = adapter
        .start(&spec(&temp, provider))
        .await
        .expect("start Codex");
    let acceptance = adapter
        .dispatch(delivery(NativeDeliveryKind::StartTurn))
        .await
        .expect("turn acceptance");

    assert_eq!(native.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(acceptance.turn_id.as_deref(), Some("turn-1"));
    let mut completed = false;
    for _ in 0..4 {
        if matches!(
            tokio::time::timeout(Duration::from_secs(1), events.next()).await,
            Ok(Some(Ok(AdapterEvent::TurnCompleted { .. })))
        ) {
            completed = true;
            break;
        }
    }
    assert!(completed, "turn/completed must be terminal evidence");
    adapter.stop().await.expect("clean Codex shutdown");
}

#[tokio::test]
async fn process_adapter_rejects_an_unverified_version_before_launch() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(temp.path(), "wrong-omp", "#!/bin/sh\necho 'omp/17.0.3'\n");
    let mut adapter = OmpProcessAdapter::new();

    let error = adapter
        .start(&spec(&temp, provider))
        .await
        .expect_err("unverified version must fail");

    assert!(error.to_string().contains("unsupported Omp version"));
}
