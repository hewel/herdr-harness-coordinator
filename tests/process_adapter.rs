use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};

use futures::StreamExt;
use herdr_harness_coordinator::{
    adapter::{
        AdapterEvent, HarnessAdapter, HarnessStartSpec, NativeDeliveryKind, NativeSessionResume,
        ResolvedDelivery,
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

#[tokio::test]
async fn omp_process_adapter_resumes_the_exact_native_session() {
    let temp = TempDir::new().expect("temp directory");
    let arguments = temp.path().join("omp-arguments");
    let provider = executable(
        temp.path(),
        "fake-omp-resume",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'omp/17.0.4'; exit 0; fi
printf '%s\n' "$@" > "$ARGS_PATH"
echo '{"type":"ready"}'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"type":"set_host_tools"'*) printf '{"type":"response","id":"%s","command":"set_host_tools","success":true,"data":{}}\n' "$id" ;;
    *'"type":"get_state"'*) printf '{"type":"response","id":"%s","command":"get_state","success":true,"data":{"sessionId":"omp-existing","isStreaming":false,"queuedMessageCount":0,"model":{"id":"fixture-model"}}}\n' "$id" ;;
    *'"type":"get_messages"'*) printf '{"type":"response","id":"%s","command":"get_messages","success":true,"data":{"messages":[{"role":"user","content":"Coordinator Event ID event-visible"}]}}\n' "$id" ;;
  esac
done
"#,
    );
    let mut resume_spec = spec(&temp, provider);
    resume_spec.environment.insert(
        "ARGS_PATH".to_owned(),
        arguments.to_string_lossy().into_owned(),
    );
    let mut adapter = OmpProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    let native = adapter
        .resume(
            &resume_spec,
            &NativeSessionResume {
                session_id: Some("omp-existing".to_owned()),
                thread_id: None,
            },
        )
        .await
        .expect("resume OMP Session");

    assert_eq!(native.session_id.as_deref(), Some("omp-existing"));
    let arguments = fs::read_to_string(arguments).expect("captured OMP arguments");
    assert!(
        arguments
            .lines()
            .collect::<Vec<_>>()
            .windows(2)
            .any(|pair| pair == ["--resume", "omp-existing"]),
        "OMP must receive the exact durable Session ID: {arguments}"
    );
    assert!(
        adapter
            .conversation_contains("event-visible")
            .await
            .expect("read OMP conversation")
    );
    adapter.stop().await.expect("clean OMP shutdown");
}

fn spec(temp: &TempDir, executable: std::path::PathBuf) -> HarnessStartSpec {
    HarnessStartSpec {
        session_id: HarnessSessionId::new(),
        tier: herdr_harness_coordinator::contract::HarnessTier::Worker,
        executable,
        cwd: temp.path().to_path_buf(),
        provider_state_dir: temp.path().join("state"),
        provider_profile: None,
        model: Some("fixture-model".to_owned()),
        config_overlays: Vec::new(),
        codex_approval_policy: Some(
            herdr_harness_coordinator::contract::CodexApprovalPolicy::Never,
        ),
        codex_sandbox_mode: Some(
            herdr_harness_coordinator::contract::CodexSandboxMode::WorkspaceWrite,
        ),
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
if [ "$1" = "--version" ]; then echo 'omp/17.0.4'; exit 0; fi
echo '{"type":"ready"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"set_host_tools"'*)
      id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
      printf '{"type":"response","id":"%s","command":"set_host_tools","success":true,"data":{"toolNames":["harness_list"]}}\n' "$id"
      ;;
    *'"type":"get_state"'*)
      id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
      printf '{"type":"response","id":"%s","command":"get_state","success":true,"data":{"sessionId":"omp-session","isStreaming":false,"queuedMessageCount":0,"model":{"id":"k3"}}}\n' "$id"
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
    let mut start_spec = spec(&temp, provider);
    start_spec.model = Some("kimi-code/k3:high".to_owned());
    let mut adapter = OmpProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );
    let mut events = adapter.events();

    let native = adapter.start(&start_spec).await.expect("start OMP");
    let acceptance = adapter
        .dispatch(delivery(NativeDeliveryKind::StartTurn))
        .await
        .expect("prompt acceptance");

    assert_eq!(native.session_id.as_deref(), Some("omp-session"));
    assert_eq!(native.observed_version, "omp/17.0.4");
    assert_eq!(native.model.as_deref(), Some("kimi-code/k3:high"));
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

#[test]
fn omp_process_adapter_reports_direct_completion_tools() {
    let adapter = OmpProcessAdapter::new();

    assert_eq!(
        adapter.completion_tools(),
        herdr_harness_coordinator::adapter::WorkerCompletionTools {
            attachment_create: "harness_attachment_create",
            complete: "harness_complete",
        }
    );
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
    *'"method":"mcpServerStatus/list"'*) printf '{"id":"%s","error":{"code":-32601,"message":"method not found"}}\n' "$id" ;;
    *'"method":"turn/start"'*)
      printf '{"id":"%s","result":{"turn":{"id":"turn-1"}}}\n' "$id"
      echo '{"method":"turn/started","params":{"threadId":"child-thread","turn":{"id":"child-turn"}}}'
      echo '{"method":"turn/completed","params":{"threadId":"child-thread","turn":{"id":"child-turn","status":"completed"}}}'
      echo '{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1"}}}'
      echo '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}}'
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
    for _ in 0..6 {
        if let Ok(Some(Ok(event))) =
            tokio::time::timeout(Duration::from_secs(1), events.next()).await
        {
            match event {
                AdapterEvent::TurnStarted { turn_id } => {
                    assert_eq!(turn_id.as_deref(), Some("turn-1"));
                }
                AdapterEvent::TurnCompleted { turn_id, .. } => {
                    assert_eq!(turn_id.as_deref(), Some("turn-1"));
                    completed = true;
                    break;
                }
                _ => {}
            }
        }
    }
    assert!(
        completed,
        "the root turn/completed must be terminal evidence"
    );
    adapter.stop().await.expect("clean Codex shutdown");
}

#[tokio::test]
async fn codex_process_adapter_resumes_the_exact_thread_after_mcp_readiness() {
    let temp = TempDir::new().expect("temp directory");
    let methods = temp.path().join("codex-methods");
    let provider = executable(
        temp.path(),
        "fake-codex-resume",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'codex-cli 0.144.5'; exit 0; fi
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  [ -z "$method" ] || printf '%s\n' "$method" >> "$METHODS_PATH"
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"serverInfo":{"name":"fixture"}}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"thread/start"'*) printf '{"id":"%s","error":{"code":-32600,"message":"fresh start is forbidden during resume"}}\n' "$id" ;;
    *'"method":"thread/resume"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-existing"},"cwd":"%s","model":"fixture-model"}}\n' "$id" "$PWD" ;;
    *'"method":"mcpServerStatus/list"'*) printf '{"id":"%s","result":{"data":[{"name":"herdr","serverInfo":null,"tools":{"harness_list":{},"harness_status":{},"harness_inbox":{},"harness_request":{},"harness_send":{},"harness_complete":{},"harness_attachment_create":{}},"resources":[],"resourceTemplates":[],"authStatus":"notLoginRequired"}],"nextCursor":null}}\n' "$id" ;;
    *'"method":"thread/read"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-existing","turns":[{"items":[{"type":"userMessage","text":"Coordinator Event ID event-visible"}]}]}}}\n' "$id" ;;
  esac
done
"#,
    );
    let mut resume_spec = spec(&temp, provider);
    resume_spec.environment.insert(
        "METHODS_PATH".to_owned(),
        methods.to_string_lossy().into_owned(),
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    let native = adapter
        .resume(
            &resume_spec,
            &NativeSessionResume {
                session_id: None,
                thread_id: Some("thread-existing".to_owned()),
            },
        )
        .await
        .expect("resume Codex thread");

    assert_eq!(native.thread_id.as_deref(), Some("thread-existing"));
    let methods = fs::read_to_string(methods).expect("captured Codex methods");
    assert!(methods.contains("thread/resume"));
    assert!(methods.contains("mcpServerStatus/list"));
    assert!(!methods.contains("thread/start"));
    assert!(
        adapter
            .conversation_contains("event-visible")
            .await
            .expect("read Codex conversation")
    );
    adapter.stop().await.expect("clean Codex shutdown");
}

#[tokio::test]
async fn codex_process_adapter_rejects_resume_identity_drift() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(
        temp.path(),
        "fake-codex-resume-drift",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'codex-cli 0.144.5'; exit 0; fi
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"serverInfo":{"name":"fixture"}}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"thread/resume"'*) printf '{"id":"%s","result":{"thread":{"id":"different-thread"},"cwd":"%s","model":"fixture-model"}}\n' "$id" "$PWD" ;;
  esac
done
"#,
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    let error = adapter
        .resume(
            &spec(&temp, provider),
            &NativeSessionResume {
                session_id: None,
                thread_id: Some("thread-existing".to_owned()),
            },
        )
        .await
        .expect_err("resume identity drift must fail closed");

    assert!(
        error
            .to_string()
            .contains("resumed thread identity mismatch")
    );
}

#[tokio::test]
async fn codex_process_adapter_fails_closed_when_herdr_mcp_tools_are_missing() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(
        temp.path(),
        "fake-codex-missing-mcp",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'codex-cli 0.144.5'; exit 0; fi
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"serverInfo":{"name":"fixture"}}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"thread/start"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-1","cwd":"%s","model":"fixture-model"}}}\n' "$id" "$PWD" ;;
    *'"method":"mcpServerStatus/list"'*) printf '{"id":"%s","result":{"data":[],"nextCursor":null}}\n' "$id" ;;
  esac
done
"#,
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    let error = adapter
        .start(&spec(&temp, provider))
        .await
        .expect_err("missing herdr MCP readiness must fail startup");

    assert!(error.to_string().contains("herdr MCP server is not ready"));
}

#[test]
fn codex_process_adapter_reports_orchestrated_completion_tools() {
    let adapter = CodexProcessAdapter::new();

    assert_eq!(
        adapter.completion_tools(),
        herdr_harness_coordinator::adapter::WorkerCompletionTools {
            attachment_create: "tools.mcp__herdr__harness_attachment_create",
            complete: "tools.mcp__herdr__harness_complete",
        }
    );
}

#[tokio::test]
async fn codex_worker_configures_the_identity_bound_coordinator_mcp_server() {
    let temp = TempDir::new().expect("temp directory");
    let arguments = temp.path().join("codex-arguments");
    let provider = executable(
        temp.path(),
        "fake-codex",
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'codex-cli 0.144.5'; exit 0; fi
printf '%s\n' "$@" > "$ARGS_PATH"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"serverInfo":{"name":"fixture"}}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"thread/start"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-1","cwd":"%s","model":"fixture-model"}}}\n' "$id" "$PWD" ;;
    *'"method":"mcpServerStatus/list"'*) printf '{"id":"%s","error":{"code":-32601,"message":"method not found"}}\n' "$id" ;;
  esac
done
"#,
    );
    let mut worker_spec = spec(&temp, provider);
    worker_spec.environment.insert(
        "ARGS_PATH".to_owned(),
        arguments.to_string_lossy().into_owned(),
    );
    worker_spec.environment.insert(
        "HERDR_COORDINATOR_SOCKET".to_owned(),
        "/tmp/coordinator.sock".to_owned(),
    );
    worker_spec.environment.insert(
        "HERDR_HARNESS_CAPABILITY".to_owned(),
        "worker-capability".to_owned(),
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    adapter
        .start(&worker_spec)
        .await
        .expect("start Codex Worker");

    let arguments = fs::read_to_string(arguments).expect("captured Codex arguments");
    assert!(arguments.contains("mcp_servers.herdr.command="));
    assert!(arguments.contains("mcp_servers.herdr.args=[\"mcp\"]"));
    assert!(arguments.contains("mcp_servers.herdr.env_vars=[\"HERDR_HARNESS_CAPABILITY\",\"HERDR_HARNESS_ACTOR\",\"HERDR_PLUGIN_STATE_DIR\",\"HERDR_COORDINATOR_SOCKET\"]"));
    adapter.stop().await.expect("clean Codex shutdown");
}

#[tokio::test]
async fn codex_snapshot_does_not_request_turns_from_an_unmaterialized_thread() {
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
    *'"method":"mcpServerStatus/list"'*) printf '{"id":"%s","error":{"code":-32601,"message":"method not found"}}\n' "$id" ;;
    *'"method":"thread/read"'*'"includeTurns":true'*) printf '{"id":"%s","error":{"code":-32600,"message":"thread is not materialized yet; includeTurns is unavailable before first user message"}}\n' "$id" ;;
    *'"method":"thread/read"'*) printf '{"id":"%s","result":{"thread":{"id":"thread-1"}}}\n' "$id" ;;
  esac
done
"#,
    );
    let mut adapter = CodexProcessAdapter::new().with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    adapter
        .start(&spec(&temp, provider))
        .await
        .expect("start Codex");

    adapter
        .snapshot()
        .await
        .expect("snapshot unmaterialized Codex thread");
    adapter.stop().await.expect("clean Codex shutdown");
}

#[tokio::test]
async fn process_adapter_rejects_malformed_version_evidence_before_launch() {
    let temp = TempDir::new().expect("temp directory");
    let provider = executable(temp.path(), "wrong-omp", "#!/bin/sh\necho '   '\n");
    let mut adapter = OmpProcessAdapter::new();

    let error = adapter
        .start(&spec(&temp, provider))
        .await
        .expect_err("malformed version evidence must fail");

    assert!(error.to_string().contains("invalid Omp version"));
}
