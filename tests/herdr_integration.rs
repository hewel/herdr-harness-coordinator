use std::{collections::BTreeMap, path::PathBuf};

use herdr_harness_coordinator::herdr::{
    HERDR_PROTOCOL, HERDR_VERSION, HerdrSocketClient, MetadataProjection, PaneInfo, PaneLocation,
    PluginPaneOpenParams, SessionSnapshot,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

#[test]
fn resolves_a_current_pane_location_by_stable_terminal_identity() {
    let snapshot = SessionSnapshot {
        version: HERDR_VERSION.to_owned(),
        protocol: HERDR_PROTOCOL,
        panes: vec![
            pane("1-1", "terminal-supervisor", "1", "1:1"),
            pane("2-3", "terminal-worker", "2", "2:2"),
        ],
    };

    assert_eq!(
        snapshot.resolve_terminal("terminal-worker").unwrap(),
        PaneLocation {
            pane_id: "2-3".to_owned(),
            terminal_id: "terminal-worker".to_owned(),
            workspace_id: "2".to_owned(),
            tab_id: "2:2".to_owned(),
        }
    );
}

#[test]
fn rejects_a_snapshot_from_an_unverified_herdr_protocol() {
    let snapshot = SessionSnapshot {
        version: HERDR_VERSION.to_owned(),
        protocol: HERDR_PROTOCOL + 1,
        panes: Vec::new(),
    };

    let error = snapshot.validate_compatibility().unwrap_err();
    assert!(error.to_string().contains("protocol 16"));
}

#[test]
fn accepts_a_newer_herdr_release_when_socket_protocol_is_compatible() {
    let snapshot = SessionSnapshot {
        version: "0.9.1".to_owned(),
        protocol: HERDR_PROTOCOL,
        panes: Vec::new(),
    };

    snapshot.validate_compatibility().unwrap();
}

#[test]
fn worker_open_request_is_unfocused_and_carries_the_durable_session_capability() {
    let request = PluginPaneOpenParams::worker(
        "session-capability-1",
        &PathBuf::from("/repo"),
        Some("4".to_owned()),
    );

    assert_eq!(request.entrypoint, "worker");
    assert_eq!(request.placement.as_deref(), Some("tab"));
    assert!(!request.focus);
    assert_eq!(request.workspace_id.as_deref(), Some("4"));
    assert_eq!(request.cwd, None);
    assert_eq!(
        request.env.get("HERDR_HARNESS_CWD").map(String::as_str),
        Some("/repo")
    );
    assert_eq!(
        request
            .env
            .get("HERDR_HARNESS_SESSION_ID")
            .map(String::as_str),
        Some("session-capability-1")
    );
}

#[test]
fn supervisor_open_request_preserves_the_plugin_root_for_command_resolution() {
    let request = PluginPaneOpenParams::supervisor(
        "supervisor-capability-1",
        &PathBuf::from("/repo"),
        Some("4".to_owned()),
    );

    assert_eq!(request.entrypoint, "supervisor");
    assert_eq!(request.cwd, None);
}

#[test]
fn popup_open_request_targets_the_invoking_workspace() {
    let request = PluginPaneOpenParams::popup("wF".to_owned());

    assert_eq!(request.workspace_id.as_deref(), Some("wF"));
    assert!(request.focus);
}

#[test]
fn metadata_projection_does_not_claim_agent_status_authority() {
    let params = MetadataProjection {
        title: "OMP Worker".to_owned(),
        state: "working".to_owned(),
        detail: "download queue fix".to_owned(),
        inbox: 0,
    }
    .for_pane("2-3", 7);

    let value = serde_json::to_value(params).unwrap();
    assert_eq!(value["source"], "herdr-harness-coordinator");
    assert_eq!(value["state_labels"]["state"], "working");
    assert_eq!(value["state_labels"]["detail"], "download queue fix");
    assert_eq!(value["state_labels"]["inbox"], "0");
    assert!(value.get("agent_status").is_none());
}

#[tokio::test]
async fn socket_client_sends_jsonl_requests_without_touching_the_live_session() {
    let (client, server) = duplex(4096);
    let server = tokio::spawn(async move {
        let (reader, mut writer) = split(server);
        let mut line = String::new();
        BufReader::new(reader).read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["method"], "session.snapshot");
        assert_eq!(request["params"], json!({}));
        let id = request["id"].as_str().unwrap();
        let response = json!({
            "id": id,
            "result": {
                "type": "session_snapshot",
                "snapshot": {
                    "version": HERDR_VERSION,
                    "protocol": HERDR_PROTOCOL,
                    "workspaces": [],
                    "tabs": [],
                    "panes": [],
                    "layouts": [],
                    "agents": []
                }
            }
        });
        writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
    });

    let snapshot = HerdrSocketClient::snapshot_over(client).await.unwrap();
    assert_eq!(snapshot.protocol, HERDR_PROTOCOL);
    server.await.unwrap();
}

#[test]
fn plugin_manifest_declares_the_resolved_mvp_entrypoints() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        std::fs::read_to_string(root.join("plugin/herdr-harness-coordinator/herdr-plugin.toml"))
            .unwrap();
    let manifest: toml::Value = toml::from_str(&manifest).unwrap();

    assert_eq!(manifest["min_herdr_version"].as_str(), Some(HERDR_VERSION));
    assert_eq!(manifest["id"].as_str(), Some("herdr-harness-coordinator"));
    let actions = manifest["actions"].as_array().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0]["id"].as_str(), Some("workspace"));
    assert_eq!(actions[0]["contexts"][0].as_str(), Some("workspace"));
    assert_eq!(
        actions[0]["command"][0].as_str(),
        Some("./scripts/workspace")
    );
    let panes = manifest["panes"].as_array().unwrap();
    assert_eq!(panes.len(), 3);
    assert_eq!(panes[0]["id"].as_str(), Some("supervisor"));
    assert_eq!(panes[0]["placement"].as_str(), Some("tab"));
    assert_eq!(panes[1]["id"].as_str(), Some("worker"));
    assert_eq!(panes[1]["placement"].as_str(), Some("tab"));
    assert_eq!(panes[2]["id"].as_str(), Some("harness-network"));
    assert_eq!(panes[2]["placement"].as_str(), Some("popup"));
}

#[test]
fn workspace_action_opens_the_setup_popup_in_the_invoking_workspace() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output =
        std::process::Command::new(root.join("plugin/herdr-harness-coordinator/scripts/workspace"))
            .env("HERDR_BIN", "/bin/echo")
            .env("HERDR_WORKSPACE_ID", "wF")
            .output()
            .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "plugin pane open --plugin herdr-harness-coordinator --entrypoint harness-network --workspace wF --focus\n"
    );
}

#[test]
fn worker_script_forwards_the_session_capability_and_inherited_herdr_environment() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output =
        std::process::Command::new(root.join("plugin/herdr-harness-coordinator/scripts/worker"))
            .env("HERDR_COORDINATOR_BIN", "/bin/echo")
            .env("HERDR_SOCKET_PATH", "/tmp/herdr.sock")
            .env("HERDR_PLUGIN_STATE_DIR", "/tmp/plugin-state")
            .env("HERDR_COORDINATOR_STATE_DIR", "/tmp/coordinator-state")
            .env("HERDR_HARNESS_SESSION_ID", "session-capability-1")
            .env("HERDR_HARNESS_CWD", &root)
            .output()
            .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "worker-host --session-id session-capability-1 --state-dir /tmp/coordinator-state\n"
    );
}

#[test]
fn supervisor_script_uses_the_explicit_workspace_state_directory() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output = std::process::Command::new(
        root.join("plugin/herdr-harness-coordinator/scripts/supervisor"),
    )
    .env("HERDR_COORDINATOR_BIN", "/bin/echo")
    .env("HERDR_SOCKET_PATH", "/tmp/herdr.sock")
    .env("HERDR_PLUGIN_STATE_DIR", "/tmp/plugin-state")
    .env("HERDR_COORDINATOR_STATE_DIR", "/tmp/coordinator-state")
    .env("HERDR_SUPERVISOR_CAPABILITY", "supervisor-capability-1")
    .output()
    .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "supervisor-host --state-dir /tmp/coordinator-state\n"
    );
}

#[test]
fn popup_script_never_receives_or_assumes_a_harness_identity() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output = std::process::Command::new(
        root.join("plugin/herdr-harness-coordinator/scripts/harness-network"),
    )
    .env("HERDR_COORDINATOR_BIN", "/bin/echo")
    .env("HERDR_SOCKET_PATH", "/tmp/herdr.sock")
    .env("HERDR_PLUGIN_STATE_DIR", "/tmp/coordinator-state")
    .env("HERDR_SUPERVISOR_CAPABILITY", "capability")
    .output()
    .unwrap();

    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "popup\n");
}

fn pane(pane_id: &str, terminal_id: &str, workspace_id: &str, tab_id: &str) -> PaneInfo {
    PaneInfo {
        pane_id: pane_id.to_owned(),
        terminal_id: terminal_id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        tab_id: tab_id.to_owned(),
        focused: false,
        revision: 1,
        agent_status: "working".to_owned(),
        state_labels: BTreeMap::new(),
        tokens: BTreeMap::new(),
    }
}
