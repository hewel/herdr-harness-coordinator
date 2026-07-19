use std::path::PathBuf;

use herdr_harness_coordinator::{
    core::SessionCapability,
    mcp::{MCP_PROTOCOL_VERSION, McpServer},
};
use serde_json::json;

#[tokio::test]
async fn mcp_initialization_and_tool_discovery_match_the_pinned_revision() {
    let server = McpServer::new(
        PathBuf::from("/tmp/not-connected.sock"),
        SessionCapability::from_bearer("0".repeat(64)).expect("valid bearer shape"),
    );
    let initialized = server
        .handle(json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{"protocolVersion":MCP_PROTOCOL_VERSION,"capabilities":{},"clientInfo":{"name":"test","version":"1"}}
        }))
        .await
        .expect("request response");
    assert_eq!(
        initialized["result"]["protocolVersion"],
        MCP_PROTOCOL_VERSION
    );
    assert_eq!(
        initialized["result"]["capabilities"]["tools"]["listChanged"],
        false
    );

    let listed = server
        .handle(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
        .await
        .expect("request response");
    let tools = listed["result"]["tools"].as_array().expect("tools");
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    assert!(names.contains(&"harness_complete"));
    assert!(names.contains(&"harness_attachment_create"));
    assert!(names.contains(&"harness_task_approve"));
    assert!(names.contains(&"harness_task_graph"));
    assert!(names.contains(&"harness_hold_clear"));
    assert!(names.contains(&"harness_supervisor_events"));
    assert!(names.contains(&"harness_supervisor_event_ack"));
    assert!(names.contains(&"harness_supervisor_event_reconcile"));
    assert!(names.contains(&"harness_task_graph_watch"));
    let task_create = tools
        .iter()
        .find(|tool| tool["name"] == "harness_task_create")
        .expect("Task creation tool");
    let required = task_create["inputSchema"]["required"]
        .as_array()
        .expect("typed Task schema");
    assert!(required.iter().any(|field| field == "attachments"));
    assert!(required.iter().any(|field| field == "repository"));
    let complete = tools
        .iter()
        .find(|tool| tool["name"] == "harness_complete")
        .expect("completion tool");
    assert_eq!(complete["inputSchema"]["required"][0], "manifest");
}

#[tokio::test]
async fn mcp_notifications_do_not_emit_json_rpc_responses() {
    let server = McpServer::new(
        PathBuf::from("/tmp/not-connected.sock"),
        SessionCapability::from_bearer("0".repeat(64)).expect("valid bearer shape"),
    );
    assert!(
        server
            .handle(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn completion_without_provider_turn_id_reaches_the_broker_boundary() {
    let server = McpServer::new(
        PathBuf::from("/tmp/not-connected.sock"),
        SessionCapability::from_bearer("0".repeat(64)).expect("valid bearer shape"),
    );
    let response = server
        .handle(json!({
            "jsonrpc":"2.0",
            "id":3,
            "method":"tools/call",
            "params":{
                "name":"harness_complete",
                "arguments":{
                    "manifest":{
                        "schema_version":1,
                        "task_id":"019f7606-a26b-7a41-87dd-95f3a072a226",
                        "summary":"candidate Result",
                        "changed_files":[],
                        "verification":[{
                            "command":"true",
                            "exit_code":0,
                            "passed":true,
                            "evidence":"019f7606-a26b-7a41-87dd-95f3a072a227"
                        }],
                        "deviations":[],
                        "risks":[],
                        "attachments":[]
                    }
                }
            }
        }))
        .await
        .expect("tool call response");
    let diagnostic = response["result"]["content"][0]["text"]
        .as_str()
        .expect("tool error diagnostic");
    assert!(response["result"]["isError"].as_bool().unwrap_or(false));
    assert!(!diagnostic.contains("native turn ID is required"));
}
