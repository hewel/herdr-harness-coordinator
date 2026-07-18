use std::path::PathBuf;

use herdr_harness_coordinator::{
    activation::{
        ActivationRegistry, DesiredActivation, SetActivationRequest, SupervisorSelection,
        WorkerSelection, WorkspaceIdentity, WorkspaceSelection,
    },
    contract::{CodexApprovalPolicy, CodexSandboxMode, HarnessId, HarnessKind},
};

fn identity(root: &std::path::Path, workspace_id: &str) -> WorkspaceIdentity {
    WorkspaceIdentity::new(
        PathBuf::from("/run/user/1000/herdr.sock"),
        workspace_id.to_owned(),
        root.to_path_buf(),
    )
}

fn selection() -> WorkspaceSelection {
    WorkspaceSelection {
        schema_version: 1,
        supervisor: SupervisorSelection {
            kind: HarnessKind::Codex,
            model: "gpt-5.6-sol".to_owned(),
            reasoning_effort: Some("high".to_owned()),
            codex_approval_policy: Some(CodexApprovalPolicy::Never),
            codex_sandbox_mode: Some(CodexSandboxMode::DangerFullAccess),
        },
        workers: vec![WorkerSelection {
            worker_id: "implementer".parse::<HarnessId>().unwrap(),
            profile_id: "omp-kimi".parse::<HarnessId>().unwrap(),
        }],
    }
}

#[tokio::test]
async fn unknown_workspace_is_off_by_default() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();

    let view = registry.get(&identity(root.path(), "wA")).await.unwrap();

    assert_eq!(view.desired, DesiredActivation::Off);
}

#[tokio::test]
async fn set_on_requires_explicit_selection_only_the_first_time() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();
    let target = identity(root.path(), "wA");

    let error = registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: None,
            },
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("selection is required"));
}

#[tokio::test]
async fn workspaces_toggle_independently_and_reuse_saved_selection() {
    let state = tempfile::tempdir().unwrap();
    let root_a = tempfile::tempdir().unwrap();
    let root_b = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();
    let a = identity(root_a.path(), "wA");
    let b = identity(root_b.path(), "wB");

    registry
        .set(
            &a,
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap();
    let b_view = registry.get(&b).await.unwrap();
    let off = registry
        .set(
            &a,
            SetActivationRequest {
                desired: DesiredActivation::Off,
                expected_revision: Some(1),
                selection: None,
            },
        )
        .await
        .unwrap();
    let on_again = registry
        .set(
            &a,
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(2),
                selection: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(b_view.desired, DesiredActivation::Off);
    assert_eq!(off.desired, DesiredActivation::Off);
    assert_eq!(on_again.selection, Some(selection()));
}

#[tokio::test]
async fn stale_revision_is_rejected_but_repeated_desired_state_is_idempotent() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();
    let target = identity(root.path(), "wA");
    let request = SetActivationRequest {
        desired: DesiredActivation::On,
        expected_revision: Some(0),
        selection: Some(selection()),
    };

    let first = registry.set(&target, request.clone()).await.unwrap();
    let repeated = registry
        .set(
            &target,
            SetActivationRequest {
                expected_revision: None,
                ..request
            },
        )
        .await
        .unwrap();
    let error = registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::Off,
                expected_revision: Some(0),
                selection: None,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(repeated.revision, first.revision);
    assert!(error.to_string().contains("revision conflict"));
}

#[tokio::test]
async fn durable_disable_does_not_remove_the_live_supervisor_capability() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();
    let target = identity(root.path(), "wA");
    let on = registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap();
    let capability = on.state_dir.join("supervisor.capability");
    tokio::fs::write(&capability, "live-capability")
        .await
        .unwrap();

    registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::Off,
                expected_revision: Some(on.revision),
                selection: None,
            },
        )
        .await
        .unwrap();

    assert!(capability.exists());
}

#[tokio::test]
async fn second_workspace_cannot_enable_the_same_canonical_root() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();

    registry
        .set(
            &identity(root.path(), "wA"),
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap();
    let error = registry
        .set(
            &identity(root.path(), "wB"),
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("already enabled"));
}

#[tokio::test]
async fn recycled_workspace_id_cannot_inherit_another_root() {
    let state = tempfile::tempdir().unwrap();
    let root_a = tempfile::tempdir().unwrap();
    let root_b = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();

    registry
        .set(
            &identity(root_a.path(), "wA"),
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap();
    let error = registry
        .get(&identity(root_b.path(), "wA"))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("different repository root"));
}

#[tokio::test]
async fn disable_refuses_a_task_awaiting_supervisor_review() {
    let state = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let registry = ActivationRegistry::open(state.path()).await.unwrap();
    let target = identity(root.path(), "wA");
    let on = registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::On,
                expected_revision: Some(0),
                selection: Some(selection()),
            },
        )
        .await
        .unwrap();
    herdr_harness_coordinator::core::Coordinator::open(&on.state_dir)
        .await
        .unwrap();
    let database = format!(
        "sqlite://{}",
        on.state_dir.join("coordinator.sqlite3").display()
    );
    let pool = sqlx::SqlitePool::connect(&database).await.unwrap();
    sqlx::query("INSERT INTO harnesses (id, definition_json, kind, tier, cwd, created_at) VALUES ('implementer', '{}', 'omp', 'worker', ?, '2026-01-01T00:00:00Z')")
        .bind(root.path().to_string_lossy().as_ref())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks (id, worker_id, submission_json, state, created_sequence, created_at, updated_at) VALUES ('019c0000-0000-7000-8000-000000000001', 'implementer', '{}', 'reviewing', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    let error = registry
        .set(
            &target,
            SetActivationRequest {
                desired: DesiredActivation::Off,
                expected_revision: Some(1),
                selection: None,
            },
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("active or awaiting review"));
}
