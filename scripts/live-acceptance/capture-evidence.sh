#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <workspace-state-dir>" >&2
  exit 2
fi

state_dir=$1
db=$state_dir/coordinator.sqlite3
test -f "$db"

sqlite3 -json "$db" '
select id,state,result_revision,approved_result_revision,active_session_id,created_at,updated_at from tasks;
select id,harness_id,harness_tier,native_session_id,native_thread_id,terminal_id,pane_id,presence,activity,last_seen_at,ended_at,observed_version,effective_model from harness_sessions;
select task_id,revision,native_turn_id,accepted_at,terminal_at,manifest_json from results;
select id,kind,task_id,result_revision,delivery_intent,state,created_at,updated_at,processed_at from supervisor_events;
select event_id,attempt_number,target_session_id,target_host_connection_id,state,provider_bytes_may_have_been_written,native_correlation,acceptance_evidence_json,created_at,updated_at from supervisor_event_attempts;
select id,session_id,generation,instance_id,status,bound_at,last_heartbeat_at,expires_at,disconnected_at,disconnect_reason from host_connections;
select observation_key,event_id,attempt_id,observation_kind,native_session_id,native_thread_id,native_turn_id,evidence_json,observed_at from supervisor_event_observations;
select id,event_id,attempt_id,resolution,audit_note,created_at from supervisor_event_reconciliations;
select task_id,harness_session_id,reused,reason_code,decision_reason,bound_at,superseded_at from task_session_bindings;
select task_id,dependency_task_id,condition,failure_policy,satisfied_by_result_revision,result_snapshot_attachment_id,bound_at from task_dependencies;
select repository_key,task_id,acquired_at,released_at from worktree_leases;
select id,repository_key,task_id,reason,observation_id,created_at,cleared_at,audit_note from worktree_holds;
select id,task_id,checkpoint,digest,created_at from repository_observations;
'
