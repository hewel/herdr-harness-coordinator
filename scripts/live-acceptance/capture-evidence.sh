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
select id,harness_id,harness_tier,native_session_id,native_thread_id,presence,activity,observed_version,effective_model from harness_sessions;
select task_id,revision,native_turn_id,accepted_at,terminal_at,manifest_json from results;
select id,kind,task_id,result_revision,delivery_intent,state,created_at,updated_at,processed_at from supervisor_events;
select event_id,attempt_number,target_session_id,state,provider_bytes_may_have_been_written,native_correlation,acceptance_evidence_json,created_at,updated_at from supervisor_event_attempts;
select task_id,harness_session_id,reused,reason_code,decision_reason,bound_at,superseded_at from task_session_bindings;
select task_id,depends_on_task_id,condition,failure_policy,bound_result_revision from task_dependencies;
'
