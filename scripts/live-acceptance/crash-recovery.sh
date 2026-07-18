#!/bin/sh
set -eu

usage() {
  cat >&2 <<'EOF'
usage: crash-recovery.sh <workspace-state-dir> <phase>

phases:
  controlled-restart  restart only the Coordinator daemon on LIVE_CANDIDATE_BIN
  supervisor-sigkill  SIGKILL the validated managed Supervisor Host, then reactivate
  worker-sigkill      SIGKILL the validated Worker Host and verify safe recovery

Required for every phase:
  LIVE_CRASH_ACCEPT=I_UNDERSTAND_THIS_SENDS_SIGNALS
  LIVE_CANDIDATE_BIN=/absolute/path/to/tested/herdr-harness-coordinator
  HERDR_WORKSPACE_ID=<live Herdr workspace>
  HERDR_SOCKET_PATH=<live Herdr session socket>

Additional phase prerequisites:
  supervisor-sigkill: LIVE_SUPERVISOR_EVENT_ID=<dispatching-or-accepted event UUID>
  worker-sigkill:     LIVE_WORKER_ID=<Harness ID>
                      LIVE_WORKER_SESSION_ID=<Coordinator Session UUID>
                      LIVE_MUTATING_TASK_ID=<active mutating Task UUID>
  worker-sigkill may also set LIVE_DOWNSTREAM_TASK_ID=<expected-blocked Task UUID>.

Use a disposable live-acceptance state root. The script never reconciles Unknown
events, clears Holds, approves Tasks, or replays ambiguous native work.
EOF
  exit 2
}

[ "$#" -eq 2 ] || usage
state_dir=${1%/}
phase=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
control=$script_dir/control.sh

[ "${LIVE_CRASH_ACCEPT:-}" = "I_UNDERSTAND_THIS_SENDS_SIGNALS" ] || {
  echo "refusing crash test without LIVE_CRASH_ACCEPT=I_UNDERSTAND_THIS_SENDS_SIGNALS" >&2
  exit 2
}
[ -d "$state_dir" ] || { echo "workspace state directory does not exist: $state_dir" >&2; exit 1; }
[ -f "$state_dir/coordinator.sqlite3" ] || { echo "Coordinator database is missing" >&2; exit 1; }
[ -x "$control" ] || { echo "live control helper is not executable: $control" >&2; exit 1; }
command -v jq >/dev/null
command -v sqlite3 >/dev/null
command -v rg >/dev/null
command -v sha256sum >/dev/null
command -v herdr >/dev/null

candidate=$(readlink -f "${LIVE_CANDIDATE_BIN:?set LIVE_CANDIDATE_BIN to a tested candidate}")
[ -x "$candidate" ] || { echo "candidate is not executable: $candidate" >&2; exit 1; }
workspace_id=${HERDR_WORKSPACE_ID:?set HERDR_WORKSPACE_ID to the live workspace}
session_socket=${HERDR_SOCKET_PATH:?set HERDR_SOCKET_PATH to the live Herdr socket}
db=$state_dir/coordinator.sqlite3
run_id=$(date -u '+%Y%m%dT%H%M%SZ')
evidence_dir=$state_dir/live-acceptance/crash-recovery-$phase-$run_id
mkdir -p "$evidence_dir"

sql_value() {
  sqlite3 -noheader "$db" "$1"
}

require_uuid() {
  label=$1
  value=$2
  printf '%s\n' "$value" | rg -x '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' >/dev/null || {
    echo "$label must be a lowercase UUID" >&2
    exit 2
  }
}

pid_start_time() {
  awk '{print $22}' "/proc/$1/stat"
}

argv_has() {
  pid=$1
  expected=$2
  tr '\0' '\n' <"/proc/$pid/cmdline" | rg -F -x "$expected" >/dev/null
}

validate_host_pid() {
  pid=$1
  subcommand=$2
  session_id=${3:-}
  kill -0 "$pid" 2>/dev/null || return 1
  executable=$(readlink -f "/proc/$pid/exe") || return 1
  [ "$(basename "$executable")" = "herdr-harness-coordinator" ] || return 1
  argv_has "$pid" "$subcommand" || return 1
  argv_has "$pid" "$state_dir" || return 1
  if [ -n "$session_id" ]; then
    argv_has "$pid" "$session_id" || return 1
  fi
}

require_candidate_daemon() {
  daemon_pid_file=$state_dir/coordinator.pid
  [ -r "$daemon_pid_file" ] || { echo "Coordinator daemon PID file is missing" >&2; exit 1; }
  daemon_pid=$(sed -n '1p' "$daemon_pid_file")
  validate_host_pid "$daemon_pid" daemon || {
    echo "PID file does not identify this workspace's Coordinator daemon" >&2
    exit 1
  }
  daemon_executable=$(readlink -f "/proc/$daemon_pid/exe")
  [ "$daemon_executable" = "$candidate" ] || {
    echo "crash phases require the tested candidate daemon; run controlled-restart first" >&2
    exit 1
  }
}

wait_for_original_exit() {
  pid=$1
  start_time=$2
  attempts=0
  while [ -r "/proc/$pid/stat" ] && [ "$(pid_start_time "$pid" 2>/dev/null || true)" = "$start_time" ]; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 100 ] || {
      echo "validated process did not exit after SIGKILL: $pid" >&2
      exit 1
    }
    sleep 0.1
  done
}

capture() {
  label=$1
  HERDR_COORDINATOR_BIN=$candidate "$control" "$state_dir" evidence \
    "$evidence_dir/$label-durable.jsonl"
  herdr pane list --workspace "$workspace_id" >"$evidence_dir/$label-panes.txt"
  {
    printf 'captured_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'candidate=%s\n' "$candidate"
    printf 'candidate_sha256=%s\n' "$(sha256sum "$candidate" | awk '{print $1}')"
    printf 'workspace=%s\n' "$workspace_id"
    printf 'state_dir=%s\n' "$state_dir"
  } >"$evidence_dir/$label-environment.txt"
  sqlite3 -tabs "$db" "
SELECT 'task-transition', task_id, from_state, to_state, created_at FROM task_transitions ORDER BY sequence;
SELECT 'scheduling-transition', task_id, from_state, to_state, created_at FROM task_scheduling_transitions ORDER BY sequence;
SELECT 'hold', id, task_id, reason, created_at, cleared_at FROM worktree_holds ORDER BY created_at;
" >"$evidence_dir/$label-recovery.tsv"
  if [ "$(sql_value "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'host_connections'")" -eq 1 ]; then
    sqlite3 -tabs "$db" "SELECT 'host-connection', id, session_id, generation, instance_id, status, bound_at, last_heartbeat_at, expires_at, disconnected_at, disconnect_reason FROM host_connections ORDER BY session_id, generation" \
      >>"$evidence_dir/$label-recovery.tsv"
  fi
  if [ "$(sql_value "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'supervisor_event_observations'")" -eq 1 ]; then
    sqlite3 -tabs "$db" "SELECT 'event-observation', id, event_id, attempt_id, observation_kind, native_session_id, native_thread_id, native_turn_id, evidence_json, observed_at FROM supervisor_event_observations ORDER BY observed_at" \
      >>"$evidence_dir/$label-recovery.tsv"
    sqlite3 -tabs "$db" "SELECT 'event-reconciliation', id, event_id, attempt_id, resolution, audit_note, created_at FROM supervisor_event_reconciliations ORDER BY created_at" \
      >>"$evidence_dir/$label-recovery.tsv"
  fi
}

immutable_projection() {
  sqlite3 -tabs "$db" "
SELECT 'task', id, created_sequence, json_extract(submission_json, '\$.request_key') FROM tasks ORDER BY id;
SELECT 'dependency', task_id, dependency_task_id, condition, failure_policy, satisfied_by_result_revision, result_snapshot_attachment_id, bound_at FROM task_dependencies ORDER BY task_id, dependency_task_id;
SELECT 'result', task_id, revision, native_turn_id, accepted_at, terminal_at FROM results ORDER BY task_id, revision;
"
}

binding_projection() {
  sqlite3 -tabs "$db" "SELECT task_id, harness_session_id, reused, reason_code, bound_at, superseded_at FROM task_session_bindings ORDER BY task_id, bound_at"
}

unsettled_attempt_projection() {
  sqlite3 -tabs "$db" "SELECT e.id, COUNT(a.id) FROM supervisor_events e LEFT JOIN supervisor_event_attempts a ON a.event_id = e.id WHERE e.state IN ('dispatching','accepted','unknown') GROUP BY e.id ORDER BY e.id"
}

activation_values() {
  plugin_state=$(dirname "$(dirname "$state_dir")")
  activation=$($candidate workspace --state-dir "$plugin_state" list --json)
  repository_root=$(printf '%s' "$activation" | jq -er --arg state "$state_dir" \
    '.[] | select(.state_dir == $state) | .repository_root')
}

reactivate_workspace() {
  activation_values
  $candidate workspace --state-dir "$plugin_state" set on \
    --workspace "$workspace_id" --root "$repository_root" \
    --session-socket "$session_socket" --json
}

case "$phase" in
  controlled-restart)
    capture before
    immutable_projection >"$evidence_dir/before-immutable.tsv"
    binding_projection >"$evidence_dir/before-bindings.tsv"
    unsettled_attempt_projection >"$evidence_dir/before-unsettled-attempts.tsv"
    HERDR_COORDINATOR_BIN=$candidate HERDR_SOCKET_PATH=$session_socket \
      "$control" "$state_dir" handoff "$candidate" | tee "$evidence_dir/handoff.txt"
    capture after
    immutable_projection >"$evidence_dir/after-immutable.tsv"
    binding_projection >"$evidence_dir/after-bindings.tsv"
    diff -u "$evidence_dir/before-immutable.tsv" "$evidence_dir/after-immutable.tsv" \
      >"$evidence_dir/immutable.diff" || {
        echo "controlled restart changed immutable Task, dependency, or Result evidence" >&2
        exit 1
      }
    sort "$evidence_dir/before-bindings.tsv" >"$evidence_dir/before-bindings.sorted.tsv"
    sort "$evidence_dir/after-bindings.tsv" >"$evidence_dir/after-bindings.sorted.tsv"
    comm -23 "$evidence_dir/before-bindings.sorted.tsv" "$evidence_dir/after-bindings.sorted.tsv" \
      >"$evidence_dir/missing-bindings.tsv"
    [ ! -s "$evidence_dir/missing-bindings.tsv" ] || {
      echo "controlled restart lost a durable Task-to-Session binding" >&2
      exit 1
    }
    while IFS="$(printf '\t')" read -r unsettled_event before_attempts; do
      [ -n "$unsettled_event" ] || continue
      after_attempts=$(sql_value "SELECT COUNT(*) FROM supervisor_event_attempts WHERE event_id = '$unsettled_event'")
      [ "$after_attempts" = "$before_attempts" ] || {
        echo "controlled restart blindly retried unsettled Supervisor event $unsettled_event" >&2
        exit 1
      }
    done <"$evidence_dir/before-unsettled-attempts.tsv"
    ;;

  supervisor-sigkill)
    require_candidate_daemon
    event_id=${LIVE_SUPERVISOR_EVENT_ID:?set LIVE_SUPERVISOR_EVENT_ID}
    require_uuid LIVE_SUPERVISOR_EVENT_ID "$event_id"
    event_state=$(sql_value "SELECT state FROM supervisor_events WHERE id = '$event_id'")
    case "$event_state" in dispatching|accepted) ;; *)
      echo "Supervisor event must be dispatching or accepted before SIGKILL, got: ${event_state:-missing}" >&2
      exit 1
    esac
    pid_file=$state_dir/supervisor-host.pid
    [ -r "$pid_file" ] || { echo "Supervisor Host PID file is missing" >&2; exit 1; }
    supervisor_pid=$(sed -n '1p' "$pid_file")
    validate_host_pid "$supervisor_pid" supervisor-host || {
      echo "PID file does not identify this workspace's managed Supervisor Host" >&2
      exit 1
    }
    original_start=$(pid_start_time "$supervisor_pid")
    attempt_count=$(sql_value "SELECT COUNT(*) FROM supervisor_event_attempts WHERE event_id = '$event_id'")
    capture before
    event_state=$(sql_value "SELECT state FROM supervisor_events WHERE id = '$event_id'")
    case "$event_state" in dispatching|accepted) ;; *)
      echo "Supervisor event settled while evidence was captured; prepare a new event" >&2
      exit 1
    esac
    validate_host_pid "$supervisor_pid" supervisor-host || {
      echo "Supervisor Host identity changed before SIGKILL" >&2
      exit 1
    }
    [ "$(pid_start_time "$supervisor_pid")" = "$original_start" ] || {
      echo "Supervisor PID was reused before SIGKILL; refusing to signal" >&2
      exit 1
    }
    kill -KILL "$supervisor_pid"
    wait_for_original_exit "$supervisor_pid" "$original_start"
    capture after-kill
    after_kill_state=$(sql_value "SELECT state FROM supervisor_events WHERE id = '$event_id'")
    case "$after_kill_state" in dispatching|accepted|unknown) ;; *)
      echo "event was unsafely settled or replayed after Supervisor SIGKILL: $after_kill_state" >&2
      exit 1
    esac
    if [ -r "$pid_file" ] && [ "$(sed -n '1p' "$pid_file")" = "$supervisor_pid" ]; then
      if [ -r "/proc/$supervisor_pid/stat" ] && \
         [ "$(pid_start_time "$supervisor_pid" 2>/dev/null || true)" = "$original_start" ]; then
        echo "original Supervisor process still exists; refusing stale-PID recovery" >&2
        exit 1
      fi
      mv "$pid_file" "$evidence_dir/supervisor-host.pid.stale"
    fi
    attempts=0
    while [ ! -r "$pid_file" ]; do
      attempts=$((attempts + 1)); [ "$attempts" -lt 300 ] || { echo "daemon did not automatically reopen the Supervisor Host" >&2; exit 1; }
      sleep 0.1
    done
    replacement_pid=$(sed -n '1p' "$pid_file")
    validate_host_pid "$replacement_pid" supervisor-host || {
      echo "replacement Supervisor Host identity could not be proved" >&2
      exit 1
    }
    capture after-rebind
    rebound_state=$(sql_value "SELECT state FROM supervisor_events WHERE id = '$event_id'")
    [ "$rebound_state" = unknown ] || {
      echo "unsettled Supervisor event must be Unknown after rebind, got: $rebound_state" >&2
      exit 1
    }
    rebound_attempts=$(sql_value "SELECT COUNT(*) FROM supervisor_event_attempts WHERE event_id = '$event_id'")
    [ "$rebound_attempts" = "$attempt_count" ] || {
      echo "Unknown Supervisor event was blindly replayed" >&2
      exit 1
    }
    ;;

  worker-sigkill)
    require_candidate_daemon
    worker_id=${LIVE_WORKER_ID:?set LIVE_WORKER_ID}
    worker_session=${LIVE_WORKER_SESSION_ID:?set LIVE_WORKER_SESSION_ID}
    task_id=${LIVE_MUTATING_TASK_ID:?set LIVE_MUTATING_TASK_ID}
    require_uuid LIVE_WORKER_SESSION_ID "$worker_session"
    require_uuid LIVE_MUTATING_TASK_ID "$task_id"
    if [ -n "${LIVE_DOWNSTREAM_TASK_ID:-}" ]; then
      require_uuid LIVE_DOWNSTREAM_TASK_ID "$LIVE_DOWNSTREAM_TASK_ID"
    fi
    printf '%s\n' "$worker_id" | rg -x '[a-z0-9][a-z0-9._-]{0,127}' >/dev/null || {
      echo "LIVE_WORKER_ID is invalid" >&2
      exit 2
    }
    task_row=$(sql_value "SELECT state || '|' || active_session_id || '|' || json_extract(submission_json, '$.repository.access') FROM tasks WHERE id = '$task_id'")
    task_state=${task_row%%|*}
    task_rest=${task_row#*|}
    task_session=${task_rest%%|*}
    repository_access=${task_rest##*|}
    case "$task_state" in dispatching|working|waiting|reviewing|cancelling|delivery_unknown) ;; *)
      echo "Worker crash Task must have possibly active native work, got: ${task_state:-missing}" >&2
      exit 1
    esac
    [ "$task_session" = "$worker_session" ] || { echo "Task is not bound to LIVE_WORKER_SESSION_ID" >&2; exit 1; }
    [ "$repository_access" = mutating ] || { echo "LIVE_MUTATING_TASK_ID is not mutating" >&2; exit 1; }
    worker_pid=${LIVE_WORKER_PID:-}
    if [ -n "$worker_pid" ]; then
      validate_host_pid "$worker_pid" worker-host "$worker_session" || {
        echo "LIVE_WORKER_PID does not identify the selected Worker Host" >&2
        exit 1
      }
    else
      matches=
      for cmdline in /proc/[0-9]*/cmdline; do
        pid=${cmdline#/proc/}; pid=${pid%/cmdline}
        if validate_host_pid "$pid" worker-host "$worker_session" 2>/dev/null; then
          matches="$matches $pid"
        fi
      done
      set -- $matches
      [ "$#" -eq 1 ] || { echo "expected exactly one validated Worker Host, found $#" >&2; exit 1; }
      worker_pid=$1
    fi
    original_start=$(pid_start_time "$worker_pid")
    dispatch_count=$(sql_value "SELECT COUNT(*) FROM task_transitions WHERE task_id = '$task_id' AND to_state = 'dispatching'")
    capture before
    current_task_state=$(sql_value "SELECT state FROM tasks WHERE id = '$task_id'")
    case "$current_task_state" in dispatching|working|waiting|reviewing|cancelling|delivery_unknown) ;; *)
      echo "Worker Task settled while evidence was captured; prepare another active Task" >&2
      exit 1
    esac
    validate_host_pid "$worker_pid" worker-host "$worker_session" || {
      echo "Worker Host identity changed before SIGKILL" >&2
      exit 1
    }
    [ "$(pid_start_time "$worker_pid")" = "$original_start" ] || {
      echo "Worker PID was reused before SIGKILL; refusing to signal" >&2
      exit 1
    }
    kill -KILL "$worker_pid"
    wait_for_original_exit "$worker_pid" "$original_start"
    sleep "${LIVE_RECOVERY_WAIT_SECONDS:-35}"
    capture after-kill
    safe_state=$(sql_value "SELECT state FROM tasks WHERE id = '$task_id'")
    case "$safe_state" in failed|cancelled|delivery_unknown|reviewing) ;; *)
      echo "Worker loss is not represented by an explicit safe Task state: $safe_state" >&2
      exit 1
    esac
    hold_count=$(sql_value "SELECT COUNT(*) FROM worktree_holds WHERE task_id = '$task_id' AND cleared_at IS NULL")
    [ "$hold_count" -gt 0 ] || { echo "mutating Worker loss did not create a Worktree Hold" >&2; exit 1; }
    attention_count=$(sql_value "SELECT COUNT(*) FROM supervisor_events WHERE task_id = '$task_id' AND kind IN ('task_failed','delivery_unknown','worktree_hold_created')")
    [ "$attention_count" -gt 0 ] || { echo "Worker loss did not create Supervisor attention" >&2; exit 1; }
    after_dispatch_count=$(sql_value "SELECT COUNT(*) FROM task_transitions WHERE task_id = '$task_id' AND to_state = 'dispatching'")
    [ "$after_dispatch_count" = "$dispatch_count" ] || { echo "ambiguous native Task was replayed" >&2; exit 1; }
    if [ -n "${LIVE_DOWNSTREAM_TASK_ID:-}" ]; then
      downstream=$(sql_value "SELECT state || '|' || scheduling_state FROM tasks WHERE id = '$LIVE_DOWNSTREAM_TASK_ID'")
      case "$downstream" in queued\|blocked|cancelled\|blocked) ;; *)
        echo "downstream Task is not safely blocked or dependency-cancelled: $downstream" >&2
        exit 1
      esac
    fi
    HERDR_COORDINATOR_BIN=$candidate "$control" "$state_dir" start "$worker_id" \
      >"$evidence_dir/replacement-worker.json"
    capture after-replacement
    final_dispatch_count=$(sql_value "SELECT COUNT(*) FROM task_transitions WHERE task_id = '$task_id' AND to_state = 'dispatching'")
    [ "$final_dispatch_count" = "$dispatch_count" ] || { echo "replacement Worker replayed ambiguous work" >&2; exit 1; }
    ;;

  *) usage ;;
esac

printf 'crash/recovery phase passed: %s\nevidence: %s\n' "$phase" "$evidence_dir"
