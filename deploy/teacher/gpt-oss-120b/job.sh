#!/bin/sh
set -eu
umask 077

usage() {
  printf 'usage: %s --config ABS --teacher-run-id HEX64\n' "$0" >&2
  exit 64
}

fail() {
  printf 'teacher-job: %s\n' "$1" >&2
  exit "${2:-1}"
}

[ "$#" -eq 4 ] || usage
[ "$1" = --config ] && [ "$3" = --teacher-run-id ] || usage
config=$2
teacher_run_id=$4
case $config in /*) ;; *) usage ;; esac
[ -f "$config" ] || fail 'config is not a file'
case $teacher_run_id in *[!0-9a-f]*|'') usage ;; esac
[ "${#teacher_run_id}" -eq 64 ] || usage

gateway=/usr/local/bin/milk-carton
runtime=/opt/dragontales/runtime.py
[ -x "$gateway" ] || fail 'gateway executable is unavailable'
[ -r "$runtime" ] || fail 'teacher runtime is unavailable'
capture_store_access_key=${MILK_CAPTURE_STORE_ACCESS_KEY_ID:-}
capture_store_secret_key=${MILK_CAPTURE_STORE_SECRET_ACCESS_KEY:-}
capture_store_session_token=${MILK_CAPTURE_STORE_SESSION_TOKEN-}
capture_store_session_set=${MILK_CAPTURE_STORE_SESSION_TOKEN+set}
control_store_access_key=${MILK_CONTROL_STORE_ACCESS_KEY_ID:-}
control_store_secret_key=${MILK_CONTROL_STORE_SECRET_ACCESS_KEY:-}
control_store_session_token=${MILK_CONTROL_STORE_SESSION_TOKEN-}
control_store_session_set=${MILK_CONTROL_STORE_SESSION_TOKEN+set}
[ -n "$capture_store_access_key" ] || fail 'capture-store access key is unavailable'
[ -n "$capture_store_secret_key" ] || fail 'capture-store secret key is unavailable'
[ -n "$control_store_access_key" ] || fail 'control-store access key is unavailable'
[ -n "$control_store_secret_key" ] || fail 'control-store secret key is unavailable'
if [ "$capture_store_session_set" = set ] && [ -z "$capture_store_session_token" ]; then
  fail 'capture-store session token cannot be empty'
fi
if [ "$control_store_session_set" = set ] && [ -z "$control_store_session_token" ]; then
  fail 'control-store session token cannot be empty'
fi
unset MILK_CAPTURE_STORE_ACCESS_KEY_ID MILK_CAPTURE_STORE_SECRET_ACCESS_KEY
unset MILK_CAPTURE_STORE_SESSION_TOKEN
unset MILK_CONTROL_STORE_ACCESS_KEY_ID MILK_CONTROL_STORE_SECRET_ACCESS_KEY
unset MILK_CONTROL_STORE_SESSION_TOKEN
unset MILK_ROUTE_STORE_ACCESS_KEY_ID MILK_ROUTE_STORE_SECRET_ACCESS_KEY
unset MILK_ROUTE_STORE_SESSION_TOKEN

run_control_gateway() {
  if [ "$control_store_session_set" = set ]; then
    env -i \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      MILK_CONTROL_STORE_SESSION_TOKEN="$control_store_session_token" \
      "$gateway" --config "$config" "$@"
  else
    env -i \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      "$gateway" --config "$config" "$@"
  fi
}

run_execute_gateway() {
  if [ "$capture_store_session_set" = set ] && [ "$control_store_session_set" = set ]; then
    exec env -i \
      MILK_CAPTURE_STORE_ACCESS_KEY_ID="$capture_store_access_key" \
      MILK_CAPTURE_STORE_SECRET_ACCESS_KEY="$capture_store_secret_key" \
      MILK_CAPTURE_STORE_SESSION_TOKEN="$capture_store_session_token" \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      MILK_CONTROL_STORE_SESSION_TOKEN="$control_store_session_token" \
      MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
      "$gateway" --config "$config" execute-teacher-run \
      --teacher-run-id "$teacher_run_id"
  elif [ "$capture_store_session_set" = set ]; then
    exec env -i \
      MILK_CAPTURE_STORE_ACCESS_KEY_ID="$capture_store_access_key" \
      MILK_CAPTURE_STORE_SECRET_ACCESS_KEY="$capture_store_secret_key" \
      MILK_CAPTURE_STORE_SESSION_TOKEN="$capture_store_session_token" \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
      "$gateway" --config "$config" execute-teacher-run \
      --teacher-run-id "$teacher_run_id"
  elif [ "$control_store_session_set" = set ]; then
    exec env -i \
      MILK_CAPTURE_STORE_ACCESS_KEY_ID="$capture_store_access_key" \
      MILK_CAPTURE_STORE_SECRET_ACCESS_KEY="$capture_store_secret_key" \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      MILK_CONTROL_STORE_SESSION_TOKEN="$control_store_session_token" \
      MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
      "$gateway" --config "$config" execute-teacher-run \
      --teacher-run-id "$teacher_run_id"
  else
    exec env -i \
      MILK_CAPTURE_STORE_ACCESS_KEY_ID="$capture_store_access_key" \
      MILK_CAPTURE_STORE_SECRET_ACCESS_KEY="$capture_store_secret_key" \
      MILK_CONTROL_STORE_ACCESS_KEY_ID="$control_store_access_key" \
      MILK_CONTROL_STORE_SECRET_ACCESS_KEY="$control_store_secret_key" \
      MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
      "$gateway" --config "$config" execute-teacher-run \
      --teacher-run-id "$teacher_run_id"
  fi
}

work_dir=/tmp/dragontales/teacher
mkdir -m 0700 -- "$work_dir" || fail 'teacher work directory must be new'
begin_stdout=$work_dir/begin.stdout
terminal_stdout=$work_dir/terminal.stdout
runtime_output=$work_dir/runtime.log
runtime_pid=
readiness_pid=
execute_pid=
begun=0
terminal=0

cleanup() {
  status=$?
  trap - EXIT
  trap '' HUP INT TERM
  for child_pid in "$execute_pid" "$readiness_pid" "$runtime_pid"; do
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
      kill "$child_pid" 2>/dev/null || :
    fi
  done
  wait_seconds=0
  while [ "$wait_seconds" -lt 60 ]; do
    children_alive=0
    for child_pid in "$execute_pid" "$readiness_pid" "$runtime_pid"; do
      if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        children_alive=1
      fi
    done
    [ "$children_alive" -eq 1 ] || break
    sleep 1
    wait_seconds=$((wait_seconds + 1))
  done
  for child_pid in "$execute_pid" "$readiness_pid" "$runtime_pid"; do
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
      kill -KILL "$child_pid" 2>/dev/null || :
    fi
    if [ -n "$child_pid" ]; then
      wait "$child_pid" 2>/dev/null || :
    fi
  done
  if [ "$begun" -eq 1 ] && [ "$terminal" -eq 0 ]; then
    if run_control_gateway terminalize-teacher-run \
      --teacher-run-id "$teacher_run_id" >"$terminal_stdout"; then
      :
    else
      printf '%s\n' 'teacher-job: teacher run terminalization failed' >&2
    fi
  fi
  unset MILK_CARTON_TEACHER_API_KEY teacher_key
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

unset MILK_CARTON_TEACHER_API_KEY
if run_control_gateway begin-teacher-run \
  --teacher-run-id "$teacher_run_id" >"$begin_stdout"; then
  :
else
  status=$?
  fail 'teacher run start was not acquired' "$status"
fi
begun=1

teacher_key=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
[ "${#teacher_key}" -ge 32 ] || fail 'internal teacher credential generation failed'
export MILK_CARTON_TEACHER_API_KEY=$teacher_key

env -u MILK_CARTON_CONFIG_JSON \
  -u MILK_CAPTURE_STORE_ACCESS_KEY_ID \
  -u MILK_CAPTURE_STORE_SECRET_ACCESS_KEY \
  -u MILK_CAPTURE_STORE_SESSION_TOKEN \
  -u MILK_CONTROL_STORE_ACCESS_KEY_ID \
  -u MILK_CONTROL_STORE_SECRET_ACCESS_KEY \
  -u MILK_CONTROL_STORE_SESSION_TOKEN \
  -u MILK_ROUTE_STORE_ACCESS_KEY_ID \
  -u MILK_ROUTE_STORE_SECRET_ACCESS_KEY \
  -u MILK_ROUTE_STORE_SESSION_TOKEN \
  MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
  python3 "$runtime" serve >"$runtime_output" 2>&1 &
runtime_pid=$!

env -u MILK_CARTON_CONFIG_JSON \
  -u MILK_CAPTURE_STORE_ACCESS_KEY_ID \
  -u MILK_CAPTURE_STORE_SECRET_ACCESS_KEY \
  -u MILK_CAPTURE_STORE_SESSION_TOKEN \
  -u MILK_CONTROL_STORE_ACCESS_KEY_ID \
  -u MILK_CONTROL_STORE_SECRET_ACCESS_KEY \
  -u MILK_CONTROL_STORE_SESSION_TOKEN \
  -u MILK_ROUTE_STORE_ACCESS_KEY_ID \
  -u MILK_ROUTE_STORE_SECRET_ACCESS_KEY \
  -u MILK_ROUTE_STORE_SESSION_TOKEN \
  MILK_CARTON_TEACHER_API_KEY="$teacher_key" \
  python3 "$runtime" wait-ready &
readiness_pid=$!
if wait "$readiness_pid"; then
  readiness_pid=
  :
else
  status=$?
  readiness_pid=
  fail "teacher runtime did not become ready; work preserved at $work_dir" "$status"
fi

run_execute_gateway &
execute_pid=$!
if wait "$execute_pid"; then
  execute_pid=
  terminal=1
else
  status=$?
  execute_pid=
  fail 'teacher run execution failed' "$status"
fi
