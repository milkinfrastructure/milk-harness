#!/bin/sh
set -eu
umask 077

usage() {
  printf 'usage: %s train --config ABS --student-job-id HEX64 --work-dir NEW_ABS\n' "$0" >&2
  printf '       %s branch --config ABS --student-job-id HEX64 --variant VARIANT --work-dir NEW_ABS\n' "$0" >&2
  printf '       %s serve --model ABS --model-manifest ABS --model-alias ALIAS {--api-key-file ABS|--trusted-ingress-auth}\n' "$0" >&2
  exit 64
}

fail() {
  printf 'student-job: %s\n' "$1" >&2
  exit "${2:-1}"
}

resolve_executable() {
  case $1 in
    -*|'') return 1 ;;
    */*) resolved=$1 ;;
    *) resolved=$(command -v "$1") || return 1 ;;
  esac
  [ -f "$resolved" ] && [ -x "$resolved" ] || return 1
  printf '%s\n' "$resolved"
}

read_one_line() {
  line=
  extra=
  exec 3< "$1" || return 1
  if ! IFS= read -r line <&3; then
    exec 3<&-
    return 1
  fi
  if IFS= read -r extra <&3 || [ -n "$extra" ]; then
    exec 3<&-
    return 1
  fi
  exec 3<&-
}

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
role_path=/usr/share/milk/student-artifact-role
if [ ! -f "$role_path" ] || [ -L "$role_path" ]; then
  fail 'student artifact role authority is unavailable'
fi
read_one_line "$role_path" || fail 'student artifact role authority is invalid'
artifact_role=$line
case "$artifact_role:${1:-}" in
  student-train:train) ;;
  student-branch:branch|student-branch:serve) ;;
  student-train:*|student-branch:*) fail 'operation is forbidden in this student artifact' 64 ;;
  *) fail 'student artifact role authority is invalid' ;;
esac
worker_uid=65532
worker_gid=65532
privilege=$(resolve_executable /usr/bin/setpriv) ||
  fail 'setpriv is unavailable'
control_store_access_key=${MILK_CONTROL_STORE_ACCESS_KEY_ID:-}
control_store_secret_key=${MILK_CONTROL_STORE_SECRET_ACCESS_KEY:-}
control_store_session_token=${MILK_CONTROL_STORE_SESSION_TOKEN-}
control_store_session_set=${MILK_CONTROL_STORE_SESSION_TOKEN+set}
unset DRAGONTALES_CONFIG_JSON
unset MILK_CAPTURE_STORE_ACCESS_KEY_ID MILK_CAPTURE_STORE_SECRET_ACCESS_KEY
unset MILK_CAPTURE_STORE_SESSION_TOKEN
unset MILK_CONTROL_STORE_ACCESS_KEY_ID MILK_CONTROL_STORE_SECRET_ACCESS_KEY
unset MILK_CONTROL_STORE_SESSION_TOKEN
unset MILK_ROUTE_STORE_ACCESS_KEY_ID MILK_ROUTE_STORE_SECRET_ACCESS_KEY
unset MILK_ROUTE_STORE_SESSION_TOKEN
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY

run_as_worker() {
  "$privilege" --reuid="$worker_uid" --regid="$worker_gid" \
    --clear-groups --no-new-privs "$@"
}

if [ "${1:-}" = serve ]; then
  [ "$#" -ge 8 ] || usage
  if [ "$2" != --model ] || [ "$4" != --model-manifest ] ||
    [ "$6" != --model-alias ]; then
    usage
  fi
  case $#:$8 in
    9:--api-key-file) case $3:$5:$9 in /*:/*:/*) ;; *) usage ;; esac ;;
    8:--trusted-ingress-auth) case $3:$5 in /*:/*) ;; *) usage ;; esac ;;
    *) usage ;;
  esac
  runner=$(resolve_executable "$script_dir/student-run.sh") ||
    fail 'student runner is unavailable'
  serve_dir=${3%/*}
  if [ -z "$serve_dir" ] || [ "$3" != "$serve_dir/model" ] ||
    [ "$5" != "$serve_dir/model-manifest.json" ]; then
    fail 'model and manifest are not one fixed materialized tree'
  fi
  if [ ! -d "$serve_dir" ] || [ -L "$serve_dir" ]; then
    fail 'materialized tree root is not a directory'
  fi
  if [ ! -d "$3" ] || [ -L "$3" ]; then
    fail 'model is not a directory'
  fi
  if [ ! -f "$5" ] || [ -L "$5" ]; then
    fail 'model manifest is not a file'
  fi
  chown -hR "$worker_uid:$worker_gid" "$serve_dir" ||
    fail 'could not transfer materialized tree ownership'
  if [ "$8" = --api-key-file ]; then
    if [ ! -f "$9" ] || [ -L "$9" ]; then
      fail 'API key is not a file'
    fi
    chown "$worker_uid:$worker_gid" "$9" || fail 'could not transfer API key ownership'
  fi
  if ! run_as_worker /usr/bin/test -x "$3" ||
    ! run_as_worker /usr/bin/test -r "$5"; then
    fail 'materialized tree is not accessible to the student worker'
  fi
  if [ "$8" = --api-key-file ]; then
    run_as_worker /usr/bin/test -r "$9" || fail 'API key is not accessible to the student worker'
  fi
  exec "$privilege" --reuid="$worker_uid" --regid="$worker_gid" \
    --clear-groups --no-new-privs "$runner" "$@"
fi

case ${1:-} in
  train)
    [ "$#" -eq 7 ] || usage
    if [ "$2" != --config ] || [ "$4" != --student-job-id ] ||
      [ "$6" != --work-dir ]; then
      usage
    fi
    operation=$1
    variant=
    work_dir=$7
    ;;
  branch)
    [ "$#" -eq 9 ] || usage
    if [ "$2" != --config ] || [ "$4" != --student-job-id ] ||
      [ "$6" != --variant ] || [ "$8" != --work-dir ]; then
      usage
    fi
    operation=$1
    variant=$7
    case $variant in bf16|dynamic_fp8|static_fp8) ;; *) usage ;; esac
    work_dir=$9
    ;;
  *) usage ;;
esac
config=$3
student_job_id=$5

case $config in /*) ;; *) usage ;; esac
[ -f "$config" ] || fail 'config is not a file'
case $student_job_id in *[!0-9a-f]*|'') usage ;; esac
[ "${#student_job_id}" -eq 64 ] || usage
case $work_dir in /*) ;; *) usage ;; esac
[ ! -e "$work_dir" ] || fail 'work directory must be new'
[ -n "$control_store_access_key" ] || fail 'control-store access key is unavailable'
[ -n "$control_store_secret_key" ] || fail 'control-store secret key is unavailable'
if [ "$control_store_session_set" = set ] && [ -z "$control_store_session_token" ]; then
  fail 'control-store session token cannot be empty'
fi

gateway=$(resolve_executable /usr/local/bin/dragontales-gateway) ||
  fail 'gateway executable is unavailable'
runner=$(resolve_executable "$script_dir/student-run.sh") ||
  fail 'student runner is unavailable'

if run_as_worker /usr/bin/test -r "$config"; then
  fail 'config is readable by the student worker'
else
  status=$?
  [ "$status" -eq 1 ] || fail 'could not verify config isolation' "$status"
fi

run_gateway() {
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

mkdir -m 0711 -- "$work_dir" || fail 'could not create work directory'
materialize_stdout=$work_dir/materialize.stdout
runner_stdout=$work_dir/runner.stdout
ingest_stdout=$work_dir/ingest.stdout
worker_dir=$work_dir/worker

if [ -z "$variant" ]; then
  set -- materialize-student-job --student-job-id "$student_job_id" --stage-dir "$work_dir/input"
else
  set -- materialize-student-branch --student-job-id "$student_job_id" \
    --variant "$variant" --stage-dir "$work_dir/input"
fi
if run_gateway "$@" >"$materialize_stdout"; then
  :
else
  status=$?
  fail "materialization failed; work preserved at $work_dir" "$status"
fi
if ! read_one_line "$materialize_stdout" || [ "$line" != "$student_job_id" ]; then
  fail "materialization receipt is ambiguous; work preserved at $work_dir"
fi
mkdir -m 0700 -- "$worker_dir" ||
  fail "could not create worker directory; work preserved at $work_dir"
chown -hR "$worker_uid:$worker_gid" "$work_dir/input" "$worker_dir" ||
  fail "could not transfer work ownership; work preserved at $work_dir"
run_as_worker /usr/bin/test -r "$work_dir/input/claim.json" ||
  fail "materialized work is not accessible to the student worker; work preserved at $work_dir"

if [ -z "$variant" ]; then
  set -- "$operation" --claim "$work_dir/input/claim.json" \
    --input "$work_dir/input/input.json" --output "$worker_dir/output"
else
  set -- "$operation" --claim "$work_dir/input/claim.json" \
    --input "$work_dir/input/input.json" \
    --train-result "$work_dir/input/train-result.json" \
    --fanout-claim "$work_dir/input/fanout-claim.json" \
    --merged-model "$work_dir/input/merged-model/model" \
    --merged-model-manifest "$work_dir/input/merged-model/model-manifest.json" \
    --variant "$variant" --output "$worker_dir/output"
fi
if run_as_worker "$runner" "$@" >"$runner_stdout"; then
  runner_status=0
else
  runner_status=$?
fi

result=$worker_dir/output/result.json
upload=$worker_dir/output/upload.json
artifact_dir=$worker_dir/output/artifact
if [ -f "$result" ] && [ -f "$upload" ]; then
  if [ -z "$variant" ]; then
    ingest='ingest-student-train-execution'
  else
    ingest='ingest-student-branch-execution'
  fi
  if run_gateway "$ingest" \
    --result "$result" --upload "$upload" --artifact-dir "$artifact_dir" \
    >"$ingest_stdout"; then
    :
  else
    status=$?
    fail "ingestion failed; work preserved at $work_dir" "$status"
  fi
  read_one_line "$ingest_stdout" ||
    fail "ingestion receipt is ambiguous; work preserved at $work_dir"
  if [ "$runner_status" -ne 0 ]; then
    fail "student runner failed after emitting an ingested result; work preserved at $work_dir" \
      "$runner_status"
  fi
  printf '%s\n' "$line"
  exit 0
fi

if [ "$runner_status" -ne 0 ]; then
  fail "student runner failed without a complete result; work preserved at $work_dir" \
    "$runner_status"
fi
fail "student runner emitted an incomplete result; work preserved at $work_dir"
