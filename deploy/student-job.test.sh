#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_wrapper=$root/deploy/student-job.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/milk-student-job.XXXXXX")
image_root=$test_root/image
wrapper=$image_root/student-job.sh
fake_gateway=$test_root/gateway
fake_runner=$test_root/runner
fake_privilege=$test_root/setpriv
log=$test_root/calls
config=$test_root/config.json
worker_uid=$(id -u)
worker_gid=$(id -g)

cleanup_work() {
  work=$1
  rm -f -- "$work/materialize.stdout" "$work/runner.stdout" "$work/ingest.stdout" \
    "$work/input/claim.json" "$work/input/input.json" "$work/input/train-result.json" \
    "$work/input/fanout-claim.json" "$work/input/merged-model/model/config.json" \
    "$work/input/merged-model/model-manifest.json" \
    "$work/worker/output/result.json" "$work/worker/output/upload.json"
  rmdir -- "$work/input/merged-model/model" "$work/input/merged-model" \
    "$work/worker/output/artifact" "$work/worker/output" "$work/worker" \
    "$work/input" "$work" 2>/dev/null || :
}

cleanup() {
  for name in success without-session empty-session branch failed-result missing missing-control existing production; do
    cleanup_work "$test_root/$name"
  done
  rm -f -- "$fake_gateway" "$fake_runner" "$fake_privilege" "$log" "$config" \
    "$test_root/serve-key" "$test_root/serve/model-manifest.json" \
    "$image_root/student-job.sh" "$image_root/student-run.sh"
  rmdir -- "$test_root/serve/model" "$test_root/serve" "$image_root" "$test_root" 2>/dev/null || :
}
trap cleanup EXIT HUP INT TERM

mkdir "$image_root"
: >"$config"
chmod 0400 "$config"

cat >"$fake_gateway" <<'EOF'
#!/bin/sh
set -eu
base=${0%/*}
log=$base/calls
config=$base/config.json
[ "${MILK_CONTROL_STORE_ACCESS_KEY_ID:-}" = control-access ] || exit 70
[ "${MILK_CONTROL_STORE_SECRET_ACCESS_KEY:-}" = control-secret ] || exit 71
case ${MILK_CONTROL_STORE_SESSION_TOKEN+set}:${MILK_CONTROL_STORE_SESSION_TOKEN-} in
  set:control-session|:) ;;
  *) exit 72 ;;
esac
[ -z "${MILK_CAPTURE_STORE_ACCESS_KEY_ID+x}" ] || exit 73
[ -z "${DRAGONTALES_CONFIG_JSON+x}" ] || exit 72
[ -z "${TEST_RUNNER_BEHAVIOR+x}" ] || exit 73
printf 'gateway' >>"$log"
for argument do printf '|%s' "$argument" >>"$log"; done
printf '\n' >>"$log"
[ "$1" = --config ] && [ "$2" = "$config" ] || exit 80
case $3 in
  materialize-student-job)
    [ "$4" = --student-job-id ] && [ "$6" = --stage-dir ] || exit 81
    /bin/mkdir -m 0700 -- "$7"
    printf '{}\n' >"$7/claim.json"
    printf '{}\n' >"$7/input.json"
    printf '%s\n' "$5"
    ;;
  materialize-student-branch)
    [ "$4" = --student-job-id ] && [ "$6" = --variant ] && \
      [ "$8" = --stage-dir ] || exit 81
    /bin/mkdir -m 0700 -p -- "$9/merged-model/model"
    printf '{}\n' >"$9/claim.json"
    printf '{}\n' >"$9/input.json"
    printf '{}\n' >"$9/train-result.json"
    printf '{}\n' >"$9/fanout-claim.json"
    printf '{}\n' >"$9/merged-model/model-manifest.json"
    printf '{}\n' >"$9/merged-model/model/config.json"
    printf '%s\n' "$5"
    ;;
  ingest-student-train-execution|ingest-student-branch-execution)
    [ "$4" = --result ] && [ "$6" = --upload ] && [ "$8" = --artifact-dir ] || exit 82
    [ -f "$5" ] && [ -f "$7" ] && [ -d "$9" ] || exit 83
    printf '{"status":"ready"}\n'
    ;;
  *) exit 84 ;;
esac
EOF

cat >"$fake_runner" <<'EOF'
#!/bin/sh
set -eu
[ "${SET_PRIV_CALLED:-}" = 1 ] || exit 74
for secret in DRAGONTALES_CONFIG_JSON MILK_CAPTURE_STORE_ACCESS_KEY_ID \
  MILK_CAPTURE_STORE_SECRET_ACCESS_KEY MILK_CAPTURE_STORE_SESSION_TOKEN \
  MILK_CONTROL_STORE_ACCESS_KEY_ID MILK_CONTROL_STORE_SECRET_ACCESS_KEY \
  MILK_CONTROL_STORE_SESSION_TOKEN MILK_ROUTE_STORE_ACCESS_KEY_ID \
  MILK_ROUTE_STORE_SECRET_ACCESS_KEY MILK_ROUTE_STORE_SESSION_TOKEN \
  AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
  AWS_SESSION_TOKEN R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
do
  eval "present=\${$secret+x}"
  [ -z "$present" ] || exit 75
done
printf 'runner' >>"$TEST_LOG"
for argument do printf '|%s' "$argument" >>"$TEST_LOG"; done
printf '\n' >>"$TEST_LOG"
case $1 in
  serve)
    [ "$2" = --model ] && [ -d "$3" ] && \
      [ "$4" = --model-manifest ] && [ -f "$5" ] && \
      [ "$6" = --model-alias ] && [ "$7" = logical-model ] || exit 85
    case $#:$8 in
      9:--api-key-file) [ -f "$9" ] || exit 85 ;;
      8:--trusted-ingress-auth) ;;
      *) exit 85 ;;
    esac
    exit 0
    ;;
  fixture-train)
    [ "$2" = --claim ] && [ -f "$3" ] && [ "$4" = --input ] && [ -f "$5" ] && \
      [ "$6" = --output ] || exit 86
    output=$7
    ;;
  fixture-branch)
    [ "$2" = --claim ] && [ -f "$3" ] && [ "$4" = --input ] && [ -f "$5" ] && \
      [ "$6" = --train-result ] && [ -f "$7" ] && \
      [ "$8" = --fanout-claim ] && [ -f "$9" ] && \
      [ "${10}" = --merged-model ] && [ -d "${11}" ] && \
      [ "${12}" = --merged-model-manifest ] && [ -f "${13}" ] && \
      [ "${14}" = --variant ] && [ "${16}" = --output ] || exit 86
    output=${17}
    ;;
  *) exit 85 ;;
esac
case $TEST_RUNNER_BEHAVIOR in
  success|failed-result)
    mkdir -m 0700 -- "$output" "$output/artifact"
    printf '{}\n' >"$output/result.json"
    printf '{}\n' >"$output/upload.json"
    [ "$TEST_RUNNER_BEHAVIOR" = success ] || exit 23
    ;;
  missing)
    mkdir -m 0700 -- "$output" "$output/artifact"
    printf '{}\n' >"$output/upload.json"
    exit 29
    ;;
  *) exit 87 ;;
esac
EOF

cat >"$fake_privilege" <<'EOF'
#!/bin/sh
set -eu
[ "$1" = "--reuid=$TEST_WORKER_UID" ]
[ "$2" = "--regid=$TEST_WORKER_GID" ]
[ "$3" = --clear-groups ]
[ "$4" = --no-new-privs ]
shift 4
if [ "${1:-}" = /usr/bin/test ]; then
  if [ "${2:-}" = -r ] && [ "${3:-}" = "$TEST_CONFIG" ]; then
    exit 1
  fi
  exec /bin/test "$2" "$3"
fi
export SET_PRIV_CALLED=1
exec "$@"
EOF

sed -e "s|/usr/bin/setpriv|$fake_privilege|" \
  -e "s/^worker_uid=65532$/worker_uid=$worker_uid/" \
  -e "s/^worker_gid=65532$/worker_gid=$worker_gid/" \
  "$source_wrapper" >"$wrapper"
cp "$fake_runner" "$image_root/student-run.sh"
chmod 0700 "$fake_gateway" "$fake_runner" "$fake_privilege" \
  "$wrapper" "$image_root/student-run.sh"

run_wrapper() {
  DRAGONTALES_GATEWAY_BIN=$fake_gateway \
  DRAGONTALES_STUDENT_RUNNER=$fake_runner \
  DRAGONTALES_CONFIG_JSON=config-secret \
  MILK_CAPTURE_STORE_ACCESS_KEY_ID=capture-poison \
  MILK_CAPTURE_STORE_SECRET_ACCESS_KEY=capture-poison \
  MILK_CAPTURE_STORE_SESSION_TOKEN=capture-poison \
  MILK_ROUTE_STORE_ACCESS_KEY_ID=route-poison \
  MILK_ROUTE_STORE_SECRET_ACCESS_KEY=route-poison \
  MILK_ROUTE_STORE_SESSION_TOKEN=route-poison \
  MILK_CONTROL_STORE_ACCESS_KEY_ID=control-access \
  MILK_CONTROL_STORE_SECRET_ACCESS_KEY=control-secret \
  MILK_CONTROL_STORE_SESSION_TOKEN=control-session \
  AWS_ACCESS_KEY_ID=aws-access AWS_SECRET_ACCESS_KEY=aws-secret AWS_SESSION_TOKEN=aws-session \
  R2_ACCESS_KEY_ID=other-access R2_SECRET_ACCESS_KEY=other-secret \
  TEST_LOG=$log TEST_CONFIG=$config TEST_RUNNER_BEHAVIOR=$2 \
  TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
    "$wrapper" "$3" --config "$config" --student-job-id "$1" --work-dir "$4"
}

run_branch() {
  DRAGONTALES_GATEWAY_BIN=$fake_gateway \
  DRAGONTALES_STUDENT_RUNNER=$fake_runner \
  DRAGONTALES_CONFIG_JSON=config-secret \
  MILK_CONTROL_STORE_ACCESS_KEY_ID=control-access \
  MILK_CONTROL_STORE_SECRET_ACCESS_KEY=control-secret \
  MILK_CONTROL_STORE_SESSION_TOKEN=control-session \
  TEST_LOG=$log TEST_CONFIG=$config TEST_RUNNER_BEHAVIOR=$2 \
  TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
    "$wrapper" fixture-branch --config "$config" --student-job-id "$1" \
      --variant "$3" --work-dir "$4"
}

assert_calls() {
  expected=$1
  actual=$(cat "$log")
  [ "$actual" = "$expected" ] || {
    printf 'unexpected calls:\n%s\nexpected:\n%s\n' "$actual" "$expected" >&2
    exit 1
  }
}

job_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
success_work=$test_root/success
: >"$log"
receipt=$(run_wrapper "$job_id" success fixture-train "$success_work")
[ "$receipt" = '{"status":"ready"}' ]
assert_calls "gateway|--config|$config|materialize-student-job|--student-job-id|$job_id|--stage-dir|$success_work/input
runner|fixture-train|--claim|$success_work/input/claim.json|--input|$success_work/input/input.json|--output|$success_work/worker/output
gateway|--config|$config|ingest-student-train-execution|--result|$success_work/worker/output/result.json|--upload|$success_work/worker/output/upload.json|--artifact-dir|$success_work/worker/output/artifact"

without_session_work=$test_root/without-session
: >"$log"
receipt=$(DRAGONTALES_GATEWAY_BIN=$fake_gateway DRAGONTALES_STUDENT_RUNNER=$fake_runner \
  DRAGONTALES_CONFIG_JSON=config-secret \
  MILK_CONTROL_STORE_ACCESS_KEY_ID=control-access \
  MILK_CONTROL_STORE_SECRET_ACCESS_KEY=control-secret \
  TEST_LOG=$log TEST_CONFIG=$config TEST_RUNNER_BEHAVIOR=success \
  TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
  "$wrapper" fixture-train --config "$config" --student-job-id "$job_id" \
    --work-dir "$without_session_work")
[ "$receipt" = '{"status":"ready"}' ]

empty_session_work=$test_root/empty-session
: >"$log"
set +e
DRAGONTALES_GATEWAY_BIN=$fake_gateway DRAGONTALES_STUDENT_RUNNER=$fake_runner \
DRAGONTALES_CONFIG_JSON=config-secret MILK_CONTROL_STORE_ACCESS_KEY_ID=control-access \
MILK_CONTROL_STORE_SECRET_ACCESS_KEY=control-secret MILK_CONTROL_STORE_SESSION_TOKEN= \
TEST_LOG=$log TEST_CONFIG=$config TEST_RUNNER_BEHAVIOR=success \
TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
  "$wrapper" fixture-train --config "$config" --student-job-id "$job_id" \
  --work-dir "$empty_session_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -ne 0 ] && [ ! -s "$log" ] && [ ! -e "$empty_session_work" ]

branch_work=$test_root/branch
: >"$log"
receipt=$(run_branch "$job_id" success dynamic_fp8 "$branch_work")
[ "$receipt" = '{"status":"ready"}' ]
assert_calls "gateway|--config|$config|materialize-student-branch|--student-job-id|$job_id|--variant|dynamic_fp8|--stage-dir|$branch_work/input
runner|fixture-branch|--claim|$branch_work/input/claim.json|--input|$branch_work/input/input.json|--train-result|$branch_work/input/train-result.json|--fanout-claim|$branch_work/input/fanout-claim.json|--merged-model|$branch_work/input/merged-model/model|--merged-model-manifest|$branch_work/input/merged-model/model-manifest.json|--variant|dynamic_fp8|--output|$branch_work/worker/output
gateway|--config|$config|ingest-student-branch-execution|--result|$branch_work/worker/output/result.json|--upload|$branch_work/worker/output/upload.json|--artifact-dir|$branch_work/worker/output/artifact"

failed_work=$test_root/failed-result
: >"$log"
set +e
run_wrapper "$job_id" failed-result fixture-train "$failed_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 23 ]
[ "$(grep -c '^gateway|' "$log")" -eq 2 ]

missing_work=$test_root/missing
: >"$log"
set +e
run_wrapper "$job_id" missing fixture-train "$missing_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 29 ] && [ "$(grep -c '^gateway|' "$log")" -eq 1 ]

missing_control_work=$test_root/missing-control
: >"$log"
set +e
DRAGONTALES_GATEWAY_BIN=$fake_gateway DRAGONTALES_STUDENT_RUNNER=$fake_runner \
TEST_LOG=$log TEST_CONFIG=$config TEST_RUNNER_BEHAVIOR=success \
TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
  "$wrapper" fixture-train --config "$config" --student-job-id "$job_id" \
  --work-dir "$missing_control_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -ne 0 ] && [ ! -s "$log" ] && [ ! -e "$missing_control_work" ]

existing_work=$test_root/existing
mkdir "$existing_work"
: >"$log"
set +e
run_wrapper "$job_id" success train "$existing_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -ne 0 ] && [ ! -s "$log" ]

production_work=$test_root/production
: >"$log"
set +e
run_wrapper "$job_id" success train "$production_work" >/dev/null 2>&1
status=$?
set -e
[ "$status" -ne 0 ] && [ ! -s "$log" ] && [ ! -e "$production_work" ]

serve_model=$test_root/serve/model
serve_manifest=$test_root/serve/model-manifest.json
serve_key=$test_root/serve-key
mkdir -p "$serve_model"
: >"$serve_manifest"
: >"$serve_key"
for auth in key trusted; do
  : >"$log"
  if [ "$auth" = key ]; then
    set -- --api-key-file "$serve_key"
  else
    set -- --trusted-ingress-auth
  fi
  DRAGONTALES_CONFIG_JSON=config-secret \
  MILK_CONTROL_STORE_ACCESS_KEY_ID=control-access \
  MILK_CONTROL_STORE_SECRET_ACCESS_KEY=control-secret \
  MILK_CONTROL_STORE_SESSION_TOKEN=control-session \
  TEST_LOG=$log TEST_CONFIG=$config TEST_WORKER_UID=$worker_uid TEST_WORKER_GID=$worker_gid \
    "$wrapper" serve --model "$serve_model" --model-manifest "$serve_manifest" \
    --model-alias logical-model "$@"
  if [ "$auth" = key ]; then
    expected="runner|serve|--model|$serve_model|--model-manifest|$serve_manifest|--model-alias|logical-model|--api-key-file|$serve_key"
  else
    expected="runner|serve|--model|$serve_model|--model-manifest|$serve_manifest|--model-alias|logical-model|--trusted-ingress-auth"
  fi
  assert_calls "$expected"
done

grep -Fq 'worker_uid=65532' "$source_wrapper"
grep -Fq 'worker_gid=65532' "$source_wrapper"
grep -Fq 'env -i \' "$source_wrapper"
grep -Fq 'run_as_worker /usr/bin/test -r "$config"' "$source_wrapper"
grep -Fq 'mkdir -m 0711 -- "$work_dir"' "$source_wrapper"
grep -Fq 'chown -hR "$worker_uid:$worker_gid" "$work_dir/input" "$worker_dir"' "$source_wrapper"
grep -Fq -- '--no-new-privs "$runner" "$@"' "$source_wrapper"

sh -n "$source_wrapper" "$wrapper" "$0"
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -s sh "$source_wrapper" "$0"
fi
printf 'student job wrapper tests: ok\n'
