#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd -P)
source_command=$root/tools/exo/milk-managed
installer=$root/tools/exo/install-host-command
temporary_root=${TMPDIR:-/tmp}
temporary_root=${temporary_root%/}
temporary_root=$(CDPATH='' cd -- "$temporary_root" && pwd -P)
test_root=$(mktemp -d "$temporary_root/milk-managed-test.XXXXXX")

cleanup() {
  case "$test_root" in
    "$temporary_root"/milk-managed-test.*) rm -rf -- "$test_root" ;;
  esac
}
trap cleanup EXIT HUP INT TERM
mkdir -m 0700 "$test_root/bin" "$test_root/minimal-bin" "$test_root/state" \
  "$test_root/command"
mkdir -m 0750 "$test_root/evals"
cp "$source_command" "$test_root/command/milk-managed"
chmod 0700 "$test_root/command/milk-managed"
command=$test_root/command/milk-managed
github_token_file=$test_root/github-token
printf '%s\n' github-secret >"$github_token_file"
chmod 0400 "$github_token_file"

for utility in id mkdir python3 rmdir sed stat tr wc
do
  ln -s "$(command -v "$utility")" "$test_root/minimal-bin/$utility"
done

cat >"$test_root/command/github-rest.py" <<'SH'
#!/bin/sh
set -eu
operation=${1:-}
token_file=${2:-}
[ -f "$token_file" ] && [ ! -L "$token_file" ]
[ "$(stat -c '%a' "$token_file" 2>/dev/null || stat -f '%Lp' "$token_file")" = 400 ]
IFS= read -r token <"$token_file"
[ "$token" = github-secret ]
shift 2
case "$operation" in
  check)
    [ "$#" -eq 0 ]
    ;;
  correlate)
    : >"$TEST_GITHUB_LIST_LOG"
    for argument do printf '%s\n' "$argument" >>"$TEST_GITHUB_LIST_LOG"; done
    [ "${TEST_GITHUB_LIST_FAIL:-0}" -eq 0 ] || exit 1
    [ -z "${TEST_GITHUB_DATABASE_ID:-}" ] || \
      printf '%s\n' "$TEST_GITHUB_DATABASE_ID"
    ;;
  view)
    printf '%s\n' "${5:-}" >"$TEST_GITHUB_VIEW_LOG"
    [ "${TEST_GITHUB_VIEW_FAIL:-0}" -eq 0 ] || exit 1
    : >"$TEST_GITHUB_JOB_VIEW_LOG"
    for argument do printf '%s\n' "$argument" >>"$TEST_GITHUB_JOB_VIEW_LOG"; done
    printf '%s\n' "${TEST_GITHUB_OBSERVATION:-queued:}"
    [ "${TEST_GITHUB_OBSERVATION:-queued:}" != completed:success ] || \
      printf '%s\n' "$TEST_GITHUB_GENERATION_JOB_NAME"
    ;;
  dispatch)
    : >"$TEST_GITHUB_DISPATCH_LOG"
    for argument do printf '%s\n' "$argument" >>"$TEST_GITHUB_DISPATCH_LOG"; done
    [ "${TEST_GITHUB_WORKFLOW_SLEEP:-0}" = 0 ] || \
      sleep "$TEST_GITHUB_WORKFLOW_SLEEP"
    [ "${TEST_GITHUB_WORKFLOW_FAIL:-0}" -eq 0 ] || exit 1
    ;;
  *) exit 64 ;;
esac
SH
chmod 0700 "$test_root/command/github-rest.py"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 -- "$1" | cut -d ' ' -f 1
  fi
}

admit_eval() {
  source_file=$1
  admitted_eval_id=$2
  mv -- "$source_file" "$test_root/evals/$admitted_eval_id.json"
  chmod 0440 "$test_root/evals/$admitted_eval_id.json"
}

eval_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
second_eval_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"},"name":"primary"}\n' \
  "$eval_id" "$eval_id" >"$test_root/primary.json"
eval_confirmation_sha256=$(sha256_file "$test_root/primary.json")
admit_eval "$test_root/primary.json" "$eval_id"
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"},"name":"secondary"}\n' \
  "$second_eval_id" "$second_eval_id" >"$test_root/secondary.json"
second_eval_confirmation_sha256=$(sha256_file "$test_root/secondary.json")
admit_eval "$test_root/secondary.json" "$second_eval_id"

dispatch_log=$test_root/dispatch.log
list_log=$test_root/list.log
view_log=$test_root/view.log
job_view_log=$test_root/job-view.log

run() {
  PATH="$test_root/bin:$PATH" \
  MILK_MANAGED_EVAL_DIR="$test_root/evals" \
  MILK_MANAGED_STATE_ROOT="$test_root/state" \
  MILK_MANAGED_EVAL_OWNER_UID="$(id -u)" \
  MILK_MANAGED_GITHUB_TOKEN_FILE="$github_token_file" \
  TEST_GITHUB_DATABASE_ID="${TEST_GITHUB_DATABASE_ID:-}" \
  TEST_GITHUB_OBSERVATION="${TEST_GITHUB_OBSERVATION:-queued:}" \
  TEST_GITHUB_GENERATION_JOB_NAME="${TEST_GITHUB_GENERATION_JOB_NAME-Provider reconciliation and dispatch [generation_done=false]}" \
  TEST_GITHUB_LIST_FAIL="${TEST_GITHUB_LIST_FAIL:-0}" \
  TEST_GITHUB_VIEW_FAIL="${TEST_GITHUB_VIEW_FAIL:-0}" \
  TEST_GITHUB_WORKFLOW_FAIL="${TEST_GITHUB_WORKFLOW_FAIL:-0}" \
  TEST_GITHUB_WORKFLOW_SLEEP="${TEST_GITHUB_WORKFLOW_SLEEP:-0}" \
  TEST_GITHUB_DISPATCH_LOG="$dispatch_log" \
  TEST_GITHUB_LIST_LOG="$list_log" \
  TEST_GITHUB_VIEW_LOG="$view_log" \
  TEST_GITHUB_JOB_VIEW_LOG="$job_view_log" \
    "$command" "$@"
}

run_without_credential() {
  PATH="$test_root/minimal-bin" \
  MILK_MANAGED_EVAL_DIR="$test_root/evals" \
  MILK_MANAGED_STATE_ROOT="$test_root/state" \
  MILK_MANAGED_EVAL_OWNER_UID="$(id -u)" \
    "$command" "$@"
}

expected() {
  expected_generation_done=${7:-false}
  printf '{"ok":%s,"eval_id":"%s","state":"%s","dispatch_state":"%s","generation_done":%s,"changed":%s,"approval_required":%s}' \
    "$1" "$2" "$3" "$4" "$expected_generation_done" "$5" "$6"
}

state_dir=$test_root/state/$eval_id
state=$state_dir/managed-dispatch.state
approval=$state_dir/run-confirmed.approval
second_state=$test_root/state/$second_eval_id/managed-dispatch.state

[ "$(run_without_credential status "$second_eval_id")" = \
  "$(expected true "$second_eval_id" idle idle false false)" ]
[ "$(run_without_credential reconcile "$second_eval_id")" = \
  "$(expected false "$second_eval_id" blocked unknown false false)" ]
[ "$(run_without_credential run_confirmed "$second_eval_id")" = \
  "$(expected false "$second_eval_id" blocked unknown false false)" ]
[ ! -e "$second_state" ]

chmod 0644 "$github_token_file"
[ "$(run reconcile "$second_eval_id")" = \
  "$(expected false "$second_eval_id" blocked unknown false false)" ]
[ ! -e "$second_state" ]
chmod 0400 "$github_token_file"

[ "$(run status "$eval_id")" = \
  "$(expected true "$eval_id" idle idle false false)" ]
[ -d "$state_dir" ]

first_output=$test_root/first-output
TEST_GITHUB_WORKFLOW_SLEEP=1 TEST_GITHUB_DATABASE_ID=77 \
  run reconcile "$eval_id" >"$first_output" &
first_pid=$!
lock_attempts=100
while [ ! -d "$state.lock" ] && [ "$lock_attempts" -gt 0 ]; do
  lock_attempts=$((lock_attempts - 1))
  sleep 0.01
done
[ -d "$state.lock" ]
[ "$(run reconcile "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
[ "$(run status "$second_eval_id")" = \
  "$(expected true "$second_eval_id" idle idle false false)" ]
wait "$first_pid"
[ "$(cat "$first_output")" = \
  "$(expected true "$eval_id" running running true false)" ]
[ ! -e "$state.lock" ]
rm -f "$state" "$dispatch_log" "$list_log" "$view_log"

[ "$(TEST_GITHUB_DATABASE_ID=101 run reconcile "$eval_id")" = \
  "$(expected true "$eval_id" running running true false)" ]
[ -f "$state" ] && [ ! -L "$state" ]
[ "$(stat -c '%a' "$state" 2>/dev/null || stat -f '%Lp' "$state")" = 600 ]
[ "$(sed -n '1p' "$state")" = milk-managed-state-v3 ]
[ "$(sed -n '2p' "$state")" = reconcile ]
request_id=$(sed -n '3p' "$state")
[ "${#request_id}" -eq 97 ]
[ "${request_id#"$eval_id"-}" != "$request_id" ]
request_nonce=${request_id#"$eval_id"-}
[ "${#request_nonce}" -eq 32 ]
case "$request_nonce" in *[!0-9a-f]*) exit 1 ;; esac
managed_title="Milk production loop [managed:$request_id]"
[ "${#managed_title}" -le 255 ]
[ "$(sed -n '4p' "$state")" = 101 ]
[ "$(sed -n '5p' "$state")" = "$eval_id" ]
[ "$(sed -n '6p' "$state")" = "$eval_confirmation_sha256" ]
[ "$(wc -l <"$state" | tr -d '[:space:]')" = 6 ]
[ "$(sed -n '1p' "$dispatch_log")" = milkinfrastructure ]
[ "$(sed -n '2p' "$dispatch_log")" = milk-harness ]
[ "$(sed -n '3p' "$dispatch_log")" = production-loop.yml ]
[ "$(sed -n '4p' "$dispatch_log")" = main ]
[ "$(sed -n '5p' "$dispatch_log")" = false ]
[ "$(sed -n '6p' "$dispatch_log")" = "$eval_confirmation_sha256" ]
[ "$(sed -n '7p' "$dispatch_log")" = "$eval_id" ]
[ "$(sed -n '8p' "$dispatch_log")" = "$request_id" ]
[ "$(sed -n '1p' "$list_log")" = milkinfrastructure ]
[ "$(sed -n '2p' "$list_log")" = milk-harness ]
[ "$(sed -n '3p' "$list_log")" = production-loop.yml ]
[ "$(sed -n '4p' "$list_log")" = main ]
[ "$(sed -n '5p' "$list_log")" = "$request_id" ]
[ "$(run_without_credential status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]

rm -f "$dispatch_log" "$list_log" "$view_log"
[ "$(MILK_MANAGED_REPOSITORY=github.com/example/milk-harness \
  MILK_MANAGED_WORKFLOW=self-host-loop.yml \
  MILK_MANAGED_WORKFLOW_REF=stable \
  TEST_GITHUB_DATABASE_ID=404 run reconcile "$second_eval_id")" = \
  "$(expected true "$second_eval_id" running running true false)" ]
[ "$(sed -n '1p' "$dispatch_log")" = example ]
[ "$(sed -n '2p' "$dispatch_log")" = milk-harness ]
[ "$(sed -n '3p' "$dispatch_log")" = self-host-loop.yml ]
[ "$(sed -n '4p' "$dispatch_log")" = stable ]
[ "$(sed -n '6p' "$dispatch_log")" = "$second_eval_confirmation_sha256" ]
[ "$(sed -n '7p' "$dispatch_log")" = "$second_eval_id" ]

for invalid_target in \
  'MILK_MANAGED_REPOSITORY=example/milk-harness' \
  'MILK_MANAGED_REPOSITORY=github.com/example/../milk-harness' \
  'MILK_MANAGED_WORKFLOW=../production-loop.yml' \
  'MILK_MANAGED_WORKFLOW_REF=refs//heads/main'
do
  target_name=${invalid_target%%=*}
  target_value=${invalid_target#*=}
  set +e
  invalid_output=$(env "$target_name=$target_value" \
    PATH="$test_root/bin:$PATH" \
    MILK_MANAGED_EVAL_DIR="$test_root/evals" \
    MILK_MANAGED_STATE_ROOT="$test_root/state" \
    MILK_MANAGED_EVAL_OWNER_UID="$(id -u)" \
    "$command" status "$second_eval_id" 2>/dev/null)
  invalid_status=$?
  set -e
  [ "$invalid_status" -eq 64 ]
  [ -z "$invalid_output" ]
done

rm -f "$list_log" "$view_log"
[ "$(TEST_GITHUB_OBSERVATION=queued: run status "$eval_id")" = \
  "$(expected true "$eval_id" running running false false)" ]
[ "$(cat "$view_log")" = 101 ]
[ ! -e "$list_log" ]
[ "$(TEST_GITHUB_OBSERVATION=completed:success run status "$eval_id")" = \
  "$(expected true "$eval_id" ready succeeded false false)" ]
[ "$(sed -n '1p' "$job_view_log")" = milkinfrastructure ]
[ "$(sed -n '2p' "$job_view_log")" = milk-harness ]
[ "$(sed -n '3p' "$job_view_log")" = production-loop.yml ]
[ "$(sed -n '4p' "$job_view_log")" = main ]
[ "$(sed -n '5p' "$job_view_log")" = 101 ]
[ "$(TEST_GITHUB_OBSERVATION=completed:success \
  TEST_GITHUB_GENERATION_JOB_NAME='Provider reconciliation and dispatch [generation_done=true]' \
  run status "$eval_id")" = \
  "$(expected true "$eval_id" ready succeeded false false true)" ]
[ "$(TEST_GITHUB_OBSERVATION=completed:success \
  TEST_GITHUB_GENERATION_JOB_NAME='Provider reconciliation and dispatch [generation_done=unknown]' \
  run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
duplicate_job_names=$(printf '%s\n%s' \
  'Provider reconciliation and dispatch [generation_done=false]' \
  'Provider reconciliation and dispatch [generation_done=true]')
[ "$(TEST_GITHUB_OBSERVATION=completed:success \
  TEST_GITHUB_GENERATION_JOB_NAME="$duplicate_job_names" run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
[ "$(TEST_GITHUB_OBSERVATION=completed:success TEST_GITHUB_GENERATION_JOB_NAME='' \
  run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]

rm -f "$dispatch_log" "$approval"
[ "$(TEST_GITHUB_OBSERVATION=completed:success \
  TEST_GITHUB_GENERATION_JOB_NAME='Provider reconciliation and dispatch [generation_done=true]' \
  TEST_GITHUB_DATABASE_ID=201 \
  run reconcile "$eval_id")" = \
  "$(expected true "$eval_id" running running true false true)" ]
[ "$(sed -n '5p' "$dispatch_log")" = false ]
rm -f "$dispatch_log"
printf '%s\n' "$eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GITHUB_OBSERVATION=completed:success \
  TEST_GITHUB_GENERATION_JOB_NAME='Provider reconciliation and dispatch [generation_done=true]' \
  TEST_GITHUB_DATABASE_ID=202 \
  run run_confirmed "$eval_id")" = \
  "$(expected true "$eval_id" running running true false true)" ]
[ ! -e "$approval" ]
[ "$(sed -n '5p' "$dispatch_log")" = true ]
[ "$(sed -n '6p' "$dispatch_log")" = "$eval_confirmation_sha256" ]
[ "$(sed -n '7p' "$dispatch_log")" = "$eval_id" ]
rm -f "$dispatch_log"
[ "$(TEST_GITHUB_OBSERVATION=completed:neutral run status "$eval_id")" = \
  "$(expected false "$eval_id" failed failed false false)" ]
[ "$(TEST_GITHUB_OBSERVATION=completed:skipped run status "$eval_id")" = \
  "$(expected false "$eval_id" failed failed false false)" ]
[ "$(TEST_GITHUB_OBSERVATION=completed:action_required run status "$eval_id")" = \
  "$(expected true "$eval_id" blocked action_required false true)" ]
[ "$(TEST_GITHUB_VIEW_FAIL=1 run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]

rm -f "$approval" "$dispatch_log"
[ "$(TEST_GITHUB_OBSERVATION=completed:success run run_confirmed "$eval_id")" = \
  "$(expected true "$eval_id" waiting_for_confirmation succeeded false true)" ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GITHUB_OBSERVATION=completed:success TEST_GITHUB_DATABASE_ID=202 run run_confirmed "$eval_id")" = \
  "$(expected true "$eval_id" running running true false)" ]
[ ! -e "$approval" ]
[ "$(sed -n '2p' "$state")" = run_confirmed ]
request_id=$(sed -n '3p' "$state")
[ "$(sed -n '4p' "$state")" = 202 ]
[ "$(sed -n '5p' "$dispatch_log")" = true ]
[ "$(sed -n '6p' "$dispatch_log")" = "$eval_confirmation_sha256" ]
[ "$(sed -n '7p' "$dispatch_log")" = "$eval_id" ]
[ "$(sed -n '8p' "$dispatch_log")" = "$request_id" ]

rm -f "$dispatch_log"
printf '%s\n' "$second_eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GITHUB_OBSERVATION=completed:success run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked succeeded true true)" ]
[ ! -e "$approval" ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
chmod 0640 "$test_root/evals/$eval_id.json"
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"},"name":"changed-after-approval"}\n' \
  "$eval_id" "$eval_id" >"$test_root/evals/$eval_id.json"
chmod 0440 "$test_root/evals/$eval_id.json"
[ "$(TEST_GITHUB_OBSERVATION=completed:success run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
[ -e "$approval" ]
[ ! -e "$dispatch_log" ]

chmod 0640 "$test_root/evals/$eval_id.json"
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"},"name":"primary"}\n' \
  "$eval_id" "$eval_id" >"$test_root/evals/$eval_id.json"
chmod 0440 "$test_root/evals/$eval_id.json"
[ "$(sha256_file "$test_root/evals/$eval_id.json")" = "$eval_confirmation_sha256" ]

printf '%s\n' "$eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GITHUB_OBSERVATION=completed:success TEST_GITHUB_WORKFLOW_FAIL=1 TEST_GITHUB_LIST_FAIL=1 run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown true false)" ]
[ ! -e "$approval" ]
[ "$(sed -n '4p' "$state")" = pending ]

printf '%s\n' "$eval_confirmation_sha256" >"$approval"
chmod 0600 "$approval"
rm -f "$dispatch_log"
[ "$(TEST_GITHUB_LIST_FAIL=1 run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
[ -e "$approval" ]
[ ! -e "$dispatch_log" ]

[ "$(TEST_GITHUB_DATABASE_ID=303 TEST_GITHUB_OBSERVATION=queued: run status "$eval_id")" = \
  "$(expected true "$eval_id" running running false false)" ]
[ "$(sed -n '4p' "$state")" = 303 ]
[ -e "$approval" ]

rm -f "$state" "$approval"
chmod 0640 "$test_root/evals/$eval_id.json"
printf 'tamper\n' >>"$test_root/evals/$eval_id.json"
chmod 0440 "$test_root/evals/$eval_id.json"
[ "$(run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]

missing_eval_id=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
[ "$(run status "$missing_eval_id")" = \
  "$(expected false "$missing_eval_id" blocked unknown false false)" ]

wrong_binding_id=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"}}\n' \
  "$eval_id" "$wrong_binding_id" >"$test_root/evals/$wrong_binding_id.json"
chmod 0440 "$test_root/evals/$wrong_binding_id.json"
[ "$(run status "$wrong_binding_id")" = \
  "$(expected false "$wrong_binding_id" blocked unknown false false)" ]

symlink_eval_id=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
printf '{"schema_version":"milk.eval.test.v1","manifest":{"campaign_id":"%s"},"gateway_config":{"eval_id":"%s"},"name":"symlink"}\n' \
  "$symlink_eval_id" "$symlink_eval_id" >"$test_root/symlink-source.json"
ln -s "$test_root/symlink-source.json" "$test_root/evals/$symlink_eval_id.json"
[ "$(run status "$symlink_eval_id")" = \
  "$(expected false "$symlink_eval_id" blocked unknown false false)" ]

chmod 0755 "$test_root/evals"
[ "$(run status "$second_eval_id")" = \
  "$(expected false "$second_eval_id" blocked unknown false false)" ]
chmod 0750 "$test_root/evals"
chmod 0755 "$test_root/state"
[ "$(run status "$second_eval_id")" = \
  "$(expected false "$second_eval_id" blocked unknown false false)" ]
chmod 0700 "$test_root/state"

for invalid_args in \
  'unknown aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  'status AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' \
  'status ../../arbitrary-path' \
  'status' \
  'status aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa extra'
do
  set +e
  invalid_output=$(run $invalid_args 2>/dev/null)
  invalid_status=$?
  set -e
  [ "$invalid_status" -eq 64 ]
  [ -z "$invalid_output" ]
done

sh -n "$source_command" "$command" "$installer" "$0"
grep -Fq "install -d -o root -g \"\$service_gid\" -m 0750 /etc/milk/evals" "$installer"
grep -Fq '/var/lib/milk /var/lib/milk/evals' "$installer"
grep -Fq '/opt/milk/bin/github-rest.py' "$installer"
! grep -Fq 'command -v gh' "$source_command"
! grep -Fq 'gh workflow' "$source_command"
grep -Fq '/opt/milk/bin/milk-managed' "$root/tools/exo/README.md"
grep -Fq '/etc/milk/github-token' "$root/tools/exo/README.md"
grep -Fq 'sudo install -o root' "$root/tools/exo/README.md"
grep -Fq "\"/etc/milk/evals/\$EVAL_ID.json\"" "$root/tools/exo/README.md"
grep -Fq 'generation_done' "$root/tools/exo/README.md"
grep -Fq 'Provider reconciliation and dispatch [generation_done=true|false]' "$root/tools/exo/README.md"
grep -Fq 'exact document SHA-256' "$root/tools/exo/README.md"
for secret_check in "$dispatch_log" "$list_log" "$view_log" "$job_view_log"; do
  [ ! -f "$secret_check" ] || ! grep -Fq github-secret "$secret_check"
done
if grep -R -Fq github-secret "$test_root/state"; then
  exit 1
else
  secret_status=$?
fi
[ "$secret_status" -eq 1 ]
printf 'managed Exo host command tests: ok\n'
