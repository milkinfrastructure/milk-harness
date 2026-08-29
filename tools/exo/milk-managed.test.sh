#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
command=$root/tools/exo/milk-managed
installer=$root/tools/exo/install-host-command
temporary_root=${TMPDIR:-/tmp}
temporary_root=${temporary_root%/}
temporary_root=$(CDPATH= cd -- "$temporary_root" && pwd -P)
test_root=$(mktemp -d "$temporary_root/milk-managed-test.XXXXXX")

cleanup() {
  case "$test_root" in
    "$temporary_root"/milk-managed-test.*) rm -rf -- "$test_root" ;;
  esac
}
trap cleanup EXIT HUP INT TERM
mkdir -m 0700 "$test_root/bin" "$test_root/state"
mkdir -m 0750 "$test_root/evals"

cat >"$test_root/bin/gh" <<'SH'
#!/bin/sh
set -eu
case "${1:-} ${2:-}" in
  'run list')
    : >"$TEST_GH_LIST_LOG"
    for argument do printf '%s\n' "$argument" >>"$TEST_GH_LIST_LOG"; done
    [ "${TEST_GH_LIST_FAIL:-0}" -eq 0 ] || exit 1
    [ -z "${TEST_GH_DATABASE_ID:-}" ] || printf '%s\n' "$TEST_GH_DATABASE_ID"
    ;;
  'run view')
    printf '%s\n' "${3:-}" >"$TEST_GH_VIEW_LOG"
    [ "${TEST_GH_VIEW_FAIL:-0}" -eq 0 ] || exit 1
    printf '%s\n' "${TEST_GH_OBSERVATION:-queued:}"
    ;;
  'workflow run')
    : >"$TEST_GH_DISPATCH_LOG"
    for argument do printf '%s\n' "$argument" >>"$TEST_GH_DISPATCH_LOG"; done
    [ "${TEST_GH_WORKFLOW_SLEEP:-0}" = 0 ] || sleep "$TEST_GH_WORKFLOW_SLEEP"
    [ "${TEST_GH_WORKFLOW_FAIL:-0}" -eq 0 ] || exit 1
    ;;
  *) exit 64 ;;
esac
SH
chmod 0700 "$test_root/bin/gh"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 -- "$1" | cut -d ' ' -f 1
  fi
}

admit_eval() {
  source_file=$1
  eval_hash=$(sha256_file "$source_file")
  mv -- "$source_file" "$test_root/evals/$eval_hash.json"
  chmod 0440 "$test_root/evals/$eval_hash.json"
  printf '%s\n' "$eval_hash"
}

printf '{"schema_version":"milk.eval.test.v1","name":"primary"}\n' >"$test_root/primary.json"
eval_id=$(admit_eval "$test_root/primary.json")
printf '{"schema_version":"milk.eval.test.v1","name":"secondary"}\n' >"$test_root/secondary.json"
second_eval_id=$(admit_eval "$test_root/secondary.json")

dispatch_log=$test_root/dispatch.log
list_log=$test_root/list.log
view_log=$test_root/view.log

run() {
  PATH="$test_root/bin:$PATH" \
  MILK_MANAGED_EVAL_DIR="$test_root/evals" \
  MILK_MANAGED_STATE_ROOT="$test_root/state" \
  MILK_MANAGED_EVAL_OWNER_UID="$(id -u)" \
  TEST_GH_DATABASE_ID="${TEST_GH_DATABASE_ID:-}" \
  TEST_GH_OBSERVATION="${TEST_GH_OBSERVATION:-queued:}" \
  TEST_GH_LIST_FAIL="${TEST_GH_LIST_FAIL:-0}" \
  TEST_GH_VIEW_FAIL="${TEST_GH_VIEW_FAIL:-0}" \
  TEST_GH_WORKFLOW_FAIL="${TEST_GH_WORKFLOW_FAIL:-0}" \
  TEST_GH_WORKFLOW_SLEEP="${TEST_GH_WORKFLOW_SLEEP:-0}" \
  TEST_GH_DISPATCH_LOG="$dispatch_log" \
  TEST_GH_LIST_LOG="$list_log" \
  TEST_GH_VIEW_LOG="$view_log" \
    "$command" "$@"
}

expected() {
  printf '{"ok":%s,"eval_id":"%s","state":"%s","dispatch_state":"%s","generation_done":false,"changed":%s,"approval_required":%s}' \
    "$1" "$2" "$3" "$4" "$5" "$6"
}

state_dir=$test_root/state/$eval_id
state=$state_dir/managed-dispatch.state
approval=$state_dir/run-confirmed.approval

[ "$(run status "$eval_id")" = \
  "$(expected true "$eval_id" idle idle false false)" ]
[ -d "$state_dir" ]

first_output=$test_root/first-output
TEST_GH_WORKFLOW_SLEEP=1 TEST_GH_DATABASE_ID=77 \
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

[ "$(TEST_GH_DATABASE_ID=101 run reconcile "$eval_id")" = \
  "$(expected true "$eval_id" running running true false)" ]
[ -f "$state" ] && [ ! -L "$state" ]
[ "$(stat -f '%Lp' "$state" 2>/dev/null || stat -c '%a' "$state")" = 600 ]
[ "$(sed -n '1p' "$state")" = milk-managed-state-v2 ]
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
grep -Fxq 'production-loop.yml' "$dispatch_log"
grep -Fxq 'github.com/milkinfrastructure/milk-harness' "$dispatch_log"
grep -Fxq 'main' "$dispatch_log"
grep -Fxq 'authorize_provider_creates=false' "$dispatch_log"
grep -Fxq "confirmed_run_config_sha256=$eval_id" "$dispatch_log"
grep -Fxq "managed_request_id=$request_id" "$dispatch_log"
grep -Fxq -- '--all' "$list_log"
grep -Fxq -- '--branch' "$list_log"
grep -Fxq -- '--event' "$list_log"
grep -Fxq 'workflow_dispatch' "$list_log"

rm -f "$list_log" "$view_log"
[ "$(TEST_GH_OBSERVATION=queued: run status "$eval_id")" = \
  "$(expected true "$eval_id" running running false false)" ]
[ "$(cat "$view_log")" = 101 ]
[ ! -e "$list_log" ]
[ "$(TEST_GH_OBSERVATION=completed:success run status "$eval_id")" = \
  "$(expected true "$eval_id" ready succeeded false false)" ]
[ "$(TEST_GH_OBSERVATION=completed:neutral run status "$eval_id")" = \
  "$(expected false "$eval_id" failed failed false false)" ]
[ "$(TEST_GH_OBSERVATION=completed:skipped run status "$eval_id")" = \
  "$(expected false "$eval_id" failed failed false false)" ]
[ "$(TEST_GH_OBSERVATION=completed:action_required run status "$eval_id")" = \
  "$(expected true "$eval_id" blocked action_required false true)" ]
[ "$(TEST_GH_VIEW_FAIL=1 run status "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]

rm -f "$approval" "$dispatch_log"
[ "$(TEST_GH_OBSERVATION=completed:success run run_confirmed "$eval_id")" = \
  "$(expected true "$eval_id" waiting_for_confirmation succeeded false true)" ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$eval_id" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success TEST_GH_DATABASE_ID=202 run run_confirmed "$eval_id")" = \
  "$(expected true "$eval_id" running running true false)" ]
[ ! -e "$approval" ]
[ "$(sed -n '2p' "$state")" = run_confirmed ]
request_id=$(sed -n '3p' "$state")
[ "$(sed -n '4p' "$state")" = 202 ]
grep -Fxq 'authorize_provider_creates=true' "$dispatch_log"
grep -Fxq "confirmed_run_config_sha256=$eval_id" "$dispatch_log"
grep -Fxq "managed_request_id=$request_id" "$dispatch_log"

rm -f "$dispatch_log"
printf '%s\n' "$second_eval_id" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked succeeded true true)" ]
[ ! -e "$approval" ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$eval_id" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success TEST_GH_WORKFLOW_FAIL=1 TEST_GH_LIST_FAIL=1 run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown true false)" ]
[ ! -e "$approval" ]
[ "$(sed -n '4p' "$state")" = pending ]

printf '%s\n' "$eval_id" >"$approval"
chmod 0600 "$approval"
rm -f "$dispatch_log"
[ "$(TEST_GH_LIST_FAIL=1 run run_confirmed "$eval_id")" = \
  "$(expected false "$eval_id" blocked unknown false false)" ]
[ -e "$approval" ]
[ ! -e "$dispatch_log" ]

[ "$(TEST_GH_DATABASE_ID=303 TEST_GH_OBSERVATION=queued: run status "$eval_id")" = \
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

printf '{"schema_version":"milk.eval.test.v1","name":"symlink"}\n' >"$test_root/symlink-source.json"
symlink_eval_id=$(sha256_file "$test_root/symlink-source.json")
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

sh -n "$command" "$installer" "$0"
grep -Fq 'install -d -o root -g "$service_gid" -m 0750 /etc/milk/evals' "$installer"
grep -Fq '/var/lib/milk /var/lib/milk/evals' "$installer"
grep -Fq '/opt/milk/bin/milk-managed' "$root/tools/exo/README.md"
grep -Fq 'sudo install -o root' "$root/tools/exo/README.md"
grep -Fq '"/etc/milk/evals/$EVAL_ID.json"' "$root/tools/exo/README.md"
grep -Fq 'generation_done' "$root/tools/exo/README.md"
printf 'managed Exo host command tests: ok\n'
