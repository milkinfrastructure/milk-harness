#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
command=$root/tools/exo/milk-managed
installer=$root/tools/exo/install-host-command
test_root=$(mktemp -d "${TMPDIR:-/tmp}/milk-managed-test.XXXXXX")

cleanup() {
  case "$test_root" in
    "${TMPDIR:-/tmp}"/milk-managed-test.*) rm -rf -- "$test_root" ;;
  esac
}
trap cleanup EXIT HUP INT TERM
mkdir -m 0700 "$test_root/bin" "$test_root/state" "$test_root/approval"

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

sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
approval=$test_root/approval/run-confirmed.approval
state=$test_root/state/managed-dispatch.state
dispatch_log=$test_root/dispatch.log
list_log=$test_root/list.log
view_log=$test_root/view.log

run() {
  PATH="$test_root/bin:$PATH" \
  MILK_CONFIRMED_RUN_CONFIG_SHA256="${TEST_SHA:-$sha}" \
  MILK_MANAGED_APPROVAL_FILE="$approval" \
  MILK_MANAGED_STATE_FILE="$state" \
  TEST_GH_DISPATCH_LOG="$dispatch_log" \
  TEST_GH_LIST_LOG="$list_log" \
  TEST_GH_VIEW_LOG="$view_log" \
    "$command" "$@"
}

[ "$(run status)" = \
  '{"ok":true,"state":"idle","changed":false,"approval_required":false}' ]

first_output=$test_root/first-output
TEST_GH_WORKFLOW_SLEEP=1 TEST_GH_DATABASE_ID=77 run reconcile >"$first_output" &
first_pid=$!
lock_attempts=100
while [ ! -d "$state.lock" ] && [ "$lock_attempts" -gt 0 ]; do
  lock_attempts=$((lock_attempts - 1))
  sleep 0.01
done
[ -d "$state.lock" ]
[ "$(run reconcile)" = \
  '{"ok":false,"state":"blocked","changed":false,"approval_required":false}' ]
wait "$first_pid"
[ "$(cat "$first_output")" = \
  '{"ok":true,"state":"running","changed":true,"approval_required":false}' ]
[ ! -e "$state.lock" ]
rm -f "$state" "$dispatch_log" "$list_log" "$view_log"

[ "$(TEST_GH_DATABASE_ID=101 run reconcile)" = \
  '{"ok":true,"state":"running","changed":true,"approval_required":false}' ]
[ -f "$state" ] && [ ! -L "$state" ]
[ "$(stat -f '%Lp' "$state" 2>/dev/null || stat -c '%a' "$state")" = 600 ]
[ "$(sed -n '1p' "$state")" = milk-managed-state-v1 ]
[ "$(sed -n '2p' "$state")" = reconcile ]
request_id=$(sed -n '3p' "$state")
[ "${#request_id}" -eq 32 ]
case "$request_id" in *[!0-9a-f]*) exit 1 ;; esac
[ "$(sed -n '4p' "$state")" = 101 ]
[ "$(sed -n '5p' "$state")" = "$sha" ]
grep -Fxq 'production-loop.yml' "$dispatch_log"
grep -Fxq 'github.com/milkinfrastructure/milk-harness' "$dispatch_log"
grep -Fxq 'main' "$dispatch_log"
grep -Fxq 'authorize_provider_creates=false' "$dispatch_log"
grep -Fxq "confirmed_run_config_sha256=$sha" "$dispatch_log"
grep -Fxq "managed_request_id=$request_id" "$dispatch_log"
grep -Fxq -- '--all' "$list_log"
grep -Fxq -- '--branch' "$list_log"
grep -Fxq -- '--event' "$list_log"
grep -Fxq 'workflow_dispatch' "$list_log"

rm -f "$list_log" "$view_log"
[ "$(TEST_GH_OBSERVATION=queued: run status)" = \
  '{"ok":true,"state":"running","changed":false,"approval_required":false}' ]
[ "$(cat "$view_log")" = 101 ]
[ ! -e "$list_log" ]
[ "$(TEST_GH_OBSERVATION=completed:success run status)" = \
  '{"ok":true,"state":"complete","changed":false,"approval_required":false}' ]
[ "$(TEST_GH_OBSERVATION=completed:neutral run status)" = \
  '{"ok":false,"state":"failed","changed":false,"approval_required":false}' ]
[ "$(TEST_GH_OBSERVATION=completed:skipped run status)" = \
  '{"ok":false,"state":"failed","changed":false,"approval_required":false}' ]
[ "$(TEST_GH_OBSERVATION=completed:action_required run status)" = \
  '{"ok":true,"state":"blocked","changed":false,"approval_required":true}' ]
[ "$(TEST_GH_VIEW_FAIL=1 run status)" = \
  '{"ok":false,"state":"blocked","changed":false,"approval_required":false}' ]

rm -f "$approval" "$dispatch_log"
[ "$(TEST_GH_OBSERVATION=completed:success run run_confirmed)" = \
  '{"ok":true,"state":"waiting_for_confirmation","changed":false,"approval_required":true}' ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$sha" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success TEST_GH_DATABASE_ID=202 run run_confirmed)" = \
  '{"ok":true,"state":"running","changed":true,"approval_required":false}' ]
[ ! -e "$approval" ]
[ "$(sed -n '2p' "$state")" = run_confirmed ]
request_id=$(sed -n '3p' "$state")
[ "$(sed -n '4p' "$state")" = 202 ]
grep -Fxq 'authorize_provider_creates=true' "$dispatch_log"
grep -Fxq "confirmed_run_config_sha256=$sha" "$dispatch_log"
grep -Fxq "managed_request_id=$request_id" "$dispatch_log"

rm -f "$dispatch_log"
printf '%064d\n' 1 >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success run run_confirmed)" = \
  '{"ok":false,"state":"blocked","changed":true,"approval_required":true}' ]
[ ! -e "$approval" ]
[ ! -e "$dispatch_log" ]

printf '%s\n' "$sha" >"$approval"
chmod 0600 "$approval"
[ "$(TEST_GH_OBSERVATION=completed:success TEST_GH_WORKFLOW_FAIL=1 TEST_GH_LIST_FAIL=1 run run_confirmed)" = \
  '{"ok":false,"state":"blocked","changed":true,"approval_required":false}' ]
[ ! -e "$approval" ]
[ "$(sed -n '4p' "$state")" = pending ]

printf '%s\n' "$sha" >"$approval"
chmod 0600 "$approval"
rm -f "$dispatch_log"
[ "$(TEST_GH_LIST_FAIL=1 run run_confirmed)" = \
  '{"ok":false,"state":"blocked","changed":false,"approval_required":false}' ]
[ -e "$approval" ]
[ ! -e "$dispatch_log" ]

[ "$(TEST_GH_DATABASE_ID=303 TEST_GH_OBSERVATION=queued: run status)" = \
  '{"ok":true,"state":"running","changed":false,"approval_required":false}' ]
[ "$(sed -n '4p' "$state")" = 303 ]
[ -e "$approval" ]

rm -f "$state" "$approval"
[ "$(TEST_SHA=bad run reconcile)" = \
  '{"ok":false,"state":"blocked","changed":false,"approval_required":false}' ]

chmod 0755 "$test_root/state"
[ "$(run status)" = \
  '{"ok":false,"state":"blocked","changed":false,"approval_required":false}' ]
chmod 0700 "$test_root/state"

set +e
invalid_output=$(run unknown 2>/dev/null)
invalid_status=$?
set -e
[ "$invalid_status" -eq 64 ]
[ -z "$invalid_output" ]

set +e
invalid_output=$(run status extra 2>/dev/null)
invalid_status=$?
set -e
[ "$invalid_status" -eq 64 ]
[ -z "$invalid_output" ]

sh -n "$command" "$installer" "$0"
! grep -Eq '(^|[^[:alnum:]_])eval([^[:alnum:]_]|$)' "$command" "$installer"
grep -Fq '/opt/milk/bin/milk-managed' "$root/tools/exo/README.md"
grep -Fq 'chmod 0600' "$root/tools/exo/README.md"
printf 'managed Exo host command tests: ok\n'
