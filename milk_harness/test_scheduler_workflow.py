import os
import re
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

from milk_harness.scheduler import (
    GATEWAY_DEPLOYMENT_FIELDS,
    PROVIDER_RUNTIME_FIELDS,
    PROVIDER_LEASE_MAX_CLOCK_SKEW_SECONDS,
    PROVIDER_LEASE_TTL_SECONDS,
)


ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github/workflows/production-loop.yml"
OFFLINE_WORKFLOW = ROOT / ".github/workflows/offline-gates.yml"


class SchedulerWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")
        cls.gateway, remainder = cls.text.split("\n  provider-jobs:", 1)
        cls.provider, cls.route = remainder.split("\n  route-control:", 1)
        run_start = cls.provider.index("started_at=$(date")
        run_end = cls.provider.index("          status=$?", run_start)
        cls.provider_container = cls.provider[run_start:run_end]
        ingest_start = cls.provider.index('if [ "$status" -eq 0 ]; then', run_end)
        ingest_end = cls.provider.index("completed_at=$(date", ingest_start)
        cls.gateway_ingest = cls.provider[ingest_start:ingest_end]

    def test_there_is_exactly_one_fixed_non_overlapping_production_workflow(self):
        workflows = sorted((ROOT / ".github/workflows").glob("*"))
        self.assertEqual(workflows, [OFFLINE_WORKFLOW, WORKFLOW])
        offline = OFFLINE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("schedule:", offline)
        self.assertIn("group: milk-harness-offline-${{ github.ref }}", offline)
        self.assertIn('cron: "2/5 * * * *"', self.text)
        self.assertIn("group: milk-production-loop-v1", self.text)
        self.assertIn("cancel-in-progress: false", self.text)
        self.assertIn("queue: max", self.text)
        self.assertEqual(
            re.findall(
                r"^  ([a-z][a-z0-9-]+):$",
                self.text.split("\njobs:\n", 1)[1],
                flags=re.MULTILINE,
            ),
            ["gateway-tick", "provider-jobs", "route-control"],
        )

    def test_cron_and_manual_false_reconcile_while_manual_create_is_confirmed(self):
        workflow_header, workflow_jobs = self.text.split("\njobs:\n", 1)
        self.assertIn("managed_request_id:", workflow_header)
        self.assertIn(
            "format('Milk production loop [managed:{0}]', inputs.managed_request_id)",
            workflow_header,
        )
        self.assertNotIn("managed_request_id", workflow_jobs)
        self.assertIn("github.event_name == 'schedule'", self.gateway)
        self.assertIn("github.event_name == 'workflow_dispatch'", self.gateway)
        self.assertIn("inputs.authorize_provider_creates == true", self.gateway)
        self.assertIn(
            "inputs.confirmed_run_config_sha256 == vars.MILK_EVAL_ID",
            self.gateway,
        )
        gateway_condition = self.gateway.split("    runs-on:", 1)[0]
        self.assertNotIn("inputs.authorize_provider_creates", gateway_condition)
        self.assertIn(
            "github.event_name == 'workflow_dispatch' && inputs.authorize_provider_creates == true && inputs.confirmed_run_config_sha256 || ''",
            self.gateway,
        )
        self.assertIn("needs: gateway-tick", self.provider)
        provider_condition = self.provider.split("    runs-on:", 1)[0]
        self.assertIn("always()", provider_condition)
        self.assertIn(
            "inputs.confirmed_run_config_sha256 == vars.MILK_EVAL_ID",
            provider_condition,
        )
        self.assertNotIn("inputs.authorize_provider_creates", provider_condition)
        self.assertIn("create-gate", self.provider)
        self.assertIn("validate-run-config", self.gateway)
        self.assertIn("validate-run-config", self.provider)
        self.assertIn("MILK_CONFIRMED_RUN_CONFIG_PATH", self.gateway)
        self.assertIn("MILK_CONFIRMED_RUN_CONFIG_PATH", self.provider)
        self.assertIn(
            "MILK_PROVIDER_CREATE_REQUESTED: ${{ github.event_name == 'workflow_dispatch' && inputs.authorize_provider_creates == true }}",
            self.gateway,
        )
        self.assertIn('--requested "$MILK_PROVIDER_CREATE_REQUESTED"', self.gateway)
        self.assertIn(
            '--confirmed-sha256 "$MILK_CONFIRMED_RUN_CONFIG_SHA256"',
            self.gateway,
        )
        self.assertIn('--expected-sha256 "$MILK_EVAL_ID"', self.gateway)
        self.assertIn('--expected-sha256 "$MILK_EVAL_ID"', self.provider)
        self.assertIn('if [ "$create_granted" = true ]; then', self.provider)
        self.assertNotIn("--allow-provider-creates", self.provider)
        self.assertLess(
            self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"'),
            self.provider.index("milk_harness.scheduler archive"),
        )
        for argument in (
            "--jobs-image",
            "--student-train-image",
            "--student-branch-image",
            "--teacher-image",
            "--image-release-sha256",
            "--provider-create-requested",
            "--provider-confirmation-valid",
            "--provider-configuration-verified",
            "--provider-create-granted",
        ):
            self.assertIn(argument, self.provider)
        self.assertNotIn("--image-release-evidence-dir", self.text)

    def test_campaign_lease_guards_every_provider_pass(self):
        acquire = self.provider.index("milk_harness.scheduler lease-acquire")
        run_jobs = self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"')
        archive = self.provider.index("milk_harness.scheduler archive")
        release = self.provider.rindex("milk_harness.scheduler lease-release")
        self.assertLess(acquire, run_jobs)
        self.assertLess(run_jobs, archive)
        self.assertLess(archive, release)
        self.assertIn('timeout-minutes: 12', self.provider)
        self.assertIn('--owner-id "$pass_id"', self.provider)
        self.assertIn('--campaign-id "$MILK_CAMPAIGN_ID"', self.provider)
        self.assertIn('--scope-prefix "$MILK_SCOPE_PREFIX"', self.provider)
        self.assertIn('--create-gate "$create_gate"', self.provider)
        self.assertIn(
            '--configuration-verified "$configuration_verified"', self.provider
        )
        self.assertIn('--lease-token "$lease_container_token"', self.provider)
        self.assertIn('--lease-token "$lease_token"', self.provider)
        self.assertIn(
            '--mount "type=bind,src=$lease_token,dst=$lease_container_token,readonly"',
            self.provider,
        )
        self.assertIn("MILK_CREATE_AUTHORITY_WRITE_R2_ACCESS_KEY_ID", self.provider)
        self.assertIn("MILK_CREATE_AUTHORITY_READ_R2_ACCESS_KEY_ID", self.provider)
        self.assertNotIn("-e MILK_CREATE_AUTHORITY_WRITE_R2_", self.provider)
        self.assertIn("create_authority_env=()", self.provider)
        self.assertIn('if [ "$create_granted" = true ]; then', self.provider)
        self.assertIn("unset \\\n              MILK_CREATE_AUTHORITY_WRITE_R2_ACCOUNT_ID", self.provider)
        self.assertIn('"${create_authority_env[@]}"', self.provider)
        for line in self.provider.splitlines():
            if not re.match(r"^      MILK_CREATE_AUTHORITY_", line):
                continue
            self.assertIn("github.event_name == 'workflow_dispatch'", line)
            self.assertIn("inputs.authorize_provider_creates == true", line)
            self.assertIn("needs.gateway-tick.outputs.accepted == 'true'", line)
            self.assertIn("|| ''", line)
            self.assertNotRegex(line, r": \$\{\{ secrets\.")
        self.assertGreaterEqual(
            PROVIDER_LEASE_TTL_SECONDS - 12 * 60,
            PROVIDER_LEASE_MAX_CLOCK_SKEW_SECONDS,
        )

    def test_job_identities_do_not_cross_credential_boundaries(self):
        self.assertIn("environment: milk-gateway-tick-prod", self.gateway)
        for forbidden in (
            "BASETEN_API_KEY",
            "MILK_EVIDENCE_R2_",
            "MILK_OPS_LOG_R2_",
            "MILK_ROUTE_STORE_",
            "MODAL_",
            "SIGNING",
        ):
            self.assertNotIn(forbidden, self.gateway)
        self.assertIn("environment: milk-provider-jobs-prod", self.provider)
        for forbidden in ("MILK_CAPTURE_STORE_", "SIGNING"):
            self.assertNotIn(forbidden, self.provider)
        for forbidden in (
            "MILK_GATEWAY_INGEST_",
            "MILK_CONTROL_STORE_",
            "MILK_ROUTE_STORE_",
            "SIGNING",
        ):
            self.assertNotIn(forbidden, self.provider_container)
        self.assertIn('"${provider_credential_env[@]}"', self.provider_container)
        for forbidden in ("BASETEN_API_KEY", "MODAL_TOKEN_", "MILK_CONTROL_R2_"):
            self.assertNotIn(forbidden, self.gateway_ingest)
        self.assertIn("MILK_CONTROL_STORE_ACCESS_KEY_ID", self.gateway_ingest)
        self.assertIn("MILK_ROUTE_STORE_ACCESS_KEY_ID", self.gateway_ingest)
        self.assertNotIn("write", self.text.split("concurrency:", 1)[0])
        self.assertIn("persist-credentials: false", self.gateway)
        self.assertIn("persist-credentials: false", self.provider)

    def test_empty_r2_session_tokens_are_not_passed_to_containers(self):
        cases = (
            (
                self.gateway,
                "gateway_store_env",
                "MILK_CAPTURE_STORE_SESSION_TOKEN",
            ),
            (
                self.gateway,
                "gateway_store_env",
                "MILK_CONTROL_STORE_SESSION_TOKEN",
            ),
            (
                self.provider,
                "provider_store_env",
                "MILK_CONTROL_R2_SESSION_TOKEN",
            ),
            (
                self.provider,
                "provider_store_env",
                "MILK_EVIDENCE_R2_SESSION_TOKEN",
            ),
            (
                self.provider,
                "create_authority_env",
                "MILK_CREATE_AUTHORITY_READ_R2_SESSION_TOKEN",
            ),
        )
        for section, array, name in cases:
            with self.subTest(name=name):
                self.assertRegex(
                    section,
                    rf'\[ -z "\${name}" \] \|\| \\\n\s+{array}\+=\(-e {name}\)',
                )
                self.assertNotRegex(section, rf"(?m)^\s+-e {name} \\$")
                self.assertIn(f'"${{{array}[@]}}"', section)

        for source, target, array in (
            (
                "MILK_GATEWAY_INGEST_CONTROL_R2_SESSION_TOKEN",
                "MILK_CONTROL_STORE_SESSION_TOKEN",
                "gateway_ingest_control_env",
            ),
            (
                "MILK_GATEWAY_INGEST_ROUTE_R2_SESSION_TOKEN",
                "MILK_ROUTE_STORE_SESSION_TOKEN",
                "gateway_ingest_route_env",
            ),
        ):
            with self.subTest(name=target):
                self.assertIn(f'if [ -n "${source}" ]; then', self.gateway_ingest)
                self.assertIn(
                    f'{array}+=(-e {target})',
                    self.gateway_ingest,
                )
                self.assertIn(f"unset {target}", self.gateway_ingest)
                self.assertNotRegex(
                    self.gateway_ingest,
                    rf"(?m)^\s+-e {target} \\$",
                )
                self.assertIn(f'"${{{array}[@]}}"', self.gateway_ingest)

    def test_exact_cross_provider_runtime_is_bound_to_the_confirmed_config(self):
        for name in PROVIDER_RUNTIME_FIELDS:
            argument = f"--{name.replace('_', '-')}"
            self.assertGreaterEqual(self.provider.count(argument), 2, argument)
            if name == "gpu_provider":
                self.assertIn(
                    '["provider_runtime"]["gpu_provider"]',
                    self.provider,
                )
            else:
                self.assertIn(f"MILK_{name.upper()}", self.provider)
        self.assertIn(
            "BASETEN_API_KEY: ${{ secrets.BASETEN_API_KEY }}",
            self.provider,
        )
        self.assertIn(
            "MODAL_TOKEN_ID: ${{ secrets.MODAL_TOKEN_ID }}",
            self.provider,
        )
        self.assertIn(
            "MODAL_TOKEN_SECRET: ${{ secrets.MODAL_TOKEN_SECRET }}",
            self.provider,
        )
        self.assertIn('case "$confirmed_gpu_provider" in', self.provider)
        self.assertLess(
            self.provider.index('confirmed_gpu_provider=$(python3 -c'),
            self.provider.index('case "$confirmed_gpu_provider" in'),
        )
        self.assertIn('provider_credential_env=(-e BASETEN_API_KEY)', self.provider)
        self.assertIn(
            'provider_credential_env=(-e MODAL_TOKEN_ID -e MODAL_TOKEN_SECRET)',
            self.provider,
        )
        self.assertIn('[ -n "$MODAL_TOKEN_ID" ] || fail', self.provider)
        self.assertIn('[ -n "$MODAL_TOKEN_SECRET" ] || fail', self.provider)

    def test_gateway_handoffs_are_ingested_before_summary_and_archive(self):
        run_jobs = self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"')
        stage = self.provider.index("milk_harness.scheduler stage-gateway-ingest")
        validations = [
            match.start()
            for match in re.finditer("--phase gateway_ingest", self.provider)
        ]
        self.assertEqual(len(validations), 2)
        pre_spend_validate, validate = validations
        create_gate = self.provider.index("milk_harness.scheduler create-gate")
        winner = self.provider.index("ingest-student-winner-deployment-result")
        teardown = self.provider.index("ingest-provider-teardown-result")
        summarize = self.provider.index("milk_harness.scheduler summarize")
        archive = self.provider.index("milk_harness.scheduler archive")
        self.assertLess(pre_spend_validate, create_gate)
        self.assertLess(create_gate, run_jobs)
        self.assertLess(run_jobs, stage)
        self.assertLess(stage, validate)
        self.assertLess(validate, winner)
        self.assertLess(validate, teardown)
        self.assertLess(winner, summarize)
        self.assertLess(teardown, summarize)
        self.assertLess(summarize, archive)
        self.assertIn('if [ "$status" -eq 0 ]; then', self.gateway_ingest)
        self.assertIn('status=$ingest_status', self.gateway_ingest)
        self.assertIn("gateway result handoff failed", self.gateway_ingest)
        self.assertIn('chmod 0600 "$result"', self.gateway_ingest)
        self.assertIn('sudo chown 65532:65532 "$result"', self.gateway_ingest)

        winner_run, teardown_run = self.gateway_ingest.split(
            'if [ "$kind" = winner ]; then', 1
        )[1].split("\n                else\n", 1)
        teardown_run = teardown_run.split("\n                fi\n", 1)[0]
        self.assertNotIn("MILK_ROUTE_STORE_", winner_run)
        self.assertIn("-e MILK_ROUTE_STORE_ACCESS_KEY_ID", self.gateway_ingest)
        self.assertIn('"${gateway_ingest_route_env[@]}"', teardown_run)
        self.assertIn('>"$gateway_ingest_stdout" 2>"$gateway_ingest_stderr"', winner_run)
        self.assertIn(
            '>"$gateway_ingest_stdout" 2>"$gateway_ingest_stderr"', teardown_run
        )
        self.assertIn(': >"$gateway_ingest_stdout"', self.gateway_ingest)
        self.assertIn(': >"$gateway_ingest_stderr"', self.gateway_ingest)
        self.assertEqual(
            self.gateway_ingest.count(
                "timeout --signal=TERM --kill-after=10s 90s"
            ),
            2,
        )

    def test_baseten_candidate_key_uses_the_exact_bounded_gateway_helper(self):
        helper_checkout = self.provider.index("Read exact gateway credential helper")
        helper_start = self.provider.index("serve-baseten")
        jobs_start = self.provider.index('started_at=$(date')
        jobs_run = self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"')
        self.assertLess(helper_checkout, helper_start)
        self.assertLess(helper_start, jobs_start)
        self.assertLess(jobs_start, jobs_run)
        self.assertIn("repository: milkinfrastructure/milk-gateway", self.provider)
        self.assertIn("ref: ${{ steps.eval.outputs.gateway_source_commit }}", self.provider)
        self.assertIn("token: ${{ secrets.MILK_GATEWAY_SOURCE_READ_TOKEN }}", self.provider)
        self.assertGreaterEqual(self.provider.count("persist-credentials: false"), 2)
        self.assertIn('gateway_image_commit=$(docker --config "$docker_config" image inspect', self.provider)
        self.assertIn('"org.opencontainers.image.revision"', self.provider)
        self.assertIn('[ "$gateway_image_commit" = "$MILK_GATEWAY_SOURCE_COMMIT" ]', self.provider)
        self.assertIn("npm ci --ignore-scripts --no-audit --no-fund", self.provider)
        self.assertIn('deploy/cloudflare/node_modules/.bin', self.provider)
        self.assertNotIn("npm install", self.provider)
        self.assertNotIn("npx ", self.provider)
        job_environment = self.provider.split("\n    steps:", 1)[0]
        self.assertNotIn("CLOUDFLARE_CANDIDATE_SECRET_API_TOKEN", job_environment)
        self.assertNotIn("MILK_GATEWAY_CONTAINER_ADMIN_KEY", job_environment)

        self.assertIn('candidate_socket_dir=$(mktemp -d "/tmp/milk-candidate-runtime.XXXXXX")', self.provider)
        self.assertIn('sudo chown 65532:65532 "$candidate_socket_dir"', self.provider)
        self.assertIn("sudo -n -u '#65532' -- env -i", self.provider)
        self.assertIn("sh -c 'exec 3<&0; exec \"$@\"'", self.provider)
        self.assertIn("--admin-key-fd 3", self.provider)
        self.assertIn('--socket-path "$candidate_socket"', self.provider)
        self.assertIn("timeout --signal=TERM --kill-after=10s 600s", self.provider)
        self.assertIn('unset MILK_GATEWAY_CONTAINER_ADMIN_KEY', self.provider)
        self.assertIn('unset candidate_admin_key CLOUDFLARE_CANDIDATE_SECRET_API_TOKEN', self.provider)
        self.assertIn('[ -S "$candidate_socket" ]', self.provider)
        self.assertGreaterEqual(self.provider.count('[ -S "$candidate_socket" ]'), 2)
        self.assertIn('wait "$candidate_helper_pid" || candidate_helper_status=$?', self.provider)
        self.assertIn('--candidate-key-socket "$candidate_container_socket"', self.provider)
        self.assertIn('--mount "type=bind,src=$candidate_socket,dst=$candidate_container_socket"', self.provider_container)
        self.assertNotIn("--user", self.provider_container)
        for forbidden in (
            "-e CLOUDFLARE_",
            "-e MILK_GATEWAY_CONTAINER_ADMIN_KEY",
            "-e MILK_GATEWAY_SOURCE_READ_TOKEN",
        ):
            self.assertNotIn(forbidden, self.provider_container)

    def test_provider_emits_only_a_sanitized_route_trigger(self):
        summarize = self.provider.index("milk_harness.scheduler summarize")
        route_output = self.provider.index("route_required=%s")
        archive = self.provider.index("milk_harness.scheduler archive")
        self.assertLess(summarize, route_output)
        self.assertLess(route_output, archive)
        self.assertIn("winner_result_count", self.provider)
        self.assertIn("candidate_key_delivery_count", self.provider)
        self.assertIn("winner_result_references", self.provider)
        self.assertIn("route_candidate_ready=%s", self.provider)
        self.assertIn("route_student_job_id=%s", self.provider)
        self.assertIn("route trigger identity is invalid", self.provider)
        self.assertIn(
            "route_required: ${{ steps.provider.outputs.route_required }}",
            self.provider,
        )
        self.assertIn(
            "route_candidate_ready: ${{ steps.provider.outputs.route_candidate_ready }}",
            self.provider,
        )
        self.assertIn(
            "route_student_job_id: ${{ steps.provider.outputs.route_student_job_id }}",
            self.provider,
        )

    def test_route_control_is_one_restart_safe_paid_canary_then_zero(self):
        self.assertIn("needs: provider-jobs", self.route)
        self.assertIn("environment: milk-route-control-prod", self.route)
        self.assertIn("needs.provider-jobs.outputs.route_required == 'true'", self.route)
        self.assertIn("inputs.authorize_provider_creates == true", self.route)
        route_condition = self.route.split("    runs-on:", 1)[0]
        self.assertNotIn("route_candidate_ready", route_condition)
        self.assertNotIn("inputs.authorize_provider_creates", route_condition)
        self.assertIn(
            "inputs.confirmed_run_config_sha256 == vars.MILK_EVAL_ID",
            route_condition,
        )
        self.assertIn(
            "MILK_ROUTE_PAID_PROOF_AUTHORIZED: ${{ github.event_name == 'workflow_dispatch' && inputs.authorize_provider_creates == true }}",
            self.route,
        )
        self.assertIn("repository: milkinfrastructure/milk-gateway", self.route)
        self.assertIn("ref: ${{ steps.eval.outputs.gateway_source_commit }}", self.route)
        self.assertIn("org.opencontainers.image.revision", self.route)
        self.assertIn("npm ci --ignore-scripts --no-audit --no-fund", self.route)
        self.assertIn('deploy/cloudflare/node_modules/.bin', self.route)
        self.assertIn("openai-production-smoke.mjs", self.route)
        self.assertIn("https://api.dragontales.milkinfrastructure.com/v1", self.route)
        self.assertIn("milk.official-openai-sdk-route-smoke.v1", self.route)
        self.assertIn("milk.official-openai-sdk-route-smoke-intent.v1", self.route)
        self.assertIn("milk.official-openai-sdk-saturation-fallback-smoke.v1", self.route)
        self.assertIn(
            "milk.official-openai-sdk-saturation-fallback-smoke-intent.v1",
            self.route,
        )
        self.assertIn("--saturation-fallback", self.route)
        self.assertIn('value.get("fallback_route_target") == "openai"', self.route)
        self.assertIn("create_same", self.route)
        self.assertIn("except FileNotFoundError:", self.route)
        self.assertNotIn("except urllib.error.HTTPError", self.route)
        self.assertIn("advance-winner-route", self.route)
        self.assertIn("advance_phase canary", self.route)
        self.assertIn("advance_phase zero", self.route)
        self.assertIn("dragontales-publish-route.sh", self.route)
        self.assertIn("DRAGONTALES_ROUTE_SECRET_HEX", self.route)
        self.assertIn('manifest.get("candidate_basis_points") != 100', self.route)
        self.assertIn('config["route"].get("candidate_max_in_flight") != 1', self.route)
        self.assertIn("smoke traffic key is outside the one-percent canary", self.route)
        self.assertGreaterEqual(self.route.count('"capture_allowed": False'), 2)
        self.assertIn('remaining=$(seconds_until "$canary_not_after")', self.route)
        self.assertNotIn('while [ "$remaining" -gt 0 ]', self.route)
        self.assertIn("emergency zero publication failed", self.route)
        self.assertIn("zero_published=true", self.route)
        self.assertNotIn("prepare-route", self.route)
        self.assertNotIn("500", self.route)
        self.assertNotIn("fallback injection", self.route)
        for forbidden in (
            "BASETEN_API_KEY",
            "MODAL_TOKEN_",
            "MILK_CREATE_AUTHORITY_",
        ):
            self.assertNotIn(forbidden, self.route)

    def test_modal_candidate_credential_is_gateway_owned_and_pipe_only(self):
        route_job_environment = self.route.split("\n    steps:", 1)[0]
        self.assertNotIn("MILK_MODAL_CANDIDATE_API_KEY", route_job_environment)
        self.assertNotIn("MILK_GATEWAY_CONTAINER_ADMIN_KEY", route_job_environment)
        self.assertNotIn("CLOUDFLARE_CANDIDATE_SECRET_API_TOKEN", route_job_environment)
        self.assertIn(
            "MILK_MODAL_CANDIDATE_API_KEY: ${{ github.event_name == 'workflow_dispatch' && inputs.authorize_provider_creates == true",
            self.route,
        )
        self.assertIn("prepare-modal-candidate-credential", self.route)
        self.assertIn("ingest-modal-candidate-credential-ack", self.route)
        self.assertNotIn("--recover-install", self.route)
        self.assertIn("dragontales.modal-candidate-credential-preparation.v1", self.route)
        self.assertIn("dragontales.modal-candidate-credential-ack-write.v1", self.route)
        self.assertIn('reconcile_candidate ready', self.route)
        self.assertIn('reconcile_candidate absent', self.route)
        self.assertLess(
            self.route.rindex('advance_phase canary'),
            self.route.rindex('reconcile_candidate ready'),
        )
        self.assertIn('[ "$zero_live" = true ] && [ "$candidate_absent" != true ]', self.route)
        self.assertIn('[ "$MILK_ROUTE_CANDIDATE_READY" = true ]', self.route)
        self.assertIn('[ "$prepared_provider" = baseten ]', self.route)
        self.assertIn('"${action}-modal"', self.route)
        self.assertIn("--admin-key-fd 3", self.route)
        self.assertIn("--candidate-key-fd 4", self.route)
        self.assertIn("3< <(printf '%s' \"$gateway_admin_key\")", self.route)
        self.assertIn("4< <(printf '%s' \"$modal_candidate_key\")", self.route)
        helper = self.route.split("run_candidate_helper() {", 1)[1].split(
            "\n          reconcile_candidate() {", 1
        )[0]
        self.assertIn("env -i", helper)
        self.assertNotIn("MILK_GATEWAY_CONTAINER_ADMIN_KEY=", helper)
        self.assertNotIn("MILK_MODAL_CANDIDATE_API_KEY=", helper)
        self.assertIn(': >"$helper_stderr"', helper)
        self.assertIn(': >"$ack"', helper)
        self.assertIn(': >"$ingest_stdout"', helper)
        for field in (
            "MILK_GATEWAY_IMAGE_ADMISSION_SHA256",
            "MILK_GATEWAY_RELEASE_SHA256",
            "MILK_GATEWAY_CONTAINER_APPLICATION_ID",
            "MILK_GATEWAY_CONTAINER_APPLICATION_VERSION",
            "MILK_GATEWAY_CONTAINER_IMAGE",
            "MILK_GATEWAY_WORKER_VERSION_ID",
        ):
            self.assertIn(field, self.route)

    def test_gateway_deployment_is_bound_before_provider_or_route_mutation(self):
        arguments = {
            "source_commit": "--gateway-source-commit",
            "image_admission_sha256": "--gateway-image-admission-sha256",
            "release_sha256": "--gateway-release-sha256",
            "application_id": "--gateway-container-application-id",
            "application_version": "--gateway-container-application-version",
            "container_image": "--gateway-container-image",
            "worker_version_id": "--gateway-worker-version-id",
        }
        self.assertEqual(set(arguments), set(GATEWAY_DEPLOYMENT_FIELDS))
        for argument in arguments.values():
            self.assertIn(argument, self.provider)
            self.assertIn(argument, self.route)
        provider_validate = self.provider.index("--phase gateway_anchor")
        self.assertLess(provider_validate, self.provider.index("serve-baseten"))
        provider_base_validate = self.provider.index("--phase provider_base")
        self.assertLess(
            provider_base_validate,
            self.provider.index("milk_harness.scheduler create-gate"),
        )
        validate = self.route.index("--phase route")
        self.assertLess(validate, self.route.index("advance_phase canary"))
        self.assertLess(validate, self.route.index("prepare-modal-candidate-credential"))
        self.assertIn('--gateway-anchor-output "$gateway_anchor"', self.route)

    def test_reconcile_restart_recovers_expired_or_live_canary_without_create(self):
        self.assertIn(
            '[ "$MILK_ROUTE_PAID_PROOF_AUTHORIZED" != true ] || recovery_only=false',
            self.route,
        )
        start = self.route.index('if [ "$recovery_only" = true ]; then')
        end = self.route.index('\n          if [ "$canary_action" = done ]; then', start)
        branch = textwrap.dedent(self.route[start:end])
        self.assertNotIn("reconcile_candidate ready", branch)
        self.assertNotIn("publish_manifest", branch)
        self.assertIn("reconcile_candidate absent", branch)
        reconcile = self.route.split("reconcile_candidate() {", 1)[1].split(
            "\n          advance_phase() {", 1
        )[0]
        self.assertIn('[[ "$candidate_action" =~ ^(remove|verify)$ ]]', reconcile)

        for action, expected in (
            ("prepare", "ingest-absent-ack\n"),
            ("observe", "signed-zero\ningest-absent-ack\n"),
        ):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                events = Path(directory) / "events"
                output = Path(directory) / "output"
                script = textwrap.dedent(
                    f"""
                    force_zero() {{
                      printf 'signed-zero\\n' >>"$events"
                      zero_live=true
                    }}
                    reconcile_candidate() {{
                      [ "$1" = absent ] || return 70
                      printf 'ingest-absent-ack\\n' >>"$events"
                      candidate_absent=true
                    }}
                    fail() {{ return "${{2:-64}}"; }}
                    events=$1
                    GITHUB_OUTPUT=$2
                    recovery_only=true
                    canary_action={action}
                    canary_revision={'a' * 64}
                    zero_needed=false
                    zero_live=false
                    candidate_absent=false
                    {branch}
                    """
                )
                result = subprocess.run(
                    ["bash", "-s", "--", str(events), str(output)],
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(events.read_text(encoding="utf-8"), expected)

    def test_modal_failure_after_ready_forces_zero_then_absence(self):
        cleanup = self.route.split("\n          cleanup() {", 1)[1].split(
            "\n          trap cleanup EXIT", 1
        )[0]
        self.assertLess(cleanup.index("force_zero"), cleanup.index("reconcile_candidate absent"))
        ready = self.route.rindex("reconcile_candidate ready")
        self.assertLess(self.route.rindex("zero_needed=true", 0, ready), ready)
        self.assertLess(ready, self.route.index("proof_evidence candidate check", ready))
        done = self.route.index('\n          if [ "$canary_action" = done ]; then')
        done_end = self.route.index("\n          fi", done)
        done_branch = self.route[done:done_end]
        self.assertLess(done_branch.index("zero_live=true"), done_branch.index("reconcile_candidate absent"))
        self.assertLess(done_branch.index("reconcile_candidate absent"), done_branch.index("proof_evidence candidate require-any"))

        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events"
            script = "cleanup() {" + textwrap.dedent(cleanup) + textwrap.dedent(
                """
                force_zero() {
                  printf 'signed-zero\n' >>"$events"
                  zero_live=true
                }
                reconcile_candidate() {
                  [ "$1" = absent ] && [ "$zero_live" = true ] || return 70
                  printf 'ingest-absent-ack\n' >>"$events"
                  candidate_absent=true
                }
                docker() { :; }
                sudo() { :; }
                events=$1
                zero_needed=true
                zero_live=false
                candidate_absent=false
                candidate_provider=modal
                gateway_container_id=
                docker_config=unused
                scratch="$RUNNER_TEMP/milk-route-control.injected"
                modal_candidate_key=redacted
                gateway_admin_key=redacted
                cloudflare_candidate_token=redacted
                false
                cleanup
                """
            )
            environment = dict(os.environ, RUNNER_TEMP=directory)
            result = subprocess.run(
                ["bash", "-s", "--", str(events)],
                input=script,
                text=True,
                capture_output=True,
                env=environment,
                check=False,
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(
                events.read_text(encoding="utf-8"),
                "signed-zero\ningest-absent-ack\n",
            )

    def test_only_sanitized_summaries_cross_jobs_or_reach_ops_r2(self):
        self.assertIn('>"$stdout" 2>"$stderr"', self.gateway)
        self.assertIn('>"$stdout" 2>"$stderr"', self.provider)
        self.assertIn("--gateway-summary-b64", self.provider)
        self.assertIn("MILK_OPS_LOG_R2_", self.provider)
        for forbidden in (
            "upload-artifact",
            "actions/cache",
            "env |",
            "printenv",
            "set -x",
            ":latest",
        ):
            self.assertNotIn(forbidden, self.text)

    def test_one_eval_document_replaces_nonsecret_variable_and_json_secret_bundles(self):
        variables = set(re.findall(r"vars\.([A-Z0-9_]+)", self.text))
        secrets = set(re.findall(r"secrets\.([A-Z0-9_]+)", self.text))
        self.assertEqual(variables, {"MILK_EVAL_ID", "MILK_EVAL_CONFIG_JSON"})
        self.assertEqual(len(secrets), 48)
        for removed in (
            "MILK_GATEWAY_TICK_CONFIG_JSON",
            "MILK_GATEWAY_INGEST_CONFIG_JSON",
            "MILK_ROUTE_GATEWAY_CONFIG_JSON",
            "MILK_CONFIRMED_RUN_CONFIG_JSON",
        ):
            self.assertNotIn(removed, self.text)
        self.assertNotRegex(
            self.text,
            r"secrets\.MILK_[A-Z0-9_]+_R2_(?:ACCOUNT_ID|BUCKET)",
        )
        self.assertEqual(self.text.count("python3 -m milk_harness.eval_config"), 3)
        self.assertIn("group: milk-production-loop-v1-${{ vars.MILK_EVAL_ID }}", self.text)
        self.assertIn(
            '--confirmed-sha256 "$MILK_CONFIRMED_RUN_CONFIG_SHA256"',
            self.provider,
        )
        self.assertIn(
            '--expected-confirmation-sha256 "$MILK_EVAL_ID"',
            self.provider,
        )

    def test_embedded_shell_is_valid(self):
        lines = self.text.splitlines()
        scripts = []
        for index, line in enumerate(lines):
            if line.strip() != "run: |":
                continue
            indent = len(line) - len(line.lstrip())
            body = []
            for candidate in lines[index + 1 :]:
                if candidate.strip() and len(candidate) - len(candidate.lstrip()) <= indent:
                    break
                body.append(candidate)
            scripts.append(textwrap.dedent("\n".join(body)))
        self.assertEqual(len(scripts), 6)
        for script in scripts:
            result = subprocess.run(
                ["bash", "-n"], input=script, text=True, capture_output=True, check=False
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
