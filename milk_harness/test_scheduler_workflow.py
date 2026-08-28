import re
import subprocess
import textwrap
import unittest
from pathlib import Path

from milk_harness.scheduler import (
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
        self.assertEqual(
            re.findall(
                r"^  ([a-z][a-z0-9-]+):$",
                self.text.split("\njobs:\n", 1)[1],
                flags=re.MULTILINE,
            ),
            ["gateway-tick", "provider-jobs", "route-control"],
        )

    def test_cron_is_reconcile_only_and_manual_create_is_exactly_confirmed(self):
        self.assertIn("github.event_name == 'workflow_dispatch'", self.gateway)
        self.assertIn("inputs.authorize_provider_creates == true", self.gateway)
        self.assertIn(
            "inputs.confirmed_run_config_sha256 == vars.MILK_CONFIRMED_RUN_CONFIG_SHA256",
            self.gateway,
        )
        self.assertIn("needs: gateway-tick", self.provider)
        self.assertIn("if: ${{ always() }}", self.provider)
        self.assertIn("create-gate", self.provider)
        self.assertIn("validate-run-config", self.gateway)
        self.assertIn("validate-run-config", self.provider)
        self.assertIn("MILK_CONFIRMED_RUN_CONFIG_JSON", self.gateway)
        self.assertIn("MILK_CONFIRMED_RUN_CONFIG_JSON", self.provider)
        self.assertIn('if [ "$create_granted" = true ]; then', self.provider)
        self.assertNotIn("--allow-provider-creates", self.provider)
        self.assertLess(
            self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"'),
            self.provider.index("milk_harness.scheduler archive"),
        )
        for argument in (
            "--jobs-image",
            "--student-image",
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
        self.assertIn("-e MODAL_TOKEN_ID", self.provider_container)
        self.assertIn("-e MODAL_TOKEN_SECRET", self.provider_container)
        for forbidden in ("BASETEN_API_KEY", "MODAL_TOKEN_", "MILK_CONTROL_R2_"):
            self.assertNotIn(forbidden, self.gateway_ingest)
        self.assertIn("MILK_CONTROL_STORE_ACCESS_KEY_ID", self.gateway_ingest)
        self.assertIn("MILK_ROUTE_STORE_ACCESS_KEY_ID", self.gateway_ingest)
        self.assertNotIn("write", self.text.split("concurrency:", 1)[0])
        self.assertIn("persist-credentials: false", self.gateway)
        self.assertIn("persist-credentials: false", self.provider)

    def test_exact_cross_provider_runtime_is_bound_to_the_confirmed_config(self):
        for name in PROVIDER_RUNTIME_FIELDS:
            argument = f"--{name.replace('_', '-')}"
            environment = f"MILK_{name.upper()}"
            self.assertGreaterEqual(self.provider.count(argument), 2, argument)
            self.assertIn(environment, self.provider)
        self.assertIn("MODAL_TOKEN_ID: ${{ secrets.MODAL_TOKEN_ID }}", self.provider)
        self.assertIn(
            "MODAL_TOKEN_SECRET: ${{ secrets.MODAL_TOKEN_SECRET }}", self.provider
        )
        self.assertIn('[ -n "$MODAL_TOKEN_ID" ] || fail', self.provider)
        self.assertIn('[ -n "$MODAL_TOKEN_SECRET" ] || fail', self.provider)

    def test_gateway_handoffs_are_ingested_before_summary_and_archive(self):
        run_jobs = self.provider.index('"$MILK_JOBS_IMAGE" "${job_arguments[@]}"')
        stage = self.provider.index("milk_harness.scheduler stage-gateway-ingest")
        validate = self.provider.index("--phase gateway_ingest")
        winner = self.provider.index("ingest-student-winner-deployment-result")
        teardown = self.provider.index("ingest-provider-teardown-result")
        summarize = self.provider.index("milk_harness.scheduler summarize")
        archive = self.provider.index("milk_harness.scheduler archive")
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
        self.assertIn("MILK_ROUTE_STORE_ACCESS_KEY_ID", teardown_run)
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
        self.assertIn("ref: ${{ vars.MILK_GATEWAY_SOURCE_COMMIT }}", self.provider)
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
        self.assertIn("candidate credential proof is unavailable", self.route)
        self.assertIn("repository: milkinfrastructure/milk-gateway", self.route)
        self.assertIn("ref: ${{ vars.MILK_GATEWAY_SOURCE_COMMIT }}", self.route)
        self.assertIn("org.opencontainers.image.revision", self.route)
        self.assertIn("npm ci --ignore-scripts --no-audit --no-fund", self.route)
        self.assertIn("openai-production-smoke.mjs", self.route)
        self.assertIn("https://api.dragontales.milkinfrastructure.com/v1", self.route)
        self.assertIn("milk.official-openai-sdk-route-smoke.v1", self.route)
        self.assertIn("milk.official-openai-sdk-route-smoke-intent.v1", self.route)
        self.assertIn("create_same", self.route)
        self.assertIn("advance-winner-route", self.route)
        self.assertIn("advance_phase canary", self.route)
        self.assertIn("advance_phase zero", self.route)
        self.assertIn("dragontales-publish-route.sh", self.route)
        self.assertIn("DRAGONTALES_ROUTE_SECRET_HEX", self.route)
        self.assertIn('manifest.get("candidate_basis_points") != 100', self.route)
        self.assertIn("smoke traffic key is outside the one-percent canary", self.route)
        self.assertIn('remaining=$(seconds_until "$canary_not_after")', self.route)
        self.assertIn("emergency zero publication failed", self.route)
        self.assertIn("zero_published=true", self.route)
        self.assertNotIn("prepare-route", self.route)
        self.assertNotIn("500", self.route)
        self.assertNotIn("fallback injection", self.route)
        for forbidden in (
            "BASETEN_API_KEY",
            "MODAL_TOKEN_",
            "MILK_CREATE_AUTHORITY_",
            "MILK_GATEWAY_CONTAINER_ADMIN_KEY",
        ):
            self.assertNotIn(forbidden, self.route)

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
        self.assertEqual(len(scripts), 3)
        for script in scripts:
            result = subprocess.run(
                ["bash", "-n"], input=script, text=True, capture_output=True, check=False
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
