import json
import re
import unittest
from pathlib import Path

from milk_harness.run_once import RunConfig


ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github/workflows/production-loop.yml"
CONFIG = ROOT / "deploy/run-once.production.json"
MECHANICS_CONFIG = ROOT / "deploy/run-once.mechanics.json"


class ProductionWorkflowTest(unittest.TestCase):
    def test_single_flight_workflow_runs_only_run_once(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("schedule:", text)
        self.assertNotIn("cron:", text)
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("group: milk-harness-run-once-production", text)
        self.assertIn("cancel-in-progress: false", text)
        jobs = text.split("\njobs:\n", 1)[1]
        self.assertEqual(
            re.findall(r"^  ([a-z][a-z0-9-]+):$", jobs, re.MULTILINE),
            ["run-once"],
        )
        self.assertEqual(text.count("python -m milk_harness run-once --config"), 1)
        self.assertIn('"deploy/run-once.${MILK_RUN_PROFILE}.json"', text)
        self.assertIn("persist-credentials: false", text)
        self.assertNotIn("route-signing", text.casefold())
        self.assertNotIn("docker", text.casefold())
        self.assertNotIn("gpu", text.casefold())
        self.assertNotIn("training", text.casefold())

    def test_workflow_exposes_only_the_run_once_credentials(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(
            set(re.findall(r"secrets\.([A-Z0-9_]+)", text)),
            {
                "BASETEN_API_KEY",
                "MILK_CONTROL_R2_ACCOUNT_ID",
                "MILK_CONTROL_R2_ACCESS_KEY_ID",
                "MILK_CONTROL_R2_BUCKET",
                "MILK_CONTROL_R2_SECRET_ACCESS_KEY",
                "MILK_CONTROL_R2_SESSION_TOKEN",
            },
        )
        self.assertEqual(set(re.findall(r"vars\.([A-Z0-9_]+)", text)), set())
        self.assertIn("environment: milk-provider-jobs-prod", text)
        self.assertIn(
            "MILK_CONTROL_R2_ACCOUNT_ID: ${{ secrets.MILK_CONTROL_R2_ACCOUNT_ID }}",
            text,
        )
        self.assertIn(
            "MILK_CONTROL_R2_BUCKET: ${{ secrets.MILK_CONTROL_R2_BUCKET }}",
            text,
        )

    def test_example_is_the_exact_public_run_config(self):
        raw = CONFIG.read_bytes()
        value = json.loads(raw)
        config = RunConfig.parse(value)
        self.assertEqual(config.profile, "production")
        self.assertEqual(config.scope_id, "ae052f80-006d-4fcd-94af-1bbbdf6dc4ca")
        self.assertEqual(config.store.kind, "r2")
        self.assertEqual(config.store.environment_prefix, "MILK_CONTROL_R2_")
        self.assertEqual(config.source.max_traces, 3000)
        self.assertEqual(config.teacher.api_url, "https://inference.baseten.co/v1/chat/completions")
        self.assertEqual(config.teacher.model, "zai-org/GLM-5.3-Flash")
        self.assertEqual(config.teacher.reasoning_effort, "low")
        self.assertEqual(config.teacher.api_key_env, "BASETEN_API_KEY")
        self.assertEqual(config.teacher.input_rate_microusd_per_million, 150_000)
        self.assertEqual(config.teacher.output_rate_microusd_per_million, 500_000)
        self.assertEqual(config.budget.stop_new_spend_microusd, 20_000_000)
        self.assertEqual(config.budget.absolute_spend_microusd, 25_000_000)
        self.assertTrue(config.route_proposal.enabled)
        self.assertEqual(config.route_proposal.candidate_basis_points, 0)
        self.assertNotIn(b"secret_access_key", raw.lower())

    def test_mechanics_config_is_isolated_and_bounded(self):
        raw = MECHANICS_CONFIG.read_bytes()
        config = RunConfig.parse(json.loads(raw))
        production = RunConfig.parse(json.loads(CONFIG.read_bytes()))
        self.assertEqual(config.profile, "mechanics")
        self.assertNotEqual(config.scope_id, production.scope_id)
        self.assertEqual(config.source.classifier_sample_sessions, 100)
        self.assertEqual(config.teacher.reasoning_effort, "low")
        self.assertEqual(config.route_proposal.candidate_basis_points, 500)
        self.assertLessEqual(
            config.teacher.reserved_cost() * config.teacher.max_calls_per_run,
            config.budget.stop_new_spend_microusd,
        )
        self.assertNotIn(b"secret_access_key", raw.lower())


if __name__ == "__main__":
    unittest.main()
