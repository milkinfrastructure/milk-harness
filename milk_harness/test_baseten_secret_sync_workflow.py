import pathlib
import unittest


WORKFLOW = (
    pathlib.Path(__file__).parents[1]
    / ".github"
    / "workflows"
    / "sync-baseten-registry.yml"
).read_text(encoding="utf-8")


class BasetenSecretSyncWorkflowTests(unittest.TestCase):
    def test_sync_is_manual_bounded_and_uses_existing_environment_secrets(self):
        self.assertIn("workflow_dispatch:", WORKFLOW)
        self.assertNotIn("schedule:", WORKFLOW)
        self.assertIn("environment: milk-provider-jobs-prod", WORKFLOW)
        self.assertIn("timeout-minutes: 5", WORKFLOW)
        self.assertIn("secrets.MILK_GHCR_PULL_USERNAME", WORKFLOW)
        self.assertIn("secrets.MILK_GHCR_PULL_TOKEN", WORKFLOW)
        self.assertIn("secrets.BASETEN_API_KEY", WORKFLOW)

    def test_sync_writes_only_the_two_registry_secret_names(self):
        self.assertIn("milk-prod-ghcr DOCKER_REGISTRY_ghcr.io", WORKFLOW)
        self.assertIn("https://api.baseten.co/v1/secrets", WORKFLOW)
        self.assertNotIn("/training_projects", WORKFLOW)
        self.assertNotIn("/training_jobs", WORKFLOW)
        self.assertNotIn("set -x", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
