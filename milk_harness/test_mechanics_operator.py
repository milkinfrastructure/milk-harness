import datetime as dt
import hashlib
from pathlib import Path
import tempfile
import types
import unittest

from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.mechanics_operator import (
    ENDPOINT,
    FIXED_PROOF_BUDGET,
    INTENT_KEY,
    MECHANICS_EVAL_ID,
    PROOF_CONTRACT_SHA256,
    RECEIPT_KEY,
    run_once,
)
from milk_harness.scheduler import store_identity_sha256


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 29, 23, 0, 0, tzinfo=UTC)
SOURCE_COMMIT = "a" * 40
EVAL_SHA256 = "3" * 64
ACCOUNT = "cloudflare-account"
BUCKET = "milk-prod-route-evidence"
ACCESS_KEY = "route-evidence-access"
API_KEY = "dt_live_00000000-0000-4000-8000-000000000001_abcdefghijklmnop"
COHORT = "generated-mechanics-v1"


class Runner:
    def __init__(self, stdout, returncode=0):
        self.stdout = stdout
        self.returncode = returncode
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        return types.SimpleNamespace(
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=b"not persisted",
        )


class MechanicsOperatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "gateway"
        (self.source / "tools").mkdir(parents=True)
        self.tool = self.source / "tools" / "openai-production-smoke.mjs"
        self.tool.write_bytes(b"// exact pinned gateway tool\n")
        self.tool_sha256 = hashlib.sha256(self.tool.read_bytes()).hexdigest()
        self.node = self.root / "node"
        self.node.write_bytes(b"node")
        self.node.chmod(0o700)
        self.config = self.root / "gateway.json"
        self.config.write_bytes(
            canonical_json(
                {
                    "eval_id": MECHANICS_EVAL_ID,
                    "traffic_keys": [
                        {
                            "api_key_sha256": hashlib.sha256(API_KEY.encode()).hexdigest(),
                            "capture_allowed": True,
                            "cohort_id": COHORT,
                        }
                    ],
                }
            )
        )
        self.config.chmod(0o400)
        self.config_sha256 = hashlib.sha256(self.config.read_bytes()).hexdigest()
        self.credential = self.root / "gateway-credential.json"
        self.credential.write_bytes(
            canonical_json(
                {"api_key": API_KEY, "cohort_id": COHORT, "model": "gpt-5.4"}
            )
        )
        self.credential.chmod(0o600)
        self.home = self.root / "home"
        self.home.mkdir()
        self.store = LocalEvidenceStore(self.root / "evidence")
        self.manifest = {
            "schema_version": "milk.confirmed-production-run-config.v7",
            "campaign_id": MECHANICS_EVAL_ID,
            "scope_prefix": (
                f"dt/v3/{MECHANICS_EVAL_ID}/"
                "00000000-0000-4000-8000-000000000001/"
                "00000000-0000-4000-8000-000000000002/"
                "00000000-0000-4000-8000-000000000003/"
                "00000000-0000-4000-8000-000000000004"
            ),
            "gateway_deployment": {"source_commit": SOURCE_COMMIT},
            "gateway_config_sha256": self.config_sha256,
            "store_identity_sha256s": {
                "route_evidence": store_identity_sha256(ACCOUNT, BUCKET, ACCESS_KEY)
            },
            "proof_budget": {
                **FIXED_PROOF_BUDGET,
                "gateway_tool_sha256": self.tool_sha256,
            },
        }

    def tearDown(self):
        self.temporary.cleanup()

    def receipt(self, **changes):
        value = {
            "schema_version": "milk.official-openai-sdk-generated-mechanics.v2",
            "eval_id": MECHANICS_EVAL_ID,
            "gateway_config_sha256": self.config_sha256,
            "tool_sha256": self.tool_sha256,
            "sdk": "openai-node",
            "sdk_version": "6.33.0",
            "endpoint_sha256": hashlib.sha256(ENDPOINT.encode()).hexdigest(),
            "model": "gpt-5.4",
            "model_sha256": hashlib.sha256(b"gpt-5.4").hexdigest(),
            "proof_contract_sha256": PROOF_CONTRACT_SHA256,
            "proof_step": "generated_mechanics",
            "request_timeout_ms": 30_000,
            "traffic_cohort_sha256": "c" * 64,
            "concurrency": 4,
            "candidates_scanned": 1_692,
            "planned": 320,
            "attempted": 320,
            "sdk_request_count": 320,
            "baseline_request_count": 320,
            "candidate_request_count": 0,
            "max_completion_tokens": 128,
            "successful": 320,
            "failed": 0,
            "transported": 320,
            "unique_trace_ids": 320,
            "first_partition_counts": {"train": 50, "dev": 73, "calibration": 128},
            "retry_partition_counts": {"train": 13, "dev": 18, "calibration": 38},
            "total_partition_counts": {"train": 63, "dev": 91, "calibration": 166},
            "http_status_counts": {"200": 320},
            "request_set_sha256": "d" * 64,
            "response_set_sha256": "e" * 64,
            "trace_set_sha256": "f" * 64,
            "route_revision": "openai-baseline-v1",
            "preflight_health_sha256": "1" * 64,
            "postflight_health_sha256": "2" * 64,
            "postflight_health_succeeded": True,
            "content_retained": False,
            "started_at": "2026-08-29T23:00:00.000Z",
            "completed_at": "2026-08-29T23:00:00.000Z",
            "succeeded": True,
        }
        value.update(changes)
        return canonical_json(value)

    def invoke(self, runner, *, manifest=None, confirmed=None, environ=None, **changes):
        raw = canonical_json(self.manifest if manifest is None else manifest)
        arguments = {
            "confirmed_eval_sha256": EVAL_SHA256,
            "gateway_source_root": self.source,
            "gateway_source_commit": SOURCE_COMMIT,
            "gateway_config_path": self.config,
            "gateway_config_sha256": self.config_sha256,
            "gateway_credential_path": self.credential,
            "node_binary": self.node,
            "route_evidence_account_id": ACCOUNT,
            "route_evidence_bucket": BUCKET,
            "route_evidence_access_key_id": ACCESS_KEY,
            "runner": runner,
            "now": lambda: NOW,
            "environ": {} if environ is None else environ,
            "home": self.home,
        }
        arguments.update(changes)
        return run_once(
            self.store,
            raw,
            hashlib.sha256(raw).hexdigest() if confirmed is None else confirmed,
            **arguments,
        )

    def test_success_creates_and_reads_back_content_free_receipt(self):
        runner = Runner(self.receipt())
        result = self.invoke(runner)

        self.assertTrue(result["succeeded"])
        self.assertEqual(self.store.get(RECEIPT_KEY), self.receipt())
        self.assertIn(INTENT_KEY, self.store.list("proofs/v1"))
        self.assertNotIn(b"not persisted", self.store.get(RECEIPT_KEY))
        self.assertEqual(len(runner.calls), 1)
        command, options = runner.calls[0]
        self.assertEqual(
            command,
            [
                str(self.node),
                str(self.tool),
                ENDPOINT,
                str(self.credential),
                self.config_sha256,
                "--generated-mechanics",
            ],
        )
        self.assertFalse(options["shell"])
        self.assertEqual(
            options["env"],
            {"HOME": str(self.home), "LANG": "C.UTF-8", "PATH": "/usr/bin:/bin"},
        )

    def test_completed_replay_returns_receipt_without_runner(self):
        first = self.invoke(Runner(self.receipt()))
        self.node.unlink()
        self.credential.unlink()
        never = Runner(b"", returncode=99)
        second = self.invoke(never)
        self.assertEqual(second, first)
        self.assertEqual(never.calls, [])

    def test_success_longer_than_five_minutes_uses_completion_clock(self):
        times = iter((NOW, NOW + dt.timedelta(minutes=10)))
        receipt = self.receipt(
            started_at="2026-08-29T23:00:00.000Z",
            completed_at="2026-08-29T23:09:00.000Z",
        )
        result = self.invoke(Runner(receipt), now=lambda: next(times))
        self.assertTrue(result["succeeded"])

    def test_receipt_cannot_predate_its_create_only_intent(self):
        receipt = self.receipt(
            started_at="2026-08-29T22:59:59.999Z",
            completed_at="2026-08-29T23:00:00.000Z",
        )
        with self.assertRaisesRegex(ValueError, "receipt interval"):
            self.invoke(Runner(receipt))
        with self.assertRaises(FileNotFoundError):
            self.store.get(RECEIPT_KEY)

    def test_local_setup_failure_does_not_create_intent(self):
        with self.assertRaisesRegex(ValueError, "runner HOME"):
            self.invoke(Runner(self.receipt()), home=self.root / "missing")
        with self.assertRaises(FileNotFoundError):
            self.store.get(INTENT_KEY)

    def test_bad_node_or_credential_does_not_create_intent(self):
        self.node.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "not executable"):
            self.invoke(Runner(self.receipt()))
        self.node.chmod(0o700)
        self.credential.write_bytes(canonical_json({"bad": "credential"}))
        with self.assertRaisesRegex(ValueError, "mechanics credential"):
            self.invoke(Runner(self.receipt()))
        with self.assertRaises(FileNotFoundError):
            self.store.get(INTENT_KEY)

    def test_non_capture_config_does_not_create_intent(self):
        config = {
            "eval_id": MECHANICS_EVAL_ID,
            "traffic_keys": [
                {
                    "api_key_sha256": hashlib.sha256(API_KEY.encode()).hexdigest(),
                    "capture_allowed": False,
                    "cohort_id": COHORT,
                }
            ],
        }
        self.config.chmod(0o600)
        self.config.write_bytes(canonical_json(config))
        self.config.chmod(0o400)
        changed_sha256 = hashlib.sha256(self.config.read_bytes()).hexdigest()
        manifest = {**self.manifest, "gateway_config_sha256": changed_sha256}
        with self.assertRaisesRegex(ValueError, "admitted capture key"):
            self.invoke(
                Runner(self.receipt()),
                manifest=manifest,
                gateway_config_sha256=changed_sha256,
            )
        with self.assertRaises(FileNotFoundError):
            self.store.get(INTENT_KEY)

    def test_subprocess_failure_leaves_create_only_intent(self):
        runner = Runner(b"", returncode=70)
        with self.assertRaisesRegex(RuntimeError, "subprocess failed"):
            self.invoke(runner)
        self.assertIsNotNone(self.store.get(INTENT_KEY))
        with self.assertRaises(FileNotFoundError):
            self.store.get(RECEIPT_KEY)

    def test_ambiguous_intent_never_replays(self):
        with self.assertRaises(RuntimeError):
            self.invoke(Runner(b"", returncode=70))
        never = Runner(self.receipt())
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            self.invoke(never)
        self.assertEqual(never.calls, [])

    def test_malformed_or_content_bearing_receipt_is_rejected(self):
        malformed = self.receipt(content="model output")
        with self.assertRaisesRegex(ValueError, "receipt is invalid"):
            self.invoke(Runner(malformed))
        with self.assertRaises(FileNotFoundError):
            self.store.get(RECEIPT_KEY)

    def test_wrong_proof_model_and_receipt_hash_are_rejected(self):
        cases = (
            ("proof", {"proof_contract_sha256": "0" * 64}),
            ("model", {"model": "other"}),
            ("hash", {"gateway_config_sha256": "0" * 64}),
        )
        for label, change in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root:
                self.store = LocalEvidenceStore(root)
                with self.assertRaisesRegex(ValueError, "receipt is invalid"):
                    self.invoke(Runner(self.receipt(**change)))

    def test_wrong_manifest_hash_proof_tool_or_store_identity_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
            self.invoke(Runner(self.receipt()), confirmed="0" * 64)
        with self.assertRaisesRegex(ValueError, "outer eval SHA-256"):
            self.invoke(Runner(self.receipt()), confirmed_eval_sha256="invalid")

        wrong_proof = {
            **self.manifest,
            "proof_budget": {
                **self.manifest["proof_budget"],
                "max_sdk_requests": 325,
            },
        }
        with self.assertRaisesRegex(ValueError, "proof contract"):
            self.invoke(Runner(self.receipt()), manifest=wrong_proof)

        wrong_tool = {
            **self.manifest,
            "proof_budget": {
                **self.manifest["proof_budget"],
                "gateway_tool_sha256": "0" * 64,
            },
        }
        with self.assertRaisesRegex(ValueError, "identities differ"):
            self.invoke(Runner(self.receipt()), manifest=wrong_tool)

        with self.assertRaisesRegex(ValueError, "identities differ"):
            self.invoke(
                Runner(self.receipt()),
                route_evidence_access_key_id="wrong-access",
            )

    def test_ambient_provider_authority_is_rejected_before_intent(self):
        runner = Runner(self.receipt())
        with self.assertRaisesRegex(ValueError, "provider authority"):
            self.invoke(runner, environ={"BASETEN_API_KEY": "secret"})
        self.assertEqual(runner.calls, [])
        with self.assertRaises(FileNotFoundError):
            self.store.get(INTENT_KEY)


if __name__ == "__main__":
    unittest.main()
