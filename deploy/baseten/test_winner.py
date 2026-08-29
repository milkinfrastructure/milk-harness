import ast
import datetime as dt
import hashlib
import json
from pathlib import Path
import unittest

from deploy.baseten import winner
from milk_harness import provider_acceptance


RUN = "1" * 64
CLAIM = "2" * 64
STUDENT = "3" * 64
IMAGE = "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "4" * 64
MODEL = "model123"
DEPLOYMENT = "deployment_123"
ALIAS = "milk-winner"
KEY = "candidate-key"


def values():
    return winner.settings(
        IMAGE,
        STUDENT,
        ALIAS,
        "DOCKER_REGISTRY_ghcr.io",
        "gateway_config",
        "control_access",
        "control_secret",
        "control_session",
    )


def identity():
    return {
        "model_id": MODEL,
        "deployment_id": DEPLOYMENT,
        "execution_name": winner.execution_name(RUN),
        "labels": winner.labels(RUN, CLAIM),
        "endpoint": (
            f"https://model-{MODEL}.api.baseten.co/deployment/{DEPLOYMENT}"
            "/sync/v1/chat/completions"
        ),
    }


def deployment(*, status="ACTIVE", replicas=1, autoscaling=None):
    return {
        "id": DEPLOYMENT,
        "created_at": "2026-08-27T20:00:00Z",
        "name": winner.execution_name(RUN),
        "model_id": MODEL,
        "is_production": True,
        "is_development": False,
        "status": status,
        "active_replica_count": replicas,
        "autoscaling_settings": (
            dict(winner.SERVING_AUTOSCALING)
            if autoscaling is None
            else autoscaling
        ),
        "instance_type_name": "H100",
        "environment": "production",
        "labels": winner.labels(RUN, CLAIM),
    }


def materialization():
    return {
        "schema_version": winner.MATERIALIZATION_SCHEMA,
        "student_job_id": STUDENT,
        "variant": "static_fp8",
        "model_manifest_sha256": "5" * 64,
        "file_count": 12,
        "artifact_bytes": 4096,
        "stage_path": "/tmp/dragontales-winner",
        "model_path": "/tmp/dragontales-winner/model",
        "model_manifest_path": "/tmp/dragontales-winner/model-manifest.json",
    }


def acceptance():
    return {
        "schema_version": provider_acceptance.SCHEMA,
        "campaign_id": "a" * 64,
        "run_id": RUN,
        "claim_sha256": CLAIM,
        "outbox_sha256": "b" * 64,
        "provider_binding_sha256": "c" * 64,
        "selection": {
            "selected_provider": "baseten",
            "provider_identity": {"provider": "baseten", "team_name": "milk"},
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": "d" * 64,
                "observed_at": "2026-08-27T20:00:00Z",
            },
        },
        "image_release_sha256": "e" * 64,
        "image_admission_sha256": "f" * 64,
        "provider_pass_claim_sha256": "1" * 64,
        "create_authorization_sha256": "2" * 64,
        "budget_reservation_sha256": "3" * 64,
        "reserved_microusd": 1,
        "reserved_at": "2026-08-27T20:00:00Z",
        "accepted_at": "2026-08-27T20:00:00Z",
        "create_not_after": "2026-08-27T20:02:00Z",
        "provider_not_after": "2026-08-27T20:30:00Z",
        "max_wall_seconds": 1800,
        "max_cost_microusd": 1_000_000,
        "state": "accepted",
    }


class WinnerPureContractTest(unittest.TestCase):
    def test_module_has_no_independent_mutation_or_transport(self):
        forbidden = {
            "start",
            "stop",
            "http_json",
            "write_config",
            "main",
            "_OPENER",
            "_run_push",
            "_run_gateway_status",
        }
        self.assertTrue(forbidden.isdisjoint(vars(winner)))
        tree = ast.parse(Path(winner.__file__).read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertTrue(
            {"subprocess", "urllib.request", "http.client", "socket"}.isdisjoint(
                imports
            )
        )

    def test_config_is_one_private_no_build_h100_with_zero_floor(self):
        self.assertFalse(winner.DIRECT_IMAGE_RUNTIME_VERIFIED)
        self.assertIn("non-root", winner.DIRECT_IMAGE_RUNTIME_BLOCKER)
        raw = winner.truss_config(values(), RUN)
        self.assertIn(f"model_name: {winner.model_name(RUN)}\n", raw)
        self.assertIn(f"  image: {IMAGE}\n", raw)
        self.assertIn(
            "  docker_auth:\n"
            "    auth_method: REGISTRY_SECRET\n"
            "    registry: ghcr.io\n"
            "    secret_name: DOCKER_REGISTRY_ghcr.io\n",
            raw,
        )
        self.assertIn("  no_build: true\n", raw)
        self.assertIn("  accelerator: H100\n", raw)
        self.assertIn("--trusted-ingress-auth", raw)
        self.assertIn(
            "/usr/bin/test ! -r /secrets/DOCKER_REGISTRY_ghcr.io",
            raw,
        )
        self.assertNotIn("BASETEN_API_KEY", raw)
        self.assertNotIn(KEY, raw)
        self.assertEqual(winner.SERVING_AUTOSCALING["min_replica"], 0)
        self.assertEqual(winner.SERVING_AUTOSCALING["max_replica"], 1)
        argv = winner.push_argv("/tmp/milk-winner/truss", RUN, CLAIM, "milk")
        self.assertNotIn("--promote", argv)
        self.assertEqual(argv[argv.index("--team") + 1], "milk")
        self.assertEqual(
            json.loads(argv[argv.index("--labels") + 1]),
            winner.labels(RUN, CLAIM),
        )

    def test_push_identity_targets_exact_deployment_not_environment(self):
        raw = json.dumps(
            {
                "model_id": MODEL,
                "model_version_id": DEPLOYMENT,
                "predict_url": f"https://model-{MODEL}.api.baseten.co/predict",
                "logs_url": "https://app.baseten.co/models/logs",
                "is_draft": False,
            },
            separators=(",", ":"),
        ).encode()
        self.assertEqual(winner.parse_push(raw, RUN, CLAIM), identity())
        self.assertIn(f"/deployment/{DEPLOYMENT}/sync/", identity()["endpoint"])
        self.assertNotIn("/environments/", identity()["endpoint"])

    def test_deployment_and_scoped_key_validation_fail_closed(self):
        self.assertEqual(
            winner.validate_deployment(deployment(), identity())["id"],
            DEPLOYMENT,
        )
        wrong = deployment()
        wrong["autoscaling_settings"] = dict(winner.SERVING_AUTOSCALING)
        wrong["autoscaling_settings"]["min_replica"] = 1
        with self.assertRaisesRegex(ValueError, "autoscaling"):
            winner.validate_deployment(wrong, identity())
        with self.assertRaisesRegex(ValueError, "H100 state"):
            winner.validate_deployment(
                deployment(status="UNRECOGNIZED_PROVIDER_STATE"),
                identity(),
            )
        stopped = deployment(status="INACTIVE", replicas=0)
        self.assertEqual(
            winner.validate_deployment(stopped, identity(), stopped=True)[
                "active_replica_count"
            ],
            0,
        )
        metadata = {
            "keys": [
                {
                    "prefix": "manager",
                    "name": "key-manager",
                    "type": "WORKSPACE_MANAGE_API_KEYS",
                    "model_ids": None,
                    "team_name": None,
                    "owner": None,
                },
                {
                    "prefix": "candidate",
                    "name": "winner",
                    "type": "WORKSPACE_INVOKE",
                    "model_ids": [MODEL],
                    "team_name": "milk",
                    "owner": None,
                },
            ]
        }
        self.assertEqual(
            winner.validate_invocation_key(metadata, KEY, MODEL, "milk")["model_ids"],
            [MODEL],
        )
        metadata["keys"][1]["model_ids"] = ["other"]
        with self.assertRaisesRegex(ValueError, "exact Baseten model"):
            winner.validate_invocation_key(metadata, KEY, MODEL, "milk")

    def test_log_probe_and_gateway_wire_are_strict(self):
        receipt = materialization()
        logs = {
            "logs": [
                {
                    "timestamp": "2026-08-27T20:00:00Z",
                    "message": json.dumps(receipt, separators=(",", ":")),
                    "replica": "replica-1",
                }
            ]
        }
        self.assertEqual(
            winner.materialization_from_logs(logs, STUDENT),
            receipt,
        )
        probe = {
            "schema_version": winner.PROBE_SCHEMA,
            "student_job_id": STUDENT,
            "student_variant": "static_fp8",
            "model_manifest_sha256": receipt["model_manifest_sha256"],
            "model_alias_sha256": hashlib.sha256(ALIAS.encode()).hexdigest(),
            "candidate_api_key_sha256": hashlib.sha256(KEY.encode()).hexdigest(),
            "models_response_sha256": "6" * 64,
            "chat_request_sha256": "7" * 64,
            "chat_response_sha256": "8" * 64,
        }
        winner.validate_probe(probe, STUDENT, ALIAS, receipt, KEY)
        admission = {
            "schema_version": winner.RECEIPT_SCHEMA,
            "provider": "baseten",
            "student_job_id": STUDENT,
            "student_variant": "static_fp8",
            "model_manifest_sha256": "5" * 64,
            "model_alias": ALIAS,
            "model_alias_sha256": probe["model_alias_sha256"],
            "candidate_api_key_sha256": probe["candidate_api_key_sha256"],
            "student_branch_runtime_image_reference": IMAGE,
            "admission_program_sha256": "9" * 64,
            "execution_id": DEPLOYMENT,
            "execution_name": winner.execution_name(RUN),
            "chat_completions_url": identity()["endpoint"],
            "models_response_sha256": probe["models_response_sha256"],
            "chat_request_sha256": probe["chat_request_sha256"],
            "chat_response_sha256": probe["chat_response_sha256"],
            "launch_started_at": "2026-08-27T20:00:00Z",
            "ready_at": "2026-08-27T20:01:00Z",
            "admitted_at": "2026-08-27T20:02:00Z",
            "service_not_after": "2026-08-27T20:17:00Z",
        }
        result = {
            "schema_version": winner.RESULT_SCHEMA,
            "scope": {
                "tenant_id": "00000000-0000-0000-0000-000000000001",
                "project_id": "00000000-0000-0000-0000-000000000002",
                "environment_id": "00000000-0000-0000-0000-000000000003",
                "workload_id": "00000000-0000-0000-0000-000000000004",
                "eval_id": acceptance()["campaign_id"],
            },
            "student_job_id": STUDENT,
            "claim_sha256": CLAIM,
            "provider_binding_sha256": "a" * 64,
            "provider_acceptance": acceptance(),
            "observed_cost_microusd": 1,
            "admission": admission,
        }
        raw = winner.result_bytes(result)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertLess(raw.index(b'"scope"'), raw.index(b'"student_job_id"'))
        self.assertEqual(json.loads(raw), result)

        cross_eval = json.loads(raw)
        cross_eval["scope"]["eval_id"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "eval differs"):
            winner.result_bytes(cross_eval)

    def test_timestamp_requires_whole_second_utc(self):
        now = dt.datetime(2026, 8, 27, 20, 0, tzinfo=dt.timezone.utc)
        self.assertEqual(winner._timestamp(now), "2026-08-27T20:00:00Z")
        with self.assertRaisesRegex(ValueError, "whole-second"):
            winner._timestamp(now.replace(microsecond=1))


if __name__ == "__main__":
    unittest.main()
