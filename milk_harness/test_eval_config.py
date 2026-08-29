import copy
import hashlib
import json
from pathlib import Path
import stat
import tempfile
import unittest

from milk_harness.eval_config import (
    MAX_EVAL_BYTES,
    MECHANICS_PROOF_EVAL_ID,
    _validate_mechanics_proof_budget,
    main,
    validate_eval_document,
)
from milk_harness.evidence import canonical_json
from milk_harness.scheduler import (
    PRODUCTION_PROOF_BUDGET_FIXED,
    RUN_ARTIFACTS,
    RUN_LIMITS,
    store_identity_sha256,
)


ROOT = Path(__file__).parents[1]


def eval_document():
    ids = [f"00000000-0000-4000-8000-0000000000{value:02d}" for value in range(10, 14)]
    eval_id = "e" * 64
    images = {
        "gateway": "ghcr.io/milkinfrastructure/milk-gateway@sha256:" + "1" * 64,
        "jobs": "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + "2" * 64,
        "student_train": "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "3" * 64,
        "student_branch": "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "4" * 64,
        "teacher": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:" + "5" * 64,
    }
    gateway = {
        "tenant_id": ids[0],
        "project_id": ids[1],
        "environment_id": ids[2],
        "workload_id": ids[3],
        "eval_id": eval_id,
        "stores": {
            name: {
                "type": "cloudflare_r2",
                "account_id": "gateway-account",
                "bucket": f"gateway-{name}",
            }
            for name in ("capture", "control", "routes")
        },
        "teacher": {
            "max_decisions": 4096,
            "execution": {
                "type": "gpu_job",
                "runtime_image_reference": images["teacher"],
                "max_gpu_seconds": 3600,
                "max_calls": 10,
                "max_parallel_runs": 1,
            },
            "student_train_runtime_image_reference": images["student_train"],
            "student_branch_runtime_image_reference": images["student_branch"],
            "student_recipe_sha256": "6" * 64,
            "deployment_sha256": "7" * 64,
            "terms_sha256": "8" * 64,
        },
    }
    provider_runtime = {
        "baseten_team_name": "milk-production",
        "winner_model_alias": "gpt-5.4",
    }
    provider_secret_names = {
        "registry": "registry-secret",
        "config": "config-secret",
        "capture_access": "capture-access-secret",
        "capture_secret": "capture-secret-secret",
        "capture_session": None,
        "control_access": "control-access-secret",
        "control_secret": "control-secret-secret",
        "control_session": None,
    }
    access = {
        "gateway_capture": ("gateway-account", "gateway-capture", "capture-reader"),
        "gateway_control": ("gateway-account", "gateway-control", "control-reader"),
        "provider_control": ("gateway-account", "gateway-control", "provider-reader"),
        "provider_gpu_capture": (
            "gateway-account",
            "gateway-capture",
            "provider-gpu-capture",
        ),
        "provider_gpu_control": (
            "gateway-account",
            "gateway-control",
            "provider-gpu-control",
        ),
        "evidence": ("evidence-account", "evidence", "evidence-writer"),
        "ops_log": ("ops-account", "ops", "ops-writer"),
        "create_authority_write": ("create-account", "create", "create-writer"),
        "create_authority_read": ("create-account", "create", "create-reader"),
        "gateway_ingest_control": ("gateway-account", "gateway-control", "ingest-writer"),
        "gateway_ingest_route": ("gateway-account", "gateway-routes", "route-reader"),
        "route_control": ("gateway-account", "gateway-control", "route-control"),
        "route_routes": ("gateway-account", "gateway-routes", "route-writer"),
        "route_evidence": ("route-account", "route-evidence", "route-evidence-writer"),
    }
    gateway_deployment = {
        "source_commit": "9" * 40,
        "image_admission_sha256": "a" * 64,
        "release_sha256": "b" * 64,
        "official_openai_sdk_baseline_receipt_sha256": "d" * 64,
        "application_id": "00000000-0000-4000-8000-000000000020",
        "application_version": 7,
        "container_image": "registry.cloudflare.com/milk-gateway@sha256:" + "c" * 64,
        "worker_version_id": "00000000-0000-4000-8000-000000000021",
    }
    manifest = {
        "schema_version": "milk.confirmed-production-run-config.v7",
        "provider_policy": {"only": "baseten"},
        "harness_source_commit": "d" * 40,
        "campaign_id": eval_id,
        "provider_project_id": "project_1",
        "provider_runtime": provider_runtime,
        "scope_prefix": "dt/v3/" + "/".join((eval_id, *ids)),
        "images": images,
        "image_release_sha256": "f" * 64,
        "gateway_config_sha256": hashlib.sha256(canonical_json(gateway)).hexdigest(),
        "gateway_deployment": gateway_deployment,
        "gateway_job_contract": {
            "teacher_runtime_image_reference": images["teacher"],
            "teacher_max_gpu_seconds": 3600,
            "teacher_max_calls": 10,
            "teacher_max_parallel_runs": 1,
            "teacher_max_decisions": 4096,
            "student_train_runtime_image_reference": images["student_train"],
            "student_branch_runtime_image_reference": images["student_branch"],
            "student_recipe_sha256": "6" * 64,
            "teacher_deployment_sha256": "7" * 64,
            "teacher_terms_sha256": "8" * 64,
        },
        "provider_secret_names": provider_secret_names,
        "store_identity_sha256s": {
            name: store_identity_sha256(*location) for name, location in access.items()
        },
        "harness_artifact_sha256s": {
            relative: hashlib.sha256(ROOT.joinpath(relative).read_bytes()).hexdigest()
            for relative in RUN_ARTIFACTS
        },
        "limits": RUN_LIMITS,
        "proof_budget": {
            **PRODUCTION_PROOF_BUDGET_FIXED,
            "gateway_tool_sha256": "1" * 64,
        },
    }
    return {
        "schema_version": "milk.eval.v1",
        "manifest_sha256": hashlib.sha256(canonical_json(manifest)).hexdigest(),
        "manifest": manifest,
        "gateway_config": gateway,
        "cloudflare_account_id": "cloudflare-production-account",
        "locations": {
            "evidence": {"account_id": "evidence-account", "bucket": "evidence"},
            "ops_log": {"account_id": "ops-account", "bucket": "ops"},
            "create_authority": {"account_id": "create-account", "bucket": "create"},
            "route_evidence": {"account_id": "route-account", "bucket": "route-evidence"},
        },
    }


def mechanics_proof_document():
    value = eval_document()
    old_eval_id = value["manifest"]["campaign_id"]
    gateway = value["gateway_config"]
    gateway["eval_id"] = MECHANICS_PROOF_EVAL_ID
    gateway["teacher"]["max_decisions"] = 320
    gateway["teacher"]["execution"].update(
        {
            "max_calls": 20,
            "max_gpu_seconds": 3_600,
            "max_parallel_runs": 1,
        }
    )
    gateway["route"] = {
        "winner_max_wall_seconds": 3_600,
        "winner_max_cost_microusd": 7_500_000,
    }
    manifest = value["manifest"]
    manifest["campaign_id"] = MECHANICS_PROOF_EVAL_ID
    manifest["scope_prefix"] = manifest["scope_prefix"].replace(
        old_eval_id, MECHANICS_PROOF_EVAL_ID, 1
    )
    manifest["gateway_job_contract"].update(
        {
            "teacher_max_calls": 20,
            "teacher_max_decisions": 320,
            "teacher_max_gpu_seconds": 3_600,
            "teacher_max_parallel_runs": 1,
        }
    )
    manifest["gateway_config_sha256"] = hashlib.sha256(
        canonical_json(gateway)
    ).hexdigest()
    value["manifest_sha256"] = hashlib.sha256(canonical_json(manifest)).hexdigest()
    return value


class EvalConfigTest(unittest.TestCase):
    def test_mechanics_proof_gpu_limits_fit_the_fixed_reservation(self):
        value = mechanics_proof_document()
        raw = canonical_json(value)
        validated, _, _ = validate_eval_document(
            raw,
            MECHANICS_PROOF_EVAL_ID,
            "d" * 40,
            ROOT,
        )
        self.assertEqual(validated, value)
        self.assertEqual(
            _validate_mechanics_proof_budget(
                value["gateway_config"], value["manifest"]
            ),
            142_500_000,
        )

        below_reservation = copy.deepcopy(value["manifest"])
        below_reservation["proof_budget"]["gpu_reservation_microusd"] = 142_499_999
        with self.assertRaisesRegex(ValueError, "exceeds its budget"):
            _validate_mechanics_proof_budget(
                value["gateway_config"], below_reservation
            )

    def test_mechanics_proof_rejects_any_gateway_gpu_limit_drift(self):
        cases = (
            (("teacher", "max_decisions"), 319),
            (("teacher", "execution", "max_calls"), 19),
            (("teacher", "execution", "max_gpu_seconds"), 3_599),
            (("teacher", "execution", "max_parallel_runs"), 2),
            (("route", "winner_max_wall_seconds"), 3_599),
            (("route", "winner_max_cost_microusd"), 7_499_999),
        )
        for path, replacement in cases:
            value = mechanics_proof_document()
            target = value["gateway_config"]
            for name in path[:-1]:
                target = target[name]
            target[path[-1]] = replacement
            value["manifest"]["gateway_config_sha256"] = hashlib.sha256(
                canonical_json(value["gateway_config"])
            ).hexdigest()
            value["manifest_sha256"] = hashlib.sha256(
                canonical_json(value["manifest"])
            ).hexdigest()
            with self.subTest(path=path), self.assertRaisesRegex(
                ValueError, "mechanics proof gateway GPU limits"
            ):
                validate_eval_document(
                    canonical_json(value),
                    MECHANICS_PROOF_EVAL_ID,
                    "d" * 40,
                    ROOT,
                )

        value = mechanics_proof_document()
        value["manifest"]["gateway_job_contract"]["teacher_max_calls"] = 19
        value["manifest_sha256"] = hashlib.sha256(
            canonical_json(value["manifest"])
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "mechanics proof gateway GPU limits"):
            validate_eval_document(
                canonical_json(value),
                MECHANICS_PROOF_EVAL_ID,
                "d" * 40,
                ROOT,
            )

    def test_validates_and_materializes_the_single_eval_authority(self):
        value = eval_document()
        raw = canonical_json(value)
        eval_id = value["manifest"]["campaign_id"]
        self.assertNotEqual(eval_id, hashlib.sha256(raw).hexdigest())
        validated, manifest_raw, gateway_raw = validate_eval_document(
            raw, eval_id, "d" * 40, ROOT
        )
        self.assertEqual(validated, value)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = root / "eval.json"
            document.write_bytes(raw)
            environment = root / "github-env"
            output = root / "github-output"
            manifest = root / "manifest.json"
            gateway = root / "gateway.json"
            self.assertEqual(
                main(
                    [
                        "--document", str(document),
                        "--eval-id", eval_id,
                        "--harness-source-commit", "d" * 40,
                        "--root", str(ROOT),
                        "--manifest-output", str(manifest),
                        "--gateway-config-output", str(gateway),
                        "--github-env", str(environment),
                        "--github-output", str(output),
                    ]
                ),
                0,
            )
            self.assertEqual(manifest.read_bytes(), manifest_raw)
            self.assertEqual(gateway.read_bytes(), gateway_raw)
            self.assertEqual(stat.S_IMODE(manifest.stat().st_mode), 0o400)
            env = dict(
                line.split("=", 1)
                for line in environment.read_text(encoding="utf-8").splitlines()
            )
            self.assertEqual(env["MILK_GATEWAY_IMAGE"], value["manifest"]["images"]["gateway"])
            self.assertEqual(env["MILK_GATEWAY_SOURCE_COMMIT"], "9" * 40)
            self.assertEqual(env["MILK_BASETEN_TEAM_NAME"], "milk-production")
            self.assertEqual(env["MILK_WINNER_MODEL_ALIAS"], "gpt-5.4")
            self.assertFalse(any(name.startswith("MILK_MODAL_") for name in env))
            self.assertEqual(
                env["MILK_EVAL_CONFIG_SHA256"], hashlib.sha256(raw).hexdigest()
            )
            self.assertEqual(env["MILK_CONTROL_R2_BUCKET"], "gateway-control")
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CAPTURE_R2_BUCKET"], "gateway-capture"
            )
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CAPTURE_R2_ACCOUNT_ID"], "gateway-account"
            )
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CONTROL_R2_BUCKET"], "gateway-control"
            )
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CONTROL_R2_ACCOUNT_ID"], "gateway-account"
            )
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CAPTURE_IDENTITY_SHA256"],
                value["manifest"]["store_identity_sha256s"]["provider_gpu_capture"],
            )
            self.assertEqual(
                env["MILK_PROVIDER_GPU_CONTROL_IDENTITY_SHA256"],
                value["manifest"]["store_identity_sha256s"]["provider_gpu_control"],
            )
            self.assertEqual(env["MILK_CREATE_AUTHORITY_READ_R2_BUCKET"], "create")
            self.assertEqual(env["MILK_CREATE_AUTHORITY_WRITE_R2_BUCKET"], "create")
            self.assertEqual(env["CLOUDFLARE_ACCOUNT_ID"], "cloudflare-production-account")
            self.assertEqual(
                output.read_text(encoding="utf-8"),
                "gateway_source_commit=" + "9" * 40 + "\n",
            )

    def test_rejects_drift_and_noncanonical_or_oversized_documents(self):
        value = eval_document()

        def rejected(changed, message=None):
            raw = canonical_json(changed)
            context = self.assertRaisesRegex(ValueError, message) if message else self.assertRaises(ValueError)
            with context:
                validate_eval_document(raw, value["manifest"]["campaign_id"], "d" * 40, ROOT)

        noncanonical = json.dumps(value, indent=2).encode()
        with self.assertRaisesRegex(ValueError, "canonical JSON"):
            validate_eval_document(
                noncanonical,
                value["manifest"]["campaign_id"],
                "d" * 40,
                ROOT,
            )
        with self.assertRaisesRegex(ValueError, "canonical JSON"):
            validate_eval_document(
                canonical_json(value).removesuffix(b"\n"),
                value["manifest"]["campaign_id"],
                "d" * 40,
                ROOT,
            )
        with self.assertRaisesRegex(ValueError, "eval ID"):
            validate_eval_document(canonical_json(value), "0" * 64, "d" * 40, ROOT)
        cross_scope = copy.deepcopy(value)
        cross_scope["manifest"]["scope_prefix"] = cross_scope["manifest"][
            "scope_prefix"
        ].replace(value["manifest"]["campaign_id"], "0" * 64, 1)
        cross_scope["manifest_sha256"] = hashlib.sha256(
            canonical_json(cross_scope["manifest"])
        ).hexdigest()
        rejected(cross_scope, "scope eval")
        cross_gateway = copy.deepcopy(value)
        cross_gateway["gateway_config"]["eval_id"] = "0" * 64
        cross_gateway["manifest"]["gateway_config_sha256"] = hashlib.sha256(
            canonical_json(cross_gateway["gateway_config"])
        ).hexdigest()
        cross_gateway["manifest_sha256"] = hashlib.sha256(
            canonical_json(cross_gateway["manifest"])
        ).hexdigest()
        rejected(cross_gateway, "eval ID")
        rejected({**value, "manifest_sha256": "0" * 64}, "manifest SHA-256")
        old_schema = copy.deepcopy(value)
        old_schema["manifest"]["schema_version"] = (
            "milk.confirmed-production-run-config.v6"
        )
        old_schema["manifest_sha256"] = hashlib.sha256(
            canonical_json(old_schema["manifest"])
        ).hexdigest()
        rejected(old_schema, "confirmed run config is invalid")
        fallback = copy.deepcopy(value)
        fallback["manifest"]["provider_policy"] = {
            "primary": "baseten",
            "fallback": "modal",
        }
        fallback["manifest_sha256"] = hashlib.sha256(
            canonical_json(fallback["manifest"])
        ).hexdigest()
        rejected(fallback, "confirmed run config is invalid")
        modal_runtime = copy.deepcopy(value)
        modal_runtime["manifest"]["provider_runtime"]["modal_app_id"] = "ap-1"
        modal_runtime["manifest_sha256"] = hashlib.sha256(
            canonical_json(modal_runtime["manifest"])
        ).hexdigest()
        rejected(modal_runtime, "provider runtime settings")
        drifted_gateway = {**value["gateway_config"], "tenant_id": "0" * 36}
        rejected({**value, "gateway_config": drifted_gateway}, "gateway config SHA-256")
        shared = {
            **value,
            "locations": {
                **value["locations"],
                "evidence": {"account_id": "gateway-account", "bucket": "gateway-control"},
            },
        }
        rejected(shared, "pairwise distinct")
        with self.assertRaisesRegex(ValueError, "bounded JSON bytes"):
            validate_eval_document(b"{" + b" " * MAX_EVAL_BYTES + b"}", "0" * 64, "d" * 40, ROOT)


if __name__ == "__main__":
    unittest.main()
