import copy
import hashlib
import json
from pathlib import Path
import stat
import tempfile
import unittest

from milk_harness.eval_config import MAX_EVAL_BYTES, main, validate_eval_document
from milk_harness.evidence import canonical_json
from milk_harness.scheduler import (
    RUN_ARTIFACTS,
    RUN_LIMITS,
    store_identity_sha256,
)


ROOT = Path(__file__).parents[1]
SELF_HOST_EXAMPLE = ROOT / "examples/self-host/milk.eval.example.json"


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
        "winner_model_alias": "milk-student",
        "modal_workspace_id": "ws-1",
        "modal_workspace_name": "milk-workspace",
        "modal_environment_id": "en-1",
        "modal_environment_name": "milk-production",
        "modal_app_id": "ap-1",
        "modal_app_name": "milk-gpu",
        "modal_registry_secret_name": "modal-registry",
        "modal_registry_secret_id": "st-registry",
        "modal_config_secret_name": "modal-config",
        "modal_config_secret_id": "st-config",
        "modal_control_secret_name": "modal-control",
        "modal_control_secret_id": "st-control",
        "modal_capture_secret_name": "modal-capture",
        "modal_capture_secret_id": "st-capture",
        "modal_candidate_secret_name": "modal-candidate",
        "modal_candidate_secret_id": "st-candidate",
        "modal_teacher_volume_name": "modal-teacher",
        "modal_teacher_volume_id": "vo-teacher",
        "modal_student_train_volume_name": "modal-student",
        "modal_student_train_volume_id": "vo-student",
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
        "application_id": "00000000-0000-4000-8000-000000000020",
        "application_version": 7,
        "container_image": "registry.cloudflare.com/milk-gateway@sha256:" + "c" * 64,
        "worker_version_id": "00000000-0000-4000-8000-000000000021",
    }
    manifest = {
        "schema_version": "milk.confirmed-production-run-config.v5",
        "provider_policy": {"primary": "baseten", "fallback": "modal"},
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


class EvalConfigTest(unittest.TestCase):
    def test_bundled_self_host_example_is_a_bounded_nonsecret_config_smoke(self):
        raw = SELF_HOST_EXAMPLE.read_bytes()
        value = json.loads(raw)
        validated, _, _ = validate_eval_document(
            raw,
            value["manifest"]["campaign_id"],
            value["manifest"]["harness_source_commit"],
            ROOT,
        )
        self.assertEqual(validated["gateway_config"]["teacher"]["max_decisions"], 1)
        self.assertEqual(
            validated["manifest"]["gateway_job_contract"]["teacher_max_decisions"],
            1,
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
            self.assertEqual(
                env["MILK_EVAL_CONFIG_SHA256"], hashlib.sha256(raw).hexdigest()
            )
            self.assertEqual(env["MILK_CONTROL_R2_BUCKET"], "gateway-control")
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
