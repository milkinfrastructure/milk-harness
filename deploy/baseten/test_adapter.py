import ast
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import unittest


sys.path.insert(0, str(Path(__file__).parent))
import adapter


STUDENT = "1" * 64
TRAIN_IMAGE = "ghcr.io/example/dragontales-train@sha256:" + "2" * 64
BRANCH_IMAGE = "ghcr.io/example/dragontales-branch@sha256:" + "3" * 64
FIXTURE_IMAGE = "fixture/dragontales@sha256:" + "9" * 64
TEACHER_RUN = "8" * 64
TEACHER_IMAGE = "ghcr.io/example/dragontales-teacher@sha256:" + "a" * 64
PROJECT = "project_123"


def settings():
    return {
        "student_train_image": TRAIN_IMAGE,
        "student_branch_image": BRANCH_IMAGE,
        "teacher_image": TEACHER_IMAGE,
        "registry_secret": "dragontales_ghcr",
        "config_secret": "dragontales_config",
        "capture_store_account_id": "gateway-account",
        "capture_store_identity_sha256": "5" * 64,
        "capture_store_access_key_secret": "capture_access",
        "capture_store_secret_key_secret": "capture_secret",
        "capture_store_session_token_secret": "capture_session",
        "control_store_account_id": "gateway-account",
        "control_store_identity_sha256": "6" * 64,
        "control_store_access_key_secret": "control_access",
        "control_store_secret_key_secret": "control_secret",
        "control_store_session_token_secret": "control_session",
    }


def teacher_launch():
    return {
        "schema_version": "dragontales.teacher-gpu-run-launch.v1",
        "teacher_run_id": TEACHER_RUN,
        "claim_object_key": f"scope/jobs/teacher-gpu/{TEACHER_RUN}/claim.json",
        "claim_sha256": "b" * 64,
        "provider_binding_sha256": "c" * 64,
        "slot": 0,
        "runtime_image_reference": TEACHER_IMAGE,
        "call_count": 12,
        "max_gpu_seconds": 3_600,
        "expires_at": "2099-08-27T18:00:00.852709Z",
        "state": "claimed",
    }


def line(value):
    return (json.dumps(value, separators=(",", ":")) + "\n").encode()


def provider_response(name, job_id="job_1", status="TRAINING_JOB_CREATED"):
    return line(
        {
            "training_job": {
                "id": job_id,
                "created_at": "2026-08-27T18:00:00Z",
                "current_status": status,
                "instance_type": {
                    "id": "h100_instance",
                    "name": "H100",
                    "memory_limit_mib": 65_536,
                    "millicpu_limit": 8_000,
                    "gpu_count": 1,
                    "gpu_type": "H100",
                    "gpu_memory_limit_mib": 81_920,
                },
                "updated_at": "2026-08-27T18:00:00Z",
                "training_project_id": PROJECT,
                "training_project": {"id": PROJECT, "name": "dragontales"},
                "error_message": None,
                "node_count": 1,
                "name": name,
                "checkpoint_sync_status": None,
                "priority": 0,
                "availability_model": "dedicated",
                "user": None,
            }
        }
    )


class AdapterTest(unittest.TestCase):
    def test_adapter_is_non_executable_and_has_no_provider_transport(self):
        forbidden_symbols = {
            "API_ROOT",
            "Dispatcher",
            "_NoRedirect",
            "_OPENER",
            "_http_post",
            "dispatch_ticks",
            "GATEWAY_ENVIRONMENT_KEYS",
            "MAX_DISPATCHES",
            "MAX_TICK_TIMEOUT_SECONDS",
            "TEACHER_PROFILE_PATH",
            "_arguments",
            "_gateway_environment",
            "_preflight_teacher_config",
            "_require_teacher_runway",
            "main",
            "parse_launch_line",
        }
        self.assertTrue(forbidden_symbols.isdisjoint(vars(adapter)))

        source_path = Path(adapter.__file__)
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            alias.name.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertTrue(
            {"urllib", "http", "requests", "socket"}.isdisjoint(imports)
        )
        self.assertNotIn("api.baseten.co", source)
        self.assertNotIn("Authorization", source)

        environment = os.environ.copy()
        environment["BASETEN_API_KEY"] = "must-not-be-used"
        completed = subprocess.run(
            [sys.executable, str(source_path)],
            check=False,
            capture_output=True,
            env=environment,
            timeout=5,
        )
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, b"")
        self.assertEqual(completed.stderr, b"")

    def test_jobs_consumed_surface_remains_available(self):
        names = {
            "OUTER_TIMEOUT_SECONDS",
            "TEACHER_TERMINALIZATION_GRACE_SECONDS",
            "_canonical",
            "_provider_execution",
            "_request_body",
            "_teacher_request_body",
            "identifier",
            "immutable_ghcr_image",
        }
        self.assertTrue(names.issubset(vars(adapter)))
        self.assertTrue(
            all(
                callable(getattr(adapter, name))
                for name in names
                if not name.isupper()
            )
        )

    def test_student_request_builders_are_pure_and_bounded(self):
        self.assertEqual(
            (
                adapter.MAX_GPU_SECONDS,
                adapter.TERMINALIZATION_GRACE_SECONDS,
                adapter.OUTER_TIMEOUT_SECONDS,
            ),
            (1_800, 900, 2_700),
        )
        name, payload = adapter._request_body(settings(), STUDENT)
        self.assertEqual(name, f"dt-train-{STUDENT}")
        job = payload["training_job"]
        self.assertEqual(job["image"]["base_image"], TRAIN_IMAGE)
        self.assertEqual(
            job["compute"],
            {
                "node_count": 1,
                "cpu_count": 8,
                "memory": "64Gi",
                "accelerator": {"accelerator": "H100", "count": 1},
                "availability_model": "dedicated",
            },
        )
        self.assertEqual(
            job["weights"],
            [
                {
                    "source": adapter.STUDENT_MODEL_SOURCE,
                    "mount_location": adapter.STUDENT_MODEL_MOUNT,
                    "allow_patterns": list(adapter.STUDENT_MODEL_ALLOW_PATTERNS),
                }
            ],
        )
        manifest = (
            Path(__file__).parents[1]
            / "models/qwen3-4b-instruct-2507/model.manifest.sha256"
        ).read_text(encoding="utf-8")
        self.assertEqual(
            list(adapter.STUDENT_MODEL_ALLOW_PATTERNS),
            [line.split("  ", 1)[1] for line in manifest.splitlines()],
        )
        environment = job["runtime"]["environment_variables"]
        self.assertEqual(
            {
                key: value
                for key, value in environment.items()
                if key.startswith("MILK_")
            },
            {
                "MILK_CONTROL_STORE_ACCOUNT_ID": "gateway-account",
                "MILK_EXPECTED_CONTROL_STORE_IDENTITY_SHA256": "6" * 64,
                "MILK_CONTROL_STORE_ACCESS_KEY_ID": {"name": "control_access"},
                "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": {"name": "control_secret"},
                "MILK_CONTROL_STORE_SESSION_TOKEN": {"name": "control_session"},
            },
        )
        command = job["runtime"]["start_commands"][0]
        self.assertIn(
            f"/usr/bin/timeout --signal=TERM --kill-after=30s "
            f"{adapter.OUTER_TIMEOUT_SECONDS}s",
            command,
        )
        self.assertLess(
            command.index("hashlib.sha256(raw).hexdigest()"),
            command.index("/opt/dragontales/deploy/student-job.sh"),
        )
        self.assertLess(
            command.index("unset MILK_CONTROL_STORE_ACCOUNT_ID"),
            command.index("/opt/dragontales/deploy/student-job.sh"),
        )
        self.assertEqual(
            shlex.split(command.splitlines()[-1]),
            [
                "exec",
                "/usr/bin/timeout",
                "--signal=TERM",
                "--kill-after=30s",
                f"{adapter.OUTER_TIMEOUT_SECONDS}s",
                "/opt/dragontales/deploy/student-job.sh",
                "train",
                "--config",
                "/tmp/dragontales/gateway.json",
                "--student-job-id",
                "$DRAGONTALES_STUDENT_JOB_ID",
                "--work-dir",
                "/tmp/dragontales-work",
            ],
        )

        branch_name, branch_payload = adapter._request_body(
            settings(), STUDENT, "static_fp8", "7" * 64
        )
        self.assertTrue(branch_name.startswith("dt-static-fp8-"))
        self.assertIn(
            "--variant static_fp8",
            branch_payload["training_job"]["runtime"]["start_commands"][0],
        )
        self.assertEqual(
            branch_payload["training_job"]["image"]["base_image"], BRANCH_IMAGE
        )
        self.assertNotIn("weights", branch_payload["training_job"])

    def test_teacher_request_builder_is_pure_pinned_and_bounded(self):
        name, payload = adapter._teacher_request_body(settings(), teacher_launch())
        self.assertEqual(name, f"dt-teacher-{TEACHER_RUN}")
        job = payload["training_job"]
        self.assertEqual(job["image"]["base_image"], TEACHER_IMAGE)
        self.assertEqual(
            job["weights"],
            [
                {
                    "source": adapter.TEACHER_MODEL_SOURCE,
                    "mount_location": adapter.TEACHER_MODEL_MOUNT,
                    "allow_patterns": list(adapter.TEACHER_MODEL_ALLOW_PATTERNS),
                }
            ],
        )
        manifest = json.loads(
            (
                Path(__file__).parents[1]
                / "teacher/gpt-oss-120b/model.manifest.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            list(adapter.TEACHER_MODEL_ALLOW_PATTERNS),
            [item["path"] for item in manifest["files"]],
        )
        self.assertEqual(
            job["compute"]["accelerator"],
            {"accelerator": "H100", "count": 1},
        )
        environment = job["runtime"]["environment_variables"]
        self.assertEqual(
            {
                key: value
                for key, value in environment.items()
                if key.startswith("MILK_")
            },
            {
                "MILK_CAPTURE_STORE_ACCOUNT_ID": "gateway-account",
                "MILK_EXPECTED_CAPTURE_STORE_IDENTITY_SHA256": "5" * 64,
                "MILK_CAPTURE_STORE_ACCESS_KEY_ID": {"name": "capture_access"},
                "MILK_CAPTURE_STORE_SECRET_ACCESS_KEY": {"name": "capture_secret"},
                "MILK_CAPTURE_STORE_SESSION_TOKEN": {"name": "capture_session"},
                "MILK_CONTROL_STORE_ACCOUNT_ID": "gateway-account",
                "MILK_EXPECTED_CONTROL_STORE_IDENTITY_SHA256": "6" * 64,
                "MILK_CONTROL_STORE_ACCESS_KEY_ID": {"name": "control_access"},
                "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": {"name": "control_secret"},
                "MILK_CONTROL_STORE_SESSION_TOKEN": {"name": "control_session"},
            },
        )
        self.assertNotIn("DRAGONTALES_TEACHER_API_KEY", environment)
        command = job["runtime"]["start_commands"][0]
        self.assertLess(
            command.index("hashlib.sha256(raw).hexdigest()"),
            command.index("/opt/dragontales/job.sh"),
        )
        self.assertLess(
            command.index("unset MILK_CAPTURE_STORE_ACCOUNT_ID"),
            command.index("/opt/dragontales/job.sh"),
        )
        self.assertIn(
            f"--kill-after={adapter.TEACHER_TERMINALIZATION_GRACE_SECONDS}s "
            "3600s",
            command,
        )

    def test_store_identity_wrapper_fails_before_entrypoint_and_removes_identity_inputs(self):
        account = "gateway-account"
        access = "control-access"
        expected = hashlib.sha256(
            (json.dumps(
                {"account_id": account, "access_key_id": access},
                sort_keys=True,
                separators=(",", ":"),
            ) + "\n").encode()
        ).hexdigest()
        command = "\n".join(
            (
                *adapter.store_identity_commands(False),
                '[ -z "${MILK_CONTROL_STORE_ACCOUNT_ID+x}" ]',
                '[ -z "${MILK_EXPECTED_CONTROL_STORE_IDENTITY_SHA256+x}" ]',
                "printf passed",
            )
        )
        environment = {
            "PATH": os.environ["PATH"],
            "MILK_CONTROL_STORE_ACCOUNT_ID": account,
            "MILK_CONTROL_STORE_ACCESS_KEY_ID": access,
            "MILK_EXPECTED_CONTROL_STORE_IDENTITY_SHA256": expected,
        }
        passed = subprocess.run(
            ["/bin/sh", "-c", command],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(passed.returncode, 0, passed.stderr)
        self.assertEqual(passed.stdout, "passed")
        failed = subprocess.run(
            ["/bin/sh", "-c", command],
            env={**environment, "MILK_EXPECTED_CONTROL_STORE_IDENTITY_SHA256": "0" * 64},
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(failed.returncode, 64)
        self.assertEqual(failed.stdout, "")

    def test_provider_response_validator_is_exact(self):
        name = f"dt-train-{STUDENT}"
        self.assertEqual(
            adapter._provider_execution(
                provider_response(name, "train_job_1"),
                PROJECT,
                name,
            ),
            {
                "execution_id": "train_job_1",
                "execution_name": name,
            },
        )
        with self.assertRaisesRegex(ValueError, "identity is invalid"):
            adapter._provider_execution(
                provider_response("wrong-name", "train_job_1"),
                PROJECT,
                name,
            )
        malformed = line({"training_job": {"id": "unbound"}})
        with self.assertRaisesRegex(ValueError, "identity is invalid"):
            adapter._provider_execution(malformed, PROJECT, name)

    def test_image_validation_remains_strict(self):
        self.assertEqual(adapter.immutable_ghcr_image(TRAIN_IMAGE), TRAIN_IMAGE)
        self.assertEqual(adapter.immutable_ghcr_image(BRANCH_IMAGE), BRANCH_IMAGE)
        self.assertEqual(adapter.immutable_image(FIXTURE_IMAGE), FIXTURE_IMAGE)
        for value in (
            "ghcr.io/example/image:latest",
            "ghcr.io/example/image@sha256:" + "A" * 64,
            "example/image@sha256:" + "a" * 64,
        ):
            with self.assertRaisesRegex(ValueError, "immutable|ghcr.io"):
                adapter.immutable_ghcr_image(value)


if __name__ == "__main__":
    unittest.main()
