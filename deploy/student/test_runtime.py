from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import inspect
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_PATH = Path(__file__).resolve().with_name("runtime.py")
SPEC = importlib.util.spec_from_file_location("student_runtime", RUNTIME_PATH)
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)
PRODUCTION_RECIPE_SHA256 = "a43facaa96c84839abe5a9ea14b915f93e5171a5e95a801009565311b941b5a2"
FIXTURE_TRAIN_IMAGE = "fixture/dragontales-student-train@sha256:" + "1" * 64
FIXTURE_BRANCH_IMAGE = "fixture/dragontales-student-branch@sha256:" + "2" * 64


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def line(value):
    return compact(value) + b"\n"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def projection(label):
    return {
        "model": "baseline",
        "messages": [{"role": "user", "content": label}],
    }


def rows(prefix, count, kind):
    values = []
    evaluators = (
        "exact_text_v1",
        "normalized_text_v1",
        "char_similarity_v1",
        "word_similarity_v1",
    )
    for index in range(count):
        label = f"{prefix}-{index}"
        row = {
            "request_sha256": digest(label.encode()),
            "projection": projection(label),
        }
        if kind == "train":
            row["target"] = f"target {label}"
        elif kind == "dev":
            row["evaluator_id"] = evaluators[index % len(evaluators)]
            row["reference"] = f"reference {label}"
        values.append(row)
    return sorted(values, key=lambda row: row["request_sha256"])


def fixture_files(root):
    now = dt.datetime.now(dt.timezone.utc)
    scope = {
        "tenant_id": "00000000-0000-0000-0000-000000000001",
        "project_id": "00000000-0000-0000-0000-000000000002",
        "environment_id": "00000000-0000-0000-0000-000000000003",
        "workload_id": "00000000-0000-0000-0000-000000000004",
        "eval_id": "e" * 64,
    }
    inputs = {
        "schema_version": "dragontales.student-input.v1",
        "scope": scope,
        "teacher_provider_binding_sha256": "2" * 64,
        "train": rows("train", 50, "train"),
        "dev": rows("dev", 73, "dev"),
        "calibration": rows("calibration", 128, "calibration"),
    }
    input_raw = line(inputs)
    definition = {
        "schema_version": "dragontales.student-job-definition.v4",
        "teacher_provider_binding_sha256": "2" * 64,
        "teacher_results": [
            {"teacher_job_id": "3" * 64, "object_sha256": "4" * 64, "bytes": 1}
        ],
        "input_sha256": digest(input_raw),
        "dev_set_sha256": digest(runtime.DEV_SET_DOMAIN + compact(inputs["dev"])),
        "counts": {"train": 50, "dev": 73, "calibration": 128},
        "recipe_sha256": runtime.recipe_sha256(),
        "student_train_runtime_image_reference": FIXTURE_TRAIN_IMAGE,
        "student_branch_runtime_image_reference": FIXTURE_BRANCH_IMAGE,
        "max_train_gpu_seconds": runtime.MAX_TRAIN_GPU_SECONDS,
        "max_branch_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
        "max_total_gpu_seconds": runtime.MAX_TOTAL_GPU_SECONDS,
        "quality": {
            "min_mean_score_bps": 9500,
            "max_mean_loss_vs_bf16_bps": 0,
            "max_p95_latency_ms": 30_000,
        },
        "expires_at": runtime._format_time(now + dt.timedelta(hours=1)),
    }
    claim = {
        "schema_version": "dragontales.student-job-claim.v4",
        "scope": scope,
        "student_job_id": digest(compact(definition)),
        "definition": definition,
        "started_at": runtime._format_time(now - dt.timedelta(seconds=5)),
    }
    claim_path = root / "claim.json"
    input_path = root / "input.json"
    claim_path.write_bytes(compact(claim))
    input_path.write_bytes(input_raw)
    return claim_path, input_path, claim, inputs


def parse_line(path):
    raw = path.read_bytes()
    assert raw.endswith(b"\n") and not raw[:-1].endswith(b"\n")
    value = json.loads(raw)
    assert raw == line(value)
    return value


def retained_model(root, variant="bf16"):
    model = root / "model"
    model.mkdir(mode=0o700)
    (model / "config.json").write_bytes(b"{}\n")
    (model / "model.safetensors").write_bytes(b"retained-model\n")
    inventory, inventory_sha256 = runtime._inventory(model)
    job_id = "a" * 64
    files = [
        {
            "relative_path": item["relative_path"],
            "object_key": f"dt/v3/{'e' * 64}/tenant/project/environment/workload/artifacts/{job_id}/{item['sha256']}",
            "sha256": item["sha256"],
            "bytes": item["bytes"],
        }
        for item in inventory
    ]
    manifest = {
        "schema_version": "dragontales.student-model-manifest.v1",
        "student_job_id": job_id,
        "variant": variant,
        "retained": True,
        "inventory_sha256": inventory_sha256,
        "file_count": len(files),
        "artifact_bytes": sum(item["bytes"] for item in files),
        "files": files,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_bytes(line(manifest))
    manifest_path.chmod(0o400)
    return model, manifest_path, manifest


def fixture_fanout(root, claim, train_output):
    train_raw = (train_output / "result.json").read_bytes()
    train = json.loads(train_raw)
    branches = []
    for variant in runtime.VARIANTS:
        definition = {
            "schema_version": "dragontales.student-branch-definition.v2",
            "student_job_id": claim["student_job_id"],
            "variant": variant,
            "train_result_sha256": digest(train_raw),
            "recipe_sha256": claim["definition"]["recipe_sha256"],
            "student_branch_runtime_image_reference": claim["definition"][
                "student_branch_runtime_image_reference"
            ],
            "max_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
            "expires_at": claim["definition"]["expires_at"],
        }
        branches.append(
            {
                "schema_version": "dragontales.student-branch-claim.v2",
                "branch_id": digest(compact(definition)),
                "variant": variant,
                "max_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
            }
        )
    fanout = {
        "schema_version": "dragontales.student-fanout-claim.v4",
        "scope": claim["scope"],
        "student_job_id": claim["student_job_id"],
        "train_result_sha256": digest(train_raw),
        "recipe_sha256": claim["definition"]["recipe_sha256"],
        "student_branch_runtime_image_reference": claim["definition"][
            "student_branch_runtime_image_reference"
        ],
        "max_total_gpu_seconds": runtime.MAX_TOTAL_GPU_SECONDS,
        "created_at": train["finished_at"],
        "expires_at": claim["definition"]["expires_at"],
        "branches": branches,
    }
    stage = root / "branch-input"
    model = stage / "merged-model/model"
    model.mkdir(parents=True)
    artifact = train_output / "artifact"
    reference = train["outcome"]["merged_model_manifest"]
    manifest_raw = (artifact / reference["sha256"]).read_bytes()
    manifest_path = stage / "merged-model/model-manifest.json"
    manifest_path.write_bytes(manifest_raw)
    manifest_path.chmod(0o400)
    for item in json.loads(manifest_raw)["files"]:
        destination = model / item["relative_path"]
        shutil.copyfile(artifact / item["sha256"], destination)
        destination.chmod(0o400)
    model.chmod(0o500)
    (stage / "merged-model").chmod(0o500)
    train_path = stage / "train-result.json"
    train_path.write_bytes(train_raw)
    train_path.chmod(0o400)
    fanout_path = stage / "fanout-claim.json"
    fanout_path.write_bytes(compact(fanout))
    fanout_path.chmod(0o400)
    return train_path, fanout_path, model, manifest_path, fanout


def gpu_probe(stage):
    image = (
        {
            "stage": "train",
            "schema_version": "dragontales.student-train-image-probe.v1",
            "platform": "linux/amd64",
            "prime_rl_version": "0.9.0",
            "accelerate_version": "1.14.0",
            "peft_version": "0.20.0",
            "torch_cuda_version": "12.8",
        }
        if stage == "train"
        else {
            "stage": "branch",
            "schema_version": "dragontales.h100-fp8-image-probe.v2",
            "platform": "linux/amd64",
            "vllm_version": "0.28.0",
            "vllm_metadata_sha256": "5" * 64,
            "compressed_tensors_version": "0.18.0",
            "compressed_tensors_wheel_sha256": "6" * 64,
            "fp8_dynamic_supported": True,
            "fp8_static_supported": True,
        }
    )
    return {
        "device": {
            "uuid": "GPU-00000000-0000-0000-0000-000000000001",
            "cuda": "12.8" if stage == "train" else "12.9",
            "name": "NVIDIA H100 80GB HBM3",
            "compute_capability": [9, 0],
            "total_memory_bytes": 80 * 1024**3,
        },
        "runtime": image,
    }


def test_kernel(variant):
    return {
        "variant": variant,
        "model_verification": {
            "schema_version": "dragontales.h100-model-verification.v2",
            "model_type": "qwen3",
            "variant": "bf16" if variant == "bf16" else variant.removesuffix("_fp8"),
            "shards": 1,
        },
        "startup_log_sha256": digest((variant + "-startup").encode()),
        "kernel_selection": None
        if variant == "bf16"
        else "Selected CutlassFP8ScaledMMLinearKernel for CompressedTensorsW8A8Fp8",
        "occurrences": 0 if variant == "bf16" else 1,
    }


@contextlib.contextmanager
def mocked_train_compute(root):
    model = root / "base-model"
    model.mkdir(mode=0o700)
    (model / "config.json").write_bytes(b"{}\n")
    run = root / "runs/run"

    def prepare(_input_raw, _inputs, _working, _model_manifest_sha256):
        adapter = run / "adapter"
        adapter.mkdir(mode=0o700, parents=True)
        (adapter / "adapter_config.json").write_bytes(b"{}\n")
        (adapter / "adapter_model.safetensors").write_bytes(b"adapter\n")

    def subprocess(_interpreter, command, arguments, _log, _timeout):
        if command != "_merge":
            raise AssertionError("unexpected train subprocess")
        output = Path(arguments[-1])
        output.mkdir(mode=0o700)
        (output / "config.json").write_bytes(b"{}\n")
        (output / "model.safetensors").write_bytes(b"merged-model\n")

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runtime, "MODEL", model))
        stack.enter_context(mock.patch.object(runtime, "RUN", run))
        stack.enter_context(mock.patch.object(runtime, "_verify_base_model", return_value="7" * 64))
        stack.enter_context(mock.patch.object(runtime, "_run_checked"))
        stack.enter_context(mock.patch.object(runtime, "_gpu_probe", return_value=gpu_probe("train")))
        stack.enter_context(mock.patch.object(runtime, "_prepare_training", side_effect=prepare))
        stack.enter_context(mock.patch.object(runtime, "_subprocess_stage", side_effect=subprocess))
        yield


@contextlib.contextmanager
def mocked_branch_compute():
    def subprocess(_interpreter, command, arguments, _log, _timeout):
        if command != "_quantize":
            raise AssertionError("unexpected branch subprocess")
        output = Path(arguments[-1])
        output.mkdir(mode=0o700)
        (output / "config.json").write_bytes(b"{}\n")
        (output / "model.safetensors").write_bytes(b"quantized-model\n")

    def evaluate(variant, _model, claim, dev, _working):
        latency = {"bf16": 30, "dynamic_fp8": 20, "static_fp8": 10}[variant]
        receipt, summary = runtime._receipt(
            variant,
            claim,
            dev,
            [
                (
                    1.0,
                    latency,
                    None,
                    digest((variant + row["reference"]).encode()),
                    row["reference"],
                )
                for row in dev
            ],
        )
        return receipt, summary, test_kernel(variant)

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runtime, "_h100", return_value=object()))
        stack.enter_context(mock.patch.object(runtime, "_verify_retained_model"))
        stack.enter_context(mock.patch.object(runtime, "_gpu_probe", return_value=gpu_probe("branch")))
        stack.enter_context(mock.patch.object(runtime, "_subprocess_stage", side_effect=subprocess))
        stack.enter_context(mock.patch.object(runtime, "_evaluate", side_effect=evaluate))
        yield


class StudentRuntimeTest(unittest.TestCase):
    def test_artifact_namespace_is_bound_to_the_claim_eval(self):
        with tempfile.TemporaryDirectory() as temporary:
            _claim_path, _input_path, claim, _inputs = fixture_files(Path(temporary))
            reference = {
                "object_key": runtime._artifact_key(claim, "a" * 64),
                "sha256": "a" * 64,
                "bytes": 1,
            }
            self.assertEqual(
                runtime._artifact_ref(reference, "student artifact", claim),
                reference,
            )
            reference["object_key"] = reference["object_key"].replace(
                claim["scope"]["eval_id"],
                "f" * 64,
                1,
            )
            with self.assertRaisesRegex(ValueError, "object key"):
                runtime._artifact_ref(reference, "student artifact", claim)

    def test_v4_split_images_and_budget_wire_are_exact(self):
        self.assertEqual(
            (
                runtime.MAX_TRAIN_GPU_SECONDS,
                runtime.MAX_BRANCH_GPU_SECONDS,
                runtime.MAX_TOTAL_GPU_SECONDS,
                runtime.TERMINALIZATION_GRACE_SECONDS,
            ),
            (1_800, 1_800, 7_200, 900),
        )
        with tempfile.TemporaryDirectory(prefix="dragontales-budget-wire-") as temporary:
            claim_path, _input_path, claim, _inputs = fixture_files(Path(temporary))
            definition = claim["definition"]
            self.assertEqual(tuple(definition), runtime.DEFINITION_KEYS)
            self.assertEqual(
                (
                    definition["max_train_gpu_seconds"],
                    definition["max_branch_gpu_seconds"],
                    definition["max_total_gpu_seconds"],
                ),
                (
                    runtime.MAX_TRAIN_GPU_SECONDS,
                    runtime.MAX_BRANCH_GPU_SECONDS,
                    runtime.MAX_TOTAL_GPU_SECONDS,
                ),
            )

            mixed = json.loads(claim_path.read_bytes())
            mixed["definition"]["max_gpu_seconds"] = runtime.MAX_TRAIN_GPU_SECONDS
            with self.assertRaisesRegex(ValueError, "wrong typed fields"):
                runtime._validate_claim(compact(mixed), False)

            untyped = json.loads(claim_path.read_bytes())
            untyped["definition"]["max_total_gpu_seconds"] = float(
                runtime.MAX_TOTAL_GPU_SECONDS
            )
            untyped["student_job_id"] = digest(compact(untyped["definition"]))
            with self.assertRaisesRegex(ValueError, "outside its fixed range"):
                runtime._validate_claim(compact(untyped), False)

            mutable = json.loads(claim_path.read_bytes())
            mutable["definition"]["student_train_runtime_image_reference"] = (
                "fixture/dragontales-student-train:latest"
            )
            mutable["student_job_id"] = digest(compact(mutable["definition"]))
            with self.assertRaisesRegex(ValueError, "immutable"):
                runtime._validate_claim(compact(mutable), False)

            same_digest = json.loads(claim_path.read_bytes())
            same_digest["definition"]["student_branch_runtime_image_reference"] = (
                "fixture/dragontales-student-branch@sha256:" + "1" * 64
            )
            same_digest["student_job_id"] = digest(compact(same_digest["definition"]))
            with self.assertRaisesRegex(ValueError, "distinct digests"):
                runtime._validate_claim(compact(same_digest), False)

            reordered = json.loads(claim_path.read_bytes())
            image = reordered["definition"].pop("student_train_runtime_image_reference")
            reordered["definition"]["student_train_runtime_image_reference"] = image
            reordered["student_job_id"] = digest(compact(reordered["definition"]))
            with self.assertRaisesRegex(ValueError, "wrong typed fields"):
                runtime._validate_claim(compact(reordered), False)

    def test_artifact_roles_are_exact_and_operations_are_separated(self):
        with tempfile.TemporaryDirectory(prefix="dragontales-student-role-") as temporary:
            role = Path(temporary) / "role"
            role.write_text("student-train\n")
            role.chmod(0o444)
            with mock.patch.object(runtime, "ARTIFACT_ROLE_PATH", role), mock.patch.object(
                runtime, "IMAGE_OWNER_UID", os.geteuid()
            ):
                self.assertEqual(runtime._artifact_role(), "student-train")
                role.chmod(0o644)
                role.write_text("student-branch\n")
                role.chmod(0o444)
                self.assertEqual(runtime._artifact_role(), "student-branch")

        missing = Path("/not-read-after-role-rejection")
        with mock.patch.object(
            runtime, "_artifact_role", return_value="student-branch"
        ), self.assertRaisesRegex(ValueError, "student-train operation is forbidden"):
            runtime.train_stage(missing, missing, missing)
        with mock.patch.object(
            runtime, "_artifact_role", return_value="student-train"
        ), self.assertRaisesRegex(ValueError, "student-branch operation is forbidden"):
            runtime.branch_stage(
                missing,
                missing,
                missing,
                missing,
                missing,
                missing,
                "bf16",
                missing,
            )
        with mock.patch.object(
            runtime, "_artifact_role", return_value="student-train"
        ), self.assertRaisesRegex(ValueError, "student-branch operation is forbidden"):
            runtime.serve(missing, missing, "logical-model", missing)

    def test_compute_alarm_reserves_terminalization_grace(self):
        now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self.assertEqual(
            runtime._compute_alarm_seconds(
                now
                + dt.timedelta(
                    seconds=runtime.TERMINALIZATION_GRACE_SECONDS
                    + runtime.MAX_TRAIN_GPU_SECONDS
                    + 1
                ),
                runtime.MAX_TRAIN_GPU_SECONDS,
                now,
            ),
            runtime.MAX_TRAIN_GPU_SECONDS,
        )
        self.assertEqual(
            runtime._compute_alarm_seconds(
                now
                + dt.timedelta(
                    seconds=runtime.TERMINALIZATION_GRACE_SECONDS + 37, microseconds=900_000
                ),
                runtime.MAX_BRANCH_GPU_SECONDS,
                now,
            ),
            37,
        )
        with self.assertRaisesRegex(TimeoutError, "no compute time"):
            runtime._compute_alarm_seconds(
                now + dt.timedelta(seconds=runtime.TERMINALIZATION_GRACE_SECONDS),
                runtime.MAX_BRANCH_GPU_SECONDS,
                now,
            )

    def test_stages_disarm_compute_alarm_before_terminalization(self):
        with tempfile.TemporaryDirectory(prefix="dragontales-stage-alarm-") as temporary:
            root = Path(temporary)
            claim_path, input_path, claim, _inputs = fixture_files(root)
            events = []
            write_terminal = runtime._write_terminal

            def budget(_expires, stage_max, now=None):
                self.assertIsNone(now)
                events.append(("budget", stage_max))
                return 17 if sum(event[0] == "budget" for event in events) == 1 else 23

            def terminal(output, store, result):
                events.append(("terminal", result["schema_version"]))
                write_terminal(output, store, result)

            train_output = root / "train"
            with mocked_train_compute(root), mock.patch.object(
                runtime, "_compute_alarm_seconds", side_effect=budget
            ), mock.patch.object(
                runtime.signal,
                "alarm",
                side_effect=lambda seconds: events.append(("alarm", seconds)),
            ), mock.patch.object(
                runtime, "_write_terminal", side_effect=terminal
            ), mock.patch.object(
                runtime, "_artifact_role", return_value="student-train"
            ):
                runtime.train_stage(claim_path, input_path, train_output)

            train_path, fanout_path, model, manifest, _fanout = fixture_fanout(
                root, claim, train_output
            )
            branch_output = root / "branch"
            with mocked_branch_compute(), mock.patch.object(
                runtime, "_compute_alarm_seconds", side_effect=budget
            ), mock.patch.object(
                runtime.signal,
                "alarm",
                side_effect=lambda seconds: events.append(("alarm", seconds)),
            ), mock.patch.object(
                runtime, "_write_terminal", side_effect=terminal
            ), mock.patch.object(
                runtime, "_artifact_role", return_value="student-branch"
            ):
                runtime.branch_stage(
                    claim_path,
                    input_path,
                    train_path,
                    fanout_path,
                    model,
                    manifest,
                    "bf16",
                    branch_output,
                )

        self.assertEqual(
            events,
            [
                ("budget", runtime.MAX_TRAIN_GPU_SECONDS),
                ("alarm", 17),
                ("alarm", 0),
                ("terminal", "dragontales.student-train-result.v1"),
                ("alarm", 0),
                ("budget", runtime.MAX_BRANCH_GPU_SECONDS),
                ("alarm", 23),
                ("alarm", 0),
                ("terminal", "dragontales.student-branch-result.v1"),
                ("alarm", 0),
            ],
        )

    def test_developer_role_and_reasoning_parser_are_train_eval_aligned(self):
        source = {
            "model": "baseline",
            "messages": [
                {"role": "developer", "content": "Preserve this instruction exactly."},
                {"role": "user", "content": "Question"},
            ],
        }
        normalized = runtime._model_messages(source)
        self.assertEqual([item["role"] for item in normalized], ["system", "user"])
        self.assertEqual(
            [item["content"] for item in normalized],
            [item["content"] for item in source["messages"]],
        )

        captured = []

        class Contract:
            @staticmethod
            def _source_messages(projection):
                captured.append(projection["messages"])
                return list(projection["messages"])

            @staticmethod
            def package_sha256(_path):
                return "a" * 64

            @staticmethod
            def _config(*_args):
                return b"config\n"

        with tempfile.TemporaryDirectory(prefix="dragontales-role-parity-") as temporary:
            root = Path(temporary)
            model = root / "model"
            model.mkdir()
            (model / "config.json").write_bytes(b"{}\n")
            run = root / "run"

            def checked(_command, _log, _timeout, _environment=None):
                adapter = run / "adapter"
                if not adapter.exists():
                    adapter.mkdir(mode=0o700)
                    (adapter / ".finished").touch()
                    (adapter / "adapter_config.json").write_bytes(b"{}\n")
                    (adapter / "adapter_model.safetensors").write_bytes(b"model\n")

            inputs = {
                "teacher_provider_binding_sha256": "b" * 64,
                "train": [
                    {
                        "request_sha256": "c" * 64,
                        "projection": source,
                        "target": "Answer",
                    }
                ],
            }
            with mock.patch.object(runtime, "MODEL", model), mock.patch.object(
                runtime, "RUN", run
            ), mock.patch.object(runtime, "_load_module", return_value=Contract), mock.patch.object(
                runtime, "_run_checked", side_effect=checked
            ):
                runtime._prepare_training(b"input\n", inputs, root, "d" * 64)

        request = {}

        def respond(_url, payload=None, timeout=60):
            request.update(payload)
            value = {
                "model": runtime.MODEL_ALIAS,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "Answer",
                            "reasoning_content": "",
                            "refusal": None,
                            "tool_calls": None,
                            "function_call": None,
                        },
                    }
                ],
            }
            return line(value), value

        row = {
            "projection": source,
            "evaluator_id": "exact_text_v1",
            "reference": "Answer",
        }
        with mock.patch.object(runtime, "_request_json", side_effect=respond):
            score, _latency, error, _digest, candidate = runtime._call_dev("http://unit", row)
        self.assertEqual(captured, [normalized])
        self.assertEqual(request["messages"], source["messages"])
        self.assertEqual(request["chat_template_kwargs"], {"enable_thinking": True})
        template = runtime._chat_template()
        self.assertIn("message.role == 'developer'", template)
        self.assertIn("role = 'system' if", template)
        self.assertEqual((score, error, candidate), (1.0, None, "Answer"))
        with self.assertRaisesRegex(ValueError, "candidate text"):
            runtime._response_text(
                {
                    "model": runtime.MODEL_ALIAS,
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "<think>hidden</think>Answer",
                            },
                        }
                    ],
                }
            )

    def test_model_subprocess_environment_scrubs_credentials_and_forces_offline(self):
        secrets = {
            "ANTHROPIC_AUTH_TOKEN": "secret",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "CF_API_TOKEN": "secret",
            "CLOUDFLARE_API_TOKEN": "secret",
            "DATABASE_URL": "https://user:secret@example.invalid",
            "MILK_CARTON_CONFIG_JSON": "secret",
            "MILK_CONTROL_STORE_ACCESS_KEY_ID": "secret",
            "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": "secret",
            "GOOGLE_APPLICATION_CREDENTIALS": "/secret.json",
            "KUBECONFIG": "/secret",
            "MILK_TEACHER_KEY": "secret",
            "MODAL_TOKEN_SECRET": "secret",
            "OPENAI_API_KEY": "secret",
            "R2_SECRET_ACCESS_KEY": "secret",
            "SSH_AUTH_SOCK": "/secret.sock",
        }
        inherited = {
            **secrets,
            "CUDA_VISIBLE_DEVICES": "0",
            "NCCL_DEBUG": "WARN",
            "PATH": os.environ["PATH"],
        }
        with tempfile.TemporaryDirectory(prefix="dragontales-student-env-") as temporary, mock.patch.dict(
            runtime.os.environ, inherited, clear=True
        ):
            log = Path(temporary) / "environment.json"
            runtime._run_checked(
                [
                    sys.executable,
                    "-c",
                    "import json,os; print(json.dumps(dict(os.environ),sort_keys=True))",
                ],
                log,
                10,
            )
            observed = json.loads(log.read_text())

        self.assertTrue(secrets.keys().isdisjoint(observed))
        self.assertEqual(observed["CUDA_VISIBLE_DEVICES"], "0")
        self.assertEqual(observed["NCCL_DEBUG"], "WARN")
        for name, value in runtime.MODEL_OFFLINE_ENV.items():
            self.assertEqual(observed[name], value)

    def test_dev_model_server_receives_scrubbed_environment(self):
        class FakeRuntime:
            KERNEL_SELECTION = "fixed-kernel"
            MAX_LOG_BYTES = 1024

            @staticmethod
            def verify_model(_model):
                return {"variant": "bf16"}

            @staticmethod
            def serving_argv():
                return ["/fixed/vllm", "--host", "127.0.0.1"]

        with tempfile.TemporaryDirectory(prefix="dragontales-student-eval-") as temporary, mock.patch.dict(
            runtime.os.environ,
            {
                "AWS_SECRET_ACCESS_KEY": "secret",
                "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": "secret",
                "OPENAI_API_KEY": "secret",
            },
        ), mock.patch.object(runtime, "_h100", return_value=FakeRuntime()), mock.patch.object(
            runtime.subprocess, "Popen"
        ) as launch, mock.patch.object(
            runtime, "_wait_ready"
        ), mock.patch.object(runtime, "_stop"), mock.patch.object(
            runtime, "_receipt", return_value=({}, {})
        ), mock.patch.object(
            runtime, "_read", return_value=b"startup\n"
        ), mock.patch.object(
            runtime, "_chat_template", return_value="fixed"
        ):
            runtime._evaluate("bf16", Path(temporary) / "model", {}, [], Path(temporary))

        environment = launch.call_args.kwargs["env"]
        self.assertEqual(
            launch.call_args.args[0][-2:], ["--reasoning-parser", "qwen3"]
        )
        self.assertEqual(
            launch.call_args.args[0][
                launch.call_args.args[0].index("--chat-template") + 1
            ],
            str(runtime.CHAT_TEMPLATE),
        )
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("MILK_CONTROL_STORE_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("OPENAI_API_KEY", environment)
        self.assertEqual(environment["TRANSFORMERS_OFFLINE"], "1")
        command = launch.call_args.args[0]
        self.assertEqual(command[command.index("--host") + 1], "127.0.0.1")

    def test_image_entrypoint_and_recipe_identities_are_exact(self):
        train_dockerfile = (ROOT / "deploy/student-train/Dockerfile").read_text()
        branch_dockerfile = (ROOT / "deploy/student-branch/Dockerfile").read_text()
        train_provenance_raw = (
            ROOT / "deploy/student-train/provenance.json"
        ).read_bytes()
        branch_provenance_raw = (
            ROOT / "deploy/student-branch/provenance.json"
        ).read_bytes()
        train_provenance = json.loads(train_provenance_raw)
        branch_provenance = json.loads(branch_provenance_raw)
        prime_license = (ROOT / "deploy/licenses/prime-rl-v0.9.0.LICENSE").read_bytes()
        verifier = runtime._load_module(
            "dragontales_test_qwen_image_release",
            ROOT / "deploy/models/qwen3-4b-instruct-2507/verify.py",
        )
        self.assertFalse((ROOT / "deploy/student/Dockerfile").exists())
        self.assertFalse((ROOT / "deploy/student/provenance.json").exists())

        authority_files = {
            "student-job.sh",
            "student-run.sh",
            "student/runtime.py",
            "student/qwen3-chat-template.jinja",
            "student-train/Dockerfile",
            "student-train/provenance.json",
            "student-branch/Dockerfile",
            "student-branch/provenance.json",
            "adapter_train/__init__.py",
            "adapter_train/contract.py",
            "adapter_train/sft_entrypoint.py",
            "h100-fp8-runtime/runtime.py",
            "licenses/prime-rl-v0.9.0.LICENSE",
            "models/qwen3-4b-instruct-2507/profile.json",
            "models/qwen3-4b-instruct-2507/model.manifest.sha256",
            "models/qwen3-4b-instruct-2507/verify.py",
        }
        self.assertEqual(set(runtime.AUTHORITY_FILES), authority_files)
        expected_authority = {f"deploy/{path}" for path in authority_files}
        for dockerfile, role in (
            (train_dockerfile, "student-train"),
            (branch_dockerfile, "student-branch"),
        ):
            sources = set(re.findall(r"deploy/[A-Za-z0-9_./-]+", dockerfile))
            self.assertEqual(sources, expected_authority)
            self.assertIn(
                "ARG MILK_GATEWAY_IMAGE\nFROM ${MILK_GATEWAY_IMAGE} AS gateway",
                dockerfile,
            )
            self.assertIn(
                "COPY --from=gateway --chmod=0555 /usr/local/bin/milk-carton "
                "/usr/local/bin/milk-carton",
                dockerfile,
            )
            self.assertIn('test "${#gateway_digest}" -eq 64', dockerfile)
            self.assertIn('case "$gateway_digest" in *[!0-9a-f]*) exit 1', dockerfile)
            self.assertIn(f"printf '%s\\n' {role} > /usr/share/milk/student-artifact-role", dockerfile)
            self.assertIn('ENTRYPOINT ["/opt/dragontales/deploy/student-job.sh"]', dockerfile)
            self.assertIn("RUN test -x /usr/bin/setpriv", dockerfile)
            self.assertIn("USER 0:0", dockerfile)
            self.assertIn("FROM scratch AS wheels", dockerfile)
            expected_runtime_wheel_copies = 1 if role == "student-branch" else 0
            self.assertEqual(
                dockerfile.count("COPY --from=wheels"),
                expected_runtime_wheel_copies,
            )
            self.assertNotIn("FROM rust:", dockerfile)
            self.assertNotIn("cargo build", dockerfile)
            self.assertNotIn("COPY crates/", dockerfile)
            self.assertNotIn("test_runtime.py", dockerfile)
            self.assertNotIn("deploy/student/Dockerfile", dockerfile)
            self.assertNotIn("huggingface.co/Qwen", dockerfile)
            self.assertNotIn("model.safetensors", dockerfile)

        self.assertIn("ghcr.io/primeintellect-ai/prime-rl@sha256:", train_dockerfile)
        self.assertNotIn("vllm/vllm-openai@sha256:", train_dockerfile)
        self.assertLess(
            train_dockerfile.index("USER 0:0"),
            train_dockerfile.index('RUN gateway_repository="${MILK_GATEWAY_IMAGE%@sha256:*}"'),
        )
        self.assertIn('assert version("prime-rl") == "0.9.0"', train_dockerfile)
        self.assertIn('assert torch.version.cuda == "12.8"', train_dockerfile)
        self.assertNotIn("llmcompressor", train_dockerfile)
        self.assertNotIn("compressed_tensors", train_dockerfile)

        self.assertIn("vllm/vllm-openai@sha256:", branch_dockerfile)
        self.assertNotIn("ghcr.io/primeintellect-ai/prime-rl@sha256:", branch_dockerfile)
        self.assertIn('assert version("vllm") == "0.28.0+cu129"', branch_dockerfile)
        self.assertIn('assert torch.version.cuda == "12.9"', branch_dockerfile)
        self.assertIsNone(
            re.search(r"(?<![A-Za-z0-9_.-])/model(?:/|\s|$)", branch_dockerfile)
        )
        self.assertIn("llmcompressor-0.13.0", branch_dockerfile)
        self.assertIn("compressed_tensors-0.18.0", branch_dockerfile)
        self.assertIn(
            "/opt/compressed_tensors-0.18.0-py3-none-any.whl",
            branch_dockerfile,
        )
        self.assertEqual(runtime.LLM_COMPRESSOR_VERSION, "0.13.0")
        self.assertEqual(runtime.COMPRESSED_TENSORS_VERSION, "0.18.0")

        self.assertEqual(
            hashlib.sha256(prime_license).hexdigest(),
            "f5118b9c9e98b0f4076214ee13f68d5f73c13b077c44544cb9a0c4ed9155065c",
        )
        self.assertEqual(len(prime_license), 11_695)
        self.assertEqual(
            hashlib.sha256(train_provenance_raw).hexdigest(),
            "4d863132519b4246cefdbd0773190abfe9c6c485573260d1cf28965791985203",
        )
        self.assertEqual(
            hashlib.sha256(branch_provenance_raw).hexdigest(),
            "86a0d1814d3dc46d1750fb392c64464433aaf11ede6e63aa9db30b82e5bd2643",
        )
        self.assertEqual(
            train_provenance["schema_version"],
            "milk.student-train-image-provenance.v1",
        )
        self.assertEqual(train_provenance["artifact_role"], "student-train")
        self.assertEqual(train_provenance["platform"], "linux/amd64")
        self.assertEqual(
            train_provenance["base_image"]["source_revision"],
            "ab5de8fff44b2c4a5c85e24b6e6e3f7d57eee7b1",
        )
        self.assertEqual(
            train_provenance["model"]["revision"],
            verifier.EXPECTED_PROFILE["model_revision"],
        )
        self.assertEqual(
            train_provenance["model"]["distribution"], "external_read_only_mount"
        )
        self.assertEqual(train_provenance["model"]["mount_path"], str(runtime.MODEL))
        self.assertIs(train_provenance["model"]["verified_before_training"], True)
        self.assertEqual(
            [wheel["name"] for wheel in train_provenance["wheels"]],
            ["accelerate", "peft"],
        )

        self.assertEqual(
            branch_provenance["schema_version"],
            "milk.student-branch-image-provenance.v1",
        )
        self.assertEqual(branch_provenance["artifact_role"], "student-branch")
        self.assertEqual(branch_provenance["platform"], "linux/amd64")
        self.assertEqual(
            branch_provenance["base_image"]["source_revision"],
            "2cf0a6915ce544dc493a0990f2ea38d81601128a",
        )
        self.assertEqual(
            branch_provenance["model_distribution"],
            "gateway_materialized_candidate_only",
        )
        self.assertIsNone(branch_provenance["qwen_base_mount"])
        for role, provenance, dockerfile in (
            ("student-train", train_provenance, train_dockerfile),
            ("student-branch", branch_provenance, branch_dockerfile),
        ):
            names = [wheel["name"] for wheel in provenance["wheels"]]
            self.assertEqual(names, list(dict.fromkeys(names)))
            for wheel in provenance["wheels"]:
                self.assertIn(f"--checksum=sha256:{wheel['sha256']}", dockerfile)
                expected_occurrences = (
                    3
                    if role == "student-branch"
                    and wheel["name"] == "compressed-tensors"
                    else 2
                )
                self.assertEqual(
                    dockerfile.count(f"/wheels/{wheel['artifact']}"),
                    expected_occurrences,
                )
        retained = [
            wheel
            for wheel in branch_provenance["wheels"]
            if wheel.get("retained_for_runtime_probe") is True
        ]
        self.assertEqual(
            [wheel["name"] for wheel in retained],
            ["compressed-tensors"],
        )

        train_source = inspect.getsource(runtime.train_stage)
        self.assertLess(
            train_source.index("model_manifest_sha256 = _verify_base_model()"),
            train_source.index("_gpu_probe(working)"),
        )
        self.assertLess(
            train_source.index('"parity"'),
            train_source.index("_gpu_probe(working)"),
        )
        self.assertEqual(runtime.recipe_sha256(), PRODUCTION_RECIPE_SHA256)

    def test_quantization_recipes_are_literal(self):
        dynamic = runtime._quant_recipe("dynamic_fp8")
        static = runtime._quant_recipe("static_fp8")
        self.assertEqual(
            dynamic,
            {
                "schema_version": "dragontales.h100-fp8-recipe.v1",
                "variant": "dynamic",
                "entrypoint": "oneshot",
                "modifier": "QuantizationModifier",
                "scheme": "FP8_DYNAMIC",
                "targets": ["Linear"],
                "ignore": ["lm_head"],
                "activation_observer": None,
                "batch_size": 1,
                "num_calibration_samples": 0,
                "shuffle_calibration_samples": False,
                "max_sequence_length": 4096,
                "save_compressed": True,
            },
        )
        self.assertEqual(static["variant"], "static")
        self.assertEqual(static["scheme"], "FP8")
        self.assertEqual(static["activation_observer"], "static_minmax")
        self.assertEqual(static["num_calibration_samples"], 128)

    def test_base_model_tree_is_exact_regular_release(self):
        verifier = runtime._load_module(
            "dragontales_test_qwen_release",
            ROOT / "deploy/models/qwen3-4b-instruct-2507/verify.py",
        )
        with tempfile.TemporaryDirectory(prefix="dragontales-base-model-") as temporary:
            model = Path(temporary) / "model"
            model.mkdir(mode=0o700)
            payload = b"exact-model-file\n"
            item = model / "model.safetensors"
            item.write_bytes(payload)
            files = ((item.name, digest(payload), len(payload), "bytes"),)
            with mock.patch.object(verifier, "verify", return_value={}), mock.patch.object(
                verifier, "FILES", files
            ), mock.patch.object(verifier, "MODEL_OWNER_UID", os.geteuid()):
                verifier.verify_tree(model)
                extra = model / "extra"
                extra.write_bytes(b"extra\n")
                with self.assertRaisesRegex(ValueError, "exact Qwen inventory"):
                    verifier.verify_tree(model)
                extra.unlink()
                linked = model.parent / "linked"
                os.link(item, linked)
                with self.assertRaisesRegex(ValueError, "regular file"):
                    verifier.verify_tree(model)

    @mock.patch.object(runtime, "_artifact_role", return_value="student-branch")
    def test_retained_model_and_serve_command_are_fail_closed(self, _role):
        real_h100 = runtime._h100()

        class FakeRuntime:
            def __init__(self, executable):
                self.VLLM = executable
                self.hardware_verified = False
                self.variant = "bf16"

            @staticmethod
            def _api_key(path):
                return real_h100._api_key(path)

            @staticmethod
            def _read_regular(path, maximum, mode=None):
                return real_h100._read_regular(path, maximum, mode)

            @staticmethod
            def image_probe():
                return {"fp8_dynamic_supported": True, "fp8_static_supported": True}

            def verify_hardware(self):
                self.hardware_verified = True

            def verify_model(self, _model):
                return {"variant": self.variant}

        with tempfile.TemporaryDirectory(prefix="dragontales-retained-model-") as temporary:
            root = Path(temporary)
            model, manifest_path, manifest = retained_model(root)
            key = root / "api-key"
            key.write_text("k" * 32)
            key.chmod(0o400)
            vllm = root / "vllm"
            vllm.write_text("#!/bin/sh\nexit 1\n")
            vllm.chmod(0o500)
            fixed = FakeRuntime(vllm)
            for declared, observed in (
                ("bf16", "bf16"),
                ("dynamic_fp8", "dynamic"),
                ("static_fp8", "static"),
            ):
                manifest["variant"] = declared
                manifest_path.chmod(0o600)
                manifest_path.write_bytes(line(manifest))
                manifest_path.chmod(0o400)
                fixed.variant = observed
                runtime._verify_retained_model(model, manifest_path, fixed)
            manifest["variant"] = "bf16"
            manifest_path.chmod(0o600)
            manifest_path.write_bytes(line(manifest))
            manifest_path.chmod(0o400)
            fixed.variant = "bf16"
            with mock.patch.dict(
                runtime.os.environ,
                {
                    "AWS_SECRET_ACCESS_KEY": "secret",
                    "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": "secret",
                    "MODAL_TOKEN_SECRET": "secret",
                },
            ), mock.patch.object(runtime, "_h100", return_value=fixed), mock.patch.object(
                runtime, "IMAGE_OWNER_UID", os.geteuid()
            ), mock.patch.object(runtime.os, "execve") as execute:
                runtime.serve(model, manifest_path, "logical-model", key)
            self.assertTrue(fixed.hardware_verified)
            argv = execute.call_args.args[1]
            self.assertEqual(argv[0:3], [str(vllm), "serve", str(model)])
            self.assertEqual(argv[argv.index("--host") + 1], "0.0.0.0")
            self.assertEqual(argv[argv.index("--port") + 1], "8000")
            self.assertEqual(argv[argv.index("--served-model-name") + 1], "logical-model")
            self.assertEqual(argv[argv.index("--chat-template") + 1], str(runtime.CHAT_TEMPLATE))
            self.assertEqual(argv[argv.index("--reasoning-parser") + 1], "qwen3")
            environment = execute.call_args.args[2]
            self.assertEqual(environment["VLLM_API_KEY"], "k" * 32)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("MILK_CONTROL_STORE_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("MODAL_TOKEN_SECRET", environment)

            with mock.patch.dict(
                runtime.os.environ,
                {
                    "BASETEN_API_KEY": "provider-secret",
                    "MILK_CONTROL_STORE_ACCESS_KEY_ID": "r2-secret",
                },
            ), mock.patch.object(runtime, "_h100", return_value=fixed), mock.patch.object(
                runtime, "IMAGE_OWNER_UID", os.geteuid()
            ), mock.patch.object(runtime.os, "execve") as execute:
                runtime.serve(
                    model,
                    manifest_path,
                    "logical-model",
                    None,
                    trusted_ingress_auth=True,
                )
            environment = execute.call_args.args[2]
            self.assertNotIn("VLLM_API_KEY", environment)
            self.assertNotIn("BASETEN_API_KEY", environment)
            self.assertNotIn("MILK_CONTROL_STORE_ACCESS_KEY_ID", environment)

            for key_file, trusted in ((None, False), (key, True)):
                with self.assertRaisesRegex(ValueError, "exactly one"):
                    runtime.serve(
                        model,
                        manifest_path,
                        "logical-model",
                        key_file,
                        trusted_ingress_auth=trusted,
                    )

            key.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "unsafe"):
                fixed._api_key(key)
            key.chmod(0o400)
            manifest["retained"] = False
            manifest_path.chmod(0o600)
            manifest_path.write_bytes(line(manifest))
            manifest_path.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "retained candidate"):
                runtime._verify_retained_model(model, manifest_path, fixed)
            with self.assertRaisesRegex(ValueError, "alias"):
                runtime.serve(model, manifest_path, "invalid alias", key)

    def test_production_contract_with_mocked_compute_runs_three_independent_branches(self):
        with tempfile.TemporaryDirectory(prefix="dragontales-staged-student-") as temporary:
            root = Path(temporary)
            claim_path, input_path, claim, inputs = fixture_files(root)
            train_output = root / "train-output"
            with mocked_train_compute(root), mock.patch.object(
                runtime, "_artifact_role", return_value="student-train"
            ):
                runtime.train_stage(claim_path, input_path, train_output)
            train = parse_line(train_output / "result.json")
            self.assertEqual(train["schema_version"], "dragontales.student-train-result.v1")
            self.assertEqual(train["outcome"]["kind"], "succeeded")
            self.assertEqual(
                list(train["outcome"]),
                ["kind", "adapter_manifest", "merged_model_manifest"],
            )
            self.assertEqual(parse_line(train_output / "upload.json")["student_job_id"], claim["student_job_id"])
            train_gpu = parse_line(
                train_output / "artifact" / train["gpu_evidence"]["sha256"]
            )
            self.assertEqual(
                (train_gpu["schema_version"], train_gpu["runtime"]["stage"], train_gpu["kernels"]),
                ("dragontales.student-gpu-evidence.v2", "train", []),
            )

            train_path, fanout_path, model, manifest, fanout = fixture_fanout(
                root, claim, train_output
            )
            self.assertEqual(fanout["schema_version"], "dragontales.student-fanout-claim.v4")
            self.assertEqual(tuple(fanout), runtime.FANOUT_KEYS)
            self.assertEqual(
                fanout["max_total_gpu_seconds"], runtime.MAX_TOTAL_GPU_SECONDS
            )
            wrong_image_fanout = root / "wrong-image-fanout.json"
            wrong_image = dict(fanout)
            wrong_image["student_branch_runtime_image_reference"] = (
                "fixture/other-student-branch@sha256:" + "9" * 64
            )
            wrong_image_fanout.write_bytes(compact(wrong_image))
            wrong_image_fanout.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "differs from its train authority"):
                runtime._branch_inputs(
                    claim_path,
                    input_path,
                    train_path,
                    wrong_image_fanout,
                    "bf16",
                )
            legacy_fanout = root / "legacy-fanout.json"
            legacy = dict(fanout)
            legacy["schema_version"] = "dragontales.student-fanout-claim.v3"
            legacy_fanout.write_bytes(compact(legacy))
            legacy_fanout.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "differs from its train authority"):
                runtime._branch_inputs(
                    claim_path,
                    input_path,
                    train_path,
                    legacy_fanout,
                    "bf16",
                )
            untyped_fanout = root / "untyped-fanout.json"
            untyped = json.loads(compact(fanout))
            untyped["max_total_gpu_seconds"] = float(runtime.MAX_TOTAL_GPU_SECONDS)
            untyped_fanout.write_bytes(compact(untyped))
            untyped_fanout.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "outside its fixed range"):
                runtime._branch_inputs(
                    claim_path,
                    input_path,
                    train_path,
                    untyped_fanout,
                    "bf16",
                )
            untyped_branch = root / "untyped-branch.json"
            untyped = json.loads(compact(fanout))
            untyped["branches"][0]["max_gpu_seconds"] = float(
                runtime.MAX_BRANCH_GPU_SECONDS
            )
            untyped_branch.write_bytes(compact(untyped))
            untyped_branch.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "outside its fixed range"):
                runtime._branch_inputs(
                    claim_path,
                    input_path,
                    train_path,
                    untyped_branch,
                    "bf16",
                )
            branch_results = []
            for variant, branch_claim in zip(runtime.VARIANTS, fanout["branches"]):
                output = root / f"branch-{variant}"
                with mocked_branch_compute(), mock.patch.object(
                    runtime, "_artifact_role", return_value="student-branch"
                ):
                    runtime.branch_stage(
                        claim_path,
                        input_path,
                        train_path,
                        fanout_path,
                        model,
                        manifest,
                        variant,
                        output,
                    )
                result = parse_line(output / "result.json")
                branch_results.append(result)
                self.assertEqual(result["schema_version"], "dragontales.student-branch-result.v1")
                self.assertEqual(result["branch_id"], branch_claim["branch_id"])
                self.assertEqual(result["variant"], variant)
                self.assertEqual(result["outcome"]["kind"], "succeeded")
                self.assertEqual(result["outcome"]["summary"]["dev_rows"], len(inputs["dev"]))
                self.assertEqual(result["outcome"]["summary"]["mean_score_bps"], 10_000)
                branch_gpu = parse_line(
                    output
                    / "artifact"
                    / result["outcome"]["gpu_evidence"]["sha256"]
                )
                self.assertEqual(branch_gpu["schema_version"], "dragontales.student-gpu-evidence.v2")
                self.assertEqual(branch_gpu["runtime"]["stage"], "branch")
                self.assertEqual([item["variant"] for item in branch_gpu["kernels"]], [variant])
                receipt = parse_line(
                    output / "artifact" / result["outcome"]["dev_receipt"]["sha256"]
                )
                self.assertEqual(receipt["summary"], result["outcome"]["summary"])
                self.assertEqual(len(receipt["rows"]), len(inputs["dev"]))
            self.assertEqual(
                [result["outcome"]["summary"]["total_latency_ms"] for result in branch_results],
                [2190, 1460, 730],
            )

            failed_output = root / "branch-failed"
            with mocked_branch_compute(), mock.patch.object(
                runtime, "_model_artifact", side_effect=RuntimeError("seal failed")
            ), mock.patch.object(
                runtime, "_artifact_role", return_value="student-branch"
            ), self.assertRaisesRegex(RuntimeError, "seal failed"):
                runtime.branch_stage(
                    claim_path,
                    input_path,
                    train_path,
                    fanout_path,
                    model,
                    manifest,
                    "bf16",
                    failed_output,
                )
            failed = parse_line(failed_output / "result.json")["outcome"]
            self.assertEqual(failed["kind"], "failed")
            self.assertEqual(failed["failure_class"], "runtimeerror")
            self.assertIsNotNone(failed["diagnostics"])
            self.assertIsNotNone(failed["gpu_evidence"])
            self.assertIsNone(failed["model_manifest"])
            self.assertIsNotNone(failed["dev_receipt"])
            self.assertEqual(failed["summary"]["mean_score_bps"], 10_000)

    def test_input_and_recipe_tampering_fail_before_output(self):
        with tempfile.TemporaryDirectory(prefix="dragontales-student-invalid-") as temporary:
            root = Path(temporary)
            claim_path, input_path, _claim, _inputs = fixture_files(root)
            input_path.write_bytes(input_path.read_bytes() + b"\n")
            output = root / "output"
            with mock.patch.object(
                runtime, "_artifact_role", return_value="student-train"
            ), self.assertRaisesRegex(ValueError, "exactly one LF"):
                runtime.train_stage(claim_path, input_path, output)
            self.assertFalse(output.exists())
            self.assertNotIn("fixture", inspect.getsource(runtime.main))

    def test_fixed_scores_use_reference_denominator_and_floor(self):
        self.assertEqual(runtime._score("exact_text_v1", "A", "a"), 0)
        self.assertEqual(runtime._score("normalized_text_v1", " Hello\nWORLD ", "hello world"), 1)
        self.assertEqual(runtime._score("char_similarity_v1", "kitten", "sitting"), 1 / 2)
        score = runtime._score("word_similarity_v1", "one two three", "one four three")
        self.assertEqual(int(score * 10_000), 6666)


if __name__ == "__main__":
    unittest.main()
