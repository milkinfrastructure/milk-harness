from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "deploy/student-run.sh"
RUNTIME_PATH = Path(__file__).resolve().with_name("runtime.py")
SPEC = importlib.util.spec_from_file_location("student_runtime", RUNTIME_PATH)
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)
PRODUCTION_RECIPE_SHA256 = "4b885bd86d91c8ef1303652bc2bc0fb14ebc3b80c5eeb850f8709746caa1e7eb"
FIXTURE_RECIPE_SHA256 = "cac916e7b430fb318685c8fe4435db6415181d770857881cf933ceada7ac1770"
FIXTURE_IMAGE = "fixture/dragontales-student@sha256:" + "1" * 64


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
    scope = {
        "tenant_id": "00000000-0000-0000-0000-000000000001",
        "project_id": "00000000-0000-0000-0000-000000000002",
        "environment_id": "00000000-0000-0000-0000-000000000003",
        "workload_id": "00000000-0000-0000-0000-000000000004",
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
        "schema_version": "dragontales.student-job-definition.v3",
        "teacher_provider_binding_sha256": "2" * 64,
        "teacher_results": [
            {"teacher_job_id": "3" * 64, "object_sha256": "4" * 64, "bytes": 1}
        ],
        "input_sha256": digest(input_raw),
        "dev_set_sha256": digest(runtime.DEV_SET_DOMAIN + compact(inputs["dev"])),
        "counts": {"train": 50, "dev": 73, "calibration": 128},
        "recipe_sha256": runtime.recipe_sha256("fixture"),
        "runtime_image_reference": FIXTURE_IMAGE,
        "max_train_gpu_seconds": runtime.MAX_TRAIN_GPU_SECONDS,
        "max_branch_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
        "max_total_gpu_seconds": runtime.MAX_TOTAL_GPU_SECONDS,
        "quality": {
            "min_mean_score_bps": 9500,
            "max_mean_loss_vs_bf16_bps": 0,
            "max_p95_latency_ms": 30_000,
        },
        "expires_at": "2099-01-02T00:00:00Z",
        "initial_stage": "train_merge",
    }
    claim = {
        "schema_version": "dragontales.student-job-claim.v3",
        "scope": scope,
        "student_job_id": digest(compact(definition)),
        "definition": definition,
        "started_at": "2026-01-01T00:00:00Z",
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
            "object_key": f"dt/v2/tenant/project/environment/workload/artifacts/{job_id}/{item['sha256']}",
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
            "schema_version": "dragontales.student-branch-definition.v1",
            "student_job_id": claim["student_job_id"],
            "variant": variant,
            "train_result_sha256": digest(train_raw),
            "recipe_sha256": claim["definition"]["recipe_sha256"],
            "max_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
            "expires_at": claim["definition"]["expires_at"],
        }
        branches.append(
            {
                "schema_version": "dragontales.student-branch-claim.v1",
                "branch_id": digest(compact(definition)),
                "variant": variant,
                "max_gpu_seconds": runtime.MAX_BRANCH_GPU_SECONDS,
            }
        )
    fanout = {
        "schema_version": "dragontales.student-fanout-claim.v3",
        "scope": claim["scope"],
        "student_job_id": claim["student_job_id"],
        "train_result_sha256": digest(train_raw),
        "recipe_sha256": claim["definition"]["recipe_sha256"],
        "runtime_image_reference": claim["definition"]["runtime_image_reference"],
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


class StudentRuntimeTest(unittest.TestCase):
    def test_v3_image_and_budget_wire_is_exact(self):
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
                runtime._validate_claim(compact(mixed), False, "fixture")

            untyped = json.loads(claim_path.read_bytes())
            untyped["definition"]["max_total_gpu_seconds"] = float(
                runtime.MAX_TOTAL_GPU_SECONDS
            )
            untyped["student_job_id"] = digest(compact(untyped["definition"]))
            with self.assertRaisesRegex(ValueError, "outside its fixed range"):
                runtime._validate_claim(compact(untyped), False, "fixture")

            mutable = json.loads(claim_path.read_bytes())
            mutable["definition"]["runtime_image_reference"] = (
                "fixture/dragontales-student:latest"
            )
            mutable["student_job_id"] = digest(compact(mutable["definition"]))
            with self.assertRaisesRegex(ValueError, "immutable"):
                runtime._validate_claim(compact(mutable), False, "fixture")

            reordered = json.loads(claim_path.read_bytes())
            image = reordered["definition"].pop("runtime_image_reference")
            reordered["definition"]["runtime_image_reference"] = image
            reordered["student_job_id"] = digest(compact(reordered["definition"]))
            with self.assertRaisesRegex(ValueError, "wrong typed fields"):
                runtime._validate_claim(compact(reordered), False, "fixture")

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
            with mock.patch.object(
                runtime, "_compute_alarm_seconds", side_effect=budget
            ), mock.patch.object(
                runtime.signal,
                "alarm",
                side_effect=lambda seconds: events.append(("alarm", seconds)),
            ), mock.patch.object(runtime, "_write_terminal", side_effect=terminal):
                runtime.train_stage(
                    claim_path, input_path, train_output, mode="fixture"
                )

            train_path, fanout_path, model, manifest, _fanout = fixture_fanout(
                root, claim, train_output
            )
            branch_output = root / "branch"
            with mock.patch.object(
                runtime, "_compute_alarm_seconds", side_effect=budget
            ), mock.patch.object(
                runtime.signal,
                "alarm",
                side_effect=lambda seconds: events.append(("alarm", seconds)),
            ), mock.patch.object(runtime, "_write_terminal", side_effect=terminal):
                runtime.branch_stage(
                    claim_path,
                    input_path,
                    train_path,
                    fanout_path,
                    model,
                    manifest,
                    "bf16",
                    branch_output,
                    mode="fixture",
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
            "DRAGONTALES_CONFIG_JSON": "secret",
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
        dockerfile = (ROOT / "deploy/student/Dockerfile").read_text()
        provenance_raw = (ROOT / "deploy/student/provenance.json").read_bytes()
        provenance = json.loads(provenance_raw)
        prime_license = (ROOT / "deploy/licenses/prime-rl-v0.9.0.LICENSE").read_bytes()
        verifier = runtime._load_module(
            "dragontales_test_qwen_image_release",
            ROOT / "deploy/models/qwen3-4b-instruct-2507/verify.py",
        )
        source = (
            "https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/"
            + verifier.EXPECTED_PROFILE["model_revision"]
        )
        self.assertIn(
            "ARG MILK_GATEWAY_IMAGE\nFROM ${MILK_GATEWAY_IMAGE} AS gateway",
            dockerfile,
        )
        self.assertIn(
            "COPY --from=gateway --chmod=0555 /usr/local/bin/dragontales-gateway "
            "/usr/local/bin/dragontales-gateway",
            dockerfile,
        )
        self.assertNotIn("FROM rust:", dockerfile)
        self.assertNotIn("cargo build", dockerfile)
        self.assertNotIn("COPY crates/", dockerfile)
        self.assertIn('test "${#gateway_digest}" -eq 64', dockerfile)
        self.assertIn('case "$gateway_digest" in *[!0-9a-f]*) exit 1', dockerfile)
        self.assertIn("COPY deploy/student-job.sh ./student-job.sh", dockerfile)
        self.assertIn("COPY deploy/student-run.sh ./student-run.sh", dockerfile)
        self.assertIn(
            "COPY deploy/student/Dockerfile deploy/student/runtime.py "
            "deploy/student/qwen3-chat-template.jinja ./student/",
            dockerfile,
        )
        self.assertIn(
            "COPY deploy/adapter_train/__init__.py deploy/adapter_train/contract.py \\\n"
            "     deploy/adapter_train/sft_entrypoint.py ./adapter_train/",
            dockerfile,
        )
        self.assertNotIn("test_runtime.py", dockerfile)
        self.assertNotIn("adapter_merge", dockerfile)
        self.assertNotIn("quantization/runtime_contract.py", dockerfile)
        self.assertEqual(runtime.LLM_COMPRESSOR_VERSION, "0.13.0")
        self.assertEqual(runtime.COMPRESSED_TENSORS_VERSION, "0.18.0")
        self.assertIn("llmcompressor-0.13.0", dockerfile)
        self.assertIn("compressed_tensors-0.18.0", dockerfile)
        wheels = (
            ("auto_round-0.14.2-py3-none-any.whl", "4265c11d33a6f688c211bfed87e4698b685473efe1d6faf95d744a3f7c19ca24"),
            ("datasets-5.0.1-py3-none-any.whl", "9fbf73688f8c18f7529b4fe592abd04015f81d1e58001e4bac73ffb2b39d7cc4"),
            ("nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl", "ecd0a012051abc20c1aa87328841efa8cade3ced65803046e38c2f03c0891fea"),
            ("transformers-5.14.1-py3-none-any.whl", "9db974c4079ede2d1a3ea7ca5a240df33f2cc26fc2b36ba64c5f2a4f43b6e725"),
            ("fsspec-2026.6.0-py3-none-any.whl", "02e0b71817df9b2169dc30a16832045764def1191b43dcff5bb85bdee212d2a1"),
            ("multiprocess-0.70.19-py312-none-any.whl", "3a56c0e85dd5025161bac5ce138dcac1e49174c7d8e74596537e729fd5c53c28"),
            ("pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl", "5389cdf79447ed1515c9e31620e6e1e2302249564d603f2ad727d4f6d313e4c3"),
            ("pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl", "d373ce03ffd84010ed9839fa73672a9c8256990532e158440c0085db7d914b34"),
            ("xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl", "237b8f63a2a0fcfb1ffc06e21dad23add44e6d354b2b014364a1d41e419a4dee"),
        )
        for wheel, sha256 in wheels:
            self.assertIn(f"--checksum=sha256:{sha256}", dockerfile)
            self.assertGreaterEqual(dockerfile.count(f"/opt/{wheel}"), 2)
        self.assertIn("pip install --no-index --no-deps --force-reinstall", dockerfile)
        self.assertNotIn("pip install --no-cache-dir /opt/llmcompressor", dockerfile)
        for runtime_import in (
            "from compressed_tensors.quantization import QuantizationConfig",
            "from datasets import Dataset",
            "from llmcompressor import oneshot",
            "from llmcompressor.modifiers.quantization import QuantizationModifier",
            "from transformers import AutoTokenizer",
        ):
            self.assertIn(runtime_import, dockerfile)
        self.assertIn(
            "COPY --from=gateway --chmod=0444 /usr/share/licenses/dragontales/ "
            "/usr/share/licenses/dragontales/",
            dockerfile,
        )
        self.assertIn("COPY --chmod=0444 LICENSE /usr/share/licenses/milk-harness/LICENSE", dockerfile)
        self.assertEqual(
            hashlib.sha256(prime_license).hexdigest(),
            "f5118b9c9e98b0f4076214ee13f68d5f73c13b077c44544cb9a0c4ed9155065c",
        )
        self.assertEqual(len(prime_license), 11_695)
        self.assertEqual(
            hashlib.sha256(provenance_raw).hexdigest(),
            "6371f282f372c5f5cadc6530bf81754c307b100067bb56d3324e75f3bf4a38ec",
        )
        self.assertEqual(
            tuple(provenance),
            ("schema_version", "platform", "gateway", "base_images", "model", "wheels"),
        )
        self.assertEqual(provenance["schema_version"], "milk.student-image-provenance.v1")
        self.assertEqual(provenance["platform"], "linux/amd64")
        self.assertEqual(provenance["gateway"]["build_argument"], "MILK_GATEWAY_IMAGE")
        self.assertEqual(provenance["base_images"]["prime_rl"]["source_revision"], "ab5de8fff44b2c4a5c85e24b6e6e3f7d57eee7b1")
        self.assertEqual(provenance["base_images"]["vllm"]["source_revision"], "2cf0a6915ce544dc493a0990f2ea38d81601128a")
        self.assertEqual(provenance["model"]["revision"], verifier.EXPECTED_PROFILE["model_revision"])
        self.assertEqual(len(provenance["wheels"]), 13)
        self.assertEqual([wheel["name"] for wheel in provenance["wheels"]], list(dict.fromkeys(wheel["name"] for wheel in provenance["wheels"])))
        for wheel in provenance["wheels"]:
            self.assertIn(f"--checksum=sha256:{wheel['sha256']}", dockerfile)
            self.assertIn(wheel["artifact"], dockerfile)
        self.assertIn(
            "COPY --chmod=0444 deploy/licenses/prime-rl-v0.9.0.LICENSE "
            "/usr/share/licenses/prime-rl/LICENSE",
            dockerfile,
        )
        self.assertIn(
            "COPY --chmod=0444 deploy/student/provenance.json "
            "/usr/share/milk/student-provenance.json",
            dockerfile,
        )
        self.assertIn("6371f282f372c5f5cadc6530bf81754c307b100067bb56d3324e75f3bf4a38ec", dockerfile)
        self.assertEqual(dockerfile.count("/model/LICENSE"), 1)
        for authority in ("profile.json", "model.manifest.sha256", "verify.py"):
            self.assertIn(f"deploy/models/qwen3-4b-instruct-2507/{authority}", dockerfile)
        for name, sha256, _size, _source in verifier.FILES:
            self.assertIn(
                f"ADD --checksum=sha256:{sha256} \\\n    {source}/{name} \\\n    /model/{name}",
                dockerfile,
            )
        self.assertEqual(dockerfile.count(source + "/"), len(verifier.FILES))
        self.assertIn(
            'runpy.run_path("models/qwen3-4b-instruct-2507/verify.py")["verify_tree"](Path("/model"))',
            dockerfile,
        )
        self.assertIn('ENTRYPOINT ["/opt/dragontales/deploy/student-job.sh"]', dockerfile)
        self.assertIn("RUN test -x /usr/bin/setpriv", dockerfile)
        self.assertIn("USER 0:0", dockerfile)
        self.assertIn("deploy/student/qwen3-chat-template.jinja", dockerfile)
        self.assertIn(
            "/app/.venv/bin/python adapter_train/sft_entrypoint.py parity \\\n"
            "      --model /model --template student/qwen3-chat-template.jinja",
            dockerfile,
        )
        self.assertEqual(runtime.recipe_sha256("production"), PRODUCTION_RECIPE_SHA256)
        self.assertEqual(runtime.recipe_sha256("fixture"), FIXTURE_RECIPE_SHA256)

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

    def test_retained_model_and_serve_command_are_fail_closed(self):
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

    def test_fixture_train_then_three_independent_branches(self):
        with tempfile.TemporaryDirectory(prefix="dragontales-staged-student-") as temporary:
            root = Path(temporary)
            claim_path, input_path, claim, inputs = fixture_files(root)
            train_output = root / "train-output"
            subprocess.run(
                [
                    str(RUNNER),
                    "fixture-train",
                    "--claim",
                    str(claim_path),
                    "--input",
                    str(input_path),
                    "--output",
                    str(train_output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            train = parse_line(train_output / "result.json")
            self.assertEqual(train["schema_version"], "dragontales.student-train-result.v1")
            self.assertEqual(train["outcome"]["kind"], "succeeded")
            self.assertEqual(
                list(train["outcome"]),
                ["kind", "adapter_manifest", "merged_model_manifest"],
            )
            self.assertEqual(parse_line(train_output / "upload.json")["student_job_id"], claim["student_job_id"])

            train_path, fanout_path, model, manifest, fanout = fixture_fanout(
                root, claim, train_output
            )
            self.assertEqual(fanout["schema_version"], "dragontales.student-fanout-claim.v3")
            self.assertEqual(tuple(fanout), runtime.FANOUT_KEYS)
            self.assertEqual(
                fanout["max_total_gpu_seconds"], runtime.MAX_TOTAL_GPU_SECONDS
            )
            wrong_image_fanout = root / "wrong-image-fanout.json"
            wrong_image = dict(fanout)
            wrong_image["runtime_image_reference"] = (
                "fixture/other-student@sha256:" + "9" * 64
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
                    "fixture",
                )
            legacy_fanout = root / "legacy-fanout.json"
            legacy = dict(fanout)
            legacy["schema_version"] = "dragontales.student-fanout-claim.v2"
            legacy_fanout.write_bytes(compact(legacy))
            legacy_fanout.chmod(0o400)
            with self.assertRaisesRegex(ValueError, "differs from its train authority"):
                runtime._branch_inputs(
                    claim_path,
                    input_path,
                    train_path,
                    legacy_fanout,
                    "bf16",
                    "fixture",
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
                    "fixture",
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
                    "fixture",
                )
            branch_results = []
            for variant, branch_claim in zip(runtime.VARIANTS, fanout["branches"]):
                output = root / f"branch-{variant}"
                subprocess.run(
                    [
                        str(RUNNER),
                        "fixture-branch",
                        "--claim",
                        str(claim_path),
                        "--input",
                        str(input_path),
                        "--train-result",
                        str(train_path),
                        "--fanout-claim",
                        str(fanout_path),
                        "--merged-model",
                        str(model),
                        "--merged-model-manifest",
                        str(manifest),
                        "--variant",
                        variant,
                        "--output",
                        str(output),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                result = parse_line(output / "result.json")
                branch_results.append(result)
                self.assertEqual(result["schema_version"], "dragontales.student-branch-result.v1")
                self.assertEqual(result["branch_id"], branch_claim["branch_id"])
                self.assertEqual(result["variant"], variant)
                self.assertEqual(result["outcome"]["kind"], "succeeded")
                self.assertEqual(result["outcome"]["summary"]["dev_rows"], len(inputs["dev"]))
                self.assertEqual(result["outcome"]["summary"]["mean_score_bps"], 10_000)
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
            with mock.patch.object(
                runtime, "_model_artifact", side_effect=RuntimeError("seal failed")
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
                    "fixture",
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
            process = subprocess.run(
                [
                    str(RUNNER),
                    "fixture-train",
                    "--claim",
                    str(claim_path),
                    "--input",
                    str(input_path),
                    "--output",
                    str(output),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(process.returncode, 0)
            self.assertFalse(output.exists())
            self.assertIn("exactly one LF", process.stderr)
            self.assertNotEqual(
                runtime.recipe_sha256("production"), runtime.recipe_sha256("fixture")
            )

    def test_fixed_scores_use_reference_denominator_and_floor(self):
        self.assertEqual(runtime._score("exact_text_v1", "A", "a"), 0)
        self.assertEqual(runtime._score("normalized_text_v1", " Hello\nWORLD ", "hello world"), 1)
        self.assertEqual(runtime._score("char_similarity_v1", "kitten", "sitting"), 1 / 2)
        score = runtime._score("word_similarity_v1", "one two three", "one four three")
        self.assertEqual(int(score * 10_000), 6666)


if __name__ == "__main__":
    unittest.main()
