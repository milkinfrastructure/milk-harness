#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


MODEL = Path("/model")
RUN = Path("/runs/run")
PRIME_PYTHON = Path("/app/.venv/bin/python")
TORCHRUN = Path("/app/.venv/bin/torchrun")
QUANT_PYTHON = Path("/opt/quant/bin/python")
MODEL_ALIAS = "dragontales-qwen3-4b"
REASONING_PARSER = "qwen3"
CHAT_TEMPLATE = Path(__file__).resolve().with_name("qwen3-chat-template.jinja")
CHAT_TEMPLATE_SHA256 = "071ab92ad2e5b0f8091800ecb4a80c05f98f069669e3dff0389012547414e85b"
ARTIFACT_ROLE_PATH = Path("/usr/share/milk/student-artifact-role")
LOOPBACK_PROXY = "http://127.0.0.1:9"
LLM_COMPRESSOR_VERSION = "0.13.0"
COMPRESSED_TENSORS_VERSION = "0.18.0"
MAX_CLAIM_BYTES = 1024 * 1024
MAX_INPUT_BYTES = 128 * 1024 * 1024
MAX_RESULT_BYTES = 1024 * 1024
MAX_RESULT_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_UPLOAD_BYTES = 4 * 1024 * 1024
MAX_UPLOAD_FILES = 4096
MAX_UPLOAD_TOTAL_BYTES = 16 * 1024**4
MAX_TRAIN_GPU_SECONDS = 1_800
MAX_BRANCH_GPU_SECONDS = 1_800
MAX_TOTAL_GPU_SECONDS = MAX_TRAIN_GPU_SECONDS + 3 * MAX_BRANCH_GPU_SECONDS
TERMINALIZATION_GRACE_SECONDS = 900
MAX_SOURCE_BYTES = 1024 * 1024
MAX_TARGET_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_EDIT_DISTANCE_CELLS = 16 * 1024 * 1024
DEV_SET_DOMAIN = b"dragontales.student-dev-set.v1\0"
MODEL_INVENTORY_DOMAIN = b"dragontales.student-model-inventory.v1\0"
RECIPE_DOMAIN = b"dragontales.student-recipe.production.v1\0"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_NAME = re.compile(r"[A-Za-z0-9._-]{1,255}\Z")
LOGICAL_MODEL_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
IMAGE_OWNER_UID = 0
RUST_WORDS = re.compile(
    r"[^\u0009-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+"
)
VARIANTS = ("bf16", "dynamic_fp8", "static_fp8")
EVALUATORS = {
    "exact_text_v1",
    "normalized_text_v1",
    "char_similarity_v1",
    "word_similarity_v1",
}
AUTHORITY_FILES = (
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
)

SCOPE_UUID_KEYS = ("tenant_id", "project_id", "environment_id", "workload_id")
SCOPE_KEYS = SCOPE_UUID_KEYS + ("eval_id",)
CLAIM_KEYS = ("schema_version", "scope", "student_job_id", "definition", "started_at")
DEFINITION_KEYS = (
    "schema_version",
    "teacher_provider_binding_sha256",
    "teacher_results",
    "input_sha256",
    "dev_set_sha256",
    "counts",
    "recipe_sha256",
    "student_train_runtime_image_reference",
    "student_branch_runtime_image_reference",
    "max_train_gpu_seconds",
    "max_branch_gpu_seconds",
    "max_total_gpu_seconds",
    "quality",
    "expires_at",
)
TEACHER_REF_KEYS = ("teacher_job_id", "object_sha256", "bytes")
COUNT_KEYS = ("train", "dev", "calibration")
QUALITY_KEYS = (
    "min_mean_score_bps",
    "max_mean_loss_vs_bf16_bps",
    "max_p95_latency_ms",
)
INPUT_KEYS = (
    "schema_version",
    "scope",
    "teacher_provider_binding_sha256",
    "train",
    "dev",
    "calibration",
)
TRAIN_KEYS = ("request_sha256", "projection", "target")
DEV_KEYS = ("request_sha256", "projection", "evaluator_id", "reference")
CALIBRATION_KEYS = ("request_sha256", "projection")
PROJECTION_KEYS = ("model", "messages")
MESSAGE_KEYS = ("role", "content")
UPLOAD_KEYS = ("schema_version", "student_job_id", "files")
UPLOAD_FILE_KEYS = ("object_key", "relative_path", "sha256", "bytes")
ARTIFACT_REF_KEYS = ("object_key", "sha256", "bytes")
TRAIN_RESULT_KEYS = (
    "schema_version",
    "scope",
    "student_job_id",
    "claim_sha256",
    "runner_sha256",
    "started_at",
    "finished_at",
    "gpu_evidence",
    "observed_gpu_seconds",
    "outcome",
)
FANOUT_KEYS = (
    "schema_version",
    "scope",
    "student_job_id",
    "train_result_sha256",
    "recipe_sha256",
    "student_branch_runtime_image_reference",
    "max_total_gpu_seconds",
    "created_at",
    "expires_at",
    "branches",
)
BRANCH_CLAIM_KEYS = ("schema_version", "branch_id", "variant", "max_gpu_seconds")
MODEL_MANIFEST_KEYS = (
    "schema_version",
    "student_job_id",
    "variant",
    "retained",
    "inventory_sha256",
    "file_count",
    "artifact_bytes",
    "files",
)
MODEL_FILE_KEYS = ("relative_path", "object_key", "sha256", "bytes")
SENSITIVE_ENV_PREFIXES = (
    "AWS_",
    "CF_",
    "CLOUDFLARE_",
    "DRAGONTALES_",
    "MILK_",
    "MODAL_",
    "R2_",
    "S3_",
)
SENSITIVE_ENV_NAMES = {
    "DOCKER_CONFIG",
    "GIT_ASKPASS",
    "KUBECONFIG",
    "NETRC",
    "SSH_AUTH_SOCK",
}
SENSITIVE_ENV_WORDS = {
    "AUTH",
    "CERTIFICATE",
    "CREDENTIAL",
    "CREDENTIALS",
    "KEY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
}
MODEL_OFFLINE_ENV = {
    "ALL_PROXY": LOOPBACK_PROXY,
    "DO_NOT_TRACK": "1",
    "HF_DATASETS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
    "HF_HUB_DISABLE_XET": "1",
    "HF_HUB_OFFLINE": "1",
    "HTTP_PROXY": LOOPBACK_PROXY,
    "HTTPS_PROXY": LOOPBACK_PROXY,
    "NO_PROXY": "127.0.0.1,localhost,::1",
    "PIP_NO_INDEX": "1",
    "TOKENIZERS_PARALLELISM": "false",
    "TRANSFORMERS_OFFLINE": "1",
    "UV_OFFLINE": "1",
    "VLLM_NO_USAGE_STATS": "1",
    "WANDB_DISABLED": "true",
    "WANDB_MODE": "disabled",
    "all_proxy": LOOPBACK_PROXY,
    "http_proxy": LOOPBACK_PROXY,
    "https_proxy": LOOPBACK_PROXY,
    "no_proxy": "127.0.0.1,localhost,::1",
}


def _duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _compact(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def _line(value) -> bytes:
    return _compact(value) + b"\n"


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _keys(value, expected, name):
    if not isinstance(value, dict) or tuple(value) != expected:
        raise ValueError(f"{name} has the wrong typed fields")
    return value


def _digest(value, name):
    if not isinstance(value, str) or HEX64.fullmatch(value) is None:
        raise ValueError(f"{name} is not one lowercase SHA-256 digest")
    return value


def _integer(value, name, minimum=0, maximum=None):
    if (
        type(value) is not int
        or value < minimum
        or maximum is not None
        and value > maximum
    ):
        raise ValueError(f"{name} is outside its fixed range")
    return value


def _read(path: Path, maximum: int) -> bytes:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError(f"input is not one absolute regular file: {path}")
    before = path.stat()
    if before.st_nlink != 1 or not 0 < before.st_size <= maximum:
        raise ValueError(f"input is empty, linked, or oversized: {path}")
    raw = path.read_bytes()
    after = path.stat()
    stable = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
    if len(raw) != before.st_size or any(getattr(before, key) != getattr(after, key) for key in stable):
        raise ValueError(f"input changed while reading: {path}")
    return raw


def _json(raw: bytes, name: str):
    try:
        return json.loads(
            raw,
            object_pairs_hook=_duplicates,
            parse_constant=_invalid_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} is not strict JSON") from error


def _canonical(raw: bytes, name: str, newline: bool):
    payload = raw[:-1] if newline and raw.endswith(b"\n") else raw
    if newline and (not raw.endswith(b"\n") or payload.endswith(b"\n")):
        raise ValueError(f"{name} must end in exactly one LF")
    value = _json(payload, name)
    expected = _line(value) if newline else _compact(value)
    if raw != expected:
        suffix = " plus one LF" if newline else ""
        raise ValueError(f"{name} is not canonical compact JSON{suffix}")
    return value


def _time(value, name):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{name} is not an RFC3339 UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{name} is not an RFC3339 UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{name} is not UTC")
    return parsed


def _format_time(value: dt.datetime) -> str:
    value = value.astimezone(dt.timezone.utc)
    precision = "microseconds" if value.microsecond else "seconds"
    return value.isoformat(timespec=precision).replace("+00:00", "Z")


def _uuid(value, name):
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise ValueError(f"{name} is not a UUID") from error
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError(f"{name} is not one canonical non-nil UUID")
    return value


def _scope(value):
    _keys(value, SCOPE_KEYS, "student scope")
    identities = [_uuid(value[key], key) for key in SCOPE_UUID_KEYS]
    if len(set(identities)) != len(identities):
        raise ValueError("student scope UUIDs must be distinct")
    _digest(value["eval_id"], "student eval ID")
    return value


def _projection(value, name):
    _keys(value, PROJECTION_KEYS, name)
    if not isinstance(value["model"], str) or not value["model"] or len(value["model"].encode()) > 256:
        raise ValueError(f"{name} has an invalid model")
    messages = value["messages"]
    if not isinstance(messages, list) or not 1 <= len(messages) <= 256:
        raise ValueError(f"{name} has an invalid message count")
    source_bytes = 0
    has_user = False
    for message in messages:
        _keys(message, MESSAGE_KEYS, f"{name} message")
        role, content = message["role"], message["content"]
        if role not in {"system", "developer", "user", "assistant"}:
            raise ValueError(f"{name} has an unsupported role")
        if not isinstance(content, str) or not content or len(content.encode()) > MAX_SOURCE_BYTES:
            raise ValueError(f"{name} has invalid text")
        source_bytes += len(content.encode())
        has_user = has_user or role == "user"
    if not has_user or source_bytes > MAX_SOURCE_BYTES:
        raise ValueError(f"{name} has no bounded user message")
    return value


def _model_messages(projection):
    return [
        {
            "role": "system" if message["role"] == "developer" else message["role"],
            "content": message["content"],
        }
        for message in projection["messages"]
    ]


def _runtime_image_reference(value):
    if not isinstance(value, str):
        raise ValueError("student runtime image must be an immutable @sha256 reference")
    name, separator, digest = value.rpartition("@sha256:")
    if (
        not separator
        or not name
        or any(character.isspace() for character in name)
        or HEX64.fullmatch(digest) is None
    ):
        raise ValueError("student runtime image must be an immutable @sha256 reference")
    return value


def _chat_template() -> str:
    raw = _read(CHAT_TEMPLATE, 64 * 1024)
    if _sha256(raw) != CHAT_TEMPLATE_SHA256:
        raise ValueError("student chat template differs from the fixed recipe")
    return raw.decode("utf-8")


def _artifact_role() -> str:
    before = ARTIFACT_ROLE_PATH.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != IMAGE_OWNER_UID
        or before.st_nlink != 1
        or before.st_mode & 0o022
    ):
        raise ValueError("student artifact role authority is unsafe")
    raw = _read(ARTIFACT_ROLE_PATH, 64)
    role = raw.removesuffix(b"\n")
    if raw != role + b"\n" or role not in {b"student-train", b"student-branch"}:
        raise ValueError("student artifact role authority is invalid")
    return role.decode()


def _require_role(expected: str) -> None:
    if _artifact_role() != expected:
        raise ValueError(f"{expected} operation is forbidden in this artifact")


def recipe_sha256() -> str:
    deploy = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256(RECIPE_DOMAIN)
    for relative in AUTHORITY_FILES:
        path = deploy / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"student recipe authority is missing or unsafe: {relative}")
        raw = path.read_bytes()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(str(len(raw)).encode())
        digest.update(b"\0")
        digest.update(raw)
    return digest.hexdigest()


def _validate_claim(raw: bytes, check_clock: bool):
    claim = _keys(_canonical(raw, "student claim", False), CLAIM_KEYS, "student claim")
    if claim["schema_version"] != "dragontales.student-job-claim.v4":
        raise ValueError("student claim has an unsupported schema")
    _scope(claim["scope"])
    job_id = _digest(claim["student_job_id"], "student job")
    definition = _keys(claim["definition"], DEFINITION_KEYS, "student definition")
    if definition["schema_version"] != "dragontales.student-job-definition.v4":
        raise ValueError("student definition has an unsupported schema")
    if _sha256(_compact(definition)) != job_id:
        raise ValueError("student job ID differs from its canonical definition")
    _digest(definition["teacher_provider_binding_sha256"], "teacher provider binding")
    _digest(definition["input_sha256"], "student input")
    _digest(definition["dev_set_sha256"], "student DEV set")
    expected_recipe = recipe_sha256()
    if _digest(definition["recipe_sha256"], "student recipe") != expected_recipe:
        raise ValueError("student claim does not bind this exact runner recipe")
    train_image = _runtime_image_reference(
        definition["student_train_runtime_image_reference"]
    )
    branch_image = _runtime_image_reference(
        definition["student_branch_runtime_image_reference"]
    )
    if train_image.rpartition("@sha256:")[2] == branch_image.rpartition("@sha256:")[2]:
        raise ValueError("student train and branch images must have distinct digests")
    budgets = (
        _integer(definition["max_train_gpu_seconds"], "student train GPU seconds"),
        _integer(definition["max_branch_gpu_seconds"], "student branch GPU seconds"),
        _integer(definition["max_total_gpu_seconds"], "student total GPU seconds"),
    )
    if budgets != (
        MAX_TRAIN_GPU_SECONDS,
        MAX_BRANCH_GPU_SECONDS,
        MAX_TOTAL_GPU_SECONDS,
    ):
        raise ValueError("student claim does not bind the fixed staged GPU budgets")
    teacher_results = definition["teacher_results"]
    if not isinstance(teacher_results, list) or not 1 <= len(teacher_results) <= 4096:
        raise ValueError("student claim has an invalid teacher-result count")
    teacher_ids = []
    for reference in teacher_results:
        _keys(reference, TEACHER_REF_KEYS, "teacher-result reference")
        teacher_ids.append(_digest(reference["teacher_job_id"], "teacher job"))
        _digest(reference["object_sha256"], "teacher result object")
        _integer(reference["bytes"], "teacher result bytes", 1)
    if teacher_ids != sorted(set(teacher_ids)):
        raise ValueError("student teacher-result references are not sorted and unique")
    counts = _keys(definition["counts"], COUNT_KEYS, "student counts")
    if not (
        50 <= _integer(counts["train"], "TRAIN count") <= 10_000
        and 73 <= _integer(counts["dev"], "DEV count") <= 128
        and _integer(counts["calibration"], "CALIBRATION count") == 128
    ):
        raise ValueError("student counts are not ready for the fixed vertical")
    quality = _keys(definition["quality"], QUALITY_KEYS, "student quality gates")
    if quality != {
        "min_mean_score_bps": 9500,
        "max_mean_loss_vs_bf16_bps": 0,
        "max_p95_latency_ms": 30_000,
    }:
        raise ValueError("student quality gates differ from the fixed vertical")
    started = _time(claim["started_at"], "student claim start")
    expires = _time(definition["expires_at"], "student claim expiry")
    if started >= expires:
        raise ValueError("student claim has no execution interval")
    now = dt.datetime.now(dt.timezone.utc)
    if check_clock and (
        now < started or now >= expires or now >= started + dt.timedelta(minutes=15)
    ):
        raise ValueError("student claim is not currently executable")
    return claim, started, expires


def _validate_input(raw: bytes, claim):
    value = _keys(_canonical(raw, "student input", True), INPUT_KEYS, "student input")
    definition = claim["definition"]
    if value["schema_version"] != "dragontales.student-input.v1":
        raise ValueError("student input has an unsupported schema")
    if value["scope"] != claim["scope"]:
        raise ValueError("student input scope differs from the claim")
    _scope(value["scope"])
    if value["teacher_provider_binding_sha256"] != definition["teacher_provider_binding_sha256"]:
        raise ValueError("student input teacher binding differs from the claim")
    if _sha256(raw) != definition["input_sha256"]:
        raise ValueError("student input bytes differ from the claim")
    sections = (
        ("train", TRAIN_KEYS),
        ("dev", DEV_KEYS),
        ("calibration", CALIBRATION_KEYS),
    )
    all_ids = []
    for section, keys in sections:
        rows = value[section]
        if not isinstance(rows, list) or len(rows) != definition["counts"][section]:
            raise ValueError(f"student {section} rows differ from the claim")
        section_ids = []
        for row in rows:
            _keys(row, keys, f"student {section} row")
            section_ids.append(_digest(row["request_sha256"], f"student {section} request"))
            _projection(row["projection"], f"student {section} projection")
            if section == "train":
                target = row["target"]
                if not isinstance(target, str) or not target.strip() or len(target.encode()) > MAX_TARGET_BYTES:
                    raise ValueError("student TRAIN target is empty or oversized")
            elif section == "dev":
                if row["evaluator_id"] not in EVALUATORS:
                    raise ValueError("student DEV evaluator is not automatic")
                reference = row["reference"]
                if not isinstance(reference, str) or not reference.strip() or len(reference.encode()) > MAX_TARGET_BYTES:
                    raise ValueError("student DEV reference is empty or oversized")
        if section_ids != sorted(section_ids) or len(set(section_ids)) != len(section_ids):
            raise ValueError(f"student {section} selection is not sorted and unique")
        all_ids.extend(section_ids)
    if len(set(all_ids)) != len(all_ids):
        raise ValueError("student partitions overlap")
    dev_sha256 = hashlib.sha256(DEV_SET_DOMAIN + _compact(value["dev"])).hexdigest()
    if dev_sha256 != definition["dev_set_sha256"]:
        raise ValueError("student DEV set differs from the claim")
    return value


def load_inputs(claim_path: Path, input_path: Path, check_clock: bool):
    claim_raw = _read(claim_path, MAX_CLAIM_BYTES)
    input_raw = _read(input_path, MAX_INPUT_BYTES)
    claim, started, expires = _validate_claim(claim_raw, check_clock)
    inputs = _validate_input(input_raw, claim)
    return claim_raw, input_raw, claim, inputs, started, expires


def _artifact_prefix(claim):
    scope = claim["scope"]
    identities = "/".join(scope[key] for key in SCOPE_UUID_KEYS)
    return (
        f"dt/v3/{scope['eval_id']}/{identities}/artifacts/"
        f"{claim['student_job_id']}/"
    )


def _artifact_ref(value, name, claim):
    _keys(value, ARTIFACT_REF_KEYS, name)
    key = value["object_key"]
    digest = _digest(value["sha256"], f"{name} object")
    if (
        not isinstance(key, str)
        or key != _artifact_prefix(claim) + digest
    ):
        raise ValueError(f"{name} has an invalid object key")
    _integer(value["bytes"], f"{name} bytes", 1)
    return value


def _branch_inputs(
    claim_path: Path,
    input_path: Path,
    train_result_path: Path,
    fanout_claim_path: Path,
    variant: str,
):
    claim_raw, input_raw, claim, inputs, claim_started, expires = load_inputs(
        claim_path, input_path, False
    )
    train_raw = _read(train_result_path, MAX_RESULT_BYTES)
    train = _keys(
        _canonical(train_raw, "student train result", True),
        TRAIN_RESULT_KEYS,
        "student train result",
    )
    train_outcome = train["outcome"]
    if (
        train["schema_version"] != "dragontales.student-train-result.v1"
        or train["scope"] != claim["scope"]
        or train["student_job_id"] != claim["student_job_id"]
        or train["claim_sha256"] != _sha256(claim_raw)
        or train["runner_sha256"] != claim["definition"]["recipe_sha256"]
        or not isinstance(train_outcome, dict)
        or tuple(train_outcome)
        != ("kind", "adapter_manifest", "merged_model_manifest")
        or train_outcome["kind"] != "succeeded"
    ):
        raise ValueError("student train result does not authorize fanout")
    _artifact_ref(
        train_outcome["adapter_manifest"],
        "student adapter manifest",
        claim,
    )
    merged_manifest = _artifact_ref(
        train_outcome["merged_model_manifest"],
        "student merged model manifest",
        claim,
    )
    train_started = _time(train["started_at"], "student train start")
    train_finished = _time(train["finished_at"], "student train finish")
    if (
        train_started < claim_started
        or train_started > train_finished
        or train_finished > expires
        or _integer(train["observed_gpu_seconds"], "student train GPU seconds")
        > MAX_TRAIN_GPU_SECONDS
    ):
        raise ValueError("student train result has an invalid execution interval")
    if train["gpu_evidence"] is not None:
        _artifact_ref(train["gpu_evidence"], "student train GPU evidence", claim)

    fanout_raw = _read(fanout_claim_path, MAX_CLAIM_BYTES)
    fanout = _keys(
        _canonical(fanout_raw, "student fanout claim", False),
        FANOUT_KEYS,
        "student fanout claim",
    )
    created = _time(fanout["created_at"], "student fanout creation")
    fanout_expires = _time(fanout["expires_at"], "student fanout expiry")
    fanout_total = _integer(
        fanout["max_total_gpu_seconds"], "student fanout total GPU seconds"
    )
    now = dt.datetime.now(dt.timezone.utc)
    if (
        fanout["schema_version"] != "dragontales.student-fanout-claim.v4"
        or fanout["scope"] != claim["scope"]
        or fanout["student_job_id"] != claim["student_job_id"]
        or fanout["train_result_sha256"] != _sha256(train_raw)
        or fanout["recipe_sha256"] != claim["definition"]["recipe_sha256"]
        or _runtime_image_reference(fanout["student_branch_runtime_image_reference"])
        != claim["definition"]["student_branch_runtime_image_reference"]
        or fanout_total != claim["definition"]["max_total_gpu_seconds"]
        or fanout_total != MAX_TOTAL_GPU_SECONDS
        or fanout_expires != expires
        or created < train_finished
        or created >= fanout_expires
        or (
            now < created
            or now >= created + dt.timedelta(minutes=15)
            or now >= fanout_expires
        )
    ):
        raise ValueError("student fanout claim differs from its train authority")
    branches = fanout["branches"]
    if not isinstance(branches, list) or len(branches) != len(VARIANTS):
        raise ValueError("student fanout claim does not contain three branches")
    branch = None
    for expected_variant, item in zip(VARIANTS, branches):
        _keys(item, BRANCH_CLAIM_KEYS, "student branch claim")
        _integer(item["max_gpu_seconds"], "student branch GPU seconds")
        definition = {
            "schema_version": "dragontales.student-branch-definition.v2",
            "student_job_id": claim["student_job_id"],
            "variant": expected_variant,
            "train_result_sha256": fanout["train_result_sha256"],
            "recipe_sha256": fanout["recipe_sha256"],
            "student_branch_runtime_image_reference": fanout[
                "student_branch_runtime_image_reference"
            ],
            "max_gpu_seconds": MAX_BRANCH_GPU_SECONDS,
            "expires_at": fanout["expires_at"],
        }
        if item != {
            "schema_version": "dragontales.student-branch-claim.v2",
            "branch_id": _sha256(_compact(definition)),
            "variant": expected_variant,
            "max_gpu_seconds": MAX_BRANCH_GPU_SECONDS,
        }:
            raise ValueError("student branch claim differs from its canonical definition")
        if expected_variant == variant:
            branch = item
    if branch is None:
        raise ValueError("student fanout claim has no requested branch")
    return (
        claim_raw,
        input_raw,
        claim,
        inputs,
        train_raw,
        fanout_raw,
        fanout,
        branch,
        merged_manifest,
        expires,
    )


def _write_new(path: Path, raw: bytes, mode=0o400) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _artifact_key(claim, digest: str) -> str:
    return _artifact_prefix(claim) + digest


class ArtifactStore:
    def __init__(self, output: Path, claim):
        self.directory = output / "artifact"
        self.directory.mkdir(mode=0o700)
        self.claim = claim
        self.files = {}
        self.total = 0

    def _record(self, digest: str, size: int):
        key = _artifact_key(self.claim, digest)
        item = {
            "object_key": key,
            "relative_path": digest,
            "sha256": digest,
            "bytes": size,
        }
        previous = self.files.get(key)
        if previous is not None and previous != item:
            raise ValueError("content-addressed upload collision")
        if previous is None:
            self.files[key] = item
            self.total += size
        if len(self.files) > MAX_UPLOAD_FILES or self.total > MAX_UPLOAD_TOTAL_BYTES:
            raise ValueError("student upload closure exceeds its fixed bound")
        return {"object_key": key, "sha256": digest, "bytes": size}

    def add_bytes(self, raw: bytes):
        if not raw:
            raise ValueError("student artifact is empty")
        digest = _sha256(raw)
        path = self.directory / digest
        if not path.exists():
            _write_new(path, raw)
        elif path.read_bytes() != raw:
            raise ValueError("student artifact digest collision")
        return self._record(digest, len(raw))

    def add_path(self, source: Path):
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"student output is not a regular file: {source}")
        before = source.stat()
        if before.st_nlink != 1 or before.st_size <= 0:
            raise ValueError(f"student output is empty or linked: {source}")
        temporary = self.directory / f"part-{len(self.files):04d}"
        digest = hashlib.sha256()
        size = 0
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        try:
            with source.open("rb") as reader, os.fdopen(descriptor, "wb", closefd=False) as writer:
                while chunk := reader.read(1024 * 1024):
                    writer.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                    if size > MAX_UPLOAD_TOTAL_BYTES:
                        raise ValueError("student artifact exceeds the fixed upload bound")
                writer.flush()
                os.fsync(writer.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            os.close(descriptor)
        after = source.stat()
        stable = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
        if size != before.st_size or any(getattr(before, key) != getattr(after, key) for key in stable):
            temporary.unlink()
            raise ValueError(f"student output changed while sealing: {source}")
        value = digest.hexdigest()
        destination = self.directory / value
        if destination.exists():
            temporary.unlink()
            if destination.stat().st_size != size:
                raise ValueError("student artifact digest collision")
        else:
            temporary.rename(destination)
        return self._record(value, size)

    def add_json(self, value):
        raw = _line(value)
        if len(raw) > MAX_RESULT_ARTIFACT_BYTES:
            raise ValueError("student result artifact exceeds 16 MiB")
        return self.add_bytes(raw)

    def seal(self):
        if any(not HEX64.fullmatch(path.name) for path in self.directory.iterdir()):
            raise ValueError("student artifact directory contains a non-content file")
        for path in self.directory.iterdir():
            path.chmod(0o400)
        self.directory.chmod(0o500)

    def upload(self):
        return {
            "schema_version": "dragontales.student-upload.v1",
            "student_job_id": self.claim["student_job_id"],
            "files": [self.files[key] for key in sorted(self.files)],
        }


def _inventory(directory: Path):
    if not directory.is_absolute() or directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"student model is not one absolute directory: {directory}")
    root = directory.lstat()
    if root.st_uid != os.geteuid() or root.st_mode & 0o022:
        raise ValueError("student model root must be current-owner and protected from mutation")
    entries = sorted(directory.iterdir(), key=lambda item: item.name)
    if not 1 <= len(entries) <= MAX_UPLOAD_FILES:
        raise ValueError("student model has an invalid file count")
    inventory = []
    for path in entries:
        if SAFE_NAME.fullmatch(path.name) is None:
            raise ValueError(f"student model contains an unsafe entry: {path.name}")
        digest = hashlib.sha256()
        size = 0
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        before = os.fstat(descriptor)
        try:
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or before.st_nlink != 1
                or before.st_mode & 0o022
            ):
                raise ValueError(f"student model file metadata is unsafe: {path.name}")
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        stable = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
        if size <= 0 or size != before.st_size or any(
            getattr(before, key) != getattr(after, key) for key in stable
        ):
            raise ValueError(f"student model file changed while hashing: {path.name}")
        inventory.append({"relative_path": path.name, "sha256": digest.hexdigest(), "bytes": size})
    raw = _compact(inventory)
    return inventory, hashlib.sha256(MODEL_INVENTORY_DOMAIN + raw).hexdigest()


def _manifest_file(relative_path: str, reference):
    return {
        "relative_path": relative_path,
        "object_key": reference["object_key"],
        "sha256": reference["sha256"],
        "bytes": reference["bytes"],
    }


def _artifact_reference(reference):
    return {
        "object_key": reference["object_key"],
        "sha256": reference["sha256"],
        "bytes": reference["bytes"],
    }


def _load_module(name: str, path: Path):
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise ValueError(f"could not load fixed runtime authority: {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _deploy() -> Path:
    return Path(__file__).resolve().parents[1]


def _h100():
    return _load_module("dragontales_h100_runtime", _deploy() / "h100-fp8-runtime/runtime.py")


def _train_image_probe():
    import torch

    probe = {
        "stage": "train",
        "schema_version": "dragontales.student-train-image-probe.v1",
        "platform": "linux/amd64",
        "prime_rl_version": importlib.metadata.version("prime-rl"),
        "accelerate_version": importlib.metadata.version("accelerate"),
        "peft_version": importlib.metadata.version("peft"),
        "torch_cuda_version": torch.version.cuda,
    }
    if probe != {
        "stage": "train",
        "schema_version": "dragontales.student-train-image-probe.v1",
        "platform": "linux/amd64",
        "prime_rl_version": "0.9.0",
        "accelerate_version": "1.14.0",
        "peft_version": "0.20.0",
        "torch_cuda_version": "12.8",
    }:
        raise ValueError("student train image differs from its fixed runtime")
    return probe


def _model_environment(overrides=None):
    def sensitive(name: str) -> bool:
        upper = name.upper()
        return (
            upper in SENSITIVE_ENV_NAMES
            or upper.startswith(SENSITIVE_ENV_PREFIXES)
            or upper.endswith(("_ENDPOINT", "_URL"))
            or not SENSITIVE_ENV_WORDS.isdisjoint(upper.split("_"))
        )

    environment = {name: value for name, value in os.environ.items() if not sensitive(name)}
    environment.update(MODEL_OFFLINE_ENV)
    if overrides is not None:
        environment.update(overrides)
    return environment


def _verify_base_model() -> str:
    verifier = _load_module(
        "dragontales_qwen3_4b_release",
        _deploy() / "models/qwen3-4b-instruct-2507/verify.py",
    )
    verifier.verify_tree(MODEL)
    inventory = [
        {"relative_path": name, "sha256": digest, "bytes": size}
        for name, digest, size, _source in verifier.FILES
    ]
    return hashlib.sha256(MODEL_INVENTORY_DOMAIN + _compact(inventory)).hexdigest()


def _run_checked(command, log: Path, timeout: int, overrides=None):
    with log.open("xb") as handle:
        process = subprocess.Popen(
            [str(item) for item in command],
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            env=_model_environment(overrides),
            start_new_session=True,
        )
        try:
            status = process.wait(timeout=timeout)
        except BaseException:
            _stop(process)
            raise
    if status != 0:
        raise RuntimeError(f"subprocess failed with status {status}: {command[0]}")


def _prepare_training(input_raw: bytes, inputs, working: Path, model_manifest_sha256: str):
    contract = _load_module("dragontales_adapter_train", _deploy() / "adapter_train/contract.py")
    launcher = RUN / "launcher"
    prepared = launcher / "prepared"
    RUN.mkdir(mode=0o700, parents=True)
    launcher.mkdir(mode=0o700)
    prepared.mkdir(mode=0o700)
    rows = []
    provider = inputs["teacher_provider_binding_sha256"]
    for row in inputs["train"]:
        target = row["target"]
        messages = contract._source_messages(
            {"model": row["projection"]["model"], "messages": _model_messages(row["projection"])}
        )
        messages.append({"role": "assistant", "content": target})
        rows.append(
            {
                "schema_version": "dragontales.adapter-train-row.v1",
                "trace_id": row["request_sha256"],
                "request_sha256": row["request_sha256"],
                "outcome_submission_sha256": hashlib.sha256(
                    b"dragontales.student-target.v1\0" + target.encode()
                ).hexdigest(),
                "target_kind": "teacher_distillation",
                "target_authority": provider,
                "target_sha256": _sha256(target.encode()),
                "messages": messages,
            }
        )
    dataset = b"".join(_line(row) for row in rows)
    _write_new(prepared / "train.jsonl", dataset, 0o444)
    model_profile_sha256 = _sha256(_read(MODEL / "config.json", 1024 * 1024))
    builder_sha256 = contract.package_sha256(_deploy() / "adapter_train")
    config = contract._config(
        _sha256(input_raw),
        model_manifest_sha256,
        model_profile_sha256,
        builder_sha256,
    )
    _write_new(launcher / "dragontales-sft.toml", config, 0o444)
    prepared.chmod(0o500)
    launcher.chmod(0o500)
    environment = {"PYTHONPATH": str(_deploy()), "PYTHONDONTWRITEBYTECODE": "1"}
    config_path = launcher / "dragontales-sft.toml"
    _run_checked(
        [PRIME_PYTHON, "-m", "adapter_train.sft_entrypoint", "preflight", "--config", config_path],
        working / "train-preflight.log",
        300,
        environment,
    )
    _run_checked(
        [
            TORCHRUN,
            "--standalone",
            "--nnodes=1",
            "--nproc-per-node=1",
            "-m",
            "adapter_train.sft_entrypoint",
            "@",
            config_path,
            "--output-dir",
            "/runs",
            "--run.name",
            "run",
        ],
        working / "train.log",
        7200,
        environment,
    )
    adapter = RUN / "adapter"
    expected = [".finished", "adapter_config.json", "adapter_model.safetensors"]
    if sorted(item.name for item in adapter.iterdir()) != expected:
        raise ValueError("student training did not emit the exact PEFT adapter")


def _verify_retained_model(model: Path, manifest_path: Path, runtime):
    manifest = _keys(
        _canonical(
            runtime._read_regular(manifest_path, MAX_RESULT_ARTIFACT_BYTES, 0o400),
            "student model manifest",
            True,
        ),
        MODEL_MANIFEST_KEYS,
        "student model manifest",
    )
    job_id = _digest(manifest["student_job_id"], "student job")
    inventory_sha256 = _digest(manifest["inventory_sha256"], "student model inventory")
    variant = manifest["variant"]
    files = manifest["files"]
    if (
        manifest["schema_version"] != "dragontales.student-model-manifest.v1"
        or variant not in VARIANTS
        or manifest["retained"] is not True
        or not isinstance(files, list)
        or not 1 <= len(files) <= MAX_UPLOAD_FILES
        or _integer(manifest["file_count"], "student model file count", 1, MAX_UPLOAD_FILES)
        != len(files)
    ):
        raise ValueError("student model manifest is not one retained candidate")
    expected = []
    total = 0
    for item in files:
        _keys(item, MODEL_FILE_KEYS, "student model file")
        relative = item["relative_path"]
        digest = _digest(item["sha256"], "student model file")
        size = _integer(item["bytes"], "student model file bytes", 1)
        if (
            not isinstance(relative, str)
            or SAFE_NAME.fullmatch(relative) is None
            or not isinstance(item["object_key"], str)
            or not item["object_key"].startswith("dt/v3/")
            or not item["object_key"].endswith(f"/artifacts/{job_id}/{digest}")
        ):
            raise ValueError("student model manifest contains an unsafe file binding")
        expected.append({"relative_path": relative, "sha256": digest, "bytes": size})
        total += size
        if total > MAX_UPLOAD_TOTAL_BYTES:
            raise ValueError("retained student model exceeds the fixed artifact bound")
    if [item["relative_path"] for item in expected] != sorted(
        {item["relative_path"] for item in expected}
    ):
        raise ValueError("student model manifest files are not sorted and unique")
    inventory, observed_inventory_sha256 = _inventory(model)
    if (
        expected != inventory
        or observed_inventory_sha256 != inventory_sha256
        or _integer(manifest["artifact_bytes"], "student model artifact bytes", 1)
        != total
    ):
        raise ValueError("retained student model differs from its exact inventory")
    verification = runtime.verify_model(model)
    expected_variant = "bf16" if variant == "bf16" else variant.removesuffix("_fp8")
    if verification.get("variant") != expected_variant:
        raise ValueError("retained student model differs from its declared H100 variant")
    return verification


def _serving_argv(runtime, model: Path, alias: str):
    _chat_template()
    return [
        str(runtime.VLLM),
        "serve",
        str(model),
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--served-model-name",
        alias,
        "--tensor-parallel-size",
        "1",
        "--dtype",
        "auto",
        "--kv-cache-dtype",
        "auto",
        "--max-model-len",
        "4096",
        "--max-num-seqs",
        "8",
        "--max-num-batched-tokens",
        "4096",
        "--gpu-memory-utilization",
        "0.80",
        "--no-enable-prefix-caching",
        "--generation-config",
        "vllm",
        "--chat-template",
        str(CHAT_TEMPLATE),
        "--reasoning-parser",
        REASONING_PARSER,
        "--no-enable-log-requests",
        "--disable-log-stats",
    ]


def serve(
    model: Path,
    manifest: Path,
    alias: str,
    api_key_file: Path | None,
    trusted_ingress_auth: bool = False,
):
    _require_role("student-branch")
    if not isinstance(alias, str) or LOGICAL_MODEL_ALIAS.fullmatch(alias) is None:
        raise ValueError("logical model alias is invalid or oversized")
    if type(trusted_ingress_auth) is not bool or (api_key_file is None) == (
        trusted_ingress_auth is False
    ):
        raise ValueError("exactly one serving authentication mode is required")
    runtime = _h100()
    _verify_retained_model(model, manifest, runtime)
    probe = runtime.image_probe()
    if probe["fp8_dynamic_supported"] is not True or probe["fp8_static_supported"] is not True:
        raise ValueError("student serving image package probe failed")
    runtime.verify_hardware()
    executable = runtime.VLLM.lstat()
    if (
        not stat.S_ISREG(executable.st_mode)
        or executable.st_uid != IMAGE_OWNER_UID
        or executable.st_nlink != 1
        or executable.st_mode & 0o022
        or not os.access(runtime.VLLM, os.X_OK)
    ):
        raise ValueError("fixed vLLM executable is unavailable or mutable")
    overrides = {
        "VLLM_ALLOW_RUNTIME_LORA_UPDATING": "False",
        "VLLM_PLUGINS": "",
    }
    if api_key_file is not None:
        overrides["VLLM_API_KEY"] = runtime._api_key(api_key_file)
    environment = _model_environment(overrides)
    os.execve(runtime.VLLM, _serving_argv(runtime, model, alias), environment)


def _merge(model: Path, adapter: Path, output: Path):
    if sorted(item.name for item in adapter.iterdir()) != [
        ".finished",
        "adapter_config.json",
        "adapter_model.safetensors",
    ]:
        raise ValueError("student merge input is not the exact PEFT adapter")
    output.mkdir(mode=0o700)
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(model, local_files_only=True, trust_remote_code=False)
    base = AutoModelForCausalLM.from_pretrained(
        model,
        local_files_only=True,
        trust_remote_code=False,
        use_safetensors=True,
        torch_dtype="auto",
        device_map="auto",
        low_cpu_mem_usage=True,
    )
    peft = PeftModel.from_pretrained(base, adapter, local_files_only=True, is_trainable=False)
    merged = peft.merge_and_unload(safe_merge=True, progressbar=False)
    merged.save_pretrained(output, safe_serialization=True, max_shard_size="5GB")
    tokenizer.save_pretrained(output)


def _quant_recipe(variant: str):
    legacy = "dynamic" if variant == "dynamic_fp8" else "static"
    return {
        "schema_version": "dragontales.h100-fp8-recipe.v1",
        "variant": legacy,
        "entrypoint": "oneshot",
        "modifier": "QuantizationModifier",
        "scheme": "FP8_DYNAMIC" if legacy == "dynamic" else "FP8",
        "targets": ["Linear"],
        "ignore": ["lm_head"],
        "activation_observer": None if legacy == "dynamic" else "static_minmax",
        "batch_size": 1,
        "num_calibration_samples": 0 if legacy == "dynamic" else 128,
        "shuffle_calibration_samples": False,
        "max_sequence_length": 4096,
        "save_compressed": True,
    }


def _quantize(variant: str, model: Path, input_path: Path, output: Path):
    if variant not in {"dynamic_fp8", "static_fp8"}:
        raise ValueError("student quantization variant is invalid")
    if (
        importlib.metadata.version("llmcompressor") != LLM_COMPRESSOR_VERSION
        or importlib.metadata.version("compressed-tensors") != COMPRESSED_TENSORS_VERSION
    ):
        raise ValueError("student quantization packages differ from the fixed recipe")
    recipe = _quant_recipe(variant)
    from llmcompressor import oneshot
    from llmcompressor.modifiers.quantization import QuantizationModifier

    modifier = QuantizationModifier(
        targets=recipe["targets"], scheme=recipe["scheme"], ignore=recipe["ignore"]
    )
    call = {
        "model": str(model),
        "recipe": modifier,
        "output_dir": str(output),
        "save_compressed": True,
        "trust_remote_code_model": False,
    }
    if variant == "static_fp8":
        from datasets import Dataset
        from transformers import AutoTokenizer

        inputs = _canonical(_read(input_path, MAX_INPUT_BYTES), "student input", True)
        calibration = inputs["calibration"]
        if len(calibration) != 128:
            raise ValueError("static FP8 requires exactly 128 calibration rows")
        tokenizer = AutoTokenizer.from_pretrained(model, local_files_only=True, trust_remote_code=False)
        chat_template = _chat_template()
        projected = [
            tokenizer.apply_chat_template(
                row["projection"]["messages"],
                tokenize=False,
                add_generation_prompt=True,
                enable_thinking=True,
                chat_template=chat_template,
            )
            for row in calibration
        ]
        if any(not isinstance(text, str) or not text for text in projected):
            raise ValueError("static FP8 tokenizer emitted an empty calibration projection")
        call.update(
            tokenizer=tokenizer,
            dataset=Dataset.from_dict({"text": projected}),
            max_seq_length=4096,
            num_calibration_samples=128,
            batch_size=1,
            shuffle_calibration_samples=False,
        )
    oneshot(**call)


def _subprocess_stage(interpreter: Path, command: str, arguments, log: Path, timeout: int):
    if not interpreter.is_file():
        raise ValueError(f"fixed student interpreter is unavailable: {interpreter}")
    _run_checked(
        [interpreter, Path(__file__).resolve(), command] + list(arguments),
        log,
        timeout,
        {"PYTHONDONTWRITEBYTECODE": "1"},
    )


def _request_json(url: str, payload=None, timeout=60):
    data = None if payload is None else _compact(payload)
    headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method="GET" if data is None else "POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise ValueError(f"candidate returned HTTP {response.status}")
        content_type = response.headers.get_content_type()
        encoding = response.headers.get("Content-Encoding")
        if content_type != "application/json" or encoding not in (None, "identity"):
            raise ValueError("candidate returned an unsupported HTTP representation")
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("candidate response exceeds 1 MiB")
    return raw, _json(raw, "candidate response")


def _wait_ready(process: subprocess.Popen, endpoint: str):
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"vLLM exited before readiness with status {process.returncode}")
        try:
            _request_json(endpoint + "/v1/models", timeout=2)
            return
        except (OSError, ValueError, urllib.error.URLError):
            time.sleep(1)
    raise TimeoutError("vLLM did not become ready within 900 seconds")


def _stop(process: subprocess.Popen):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=30)


def _response_text(value):
    if not isinstance(value, dict) or value.get("model") != MODEL_ALIAS:
        raise ValueError("candidate response model differs from the fixed alias")
    choices = value.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("candidate response does not contain exactly one choice")
    choice = choices[0]
    message = choice.get("message") if isinstance(choice, dict) else None
    if (
        choice.get("index") != 0
        or choice.get("finish_reason") != "stop"
        or not isinstance(message, dict)
        or message.get("role") != "assistant"
        or not isinstance(message.get("content"), str)
        or message.get("refusal") is not None
        or message.get("tool_calls") is not None
        or message.get("function_call") is not None
    ):
        raise ValueError("candidate response is not one stopped assistant text choice")
    content = message["content"]
    if (
        not content.strip()
        or len(content.encode()) > MAX_TARGET_BYTES
        or "<think>" in content
        or "</think>" in content
        or message.get("reasoning_content") is not None
        and not isinstance(message["reasoning_content"], str)
    ):
        raise ValueError("candidate text is empty or exceeds the fixed DEV receipt bound")
    return content


def _levenshtein(left, right):
    rows, columns = (left, right) if len(left) >= len(right) else (right, left)
    previous = list(range(len(columns) + 1))
    current = [0] * (len(columns) + 1)
    for row_index, row in enumerate(rows):
        current[0] = row_index + 1
        for column_index, column in enumerate(columns):
            current[column_index + 1] = min(
                previous[column_index + 1] + 1,
                current[column_index] + 1,
                previous[column_index] + (row != column),
            )
        previous, current = current, previous
    return previous[len(columns)]


def _score(evaluator: str, reference: str, candidate: str):
    if evaluator == "exact_text_v1":
        return 1.0 if reference == candidate else 0.0
    if evaluator == "normalized_text_v1":
        normalize = lambda value: " ".join(
            "".join(character.lower() for character in word) for word in RUST_WORDS.findall(value)
        )
        return 1.0 if normalize(reference) == normalize(candidate) else 0.0
    left = list(reference) if evaluator == "char_similarity_v1" else RUST_WORDS.findall(reference)
    right = list(candidate) if evaluator == "char_similarity_v1" else RUST_WORDS.findall(candidate)
    if len(left) * len(right) > MAX_EDIT_DISTANCE_CELLS:
        raise ValueError("student DEV edit-distance input exceeds its fixed work bound")
    distance = _levenshtein(left, right)
    denominator = max(len(left), 1)
    return max(0.0, min(1.0, 1.0 - distance / denominator))


def _call_dev(endpoint: str, row):
    started = time.monotonic()
    try:
        request = {
            "model": MODEL_ALIAS,
            "messages": row["projection"]["messages"],
            "chat_template_kwargs": {"enable_thinking": True},
            "temperature": 0.0,
            "top_p": 1.0,
            "max_tokens": 1024,
            "seed": 0,
            "n": 1,
            "stream": False,
        }
        raw, value = _request_json(endpoint + "/v1/chat/completions", request)
        content = _response_text(value)
        score = _score(row["evaluator_id"], row["reference"], content)
        error = None
        response_sha256 = _sha256(raw)
        candidate = content
    except (OSError, ValueError, urllib.error.URLError):
        score = 0.0
        error = "provider_error"
        response_sha256 = None
        candidate = None
    latency = max(1, int((time.monotonic() - started) * 1000))
    return score, latency, error, response_sha256, candidate


def _receipt(variant: str, claim, dev, results):
    rows = []
    scores = []
    latencies = []
    errors = 0
    for row, (score, latency, error, response_sha256, candidate) in zip(dev, results):
        scores.append(score)
        latencies.append(latency)
        errors += error is not None
        rows.append(
            {
                "request_sha256": row["request_sha256"],
                "projection_sha256": _sha256(_compact(row["projection"])),
                "evaluator_id": row["evaluator_id"],
                "reference_sha256": _sha256(row["reference"].encode()),
                "response_sha256": response_sha256,
                "candidate": candidate,
                "score_bps": int(score * 10_000),
                "latency_ms": latency,
                "error": error,
            }
        )
    ordered = sorted(latencies)
    p95 = ordered[(len(ordered) * 95 + 99) // 100 - 1]
    mean = sum(scores) / len(scores)
    summary = {
        "dev_rows": len(dev),
        "mean_score_bps": int(mean * 10_000),
        "errors": errors,
        "total_latency_ms": sum(latencies),
        "p95_latency_ms": p95,
    }
    receipt = {
        "schema_version": "dragontales.student-dev-receipt.v1",
        "student_job_id": claim["student_job_id"],
        "variant": variant,
        "dev_set_sha256": claim["definition"]["dev_set_sha256"],
        "rows": rows,
        "summary": summary,
    }
    return receipt, summary


def _evaluate(variant: str, model: Path, claim, dev, working: Path):
    runtime = _h100()
    verification = runtime.verify_model(model)
    expected = "bf16" if variant == "bf16" else variant.removesuffix("_fp8")
    if verification["variant"] != expected:
        raise ValueError("candidate model differs from its fixed variant")
    host = "127.0.0.1"
    runtime.MODEL_PATH = model
    _chat_template()
    command = runtime.serving_argv() + [
        "--chat-template",
        str(CHAT_TEMPLATE),
        "--reasoning-parser",
        REASONING_PARSER,
    ]
    log_path = working / f"{variant}.vllm.log"
    environment = _model_environment()
    with log_path.open("xb") as log:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
            start_new_session=True,
        )
    endpoint = f"http://{host}:8000"
    try:
        _wait_ready(process, endpoint)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda row: _call_dev(endpoint, row), dev))
    finally:
        _stop(process)
    log_raw = _read(log_path, runtime.MAX_LOG_BYTES)
    selections = [
        line
        for line in log_raw.splitlines()
        if b"Selected " in line and b"for CompressedTensorsW8A8Fp8" in line
    ]
    occurrences = log_raw.count(runtime.KERNEL_SELECTION.encode())
    if (variant == "bf16" and selections) or (
        variant != "bf16"
        and (occurrences != 1 or len(selections) != 1 or runtime.KERNEL_SELECTION.encode() not in selections[0])
    ):
        raise ValueError("candidate startup log differs from the fixed H100 kernel selection")
    receipt, summary = _receipt(variant, claim, dev, results)
    kernel = {
        "variant": variant,
        "model_verification": verification,
        "startup_log_sha256": _sha256(log_raw),
        "kernel_selection": runtime.KERNEL_SELECTION if variant != "bf16" else None,
        "occurrences": occurrences,
    }
    return receipt, summary, kernel


def _gpu_probe(working: Path):
    output = working / "gpu-probe.json"
    _run_checked(
        [sys.executable, Path(__file__).resolve(), "_gpu_probe"],
        output,
        60,
    )
    return _canonical(_read(output, 1024 * 1024), "student GPU probe", True)


def _write_terminal(output, store, result):
    upload = store.upload()
    _keys(upload, UPLOAD_KEYS, "student upload")
    for item in upload["files"]:
        _keys(item, UPLOAD_FILE_KEYS, "student upload file")
    upload_raw = _line(upload)
    result_raw = _line(result)
    if len(upload_raw) > MAX_UPLOAD_BYTES or len(result_raw) > MAX_RESULT_BYTES:
        raise ValueError("student terminal output exceeds its fixed bound")
    store.seal()
    _write_new(output / "upload.json", upload_raw, 0o600)
    _write_new(output / "result.json", result_raw, 0o600)


def _model_artifact(store, claim, variant, model):
    inventory, inventory_sha256 = _inventory(model)
    files = [
        _manifest_file(item["relative_path"], store.add_path(model / item["relative_path"]))
        for item in inventory
    ]
    return store.add_json(
        {
            "schema_version": "dragontales.student-model-manifest.v1",
            "student_job_id": claim["student_job_id"],
            "variant": variant,
            "retained": True,
            "inventory_sha256": inventory_sha256,
            "file_count": len(inventory),
            "artifact_bytes": sum(item["bytes"] for item in inventory),
            "files": files,
        }
    )


def _diagnostics(store, stage, failure_class):
    return store.add_json(
        {
            "schema_version": "dragontales.student-stage-diagnostics.v1",
            "stage": stage,
            "failure_class": failure_class,
        }
    )


def _compute_alarm_seconds(expires, stage_max, now=None):
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    available = int((expires - now).total_seconds()) - TERMINALIZATION_GRACE_SECONDS
    if available <= 0:
        raise TimeoutError("student stage has no compute time before terminalization grace")
    return min(stage_max, available)


def _observed_gpu_seconds(started, finished, gpu_started, stage_max):
    if gpu_started is None:
        return 0
    wall = max(1, int((finished - started).total_seconds()) + 1)
    return min(max(1, int(time.monotonic() - gpu_started)), wall, stage_max)


def _clear_partial(output):
    for name in ("artifact", "working"):
        path = output / name
        if path.exists():
            shutil.rmtree(path)
    for name in ("result.json", "upload.json"):
        (output / name).unlink(missing_ok=True)


def _gpu_record(claim, probe, kernels, stage):
    if stage not in {"train", "branch"}:
        raise ValueError("student GPU evidence stage is invalid")
    if not isinstance(probe, dict) or probe.get("runtime", {}).get("stage") != stage:
        raise ValueError("student GPU probe differs from its stage")
    if stage == "train" and kernels:
        raise ValueError("student train GPU evidence cannot contain branch kernels")
    return {
        "schema_version": "dragontales.student-gpu-evidence.v2",
        "student_job_id": claim["student_job_id"],
        "mode": "h100",
        "recipe_sha256": claim["definition"]["recipe_sha256"],
        "device": probe["device"],
        "runtime": probe["runtime"],
        "kernels": kernels,
    }

def _train_terminal(claim_raw, claim, started, finished, gpu_seconds, gpu, outcome):
    return {
        "schema_version": "dragontales.student-train-result.v1",
        "scope": claim["scope"],
        "student_job_id": claim["student_job_id"],
        "claim_sha256": _sha256(claim_raw),
        "runner_sha256": claim["definition"]["recipe_sha256"],
        "started_at": _format_time(started),
        "finished_at": _format_time(finished),
        "gpu_evidence": gpu,
        "observed_gpu_seconds": gpu_seconds,
        "outcome": outcome,
    }


def train_stage(claim_path, input_path, output):
    _require_role("student-train")
    claim_raw, input_raw, claim, inputs, _claim_started, expires = load_inputs(
        claim_path, input_path, True
    )
    compute_seconds = _compute_alarm_seconds(expires, MAX_TRAIN_GPU_SECONDS)
    if not output.is_absolute() or output.exists() or not output.parent.is_dir():
        raise ValueError("student output must be one new absolute directory")
    if RUN.exists() or not MODEL.is_dir():
        raise ValueError("student container must provide /model and a new /runs/run")
    output.mkdir(mode=0o700)
    working = output / "working"
    working.mkdir(mode=0o700)
    started = dt.datetime.now(dt.timezone.utc)
    gpu_started = time.monotonic()
    gpu = None
    old_alarm = signal.getsignal(signal.SIGALRM)

    def expire(_signal, _frame):
        raise TimeoutError("student train stage exceeded its fixed deadline")

    signal.signal(signal.SIGALRM, expire)
    signal.alarm(compute_seconds)
    try:
        model_manifest_sha256 = _verify_base_model()
        _run_checked(
            [
                PRIME_PYTHON,
                "-m",
                "adapter_train.sft_entrypoint",
                "parity",
                "--model",
                MODEL,
                "--template",
                CHAT_TEMPLATE,
            ],
            working / "chat-template-parity.log",
            300,
            {"PYTHONPATH": str(_deploy()), "PYTHONDONTWRITEBYTECODE": "1"},
        )
        gpu = _gpu_record(claim, _gpu_probe(working), [], "train")
        _prepare_training(input_raw, inputs, working, model_manifest_sha256)
        adapter = RUN / "adapter"
        model = working / "bf16"
        _subprocess_stage(
            PRIME_PYTHON,
            "_merge",
            ["--model", MODEL, "--adapter", adapter, "--output", model],
            working / "merge.log",
            7200,
        )
        finished = dt.datetime.now(dt.timezone.utc)
        if finished > expires:
            raise TimeoutError("student train stage exceeded claim expiry")
        signal.alarm(0)
        store = ArtifactStore(output, claim)
        adapter_manifest = store.add_json(
            {
                "schema_version": "dragontales.student-adapter-manifest.v1",
                "student_job_id": claim["student_job_id"],
                "files": [
                    _manifest_file(name, store.add_path(adapter / name))
                    for name in ("adapter_config.json", "adapter_model.safetensors")
                ],
            }
        )
        model_manifest = _model_artifact(store, claim, "bf16", model)
        gpu_reference = store.add_json(gpu)
        result = _train_terminal(
            claim_raw,
            claim,
            started,
            finished,
            _observed_gpu_seconds(started, finished, gpu_started, MAX_TRAIN_GPU_SECONDS),
            _artifact_reference(gpu_reference),
            {
                "kind": "succeeded",
                "adapter_manifest": _artifact_reference(adapter_manifest),
                "merged_model_manifest": _artifact_reference(model_manifest),
            },
        )
        shutil.rmtree(working)
        _write_terminal(output, store, result)
    except BaseException as error:
        signal.alarm(0)
        finished = min(
            dt.datetime.now(dt.timezone.utc),
            expires,
            started + dt.timedelta(seconds=MAX_TRAIN_GPU_SECONDS),
        )
        _clear_partial(output)
        store = ArtifactStore(output, claim)
        failure = type(error).__name__.lower()[:128]
        result = _train_terminal(
            claim_raw,
            claim,
            started,
            finished,
            0
            if gpu is None
            else _observed_gpu_seconds(
                started, finished, gpu_started, MAX_TRAIN_GPU_SECONDS
            ),
            None if gpu is None else _artifact_reference(store.add_json(gpu)),
            {
                "kind": "failed",
                "failure_class": failure,
                "diagnostics": _artifact_reference(_diagnostics(store, "train_merge", failure)),
            },
        )
        _write_terminal(output, store, result)
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_alarm)


def _branch_terminal(
    claim, fanout_raw, branch_claim, variant, started, finished, gpu_seconds, outcome
):
    return {
        "schema_version": "dragontales.student-branch-result.v1",
        "scope": claim["scope"],
        "student_job_id": claim["student_job_id"],
        "branch_id": branch_claim["branch_id"],
        "fanout_claim_sha256": _sha256(fanout_raw),
        "runner_sha256": claim["definition"]["recipe_sha256"],
        "variant": variant,
        "started_at": _format_time(started),
        "finished_at": _format_time(finished),
        "observed_gpu_seconds": gpu_seconds,
        "outcome": outcome,
    }


def branch_stage(
    claim_path,
    input_path,
    train_result_path,
    fanout_claim_path,
    merged_model,
    merged_model_manifest,
    variant,
    output,
):
    _require_role("student-branch")
    (
        _claim_raw,
        _input_raw,
        claim,
        inputs,
        _train_raw,
        fanout_raw,
        fanout,
        branch_claim,
        merged_manifest_ref,
        expires,
    ) = _branch_inputs(
        claim_path,
        input_path,
        train_result_path,
        fanout_claim_path,
        variant,
    )
    compute_seconds = _compute_alarm_seconds(expires, MAX_BRANCH_GPU_SECONDS)
    manifest_raw = _read(merged_model_manifest, MAX_RESULT_ARTIFACT_BYTES)
    if (
        _sha256(manifest_raw) != merged_manifest_ref["sha256"]
        or len(manifest_raw) != merged_manifest_ref["bytes"]
    ):
        raise ValueError("materialized merged model manifest differs from train result")
    if not output.is_absolute() or output.exists() or not output.parent.is_dir():
        raise ValueError("student output must be one new absolute directory")
    output.mkdir(mode=0o700)
    working = output / "working"
    working.mkdir(mode=0o700)
    started = dt.datetime.now(dt.timezone.utc)
    gpu_started = time.monotonic()
    gpu = None
    receipt = None
    summary = None
    old_alarm = signal.getsignal(signal.SIGALRM)

    def expire(_signal, _frame):
        raise TimeoutError("student branch exceeded its fixed deadline")

    signal.signal(signal.SIGALRM, expire)
    signal.alarm(compute_seconds)
    try:
        runtime = _h100()
        _verify_retained_model(merged_model, merged_model_manifest, runtime)
        probe = _gpu_probe(working)
        gpu = _gpu_record(claim, probe, [], "branch")
        model = merged_model
        if variant != "bf16":
            model = working / variant
            _subprocess_stage(
                QUANT_PYTHON,
                "_quantize",
                [
                    "--variant",
                    variant,
                    "--model",
                    merged_model,
                    "--input",
                    input_path,
                    "--output",
                    model,
                ],
                working / f"{variant}.quantize.log",
                7200,
            )
        receipt, summary, kernel = _evaluate(
            variant, model, claim, inputs["dev"], working
        )
        gpu["kernels"].append(kernel)
        finished = dt.datetime.now(dt.timezone.utc)
        if finished > expires:
            raise TimeoutError("student branch exceeded claim expiry")
        signal.alarm(0)
        store = ArtifactStore(output, claim)
        model_reference = _model_artifact(store, claim, variant, model)
        receipt_reference = store.add_json(receipt)
        gpu_reference = store.add_json(gpu)
        result = _branch_terminal(
            claim,
            fanout_raw,
            branch_claim,
            variant,
            started,
            finished,
            _observed_gpu_seconds(started, finished, gpu_started, MAX_BRANCH_GPU_SECONDS),
            {
                "kind": "succeeded",
                "gpu_evidence": _artifact_reference(gpu_reference),
                "model_manifest": _artifact_reference(model_reference),
                "dev_receipt": _artifact_reference(receipt_reference),
                "summary": summary,
            },
        )
        shutil.rmtree(working)
        _write_terminal(output, store, result)
    except BaseException as error:
        signal.alarm(0)
        finished = min(
            dt.datetime.now(dt.timezone.utc),
            expires,
            started + dt.timedelta(seconds=MAX_BRANCH_GPU_SECONDS),
        )
        _clear_partial(output)
        store = ArtifactStore(output, claim)
        failure = type(error).__name__.lower()[:128]
        receipt_reference = None
        if receipt is not None:
            receipt_reference = _artifact_reference(store.add_json(receipt))
        result = _branch_terminal(
            claim,
            fanout_raw,
            branch_claim,
            variant,
            started,
            finished,
            0
            if gpu is None
            else _observed_gpu_seconds(
                started, finished, gpu_started, MAX_BRANCH_GPU_SECONDS
            ),
            {
                "kind": "failed",
                "failure_class": failure,
                "diagnostics": _artifact_reference(_diagnostics(store, variant, failure)),
                "gpu_evidence": None
                if gpu is None
                else _artifact_reference(store.add_json(gpu)),
                "model_manifest": None,
                "dev_receipt": receipt_reference,
                "summary": summary,
            },
        )
        _write_terminal(output, store, result)
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_alarm)


def _internal_parser(command: str, arguments):
    parser = argparse.ArgumentParser(add_help=False)
    if command == "_merge":
        parser.add_argument("--model", type=Path, required=True)
        parser.add_argument("--adapter", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
    else:
        parser.add_argument("--variant", choices=("dynamic_fp8", "static_fp8"), required=True)
        parser.add_argument("--model", type=Path, required=True)
        parser.add_argument("--input", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(arguments)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "_gpu_probe":
        role = _artifact_role()
        import torch

        if role == "student-train":
            image_probe = _train_image_probe()
            if (
                not torch.cuda.is_available()
                or torch.cuda.device_count() != 1
                or torch.cuda.get_device_capability(0) < (9, 0)
                or torch.cuda.get_device_properties(0).total_memory < 78 * 1024**3
            ):
                raise ValueError("student train runtime requires exactly one H100")
        else:
            runtime = _h100()
            image_probe = {"stage": "branch", **runtime.image_probe()}
            runtime.verify_hardware()

        properties = torch.cuda.get_device_properties(0)
        observed = subprocess.run(
            [
                "/usr/bin/nvidia-smi",
                "--query-gpu=uuid",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=_model_environment(),
        ).stdout.strip().splitlines()
        if len(observed) != 1 or re.fullmatch(r"GPU-[0-9A-Fa-f-]{16,64}", observed[0]) is None:
            raise ValueError("student runtime did not observe one canonical GPU UUID")
        sys.stdout.buffer.write(
            _line(
                {
                    "device": {
                        "uuid": observed[0],
                        "cuda": torch.version.cuda,
                        "name": properties.name,
                        "compute_capability": list(torch.cuda.get_device_capability(0)),
                        "total_memory_bytes": properties.total_memory,
                    },
                    "runtime": image_probe,
                }
            )
        )
        return
    if len(sys.argv) > 1 and sys.argv[1] in {"_merge", "_quantize"}:
        command = sys.argv[1]
        arguments = _internal_parser(command, sys.argv[2:])
        if command == "_merge":
            _require_role("student-train")
            _merge(arguments.model, arguments.adapter, arguments.output)
        else:
            _require_role("student-branch")
            _quantize(arguments.variant, arguments.model, arguments.input, arguments.output)
        return
    parser = argparse.ArgumentParser(prog="student-run")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("recipe-sha256")
    train_command = commands.add_parser("train")
    train_command.add_argument("--claim", type=Path, required=True)
    train_command.add_argument("--input", type=Path, required=True)
    train_command.add_argument("--output", type=Path, required=True)
    branch_command = commands.add_parser("branch")
    branch_command.add_argument("--claim", type=Path, required=True)
    branch_command.add_argument("--input", type=Path, required=True)
    branch_command.add_argument("--train-result", type=Path, required=True)
    branch_command.add_argument("--fanout-claim", type=Path, required=True)
    branch_command.add_argument("--merged-model", type=Path, required=True)
    branch_command.add_argument("--merged-model-manifest", type=Path, required=True)
    branch_command.add_argument("--variant", choices=VARIANTS, required=True)
    branch_command.add_argument("--output", type=Path, required=True)
    serve_command = commands.add_parser("serve")
    serve_command.add_argument("--model", type=Path, required=True)
    serve_command.add_argument("--model-manifest", type=Path, required=True)
    serve_command.add_argument("--model-alias", required=True)
    serve_auth = serve_command.add_mutually_exclusive_group(required=True)
    serve_auth.add_argument("--api-key-file", type=Path)
    serve_auth.add_argument("--trusted-ingress-auth", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "recipe-sha256":
        print(recipe_sha256())
    elif arguments.command == "serve":
        serve(
            arguments.model,
            arguments.model_manifest,
            arguments.model_alias,
            arguments.api_key_file,
            arguments.trusted_ingress_auth,
        )
    elif arguments.command == "train":
        train_stage(
            arguments.claim,
            arguments.input,
            arguments.output,
        )
    else:
        branch_stage(
            arguments.claim,
            arguments.input,
            arguments.train_result,
            arguments.fanout_claim,
            arguments.merged_model,
            arguments.merged_model_manifest,
            arguments.variant,
            arguments.output,
        )


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        raise SystemExit(str(error)) from error
