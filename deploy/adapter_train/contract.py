from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path


MAX_BUNDLE_BYTES = 64 * 1024 * 1024
MAX_DATASET_BYTES = 128 * 1024 * 1024
MAX_ROWS = 10_000
MIN_ROWS = 50
MAX_TARGET_BYTES = 64 * 1024
MAX_SOURCE_BYTES = 1024 * 1024
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
BUILDER_FILES = ("__init__.py", "contract.py", "sft_entrypoint.py")

TOP_KEYS = (
    "schema_version",
    "scope",
    "release_sha256",
    "release_seed_sha256",
    "analysis_id",
    "analysis_proposal_sha256",
    "analysis_provider_binding_sha256",
    "entries",
)
ROW_KEYS = (
    "trace_id",
    "request_sha256",
    "request",
    "trace_payload_sha256",
    "outcome_payload_sha256",
    "outcome_submission_sha256",
    "target_source",
    "target",
    "target_sha256",
)
PREPARED_KEYS = {
    "schema_version",
    "trace_id",
    "request_sha256",
    "outcome_submission_sha256",
    "target_kind",
    "target_authority",
    "target_sha256",
    "messages",
}


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def _load_canonical_line(raw: bytes, name: str) -> object:
    if not raw.endswith(b"\n"):
        raise ValueError(f"{name} must end in one LF")
    value = json.loads(raw[:-1], object_pairs_hook=_object)
    if raw != _canonical(value) + b"\n":
        raise ValueError(f"{name} must be canonical compact JSON plus one LF")
    return value


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def package_sha256(directory: Path) -> str:
    digest = hashlib.sha256(b"dragontales-adapter-train-builder-v1\0")
    for name in BUILDER_FILES:
        item = directory / name
        if item.is_symlink() or not item.is_file():
            raise ValueError(f"builder source is missing or unsafe: {name}")
        raw = item.read_bytes()
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(str(len(raw)).encode())
        digest.update(b"\0")
        digest.update(raw)
    return digest.hexdigest()


def _write(path: Path, raw: bytes, mode: int = 0o400) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or HEX64.fullmatch(value) is None:
        raise ValueError(f"invalid {name}")
    return value


def _source_messages(request: object) -> list[dict[str, str]]:
    if (
        not isinstance(request, dict)
        or tuple(request) != ("model", "messages")
        or not isinstance(request["model"], str)
        or not request["model"]
    ):
        raise ValueError("adapter train request is not Chat Completions JSON")
    messages = request["messages"]
    if not isinstance(messages, list) or not messages:
        raise ValueError("adapter train request has no Chat messages")
    prepared = []
    source_bytes = 0
    user_messages = 0
    for message in messages:
        if not isinstance(message, dict) or tuple(message) != ("role", "content"):
            raise ValueError("source Chat message has unsupported fields")
        role, content = message["role"], message["content"]
        if role not in {"system", "developer", "user", "assistant"} or not isinstance(content, str):
            raise ValueError("source history must be an ordinary text Chat Completions thread")
        source_bytes += len(content.encode("utf-8"))
        user_messages += role == "user"
        prepared.append({"role": role, "content": content})
    if user_messages == 0 or source_bytes > MAX_SOURCE_BYTES:
        raise ValueError("source history has no bounded user message")
    return prepared


def eligible_rows(bundle: object) -> list[dict]:
    if not isinstance(bundle, dict) or tuple(bundle) != TOP_KEYS:
        raise ValueError("unexpected adapter train bundle fields")
    if bundle["schema_version"] != "dragontales.adapter-train-curriculum-bundle.v1":
        raise ValueError("unexpected adapter train bundle schema")
    entries = bundle["entries"]
    if not isinstance(entries, list) or not MIN_ROWS <= len(entries) <= MAX_ROWS:
        raise ValueError(f"adapter train bundle must contain {MIN_ROWS}..={MAX_ROWS} entries")

    rows = []
    bundle_target_kind = None
    bundle_teacher_receipt = None
    for entry in entries:
        if not isinstance(entry, dict) or tuple(entry) != ROW_KEYS:
            raise ValueError("unexpected adapter train row fields")
        for key in (
            "request_sha256",
            "trace_payload_sha256",
            "outcome_payload_sha256",
            "outcome_submission_sha256",
            "target_sha256",
        ):
            _digest(entry[key], key)
        if sha256_bytes(_canonical(entry["request"])) != entry["request_sha256"]:
            raise ValueError("adapter train request digest differs from its canonical bytes")
        if not isinstance(entry["trace_id"], str) or not entry["trace_id"]:
            raise ValueError("adapter train row has no trace ID")

        source = entry["target_source"]
        if not isinstance(source, dict):
            raise ValueError("adapter train target source is invalid")
        kind = source.get("kind")
        if kind == "independent_correction":
            if (
                tuple(source) != ("kind", "verifier_id")
                or not isinstance(source["verifier_id"], str)
                or not 1 <= len(source["verifier_id"]) <= 256
            ):
                raise ValueError("independent correction verifier identity is invalid")
            target_authority = source["verifier_id"]
        elif kind == "teacher_distillation":
            if tuple(source) != (
                "kind",
                "distillation_run_receipt_sha256",
                "teacher_response_sha256",
            ):
                raise ValueError("teacher target source fields are invalid")
            target_authority = _digest(
                source["distillation_run_receipt_sha256"], "distillation receipt SHA-256"
            )
            teacher_response = _digest(
                source["teacher_response_sha256"], "teacher response SHA-256"
            )
            if teacher_response != entry["target_sha256"]:
                raise ValueError("teacher response digest differs from the target")
            if bundle_teacher_receipt not in {None, target_authority}:
                raise ValueError("adapter train bundle mixes distillation receipts")
            bundle_teacher_receipt = target_authority
        else:
            raise ValueError("adapter train target source is invalid")
        if bundle_target_kind not in {None, kind}:
            raise ValueError("adapter train bundle mixes target source kinds")
        bundle_target_kind = kind

        target = entry["target"]
        if (
            not isinstance(target, str)
            or not target.strip()
            or len(target.encode("utf-8")) > MAX_TARGET_BYTES
            or sha256_bytes(target.encode()) != entry["target_sha256"]
        ):
            raise ValueError("adapter train target is invalid")
        messages = _source_messages(entry["request"])
        messages.append({"role": "assistant", "content": target})
        rows.append(
            {
                "schema_version": "dragontales.adapter-train-row.v1",
                "trace_id": entry["trace_id"],
                "request_sha256": entry["request_sha256"],
                "outcome_submission_sha256": entry["outcome_submission_sha256"],
                "target_kind": kind,
                "target_authority": target_authority,
                "target_sha256": entry["target_sha256"],
                "messages": messages,
            }
        )
    return rows


def _toml_string(value: object) -> str:
    return json.dumps(value, ensure_ascii=True)


def _config(bundle_sha256: str, model_manifest_sha256: str, model_profile_sha256: str, builder_sha256: str) -> bytes:
    target_modules = _toml_string(
        ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]
    )
    lines = [
        "max_steps = 100",
        "matmul_precision = \"high\"",
        "dashboard = false",
        "",
        "[deployment]",
        'type = "single_node"',
        "gpus_per_node = 1",
        "num_train_gpus = 1",
        "",
        "[model]",
        'name = "/model"',
        'impl = "hf"',
        'attn = "flash_attention_2"',
        "seq_len = 4096",
        'optimization_dtype = "bfloat16"',
        'reduce_dtype = "bfloat16"',
        "optim_cpu_offload = false",
        'compile = "None"',
        'ac_offloading = "None"',
        "",
        "[tokenizer]",
        'name = "/model"',
        "trust_remote_code = false",
        "",
        "[renderer]",
        'name = "qwen3"',
        "enable_thinking = true",
        "",
        "[model.ac]",
        "freq = 1",
        "",
        "[model.lora]",
        "rank = 32",
        "alpha = 64",
        "dropout = 0.0",
        f"target_modules = {target_modules}",
        "",
        "[data]",
        'type = "sft"',
        'name = "/runs/run/launcher/prepared"',
        "batch_size = 8",
        "micro_batch_size = 1",
        "seq_len = 4096",
        "num_workers = 1",
        "shuffle = true",
        "seed = 0",
        "",
        "[data.loss_mask]",
        "system = false",
        "user = false",
        "assistant = true",
        "tool = false",
        "",
        "[optim]",
        'type = "adamw"',
        "lr = 1e-5",
        "",
        "[ckpt]",
        "",
        "[env_vars]",
        f"DRAGONTALES_TRAIN_BUNDLE_SHA256 = {_toml_string(bundle_sha256)}",
        f"DRAGONTALES_MODEL_MANIFEST_SHA256 = {_toml_string(model_manifest_sha256)}",
        f"DRAGONTALES_MODEL_PROFILE_SHA256 = {_toml_string(model_profile_sha256)}",
        f"DRAGONTALES_BUILDER_SHA256 = {_toml_string(builder_sha256)}",
        'HF_HUB_OFFLINE = "1"',
        'TRANSFORMERS_OFFLINE = "1"',
        'HF_DATASETS_OFFLINE = "1"',
        'HF_HUB_DISABLE_TELEMETRY = "1"',
        'TOKENIZERS_PARALLELISM = "false"',
        'DO_NOT_TRACK = "1"',
        'WANDB_MODE = "disabled"',
        "",
    ]
    return "\n".join(lines).encode()


def prepare(
    bundle: Path,
    bundle_sha256: str,
    run_dir: Path,
    model_manifest_sha256: str,
    model_profile_sha256: str,
    expected_builder_sha256: str,
) -> None:
    for value, name in (
        (bundle_sha256, "train bundle SHA-256"),
        (model_manifest_sha256, "model manifest SHA-256"),
        (model_profile_sha256, "model profile SHA-256"),
        (expected_builder_sha256, "builder SHA-256"),
    ):
        _digest(value, name)
    raw = bundle.read_bytes()
    if len(raw) > MAX_BUNDLE_BYTES or sha256_bytes(raw) != bundle_sha256:
        raise ValueError("adapter train bundle changed before preparation")
    rows = eligible_rows(_load_canonical_line(raw, "adapter train bundle"))

    if not run_dir.is_absolute() or run_dir.exists():
        raise ValueError("adapter train run directory must be a new absolute path")
    launcher = run_dir / "launcher"
    prepared = launcher / "prepared"
    builder_root = launcher / "builder"
    builder = builder_root / "adapter_train"
    for directory in (prepared, builder_root, builder):
        directory.mkdir(mode=0o700, parents=True)

    source = Path(__file__).resolve().parent
    actual_builder_sha256 = package_sha256(source)
    if actual_builder_sha256 != expected_builder_sha256:
        raise ValueError("adapter train definition does not bind this builder package")
    for name in BUILDER_FILES:
        _write(builder / name, (source / name).read_bytes(), 0o444)
    if package_sha256(builder) != actual_builder_sha256:
        raise ValueError("copied adapter train builder digest mismatch")

    dataset_raw = b"".join(
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
        for row in rows
    )
    if len(dataset_raw) > MAX_DATASET_BYTES:
        raise ValueError("prepared adapter train dataset exceeds its fixed byte limit")
    _write(prepared / "train.jsonl", dataset_raw, 0o444)
    _write(
        launcher / "dragontales-sft.toml",
        _config(bundle_sha256, model_manifest_sha256, model_profile_sha256, actual_builder_sha256),
        0o444,
    )
    for directory in (prepared, builder, builder_root):
        directory.chmod(0o500)


def read_prepared(path: Path) -> list[dict]:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError("prepared adapter train dataset is unsafe")
    if stat.S_IMODE(path.stat().st_mode) & 0o222:
        raise ValueError("prepared adapter train dataset must be read-only")
    rows = []
    with path.open("rb") as handle:
        for raw in handle:
            row = _load_canonical_line(raw, "prepared adapter train row")
            if not isinstance(row, dict) or set(row) != PREPARED_KEYS:
                raise ValueError("unexpected prepared adapter train row")
            rows.append(row)
    if not MIN_ROWS <= len(rows) <= MAX_ROWS:
        raise ValueError("prepared adapter train row count is invalid")
    return rows
