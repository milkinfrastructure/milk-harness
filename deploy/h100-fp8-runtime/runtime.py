#!/usr/bin/python3
"""Verify the fixed Dragontales Qwen3-4B H100 student runtime."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import stat
from pathlib import Path


VLLM_VERSION = "0.28.0+cu129"
VLLM_METADATA_SHA256 = "bed53f35943472068beef63476639f29cac203ef29b9037ee9d22a4bf3a6cebc"
COMPRESSED_TENSORS_VERSION = "0.18.0"
COMPRESSED_TENSORS_WHEEL_SHA256 = "91168237c2d815614c44dfc354c61c613085c811dabbb2ccdb929e9aaa313b8f"
PLATFORM = "linux/amd64"
MODEL_PATH = Path("/model")
MODEL_ALIAS = "dragontales-qwen3-4b"
VLLM = Path("/usr/local/bin/vllm")
WHEEL = Path("/opt/compressed_tensors-0.18.0-py3-none-any.whl")
KERNEL_SELECTION = "Selected CutlassFP8ScaledMMLinearKernel for CompressedTensorsW8A8Fp8"
QWEN3_4B_ARCHITECTURE = {
    "head_dim": 128,
    "hidden_size": 2560,
    "intermediate_size": 9728,
    "max_position_embeddings": 262144,
    "num_attention_heads": 32,
    "num_hidden_layers": 36,
    "num_key_value_heads": 8,
    "vocab_size": 151936,
}
MAX_JSON_BYTES = 1024 * 1024
MAX_LOG_BYTES = 8 * 1024 * 1024
KEY = re.compile(rb"[A-Za-z0-9._-]{32,256}\n?\Z")


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError(f"non-finite JSON value is forbidden: {value}")


def _read_regular(path: Path, maximum: int, mode: int | None = None) -> bytes:
    if not path.is_absolute():
        raise ValueError("input path must be absolute")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    before = os.fstat(descriptor)
    try:
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > maximum
            or mode is not None
            and (before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != mode)
        ):
            raise ValueError(f"unsafe, empty, or oversized file: {path}")
        raw = bytearray()
        while chunk := os.read(descriptor, min(1024 * 1024, maximum + 1 - len(raw))):
            raw.extend(chunk)
            if len(raw) > maximum:
                raise ValueError(f"oversized file: {path}")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
    if len(raw) != before.st_size or any(getattr(before, key) != getattr(after, key) for key in stable):
        raise ValueError(f"file changed while reading: {path}")
    return bytes(raw)


def _json(raw: bytes):
    try:
        return json.loads(raw, object_pairs_hook=_object, parse_constant=_invalid_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("input is not strict JSON") from error


def _distribution_metadata(name: str) -> bytes:
    distribution = importlib.metadata.distribution(name)
    entries = [
        item
        for item in distribution.files or ()
        if item.name == "METADATA" and item.parent.name.endswith(".dist-info")
    ]
    if len(entries) != 1:
        raise ValueError(f"{name} has no unique installed METADATA")
    return Path(distribution.locate_file(entries[0])).read_bytes()


def image_probe() -> dict:
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise ValueError("runtime must be linux/amd64")
    if importlib.metadata.version("vllm") != VLLM_VERSION:
        raise ValueError("vLLM version differs from the pinned image")
    if importlib.metadata.version("compressed-tensors") != COMPRESSED_TENSORS_VERSION:
        raise ValueError("compressed-tensors version differs from the pinned wheel")
    metadata_sha256 = hashlib.sha256(_distribution_metadata("vllm")).hexdigest()
    if metadata_sha256 != VLLM_METADATA_SHA256:
        raise ValueError("vLLM dependency metadata is not the exact audited patch")
    wheel_sha256 = hashlib.sha256(_read_regular(WHEEL, 256 * 1024 * 1024)).hexdigest()
    if wheel_sha256 != COMPRESSED_TENSORS_WHEEL_SHA256:
        raise ValueError("compressed-tensors wheel differs from the audited artifact")
    from compressed_tensors.quantization import (  # pylint: disable=import-outside-toplevel
        QuantizationArgs,
        QuantizationStrategy,
        QuantizationType,
    )

    weight = QuantizationArgs(
        num_bits=8,
        type=QuantizationType.FLOAT,
        strategy=QuantizationStrategy.CHANNEL,
        symmetric=True,
        dynamic=False,
    )
    activation = QuantizationArgs(
        num_bits=8,
        type=QuantizationType.FLOAT,
        strategy=QuantizationStrategy.TOKEN,
        symmetric=True,
        dynamic=True,
    )
    static_weight = QuantizationArgs(
        num_bits=8,
        type=QuantizationType.FLOAT,
        strategy=QuantizationStrategy.TENSOR,
        symmetric=True,
        dynamic=False,
    )
    static_activation = QuantizationArgs(
        num_bits=8,
        type=QuantizationType.FLOAT,
        strategy=QuantizationStrategy.TENSOR,
        symmetric=True,
        dynamic=False,
    )
    if weight.dynamic or not activation.dynamic or static_weight.dynamic or static_activation.dynamic:
        raise ValueError("installed compressed-tensors cannot represent both H100 FP8 variants")
    return {
        "schema_version": "dragontales.h100-fp8-image-probe.v2",
        "platform": PLATFORM,
        "vllm_version": VLLM_VERSION,
        "vllm_metadata_sha256": metadata_sha256,
        "compressed_tensors_version": COMPRESSED_TENSORS_VERSION,
        "compressed_tensors_wheel_sha256": wheel_sha256,
        "fp8_dynamic_supported": True,
        "fp8_static_supported": True,
    }


def _strict_json_file(path: Path, maximum: int):
    return _json(_read_regular(path, maximum))


def _quant_args(value, *, dynamic: bool, strategy: str, observer: str | None) -> bool:
    return isinstance(value, dict) and value == {
        "num_bits": 8,
        "type": "float",
        "symmetric": True,
        "group_size": None,
        "strategy": strategy,
        "block_structure": None,
        "dynamic": dynamic,
        "actorder": None,
        "scale_dtype": None,
        "zp_dtype": None,
        "observer": observer,
        "observer_kwargs": {},
    }


def _safetensor_keys(model: Path, names: set[str]) -> set[str]:
    keys = set()
    for name in sorted(names):
        descriptor = os.open(model / name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            before = os.fstat(descriptor)
            prefix = os.read(descriptor, 8)
            header_bytes = int.from_bytes(prefix, "little") if len(prefix) == 8 else 0
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or not 2 <= header_bytes <= 16 * 1024 * 1024
                or header_bytes > before.st_size - 8
            ):
                raise ValueError("model has an invalid safetensors header")
            raw = bytearray()
            while len(raw) < header_bytes:
                chunk = os.read(descriptor, header_bytes - len(raw))
                if not chunk:
                    raise ValueError("model has a truncated safetensors header")
                raw.extend(chunk)
            header = _json(bytes(raw))
            if not isinstance(header, dict):
                raise ValueError("model safetensors header is not an object")
            data_bytes = before.st_size - 8 - header_bytes
            for key, tensor in header.items():
                if key == "__metadata__":
                    continue
                offsets = tensor.get("data_offsets") if isinstance(tensor, dict) else None
                if (
                    not isinstance(key, str)
                    or not key
                    or key in keys
                    or not isinstance(tensor, dict)
                    or not isinstance(tensor.get("dtype"), str)
                    or not isinstance(tensor.get("shape"), list)
                    or any(type(dimension) is not int or dimension < 0 for dimension in tensor.get("shape", ()))
                    or not isinstance(offsets, list)
                    or len(offsets) != 2
                    or any(type(item) is not int or item < 0 for item in offsets)
                    or offsets[0] >= offsets[1]
                    or offsets[1] > data_bytes
                ):
                    raise ValueError("model has invalid safetensors metadata")
                keys.add(key)
            after = os.fstat(descriptor)
            stable = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
            if any(getattr(before, field) != getattr(after, field) for field in stable):
                raise ValueError("model safetensors changed while reading")
        finally:
            os.close(descriptor)
    if not keys:
        raise ValueError("model safetensors contain no tensors")
    return keys


def verify_model(model: Path = MODEL_PATH) -> dict:
    if not model.is_absolute() or not model.is_dir() or model.is_symlink():
        raise ValueError("model must be an absolute non-symlink directory")
    config = _strict_json_file(model / "config.json", MAX_JSON_BYTES)
    if (
        not isinstance(config, dict)
        or config.get("model_type") != "qwen3"
        or config.get("architectures") != ["Qwen3ForCausalLM"]
        or config.get("auto_map") not in (None, {})
        or any(config.get(key) != value for key, value in QWEN3_4B_ARCHITECTURE.items())
    ):
        raise ValueError("model is not the fixed native Qwen3-4B causal LM")
    quantization = config.get("quantization_config")
    if quantization is None:
        variant = "bf16"
    else:
        groups = quantization.get("config_groups") if isinstance(quantization, dict) else None
        group = groups.get("group_0") if isinstance(groups, dict) and tuple(groups) == ("group_0",) else None
        if (
            not isinstance(group, dict)
            or quantization.get("quant_method") != "compressed-tensors"
            or quantization.get("quantization_status") != "compressed"
            or quantization.get("format") != "float-quantized"
            or quantization.get("ignore") != ["lm_head"]
            or quantization.get("kv_cache_scheme") is not None
            or group.get("targets") != ["Linear"]
            or "output_activations" not in group
            or group.get("output_activations") is not None
        ):
            raise ValueError("model is not an exact compressed-tensors H100 FP8 artifact")
        if _quant_args(
            group.get("weights"),
            dynamic=False,
            strategy="channel",
            observer="memoryless_minmax",
        ) and _quant_args(
            group.get("input_activations"),
            dynamic=True,
            strategy="token",
            observer=None,
        ):
            variant = "dynamic"
        elif _quant_args(
            group.get("weights"),
            dynamic=False,
            strategy="tensor",
            observer="memoryless_minmax",
        ) and _quant_args(
            group.get("input_activations"),
            dynamic=False,
            strategy="tensor",
            observer="static_minmax",
        ):
            variant = "static"
        else:
            raise ValueError("compressed-tensors group is not an authorized H100 FP8 variant")
    actual = set()
    artifact_bytes = 0
    for entry in model.iterdir():
        if entry.name.startswith("adapter_"):
            raise ValueError("LoRA adapter files are forbidden in the merged FP8 runtime")
        if entry.name.endswith(".safetensors"):
            if entry.is_symlink() or not entry.is_file():
                raise ValueError("model shard is not a regular file")
            actual.add(entry.name)
            artifact_bytes += entry.stat().st_size
    index_path = model / "model.safetensors.index.json"
    weight_map = None
    if index_path.exists():
        index = _strict_json_file(index_path, 16 * 1024 * 1024)
        weight_map = index.get("weight_map") if isinstance(index, dict) else None
        metadata = index.get("metadata") if isinstance(index, dict) else None
        total_size = metadata.get("total_size") if isinstance(metadata, dict) else None
        if (
            not isinstance(weight_map, dict)
            or not weight_map
            or type(total_size) is not int
            or total_size <= 0
            or total_size > 16 * 1024**3
        ):
            raise ValueError("model safetensors index has no weights")
        names = set()
        for name in weight_map.values():
            if not isinstance(name, str) or Path(name).name != name or not name.endswith(".safetensors"):
                raise ValueError("model safetensors index contains an unsafe shard path")
            names.add(name)
        if actual != names or artifact_bytes < total_size:
            raise ValueError("safetensors files differ from the model index")
    elif actual != {"model.safetensors"}:
        raise ValueError("unindexed model must contain exactly model.safetensors")
    tensor_keys = _safetensor_keys(model, actual)
    if weight_map is not None and set(weight_map) != tensor_keys:
        raise ValueError("safetensors tensor keys differ from the model index")
    input_scales = {key.removesuffix(".input_scale") for key in tensor_keys if key.endswith(".input_scale")}
    weight_scales = {key.removesuffix(".weight_scale") for key in tensor_keys if key.endswith(".weight_scale")}
    if (variant == "static" and (not input_scales or input_scales != weight_scales)) or (
        variant != "static" and input_scales
    ) or (variant == "bf16" and weight_scales):
        raise ValueError("model input_scale tensors differ from its declared H100 variant")
    return {
        "schema_version": "dragontales.h100-model-verification.v2",
        "model_type": "qwen3",
        "variant": variant,
        "shards": len(actual),
    }


def verify_hardware() -> None:
    import torch  # pylint: disable=import-outside-toplevel

    if torch.version.cuda is None or not torch.version.cuda.startswith("12.9"):
        raise ValueError("runtime is not the pinned CUDA 12.9 build")
    if torch.cuda.device_count() != 1:
        raise ValueError("exactly one visible GPU is required")
    properties = torch.cuda.get_device_properties(0)
    if (
        torch.cuda.get_device_capability(0) != (9, 0)
        or "H100" not in properties.name
        or properties.total_memory < 79 * 1024**3
    ):
        raise ValueError("the visible GPU is not an 80 GiB-class H100 sm_90")


def _api_key(path: Path) -> str:
    raw = _read_regular(path, 257, 0o400)
    if KEY.fullmatch(raw) is None:
        raise ValueError("API key must be one 32-256 byte safe ASCII line")
    return raw.rstrip(b"\n").decode("ascii")


def serving_argv() -> list[str]:
    return [
        str(VLLM),
        "serve",
        str(MODEL_PATH),
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
        "--served-model-name",
        MODEL_ALIAS,
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
        "--no-enable-log-requests",
        "--disable-log-stats",
    ]
