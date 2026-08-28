#!/usr/bin/python3

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("h100_fp8_runtime", ROOT / "runtime.py")
runtime = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(runtime)


def write(path: Path, raw: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(raw)
    path.chmod(mode)
    return path


def safetensors(keys) -> bytes:
    header = {
        key: {"dtype": "F8_E4M3", "shape": [1], "data_offsets": [index, index + 1]}
        for index, key in enumerate(keys)
    }
    raw = json.dumps(header, separators=(",", ":")).encode()
    raw += b" " * (-len(raw) % 8)
    return len(raw).to_bytes(8, "little") + raw + b"x" * len(keys)


def valid_config(variant="dynamic"):
    value = {
        "architectures": ["Qwen3ForCausalLM"],
        "model_type": "qwen3",
        **runtime.QWEN3_4B_ARCHITECTURE,
    }
    if variant == "bf16":
        return value
    value["quantization_config"] = {
        "config_groups": {
            "group_0": {
                "input_activations": {
                    "num_bits": 8,
                    "type": "float",
                    "symmetric": True,
                    "group_size": None,
                    "strategy": "token" if variant == "dynamic" else "tensor",
                    "block_structure": None,
                    "dynamic": variant == "dynamic",
                    "actorder": None,
                    "scale_dtype": None,
                    "zp_dtype": None,
                    "observer": None if variant == "dynamic" else "static_minmax",
                    "observer_kwargs": {},
                },
                "output_activations": None,
                "targets": ["Linear"],
                "weights": {
                    "num_bits": 8,
                    "type": "float",
                    "symmetric": True,
                    "group_size": None,
                    "strategy": "channel" if variant == "dynamic" else "tensor",
                    "block_structure": None,
                    "dynamic": False,
                    "actorder": None,
                    "scale_dtype": None,
                    "zp_dtype": None,
                    "observer": "memoryless_minmax",
                    "observer_kwargs": {},
                },
            }
        },
        "format": "float-quantized",
        "ignore": ["lm_head"],
        "kv_cache_scheme": None,
        "quant_method": "compressed-tensors",
        "quantization_status": "compressed",
    }
    return value


class QuantizationArgs:
    def __init__(self, *, dynamic, **_values):
        self.dynamic = dynamic


class RuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def make_model(self, variant="dynamic") -> Path:
        model = self.root / f"model-{variant}"
        model.mkdir()
        write(model / "config.json", json.dumps(valid_config(variant)).encode())
        weight_map = {"x": "model.safetensors"}
        if variant != "bf16":
            weight_map["layer.weight_scale"] = "model.safetensors"
        if variant == "static":
            weight_map["layer.input_scale"] = "model.safetensors"
        write(
            model / "model.safetensors.index.json",
            json.dumps({"metadata": {"total_size": 1}, "weight_map": weight_map}).encode(),
        )
        write(model / "model.safetensors", safetensors(weight_map))
        return model

    def test_only_student_runtime_authority_remains(self):
        self.assertFalse((ROOT / "Dockerfile").exists())
        for name in ("verify_build", "runtime_authority", "serve", "verify_kernel_log", "main"):
            self.assertFalse(hasattr(runtime, name), name)

    def test_image_probe_verifies_exact_installed_artifacts(self):
        metadata = b"vllm metadata"
        wheel = write(self.root / "compressed-tensors.whl", b"compressed tensors wheel")
        versions = {
            "vllm": runtime.VLLM_VERSION,
            "compressed-tensors": runtime.COMPRESSED_TENSORS_VERSION,
        }
        quantization = types.ModuleType("compressed_tensors.quantization")
        quantization.QuantizationArgs = QuantizationArgs
        quantization.QuantizationStrategy = types.SimpleNamespace(CHANNEL="channel", TOKEN="token", TENSOR="tensor")
        quantization.QuantizationType = types.SimpleNamespace(FLOAT="float")
        package = types.ModuleType("compressed_tensors")
        package.__path__ = []
        with (
            mock.patch.object(runtime.platform, "system", return_value="Linux"),
            mock.patch.object(runtime.platform, "machine", return_value="x86_64"),
            mock.patch.object(runtime.importlib.metadata, "version", side_effect=versions.__getitem__),
            mock.patch.object(runtime, "_distribution_metadata", return_value=metadata),
            mock.patch.object(runtime, "VLLM_METADATA_SHA256", hashlib.sha256(metadata).hexdigest()),
            mock.patch.object(runtime, "COMPRESSED_TENSORS_WHEEL_SHA256", hashlib.sha256(wheel.read_bytes()).hexdigest()),
            mock.patch.object(runtime, "WHEEL", wheel),
            mock.patch.dict(
                sys.modules,
                {
                    "compressed_tensors": package,
                    "compressed_tensors.quantization": quantization,
                },
            ),
        ):
            self.assertEqual(
                runtime.image_probe(),
                {
                    "schema_version": "dragontales.h100-fp8-image-probe.v2",
                    "platform": "linux/amd64",
                    "vllm_version": runtime.VLLM_VERSION,
                    "vllm_metadata_sha256": hashlib.sha256(metadata).hexdigest(),
                    "compressed_tensors_version": runtime.COMPRESSED_TENSORS_VERSION,
                    "compressed_tensors_wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                    "fp8_dynamic_supported": True,
                    "fp8_static_supported": True,
                },
            )

        with mock.patch.object(runtime.platform, "system", return_value="Darwin"):
            with self.assertRaisesRegex(ValueError, "linux/amd64"):
                runtime.image_probe()
        with (
            mock.patch.object(runtime.platform, "system", return_value="Linux"),
            mock.patch.object(runtime.platform, "machine", return_value="x86_64"),
            mock.patch.object(runtime.importlib.metadata, "version", return_value="wrong"),
        ):
            with self.assertRaisesRegex(ValueError, "vLLM version"):
                runtime.image_probe()

    def test_hardware_verifier_requires_one_80_gib_h100(self):
        properties = types.SimpleNamespace(name="NVIDIA H100 80GB HBM3", total_memory=80 * 1024**3)
        cuda = types.SimpleNamespace(
            device_count=lambda: 1,
            get_device_properties=lambda _index: properties,
            get_device_capability=lambda _index: (9, 0),
        )
        torch = types.SimpleNamespace(version=types.SimpleNamespace(cuda="12.9"), cuda=cuda)
        with mock.patch.dict(sys.modules, {"torch": torch}):
            runtime.verify_hardware()
            cuda.device_count = lambda: 2
            with self.assertRaisesRegex(ValueError, "exactly one"):
                runtime.verify_hardware()
            cuda.device_count = lambda: 1
            cuda.get_device_capability = lambda _index: (8, 9)
            with self.assertRaisesRegex(ValueError, "H100"):
                runtime.verify_hardware()

    def test_model_verifier_accepts_bf16_dynamic_and_static(self):
        for variant in ("bf16", "dynamic", "static"):
            with self.subTest(variant=variant):
                self.assertEqual(
                    runtime.verify_model(self.make_model(variant)),
                    {
                        "schema_version": "dragontales.h100-model-verification.v2",
                        "model_type": "qwen3",
                        "variant": variant,
                        "shards": 1,
                    },
                )

    def test_model_verifier_rejects_wrong_model_and_quantization(self):
        model = self.make_model()
        changes = (
            lambda value: value.update({"model_type": "llama"}),
            lambda value: value.update({"hidden_size": 4096}),
            lambda value: value.update({"auto_map": {"AutoModel": "remote.Code"}}),
            lambda value: value["quantization_config"].update({"quant_method": "bitsandbytes"}),
            lambda value: value["quantization_config"].update({"quantization_status": "initialized"}),
            lambda value: value["quantization_config"].update({"format": "dense"}),
            lambda value: value["quantization_config"].update({"ignore": []}),
            lambda value: value["quantization_config"].update({"kv_cache_scheme": {"num_bits": 8}}),
            lambda value: value["quantization_config"]["config_groups"]["group_0"]["input_activations"].update({"dynamic": False}),
            lambda value: value["quantization_config"]["config_groups"]["group_0"]["weights"].update({"strategy": "tensor"}),
            lambda value: value["quantization_config"]["config_groups"]["group_0"]["weights"].update({"observer_kwargs": {"unexpected": True}}),
        )
        for change in changes:
            with self.subTest(change=change):
                value = valid_config("dynamic")
                change(value)
                write(model / "config.json", json.dumps(value).encode())
                with self.assertRaises(ValueError):
                    runtime.verify_model(model)

    def test_model_verifier_requires_exact_input_scales(self):
        model = self.make_model("static")
        index = json.loads((model / "model.safetensors.index.json").read_bytes())
        del index["weight_map"]["layer.input_scale"]
        write(model / "model.safetensors.index.json", json.dumps(index).encode())
        write(model / "model.safetensors", safetensors(index["weight_map"]))
        with self.assertRaisesRegex(ValueError, "input_scale"):
            runtime.verify_model(model)

        dynamic = self.make_model("dynamic")
        index = json.loads((dynamic / "model.safetensors.index.json").read_bytes())
        index["weight_map"]["layer.input_scale"] = "model.safetensors"
        write(dynamic / "model.safetensors.index.json", json.dumps(index).encode())
        write(dynamic / "model.safetensors", safetensors(index["weight_map"]))
        with self.assertRaisesRegex(ValueError, "input_scale"):
            runtime.verify_model(dynamic)

    def test_single_safetensors_model_does_not_require_an_index(self):
        model = self.make_model("static")
        (model / "model.safetensors.index.json").unlink()
        self.assertEqual(runtime.verify_model(model)["variant"], "static")

    def test_model_verifier_rejects_adapter_path_traversal_and_unindexed_shards(self):
        model = self.make_model()
        write(model / "adapter_config.json", b"{}")
        with self.assertRaisesRegex(ValueError, "LoRA"):
            runtime.verify_model(model)
        (model / "adapter_config.json").unlink()

        write(
            model / "model.safetensors.index.json",
            json.dumps({"metadata": {"total_size": 1}, "weight_map": {"x": "../model.safetensors"}}).encode(),
        )
        with self.assertRaisesRegex(ValueError, "unsafe shard"):
            runtime.verify_model(model)
        write(
            model / "model.safetensors.index.json",
            json.dumps({"metadata": {"total_size": 1}, "weight_map": {"x": "model.safetensors"}}).encode(),
        )
        write(model / "unexpected.safetensors", b"x")
        with self.assertRaisesRegex(ValueError, "differ"):
            runtime.verify_model(model)

    def test_serving_command_is_fixed_and_private(self):
        command = runtime.serving_argv()
        self.assertEqual(
            command,
            [
                "/usr/local/bin/vllm",
                "serve",
                "/model",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
                "--served-model-name",
                "dragontales-qwen3-4b",
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
            ],
        )

    def test_api_key_is_sealed_and_not_put_in_argv(self):
        key = write(self.root / "api-key", b"a" * 32 + b"\n", 0o400)
        self.assertEqual(runtime._api_key(key), "a" * 32)
        key = write(self.root / "short-key", b"short\n", 0o400)
        with self.assertRaisesRegex(ValueError, "API key"):
            runtime._api_key(key)
        key = write(self.root / "mutable-key", b"a" * 32 + b"\n", 0o600)
        with self.assertRaisesRegex(ValueError, "unsafe"):
            runtime._api_key(key)


if __name__ == "__main__":
    unittest.main()
