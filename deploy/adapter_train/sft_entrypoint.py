from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path


HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_STEPS = 100
MODEL_PATH = "/model"
DATASET_PATH = "/runs/run/launcher/prepared"
ADAPTER_PATH = Path("/runs/run/adapter")
CHAT_TEMPLATE_SHA256 = "071ab92ad2e5b0f8091800ecb4a80c05f98f069669e3dff0389012547414e85b"
CHAT_PARITY_TOKEN_COUNT = 20
CHAT_PARITY_TOKEN_SHA256 = "ba3c0b14ac4a26790e7facb63b7ccf2b582d48b0dc159b55a0b9eb0c696c8b64"
TARGET_MODULES = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]


def _exact_config(config) -> None:
    lora = config.model.lora
    loss_mask = config.data.loss_mask
    if (
        config.max_steps != MAX_STEPS
        or config.dashboard
        or config.deployment.type != "single_node"
        or config.deployment.gpus_per_node != 1
        or config.deployment.num_train_gpus != 1
        or config.model.name != MODEL_PATH
        or config.model.impl != "hf"
        or config.model.attn != "flash_attention_2"
        or config.model.seq_len != 4096
        or config.model.optimization_dtype != "bfloat16"
        or config.model.reduce_dtype != "bfloat16"
        or config.model.optim_cpu_offload
        or config.model.compile is not None
        or config.model.ac is None
        or config.model.ac.freq != 1
        or config.model.ac_offloading is not None
        or config.tokenizer.name != MODEL_PATH
        or config.tokenizer.trust_remote_code is not False
        or config.renderer.name != "qwen3"
        or config.renderer.enable_thinking is not True
        or lora is None
        or lora.rank != 32
        or lora.alpha != 64
        or lora.dropout != 0
        or lora.target_modules != TARGET_MODULES
        or config.data.type != "sft"
        or config.data.name != DATASET_PATH
        or config.data.batch_size != 8
        or config.data.micro_batch_size != 1
        or config.data.seq_len != 4096
        or config.data.num_workers != 1
        or not config.data.shuffle
        or config.data.seed != 0
        or loss_mask.system
        or loss_mask.user
        or not loss_mask.assistant
        or loss_mask.tool
        or config.optim.type != "adamw"
        or config.optim.lr != 1e-5
        or config.ckpt is None
        or config.ckpt.interval is not None
        or config.val is not None
        or config.eval is not None
        or config.inference is not None
        or config.weight_broadcast is not None
    ):
        raise ValueError("resolved SFT config differs from the fixed one-H100 recipe")
    for name in (
        "DRAGONTALES_TRAIN_BUNDLE_SHA256",
        "DRAGONTALES_MODEL_MANIFEST_SHA256",
        "DRAGONTALES_MODEL_PROFILE_SHA256",
        "DRAGONTALES_BUILDER_SHA256",
    ):
        if HEX64.fullmatch(config.env_vars.get(name, "")) is None:
            raise ValueError(f"resolved SFT config has no valid {name} binding")
    expected_env = {
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "HF_DATASETS_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "DO_NOT_TRACK": "1",
        "WANDB_MODE": "disabled",
    }
    if any(config.env_vars.get(name) != value for name, value in expected_env.items()):
        raise ValueError("resolved SFT config does not enforce offline execution")


def preflight(config_path: Path) -> None:
    from datasets import load_dataset
    from prime_rl.configs.sft import SFTConfig
    from prime_rl.utils.config import cli
    from renderers.base import build_training_sample, create_renderer
    from transformers import AutoTokenizer

    config = cli(
        SFTConfig,
        args=["@", str(config_path), "--output-dir", "/runs", "--run.name", "run"],
    )
    _exact_config(config)
    dataset = load_dataset(config.data.name, split="train")
    if not 50 <= len(dataset) <= 10_000:
        raise ValueError("local SFT dataset row count is invalid")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=False)
    if not tokenizer.chat_template:
        raise ValueError("fixed student tokenizer has no chat template")
    renderer = create_renderer(tokenizer, config.renderer)
    for row in dataset:
        messages = row.get("messages")
        if (
            not isinstance(messages, list)
            or len(messages) < 2
            or messages[-1].get("role") != "assistant"
            or not isinstance(messages[-1].get("content"), str)
            or not messages[-1]["content"].strip()
            or not any(message.get("role") == "user" for message in messages[:-1])
        ):
            raise ValueError("local SFT dataset has an invalid conversation")
        if hashlib.sha256(messages[-1]["content"].encode()).hexdigest() != row.get("target_sha256"):
            raise ValueError("local SFT target digest mismatch")
        sample = build_training_sample(renderer, messages, ensure_final_stop=True)
        target_ids = sample.token_ids[1:]
        if (
            not 2 <= len(sample.token_ids) <= 4097
            or not any(sample.loss_mask[1:4097])
            or not set(renderer.get_stop_token_ids()).intersection(target_ids)
        ):
            raise ValueError("local SFT conversation exceeds the fixed 4096-token training window")
    print(json.dumps({"schema_version": "dragontales.adapter-train-preflight.v1", "rows": len(dataset)}))


def chat_template_parity(model_path: Path, template_path: Path) -> None:
    from renderers.base import create_renderer
    from renderers.configs import Qwen3RendererConfig
    from transformers import AutoTokenizer

    template_raw = template_path.read_bytes()
    template_sha256 = hashlib.sha256(template_raw).hexdigest()
    if template_sha256 != CHAT_TEMPLATE_SHA256:
        raise ValueError("student chat template differs from the pinned build authority")
    template = template_raw.decode("utf-8")
    tokenizer = AutoTokenizer.from_pretrained(
        str(model_path), local_files_only=True, trust_remote_code=False
    )
    raw = [
        {"role": "developer", "content": "Preserve this instruction exactly."},
        {"role": "user", "content": "Question"},
    ]
    normalized = [{"role": "system", "content": raw[0]["content"]}, raw[1]]
    rendered = tokenizer.apply_chat_template(
        raw,
        tokenize=False,
        add_generation_prompt=True,
        chat_template=template,
        enable_thinking=True,
    )
    serving_ids = tokenizer.encode(rendered, add_special_tokens=False)
    training_ids = create_renderer(
        tokenizer, Qwen3RendererConfig(enable_thinking=True)
    ).render_ids(normalized, add_generation_prompt=True)
    if serving_ids != training_ids:
        raise ValueError("student serving and training chat templates produce different token IDs")
    token_raw = json.dumps(serving_ids, separators=(",", ":")).encode()
    token_sha256 = hashlib.sha256(token_raw).hexdigest()
    if len(serving_ids) != CHAT_PARITY_TOKEN_COUNT or token_sha256 != CHAT_PARITY_TOKEN_SHA256:
        raise ValueError("student chat-template parity receipt differs from the exact image proof")
    print(
        json.dumps(
            {
                "template_sha256": template_sha256,
                "token_count": len(serving_ids),
                "token_sha256": token_sha256,
            },
            separators=(",", ":"),
        )
    )


def train() -> None:
    from prime_rl.configs.sft import SFTConfig
    from prime_rl.configs.trainer import FileSystemWeightBroadcastConfig
    from prime_rl.trainer.ckpt import CheckpointManager
    from prime_rl.trainer.sft.train import train as train_sft
    from prime_rl.transports.weights.filesystem import FileSystemWeightSender
    from prime_rl.utils.config import cli

    config = cli(SFTConfig)
    _exact_config(config)
    if config.run_dir != Path("/runs/run") or ADAPTER_PATH.exists():
        raise ValueError("adapter output path is not a new fixed run path")

    exported = False
    original_save = CheckpointManager.save

    def export_adapter(self, step, model, optimizers, scheduler, progress, dataloader=None):
        nonlocal exported
        if exported or step != MAX_STEPS:
            raise ValueError("SFT attempted an unexpected checkpoint")
        sender = FileSystemWeightSender(
            config.run_dir,
            FileSystemWeightBroadcastConfig(),
            config.model.lora,
        )
        sender._broadcast(model, step, ADAPTER_PATH)
        if sender.world.is_master:
            names = sorted(item.name for item in ADAPTER_PATH.iterdir())
            if names != ["adapter_config.json", "adapter_model.safetensors"]:
                raise ValueError("SFT export did not produce the exact PEFT artifact")
            for name in names:
                path = ADAPTER_PATH / name
                descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                try:
                    metadata = os.fstat(descriptor)
                    if metadata.st_size <= 0 or metadata.st_nlink != 1:
                        raise ValueError("SFT export produced an unsafe PEFT file")
                    os.fsync(descriptor)
                    os.fchmod(descriptor, 0o400)
                finally:
                    os.close(descriptor)
            descriptor = os.open(ADAPTER_PATH / ".finished", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            ADAPTER_PATH.chmod(0o500)
            directory = os.open(ADAPTER_PATH, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        exported = True

    CheckpointManager.save = export_adapter
    try:
        train_sft(config)
    finally:
        CheckpointManager.save = original_save
    if not exported or not (ADAPTER_PATH / ".finished").is_file():
        raise ValueError("SFT finished without a committed adapter")


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] in ("parity", "preflight"):
        parser = argparse.ArgumentParser()
        parser.add_argument("command", choices=("parity", "preflight"))
        parser.add_argument("--config", type=Path)
        parser.add_argument("--model", type=Path)
        parser.add_argument("--template", type=Path)
        args = parser.parse_args()
        if args.command == "preflight":
            if args.config is None or args.model is not None or args.template is not None:
                parser.error("preflight requires only --config")
            preflight(args.config)
        else:
            if args.config is not None or args.model is None or args.template is None:
                parser.error("parity requires only --model and --template")
            chat_template_parity(args.model, args.template)
    else:
        train()


if __name__ == "__main__":
    main()
