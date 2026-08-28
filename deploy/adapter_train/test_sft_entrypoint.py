from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sys
import tempfile
from pathlib import Path
from types import ModuleType, SimpleNamespace

import sft_entrypoint


class CheckpointManager:
    def save(self, *args, **kwargs):
        raise AssertionError("the DCP checkpoint path must be replaced by the fixed adapter export")


class FileSystemWeightBroadcastConfig:
    pass


class Qwen3RendererConfig:
    def __init__(self, enable_thinking):
        assert enable_thinking is True


class FileSystemWeightSender:
    def __init__(self, output_dir, config, lora):
        assert output_dir == Path("/runs/run")
        assert lora.rank == 32
        self.world = SimpleNamespace(is_master=True)

    def _broadcast(self, model, step, output):
        assert step == 100 and model == "model"
        output.mkdir()
        (output / "adapter_config.json").write_text('{"base_model_name_or_path":"/model"}')
        (output / "adapter_model.safetensors").write_bytes(b"adapter")


def module(name: str, **values) -> None:
    item = ModuleType(name)
    vars(item).update(values)
    sys.modules[name] = item


def config():
    return SimpleNamespace(
        max_steps=100,
        dashboard=False,
        deployment=SimpleNamespace(type="single_node", gpus_per_node=1, num_train_gpus=1),
        model=SimpleNamespace(
            name="/model",
            impl="hf",
            attn="flash_attention_2",
            seq_len=4096,
            optimization_dtype="bfloat16",
            reduce_dtype="bfloat16",
            optim_cpu_offload=False,
            compile=None,
            ac=SimpleNamespace(freq=1),
            ac_offloading=None,
            lora=SimpleNamespace(
                rank=32,
                alpha=64,
                dropout=0,
                target_modules=list(sft_entrypoint.TARGET_MODULES),
            ),
        ),
        tokenizer=SimpleNamespace(name="/model", trust_remote_code=False),
        renderer=SimpleNamespace(name="qwen3", enable_thinking=True),
        data=SimpleNamespace(
            type="sft",
            name="/runs/run/launcher/prepared",
            batch_size=8,
            micro_batch_size=1,
            seq_len=4096,
            num_workers=1,
            shuffle=True,
            seed=0,
            loss_mask=SimpleNamespace(system=False, user=False, assistant=True, tool=False),
        ),
        optim=SimpleNamespace(type="adamw", lr=1e-5),
        ckpt=SimpleNamespace(interval=None),
        val=None,
        eval=None,
        inference=None,
        weight_broadcast=None,
        env_vars={
            "DRAGONTALES_TRAIN_BUNDLE_SHA256": "1" * 64,
            "DRAGONTALES_MODEL_MANIFEST_SHA256": "2" * 64,
            "DRAGONTALES_MODEL_PROFILE_SHA256": "3" * 64,
            "DRAGONTALES_BUILDER_SHA256": "4" * 64,
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_DATASETS_OFFLINE": "1",
            "HF_HUB_DISABLE_TELEMETRY": "1",
            "TOKENIZERS_PARALLELISM": "false",
            "DO_NOT_TRACK": "1",
            "WANDB_MODE": "disabled",
        },
        run_dir=Path("/runs/run"),
    )


def main() -> None:
    fixed = config()

    class Renderer:
        @staticmethod
        def get_stop_token_ids():
            return [3]

        @staticmethod
        def render_ids(messages, add_generation_prompt):
            assert add_generation_prompt is True
            assert [message["role"] for message in messages] == ["system", "user"]
            return [1, 2, 3]

    renderer = Renderer()

    def load_dataset(name, split):
        assert name == "/runs/run/launcher/prepared" and split == "train"
        target = "answer"
        return [
            {
                "messages": [
                    {"role": "user", "content": "question"},
                    {"role": "assistant", "content": target},
                ],
                "target_sha256": hashlib.sha256(target.encode()).hexdigest(),
            }
        ] * 50

    module("datasets", load_dataset=load_dataset)
    module("renderers")
    module("renderers.configs", Qwen3RendererConfig=Qwen3RendererConfig)
    module(
        "renderers.base",
        create_renderer=lambda tokenizer, renderer_config: renderer,
        build_training_sample=lambda renderer, messages, ensure_final_stop: SimpleNamespace(
            token_ids=[1, 2, 3], loss_mask=[False, True, True]
        ),
    )
    def apply_chat_template(messages, **kwargs):
        assert [message["role"] for message in messages] == ["developer", "user"]
        assert kwargs["tokenize"] is False
        assert kwargs["add_generation_prompt"] is True
        assert kwargs["enable_thinking"] is True
        assert hashlib.sha256(kwargs["chat_template"].encode()).hexdigest() == (
            sft_entrypoint.CHAT_TEMPLATE_SHA256
        )
        return "fixed"

    tokenizer = SimpleNamespace(
        chat_template="fixed",
        apply_chat_template=apply_chat_template,
        encode=lambda text, add_special_tokens: [1, 2, 3]
        if text == "fixed" and add_special_tokens is False
        else None,
    )
    module(
        "transformers",
        AutoTokenizer=SimpleNamespace(from_pretrained=lambda *args, **kwargs: tokenizer),
    )

    def pinned_sft_train(received):
        assert received is fixed
        manager = CheckpointManager()
        manager.save(100, "model", [], None, None)

    module("prime_rl")
    module("prime_rl.configs")
    module("prime_rl.configs.sft", SFTConfig=object)
    module(
        "prime_rl.configs.trainer",
        FileSystemWeightBroadcastConfig=FileSystemWeightBroadcastConfig,
    )
    module("prime_rl.trainer")
    module("prime_rl.trainer.ckpt", CheckpointManager=CheckpointManager)
    module("prime_rl.trainer.sft")
    module("prime_rl.trainer.sft.train", train=pinned_sft_train)
    module("prime_rl.transports")
    module("prime_rl.transports.weights")
    module(
        "prime_rl.transports.weights.filesystem",
        FileSystemWeightSender=FileSystemWeightSender,
    )
    module("prime_rl.utils")
    module("prime_rl.utils.config", cli=lambda _, args=None: fixed)

    with tempfile.TemporaryDirectory() as temporary:
        config_path = Path(temporary) / "config.toml"
        config_path.write_text("fixed")
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            sft_entrypoint.preflight(config_path)
        assert '"rows": 50' in stdout.getvalue()

    stdout = io.StringIO()
    template = Path(__file__).resolve().parents[1] / "student/qwen3-chat-template.jinja"
    parity_count = sft_entrypoint.CHAT_PARITY_TOKEN_COUNT
    parity_sha256 = sft_entrypoint.CHAT_PARITY_TOKEN_SHA256
    assert parity_count == 20
    assert parity_sha256 == (
        "ba3c0b14ac4a26790e7facb63b7ccf2b582d48b0dc159b55a0b9eb0c696c8b64"
    )
    sft_entrypoint.CHAT_PARITY_TOKEN_COUNT = 3
    sft_entrypoint.CHAT_PARITY_TOKEN_SHA256 = hashlib.sha256(b"[1,2,3]").hexdigest()
    try:
        with contextlib.redirect_stdout(stdout):
            sft_entrypoint.chat_template_parity(Path("/model"), template)
    finally:
        sft_entrypoint.CHAT_PARITY_TOKEN_COUNT = parity_count
        sft_entrypoint.CHAT_PARITY_TOKEN_SHA256 = parity_sha256
    receipt = json.loads(stdout.getvalue())
    assert receipt == {
        "template_sha256": sft_entrypoint.CHAT_TEMPLATE_SHA256,
        "token_count": 3,
        "token_sha256": hashlib.sha256(b"[1,2,3]").hexdigest(),
    }

    original_save = CheckpointManager.save
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary) / "adapter"
        previous = sft_entrypoint.ADAPTER_PATH
        sft_entrypoint.ADAPTER_PATH = output
        try:
            sft_entrypoint.train()
        finally:
            sft_entrypoint.ADAPTER_PATH = previous
        assert sorted(item.name for item in output.iterdir()) == [
            ".finished",
            "adapter_config.json",
            "adapter_model.safetensors",
        ]
        assert output.stat().st_mode & 0o777 == 0o500
        assert all(item.stat().st_mode & 0o777 == 0o400 for item in output.iterdir())
        output.chmod(0o700)
    assert CheckpointManager.save is original_save


if __name__ == "__main__":
    main()
