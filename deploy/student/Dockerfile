# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG MILK_GATEWAY_IMAGE
FROM ${MILK_GATEWAY_IMAGE} AS gateway

FROM --platform=linux/amd64 ghcr.io/primeintellect-ai/prime-rl@sha256:6b8b2cf339de90b67ed25f246ad3dcfd773cb80e629577eb31aeb2a6b81db80e AS prime

ADD --chmod=0444 --checksum=sha256:e94390c2863b873be18f623f9df48a0d8fe5eff13ea7f1a00092b0a7904888c6 \
    https://files.pythonhosted.org/packages/a8/db/253133d7e7cb40d3af384bb2f5c0b4a2b7fdcffbc95c688cc67a20a3c103/accelerate-1.14.0-py3-none-any.whl \
    /opt/accelerate-1.14.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:0fbba16ffebfad3de96e06f2da6860fd860292324b85b6141909fa1e26ea9233 \
    https://files.pythonhosted.org/packages/28/79/13bcabb8048126422d5c4b880575d40886c726f354db88cfeed4325525bb/peft-0.20.0-py3-none-any.whl \
    /opt/peft-0.20.0-py3-none-any.whl
RUN /usr/local/bin/uv pip install --offline --no-deps --python /app/.venv/bin/python \
      /opt/accelerate-1.14.0-py3-none-any.whl \
      /opt/peft-0.20.0-py3-none-any.whl && \
    /app/.venv/bin/python - <<'PY'
from importlib.metadata import version
from peft import PeftModel

assert version("accelerate") == "1.14.0"
assert version("peft") == "0.20.0"
assert PeftModel
PY

FROM --platform=linux/amd64 vllm/vllm-openai@sha256:50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d
ARG MILK_GATEWAY_IMAGE
RUN gateway_repository="${MILK_GATEWAY_IMAGE%@sha256:*}" && \
    gateway_digest="${MILK_GATEWAY_IMAGE##*@sha256:}" && \
    test -n "$gateway_repository" && test "$gateway_repository" != "$MILK_GATEWAY_IMAGE" && \
    test "${#gateway_digest}" -eq 64 && \
    case "$gateway_repository" in *@*) exit 1 ;; esac && \
    case "$gateway_digest" in *[!0-9a-f]*) exit 1 ;; esac

ADD --chmod=0444 --checksum=sha256:91168237c2d815614c44dfc354c61c613085c811dabbb2ccdb929e9aaa313b8f \
    https://files.pythonhosted.org/packages/2f/bf/e17189bd834e5a4487c97ed576564a66ab56ccd7840608f49dcecf61abfc/compressed_tensors-0.18.0-py3-none-any.whl \
    /opt/compressed_tensors-0.18.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:c703127445219309bcc77b8fea8b928e81fa70a8a6be9c98eef23b51165fff0c \
    https://files.pythonhosted.org/packages/e8/78/9be717e36007a5edac07ce6a662c1a573e024933090191bc2f88c5818c4c/llmcompressor-0.13.0-py3-none-any.whl \
    /opt/llmcompressor-0.13.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:4265c11d33a6f688c211bfed87e4698b685473efe1d6faf95d744a3f7c19ca24 \
    https://files.pythonhosted.org/packages/b1/f6/81cddb633e62bb14aa0912443620f43104bda8458505e7c943903d63a134/auto_round-0.14.2-py3-none-any.whl \
    /opt/auto_round-0.14.2-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:9fbf73688f8c18f7529b4fe592abd04015f81d1e58001e4bac73ffb2b39d7cc4 \
    https://files.pythonhosted.org/packages/44/0b/98fc6eb83333508ca5f44c52b3e287ea8137a0ad582714e2cbc67a02154b/datasets-5.0.1-py3-none-any.whl \
    /opt/datasets-5.0.1-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:ecd0a012051abc20c1aa87328841efa8cade3ced65803046e38c2f03c0891fea \
    https://files.pythonhosted.org/packages/31/1e/9e366f36efc550f07d6737f199e3f6bffafdf28795d007f10a77dd274f5c/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl \
    /opt/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl
ADD --chmod=0444 --checksum=sha256:9db974c4079ede2d1a3ea7ca5a240df33f2cc26fc2b36ba64c5f2a4f43b6e725 \
    https://files.pythonhosted.org/packages/6f/67/8d85ca2323233ae3c0365a659c4e52ee1f587b440e4bc577e7d8e4416d0f/transformers-5.14.1-py3-none-any.whl \
    /opt/transformers-5.14.1-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:02e0b71817df9b2169dc30a16832045764def1191b43dcff5bb85bdee212d2a1 \
    https://files.pythonhosted.org/packages/e5/22/4222d7ddf3da30f363edaa98e329c2bce6c65497c9cb2810931c8b2c0fbc/fsspec-2026.6.0-py3-none-any.whl \
    /opt/fsspec-2026.6.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:3a56c0e85dd5025161bac5ce138dcac1e49174c7d8e74596537e729fd5c53c28 \
    https://files.pythonhosted.org/packages/71/70/38998b950a97ea279e6bd657575d22d1a2047256caf707d9a10fbce4f065/multiprocess-0.70.19-py312-none-any.whl \
    /opt/multiprocess-0.70.19-py312-none-any.whl
ADD --chmod=0444 --checksum=sha256:5389cdf79447ed1515c9e31620e6e1e2302249564d603f2ad727d4f6d313e4c3 \
    https://files.pythonhosted.org/packages/60/c9/711ca85d79f1ec98f29a5eae2b051e25b4ecec5de3e3c0e2d5c5dcb15664/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl \
    /opt/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl
ADD --chmod=0444 --checksum=sha256:d373ce03ffd84010ed9839fa73672a9c8256990532e158440c0085db7d914b34 \
    https://files.pythonhosted.org/packages/12/98/e976540c1addf70442be7842a18cf70884a964abbf69442504f4d2939989/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl \
    /opt/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl
ADD --chmod=0444 --checksum=sha256:237b8f63a2a0fcfb1ffc06e21dad23add44e6d354b2b014364a1d41e419a4dee \
    https://files.pythonhosted.org/packages/83/97/31bd8b8279e6935a0719f6910ced15e9d5a2cd554b253f6027ce1b5a1c2c/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl \
    /opt/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl

RUN python3 -m pip install --no-deps --force-reinstall \
      /opt/compressed_tensors-0.18.0-py3-none-any.whl && \
    python3 - <<'PY'
import base64
import csv
import hashlib
from importlib.metadata import distribution, version
from pathlib import Path

assert version("vllm") == "0.28.0+cu129"
assert version("compressed-tensors") == "0.18.0"
dist = distribution("vllm")
entries = [item for item in dist.files or [] if item.name == "METADATA" and item.parent.name.endswith(".dist-info")]
assert len(entries) == 1
metadata = Path(dist.locate_file(entries[0]))
raw = metadata.read_bytes()
old = b"Requires-Dist: compressed-tensors==0.17.0\n"
new = b"Requires-Dist: compressed-tensors==0.18.0\n"
assert raw.count(old) == 1
patched = raw.replace(old, new)
assert hashlib.sha256(patched).hexdigest() == "bed53f35943472068beef63476639f29cac203ef29b9037ee9d22a4bf3a6cebc"
metadata.write_bytes(patched)
site = Path(dist.locate_file(""))
record = metadata.with_name("RECORD")
with record.open(newline="", encoding="utf-8") as handle:
    rows = list(csv.reader(handle))
relative = metadata.relative_to(site).as_posix()
digest = base64.urlsafe_b64encode(hashlib.sha256(patched).digest()).rstrip(b"=").decode()
for row in rows:
    if row[0] == relative:
        row[1:] = [f"sha256={digest}", str(len(patched))]
        break
else:
    raise AssertionError("vLLM RECORD has no METADATA row")
with record.open("w", newline="", encoding="utf-8") as handle:
    csv.writer(handle, lineterminator="\n").writerows(rows)
PY

RUN python3 -m venv --system-site-packages /opt/quant && \
    /opt/quant/bin/pip install --no-index --no-deps --force-reinstall \
      /opt/llmcompressor-0.13.0-py3-none-any.whl \
      /opt/auto_round-0.14.2-py3-none-any.whl \
      /opt/datasets-5.0.1-py3-none-any.whl \
      /opt/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl \
      /opt/transformers-5.14.1-py3-none-any.whl \
      /opt/fsspec-2026.6.0-py3-none-any.whl \
      /opt/multiprocess-0.70.19-py312-none-any.whl \
      /opt/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl \
      /opt/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl \
      /opt/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl && \
    /opt/quant/bin/pip check && \
    /opt/quant/bin/python - <<'PY'
from importlib.metadata import version
from compressed_tensors.quantization import QuantizationConfig
from datasets import Dataset
from llmcompressor import oneshot
from llmcompressor.modifiers.quantization import QuantizationModifier
from transformers import AutoTokenizer
import torch

assert version("llmcompressor") == "0.13.0"
assert version("auto-round") == "0.14.2"
assert version("datasets") == "5.0.1"
assert version("compressed-tensors") == "0.18.0"
assert version("transformers") == "5.14.1"
assert version("nvidia-nccl-cu12") == "2.29.7"
assert version("torch") == "2.13.0+cu129"
assert torch.version.cuda == "12.9"
assert torch.__file__.startswith("/usr/local/lib/python3.12/dist-packages/torch/")
assert all((oneshot, QuantizationModifier, Dataset, AutoTokenizer, QuantizationConfig))
PY

COPY --from=prime /app /app
RUN test -x /usr/bin/python3.12 && \
    ln -s /usr/bin/python3.12 /usr/local/bin/python && \
    /app/.venv/bin/python - <<'PY'
import sys
import sysconfig
from importlib.metadata import version
from peft import PeftModel

assert sys.version_info[:3] == (3, 12, 3)
assert sysconfig.get_config_var("SOABI") == "cpython-312-x86_64-linux-gnu"
assert version("prime-rl") == "0.9.0"
assert version("peft") == "0.20.0"
assert PeftModel
PY

ADD --checksum=sha256:34448b82c17d60fec9b65b1f093c115ddbaadc04beb1b0140b6bfed2e012a930 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/.gitattributes \
    /model/.gitattributes
ADD --checksum=sha256:832dd9e00a68dd83b3c3fb9f5588dad7dcf337a0db50f7d9483f310cd292e92e \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/LICENSE \
    /model/LICENSE
ADD --checksum=sha256:8e3dd0c3b5b11897cc71092ccfe517bb7a9783479baa3665aad73c8d1a2041cd \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/README.md \
    /model/README.md
ADD --checksum=sha256:5beea1a4a34c62782bfb2f911c606741a3bab8f92d80a118fa053c28af12e8ba \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/config.json \
    /model/config.json
ADD --checksum=sha256:835fffe355c9438e7a25be099b3fccaa98350b83451f9fd2d99512e74f1ade48 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/generation_config.json \
    /model/generation_config.json
ADD --checksum=sha256:599bab54075088774b1733fde865d5bd747cbcc7a547c5bc12610e874e26f5e3 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/merges.txt \
    /model/merges.txt
ADD --checksum=sha256:75311d91bb08cf0b882913da464a1e722a31fb44db35208663487efb7a3d8ed6 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/model-00001-of-00003.safetensors \
    /model/model-00001-of-00003.safetensors
ADD --checksum=sha256:0b48adbb1f60e901153d91907ba11ce63bd4b8b584482e730f48808d055dfba1 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/model-00002-of-00003.safetensors \
    /model/model-00002-of-00003.safetensors
ADD --checksum=sha256:7dd39ccca5e4de123c74c14af44c9bf2eb75df33b4614382af0134528e060d5d \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/model-00003-of-00003.safetensors \
    /model/model-00003-of-00003.safetensors
ADD --checksum=sha256:d6c42883a895dfef5b0080ed2116a1bcd764f558406b98923d675978a1abf29c \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/model.safetensors.index.json \
    /model/model.safetensors.index.json
ADD --checksum=sha256:aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/tokenizer.json \
    /model/tokenizer.json
ADD --checksum=sha256:a62ff0a2472a0fa1b8eaabcb57c59b58afa42a22831dc141400b6e0cf2b65ce3 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/tokenizer_config.json \
    /model/tokenizer_config.json
ADD --checksum=sha256:ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910 \
    https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507/resolve/cdbee75f17c01a7cc42f958dc650907174af0554/vocab.json \
    /model/vocab.json

WORKDIR /opt/dragontales/deploy
COPY --from=gateway --chmod=0555 /usr/local/bin/dragontales-gateway /usr/local/bin/dragontales-gateway
COPY --from=gateway --chmod=0444 /usr/share/licenses/dragontales/ /usr/share/licenses/dragontales/
COPY --chmod=0444 LICENSE /usr/share/licenses/milk-harness/LICENSE
COPY --chmod=0444 deploy/licenses/prime-rl-v0.9.0.LICENSE /usr/share/licenses/prime-rl/LICENSE
COPY --chmod=0444 deploy/student/provenance.json /usr/share/milk/student-provenance.json
COPY deploy/student-job.sh ./student-job.sh
COPY deploy/student-run.sh ./student-run.sh
COPY deploy/student/Dockerfile deploy/student/runtime.py deploy/student/qwen3-chat-template.jinja ./student/
COPY deploy/adapter_train/__init__.py deploy/adapter_train/contract.py \
     deploy/adapter_train/sft_entrypoint.py ./adapter_train/
COPY deploy/h100-fp8-runtime/runtime.py ./h100-fp8-runtime/runtime.py
COPY deploy/models/qwen3-4b-instruct-2507/profile.json \
     deploy/models/qwen3-4b-instruct-2507/model.manifest.sha256 \
     deploy/models/qwen3-4b-instruct-2507/verify.py \
     ./models/qwen3-4b-instruct-2507/
RUN test -x /usr/bin/setpriv && \
    printf '%s  %s\n' \
      f5118b9c9e98b0f4076214ee13f68d5f73c13b077c44544cb9a0c4ed9155065c \
      /usr/share/licenses/prime-rl/LICENSE \
      6371f282f372c5f5cadc6530bf81754c307b100067bb56d3324e75f3bf4a38ec \
      /usr/share/milk/student-provenance.json | sha256sum -c - && \
    chmod 0555 /usr/local/bin/dragontales-gateway ./student-job.sh ./student-run.sh && \
    find ./student ./adapter_train ./h100-fp8-runtime ./models \
      -type f -exec chmod 0444 {} + && \
    chmod 0555 /model && find /model -type f -exec chmod 0444 {} + && \
    python3 -c 'from pathlib import Path; import runpy; runpy.run_path("models/qwen3-4b-instruct-2507/verify.py")["verify_tree"](Path("/model"))' && \
    /app/.venv/bin/python adapter_train/sft_entrypoint.py parity \
      --model /model --template student/qwen3-chat-template.jinja

ENV PYTHONDONTWRITEBYTECODE=1
LABEL org.opencontainers.image.licenses="MIT AND Apache-2.0"
USER 0:0
ENTRYPOINT ["/opt/dragontales/deploy/student-job.sh"]
