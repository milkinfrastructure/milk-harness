# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG MILK_GATEWAY_IMAGE
FROM ${MILK_GATEWAY_IMAGE} AS gateway

FROM scratch AS wheels
ADD --chmod=0444 --checksum=sha256:91168237c2d815614c44dfc354c61c613085c811dabbb2ccdb929e9aaa313b8f \
    https://files.pythonhosted.org/packages/2f/bf/e17189bd834e5a4487c97ed576564a66ab56ccd7840608f49dcecf61abfc/compressed_tensors-0.18.0-py3-none-any.whl \
    /wheels/compressed_tensors-0.18.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:c703127445219309bcc77b8fea8b928e81fa70a8a6be9c98eef23b51165fff0c \
    https://files.pythonhosted.org/packages/e8/78/9be717e36007a5edac07ce6a662c1a573e024933090191bc2f88c5818c4c/llmcompressor-0.13.0-py3-none-any.whl \
    /wheels/llmcompressor-0.13.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:4265c11d33a6f688c211bfed87e4698b685473efe1d6faf95d744a3f7c19ca24 \
    https://files.pythonhosted.org/packages/b1/f6/81cddb633e62bb14aa0912443620f43104bda8458505e7c943903d63a134/auto_round-0.14.2-py3-none-any.whl \
    /wheels/auto_round-0.14.2-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:9fbf73688f8c18f7529b4fe592abd04015f81d1e58001e4bac73ffb2b39d7cc4 \
    https://files.pythonhosted.org/packages/44/0b/98fc6eb83333508ca5f44c52b3e287ea8137a0ad582714e2cbc67a02154b/datasets-5.0.1-py3-none-any.whl \
    /wheels/datasets-5.0.1-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:ecd0a012051abc20c1aa87328841efa8cade3ced65803046e38c2f03c0891fea \
    https://files.pythonhosted.org/packages/31/1e/9e366f36efc550f07d6737f199e3f6bffafdf28795d007f10a77dd274f5c/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl \
    /wheels/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl
ADD --chmod=0444 --checksum=sha256:9db974c4079ede2d1a3ea7ca5a240df33f2cc26fc2b36ba64c5f2a4f43b6e725 \
    https://files.pythonhosted.org/packages/6f/67/8d85ca2323233ae3c0365a659c4e52ee1f587b440e4bc577e7d8e4416d0f/transformers-5.14.1-py3-none-any.whl \
    /wheels/transformers-5.14.1-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:02e0b71817df9b2169dc30a16832045764def1191b43dcff5bb85bdee212d2a1 \
    https://files.pythonhosted.org/packages/e5/22/4222d7ddf3da30f363edaa98e329c2bce6c65497c9cb2810931c8b2c0fbc/fsspec-2026.6.0-py3-none-any.whl \
    /wheels/fsspec-2026.6.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:3a56c0e85dd5025161bac5ce138dcac1e49174c7d8e74596537e729fd5c53c28 \
    https://files.pythonhosted.org/packages/71/70/38998b950a97ea279e6bd657575d22d1a2047256caf707d9a10fbce4f065/multiprocess-0.70.19-py312-none-any.whl \
    /wheels/multiprocess-0.70.19-py312-none-any.whl
ADD --chmod=0444 --checksum=sha256:5389cdf79447ed1515c9e31620e6e1e2302249564d603f2ad727d4f6d313e4c3 \
    https://files.pythonhosted.org/packages/60/c9/711ca85d79f1ec98f29a5eae2b051e25b4ecec5de3e3c0e2d5c5dcb15664/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl \
    /wheels/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl
ADD --chmod=0444 --checksum=sha256:d373ce03ffd84010ed9839fa73672a9c8256990532e158440c0085db7d914b34 \
    https://files.pythonhosted.org/packages/12/98/e976540c1addf70442be7842a18cf70884a964abbf69442504f4d2939989/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl \
    /wheels/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl
ADD --chmod=0444 --checksum=sha256:237b8f63a2a0fcfb1ffc06e21dad23add44e6d354b2b014364a1d41e419a4dee \
    https://files.pythonhosted.org/packages/83/97/31bd8b8279e6935a0719f6910ced15e9d5a2cd554b253f6027ce1b5a1c2c/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl \
    /wheels/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl

FROM --platform=linux/amd64 vllm/vllm-openai@sha256:50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d
ARG MILK_GATEWAY_IMAGE
COPY --from=wheels --chmod=0444 \
    /wheels/compressed_tensors-0.18.0-py3-none-any.whl \
    /opt/compressed_tensors-0.18.0-py3-none-any.whl
RUN gateway_repository="${MILK_GATEWAY_IMAGE%@sha256:*}" && \
    gateway_digest="${MILK_GATEWAY_IMAGE##*@sha256:}" && \
    test -n "$gateway_repository" && test "$gateway_repository" != "$MILK_GATEWAY_IMAGE" && \
    test "${#gateway_digest}" -eq 64 && \
    case "$gateway_repository" in *@*) exit 1 ;; esac && \
    case "$gateway_digest" in *[!0-9a-f]*) exit 1 ;; esac
RUN --mount=type=bind,from=wheels,source=/wheels,target=/wheels \
    python3 -m pip install --no-deps --force-reinstall \
      /wheels/compressed_tensors-0.18.0-py3-none-any.whl && \
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
RUN --mount=type=bind,from=wheels,source=/wheels,target=/wheels \
    python3 -m venv --system-site-packages /opt/quant && \
    /opt/quant/bin/pip install --no-index --no-deps --force-reinstall \
      /wheels/llmcompressor-0.13.0-py3-none-any.whl \
      /wheels/auto_round-0.14.2-py3-none-any.whl \
      /wheels/datasets-5.0.1-py3-none-any.whl \
      /wheels/nvidia_nccl_cu12-2.29.7-py3-none-manylinux_2_18_x86_64.whl \
      /wheels/transformers-5.14.1-py3-none-any.whl \
      /wheels/fsspec-2026.6.0-py3-none-any.whl \
      /wheels/multiprocess-0.70.19-py312-none-any.whl \
      /wheels/pyarrow-25.0.1-cp312-cp312-manylinux_2_28_x86_64.whl \
      /wheels/pandas-3.0.5-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl \
      /wheels/xxhash-4.0.1-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl && \
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

WORKDIR /opt/dragontales/deploy
COPY --from=gateway --chmod=0555 /usr/local/bin/dragontales-gateway /usr/local/bin/dragontales-gateway
COPY --from=gateway --chmod=0444 /usr/share/licenses/dragontales/ /usr/share/licenses/dragontales/
COPY --chmod=0444 LICENSE /usr/share/licenses/milk-harness/LICENSE
COPY --chmod=0444 deploy/licenses/prime-rl-v0.9.0.LICENSE /usr/share/licenses/prime-rl/LICENSE
COPY --chmod=0444 deploy/student-branch/provenance.json /usr/share/milk/student-branch-provenance.json
COPY deploy/student-job.sh ./student-job.sh
COPY deploy/student-run.sh ./student-run.sh
COPY deploy/student/runtime.py deploy/student/qwen3-chat-template.jinja ./student/
COPY deploy/student-train/Dockerfile deploy/student-train/provenance.json ./student-train/
COPY deploy/student-branch/Dockerfile deploy/student-branch/provenance.json ./student-branch/
COPY deploy/adapter_train/__init__.py deploy/adapter_train/contract.py \
     deploy/adapter_train/sft_entrypoint.py ./adapter_train/
COPY deploy/h100-fp8-runtime/runtime.py ./h100-fp8-runtime/runtime.py
COPY deploy/licenses/prime-rl-v0.9.0.LICENSE ./licenses/prime-rl-v0.9.0.LICENSE
COPY deploy/models/qwen3-4b-instruct-2507/profile.json \
     deploy/models/qwen3-4b-instruct-2507/model.manifest.sha256 \
     deploy/models/qwen3-4b-instruct-2507/verify.py \
     ./models/qwen3-4b-instruct-2507/
RUN test -x /usr/bin/setpriv && \
    printf '%s\n' student-branch > /usr/share/milk/student-artifact-role && \
    chmod 0444 /usr/share/milk/student-artifact-role && \
    chmod 0555 /usr/local/bin/dragontales-gateway ./student-job.sh ./student-run.sh && \
    find ./student ./student-train ./student-branch ./adapter_train \
      ./h100-fp8-runtime ./licenses ./models -type f -exec chmod 0444 {} + && \
    python3 models/qwen3-4b-instruct-2507/verify.py >/dev/null

ENV PYTHONDONTWRITEBYTECODE=1
LABEL org.opencontainers.image.licenses="MIT AND Apache-2.0"
USER 0:0
ENTRYPOINT ["/opt/dragontales/deploy/student-job.sh"]
