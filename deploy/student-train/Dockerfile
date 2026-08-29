# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG MILK_GATEWAY_IMAGE
FROM ${MILK_GATEWAY_IMAGE} AS gateway

FROM scratch AS wheels
ADD --chmod=0444 --checksum=sha256:e94390c2863b873be18f623f9df48a0d8fe5eff13ea7f1a00092b0a7904888c6 \
    https://files.pythonhosted.org/packages/a8/db/253133d7e7cb40d3af384bb2f5c0b4a2b7fdcffbc95c688cc67a20a3c103/accelerate-1.14.0-py3-none-any.whl \
    /wheels/accelerate-1.14.0-py3-none-any.whl
ADD --chmod=0444 --checksum=sha256:0fbba16ffebfad3de96e06f2da6860fd860292324b85b6141909fa1e26ea9233 \
    https://files.pythonhosted.org/packages/28/79/13bcabb8048126422d5c4b880575d40886c726f354db88cfeed4325525bb/peft-0.20.0-py3-none-any.whl \
    /wheels/peft-0.20.0-py3-none-any.whl

FROM --platform=linux/amd64 ghcr.io/primeintellect-ai/prime-rl@sha256:6b8b2cf339de90b67ed25f246ad3dcfd773cb80e629577eb31aeb2a6b81db80e
ARG MILK_GATEWAY_IMAGE
RUN gateway_repository="${MILK_GATEWAY_IMAGE%@sha256:*}" && \
    gateway_digest="${MILK_GATEWAY_IMAGE##*@sha256:}" && \
    test -n "$gateway_repository" && test "$gateway_repository" != "$MILK_GATEWAY_IMAGE" && \
    test "${#gateway_digest}" -eq 64 && \
    case "$gateway_repository" in *@*) exit 1 ;; esac && \
    case "$gateway_digest" in *[!0-9a-f]*) exit 1 ;; esac
RUN --mount=type=bind,from=wheels,source=/wheels,target=/wheels \
    /usr/local/bin/uv pip install --offline --no-deps --python /app/.venv/bin/python \
      /wheels/accelerate-1.14.0-py3-none-any.whl \
      /wheels/peft-0.20.0-py3-none-any.whl && \
    /app/.venv/bin/python - <<'PY'
from importlib.metadata import version
from peft import PeftModel
import torch

assert version("prime-rl") == "0.9.0"
assert version("accelerate") == "1.14.0"
assert version("peft") == "0.20.0"
assert torch.version.cuda == "12.8"
assert PeftModel
PY

WORKDIR /opt/dragontales/deploy
COPY --from=gateway --chmod=0555 /usr/local/bin/dragontales-gateway /usr/local/bin/dragontales-gateway
COPY --from=gateway --chmod=0444 /usr/share/licenses/dragontales/ /usr/share/licenses/dragontales/
COPY --chmod=0444 LICENSE /usr/share/licenses/milk-harness/LICENSE
COPY --chmod=0444 deploy/licenses/prime-rl-v0.9.0.LICENSE /usr/share/licenses/prime-rl/LICENSE
COPY --chmod=0444 deploy/student-train/provenance.json /usr/share/milk/student-train-provenance.json
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
USER 0:0
RUN test -x /usr/bin/setpriv && \
    printf '%s\n' student-train > /usr/share/milk/student-artifact-role && \
    chmod 0444 /usr/share/milk/student-artifact-role && \
    chmod 0555 /usr/local/bin/dragontales-gateway ./student-job.sh ./student-run.sh && \
    find ./student ./student-train ./student-branch ./adapter_train \
      ./h100-fp8-runtime ./licenses ./models -type f -exec chmod 0444 {} + && \
    python3 models/qwen3-4b-instruct-2507/verify.py >/dev/null

ENV PYTHONDONTWRITEBYTECODE=1
LABEL org.opencontainers.image.licenses="MIT AND Apache-2.0"
ENTRYPOINT ["/opt/dragontales/deploy/student-job.sh"]
