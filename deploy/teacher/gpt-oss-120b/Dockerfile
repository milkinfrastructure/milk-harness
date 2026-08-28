# syntax=docker/dockerfile:1.7
ARG MILK_GATEWAY_IMAGE
FROM ${MILK_GATEWAY_IMAGE} AS gateway

FROM vllm/vllm-openai@sha256:50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d

RUN python3 - <<'PY'
from importlib.metadata import version
from openai_harmony import HarmonyEncodingName

assert version("vllm") == "0.28.0+cu129"
assert HarmonyEncodingName.HARMONY_GPT_OSS
PY

COPY --chmod=0444 LICENSE /licenses/dragontales/LICENSE
COPY --chmod=0444 deploy/teacher/gpt-oss-120b/LICENSE.openai-gpt-oss /licenses/openai-gpt-oss/LICENSE
COPY --chmod=0444 deploy/teacher/gpt-oss-120b/USAGE_POLICY.openai-gpt-oss /licenses/openai-gpt-oss/USAGE_POLICY
COPY --chmod=0555 deploy/teacher/gpt-oss-120b/runtime.py /opt/dragontales/runtime.py
COPY --chmod=0555 deploy/teacher/gpt-oss-120b/job.sh /opt/dragontales/job.sh
COPY --chmod=0444 deploy/teacher/gpt-oss-120b/profile.json /opt/dragontales/profile.json
COPY --chmod=0444 deploy/teacher/gpt-oss-120b/model.manifest.json /opt/dragontales/model.manifest.json
COPY --chmod=0444 deploy/teacher/gpt-oss-120b/provenance.json /opt/dragontales/provenance.json
COPY --from=gateway --chmod=0555 /usr/local/bin/dragontales-gateway /usr/local/bin/dragontales-gateway
COPY --from=gateway --chmod=0444 /usr/share/licenses/dragontales/ /usr/share/licenses/dragontales/

ENV VLLM_SYSTEM_START_DATE=2025-08-05 \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    DO_NOT_TRACK=1 \
    VLLM_NO_USAGE_STATS=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp

LABEL org.opencontainers.image.licenses="MIT AND Apache-2.0" \
      ai.dragontales.model="openai/gpt-oss-120b@b5c939de8f754692c1647ca79fbf85e8c1e70f8a" \
      ai.dragontales.model-inventory-sha256="ea2e51a45254c0edd93cc42d9242065dd5aa094254525f7e69dedd5b0f7b1b26" \
      ai.dragontales.vllm-source-revision="2cf0a6915ce544dc493a0990f2ea38d81601128a"

USER 65532:65532
HEALTHCHECK NONE
ENTRYPOINT ["/opt/dragontales/job.sh"]
