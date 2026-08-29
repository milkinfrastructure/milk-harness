# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG MILK_GATEWAY_IMAGE
FROM ${MILK_GATEWAY_IMAGE} AS gateway

FROM vllm/vllm-openai@sha256:50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d

COPY --link --chmod=0444 LICENSE /licenses/dragontales/LICENSE
COPY --link --chmod=0444 deploy/teacher/gpt-oss-120b/LICENSE.openai-gpt-oss /licenses/openai-gpt-oss/LICENSE
COPY --link --chmod=0444 deploy/teacher/gpt-oss-120b/USAGE_POLICY.openai-gpt-oss /licenses/openai-gpt-oss/USAGE_POLICY
COPY --link --chmod=0555 deploy/teacher/gpt-oss-120b/runtime.py /opt/dragontales/runtime.py
COPY --link --chmod=0555 deploy/teacher/gpt-oss-120b/job.sh /opt/dragontales/job.sh
COPY --link --chmod=0444 deploy/teacher/gpt-oss-120b/profile.json /opt/dragontales/profile.json
COPY --link --chmod=0444 deploy/teacher/gpt-oss-120b/model.manifest.json /opt/dragontales/model.manifest.json
COPY --link --chmod=0444 deploy/teacher/gpt-oss-120b/provenance.json /opt/dragontales/provenance.json
COPY --from=gateway --link --chmod=0555 /usr/local/bin/milk-carton /usr/local/bin/milk-carton
COPY --from=gateway --link --chmod=0444 /usr/share/licenses/milk-carton/ /usr/share/licenses/milk-carton/

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
