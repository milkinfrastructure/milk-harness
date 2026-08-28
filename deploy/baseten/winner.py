#!/usr/bin/env python3
"""Pure Baseten winner configuration and response validation.

Provider mutation belongs exclusively to ``milk_harness.baseten_winner``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import urllib.parse

from . import adapter as shared
from milk_harness import provider_acceptance


ADMISSION_PATH = Path(__file__).resolve().parents[1] / "modal/admit.py"
SERVING_AUTOSCALING = {
    "min_replica": 0,
    "max_replica": 1,
    "concurrency_target": 8,
    "target_utilization_percentage": 70,
    "autoscaling_window": 10,
    "scale_down_delay": 0,
}
MAX_RESPONSE_BYTES = 1024 * 1024
TRUSS_VERSION = "0.18.25"
TRUSS_VERSION_OUTPUT = f"truss, version {TRUSS_VERSION}\n".encode()
DIRECT_IMAGE_RUNTIME_VERIFIED = False
DIRECT_IMAGE_RUNTIME_BLOCKER = (
    "Baseten winner is disabled until no-build is provider-enabled and an "
    "admitted non-root image can start from a pre-materialized winner without "
    "receiving control-store secrets"
)
MATERIALIZATION_SCHEMA = "dragontales.student-winner-materialization-receipt.v1"
PROBE_SCHEMA = "dragontales.winner-admission-probe.v1"
RECEIPT_SCHEMA = "dragontales.winner-admission-receipt.v2"
RESULT_SCHEMA = "dragontales.student-winner-deployment-result.v1"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MODEL_ID = re.compile(r"[A-Za-z0-9-]{1,128}\Z")
MODEL_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
DEPLOYMENT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
DEPLOYMENT_STATUSES = {
    "ACTIVE",
    "BUILD_FAILED",
    "BUILD_STOPPED",
    "BUILDING",
    "DEACTIVATING",
    "DEPLOY_FAILED",
    "DEPLOYING",
    "FAILED",
    "INACTIVE",
    "LOADING_MODEL",
    "SCALED_TO_ZERO",
    "UNHEALTHY",
    "UPDATING",
    "WAKING_UP",
}
MATERIALIZATION_KEYS = {
    "schema_version",
    "student_job_id",
    "variant",
    "model_manifest_sha256",
    "file_count",
    "artifact_bytes",
    "stage_path",
    "model_path",
    "model_manifest_path",
}
PROBE_KEYS = {
    "schema_version",
    "student_job_id",
    "student_variant",
    "model_manifest_sha256",
    "model_alias_sha256",
    "candidate_api_key_sha256",
    "models_response_sha256",
    "chat_request_sha256",
    "chat_response_sha256",
}
API_KEY_KEYS = {"prefix", "name", "type", "model_ids", "team_name", "owner"}
API_KEY_TYPES = {
    "PERSONAL",
    "WORKSPACE_MANAGE_ALL",
    "WORKSPACE_MANAGE_API_KEYS",
    "WORKSPACE_EXPORT_METRICS",
    "WORKSPACE_INVOKE",
}
RECEIPT_KEY_ORDER = (
    "schema_version",
    "provider",
    "student_job_id",
    "student_variant",
    "model_manifest_sha256",
    "model_alias",
    "model_alias_sha256",
    "candidate_api_key_sha256",
    "student_branch_runtime_image_reference",
    "admission_program_sha256",
    "execution_id",
    "execution_name",
    "chat_completions_url",
    "models_response_sha256",
    "chat_request_sha256",
    "chat_response_sha256",
    "launch_started_at",
    "ready_at",
    "admitted_at",
    "service_not_after",
)
RESULT_KEY_ORDER = (
    "schema_version",
    "scope",
    "student_job_id",
    "claim_sha256",
    "provider_binding_sha256",
    "provider_acceptance",
    "observed_cost_microusd",
    "admission",
)


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _object(value, label):
    if isinstance(value, dict):
        try:
            size = len(json.dumps(value, separators=(",", ":")).encode())
        except (TypeError, ValueError) as error:
            raise ValueError(f"{label} is not strict JSON") from error
        if not 1 <= size <= MAX_RESPONSE_BYTES:
            raise ValueError(f"{label} is not bounded")
        return value
    if not isinstance(value, (bytes, bytearray)) or not 1 <= len(value) <= MAX_RESPONSE_BYTES:
        raise ValueError(f"{label} is not bounded")
    try:
        return shared._strict_object(bytes(value))
    except (UnicodeError, ValueError) as error:
        raise ValueError(f"{label} is not strict JSON") from error


def _canonical(value):
    return shared._canonical(value)


def _digest(value):
    return hashlib.sha256(value).hexdigest()


def _timestamp(value):
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("winner timestamp must be UTC")
    value = value.astimezone(dt.timezone.utc)
    if value.microsecond:
        raise ValueError("winner timestamp must have whole-second precision")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_timestamp(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("winner timestamp is invalid")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("winner timestamp is invalid") from error
    if _timestamp(parsed) != value:
        raise ValueError("winner timestamp is not canonical")
    return parsed


def settings(
    student_branch_image,
    student_job_id,
    model_alias,
    registry_secret,
    config_secret,
    control_store_access_key_secret,
    control_store_secret_key_secret,
    control_store_session_token_secret=None,
):
    if not _matches(HEX64, student_job_id):
        raise ValueError("student job ID must be lowercase hexadecimal SHA-256")
    if not _matches(MODEL_ALIAS, model_alias):
        raise ValueError("model alias is invalid")
    if registry_secret != "DOCKER_REGISTRY_ghcr.io":
        raise ValueError("GHCR registry secret must be named DOCKER_REGISTRY_ghcr.io")
    names = {
        "registry_secret": registry_secret,
        "config_secret": shared.identifier(config_secret, "config secret name"),
        "control_store_access_key_secret": shared.identifier(
            control_store_access_key_secret, "control-store access-key secret name"
        ),
        "control_store_secret_key_secret": shared.identifier(
            control_store_secret_key_secret, "control-store secret-key secret name"
        ),
    }
    if control_store_session_token_secret is not None:
        names["control_store_session_token_secret"] = shared.identifier(
            control_store_session_token_secret,
            "control-store session-token secret name",
        )
    if len(set(names.values())) != len(names):
        raise ValueError("winner secret names must be distinct")
    return {
        "student_branch_image": shared.immutable_ghcr_image(student_branch_image),
        "student_job_id": student_job_id,
        "model_alias": model_alias,
        **names,
    }


def execution_name(run_id):
    if not _matches(HEX64, run_id):
        raise ValueError("winner run ID is invalid")
    return f"milk-winner-{run_id}"


def model_name(run_id):
    if not _matches(HEX64, run_id):
        raise ValueError("winner run ID is invalid")
    return f"milk-winner-{run_id}"


def labels(run_id, claim_sha256):
    if not _matches(HEX64, run_id) or not _matches(HEX64, claim_sha256):
        raise ValueError("winner labels are invalid")
    return {"milk-claim": claim_sha256, "milk-run": run_id}


def truss_config(values, run_id):
    secret_paths = {
        name: f"/secrets/{values[name]}"
        for name in (
            "registry_secret",
            "config_secret",
            "control_store_access_key_secret",
            "control_store_secret_key_secret",
            "control_store_session_token_secret",
        )
        if name in values
    }
    isolation = "\n".join(
        "/usr/bin/setpriv --reuid=65532 --regid=65532 --clear-groups "
        f"--no-new-privs /usr/bin/test ! -r {path}"
        for path in secret_paths.values()
    )
    session_path = secret_paths.get("control_store_session_token_secret")
    session_read = (
        f"MILK_CONTROL_STORE_SESSION_TOKEN=$(cat {session_path})\n"
        if session_path is not None
        else ""
    )
    session_export = " MILK_CONTROL_STORE_SESSION_TOKEN" if session_path else ""
    command = (
        "set -eu\n"
        "umask 077\n"
        "mkdir -m 0700 -p /tmp/dragontales\n"
        f"cp {secret_paths['config_secret']} /tmp/dragontales/gateway.json\n"
        "chmod 0400 /tmp/dragontales/gateway.json\n"
        "MILK_CONTROL_STORE_ACCESS_KEY_ID=$(cat "
        f"{secret_paths['control_store_access_key_secret']})\n"
        "MILK_CONTROL_STORE_SECRET_ACCESS_KEY=$(cat "
        f"{secret_paths['control_store_secret_key_secret']})\n"
        f"{session_read}"
        "export MILK_CONTROL_STORE_ACCESS_KEY_ID "
        f"MILK_CONTROL_STORE_SECRET_ACCESS_KEY{session_export}\n"
        "/usr/local/bin/dragontales-gateway "
        "--config /tmp/dragontales/gateway.json materialize-student-winner "
        '--student-job-id "$DRAGONTALES_STUDENT_JOB_ID" '
        "--stage-dir /tmp/dragontales-winner "
        "> /tmp/dragontales/materialize.stdout\n"
        "unset MILK_CONTROL_STORE_ACCESS_KEY_ID "
        "MILK_CONTROL_STORE_SECRET_ACCESS_KEY MILK_CONTROL_STORE_SESSION_TOKEN\n"
        f"{isolation}\n"
        "cat /tmp/dragontales/materialize.stdout\n"
        "exec /opt/dragontales/deploy/student-job.sh serve "
        "--model /tmp/dragontales-winner/model "
        "--model-manifest /tmp/dragontales-winner/model-manifest.json "
        '--model-alias "$DRAGONTALES_MODEL_ALIAS" --trusted-ingress-auth'
    )
    indented = "\n".join(f"    {line}" for line in command.splitlines())
    secret_names = sorted(
        {
            "DOCKER_REGISTRY_ghcr.io",
            values["config_secret"],
            values["control_store_access_key_secret"],
            values["control_store_secret_key_secret"],
            values.get("control_store_session_token_secret"),
        }
        - {None}
    )
    secrets = "\n".join(f"  {name}: null" for name in secret_names)
    return (
        f"model_name: {model_name(run_id)}\n"
        "base_image:\n"
        f"  image: {values['student_branch_image']}\n"
        "  docker_auth:\n"
        "    auth_method: REGISTRY_SECRET\n"
        "    registry: ghcr.io\n"
        f"    secret_name: {values['registry_secret']}\n"
        "docker_server:\n"
        "  no_build: true\n"
        "  start_command: |-\n"
        f"{indented}\n"
        "  server_port: 8000\n"
        "  predict_endpoint: /v1/chat/completions\n"
        "  readiness_endpoint: /health\n"
        "  liveness_endpoint: /health\n"
        "environment_variables:\n"
        f"  DRAGONTALES_STUDENT_JOB_ID: {json.dumps(values['student_job_id'])}\n"
        f"  DRAGONTALES_MODEL_ALIAS: {json.dumps(values['model_alias'])}\n"
        "resources:\n"
        "  accelerator: H100\n"
        "secrets:\n"
        f"{secrets}\n"
    )


def push_argv(truss_dir, run_id, claim_sha256, team_name):
    if not isinstance(truss_dir, str) or not truss_dir.startswith("/"):
        raise ValueError("Truss directory must be absolute")
    if not _matches(provider_acceptance.TEAM_NAME, team_name):
        raise ValueError("Baseten team name is invalid")
    return [
        "/usr/local/bin/truss",
        "--non-interactive",
        "push",
        truss_dir,
        "--team",
        team_name,
        "--deployment-name",
        execution_name(run_id),
        "--labels",
        json.dumps(labels(run_id, claim_sha256), sort_keys=True, separators=(",", ":")),
        "--wait",
        "--output",
        "json",
        "--disable-truss-download",
    ]


def parse_push(raw, run_id, claim_sha256):
    value = _object(raw, "Truss push output")
    required = {"model_id", "model_version_id", "predict_url", "logs_url", "is_draft"}
    if not required.issubset(value) or not set(value).issubset(required | {"deployment"}):
        raise ValueError("Truss push output fields are invalid")
    model_id = value.get("model_id")
    deployment_id = value.get("model_version_id")
    if (
        not isinstance(model_id, str)
        or not _matches(MODEL_ID, model_id)
        or not isinstance(deployment_id, str)
        or not _matches(DEPLOYMENT_ID, deployment_id)
        or value.get("is_draft") is not False
    ):
        raise ValueError("Truss push did not return one published deployment")
    expected_host = f"model-{model_id}.api.baseten.co"
    for key in ("predict_url", "logs_url"):
        parsed = urllib.parse.urlsplit(value.get(key)) if isinstance(value.get(key), str) else None
        if (
            parsed is None
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError(f"Truss {key} is not canonical HTTPS")
        if key == "predict_url" and parsed.hostname != expected_host:
            raise ValueError("Truss predict URL differs from its model ID")
    return {
        "model_id": model_id,
        "deployment_id": deployment_id,
        "execution_name": execution_name(run_id),
        "labels": labels(run_id, claim_sha256),
        "endpoint": (
            f"https://{expected_host}/deployment/{deployment_id}/sync/v1/chat/completions"
        ),
    }


def validate_invocation_key(value, invocation_key, model_id, team_name):
    if (
        not isinstance(invocation_key, str)
        or not 1 <= len(invocation_key) <= 4096
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in invocation_key)
    ):
        raise ValueError("model-scoped invocation key is invalid")
    response = _object(value, "Baseten API-key metadata")
    keys = response.get("keys")
    if set(response) != {"keys"} or not isinstance(keys, list):
        raise ValueError("Baseten API-key metadata is invalid")
    matches = []
    for item in keys:
        if not isinstance(item, dict) or set(item) != API_KEY_KEYS:
            raise ValueError("Baseten API-key metadata is invalid")
        if (
            not isinstance(item.get("prefix"), str)
            or not item["prefix"]
            or not isinstance(item.get("name"), (str, type(None)))
            or item.get("type") not in API_KEY_TYPES
            or not isinstance(item.get("model_ids"), (list, type(None)))
            or item.get("team_name") is not None
            and not isinstance(item["team_name"], str)
            or item.get("owner") is not None
            and not isinstance(item["owner"], dict)
        ):
            raise ValueError("Baseten API-key metadata is invalid")
        if invocation_key.startswith(item["prefix"]):
            matches.append(item)
    if (
        len(matches) != 1
        or matches[0]["type"] != "WORKSPACE_INVOKE"
        or matches[0]["model_ids"] != [model_id]
        or matches[0]["team_name"] != team_name
        or matches[0]["owner"] is not None
    ):
        raise ValueError("invocation key is not scoped to the exact Baseten model")
    return matches[0]


def validate_deployment(
    value,
    identity,
    *,
    configured=True,
    ready=False,
    stopped=False,
):
    deployment = _object(value, "Baseten deployment")
    autoscaling = deployment.get("autoscaling_settings")
    if (
        deployment.get("id") != identity["deployment_id"]
        or deployment.get("model_id") != identity["model_id"]
        or deployment.get("name") != identity["execution_name"]
        or deployment.get("labels") != identity["labels"]
        or deployment.get("is_production") is not True
        or deployment.get("is_development") is not False
        or deployment.get("environment") not in {None, "production"}
        or deployment.get("instance_type_name") != "H100"
        or type(deployment.get("active_replica_count")) is not int
        or deployment["active_replica_count"] < 0
        or deployment.get("status") not in DEPLOYMENT_STATUSES
    ):
        raise ValueError("Baseten deployment identity or H100 state is invalid")
    if stopped:
        if deployment["status"] != "INACTIVE" or deployment["active_replica_count"] != 0:
            raise ValueError("Baseten deployment is not inactive with zero replicas")
        return deployment
    if configured and (
        not isinstance(autoscaling, dict)
        or any(
            autoscaling.get(key) != expected
            for key, expected in SERVING_AUTOSCALING.items()
        )
    ):
        raise ValueError(
            "Baseten deployment autoscaling differs from the winner contract"
        )
    if ready and deployment["status"] != "ACTIVE":
        raise ValueError("Baseten deployment is not active")
    return deployment


def validate_autoscaling_update(value):
    response = _object(value, "Baseten autoscaling update")
    if (
        set(response) != {"status", "message"}
        or response.get("status") not in {"ACCEPTED", "QUEUED", "UNCHANGED"}
        or not isinstance(response.get("message"), str)
    ):
        raise ValueError("Baseten autoscaling update response is invalid")
    return response


def deployment_logs(value):
    response = _object(value, "Baseten deployment logs")
    if set(response) != {"logs"} or not isinstance(response["logs"], list) or len(response["logs"]) > 1000:
        raise ValueError("Baseten deployment logs response is invalid")
    for log in response["logs"]:
        if (
            not isinstance(log, dict)
            or set(log) != {"timestamp", "message", "replica"}
            or not isinstance(log.get("timestamp"), str)
            or not log["timestamp"].endswith("Z")
            or len(log["timestamp"]) > 64
            or not isinstance(log.get("message"), str)
            or len(log["message"].encode()) > 16 * 1024
            or not isinstance(log.get("replica"), str)
            or not 1 <= len(log["replica"]) <= 255
        ):
            raise ValueError("Baseten deployment log line is invalid")
        try:
            timestamp = dt.datetime.fromisoformat(log["timestamp"][:-1] + "+00:00")
        except ValueError as error:
            raise ValueError("Baseten deployment log timestamp is invalid") from error
        if timestamp.utcoffset() != dt.timedelta(0):
            raise ValueError("Baseten deployment log timestamp is invalid")
    return response


def materialization_from_logs(value, student_job_id):
    response = deployment_logs(value)
    receipts = []
    for log in response["logs"]:
        if MATERIALIZATION_SCHEMA not in log["message"]:
            continue
        receipt = _object(log["message"].encode(), "winner materialization receipt")
        if (
            set(receipt) != MATERIALIZATION_KEYS
            or receipt.get("schema_version") != MATERIALIZATION_SCHEMA
            or receipt.get("student_job_id") != student_job_id
            or receipt.get("variant") not in {"bf16", "dynamic_fp8", "static_fp8"}
            or not _matches(HEX64, receipt.get("model_manifest_sha256"))
            or type(receipt.get("file_count")) is not int
            or receipt["file_count"] <= 0
            or type(receipt.get("artifact_bytes")) is not int
            or receipt["artifact_bytes"] <= 0
            or receipt.get("stage_path") != "/tmp/dragontales-winner"
            or receipt.get("model_path") != "/tmp/dragontales-winner/model"
            or receipt.get("model_manifest_path")
            != "/tmp/dragontales-winner/model-manifest.json"
        ):
            raise ValueError("winner materialization receipt is invalid")
        receipts.append(receipt)
    if not receipts or any(receipt != receipts[0] for receipt in receipts[1:]):
        raise ValueError("winner materialization receipt is missing or conflicting")
    return receipts[0]


def validate_probe(value, student_job_id, alias, materialization, candidate_key):
    probe = _object(value, "winner admission probe")
    if (
        set(probe) != PROBE_KEYS
        or probe.get("schema_version") != PROBE_SCHEMA
        or probe.get("student_job_id") != student_job_id
        or probe.get("student_variant") != materialization.get("variant")
        or probe.get("model_manifest_sha256") != materialization.get("model_manifest_sha256")
        or probe.get("model_alias_sha256") != _digest(alias.encode())
        or probe.get("candidate_api_key_sha256") != _digest(candidate_key.encode())
        or any(not _matches(HEX64, probe.get(key)) for key in (
            "models_response_sha256",
            "chat_request_sha256",
            "chat_response_sha256",
        ))
    ):
        raise ValueError("winner admission probe binding is invalid")
    return probe


def receipt_bytes(receipt):
    if set(receipt) != set(RECEIPT_KEY_ORDER) or receipt.get("schema_version") != RECEIPT_SCHEMA:
        raise ValueError("winner admission receipt fields are invalid")
    ordered = {key: receipt[key] for key in RECEIPT_KEY_ORDER}
    return (json.dumps(ordered, separators=(",", ":")) + "\n").encode()


def result_bytes(result):
    if set(result) != set(RESULT_KEY_ORDER) or result.get("schema_version") != RESULT_SCHEMA:
        raise ValueError("winner deployment result fields are invalid")
    admission = result.get("admission")
    receipt_bytes(admission)
    provider_acceptance.validate(result.get("provider_acceptance"))
    ordered = {key: result[key] for key in RESULT_KEY_ORDER}
    ordered["scope"] = {
        key: result["scope"][key]
        for key in ("tenant_id", "project_id", "environment_id", "workload_id")
    }
    ordered["provider_acceptance"] = result["provider_acceptance"]
    ordered["admission"] = {key: admission[key] for key in RECEIPT_KEY_ORDER}
    return (json.dumps(ordered, separators=(",", ":")) + "\n").encode()
