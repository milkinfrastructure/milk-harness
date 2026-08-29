from __future__ import annotations

import concurrent.futures
import copy
import datetime as dt
import hashlib
import json
import math
import os
import re
import socket
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from deploy.baseten import adapter, winner as winner_contract
from milk_harness.budget import (
    CampaignBudget,
    H100_RESERVATION_RATE_MICROUSD_PER_MINUTE as RESERVATION_RATE_MICROUSD_PER_MINUTE,
    baseten_provider_identity,
    baseten_serving_provider_identity,
    modal_provider_identity,
)
from milk_harness import baseten_winner
from milk_harness.evidence import (
    HEX64,
    MAX_LIST_KEYS,
    R2EvidenceStore,
    canonical_json,
    create_same,
)
from milk_harness.image_admission import (
    JOBS_IMAGE_REPOSITORY,
    PRIVATE_IMAGE_REPOSITORIES,
    RELEASE_IMAGE_REPOSITORIES,
    STUDENT_BRANCH_IMAGE_REPOSITORY,
    STUDENT_TRAIN_IMAGE_REPOSITORY,
    TEACHER_IMAGE_REPOSITORY,
    load_published_private_image_release,
)
from milk_harness.log_collection import collect_baseten_terminal_logs
from milk_harness.metrics_collection import collect_baseten_terminal_metrics
from milk_harness.modal_jobs import (
    ModalJobs,
    acceptance_key as modal_acceptance_key,
    definition_key as modal_definition_key,
    validate_definition as validate_modal_definition,
)
from milk_harness.provider_acceptance import (
    OPAQUE,
    TEAM_NAME,
    encode as encode_provider_acceptance,
    sha256 as provider_acceptance_sha256,
    validate_route_retirement,
    winner_run_id as provider_neutral_winner_run_id,
)
from milk_harness.scheduler import (
    claim_provider_jobs_pass,
    load_provider_lease_token,
    store_identity_sha256,
    store_location_sha256,
    verify_provider_lease_token,
)


API_ROOT = "https://api.baseten.co/v1"
MAX_PROVIDER_BYTES = 1024 * 1024
MAX_RECONCILE_GROUPS = 32
MAX_EXECUTIONS = 3
MAX_DUPLICATE_EXECUTIONS = 64
MAX_JOB_EVIDENCE_OBJECTS = 4096
MAX_JOB_EVIDENCE_BYTES = 128 * 1024 * 1024
MAX_MANIFEST_PAGE_OBJECTS = 256
MAX_LOG_RECORDS = 1000
MAX_STOP_ATTEMPTS = 3
MAX_EVIDENCE_FAILURES = 3
MAX_ACTIVE_GPU_LAUNCHES = 18
GPU_LAUNCH_SCAN_LIMIT = MAX_ACTIVE_GPU_LAUNCHES + 1
MAX_GPU_LAUNCH_FRONTIER_BYTES = 4 * 1024
MAX_GPU_LAUNCH_OUTBOX_BYTES = 16 * 1024
MAX_STUDENT_ARTIFACT_REFERENCE_BYTES = 16 * 1024 * 1024
MAX_WINNER_DEPLOYMENT_WALL_SECONDS = 24 * 60 * 60
MAX_WINNER_DEPLOYMENT_COST_MICROUSD = 1_000_000_000
MAX_WINNER_RESULT_REFERENCES = 18
MAX_CANDIDATE_KEY_DELIVERY_REFERENCES = 1
MAX_PROVIDER_TEARDOWN_REFERENCES = 18
PROVIDER_TEARDOWN_SCAN_LIMIT = MAX_PROVIDER_TEARDOWN_REFERENCES + 1
MAX_PROVIDER_TEARDOWN_FRONTIER_BYTES = 16 * 1024
CANDIDATE_KEY_SOCKET = "/run/milk-candidate-key.sock"
MAX_CANDIDATE_KEY_DELIVERY_BYTES = 4096
MAX_CANDIDATE_KEY_SOCKET_REQUEST_BYTES = 32 * 1024
MAX_CANDIDATE_KEY_SOCKET_RESPONSE_BYTES = 4096
MAX_MODAL_CANDIDATE_ACK_BYTES = 4096
BASETEN_WINNER_ADMIT_STATES = {
    "admitted",
    "create_ambiguous_no_replay",
    "candidate_key_create_ambiguous_no_replay",
    "candidate_key_revoke_ambiguous_no_replay",
    "candidate_key_intent_raced",
    "candidate_key_attempts_exhausted",
    "candidate_key_delivery_intent_raced",
    "candidate_key_delivery_failed_revoked",
    "candidate_key_delivery_failed_revoke_ambiguous",
    "candidate_key_delivery_recovery_failed_revoked",
    "candidate_key_delivery_recovery_failed_revoke_ambiguous",
}
BASETEN_TRAINING_PREFLIGHT_SCHEMA = "milk.baseten-training-preflight.v1"
MODAL_PREFLIGHT_SCHEMA = "milk.modal-preflight.v1"
PREPARATION_TIMEOUT_SECONDS = 60
CREATE_MUTATION_TAIL_SECONDS = 120
PROVIDER_CLOCK_SKEW_SECONDS = 300
TERMINAL_STATUSES = {
    "TRAINING_JOB_COMPLETED",
    "TRAINING_JOB_DEPLOY_FAILED",
    "TRAINING_JOB_FAILED",
    "TRAINING_JOB_PREEMPTED",
    "TRAINING_JOB_STOPPED",
}
ACTIVE_STATUSES = {
    "TRAINING_JOB_PENDING",
    "TRAINING_JOB_CREATED",
    "TRAINING_JOB_DEPLOYING",
    "TRAINING_JOB_RUNNING",
}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
GATEWAY_UTC_RFC3339 = re.compile(
    r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.([0-9]{1,9}))?Z\Z"
)
SCOPE_FIELDS = ("tenant_id", "project_id", "environment_id", "workload_id")
STUDENT_VARIANTS = ("bf16", "dynamic_fp8", "static_fp8")
EXECUTION_FIELDS = {
    "provider",
    "project_id",
    "execution_id",
    "execution_name",
    "gpu_type",
    "gpu_count",
    "availability_model",
    "node_count",
    "status",
    "created_at",
    "updated_at",
}


def _strict_object(raw):
    if not isinstance(raw, (bytes, bytearray)) or len(raw) > MAX_PROVIDER_BYTES:
        raise ValueError("provider JSON must be bounded bytes")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("provider JSON contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict):
        raise ValueError("provider JSON must be an object")
    return value


def _candidate_key_socket_transaction(path, raw, timeout_seconds=300):
    if path != CANDIDATE_KEY_SOCKET:
        raise ValueError("candidate-key socket path is invalid")
    value = _strict_object(raw)
    if (
        canonical_json(value) != raw
        or not 1 <= len(raw) <= MAX_CANDIDATE_KEY_SOCKET_REQUEST_BYTES
        or value.get("schema_version")
        not in {
            "milk.baseten-candidate-key-delivery.v1",
            "milk.baseten-candidate-key-delivery-verify.v1",
            "milk.baseten-candidate-key-remove.v1",
        }
    ):
        raise ValueError("candidate-key socket request is invalid")
    if value["schema_version"] == "milk.baseten-candidate-key-delivery.v1" and (
        len(raw) > MAX_CANDIDATE_KEY_DELIVERY_BYTES
        or set(value)
        != {
            "schema_version",
            "run_id",
            "provider",
            "team_name",
            "model_id",
            "key_name",
            "key_prefix",
            "candidate_key_sha256",
            "candidate_api_key",
        }
        or not _is_hex64(value.get("run_id"))
        or value.get("provider") != "baseten"
        or not isinstance(value.get("team_name"), str)
        or TEAM_NAME.fullmatch(value["team_name"]) is None
        or not isinstance(value.get("model_id"), str)
        or winner_contract.MODEL_ID.fullmatch(value["model_id"]) is None
        or not isinstance(value.get("key_name"), str)
        or re.fullmatch(r"[a-z0-9-]{1,64}", value["key_name"]) is None
        or not isinstance(value.get("key_prefix"), str)
        or not 1 <= len(value["key_prefix"]) <= 256
        or any(
            ord(character) < 0x21 or ord(character) > 0x7E
            for character in value["key_prefix"]
        )
        or not isinstance(value.get("candidate_api_key"), str)
        or not 1 <= len(value["candidate_api_key"]) <= 4096
        or any(
            ord(character) < 0x21 or ord(character) > 0x7E
            for character in value["candidate_api_key"]
        )
        or hashlib.sha256(value["candidate_api_key"].encode()).hexdigest()
        != value.get("candidate_key_sha256")
    ):
        raise ValueError("candidate-key delivery request is invalid")
    if value["schema_version"] != "milk.baseten-candidate-key-delivery.v1" and (
        "candidate_api_key" in value
    ):
        raise ValueError("candidate-key control request contains plaintext")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 600:
        raise ValueError("candidate-key socket timeout is invalid")
    metadata = os.lstat(path)
    if not stat.S_ISSOCK(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError("candidate-key endpoint must be an owner-only socket")
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.settimeout(timeout_seconds)
        connection.connect(path)
        connected = os.lstat(path)
        if (
            not stat.S_ISSOCK(connected.st_mode)
            or stat.S_IMODE(connected.st_mode) != 0o600
            or (connected.st_dev, connected.st_ino)
            != (metadata.st_dev, metadata.st_ino)
        ):
            raise OSError("candidate-key socket changed during connection")
        connection.sendall(raw)
        connection.shutdown(socket.SHUT_WR)
        response = bytearray()
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > MAX_CANDIDATE_KEY_SOCKET_RESPONSE_BYTES:
                raise ValueError("candidate-key socket response is oversized")
    finally:
        connection.close()
    response_raw = bytes(response)
    response_value = _strict_object(response_raw)
    if (
        canonical_json(response_value) != response_raw
        or response_value.get("schema_version")
        != "milk.baseten-candidate-key-delivery-ack.v1"
        or "candidate_api_key" in response_value
    ):
        raise ValueError("candidate-key socket response is invalid")
    return response_raw


def _utc(value, label="timestamp"):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC RFC3339")
    return parsed


def _utc_text(value):
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() != dt.timedelta(0):
        raise ValueError("clock must return UTC")
    return value.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _gateway_utc_text(value):
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("gateway clock must return UTC")
    value = value.astimezone(dt.timezone.utc)
    text = value.strftime("%Y-%m-%dT%H:%M:%S")
    if value.microsecond:
        text += "." + f"{value.microsecond:06d}".rstrip("0")
    return text + "Z"


def _gateway_json(raw, max_bytes, label):
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(raw) <= max_bytes:
        raise ValueError(f"{label} must be bounded bytes")
    value = _strict_object(raw)
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    if encoded != bytes(raw):
        raise ValueError(f"{label} is not canonical typed JSON")
    return value


def _datetime_nanos(value):
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("clock must return UTC")
    value = value.astimezone(dt.timezone.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    delta = value - epoch
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _gateway_time(value, label):
    match = GATEWAY_UTC_RFC3339.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.strptime(
            match.group(1),
            "%Y-%m-%dT%H:%M:%S",
        ).replace(tzinfo=dt.timezone.utc)
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    fraction = match.group(2) or ""
    nanos = int(fraction.ljust(9, "0") or "0")
    return parsed.replace(microsecond=nanos // 1_000), (
        _datetime_nanos(parsed) + nanos
    )


def _retained_time_key(nanos):
    if type(nanos) is not int or nanos < 0:
        raise ValueError("retained time is invalid")
    return f"{nanos:020d}"


def _scope_from_prefix(prefix):
    parts = prefix.split("/") if isinstance(prefix, str) else []
    if len(parts) != 6 or parts[:2] != ["dt", "v2"]:
        raise ValueError("scope prefix is invalid")
    values = []
    for value in parts[2:]:
        try:
            parsed = uuid.UUID(value)
        except (AttributeError, ValueError) as error:
            raise ValueError("scope prefix is invalid") from error
        if parsed.int == 0 or str(parsed) != value:
            raise ValueError("scope prefix is invalid")
        values.append(value)
    return dict(zip(SCOPE_FIELDS, values))


def _scope_prefix(scope):
    if not isinstance(scope, dict) or set(scope) != set(SCOPE_FIELDS):
        raise ValueError("gateway scope is invalid")
    prefix = "dt/v2/" + "/".join(scope[field] for field in SCOPE_FIELDS)
    if _scope_from_prefix(prefix) != scope:
        raise ValueError("gateway scope is invalid")
    return prefix


def _is_hex64(value):
    return isinstance(value, str) and HEX64.fullmatch(value) is not None


def _valid_counts(value):
    return (
        isinstance(value, dict)
        and tuple(value) == ("train", "dev", "calibration")
        and type(value.get("train")) is int
        and 50 <= value["train"] <= 10_000
        and type(value.get("dev")) is int
        and 73 <= value["dev"] <= 128
        and value.get("calibration") == 128
    )


def _valid_branches(value):
    if not isinstance(value, list) or len(value) != len(STUDENT_VARIANTS):
        return False
    for branch, variant in zip(value, STUDENT_VARIANTS):
        if (
            not isinstance(branch, dict)
            or tuple(branch)
            != ("schema_version", "branch_id", "variant", "max_gpu_seconds")
            or branch.get("schema_version")
            != "dragontales.student-branch-claim.v2"
            or not _is_hex64(branch.get("branch_id"))
            or branch.get("variant") != variant
            or branch.get("max_gpu_seconds") != 1_800
        ):
            return False
    return True


def _gateway_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _valid_uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError):
        return False
    return parsed.int != 0 and str(parsed) == value


def _validate_gpu_operation(operation):
    if not isinstance(operation, dict):
        raise ValueError("GPU launch operation is invalid")
    kind = operation.get("kind")
    if kind == "teacher_run":
        if (
            tuple(operation)
            != (
                "kind",
                "teacher_run_id",
                "provider_binding_sha256",
                "slot",
                "call_count",
                "max_gpu_seconds",
            )
            or not _is_hex64(operation.get("teacher_run_id"))
            or not _is_hex64(operation.get("provider_binding_sha256"))
            or type(operation.get("slot")) is not int
            or not 0 <= operation["slot"] < 16
            or type(operation.get("call_count")) is not int
            or not 1 <= operation["call_count"] <= 64
            or type(operation.get("max_gpu_seconds")) is not int
            or not 1 <= operation["max_gpu_seconds"] <= 3_600
        ):
            raise ValueError("teacher GPU launch operation is invalid")
        return
    if kind == "student_train_merge":
        if (
            tuple(operation)
            != (
                "kind",
                "student_job_id",
                "counts",
                "max_gpu_seconds",
            )
            or not _is_hex64(operation.get("student_job_id"))
            or not _valid_counts(operation.get("counts"))
            or operation.get("max_gpu_seconds") != 1_800
        ):
            raise ValueError("student train GPU launch operation is invalid")
        return
    if kind == "student_fanout":
        if (
            tuple(operation)
            != (
                "kind",
                "student_job_id",
                "max_total_gpu_seconds",
                "branches",
            )
            or not _is_hex64(operation.get("student_job_id"))
            or operation.get("max_total_gpu_seconds") != 7_200
            or not _valid_branches(operation.get("branches"))
        ):
            raise ValueError("student fanout GPU launch operation is invalid")
        return
    if kind == "student_winner_deployment":
        if (
            tuple(operation)
            != (
                "kind",
                "student_job_id",
                "student_result_sha256",
                "winner",
                "provider_binding_sha256",
                "provider_policy",
                "max_wall_seconds",
                "max_cost_microusd",
            )
            or not _is_hex64(operation.get("student_job_id"))
            or not _is_hex64(operation.get("student_result_sha256"))
            or operation.get("winner") not in STUDENT_VARIANTS
            or not _is_hex64(operation.get("provider_binding_sha256"))
            or operation.get("provider_policy")
            != {"primary": "baseten", "fallback": "modal"}
            or tuple(operation.get("provider_policy", ()))
            != ("primary", "fallback")
            or type(operation.get("max_wall_seconds")) is not int
            or not 60
            <= operation["max_wall_seconds"]
            <= MAX_WINNER_DEPLOYMENT_WALL_SECONDS
            or type(operation.get("max_cost_microusd")) is not int
            or not 1
            <= operation["max_cost_microusd"]
            <= MAX_WINNER_DEPLOYMENT_COST_MICROUSD
        ):
            raise ValueError("student winner GPU launch operation is invalid")
        return
    raise ValueError("GPU launch operation kind is invalid")


def _gpu_launch_keys(scope_prefix, operation):
    kind = operation["kind"]
    if kind == "teacher_run":
        parent = f"{scope_prefix}/jobs/teacher-gpu/{operation['teacher_run_id']}"
    elif kind == "student_train_merge":
        parent = f"{scope_prefix}/jobs/student/{operation['student_job_id']}"
    elif kind == "student_fanout":
        parent = (
            f"{scope_prefix}/jobs/student/{operation['student_job_id']}/fanout"
        )
    else:
        parent = (
            f"{scope_prefix}/jobs/student/{operation['student_job_id']}"
            "/winner-deployment"
        )
    return parent + "/claim.json", parent + "/launch-outbox.json"


def _verify_teacher_claim(claim, operation, outbox, scope):
    definition = claim.get("definition") if isinstance(claim, dict) else None
    calls = definition.get("calls") if isinstance(definition, dict) else None
    if (
        tuple(claim)
        != ("schema_version", "scope", "teacher_run_id", "definition", "claimed_at")
        or claim.get("schema_version")
        != "dragontales.teacher-gpu-run-claim.v1"
        or not isinstance(claim.get("scope"), dict)
        or tuple(claim["scope"]) != SCOPE_FIELDS
        or claim.get("scope") != scope
        or claim.get("teacher_run_id") != operation["teacher_run_id"]
        or not isinstance(definition, dict)
        or tuple(definition)
        != (
            "schema_version",
            "provider_binding_sha256",
            "slot",
            "run_nonce",
            "slot_claim_sha256",
            "runtime_image_reference",
            "max_gpu_seconds",
            "calls",
            "expires_at",
        )
        or definition.get("schema_version")
        != "dragontales.teacher-gpu-run-definition.v1"
        or definition.get("provider_binding_sha256")
        != operation["provider_binding_sha256"]
        or definition.get("slot") != operation["slot"]
        or not _valid_uuid(definition.get("run_nonce"))
        or not _is_hex64(definition.get("slot_claim_sha256"))
        or definition.get("runtime_image_reference")
        != outbox["runtime_image_reference"]
        or definition.get("max_gpu_seconds") != operation["max_gpu_seconds"]
        or not isinstance(calls, list)
        or len(calls) != operation["call_count"]
        or definition.get("expires_at") != outbox["expires_at"]
        or claim.get("claimed_at") != outbox["created_at"]
        or hashlib.sha256(_gateway_bytes(definition)).hexdigest()
        != operation["teacher_run_id"]
    ):
        raise ValueError("teacher GPU launch claim is invalid")
    prior = None
    for call in calls:
        if (
            not isinstance(call, dict)
            or tuple(call)
            != ("teacher_job_id", "claim_sha256", "dispatch_sha256")
            or not _is_hex64(call.get("teacher_job_id"))
            or not _is_hex64(call.get("claim_sha256"))
            or not _is_hex64(call.get("dispatch_sha256"))
            or prior is not None
            and call["teacher_job_id"] <= prior
        ):
            raise ValueError("teacher GPU launch calls are invalid")
        prior = call["teacher_job_id"]


def _verify_student_claim(claim, operation, outbox, scope):
    definition = claim.get("definition") if isinstance(claim, dict) else None
    references = (
        definition.get("teacher_results") if isinstance(definition, dict) else None
    )
    quality = definition.get("quality") if isinstance(definition, dict) else None
    if (
        tuple(claim)
        != ("schema_version", "scope", "student_job_id", "definition", "started_at")
        or claim.get("schema_version") != "dragontales.student-job-claim.v4"
        or not isinstance(claim.get("scope"), dict)
        or tuple(claim["scope"]) != SCOPE_FIELDS
        or claim.get("scope") != scope
        or claim.get("student_job_id") != operation["student_job_id"]
        or not isinstance(definition, dict)
        or tuple(definition)
        != (
            "schema_version",
            "teacher_provider_binding_sha256",
            "teacher_results",
            "input_sha256",
            "dev_set_sha256",
            "counts",
            "recipe_sha256",
            "student_train_runtime_image_reference",
            "student_branch_runtime_image_reference",
            "max_train_gpu_seconds",
            "max_branch_gpu_seconds",
            "max_total_gpu_seconds",
            "quality",
            "expires_at",
        )
        or definition.get("schema_version")
        != "dragontales.student-job-definition.v4"
        or not _is_hex64(definition.get("teacher_provider_binding_sha256"))
        or not isinstance(references, list)
        or not references
        or len(references) > 4_096
        or not _is_hex64(definition.get("input_sha256"))
        or not _is_hex64(definition.get("dev_set_sha256"))
        or definition.get("counts") != operation["counts"]
        or not _is_hex64(definition.get("recipe_sha256"))
        or definition.get("student_train_runtime_image_reference")
        != outbox["runtime_image_reference"]
        or adapter.immutable_image(
            definition.get("student_branch_runtime_image_reference")
        )
        != definition["student_branch_runtime_image_reference"]
        or definition.get("max_train_gpu_seconds") != 1_800
        or definition.get("max_branch_gpu_seconds") != 1_800
        or definition.get("max_total_gpu_seconds") != 7_200
        or quality
        != {
            "min_mean_score_bps": 9_500,
            "max_mean_loss_vs_bf16_bps": 0,
            "max_p95_latency_ms": 30_000,
        }
        or tuple(quality or ())
        != (
            "min_mean_score_bps",
            "max_mean_loss_vs_bf16_bps",
            "max_p95_latency_ms",
        )
        or definition.get("expires_at") != outbox["expires_at"]
        or claim.get("started_at") != outbox["created_at"]
        or hashlib.sha256(_gateway_bytes(definition)).hexdigest()
        != operation["student_job_id"]
    ):
        raise ValueError("student train GPU launch claim is invalid")
    prior = None
    for reference in references:
        if (
            not isinstance(reference, dict)
            or tuple(reference) != ("teacher_job_id", "object_sha256", "bytes")
            or not _is_hex64(reference.get("teacher_job_id"))
            or not _is_hex64(reference.get("object_sha256"))
            or type(reference.get("bytes")) is not int
            or reference["bytes"] <= 0
            or prior is not None
            and reference["teacher_job_id"] <= prior
        ):
            raise ValueError("student train teacher references are invalid")
        prior = reference["teacher_job_id"]


def _verify_fanout_claim(claim, operation, outbox, scope):
    if (
        tuple(claim)
        != (
            "schema_version",
            "scope",
            "student_job_id",
            "train_result_sha256",
            "recipe_sha256",
            "student_branch_runtime_image_reference",
            "max_total_gpu_seconds",
            "created_at",
            "expires_at",
            "branches",
        )
        or claim.get("schema_version")
        != "dragontales.student-fanout-claim.v4"
        or not isinstance(claim.get("scope"), dict)
        or tuple(claim["scope"]) != SCOPE_FIELDS
        or claim.get("scope") != scope
        or claim.get("student_job_id") != operation["student_job_id"]
        or not _is_hex64(claim.get("train_result_sha256"))
        or not _is_hex64(claim.get("recipe_sha256"))
        or claim.get("student_branch_runtime_image_reference")
        != outbox["runtime_image_reference"]
        or claim.get("max_total_gpu_seconds")
        != operation["max_total_gpu_seconds"]
        or claim.get("created_at") != outbox["created_at"]
        or claim.get("expires_at") != outbox["expires_at"]
        or claim.get("branches") != operation["branches"]
        or not _valid_branches(claim.get("branches"))
    ):
        raise ValueError("student fanout GPU launch claim is invalid")


def _verify_winner_artifact(reference, scope_prefix, student_job_id):
    if (
        not isinstance(reference, dict)
        or tuple(reference) != ("object_key", "sha256", "bytes")
        or not _is_hex64(reference.get("sha256"))
        or reference.get("object_key")
        != (
            f"{scope_prefix}/artifacts/{student_job_id}/"
            f"{reference.get('sha256')}"
        )
        or type(reference.get("bytes")) is not int
        or not 1 <= reference["bytes"] <= MAX_STUDENT_ARTIFACT_REFERENCE_BYTES
    ):
        raise ValueError("student winner artifact reference is invalid")


def _verify_winner_authority(authority, operation, outbox):
    if (
        not isinstance(authority, dict)
        or tuple(authority)
        != (
            "schema_version",
            "provider_policy",
            "provider_terms_sha256",
            "student_branch_runtime_image_reference",
            "admission_program_sha256",
            "authorization_not_after",
            "max_wall_seconds",
            "max_cost_microusd",
            "allow_private_candidate_http",
            "signing_public_key_hex",
            "signing_key_id",
            "candidate_max_in_flight",
            "canary_candidate_basis_points",
            "canary_valid_for_seconds",
            "max_input_utf8_bytes",
            "max_input_messages",
            "max_input_request_bytes",
            "route_schema_version",
            "winner_admission_schema_version",
        )
        or authority.get("schema_version")
        != "dragontales.winner-deployment-authority.v2"
        or authority.get("provider_policy") != operation["provider_policy"]
        or tuple(authority.get("provider_policy", ()))
        != ("primary", "fallback")
        or not _is_hex64(authority.get("provider_terms_sha256"))
        or authority.get("student_branch_runtime_image_reference")
        != outbox["runtime_image_reference"]
        or not _is_hex64(authority.get("admission_program_sha256"))
        or authority.get("authorization_not_after") != outbox["expires_at"]
        or authority.get("max_wall_seconds") != operation["max_wall_seconds"]
        or authority.get("max_cost_microusd") != operation["max_cost_microusd"]
        or type(authority.get("allow_private_candidate_http")) is not bool
        or not _is_hex64(authority.get("signing_public_key_hex"))
        or not isinstance(authority.get("signing_key_id"), str)
        or not 1 <= len(authority["signing_key_id"].encode()) <= 128
        or type(authority.get("candidate_max_in_flight")) is not int
        or authority["candidate_max_in_flight"] <= 0
        or authority.get("canary_candidate_basis_points") != 100
        or authority.get("canary_valid_for_seconds") != 15 * 60
        or authority.get("max_input_utf8_bytes") != 2_048
        or authority.get("max_input_messages") != 16
        or authority.get("max_input_request_bytes") != 16_384
        or authority.get("route_schema_version") != "dragontales.route.v4"
        or authority.get("winner_admission_schema_version")
        != "dragontales.winner-admission-receipt.v2"
    ):
        raise ValueError("student winner deployment authority is invalid")
    unused_authorized, authorized_nanos = _gateway_time(
        authority["authorization_not_after"],
        "winner authorization expiry",
    )
    del unused_authorized
    if authorized_nanos % 1_000_000_000:
        raise ValueError("student winner authorization expiry is not whole-second")
    binding = hashlib.sha256(
        b"dragontales.winner-provider-binding.v1\0"
        + _gateway_bytes(authority)
    ).hexdigest()
    if binding != operation["provider_binding_sha256"]:
        raise ValueError("student winner provider binding differs")


def _verify_winner_claim(claim, operation, outbox, scope, scope_prefix):
    authority = claim.get("authority") if isinstance(claim, dict) else None
    if (
        tuple(claim)
        != (
            "schema_version",
            "scope",
            "student_job_id",
            "student_claim_sha256",
            "student_result_sha256",
            "winner",
            "model_manifest",
            "dev_receipt",
            "student_branch_runtime_image_reference",
            "provider_binding_sha256",
            "authority",
            "claimed_at",
            "expires_at",
        )
        or claim.get("schema_version")
        != "dragontales.student-winner-deployment-claim.v2"
        or not isinstance(claim.get("scope"), dict)
        or tuple(claim["scope"]) != SCOPE_FIELDS
        or claim.get("scope") != scope
        or claim.get("student_job_id") != operation["student_job_id"]
        or not _is_hex64(claim.get("student_claim_sha256"))
        or claim.get("student_result_sha256")
        != operation["student_result_sha256"]
        or claim.get("winner") != operation["winner"]
        or claim.get("student_branch_runtime_image_reference")
        != outbox["runtime_image_reference"]
        or claim.get("provider_binding_sha256")
        != operation["provider_binding_sha256"]
        or claim.get("claimed_at") != outbox["created_at"]
        or claim.get("expires_at") != outbox["expires_at"]
    ):
        raise ValueError("student winner GPU launch claim is invalid")
    _verify_winner_artifact(
        claim["model_manifest"], scope_prefix, operation["student_job_id"]
    )
    _verify_winner_artifact(
        claim["dev_receipt"], scope_prefix, operation["student_job_id"]
    )
    _verify_winner_authority(authority, operation, outbox)


def _winner_launch_bytes(claim, operation, claim_key, claim_sha256):
    return _gateway_bytes(
        {
            "schema_version": "dragontales.student-winner-deployment-launch.v2",
            "student_job_id": operation["student_job_id"],
            "student_result_sha256": operation["student_result_sha256"],
            "winner": operation["winner"],
            "deployment_claim_object_key": claim_key,
            "deployment_claim_sha256": claim_sha256,
            "provider_binding_sha256": operation["provider_binding_sha256"],
            "provider_policy": operation["provider_policy"],
            "student_branch_runtime_image_reference": claim[
                "student_branch_runtime_image_reference"
            ],
            "max_wall_seconds": operation["max_wall_seconds"],
            "max_cost_microusd": operation["max_cost_microusd"],
            "expires_at": claim["expires_at"],
        }
    )


def _verify_gpu_launch_chain(control_store, key, scope_prefix, scope, current_nanos):
    frontier_raw = control_store.get(key)
    frontier = _gateway_json(
        frontier_raw,
        MAX_GPU_LAUNCH_FRONTIER_BYTES,
        "GPU launch frontier",
    )
    if (
        tuple(frontier)
        != (
            "schema_version",
            "scope",
            "dispatch_id",
            "outbox_object_key",
            "outbox_sha256",
            "expires_at",
        )
        or frontier.get("schema_version")
        != "dragontales.gpu-launch-frontier.v1"
        or not isinstance(frontier.get("scope"), dict)
        or tuple(frontier["scope"]) != SCOPE_FIELDS
        or frontier.get("scope") != scope
        or not _is_hex64(frontier.get("dispatch_id"))
        or not isinstance(frontier.get("outbox_object_key"), str)
        or not _is_hex64(frontier.get("outbox_sha256"))
    ):
        raise ValueError("GPU launch frontier is invalid")
    unused_expiry, expiry_nanos = _gateway_time(
        frontier.get("expires_at"),
        "GPU launch expiry",
    )
    del unused_expiry
    expected_frontier_key = (
        f"{scope_prefix}/frontier/gpu-launch/"
        f"{_retained_time_key(expiry_nanos)}/{frontier['dispatch_id']}.json"
    )
    if key != expected_frontier_key or expiry_nanos <= current_nanos:
        raise ValueError("GPU launch frontier key or expiry is invalid")

    outbox_raw = control_store.get(frontier["outbox_object_key"])
    if hashlib.sha256(outbox_raw).hexdigest() != frontier["outbox_sha256"]:
        raise ValueError("GPU launch outbox hash differs")
    outbox = _gateway_json(
        outbox_raw,
        MAX_GPU_LAUNCH_OUTBOX_BYTES,
        "GPU launch outbox",
    )
    operation = outbox.get("operation") if isinstance(outbox, dict) else None
    if (
        tuple(outbox)
        != (
            "schema_version",
            "scope",
            "dispatch_id",
            "claim_object_key",
            "claim_sha256",
            "runtime_image_reference",
            "created_at",
            "expires_at",
            "operation",
        )
        or outbox.get("schema_version") != "dragontales.gpu-launch-outbox.v1"
        or not isinstance(outbox.get("scope"), dict)
        or tuple(outbox["scope"]) != SCOPE_FIELDS
        or outbox.get("scope") != scope
        or outbox.get("dispatch_id") != frontier["dispatch_id"]
        or outbox.get("dispatch_id") != outbox.get("claim_sha256")
        or not _is_hex64(outbox.get("claim_sha256"))
        or not isinstance(outbox.get("claim_object_key"), str)
        or outbox.get("expires_at") != frontier["expires_at"]
    ):
        raise ValueError("GPU launch outbox is invalid")
    adapter.immutable_image(outbox.get("runtime_image_reference"))
    _validate_gpu_operation(operation)
    claim_key, outbox_key = _gpu_launch_keys(scope_prefix, operation)
    if (
        outbox["claim_object_key"] != claim_key
        or frontier["outbox_object_key"] != outbox_key
    ):
        raise ValueError("GPU launch object key chain is invalid")
    unused_created, created_nanos = _gateway_time(
        outbox.get("created_at"),
        "GPU launch created_at",
    )
    del unused_created
    if created_nanos >= expiry_nanos:
        raise ValueError("GPU launch timestamps are invalid")
    runway_seconds = (
        max(branch["max_gpu_seconds"] for branch in operation["branches"])
        if operation["kind"] == "student_fanout"
        else operation["max_wall_seconds"]
        if operation["kind"] == "student_winner_deployment"
        else operation["max_gpu_seconds"]
    )
    if current_nanos + runway_seconds * 1_000_000_000 > expiry_nanos:
        raise ValueError("GPU launch has insufficient execution runway")

    claim_raw = control_store.get(claim_key)
    if hashlib.sha256(claim_raw).hexdigest() != outbox["claim_sha256"]:
        raise ValueError("GPU launch claim hash differs")
    claim = _gateway_json(claim_raw, MAX_PROVIDER_BYTES, "GPU launch claim")
    if operation["kind"] == "teacher_run":
        _verify_teacher_claim(claim, operation, outbox, scope)
    elif operation["kind"] == "student_train_merge":
        _verify_student_claim(claim, operation, outbox, scope)
    elif operation["kind"] == "student_fanout":
        _verify_fanout_claim(claim, operation, outbox, scope)
    else:
        _verify_winner_claim(claim, operation, outbox, scope, scope_prefix)
    claimed_at = (
        claim.get("claimed_at")
        if operation["kind"] == "teacher_run"
        else claim.get("started_at")
        if operation["kind"] == "student_train_merge"
        else claim.get("created_at")
        if operation["kind"] == "student_fanout"
        else claim.get("claimed_at")
    )
    unused_claimed, claimed_nanos = _gateway_time(
        claimed_at,
        "GPU launch claim creation time",
    )
    del unused_claimed
    claim_expiry = (
        claim["definition"]["expires_at"]
        if operation["kind"] in {"teacher_run", "student_train_merge"}
        else claim["expires_at"]
    )
    unused_claim_expiry, claim_expiry_nanos = _gateway_time(
        claim_expiry,
        "GPU launch claim expiry",
    )
    del unused_claim_expiry
    if claimed_nanos != created_nanos or claim_expiry_nanos != expiry_nanos:
        raise ValueError("GPU launch claim timestamps differ")
    launch_source = {
        "schema_version": "milk.gateway-launch-source.v1",
        "scope": scope,
        "dispatch_id": frontier["dispatch_id"],
        "frontier_object_key": key,
        "frontier_sha256": hashlib.sha256(frontier_raw).hexdigest(),
        "outbox_object_key": outbox_key,
        "outbox_sha256": frontier["outbox_sha256"],
        "claim_object_key": claim_key,
        "claim_sha256": outbox["claim_sha256"],
        "created_at": outbox["created_at"],
        "expires_at": outbox["expires_at"],
    }
    verified = {
        "launch_source": launch_source,
        "runtime_image_reference": outbox["runtime_image_reference"],
        "operation": operation,
    }
    if operation["kind"] == "student_winner_deployment":
        verified.update(
            gateway_launch_raw=_winner_launch_bytes(
                claim,
                operation,
                claim_key,
                outbox["claim_sha256"],
            ),
            gateway_claim_raw=bytes(claim_raw),
        )
    return verified


def discover_gpu_launches(control_store, scope_prefix, current):
    scope = _scope_from_prefix(scope_prefix)
    current_nanos = _datetime_nanos(current)
    prefix = f"{scope_prefix}/frontier/gpu-launch"
    start_after = (
        f"{prefix}/{_retained_time_key(current_nanos)}/{'f' * 64}.json"
    )
    keys = control_store.list(
        prefix,
        limit=GPU_LAUNCH_SCAN_LIMIT,
        start_after=start_after,
    )
    if keys != sorted(set(keys)):
        raise ValueError("GPU launch frontier listing is not canonical")
    if len(keys) == GPU_LAUNCH_SCAN_LIMIT:
        raise ValueError("GPU launch frontier exceeds its hard active bound")
    launches = [
        _verify_gpu_launch_chain(
            control_store,
            key,
            scope_prefix,
            scope,
            current_nanos,
        )
        for key in keys
    ]
    dispatch_ids = [item["launch_source"]["dispatch_id"] for item in launches]
    if len(dispatch_ids) != len(set(dispatch_ids)):
        raise ValueError("GPU launch frontier contains duplicate dispatches")
    return launches


def discover_provider_teardown_authorizations(
    control_store,
    scope_prefix,
    current,
):
    """Load gateway-owned teardown authority without mutating control state."""
    scope = _scope_from_prefix(scope_prefix)
    current_nanos = _datetime_nanos(current)
    prefix = f"{scope_prefix}/frontier/provider-teardown"
    keys = control_store.list(
        prefix,
        limit=PROVIDER_TEARDOWN_SCAN_LIMIT,
    )
    if keys != sorted(set(keys)):
        raise ValueError("provider teardown frontier listing is not canonical")
    if len(keys) == PROVIDER_TEARDOWN_SCAN_LIMIT:
        raise ValueError("provider teardown frontier exceeds its hard active bound")
    records = []
    for key in keys:
        student_job_id = key.removeprefix(prefix + "/").removesuffix(".json")
        if (
            not _is_hex64(student_job_id)
            or key != f"{prefix}/{student_job_id}.json"
        ):
            raise ValueError("provider teardown frontier key is invalid")
        frontier_raw = control_store.get(key)
        frontier = _gateway_json(
            frontier_raw,
            MAX_PROVIDER_TEARDOWN_FRONTIER_BYTES,
            "provider teardown frontier",
        )
        authorization_key = (
            f"{scope_prefix}/jobs/student/{student_job_id}/"
            "winner-deployment/teardown-authorization.json"
        )
        if (
            tuple(frontier)
            != (
                "schema_version",
                "scope",
                "student_job_id",
                "authorization_object_key",
                "authorization_sha256",
            )
            or frontier.get("schema_version")
            != "dragontales.provider-teardown-frontier.v1"
            or frontier.get("scope") != scope
            or tuple(frontier.get("scope", ())) != SCOPE_FIELDS
            or frontier.get("student_job_id") != student_job_id
            or frontier.get("authorization_object_key") != authorization_key
            or not _is_hex64(frontier.get("authorization_sha256"))
        ):
            raise ValueError("provider teardown frontier is invalid")
        authorization_raw = control_store.get(authorization_key)
        if (
            hashlib.sha256(authorization_raw).hexdigest()
            != frontier["authorization_sha256"]
        ):
            raise ValueError("provider teardown authorization hash differs")
        authorization = _gateway_json(
            authorization_raw,
            MAX_PROVIDER_TEARDOWN_FRONTIER_BYTES,
            "provider teardown authorization",
        )
        winner_result_key = (
            f"{scope_prefix}/jobs/student/{student_job_id}/"
            "winner-deployment/result.json"
        )
        if (
            tuple(authorization)
            != (
                "schema_version",
                "scope",
                "student_job_id",
                "claim_sha256",
                "winner_result_object_key",
                "winner_result_sha256",
                "provider_acceptance_sha256",
                "run_id",
                "selected_provider",
                "execution_id",
                "trigger",
                "authorized_at",
            )
            or authorization.get("schema_version")
            != "dragontales.provider-teardown-authorization.v1"
            or authorization.get("scope") != scope
            or tuple(authorization.get("scope", ())) != SCOPE_FIELDS
            or authorization.get("student_job_id") != student_job_id
            or not _is_hex64(authorization.get("claim_sha256"))
            or authorization.get("winner_result_object_key")
            != winner_result_key
            or not _is_hex64(authorization.get("winner_result_sha256"))
            or not _is_hex64(
                authorization.get("provider_acceptance_sha256")
            )
            or not _is_hex64(authorization.get("run_id"))
            or authorization.get("selected_provider")
            not in {"baseten", "modal"}
            or not isinstance(authorization.get("execution_id"), str)
            or OPAQUE.fullmatch(authorization["execution_id"]) is None
        ):
            raise ValueError("provider teardown authorization is invalid")
        authorized_at, authorized_nanos = _gateway_time(
            authorization.get("authorized_at"),
            "provider teardown authorized_at",
        )
        if authorized_nanos > current_nanos:
            raise ValueError("provider teardown authority is not active yet")
        winner_result_raw = control_store.get(winner_result_key)
        if (
            hashlib.sha256(winner_result_raw).hexdigest()
            != authorization["winner_result_sha256"]
        ):
            raise ValueError("provider teardown winner result hash differs")
        winner_result = _gateway_json(
            winner_result_raw,
            MAX_PROVIDER_BYTES,
            "provider teardown winner result",
        )
        if winner_contract.result_bytes(winner_result) != winner_result_raw + b"\n":
            raise ValueError("provider teardown winner result is not canonical")
        acceptance = winner_result["provider_acceptance"]
        admission = winner_result["admission"]
        if (
            provider_acceptance_sha256(acceptance)
            != authorization["provider_acceptance_sha256"]
            or winner_result.get("student_job_id") != student_job_id
            or winner_result.get("claim_sha256")
            != authorization["claim_sha256"]
            or acceptance.get("run_id") != authorization["run_id"]
            or acceptance.get("selection", {}).get("selected_provider")
            != authorization["selected_provider"]
            or admission.get("execution_id")
            != authorization["execution_id"]
            or authorized_at
            < _gateway_time(
                admission.get("admitted_at"),
                "winner admitted_at",
            )[0]
        ):
            raise ValueError(
                "provider teardown authority differs from the admitted winner"
            )
        trigger = authorization.get("trigger")
        gateway_claim_raw = None
        canary_route_raw = None
        zero_route_raw = None
        if isinstance(trigger, dict) and trigger.get("kind") == "route_zero":
            retirement_key = (
                f"{scope_prefix}/routes/retired-students/{student_job_id}.json"
            )
            if (
                tuple(trigger)
                != (
                    "kind",
                    "retirement_object_key",
                    "retirement_sha256",
                    "zero_route_revision",
                    "canary_route_receipt",
                    "zero_route_receipt",
                )
                or trigger.get("retirement_object_key") != retirement_key
                or not _is_hex64(trigger.get("retirement_sha256"))
                or not _is_hex64(trigger.get("zero_route_revision"))
            ):
                raise ValueError("provider teardown route trigger is invalid")
            gateway_claim_key = (
                f"{scope_prefix}/jobs/student/{student_job_id}/"
                "winner-deployment/claim.json"
            )
            gateway_claim_raw = control_store.get(gateway_claim_key)
            if (
                hashlib.sha256(gateway_claim_raw).hexdigest()
                != authorization["claim_sha256"]
            ):
                raise ValueError("provider teardown gateway claim hash differs")
            gateway_claim = _gateway_json(
                gateway_claim_raw,
                MAX_PROVIDER_BYTES,
                "provider teardown gateway claim",
            )
            authority = gateway_claim.get("authority")
            if (
                tuple(gateway_claim)
                != (
                    "schema_version",
                    "scope",
                    "student_job_id",
                    "student_claim_sha256",
                    "student_result_sha256",
                    "winner",
                    "model_manifest",
                    "dev_receipt",
                    "student_branch_runtime_image_reference",
                    "provider_binding_sha256",
                    "authority",
                    "claimed_at",
                    "expires_at",
                )
                or gateway_claim.get("schema_version")
                != "dragontales.student-winner-deployment-claim.v2"
                or gateway_claim.get("scope") != scope
                or gateway_claim.get("student_job_id") != student_job_id
                or gateway_claim.get("provider_binding_sha256")
                != winner_result.get("provider_binding_sha256")
                or gateway_claim.get("student_branch_runtime_image_reference")
                != admission.get("student_branch_runtime_image_reference")
                or not isinstance(authority, dict)
                or authority.get("max_wall_seconds")
                != acceptance.get("max_wall_seconds")
                or authority.get("max_cost_microusd")
                != acceptance.get("max_cost_microusd")
                or authority.get("authorization_not_after")
                != gateway_claim.get("expires_at")
            ):
                raise ValueError(
                    "provider teardown gateway claim identity differs"
                )
            canary = trigger.get("canary_route_receipt")
            zero = trigger.get("zero_route_receipt")
            if not isinstance(canary, dict) or not isinstance(zero, dict):
                raise ValueError("provider teardown route receipts are invalid")
            canary_route_raw = _gateway_bytes(canary) + b"\n"
            zero_route_raw = _gateway_bytes(zero) + b"\n"
            unused_canary, validated_zero = validate_route_retirement(
                canary_route_raw,
                zero_route_raw,
                gateway_claim,
            )
            del unused_canary
            if validated_zero["route_revision"] != trigger[
                "zero_route_revision"
            ]:
                raise ValueError("provider teardown zero route revision differs")
        elif isinstance(trigger, dict) and trigger.get("kind") == "service_expired":
            if (
                tuple(trigger) != ("kind", "service_not_after")
                or trigger.get("service_not_after")
                != admission.get("service_not_after")
            ):
                raise ValueError("provider teardown expiry trigger is invalid")
            unused_expiry, expiry_nanos = _gateway_time(
                trigger["service_not_after"],
                "provider service expiry",
            )
            del unused_expiry
            if authorized_nanos < expiry_nanos or current_nanos < expiry_nanos:
                raise ValueError("provider teardown service has not expired")
        else:
            raise ValueError("provider teardown trigger is invalid")
        records.append(
            {
                "frontier_object_key": key,
                "frontier_sha256": hashlib.sha256(frontier_raw).hexdigest(),
                "frontier": frontier,
                "authorization_object_key": authorization_key,
                "authorization_sha256": frontier[
                    "authorization_sha256"
                ],
                "authorization_raw": bytes(authorization_raw),
                "authorization": authorization,
                "winner_result_raw": bytes(winner_result_raw),
                "winner_result": winner_result,
                "gateway_claim_raw": (
                    None
                    if gateway_claim_raw is None
                    else bytes(gateway_claim_raw)
                ),
                "canary_route_raw": canary_route_raw,
                "zero_route_raw": zero_route_raw,
            }
        )
    identities = [
        item["authorization"]["student_job_id"]
        for item in records
    ]
    if identities != sorted(set(identities)):
        raise ValueError("provider teardown frontier contains duplicate students")
    return records


def _validate_launch_source(source):
    if (
        not isinstance(source, dict)
        or set(source)
        != {
            "schema_version",
            "scope",
            "dispatch_id",
            "frontier_object_key",
            "frontier_sha256",
            "outbox_object_key",
            "outbox_sha256",
            "claim_object_key",
            "claim_sha256",
            "created_at",
            "expires_at",
        }
        or source.get("schema_version") != "milk.gateway-launch-source.v1"
        or not _is_hex64(source.get("dispatch_id"))
        or source.get("dispatch_id") != source.get("claim_sha256")
        or any(
            not _is_hex64(source.get(field))
            for field in (
                "frontier_sha256",
                "outbox_sha256",
                "claim_sha256",
            )
        )
        or any(
            not isinstance(source.get(field), str) or not source[field]
            for field in (
                "frontier_object_key",
                "outbox_object_key",
                "claim_object_key",
            )
        )
    ):
        raise ValueError("gateway launch source is invalid")
    scope_prefix = _scope_prefix(source.get("scope"))
    unused_created, created_nanos = _gateway_time(
        source.get("created_at"),
        "gateway launch created_at",
    )
    del unused_created
    unused_expiry, expiry_nanos = _gateway_time(
        source.get("expires_at"),
        "gateway launch expiry",
    )
    del unused_expiry
    if (
        created_nanos >= expiry_nanos
        or source["frontier_object_key"]
        != f"{scope_prefix}/frontier/gpu-launch/"
        f"{_retained_time_key(expiry_nanos)}/{source['dispatch_id']}.json"
    ):
        raise ValueError("gateway launch source timestamps or frontier key are invalid")
    return scope_prefix


def _validate_workload(workload):
    if not isinstance(workload, dict):
        raise ValueError("workload identity is invalid")
    workload_type = workload.get("type")
    common = {
        "type",
        "launch_source",
        "image_admission_sha256",
        "image_release_sha256",
    }
    if workload_type == "teacher_run":
        expected = common | {
            "teacher_run_id",
            "provider_binding_sha256",
            "slot",
            "call_count",
            "max_gpu_seconds",
        }
        operation = {
            "kind": "teacher_run",
            "teacher_run_id": workload.get("teacher_run_id"),
            "provider_binding_sha256": workload.get("provider_binding_sha256"),
            "slot": workload.get("slot"),
            "call_count": workload.get("call_count"),
            "max_gpu_seconds": workload.get("max_gpu_seconds"),
        }
    elif workload_type == "student_train_merge":
        expected = common | {"student_job_id", "counts", "max_gpu_seconds"}
        operation = {
            "kind": "student_train_merge",
            "student_job_id": workload.get("student_job_id"),
            "counts": workload.get("counts"),
            "max_gpu_seconds": workload.get("max_gpu_seconds"),
        }
    elif workload_type == "student_fanout":
        expected = common | {
            "student_job_id",
            "max_total_gpu_seconds",
            "branches",
        }
        operation = {
            "kind": "student_fanout",
            "student_job_id": workload.get("student_job_id"),
            "max_total_gpu_seconds": workload.get("max_total_gpu_seconds"),
            "branches": workload.get("branches"),
        }
    else:
        raise ValueError("workload type is invalid")
    if (
        set(workload) != expected
        or not _is_hex64(workload.get("image_admission_sha256"))
        or not _is_hex64(workload.get("image_release_sha256"))
    ):
        raise ValueError("workload identity is invalid")
    _validate_gpu_operation(operation)
    source = workload.get("launch_source")
    scope_prefix = _validate_launch_source(source)
    claim_key, outbox_key = _gpu_launch_keys(scope_prefix, operation)
    if (
        source["claim_object_key"] != claim_key
        or source["outbox_object_key"] != outbox_key
    ):
        raise ValueError("workload differs from its gateway launch source")
    return source


def _workload_expiry(workload):
    source = _validate_workload(workload)
    return _gateway_time(
        source["expires_at"],
        "gateway launch expiry",
    )[0]


def provider_neutral_run_id(
    campaign_id,
    launch_source,
    operation,
    image_release_sha256,
    image_admission_sha256,
):
    if not _is_hex64(campaign_id):
        raise ValueError("campaign ID must be lowercase hex64")
    _validate_gpu_operation(operation)
    _validate_launch_source(launch_source)
    if not _is_hex64(image_release_sha256) or not _is_hex64(
        image_admission_sha256
    ):
        raise ValueError("provider-neutral image authority is invalid")
    identity = {
        "schema_version": "milk.provider-neutral-gpu-run.v1",
        "campaign_id": campaign_id,
        "claim_sha256": launch_source["claim_sha256"],
        "outbox_sha256": launch_source["outbox_sha256"],
        "created_at": launch_source["created_at"],
        "expires_at": launch_source["expires_at"],
        "operation": operation,
        "image_release_sha256": image_release_sha256,
        "image_admission_sha256": image_admission_sha256,
    }
    return hashlib.sha256(
        b"milk.provider-neutral-gpu-run.v1\0" + canonical_json(identity)
    ).hexdigest()


def _workload_run_id(campaign_id, workload):
    source = _validate_workload(workload)
    operation = _operation_from_workload(workload)
    return provider_neutral_run_id(
        campaign_id,
        source,
        operation,
        workload["image_release_sha256"],
        workload["image_admission_sha256"],
    )


def _baseten_training_preflight(value, team_name, project_id):
    if not isinstance(value, dict):
        raise ValueError("Baseten training preflight is invalid")
    common = {
        "schema_version",
        "provider",
        "team_name",
        "project_id",
        "outcome",
        "evidence_sha256",
        "observed_at",
    }
    outcome = value.get("outcome")
    expected = (
        common
        if outcome == "ready"
        else common | {"reason", "status"}
    )
    if (
        set(value) != expected
        or value.get("schema_version") != BASETEN_TRAINING_PREFLIGHT_SCHEMA
        or value.get("provider") != "baseten"
        or value.get("team_name") != team_name
        or value.get("project_id") != project_id
        or not isinstance(team_name, str)
        or TEAM_NAME.fullmatch(team_name) is None
        or not _is_hex64(value.get("evidence_sha256"))
    ):
        raise ValueError("Baseten training preflight is invalid")
    _utc(value.get("observed_at"), "Baseten preflight observed_at")
    if outcome == "ready":
        return value
    reason = value.get("reason")
    status = value.get("status")
    if (
        outcome != "retryable_unavailable"
        or reason == "timeout"
        and status is not None
        or reason == "rate_limited"
        and status != 429
        or reason == "server_unavailable"
        and (type(status) is not int or not 500 <= status <= 599)
        or reason == "capability_unavailable"
        and status is not None
        or reason
        not in {
            "capability_unavailable",
            "timeout",
            "rate_limited",
            "server_unavailable",
        }
    ):
        raise ValueError("Baseten preflight is not safe for Modal fallback")
    return value


def _modal_preflight(value, provider_identity):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "provider",
            "provider_identity",
            "outcome",
            "evidence_sha256",
            "observed_at",
        }
        or value.get("schema_version") != MODAL_PREFLIGHT_SCHEMA
        or value.get("provider") != "modal"
        or value.get("provider_identity") != provider_identity
        or value.get("outcome") != "ready"
        or not _is_hex64(value.get("evidence_sha256"))
        or not isinstance(provider_identity, dict)
        or tuple(provider_identity)
        != (
            "provider",
            "workspace_id",
            "workspace_name",
            "environment_id",
            "environment_name",
            "app_id",
            "app_name",
        )
        or provider_identity.get("provider") != "modal"
        or any(
            not isinstance(provider_identity.get(field), str)
            or OPAQUE.fullmatch(provider_identity[field]) is None
            for field in tuple(provider_identity)[1:]
        )
    ):
        raise ValueError("Modal preflight is invalid")
    _utc(value.get("observed_at"), "Modal preflight observed_at")
    return value


def _selection_key(campaign_id, run_id):
    return f"campaigns/v1/{campaign_id}/provider-selections/{run_id}.json"


def _preflight_key(campaign_id, run_id, receipt):
    return (
        f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
        f"preflights/{_digest(receipt)}.json"
    )


def _ordered_provider_selection(
    selection,
    baseten_team_name,
    modal_identity,
):
    """Validate stored selection semantics and restore acceptance wire order."""
    if not isinstance(selection, dict):
        raise ValueError("provider selection is invalid")
    selected = selection.get("selected_provider")
    primary = selection.get("primary_preflight")
    if selected == "baseten":
        identity = selection.get("provider_identity")
        if (
            set(selection)
            != {"selected_provider", "provider_identity", "primary_preflight"}
            or identity
            != {"provider": "baseten", "team_name": baseten_team_name}
            or not isinstance(primary, dict)
            or set(primary)
            != {"provider", "outcome", "evidence_sha256", "observed_at"}
            or primary.get("provider") != "baseten"
            or primary.get("outcome") != "ready"
            or not _is_hex64(primary.get("evidence_sha256"))
        ):
            raise ValueError("Baseten provider selection is invalid")
        _utc(primary.get("observed_at"), "Baseten preflight observed_at")
        return {
            "selected_provider": "baseten",
            "provider_identity": {
                "provider": "baseten",
                "team_name": baseten_team_name,
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": primary["evidence_sha256"],
                "observed_at": primary["observed_at"],
            },
        }
    if selected != "modal":
        raise ValueError("selected provider is invalid")
    identity = selection.get("provider_identity")
    fallback = selection.get("fallback_preflight")
    if (
        set(selection)
        != {
            "selected_provider",
            "provider_identity",
            "primary_preflight",
            "fallback_preflight",
        }
        or not isinstance(modal_identity, dict)
        or set(modal_identity)
        != {
            "provider",
            "workspace_id",
            "workspace_name",
            "environment_id",
            "environment_name",
            "app_id",
            "app_name",
        }
        or identity != modal_identity
        or modal_identity.get("provider") != "modal"
        or any(
            not isinstance(modal_identity.get(field), str)
            or OPAQUE.fullmatch(modal_identity[field]) is None
            for field in (
                "workspace_id",
                "workspace_name",
                "environment_id",
                "environment_name",
                "app_id",
                "app_name",
            )
        )
        or not isinstance(primary, dict)
        or set(primary)
        != {
            "provider",
            "outcome",
            "reason",
            "status",
            "evidence_sha256",
            "observed_at",
        }
        or primary.get("provider") != "baseten"
        or primary.get("outcome") != "retryable_unavailable"
        or not _is_hex64(primary.get("evidence_sha256"))
        or not isinstance(fallback, dict)
        or set(fallback)
        != {"provider", "outcome", "evidence_sha256", "observed_at"}
        or fallback.get("provider") != "modal"
        or fallback.get("outcome") != "ready"
        or not _is_hex64(fallback.get("evidence_sha256"))
    ):
        raise ValueError("Modal fallback provider selection is invalid")
    reason = primary["reason"]
    status = primary["status"]
    if (
        reason == "capability_unavailable"
        and status is not None
        or reason == "timeout"
        and status is not None
        or reason == "rate_limited"
        and status != 429
        or reason == "server_unavailable"
        and (type(status) is not int or not 500 <= status <= 599)
        or reason
        not in {
            "capability_unavailable",
            "timeout",
            "rate_limited",
            "server_unavailable",
        }
    ):
        raise ValueError("Baseten preflight is not safe for Modal fallback")
    primary_at = _utc(primary.get("observed_at"), "Baseten preflight observed_at")
    fallback_at = _utc(fallback.get("observed_at"), "Modal preflight observed_at")
    if fallback_at < primary_at:
        raise ValueError("Modal preflight precedes Baseten unavailability")
    ordered_identity = {
        field: modal_identity[field]
        for field in (
            "provider",
            "workspace_id",
            "workspace_name",
            "environment_id",
            "environment_name",
            "app_id",
            "app_name",
        )
    }
    return {
        "selected_provider": "modal",
        "provider_identity": ordered_identity,
        "primary_preflight": {
            "provider": "baseten",
            "outcome": "retryable_unavailable",
            "reason": reason,
            "status": status,
            "evidence_sha256": primary["evidence_sha256"],
            "observed_at": primary["observed_at"],
        },
        "fallback_preflight": {
            "provider": "modal",
            "outcome": "ready",
            "evidence_sha256": fallback["evidence_sha256"],
            "observed_at": fallback["observed_at"],
        },
    }


def _load_selection(
    store,
    campaign_id,
    run_id,
    launch_source,
    baseten_team_name,
    baseten_project_id,
    modal_identity,
):
    value = _strict_object(store.get(_selection_key(campaign_id, run_id)))
    if (
        canonical_json(value) != store.get(_selection_key(campaign_id, run_id))
        or set(value)
        != {
            "schema_version",
            "campaign_id",
            "run_id",
            "claim_sha256",
            "outbox_sha256",
            "selection",
            "state",
        }
        or value.get("schema_version") != "milk.provider-selection.v1"
        or value.get("campaign_id") != campaign_id
        or value.get("run_id") != run_id
        or value.get("claim_sha256") != launch_source["claim_sha256"]
        or value.get("outbox_sha256") != launch_source["outbox_sha256"]
        or value.get("state") != "selected"
    ):
        raise ValueError("stored provider selection is invalid")
    selection = _ordered_provider_selection(
        value.get("selection"),
        baseten_team_name,
        modal_identity,
    )
    value["selection"] = selection
    selected = selection["selected_provider"]
    primary = selection["primary_preflight"]
    if selected == "baseten":
        if (
            selection.get("provider_identity")
            != {"provider": "baseten", "team_name": baseten_team_name}
        ):
            raise ValueError("stored Baseten selection is invalid")
        receipt = _strict_object(
            store.get(
                f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
                f"preflights/{primary.get('evidence_sha256')}.json"
            )
        )
        _baseten_training_preflight(receipt, baseten_team_name, baseten_project_id)
    elif selected == "modal":
        fallback = selection.get("fallback_preflight")
        if (
            selection.get("provider_identity") != modal_identity
        ):
            raise ValueError("stored Modal fallback selection is invalid")
        primary_receipt = _strict_object(
            store.get(
                f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
                f"preflights/{primary.get('evidence_sha256')}.json"
            )
        )
        fallback_receipt = _strict_object(
            store.get(
                f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
                f"preflights/{fallback.get('evidence_sha256')}.json"
            )
        )
        _baseten_training_preflight(
            primary_receipt,
            baseten_team_name,
            baseten_project_id,
        )
        _modal_preflight(fallback_receipt, modal_identity)
    else:
        raise ValueError("stored selected provider is invalid")
    for item, receipt in (
        (primary, receipt if selected == "baseten" else primary_receipt),
        (
            selection.get("fallback_preflight"),
            None if selected == "baseten" else fallback_receipt,
        ),
    ):
        if item is None:
            continue
        if (
            item.get("evidence_sha256") != _digest(receipt)
            or item.get("observed_at") != receipt["observed_at"]
        ):
            raise ValueError("stored provider selection evidence differs")
    return value


def _baseten_create_is_frozen(store, campaign_id, run_id):
    keys = (
        f"campaigns/v1/{campaign_id}/reservation-intents/{run_id}.json",
        f"campaigns/v1/{campaign_id}/jobs/{run_id}/provider-acceptance.json",
        f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}/provider-acceptance.json",
        f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}/create-intent.json",
    )
    for key in keys:
        try:
            store.get(key)
        except FileNotFoundError:
            continue
        return True
    return bool(
        store.list(
            f"campaigns/v1/{campaign_id}/jobs/{run_id}/create-intents",
            limit=1,
        )
    )


def select_baseten_primary_modal_fallback(
    *,
    store,
    campaign_id,
    run_id,
    launch_source,
    baseten_team_name,
    baseten_project_id,
    modal_identity,
    baseten_preflight,
    modal_preflight,
):
    if not callable(baseten_preflight) or not callable(modal_preflight):
        raise ValueError("fixed provider preflight callbacks are required")
    if not _is_hex64(campaign_id) or not _is_hex64(run_id):
        raise ValueError("fixed provider selection identity is invalid")
    _validate_launch_source(launch_source)
    try:
        return _load_selection(
            store,
            campaign_id,
            run_id,
            launch_source,
            baseten_team_name,
            baseten_project_id,
            modal_identity,
        )
    except FileNotFoundError:
        pass
    primary = _baseten_training_preflight(
        baseten_preflight(),
        baseten_team_name,
        baseten_project_id,
    )
    primary_digest = _digest(primary)
    create_same(
        store,
        _preflight_key(campaign_id, run_id, primary),
        canonical_json(primary),
        "application/json",
    )
    if primary["outcome"] == "ready":
        selection = {
            "selected_provider": "baseten",
            "provider_identity": {
                "provider": "baseten",
                "team_name": baseten_team_name,
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": primary_digest,
                "observed_at": primary["observed_at"],
            },
        }
    else:
        if _baseten_create_is_frozen(store, campaign_id, run_id):
            raise RuntimeError("Modal fallback is forbidden after Baseten create authority")
        fallback = _modal_preflight(modal_preflight(), modal_identity)
        if _utc(fallback["observed_at"], "Modal preflight observed_at") < _utc(
            primary["observed_at"], "Baseten preflight observed_at"
        ):
            raise ValueError("Modal preflight precedes Baseten unavailability")
        fallback_digest = _digest(fallback)
        create_same(
            store,
            _preflight_key(campaign_id, run_id, fallback),
            canonical_json(fallback),
            "application/json",
        )
        selection = {
            "selected_provider": "modal",
            "provider_identity": copy.deepcopy(modal_identity),
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "retryable_unavailable",
                "reason": primary["reason"],
                "status": primary["status"],
                "evidence_sha256": primary_digest,
                "observed_at": primary["observed_at"],
            },
            "fallback_preflight": {
                "provider": "modal",
                "outcome": "ready",
                "evidence_sha256": fallback_digest,
                "observed_at": fallback["observed_at"],
            },
        }
    value = {
        "schema_version": "milk.provider-selection.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "claim_sha256": launch_source["claim_sha256"],
        "outbox_sha256": launch_source["outbox_sha256"],
        "selection": selection,
        "state": "selected",
    }
    key = _selection_key(campaign_id, run_id)
    raw = canonical_json(value)
    if not store.create(key, raw, "application/json"):
        existing = _load_selection(
            store,
            campaign_id,
            run_id,
            launch_source,
            baseten_team_name,
            baseten_project_id,
            modal_identity,
        )
        if existing != value:
            raise RuntimeError("provider selection is already immutable")
        return existing
    return value


def _baseten_winner_preflight(value, team_name):
    if not isinstance(value, dict) or value.get("outcome") not in {
        "ready",
        "retryable_unavailable",
    }:
        raise ValueError("Baseten winner preflight is invalid")
    if value["outcome"] == "ready":
        expected = baseten_winner.baseten_preflight_receipt(
            team_name,
            value.get("team_id"),
            value.get("management_key_prefix_sha256"),
            value.get("teams_response_sha256"),
            value.get("api_keys_response_sha256"),
            _utc(value.get("observed_at"), "Baseten winner preflight time"),
        )
    else:
        expected = baseten_winner.baseten_unavailable_preflight_receipt(
            team_name,
            value.get("reason"),
            value.get("status"),
            _utc(value.get("observed_at"), "Baseten winner preflight time"),
        )
    if value != expected:
        raise ValueError("Baseten winner preflight is invalid")
    return value


def _load_winner_selection(
    store,
    campaign_id,
    run_id,
    launch_source,
    baseten_team_name,
    modal_identity,
):
    raw = store.get(_selection_key(campaign_id, run_id))
    value = _strict_object(raw)
    if (
        raw != canonical_json(value)
        or set(value)
        != {
            "schema_version",
            "campaign_id",
            "run_id",
            "claim_sha256",
            "outbox_sha256",
            "selection",
            "state",
        }
        or value.get("schema_version") != "milk.provider-selection.v1"
        or value.get("campaign_id") != campaign_id
        or value.get("run_id") != run_id
        or value.get("claim_sha256") != launch_source["claim_sha256"]
        or value.get("outbox_sha256") != launch_source["outbox_sha256"]
        or value.get("state") != "selected"
    ):
        raise ValueError("stored winner provider selection is invalid")
    selection = _ordered_provider_selection(
        value.get("selection"),
        baseten_team_name,
        modal_identity,
    )
    value["selection"] = selection
    selected = selection["selected_provider"]
    primary = selection["primary_preflight"]
    if selected == "baseten":
        if (
            selection.get("provider_identity")
            != {"provider": "baseten", "team_name": baseten_team_name}
            or not isinstance(primary, dict)
            or primary.get("outcome") != "ready"
            or selection.get("fallback_preflight") is not None
        ):
            raise ValueError("stored Baseten winner selection is invalid")
        receipts = [primary]
    elif selected == "modal":
        fallback = selection.get("fallback_preflight")
        if (
            selection.get("provider_identity") != modal_identity
            or not isinstance(primary, dict)
            or primary.get("outcome") != "retryable_unavailable"
            or not isinstance(fallback, dict)
            or fallback.get("outcome") != "ready"
        ):
            raise ValueError("stored Modal winner selection is invalid")
        receipts = [primary, fallback]
    else:
        raise ValueError("stored winner selected provider is invalid")
    full = []
    for receipt in receipts:
        evidence_sha256 = receipt.get("evidence_sha256")
        if not _is_hex64(evidence_sha256):
            raise ValueError("stored winner preflight reference is invalid")
        evidence = _strict_object(
            store.get(
                f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
                f"preflights/{evidence_sha256}.json"
            )
        )
        if (
            _digest(evidence) != evidence_sha256
            or evidence.get("observed_at") != receipt.get("observed_at")
        ):
            raise ValueError("stored winner preflight evidence differs")
        full.append(evidence)
    _baseten_winner_preflight(full[0], baseten_team_name)
    if selected == "modal":
        _modal_preflight(full[1], modal_identity)
    return value


def select_winner_baseten_primary_modal_fallback(
    *,
    store,
    campaign_id,
    run_id,
    launch_source,
    baseten_team_name,
    modal_identity,
    baseten_preflight,
    modal_preflight,
):
    if (
        not _is_hex64(campaign_id)
        or not _is_hex64(run_id)
        or not callable(baseten_preflight)
        or not callable(modal_preflight)
    ):
        raise ValueError("fixed winner provider selection is invalid")
    _validate_launch_source(launch_source)
    try:
        return _load_winner_selection(
            store,
            campaign_id,
            run_id,
            launch_source,
            baseten_team_name,
            modal_identity,
        )
    except FileNotFoundError:
        pass
    primary = _baseten_winner_preflight(
        baseten_preflight(),
        baseten_team_name,
    )
    primary_sha256 = _digest(primary)
    create_same(
        store,
        _preflight_key(campaign_id, run_id, primary),
        canonical_json(primary),
        "application/json",
    )
    if primary["outcome"] == "ready":
        selection = {
            "selected_provider": "baseten",
            "provider_identity": {
                "provider": "baseten",
                "team_name": baseten_team_name,
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": primary_sha256,
                "observed_at": primary["observed_at"],
            },
        }
    else:
        if _baseten_create_is_frozen(store, campaign_id, run_id):
            raise RuntimeError(
                "Modal winner fallback is forbidden after Baseten create authority"
            )
        fallback = _modal_preflight(modal_preflight(), modal_identity)
        if _utc(fallback["observed_at"], "Modal winner preflight time") < _utc(
            primary["observed_at"],
            "Baseten winner preflight time",
        ):
            raise ValueError("Modal winner preflight precedes Baseten unavailability")
        fallback_sha256 = _digest(fallback)
        create_same(
            store,
            _preflight_key(campaign_id, run_id, fallback),
            canonical_json(fallback),
            "application/json",
        )
        selection = {
            "selected_provider": "modal",
            "provider_identity": copy.deepcopy(modal_identity),
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "retryable_unavailable",
                "reason": primary["reason"],
                "status": primary["status"],
                "evidence_sha256": primary_sha256,
                "observed_at": primary["observed_at"],
            },
            "fallback_preflight": {
                "provider": "modal",
                "outcome": "ready",
                "evidence_sha256": fallback_sha256,
                "observed_at": fallback["observed_at"],
            },
        }
    value = {
        "schema_version": "milk.provider-selection.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "claim_sha256": launch_source["claim_sha256"],
        "outbox_sha256": launch_source["outbox_sha256"],
        "selection": selection,
        "state": "selected",
    }
    key = _selection_key(campaign_id, run_id)
    if not store.create(key, canonical_json(value), "application/json"):
        existing = _load_winner_selection(
            store,
            campaign_id,
            run_id,
            launch_source,
            baseten_team_name,
            modal_identity,
        )
        if existing != value:
            raise RuntimeError("winner provider selection is already immutable")
        return existing
    return value


def _creation_authorities(
    provider_pass_claim_raw,
    create_authorization_raw,
    campaign_id,
    scope_prefix,
):
    if not isinstance(provider_pass_claim_raw, (bytes, bytearray)) or not isinstance(
        create_authorization_raw, (bytes, bytearray)
    ):
        raise ValueError("provider create authority must be exact raw bytes")
    provider_pass_claim_raw = bytes(provider_pass_claim_raw)
    create_authorization_raw = bytes(create_authorization_raw)
    provider_pass = _strict_object(provider_pass_claim_raw)
    authorization = _strict_object(create_authorization_raw)
    if (
        canonical_json(provider_pass) != provider_pass_claim_raw
        or canonical_json(authorization) != create_authorization_raw
        or set(provider_pass)
        != {
            "schema_version",
            "campaign_id",
            "scope_prefix",
            "owner_id",
            "holder_id",
            "lease_revision",
            "lease_expires_at",
            "lease_token_sha256",
            "create_authorization_sha256",
            "create_authority_store_location_sha256",
            "provider_creates_authorized",
            "evidence_store_identity_sha256",
            "claimed_at",
        }
        or provider_pass.get("schema_version")
        != "milk.provider-jobs-pass-claim.v1"
        or provider_pass.get("campaign_id") != campaign_id
        or provider_pass.get("scope_prefix") != scope_prefix
        or provider_pass.get("provider_creates_authorized") is not True
        or set(authorization)
        != {
            "schema_version",
            "campaign_id",
            "scope_prefix",
            "owner_id",
            "holder_id",
            "lease_revision",
            "lease_acquired_at",
            "lease_expires_at",
            "evidence_store_identity_sha256",
            "create_authority_store_location_sha256",
            "requested",
            "confirmation_valid",
            "gateway_gate_granted",
            "configuration_verified",
            "granted",
            "confirmation_sha256",
        }
        or authorization.get("schema_version")
        != "milk.provider-create-authorization.v1"
        or authorization.get("campaign_id") != campaign_id
        or authorization.get("scope_prefix") != scope_prefix
        or any(
            provider_pass.get(field) != authorization.get(field)
            for field in (
                "owner_id",
                "holder_id",
                "lease_revision",
                "lease_expires_at",
                "evidence_store_identity_sha256",
                "create_authority_store_location_sha256",
            )
        )
        or any(
            authorization.get(field) is not True
            for field in (
                "requested",
                "confirmation_valid",
                "gateway_gate_granted",
                "configuration_verified",
                "granted",
            )
        )
        or not _is_hex64(authorization.get("confirmation_sha256"))
        or provider_pass.get("create_authorization_sha256")
        != hashlib.sha256(create_authorization_raw).hexdigest()
        or any(
            not _is_hex64(provider_pass.get(field))
            for field in (
                "owner_id",
                "holder_id",
                "lease_token_sha256",
                "create_authorization_sha256",
                "create_authority_store_location_sha256",
                "evidence_store_identity_sha256",
            )
        )
        or type(provider_pass.get("lease_revision")) is not int
        or provider_pass["lease_revision"] < 1
    ):
        raise ValueError("provider create authority is invalid")
    acquired = _utc(
        authorization.get("lease_acquired_at"),
        "provider lease acquired_at",
    )
    claimed = _utc(provider_pass.get("claimed_at"), "provider pass claimed_at")
    expires = _utc(
        provider_pass.get("lease_expires_at"),
        "provider lease expires_at",
    )
    if not acquired <= claimed < expires:
        raise ValueError("provider create authority interval is invalid")
    return provider_pass, authorization


def _operation_from_workload(workload):
    _validate_workload(workload)
    kind = workload["type"]
    fields = {
        "teacher_run": (
            "teacher_run_id",
            "provider_binding_sha256",
            "slot",
            "call_count",
            "max_gpu_seconds",
        ),
        "student_train_merge": (
            "student_job_id",
            "counts",
            "max_gpu_seconds",
        ),
        "student_fanout": (
            "student_job_id",
            "max_total_gpu_seconds",
            "branches",
        ),
    }[kind]
    operation = {"kind": kind}
    operation.update((field, copy.deepcopy(workload[field])) for field in fields)
    _validate_gpu_operation(operation)
    return operation


def gpu_provider_acceptance(
    *,
    campaign_id,
    workload,
    selection_record,
    reservation,
    provider_pass_claim_raw,
    create_authorization_raw,
    expected_budget_provider_identity,
    reserved_at,
    accepted_at,
):
    source = _validate_workload(workload)
    operation = _operation_from_workload(workload)
    run_id = _workload_run_id(campaign_id, workload)
    scope_prefix = _scope_prefix(source["scope"])
    provider_pass, unused_authorization = _creation_authorities(
        provider_pass_claim_raw,
        create_authorization_raw,
        campaign_id,
        scope_prefix,
    )
    del unused_authorization
    if (
        not isinstance(selection_record, dict)
        or selection_record.get("run_id") != run_id
        or selection_record.get("claim_sha256") != source["claim_sha256"]
        or selection_record.get("outbox_sha256") != source["outbox_sha256"]
        or selection_record.get("state") != "selected"
        or not isinstance(reservation, dict)
        or reservation.get("schema_version")
        != "milk.campaign-budget-reservation.v4"
        or set(reservation)
        != {
            "schema_version",
            "campaign_id",
            "run_id",
            "amount_microusd",
            "preparation_sha256",
            "provider_deadline_epoch_seconds",
            "intent_sha256",
            "state",
            "provider_role",
            "provider_identity",
            "pricing_receipt_sha256",
            "provider_rate_microusd_per_minute",
            "reservation_rate_microusd_per_minute",
        }
        or reservation.get("campaign_id") != campaign_id
        or reservation.get("run_id") != run_id
        or reservation.get("provider_identity")
        != expected_budget_provider_identity
        or reservation.get("provider_role")
        != (
            "training"
            if expected_budget_provider_identity.get("provider") == "baseten"
            and "project_id" in expected_budget_provider_identity
            else "serving"
            if expected_budget_provider_identity.get("provider") == "baseten"
            else "sandbox"
        )
        or reservation.get("state") != "reserved"
        or type(reservation.get("amount_microusd")) is not int
        or reservation["amount_microusd"] <= 0
        or not _is_hex64(reservation.get("preparation_sha256"))
        or not _is_hex64(reservation.get("intent_sha256"))
        or type(reservation.get("provider_deadline_epoch_seconds")) is not int
    ):
        raise ValueError("GPU provider reservation authority is invalid")
    selection = copy.deepcopy(selection_record["selection"])
    selected = selection.get("selected_provider") if isinstance(selection, dict) else None
    if (
        selected == "baseten"
        and expected_budget_provider_identity.get("provider") != "baseten"
        or selected == "modal"
        and expected_budget_provider_identity.get("provider") != "modal"
        or selected not in {"baseten", "modal"}
    ):
        raise ValueError("selected provider differs from its budget authority")
    reserved_at = _utc_text(reserved_at)
    accepted_at = _utc_text(accepted_at)
    reserved_time = _utc(reserved_at, "budget reserved_at")
    accepted_time = _utc(accepted_at, "provider accepted_at")
    create_time = accepted_time + dt.timedelta(seconds=60)
    provider_time = dt.datetime.fromtimestamp(
        reservation["provider_deadline_epoch_seconds"],
        tz=dt.timezone.utc,
    )
    claim_time = _gateway_time(source["expires_at"], "gateway launch expiry")[0]
    lease_time = _utc(
        provider_pass["lease_expires_at"],
        "provider lease expires_at",
    )
    observed_time = _utc(
        (
            selection["primary_preflight"]["observed_at"]
            if selected == "baseten"
            else selection["fallback_preflight"]["observed_at"]
        ),
        "selected provider preflight observed_at",
    )
    wall_seconds = (
        max(branch["max_gpu_seconds"] for branch in operation["branches"])
        if operation["kind"] == "student_fanout"
        else operation["max_gpu_seconds"]
    )
    if not (
        _utc(provider_pass["claimed_at"], "provider pass claimed_at")
        <= reserved_time
        and observed_time <= reserved_time <= accepted_time < create_time
        <= provider_time <= claim_time
        and create_time <= lease_time
        and provider_time - accepted_time <= dt.timedelta(seconds=wall_seconds)
    ):
        raise ValueError("GPU provider acceptance interval is invalid")
    amount = reservation["amount_microusd"]
    value = {
        "schema_version": "milk.gpu-provider-acceptance.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "claim_sha256": source["claim_sha256"],
        "outbox_sha256": source["outbox_sha256"],
        "operation": operation,
        "selection": selection,
        "image_release_sha256": workload["image_release_sha256"],
        "image_admission_sha256": workload["image_admission_sha256"],
        "provider_pass_claim_sha256": hashlib.sha256(
            provider_pass_claim_raw
        ).hexdigest(),
        "create_authorization_sha256": hashlib.sha256(
            create_authorization_raw
        ).hexdigest(),
        "budget_reservation_sha256": _digest(reservation),
        "reserved_microusd": amount,
        "reserved_at": reserved_at,
        "accepted_at": accepted_at,
        "create_not_after": _utc_text(create_time),
        "provider_not_after": _utc_text(provider_time),
        "max_wall_seconds": wall_seconds,
        "max_cost_microusd": amount,
        "state": "accepted",
    }
    encode_provider_acceptance(value)
    return value


def modal_winner_provider_acceptance(
    *,
    campaign_id,
    launch,
    run_id,
    selection_record,
    reservation,
    image_release_sha256,
    image_admission_sha256,
    provider_pass_claim_raw,
    create_authorization_raw,
    reserved_at,
    accepted_at,
):
    expected_run_id = _winner_run_from_launch(
        campaign_id,
        launch,
        image_release_sha256,
        image_admission_sha256,
    )
    if run_id != expected_run_id:
        raise ValueError("Modal winner run ID is not provider-neutral")
    source = launch["launch_source"]
    operation = launch["operation"]
    provider_pass, unused_authorization = _creation_authorities(
        provider_pass_claim_raw,
        create_authorization_raw,
        campaign_id,
        _scope_prefix(source["scope"]),
    )
    del unused_authorization
    selection = (
        selection_record.get("selection")
        if isinstance(selection_record, dict)
        else None
    )
    identity = (
        selection.get("provider_identity")
        if isinstance(selection, dict)
        else None
    )
    budget_identity = _modal_budget_identity(identity)
    amount = (
        _minutes(operation["max_wall_seconds"])
        * RESERVATION_RATE_MICROUSD_PER_MINUTE
    )
    if (
        not isinstance(selection_record, dict)
        or selection_record.get("run_id") != run_id
        or selection_record.get("claim_sha256") != source["claim_sha256"]
        or selection_record.get("outbox_sha256") != source["outbox_sha256"]
        or not isinstance(selection, dict)
        or selection.get("selected_provider") != "modal"
        or not isinstance(reservation, dict)
        or set(reservation)
        != {
            "schema_version",
            "campaign_id",
            "run_id",
            "amount_microusd",
            "preparation_sha256",
            "provider_deadline_epoch_seconds",
            "intent_sha256",
            "state",
            "provider_role",
            "provider_identity",
            "pricing_receipt_sha256",
            "provider_rate_microusd_per_minute",
            "reservation_rate_microusd_per_minute",
        }
        or reservation.get("schema_version")
        != "milk.campaign-budget-reservation.v4"
        or reservation.get("campaign_id") != campaign_id
        or reservation.get("run_id") != run_id
        or reservation.get("amount_microusd") != amount
        or reservation.get("provider_role") != "sandbox"
        or reservation.get("provider_identity") != budget_identity
        or reservation.get("state") != "reserved"
        or not _is_hex64(reservation.get("preparation_sha256"))
        or not _is_hex64(reservation.get("intent_sha256"))
        or type(reservation.get("provider_deadline_epoch_seconds")) is not int
        or amount > operation["max_cost_microusd"]
    ):
        raise ValueError("Modal winner reservation authority is invalid")
    reserved_at = _utc_text(reserved_at)
    accepted_at = _utc_text(accepted_at)
    reserved_time = _utc(reserved_at, "winner budget reserved_at")
    accepted_time = _utc(accepted_at, "winner provider accepted_at")
    create_time = accepted_time + dt.timedelta(seconds=60)
    provider_time = dt.datetime.fromtimestamp(
        reservation["provider_deadline_epoch_seconds"],
        tz=dt.timezone.utc,
    )
    fallback_time = _utc(
        selection["fallback_preflight"]["observed_at"],
        "Modal winner preflight time",
    )
    lease_time = _utc(
        provider_pass["lease_expires_at"],
        "provider lease expires_at",
    )
    claim_time = _gateway_time(source["expires_at"], "winner expiry")[0]
    if not (
        _utc(provider_pass["claimed_at"], "provider pass claimed_at")
        <= reserved_time
        and fallback_time <= reserved_time <= accepted_time < create_time
        <= provider_time <= claim_time
        and create_time <= lease_time
        and provider_time - accepted_time
        <= dt.timedelta(seconds=operation["max_wall_seconds"])
    ):
        raise ValueError("Modal winner acceptance interval is invalid")
    value = {
        "schema_version": "milk.winner-provider-acceptance.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "claim_sha256": source["claim_sha256"],
        "outbox_sha256": source["outbox_sha256"],
        "provider_binding_sha256": operation["provider_binding_sha256"],
        "selection": copy.deepcopy(selection),
        "image_release_sha256": image_release_sha256,
        "image_admission_sha256": image_admission_sha256,
        "provider_pass_claim_sha256": hashlib.sha256(
            provider_pass_claim_raw
        ).hexdigest(),
        "create_authorization_sha256": hashlib.sha256(
            create_authorization_raw
        ).hexdigest(),
        "budget_reservation_sha256": _digest(reservation),
        "reserved_microusd": amount,
        "reserved_at": reserved_at,
        "accepted_at": accepted_at,
        "create_not_after": _utc_text(create_time),
        "provider_not_after": _utc_text(provider_time),
        "max_wall_seconds": operation["max_wall_seconds"],
        "max_cost_microusd": operation["max_cost_microusd"],
        "state": "accepted",
    }
    encode_provider_acceptance(value)
    return value


def _modal_units(workload):
    operation = _operation_from_workload(workload)
    if operation["kind"] != "student_fanout":
        return [operation]
    return [
        {
            "kind": "student_branch",
            "student_job_id": operation["student_job_id"],
            "branch_id": branch["branch_id"],
            "variant": branch["variant"],
            "max_gpu_seconds": branch["max_gpu_seconds"],
        }
        for branch in operation["branches"]
    ]


def _winner_run_from_launch(
    campaign_id,
    launch,
    image_release_sha256,
    image_admission_sha256,
):
    if (
        not isinstance(launch, dict)
        or set(launch)
        != {
            "launch_source",
            "runtime_image_reference",
            "operation",
            "gateway_launch_raw",
            "gateway_claim_raw",
        }
    ):
        raise ValueError("verified winner GPU launch is invalid")
    source = launch["launch_source"]
    _validate_launch_source(source)
    operation = launch["operation"]
    _validate_gpu_operation(operation)
    if operation["kind"] != "student_winner_deployment":
        raise ValueError("verified winner GPU launch is invalid")
    return provider_neutral_winner_run_id(
        campaign_id=campaign_id,
        claim_sha256=source["claim_sha256"],
        outbox_sha256=source["outbox_sha256"],
        student_job_id=operation["student_job_id"],
        student_result_sha256=operation["student_result_sha256"],
        winner=operation["winner"],
        max_wall_seconds=operation["max_wall_seconds"],
        max_cost_microusd=operation["max_cost_microusd"],
        image_release_sha256=image_release_sha256,
        image_admission_sha256=image_admission_sha256,
    )


def launch_baseten_winner(
    lifecycle,
    *,
    store,
    campaign_id,
    launch,
    selection_record,
    image_release_sha256,
    image_admission_sha256,
    provider_pass_claim_raw,
    create_authorization_raw,
    values,
    observed_cost_microusd=0,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    if not callable(now) or not hasattr(lifecycle, "prepare") or not hasattr(
        lifecycle, "admit"
    ):
        raise ValueError("Baseten winner lifecycle and clock are required")
    run_id = _winner_run_from_launch(
        campaign_id,
        launch,
        image_release_sha256,
        image_admission_sha256,
    )
    source = launch["launch_source"]
    selection = (
        selection_record.get("selection")
        if isinstance(selection_record, dict)
        else None
    )
    identity = (
        selection.get("provider_identity")
        if isinstance(selection, dict)
        else None
    )
    team_name = identity.get("team_name") if isinstance(identity, dict) else None
    if (
        not isinstance(selection_record, dict)
        or selection_record.get("run_id") != run_id
        or selection_record.get("claim_sha256") != source["claim_sha256"]
        or selection_record.get("outbox_sha256") != source["outbox_sha256"]
        or not isinstance(selection, dict)
        or selection.get("selected_provider") != "baseten"
        or identity != {"provider": "baseten", "team_name": team_name}
        or not isinstance(team_name, str)
        or TEAM_NAME.fullmatch(team_name) is None
        or getattr(lifecycle, "campaign_id", campaign_id) != campaign_id
        or getattr(lifecycle, "team_name", team_name) != team_name
        or store.get(_selection_key(campaign_id, run_id))
        != canonical_json(selection_record)
    ):
        raise ValueError("Baseten winner launch requires its immutable selection")
    primary = selection["primary_preflight"]
    preflight = _strict_object(
        store.get(
            f"campaigns/v1/{campaign_id}/provider-selections/{run_id}/"
            f"preflights/{primary['evidence_sha256']}.json"
        )
    )
    _baseten_winner_preflight(preflight, team_name)
    if preflight["outcome"] != "ready" or _digest(preflight) != primary[
        "evidence_sha256"
    ]:
        raise ValueError("Baseten winner ready preflight differs")
    try:
        recovered_reference = store_gateway_winner_result_handoff(
            store,
            campaign_id=campaign_id,
            run_id=run_id,
        )
    except FileNotFoundError:
        pass
    else:
        delivery_reference = store_candidate_key_delivery_handoff(
            store,
            campaign_id=campaign_id,
            run_id=run_id,
        )
        return {
            "schema_version": "milk.baseten-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "admitted",
            "winner_result_references": [recovered_reference],
            "candidate_key_delivery_references": [delivery_reference],
        }
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}"
    budget = CampaignBudget(
        store,
        campaign_id,
        baseten_serving_provider_identity(team_name),
        now=now,
    )
    budget.initialize()
    try:
        existing_preparation_raw = store.get(
            f"{prefix}/budget-preparation.json"
        )
    except FileNotFoundError:
        existing_preparation = None
    else:
        existing_preparation = _strict_object(existing_preparation_raw)
        if (
            canonical_json(existing_preparation) != existing_preparation_raw
            or existing_preparation.get("schema_version")
            != "milk.baseten-winner-budget-preparation.v1"
            or existing_preparation.get("campaign_id") != campaign_id
            or existing_preparation.get("run_id") != run_id
            or existing_preparation.get("selection_sha256")
            != _digest(selection_record)
            or existing_preparation.get("claim_sha256")
            != source["claim_sha256"]
            or existing_preparation.get("outbox_sha256")
            != source["outbox_sha256"]
            or existing_preparation.get("image_release_sha256")
            != image_release_sha256
            or existing_preparation.get("image_admission_sha256")
            != image_admission_sha256
            or existing_preparation.get("values_sha256") != _digest(values)
            or existing_preparation.get("state") != "preparing"
        ):
            raise ValueError("stored Baseten winner preparation differs")
        outstanding = budget.status()["outstanding"].get(run_id)
        if (
            outstanding is None
            or outstanding.get("preparation_sha256")
            != _digest(existing_preparation)
            or outstanding.get("provider_identity")
            != baseten_serving_provider_identity(team_name)
            or outstanding.get("provider_role") != "serving"
        ):
            raise ValueError(
                "stored Baseten winner preparation differs from budget"
            )
        try:
            store.get(f"{prefix}/create-intent.json")
            acceptance_raw = store.get(f"{prefix}/provider-acceptance.json")
        except FileNotFoundError:
            admitted = None
        else:
            acceptance = _strict_object(acceptance_raw)
            if (
                encode_provider_acceptance(acceptance) != acceptance_raw
                or acceptance.get("selection") != selection
            ):
                raise ValueError("stored Baseten winner acceptance differs")
            admitted = lifecycle.admit(
                acceptance=acceptance,
                values=values,
                observed_cost_microusd=observed_cost_microusd,
            )
            if admitted.get("state") not in BASETEN_WINNER_ADMIT_STATES:
                raise RuntimeError("Baseten winner admission returned an unknown state")
        references = []
        delivery_references = []
        if admitted is not None and admitted.get("state") == "admitted":
            references.append(
                store_gateway_winner_result_handoff(
                    store,
                    campaign_id=campaign_id,
                    run_id=run_id,
                )
            )
            delivery_references.append(
                store_candidate_key_delivery_handoff(
                    store,
                    campaign_id=campaign_id,
                    run_id=run_id,
                )
            )
        return {
            "schema_version": "milk.baseten-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": (
                "reconcile_only"
                if admitted is None
                else admitted.get("state")
            ),
            "winner_result_references": references,
            "candidate_key_delivery_references": delivery_references,
        }

    current = now()
    if (
        not isinstance(current, dt.datetime)
        or current.tzinfo is None
        or current.utcoffset() != dt.timedelta(0)
        or current.microsecond
    ):
        raise ValueError("winner budget clock must return whole-second UTC")
    preparation_deadline = current + dt.timedelta(
        seconds=PREPARATION_TIMEOUT_SECONDS
    )
    provider_deadline = min(
        current
        + dt.timedelta(seconds=launch["operation"]["max_wall_seconds"]),
        _gateway_time(source["expires_at"], "winner gateway expiry")[0],
    )
    if provider_deadline <= preparation_deadline:
        raise ValueError("Baseten winner has insufficient provider runway")
    preparation = {
        "schema_version": "milk.baseten-winner-budget-preparation.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "selection_sha256": _digest(selection_record),
        "claim_sha256": source["claim_sha256"],
        "outbox_sha256": source["outbox_sha256"],
        "image_release_sha256": image_release_sha256,
        "image_admission_sha256": image_admission_sha256,
        "values_sha256": _digest(values),
        "requested_at": _utc_text(current),
        "preparation_deadline": _utc_text(preparation_deadline),
        "provider_deadline_epoch_seconds": math.floor(
            provider_deadline.timestamp()
        ),
        "state": "preparing",
    }
    preparation_sha256 = _digest(preparation)
    owner = budget.prepare(
        run_id,
        preparation_sha256,
        math.floor(preparation_deadline.timestamp()),
        preparation["provider_deadline_epoch_seconds"],
    )
    if owner != "created":
        return {
            "schema_version": "milk.baseten-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "reconcile_only",
            "winner_result_references": [],
            "candidate_key_delivery_references": [],
        }
    create_same(
        store,
        f"{prefix}/budget-preparation.json",
        canonical_json(preparation),
        "application/json",
    )
    amount = baseten_winner.reservation_microusd(
        launch["gateway_launch_raw"],
        launch["gateway_claim_raw"],
    )
    reservation = budget.reserve(run_id, amount, preparation_sha256)
    reserved_at = now()
    create_same(
        store,
        f"{prefix}/reservation.json",
        canonical_json(reservation),
        "application/json",
    )
    if reservation["state"] != "reserved":
        return {
            "schema_version": "milk.baseten-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "blocked_budget",
            "winner_result_references": [],
            "candidate_key_delivery_references": [],
        }
    acceptance = baseten_winner.provider_acceptance(
        campaign_id=campaign_id,
        run_id=run_id,
        team_name=team_name,
        launch_raw=launch["gateway_launch_raw"],
        claim_raw=launch["gateway_claim_raw"],
        outbox_sha256=source["outbox_sha256"],
        image_release_sha256=image_release_sha256,
        image_admission_sha256=image_admission_sha256,
        preflight=preflight,
        reservation=reservation,
        provider_pass_claim_raw=provider_pass_claim_raw,
        create_authorization_raw=create_authorization_raw,
        reserved_at=reserved_at,
        accepted_at=now(),
    )
    prepared = lifecycle.prepare(
        launch_raw=launch["gateway_launch_raw"],
        claim_raw=launch["gateway_claim_raw"],
        outbox_sha256=source["outbox_sha256"],
        acceptance=acceptance,
        preflight=preflight,
        reservation=reservation,
        provider_pass_claim_raw=provider_pass_claim_raw,
        create_authorization_raw=create_authorization_raw,
        values=values,
    )
    if prepared.get("state") != "prepared":
        raise ValueError("Baseten winner preparation did not become immutable")
    admitted = lifecycle.admit(
        acceptance=acceptance,
        values=values,
        observed_cost_microusd=observed_cost_microusd,
    )
    if admitted.get("state") not in BASETEN_WINNER_ADMIT_STATES:
        raise RuntimeError("Baseten winner admission returned an unknown state")
    references = []
    delivery_references = []
    if admitted.get("state") == "admitted":
        references.append(
            store_gateway_winner_result_handoff(
                store,
                campaign_id=campaign_id,
                run_id=run_id,
            )
        )
        delivery_references.append(
            store_candidate_key_delivery_handoff(
                store,
                campaign_id=campaign_id,
                run_id=run_id,
            )
        )
    validate_winner_result_references(campaign_id, references)
    validate_candidate_key_delivery_references(campaign_id, delivery_references)
    return {
        "schema_version": "milk.baseten-winner-dispatch-result.v1",
        "run_id": run_id,
        "state": admitted.get("state"),
        "winner_result_references": references,
        "candidate_key_delivery_references": delivery_references,
    }


def recover_pending_baseten_winner_admissions(
    lifecycle,
    *,
    store,
    control_store,
    campaign_id,
    scope_prefix,
    settings,
    winner_model_alias,
    current,
):
    """Finish only an already-intended Baseten key handoff under a later lease."""
    if (
        not isinstance(winner_model_alias, str)
        or winner_contract.MODEL_ALIAS.fullmatch(winner_model_alias) is None
    ):
        raise ValueError("winner model alias is invalid")
    launches = discover_gpu_launches(control_store, scope_prefix, current)
    winners = [
        launch
        for launch in launches
        if launch["operation"]["kind"] == "student_winner_deployment"
    ]
    if len(winners) > MAX_CANDIDATE_KEY_DELIVERY_REFERENCES:
        raise ValueError("scope has more than one active winner deployment")
    results = []
    references = []
    delivery_references = []
    for launch in winners:
        run_id = _winner_run_from_launch(
            campaign_id,
            launch,
            settings["image_release_sha256"],
            settings["student_branch_image_admission_sha256"],
        )
        values = winner_contract.settings(
            launch["runtime_image_reference"],
            launch["operation"]["student_job_id"],
            winner_model_alias,
            "DOCKER_REGISTRY_ghcr.io",
            settings["config_secret"],
            settings["control_store_access_key_secret"],
            settings["control_store_secret_key_secret"],
            settings["control_store_session_token_secret"],
        )
        _winner_values(launch, values)
        prefix = f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}"
        if not store.list(f"{prefix}/candidate-key/delivery-intents/"):
            continue
        try:
            store.get(f"{prefix}/gateway-result.json")
        except FileNotFoundError:
            pass
        else:
            continue
        selection_raw = store.get(_selection_key(campaign_id, run_id))
        selection_record = _strict_object(selection_raw)
        acceptance_raw = store.get(f"{prefix}/provider-acceptance.json")
        acceptance = _strict_object(acceptance_raw)
        if (
            canonical_json(selection_record) != selection_raw
            or encode_provider_acceptance(acceptance) != acceptance_raw
            or acceptance.get("run_id") != run_id
            or acceptance.get("selection") != selection_record.get("selection")
            or selection_record.get("selection", {}).get("selected_provider")
            != "baseten"
        ):
            raise ValueError("pending Baseten winner recovery identity differs")
        admitted = lifecycle.admit(
            acceptance=acceptance,
            values=values,
            observed_cost_microusd=0,
        )
        if admitted.get("state") not in BASETEN_WINNER_ADMIT_STATES:
            raise RuntimeError("Baseten winner recovery returned an unknown state")
        results.append(admitted)
        if admitted.get("state") == "admitted":
            references.append(
                store_gateway_winner_result_handoff(
                    store,
                    campaign_id=campaign_id,
                    run_id=run_id,
                )
            )
            delivery_references.append(
                store_candidate_key_delivery_handoff(
                    store,
                    campaign_id=campaign_id,
                    run_id=run_id,
                )
            )
    references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    delivery_references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    validate_winner_result_references(campaign_id, references)
    validate_candidate_key_delivery_references(campaign_id, delivery_references)
    return {
        "schema_version": "milk.baseten-winner-recovery-pass.v1",
        "scope_prefix": scope_prefix,
        "verified": len(winners),
        "recovered": len(references),
        "state": "hold" if not results else results[-1]["state"],
        "results": results,
        "winner_result_references": references,
        "candidate_key_delivery_references": delivery_references,
    }


def teardown_baseten_winner(
    lifecycle,
    *,
    store,
    campaign_id,
    run_id,
    values,
    canary_route_raw=None,
    zero_route_raw=None,
):
    """Teardown a prepared winner using only stored acceptance and a live lease."""
    if (
        not _is_hex64(campaign_id)
        or not _is_hex64(run_id)
        or not hasattr(lifecycle, "teardown")
    ):
        raise ValueError("Baseten winner teardown identity is invalid")
    raw = store.get(
        f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}/"
        "provider-acceptance.json"
    )
    acceptance = _strict_object(raw)
    if (
        encode_provider_acceptance(acceptance) != raw
        or acceptance.get("campaign_id") != campaign_id
        or acceptance.get("run_id") != run_id
        or acceptance.get("selection", {}).get("selected_provider")
        != "baseten"
    ):
        raise ValueError("stored Baseten winner acceptance differs")
    return lifecycle.teardown(
        acceptance=acceptance,
        values=values,
        canary_route_raw=canary_route_raw,
        zero_route_raw=zero_route_raw,
    )


def _validate_modal_execution_plan(plan, campaign_id, run_id, unit_count):
    executions = plan.get("executions") if isinstance(plan, dict) else None
    if (
        not isinstance(plan, dict)
        or set(plan)
        != {"schema_version", "campaign_id", "run_id", "executions"}
        or plan.get("schema_version") != "milk.modal-execution-plan.v1"
        or plan.get("campaign_id") != campaign_id
        or plan.get("run_id") != run_id
        or not isinstance(executions, list)
        or len(executions) != unit_count
    ):
        raise ValueError("Modal execution plan is invalid")
    for ordinal, item in enumerate(executions):
        if (
            not isinstance(item, dict)
            or set(item) != {"ordinal", "command", "environment", "resources"}
            or item.get("ordinal") != ordinal
            or not isinstance(item.get("command"), list)
            or not isinstance(item.get("environment"), dict)
            or not isinstance(item.get("resources"), dict)
        ):
            raise ValueError("Modal execution plan entry is invalid")
    canonical_json(plan)
    return plan


def _modal_budget_identity(modal_identity):
    if not isinstance(modal_identity, dict):
        raise ValueError("Modal provider identity is invalid")
    return modal_provider_identity(
        modal_identity.get("workspace_id"),
        modal_identity.get("environment_name"),
        modal_identity.get("app_name"),
    )


def launch_modal_gpu(
    modal_jobs,
    *,
    store,
    campaign_id,
    workload,
    runtime_image_reference,
    selection_record,
    execution_plan,
    provider_pass_claim_raw,
    create_authorization_raw,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    """Reserve and launch one already-selected Modal GPU operation.

    Provider selection is deliberately outside this function. The immutable
    selection must exist before this function can prepare or reserve budget.
    """
    if not callable(now) or not hasattr(modal_jobs, "launch"):
        raise ValueError("Modal jobs lifecycle and clock are required")
    source = _validate_workload(workload)
    run_id = _workload_run_id(campaign_id, workload)
    selection = (
        selection_record.get("selection")
        if isinstance(selection_record, dict)
        else None
    )
    modal_identity = (
        selection.get("provider_identity")
        if isinstance(selection, dict)
        else None
    )
    if (
        not isinstance(selection_record, dict)
        or selection_record.get("run_id") != run_id
        or not isinstance(selection, dict)
        or selection.get("selected_provider") != "modal"
        or store.get(_selection_key(campaign_id, run_id))
        != canonical_json(selection_record)
    ):
        raise ValueError("Modal launch requires its immutable fixed selection")
    budget_identity = _modal_budget_identity(modal_identity)
    units = _modal_units(workload)
    plan = _validate_modal_execution_plan(
        execution_plan,
        campaign_id,
        run_id,
        len(units),
    )
    prefix = f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal"
    create_same(
        store,
        f"{prefix}/execution-plan.json",
        canonical_json(plan),
        "application/json",
    )
    budget = CampaignBudget(
        store,
        campaign_id,
        budget_identity,
        now=now,
    )
    budget.initialize()
    try:
        existing_preparation_raw = store.get(f"{prefix}/preparation.json")
    except FileNotFoundError:
        existing_preparation = None
    else:
        existing_preparation = _strict_object(existing_preparation_raw)
        if (
            canonical_json(existing_preparation) != existing_preparation_raw
            or existing_preparation.get("schema_version")
            != "milk.modal-launch-group-preparation.v1"
            or existing_preparation.get("campaign_id") != campaign_id
            or existing_preparation.get("run_id") != run_id
            or existing_preparation.get("selection_sha256")
            != _digest(selection_record)
            or existing_preparation.get("execution_plan_sha256")
            != _digest(plan)
            or existing_preparation.get("state") != "preparing"
        ):
            raise ValueError("stored Modal preparation differs")
        outstanding = budget.status()["outstanding"].get(run_id)
        if (
            outstanding is None
            or outstanding.get("preparation_sha256")
            != _digest(existing_preparation)
            or outstanding.get("provider_identity") != budget_identity
            or outstanding.get("provider_role") != "sandbox"
        ):
            raise ValueError("stored Modal preparation differs from budget")
        return {
            "schema_version": "milk.modal-launch-group-result.v1",
            "run_id": run_id,
            "state": "reconcile_only",
            "executions": [],
        }

    current = now()
    if (
        not isinstance(current, dt.datetime)
        or current.tzinfo is None
        or current.utcoffset() != dt.timedelta(0)
        or current.microsecond
    ):
        raise ValueError("Modal budget clock must return whole-second UTC")
    wall_seconds = (
        max(branch["max_gpu_seconds"] for branch in workload["branches"])
        if workload["type"] == "student_fanout"
        else workload["max_gpu_seconds"]
    )
    preparation_deadline = current + dt.timedelta(
        seconds=PREPARATION_TIMEOUT_SECONDS
    )
    provider_deadline = min(
        current + dt.timedelta(seconds=wall_seconds),
        _workload_expiry(workload),
    )
    if provider_deadline <= preparation_deadline:
        raise ValueError("Modal launch has insufficient provider runway")
    preparation = {
        "schema_version": "milk.modal-launch-group-preparation.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "selection_sha256": _digest(selection_record),
        "execution_plan_sha256": _digest(plan),
        "requested_at": _utc_text(current),
        "preparation_deadline": _utc_text(preparation_deadline),
        "provider_deadline_epoch_seconds": math.floor(
            provider_deadline.timestamp()
        ),
        "state": "preparing",
    }
    preparation_sha256 = _digest(preparation)
    owner = budget.prepare(
        run_id,
        preparation_sha256,
        math.floor(preparation_deadline.timestamp()),
        preparation["provider_deadline_epoch_seconds"],
    )
    if owner == "existing":
        return {
            "schema_version": "milk.modal-launch-group-result.v1",
            "run_id": run_id,
            "state": "reconcile_only",
            "executions": [],
        }
    if owner not in {"created", "switched"}:
        raise ValueError("Modal budget preparation state is invalid")
    create_same(
        store,
        f"{prefix}/preparation.json",
        canonical_json(preparation),
        "application/json",
    )
    amount = sum(
        _minutes(
            unit.get("max_gpu_seconds", wall_seconds)
        )
        * RESERVATION_RATE_MICROUSD_PER_MINUTE
        for unit in units
    )
    reservation = budget.reserve(run_id, amount, preparation_sha256)
    reserved_at = now()
    create_same(
        store,
        f"{prefix}/reservation.json",
        canonical_json(reservation),
        "application/json",
    )
    if reservation["state"] != "reserved":
        return {
            "schema_version": "milk.modal-launch-group-result.v1",
            "run_id": run_id,
            "state": "blocked_budget",
            "executions": [],
        }
    acceptance = gpu_provider_acceptance(
        campaign_id=campaign_id,
        workload=workload,
        selection_record=selection_record,
        reservation=reservation,
        provider_pass_claim_raw=provider_pass_claim_raw,
        create_authorization_raw=create_authorization_raw,
        expected_budget_provider_identity=budget_identity,
        reserved_at=reserved_at,
        accepted_at=now(),
    )
    acceptance_raw = encode_provider_acceptance(acceptance)
    create_same(
        store,
        f"claims/v1/{source['claim_sha256']}.json",
        acceptance_raw,
        "application/json",
    )
    create_same(
        store,
        modal_acceptance_key(acceptance),
        acceptance_raw,
        "application/json",
    )
    timeout_seconds = math.floor(
        (
            _utc(acceptance["provider_not_after"], "Modal provider deadline")
            - _utc(acceptance["create_not_after"], "Modal create deadline")
        ).total_seconds()
    )
    if timeout_seconds < 60:
        raise ValueError("Modal launch has insufficient accepted sandbox runway")
    definitions = []
    for ordinal, (unit, item) in enumerate(zip(units, plan["executions"])):
        definition = {
            "schema_version": "milk.modal-execution-definition.v1",
            "campaign_id": campaign_id,
            "run_id": run_id,
            "ordinal": ordinal,
            "provider_acceptance_sha256": provider_acceptance_sha256(
                acceptance
            ),
            "unit": unit,
            "runtime_image_reference": runtime_image_reference,
            "sandbox": {
                "cpu_physical_cores": 8,
                "memory_mib": 65_536,
                "timeout_seconds": timeout_seconds,
                "workdir": "/opt/dragontales/deploy",
                "command_sha256": _digest(item["command"]),
                "environment_sha256": _digest(item["environment"]),
            },
            "resources": copy.deepcopy(item["resources"]),
        }
        validate_modal_definition(acceptance, definition)
        definitions.append(definition)
    for definition in definitions:
        create_same(
            store,
            modal_definition_key(acceptance, definition),
            canonical_json(definition),
            "application/json",
        )

    results = []
    for definition, item in zip(definitions, plan["executions"]):
        results.append(
            modal_jobs.launch(
                acceptance,
                definition,
                item["command"],
                item["environment"],
            )
        )
    result = {
        "schema_version": "milk.modal-launch-group-result.v1",
        "run_id": run_id,
        "state": (
            "created"
            if all(item.get("state") == "created" for item in results)
            else "hold"
        ),
        "executions": results,
    }
    create_same(
        store,
        f"{prefix}/launch-result.json",
        canonical_json(result),
        "application/json",
    )
    return result


def launch_modal_winner(
    modal_jobs,
    *,
    store,
    campaign_id,
    launch,
    selection_record,
    execution_plan,
    image_release_sha256,
    image_admission_sha256,
    provider_pass_claim_raw,
    create_authorization_raw,
    observed_cost_microusd=0,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    """Launch and admit one accepted Modal winner."""
    if (
        not callable(now)
        or not hasattr(modal_jobs, "launch")
        or not hasattr(modal_jobs, "admit_winner")
    ):
        raise ValueError("Modal winner lifecycle and clock are required")
    run_id = _winner_run_from_launch(
        campaign_id,
        launch,
        image_release_sha256,
        image_admission_sha256,
    )
    source = launch["launch_source"]
    selection = (
        selection_record.get("selection")
        if isinstance(selection_record, dict)
        else None
    )
    identity = (
        selection.get("provider_identity")
        if isinstance(selection, dict)
        else None
    )
    if (
        not isinstance(selection_record, dict)
        or selection_record.get("run_id") != run_id
        or selection_record.get("claim_sha256") != source["claim_sha256"]
        or selection_record.get("outbox_sha256") != source["outbox_sha256"]
        or not isinstance(selection, dict)
        or selection.get("selected_provider") != "modal"
        or store.get(_selection_key(campaign_id, run_id))
        != canonical_json(selection_record)
    ):
        raise ValueError("Modal winner launch requires its immutable selection")
    budget_identity = _modal_budget_identity(identity)
    plan = _validate_modal_execution_plan(
        execution_plan,
        campaign_id,
        run_id,
        1,
    )
    prefix = f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal"
    create_same(
        store,
        f"{prefix}/gateway-launch.json",
        bytes(launch["gateway_launch_raw"]),
        "application/json",
    )
    create_same(
        store,
        f"{prefix}/gateway-claim.json",
        bytes(launch["gateway_claim_raw"]),
        "application/json",
    )
    create_same(
        store,
        f"{prefix}/execution-plan.json",
        canonical_json(plan),
        "application/json",
    )
    try:
        recovered_reference = store_gateway_winner_result_handoff(
            store,
            campaign_id=campaign_id,
            run_id=run_id,
        )
    except FileNotFoundError:
        pass
    else:
        return {
            "schema_version": "milk.modal-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "admitted",
            "winner_result_references": [recovered_reference],
        }
    budget = CampaignBudget(
        store,
        campaign_id,
        budget_identity,
        now=now,
    )
    budget.initialize()
    try:
        existing_preparation_raw = store.get(
            f"{prefix}/budget-preparation.json"
        )
    except FileNotFoundError:
        existing_preparation = None
    else:
        existing_preparation = _strict_object(existing_preparation_raw)
        if (
            canonical_json(existing_preparation) != existing_preparation_raw
            or existing_preparation.get("schema_version")
            != "milk.modal-winner-budget-preparation.v1"
            or existing_preparation.get("campaign_id") != campaign_id
            or existing_preparation.get("run_id") != run_id
            or existing_preparation.get("selection_sha256")
            != _digest(selection_record)
            or existing_preparation.get("execution_plan_sha256")
            != _digest(plan)
            or existing_preparation.get("claim_sha256")
            != source["claim_sha256"]
            or existing_preparation.get("outbox_sha256")
            != source["outbox_sha256"]
            or existing_preparation.get("image_release_sha256")
            != image_release_sha256
            or existing_preparation.get("image_admission_sha256")
            != image_admission_sha256
            or existing_preparation.get("state") != "preparing"
        ):
            raise ValueError("stored Modal winner preparation differs")
        outstanding = budget.status()["outstanding"].get(run_id)
        if (
            outstanding is None
            or outstanding.get("preparation_sha256")
            != _digest(existing_preparation)
            or outstanding.get("provider_identity") != budget_identity
            or outstanding.get("provider_role") != "sandbox"
        ):
            raise ValueError(
                "stored Modal winner preparation differs from budget"
            )
        try:
            store.get(f"{prefix}/create-intents/00.json")
        except FileNotFoundError:
            admitted = None
        else:
            acceptance, stored_plan, definitions = _stored_modal_run(
                store,
                campaign_id,
                run_id,
            )
            if acceptance.get("selection") != selection:
                raise ValueError("stored Modal winner acceptance differs")
            item = stored_plan["executions"][0]
            admitted = modal_jobs.admit_winner(
                acceptance,
                definitions[0],
                item["command"],
                item["environment"],
                gateway_claim_raw=store.get(
                    f"{prefix}/gateway-claim.json"
                ),
                observed_cost_microusd=observed_cost_microusd,
            )
        references = []
        if admitted is not None and admitted.get("state") == "admitted":
            references.append(
                store_gateway_winner_result_handoff(
                    store,
                    campaign_id=campaign_id,
                    run_id=run_id,
                )
            )
        return {
            "schema_version": "milk.modal-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": (
                "reconcile_only"
                if admitted is None
                else admitted.get("state")
            ),
            "winner_result_references": references,
        }
    current = now()
    if (
        not isinstance(current, dt.datetime)
        or current.tzinfo is None
        or current.utcoffset() != dt.timedelta(0)
        or current.microsecond
    ):
        raise ValueError("Modal winner budget clock must return whole-second UTC")
    wall_seconds = launch["operation"]["max_wall_seconds"]
    preparation_deadline = current + dt.timedelta(
        seconds=PREPARATION_TIMEOUT_SECONDS
    )
    provider_deadline = min(
        current + dt.timedelta(seconds=wall_seconds),
        _gateway_time(source["expires_at"], "winner expiry")[0],
    )
    if provider_deadline <= preparation_deadline:
        raise ValueError("Modal winner has insufficient provider runway")
    preparation = {
        "schema_version": "milk.modal-winner-budget-preparation.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "selection_sha256": _digest(selection_record),
        "execution_plan_sha256": _digest(plan),
        "claim_sha256": source["claim_sha256"],
        "outbox_sha256": source["outbox_sha256"],
        "image_release_sha256": image_release_sha256,
        "image_admission_sha256": image_admission_sha256,
        "requested_at": _utc_text(current),
        "preparation_deadline": _utc_text(preparation_deadline),
        "provider_deadline_epoch_seconds": math.floor(
            provider_deadline.timestamp()
        ),
        "state": "preparing",
    }
    preparation_sha256 = _digest(preparation)
    owner = budget.prepare(
        run_id,
        preparation_sha256,
        math.floor(preparation_deadline.timestamp()),
        preparation["provider_deadline_epoch_seconds"],
    )
    if owner != "created":
        return {
            "schema_version": "milk.modal-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "reconcile_only",
            "winner_result_references": [],
        }
    create_same(
        store,
        f"{prefix}/budget-preparation.json",
        canonical_json(preparation),
        "application/json",
    )
    amount = _minutes(wall_seconds) * RESERVATION_RATE_MICROUSD_PER_MINUTE
    if amount > launch["operation"]["max_cost_microusd"]:
        raise ValueError("Modal winner cost authority is insufficient")
    reservation = budget.reserve(run_id, amount, preparation_sha256)
    reserved_at = now()
    create_same(
        store,
        f"{prefix}/reservation.json",
        canonical_json(reservation),
        "application/json",
    )
    if reservation["state"] != "reserved":
        return {
            "schema_version": "milk.modal-winner-dispatch-result.v1",
            "run_id": run_id,
            "state": "blocked_budget",
            "winner_result_references": [],
        }
    acceptance = modal_winner_provider_acceptance(
        campaign_id=campaign_id,
        launch=launch,
        run_id=run_id,
        selection_record=selection_record,
        reservation=reservation,
        image_release_sha256=image_release_sha256,
        image_admission_sha256=image_admission_sha256,
        provider_pass_claim_raw=provider_pass_claim_raw,
        create_authorization_raw=create_authorization_raw,
        reserved_at=reserved_at,
        accepted_at=now(),
    )
    acceptance_raw = encode_provider_acceptance(acceptance)
    create_same(
        store,
        f"claims/v1/{source['claim_sha256']}.json",
        acceptance_raw,
        "application/json",
    )
    create_same(
        store,
        modal_acceptance_key(acceptance),
        acceptance_raw,
        "application/json",
    )
    timeout_seconds = math.floor(
        (
            _utc(acceptance["provider_not_after"], "Modal winner deadline")
            - _utc(acceptance["create_not_after"], "Modal winner create deadline")
        ).total_seconds()
    )
    if timeout_seconds < 60:
        raise ValueError("Modal winner has insufficient accepted sandbox runway")
    item = plan["executions"][0]
    operation = launch["operation"]
    definition = {
        "schema_version": "milk.modal-execution-definition.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "ordinal": 0,
        "provider_acceptance_sha256": provider_acceptance_sha256(acceptance),
        "unit": {
            "kind": "student_winner_deployment",
            "student_job_id": operation["student_job_id"],
            "student_result_sha256": operation["student_result_sha256"],
            "winner": operation["winner"],
        },
        "runtime_image_reference": launch["runtime_image_reference"],
        "sandbox": {
            "cpu_physical_cores": 8,
            "memory_mib": 65_536,
            "timeout_seconds": timeout_seconds,
            "workdir": "/opt/dragontales/deploy",
            "command_sha256": _digest(item["command"]),
            "environment_sha256": _digest(item["environment"]),
        },
        "resources": copy.deepcopy(item["resources"]),
    }
    validate_modal_definition(acceptance, definition)
    create_same(
        store,
        modal_definition_key(acceptance, definition),
        canonical_json(definition),
        "application/json",
    )
    launched = modal_jobs.launch(
        acceptance,
        definition,
        item["command"],
        item["environment"],
    )
    admitted = modal_jobs.admit_winner(
        acceptance,
        definition,
        item["command"],
        item["environment"],
        gateway_claim_raw=launch["gateway_claim_raw"],
        observed_cost_microusd=observed_cost_microusd,
    )
    references = []
    if admitted.get("state") == "admitted":
        references.append(
            store_gateway_winner_result_handoff(
                store,
                campaign_id=campaign_id,
                run_id=run_id,
            )
        )
    validate_winner_result_references(campaign_id, references)
    result = {
        "schema_version": "milk.modal-winner-dispatch-result.v1",
        "run_id": run_id,
        "state": admitted.get("state", launched.get("state", "hold")),
        "winner_result_references": references,
    }
    create_same(
        store,
        f"{prefix}/launch-result.json",
        canonical_json(result),
        "application/json",
    )
    return result


def _stored_modal_run(store, campaign_id, run_id):
    if not _is_hex64(campaign_id) or not _is_hex64(run_id):
        raise ValueError("Modal run identity is invalid")
    prefix = f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal"
    acceptance_raw = store.get(f"{prefix}/provider-acceptance.json")
    acceptance = _strict_object(acceptance_raw)
    if (
        acceptance.get("campaign_id") != campaign_id
        or acceptance.get("run_id") != run_id
        or encode_provider_acceptance(acceptance) != acceptance_raw
    ):
        raise ValueError("stored Modal provider acceptance differs")
    plan_raw = store.get(f"{prefix}/execution-plan.json")
    plan = _strict_object(plan_raw)
    if canonical_json(plan) != plan_raw:
        raise ValueError("stored Modal execution plan is not canonical")
    executions = plan.get("executions") if isinstance(plan, dict) else None
    expected_count = (
        1
        if acceptance.get("schema_version")
        == "milk.winner-provider-acceptance.v1"
        or acceptance.get("operation", {}).get("kind")
        in {"teacher_run", "student_train_merge"}
        else 3
        if acceptance.get("operation", {}).get("kind") == "student_fanout"
        else -1
    )
    _validate_modal_execution_plan(
        plan,
        campaign_id,
        run_id,
        expected_count,
    )
    definitions = []
    for item in executions:
        key = f"{prefix}/execution-definitions/{item['ordinal']:02d}.json"
        definition_raw = store.get(key)
        definition = _strict_object(definition_raw)
        if canonical_json(definition) != definition_raw:
            raise ValueError("stored Modal execution definition is not canonical")
        validate_modal_definition(acceptance, definition)
        definitions.append(definition)
    return acceptance, plan, definitions


def reconcile_modal_gpu(modal_jobs, *, store, campaign_id, run_id):
    """Recover Modal work under the caller's current reconcile lease."""
    acceptance, plan, definitions = _stored_modal_run(
        store,
        campaign_id,
        run_id,
    )
    results = [
        modal_jobs.recover(
            acceptance,
            definition,
            item["command"],
            item["environment"],
        )
        for definition, item in zip(definitions, plan["executions"])
    ]
    return {
        "schema_version": "milk.modal-reconciliation-result.v1",
        "run_id": run_id,
        "state": "reconciled",
        "executions": results,
    }


def teardown_modal_gpu(modal_jobs, *, store, campaign_id, run_id):
    """Terminate Modal work under a later lease without original create bytes."""
    acceptance, plan, definitions = _stored_modal_run(
        store,
        campaign_id,
        run_id,
    )
    results = [
        modal_jobs.terminate(
            acceptance,
            definition,
            item["command"],
            item["environment"],
        )
        for definition, item in zip(definitions, plan["executions"])
    ]
    return {
        "schema_version": "milk.modal-teardown-result.v1",
        "run_id": run_id,
        "state": "zero"
        if all(item.get("state") in {"terminated", "zero"} for item in results)
        else "hold",
        "executions": results,
    }


def _authorized_winner_acceptance(authorization_record, selected_provider):
    if not isinstance(authorization_record, dict):
        raise ValueError("provider teardown authority record is invalid")
    authorization = authorization_record.get("authorization")
    authorization_raw = authorization_record.get("authorization_raw")
    winner_result = authorization_record.get("winner_result")
    gateway_claim_raw = authorization_record.get("gateway_claim_raw")
    if (
        not isinstance(authorization, dict)
        or authorization.get("selected_provider") != selected_provider
        or not isinstance(authorization_raw, (bytes, bytearray))
        or hashlib.sha256(authorization_raw).hexdigest()
        != authorization_record.get("authorization_sha256")
        or not isinstance(winner_result, dict)
        or gateway_claim_raw is not None
        and (
            not isinstance(gateway_claim_raw, (bytes, bytearray))
            or hashlib.sha256(gateway_claim_raw).hexdigest()
            != authorization.get("claim_sha256")
        )
    ):
        raise ValueError("provider teardown authority record differs")
    acceptance = winner_result.get("provider_acceptance")
    if (
        not isinstance(acceptance, dict)
        or provider_acceptance_sha256(acceptance)
        != authorization.get("provider_acceptance_sha256")
        or acceptance.get("campaign_id") is None
        or acceptance.get("run_id") != authorization.get("run_id")
        or acceptance.get("claim_sha256")
        != authorization.get("claim_sha256")
        or acceptance.get("selection", {}).get("selected_provider")
        != selected_provider
        or winner_result.get("student_job_id")
        != authorization.get("student_job_id")
        or winner_result.get("admission", {}).get("execution_id")
        != authorization.get("execution_id")
    ):
        raise ValueError("provider teardown winner acceptance differs")
    encode_provider_acceptance(acceptance)
    return authorization, winner_result, acceptance


def teardown_authorized_baseten_winner(
    lifecycle,
    *,
    store,
    campaign_id,
    authorization_record,
    values,
):
    """Run one gateway-authorized Baseten teardown under the live lease."""
    authorization, unused_result, embedded = _authorized_winner_acceptance(
        authorization_record,
        "baseten",
    )
    del unused_result
    if embedded.get("campaign_id") != campaign_id:
        raise ValueError("Baseten teardown campaign differs")
    prefix = (
        f"campaigns/v1/{campaign_id}/winner-jobs/"
        f"{authorization['run_id']}"
    )
    stored_raw = store.get(f"{prefix}/provider-acceptance.json")
    stored = _strict_object(stored_raw)
    if (
        encode_provider_acceptance(stored) != stored_raw
        or stored != embedded
        or hashlib.sha256(
            store.get(f"{prefix}/gateway-claim.json")
        ).hexdigest()
        != authorization["claim_sha256"]
        or authorization_record.get("gateway_claim_raw") is not None
        and store.get(f"{prefix}/gateway-claim.json")
        != authorization_record["gateway_claim_raw"]
    ):
        raise ValueError("stored Baseten winner differs from teardown authority")
    cleanup = lifecycle.remove_candidate_key_from_gateway(
        acceptance=stored,
        values=values,
        gateway_cleanup_authorization_raw=authorization_record[
            "authorization_raw"
        ],
        gateway_cleanup_authorization_sha256=authorization_record[
            "authorization_sha256"
        ],
        canary_route_raw=authorization_record.get("canary_route_raw"),
        zero_route_raw=authorization_record.get("zero_route_raw"),
    )
    if cleanup.get("state") != "absent":
        raise RuntimeError("gateway candidate key was not removed")
    result = teardown_baseten_winner(
        lifecycle,
        store=store,
        campaign_id=campaign_id,
        run_id=authorization["run_id"],
        values=values,
        canary_route_raw=authorization_record.get("canary_route_raw"),
        zero_route_raw=authorization_record.get("zero_route_raw"),
    )
    if result.get("state") != "zero":
        raise RuntimeError("Baseten winner teardown did not verify zero GPU")
    return result


def teardown_authorized_modal_winner(
    modal_jobs,
    *,
    store,
    campaign_id,
    authorization_record,
    candidate_absence_raw,
):
    """Run one gateway-authorized Modal teardown under the live lease."""
    authorization, unused_result, embedded = _authorized_winner_acceptance(
        authorization_record,
        "modal",
    )
    del unused_result
    if (
        embedded.get("campaign_id") != campaign_id
        or not hasattr(modal_jobs, "teardown_winner")
    ):
        raise ValueError("Modal teardown campaign or lifecycle differs")
    _validate_modal_candidate_absence_ack(
        candidate_absence_raw,
        authorization,
        authorization_record["winner_result"],
    )
    run_id = authorization["run_id"]
    acceptance, plan, definitions = _stored_modal_run(
        store,
        campaign_id,
        run_id,
    )
    prefix = f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal"
    claim_raw = store.get(f"{prefix}/gateway-claim.json")
    if (
        acceptance != embedded
        or len(definitions) != 1
        or hashlib.sha256(claim_raw).hexdigest()
        != authorization["claim_sha256"]
        or authorization_record.get("gateway_claim_raw") is not None
        and claim_raw != authorization_record["gateway_claim_raw"]
    ):
        raise ValueError("stored Modal winner differs from teardown authority")
    item = plan["executions"][0]
    result = modal_jobs.teardown_winner(
        acceptance,
        definitions[0],
        item["command"],
        item["environment"],
        gateway_claim_raw=claim_raw,
        canary_route_raw=authorization_record.get("canary_route_raw"),
        zero_route_raw=authorization_record.get("zero_route_raw"),
    )
    if result.get("state") != "zero":
        raise RuntimeError("Modal winner teardown did not verify zero GPU")
    return result


def _validate_modal_candidate_absence_ack(raw, authorization, winner_result):
    if (
        not isinstance(raw, (bytes, bytearray))
        or not 1 <= len(raw) <= MAX_MODAL_CANDIDATE_ACK_BYTES
    ):
        raise ValueError("Modal candidate absence acknowledgement is invalid")
    raw = bytes(raw)
    value = _strict_object(raw)
    anchor = value.get("gateway_anchor")
    admission = winner_result.get("admission")
    if (
        canonical_json(value) != raw
        or set(value)
        != {
            "candidate_key_sha256",
            "gateway_anchor",
            "gateway_release_id",
            "gateway_release_sha256",
            "gateway_result_sha256",
            "run_id",
            "schema_version",
            "selected_provider",
            "service_not_after",
            "state",
            "verified_at",
            "winner_admission_sha256",
        }
        or value.get("schema_version")
        != "milk.modal-candidate-key-ack.v1"
        or value.get("selected_provider") != "modal"
        or value.get("state") != "absent"
        or value.get("run_id") != authorization.get("run_id")
        or value.get("gateway_result_sha256")
        != hashlib.sha256(
            winner_contract.result_bytes(winner_result)
        ).hexdigest()
        or not isinstance(admission, dict)
        or value.get("candidate_key_sha256")
        != admission.get("candidate_api_key_sha256")
        or value.get("service_not_after")
        != admission.get("service_not_after")
        or value.get("winner_admission_sha256")
        != hashlib.sha256(winner_contract.receipt_bytes(admission)).hexdigest()
        or not _valid_uuid(value.get("gateway_release_id"))
        or not _is_hex64(value.get("gateway_release_sha256"))
        or not isinstance(anchor, dict)
        or set(anchor)
        != {
            "source_commit",
            "image_admission_sha256",
            "release_sha256",
            "application_id",
            "application_version",
            "container_image",
            "worker_version_id",
        }
        or re.fullmatch(r"[0-9a-f]{40}", anchor.get("source_commit") or "")
        is None
        or not _is_hex64(anchor.get("image_admission_sha256"))
        or not _is_hex64(anchor.get("release_sha256"))
        or not _valid_uuid(anchor.get("application_id"))
        or type(anchor.get("application_version")) is not int
        or anchor["application_version"] <= 0
        or not isinstance(anchor.get("container_image"), str)
        or not 1 <= len(anchor["container_image"].encode()) <= 2048
        or not _valid_uuid(anchor.get("worker_version_id"))
        or _gateway_time(
            value.get("verified_at"),
            "Modal candidate absence verified_at",
        )[0]
        < _gateway_time(
            authorization.get("authorized_at"),
            "provider teardown authorized_at",
        )[0]
    ):
        raise ValueError("Modal candidate absence acknowledgement differs")
    return value


def store_gateway_winner_result_handoff(
    store,
    *,
    campaign_id,
    run_id,
    result_object_key=None,
):
    """Publish a deterministic evidence reference for gateway-owned ingestion."""
    if not _is_hex64(campaign_id) or not _is_hex64(run_id):
        raise ValueError("winner result handoff identity is invalid")
    expected_key = (
        f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}/gateway-result.json"
    )
    if result_object_key is None:
        result_object_key = expected_key
    if result_object_key != expected_key:
        raise ValueError("winner result handoff object key is invalid")
    raw = store.get(result_object_key)
    result = _strict_object(raw)
    if winner_contract.result_bytes(result) != raw:
        raise ValueError("winner gateway result is not canonical")
    acceptance = result["provider_acceptance"]
    if (
        acceptance.get("campaign_id") != campaign_id
        or acceptance.get("run_id") != run_id
        or result.get("claim_sha256") != acceptance.get("claim_sha256")
        or result.get("provider_binding_sha256")
        != acceptance.get("provider_binding_sha256")
    ):
        raise ValueError("winner gateway result differs from its acceptance")
    reference = {
        "student_job_id": result["student_job_id"],
        "claim_sha256": result["claim_sha256"],
        "object_key": result_object_key,
        "object_sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }
    key = (
        f"campaigns/v1/{campaign_id}/gateway-result-handoffs/"
        f"{result['claim_sha256']}.json"
    )
    create_same(
        store,
        key,
        canonical_json(reference),
        "application/json",
    )
    return reference


def validate_winner_result_references(campaign_id, references):
    if (
        not _is_hex64(campaign_id)
        or not isinstance(references, list)
        or len(references) > MAX_WINNER_RESULT_REFERENCES
    ):
        raise ValueError("winner result references are invalid")
    identities = []
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    suffix = "/gateway-result.json"
    for reference in references:
        key = reference.get("object_key") if isinstance(reference, dict) else None
        if (
            not isinstance(reference, dict)
            or tuple(reference)
            != (
                "student_job_id",
                "claim_sha256",
                "object_key",
                "object_sha256",
                "bytes",
            )
            or not _is_hex64(reference.get("student_job_id"))
            or not _is_hex64(reference.get("claim_sha256"))
            or not _is_hex64(reference.get("object_sha256"))
            or type(reference.get("bytes")) is not int
            or not 1 <= reference["bytes"] <= MAX_PROVIDER_BYTES
            or not isinstance(key, str)
            or not key.startswith(prefix)
            or not key.endswith(suffix)
            or not _is_hex64(key[len(prefix) : -len(suffix)])
        ):
            raise ValueError("winner result reference is invalid")
        identities.append(
            (reference["student_job_id"], reference["claim_sha256"])
        )
    if identities != sorted(set(identities)):
        raise ValueError("winner result references are not sorted and unique")
    return references


def store_candidate_key_delivery_handoff(store, *, campaign_id, run_id):
    if not _is_hex64(campaign_id) or not _is_hex64(run_id):
        raise ValueError("candidate-key delivery handoff identity is invalid")
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}"
    result_raw = store.get(f"{prefix}/gateway-result.json")
    result = _strict_object(result_raw)
    if winner_contract.result_bytes(result) != result_raw:
        raise ValueError("candidate-key delivery winner result is not canonical")
    acceptance = result.get("provider_acceptance")
    if (
        not isinstance(acceptance, dict)
        or acceptance.get("campaign_id") != campaign_id
        or acceptance.get("run_id") != run_id
        or acceptance.get("selection", {}).get("selected_provider") != "baseten"
    ):
        raise ValueError("candidate-key delivery differs from its winner")
    matches = []
    for attempt in range(baseten_winner.MAX_MUTATION_ATTEMPTS):
        object_key = f"{prefix}/candidate-key/deliveries/{attempt:02d}.json"
        try:
            raw = store.get(object_key)
        except FileNotFoundError:
            continue
        receipt = _strict_object(raw)
        if (
            canonical_json(receipt) != raw
            or set(receipt)
            != {
                "schema_version",
                "provider_acceptance_sha256",
                "run_id",
                "provider",
                "team_name",
                "model_id",
                "key_name",
                "key_prefix",
                "candidate_key_sha256",
                "payload_sha256",
                "payload_bytes",
                "gateway_release_id",
                "gateway_release_sha256",
                "acknowledgement_sha256",
                "delivered_at",
                "state",
            }
            or receipt.get("schema_version")
            != "milk.baseten-candidate-key-delivery-receipt.v1"
            or receipt.get("provider_acceptance_sha256")
            != provider_acceptance_sha256(acceptance)
            or receipt.get("run_id") != run_id
            or receipt.get("provider") != "baseten"
            or not isinstance(receipt.get("team_name"), str)
            or TEAM_NAME.fullmatch(receipt["team_name"]) is None
            or receipt["team_name"]
            != acceptance["selection"]["provider_identity"].get("team_name")
            or not isinstance(receipt.get("model_id"), str)
            or winner_contract.MODEL_ID.fullmatch(receipt["model_id"]) is None
            or not isinstance(receipt.get("key_name"), str)
            or re.fullmatch(r"[a-z0-9-]{1,64}", receipt["key_name"]) is None
            or not isinstance(receipt.get("key_prefix"), str)
            or not 1 <= len(receipt["key_prefix"]) <= 256
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E
                for character in receipt["key_prefix"]
            )
            or not _is_hex64(receipt.get("candidate_key_sha256"))
            or not _is_hex64(receipt.get("payload_sha256"))
            or not isinstance(receipt.get("gateway_release_id"), str)
            or str(uuid.UUID(receipt["gateway_release_id"]))
            != receipt["gateway_release_id"]
            or not _is_hex64(receipt.get("gateway_release_sha256"))
            or not _is_hex64(receipt.get("acknowledgement_sha256"))
            or type(receipt.get("payload_bytes")) is not int
            or not 1
            <= receipt["payload_bytes"]
            <= MAX_CANDIDATE_KEY_DELIVERY_BYTES
            or _gateway_time(
                receipt.get("delivered_at"),
                "candidate-key delivered_at",
            )[0]
            < _gateway_time(
                result.get("admission", {}).get("admitted_at"),
                "winner admitted_at",
            )[0]
            or _gateway_time(
                receipt.get("delivered_at"),
                "candidate-key delivered_at",
            )[0]
            + dt.timedelta(seconds=baseten_winner.CANARY_SECONDS)
            > _gateway_time(
                result.get("admission", {}).get("service_not_after"),
                "winner service_not_after",
            )[0]
            or receipt.get("candidate_key_sha256")
            != result.get("admission", {}).get("candidate_api_key_sha256")
            or receipt.get("state") != "delivered"
        ):
            raise ValueError("stored candidate-key delivery receipt differs")
        matches.append((object_key, raw))
    if len(matches) != 1:
        raise ValueError("admitted candidate-key delivery is not uniquely evidenced")
    object_key, raw = matches[0]
    reference = {
        "student_job_id": result["student_job_id"],
        "claim_sha256": result["claim_sha256"],
        "object_key": object_key,
        "object_sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }
    create_same(
        store,
        f"campaigns/v1/{campaign_id}/candidate-key-delivery-handoffs/"
        f"{result['claim_sha256']}.json",
        canonical_json(reference),
        "application/json",
    )
    return reference


def validate_candidate_key_delivery_references(campaign_id, references):
    if (
        not _is_hex64(campaign_id)
        or not isinstance(references, list)
        or len(references) > MAX_CANDIDATE_KEY_DELIVERY_REFERENCES
    ):
        raise ValueError("candidate-key delivery references are invalid")
    identities = []
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    suffix = re.compile(r"/candidate-key/deliveries/[0-9]{2}\.json\Z")
    for reference in references:
        key = reference.get("object_key") if isinstance(reference, dict) else None
        match = suffix.search(key) if isinstance(key, str) else None
        if (
            not isinstance(reference, dict)
            or tuple(reference)
            != (
                "student_job_id",
                "claim_sha256",
                "object_key",
                "object_sha256",
                "bytes",
            )
            or not _is_hex64(reference.get("student_job_id"))
            or not _is_hex64(reference.get("claim_sha256"))
            or not _is_hex64(reference.get("object_sha256"))
            or type(reference.get("bytes")) is not int
            or not 1 <= reference["bytes"] <= MAX_PROVIDER_BYTES
            or not isinstance(key, str)
            or not key.startswith(prefix)
            or match is None
            or not _is_hex64(key[len(prefix) : match.start()])
        ):
            raise ValueError("candidate-key delivery reference is invalid")
        identities.append((reference["student_job_id"], reference["claim_sha256"]))
    if identities != sorted(set(identities)):
        raise ValueError(
            "candidate-key delivery references are not sorted and unique"
        )
    return references


def recover_gateway_winner_result_references(
    store,
    control_store,
    *,
    campaign_id,
    scope_prefix,
    image_release_sha256,
    image_admission_sha256,
    current,
):
    """Recover only immutable winner result references; never replay create."""
    references = []
    for launch in discover_gpu_launches(control_store, scope_prefix, current):
        if launch["operation"]["kind"] != "student_winner_deployment":
            continue
        run_id = _winner_run_from_launch(
            campaign_id,
            launch,
            image_release_sha256,
            image_admission_sha256,
        )
        try:
            reference = store_gateway_winner_result_handoff(
                store,
                campaign_id=campaign_id,
                run_id=run_id,
            )
        except FileNotFoundError:
            continue
        references.append(reference)
    references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    validate_winner_result_references(campaign_id, references)
    return references


def recover_candidate_key_delivery_references(
    store,
    *,
    campaign_id,
    winner_result_references,
):
    validate_winner_result_references(campaign_id, winner_result_references)
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    suffix = "/gateway-result.json"
    references = []
    for winner_reference in winner_result_references:
        key = winner_reference["object_key"]
        run_id = key[len(prefix) : -len(suffix)]
        raw = store.get(key)
        result = _strict_object(raw)
        if result.get("admission", {}).get("provider") != "baseten":
            continue
        references.append(
            store_candidate_key_delivery_handoff(
                store,
                campaign_id=campaign_id,
                run_id=run_id,
            )
        )
    references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    validate_candidate_key_delivery_references(campaign_id, references)
    return references


def _provider_evidence_reference(store, store_identity, object_key):
    if not _is_hex64(store_identity):
        raise ValueError("provider evidence store identity is invalid")
    if (
        not isinstance(object_key, str)
        or not 1 <= len(object_key.encode()) <= 1024
        or any(
            not (
                character.isascii()
                and (
                    character.isalnum()
                    or character in "/._:-"
                )
            )
            for character in object_key
        )
        or any(part in {"", ".", ".."} for part in object_key.split("/"))
    ):
        raise ValueError("provider evidence object key is invalid")
    raw = store.get(object_key)
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(
        raw
    ) <= MAX_STUDENT_ARTIFACT_REFERENCE_BYTES:
        raise ValueError("provider evidence object is not bounded bytes")
    return {
        "store_identity_sha256": store_identity,
        "object_key": object_key,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _validate_provider_evidence_reference(reference):
    if (
        not isinstance(reference, dict)
        or tuple(reference)
        != ("store_identity_sha256", "object_key", "sha256", "bytes")
        or not _is_hex64(reference.get("store_identity_sha256"))
        or not _is_hex64(reference.get("sha256"))
        or type(reference.get("bytes")) is not int
        or not 1
        <= reference["bytes"]
        <= MAX_STUDENT_ARTIFACT_REFERENCE_BYTES
    ):
        raise ValueError("provider teardown evidence reference is invalid")
    key = reference.get("object_key")
    if (
        not isinstance(key, str)
        or not 1 <= len(key.encode()) <= 1024
        or any(
            not (
                character.isascii()
                and (character.isalnum() or character in "/._:-")
            )
            for character in key
        )
        or any(part in {"", ".", ".."} for part in key.split("/"))
    ):
        raise ValueError("provider teardown evidence reference is invalid")
    return reference


def provider_teardown_result_bytes(result):
    if (
        not isinstance(result, dict)
        or tuple(result)
        != (
            "schema_version",
            "scope",
            "student_job_id",
            "claim_sha256",
            "run_id",
            "selected_provider",
            "execution_id",
            "teardown_authorization_sha256",
            "accounted_microusd",
            "provider_zero_evidence",
            "private_log_artifact",
            "budget_settlement",
            "terminated_at",
            "verified_zero_at",
            "state",
        )
        or result.get("schema_version")
        != "dragontales.provider-teardown-result.v1"
        or tuple(result.get("scope", ())) != SCOPE_FIELDS
        or not _is_hex64(result.get("student_job_id"))
        or not _is_hex64(result.get("claim_sha256"))
        or not _is_hex64(result.get("run_id"))
        or result.get("selected_provider") not in {"baseten", "modal"}
        or not isinstance(result.get("execution_id"), str)
        or OPAQUE.fullmatch(result["execution_id"]) is None
        or not _is_hex64(result.get("teardown_authorization_sha256"))
        or type(result.get("accounted_microusd")) is not int
        or result["accounted_microusd"] < 0
        or result.get("state") != "zero"
    ):
        raise ValueError("provider teardown result is invalid")
    _scope_prefix(result["scope"])
    references = [
        _validate_provider_evidence_reference(result[field])
        for field in (
            "provider_zero_evidence",
            "private_log_artifact",
            "budget_settlement",
        )
    ]
    if len(
        {
            (reference["store_identity_sha256"], reference["object_key"])
            for reference in references
        }
    ) != 3:
        raise ValueError("provider teardown evidence references are not distinct")
    terminated = _gateway_time(
        result.get("terminated_at"),
        "provider terminated_at",
    )[0]
    verified = _gateway_time(
        result.get("verified_zero_at"),
        "provider verified_zero_at",
    )[0]
    if verified < terminated:
        raise ValueError("provider zero verification precedes termination")
    return _gateway_bytes(result) + b"\n"


def build_provider_teardown_result(
    authorization_record,
    *,
    accounted_microusd,
    provider_zero_evidence,
    private_log_artifact,
    budget_settlement,
    terminated_at,
    verified_zero_at,
):
    if not isinstance(authorization_record, dict):
        raise ValueError("provider teardown authority record is invalid")
    authorization = authorization_record.get("authorization")
    authorization_raw = authorization_record.get("authorization_raw")
    winner_result = authorization_record.get("winner_result")
    if (
        not isinstance(authorization, dict)
        or not isinstance(authorization_raw, (bytes, bytearray))
        or hashlib.sha256(authorization_raw).hexdigest()
        != authorization_record.get("authorization_sha256")
        or not isinstance(winner_result, dict)
        or type(accounted_microusd) is not int
        or not 0
        <= accounted_microusd
        <= winner_result.get("provider_acceptance", {}).get(
            "reserved_microusd",
            -1,
        )
    ):
        raise ValueError("provider teardown result authority is invalid")
    terminated_at = _gateway_utc_text(terminated_at)
    verified_zero_at = _gateway_utc_text(verified_zero_at)
    if (
        _gateway_time(terminated_at, "provider terminated_at")[0]
        < _gateway_time(
            authorization["authorized_at"],
            "provider teardown authorized_at",
        )[0]
        or _gateway_time(
            verified_zero_at,
            "provider verified_zero_at",
        )[0]
        < _gateway_time(terminated_at, "provider terminated_at")[0]
    ):
        raise ValueError("provider teardown result timing is invalid")
    result = {
        "schema_version": "dragontales.provider-teardown-result.v1",
        "scope": copy.deepcopy(authorization["scope"]),
        "student_job_id": authorization["student_job_id"],
        "claim_sha256": authorization["claim_sha256"],
        "run_id": authorization["run_id"],
        "selected_provider": authorization["selected_provider"],
        "execution_id": authorization["execution_id"],
        "teardown_authorization_sha256": authorization_record[
            "authorization_sha256"
        ],
        "accounted_microusd": accounted_microusd,
        "provider_zero_evidence": copy.deepcopy(provider_zero_evidence),
        "private_log_artifact": copy.deepcopy(private_log_artifact),
        "budget_settlement": copy.deepcopy(budget_settlement),
        "terminated_at": terminated_at,
        "verified_zero_at": verified_zero_at,
        "state": "zero",
    }
    provider_teardown_result_bytes(result)
    return result


def store_gateway_teardown_result_handoff(
    store,
    *,
    campaign_id,
    result,
):
    if not _is_hex64(campaign_id):
        raise ValueError("provider teardown handoff campaign is invalid")
    raw = provider_teardown_result_bytes(result)
    run_id = result["run_id"]
    key = (
        f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}/"
        "gateway-teardown-result.json"
    )
    create_same(store, key, raw, "application/json")
    reference = {
        "student_job_id": result["student_job_id"],
        "authorization_sha256": result[
            "teardown_authorization_sha256"
        ],
        "object_key": key,
        "object_sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }
    create_same(
        store,
        (
            f"campaigns/v1/{campaign_id}/gateway-teardown-handoffs/"
            f"{result['teardown_authorization_sha256']}.json"
        ),
        canonical_json(reference),
        "application/json",
    )
    return reference


def validate_teardown_result_references(campaign_id, references):
    if (
        not _is_hex64(campaign_id)
        or not isinstance(references, list)
        or len(references) > MAX_PROVIDER_TEARDOWN_REFERENCES
    ):
        raise ValueError("provider teardown references are invalid")
    identities = []
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    suffix = "/gateway-teardown-result.json"
    for reference in references:
        key = reference.get("object_key") if isinstance(reference, dict) else None
        if (
            not isinstance(reference, dict)
            or tuple(reference)
            != (
                "student_job_id",
                "authorization_sha256",
                "object_key",
                "object_sha256",
                "bytes",
            )
            or not _is_hex64(reference.get("student_job_id"))
            or not _is_hex64(reference.get("authorization_sha256"))
            or not _is_hex64(reference.get("object_sha256"))
            or type(reference.get("bytes")) is not int
            or not 1 <= reference["bytes"] <= MAX_PROVIDER_BYTES
            or not isinstance(key, str)
            or not key.startswith(prefix)
            or not key.endswith(suffix)
            or not _is_hex64(key[len(prefix) : -len(suffix)])
        ):
            raise ValueError("provider teardown reference is invalid")
        identities.append(
            (reference["student_job_id"], reference["authorization_sha256"])
        )
    if identities != sorted(set(identities)):
        raise ValueError("provider teardown references are not sorted and unique")
    return references


def _winner_teardown_evidence(
    store,
    *,
    campaign_id,
    authorization_record,
    evidence_store_identity_sha256,
):
    authorization, winner_result, acceptance = _authorized_winner_acceptance(
        authorization_record,
        authorization_record["authorization"]["selected_provider"],
    )
    run_id = authorization["run_id"]
    provider = authorization["selected_provider"]
    winner_prefix = f"campaigns/v1/{campaign_id}/winner-jobs/{run_id}"
    if provider == "baseten":
        zero_key = f"{winner_prefix}/zero-gpu.json"
        log_key = (
            f"{winner_prefix}/logs/baseten/"
            f"{provider_acceptance_sha256(acceptance)}/index.json"
        )
        zero = _strict_object(store.get(zero_key))
        observed_at = zero.get("observed_at")
        if (
            zero.get("schema_version")
            != "milk.baseten-zero-gpu-verification.v1"
            or zero.get("provider_acceptance_sha256")
            != provider_acceptance_sha256(acceptance)
            or zero.get("state") != "zero"
        ):
            raise ValueError("Baseten zero-GPU evidence differs")
    else:
        zero_key = f"{winner_prefix}/modal-zero-gpu.json"
        zero = _strict_object(store.get(zero_key))
        verification = zero.get("zero_gpu_verification")
        observed_at = (
            verification.get("observed_at")
            if isinstance(verification, dict)
            else None
        )
        if (
            zero.get("schema_version")
            != "milk.modal-winner-teardown-result.v1"
            or zero.get("provider_acceptance_sha256")
            != provider_acceptance_sha256(acceptance)
            or zero.get("state") != "zero"
            or not isinstance(verification, dict)
            or verification.get("state") != "zero"
        ):
            raise ValueError("Modal zero-GPU evidence differs")
        plans_prefix = (
            f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal/"
            "winner/log-plans"
        )
        plan_keys = store.list(plans_prefix, limit=2)
        if (
            len(plan_keys) != 1
            or not plan_keys[0].startswith(plans_prefix + "/")
            or not plan_keys[0].endswith(".json")
            or not _is_hex64(
                plan_keys[0][len(plans_prefix) + 1 : -len(".json")]
            )
        ):
            raise ValueError("Modal winner private log plan is not unique")
        stream_id = plan_keys[0][len(plans_prefix) + 1 : -len(".json")]
        log_key = (
            f"campaigns/v1/{campaign_id}/jobs/{run_id}/modal/"
            f"logs/modal/{stream_id}/index.json"
        )
    observed_at = _gateway_time(
        observed_at,
        "provider zero observed_at",
    )[0]
    observed_cost_microusd = winner_result.get("observed_cost_microusd")
    if (
        type(observed_cost_microusd) is not int
        or not 0
        <= observed_cost_microusd
        <= acceptance["reserved_microusd"]
    ):
        raise ValueError("winner accounted cost is outside its reservation")
    # Provider billing is deliberately non-authoritative here. Until an exact
    # billing receipt is verified, settle the full reservation fail-closed.
    accounted_microusd = acceptance["reserved_microusd"]
    provider_identity = acceptance["selection"]["provider_identity"]
    budget_identity = (
        baseten_serving_provider_identity(provider_identity.get("team_name"))
        if provider == "baseten"
        else _modal_budget_identity(provider_identity)
    )
    budget = CampaignBudget(store, campaign_id, budget_identity)
    budget.initialize()
    settlement = budget.settle(run_id, accounted_microusd)
    budget.finalize(run_id)
    settlement_key = (
        f"campaigns/v1/{campaign_id}/settlements/{run_id}.json"
    )
    if store.get(settlement_key) != canonical_json(settlement):
        raise ValueError("winner budget settlement evidence differs")
    result = build_provider_teardown_result(
        authorization_record,
        accounted_microusd=accounted_microusd,
        provider_zero_evidence=_provider_evidence_reference(
            store,
            evidence_store_identity_sha256,
            zero_key,
        ),
        private_log_artifact=_provider_evidence_reference(
            store,
            evidence_store_identity_sha256,
            log_key,
        ),
        budget_settlement=_provider_evidence_reference(
            store,
            evidence_store_identity_sha256,
            settlement_key,
        ),
        terminated_at=observed_at,
        verified_zero_at=observed_at,
    )
    return store_gateway_teardown_result_handoff(
        store,
        campaign_id=campaign_id,
        result=result,
    )


def dispatch_provider_teardowns(
    baseten_winner_lifecycle,
    modal_jobs,
    *,
    store,
    control_store,
    campaign_id,
    scope_prefix,
    evidence_store_identity_sha256,
    baseten_values_by_run_id,
    now=lambda: dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
):
    """Execute the literal Baseten/Modal gateway teardown branches."""
    if (
        not callable(now)
        or not isinstance(baseten_values_by_run_id, dict)
        or not _is_hex64(campaign_id)
        or not _is_hex64(evidence_store_identity_sha256)
    ):
        raise ValueError("provider teardown dispatch configuration is invalid")
    records = discover_provider_teardown_authorizations(
        control_store,
        scope_prefix,
        now(),
    )
    baseten_run_ids = {
        record["authorization"]["run_id"]
        for record in records
        if record["authorization"]["selected_provider"] == "baseten"
    }
    if set(baseten_values_by_run_id) != baseten_run_ids:
        raise ValueError("every Baseten teardown needs its exact winner values")
    for run_id, values in baseten_values_by_run_id.items():
        if not isinstance(values, dict):
            raise ValueError("Baseten teardown winner values are invalid")
        expected_values = winner_contract.settings(
            values.get("student_branch_image"),
            values.get("student_job_id"),
            values.get("model_alias"),
            values.get("registry_secret"),
            values.get("config_secret"),
            values.get("control_store_access_key_secret"),
            values.get("control_store_secret_key_secret"),
            values.get("control_store_session_token_secret"),
        )
        if values != expected_values or not _is_hex64(run_id):
            raise ValueError("Baseten teardown run ID is invalid")
    references = []
    results = []
    for record in records:
        authorization = record["authorization"]
        if authorization["selected_provider"] == "baseten":
            result = teardown_authorized_baseten_winner(
                baseten_winner_lifecycle,
                store=store,
                campaign_id=campaign_id,
                authorization_record=record,
                values=baseten_values_by_run_id[authorization["run_id"]],
            )
        else:
            absence_key = (
                f"{scope_prefix}/jobs/student/"
                f"{authorization['student_job_id']}/winner-deployment/"
                "modal-candidate-credential/absent-ack.json"
            )
            try:
                candidate_absence_raw = control_store.get(absence_key)
            except FileNotFoundError:
                continue
            except urllib.error.HTTPError as error:
                if error.code != 404:
                    raise
                continue
            result = teardown_authorized_modal_winner(
                modal_jobs,
                store=store,
                campaign_id=campaign_id,
                authorization_record=record,
                candidate_absence_raw=candidate_absence_raw,
            )
        results.append(result)
        references.append(
            _winner_teardown_evidence(
                store,
                campaign_id=campaign_id,
                authorization_record=record,
                evidence_store_identity_sha256=(
                    evidence_store_identity_sha256
                ),
            )
        )
    references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["authorization_sha256"],
        )
    )
    validate_teardown_result_references(campaign_id, references)
    return {
        "schema_version": "milk.jobs-provider-teardown-dispatch.v1",
        "scope_prefix": scope_prefix,
        "verified": len(records),
        "completed": len(references),
        "state": "zero" if records and len(references) == len(records) else "hold",
        "results": results,
        "teardown_result_references": references,
    }


def _identifier(value, label):
    if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{label} is invalid")
    return value


def _digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _error_digest(error):
    return hashlib.sha256(f"{type(error).__name__}:{error}".encode()).hexdigest()


def _sanitize_provider_request(value):
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            if key == "secret_ref" or (
                isinstance(item, str)
                and item.startswith("${{ secrets.")
                and item.endswith(" }}")
            ):
                encoded = (
                    item.encode()
                    if isinstance(item, str)
                    else canonical_json(item)
                )
                sanitized[key] = {
                    "schema_version": "milk.sensitive-reference-digest.v1",
                    "bytes": len(encoded),
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                }
            else:
                sanitized[key] = _sanitize_provider_request(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_provider_request(item) for item in value]
    if value is None or isinstance(value, (str, bool)) or type(value) is int:
        return value
    raise ValueError("provider request contains an unsupported value")


def _minutes(seconds):
    if type(seconds) is not int or seconds <= 0:
        raise ValueError("provider wall time is invalid")
    return (seconds + 59) // 60


def _validate_execution_record(value, project_id, name, ordinal):
    if (
        not isinstance(value, dict)
        or value.get("provider") != "baseten"
        or value.get("project_id") != project_id
        or _identifier(value.get("execution_id"), "Baseten training job ID")
        != value["execution_id"]
        or value.get("execution_name") != name
        or value.get("gpu_type") != "H100"
        or value.get("gpu_count") != 1
        or value.get("availability_model") != "dedicated"
        or value.get("node_count") != 1
        or value.get("status") not in ACTIVE_STATUSES | TERMINAL_STATUSES
        or value.get("ordinal") != ordinal
    ):
        raise ValueError("stored Baseten execution identity is invalid")
    created = _utc(value.get("created_at"), "Baseten created_at")
    updated = _utc(value.get("updated_at"), "Baseten updated_at")
    if updated < created:
        raise ValueError("Baseten updated_at precedes created_at")
    return value


def _validate_create_result(value, run_id, definition_entry, project_id):
    ordinal = definition_entry["ordinal"]
    state = value.get("state") if isinstance(value, dict) else None
    common = {"schema_version", "run_id", "ordinal", "state"}
    if (
        value.get("schema_version") != "milk.baseten-provider-create-result.v1"
        or value.get("run_id") != run_id
        or value.get("ordinal") != ordinal
    ):
        raise ValueError("stored Baseten create result is invalid")
    if state == "created":
        if set(value) != common | {"response_bytes", "response_sha256"} | EXECUTION_FIELDS:
            raise ValueError("stored Baseten create result is invalid")
        if (
            type(value.get("response_bytes")) is not int
            or not 1 <= value["response_bytes"] <= MAX_PROVIDER_BYTES
            or HEX64.fullmatch(value.get("response_sha256", "")) is None
        ):
            raise ValueError("stored Baseten create response digest is invalid")
        return _validate_execution_record(
            value,
            project_id,
            definition_entry["name"],
            ordinal,
        )
    if state == "ambiguous_create":
        if set(value) != common | {
            "response_bytes",
            "response_sha256",
            "error_type",
            "error_sha256",
        }:
            raise ValueError("stored Baseten create result is invalid")
        response_bytes = value.get("response_bytes")
        response_sha256 = value.get("response_sha256")
        if (
            (response_bytes is None) != (response_sha256 is None)
            or (
                response_bytes is not None
                and (
                    type(response_bytes) is not int
                    or not 0 <= response_bytes <= MAX_PROVIDER_BYTES
                    or HEX64.fullmatch(response_sha256 or "") is None
                )
            )
            or _identifier(value.get("error_type"), "provider error type")
            != value["error_type"]
            or HEX64.fullmatch(value.get("error_sha256", "")) is None
        ):
            raise ValueError("stored ambiguous Baseten create result is invalid")
        return value
    raise ValueError("stored Baseten create result state is invalid")


def _validate_create_resolution(value, run_id, definition_entry, project_id):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "run_id",
            "ordinal",
            "state",
        }
        | EXECUTION_FIELDS
        or value.get("schema_version")
        != "milk.baseten-provider-create-resolution.v1"
        or value.get("run_id") != run_id
        or value.get("state") != "resolved"
    ):
        raise ValueError("stored Baseten create resolution is invalid")
    return _validate_execution_record(
        value,
        project_id,
        definition_entry["name"],
        definition_entry["ordinal"],
    )


def _validate_stop_result(value, run_id, execution, attempt, project_id):
    ordinal = execution["ordinal"]
    common = {
        "schema_version",
        "run_id",
        "ordinal",
        "attempt",
        "state",
    }
    if (
        not isinstance(value, dict)
        or value.get("schema_version")
        != "milk.baseten-provider-stop-attempt-result.v1"
        or value.get("run_id") != run_id
        or value.get("ordinal") != ordinal
        or value.get("attempt") != attempt
    ):
        raise ValueError("stored Baseten stop-attempt result is invalid")
    if value.get("state") in {"stopped", "stop_accepted"}:
        if set(value) != common | {"response_bytes", "response_sha256"} | EXECUTION_FIELDS:
            raise ValueError("stored Baseten stop-attempt result is invalid")
        if (
            type(value.get("response_bytes")) is not int
            or not 1 <= value["response_bytes"] <= MAX_PROVIDER_BYTES
            or HEX64.fullmatch(value.get("response_sha256", "")) is None
        ):
            raise ValueError("stored Baseten stop response digest is invalid")
        _validate_execution_record(
            value,
            project_id,
            execution["execution_name"],
            ordinal,
        )
        if value["execution_id"] != execution["execution_id"]:
            raise ValueError("stored Baseten stop execution differs")
        if value["state"] == "stopped" and value["status"] != "TRAINING_JOB_STOPPED":
            raise ValueError("stored Baseten stopped result has a non-stopped status")
        return value
    if value.get("state") == "ambiguous_stop":
        if set(value) != common | {
            "response_bytes",
            "response_sha256",
            "error_type",
            "error_sha256",
        }:
            raise ValueError("stored Baseten stop-attempt result is invalid")
        response_bytes = value.get("response_bytes")
        response_sha256 = value.get("response_sha256")
        if (
            (response_bytes is None) != (response_sha256 is None)
            or (
                response_bytes is not None
                and (
                    type(response_bytes) is not int
                    or not 0 <= response_bytes <= MAX_PROVIDER_BYTES
                    or HEX64.fullmatch(response_sha256 or "") is None
                )
            )
            or _identifier(value.get("error_type"), "provider error type")
            != value["error_type"]
            or HEX64.fullmatch(value.get("error_sha256", "")) is None
        ):
            raise ValueError("stored ambiguous Baseten stop result is invalid")
        return value
    raise ValueError("stored Baseten stop-attempt state is invalid")


def _validate_observation(value, run_id, definition_entry, project_id):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "run_id",
            "ordinal",
            "observed_at",
            "response_bytes",
            "response_sha256",
        }
        | EXECUTION_FIELDS
        or value.get("schema_version") != "milk.baseten-provider-observation.v1"
        or value.get("run_id") != run_id
        or type(value.get("response_bytes")) is not int
        or not 1 <= value["response_bytes"] <= MAX_PROVIDER_BYTES
        or HEX64.fullmatch(value.get("response_sha256", "")) is None
    ):
        raise ValueError("stored Baseten provider observation is invalid")
    observed_at = _utc(value.get("observed_at"), "provider observation time")
    value = _validate_execution_record(
        value,
        project_id,
        definition_entry["name"],
        definition_entry["ordinal"],
    )
    skew = dt.timedelta(seconds=PROVIDER_CLOCK_SKEW_SECONDS)
    if (
        _utc(value["created_at"], "Baseten created_at") > observed_at + skew
        or _utc(value["updated_at"], "Baseten updated_at") > observed_at + skew
    ):
        raise ValueError("Baseten provider timestamps exceed observation time")
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


class BasetenTransport:
    def __init__(self, api_key, *, timeout_seconds=30, opener=None):
        if (
            not isinstance(api_key, str)
            or not api_key
            or len(api_key) > 4096
            or any(character.isspace() or ord(character) < 32 for character in api_key)
        ):
            raise ValueError("BASETEN_API_KEY is required")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120:
            raise ValueError("Baseten timeout must be in 1..=120 seconds")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_NoRedirect)

    def request(self, method, path, body=None, query=None, *, timeout_seconds=None):
        if method not in {"GET", "POST"}:
            raise ValueError("Baseten method is not allowed")
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or "//" in path
            or any(part in {"", ".", ".."} for part in path[1:].split("/"))
        ):
            raise ValueError("Baseten path is invalid")
        if method == "GET":
            if body is not None:
                raise ValueError("Baseten GET cannot have a body")
            encoded = None
        else:
            if not isinstance(body, dict):
                raise ValueError("Baseten POST requires a typed body")
            encoded = canonical_json(body)
        if query is None:
            query = {}
        if not isinstance(query, dict) or any(
            not isinstance(name, str)
            or not name
            or not isinstance(value, (str, int))
            or type(value) is bool
            for name, value in query.items()
        ):
            raise ValueError("Baseten query is invalid")
        url = API_ROOT + path
        if query:
            url += "?" + urllib.parse.urlencode(sorted(query.items()))
        request = urllib.request.Request(
            url,
            data=encoded,
            method=method,
            headers={
                "Authorization": f"Api-Key {self.api_key}",
                "Accept": "application/json",
                **({"Content-Type": "application/json"} if encoded is not None else {}),
            },
        )
        if timeout_seconds is None:
            timeout_seconds = self.timeout_seconds
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= self.timeout_seconds:
            raise ValueError("Baseten request timeout is invalid")
        with self.opener.open(request, timeout=timeout_seconds) as response:
            if response.getcode() != 200:
                raise RuntimeError("Baseten returned a non-200 response")
            raw = response.read(MAX_PROVIDER_BYTES + 1)
        if len(raw) > MAX_PROVIDER_BYTES:
            raise ValueError("Baseten response is oversized")
        return raw


class JobEvidence:
    def __init__(self, store, campaign_id, run_id):
        if HEX64.fullmatch(campaign_id or "") is None or HEX64.fullmatch(run_id or "") is None:
            raise ValueError("campaign and run IDs must be lowercase hex64")
        self.store = store
        self.run_id = run_id
        self.prefix = f"campaigns/v1/{campaign_id}/jobs/{run_id}"

    def json(self, relative, value):
        if (
            not isinstance(relative, str)
            or not relative.endswith(".json")
            or relative.startswith("/")
            or any(part in {"", ".", ".."} for part in relative.split("/"))
        ):
            raise ValueError("job evidence path is invalid")
        return create_same(
            self.store,
            f"{self.prefix}/{relative}",
            canonical_json(value),
            "application/json",
        )

    def bytes(self, relative, body, content_type="application/octet-stream"):
        if (
            not isinstance(relative, str)
            or relative.startswith("/")
            or any(part in {"", ".", ".."} for part in relative.split("/"))
        ):
            raise ValueError("job evidence path is invalid")
        return create_same(self.store, f"{self.prefix}/{relative}", body, content_type)

    def manifest(self, continue_operation=lambda: True):
        if not callable(continue_operation):
            raise ValueError("manifest deadline predicate is invalid")

        def validate_entry(item, prior_key):
            if (
                not isinstance(item, dict)
                or set(item) != {"key", "bytes", "sha256"}
                or not isinstance(item.get("key"), str)
                or not item["key"].startswith(self.prefix + "/")
                or "/manifest-pages/" in item["key"]
                or item["key"].endswith("/manifest.json")
                or prior_key is not None
                and item["key"] <= prior_key
                or type(item.get("bytes")) is not int
                or not 0 <= item["bytes"] <= MAX_PROVIDER_BYTES
                or HEX64.fullmatch(item.get("sha256", "")) is None
            ):
                raise ValueError("stored job evidence manifest entry is invalid")
            return item["key"]

        try:
            existing = _strict_object(
                self.store.get(f"{self.prefix}/manifest.json")
            )
        except FileNotFoundError:
            existing = None
        if existing is not None:
            pages = existing.get("pages")
            if (
                set(existing)
                != {
                    "schema_version",
                    "run_id",
                    "object_count",
                    "total_bytes",
                    "pages",
                }
                or existing.get("schema_version")
                != "milk.job-evidence-manifest.v2"
                or existing.get("run_id") != self.run_id
                or type(existing.get("object_count")) is not int
                or not 1 <= existing["object_count"] <= MAX_JOB_EVIDENCE_OBJECTS
                or type(existing.get("total_bytes")) is not int
                or not 0 <= existing["total_bytes"] <= MAX_JOB_EVIDENCE_BYTES
                or not isinstance(pages, list)
                or not 1
                <= len(pages)
                <= math.ceil(MAX_JOB_EVIDENCE_OBJECTS / MAX_MANIFEST_PAGE_OBJECTS)
            ):
                raise ValueError("stored job evidence manifest is invalid")
            prior_key = None
            total_bytes = 0
            object_count = 0
            for page_number, reference in enumerate(pages):
                if not continue_operation():
                    raise TimeoutError(
                        "reconciliation deadline reached during manifest replay"
                    )
                if (
                    not isinstance(reference, dict)
                    or set(reference)
                    != {
                        "key",
                        "bytes",
                        "sha256",
                        "object_count",
                        "first_key",
                        "last_key",
                    }
                    or reference.get("key")
                    != f"{self.prefix}/manifest-pages/{page_number:04d}.json"
                    or type(reference.get("bytes")) is not int
                    or not 1 <= reference["bytes"] <= MAX_PROVIDER_BYTES
                    or HEX64.fullmatch(reference.get("sha256", "")) is None
                    or type(reference.get("object_count")) is not int
                    or not 1
                    <= reference["object_count"]
                    <= MAX_MANIFEST_PAGE_OBJECTS
                ):
                    raise ValueError("stored job evidence manifest page is invalid")
                raw = self.store.get(reference["key"])
                if (
                    len(raw) != reference["bytes"]
                    or hashlib.sha256(raw).hexdigest() != reference["sha256"]
                ):
                    raise ValueError("stored job evidence manifest page differs")
                page = _strict_object(raw)
                objects = page.get("objects")
                if (
                    set(page) != {"schema_version", "run_id", "page", "objects"}
                    or page.get("schema_version")
                    != "milk.job-evidence-manifest-page.v1"
                    or page.get("run_id") != self.run_id
                    or page.get("page") != page_number
                    or not isinstance(objects, list)
                    or len(objects) != reference["object_count"]
                ):
                    raise ValueError("stored job evidence manifest page is invalid")
                for item in objects:
                    prior_key = validate_entry(item, prior_key)
                    total_bytes += item["bytes"]
                if (
                    objects[0]["key"] != reference.get("first_key")
                    or objects[-1]["key"] != reference.get("last_key")
                ):
                    raise ValueError("stored job evidence manifest page range differs")
                object_count += len(objects)
            if (
                object_count != existing["object_count"]
                or total_bytes != existing["total_bytes"]
            ):
                raise ValueError("stored job evidence manifest totals differ")
            return existing
        if not continue_operation():
            raise TimeoutError("reconciliation deadline reached before manifest")
        listed = []
        start_after = None
        stored_bound = (
            MAX_JOB_EVIDENCE_OBJECTS
            + math.ceil(MAX_JOB_EVIDENCE_OBJECTS / MAX_MANIFEST_PAGE_OBJECTS)
            + 1
        )
        while True:
            if not continue_operation():
                raise TimeoutError("reconciliation deadline reached during manifest list")
            page_limit = min(MAX_LIST_KEYS, stored_bound + 1 - len(listed))
            page = self.store.list(
                self.prefix,
                limit=page_limit,
                start_after=start_after,
            )
            listed.extend(page)
            if len(listed) > stored_bound:
                raise ValueError("job evidence stored object count is invalid")
            if len(page) < page_limit:
                break
            start_after = page[-1]
        keys = sorted(
            key
            for key in listed
            if key != f"{self.prefix}/manifest.json"
            and not key.startswith(f"{self.prefix}/manifest-pages/")
        )
        if not keys or len(keys) > MAX_JOB_EVIDENCE_OBJECTS:
            raise ValueError("job evidence object count is invalid")
        objects = []
        total_bytes = 0
        for key in keys:
            if not continue_operation():
                raise TimeoutError("reconciliation deadline reached during manifest")
            body = self.store.get(key)
            total_bytes += len(body)
            if total_bytes > MAX_JOB_EVIDENCE_BYTES:
                raise ValueError("job evidence exceeds its byte bound")
            item = {
                "key": key,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
            validate_entry(item, objects[-1]["key"] if objects else None)
            objects.append(item)
        pages = []
        for page_number, offset in enumerate(
            range(0, len(objects), MAX_MANIFEST_PAGE_OBJECTS)
        ):
            if not continue_operation():
                raise TimeoutError("reconciliation deadline reached during manifest")
            page_objects = objects[offset : offset + MAX_MANIFEST_PAGE_OBJECTS]
            page = {
                "schema_version": "milk.job-evidence-manifest-page.v1",
                "run_id": self.run_id,
                "page": page_number,
                "objects": page_objects,
            }
            relative = f"manifest-pages/{page_number:04d}.json"
            raw = canonical_json(page)
            self.bytes(relative, raw, "application/json")
            pages.append(
                {
                    "key": f"{self.prefix}/{relative}",
                    "bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "object_count": len(page_objects),
                    "first_key": page_objects[0]["key"],
                    "last_key": page_objects[-1]["key"],
                }
            )
        value = {
            "schema_version": "milk.job-evidence-manifest.v2",
            "run_id": self.run_id,
            "object_count": len(objects),
            "total_bytes": total_bytes,
            "pages": pages,
        }
        self.json("manifest.json", value)
        return value


def _execution_from_job(job, project_id, name, execution_id=None):
    if not isinstance(job, dict):
        raise ValueError("Baseten training job is invalid")
    raw = canonical_json({"training_job": job})
    execution = adapter._provider_execution(
        raw,
        project_id,
        name,
        expected_id=execution_id,
        allowed_statuses=None,
    )
    status = job.get("current_status")
    if status not in ACTIVE_STATUSES | TERMINAL_STATUSES:
        raise ValueError("Baseten training job status is unknown")
    created_at = job.get("created_at")
    updated_at = job.get("updated_at")
    created = _utc(created_at, "Baseten created_at")
    updated = _utc(updated_at, "Baseten updated_at")
    if updated < created:
        raise ValueError("Baseten updated_at precedes created_at")
    return {
        "provider": "baseten",
        "project_id": project_id,
        **execution,
        "gpu_type": "H100",
        "gpu_count": 1,
        "availability_model": "dedicated",
        "node_count": 1,
        "status": status,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _execution_from_mutation_envelope(raw, project_id, name, execution_id=None):
    parsed = _strict_object(raw)
    if set(parsed) != {"training_job"}:
        raise ValueError("Baseten mutation response is invalid")
    return _execution_from_job(parsed["training_job"], project_id, name, execution_id)


def _execution_from_get_envelope(raw, project_id, name, execution_id):
    parsed = _strict_object(raw)
    if set(parsed) != {"training_project", "training_job"}:
        raise ValueError("Baseten get response is invalid")
    project = parsed["training_project"]
    if (
        not isinstance(project, dict)
        or set(project)
        != {"id", "name", "created_at", "updated_at", "latest_job", "team_name"}
        or project.get("id") != project_id
        or not isinstance(project.get("name"), str)
        or not project["name"]
        or not isinstance(project.get("team_name"), str)
        or not project["team_name"]
    ):
        raise ValueError("Baseten training project is invalid")
    created = _utc(project.get("created_at"), "Baseten project created_at")
    updated = _utc(project.get("updated_at"), "Baseten project updated_at")
    if updated < created:
        raise ValueError("Baseten project updated_at precedes created_at")
    execution = _execution_from_job(
        parsed["training_job"], project_id, name, execution_id
    )
    job_project = parsed["training_job"].get("training_project")
    if (
        not isinstance(job_project, dict)
        or job_project.get("id") != project["id"]
        or job_project.get("name") != project["name"]
    ):
        raise ValueError("Baseten project and job identities differ")
    return execution


def _search_matches(raw, project_id, name):
    parsed = _strict_object(raw)
    if set(parsed) != {"training_jobs"} or not isinstance(parsed["training_jobs"], list):
        raise ValueError("Baseten search response is invalid")
    matches = {}
    for job in parsed["training_jobs"]:
        if not isinstance(job, dict):
            raise ValueError("Baseten search result is invalid")
        if job.get("training_project_id") != project_id or job.get("name") != name:
            continue
        execution = _execution_from_job(job, project_id, name)
        prior = matches.setdefault(execution["execution_id"], execution)
        if prior != execution:
            raise ValueError("Baseten search returned a conflicting execution identity")
    return [matches[execution_id] for execution_id in sorted(matches)]


def _baseten_h100_capacity_available(value, team_name):
    if (
        not isinstance(value, dict)
        or "gpu_capacities" not in value
        or not {"gpu_capacities", "team_gpu_capacities"}.issuperset(value)
        or not isinstance(value["gpu_capacities"], list)
        or not isinstance(value.get("team_gpu_capacities", []), list)
        or len(value["gpu_capacities"]) > 64
        or len(value.get("team_gpu_capacities", [])) > 256
    ):
        raise ValueError("Baseten training capacity is invalid")

    def available(items, expected_team=None):
        matches = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Baseten training capacity is invalid")
            allowed = {
                "gpu_type",
                "baseline",
                "limit",
                "usage_count",
                "dedicated_usage_count",
                "spot_usage_count",
                "team_id",
                "team_name",
            }
            if (
                not {"gpu_type", "limit", "usage_count"}.issubset(item)
                or not allowed.issuperset(item)
                or not isinstance(item.get("gpu_type"), str)
                or type(item.get("limit")) is not int
                or type(item.get("usage_count")) is not int
                or item["limit"] < 0
                or item["usage_count"] < 0
                or any(
                    field in item
                    and (type(item[field]) is not int or item[field] < 0)
                    for field in (
                        "baseline",
                        "dedicated_usage_count",
                        "spot_usage_count",
                    )
                )
                or expected_team is not None
                and (
                    not isinstance(item.get("team_id"), str)
                    or not item["team_id"]
                    or not isinstance(item.get("team_name"), str)
                    or not item["team_name"]
                )
            ):
                raise ValueError("Baseten training capacity is invalid")
            if item["gpu_type"] == "H100" and (
                expected_team is None or item["team_name"] == expected_team
            ):
                matches.append(item)
        if len(matches) > 1:
            raise ValueError("Baseten H100 capacity is ambiguous")
        return bool(matches) and matches[0]["usage_count"] < matches[0]["limit"]

    global_available = available(value["gpu_capacities"])
    team_capacities = value.get("team_gpu_capacities", [])
    team_available = available(team_capacities, team_name)
    team_has_h100 = any(
        isinstance(item, dict)
        and item.get("gpu_type") == "H100"
        and item.get("team_name") == team_name
        for item in team_capacities
    )
    if team_has_h100:
        return team_available
    return global_available


class BasetenJobs:
    def __init__(
        self,
        *,
        store,
        campaign_id,
        project_id,
        transport,
        request_guard=None,
        now=lambda: dt.datetime.now(dt.timezone.utc),
    ):
        if HEX64.fullmatch(campaign_id or "") is None:
            raise ValueError("campaign ID must be lowercase hex64")
        if not isinstance(transport, BasetenTransport) and not hasattr(transport, "request"):
            raise ValueError("Baseten transport is invalid")
        if not callable(now):
            raise ValueError("clock must be callable")
        if request_guard is not None and not callable(request_guard):
            raise ValueError("provider request guard must be callable")
        self.store = store
        self.campaign_id = campaign_id
        self.project_id = _identifier(project_id, "Baseten training project ID")
        self.transport = transport
        self.request_guard = request_guard
        self.now = now
        self._reconcile_deadline = None
        self.budget = CampaignBudget(
            store,
            campaign_id,
            baseten_provider_identity(self.project_id),
            now=now,
        )
        self.budget.initialize()

    def preflight(self, team_name):
        if not isinstance(team_name, str) or TEAM_NAME.fullmatch(team_name) is None:
            raise ValueError("Baseten team name is invalid")
        observed_at = _utc_text(self.now())
        try:
            raw = self._request(
                "GET",
                f"/training_projects/{self.project_id}",
            )
        except urllib.error.HTTPError as error:
            status = error.code
            if status == 429:
                reason = "rate_limited"
            elif 500 <= status <= 599:
                reason = "server_unavailable"
            else:
                raise
            return {
                "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
                "provider": "baseten",
                "team_name": team_name,
                "project_id": self.project_id,
                "outcome": "retryable_unavailable",
                "reason": reason,
                "status": status,
                "evidence_sha256": hashlib.sha256(
                    f"{type(error).__name__}:{status}".encode()
                ).hexdigest(),
                "observed_at": observed_at,
            }
        except (TimeoutError, urllib.error.URLError) as error:
            reason = getattr(error, "reason", error)
            if not isinstance(reason, TimeoutError):
                raise
            return {
                "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
                "provider": "baseten",
                "team_name": team_name,
                "project_id": self.project_id,
                "outcome": "retryable_unavailable",
                "reason": "timeout",
                "status": None,
                "evidence_sha256": hashlib.sha256(
                    type(reason).__name__.encode()
                ).hexdigest(),
                "observed_at": observed_at,
            }
        response = _strict_object(raw)
        project = response.get("training_project")
        if (
            set(response) != {"training_project"}
            or not isinstance(project, dict)
            or set(project)
            != {
                "id",
                "name",
                "created_at",
                "updated_at",
                "latest_job",
                "team_name",
            }
            or project.get("id") != self.project_id
            or project.get("team_name") != team_name
            or not isinstance(project.get("name"), str)
            or not project["name"]
            or _utc(project.get("updated_at"), "Baseten project updated_at")
            < _utc(project.get("created_at"), "Baseten project created_at")
        ):
            raise ValueError("Baseten training project preflight is invalid")
        try:
            capacity_raw = self._request("GET", "/training/capacity")
        except urllib.error.HTTPError as error:
            status = error.code
            if status == 404:
                reason = "capability_unavailable"
                receipt_status = None
            elif status == 429:
                reason = "rate_limited"
                receipt_status = status
            elif 500 <= status <= 599:
                reason = "server_unavailable"
                receipt_status = status
            else:
                raise
            return {
                "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
                "provider": "baseten",
                "team_name": team_name,
                "project_id": self.project_id,
                "outcome": "retryable_unavailable",
                "reason": reason,
                "status": receipt_status,
                "evidence_sha256": hashlib.sha256(
                    f"{type(error).__name__}:{status}".encode()
                ).hexdigest(),
                "observed_at": observed_at,
            }
        except (TimeoutError, urllib.error.URLError) as error:
            reason = getattr(error, "reason", error)
            if not isinstance(reason, TimeoutError):
                raise
            return {
                "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
                "provider": "baseten",
                "team_name": team_name,
                "project_id": self.project_id,
                "outcome": "retryable_unavailable",
                "reason": "timeout",
                "status": None,
                "evidence_sha256": hashlib.sha256(
                    type(reason).__name__.encode()
                ).hexdigest(),
                "observed_at": observed_at,
            }
        capacity = _strict_object(capacity_raw)
        if not _baseten_h100_capacity_available(capacity, team_name):
            return {
                "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
                "provider": "baseten",
                "team_name": team_name,
                "project_id": self.project_id,
                "outcome": "retryable_unavailable",
                "reason": "capability_unavailable",
                "status": None,
                "evidence_sha256": hashlib.sha256(capacity_raw).hexdigest(),
                "observed_at": observed_at,
            }
        return {
            "schema_version": BASETEN_TRAINING_PREFLIGHT_SCHEMA,
            "provider": "baseten",
            "team_name": team_name,
            "project_id": self.project_id,
            "outcome": "ready",
            "evidence_sha256": hashlib.sha256(
                b"milk.baseten-training-preflight.v1\0"
                + raw
                + b"\0"
                + capacity_raw
            ).hexdigest(),
            "observed_at": observed_at,
        }

    def _request(self, method, path, body=None, query=None):
        if self.request_guard is not None:
            self.request_guard()
        if (
            self._reconcile_deadline is not None
            and isinstance(self.transport, BasetenTransport)
        ):
            remaining = math.ceil(self._reconcile_deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("reconciliation pass deadline reached")
            return self.transport.request(
                method,
                path,
                body,
                query,
                timeout_seconds=min(self.transport.timeout_seconds, remaining),
            )
        return self.transport.request(method, path, body, query)

    def _finish_terminal(self, evidence, terminal):
        manifest = evidence.manifest(
            lambda: self._reconcile_deadline is None
            or time.monotonic() < self._reconcile_deadline
        )
        if terminal.get("settlement_sha256") is not None:
            self.budget.finalize(evidence.run_id)
        else:
            self.budget.complete_unpaid(evidence.run_id)
        return manifest

    def _load_terminal(self, evidence, definition):
        value = _strict_object(
            self.store.get(f"{evidence.prefix}/terminal.json")
        )
        common_keys = {
            "schema_version",
            "run_id",
            "state",
            "observed_at",
            "evidence_complete",
            "collection_failures",
            "accounted_minutes",
            "accounted_microusd",
            "settlement_sha256",
            "executions",
        }
        state = value.get("state")
        if (
            value.get("schema_version")
            != "milk.baseten-launch-group-terminal.v1"
            or value.get("run_id") != evidence.run_id
            or state
            not in {"terminal", "not_started", "blocked_budget", "not_authorized"}
            or _utc(value.get("observed_at"), "terminal observed_at") is None
            or value.get("evidence_complete") is not True
            or value.get("collection_failures") != []
            or type(value.get("accounted_minutes")) is not int
            or value["accounted_minutes"] < 0
            or type(value.get("accounted_microusd")) is not int
            or value["accounted_microusd"] < 0
            or not isinstance(value.get("executions"), list)
            or len(value["executions"]) != len(definition["executions"])
        ):
            raise ValueError("stored launch-group terminal is invalid")
        if state != "terminal":
            if (
                set(value) != common_keys
                or value["accounted_minutes"] != 0
                or value["accounted_microusd"] != 0
            ):
                raise ValueError("stored non-provider terminal is invalid")
            settlement = self.budget.settlement(evidence.run_id)
            if state == "not_started":
                if (
                    settlement is None
                    or settlement["accounted_microusd"] != 0
                    or value.get("settlement_sha256") != _digest(settlement)
                ):
                    raise ValueError("stored not-started settlement is invalid")
            elif settlement is not None or value.get("settlement_sha256") is not None:
                raise ValueError("unpaid terminal has a settlement")
            for definition_entry, item in zip(
                definition["executions"],
                value["executions"],
            ):
                if item != {
                    "ordinal": definition_entry["ordinal"],
                    "execution_run_id": definition_entry["execution_run_id"],
                    "state": "not_authorized",
                }:
                    raise ValueError("stored non-provider terminal execution is invalid")
            return value
        if (
            set(value)
            != common_keys
            | {
                "cost_basis_sha256",
                "log_collection_sha256",
                "logs_source_complete",
                "metric_collection_sha256",
                "metrics_source_complete",
            }
            or HEX64.fullmatch(value.get("cost_basis_sha256", "")) is None
            or HEX64.fullmatch(value.get("log_collection_sha256", "")) is None
            or type(value.get("logs_source_complete")) is not bool
            or HEX64.fullmatch(value.get("metric_collection_sha256", ""))
            is None
            or type(value.get("metrics_source_complete")) is not bool
            or HEX64.fullmatch(value.get("settlement_sha256", "")) is None
        ):
            raise ValueError("stored provider terminal is invalid")
        cost_basis = self._load_cost_basis(evidence, definition)
        if (
            value["cost_basis_sha256"] != _digest(cost_basis)
            or value["accounted_minutes"] != cost_basis["accounted_minutes"]
            or value["accounted_microusd"] != cost_basis["accounted_microusd"]
        ):
            raise ValueError("stored provider terminal cost differs")
        observed_cost_items = [
            item
            for item in cost_basis["executions"]
            if item["state"] == "observed_terminal"
        ]
        log_relative = (
            "log-collections/summary.json"
            if observed_cost_items
            else "log-collections/unavailable.json"
        )
        log_collection = _strict_object(
            self.store.get(f"{evidence.prefix}/{log_relative}")
        )
        if (
            value["log_collection_sha256"] != _digest(log_collection)
            or value["logs_source_complete"]
            != (
                log_collection.get("source_complete") is True
                and len(observed_cost_items) == len(cost_basis["executions"])
            )
        ):
            raise ValueError("stored provider terminal log evidence differs")
        metric_collection = _strict_object(
            self.store.get(f"{evidence.prefix}/metric-collections/summary.json")
        )
        if (
            value["metric_collection_sha256"] != _digest(metric_collection)
            or value["metrics_source_complete"]
            is not metric_collection.get("source_complete")
        ):
            raise ValueError("stored provider terminal metric evidence differs")
        settlement = self.budget.settlement(evidence.run_id)
        if (
            settlement is None
            or settlement["accounted_microusd"] != value["accounted_microusd"]
            or value["settlement_sha256"] != _digest(settlement)
        ):
            raise ValueError("stored provider terminal settlement differs")
        for definition_entry, item, cost_item in zip(
            definition["executions"],
            value["executions"],
            cost_basis["executions"],
        ):
            if cost_item["state"] == "duplicate_create":
                duplicate_set = self._load_duplicate_set(evidence, definition_entry)
                expected_matches = []
                for match in cost_item["matches"]:
                    expected_matches.append(
                        _strict_object(
                            self.store.get(
                                f"{evidence.prefix}/duplicate-observations/"
                                f"{definition_entry['ordinal']:02d}/"
                                f"{match['execution_id']}/"
                                f"{match['observation_sha256']}.json"
                            )
                        )
                    )
                expected = {
                    "ordinal": definition_entry["ordinal"],
                    "execution_run_id": definition_entry["execution_run_id"],
                    "execution_name": definition_entry["name"],
                    "duplicate_set_sha256": _digest(duplicate_set),
                    "matches": expected_matches,
                    "state": "duplicate_create_terminal",
                }
                if item != expected:
                    raise ValueError("stored duplicate terminal execution differs")
                continue
            _validate_observation(
                item,
                evidence.run_id,
                definition_entry,
                self.project_id,
            )
            if cost_item["observation_sha256"] != _digest(item):
                raise ValueError("stored terminal observation differs from cost basis")
            if self.store.get(
                f"{evidence.prefix}/observations/"
                f"{definition_entry['ordinal']:02d}/{_digest(item)}.json"
            ) != canonical_json(item):
                raise ValueError("stored terminal observation is not authoritative")
        return value

    def _load_preparation(self, evidence, definition):
        value = _strict_object(
            self.store.get(f"{evidence.prefix}/preparation.json")
        )
        executions = value.get("executions")
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "requested_at",
                "preparation_deadline",
                "provider_deadline_epoch_seconds",
                "executions",
                "state",
            }
            or value.get("schema_version")
            != "milk.baseten-launch-group-preparation.v1"
            or value.get("campaign_id") != self.campaign_id
            or value.get("run_id") != evidence.run_id
            or value.get("state") != "preparing"
            or not isinstance(executions, list)
            or len(executions) != len(definition["executions"])
        ):
            raise ValueError("stored launch-group preparation is invalid")
        requested_at = _utc(value["requested_at"], "preparation requested_at")
        preparation_deadline = _utc(
            value["preparation_deadline"], "preparation deadline"
        )
        if preparation_deadline != requested_at + dt.timedelta(
            seconds=PREPARATION_TIMEOUT_SECONDS
        ):
            raise ValueError("stored launch-group preparation deadline is invalid")
        authority_expiry = _workload_expiry(definition["workload"])
        expected_deadlines = []
        for definition_entry, item in zip(definition["executions"], executions):
            deadline = requested_at + dt.timedelta(
                seconds=definition_entry["max_wall_seconds"]
            )
            deadline = min(deadline, authority_expiry)
            if (
                not isinstance(item, dict)
                or set(item) != {"ordinal", "deadline"}
                or item.get("ordinal") != definition_entry["ordinal"]
                or _utc(item.get("deadline"), "provider deadline") != deadline
            ):
                raise ValueError("stored provider deadline is invalid")
            expected_deadlines.append(deadline)
        expected_provider_deadline = math.floor(
            min(expected_deadlines).timestamp()
        )
        if (
            value.get("provider_deadline_epoch_seconds")
            != expected_provider_deadline
            or math.floor(preparation_deadline.timestamp())
            >= expected_provider_deadline
        ):
            raise ValueError("stored launch-group deadline bound is invalid")
        return value

    def _load_intents_complete(self, evidence, definition, preparation):
        value = _strict_object(
            self.store.get(f"{evidence.prefix}/intents-complete.json")
        )
        hashes = value.get("intent_sha256s")
        if (
            set(value)
            != {
                "schema_version",
                "run_id",
                "preparation_sha256",
                "intent_sha256s",
                "completed_at",
                "mutation_not_after",
                "state",
            }
            or value.get("schema_version")
            != "milk.baseten-create-intents-complete.v1"
            or value.get("run_id") != evidence.run_id
            or value.get("preparation_sha256") != _digest(preparation)
            or value.get("state") != "complete"
            or not isinstance(hashes, list)
            or len(hashes) != len(definition["executions"])
        ):
            raise ValueError("stored create-intents completion is invalid")
        completed_at = _utc(value["completed_at"], "intents completed_at")
        mutation_not_after = _utc(
            value["mutation_not_after"], "provider create mutation deadline"
        )
        preparation_deadline = _utc(
            preparation["preparation_deadline"], "preparation deadline"
        )
        if (
            completed_at < _utc(preparation["requested_at"], "preparation requested_at")
            or completed_at > preparation_deadline
            or mutation_not_after
            != preparation_deadline
            + dt.timedelta(seconds=CREATE_MUTATION_TAIL_SECONDS)
        ):
            raise ValueError("stored create-intents completion time is invalid")
        reservation = _strict_object(
            self.store.get(f"{evidence.prefix}/reservation.json")
        )
        reservation_sha256 = _digest(reservation)
        for definition_entry, planned, expected_sha256 in zip(
            definition["executions"], preparation["executions"], hashes
        ):
            intent = _strict_object(
                self.store.get(
                    f"{evidence.prefix}/create-intents/"
                    f"{definition_entry['ordinal']:02d}.json"
                )
            )
            if (
                set(intent)
                != {
                    "schema_version",
                    "campaign_id",
                    "run_id",
                    "ordinal",
                    "name",
                    "request_sha256",
                    "reservation_sha256",
                    "requested_at",
                    "deadline",
                }
                or intent.get("schema_version")
                != "milk.baseten-provider-create-intent.v1"
                or intent.get("campaign_id") != self.campaign_id
                or intent.get("run_id") != evidence.run_id
                or intent.get("ordinal") != definition_entry["ordinal"]
                or intent.get("name") != definition_entry["name"]
                or intent.get("request_sha256")
                != definition_entry["request_sha256"]
                or intent.get("reservation_sha256") != reservation_sha256
                or intent.get("requested_at") != preparation["requested_at"]
                or intent.get("deadline") != planned["deadline"]
                or _digest(intent) != expected_sha256
            ):
                raise ValueError("stored provider create intent is invalid")
        return value

    def _terminalize_unstarted(self, evidence, definition):
        budget = self.budget.status()
        if evidence.run_id in budget["open"] or evidence.run_id in budget["pending_settlements"]:
            settlement = self.budget.settle(evidence.run_id, 0)
            settlement_sha256 = _digest(settlement)
            state = "not_started"
        else:
            settlement = self.budget.settlement(evidence.run_id)
            if settlement is not None:
                if settlement["accounted_microusd"] != 0:
                    raise ValueError("unstarted run has a nonzero settlement")
                settlement_sha256 = _digest(settlement)
                state = "not_started"
            else:
                settlement_sha256 = None
                state = "not_authorized"
        terminal = {
            "schema_version": "milk.baseten-launch-group-terminal.v1",
            "run_id": evidence.run_id,
            "state": state,
            "observed_at": _utc_text(self.now()),
            "evidence_complete": True,
            "collection_failures": [],
            "accounted_minutes": 0,
            "accounted_microusd": 0,
            "settlement_sha256": settlement_sha256,
            "executions": [
                {
                    "ordinal": item["ordinal"],
                    "execution_run_id": item["execution_run_id"],
                    "state": "not_authorized",
                }
                for item in definition["executions"]
            ],
        }
        evidence.json("terminal.json", terminal)
        self._finish_terminal(evidence, terminal)
        return terminal

    def _definition(self, workload, entries):
        _validate_workload(workload)
        workload = copy.deepcopy(workload)
        if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_EXECUTIONS:
            raise ValueError("provider execution group size is invalid")
        definitions = []
        prepared = []
        for ordinal, entry in enumerate(entries):
            if not isinstance(entry, dict) or set(entry) != {"name", "payload", "max_wall_seconds"}:
                raise ValueError("provider execution entry is invalid")
            _identifier(entry["name"], "Baseten training job name")
            payload = copy.deepcopy(entry["payload"])
            training_job = payload.get("training_job") if isinstance(payload, dict) else None
            if not isinstance(training_job, dict) or training_job.get("name") != entry["name"]:
                raise ValueError("Baseten training payload is invalid")
            configuration = copy.deepcopy(payload)
            del configuration["training_job"]["name"]
            configuration_sha256 = hashlib.sha256(canonical_json(configuration)).hexdigest()
            execution_run_id = _digest(
                {
                    "schema_version": "milk.baseten-execution-identity.v1",
                    "provider": "baseten",
                    "project_id": self.project_id,
                    "workload": workload,
                    "ordinal": ordinal,
                    "configuration_sha256": configuration_sha256,
                }
            )
            name = f"milk-{execution_run_id}"
            payload["training_job"]["name"] = name
            payload_sha256 = hashlib.sha256(canonical_json(payload)).hexdigest()
            max_wall_seconds = entry["max_wall_seconds"]
            definitions.append(
                {
                    "ordinal": ordinal,
                    "execution_run_id": execution_run_id,
                    "name": name,
                    "configuration_sha256": configuration_sha256,
                    "request_sha256": payload_sha256,
                    "max_wall_seconds": max_wall_seconds,
                    "reserved_microusd": _minutes(max_wall_seconds)
                    * RESERVATION_RATE_MICROUSD_PER_MINUTE,
                }
            )
            prepared.append({**entry, "name": name, "payload": payload})
        definition = {
            "schema_version": "milk.baseten-launch-group-definition.v1",
            "provider": "baseten",
            "project_id": self.project_id,
            "workload": workload,
            "executions": definitions,
        }
        run_id = _workload_run_id(self.campaign_id, workload)
        return run_id, definition, prepared

    def launch(
        self,
        workload,
        entries,
        *,
        selection_record=None,
        provider_pass_claim_raw=None,
        create_authorization_raw=None,
    ):
        run_id, definition, entries = self._definition(workload, entries)
        evidence = JobEvidence(self.store, self.campaign_id, run_id)
        launch_source = definition["workload"]["launch_source"]
        integrated = selection_record is not None
        if integrated != (
            provider_pass_claim_raw is not None
            and create_authorization_raw is not None
        ):
            raise ValueError("Baseten launch authority is incomplete")
        if self.request_guard is not None and not integrated:
            raise ValueError("Baseten new launch requires fixed provider acceptance")
        if not integrated:
            acceptance = {
                "schema_version": "milk.gateway-launch-acceptance.v1",
                "launch_source": launch_source,
                "provider": "baseten",
                "project_id": self.project_id,
                "campaign_id": self.campaign_id,
                "run_id": run_id,
                "evidence_prefix": evidence.prefix,
                "execution_run_ids": [
                    item["execution_run_id"] for item in definition["executions"]
                ],
            }
            create_same(
                self.store,
                f"claims/v1/{launch_source['claim_sha256']}.json",
                canonical_json(acceptance),
                "application/json",
            )
        for item in definition["executions"]:
            create_same(
                self.store,
                f"executions/v1/{item['execution_run_id']}.json",
                canonical_json(
                    {
                        "schema_version": "milk.provider-execution-index.v1",
                        "provider": "baseten",
                        "project_id": self.project_id,
                        "execution_run_id": item["execution_run_id"],
                        "run_id": run_id,
                        "ordinal": item["ordinal"],
                        "evidence_prefix": evidence.prefix,
                    }
                ),
                "application/json",
            )
        evidence.json("definition.json", definition)

        def publish_index():
            create_same(
                self.store,
                f"campaigns/v1/{self.campaign_id}/job-index/{run_id}.json",
                canonical_json(
                    {
                        "schema_version": "milk.baseten-launch-group-index.v1",
                        "campaign_id": self.campaign_id,
                        "run_id": run_id,
                    }
                ),
                "application/json",
            )
        try:
            terminal = self._load_terminal(evidence, definition)
        except FileNotFoundError:
            terminal = None
        if terminal is not None:
            self._finish_terminal(evidence, terminal)
            return {
                "schema_version": "milk.baseten-launch-group-result.v1",
                "run_id": run_id,
                "state": "terminal",
                "executions": [],
            }
        try:
            existing_preparation = self._load_preparation(evidence, definition)
        except FileNotFoundError:
            existing_preparation = None
        if existing_preparation is not None:
            outstanding = self.budget.status()["outstanding"].get(run_id)
            if (
                outstanding is None
                or outstanding["preparation_sha256"]
                != _digest(existing_preparation)
                or outstanding["provider_deadline_epoch_seconds"]
                != existing_preparation["provider_deadline_epoch_seconds"]
            ):
                raise ValueError(
                    "stored launch-group preparation differs from budget authority"
                )
            return {
                "schema_version": "milk.baseten-launch-group-result.v1",
                "run_id": run_id,
                "state": "reconcile_only",
                "executions": [],
            }
        current = self.now()
        requested_at = _utc_text(current)
        preparation_deadline = current + dt.timedelta(
            seconds=PREPARATION_TIMEOUT_SECONDS
        )
        authority_expiry = _workload_expiry(definition["workload"])
        planned_executions = []
        provider_deadlines = []
        for definition_entry in definition["executions"]:
            deadline = current + dt.timedelta(
                seconds=definition_entry["max_wall_seconds"]
            )
            deadline = min(deadline, authority_expiry)
            if deadline <= preparation_deadline:
                raise ValueError("provider deadline has no preparation runway")
            provider_deadlines.append(deadline)
            planned_executions.append(
                {
                    "ordinal": definition_entry["ordinal"],
                    "deadline": _utc_text(deadline),
                }
            )
        preparation = {
            "schema_version": "milk.baseten-launch-group-preparation.v1",
            "campaign_id": self.campaign_id,
            "run_id": run_id,
            "requested_at": requested_at,
            "preparation_deadline": _utc_text(preparation_deadline),
            "provider_deadline_epoch_seconds": math.floor(
                min(provider_deadlines).timestamp()
            ),
            "executions": planned_executions,
            "state": "preparing",
        }
        preparation_sha256 = _digest(preparation)
        owner = self.budget.prepare(
            run_id,
            preparation_sha256,
            math.floor(preparation_deadline.timestamp()),
            preparation["provider_deadline_epoch_seconds"],
        )
        if owner != "created":
            return {
                "schema_version": "milk.baseten-launch-group-result.v1",
                "run_id": run_id,
                "state": "reconcile_only",
                "executions": [],
            }
        evidence.json("preparation.json", preparation)
        amount = sum(item["reserved_microusd"] for item in definition["executions"])
        reservation = self.budget.reserve(run_id, amount, preparation_sha256)
        reserved_at = self.now()
        evidence.json("reservation.json", reservation)
        if reservation["state"] != "reserved":
            publish_index()
            result = {
                "schema_version": "milk.baseten-launch-group-result.v1",
                "run_id": run_id,
                "state": "blocked_budget",
                "executions": [],
            }
            evidence.json("launch-result.json", result)
            terminal = {
                "schema_version": "milk.baseten-launch-group-terminal.v1",
                "run_id": run_id,
                "state": "blocked_budget",
                "observed_at": _utc_text(self.now()),
                "evidence_complete": True,
                "collection_failures": [],
                "accounted_minutes": 0,
                "accounted_microusd": 0,
                "settlement_sha256": None,
                "executions": [
                    {
                        "ordinal": item["ordinal"],
                        "execution_run_id": item["execution_run_id"],
                        "state": "not_authorized",
                    }
                    for item in definition["executions"]
                ],
            }
            evidence.json("terminal.json", terminal)
            self._finish_terminal(evidence, terminal)
            return result

        provider_acceptance = None
        if integrated:
            provider_acceptance = gpu_provider_acceptance(
                campaign_id=self.campaign_id,
                workload=workload,
                selection_record=selection_record,
                reservation=reservation,
                provider_pass_claim_raw=provider_pass_claim_raw,
                create_authorization_raw=create_authorization_raw,
                expected_budget_provider_identity=baseten_provider_identity(
                    self.project_id
                ),
                reserved_at=reserved_at,
                accepted_at=self.now(),
            )
            acceptance_raw = encode_provider_acceptance(provider_acceptance)
            create_same(
                self.store,
                f"claims/v1/{launch_source['claim_sha256']}.json",
                acceptance_raw,
                "application/json",
            )
            create_same(
                self.store,
                f"{evidence.prefix}/provider-acceptance.json",
                acceptance_raw,
                "application/json",
            )

        for definition_entry, entry in zip(definition["executions"], entries):
            evidence.json(
                f"create-requests/{definition_entry['ordinal']:02d}.json",
                {
                    "schema_version": "milk.baseten-provider-create-request.v1",
                    "run_id": run_id,
                    "ordinal": definition_entry["ordinal"],
                    "request_sha256": definition_entry["request_sha256"],
                    "sanitized_request": _sanitize_provider_request(entry["payload"]),
                },
            )
        intents = []
        for definition_entry, entry, planned in zip(
            definition["executions"], entries, planned_executions
        ):
            intent = {
                "schema_version": "milk.baseten-provider-create-intent.v1",
                "campaign_id": self.campaign_id,
                "run_id": run_id,
                "ordinal": definition_entry["ordinal"],
                "name": definition_entry["name"],
                "request_sha256": definition_entry["request_sha256"],
                "reservation_sha256": _digest(reservation),
                "requested_at": requested_at,
                "deadline": planned["deadline"],
            }
            intents.append(intent)
            evidence.json(
                f"create-intents/{definition_entry['ordinal']:02d}.json",
                intent,
            )

        completed_at = self.now()
        if completed_at > preparation_deadline:
            raise TimeoutError("provider preparation deadline reached before create authorization")
        evidence.json(
            "intents-complete.json",
            {
                "schema_version": "milk.baseten-create-intents-complete.v1",
                "run_id": run_id,
                "preparation_sha256": preparation_sha256,
                "intent_sha256s": [_digest(intent) for intent in intents],
                "completed_at": _utc_text(completed_at),
                "mutation_not_after": _utc_text(
                    preparation_deadline
                    + dt.timedelta(seconds=CREATE_MUTATION_TAIL_SECONDS)
                ),
                "state": "complete",
            },
        )
        self._load_intents_complete(evidence, definition, preparation)

        publish_index()

        try:
            terminal = self._load_terminal(evidence, definition)
        except FileNotFoundError:
            terminal = None
        if terminal is not None:
            self._finish_terminal(evidence, terminal)
            return {
                "schema_version": "milk.baseten-launch-group-result.v1",
                "run_id": run_id,
                "state": "terminal",
                "executions": [],
            }

        def create(entry_pair):
            definition_entry, entry = entry_pair
            ordinal = definition_entry["ordinal"]
            raw = None
            try:
                if provider_acceptance is not None and self.now() > _utc(
                    provider_acceptance["create_not_after"],
                    "provider create deadline",
                ):
                    raise TimeoutError("Baseten provider create authority expired")
                raw = self._request(
                    "POST",
                    f"/training_projects/{self.project_id}/jobs",
                    entry["payload"],
                )
                execution = _execution_from_mutation_envelope(
                    raw, self.project_id, definition_entry["name"]
                )
                result = {
                    "schema_version": "milk.baseten-provider-create-result.v1",
                    "run_id": run_id,
                    "ordinal": ordinal,
                    "state": "created",
                    "response_bytes": len(raw),
                    "response_sha256": hashlib.sha256(raw).hexdigest(),
                    **execution,
                }
            except Exception as error:
                result = {
                    "schema_version": "milk.baseten-provider-create-result.v1",
                    "run_id": run_id,
                    "ordinal": ordinal,
                    "state": "ambiguous_create",
                    "response_bytes": None if raw is None else len(raw),
                    "response_sha256": (
                        None if raw is None else hashlib.sha256(raw).hexdigest()
                    ),
                    "error_type": type(error).__name__,
                    "error_sha256": _error_digest(error),
                }
            evidence.json(f"create-results/{ordinal:02d}.json", result)
            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(entries)) as executor:
            results = list(executor.map(create, zip(definition["executions"], entries)))
        if len(results) > 1 and not all(item["state"] == "created" for item in results):
            abort = {
                "schema_version": "milk.baseten-launch-group-abort.v1",
                "run_id": run_id,
                "observed_at": _utc_text(self.now()),
                "failed_ordinals": [
                    item["ordinal"] for item in results if item["state"] != "created"
                ],
                "state": "aborting",
            }
            try:
                evidence.json("abort.json", abort)
            except Exception:
                pass
            for item in results:
                if item["state"] == "created":
                    item["cleanup_state"] = self._stop_once(evidence, item)
        result = {
            "schema_version": "milk.baseten-launch-group-result.v1",
            "run_id": run_id,
            "state": "created" if all(item["state"] == "created" for item in results) else "hold",
            "executions": results,
        }
        evidence.json("launch-result.json", result)
        return result

    def _stop_once(self, evidence, execution, *, duplicate=False):
        ordinal = execution["ordinal"]
        attempt_prefix = f"stop-attempts/{ordinal:02d}"
        if duplicate:
            execution_id = _identifier(
                execution.get("execution_id"),
                "duplicate Baseten training job ID",
            )
            attempt_prefix = (
                f"duplicate-stop-attempts/{ordinal:02d}/{execution_id}"
            )
        selected_attempt = None
        for attempt in range(MAX_STOP_ATTEMPTS):
            intent_key = f"{evidence.prefix}/{attempt_prefix}/{attempt:02d}-intent.json"
            result_key = f"{evidence.prefix}/{attempt_prefix}/{attempt:02d}-result.json"
            try:
                prior_intent = _strict_object(self.store.get(intent_key))
            except FileNotFoundError:
                selected_attempt = attempt
                break
            if (
                set(prior_intent)
                != {
                    "schema_version",
                    "run_id",
                    "ordinal",
                    "attempt",
                    "execution_id",
                    "execution_name",
                    "requested_at",
                }
                or
                prior_intent.get("schema_version")
                != "milk.baseten-provider-stop-attempt.v1"
                or prior_intent.get("run_id") != evidence.run_id
                or prior_intent.get("ordinal") != ordinal
                or prior_intent.get("attempt") != attempt
                or prior_intent.get("execution_id") != execution["execution_id"]
                or prior_intent.get("execution_name") != execution["execution_name"]
            ):
                raise ValueError("stored Baseten stop attempt is invalid")
            _utc(prior_intent.get("requested_at"), "Baseten stop requested_at")
            try:
                prior_result = _strict_object(self.store.get(result_key))
            except FileNotFoundError:
                continue
            prior_result = _validate_stop_result(
                prior_result,
                evidence.run_id,
                execution,
                attempt,
                self.project_id,
            )
            if prior_result["state"] == "stopped":
                return "stopped"
        if selected_attempt is None:
            return "attempt_bound_exhausted"
        intent = {
            "schema_version": "milk.baseten-provider-stop-attempt.v1",
            "run_id": evidence.run_id,
            "ordinal": ordinal,
            "attempt": selected_attempt,
            "execution_id": execution["execution_id"],
            "execution_name": execution["execution_name"],
            "requested_at": _utc_text(self.now()),
        }
        key = f"{evidence.prefix}/{attempt_prefix}/{selected_attempt:02d}-intent.json"
        try:
            created = self.store.create(key, canonical_json(intent), "application/json")
        except Exception:
            return "stop_intent_evidence_unavailable"
        if not created:
            return "reconcile_only"
        try:
            raw = None
            raw = self._request(
                "POST",
                f"/training_projects/{self.project_id}/jobs/{execution['execution_id']}/stop",
                {},
            )
            observed = _execution_from_mutation_envelope(
                raw,
                self.project_id,
                execution["execution_name"],
                execution["execution_id"],
            )
            state = "stopped" if observed["status"] == "TRAINING_JOB_STOPPED" else "stop_accepted"
            result = {
                "schema_version": "milk.baseten-provider-stop-attempt-result.v1",
                "run_id": evidence.run_id,
                "ordinal": ordinal,
                "attempt": selected_attempt,
                "state": state,
                "response_bytes": len(raw),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                **observed,
            }
        except Exception as error:
            result = {
                "schema_version": "milk.baseten-provider-stop-attempt-result.v1",
                "run_id": evidence.run_id,
                "ordinal": ordinal,
                "attempt": selected_attempt,
                "state": "ambiguous_stop",
                "response_bytes": None if raw is None else len(raw),
                "response_sha256": (
                    None if raw is None else hashlib.sha256(raw).hexdigest()
                ),
                "error_type": type(error).__name__,
                "error_sha256": _error_digest(error),
            }
        try:
            evidence.json(
                f"{attempt_prefix}/{selected_attempt:02d}-result.json",
                result,
            )
        except Exception:
            return result["state"] + "_evidence_unavailable"
        return result["state"]

    def _stop_attempts_used(self, evidence, execution, *, duplicate=False):
        ordinal = execution["ordinal"]
        prefix = f"{evidence.prefix}/stop-attempts/{ordinal:02d}"
        if duplicate:
            execution_id = _identifier(
                execution.get("execution_id"),
                "duplicate Baseten training job ID",
            )
            prefix = (
                f"{evidence.prefix}/duplicate-stop-attempts/"
                f"{ordinal:02d}/{execution_id}"
            )
        used = 0
        for attempt in range(MAX_STOP_ATTEMPTS):
            try:
                self.store.get(f"{prefix}/{attempt:02d}-intent.json")
            except FileNotFoundError:
                break
            used += 1
        return used

    def _load_definition(self, run_id):
        if HEX64.fullmatch(run_id or "") is None:
            raise ValueError("run ID must be lowercase hex64")
        evidence = JobEvidence(self.store, self.campaign_id, run_id)
        definition = _strict_object(self.store.get(f"{evidence.prefix}/definition.json"))
        if _workload_run_id(self.campaign_id, definition.get("workload")) != run_id:
            raise ValueError("launch-group definition differs from its run ID")
        workload = definition.get("workload")
        executions = definition.get("executions")
        if (
            set(definition) != {
                "schema_version",
                "provider",
                "project_id",
                "workload",
                "executions",
            }
            or definition.get("schema_version") != "milk.baseten-launch-group-definition.v1"
            or definition.get("project_id") != self.project_id
            or definition.get("provider") != "baseten"
            or not isinstance(workload, dict)
            or not isinstance(executions, list)
            or not 1 <= len(executions) <= MAX_EXECUTIONS
        ):
            raise ValueError("launch-group definition is invalid")
        _validate_workload(workload)
        workload_type = workload.get("type")
        artifact = {
            "teacher_run": "teacher-gpt-oss",
            "student_train_merge": "student-train",
            "student_fanout": "student-branch",
        }[workload_type]
        release_raw = self.store.get(
            f"image-admissions/v1/releases/{workload['image_release_sha256']}.json"
        )
        admission_raw = self.store.get(
            f"image-admissions/v1/artifacts/{workload['image_admission_sha256']}.json"
        )
        release = _strict_object(release_raw)
        admission = _strict_object(admission_raw)
        release_item_fields = {
            "admission_sha256",
            "artifact",
            "image_reference",
            "ops_log_reference_sha256",
        }
        if release.get("schema_version") == "milk.private-harness-release.v5":
            release_item_fields.add("source_commit")
        if (
            hashlib.sha256(release_raw).hexdigest()
            != workload["image_release_sha256"]
            or hashlib.sha256(admission_raw).hexdigest()
            != workload["image_admission_sha256"]
            or admission.get("schema_version")
            != "milk.private-image-admission.v1"
            or admission.get("artifact") != artifact
            or admission.get("repository") != PRIVATE_IMAGE_REPOSITORIES[artifact]
            or admission.get("image_reference", "").rpartition("@sha256:")[0]
            != PRIVATE_IMAGE_REPOSITORIES[artifact]
            or release.get("schema_version") not in RELEASE_IMAGE_REPOSITORIES
            or not isinstance(release.get("images"), list)
            or not any(
                isinstance(item, dict)
                and set(item) == release_item_fields
                and item["admission_sha256"]
                == workload["image_admission_sha256"]
                and item["artifact"] == artifact
                and item["image_reference"] == admission.get("image_reference")
                and (
                    release.get("schema_version")
                    != "milk.private-harness-release.v5"
                    or item["source_commit"] == admission.get("source_commit")
                )
                and HEX64.fullmatch(item["ops_log_reference_sha256"] or "")
                is not None
                for item in release["images"]
            )
        ):
            raise ValueError("launch-group image admission is invalid")
        for ordinal, item in enumerate(executions):
            if (
                not isinstance(item, dict)
                or set(item) != {
                    "ordinal",
                    "execution_run_id",
                    "name",
                    "configuration_sha256",
                    "request_sha256",
                    "max_wall_seconds",
                    "reserved_microusd",
                }
                or item.get("ordinal") != ordinal
                or HEX64.fullmatch(item.get("execution_run_id", "")) is None
                or item.get("name") != "milk-" + item["execution_run_id"]
                or HEX64.fullmatch(item.get("configuration_sha256", "")) is None
                or HEX64.fullmatch(item.get("request_sha256", "")) is None
                or type(item.get("max_wall_seconds")) is not int
                or item["max_wall_seconds"] <= 0
                or item.get("reserved_microusd")
                != _minutes(item["max_wall_seconds"])
                * RESERVATION_RATE_MICROUSD_PER_MINUTE
            ):
                raise ValueError("launch-group execution definition is invalid")
        launch_source = workload["launch_source"]
        acceptance = _strict_object(
            self.store.get(
                f"claims/v1/{launch_source['claim_sha256']}.json"
            )
        )
        if (
            set(acceptance)
            != {
                "schema_version",
                "launch_source",
                "provider",
                "project_id",
                "campaign_id",
                "run_id",
                "evidence_prefix",
                "execution_run_ids",
            }
            or acceptance.get("schema_version")
            != "milk.gateway-launch-acceptance.v1"
            or acceptance.get("launch_source") != launch_source
            or acceptance.get("provider") != "baseten"
            or acceptance.get("project_id") != self.project_id
            or acceptance.get("campaign_id") != self.campaign_id
            or acceptance.get("run_id") != run_id
            or acceptance.get("evidence_prefix") != evidence.prefix
            or acceptance.get("execution_run_ids")
            != [item["execution_run_id"] for item in executions]
        ):
            raise ValueError("gateway launch acceptance is invalid")
        return evidence, definition

    def _load_duplicate_set(self, evidence, definition_entry):
        ordinal = definition_entry["ordinal"]
        value = _strict_object(
            self.store.get(
                f"{evidence.prefix}/duplicate-sets/{ordinal:02d}.json"
            )
        )
        matches = value.get("matches")
        if (
            set(value)
            != {
                "schema_version",
                "run_id",
                "ordinal",
                "execution_run_id",
                "execution_name",
                "search_observation_sha256",
                "observed_at",
                "matches",
                "state",
            }
            or value.get("schema_version")
            != "milk.baseten-duplicate-create-set.v1"
            or value.get("run_id") != evidence.run_id
            or value.get("ordinal") != ordinal
            or value.get("execution_run_id")
            != definition_entry["execution_run_id"]
            or value.get("execution_name") != definition_entry["name"]
            or HEX64.fullmatch(value.get("search_observation_sha256", ""))
            is None
            or not isinstance(matches, list)
            or len(matches) < 2
            or value.get("state") != "duplicate_create"
        ):
            raise ValueError("stored duplicate-create set is invalid")
        _utc(value.get("observed_at"), "duplicate-create observation time")
        identifiers = []
        for match in matches:
            if not isinstance(match, dict) or set(match) != EXECUTION_FIELDS:
                raise ValueError("stored duplicate-create execution is invalid")
            _validate_execution_record(
                {**match, "ordinal": ordinal},
                self.project_id,
                definition_entry["name"],
                ordinal,
            )
            identifiers.append(match["execution_id"])
        if identifiers != sorted(set(identifiers)):
            raise ValueError("stored duplicate-create executions are not canonical")
        observation = _strict_object(
            self.store.get(
                f"{evidence.prefix}/search-results/{ordinal:02d}.json"
            )
        )
        if (
            _digest(observation) != value["search_observation_sha256"]
            or observation.get("schema_version")
            != "milk.baseten-search-result.v1"
            or observation.get("run_id") != evidence.run_id
            or observation.get("ordinal") != ordinal
            or observation.get("matches") != matches
            or observation.get("state") != "observed"
            or observation.get("observed_at") != value["observed_at"]
        ):
            raise ValueError("duplicate-create search observation is invalid")
        return value

    def _resolve_execution(self, evidence, definition_entry, intents_complete):
        ordinal = definition_entry["ordinal"]
        try:
            self.store.get(f"{evidence.prefix}/create-intents/{ordinal:02d}.json")
        except FileNotFoundError:
            budget = self.budget.status()
            if (
                evidence.run_id in budget["open"]
                or evidence.run_id in budget["pending_settlements"]
            ):
                return ({
                    "ordinal": ordinal,
                    "execution_name": definition_entry["name"],
                    "state": "preparing",
                }, [])
            return ({
                "ordinal": ordinal,
                "execution_name": definition_entry["name"],
                "state": "not_authorized",
            }, [])
        result_key = f"{evidence.prefix}/create-results/{ordinal:02d}.json"
        resolved_key = f"{evidence.prefix}/resolved/{ordinal:02d}.json"
        try:
            result = _strict_object(self.store.get(result_key))
        except FileNotFoundError:
            result = None
        if result is not None:
            result = _validate_create_result(
                result,
                evidence.run_id,
                definition_entry,
                self.project_id,
            )
            if result["state"] == "created":
                return result, []
        try:
            resolved = _strict_object(self.store.get(resolved_key))
            return _validate_create_resolution(
                resolved,
                evidence.run_id,
                definition_entry,
                self.project_id,
            ), []
        except FileNotFoundError:
            pass
        try:
            return self._load_duplicate_set(evidence, definition_entry), []
        except FileNotFoundError:
            pass
        if result is None and self.now() < _utc(
            intents_complete["mutation_not_after"],
            "provider create mutation deadline",
        ):
            return ({
                "ordinal": ordinal,
                "execution_name": definition_entry["name"],
                "state": "create_in_flight",
            }, [])
        request_body = {
            "project_id": self.project_id,
            "job_id": None,
            "statuses": None,
            "order_by": [{"field": "created_at", "order": "desc"}],
        }
        request_sha256 = hashlib.sha256(canonical_json(request_body)).hexdigest()
        intent_key = f"{evidence.prefix}/search-intents/{ordinal:02d}.json"
        search_key = f"{evidence.prefix}/search-results/{ordinal:02d}.json"
        try:
            intent = _strict_object(self.store.get(intent_key))
        except FileNotFoundError:
            intent = {
                "schema_version": "milk.baseten-search-intent.v1",
                "run_id": evidence.run_id,
                "ordinal": ordinal,
                "execution_run_id": definition_entry["execution_run_id"],
                "execution_name": definition_entry["name"],
                "requested_at": _utc_text(self.now()),
                "request_sha256": request_sha256,
                "state": "requested",
            }
            if not self.store.create(
                intent_key,
                canonical_json(intent),
                "application/json",
            ):
                intent = _strict_object(self.store.get(intent_key))
            else:
                raw = None
                try:
                    raw = self._request(
                        "POST",
                        "/training_jobs/search",
                        request_body,
                    )
                    matches = _search_matches(
                        raw,
                        self.project_id,
                        definition_entry["name"],
                    )
                    search = {
                        "schema_version": "milk.baseten-search-result.v1",
                        "run_id": evidence.run_id,
                        "ordinal": ordinal,
                        "execution_run_id": definition_entry["execution_run_id"],
                        "execution_name": definition_entry["name"],
                        "intent_sha256": _digest(intent),
                        "observed_at": _utc_text(self.now()),
                        "response_bytes": len(raw),
                        "response_sha256": hashlib.sha256(raw).hexdigest(),
                        "matches": matches,
                        "state": "observed",
                    }
                except Exception as error:
                    search = {
                        "schema_version": "milk.baseten-search-result.v1",
                        "run_id": evidence.run_id,
                        "ordinal": ordinal,
                        "execution_run_id": definition_entry["execution_run_id"],
                        "execution_name": definition_entry["name"],
                        "intent_sha256": _digest(intent),
                        "observed_at": _utc_text(self.now()),
                        "response_bytes": None if raw is None else len(raw),
                        "response_sha256": (
                            None if raw is None else hashlib.sha256(raw).hexdigest()
                        ),
                        "matches": None,
                        "state": "failed",
                        "error_type": type(error).__name__,
                        "error_sha256": _error_digest(error),
                    }
                try:
                    evidence.json(f"search-results/{ordinal:02d}.json", search)
                except Exception as error:
                    failure = self._collection_failure(
                        evidence,
                        ordinal,
                        "create_search_result",
                        search["observed_at"],
                        error,
                    )
                    return ({
                        "ordinal": ordinal,
                        "execution_name": definition_entry["name"],
                        "intent_sha256": _digest(intent),
                        "state": "search_observation_ambiguous",
                    }, [failure])
        if (
            set(intent)
            != {
                "schema_version",
                "run_id",
                "ordinal",
                "execution_run_id",
                "execution_name",
                "requested_at",
                "request_sha256",
                "state",
            }
            or intent.get("schema_version") != "milk.baseten-search-intent.v1"
            or intent.get("run_id") != evidence.run_id
            or intent.get("ordinal") != ordinal
            or intent.get("execution_run_id")
            != definition_entry["execution_run_id"]
            or intent.get("execution_name") != definition_entry["name"]
            or intent.get("request_sha256") != request_sha256
            or intent.get("state") != "requested"
        ):
            raise ValueError("stored Baseten search intent is invalid")
        _utc(intent.get("requested_at"), "Baseten search requested_at")
        try:
            search = _strict_object(self.store.get(search_key))
        except FileNotFoundError:
            return ({
                "ordinal": ordinal,
                "execution_name": definition_entry["name"],
                "intent_sha256": _digest(intent),
                "state": "search_observation_ambiguous",
            }, [])
        common = {
            "schema_version",
            "run_id",
            "ordinal",
            "execution_run_id",
            "execution_name",
            "intent_sha256",
            "observed_at",
            "response_bytes",
            "response_sha256",
            "matches",
            "state",
        }
        if (
            search.get("schema_version") != "milk.baseten-search-result.v1"
            or search.get("run_id") != evidence.run_id
            or search.get("ordinal") != ordinal
            or search.get("execution_run_id")
            != definition_entry["execution_run_id"]
            or search.get("execution_name") != definition_entry["name"]
            or search.get("intent_sha256") != _digest(intent)
            or search.get("state") not in {"observed", "failed"}
        ):
            raise ValueError("stored Baseten search result is invalid")
        _utc(search.get("observed_at"), "Baseten search observed_at")
        if search["state"] == "failed":
            if (
                set(search) != common | {"error_type", "error_sha256"}
                or search.get("matches") is not None
                or search.get("response_bytes") is not None
                and type(search["response_bytes"]) is not int
                or search.get("response_sha256") is not None
                and HEX64.fullmatch(search["response_sha256"]) is None
                or not isinstance(search.get("error_type"), str)
                or not search["error_type"]
                or HEX64.fullmatch(search.get("error_sha256", "")) is None
            ):
                raise ValueError("stored failed Baseten search is invalid")
            return ({
                "ordinal": ordinal,
                "execution_name": definition_entry["name"],
                "search_result_sha256": _digest(search),
                "state": "search_failed",
            }, [])
        matches = search.get("matches")
        if (
            set(search) != common
            or type(search.get("response_bytes")) is not int
            or not 1 <= search["response_bytes"] <= MAX_PROVIDER_BYTES
            or HEX64.fullmatch(search.get("response_sha256", "")) is None
            or not isinstance(matches, list)
        ):
            raise ValueError("stored observed Baseten search is invalid")
        for match in matches:
            if not isinstance(match, dict) or set(match) != EXECUTION_FIELDS:
                raise ValueError("stored Baseten search match is invalid")
            _validate_execution_record(
                {**match, "ordinal": ordinal},
                self.project_id,
                definition_entry["name"],
                ordinal,
            )
        if not matches:
            return ({
                "ordinal": ordinal,
                "execution_name": definition_entry["name"],
                "observed_at": search["observed_at"],
                "search_result_sha256": _digest(search),
                "state": "unresolved_create",
            }, [])
        if len(matches) != 1:
            duplicate_set = {
                "schema_version": "milk.baseten-duplicate-create-set.v1",
                "run_id": evidence.run_id,
                "ordinal": ordinal,
                "execution_run_id": definition_entry["execution_run_id"],
                "execution_name": definition_entry["name"],
                "search_observation_sha256": _digest(search),
                "observed_at": search["observed_at"],
                "matches": matches,
                "state": "duplicate_create",
            }
            evidence.json(f"duplicate-sets/{ordinal:02d}.json", duplicate_set)
            return self._load_duplicate_set(evidence, definition_entry), []
        resolved = {
            "schema_version": "milk.baseten-provider-create-resolution.v1",
            "run_id": evidence.run_id,
            "ordinal": ordinal,
            "state": "resolved",
            **matches[0],
        }
        evidence.json(f"resolved/{ordinal:02d}.json", resolved)
        return resolved, []

    def _terminal_observation(
        self,
        evidence,
        definition_entry,
        execution,
        *,
        duplicate=False,
        value=None,
    ):
        ordinal = definition_entry["ordinal"]
        execution_id = _identifier(
            execution.get("execution_id"), "Baseten training job ID"
        )
        relative = (
            f"duplicate-terminal-observations/{ordinal:02d}/{execution_id}.json"
            if duplicate
            else f"terminal-observations/{ordinal:02d}.json"
        )
        if value is not None:
            _validate_observation(
                value,
                evidence.run_id,
                definition_entry,
                self.project_id,
            )
            if value["status"] not in TERMINAL_STATUSES:
                raise ValueError("terminal observation has an active status")
            evidence.json(relative, value)
        try:
            stored = _strict_object(
                self.store.get(f"{evidence.prefix}/{relative}")
            )
        except FileNotFoundError:
            return None
        _validate_observation(
            stored,
            evidence.run_id,
            definition_entry,
            self.project_id,
        )
        if (
            stored["status"] not in TERMINAL_STATUSES
            or stored["execution_id"] != execution_id
            or stored["execution_name"] != execution["execution_name"]
            or stored["created_at"] != execution["created_at"]
        ):
            raise ValueError("stored terminal observation differs from its authority")
        digest = _digest(stored)
        digest_relative = (
            f"duplicate-observations/{ordinal:02d}/{execution_id}/{digest}.json"
            if duplicate
            else f"observations/{ordinal:02d}/{digest}.json"
        )
        evidence.json(digest_relative, stored)
        return stored

    def _collection_failure(self, evidence, ordinal, operation, observed_at, error):
        value = {
            "schema_version": "milk.provider-evidence-failure.v1",
            "run_id": evidence.run_id,
            "ordinal": ordinal,
            "operation": operation,
            "observed_at": observed_at,
            "error_type": type(error).__name__,
            "error_sha256": _error_digest(error),
        }
        try:
            prefix = f"{evidence.prefix}/evidence-failures/{ordinal:02d}/{operation}"
            keys = self.store.list(prefix, limit=MAX_EVIDENCE_FAILURES + 1)
            if len(keys) > MAX_EVIDENCE_FAILURES:
                raise ValueError("provider evidence failures exceed their bound")
            if len(keys) < MAX_EVIDENCE_FAILURES:
                evidence.json(
                    f"evidence-failures/{ordinal:02d}/{operation}/{len(keys):02d}.json",
                    value,
                )
            else:
                value["persistence_state"] = "attempt_bound_exhausted"
        except Exception as persistence_error:
            value["persistence_error_type"] = type(persistence_error).__name__
            value["persistence_error_sha256"] = _error_digest(persistence_error)
        return value

    def _provider_deadline(self, evidence, definition, definition_entry, execution):
        deadline = _utc(execution["created_at"], "Baseten created_at") + dt.timedelta(
            seconds=definition_entry["max_wall_seconds"]
        )
        try:
            intent = _strict_object(
                self.store.get(
                    f"{evidence.prefix}/create-intents/{definition_entry['ordinal']:02d}.json"
                )
            )
            deadline = min(deadline, _utc(intent["deadline"], "provider deadline"))
        except Exception:
            return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        deadline = min(deadline, _workload_expiry(definition["workload"]))
        return deadline

    def _load_cost_basis(self, evidence, definition):
        value = _strict_object(self.store.get(f"{evidence.prefix}/cost-basis.json"))
        items = value.get("executions")
        if (
            set(value)
            != {
                "schema_version",
                "run_id",
                "observed_at",
                "reservation_rate_microusd_per_minute",
                "executions",
                "accounted_minutes",
                "accounted_microusd",
            }
            or value.get("schema_version") != "milk.baseten-cost-basis.v1"
            or value.get("run_id") != evidence.run_id
            or value.get("reservation_rate_microusd_per_minute")
            != RESERVATION_RATE_MICROUSD_PER_MINUTE
            or not isinstance(items, list)
            or len(items) != len(definition["executions"])
        ):
            raise ValueError("stored Baseten cost basis is invalid")
        observed_at = _utc(value.get("observed_at"), "cost observation time")
        total_minutes = 0
        total_microusd = 0
        observed_item_keys = {
            "state",
            "ordinal",
            "execution_run_id",
            "observation_sha256",
            "max_wall_seconds",
            "elapsed_seconds",
            "billed_minutes",
            "accounted_microusd",
        } | EXECUTION_FIELDS
        duplicate_item_keys = {
            "state",
            "ordinal",
            "execution_run_id",
            "execution_name",
            "duplicate_set_sha256",
            "max_wall_seconds",
            "matches",
            "billed_minutes",
            "accounted_microusd",
        }
        duplicate_match_keys = {
            "observation_sha256",
            "elapsed_seconds",
            "billed_minutes",
            "accounted_microusd",
        } | EXECUTION_FIELDS
        for definition_entry, item in zip(definition["executions"], items):
            if (
                not isinstance(item, dict)
                or item.get("ordinal") != definition_entry["ordinal"]
                or item.get("execution_run_id")
                != definition_entry["execution_run_id"]
                or item.get("max_wall_seconds")
                != definition_entry["max_wall_seconds"]
            ):
                raise ValueError("stored Baseten cost execution is invalid")
            if item.get("state") == "duplicate_create":
                duplicate_set = self._load_duplicate_set(evidence, definition_entry)
                matches = item.get("matches")
                if (
                    set(item) != duplicate_item_keys
                    or item.get("execution_name") != definition_entry["name"]
                    or item.get("duplicate_set_sha256") != _digest(duplicate_set)
                    or not isinstance(matches, list)
                    or len(matches) != len(duplicate_set["matches"])
                    or item.get("billed_minutes")
                    != sum(match.get("billed_minutes", -1) for match in matches)
                    or item.get("accounted_microusd")
                    != sum(match.get("accounted_microusd", -1) for match in matches)
                ):
                    raise ValueError("stored duplicate-create cost is invalid")
                intent = _strict_object(
                    self.store.get(
                        f"{evidence.prefix}/create-intents/"
                        f"{definition_entry['ordinal']:02d}.json"
                    )
                )
                requested = _utc(intent.get("requested_at"), "provider requested_at")
                skew = dt.timedelta(seconds=PROVIDER_CLOCK_SKEW_SECONDS)
                for authority, match in zip(duplicate_set["matches"], matches):
                    if (
                        not isinstance(match, dict)
                        or set(match) != duplicate_match_keys
                        or match.get("execution_id") != authority["execution_id"]
                        or match.get("created_at") != authority["created_at"]
                        or HEX64.fullmatch(match.get("observation_sha256", ""))
                        is None
                        or match.get("status") not in TERMINAL_STATUSES
                    ):
                        raise ValueError("stored duplicate-create cost match is invalid")
                    observation = _strict_object(
                        self.store.get(
                            f"{evidence.prefix}/duplicate-observations/"
                            f"{definition_entry['ordinal']:02d}/"
                            f"{match['execution_id']}/"
                            f"{match['observation_sha256']}.json"
                        )
                    )
                    _validate_observation(
                        observation,
                        evidence.run_id,
                        definition_entry,
                        self.project_id,
                    )
                    if (
                        _digest(observation) != match["observation_sha256"]
                        or any(
                            match[field] != observation[field]
                            for field in EXECUTION_FIELDS
                        )
                    ):
                        raise ValueError(
                            "stored duplicate-create cost observation differs"
                        )
                    created = _utc(match["created_at"], "Baseten created_at")
                    updated = _utc(match["updated_at"], "Baseten updated_at")
                    if (
                        created < requested - skew
                        or created > observed_at + skew
                        or updated > observed_at + skew
                    ):
                        raise ValueError(
                            "duplicate Baseten timestamps exceed their authority window"
                        )
                    elapsed = max(1, math.ceil((updated - created).total_seconds()))
                    billed_minutes = _minutes(elapsed)
                    accounted = (
                        billed_minutes * RESERVATION_RATE_MICROUSD_PER_MINUTE
                    )
                    if (
                        match.get("elapsed_seconds") != elapsed
                        or match.get("billed_minutes") != billed_minutes
                        or match.get("accounted_microusd") != accounted
                    ):
                        raise ValueError(
                            "stored duplicate-create cost calculation is invalid"
                        )
                total_minutes += item["billed_minutes"]
                total_microusd += item["accounted_microusd"]
                continue
            if (
                item.get("state") != "observed_terminal"
                or set(item) != observed_item_keys
                or HEX64.fullmatch(item.get("observation_sha256", "")) is None
                or item.get("status") not in TERMINAL_STATUSES
            ):
                raise ValueError("stored Baseten cost execution is invalid")
            observation = _strict_object(
                self.store.get(
                    f"{evidence.prefix}/observations/"
                    f"{definition_entry['ordinal']:02d}/"
                    f"{item['observation_sha256']}.json"
                )
            )
            _validate_observation(
                observation,
                evidence.run_id,
                definition_entry,
                self.project_id,
            )
            if (
                _digest(observation) != item["observation_sha256"]
                or any(item[field] != observation[field] for field in EXECUTION_FIELDS)
            ):
                raise ValueError("stored Baseten cost observation differs")
            intent = _strict_object(
                self.store.get(
                    f"{evidence.prefix}/create-intents/"
                    f"{definition_entry['ordinal']:02d}.json"
                )
            )
            requested = _utc(intent.get("requested_at"), "provider requested_at")
            created = _utc(item["created_at"], "Baseten created_at")
            updated = _utc(item["updated_at"], "Baseten updated_at")
            skew = dt.timedelta(seconds=PROVIDER_CLOCK_SKEW_SECONDS)
            if created < requested - skew or created > observed_at + skew or updated > observed_at + skew:
                raise ValueError("Baseten provider timestamps exceed their authority window")
            elapsed = max(1, math.ceil((updated - created).total_seconds()))
            billed_minutes = _minutes(elapsed)
            accounted = billed_minutes * RESERVATION_RATE_MICROUSD_PER_MINUTE
            if (
                item.get("elapsed_seconds") != elapsed
                or item.get("billed_minutes") != billed_minutes
                or item.get("accounted_microusd") != accounted
            ):
                raise ValueError("stored Baseten cost calculation is invalid")
            total_minutes += billed_minutes
            total_microusd += accounted
        if (
            value.get("accounted_minutes") != total_minutes
            or value.get("accounted_microusd") != total_microusd
        ):
            raise ValueError("stored Baseten cost totals are invalid")
        return value

    def _cost_basis(self, evidence, definition, executions, observed_at):
        try:
            return self._load_cost_basis(evidence, definition)
        except FileNotFoundError:
            pass
        if len(executions) != len(definition["executions"]):
            raise ValueError("terminal executions are incomplete")
        items = []
        total_minutes = 0
        for definition_entry, observation in zip(definition["executions"], executions):
            if observation.get("state") == "duplicate_create_terminal":
                duplicate_set = self._load_duplicate_set(evidence, definition_entry)
                if (
                    set(observation)
                    != {
                        "ordinal",
                        "execution_run_id",
                        "execution_name",
                        "duplicate_set_sha256",
                        "matches",
                        "state",
                    }
                    or observation.get("ordinal") != definition_entry["ordinal"]
                    or observation.get("execution_run_id")
                    != definition_entry["execution_run_id"]
                    or observation.get("execution_name") != definition_entry["name"]
                    or observation.get("duplicate_set_sha256")
                    != _digest(duplicate_set)
                    or not isinstance(observation.get("matches"), list)
                    or len(observation["matches"]) != len(duplicate_set["matches"])
                ):
                    raise ValueError("terminal duplicate-create executions are invalid")
                match_costs = []
                duplicate_minutes = 0
                local_observed = _utc(observed_at, "cost observation time")
                intent = _strict_object(
                    self.store.get(
                        f"{evidence.prefix}/create-intents/"
                        f"{definition_entry['ordinal']:02d}.json"
                    )
                )
                requested = _utc(intent.get("requested_at"), "provider requested_at")
                skew = dt.timedelta(seconds=PROVIDER_CLOCK_SKEW_SECONDS)
                for authority, match in zip(
                    duplicate_set["matches"], observation["matches"]
                ):
                    if "stop_state" in match:
                        raise ValueError("terminal duplicate execution still has stop state")
                    _validate_observation(
                        match,
                        evidence.run_id,
                        definition_entry,
                        self.project_id,
                    )
                    if (
                        match["execution_id"] != authority["execution_id"]
                        or match["created_at"] != authority["created_at"]
                        or match["status"] not in TERMINAL_STATUSES
                    ):
                        raise ValueError("terminal duplicate execution differs")
                    created = _utc(match["created_at"], "Baseten created_at")
                    updated = _utc(match["updated_at"], "Baseten updated_at")
                    if (
                        created < requested - skew
                        or created > local_observed + skew
                        or updated > local_observed + skew
                    ):
                        raise ValueError(
                            "duplicate Baseten timestamps exceed their authority window"
                        )
                    elapsed = max(1, math.ceil((updated - created).total_seconds()))
                    billed_minutes = _minutes(elapsed)
                    accounted = (
                        billed_minutes * RESERVATION_RATE_MICROUSD_PER_MINUTE
                    )
                    match_costs.append(
                        {
                            "observation_sha256": _digest(match),
                            **{field: match[field] for field in EXECUTION_FIELDS},
                            "elapsed_seconds": elapsed,
                            "billed_minutes": billed_minutes,
                            "accounted_microusd": accounted,
                        }
                    )
                    duplicate_minutes += billed_minutes
                items.append(
                    {
                        "state": "duplicate_create",
                        "ordinal": definition_entry["ordinal"],
                        "execution_run_id": definition_entry["execution_run_id"],
                        "execution_name": definition_entry["name"],
                        "duplicate_set_sha256": _digest(duplicate_set),
                        "max_wall_seconds": definition_entry["max_wall_seconds"],
                        "matches": match_costs,
                        "billed_minutes": duplicate_minutes,
                        "accounted_microusd": duplicate_minutes
                        * RESERVATION_RATE_MICROUSD_PER_MINUTE,
                    }
                )
                total_minutes += duplicate_minutes
                continue
            _validate_observation(
                observation,
                evidence.run_id,
                definition_entry,
                self.project_id,
            )
            if observation["status"] not in TERMINAL_STATUSES:
                raise ValueError("cost basis requires terminal executions")
            created = _utc(observation["created_at"], "Baseten created_at")
            updated = _utc(observation["updated_at"], "Baseten updated_at")
            intent = _strict_object(
                self.store.get(
                    f"{evidence.prefix}/create-intents/"
                    f"{definition_entry['ordinal']:02d}.json"
                )
            )
            requested = _utc(intent.get("requested_at"), "provider requested_at")
            local_observed = _utc(observed_at, "cost observation time")
            skew = dt.timedelta(seconds=PROVIDER_CLOCK_SKEW_SECONDS)
            if (
                created < requested - skew
                or created > local_observed + skew
                or updated > local_observed + skew
            ):
                raise ValueError(
                    "Baseten provider timestamps exceed their authority window"
                )
            elapsed = max(1, math.ceil((updated - created).total_seconds()))
            billed_minutes = _minutes(elapsed)
            accounted = billed_minutes * RESERVATION_RATE_MICROUSD_PER_MINUTE
            items.append(
                {
                    "state": "observed_terminal",
                    "ordinal": definition_entry["ordinal"],
                    "execution_run_id": definition_entry["execution_run_id"],
                    "observation_sha256": _digest(observation),
                    **{field: observation[field] for field in EXECUTION_FIELDS},
                    "max_wall_seconds": definition_entry["max_wall_seconds"],
                    "elapsed_seconds": elapsed,
                    "billed_minutes": billed_minutes,
                    "accounted_microusd": accounted,
                }
            )
            total_minutes += billed_minutes
        value = {
            "schema_version": "milk.baseten-cost-basis.v1",
            "run_id": evidence.run_id,
            "observed_at": observed_at,
            "reservation_rate_microusd_per_minute": RESERVATION_RATE_MICROUSD_PER_MINUTE,
            "executions": items,
            "accounted_minutes": total_minutes,
            "accounted_microusd": total_minutes
            * RESERVATION_RATE_MICROUSD_PER_MINUTE,
        }
        evidence.json("cost-basis.json", value)
        return self._load_cost_basis(evidence, definition)

    def reconcile(self, run_id):
        evidence, definition = self._load_definition(run_id)
        try:
            terminal = self._load_terminal(evidence, definition)
            self._finish_terminal(evidence, terminal)
            return terminal
        except FileNotFoundError:
            pass
        budget = self.budget.status()
        outstanding = budget["outstanding"].get(run_id)
        if outstanding is None:
            raise ValueError("unterminated run is absent from the outstanding-work set")
        try:
            preparation = self._load_preparation(evidence, definition)
        except FileNotFoundError:
            if int(self.now().timestamp()) < outstanding[
                "preparation_deadline_epoch_seconds"
            ]:
                return {
                    "schema_version": "milk.baseten-reconciliation.v1",
                    "run_id": run_id,
                    "state": "preparing",
                    "observed_at": _utc_text(self.now()),
                    "evidence_complete": False,
                    "collection_failures": [],
                    "executions": [],
                }
            return self._terminalize_unstarted(evidence, definition)
        if preparation["provider_deadline_epoch_seconds"] != outstanding[
            "provider_deadline_epoch_seconds"
        ] or _digest(preparation) != outstanding["preparation_sha256"]:
            raise ValueError("launch-group preparation differs from budget authority")
        try:
            intents_complete = self._load_intents_complete(
                evidence, definition, preparation
            )
        except FileNotFoundError:
            if self.now() < _utc(
                preparation["preparation_deadline"], "preparation deadline"
            ):
                return {
                    "schema_version": "milk.baseten-reconciliation.v1",
                    "run_id": run_id,
                    "state": "preparing",
                    "observed_at": _utc_text(self.now()),
                    "evidence_complete": True,
                    "collection_failures": [],
                    "executions": [
                        {
                            "ordinal": item["ordinal"],
                            "execution_name": item["name"],
                            "state": "preparing",
                        }
                        for item in definition["executions"]
                    ],
                }
            return self._terminalize_unstarted(evidence, definition)
        try:
            abort = _strict_object(self.store.get(f"{evidence.prefix}/abort.json"))
            if (
                set(abort)
                != {
                    "schema_version",
                    "run_id",
                    "observed_at",
                    "failed_ordinals",
                    "state",
                }
                or abort.get("schema_version") != "milk.baseten-launch-group-abort.v1"
                or abort.get("run_id") != run_id
                or abort.get("state") != "aborting"
                or not isinstance(abort.get("failed_ordinals"), list)
                or any(
                    type(ordinal) is not int
                    or not 0 <= ordinal < len(definition["executions"])
                    for ordinal in abort["failed_ordinals"]
                )
            ):
                raise ValueError("launch-group abort marker is invalid")
            group_aborted = True
        except FileNotFoundError:
            group_aborted = False
        if not group_aborted and len(definition["executions"]) > 1:
            for item in definition["executions"]:
                ordinal = item["ordinal"]
                try:
                    create_result = _strict_object(
                        self.store.get(
                            f"{evidence.prefix}/create-results/{ordinal:02d}.json"
                        )
                    )
                    if create_result.get("state") == "ambiguous_create":
                        group_aborted = True
                except FileNotFoundError:
                    pass
                for attempt in range(MAX_STOP_ATTEMPTS):
                    try:
                        self.store.get(
                            f"{evidence.prefix}/stop-attempts/{ordinal:02d}/"
                            f"{attempt:02d}-intent.json"
                        )
                        group_aborted = True
                        break
                    except FileNotFoundError:
                        pass
        observed_at = _utc_text(self.now())
        executions = []
        all_terminal = True
        collection_failures = []
        for definition_entry in definition["executions"]:
            execution, resolution_failures = self._resolve_execution(
                evidence,
                definition_entry,
                intents_complete,
            )
            collection_failures.extend(resolution_failures)
            if execution.get("state") == "not_authorized":
                executions.append(execution)
                continue
            if execution.get("state") in {"preparing", "create_in_flight"}:
                all_terminal = False
                executions.append(execution)
                continue
            if execution.get("state") in {
                "unresolved_create",
                "search_observation_ambiguous",
                "search_failed",
            }:
                if len(definition["executions"]) > 1 and not group_aborted:
                    group_aborted = True
                    try:
                        evidence.json(
                            "abort.json",
                            {
                                "schema_version": "milk.baseten-launch-group-abort.v1",
                                "run_id": run_id,
                                "observed_at": observed_at,
                                "failed_ordinals": [definition_entry["ordinal"]],
                                "state": "aborting",
                            },
                        )
                    except Exception as error:
                        collection_failures.append(
                            self._collection_failure(
                                evidence,
                                definition_entry["ordinal"],
                                "unresolved_create_abort",
                                observed_at,
                                error,
                            )
                        )
                all_terminal = False
                executions.append(execution)
                continue
            if execution.get("state") == "duplicate_create":
                group_aborted = True
                abort = {
                    "schema_version": "milk.baseten-launch-group-abort.v1",
                    "run_id": run_id,
                    "observed_at": execution["observed_at"],
                    "failed_ordinals": [definition_entry["ordinal"]],
                    "state": "aborting",
                }
                try:
                    evidence.json("abort.json", abort)
                except Exception as error:
                    collection_failures.append(
                        self._collection_failure(
                            evidence,
                            definition_entry["ordinal"],
                            "duplicate_create_abort",
                            observed_at,
                            error,
                        )
                    )
                matches = []
                duplicates_terminal = True
                authorities = [
                    {"ordinal": definition_entry["ordinal"], **match}
                    for match in execution["matches"]
                ]
                terminal_by_id = {}
                pending = []
                for authority in authorities:
                    terminal_observation = self._terminal_observation(
                        evidence,
                        definition_entry,
                        authority,
                        duplicate=True,
                    )
                    if terminal_observation is None:
                        pending.append(authority)
                    else:
                        terminal_by_id[authority["execution_id"]] = terminal_observation
                stop_ids = {
                    item["execution_id"]
                    for item in sorted(
                        pending,
                        key=lambda item: (
                            self._stop_attempts_used(
                                evidence, item, duplicate=True
                            ),
                            item["execution_id"],
                        ),
                    )[:MAX_DUPLICATE_EXECUTIONS]
                }
                for stored in authorities:
                    terminal_observation = terminal_by_id.get(stored["execution_id"])
                    if terminal_observation is not None:
                        matches.append(terminal_observation)
                        continue
                    try:
                        raw = self._request(
                            "GET",
                            f"/training_projects/{self.project_id}/jobs/"
                            f"{stored['execution_id']}",
                        )
                        observed = _execution_from_get_envelope(
                            raw,
                            self.project_id,
                            definition_entry["name"],
                            stored["execution_id"],
                        )
                        if (
                            observed["created_at"] != stored["created_at"]
                            or _utc(observed["updated_at"], "Baseten updated_at")
                            < _utc(stored["updated_at"], "prior Baseten updated_at")
                        ):
                            raise ValueError(
                                "duplicate Baseten timestamps differ from their authority"
                            )
                        persisted = {
                            "schema_version": "milk.baseten-provider-observation.v1",
                            "run_id": run_id,
                            "ordinal": definition_entry["ordinal"],
                            "observed_at": observed_at,
                            "response_bytes": len(raw),
                            "response_sha256": hashlib.sha256(raw).hexdigest(),
                            **observed,
                        }
                        _validate_observation(
                            persisted,
                            run_id,
                            definition_entry,
                            self.project_id,
                        )
                    except Exception as error:
                        duplicates_terminal = False
                        stored["stop_state"] = (
                            self._stop_once(
                                evidence,
                                stored,
                                duplicate=True,
                            )
                            if stored["execution_id"] in stop_ids
                            else "deferred_operation_cap"
                        )
                        collection_failures.append(
                            self._collection_failure(
                                evidence,
                                definition_entry["ordinal"],
                                "duplicate_status",
                                observed_at,
                                error,
                            )
                        )
                        matches.append(stored)
                        continue
                    if persisted["status"] not in TERMINAL_STATUSES:
                        duplicates_terminal = False
                        persisted["stop_state"] = (
                            self._stop_once(
                                evidence,
                                persisted,
                                duplicate=True,
                            )
                            if persisted["execution_id"] in stop_ids
                            else "deferred_operation_cap"
                        )
                    else:
                        try:
                            persisted = self._terminal_observation(
                                evidence,
                                definition_entry,
                                stored,
                                duplicate=True,
                                value=persisted,
                            )
                        except Exception as error:
                            duplicates_terminal = False
                            collection_failures.append(
                                self._collection_failure(
                                    evidence,
                                    definition_entry["ordinal"],
                                    "duplicate_status_observation",
                                    observed_at,
                                    error,
                                )
                            )
                    matches.append(persisted)
                if not duplicates_terminal:
                    all_terminal = False
                    executions.append({**execution, "matches": matches})
                    continue
                executions.append(
                    {
                        "ordinal": definition_entry["ordinal"],
                        "execution_run_id": definition_entry["execution_run_id"],
                        "execution_name": definition_entry["name"],
                        "duplicate_set_sha256": _digest(execution),
                        "matches": matches,
                        "state": "duplicate_create_terminal",
                    }
                )
                continue
            terminal_observation = self._terminal_observation(
                evidence,
                definition_entry,
                execution,
            )
            if terminal_observation is not None:
                executions.append(terminal_observation)
                continue
            try:
                raw = self._request(
                    "GET",
                    f"/training_projects/{self.project_id}/jobs/{execution['execution_id']}",
                )
                observation = _execution_from_get_envelope(
                    raw,
                    self.project_id,
                    definition_entry["name"],
                    execution["execution_id"],
                )
                if (
                    observation["created_at"] != execution["created_at"]
                    or _utc(observation["updated_at"], "Baseten updated_at")
                    < _utc(execution["updated_at"], "prior Baseten updated_at")
                ):
                    raise ValueError(
                        "Baseten status timestamps differ from the create authority"
                    )
                persisted_observation = {
                    "schema_version": "milk.baseten-provider-observation.v1",
                    "run_id": run_id,
                    "ordinal": definition_entry["ordinal"],
                    "observed_at": observed_at,
                    "response_bytes": len(raw),
                    "response_sha256": hashlib.sha256(raw).hexdigest(),
                    **observation,
                }
                _validate_observation(
                    persisted_observation,
                    run_id,
                    definition_entry,
                    self.project_id,
                )
            except Exception as error:
                all_terminal = False
                failed = {
                    "ordinal": definition_entry["ordinal"],
                    "execution_id": execution["execution_id"],
                    "execution_name": definition_entry["name"],
                    "state": "status_unavailable",
                }
                if group_aborted or self.now() >= self._provider_deadline(
                    evidence, definition, definition_entry, execution
                ):
                    failed["stop_state"] = self._stop_once(evidence, failed)
                collection_failures.append(
                    self._collection_failure(
                        evidence,
                        definition_entry["ordinal"],
                        "status",
                        observed_at,
                        error,
                    )
                )
                executions.append(failed)
                continue
            stop_state = None
            if persisted_observation["status"] not in TERMINAL_STATUSES:
                all_terminal = False
                if group_aborted or self.now() >= self._provider_deadline(
                    evidence,
                    definition,
                    definition_entry,
                    persisted_observation,
                ):
                    stop_state = self._stop_once(evidence, persisted_observation)
            if persisted_observation["status"] in TERMINAL_STATUSES:
                try:
                    persisted_observation = self._terminal_observation(
                        evidence,
                        definition_entry,
                        execution,
                        value=persisted_observation,
                    )
                except Exception as error:
                    collection_failures.append(
                        self._collection_failure(
                            evidence,
                            definition_entry["ordinal"],
                            "status_observation",
                            observed_at,
                            error,
                        )
                    )
            observation = (
                persisted_observation
                if stop_state is None
                else {**persisted_observation, "stop_state": stop_state}
            )
            executions.append(observation)
        if not all_terminal:
            return {
                "schema_version": "milk.baseten-reconciliation.v1",
                "run_id": run_id,
                "state": "active_or_ambiguous",
                "observed_at": observed_at,
                "evidence_complete": not collection_failures,
                "collection_failures": collection_failures,
                "executions": executions,
            }
        if collection_failures:
            return {
                "schema_version": "milk.baseten-reconciliation.v1",
                "run_id": run_id,
                "state": "terminal_evidence_pending",
                "observed_at": observed_at,
                "evidence_complete": False,
                "collection_failures": collection_failures,
                "executions": executions,
            }
        cost_basis = self._cost_basis(
            evidence,
            definition,
            executions,
            observed_at,
        )
        executions = []
        for definition_entry, cost_item in zip(
            definition["executions"],
            cost_basis["executions"],
        ):
            if cost_item["state"] == "duplicate_create":
                executions.append(
                    {
                        "ordinal": definition_entry["ordinal"],
                        "execution_run_id": definition_entry["execution_run_id"],
                        "execution_name": definition_entry["name"],
                        "duplicate_set_sha256": cost_item["duplicate_set_sha256"],
                        "matches": [
                            _strict_object(
                                self.store.get(
                                    f"{evidence.prefix}/duplicate-observations/"
                                    f"{definition_entry['ordinal']:02d}/"
                                    f"{match['execution_id']}/"
                                    f"{match['observation_sha256']}.json"
                                )
                            )
                            for match in cost_item["matches"]
                        ],
                        "state": "duplicate_create_terminal",
                    }
                )
                continue
            executions.append(
                _strict_object(
                    self.store.get(
                        f"{evidence.prefix}/observations/"
                        f"{definition_entry['ordinal']:02d}/"
                        f"{cost_item['observation_sha256']}.json"
                    )
                )
            )
        total_minutes = cost_basis["accounted_minutes"]
        accounted_microusd = cost_basis["accounted_microusd"]
        budget = self.budget.status()
        if run_id in budget["open"] or run_id in budget["pending_settlements"]:
            settlement = self.budget.settle(run_id, accounted_microusd)
            settlement_sha256 = _digest(settlement)
            terminal_state = "terminal"
        else:
            settlement = self.budget.settlement(run_id)
            if settlement is not None:
                if settlement["accounted_microusd"] != accounted_microusd:
                    raise ValueError("provider execution cost differs from its settlement")
                settlement_sha256 = _digest(settlement)
                terminal_state = "terminal"
            else:
                if accounted_microusd != 0:
                    raise ValueError("provider execution has no budget reservation")
                settlement_sha256 = None
                terminal_state = "not_authorized"
        observed_executions = [
            item
            for item in cost_basis["executions"]
            if item["state"] == "observed_terminal"
        ]
        if observed_executions:
            log_collection = collect_baseten_terminal_logs(
                evidence=evidence,
                request=self._request,
                project_id=self.project_id,
                executions=observed_executions,
                observed_at=cost_basis["observed_at"],
                now=self.now,
                continue_collection=lambda: self._reconcile_deadline is None
                or time.monotonic() < self._reconcile_deadline,
            )
        else:
            log_collection = {
                "schema_version": "milk.baseten-log-collection-unavailable.v1",
                "run_id": run_id,
                "reason": "no_provider_execution_resolved",
                "ordinals": [
                    item["ordinal"] for item in cost_basis["executions"]
                ],
                "source_complete": False,
            }
            evidence.json("log-collections/unavailable.json", log_collection)
        if log_collection.get("schema_version") == "milk.baseten-log-collection-pending.v1":
            return {
                "schema_version": "milk.baseten-reconciliation.v1",
                "run_id": run_id,
                "state": "terminal_evidence_pending",
                "observed_at": observed_at,
                "evidence_complete": False,
                "collection_failures": collection_failures,
                "cost_basis_sha256": _digest(cost_basis),
                "log_collection": log_collection,
                "accounted_minutes": total_minutes,
                "accounted_microusd": accounted_microusd,
                "settlement_sha256": settlement_sha256,
                "executions": executions,
            }
        log_collection_sha256 = _digest(log_collection)
        logs_source_complete = (
            log_collection["source_complete"] is True
            and len(observed_executions) == len(cost_basis["executions"])
        )
        try:
            metric_collection = collect_baseten_terminal_metrics(
                evidence=evidence,
                request=self._request,
                project_id=self.project_id,
                executions=cost_basis["executions"],
                observed_at=cost_basis["observed_at"],
                now=self.now,
                continue_collection=lambda: self._reconcile_deadline is None
                or time.monotonic() < self._reconcile_deadline,
            )
        except Exception as error:
            collection_failures.append(
                self._collection_failure(
                    evidence,
                    0,
                    "metrics",
                    cost_basis["observed_at"],
                    error,
                )
            )
            metric_collection = None
        if (
            metric_collection is not None
            and metric_collection.get("schema_version")
            == "milk.baseten-metric-collection-pending.v1"
        ):
            return {
                "schema_version": "milk.baseten-reconciliation.v1",
                "run_id": run_id,
                "state": "terminal_evidence_pending",
                "observed_at": observed_at,
                "evidence_complete": False,
                "collection_failures": collection_failures,
                "cost_basis_sha256": _digest(cost_basis),
                "log_collection_sha256": log_collection_sha256,
                "logs_source_complete": logs_source_complete,
                "metric_collection": metric_collection,
                "accounted_minutes": total_minutes,
                "accounted_microusd": accounted_microusd,
                "settlement_sha256": settlement_sha256,
                "executions": executions,
            }
        metric_collection_sha256 = (
            None if metric_collection is None else _digest(metric_collection)
        )
        metrics_source_complete = (
            False
            if metric_collection is None
            else metric_collection["source_complete"] is True
        )
        if collection_failures:
            return {
                "schema_version": "milk.baseten-reconciliation.v1",
                "run_id": run_id,
                "state": "terminal_evidence_pending",
                "observed_at": observed_at,
                "evidence_complete": False,
                "collection_failures": collection_failures,
                "cost_basis_sha256": _digest(cost_basis),
                "log_collection_sha256": log_collection_sha256,
                "logs_source_complete": logs_source_complete,
                "metric_collection_sha256": metric_collection_sha256,
                "metrics_source_complete": metrics_source_complete,
                "accounted_minutes": total_minutes,
                "accounted_microusd": accounted_microusd,
                "settlement_sha256": settlement_sha256,
                "executions": executions,
            }
        terminal = {
            "schema_version": "milk.baseten-launch-group-terminal.v1",
            "run_id": run_id,
            "state": terminal_state,
            "observed_at": observed_at,
            "evidence_complete": not collection_failures,
            "collection_failures": collection_failures,
            "cost_basis_sha256": _digest(cost_basis),
            "log_collection_sha256": log_collection_sha256,
            "logs_source_complete": logs_source_complete,
            "metric_collection_sha256": metric_collection_sha256,
            "metrics_source_complete": metrics_source_complete,
            "accounted_minutes": total_minutes,
            "accounted_microusd": accounted_microusd,
            "settlement_sha256": settlement_sha256,
            "executions": executions,
        }
        evidence.json("terminal.json", terminal)
        self._finish_terminal(evidence, terminal)
        return terminal

    def reconcile_all(self, timeout_seconds=300):
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3_600:
            raise ValueError("reconciliation timeout is invalid")
        if self._reconcile_deadline is not None:
            raise RuntimeError("reconciliation pass is already running")
        self._reconcile_deadline = time.monotonic() + timeout_seconds
        try:
            return self._reconcile_all_bounded()
        finally:
            self._reconcile_deadline = None

    def _reconcile_cursor(self):
        key = (
            f"state/v1/campaigns/{self.campaign_id}/providers/"
            "baseten/reconcile-cursor.json"
        )
        try:
            raw, etag = self.store.get_versioned(key)
        except FileNotFoundError:
            return key, None, None
        value = _strict_object(raw)
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "provider_identity",
                "after_deadline_epoch_seconds",
                "after_run_id",
            }
            or value.get("schema_version") != "milk.baseten-reconcile-cursor.v2"
            or value.get("campaign_id") != self.campaign_id
            or value.get("provider_identity")
            != baseten_provider_identity(self.project_id)
            or type(value.get("after_deadline_epoch_seconds")) is not int
            or value["after_deadline_epoch_seconds"] <= 0
            or HEX64.fullmatch(value.get("after_run_id", "")) is None
        ):
            raise ValueError("stored reconciliation cursor is invalid")
        return (
            key,
            (value["after_deadline_epoch_seconds"], value["after_run_id"]),
            etag,
        )

    def _advance_reconcile_cursor(self, key, etag, deadline, run_id):
        value = {
            "schema_version": "milk.baseten-reconcile-cursor.v2",
            "campaign_id": self.campaign_id,
            "provider_identity": baseten_provider_identity(self.project_id),
            "after_deadline_epoch_seconds": deadline,
            "after_run_id": run_id,
        }
        body = canonical_json(value)
        advanced = (
            self.store.create(key, body, "application/json")
            if etag is None
            else self.store.replace(key, body, etag, "application/json")
        )
        if not advanced:
            raise RuntimeError("reconciliation cursor CAS contention")

    def _reconcile_all_bounded(self):
        budget = self.budget.status()
        identity = baseten_provider_identity(self.project_id)
        outstanding = {
            run_id: entry
            for run_id, entry in budget["outstanding"].items()
            if entry["provider_identity"] == identity
        }

        def deadline(run_id):
            entry = outstanding[run_id]
            if entry["state"] == "preparing":
                return entry["preparation_deadline_epoch_seconds"]
            return entry["provider_deadline_epoch_seconds"]

        candidates = sorted(outstanding, key=lambda run_id: (deadline(run_id), run_id))
        cursor_key, cursor, cursor_etag = self._reconcile_cursor()
        start = next(
            (
                index
                for index, run_id in enumerate(candidates)
                if cursor is not None and (deadline(run_id), run_id) > cursor
            ),
            0,
        )
        rotated = candidates[start:] + candidates[:start]
        selected = rotated[:MAX_RECONCILE_GROUPS]
        results = []
        processed = 0
        last_run_id = None
        for run_id in selected:
            if time.monotonic() >= self._reconcile_deadline:
                break
            try:
                results.append(self.reconcile(run_id))
            except Exception as error:
                results.append(
                    {
                        "schema_version": "milk.baseten-reconciliation-failure.v1",
                        "run_id": run_id,
                        "state": "reconciliation_error",
                        "evidence_complete": False,
                        "error_type": type(error).__name__,
                        "error_sha256": _error_digest(error),
                    }
                )
            processed += 1
            last_run_id = run_id
        if last_run_id is not None:
            self._advance_reconcile_cursor(
                cursor_key,
                cursor_etag,
                deadline(last_run_id),
                last_run_id,
            )
        return {
            "schema_version": "milk.baseten-reconciliation-pass.v1",
            "campaign_id": self.campaign_id,
            "processed": processed,
            "backlog": len(candidates) > processed,
            "evidence_complete": all(
                item.get("evidence_complete", True) for item in results
            ),
            "results": results,
        }


def _workload_and_entries(launch, settings):
    if (
        not isinstance(launch, dict)
        or set(launch)
        != {"launch_source", "runtime_image_reference", "operation"}
    ):
        raise ValueError("verified GPU launch is invalid")
    operation = launch["operation"]
    _validate_gpu_operation(operation)
    launch_source = launch["launch_source"]
    _validate_launch_source(launch_source)
    if operation["kind"] == "teacher_run":
        if launch["runtime_image_reference"] != settings["teacher_image"]:
            raise ValueError("teacher launch runtime image differs from configured image")
        name, payload = adapter._teacher_request_body(settings, operation)
        return (
            {
                "type": "teacher_run",
                "teacher_run_id": operation["teacher_run_id"],
                "provider_binding_sha256": operation[
                    "provider_binding_sha256"
                ],
                "slot": operation["slot"],
                "call_count": operation["call_count"],
                "max_gpu_seconds": operation["max_gpu_seconds"],
                "launch_source": launch_source,
                "image_admission_sha256": settings[
                    "teacher_image_admission_sha256"
                ],
                "image_release_sha256": settings["image_release_sha256"],
            },
            [
                {
                    "name": name,
                    "payload": payload,
                    "max_wall_seconds": operation["max_gpu_seconds"]
                    + adapter.TEACHER_TERMINALIZATION_GRACE_SECONDS,
                }
            ],
        )
    student_job_id = operation["student_job_id"]
    if operation["kind"] == "student_train_merge":
        if launch["runtime_image_reference"] != settings["student_train_image"]:
            raise ValueError("student train launch image differs from configured image")
        name, payload = adapter._request_body(settings, student_job_id)
        return (
            {
                "type": "student_train_merge",
                "student_job_id": student_job_id,
                "counts": operation["counts"],
                "max_gpu_seconds": operation["max_gpu_seconds"],
                "launch_source": launch_source,
                "image_admission_sha256": settings[
                    "student_train_image_admission_sha256"
                ],
                "image_release_sha256": settings["image_release_sha256"],
            },
            [
                {
                    "name": name,
                    "payload": payload,
                    "max_wall_seconds": adapter.OUTER_TIMEOUT_SECONDS,
                }
            ],
        )
    if launch["runtime_image_reference"] != settings["student_branch_image"]:
        raise ValueError("student branch launch image differs from configured image")
    entries = []
    for branch in operation["branches"]:
        name, payload = adapter._request_body(
            settings,
            student_job_id,
            branch["variant"],
            branch["branch_id"],
        )
        entries.append(
            {
                "name": name,
                "payload": payload,
                "max_wall_seconds": adapter.OUTER_TIMEOUT_SECONDS,
            }
        )
    return (
        {
            "type": "student_fanout",
            "student_job_id": student_job_id,
            "max_total_gpu_seconds": operation["max_total_gpu_seconds"],
            "branches": operation["branches"],
            "launch_source": launch_source,
            "image_admission_sha256": settings[
                "student_branch_image_admission_sha256"
            ],
            "image_release_sha256": settings["image_release_sha256"],
        },
        entries,
    )


def _winner_values(launch, values):
    operation = launch["operation"]
    if not isinstance(values, dict):
        raise ValueError("Baseten winner values are invalid")
    expected = winner_contract.settings(
        values.get("student_branch_image"),
        values.get("student_job_id"),
        values.get("model_alias"),
        values.get("registry_secret"),
        values.get("config_secret"),
        values.get("control_store_access_key_secret"),
        values.get("control_store_secret_key_secret"),
        values.get("control_store_session_token_secret"),
    )
    if (
        values != expected
        or values["student_branch_image"] != launch["runtime_image_reference"]
        or values["student_job_id"] != operation["student_job_id"]
    ):
        raise ValueError("Baseten winner values differ from the gateway launch")
    return values


def dispatch_cross_provider_outboxes(
    baseten_jobs,
    modal_jobs,
    baseten_winner_lifecycle,
    *,
    control_store,
    scope_prefix,
    settings,
    baseten_team_name,
    modal_identity,
    modal_preflight,
    provider_pass_claim_raw,
    create_authorization_raw,
    modal_plans_by_run_id,
    winner_values_by_run_id,
):
    """Verify, select, and dispatch one bounded cross-provider frontier pass."""
    if (
        not isinstance(baseten_jobs, BasetenJobs)
        or not hasattr(modal_jobs, "launch")
        or not callable(modal_preflight)
        or not isinstance(modal_plans_by_run_id, dict)
        or not isinstance(winner_values_by_run_id, dict)
        or not isinstance(modal_identity, dict)
        or tuple(modal_identity)
        != (
            "provider",
            "workspace_id",
            "workspace_name",
            "environment_id",
            "environment_name",
            "app_id",
            "app_name",
        )
    ):
        raise ValueError("cross-provider dispatch configuration is invalid")
    _modal_budget_identity(modal_identity)
    campaign_id = baseten_jobs.campaign_id
    store = baseten_jobs.store
    if getattr(modal_jobs, "store", store) is not store:
        raise ValueError("provider lifecycles must share one evidence store")
    if not isinstance(provider_pass_claim_raw, (bytes, bytearray)) or not isinstance(
        create_authorization_raw,
        (bytes, bytearray),
    ):
        raise ValueError("cross-provider dispatch needs exact create authority bytes")
    _creation_authorities(
        provider_pass_claim_raw,
        create_authorization_raw,
        campaign_id,
        scope_prefix,
    )

    launches = discover_gpu_launches(
        control_store,
        scope_prefix,
        baseten_jobs.now(),
    )
    staged = []
    winner_ids = []
    run_ids = []
    for launch in launches:
        if launch["operation"]["kind"] == "student_winner_deployment":
            if launch["runtime_image_reference"] != settings.get(
                "student_branch_image"
            ):
                raise ValueError(
                    "winner launch runtime image differs from configured image"
                )
            run_id = _winner_run_from_launch(
                campaign_id,
                launch,
                settings.get("image_release_sha256"),
                settings.get("student_branch_image_admission_sha256"),
            )
            winner_ids.append(run_id)
            staged.append(
                {
                    "kind": "winner",
                    "run_id": run_id,
                    "launch": launch,
                }
            )
        else:
            workload, entries = _workload_and_entries(launch, settings)
            run_id = _workload_run_id(campaign_id, workload)
            defined_run_id, unused_definition, unused_entries = (
                baseten_jobs._definition(workload, entries)
            )
            del unused_definition, unused_entries
            if defined_run_id != run_id:
                raise ValueError("provider-neutral GPU run identity differs")
            staged.append(
                {
                    "kind": "ordinary",
                    "run_id": run_id,
                    "launch": launch,
                    "workload": workload,
                    "entries": entries,
                }
            )
        run_ids.append(run_id)
    if len(run_ids) != len(set(run_ids)):
        raise ValueError("cross-provider frontier contains duplicate runs")
    if len(winner_ids) > MAX_CANDIDATE_KEY_DELIVERY_REFERENCES:
        raise ValueError("scope has more than one active winner deployment")
    if set(modal_plans_by_run_id) != set(run_ids):
        raise ValueError("every possible Modal fallback needs one exact plan")
    if set(winner_values_by_run_id) != set(winner_ids):
        raise ValueError("every winner needs one exact Baseten value set")
    for item in staged:
        run_id = item["run_id"]
        unit_count = (
            1
            if item["kind"] == "winner"
            else len(_modal_units(item["workload"]))
        )
        _validate_modal_execution_plan(
            modal_plans_by_run_id[run_id],
            campaign_id,
            run_id,
            unit_count,
        )
        if item["kind"] == "winner":
            _winner_values(
                item["launch"],
                winner_values_by_run_id[run_id],
            )
    for item in staged:
        run_id = item["run_id"]
        if item["kind"] == "winner":
            if baseten_winner_lifecycle is None or not hasattr(
                baseten_winner_lifecycle,
                "preflight",
            ):
                raise ValueError("Baseten winner lifecycle is required")
            item["selection"] = select_winner_baseten_primary_modal_fallback(
                store=store,
                campaign_id=campaign_id,
                run_id=run_id,
                launch_source=item["launch"]["launch_source"],
                baseten_team_name=baseten_team_name,
                modal_identity=modal_identity,
                baseten_preflight=baseten_winner_lifecycle.preflight,
                modal_preflight=modal_preflight,
            )
        else:
            item["selection"] = select_baseten_primary_modal_fallback(
                store=store,
                campaign_id=campaign_id,
                run_id=run_id,
                launch_source=item["workload"]["launch_source"],
                baseten_team_name=baseten_team_name,
                baseten_project_id=baseten_jobs.project_id,
                modal_identity=modal_identity,
                baseten_preflight=lambda: baseten_jobs.preflight(
                    baseten_team_name
                ),
                modal_preflight=modal_preflight,
            )

    results = []
    references = []
    delivery_references = []
    for item in staged:
        run_id = item["run_id"]
        selected = item["selection"]["selection"]["selected_provider"]
        if item["kind"] == "ordinary" and selected == "baseten":
            result = baseten_jobs.launch(
                item["workload"],
                item["entries"],
                selection_record=item["selection"],
                provider_pass_claim_raw=provider_pass_claim_raw,
                create_authorization_raw=create_authorization_raw,
            )
        elif item["kind"] == "ordinary":
            result = launch_modal_gpu(
                modal_jobs,
                store=store,
                campaign_id=campaign_id,
                workload=item["workload"],
                runtime_image_reference=item["launch"][
                    "runtime_image_reference"
                ],
                selection_record=item["selection"],
                execution_plan=modal_plans_by_run_id[run_id],
                provider_pass_claim_raw=provider_pass_claim_raw,
                create_authorization_raw=create_authorization_raw,
                now=baseten_jobs.now,
            )
        elif selected == "baseten":
            result = launch_baseten_winner(
                baseten_winner_lifecycle,
                store=store,
                campaign_id=campaign_id,
                launch=item["launch"],
                selection_record=item["selection"],
                image_release_sha256=settings["image_release_sha256"],
                image_admission_sha256=settings[
                    "student_branch_image_admission_sha256"
                ],
                provider_pass_claim_raw=provider_pass_claim_raw,
                create_authorization_raw=create_authorization_raw,
                values=winner_values_by_run_id[run_id],
                now=baseten_jobs.now,
            )
        else:
            result = launch_modal_winner(
                modal_jobs,
                store=store,
                campaign_id=campaign_id,
                launch=item["launch"],
                selection_record=item["selection"],
                execution_plan=modal_plans_by_run_id[run_id],
                image_release_sha256=settings["image_release_sha256"],
                image_admission_sha256=settings[
                    "student_branch_image_admission_sha256"
                ],
                provider_pass_claim_raw=provider_pass_claim_raw,
                create_authorization_raw=create_authorization_raw,
                now=baseten_jobs.now,
            )
        references.extend(result.get("winner_result_references", []))
        delivery_references.extend(
            result.get("candidate_key_delivery_references", [])
        )
        results.append(result)
    references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    validate_winner_result_references(campaign_id, references)
    delivery_references.sort(
        key=lambda reference: (
            reference["student_job_id"],
            reference["claim_sha256"],
        )
    )
    validate_candidate_key_delivery_references(campaign_id, delivery_references)
    return {
        "schema_version": "milk.jobs-cross-provider-dispatch.v1",
        "provider_policy": {"primary": "baseten", "fallback": "modal"},
        "scope_prefix": scope_prefix,
        "frontier_scan_limit": GPU_LAUNCH_SCAN_LIMIT,
        "verified": len(launches),
        "dispatched": len(results),
        "state": "hold" if not results else "complete",
        "results": results,
        "winner_result_references": references,
        "candidate_key_delivery_references": delivery_references,
    }


def _modal_identity_from_arguments(arguments):
    value = {
        "provider": "modal",
        "workspace_id": arguments.modal_workspace_id,
        "workspace_name": arguments.modal_workspace_name,
        "environment_id": arguments.modal_environment_id,
        "environment_name": arguments.modal_environment_name,
        "app_id": arguments.modal_app_id,
        "app_name": arguments.modal_app_name,
    }
    _modal_preflight(
        {
            "schema_version": MODAL_PREFLIGHT_SCHEMA,
            "provider": "modal",
            "provider_identity": value,
            "outcome": "ready",
            "evidence_sha256": "0" * 64,
            "observed_at": "1970-01-01T00:00:00Z",
        },
        value,
    )
    return value


def _modal_named_resource(name, object_id, required_keys):
    value = {
        "name": name,
        "object_id": object_id,
        "required_keys": sorted(required_keys),
    }
    if (
        not isinstance(name, str)
        or OPAQUE.fullmatch(name) is None
        or not isinstance(object_id, str)
        or OPAQUE.fullmatch(object_id) is None
        or not required_keys
    ):
        raise ValueError("Modal named resource is invalid")
    return value


def _modal_resources_from_arguments(arguments, settings):
    control_keys = [
        "MILK_CONTROL_STORE_ACCESS_KEY_ID",
        "MILK_CONTROL_STORE_SECRET_ACCESS_KEY",
    ]
    if settings["control_store_session_token_secret"] is not None:
        control_keys.append("MILK_CONTROL_STORE_SESSION_TOKEN")
    capture_keys = [
        "MILK_CAPTURE" + "_STORE_ACCESS_KEY_ID",
        "MILK_CAPTURE" + "_STORE_SECRET_ACCESS_KEY",
    ]
    if settings["capture_store_session_token_secret"] is not None:
        capture_keys.append("MILK_CAPTURE" + "_STORE_SESSION_TOKEN")
    resources = {
        "registry": _modal_named_resource(
            arguments.modal_registry_secret_name,
            arguments.modal_registry_secret_id,
            ["REGISTRY_PASSWORD", "REGISTRY_USERNAME"],
        ),
        "config": _modal_named_resource(
            arguments.modal_config_secret_name,
            arguments.modal_config_secret_id,
            ["DRAGONTALES_CONFIG_JSON"],
        ),
        "control": _modal_named_resource(
            arguments.modal_control_secret_name,
            arguments.modal_control_secret_id,
            control_keys,
        ),
        "capture": _modal_named_resource(
            arguments.modal_capture_secret_name,
            arguments.modal_capture_secret_id,
            capture_keys,
        ),
        "candidate": _modal_named_resource(
            arguments.modal_candidate_secret_name,
            arguments.modal_candidate_secret_id,
            ["DRAGONTALES_CANDIDATE_API_KEY"],
        ),
        "teacher_volume": {
            "name": arguments.modal_teacher_volume_name,
            "object_id": arguments.modal_teacher_volume_id,
            "mount_path": adapter.TEACHER_MODEL_MOUNT,
            "read_only": True,
        },
        "student_train_volume": {
            "name": arguments.modal_student_train_volume_name,
            "object_id": arguments.modal_student_train_volume_id,
            "mount_path": adapter.STUDENT_MODEL_MOUNT,
            "read_only": True,
        },
    }
    named = [
        resources[key]
        for key in ("registry", "config", "control", "capture", "candidate")
    ] + [resources["teacher_volume"], resources["student_train_volume"]]
    if (
        any(
            not isinstance(item.get("name"), str)
            or OPAQUE.fullmatch(item["name"]) is None
            or not isinstance(item.get("object_id"), str)
            or OPAQUE.fullmatch(item["object_id"]) is None
            for item in named
        )
        or len({item["name"] for item in named}) != len(named)
        or len({item["object_id"] for item in named}) != len(named)
    ):
        raise ValueError("Modal resource identities must be pairwise distinct")
    return resources


def _modal_plan_resources(resources, kind):
    secrets = [resources["config"], resources["control"]]
    volumes = []
    if kind == "teacher_run":
        secrets.append(resources["capture"])
        volumes.append(resources["teacher_volume"])
    elif kind == "student_train_merge":
        volumes.append(resources["student_train_volume"])
    elif kind == "student_winner_deployment":
        secrets.append(resources["candidate"])
    return {
        "registry_secret": copy.deepcopy(resources["registry"]),
        "secrets": copy.deepcopy(secrets),
        "volumes": copy.deepcopy(volumes),
    }


def _modal_execution_command(unit):
    kind = unit["kind"]
    common = (
        "set -eu; umask 077; mkdir -m 0700 -p /tmp/dragontales; "
        "printf %s \"$DRAGONTALES_CONFIG_JSON\" >"
        "/tmp/dragontales/gateway.json; chmod 0400 "
        "/tmp/dragontales/gateway.json; unset DRAGONTALES_CONFIG_JSON; "
    )
    if kind == "teacher_run":
        command = (
            common
            + "exec /opt/dragontales/job.sh --config "
            "/tmp/dragontales/gateway.json --teacher-run-id "
            '"$DRAGONTALES_TEACHER_RUN_ID"'
        )
        environment = {"DRAGONTALES_TEACHER_RUN_ID": unit["teacher_run_id"]}
    else:
        student_job_id = unit["student_job_id"]
        if kind == "student_train_merge":
            action = "train"
            arguments = ""
        elif kind == "student_branch":
            action = "branch"
            arguments = f" --variant {unit['variant']}"
        else:
            raise ValueError("Modal ordinary GPU unit is invalid")
        command = (
            common
            + "exec /opt/dragontales/deploy/student-job.sh "
            + action
            + " --config /tmp/dragontales/gateway.json --student-job-id "
            '"$DRAGONTALES_STUDENT_JOB_ID"'
            + arguments
            + " --work-dir /tmp/dragontales-work"
        )
        environment = {"DRAGONTALES_STUDENT_JOB_ID": student_job_id}
    return ["/bin/sh", "-c", command], environment


def _modal_winner_command(student_job_id, model_alias):
    gateway_binary = "/usr/local/bin/dragontales-" + "gateway"
    command = (
        "set -eu; umask 077; mkdir -m 0700 -p /tmp/dragontales; "
        "printf %s \"$DRAGONTALES_CONFIG_JSON\" >"
        "/tmp/dragontales/gateway.json; chmod 0400 "
        "/tmp/dragontales/gateway.json; printf %s "
        '"$DRAGONTALES_CANDIDATE_API_KEY" >'
        "/tmp/dragontales-candidate.key; chmod 0400 "
        "/tmp/dragontales-candidate.key; "
        + gateway_binary
        + " "
        "--config /tmp/dragontales/gateway.json materialize-student-winner "
        '--student-job-id "$DRAGONTALES_STUDENT_JOB_ID" --stage-dir '
        "/tmp/dragontales-winner > /tmp/dragontales/materialize.stdout; "
        "unset DRAGONTALES_CONFIG_JSON DRAGONTALES_CANDIDATE_API_KEY "
        "MILK_CONTROL_STORE_ACCESS_KEY_ID "
        "MILK_CONTROL_STORE_SECRET_ACCESS_KEY "
        "MILK_CONTROL_STORE_SESSION_TOKEN; cat "
        "/tmp/dragontales/materialize.stdout; exec "
        "/opt/dragontales/deploy/student-job.sh serve --model "
        "/tmp/dragontales-winner/model --model-manifest "
        "/tmp/dragontales-winner/model-manifest.json --model-alias "
        '"$DRAGONTALES_MODEL_ALIAS" --api-key-file '
        "/tmp/dragontales-candidate.key"
    )
    return ["/bin/sh", "-c", command], {
        "DRAGONTALES_MODEL_ALIAS": model_alias,
        "DRAGONTALES_STUDENT_JOB_ID": student_job_id,
    }


def deterministic_cross_provider_inputs(
    control_store,
    *,
    campaign_id,
    scope_prefix,
    settings,
    modal_resources,
    winner_model_alias,
    current,
):
    """Build every provider plan from the one immutable pass configuration."""
    if (
        not isinstance(winner_model_alias, str)
        or winner_contract.MODEL_ALIAS.fullmatch(winner_model_alias) is None
    ):
        raise ValueError("winner model alias is invalid")
    launches = discover_gpu_launches(control_store, scope_prefix, current)
    plans = {}
    winner_values = {}
    for launch in launches:
        operation = launch["operation"]
        if operation["kind"] == "student_winner_deployment":
            run_id = _winner_run_from_launch(
                campaign_id,
                launch,
                settings["image_release_sha256"],
                settings["student_branch_image_admission_sha256"],
            )
            command, environment = _modal_winner_command(
                operation["student_job_id"],
                winner_model_alias,
            )
            units = [operation]
            winner_values[run_id] = winner_contract.settings(
                launch["runtime_image_reference"],
                operation["student_job_id"],
                winner_model_alias,
                "DOCKER_REGISTRY_ghcr.io",
                settings["config_secret"],
                settings["control_store_access_key_secret"],
                settings["control_store_secret_key_secret"],
                settings["control_store_session_token_secret"],
            )
            commands = [(command, environment)]
        else:
            workload, unused_entries = _workload_and_entries(launch, settings)
            del unused_entries
            run_id = _workload_run_id(campaign_id, workload)
            units = _modal_units(workload)
            commands = [_modal_execution_command(unit) for unit in units]
        executions = []
        for ordinal, (unit, command_and_environment) in enumerate(
            zip(units, commands)
        ):
            command, environment = command_and_environment
            executions.append(
                {
                    "ordinal": ordinal,
                    "command": command,
                    "environment": environment,
                    "resources": _modal_plan_resources(
                        modal_resources,
                        unit["kind"],
                    ),
                }
            )
        plans[run_id] = {
            "schema_version": "milk.modal-execution-plan.v1",
            "campaign_id": campaign_id,
            "run_id": run_id,
            "executions": executions,
        }
        _validate_modal_execution_plan(
            plans[run_id],
            campaign_id,
            run_id,
            len(units),
        )
    return plans, winner_values


def modal_ready_preflight(
    modal_jobs,
    provider_identity,
    plans,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
):
    """Verify the exact existing Modal workspace, app, and named resources."""
    if not isinstance(modal_jobs, ModalJobs) or not callable(now):
        raise ValueError("concrete Modal preflight runtime is required")
    sdk = modal_jobs.modal

    def call(function, *arguments, **keywords):
        modal_jobs.request_guard()
        return function(*arguments, **keywords)

    workspace = call(sdk.Workspace.from_context)
    call(workspace.hydrate)
    environment = call(
        sdk.Environment.from_name,
        provider_identity["environment_name"],
        create_if_missing=False,
    )
    call(environment.hydrate)
    app = call(
        sdk.App.lookup,
        provider_identity["app_name"],
        create_if_missing=False,
        environment_name=provider_identity["environment_name"],
    )
    if (
        getattr(workspace, "name", None) != provider_identity["workspace_name"]
        or getattr(environment, "object_id", None)
        != provider_identity["environment_id"]
        or getattr(environment, "name", None)
        != provider_identity["environment_name"]
        or getattr(app, "app_id", None) != provider_identity["app_id"]
        or getattr(app, "name", None) != provider_identity["app_name"]
    ):
        raise ValueError("Modal provider identity differs during preflight")
    resources = {}
    campaign_ids = set()
    for plan in plans.values():
        campaign_ids.add(plan.get("campaign_id"))
        for execution in plan["executions"]:
            accepted = execution["resources"]
            for spec in [accepted["registry_secret"], *accepted["secrets"]]:
                key = ("secret", spec["object_id"])
                if key in resources and resources[key] != spec:
                    raise ValueError("Modal named resource identity conflicts")
                resources[key] = spec
            for spec in accepted["volumes"]:
                key = ("volume", spec["object_id"])
                if key in resources and resources[key] != spec:
                    raise ValueError("Modal named resource identity conflicts")
                resources[key] = spec
    if len(campaign_ids) != 1 or not _is_hex64(next(iter(campaign_ids), None)):
        raise ValueError("Modal preflight plans differ by campaign")
    for (kind, unused_object_id), spec in sorted(resources.items()):
        del unused_object_id
        if kind == "secret":
            handle = call(
                sdk.Secret.from_name,
                spec["name"],
                required_keys=spec["required_keys"],
                environment_name=provider_identity["environment_name"],
            )
        else:
            handle = call(
                sdk.Volume.from_name,
                spec["name"],
                create_if_missing=False,
                environment_name=provider_identity["environment_name"],
            )
        call(handle.hydrate)
        if getattr(handle, "object_id", None) != spec["object_id"]:
            raise ValueError("Modal named resource identity differs")
    observed_at = _utc_text(now())
    evidence = {
        "schema_version": "milk.modal-resource-preflight-evidence.v1",
        "provider_identity": copy.deepcopy(provider_identity),
        "resources": [copy.deepcopy(resources[key]) for key in sorted(resources)],
        "observed_at": observed_at,
    }
    evidence_sha256 = _digest(evidence)
    create_same(
        modal_jobs.store,
        (
            f"campaigns/v1/{next(iter(campaign_ids))}/provider-preflights/"
            f"modal/{evidence_sha256}.json"
        ),
        canonical_json(evidence),
        "application/json",
    )
    return {
        "schema_version": MODAL_PREFLIGHT_SCHEMA,
        "provider": "modal",
        "provider_identity": copy.deepcopy(provider_identity),
        "outcome": "ready",
        "evidence_sha256": evidence_sha256,
        "observed_at": observed_at,
    }


def dispatch_outboxes(
    jobs,
    *,
    control_store,
    scope_prefix,
    settings,
):
    launches = discover_gpu_launches(control_store, scope_prefix, jobs.now())
    prepared = []
    for launch in launches:
        workload, entries = _workload_and_entries(launch, settings)
        jobs._definition(workload, entries)
        prepared.append((workload, entries))
    results = []
    for workload, entries in prepared:
        result = jobs.launch(workload, entries)
        results.append(result)
    return {
        "schema_version": "milk.jobs-dispatch-summary.v2",
        "provider": "baseten",
        "scope_prefix": scope_prefix,
        "frontier_scan_limit": GPU_LAUNCH_SCAN_LIMIT,
        "verified": len(launches),
        "dispatched": len(results),
        "state": "hold" if not results else "complete",
        "results": results,
    }


def _require_separate_control_store(evidence_store, control_store):
    if (
        evidence_store.bucket == control_store.bucket
        or evidence_store.access_key_id == control_store.access_key_id
    ):
        raise ValueError("control R2 bucket and access authority must be dedicated")


def _require_separate_create_authority_store(evidence_store, authority_store):
    if (
        evidence_store.bucket == authority_store.bucket
        or evidence_store.access_key_id == authority_store.access_key_id
    ):
        raise ValueError(
            "provider create authority R2 bucket and read credential must be dedicated"
        )


def _image_settings_from_arguments(arguments, evidence_store):
    required = (
        "student_train_image",
        "student_branch_image",
        "teacher_image",
        "jobs_image",
        "image_release_sha256",
    )
    if any(getattr(arguments, name) is None for name in required):
        raise ValueError("private image release settings are required")
    student_train_image = adapter.immutable_ghcr_image(arguments.student_train_image)
    student_branch_image = adapter.immutable_ghcr_image(arguments.student_branch_image)
    teacher_image = adapter.immutable_ghcr_image(arguments.teacher_image)
    jobs_image = adapter.immutable_ghcr_image(arguments.jobs_image)
    if (
        student_train_image.rpartition("@sha256:")[0]
        != STUDENT_TRAIN_IMAGE_REPOSITORY
        or student_branch_image.rpartition("@sha256:")[0]
        != STUDENT_BRANCH_IMAGE_REPOSITORY
    ):
        raise ValueError("student image must use a private Milk repository")
    if (
        student_train_image.rpartition("@sha256:")[2]
        == student_branch_image.rpartition("@sha256:")[2]
    ):
        raise ValueError("student train and branch images must have distinct digests")
    if teacher_image.rpartition("@sha256:")[0] != TEACHER_IMAGE_REPOSITORY:
        raise ValueError("teacher image must use the private Milk repository")
    if jobs_image.rpartition("@sha256:")[0] != JOBS_IMAGE_REPOSITORY:
        raise ValueError("jobs image must use the private Milk repository")
    release = load_published_private_image_release(
        evidence_store,
        arguments.image_release_sha256,
    )
    for artifact, image in (
        ("student-train", student_train_image),
        ("student-branch", student_branch_image),
        ("teacher-gpt-oss", teacher_image),
        ("jobs", jobs_image),
    ):
        if release["images"][artifact]["image_reference"] != image:
            raise ValueError(f"{artifact} image differs from its private build admission")
    return {
        "student_train_image": student_train_image,
        "student_branch_image": student_branch_image,
        "teacher_image": teacher_image,
        "jobs_image": jobs_image,
        "image_release_sha256": release["release_sha256"],
        "student_train_image_admission_sha256": release["images"]["student-train"][
            "admission_sha256"
        ],
        "student_branch_image_admission_sha256": release["images"]["student-branch"][
            "admission_sha256"
        ],
        "teacher_image_admission_sha256": release["images"]["teacher-gpt-oss"][
            "admission_sha256"
        ],
    }


def _provider_settings_from_arguments(arguments, image_settings):
    required = (
        "registry_secret",
        "config_secret",
        "capture_store_access_key_secret",
        "capture_store_secret_key_secret",
        "control_store_access_key_secret",
        "control_store_secret_key_secret",
    )
    if any(getattr(arguments, name) is None for name in required):
        raise ValueError("provider launch settings are required for mutation mode")
    if arguments.registry_secret == "DOCKER_REGISTRY_ghcr.io":
        raise ValueError("training and winner registry secrets must be distinct")
    settings = {
        **image_settings,
        "registry_secret": adapter.identifier(arguments.registry_secret, "registry secret name"),
        "config_secret": adapter.identifier(arguments.config_secret, "config secret name"),
        "capture_store_access_key_secret": adapter.identifier(
            arguments.capture_store_access_key_secret,
            "capture-store access-key secret name",
        ),
        "capture_store_secret_key_secret": adapter.identifier(
            arguments.capture_store_secret_key_secret,
            "capture-store secret-key secret name",
        ),
        "capture_store_session_token_secret": (
            None
            if arguments.capture_store_session_token_secret is None
            else adapter.identifier(
                arguments.capture_store_session_token_secret,
                "capture-store session-token secret name",
            )
        ),
        "control_store_access_key_secret": adapter.identifier(
            arguments.control_store_access_key_secret,
            "control-store access-key secret name",
        ),
        "control_store_secret_key_secret": adapter.identifier(
            arguments.control_store_secret_key_secret,
            "control-store secret-key secret name",
        ),
        "control_store_session_token_secret": (
            None
            if arguments.control_store_session_token_secret is None
            else adapter.identifier(
                arguments.control_store_session_token_secret,
                "control-store session-token secret name",
            )
        ),
    }
    secret_names = [
        settings[key]
        for key in (
            "registry_secret",
            "config_secret",
            "capture_store_access_key_secret",
            "capture_store_secret_key_secret",
            "capture_store_session_token_secret",
            "control_store_access_key_secret",
            "control_store_secret_key_secret",
            "control_store_session_token_secret",
        )
        if settings[key] is not None
    ]
    if len(secret_names) != len(set(secret_names)):
        raise ValueError("provider secret names must be pairwise distinct")
    return settings


def settings_from_arguments(arguments, evidence_store):
    return _provider_settings_from_arguments(
        arguments,
        _image_settings_from_arguments(arguments, evidence_store),
    )


def _baseten_teardown_values(records, settings):
    values = {}
    for record in records:
        authorization = record["authorization"]
        if authorization["selected_provider"] != "baseten":
            continue
        result = record["winner_result"]
        admission = result["admission"]
        run_id = authorization["run_id"]
        values[run_id] = winner_contract.settings(
            admission["student_branch_runtime_image_reference"],
            authorization["student_job_id"],
            admission["model_alias"],
            "DOCKER_REGISTRY_ghcr.io",
            settings["config_secret"],
            settings["control_store_access_key_secret"],
            settings["control_store_secret_key_secret"],
            settings["control_store_session_token_secret"],
        )
    return values


def _required_provider_credentials():
    baseten_api_key = os.environ.get("BASETEN_API_KEY", "")
    if not baseten_api_key:
        raise ValueError("BASETEN_API_KEY is required for the primary provider")
    if not os.environ.get("MODAL_TOKEN_ID") or not os.environ.get(
        "MODAL_TOKEN_SECRET"
    ):
        raise ValueError("Modal credentials are required for provider fallback")
    return baseten_api_key


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description="Run one bounded Milk provider lifecycle pass")
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--baseten-team-name", dest="team_name", required=True)
    parser.add_argument("--candidate-key-socket", required=True)
    parser.add_argument("--scope-prefix", required=True)
    parser.add_argument("--lease-token", required=True)
    parser.add_argument("--student-train-image")
    parser.add_argument("--student-branch-image")
    parser.add_argument("--teacher-image")
    parser.add_argument("--jobs-image")
    parser.add_argument("--registry-secret")
    parser.add_argument("--config-secret")
    parser.add_argument("--capture-store-access-key-secret")
    parser.add_argument("--capture-store-secret-key-secret")
    parser.add_argument("--capture-store-session-token-secret")
    parser.add_argument("--control-store-access-key-secret")
    parser.add_argument("--control-store-secret-key-secret")
    parser.add_argument("--control-store-session-token-secret")
    parser.add_argument("--image-release-sha256")
    parser.add_argument("--winner-model-alias", default="milk-student")
    for name in (
        "modal-workspace-id",
        "modal-workspace-name",
        "modal-environment-id",
        "modal-environment-name",
        "modal-app-id",
        "modal-app-name",
        "modal-registry-secret-name",
        "modal-registry-secret-id",
        "modal-config-secret-name",
        "modal-config-secret-id",
        "modal-control-secret-name",
        "modal-control-secret-id",
        "modal-capture-secret-name",
        "modal-capture-secret-id",
        "modal-candidate-secret-name",
        "modal-candidate-secret-id",
        "modal-teacher-volume-name",
        "modal-teacher-volume-id",
        "modal-student-train-volume-name",
        "modal-student-train-volume-id",
    ):
        parser.add_argument(f"--{name}")
    parser.add_argument("--request-timeout-seconds", type=int, default=30)
    parser.add_argument("--reconcile-timeout-seconds", type=int, default=300)
    arguments = parser.parse_args(argv)
    try:
        _scope_from_prefix(arguments.scope_prefix)
        store = R2EvidenceStore.from_environment()
        lease_token = load_provider_lease_token(arguments.lease_token)
        evidence_store_identity = store_identity_sha256(
            os.environ.get("MILK_EVIDENCE_R2_ACCOUNT_ID"),
            os.environ.get("MILK_EVIDENCE_R2_BUCKET"),
            os.environ.get("MILK_EVIDENCE_R2_ACCESS_KEY_ID"),
        )
        create_authority_store = None
        create_authority_store_location = None
        if lease_token["create_authorization_sha256"] is None:
            authority_environment = (
                os.environ.get(prefix + name)
                for prefix in (
                    "MILK_CREATE_AUTHORITY_READ_R2_",
                    "MILK_CREATE_AUTHORITY_WRITE_R2_",
                )
                for name in (
                    "ACCOUNT_ID",
                    "BUCKET",
                    "ACCESS_KEY_ID",
                    "SECRET_ACCESS_KEY",
                    "SESSION_TOKEN",
                )
            )
            if any(authority_environment):
                raise ValueError(
                    "reconcile-only provider pass must not receive create authority credentials"
                )
        else:
            create_authority_store = R2EvidenceStore.from_environment(
                prefix="MILK_CREATE_AUTHORITY_READ_R2_"
            )
            _require_separate_create_authority_store(
                store,
                create_authority_store,
            )
            create_authority_store_location = store_location_sha256(
                os.environ.get("MILK_CREATE_AUTHORITY_READ_R2_ACCOUNT_ID"),
                os.environ.get("MILK_CREATE_AUTHORITY_READ_R2_BUCKET"),
            )
        pass_claim = claim_provider_jobs_pass(
            store,
            create_authority_store,
            lease_token,
            arguments.campaign_id,
            arguments.scope_prefix,
            evidence_store_identity,
            create_authority_store_location,
        )
        provider_creates_authorized = pass_claim["provider_creates_authorized"]
        provider_pass_claim_raw = canonical_json(pass_claim)
        create_authorization_raw = None
        if arguments.candidate_key_socket != CANDIDATE_KEY_SOCKET:
            raise ValueError("provider pass requires the fixed candidate-key socket")
        if provider_creates_authorized:
            create_authorization_raw = create_authority_store.get(
                lease_token["create_authorization_object_key"]
            )
            if (
                hashlib.sha256(create_authorization_raw).hexdigest()
                != lease_token["create_authorization_sha256"]
            ):
                raise ValueError("provider create authorization digest differs")
        control_store = R2EvidenceStore.from_environment(prefix="MILK_CONTROL_R2_")
        _require_separate_control_store(store, control_store)
        settings = _provider_settings_from_arguments(
            arguments,
            _image_settings_from_arguments(arguments, store),
        )
        recovered_winner_result_references = (
            recover_gateway_winner_result_references(
                store,
                control_store,
                campaign_id=arguments.campaign_id,
                scope_prefix=arguments.scope_prefix,
                image_release_sha256=settings["image_release_sha256"],
                image_admission_sha256=settings[
                    "student_branch_image_admission_sha256"
                ],
                current=dt.datetime.now(dt.timezone.utc).replace(
                    microsecond=0
                ),
            )
        )
        recovered_candidate_key_delivery_references = (
            recover_candidate_key_delivery_references(
                store,
                campaign_id=arguments.campaign_id,
                winner_result_references=recovered_winner_result_references,
            )
        )

        def live_request_guard():
            verify_provider_lease_token(
                store,
                create_authority_store,
                lease_token,
                arguments.campaign_id,
                arguments.scope_prefix,
                evidence_store_identity,
                create_authority_store_location,
            )
            if not provider_creates_authorized:
                return None
            return {
                "provider_pass_claim_sha256": hashlib.sha256(
                    provider_pass_claim_raw
                ).hexdigest(),
                "create_authorization_sha256": hashlib.sha256(
                    create_authorization_raw
                ).hexdigest(),
            }

        baseten_api_key = _required_provider_credentials()
        transport = BasetenTransport(
            baseten_api_key,
            timeout_seconds=arguments.request_timeout_seconds,
        )
        jobs = BasetenJobs(
            store=store,
            campaign_id=arguments.campaign_id,
            project_id=arguments.project_id,
            transport=transport,
            request_guard=live_request_guard,
        )
        winner_runtime = baseten_winner.BasetenWinnerRuntime(
            api_key=baseten_api_key,
            team_name=arguments.team_name,
            timeout_seconds=arguments.request_timeout_seconds,
        )
        winner_lifecycle = baseten_winner.BasetenWinnerLifecycle(
            store=store,
            campaign_id=arguments.campaign_id,
            team_name=arguments.team_name,
            request_guard=live_request_guard,
            runtime=winner_runtime,
            candidate_key_sink=lambda raw: _candidate_key_socket_transaction(
                arguments.candidate_key_socket,
                raw,
                min(arguments.reconcile_timeout_seconds, 600),
            ),
        )
        modal_jobs = ModalJobs(
            store=store,
            request_guard=live_request_guard,
        )
        reconciliation = jobs.reconcile_all(arguments.reconcile_timeout_seconds)
        pending_winner_recovery = recover_pending_baseten_winner_admissions(
            winner_lifecycle,
            store=store,
            control_store=control_store,
            campaign_id=arguments.campaign_id,
            scope_prefix=arguments.scope_prefix,
            settings=settings,
            winner_model_alias=arguments.winner_model_alias,
            current=dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
        )
        teardown_records = discover_provider_teardown_authorizations(
            control_store,
            arguments.scope_prefix,
            dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
        )
        teardown = dispatch_provider_teardowns(
            winner_lifecycle,
            modal_jobs,
            store=store,
            control_store=control_store,
            campaign_id=arguments.campaign_id,
            scope_prefix=arguments.scope_prefix,
            evidence_store_identity_sha256=evidence_store_identity,
            baseten_values_by_run_id=_baseten_teardown_values(
                teardown_records,
                settings,
            ),
        )
        dispatched = (
            pending_winner_recovery
            if pending_winner_recovery["results"]
            else None
        )
        if provider_creates_authorized:
            if reconciliation["evidence_complete"] and not reconciliation["backlog"]:
                modal_identity = _modal_identity_from_arguments(arguments)
                modal_resources = _modal_resources_from_arguments(
                    arguments,
                    settings,
                )
                plans, winner_values = deterministic_cross_provider_inputs(
                    control_store,
                    campaign_id=arguments.campaign_id,
                    scope_prefix=arguments.scope_prefix,
                    settings=settings,
                    modal_resources=modal_resources,
                    winner_model_alias=arguments.winner_model_alias,
                    current=dt.datetime.now(dt.timezone.utc).replace(
                        microsecond=0
                    ),
                )
                dispatched = dispatch_cross_provider_outboxes(
                    jobs,
                    modal_jobs,
                    winner_lifecycle,
                    control_store=control_store,
                    scope_prefix=arguments.scope_prefix,
                    settings=settings,
                    baseten_team_name=arguments.team_name,
                    modal_identity=modal_identity,
                    modal_preflight=lambda: modal_ready_preflight(
                        modal_jobs,
                        modal_identity,
                        plans,
                    ),
                    provider_pass_claim_raw=provider_pass_claim_raw,
                    create_authorization_raw=create_authorization_raw,
                    modal_plans_by_run_id=plans,
                    winner_values_by_run_id=winner_values,
                )
            else:
                dispatched = {
                    "state": (
                        "blocked_reconciliation_backlog"
                        if reconciliation["backlog"]
                        else "blocked_evidence_failure"
                    )
                }
        winner_references = {
            (reference["student_job_id"], reference["claim_sha256"]): reference
            for reference in recovered_winner_result_references
        }
        for reference in (
            pending_winner_recovery["winner_result_references"]
            + (
                []
                if dispatched is None or dispatched is pending_winner_recovery
                else dispatched.get("winner_result_references", [])
            )
        ):
            identity = (
                reference["student_job_id"],
                reference["claim_sha256"],
            )
            existing = winner_references.get(identity)
            if existing is not None and existing != reference:
                raise ValueError("winner result handoff changed during the pass")
            winner_references[identity] = reference
        winner_result_references = [
            winner_references[identity]
            for identity in sorted(winner_references)
        ]
        candidate_delivery_references = {
            (reference["student_job_id"], reference["claim_sha256"]): reference
            for reference in recovered_candidate_key_delivery_references
        }
        for reference in (
            pending_winner_recovery["candidate_key_delivery_references"]
            + (
                []
                if dispatched is None or dispatched is pending_winner_recovery
                else dispatched.get("candidate_key_delivery_references", [])
            )
        ):
            identity = (
                reference["student_job_id"],
                reference["claim_sha256"],
            )
            existing = candidate_delivery_references.get(identity)
            if existing is not None and existing != reference:
                raise ValueError(
                    "candidate-key delivery handoff changed during the pass"
                )
            candidate_delivery_references[identity] = reference
        candidate_key_delivery_references = [
            candidate_delivery_references[identity]
            for identity in sorted(candidate_delivery_references)
        ]
        teardown_result_references = teardown[
            "teardown_result_references"
        ]
        validate_winner_result_references(
            arguments.campaign_id,
            winner_result_references,
        )
        validate_candidate_key_delivery_references(
            arguments.campaign_id,
            candidate_key_delivery_references,
        )
        validate_teardown_result_references(
            arguments.campaign_id,
            teardown_result_references,
        )
        result = {
            "schema_version": "milk.jobs-pass.v3",
            "provider_policy": {"primary": "baseten", "fallback": "modal"},
            "scope_prefix": arguments.scope_prefix,
            "provider_creates_authorized": provider_creates_authorized,
            "provider_pass_claim_sha256": hashlib.sha256(
                canonical_json(pass_claim)
            ).hexdigest(),
            "create_authorization_sha256": lease_token[
                "create_authorization_sha256"
            ],
            "reconciliation": reconciliation,
            "dispatched": dispatched,
            "teardown": teardown,
            "winner_result_references": winner_result_references,
            "candidate_key_delivery_references": (
                candidate_key_delivery_references
            ),
            "teardown_result_references": teardown_result_references,
        }
    except (
        OSError,
        RuntimeError,
        UnicodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"milk-jobs: {error}", file=os.sys.stderr)
        return 64
    os.sys.stdout.buffer.write(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
