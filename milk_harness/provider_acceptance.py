"""Canonical provider acceptance and route-retirement wires."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import uuid


SCHEMA = "milk.winner-provider-acceptance.v1"
GPU_SCHEMA = "milk.gpu-provider-acceptance.v1"
MAX_WALL_SECONDS = 24 * 60 * 60
MAX_COST_MICROUSD = 1_000_000_000
WINNER_RUN_DOMAIN = b"milk.provider-neutral-winner-run.v1\0"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
OPAQUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
TEAM_NAME = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9 ._-]{0,126}[A-Za-z0-9])?\Z"
)
TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z\Z"
)
SCOPE_FIELDS = ("tenant_id", "project_id", "environment_id", "workload_id")
CANARY_BASIS_POINTS = 100
KEY_ORDER = (
    "schema_version",
    "campaign_id",
    "run_id",
    "claim_sha256",
    "outbox_sha256",
    "provider_binding_sha256",
    "selection",
    "image_release_sha256",
    "image_admission_sha256",
    "provider_pass_claim_sha256",
    "create_authorization_sha256",
    "budget_reservation_sha256",
    "reserved_microusd",
    "reserved_at",
    "accepted_at",
    "create_not_after",
    "provider_not_after",
    "max_wall_seconds",
    "max_cost_microusd",
    "state",
)
GPU_KEY_ORDER = (
    "schema_version",
    "campaign_id",
    "run_id",
    "claim_sha256",
    "outbox_sha256",
    "operation",
    "selection",
    "image_release_sha256",
    "image_admission_sha256",
    "provider_pass_claim_sha256",
    "create_authorization_sha256",
    "budget_reservation_sha256",
    "reserved_microusd",
    "reserved_at",
    "accepted_at",
    "create_not_after",
    "provider_not_after",
    "max_wall_seconds",
    "max_cost_microusd",
    "state",
)


def winner_run_id(
    *,
    campaign_id,
    claim_sha256,
    outbox_sha256,
    student_job_id,
    student_result_sha256,
    winner,
    max_wall_seconds,
    max_cost_microusd,
    image_release_sha256,
    image_admission_sha256,
):
    value = {
        "schema_version": "milk.provider-neutral-winner-run.v1",
        "campaign_id": campaign_id,
        "claim_sha256": claim_sha256,
        "outbox_sha256": outbox_sha256,
        "operation": {
            "kind": "student_winner_deployment",
            "student_job_id": student_job_id,
            "student_result_sha256": student_result_sha256,
            "winner": winner,
            "max_wall_seconds": max_wall_seconds,
            "max_cost_microusd": max_cost_microusd,
        },
        "image_release_sha256": image_release_sha256,
        "image_admission_sha256": image_admission_sha256,
    }
    if (
        any(
            not _match(HEX64, value.get(field))
            for field in ("campaign_id", "claim_sha256", "outbox_sha256")
        )
        or any(
            not _match(HEX64, value.get(field))
            for field in ("image_release_sha256", "image_admission_sha256")
        )
        or not _match(HEX64, student_job_id)
        or not _match(HEX64, student_result_sha256)
        or winner not in {"bf16", "dynamic_fp8", "static_fp8"}
        or type(max_wall_seconds) is not int
        or not 60 <= max_wall_seconds <= MAX_WALL_SECONDS
        or type(max_cost_microusd) is not int
        or not 1 <= max_cost_microusd <= MAX_COST_MICROUSD
    ):
        raise ValueError("provider-neutral winner run identity is invalid")
    raw = (
        json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()
    return hashlib.sha256(WINNER_RUN_DOMAIN + raw).hexdigest()


def _match(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _route_object(raw, label):
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(raw) <= 16 * 1024:
        raise ValueError(f"{label} must be bounded bytes")
    body = bytes(raw)
    if not body.endswith(b"\n") or b"\n" in body[:-1]:
        raise ValueError(f"{label} must be one JSON line")
    body = body[:-1]

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"{label} contains a duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(
            body,
            object_pairs_hook=pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not strict JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    if json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() != body:
        raise ValueError(f"{label} is not canonical gateway JSON")
    return value


def _route_scope_prefix(scope):
    if not isinstance(scope, dict) or tuple(scope) != SCOPE_FIELDS:
        raise ValueError("winner gateway scope is invalid")
    for field in SCOPE_FIELDS:
        try:
            parsed = uuid.UUID(scope[field])
        except (AttributeError, ValueError):
            raise ValueError("winner gateway scope is invalid") from None
        if parsed.int == 0 or str(parsed) != scope[field]:
            raise ValueError("winner gateway scope is invalid")
    return "dt/v2/" + "/".join(scope[field] for field in SCOPE_FIELDS)


def _route_receipt(raw, claim, basis_points, previous_revision):
    value = _route_object(raw, "gateway route publication")
    if tuple(value) != (
        "schema_version",
        "route_revision",
        "student_job_id",
        "student_result_sha256",
        "model_manifest_sha256",
        "dev_receipt_sha256",
        "previous_route_revision",
        "candidate_basis_points",
        "manifest_object_key",
        "signature_object_key",
        "live_pointer_object_key",
        "state",
    ):
        raise ValueError("gateway route publication fields or order are invalid")
    revision = value.get("route_revision")
    scope_prefix = _route_scope_prefix(claim["scope"])
    signature_prefix = f"{scope_prefix}/routes/signatures/{revision}/"
    signature_key = value.get("signature_object_key")
    if (
        value.get("schema_version")
        != "dragontales.route-publication-receipt.v2"
        or any(
            not _match(HEX64, value.get(field))
            for field in (
                "route_revision",
                "student_job_id",
                "student_result_sha256",
                "model_manifest_sha256",
                "dev_receipt_sha256",
            )
        )
        or value.get("student_job_id") != claim["student_job_id"]
        or value.get("student_result_sha256") != claim["student_result_sha256"]
        or value.get("model_manifest_sha256") != claim["model_manifest"]["sha256"]
        or value.get("dev_receipt_sha256") != claim["dev_receipt"]["sha256"]
        or value.get("candidate_basis_points") != basis_points
        or value.get("previous_route_revision") != previous_revision
        or value.get("manifest_object_key")
        != f"{scope_prefix}/routes/manifests/{revision}.json"
        or not isinstance(signature_key, str)
        or not signature_key.startswith(signature_prefix)
        or re.fullmatch(
            re.escape(signature_prefix) + r"[0-9a-f]{64}\.ed25519",
            signature_key,
        )
        is None
        or value.get("live_pointer_object_key")
        != f"{scope_prefix}/routes/live.json"
        or value.get("state") != "active"
    ):
        raise ValueError("gateway signed route publication is invalid")
    return value


def validate_route_retirement(canary_raw, zero_raw, claim):
    canary_value = _route_object(
        canary_raw,
        "gateway canary route publication",
    )
    previous = canary_value.get("previous_route_revision")
    if previous is not None and not _match(HEX64, previous):
        raise ValueError("gateway canary prior route revision is invalid")
    canary = _route_receipt(
        canary_raw,
        claim,
        CANARY_BASIS_POINTS,
        previous,
    )
    zero = _route_receipt(zero_raw, claim, 0, canary["route_revision"])
    for field in (
        "student_job_id",
        "student_result_sha256",
        "model_manifest_sha256",
        "dev_receipt_sha256",
    ):
        if zero[field] != canary[field]:
            raise ValueError("gateway zero route differs from its canary")
    return canary, zero


def _time(value, label):
    if not isinstance(value, str) or TIME.fullmatch(value) is None:
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        return dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error


def _baseten_selection(value):
    if not isinstance(value, dict) or tuple(value) != (
        "selected_provider",
        "provider_identity",
        "primary_preflight",
    ):
        raise ValueError("Baseten winner provider selection fields are invalid")
    identity = value["provider_identity"]
    primary = value["primary_preflight"]
    if (
        value["selected_provider"] != "baseten"
        or not isinstance(identity, dict)
        or tuple(identity) != ("provider", "team_name")
        or identity["provider"] != "baseten"
        or not _match(TEAM_NAME, identity.get("team_name"))
        or not isinstance(primary, dict)
        or tuple(primary)
        != ("provider", "outcome", "evidence_sha256", "observed_at")
        or primary["provider"] != "baseten"
        or primary["outcome"] != "ready"
        or not _match(HEX64, primary.get("evidence_sha256"))
    ):
        raise ValueError("Baseten winner provider selection is invalid")
    return _time(primary["observed_at"], "Baseten preflight time")


def _modal_selection(value):
    if not isinstance(value, dict) or tuple(value) != (
        "selected_provider",
        "provider_identity",
        "primary_preflight",
        "fallback_preflight",
    ):
        raise ValueError("Modal winner provider selection fields are invalid")
    identity = value["provider_identity"]
    primary = value["primary_preflight"]
    fallback = value["fallback_preflight"]
    if (
        value["selected_provider"] != "modal"
        or not isinstance(identity, dict)
        or tuple(identity)
        != (
            "provider",
            "workspace_id",
            "workspace_name",
            "environment_id",
            "environment_name",
            "app_id",
            "app_name",
        )
        or identity["provider"] != "modal"
        or any(
            not _match(OPAQUE, identity.get(key))
            for key in tuple(identity)[1:]
        )
        or not isinstance(primary, dict)
        or tuple(primary)
        != (
            "provider",
            "outcome",
            "reason",
            "status",
            "evidence_sha256",
            "observed_at",
        )
        or primary["provider"] != "baseten"
        or primary["outcome"] != "retryable_unavailable"
        or not _match(HEX64, primary.get("evidence_sha256"))
        or not isinstance(fallback, dict)
        or tuple(fallback)
        != ("provider", "outcome", "evidence_sha256", "observed_at")
        or fallback["provider"] != "modal"
        or fallback["outcome"] != "ready"
        or not _match(HEX64, fallback.get("evidence_sha256"))
    ):
        raise ValueError("Modal winner provider selection is invalid")
    reason = primary["reason"]
    status = primary["status"]
    if (
        reason == "timeout"
        and status is not None
        or reason == "rate_limited"
        and status != 429
        or reason == "server_unavailable"
        and (type(status) is not int or not 500 <= status <= 599)
        or reason not in {"timeout", "rate_limited", "server_unavailable"}
    ):
        raise ValueError("Baseten primary preflight is not fallback-safe")
    primary_at = _time(primary["observed_at"], "Baseten preflight time")
    fallback_at = _time(fallback["observed_at"], "Modal preflight time")
    if primary_at > fallback_at:
        raise ValueError("winner provider preflight order is invalid")
    return fallback_at


def _gpu_operation(value):
    if not isinstance(value, dict):
        raise ValueError("GPU provider operation is invalid")
    kind = value.get("kind")
    if kind == "teacher_run":
        if tuple(value) != (
            "kind",
            "teacher_run_id",
            "provider_binding_sha256",
            "slot",
            "call_count",
            "max_gpu_seconds",
        ) or (
            not _match(HEX64, value.get("teacher_run_id"))
            or not _match(HEX64, value.get("provider_binding_sha256"))
            or type(value.get("slot")) is not int
            or not 0 <= value["slot"] < 16
            or type(value.get("call_count")) is not int
            or not 1 <= value["call_count"] <= 64
            or type(value.get("max_gpu_seconds")) is not int
            or not 60 <= value["max_gpu_seconds"] <= 3_600
        ):
            raise ValueError("teacher GPU operation is invalid")
        return value["max_gpu_seconds"]
    if kind == "student_train_merge":
        counts = value.get("counts")
        if tuple(value) != (
            "kind",
            "student_job_id",
            "counts",
            "max_gpu_seconds",
        ) or (
            not _match(HEX64, value.get("student_job_id"))
            or not isinstance(counts, dict)
            or tuple(counts) != ("train", "dev", "calibration")
            or type(counts.get("train")) is not int
            or not 50 <= counts["train"] <= 10_000
            or type(counts.get("dev")) is not int
            or not 73 <= counts["dev"] <= 128
            or counts.get("calibration") != 128
            or value.get("max_gpu_seconds") != 1_800
        ):
            raise ValueError("student train GPU operation is invalid")
        return value["max_gpu_seconds"]
    if kind == "student_fanout":
        branches = value.get("branches")
        if tuple(value) != (
            "kind",
            "student_job_id",
            "max_total_gpu_seconds",
            "branches",
        ) or (
            not _match(HEX64, value.get("student_job_id"))
            or value.get("max_total_gpu_seconds") != 7_200
            or not isinstance(branches, list)
            or len(branches) != 3
        ):
            raise ValueError("student fanout GPU operation is invalid")
        for branch, variant in zip(
            branches,
            ("bf16", "dynamic_fp8", "static_fp8"),
        ):
            if (
                not isinstance(branch, dict)
                or tuple(branch)
                != ("schema_version", "branch_id", "variant", "max_gpu_seconds")
                or branch.get("schema_version")
                != "dragontales.student-branch-claim.v1"
                or not _match(HEX64, branch.get("branch_id"))
                or branch.get("variant") != variant
                or branch.get("max_gpu_seconds") != 1_800
            ):
                raise ValueError("student fanout branch is invalid")
        return max(branch["max_gpu_seconds"] for branch in branches)
    raise ValueError("GPU provider operation kind is invalid")


def validate(value):
    if not isinstance(value, dict):
        raise ValueError("provider acceptance is invalid")
    schema = value.get("schema_version")
    key_order = KEY_ORDER if schema == SCHEMA else GPU_KEY_ORDER if schema == GPU_SCHEMA else None
    if key_order is None or tuple(value) != key_order:
        raise ValueError("winner provider acceptance fields or order are invalid")
    if (
        any(
            not _match(HEX64, value.get(field))
            for field in (
                "campaign_id",
                "run_id",
                "claim_sha256",
                "outbox_sha256",
                "image_release_sha256",
                "image_admission_sha256",
                "provider_pass_claim_sha256",
                "create_authorization_sha256",
                "budget_reservation_sha256",
            )
        )
        or type(value.get("reserved_microusd")) is not int
        or type(value.get("max_wall_seconds")) is not int
        or type(value.get("max_cost_microusd")) is not int
        or not 1 <= value["reserved_microusd"] <= value["max_cost_microusd"]
        or not 60 <= value["max_wall_seconds"] <= MAX_WALL_SECONDS
        or not 1 <= value["max_cost_microusd"] <= MAX_COST_MICROUSD
        or value.get("state") != "accepted"
    ):
        raise ValueError("winner provider acceptance is invalid")
    selection = value.get("selection")
    selected = selection.get("selected_provider") if isinstance(selection, dict) else None
    if schema == SCHEMA:
        if not _match(HEX64, value.get("provider_binding_sha256")):
            raise ValueError("winner provider binding is invalid")
        observed = (
            _baseten_selection(selection)
            if selected == "baseten"
            else _modal_selection(selection)
            if selected == "modal"
            else None
        )
    else:
        operation_wall = _gpu_operation(value.get("operation"))
        if value["max_wall_seconds"] != operation_wall:
            raise ValueError("GPU provider acceptance authority is invalid")
        observed = (
            _baseten_selection(selection)
            if selected == "baseten"
            else _modal_selection(selection)
            if selected == "modal"
            else None
        )
    if observed is None:
        raise ValueError("winner selected provider is invalid")
    reserved = _time(value["reserved_at"], "budget reservation time")
    accepted = _time(value["accepted_at"], "provider acceptance time")
    create_deadline = _time(value["create_not_after"], "provider create deadline")
    provider_deadline = _time(value["provider_not_after"], "provider deadline")
    if not (
        observed <= reserved <= accepted < create_deadline <= provider_deadline
        and provider_deadline - accepted
        <= dt.timedelta(seconds=value["max_wall_seconds"])
    ):
        raise ValueError("winner provider acceptance timing is invalid")
    return value


def encode(value):
    validate(value)
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def sha256(value):
    return hashlib.sha256(encode(value)).hexdigest()
