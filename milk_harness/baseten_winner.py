"""Jobs-owned, provider-specific Baseten winner lifecycle."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid

from deploy.baseten import winner as contract
from deploy.modal import admit as admission_probe
from milk_harness.budget import H100_RESERVATION_RATE_MICROUSD_PER_MINUTE
from milk_harness.evidence import HEX64, canonical_json, create_same
from milk_harness.image_admission import (
    STUDENT_IMAGE_REPOSITORY,
    load_published_private_image_release,
)
from milk_harness.logs import LogArchive
from milk_harness.provider_acceptance import (
    MAX_COST_MICROUSD,
    MAX_WALL_SECONDS,
    SCHEMA as ACCEPTANCE_SCHEMA,
    TEAM_NAME,
    encode as provider_acceptance_bytes,
    sha256 as provider_acceptance_sha256,
    validate_route_retirement,
    winner_run_id as neutral_winner_run_id,
)

PREFLIGHT_SCHEMA = "milk.baseten-team-preflight.v1"
DEFINITION_SCHEMA = "milk.baseten-winner-execution-definition.v1"
CANARY_BASIS_POINTS = 100
CANARY_SECONDS = 15 * 60
ZERO_ROUTE_SECONDS = 60
ROUTE_AUTHORITY_SECONDS = CANARY_SECONDS + ZERO_ROUTE_SECONDS
CREATE_WINDOW_SECONDS = 2 * 60
MAX_MUTATION_ATTEMPTS = 3
MAX_LOG_RECORDS = 1000
MAX_LOG_BYTES = 8 * 1024 * 1024
MAX_PROVIDER_BYTES = 1024 * 1024
MAX_PROVIDER_HEADER_BYTES = 16 * 1024
MAX_TRUSS_WAIT_SECONDS = 10 * 60
API_ROOT = "https://api.baseten.co/v1"
MANAGEMENT_OPENAPI_SHA256 = (
    "dbf621164d395cce375288447e10ed5ef5737a16773636a892297ee92434f9f8"
)
SCOPE_FIELDS = ("tenant_id", "project_id", "environment_id", "workload_id")
VARIANTS = ("bf16", "dynamic_fp8", "static_fp8")
TEAM_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
GATEWAY_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z\Z"
)
RESERVATION_FIELDS = {
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
PROVIDER_PASS_FIELDS = {
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
CREATE_AUTHORIZATION_FIELDS = {
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
MODEL_FIELDS = {
    "id",
    "created_at",
    "name",
    "deployments_count",
    "production_deployment_id",
    "development_deployment_id",
    "instance_type_name",
    "team_name",
}


def _candidate_key_name(run_id, attempt):
    if (
        not _matches(HEX64, run_id)
        or type(attempt) is not int
        or not 0 <= attempt < MAX_MUTATION_ATTEMPTS
    ):
        raise ValueError("Baseten candidate-key identity is invalid")
    return f"milk-{run_id[:56]}-{attempt:02d}"


def _api_key_metadata(value):
    response = _provider_object(value, "Baseten API-key metadata")
    keys = response.get("keys")
    if set(response) != {"keys"} or not isinstance(keys, list):
        raise ValueError("Baseten API-key metadata is invalid")
    prefixes = set()
    for item in keys:
        if (
            not isinstance(item, dict)
            or set(item) != contract.API_KEY_KEYS
            or not isinstance(item.get("prefix"), str)
            or not item["prefix"]
            or len(item["prefix"]) > 256
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E
                for character in item["prefix"]
            )
            or not isinstance(item.get("name"), (str, type(None)))
            or item.get("type")
            not in {
                "PERSONAL",
                "WORKSPACE_MANAGE_ALL",
                "WORKSPACE_EXPORT_METRICS",
                "WORKSPACE_INVOKE",
            }
            or not isinstance(item.get("model_ids"), (list, type(None)))
            or item.get("team_name") is not None
            and not isinstance(item["team_name"], str)
            or item.get("owner") is not None
            and not isinstance(item["owner"], dict)
            or item["prefix"] in prefixes
        ):
            raise ValueError("Baseten API-key metadata is invalid")
        prefixes.add(item["prefix"])
    return keys


def _candidate_key_metadata(value, name, model_id, team_name):
    matches = [item for item in _api_key_metadata(value) if item["name"] == name]
    if not matches:
        return None
    if len(matches) != 1 or (
        matches[0]["type"] != "WORKSPACE_INVOKE"
        or matches[0]["model_ids"] != [model_id]
        or matches[0]["team_name"] != team_name
        or matches[0]["owner"] is not None
    ):
        raise RuntimeError("Baseten candidate-key identity is ambiguous")
    return matches[0]


def _candidate_key_prefix_metadata(value, prefix, model_id, team_name):
    matches = [
        item for item in _api_key_metadata(value) if item["prefix"] == prefix
    ]
    if not matches:
        return None
    if len(matches) != 1 or (
        matches[0]["type"] != "WORKSPACE_INVOKE"
        or matches[0]["model_ids"] != [model_id]
        or matches[0]["team_name"] != team_name
        or matches[0]["owner"] is not None
    ):
        raise RuntimeError("Baseten candidate-key identity is ambiguous")
    return matches[0]


def _strict(raw, maximum, label, *, line=False):
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(raw) <= maximum:
        raise ValueError(f"{label} must be bounded bytes")
    body = bytes(raw)
    if line:
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


def _stored(raw, label):
    value = _strict(raw, 1024 * 1024, label, line=True)
    if canonical_json(value) != raw:
        raise ValueError(f"{label} is not canonical evidence JSON")
    return value


def _digest(value):
    raw = value if isinstance(value, bytes) else canonical_json(value)
    return hashlib.sha256(raw).hexdigest()


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _canonical_uuid(value):
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _response_fingerprint(value, label):
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    else:
        raw = canonical_json(value)
    if not 1 <= len(raw) <= contract.MAX_RESPONSE_BYTES:
        raise ValueError(f"{label} response is not bounded")
    return {"response_bytes": len(raw), "response_sha256": _digest(raw)}


def _utc(value, label):
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be UTC")
    value = value.astimezone(dt.timezone.utc)
    if value.microsecond:
        raise ValueError(f"{label} must have whole-second precision")
    return value


def _utc_text(value):
    return _utc(value, "timestamp").isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _gateway_time(value, label):
    if not isinstance(value, str) or GATEWAY_TIME.fullmatch(value) is None:
        raise ValueError(f"{label} must be gateway UTC RFC3339")
    try:
        return dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be gateway UTC RFC3339") from error


def _valid_uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError):
        return False
    return parsed.int != 0 and str(parsed) == value


def _minutes(seconds):
    return max(1, math.ceil(seconds / 60))


def _validate_scope(value):
    if (
        not isinstance(value, dict)
        or tuple(value) != SCOPE_FIELDS
        or any(not _valid_uuid(value.get(field)) for field in SCOPE_FIELDS)
    ):
        raise ValueError("winner gateway scope is invalid")


def _validate_artifact(value, label):
    if (
        not isinstance(value, dict)
        or set(value) != {"object_key", "sha256", "bytes"}
        or not isinstance(value.get("object_key"), str)
        or not value["object_key"]
        or not _matches(HEX64, value.get("sha256"))
        or type(value.get("bytes")) is not int
        or value["bytes"] <= 0
    ):
        raise ValueError(f"winner {label} is invalid")


def _validate_authority(authority, claim):
    if not isinstance(authority, dict) or set(authority) != {
        "schema_version",
        "provider_policy",
        "provider_terms_sha256",
        "runtime_image_reference",
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
    }:
        raise ValueError("winner deployment authority fields are invalid")
    if (
        authority.get("schema_version")
        != "dragontales.winner-deployment-authority.v1"
        or authority.get("provider_policy")
        != {"primary": "baseten", "fallback": "modal"}
        or any(
            not _matches(HEX64, authority.get(field))
            for field in (
                "provider_terms_sha256",
                "admission_program_sha256",
                "signing_public_key_hex",
            )
        )
        or authority.get("runtime_image_reference")
        != claim["runtime_image_reference"]
        or authority.get("admission_program_sha256")
        != hashlib.sha256(contract.ADMISSION_PATH.read_bytes()).hexdigest()
        or authority.get("authorization_not_after") != claim["expires_at"]
        or type(authority.get("max_wall_seconds")) is not int
        or not 60 <= authority["max_wall_seconds"] <= MAX_WALL_SECONDS
        or type(authority.get("max_cost_microusd")) is not int
        or not 1 <= authority["max_cost_microusd"] <= MAX_COST_MICROUSD
        or type(authority.get("allow_private_candidate_http")) is not bool
        or not isinstance(authority.get("signing_key_id"), str)
        or not authority["signing_key_id"]
        or type(authority.get("candidate_max_in_flight")) is not int
        or authority["candidate_max_in_flight"] <= 0
        or authority.get("canary_candidate_basis_points") != CANARY_BASIS_POINTS
        or authority.get("canary_valid_for_seconds") != CANARY_SECONDS
        or authority.get("max_input_utf8_bytes") != 2048
        or authority.get("max_input_messages") != 16
        or authority.get("max_input_request_bytes") != 16384
        or authority.get("route_schema_version") != "dragontales.route.v3"
        or authority.get("winner_admission_schema_version")
        != contract.RECEIPT_SCHEMA
    ):
        raise ValueError("winner deployment authority is invalid")


def _validate_gateway(launch_raw, claim_raw):
    launch = _strict(launch_raw, 16 * 1024, "winner gateway launch")
    claim = _strict(claim_raw, 16 * 1024, "winner gateway claim")
    if set(launch) != {
        "schema_version",
        "student_job_id",
        "student_result_sha256",
        "winner",
        "deployment_claim_object_key",
        "deployment_claim_sha256",
        "provider_binding_sha256",
        "provider_policy",
        "runtime_image_reference",
        "max_wall_seconds",
        "max_cost_microusd",
        "expires_at",
    } or set(claim) != {
        "schema_version",
        "scope",
        "student_job_id",
        "student_claim_sha256",
        "student_result_sha256",
        "winner",
        "model_manifest",
        "dev_receipt",
        "runtime_image_reference",
        "provider_binding_sha256",
        "authority",
        "claimed_at",
        "expires_at",
    }:
        raise ValueError("winner gateway fields are invalid")
    _validate_scope(claim.get("scope"))
    _validate_artifact(claim.get("model_manifest"), "model manifest")
    _validate_artifact(claim.get("dev_receipt"), "DEV receipt")
    _validate_authority(claim.get("authority"), claim)
    claim_sha256 = hashlib.sha256(claim_raw).hexdigest()
    if (
        launch.get("schema_version")
        != "dragontales.student-winner-deployment-launch.v1"
        or claim.get("schema_version")
        != "dragontales.student-winner-deployment-claim.v1"
        or launch.get("deployment_claim_sha256") != claim_sha256
        or launch.get("student_job_id") != claim.get("student_job_id")
        or launch.get("student_result_sha256")
        != claim.get("student_result_sha256")
        or launch.get("winner") != claim.get("winner")
        or launch.get("provider_binding_sha256")
        != claim.get("provider_binding_sha256")
        or launch.get("provider_policy")
        != claim["authority"]["provider_policy"]
        or launch.get("runtime_image_reference")
        != claim.get("runtime_image_reference")
        or launch.get("max_wall_seconds")
        != claim["authority"]["max_wall_seconds"]
        or launch.get("max_cost_microusd")
        != claim["authority"]["max_cost_microusd"]
        or launch.get("expires_at") != claim.get("expires_at")
        or not _matches(HEX64, claim.get("student_job_id"))
        or not _matches(HEX64, claim.get("student_claim_sha256"))
        or not _matches(HEX64, claim.get("student_result_sha256"))
        or not _matches(HEX64, claim.get("provider_binding_sha256"))
        or claim.get("winner") not in VARIANTS
        or not isinstance(launch.get("deployment_claim_object_key"), str)
        or not launch["deployment_claim_object_key"].endswith(
            f"/jobs/student/{claim['student_job_id']}/winner-deployment/claim.json"
        )
    ):
        raise ValueError("winner gateway launch and claim differ")
    claimed_at = _gateway_time(claim["claimed_at"], "winner claimed_at")
    expires_at = _gateway_time(claim["expires_at"], "winner expires_at")
    if claimed_at >= expires_at:
        raise ValueError("winner gateway authority interval is invalid")
    image = contract.shared.immutable_ghcr_image(claim["runtime_image_reference"])
    if image.rpartition("@sha256:")[0] != STUDENT_IMAGE_REPOSITORY:
        raise ValueError("winner runtime is not the private student image")
    return launch, claim


def _scope_prefix(scope):
    _validate_scope(scope)
    return "dt/v2/" + "/".join(scope[field] for field in SCOPE_FIELDS)


def _validate_creation_authorities(
    provider_pass_claim_raw,
    create_authorization_raw,
    campaign_id,
    scope,
):
    provider_pass = _stored(provider_pass_claim_raw, "provider pass claim")
    authorization = _stored(
        create_authorization_raw,
        "provider create authorization",
    )
    scope_prefix = _scope_prefix(scope)
    authorization_sha256 = hashlib.sha256(create_authorization_raw).hexdigest()
    if (
        set(provider_pass) != PROVIDER_PASS_FIELDS
        or provider_pass.get("schema_version")
        != "milk.provider-jobs-pass-claim.v1"
        or provider_pass.get("campaign_id") != campaign_id
        or provider_pass.get("scope_prefix") != scope_prefix
        or any(
            not _matches(HEX64, provider_pass.get(field))
            for field in (
                "owner_id",
                "holder_id",
                "lease_token_sha256",
                "create_authorization_sha256",
                "evidence_store_identity_sha256",
                "create_authority_store_location_sha256",
            )
        )
        or type(provider_pass.get("lease_revision")) is not int
        or provider_pass["lease_revision"] < 1
        or provider_pass.get("create_authorization_sha256")
        != authorization_sha256
        or provider_pass.get("provider_creates_authorized") is not True
        or set(authorization) != CREATE_AUTHORIZATION_FIELDS
        or authorization.get("schema_version")
        != "milk.provider-create-authorization.v1"
        or authorization.get("campaign_id") != campaign_id
        or authorization.get("scope_prefix") != scope_prefix
        or any(
            authorization.get(field) != provider_pass.get(field)
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
        or not _matches(HEX64, authorization.get("confirmation_sha256"))
    ):
        raise ValueError("winner create authority is invalid")
    acquired = _gateway_time(
        authorization.get("lease_acquired_at"),
        "provider lease acquired_at",
    )
    claimed = _gateway_time(provider_pass.get("claimed_at"), "provider pass claimed_at")
    expires = _gateway_time(
        provider_pass.get("lease_expires_at"),
        "provider lease expires_at",
    )
    if not acquired <= claimed < expires:
        raise ValueError("winner create authority interval is invalid")
    return provider_pass, authorization


def winner_run_id(
    *,
    campaign_id,
    launch_raw,
    claim_raw,
    outbox_sha256,
    image_release_sha256,
    image_admission_sha256,
):
    if not _matches(HEX64, campaign_id):
        raise ValueError("campaign ID must be lowercase hex64")
    if any(
        not _matches(HEX64, value)
        for value in (outbox_sha256, image_release_sha256, image_admission_sha256)
    ):
        raise ValueError("winner immutable authority digest is invalid")
    launch, unused_claim = _validate_gateway(launch_raw, claim_raw)
    del unused_claim
    return neutral_winner_run_id(
        campaign_id=campaign_id,
        claim_sha256=launch["deployment_claim_sha256"],
        outbox_sha256=outbox_sha256,
        student_job_id=launch["student_job_id"],
        student_result_sha256=launch["student_result_sha256"],
        winner=launch["winner"],
        max_wall_seconds=launch["max_wall_seconds"],
        max_cost_microusd=launch["max_cost_microusd"],
        image_release_sha256=image_release_sha256,
        image_admission_sha256=image_admission_sha256,
    )


def reservation_microusd(launch_raw, claim_raw):
    launch, unused_claim = _validate_gateway(launch_raw, claim_raw)
    del unused_claim
    amount = (
        _minutes(launch["max_wall_seconds"])
        * H100_RESERVATION_RATE_MICROUSD_PER_MINUTE
    )
    if amount > launch["max_cost_microusd"]:
        raise ValueError("winner budget authority cannot cover its wall bound")
    return amount


def baseten_preflight_receipt(
    team_name,
    team_id,
    management_key_prefix_sha256,
    teams_response_sha256,
    api_keys_response_sha256,
    observed_at,
):
    if not _matches(TEAM_NAME, team_name):
        raise ValueError("Baseten team name is invalid")
    if not _matches(TEAM_ID, team_id) or any(
        not _matches(HEX64, value)
        for value in (
            management_key_prefix_sha256,
            teams_response_sha256,
            api_keys_response_sha256,
        )
    ):
        raise ValueError("Baseten preflight response digest is invalid")
    return {
        "schema_version": PREFLIGHT_SCHEMA,
        "provider": "baseten",
        "team_name": team_name,
        "team_id": team_id,
        "management_openapi_sha256": MANAGEMENT_OPENAPI_SHA256,
        "management_key_prefix_sha256": management_key_prefix_sha256,
        "teams_response_sha256": teams_response_sha256,
        "api_keys_response_sha256": api_keys_response_sha256,
        "outcome": "ready",
        "observed_at": _utc_text(_utc(observed_at, "preflight time")),
    }


def baseten_unavailable_preflight_receipt(
    team_name,
    reason,
    status,
    observed_at,
):
    if not _matches(TEAM_NAME, team_name):
        raise ValueError("Baseten team name is invalid")
    if (
        reason == "timeout"
        and status is not None
        or reason == "rate_limited"
        and status != 429
        or reason == "server_unavailable"
        and (type(status) is not int or not 500 <= status <= 599)
        or reason not in {"timeout", "rate_limited", "server_unavailable"}
    ):
        raise ValueError("Baseten retryable preflight outcome is invalid")
    observed_at = _utc_text(_utc(observed_at, "preflight time"))
    evidence = {
        "schema_version": "milk.baseten-team-preflight-error-evidence.v1",
        "management_openapi_sha256": MANAGEMENT_OPENAPI_SHA256,
        "reason": reason,
        "status": status,
        "observed_at": observed_at,
    }
    return {
        "schema_version": PREFLIGHT_SCHEMA,
        "provider": "baseten",
        "team_name": team_name,
        "management_openapi_sha256": MANAGEMENT_OPENAPI_SHA256,
        "outcome": "retryable_unavailable",
        "reason": reason,
        "status": status,
        "evidence_sha256": _digest(evidence),
        "observed_at": observed_at,
    }


def _validate_reservation(reservation, campaign_id, run_id, team_name, amount):
    if not isinstance(reservation, dict) or set(reservation) != RESERVATION_FIELDS:
        raise ValueError("winner budget reservation fields are invalid")
    if (
        reservation.get("schema_version")
        != "milk.campaign-budget-reservation.v4"
        or reservation.get("campaign_id") != campaign_id
        or reservation.get("run_id") != run_id
        or reservation.get("amount_microusd") != amount
        or not _matches(HEX64, reservation.get("preparation_sha256"))
        or type(reservation.get("provider_deadline_epoch_seconds")) is not int
        or reservation["provider_deadline_epoch_seconds"] <= 0
        or not _matches(HEX64, reservation.get("intent_sha256"))
        or reservation.get("state") != "reserved"
        or reservation.get("provider_role") != "serving"
        or reservation.get("provider_identity")
        != {"provider": "baseten", "team_name": team_name}
        or not _matches(HEX64, reservation.get("pricing_receipt_sha256"))
        or type(reservation.get("provider_rate_microusd_per_minute")) is not int
        or not 1
        <= reservation["provider_rate_microusd_per_minute"]
        <= H100_RESERVATION_RATE_MICROUSD_PER_MINUTE
        or reservation.get("reservation_rate_microusd_per_minute")
        != H100_RESERVATION_RATE_MICROUSD_PER_MINUTE
    ):
        raise ValueError("winner budget reservation is invalid")
    return reservation


def provider_acceptance(
    *,
    campaign_id,
    run_id,
    team_name,
    launch_raw,
    claim_raw,
    outbox_sha256,
    image_release_sha256,
    image_admission_sha256,
    preflight,
    reservation,
    provider_pass_claim_raw,
    create_authorization_raw,
    reserved_at,
    accepted_at,
):
    launch, claim = _validate_gateway(launch_raw, claim_raw)
    expected_run_id = winner_run_id(
        campaign_id=campaign_id,
        launch_raw=launch_raw,
        claim_raw=claim_raw,
        outbox_sha256=outbox_sha256,
        image_release_sha256=image_release_sha256,
        image_admission_sha256=image_admission_sha256,
    )
    if run_id != expected_run_id:
        raise ValueError("winner run ID is not provider-neutral")
    amount = reservation_microusd(launch_raw, claim_raw)
    _validate_reservation(reservation, campaign_id, run_id, team_name, amount)
    provider_pass, unused_authorization = _validate_creation_authorities(
        provider_pass_claim_raw,
        create_authorization_raw,
        campaign_id,
        claim["scope"],
    )
    del unused_authorization
    expected_preflight = baseten_preflight_receipt(
        team_name,
        preflight.get("team_id"),
        preflight.get("management_key_prefix_sha256"),
        preflight.get("teams_response_sha256"),
        preflight.get("api_keys_response_sha256"),
        _gateway_time(preflight.get("observed_at"), "preflight observed_at"),
    )
    if preflight != expected_preflight:
        raise ValueError("Baseten primary preflight receipt is invalid")
    reserved_at = _utc(reserved_at, "budget reserved_at")
    accepted_at = _utc(accepted_at, "provider accepted_at")
    create_not_after = accepted_at + dt.timedelta(seconds=CREATE_WINDOW_SECONDS)
    provider_not_after = dt.datetime.fromtimestamp(
        reservation["provider_deadline_epoch_seconds"],
        tz=dt.timezone.utc,
    )
    preflight_at = _gateway_time(preflight["observed_at"], "preflight observed_at")
    claim_expires = _gateway_time(claim["expires_at"], "winner expires_at")
    pass_claimed = _gateway_time(
        provider_pass["claimed_at"],
        "provider pass claimed_at",
    )
    pass_expires = _gateway_time(
        provider_pass["lease_expires_at"],
        "provider lease expires_at",
    )
    if not (
        pass_claimed <= reserved_at
        and preflight_at <= reserved_at <= accepted_at < create_not_after
        <= provider_not_after <= claim_expires
        and create_not_after <= pass_expires
        and provider_not_after - accepted_at
        <= dt.timedelta(seconds=launch["max_wall_seconds"])
    ):
        raise ValueError("winner provider acceptance interval is invalid")
    value = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "claim_sha256": launch["deployment_claim_sha256"],
        "outbox_sha256": outbox_sha256,
        "provider_binding_sha256": launch["provider_binding_sha256"],
        "selection": {
            "selected_provider": "baseten",
            "provider_identity": {
                "provider": "baseten",
                "team_name": team_name,
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": _digest(preflight),
                "observed_at": preflight["observed_at"],
            },
        },
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
        "reserved_at": _utc_text(reserved_at),
        "accepted_at": _utc_text(accepted_at),
        "create_not_after": _utc_text(create_not_after),
        "provider_not_after": _utc_text(provider_not_after),
        "max_wall_seconds": launch["max_wall_seconds"],
        "max_cost_microusd": launch["max_cost_microusd"],
        "state": "accepted",
    }
    provider_acceptance_bytes(value)
    return value


def validate_provider_acceptance(
    acceptance,
    *,
    campaign_id,
    team_name,
    launch_raw,
    claim_raw,
    outbox_sha256,
    preflight,
    reservation,
    provider_pass_claim_raw,
    create_authorization_raw,
):
    provider_acceptance_bytes(acceptance)
    expected = provider_acceptance(
        campaign_id=campaign_id,
        run_id=acceptance.get("run_id"),
        team_name=team_name,
        launch_raw=launch_raw,
        claim_raw=claim_raw,
        outbox_sha256=outbox_sha256,
        image_release_sha256=acceptance.get("image_release_sha256"),
        image_admission_sha256=acceptance.get("image_admission_sha256"),
        preflight=preflight,
        reservation=reservation,
        provider_pass_claim_raw=provider_pass_claim_raw,
        create_authorization_raw=create_authorization_raw,
        reserved_at=_gateway_time(acceptance.get("reserved_at"), "reserved_at"),
        accepted_at=_gateway_time(acceptance.get("accepted_at"), "accepted_at"),
    )
    if acceptance != expected:
        raise ValueError("winner provider acceptance is invalid")
    return acceptance


def _identity(value, definition):
    if not isinstance(value, dict) or set(value) != {
        "model_id",
        "deployment_id",
        "execution_name",
        "labels",
        "endpoint",
    }:
        raise ValueError("Baseten winner execution identity fields are invalid")
    if (
        not _matches(contract.MODEL_ID, value.get("model_id"))
        or not _matches(contract.DEPLOYMENT_ID, value.get("deployment_id"))
        or value.get("execution_name") != definition["execution_name"]
        or value.get("labels") != definition["labels"]
        or value.get("endpoint")
        != (
            f"https://model-{value['model_id']}.api.baseten.co/deployment/"
            f"{value['deployment_id']}/sync/v1/chat/completions"
        )
    ):
        raise ValueError("Baseten winner execution identity is invalid")
    return value


def _execution_definition(campaign_id, team_name, launch, claim, acceptance, values):
    run_id = acceptance["run_id"]
    truss = contract.truss_config(values, run_id)
    truss_dir = f"/tmp/milk-winner/{run_id}/truss"
    argv = contract.push_argv(
        truss_dir,
        run_id,
        launch["deployment_claim_sha256"],
        team_name,
    )
    return (
        {
            "schema_version": DEFINITION_SCHEMA,
            "campaign_id": campaign_id,
            "run_id": run_id,
            "provider": "baseten",
            "team_name": team_name,
            "provider_acceptance_sha256": provider_acceptance_sha256(acceptance),
            "student_job_id": launch["student_job_id"],
            "student_result_sha256": launch["student_result_sha256"],
            "winner": launch["winner"],
            "model_manifest_sha256": claim["model_manifest"]["sha256"],
            "runtime_image_reference": launch["runtime_image_reference"],
            "model_alias": values["model_alias"],
            "model_name": contract.model_name(run_id),
            "execution_name": contract.execution_name(run_id),
            "labels": contract.labels(run_id, launch["deployment_claim_sha256"]),
            "truss_config_sha256": hashlib.sha256(truss.encode()).hexdigest(),
            "push_argv_sha256": _digest(argv),
        },
        truss,
        argv,
    )


def _deployment_summary(value):
    return {
        "model_id": value["model_id"],
        "deployment_id": value["id"],
        "execution_name": value["name"],
        "status": value["status"],
        "active_replica_count": value["active_replica_count"],
        "instance_type_name": value["instance_type_name"],
        "autoscaling_settings": {
            key: value["autoscaling_settings"].get(key)
            for key in contract.SERVING_AUTOSCALING
        },
    }


class _LogEvidence:
    def __init__(self, store, prefix, run_id):
        self.store = store
        self.prefix = prefix
        self.run_id = run_id

    def bytes(self, relative, body, content_type="application/octet-stream"):
        return create_same(
            self.store,
            f"{self.prefix}/{relative}",
            body,
            content_type,
        )


def _mutation_intent(
    value,
    *,
    schema,
    acceptance_sha256,
    deployment_id,
    attempt,
    settings=None,
):
    fields = {
        "schema_version",
        "provider_acceptance_sha256",
        "deployment_id",
        "attempt",
        "requested_at",
    }
    if settings is not None:
        fields.add("settings")
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema_version") != schema
        or value.get("provider_acceptance_sha256") != acceptance_sha256
        or value.get("deployment_id") != deployment_id
        or value.get("attempt") != attempt
        or settings is not None
        and value.get("settings") != settings
    ):
        raise ValueError("stored Baseten mutation intent is invalid")
    _gateway_time(value.get("requested_at"), "Baseten mutation requested_at")
    return value


def _mutation_result(
    value,
    *,
    schema,
    acceptance_sha256,
    deployment_id,
    attempt,
    with_status=False,
):
    if not isinstance(value, dict):
        raise ValueError("stored Baseten mutation result is invalid")
    common = {
        "schema_version",
        "provider_acceptance_sha256",
        "deployment_id",
        "attempt",
        "observed_at",
        "state",
    }
    accepted = common | {"response_bytes", "response_sha256"}
    if with_status:
        accepted.add("status")
    fields = accepted if value.get("state") == "accepted" else common | {
        "error_class_sha256"
    }
    if (
        set(value) != fields
        or value.get("schema_version") != schema
        or value.get("provider_acceptance_sha256") != acceptance_sha256
        or value.get("deployment_id") != deployment_id
        or value.get("attempt") != attempt
        or value.get("state") not in {"accepted", "ambiguous"}
        or value.get("state") == "accepted"
        and (
            type(value.get("response_bytes")) is not int
            or not 1 <= value["response_bytes"] <= contract.MAX_RESPONSE_BYTES
            or not _matches(HEX64, value.get("response_sha256"))
            or with_status
            and value.get("status") not in {"ACCEPTED", "QUEUED", "UNCHANGED"}
        )
        or value.get("state") == "ambiguous"
        and not _matches(HEX64, value.get("error_class_sha256"))
    ):
        raise ValueError("stored Baseten mutation result is invalid")
    _gateway_time(value.get("observed_at"), "Baseten mutation observed_at")
    return value


def _provider_object(raw, label):
    if isinstance(raw, dict):
        try:
            encoded = json.dumps(
                raw,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        except (TypeError, ValueError) as error:
            raise ValueError(f"{label} is not strict JSON") from error
        if not 1 <= len(encoded) <= MAX_PROVIDER_BYTES:
            raise ValueError(f"{label} is not bounded")
        return raw
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(raw) <= MAX_PROVIDER_BYTES:
        raise ValueError(f"{label} is not bounded bytes")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"{label} contains a duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(
            bytes(raw),
            object_pairs_hook=pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not strict JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


class _RetryableBasetenUnavailable(RuntimeError):
    def __init__(self, reason, status):
        self.reason = reason
        self.status = status
        super().__init__("Baseten is retryably unavailable")


class BasetenWinnerRuntime:
    """Exact Baseten I/O used only through ``BasetenWinnerLifecycle``."""

    def __init__(
        self,
        *,
        api_key,
        team_name,
        timeout_seconds=30,
        opener=None,
        runner=subprocess.run,
    ):
        if (
            not isinstance(api_key, str)
            or not 1 <= len(api_key) <= 4096
            or any(ord(character) < 0x21 or ord(character) > 0x7E for character in api_key)
        ):
            raise ValueError("BASETEN_API_KEY is required")
        if not _matches(TEAM_NAME, team_name):
            raise ValueError("Baseten team name is invalid")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120:
            raise ValueError("Baseten timeout must be in 1..=120 seconds")
        if not callable(runner):
            raise ValueError("Baseten Truss runner is required")
        self.api_key = api_key
        self.team_name = team_name
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_NoRedirect)
        self.runner = runner

    def _team(self, team_name):
        if team_name != self.team_name:
            raise ValueError("Baseten runtime team differs from lifecycle authority")

    def _request(self, method, path, *, body=None, query=None):
        if method not in {"GET", "POST", "PATCH", "DELETE"}:
            raise ValueError("Baseten winner method is not allowed")
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or "//" in path
            or any(part in {"", ".", ".."} for part in path[1:].split("/"))
        ):
            raise ValueError("Baseten winner path is invalid")
        if method == "GET":
            if body is not None:
                raise ValueError("Baseten winner GET cannot have a body")
            encoded = None
        else:
            if body is None:
                encoded = None
            elif not isinstance(body, dict):
                raise ValueError("Baseten winner mutation requires a typed body")
            else:
                encoded = canonical_json(body)
        query = {} if query is None else query
        if not isinstance(query, dict) or any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, (str, int))
            or type(value) is bool
            for key, value in query.items()
        ):
            raise ValueError("Baseten winner query is invalid")
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
        try:
            response = self.opener.open(request, timeout=self.timeout_seconds)
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            if status == 429:
                raise _RetryableBasetenUnavailable("rate_limited", status) from error
            if type(status) is int and 500 <= status <= 599:
                raise _RetryableBasetenUnavailable(
                    "server_unavailable", status
                ) from error
            raise RuntimeError("Baseten returned a hard non-200 response") from error
        except TimeoutError as error:
            raise _RetryableBasetenUnavailable("timeout", None) from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, TimeoutError):
                raise _RetryableBasetenUnavailable("timeout", None) from error
            raise
        try:
            status = response.getcode()
            if status != 200:
                if status == 429:
                    raise _RetryableBasetenUnavailable("rate_limited", status)
                if type(status) is int and 500 <= status <= 599:
                    raise _RetryableBasetenUnavailable(
                        "server_unavailable", status
                    )
                raise RuntimeError("Baseten returned a hard non-200 response")
            headers = list(response.headers.items())
            if (
                sum(len(str(name)) + len(str(value)) + 4 for name, value in headers)
                > MAX_PROVIDER_HEADER_BYTES
                or response.getheader("Content-Type", "").strip().lower()
                != "application/json"
                or response.getheader("Content-Encoding") not in {None, ""}
            ):
                raise ValueError("Baseten response headers are invalid")
            raw = response.read(MAX_PROVIDER_BYTES + 1)
        finally:
            response.close()
        if not raw or len(raw) > MAX_PROVIDER_BYTES:
            raise ValueError("Baseten response is empty or oversized")
        _provider_object(raw, "Baseten response")
        return raw

    @staticmethod
    def _model(value, *, model_id=None, name=None, team_name=None):
        if (
            not isinstance(value, dict)
            or set(value) != MODEL_FIELDS
            or not _matches(contract.MODEL_ID, value.get("id"))
            or model_id is not None
            and value["id"] != model_id
            or not isinstance(value.get("created_at"), str)
            or name is not None
            and value.get("name") != name
            or not isinstance(value.get("name"), str)
            or type(value.get("deployments_count")) is not int
            or value["deployments_count"] < 0
            or not isinstance(value.get("production_deployment_id"), (str, type(None)))
            or not isinstance(value.get("development_deployment_id"), (str, type(None)))
            or not isinstance(value.get("instance_type_name"), (str, type(None)))
            or team_name is not None
            and value.get("team_name") != team_name
            or not _matches(TEAM_NAME, value.get("team_name"))
        ):
            raise ValueError("Baseten model identity is invalid")
        return value

    def _model_by_id(self, model_id):
        if not _matches(contract.MODEL_ID, model_id):
            raise ValueError("Baseten model ID is invalid")
        raw = self._request("GET", f"/models/{model_id}")
        return self._model(
            _provider_object(raw, "Baseten model"),
            model_id=model_id,
            team_name=self.team_name,
        )

    def _preflight(self, team_name, observed_at):
        self._team(team_name)
        teams_raw = self._request("GET", "/teams", query={"name": team_name})
        teams = _provider_object(teams_raw, "Baseten teams").get("teams")
        if not isinstance(teams, list):
            raise ValueError("configured Baseten team is not uniquely visible")
        matches = []
        for team in teams:
            if (
                not isinstance(team, dict)
                or set(team) != {"id", "name", "default", "created_at"}
                or not _matches(TEAM_ID, team.get("id"))
                or not _matches(TEAM_NAME, team.get("name"))
                or type(team.get("default")) is not bool
                or not isinstance(team.get("created_at"), str)
            ):
                raise ValueError("configured Baseten team is invalid")
            if team["name"] == team_name:
                matches.append(team)
        if len(matches) != 1:
            raise ValueError("configured Baseten team is not uniquely visible")
        team = matches[0]
        keys_raw = self._request("GET", "/api_keys")
        keys = _provider_object(keys_raw, "Baseten API keys").get("keys")
        if not isinstance(keys, list):
            raise ValueError("Baseten API-key metadata is invalid")
        matches = []
        for key in keys:
            if not isinstance(key, dict) or set(key) != contract.API_KEY_KEYS:
                raise ValueError("Baseten API-key metadata is invalid")
            prefix = key.get("prefix")
            if not isinstance(prefix, str) or not prefix:
                raise ValueError("Baseten API-key metadata is invalid")
            if self.api_key.startswith(prefix):
                matches.append(key)
        if (
            len(matches) != 1
            or matches[0].get("type") != "WORKSPACE_MANAGE_ALL"
            or matches[0].get("team_name") != team_name
            or matches[0].get("model_ids") is not None
            or matches[0].get("owner") is not None
        ):
            raise ValueError("Baseten management key is not team-scoped full access")
        return baseten_preflight_receipt(
            team_name,
            team["id"],
            hashlib.sha256(matches[0]["prefix"].encode()).hexdigest(),
            hashlib.sha256(teams_raw).hexdigest(),
            hashlib.sha256(keys_raw).hexdigest(),
            observed_at,
        )

    def _lookup_exact(self, team_name, model_name, execution_name, labels):
        self._team(team_name)
        if (
            model_name != execution_name
            or not isinstance(labels, dict)
            or set(labels) != {"milk-claim", "milk-run"}
        ):
            raise ValueError("Baseten deterministic lookup identity is invalid")
        models_raw = self._request("GET", "/models", query={"name": model_name})
        models = _provider_object(models_raw, "Baseten models")
        if set(models) != {"models"} or not isinstance(models["models"], list):
            raise ValueError("Baseten models response is invalid")
        matches = [
            self._model(item, name=model_name, team_name=team_name)
            for item in models["models"]
            if isinstance(item, dict)
            and item.get("name") == model_name
            and item.get("team_name") == team_name
        ]
        if not matches:
            return None
        if len(matches) != 1:
            raise RuntimeError("Baseten deterministic model identity is ambiguous")
        model = self._model_by_id(matches[0]["id"])
        if model != matches[0]:
            raise RuntimeError("Baseten model detail differs from exact lookup")
        deployments_raw = self._request(
            "GET",
            f"/models/{model['id']}/deployments",
            query={"name": execution_name},
        )
        deployments = _provider_object(deployments_raw, "Baseten deployments")
        if set(deployments) != {"deployments"} or not isinstance(
            deployments["deployments"], list
        ):
            raise ValueError("Baseten deployments response is invalid")
        matches = [
            item
            for item in deployments["deployments"]
            if isinstance(item, dict)
            and item.get("model_id") == model["id"]
            and item.get("name") == execution_name
            and item.get("labels") == labels
            and item.get("is_production") is True
            and item.get("is_development") is False
        ]
        if not matches:
            return None
        if len(matches) != 1 or not _matches(
            contract.DEPLOYMENT_ID, matches[0].get("id")
        ):
            raise RuntimeError("Baseten deterministic deployment identity is ambiguous")
        deployment_id = matches[0]["id"]
        direct_raw = self._request(
            "GET",
            f"/models/{model['id']}/deployments/{deployment_id}",
        )
        direct = _provider_object(direct_raw, "Baseten deployment")
        if (
            direct.get("id") != deployment_id
            or direct.get("model_id") != model["id"]
            or direct.get("name") != execution_name
            or direct.get("labels") != labels
            or direct.get("is_production") is not True
            or direct.get("is_development") is not False
        ):
            raise RuntimeError("Baseten deployment detail differs from exact lookup")
        return {
            "model_id": model["id"],
            "model_version_id": deployment_id,
            "predict_url": f"https://model-{model['id']}.api.baseten.co/predict",
            "logs_url": (
                f"https://app.baseten.co/models/{model['id']}/logs/{deployment_id}"
            ),
            "is_draft": False,
        }

    def _push_truss(self, team_name, truss, argv):
        self._team(team_name)
        if (
            not isinstance(truss, str)
            or not 1 <= len(truss.encode()) <= 64 * 1024
            or not isinstance(argv, list)
            or len(argv) < 8
            or argv[0] != "/usr/local/bin/truss"
            or argv[1:3] != ["--non-interactive", "push"]
            or argv.count("--team") != 1
            or argv[argv.index("--team") + 1] != team_name
        ):
            raise ValueError("Baseten Truss execution is invalid")
        match = re.fullmatch(
            r"/tmp/milk-winner/([0-9a-f]{64})/truss",
            argv[3],
        )
        if match is None:
            raise ValueError("Baseten Truss directory is invalid")
        base = Path("/tmp/milk-winner")
        base.mkdir(mode=0o700, exist_ok=True)
        if base.is_symlink() or not base.is_dir():
            raise RuntimeError("Baseten Truss base directory is unsafe")
        run_dir = base / match.group(1)
        if run_dir.exists() or run_dir.is_symlink():
            raise RuntimeError("Baseten Truss run directory already exists")
        run_dir.mkdir(mode=0o700)
        truss_dir = run_dir / "truss"
        home_dir = run_dir / "home"
        truss_dir.mkdir(mode=0o700)
        home_dir.mkdir(mode=0o700)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(truss_dir / "config.yaml", flags, 0o600)
        with os.fdopen(descriptor, "wb") as config:
            config.write(truss.encode())
        environment = {
            "BASETEN_API_KEY": self.api_key,
            "HOME": str(home_dir),
            "PATH": "/usr/local/bin:/usr/bin:/bin",
        }
        try:
            version = self.runner(
                ["/usr/local/bin/truss", "--version"],
                capture_output=True,
                check=False,
                timeout=30,
                env=environment,
            )
            if (
                version.returncode != 0
                or version.stdout != contract.TRUSS_VERSION_OUTPUT
                or not isinstance(version.stderr, bytes)
                or len(version.stderr) > MAX_PROVIDER_BYTES
            ):
                raise RuntimeError("pinned Truss runtime verification failed")
            result = self.runner(
                list(argv),
                capture_output=True,
                check=False,
                timeout=MAX_TRUSS_WAIT_SECONDS,
                env=environment,
            )
            if (
                result.returncode != 0
                or not isinstance(result.stdout, bytes)
                or not 1 <= len(result.stdout) <= contract.MAX_RESPONSE_BYTES
                or not isinstance(result.stderr, bytes)
                or len(result.stderr) > MAX_PROVIDER_BYTES
            ):
                raise RuntimeError("Truss push failed or returned invalid output")
            return result.stdout
        finally:
            shutil.rmtree(run_dir)

    def _inspect_deployment(self, team_name, model_id, deployment_id):
        self._team(team_name)
        self._model_by_id(model_id)
        if not _matches(contract.DEPLOYMENT_ID, deployment_id):
            raise ValueError("Baseten deployment ID is invalid")
        return self._request(
            "GET",
            f"/models/{model_id}/deployments/{deployment_id}",
        )

    def _update_autoscaling(self, team_name, model_id, deployment_id, settings):
        self._team(team_name)
        self._model_by_id(model_id)
        if (
            not _matches(contract.DEPLOYMENT_ID, deployment_id)
            or settings != contract.SERVING_AUTOSCALING
        ):
            raise ValueError("Baseten autoscaling mutation is invalid")
        return self._request(
            "PATCH",
            f"/models/{model_id}/deployments/{deployment_id}/autoscaling_settings",
            body=settings,
        )

    def _read_key_metadata(self, team_name, model_id):
        self._team(team_name)
        self._model_by_id(model_id)
        return self._request("GET", "/api_keys")

    def _create_model_scoped_key(self, team_name, model_id, name):
        self._team(team_name)
        self._model_by_id(model_id)
        if (
            not isinstance(name, str)
            or not 1 <= len(name) <= 64
            or re.fullmatch(r"[a-z0-9-]+", name) is None
        ):
            raise ValueError("Baseten candidate-key name is invalid")
        raw = self._request(
            "POST",
            "/api_keys",
            body={
                "name": name,
                "type": "WORKSPACE_INVOKE",
                "model_ids": [model_id],
            },
        )
        value = _provider_object(raw, "Baseten API-key creation")
        key = value.get("api_key")
        if (
            set(value) != {"api_key"}
            or not isinstance(key, str)
            or not 1 <= len(key) <= 4096
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E
                for character in key
            )
        ):
            raise ValueError("Baseten API-key creation response is invalid")
        return key

    def _delete_api_key(self, team_name, prefix):
        self._team(team_name)
        if (
            not isinstance(prefix, str)
            or not 1 <= len(prefix) <= 256
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E
                for character in prefix
            )
        ):
            raise ValueError("Baseten candidate-key prefix is invalid")
        raw = self._request(
            "DELETE",
            f"/api_keys/{urllib.parse.quote(prefix, safe='')}",
        )
        if _provider_object(raw, "Baseten API-key revocation") != {
            "prefix": prefix
        }:
            raise RuntimeError("Baseten did not revoke the exact API key")
        return raw

    def _read_logs(self, team_name, model_id, deployment_id, limit):
        self._team(team_name)
        self._model_by_id(model_id)
        if (
            not _matches(contract.DEPLOYMENT_ID, deployment_id)
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise ValueError("Baseten deployment log request is invalid")
        raw = self._request(
            "GET",
            f"/models/{model_id}/deployments/{deployment_id}/logs",
            query={"direction": "asc", "limit": limit},
        )
        response = _provider_object(raw, "Baseten deployment logs")
        if set(response) != {"logs"} or not isinstance(response["logs"], list):
            raise ValueError("Baseten deployment logs response is invalid")
        normalized = []
        for item in response["logs"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"timestamp", "message", "replica", "request_id", "level"}
                or not isinstance(item.get("timestamp"), str)
                or not isinstance(item.get("message"), str)
                or not isinstance(item.get("replica"), str)
                or not isinstance(item.get("request_id"), (str, type(None)))
                or item.get("level") not in {None, "DEBUG", "INFO", "WARNING", "ERROR"}
            ):
                raise ValueError("Baseten deployment log line is invalid")
            normalized.append(
                {
                    "timestamp": item["timestamp"],
                    "message": item["message"],
                    "replica": item["replica"],
                }
            )
        return {"logs": normalized}

    def _run_probe(
        self,
        endpoint,
        candidate_key,
        student_job_id,
        alias,
        materialization,
    ):
        with tempfile.TemporaryDirectory(prefix="milk-winner-probe-") as temporary:
            root = Path(temporary)
            key_path = root / "candidate.key"
            materialization_path = root / "materialization.json"
            key_path.write_bytes(candidate_key.encode())
            os.chmod(key_path, 0o600)
            materialization_path.write_bytes(canonical_json(materialization))
            return admission_probe.qualify(
                endpoint,
                alias,
                student_job_id,
                key_path=key_path,
                materialize_path=materialization_path,
                expected_manifest_sha256=materialization["model_manifest_sha256"],
            )

    def _deactivate_exact(self, team_name, model_id, deployment_id):
        self._team(team_name)
        self._model_by_id(model_id)
        if not _matches(contract.DEPLOYMENT_ID, deployment_id):
            raise ValueError("Baseten deployment ID is invalid")
        raw = self._request(
            "POST",
            f"/models/{model_id}/deployments/{deployment_id}/deactivate",
        )
        if _provider_object(raw, "Baseten deactivation") != {"success": True}:
            raise RuntimeError("Baseten did not accept exact deactivation")
        return raw

    def _read_billing(self, team_name, source, accepted_at, provider_not_after):
        self._team(team_name)
        if source not in {"model", "deployment", "workspace"}:
            raise ValueError("Baseten billing source is invalid")
        return self._request(
            "GET",
            "/billing/usage_summary",
            query={"start_date": accepted_at[:10], "end_date": provider_not_after[:10]},
        )


class BasetenWinnerLifecycle:
    def __init__(
        self,
        *,
        store,
        campaign_id,
        team_name,
        request_guard,
        runtime=None,
        candidate_key_sink=None,
        now=lambda: dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
    ):
        if not _matches(HEX64, campaign_id):
            raise ValueError("campaign ID must be lowercase hex64")
        if not _matches(TEAM_NAME, team_name):
            raise ValueError("Baseten team name is invalid")
        if not callable(request_guard) or not callable(now):
            raise ValueError("winner provider guard and clock are required")
        if runtime is not None and (
            not isinstance(runtime, BasetenWinnerRuntime)
            or runtime.team_name != team_name
        ):
            raise ValueError("winner Baseten runtime differs from lifecycle authority")
        if candidate_key_sink is not None and not callable(candidate_key_sink):
            raise ValueError("winner candidate-key sink is invalid")
        self.store = store
        self.campaign_id = campaign_id
        self.team_name = team_name
        self.request_guard = request_guard
        self.runtime = runtime
        self.candidate_key_sink = candidate_key_sink
        self.now = now

    def _prefix(self, run_id):
        if not _matches(HEX64, run_id):
            raise ValueError("winner run ID is invalid")
        return f"campaigns/v1/{self.campaign_id}/winner-jobs/{run_id}"

    def _json(self, prefix, relative, value):
        return create_same(
            self.store,
            f"{prefix}/{relative}",
            canonical_json(value),
            "application/json",
        )

    def _event(self, prefix, kind, value):
        observed_at = _utc_text(_utc(self.now(), "clock"))
        event = {
            "schema_version": "milk.baseten-winner-redacted-event.v1",
            "kind": kind,
            "observed_at": observed_at,
            **value,
        }
        safe_time = observed_at.replace(":", "").replace("-", "")
        self._json(prefix, f"events/{safe_time}-{_digest(event)}.json", event)
        return event

    def _provider(self, function, *arguments):
        self.request_guard()
        return function(*arguments)

    def _io(self, supplied, name):
        if supplied is not None:
            if not callable(supplied):
                raise ValueError("Baseten lifecycle I/O is invalid")
            return supplied
        if self.runtime is None:
            raise ValueError("concrete Baseten winner runtime is required")
        return getattr(self.runtime, name)

    def preflight(self):
        if self.runtime is None:
            raise ValueError("concrete Baseten winner runtime is required")
        observed_at = _utc(self.now(), "clock")
        try:
            return self._provider(
                self.runtime._preflight,
                self.team_name,
                observed_at,
            )
        except _RetryableBasetenUnavailable as error:
            return baseten_unavailable_preflight_receipt(
                self.team_name,
                error.reason,
                error.status,
                observed_at,
            )

    def prepare(
        self,
        *,
        launch_raw,
        claim_raw,
        outbox_sha256,
        acceptance,
        preflight,
        reservation,
        provider_pass_claim_raw,
        create_authorization_raw,
        values,
    ):
        launch, claim = _validate_gateway(launch_raw, claim_raw)
        validate_provider_acceptance(
            acceptance,
            campaign_id=self.campaign_id,
            team_name=self.team_name,
            launch_raw=launch_raw,
            claim_raw=claim_raw,
            outbox_sha256=outbox_sha256,
            preflight=preflight,
            reservation=reservation,
            provider_pass_claim_raw=provider_pass_claim_raw,
            create_authorization_raw=create_authorization_raw,
        )
        expected_values = contract.settings(
            values.get("student_image"),
            values.get("student_job_id"),
            values.get("model_alias"),
            values.get("registry_secret"),
            values.get("config_secret"),
            values.get("control_store_access_key_secret"),
            values.get("control_store_secret_key_secret"),
            values.get("control_store_session_token_secret"),
        )
        if values != expected_values:
            raise ValueError("winner Baseten settings are invalid")
        release = load_published_private_image_release(
            self.store,
            acceptance["image_release_sha256"],
        )
        student = release["images"]["student"]
        if (
            student["admission_sha256"]
            != acceptance["image_admission_sha256"]
            or student["image_reference"] != values["student_image"]
            or values["student_image"] != claim["runtime_image_reference"]
            or values["student_job_id"] != claim["student_job_id"]
        ):
            raise ValueError("winner private student image admission differs")
        run_id = acceptance["run_id"]
        definition, unused_truss, unused_argv = _execution_definition(
            self.campaign_id,
            self.team_name,
            launch,
            claim,
            acceptance,
            values,
        )
        del unused_truss, unused_argv
        prefix = self._prefix(run_id)
        create_same(
            self.store,
            f"claims/v1/{launch['deployment_claim_sha256']}.json",
            provider_acceptance_bytes(acceptance),
            "application/json",
        )
        create_same(
            self.store,
            f"{prefix}/gateway-launch.json",
            bytes(launch_raw),
            "application/json",
        )
        create_same(
            self.store,
            f"{prefix}/gateway-claim.json",
            bytes(claim_raw),
            "application/json",
        )
        create_same(
            self.store,
            f"{prefix}/provider-acceptance.json",
            provider_acceptance_bytes(acceptance),
            "application/json",
        )
        self._json(prefix, "primary-preflight.json", preflight)
        self._json(prefix, "execution-definition.json", definition)
        return {
            "schema_version": "milk.baseten-winner-prepare-result.v1",
            "run_id": run_id,
            "evidence_prefix": prefix,
            "execution_definition_sha256": _digest(definition),
            "provider_acceptance_sha256": provider_acceptance_sha256(acceptance),
            "state": "prepared",
        }

    def _context(self, acceptance, values):
        acceptance_raw = provider_acceptance_bytes(acceptance)
        run_id = acceptance["run_id"]
        prefix = self._prefix(run_id)
        if self.store.get(f"{prefix}/provider-acceptance.json") != acceptance_raw:
            raise ValueError("stored winner provider acceptance differs")
        launch_raw = self.store.get(f"{prefix}/gateway-launch.json")
        claim_raw = self.store.get(f"{prefix}/gateway-claim.json")
        launch, claim = _validate_gateway(launch_raw, claim_raw)
        if (
            acceptance["campaign_id"] != self.campaign_id
            or acceptance["claim_sha256"] != launch["deployment_claim_sha256"]
            or acceptance["provider_binding_sha256"]
            != launch["provider_binding_sha256"]
            or acceptance["selection"]["provider_identity"]
            != {"provider": "baseten", "team_name": self.team_name}
        ):
            raise ValueError("stored winner authority differs from this Baseten lifecycle")
        expected_values = contract.settings(
            values.get("student_image"),
            values.get("student_job_id"),
            values.get("model_alias"),
            values.get("registry_secret"),
            values.get("config_secret"),
            values.get("control_store_access_key_secret"),
            values.get("control_store_secret_key_secret"),
            values.get("control_store_session_token_secret"),
        )
        if values != expected_values or values["student_job_id"] != launch["student_job_id"]:
            raise ValueError("winner Baseten settings differ from the prepared job")
        definition, truss, argv = _execution_definition(
            self.campaign_id,
            self.team_name,
            launch,
            claim,
            acceptance,
            values,
        )
        stored_definition = _stored(
            self.store.get(f"{prefix}/execution-definition.json"),
            "Baseten winner execution definition",
        )
        if stored_definition != definition:
            raise ValueError("stored Baseten winner execution definition differs")
        preflight = _stored(
            self.store.get(f"{prefix}/primary-preflight.json"),
            "Baseten winner primary preflight",
        )
        expected_preflight = baseten_preflight_receipt(
            self.team_name,
            preflight.get("team_id"),
            preflight.get("management_key_prefix_sha256"),
            preflight.get("teams_response_sha256"),
            preflight.get("api_keys_response_sha256"),
            _gateway_time(preflight.get("observed_at"), "preflight observed_at"),
        )
        if (
            preflight != expected_preflight
            or acceptance["selection"]["primary_preflight"]
            != {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": _digest(preflight),
                "observed_at": preflight["observed_at"],
            }
        ):
            raise ValueError("stored Baseten winner primary preflight differs")
        return {
            "acceptance": acceptance,
            "acceptance_sha256": provider_acceptance_sha256(acceptance),
            "definition": definition,
            "definition_sha256": _digest(definition),
            "launch": launch,
            "claim": claim,
            "preflight": preflight,
            "prefix": prefix,
            "truss": truss,
            "argv": argv,
            "values": values,
        }

    def _lookup_identity(self, context, lookup_exact):
        raw = self._provider(
            lookup_exact,
            self.team_name,
            context["definition"]["model_name"],
            context["definition"]["execution_name"],
            context["definition"]["labels"],
        )
        if raw is None:
            return None
        return _identity(
            contract.parse_push(
                raw,
                context["acceptance"]["run_id"],
                context["acceptance"]["claim_sha256"],
            ),
            context["definition"],
        )

    def _load_create_intent(self, context):
        key = f"{context['prefix']}/create-intent.json"
        try:
            value = _stored(self.store.get(key), "Baseten create intent")
        except FileNotFoundError:
            return None
        if (
            set(value)
            != {
                "schema_version",
                "provider_acceptance_sha256",
                "execution_definition_sha256",
                "team_name",
                "model_name",
                "execution_name",
                "labels",
                "requested_at",
            }
            or value.get("schema_version") != "milk.baseten-create-intent.v1"
            or value.get("provider_acceptance_sha256")
            != context["acceptance_sha256"]
            or value.get("execution_definition_sha256")
            != context["definition_sha256"]
            or value.get("team_name") != self.team_name
            or value.get("model_name") != context["definition"]["model_name"]
            or value.get("execution_name")
            != context["definition"]["execution_name"]
            or value.get("labels") != context["definition"]["labels"]
        ):
            raise ValueError("stored Baseten create intent is invalid")
        requested = _gateway_time(value.get("requested_at"), "create requested_at")
        if not (
            _gateway_time(context["acceptance"]["accepted_at"], "accepted_at")
            <= requested
            <= _gateway_time(
                context["acceptance"]["create_not_after"],
                "create_not_after",
            )
        ):
            raise ValueError("stored Baseten create intent is outside its authority")
        return value

    def _load_create_result(self, context):
        try:
            value = _stored(
                self.store.get(f"{context['prefix']}/create-result.json"),
                "Baseten create result",
            )
        except FileNotFoundError:
            return None
        if (
            set(value)
            != {
                "schema_version",
                "provider_acceptance_sha256",
                "execution_definition_sha256",
                "identity",
                "observed_at",
                "state",
            }
            or value.get("schema_version") != "milk.baseten-create-result.v1"
            or value.get("provider_acceptance_sha256")
            != context["acceptance_sha256"]
            or value.get("execution_definition_sha256")
            != context["definition_sha256"]
            or value.get("state") not in {"created", "recovered"}
        ):
            raise ValueError("stored Baseten create result is invalid")
        _gateway_time(value.get("observed_at"), "create observed_at")
        return _identity(value.get("identity"), context["definition"])

    def _created_identity(self, context, lookup_exact, push_truss=None):
        stored = self._load_create_result(context)
        if stored is not None:
            return stored
        intent = self._load_create_intent(context)
        observed = self._lookup_identity(context, lookup_exact)
        if intent is not None:
            if observed is None:
                self._event(
                    context["prefix"],
                    "create_recovery_ambiguous",
                    {
                        "provider_acceptance_sha256": context[
                            "acceptance_sha256"
                        ],
                        "execution_name": context["definition"]["execution_name"],
                        "create_replayed": False,
                    },
                )
                return None
            result = {
                "schema_version": "milk.baseten-create-result.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "execution_definition_sha256": context["definition_sha256"],
                "identity": observed,
                "observed_at": _utc_text(_utc(self.now(), "clock")),
                "state": "recovered",
            }
            self._json(context["prefix"], "create-result.json", result)
            return observed
        if observed is not None:
            raise RuntimeError(
                "deterministic Baseten model exists without an immutable create intent"
            )
        if push_truss is None:
            return None
        current = _utc(self.now(), "clock")
        if current > _gateway_time(
            context["acceptance"]["create_not_after"],
            "create_not_after",
        ):
            raise RuntimeError("Baseten create authority expired")
        intent = {
            "schema_version": "milk.baseten-create-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "execution_definition_sha256": context["definition_sha256"],
            "team_name": self.team_name,
            "model_name": context["definition"]["model_name"],
            "execution_name": context["definition"]["execution_name"],
            "labels": context["definition"]["labels"],
            "requested_at": _utc_text(current),
        }
        intent_key = f"{context['prefix']}/create-intent.json"
        if not self.store.create(
            intent_key,
            canonical_json(intent),
            "application/json",
        ):
            return self._created_identity(context, lookup_exact)
        try:
            raw = self._provider(
                push_truss,
                self.team_name,
                context["truss"],
                list(context["argv"]),
            )
            identity = _identity(
                contract.parse_push(
                    raw,
                    context["acceptance"]["run_id"],
                    context["acceptance"]["claim_sha256"],
                ),
                context["definition"],
            )
            verified = self._lookup_identity(context, lookup_exact)
            if verified != identity:
                raise RuntimeError(
                    "Truss result is not visible under the exact Baseten team identity"
                )
        except Exception as error:
            self._event(
                context["prefix"],
                "create_response_ambiguous",
                {
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "error_class_sha256": hashlib.sha256(
                        type(error).__name__.encode()
                    ).hexdigest(),
                    "create_replayed": False,
                },
            )
            raise
        result = {
            "schema_version": "milk.baseten-create-result.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "execution_definition_sha256": context["definition_sha256"],
            "identity": identity,
            "observed_at": _utc_text(_utc(self.now(), "clock")),
            "state": "created",
        }
        self._json(context["prefix"], "create-result.json", result)
        return identity

    def _inspect(self, context, identity, inspect_deployment, *, configured=False):
        raw = self._provider(
            inspect_deployment,
            self.team_name,
            identity["model_id"],
            identity["deployment_id"],
        )
        return contract.validate_deployment(
            raw,
            identity,
            configured=configured,
        )

    def _configure(
        self,
        context,
        identity,
        inspect_deployment,
        update_autoscaling,
        *,
        enforce_deadline=True,
        evidence_name="autoscaling",
    ):
        if evidence_name not in {"autoscaling", "scale-to-zero"}:
            raise ValueError("Baseten autoscaling evidence name is invalid")
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            deployment = self._inspect(context, identity, inspect_deployment)
            autoscaling = deployment.get("autoscaling_settings")
            if isinstance(autoscaling, dict) and all(
                autoscaling.get(key) == expected
                for key, expected in contract.SERVING_AUTOSCALING.items()
            ):
                return deployment
            intent_key = (
                f"{context['prefix']}/{evidence_name}/intents/{attempt:02d}.json"
            )
            result_key = (
                f"{context['prefix']}/{evidence_name}/results/{attempt:02d}.json"
            )
            try:
                result = _mutation_result(
                    _stored(
                        self.store.get(result_key),
                        "Baseten autoscaling result",
                    ),
                    schema="milk.baseten-autoscaling-result.v1",
                    acceptance_sha256=context["acceptance_sha256"],
                    deployment_id=identity["deployment_id"],
                    attempt=attempt,
                    with_status=True,
                )
            except FileNotFoundError:
                result = None
            try:
                intent = _stored(
                    self.store.get(intent_key),
                    "Baseten autoscaling intent",
                )
            except FileNotFoundError:
                intent = None
            if intent is not None:
                _mutation_intent(
                    intent,
                    schema="milk.baseten-autoscaling-intent.v1",
                    acceptance_sha256=context["acceptance_sha256"],
                    deployment_id=identity["deployment_id"],
                    attempt=attempt,
                    settings=contract.SERVING_AUTOSCALING,
                )
                continue
            if result is not None:
                raise ValueError("Baseten autoscaling result has no immutable intent")
            current = _utc(self.now(), "clock")
            if enforce_deadline and current > _gateway_time(
                context["acceptance"]["provider_not_after"],
                "provider_not_after",
            ):
                raise RuntimeError("Baseten provider authority expired")
            intent = {
                "schema_version": "milk.baseten-autoscaling-intent.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "deployment_id": identity["deployment_id"],
                "attempt": attempt,
                "settings": dict(contract.SERVING_AUTOSCALING),
                "requested_at": _utc_text(current),
            }
            if not self.store.create(
                intent_key,
                canonical_json(intent),
                "application/json",
            ):
                continue
            try:
                response = self._provider(
                    update_autoscaling,
                    self.team_name,
                    identity["model_id"],
                    identity["deployment_id"],
                    dict(contract.SERVING_AUTOSCALING),
                )
                validated = contract.validate_autoscaling_update(response)
                result = {
                    "schema_version": "milk.baseten-autoscaling-result.v1",
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "deployment_id": identity["deployment_id"],
                    "attempt": attempt,
                    "status": validated["status"],
                    **_response_fingerprint(response, "Baseten autoscaling"),
                    "observed_at": _utc_text(_utc(self.now(), "clock")),
                    "state": "accepted",
                }
            except Exception as error:
                result = {
                    "schema_version": "milk.baseten-autoscaling-result.v1",
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "deployment_id": identity["deployment_id"],
                    "attempt": attempt,
                    "error_class_sha256": hashlib.sha256(
                        type(error).__name__.encode()
                    ).hexdigest(),
                    "observed_at": _utc_text(_utc(self.now(), "clock")),
                    "state": "ambiguous",
                }
            create_same(
                self.store,
                result_key,
                canonical_json(result),
                "application/json",
            )
        deployment = self._inspect(context, identity, inspect_deployment)
        autoscaling = deployment.get("autoscaling_settings")
        if isinstance(autoscaling, dict) and all(
            autoscaling.get(key) == expected
            for key, expected in contract.SERVING_AUTOSCALING.items()
        ):
            return deployment
        raise RuntimeError("Baseten autoscaling attempts were exhausted")

    def _record_logs(self, context, logs):
        response = contract.deployment_logs(logs)
        materialization = contract.materialization_from_logs(
            response,
            context["launch"]["student_job_id"],
        )
        archive = LogArchive(
            _LogEvidence(
                self.store,
                context["prefix"],
                context["acceptance"]["run_id"],
            ),
            "baseten",
            stream_id=context["acceptance_sha256"],
            max_run_bytes=MAX_LOG_BYTES,
        )
        complete = True
        for entry in response["logs"]:
            cursor = _digest(
                {
                    "timestamp": entry["timestamp"],
                    "replica": entry["replica"],
                }
            )
            if not archive.write(
                at=entry["timestamp"],
                line=entry["message"],
                provider_cursor=cursor,
            ):
                complete = False
                break
        index = archive.close(
            source_complete=complete,
            missing_reason=None if complete else "bounded_log_limit",
        )
        summary = {
            "schema_version": "milk.baseten-winner-log-summary.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            **_response_fingerprint(logs, "Baseten logs"),
            "stored_records": index["stored_records"],
            "source_complete": index["source_complete"],
            "materialization_sha256": _digest(materialization),
        }
        self._json(context["prefix"], "logs-summary.json", summary)
        return materialization

    def _candidate_create_intent(self, context, identity, attempt):
        key = (
            f"{context['prefix']}/candidate-key/create-intents/"
            f"{attempt:02d}.json"
        )
        try:
            value = _stored(self.store.get(key), "Baseten candidate-key create intent")
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": "milk.baseten-candidate-key-create-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_id": context["preflight"]["team_id"],
            "team_name": self.team_name,
            "key_name": _candidate_key_name(context["acceptance"]["run_id"], attempt),
            "key_type": "WORKSPACE_INVOKE",
            "model_ids": [identity["model_id"]],
            "attempt": attempt,
        }
        if (
            not isinstance(value, dict)
            or set(value) != set(expected) | {"action_authority", "requested_at"}
            or any(value.get(field) != item for field, item in expected.items())
            or not isinstance(value.get("action_authority"), dict)
            or set(value["action_authority"])
            != {
                "provider_pass_claim_sha256",
                "create_authorization_sha256",
            }
            or any(
                not _matches(HEX64, value["action_authority"].get(field))
                for field in value["action_authority"]
            )
        ):
            raise ValueError("stored Baseten candidate-key create intent is invalid")
        requested_at = _gateway_time(
            value.get("requested_at"),
            "candidate-key create requested_at",
        )
        if requested_at > _gateway_time(
            context["acceptance"]["provider_not_after"],
            "provider_not_after",
        ):
            raise ValueError("stored Baseten candidate-key create intent is late")
        return value

    def _candidate_create_result(self, context, identity, attempt):
        key = (
            f"{context['prefix']}/candidate-key/create-results/"
            f"{attempt:02d}.json"
        )
        try:
            value = _stored(self.store.get(key), "Baseten candidate-key create result")
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": "milk.baseten-candidate-key-create-result.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_name": self.team_name,
            "key_name": _candidate_key_name(context["acceptance"]["run_id"], attempt),
            "attempt": attempt,
            "state": "created",
        }
        if (
            not isinstance(value, dict)
            or set(value)
            != set(expected)
            | {"key_prefix", "candidate_key_sha256", "observed_at"}
            or any(value.get(field) != item for field, item in expected.items())
            or not isinstance(value.get("key_prefix"), str)
            or not value["key_prefix"]
            or not _matches(HEX64, value.get("candidate_key_sha256"))
        ):
            raise ValueError("stored Baseten candidate-key create result is invalid")
        _gateway_time(value.get("observed_at"), "candidate-key create observed_at")
        return value

    def _candidate_revoke_intent(
        self,
        context,
        identity,
        attempt,
        key_name,
        key_prefix,
        candidate_key_sha256,
    ):
        key = (
            f"{context['prefix']}/candidate-key/revoke-intents/"
            f"{attempt:02d}.json"
        )
        try:
            value = _stored(self.store.get(key), "Baseten candidate-key revoke intent")
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": "milk.baseten-candidate-key-revoke-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_name": self.team_name,
            "key_name": key_name,
            "key_prefix": key_prefix,
            "candidate_key_sha256": candidate_key_sha256,
            "attempt": attempt,
        }
        if (
            not isinstance(value, dict)
            or set(value) != set(expected) | {"requested_at"}
            or any(value.get(field) != item for field, item in expected.items())
        ):
            raise ValueError("stored Baseten candidate-key revoke intent is invalid")
        _gateway_time(value.get("requested_at"), "candidate-key revoke requested_at")
        return value

    def _revoke_candidate_key(
        self,
        context,
        identity,
        attempt,
        metadata,
        created,
        read_key_metadata,
        delete_api_key,
    ):
        key_name = _candidate_key_name(context["acceptance"]["run_id"], attempt)
        key_prefix = (
            created["key_prefix"] if metadata is None else metadata["prefix"]
        )
        candidate_key_sha256 = (
            None if created is None else created["candidate_key_sha256"]
        )
        if created is not None and (
            created["key_name"] != key_name
            or created["key_prefix"] != key_prefix
        ):
            raise ValueError("stored Baseten candidate key differs from provider metadata")
        intent = self._candidate_revoke_intent(
            context,
            identity,
            attempt,
            key_name,
            key_prefix,
            candidate_key_sha256,
        )
        result_key = (
            f"{context['prefix']}/candidate-key/revoke-results/"
            f"{attempt:02d}.json"
        )
        try:
            stored_result = _stored(
                self.store.get(result_key),
                "Baseten candidate-key revoke result",
            )
        except FileNotFoundError:
            stored_result = None
        if stored_result is not None:
            common = {
                "schema_version": "milk.baseten-candidate-key-revoke-result.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "model_id": identity["model_id"],
                "team_name": self.team_name,
                "key_name": key_name,
                "key_prefix": key_prefix,
                "candidate_key_sha256": candidate_key_sha256,
                "attempt": attempt,
            }
            expected_fields = set(common) | {"observed_at", "state"}
            if stored_result.get("state") == "revoked":
                expected_fields |= {"response_bytes", "response_sha256"}
            if (
                set(stored_result) != expected_fields
                or any(
                    stored_result.get(field) != item
                    for field, item in common.items()
                )
                or stored_result.get("state")
                not in {"revoked", "verified_absent"}
                or stored_result.get("state") == "revoked"
                and (
                    type(stored_result.get("response_bytes")) is not int
                    or not 1
                    <= stored_result["response_bytes"]
                    <= contract.MAX_RESPONSE_BYTES
                    or not _matches(
                        HEX64,
                        stored_result.get("response_sha256"),
                    )
                )
            ):
                raise ValueError(
                    "stored Baseten candidate-key revoke result is invalid"
                )
            _gateway_time(
                stored_result.get("observed_at"),
                "candidate-key revoke observed_at",
            )
            return metadata is None
        if metadata is None:
            result = {
                "schema_version": "milk.baseten-candidate-key-revoke-result.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "model_id": identity["model_id"],
                "team_name": self.team_name,
                "key_name": key_name,
                "key_prefix": key_prefix,
                "candidate_key_sha256": candidate_key_sha256,
                "attempt": attempt,
                "observed_at": _utc_text(_utc(self.now(), "clock")),
                "state": "verified_absent",
            }
            create_same(
                self.store,
                result_key,
                canonical_json(result),
                "application/json",
            )
            return True
        if intent is not None:
            return False
        intent = {
            "schema_version": "milk.baseten-candidate-key-revoke-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_name": self.team_name,
            "key_name": key_name,
            "key_prefix": key_prefix,
            "candidate_key_sha256": candidate_key_sha256,
            "attempt": attempt,
            "requested_at": _utc_text(_utc(self.now(), "clock")),
        }
        intent_key = (
            f"{context['prefix']}/candidate-key/revoke-intents/"
            f"{attempt:02d}.json"
        )
        if not self.store.create(
            intent_key,
            canonical_json(intent),
            "application/json",
        ):
            return False
        try:
            response = self._provider(
                delete_api_key,
                self.team_name,
                key_prefix,
            )
            after = self._provider(
                read_key_metadata,
                self.team_name,
                identity["model_id"],
            )
            if _candidate_key_prefix_metadata(
                after,
                key_prefix,
                identity["model_id"],
                self.team_name,
            ) is not None:
                raise RuntimeError("Baseten candidate key remains after revocation")
        except Exception as error:
            self._event(
                context["prefix"],
                "candidate_key_revoke_ambiguous",
                {
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "model_id": identity["model_id"],
                    "key_prefix_sha256": _digest(key_prefix.encode()),
                    "error_class_sha256": _digest(type(error).__name__.encode()),
                    "replayed": False,
                },
            )
            return False
        result = {
            "schema_version": "milk.baseten-candidate-key-revoke-result.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_name": self.team_name,
            "key_name": key_name,
            "key_prefix": key_prefix,
            "candidate_key_sha256": candidate_key_sha256,
            "attempt": attempt,
            **_response_fingerprint(response, "Baseten candidate-key revocation"),
            "observed_at": _utc_text(_utc(self.now(), "clock")),
            "state": "revoked",
        }
        create_same(
            self.store,
            result_key,
            canonical_json(result),
            "application/json",
        )
        return True

    def _provision_candidate_key(
        self,
        context,
        identity,
        read_key_metadata,
        create_model_scoped_key,
        delete_api_key,
    ):
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            key_name = _candidate_key_name(
                context["acceptance"]["run_id"],
                attempt,
            )
            metadata_raw = self._provider(
                read_key_metadata,
                self.team_name,
                identity["model_id"],
            )
            metadata = _candidate_key_metadata(
                metadata_raw,
                key_name,
                identity["model_id"],
                self.team_name,
            )
            intent = self._candidate_create_intent(context, identity, attempt)
            created = self._candidate_create_result(context, identity, attempt)
            if created is not None and intent is None:
                raise ValueError("Baseten candidate-key result has no immutable intent")
            if intent is not None:
                if metadata is None:
                    continue
                if not self._revoke_candidate_key(
                    context,
                    identity,
                    attempt,
                    metadata,
                    created,
                    read_key_metadata,
                    delete_api_key,
                ):
                    return {
                        "state": "candidate_key_revoke_ambiguous_no_replay",
                    }
                continue
            if metadata is not None:
                raise RuntimeError(
                    "deterministic Baseten candidate key exists without an immutable intent"
                )
            current = _utc(self.now(), "clock")
            if current > _gateway_time(
                context["acceptance"]["provider_not_after"],
                "provider_not_after",
            ):
                raise RuntimeError("Baseten provider authority expired")
            action_authority = self.request_guard()
            if (
                not isinstance(action_authority, dict)
                or set(action_authority)
                != {
                    "provider_pass_claim_sha256",
                    "create_authorization_sha256",
                }
                or any(
                    not _matches(HEX64, action_authority.get(field))
                    for field in action_authority
                )
            ):
                raise RuntimeError("live provider create authority is required")
            intent = {
                "schema_version": "milk.baseten-candidate-key-create-intent.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "model_id": identity["model_id"],
                "team_id": context["preflight"]["team_id"],
                "team_name": self.team_name,
                "key_name": key_name,
                "key_type": "WORKSPACE_INVOKE",
                "model_ids": [identity["model_id"]],
                "attempt": attempt,
                "action_authority": dict(action_authority),
                "requested_at": _utc_text(current),
            }
            intent_key = (
                f"{context['prefix']}/candidate-key/create-intents/"
                f"{attempt:02d}.json"
            )
            if not self.store.create(
                intent_key,
                canonical_json(intent),
                "application/json",
            ):
                return {"state": "candidate_key_intent_raced"}
            try:
                candidate_key = create_model_scoped_key(
                    self.team_name,
                    identity["model_id"],
                    key_name,
                )
                metadata_raw = self._provider(
                    read_key_metadata,
                    self.team_name,
                    identity["model_id"],
                )
                metadata = _candidate_key_metadata(
                    metadata_raw,
                    key_name,
                    identity["model_id"],
                    self.team_name,
                )
                verified = contract.validate_invocation_key(
                    metadata_raw,
                    candidate_key,
                    identity["model_id"],
                    self.team_name,
                )
                if metadata is None or verified != metadata:
                    raise RuntimeError("created Baseten candidate key is not exact")
            except Exception as error:
                self._event(
                    context["prefix"],
                    "candidate_key_create_ambiguous",
                    {
                        "provider_acceptance_sha256": context[
                            "acceptance_sha256"
                        ],
                        "model_id": identity["model_id"],
                        "attempt": attempt,
                        "error_class_sha256": _digest(type(error).__name__.encode()),
                        "replayed": False,
                    },
                )
                return {"state": "candidate_key_create_ambiguous_no_replay"}
            created = {
                "schema_version": "milk.baseten-candidate-key-create-result.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "model_id": identity["model_id"],
                "team_name": self.team_name,
                "key_name": key_name,
                "key_prefix": metadata["prefix"],
                "candidate_key_sha256": _digest(candidate_key.encode()),
                "attempt": attempt,
                "observed_at": _utc_text(_utc(self.now(), "clock")),
                "state": "created",
            }
            self._json(
                context["prefix"],
                f"candidate-key/create-results/{attempt:02d}.json",
                created,
            )
            return {"state": "ready", "candidate_key": candidate_key, "created": created}
        return {"state": "candidate_key_attempts_exhausted"}

    def _candidate_delivery_intent(self, context, identity, created):
        key = (
            f"{context['prefix']}/candidate-key/delivery-intents/"
            f"{created['attempt']:02d}.json"
        )
        try:
            value = _stored(
                self.store.get(key),
                "Baseten candidate-key delivery intent",
            )
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": "milk.baseten-candidate-key-delivery-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": context["acceptance"]["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "attempt": created["attempt"],
            "state": "pending",
        }
        if (
            not isinstance(value, dict)
            or set(value)
            != set(expected)
            | {
                "payload_sha256",
                "payload_bytes",
                "winner_admission_sha256",
                "gateway_result_sha256",
                "requested_at",
            }
            or any(value.get(field) != item for field, item in expected.items())
            or not _matches(HEX64, value.get("payload_sha256"))
            or type(value.get("payload_bytes")) is not int
            or not 1 <= value["payload_bytes"] <= 4096
            or not _matches(HEX64, value.get("winner_admission_sha256"))
            or not _matches(HEX64, value.get("gateway_result_sha256"))
        ):
            raise ValueError("stored Baseten candidate-key delivery intent is invalid")
        _gateway_time(value.get("requested_at"), "candidate-key delivery requested_at")
        return value

    def _candidate_delivery_ack(self, raw, intent):
        value = _strict(raw, 4096, "candidate-key delivery acknowledgement", line=True)
        common = {
            "schema_version": "milk.baseten-candidate-key-delivery-ack.v1",
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": intent["team_name"],
            "model_id": intent["model_id"],
            "key_name": intent["key_name"],
            "key_prefix": intent["key_prefix"],
            "candidate_key_sha256": intent["candidate_key_sha256"],
            "payload_sha256": intent["payload_sha256"],
            "payload_bytes": intent["payload_bytes"],
        }
        if (
            canonical_json(value) != raw
            or set(value)
            != set(common)
            | {
                "gateway_release_id",
                "gateway_release_sha256",
                "verified_at",
                "state",
            }
            or any(value.get(field) != item for field, item in common.items())
            or value.get("state") not in {"installed", "absent"}
        ):
            raise ValueError("candidate-key delivery acknowledgement differs")
        _gateway_time(value.get("verified_at"), "candidate-key delivery verified_at")
        if value["state"] == "installed":
            if not _canonical_uuid(value.get("gateway_release_id")) or not _matches(
                HEX64,
                value.get("gateway_release_sha256"),
            ):
                raise ValueError("candidate-key installed acknowledgement is invalid")
        elif (
            value.get("gateway_release_id") is not None
            or value.get("gateway_release_sha256") is not None
        ):
            raise ValueError("candidate-key absent acknowledgement is invalid")
        return value

    def _candidate_delivery_receipt(self, context, identity, created, intent):
        key = (
            f"{context['prefix']}/candidate-key/deliveries/"
            f"{created['attempt']:02d}.json"
        )
        try:
            value = _stored(
                self.store.get(key),
                "Baseten candidate-key delivery receipt",
            )
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": "milk.baseten-candidate-key-delivery-receipt.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "payload_sha256": intent["payload_sha256"],
            "payload_bytes": intent["payload_bytes"],
            "state": "delivered",
        }
        if (
            not isinstance(value, dict)
            or set(value)
            != set(expected)
            | {
                "gateway_release_id",
                "gateway_release_sha256",
                "acknowledgement_sha256",
                "delivered_at",
            }
            or any(value.get(field) != item for field, item in expected.items())
            or not _canonical_uuid(value.get("gateway_release_id"))
            or not _matches(HEX64, value.get("gateway_release_sha256"))
            or not _matches(HEX64, value.get("acknowledgement_sha256"))
        ):
            raise ValueError("stored Baseten candidate-key delivery receipt is invalid")
        _gateway_time(value.get("delivered_at"), "candidate-key delivered_at")
        return value

    def _candidate_recovery_release_receipt(
        self,
        context,
        identity,
        created,
        intent,
        delivery_receipt,
    ):
        key = (
            f"{context['prefix']}/candidate-key/recovery-releases/"
            f"{created['attempt']:02d}.json"
        )
        try:
            value = _stored(
                self.store.get(key),
                "Baseten candidate-key recovery release receipt",
            )
        except FileNotFoundError:
            return None
        expected = {
            "schema_version": (
                "milk.baseten-candidate-key-recovery-release-receipt.v1"
            ),
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "delivery_receipt_sha256": _digest(delivery_receipt),
            "state": "installed",
        }
        if (
            not isinstance(value, dict)
            or set(value)
            != set(expected)
            | {
                "gateway_release_id",
                "gateway_release_sha256",
                "acknowledgement_sha256",
                "verified_at",
            }
            or any(value.get(field) != item for field, item in expected.items())
            or not _canonical_uuid(value.get("gateway_release_id"))
            or not _matches(HEX64, value.get("gateway_release_sha256"))
            or not _matches(HEX64, value.get("acknowledgement_sha256"))
        ):
            raise ValueError(
                "stored Baseten candidate-key recovery release receipt is invalid"
            )
        _gateway_time(
            value.get("verified_at"),
            "candidate-key recovery release verified_at",
        )
        return value

    def _store_candidate_recovery_release_receipt(
        self,
        context,
        identity,
        created,
        intent,
        delivery_receipt,
        ack,
        ack_raw,
    ):
        if ack["state"] != "installed":
            raise ValueError("candidate-key recovery release is not installed")
        receipt = {
            "schema_version": (
                "milk.baseten-candidate-key-recovery-release-receipt.v1"
            ),
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "delivery_receipt_sha256": _digest(delivery_receipt),
            "gateway_release_id": ack["gateway_release_id"],
            "gateway_release_sha256": ack["gateway_release_sha256"],
            "acknowledgement_sha256": hashlib.sha256(ack_raw).hexdigest(),
            "verified_at": ack["verified_at"],
            "state": "installed",
        }
        self._json(
            context["prefix"],
            f"candidate-key/recovery-releases/{created['attempt']:02d}.json",
            receipt,
        )
        return receipt

    def _pending_candidate_admission(self, context, identity, created, intent):
        admission_raw = self.store.get(
            f"{context['prefix']}/candidate-key/pending-admissions/"
            f"{created['attempt']:02d}.json"
        )
        result_raw = self.store.get(
            f"{context['prefix']}/candidate-key/pending-results/"
            f"{created['attempt']:02d}.json"
        )
        admission = _strict(
            admission_raw,
            16 * 1024,
            "pending winner admission",
            line=True,
        )
        result = _strict(
            result_raw,
            64 * 1024,
            "pending winner result",
            line=True,
        )
        if (
            contract.receipt_bytes(admission) != admission_raw
            or contract.result_bytes(result) != result_raw
            or hashlib.sha256(admission_raw).hexdigest()
            != intent["winner_admission_sha256"]
            or hashlib.sha256(result_raw).hexdigest()
            != intent["gateway_result_sha256"]
            or result.get("provider_acceptance") != context["acceptance"]
            or result.get("admission") != admission
            or admission.get("execution_id") != identity["deployment_id"]
            or admission.get("execution_name") != identity["execution_name"]
            or admission.get("chat_completions_url") != identity["endpoint"]
            or admission.get("candidate_api_key_sha256")
            != created["candidate_key_sha256"]
        ):
            raise ValueError("pending winner admission differs from delivery intent")
        return admission_raw, result_raw

    def _store_candidate_delivery_receipt(self, context, created, intent, ack, ack_raw):
        if ack["state"] != "installed":
            raise ValueError("candidate-key delivery was not installed")
        receipt = {
            "schema_version": "milk.baseten-candidate-key-delivery-receipt.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": intent["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "payload_sha256": intent["payload_sha256"],
            "payload_bytes": intent["payload_bytes"],
            "gateway_release_id": ack["gateway_release_id"],
            "gateway_release_sha256": ack["gateway_release_sha256"],
            "acknowledgement_sha256": hashlib.sha256(ack_raw).hexdigest(),
            "delivered_at": ack["verified_at"],
            "state": "delivered",
        }
        self._json(
            context["prefix"],
            f"candidate-key/deliveries/{created['attempt']:02d}.json",
            receipt,
        )
        return receipt

    def _verify_candidate_delivery_runway(self, ack, admission_raw):
        admission = _strict(
            admission_raw,
            16 * 1024,
            "pending winner admission",
            line=True,
        )
        verified_at = _gateway_time(
            ack["verified_at"],
            "candidate-key delivery verified_at",
        )
        admitted_at = _gateway_time(admission["admitted_at"], "admitted_at")
        service_not_after = _gateway_time(
            admission["service_not_after"],
            "service_not_after",
        )
        if not (
            admitted_at <= verified_at
            and verified_at + dt.timedelta(seconds=ROUTE_AUTHORITY_SECONDS)
            <= service_not_after
        ):
            raise RuntimeError("candidate-key delivery left insufficient route authority")

    def _commit_candidate_admission(self, context, intent, receipt, admission_raw, result_raw):
        admission = _strict(admission_raw, 16 * 1024, "winner admission", line=True)
        delivered_at = _gateway_time(
            receipt["delivered_at"],
            "candidate-key delivered_at",
        )
        admitted_at = _gateway_time(admission["admitted_at"], "admitted_at")
        service_not_after = _gateway_time(
            admission["service_not_after"],
            "service_not_after",
        )
        if not (
            admitted_at <= delivered_at
            and delivered_at + dt.timedelta(seconds=ROUTE_AUTHORITY_SECONDS)
            <= service_not_after
        ):
            raise RuntimeError("candidate-key delivery left insufficient route authority")
        create_same(
            self.store,
            f"{context['prefix']}/winner-admission.json",
            admission_raw,
            "application/json",
        )
        create_same(
            self.store,
            f"{context['prefix']}/gateway-result.json",
            result_raw,
            "application/json",
        )
        return {
            "schema_version": "milk.baseten-winner-admit-result.v1",
            "run_id": context["acceptance"]["run_id"],
            "candidate_key_delivery_sha256": _digest(receipt),
            "gateway_result_sha256": intent["gateway_result_sha256"],
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "service_not_after": admission["service_not_after"],
            "state": "admitted",
        }

    def _deliver_candidate_key(
        self,
        context,
        identity,
        candidate_key,
        created,
        sink,
        read_key_metadata,
        delete_api_key,
        admission_raw,
        result_raw,
    ):
        if not callable(sink):
            raise RuntimeError("winner candidate-key delivery sink is required")
        payload = {
            "schema_version": "milk.baseten-candidate-key-delivery.v1",
            "run_id": context["acceptance"]["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "candidate_api_key": candidate_key,
        }
        payload_raw = canonical_json(payload)
        create_same(
            self.store,
            f"{context['prefix']}/candidate-key/pending-admissions/"
            f"{created['attempt']:02d}.json",
            admission_raw,
            "application/json",
        )
        create_same(
            self.store,
            f"{context['prefix']}/candidate-key/pending-results/"
            f"{created['attempt']:02d}.json",
            result_raw,
            "application/json",
        )
        intent = {
            "schema_version": "milk.baseten-candidate-key-delivery-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "run_id": context["acceptance"]["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "payload_sha256": _digest(payload_raw),
            "payload_bytes": len(payload_raw),
            "winner_admission_sha256": hashlib.sha256(admission_raw).hexdigest(),
            "gateway_result_sha256": hashlib.sha256(result_raw).hexdigest(),
            "attempt": created["attempt"],
            "requested_at": _utc_text(_utc(self.now(), "clock")),
            "state": "pending",
        }
        intent_key = (
            f"{context['prefix']}/candidate-key/delivery-intents/"
            f"{created['attempt']:02d}.json"
        )
        if not self.store.create(
            intent_key,
            canonical_json(intent),
            "application/json",
        ):
            return {"state": "candidate_key_delivery_intent_raced"}
        try:
            ack_raw = sink(payload_raw)
            ack = self._candidate_delivery_ack(ack_raw, intent)
            if ack["state"] != "installed":
                raise RuntimeError("gateway did not install the candidate key")
            self._verify_candidate_delivery_runway(ack, admission_raw)
        except Exception as delivery_error:
            self._event(
                context["prefix"],
                "candidate_key_delivery_failed",
                {
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "model_id": identity["model_id"],
                    "candidate_key_sha256": created["candidate_key_sha256"],
                    "error_class_sha256": _digest(
                        type(delivery_error).__name__.encode()
                    ),
                },
            )
            try:
                metadata_raw = self._provider(
                    read_key_metadata,
                    self.team_name,
                    identity["model_id"],
                )
                metadata = _candidate_key_prefix_metadata(
                    metadata_raw,
                    created["key_prefix"],
                    identity["model_id"],
                    self.team_name,
                )
                revoked = self._revoke_candidate_key(
                    context,
                    identity,
                    created["attempt"],
                    metadata,
                    created,
                    read_key_metadata,
                    delete_api_key,
                )
            except Exception as cleanup_error:
                self._event(
                    context["prefix"],
                    "candidate_key_delivery_cleanup_ambiguous",
                    {
                        "provider_acceptance_sha256": context[
                            "acceptance_sha256"
                        ],
                        "model_id": identity["model_id"],
                        "candidate_key_sha256": created[
                            "candidate_key_sha256"
                        ],
                        "error_class_sha256": _digest(
                            type(cleanup_error).__name__.encode()
                        ),
                        "replayed": False,
                    },
                )
                revoked = False
            return {
                "state": (
                    "candidate_key_delivery_failed_revoked"
                    if revoked
                    else "candidate_key_delivery_failed_revoke_ambiguous"
                )
            }
        receipt = self._store_candidate_delivery_receipt(
            context,
            created,
            intent,
            ack,
            ack_raw,
        )
        return {"state": "delivered", "receipt": receipt}

    def _recover_candidate_delivery(
        self,
        context,
        identity,
        sink,
        read_key_metadata,
        delete_api_key,
    ):
        pending = []
        delivered = []
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            created = self._candidate_create_result(context, identity, attempt)
            if created is None:
                continue
            intent = self._candidate_delivery_intent(context, identity, created)
            if intent is None:
                continue
            admission_raw, result_raw = self._pending_candidate_admission(
                context,
                identity,
                created,
                intent,
            )
            receipt = self._candidate_delivery_receipt(
                context,
                identity,
                created,
                intent,
            )
            if receipt is not None:
                delivered.append(
                    (created, intent, receipt, admission_raw, result_raw)
                )
                continue
            revoke_result_key = (
                f"{context['prefix']}/candidate-key/revoke-results/"
                f"{attempt:02d}.json"
            )
            try:
                self.store.get(revoke_result_key)
            except FileNotFoundError:
                pending.append(
                    (created, intent, admission_raw, result_raw)
                )
            else:
                if not self._revoke_candidate_key(
                    context,
                    identity,
                    attempt,
                    None,
                    created,
                    read_key_metadata,
                    delete_api_key,
                ):
                    raise ValueError("stored candidate-key revocation is invalid")
        if len(delivered) > 1 or delivered and pending:
            raise ValueError("candidate-key delivery evidence is not unique")
        if delivered:
            created, intent, receipt, admission_raw, result_raw = delivered[0]
            recovery_release = self._candidate_recovery_release_receipt(
                context,
                identity,
                created,
                intent,
                receipt,
            )
            if recovery_release is None:
                if not callable(sink):
                    raise RuntimeError("candidate-key delivery recovery is unavailable")
                verify = {
                    "schema_version": "milk.baseten-candidate-key-delivery-verify.v1",
                    "run_id": intent["run_id"],
                    "provider": "baseten",
                    "team_name": self.team_name,
                    "model_id": identity["model_id"],
                    "key_name": created["key_name"],
                    "key_prefix": created["key_prefix"],
                    "candidate_key_sha256": created["candidate_key_sha256"],
                    "payload_sha256": intent["payload_sha256"],
                    "payload_bytes": intent["payload_bytes"],
                }
                try:
                    ack_raw = sink(canonical_json(verify))
                    ack = self._candidate_delivery_ack(ack_raw, intent)
                    self._verify_candidate_delivery_runway(ack, admission_raw)
                    if ack["state"] != "installed":
                        raise RuntimeError("gateway candidate key is absent")
                    recovery_release = self._store_candidate_recovery_release_receipt(
                        context,
                        identity,
                        created,
                        intent,
                        receipt,
                        ack,
                        ack_raw,
                    )
                except Exception:
                    metadata_raw = self._provider(
                        read_key_metadata,
                        self.team_name,
                        identity["model_id"],
                    )
                    metadata = _candidate_key_prefix_metadata(
                        metadata_raw,
                        created["key_prefix"],
                        identity["model_id"],
                        self.team_name,
                    )
                    revoked = self._revoke_candidate_key(
                        context,
                        identity,
                        created["attempt"],
                        metadata,
                        created,
                        read_key_metadata,
                        delete_api_key,
                    )
                    return {
                        "schema_version": "milk.baseten-winner-admit-result.v1",
                        "run_id": context["acceptance"]["run_id"],
                        "model_id": identity["model_id"],
                        "state": (
                            "candidate_key_delivery_recovery_failed_revoked"
                            if revoked
                            else "candidate_key_delivery_recovery_failed_revoke_ambiguous"
                        ),
                    }
            self._verify_candidate_delivery_runway(recovery_release, admission_raw)
            return self._commit_candidate_admission(
                context,
                intent,
                receipt,
                admission_raw,
                result_raw,
            )
        if not pending:
            return None
        if len(pending) != 1 or not callable(sink):
            raise RuntimeError("candidate-key delivery recovery is unavailable")
        created, intent, admission_raw, result_raw = pending[0]
        verify = {
            "schema_version": "milk.baseten-candidate-key-delivery-verify.v1",
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": identity["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "payload_sha256": intent["payload_sha256"],
            "payload_bytes": intent["payload_bytes"],
        }
        try:
            ack_raw = sink(canonical_json(verify))
            ack = self._candidate_delivery_ack(ack_raw, intent)
        except Exception as error:
            ack = None
            self._event(
                context["prefix"],
                "candidate_key_delivery_recovery_ambiguous",
                {
                    "provider_acceptance_sha256": context[
                        "acceptance_sha256"
                    ],
                    "model_id": identity["model_id"],
                    "candidate_key_sha256": created[
                        "candidate_key_sha256"
                    ],
                    "error_class_sha256": _digest(type(error).__name__.encode()),
                },
            )
        if ack is not None and ack["state"] == "installed":
            try:
                self._verify_candidate_delivery_runway(ack, admission_raw)
            except RuntimeError:
                ack = None
            else:
                receipt = self._store_candidate_delivery_receipt(
                    context,
                    created,
                    intent,
                    ack,
                    ack_raw,
                )
                return self._commit_candidate_admission(
                    context,
                    intent,
                    receipt,
                    admission_raw,
                    result_raw,
                )
        metadata_raw = self._provider(
            read_key_metadata,
            self.team_name,
            identity["model_id"],
        )
        metadata = _candidate_key_prefix_metadata(
            metadata_raw,
            created["key_prefix"],
            identity["model_id"],
            self.team_name,
        )
        revoked = self._revoke_candidate_key(
            context,
            identity,
            created["attempt"],
            metadata,
            created,
            read_key_metadata,
            delete_api_key,
        )
        return {
            "schema_version": "milk.baseten-winner-admit-result.v1",
            "run_id": context["acceptance"]["run_id"],
            "model_id": identity["model_id"],
            "state": (
                "candidate_key_delivery_recovery_failed_revoked"
                if revoked
                else "candidate_key_delivery_recovery_failed_revoke_ambiguous"
            ),
        }

    def remove_candidate_key_from_gateway(
        self,
        *,
        acceptance,
        values,
        gateway_cleanup_authorization_raw,
        gateway_cleanup_authorization_sha256,
        canary_route_raw=None,
        zero_route_raw=None,
        candidate_key_sink=None,
    ):
        context = self._context(acceptance, values)
        if (
            not isinstance(gateway_cleanup_authorization_raw, (bytes, bytearray))
            or not _matches(HEX64, gateway_cleanup_authorization_sha256)
            or hashlib.sha256(gateway_cleanup_authorization_raw).hexdigest()
            != gateway_cleanup_authorization_sha256
        ):
            raise ValueError("gateway candidate-key cleanup authority is invalid")
        authorization = _strict(
            gateway_cleanup_authorization_raw,
            32 * 1024,
            "gateway candidate-key cleanup authority",
        )
        admission_raw = self.store.get(f"{context['prefix']}/winner-admission.json")
        admission = _strict(
            admission_raw,
            16 * 1024,
            "winner admission receipt",
            line=True,
        )
        trigger = authorization.get("trigger")
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
            or authorization.get("scope") != context["claim"]["scope"]
            or authorization.get("student_job_id")
            != context["launch"]["student_job_id"]
            or authorization.get("claim_sha256")
            != context["acceptance"]["claim_sha256"]
            or authorization.get("provider_acceptance_sha256")
            != context["acceptance_sha256"]
            or authorization.get("run_id") != context["acceptance"]["run_id"]
            or authorization.get("selected_provider") != "baseten"
            or authorization.get("execution_id") != admission.get("execution_id")
        ):
            raise ValueError("gateway candidate-key cleanup authority differs")
        if isinstance(trigger, dict) and trigger.get("kind") == "route_zero":
            if canary_route_raw is None or zero_route_raw is None:
                raise ValueError("gateway candidate-key cleanup needs route receipts")
            canary, zero = validate_route_retirement(
                canary_route_raw,
                zero_route_raw,
                context["claim"],
            )
            if (
                trigger.get("canary_route_receipt") != canary
                or trigger.get("zero_route_receipt") != zero
            ):
                raise ValueError("gateway cleanup route receipts differ")
        elif isinstance(trigger, dict) and trigger.get("kind") == "service_expired":
            if (
                canary_route_raw is not None
                or zero_route_raw is not None
                or trigger.get("service_not_after")
                != admission.get("service_not_after")
                or _utc(self.now(), "clock")
                < _gateway_time(trigger["service_not_after"], "service_not_after")
            ):
                raise ValueError("gateway cleanup expiry authority differs")
        else:
            raise ValueError("gateway candidate-key cleanup trigger is invalid")
        matches = []
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            create_result_key = (
                f"{context['prefix']}/candidate-key/create-results/"
                f"{attempt:02d}.json"
            )
            try:
                candidate_identity = _stored(
                    self.store.get(create_result_key),
                    "Baseten candidate-key create result identity",
                )
            except FileNotFoundError:
                continue
            model_id = candidate_identity.get("model_id")
            if not _matches(contract.MODEL_ID, model_id):
                raise ValueError("stored Baseten candidate-key model ID is invalid")
            created = self._candidate_create_result(
                context,
                {"model_id": model_id},
                attempt,
            )
            identity = {
                "model_id": created["model_id"],
            }
            intent = self._candidate_delivery_intent(context, identity, created)
            if intent is None:
                continue
            receipt = self._candidate_delivery_receipt(
                context,
                identity,
                created,
                intent,
            )
            if (
                receipt is not None
                and receipt["candidate_key_sha256"]
                == admission["candidate_api_key_sha256"]
            ):
                release = self._candidate_recovery_release_receipt(
                    context,
                    identity,
                    created,
                    intent,
                    receipt,
                ) or receipt
                matches.append((created, intent, receipt, release))
        if len(matches) != 1:
            raise ValueError("admitted gateway candidate key is not uniquely evidenced")
        created, intent, receipt, release = matches[0]
        request = {
            "schema_version": "milk.baseten-candidate-key-remove.v1",
            "run_id": intent["run_id"],
            "provider": "baseten",
            "team_name": self.team_name,
            "model_id": intent["model_id"],
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "payload_sha256": intent["payload_sha256"],
            "payload_bytes": intent["payload_bytes"],
            "gateway_release_id": release["gateway_release_id"],
            "gateway_release_sha256": release["gateway_release_sha256"],
            "gateway_cleanup_authorization": authorization,
            "gateway_cleanup_authorization_sha256": (
                gateway_cleanup_authorization_sha256
            ),
            "trigger": trigger,
        }
        request_raw = canonical_json(request)
        cleanup_common = {
            "schema_version": "milk.baseten-candidate-key-gateway-remove-intent.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "gateway_cleanup_authorization_sha256": (
                gateway_cleanup_authorization_sha256
            ),
            "request_sha256": hashlib.sha256(request_raw).hexdigest(),
            "request_bytes": len(request_raw),
            "state": "pending",
        }
        intent_key = f"{context['prefix']}/candidate-key/gateway-remove-intent.json"
        try:
            cleanup_intent = _stored(
                self.store.get(intent_key),
                "gateway candidate-key remove intent",
            )
        except FileNotFoundError:
            cleanup_intent = {
                **cleanup_common,
                "requested_at": _utc_text(_utc(self.now(), "clock")),
            }
            create_same(
                self.store,
                intent_key,
                canonical_json(cleanup_intent),
                "application/json",
            )
        if (
            set(cleanup_intent) != set(cleanup_common) | {"requested_at"}
            or any(
                cleanup_intent.get(field) != item
                for field, item in cleanup_common.items()
            )
        ):
            raise ValueError("stored gateway candidate-key removal intent differs")
        _gateway_time(
            cleanup_intent.get("requested_at"),
            "gateway candidate-key removal requested_at",
        )
        result_key = f"{context['prefix']}/candidate-key/gateway-remove-result.json"
        try:
            result = _stored(
                self.store.get(result_key),
                "gateway candidate-key remove result",
            )
        except FileNotFoundError:
            result = None
        if result is not None:
            if (
                set(result)
                != {
                    "schema_version",
                    "provider_acceptance_sha256",
                    "candidate_key_sha256",
                    "gateway_cleanup_authorization_sha256",
                    "request_sha256",
                    "acknowledgement_sha256",
                    "verified_at",
                    "state",
                }
                or result.get("schema_version")
                != "milk.baseten-candidate-key-gateway-remove-result.v1"
                or result.get("provider_acceptance_sha256")
                != context["acceptance_sha256"]
                or result.get("candidate_key_sha256")
                != created["candidate_key_sha256"]
                or result.get("gateway_cleanup_authorization_sha256")
                != gateway_cleanup_authorization_sha256
                or result.get("request_sha256") != cleanup_intent["request_sha256"]
                or not _matches(HEX64, result.get("acknowledgement_sha256"))
                or result.get("state") != "absent"
            ):
                raise ValueError("stored gateway candidate-key removal differs")
            _gateway_time(
                result.get("verified_at"),
                "gateway candidate-key removal verified_at",
            )
            return result
        sink = candidate_key_sink or self.candidate_key_sink
        if not callable(sink):
            raise RuntimeError("gateway candidate-key cleanup socket is required")
        ack_raw = sink(request_raw)
        ack = self._candidate_delivery_ack(ack_raw, intent)
        if ack["state"] != "absent":
            raise RuntimeError("gateway candidate key remains installed")
        result = {
            "schema_version": "milk.baseten-candidate-key-gateway-remove-result.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "gateway_cleanup_authorization_sha256": (
                gateway_cleanup_authorization_sha256
            ),
            "request_sha256": cleanup_intent["request_sha256"],
            "acknowledgement_sha256": hashlib.sha256(ack_raw).hexdigest(),
            "verified_at": ack["verified_at"],
            "state": "absent",
        }
        self._json(
            context["prefix"],
            "candidate-key/gateway-remove-result.json",
            result,
        )
        return result

    def _revoke_admitted_candidate_key(
        self,
        context,
        identity,
        admission,
        read_key_metadata,
        delete_api_key,
    ):
        matches = []
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            created = self._candidate_create_result(context, identity, attempt)
            if (
                created is not None
                and created["candidate_key_sha256"]
                == admission["candidate_api_key_sha256"]
            ):
                matches.append(created)
        if len(matches) != 1:
            raise ValueError("admitted Baseten candidate key is not uniquely evidenced")
        created = matches[0]
        metadata_raw = self._provider(
            read_key_metadata,
            self.team_name,
            identity["model_id"],
        )
        metadata = _candidate_key_prefix_metadata(
            metadata_raw,
            created["key_prefix"],
            identity["model_id"],
            self.team_name,
        )
        if not self._revoke_candidate_key(
            context,
            identity,
            created["attempt"],
            metadata,
            created,
            read_key_metadata,
            delete_api_key,
        ):
            raise RuntimeError("Baseten candidate-key revocation is ambiguous")
        zero = {
            "schema_version": "milk.baseten-candidate-key-zero.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "team_name": self.team_name,
            "key_name": created["key_name"],
            "key_prefix": created["key_prefix"],
            "candidate_key_sha256": created["candidate_key_sha256"],
            "observed_at": _utc_text(_utc(self.now(), "clock")),
            "state": "zero",
        }
        self._json(context["prefix"], "candidate-key/zero.json", zero)
        return zero

    def billing_cross_check(self, acceptance, values, source, read_billing=None):
        if source not in {"model", "deployment", "workspace"}:
            raise ValueError("Baseten billing cross-check source is invalid")
        context = self._context(acceptance, values)
        read_billing = self._io(read_billing, "_read_billing")
        raw = self._provider(
            read_billing,
            self.team_name,
            source,
            context["acceptance"]["accepted_at"],
            context["acceptance"]["provider_not_after"],
        )
        result = {
            "schema_version": "milk.baseten-billing-cross-check.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "source": source,
            "observed_at": _utc_text(_utc(self.now(), "clock")),
            **_response_fingerprint(raw, "Baseten billing"),
            "authoritative_for_budget": False,
        }
        self._json(
            context["prefix"],
            f"billing-cross-checks/{_digest(result)}.json",
            result,
        )
        return result

    def admit(
        self,
        *,
        acceptance,
        values,
        lookup_exact=None,
        push_truss=None,
        inspect_deployment=None,
        update_autoscaling=None,
        read_key_metadata=None,
        create_model_scoped_key=None,
        delete_api_key=None,
        deliver_candidate_key=None,
        read_logs=None,
        run_probe=None,
        observed_cost_microusd=0,
    ):
        lookup_exact = self._io(lookup_exact, "_lookup_exact")
        push_truss = self._io(push_truss, "_push_truss")
        inspect_deployment = self._io(inspect_deployment, "_inspect_deployment")
        update_autoscaling = self._io(update_autoscaling, "_update_autoscaling")
        context = self._context(acceptance, values)
        identity = self._created_identity(context, lookup_exact, push_truss)
        if identity is None:
            return {
                "schema_version": "milk.baseten-winner-admit-result.v1",
                "run_id": acceptance["run_id"],
                "state": "create_ambiguous_no_replay",
            }
        self._configure(
            context,
            identity,
            inspect_deployment,
            update_autoscaling,
        )
        read_key_metadata = self._io(read_key_metadata, "_read_key_metadata")
        create_model_scoped_key = self._io(
            create_model_scoped_key,
            "_create_model_scoped_key",
        )
        delete_api_key = self._io(delete_api_key, "_delete_api_key")
        sink = deliver_candidate_key or self.candidate_key_sink
        recovered = self._recover_candidate_delivery(
            context,
            identity,
            sink,
            read_key_metadata,
            delete_api_key,
        )
        if recovered is not None:
            return recovered
        provisioned = self._provision_candidate_key(
            context,
            identity,
            read_key_metadata,
            create_model_scoped_key,
            delete_api_key,
        )
        if provisioned["state"] != "ready":
            return {
                "schema_version": "milk.baseten-winner-admit-result.v1",
                "run_id": acceptance["run_id"],
                "model_id": identity["model_id"],
                "state": provisioned["state"],
            }
        candidate_key = provisioned["candidate_key"]
        read_logs = self._io(read_logs, "_read_logs")
        run_probe = self._io(run_probe, "_run_probe")
        metadata = self._provider(
            read_key_metadata,
            self.team_name,
            identity["model_id"],
        )
        contract.validate_invocation_key(
            metadata,
            candidate_key,
            identity["model_id"],
            self.team_name,
        )
        deployment = self._inspect(
            context,
            identity,
            inspect_deployment,
            configured=True,
        )
        contract.validate_deployment(
            deployment,
            identity,
            configured=True,
            ready=True,
        )
        ready_at = _utc(self.now(), "clock")
        logs = self._provider(
            read_logs,
            self.team_name,
            identity["model_id"],
            identity["deployment_id"],
            MAX_LOG_RECORDS,
        )
        materialization = self._record_logs(context, logs)
        if (
            materialization["variant"] != context["launch"]["winner"]
            or materialization["model_manifest_sha256"]
            != context["definition"]["model_manifest_sha256"]
        ):
            raise ValueError("Baseten materialization differs from the winner claim")
        probe = self._provider(
            run_probe,
            identity["endpoint"],
            candidate_key,
            context["launch"]["student_job_id"],
            values["model_alias"],
            materialization,
        )
        probe = contract.validate_probe(
            probe,
            context["launch"]["student_job_id"],
            values["model_alias"],
            materialization,
            candidate_key,
        )
        admitted_at = _utc(self.now(), "clock")
        claim_expires = _gateway_time(
            context["claim"]["expires_at"],
            "winner expires_at",
        )
        provider_expires = _gateway_time(
            acceptance["provider_not_after"],
            "provider_not_after",
        )
        service_not_after = provider_expires
        intent = self._load_create_intent(context)
        launch_started = _gateway_time(
            intent["requested_at"],
            "launch started_at",
        )
        if not (
            launch_started <= ready_at <= admitted_at
            and admitted_at + dt.timedelta(seconds=ROUTE_AUTHORITY_SECONDS)
            <= service_not_after
            <= claim_expires
        ):
            raise RuntimeError("winner cannot receive its fixed 15-minute authority")
        if (
            type(observed_cost_microusd) is not int
            or not 0
            <= observed_cost_microusd
            <= acceptance["max_cost_microusd"]
        ):
            raise ValueError("winner observed cost is outside its shared budget authority")
        admission = {
            "schema_version": contract.RECEIPT_SCHEMA,
            "provider": "baseten",
            "student_job_id": context["launch"]["student_job_id"],
            "student_variant": context["launch"]["winner"],
            "model_manifest_sha256": context["definition"][
                "model_manifest_sha256"
            ],
            "model_alias": values["model_alias"],
            "model_alias_sha256": probe["model_alias_sha256"],
            "candidate_api_key_sha256": probe["candidate_api_key_sha256"],
            "runtime_image_reference": values["student_image"],
            "admission_program_sha256": context["claim"]["authority"][
                "admission_program_sha256"
            ],
            "execution_id": identity["deployment_id"],
            "execution_name": identity["execution_name"],
            "chat_completions_url": identity["endpoint"],
            "models_response_sha256": probe["models_response_sha256"],
            "chat_request_sha256": probe["chat_request_sha256"],
            "chat_response_sha256": probe["chat_response_sha256"],
            "launch_started_at": _utc_text(launch_started),
            "ready_at": _utc_text(ready_at),
            "admitted_at": _utc_text(admitted_at),
            "service_not_after": _utc_text(service_not_after),
        }
        admission_raw = contract.receipt_bytes(admission)
        result = {
            "schema_version": contract.RESULT_SCHEMA,
            "scope": context["claim"]["scope"],
            "student_job_id": context["launch"]["student_job_id"],
            "claim_sha256": context["acceptance"]["claim_sha256"],
            "provider_binding_sha256": context["acceptance"][
                "provider_binding_sha256"
            ],
            "provider_acceptance": acceptance,
            "observed_cost_microusd": observed_cost_microusd,
            "admission": admission,
        }
        result_raw = contract.result_bytes(result)
        delivered = self._deliver_candidate_key(
            context,
            identity,
            candidate_key,
            provisioned["created"],
            sink,
            read_key_metadata,
            delete_api_key,
            admission_raw,
            result_raw,
        )
        if delivered["state"] != "delivered":
            return {
                "schema_version": "milk.baseten-winner-admit-result.v1",
                "run_id": acceptance["run_id"],
                "model_id": identity["model_id"],
                "state": delivered["state"],
            }
        intent = self._candidate_delivery_intent(
            context,
            identity,
            provisioned["created"],
        )
        return self._commit_candidate_admission(
            context,
            intent,
            delivered["receipt"],
            admission_raw,
            result_raw,
        )

    def teardown(
        self,
        *,
        acceptance,
        values,
        lookup_exact=None,
        inspect_deployment=None,
        update_autoscaling=None,
        deactivate_exact=None,
        read_key_metadata=None,
        delete_api_key=None,
        canary_route_raw=None,
        zero_route_raw=None,
    ):
        lookup_exact = self._io(lookup_exact, "_lookup_exact")
        inspect_deployment = self._io(inspect_deployment, "_inspect_deployment")
        update_autoscaling = self._io(update_autoscaling, "_update_autoscaling")
        deactivate_exact = self._io(deactivate_exact, "_deactivate_exact")
        read_key_metadata = self._io(read_key_metadata, "_read_key_metadata")
        delete_api_key = self._io(delete_api_key, "_delete_api_key")
        context = self._context(acceptance, values)
        admission_raw = self.store.get(f"{context['prefix']}/winner-admission.json")
        admission = _strict(
            admission_raw,
            16 * 1024,
            "winner admission receipt",
            line=True,
        )
        if contract.receipt_bytes(admission) != admission_raw:
            raise ValueError("stored winner admission receipt differs")
        admitted_at = _gateway_time(admission.get("admitted_at"), "admitted_at")
        current = _utc(self.now(), "clock")
        service_not_after = _gateway_time(
            admission["service_not_after"],
            "service_not_after",
        )
        if (
            admission.get("provider") != "baseten"
            or admission.get("student_job_id") != context["launch"]["student_job_id"]
            or admission.get("student_variant") != context["launch"]["winner"]
            or admission.get("model_manifest_sha256")
            != context["definition"]["model_manifest_sha256"]
            or admission.get("model_alias") != values["model_alias"]
            or admission.get("runtime_image_reference") != values["student_image"]
            or admission.get("admission_program_sha256")
            != context["claim"]["authority"]["admission_program_sha256"]
            or admission.get("service_not_after")
            != acceptance["provider_not_after"]
            or service_not_after - admitted_at
            < dt.timedelta(seconds=ROUTE_AUTHORITY_SECONDS)
        ):
            raise ValueError("stored winner admission differs from its immutable job")
        if zero_route_raw is not None:
            if canary_route_raw is None:
                raise ValueError("zero route requires its signed canary receipt")
            canary, zero = validate_route_retirement(
                canary_route_raw,
                zero_route_raw,
                context["claim"],
            )
            create_same(
                self.store,
                f"{context['prefix']}/routes/canary-receipt.json",
                bytes(canary_route_raw),
                "application/json",
            )
            create_same(
                self.store,
                f"{context['prefix']}/routes/zero-receipt.json",
                bytes(zero_route_raw),
                "application/json",
            )
            del canary, zero
        elif current < service_not_after:
            raise RuntimeError("signed zero-basis-point route is required before expiry")
        else:
            self._event(
                context["prefix"],
                "service_authority_expired",
                {
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "service_not_after": admission["service_not_after"],
                },
            )
        identity = self._created_identity(context, lookup_exact)
        if identity is None:
            return {
                "schema_version": "milk.baseten-winner-teardown-result.v1",
                "run_id": acceptance["run_id"],
                "state": "create_ambiguous_no_replay",
            }
        if (
            admission["execution_id"] != identity["deployment_id"]
            or admission["execution_name"] != identity["execution_name"]
            or admission["chat_completions_url"] != identity["endpoint"]
        ):
            raise ValueError("stored winner admission differs from its Baseten identity")
        deployment = self._inspect(context, identity, inspect_deployment)
        if not (
            deployment["status"] == "INACTIVE"
            and deployment["active_replica_count"] == 0
        ):
            self._configure(
                context,
                identity,
                inspect_deployment,
                update_autoscaling,
                enforce_deadline=False,
                evidence_name="scale-to-zero",
            )
        for attempt in range(MAX_MUTATION_ATTEMPTS):
            deployment = self._inspect(context, identity, inspect_deployment)
            if (
                deployment["status"] == "INACTIVE"
                and deployment["active_replica_count"] == 0
            ):
                break
            intent_key = (
                f"{context['prefix']}/deactivation/intents/{attempt:02d}.json"
            )
            result_key = (
                f"{context['prefix']}/deactivation/results/{attempt:02d}.json"
            )
            try:
                result = _mutation_result(
                    _stored(
                        self.store.get(result_key),
                        "Baseten deactivation result",
                    ),
                    schema="milk.baseten-deactivation-result.v1",
                    acceptance_sha256=context["acceptance_sha256"],
                    deployment_id=identity["deployment_id"],
                    attempt=attempt,
                )
            except FileNotFoundError:
                result = None
            try:
                intent = _stored(
                    self.store.get(intent_key),
                    "Baseten deactivation intent",
                )
            except FileNotFoundError:
                intent = None
            if intent is not None:
                _mutation_intent(
                    intent,
                    schema="milk.baseten-deactivation-intent.v1",
                    acceptance_sha256=context["acceptance_sha256"],
                    deployment_id=identity["deployment_id"],
                    attempt=attempt,
                )
                continue
            if result is not None:
                raise ValueError("Baseten deactivation result has no immutable intent")
            intent = {
                "schema_version": "milk.baseten-deactivation-intent.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "deployment_id": identity["deployment_id"],
                "attempt": attempt,
                "requested_at": _utc_text(_utc(self.now(), "clock")),
            }
            if not self.store.create(
                intent_key,
                canonical_json(intent),
                "application/json",
            ):
                continue
            try:
                response = self._provider(
                    deactivate_exact,
                    self.team_name,
                    identity["model_id"],
                    identity["deployment_id"],
                )
                result = {
                    "schema_version": "milk.baseten-deactivation-result.v1",
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "deployment_id": identity["deployment_id"],
                    "attempt": attempt,
                    **_response_fingerprint(response, "Baseten deactivation"),
                    "observed_at": _utc_text(_utc(self.now(), "clock")),
                    "state": "accepted",
                }
            except Exception as error:
                result = {
                    "schema_version": "milk.baseten-deactivation-result.v1",
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "deployment_id": identity["deployment_id"],
                    "attempt": attempt,
                    "error_class_sha256": hashlib.sha256(
                        type(error).__name__.encode()
                    ).hexdigest(),
                    "observed_at": _utc_text(_utc(self.now(), "clock")),
                    "state": "ambiguous",
                }
            create_same(
                self.store,
                result_key,
                canonical_json(result),
                "application/json",
            )
        final = self._inspect(context, identity, inspect_deployment)
        if not (
            final["status"] == "INACTIVE"
            and final["active_replica_count"] == 0
        ):
            self._event(
                context["prefix"],
                "deactivation_attempts_exhausted",
                {
                    "provider_acceptance_sha256": context["acceptance_sha256"],
                    "deployment_id": identity["deployment_id"],
                    "attempts": MAX_MUTATION_ATTEMPTS,
                    "status": final["status"],
                    "active_replica_count": final["active_replica_count"],
                },
            )
        contract.validate_deployment(final, identity, stopped=True)
        candidate_key_zero = self._revoke_admitted_candidate_key(
            context,
            identity,
            admission,
            read_key_metadata,
            delete_api_key,
        )
        zero = {
            "schema_version": "milk.baseten-zero-gpu-verification.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "model_id": identity["model_id"],
            "deployment_id": identity["deployment_id"],
            "observed_at": _utc_text(_utc(self.now(), "clock")),
            "deployment": _deployment_summary(final),
            "candidate_key_zero_sha256": _digest(candidate_key_zero),
            "state": "zero",
        }
        self._json(context["prefix"], "zero-gpu.json", zero)
        return {
            "schema_version": "milk.baseten-winner-teardown-result.v1",
            "run_id": acceptance["run_id"],
            "candidate_key_zero_sha256": _digest(candidate_key_zero),
            "zero_gpu_sha256": _digest(zero),
            "state": "zero",
        }
