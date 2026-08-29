from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import urllib.error
import uuid

from milk_harness.budget import (
    ABSOLUTE_CEILING_MICROUSD,
    H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
    LAUNCH_CUTOFF_MICROUSD,
    TEARDOWN_RESERVE_MICROUSD,
)
from milk_harness.evidence import (
    HEX64,
    R2EvidenceStore,
    canonical_json,
    create_same,
)
from milk_harness.provider_acceptance import OPAQUE, TEAM_NAME


MAX_STREAM_BYTES = 1024 * 1024
IMAGE_REPOSITORIES = {
    "gateway": "ghcr.io/milkinfrastructure/milk-gateway",
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
    "student_train": "ghcr.io/milkinfrastructure/milk-student-train",
    "student_branch": "ghcr.io/milkinfrastructure/milk-student-branch",
    "teacher": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
}
SAFE_RESULT = re.compile(r"[a-z][a-z0-9_.-]{0,127}\Z")
GATEWAY_RESULT_SCHEMAS = frozenset(
    {
        "dragontales.expiry-receipt.v1",
        "dragontales.snapshot-analysis-local-rejection-receipt.v1",
        "dragontales.student-fanout-launch.v3",
        "dragontales.student-job-claim-receipt.v3",
        "dragontales.student-winner-deployment-launch.v2",
        "dragontales.teacher-gpu-run-launch.v1",
    }
)
POSITIVE_INTEGER = re.compile(r"[1-9][0-9]{0,31}\Z")
SOURCE_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
PROVIDER_PROJECT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
MODEL_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
SECRET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
PROVIDER_SECRET_FIELDS = (
    "registry",
    "config",
    "capture_access",
    "capture_secret",
    "capture_session",
    "control_access",
    "control_secret",
    "control_session",
)
STORE_IDENTITIES = (
    "gateway_capture",
    "gateway_control",
    "provider_control",
    "evidence",
    "ops_log",
    "create_authority_write",
    "create_authority_read",
    "gateway_ingest_control",
    "gateway_ingest_route",
    "route_control",
    "route_routes",
    "route_evidence",
)
PROVIDER_RUNTIME_FIELDS = (
    "gpu_provider",
    "baseten_team_name",
    "winner_model_alias",
    "modal_workspace_id",
    "modal_workspace_name",
    "modal_environment_id",
    "modal_environment_name",
    "modal_app_id",
    "modal_app_name",
    "modal_registry_secret_name",
    "modal_registry_secret_id",
    "modal_config_secret_name",
    "modal_config_secret_id",
    "modal_control_secret_name",
    "modal_control_secret_id",
    "modal_capture_secret_name",
    "modal_capture_secret_id",
    "modal_candidate_secret_name",
    "modal_candidate_secret_id",
    "modal_teacher_volume_name",
    "modal_teacher_volume_id",
    "modal_student_train_volume_name",
    "modal_student_train_volume_id",
)
GATEWAY_DEPLOYMENT_FIELDS = (
    "source_commit",
    "image_admission_sha256",
    "release_sha256",
    "application_id",
    "application_version",
    "container_image",
    "worker_version_id",
)
RUN_ARTIFACTS = (
    "contracts/snapshot-analyzer.json",
    "deploy/teacher/gpt-oss-120b/profile.json",
    "deploy/teacher/gpt-oss-120b/model.manifest.json",
    "deploy/student-train/provenance.json",
    "deploy/student-branch/provenance.json",
    "deploy/student/qwen3-chat-template.jinja",
    "deploy/models/qwen3-4b-instruct-2507/profile.json",
    "deploy/models/qwen3-4b-instruct-2507/model.manifest.sha256",
)
RUN_LIMITS = {
    "absolute_ceiling_microusd": ABSOLUTE_CEILING_MICROUSD,
    "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
    "teardown_reserve_microusd": TEARDOWN_RESERVE_MICROUSD,
    "h100_reservation_rate_microusd_per_minute": H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
    "provider_request_timeout_seconds": 30,
    "provider_reconcile_timeout_seconds": 300,
}
PROVIDER_LEASE_TTL_SECONDS = 15 * 60
PROVIDER_LEASE_MAX_CLOCK_SKEW_SECONDS = 30
PROVIDER_LEASE_ATTEMPTS = 8
MAX_GATEWAY_INGEST_REFERENCES = 18
MAX_TEACHER_DECISIONS = 4_096
SUMMARY_KEYS = {
    "schema_version",
    "source",
    "state",
    "started_at",
    "completed_at",
    "exit_code",
    "stdout_bytes",
    "stderr_bytes",
    "accepted_fields",
    "gaps",
}
SANITIZATION_PROFILE = {
    "schema_version": "milk.scheduler-sanitization-profile.v1",
    "accepted": "fixed workflow metadata and whitelisted typed result fields",
    "raw_streams": "discarded after bounded validation",
    "forbidden": [
        "prompts",
        "model_outputs",
        "datasets",
        "secrets",
        "headers",
        "signed_urls",
        "environment_dumps",
    ],
    "max_stream_bytes": MAX_STREAM_BYTES,
}
SANITIZATION_PROFILE_SHA256 = hashlib.sha256(
    canonical_json(SANITIZATION_PROFILE)
).hexdigest()
RETENTION_CONTRACT = {
    "ops_prefix": "operational/v1/scheduler-passes/",
    "ops_bucket_lock_seconds": 90 * 24 * 60 * 60,
    "ops_lifecycle_expiration_days": 90,
    "authority_reference_prefix": "operational-log-references/v1/scheduler-passes/",
    "authority_reference_lock": "indefinite",
    "writer_policy_verification": "not_performed",
}
COVERAGE = [
    {
        "source": "scheduler_setup",
        "state": "gap",
        "reason": "pre_archive_runner_or_required_store_failure_has_no_ops_artifact",
    },
    {
        "source": "local",
        "state": "gap",
        "reason": "scheduled_pass_has_no_local_operator_stream",
    },
    {
        "source": "ci",
        "state": "partial",
        "reason": "current_github_run_log_unavailable_before_workflow_completion",
    },
    {
        "source": "ghcr",
        "state": "partial",
        "reason": "exact_image_references_retained_registry_client_stream_discarded",
    },
    {
        "source": "cloudflare",
        "state": "gap",
        "reason": "runtime_stream_not_observed_by_scheduler_pass",
    },
    {
        "source": "baseten",
        "state": "partial",
        "reason": "sanitized_jobs_pass_retained_terminal_stream_not_in_this_ops_artifact",
    },
    {
        "source": "modal",
        "state": "gap",
        "reason": "modal_not_selected_by_this_scheduler_pass",
    },
    {
        "source": "gateway",
        "state": "partial",
        "reason": "sanitized_tick_result_retained_runtime_stream_not_observed",
    },
    {
        "source": "retention_policy",
        "state": "gap",
        "reason": "bucket_lock_and_lifecycle_not_observed_by_writer",
    },
]


def _strict_object(raw, *, limit=MAX_STREAM_BYTES):
    if not isinstance(raw, bytes) or len(raw) > limit:
        raise ValueError("bounded JSON bytes are required")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("JSON contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict):
        raise ValueError("JSON must be an object")
    return value


def _utc(value, label):
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 64:
        raise ValueError(f"{label} must be canonical UTC text")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be canonical UTC text") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be canonical UTC text")
    return parsed


def scheduler_pass_id(repository_id, run_id, run_attempt):
    values = (repository_id, run_id, run_attempt)
    if any(not isinstance(value, str) or POSITIVE_INTEGER.fullmatch(value) is None for value in values):
        raise ValueError("GitHub run identity is invalid")
    return hashlib.sha256(
        b"milk.scheduler-pass.v1\0"
        + b"\0".join(value.encode("ascii") for value in values)
    ).hexdigest()


def _utc_text(value):
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("scheduler clock must return UTC")
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _lease_record(value, campaign_id):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "campaign_id",
            "owner_id",
            "holder_id",
            "state",
            "revision",
            "acquired_at",
            "expires_at",
            "released_at",
        }
        or value.get("schema_version") != "milk.provider-campaign-lease.v1"
        or value.get("campaign_id") != campaign_id
        or HEX64.fullmatch(value.get("owner_id") or "") is None
        or HEX64.fullmatch(value.get("holder_id") or "") is None
        or value.get("state") not in {"active", "released"}
        or type(value.get("revision")) is not int
        or value["revision"] < 1
    ):
        raise ValueError("provider campaign lease is invalid")
    acquired = _utc(value.get("acquired_at"), "provider lease acquisition")
    expires = _utc(value.get("expires_at"), "provider lease expiry")
    if expires <= acquired:
        raise ValueError("provider campaign lease interval is invalid")
    if value["state"] == "active":
        if value.get("released_at") is not None:
            raise ValueError("active provider campaign lease is invalid")
    else:
        released = _utc(value.get("released_at"), "provider lease release")
        if released < acquired:
            raise ValueError("released provider campaign lease is invalid")
    return value


def _stored_lease_record(raw, campaign_id):
    value = _lease_record(_strict_object(raw), campaign_id)
    if canonical_json(value) != raw:
        raise ValueError("provider campaign lease is not canonical")
    return value


def _lease_key(campaign_id):
    if not isinstance(campaign_id, str) or HEX64.fullmatch(campaign_id) is None:
        raise ValueError("campaign ID must be lowercase hex64")
    return f"state/v1/campaigns/{campaign_id}/provider-pass-lease.json"


def acquire_provider_lease(
    store,
    campaign_id,
    owner_id,
    holder_id,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    key = _lease_key(campaign_id)
    if HEX64.fullmatch(owner_id or "") is None or HEX64.fullmatch(holder_id or "") is None:
        raise ValueError("provider campaign lease holder is invalid")
    current_time = now()
    acquired_at = _utc_text(current_time)
    expires_at = _utc_text(current_time + dt.timedelta(seconds=PROVIDER_LEASE_TTL_SECONDS))
    for _ in range(PROVIDER_LEASE_ATTEMPTS):
        try:
            raw, etag = store.get_versioned(key)
        except (FileNotFoundError, urllib.error.HTTPError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code != 404:
                raise
            proposal = {
                "schema_version": "milk.provider-campaign-lease.v1",
                "campaign_id": campaign_id,
                "owner_id": owner_id,
                "holder_id": holder_id,
                "state": "active",
                "revision": 1,
                "acquired_at": acquired_at,
                "expires_at": expires_at,
                "released_at": None,
            }
            if store.create(key, canonical_json(proposal), "application/json"):
                return proposal
            continue
        current = _stored_lease_record(raw, campaign_id)
        current_expires = _utc(current["expires_at"], "provider lease expiry")
        if (
            current["state"] == "active"
            and current["owner_id"] == owner_id
            and current["holder_id"] == holder_id
            and current_expires > current_time
        ):
            return current
        if current["state"] == "active" and current_expires > current_time:
            raise RuntimeError("provider campaign lease is already held")
        proposal = {
            "schema_version": "milk.provider-campaign-lease.v1",
            "campaign_id": campaign_id,
            "owner_id": owner_id,
            "holder_id": holder_id,
            "state": "active",
            "revision": current["revision"] + 1,
            "acquired_at": acquired_at,
            "expires_at": expires_at,
            "released_at": None,
        }
        if store.replace(key, canonical_json(proposal), etag, "application/json"):
            return proposal
    raise RuntimeError("provider campaign lease changed too many times")


def release_provider_lease(
    store,
    campaign_id,
    owner_id,
    holder_id,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    key = _lease_key(campaign_id)
    if HEX64.fullmatch(owner_id or "") is None or HEX64.fullmatch(holder_id or "") is None:
        raise ValueError("provider campaign lease holder is invalid")
    released_at = _utc_text(now())
    for _ in range(PROVIDER_LEASE_ATTEMPTS):
        raw, etag = store.get_versioned(key)
        current = _stored_lease_record(raw, campaign_id)
        if current["owner_id"] != owner_id or current["holder_id"] != holder_id:
            raise RuntimeError("provider campaign lease belongs to another holder")
        if current["state"] == "released":
            return current
        released = {
            **current,
            "state": "released",
            "revision": current["revision"] + 1,
            "released_at": released_at,
        }
        if store.replace(key, canonical_json(released), etag, "application/json"):
            return released
    raise RuntimeError("provider campaign lease changed too many times")


def active_provider_lease_reference(
    store,
    campaign_id,
    owner_id,
    holder_id,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    current_time = now()
    _utc_text(current_time)
    current = _stored_lease_record(
        store.get(_lease_key(campaign_id)),
        campaign_id,
    )
    if (
        current["state"] != "active"
        or current["owner_id"] != owner_id
        or current["holder_id"] != holder_id
        or _utc(current["expires_at"], "provider lease expiry") <= current_time
    ):
        raise RuntimeError("provider campaign lease is not active for this pass")
    return {
        "object_key": _lease_key(campaign_id),
        "owner_id": owner_id,
        "revision": current["revision"],
        "acquired_at": current["acquired_at"],
        "expires_at": current["expires_at"],
    }


def _provider_lease_reference(value, campaign_id, owner_id):
    if (
        not isinstance(value, dict)
        or set(value)
        != {"object_key", "owner_id", "revision", "acquired_at", "expires_at"}
        or value.get("object_key") != _lease_key(campaign_id)
        or value.get("owner_id") != owner_id
        or type(value.get("revision")) is not int
        or value["revision"] < 1
    ):
        raise ValueError("provider campaign lease reference is invalid")
    acquired = _utc(value.get("acquired_at"), "provider lease acquisition")
    expires = _utc(value.get("expires_at"), "provider lease expiry")
    if expires <= acquired:
        raise ValueError("provider campaign lease reference is invalid")
    return value


def immutable_image(name, value):
    repository = IMAGE_REPOSITORIES.get(name)
    if repository is None or not isinstance(value, str):
        raise ValueError("image identity is invalid")
    prefix = repository + "@sha256:"
    digest = value.removeprefix(prefix)
    if not value.startswith(prefix) or HEX64.fullmatch(digest) is None:
        raise ValueError(f"{name} image must be an immutable private Milk reference")
    return value


def _require_distinct_student_image_digests(images):
    if (
        images["student_train"].rpartition("@sha256:")[2]
        == images["student_branch"].rpartition("@sha256:")[2]
    ):
        raise ValueError("student train and branch images must have distinct digests")


def _scope(prefix):
    parts = prefix.split("/") if isinstance(prefix, str) else []
    if len(parts) != 6 or parts[:2] != ["dt", "v2"]:
        raise ValueError("scope prefix is invalid")
    values = []
    for item in parts[2:]:
        try:
            parsed = uuid.UUID(item)
        except (AttributeError, ValueError) as error:
            raise ValueError("scope prefix is invalid") from error
        if parsed.int == 0 or str(parsed) != item:
            raise ValueError("scope prefix is invalid")
        values.append(item)
    return dict(zip(("tenant_id", "project_id", "environment_id", "workload_id"), values))


def _artifact_hashes(root):
    root = Path(root).resolve()
    values = {}
    for relative in RUN_ARTIFACTS:
        path = root.joinpath(*relative.split("/"))
        if root not in path.resolve().parents or path.is_symlink() or not path.is_file():
            raise ValueError("confirmed run artifact is absent or unsafe")
        raw = path.read_bytes()
        if len(raw) > MAX_STREAM_BYTES:
            raise ValueError("confirmed run artifact is oversized")
        values[relative] = hashlib.sha256(raw).hexdigest()
    return values


def store_identity_sha256(account_id, bucket, access_key_id):
    if any(
        not isinstance(value, str) or not value or any(character.isspace() for character in value)
        for value in (account_id, bucket, access_key_id)
    ):
        raise ValueError("object-store identity is invalid")
    return hashlib.sha256(
        canonical_json(
            {
                "account_id": account_id,
                "bucket": bucket,
                "access_key_id": access_key_id,
            }
        )
    ).hexdigest()


def store_location_sha256(account_id, bucket):
    if any(
        not isinstance(value, str) or not value or any(character.isspace() for character in value)
        for value in (account_id, bucket)
    ):
        raise ValueError("object-store location is invalid")
    return hashlib.sha256(
        canonical_json({"account_id": account_id, "bucket": bucket})
    ).hexdigest()


def _provider_create_gate(value):
    fields = {
        "schema_version",
        "requested",
        "confirmation_valid",
        "granted",
        "confirmation_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema_version") != "milk.provider-create-gate.v1"
        or type(value.get("requested")) is not bool
        or type(value.get("confirmation_valid")) is not bool
        or type(value.get("granted")) is not bool
        or (
            value["confirmation_sha256"] is not None
            and HEX64.fullmatch(value["confirmation_sha256"] or "") is None
        )
        or value["confirmation_valid"] != (value["confirmation_sha256"] is not None)
        or (value["confirmation_valid"] and not value["requested"])
        or (value["granted"] and not value["confirmation_valid"])
    ):
        raise ValueError("provider create gate is invalid")
    return value


def _provider_create_authorization_key(campaign_id, holder_id):
    _lease_key(campaign_id)
    if HEX64.fullmatch(holder_id or "") is None:
        raise ValueError("provider create authorization holder is invalid")
    return (
        f"campaigns/v1/{campaign_id}/provider-create-authorizations/"
        f"{holder_id}.json"
    )


def _provider_create_authorization(
    value,
    record,
    scope_prefix,
    evidence_store_identity_sha256,
    create_authority_store_location_sha256,
):
    fields = {
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
    record = _lease_record(record, record.get("campaign_id") if isinstance(record, dict) else None)
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema_version") != "milk.provider-create-authorization.v1"
        or value.get("campaign_id") != record["campaign_id"]
        or value.get("scope_prefix") != scope_prefix
        or value.get("owner_id") != record["owner_id"]
        or value.get("holder_id") != record["holder_id"]
        or value.get("lease_revision") != record["revision"]
        or value.get("lease_acquired_at") != record["acquired_at"]
        or value.get("lease_expires_at") != record["expires_at"]
        or value.get("evidence_store_identity_sha256")
        != evidence_store_identity_sha256
        or value.get("create_authority_store_location_sha256")
        != create_authority_store_location_sha256
        or type(value.get("requested")) is not bool
        or type(value.get("confirmation_valid")) is not bool
        or type(value.get("gateway_gate_granted")) is not bool
        or type(value.get("configuration_verified")) is not bool
        or type(value.get("granted")) is not bool
        or (
            value["confirmation_sha256"] is not None
            and HEX64.fullmatch(value["confirmation_sha256"] or "") is None
        )
        or value["confirmation_valid"] != (value["confirmation_sha256"] is not None)
        or (value["confirmation_valid"] and not value["requested"])
        or (
            value["gateway_gate_granted"]
            and not (value["requested"] and value["confirmation_valid"])
        )
        or (value["configuration_verified"] and not value["confirmation_valid"])
        or value["granted"]
        != (value["gateway_gate_granted"] and value["configuration_verified"])
    ):
        raise ValueError("provider create authorization is invalid")
    _scope(scope_prefix)
    if HEX64.fullmatch(evidence_store_identity_sha256 or "") is None:
        raise ValueError("provider create authorization store is invalid")
    if HEX64.fullmatch(create_authority_store_location_sha256 or "") is None:
        raise ValueError("provider create authority location is invalid")
    return value


def create_provider_create_authorization(
    store,
    record,
    scope_prefix,
    evidence_store_identity_sha256,
    create_authority_store_location_sha256,
    gate,
    configuration_verified,
):
    gate = _provider_create_gate(gate)
    if type(configuration_verified) is not bool:
        raise ValueError("provider configuration verification is invalid")
    authorization = {
        "schema_version": "milk.provider-create-authorization.v1",
        "campaign_id": record["campaign_id"],
        "scope_prefix": scope_prefix,
        "owner_id": record["owner_id"],
        "holder_id": record["holder_id"],
        "lease_revision": record["revision"],
        "lease_acquired_at": record["acquired_at"],
        "lease_expires_at": record["expires_at"],
        "evidence_store_identity_sha256": evidence_store_identity_sha256,
        "create_authority_store_location_sha256": (
            create_authority_store_location_sha256
        ),
        "requested": gate["requested"],
        "confirmation_valid": gate["confirmation_valid"],
        "gateway_gate_granted": gate["granted"],
        "configuration_verified": configuration_verified,
        "granted": gate["granted"] and configuration_verified,
        "confirmation_sha256": gate["confirmation_sha256"],
    }
    _provider_create_authorization(
        authorization,
        record,
        scope_prefix,
        evidence_store_identity_sha256,
        create_authority_store_location_sha256,
    )
    if not authorization["granted"]:
        raise ValueError("provider create authorization must be granted")
    raw = canonical_json(authorization)
    key = _provider_create_authorization_key(record["campaign_id"], record["holder_id"])
    create_same(store, key, raw, "application/json")
    return {
        "object_key": key,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def provider_lease_token(
    record,
    scope_prefix,
    evidence_store_identity_sha256,
    create_authority_store_location_sha256,
    create_authorization_reference,
):
    campaign_id = record.get("campaign_id") if isinstance(record, dict) else None
    record = _lease_record(record, campaign_id)
    _scope(scope_prefix)
    if (
        record["state"] != "active"
        or HEX64.fullmatch(evidence_store_identity_sha256 or "") is None
    ):
        raise ValueError("active provider campaign lease token inputs are required")
    if create_authorization_reference is None:
        if create_authority_store_location_sha256 is not None:
            raise ValueError("provider create authorization reference is invalid")
        authorization_key = None
        authorization_sha256 = None
    else:
        expected_key = _provider_create_authorization_key(
            campaign_id, record["holder_id"]
        )
        if (
            HEX64.fullmatch(create_authority_store_location_sha256 or "") is None
            or not isinstance(create_authorization_reference, dict)
            or set(create_authorization_reference) != {"object_key", "sha256"}
            or create_authorization_reference.get("object_key") != expected_key
            or HEX64.fullmatch(create_authorization_reference.get("sha256") or "")
            is None
        ):
            raise ValueError("provider create authorization reference is invalid")
        authorization_key = create_authorization_reference["object_key"]
        authorization_sha256 = create_authorization_reference["sha256"]
    return {
        "schema_version": "milk.provider-campaign-lease-token.v3",
        "object_key": _lease_key(campaign_id),
        "campaign_id": campaign_id,
        "scope_prefix": scope_prefix,
        "owner_id": record["owner_id"],
        "holder_id": record["holder_id"],
        "revision": record["revision"],
        "acquired_at": record["acquired_at"],
        "expires_at": record["expires_at"],
        "evidence_store_identity_sha256": evidence_store_identity_sha256,
        "create_authority_store_location_sha256": (
            create_authority_store_location_sha256
        ),
        "create_authorization_object_key": authorization_key,
        "create_authorization_sha256": authorization_sha256,
    }


def _provider_lease_token(value):
    fields = {
        "schema_version",
        "object_key",
        "campaign_id",
        "scope_prefix",
        "owner_id",
        "holder_id",
        "revision",
        "acquired_at",
        "expires_at",
        "evidence_store_identity_sha256",
        "create_authority_store_location_sha256",
        "create_authorization_object_key",
        "create_authorization_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema_version") != "milk.provider-campaign-lease-token.v3"
        or HEX64.fullmatch(value.get("campaign_id") or "") is None
        or value.get("object_key") != _lease_key(value["campaign_id"])
        or HEX64.fullmatch(value.get("owner_id") or "") is None
        or HEX64.fullmatch(value.get("holder_id") or "") is None
        or type(value.get("revision")) is not int
        or value["revision"] < 1
        or HEX64.fullmatch(value.get("evidence_store_identity_sha256") or "")
        is None
    ):
        raise ValueError("provider campaign lease token is invalid")
    authorization_values = (
        value.get("create_authority_store_location_sha256"),
        value.get("create_authorization_object_key"),
        value.get("create_authorization_sha256"),
    )
    if any(item is not None for item in authorization_values):
        if (
            any(item is None for item in authorization_values)
            or HEX64.fullmatch(authorization_values[0] or "") is None
            or authorization_values[1]
            != _provider_create_authorization_key(
                value["campaign_id"], value["holder_id"]
            )
            or HEX64.fullmatch(authorization_values[2] or "") is None
        ):
            raise ValueError("provider campaign lease token is invalid")
    _scope(value.get("scope_prefix"))
    acquired = _utc(value.get("acquired_at"), "provider lease acquisition")
    expires = _utc(value.get("expires_at"), "provider lease expiry")
    if expires <= acquired:
        raise ValueError("provider campaign lease token is invalid")
    return value


def _load_provider_create_authorization(store, token):
    token = _provider_lease_token(token)
    if token["create_authorization_object_key"] is None:
        raise ValueError("provider create authorization is absent")
    raw = store.get(token["create_authorization_object_key"])
    if hashlib.sha256(raw).hexdigest() != token["create_authorization_sha256"]:
        raise ValueError("provider create authorization digest differs")
    value = _strict_object(raw)
    if canonical_json(value) != raw:
        raise ValueError("provider create authorization is not canonical")
    record = {
        "schema_version": "milk.provider-campaign-lease.v1",
        "campaign_id": token["campaign_id"],
        "owner_id": token["owner_id"],
        "holder_id": token["holder_id"],
        "state": "active",
        "revision": token["revision"],
        "acquired_at": token["acquired_at"],
        "expires_at": token["expires_at"],
        "released_at": None,
    }
    return _provider_create_authorization(
        value,
        record,
        token["scope_prefix"],
        token["evidence_store_identity_sha256"],
        token["create_authority_store_location_sha256"],
    )


def verify_provider_lease_token(
    store,
    create_authority_store,
    token,
    campaign_id,
    scope_prefix,
    evidence_store_identity_sha256,
    create_authority_store_location_sha256,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    token = _provider_lease_token(token)
    if (
        token["campaign_id"] != campaign_id
        or token["scope_prefix"] != scope_prefix
        or token["evidence_store_identity_sha256"]
        != evidence_store_identity_sha256
        or token["create_authority_store_location_sha256"]
        != create_authority_store_location_sha256
    ):
        raise ValueError("provider campaign lease token differs from this jobs pass")
    reference = active_provider_lease_reference(
        store,
        campaign_id,
        token["owner_id"],
        token["holder_id"],
        now=now,
    )
    if reference != {
        "object_key": token["object_key"],
        "owner_id": token["owner_id"],
        "revision": token["revision"],
        "acquired_at": token["acquired_at"],
        "expires_at": token["expires_at"],
    }:
        raise RuntimeError("provider campaign lease token is stale")
    if token["create_authorization_sha256"] is None:
        if create_authority_store is not None:
            raise ValueError("reconcile-only provider pass must not receive create authority")
        authorization = None
    else:
        if create_authority_store is None:
            raise ValueError("provider create authority store is required")
        authorization = _load_provider_create_authorization(
            create_authority_store, token
        )
    return {**reference, "create_authorization": authorization}


def claim_provider_jobs_pass(
    store,
    create_authority_store,
    token,
    campaign_id,
    scope_prefix,
    evidence_store_identity_sha256,
    create_authority_store_location_sha256,
    *,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    current = now()
    verified = verify_provider_lease_token(
        store,
        create_authority_store,
        token,
        campaign_id,
        scope_prefix,
        evidence_store_identity_sha256,
        create_authority_store_location_sha256,
        now=lambda: current,
    )
    token = _provider_lease_token(token)
    claim = {
        "schema_version": "milk.provider-jobs-pass-claim.v1",
        "campaign_id": campaign_id,
        "scope_prefix": scope_prefix,
        "owner_id": token["owner_id"],
        "holder_id": token["holder_id"],
        "lease_revision": verified["revision"],
        "lease_expires_at": verified["expires_at"],
        "lease_token_sha256": hashlib.sha256(canonical_json(token)).hexdigest(),
        "create_authorization_sha256": token["create_authorization_sha256"],
        "create_authority_store_location_sha256": token[
            "create_authority_store_location_sha256"
        ],
        "provider_creates_authorized": (
            verified["create_authorization"] is not None
            and verified["create_authorization"]["granted"]
        ),
        "evidence_store_identity_sha256": evidence_store_identity_sha256,
        "claimed_at": _utc_text(current),
    }
    key = (
        f"campaigns/v1/{campaign_id}/provider-jobs-pass-claims/"
        f"{token['holder_id']}.json"
    )
    if not store.create(key, canonical_json(claim), "application/json"):
        raise RuntimeError("provider campaign lease token was already claimed")
    return claim


def _environment_store_identity(prefix):
    return store_identity_sha256(
        os.environ.get(prefix + "ACCOUNT_ID"),
        os.environ.get(prefix + "BUCKET"),
        os.environ.get(prefix + "ACCESS_KEY_ID"),
    )


def _validate_create_authority_environment():
    write = tuple(
        os.environ.get("MILK_CREATE_AUTHORITY_WRITE_R2_" + name)
        for name in ("ACCOUNT_ID", "BUCKET", "ACCESS_KEY_ID")
    )
    read = tuple(
        os.environ.get("MILK_CREATE_AUTHORITY_READ_R2_" + name)
        for name in ("ACCOUNT_ID", "BUCKET", "ACCESS_KEY_ID")
    )
    if (
        any(not isinstance(value, str) or not value for value in (*write, *read))
        or write[:2] != read[:2]
        or write[2] == read[2]
    ):
        raise ValueError(
            "provider create authority requires one bucket and distinct read/write credentials"
        )
    return store_location_sha256(write[0], write[1])


def _require_no_create_authority_environment():
    names = (
        "ACCOUNT_ID",
        "BUCKET",
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
        "SESSION_TOKEN",
    )
    if any(
        os.environ.get(prefix + name)
        for prefix in (
            "MILK_CREATE_AUTHORITY_WRITE_R2_",
            "MILK_CREATE_AUTHORITY_READ_R2_",
        )
        for name in names
    ):
        raise ValueError(
            "reconcile-only provider pass must not receive create authority credentials"
        )


def _provider_secret_names(values):
    if not isinstance(values, dict) or set(values) != set(PROVIDER_SECRET_FIELDS):
        raise ValueError("provider secret-name set is invalid")
    required = set(PROVIDER_SECRET_FIELDS) - {"capture_session", "control_session"}
    for name, value in values.items():
        if value is None and name not in required:
            continue
        if not isinstance(value, str) or SECRET_NAME.fullmatch(value) is None:
            raise ValueError("provider secret name is invalid")
    retained = [value for value in values.values() if value is not None]
    if len(retained) != len(set(retained)):
        raise ValueError("provider secret names must be distinct")
    return values


def _provider_runtime(values):
    if not isinstance(values, dict) or set(values) != set(PROVIDER_RUNTIME_FIELDS):
        raise ValueError("provider runtime settings are invalid")
    if (
        not isinstance(values["gpu_provider"], str)
        or values["gpu_provider"] not in {"baseten", "modal"}
        or not isinstance(values["baseten_team_name"], str)
        or TEAM_NAME.fullmatch(values["baseten_team_name"]) is None
        or not isinstance(values["winner_model_alias"], str)
        or MODEL_ALIAS.fullmatch(values["winner_model_alias"]) is None
        or any(
            not isinstance(values[name], str) or OPAQUE.fullmatch(values[name]) is None
            for name in PROVIDER_RUNTIME_FIELDS
            if name not in {
                "gpu_provider",
                "baseten_team_name",
                "winner_model_alias",
            }
        )
    ):
        raise ValueError("provider runtime setting is invalid")
    resource_names = [
        values[name]
        for name in (
            "modal_registry_secret_name",
            "modal_config_secret_name",
            "modal_control_secret_name",
            "modal_capture_secret_name",
            "modal_candidate_secret_name",
            "modal_teacher_volume_name",
            "modal_student_train_volume_name",
        )
    ]
    resource_ids = [
        values[name]
        for name in (
            "modal_registry_secret_id",
            "modal_config_secret_id",
            "modal_control_secret_id",
            "modal_capture_secret_id",
            "modal_candidate_secret_id",
            "modal_teacher_volume_id",
            "modal_student_train_volume_id",
        )
    ]
    if len(resource_names) != len(set(resource_names)) or len(resource_ids) != len(
        set(resource_ids)
    ):
        raise ValueError("Modal resource identities must be pairwise distinct")
    return values


def _actual_store_identities(values, names):
    if (
        not isinstance(values, dict)
        or set(values) != set(names)
        or any(HEX64.fullmatch(value or "") is None for value in values.values())
    ):
        raise ValueError("actual object-store identities are invalid")
    return values


def _gateway_store_identity(config, name, access_key_id):
    stores = config.get("stores") if isinstance(config, dict) else None
    if not isinstance(stores, dict):
        raise ValueError("gateway object-store config is invalid")
    store = stores.get(name)
    if (
        not isinstance(store, dict)
        or store.get("type") != "cloudflare_r2"
        or set(store) != {"type", "account_id", "bucket"}
    ):
        raise ValueError("gateway object-store config is invalid")
    return store_identity_sha256(
        store["account_id"], store["bucket"], access_key_id
    )


def _gateway_store_identities(config, capture_access_key_id, control_access_key_id):
    return {
        "gateway_capture": _gateway_store_identity(
            config, "capture", capture_access_key_id
        ),
        "gateway_control": _gateway_store_identity(
            config, "control", control_access_key_id
        ),
    }


def _gateway_job_contract(config):
    teacher = config.get("teacher") if isinstance(config, dict) else None
    execution = teacher.get("execution") if isinstance(teacher, dict) else None
    if (
        not isinstance(execution, dict)
        or set(execution)
        != {
            "type",
            "runtime_image_reference",
            "max_gpu_seconds",
            "max_calls",
            "max_parallel_runs",
        }
        or execution.get("type") != "gpu_job"
        or any(
            type(execution.get(name)) is not int or execution[name] <= 0
            for name in ("max_gpu_seconds", "max_calls", "max_parallel_runs")
        )
        or type(teacher.get("max_decisions")) is not int
        or not 1 <= teacher["max_decisions"] <= MAX_TEACHER_DECISIONS
        or not isinstance(teacher.get("student_recipe_sha256"), str)
        or HEX64.fullmatch(teacher["student_recipe_sha256"]) is None
    ):
        raise ValueError("gateway GPU job contract is invalid")
    return {
        "teacher_runtime_image_reference": execution["runtime_image_reference"],
        "teacher_max_gpu_seconds": execution["max_gpu_seconds"],
        "teacher_max_calls": execution["max_calls"],
        "teacher_max_parallel_runs": execution["max_parallel_runs"],
        "teacher_max_decisions": teacher["max_decisions"],
        "student_train_runtime_image_reference": teacher.get(
            "student_train_runtime_image_reference"
        ),
        "student_branch_runtime_image_reference": teacher.get(
            "student_branch_runtime_image_reference"
        ),
        "student_recipe_sha256": teacher["student_recipe_sha256"],
        "teacher_deployment_sha256": teacher.get("deployment_sha256"),
        "teacher_terms_sha256": teacher.get("terms_sha256"),
    }


def _validated_run_manifest(raw, confirmed_sha256, harness_source_commit, root):
    manifest = _strict_object(raw)
    canonical = canonical_json(manifest)
    if raw not in {canonical, canonical.removesuffix(b"\n")}:
        raise ValueError("confirmed run config must be canonical JSON")
    if (
        not isinstance(confirmed_sha256, str)
        or hashlib.sha256(canonical).hexdigest() != confirmed_sha256
    ):
        raise ValueError("confirmed run config SHA-256 differs")
    expected_keys = {
        "schema_version",
        "provider_policy",
        "harness_source_commit",
        "campaign_id",
        "provider_project_id",
        "provider_runtime",
        "scope_prefix",
        "images",
        "image_release_sha256",
        "gateway_config_sha256",
        "gateway_deployment",
        "gateway_job_contract",
        "provider_secret_names",
        "store_identity_sha256s",
        "harness_artifact_sha256s",
        "limits",
    }
    if (
        set(manifest) != expected_keys
        or manifest.get("schema_version") != "milk.confirmed-production-run-config.v5"
        or manifest.get("provider_policy")
        != {"primary": "baseten", "fallback": "modal"}
        or not isinstance(harness_source_commit, str)
        or SOURCE_COMMIT.fullmatch(harness_source_commit) is None
        or manifest.get("harness_source_commit") != harness_source_commit
        or HEX64.fullmatch(manifest.get("campaign_id") or "") is None
        or PROVIDER_PROJECT.fullmatch(manifest.get("provider_project_id") or "") is None
        or HEX64.fullmatch(manifest.get("image_release_sha256") or "") is None
        or HEX64.fullmatch(manifest.get("gateway_config_sha256") or "") is None
        or manifest.get("limits") != RUN_LIMITS
        or manifest.get("harness_artifact_sha256s") != _artifact_hashes(root)
    ):
        raise ValueError("confirmed run config is invalid")
    _scope(manifest["scope_prefix"])
    _gateway_deployment(manifest.get("gateway_deployment"))
    _provider_runtime(manifest.get("provider_runtime"))
    _provider_secret_names(manifest.get("provider_secret_names"))
    store_identities = manifest.get("store_identity_sha256s")
    if (
        not isinstance(store_identities, dict)
        or set(store_identities) != set(STORE_IDENTITIES)
        or any(HEX64.fullmatch(value or "") is None for value in store_identities.values())
    ):
        raise ValueError("confirmed object-store identities are invalid")
    images = manifest.get("images")
    if not isinstance(images, dict) or set(images) != set(IMAGE_REPOSITORIES):
        raise ValueError("confirmed run config images are invalid")
    for name, value in images.items():
        immutable_image(name, value)
    _require_distinct_student_image_digests(images)
    contract = manifest.get("gateway_job_contract")
    if (
        not isinstance(contract, dict)
        or set(contract)
        != {
            "teacher_runtime_image_reference",
            "teacher_max_gpu_seconds",
            "teacher_max_calls",
            "teacher_max_parallel_runs",
            "teacher_max_decisions",
            "student_train_runtime_image_reference",
            "student_branch_runtime_image_reference",
            "student_recipe_sha256",
            "teacher_deployment_sha256",
            "teacher_terms_sha256",
        }
        or contract.get("teacher_runtime_image_reference") != images["teacher"]
        or contract.get("student_train_runtime_image_reference")
        != images["student_train"]
        or contract.get("student_branch_runtime_image_reference")
        != images["student_branch"]
        or any(
            type(contract.get(name)) is not int or contract[name] <= 0
            for name in (
                "teacher_max_gpu_seconds",
                "teacher_max_calls",
                "teacher_max_parallel_runs",
            )
        )
        or type(contract.get("teacher_max_decisions")) is not int
        or not (
            1 <= contract["teacher_max_decisions"] <= MAX_TEACHER_DECISIONS
        )
        or any(
            HEX64.fullmatch(contract.get(name) or "") is None
            for name in (
                "student_recipe_sha256",
                "teacher_deployment_sha256",
                "teacher_terms_sha256",
            )
        )
    ):
        raise ValueError("confirmed gateway GPU job contract is invalid")
    return manifest


def _gateway_deployment(value):
    if not isinstance(value, dict) or set(value) != set(GATEWAY_DEPLOYMENT_FIELDS):
        raise ValueError("gateway deployment identity is invalid")
    for name in ("application_id", "worker_version_id"):
        item = value.get(name)
        try:
            parsed = uuid.UUID(item)
        except (AttributeError, ValueError) as error:
            raise ValueError("gateway deployment identity is invalid") from error
        if parsed.int == 0 or str(parsed) != item:
            raise ValueError("gateway deployment identity is invalid")
    if (
        SOURCE_COMMIT.fullmatch(value.get("source_commit") or "") is None
        or HEX64.fullmatch(value.get("image_admission_sha256") or "") is None
        or HEX64.fullmatch(value.get("release_sha256") or "") is None
        or type(value.get("application_version")) is not int
        or value["application_version"] <= 0
        or not isinstance(value.get("container_image"), str)
        or not 1 <= len(value["container_image"].encode()) <= 2048
    ):
        raise ValueError("gateway deployment identity is invalid")
    return value


def _gateway_deployment_arguments(arguments):
    return {
        "source_commit": arguments.gateway_source_commit,
        "image_admission_sha256": arguments.gateway_image_admission_sha256,
        "release_sha256": arguments.gateway_release_sha256,
        "application_id": arguments.gateway_container_application_id,
        "application_version": arguments.gateway_container_application_version,
        "container_image": arguments.gateway_container_image,
        "worker_version_id": arguments.gateway_worker_version_id,
    }


def verify_route_run_config(
    manifest_raw,
    confirmed_sha256,
    harness_source_commit,
    root,
    gateway_config_raw,
    gateway_image,
    gateway_deployment,
    control_access_key_id,
    route_access_key_id,
    route_evidence_identity,
):
    manifest = _validated_run_manifest(
        manifest_raw, confirmed_sha256, harness_source_commit, root
    )
    config = _strict_object(gateway_config_raw)
    actual_identities = {
        "route_control": _gateway_store_identity(
            config, "control", control_access_key_id
        ),
        "route_routes": _gateway_store_identity(config, "routes", route_access_key_id),
        "route_evidence": route_evidence_identity,
    }
    expected_identities = {
        name: manifest["store_identity_sha256s"][name]
        for name in actual_identities
    }
    if (
        hashlib.sha256(canonical_json(config)).hexdigest()
        != manifest["gateway_config_sha256"]
        or manifest["images"]["gateway"] != immutable_image("gateway", gateway_image)
        or manifest["gateway_deployment"]
        != _gateway_deployment(gateway_deployment)
        or actual_identities != expected_identities
    ):
        raise ValueError("route gateway settings differ from the confirmed run config")
    return manifest


def verify_gateway_anchor_run_config(
    manifest_raw,
    confirmed_sha256,
    harness_source_commit,
    root,
    gateway_config_raw,
    gateway_image,
    gateway_deployment,
):
    manifest = _validated_run_manifest(
        manifest_raw, confirmed_sha256, harness_source_commit, root
    )
    config = _strict_object(gateway_config_raw)
    if (
        hashlib.sha256(canonical_json(config)).hexdigest()
        != manifest["gateway_config_sha256"]
        or manifest["images"]["gateway"] != immutable_image("gateway", gateway_image)
        or manifest["gateway_deployment"]
        != _gateway_deployment(gateway_deployment)
    ):
        raise ValueError("gateway anchor differs from the confirmed run config")
    return manifest


def verify_gateway_run_config(
    manifest_raw,
    confirmed_sha256,
    harness_source_commit,
    root,
    gateway_config_raw,
    gateway_image,
    capture_access_key_id,
    control_access_key_id,
):
    manifest = _validated_run_manifest(
        manifest_raw, confirmed_sha256, harness_source_commit, root
    )
    config = _strict_object(gateway_config_raw)
    canonical_config = canonical_json(config)
    store_identities = _gateway_store_identities(
        config, capture_access_key_id, control_access_key_id
    )
    if (
        hashlib.sha256(canonical_config).hexdigest()
        != manifest["gateway_config_sha256"]
        or manifest["images"]["gateway"] != immutable_image("gateway", gateway_image)
        or _gateway_job_contract(config) != manifest["gateway_job_contract"]
        or _scope(manifest["scope_prefix"])
        != {
            name: config.get(name)
            for name in ("tenant_id", "project_id", "environment_id", "workload_id")
        }
        or {
            name: store_identities[name]
            for name in ("gateway_capture", "gateway_control")
        }
        != {
            name: manifest["store_identity_sha256s"][name]
            for name in ("gateway_capture", "gateway_control")
        }
    ):
        raise ValueError("actual gateway config differs from the confirmed run config")
    return manifest


def verify_gateway_ingest_run_config(
    manifest_raw,
    confirmed_sha256,
    gateway_config_raw,
    gateway_image,
    control_access_key_id,
    route_access_key_id,
):
    manifest = _strict_object(manifest_raw)
    canonical_manifest = canonical_json(manifest)
    if (
        manifest_raw not in {canonical_manifest, canonical_manifest.removesuffix(b"\n")}
        or not isinstance(confirmed_sha256, str)
        or hashlib.sha256(canonical_manifest).hexdigest() != confirmed_sha256
        or manifest.get("schema_version") != "milk.confirmed-production-run-config.v5"
        or manifest.get("provider_policy")
        != {"primary": "baseten", "fallback": "modal"}
        or not isinstance(manifest.get("images"), dict)
        or not isinstance(manifest.get("scope_prefix"), str)
        or not isinstance(manifest["images"].get("gateway"), str)
        or HEX64.fullmatch(manifest.get("gateway_config_sha256") or "") is None
        or not isinstance(manifest.get("store_identity_sha256s"), dict)
        or any(
            HEX64.fullmatch(
                manifest["store_identity_sha256s"].get(name, "")
            )
            is None
            for name in ("gateway_ingest_control", "gateway_ingest_route")
        )
    ):
        raise ValueError("confirmed gateway ingest config is invalid")
    _gateway_deployment(manifest.get("gateway_deployment"))
    config = _strict_object(gateway_config_raw)
    canonical_config = canonical_json(config)
    actual_identities = {
        "gateway_ingest_control": _gateway_store_identity(
            config, "control", control_access_key_id
        ),
        "gateway_ingest_route": _gateway_store_identity(
            config, "routes", route_access_key_id
        ),
    }
    if (
        hashlib.sha256(canonical_config).hexdigest()
        != manifest["gateway_config_sha256"]
        or manifest["images"].get("gateway")
        != immutable_image("gateway", gateway_image)
        or _scope(manifest["scope_prefix"])
        != {
            name: config.get(name)
            for name in ("tenant_id", "project_id", "environment_id", "workload_id")
        }
        or actual_identities
        != {
            name: manifest["store_identity_sha256s"][name]
            for name in ("gateway_ingest_control", "gateway_ingest_route")
        }
    ):
        raise ValueError("gateway ingest config differs from the confirmed run config")
    return manifest


def verify_provider_run_config(
    manifest_raw,
    confirmed_sha256,
    harness_source_commit,
    root,
    *,
    campaign_id,
    provider_project_id,
    scope_prefix,
    images,
    image_release_sha256,
    provider_runtime,
    provider_secret_names,
    store_identities,
    gateway_deployment,
    include_create_authority=True,
):
    manifest = _validated_run_manifest(
        manifest_raw, confirmed_sha256, harness_source_commit, root
    )
    if not isinstance(images, dict) or set(images) != set(IMAGE_REPOSITORIES):
        raise ValueError("actual provider images are invalid")
    actual_images = {
        name: immutable_image(name, images[name]) for name in IMAGE_REPOSITORIES
    }
    required_store_identities = (
        "provider_control",
        "evidence",
        "ops_log",
        "gateway_ingest_control",
        "gateway_ingest_route",
    ) + (
        ("create_authority_write", "create_authority_read")
        if include_create_authority
        else ()
    )
    store_identities = _actual_store_identities(
        store_identities,
        required_store_identities,
    )
    if (
        manifest["campaign_id"] != campaign_id
        or manifest["provider_project_id"] != provider_project_id
        or manifest["scope_prefix"] != scope_prefix
        or manifest["images"] != actual_images
        or manifest["image_release_sha256"] != image_release_sha256
        or manifest["gateway_deployment"]
        != _gateway_deployment(gateway_deployment)
        or manifest["provider_runtime"] != _provider_runtime(provider_runtime)
        or manifest["provider_secret_names"]
        != _provider_secret_names(provider_secret_names)
        or {
            name: store_identities[name]
            for name in required_store_identities
        }
        != {
            name: manifest["store_identity_sha256s"][name]
            for name in required_store_identities
        }
    ):
        raise ValueError("actual provider settings differ from the confirmed run config")
    return manifest


def _regular_file(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("scheduler stream path must be a regular file")
    return path


def _gateway_ingest_references(value, field, campaign_id):
    if field == "winner_result_references":
        identity_field = "claim_sha256"
        suffix = "/gateway-result.json"
        kind = "winner"
    elif field == "teardown_result_references":
        identity_field = "authorization_sha256"
        suffix = "/gateway-teardown-result.json"
        kind = "teardown"
    else:
        raise ValueError("gateway ingest reference type is invalid")
    references = value.get(field)
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    if not isinstance(references, list) or len(references) > MAX_GATEWAY_INGEST_REFERENCES:
        raise ValueError("gateway ingest references exceed their hard bound")
    identities = []
    for reference in references:
        key = reference.get("object_key") if isinstance(reference, dict) else None
        if (
            not isinstance(reference, dict)
            or tuple(reference)
            != tuple(
                sorted(
                    (
                        "student_job_id",
                        identity_field,
                        "object_key",
                        "object_sha256",
                        "bytes",
                    )
                )
            )
            or HEX64.fullmatch(reference.get("student_job_id", "")) is None
            or HEX64.fullmatch(reference.get(identity_field, "")) is None
            or HEX64.fullmatch(reference.get("object_sha256", "")) is None
            or type(reference.get("bytes")) is not int
            or not 1 <= reference["bytes"] <= MAX_STREAM_BYTES
            or not isinstance(key, str)
            or not key.startswith(prefix)
            or not key.endswith(suffix)
            or HEX64.fullmatch(key[len(prefix) : -len(suffix)]) is None
        ):
            raise ValueError("gateway ingest reference is invalid")
        identities.append((reference["student_job_id"], reference[identity_field]))
    if identities != sorted(set(identities)):
        raise ValueError("gateway ingest references are not sorted and unique")
    return [(kind, reference) for reference in references]


def _candidate_key_delivery_references(value, campaign_id):
    references = value.get("candidate_key_delivery_references")
    prefix = f"campaigns/v1/{campaign_id}/winner-jobs/"
    suffix = re.compile(r"/candidate-key/deliveries/[0-9]{2}\.json\Z")
    if not isinstance(references, list) or len(references) > MAX_GATEWAY_INGEST_REFERENCES:
        raise ValueError("candidate-key delivery references exceed their hard bound")
    identities = []
    for reference in references:
        key = reference.get("object_key") if isinstance(reference, dict) else None
        match = suffix.search(key) if isinstance(key, str) else None
        if (
            not isinstance(reference, dict)
            or tuple(reference)
            != ("bytes", "claim_sha256", "object_key", "object_sha256", "student_job_id")
            or HEX64.fullmatch(reference.get("student_job_id", "")) is None
            or HEX64.fullmatch(reference.get("claim_sha256", "")) is None
            or HEX64.fullmatch(reference.get("object_sha256", "")) is None
            or type(reference.get("bytes")) is not int
            or not 1 <= reference["bytes"] <= MAX_STREAM_BYTES
            or not isinstance(key, str)
            or not key.startswith(prefix)
            or match is None
            or HEX64.fullmatch(key[len(prefix) : match.start()]) is None
        ):
            raise ValueError("candidate-key delivery reference is invalid")
        identities.append((reference["student_job_id"], reference["claim_sha256"]))
    if identities != sorted(set(identities)):
        raise ValueError("candidate-key delivery references are not sorted and unique")
    return references


def stage_gateway_ingest(
    evidence_store,
    provider_result_raw,
    campaign_id,
    scope_prefix,
    output_dir,
):
    """Materialize only the exact jobs results consumed by the gateway."""
    if HEX64.fullmatch(campaign_id) is None:
        raise ValueError("gateway ingest campaign is invalid")
    value = _strict_object(provider_result_raw)
    if canonical_json(value) != provider_result_raw or tuple(value) != (
        "candidate_key_delivery_references",
        "create_authorization_sha256",
        "dispatched",
        "provider_creates_authorized",
        "provider_pass_claim_sha256",
        "provider_policy",
        "reconciliation",
        "schema_version",
        "scope_prefix",
        "teardown",
        "teardown_result_references",
        "winner_result_references",
    ):
        raise ValueError("provider jobs result is not canonical")
    if (
        value.get("schema_version") != "milk.jobs-pass.v3"
        or value.get("provider_policy")
        != {"fallback": "modal", "primary": "baseten"}
        or value.get("scope_prefix") != scope_prefix
        or type(value.get("provider_creates_authorized")) is not bool
        or HEX64.fullmatch(value.get("provider_pass_claim_sha256", "")) is None
        or (
            value.get("create_authorization_sha256") is not None
            and HEX64.fullmatch(value["create_authorization_sha256"]) is None
        )
    ):
        raise ValueError("provider jobs result identity is invalid")
    references = []
    for field in ("winner_result_references", "teardown_result_references"):
        references.extend(_gateway_ingest_references(value, field, campaign_id))
    candidate_references = _candidate_key_delivery_references(value, campaign_id)
    winner_identities = {
        (reference["student_job_id"], reference["claim_sha256"])
        for kind, reference in references
        if kind == "winner"
    }
    for reference in candidate_references:
        identity = (reference["student_job_id"], reference["claim_sha256"])
        if identity not in winner_identities:
            raise ValueError("candidate-key delivery has no matching winner result")
        raw = evidence_store.get(reference["object_key"])
        if (
            not isinstance(raw, bytes)
            or len(raw) != reference["bytes"]
            or hashlib.sha256(raw).hexdigest() != reference["object_sha256"]
        ):
            raise ValueError("candidate-key delivery differs from its evidence reference")
    output_dir = Path(output_dir)
    if (
        not output_dir.is_absolute()
        or output_dir.is_symlink()
        or not output_dir.is_dir()
        or any(output_dir.iterdir())
    ):
        raise ValueError("gateway ingest directory must be a fresh absolute directory")
    os.chmod(output_dir, 0o700)
    entries = []
    for index, (kind, reference) in enumerate(references):
        raw = evidence_store.get(reference["object_key"])
        if (
            not isinstance(raw, bytes)
            or len(raw) != reference["bytes"]
            or hashlib.sha256(raw).hexdigest() != reference["object_sha256"]
        ):
            raise ValueError("gateway ingest object differs from its evidence reference")
        name = f"{index:02d}-{kind}-{reference['object_sha256']}.json"
        path = output_dir / name
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o400,
        )
        with os.fdopen(descriptor, "wb") as output:
            if output.write(raw) != len(raw):
                raise OSError("gateway ingest object write was incomplete")
        entries.append(
            {
                "kind": kind,
                "file": name,
                "object_key": reference["object_key"],
                "sha256": reference["object_sha256"],
                "bytes": reference["bytes"],
            }
        )
    return {
        "schema_version": "milk.gateway-ingest-stage.v1",
        "campaign_id": campaign_id,
        "scope_prefix": scope_prefix,
        "entries": entries,
    }


def _accepted_fields(source, raw):
    value = _strict_object(raw)
    if source == "gateway":
        schema = value.get("schema_version")
        action = value.get("action")
        state = value.get("state")
        if action is not None:
            if value != {"action": "hold"}:
                raise ValueError("gateway result action is invalid")
            return {"schema_version": None, "action": "hold", "state": None}
        if schema not in GATEWAY_RESULT_SCHEMAS:
            raise ValueError("gateway result schema is invalid")
        if state is not None and (not isinstance(state, str) or SAFE_RESULT.fullmatch(state) is None):
            raise ValueError("gateway result state is invalid")
        return {"schema_version": schema, "action": None, "state": state}
    if source == "provider_jobs":
        if (
            value.get("schema_version") != "milk.jobs-pass.v3"
            or value.get("provider_policy")
            != {"fallback": "modal", "primary": "baseten"}
            or type(value.get("provider_creates_authorized")) is not bool
            or not isinstance(value.get("winner_result_references"), list)
            or len(value["winner_result_references"]) > MAX_GATEWAY_INGEST_REFERENCES
            or not isinstance(value.get("candidate_key_delivery_references"), list)
            or len(value["candidate_key_delivery_references"])
            > MAX_GATEWAY_INGEST_REFERENCES
            or not isinstance(value.get("teardown_result_references"), list)
            or len(value["teardown_result_references"]) > MAX_GATEWAY_INGEST_REFERENCES
        ):
            raise ValueError("provider jobs result is invalid")
        return {
            "schema_version": "milk.jobs-pass.v3",
            "provider_policy": "baseten_primary_modal_fallback",
            "provider_creates_authorized": value["provider_creates_authorized"],
            "winner_result_count": len(value["winner_result_references"]),
            "candidate_key_delivery_count": len(
                value["candidate_key_delivery_references"]
            ),
            "teardown_result_count": len(value["teardown_result_references"]),
        }
    raise ValueError("scheduler source is invalid")


def summarize_step(source, stdout_path, stderr_path, exit_code, started_at, completed_at):
    if source not in {"gateway", "provider_jobs"}:
        raise ValueError("scheduler source is invalid")
    if type(exit_code) is not int or not 0 <= exit_code <= 255:
        raise ValueError("scheduler exit code is invalid")
    started = _utc(started_at, "step start")
    completed = _utc(completed_at, "step completion")
    if completed < started:
        raise ValueError("scheduler step interval is invalid")
    stdout_path = _regular_file(stdout_path)
    stderr_path = _regular_file(stderr_path)
    stdout_bytes = stdout_path.stat().st_size
    stderr_bytes = stderr_path.stat().st_size
    gaps = []
    accepted = None
    if stdout_bytes:
        gaps.append(
            {
                "stream": "stdout",
                "reason": "raw_stream_discarded_after_whitelist",
                "observed_bytes": stdout_bytes,
            }
        )
    else:
        gaps.append(
            {
                "stream": "stdout",
                "reason": "empty_stream",
                "observed_bytes": 0,
            }
        )
    if stderr_bytes:
        gaps.append(
            {
                "stream": "stderr",
                "reason": "unstructured_stream_discarded_sensitive_content_risk",
                "observed_bytes": stderr_bytes,
            }
        )
    else:
        gaps.append(
            {"stream": "stderr", "reason": "empty_stream", "observed_bytes": 0}
        )
    state = "failed"
    if exit_code == 0:
        if max(stdout_bytes, stderr_bytes) > MAX_STREAM_BYTES:
            state = "invalid_result"
        else:
            try:
                accepted = _accepted_fields(source, stdout_path.read_bytes())
                state = "succeeded"
            except (UnicodeError, ValueError):
                state = "invalid_result"
    return {
        "schema_version": "milk.scheduler-step-summary.v1",
        "source": source,
        "state": state,
        "started_at": started_at,
        "completed_at": completed_at,
        "exit_code": exit_code,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
        "accepted_fields": accepted,
        "gaps": gaps,
    }


def _missing_gateway_summary():
    return {
        "schema_version": "milk.scheduler-step-summary.v1",
        "source": "gateway",
        "state": "missing",
        "started_at": None,
        "completed_at": None,
        "exit_code": None,
        "stdout_bytes": 0,
        "stderr_bytes": 0,
        "accepted_fields": None,
        "gaps": [
            {
                "stream": "step",
                "reason": "gateway_job_ended_before_sanitized_summary",
                "observed_bytes": 0,
            }
        ],
    }


def _validate_summary(value, source, *, allow_missing=False):
    if (
        not isinstance(value, dict)
        or set(value) != SUMMARY_KEYS
        or value.get("schema_version") != "milk.scheduler-step-summary.v1"
        or value.get("source") != source
        or value.get("state") not in {"succeeded", "failed", "invalid_result", "missing"}
        or type(value.get("stdout_bytes")) is not int
        or value["stdout_bytes"] < 0
        or type(value.get("stderr_bytes")) is not int
        or value["stderr_bytes"] < 0
        or not isinstance(value.get("gaps"), list)
    ):
        raise ValueError("scheduler step summary is invalid")
    if value["state"] == "missing":
        if not allow_missing or value != _missing_gateway_summary():
            raise ValueError("missing scheduler step summary is invalid")
        return value
    started = _utc(value.get("started_at"), "step start")
    completed = _utc(value.get("completed_at"), "step completion")
    if completed < started or type(value.get("exit_code")) is not int or not 0 <= value["exit_code"] <= 255:
        raise ValueError("scheduler step summary interval is invalid")
    if value["state"] == "succeeded":
        if value["exit_code"] != 0 or not isinstance(value.get("accepted_fields"), dict):
            raise ValueError("successful scheduler step summary is invalid")
        fields = value["accepted_fields"]
        if source == "gateway":
            if set(fields) != {"schema_version", "action", "state"}:
                raise ValueError("gateway accepted fields are invalid")
            for name in ("schema_version", "action", "state"):
                item = fields[name]
                if item is not None and (
                    not isinstance(item, str) or SAFE_RESULT.fullmatch(item) is None
                ):
                    raise ValueError("gateway accepted fields are invalid")
            if fields["schema_version"] is None and fields["action"] is None:
                raise ValueError("gateway accepted fields are invalid")
        elif source == "provider_jobs":
            if fields != {
                "schema_version": "milk.jobs-pass.v3",
                "provider_policy": "baseten_primary_modal_fallback",
                "provider_creates_authorized": fields.get("provider_creates_authorized"),
                "winner_result_count": fields.get("winner_result_count"),
                "candidate_key_delivery_count": fields.get(
                    "candidate_key_delivery_count"
                ),
                "teardown_result_count": fields.get("teardown_result_count"),
            } or (
                type(fields["provider_creates_authorized"]) is not bool
                or type(fields["winner_result_count"]) is not int
                or not 0
                <= fields["winner_result_count"]
                <= MAX_GATEWAY_INGEST_REFERENCES
                or type(fields["candidate_key_delivery_count"]) is not int
                or not 0
                <= fields["candidate_key_delivery_count"]
                <= MAX_GATEWAY_INGEST_REFERENCES
                or type(fields["teardown_result_count"]) is not int
                or not 0
                <= fields["teardown_result_count"]
                <= MAX_GATEWAY_INGEST_REFERENCES
            ):
                raise ValueError("provider jobs accepted fields are invalid")
        else:
            raise ValueError("scheduler source is invalid")
    elif value.get("accepted_fields") is not None:
        raise ValueError("unsuccessful scheduler step retained accepted fields")
    elif value["state"] == "invalid_result" and value["exit_code"] != 0:
        raise ValueError("invalid-result scheduler summary is invalid")
    elif value["state"] == "failed" and value["exit_code"] == 0:
        raise ValueError("failed scheduler summary is invalid")
    expected_gaps = [
        {
            "stream": "stdout",
            "reason": (
                "raw_stream_discarded_after_whitelist"
                if value["stdout_bytes"]
                else "empty_stream"
            ),
            "observed_bytes": value["stdout_bytes"],
        },
        {
            "stream": "stderr",
            "reason": (
                "unstructured_stream_discarded_sensitive_content_risk"
                if value["stderr_bytes"]
                else "empty_stream"
            ),
            "observed_bytes": value["stderr_bytes"],
        },
    ]
    if value["gaps"] != expected_gaps:
        raise ValueError("scheduler gap marker is invalid")
    return value


def encode_summary(value):
    _validate_summary(value, value.get("source"), allow_missing=True)
    return base64.urlsafe_b64encode(canonical_json(value)).decode("ascii")


def decode_gateway_summary(value):
    if value == "":
        return _missing_gateway_summary()
    if not isinstance(value, str) or len(value) > 16 * 1024:
        raise ValueError("gateway summary transfer is invalid")
    try:
        raw = base64.b64decode(value, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise ValueError("gateway summary transfer is invalid") from error
    summary = _strict_object(raw, limit=16 * 1024)
    if canonical_json(summary) != raw:
        raise ValueError("gateway summary transfer is not canonical")
    return _validate_summary(summary, "gateway", allow_missing=False)


def _event(value):
    if value not in {"schedule", "workflow_dispatch"}:
        raise ValueError("GitHub event is invalid")
    return value


def _boolean(value, label):
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{label} must be true or false")


def validate_create_confirmation(requested, provided_sha256, expected_sha256):
    requested = _boolean(requested, "provider create request")
    if not requested:
        if provided_sha256 not in {"", None}:
            raise ValueError("reconcile-only pass cannot carry a create confirmation")
        return None
    if (
        not isinstance(provided_sha256, str)
        or not isinstance(expected_sha256, str)
        or HEX64.fullmatch(provided_sha256) is None
        or HEX64.fullmatch(expected_sha256) is None
        or provided_sha256 != expected_sha256
    ):
        raise ValueError("provider create confirmation differs from the confirmed run config")
    return provided_sha256


def _create_authority(
    requested,
    confirmation_valid,
    configuration_verified,
    granted,
    provided_sha256,
    expected_sha256,
):
    requested = _boolean(requested, "provider create request")
    confirmation_valid = _boolean(confirmation_valid, "provider confirmation validity")
    configuration_verified = _boolean(
        configuration_verified, "provider configuration verification"
    )
    granted = _boolean(granted, "provider create grant")
    if confirmation_valid:
        confirmation = validate_create_confirmation(
            "true" if requested else "false", provided_sha256, expected_sha256
        )
    else:
        if provided_sha256 not in {"", None}:
            raise ValueError("invalid create confirmation input must be discarded")
        confirmation = None
    if confirmation_valid and not requested:
        raise ValueError("reconcile-only pass cannot validate create confirmation")
    if configuration_verified and not (requested and confirmation_valid):
        raise ValueError("run configuration cannot be verified without confirmation")
    if granted and not (requested and confirmation_valid and configuration_verified):
        raise ValueError("provider creates require verified confirmed configuration")
    return {
        "requested": requested,
        "confirmation_valid": confirmation_valid,
        "configuration_verified": configuration_verified,
        "granted": granted,
        "confirmation_sha256": confirmation,
    }


def provider_create_gate(
    event,
    requested,
    provided_sha256,
    expected_sha256,
    gateway_result,
    gateway_accepted,
):
    _event(event)
    if requested not in {"", "true", "false"}:
        raise ValueError("provider create request is invalid")
    requested_bool = event == "workflow_dispatch" and requested == "true"
    confirmation_valid = (
        requested_bool
        and isinstance(provided_sha256, str)
        and isinstance(expected_sha256, str)
        and HEX64.fullmatch(provided_sha256) is not None
        and provided_sha256 == expected_sha256
    )
    if gateway_result not in {"", "success", "failure", "cancelled", "skipped"}:
        raise ValueError("gateway job result is invalid")
    if gateway_accepted not in {"", "true", "false"}:
        raise ValueError("gateway summary acceptance is invalid")
    granted = (
        confirmation_valid
        and gateway_result == "success"
        and gateway_accepted == "true"
    )
    return {
        "schema_version": "milk.provider-create-gate.v1",
        "requested": requested_bool,
        "confirmation_valid": confirmation_valid,
        "granted": granted,
        "confirmation_sha256": provided_sha256 if confirmation_valid else None,
    }


def archive_scheduler_pass(
    *,
    ops_store,
    evidence_store,
    repository_id,
    run_id,
    run_attempt,
    event,
    gateway_summary,
    provider_summary,
    gateway_image,
    jobs_image,
    student_train_image,
    student_branch_image,
    teacher_image,
    image_release_sha256,
    campaign_id,
    provider_lease,
    provider_create_requested="false",
    provider_confirmation_valid="false",
    provider_configuration_verified="false",
    provider_create_granted="false",
    confirmation_sha256="",
    expected_confirmation_sha256="",
):
    pass_id = scheduler_pass_id(repository_id, run_id, run_attempt)
    provider_lease = _provider_lease_reference(
        provider_lease, campaign_id, pass_id
    )
    gateway_summary = _validate_summary(gateway_summary, "gateway", allow_missing=True)
    provider_summary = _validate_summary(provider_summary, "provider_jobs")
    images = {
        "gateway": immutable_image("gateway", gateway_image),
        "jobs": immutable_image("jobs", jobs_image),
        "student_train": immutable_image("student_train", student_train_image),
        "student_branch": immutable_image("student_branch", student_branch_image),
        "teacher": immutable_image("teacher", teacher_image),
    }
    _require_distinct_student_image_digests(images)
    if not isinstance(image_release_sha256, str) or HEX64.fullmatch(image_release_sha256) is None:
        raise ValueError("private image release SHA-256 is invalid")
    create_authority = _create_authority(
        provider_create_requested,
        provider_confirmation_valid,
        provider_configuration_verified,
        provider_create_granted,
        confirmation_sha256,
        expected_confirmation_sha256,
    )
    if gateway_summary["state"] != "succeeded" and create_authority["granted"]:
        raise ValueError("provider creates require a successful gateway tick")
    if provider_summary["state"] == "succeeded" and (
        provider_summary["accepted_fields"]["provider_creates_authorized"]
        != create_authority["granted"]
    ):
        raise ValueError("provider jobs result differs from scheduler create authority")
    artifact = {
        "schema_version": "milk.sanitized-ops-scheduler-pass.v1",
        "pass_id": pass_id,
        "github": {
            "repository_id": repository_id,
            "run_id": run_id,
            "run_attempt": run_attempt,
            "event": _event(event),
        },
        "images": images,
        "image_release_sha256": image_release_sha256,
        "provider_campaign_lease": provider_lease,
        "provider_create_authority": create_authority,
        "sanitization_profile": SANITIZATION_PROFILE,
        "sanitization_profile_sha256": SANITIZATION_PROFILE_SHA256,
        "retention_contract": RETENTION_CONTRACT,
        "steps": [gateway_summary, provider_summary],
        "coverage": COVERAGE,
    }
    raw = canonical_json(artifact)
    ops_key = f"operational/v1/scheduler-passes/{pass_id}/archive.json"
    create_same(ops_store, ops_key, raw, "application/json")
    reference = {
        "schema_version": "milk.operational-log-reference.v1",
        "pass_id": pass_id,
        "referenced_artifact_authoritative": False,
        "github": artifact["github"],
        "image_release_sha256": image_release_sha256,
        "provider_campaign_lease": artifact["provider_campaign_lease"],
        "provider_create_authority": create_authority,
        "ops_bucket": getattr(ops_store, "bucket", "local"),
        "ops_object_key": ops_key,
        "ops_object_bytes": len(raw),
        "ops_object_sha256": hashlib.sha256(raw).hexdigest(),
        "sanitization_profile_sha256": SANITIZATION_PROFILE_SHA256,
        "coverage_gap_count": sum(item["state"] == "gap" for item in COVERAGE),
        "retention_contract": RETENTION_CONTRACT,
    }
    reference_raw = canonical_json(reference)
    reference_key = f"operational-log-references/v1/scheduler-passes/{pass_id}.json"
    create_same(evidence_store, reference_key, reference_raw, "application/json")
    return reference


def _require_distinct_r2(ops_store, evidence_store):
    if (
        ops_store.bucket == evidence_store.bucket
        or ops_store.access_key_id == evidence_store.access_key_id
    ):
        raise ValueError("ops-log R2 bucket and access authority must be dedicated")
    control_bucket = os.environ.get("MILK_CONTROL_R2_BUCKET")
    control_access = os.environ.get("MILK_CONTROL_R2_ACCESS_KEY_ID")
    if ops_store.bucket == control_bucket or ops_store.access_key_id == control_access:
        raise ValueError("ops-log and control R2 authority must be dedicated")


def _read_summary(path, source):
    path = _regular_file(path)
    raw = path.read_bytes()
    value = _strict_object(raw, limit=16 * 1024)
    if canonical_json(value) != raw:
        raise ValueError("scheduler summary file is not canonical")
    return _validate_summary(value, source)


def load_provider_lease_token(path):
    path = _regular_file(path)
    if path.stat().st_size > 16 * 1024:
        raise ValueError("provider campaign lease token is invalid")
    raw = path.read_bytes()
    value = _strict_object(raw, limit=16 * 1024)
    if canonical_json(value) != raw:
        raise ValueError("provider campaign lease token is invalid")
    return _provider_lease_token(value)


def _write_github_output(path, summary):
    path = Path(path)
    encoded = encode_summary(summary)
    with path.open("a", encoding="utf-8") as output:
        output.write(f"summary_b64={encoded}\n")
        output.write(f"accepted={'true' if summary['state'] == 'succeeded' else 'false'}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the bounded Milk scheduler boundary")
    commands = parser.add_subparsers(dest="command", required=True)

    pass_id = commands.add_parser("pass-id")
    pass_id.add_argument("--repository-id", required=True)
    pass_id.add_argument("--run-id", required=True)
    pass_id.add_argument("--run-attempt", required=True)

    validate = commands.add_parser("validate-images")
    for name in IMAGE_REPOSITORIES:
        validate.add_argument(f"--{name.replace('_', '-')}-image", required=True)
    validate.add_argument("--image-release-sha256", required=True)

    validate_one = commands.add_parser("validate-image")
    validate_one.add_argument("--name", choices=tuple(IMAGE_REPOSITORIES), required=True)
    validate_one.add_argument("--image", required=True)

    confirmation = commands.add_parser("validate-confirmation")
    confirmation.add_argument("--requested", required=True)
    confirmation.add_argument("--provided-sha256", default="")
    confirmation.add_argument("--expected-sha256", default="")

    create_gate = commands.add_parser("create-gate")
    create_gate.add_argument("--event", required=True)
    create_gate.add_argument("--requested", default="")
    create_gate.add_argument("--provided-sha256", default="")
    create_gate.add_argument("--expected-sha256", default="")
    create_gate.add_argument("--gateway-result", default="")
    create_gate.add_argument("--gateway-accepted", default="")
    create_gate.add_argument("--output", required=True)

    run_config = commands.add_parser("validate-run-config")
    run_config.add_argument(
        "--phase",
        choices=(
            "gateway",
            "provider",
            "provider_base",
            "gateway_ingest",
            "route",
            "gateway_anchor",
        ),
        required=True,
    )
    run_config.add_argument("--manifest", required=True)
    run_config.add_argument("--confirmed-sha256", required=True)
    run_config.add_argument("--harness-source-commit", required=True)
    run_config.add_argument("--root", default=".")
    run_config.add_argument("--gateway-config")
    run_config.add_argument("--campaign-id")
    run_config.add_argument("--provider-project-id")
    run_config.add_argument("--scope-prefix")
    for name in IMAGE_REPOSITORIES:
        run_config.add_argument(f"--{name.replace('_', '-')}-image")
    run_config.add_argument("--image-release-sha256")
    run_config.add_argument("--gateway-source-commit")
    run_config.add_argument("--gateway-image-admission-sha256")
    run_config.add_argument("--gateway-release-sha256")
    run_config.add_argument("--gateway-container-application-id")
    run_config.add_argument("--gateway-container-application-version", type=int)
    run_config.add_argument("--gateway-container-image")
    run_config.add_argument("--gateway-worker-version-id")
    run_config.add_argument("--gateway-anchor-output")
    run_config.add_argument(
        "--include-create-authority", choices=("true", "false"), default="true"
    )
    for name in PROVIDER_RUNTIME_FIELDS:
        run_config.add_argument(f"--{name.replace('_', '-')}")
    for name in PROVIDER_SECRET_FIELDS:
        run_config.add_argument(f"--{name.replace('_', '-')}-secret")

    lease_acquire = commands.add_parser("lease-acquire")
    lease_acquire.add_argument("--campaign-id", required=True)
    lease_acquire.add_argument("--scope-prefix", required=True)
    lease_acquire.add_argument("--owner-id", required=True)
    lease_acquire.add_argument("--create-gate", required=True)
    lease_acquire.add_argument("--configuration-verified", required=True)
    lease_acquire.add_argument("--output", required=True)

    lease_release = commands.add_parser("lease-release")
    lease_release.add_argument("--token", required=True)

    stage_ingest = commands.add_parser("stage-gateway-ingest")
    stage_ingest.add_argument("--provider-result", required=True)
    stage_ingest.add_argument("--campaign-id", required=True)
    stage_ingest.add_argument("--scope-prefix", required=True)
    stage_ingest.add_argument("--output-dir", required=True)
    stage_ingest.add_argument("--output", required=True)

    summarize = commands.add_parser("summarize")
    summarize.add_argument("--source", choices=("gateway", "provider_jobs"), required=True)
    summarize.add_argument("--stdout", required=True)
    summarize.add_argument("--stderr", required=True)
    summarize.add_argument("--exit-code", type=int, required=True)
    summarize.add_argument("--started-at", required=True)
    summarize.add_argument("--completed-at", required=True)
    summarize.add_argument("--output", required=True)
    summarize.add_argument("--github-output")

    archive = commands.add_parser("archive")
    archive.add_argument("--repository-id", required=True)
    archive.add_argument("--run-id", required=True)
    archive.add_argument("--run-attempt", required=True)
    archive.add_argument("--event", required=True)
    archive.add_argument("--gateway-summary-b64", default="")
    archive.add_argument("--provider-summary", required=True)
    for name in IMAGE_REPOSITORIES:
        archive.add_argument(f"--{name.replace('_', '-')}-image", required=True)
    archive.add_argument("--image-release-sha256", required=True)
    archive.add_argument("--campaign-id", required=True)
    archive.add_argument("--lease-token", required=True)
    archive.add_argument("--provider-create-requested", required=True)
    archive.add_argument("--provider-confirmation-valid", required=True)
    archive.add_argument("--provider-configuration-verified", required=True)
    archive.add_argument("--provider-create-granted", required=True)
    archive.add_argument("--confirmation-sha256", default="")
    archive.add_argument("--expected-confirmation-sha256", default="")

    arguments = parser.parse_args(argv)
    if arguments.command == "pass-id":
        print(scheduler_pass_id(arguments.repository_id, arguments.run_id, arguments.run_attempt))
        return 0
    if arguments.command == "validate-images":
        for name in IMAGE_REPOSITORIES:
            immutable_image(name, getattr(arguments, f"{name}_image"))
        if HEX64.fullmatch(arguments.image_release_sha256) is None:
            raise ValueError("private image release SHA-256 is invalid")
        return 0
    if arguments.command == "validate-image":
        immutable_image(arguments.name, arguments.image)
        return 0
    if arguments.command == "validate-confirmation":
        validate_create_confirmation(
            arguments.requested,
            arguments.provided_sha256,
            arguments.expected_sha256,
        )
        return 0
    if arguments.command == "create-gate":
        value = provider_create_gate(
            arguments.event,
            arguments.requested,
            arguments.provided_sha256,
            arguments.expected_sha256,
            arguments.gateway_result,
            arguments.gateway_accepted,
        )
        output = Path(arguments.output)
        if output.exists():
            raise ValueError("provider create gate output must be new")
        output.write_bytes(canonical_json(value))
        return 0
    if arguments.command == "validate-run-config":
        manifest_raw = _regular_file(arguments.manifest).read_bytes()
        if arguments.phase == "gateway":
            if arguments.gateway_config is None or arguments.gateway_image is None:
                raise ValueError("gateway config validation inputs are required")
            verify_gateway_run_config(
                manifest_raw,
                arguments.confirmed_sha256,
                arguments.harness_source_commit,
                arguments.root,
                _regular_file(arguments.gateway_config).read_bytes(),
                arguments.gateway_image,
                os.environ.get("MILK_CAPTURE_STORE_ACCESS_KEY_ID"),
                os.environ.get("MILK_CONTROL_STORE_ACCESS_KEY_ID"),
            )
        elif arguments.phase == "gateway_ingest":
            if arguments.gateway_config is None or arguments.gateway_image is None:
                raise ValueError("gateway ingest config validation inputs are required")
            verify_gateway_ingest_run_config(
                manifest_raw,
                arguments.confirmed_sha256,
                _regular_file(arguments.gateway_config).read_bytes(),
                arguments.gateway_image,
                os.environ.get("MILK_GATEWAY_INGEST_CONTROL_R2_ACCESS_KEY_ID"),
                os.environ.get("MILK_GATEWAY_INGEST_ROUTE_R2_ACCESS_KEY_ID"),
            )
        elif arguments.phase == "provider_base":
            confirmed_manifest = _validated_run_manifest(
                manifest_raw,
                arguments.confirmed_sha256,
                arguments.harness_source_commit,
                arguments.root,
            )
            runtime_environment = {
                "baseten_team_name": "MILK_BASETEN_TEAM_NAME",
                "winner_model_alias": "MILK_WINNER_MODEL_ALIAS",
                **{
                    name: f"MILK_{name.upper()}"
                    for name in PROVIDER_RUNTIME_FIELDS
                    if name
                    not in {"gpu_provider", "baseten_team_name", "winner_model_alias"}
                },
            }
            secret_environment = {
                "registry": "MILK_BASETEN_REGISTRY_SECRET",
                "config": "MILK_BASETEN_CONFIG_SECRET",
                "capture_access": "MILK_BASETEN_CAPTURE_ACCESS_SECRET",
                "capture_secret": "MILK_BASETEN_CAPTURE_SECRET_SECRET",
                "capture_session": "MILK_BASETEN_CAPTURE_SESSION_SECRET",
                "control_access": "MILK_BASETEN_CONTROL_ACCESS_SECRET",
                "control_secret": "MILK_BASETEN_CONTROL_SECRET_SECRET",
                "control_session": "MILK_BASETEN_CONTROL_SESSION_SECRET",
            }
            verify_provider_run_config(
                manifest_raw,
                arguments.confirmed_sha256,
                arguments.harness_source_commit,
                arguments.root,
                campaign_id=os.environ.get("MILK_CAMPAIGN_ID"),
                provider_project_id=os.environ.get("MILK_BASETEN_PROJECT_ID"),
                scope_prefix=os.environ.get("MILK_SCOPE_PREFIX"),
                images={
                    name: os.environ.get(f"MILK_{name.upper()}_IMAGE")
                    for name in IMAGE_REPOSITORIES
                },
                image_release_sha256=os.environ.get("MILK_IMAGE_RELEASE_SHA256"),
                provider_runtime={
                    "gpu_provider": confirmed_manifest["provider_runtime"][
                        "gpu_provider"
                    ],
                    **{
                        name: os.environ.get(environment)
                        for name, environment in runtime_environment.items()
                    },
                },
                provider_secret_names={
                    name: os.environ.get(environment) or None
                    for name, environment in secret_environment.items()
                },
                store_identities={
                    "provider_control": _environment_store_identity("MILK_CONTROL_R2_"),
                    "evidence": _environment_store_identity("MILK_EVIDENCE_R2_"),
                    "ops_log": _environment_store_identity("MILK_OPS_LOG_R2_"),
                    "gateway_ingest_control": _environment_store_identity(
                        "MILK_GATEWAY_INGEST_CONTROL_R2_"
                    ),
                    "gateway_ingest_route": _environment_store_identity(
                        "MILK_GATEWAY_INGEST_ROUTE_R2_"
                    ),
                },
                gateway_deployment={
                    "source_commit": os.environ.get("MILK_GATEWAY_SOURCE_COMMIT"),
                    "image_admission_sha256": os.environ.get(
                        "MILK_GATEWAY_IMAGE_ADMISSION_SHA256"
                    ),
                    "release_sha256": os.environ.get("MILK_GATEWAY_RELEASE_SHA256"),
                    "application_id": os.environ.get(
                        "MILK_GATEWAY_CONTAINER_APPLICATION_ID"
                    ),
                    "application_version": int(
                        os.environ.get("MILK_GATEWAY_CONTAINER_APPLICATION_VERSION", "0")
                    ),
                    "container_image": os.environ.get(
                        "MILK_GATEWAY_CONTAINER_IMAGE"
                    ),
                    "worker_version_id": os.environ.get(
                        "MILK_GATEWAY_WORKER_VERSION_ID"
                    ),
                },
                include_create_authority=False,
            )
        elif arguments.phase in {"route", "gateway_anchor"}:
            if (
                arguments.gateway_config is None
                or arguments.gateway_image is None
            ):
                raise ValueError("route config validation inputs are required")
            common = (
                manifest_raw,
                arguments.confirmed_sha256,
                arguments.harness_source_commit,
                arguments.root,
                _regular_file(arguments.gateway_config).read_bytes(),
                arguments.gateway_image,
                _gateway_deployment_arguments(arguments),
            )
            manifest = (
                verify_route_run_config(
                    *common,
                    os.environ.get("MILK_CONTROL_STORE_ACCESS_KEY_ID"),
                    os.environ.get("MILK_ROUTE_STORE_ACCESS_KEY_ID"),
                    _environment_store_identity("MILK_ROUTE_EVIDENCE_R2_"),
                )
                if arguments.phase == "route"
                else verify_gateway_anchor_run_config(*common)
            )
            if arguments.gateway_anchor_output is not None:
                output = Path(arguments.gateway_anchor_output)
                if output.exists() or output.is_symlink():
                    raise ValueError("gateway anchor output must be new")
                with output.open("xb") as stream:
                    stream.write(canonical_json(manifest["gateway_deployment"]))
                output.chmod(0o600)
        else:
            include_create_authority = arguments.include_create_authority == "true"
            if include_create_authority:
                _validate_create_authority_environment()
            else:
                _require_no_create_authority_environment()
            images = {
                name: getattr(arguments, f"{name}_image")
                for name in IMAGE_REPOSITORIES
            }
            verify_provider_run_config(
                manifest_raw,
                arguments.confirmed_sha256,
                arguments.harness_source_commit,
                arguments.root,
                campaign_id=arguments.campaign_id,
                provider_project_id=arguments.provider_project_id,
                scope_prefix=arguments.scope_prefix,
                images=images,
                image_release_sha256=arguments.image_release_sha256,
                gateway_deployment=_gateway_deployment_arguments(arguments),
                provider_runtime={
                    name: getattr(arguments, name)
                    for name in PROVIDER_RUNTIME_FIELDS
                },
                provider_secret_names={
                    name: (
                        getattr(arguments, f"{name}_secret")
                        or None
                    )
                    for name in PROVIDER_SECRET_FIELDS
                },
                store_identities={
                    "provider_control": _environment_store_identity("MILK_CONTROL_R2_"),
                    "evidence": _environment_store_identity("MILK_EVIDENCE_R2_"),
                    "ops_log": _environment_store_identity("MILK_OPS_LOG_R2_"),
                    **(
                        {
                            "create_authority_write": _environment_store_identity(
                                "MILK_CREATE_AUTHORITY_WRITE_R2_"
                            ),
                            "create_authority_read": _environment_store_identity(
                                "MILK_CREATE_AUTHORITY_READ_R2_"
                            ),
                        }
                        if include_create_authority
                        else {}
                    ),
                    "gateway_ingest_control": _environment_store_identity(
                        "MILK_GATEWAY_INGEST_CONTROL_R2_"
                    ),
                    "gateway_ingest_route": _environment_store_identity(
                        "MILK_GATEWAY_INGEST_ROUTE_R2_"
                    ),
                },
                include_create_authority=include_create_authority,
            )
        return 0
    if arguments.command == "lease-acquire":
        store = R2EvidenceStore.from_environment()
        holder_id = secrets.token_hex(32)
        gate_raw = _regular_file(arguments.create_gate).read_bytes()
        gate = _strict_object(gate_raw, limit=16 * 1024)
        if canonical_json(gate) != gate_raw:
            raise ValueError("provider create gate is not canonical")
        _provider_create_gate(gate)
        if arguments.configuration_verified not in {"true", "false"}:
            raise ValueError("provider configuration verification is invalid")
        configuration_verified = arguments.configuration_verified == "true"
        creates_authorized = gate["granted"] and configuration_verified
        output = Path(arguments.output)
        if output.exists():
            raise ValueError("provider campaign lease token output must be new")
        create_authority_store = None
        create_authority_location = None
        if creates_authorized:
            create_authority_location = _validate_create_authority_environment()
            create_authority_store = R2EvidenceStore.from_environment(
                prefix="MILK_CREATE_AUTHORITY_WRITE_R2_"
            )
            if (
                store.bucket == create_authority_store.bucket
                or store.access_key_id == create_authority_store.access_key_id
            ):
                raise ValueError(
                    "provider create authority must use a dedicated bucket and writer"
                )
        else:
            _require_no_create_authority_environment()
        acquired = acquire_provider_lease(
            store, arguments.campaign_id, arguments.owner_id, holder_id
        )
        try:
            evidence_identity = _environment_store_identity("MILK_EVIDENCE_R2_")
            authorization = (
                create_provider_create_authorization(
                    create_authority_store,
                    acquired,
                    arguments.scope_prefix,
                    evidence_identity,
                    create_authority_location,
                    gate,
                    configuration_verified,
                )
                if creates_authorized
                else None
            )
            token = provider_lease_token(
                acquired,
                arguments.scope_prefix,
                evidence_identity,
                create_authority_location,
                authorization,
            )
            output.write_bytes(canonical_json(token))
        except Exception:
            release_provider_lease(
                store,
                acquired["campaign_id"],
                acquired["owner_id"],
                acquired["holder_id"],
            )
            raise
        return 0
    if arguments.command == "lease-release":
        token = load_provider_lease_token(arguments.token)
        store = R2EvidenceStore.from_environment()
        release_provider_lease(
            store,
            token["campaign_id"],
            token["owner_id"],
            token["holder_id"],
        )
        return 0
    if arguments.command == "stage-gateway-ingest":
        output = Path(arguments.output)
        if output.exists():
            raise ValueError("gateway ingest manifest output must be new")
        stage = stage_gateway_ingest(
            R2EvidenceStore.from_environment(),
            _regular_file(arguments.provider_result).read_bytes(),
            arguments.campaign_id,
            arguments.scope_prefix,
            arguments.output_dir,
        )
        output.write_bytes(canonical_json(stage))
        return 0
    if arguments.command == "summarize":
        summary = summarize_step(
            arguments.source,
            arguments.stdout,
            arguments.stderr,
            arguments.exit_code,
            arguments.started_at,
            arguments.completed_at,
        )
        output = Path(arguments.output)
        if output.exists():
            raise ValueError("scheduler summary output must be new")
        output.write_bytes(canonical_json(summary))
        if arguments.github_output:
            _write_github_output(arguments.github_output, summary)
        return 0
    gateway = decode_gateway_summary(arguments.gateway_summary_b64)
    provider = _read_summary(arguments.provider_summary, "provider_jobs")
    ops_store = R2EvidenceStore.from_environment(prefix="MILK_OPS_LOG_R2_")
    evidence_store = R2EvidenceStore.from_environment()
    _require_distinct_r2(ops_store, evidence_store)
    lease_token = load_provider_lease_token(arguments.lease_token)
    if lease_token["create_authorization_sha256"] is None:
        _require_no_create_authority_environment()
        create_authority_store = None
        create_authority_location = None
    else:
        create_authority_location = _validate_create_authority_environment()
        create_authority_store = R2EvidenceStore.from_environment(
            prefix="MILK_CREATE_AUTHORITY_READ_R2_"
        )
        _require_distinct_r2(create_authority_store, evidence_store)
    pass_id = scheduler_pass_id(
        arguments.repository_id, arguments.run_id, arguments.run_attempt
    )
    if (
        lease_token["campaign_id"] != arguments.campaign_id
        or lease_token["owner_id"] != pass_id
    ):
        raise ValueError("provider campaign lease token differs from this pass")
    verified_provider_lease = verify_provider_lease_token(
        evidence_store,
        create_authority_store,
        lease_token,
        arguments.campaign_id,
        lease_token["scope_prefix"],
        _environment_store_identity("MILK_EVIDENCE_R2_"),
        create_authority_location,
    )
    provider_lease = {
        name: verified_provider_lease[name]
        for name in ("object_key", "owner_id", "revision", "acquired_at", "expires_at")
    }
    reference = archive_scheduler_pass(
        ops_store=ops_store,
        evidence_store=evidence_store,
        repository_id=arguments.repository_id,
        run_id=arguments.run_id,
        run_attempt=arguments.run_attempt,
        event=arguments.event,
        gateway_summary=gateway,
        provider_summary=provider,
        gateway_image=arguments.gateway_image,
        jobs_image=arguments.jobs_image,
        student_train_image=arguments.student_train_image,
        student_branch_image=arguments.student_branch_image,
        teacher_image=arguments.teacher_image,
        image_release_sha256=arguments.image_release_sha256,
        campaign_id=arguments.campaign_id,
        provider_lease=provider_lease,
        provider_create_requested=arguments.provider_create_requested,
        provider_confirmation_valid=arguments.provider_confirmation_valid,
        provider_configuration_verified=arguments.provider_configuration_verified,
        provider_create_granted=arguments.provider_create_granted,
        confirmation_sha256=arguments.confirmation_sha256,
        expected_confirmation_sha256=arguments.expected_confirmation_sha256,
    )
    os.sys.stdout.buffer.write(canonical_json(reference))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, UnicodeError, ValueError) as error:
        print(f"milk-scheduler: {error}", file=os.sys.stderr)
        raise SystemExit(64) from None
