import datetime as dt
import hashlib
import io
import inspect
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import types
import unittest
from unittest import mock
import urllib.error
import urllib.parse

from deploy.baseten import adapter
import milk_harness.jobs as jobs_module
from milk_harness.budget import (
    CampaignBudget,
    MODAL_RATES_SOURCE_COMMAND,
    baseten_pricing_observation,
    baseten_provider_identity,
    modal_provider_identity,
    prepare_campaign_authority,
)
from milk_harness.evidence import LocalEvidenceStore, canonical_json, create_same
from milk_harness.image_admission import (
    load_local_private_image_release,
    load_published_private_image_release,
)
from milk_harness.log_collection import OOM_CLASSIFIER_SHA256
from milk_harness.publish_image_admission import publish_private_image_release
from milk_harness.scheduler import (
    acquire_provider_lease,
    create_provider_create_authorization,
    provider_lease_token,
    release_provider_lease,
    store_identity_sha256,
    store_location_sha256,
)
from milk_harness.jobs import (
    BasetenJobs,
    BasetenTransport,
    GPU_LAUNCH_SCAN_LIMIT,
    JobEvidence,
    RESERVATION_RATE_MICROUSD_PER_MINUTE,
    _require_separate_control_store,
    dispatch_outboxes,
    settings_from_arguments,
)


UTC = dt.timezone.utc
CAMPAIGN = "c" * 64
PROJECT = "project_123"
MODAL_WORKSPACE = "ws_123"
MODAL_ENVIRONMENT = "main"
MODAL_APP = "milk-gpu-jobs"
NOW = dt.datetime(2026, 8, 27, 20, 0, 0, tzinfo=UTC)
EVIDENCE_R2_ENVIRONMENT = {
    "MILK_EVIDENCE_R2_ACCOUNT_ID": "0" * 32,
    "MILK_EVIDENCE_R2_BUCKET": "evidence",
    "MILK_EVIDENCE_R2_ACCESS_KEY_ID": "evidence-key",
    "MILK_EVIDENCE_R2_SECRET_ACCESS_KEY": "evidence-secret",
}
CREATE_AUTHORITY_R2_ENVIRONMENT = {
    "MILK_CREATE_AUTHORITY_READ_R2_ACCOUNT_ID": "0" * 32,
    "MILK_CREATE_AUTHORITY_READ_R2_BUCKET": "create-authority",
    "MILK_CREATE_AUTHORITY_READ_R2_ACCESS_KEY_ID": "create-authority-read",
    "MILK_CREATE_AUTHORITY_READ_R2_SECRET_ACCESS_KEY": "create-authority-secret",
}
SCOPE = {
    "tenant_id": "11111111-1111-4111-8111-111111111111",
    "project_id": "22222222-2222-4222-8222-222222222222",
    "environment_id": "33333333-3333-4333-8333-333333333333",
    "workload_id": "44444444-4444-4444-8444-444444444444",
    "eval_id": CAMPAIGN,
}
SCOPE_PREFIX = "dt/v3/" + "/".join(
    (
        CAMPAIGN,
        SCOPE["tenant_id"],
        SCOPE["project_id"],
        SCOPE["environment_id"],
        SCOPE["workload_id"],
    )
)
TEST_IMAGE_ADMISSIONS = {}
for _artifact, _repository, _digit in (
    ("student-train", "ghcr.io/milkinfrastructure/milk-student-train", "1"),
    ("student-branch", "ghcr.io/milkinfrastructure/milk-student-branch", "f"),
    (
        "teacher-gpt-oss",
        "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
        "2",
    ),
):
    _raw = canonical_json(
        {
            "schema_version": "milk.private-image-admission.v1",
            "artifact": _artifact,
            "repository": _repository,
            "image_reference": _repository + "@sha256:" + _digit * 64,
        }
    )
    TEST_IMAGE_ADMISSIONS[_artifact] = {
        "raw": _raw,
        "sha256": hashlib.sha256(_raw).hexdigest(),
        "image_reference": _repository + "@sha256:" + _digit * 64,
    }
TEST_IMAGE_RELEASE_RAW = canonical_json(
    {
        "schema_version": "milk.private-harness-release.v4",
        "images": [
            {
                "admission_sha256": TEST_IMAGE_ADMISSIONS[artifact]["sha256"],
                "artifact": artifact,
                "image_reference": TEST_IMAGE_ADMISSIONS[artifact]["image_reference"],
                "ops_log_reference_sha256": "a" * 64,
            }
            for artifact in ("student-train", "student-branch", "teacher-gpt-oss")
        ],
    }
)
TEST_IMAGE_RELEASE_SHA256 = hashlib.sha256(TEST_IMAGE_RELEASE_RAW).hexdigest()


def lease_token_path(
    store, root, *, campaign_id=CAMPAIGN, now=None, authorized=True
):
    current = now or dt.datetime.now(UTC).replace(microsecond=0)
    if not hasattr(store, "host"):
        store.host = "0" * 32 + ".r2.cloudflarestorage.com"
        store.bucket = "evidence"
        store.access_key_id = "evidence-key"
    acquired = acquire_provider_lease(
        store,
        campaign_id,
        "8" * 64,
        "9" * 64,
        now=lambda: current,
    )
    identity = store_identity_sha256(
        EVIDENCE_R2_ENVIRONMENT["MILK_EVIDENCE_R2_ACCOUNT_ID"],
        EVIDENCE_R2_ENVIRONMENT["MILK_EVIDENCE_R2_BUCKET"],
        EVIDENCE_R2_ENVIRONMENT["MILK_EVIDENCE_R2_ACCESS_KEY_ID"],
    )
    authority = LocalEvidenceStore(Path(root) / "create-authority")
    authority.host = "0" * 32 + ".r2.cloudflarestorage.com"
    authority.bucket = "create-authority"
    authority.access_key_id = "create-authority-read"
    authority_location = store_location_sha256(
        CREATE_AUTHORITY_R2_ENVIRONMENT[
            "MILK_CREATE_AUTHORITY_READ_R2_ACCOUNT_ID"
        ],
        CREATE_AUTHORITY_R2_ENVIRONMENT["MILK_CREATE_AUTHORITY_READ_R2_BUCKET"],
    )
    authorization = (
        create_provider_create_authorization(
            authority,
            acquired,
            SCOPE_PREFIX,
            identity,
            authority_location,
            {
                "schema_version": "milk.provider-create-gate.v1",
                "requested": True,
                "confirmation_valid": True,
                "granted": True,
                "confirmation_sha256": "a" * 64,
            },
            True,
        )
        if authorized
        else None
    )
    token = provider_lease_token(
        acquired,
        SCOPE_PREFIX,
        identity,
        authority_location if authorized else None,
        authorization,
    )
    path = Path(root) / "provider-lease-token.json"
    path.write_bytes(canonical_json(token))
    return path, token, authority


def authorize(store, campaign_id=CAMPAIGN):
    baseten_source_body = (
        b"https://docs.baseten.co/deployment/resources\nH100 $0.10833/min\n"
    )
    baseten_observation = canonical_json(
        baseten_pricing_observation(
            campaign_id,
            hashlib.sha256(baseten_source_body).hexdigest(),
            NOW,
        )
    )
    modal_source_body = (
        b'{"gpu":"H100","gpuPrice":"0.001097",'
        b'"cpuPrice":"0.00003942","memoryPrice":"0.00000667"}\n'
    )
    modal_rates = canonical_json(
        {
            "schema_version": "milk.modal-workspace-rates.v1",
            "campaign_id": campaign_id,
            "provider_identity": modal_provider_identity(
                MODAL_WORKSPACE,
                MODAL_ENVIRONMENT,
                MODAL_APP,
            ),
            "source_command": MODAL_RATES_SOURCE_COMMAND,
            "source_sha256": hashlib.sha256(modal_source_body).hexdigest(),
            "observed_at": "2026-08-27T20:00:00Z",
            "currency": "USD",
            "rate_unit": "second",
            "h100_usd_per_second": "0.001097",
            "sandbox_cpu_usd_per_physical_core_second": "0.00003942",
            "sandbox_memory_usd_per_gib_second": "0.00000667",
            "region_mode": "default",
            "region_multiplier": "1",
        }
    )
    authority = prepare_campaign_authority(
        store,
        campaign_id,
        PROJECT,
        "milk",
        MODAL_WORKSPACE,
        MODAL_ENVIRONMENT,
        MODAL_APP,
        baseten_pricing_observation_raw=baseten_observation,
        baseten_pricing_source_body=baseten_source_body,
        modal_rates_receipt_raw=modal_rates,
        modal_rates_source_body=modal_source_body,
        now=lambda: NOW,
    )
    create_same(
        store,
        f"image-admissions/v1/releases/{TEST_IMAGE_RELEASE_SHA256}.json",
        TEST_IMAGE_RELEASE_RAW,
        "application/json",
    )
    for item in TEST_IMAGE_ADMISSIONS.values():
        create_same(
            store,
            f"image-admissions/v1/artifacts/{item['sha256']}.json",
            item["raw"],
            "application/json",
        )
    return authority


def provider_job(name, job_id, status="TRAINING_JOB_CREATED", *, seconds=0):
    created = NOW
    updated = NOW + dt.timedelta(seconds=seconds)
    return {
        "training_job": {
            "id": job_id,
            "created_at": created.isoformat().replace("+00:00", "Z"),
            "current_status": status,
            "instance_type": {
                "id": "h100_instance",
                "name": "H100",
                "memory_limit_mib": 65_536,
                "millicpu_limit": 8_000,
                "gpu_count": 1,
                "gpu_type": "H100",
                "gpu_memory_limit_mib": 81_920,
            },
            "updated_at": updated.isoformat().replace("+00:00", "Z"),
            "training_project_id": PROJECT,
            "training_project": {"id": PROJECT, "name": "milk"},
            "error_message": None,
            "node_count": 1,
            "name": name,
            "checkpoint_sync_status": None,
            "priority": 0,
            "availability_model": "dedicated",
            "user": None,
        }
    }


def provider_get(name, job_id, status="TRAINING_JOB_CREATED", *, seconds=0):
    envelope = provider_job(name, job_id, status, seconds=seconds)
    job = envelope["training_job"]
    return {
        "training_project": {
            "id": PROJECT,
            "name": "milk",
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "latest_job": job,
            "team_name": "milk",
        },
        "training_job": job,
    }


def provider_metrics(name, job_id, status, *, seconds=0):
    job = provider_job(name, job_id, status, seconds=seconds)["training_job"]
    return {
        "gpu_memory_usage_bytes": {},
        "gpu_utilization": {},
        "cpu_usage": [],
        "cpu_memory_usage_bytes": [],
        "ephemeral_storage": {"usage_bytes": [], "utilization": []},
        "training_job": job,
        "cache": {"usage_bytes": [], "utilization": []},
        "per_node_metrics": [],
    }


class FakeTransport:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []
        self.lock = threading.Lock()

    def request(self, method, path, body=None, query=None):
        with self.lock:
            self.calls.append((method, path, body, query))
        value = self.handler(method, path, body, query)
        if isinstance(value, Exception):
            raise value
        return (json.dumps(value, separators=(",", ":")) + "\n").encode()


def entry(name, seconds=120):
    return {
        "name": name,
        "payload": {"training_job": {"name": name}},
        "max_wall_seconds": seconds,
    }


def retained_time(value):
    epoch = dt.datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return f"{((delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000):020d}"


def workload(character, kind="teacher_run"):
    artifact = {
        "teacher_run": "teacher-gpt-oss",
        "student_train_merge": "student-train",
        "student_fanout": "student-branch",
    }[kind]
    identity = character * 64
    expires = NOW + dt.timedelta(days=7)
    expires_at = expires.isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    if kind == "teacher_run":
        claim_key = f"{SCOPE_PREFIX}/jobs/teacher-gpu/{identity}/claim.json"
        outbox_key = f"{SCOPE_PREFIX}/jobs/teacher-gpu/{identity}/launch-outbox.json"
        operation = {
            "teacher_run_id": identity,
            "provider_binding_sha256": "f" * 64,
            "slot": 0,
            "call_count": 1,
            "max_gpu_seconds": 3_600,
        }
    elif kind == "student_train_merge":
        claim_key = f"{SCOPE_PREFIX}/jobs/student/{identity}/claim.json"
        outbox_key = f"{SCOPE_PREFIX}/jobs/student/{identity}/launch-outbox.json"
        operation = {
            "student_job_id": identity,
            "counts": {"train": 50, "dev": 73, "calibration": 128},
            "max_gpu_seconds": 1_800,
        }
    else:
        claim_key = f"{SCOPE_PREFIX}/jobs/student/{identity}/fanout/claim.json"
        outbox_key = (
            f"{SCOPE_PREFIX}/jobs/student/{identity}/fanout/launch-outbox.json"
        )
        operation = {
            "student_job_id": identity,
            "max_total_gpu_seconds": 7_200,
            "branches": [
                {
                    "schema_version": "dragontales.student-branch-claim.v2",
                    "branch_id": digit * 64,
                    "variant": variant,
                    "max_gpu_seconds": 1_800,
                }
                for digit, variant in zip(
                    "123",
                    ("bf16", "dynamic_fp8", "static_fp8"),
                )
            ],
        }
    source = {
        "schema_version": "milk.gateway-launch-source.v1",
        "scope": SCOPE,
        "dispatch_id": identity,
        "frontier_object_key": (
            f"{SCOPE_PREFIX}/frontier/gpu-launch/"
            f"{retained_time(expires)}/{identity}.json"
        ),
        "frontier_sha256": "d" * 64,
        "outbox_object_key": outbox_key,
        "outbox_sha256": "e" * 64,
        "claim_object_key": claim_key,
        "claim_sha256": identity,
        "created_at": NOW.isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        ),
        "expires_at": expires_at,
    }
    return {
        "type": kind,
        **operation,
        "launch_source": source,
        "image_admission_sha256": TEST_IMAGE_ADMISSIONS[artifact]["sha256"],
        "image_release_sha256": TEST_IMAGE_RELEASE_SHA256,
    }


def launch_acceptance(source, run_id, definition, evidence):
    return {
        "schema_version": "milk.gateway-launch-acceptance.v1",
        "launch_source": source["launch_source"],
        "provider": "baseten",
        "project_id": PROJECT,
        "campaign_id": CAMPAIGN,
        "run_id": run_id,
        "evidence_prefix": evidence.prefix,
        "execution_run_ids": [
            item["execution_run_id"] for item in definition["executions"]
        ],
    }


def gateway_json(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def control_launch(
    store,
    kind,
    nonce,
    *,
    created=None,
    expires=None,
    claim_mutator=None,
):
    created = created or NOW
    expires = expires or NOW + dt.timedelta(hours=5)
    created_at = created.isoformat().replace("+00:00", "Z")
    expires_at = expires.isoformat().replace("+00:00", "Z")
    scope = dict(SCOPE)
    if kind == "teacher_run":
        definition = {
            "schema_version": "dragontales.teacher-gpu-run-definition.v1",
            "provider_binding_sha256": hashlib.sha256(
                f"provider-{nonce}".encode()
            ).hexdigest(),
            "slot": nonce % 16,
            "run_nonce": f"00000000-0000-4000-8000-{nonce + 1:012x}",
            "slot_claim_sha256": hashlib.sha256(
                f"slot-{nonce}".encode()
            ).hexdigest(),
            "runtime_image_reference": TEST_IMAGE_ADMISSIONS[
                "teacher-gpt-oss"
            ]["image_reference"],
            "max_gpu_seconds": 3_600,
            "calls": [
                {
                    "teacher_job_id": hashlib.sha256(
                        f"teacher-job-{nonce}".encode()
                    ).hexdigest(),
                    "claim_sha256": hashlib.sha256(
                        f"teacher-claim-{nonce}".encode()
                    ).hexdigest(),
                    "dispatch_sha256": hashlib.sha256(
                        f"teacher-dispatch-{nonce}".encode()
                    ).hexdigest(),
                }
            ],
            "expires_at": expires_at,
        }
        identity = hashlib.sha256(gateway_json(definition)).hexdigest()
        claim = {
            "schema_version": "dragontales.teacher-gpu-run-claim.v1",
            "scope": scope,
            "teacher_run_id": identity,
            "definition": definition,
            "claimed_at": created_at,
        }
        operation = {
            "kind": "teacher_run",
            "teacher_run_id": identity,
            "provider_binding_sha256": definition[
                "provider_binding_sha256"
            ],
            "slot": definition["slot"],
            "call_count": len(definition["calls"]),
            "max_gpu_seconds": definition["max_gpu_seconds"],
        }
        parent = f"{SCOPE_PREFIX}/jobs/teacher-gpu/{identity}"
    elif kind == "student_train_merge":
        counts = {"train": 50, "dev": 73, "calibration": 128}
        definition = {
            "schema_version": "dragontales.student-job-definition.v4",
            "teacher_provider_binding_sha256": hashlib.sha256(
                f"student-provider-{nonce}".encode()
            ).hexdigest(),
            "teacher_results": [
                {
                    "teacher_job_id": hashlib.sha256(
                        f"student-teacher-{nonce}".encode()
                    ).hexdigest(),
                    "object_sha256": hashlib.sha256(
                        f"student-result-{nonce}".encode()
                    ).hexdigest(),
                    "bytes": 1,
                }
            ],
            "input_sha256": hashlib.sha256(
                f"student-input-{nonce}".encode()
            ).hexdigest(),
            "dev_set_sha256": hashlib.sha256(
                f"student-dev-{nonce}".encode()
            ).hexdigest(),
            "counts": counts,
            "recipe_sha256": hashlib.sha256(
                f"student-recipe-{nonce}".encode()
            ).hexdigest(),
            "student_train_runtime_image_reference": TEST_IMAGE_ADMISSIONS[
                "student-train"
            ]["image_reference"],
            "student_branch_runtime_image_reference": TEST_IMAGE_ADMISSIONS[
                "student-branch"
            ][
                "image_reference"
            ],
            "max_train_gpu_seconds": 1_800,
            "max_branch_gpu_seconds": 1_800,
            "max_total_gpu_seconds": 7_200,
            "quality": {
                "min_mean_score_bps": 9_500,
                "max_mean_loss_vs_bf16_bps": 0,
                "max_p95_latency_ms": 30_000,
            },
            "expires_at": expires_at,
        }
        identity = hashlib.sha256(gateway_json(definition)).hexdigest()
        claim = {
            "schema_version": "dragontales.student-job-claim.v4",
            "scope": scope,
            "student_job_id": identity,
            "definition": definition,
            "started_at": created_at,
        }
        operation = {
            "kind": "student_train_merge",
            "student_job_id": identity,
            "counts": counts,
            "max_gpu_seconds": 1_800,
        }
        parent = f"{SCOPE_PREFIX}/jobs/student/{identity}"
    elif kind == "student_fanout":
        identity = hashlib.sha256(f"fanout-student-{nonce}".encode()).hexdigest()
        branches = [
            {
                "schema_version": "dragontales.student-branch-claim.v2",
                "branch_id": hashlib.sha256(
                    f"fanout-{variant}-{nonce}".encode()
                ).hexdigest(),
                "variant": variant,
                "max_gpu_seconds": 1_800,
            }
            for variant in ("bf16", "dynamic_fp8", "static_fp8")
        ]
        claim = {
            "schema_version": "dragontales.student-fanout-claim.v4",
            "scope": scope,
            "student_job_id": identity,
            "train_result_sha256": hashlib.sha256(
                f"fanout-train-{nonce}".encode()
            ).hexdigest(),
            "recipe_sha256": hashlib.sha256(
                f"fanout-recipe-{nonce}".encode()
            ).hexdigest(),
            "student_branch_runtime_image_reference": TEST_IMAGE_ADMISSIONS[
                "student-branch"
            ][
                "image_reference"
            ],
            "max_total_gpu_seconds": 7_200,
            "created_at": created_at,
            "expires_at": expires_at,
            "branches": branches,
        }
        operation = {
            "kind": "student_fanout",
            "student_job_id": identity,
            "max_total_gpu_seconds": 7_200,
            "branches": branches,
        }
        parent = f"{SCOPE_PREFIX}/jobs/student/{identity}/fanout"
    elif kind == "student_winner_deployment":
        identity = hashlib.sha256(f"winner-student-{nonce}".encode()).hexdigest()
        authority = {
            "schema_version": "dragontales.winner-deployment-authority.v2",
            "provider_policy": {"primary": "baseten", "fallback": "modal"},
            "provider_terms_sha256": hashlib.sha256(
                f"winner-terms-{nonce}".encode()
            ).hexdigest(),
            "student_branch_runtime_image_reference": TEST_IMAGE_ADMISSIONS[
                "student-branch"
            ][
                "image_reference"
            ],
            "admission_program_sha256": hashlib.sha256(
                f"winner-admission-{nonce}".encode()
            ).hexdigest(),
            "authorization_not_after": expires_at,
            "max_wall_seconds": 3_600,
            "max_cost_microusd": 100_000_000,
            "allow_private_candidate_http": False,
            "signing_public_key_hex": hashlib.sha256(
                f"winner-signing-{nonce}".encode()
            ).hexdigest(),
            "signing_key_id": "winner-test-key",
            "candidate_max_in_flight": 1,
            "canary_candidate_basis_points": 100,
            "canary_valid_for_seconds": 900,
            "max_input_utf8_bytes": 2_048,
            "max_input_messages": 16,
            "max_input_request_bytes": 16_384,
            "route_schema_version": "dragontales.route.v4",
            "winner_admission_schema_version": (
                "dragontales.winner-admission-receipt.v2"
            ),
        }
        provider_binding = hashlib.sha256(
            b"dragontales.winner-provider-binding.v1\0"
            + gateway_json(authority)
        ).hexdigest()
        student_result_sha256 = hashlib.sha256(
            f"winner-result-{nonce}".encode()
        ).hexdigest()
        artifacts = {}
        for label in ("model_manifest", "dev_receipt"):
            digest = hashlib.sha256(f"winner-{label}-{nonce}".encode()).hexdigest()
            artifacts[label] = {
                "object_key": f"{SCOPE_PREFIX}/artifacts/{identity}/{digest}",
                "sha256": digest,
                "bytes": 1,
            }
        claim = {
            "schema_version": "dragontales.student-winner-deployment-claim.v2",
            "scope": scope,
            "student_job_id": identity,
            "student_claim_sha256": hashlib.sha256(
                f"winner-student-claim-{nonce}".encode()
            ).hexdigest(),
            "student_result_sha256": student_result_sha256,
            "winner": "static_fp8",
            "model_manifest": artifacts["model_manifest"],
            "dev_receipt": artifacts["dev_receipt"],
            "student_branch_runtime_image_reference": authority[
                "student_branch_runtime_image_reference"
            ],
            "provider_binding_sha256": provider_binding,
            "authority": authority,
            "claimed_at": created_at,
            "expires_at": expires_at,
        }
        operation = {
            "kind": "student_winner_deployment",
            "student_job_id": identity,
            "student_result_sha256": student_result_sha256,
            "winner": claim["winner"],
            "provider_binding_sha256": provider_binding,
            "provider_policy": authority["provider_policy"],
            "max_wall_seconds": authority["max_wall_seconds"],
            "max_cost_microusd": authority["max_cost_microusd"],
        }
        parent = f"{SCOPE_PREFIX}/jobs/student/{identity}/winner-deployment"
    else:
        raise AssertionError(kind)
    if claim_mutator is not None:
        claim_mutator(claim)
    claim_raw = gateway_json(claim)
    claim_sha256 = hashlib.sha256(claim_raw).hexdigest()
    claim_key = parent + "/claim.json"
    outbox_key = parent + "/launch-outbox.json"
    runtime_image = (
        claim["definition"]["runtime_image_reference"]
        if kind == "teacher_run"
        else claim["definition"]["student_train_runtime_image_reference"]
        if kind == "student_train_merge"
        else claim["student_branch_runtime_image_reference"]
    )
    outbox = {
        "schema_version": "dragontales.gpu-launch-outbox.v1",
        "scope": scope,
        "dispatch_id": claim_sha256,
        "claim_object_key": claim_key,
        "claim_sha256": claim_sha256,
        "runtime_image_reference": runtime_image,
        "created_at": created_at,
        "expires_at": expires_at,
        "operation": operation,
    }
    outbox_raw = gateway_json(outbox)
    frontier = {
        "schema_version": "dragontales.gpu-launch-frontier.v1",
        "scope": scope,
        "dispatch_id": claim_sha256,
        "outbox_object_key": outbox_key,
        "outbox_sha256": hashlib.sha256(outbox_raw).hexdigest(),
        "expires_at": expires_at,
    }
    frontier_key = (
        f"{SCOPE_PREFIX}/frontier/gpu-launch/{retained_time(expires)}/"
        f"{claim_sha256}.json"
    )
    created_objects = (
        store.create(claim_key, claim_raw, "application/json"),
        store.create(outbox_key, outbox_raw, "application/json"),
        store.create(frontier_key, gateway_json(frontier), "application/json"),
    )
    if created_objects != (True, True, True):
        raise AssertionError("control launch fixture collided")
    return {
        "claim_sha256": claim_sha256,
        "claim_key": claim_key,
        "outbox_key": outbox_key,
        "frontier_key": frontier_key,
        "claim_raw": claim_raw,
        "outbox_raw": outbox_raw,
        "frontier": frontier,
    }


def private_image_release(
    root, schema_version="milk.private-harness-release.v4"
):
    root = Path(root)
    if schema_version == "milk.private-harness-release.v3":
        repositories = {
            "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
            "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
            "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
            "planner": "ghcr.io/milkinfrastructure/milk-planner",
            "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
        }
    elif schema_version in {
        "milk.private-harness-release.v4",
        "milk.private-harness-release.v5",
    }:
        repositories = {
            "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
            "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
            "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
            "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
        }
    else:
        raise ValueError("test private image release schema is invalid")
    digests = {
        "student-train": "1" * 64,
        "student-branch": "f" * 64,
        "teacher-gpt-oss": "2" * 64,
        "planner": "3" * 64,
        "jobs": "4" * 64,
    }
    gateway = "ghcr.io/milkinfrastructure/milk-gateway@sha256:" + "5" * 64
    buildkit = "moby/buildkit@sha256:" + "6" * 64
    frontend = "docker/dockerfile@sha256:" + "7" * 64
    builder = {
        "authority": "local-socket",
        "driver": "docker-container",
        "endpoint_kind": "local-socket",
        "buildkit_image_reference": buildkit,
        "buildkit_version": "v0.23.2",
        "dockerfile_frontend_reference": frontend,
        "provenance_mode": "max",
        "provenance_version": "v1",
        "sbom": True,
    }
    images = []
    for artifact, repository in repositories.items():
        source_commit = (
            "a" * 40
            if schema_version == "milk.private-harness-release.v5"
            and artifact == "jobs"
            else "8" * 40
        )
        image = repository + "@sha256:" + digests[artifact]
        build_log_raw = canonical_json(
            {
                "schema_version": "milk.content-free-build-log.v1",
                "artifact": artifact,
                "exit_code": 0,
                "started_at": "2026-08-27T20:00:00Z",
                "completed_at": "2026-08-27T20:01:00Z",
                "sha256": digests[artifact],
                "bytes": 1,
                "content_retained": False,
            }
        )
        ops_log = {
            "schema_version": "milk.private-ops-log-reference.v1",
            "authority": "private-release-evidence",
            "reference": "build-log.json",
            "receipt_sha256": hashlib.sha256(build_log_raw).hexdigest(),
            "immutable": True,
            "content_retained": False,
        }
        ops_log_raw = canonical_json(ops_log)
        admission = {
            "schema_version": "milk.private-image-admission.v1",
            "artifact": artifact,
            "repository": repository,
            "image_reference": image,
            "source_repository": "https://github.com/milkinfrastructure/milk-harness",
            "source_commit": source_commit,
            "source_context_method": "git-archive-tar-v1",
            "source_context_sha256": "9" * 64,
            "gateway_image_reference": (
                None if artifact in {"planner", "jobs"} else gateway
            ),
            "index_sha256": digests[artifact],
            "amd64_manifest_sha256": "a" * 64,
            "config_sha256": "b" * 64,
            "attestation_manifest_sha256": "c" * 64,
            "attestations": [
                {
                    "layer_sha256": "d" * 64,
                    "predicate_type": "https://slsa.dev/provenance/v1",
                },
                {
                    "layer_sha256": "e" * 64,
                    "predicate_type": "https://spdx.dev/Document",
                },
            ],
            "ops_log_reference": ops_log,
            "ops_log_reference_sha256": hashlib.sha256(
                ops_log_raw
            ).hexdigest(),
            "platform": "linux/amd64",
            "visibility": "private",
            "builder": builder,
        }
        raw = canonical_json(admission)
        path = root / artifact / "admission.json"
        path.parent.mkdir(parents=True)
        path.write_bytes(raw)
        (path.parent / "build-log.json").write_bytes(build_log_raw)
        (path.parent / "ops-log-reference.json").write_bytes(ops_log_raw)
        release_item = {
            "admission_sha256": hashlib.sha256(raw).hexdigest(),
            "artifact": artifact,
            "image_reference": image,
            "ops_log_reference_sha256": hashlib.sha256(
                ops_log_raw
            ).hexdigest(),
        }
        if schema_version == "milk.private-harness-release.v5":
            release_item["source_commit"] = source_commit
        images.append(release_item)
    (root / "release.json").write_bytes(
        canonical_json(
            {
                "schema_version": schema_version,
                "source_commit": (
                    "a" * 40
                    if schema_version == "milk.private-harness-release.v5"
                    else "8" * 40
                ),
                "source_date_epoch": 1_777_777_777,
                "source_repository": "https://github.com/milkinfrastructure/milk-harness",
                "gateway_image_reference": gateway,
                "buildkit_image_reference": buildkit,
                "dockerfile_frontend_reference": frontend,
                "build_authority": "local-socket",
                "platform": "linux/amd64",
                "images": images,
                "started_at": "2026-08-27T20:00:00Z",
                "completed_at": "2026-08-27T20:01:00Z",
            }
        )
    )
    return str(root)


def publish_release(store, release_dir):
    release = load_local_private_image_release(private_image_release(release_dir))
    publish_private_image_release(store, release)
    return release


def provider_arguments(release_sha256):
    return types.SimpleNamespace(
        student_train_image=(
            "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "1" * 64
        ),
        student_branch_image=(
            "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "f" * 64
        ),
        teacher_image=(
            "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:"
            + "2" * 64
        ),
        jobs_image=(
            "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + "4" * 64
        ),
        registry_secret="milk-registry",
        config_secret="milk-config",
        capture_store_access_key_secret="capture-access",
        capture_store_secret_key_secret="capture-secret",
        capture_store_session_token_secret=None,
        control_store_access_key_secret="control-access",
        control_store_secret_key_secret="control-secret",
        control_store_session_token_secret=None,
        image_release_sha256=release_sha256,
    )


class BasetenJobsTests(unittest.TestCase):
    def test_provider_run_identity_rejects_another_eval_namespace(self):
        value = workload("a")
        other_eval = "b" * 64
        other_prefix = SCOPE_PREFIX.replace(CAMPAIGN, other_eval, 1)
        source = value["launch_source"]
        value["launch_source"] = {
            **source,
            "scope": {**source["scope"], "eval_id": other_eval},
            **{
                field: source[field].replace(SCOPE_PREFIX, other_prefix, 1)
                for field in (
                    "frontier_object_key",
                    "outbox_object_key",
                    "claim_object_key",
                )
            },
        }

        with self.assertRaisesRegex(ValueError, "eval differs"):
            jobs_module._workload_run_id(CAMPAIGN, value)

    def test_baseten_preflight_requires_live_h100_capacity(self):
        project = {
            "training_project": {
                "id": PROJECT,
                "name": "milk-production",
                "created_at": "2026-08-27T20:00:00Z",
                "updated_at": "2026-08-27T20:00:00Z",
                "latest_job": None,
                "team_name": "milk",
            }
        }
        capacity = {
            "gpu_capacities": [
                {
                    "gpu_type": "H100",
                    "baseline": 1,
                    "limit": 2,
                    "usage_count": 0,
                    "dedicated_usage_count": 0,
                    "spot_usage_count": 0,
                }
            ],
            "team_gpu_capacities": [],
        }

        def handler(method, path, body, query):
            self.assertEqual((method, body, query), ("GET", None, None))
            return project if path.endswith(PROJECT) else capacity

        with tempfile.TemporaryDirectory() as root:
            jobs = self.jobs(root, FakeTransport(handler))
            receipt = jobs.preflight("milk")
        project_raw = (json.dumps(project, separators=(",", ":")) + "\n").encode()
        capacity_raw = (json.dumps(capacity, separators=(",", ":")) + "\n").encode()
        self.assertEqual(receipt["outcome"], "ready")
        self.assertEqual(
            receipt["evidence_sha256"],
            hashlib.sha256(
                b"milk.baseten-training-preflight.v1\0"
                + project_raw
                + b"\0"
                + capacity_raw
            ).hexdigest(),
        )

    def test_baseten_preflight_falls_back_before_create_without_h100_capacity(self):
        project = {
            "training_project": {
                "id": PROJECT,
                "name": "milk-production",
                "created_at": "2026-08-27T20:00:00Z",
                "updated_at": "2026-08-27T20:00:00Z",
                "latest_job": None,
                "team_name": "milk",
            }
        }
        capacity = {
            "gpu_capacities": [
                {
                    "gpu_type": "H100",
                    "baseline": 0,
                    "limit": 1,
                    "usage_count": 1,
                }
            ],
            "team_gpu_capacities": [],
        }

        def handler(_method, path, _body, _query):
            return project if path.endswith(PROJECT) else capacity

        with tempfile.TemporaryDirectory() as root:
            jobs = self.jobs(root, FakeTransport(handler))
            receipt = jobs.preflight("milk")
        self.assertEqual(
            (receipt["outcome"], receipt["reason"], receipt["status"]),
            ("retryable_unavailable", "capability_unavailable", None),
        )
        capacity_raw = (json.dumps(capacity, separators=(",", ":")) + "\n").encode()
        self.assertEqual(
            receipt["evidence_sha256"], hashlib.sha256(capacity_raw).hexdigest()
        )

    def test_baseten_preflight_treats_missing_capacity_surface_as_unavailable(self):
        project = {
            "training_project": {
                "id": PROJECT,
                "name": "milk-production",
                "created_at": "2026-08-27T20:00:00Z",
                "updated_at": "2026-08-27T20:00:00Z",
                "latest_job": None,
                "team_name": "milk",
            }
        }

        def handler(_method, path, _body, _query):
            if path.endswith(PROJECT):
                return project
            return urllib.error.HTTPError(path, 404, "missing", None, None)

        with tempfile.TemporaryDirectory() as root:
            jobs = self.jobs(root, FakeTransport(handler))
            receipt = jobs.preflight("milk")
        self.assertEqual(
            (receipt["outcome"], receipt["reason"], receipt["status"]),
            ("retryable_unavailable", "capability_unavailable", None),
        )

    def test_baseten_preflight_retries_capacity_rate_limit(self):
        project = {
            "training_project": {
                "id": PROJECT,
                "name": "milk-production",
                "created_at": "2026-08-27T20:00:00Z",
                "updated_at": "2026-08-27T20:00:00Z",
                "latest_job": None,
                "team_name": "milk",
            }
        }

        def handler(_method, path, _body, _query):
            if path.endswith(PROJECT):
                return project
            return urllib.error.HTTPError(path, 429, "limited", None, None)

        with tempfile.TemporaryDirectory() as root:
            receipt = self.jobs(root, FakeTransport(handler)).preflight("milk")
        self.assertEqual(
            (receipt["outcome"], receipt["reason"], receipt["status"]),
            ("retryable_unavailable", "rate_limited", 429),
        )

    def test_baseten_preflight_retries_capacity_timeout(self):
        project = {
            "training_project": {
                "id": PROJECT,
                "name": "milk-production",
                "created_at": "2026-08-27T20:00:00Z",
                "updated_at": "2026-08-27T20:00:00Z",
                "latest_job": None,
                "team_name": "milk",
            }
        }

        def handler(_method, path, _body, _query):
            if path.endswith(PROJECT):
                return project
            return TimeoutError("capacity timed out")

        with tempfile.TemporaryDirectory() as root:
            receipt = self.jobs(root, FakeTransport(handler)).preflight("milk")
        self.assertEqual(
            (receipt["outcome"], receipt["reason"], receipt["status"]),
            ("retryable_unavailable", "timeout", None),
        )

    def test_modal_preflight_uses_live_1_5_4_identity_surface(self):
        calls = []

        class WorkspaceHandle:
            name = "milk-production"

            def hydrate(self):
                calls.append("workspace")

        class EnvironmentHandle:
            object_id = "en-main"
            name = "main"

            def hydrate(self):
                calls.append("environment")

        class AppHandle:
            app_id = "ap-production"
            name = "milk-gpu-jobs"

        sdk = types.SimpleNamespace(
            __version__="1.5.4",
            Workspace=types.SimpleNamespace(from_context=lambda: WorkspaceHandle()),
            Environment=types.SimpleNamespace(
                from_name=lambda name, **unused: EnvironmentHandle()
            ),
            App=types.SimpleNamespace(
                lookup=lambda name, **unused: AppHandle()
            ),
        )
        with tempfile.TemporaryDirectory() as root:
            runtime = jobs_module.ModalJobs(
                store=LocalEvidenceStore(Path(root) / "evidence"),
                request_guard=lambda: None,
                modal_sdk=sdk,
            )
            with self.assertRaisesRegex(ValueError, "plans differ by campaign"):
                jobs_module.modal_ready_preflight(
                    runtime,
                    {
                        "workspace_id": "ws-production",
                        "workspace_name": "milk-production",
                        "environment_id": "en-main",
                        "environment_name": "main",
                        "app_id": "ap-production",
                        "app_name": "milk-gpu-jobs",
                    },
                    {},
                )
        self.assertEqual(calls, ["workspace", "environment"])

    def test_modal_model_volumes_are_distinct_and_used_only_when_needed(self):
        arguments = types.SimpleNamespace(
            modal_registry_secret_name="registry",
            modal_registry_secret_id="st-registry",
            modal_config_secret_name="config",
            modal_config_secret_id="st-config",
            modal_control_secret_name="control",
            modal_control_secret_id="st-control",
            modal_capture_secret_name="capture",
            modal_capture_secret_id="st-capture",
            modal_candidate_secret_name="candidate",
            modal_candidate_secret_id="st-candidate",
            modal_teacher_volume_name="teacher-model",
            modal_teacher_volume_id="vo-teacher",
            modal_student_train_volume_name="student-model",
            modal_student_train_volume_id="vo-student",
        )
        settings = {
            "control_store_session_token_secret": None,
            "capture_store_session_token_secret": None,
        }
        resources = jobs_module._modal_resources_from_arguments(arguments, settings)
        self.assertEqual(
            resources["student_train_volume"],
            {
                "name": "student-model",
                "object_id": "vo-student",
                "mount_path": adapter.STUDENT_MODEL_MOUNT,
                "read_only": True,
            },
        )
        self.assertEqual(
            jobs_module._modal_plan_resources(
                resources, "student_train_merge"
            )["volumes"],
            [resources["student_train_volume"]],
        )
        self.assertEqual(
            jobs_module._modal_plan_resources(resources, "student_branch")[
                "volumes"
            ],
            [],
        )
        arguments.modal_student_train_volume_id = arguments.modal_teacher_volume_id
        with self.assertRaisesRegex(ValueError, "pairwise distinct"):
            jobs_module._modal_resources_from_arguments(arguments, settings)

    def test_cross_provider_policy_requires_both_provider_credentials(self):
        self.assertNotIn(
            "gpu_provider",
            inspect.signature(
                jobs_module.dispatch_cross_provider_outboxes
            ).parameters,
        )
        self.assertNotIn(
            "allow_modal_winner_fallback",
            inspect.signature(
                jobs_module.dispatch_cross_provider_outboxes
            ).parameters,
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "primary provider"):
                jobs_module._required_provider_credentials()
        with mock.patch.dict(
            os.environ,
            {"BASETEN_API_KEY": "baseten-key"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "provider fallback"):
                jobs_module._required_provider_credentials()
        with mock.patch.dict(
            os.environ,
            {
                "BASETEN_API_KEY": "baseten-key",
                "MODAL_TOKEN_ID": "modal-id",
                "MODAL_TOKEN_SECRET": "modal-secret",
            },
            clear=True,
        ):
            self.assertEqual(
                jobs_module._required_provider_credentials(),
                "baseten-key",
            )

    def test_production_api_has_no_operator_candidate_key_escape_hatch(self):
        self.assertNotIn(
            "candidate_key",
            inspect.signature(jobs_module.launch_baseten_winner).parameters,
        )
        self.assertNotIn(
            "winner_candidate_keys_by_run_id",
            inspect.signature(
                jobs_module.dispatch_cross_provider_outboxes
            ).parameters,
        )
        self.assertNotIn(
            "needs_model_scoped_candidate_key",
            jobs_module.BASETEN_WINNER_ADMIT_STATES,
        )

    def test_production_main_recovers_key_handoff_before_create_gate(self):
        source = inspect.getsource(jobs_module.main)
        self.assertLess(
            source.index("recover_pending_baseten_winner_admissions("),
            source.rindex("if provider_creates_authorized:"),
        )
        self.assertIn('"--candidate-key-socket", required=True', source)
        self.assertNotIn("candidate-key-output-fifo", source)

    def test_candidate_key_delivery_requires_owner_only_socket_ack(self):
        key = "candidate-secret"
        raw = canonical_json(
            {
                "schema_version": "milk.baseten-candidate-key-delivery.v1",
                "run_id": "a" * 64,
                "provider": "baseten",
                "team_name": "milk",
                "model_id": "model1",
                "key_name": "milk-" + "a" * 56 + "-00",
                "key_prefix": "candidate",
                "candidate_key_sha256": hashlib.sha256(key.encode()).hexdigest(),
                "candidate_api_key": key,
            }
        )
        with tempfile.TemporaryDirectory() as root:
            path = str(Path(root) / "candidate.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            os.chmod(path, 0o600)
            server.listen(1)
            received = []

            def exchange_once():
                connection, unused_address = server.accept()
                del unused_address
                with connection:
                    request = bytearray()
                    while True:
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        request.extend(chunk)
                    received.append(bytes(request))
                    value = json.loads(request)
                    connection.sendall(
                        canonical_json(
                            {
                                "schema_version": (
                                    "milk.baseten-candidate-key-delivery-ack.v1"
                                ),
                                "run_id": value["run_id"],
                                "provider": "baseten",
                                "team_name": value["team_name"],
                                "model_id": value["model_id"],
                                "key_name": value["key_name"],
                                "key_prefix": value["key_prefix"],
                                "candidate_key_sha256": value[
                                    "candidate_key_sha256"
                                ],
                                "payload_sha256": hashlib.sha256(
                                    request
                                ).hexdigest(),
                                "payload_bytes": len(request),
                                "gateway_release_id": (
                                    "11111111-1111-4111-8111-111111111111"
                                ),
                                "gateway_release_sha256": "f" * 64,
                                "verified_at": "2026-08-27T20:00:00Z",
                                "state": "installed",
                            }
                        )
                    )

            reader = threading.Thread(target=exchange_once)
            reader.start()
            with mock.patch.object(
                jobs_module,
                "CANDIDATE_KEY_SOCKET",
                path,
            ):
                response = jobs_module._candidate_key_socket_transaction(
                    path,
                    raw,
                    2,
                )
            reader.join(timeout=2)
            server.close()
            self.assertFalse(reader.is_alive())
            self.assertEqual(received, [raw])
            self.assertEqual(json.loads(response)["state"], "installed")

            regular = str(Path(root) / "candidate.file")
            Path(regular).write_bytes(b"")
            os.chmod(regular, 0o600)
            with (
                mock.patch.object(
                    jobs_module,
                    "CANDIDATE_KEY_SOCKET",
                    regular,
                ),
                self.assertRaisesRegex(ValueError, "owner-only socket"),
            ):
                jobs_module._candidate_key_socket_transaction(regular, raw)

    def test_control_store_requires_a_dedicated_bucket_and_access_key(self):
        evidence = types.SimpleNamespace(
            host="account.r2.cloudflarestorage.com",
            bucket="evidence",
            access_key_id="evidence-key",
        )
        _require_separate_control_store(
            evidence,
            types.SimpleNamespace(
                host=evidence.host,
                bucket="control",
                access_key_id="control-key",
            ),
        )
        for control in (
            types.SimpleNamespace(
                host="other.r2.cloudflarestorage.com",
                bucket=evidence.bucket,
                access_key_id="control-key",
            ),
            types.SimpleNamespace(
                host=evidence.host,
                bucket="control",
                access_key_id=evidence.access_key_id,
            ),
        ):
            with self.assertRaisesRegex(ValueError, "must be dedicated"):
                _require_separate_control_store(evidence, control)

    def test_cli_rejects_control_authority_aliases_before_provider_setup(self):
        base_environment = {
            **EVIDENCE_R2_ENVIRONMENT,
            **CREATE_AUTHORITY_R2_ENVIRONMENT,
            "MILK_CONTROL_R2_ACCOUNT_ID": "0" * 32,
            "MILK_CONTROL_R2_BUCKET": "control",
            "MILK_CONTROL_R2_ACCESS_KEY_ID": "control-key",
            "MILK_CONTROL_R2_SECRET_ACCESS_KEY": "control-secret",
        }
        aliases = (
            {"MILK_CONTROL_R2_BUCKET": "evidence"},
            {"MILK_CONTROL_R2_ACCESS_KEY_ID": "evidence-key"},
        )
        with tempfile.TemporaryDirectory() as root:
            for alias in aliases:
                environment = {**base_environment, **alias}
                name = hashlib.sha256(
                    json.dumps(alias, sort_keys=True).encode()
                ).hexdigest()
                case_root = Path(root) / name
                case_root.mkdir()
                evidence = LocalEvidenceStore(case_root / "evidence")
                evidence.host = "0" * 32 + ".r2.cloudflarestorage.com"
                evidence.bucket = "evidence"
                evidence.access_key_id = "evidence-key"
                token_path, unused_token, authority = lease_token_path(
                    evidence, case_root
                )
                del unused_token
                control = LocalEvidenceStore(case_root / "control")
                control.host = "0" * 32 + ".r2.cloudflarestorage.com"
                control.bucket = environment["MILK_CONTROL_R2_BUCKET"]
                control.access_key_id = environment[
                    "MILK_CONTROL_R2_ACCESS_KEY_ID"
                ]
                arguments = [
                    "--campaign-id",
                    CAMPAIGN,
                    "--project-id",
                    PROJECT,
                    "--baseten-team-name",
                    "milk",
                    "--candidate-key-socket",
                    jobs_module.CANDIDATE_KEY_SOCKET,
                    "--scope-prefix",
                    SCOPE_PREFIX,
                    "--lease-token",
                    str(token_path),
                ]
                with (
                    self.subTest(alias=alias),
                    mock.patch.dict(os.environ, environment, clear=True),
                    mock.patch.object(
                        jobs_module.R2EvidenceStore,
                        "from_environment",
                        side_effect=[evidence, authority, control],
                    ),
                    mock.patch.object(
                        jobs_module,
                        "BasetenTransport",
                        side_effect=AssertionError("provider setup reached"),
                    ),
                    mock.patch("builtins.print"),
                ):
                    self.assertEqual(jobs_module.main(arguments), 64)

    def test_cli_verifies_published_images_before_provider_setup(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = LocalEvidenceStore(Path(root) / "evidence")
            evidence.host = "0" * 32 + ".r2.cloudflarestorage.com"
            evidence.bucket = "evidence"
            evidence.access_key_id = "evidence-key"
            control = LocalEvidenceStore(Path(root) / "control")
            control.host = "0" * 32 + ".r2.cloudflarestorage.com"
            control.bucket = "control"
            control.access_key_id = "control-key"
            token_path, unused_token, authority = lease_token_path(evidence, root)
            del unused_token
            arguments = [
                "--campaign-id",
                CAMPAIGN,
                "--project-id",
                PROJECT,
                "--baseten-team-name",
                "milk",
                "--candidate-key-socket",
                jobs_module.CANDIDATE_KEY_SOCKET,
                "--scope-prefix",
                SCOPE_PREFIX,
                "--lease-token",
                str(token_path),
                "--student-train-image",
                "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "1" * 64,
                "--student-branch-image",
                "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "f" * 64,
                "--teacher-image",
                "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:" + "2" * 64,
                "--jobs-image",
                "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + "4" * 64,
                "--registry-secret",
                "milk-registry",
                "--config-secret",
                "milk-config",
                "--capture-store-access-key-secret",
                "capture-access",
                "--capture-store-secret-key-secret",
                "capture-secret",
                "--control-store-access-key-secret",
                "control-access",
                "--control-store-secret-key-secret",
                "control-secret",
                "--image-release-sha256",
                "f" * 64,
            ]
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        **EVIDENCE_R2_ENVIRONMENT,
                        **CREATE_AUTHORITY_R2_ENVIRONMENT,
                        "MILK_CONTROL_R2_ACCOUNT_ID": "0" * 32,
                        "MILK_CONTROL_R2_BUCKET": "control",
                        "MILK_CONTROL_R2_ACCESS_KEY_ID": "control-key",
                        "MILK_CONTROL_R2_SECRET_ACCESS_KEY": "control-secret",
                    },
                    clear=True,
                ),
                mock.patch.object(
                    jobs_module.R2EvidenceStore,
                    "from_environment",
                    side_effect=[evidence, authority, control],
                ),
                mock.patch.object(
                    jobs_module,
                    "BasetenTransport",
                    side_effect=AssertionError("provider setup reached"),
                ),
                mock.patch("builtins.print"),
            ):
                self.assertEqual(jobs_module.main(arguments), 64)

    def test_cli_requires_lease_token_before_store_or_provider_setup(self):
        arguments = [
            "--campaign-id",
            CAMPAIGN,
            "--project-id",
            PROJECT,
            "--baseten-team-name",
            "milk",
            "--candidate-key-socket",
            jobs_module.CANDIDATE_KEY_SOCKET,
            "--scope-prefix",
            SCOPE_PREFIX,
        ]
        with (
            mock.patch.object(
                jobs_module.R2EvidenceStore,
                "from_environment",
                side_effect=AssertionError("evidence store setup reached"),
            ),
            mock.patch.object(
                jobs_module,
                "BasetenTransport",
                side_effect=AssertionError("provider setup reached"),
            ),
            mock.patch("sys.stderr", io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            jobs_module.main(arguments)
        self.assertEqual(raised.exception.code, 2)

    def test_cli_create_authority_is_not_a_caller_flag(self):
        arguments = [
            "--campaign-id",
            CAMPAIGN,
            "--project-id",
            PROJECT,
            "--baseten-team-name",
            "milk",
            "--candidate-key-socket",
            jobs_module.CANDIDATE_KEY_SOCKET,
            "--scope-prefix",
            SCOPE_PREFIX,
            "--lease-token",
            "/does/not/matter.json",
            "--allow-provider-creates",
        ]
        with (
            mock.patch.object(
                jobs_module.R2EvidenceStore,
                "from_environment",
                side_effect=AssertionError("evidence store setup reached"),
            ),
            mock.patch("sys.stderr", io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            jobs_module.main(arguments)
        self.assertEqual(raised.exception.code, 2)

    def test_cli_requires_baseten_team_before_any_store_or_provider_setup(self):
        arguments = [
            "--campaign-id",
            CAMPAIGN,
            "--project-id",
            PROJECT,
            "--candidate-key-socket",
            jobs_module.CANDIDATE_KEY_SOCKET,
            "--scope-prefix",
            SCOPE_PREFIX,
            "--lease-token",
            "/does/not/matter.json",
        ]
        with (
            mock.patch.object(
                jobs_module.R2EvidenceStore,
                "from_environment",
                side_effect=AssertionError("evidence store setup reached"),
            ),
            mock.patch.object(
                jobs_module,
                "BasetenTransport",
                side_effect=AssertionError("provider setup reached"),
            ),
            mock.patch("sys.stderr", io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            jobs_module.main(arguments)
        self.assertEqual(raised.exception.code, 2)

    def test_cli_reconcile_token_rejects_create_authority_credentials(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = LocalEvidenceStore(Path(root) / "evidence")
            token_path, unused_token, unused_authority = lease_token_path(
                evidence,
                root,
                authorized=False,
            )
            del unused_token, unused_authority
            arguments = [
                "--campaign-id",
                CAMPAIGN,
                "--project-id",
                PROJECT,
                "--baseten-team-name",
                "milk",
                "--candidate-key-socket",
                jobs_module.CANDIDATE_KEY_SOCKET,
                "--scope-prefix",
                SCOPE_PREFIX,
                "--lease-token",
                str(token_path),
            ]
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        **EVIDENCE_R2_ENVIRONMENT,
                        **CREATE_AUTHORITY_R2_ENVIRONMENT,
                        "BASETEN_API_KEY": "baseten-test-key",
                    },
                    clear=True,
                ),
                mock.patch.object(
                    jobs_module.R2EvidenceStore,
                    "from_environment",
                    return_value=evidence,
                ) as stores,
                mock.patch.object(
                    jobs_module,
                    "BasetenTransport",
                    side_effect=AssertionError("provider setup reached"),
                ),
                mock.patch("sys.stderr", io.StringIO()),
            ):
                self.assertEqual(jobs_module.main(arguments), 64)
            self.assertEqual(stores.call_count, 1)

    def test_cli_requires_candidate_key_socket_before_store_setup(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = LocalEvidenceStore(Path(root) / "evidence")
            token_path, unused_token, authority = lease_token_path(evidence, root)
            del unused_token
            arguments = [
                "--campaign-id",
                CAMPAIGN,
                "--project-id",
                PROJECT,
                "--baseten-team-name",
                "milk",
                "--scope-prefix",
                SCOPE_PREFIX,
                "--lease-token",
                str(token_path),
            ]
            with (
                mock.patch.dict(
                    os.environ,
                    {**EVIDENCE_R2_ENVIRONMENT, **CREATE_AUTHORITY_R2_ENVIRONMENT},
                    clear=True,
                ),
                mock.patch.object(
                    jobs_module.R2EvidenceStore,
                    "from_environment",
                    side_effect=AssertionError("store setup reached"),
                ),
                mock.patch.object(
                    jobs_module,
                    "BasetenTransport",
                    side_effect=AssertionError("provider setup reached"),
                ),
                mock.patch("sys.stderr", io.StringIO()),
                self.assertRaises(SystemExit) as raised,
            ):
                jobs_module.main(arguments)
            self.assertEqual(raised.exception.code, 2)

    def test_cli_rejects_corrupt_expired_successor_and_claimed_tokens(self):
        base_arguments = [
            "--campaign-id",
            CAMPAIGN,
            "--project-id",
            PROJECT,
            "--baseten-team-name",
            "milk",
            "--candidate-key-socket",
            jobs_module.CANDIDATE_KEY_SOCKET,
            "--scope-prefix",
            SCOPE_PREFIX,
        ]
        with tempfile.TemporaryDirectory() as root:
            current = dt.datetime.now(UTC).replace(microsecond=0)
            cases = []

            corrupt_root = Path(root) / "corrupt"
            corrupt_root.mkdir()
            corrupt_store = LocalEvidenceStore(corrupt_root / "evidence")
            corrupt_path, unused_token, corrupt_authority = lease_token_path(
                corrupt_store,
                corrupt_root,
                now=current,
            )
            del unused_token
            corrupt_path.write_bytes(corrupt_path.read_bytes() + b" ")
            cases.append(
                ("corrupt", corrupt_store, corrupt_authority, corrupt_path)
            )

            expired_root = Path(root) / "expired"
            expired_root.mkdir()
            expired_store = LocalEvidenceStore(expired_root / "evidence")
            expired_path, unused_token, expired_authority = lease_token_path(
                expired_store,
                expired_root,
                now=current - dt.timedelta(minutes=16),
            )
            del unused_token
            cases.append(
                ("expired", expired_store, expired_authority, expired_path)
            )

            successor_root = Path(root) / "successor"
            successor_root.mkdir()
            successor_store = LocalEvidenceStore(successor_root / "evidence")
            successor_path, old_token, successor_authority = lease_token_path(
                successor_store,
                successor_root,
                now=current - dt.timedelta(seconds=5),
            )
            release_provider_lease(
                successor_store,
                CAMPAIGN,
                old_token["owner_id"],
                old_token["holder_id"],
                now=lambda: current - dt.timedelta(seconds=4),
            )
            acquire_provider_lease(
                successor_store,
                CAMPAIGN,
                "a" * 64,
                "b" * 64,
                now=lambda: current - dt.timedelta(seconds=3),
            )
            cases.append(
                ("successor", successor_store, successor_authority, successor_path)
            )

            claimed_root = Path(root) / "claimed"
            claimed_root.mkdir()
            claimed_store = LocalEvidenceStore(claimed_root / "evidence")
            claimed_path, claimed_token, claimed_authority = lease_token_path(
                claimed_store,
                claimed_root,
                now=current,
            )
            jobs_module.claim_provider_jobs_pass(
                claimed_store,
                claimed_authority,
                claimed_token,
                CAMPAIGN,
                SCOPE_PREFIX,
                claimed_token["evidence_store_identity_sha256"],
                claimed_token["create_authority_store_location_sha256"],
            )
            cases.append(
                ("claimed", claimed_store, claimed_authority, claimed_path)
            )

            for label, evidence, authority, token_path in cases:
                with (
                    self.subTest(label=label),
                    mock.patch.dict(
                        os.environ,
                        {**EVIDENCE_R2_ENVIRONMENT, **CREATE_AUTHORITY_R2_ENVIRONMENT},
                        clear=True,
                    ),
                    mock.patch.object(
                        jobs_module.R2EvidenceStore,
                        "from_environment",
                        side_effect=[evidence, authority],
                    ) as stores,
                    mock.patch.object(
                        jobs_module,
                        "BasetenTransport",
                        side_effect=AssertionError("provider setup reached"),
                    ),
                    mock.patch("builtins.print"),
                ):
                    self.assertEqual(
                        jobs_module.main(
                            base_arguments
                            + ["--lease-token", str(token_path)]
                        ),
                        64,
                    )
                    self.assertEqual(stores.call_count, 1 if label == "corrupt" else 2)

    def test_every_provider_request_runs_the_live_lease_guard(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            authorize(store)
            unused_path, token, authority = lease_token_path(store, root, now=NOW)
            del unused_path
            guard_calls = []
            transport = FakeTransport(lambda *_arguments: {})

            def guard():
                guard_calls.append("verified")
                jobs_module.verify_provider_lease_token(
                    store,
                    authority,
                    token,
                    CAMPAIGN,
                    SCOPE_PREFIX,
                    token["evidence_store_identity_sha256"],
                    token["create_authority_store_location_sha256"],
                    now=lambda: NOW,
                )

            jobs = BasetenJobs(
                store=store,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=transport,
                request_guard=guard,
                now=lambda: NOW,
            )
            jobs._request("GET", "/training_jobs/search")
            jobs._request("POST", "/training_projects/project/jobs", {})
            self.assertEqual(guard_calls, ["verified", "verified"])
            self.assertEqual(len(transport.calls), 2)
            release_provider_lease(
                store,
                CAMPAIGN,
                token["owner_id"],
                token["holder_id"],
                now=lambda: NOW + dt.timedelta(seconds=1),
            )
            with self.assertRaisesRegex(RuntimeError, "not active"):
                jobs._request("POST", "/training_projects/project/jobs", {})
            self.assertEqual(guard_calls, ["verified", "verified", "verified"])
            self.assertEqual(len(transport.calls), 2)

    def test_provider_secret_names_are_pairwise_distinct(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(Path(root) / "evidence")
            release = publish_release(store, Path(root) / "release")
            arguments = provider_arguments(release["release_sha256"])
            self.assertEqual(
                settings_from_arguments(arguments, store)["control_store_access_key_secret"],
                "control-access",
            )
            arguments.control_store_access_key_secret = "capture-access"
            with self.assertRaisesRegex(ValueError, "pairwise distinct"):
                settings_from_arguments(arguments, store)

    def test_provider_images_are_exact_private_milk_repositories(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(Path(root) / "evidence")
            release = publish_release(store, Path(root) / "release")
            arguments = provider_arguments(release["release_sha256"])
            self.assertEqual(
                settings_from_arguments(arguments, store)["student_branch_image"],
                arguments.student_branch_image,
            )
            for field, image in (
                (
                    "student_train_image",
                    "ghcr.io/another-org/milk-student-train@sha256:" + "1" * 64,
                ),
                (
                    "student_branch_image",
                    "ghcr.io/another-org/milk-student-branch@sha256:" + "f" * 64,
                ),
                (
                    "teacher_image",
                    "ghcr.io/milkinfrastructure/not-the-teacher@sha256:" + "2" * 64,
                ),
            ):
                rejected = provider_arguments(release["release_sha256"])
                setattr(rejected, field, image)
                with self.assertRaisesRegex(ValueError, "private Milk repository"):
                    settings_from_arguments(rejected, store)
            rejected = provider_arguments(release["release_sha256"])
            rejected.jobs_image = (
                "ghcr.io/milkinfrastructure/not-the-jobs@sha256:" + "4" * 64
            )
            with self.assertRaisesRegex(ValueError, "private Milk repository"):
                settings_from_arguments(rejected, store)
            for field, image in (
                (
                    "student_train_image",
                    "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "0" * 64,
                ),
                (
                    "jobs_image",
                    "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + "f" * 64,
                ),
            ):
                mismatched = provider_arguments(release["release_sha256"])
                setattr(mismatched, field, image)
                with self.assertRaisesRegex(ValueError, "private build admission"):
                    settings_from_arguments(mismatched, store)
            same_digest = provider_arguments(release["release_sha256"])
            same_digest.student_train_image = (
                "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "f" * 64
            )
            with self.assertRaisesRegex(ValueError, "distinct digests"):
                settings_from_arguments(same_digest, store)

    def test_private_image_release_publish_replays_and_loads_without_local_files(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            release_dir = Path(root) / "release"
            evidence_dir = Path(root) / "evidence"
            store = LocalEvidenceStore(evidence_dir)
            release = load_local_private_image_release(
                private_image_release(release_dir)
            )
            receipt = publish_private_image_release(store, release)
            self.assertEqual(receipt["state"], "published")
            self.assertEqual(publish_private_image_release(store, release), receipt)
            release_sha = release["release_sha256"]
            self.assertEqual(
                store.get(f"image-admissions/v1/releases/{release_sha}.json"),
                (release_dir / "release.json").read_bytes(),
            )
            for artifact, admission_sha in receipt[
                "artifact_admission_sha256s"
            ].items():
                self.assertEqual(
                    store.get(
                        f"image-admissions/v1/artifacts/{admission_sha}.json"
                    ),
                    (release_dir / artifact / "admission.json").read_bytes(),
                )
            release_dir.rename(root / "detached-release")
            loaded = load_published_private_image_release(store, release_sha)
            self.assertEqual(loaded, release)
            arguments = provider_arguments(release_sha)
            self.assertFalse(hasattr(arguments, "image_release_evidence_dir"))
            self.assertEqual(
                settings_from_arguments(arguments, store)["jobs_image"],
                arguments.jobs_image,
            )

    def test_control_outbox_accepts_all_operations_and_restart_never_replays(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            control = LocalEvidenceStore(root / "control")
            launches = [
                control_launch(control, "teacher_run", 1),
                control_launch(control, "student_train_merge", 2),
                control_launch(control, "student_fanout", 3),
            ]
            evidence = LocalEvidenceStore(root / "evidence")
            authorize(evidence)
            release = publish_release(evidence, root / "release")
            settings = settings_from_arguments(
                provider_arguments(release["release_sha256"]),
                evidence,
            )
            transport = FakeTransport(
                lambda _method, _path, body, _query: provider_job(
                    body["training_job"]["name"],
                    "job_" + hashlib.sha256(
                        body["training_job"]["name"].encode()
                    ).hexdigest()[:16],
                )
            )
            service = BasetenJobs(
                store=evidence,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=transport,
                now=lambda: NOW,
            )
            first = dispatch_outboxes(
                service,
                control_store=control,
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
            )
            self.assertEqual(first["frontier_scan_limit"], GPU_LAUNCH_SCAN_LIMIT)
            self.assertEqual(first["verified"], 3)
            self.assertEqual(first["dispatched"], 3)
            self.assertEqual(len(transport.calls), 5)
            for launch in launches:
                acceptance = json.loads(
                    evidence.get(
                        f"claims/v1/{launch['claim_sha256']}.json"
                    )
                )
                self.assertEqual(
                    acceptance["schema_version"],
                    "milk.gateway-launch-acceptance.v1",
                )
                self.assertEqual(
                    acceptance["launch_source"]["claim_object_key"],
                    launch["claim_key"],
                )
            second = dispatch_outboxes(
                service,
                control_store=control,
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
            )
            self.assertEqual(
                [item["state"] for item in second["results"]],
                ["reconcile_only"] * 3,
            )
            self.assertEqual(len(transport.calls), 5)

    def test_all_frontier_chains_verify_before_any_acceptance_or_provider_call(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            control = LocalEvidenceStore(root / "control")
            control_launch(control, "teacher_run", 1)
            control_launch(
                control,
                "student_train_merge",
                2,
                claim_mutator=lambda claim: claim.__setitem__("unknown", True),
            )
            evidence = LocalEvidenceStore(root / "evidence")
            authorize(evidence)
            release = publish_release(evidence, root / "release")
            settings = settings_from_arguments(
                provider_arguments(release["release_sha256"]),
                evidence,
            )
            transport = FakeTransport(
                lambda *_arguments: self.fail("provider called before verification")
            )
            service = BasetenJobs(
                store=evidence,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=transport,
                now=lambda: NOW,
            )
            with self.assertRaisesRegex(ValueError, "claim is invalid"):
                dispatch_outboxes(
                    service,
                    control_store=control,
                    scope_prefix=SCOPE_PREFIX,
                    settings=settings,
                )
            self.assertEqual(transport.calls, [])
            self.assertEqual(evidence.list("claims/v1"), [])

    def test_control_frontier_rejects_hash_mismatch_and_nineteenth_entry(self):
        class OverrideGet:
            def __init__(self, store, key, raw):
                self.store = store
                self.key = key
                self.raw = raw

            def list(self, *arguments, **keywords):
                return self.store.list(*arguments, **keywords)

            def get(self, key):
                return self.raw if key == self.key else self.store.get(key)

        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            control = LocalEvidenceStore(root / "control-hash")
            launch = control_launch(control, "teacher_run", 1)
            evidence = LocalEvidenceStore(root / "evidence-hash")
            authorize(evidence)
            service = BasetenJobs(
                store=evidence,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=FakeTransport(
                    lambda *_arguments: self.fail("provider called")
                ),
                now=lambda: NOW,
            )
            release = publish_release(evidence, root / "release-hash")
            settings = settings_from_arguments(
                provider_arguments(release["release_sha256"]),
                evidence,
            )
            with self.assertRaisesRegex(ValueError, "claim hash differs"):
                dispatch_outboxes(
                    service,
                    control_store=OverrideGet(
                        control,
                        launch["claim_key"],
                        launch["claim_raw"] + b" ",
                    ),
                    scope_prefix=SCOPE_PREFIX,
                    settings=settings,
                )
            self.assertEqual(service.transport.calls, [])

            crowded = LocalEvidenceStore(root / "control-crowded")
            for nonce in range(1, GPU_LAUNCH_SCAN_LIMIT + 1):
                control_launch(crowded, "teacher_run", nonce)
            with self.assertRaisesRegex(ValueError, "hard active bound"):
                dispatch_outboxes(
                    service,
                    control_store=crowded,
                    scope_prefix=SCOPE_PREFIX,
                    settings=settings,
                )
            self.assertEqual(service.transport.calls, [])

    def test_expired_frontier_is_not_discovered(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            control = LocalEvidenceStore(root / "control")
            control_launch(
                control,
                "teacher_run",
                1,
                created=NOW - dt.timedelta(hours=2),
                expires=NOW - dt.timedelta(hours=1),
            )
            evidence = LocalEvidenceStore(root / "evidence")
            authorize(evidence)
            release = publish_release(evidence, root / "release")
            service = BasetenJobs(
                store=evidence,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=FakeTransport(
                    lambda *_arguments: self.fail("expired launch reached provider")
                ),
                now=lambda: NOW,
            )
            summary = dispatch_outboxes(
                service,
                control_store=control,
                scope_prefix=SCOPE_PREFIX,
                settings=settings_from_arguments(
                    provider_arguments(release["release_sha256"]),
                    evidence,
                ),
            )
            self.assertEqual(summary["verified"], 0)
            self.assertEqual(summary["state"], "hold")
            self.assertEqual(service.transport.calls, [])

    def test_jobs_has_no_gateway_process_or_capture_authority_surface(self):
        source = Path(jobs_module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "dispatch_ticks",
            "subprocess",
            "--gateway-bin",
            "--tick-timeout-seconds",
            "--max-dispatches",
            "MILK_CAPTURE_STORE_",
            "dragontales-gateway",
            "--allow-provider-creates",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn('parser.add_argument("--scope-prefix", required=True)', source)
        self.assertIn(
            'R2EvidenceStore.from_environment(prefix="MILK_CONTROL_R2_")',
            source,
        )

    def test_transport_is_one_bounded_api_key_request_with_exact_query(self):
        requests = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_unused):
                return False

            def getcode(self):
                return 200

            def read(self, _limit):
                return b"{}\n"

        class Opener:
            def open(self, request, timeout):
                requests.append((request, timeout))
                return Response()

        transport = BasetenTransport("secret", timeout_seconds=17, opener=Opener())
        self.assertEqual(
            transport.request(
                "GET",
                "/training_projects/project/jobs/job/logs",
                query={"limit": 1000, "direction": "asc"},
            ),
            b"{}\n",
        )
        request, timeout = requests[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(parsed.scheme + "://" + parsed.netloc, "https://api.baseten.co")
        self.assertEqual(parsed.path, "/v1/training_projects/project/jobs/job/logs")
        self.assertEqual(parsed.query, "direction=asc&limit=1000")
        self.assertEqual(request.get_header("Authorization"), "Api-Key secret")
        self.assertEqual(timeout, 17)

    def jobs(self, root, transport, now=lambda: NOW):
        store = LocalEvidenceStore(root)
        authorize(store)
        return BasetenJobs(
            store=store,
            campaign_id=CAMPAIGN,
            project_id=PROJECT,
            transport=transport,
            now=now,
        )

    def prepare_without_intent(self, jobs, character):
        source = workload(character)
        run_id, definition, unused_entries = jobs._definition(
            source, [entry("milk-no-intent")]
        )
        del unused_entries
        evidence = JobEvidence(jobs.store, CAMPAIGN, run_id)
        evidence.json("definition.json", definition)
        claim_sha256 = source["launch_source"]["claim_sha256"]
        jobs.store.create(
            f"claims/v1/{claim_sha256}.json",
            canonical_json(
                launch_acceptance(source, run_id, definition, evidence)
            ),
            "application/json",
        )
        jobs.store.create(
            f"campaigns/v1/{CAMPAIGN}/job-index/{run_id}.json",
            (
                json.dumps(
                    {
                        "schema_version": "milk.baseten-launch-group-index.v1",
                        "campaign_id": CAMPAIGN,
                        "run_id": run_id,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode(),
            "application/json",
        )
        preparation_deadline = jobs.now() + dt.timedelta(seconds=60)
        provider_deadline = jobs.now() + dt.timedelta(
            seconds=definition["executions"][0]["max_wall_seconds"]
        )
        preparation = {
            "schema_version": "milk.baseten-launch-group-preparation.v1",
            "campaign_id": CAMPAIGN,
            "run_id": run_id,
            "requested_at": jobs.now().isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "preparation_deadline": preparation_deadline.isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z"),
            "provider_deadline_epoch_seconds": int(provider_deadline.timestamp()),
            "executions": [
                {
                    "ordinal": 0,
                    "deadline": provider_deadline.isoformat(
                        timespec="microseconds"
                    ).replace("+00:00", "Z"),
                }
            ],
            "state": "preparing",
        }
        preparation_sha256 = hashlib.sha256(canonical_json(preparation)).hexdigest()
        self.assertEqual(
            jobs.budget.prepare(
                run_id,
                preparation_sha256,
                int(preparation_deadline.timestamp()),
                int(provider_deadline.timestamp()),
            ),
            "created",
        )
        JobEvidence(jobs.store, CAMPAIGN, run_id).json(
            "preparation.json", preparation
        )
        return run_id, definition, preparation_sha256

    def test_single_create_is_budgeted_once_and_never_replayed(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            authorize(store)

            def handler(method, path, body, query):
                self.assertEqual((method, query), ("POST", None))
                head = json.loads(
                    store.get(
                        f"state/v1/campaigns/{CAMPAIGN}/budget-head.json"
                    )
                )
                self.assertEqual(
                    head["reserved_microusd"],
                    2 * RESERVATION_RATE_MICROUSD_PER_MINUTE,
                )
                intents = store.list(f"campaigns/v1/{CAMPAIGN}/jobs")
                self.assertEqual(len([key for key in intents if "/create-intents/" in key]), 1)
                return provider_job(body["training_job"]["name"], "job_1")

            transport = FakeTransport(handler)
            jobs = BasetenJobs(
                store=store,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=transport,
                now=lambda: NOW,
            )
            self.assertEqual(
                jobs.budget.provider_identity,
                baseten_provider_identity(PROJECT),
            )
            first = jobs.launch(workload("a"), [entry("milk-one")])
            self.assertEqual(first["state"], "created")
            reservation_keys = store.list(
                f"campaigns/v1/{CAMPAIGN}/reservations"
            )
            self.assertEqual(len(reservation_keys), 1)
            self.assertEqual(
                json.loads(store.get(reservation_keys[0]))["provider_identity"],
                baseten_provider_identity(PROJECT),
            )
            second = jobs.launch(workload("a"), [entry("milk-one")])
            self.assertEqual(second["state"], "reconcile_only")
            self.assertEqual(len(transport.calls), 1)

    def test_fanout_has_one_atomic_reservation_and_all_intents_precede_posts(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            authorize(store)
            first = threading.Event()

            def handler(method, path, body, query):
                del method, path, query
                keys = store.list(f"campaigns/v1/{CAMPAIGN}/jobs")
                self.assertEqual(len([key for key in keys if "/create-intents/" in key]), 3)
                head = json.loads(
                    store.get(
                        f"state/v1/campaigns/{CAMPAIGN}/budget-head.json"
                    )
                )
                self.assertEqual(
                    head["reserved_microusd"],
                    3 * 2 * RESERVATION_RATE_MICROUSD_PER_MINUTE,
                )
                first.set()
                name = body["training_job"]["name"]
                return provider_job(name, "job_" + name[-1])

            jobs = self.jobs(root, FakeTransport(handler))
            result = jobs.launch(
                workload("b", "student_fanout"),
                [entry("milk-branch-1"), entry("milk-branch-2"), entry("milk-branch-3")],
            )
            self.assertTrue(first.is_set())
            self.assertEqual(result["state"], "created")
            self.assertEqual(len(result["executions"]), 3)

    def test_baseten_reconcile_ignores_modal_outstanding_work(self):
        with tempfile.TemporaryDirectory() as root:
            service = self.jobs(
                root,
                FakeTransport(
                    lambda *_arguments: self.fail("provider must not be called")
                ),
            )
            modal_budget = CampaignBudget(
                service.store,
                CAMPAIGN,
                modal_provider_identity(
                    MODAL_WORKSPACE,
                    MODAL_ENVIRONMENT,
                    MODAL_APP,
                ),
                now=lambda: NOW,
            )
            modal_budget.initialize()
            baseten_run = "a" * 64
            modal_run = "b" * 64
            for budget, run_id in (
                (service.budget, baseten_run),
                (modal_budget, modal_run),
            ):
                budget.prepare(
                    run_id,
                    hashlib.sha256(run_id.encode()).hexdigest(),
                    int((NOW + dt.timedelta(minutes=1)).timestamp()),
                    int((NOW + dt.timedelta(hours=1)).timestamp()),
                )
            visited = []

            def reconcile(run_id):
                visited.append(run_id)
                return {
                    "schema_version": "milk.test-reconciliation.v1",
                    "run_id": run_id,
                    "evidence_complete": True,
                }

            service.reconcile = reconcile
            summary = service.reconcile_all(1)
            self.assertEqual(visited, [baseten_run])
            self.assertEqual(summary["processed"], 1)
            self.assertFalse(summary["backlog"])
            outstanding = service.budget.status()["outstanding"]
            self.assertEqual(
                [
                    run_id
                    for run_id, item in outstanding.items()
                    if item["provider_identity"]
                    == modal_provider_identity(
                        MODAL_WORKSPACE,
                        MODAL_ENVIRONMENT,
                        MODAL_APP,
                    )
                ],
                [modal_run],
            )
            cursor = json.loads(
                service.store.get(
                    f"state/v1/campaigns/{CAMPAIGN}/providers/"
                    "baseten/reconcile-cursor.json"
                )
            )
            self.assertEqual(
                cursor["provider_identity"],
                baseten_provider_identity(PROJECT),
            )

    def test_active_campaign_authority_blocks_budget_reset(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            authorize(store)

            def handler(method, path, body, query):
                del method, path, query
                return provider_job(body["training_job"]["name"], "job_1")

            transport = FakeTransport(handler)
            first = BasetenJobs(
                store=store,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=transport,
                now=lambda: NOW,
            )
            other_campaign = "d" * 64
            first.launch(workload("9"), [entry("milk-global")])
            with self.assertRaisesRegex(ValueError, "different campaign"):
                BasetenJobs(
                    store=store,
                    campaign_id=other_campaign,
                    project_id=PROJECT,
                    transport=transport,
                    now=lambda: NOW,
                )
            self.assertEqual(len(transport.calls), 1)

    def test_ambiguous_create_is_resolved_read_only_and_settled(self):
        with tempfile.TemporaryDirectory() as root:
            calls = {"create": 0, "name": None}
            current = [NOW]
            oom_message = "torch.OutOfMemoryError: CUDA out of memory; prompt data"

            def handler(method, path, body, query):
                if path == f"/training_projects/{PROJECT}/jobs" and method == "POST":
                    calls["create"] += 1
                    calls["name"] = body["training_job"]["name"]
                    return TimeoutError("response lost")
                if path == "/training_jobs/search":
                    job = provider_job(
                        calls["name"], "job_ambiguous", "TRAINING_JOB_COMPLETED", seconds=61
                    )["training_job"]
                    return {"training_jobs": [job]}
                if path.endswith("/job_ambiguous") and method == "GET":
                    return provider_get(
                        calls["name"], "job_ambiguous", "TRAINING_JOB_COMPLETED", seconds=61
                    )
                if path.endswith("/logs"):
                    return {
                        "logs": [
                            {
                                "timestamp": "2026-08-27T20:00:01Z",
                                "message": oom_message,
                                "replica": "worker-0",
                                "request_id": "request-1",
                                "level": "INFO",
                            }
                        ]
                    }
                if path.endswith("/metrics"):
                    self.assertEqual(
                        query,
                        {
                            "start_epoch_millis": int(NOW.timestamp() * 1000),
                            "end_epoch_millis": int(
                                (NOW + dt.timedelta(seconds=121)).timestamp()
                                * 1000
                            ),
                            "step_seconds": 60,
                        },
                    )
                    return provider_metrics(
                        calls["name"],
                        "job_ambiguous",
                        "TRAINING_JOB_COMPLETED",
                        seconds=61,
                    )
                self.fail((method, path, body))

            jobs = self.jobs(root, FakeTransport(handler), now=lambda: current[0])
            launch = jobs.launch(
                workload("d"),
                [entry("milk-ambiguous")],
            )
            self.assertEqual(launch["state"], "hold")
            run_id = launch["run_id"]
            self.assertEqual(
                jobs.launch(
                    workload("d"),
                    [entry("milk-ambiguous")],
                )["state"],
                "reconcile_only",
            )
            pending = jobs.reconcile(run_id)
            self.assertEqual(pending["state"], "terminal_evidence_pending")
            current[0] = NOW + dt.timedelta(seconds=122)
            terminal = jobs.reconcile(run_id)
            self.assertEqual(terminal["state"], "terminal")
            self.assertEqual(terminal["accounted_minutes"], 2)
            self.assertEqual(terminal["oom_classifier_sha256"], OOM_CLASSIFIER_SHA256)
            self.assertEqual(terminal["oom_matched_record_count"], 1)
            self.assertTrue(terminal["oom_source_complete"])
            self.assertEqual(calls["create"], 1)
            stored = b"".join(
                LocalEvidenceStore(root).get(key)
                for key in LocalEvidenceStore(root).list(
                    f"campaigns/v1/{CAMPAIGN}/jobs/{run_id}/logs"
                )
            )
            self.assertNotIn(b"prompt data", stored)
            self.assertNotIn(b"gpu_utilization", stored)
            self.assertEqual(jobs.budget.status()["reserved_microusd"], 0)

            prefix = f"campaigns/v1/{CAMPAIGN}/jobs/{run_id}"
            summary_key = f"{prefix}/log-collections/summary.json"
            summary_raw, summary_etag = jobs.store.get_versioned(summary_key)
            tampered_summary = json.loads(summary_raw)
            tampered_summary["oom_classifier_sha256"] = "0" * 64
            tampered_summary_raw = canonical_json(tampered_summary)
            self.assertTrue(
                jobs.store.replace(summary_key, tampered_summary_raw, summary_etag)
            )
            terminal_key = f"{prefix}/terminal.json"
            terminal_raw, terminal_etag = jobs.store.get_versioned(terminal_key)
            tampered_terminal = json.loads(terminal_raw)
            tampered_terminal["log_collection_sha256"] = hashlib.sha256(
                tampered_summary_raw
            ).hexdigest()
            self.assertTrue(
                jobs.store.replace(
                    terminal_key,
                    canonical_json(tampered_terminal),
                    terminal_etag,
                )
            )
            with self.assertRaisesRegex(ValueError, "log evidence differs"):
                jobs.reconcile(run_id)

    def test_status_failure_after_deadline_still_attempts_stop_once(self):
        with tempfile.TemporaryDirectory() as root:
            current = [NOW]
            stop_calls = []
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if path == f"/training_projects/{PROJECT}/jobs":
                    created_name[0] = body["training_job"]["name"]
                    return provider_job(created_name[0], "job_deadline")
                if path.endswith("/job_deadline") and method == "GET":
                    return TimeoutError("status unavailable")
                if path.endswith("/stop"):
                    stop_calls.append(path)
                    return provider_job(
                        created_name[0], "job_deadline", "TRAINING_JOB_STOPPED", seconds=121
                    )
                self.fail((method, path, body))

            jobs = self.jobs(root, FakeTransport(handler), now=lambda: current[0])
            launch = jobs.launch(
                workload("e"),
                [entry("milk-deadline")],
            )
            current[0] = NOW + dt.timedelta(seconds=121)
            observed = jobs.reconcile(launch["run_id"])
            self.assertEqual(observed["state"], "active_or_ambiguous")
            self.assertEqual(observed["executions"][0]["stop_state"], "stopped")
            jobs.reconcile(launch["run_id"])
            self.assertEqual(len(stop_calls), 1)

    def test_missing_provider_intent_is_known_not_launched_and_costs_zero(self):
        with tempfile.TemporaryDirectory() as root:
            transport = FakeTransport(lambda *_arguments: self.fail("provider called"))
            jobs = self.jobs(root, transport)
            run_id, definition, preparation_sha256 = self.prepare_without_intent(
                jobs, "f"
            )
            amount = definition["executions"][0]["reserved_microusd"]
            jobs.budget.reserve(run_id, amount, preparation_sha256)
            observed = jobs.reconcile(run_id)
            self.assertEqual(observed["state"], "preparing")
            self.assertEqual(observed["executions"][0]["state"], "preparing")
            self.assertIn(run_id, jobs.budget.status()["open"])
            self.assertEqual(transport.calls, [])

    def test_missing_intent_without_reservation_is_known_not_authorized(self):
        with tempfile.TemporaryDirectory() as root:
            transport = FakeTransport(lambda *_arguments: self.fail("provider called"))
            current = [NOW]
            jobs = self.jobs(root, transport, now=lambda: current[0])
            run_id, unused_definition, unused_preparation = self.prepare_without_intent(
                jobs, "8"
            )
            del unused_definition, unused_preparation
            current[0] = NOW + dt.timedelta(seconds=61)
            terminal = jobs.reconcile(run_id)
            self.assertEqual(terminal["accounted_microusd"], 0)
            self.assertEqual(terminal["executions"][0]["state"], "not_authorized")
            self.assertEqual(transport.calls, [])


if __name__ == "__main__":
    unittest.main()
