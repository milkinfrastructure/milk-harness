from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re

from deploy.baseten import adapter
from milk_harness.evidence import (
    HEX64,
    MAX_OBJECT_BYTES,
    canonical_json,
)


PRIVATE_IMAGE_REPOSITORIES = {
    "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
    "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
    "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
    "planner": "ghcr.io/milkinfrastructure/milk-planner",
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
}
STUDENT_TRAIN_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["student-train"]
STUDENT_BRANCH_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["student-branch"]
TEACHER_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["teacher-gpt-oss"]
JOBS_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["jobs"]
RELEASE_PREFIX = "image-admissions/v1/releases"
ARTIFACT_PREFIX = "image-admissions/v1/artifacts"


def _strict_object(raw):
    if not isinstance(raw, (bytes, bytearray)) or not 1 <= len(raw) <= MAX_OBJECT_BYTES:
        raise ValueError("private image admission JSON must be bounded bytes")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("private image admission JSON contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict) or canonical_json(value) != bytes(raw):
        raise ValueError("private image admission JSON must be a canonical object")
    return value


def _utc(value, label):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC RFC3339")
    return parsed


def _validate_release(release_raw, read_admission, expected_release_sha256=None):
    if not isinstance(release_raw, (bytes, bytearray)):
        raise ValueError("private image release authority is invalid")
    if (
        expected_release_sha256 is not None
        and (
            not isinstance(expected_release_sha256, str)
            or HEX64.fullmatch(expected_release_sha256) is None
        )
    ):
        raise ValueError("private image release SHA-256 is invalid")
    release_sha256 = hashlib.sha256(release_raw).hexdigest()
    if expected_release_sha256 is not None and release_sha256 != expected_release_sha256:
        raise ValueError("private image release SHA-256 differs from its authority key")
    release = _strict_object(release_raw)
    images = release.get("images")
    if (
        set(release)
        != {
            "schema_version",
            "source_commit",
            "source_date_epoch",
            "source_repository",
            "gateway_image_reference",
            "buildkit_image_reference",
            "dockerfile_frontend_reference",
            "build_authority",
            "platform",
            "images",
            "started_at",
            "completed_at",
        }
        or release.get("schema_version") != "milk.private-harness-release.v2"
        or re.fullmatch(r"[0-9a-f]{40}", release.get("source_commit", "")) is None
        or type(release.get("source_date_epoch")) is not int
        or release["source_date_epoch"] <= 0
        or release.get("source_repository")
        != "https://github.com/milkinfrastructure/milk-harness"
        or release.get("build_authority") != "local-socket"
        or release.get("platform") != "linux/amd64"
        or not isinstance(images, list)
        or len(images) != len(PRIVATE_IMAGE_REPOSITORIES)
    ):
        raise ValueError("private image release receipt is invalid")
    started = _utc(release.get("started_at"), "image release started_at")
    completed = _utc(release.get("completed_at"), "image release completed_at")
    if completed < started:
        raise ValueError("private image release timestamps are invalid")
    gateway = adapter.immutable_ghcr_image(release.get("gateway_image_reference"))
    if gateway.rpartition("@sha256:")[0] != "ghcr.io/milkinfrastructure/milk-gateway":
        raise ValueError("private image release gateway is invalid")
    buildkit = adapter.immutable_image(release.get("buildkit_image_reference"))
    frontend = adapter.immutable_image(release.get("dockerfile_frontend_reference"))
    admissions = {}
    for expected_artifact, release_item in zip(PRIVATE_IMAGE_REPOSITORIES, images):
        if (
            not isinstance(release_item, dict)
            or set(release_item) != {"admission_sha256", "artifact", "image_reference"}
            or release_item.get("artifact") != expected_artifact
            or HEX64.fullmatch(release_item.get("admission_sha256", "")) is None
        ):
            raise ValueError("private image release item is invalid")
        admission_raw = read_admission(
            expected_artifact,
            release_item["admission_sha256"],
        )
        admission = _strict_object(admission_raw)
        repository = PRIVATE_IMAGE_REPOSITORIES[expected_artifact]
        image_reference = adapter.immutable_ghcr_image(release_item.get("image_reference"))
        if (
            hashlib.sha256(admission_raw).hexdigest() != release_item["admission_sha256"]
            or image_reference.rpartition("@sha256:")[0] != repository
            or admission.get("image_reference") != image_reference
            or set(admission)
            != {
                "schema_version",
                "artifact",
                "repository",
                "image_reference",
                "source_repository",
                "source_commit",
                "source_context_method",
                "source_context_sha256",
                "gateway_image_reference",
                "index_sha256",
                "amd64_manifest_sha256",
                "config_sha256",
                "attestation_manifest_sha256",
                "attestations",
                "platform",
                "visibility",
                "builder",
            }
            or admission.get("schema_version") != "milk.private-image-admission.v1"
            or admission.get("artifact") != expected_artifact
            or admission.get("repository") != repository
            or admission.get("source_repository") != release["source_repository"]
            or admission.get("source_commit") != release["source_commit"]
            or admission.get("source_context_method") != "git-archive-tar-v1"
            or HEX64.fullmatch(admission.get("source_context_sha256", "")) is None
            or admission.get("index_sha256") != image_reference.rpartition("@sha256:")[2]
            or any(
                HEX64.fullmatch(admission.get(field, "")) is None
                for field in (
                    "amd64_manifest_sha256",
                    "config_sha256",
                    "attestation_manifest_sha256",
                )
            )
            or admission.get("platform") != "linux/amd64"
            or admission.get("visibility") != "private"
        ):
            raise ValueError("private image admission is invalid")
        expected_gateway = None if expected_artifact in {"planner", "jobs"} else gateway
        if admission.get("gateway_image_reference") != expected_gateway:
            raise ValueError("private image gateway binding is invalid")
        attestations = admission.get("attestations")
        if (
            not isinstance(attestations, list)
            or [item.get("predicate_type") for item in attestations]
            != ["https://slsa.dev/provenance/v1", "https://spdx.dev/Document"]
            or any(
                not isinstance(item, dict)
                or set(item) != {"layer_sha256", "predicate_type"}
                or HEX64.fullmatch(item.get("layer_sha256", "")) is None
                for item in attestations
            )
        ):
            raise ValueError("private image attestations are invalid")
        if admission.get("builder") != {
            "authority": "local-socket",
            "driver": "docker-container",
            "endpoint_kind": "local-socket",
            "buildkit_image_reference": buildkit,
            "buildkit_version": "v0.23.2",
            "dockerfile_frontend_reference": frontend,
            "provenance_mode": "max",
            "provenance_version": "v1",
            "sbom": True,
        }:
            raise ValueError("private image builder admission is invalid")
        admissions[expected_artifact] = {
            "image_reference": image_reference,
            "admission_sha256": release_item["admission_sha256"],
            "raw": bytes(admission_raw),
        }
    return {
        "release_sha256": release_sha256,
        "release_raw": bytes(release_raw),
        "images": admissions,
    }


def load_local_private_image_release(directory):
    if not isinstance(directory, str) or not os.path.isabs(directory):
        raise ValueError("private image release evidence path must be absolute")
    root_path = Path(directory)
    if root_path.is_symlink() or not root_path.is_dir():
        raise ValueError("private image release evidence directory is invalid")
    root = root_path.resolve()

    def read(relative):
        path = root.joinpath(*relative.split("/"))
        if path.is_symlink() or not path.is_file() or root not in path.resolve().parents:
            raise ValueError("private image admission file is invalid")
        with path.open("rb") as source:
            return source.read(MAX_OBJECT_BYTES + 1)

    release_raw = read("release.json")
    return _validate_release(
        release_raw,
        lambda artifact, unused_sha256: read(f"{artifact}/admission.json"),
    )

def load_published_private_image_release(store, release_sha256):
    if not isinstance(release_sha256, str) or HEX64.fullmatch(release_sha256) is None:
        raise ValueError("private image release SHA-256 is invalid")
    release_raw = store.get(f"{RELEASE_PREFIX}/{release_sha256}.json")
    return _validate_release(
        release_raw,
        lambda unused_artifact, admission_sha256: store.get(
            f"{ARTIFACT_PREFIX}/{admission_sha256}.json"
        ),
        release_sha256,
    )
