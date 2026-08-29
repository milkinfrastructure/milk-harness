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
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
}
RELEASE_IMAGE_REPOSITORIES = {
    "milk.private-harness-release.v3": {
        "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
        "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
        "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
        "planner": "ghcr.io/milkinfrastructure/milk-planner",
        "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
    },
    "milk.private-harness-release.v4": PRIVATE_IMAGE_REPOSITORIES,
    "milk.private-harness-release.v5": PRIVATE_IMAGE_REPOSITORIES,
}
STUDENT_TRAIN_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["student-train"]
STUDENT_BRANCH_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["student-branch"]
TEACHER_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["teacher-gpt-oss"]
JOBS_IMAGE_REPOSITORY = PRIVATE_IMAGE_REPOSITORIES["jobs"]
GATEWAY_IMAGE_REPOSITORY = "ghcr.io/milkinfrastructure/milk-carton"
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


def _validate_local_ops_log(
    ops_log_raw,
    build_log_raw,
    artifact,
    expected_ops_log_sha256,
):
    ops_log = _strict_object(ops_log_raw)
    build_log = _strict_object(build_log_raw)
    if (
        hashlib.sha256(ops_log_raw).hexdigest() != expected_ops_log_sha256
        or ops_log
        != {
            "schema_version": "milk.private-ops-log-reference.v1",
            "authority": "private-release-evidence",
            "reference": "build-log.json",
            "receipt_sha256": hashlib.sha256(build_log_raw).hexdigest(),
            "immutable": True,
            "content_retained": False,
        }
        or set(build_log)
        != {
            "schema_version",
            "artifact",
            "exit_code",
            "started_at",
            "completed_at",
            "sha256",
            "bytes",
            "content_retained",
        }
        or build_log.get("schema_version") != "milk.content-free-build-log.v1"
        or build_log.get("artifact") != artifact
        or build_log.get("exit_code") != 0
        or HEX64.fullmatch(build_log.get("sha256", "")) is None
        or type(build_log.get("bytes")) is not int
        or build_log["bytes"] < 0
        or build_log.get("content_retained") is not False
    ):
        raise ValueError("private image ops-log reference is invalid")
    started = _utc(build_log.get("started_at"), "image build started_at")
    completed = _utc(build_log.get("completed_at"), "image build completed_at")
    if completed < started:
        raise ValueError("private image build log timestamps are invalid")
    return ops_log, started, completed


def _validate_release(
    release_raw,
    read_admission,
    expected_release_sha256=None,
    read_ops_log=None,
):
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
    schema_version = release.get("schema_version")
    repositories = RELEASE_IMAGE_REPOSITORIES.get(schema_version)
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
        or repositories is None
        or re.fullmatch(r"[0-9a-f]{40}", release.get("source_commit", "")) is None
        or type(release.get("source_date_epoch")) is not int
        or release["source_date_epoch"] <= 0
        or release.get("source_repository")
        != "https://github.com/milkinfrastructure/milk-harness"
        or release.get("build_authority") != "local-socket"
        or release.get("platform") != "linux/amd64"
        or not isinstance(images, list)
        or len(images) != len(repositories)
    ):
        raise ValueError("private image release receipt is invalid")
    started = _utc(release.get("started_at"), "image release started_at")
    completed = _utc(release.get("completed_at"), "image release completed_at")
    if completed < started:
        raise ValueError("private image release timestamps are invalid")
    gateway = adapter.immutable_ghcr_image(release.get("gateway_image_reference"))
    if gateway.rpartition("@sha256:")[0] != "ghcr.io/milkinfrastructure/milk-carton":
        raise ValueError("private image release gateway is invalid")
    buildkit = adapter.immutable_image(release.get("buildkit_image_reference"))
    frontend = adapter.immutable_image(release.get("dockerfile_frontend_reference"))
    admissions = {}
    for expected_artifact, release_item in zip(repositories, images):
        release_item_fields = {
            "admission_sha256",
            "artifact",
            "image_reference",
            "ops_log_reference_sha256",
        }
        if schema_version == "milk.private-harness-release.v5":
            release_item_fields.add("source_commit")
        if (
            not isinstance(release_item, dict)
            or set(release_item) != release_item_fields
            or release_item.get("artifact") != expected_artifact
            or HEX64.fullmatch(release_item.get("admission_sha256", "")) is None
            or HEX64.fullmatch(
                release_item.get("ops_log_reference_sha256", "")
            )
            is None
            or schema_version == "milk.private-harness-release.v5"
            and re.fullmatch(r"[0-9a-f]{40}", release_item.get("source_commit", ""))
            is None
        ):
            raise ValueError("private image release item is invalid")
        local_ops_log = None
        if read_ops_log is not None:
            ops_log_raw, build_log_raw = read_ops_log(expected_artifact)
            local_ops_log, unused_started, unused_completed = _validate_local_ops_log(
                ops_log_raw,
                build_log_raw,
                expected_artifact,
                release_item["ops_log_reference_sha256"],
            )
        admission_raw = read_admission(
            expected_artifact,
            release_item["admission_sha256"],
        )
        admission = _strict_object(admission_raw)
        repository = repositories[expected_artifact]
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
                "ops_log_reference",
                "ops_log_reference_sha256",
                "platform",
                "visibility",
                "builder",
            }
            or admission.get("schema_version") != "milk.private-image-admission.v1"
            or admission.get("artifact") != expected_artifact
            or admission.get("repository") != repository
            or admission.get("source_repository") != release["source_repository"]
            or admission.get("source_commit")
            != (
                release_item["source_commit"]
                if schema_version == "milk.private-harness-release.v5"
                else release["source_commit"]
            )
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
        expected_gateway = (
            None if expected_artifact in {"planner", "jobs"} else gateway
        )
        if admission.get("gateway_image_reference") != expected_gateway:
            raise ValueError("private image gateway binding is invalid")
        admission_ops_log = admission.get("ops_log_reference")
        if (
            not isinstance(admission_ops_log, dict)
            or set(admission_ops_log)
            != {
                "schema_version",
                "authority",
                "reference",
                "receipt_sha256",
                "immutable",
                "content_retained",
            }
            or admission_ops_log.get("schema_version")
            != "milk.private-ops-log-reference.v1"
            or admission_ops_log.get("authority")
            != "private-release-evidence"
            or admission_ops_log.get("reference") != "build-log.json"
            or HEX64.fullmatch(
                admission_ops_log.get("receipt_sha256", "")
            )
            is None
            or admission_ops_log.get("immutable") is not True
            or admission_ops_log.get("content_retained") is not False
            or admission.get("ops_log_reference_sha256")
            != release_item["ops_log_reference_sha256"]
            or hashlib.sha256(canonical_json(admission_ops_log)).hexdigest()
            != release_item["ops_log_reference_sha256"]
            or local_ops_log is not None
            and admission_ops_log != local_ops_log
        ):
            raise ValueError("private image admission ops-log reference is invalid")
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
        admitted_builder = admission.get("builder")
        admitted_buildkit = buildkit
        admitted_frontend = frontend
        if schema_version == "milk.private-harness-release.v5" and isinstance(
            admitted_builder, dict
        ):
            admitted_buildkit = adapter.immutable_image(
                admitted_builder.get("buildkit_image_reference")
            )
            admitted_frontend = adapter.immutable_image(
                admitted_builder.get("dockerfile_frontend_reference")
            )
        if admitted_builder != {
            "authority": "local-socket",
            "driver": "docker-container",
            "endpoint_kind": "local-socket",
            "buildkit_image_reference": admitted_buildkit,
            "buildkit_version": "v0.23.2",
            "dockerfile_frontend_reference": admitted_frontend,
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


def _validate_gateway_release(
    release_raw,
    read_admission,
    expected_release_sha256=None,
    read_ops_log=None,
):
    if not isinstance(release_raw, (bytes, bytearray)):
        raise ValueError("private gateway release authority is invalid")
    if (
        expected_release_sha256 is not None
        and (
            not isinstance(expected_release_sha256, str)
            or HEX64.fullmatch(expected_release_sha256) is None
        )
    ):
        raise ValueError("private gateway release SHA-256 is invalid")
    release_sha256 = hashlib.sha256(release_raw).hexdigest()
    if expected_release_sha256 is not None and release_sha256 != expected_release_sha256:
        raise ValueError("private gateway release SHA-256 differs from its authority key")
    release = _strict_object(release_raw)
    image = release.get("image")
    if (
        set(release)
        != {
            "schema_version",
            "source_commit",
            "source_date_epoch",
            "source_repository",
            "buildkit_image_reference",
            "dockerfile_frontend_reference",
            "build_authority",
            "platform",
            "image",
            "ops_log_reference_sha256",
            "started_at",
            "completed_at",
        }
        or release.get("schema_version") != "milk.private-gateway-release.v1"
        or re.fullmatch(r"[0-9a-f]{40}", release.get("source_commit", "")) is None
        or type(release.get("source_date_epoch")) is not int
        or release["source_date_epoch"] <= 0
        or release.get("source_repository")
        != "https://github.com/milkinfrastructure/milk-carton"
        or release.get("build_authority") != "local-socket"
        or release.get("platform") != "linux/amd64"
        or HEX64.fullmatch(release.get("ops_log_reference_sha256", "")) is None
        or not isinstance(image, dict)
        or set(image) != {"admission_sha256", "artifact", "image_reference"}
        or image.get("artifact") != "gateway"
        or HEX64.fullmatch(image.get("admission_sha256", "")) is None
    ):
        raise ValueError("private gateway release receipt is invalid")
    started = _utc(release.get("started_at"), "gateway release started_at")
    completed = _utc(release.get("completed_at"), "gateway release completed_at")
    if completed < started:
        raise ValueError("private gateway release timestamps are invalid")
    buildkit = adapter.immutable_image(release.get("buildkit_image_reference"))
    frontend = adapter.immutable_image(
        release.get("dockerfile_frontend_reference")
    )
    image_reference = adapter.immutable_ghcr_image(image.get("image_reference"))
    if image_reference.rpartition("@sha256:")[0] != GATEWAY_IMAGE_REPOSITORY:
        raise ValueError("private gateway release image is invalid")
    if read_ops_log is not None:
        ops_log_raw, build_log_raw = read_ops_log()
        unused_ops_log, build_started, build_completed = _validate_local_ops_log(
            ops_log_raw,
            build_log_raw,
            "gateway",
            release["ops_log_reference_sha256"],
        )
        if build_started < started or build_completed > completed:
            raise ValueError("private gateway build log timestamps are invalid")
    admission_raw = read_admission(image["admission_sha256"])
    admission = _strict_object(admission_raw)
    if (
        hashlib.sha256(admission_raw).hexdigest() != image["admission_sha256"]
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
        or admission.get("artifact") != "gateway"
        or admission.get("repository") != GATEWAY_IMAGE_REPOSITORY
        or admission.get("image_reference") != image_reference
        or admission.get("source_repository") != release["source_repository"]
        or admission.get("source_commit") != release["source_commit"]
        or admission.get("source_context_method") != "git-archive-tar-v1"
        or HEX64.fullmatch(admission.get("source_context_sha256", "")) is None
        or admission.get("gateway_image_reference") is not None
        or admission.get("index_sha256")
        != image_reference.rpartition("@sha256:")[2]
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
        raise ValueError("private gateway admission is invalid")
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
        raise ValueError("private gateway attestations are invalid")
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
        raise ValueError("private gateway builder admission is invalid")
    return {
        "release_sha256": release_sha256,
        "release_raw": bytes(release_raw),
        "image": {
            "image_reference": image_reference,
            "admission_sha256": image["admission_sha256"],
            "raw": bytes(admission_raw),
        },
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
        read_ops_log=lambda artifact: (
            read(f"{artifact}/ops-log-reference.json"),
            read(f"{artifact}/build-log.json"),
        ),
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


def load_local_private_gateway_release(directory):
    if not isinstance(directory, str) or not os.path.isabs(directory):
        raise ValueError("private gateway release evidence path must be absolute")
    root_path = Path(directory)
    if root_path.is_symlink() or not root_path.is_dir():
        raise ValueError("private gateway release evidence directory is invalid")
    root = root_path.resolve()

    def read(relative):
        path = root.joinpath(*relative.split("/"))
        if path.is_symlink() or not path.is_file() or root not in path.resolve().parents:
            raise ValueError("private gateway admission file is invalid")
        with path.open("rb") as source:
            return source.read(MAX_OBJECT_BYTES + 1)

    return _validate_gateway_release(
        read("release.json"),
        lambda unused_sha256: read("admission.json"),
        read_ops_log=lambda: (
            read("ops-log-reference.json"),
            read("build-log.json"),
        ),
    )


def load_published_private_gateway_release(store, release_sha256):
    if not isinstance(release_sha256, str) or HEX64.fullmatch(release_sha256) is None:
        raise ValueError("private gateway release SHA-256 is invalid")
    release_raw = store.get(f"{RELEASE_PREFIX}/{release_sha256}.json")
    return _validate_gateway_release(
        release_raw,
        lambda admission_sha256: store.get(
            f"{ARTIFACT_PREFIX}/{admission_sha256}.json"
        ),
        release_sha256,
    )
