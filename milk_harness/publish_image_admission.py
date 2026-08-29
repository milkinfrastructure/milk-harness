from __future__ import annotations

import argparse
import os
import urllib.error

from milk_harness.evidence import R2EvidenceStore, canonical_json, create_same
from milk_harness.image_admission import (
    ARTIFACT_PREFIX,
    RELEASE_IMAGE_REPOSITORIES,
    RELEASE_PREFIX,
    _validate_gateway_release,
    _validate_release,
    load_local_private_gateway_release,
    load_local_private_image_release,
)


def publish_private_image_release(store, release):
    if not isinstance(release, dict) or set(release) != {
        "release_sha256",
        "release_raw",
        "images",
    }:
        raise ValueError("private image release authority is invalid")
    images = release.get("images")
    if not isinstance(images, dict) or not any(
        set(images) == set(repositories)
        for repositories in RELEASE_IMAGE_REPOSITORIES.values()
    ):
        raise ValueError("private image release authority is invalid")

    def admission(artifact, admission_sha256):
        item = images.get(artifact)
        if (
            not isinstance(item, dict)
            or set(item) != {"image_reference", "admission_sha256", "raw"}
            or item.get("admission_sha256") != admission_sha256
        ):
            raise ValueError("private image admission authority is invalid")
        return item.get("raw")

    validated = _validate_release(
        release.get("release_raw"),
        admission,
        release.get("release_sha256"),
    )
    for artifact in validated["images"]:
        item = validated["images"][artifact]
        key = f"{ARTIFACT_PREFIX}/{item['admission_sha256']}.json"
        create_same(
            store,
            key,
            item["raw"],
            "application/json",
        )
        if store.get(key) != item["raw"]:
            raise ValueError("published private image admission failed full-body readback")
    release_key = f"{RELEASE_PREFIX}/{validated['release_sha256']}.json"
    create_same(
        store,
        release_key,
        validated["release_raw"],
        "application/json",
    )
    if store.get(release_key) != validated["release_raw"]:
        raise ValueError("published private image release failed full-body readback")
    return {
        "schema_version": "milk.private-image-release-published.v1",
        "release_sha256": validated["release_sha256"],
        "artifact_admission_sha256s": {
            artifact: validated["images"][artifact]["admission_sha256"]
            for artifact in validated["images"]
        },
        "state": "published",
    }


def publish_private_gateway_release(store, release):
    if not isinstance(release, dict) or set(release) != {
        "release_sha256",
        "release_raw",
        "image",
    }:
        raise ValueError("private gateway release authority is invalid")
    image = release.get("image")

    def admission(admission_sha256):
        if (
            not isinstance(image, dict)
            or set(image) != {"image_reference", "admission_sha256", "raw"}
            or image.get("admission_sha256") != admission_sha256
        ):
            raise ValueError("private gateway admission authority is invalid")
        return image.get("raw")

    validated = _validate_gateway_release(
        release.get("release_raw"),
        admission,
        release.get("release_sha256"),
    )
    image = validated["image"]
    admission_key = f"{ARTIFACT_PREFIX}/{image['admission_sha256']}.json"
    create_same(store, admission_key, image["raw"], "application/json")
    if store.get(admission_key) != image["raw"]:
        raise ValueError("published private gateway admission failed full-body readback")
    release_key = f"{RELEASE_PREFIX}/{validated['release_sha256']}.json"
    create_same(store, release_key, validated["release_raw"], "application/json")
    if store.get(release_key) != validated["release_raw"]:
        raise ValueError("published private gateway release failed full-body readback")
    return {
        "schema_version": "milk.private-image-release-published.v1",
        "release_sha256": validated["release_sha256"],
        "artifact_admission_sha256s": {
            "gateway": image["admission_sha256"],
        },
        "state": "published",
    }


def _reject_ambient_authorities(environ):
    evidence_names = {
        "MILK_EVIDENCE_R2_ACCOUNT_ID",
        "MILK_EVIDENCE_R2_BUCKET",
        "MILK_EVIDENCE_R2_ACCESS_KEY_ID",
        "MILK_EVIDENCE_R2_SECRET_ACCESS_KEY",
        "MILK_EVIDENCE_R2_SESSION_TOKEN",
    }
    forbidden_prefixes = (
        "AWS_",
        "AZURE_",
        "BASETEN_",
        "BUILDKIT_",
        "BUILDX_",
        "CLOUDFLARE_",
        "DOCKER_",
        "DRAGONTALES_",
        "GCP_",
        "MILK_CAPTURE_",
        "MILK_CONTROL_",
        "MILK_ROUTE_",
        "MILK_TEACHER_",
        "MODAL_",
        "OPENAI_",
        "TEACHER_",
        "WANDB_",
    )
    forbidden_names = {
        "CI_JOB_TOKEN",
        "CI_REGISTRY_PASSWORD",
        "CR_PAT",
        "GH_ENTERPRISE_TOKEN",
        "GH_TOKEN",
        "GITHUB_ENTERPRISE_TOKEN",
        "GITHUB_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "HF_TOKEN",
        "HUGGING_FACE_HUB_TOKEN",
        "NGC_API_KEY",
        "NVIDIA_API_KEY",
        "REGISTRY_AUTH_FILE",
    }
    if any(
        value
        and (
            name in forbidden_names
            or name.startswith(forbidden_prefixes)
            or (name.startswith("MILK_") and name not in evidence_names)
        )
        for name, value in environ.items()
    ):
        raise ValueError(
            "publisher must receive only its evidence authority, not builder, provider, or other store credentials"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Publish one immutable private Milk image release to evidence R2"
    )
    release = parser.add_mutually_exclusive_group(required=True)
    release.add_argument("--release-dir")
    release.add_argument("--gateway-release-dir")
    arguments = parser.parse_args(argv)
    try:
        _reject_ambient_authorities(os.environ)
        store = R2EvidenceStore.from_environment()
        if arguments.gateway_release_dir is not None:
            receipt = publish_private_gateway_release(
                store,
                load_local_private_gateway_release(arguments.gateway_release_dir),
            )
        else:
            receipt = publish_private_image_release(
                store,
                load_local_private_image_release(arguments.release_dir),
            )
    except (
        OSError,
        RuntimeError,
        UnicodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"milk-image-admission: {error}", file=os.sys.stderr)
        return 64
    os.sys.stdout.buffer.write(canonical_json(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
