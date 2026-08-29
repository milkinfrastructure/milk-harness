from __future__ import annotations

import argparse
import os
import urllib.error

from milk_harness.evidence import R2EvidenceStore, canonical_json, create_same
from milk_harness.image_admission import (
    ARTIFACT_PREFIX,
    RELEASE_IMAGE_REPOSITORIES,
    RELEASE_PREFIX,
    _validate_release,
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
        create_same(
            store,
            f"{ARTIFACT_PREFIX}/{item['admission_sha256']}.json",
            item["raw"],
            "application/json",
        )
    create_same(
        store,
        f"{RELEASE_PREFIX}/{validated['release_sha256']}.json",
        validated["release_raw"],
        "application/json",
    )
    return {
        "schema_version": "milk.private-image-release-published.v1",
        "release_sha256": validated["release_sha256"],
        "artifact_admission_sha256s": {
            artifact: validated["images"][artifact]["admission_sha256"]
            for artifact in validated["images"]
        },
        "state": "published",
    }


def _reject_ambient_authorities(environ):
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
        and (name in forbidden_names or name.startswith(forbidden_prefixes))
        for name, value in environ.items()
    ):
        raise ValueError(
            "image admission publisher must receive only its evidence authority, not builder, provider, or other store credentials"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Publish one immutable private Milk image release to evidence R2"
    )
    parser.add_argument("--release-dir", required=True)
    arguments = parser.parse_args(argv)
    try:
        _reject_ambient_authorities(os.environ)
        receipt = publish_private_image_release(
            R2EvidenceStore.from_environment(),
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
