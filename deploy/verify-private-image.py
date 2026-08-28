#!/usr/bin/env python3
import argparse
import base64
import binascii
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request


SOURCE_REPOSITORY = "https://github.com/milkinfrastructure/milk-harness"
GITHUB_USERNAME = "ShantanuJoshi"
GATEWAY_REPOSITORY = "ghcr.io/milkinfrastructure/milk-gateway"
BUILDKIT_IMAGE = "moby/buildkit@sha256:ddd1ca44b21eda906e81ab14a3d467fa6c39cd73b9a39df1196210edcb8db59e"
BUILDKIT_VERSION = "v0.23.2"
DOCKERFILE_FRONTEND = "docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e"
BUILDKIT_BUILD_TYPE = (
    "https://github.com/moby/buildkit/blob/master/"
    "docs/attestations/slsa-definitions.md"
)
SBOM_SCANNER_SHA256 = (
    "ae4f3b554449e7e25548e7d8ccc029d17357348e30c6e3df01b92bc93654d6a9"
)
SLSA_V1 = "https://slsa.dev/provenance/v1"
SPDX = "https://spdx.dev/Document"
MANIFEST_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
CONFIG_MEDIA_TYPES = {
    "application/vnd.oci.image.config.v1+json",
    "application/vnd.docker.container.image.v1+json",
}
ARTIFACTS = {
    "student-train": {
        "repository": "ghcr.io/milkinfrastructure/milk-student-train",
        "dockerfile": "deploy/student-train/Dockerfile",
        "base_sha256": {
            "6b8b2cf339de90b67ed25f246ad3dcfd773cb80e629577eb31aeb2a6b81db80e",
        },
        "gateway": True,
    },
    "student-branch": {
        "repository": "ghcr.io/milkinfrastructure/milk-student-branch",
        "dockerfile": "deploy/student-branch/Dockerfile",
        "base_sha256": {
            "50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d",
        },
        "gateway": True,
    },
    "teacher-gpt-oss": {
        "repository": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
        "dockerfile": "deploy/teacher/gpt-oss-120b/Dockerfile",
        "base_sha256": {
            "50509e700235cea487715cedeb501d20a1cd15fa6a54ce93688284bd0d96995d",
        },
        "gateway": True,
    },
    "planner": {
        "repository": "ghcr.io/milkinfrastructure/milk-planner",
        "dockerfile": "Dockerfile.planner",
        "base_sha256": {
            "b4f875e650466fa0fe62c6fd3f02517a392123eea85f1d7e69d85f780e4db1c1",
            "14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce",
        },
        "gateway": False,
    },
    "jobs": {
        "repository": "ghcr.io/milkinfrastructure/milk-jobs",
        "dockerfile": "Dockerfile.jobs",
        "base_sha256": {
            "519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7",
        },
        "gateway": False,
    },
}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")
MAX_TOKEN_BYTES = 8_192
MAX_AUTH_BYTES = 1_048_576
MAX_BLOB_BYTES = 67_108_864


def _object(raw, label):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"{label} contains a duplicate JSON key")
            value[key] = item
        return value

    try:
        value = json.loads(raw, object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _descriptor(value, label):
    if not isinstance(value, dict):
        raise ValueError(f"{label} is not a descriptor")
    digest = value.get("digest")
    size = value.get("size")
    media_type = value.get("mediaType")
    if DIGEST.fullmatch(digest if isinstance(digest, str) else "") is None:
        raise ValueError(f"{label} digest is invalid")
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError(f"{label} size is invalid")
    if not isinstance(media_type, str) or not media_type:
        raise ValueError(f"{label} media type is invalid")
    return digest, size, media_type


def _run(*arguments):
    return subprocess.run(
        arguments,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        if urllib.parse.urlsplit(new_url).scheme != "https":
            raise ValueError("registry redirect left HTTPS")
        redirected = super().redirect_request(
            request, file_pointer, code, message, headers, new_url
        )
        if redirected is None:
            return None
        if urllib.parse.urlsplit(request.full_url).hostname != urllib.parse.urlsplit(
            new_url
        ).hostname:
            redirected.remove_header("Authorization")
        return redirected


def _open_bytes(request, limit, label):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _SafeRedirect())
    try:
        with opener.open(request, timeout=60) as response:
            if urllib.parse.urlsplit(response.url).scheme != "https":
                raise ValueError(f"{label} left HTTPS")
            declared = response.headers.get("Content-Length")
            if declared is not None:
                try:
                    declared_size = int(declared)
                except ValueError as error:
                    raise ValueError(f"{label} Content-Length is invalid") from error
                if declared_size < 0 or declared_size > limit:
                    raise ValueError(f"{label} exceeds its byte limit")
            raw = response.read(limit + 1)
    except (OSError, urllib.error.URLError):
        raise ValueError(f"cannot fetch {label}") from None
    if len(raw) > limit:
        raise ValueError(f"{label} exceeds its byte limit")
    return raw


def _registry_bearer(github_token, package):
    query = urllib.parse.urlencode(
        {
            "service": "ghcr.io",
            "scope": f"repository:milkinfrastructure/{package}:pull",
        }
    )
    basic = base64.b64encode(
        f"{GITHUB_USERNAME}:{github_token}".encode("ascii")
    ).decode("ascii")
    request = urllib.request.Request(
        "https://ghcr.io/token?" + query,
        headers={
            "Accept": "application/json",
            "Authorization": "Basic " + basic,
            "User-Agent": "milk-harness-release/1",
        },
    )
    response = _object(
        _open_bytes(request, MAX_AUTH_BYTES, "GHCR authorization"),
        "GHCR authorization",
    )
    bearer = response.get("token", response.get("access_token"))
    if not isinstance(bearer, str) or not bearer or len(bearer) > MAX_TOKEN_BYTES:
        raise ValueError("GHCR authorization returned no bounded token")
    return bearer


def _registry_blob(bearer, package, digest):
    if DIGEST.fullmatch(digest) is None:
        raise ValueError("registry blob digest is invalid")
    request = urllib.request.Request(
        f"https://ghcr.io/v2/milkinfrastructure/{package}/blobs/"
        + urllib.parse.quote(digest, safe=":"),
        headers={
            "Accept": "application/octet-stream",
            "Authorization": "Bearer " + bearer,
            "User-Agent": "milk-harness-release/1",
        },
    )
    raw = _open_bytes(request, MAX_BLOB_BYTES, "GHCR content blob")
    if _digest(raw) != digest:
        raise ValueError("GHCR content blob differs from its descriptor digest")
    return raw


def _subject_binds_exactly(statement, child_digest):
    subjects = statement.get("subject")
    if not isinstance(subjects, list) or len(subjects) != 1:
        return False
    subject = subjects[0]
    if not isinstance(subject, dict) or set(subject) != {"name", "digest"}:
        return False
    name = subject["name"]
    digest = subject["digest"]
    return (
        isinstance(name, str)
        and 1 <= len(name) <= 4096
        and isinstance(digest, dict)
        and digest == {"sha256": child_digest.removeprefix("sha256:")}
    )


def _write_new(path, raw):
    with path.open("xb") as output:
        output.write(raw)


def _expected_dependencies(arguments, artifact):
    expected = set(artifact["base_sha256"])
    expected.add(DOCKERFILE_FRONTEND.rsplit("@sha256:", 1)[1])
    expected.add(SBOM_SCANNER_SHA256)
    if artifact["gateway"]:
        expected.add(arguments.gateway_image_reference.rsplit("@sha256:", 1)[1])
    return expected


def _validate_buildkit_metadata(value, artifact):
    if not isinstance(value, dict) or set(value) != {"source"}:
        raise ValueError("SLSA v1 BuildKit metadata is invalid")
    source = value["source"]
    if not isinstance(source, dict) or set(source) != {"infos", "locations"}:
        raise ValueError("SLSA v1 BuildKit source metadata is invalid")
    infos = source["infos"]
    locations = source["locations"]
    if not isinstance(infos, list) or len(infos) != 1:
        raise ValueError("SLSA v1 Dockerfile source metadata is invalid")
    info = infos[0]
    if not isinstance(info, dict) or set(info) != {
        "data",
        "digestMapping",
        "filename",
        "language",
        "llbDefinition",
    }:
        raise ValueError("SLSA v1 Dockerfile source metadata is invalid")
    expected_name = Path(artifact["dockerfile"]).name
    if info["filename"] != expected_name or info["language"] != "Dockerfile":
        raise ValueError("SLSA v1 Dockerfile identity is invalid")
    encoded = info["data"]
    if not isinstance(encoded, str) or not 1 <= len(encoded) <= 1_048_576:
        raise ValueError("SLSA v1 Dockerfile source is invalid")
    try:
        dockerfile = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError("SLSA v1 Dockerfile source is invalid") from error
    expected = Path(__file__).resolve().parents[1] / artifact["dockerfile"]
    if dockerfile != expected.read_bytes():
        raise ValueError("SLSA v1 Dockerfile differs from the committed source")
    if (
        not isinstance(info["digestMapping"], dict)
        or not info["digestMapping"]
        or len(info["digestMapping"]) > 4096
        or not isinstance(info["llbDefinition"], list)
        or not info["llbDefinition"]
        or len(info["llbDefinition"]) > 4096
        or not isinstance(locations, dict)
        or not locations
        or len(locations) > 4096
    ):
        raise ValueError("SLSA v1 max-mode source mapping is incomplete")


def _validate_slsa(predicate, arguments, artifact):
    if set(predicate) != {"buildDefinition", "runDetails"}:
        raise ValueError("SLSA v1 provenance predicate is not strict")
    definition = predicate["buildDefinition"]
    details = predicate["runDetails"]
    if not isinstance(definition, dict) or set(definition) != {
        "buildType",
        "externalParameters",
        "internalParameters",
        "resolvedDependencies",
    }:
        raise ValueError("SLSA v1 build definition is invalid")
    if definition["buildType"] != BUILDKIT_BUILD_TYPE:
        raise ValueError("SLSA v1 build type is not BuildKit")
    external = definition["externalParameters"]
    if not isinstance(external, dict) or set(external) != {
        "configSource",
        "request",
    }:
        raise ValueError("SLSA v1 external parameters are invalid")
    config_source = external["configSource"]
    expected_dockerfile = Path(artifact["dockerfile"]).name
    if not isinstance(config_source, dict) or config_source != {
        "path": expected_dockerfile
    }:
        raise ValueError("SLSA v1 configuration source is invalid")
    request = external["request"]
    if not isinstance(request, dict) or set(request) != {
        "frontend",
        "args",
        "locals",
    }:
        raise ValueError("SLSA v1 frontend request is invalid")
    if request["frontend"] != "gateway.v0":
        raise ValueError("SLSA v1 frontend is not the pinned gateway frontend")
    locals_ = request["locals"]
    if (
        not isinstance(locals_, list)
        or any(
            not isinstance(item, dict)
            or set(item) != {"name"}
            or not isinstance(item["name"], str)
            for item in locals_
        )
        or sorted(item["name"] for item in locals_) != ["context", "dockerfile"]
    ):
        raise ValueError("SLSA v1 local inputs are invalid")
    expected = {
        "build-arg:BUILDKIT_SYNTAX": DOCKERFILE_FRONTEND,
        "build-arg:MILK_SOURCE_CONTEXT_SHA256": arguments.source_context_sha256,
        "build-arg:SOURCE_DATE_EPOCH": str(arguments.source_date_epoch),
        "label:org.opencontainers.image.revision": arguments.source_commit,
        "label:org.opencontainers.image.source": SOURCE_REPOSITORY,
        "cmdline": DOCKERFILE_FRONTEND,
        "source": DOCKERFILE_FRONTEND,
    }
    if artifact["gateway"]:
        expected["build-arg:MILK_GATEWAY_IMAGE"] = arguments.gateway_image_reference
    parameters = request["args"]
    if parameters != expected:
        raise ValueError("SLSA v1 build arguments do not bind release inputs")
    internal = definition["internalParameters"]
    if not isinstance(internal, dict) or set(internal) != {
        "builderPlatform",
        "buildConfig",
    }:
        raise ValueError("SLSA v1 internal parameters are invalid")
    if internal["builderPlatform"] != "linux/amd64":
        raise ValueError("SLSA v1 builder platform is not linux/amd64")
    build_config = internal["buildConfig"]
    if not isinstance(build_config, dict) or set(build_config) != {
        "llbDefinition",
        "digestMapping",
    }:
        raise ValueError("SLSA v1 max-mode build configuration is invalid")
    llb = build_config["llbDefinition"]
    mapping = build_config["digestMapping"]
    if (
        not isinstance(llb, list)
        or not llb
        or len(llb) > 4096
        or not isinstance(mapping, dict)
        or not mapping
        or len(mapping) > 4096
    ):
        raise ValueError("SLSA v1 max-mode build configuration is empty")
    dependencies = definition["resolvedDependencies"]
    if not isinstance(dependencies, list) or not 3 <= len(dependencies) <= 64:
        raise ValueError("SLSA v1 resolved dependencies are invalid")
    dependency_sha256 = set()
    dependency_uris = set()
    for dependency in dependencies:
        if not isinstance(dependency, dict) or not set(dependency).issubset(
            {
                "name",
                "uri",
                "digest",
                "content",
                "downloadLocation",
                "mediaType",
                "annotations",
            }
        ):
            raise ValueError("SLSA v1 resolved dependency is invalid")
        uri = dependency.get("uri")
        digests = dependency.get("digest")
        if (
            not isinstance(uri, str)
            or not 1 <= len(uri) <= 4096
            or uri in dependency_uris
            or not isinstance(digests, dict)
            or set(digests) != {"sha256"}
            or SHA256.fullmatch(
                digests["sha256"] if isinstance(digests["sha256"], str) else ""
            )
            is None
        ):
            raise ValueError("SLSA v1 resolved dependency identity is invalid")
        dependency_uris.add(uri)
        dependency_sha256.add(digests["sha256"])
    if not _expected_dependencies(arguments, artifact).issubset(dependency_sha256):
        raise ValueError(
            "SLSA v1 dependencies do not bind the frontend, scanner, and every base image"
        )

    if not isinstance(details, dict) or set(details) != {"builder", "metadata"}:
        raise ValueError("SLSA v1 run details are invalid")
    builder = details["builder"]
    metadata = details["metadata"]
    if (
        not isinstance(builder, dict)
        or set(builder) != {"id"}
        or builder["id"] != ""
        or not isinstance(metadata, dict)
        or set(metadata)
        != {
            "invocationID",
            "startedOn",
            "finishedOn",
            "buildkit_metadata",
            "buildkit_completeness",
        }
        or any(
            not isinstance(metadata[key], str) or not metadata[key]
            for key in ("invocationID", "startedOn", "finishedOn")
        )
    ):
        raise ValueError("SLSA v1 run details are incomplete")
    if metadata["buildkit_completeness"] != {
        "request": True,
        "resolvedDependencies": False,
    }:
        raise ValueError("SLSA v1 BuildKit completeness is invalid")
    _validate_buildkit_metadata(metadata["buildkit_metadata"], artifact)


def _validate_checksums(checksums, label):
    if not isinstance(checksums, list):
        raise ValueError(f"SPDX {label} checksums are invalid")
    sha256 = 0
    for checksum in checksums:
        if not isinstance(checksum, dict) or set(checksum) != {
            "algorithm",
            "checksumValue",
        }:
            raise ValueError(f"SPDX {label} checksum is invalid")
        algorithm = checksum["algorithm"]
        value = checksum["checksumValue"]
        if (
            not isinstance(algorithm, str)
            or not algorithm
            or not isinstance(value, str)
            or not 1 <= len(value) <= 512
        ):
            raise ValueError(f"SPDX {label} checksum is invalid")
        if algorithm == "SHA256":
            if SHA256.fullmatch(value) is None:
                raise ValueError(f"SPDX {label} SHA-256 is invalid")
            sha256 += 1
    return sha256


def _validate_spdx(predicate):
    if predicate.get("spdxVersion") != "SPDX-2.3" or predicate.get(
        "SPDXID"
    ) != "SPDXRef-DOCUMENT":
        raise ValueError("SPDX document identity is invalid")
    packages = predicate.get("packages")
    if not isinstance(packages, list) or not 1 <= len(packages) <= 50_000:
        raise ValueError("SPDX document packages are incomplete")
    inventory_ids = set()
    sha256_checksums = 0
    for package in packages:
        if not isinstance(package, dict):
            raise ValueError("SPDX package is invalid")
        identifier = package.get("SPDXID")
        if (
            not isinstance(identifier, str)
            or not identifier.startswith("SPDXRef-")
            or identifier in inventory_ids
            or not isinstance(package.get("name"), str)
            or not package["name"]
        ):
            raise ValueError("SPDX package identity is invalid")
        inventory_ids.add(identifier)
        sha256_checksums += _validate_checksums(
            package.get("checksums", []), "package"
        )
    files = predicate.get("files", [])
    if not isinstance(files, list) or len(files) > 100_000:
        raise ValueError("SPDX file inventory is invalid")
    for file_ in files:
        if not isinstance(file_, dict):
            raise ValueError("SPDX file is invalid")
        identifier = file_.get("SPDXID")
        if (
            not isinstance(identifier, str)
            or not identifier.startswith("SPDXRef-")
            or identifier in inventory_ids
            or not isinstance(file_.get("fileName"), str)
            or not file_["fileName"]
        ):
            raise ValueError("SPDX file identity is invalid")
        inventory_ids.add(identifier)
        sha256_checksums += _validate_checksums(file_.get("checksums", []), "file")
    if sha256_checksums == 0:
        raise ValueError("SPDX document has no image-bound SHA-256 checksum")


def _validate_release_arguments(arguments):
    artifact = ARTIFACTS.get(arguments.artifact)
    if artifact is None:
        raise ValueError("artifact is not an admitted harness image")
    repository = artifact["repository"]
    if arguments.tagged_reference != repository + ":source-" + arguments.source_commit:
        raise ValueError("tagged reference does not bind the artifact and source commit")
    if COMMIT.fullmatch(arguments.source_commit) is None:
        raise ValueError("source commit is invalid")
    if not isinstance(arguments.source_date_epoch, int) or arguments.source_date_epoch <= 0:
        raise ValueError("source date epoch is invalid")
    if SHA256.fullmatch(arguments.source_context_sha256) is None:
        raise ValueError("source context digest is invalid")
    if artifact["gateway"]:
        gateway = arguments.gateway_image_reference or ""
        if not gateway.startswith(GATEWAY_REPOSITORY + "@sha256:") or DIGEST.fullmatch(
            gateway.removeprefix(GATEWAY_REPOSITORY + "@")
        ) is None:
            raise ValueError("gateway image reference is invalid")
    elif arguments.gateway_image_reference is not None:
        raise ValueError("artifact must not carry a gateway image reference")
    if not arguments.evidence_dir.is_absolute() or not arguments.evidence_dir.is_dir():
        raise ValueError("evidence directory must be an existing absolute directory")
    if not arguments.docker_config.is_absolute() or not arguments.docker_config.is_dir():
        raise ValueError("Docker configuration must be an existing absolute directory")
    if not arguments.metadata.is_file():
        raise ValueError("build metadata is missing")
    return artifact


def verify(arguments, github_token):
    artifact = _validate_release_arguments(arguments)
    repository = artifact["repository"]
    package = repository.rsplit("/", 1)[1]
    metadata = _object(arguments.metadata.read_bytes(), "build metadata")
    index_digest = metadata.get("containerimage.digest")
    if DIGEST.fullmatch(index_digest if isinstance(index_digest, str) else "") is None:
        raise ValueError("build metadata has no immutable index digest")
    immutable_reference = repository + "@" + index_digest
    docker_prefix = (
        "docker",
        "--config",
        str(arguments.docker_config),
        "buildx",
        "imagetools",
        "inspect",
        "--raw",
    )

    raw_index = _run(*docker_prefix, immutable_reference)
    if _digest(raw_index) != index_digest:
        raise ValueError("registry index differs from the build digest")
    index = _object(raw_index, "registry index")
    if index.get("schemaVersion") != 2 or index.get("mediaType") not in {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }:
        raise ValueError("registry index identity is invalid")
    manifests = index.get("manifests")
    if not isinstance(manifests, list):
        raise ValueError("registry index has no descriptor list")
    runnable = []
    attestations = []
    for descriptor in manifests:
        _descriptor(descriptor, "registry index descriptor")
        platform = descriptor.get("platform")
        annotations = descriptor.get("annotations")
        if platform == {"architecture": "amd64", "os": "linux"}:
            runnable.append(descriptor)
        elif (
            platform == {"architecture": "unknown", "os": "unknown"}
            and isinstance(annotations, dict)
            and annotations.get("vnd.docker.reference.type")
            == "attestation-manifest"
        ):
            attestations.append(descriptor)
        else:
            raise ValueError("registry index contains an unauthorized descriptor")
    if len(runnable) != 1 or len(attestations) != 1:
        raise ValueError(
            "registry index must contain one runnable image and one attestation manifest"
        )
    child_digest, child_size, child_media_type = _descriptor(
        runnable[0], "runnable image"
    )
    attestation_digest, attestation_size, attestation_media_type = _descriptor(
        attestations[0], "attestation manifest"
    )
    if child_media_type not in MANIFEST_MEDIA_TYPES:
        raise ValueError("runnable image media type is invalid")
    if attestation_media_type != "application/vnd.oci.image.manifest.v1+json":
        raise ValueError("attestation manifest media type is invalid")
    if attestations[0]["annotations"].get(
        "vnd.docker.reference.digest"
    ) != child_digest:
        raise ValueError("attestation manifest does not bind the runnable image")

    raw_manifest = _run(*docker_prefix, repository + "@" + child_digest)
    if _digest(raw_manifest) != child_digest or len(raw_manifest) != child_size:
        raise ValueError("runnable manifest differs from its descriptor")
    manifest = _object(raw_manifest, "runnable manifest")
    if (
        manifest.get("schemaVersion") != 2
        or manifest.get("mediaType") != child_media_type
    ):
        raise ValueError("runnable manifest identity is invalid")
    config_digest, config_size, config_media_type = _descriptor(
        manifest.get("config"), "runtime configuration"
    )
    if config_media_type not in CONFIG_MEDIA_TYPES:
        raise ValueError("runtime configuration media type is invalid")
    layers = manifest.get("layers")
    if not isinstance(layers, list):
        raise ValueError("runnable manifest has no layer list")
    for layer in layers:
        _descriptor(layer, "runnable layer")

    raw_attestation = _run(*docker_prefix, repository + "@" + attestation_digest)
    if (
        _digest(raw_attestation) != attestation_digest
        or len(raw_attestation) != attestation_size
    ):
        raise ValueError("attestation manifest differs from its descriptor")
    attestation = _object(raw_attestation, "attestation manifest")
    if (
        set(attestation) != {"schemaVersion", "mediaType", "config", "layers"}
        or attestation["schemaVersion"] != 2
        or attestation["mediaType"] != attestation_media_type
    ):
        raise ValueError("attestation manifest identity is invalid")
    (
        attestation_config_digest,
        attestation_config_size,
        attestation_config_media_type,
    ) = _descriptor(
        attestation["config"], "attestation configuration"
    )
    if attestation_config_media_type != "application/vnd.oci.image.config.v1+json":
        raise ValueError("attestation configuration media type is invalid")
    attestation_layers = attestation.get("layers")
    if not isinstance(attestation_layers, list) or len(attestation_layers) != 2:
        raise ValueError("attestation manifest must contain exactly two statements")

    bearer = _registry_bearer(github_token, package)
    raw_attestation_config = _registry_blob(
        bearer, package, attestation_config_digest
    )
    if len(raw_attestation_config) != attestation_config_size:
        raise ValueError("attestation configuration size differs from its descriptor")
    attestation_config = _object(
        raw_attestation_config, "attestation configuration"
    )
    statement_digests = [
        _descriptor(layer, "attestation statement")[0]
        for layer in attestation_layers
    ]
    if attestation_config != {
        "architecture": "unknown",
        "config": {},
        "os": "unknown",
        "rootfs": {"diff_ids": statement_digests, "type": "layers"},
    }:
        raise ValueError("attestation configuration is invalid")
    raw_config = _registry_blob(bearer, package, config_digest)
    if len(raw_config) != config_size:
        raise ValueError("runtime configuration size differs from its descriptor")
    config = _object(raw_config, "runtime configuration")
    if config.get("architecture") != "amd64" or config.get("os") != "linux":
        raise ValueError("runtime configuration platform is not linux/amd64")
    runtime = config.get("config")
    labels = runtime.get("Labels") if isinstance(runtime, dict) else None
    if not isinstance(labels, dict):
        raise ValueError("runtime configuration labels are missing")
    if labels.get("org.opencontainers.image.source") != SOURCE_REPOSITORY:
        raise ValueError("runtime source label is invalid")
    if labels.get("org.opencontainers.image.revision") != arguments.source_commit:
        raise ValueError("runtime revision label is invalid")
    rootfs = config.get("rootfs")
    if not isinstance(rootfs, dict) or rootfs.get("type") != "layers":
        raise ValueError("runtime root filesystem is invalid")
    diff_ids = rootfs.get("diff_ids")
    if not isinstance(diff_ids, list) or len(diff_ids) != len(layers):
        raise ValueError("runtime root filesystem does not bind every layer")
    if any(
        DIGEST.fullmatch(item if isinstance(item, str) else "") is None
        for item in diff_ids
    ):
        raise ValueError("runtime root filesystem digest is invalid")

    statement_raw = {}
    statements = []
    for layer in attestation_layers:
        layer_digest, layer_size, layer_media_type = _descriptor(
            layer, "attestation statement"
        )
        annotations = layer.get("annotations")
        if layer_media_type != "application/vnd.in-toto+json" or not isinstance(
            annotations, dict
        ):
            raise ValueError("attestation statement descriptor is invalid")
        declared_predicate = annotations.get("in-toto.io/predicate-type")
        if declared_predicate not in {SLSA_V1, SPDX}:
            raise ValueError("attestation statement predicate is unauthorized")
        raw_statement = _registry_blob(bearer, package, layer_digest)
        if len(raw_statement) != layer_size:
            raise ValueError("attestation statement size differs from its descriptor")
        statement = _object(raw_statement, "attestation statement")
        if set(statement) != {"_type", "subject", "predicateType", "predicate"}:
            raise ValueError("attestation statement is not strict")
        if statement["_type"] != "https://in-toto.io/Statement/v0.1":
            raise ValueError("attestation statement type is invalid")
        if statement["predicateType"] != declared_predicate:
            raise ValueError(
                "attestation statement predicate differs from its descriptor"
            )
        if not _subject_binds_exactly(statement, child_digest):
            raise ValueError(
                "attestation statement does not bind exactly the runnable image"
            )
        predicate = statement["predicate"]
        if not isinstance(predicate, dict):
            raise ValueError("attestation statement predicate payload is missing")
        if declared_predicate == SLSA_V1:
            _validate_slsa(predicate, arguments, artifact)
        else:
            _validate_spdx(predicate)
        if declared_predicate in statement_raw:
            raise ValueError("attestation predicate is duplicated")
        statement_raw[declared_predicate] = raw_statement
        statements.append(
            {
                "layer_sha256": layer_digest.removeprefix("sha256:"),
                "predicate_type": declared_predicate,
            }
        )
    if set(statement_raw) != {SLSA_V1, SPDX}:
        raise ValueError("SLSA v1 provenance and SPDX SBOM are both required")
    statements.sort(key=lambda item: (item["predicate_type"], item["layer_sha256"]))

    visibility = _run(
        "gh",
        "api",
        "--hostname",
        "github.com",
        f"/orgs/milkinfrastructure/packages/container/{package}",
        "--jq",
        ".visibility",
    ).decode("utf-8", errors="strict").strip()
    if visibility != "private":
        raise ValueError(f"{package} package is not private")

    build_log = _object(
        arguments.evidence_dir.joinpath("build-log.json").read_bytes(),
        "build log observation",
    )
    if (
        build_log.get("schema_version") != "milk.content-free-build-log.v1"
        or build_log.get("artifact") != arguments.artifact
        or build_log.get("exit_code") != 0
        or build_log.get("content_retained") is not False
        or not isinstance(build_log.get("sha256"), str)
        or SHA256.fullmatch(build_log["sha256"]) is None
        or not isinstance(build_log.get("bytes"), int)
        or isinstance(build_log.get("bytes"), bool)
        or build_log["bytes"] < 0
    ):
        raise ValueError("build log observation is invalid")

    _write_new(arguments.evidence_dir / "index.json", raw_index)
    _write_new(arguments.evidence_dir / "amd64-manifest.json", raw_manifest)
    _write_new(arguments.evidence_dir / "config.json", raw_config)
    _write_new(arguments.evidence_dir / "attestation-manifest.json", raw_attestation)
    _write_new(
        arguments.evidence_dir / "slsa-provenance.json", statement_raw[SLSA_V1]
    )
    _write_new(arguments.evidence_dir / "spdx-sbom.json", statement_raw[SPDX])

    gateway = arguments.gateway_image_reference
    receipt = {
        "schema_version": "milk.private-harness-image-build.v1",
        "artifact": arguments.artifact,
        "source_commit": arguments.source_commit,
        "source_date_epoch": arguments.source_date_epoch,
        "source_repository": SOURCE_REPOSITORY,
        "tagged_reference": arguments.tagged_reference,
        "image_reference": immutable_reference,
        "index_sha256": index_digest.removeprefix("sha256:"),
        "amd64_manifest_sha256": child_digest.removeprefix("sha256:"),
        "attestation_manifest_sha256": attestation_digest.removeprefix("sha256:"),
        "attestation_predicates": [item["predicate_type"] for item in statements],
        "config_sha256": config_digest.removeprefix("sha256:"),
        "gateway_image_reference": gateway,
        "build_log": build_log,
        "visibility": "private",
        "build_authority": "local-socket",
        "buildkit_image_reference": BUILDKIT_IMAGE,
        "dockerfile_frontend_reference": DOCKERFILE_FRONTEND,
        "platform": "linux/amd64",
        "provenance": "max",
        "provenance_version": "v1",
        "sbom": True,
    }
    _write_new(
        arguments.evidence_dir / "receipt.json",
        (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )
    admission = {
        "schema_version": "milk.private-image-admission.v1",
        "artifact": arguments.artifact,
        "repository": repository,
        "image_reference": immutable_reference,
        "source_repository": SOURCE_REPOSITORY,
        "source_commit": arguments.source_commit,
        "source_context_method": "git-archive-tar-v1",
        "source_context_sha256": arguments.source_context_sha256,
        "gateway_image_reference": gateway,
        "index_sha256": index_digest.removeprefix("sha256:"),
        "amd64_manifest_sha256": child_digest.removeprefix("sha256:"),
        "config_sha256": config_digest.removeprefix("sha256:"),
        "attestation_manifest_sha256": attestation_digest.removeprefix("sha256:"),
        "attestations": statements,
        "platform": "linux/amd64",
        "visibility": "private",
        "builder": {
            "authority": "local-socket",
            "driver": "docker-container",
            "endpoint_kind": "local-socket",
            "buildkit_image_reference": BUILDKIT_IMAGE,
            "buildkit_version": BUILDKIT_VERSION,
            "dockerfile_frontend_reference": DOCKERFILE_FRONTEND,
            "provenance_mode": "max",
            "provenance_version": "v1",
            "sbom": True,
        },
    }
    admission_raw = json.dumps(admission, sort_keys=True, separators=(",", ":")) + "\n"
    _write_new(arguments.evidence_dir / "admission.json", admission_raw.encode())
    return immutable_reference, hashlib.sha256(admission_raw.encode()).hexdigest()


def _parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", required=True, choices=tuple(ARTIFACTS))
    parser.add_argument("--tagged-reference", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-date-epoch", type=int, required=True)
    parser.add_argument("--source-context-sha256", required=True)
    parser.add_argument("--gateway-image-reference")
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--docker-config", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--registry-token-stdin", action="store_true", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    try:
        arguments = _parse_arguments(argv)
        raw_token = sys.stdin.buffer.read(MAX_TOKEN_BYTES + 1)
        if len(raw_token) > MAX_TOKEN_BYTES:
            raise ValueError("registry credential exceeds its byte limit")
        token_bytes = raw_token.strip()
        if (
            not token_bytes
            or any(byte < 33 or byte > 126 for byte in token_bytes)
            or b"\n" in token_bytes
            or b"\r" in token_bytes
        ):
            raise ValueError("registry credential is invalid")
        image_reference, admission_sha256 = verify(
            arguments, token_bytes.decode("ascii")
        )
        print(image_reference + "\t" + admission_sha256)
        return 0
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError) as error:
        print(f"verify-private-image: {error}", file=sys.stderr)
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
