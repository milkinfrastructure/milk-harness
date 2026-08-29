from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import re

from milk_harness.evidence import HEX64, canonical_json
from milk_harness.scheduler import (
    GATEWAY_DEPLOYMENT_FIELDS,
    IMAGE_REPOSITORIES,
    PROVIDER_RUNTIME_FIELDS,
    PROVIDER_SECRET_FIELDS,
    _strict_object,
    _validated_run_manifest,
    store_location_sha256,
)


MAX_EVAL_BYTES = 48 * 1024
LOCATION_NAMES = ("evidence", "ops_log", "create_authority", "route_evidence")
SAFE_ACCOUNT = re.compile(r"[^\s]{1,128}\Z")


def _location(value, label):
    if not isinstance(value, dict) or set(value) != {"account_id", "bucket"}:
        raise ValueError(f"{label} location is invalid")
    store_location_sha256(value.get("account_id"), value.get("bucket"))
    return value


def validate_eval_document(raw, eval_id, harness_source_commit, root):
    value = _strict_object(raw, limit=MAX_EVAL_BYTES)
    canonical = canonical_json(value)
    if raw != canonical:
        raise ValueError("eval document must be canonical JSON")
    if HEX64.fullmatch(eval_id or "") is None:
        raise ValueError("eval ID is invalid")
    if (
        set(value)
        != {
            "schema_version",
            "manifest_sha256",
            "manifest",
            "gateway_config",
            "cloudflare_account_id",
            "locations",
        }
        or value.get("schema_version") != "milk.eval.v1"
        or HEX64.fullmatch(value.get("manifest_sha256") or "") is None
        or SAFE_ACCOUNT.fullmatch(value.get("cloudflare_account_id") or "") is None
        or not isinstance(value.get("manifest"), dict)
        or not isinstance(value.get("gateway_config"), dict)
        or not isinstance(value.get("locations"), dict)
        or set(value["locations"]) != set(LOCATION_NAMES)
    ):
        raise ValueError("eval document is invalid")

    manifest_raw = canonical_json(value["manifest"])
    if hashlib.sha256(manifest_raw).hexdigest() != value["manifest_sha256"]:
        raise ValueError("eval manifest SHA-256 differs")
    manifest = _validated_run_manifest(
        manifest_raw,
        value["manifest_sha256"],
        harness_source_commit,
        root,
    )
    if (
        manifest["campaign_id"] != eval_id
        or value["gateway_config"].get("eval_id") != eval_id
    ):
        raise ValueError("eval ID differs from the campaign or gateway config")
    gateway_raw = canonical_json(value["gateway_config"])
    if hashlib.sha256(gateway_raw).hexdigest() != manifest["gateway_config_sha256"]:
        raise ValueError("eval gateway config SHA-256 differs")

    stores = value["gateway_config"].get("stores")
    if not isinstance(stores, dict) or set(stores) != {"capture", "control", "routes"}:
        raise ValueError("eval gateway store locations are invalid")
    gateway_locations = {}
    for name in ("capture", "control", "routes"):
        store = stores[name]
        if not isinstance(store, dict) or store.get("type") != "cloudflare_r2":
            raise ValueError("eval gateway store locations are invalid")
        gateway_locations[name] = _location(
            {key: item for key, item in store.items() if key != "type"},
            f"gateway {name}",
        )
    extra_locations = {
        name: _location(value["locations"][name], name)
        for name in LOCATION_NAMES
    }
    identities = {
        (location["account_id"], location["bucket"])
        for location in (*gateway_locations.values(), *extra_locations.values())
    }
    if len(identities) != len(gateway_locations) + len(extra_locations):
        raise ValueError("eval store locations must be pairwise distinct")
    return value, manifest_raw, gateway_raw


def _single_line(value, name):
    text = str(value)
    if "\n" in text or "\r" in text:
        raise ValueError(f"{name} cannot be written to the workflow environment")
    return text


def materialized_environment(
    value,
    manifest_sha256,
    eval_config_sha256,
    manifest_path,
    gateway_path,
):
    manifest = value["manifest"]
    gateway = value["gateway_config"]
    runtime = manifest["provider_runtime"]
    secret_names = manifest["provider_secret_names"]
    deployment = manifest["gateway_deployment"]
    locations = value["locations"]
    stores = gateway["stores"]
    env = {
        "MILK_CONFIRMED_RUN_CONFIG_SHA256": manifest_sha256,
        "MILK_EVAL_CONFIG_SHA256": eval_config_sha256,
        "MILK_CONFIRMED_RUN_CONFIG_PATH": str(Path(manifest_path).resolve()),
        "MILK_GATEWAY_CONFIG_PATH": str(Path(gateway_path).resolve()),
        "MILK_CAMPAIGN_ID": manifest["campaign_id"],
        "MILK_BASETEN_PROJECT_ID": manifest["provider_project_id"],
        "MILK_SCOPE_PREFIX": manifest["scope_prefix"],
        "MILK_IMAGE_RELEASE_SHA256": manifest["image_release_sha256"],
        "CLOUDFLARE_ACCOUNT_ID": value["cloudflare_account_id"],
    }
    env.update(
        {f"MILK_{name.upper()}_IMAGE": manifest["images"][name] for name in IMAGE_REPOSITORIES}
    )
    env.update(
        {
            f"MILK_{name.upper()}": runtime[name]
            for name in PROVIDER_RUNTIME_FIELDS
        }
    )
    env.update(
        {
            f"MILK_BASETEN_{name.upper()}_SECRET": secret_names[name] or ""
            for name in PROVIDER_SECRET_FIELDS
        }
    )
    deployment_environment = {
        "source_commit": "MILK_GATEWAY_SOURCE_COMMIT",
        "image_admission_sha256": "MILK_GATEWAY_IMAGE_ADMISSION_SHA256",
        "release_sha256": "MILK_GATEWAY_RELEASE_SHA256",
        "application_id": "MILK_GATEWAY_CONTAINER_APPLICATION_ID",
        "application_version": "MILK_GATEWAY_CONTAINER_APPLICATION_VERSION",
        "container_image": "MILK_GATEWAY_CONTAINER_IMAGE",
        "worker_version_id": "MILK_GATEWAY_WORKER_VERSION_ID",
    }
    if set(deployment_environment) != set(GATEWAY_DEPLOYMENT_FIELDS):
        raise AssertionError("gateway deployment environment mapping is incomplete")
    env.update(
        {environment: deployment[name] for name, environment in deployment_environment.items()}
    )

    def bind(prefix, location):
        env[prefix + "ACCOUNT_ID"] = location["account_id"]
        env[prefix + "BUCKET"] = location["bucket"]

    bind("MILK_CONTROL_R2_", stores["control"])
    bind("MILK_EVIDENCE_R2_", locations["evidence"])
    bind("MILK_OPS_LOG_R2_", locations["ops_log"])
    bind("MILK_CREATE_AUTHORITY_WRITE_R2_", locations["create_authority"])
    bind("MILK_CREATE_AUTHORITY_READ_R2_", locations["create_authority"])
    bind("MILK_GATEWAY_INGEST_CONTROL_R2_", stores["control"])
    bind("MILK_GATEWAY_INGEST_ROUTE_R2_", stores["routes"])
    bind("MILK_ROUTE_EVIDENCE_R2_", locations["route_evidence"])
    return {name: _single_line(item, name) for name, item in env.items()}


def _write_new(path, raw):
    path = Path(path)
    if path.exists() or path.is_symlink():
        raise ValueError("eval materialization output must be new")
    with path.open("xb") as stream:
        stream.write(raw)
    path.chmod(0o400)


def _append(path, values):
    path = Path(path)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise ValueError("workflow output path is unsafe")
    with path.open("a", encoding="utf-8") as stream:
        for name, value in values.items():
            stream.write(f"{name}={value}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate and materialize one Milk eval")
    parser.add_argument("--document", required=True)
    parser.add_argument("--eval-id", required=True)
    parser.add_argument("--harness-source-commit", required=True)
    parser.add_argument("--root", default=".")
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--gateway-config-output", required=True)
    parser.add_argument("--github-env", required=True)
    parser.add_argument("--github-output", required=True)
    arguments = parser.parse_args(argv)

    document = Path(arguments.document)
    if document.is_symlink() or not document.is_file():
        raise ValueError("eval document must be a regular file")
    raw = document.read_bytes()
    value, manifest_raw, gateway_raw = validate_eval_document(
        raw,
        arguments.eval_id,
        arguments.harness_source_commit,
        arguments.root,
    )
    _write_new(arguments.manifest_output, manifest_raw)
    _write_new(arguments.gateway_config_output, gateway_raw)
    _append(
        arguments.github_env,
        materialized_environment(
            value,
            value["manifest_sha256"],
            hashlib.sha256(raw).hexdigest(),
            arguments.manifest_output,
            arguments.gateway_config_output,
        ),
    )
    _append(
        arguments.github_output,
        {"gateway_source_commit": value["manifest"]["gateway_deployment"]["source_commit"]},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
