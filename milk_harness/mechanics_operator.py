from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import urllib.error

from milk_harness.evidence import (
    HEX64,
    MAX_OBJECT_BYTES,
    R2EvidenceStore,
    canonical_json,
)
from milk_harness.scheduler import (
    PRODUCTION_PROOF_BUDGET_FIXED as FIXED_PROOF_BUDGET,
    store_identity_sha256,
)


MECHANICS_EVAL_ID = (
    "959caacb397004bf3e60f13613da50f4ed3160a65d18b178c3d996398e29b5a0"
)
ENDPOINT = "https://api.dragontales.milkinfrastructure.com/v1"
SDK_PROOF_CONTRACT = {
    "baseline_requests": 322,
    "candidate_requests": 2,
    "generated_health_timeout_ms": 30_000,
    "generated_mechanics_requests": 320,
    "generated_request_timeout_ms": 30_000,
    "max_sdk_requests": 324,
    "model": "gpt-5.4",
    "saturation_max_completion_tokens": 3_840,
    "short_max_completion_tokens": 128,
}
PROOF_CONTRACT_SHA256 = hashlib.sha256(
    json.dumps(SDK_PROOF_CONTRACT, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
if FIXED_PROOF_BUDGET["proof_contract_sha256"] != PROOF_CONTRACT_SHA256:
    raise AssertionError("scheduler and gateway SDK proof contracts differ")
SOURCE_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
RECEIPT_TIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")
TRAFFIC_KEY = re.compile(
    r"dt_live_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{12}_[A-Za-z0-9._~-]{16,256}\Z"
)
COHORT_ID = re.compile(r"[A-Za-z0-9._~-]{1,128}\Z")
INTENT_PREFIX = f"proofs/v1/generated-mechanics/{MECHANICS_EVAL_ID}"
INTENT_KEY = f"{INTENT_PREFIX}/intent.json"
RECEIPT_KEY = f"{INTENT_PREFIX}/receipt.json"
RUN_TIMEOUT_SECONDS = 45 * 60


def _strict_object(raw, label):
    if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_OBJECT_BYTES:
        raise ValueError(f"{label} must be bounded bytes")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"{label} contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict) or canonical_json(value) != raw:
        raise ValueError(f"{label} must be one exact canonical object")
    return value


def _utc(value, label):
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be UTC")
    if value.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC")
    return value.astimezone(dt.timezone.utc)


def _utc_text(value):
    return _utc(value, "clock").isoformat(timespec="seconds").replace("+00:00", "Z")


def _receipt_time(value, label):
    if not isinstance(value, str) or RECEIPT_TIME.fullmatch(value) is None:
        raise ValueError(f"generated mechanics {label} is invalid")
    return dt.datetime.fromisoformat(value[:-1] + "+00:00")


def _missing(store, key):
    try:
        return store.get(key)
    except (FileNotFoundError, urllib.error.HTTPError) as error:
        if isinstance(error, urllib.error.HTTPError) and error.code != 404:
            raise
        return None


def _sha256_file(path, label):
    return hashlib.sha256(_read_exact(path, label)).hexdigest()


def _regular_file(path, label):
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be an absolute regular file")
    return path


def _mechanics_inputs(config_path, credential_path, expected_config_sha256):
    config_raw = _read_exact(config_path, "gateway config")
    if hashlib.sha256(config_raw).hexdigest() != expected_config_sha256:
        raise ValueError("gateway config SHA-256 differs")
    config = _strict_object(config_raw, "gateway config")
    credential_path = Path(credential_path)
    if (
        not credential_path.is_absolute()
        or credential_path.is_symlink()
        or not credential_path.is_file()
    ):
        raise ValueError("gateway credential must be an absolute regular file")
    metadata = credential_path.stat()
    if (
        metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o077
        or not 1 <= metadata.st_size <= 8_192
    ):
        raise ValueError("gateway credential must be one owner-only file")
    credential = _strict_object(credential_path.read_bytes(), "gateway credential")
    api_key = credential.get("api_key")
    cohort_id = credential.get("cohort_id")
    api_key_text = api_key if isinstance(api_key, str) else ""
    cohort_id_text = cohort_id if isinstance(cohort_id, str) else ""
    expected_key = {
        "api_key_sha256": hashlib.sha256(api_key_text.encode()).hexdigest(),
        "capture_allowed": True,
        "cohort_id": cohort_id_text,
    }
    if (
        set(credential) != {"api_key", "cohort_id", "model"}
        or TRAFFIC_KEY.fullmatch(api_key_text) is None
        or COHORT_ID.fullmatch(cohort_id_text) is None
        or credential.get("model") != FIXED_PROOF_BUDGET["openai_model"]
        or config.get("eval_id") != MECHANICS_EVAL_ID
        or not isinstance(config.get("traffic_keys"), list)
        or config["traffic_keys"].count(expected_key) != 1
    ):
        raise ValueError("gateway mechanics credential is not an admitted capture key")
    return credential_path


def _manifest(raw, confirmed_sha256):
    value = _strict_object(raw, "confirmed run config")
    digest = hashlib.sha256(raw).hexdigest()
    if HEX64.fullmatch(confirmed_sha256 or "") is None or digest != confirmed_sha256:
        raise ValueError("confirmed run config SHA-256 differs")
    deployment = value.get("gateway_deployment")
    stores = value.get("store_identity_sha256s")
    proof = value.get("proof_budget")
    if (
        value.get("schema_version") != "milk.confirmed-production-run-config.v6"
        or value.get("campaign_id") != MECHANICS_EVAL_ID
        or not isinstance(value.get("scope_prefix"), str)
        or value["scope_prefix"].split("/")[2:3] != [MECHANICS_EVAL_ID]
        or not isinstance(deployment, dict)
        or SOURCE_COMMIT.fullmatch(deployment.get("source_commit") or "") is None
        or HEX64.fullmatch(value.get("gateway_config_sha256") or "") is None
        or not isinstance(stores, dict)
        or HEX64.fullmatch(stores.get("route_evidence") or "") is None
        or not isinstance(proof, dict)
        or set(proof) != set(FIXED_PROOF_BUDGET) | {"gateway_tool_sha256"}
        or {name: proof.get(name) for name in FIXED_PROOF_BUDGET}
        != FIXED_PROOF_BUDGET
        or HEX64.fullmatch(proof.get("gateway_tool_sha256") or "") is None
    ):
        raise ValueError("confirmed mechanics proof contract is invalid")
    return value, digest


def _intent(raw, expected):
    value = _strict_object(raw, "generated mechanics intent")
    if (
        set(value) != set(expected) | {"created_at"}
        or {name: value.get(name) for name in expected} != expected
        or not isinstance(value.get("created_at"), str)
    ):
        raise ValueError("generated mechanics intent differs")
    try:
        parsed = dt.datetime.fromisoformat(value["created_at"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("generated mechanics intent timestamp is invalid") from error
    if _utc_text(parsed) != value["created_at"]:
        raise ValueError("generated mechanics intent timestamp is invalid")
    return parsed


def _receipt(raw, manifest, current, intent_created_at):
    value = _strict_object(raw, "generated mechanics receipt")
    exact = {
        "schema_version": "milk.official-openai-sdk-generated-mechanics.v2",
        "eval_id": MECHANICS_EVAL_ID,
        "gateway_config_sha256": manifest["gateway_config_sha256"],
        "tool_sha256": manifest["proof_budget"]["gateway_tool_sha256"],
        "sdk": "openai-node",
        "sdk_version": "6.33.0",
        "endpoint_sha256": hashlib.sha256(ENDPOINT.encode()).hexdigest(),
        "model": "gpt-5.4",
        "model_sha256": hashlib.sha256(b"gpt-5.4").hexdigest(),
        "proof_contract_sha256": PROOF_CONTRACT_SHA256,
        "proof_step": "generated_mechanics",
        "request_timeout_ms": 30_000,
        "concurrency": 4,
        "candidates_scanned": 1_692,
        "planned": 320,
        "attempted": 320,
        "sdk_request_count": 320,
        "baseline_request_count": 320,
        "candidate_request_count": 0,
        "max_completion_tokens": 128,
        "successful": 320,
        "failed": 0,
        "transported": 320,
        "unique_trace_ids": 320,
        "first_partition_counts": {"train": 50, "dev": 73, "calibration": 128},
        "retry_partition_counts": {"train": 13, "dev": 18, "calibration": 38},
        "total_partition_counts": {"train": 63, "dev": 91, "calibration": 166},
        "http_status_counts": {"200": 320},
        "route_revision": "openai-baseline-v1",
        "postflight_health_succeeded": True,
        "content_retained": False,
        "succeeded": True,
    }
    digest_fields = {
        "traffic_cohort_sha256",
        "request_set_sha256",
        "response_set_sha256",
        "trace_set_sha256",
        "preflight_health_sha256",
        "postflight_health_sha256",
    }
    if (
        set(value) != set(exact) | digest_fields | {"started_at", "completed_at"}
        or any(value.get(name) != expected for name, expected in exact.items())
        or any(HEX64.fullmatch(value.get(name) or "") is None for name in digest_fields)
    ):
        raise ValueError("generated mechanics receipt is invalid")
    started = _receipt_time(value.get("started_at"), "start time")
    completed = _receipt_time(value.get("completed_at"), "completion time")
    if (
        started < intent_created_at
        or completed < started
        or completed > current + dt.timedelta(minutes=5)
    ):
        raise ValueError("generated mechanics receipt interval is invalid")
    return value


def _reject_ambient_authorities(environ):
    allowed = "MILK_ROUTE_EVIDENCE_R2_"
    forbidden_prefixes = (
        "AWS_",
        "BASETEN_",
        "MODAL_",
        "OPENAI_",
        "MILK_CAPTURE_",
        "MILK_CONTROL_",
        "MILK_CREATE_AUTHORITY_",
        "MILK_GATEWAY_",
        "MILK_GHCR_",
        "MILK_JOB_",
        "MILK_PROVIDER_",
        "MILK_ROUTE_",
    )
    forbidden_exact = {"CLOUDFLARE_API_KEY", "CLOUDFLARE_API_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"}
    for name, value in environ.items():
        if not value or name.startswith(allowed):
            continue
        if name in forbidden_exact or name.startswith(forbidden_prefixes):
            raise ValueError("mechanics operator must not receive provider authority")


def run_once(
    store,
    manifest_raw,
    confirmed_manifest_sha256,
    *,
    confirmed_eval_sha256,
    gateway_source_root,
    gateway_source_commit,
    gateway_config_path,
    gateway_config_sha256,
    gateway_credential_path,
    node_binary,
    route_evidence_account_id,
    route_evidence_bucket,
    route_evidence_access_key_id,
    runner=subprocess.run,
    now=lambda: dt.datetime.now(dt.timezone.utc),
    environ=None,
    home=None,
):
    current = _utc(now(), "clock")
    _reject_ambient_authorities(os.environ if environ is None else environ)
    manifest, manifest_sha256 = _manifest(manifest_raw, confirmed_manifest_sha256)
    if HEX64.fullmatch(confirmed_eval_sha256 or "") is None:
        raise ValueError("confirmed outer eval SHA-256 is invalid")
    source_root = Path(gateway_source_root)
    tool = source_root / "tools" / "openai-production-smoke.mjs"
    tool_sha256 = _sha256_file(tool, "gateway mechanics tool")
    actual_store_identity = store_identity_sha256(
        route_evidence_account_id,
        route_evidence_bucket,
        route_evidence_access_key_id,
    )
    if (
        not source_root.is_absolute()
        or source_root.is_symlink()
        or not source_root.is_dir()
        or gateway_source_commit != manifest["gateway_deployment"]["source_commit"]
        or gateway_config_sha256 != manifest["gateway_config_sha256"]
        or tool_sha256 != manifest["proof_budget"]["gateway_tool_sha256"]
        or actual_store_identity
        != manifest["store_identity_sha256s"]["route_evidence"]
    ):
        raise ValueError("actual mechanics identities differ from the confirmed run config")
    expected_intent = {
        "schema_version": "milk.generated-mechanics-intent.v1",
        "confirmed_eval_sha256": confirmed_eval_sha256,
        "manifest_sha256": manifest_sha256,
        "eval_id": MECHANICS_EVAL_ID,
        "gateway_source_commit": gateway_source_commit,
        "gateway_tool_sha256": tool_sha256,
        "gateway_config_sha256": gateway_config_sha256,
        "route_evidence_store_identity_sha256": actual_store_identity,
        "proof_contract_sha256": PROOF_CONTRACT_SHA256,
        "endpoint_sha256": hashlib.sha256(ENDPOINT.encode()).hexdigest(),
    }
    stored_intent_raw = _missing(store, INTENT_KEY)
    stored_receipt_raw = _missing(store, RECEIPT_KEY)
    if stored_receipt_raw is not None:
        if stored_intent_raw is None:
            raise ValueError("generated mechanics receipt has no intent")
        intent_created_at = _intent(stored_intent_raw, expected_intent)
        return _receipt(
            stored_receipt_raw,
            manifest,
            current,
            intent_created_at,
        )
    if stored_intent_raw is not None:
        _intent(stored_intent_raw, expected_intent)
        raise RuntimeError("generated mechanics intent is ambiguous; refusing replay")
    node_binary = _regular_file(node_binary, "Node executable")
    if not os.access(node_binary, os.X_OK):
        raise ValueError("Node executable is not executable")
    gateway_credential_path = _mechanics_inputs(
        gateway_config_path,
        gateway_credential_path,
        gateway_config_sha256,
    )
    if home is None:
        raise ValueError("sanitized runner HOME is required")
    runner_home = Path(home)
    if not runner_home.is_absolute() or runner_home.is_symlink() or not runner_home.is_dir():
        raise ValueError("sanitized runner HOME must be an absolute directory")
    intent_raw = canonical_json({**expected_intent, "created_at": _utc_text(current)})
    if not store.create(INTENT_KEY, intent_raw, "application/json"):
        stored_intent_raw = store.get(INTENT_KEY)
        intent_created_at = _intent(stored_intent_raw, expected_intent)
        stored_receipt_raw = _missing(store, RECEIPT_KEY)
        if stored_receipt_raw is not None:
            return _receipt(
                stored_receipt_raw,
                manifest,
                current,
                intent_created_at,
            )
        raise RuntimeError("generated mechanics intent is ambiguous; refusing replay")
    completed = runner(
        [
            str(node_binary),
            str(tool),
            ENDPOINT,
            str(gateway_credential_path),
            gateway_config_sha256,
            "--generated-mechanics",
        ],
        cwd=str(source_root),
        env={"HOME": str(runner_home), "LANG": "C.UTF-8", "PATH": "/usr/bin:/bin"},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=RUN_TIMEOUT_SECONDS,
        check=False,
        shell=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("generated mechanics subprocess failed; intent is ambiguous")
    receipt_raw = completed.stdout
    receipt = _receipt(
        receipt_raw,
        manifest,
        _utc(now(), "clock"),
        current,
    )
    if not store.create(RECEIPT_KEY, receipt_raw, "application/json"):
        if store.get(RECEIPT_KEY) != receipt_raw:
            raise ValueError("stored generated mechanics receipt differs")
    if store.get(RECEIPT_KEY) != receipt_raw:
        raise RuntimeError("generated mechanics receipt readback differs")
    return receipt


def _read_exact(path, label):
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be an absolute regular file")
    raw = path.read_bytes()
    if not 1 <= len(raw) <= MAX_OBJECT_BYTES:
        raise ValueError(f"{label} must be bounded bytes")
    return raw


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the generated cloud-mechanics proof once")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--confirm-manifest-sha256", required=True)
    parser.add_argument("--confirm-eval-sha256", required=True)
    parser.add_argument("--gateway-source-root", required=True)
    parser.add_argument("--gateway-source-commit", required=True)
    parser.add_argument("--gateway-config", required=True)
    parser.add_argument("--gateway-config-sha256", required=True)
    parser.add_argument("--gateway-credential", required=True)
    parser.add_argument("--node", required=True)
    arguments = parser.parse_args(argv)
    try:
        store = R2EvidenceStore.from_environment("MILK_ROUTE_EVIDENCE_R2_")
        with tempfile.TemporaryDirectory(prefix="milk-mechanics-") as scratch:
            result = run_once(
                store,
                _read_exact(arguments.manifest, "confirmed run config"),
                arguments.confirm_manifest_sha256,
                confirmed_eval_sha256=arguments.confirm_eval_sha256,
                gateway_source_root=arguments.gateway_source_root,
                gateway_source_commit=arguments.gateway_source_commit,
                gateway_config_path=arguments.gateway_config,
                gateway_config_sha256=arguments.gateway_config_sha256,
                gateway_credential_path=arguments.gateway_credential,
                node_binary=arguments.node,
                route_evidence_account_id=store.host.removesuffix(
                    ".r2.cloudflarestorage.com"
                ),
                route_evidence_bucket=store.bucket,
                route_evidence_access_key_id=store.access_key_id,
                home=scratch,
            )
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        UnicodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"milk-mechanics: {error}", file=os.sys.stderr)
        return 64
    os.sys.stdout.buffer.write(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
