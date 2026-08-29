from __future__ import annotations

import base64
import datetime as dt
import hashlib
import importlib
import json
import re
from urllib.parse import urlsplit

from deploy.baseten import winner as winner_contract
from deploy.baseten.adapter import (
    STUDENT_MODEL_MOUNT,
    TEACHER_MODEL_MOUNT,
    immutable_ghcr_image,
)
from milk_harness.evidence import HEX64, canonical_json, create_same
from milk_harness.logs import LogArchive
from milk_harness.provider_acceptance import (
    encode as encode_provider_acceptance,
    sha256 as provider_acceptance_sha256,
    validate as validate_provider_acceptance,
    validate_route_retirement,
)


MODAL_VERSION = "1.5.4"
MAX_CPU_PHYSICAL_CORES = 8
MAX_MEMORY_MIB = 65_536
MAX_TERMINATE_ATTEMPTS = 3
MAX_LOG_BYTES = 8 * 1024 * 1024
MAX_LOG_RECORDS = 10_000
MAX_ADMISSION_LOG_RECORDS = 1_000
MAX_PROBE_OUTPUT_BYTES = 64 * 1024
WINNER_CANARY_SECONDS = 15 * 60
WINNER_ZERO_ROUTE_SECONDS = 60
WINNER_ROUTE_AUTHORITY_SECONDS = (
    WINNER_CANARY_SECONDS + WINNER_ZERO_ROUTE_SECONDS
)
WINNER_PORT = 8000
WINNER_READY_TIMEOUT_SECONDS = 300
WINNER_TUNNEL_TIMEOUT_SECONDS = 50
WINNER_PROBE_TIMEOUT_SECONDS = 120
WINNER_API_KEY_ENVIRONMENT = "DRAGONTALES_CANDIDATE_API_KEY"
TERMINALIZATION_GRACE_SECONDS = 300
OPAQUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
ENVIRONMENT_KEY = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
SENSITIVE_ENVIRONMENT_KEY = re.compile(
    r"(?:API_KEY|AUTHORIZATION|CREDENTIAL|PASSWORD|SECRET|TOKEN)"
)
WINNER_CLAIM_KEYS = (
    "schema_version",
    "scope",
    "student_job_id",
    "student_claim_sha256",
    "student_result_sha256",
    "winner",
    "model_manifest",
    "dev_receipt",
    "student_branch_runtime_image_reference",
    "provider_binding_sha256",
    "authority",
    "claimed_at",
    "expires_at",
)
WINNER_AUTHORITY_KEYS = (
    "schema_version",
    "provider_policy",
    "provider_terms_sha256",
    "student_branch_runtime_image_reference",
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
)
SCOPE_UUID_KEYS = ("tenant_id", "project_id", "environment_id", "workload_id")
SCOPE_KEYS = SCOPE_UUID_KEYS + ("eval_id",)


def _digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _stored_json(store, key, label):
    raw = store.get(key)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"stored {label} is not strict JSON") from error
    if not isinstance(value, dict) or raw != canonical_json(value):
        raise ValueError(f"stored {label} is not canonical")
    return value


def _strict_object(raw, maximum, label, *, line=False):
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


def _utc_text(value):
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("clock must return UTC")
    return (
        value.astimezone(dt.timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _winner_utc(value, label):
    parsed = _utc(value, label) if isinstance(value, str) else value
    if (
        not isinstance(parsed, dt.datetime)
        or parsed.tzinfo is None
        or parsed.utcoffset() != dt.timedelta(0)
        or parsed.microsecond
    ):
        raise ValueError(f"{label} must be whole-second UTC")
    return parsed.astimezone(dt.timezone.utc)


def _winner_utc_text(value):
    return _winner_utc(value, "winner timestamp").isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _opaque(value, label):
    if not isinstance(value, str) or OPAQUE.fullmatch(value) is None:
        raise ValueError(f"{label} is invalid")
    return value


def _hex64(value, label):
    if not isinstance(value, str) or HEX64.fullmatch(value) is None:
        raise ValueError(f"{label} must be lowercase hex64")
    return value


def _resource(value, label, *, volume=False):
    required = {"name", "object_id"} | (
        {"mount_path", "read_only"} if volume else {"required_keys"}
    )
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError(f"Modal {label} is invalid")
    _opaque(value["name"], f"Modal {label} name")
    _opaque(value["object_id"], f"Modal {label} object ID")
    if volume:
        path = value["mount_path"]
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or len(path) > 255
            or any(part in {"", ".", ".."} for part in path.split("/")[1:])
            or value["read_only"] is not True
        ):
            raise ValueError("Modal volume mount is invalid")
        return
    keys = value["required_keys"]
    if (
        not isinstance(keys, list)
        or not 1 <= len(keys) <= 8
        or keys != sorted(set(keys))
        or any(
            not isinstance(key, str) or ENVIRONMENT_KEY.fullmatch(key) is None
            for key in keys
        )
    ):
        raise ValueError(f"Modal {label} required keys are invalid")


def _sandbox_inputs(command, environment):
    if (
        not isinstance(command, (list, tuple))
        or not 1 <= len(command) <= 32
        or any(
            not isinstance(item, str)
            or not item
            or "\0" in item
            or len(item.encode()) > 16 * 1024
            for item in command
        )
    ):
        raise ValueError("Modal sandbox command is invalid")
    if (
        not isinstance(environment, dict)
        or len(environment) > 32
        or any(
            not isinstance(key, str)
            or ENVIRONMENT_KEY.fullmatch(key) is None
            or SENSITIVE_ENVIRONMENT_KEY.search(key) is not None
            or not isinstance(value, str)
            or not value
            or len(value.encode()) > 1024
            or "\0" in value
            for key, value in environment.items()
        )
    ):
        raise ValueError("Modal sandbox environment is invalid")
    return list(command), dict(environment)


def validate_acceptance(value):
    validate_provider_acceptance(value)
    if value["selection"]["selected_provider"] != "modal":
        raise ValueError("Modal provider selection is invalid")
    return value


def _unit(acceptance, ordinal, value):
    if not isinstance(value, dict):
        raise ValueError("Modal execution unit is invalid")
    schema = acceptance["schema_version"]
    if schema == "milk.winner-provider-acceptance.v1":
        if ordinal != 0 or set(value) != {
            "kind",
            "student_job_id",
            "student_result_sha256",
            "winner",
        } or value.get("kind") != "student_winner_deployment":
            raise ValueError("Modal winner execution unit is invalid")
        _hex64(value.get("student_job_id"), "winner student job ID")
        _hex64(value.get("student_result_sha256"), "winner student result SHA-256")
        if value.get("winner") not in {"bf16", "dynamic_fp8", "static_fp8"}:
            raise ValueError("Modal winner variant is invalid")
        return
    operation = acceptance["operation"]
    if operation["kind"] in {"teacher_run", "student_train_merge"}:
        if ordinal != 0 or value != operation:
            raise ValueError("Modal execution unit differs from its GPU operation")
        return
    branches = operation["branches"]
    if not 0 <= ordinal < len(branches):
        raise ValueError("Modal fanout execution ordinal is invalid")
    branch = branches[ordinal]
    expected = {
        "kind": "student_branch",
        "student_job_id": operation["student_job_id"],
        "branch_id": branch["branch_id"],
        "variant": branch["variant"],
        "max_gpu_seconds": branch["max_gpu_seconds"],
    }
    if value != expected:
        raise ValueError("Modal fanout execution unit differs from its branch")


def validate_definition(acceptance, value):
    validate_acceptance(acceptance)
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "campaign_id",
        "run_id",
        "ordinal",
        "provider_acceptance_sha256",
        "unit",
        "runtime_image_reference",
        "sandbox",
        "resources",
    } or value["schema_version"] != "milk.modal-execution-definition.v1":
        raise ValueError("Modal execution definition is invalid")
    if (
        value["campaign_id"] != acceptance["campaign_id"]
        or value["run_id"] != acceptance["run_id"]
        or value["provider_acceptance_sha256"]
        != provider_acceptance_sha256(acceptance)
        or type(value["ordinal"]) is not int
        or not 0 <= value["ordinal"] <= 2
    ):
        raise ValueError("Modal execution definition identity is invalid")
    _unit(acceptance, value["ordinal"], value["unit"])
    image = immutable_ghcr_image(value["runtime_image_reference"])
    image_repository = {
        "teacher_run": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:",
        "student_train_merge": "ghcr.io/milkinfrastructure/milk-student-train@sha256:",
        "student_branch": "ghcr.io/milkinfrastructure/milk-student-branch@sha256:",
        "student_winner_deployment": "ghcr.io/milkinfrastructure/milk-student-branch@sha256:",
    }[value["unit"]["kind"]]
    if not image.startswith(image_repository):
        raise ValueError("Modal runtime image repository is invalid")

    sandbox = value["sandbox"]
    if not isinstance(sandbox, dict) or set(sandbox) != {
        "cpu_physical_cores",
        "memory_mib",
        "timeout_seconds",
        "workdir",
        "command_sha256",
        "environment_sha256",
    }:
        raise ValueError("Modal sandbox authority is invalid")
    if (
        type(sandbox["cpu_physical_cores"]) is not int
        or not 1 <= sandbox["cpu_physical_cores"] <= MAX_CPU_PHYSICAL_CORES
        or type(sandbox["memory_mib"]) is not int
        or not 1 <= sandbox["memory_mib"] <= MAX_MEMORY_MIB
        or type(sandbox["timeout_seconds"]) is not int
        or not 60 <= sandbox["timeout_seconds"] <= acceptance["max_wall_seconds"]
        or _utc(acceptance["create_not_after"], "provider create deadline")
        + dt.timedelta(seconds=sandbox["timeout_seconds"])
        > _utc(acceptance["provider_not_after"], "provider deadline")
        or not isinstance(sandbox["workdir"], str)
        or not sandbox["workdir"].startswith("/")
        or len(sandbox["workdir"]) > 255
        or any(part in {"", ".", ".."} for part in sandbox["workdir"].split("/")[1:])
    ):
        raise ValueError("Modal sandbox bounds are invalid")
    _hex64(sandbox["command_sha256"], "Modal command SHA-256")
    _hex64(sandbox["environment_sha256"], "Modal environment SHA-256")

    resources = value["resources"]
    if not isinstance(resources, dict) or set(resources) != {
        "registry_secret",
        "secrets",
        "volumes",
    }:
        raise ValueError("Modal accepted resources are invalid")
    _resource(resources["registry_secret"], "registry secret")
    if resources["registry_secret"]["required_keys"] != [
        "REGISTRY_PASSWORD",
        "REGISTRY_USERNAME",
    ]:
        raise ValueError("Modal registry secret keys are invalid")
    secrets = resources["secrets"]
    volumes = resources["volumes"]
    if (
        not isinstance(secrets, list)
        or not 1 <= len(secrets) <= 8
        or not isinstance(volumes, list)
        or len(volumes) > 1
    ):
        raise ValueError("Modal accepted named resources are invalid")
    for secret in secrets:
        _resource(secret, "secret")
    for volume in volumes:
        _resource(volume, "volume", volume=True)
    named = [resources["registry_secret"], *secrets]
    secret_keys = [
        key for secret in secrets for key in secret["required_keys"]
    ]
    kind = value["unit"]["kind"]
    expected_mount = {
        "teacher_run": TEACHER_MODEL_MOUNT,
        "student_train_merge": STUDENT_MODEL_MOUNT,
    }.get(kind)
    if (
        len({item["name"] for item in named}) != len(named)
        or len({item["object_id"] for item in named}) != len(named)
        or len(secret_keys) != len(set(secret_keys))
        or len({item["mount_path"] for item in volumes}) != len(volumes)
        or [item["mount_path"] for item in volumes]
        != ([] if expected_mount is None else [expected_mount])
    ):
        raise ValueError("Modal named resource identities are invalid")
    canonical_json(value)
    return value


def _winner_claim(raw, acceptance, definition):
    if (
        acceptance["schema_version"] != "milk.winner-provider-acceptance.v1"
        or definition["unit"]["kind"] != "student_winner_deployment"
        or not isinstance(raw, (bytes, bytearray))
        or hashlib.sha256(bytes(raw)).hexdigest() != acceptance["claim_sha256"]
    ):
        raise ValueError("Modal winner claim binding is invalid")
    claim = _strict_object(raw, 16 * 1024, "Modal winner gateway claim")
    if (
        tuple(claim) != WINNER_CLAIM_KEYS
        or json.dumps(
            claim,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        != bytes(raw)
    ):
        raise ValueError("Modal winner gateway claim is not canonical")
    scope = claim.get("scope")
    authority = claim.get("authority")
    model_manifest = claim.get("model_manifest")
    dev_receipt = claim.get("dev_receipt")
    unit = definition["unit"]
    if (
        claim.get("schema_version")
        != "dragontales.student-winner-deployment-claim.v2"
        or not isinstance(scope, dict)
        or tuple(scope) != SCOPE_KEYS
        or any(not isinstance(scope[key], str) or not scope[key] for key in scope)
        or HEX64.fullmatch(scope["eval_id"]) is None
        or scope["eval_id"] != acceptance["campaign_id"]
        or not isinstance(authority, dict)
        or tuple(authority) != WINNER_AUTHORITY_KEYS
        or not isinstance(model_manifest, dict)
        or tuple(model_manifest) != ("object_key", "sha256", "bytes")
        or not isinstance(dev_receipt, dict)
        or tuple(dev_receipt) != ("object_key", "sha256", "bytes")
        or any(
            not isinstance(artifact["object_key"], str)
            or not artifact["object_key"]
            or not isinstance(artifact["sha256"], str)
            or HEX64.fullmatch(artifact["sha256"]) is None
            or type(artifact["bytes"]) is not int
            or artifact["bytes"] <= 0
            for artifact in (model_manifest, dev_receipt)
        )
        or claim.get("student_job_id") != unit["student_job_id"]
        or claim.get("student_result_sha256")
        != unit["student_result_sha256"]
        or claim.get("winner") != unit["winner"]
        or claim.get("student_branch_runtime_image_reference")
        != definition["runtime_image_reference"]
        or claim.get("provider_binding_sha256")
        != acceptance["provider_binding_sha256"]
    ):
        raise ValueError("Modal winner claim differs from its accepted workload")
    authority_raw = json.dumps(
        authority,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    admission_program_sha256 = hashlib.sha256(
        winner_contract.ADMISSION_PATH.read_bytes()
    ).hexdigest()
    if (
        authority.get("schema_version")
        != "dragontales.winner-deployment-authority.v2"
        or authority.get("provider_policy")
        != {"primary": "baseten", "fallback": "modal"}
        or tuple(authority["provider_policy"]) != ("primary", "fallback")
        or authority.get("student_branch_runtime_image_reference")
        != definition["runtime_image_reference"]
        or authority.get("admission_program_sha256")
        != admission_program_sha256
        or authority.get("authorization_not_after") != claim.get("expires_at")
        or authority.get("max_wall_seconds") != acceptance["max_wall_seconds"]
        or authority.get("max_cost_microusd")
        != acceptance["max_cost_microusd"]
        or authority.get("allow_private_candidate_http") is not False
        or not _hex_authority(authority)
        or type(authority.get("candidate_max_in_flight")) is not int
        or authority["candidate_max_in_flight"] <= 0
        or authority.get("canary_candidate_basis_points") != 100
        or authority.get("canary_valid_for_seconds") != WINNER_CANARY_SECONDS
        or authority.get("max_input_utf8_bytes") != 2_048
        or authority.get("max_input_messages") != 16
        or authority.get("max_input_request_bytes") != 16_384
        or authority.get("route_schema_version") != "dragontales.route.v4"
        or authority.get("winner_admission_schema_version")
        != winner_contract.RECEIPT_SCHEMA
        or hashlib.sha256(
            b"dragontales.winner-provider-binding.v1\0" + authority_raw
        ).hexdigest()
        != acceptance["provider_binding_sha256"]
    ):
        raise ValueError("Modal winner deployment authority is invalid")
    claim_expiry = _winner_utc(claim.get("expires_at"), "winner claim expiry")
    if _winner_utc(
        acceptance["provider_not_after"], "Modal provider deadline"
    ) > claim_expiry:
        raise ValueError("Modal provider deadline exceeds the winner claim")
    return claim


def _hex_authority(authority):
    return (
        all(
            isinstance(authority.get(field), str)
            and HEX64.fullmatch(authority[field]) is not None
            for field in (
                "provider_terms_sha256",
                "admission_program_sha256",
                "signing_public_key_hex",
            )
        )
        and isinstance(authority.get("signing_key_id"), str)
        and 1 <= len(authority["signing_key_id"].encode()) <= 128
    )


def _materialization(message, definition):
    if not isinstance(message, str):
        raise ValueError("Modal materialization log is invalid")
    raw = message.encode()
    if not raw.endswith(b"\n"):
        raw += b"\n"
    value = _strict_object(
        raw,
        16 * 1024,
        "Modal winner materialization receipt",
        line=True,
    )
    if (
        set(value) != winner_contract.MATERIALIZATION_KEYS
        or value.get("schema_version") != winner_contract.MATERIALIZATION_SCHEMA
        or value.get("student_job_id") != definition["unit"]["student_job_id"]
        or value.get("variant") != definition["unit"]["winner"]
        or not isinstance(value.get("model_manifest_sha256"), str)
        or HEX64.fullmatch(value["model_manifest_sha256"]) is None
        or type(value.get("file_count")) is not int
        or value["file_count"] <= 0
        or type(value.get("artifact_bytes")) is not int
        or value["artifact_bytes"] <= 0
        or value.get("stage_path") != "/tmp/dragontales-winner"
        or value.get("model_path") != "/tmp/dragontales-winner/model"
        or value.get("model_manifest_path")
        != "/tmp/dragontales-winner/model-manifest.json"
    ):
        raise ValueError("Modal winner materialization receipt is invalid")
    return value


def _probe(raw, definition, alias, materialization):
    value = _strict_object(
        raw,
        MAX_PROBE_OUTPUT_BYTES,
        "Modal winner admission probe",
        line=True,
    )
    if (
        bytes(raw)
        != (
            json.dumps(
                value,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
        or set(value) != winner_contract.PROBE_KEYS
        or value.get("schema_version") != winner_contract.PROBE_SCHEMA
        or value.get("student_job_id") != definition["unit"]["student_job_id"]
        or value.get("student_variant") != definition["unit"]["winner"]
        or value.get("student_variant") != materialization["variant"]
        or value.get("model_manifest_sha256")
        != materialization["model_manifest_sha256"]
        or value.get("model_alias_sha256")
        != hashlib.sha256(alias.encode()).hexdigest()
        or any(
            not isinstance(value.get(field), str)
            or HEX64.fullmatch(value[field]) is None
            for field in (
                "candidate_api_key_sha256",
                "models_response_sha256",
                "chat_request_sha256",
                "chat_response_sha256",
            )
        )
    ):
        raise ValueError("Modal winner admission probe binding is invalid")
    return value


def _winner_prefix(acceptance):
    return (
        f"campaigns/v1/{acceptance['campaign_id']}/winner-jobs/"
        f"{acceptance['run_id']}"
    )


def _validate_admission(
    admission,
    acceptance,
    definition,
    claim,
    intent,
    create_result,
    alias,
    *,
    endpoint=None,
):
    winner_contract.receipt_bytes(admission)
    unit = definition["unit"]
    parsed_endpoint = urlsplit(admission.get("chat_completions_url", ""))
    try:
        endpoint_port = parsed_endpoint.port
    except ValueError as error:
        raise ValueError("stored Modal winner endpoint is invalid") from error
    launch_started = _winner_utc(
        admission.get("launch_started_at"),
        "winner launch time",
    )
    ready_at = _winner_utc(admission.get("ready_at"), "winner ready time")
    admitted_at = _winner_utc(
        admission.get("admitted_at"),
        "winner admission time",
    )
    service_not_after = _winner_utc(
        admission.get("service_not_after"),
        "winner service deadline",
    )
    provider_deadline = min(
        _winner_utc(
            acceptance["provider_not_after"],
            "Modal provider deadline",
        ),
        _winner_utc(intent["provider_timeout_at"], "Modal sandbox deadline"),
    )
    if (
        admission.get("provider") != "modal"
        or admission.get("student_job_id") != unit["student_job_id"]
        or admission.get("student_variant") != unit["winner"]
        or admission.get("model_manifest_sha256")
        != claim["model_manifest"]["sha256"]
        or admission.get("model_alias") != alias
        or admission.get("model_alias_sha256")
        != hashlib.sha256(alias.encode()).hexdigest()
        or any(
            not isinstance(admission.get(field), str)
            or HEX64.fullmatch(admission[field]) is None
            for field in (
                "candidate_api_key_sha256",
                "models_response_sha256",
                "chat_request_sha256",
                "chat_response_sha256",
            )
        )
        or admission.get("student_branch_runtime_image_reference")
        != definition["runtime_image_reference"]
        or admission.get("admission_program_sha256")
        != claim["authority"]["admission_program_sha256"]
        or not isinstance(admission.get("execution_id"), str)
        or OPAQUE.fullmatch(admission["execution_id"]) is None
        or admission.get("execution_name")
        != _sandbox_name(_normalized(acceptance, definition))
        or parsed_endpoint.scheme != "https"
        or not parsed_endpoint.hostname
        or parsed_endpoint.username is not None
        or parsed_endpoint.password is not None
        or endpoint_port not in {None, 443}
        or parsed_endpoint.path != "/v1/chat/completions"
        or parsed_endpoint.query
        or parsed_endpoint.fragment
        or endpoint is not None
        and admission["chat_completions_url"] != endpoint
        or admission.get("launch_started_at")
        != _winner_utc_text(
            _winner_utc(intent["requested_at"], "winner launch time")
        )
        or not launch_started <= ready_at <= admitted_at < service_not_after
        or admitted_at + dt.timedelta(seconds=WINNER_ROUTE_AUTHORITY_SECONDS)
        > service_not_after
        or service_not_after != provider_deadline
        or service_not_after
        > _winner_utc(claim["expires_at"], "winner claim expiry")
        or create_result is not None
        and create_result["state"] == "created"
        and admission["execution_id"] != create_result["execution_id"]
    ):
        raise ValueError("stored Modal winner admission differs")
    return admission


def acceptance_key(value):
    validate_acceptance(value)
    return (
        f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}/modal/"
        "provider-acceptance.json"
    )


def definition_key(acceptance, value):
    validate_definition(acceptance, value)
    return (
        f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}/modal/"
        f"execution-definitions/{value['ordinal']:02d}.json"
    )


def _normalized(acceptance, definition):
    validate_definition(acceptance, definition)
    identity = acceptance["selection"]["provider_identity"]
    return {
        "campaign_id": acceptance["campaign_id"],
        "run_id": acceptance["run_id"],
        "ordinal": definition["ordinal"],
        "provider_acceptance_sha256": provider_acceptance_sha256(acceptance),
        "provider_identity": identity,
        "gateway_claim_sha256": acceptance["claim_sha256"],
        "provider_pass_claim_sha256": acceptance["provider_pass_claim_sha256"],
        "create_authorization_sha256": acceptance["create_authorization_sha256"],
        "workload": {
            "kind": definition["unit"]["kind"],
            "runtime_image_reference": definition["runtime_image_reference"],
            "expires_at": acceptance["provider_not_after"],
        },
        "sandbox": definition["sandbox"],
        "resources": definition["resources"],
        "accepted_at": acceptance["accepted_at"],
        "create_not_after": acceptance["create_not_after"],
    }


def _sandbox_name(value):
    identity = _digest(
        {
            "schema_version": "milk.modal-sandbox-identity.v1",
            "campaign_id": value["campaign_id"],
            "run_id": value["run_id"],
            "ordinal": value["ordinal"],
            "gateway_claim_sha256": value["gateway_claim_sha256"],
        }
    )
    encoded = base64.urlsafe_b64encode(bytes.fromhex(identity)).decode().rstrip("=")
    return "milk-" + encoded


def sandbox_name(acceptance, definition):
    return _sandbox_name(_normalized(acceptance, definition))


def _sandbox_tags(value):
    return {
        "milk-campaign-id": value["campaign_id"],
        "milk-run-id": value["run_id"],
        "milk-ordinal": str(value["ordinal"]),
        "milk-claim-sha256": value["gateway_claim_sha256"],
        "milk-provider-acceptance-sha256": value["provider_acceptance_sha256"],
        "milk-workload-kind": value["workload"]["kind"],
    }


def sandbox_tags(acceptance, definition):
    return _sandbox_tags(_normalized(acceptance, definition))


class _Evidence:
    def __init__(self, store, acceptance):
        self.store = store
        self.run_id = acceptance["run_id"]
        self.prefix = (
            f"campaigns/v1/{acceptance['campaign_id']}/jobs/{self.run_id}/modal"
        )

    def json(self, relative, value):
        return create_same(
            self.store,
            f"{self.prefix}/{relative}",
            canonical_json(value),
            "application/json",
        )

    def bytes(self, relative, body, content_type="application/octet-stream"):
        return create_same(
            self.store,
            f"{self.prefix}/{relative}",
            body,
            content_type,
        )


class ModalJobs:
    def __init__(
        self,
        *,
        store,
        request_guard,
        modal_sdk=None,
        now=lambda: dt.datetime.now(dt.timezone.utc),
    ):
        if not callable(request_guard):
            raise ValueError("Modal request guard must be callable")
        if not callable(now):
            raise ValueError("Modal clock must be callable")
        self.store = store
        self.request_guard = request_guard
        self.modal = modal_sdk or importlib.import_module("modal")
        self.now = now
        if getattr(self.modal, "__version__", None) != MODAL_VERSION:
            raise RuntimeError(f"Modal {MODAL_VERSION} is required")

    def _guard(self, value, *, create=False):
        expected = {
            "provider_pass_claim_sha256": value["provider_pass_claim_sha256"],
            "create_authorization_sha256": value["create_authorization_sha256"],
        }
        current = self.request_guard()
        if create and current != expected:
            raise ValueError("live Modal create authority differs")

    def _call(self, value, function, *args, create=False, **kwargs):
        self._guard(value, create=create)
        return function(*args, **kwargs)

    def _guarded(self, value, iterable):
        iterator = iter(iterable)
        while True:
            self._guard(value)
            try:
                yield next(iterator)
            except StopIteration:
                return

    def _load_acceptance(self, acceptance, definition):
        validate_definition(acceptance, definition)
        raw = self.store.get(acceptance_key(acceptance))
        if raw != encode_provider_acceptance(acceptance):
            raise ValueError("stored Modal provider acceptance differs")
        raw = self.store.get(definition_key(acceptance, definition))
        if raw != canonical_json(definition):
            raise ValueError("stored Modal execution definition differs")
        value = _normalized(acceptance, definition)
        return value["provider_acceptance_sha256"], _Evidence(self.store, value), value

    @staticmethod
    def _identity(handle, expected_id, label, expected_name=None):
        if getattr(handle, "object_id", None) != expected_id:
            raise ValueError(f"Modal {label} object ID differs")
        if expected_name is not None and getattr(handle, "name", None) != expected_name:
            raise ValueError(f"Modal {label} name differs")

    def _hydrate(
        self, value, handle, expected_id, label, expected_name=None, *, create=False
    ):
        self._call(value, handle.hydrate, create=create)
        self._identity(handle, expected_id, label, expected_name)
        return handle

    def _app(self, value, *, create=False):
        identity = value["provider_identity"]
        workspace = self._call(
            value, self.modal.Workspace.from_context, create=create
        )
        self._call(value, workspace.hydrate, create=create)
        if getattr(workspace, "name", None) != identity["workspace_name"]:
            raise ValueError("Modal workspace name differs")
        environment = self._call(
            value,
            self.modal.Environment.from_name,
            identity["environment_name"],
            create_if_missing=False,
            create=create,
        )
        self._hydrate(
            value,
            environment,
            identity["environment_id"],
            "environment",
            identity["environment_name"],
            create=create,
        )
        app = self._call(
            value,
            self.modal.App.lookup,
            identity["app_name"],
            create_if_missing=False,
            environment_name=identity["environment_name"],
            create=create,
        )
        if (
            getattr(app, "app_id", None) != identity["app_id"]
            or getattr(app, "name", None) != identity["app_name"]
        ):
            raise ValueError("Modal app identity differs")
        return app

    def _create_resources(self, value, app):
        identity = value["provider_identity"]
        resources = value["resources"]

        def secret(spec):
            handle = self._call(
                value,
                self.modal.Secret.from_name,
                spec["name"],
                required_keys=spec["required_keys"],
                environment_name=identity["environment_name"],
                create=True,
            )
            return self._hydrate(
                value, handle, spec["object_id"], "secret", create=True
            )

        registry = secret(resources["registry_secret"])
        secrets = [secret(spec) for spec in resources["secrets"]]
        image = self._call(
            value,
            self.modal.Image.from_registry,
            value["workload"]["runtime_image_reference"],
            secret=registry,
            create=True,
        )
        image = self._call(value, image.entrypoint, [], create=True)
        volumes = {}
        for spec in resources["volumes"]:
            volume = self._call(
                value,
                self.modal.Volume.from_name,
                spec["name"],
                create_if_missing=False,
                environment_name=identity["environment_name"],
                create=True,
            )
            volume = self._hydrate(
                value, volume, spec["object_id"], "volume", create=True
            )
            volumes[spec["mount_path"]] = self._call(
                value,
                volume.with_mount_options,
                read_only=True,
                create=True,
            )
        return app, image, secrets, volumes

    @staticmethod
    def _request(value, command, environment):
        return {
            "schema_version": "milk.modal-sandbox-create-request.v1",
            "provider_acceptance_sha256": value["provider_acceptance_sha256"],
            "execution_name": _sandbox_name(value),
            "tags": _sandbox_tags(value),
            "runtime_image_reference": value["workload"]["runtime_image_reference"],
            "command_sha256": _digest(command),
            "environment_sha256": _digest(environment),
            "resources_sha256": _digest(value["resources"]),
            "gpu": "H100!",
            "cpu_physical_cores": value["sandbox"]["cpu_physical_cores"],
            "memory_mib": value["sandbox"]["memory_mib"],
            "timeout_seconds": value["sandbox"]["timeout_seconds"],
            "workdir": value["sandbox"]["workdir"],
            "encrypted_ports": (
                [8000]
                if value["workload"]["kind"] == "student_winner_deployment"
                else []
            ),
        }

    @classmethod
    def _accepted_request(cls, value, command, environment):
        command, environment = _sandbox_inputs(command, environment)
        secret_keys = {
            key
            for secret in value["resources"]["secrets"]
            for key in secret["required_keys"]
        }
        if secret_keys & environment.keys():
            raise ValueError("Modal sandbox environment shadows a named secret")
        request = cls._request(value, command, environment)
        if (
            request["command_sha256"] != value["sandbox"]["command_sha256"]
            or request["environment_sha256"]
            != value["sandbox"]["environment_sha256"]
        ):
            raise ValueError("Modal sandbox inputs differ from provider acceptance")
        return command, environment, request

    def _intent(self, value, request, current):
        _utc_text(current)
        timeout = value["sandbox"]["timeout_seconds"]
        timeout_at = current + dt.timedelta(seconds=timeout)
        if (
            current < _utc(value["accepted_at"], "Modal acceptance time")
            or current
            > _utc(value["create_not_after"], "Modal provider create deadline")
            or timeout_at
            > _utc(value["workload"]["expires_at"], "Modal workload expiry")
        ):
            raise ValueError("Modal workload has insufficient provider-timeout runway")
        intent = {
            "schema_version": "milk.modal-sandbox-create-intent.v1",
            "campaign_id": value["campaign_id"],
            "run_id": value["run_id"],
            "ordinal": value["ordinal"],
            "provider_acceptance_sha256": value["provider_acceptance_sha256"],
            "request_sha256": _digest(request),
            "execution_name": request["execution_name"],
            "requested_at": _utc_text(current),
            "provider_timeout_at": _utc_text(timeout_at),
            "recovery_not_after": _utc_text(
                timeout_at + dt.timedelta(seconds=TERMINALIZATION_GRACE_SECONDS)
            ),
        }
        return self._validate_intent(value, request, intent)

    @staticmethod
    def _validate_intent(value, request, intent):
        if not isinstance(intent, dict) or set(intent) != {
            "schema_version",
            "campaign_id",
            "run_id",
            "ordinal",
            "provider_acceptance_sha256",
            "request_sha256",
            "execution_name",
            "requested_at",
            "provider_timeout_at",
            "recovery_not_after",
        }:
            raise ValueError("stored Modal create intent is invalid")
        requested = _utc(intent.get("requested_at"), "Modal create request time")
        timeout_at = _utc(intent.get("provider_timeout_at"), "Modal provider timeout")
        recovery = _utc(intent.get("recovery_not_after"), "Modal recovery deadline")
        if (
            intent["schema_version"] != "milk.modal-sandbox-create-intent.v1"
            or intent["campaign_id"] != value["campaign_id"]
            or intent["run_id"] != value["run_id"]
            or intent["ordinal"] != value["ordinal"]
            or intent["provider_acceptance_sha256"]
            != value["provider_acceptance_sha256"]
            or intent["request_sha256"] != _digest(request)
            or intent["execution_name"] != request["execution_name"]
            or requested < _utc(value["accepted_at"], "Modal acceptance time")
            or requested
            > _utc(value["create_not_after"], "Modal provider create deadline")
            or timeout_at
            != requested + dt.timedelta(seconds=value["sandbox"]["timeout_seconds"])
            or timeout_at
            > _utc(value["workload"]["expires_at"], "Modal workload expiry")
            or recovery
            != timeout_at + dt.timedelta(seconds=TERMINALIZATION_GRACE_SECONDS)
        ):
            raise ValueError("stored Modal create intent differs")
        return intent

    def _existing_intent(self, value, request, evidence):
        relative = f"create-intents/{value['ordinal']:02d}.json"
        try:
            return self._validate_intent(
                value,
                request,
                _stored_json(
                    self.store,
                    f"{evidence.prefix}/{relative}",
                    "Modal create intent",
                ),
            )
        except FileNotFoundError:
            return None

    @staticmethod
    def _validate_create_result(value, request, intent, result):
        common = {
            "schema_version",
            "provider_acceptance_sha256",
            "request_sha256",
            "execution_name",
            "observed_at",
            "state",
        }
        state = result.get("state") if isinstance(result, dict) else None
        if (
            not isinstance(result, dict)
            or result.get("schema_version")
            != "milk.modal-sandbox-create-result.v1"
            or result.get("provider_acceptance_sha256")
            != value["provider_acceptance_sha256"]
            or result.get("request_sha256") != _digest(request)
            or result.get("execution_name") != request["execution_name"]
            or _utc(result.get("observed_at"), "Modal create result time")
            < _utc(intent["requested_at"], "Modal create request time")
            or state not in {"created", "ambiguous"}
        ):
            raise ValueError("stored Modal create result is invalid")
        if state == "created":
            if (
                set(result) != common | {"execution_id", "tags_sha256"}
                or OPAQUE.fullmatch(result.get("execution_id", "")) is None
                or result.get("tags_sha256") != _digest(request["tags"])
            ):
                raise ValueError("stored Modal create result is invalid")
        elif (
            set(result) != common | {"error_class_sha256"}
            or HEX64.fullmatch(result.get("error_class_sha256", "")) is None
        ):
            raise ValueError("stored Modal create result is invalid")
        return result

    def _create_result(self, value, request, intent, evidence):
        key = f"{evidence.prefix}/create-results/{value['ordinal']:02d}.json"
        try:
            result = _stored_json(self.store, key, "Modal create result")
        except FileNotFoundError:
            return None
        if intent is None:
            raise ValueError("Modal create result has no create intent")
        return self._validate_create_result(value, request, intent, result)

    @staticmethod
    def _validate_terminate_intent(value, request, execution_id, attempt, intent):
        if (
            not isinstance(intent, dict)
            or set(intent)
            != {
                "schema_version",
                "provider_acceptance_sha256",
                "request_sha256",
                "execution_name",
                "execution_id",
                "attempt",
                "requested_at",
            }
            or intent.get("schema_version")
            != "milk.modal-sandbox-terminate-intent.v1"
            or intent.get("provider_acceptance_sha256")
            != value["provider_acceptance_sha256"]
            or intent.get("request_sha256") != _digest(request)
            or intent.get("execution_name") != request["execution_name"]
            or intent.get("execution_id") != execution_id
            or intent.get("attempt") != attempt
            or _utc(intent.get("requested_at"), "Modal terminate request time")
            < _utc(value["accepted_at"], "Modal acceptance time")
        ):
            raise ValueError("stored Modal terminate intent is invalid")
        return intent

    @staticmethod
    def _validate_terminate_result(
        value, request, execution_id, attempt, intent, result
    ):
        common = {
            "schema_version",
            "provider_acceptance_sha256",
            "request_sha256",
            "execution_name",
            "execution_id",
            "attempt",
            "observed_at",
            "state",
        }
        state = result.get("state") if isinstance(result, dict) else None
        if (
            not isinstance(result, dict)
            or result.get("schema_version")
            != "milk.modal-sandbox-terminate-result.v1"
            or result.get("provider_acceptance_sha256")
            != value["provider_acceptance_sha256"]
            or result.get("request_sha256") != _digest(request)
            or result.get("execution_name") != request["execution_name"]
            or result.get("execution_id") != execution_id
            or result.get("attempt") != attempt
            or _utc(result.get("observed_at"), "Modal terminate result time")
            < _utc(intent["requested_at"], "Modal terminate request time")
            or state not in {"terminated", "ambiguous"}
        ):
            raise ValueError("stored Modal terminate result is invalid")
        if "exit_code" in result:
            if (
                set(result) != common | {"exit_code"}
                or result["exit_code"] is not None
                and type(result["exit_code"]) is not int
                or (state == "terminated") != (result["exit_code"] is not None)
            ):
                raise ValueError("stored Modal terminate result is invalid")
        elif (
            set(result) != common | {"error_class_sha256"}
            or state != "ambiguous"
            or HEX64.fullmatch(result.get("error_class_sha256", "")) is None
        ):
            raise ValueError("stored Modal terminate result is invalid")
        return result

    def _termination_records(self, value, request, evidence, execution_id):
        records = []
        gap = False
        terminal = False
        for attempt in range(MAX_TERMINATE_ATTEMPTS):
            stem = f"{value['ordinal']:02d}/{attempt:02d}.json"
            try:
                intent = _stored_json(
                    self.store,
                    f"{evidence.prefix}/terminate-intents/{stem}",
                    "Modal terminate intent",
                )
            except FileNotFoundError:
                intent = None
            try:
                result = _stored_json(
                    self.store,
                    f"{evidence.prefix}/terminate-results/{stem}",
                    "Modal terminate result",
                )
            except FileNotFoundError:
                result = None
            if result is not None and intent is None:
                raise ValueError("Modal terminate result has no terminate intent")
            if intent is not None:
                if gap or terminal:
                    raise ValueError("Modal terminate attempts are not contiguous")
                if execution_id is None:
                    raise ValueError("Modal terminate intent has no known execution")
                self._validate_terminate_intent(
                    value, request, execution_id, attempt, intent
                )
            if result is not None:
                self._validate_terminate_result(
                    value, request, execution_id, attempt, intent, result
                )
                terminal = result["state"] == "terminated"
            if intent is None:
                gap = True
            records.append((intent, result))
        return records

    def _not_found(self, error):
        exception = getattr(self.modal, "exception", None)
        kind = getattr(exception, "NotFoundError", None)
        return isinstance(kind, type) and isinstance(error, kind)

    def _recover_handle(self, value, app):
        name = _sandbox_name(value)
        try:
            sandbox = self._call(
                value,
                self.modal.Sandbox.from_name,
                value["provider_identity"]["app_name"],
                name,
                environment_name=value["provider_identity"]["environment_name"],
            )
        except Exception as error:
            if self._not_found(error):
                return None
            raise
        tags = self._call(value, sandbox.get_tags)
        if tags != _sandbox_tags(value):
            raise ValueError("named Modal sandbox tags differ from its create intent")
        if getattr(sandbox, "name", name) != name:
            raise ValueError("named Modal sandbox name differs from its create intent")
        _opaque(getattr(sandbox, "object_id", None), "Modal sandbox object ID")
        if getattr(app, "app_id", None) != value["provider_identity"]["app_id"]:
            raise ValueError("Modal sandbox app identity changed")
        return sandbox

    def _winner_inputs(
        self,
        acceptance,
        definition,
        command,
        environment,
        gateway_claim_raw,
    ):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance,
            definition,
        )
        command, environment, request = self._accepted_request(
            value,
            command,
            environment,
        )
        if acceptance["schema_version"] != "milk.winner-provider-acceptance.v1":
            raise ValueError("Modal winner admission requires a winner acceptance")
        intent = self._existing_intent(value, request, evidence)
        if intent is None:
            raise ValueError("Modal winner admission requires a create intent")
        create_result = self._create_result(value, request, intent, evidence)
        claim = _winner_claim(gateway_claim_raw, acceptance, definition)
        alias = environment.get("DRAGONTALES_MODEL_ALIAS")
        if (
            not isinstance(alias, str)
            or winner_contract.MODEL_ALIAS.fullmatch(alias) is None
            or environment.get(
                "DRAGONTALES_STUDENT_JOB_ID",
                definition["unit"]["student_job_id"],
            )
            != definition["unit"]["student_job_id"]
        ):
            raise ValueError("Modal winner serving environment is invalid")
        secret_specs = [
            spec
            for spec in value["resources"]["secrets"]
            if spec["required_keys"] == [WINNER_API_KEY_ENVIRONMENT]
        ]
        if len(secret_specs) != 1:
            raise ValueError("Modal winner requires one exact application-key secret")
        return {
            "acceptance_sha256": acceptance_sha256,
            "evidence": evidence,
            "value": value,
            "request": request,
            "intent": intent,
            "create_result": create_result,
            "claim": claim,
            "alias": alias,
            "secret_spec": secret_specs[0],
        }

    def _winner_handle(self, context):
        value = context["value"]
        app = self._app(value)
        sandbox = self._recover_handle(value, app)
        create_result = context["create_result"]
        if (
            sandbox is not None
            and create_result is not None
            and create_result["state"] == "created"
            and create_result["execution_id"] != sandbox.object_id
        ):
            raise ValueError("named Modal sandbox ID differs from its create result")
        return app, sandbox

    def _winner_secret(self, context):
        value = context["value"]
        spec = context["secret_spec"]
        handle = self._call(
            value,
            self.modal.Secret.from_name,
            spec["name"],
            required_keys=spec["required_keys"],
            environment_name=value["provider_identity"]["environment_name"],
        )
        return self._hydrate(
            value,
            handle,
            spec["object_id"],
            "winner application-key secret",
        )

    def _winner_endpoint(self, value, sandbox):
        tunnels = self._call(
            value,
            sandbox.tunnels,
            timeout=WINNER_TUNNEL_TIMEOUT_SECONDS,
        )
        tunnel_type = getattr(self.modal, "Tunnel", None)
        if (
            type(tunnels) is not dict
            or set(tunnels) != {WINNER_PORT}
            or not isinstance(tunnel_type, type)
            or not isinstance(tunnels[WINNER_PORT], tunnel_type)
        ):
            raise ValueError("Modal winner tunnel lookup is invalid")
        tunnel = tunnels[WINNER_PORT]
        url = getattr(tunnel, "url", None)
        parsed = urlsplit(url) if isinstance(url, str) else None
        try:
            port = parsed.port if parsed is not None else None
        except ValueError as error:
            raise ValueError("Modal winner tunnel is invalid") from error
        port = port or 443
        if (
            parsed is None
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or port != 443
            or getattr(tunnel, "host", None) != parsed.hostname
            or getattr(tunnel, "port", None) != port
            or getattr(tunnel, "tls_socket", None) != (parsed.hostname, port)
        ):
            raise ValueError("Modal winner tunnel is invalid")
        return url.rstrip("/") + "/v1/chat/completions"

    def _winner_logs(
        self,
        context,
        app,
        sandbox,
        definition,
        *,
        since,
        until,
    ):
        value = context["value"]
        evidence = context["evidence"]
        plan = {
            "schema_version": "milk.modal-winner-log-collection-plan.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "execution_id": sandbox.object_id,
            "since": _winner_utc_text(since),
            "until": _winner_utc_text(until),
            "source": None,
            "search_text": definition["unit"]["student_job_id"],
            "max_bytes": MAX_LOG_BYTES,
            "max_records": MAX_ADMISSION_LOG_RECORDS,
        }
        stream_id = _digest(plan)
        evidence.json(f"winner/log-plans/{stream_id}.json", plan)
        archive = LogArchive(
            evidence,
            "modal",
            stream_id=stream_id,
            max_run_bytes=MAX_LOG_BYTES,
        )
        materializations = []
        previous_at = None
        message_bytes = 0
        complete = True
        missing_reason = None
        try:
            stream = self._call(
                value,
                app.logs.fetch,
                since=since,
                until=until,
                source=None,
                search_text=definition["unit"]["student_job_id"],
            )
            for record_number, entry in enumerate(self._guarded(value, stream)):
                if record_number >= MAX_ADMISSION_LOG_RECORDS:
                    complete = False
                    missing_reason = "bounded_log_record_limit"
                    break
                if not isinstance(entry, self.modal.types.LogEntry):
                    raise ValueError("Modal winner log record is invalid")
                timestamp = entry.timestamp
                context_ids = entry.context_ids
                encoded = entry.message.encode() if isinstance(entry.message, str) else b""
                if (
                    not isinstance(entry.message, str)
                    or len(encoded) > 16 * 1024
                    or not isinstance(timestamp, dt.datetime)
                    or timestamp.tzinfo is None
                    or timestamp.utcoffset() != dt.timedelta(0)
                    or not since <= timestamp <= until
                    or previous_at is not None
                    and timestamp < previous_at
                    or entry.source not in {"stdout", "stderr", "system"}
                    or not isinstance(entry.object_id, str)
                    or OPAQUE.fullmatch(entry.object_id) is None
                    or not isinstance(context_ids, list)
                    or len(context_ids) > 32
                    or any(
                        not isinstance(item, str)
                        or OPAQUE.fullmatch(item) is None
                        for item in context_ids
                    )
                    or entry.object_id != sandbox.object_id
                    and sandbox.object_id not in context_ids
                ):
                    raise ValueError("Modal winner log record is invalid")
                previous_at = timestamp
                message_bytes += len(encoded)
                if message_bytes > MAX_LOG_BYTES:
                    complete = False
                    missing_reason = "bounded_log_byte_limit"
                    break
                cursor = _digest(
                    {
                        "source": entry.source,
                        "object_id": entry.object_id,
                        "context_ids": context_ids,
                    }
                )
                if not archive.write(
                    at=_utc_text(timestamp),
                    line=entry.message,
                    provider_cursor=cursor,
                ):
                    complete = False
                    missing_reason = "bounded_log_byte_limit"
                    break
                if winner_contract.MATERIALIZATION_SCHEMA in entry.message:
                    materializations.append(_materialization(entry.message, definition))
        except Exception:
            archive.close(
                source_complete=False,
                missing_reason="provider_log_read_failed",
            )
            raise
        archive.close(
            source_complete=complete,
            missing_reason=missing_reason,
        )
        if not complete:
            raise RuntimeError("Modal winner logs exceeded their evidence bound")
        if not materializations or any(
            item != materializations[0] for item in materializations[1:]
        ):
            raise ValueError("Modal winner materialization is missing or conflicting")
        materialization = materializations[0]
        evidence.json("winner/materialization.json", materialization)
        return materialization

    def _winner_probe(
        self,
        context,
        sandbox,
        definition,
        endpoint,
        materialization,
    ):
        value = context["value"]
        source = winner_contract.ADMISSION_PATH.read_bytes()
        if (
            not 1 <= len(source) <= 16 * 1024
            or hashlib.sha256(source).hexdigest()
            != context["claim"]["authority"]["admission_program_sha256"]
        ):
            raise ValueError("Modal winner admission program differs from its claim")
        secret = self._winner_secret(context)
        process = self._call(
            value,
            sandbox.exec,
            "python3",
            "-c",
            source.decode("utf-8"),
            endpoint,
            context["alias"],
            definition["unit"]["student_job_id"],
            timeout=WINNER_PROBE_TIMEOUT_SECONDS,
            secrets=[secret],
            text=False,
        )
        exit_code = self._call(value, process.wait)
        stdout = self._call(value, process.stdout.read)
        stderr = self._call(value, process.stderr.read)
        self._guard(value)
        returncode = process.returncode
        if (
            type(exit_code) is not int
            or returncode != exit_code
            or not isinstance(stdout, bytes)
            or not isinstance(stderr, bytes)
            or len(stdout) > MAX_PROBE_OUTPUT_BYTES
            or len(stderr) > MAX_PROBE_OUTPUT_BYTES
        ):
            raise ValueError("Modal winner probe process result is invalid")
        record = {
            "schema_version": "milk.modal-winner-probe-execution.v1",
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "execution_id": sandbox.object_id,
            "admission_program_sha256": hashlib.sha256(source).hexdigest(),
            "endpoint_sha256": hashlib.sha256(endpoint.encode()).hexdigest(),
            "stdout_bytes": len(stdout),
            "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
            "stderr_bytes": len(stderr),
            "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
            "exit_code": exit_code,
            "observed_at": _winner_utc_text(self.now()),
            "state": "accepted" if exit_code == 0 and not stderr else "rejected",
        }
        context["evidence"].json(
            f"winner/probes/{_digest(record)}.json",
            record,
        )
        if exit_code != 0 or stderr:
            raise RuntimeError("Modal winner admission probe was rejected")
        return _probe(stdout, definition, context["alias"], materialization)

    def _load_winner_admission(
        self,
        acceptance,
        definition,
        context,
        observed_cost_microusd,
    ):
        prefix = _winner_prefix(acceptance)
        receipt_key = f"{prefix}/winner-admission.json"
        result_key = f"{prefix}/gateway-result.json"
        try:
            receipt_raw = self.store.get(receipt_key)
        except FileNotFoundError:
            try:
                self.store.get(result_key)
            except FileNotFoundError:
                return None
            raise ValueError("Modal gateway result has no admission receipt")
        receipt = _strict_object(
            receipt_raw,
            16 * 1024,
            "stored Modal winner admission",
            line=True,
        )
        if winner_contract.receipt_bytes(receipt) != receipt_raw:
            raise ValueError("stored Modal winner admission is not canonical")
        _validate_admission(
            receipt,
            acceptance,
            definition,
            context["claim"],
            context["intent"],
            context["create_result"],
            context["alias"],
        )
        try:
            stored_result_raw = self.store.get(result_key)
        except FileNotFoundError:
            stored_result_raw = None
        if stored_result_raw is not None:
            stored_result = _strict_object(
                stored_result_raw,
                64 * 1024,
                "stored Modal gateway result",
                line=True,
            )
            if winner_contract.result_bytes(stored_result) != stored_result_raw:
                raise ValueError("stored Modal gateway result is not canonical")
            if observed_cost_microusd is None:
                observed_cost_microusd = stored_result.get(
                    "observed_cost_microusd"
                )
        if (
            type(observed_cost_microusd) is not int
            or not 0
            <= observed_cost_microusd
            <= acceptance["max_cost_microusd"]
        ):
            raise ValueError("Modal winner observed cost is outside its authority")
        result = {
            "schema_version": winner_contract.RESULT_SCHEMA,
            "scope": context["claim"]["scope"],
            "student_job_id": definition["unit"]["student_job_id"],
            "claim_sha256": acceptance["claim_sha256"],
            "provider_binding_sha256": acceptance["provider_binding_sha256"],
            "provider_acceptance": acceptance,
            "observed_cost_microusd": observed_cost_microusd,
            "admission": receipt,
        }
        result_raw = winner_contract.result_bytes(result)
        if stored_result_raw is None:
            create_same(
                self.store,
                result_key,
                result_raw,
                "application/json",
            )
        elif stored_result_raw != result_raw:
            raise ValueError("stored Modal gateway result differs")
        return receipt, receipt_raw, result_raw

    def _load_winner_teardown(self, acceptance, context, admission, admission_raw):
        try:
            raw = self.store.get(
                f"{_winner_prefix(acceptance)}/modal-zero-gpu.json"
            )
        except FileNotFoundError:
            return None
        result = _strict_object(
            raw,
            64 * 1024,
            "stored Modal winner teardown",
            line=True,
        )
        zero = result.get("zero_gpu_verification")
        if (
            raw != canonical_json(result)
            or set(result)
            != {
                "schema_version",
                "run_id",
                "provider_acceptance_sha256",
                "winner_admission_sha256",
                "teardown_authority",
                "termination_state",
                "zero_gpu_verification",
                "state",
            }
            or result.get("schema_version")
            != "milk.modal-winner-teardown-result.v1"
            or result.get("run_id") != acceptance["run_id"]
            or result.get("provider_acceptance_sha256")
            != context["acceptance_sha256"]
            or result.get("winner_admission_sha256")
            != hashlib.sha256(admission_raw).hexdigest()
            or result.get("teardown_authority")
            not in {"signed_zero_route", "service_authority_expired"}
            or not isinstance(result.get("termination_state"), str)
            or result.get("state") != "zero"
            or not isinstance(zero, dict)
            or set(zero)
            != {
                "schema_version",
                "provider_acceptance_sha256",
                "execution_name",
                "observed_at",
                "matched_sandboxes",
                "active_execution_ids",
                "state",
            }
            or zero.get("schema_version")
            != "milk.modal-zero-gpu-verification.v1"
            or zero.get("provider_acceptance_sha256")
            != context["acceptance_sha256"]
            or zero.get("execution_name") != context["request"]["execution_name"]
            or type(zero.get("matched_sandboxes")) is not int
            or not 0 <= zero["matched_sandboxes"] <= 1
            or zero.get("active_execution_ids") != []
            or zero.get("state") != "zero"
        ):
            raise ValueError("stored Modal winner teardown differs")
        observed_at = _utc(zero.get("observed_at"), "Modal zero-GPU time")
        if result["teardown_authority"] == "signed_zero_route":
            prefix = _winner_prefix(acceptance)
            validate_route_retirement(
                self.store.get(f"{prefix}/routes/canary-receipt.json"),
                self.store.get(f"{prefix}/routes/zero-receipt.json"),
                context["claim"],
            )
        elif observed_at < _winner_utc(
            admission["service_not_after"],
            "winner service deadline",
        ):
            raise ValueError("stored Modal winner expiry teardown is premature")
        zero_key = (
            f"{context['evidence'].prefix}/zero-verifications/"
            f"{_digest(zero)}.json"
        )
        if self.store.get(zero_key) != canonical_json(zero):
            raise ValueError("stored Modal zero-GPU evidence differs")
        return result

    def admit_winner(
        self,
        acceptance,
        definition,
        command,
        environment,
        *,
        gateway_claim_raw,
        observed_cost_microusd=0,
    ):
        context = self._winner_inputs(
            acceptance,
            definition,
            command,
            environment,
            gateway_claim_raw,
        )
        if (
            type(observed_cost_microusd) is not int
            or not 0
            <= observed_cost_microusd
            <= acceptance["max_cost_microusd"]
        ):
            raise ValueError("Modal winner observed cost is outside its authority")
        stored = self._load_winner_admission(
            acceptance,
            definition,
            context,
            observed_cost_microusd,
        )
        if stored is not None:
            receipt, receipt_raw, result_raw = stored
            return {
                "schema_version": "milk.modal-winner-admit-result.v1",
                "run_id": acceptance["run_id"],
                "gateway_result_sha256": hashlib.sha256(result_raw).hexdigest(),
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "winner_admission_sha256": hashlib.sha256(receipt_raw).hexdigest(),
                "service_not_after": receipt["service_not_after"],
                "state": "admitted",
            }
        app, sandbox = self._winner_handle(context)
        if sandbox is None:
            return {
                "schema_version": "milk.modal-winner-admit-result.v1",
                "run_id": acceptance["run_id"],
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "state": "create_ambiguous_no_replay",
            }
        value = context["value"]
        exit_code = self._call(value, sandbox.poll)
        if exit_code is not None:
            if type(exit_code) is not int:
                raise ValueError("Modal sandbox poll result is invalid")
            raise RuntimeError("Modal winner sandbox terminated before admission")
        provider_deadline = min(
            _winner_utc(
                acceptance["provider_not_after"],
                "Modal provider deadline",
            ),
            _winner_utc(
                context["intent"]["provider_timeout_at"],
                "Modal sandbox deadline",
            ),
        )
        current = _winner_utc(self.now(), "Modal winner clock")
        admission_runway = int(
            (provider_deadline - current).total_seconds()
        ) - WINNER_ROUTE_AUTHORITY_SECONDS
        if admission_runway < 1:
            raise RuntimeError("Modal winner has no fixed canary runway")
        self._call(
            value,
            sandbox.wait_until_ready,
            timeout=min(WINNER_READY_TIMEOUT_SECONDS, admission_runway),
        )
        ready_at = _winner_utc(self.now(), "Modal winner ready time")
        if (
            ready_at + dt.timedelta(seconds=WINNER_ROUTE_AUTHORITY_SECONDS)
            > provider_deadline
        ):
            raise RuntimeError("Modal winner became ready without canary runway")
        endpoint = self._winner_endpoint(value, sandbox)
        launch_started = _winner_utc(
            context["intent"]["requested_at"],
            "Modal winner launch time",
        )
        log_until = _winner_utc(self.now(), "Modal winner log time")
        if log_until <= launch_started:
            log_until = launch_started + dt.timedelta(seconds=1)
        materialization = self._winner_logs(
            context,
            app,
            sandbox,
            definition,
            since=launch_started,
            until=log_until,
        )
        if (
            materialization["model_manifest_sha256"]
            != context["claim"]["model_manifest"]["sha256"]
        ):
            raise ValueError("Modal materialization differs from the winner claim")
        probe = self._winner_probe(
            context,
            sandbox,
            definition,
            endpoint,
            materialization,
        )
        admitted_at = _winner_utc(self.now(), "Modal winner admission time")
        service_not_after = provider_deadline
        if not (
            launch_started <= ready_at <= admitted_at
            and admitted_at + dt.timedelta(seconds=WINNER_ROUTE_AUTHORITY_SECONDS)
            <= service_not_after
            <= provider_deadline
            <= _winner_utc(
                context["claim"]["expires_at"],
                "winner claim expiry",
            )
        ):
            raise RuntimeError("Modal winner cannot receive fixed canary authority")
        if self._call(value, sandbox.poll) is not None:
            raise RuntimeError("Modal winner sandbox terminated during admission")
        admission = {
            "schema_version": winner_contract.RECEIPT_SCHEMA,
            "provider": "modal",
            "student_job_id": definition["unit"]["student_job_id"],
            "student_variant": definition["unit"]["winner"],
            "model_manifest_sha256": context["claim"]["model_manifest"][
                "sha256"
            ],
            "model_alias": context["alias"],
            "model_alias_sha256": probe["model_alias_sha256"],
            "candidate_api_key_sha256": probe["candidate_api_key_sha256"],
            "student_branch_runtime_image_reference": definition[
                "runtime_image_reference"
            ],
            "admission_program_sha256": context["claim"]["authority"][
                "admission_program_sha256"
            ],
            "execution_id": sandbox.object_id,
            "execution_name": context["request"]["execution_name"],
            "chat_completions_url": endpoint,
            "models_response_sha256": probe["models_response_sha256"],
            "chat_request_sha256": probe["chat_request_sha256"],
            "chat_response_sha256": probe["chat_response_sha256"],
            "launch_started_at": _winner_utc_text(launch_started),
            "ready_at": _winner_utc_text(ready_at),
            "admitted_at": _winner_utc_text(admitted_at),
            "service_not_after": _winner_utc_text(service_not_after),
        }
        _validate_admission(
            admission,
            acceptance,
            definition,
            context["claim"],
            context["intent"],
            context["create_result"],
            context["alias"],
            endpoint=endpoint,
        )
        receipt_raw = winner_contract.receipt_bytes(admission)
        result = {
            "schema_version": winner_contract.RESULT_SCHEMA,
            "scope": context["claim"]["scope"],
            "student_job_id": definition["unit"]["student_job_id"],
            "claim_sha256": acceptance["claim_sha256"],
            "provider_binding_sha256": acceptance["provider_binding_sha256"],
            "provider_acceptance": acceptance,
            "observed_cost_microusd": observed_cost_microusd,
            "admission": admission,
        }
        result_raw = winner_contract.result_bytes(result)
        prefix = _winner_prefix(acceptance)
        create_same(
            self.store,
            f"{prefix}/winner-admission.json",
            receipt_raw,
            "application/json",
        )
        create_same(
            self.store,
            f"{prefix}/gateway-result.json",
            result_raw,
            "application/json",
        )
        return {
            "schema_version": "milk.modal-winner-admit-result.v1",
            "run_id": acceptance["run_id"],
            "gateway_result_sha256": hashlib.sha256(result_raw).hexdigest(),
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "winner_admission_sha256": hashlib.sha256(receipt_raw).hexdigest(),
            "service_not_after": admission["service_not_after"],
            "state": "admitted",
        }

    def teardown_winner(
        self,
        acceptance,
        definition,
        command,
        environment,
        *,
        gateway_claim_raw,
        canary_route_raw=None,
        zero_route_raw=None,
    ):
        context = self._winner_inputs(
            acceptance,
            definition,
            command,
            environment,
            gateway_claim_raw,
        )
        stored = self._load_winner_admission(
            acceptance,
            definition,
            context,
            None,
        )
        if stored is None:
            raise ValueError("Modal winner teardown requires its admission receipt")
        admission, admission_raw, unused_result_raw = stored
        completed = self._load_winner_teardown(
            acceptance,
            context,
            admission,
            admission_raw,
        )
        if completed is not None:
            return completed
        current = _winner_utc(self.now(), "Modal winner teardown clock")
        service_not_after = _winner_utc(
            admission["service_not_after"],
            "winner service deadline",
        )
        if (canary_route_raw is None) != (zero_route_raw is None):
            raise ValueError("signed zero route requires its canary receipt")
        if zero_route_raw is not None:
            validate_route_retirement(
                canary_route_raw,
                zero_route_raw,
                context["claim"],
            )
            prefix = _winner_prefix(acceptance)
            create_same(
                self.store,
                f"{prefix}/routes/canary-receipt.json",
                bytes(canary_route_raw),
                "application/json",
            )
            create_same(
                self.store,
                f"{prefix}/routes/zero-receipt.json",
                bytes(zero_route_raw),
                "application/json",
            )
            authority = "signed_zero_route"
        elif current < service_not_after:
            raise RuntimeError(
                "signed zero-basis-point route is required before expiry"
            )
        else:
            authority = "service_authority_expired"
            event = {
                "schema_version": "milk.modal-winner-expiry-evidence.v1",
                "provider_acceptance_sha256": context["acceptance_sha256"],
                "service_not_after": admission["service_not_after"],
                "observed_at": _winner_utc_text(current),
            }
            context["evidence"].json(
                f"winner/expiry/{_digest(event)}.json",
                event,
            )
        termination = self.terminate(
            acceptance,
            definition,
            command,
            environment,
        )
        zero = self.verify_zero(acceptance, definition)
        result = {
            "schema_version": "milk.modal-winner-teardown-result.v1",
            "run_id": acceptance["run_id"],
            "provider_acceptance_sha256": context["acceptance_sha256"],
            "winner_admission_sha256": hashlib.sha256(admission_raw).hexdigest(),
            "teardown_authority": authority,
            "termination_state": termination["state"],
            "zero_gpu_verification": zero,
            "state": "zero" if zero["state"] == "zero" else "hold",
        }
        context["evidence"].json(
            f"winner/teardowns/{_digest(result)}.json",
            result,
        )
        if result["state"] == "zero":
            create_same(
                self.store,
                f"{_winner_prefix(acceptance)}/modal-zero-gpu.json",
                canonical_json(result),
                "application/json",
            )
        return result

    def recover(self, acceptance, definition, command, environment):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance, definition
        )
        unused_command, unused_environment, request = self._accepted_request(
            value, command, environment
        )
        intent = self._existing_intent(value, request, evidence)
        create_result = self._create_result(value, request, intent, evidence)
        if intent is None:
            raise ValueError("Modal recovery requires an immutable create intent")
        app = self._app(value)
        sandbox = self._recover_handle(value, app)
        if (
            sandbox is not None
            and create_result is not None
            and create_result["state"] == "created"
            and create_result["execution_id"] != sandbox.object_id
        ):
            raise ValueError("named Modal sandbox ID differs from its create result")
        current = _utc_text(self.now())
        if sandbox is None:
            result = {
                "schema_version": "milk.modal-sandbox-observation.v1",
                "provider_acceptance_sha256": acceptance_sha256,
                "request_sha256": _digest(request),
                "execution_name": request["execution_name"],
                "observed_at": current,
                "state": "ambiguous_not_observed",
            }
        else:
            exit_code = self._call(value, sandbox.poll)
            if exit_code is not None and type(exit_code) is not int:
                raise ValueError("Modal sandbox poll result is invalid")
            result = {
                "schema_version": "milk.modal-sandbox-observation.v1",
                "provider_acceptance_sha256": acceptance_sha256,
                "request_sha256": _digest(request),
                "execution_name": request["execution_name"],
                "execution_id": sandbox.object_id,
                "observed_at": current,
                "exit_code": exit_code,
                "state": "active" if exit_code is None else "terminal",
            }
        evidence.json(f"observations/{_digest(result)}.json", result)
        return result

    def launch(self, acceptance, definition, command, environment):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance, definition
        )
        command, environment, request = self._accepted_request(
            value, command, environment
        )
        intent = self._existing_intent(value, request, evidence)
        create_result = self._create_result(value, request, intent, evidence)
        if intent is not None:
            return self.recover(acceptance, definition, command, environment)

        app = self._app(value, create=True)
        app, image, secrets, volumes = self._create_resources(value, app)
        intent = self._intent(value, request, self.now())
        intent_key = (
            f"{evidence.prefix}/create-intents/{value['ordinal']:02d}.json"
        )
        intent_raw = canonical_json(intent)
        if not self.store.create(intent_key, intent_raw, "application/json"):
            if self.store.get(intent_key) != intent_raw:
                existing = self._existing_intent(value, request, evidence)
                if existing is None:
                    raise ValueError("Modal create intent disappeared")
            return self.recover(acceptance, definition, command, environment)

        create_options = {
            "app": app,
            "name": request["execution_name"],
            "tags": request["tags"],
            "image": image,
            "env": environment,
            "secrets": secrets,
            "timeout": request["timeout_seconds"],
            "gpu": "H100!",
            "cpu": request["cpu_physical_cores"],
            "memory": request["memory_mib"],
            "workdir": request["workdir"],
            "volumes": volumes,
        }
        if request["encrypted_ports"]:
            create_options.update(
                encrypted_ports=request["encrypted_ports"],
                readiness_probe=self._call(
                    value, self.modal.Probe.with_tcp, 8000, create=True
                ),
            )
        try:
            sandbox = self._call(
                value,
                self.modal.Sandbox.create,
                *command,
                create=True,
                **create_options,
            )
        except Exception as error:
            result = {
                "schema_version": "milk.modal-sandbox-create-result.v1",
                "provider_acceptance_sha256": acceptance_sha256,
                "request_sha256": _digest(request),
                "execution_name": request["execution_name"],
                "observed_at": _utc_text(self.now()),
                "error_class_sha256": hashlib.sha256(
                    type(error).__name__.encode()
                ).hexdigest(),
                "state": "ambiguous",
            }
            self._validate_create_result(value, request, intent, result)
            evidence.json(f"create-results/{value['ordinal']:02d}.json", result)
            raise
        _opaque(getattr(sandbox, "object_id", None), "Modal sandbox object ID")
        result = {
            "schema_version": "milk.modal-sandbox-create-result.v1",
            "provider_acceptance_sha256": acceptance_sha256,
            "request_sha256": _digest(request),
            "execution_name": request["execution_name"],
            "execution_id": sandbox.object_id,
            "tags_sha256": _digest(request["tags"]),
            "observed_at": _utc_text(self.now()),
            "state": "created",
        }
        self._validate_create_result(value, request, intent, result)
        evidence.json(f"create-results/{value['ordinal']:02d}.json", result)
        self._call(value, sandbox.detach, create=True)
        return result

    def terminate(self, acceptance, definition, command, environment):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance, definition
        )
        unused_command, unused_environment, request = self._accepted_request(
            value, command, environment
        )
        intent = self._existing_intent(value, request, evidence)
        create_result = self._create_result(value, request, intent, evidence)
        if intent is None:
            raise ValueError("Modal termination requires an immutable create intent")
        app = self._app(value)
        sandbox = self._recover_handle(value, app)
        execution_id = (
            create_result["execution_id"]
            if create_result is not None and create_result["state"] == "created"
            else getattr(sandbox, "object_id", None)
        )
        if (
            sandbox is not None
            and execution_id is not None
            and sandbox.object_id != execution_id
        ):
            raise ValueError("named Modal sandbox ID differs from its create result")
        records = self._termination_records(
            value, request, evidence, execution_id
        )
        if sandbox is None:
            return self.verify_zero(acceptance, definition)
        exit_code = self._call(value, sandbox.poll)
        if exit_code is not None and type(exit_code) is not int:
            raise ValueError("Modal sandbox poll result is invalid")
        terminal_result = next(
            (
                result
                for unused_intent, result in records
                if result is not None and result["state"] == "terminated"
            ),
            None,
        )
        if terminal_result is not None:
            zero = self.verify_zero(acceptance, definition)
            if zero["state"] != "zero":
                raise ValueError("terminated Modal sandbox is still active")
            return terminal_result
        if exit_code is not None:
            return self.verify_zero(acceptance, definition)

        for attempt, (attempt_intent, unused_result) in enumerate(records):
            if attempt_intent is not None:
                continue
            stem = f"{value['ordinal']:02d}/{attempt:02d}.json"
            attempt_key = f"{evidence.prefix}/terminate-intents/{stem}"
            attempt_intent = {
                "schema_version": "milk.modal-sandbox-terminate-intent.v1",
                "provider_acceptance_sha256": acceptance_sha256,
                "request_sha256": _digest(request),
                "execution_name": request["execution_name"],
                "execution_id": sandbox.object_id,
                "attempt": attempt,
                "requested_at": _utc_text(self.now()),
            }
            self._validate_terminate_intent(
                value, request, sandbox.object_id, attempt, attempt_intent
            )
            if not self.store.create(
                attempt_key,
                canonical_json(attempt_intent),
                "application/json",
            ):
                return {
                    "schema_version": "milk.modal-sandbox-terminate-result.v1",
                    "provider_acceptance_sha256": acceptance_sha256,
                    "attempt": attempt,
                    "state": "in_progress",
                }
            try:
                self._call(value, sandbox.terminate, wait=True)
                exit_code = self._call(value, sandbox.poll)
                if exit_code is not None and type(exit_code) is not int:
                    raise ValueError("Modal sandbox poll result is invalid")
                result = {
                    "schema_version": "milk.modal-sandbox-terminate-result.v1",
                    "provider_acceptance_sha256": acceptance_sha256,
                    "request_sha256": _digest(request),
                    "execution_name": request["execution_name"],
                    "execution_id": sandbox.object_id,
                    "attempt": attempt,
                    "observed_at": _utc_text(self.now()),
                    "exit_code": exit_code,
                    "state": "terminated" if exit_code is not None else "ambiguous",
                }
            except Exception as error:
                result = {
                    "schema_version": "milk.modal-sandbox-terminate-result.v1",
                    "provider_acceptance_sha256": acceptance_sha256,
                    "request_sha256": _digest(request),
                    "execution_name": request["execution_name"],
                    "execution_id": sandbox.object_id,
                    "attempt": attempt,
                    "observed_at": _utc_text(self.now()),
                    "error_class_sha256": hashlib.sha256(
                        type(error).__name__.encode()
                    ).hexdigest(),
                    "state": "ambiguous",
                }
                self._validate_terminate_result(
                    value,
                    request,
                    sandbox.object_id,
                    attempt,
                    attempt_intent,
                    result,
                )
                evidence.json(f"terminate-results/{stem}", result)
                raise
            self._validate_terminate_result(
                value,
                request,
                sandbox.object_id,
                attempt,
                attempt_intent,
                result,
            )
            evidence.json(f"terminate-results/{stem}", result)
            return result
        return {
            "schema_version": "milk.modal-sandbox-terminate-result.v1",
            "provider_acceptance_sha256": acceptance_sha256,
            "state": "attempts_exhausted",
        }

    def verify_zero(self, acceptance, definition):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance, definition
        )
        app = self._app(value)
        sandboxes = self._call(
            value,
            self.modal.Sandbox.list,
            app_id=app.app_id,
            tags=_sandbox_tags(value),
        )
        active = []
        observed = 0
        for sandbox in self._guarded(value, sandboxes):
            observed += 1
            if observed > 1:
                raise ValueError("Modal sandbox identity is not unique")
            if self._call(value, sandbox.get_tags) != _sandbox_tags(value):
                raise ValueError("listed Modal sandbox tags differ")
            if getattr(sandbox, "name", _sandbox_name(value)) != _sandbox_name(value):
                raise ValueError("listed Modal sandbox name differs")
            exit_code = self._call(value, sandbox.poll)
            if exit_code is None:
                active.append(_opaque(sandbox.object_id, "Modal sandbox object ID"))
            elif type(exit_code) is not int:
                raise ValueError("Modal sandbox poll result is invalid")
        result = {
            "schema_version": "milk.modal-zero-gpu-verification.v1",
            "provider_acceptance_sha256": acceptance_sha256,
            "execution_name": _sandbox_name(value),
            "observed_at": _utc_text(self.now()),
            "matched_sandboxes": observed,
            "active_execution_ids": active,
            "state": "zero" if not active else "active",
        }
        evidence.json(f"zero-verifications/{_digest(result)}.json", result)
        return result

    def collect_logs(self, acceptance, definition, *, since, until):
        acceptance_sha256, evidence, value = self._load_acceptance(
            acceptance, definition
        )
        if (
            not isinstance(since, dt.datetime)
            or not isinstance(until, dt.datetime)
            or since.tzinfo is None
            or until.tzinfo is None
            or since.utcoffset() != dt.timedelta(0)
            or until.utcoffset() != dt.timedelta(0)
            or not since < until <= since + dt.timedelta(hours=48)
        ):
            raise ValueError("Modal log window is invalid")
        plan = {
            "schema_version": "milk.modal-log-collection-plan.v1",
            "provider_acceptance_sha256": acceptance_sha256,
            "execution_definition_sha256": _digest(definition),
            "since": _utc_text(since),
            "until": _utc_text(until),
            "source": None,
            "search_text": value["run_id"],
            "max_bytes": MAX_LOG_BYTES,
            "max_records": MAX_LOG_RECORDS,
        }
        stream_id = _digest(plan)
        evidence.json(f"log-plans/{stream_id}.json", plan)
        archive = LogArchive(
            evidence,
            "modal",
            stream_id=stream_id,
            max_run_bytes=MAX_LOG_BYTES,
        )
        complete = True
        missing_reason = None
        previous_at = None
        try:
            app = self._app(value)
            stream = self._call(
                value,
                app.logs.fetch,
                since=since,
                until=until,
                source=None,
                search_text=value["run_id"],
            )
            for record_number, entry in enumerate(self._guarded(value, stream)):
                if record_number >= MAX_LOG_RECORDS:
                    complete = False
                    missing_reason = "bounded_log_record_limit"
                    break
                if not isinstance(entry, self.modal.types.LogEntry):
                    raise ValueError("Modal log record is invalid")
                timestamp = entry.timestamp
                context_ids = entry.context_ids
                if (
                    not isinstance(entry.message, str)
                    or not isinstance(timestamp, dt.datetime)
                    or timestamp.tzinfo is None
                    or timestamp.utcoffset() != dt.timedelta(0)
                    or not since <= timestamp <= until
                    or previous_at is not None
                    and timestamp < previous_at
                    or entry.source not in {"stdout", "stderr", "system"}
                    or not isinstance(entry.object_id, str)
                    or OPAQUE.fullmatch(entry.object_id) is None
                    or not isinstance(context_ids, list)
                    or len(context_ids) > 32
                    or any(
                        not isinstance(item, str)
                        or OPAQUE.fullmatch(item) is None
                        for item in context_ids
                    )
                ):
                    raise ValueError("Modal log record is invalid")
                previous_at = timestamp
                cursor = _digest(
                    {
                        "source": entry.source,
                        "object_id": entry.object_id,
                        "context_ids": context_ids,
                    }
                )
                if not archive.write(
                    at=_utc_text(timestamp),
                    line=entry.message,
                    provider_cursor=cursor,
                ):
                    complete = False
                    missing_reason = "bounded_log_byte_limit"
                    break
        except Exception:
            archive.close(
                source_complete=False,
                missing_reason="provider_log_read_failed",
            )
            raise
        return archive.close(
            source_complete=complete,
            missing_reason=missing_reason,
        )

    def record_billing_cross_check(
        self, acceptance, definition, *, source, raw
    ):
        acceptance_sha256, evidence, unused_value = self._load_acceptance(
            acceptance, definition
        )
        if source not in {"rates", "report", "summary"}:
            raise ValueError("Modal billing cross-check source is invalid")
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= 1024 * 1024:
            raise ValueError("Modal billing cross-check must be bounded bytes")
        result = {
            "schema_version": "milk.modal-billing-cross-check.v1",
            "provider_acceptance_sha256": acceptance_sha256,
            "source": f"Workspace.billing.{source}",
            "observed_at": _utc_text(self.now()),
            "response_bytes": len(raw),
            "response_sha256": hashlib.sha256(raw).hexdigest(),
            "authoritative_for_budget": False,
        }
        evidence.json(f"billing-cross-checks/{_digest(result)}.json", result)
        return result
