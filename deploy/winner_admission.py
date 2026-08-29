import hashlib
import http.client
import json
import os
import re
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit


MAX_RESPONSE_BYTES = 64 * 1024
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MODEL_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
CHAT_PATH = re.compile(
    r"(?:/[A-Za-z0-9][A-Za-z0-9._~-]*)*/v1/chat/completions\Z"
)
MATERIALIZE_KEYS = {
    "schema_version",
    "student_job_id",
    "variant",
    "model_manifest_sha256",
    "file_count",
    "artifact_bytes",
    "stage_path",
    "model_path",
    "model_manifest_path",
}


def _canonical(value):
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()


def _strict(raw, label):
    def invalid_constant(constant):
        raise ValueError(f"{label} contains {constant}")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"{label} contains a duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=invalid_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not strict JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} is not an object")
    return value


def _read(path, maximum, label):
    with path.open("rb") as source:
        raw = source.read(maximum + 1)
    if not raw or len(raw) > maximum:
        raise ValueError(f"{label} has invalid size")
    return raw


def _request(
    factory,
    host,
    port,
    key,
    method,
    path,
    body=None,
    expected_statuses=frozenset({200}),
):
    connection = factory(
        host,
        port,
        timeout=30,
        context=ssl.create_default_context(),
    )
    headers = {"Accept": "application/json"}
    if key is not None:
        headers["Authorization"] = f"Bearer {key}"
    if body is not None:
        headers["Content-Type"] = "application/json"
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        content_type = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
        if response.status not in expected_statuses:
            raise ValueError("winner admission received an invalid HTTP response")
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("winner admission response has invalid size")
        if response.status == 200 and (not raw or content_type != "application/json"):
            raise ValueError("winner admission received an invalid HTTP response")
        return raw
    finally:
        connection.close()


def qualify(
    endpoint,
    alias,
    student_job_id,
    *,
    key_path=Path("/tmp/dragontales-candidate.key"),
    candidate_key=None,
    materialize_path=Path("/tmp/dragontales/materialize.stdout"),
    manifest_path=Path("/tmp/dragontales-winner/model-manifest.json"),
    expected_manifest_sha256=None,
    connection_factory=http.client.HTTPSConnection,
):
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or CHAT_PATH.fullmatch(parsed.path) is None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("winner admission endpoint is invalid")
    try:
        port = parsed.port or 443
    except ValueError as error:
        raise ValueError("winner admission endpoint port is invalid") from error
    prefix = parsed.path.removesuffix("/v1/chat/completions")
    models_path = f"{prefix}/v1/models"
    chat_path = f"{prefix}/v1/chat/completions"
    if not isinstance(alias, str) or MODEL_ALIAS.fullmatch(alias) is None:
        raise ValueError("winner admission model alias is invalid")
    if HEX64.fullmatch(student_job_id) is None:
        raise ValueError("winner admission student job is invalid")

    if candidate_key is None:
        key_raw = _read(key_path, 4_096, "candidate key")
    elif not isinstance(candidate_key, str):
        raise ValueError("candidate key is invalid")
    else:
        key_raw = candidate_key.encode("utf-8")
        if not 1 <= len(key_raw) <= 4_096:
            raise ValueError("candidate key is invalid")
    if any(byte < 0x21 or byte > 0x7E for byte in key_raw):
        raise ValueError("candidate key is invalid")
    key = key_raw.decode("ascii")

    materialize_raw = _read(materialize_path, 16 * 1024, "materialization receipt")
    if not materialize_raw.endswith(b"\n") or b"\n" in materialize_raw[:-1]:
        raise ValueError("materialization receipt is not one JSON line")
    materialize = _strict(materialize_raw[:-1], "materialization receipt")
    manifest_sha256 = materialize.get("model_manifest_sha256")
    if (
        set(materialize) != MATERIALIZE_KEYS
        or materialize.get("schema_version")
        != "dragontales.student-winner-materialization-receipt.v1"
        or materialize.get("student_job_id") != student_job_id
        or materialize.get("variant") not in {"bf16", "dynamic_fp8", "static_fp8"}
        or type(materialize.get("file_count")) is not int
        or materialize["file_count"] <= 0
        or type(materialize.get("artifact_bytes")) is not int
        or materialize["artifact_bytes"] <= 0
        or not isinstance(manifest_sha256, str)
        or HEX64.fullmatch(manifest_sha256) is None
        or materialize.get("stage_path") != "/tmp/dragontales-winner"
        or materialize.get("model_path") != "/tmp/dragontales-winner/model"
        or materialize.get("model_manifest_path")
        != "/tmp/dragontales-winner/model-manifest.json"
    ):
        raise ValueError("materialization receipt binding is invalid")
    if expected_manifest_sha256 is None:
        manifest_raw = _read(manifest_path, 1024 * 1024, "model manifest")
        expected_manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
    if (
        not isinstance(expected_manifest_sha256, str)
        or HEX64.fullmatch(expected_manifest_sha256) is None
        or expected_manifest_sha256 != manifest_sha256
    ):
        raise ValueError("materialized model manifest digest differs")

    _request(
        connection_factory,
        parsed.hostname,
        port,
        None,
        "GET",
        models_path,
        expected_statuses=frozenset({401, 403}),
    )
    models_raw = _request(
        connection_factory, parsed.hostname, port, key, "GET", models_path
    )
    models = _strict(models_raw, "models response")
    data = models.get("data")
    if (
        models.get("object") != "list"
        or not isinstance(data, list)
        or len(data) != 1
        or not isinstance(data[0], dict)
        or data[0].get("id") != alias
        or data[0].get("object") != "model"
    ):
        raise ValueError("models response does not expose the admitted alias")

    chat_request = _canonical(
        {
            "max_tokens": 8,
            "messages": [{"content": "Respond with OK.", "role": "user"}],
            "model": alias,
            "seed": 0,
            "stream": False,
            "temperature": 0,
        }
    )
    chat_raw = _request(
        connection_factory,
        parsed.hostname,
        port,
        key,
        "POST",
        chat_path,
        chat_request,
    )
    chat = _strict(chat_raw, "chat response")
    choices = chat.get("choices")
    message = choices[0].get("message") if isinstance(choices, list) and len(choices) == 1 else None
    content = message.get("content") if isinstance(message, dict) else None
    if (
        chat.get("object") != "chat.completion"
        or chat.get("model") != alias
        or not isinstance(chat.get("id"), str)
        or not chat["id"]
        or not isinstance(message, dict)
        or message.get("role") != "assistant"
        or not isinstance(content, str)
        or not content
        or len(content.encode()) > 4_096
    ):
        raise ValueError("chat response is not an admitted completion")

    return {
        "schema_version": "dragontales.winner-admission-probe.v1",
        "student_job_id": student_job_id,
        "student_variant": materialize["variant"],
        "model_manifest_sha256": manifest_sha256,
        "model_alias_sha256": hashlib.sha256(alias.encode()).hexdigest(),
        "candidate_api_key_sha256": hashlib.sha256(key_raw).hexdigest(),
        "models_response_sha256": hashlib.sha256(models_raw).hexdigest(),
        "chat_request_sha256": hashlib.sha256(chat_request).hexdigest(),
        "chat_response_sha256": hashlib.sha256(chat_raw).hexdigest(),
    }


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: admit ENDPOINT MODEL_ALIAS STUDENT_JOB_ID")
    try:
        candidate_key = os.environ.pop("DRAGONTALES_CANDIDATE_API_KEY", None)
        if candidate_key is None:
            raise ValueError("candidate key secret is required")
        print(
            json.dumps(
                qualify(*sys.argv[1:], candidate_key=candidate_key),
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
