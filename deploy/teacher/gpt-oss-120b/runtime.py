#!/usr/bin/env python3
"""Bounded loopback Chat Completions runtime for one pinned gpt-oss job."""

import hashlib
import hmac
import json
import os
import signal
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path, PurePosixPath


HERE = Path(__file__).resolve().parent
PROFILE_RAW = (HERE / "profile.json").read_bytes()
PROFILE = json.loads(PROFILE_RAW)
INVENTORY = json.loads((HERE / "model.manifest.json").read_text(encoding="utf-8"))

CHAT_PATH = "/v1/chat/completions"
HEALTH_PATH = "/health"
UPSTREAM_URL = PROFILE["ingress"]["upstream"]
HEALTH_URL = "http://127.0.0.1:8001/health"
INGRESS_HEALTH_URL = (
    f"http://{PROFILE['ingress']['host']}:{PROFILE['ingress']['port']}{HEALTH_PATH}"
)
MAX_REQUEST_BYTES = PROFILE["contract"]["max_request_bytes"]
MAX_RESPONSE_BYTES = PROFILE["contract"]["max_response_bytes"]
UPSTREAM_TIMEOUT_SECONDS = 175
MODEL_ROOT = Path(PROFILE["model"]["mount"])
API_KEY_ENV = PROFILE["ingress"]["api_key_environment"]
READINESS_BODY = json.dumps(
    {
        "schema_version": "dragontales.teacher-readiness.v1",
        "model": PROFILE["model"]["served_name"],
        "profile_sha256": hashlib.sha256(PROFILE_RAW).hexdigest(),
        "state": "ready",
    },
    separators=(",", ":"),
).encode("utf-8")
if len(READINESS_BODY) > 4096:
    raise RuntimeError("teacher readiness response exceeds limit")
SENSITIVE_ENV_PREFIXES = (
    "AWS_",
    "CF_",
    "CLOUDFLARE_",
    "DRAGONTALES_",
    "MILK_",
    "MODAL_",
    "R2_",
    "S3_",
)
SENSITIVE_ENV_NAMES = {"DOCKER_CONFIG", "GIT_ASKPASS", "KUBECONFIG", "NETRC", "SSH_AUTH_SOCK"}
SENSITIVE_ENV_WORDS = {
    "AUTH",
    "CERTIFICATE",
    "CREDENTIAL",
    "CREDENTIALS",
    "KEY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
}


class RequestRejected(ValueError):
    pass


class UpstreamFailed(RuntimeError):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _canonical_sha256(value):
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _object_without_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise RequestRejected("duplicate JSON member")
        value[key] = item
    return value


def _reject_constant(_value):
    raise RequestRejected("non-finite JSON number")


def _exact_keys(value, expected, description):
    if type(value) is not dict or set(value) != set(expected):
        raise RequestRejected(f"invalid {description}")


def prepare_forward(raw):
    """Validate the exact Dragontales teacher request and add one privacy control."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_REQUEST_BYTES:
        raise RequestRejected("invalid request size")
    try:
        text = raw.decode("utf-8")
        request = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise RequestRejected("invalid JSON") from error

    expected_keys = {
        "model",
        "messages",
        "n",
        "stream",
        "temperature",
        "top_p",
        "reasoning_effort",
        "max_tokens",
        "response_format",
    }
    _exact_keys(request, expected_keys, "teacher request")
    if request["model"] != PROFILE["model"]["served_name"]:
        raise RequestRejected("invalid model")

    messages = request["messages"]
    if type(messages) is not list or len(messages) != 2:
        raise RequestRejected("invalid messages")
    for message, role in zip(messages, ("system", "user")):
        _exact_keys(message, {"role", "content"}, "message")
        if message["role"] != role or type(message["content"]) is not str:
            raise RequestRejected("invalid message")
    system_bytes = messages[0]["content"].encode("utf-8")
    if hashlib.sha256(system_bytes).hexdigest() != PROFILE["contract"]["system_prompt_sha256"]:
        raise RequestRejected("invalid system prompt")

    if type(request["n"]) is not int or request["n"] != 1:
        raise RequestRejected("invalid n")
    if request["stream"] is not False:
        raise RequestRejected("streaming is forbidden")
    if (
        type(request["temperature"]) not in (int, float)
        or request["temperature"] != 1.0
        or type(request["top_p"]) not in (int, float)
        or request["top_p"] != 0.95
    ):
        raise RequestRejected("invalid sampling controls")
    if request["reasoning_effort"] != PROFILE["contract"]["reasoning_effort"]:
        raise RequestRejected("invalid reasoning effort")
    max_tokens = request["max_tokens"]
    if (
        type(max_tokens) is not int
        or not PROFILE["contract"]["min_output_tokens"]
        <= max_tokens
        <= PROFILE["contract"]["max_output_tokens"]
    ):
        raise RequestRejected("invalid output-token budget")
    if (
        _canonical_sha256(request["response_format"])
        != PROFILE["contract"]["response_format_sha256"]
    ):
        raise RequestRejected("invalid response format")

    forwarded = dict(request)
    forwarded["include_reasoning"] = False
    encoded = json.dumps(
        forwarded, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise RequestRejected("forwarded request exceeds limit")
    return encoded


def _read_bounded(response):
    body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise UpstreamFailed("upstream response exceeds limit")
    encoding = response.headers.get("Content-Encoding")
    if encoding is not None and encoding.lower() != "identity":
        raise UpstreamFailed("encoded upstream response")
    return body


def forward_once(body, opener=None):
    """Make one loopback request. The caller owns any retry decision."""
    opener = opener or urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirect()
    )
    request = urllib.request.Request(
        UPSTREAM_URL,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
        },
    )
    try:
        response = opener.open(request, timeout=UPSTREAM_TIMEOUT_SECONDS)
    except urllib.error.HTTPError as error:
        response = error
    except (OSError, urllib.error.URLError) as error:
        raise UpstreamFailed("loopback vLLM request failed") from error
    try:
        body = _read_bounded(response)
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        return response.status, content_type, body
    finally:
        response.close()


def _sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verify_model(root=MODEL_ROOT, require_readonly=True):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("model root must be a directory, not a symlink")
    if require_readonly and not os.statvfs(root).f_flag & os.ST_RDONLY:
        raise RuntimeError("model root must be mounted read-only")

    files = INVENTORY["files"]
    weight_names = set(INVENTORY["weights"]["files"])
    if len(weight_names) != 15:
        raise RuntimeError("model inventory must contain 15 weight shards")
    expected_names = []
    for item in files:
        relative = PurePosixPath(item["path"])
        if relative.is_absolute() or len(relative.parts) != 1 or relative.name in {"", ".", ".."}:
            raise RuntimeError("model inventory path must be one safe top-level name")
        expected_names.append(relative.name)
    if expected_names != sorted(set(expected_names)):
        raise RuntimeError("model inventory names must be sorted and unique")
    entries = sorted(root.iterdir(), key=lambda path: path.name)
    if [path.name for path in entries] != expected_names:
        raise RuntimeError("model root differs from the exact inventory")
    for path, item in zip(entries, files):
        relative = PurePosixPath(item["path"])
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != item["bytes"]:
            raise RuntimeError(f"model file metadata differs: {relative}")
        if _sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"model file digest differs: {relative}")

    index = json.loads((root / "model.safetensors.index.json").read_text("utf-8"))
    referenced = set(index["weight_map"].values())
    if referenced != weight_names:
        raise RuntimeError("model index and weight inventory differ")
    if index["metadata"]["total_size"] != INVENTORY["weights"]["tensor_bytes"]:
        raise RuntimeError("model tensor-byte total differs")
    weight_bytes = sum(item["bytes"] for item in files if item["path"] in weight_names)
    if weight_bytes != INVENTORY["weights"]["file_bytes"]:
        raise RuntimeError("model weight-file total differs")


class TeacherHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_arguments):
        return

    def _send(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _reject(self, status):
        self._send(
            status,
            "application/json",
            b'{"error":{"message":"request rejected","type":"invalid_request_error"}}',
        )

    def _authenticated(self):
        key = os.environ.get(API_KEY_ENV)
        if key is None:
            return False
        authorizations = self.headers.get_all("Authorization", [])
        return len(authorizations) == 1 and hmac.compare_digest(
            authorizations[0], "Bearer " + key
        )

    def do_GET(self):
        if self.path != HEALTH_PATH:
            self._reject(404)
            return
        if not self._authenticated():
            self._reject(401)
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self._reject(400)
            return
        lengths = self.headers.get_all("Content-Length", [])
        if lengths and (len(lengths) != 1 or lengths[0] != "0"):
            self._reject(400)
            return
        self.send_response_only(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Encoding", "identity")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(READINESS_BODY)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(READINESS_BODY)
        self.close_connection = True

    def do_POST(self):
        if self.path != CHAT_PATH:
            self._reject(404)
            return
        if not self._authenticated():
            self._reject(401)
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self._reject(400)
            return
        content_encodings = self.headers.get_all("Content-Encoding", [])
        if len(content_encodings) > 1 or (
            content_encodings and content_encodings[0].lower() != "identity"
        ):
            self._reject(415)
            return
        content_types = self.headers.get_all("Content-Type", [])
        if (
            len(content_types) != 1
            or content_types[0].partition(";")[0].strip().lower() != "application/json"
        ):
            self._reject(415)
            return
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not lengths[0].isdigit():
            self._reject(411)
            return
        length = int(lengths[0])
        if not 0 < length <= MAX_REQUEST_BYTES:
            self._reject(413)
            return
        self.connection.settimeout(10)
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise RequestRejected("short request")
            body = prepare_forward(raw)
            status, content_type, response = forward_once(body)
        except RequestRejected:
            self._reject(400)
            return
        except (OSError, UpstreamFailed):
            self._reject(502)
            return
        self._send(status, content_type, response)


def _wait_for_vllm(process):
    deadline = time.monotonic() + PROFILE["vllm"]["startup_timeout_seconds"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("vLLM exited before readiness")
        try:
            with opener.open(HEALTH_URL, timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(1)
    raise RuntimeError("vLLM readiness deadline exceeded")


def wait_ready(timeout_seconds=None, opener=None, sleep=time.sleep):
    key = os.environ.get(API_KEY_ENV)
    if key is None or not 1 <= len(key.encode("utf-8")) <= 4096:
        raise RuntimeError(f"{API_KEY_ENV} is required and must be bounded")
    timeout_seconds = timeout_seconds or PROFILE["vllm"]["startup_timeout_seconds"]
    opener = opener or urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirect()
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        request = urllib.request.Request(
            INGRESS_HEALTH_URL,
            method="GET",
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Authorization": "Bearer " + key,
            },
        )
        try:
            response = opener.open(request, timeout=1)
        except (OSError, urllib.error.URLError):
            sleep(1)
            continue
        try:
            body = response.read(4097)
            if (
                response.status != 200
                or body != READINESS_BODY
                or response.headers.get("Content-Type") != "application/json"
                or response.headers.get("Content-Encoding") != "identity"
            ):
                raise RuntimeError("teacher readiness response is invalid")
            return
        finally:
            response.close()
    raise RuntimeError("teacher readiness deadline exceeded")


def _stop(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        process.wait(timeout=10)


def _vllm_environment():
    def sensitive(name):
        upper = name.upper()
        return (
            upper in SENSITIVE_ENV_NAMES
            or upper.startswith(SENSITIVE_ENV_PREFIXES)
            or upper.endswith(("_ENDPOINT", "_URL"))
            or not SENSITIVE_ENV_WORDS.isdisjoint(upper.split("_"))
        )

    environment = {name: value for name, value in os.environ.items() if not sensitive(name)}
    environment.update(
        {
            "DO_NOT_TRACK": "1",
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "VLLM_NO_USAGE_STATS": "1",
        }
    )
    return environment


def _serve_while_child(server, process):
    server.timeout = 0.5
    while process.poll() is None:
        server.handle_request()
    raise RuntimeError("vLLM exited after readiness")


def serve():
    key = os.environ.get(API_KEY_ENV)
    if key is None or not 1 <= len(key.encode("utf-8")) <= 4096:
        raise RuntimeError(f"{API_KEY_ENV} is required and must be bounded")
    expected_date = PROFILE["vllm"]["system_start_date"]
    if os.environ.get("VLLM_SYSTEM_START_DATE") != expected_date:
        raise RuntimeError("VLLM_SYSTEM_START_DATE differs from the fixed profile")
    verify_model()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))

    process = None
    try:
        process = subprocess.Popen(
            PROFILE["vllm"]["argv"],
            close_fds=True,
            env=_vllm_environment(),
            start_new_session=True,
        )
        _wait_for_vllm(process)
        with HTTPServer(
            (PROFILE["ingress"]["host"], PROFILE["ingress"]["port"]),
            TeacherHandler,
        ) as server:
            _serve_while_child(server, process)
    finally:
        if process is not None:
            _stop(process)


def main():
    command = sys.argv[1:] or ["serve"]
    if command == ["serve"]:
        serve()
    elif command == ["wait-ready"]:
        wait_ready()
    elif command == ["verify-model"]:
        verify_model()
    else:
        raise SystemExit("usage: runtime.py [serve|wait-ready|verify-model]")


if __name__ == "__main__":
    main()
