from __future__ import annotations

import argparse
import base64
from collections import Counter
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal, ROUND_HALF_UP, localcontext
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from milk_harness.evidence import LocalEvidenceStore, R2EvidenceStore, canonical_json, create_same


CONFIG_SCHEMA = "milk.harness-run-config.v1"
CODE_VERSION = "milk.harness-run-once.v1"
TAXONOMY_VERSION = "milk.semantic-taxonomy.v1"
MAX_INCREMENTAL_SPEND_MICROUSD = 25_000_000
MAX_STOP_NEW_SPEND_MICROUSD = 20_000_000
MAX_TRACE_OBJECT_BYTES = 16 * 1024 * 1024
MAX_DECODED_TRACE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_SOURCE_BYTES = 512 * 1024 * 1024
MAX_SOURCE_TRACES = 3_000
PRODUCTION_SOURCE_TRACES = 3_000
MAX_ACCOUNTING_JOB_OBJECTS = 100_000
S3_LIST_PAGE_SIZE = 1_000
MAX_EVAL_TEXT_BYTES = 512 * 1024
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
WINDOW_KEY = re.compile(r"(?:traffic|stats)/(\d{4}/\d{2}/\d{2}/\d{2})/")

OPERATION_VALUES = (
    "answer",
    "summarize",
    "extract",
    "classify",
    "transform",
    "generate",
    "code",
    "plan_or_tool_use",
    "conversation",
    "other",
)
DOMAIN_VALUES = (
    "general",
    "software",
    "math_science",
    "business",
    "legal",
    "finance",
    "health",
    "creative",
    "other",
)
CAPABILITY_VALUES = (
    "knowledge",
    "reasoning",
    "instruction_following",
    "structured_output",
    "tool_use",
    "multimodal",
)
ORACLE_VALUES = (
    "exact",
    "schema",
    "executable",
    "reference",
    "pairwise_judge",
    "human",
)
ORACLES = frozenset(ORACLE_VALUES)


def _object(value, name, *, required, optional=()):
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be an object")
    required = set(required)
    allowed = required | set(optional)
    missing = required - value.keys()
    unknown = value.keys() - allowed
    if missing or unknown:
        raise ValueError(f"{name} has invalid fields: missing={sorted(missing)}, unknown={sorted(unknown)}")
    return value


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be in {minimum}..={maximum}")
    return value


def _string(value, name, *, maximum=512):
    if not isinstance(value, str) or not value or len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValueError(f"{name} is invalid")
    return value


def _identifier(value, name):
    value = _string(value, name, maximum=128)
    if SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{name} is invalid")
    return value


def _scope_id(value):
    value = _string(value, "scope_id", maximum=36)
    parsed = uuid.UUID(value)
    if str(parsed) != value or parsed.version is None:
        raise ValueError("scope_id must be a canonical UUID")
    return value


def _environment_name(value, name):
    value = _string(value, name, maximum=128)
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", value) is None:
        raise ValueError(f"{name} must be an environment variable name")
    return value


def _utc(value, name):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{name} must be a UTC timestamp") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{name} must be a UTC timestamp")
    return parsed


def _utc_text(value):
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _json(raw, name):
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return value


@dataclass(frozen=True)
class StoreConfig:
    kind: str
    root: str | None
    environment_prefix: str | None

    @classmethod
    def parse(cls, value):
        value = _object(value, "store", required={"kind"}, optional={"root", "environment_prefix"})
        kind = value["kind"]
        if kind == "local":
            if set(value) != {"kind", "root"}:
                raise ValueError("local store requires only kind and root")
            root = _string(value["root"], "store.root", maximum=4096)
            if not Path(root).is_absolute():
                raise ValueError("store.root must be absolute")
            return cls(kind, root, None)
        if kind == "r2":
            if set(value) != {"kind", "environment_prefix"}:
                raise ValueError("R2 store requires only kind and environment_prefix")
            prefix = _string(value["environment_prefix"], "store.environment_prefix", maximum=64)
            if re.fullmatch(r"[A-Z][A-Z0-9_]*_", prefix) is None:
                raise ValueError("store.environment_prefix is invalid")
            return cls(kind, None, prefix)
        raise ValueError("store.kind must be local or r2")

    def open(self, max_trace_object_bytes):
        if self.kind == "local":
            return _PipelineLocalStore(self.root, max_trace_object_bytes)
        return _PipelineR2Store.from_environment_with_trace_limit(
            self.environment_prefix, max_trace_object_bytes
        )


@dataclass(frozen=True)
class SourceConfig:
    close_delay_seconds: int
    max_windows: int
    max_stats_shards: int
    max_traces: int
    max_trace_object_bytes: int
    max_total_trace_bytes: int
    teacher_trace_bytes: int
    classifier_sample_sessions: int

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "source",
            required={
                "close_delay_seconds",
                "max_windows",
                "max_stats_shards",
                "max_traces",
                "max_trace_object_bytes",
                "max_total_trace_bytes",
                "teacher_trace_bytes",
                "classifier_sample_sessions",
            },
        )
        return cls(
            _integer(value["close_delay_seconds"], "source.close_delay_seconds", 0, 86_400),
            _integer(value["max_windows"], "source.max_windows", 1, 168),
            _integer(value["max_stats_shards"], "source.max_stats_shards", 1, 999),
            _integer(
                value["max_traces"],
                "source.max_traces",
                1,
                MAX_SOURCE_TRACES,
            ),
            _integer(
                value["max_trace_object_bytes"],
                "source.max_trace_object_bytes",
                1024,
                MAX_TRACE_OBJECT_BYTES,
            ),
            _integer(
                value["max_total_trace_bytes"],
                "source.max_total_trace_bytes",
                1024,
                MAX_TOTAL_SOURCE_BYTES,
            ),
            _integer(
                value["teacher_trace_bytes"],
                "source.teacher_trace_bytes",
                64,
                4096,
            ),
            _integer(value["classifier_sample_sessions"], "source.classifier_sample_sessions", 1, 750),
        )


@dataclass(frozen=True)
class TeacherConfig:
    api_url: str
    model: str
    reasoning_effort: str
    api_key_env: str
    timeout_seconds: int
    max_calls_per_run: int
    max_input_tokens_per_call: int
    max_output_tokens_per_call: int
    max_total_tokens_per_run: int
    input_rate_microusd_per_million: int
    output_rate_microusd_per_million: int

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "teacher",
            required={
                "api_url",
                "model",
                "reasoning_effort",
                "api_key_env",
                "timeout_seconds",
                "max_calls_per_run",
                "max_input_tokens_per_call",
                "max_output_tokens_per_call",
                "max_total_tokens_per_run",
                "input_rate_microusd_per_million",
                "output_rate_microusd_per_million",
            },
        )
        api_url = _string(value["api_url"], "teacher.api_url", maximum=2048)
        parsed = urllib.parse.urlsplit(api_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
            raise ValueError("teacher.api_url must be HTTPS without embedded credentials or a fragment")
        reasoning_effort = _string(
            value["reasoning_effort"], "teacher.reasoning_effort", maximum=8
        )
        if reasoning_effort not in {"low", "high", "max"}:
            raise ValueError("teacher.reasoning_effort must be low, high, or max")
        return cls(
            api_url,
            _string(value["model"], "teacher.model", maximum=256),
            reasoning_effort,
            _environment_name(value["api_key_env"], "teacher.api_key_env"),
            _integer(value["timeout_seconds"], "teacher.timeout_seconds", 1, 120),
            _integer(value["max_calls_per_run"], "teacher.max_calls_per_run", 1, 2),
            _integer(value["max_input_tokens_per_call"], "teacher.max_input_tokens_per_call", 128, 100_000),
            _integer(value["max_output_tokens_per_call"], "teacher.max_output_tokens_per_call", 64, 16_384),
            _integer(value["max_total_tokens_per_run"], "teacher.max_total_tokens_per_run", 192, 250_000),
            _integer(value["input_rate_microusd_per_million"], "teacher.input_rate", 0, 1_000_000_000),
            _integer(value["output_rate_microusd_per_million"], "teacher.output_rate", 0, 1_000_000_000),
        )

    def reserved_cost(self):
        return _token_cost(self.max_input_tokens_per_call, self.input_rate_microusd_per_million) + _token_cost(
            self.max_output_tokens_per_call, self.output_rate_microusd_per_million
        )

    def public_binding(self):
        return {
            "api_url": self.api_url,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "timeout_seconds": self.timeout_seconds,
            "max_input_tokens": self.max_input_tokens_per_call,
            "max_output_tokens": self.max_output_tokens_per_call,
            "input_rate_microusd_per_million": self.input_rate_microusd_per_million,
            "output_rate_microusd_per_million": self.output_rate_microusd_per_million,
        }


@dataclass(frozen=True)
class BudgetConfig:
    starting_spend_microusd: int
    stop_new_spend_microusd: int
    absolute_spend_microusd: int

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "budget",
            required={"starting_spend_microusd", "stop_new_spend_microusd", "absolute_spend_microusd"},
        )
        starting = _integer(value["starting_spend_microusd"], "budget.starting_spend_microusd", 0, MAX_INCREMENTAL_SPEND_MICROUSD)
        stop = _integer(value["stop_new_spend_microusd"], "budget.stop_new_spend_microusd", 0, MAX_STOP_NEW_SPEND_MICROUSD)
        absolute = _integer(value["absolute_spend_microusd"], "budget.absolute_spend_microusd", 1, MAX_INCREMENTAL_SPEND_MICROUSD)
        if starting > stop or stop > absolute:
            raise ValueError("budget requires starting <= stop_new <= absolute")
        return cls(starting, stop, absolute)


@dataclass(frozen=True)
class EvalConfig:
    series_id: str
    representative_cases: int
    tail_cases: int
    max_source_traces: int

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "eval",
            required={
                "series_id",
                "representative_cases",
                "tail_cases",
                "max_source_traces",
            },
        )
        return cls(
            _identifier(value["series_id"], "eval.series_id"),
            _integer(value["representative_cases"], "eval.representative_cases", 1, 100),
            _integer(value["tail_cases"], "eval.tail_cases", 1, 100),
            _integer(value["max_source_traces"], "eval.max_source_traces", 1, 256),
        )


@dataclass(frozen=True)
class RouteProposalConfig:
    enabled: bool
    candidate_id: str
    api_base_url: str
    model: str
    candidate_basis_points: int

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "route_proposal",
            required={"enabled", "candidate_id", "api_base_url", "model", "candidate_basis_points"},
        )
        if type(value["enabled"]) is not bool:
            raise ValueError("route_proposal.enabled must be a boolean")
        api_base_url = _string(value["api_base_url"], "route_proposal.api_base_url", maximum=2048)
        parsed = urllib.parse.urlsplit(api_base_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("route_proposal.api_base_url must be an HTTPS base URL")
        api_base_url = api_base_url.rstrip("/") + "/"
        if not urllib.parse.urlsplit(api_base_url).path.endswith("/v1/"):
            raise ValueError("route_proposal.api_base_url must end in /v1/")
        return cls(
            value["enabled"],
            _identifier(value["candidate_id"], "route_proposal.candidate_id"),
            api_base_url,
            _string(value["model"], "route_proposal.model", maximum=256),
            _integer(
                value["candidate_basis_points"],
                "route_proposal.candidate_basis_points",
                0,
                1000,
            ),
        )


@dataclass(frozen=True)
class RunConfig:
    scope_id: str
    profile: str
    store: StoreConfig
    source: SourceConfig
    teacher: TeacherConfig
    budget: BudgetConfig
    eval: EvalConfig
    route_proposal: RouteProposalConfig

    @classmethod
    def parse(cls, value):
        value = _object(
            value,
            "config",
            required={"schema_version", "scope_id", "profile", "store", "source", "teacher", "budget", "eval", "route_proposal"},
        )
        if value["schema_version"] != CONFIG_SCHEMA:
            raise ValueError(f"schema_version must be {CONFIG_SCHEMA}")
        if value["profile"] not in {"production", "mechanics"}:
            raise ValueError("profile must be production or mechanics")
        config = cls(
            _scope_id(value["scope_id"]),
            value["profile"],
            StoreConfig.parse(value["store"]),
            SourceConfig.parse(value["source"]),
            TeacherConfig.parse(value["teacher"]),
            BudgetConfig.parse(value["budget"]),
            EvalConfig.parse(value["eval"]),
            RouteProposalConfig.parse(value["route_proposal"]),
        )
        if config.profile == "production":
            if config.source.max_traces != PRODUCTION_SOURCE_TRACES:
                raise ValueError(
                    f"production source.max_traces must be {PRODUCTION_SOURCE_TRACES}"
                )
            if config.source.classifier_sample_sessions != 750:
                raise ValueError(
                    "production classifier sample must be 750"
                )
        return config

    @classmethod
    def load(cls, path):
        path = Path(path)
        raw = path.read_bytes()
        if len(raw) > 1024 * 1024:
            raise ValueError("config is oversized")
        return cls.parse(_json(raw, "config"))

    @property
    def prefix(self):
        return f"milk/v1/scopes/{self.scope_id}"


@dataclass(frozen=True)
class Trace:
    key: str
    object_sha256: str
    request_id: str
    occurred_at: dt.datetime
    endpoint: str
    session_hmac: str
    independent: bool
    catalog: dict
    request: dict | None
    response: dict | None
    request_raw: bytes
    response_raw: bytes
    parse_success: bool
    unknown_items: int
    total_items: int
    model: str | None
    input_tokens: int | None
    output_tokens: int | None
    cached_tokens: int | None
    reasoning_tokens: int | None


@dataclass(frozen=True)
class TeacherResponse:
    value: dict
    provider_request_id: str
    input_tokens: int
    output_tokens: int


class DirectTeacher:
    def __init__(self, config, opener=None):
        self.config = config
        self.opener = opener or urllib.request.build_opener(_NoRedirect)

    def complete(self, *, task, instructions, payload, job_id):
        api_key = os.environ.get(self.config.api_key_env)
        if not api_key:
            raise ValueError(f"{self.config.api_key_env} is required")
        body = _teacher_request_body(self.config, instructions, payload)
        if _conservative_token_bound(body) > self.config.max_input_tokens_per_call:
            raise ValueError(f"{task} teacher request exceeds the input-token cap")
        request = urllib.request.Request(
            self.config.api_url,
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + api_key,
                "Content-Type": "application/json",
                "Idempotency-Key": job_id,
                "User-Agent": "milk-harness-run-once/1",
            },
        )
        with self.opener.open(request, timeout=self.config.timeout_seconds) as response:
            raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("teacher response is oversized")
            provider_request_id = response.headers.get("x-request-id") or response.headers.get("request-id") or "unavailable"
        envelope = _json(raw, "teacher response")
        try:
            content = envelope["choices"][0]["message"]["content"]
            usage = envelope["usage"]
            input_tokens = usage.get("prompt_tokens", usage.get("input_tokens"))
            output_tokens = usage.get("completion_tokens", usage.get("output_tokens"))
        except (KeyError, IndexError, TypeError) as error:
            raise ValueError("teacher response does not match the OpenAI-compatible contract") from error
        if not isinstance(content, str):
            raise ValueError("teacher response content must be a JSON string")
        value = _json(content.encode(), "teacher response content")
        input_tokens = _integer(input_tokens, "teacher input_tokens", 0, self.config.max_input_tokens_per_call)
        output_tokens = _integer(output_tokens, "teacher output_tokens", 0, self.config.max_output_tokens_per_call)
        return TeacherResponse(value, _string(provider_request_id, "provider request ID", maximum=512), input_tokens, output_tokens)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


class _PipelineLocalStore(LocalEvidenceStore):
    def __init__(self, root, max_trace_object_bytes):
        super().__init__(root)
        self.max_trace_object_bytes = max_trace_object_bytes

    def get(self, key):
        if "/traffic/" not in key:
            return super().get(key)
        path = self._path(key)
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(key)
        with path.open("rb") as source:
            body = source.read(self.max_trace_object_bytes + 1)
        if len(body) > self.max_trace_object_bytes:
            raise ValueError("stored traffic object is oversized")
        return body

    def get_versioned(self, key):
        body = self.get(key)
        return body, '"' + hashlib.sha256(body).hexdigest() + '"'


class _PipelineR2Store(R2EvidenceStore):
    def __init__(self, *, max_trace_object_bytes, **kwargs):
        super().__init__(**kwargs)
        self.max_trace_object_bytes = max_trace_object_bytes

    @classmethod
    def from_environment_with_trace_limit(cls, prefix, max_trace_object_bytes):
        def required(name):
            value = os.environ.get(prefix + name, "")
            if not value:
                raise ValueError(f"{prefix}{name} is required")
            return value

        return cls(
            account_id=required("ACCOUNT_ID"),
            bucket=required("BUCKET"),
            access_key_id=required("ACCESS_KEY_ID"),
            secret_access_key=required("SECRET_ACCESS_KEY"),
            session_token=os.environ.get(prefix + "SESSION_TOKEN") or None,
            max_trace_object_bytes=max_trace_object_bytes,
        )

    def get(self, key):
        body, unused_etag = self.get_versioned(key)
        del unused_etag
        return body

    def get_versioned(self, key):
        if "/traffic/" not in key:
            return super().get_versioned(key)
        request = self._request("GET", key)
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                if response.getcode() != 200:
                    raise RuntimeError("R2 get returned an unexpected status")
                body = response.read(self.max_trace_object_bytes + 1)
                etag = response.headers.get("ETag")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise FileNotFoundError(key) from error
            raise
        if len(body) > self.max_trace_object_bytes:
            raise ValueError("stored traffic object is oversized")
        if not isinstance(etag, str) or not etag:
            raise ValueError("R2 get response has no ETag")
        return body, etag


def _token_cost(tokens, rate):
    return (tokens * rate + 999_999) // 1_000_000


def _conservative_token_bound(raw):
    if not isinstance(raw, bytes):
        raise TypeError("token-bound input must be bytes")
    # The request is UTF-8 JSON. A byte-fallback tokenizer cannot emit more
    # content tokens than input bytes, so bytes are the portable upper bound;
    # chars/4 is only an average and is unsafe for code or arbitrary Unicode.
    return len(raw)


def _teacher_request_body(config, instructions, payload):
    return canonical_json(
        {
            "model": config.model,
            "reasoning_effort": config.reasoning_effort,
            "messages": [
                {"role": "system", "content": instructions},
                {
                    "role": "user",
                    "content": json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                },
            ],
            "temperature": 0,
            "max_tokens": config.max_output_tokens_per_call,
            "response_format": {"type": "json_object"},
        }
    )


def _decompress_trace(key, raw):
    if key.endswith(".json"):
        return raw
    if not key.endswith(".json.zst"):
        raise ValueError("traffic object suffix is invalid")
    command = shutil.which("zstd")
    if command is None:
        raise RuntimeError("zstd is required to read gateway traffic objects")
    with tempfile.TemporaryFile() as compressed:
        compressed.write(raw)
        compressed.seek(0)
        process = subprocess.Popen(
            [command, "--decompress", "--stdout", "--quiet"],
            stdin=compressed,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = bytearray()
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 15
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError("zstd decompression timed out")
                chunk = os.read(process.stdout.fileno(), 64 * 1024)
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > MAX_DECODED_TRACE_BYTES:
                    raise ValueError("decoded traffic object is oversized")
            return_code = process.wait(
                timeout=max(0.1, deadline - time.monotonic())
            )
            stderr = process.stderr.read(8193)
            if return_code != 0 or len(stderr) > 8192:
                raise ValueError("traffic object is not valid zstd")
        finally:
            selector.close()
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
            process.stderr.close()
        return bytes(output)


def _body(value, name):
    value = _object(value, name, required={"content_type", "content_encoding", "body_base64", "byte_len"})
    encoded = value["body_base64"]
    if not isinstance(encoded, str):
        raise ValueError(f"{name}.body_base64 is invalid")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise ValueError(f"{name}.body_base64 is invalid") from error
    if len(raw) != _integer(value["byte_len"], f"{name}.byte_len", 0, MAX_DECODED_TRACE_BYTES):
        raise ValueError(f"{name} byte length differs")
    encoding = value["content_encoding"]
    if encoding not in {None, "", "identity"}:
        return raw, None
    content_type = value["content_type"]
    if content_type is not None and not isinstance(content_type, str):
        raise ValueError(f"{name}.content_type is invalid")
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw, None
    return raw, parsed if isinstance(parsed, dict) else None


def _stream_response(endpoint, raw):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("stream response is not UTF-8 SSE") from error
    events = []
    terminal = False
    for block in re.split(r"\r?\n\r?\n", text):
        if not block.strip():
            continue
        event_type = None
        data = []
        for line in block.splitlines():
            if line.startswith("event:"):
                event_type = line.removeprefix("event:").strip()
            elif line.startswith("data:"):
                data.append(line.removeprefix("data:").lstrip())
        if not data:
            continue
        payload = "\n".join(data)
        if payload == "[DONE]":
            terminal = endpoint == "chat_completions"
            continue
        try:
            value = json.loads(payload)
        except json.JSONDecodeError as error:
            raise ValueError("stream response contains invalid JSON data") from error
        if not isinstance(value, dict):
            raise ValueError("stream response event data must be an object")
        events.append(value)
        if endpoint == "responses" and (
            event_type == "response.completed"
            or value.get("type") == "response.completed"
        ):
            terminal = True
    if not terminal:
        raise ValueError("stream response has no terminal event")
    if endpoint == "responses":
        for event in reversed(events):
            response = event.get("response")
            if isinstance(response, dict) and (
                event.get("type") == "response.completed"
                or response.get("status") == "completed"
            ):
                return response
        raise ValueError("Responses stream has no completed response object")
    choices = []
    usage = None
    response_id = None
    for event in events:
        if isinstance(event.get("id"), str):
            response_id = event["id"]
        if isinstance(event.get("choices"), list):
            choices.extend(
                choice for choice in event["choices"] if isinstance(choice, dict)
            )
        if isinstance(event.get("usage"), dict):
            usage = event["usage"]
    return {"id": response_id, "choices": choices, "usage": usage}


def _catalog(value, scope_id, key_request_id):
    if not isinstance(value, dict):
        raise ValueError("trace catalog must be an object")
    required = {
        "scope_id",
        "request_id",
        "occurred_at",
        "endpoint",
        "request_parse_success",
        "streaming",
        "route_revision",
        "route_observation",
        "provider_status",
        "error_class",
        "ttft_ms",
        "completion_ms",
        "request_bytes",
        "response_bytes",
        "sampling_algorithm",
        "sampling_policy_version",
        "inclusion_probability_basis_points",
        "capture_eligible",
        "capture_selected",
        "sampling_unit_kind",
        "sampling_unit_hmac_sha256",
        "sampling_independence",
        "sampling_key_version",
        "previous_response_hmac_sha256",
        "rights_state",
        "retention_until",
    }
    missing = required - value.keys()
    if missing:
        raise ValueError(f"trace catalog is missing {sorted(missing)}")
    if value["scope_id"] != scope_id or value["request_id"] != key_request_id:
        raise ValueError("trace catalog identity differs from its key")
    request_id = uuid.UUID(value["request_id"])
    if str(request_id) != value["request_id"] or request_id.version != 7:
        raise ValueError("trace request_id must be UUIDv7")
    if value["endpoint"] not in {"responses", "chat_completions"}:
        raise ValueError("trace endpoint is invalid")
    for field in (
        "request_parse_success",
        "streaming",
        "capture_eligible",
        "capture_selected",
    ):
        if type(value[field]) is not bool:
            raise ValueError(f"trace {field} must be boolean")
    if value["sampling_algorithm"] != "hmac_sha256_session_root_v1":
        raise ValueError("trace sampling_algorithm is invalid")
    session_hmac = value["sampling_unit_hmac_sha256"]
    if not isinstance(session_hmac, str) or HEX64.fullmatch(session_hmac) is None:
        raise ValueError("trace sampling_unit_hmac_sha256 is invalid")
    if value["sampling_independence"] not in {"independent", "uncertain"}:
        raise ValueError("trace sampling_independence is invalid")
    if value["sampling_unit_kind"] not in {
        "chat_session_header",
        "responses_conversation",
        "request",
    }:
        raise ValueError("trace sampling_unit_kind is invalid")
    if (
        value["sampling_independence"] == "independent"
        and value["sampling_unit_kind"] == "request"
    ):
        raise ValueError("request sampling units cannot assert independence")
    previous = value["previous_response_hmac_sha256"]
    if previous is not None and (
        not isinstance(previous, str) or HEX64.fullmatch(previous) is None
    ):
        raise ValueError("trace previous_response_hmac_sha256 is invalid")
    _integer(value["inclusion_probability_basis_points"], "trace inclusion probability", 1, 10_000)
    for field in ("request_bytes", "response_bytes"):
        _integer(value[field], "trace " + field, 0, MAX_DECODED_TRACE_BYTES)
    for field in ("provider_status", "ttft_ms", "completion_ms"):
        if value[field] is not None:
            _integer(value[field], "trace " + field, 0, 86_400_000)
    return request_id, _utc(value["occurred_at"], "trace occurred_at"), session_hmac


def _unknown_items(endpoint, request, response):
    known = {
        "message",
        "input_text",
        "output_text",
        "text",
        "input_image",
        "image_url",
        "input_file",
        "file",
        "function_call",
        "function_call_output",
        "tool_call",
        "refusal",
        "reasoning",
    }
    total = 0
    unknown = 0

    def visit(value):
        nonlocal total, unknown
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            item_type = value.get("type")
            if isinstance(item_type, str):
                total += 1
                if item_type not in known:
                    unknown += 1
            for key in ("content", "input", "output"):
                if key in value:
                    visit(value[key])

    if endpoint == "responses":
        visit(request.get("input") if request else None)
        visit(response.get("output") if response else None)
    else:
        visit(request.get("messages") if request else None)
        visit(response.get("choices") if response else None)
    return unknown, total


def _usage(endpoint, response):
    if response is None or not isinstance(response.get("usage"), dict):
        return (None, None, None, None)
    usage = response["usage"]
    if endpoint == "responses":
        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")
        cached = usage.get("input_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("input_tokens_details"), dict) else None
        reasoning = usage.get("output_tokens_details", {}).get("reasoning_tokens") if isinstance(usage.get("output_tokens_details"), dict) else None
    else:
        input_tokens = usage.get("prompt_tokens")
        output_tokens = usage.get("completion_tokens")
        cached = usage.get("prompt_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("prompt_tokens_details"), dict) else None
        reasoning = usage.get("completion_tokens_details", {}).get("reasoning_tokens") if isinstance(usage.get("completion_tokens_details"), dict) else None
    values = []
    for value in (input_tokens, output_tokens, cached, reasoning):
        values.append(value if type(value) is int and value >= 0 else None)
    return tuple(values)


def _parse_trace(store, config, key):
    if config.profile == "production" and not key.endswith(".json.zst"):
        raise ValueError("production traffic objects must use .json.zst")
    raw = store.get(key)
    object_sha256 = hashlib.sha256(raw).hexdigest()
    value = _json(_decompress_trace(key, raw), "traffic object")
    value = _object(value, "traffic object", required={"schema_version", "catalog", "request", "response"})
    if value["schema_version"] != "milk.trace.v1":
        raise ValueError("traffic schema_version must be milk.trace.v1")
    key_request_id = key.rsplit("/", 1)[1].removesuffix(".json.zst").removesuffix(".json")
    request_id, occurred_at, session_hmac = _catalog(value["catalog"], config.scope_id, key_request_id)
    key_window = _window_from_key(key)
    uuid_millis = request_id.int >> 80
    uuid_time = dt.datetime.fromtimestamp(
        uuid_millis // 1000,
        tz=dt.timezone.utc,
    ) + dt.timedelta(milliseconds=uuid_millis % 1000)
    if occurred_at.replace(minute=0, second=0, microsecond=0) != key_window:
        raise ValueError("trace occurred_at differs from its key window")
    if uuid_time.replace(minute=0, second=0, microsecond=0) != key_window:
        raise ValueError("trace UUIDv7 timestamp differs from its key window")
    request_raw, request = _body(value["request"], "trace request")
    response_raw, response = _body(value["response"], "trace response")
    if len(request_raw) != value["catalog"]["request_bytes"] or len(response_raw) != value["catalog"]["response_bytes"]:
        raise ValueError("trace body byte counts differ from catalog")
    endpoint = value["catalog"]["endpoint"]
    response_content_type = value["response"]["content_type"]
    is_sse = isinstance(response_content_type, str) and (
        response_content_type.split(";", 1)[0].strip().lower()
        == "text/event-stream"
    )
    if value["catalog"]["streaming"] and is_sse:
        response = _stream_response(endpoint, response_raw)
    unknown, total = _unknown_items(endpoint, request, response)
    usage = _usage(endpoint, response)
    model = request.get("model") if request and isinstance(request.get("model"), str) else None
    return Trace(
        key,
        object_sha256,
        str(request_id),
        occurred_at,
        endpoint,
        session_hmac,
        value["catalog"]["sampling_independence"] == "independent",
        value["catalog"],
        request,
        response,
        request_raw,
        response_raw,
        value["catalog"]["request_parse_success"]
        and request is not None
        and response is not None,
        unknown,
        total,
        model,
        *usage,
    )


def _window_from_key(key):
    match = WINDOW_KEY.search(key)
    if match is None:
        raise ValueError("source object key has no hourly window")
    return dt.datetime.strptime(match.group(1), "%Y/%m/%d/%H").replace(tzinfo=dt.timezone.utc)


def _list_up_to(store, prefix, count, *, start_after=None):
    keys = []
    cursor = start_after
    while len(keys) < count:
        page_limit = min(S3_LIST_PAGE_SIZE, count - len(keys))
        page = store.list(prefix, limit=page_limit, start_after=cursor)
        if any(not isinstance(key, str) for key in page) or page != sorted(page):
            raise ValueError(f"{prefix} returned an invalid object page")
        if cursor is not None and any(key <= cursor for key in page):
            raise ValueError(f"{prefix} object pagination did not advance")
        if len(page) != len(set(page)):
            raise ValueError(f"{prefix} returned duplicate object keys")
        keys.extend(page)
        if len(page) < page_limit:
            break
        cursor = page[-1]
    return keys


def _source_page(store, prefix, limit, start_after, closed_before):
    keys = _list_up_to(store, prefix, limit + 1, start_after=start_after)
    overflow_window = None
    if len(keys) > limit:
        overflow = keys.pop()
        window = _window_from_key(overflow)
        if window + dt.timedelta(hours=1) <= closed_before:
            overflow_window = window
    closed = [
        key
        for key in keys
        if _window_from_key(key) + dt.timedelta(hours=1) <= closed_before
    ]
    if overflow_window is not None:
        closed = [key for key in closed if _window_from_key(key) != overflow_window]
        if not closed:
            raise ValueError(f"one closed window under {prefix} exceeds its object bound")
    return closed


def _stats_shard(store, config, key, window):
    raw = store.get(key)
    value = _json(raw, "stats shard")
    required = {
        "schema_version",
        "scope_id",
        "writer_id",
        "flush_id",
        "hour",
        "recorded_at",
        "sampling_algorithm",
        "sampling_policy_version",
        "sampling_key_version",
        "inclusion_probability_basis_points",
        "values",
    }
    value = _object(value, "stats shard", required=required)
    if value["schema_version"] != "milk.stats-shard.v1" or value["scope_id"] != config.scope_id:
        raise ValueError("stats shard identity is invalid")
    if _utc(value["hour"], "stats hour") != window:
        raise ValueError("stats shard hour differs from its key")
    if value["sampling_algorithm"] != "hmac_sha256_session_root_v1":
        raise ValueError("stats sampling_algorithm is invalid")
    policy_version = _string(
        value["sampling_policy_version"],
        "stats sampling_policy_version",
        maximum=256,
    )
    key_version = _string(
        value["sampling_key_version"],
        "stats sampling_key_version",
        maximum=128,
    )
    basis_points = _integer(
        value["inclusion_probability_basis_points"],
        "stats inclusion_probability_basis_points",
        1,
        10_000,
    )
    path_writer, path_flush = key.rsplit("/", 2)[-2:]
    path_flush = path_flush.removesuffix(".json")
    if value["writer_id"] != path_writer or value["flush_id"] != path_flush:
        raise ValueError("stats writer or flush identity differs from its key")
    for field in ("writer_id", "flush_id"):
        parsed = uuid.UUID(value[field])
        if str(parsed) != value[field] or parsed.version != 7:
            raise ValueError(f"stats {field} must be UUIDv7")
    if _utc(value["recorded_at"], "stats recorded_at") < window:
        raise ValueError("stats recorded_at precedes its hour")
    if not isinstance(value["values"], dict):
        raise ValueError("stats values must be an object")
    _validate_counts(value["values"], "stats values")
    binding = {
        "sampling_algorithm": value["sampling_algorithm"],
        "sampling_policy_version": policy_version,
        "sampling_key_version": key_version,
        "inclusion_probability_basis_points": basis_points,
    }
    return value, hashlib.sha256(raw).hexdigest(), binding


def _validate_counts(value, name):
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{name} has an invalid key")
            _validate_counts(item, name)
    elif isinstance(value, list):
        for item in value:
            _validate_counts(item, name)
    elif type(value) is not int or value < 0:
        raise ValueError(f"{name} must contain only nonnegative integer counters")


def _merge_counts(target, source):
    for key, value in source.items():
        if isinstance(value, dict):
            current = target.setdefault(key, {})
            if not isinstance(current, dict):
                raise ValueError("stats counter type changed between shards")
            _merge_counts(current, value)
        elif isinstance(value, list):
            current = target.setdefault(key, [0] * len(value))
            if not isinstance(current, list) or len(current) != len(value):
                raise ValueError("stats bucket layout changed between shards")
            for index, item in enumerate(value):
                current[index] += item
        else:
            current = target.setdefault(key, 0)
            if type(current) is not int:
                raise ValueError("stats counter type changed between shards")
            target[key] = current + value


def _validate_merged_stats(stats):
    required = {
        "observed",
        "request_parse_success",
        "request_parse_failure",
        "eligible",
        "selected",
        "captured",
        "not_selected",
        "oversized",
        "interrupted",
        "capture_failed",
        "queued",
        "dropped",
        "traces_persisted",
        "trace_persist_failures",
        "stats_persist_failures",
    }
    missing = required - stats.keys()
    if missing:
        raise ValueError(f"merged stats are missing {sorted(missing)}")
    observed = _counter(stats, "observed")
    eligible = _counter(stats, "eligible")
    selected = _counter(stats, "selected")
    captured = _counter(stats, "captured")
    terminal = sum(
        _counter(stats, key)
        for key in (
            "captured",
            "not_selected",
            "oversized",
            "interrupted",
            "capture_failed",
        )
    )
    if (
        not captured <= selected <= eligible <= observed
        or _counter(stats, "queued") != observed
        or terminal != observed
        or _counter(stats, "traces_persisted") != captured
        or _counter(stats, "trace_persist_failures")
        != _counter(stats, "capture_failed")
        or _counter(stats, "request_parse_success")
        + _counter(stats, "request_parse_failure")
        != observed
    ):
        raise ValueError("merged stats do not conserve gateway observations")


def _validate_window_pairing(stats_by_window, traces):
    traces_by_window = Counter(
        trace.occurred_at.replace(minute=0, second=0, microsecond=0)
        for trace in traces
    )
    for window in sorted(set(stats_by_window) | set(traces_by_window)):
        window_stats = stats_by_window.get(window)
        if window_stats is None:
            raise ValueError("closed traffic window has no statistics shard")
        _validate_merged_stats(window_stats)
        if _counter(window_stats, "captured") != traces_by_window[window]:
            raise ValueError(
                "closed window retained trace count differs from its statistics"
            )


def _collect(store, config, now, watermark):
    closed_before = now - dt.timedelta(seconds=config.source.close_delay_seconds)
    stats_keys = _source_page(
        store,
        config.prefix + "/stats",
        config.source.max_stats_shards,
        watermark["stats_start_after"],
        closed_before,
    )
    traffic_keys = _source_page(
        store,
        config.prefix + "/traffic",
        config.source.max_traces,
        watermark["traffic_start_after"],
        closed_before,
    )
    candidates = sorted({_window_from_key(key) for key in stats_keys + traffic_keys})[
        : config.source.max_windows
    ]
    selected = set(candidates)
    stats_keys = [key for key in stats_keys if _window_from_key(key) in selected]
    traffic_keys = [key for key in traffic_keys if _window_from_key(key) in selected]
    stats = {}
    stats_by_window = {}
    stats_refs = []
    sampling_bindings = {}
    for key in stats_keys:
        window = _window_from_key(key)
        value, sha256, binding = _stats_shard(
            store, config, key, window
        )
        _merge_counts(stats, value["values"])
        _merge_counts(stats_by_window.setdefault(window, {}), value["values"])
        stats_refs.append({"key": key, "sha256": sha256})
        sampling_bindings[_digest(binding)] = binding
    if len(sampling_bindings) > 1:
        raise ValueError("closed source mixes sampling policies")
    sampling_binding = next(iter(sampling_bindings.values()), None)
    if stats:
        _validate_merged_stats(stats)
    traces = []
    total_trace_bytes = 0
    for key in traffic_keys:
        trace = _parse_trace(store, config, key)
        total_trace_bytes += len(trace.request_raw) + len(trace.response_raw)
        if total_trace_bytes > config.source.max_total_trace_bytes:
            raise ValueError("closed source exceeds max_total_trace_bytes")
        if sampling_binding is None or any(
            trace.catalog.get(field) != expected
            for field, expected in sampling_binding.items()
        ):
            raise ValueError("trace sampling policy differs from stats")
        traces.append(trace)
    _validate_window_pairing(stats_by_window, traces)
    source = {
        "schema_version": "milk.summary-source-manifest.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "windows": [_utc_text(window) for window in candidates],
        "stats": stats_refs,
        "traces": [{"key": trace.key, "sha256": trace.object_sha256} for trace in traces],
        "code_version": CODE_VERSION,
        "decoded_trace_bytes": total_trace_bytes,
        "sampling_policy": sampling_binding,
    }
    next_watermark = {
        "schema_version": "milk.closed-window-watermark.v1",
        "stats_start_after": stats_keys[-1] if stats_keys else watermark["stats_start_after"],
        "traffic_start_after": traffic_keys[-1] if traffic_keys else watermark["traffic_start_after"],
    }
    return source, stats, traces, next_watermark


def _empty_source(config):
    return {
        "schema_version": "milk.summary-source-manifest.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "windows": [],
        "stats": [],
        "traces": [],
        "code_version": CODE_VERSION,
        "decoded_trace_bytes": 0,
        "sampling_policy": None,
    }


def _validate_source_manifest(config, value):
    value = _object(
        value,
        "source manifest",
        required={
            "schema_version",
            "scope_id",
            "profile",
            "windows",
            "stats",
            "traces",
            "code_version",
            "decoded_trace_bytes",
            "sampling_policy",
        },
    )
    if (
        value["schema_version"] != "milk.summary-source-manifest.v1"
        or value["scope_id"] != config.scope_id
        or value["profile"] != config.profile
        or value["code_version"] != CODE_VERSION
    ):
        raise ValueError("source manifest identity is invalid")
    if not isinstance(value["windows"], list) or any(
        not isinstance(item, str) for item in value["windows"]
    ):
        raise ValueError("source manifest windows are invalid")
    for name in ("stats", "traces"):
        if not isinstance(value[name], list):
            raise ValueError(f"source manifest {name} are invalid")
        for reference in value[name]:
            reference = _object(
                reference,
                f"source manifest {name} reference",
                required={"key", "sha256"},
            )
            _string(reference["key"], "source reference key", maximum=1024)
            if not isinstance(reference["sha256"], str) or HEX64.fullmatch(
                reference["sha256"]
            ) is None:
                raise ValueError("source reference SHA-256 is invalid")
    _integer(
        value["decoded_trace_bytes"],
        "source decoded_trace_bytes",
        0,
        config.source.max_total_trace_bytes,
    )
    return value


def _merge_sources(config, current, discovered):
    current = _validate_source_manifest(config, current)
    discovered = _validate_source_manifest(config, discovered)
    policies = [
        value
        for value in (current["sampling_policy"], discovered["sampling_policy"])
        if value is not None
    ]
    if len({_digest(value) for value in policies}) > 1:
        raise ValueError("pending source mixes sampling policies")

    def merge_references(name, limit):
        merged = {}
        for reference in current[name] + discovered[name]:
            previous = merged.setdefault(reference["key"], reference["sha256"])
            if previous != reference["sha256"]:
                raise ValueError("pending source object changed at an immutable key")
        if len(merged) > limit:
            raise ValueError(f"pending source exceeds configured {name} bound")
        return [
            {"key": key, "sha256": sha256}
            for key, sha256 in sorted(merged.items())
        ]

    return {
        **_empty_source(config),
        "windows": sorted(set(current["windows"] + discovered["windows"])),
        "stats": merge_references("stats", config.source.max_stats_shards),
        "traces": merge_references("traces", config.source.max_traces),
        "sampling_policy": policies[0] if policies else None,
    }


def _materialize_source(store, config, source):
    source = _validate_source_manifest(config, source)
    stats = {}
    stats_by_window = {}
    bindings = {}
    for reference in source["stats"]:
        key = reference["key"]
        window = _window_from_key(key)
        value, sha256, binding = _stats_shard(
            store, config, key, window
        )
        if sha256 != reference["sha256"]:
            raise ValueError("pending stats object digest changed")
        _merge_counts(stats, value["values"])
        _merge_counts(stats_by_window.setdefault(window, {}), value["values"])
        bindings[_digest(binding)] = binding
    if len(bindings) > 1:
        raise ValueError("pending source mixes sampling policies")
    sampling_policy = next(iter(bindings.values()), None)
    if stats:
        _validate_merged_stats(stats)
    traces = []
    total_trace_bytes = 0
    for reference in source["traces"]:
        trace = _parse_trace(store, config, reference["key"])
        if trace.object_sha256 != reference["sha256"]:
            raise ValueError("pending traffic object digest changed")
        if sampling_policy is None or any(
            trace.catalog.get(field) != expected
            for field, expected in sampling_policy.items()
        ):
            raise ValueError("trace sampling policy differs from stats")
        total_trace_bytes += len(trace.request_raw) + len(trace.response_raw)
        if total_trace_bytes > config.source.max_total_trace_bytes:
            raise ValueError("pending source exceeds max_total_trace_bytes")
        traces.append(trace)
    _validate_window_pairing(stats_by_window, traces)
    normalized = {
        **source,
        "decoded_trace_bytes": total_trace_bytes,
        "sampling_policy": sampling_policy,
    }
    return normalized, stats, traces


def _load_pending_source(store, config):
    sha256, value = _current_version(
        store, f"{config.prefix}/pending-source/current.json"
    )
    return sha256, _empty_source(config) if value is None else _validate_source_manifest(config, value)


def _publish_pending_source(store, config, source, parent_sha256):
    source = _validate_source_manifest(config, source)
    sha256 = _digest(source)
    key = f"{config.prefix}/pending-source/versions/{sha256}.json"
    create_same(store, key, canonical_json(source), "application/json")
    status = _advance_pointer(
        store,
        f"{config.prefix}/pending-source/current.json",
        "pending_source",
        sha256,
        key,
        parent_sha256,
    )
    return sha256, status


def _counter(stats, key):
    value = stats.get(key, 0)
    return value if type(value) is int else 0


def _rate_basis_points(numerator, denominator):
    if denominator <= 0:
        return 0
    return min(10_000, (numerator * 10_000 + denominator // 2) // denominator)


def _wilson_basis_points(successes, total):
    if total <= 0:
        return [0, 0]
    with localcontext() as context:
        context.prec = 40
        n = Decimal(total)
        p = Decimal(successes) / n
        z = Decimal("1.959963984540054")
        denominator = Decimal(1) + z * z / n
        center = (p + z * z / (Decimal(2) * n)) / denominator
        margin = z * ((p * (Decimal(1) - p) / n + z * z / (Decimal(4) * n * n)).sqrt()) / denominator
        return [
            int(((center - margin).max(Decimal(0)) * 10_000).to_integral_value(rounding=ROUND_HALF_UP)),
            int(((center + margin).min(Decimal(1)) * 10_000).to_integral_value(rounding=ROUND_HALF_UP)),
        ]


def _series(values):
    values = sorted(value for value in values if type(value) is int and value >= 0)
    if not values:
        return {"count": 0}

    def percentile(numerator, denominator):
        index = ((len(values) - 1) * numerator + denominator - 1) // denominator
        return values[index]

    total = sum(values)
    mean_milli = (total * 1000 + len(values) // 2) // len(values)
    variance_milli2 = sum((value * 1000 - mean_milli) ** 2 for value in values) // len(values)
    return {
        "count": len(values),
        "min": values[0],
        "p50": percentile(1, 2),
        "p95": percentile(95, 100),
        "p99": percentile(99, 100),
        "max": values[-1],
        "mean_milli": mean_milli,
        "variance_milli2": variance_milli2,
    }


def _published_counts(values):
    counts = Counter(values)
    published = {key: count for key, count in sorted(counts.items()) if count >= 10}
    suppressed = sum(count for count in counts.values() if count < 10)
    if suppressed:
        published["suppressed_lt_10"] = suppressed
    return published


def _walk_json(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_json(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json(item)


def _trace_analytics(trace):
    request = trace.request or {}
    response = trace.response or {}
    nodes = list(_walk_json(request)) + list(_walk_json(response))
    item_types = {
        node.get("type") for node in nodes if isinstance(node.get("type"), str)
    }
    modalities = set()
    if any(value in item_types for value in {"input_image", "image_url"}):
        modalities.add("image")
    if any(value in item_types for value in {"input_file", "file"}):
        modalities.add("file")
    if any(value in item_types for value in {"input_audio", "audio"}):
        modalities.add("audio")
    if any(
        isinstance(node.get("text"), str)
        or isinstance(node.get("content"), str)
        for node in nodes
    ):
        modalities.add("text")
    if not modalities:
        modalities.add("unknown")
    tools = request.get("tools")
    tool_definition_count = len(tools) if isinstance(tools, list) else 0
    tool_call_count = sum(
        node.get("type") in {"function_call", "tool_call"}
        for node in _walk_json(response)
    )
    if trace.endpoint == "chat_completions":
        for choice in response.get("choices", []):
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if isinstance(message, dict) and isinstance(message.get("tool_calls"), list):
                tool_call_count += len(message["tool_calls"])
        finish = [
            str(choice.get("finish_reason", "missing"))
            for choice in response.get("choices", [])
            if isinstance(choice, dict)
        ]
        outcome = "+".join(sorted(set(finish))) if finish else "missing"
        message_count = len(request.get("messages", [])) if isinstance(request.get("messages"), list) else 0
        input_item_count = message_count
    else:
        status = response.get("status")
        outcome = str(status) if isinstance(status, str) else "missing"
        incomplete = response.get("incomplete_details")
        if isinstance(incomplete, dict) and isinstance(incomplete.get("reason"), str):
            outcome += ":" + incomplete["reason"]
        response_input = request.get("input")
        input_item_count = len(response_input) if isinstance(response_input, list) else int(response_input is not None)
        message_count = sum(node.get("type") == "message" for node in _walk_json(response_input))
    text_config = request.get("text")
    structured = isinstance(request.get("response_format"), dict) or (
        isinstance(text_config, dict) and isinstance(text_config.get("format"), dict)
    )
    reasoning = request.get("reasoning")
    reasoning_effort = request.get("reasoning_effort")
    if isinstance(reasoning, dict) and isinstance(reasoning.get("effort"), str):
        reasoning_effort = reasoning["effort"]
    if not isinstance(reasoning_effort, str):
        reasoning_effort = "unspecified"
    conversation_linked = (
        trace.catalog.get("sampling_unit_kind")
        in {"chat_session_header", "responses_conversation"}
    )
    refusal = any(
        node.get("type") == "refusal"
        or (isinstance(node.get("refusal"), str) and bool(node["refusal"]))
        for node in nodes
    )
    token_rate = None
    completion_ms = trace.catalog.get("completion_ms")
    if trace.output_tokens is not None and type(completion_ms) is int and completion_ms > 0:
        token_rate = (trace.output_tokens * 1000 + completion_ms // 2) // completion_ms
    tool_arguments = []
    for node in _walk_json(response):
        arguments = node.get("arguments")
        if arguments is None and isinstance(node.get("function"), dict):
            arguments = node["function"].get("arguments")
        if arguments is not None:
            tool_arguments.append(arguments)
    valid_tool_arguments = 0
    for arguments in tool_arguments:
        if isinstance(arguments, dict):
            valid_tool_arguments += 1
        elif isinstance(arguments, str):
            try:
                valid_tool_arguments += int(
                    isinstance(json.loads(arguments), (dict, list))
                )
            except json.JSONDecodeError:
                pass
    return {
        "stream": trace.catalog["streaming"],
        "modality": "+".join(sorted(modalities)),
        "has_tools": tool_definition_count > 0,
        "has_tool_calls": tool_call_count > 0,
        "structured_output": structured,
        "reasoning_effort": reasoning_effort,
        "conversation_linked": conversation_linked,
        "outcome": outcome,
        "refusal": refusal,
        "tool_definition_count": tool_definition_count,
        "tool_call_count": tool_call_count,
        "message_count": message_count,
        "input_item_count": input_item_count,
        "output_tokens_per_second": token_rate,
        "tool_argument_count": len(tool_arguments),
        "valid_tool_argument_count": valid_tool_arguments,
    }


def _peak_concurrency(traces):
    events = []
    for trace in traces:
        completion_ms = trace.catalog.get("completion_ms")
        if type(completion_ms) is not int:
            continue
        events.append((trace.occurred_at, 1))
        events.append(
            (
                trace.occurred_at + dt.timedelta(milliseconds=completion_ms),
                -1,
            )
        )
    active = 0
    peak = 0
    for unused_when, delta in sorted(events, key=lambda item: (item[0], -item[1])):
        active += delta
        peak = max(peak, active)
    return peak


def _structural_summary(source, stats, traces):
    selected = _counter(stats, "selected")
    captured = _counter(stats, "captured")
    observed = _counter(stats, "observed")
    request_parsed = _counter(stats, "request_parse_success")
    paired = min(captured, selected)
    analytically_parsed = sum(trace.parse_success for trace in traces)
    unknown = sum(trace.unknown_items for trace in traces)
    items = sum(trace.total_items for trace in traces)
    content_hashes = [
        hashlib.sha256(
            b"milk.trace-content.v1\0"
            + len(trace.request_raw).to_bytes(8, "big")
            + trace.request_raw
            + len(trace.response_raw).to_bytes(8, "big")
            + trace.response_raw
        ).hexdigest()
        for trace in traces
    ]
    duplicate = len(content_hashes) - len(set(content_hashes))
    independent = {trace.session_hmac for trace in traces if trace.independent}
    all_sessions = {trace.session_hmac for trace in traces}
    failed = sum(
        trace.catalog.get("error_class") is not None
        or (isinstance(trace.catalog.get("provider_status"), int) and trace.catalog["provider_status"] >= 400)
        for trace in traces
    )
    analytics = [_trace_analytics(trace) for trace in traces]
    requests_per_session = Counter(trace.session_hmac for trace in traces)
    retained_per_hour = Counter(
        trace.occurred_at.replace(minute=0, second=0, microsecond=0)
        for trace in traces
    )
    sampling_policy = source.get("sampling_policy") or {}
    eligible = _counter(stats, "eligible")
    expected_basis_points = sampling_policy.get(
        "inclusion_probability_basis_points"
    )
    realized_basis_points = _rate_basis_points(selected, eligible)
    summary = {
        "schema_version": "milk.structural-summary.v1",
        "scope_id": source["scope_id"],
        "profile": source["profile"],
        "source_manifest_sha256": _digest(source),
        "source": source,
        "counts": {
            "accepted": _counter(stats, "observed"),
            "completed": len(traces),
            "failed": failed,
            "sampled": _counter(stats, "selected"),
            "retained": len(traces),
            "dropped": _counter(stats, "dropped"),
            "independent_sessions": len(independent),
            "all_session_roots": len(all_sessions),
        },
        "quality": {
            "paired": paired,
            "pairing_denominator": selected,
            "pairing_basis_points": _rate_basis_points(paired, selected),
            "pairing_wilson_95_basis_points": _wilson_basis_points(paired, selected),
            "parsed": analytically_parsed,
            "parse_denominator": len(traces),
            "parse_basis_points": _rate_basis_points(
                analytically_parsed, len(traces)
            ),
            "parse_wilson_95_basis_points": _wilson_basis_points(
                analytically_parsed, len(traces)
            ),
            "gateway_requests_parsed": request_parsed,
            "gateway_request_parse_denominator": observed,
            "gateway_request_parse_basis_points": _rate_basis_points(
                request_parsed, observed
            ),
            "unknown_items": unknown,
            "total_items": items,
            "unknown_item_basis_points": _rate_basis_points(unknown, items),
            "duplicate_traces": duplicate,
            "duplicate_basis_points": _rate_basis_points(duplicate, len(traces)),
            "missing_usage": sum(trace.input_tokens is None or trace.output_tokens is None for trace in traces),
            "independence_verified": bool(traces)
            and all(trace.independent for trace in traces),
            "capture_gap": selected != captured
            or captured != len(traces)
            or any(
                _counter(stats, key)
                for key in (
                    "capture_failed",
                    "trace_persist_failures",
                    "stats_persist_failures",
                    "dropped",
                    "oversized",
                    "interrupted",
                )
            ),
        },
        "distributions": {
            "endpoint": _published_counts(trace.endpoint for trace in traces),
            "model": _published_counts(trace.model or "unknown" for trace in traces),
            "sampling_unit_kind": _published_counts(str(trace.catalog.get("sampling_unit_kind", "unknown")) for trace in traces),
            "provider_status": _published_counts(str(trace.catalog.get("provider_status", "missing")) for trace in traces),
        },
        "sampled_distributions": {
            "denominator": "retained_sampled_traces",
            "stream": _published_counts(str(value["stream"]).lower() for value in analytics),
            "modality": _published_counts(value["modality"] for value in analytics),
            "has_tools": _published_counts(str(value["has_tools"]).lower() for value in analytics),
            "has_tool_calls": _published_counts(str(value["has_tool_calls"]).lower() for value in analytics),
            "structured_output": _published_counts(str(value["structured_output"]).lower() for value in analytics),
            "reasoning_effort": _published_counts(value["reasoning_effort"] for value in analytics),
            "conversation_linked": _published_counts(str(value["conversation_linked"]).lower() for value in analytics),
            "completion_or_incomplete": _published_counts(value["outcome"] for value in analytics),
            "refusal": _published_counts(str(value["refusal"]).lower() for value in analytics),
        },
        "sampled_numeric": {
            "denominator": "retained_sampled_traces",
            "tool_definition_count": _series([value["tool_definition_count"] for value in analytics]),
            "tool_call_count": _series([value["tool_call_count"] for value in analytics]),
            "message_count": _series([value["message_count"] for value in analytics]),
            "input_item_count": _series([value["input_item_count"] for value in analytics]),
            "output_tokens_per_second": _series([value["output_tokens_per_second"] for value in analytics]),
            "tool_argument_count": _series(
                [value["tool_argument_count"] for value in analytics]
            ),
            "valid_tool_argument_count": _series(
                [value["valid_tool_argument_count"] for value in analytics]
            ),
            "requests_per_session": _series(list(requests_per_session.values())),
            "retained_requests_per_hour": _series(list(retained_per_hour.values())),
        },
        "numeric": {
            "request_bytes": _series([len(trace.request_raw) for trace in traces]),
            "response_bytes": _series([len(trace.response_raw) for trace in traces]),
            "completion_ms": _series([trace.catalog.get("completion_ms") for trace in traces]),
            "ttft_ms": _series([trace.catalog.get("ttft_ms") for trace in traces]),
            "input_tokens": _series([trace.input_tokens for trace in traces]),
            "output_tokens": _series([trace.output_tokens for trace in traces]),
            "cached_tokens": _series([trace.cached_tokens for trace in traces]),
            "reasoning_tokens": _series([trace.reasoning_tokens for trace in traces]),
        },
        "operations": {
            "retained_peak_concurrency": _peak_concurrency(traces),
            "sampling_realized_basis_points": realized_basis_points,
            "sampling_expected_basis_points": expected_basis_points,
            "sampling_absolute_error_basis_points": (
                abs(realized_basis_points - expected_basis_points)
                if type(expected_basis_points) is int
                else None
            ),
            "unsupported_metrics": [
                "gateway_overhead_ms",
                "schema_validity",
                "watermark_lag_seconds",
            ],
        },
        "content_free": True,
    }
    return summary


def _job_write(store, prefix, job_type, job_id, name, value, compressed=False):
    suffix = ".json.zst" if compressed else ".json"
    key = f"{prefix}/jobs/{job_type}/{job_id}/{name}{suffix}"
    body = canonical_json(value)
    if compressed:
        command = shutil.which("zstd")
        if command is None:
            raise RuntimeError("zstd is required to publish compressed job results")
        result = subprocess.run([command, "--compress", "--stdout", "--quiet", "-3"], input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=15)
        if result.returncode != 0:
            raise RuntimeError("zstd failed to compress a job result")
        body = result.stdout
    return key, create_same(store, key, body, "application/zstd" if compressed else "application/json")


def _load_optional_json(store, key, *, compressed=False):
    try:
        raw = store.get(key)
    except FileNotFoundError:
        return None
    if compressed:
        raw = _decompress_trace(key, raw)
    return _json(raw, key)


def _advance_pointer(store, key, kind, version_sha256, version_key, parent_sha256):
    pointer = {
        "schema_version": "milk.current-pointer.v1",
        "kind": kind,
        "version_sha256": version_sha256,
        "version_key": version_key,
        "parent_version_sha256": parent_sha256,
    }
    body = canonical_json(pointer)
    try:
        existing, etag = store.get_versioned(key)
    except FileNotFoundError:
        if store.create(key, body, "application/json"):
            return "created"
        existing, unused_etag = store.get_versioned(key)
        del unused_etag
        if existing == body:
            return "existing"
        raise RuntimeError(f"concurrent create changed {key}")
    current = _json(existing, key)
    if current.get("version_sha256") == version_sha256:
        return "existing"
    if current.get("version_sha256") != parent_sha256:
        raise RuntimeError(f"current pointer parent changed for {key}")
    if store.replace(key, body, etag, "application/json"):
        return "advanced"
    latest = _json(store.get(key), key)
    if latest.get("version_sha256") == version_sha256:
        return "existing"
    raise RuntimeError(f"concurrent update changed {key}")


def _current_version(store, key):
    pointer = _load_optional_json(store, key)
    if pointer is None:
        return None, None
    version_sha256 = pointer.get("version_sha256")
    version_key = pointer.get("version_key")
    if not isinstance(version_sha256, str) or HEX64.fullmatch(version_sha256) is None or not isinstance(version_key, str):
        raise ValueError(f"{key} is invalid")
    value = _json(store.get(version_key), version_key)
    if hashlib.sha256(canonical_json(value)).hexdigest() != version_sha256:
        raise ValueError(f"{key} target digest is invalid")
    return version_sha256, value


def _load_watermark(store, config):
    key = f"{config.prefix}/watermarks/closed.json"
    try:
        raw, etag = store.get_versioned(key)
    except FileNotFoundError:
        return {
            "schema_version": "milk.closed-window-watermark.v1",
            "stats_start_after": None,
            "traffic_start_after": None,
        }, None
    value = _object(
        _json(raw, key),
        "closed watermark",
        required={"schema_version", "stats_start_after", "traffic_start_after"},
    )
    if value["schema_version"] != "milk.closed-window-watermark.v1":
        raise ValueError("closed watermark schema is invalid")
    for field, source in (
        ("stats_start_after", "stats"),
        ("traffic_start_after", "traffic"),
    ):
        frontier = value[field]
        if frontier is not None and (
            not isinstance(frontier, str)
            or not frontier.startswith(f"{config.prefix}/{source}/")
        ):
            raise ValueError("closed watermark frontier is invalid")
    return value, etag


def _advance_watermark(store, config, previous, etag, target):
    if target == previous:
        return "existing"
    key = f"{config.prefix}/watermarks/closed.json"
    body = canonical_json(target)
    if etag is None:
        if store.create(key, body, "application/json"):
            return "created"
        if store.get(key) == body:
            return "existing"
        raise RuntimeError("closed watermark was concurrently created")
    if store.replace(key, body, etag, "application/json"):
        return "advanced"
    if store.get(key) == body:
        return "existing"
    raise RuntimeError("closed watermark was concurrently changed")


def _existing_report(store, config, meter):
    summary_sha256, summary = _current_version(
        store, f"{config.prefix}/summaries/current.json"
    )
    readiness_sha256, readiness = _current_version(
        store, f"{config.prefix}/readiness/current.json"
    )
    eval_sha256, eval_value = _current_version(
        store, f"{config.prefix}/evals/{config.eval.series_id}/current.json"
    )
    proposal_sha256, unused_proposal = _current_version(
        store, f"{config.prefix}/route-proposals/current.json"
    )
    del unused_proposal
    return {
        "schema_version": "milk.run-once-report.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "source_manifest_sha256": None,
        "closed_windows": [],
        "trace_count": 0,
        "summary_sha256": summary_sha256,
        "summary_pointer": "existing" if summary else "absent",
        "classifier_job_id": summary.get("classifier_job_id") if summary else None,
        "classifier_provider_called": False,
        "readiness_sha256": readiness_sha256,
        "ready": readiness.get("ready", False) if readiness else False,
        "statistically_qualified": readiness.get("statistically_qualified", False)
        if readiness
        else False,
        "eval_job_id": eval_value.get("generation_job_id") if eval_value else None,
        "eval_provider_called": False,
        "eval_sha256": eval_sha256,
        "eval_pointer": "existing" if eval_value else "absent",
        "route_proposal_sha256": proposal_sha256,
        "provider_calls": 0,
        "provider_tokens": 0,
        "accounted_incremental_spend_microusd": meter.accounted_spend,
        "route_activation_attempted": False,
        "watermark": "existing",
    }


def _sample_traces(traces, config):
    sessions = {}
    for trace in traces:
        sessions.setdefault(trace.session_hmac, []).append(trace)
    representatives = {}
    for session, session_traces in sessions.items():
        candidates = sorted(
            (trace for trace in session_traces if trace.parse_success),
            key=lambda trace: (trace.occurred_at, trace.request_id),
        )
        if candidates:
            representatives[session] = candidates[0]
    ranked = sorted(
        representatives,
        key=lambda value: hashlib.sha256(
            ("milk.semantic-sample.v1\0" + value).encode()
        ).hexdigest(),
    )
    return [
        representatives[session]
        for session in ranked[: config.source.classifier_sample_sessions]
    ]


def _safe_text_prefix(value, limit):
    def fragments(item):
        if isinstance(item, str):
            yield item
        elif isinstance(item, list):
            for child in item:
                yield from fragments(child)
        elif isinstance(item, dict):
            if isinstance(item.get("text"), str):
                yield item["text"]
            elif "content" in item:
                yield from fragments(item["content"])

    raw = "\n".join(fragments(value)).encode()[:limit]
    text = raw.decode("utf-8", errors="ignore")
    return "".join(
        " " if ord(character) < 32 else "/" if character == "\\" else "'"
        if character == '"'
        else character
        for character in text
    )


def _request_text_prefix(request, endpoint, limit):
    if endpoint == "chat_completions":
        messages = request.get("messages")
        messages = messages if isinstance(messages, list) else []
        users = [
            message
            for message in messages
            if isinstance(message, dict) and message.get("role") == "user"
        ]
        source = users[-1].get("content") if users else None
    else:
        source = request.get("input")
    return _safe_text_prefix(source, limit)


def _response_text_prefix(response, endpoint, limit):
    if endpoint == "chat_completions":
        choices = response.get("choices")
        choices = choices if isinstance(choices, list) else []
        source = [
            choice.get("message", {}).get("content")
            for choice in choices
            if isinstance(choice, dict) and isinstance(choice.get("message"), dict)
        ]
    else:
        source = response.get("output")
    return _safe_text_prefix(source, limit)


def _modality_bits(analytics):
    return sum(
        bit
        for modality, bit in {
            "audio": 1,
            "file": 2,
            "image": 4,
            "text": 8,
            "unknown": 16,
        }.items()
        if modality in analytics["modality"].split("+")
    )


def _teacher_trace(trace, limit):
    request = json.dumps(
        trace.request, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    response = json.dumps(
        trace.response, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    analytics = _trace_analytics(trace)
    flags = int(len(request) > limit)
    flags |= int(len(response) > limit) << 1
    flags |= int(
        trace.catalog.get("error_class") is not None
        or (
            isinstance(trace.catalog.get("provider_status"), int)
            and trace.catalog["provider_status"] >= 400
        )
    ) << 2
    flags |= int(analytics["has_tools"] or analytics["has_tool_calls"]) << 3
    return [
        trace.object_sha256,
        "c" if trace.endpoint == "chat_completions" else "r",
        _request_text_prefix(trace.request, trace.endpoint, limit),
        _response_text_prefix(trace.response, trace.endpoint, limit),
        flags,
        _modality_bits(analytics),
        len(trace.request_raw),
    ]


def _classifier_row(trace, limit):
    request = json.dumps(
        trace.request, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    analytics = _trace_analytics(trace)
    flags = int(len(request) > limit)
    flags |= int(
        trace.catalog.get("error_class") is not None
        or (
            isinstance(trace.catalog.get("provider_status"), int)
            and trace.catalog["provider_status"] >= 400
        )
    ) << 1
    flags |= int(analytics["has_tools"] or analytics["has_tool_calls"]) << 2
    return [
        "c" if trace.endpoint == "chat_completions" else "r",
        _request_text_prefix(trace.request, trace.endpoint, limit),
        flags,
        _modality_bits(analytics),
    ]


def _validate_labels(value, expected):
    value = _object(value, "classification output", required={"labels"})
    rows = value["labels"]
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise ValueError("classification output must contain one label per trace")
    checked = []
    for trace, row in zip(expected, rows):
        if not isinstance(row, list) or len(row) != 6:
            raise ValueError("classification label must be a compact six-value row")
        operation_index = _integer(
            row[0], "classification operation code", 0, len(OPERATION_VALUES) - 1
        )
        domain_index = _integer(
            row[1], "classification domain code", 0, len(DOMAIN_VALUES) - 1
        )
        capability_mask = _integer(
            row[2],
            "classification capability mask",
            1,
            (1 << len(CAPABILITY_VALUES)) - 1,
        )
        oracle_index = _integer(
            row[3], "classification oracle code", 0, len(ORACLE_VALUES) - 1
        )
        language = _string(row[4], "classification language", maximum=32)
        if type(row[5]) is not bool:
            raise ValueError("classification abstain must be boolean")
        checked.append(
            {
                "trace_sha256": trace.object_sha256,
                "operation": OPERATION_VALUES[operation_index],
                "domain": DOMAIN_VALUES[domain_index],
                "capabilities": [
                    capability
                    for index, capability in enumerate(CAPABILITY_VALUES)
                    if capability_mask & (1 << index)
                ],
                "expected_oracle": ORACLE_VALUES[oracle_index],
                "language": language,
                "abstain": row[5],
            }
        )
    return sorted(checked, key=lambda label: label["trace_sha256"])


def _claim_reserved_cost(claim, job_root):
    if not isinstance(claim, dict):
        raise ValueError("teacher claim must be an object")
    job_id = job_root.rsplit("/", 1)[-1]
    if (
        claim.get("schema_version") != "milk.teacher-job-claim.v1"
        or claim.get("job_id") != job_id
        or not isinstance(claim.get("identity"), dict)
    ):
        raise ValueError("teacher claim accounting identity is invalid")
    teacher = _object(
        claim["identity"].get("teacher"),
        "teacher claim provider binding",
        required={
            "api_url",
            "model",
            "reasoning_effort",
            "timeout_seconds",
            "max_input_tokens",
            "max_output_tokens",
            "input_rate_microusd_per_million",
            "output_rate_microusd_per_million",
        },
    )
    max_input = _integer(
        teacher["max_input_tokens"], "claim max input tokens", 0, 500_000
    )
    max_output = _integer(
        teacher["max_output_tokens"], "claim max output tokens", 0, 100_000
    )
    input_rate = _integer(
        teacher["input_rate_microusd_per_million"],
        "claim input rate",
        0,
        1_000_000_000,
    )
    output_rate = _integer(
        teacher["output_rate_microusd_per_million"],
        "claim output rate",
        0,
        1_000_000_000,
    )
    return _token_cost(max_input, input_rate) + _token_cost(
        max_output, output_rate
    )


class _RunMeter:
    def __init__(self, store, config):
        self.config = config
        self.calls = 0
        self.tokens = 0
        self.accounted_spend = config.budget.starting_spend_microusd
        keys = []
        for job_type in ("classify", "generate-eval"):
            remaining = MAX_ACCOUNTING_JOB_OBJECTS - len(keys)
            page = _list_up_to(
                store,
                f"{config.prefix}/jobs/{job_type}",
                remaining + 1,
            )
            keys.extend(page)
            if len(keys) > MAX_ACCOUNTING_JOB_OBJECTS:
                raise ValueError("teacher job accounting history exceeds its bound")
        result_roots = {
            key.removesuffix("/result.json.zst")
            for key in keys
            if key.endswith("/result.json.zst")
        }
        self.orphan_claim_roots = {}
        for key in keys:
            if not key.endswith("/result.json.zst"):
                continue
            result = _load_optional_json(store, key, compressed=True)
            root = key.removesuffix("/result.json.zst")
            if (
                result.get("schema_version") != "milk.teacher-job-result.v1"
                or result.get("job_id") != root.rsplit("/", 1)[-1]
            ):
                raise ValueError("teacher result accounting identity is invalid")
            cost = _integer(
                result.get("accounted_cost_microusd"),
                "teacher result accounted cost",
                0,
                MAX_INCREMENTAL_SPEND_MICROUSD,
            )
            self.accounted_spend += cost
        for key in keys:
            if not key.endswith("/claim.json"):
                continue
            claim = _load_optional_json(store, key)
            root = key.removesuffix("/claim.json")
            reserved = _claim_reserved_cost(claim, root)
            if root not in result_roots:
                self.orphan_claim_roots[root] = reserved
                self.accounted_spend += reserved

    def can_start(self):
        reserved = self.config.teacher.reserved_cost()
        return (
            self.calls < self.config.teacher.max_calls_per_run
            and self.tokens + self.config.teacher.max_input_tokens_per_call + self.config.teacher.max_output_tokens_per_call
            <= self.config.teacher.max_total_tokens_per_run
            and self.accounted_spend < self.config.budget.stop_new_spend_microusd
            and self.accounted_spend + reserved <= self.config.budget.absolute_spend_microusd
        )

    def record(self, input_tokens, output_tokens, accounted_cost):
        self.calls += 1
        self.tokens += input_tokens + output_tokens
        self.accounted_spend += accounted_cost

    def observe_unresolved_claim(self, root, reserved):
        if root not in self.orphan_claim_roots:
            self.orphan_claim_roots[root] = reserved
            self.accounted_spend += reserved


CLASSIFIER_INSTRUCTIONS = """Return only JSON as {"labels":[row,...]} with one label per input row in exact order. Input taxonomy arrays are operations, domains, capability bits, then oracles. Each input row is [endpoint,request_text_prefix,flags,modality_bits], where endpoint is c or r, flags bits are request_truncated=1,error=2,tool_use=4, and modality bits are audio=1,file=2,image=4,text=8,unknown=16. Each output row is [operation_code,domain_code,capability_bitmask,oracle_code,language,abstain]. Do not return trace IDs, reasoning, or prose."""


def _unresolved_claim(job_id, reserved):
    return {
        "schema_version": "milk.teacher-job-result.v1",
        "job_id": job_id,
        "outcome": "in_progress_or_ambiguous",
        "error_class": "unresolved_claim",
        "provider_request_id": None,
        "input_tokens": 0,
        "output_tokens": 0,
        "calculated_cost_microusd": None,
        "accounted_cost_microusd": reserved,
    }


def _reconcile_existing_claim(
    store, config, meter, job_type, job_id, job_root, claim, identity, now
):
    claim = _object(
        claim,
        "teacher claim",
        required={
            "schema_version",
            "job_id",
            "identity",
            "claimed_at",
            "reconcile_after",
        },
    )
    if (
        claim["schema_version"] != "milk.teacher-job-claim.v1"
        or claim["job_id"] != job_id
        or claim["identity"] != identity
    ):
        raise ValueError("existing teacher claim differs")
    _utc(claim["claimed_at"], "teacher claim claimed_at")
    reconcile_after = _utc(
        claim["reconcile_after"], "teacher claim reconcile_after"
    )
    reserved = config.teacher.reserved_cost()
    meter.observe_unresolved_claim(job_root, reserved)
    if now < reconcile_after:
        return _unresolved_claim(job_id, reserved)
    result = {
        "schema_version": "milk.teacher-job-result.v1",
        "job_id": job_id,
        "outcome": "ambiguous",
        "error_class": "stale_unresolved_claim",
        "provider_request_id": None,
        "input_tokens": config.teacher.max_input_tokens_per_call,
        "output_tokens": config.teacher.max_output_tokens_per_call,
        "calculated_cost_microusd": None,
        "accounted_cost_microusd": reserved,
    }
    _job_write(
        store,
        config.prefix,
        job_type,
        job_id,
        "result",
        result,
        compressed=True,
    )
    return result


def _provider_job(
    store,
    config,
    meter,
    teacher,
    lease,
    now,
    *,
    job_type,
    task,
    input_value,
    validate,
):
    instructions = CLASSIFIER_INSTRUCTIONS if task == "classify" else EVAL_INSTRUCTIONS
    prompt_sha256 = hashlib.sha256(instructions.encode()).hexdigest()
    identity = {
        "schema_version": "milk.teacher-job-identity.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "job_type": job_type,
        "teacher": config.teacher.public_binding(),
        "prompt_sha256": prompt_sha256,
        "input_sha256": _digest(input_value),
        "code_version": CODE_VERSION,
    }
    job_id = _digest(identity)
    result_key = f"{config.prefix}/jobs/{job_type}/{job_id}/result.json.zst"
    claim_key = f"{config.prefix}/jobs/{job_type}/{job_id}/claim.json"
    job_root = claim_key.removesuffix("/claim.json")
    existing = _load_optional_json(store, result_key, compressed=True)
    if existing is not None:
        return existing, job_id, False
    existing_claim = _load_optional_json(store, claim_key)
    if existing_claim is not None:
        return _reconcile_existing_claim(
            store,
            config,
            meter,
            job_type,
            job_id,
            job_root,
            existing_claim,
            identity,
            now,
        ), job_id, False
    try:
        preflight = _teacher_request_body(
            config.teacher, instructions, input_value
        )
    except ValueError:
        return {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "not_started_input_size_cap",
            "provider_request_id": None,
            "input_tokens": 0,
            "output_tokens": 0,
            "calculated_cost_microusd": 0,
            "accounted_cost_microusd": 0,
        }, job_id, False
    if _conservative_token_bound(preflight) > config.teacher.max_input_tokens_per_call:
        return {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "not_started_input_token_cap",
            "provider_request_id": None,
            "input_tokens": 0,
            "output_tokens": 0,
            "calculated_cost_microusd": 0,
            "accounted_cost_microusd": 0,
        }, job_id, False
    if not meter.can_start():
        return {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "not_started_budget_or_call_cap",
            "provider_request_id": None,
            "input_tokens": 0,
            "output_tokens": 0,
            "calculated_cost_microusd": 0,
            "accounted_cost_microusd": 0,
        }, job_id, False
    if isinstance(teacher, DirectTeacher) and not os.environ.get(
        config.teacher.api_key_env
    ):
        raise ValueError(f"{config.teacher.api_key_env} is required")
    lease.renew()
    claim = {
        "schema_version": "milk.teacher-job-claim.v1",
        "job_id": job_id,
        "identity": identity,
        "claimed_at": _utc_text(now),
        "reconcile_after": _utc_text(
            now + dt.timedelta(seconds=config.teacher.timeout_seconds + 60)
        ),
    }
    unused_claim_key, disposition = _job_write(
        store, config.prefix, job_type, job_id, "claim", claim
    )
    del unused_claim_key
    if disposition == "existing":
        existing_claim = _load_optional_json(store, claim_key)
        return _reconcile_existing_claim(
            store,
            config,
            meter,
            job_type,
            job_id,
            job_root,
            existing_claim,
            identity,
            now,
        ), job_id, False
    lease.renew()
    try:
        response = teacher.complete(
            task=task,
            instructions=instructions,
            payload=input_value,
            job_id=job_id,
        )
        checked = validate(response.value)
        calculated = _token_cost(response.input_tokens, config.teacher.input_rate_microusd_per_million) + _token_cost(
            response.output_tokens, config.teacher.output_rate_microusd_per_million
        )
        result = {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "succeeded",
            "provider_request_id": response.provider_request_id,
            "input_tokens": response.input_tokens,
            "output_tokens": response.output_tokens,
            "calculated_cost_microusd": calculated,
            "accounted_cost_microusd": calculated,
            "output": checked,
        }
        meter.record(response.input_tokens, response.output_tokens, calculated)
    except urllib.error.HTTPError as error:
        reserved = config.teacher.reserved_cost()
        result = {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "definitive_http_error",
            "http_status": error.code,
            "provider_request_id": error.headers.get("x-request-id") if error.headers else None,
            "input_tokens": config.teacher.max_input_tokens_per_call,
            "output_tokens": 0,
            "calculated_cost_microusd": None,
            "accounted_cost_microusd": reserved,
        }
        meter.record(config.teacher.max_input_tokens_per_call, 0, reserved)
    except (TimeoutError, urllib.error.URLError, ConnectionError, OSError) as error:
        reserved = config.teacher.reserved_cost()
        result = {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "ambiguous",
            "error_class": type(error).__name__,
            "provider_request_id": None,
            "input_tokens": config.teacher.max_input_tokens_per_call,
            "output_tokens": config.teacher.max_output_tokens_per_call,
            "calculated_cost_microusd": None,
            "accounted_cost_microusd": reserved,
        }
        meter.record(config.teacher.max_input_tokens_per_call, config.teacher.max_output_tokens_per_call, reserved)
    except ValueError as error:
        reserved = config.teacher.reserved_cost()
        result = {
            "schema_version": "milk.teacher-job-result.v1",
            "job_id": job_id,
            "outcome": "invalid_provider_response",
            "error_class": type(error).__name__,
            "provider_request_id": None,
            "input_tokens": config.teacher.max_input_tokens_per_call,
            "output_tokens": config.teacher.max_output_tokens_per_call,
            "calculated_cost_microusd": None,
            "accounted_cost_microusd": reserved,
        }
        meter.record(config.teacher.max_input_tokens_per_call, config.teacher.max_output_tokens_per_call, reserved)
    _job_write(store, config.prefix, job_type, job_id, "result", result, compressed=True)
    return result, job_id, True


def _classification(
    store, config, meter, teacher, lease, now, traces, source_sha256
):
    sample = _sample_traces(traces, config)
    if not sample:
        return None, None, False, sample
    payload = {
        "schema_version": "milk.classification-input.v1",
        "taxonomy_version": TAXONOMY_VERSION,
        "taxonomy": [
            list(OPERATION_VALUES),
            list(DOMAIN_VALUES),
            list(CAPABILITY_VALUES),
            list(ORACLE_VALUES),
        ],
        "source_manifest_sha256": source_sha256,
        "rows": [
            _classifier_row(trace, config.source.teacher_trace_bytes)
            for trace in sample
        ],
    }
    result, job_id, called = _provider_job(
        store,
        config,
        meter,
        teacher,
        lease,
        now,
        job_type="classify",
        task="classify",
        input_value=payload,
        validate=lambda value: _validate_labels(value, sample),
    )
    return result, job_id, called, sample


def _semantic_summary(labels):
    if not labels:
        return {
            "classified": 0,
            "abstained": 0,
            "abstain_basis_points": 10_000,
            "other_or_abstain": 0,
            "other_or_abstain_basis_points": 10_000,
            "operation": {},
            "capability": {},
            "operation_domain": {},
        }
    other_or_abstain = sum(label["operation"] == "other" or label["abstain"] for label in labels)
    abstained = sum(label["abstain"] for label in labels)
    return {
        "classified": len(labels),
        "abstained": abstained,
        "abstain_basis_points": _rate_basis_points(abstained, len(labels)),
        "other_or_abstain": other_or_abstain,
        "other_or_abstain_basis_points": _rate_basis_points(other_or_abstain, len(labels)),
        "operation": _published_counts(label["operation"] for label in labels),
        "domain": _published_counts(label["domain"] for label in labels),
        "oracle": _published_counts(label["expected_oracle"] for label in labels),
        "language": _published_counts(label["language"] for label in labels),
        "capability": _published_counts(
            capability
            for label in labels
            for capability in label["capabilities"]
        ),
        "operation_domain": _published_counts(
            label["operation"] + "/" + label["domain"] for label in labels
        ),
        "teacher_strength": "teacher_weak",
    }


def _structural_can_classify(config, structural):
    quality = structural["quality"]
    counts = structural["counts"]
    minimum = 750 if config.profile == "production" else 100
    return (
        counts["independent_sessions"] >= minimum
        and quality["independence_verified"]
        and quality["pairing_basis_points"] >= 9900
        and quality["parse_basis_points"] >= 9950
        and quality["unknown_item_basis_points"] <= 100
        and quality["duplicate_basis_points"] <= 10
        and not quality["capture_gap"]
    )


def _publish_version(store, prefix, kind, value, parent_sha256, version_root, pointer_key):
    value = {**value, "parent_version_sha256": parent_sha256}
    sha256 = _digest(value)
    key = f"{prefix}/{version_root}/versions/{sha256}.json"
    create_same(store, key, canonical_json(value), "application/json")
    pointer_status = _advance_pointer(store, f"{prefix}/{pointer_key}", kind, sha256, key, parent_sha256)
    return sha256, key, value, pointer_status


def _readiness(config, summary, labels, traces, meter, eval_result_exists):
    quality = summary["structural"]["quality"]
    counts = summary["structural"]["counts"]
    minimum = 750 if config.profile == "production" else 100
    trace_sessions = {trace.object_sha256: trace.session_hmac for trace in traces}
    per_session = {}
    for label in labels:
        session = trace_sessions.get(label["trace_sha256"])
        if session is not None:
            per_session.setdefault(session, []).append(label)
    session_operations = []
    session_other_or_abstain = 0
    for session_labels in per_session.values():
        if all(label["abstain"] for label in session_labels):
            session_other_or_abstain += 1
            continue
        counts_by_operation = Counter(
            label["operation"]
            for label in session_labels
            if not label["abstain"]
        )
        operation = min(
            counts_by_operation,
            key=lambda value: (-counts_by_operation[value], value),
        )
        session_operations.append(operation)
        session_other_or_abstain += operation == "other"
    operation_counts = Counter(session_operations)
    minimum_class_sessions = 30 if config.profile == "production" else 1
    represented = []
    class_failures = []
    for operation, count in sorted(operation_counts.items()):
        if _rate_basis_points(count, len(per_session)) >= 500:
            represented.append({"operation": operation, "sessions": count})
            if count < minimum_class_sessions:
                class_failures.append(operation)
    checks = {
        "minimum_independent_sessions": counts["independent_sessions"] >= minimum,
        "minimum_classified_sessions": len(per_session) >= minimum,
        "independence_verified": quality["independence_verified"],
        "pairing_at_least_99_percent": quality["pairing_basis_points"] >= 9900,
        "parse_at_least_99_5_percent": quality["parse_basis_points"] >= 9950,
        "unknown_items_at_most_1_percent": quality["unknown_item_basis_points"] <= 100,
        "duplicates_at_most_0_1_percent": quality["duplicate_basis_points"] <= 10,
        "other_plus_abstain_at_most_15_percent": _rate_basis_points(
            session_other_or_abstain, len(per_session)
        )
        <= 1500,
        "represented_classes_meet_minimum": not class_failures and bool(represented),
        "representative_eval_capacity": len(represented)
        <= config.eval.representative_cases,
        "closed_watermark_without_capture_gap": bool(summary["structural"]["source"]["windows"]) and not quality["capture_gap"],
        "eval_generation_budget_available": eval_result_exists or meter.can_start(),
    }
    ready = all(checks.values())
    return {
        "schema_version": "milk.readiness.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "summary_sha256": summary["summary_sha256"],
        "ready": ready,
        "statistically_qualified": ready and config.profile == "production",
        "minimum_independent_sessions": minimum,
        "minimum_represented_class_sessions": minimum_class_sessions,
        "checks": checks,
        "represented_classes": represented,
        "class_failures": class_failures,
    }


EVAL_INSTRUCTIONS = """Return only JSON with a cases array. Input label rows are [trace_sha256,operation,domain,capability_bits,oracle,language], where capability bits are knowledge=1,reasoning=2,instruction_following=4,structured_output=8,tool_use=16,multimodal=32. Input trace rows are [trace_sha256,endpoint,request_text_prefix,response_text_prefix,flags,modality_bits,request_bytes], where endpoint is c or r, flags bits are request_truncated=1,response_truncated=2,error=4,tool_use=8, and modality bits are audio=1,file=2,image=4,text=8,unknown=16. Each case must contain exactly suite, source_trace_sha256, input, expected, oracle, operation, and selection_reason. operation must equal the supplied source label. Representative cases use selection_reason representative_mix. Tail cases use rare, long_context, error, multimodal, or tool_use. Use only supplied trace_sha256 values. Produce exactly the requested representative and tail counts. Never put expected text in input. Do not include reasoning, configuration, routes, budgets, or prose."""


def _validate_eval_output(
    value,
    traces,
    labels,
    representative_count,
    tail_count,
):
    value = _object(value, "eval output", required={"cases"})
    cases = value["cases"]
    if not isinstance(cases, list) or len(cases) != representative_count + tail_count:
        raise ValueError("eval output case count is invalid")
    labels = [label for label in labels if not label["abstain"]]
    if not labels:
        raise ValueError("eval generation requires non-abstained labels")
    sources = {trace.object_sha256 for trace in traces}
    traces_by_sha = {trace.object_sha256: trace for trace in traces}
    source_operations = {
        label["trace_sha256"]: label["operation"] for label in labels
    }
    label_counts = Counter(label["operation"] for label in labels)
    required_operations = {
        operation
        for operation, count in label_counts.items()
        if _rate_basis_points(count, len(labels)) >= 500
    }
    if len(required_operations) > representative_count:
        raise ValueError(
            "representative eval capacity is smaller than measured operation slices"
        )
    remaining_representative = representative_count - len(required_operations)
    required_total = sum(label_counts[value] for value in required_operations)
    representative_quotas = {
        operation: 1
        + remaining_representative * count // max(1, required_total)
        for operation, count in label_counts.items()
        if operation in required_operations
    }
    request_lengths = sorted(len(trace.request_raw) for trace in traces)
    long_context_threshold = request_lengths[
        ((len(request_lengths) - 1) * 9 + 9) // 10
    ]
    counts = Counter()
    seen = set()
    checked = []
    total_text_bytes = 0
    for case in cases:
        case = _object(
            case,
            "eval case",
            required={
                "suite",
                "source_trace_sha256",
                "input",
                "expected",
                "oracle",
                "operation",
                "selection_reason",
            },
        )
        if case["suite"] not in {"representative", "tail"} or case["source_trace_sha256"] not in sources or case["oracle"] not in ORACLES:
            raise ValueError("eval case identity or taxonomy is invalid")
        input_text = _string(case["input"], "eval input", maximum=32_768)
        expected = _string(case["expected"], "eval expected", maximum=32_768)
        total_text_bytes += len(input_text.encode()) + len(expected.encode())
        if total_text_bytes > MAX_EVAL_TEXT_BYTES:
            raise ValueError("eval output text exceeds the aggregate byte cap")
        if expected.casefold() in input_text.casefold():
            raise ValueError("eval input leaks its expected answer")
        if case["operation"] != source_operations.get(case["source_trace_sha256"]):
            raise ValueError("eval case operation differs from its source label")
        if case["suite"] == "representative":
            if case["selection_reason"] != "representative_mix":
                raise ValueError("representative eval selection reason is invalid")
        elif case["selection_reason"] not in {
            "rare",
            "long_context",
            "error",
            "multimodal",
            "tool_use",
        }:
            raise ValueError("tail eval selection reason is invalid")
        if case["suite"] == "tail":
            trace = traces_by_sha[case["source_trace_sha256"]]
            analytics = _trace_analytics(trace)
            operation_count = label_counts[case["operation"]]
            reason_supported = {
                "rare": _rate_basis_points(operation_count, len(labels)) < 500,
                "long_context": len(trace.request_raw) >= long_context_threshold,
                "error": trace.catalog.get("error_class") is not None
                or (
                    isinstance(trace.catalog.get("provider_status"), int)
                    and trace.catalog["provider_status"] >= 400
                ),
                "multimodal": analytics["modality"]
                not in {"text", "unknown"},
                "tool_use": analytics["has_tools"]
                or analytics["has_tool_calls"],
            }[case["selection_reason"]]
            if not reason_supported:
                raise ValueError("tail eval selection reason lacks source evidence")
        identity = _digest(
            {
                "source": case["source_trace_sha256"],
                "input": input_text,
                "expected": expected,
            }
        )
        if identity in seen:
            raise ValueError("eval output contains a duplicate case")
        seen.add(identity)
        counts[case["suite"]] += 1
        checked.append({**case, "case_id": identity})
    if counts != Counter({"representative": representative_count, "tail": tail_count}):
        raise ValueError("eval output suite counts are invalid")
    covered_operations = {
        case["operation"] for case in checked if case["suite"] == "representative"
    }
    if not required_operations.issubset(covered_operations):
        raise ValueError("representative eval cases do not cover measured operation slices")
    representative_counts = Counter(
        case["operation"]
        for case in checked
        if case["suite"] == "representative"
    )
    if any(
        representative_counts[operation] < quota
        for operation, quota in representative_quotas.items()
    ):
        raise ValueError("representative eval cases do not preserve measured mixture")
    return sorted(checked, key=lambda case: (case["suite"], case["case_id"]))


def _select_eval_sources(traces, labels, limit):
    traces_by_sha = {trace.object_sha256: trace for trace in traces}
    labels_by_sha = {
        label["trace_sha256"]: label
        for label in labels
        if not label["abstain"] and label["trace_sha256"] in traces_by_sha
    }
    selected = set()
    for operation in sorted({label["operation"] for label in labels_by_sha.values()}):
        selected.add(
            min(
                sha256
                for sha256, label in labels_by_sha.items()
                if label["operation"] == operation
            )
        )
    ranked_traces = sorted(
        (
            trace
            for sha256, trace in traces_by_sha.items()
            if sha256 in labels_by_sha
        ),
        key=lambda trace: hashlib.sha256(
            ("milk.eval-source.v1\0" + trace.object_sha256).encode()
        ).hexdigest(),
    )
    if ranked_traces:
        selected.add(max(ranked_traces, key=lambda trace: len(trace.request_raw)).object_sha256)
    for predicate in (
        lambda trace, analytics: trace.catalog.get("error_class") is not None
        or (
            isinstance(trace.catalog.get("provider_status"), int)
            and trace.catalog["provider_status"] >= 400
        ),
        lambda unused_trace, analytics: analytics["modality"]
        not in {"text", "unknown"},
        lambda unused_trace, analytics: analytics["has_tools"]
        or analytics["has_tool_calls"],
    ):
        match = next(
            (
                trace
                for trace in ranked_traces
                if predicate(trace, _trace_analytics(trace))
            ),
            None,
        )
        if match is not None:
            selected.add(match.object_sha256)
    for trace in ranked_traces:
        if len(selected) >= limit:
            break
        selected.add(trace.object_sha256)
    if len(selected) > limit:
        raise ValueError("eval.max_source_traces cannot cover required source slices")
    selected_traces = [
        trace for trace in ranked_traces if trace.object_sha256 in selected
    ]
    selected_labels = [
        labels_by_sha[trace.object_sha256] for trace in selected_traces
    ]
    return selected_traces, selected_labels


def _eval_generation(
    store,
    config,
    meter,
    teacher,
    lease,
    now,
    traces,
    labels,
    summary_sha256,
    readiness_sha256,
):
    labels = [label for label in labels if not label["abstain"]]
    if not labels:
        raise ValueError("eval generation requires non-abstained labels")
    selected, selected_labels = _select_eval_sources(
        traces, labels, config.eval.max_source_traces
    )
    compact_labels = [
        [
            label["trace_sha256"],
            label["operation"],
            label["domain"],
            sum(
                1 << CAPABILITY_VALUES.index(capability)
                for capability in label["capabilities"]
            ),
            label["expected_oracle"],
            label["language"],
        ]
        for label in selected_labels
    ]
    payload = {
        "schema_version": "milk.eval-generation-input.v1",
        "summary_sha256": summary_sha256,
        "readiness_sha256": readiness_sha256,
        "series_id": config.eval.series_id,
        "representative_cases": config.eval.representative_cases,
        "tail_cases": config.eval.tail_cases,
        "operation_counts": dict(
            sorted(Counter(label["operation"] for label in labels).items())
        ),
        "labels": compact_labels,
        "traces": [
            _teacher_trace(trace, config.source.teacher_trace_bytes)
            for trace in selected
        ],
    }
    result, job_id, called = _provider_job(
        store,
        config,
        meter,
        teacher,
        lease,
        now,
        job_type="generate-eval",
        task="generate_eval",
        input_value=payload,
        validate=lambda value: _validate_eval_output(
            value,
            selected,
            labels,
            config.eval.representative_cases,
            config.eval.tail_cases,
        ),
    )
    return result, job_id, called


@dataclass(frozen=True)
class _RunLease:
    store: object
    key: str
    owner_id: str
    lease_seconds: int
    clock: object

    def _now(self):
        now = self.clock()
        if not isinstance(now, dt.datetime) or now.utcoffset() != dt.timedelta(0):
            raise ValueError("run lease clock must return UTC")
        return now

    def _active(self):
        raw, etag = self.store.get_versioned(self.key)
        value = _object(
            _json(raw, self.key),
            "run-once lease",
            required={
                "schema_version",
                "state",
                "owner_id",
                "acquired_at",
                "renewed_at",
                "expires_at",
                "released_at",
            },
        )
        now = self._now()
        if (
            value.get("schema_version") != "milk.run-once-lease.v1"
            or value.get("state") != "active"
            or value.get("owner_id") != self.owner_id
            or _utc(value.get("expires_at"), "run lease expires_at") <= now
        ):
            raise RuntimeError("run-once lease is no longer active")
        return value, etag, now

    def assert_active(self):
        self._active()

    def guard(self):
        value, unused_etag, now = self._active()
        del unused_etag
        expires_at = _utc(value["expires_at"], "run lease expires_at")
        if expires_at - now <= dt.timedelta(seconds=60):
            self.renew()

    def renew(self):
        for _ in range(3):
            value, etag, now = self._active()
            renewed = {
                **value,
                "renewed_at": _utc_text(now),
                "expires_at": _utc_text(
                    now + dt.timedelta(seconds=self.lease_seconds)
                ),
            }
            body = canonical_json(renewed)
            if self.store.replace(self.key, body, etag, "application/json"):
                return
        raise RuntimeError("run-once lease renewal did not converge")

    def release(self):
        raw, etag = self.store.get_versioned(self.key)
        value = _json(raw, self.key)
        if value.get("owner_id") != self.owner_id:
            raise RuntimeError("run-once lease owner changed")
        if value.get("state") == "released":
            return
        if value.get("state") != "active":
            raise RuntimeError("run-once lease state changed")
        released = {
            **value,
            "state": "released",
            "released_at": _utc_text(self._now()),
        }
        body = canonical_json(released)
        if self.store.replace(self.key, body, etag, "application/json"):
            return
        latest = _json(self.store.get(self.key), self.key)
        if latest == released:
            return
        raise RuntimeError("run-once lease release raced another writer")


class _LeasedStore:
    def __init__(self, store, lease):
        self.store = store
        self.lease = lease

    def create(self, key, body, content_type="application/octet-stream"):
        self.lease.guard()
        return self.store.create(key, body, content_type)

    def replace(self, key, body, etag, content_type="application/octet-stream"):
        self.lease.guard()
        return self.store.replace(key, body, etag, content_type)

    def __getattr__(self, name):
        return getattr(self.store, name)


def _acquire_run_lease(store, config, clock):
    key = f"{config.prefix}/locks/run-once.json"
    owner_id = str(uuid.uuid4())
    lease_seconds = max(
        300,
        config.teacher.timeout_seconds * config.teacher.max_calls_per_run + 120,
    )
    for _ in range(3):
        now = clock()
        if not isinstance(now, dt.datetime) or now.utcoffset() != dt.timedelta(0):
            raise ValueError("run lease clock must return UTC")
        active = {
            "schema_version": "milk.run-once-lease.v1",
            "state": "active",
            "owner_id": owner_id,
            "acquired_at": _utc_text(now),
            "renewed_at": _utc_text(now),
            "expires_at": _utc_text(now + dt.timedelta(seconds=lease_seconds)),
            "released_at": None,
        }
        body = canonical_json(active)
        try:
            raw, etag = store.get_versioned(key)
        except FileNotFoundError:
            if store.create(key, body, "application/json"):
                return _RunLease(store, key, owner_id, lease_seconds, clock)
            continue
        current = _object(
            _json(raw, key),
            "run-once lease",
            required={
                "schema_version",
                "state",
                "owner_id",
                "acquired_at",
                "renewed_at",
                "expires_at",
                "released_at",
            },
        )
        if current["schema_version"] != "milk.run-once-lease.v1":
            raise ValueError("run-once lease schema is invalid")
        current_expires = _utc(current["expires_at"], "run lease expires_at")
        if current["state"] == "active" and current_expires > now:
            return None
        if current["state"] not in {"active", "released"}:
            raise ValueError("run-once lease state is invalid")
        if store.replace(key, body, etag, "application/json"):
            return _RunLease(store, key, owner_id, lease_seconds, clock)
    now = clock()
    latest = _json(store.get(key), key)
    if latest.get("state") == "active" and _utc(
        latest.get("expires_at"), "run lease expires_at"
    ) > now:
        return None
    raise RuntimeError("run-once lease acquisition did not converge")


def _bind_scope_profile(store, config):
    binding = {
        "schema_version": "milk.scope-profile.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
    }
    create_same(
        store,
        f"{config.prefix}/scope-profile.json",
        canonical_json(binding),
        "application/json",
    )


def run_once(config, *, store=None, teacher=None, now=None, clock=None):
    if not isinstance(config, RunConfig):
        raise TypeError("config must be RunConfig")
    store = store or config.store.open(config.source.max_trace_object_bytes)
    if teacher is None:
        teacher = DirectTeacher(config.teacher)
    if clock is None:
        if now is None:
            clock = lambda: dt.datetime.now(dt.timezone.utc)
        else:
            clock = lambda: now
    now = now or clock()
    if not isinstance(now, dt.datetime) or now.utcoffset() != dt.timedelta(0):
        raise ValueError("now must be UTC")
    _bind_scope_profile(store, config)
    lease = _acquire_run_lease(store, config, clock)
    if lease is None:
        report = _existing_report(store, config, _RunMeter(store, config))
        report["run_lock"] = "busy"
        return report
    try:
        report = _run_once_locked(
            config, _LeasedStore(store, lease), teacher, now, lease
        )
        report["run_lock"] = "acquired"
        return report
    finally:
        lease.release()


def _run_once_locked(config, store, teacher, now, lease):
    lease.assert_active()
    watermark, watermark_etag = _load_watermark(store, config)
    discovered, unused_stats, unused_traces, next_watermark = _collect(
        store, config, now, watermark
    )
    del unused_stats, unused_traces
    pending_parent, pending = _load_pending_source(store, config)
    source = _merge_sources(config, pending, discovered)
    source, stats, traces = _materialize_source(store, config, source)
    if not source["windows"]:
        return _existing_report(store, config, _RunMeter(store, config))
    pending_sha256, pending_status = _publish_pending_source(
        store, config, source, pending_parent
    )
    watermark_status = _advance_watermark(
        store,
        config,
        watermark,
        watermark_etag,
        next_watermark,
    )
    source_sha256 = _digest(source)
    structural = _structural_summary(source, stats, traces)
    summary_job_id = _digest({"job_type": "summary", "source_manifest_sha256": source_sha256, "code_version": CODE_VERSION})
    _job_write(
        store,
        config.prefix,
        "summary",
        summary_job_id,
        "claim",
        {"schema_version": "milk.summary-job-claim.v1", "job_id": summary_job_id, "source_manifest_sha256": source_sha256},
    )
    _job_write(store, config.prefix, "summary", summary_job_id, "result", structural)
    meter = _RunMeter(store, config)
    classification_eligible = _structural_can_classify(config, structural)
    if classification_eligible:
        classification, classifier_job_id, classifier_called, sample = _classification(
            store,
            config,
            meter,
            teacher,
            lease,
            now,
            traces,
            source_sha256,
        )
    else:
        classification, classifier_job_id, classifier_called, sample = (
            None,
            None,
            False,
            [],
        )
    labels = classification.get("output", []) if classification and classification.get("outcome") == "succeeded" else []
    summary_pointer = f"{config.prefix}/summaries/current.json"
    parent_summary, current_summary = _current_version(store, summary_pointer)
    summary_identity = {
        "schema_version": "milk.summary-version.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "structural": structural,
        "classifier_job_id": classifier_job_id,
        "classifier_result_sha256": _digest(classification) if classification else None,
        "semantic": _semantic_summary(labels),
        "code_version": CODE_VERSION,
    }
    if current_summary and current_summary.get("structural", {}).get("source_manifest_sha256") == source_sha256 and current_summary.get("classifier_result_sha256") == summary_identity["classifier_result_sha256"]:
        summary_sha256 = parent_summary
        summary = current_summary
        summary_status = "existing"
    else:
        summary_sha256, summary_key, summary, summary_status = _publish_version(
            store, config.prefix, "summary", summary_identity, parent_summary, "summaries", "summaries/current.json"
        )
        del summary_key
    summary["summary_sha256"] = summary_sha256
    unused_eval_sha256, current_eval = _current_version(
        store, f"{config.prefix}/evals/{config.eval.series_id}/current.json"
    )
    del unused_eval_sha256
    eval_result_exists = bool(
        current_eval and current_eval.get("summary_sha256") == summary_sha256
    )
    readiness = _readiness(
        config,
        summary,
        labels,
        sample,
        meter,
        eval_result_exists,
    )
    readiness_sha256 = _digest(readiness)
    readiness_key = f"{config.prefix}/readiness/versions/{readiness_sha256}.json"
    create_same(store, readiness_key, canonical_json(readiness), "application/json")
    readiness_parent, unused_current = _current_version(store, f"{config.prefix}/readiness/current.json")
    del unused_current
    readiness_status = _advance_pointer(
        store,
        f"{config.prefix}/readiness/current.json",
        "readiness",
        readiness_sha256,
        readiness_key,
        readiness_parent,
    )
    eval_sha256 = None
    eval_called = False
    eval_job_id = None
    eval_status = "not_ready"
    eval_result = None
    route_proposal_sha256 = None
    if readiness["ready"]:
        eval_result, eval_job_id, eval_called = _eval_generation(
            store,
            config,
            meter,
            teacher,
            lease,
            now,
            sample,
            labels,
            summary_sha256,
            readiness_sha256,
        )
        if eval_result.get("outcome") == "succeeded":
            eval_pointer_key = f"{config.prefix}/evals/{config.eval.series_id}/current.json"
            parent_eval, current_eval = _current_version(store, eval_pointer_key)
            eval_identity = {
                "schema_version": "milk.eval-revision.v1",
                "scope_id": config.scope_id,
                "profile": config.profile,
                "series_id": config.eval.series_id,
                "summary_sha256": summary_sha256,
                "readiness_sha256": readiness_sha256,
                "generation_job_id": eval_job_id,
                "provider_request_id": eval_result["provider_request_id"],
                "token_usage": {
                    "input_tokens": eval_result["input_tokens"],
                    "output_tokens": eval_result["output_tokens"],
                },
                "cost_microusd": eval_result["calculated_cost_microusd"],
                "cases": eval_result["output"],
                "code_version": CODE_VERSION,
            }
            if current_eval and current_eval.get("generation_job_id") == eval_job_id:
                eval_sha256 = parent_eval
                eval_status = "existing"
            else:
                eval_sha256, eval_key, unused_eval, eval_status = _publish_version(
                    store,
                    config.prefix,
                    "eval",
                    eval_identity,
                    parent_eval,
                    f"evals/{config.eval.series_id}",
                    f"evals/{config.eval.series_id}/current.json",
                )
                del eval_key, unused_eval
            if config.route_proposal.enabled:
                proposal = {
                    "schema_version": "milk.unsigned-route-proposal.v1",
                    "scope_id": config.scope_id,
                    "eval_sha256": eval_sha256,
                    "candidate_id": config.route_proposal.candidate_id,
                    "api_base_url": config.route_proposal.api_base_url,
                    "model": config.route_proposal.model,
                    "candidate_basis_points": config.route_proposal.candidate_basis_points,
                }
                route_proposal_sha256 = _digest(proposal)
                proposal_key = f"{config.prefix}/route-proposals/versions/{route_proposal_sha256}.json"
                create_same(store, proposal_key, canonical_json(proposal), "application/json")
                proposal_parent, unused_proposal = _current_version(store, f"{config.prefix}/route-proposals/current.json")
                del unused_proposal
                _advance_pointer(
                    store,
                    f"{config.prefix}/route-proposals/current.json",
                    "route_proposal",
                    route_proposal_sha256,
                    proposal_key,
                    proposal_parent,
                )
    classification_terminal = classification is not None and classification.get("outcome") not in {
        "not_started_budget_or_call_cap",
        "not_started_input_token_cap",
        "not_started_input_size_cap",
        "in_progress_or_ambiguous",
    }
    eval_terminal = (
        not readiness["ready"]
        or (
            eval_result is not None
            and eval_result.get("outcome")
            not in {
                "not_started_budget_or_call_cap",
                "not_started_input_token_cap",
                "not_started_input_size_cap",
                "in_progress_or_ambiguous",
            }
        )
    )
    if classification_terminal and eval_terminal:
        unused_empty_sha256, pending_status = _publish_pending_source(
            store,
            config,
            _empty_source(config),
            pending_sha256,
        )
        del unused_empty_sha256
    return {
        "schema_version": "milk.run-once-report.v1",
        "scope_id": config.scope_id,
        "profile": config.profile,
        "source_manifest_sha256": source_sha256,
        "closed_windows": source["windows"],
        "trace_count": len(traces),
        "summary_sha256": summary_sha256,
        "summary_pointer": summary_status,
        "classifier_job_id": classifier_job_id,
        "classifier_provider_called": classifier_called,
        "readiness_sha256": readiness_sha256,
        "ready": readiness["ready"],
        "statistically_qualified": readiness["statistically_qualified"],
        "eval_job_id": eval_job_id,
        "eval_provider_called": eval_called,
        "eval_sha256": eval_sha256,
        "eval_pointer": eval_status,
        "route_proposal_sha256": route_proposal_sha256,
        "provider_calls": meter.calls,
        "provider_tokens": meter.tokens,
        "accounted_incremental_spend_microusd": meter.accounted_spend,
        "route_activation_attempted": False,
        "watermark": watermark_status,
        "pending_source": pending_status,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run one deterministic Milk summary/eval reconciliation pass")
    parser.add_argument("--config", required=True, type=Path)
    arguments = parser.parse_args(argv)
    report = run_once(RunConfig.load(arguments.config))
    sys.stdout.buffer.write(canonical_json(report))


if __name__ == "__main__":
    main()
