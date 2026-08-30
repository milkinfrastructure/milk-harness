from __future__ import annotations

import base64
from dataclasses import replace
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import uuid

from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.run_once import (
    CAPABILITY_VALUES,
    CLASSIFIER_INSTRUCTIONS,
    DirectTeacher,
    DirectScoreClient,
    DOMAIN_VALUES,
    EVAL_INSTRUCTIONS,
    MAX_DECODED_TRACE_BYTES,
    OPERATION_VALUES,
    ORACLE_VALUES,
    RunConfig,
    ScoreResponse,
    TeacherResponse,
    _RunMeter,
    _advance_pointer,
    _classification_sources,
    _conservative_token_bound,
    _decompress_trace,
    _eligible_eval_inputs,
    _eval_case_plan,
    _eval_cases_from_pairs,
    _eval_operation_quotas,
    _harness_revision,
    _load_optional_json,
    _parse_trace,
    _request_text_prefix,
    _response_text_prefix,
    _stream_response,
    _teacher_request_body,
    _utc,
    _validate_eval_output,
    run_once,
)


SCOPE_ID = "01890f1e-2c40-7000-8000-000000000001"
HOUR = dt.datetime(2026, 8, 29, 10, tzinfo=dt.timezone.utc)
NOW = dt.datetime(2026, 8, 29, 12, tzinfo=dt.timezone.utc)


class FakeTeacher:
    def __init__(self):
        self.calls = []

    def complete(self, *, task, instructions, payload, job_id):
        del instructions
        self.calls.append((task, job_id))
        if task == "classify":
            value = {
                "labels": [
                    [0, 0, 0b000101, 3, "en", False]
                    for unused_item in payload["rows"]
                ]
            }
        elif task == "generate_eval":
            value = {
                "pairs": [
                    [f"new task {index}", f"expected result {index}"]
                    for index in range(len(payload["case_plan"]))
                ]
            }
        elif task == "validate_eval":
            value = {
                "verdicts": [
                    [True, "accepted"] for unused_case in payload["cases"]
                ]
            }
        else:
            raise AssertionError(task)
        return TeacherResponse(
            value,
            "fake-" + job_id[:16],
            max(1, _conservative_token_bound(canonical_json(payload))),
            16,
        )

    def invoke(self, *, target_name, target, case, job_id):
        del target
        self.calls.append(("score_" + target_name, job_id))
        return ScoreResponse(
            case["expected"],
            f"fake-{target_name}-{case['case_id']}",
            8,
            4,
            10 if target_name == "incumbent" else 9,
        )


class PayloadTeacher(FakeTeacher):
    def __init__(self):
        super().__init__()
        self.payloads = {}

    def complete(self, **kwargs):
        self.payloads[kwargs["task"]] = kwargs["payload"]
        return super().complete(**kwargs)


class TailSupplementTeacher(PayloadTeacher):
    def complete(self, **kwargs):
        if kwargs["task"] != "classify":
            return super().complete(**kwargs)
        self.payloads["classify"] = kwargs["payload"]
        self.calls.append(("classify", kwargs["job_id"]))
        rare_codes = iter((1, 2, 4, 5))
        labels = []
        for row in kwargs["payload"]["rows"]:
            operation = next(rare_codes) if row[1].startswith("rare operation") else 0
            labels.append([operation, 0, 0b000101, 3, "en", False])
        return TeacherResponse(
            {"labels": labels},
            "tail-supplement-classifier",
            1000,
            100,
        )


class VagueEvalTeacher(FakeTeacher):
    def complete(self, **kwargs):
        response = super().complete(**kwargs)
        if kwargs["task"] == "generate_eval":
            for pair in response.value["pairs"]:
                pair[1] = "OK"
        return response


class BadScoreClient:
    def __init__(self):
        self.calls = []

    def invoke(self, *, target_name, target, case, job_id):
        del target
        self.calls.append((target_name, case["case_id"], job_id))
        output = case["expected"] if target_name == "incumbent" else "unrelated"
        return ScoreResponse(
            output,
            f"bad-score-{target_name}-{case['case_id']}",
            8,
            4,
            12,
        )


class FakeHttpResponse:
    def __init__(self, value, request_id="score-request-1"):
        self.raw = canonical_json(value)
        self.headers = {"x-request-id": request_id}

    def __enter__(self):
        return self

    def __exit__(self, unused_type, unused_value, unused_traceback):
        return False

    def read(self, limit):
        return self.raw[:limit]


class RecordingOpener:
    def __init__(self, response):
        self.response = response
        self.request = None
        self.timeout = None

    def open(self, request, timeout):
        self.request = request
        self.timeout = timeout
        return self.response


def config(root, profile="mechanics"):
    return RunConfig.parse(
        {
            "schema_version": "milk.harness-run-config.v1",
            "scope_id": SCOPE_ID,
            "profile": profile,
            "store": {"kind": "local", "root": str(root)},
            "source": {
                "close_delay_seconds": 300,
                "max_windows": 24,
                "max_stats_shards": 999,
                "max_traces": 3000 if profile == "production" else 999,
                "max_trace_object_bytes": 4 * 1024 * 1024,
                "max_total_trace_bytes": 64 * 1024 * 1024,
                "teacher_trace_bytes": 64,
                "eval_trace_bytes": 1024,
                "classifier_sample_sessions": 750 if profile == "production" else 100,
            },
            "teacher": {
                "api_url": "https://model.example.test/v1/chat/completions",
                "model": "glm-test",
                "reasoning_effort": "low",
                "api_key_env": "MILK_TEST_TEACHER_API_KEY",
                "timeout_seconds": 30,
                "max_calls_per_run": 2,
                "max_input_tokens_per_call": 100_000,
                "max_output_tokens_per_call": 4096,
                "max_total_tokens_per_run": 200_000,
                "input_rate_microusd_per_million": 1000,
                "output_rate_microusd_per_million": 2000,
            },
            "budget": {
                "starting_spend_microusd": 0,
                "stop_new_spend_microusd": 20_000_000,
                "absolute_spend_microusd": 25_000_000,
            },
            "eval": {
                "series_id": "mechanics-v1",
                "representative_cases": 3,
                "tail_cases": 2,
                "max_source_traces": 32,
            },
            "route_proposal": {
                "enabled": True,
                "candidate_id": "glm-mechanics",
                "api_base_url": "https://candidate.example.test/v1/",
                "model": "glm-test",
                "candidate_basis_points": 100,
            },
            "candidate_score": {
                "incumbent": {
                    "api_url": "https://incumbent.example.test/v1/chat/completions",
                    "model": "glm-test",
                    "api_key_env": "MILK_TEST_INCUMBENT_API_KEY",
                    "input_rate_microusd_per_million": 1000,
                    "output_rate_microusd_per_million": 2000,
                },
                "candidate": {
                    "api_url": "https://candidate.example.test/v1/chat/completions",
                    "model": "glm-test",
                    "api_key_env": "MILK_TEST_CANDIDATE_API_KEY",
                    "input_rate_microusd_per_million": 1000,
                    "output_rate_microusd_per_million": 2000,
                },
                "held_out_cases": 3,
                "timeout_seconds": 30,
                "minimum_request_interval_ms": 0,
                "max_calls_per_run": 6,
                "max_input_tokens_per_call": 128,
                "max_output_tokens_per_call": 16,
                "max_total_tokens_per_run": 864,
                "case_reference_similarity_basis_points": 9500,
                "minimum_candidate_reference_pass_basis_points": 9000,
                "minimum_reference_pass_delta_basis_points": 0,
                "maximum_candidate_error_basis_points": 0,
                "maximum_candidate_p95_latency_ms": 1000,
            },
        }
    )


def request_id(index, hour=HOUR):
    millis = int(hour.timestamp() * 1000) + index + 1
    value = (
        (millis << 80)
        | (0x7 << 76)
        | ((index & 0xFFF) << 64)
        | (0b10 << 62)
        | (index + 1)
    )
    return str(uuid.UUID(int=value))


def body(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return raw, {
        "content_type": "application/json",
        "content_encoding": None,
        "body_base64": base64.b64encode(raw).decode(),
        "byte_len": len(raw),
    }


def raw_body(raw, content_type):
    return raw, {
        "content_type": content_type,
        "content_encoding": None,
        "body_base64": base64.b64encode(raw).decode(),
        "byte_len": len(raw),
    }


def trace(index, scope_id=SCOPE_ID, hour=HOUR):
    identifier = request_id(index, hour)
    request_raw, request = body(
        {
            "model": "baseline-model",
            "messages": [{"role": "user", "content": f"question {index}"}],
            "stream": False,
            "future_optional_field": {"retained": True},
        }
    )
    response_raw, response = body(
        {
            "id": f"response-{index}",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": f"answer {index}"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10 + index, "completion_tokens": 5},
        }
    )
    value = {
        "schema_version": "milk.trace.v1",
        "catalog": {
            "scope_id": scope_id,
            "request_id": identifier,
            "occurred_at": f"2026-08-29T10:{index % 60:02d}:00Z",
            "endpoint": "chat_completions",
            "request_parse_success": True,
            "streaming": False,
            "route_revision": "baseline-v1",
            "route_observation": {
                "state": "ineligible",
                "reason": "policy_absent",
            },
            "provider_status": 200,
            "error_class": None,
            "ttft_ms": 10 + index,
            "completion_ms": 20 + index,
            "request_bytes": len(request_raw),
            "response_bytes": len(response_raw),
            "sampling_algorithm": "hmac_sha256_session_root_v1",
            "sampling_policy_version": "mechanics-v1",
            "inclusion_probability_basis_points": 10_000,
            "capture_eligible": True,
            "capture_selected": True,
            "sampling_unit_kind": "chat_session_header",
            "sampling_unit_hmac_sha256": hashlib.sha256(f"session-{index}".encode()).hexdigest(),
            "sampling_independence": "independent",
            "sampling_key_version": "mechanics-v1",
            "previous_response_hmac_sha256": None,
            "rights_state": "mechanics",
            "retention_until": "2026-09-05T10:00:00Z",
        },
        "request": request,
        "response": response,
    }
    return identifier, value


def seed(root, count=100, profile="mechanics", hour=HOUR, index_offset=0):
    store = LocalEvidenceStore(root)
    prefix = f"milk/v1/scopes/{SCOPE_ID}"
    for index in range(count):
        identifier, value = trace(index + index_offset, hour=hour)
        value["catalog"]["occurred_at"] = (
            hour + dt.timedelta(minutes=index % 60)
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        payload = canonical_json(value)
        suffix = ".json"
        content_type = "application/json"
        if profile == "production":
            command = shutil.which("zstd")
            if command is None:
                raise unittest.SkipTest("zstd is required for production wire fixtures")
            payload = subprocess.run(
                [command, "--compress", "--stdout", "--quiet", "-3"],
                input=payload,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            ).stdout
            suffix = ".json.zst"
            content_type = "application/zstd"
        store.create(
            f"{prefix}/traffic/{hour:%Y/%m/%d/%H}/{identifier}{suffix}",
            payload,
            content_type,
        )
    stats = {
        "schema_version": "milk.stats-shard.v1",
        "scope_id": SCOPE_ID,
        "writer_id": str(uuid.UUID(int=uuid.UUID("01890f1e-2c40-7000-8000-00000000abcd").int + index_offset)),
        "flush_id": str(uuid.UUID(int=uuid.UUID("01890f1e-2c40-7000-8000-00000000dcba").int + index_offset)),
        "hour": hour.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "recorded_at": (hour + dt.timedelta(hours=1)).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sampling_algorithm": "hmac_sha256_session_root_v1",
        "sampling_policy_version": "mechanics-v1",
        "sampling_key_version": "mechanics-v1",
        "inclusion_probability_basis_points": 10_000,
        "values": {
            "observed": count,
            "request_parse_success": count,
            "request_parse_failure": 0,
            "eligible": count,
            "selected": count,
            "captured": count,
            "not_selected": 0,
            "oversized": 0,
            "interrupted": 0,
            "queued": count,
            "dropped": 0,
            "capture_failed": 0,
            "traces_persisted": count,
            "trace_persist_failures": 0,
            "stats_persist_failures": 0,
        },
    }
    store.create(
        f"{prefix}/stats/{hour:%Y/%m/%d/%H}/{stats['writer_id']}/{stats['flush_id']}.json",
        canonical_json(stats),
        "application/json",
    )
    return store


class TimeoutTeacher(FakeTeacher):
    def complete(self, **kwargs):
        self.calls.append((kwargs["task"], kwargs["job_id"]))
        raise TimeoutError("accepted request disconnected before response")


class ClaimAcceptedThenTimeoutStore(LocalEvidenceStore):
    def __init__(self, root):
        super().__init__(root)
        self.failed = False

    def create(self, key, body, content_type="application/octet-stream"):
        created = super().create(key, body, content_type)
        if not self.failed and "/jobs/classify/" in key and key.endswith("/claim.json"):
            self.failed = True
            raise TimeoutError("object create accepted but response was lost")
        return created


class InvalidTeacher(FakeTeacher):
    def complete(self, **kwargs):
        self.calls.append((kwargs["task"], kwargs["job_id"]))
        return TeacherResponse(
            {"labels": []},
            "invalid-response",
            10,
            10,
        )


class LeakingEvalTeacher(FakeTeacher):
    def complete(self, **kwargs):
        response = super().complete(**kwargs)
        if kwargs["task"] == "generate_eval":
            response.value["pairs"][0] = [
                "Compute two plus two without copying four.",
                "four",
            ]
        return response


class ContractErrorTeacher(FakeTeacher):
    def complete(self, **kwargs):
        self.calls.append((kwargs["task"], kwargs["job_id"]))
        raise ValueError("teacher response content must be a JSON string")


class HttpErrorTeacher(FakeTeacher):
    def complete(self, **kwargs):
        self.calls.append((kwargs["task"], kwargs["job_id"]))
        raise urllib.error.HTTPError(
            kwargs["job_id"],
            429,
            "rate limited",
            {"x-request-id": "provider-http-429"},
            None,
        )


class BlockingTeacher(FakeTeacher):
    def __init__(self):
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def complete(self, **kwargs):
        if kwargs["task"] == "classify":
            self.started.set()
            if not self.release.wait(5):
                raise TimeoutError("test did not release classifier")
        return super().complete(**kwargs)


class MutableClock:
    def __init__(self, current):
        self.current = current

    def __call__(self):
        return self.current


class LeaseExpiringTeacher(FakeTeacher):
    def __init__(self, clock):
        super().__init__()
        self.clock = clock

    def complete(self, **kwargs):
        response = super().complete(**kwargs)
        if kwargs["task"] == "classify":
            self.clock.current += dt.timedelta(seconds=301)
        return response


class AbstainingTeacher(FakeTeacher):
    def __init__(self):
        super().__init__()
        self.eval_payload = None

    def complete(self, **kwargs):
        if kwargs["task"] == "classify":
            self.calls.append((kwargs["task"], kwargs["job_id"]))
            count = len(kwargs["payload"]["rows"])
            labels = [[0, 0, 1, 3, "en", False] for _ in range(count - 10)]
            labels.extend([[9, 0, 1, 3, "en", True] for _ in range(10)])
            return TeacherResponse(
                {"labels": labels}, "abstain-classifier", 1000, 100
            )
        self.eval_payload = kwargs["payload"]
        return super().complete(**kwargs)


class PagedKeyStore:
    def __init__(self, keys):
        self.keys = sorted(keys)

    def list(self, prefix, limit=None, *, start_after=None):
        keys = [
            key
            for key in self.keys
            if key.startswith(prefix + "/")
            and (start_after is None or key > start_after)
        ]
        return keys[:limit]


def add_responses_zstd_trace(root):
    command = shutil.which("zstd")
    if command is None:
        raise unittest.SkipTest("zstd is required for the gateway wire fixture")
    store = LocalEvidenceStore(root)
    prefix = f"milk/v1/scopes/{SCOPE_ID}"
    identifier = request_id(900)
    request_raw, request = body(
        {
            "model": "baseline-model",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Find the weather"},
                        {"type": "future_input_item", "opaque": True},
                    ],
                }
            ],
            "tools": [
                {
                    "type": "function",
                    "name": "weather",
                    "parameters": {"type": "object"},
                }
            ],
            "text": {"format": {"type": "json_schema", "name": "weather"}},
            "reasoning": {"effort": "medium"},
            "conversation": "conversation-1",
            "stream": True,
            "unknown_optional_request_field": {"preserve": True},
        }
    )
    completed = {
        "id": "resp_1",
        "status": "completed",
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Calling weather"}],
            },
            {
                "type": "function_call",
                "name": "weather",
                "arguments": "{}",
            },
        ],
        "usage": {
            "input_tokens": 44,
            "output_tokens": 12,
            "input_tokens_details": {"cached_tokens": 4},
            "output_tokens_details": {"reasoning_tokens": 3},
        },
    }
    event = json.dumps(
        {"type": "response.completed", "response": completed},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    response_raw, response = raw_body(
        b"event: response.completed\ndata: " + event + b"\n\n",
        "text/event-stream",
    )
    value = {
        "schema_version": "milk.trace.v1",
        "catalog": {
            "scope_id": SCOPE_ID,
            "request_id": identifier,
            "occurred_at": "2026-08-29T10:59:00Z",
            "endpoint": "responses",
            "request_parse_success": True,
            "streaming": True,
            "route_revision": "baseline-v1",
            "route_observation": {
                "state": "ineligible",
                "reason": "policy_absent",
            },
            "provider_status": 200,
            "error_class": None,
            "ttft_ms": 15,
            "completion_ms": 30,
            "request_bytes": len(request_raw),
            "response_bytes": len(response_raw),
            "sampling_algorithm": "hmac_sha256_session_root_v1",
            "sampling_policy_version": "mechanics-v1",
            "inclusion_probability_basis_points": 10_000,
            "capture_eligible": True,
            "capture_selected": True,
            "sampling_unit_kind": "responses_conversation",
            "sampling_unit_hmac_sha256": hashlib.sha256(b"responses-session").hexdigest(),
            "sampling_independence": "independent",
            "sampling_key_version": "mechanics-v1",
            "previous_response_hmac_sha256": None,
            "rights_state": "mechanics",
            "retention_until": "2026-09-05T10:00:00Z",
        },
        "request": request,
        "response": response,
    }
    compressed = subprocess.run(
        [command, "--compress", "--stdout", "--quiet", "-3"],
        input=canonical_json(value),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout
    store.create(
        f"{prefix}/traffic/2026/08/29/10/{identifier}.json.zst",
        compressed,
        "application/zstd",
    )
    stats = {
        "schema_version": "milk.stats-shard.v1",
        "scope_id": SCOPE_ID,
        "writer_id": "01890f1e-2c40-7000-8000-00000000eeee",
        "flush_id": "01890f1e-2c40-7000-8000-00000000ffff",
        "hour": "2026-08-29T10:00:00Z",
        "recorded_at": "2026-08-29T11:00:00Z",
        "sampling_algorithm": "hmac_sha256_session_root_v1",
        "sampling_policy_version": "mechanics-v1",
        "sampling_key_version": "mechanics-v1",
        "inclusion_probability_basis_points": 10_000,
        "values": {
            "observed": 1,
            "request_parse_success": 1,
            "request_parse_failure": 0,
            "eligible": 1,
            "selected": 1,
            "captured": 1,
            "not_selected": 0,
            "oversized": 0,
            "interrupted": 0,
            "queued": 1,
            "dropped": 0,
            "capture_failed": 0,
            "traces_persisted": 1,
            "trace_persist_failures": 0,
            "stats_persist_failures": 0,
        },
    }
    store.create(
        f"{prefix}/stats/2026/08/29/10/{stats['writer_id']}/{stats['flush_id']}.json",
        canonical_json(stats),
        "application/json",
    )
    return store


class RunOnceTests(unittest.TestCase):
    def test_direct_teacher_json_content_contract(self):
        bounded = config("/tmp")
        expected = {"labels": [[0, 0, 1, 0, "en", False]]}

        def complete(content):
            response = FakeHttpResponse(
                {
                    "choices": [{"message": {"content": content}}],
                    "usage": {"prompt_tokens": 7, "completion_tokens": 3},
                }
            )
            with mock.patch.dict(
                os.environ,
                {"MILK_TEST_TEACHER_API_KEY": "secret-test-key"},
                clear=False,
            ):
                return DirectTeacher(
                    bounded.teacher, opener=RecordingOpener(response)
                ).complete(
                    task="classify",
                    instructions=CLASSIFIER_INSTRUCTIONS,
                    payload={"rows": [["c", "request", 0, 8]]},
                    job_id="a" * 64,
                )

        for content in (json.dumps(expected), expected):
            with self.subTest(content_type=type(content).__name__):
                self.assertEqual(complete(content).value, expected)

        for content in ([], 1, True, None):
            with self.subTest(content=content):
                with self.assertRaisesRegex(
                    ValueError,
                    "teacher response content must be a JSON string or object",
                ):
                    complete(content)

    def test_harness_revision_is_exact_and_fail_closed(self):
        with mock.patch.dict(
            os.environ, {"MILK_HARNESS_REVISION": "a" * 40}, clear=False
        ):
            self.assertEqual(_harness_revision(), "a" * 40)
        with mock.patch.dict(
            os.environ, {"MILK_HARNESS_REVISION": "A" * 40}, clear=False
        ):
            with self.assertRaisesRegex(ValueError, "lowercase Git commit"):
                _harness_revision()

    def test_real_rust_nanosecond_trace_timestamp_is_accepted(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            identifier, value = trace(0)
            value["catalog"]["occurred_at"] = "2026-08-29T10:00:00.240391261Z"
            key = (
                f"milk/v1/scopes/{SCOPE_ID}/traffic/2026/08/29/10/"
                f"{identifier}.json"
            )
            store.create(key, canonical_json(value), "application/json")

            parsed = _parse_trace(store, config(root), key)

            self.assertEqual(
                parsed.occurred_at,
                HOUR + dt.timedelta(microseconds=240391),
            )

    def test_utc_rejects_more_than_rust_nanosecond_precision(self):
        with self.assertRaisesRegex(ValueError, "must be a UTC timestamp"):
            _utc("2026-08-29T10:00:00.2403912610Z", "occurred_at")

    def test_subthreshold_free_run_does_not_require_teacher_key(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=30)
            with mock.patch.dict(
                os.environ, {"MILK_TEST_TEACHER_API_KEY": ""}, clear=False
            ):
                report = run_once(config(root), store=store, now=NOW)
            self.assertEqual(report["provider_calls"], 0)
            self.assertEqual(report["trace_count"], 30)

    def test_exact_teacher_body_is_bounded_before_claim(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            value = _config_dict(root)
            value["teacher"]["max_input_tokens_per_call"] = 4_000
            bounded = RunConfig.parse(value)
            teacher = FakeTeacher()
            report = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )
            self.assertFalse(report["ready"])
            self.assertEqual(teacher.calls, [])
            self.assertFalse(store.list(bounded.prefix + "/jobs/classify"))

    def test_mixed_sampling_policy_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            source_key = store.list(config(root).prefix + "/stats")[0]
            shard = json.loads(store.get(source_key))
            shard["writer_id"] = "01890f1e-2c40-7000-8000-00000000eeee"
            shard["flush_id"] = "01890f1e-2c40-7000-8000-00000000ffff"
            shard["sampling_policy_version"] = "mechanics-v2"
            mixed_key = (
                f"{config(root).prefix}/stats/{HOUR:%Y/%m/%d/%H}/"
                f"{shard['writer_id']}/{shard['flush_id']}.json"
            )
            store.create(mixed_key, canonical_json(shard), "application/json")
            with self.assertRaisesRegex(ValueError, "mixes sampling policies"):
                run_once(config(root), store=store, teacher=FakeTeacher(), now=NOW)

    def test_total_decoded_source_bytes_are_bounded(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            value = _config_dict(root)
            value["source"]["max_total_trace_bytes"] = 1024
            bounded = RunConfig.parse(value)
            teacher = FakeTeacher()
            with self.assertRaisesRegex(ValueError, "max_total_trace_bytes"):
                run_once(bounded, store=store, teacher=teacher, now=NOW)
            self.assertEqual(teacher.calls, [])

    def test_zstd_expansion_is_stopped_at_decoded_limit(self):
        command = shutil.which("zstd")
        if command is None:
            self.skipTest("zstd is required")
        compressed = subprocess.run(
            [command, "--compress", "--stdout", "--quiet", "-3"],
            input=b"x" * (MAX_DECODED_TRACE_BYTES + 1),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout
        self.assertLess(len(compressed), 1024)
        with self.assertRaisesRegex(ValueError, "oversized"):
            _decompress_trace("traffic.json.zst", compressed)

    def test_chat_and_responses_stream_terminals_are_strict(self):
        chat_event = json.dumps(
            {
                "id": "chat-1",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2},
            },
            separators=(",", ":"),
        ).encode()
        chat = _stream_response(
            "chat_completions", b"data: " + chat_event + b"\n\ndata: [DONE]\n\n"
        )
        self.assertEqual(chat["usage"]["completion_tokens"], 2)

        completed = {"id": "resp-1", "status": "completed", "output": []}
        response_event = json.dumps(
            {"type": "response.completed", "response": completed},
            separators=(",", ":"),
        ).encode()
        response = _stream_response(
            "responses",
            b"event: response.completed\ndata: " + response_event + b"\n\n",
        )
        self.assertEqual(response, completed)
        with self.assertRaisesRegex(ValueError, "terminal"):
            _stream_response("chat_completions", b"data: " + chat_event + b"\n\n")
        with self.assertRaisesRegex(ValueError, "terminal"):
            _stream_response(
                "responses", b"event: response.output_text.delta\ndata: {}\n\n"
            )

    def test_streamed_request_can_capture_json_provider_error(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            identifier, value = trace(1)
            value["catalog"]["streaming"] = True
            value["catalog"]["provider_status"] = 429
            value["catalog"]["error_class"] = "upstream_status"
            response_raw, response = body(
                {"error": {"type": "rate_limit_error", "message": "bounded"}}
            )
            value["response"] = response
            value["catalog"]["response_bytes"] = len(response_raw)
            key = (
                f"{config(root).prefix}/traffic/2026/08/29/10/{identifier}.json"
            )
            store.create(key, canonical_json(value), "application/json")
            parsed = _parse_trace(store, config(root), key)
            self.assertTrue(parsed.parse_success)
            self.assertEqual(parsed.response["error"]["type"], "rate_limit_error")

    def test_repeated_provider_failures_do_not_block_32_successful_eval_sources(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=49)
            value = _config_dict(root)
            value["source"]["classifier_sample_sessions"] = 32
            value["eval"]["representative_cases"] = 24
            value["eval"]["tail_cases"] = 8
            value["eval"]["max_source_traces"] = 32
            bounded = RunConfig.parse(value)
            traffic_keys = store.list(bounded.prefix + "/traffic")
            for index, key in enumerate(traffic_keys):
                raw, etag = store.get_versioned(key)
                retained = json.loads(raw)
                request_raw, request = body(
                    {
                        "model": "baseline-model",
                        "messages": [
                            {
                                "role": "user",
                                "content": f"question {index:03d}",
                            }
                        ],
                        "stream": False,
                        "future_optional_field": {"retained": True},
                    }
                )
                retained["request"] = request
                retained["catalog"]["request_bytes"] = len(request_raw)
                self.assertTrue(
                    store.replace(key, canonical_json(retained), etag)
                )
            failed_keys = sorted(
                traffic_keys,
                key=lambda key: hashlib.sha256(
                    (
                        "milk.semantic-sample.v1\0"
                        + json.loads(store.get(key))["catalog"][
                            "sampling_unit_hmac_sha256"
                        ]
                    ).encode()
                ).hexdigest(),
            )[:17]
            failure_request_raw, failure_request = body(
                {
                    "model": "baseline-model",
                    "messages": [
                        {"role": "user", "content": "repeated failure"}
                    ],
                    "stream": False,
                }
            )
            failure_response_raw, failure_response = body(
                {
                    "error": {
                        "type": "rate_limit_error",
                        "message": "bounded",
                    }
                }
            )
            failed_sha256s = set()
            for failed_key in failed_keys:
                raw, etag = store.get_versioned(failed_key)
                failed = json.loads(raw)
                failed["request"] = failure_request
                failed["response"] = failure_response
                failed["catalog"]["request_bytes"] = len(failure_request_raw)
                failed["catalog"]["response_bytes"] = len(failure_response_raw)
                failed["catalog"]["provider_status"] = 429
                failed["catalog"]["error_class"] = "upstream_status"
                self.assertTrue(
                    store.replace(failed_key, canonical_json(failed), etag)
                )
                failed_sha256s.add(
                    _parse_trace(store, bounded, failed_key).object_sha256
                )
            teacher = PayloadTeacher()

            report = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )

            summary_pointer = json.loads(
                store.get(bounded.prefix + "/summaries/current.json")
            )
            summary = json.loads(store.get(summary_pointer["version_key"]))
            readiness_pointer = json.loads(
                store.get(bounded.prefix + "/readiness/current.json")
            )
            readiness = json.loads(
                store.get(readiness_pointer["version_key"])
            )
            plan = teacher.payloads["generate_eval"]["case_plan"]
            source_sha256s = [row[1] for row in plan]

            self.assertTrue(report["ready"])
            self.assertEqual(summary["structural"]["counts"]["failed"], 17)
            self.assertEqual(
                summary["structural"]["quality"]["duplicate_traces"], 16
            )
            self.assertGreater(
                summary["structural"]["quality"]["duplicate_basis_points"],
                10,
            )
            self.assertEqual(summary["semantic"]["classified"], 32)
            self.assertEqual(readiness["minimum_independent_sessions"], 32)
            self.assertEqual(readiness["eval_eligible_cases"], 32)
            self.assertEqual([row[0] for row in plan].count("representative"), 24)
            self.assertEqual([row[0] for row in plan].count("tail"), 8)
            self.assertEqual(len(source_sha256s), len(set(source_sha256s)))
            self.assertTrue(failed_sha256s.isdisjoint(source_sha256s))

    def test_mechanics_pipeline_is_content_addressed_and_replay_calls_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = FakeTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)

            prefix = config(root).prefix
            self.assertTrue(first["ready"])
            self.assertEqual(first["schema_version"], "milk.run-once-report.v2")
            self.assertFalse(first["statistically_qualified"])
            self.assertEqual(first["trace_count"], 100)
            self.assertEqual(first["provider_calls"], 2)
            self.assertGreater(first["accounted_incremental_spend_microusd"], 0)
            self.assertIsNotNone(first["eval_sha256"])
            self.assertIsNone(first["eval_validation_sha256"])
            self.assertIsNone(first["candidate_score_sha256"])
            self.assertIsNone(first["route_proposal_sha256"])
            self.assertFalse(first["route_activation_attempted"])
            refs = first["artifact_refs"]
            self.assertEqual(
                set(refs),
                {"scope_prefix", "source_manifest_key", "nodes", "provider_jobs"},
            )
            self.assertEqual(refs["scope_prefix"], prefix)
            self.assertEqual(
                refs["source_manifest_key"],
                f"{prefix}/pending-source/versions/"
                f"{first['source_manifest_sha256']}.json",
            )
            for name, root_name, digest_name in (
                ("summary", "summaries", "summary_sha256"),
                ("readiness", "readiness", "readiness_sha256"),
                ("eval", "evals/mechanics-v1", "eval_sha256"),
            ):
                self.assertEqual(
                    refs["nodes"][name],
                    {
                        "pointer_key": f"{prefix}/{root_name}/current.json",
                        "version_key": (
                            f"{prefix}/{root_name}/versions/{first[digest_name]}.json"
                        ),
                    },
                )
            self.assertEqual(
                refs["provider_jobs"],
                {
                    "classifier": f"{prefix}/jobs/classify/{first['classifier_job_id']}",
                    "eval_generation": f"{prefix}/jobs/generate-eval/{first['eval_job_id']}",
                    "eval_validation": None,
                    "candidate_score": None,
                },
            )
            self.assertEqual([call[0] for call in teacher.calls], ["classify", "generate_eval"])
            claim_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/claim.json")
            )
            identity = json.loads(store.get(claim_key))["identity"]
            self.assertEqual(
                identity["schema_version"], "milk.teacher-job-identity.v3"
            )
            self.assertEqual(len(identity["response_format_sha256"]), 64)
            self.assertEqual(
                identity["response_content_contract"],
                "milk.teacher-json-string-or-object.v1",
            )
            eval_claim_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/generate-eval")
                if key.endswith("/claim.json")
            )
            eval_identity = json.loads(store.get(eval_claim_key))["identity"]
            self.assertEqual(
                eval_identity["schema_version"], "milk.teacher-job-identity.v3"
            )
            self.assertNotEqual(
                identity["response_format_sha256"],
                eval_identity["response_format_sha256"],
            )
            prior_identity = dict(identity)
            prior_identity["schema_version"] = "milk.teacher-job-identity.v2"
            del prior_identity["response_content_contract"]
            prior_job_id = hashlib.sha256(
                canonical_json(prior_identity)
            ).hexdigest()
            self.assertNotEqual(first["classifier_job_id"], prior_job_id)

            second = run_once(config(root), store=store, teacher=teacher, now=NOW + dt.timedelta(days=1))

            self.assertEqual(second["provider_calls"], 7)
            self.assertFalse(second["classifier_provider_called"])
            self.assertFalse(second["eval_provider_called"])
            self.assertEqual(second["summary_sha256"], first["summary_sha256"])
            self.assertEqual(second["readiness_sha256"], first["readiness_sha256"])
            self.assertEqual(second["eval_sha256"], first["eval_sha256"])
            self.assertIsNotNone(second["eval_validation_sha256"])
            self.assertIsNotNone(second["candidate_score_sha256"])
            self.assertIsNotNone(second["route_proposal_sha256"])
            self.assertEqual(second["pending_source"], "advanced")

            third = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(days=2),
            )
            self.assertEqual(third["provider_calls"], 0)
            self.assertFalse(third["eval_validation_provider_called"])
            self.assertFalse(third["candidate_score_provider_called"])
            self.assertEqual(
                third["eval_validation_sha256"],
                second["eval_validation_sha256"],
            )
            self.assertEqual(
                third["candidate_score_sha256"],
                second["candidate_score_sha256"],
            )
            self.assertEqual(
                third["route_proposal_sha256"],
                second["route_proposal_sha256"],
            )
            self.assertEqual(third["artifact_refs"], second["artifact_refs"])
            for name, sha256 in (
                ("validation", second["eval_validation_sha256"]),
                ("score", second["candidate_score_sha256"]),
                ("proposal", second["route_proposal_sha256"]),
            ):
                self.assertTrue(
                    second["artifact_refs"]["nodes"][name]["version_key"].endswith(
                        f"/versions/{sha256}.json"
                    )
                )
            for name, job_type in (
                ("eval_validation", "validate-eval"),
                ("candidate_score", "score-candidate"),
            ):
                self.assertRegex(
                    second["artifact_refs"]["provider_jobs"][name],
                    rf"^{prefix}/jobs/{job_type}/[0-9a-f]{{64}}$",
                )

            route_pointer = json.loads(
                store.get(f"{config(root).prefix}/route-proposals/current.json")
            )
            proposal = json.loads(store.get(route_pointer["version_key"]))
            self.assertEqual(
                proposal["schema_version"], "milk.unsigned-route-proposal.v2"
            )
            self.assertEqual(
                proposal["eval_validation_sha256"],
                second["eval_validation_sha256"],
            )
            self.assertEqual(
                proposal["candidate_score_sha256"],
                second["candidate_score_sha256"],
            )
            self.assertEqual(len(proposal["provenance"]["harness_revision"]), 40)
            self.assertEqual(len(proposal["provenance"]["config_sha256"]), 64)
            self.assertNotIn("activation_authorized", proposal)
            self.assertNotIn("signature", proposal)
            self.assertFalse(store.list(f"{config(root).prefix}/routes"))

    def test_provider_timeout_is_terminal_and_never_retried(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = TimeoutTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            second = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertEqual(len(teacher.calls), 1)
            self.assertFalse(first["ready"])
            self.assertFalse(second["classifier_provider_called"])
            result_keys = [
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/result.json.zst")
            ]
            self.assertEqual(len(result_keys), 1)

    def test_vacuous_eval_is_rejected_content_addressed_and_replay_safe(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            bounded = config(root)
            teacher = VagueEvalTeacher()

            generated = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )
            rejected = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )
            replay = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )

            self.assertIsNotNone(generated["eval_sha256"])
            self.assertIsNone(generated["route_proposal_sha256"])
            self.assertIsNotNone(rejected["eval_validation_sha256"])
            self.assertIsNone(rejected["candidate_score_sha256"])
            self.assertIsNone(rejected["route_proposal_sha256"])
            self.assertEqual(rejected["pending_source"], "existing")
            self.assertFalse(rejected["route_activation_attempted"])
            self.assertEqual(replay["provider_calls"], 0)
            self.assertEqual(
                replay["eval_validation_sha256"],
                rejected["eval_validation_sha256"],
            )
            pointer = json.loads(
                store.get(
                    bounded.prefix
                    + "/eval-validations/mechanics-v1/current.json"
                )
            )
            revision = json.loads(store.get(pointer["version_key"]))
            self.assertFalse(revision["accepted"])
            self.assertEqual(
                {item["reason"] for item in revision["output"]["rejections"]},
                {"vacuous"},
            )
            route_pointer = json.loads(
                store.get(bounded.prefix + "/route-proposals/current.json")
            )
            blocked = json.loads(store.get(route_pointer["version_key"]))
            self.assertEqual(
                blocked["schema_version"], "milk.route-proposal-blocked.v1"
            )
            self.assertEqual(blocked["reason"], "eval_validation_rejected")

    def test_failed_candidate_score_replaces_prior_proposal_and_retains_source(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            bounded = config(root)
            teacher = FakeTeacher()
            run_once(bounded, store=store, teacher=teacher, now=NOW)
            accepted = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )
            self.assertIsNotNone(accepted["route_proposal_sha256"])

            second_hour = HOUR + dt.timedelta(hours=1)
            seed(
                root,
                count=100,
                hour=second_hour,
                index_offset=200,
            )
            run_once(
                bounded,
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(hours=2),
            )
            scorer = BadScoreClient()
            rejected = run_once(
                bounded,
                store=store,
                teacher=teacher,
                scorer=scorer,
                now=NOW + dt.timedelta(hours=2),
            )

            self.assertIsNotNone(rejected["eval_validation_sha256"])
            self.assertIsNotNone(rejected["candidate_score_sha256"])
            self.assertIsNone(rejected["route_proposal_sha256"])
            self.assertEqual(rejected["pending_source"], "existing")
            self.assertEqual(len(scorer.calls), 6)
            route_pointer = json.loads(
                store.get(bounded.prefix + "/route-proposals/current.json")
            )
            blocked = json.loads(store.get(route_pointer["version_key"]))
            self.assertEqual(blocked["reason"], "candidate_score_rejected")
            self.assertNotEqual(
                route_pointer["version_sha256"],
                accepted["route_proposal_sha256"],
            )

    def test_direct_score_client_is_bounded_and_uses_exact_target(self):
        bounded = config("/tmp")
        response = FakeHttpResponse(
            {
                "choices": [
                    {"message": {"content": "expected result"}}
                ],
                "usage": {"prompt_tokens": 7, "completion_tokens": 3},
            }
        )
        opener = RecordingOpener(response)
        clock = iter((1.0, 1.012))
        client = DirectScoreClient(
            bounded.candidate_score,
            opener=opener,
            monotonic=lambda: next(clock),
        )
        target = bounded.candidate_score.candidate
        case = {
            "case_id": "a" * 64,
            "input": "answer this bounded request",
            "expected": "expected result",
        }

        with mock.patch.dict(
            os.environ,
            {"MILK_TEST_CANDIDATE_API_KEY": "secret-test-key"},
            clear=False,
        ):
            result = client.invoke(
                target_name="candidate",
                target=target,
                case=case,
                job_id="b" * 64,
            )

        self.assertEqual(result.output, "expected result")
        self.assertEqual(result.input_tokens, 7)
        self.assertEqual(result.output_tokens, 3)
        self.assertEqual(result.latency_ms, 12)
        self.assertEqual(opener.request.full_url, target.api_url)
        self.assertEqual(opener.timeout, bounded.candidate_score.timeout_seconds)
        request = json.loads(opener.request.data)
        self.assertEqual(request["model"], target.model)
        self.assertEqual(request["temperature"], 0)
        self.assertEqual(
            request["max_tokens"],
            bounded.candidate_score.max_output_tokens_per_call,
        )
        headers = dict(opener.request.header_items())
        self.assertEqual(headers["Authorization"], "Bearer secret-test-key")
        self.assertIn("candidate", headers["Idempotency-key"])

    def test_direct_score_client_paces_request_starts_from_config(self):
        bounded = config("/tmp")
        score_config = replace(
            bounded.candidate_score,
            minimum_request_interval_ms=4100,
        )
        response = FakeHttpResponse(
            {
                "choices": [{"message": {"content": "expected result"}}],
                "usage": {"prompt_tokens": 7, "completion_tokens": 3},
            }
        )
        sleeps = []
        clock = iter((1.0, 1.01, 1.5, 5.2, 5.21))
        client = DirectScoreClient(
            score_config,
            opener=RecordingOpener(response),
            monotonic=lambda: next(clock),
            sleeper=sleeps.append,
        )
        case = {
            "case_id": "a" * 64,
            "input": "answer this bounded request",
            "expected": "expected result",
        }

        with mock.patch.dict(
            os.environ,
            {
                "MILK_TEST_INCUMBENT_API_KEY": "secret-test-key",
                "MILK_TEST_CANDIDATE_API_KEY": "secret-test-key",
            },
            clear=False,
        ):
            client.invoke(
                target_name="incumbent",
                target=score_config.incumbent,
                case=case,
                job_id="b" * 64,
            )
            client.invoke(
                target_name="candidate",
                target=score_config.candidate,
                case=case,
                job_id="b" * 64,
            )

        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 3.6)

    def test_invalid_provider_output_is_terminal_and_never_retried(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = InvalidTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            second = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertFalse(first["ready"])
            self.assertFalse(second["classifier_provider_called"])
            self.assertEqual(len(teacher.calls), 1)
            self.assertEqual(first["provider_tokens"], 20)
            self.assertEqual(first["accounted_incremental_spend_microusd"], 2)
            self.assertEqual(second["accounted_incremental_spend_microusd"], 0)
            result_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/result.json.zst")
            )
            result = _load_optional_json(store, result_key, compressed=True)
            self.assertEqual(result["failure_stage"], "validation")
            self.assertEqual(result["provider_request_id"], "invalid-response")
            self.assertEqual(result["input_tokens"], 10)
            self.assertEqual(result["output_tokens"], 10)
            self.assertEqual(result["calculated_cost_microusd"], 2)
            self.assertEqual(result["accounted_cost_microusd"], 2)
            self.assertEqual(
                result["failure_message_sha256"],
                hashlib.sha256(
                    b"classification output must contain one label per trace"
                ).hexdigest(),
            )
            self.assertNotIn("output", result)

    def test_provider_contract_failure_uses_conservative_accounting(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            bounded = config(root)
            teacher = ContractErrorTeacher()

            report = run_once(bounded, store=store, teacher=teacher, now=NOW)

            result_key = next(
                key
                for key in store.list(bounded.prefix + "/jobs/classify")
                if key.endswith("/result.json.zst")
            )
            result = _load_optional_json(store, result_key, compressed=True)
            self.assertEqual(result["failure_stage"], "provider_contract")
            self.assertIsNone(result["provider_request_id"])
            self.assertEqual(
                result["input_tokens"], bounded.teacher.max_input_tokens_per_call
            )
            self.assertEqual(
                result["output_tokens"], bounded.teacher.max_output_tokens_per_call
            )
            self.assertIsNone(result["calculated_cost_microusd"])
            self.assertEqual(
                result["accounted_cost_microusd"],
                bounded.teacher.reserved_cost(),
            )
            self.assertEqual(
                report["accounted_incremental_spend_microusd"],
                bounded.teacher.reserved_cost(),
            )
            self.assertEqual(
                result["failure_message_sha256"],
                hashlib.sha256(
                    b"teacher response content must be a JSON string"
                ).hexdigest(),
            )
            self.assertNotIn("output", result)

    def test_eval_answer_leak_is_repaired_before_strict_validation(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = LeakingEvalTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertTrue(first["ready"])
            self.assertIsNotNone(first["eval_sha256"])
            self.assertEqual(
                [call[0] for call in teacher.calls], ["classify", "generate_eval"]
            )
            result_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/generate-eval")
                if key.endswith("/result.json.zst")
            )
            result = _load_optional_json(store, result_key, compressed=True)
            self.assertEqual(result["outcome"], "succeeded")
            self.assertEqual(len(result["output"]), 5)
            repaired = next(
                case for case in result["output"] if case["expected"] == "four"
            )
            self.assertNotIn("four", repaired["input"].casefold())

    def test_numeric_operand_is_not_rewritten_as_answer_leak(self):
        plan = [
            {
                "suite": "representative",
                "source_trace_sha256": "a" * 64,
                "oracle": "reference",
                "operation": "answer",
                "selection_reason": "representative_mix",
            }
        ]

        value = _eval_cases_from_pairs(
            {"pairs": [["What is 0 plus 1?", "1"]]}, plan
        )

        self.assertEqual(value["cases"][0]["input"], "What is 0 plus 1?")

    def test_failed_teacher_source_retries_after_teacher_change(self):
        failures = (
            (HttpErrorTeacher, ["classify"]),
            (InvalidTeacher, ["classify"]),
        )
        for teacher_type, failed_tasks in failures:
            with self.subTest(
                teacher=teacher_type.__name__
            ), tempfile.TemporaryDirectory() as root:
                store = seed(root)
                failed_teacher = teacher_type()
                failed = run_once(
                    config(root), store=store, teacher=failed_teacher, now=NOW
                )
                pointer = json.loads(
                    store.get(config(root).prefix + "/pending-source/current.json")
                )
                pending = json.loads(store.get(pointer["version_key"]))

                same_identity_teacher = FakeTeacher()
                replay = run_once(
                    config(root),
                    store=store,
                    teacher=same_identity_teacher,
                    now=NOW,
                )

                changed = _config_dict(root)
                changed["teacher"]["model"] = "glm-test-retry"
                retry_config = RunConfig.parse(changed)
                retry_teacher = FakeTeacher()
                retried = run_once(
                    retry_config,
                    store=store,
                    teacher=retry_teacher,
                    now=NOW,
                )
                completed = run_once(
                    retry_config,
                    store=store,
                    teacher=retry_teacher,
                    now=NOW,
                )

                self.assertEqual(
                    [call[0] for call in failed_teacher.calls], failed_tasks
                )
                self.assertEqual(len(pending["traces"]), 100)
                self.assertEqual(
                    replay["source_manifest_sha256"],
                    failed["source_manifest_sha256"],
                )
                self.assertEqual(same_identity_teacher.calls, [])
                self.assertEqual(
                    [call[0] for call in retry_teacher.calls[:2]],
                    ["classify", "generate_eval"],
                )
                self.assertEqual(
                    retried["source_manifest_sha256"],
                    failed["source_manifest_sha256"],
                )
                self.assertIsNotNone(retried["eval_sha256"])
                self.assertEqual(retried["pending_source"], "existing")
                self.assertIsNotNone(completed["eval_validation_sha256"])
                self.assertIsNotNone(completed["candidate_score_sha256"])
                self.assertEqual(completed["pending_source"], "advanced")

    def test_explicit_http_error_is_terminal_not_transport_ambiguous(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = HttpErrorTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            second = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertFalse(first["ready"])
            self.assertFalse(second["classifier_provider_called"])
            self.assertEqual(len(teacher.calls), 1)
            result_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/result.json.zst")
            )
            result = _load_optional_json(store, result_key, compressed=True)
            self.assertEqual(result["outcome"], "definitive_http_error")
            self.assertEqual(result["http_status"], 429)
            self.assertEqual(result["provider_request_id"], "provider-http-429")

    def test_real_zstd_responses_trace_is_parsed_without_rejecting_unknown_items(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=99)
            add_responses_zstd_trace(root)
            report = run_once(config(root), store=store, teacher=FakeTeacher(), now=NOW)

            self.assertEqual(report["trace_count"], 100)
            pointer = json.loads(store.get(config(root).prefix + "/summaries/current.json"))
            summary = json.loads(store.get(pointer["version_key"]))
            quality = summary["structural"]["quality"]
            sampled = summary["structural"]["sampled_distributions"]
            numeric = summary["structural"]["sampled_numeric"]
            self.assertEqual(quality["parse_basis_points"], 10_000)
            self.assertEqual(quality["unknown_items"], 1)
            self.assertEqual(sampled["stream"]["suppressed_lt_10"], 1)
            self.assertEqual(sampled["has_tools"]["suppressed_lt_10"], 1)
            self.assertEqual(sampled["structured_output"]["suppressed_lt_10"], 1)
            self.assertEqual(numeric["tool_call_count"]["max"], 1)
            self.assertEqual(summary["structural"]["numeric"]["reasoning_tokens"]["max"], 3)

    def test_stats_and_retained_trace_counts_must_match(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            traffic_key = store.list(config(root).prefix + "/traffic")[0]
            store._path(traffic_key).unlink()
            missing_teacher = FakeTeacher()
            with self.assertRaisesRegex(ValueError, "retained trace count"):
                run_once(
                    config(root), store=store, teacher=missing_teacher, now=NOW
                )
            self.assertEqual(missing_teacher.calls, [])

        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=99)
            identifier, value = trace(999)
            key = f"{config(root).prefix}/traffic/{HOUR:%Y/%m/%d/%H}/{identifier}.json"
            store.create(key, canonical_json(value), "application/json")
            extra_teacher = FakeTeacher()
            with self.assertRaisesRegex(ValueError, "retained trace count"):
                run_once(
                    config(root), store=store, teacher=extra_teacher, now=NOW
                )
            self.assertEqual(extra_teacher.calls, [])

    def test_stats_and_traces_must_pair_within_each_window(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=100, hour=HOUR)
            second_hour = HOUR + dt.timedelta(hours=1)
            seed(root, count=100, hour=second_hour, index_offset=200)
            for key in store.list(config(root).prefix + "/traffic"):
                if f"/traffic/{HOUR:%Y/%m/%d/%H}/" in key:
                    store._path(key).unlink()
            for key in store.list(config(root).prefix + "/stats"):
                if f"/stats/{second_hour:%Y/%m/%d/%H}/" in key:
                    store._path(key).unlink()
            teacher = FakeTeacher()

            with self.assertRaisesRegex(ValueError, "retained trace count"):
                run_once(
                    config(root),
                    store=store,
                    teacher=teacher,
                    now=NOW + dt.timedelta(hours=2),
                )
            self.assertEqual(teacher.calls, [])

    def test_content_duplicates_cannot_satisfy_readiness(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            traffic_keys = store.list(config(root).prefix + "/traffic")
            first = json.loads(store.get(traffic_keys[0]))
            for key in traffic_keys:
                raw, etag = store.get_versioned(key)
                value = json.loads(raw)
                value["request"] = first["request"]
                value["response"] = first["response"]
                value["catalog"]["request_bytes"] = first["request"]["byte_len"]
                value["catalog"]["response_bytes"] = first["response"]["byte_len"]
                self.assertTrue(
                    store.replace(key, canonical_json(value), etag, "application/json")
                )
            teacher = FakeTeacher()

            report = run_once(
                config(root), store=store, teacher=teacher, now=NOW
            )

            pointer = json.loads(
                store.get(config(root).prefix + "/summaries/current.json")
            )
            summary = json.loads(store.get(pointer["version_key"]))
            self.assertFalse(report["ready"])
            self.assertEqual(
                summary["structural"]["quality"]["duplicate_traces"], 99
            )
            self.assertEqual(teacher.calls, [])

    def test_source_paginates_past_one_thousand_traces(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=1000)
            value = _config_dict(root)
            value["source"]["max_traces"] = 1500
            bounded = RunConfig.parse(value)

            report = run_once(
                bounded, store=store, teacher=FakeTeacher(), now=NOW
            )

            self.assertEqual(report["trace_count"], 1000)
            self.assertTrue(report["ready"])

    def test_watermark_accumulates_until_mechanics_threshold_then_advances(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=30)
            teacher = FakeTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            seed(
                root,
                count=30,
                hour=HOUR + dt.timedelta(hours=1),
                index_offset=100,
            )
            second = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(hours=1),
            )
            seed(
                root,
                count=40,
                hour=HOUR + dt.timedelta(hours=2),
                index_offset=200,
            )
            third = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(hours=2),
            )
            fourth = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(hours=3),
            )
            fifth = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(hours=4),
            )

            self.assertNotEqual(first["source_manifest_sha256"], second["source_manifest_sha256"])
            self.assertEqual(first["trace_count"], 30)
            self.assertEqual(second["trace_count"], 60)
            self.assertEqual(third["trace_count"], 100)
            self.assertEqual(fourth["trace_count"], 100)
            self.assertEqual(fifth["trace_count"], 0)
            self.assertEqual(first["watermark"], "created")
            self.assertEqual(second["watermark"], "advanced")
            self.assertEqual(third["watermark"], "advanced")
            self.assertEqual(
                [call[0] for call in teacher.calls[:3]],
                ["classify", "generate_eval", "validate_eval"],
            )

    def test_pending_source_crosses_max_windows_without_deadlock(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(
                root,
                count=40,
                hour=HOUR - dt.timedelta(hours=2),
                index_offset=0,
            )
            seed(
                root,
                count=40,
                hour=HOUR - dt.timedelta(hours=1),
                index_offset=100,
            )
            seed(root, count=40, hour=HOUR, index_offset=200)
            value = _config_dict(root)
            value["source"]["max_windows"] = 2
            bounded = RunConfig.parse(value)
            teacher = FakeTeacher()

            first = run_once(bounded, store=store, teacher=teacher, now=NOW)
            second = run_once(bounded, store=store, teacher=teacher, now=NOW)

            self.assertEqual(first["trace_count"], 80)
            self.assertEqual(first["provider_calls"], 0)
            self.assertEqual(second["trace_count"], 120)
            self.assertTrue(second["ready"])
            self.assertEqual([call[0] for call in teacher.calls], ["classify", "generate_eval"])

    def test_750_session_projection_fits_bounded_two_call_contract(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=750)
            value = _config_dict(root)
            value["source"]["teacher_trace_bytes"] = 64
            value["source"]["classifier_sample_sessions"] = 750
            value["teacher"]["max_input_tokens_per_call"] = 100_000
            value["teacher"]["max_output_tokens_per_call"] = 16_384
            value["teacher"]["max_total_tokens_per_run"] = 232_768
            value["eval"]["representative_cases"] = 24
            value["eval"]["tail_cases"] = 8
            value["eval"]["max_source_traces"] = 128
            bounded = RunConfig.parse(value)
            teacher = FakeTeacher()

            report = run_once(
                bounded, store=store, teacher=teacher, now=NOW
            )

            self.assertTrue(report["ready"])
            self.assertEqual(report["provider_calls"], 2)
            self.assertIsNotNone(report["eval_sha256"])

    def test_750_classifier_rows_fit_for_arbitrary_utf8_and_json_escapes(self):
        bounded = config("/tmp")
        prefix = _request_text_prefix(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": ('"\\\n雪' * 100),
                    }
                ]
            },
            "chat_completions",
            64,
        )
        self.assertNotIn('"', prefix)
        self.assertNotIn("\\", prefix)
        self.assertTrue(all(ord(character) >= 32 for character in prefix))
        payload = {
            "schema_version": "milk.classification-input.v1",
            "taxonomy_version": "milk.semantic-taxonomy.v1",
            "taxonomy": [
                list(OPERATION_VALUES),
                list(DOMAIN_VALUES),
                list(CAPABILITY_VALUES),
                list(ORACLE_VALUES),
            ],
            "source_manifest_sha256": "f" * 64,
            "rows": [],
        }
        for expected in (100, 750):
            with self.subTest(expected=expected):
                payload["rows"] = [
                    ["c", prefix, 7, 15] for _ in range(expected)
                ]
                request = _teacher_request_body(
                    bounded.teacher, CLASSIFIER_INSTRUCTIONS, payload, "classify"
                )
                request_value = json.loads(request)
                self.assertIn(
                    "including trivial or synthetic requests",
                    request_value["messages"][0]["content"],
                )
                response_format = request_value["response_format"]
                self.assertEqual(response_format["type"], "json_schema")
                self.assertTrue(response_format["json_schema"]["strict"])
                schema = response_format["json_schema"]["schema"]
                labels = schema["properties"]["labels"]
                self.assertEqual(labels["minItems"], expected)
                self.assertEqual(labels["maxItems"], expected)
                self.assertEqual(labels["items"]["minItems"], 6)
                self.assertEqual(labels["items"]["maxItems"], 6)
                self.assertEqual(
                    [
                        item["type"]
                        for item in labels["items"]["prefixItems"]
                    ],
                    [
                        "integer",
                        "integer",
                        "integer",
                        "integer",
                        "string",
                        "boolean",
                    ],
                )
                self.assertEqual(schema["required"], ["labels"])
                self.assertFalse(schema["additionalProperties"])
                self.assertLessEqual(
                    _conservative_token_bound(request),
                    bounded.teacher.max_input_tokens_per_call,
                )

    def test_eval_request_uses_strict_exact_case_schema(self):
        payload = {
            "case_plan": [
                [
                    "representative" if index < 24 else "tail",
                    f"{index:064x}",
                    "reference",
                    "answer",
                    "representative_mix" if index < 24 else "long_context",
                ]
                for index in range(32)
            ],
        }
        request = _teacher_request_body(
            config("/tmp").teacher,
            EVAL_INSTRUCTIONS,
            payload,
            "generate_eval",
        )
        response_format = json.loads(request)["response_format"]
        self.assertEqual(response_format["type"], "json_schema")
        self.assertTrue(response_format["json_schema"]["strict"])
        schema = response_format["json_schema"]["schema"]
        pairs = schema["properties"]["pairs"]
        self.assertEqual(pairs["minItems"], 32)
        self.assertEqual(pairs["maxItems"], 32)
        item = pairs["items"]
        self.assertEqual(item["minItems"], 2)
        self.assertEqual(item["maxItems"], 2)
        self.assertEqual(
            [field["type"] for field in item["prefixItems"]],
            ["string", "string"],
        )
        self.assertEqual(schema["required"], ["pairs"])
        self.assertFalse(schema["additionalProperties"])
        self.assertIn("same order", EVAL_INSTRUCTIONS)
        self.assertIn("casefold(expected)", EVAL_INSTRUCTIONS)
        self.assertIn(
            "while a non-numeric casefold(expected) is a substring of casefold(input)",
            EVAL_INSTRUCTIONS,
        )
        self.assertIn("not a copy of the source request or response", EVAL_INSTRUCTIONS)
        self.assertNotIn("24 in the deployed configuration", EVAL_INSTRUCTIONS)
        self.assertNotIn("8 in the deployed configuration", EVAL_INSTRUCTIONS)

    def test_all_answer_plain_text_eval_plan_is_deterministic(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            bounded = config(root)
            traces = [
                replace(
                    _parse_trace(store, bounded, key),
                    request_raw=b"x" * 182,
                )
                for key in store.list(bounded.prefix + "/traffic")
            ]
            labels = [
                {
                    "trace_sha256": item.object_sha256,
                    "operation": "answer",
                    "domain": "general",
                    "capabilities": ["knowledge"],
                    "expected_oracle": "reference",
                    "language": "en",
                    "abstain": False,
                }
                for item in traces
            ]

            first = _eval_case_plan(traces, labels, 24, 8)
            second = _eval_case_plan(list(reversed(traces)), labels, 24, 8)

            self.assertEqual(first, second)
            self.assertEqual(len(first), 32)
            self.assertEqual(
                [item["suite"] for item in first],
                ["representative"] * 24 + ["tail"] * 8,
            )
            self.assertTrue(all(item["operation"] == "answer" for item in first))
            self.assertTrue(all(item["oracle"] == "reference" for item in first))
            self.assertTrue(
                all(
                    item["selection_reason"] == "long_context"
                    for item in first[24:]
                )
            )
            self.assertEqual(
                len({item["source_trace_sha256"] for item in first[24:]}),
                8,
            )

            pairs = [
                [f"new task {index}", f"expected result {index}"]
                for index in range(32)
            ]
            checked = _validate_eval_output(
                _eval_cases_from_pairs({"pairs": pairs}, first),
                traces,
                labels,
                24,
                8,
                bounded.source.teacher_trace_bytes,
            )
            self.assertEqual(len(checked), 32)

            duplicate_pairs = [list(pair) for pair in pairs]
            duplicate_pairs[1] = list(duplicate_pairs[0])
            self.assertNotEqual(
                first[0]["source_trace_sha256"],
                first[1]["source_trace_sha256"],
            )
            duplicate_checked = _validate_eval_output(
                _eval_cases_from_pairs({"pairs": duplicate_pairs}, first),
                traces,
                labels,
                24,
                8,
                bounded.source.teacher_trace_bytes,
            )
            self.assertEqual(
                len({case["case_id"] for case in duplicate_checked}), 32
            )

            traces_by_sha = {item.object_sha256: item for item in traces}
            source = traces_by_sha[first[0]["source_trace_sha256"]]
            for copied_input in (
                _request_text_prefix(
                    source.request,
                    source.endpoint,
                    bounded.source.teacher_trace_bytes,
                ),
                _response_text_prefix(
                    source.response,
                    source.endpoint,
                    bounded.source.teacher_trace_bytes,
                ),
            ):
                copied_pairs = [list(pair) for pair in pairs]
                copied_pairs[0][0] = copied_input.swapcase()
                with self.assertRaisesRegex(ValueError, "copies its source trace"):
                    _validate_eval_output(
                        _eval_cases_from_pairs({"pairs": copied_pairs}, first),
                        traces,
                        labels,
                        24,
                        8,
                        bounded.source.teacher_trace_bytes,
                    )

    def test_eval_plan_reserves_live_tail_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=32)
            bounded = config(root)
            traces = sorted(
                (
                    _parse_trace(store, bounded, key)
                    for key in store.list(bounded.prefix + "/traffic")
                ),
                key=lambda trace: (
                    hashlib.sha256(
                        ("milk.eval-source.v1\0" + trace.object_sha256).encode()
                    ).hexdigest(),
                    trace.object_sha256,
                ),
            )
            traces = [
                replace(trace, request_raw=b"x" * (1000 if index < 4 else 10))
                for index, trace in enumerate(traces)
            ]
            operations = (
                ["answer"] * 26
                + ["classify"] * 2
                + ["code", "extract", "generate", "summarize"]
            )
            labels = [
                {
                    "trace_sha256": trace.object_sha256,
                    "operation": operation,
                    "expected_oracle": "reference",
                    "abstain": False,
                }
                for trace, operation in zip(traces, operations)
            ]

            plan = _eval_case_plan(traces, labels, 24, 8)
            representative = plan[:24]
            tail = plan[24:]

            self.assertEqual(
                [item["operation"] for item in representative].count("answer"),
                22,
            )
            self.assertEqual(
                [item["operation"] for item in representative].count("classify"),
                2,
            )
            self.assertEqual(
                [item["source_trace_sha256"] for item in tail],
                [trace.object_sha256 for trace in traces[:4] + traces[28:]],
            )
            self.assertEqual(
                [item["operation"] for item in tail[:4]],
                ["answer"] * 4,
            )
            self.assertEqual(
                [item["selection_reason"] for item in tail],
                ["long_context"] * 4 + ["rare"] * 4,
            )

    def test_classifier_supplements_missing_tail_rows_without_biasing_summary(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=40)
            value = _config_dict(root)
            value["source"]["classifier_sample_sessions"] = 32
            value["eval"]["representative_cases"] = 24
            value["eval"]["tail_cases"] = 8
            value["eval"]["max_source_traces"] = 32
            bounded = RunConfig.parse(value)
            keys = store.list(bounded.prefix + "/traffic")
            ranked = sorted(
                keys,
                key=lambda key: hashlib.sha256(
                    (
                        "milk.semantic-sample.v1\0"
                        + json.loads(store.get(key))["catalog"][
                            "sampling_unit_hmac_sha256"
                        ]
                    ).encode()
                ).hexdigest(),
            )
            semantic_keys = ranked[:32]
            tool_keys = semantic_keys[:2]
            long_keys = ranked[32:40]

            for index, key in enumerate(tool_keys + long_keys):
                raw, etag = store.get_versioned(key)
                retained = json.loads(raw)
                request_value = {
                    "model": "baseline-model",
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                f"long operation {index} " + "x" * 1000
                                if key in long_keys
                                else f"tool operation {index}"
                            ),
                        }
                    ],
                    "stream": False,
                }
                if key in tool_keys:
                    request_value["tools"] = [
                        {
                            "type": "function",
                            "function": {
                                "name": "lookup",
                                "parameters": {"type": "object"},
                            },
                        }
                    ]
                request_raw, request = body(request_value)
                retained["request"] = request
                retained["catalog"]["request_bytes"] = len(request_raw)
                self.assertTrue(
                    store.replace(key, canonical_json(retained), etag)
                )
            tool_sha256s = {
                _parse_trace(store, bounded, key).object_sha256 for key in tool_keys
            }

            teacher = TailSupplementTeacher()
            report = run_once(
                bounded,
                store=store,
                teacher=teacher,
                now=NOW,
            )
            validated_report = run_once(
                bounded,
                store=store,
                teacher=teacher,
                now=NOW,
            )
            summary_pointer = json.loads(
                store.get(bounded.prefix + "/summaries/current.json")
            )
            summary = json.loads(store.get(summary_pointer["version_key"]))
            readiness_pointer = json.loads(
                store.get(bounded.prefix + "/readiness/current.json")
            )
            readiness = json.loads(
                store.get(readiness_pointer["version_key"])
            )
            validation_pointer = json.loads(
                store.get(
                    bounded.prefix
                    + "/eval-validations/mechanics-v1/current.json"
                )
            )
            validation = json.loads(
                store.get(validation_pointer["version_key"])
            )
            plan = teacher.payloads["generate_eval"]["case_plan"]

            self.assertTrue(report["ready"])
            self.assertIsNotNone(validated_report["eval_validation_sha256"])
            self.assertEqual(
                teacher.payloads["classify"]["semantic_row_count"], 32
            )
            self.assertEqual(len(teacher.payloads["classify"]["rows"]), 40)
            self.assertEqual(
                sum(bool(row[2] & 4) for row in teacher.payloads["classify"]["rows"]),
                2,
            )
            self.assertEqual(summary["semantic"]["classified"], 32)
            self.assertEqual(summary["semantic"]["operation"]["answer"], 32)
            self.assertEqual(
                readiness["represented_classes"],
                [{"operation": "answer", "sessions": 32}],
            )
            self.assertEqual(readiness["eval_eligible_cases"], 38)
            self.assertEqual(
                readiness["unsupported_eval_categories"],
                {"tool_use": 2},
            )
            self.assertTrue(validation["accepted"])
            self.assertEqual(validation["output"]["accepted_cases"], 32)
            self.assertEqual([row[0] for row in plan].count("representative"), 24)
            self.assertEqual([row[0] for row in plan].count("tail"), 8)
            self.assertEqual(
                sorted(row[4] for row in plan if row[0] == "tail"),
                ["long_context"] * 8,
            )
            self.assertEqual(len({row[1] for row in plan}), 32)
            self.assertTrue(tool_sha256s.isdisjoint(row[1] for row in plan))
            self.assertEqual(
                len(teacher.payloads["generate_eval"]["traces"]), 32
            )
            self.assertTrue(
                all(
                    not row[4] & 8
                    for row in teacher.payloads["generate_eval"]["traces"]
                )
            )

    def test_production_classifier_allows_eight_tail_supplements(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=1)
            base_config = config(root)
            base = _parse_trace(
                store,
                base_config,
                store.list(base_config.prefix + "/traffic")[0],
            )
            traces = []
            for index in range(758):
                traces.append(
                    replace(
                        base,
                        object_sha256=hashlib.sha256(
                            f"production-trace-{index}".encode()
                        ).hexdigest(),
                        session_hmac=hashlib.sha256(
                            f"production-session-{index}".encode()
                        ).hexdigest(),
                        request_raw=b"x",
                    )
                )
            value = _config_dict(root)
            value["profile"] = "production"
            value["source"]["max_traces"] = 3000
            value["source"]["classifier_sample_sessions"] = 750
            value["eval"]["representative_cases"] = 24
            value["eval"]["tail_cases"] = 8
            value["eval"]["max_source_traces"] = 32
            bounded = RunConfig.parse(value)

            semantic, classifier, long_context_threshold = (
                _classification_sources(traces, bounded)
            )

            self.assertEqual(len(semantic), 750)
            self.assertEqual(len(classifier), 758)
            self.assertEqual(long_context_threshold, 1)

    def test_full_source_long_context_evidence_survives_bounded_plan(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=1)
            base_config = config(root)
            base = _parse_trace(
                store,
                base_config,
                store.list(base_config.prefix + "/traffic")[0],
            )
            traces = [
                replace(
                    base,
                    object_sha256=hashlib.sha256(
                        f"long-context-trace-{index}".encode()
                    ).hexdigest(),
                    session_hmac=hashlib.sha256(
                        f"long-context-session-{index}".encode()
                    ).hexdigest(),
                    request_raw=b"x" * 10,
                )
                for index in range(100)
            ]
            ranked = sorted(
                traces,
                key=lambda trace: hashlib.sha256(
                    ("milk.semantic-sample.v1\0" + trace.session_hmac).encode()
                ).hexdigest(),
            )
            long_sha256s = {
                trace.object_sha256 for trace in ranked[:2] + ranked[32:40]
            }
            traces = [
                replace(trace, request_raw=b"x" * 1000)
                if trace.object_sha256 in long_sha256s
                else trace
                for trace in traces
            ]
            value = _config_dict(root)
            value["source"]["classifier_sample_sessions"] = 32
            value["eval"]["representative_cases"] = 24
            value["eval"]["tail_cases"] = 8
            value["eval"]["max_source_traces"] = 32
            bounded = RunConfig.parse(value)
            semantic, classifier, long_context_threshold = (
                _classification_sources(traces, bounded)
            )
            labels = [
                {
                    "trace_sha256": trace.object_sha256,
                    "operation": "answer",
                    "domain": "general",
                    "capabilities": ["knowledge"],
                    "expected_oracle": "reference",
                    "language": "en",
                    "abstain": False,
                }
                for trace in classifier
            ]
            semantic_sha256s = {trace.object_sha256 for trace in semantic}
            semantic_labels = [
                label
                for label in labels
                if label["trace_sha256"] in semantic_sha256s
            ]

            plan = _eval_case_plan(
                classifier,
                labels,
                24,
                8,
                semantic_labels,
                long_context_threshold,
            )
            planned_sha256s = {
                item["source_trace_sha256"] for item in plan
            }
            planned = [
                trace
                for trace in classifier
                if trace.object_sha256 in planned_sha256s
            ]
            checked = _validate_eval_output(
                _eval_cases_from_pairs(
                    {
                        "pairs": [
                            [f"new long task {index}", f"long result {index}"]
                            for index in range(32)
                        ]
                    },
                    plan,
                ),
                planned,
                labels,
                24,
                8,
                bounded.source.eval_trace_bytes,
                semantic_labels,
                long_context_threshold,
            )

            self.assertEqual(len(semantic), 32)
            self.assertEqual(len(classifier), 40)
            self.assertEqual(long_context_threshold, 1000)
            self.assertEqual(len(planned), 32)
            self.assertEqual(len(checked), 32)
            self.assertTrue(
                all(
                    item["selection_reason"] == "long_context"
                    for item in plan[24:]
                )
            )

    def test_full_pool_plan_preserves_semantic_operation_quotas(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=1)
            bounded = config(root)
            base = _parse_trace(
                store,
                bounded,
                store.list(bounded.prefix + "/traffic")[0],
            )
            traces = []
            labels = []
            semantic_labels = []
            for group, count, operation, tool_use in (
                ("answer", 24, "answer", True),
                ("classify", 8, "classify", False),
                ("supplement", 8, "classify", True),
            ):
                for index in range(count):
                    sha256 = hashlib.sha256(
                        f"quota-{group}-{index}".encode()
                    ).hexdigest()
                    request = {
                        "model": "baseline-model",
                        "messages": [{"role": "user", "content": "question"}],
                        "stream": False,
                    }
                    if tool_use:
                        request["tools"] = [
                            {
                                "type": "function",
                                "function": {
                                    "name": "lookup",
                                    "parameters": {"type": "object"},
                                },
                            }
                        ]
                    traces.append(
                        replace(
                            base,
                            object_sha256=sha256,
                            session_hmac=sha256,
                            request=request,
                            request_raw=canonical_json(request),
                        )
                    )
                    label = {
                        "trace_sha256": sha256,
                        "operation": operation,
                        "expected_oracle": "schema",
                        "abstain": False,
                    }
                    labels.append(label)
                    if group != "supplement":
                        semantic_labels.append(label)

            plan = _eval_case_plan(
                traces,
                labels,
                24,
                8,
                semantic_labels,
                10**9,
            )
            representative_operations = [
                item["operation"] for item in plan[:24]
            ]

            self.assertEqual(representative_operations.count("answer"), 18)
            self.assertEqual(representative_operations.count("classify"), 6)
            self.assertEqual(len(plan), 32)
            self.assertEqual({item["oracle"] for item in plan}, {"reference"})
            self.assertEqual(
                len({item["source_trace_sha256"] for item in plan}), 32
            )

    def test_production_eval_filter_reports_unsupported_categories(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=1)
            bounded = config(root)
            source = _parse_trace(
                store,
                bounded,
                store.list(bounded.prefix + "/traffic")[0],
            )
            tool_trace = replace(
                source,
                object_sha256="b" * 64,
                request={
                    "messages": [{"role": "user", "content": "use a tool"}],
                    "tools": [{"type": "function", "name": "lookup"}],
                },
            )
            non_reference = replace(source, object_sha256="c" * 64)
            abstained = replace(source, object_sha256="d" * 64)
            labels = [
                {
                    "trace_sha256": trace_value.object_sha256,
                    "operation": "answer",
                    "domain": "general",
                    "capabilities": ["knowledge"],
                    "expected_oracle": oracle,
                    "language": "en",
                    "abstain": is_abstained,
                }
                for trace_value, oracle, is_abstained in (
                    (source, "reference", False),
                    (tool_trace, "reference", False),
                    (non_reference, "human", False),
                    (abstained, "reference", True),
                )
            ]

            eligible_traces, eligible_labels, unsupported = (
                _eligible_eval_inputs(
                    replace(bounded, profile="production"),
                    [source, tool_trace, non_reference, abstained],
                    labels,
                )
            )

            self.assertEqual(
                [item.object_sha256 for item in eligible_traces],
                [source.object_sha256],
            )
            self.assertEqual(
                [item["trace_sha256"] for item in eligible_labels],
                [source.object_sha256],
            )
            self.assertEqual(
                unsupported,
                {"abstained": 1, "non_reference_oracle": 1, "tool_use": 1},
            )

            mechanics_traces, mechanics_labels, mechanics_unsupported = (
                _eligible_eval_inputs(
                    bounded,
                    [source, tool_trace, non_reference, abstained],
                    labels,
                )
            )
            self.assertEqual(
                [item.object_sha256 for item in mechanics_traces],
                [source.object_sha256, non_reference.object_sha256],
            )
            self.assertEqual(
                [item["trace_sha256"] for item in mechanics_labels],
                [source.object_sha256, non_reference.object_sha256],
            )
            self.assertEqual(
                mechanics_unsupported,
                {"abstained": 1, "tool_use": 1},
            )

    def test_representative_quotas_are_deterministic_and_capacity_capped(self):
        labels = [
            {"operation": operation, "abstain": False}
            for operation, count in (("answer", 6), ("summarize", 3), ("code", 1))
            for unused_index in range(count)
        ]

        quotas = _eval_operation_quotas(labels, 8)[2]

        self.assertEqual(quotas, {"answer": 4, "code": 1, "summarize": 3})

    def test_representative_quotas_redistribute_live_capacity_shortfall(self):
        labels = [
            {"operation": operation, "abstain": False}
            for operation, count in (
                ("answer", 26),
                ("classify", 2),
                ("code", 1),
                ("extract", 1),
                ("generate", 1),
                ("summarize", 1),
            )
            for unused_index in range(count)
        ]

        label_counts, required_operations, quotas = _eval_operation_quotas(
            labels, 24
        )

        self.assertEqual(required_operations, {"answer", "classify"})
        self.assertEqual(quotas, {"answer": 22, "classify": 2})
        self.assertEqual(sum(quotas.values()), 24)
        self.assertTrue(
            all(quota <= label_counts[operation] for operation, quota in quotas.items())
        )

    def test_eval_pairs_must_match_plan_count_exactly(self):
        plan = [
            {
                "suite": "representative",
                "source_trace_sha256": f"{index:064x}",
                "oracle": "reference",
                "operation": "answer",
                "selection_reason": "representative_mix",
            }
            for index in range(32)
        ]
        with self.assertRaisesRegex(ValueError, "pair count"):
            _eval_cases_from_pairs(
                {"pairs": [[f"task {index}", f"result {index}"] for index in range(31)]},
                plan,
            )
        cases = _eval_cases_from_pairs(
            {"pairs": [[f"task {index}", f"result {index}"] for index in range(32)]},
            plan,
        )
        self.assertEqual(len(cases["cases"]), 32)
        self.assertEqual(cases["cases"][0]["source_trace_sha256"], "0" * 64)

    def test_eval_pair_leak_repair_preserves_count_and_plan_bindings(self):
        plan = [
            {
                "suite": "representative",
                "source_trace_sha256": f"{index:064x}",
                "oracle": "reference",
                "operation": "answer",
                "selection_reason": "representative_mix",
            }
            for index in range(32)
        ]
        pairs = [[f"task {index}", f"result {index}"] for index in range(32)]
        pairs[0] = ["same", "same"]
        pairs[1] = ["aabb", "ab"]
        pairs[2] = ["straße", "SS"]

        cases = _eval_cases_from_pairs({"pairs": pairs}, plan)["cases"]

        self.assertEqual(len(cases), 32)
        self.assertEqual(
            [case["source_trace_sha256"] for case in cases],
            [item["source_trace_sha256"] for item in plan],
        )
        self.assertEqual(
            [case["expected"] for case in cases], [pair[1] for pair in pairs]
        )
        for case in cases[:3]:
            self.assertNotIn(
                case["expected"].casefold(), case["input"].casefold()
            )
        self.assertEqual(cases[3]["input"], pairs[3][0])

    def test_sampling_skips_unparseable_sessions_before_applying_its_cap(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=201)
            traffic_keys = store.list(config(root).prefix + "/traffic")
            victim_key = min(
                traffic_keys,
                key=lambda key: hashlib.sha256(
                    (
                        "milk.semantic-sample.v1\0"
                        + json.loads(store.get(key))["catalog"][
                            "sampling_unit_hmac_sha256"
                        ]
                    ).encode()
                ).hexdigest(),
            )
            raw, etag = store.get_versioned(victim_key)
            value = json.loads(raw)
            invalid_response, encoded_response = raw_body(
                b"not-json", "application/json"
            )
            value["response"] = encoded_response
            value["catalog"]["response_bytes"] = len(invalid_response)
            self.assertTrue(
                store.replace(
                    victim_key, canonical_json(value), etag, "application/json"
                )
            )

            report = run_once(
                config(root), store=store, teacher=FakeTeacher(), now=NOW
            )

            pointer = json.loads(
                store.get(config(root).prefix + "/summaries/current.json")
            )
            summary = json.loads(store.get(pointer["version_key"]))
            self.assertTrue(report["ready"])
            self.assertEqual(summary["semantic"]["classified"], 100)

    def test_stop_new_threshold_creates_no_paid_claim(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            value = _config_dict(root)
            value["budget"]["starting_spend_microusd"] = 20_000_000
            bounded = RunConfig.parse(value)
            teacher = FakeTeacher()

            report = run_once(bounded, store=store, teacher=teacher, now=NOW)

            self.assertFalse(report["ready"])
            self.assertEqual(report["watermark"], "created")
            self.assertEqual(teacher.calls, [])
            self.assertFalse(store.list(bounded.prefix + "/jobs/classify"))

    def test_accepted_object_claim_timeout_blocks_provider_replay(self):
        with tempfile.TemporaryDirectory() as root:
            seed(root)
            store = ClaimAcceptedThenTimeoutStore(root)
            teacher = FakeTeacher()
            with self.assertRaisesRegex(TimeoutError, "accepted"):
                run_once(config(root), store=store, teacher=teacher, now=NOW)
            self.assertEqual(teacher.calls, [])

            replay = run_once(config(root), store=store, teacher=teacher, now=NOW)
            self.assertFalse(replay["ready"])
            self.assertEqual(teacher.calls, [])
            self.assertEqual(replay["accounted_incremental_spend_microusd"], 0)
            reconciled = run_once(
                config(root),
                store=store,
                teacher=teacher,
                now=NOW + dt.timedelta(seconds=91),
            )
            self.assertEqual(teacher.calls, [])
            self.assertEqual(reconciled["watermark"], "existing")
            result_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/result.json.zst")
            )
            result = _load_optional_json(store, result_key, compressed=True)
            self.assertEqual(result["outcome"], "ambiguous")
            self.assertEqual(result["error_class"], "stale_unresolved_claim")

    def test_concurrent_runner_observes_claim_without_finalizing_active_call(self):
        with tempfile.TemporaryDirectory() as root:
            seed(root)
            first_teacher = BlockingTeacher()
            first_result = []
            first_error = []

            def invoke_first():
                try:
                    first_result.append(
                        run_once(
                            config(root),
                            store=LocalEvidenceStore(root),
                            teacher=first_teacher,
                            now=NOW,
                        )
                    )
                except Exception as error:  # pragma: no cover - asserted below
                    first_error.append(error)

            thread = threading.Thread(target=invoke_first)
            thread.start()
            self.assertTrue(first_teacher.started.wait(5))
            seed(root, count=1, hour=HOUR, index_offset=300)
            second_teacher = FakeTeacher()
            second = run_once(
                config(root),
                store=LocalEvidenceStore(root),
                teacher=second_teacher,
                now=NOW,
            )
            self.assertFalse(second["classifier_provider_called"])
            self.assertEqual(second["run_lock"], "busy")
            self.assertEqual(second_teacher.calls, [])

            first_teacher.release.set()
            thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(first_error, [])
            self.assertEqual(first_result[0]["provider_calls"], 2)
            classifier_keys = LocalEvidenceStore(root).list(
                config(root).prefix + "/jobs/classify"
            )
            self.assertEqual(
                sum(key.endswith("/claim.json") for key in classifier_keys), 1
            )
            self.assertEqual(
                sum(key.endswith("/result.json.zst") for key in classifier_keys),
                1,
            )

    def test_expired_lease_blocks_provider_result_write_and_next_call(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            clock = MutableClock(NOW)
            teacher = LeaseExpiringTeacher(clock)

            with self.assertRaisesRegex(RuntimeError, "lease is no longer active"):
                run_once(
                    config(root),
                    store=store,
                    teacher=teacher,
                    now=NOW,
                    clock=clock,
                )

            self.assertEqual([call[0] for call in teacher.calls], ["classify"])
            classifier_keys = store.list(config(root).prefix + "/jobs/classify")
            self.assertEqual(
                sum(key.endswith("/claim.json") for key in classifier_keys), 1
            )
            self.assertEqual(
                sum(key.endswith("/result.json.zst") for key in classifier_keys),
                0,
            )

    def test_accounting_enumerates_more_than_one_s3_page(self):
        with tempfile.TemporaryDirectory() as root:
            bounded = config(root)
            prefix = bounded.prefix + "/jobs/classify"
            keys = [
                f"{prefix}/{index:064x}/result.json.zst"
                for index in range(1001)
            ]
            store = PagedKeyStore(keys)

            def load_result(unused_store, key, *, compressed=False):
                self.assertTrue(compressed)
                job_id = key.rsplit("/", 2)[-2]
                return {
                    "schema_version": "milk.teacher-job-result.v1",
                    "job_id": job_id,
                    "accounted_cost_microusd": 1,
                }

            with mock.patch(
                "milk_harness.run_once._load_optional_json",
                side_effect=load_result,
            ):
                meter = _RunMeter(store, bounded)

            self.assertEqual(meter.accounted_spend, 1001)
            self.assertEqual(meter.incremental_spend, 0)

    def test_accounting_history_fails_closed_at_its_lifetime_bound(self):
        with tempfile.TemporaryDirectory() as root:
            bounded = config(root)
            prefix = bounded.prefix + "/jobs/classify"
            keys = [
                f"{prefix}/{index:064x}/result.json.zst"
                for index in range(1001)
            ]
            store = PagedKeyStore(keys)

            with mock.patch(
                "milk_harness.run_once.MAX_ACCOUNTING_JOB_OBJECTS", 1000
            ):
                with self.assertRaisesRegex(ValueError, "history exceeds"):
                    _RunMeter(store, bounded)

    def test_abstained_labels_never_reach_eval_generation(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = AbstainingTeacher()

            report = run_once(
                config(root), store=store, teacher=teacher, now=NOW
            )

            self.assertTrue(report["ready"])
            self.assertIsNotNone(report["eval_sha256"])
            self.assertEqual(
                teacher.eval_payload["schema_version"],
                "milk.eval-generation-input.v5",
            )
            self.assertEqual(
                teacher.eval_payload["answer_leak_repair"],
                "milk.eval-answer-leak-repair.v2",
            )
            self.assertEqual(
                teacher.eval_payload["eval_oracle_policy"],
                "generated-reference-from-text-source-v1",
            )
            self.assertTrue(
                all(item[3] == "answer" for item in teacher.eval_payload["case_plan"])
            )
            self.assertNotIn("labels", teacher.eval_payload)
            self.assertNotIn("operation_counts", teacher.eval_payload)

    def test_zero_basis_points_emits_baseline_only_proposal(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            value = _config_dict(root)
            value["route_proposal"]["candidate_basis_points"] = 0
            bounded = RunConfig.parse(value)

            report = run_once(
                bounded, store=store, teacher=FakeTeacher(), now=NOW
            )
            report = run_once(
                bounded, store=store, teacher=FakeTeacher(), now=NOW
            )

            self.assertIsNotNone(report["route_proposal_sha256"])
            pointer = json.loads(
                store.get(bounded.prefix + "/route-proposals/current.json")
            )
            proposal = json.loads(store.get(pointer["version_key"]))
            self.assertEqual(proposal["candidate_basis_points"], 0)
            self.assertNotIn("activation_authorized", proposal)
            self.assertNotIn("signature", proposal)

    def test_utf8_byte_count_is_the_conservative_preflight_bound(self):
        raw = '{"text":"雪λ"}'.encode()
        self.assertEqual(_conservative_token_bound(raw), len(raw))

    def test_production_profile_cannot_qualify_from_mechanics_scale(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, profile="production")
            report = run_once(config(root, profile="production"), store=store, teacher=FakeTeacher(), now=NOW)
            self.assertFalse(report["ready"])
            self.assertFalse(report["statistically_qualified"])
            self.assertIsNone(report["eval_sha256"])

    def test_scope_cannot_switch_between_mechanics_and_production(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root, count=30)
            run_once(config(root), store=store, teacher=FakeTeacher(), now=NOW)
            with self.assertRaisesRegex(ValueError, "existing evidence object differs"):
                run_once(
                    config(root, profile="production"),
                    store=store,
                    teacher=FakeTeacher(),
                    now=NOW,
                )

    def test_budget_ceiling_and_route_base_are_fixed(self):
        with tempfile.TemporaryDirectory() as root:
            value = json.loads(canonical_json(_config_dict(root)))
            value["budget"]["absolute_spend_microusd"] = 25_000_001
            with self.assertRaisesRegex(ValueError, "absolute"):
                RunConfig.parse(value)
            value = _config_dict(root)
            value["route_proposal"]["api_base_url"] = "https://candidate.example.test/"
            with self.assertRaisesRegex(ValueError, "/v1/"):
                RunConfig.parse(value)

            value = _config_dict(root)
            value["eval"]["max_source_traces"] = 4
            with self.assertRaisesRegex(
                ValueError, "cannot cover representative and tail cases"
            ):
                RunConfig.parse(value)

            value = _config_dict(root)
            value["profile"] = "production"
            value["source"]["max_traces"] = 3000
            value["source"]["classifier_sample_sessions"] = 749
            with self.assertRaisesRegex(ValueError, "sample must be 750"):
                RunConfig.parse(value)

    def test_stale_pointer_parent_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            version = {"schema_version": "test.version.v1", "value": 1}
            version_sha256 = hashlib.sha256(canonical_json(version)).hexdigest()
            version_key = f"versions/{version_sha256}.json"
            store.create(version_key, canonical_json(version), "application/json")
            _advance_pointer(store, "current.json", "test", version_sha256, version_key, None)
            with self.assertRaisesRegex(RuntimeError, "parent changed"):
                _advance_pointer(store, "current.json", "test", "f" * 64, "versions/f.json", "e" * 64)


def _config_dict(root):
    value = {
        "schema_version": "milk.harness-run-config.v1",
        "scope_id": SCOPE_ID,
        "profile": "mechanics",
        "store": {"kind": "local", "root": str(Path(root).resolve())},
        "source": {
            "close_delay_seconds": 300,
            "max_windows": 24,
            "max_stats_shards": 999,
            "max_traces": 999,
            "max_trace_object_bytes": 4 * 1024 * 1024,
            "max_total_trace_bytes": 64 * 1024 * 1024,
            "teacher_trace_bytes": 64,
            "eval_trace_bytes": 1024,
            "classifier_sample_sessions": 100,
        },
        "teacher": {
            "api_url": "https://model.example.test/v1/chat/completions",
            "model": "glm-test",
            "reasoning_effort": "low",
            "api_key_env": "MILK_TEST_TEACHER_API_KEY",
            "timeout_seconds": 30,
            "max_calls_per_run": 2,
            "max_input_tokens_per_call": 100_000,
            "max_output_tokens_per_call": 4096,
            "max_total_tokens_per_run": 200_000,
            "input_rate_microusd_per_million": 1000,
            "output_rate_microusd_per_million": 2000,
        },
        "budget": {
            "starting_spend_microusd": 0,
            "stop_new_spend_microusd": 20_000_000,
            "absolute_spend_microusd": 25_000_000,
        },
        "eval": {
            "series_id": "mechanics-v1",
            "representative_cases": 3,
            "tail_cases": 2,
            "max_source_traces": 32,
        },
        "route_proposal": {
            "enabled": True,
            "candidate_id": "glm-mechanics",
            "api_base_url": "https://candidate.example.test/v1/",
            "model": "glm-test",
            "candidate_basis_points": 100,
        },
        "candidate_score": {
            "incumbent": {
                "api_url": "https://incumbent.example.test/v1/chat/completions",
                "model": "glm-test",
                "api_key_env": "MILK_TEST_INCUMBENT_API_KEY",
                "input_rate_microusd_per_million": 1000,
                "output_rate_microusd_per_million": 2000,
            },
            "candidate": {
                "api_url": "https://candidate.example.test/v1/chat/completions",
                "model": "glm-test",
                "api_key_env": "MILK_TEST_CANDIDATE_API_KEY",
                "input_rate_microusd_per_million": 1000,
                "output_rate_microusd_per_million": 2000,
            },
            "held_out_cases": 3,
            "timeout_seconds": 30,
            "minimum_request_interval_ms": 0,
            "max_calls_per_run": 6,
            "max_input_tokens_per_call": 128,
            "max_output_tokens_per_call": 16,
            "max_total_tokens_per_run": 864,
            "case_reference_similarity_basis_points": 9500,
            "minimum_candidate_reference_pass_basis_points": 9000,
            "minimum_reference_pass_delta_basis_points": 0,
            "maximum_candidate_error_basis_points": 0,
            "maximum_candidate_p95_latency_ms": 1000,
        },
    }
    return value


if __name__ == "__main__":
    unittest.main()
