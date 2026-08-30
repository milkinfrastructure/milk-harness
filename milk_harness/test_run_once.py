from __future__ import annotations

import base64
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
    DOMAIN_VALUES,
    EVAL_INSTRUCTIONS,
    MAX_DECODED_TRACE_BYTES,
    OPERATION_VALUES,
    ORACLE_VALUES,
    RunConfig,
    TeacherResponse,
    _RunMeter,
    _advance_pointer,
    _conservative_token_bound,
    _decompress_trace,
    _load_optional_json,
    _parse_trace,
    _request_text_prefix,
    _stream_response,
    _teacher_request_body,
    _utc,
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
            traces = payload["traces"]
            operations = {label[0]: label[1] for label in payload["labels"]}
            longest = max(traces, key=lambda value: value[6])
            cases = []
            for suite, count in (
                ("representative", payload["representative_cases"]),
                ("tail", payload["tail_cases"]),
            ):
                for index in range(count):
                    source = (
                        traces[index % len(traces)]
                        if suite == "representative"
                        else longest
                    )
                    cases.append(
                        {
                            "suite": suite,
                            "source_trace_sha256": source[0],
                            "input": f"{suite} input {index}",
                            "expected": f"{suite} expected {index}",
                            "oracle": "reference",
                            "operation": operations[source[0]],
                            "selection_reason": "representative_mix"
                            if suite == "representative"
                            else "long_context",
                        }
                    )
            value = {"cases": cases}
        else:
            raise AssertionError(task)
        return TeacherResponse(
            value,
            "fake-" + job_id[:16],
            max(1, _conservative_token_bound(canonical_json(payload))),
            16,
        )


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


class UnsupportedTailTeacher(FakeTeacher):
    def complete(self, **kwargs):
        response = super().complete(**kwargs)
        if kwargs["task"] == "generate_eval":
            for case in response.value["cases"]:
                if case["suite"] == "tail":
                    case["selection_reason"] = "tool_use"
        return response


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

    def test_mechanics_pipeline_is_content_addressed_and_replay_calls_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = FakeTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertTrue(first["ready"])
            self.assertFalse(first["statistically_qualified"])
            self.assertEqual(first["trace_count"], 100)
            self.assertEqual(first["provider_calls"], 2)
            self.assertIsNotNone(first["eval_sha256"])
            self.assertIsNotNone(first["route_proposal_sha256"])
            self.assertFalse(first["route_activation_attempted"])
            self.assertEqual([call[0] for call in teacher.calls], ["classify", "generate_eval"])
            claim_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/classify")
                if key.endswith("/claim.json")
            )
            identity = json.loads(store.get(claim_key))["identity"]
            self.assertEqual(
                identity["schema_version"], "milk.teacher-job-identity.v2"
            )
            self.assertEqual(len(identity["response_format_sha256"]), 64)
            eval_claim_key = next(
                key
                for key in store.list(config(root).prefix + "/jobs/generate-eval")
                if key.endswith("/claim.json")
            )
            eval_identity = json.loads(store.get(eval_claim_key))["identity"]
            self.assertEqual(
                eval_identity["schema_version"], "milk.teacher-job-identity.v2"
            )
            self.assertNotEqual(
                identity["response_format_sha256"],
                eval_identity["response_format_sha256"],
            )
            legacy_identity = dict(identity)
            legacy_identity["schema_version"] = "milk.teacher-job-identity.v1"
            del legacy_identity["response_format_sha256"]
            legacy_job_id = hashlib.sha256(
                canonical_json(legacy_identity)
            ).hexdigest()
            self.assertNotEqual(first["classifier_job_id"], legacy_job_id)

            second = run_once(config(root), store=store, teacher=teacher, now=NOW + dt.timedelta(days=1))

            self.assertEqual(len(teacher.calls), 2)
            self.assertFalse(second["classifier_provider_called"])
            self.assertFalse(second["eval_provider_called"])
            self.assertEqual(second["summary_sha256"], first["summary_sha256"])
            self.assertEqual(second["readiness_sha256"], first["readiness_sha256"])
            self.assertEqual(second["eval_sha256"], first["eval_sha256"])
            self.assertEqual(second["route_proposal_sha256"], first["route_proposal_sha256"])

            route_pointer = json.loads(
                store.get(f"{config(root).prefix}/route-proposals/current.json")
            )
            proposal = json.loads(store.get(route_pointer["version_key"]))
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

    def test_invalid_provider_output_is_terminal_and_never_retried(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = InvalidTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            second = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertFalse(first["ready"])
            self.assertFalse(second["classifier_provider_called"])
            self.assertEqual(len(teacher.calls), 1)

    def test_eval_tail_requires_source_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            teacher = UnsupportedTailTeacher()
            first = run_once(config(root), store=store, teacher=teacher, now=NOW)
            second = run_once(config(root), store=store, teacher=teacher, now=NOW)

            self.assertTrue(first["ready"])
            self.assertIsNone(first["eval_sha256"])
            self.assertEqual(
                [call[0] for call in teacher.calls], ["classify", "generate_eval"]
            )
            self.assertFalse(second["eval_provider_called"])

    def test_failed_teacher_source_retries_after_teacher_change(self):
        failures = (
            (HttpErrorTeacher, ["classify"]),
            (InvalidTeacher, ["classify"]),
            (UnsupportedTailTeacher, ["classify", "generate_eval"]),
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
                    [call[0] for call in retry_teacher.calls],
                    ["classify", "generate_eval"],
                )
                self.assertEqual(
                    retried["source_manifest_sha256"],
                    failed["source_manifest_sha256"],
                )
                self.assertIsNotNone(retried["eval_sha256"])
                self.assertEqual(retried["pending_source"], "advanced")

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

            self.assertNotEqual(first["source_manifest_sha256"], second["source_manifest_sha256"])
            self.assertEqual(first["trace_count"], 30)
            self.assertEqual(second["trace_count"], 60)
            self.assertEqual(third["trace_count"], 100)
            self.assertEqual(fourth["trace_count"], 0)
            self.assertEqual(first["watermark"], "created")
            self.assertEqual(second["watermark"], "advanced")
            self.assertEqual(third["watermark"], "advanced")
            self.assertEqual([call[0] for call in teacher.calls], ["classify", "generate_eval"])

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
            "representative_cases": 24,
            "tail_cases": 8,
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
        cases = schema["properties"]["cases"]
        self.assertEqual(cases["minItems"], 32)
        self.assertEqual(cases["maxItems"], 32)
        item = cases["items"]
        self.assertEqual(
            item["required"],
            [
                "suite",
                "source_trace_sha256",
                "input",
                "expected",
                "oracle",
                "operation",
                "selection_reason",
            ],
        )
        self.assertFalse(item["additionalProperties"])
        self.assertEqual(
            item["properties"]["suite"]["enum"],
            ["representative", "tail"],
        )
        self.assertEqual(
            item["properties"]["oracle"]["enum"], list(ORACLE_VALUES)
        )
        self.assertEqual(
            item["properties"]["operation"]["enum"], list(OPERATION_VALUES)
        )

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
            self.assertGreater(replay["accounted_incremental_spend_microusd"], 0)
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
            self.assertEqual(teacher.eval_payload["operation_counts"], {"answer": 90})
            self.assertTrue(
                all(label[1] == "answer" for label in teacher.eval_payload["labels"])
            )

    def test_zero_basis_points_emits_baseline_only_proposal(self):
        with tempfile.TemporaryDirectory() as root:
            store = seed(root)
            value = _config_dict(root)
            value["route_proposal"]["candidate_basis_points"] = 0
            bounded = RunConfig.parse(value)

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
    }
    return value


if __name__ == "__main__":
    unittest.main()
