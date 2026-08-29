import datetime as dt
import gzip
import json
import tempfile
import unittest

from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.jobs import JobEvidence
from milk_harness.log_collection import (
    MAX_PROVIDER_BYTES,
    OOM_CLASSIFIER_SHA256,
    collect_baseten_terminal_logs,
)


UTC = dt.timezone.utc
CAMPAIGN = "c" * 64
RUN = "a" * 64
PROJECT = "project_123"
CREATED = dt.datetime(2026, 8, 27, 20, 0, 0, tzinfo=UTC)
FIRST = CREATED + dt.timedelta(seconds=1)
READY = FIRST + dt.timedelta(seconds=61)


def utc(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def execution(created=CREATED, updated=FIRST):
    return {
        "ordinal": 0,
        "execution_id": "job_1",
        "status": "TRAINING_JOB_COMPLETED",
        "created_at": utc(created),
        "updated_at": utc(updated),
    }


def response(records):
    return (json.dumps({"logs": records}, separators=(",", ":")) + "\n").encode()


def record(at, message="prompt=must-not-persist", sequence=0):
    return {
        "timestamp": utc(at),
        "message": message,
        "replica": "worker-0",
        "request_id": f"request-{sequence}",
        "level": "INFO",
    }


class CollectorTests(unittest.TestCase):
    def evidence(self, root):
        return JobEvidence(LocalEvidenceStore(root), CAMPAIGN, RUN)

    def collect(
        self,
        evidence,
        request,
        current,
        executions=None,
        continue_collection=lambda: True,
    ):
        return collect_baseten_terminal_logs(
            evidence=evidence,
            request=request,
            project_id=PROJECT,
            executions=executions or [execution()],
            observed_at=utc(FIRST),
            now=lambda: current,
            continue_collection=continue_collection,
        )

    def test_999_records_freezes_plan_redacts_and_replays_without_get(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = []

            def request(method, path, query=None):
                calls.append((method, path, query))
                start = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response([record(start, sequence=index) for index in range(999)])

            pending = self.collect(evidence, request, FIRST)
            self.assertEqual(pending["state"], "not_before")
            self.assertEqual(calls, [])
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 1)
            self.assertEqual(summary["records_hashed"], 999)
            self.assertEqual(summary["oom_classifier_sha256"], OOM_CLASSIFIER_SHA256)
            self.assertEqual(summary["oom_matched_record_count"], 0)
            self.assertTrue(summary["oom_source_complete"])
            self.assertEqual(len(calls), 1)
            query = calls[0][2]
            self.assertEqual(query["start_epoch_millis"], int(CREATED.timestamp() * 1000))
            self.assertEqual(query["end_epoch_millis"], int(READY.timestamp() * 1000) - 1000)
            replay = self.collect(evidence, request, READY + dt.timedelta(days=1))
            self.assertEqual(replay, summary)
            self.assertEqual(len(calls), 1)
            stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"must-not-persist", stored)
            chunk = next(
                key
                for key in evidence.store.list(evidence.prefix)
                if key.endswith(".jsonl.gz")
            )
            self.assertNotIn(b"must-not-persist", gzip.decompress(evidence.store.get(chunk)))

    def test_oom_matches_are_content_free_and_replay_without_get(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0
            messages = [
                "worker ready",
                "torch.OutOfMemoryError: CUDA out of memory",
                "kernel marked replica OOMKilled",
                "runtime reported CUDA OOM",
                "runtime reported GPU OOM",
                "worker was Killed (OOM)",
            ]

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path
                calls += 1
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response(
                    [
                        record(at, message=message, sequence=index)
                        for index, message in enumerate(messages)
                    ]
                )

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["oom_matched_record_count"], 5)
            self.assertTrue(summary["oom_source_complete"])
            self.assertEqual(
                summary["leaf_decisions"][0]["oom_matched_record_count"],
                5,
            )
            replay = self.collect(evidence, request, READY + dt.timedelta(days=1))
            self.assertEqual(replay, summary)
            self.assertEqual(calls, 1)
            stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(messages[1].encode(), stored)
            self.assertNotIn(messages[2].encode(), stored)

    def test_oom_classifier_ignores_non_oom_substrings(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)

            def request(unused_method, unused_path, query=None):
                del unused_method, unused_path
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response(
                    [
                        record(at, message="worker has GPU memory headroom", sequence=0),
                        record(at, message="oomph library loaded", sequence=1),
                        record(at, message="CUDA memory healthy", sequence=2),
                    ]
                )

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["oom_matched_record_count"], 0)
            self.assertTrue(summary["oom_source_complete"])

    def test_malformed_oom_response_makes_classifier_evidence_incomplete(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                if calls == 1:
                    return b'{"logs":[{"message":"CUDA OOM"}'
                return response([])

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_without_known_response"], 0)
            self.assertEqual(summary["attempts_without_oom_classification"], 1)
            self.assertEqual(summary["oom_matched_record_count"], 0)
            self.assertFalse(summary["oom_source_complete"])
            self.assertEqual(calls, 2)

            key = f"{evidence.prefix}/log-collections/summary.json"
            raw, etag = evidence.store.get_versioned(key)
            tampered = json.loads(raw)
            tampered["attempts_without_oom_classification"] = 0
            tampered["oom_source_complete"] = True
            self.assertTrue(evidence.store.replace(key, canonical_json(tampered), etag))
            with self.assertRaisesRegex(ValueError, "inconsistent"):
                self.collect(evidence, request, READY + dt.timedelta(days=1))

    def test_saturated_parent_splits_into_nonoverlapping_inclusive_leaves(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            windows = []

            def request(unused_method, unused_path, query=None):
                del unused_method, unused_path
                start = query["start_epoch_millis"]
                end = query["end_epoch_millis"]
                windows.append((start, end))
                at = dt.datetime.fromtimestamp(start / 1000, UTC)
                count = 1000 if len(windows) == 1 else 1
                message = "worker ready" if len(windows) == 1 else "CUDA out of memory"
                return response(
                    [
                        record(at, message=message, sequence=index)
                        for index in range(count)
                    ]
                )

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(len(windows), 3)
            root_window, left, right = windows
            self.assertEqual(left[0], root_window[0])
            self.assertEqual(right[1], root_window[1])
            self.assertEqual(left[1] + 1, right[0])
            self.assertEqual(summary["records_hashed"], 2)
            self.assertEqual(summary["oom_matched_record_count"], 2)
            archives = [
                key
                for key in evidence.store.list(evidence.prefix)
                if key.endswith("/index.json")
            ]
            self.assertEqual(len(archives), 2)

    def test_saturated_parent_oom_survives_empty_child_windows(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path
                calls += 1
                if calls > 1:
                    return response([])
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response(
                    [
                        record(
                            at,
                            message=("CUDA OOM" if index == 0 else "worker ready"),
                            sequence=index,
                        )
                        for index in range(1000)
                    ]
                )

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["records_hashed"], 0)
            self.assertEqual(summary["oom_matched_record_count"], 1)
            self.assertTrue(summary["oom_source_complete"])

    def test_saturated_same_millisecond_seals_explicit_gap_once(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            hot = int(CREATED.timestamp() * 1000)
            calls = 0

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path
                calls += 1
                if query["start_epoch_millis"] <= hot <= query["end_epoch_millis"]:
                    at = dt.datetime.fromtimestamp(hot / 1000, UTC)
                    return response(
                        [
                            record(
                                at,
                                message=(
                                    "CUDA out of memory"
                                    if index == 0
                                    else "worker ready"
                                ),
                                sequence=index,
                            )
                            for index in range(1000)
                        ]
                    )
                return response([])

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertFalse(summary["source_complete"])
            gap = next(
                item
                for item in summary["leaf_decisions"]
                if item["state"] == "gap"
            )
            self.assertEqual(
                gap["gap"],
                {
                    "reason": "provider_limit_same_millisecond",
                    "at_epoch_millis": hot,
                    "observed_records": 1000,
                    "missing_records": None,
                },
            )
            self.assertEqual(summary["records_hashed"], 1000)
            self.assertGreaterEqual(summary["oom_matched_record_count"], 1)
            before = calls
            self.collect(evidence, request, READY + dt.timedelta(days=1))
            self.assertEqual(calls, before)

    def test_tampered_oom_summary_is_rejected_on_replay(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)

            def request(unused_method, unused_path, query=None):
                del unused_method, unused_path, query
                return response([])

            self.collect(evidence, request, FIRST)
            self.collect(evidence, request, READY)
            key = f"{evidence.prefix}/log-collections/summary.json"
            raw, etag = evidence.store.get_versioned(key)
            value = json.loads(raw)
            value["oom_matched_record_count"] = 1
            self.assertTrue(evidence.store.replace(key, canonical_json(value), etag))
            with self.assertRaisesRegex(ValueError, "log summary"):
                self.collect(evidence, request, READY + dt.timedelta(days=1))

    def test_orphan_archive_makes_zero_oom_signal_incomplete(self):
        class Crash(BaseException):
            pass

        class CrashBeforeAttemptResult:
            def __init__(self, inner):
                self.inner = inner
                self.armed = True

            def __getattr__(self, name):
                return getattr(self.inner, name)

            def create(self, key, body, content_type="application/octet-stream"):
                if self.armed and key.endswith("/000-result.json"):
                    self.armed = False
                    raise Crash()
                return self.inner.create(key, body, content_type)

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            crashing_evidence = JobEvidence(
                CrashBeforeAttemptResult(store), CAMPAIGN, RUN
            )

            def oom_response(unused_method, unused_path, query=None):
                del unused_method, unused_path
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response([record(at, message="CUDA OOM")])

            self.collect(crashing_evidence, oom_response, FIRST)
            with self.assertRaises(Crash):
                self.collect(crashing_evidence, oom_response, READY)
            self.assertTrue(
                any(key.endswith("/index.json") for key in store.list(crashing_evidence.prefix))
            )

            evidence = self.evidence(root)

            def empty(unused_method, unused_path, query=None):
                del unused_method, unused_path, query
                return response([])

            summary = self.collect(evidence, empty, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_without_known_response"], 1)
            self.assertEqual(summary["oom_matched_record_count"], 0)
            self.assertFalse(summary["oom_source_complete"])
            self.assertEqual(
                self.collect(evidence, empty, READY + dt.timedelta(days=1)),
                summary,
            )

    def test_crash_burns_slot_and_window_stops_at_three_attempts(self):
        class Crash(BaseException):
            pass

        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0

            def crash(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                raise Crash()

            self.collect(evidence, crash, FIRST)
            with self.assertRaises(Crash):
                self.collect(evidence, crash, READY)

            def fail(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                raise TimeoutError("raw provider text is not evidence")

            summary = self.collect(evidence, fail, READY)
            self.assertFalse(summary["source_complete"])
            self.assertEqual(summary["oom_matched_record_count"], 0)
            self.assertFalse(summary["oom_source_complete"])
            self.assertEqual(summary["attempts_used"], 3)
            self.assertEqual(calls, 3)
            self.assertEqual(
                summary["leaf_decisions"][0]["gap"]["reason"],
                "request_attempt_bound",
            )
            self.assertEqual(
                summary["pessimistic_provider_bytes"], 3 * MAX_PROVIDER_BYTES
            )
            stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"raw provider text", stored)

    def test_global_attempt_bound_is_100_mib_and_never_grows(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0

            def saturated(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path
                calls += 1
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response([record(at, sequence=index) for index in range(1000)])

            self.collect(evidence, saturated, FIRST)
            summary = self.collect(evidence, saturated, READY)
            self.assertFalse(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 100)
            self.assertEqual(calls, 100)
            self.assertEqual(
                summary["pessimistic_provider_bytes"], 100 * MAX_PROVIDER_BYTES
            )
            self.assertLessEqual(
                summary["pessimistic_provider_bytes"], 100 * 1024 * 1024
            )
            self.assertTrue(
                any(
                    item["gap"]["reason"] == "run_attempt_bound"
                    for item in summary["leaf_decisions"]
                    if item["state"] == "gap"
                )
            )
            replay = self.collect(evidence, saturated, READY + dt.timedelta(days=1))
            self.assertEqual(replay, summary)
            self.assertEqual(calls, 100)

    def test_summary_binds_each_provider_execution_and_frozen_interval(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            second = {
                **execution(CREATED + dt.timedelta(seconds=1), FIRST),
                "ordinal": 1,
                "execution_id": "job_2",
            }
            executions = [second, execution()]
            calls = []

            def request(unused_method, path, query=None):
                del unused_method
                calls.append((path, query))
                return response([])

            self.collect(evidence, request, FIRST, executions)
            summary = self.collect(evidence, request, READY, executions)
            self.assertEqual([item["ordinal"] for item in summary["executions"]], [0, 1])
            self.assertEqual(
                [item["execution_id"] for item in summary["executions"]],
                ["job_1", "job_2"],
            )
            self.assertTrue(all(item["source_complete"] for item in summary["executions"]))
            self.assertEqual(len(calls), 2)

    def test_pass_deadline_yields_without_burning_another_attempt(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0
            allowed = iter((True, False))

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path
                calls += 1
                at = dt.datetime.fromtimestamp(
                    query["start_epoch_millis"] / 1000, UTC
                )
                return response([record(at, sequence=index) for index in range(1000)])

            self.collect(evidence, request, FIRST)
            pending = self.collect(
                evidence,
                request,
                READY,
                continue_collection=lambda: next(allowed),
            )
            self.assertEqual(pending["state"], "pass_deadline")
            self.assertEqual(pending["attempts_used"], 1)
            self.assertEqual(calls, 1)
            attempts = evidence.store.list(
                f"{evidence.prefix}/log-collections/attempts"
            )
            self.assertEqual(len(attempts), 2)

            def empty(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                return response([])

            summary = self.collect(evidence, empty, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 3)
            self.assertEqual(calls, 3)


if __name__ == "__main__":
    unittest.main()
