import datetime as dt
import gzip
import json
import tempfile
import unittest

from milk_harness.evidence import LocalEvidenceStore
from milk_harness.jobs import JobEvidence
from milk_harness.log_collection import (
    MAX_PROVIDER_BYTES,
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
                return response([record(at, sequence=index) for index in range(count)])

            self.collect(evidence, request, FIRST)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(len(windows), 3)
            root_window, left, right = windows
            self.assertEqual(left[0], root_window[0])
            self.assertEqual(right[1], root_window[1])
            self.assertEqual(left[1] + 1, right[0])
            self.assertEqual(summary["records_hashed"], 2)
            archives = [
                key
                for key in evidence.store.list(evidence.prefix)
                if key.endswith("/index.json")
            ]
            self.assertEqual(len(archives), 2)

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
                    return response([record(at, sequence=index) for index in range(1000)])
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
            before = calls
            self.collect(evidence, request, READY + dt.timedelta(days=1))
            self.assertEqual(calls, before)

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
