import datetime as dt
import hashlib
import json
import tempfile
import unittest

from milk_harness.evidence import LocalEvidenceStore
from milk_harness.metrics_collection import (
    MAX_PROVIDER_BYTES,
    collect_baseten_terminal_metrics,
)


UTC = dt.timezone.utc
CAMPAIGN = "c" * 64
RUN = "b" * 64
PROJECT = "project_123"
NAME = "milk-job"
CREATED = dt.datetime(2026, 8, 27, 20, 0, 0, tzinfo=UTC)
TERMINAL = CREATED + dt.timedelta(seconds=61)
READY = TERMINAL + dt.timedelta(seconds=61)


class Evidence:
    def __init__(self, store):
        self.store = store
        self.run_id = RUN
        self.prefix = f"campaigns/v1/{CAMPAIGN}/jobs/{RUN}"


def utc(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def execution():
    return {
        "state": "observed_terminal",
        "ordinal": 0,
        "provider": "baseten",
        "project_id": PROJECT,
        "execution_id": "job_1",
        "execution_name": NAME,
        "gpu_type": "H100",
        "gpu_count": 1,
        "availability_model": "dedicated",
        "node_count": 1,
        "status": "TRAINING_JOB_COMPLETED",
        "created_at": utc(CREATED),
        "updated_at": utc(TERMINAL),
    }


def job(job_id="job_1"):
    return {
        "id": job_id,
        "created_at": utc(CREATED),
        "current_status": "TRAINING_JOB_COMPLETED",
        "instance_type": {
            "id": "h100_instance",
            "name": "H100",
            "memory_limit_mib": 65_536,
            "millicpu_limit": 8_000,
            "gpu_count": 1,
            "gpu_type": "H100",
            "gpu_memory_limit_mib": 81_920,
        },
        "updated_at": utc(READY),
        "training_project_id": PROJECT,
        "training_project": {"id": PROJECT, "name": "milk"},
        "error_message": "private provider failure text",
        "node_count": 1,
        "name": NAME,
        "checkpoint_sync_status": None,
        "priority": 0,
        "availability_model": "dedicated",
        "user": {"email": "private@example.test"},
    }


def envelope(job_id="job_1", cpu_value=1.25):
    sample_at = TERMINAL + dt.timedelta(seconds=30)
    sample = {"value": cpu_value, "timestamp": utc(sample_at)}
    node_sample = {"value": 2, "timestamp": utc(sample_at)}
    empty_storage = {"usage_bytes": [], "utilization": []}
    return {
        "gpu_memory_usage_bytes": {"0": [node_sample]},
        "gpu_utilization": {"0": [sample]},
        "cpu_usage": [sample],
        "cpu_memory_usage_bytes": [node_sample],
        "ephemeral_storage": empty_storage,
        "training_job": job(job_id),
        "cache": None,
        "per_node_metrics": [
            {
                "node_id": "private-node-name",
                "metrics": {
                    "gpu_memory_usage_bytes": {},
                    "gpu_utilization": {},
                    "cpu_usage": [sample],
                    "cpu_memory_usage_bytes": [],
                    "ephemeral_storage": empty_storage,
                },
            }
        ],
    }


def raw(value):
    return (json.dumps(value, separators=(",", ":")) + "\n").encode()


class MetricCollectionTests(unittest.TestCase):
    def evidence(self, root):
        return Evidence(LocalEvidenceStore(root))

    def collect(
        self,
        evidence,
        request,
        current,
        continue_collection=lambda: True,
        executions=None,
    ):
        return collect_baseten_terminal_metrics(
            evidence=evidence,
            request=request,
            project_id=PROJECT,
            executions=[execution()] if executions is None else executions,
            observed_at=utc(TERMINAL),
            now=lambda: current,
            continue_collection=continue_collection,
        )

    def test_success_normalizes_numeric_series_and_replay_uses_zero_gets(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = []

            def request(method, path, query=None):
                calls.append((method, path, query))
                return raw(envelope())

            pending = self.collect(evidence, request, TERMINAL)
            self.assertEqual(pending["state"], "not_before")
            self.assertEqual(calls, [])
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 1)
            self.assertEqual(
                calls[0][2],
                {
                    "start_epoch_millis": int(CREATED.timestamp() * 1000),
                    "end_epoch_millis": int((TERMINAL + dt.timedelta(seconds=60)).timestamp() * 1000),
                    "step_seconds": 60,
                },
            )
            stored_summary = json.loads(
                evidence.store.get(
                    f"{evidence.prefix}/metric-collections/00/summary.json"
                )
            )
            sample = stored_summary["series"]["cpu_usage"][0]
            self.assertEqual(sample["value_decimal"], "1.25")
            self.assertEqual(
                sample["timestamp_epoch_millis"],
                int((TERMINAL + dt.timedelta(seconds=30)).timestamp() * 1000),
            )
            node = stored_summary["series"]["per_node_metrics"][0]
            self.assertEqual(node["node_id_bytes"], len(b"private-node-name"))
            all_stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"private-node-name", all_stored)
            self.assertNotIn(b"private provider failure text", all_stored)
            self.assertNotIn(b"private@example.test", all_stored)
            replay = self.collect(evidence, request, READY + dt.timedelta(days=1))
            self.assertEqual(replay, summary)
            self.assertEqual(len(calls), 1)

    def test_malformed_identity_and_free_text_are_digest_only_failures(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            values = [
                envelope(job_id="wrong_job"),
                envelope(cpu_value="secret metric label"),
                envelope(),
            ]
            calls = 0

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                value = values[calls]
                calls += 1
                return raw(value)

            self.collect(evidence, request, TERMINAL)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 3)
            self.assertEqual(calls, 3)
            all_stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"wrong_job", all_stored)
            self.assertNotIn(b"secret metric label", all_stored)

    def test_crash_and_failures_burn_three_slots_then_seal_incomplete(self):
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

            self.collect(evidence, crash, TERMINAL)
            with self.assertRaises(Crash):
                self.collect(evidence, crash, READY)

            def fail(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                raise TimeoutError("provider text must not persist")

            summary = self.collect(evidence, fail, READY)
            self.assertFalse(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 3)
            self.assertEqual(summary["pessimistic_provider_bytes"], 3 * MAX_PROVIDER_BYTES)
            self.assertEqual(calls, 3)
            all_stored = b"".join(
                evidence.store.get(key)
                for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"provider text", all_stored)
            replay = self.collect(evidence, fail, READY + dt.timedelta(days=1))
            self.assertEqual(replay, summary)
            self.assertEqual(calls, 3)

    def test_deadline_yields_without_creating_an_attempt(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            calls = 0

            def request(unused_method, unused_path, query=None):
                nonlocal calls
                del unused_method, unused_path, query
                calls += 1
                return raw(envelope())

            self.collect(evidence, request, TERMINAL)
            pending = self.collect(
                evidence,
                request,
                READY,
                continue_collection=lambda: False,
            )
            self.assertEqual(pending["state"], "pass_deadline")
            self.assertEqual(pending["attempts_used"], 0)
            self.assertEqual(calls, 0)
            self.assertEqual(
                evidence.store.list(
                    f"{evidence.prefix}/metric-collections/00/attempts"
                ),
                [],
            )
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(calls, 1)

    def test_duplicate_create_is_explicitly_unavailable_without_a_get(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)

            def match(job_id, digest):
                return {
                    "observation_sha256": digest,
                    "provider": "baseten",
                    "project_id": PROJECT,
                    "execution_id": job_id,
                    "execution_name": NAME,
                    "gpu_type": "H100",
                    "gpu_count": 1,
                    "availability_model": "dedicated",
                    "node_count": 1,
                    "status": "TRAINING_JOB_COMPLETED",
                    "created_at": utc(CREATED),
                    "updated_at": utc(TERMINAL),
                    "elapsed_seconds": 61,
                    "billed_minutes": 2,
                    "accounted_microusd": 216_660,
                }

            duplicate = {
                "state": "duplicate_create",
                "ordinal": 0,
                "execution_run_id": "d" * 64,
                "execution_name": NAME,
                "duplicate_set_sha256": "e" * 64,
                "max_wall_seconds": 600,
                "matches": [match("job_1", "1" * 64), match("job_2", "2" * 64)],
                "billed_minutes": 4,
                "accounted_microusd": 433_320,
            }
            summary = self.collect(
                evidence,
                lambda *args, **kwargs: self.fail("duplicate create issued a GET"),
                READY,
                executions=[duplicate],
            )
            self.assertFalse(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 0)
            entry = summary["executions"][0]
            self.assertEqual(entry["duplicate_set_sha256"], "e" * 64)
            self.assertEqual(
                entry["duplicate_execution_id_sha256s"],
                sorted(
                    hashlib.sha256(item.encode()).hexdigest()
                    for item in ("job_1", "job_2")
                ),
            )
            unavailable = json.loads(
                evidence.store.get(
                    f"{evidence.prefix}/metric-collections/00/summary.json"
                )
            )
            self.assertEqual(
                unavailable["missing_reason"], "provider_execution_unavailable"
            )
            stored = b"".join(
                evidence.store.get(key) for key in evidence.store.list(evidence.prefix)
            )
            self.assertNotIn(b"job_1", stored)
            self.assertNotIn(b"job_2", stored)

    def test_materially_future_job_update_is_retried_without_changing_the_plan(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            future = envelope()
            future["training_job"]["updated_at"] = utc(
                READY + dt.timedelta(minutes=10)
            )
            values = [future, envelope()]

            def request(unused_method, unused_path, query=None):
                del unused_method, unused_path, query
                return raw(values.pop(0))

            self.collect(evidence, request, TERMINAL)
            summary = self.collect(evidence, request, READY)
            self.assertTrue(summary["source_complete"])
            self.assertEqual(summary["attempts_used"], 2)
            self.assertEqual(
                summary["executions"][0]["end_epoch_millis"],
                int((TERMINAL + dt.timedelta(seconds=60)).timestamp() * 1000),
            )


if __name__ == "__main__":
    unittest.main()
