import datetime as dt
import hashlib
import json
import tempfile
import threading
import unittest

from milk_harness.budget import baseten_provider_identity
from milk_harness.evidence import LocalEvidenceStore, canonical_json, create_same
from milk_harness.log_collection import MAX_RUN_ATTEMPTS
from milk_harness.jobs import (
    CREATE_MUTATION_TAIL_SECONDS,
    MAX_DUPLICATE_EXECUTIONS,
    MAX_RECONCILE_GROUPS,
    PREPARATION_TIMEOUT_SECONDS,
    RESERVATION_RATE_MICROUSD_PER_MINUTE,
    BasetenJobs,
    JobEvidence,
    _digest,
    dispatch_outboxes,
    settings_from_arguments,
)
from milk_harness.test_jobs import (
    CAMPAIGN,
    NOW,
    PROJECT,
    SCOPE_PREFIX,
    FakeTransport,
    authorize,
    control_launch,
    entry,
    launch_acceptance,
    provider_get,
    provider_arguments,
    provider_job,
    provider_metrics,
    publish_release,
    workload,
)


class SimulatedProcessCrash(BaseException):
    pass


class StoreProxy:
    def __init__(self, inner):
        self.inner = inner

    def __getattr__(self, name):
        return getattr(self.inner, name)


class CrashOnCreate(StoreProxy):
    def __init__(self, inner, key_fragment, *, commit=True, error=RuntimeError):
        super().__init__(inner)
        self.key_fragment = key_fragment
        self.commit = commit
        self.error = error
        self.armed = True

    def create(self, key, body, content_type="application/octet-stream"):
        if self.armed and self.key_fragment in key:
            self.armed = False
            if self.commit:
                created = self.inner.create(key, body, content_type)
                if not created:
                    return False
            raise self.error("simulated crash at " + self.key_fragment)
        return self.inner.create(key, body, content_type)


class CorruptOnCreate(StoreProxy):
    def __init__(self, inner, key_fragment, mutate):
        super().__init__(inner)
        self.key_fragment = key_fragment
        self.mutate = mutate
        self.armed = True

    def create(self, key, body, content_type="application/octet-stream"):
        if self.armed and self.key_fragment in key:
            self.armed = False
            value = json.loads(body)
            self.mutate(value)
            body = canonical_json(value)
        return self.inner.create(key, body, content_type)


class CrashAfterFinalizationCommit(StoreProxy):
    def __init__(self, inner, run_id):
        super().__init__(inner)
        self.run_id = run_id
        self.armed = True

    def replace(self, key, body, etag, content_type="application/octet-stream"):
        value = json.loads(body)
        finalizing = (
            self.armed
            and key.endswith("/budget-head.json")
            and value.get("pending_settlements") == {}
            and self.run_id not in value.get("outstanding", {})
        )
        replaced = self.inner.replace(key, body, etag, content_type)
        if finalizing and replaced:
            self.armed = False
            raise RuntimeError("simulated crash after finalization CAS")
        return replaced


class FailStopIntent(StoreProxy):
    def __init__(self, inner):
        super().__init__(inner)
        self.enabled = True

    def create(self, key, body, content_type="application/octet-stream"):
        if self.enabled and "/stop-attempts/" in key:
            raise OSError("stop intent store unavailable")
        return self.inner.create(key, body, content_type)


def jobs(store, transport, now=lambda: NOW):
    return BasetenJobs(
        store=store,
        campaign_id=CAMPAIGN,
        project_id=PROJECT,
        transport=transport,
        now=now,
    )


def create_calls(transport):
    return [
        call
        for call in transport.calls
        if call[0] == "POST" and call[1] == f"/training_projects/{PROJECT}/jobs"
    ]


def seed_definition(service, source, entries):
    run_id, definition, unused_entries = service._definition(source, entries)
    del unused_entries
    evidence = JobEvidence(service.store, CAMPAIGN, run_id)
    evidence.json("definition.json", definition)
    create_same(
        service.store,
        f"claims/v1/{source['launch_source']['claim_sha256']}.json",
        canonical_json(launch_acceptance(source, run_id, definition, evidence)),
        "application/json",
    )
    return run_id, definition, evidence


class BasetenJobsCrashTests(unittest.TestCase):
    def test_acceptance_commit_crash_restarts_without_replay(self):
        with tempfile.TemporaryDirectory() as root:
            control = LocalEvidenceStore(root + "/control")
            launch = control_launch(control, "teacher_run", 1)
            base = LocalEvidenceStore(root + "/evidence")
            authorize(base)
            transport = FakeTransport(
                lambda _method, _path, body, _query: provider_job(
                    body["training_job"]["name"], "job_acceptance_restart"
                )
            )
            release = publish_release(base, root + "/release")
            settings = settings_from_arguments(
                provider_arguments(release["release_sha256"]),
                base,
            )
            acceptance_key = f"claims/v1/{launch['claim_sha256']}.json"
            crashing = jobs(
                CrashOnCreate(
                    base,
                    acceptance_key,
                    error=SimulatedProcessCrash,
                ),
                transport,
            )
            with self.assertRaises(SimulatedProcessCrash):
                dispatch_outboxes(
                    crashing,
                    control_store=control,
                    scope_prefix=SCOPE_PREFIX,
                    settings=settings,
                )
            accepted = base.get(acceptance_key)
            self.assertEqual(transport.calls, [])
            self.assertEqual(base.list(f"campaigns/v1/{CAMPAIGN}/jobs"), [])

            recovered = jobs(base, transport)
            first = dispatch_outboxes(
                recovered,
                control_store=control,
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
            )
            self.assertEqual(first["results"][0]["state"], "created")
            self.assertEqual(len(create_calls(transport)), 1)
            second = dispatch_outboxes(
                recovered,
                control_store=control,
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
            )
            self.assertEqual(second["results"][0]["state"], "reconcile_only")
            self.assertEqual(len(create_calls(transport)), 1)
            self.assertEqual(base.get(acceptance_key), accepted)

    def test_concurrent_identical_launch_has_one_create_per_child(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            authorize(store)
            transport = FakeTransport(
                lambda _method, _path, body, _query: provider_job(
                    body["training_job"]["name"], "job_single_owner"
                )
            )
            services = [jobs(store, transport), jobs(store, transport)]
            start = threading.Barrier(3)
            results = []
            failures = []

            def launch(service):
                try:
                    start.wait()
                    results.append(
                        service.launch(workload("a"), [entry("same-child")])
                    )
                except Exception as error:
                    failures.append(error)

            threads = [threading.Thread(target=launch, args=(service,)) for service in services]
            for thread in threads:
                thread.start()
            start.wait()
            for thread in threads:
                thread.join()

            self.assertEqual(failures, [])
            self.assertEqual(
                sorted(result["state"] for result in results),
                ["created", "reconcile_only"],
            )
            self.assertEqual(len(create_calls(transport)), 1)

    def test_crash_before_complete_intent_set_terminalizes_without_provider_calls(self):
        stages = (
            ("/preparation.json", "not_authorized"),
            ("/reservation.json", "not_started"),
            ("/create-intents/00.json", "not_started"),
        )
        for key_fragment, terminal_state in stages:
            with self.subTest(stage=key_fragment), tempfile.TemporaryDirectory() as root:
                base = LocalEvidenceStore(root)
                authorize(base)
                current = [NOW]
                transport = FakeTransport(
                    lambda *_arguments: AssertionError("provider must not be called")
                )
                source = workload("b")
                entries = [entry("crash-child-0"), entry("crash-child-1")]
                crashing = jobs(
                    CrashOnCreate(base, key_fragment),
                    transport,
                    now=lambda: current[0],
                )
                run_id = crashing._definition(source, entries)[0]

                with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                    crashing.launch(source, entries)
                self.assertEqual(transport.calls, [])

                recovered = jobs(base, transport, now=lambda: current[0])
                self.assertEqual(recovered.reconcile(run_id)["state"], "preparing")
                current[0] = NOW + dt.timedelta(seconds=61)
                terminal = recovered.reconcile(run_id)
                self.assertEqual(terminal["state"], terminal_state)
                self.assertEqual(terminal["accounted_microusd"], 0)
                self.assertNotIn(run_id, recovered.budget.status()["outstanding"])
                self.assertEqual(transport.calls, [])

    def test_crash_after_intents_complete_never_replays_create(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]

            def handler(method, path, body, query):
                del body, query
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {"training_jobs": []}
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            source = workload("c")
            entries = [entry("complete-intents-child")]
            crashing = jobs(
                CrashOnCreate(base, "/intents-complete.json"),
                transport,
                now=lambda: current[0],
            )
            run_id = crashing._definition(source, entries)[0]
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                crashing.launch(source, entries)

            recovered = jobs(base, transport, now=lambda: current[0])
            self.assertEqual(
                recovered.launch(source, entries)["state"], "reconcile_only"
            )
            held = recovered.reconcile(run_id)
            self.assertEqual(held["state"], "active_or_ambiguous")
            self.assertEqual(held["executions"][0]["state"], "create_in_flight")
            self.assertEqual(create_calls(transport), [])
            self.assertEqual(transport.calls, [])
            current[0] += dt.timedelta(
                seconds=PREPARATION_TIMEOUT_SECONDS
                + CREATE_MUTATION_TAIL_SECONDS
                + 1
            )
            self.assertEqual(recovered.reconcile(run_id)["state"], "active_or_ambiguous")
            self.assertEqual(
                [(call[0], call[1]) for call in transport.calls],
                [("POST", "/training_jobs/search")],
            )

    def test_reconcile_cannot_search_while_creator_post_is_in_flight(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]
            post_started = threading.Event()
            release_post = threading.Event()
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    post_started.set()
                    if not release_post.wait(5):
                        return TimeoutError("test creator gate timed out")
                    return provider_job(created_name[0], "job_in_flight")
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {
                        "training_jobs": [
                            provider_job(created_name[0], "job_in_flight")[
                                "training_job"
                            ]
                        ]
                    }
                if (method, path) == (
                    "GET",
                    f"/training_projects/{PROJECT}/jobs/job_in_flight",
                ):
                    return provider_get(created_name[0], "job_in_flight")
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            source = workload("9")
            entries = [entry("in-flight-child")]
            crashing = jobs(
                CrashOnCreate(
                    base,
                    "/create-results/00.json",
                    commit=False,
                    error=SimulatedProcessCrash,
                ),
                transport,
                now=lambda: current[0],
            )
            run_id = crashing._definition(source, entries)[0]
            failures = []

            def launch():
                try:
                    crashing.launch(source, entries)
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=launch)
            thread.start()
            self.assertTrue(post_started.wait(5))

            recovered = jobs(base, transport, now=lambda: current[0])
            held = recovered.reconcile(run_id)
            self.assertEqual(held["executions"][0]["state"], "create_in_flight")
            self.assertFalse(
                any(call[:2] == ("POST", "/training_jobs/search") for call in transport.calls)
            )

            release_post.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], SimulatedProcessCrash)
            current[0] += dt.timedelta(
                seconds=PREPARATION_TIMEOUT_SECONDS
                + CREATE_MUTATION_TAIL_SECONDS
                + 1
            )
            observed = recovered.reconcile(run_id)
            self.assertEqual(observed["executions"][0]["execution_id"], "job_in_flight")
            self.assertEqual(len(create_calls(transport)), 1)
            self.assertEqual(
                sum(
                    call[:2] == ("POST", "/training_jobs/search")
                    for call in transport.calls
                ),
                1,
            )

    def test_permanently_unresolved_fanout_aborts_a_created_sibling(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]
            names = {}

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    job_id = f"job_fanout_{len(names)}"
                    names[job_id] = body["training_job"]["name"]
                    return provider_job(names[job_id], job_id)
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {"training_jobs": []}
                if method == "GET" and path.rsplit("/", 1)[-1] in names:
                    job_id = path.rsplit("/", 1)[-1]
                    return provider_get(names[job_id], job_id)
                if method == "POST" and path.endswith("/stop"):
                    job_id = path.split("/")[-2]
                    return provider_job(
                        names[job_id],
                        job_id,
                        "TRAINING_JOB_STOPPED",
                        seconds=1,
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            source = workload("0")
            entries = [entry("fanout-a"), entry("fanout-b")]
            crashing = jobs(
                CrashOnCreate(
                    base,
                    "/create-results/01.json",
                    commit=False,
                    error=SimulatedProcessCrash,
                ),
                transport,
                now=lambda: current[0],
            )
            run_id = crashing._definition(source, entries)[0]
            with self.assertRaises(SimulatedProcessCrash):
                crashing.launch(source, entries)

            recovered = jobs(base, transport, now=lambda: current[0])
            held = recovered.reconcile(run_id)
            self.assertEqual(held["executions"][1]["state"], "create_in_flight")
            current[0] += dt.timedelta(
                seconds=PREPARATION_TIMEOUT_SECONDS
                + CREATE_MUTATION_TAIL_SECONDS
                + 1
            )
            unresolved = recovered.reconcile(run_id)
            self.assertEqual(unresolved["executions"][1]["state"], "unresolved_create")
            aborted = recovered.reconcile(run_id)
            self.assertEqual(aborted["executions"][0]["stop_state"], "stopped")
            self.assertEqual(
                sum(call[0] == "POST" and call[1].endswith("/stop") for call in transport.calls),
                1,
            )

    def test_crash_after_provider_create_is_resolved_without_create_replay(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return provider_job(created_name[0], "job_after_crash")
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {
                        "training_jobs": [
                            provider_job(created_name[0], "job_after_crash")[
                                "training_job"
                            ]
                        ]
                    }
                if (method, path) == (
                    "GET",
                    f"/training_projects/{PROJECT}/jobs/job_after_crash",
                ):
                    return provider_get(created_name[0], "job_after_crash")
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            source = workload("d")
            entries = [entry("provider-created-child")]
            crashing = jobs(
                CrashOnCreate(
                    base,
                    "/create-results/00.json",
                    commit=False,
                    error=SimulatedProcessCrash,
                ),
                transport,
            )
            run_id = crashing._definition(source, entries)[0]
            with self.assertRaises(SimulatedProcessCrash):
                crashing.launch(source, entries)
            self.assertEqual(len(create_calls(transport)), 1)

            recovered = jobs(base, transport)
            self.assertEqual(recovered.reconcile(run_id)["state"], "active_or_ambiguous")
            self.assertEqual(
                recovered.launch(source, entries)["state"], "reconcile_only"
            )
            self.assertEqual(len(create_calls(transport)), 1)

    def test_zero_match_create_remains_ambiguous_without_provider_absence_proof(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]

            def handler(method, path, body, query):
                del body, query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    return TimeoutError("create response lost")
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {"training_jobs": []}
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            service = jobs(base, transport, now=lambda: current[0])
            launched = service.launch(
                workload("7"), [entry("zero-match-child", seconds=120)]
            )
            run_id = launched["run_id"]
            definition = json.loads(
                base.get(
                    f"campaigns/v1/{CAMPAIGN}/jobs/{run_id}/definition.json"
                )
            )
            current[0] = NOW + dt.timedelta(days=1)
            pending = service.reconcile(run_id)
            self.assertEqual(pending["state"], "active_or_ambiguous")
            self.assertEqual(pending["executions"][0]["state"], "unresolved_create")
            self.assertIn(run_id, service.budget.status()["outstanding"])
            self.assertEqual(len(create_calls(transport)), 1)
            self.assertIsNone(service.budget.settlement(run_id))

            later = service.reconcile(run_id)
            self.assertEqual(later["state"], "active_or_ambiguous")
            self.assertIn(run_id, service.budget.status()["outstanding"])
            self.assertEqual(len(create_calls(transport)), 1)

    def test_crash_after_search_intent_never_retries_or_resolves_from_a_later_view(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return TimeoutError("create response lost")
                if (method, path) == ("POST", "/training_jobs/search"):
                    return {
                        "training_jobs": [
                            provider_job(
                                created_name[0],
                                "job_late_view",
                                "TRAINING_JOB_CREATED",
                            )["training_job"]
                        ]
                    }
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            launched = jobs(base, transport).launch(
                workload("6"), [entry("search-crash-child", seconds=120)]
            )
            run_id = launched["run_id"]
            transport.calls.clear()
            crashing = jobs(
                CrashOnCreate(
                    base,
                    "/search-intents/00.json",
                    error=SimulatedProcessCrash,
                ),
                transport,
            )
            with self.assertRaises(SimulatedProcessCrash):
                crashing.reconcile(run_id)
            self.assertEqual(transport.calls, [])

            held = jobs(base, transport).reconcile(run_id)
            self.assertEqual(held["state"], "active_or_ambiguous")
            self.assertEqual(
                held["executions"][0]["state"],
                "search_observation_ambiguous",
            )
            self.assertEqual(transport.calls, [])
            self.assertIn(run_id, jobs(base, transport).budget.status()["outstanding"])

    def test_duplicate_exact_name_stops_both_and_holds_reservation_until_terminal(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]
            created_name = [None]
            searches = [0]
            stopped = set()

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return TimeoutError("create response lost")
                if (method, path) == ("POST", "/training_jobs/search"):
                    searches[0] += 1
                    status = (
                        "TRAINING_JOB_CREATED"
                        if searches[0] == 1
                        else "TRAINING_JOB_STOPPED"
                    )
                    seconds = 0 if searches[0] == 1 else 1
                    job_ids = (
                        ("job_duplicate_a", "job_duplicate_b")
                        if searches[0] == 1
                        else ("job_duplicate_a",)
                    )
                    return {
                        "training_jobs": [
                            provider_job(
                                created_name[0], job_id, status, seconds=seconds
                            )["training_job"]
                            for job_id in job_ids
                        ]
                    }
                if method == "POST" and path.endswith("/stop"):
                    job_id = path.split("/")[-2]
                    stopped.add(job_id)
                    return provider_job(
                        created_name[0],
                        job_id,
                        "TRAINING_JOB_STOPPED",
                        seconds=1,
                    )
                if method == "GET" and path.rsplit("/", 1)[-1] in {
                    "job_duplicate_a",
                    "job_duplicate_b",
                }:
                    job_id = path.rsplit("/", 1)[-1]
                    return provider_get(
                        created_name[0],
                        job_id,
                        (
                            "TRAINING_JOB_STOPPED"
                            if job_id in stopped
                            else "TRAINING_JOB_CREATED"
                        ),
                        seconds=1 if job_id in stopped else 0,
                    )
                if path.endswith("/logs"):
                    return {"logs": []}
                if path.endswith("/metrics"):
                    return provider_metrics(
                        created_name[0],
                        "job_duplicate_a",
                        "TRAINING_JOB_STOPPED",
                        seconds=1,
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            service = jobs(base, transport, now=lambda: current[0])
            launched = service.launch(
                workload("8"), [entry("duplicate-name-child", seconds=120)]
            )
            run_id = launched["run_id"]
            observed = service.reconcile(run_id)
            self.assertEqual(observed["state"], "active_or_ambiguous")
            duplicate = observed["executions"][0]
            self.assertEqual(duplicate["state"], "duplicate_create")
            self.assertEqual(
                [item["execution_id"] for item in duplicate["matches"]],
                ["job_duplicate_a", "job_duplicate_b"],
            )
            self.assertEqual(
                [item["stop_state"] for item in duplicate["matches"]],
                ["stopped", "stopped"],
            )
            self.assertIn(run_id, service.budget.status()["outstanding"])
            stop_paths = sorted(
                call[1]
                for call in transport.calls
                if call[0] == "POST" and call[1].endswith("/stop")
            )
            self.assertEqual(
                stop_paths,
                [
                    f"/training_projects/{PROJECT}/jobs/job_duplicate_a/stop",
                    f"/training_projects/{PROJECT}/jobs/job_duplicate_b/stop",
                ],
            )
            evidence = JobEvidence(base, CAMPAIGN, run_id)
            for job_id in ("job_duplicate_a", "job_duplicate_b"):
                self.assertTrue(
                    base.list(
                        f"{evidence.prefix}/duplicate-stop-attempts/00/{job_id}"
                    )
                )

            current[0] = NOW + dt.timedelta(seconds=62)
            terminal = service.reconcile(run_id)
            self.assertEqual(terminal["state"], "terminal")
            self.assertEqual(
                [
                    item["execution_id"]
                    for item in terminal["executions"][0]["matches"]
                ],
                ["job_duplicate_a", "job_duplicate_b"],
            )
            self.assertEqual(
                terminal["accounted_microusd"],
                2 * RESERVATION_RATE_MICROUSD_PER_MINUTE,
            )
            self.assertNotIn(run_id, service.budget.status()["outstanding"])
            self.assertEqual(len(create_calls(transport)), 1)

            transport.calls.clear()
            self.assertEqual(service.reconcile(run_id), terminal)
            self.assertEqual(transport.calls, [])

    def test_duplicate_identity_set_above_operation_cap_survives_crash(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            created_name = [None]
            searches = [0]
            stopped = set()
            execution_ids = [
                f"job_duplicate_{index:03d}"
                for index in range(MAX_DUPLICATE_EXECUTIONS + 1)
            ]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return TimeoutError("create response lost")
                if (method, path) == ("POST", "/training_jobs/search"):
                    searches[0] += 1
                    return {
                        "training_jobs": [
                            provider_job(created_name[0], execution_id)[
                                "training_job"
                            ]
                            for execution_id in execution_ids + execution_ids[:1]
                        ]
                    }
                if method == "GET" and path.rsplit("/", 1)[-1] in execution_ids:
                    execution_id = path.rsplit("/", 1)[-1]
                    return provider_get(
                        created_name[0],
                        execution_id,
                        (
                            "TRAINING_JOB_STOPPED"
                            if execution_id in stopped
                            else "TRAINING_JOB_CREATED"
                        ),
                        seconds=1 if execution_id in stopped else 0,
                    )
                if method == "POST" and path.endswith("/stop"):
                    execution_id = path.split("/")[-2]
                    stopped.add(execution_id)
                    return provider_job(
                        created_name[0],
                        execution_id,
                        "TRAINING_JOB_STOPPED",
                        seconds=1,
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            launched = jobs(base, transport).launch(
                workload("a"), [entry("large-duplicate-set")]
            )
            run_id = launched["run_id"]
            crashing = jobs(
                CrashOnCreate(
                    base,
                    "/search-results/00.json",
                    error=SimulatedProcessCrash,
                ),
                transport,
            )
            with self.assertRaises(SimulatedProcessCrash):
                crashing.reconcile(run_id)

            evidence = JobEvidence(base, CAMPAIGN, run_id)
            search = json.loads(base.get(f"{evidence.prefix}/search-results/00.json"))
            self.assertEqual(
                [item["execution_id"] for item in search["matches"]],
                execution_ids,
            )
            first = jobs(base, transport).reconcile(run_id)
            duplicate = first["executions"][0]
            self.assertEqual(len(duplicate["matches"]), len(execution_ids))
            self.assertEqual(
                sum(item["stop_state"] == "deferred_operation_cap" for item in duplicate["matches"]),
                1,
            )
            duplicate_set = json.loads(
                base.get(f"{evidence.prefix}/duplicate-sets/00.json")
            )
            self.assertEqual(
                [item["execution_id"] for item in duplicate_set["matches"]],
                execution_ids,
            )
            first_stop_calls = [
                call for call in transport.calls if call[0] == "POST" and call[1].endswith("/stop")
            ]
            self.assertEqual(len(first_stop_calls), MAX_DUPLICATE_EXECUTIONS)

            jobs(base, transport).reconcile(run_id)
            second_stop_calls = [
                call for call in transport.calls if call[0] == "POST" and call[1].endswith("/stop")
            ]
            self.assertEqual(len(second_stop_calls), len(execution_ids))
            self.assertIn(execution_ids[-1], stopped)
            self.assertEqual(searches[0], 1)

    def test_stop_attempts_are_bounded_and_crashed_intents_are_burned(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            transport = FakeTransport(lambda *_arguments: TimeoutError("stop lost"))
            service = jobs(base, transport)
            execution = {
                "ordinal": 0,
                "execution_id": "job_stop",
                "execution_name": "milk-stop-child",
            }
            evidence = JobEvidence(base, CAMPAIGN, "e" * 64)
            self.assertEqual(
                [service._stop_once(evidence, execution) for _ in range(3)],
                ["ambiguous_stop", "ambiguous_stop", "ambiguous_stop"],
            )
            self.assertEqual(service._stop_once(evidence, execution), "attempt_bound_exhausted")
            self.assertEqual(len(transport.calls), 3)

            crashed = JobEvidence(base, CAMPAIGN, "f" * 64)
            for attempt in range(3):
                crashed.json(
                    f"stop-attempts/00/{attempt:02d}-intent.json",
                    {
                        "schema_version": "milk.baseten-provider-stop-attempt.v1",
                        "run_id": crashed.run_id,
                        "ordinal": 0,
                        "attempt": attempt,
                        "execution_id": execution["execution_id"],
                        "execution_name": execution["execution_name"],
                        "requested_at": NOW.isoformat(timespec="microseconds").replace(
                            "+00:00", "Z"
                        ),
                    },
                )
            self.assertEqual(
                service._stop_once(crashed, execution), "attempt_bound_exhausted"
            )
            self.assertEqual(len(transport.calls), 3)

    def test_stop_store_outage_never_causes_an_unjournaled_post(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            failing = FailStopIntent(base)
            transport = FakeTransport(lambda *_arguments: TimeoutError("stop lost"))
            execution = {
                "ordinal": 0,
                "execution_id": "job_stop_outage",
                "execution_name": "milk-stop-outage-child",
            }
            evidence = JobEvidence(failing, CAMPAIGN, "d" * 64)

            unavailable = [
                jobs(failing, transport)._stop_once(evidence, execution)
                for _ in range(4)
            ]
            self.assertEqual(
                unavailable,
                ["stop_intent_evidence_unavailable"] * 4,
            )
            self.assertEqual(transport.calls, [])

            failing.enabled = False
            recovered = [
                jobs(failing, transport)._stop_once(evidence, execution)
                for _ in range(4)
            ]
            self.assertEqual(
                recovered,
                ["ambiguous_stop"] * 3 + ["attempt_bound_exhausted"],
            )
            self.assertEqual(len(transport.calls), 3)

    def test_reconcile_cursor_covers_thirty_three_runs_across_two_passes(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            service = jobs(
                base,
                FakeTransport(
                    lambda *_arguments: AssertionError("provider must not be called")
                ),
            )
            run_ids = [f"{index:064x}" for index in range(1, 34)]
            service.budget.status = lambda: {
                "outstanding": {
                    run_id: {
                        "state": "reserved",
                        "provider_identity": baseten_provider_identity(PROJECT),
                        "provider_deadline_epoch_seconds": int(NOW.timestamp()) + 60,
                    }
                    for run_id in run_ids
                }
            }
            visited = []

            def reconcile(run_id):
                visited.append(run_id)
                return {
                    "schema_version": "milk.test-reconciliation.v1",
                    "run_id": run_id,
                    "evidence_complete": True,
                }

            service.reconcile = reconcile
            first = service.reconcile_all(1)
            first_visited = list(visited)
            second = service.reconcile_all(1)
            second_visited = visited[len(first_visited) :]

            self.assertEqual(first["processed"], MAX_RECONCILE_GROUPS)
            self.assertEqual(second["processed"], MAX_RECONCILE_GROUPS)
            self.assertEqual(first_visited, run_ids[:MAX_RECONCILE_GROUPS])
            self.assertEqual(second_visited[0], run_ids[-1])
            self.assertEqual(set(first_visited + second_visited), set(run_ids))
            cursor_key = (
                f"state/v1/campaigns/{CAMPAIGN}/providers/"
                "baseten/reconcile-cursor.json"
            )
            cursor = json.loads(base.get(cursor_key))
            self.assertEqual(
                cursor["schema_version"],
                "milk.baseten-reconcile-cursor.v2",
            )
            self.assertEqual(
                cursor["provider_identity"],
                baseten_provider_identity(PROJECT),
            )
            with self.assertRaises(FileNotFoundError):
                base.get(f"campaigns/v1/{CAMPAIGN}/reconcile-cursor.json")
            with self.assertRaises(FileNotFoundError):
                base.get(f"state/v1/campaigns/{CAMPAIGN}/reconcile-cursor.json")

    def test_five_hundred_twenty_status_polls_and_max_logs_still_terminalize(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]
            created_name = [None]
            terminal = [False]
            active_status_calls = [0]
            log_calls = [0]

            def handler(method, path, body, query):
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return provider_job(created_name[0], "job_many_polls")
                if (method, path) == (
                    "GET",
                    f"/training_projects/{PROJECT}/jobs/job_many_polls",
                ):
                    if not terminal[0]:
                        active_status_calls[0] += 1
                    return provider_get(
                        created_name[0],
                        "job_many_polls",
                        (
                            "TRAINING_JOB_COMPLETED"
                            if terminal[0]
                            else "TRAINING_JOB_RUNNING"
                        ),
                        seconds=520 if terminal[0] else 0,
                    )
                if method == "GET" and path.endswith("/logs"):
                    log_calls[0] += 1
                    timestamp = (
                        dt.datetime.fromtimestamp(
                            query["start_epoch_millis"] / 1000,
                            tz=dt.timezone.utc,
                        )
                        .isoformat(timespec="milliseconds")
                        .replace("+00:00", "Z")
                    )
                    return {
                        "logs": [
                            {
                                "timestamp": timestamp,
                                "message": "bounded",
                                "replica": "worker-0",
                                "request_id": f"request-{index:04d}",
                                "level": "INFO",
                            }
                            for index in range(1000)
                        ]
                    }
                if method == "GET" and path.endswith("/metrics"):
                    return provider_metrics(
                        created_name[0],
                        "job_many_polls",
                        "TRAINING_JOB_COMPLETED",
                        seconds=520,
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            service = jobs(base, transport, now=lambda: current[0])
            launched = service.launch(
                workload("b"), [entry("many-polls-child", seconds=1000)]
            )
            run_id = launched["run_id"]
            evidence = JobEvidence(base, CAMPAIGN, run_id)
            for second in range(1, 521):
                current[0] = NOW + dt.timedelta(seconds=second)
                observed = service.reconcile(run_id)
                self.assertEqual(observed["state"], "active_or_ambiguous")
            self.assertEqual(active_status_calls[0], 520)
            self.assertEqual(
                base.list(f"{evidence.prefix}/observations"),
                [],
            )
            self.assertEqual(
                base.list(f"{evidence.prefix}/terminal-observations"),
                [],
            )

            terminal[0] = True
            current[0] = NOW + dt.timedelta(seconds=580)
            pending = service.reconcile(run_id)
            self.assertEqual(pending["state"], "terminal_evidence_pending")
            current[0] += dt.timedelta(seconds=61)
            completed = service.reconcile(run_id)
            self.assertEqual(completed["state"], "terminal")
            self.assertEqual(log_calls[0], MAX_RUN_ATTEMPTS)

            log_summary = json.loads(
                base.get(f"{evidence.prefix}/log-collections/summary.json")
            )
            self.assertEqual(log_summary["attempts_used"], MAX_RUN_ATTEMPTS)
            manifest = json.loads(base.get(f"{evidence.prefix}/manifest.json"))
            self.assertEqual(
                manifest["schema_version"], "milk.job-evidence-manifest.v2"
            )
            self.assertGreater(manifest["object_count"], 256)
            self.assertGreater(len(manifest["pages"]), 1)
            self.assertEqual(evidence.manifest(), manifest)

    def test_future_provider_timestamps_are_rejected_with_bounded_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return provider_job(created_name[0], "job_future_timestamp")
                if (method, path) == (
                    "GET",
                    f"/training_projects/{PROJECT}/jobs/job_future_timestamp",
                ):
                    return provider_get(
                        created_name[0],
                        "job_future_timestamp",
                        "TRAINING_JOB_COMPLETED",
                        seconds=301,
                    )
                return AssertionError("unexpected provider operation")

            service = jobs(base, FakeTransport(handler))
            launched = service.launch(
                workload("c"), [entry("future-timestamp-child")]
            )
            evidence = JobEvidence(base, CAMPAIGN, launched["run_id"])
            observations = [service.reconcile(launched["run_id"]) for _ in range(4)]

            self.assertTrue(
                all(item["executions"][0]["state"] == "status_unavailable" for item in observations)
            )
            self.assertEqual(
                observations[-1]["collection_failures"][0]["persistence_state"],
                "attempt_bound_exhausted",
            )
            self.assertEqual(
                len(base.list(f"{evidence.prefix}/evidence-failures/00/status")),
                3,
            )
            self.assertEqual(base.list(f"{evidence.prefix}/observations"), [])
            self.assertEqual(
                base.list(f"{evidence.prefix}/terminal-observations"),
                [],
            )

    def test_terminal_manifest_and_finalization_repair_after_committed_crash(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            transport = FakeTransport(
                lambda *_arguments: AssertionError("provider must not be called")
            )
            service = jobs(base, transport)
            run_id, definition, evidence = seed_definition(
                service, workload("1"), [entry("finalize-child")]
            )
            preparation_sha256 = hashlib.sha256(b"finalize preparation").hexdigest()
            service.budget.prepare(
                run_id,
                preparation_sha256,
                int((NOW + dt.timedelta(seconds=60)).timestamp()),
                int((NOW + dt.timedelta(seconds=120)).timestamp()),
            )
            service.budget.reserve(
                run_id,
                definition["executions"][0]["reserved_microusd"],
                preparation_sha256,
            )
            settlement = service.budget.settle(run_id, 0)
            evidence.json(
                "terminal.json",
                {
                    "schema_version": "milk.baseten-launch-group-terminal.v1",
                    "run_id": run_id,
                    "state": "not_started",
                    "observed_at": NOW.isoformat(timespec="microseconds").replace(
                        "+00:00", "Z"
                    ),
                    "evidence_complete": True,
                    "collection_failures": [],
                    "accounted_minutes": 0,
                    "accounted_microusd": 0,
                    "settlement_sha256": _digest(settlement),
                    "executions": [
                        {
                            "ordinal": definition["executions"][0]["ordinal"],
                            "execution_run_id": definition["executions"][0][
                                "execution_run_id"
                            ],
                            "state": "not_authorized",
                        }
                    ],
                },
            )

            crashing = jobs(CrashAfterFinalizationCommit(base, run_id), transport)
            with self.assertRaisesRegex(RuntimeError, "after finalization CAS"):
                crashing.reconcile(run_id)
            manifest_key = f"{evidence.prefix}/manifest.json"
            manifest = base.get(manifest_key)

            repaired = jobs(base, transport)
            self.assertEqual(repaired.reconcile(run_id)["state"], "not_started")
            self.assertEqual(base.get(manifest_key), manifest)
            status = repaired.budget.status()
            self.assertNotIn(run_id, status["pending_settlements"])
            self.assertNotIn(run_id, status["outstanding"])
            self.assertEqual(transport.calls, [])

    def test_stale_pricing_allows_reconcile_but_blocks_new_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            current = [NOW]
            created_name = [None]

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    created_name[0] = body["training_job"]["name"]
                    return provider_job(created_name[0], "job_stale")
                if (method, path) == (
                    "GET",
                    f"/training_projects/{PROJECT}/jobs/job_stale",
                ):
                    return provider_get(
                        created_name[0],
                        "job_stale",
                        "TRAINING_JOB_COMPLETED",
                        seconds=61,
                    )
                if path.endswith("/logs"):
                    return {"logs": []}
                if path.endswith("/metrics"):
                    return provider_metrics(
                        created_name[0],
                        "job_stale",
                        "TRAINING_JOB_COMPLETED",
                        seconds=61,
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            service = jobs(base, transport, now=lambda: current[0])
            launched = service.launch(workload("2"), [entry("stale-child")])
            current[0] = NOW + dt.timedelta(hours=24, seconds=1)
            self.assertEqual(
                service.reconcile(launched["run_id"])["state"],
                "terminal_evidence_pending",
            )
            current[0] += dt.timedelta(seconds=61)
            self.assertEqual(service.reconcile(launched["run_id"])["state"], "terminal")
            with self.assertRaisesRegex(ValueError, "pricing receipt is stale"):
                service.launch(workload("3"), [entry("new-stale-child")])
            self.assertEqual(len(create_calls(transport)), 1)

    def test_malformed_create_intent_fails_before_provider_create(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            transport = FakeTransport(
                lambda _method, _path, body, _query: provider_job(
                    body["training_job"]["name"], "job_malformed_intent"
                )
            )
            corrupt = CorruptOnCreate(
                base,
                "/create-intents/00.json",
                lambda value: value.__setitem__("run_id", "0" * 64),
            )
            with self.assertRaisesRegex(ValueError, "stored provider create intent"):
                jobs(corrupt, transport).launch(
                    workload("4"), [entry("malformed-intent-child")]
                )
            self.assertEqual(transport.calls, [])

    def test_malformed_create_result_fails_before_followup_provider_call(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)

            def handler(method, path, body, query):
                del query
                if (method, path) == (
                    "POST",
                    f"/training_projects/{PROJECT}/jobs",
                ):
                    return provider_job(body["training_job"]["name"], "job_bad_result")
                if method == "GET":
                    return provider_get(
                        body["training_job"]["name"] if body else "unused",
                        "job_bad_result",
                    )
                return AssertionError("unexpected provider operation")

            transport = FakeTransport(handler)
            corrupt = CorruptOnCreate(
                base,
                "/create-results/00.json",
                lambda value: value.__setitem__("run_id", "0" * 64),
            )
            service = jobs(corrupt, transport)
            launched = service.launch(workload("5"), [entry("bad-result-child")])
            transport.calls.clear()
            with self.assertRaisesRegex(ValueError, "stored Baseten create result"):
                service.reconcile(launched["run_id"])
            self.assertEqual(transport.calls, [])

    def test_malformed_resolution_fails_before_followup_provider_call(self):
        with tempfile.TemporaryDirectory() as root:
            base = LocalEvidenceStore(root)
            authorize(base)
            transport = FakeTransport(
                lambda *_arguments: TimeoutError("create response lost")
            )
            service = jobs(base, transport)
            launched = service.launch(workload("6"), [entry("bad-resolution-child")])
            evidence = JobEvidence(base, CAMPAIGN, launched["run_id"])
            definition = json.loads(base.get(f"{evidence.prefix}/definition.json"))
            evidence.json(
                "resolved/00.json",
                {
                    "schema_version": "milk.baseten-provider-create-resolution.v1",
                    "run_id": "0" * 64,
                    "ordinal": 0,
                    "state": "resolved",
                    "execution_id": "job_bad_resolution",
                    "execution_name": definition["executions"][0]["name"],
                    "status": "TRAINING_JOB_CREATED",
                    "created_at": NOW.isoformat().replace("+00:00", "Z"),
                    "updated_at": NOW.isoformat().replace("+00:00", "Z"),
                },
            )
            transport.calls.clear()
            with self.assertRaisesRegex(ValueError, "stored Baseten create resolution"):
                service.reconcile(launched["run_id"])
            self.assertEqual(transport.calls, [])


if __name__ == "__main__":
    unittest.main()
