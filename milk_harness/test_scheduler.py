import base64
import datetime as dt
import hashlib
import json
import os
import tempfile
import threading
import unittest
import urllib.error
from unittest import mock
from pathlib import Path

from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.scheduler import (
    RUN_ARTIFACTS,
    RUN_LIMITS,
    SANITIZATION_PROFILE_SHA256,
    PROVIDER_LEASE_MAX_CLOCK_SKEW_SECONDS,
    PROVIDER_LEASE_TTL_SECONDS,
    active_provider_lease_reference,
    _validate_create_authority_environment,
    _require_no_create_authority_environment,
    acquire_provider_lease,
    archive_scheduler_pass,
    claim_provider_jobs_pass,
    create_provider_create_authorization,
    decode_gateway_summary,
    encode_summary,
    immutable_image,
    load_provider_lease_token,
    provider_create_gate,
    provider_lease_token,
    release_provider_lease,
    scheduler_pass_id,
    stage_gateway_ingest,
    store_identity_sha256,
    store_location_sha256,
    summarize_step,
    validate_create_confirmation,
    verify_gateway_run_config,
    verify_gateway_ingest_run_config,
    verify_provider_run_config,
    verify_provider_lease_token,
    verify_route_run_config,
)


HEX = "a" * 64
AUTHORITY_LOCATION = store_location_sha256("account", "create-authority")


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def streams(self, stdout, stderr=b""):
        out = self.root / "stdout"
        err = self.root / "stderr"
        out.write_bytes(stdout)
        err.write_bytes(stderr)
        return out, err

    def gateway_summary(self):
        out, err = self.streams(b'{"action":"hold"}\n')
        return summarize_step(
            "gateway", out, err, 0, "2026-08-27T00:00:00Z", "2026-08-27T00:00:01Z"
        )

    def provider_summary(self, authorized=False):
        out, err = self.streams(
            canonical_json(
                {
                    "schema_version": "milk.jobs-pass.v3",
                    "provider_policy": {
                        "primary": "baseten",
                        "fallback": "modal",
                    },
                    "provider_creates_authorized": authorized,
                    "provider_pass_claim_sha256": HEX,
                    "create_authorization_sha256": HEX if authorized else None,
                    "scope_prefix": "dt/v2/a/b/c/d",
                    "reconciliation": {},
                    "dispatched": None,
                    "teardown": None,
                    "winner_result_references": [],
                    "candidate_key_delivery_references": [],
                    "teardown_result_references": [],
                }
            )
        )
        return summarize_step(
            "provider_jobs", out, err, 0, "2026-08-27T00:00:01Z", "2026-08-27T00:00:02Z"
        )

    def lease_token(self, store, record, scope, identity, *, authorized=True):
        gate = {
            "schema_version": "milk.provider-create-gate.v1",
            "requested": authorized,
            "confirmation_valid": authorized,
            "granted": authorized,
            "confirmation_sha256": HEX if authorized else None,
        }
        reference = create_provider_create_authorization(
            store,
            record,
            scope,
            identity,
            AUTHORITY_LOCATION,
            gate,
            authorized,
        )
        return provider_lease_token(
            record, scope, identity, AUTHORITY_LOCATION, reference
        )

    def test_pass_id_and_images_are_exact(self):
        expected = hashlib.sha256(b"milk.scheduler-pass.v1\x00123\x00456\x001").hexdigest()
        self.assertEqual(scheduler_pass_id("123", "456", "1"), expected)
        reference = "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + HEX
        self.assertEqual(immutable_image("jobs", reference), reference)
        for invalid in (
            "ghcr.io/shantanujoshi/milk-jobs@sha256:" + HEX,
            "ghcr.io/milkinfrastructure/milk-jobs:latest",
            "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + "A" * 64,
        ):
            with self.assertRaises(ValueError):
                immutable_image("jobs", invalid)

    def test_campaign_lease_serializes_workflow_and_local_passes(self):
        store = LocalEvidenceStore(self.root / "lease")
        now = dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc)
        campaign = "c" * 64
        first = acquire_provider_lease(
            store, campaign, "1" * 64, "2" * 64, now=lambda: now
        )
        self.assertEqual(first["state"], "active")
        with self.assertRaises(RuntimeError):
            acquire_provider_lease(
                store,
                campaign,
                "3" * 64,
                "4" * 64,
                now=lambda: now + dt.timedelta(seconds=1),
            )
        released = release_provider_lease(
            store,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now + dt.timedelta(seconds=2),
        )
        self.assertEqual(released["state"], "released")
        second = acquire_provider_lease(
            store,
            campaign,
            "3" * 64,
            "4" * 64,
            now=lambda: now + dt.timedelta(seconds=3),
        )
        self.assertEqual(second["revision"], released["revision"] + 1)
        with self.assertRaises(RuntimeError):
            release_provider_lease(
                store,
                campaign,
                "1" * 64,
                "2" * 64,
                now=lambda: now + dt.timedelta(seconds=4),
            )

        expired_store = LocalEvidenceStore(self.root / "expired-lease")
        acquire_provider_lease(
            expired_store, campaign, "1" * 64, "2" * 64, now=lambda: now
        )
        takeover = acquire_provider_lease(
            expired_store,
            campaign,
            "3" * 64,
            "4" * 64,
            now=lambda: now + dt.timedelta(minutes=16),
        )
        self.assertEqual(takeover["owner_id"], "3" * 64)
        with self.assertRaises(RuntimeError):
            active_provider_lease_reference(
                expired_store,
                campaign,
                "1" * 64,
                "2" * 64,
                now=lambda: now + dt.timedelta(minutes=16),
            )

        same_holder_store = LocalEvidenceStore(self.root / "same-holder-lease")
        acquire_provider_lease(
            same_holder_store,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now,
        )
        reacquired = acquire_provider_lease(
            same_holder_store,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now + dt.timedelta(minutes=16),
        )
        self.assertEqual(reacquired["revision"], 2)
        self.assertEqual(reacquired["acquired_at"], "2026-08-27T00:16:00Z")
        self.assertLessEqual(
            PROVIDER_LEASE_MAX_CLOCK_SKEW_SECONDS,
            PROVIDER_LEASE_TTL_SECONDS - 12 * 60,
        )

        class EmptyRemoteStore:
            def get_versioned(self, key):
                raise urllib.error.HTTPError(key, 404, "missing", None, None)

            def create(self, key, body, content_type):
                self.created = (key, body, content_type)
                return True

        remote = EmptyRemoteStore()
        acquired = acquire_provider_lease(
            remote, campaign, "5" * 64, "6" * 64, now=lambda: now
        )
        self.assertEqual(acquired["owner_id"], "5" * 64)
        self.assertEqual(remote.created[2], "application/json")

    def test_jobs_lease_token_binds_live_revision_scope_and_store(self):
        store = LocalEvidenceStore(self.root / "jobs-lease")
        now = dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc)
        campaign = "c" * 64
        scope = (
            "dt/v2/11111111-1111-4111-8111-111111111111/"
            "22222222-2222-4222-8222-222222222222/"
            "33333333-3333-4333-8333-333333333333/"
            "44444444-4444-4444-8444-444444444444"
        )
        identity = store_identity_sha256("account", "evidence", "access")
        acquired = acquire_provider_lease(
            store,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now,
        )
        token = self.lease_token(store, acquired, scope, identity)
        token_path = self.root / "provider-lease-token.json"
        token_path.write_bytes(canonical_json(token))
        self.assertEqual(load_provider_lease_token(token_path), token)
        expected = {
            "object_key": (
                f"state/v1/campaigns/{campaign}/provider-pass-lease.json"
            ),
            "owner_id": "1" * 64,
            "revision": 1,
            "acquired_at": "2026-08-27T00:00:00Z",
            "expires_at": "2026-08-27T00:15:00Z",
            "create_authorization": {
                "schema_version": "milk.provider-create-authorization.v1",
                "campaign_id": campaign,
                "scope_prefix": scope,
                "owner_id": "1" * 64,
                "holder_id": "2" * 64,
                "lease_revision": 1,
                "lease_acquired_at": "2026-08-27T00:00:00Z",
                "lease_expires_at": "2026-08-27T00:15:00Z",
                "evidence_store_identity_sha256": identity,
                "create_authority_store_location_sha256": AUTHORITY_LOCATION,
                "requested": True,
                "confirmation_valid": True,
                "gateway_gate_granted": True,
                "configuration_verified": True,
                "granted": True,
                "confirmation_sha256": HEX,
            },
        }
        self.assertEqual(
            verify_provider_lease_token(
                store,
                store,
                token,
                campaign,
                scope,
                identity,
                AUTHORITY_LOCATION,
                now=lambda: now + dt.timedelta(seconds=1),
            ),
            expected,
        )
        barrier = threading.Barrier(3)
        claims = []
        failures = []

        def claim():
            try:
                barrier.wait()
                claims.append(
                    claim_provider_jobs_pass(
                        store,
                        store,
                        token,
                        campaign,
                        scope,
                        identity,
                        AUTHORITY_LOCATION,
                        now=lambda: now + dt.timedelta(seconds=1),
                    )
                )
            except Exception as error:
                failures.append(error)

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(len(claims), 1)
        self.assertEqual(len(failures), 1)
        self.assertRegex(str(failures[0]), "already claimed")
        with self.assertRaisesRegex(RuntimeError, "already claimed"):
            claim_provider_jobs_pass(
                store,
                store,
                token,
                campaign,
                scope,
                identity,
                AUTHORITY_LOCATION,
                now=lambda: now + dt.timedelta(seconds=1),
            )
        for label, changed, error in (
            (
                "scope",
                {**token, "scope_prefix": scope.replace("11111111", "aaaaaaaa")},
                ValueError,
            ),
            ("owner", {**token, "owner_id": "3" * 64}, RuntimeError),
            ("holder", {**token, "holder_id": "4" * 64}, ValueError),
            ("revision", {**token, "revision": 2}, RuntimeError),
            (
                "expiry",
                {**token, "expires_at": "2026-08-27T00:14:59Z"},
                RuntimeError,
            ),
            (
                "store",
                {**token, "evidence_store_identity_sha256": "5" * 64},
                ValueError,
            ),
        ):
            with self.subTest(label=label), self.assertRaises(error):
                verify_provider_lease_token(
                    store,
                    store,
                    changed,
                    campaign,
                    scope,
                    identity,
                    AUTHORITY_LOCATION,
                    now=lambda: now + dt.timedelta(seconds=1),
                )
        with self.assertRaisesRegex(RuntimeError, "not active"):
            verify_provider_lease_token(
                store,
                store,
                token,
                campaign,
                scope,
                identity,
                AUTHORITY_LOCATION,
                now=lambda: now + dt.timedelta(minutes=15),
            )

        release_provider_lease(
            store,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now + dt.timedelta(seconds=2),
        )
        successor = acquire_provider_lease(
            store,
            campaign,
            "6" * 64,
            "7" * 64,
            now=lambda: now + dt.timedelta(seconds=3),
        )
        with self.assertRaisesRegex(RuntimeError, "not active"):
            verify_provider_lease_token(
                store,
                store,
                token,
                campaign,
                scope,
                identity,
                AUTHORITY_LOCATION,
                now=lambda: now + dt.timedelta(seconds=4),
            )
        successor_token = self.lease_token(store, successor, scope, identity)
        for _ in range(2):
            self.assertEqual(
                verify_provider_lease_token(
                    store,
                    store,
                    successor_token,
                    campaign,
                    scope,
                    identity,
                    AUTHORITY_LOCATION,
                    now=lambda: now + dt.timedelta(seconds=4),
                )["revision"],
                3,
            )

        token_path.write_bytes(canonical_json({**token, "revision": 0}))
        with self.assertRaisesRegex(ValueError, "token is invalid"):
            load_provider_lease_token(token_path)
        token_path.write_bytes(canonical_json(token) + b" ")
        with self.assertRaisesRegex(ValueError, "token is invalid"):
            load_provider_lease_token(token_path)

    def test_paid_create_confirmation_is_exact(self):
        self.assertIsNone(validate_create_confirmation("false", "", ""))
        self.assertEqual(validate_create_confirmation("true", HEX, HEX), HEX)
        for requested, provided, expected in (
            ("true", "", HEX),
            ("true", "b" * 64, HEX),
            ("false", HEX, HEX),
            ("yes", HEX, HEX),
        ):
            with self.assertRaises(ValueError):
                validate_create_confirmation(requested, provided, expected)

        self.assertEqual(
            provider_create_gate("schedule", "", "", HEX, "skipped", ""),
            {
                "schema_version": "milk.provider-create-gate.v1",
                "requested": False,
                "confirmation_valid": False,
                "granted": False,
                "confirmation_sha256": None,
            },
        )
        self.assertEqual(
            provider_create_gate(
                "workflow_dispatch", "true", HEX, HEX, "success", "true"
            ),
            {
                "schema_version": "milk.provider-create-gate.v1",
                "requested": True,
                "confirmation_valid": True,
                "granted": True,
                "confirmation_sha256": HEX,
            },
        )
        denied = provider_create_gate(
            "workflow_dispatch", "true", "b" * 64, HEX, "skipped", ""
        )
        self.assertTrue(denied["requested"])
        self.assertFalse(denied["confirmation_valid"])
        self.assertFalse(denied["granted"])

    def test_provider_create_authority_is_external_and_tamper_evident(self):
        evidence = LocalEvidenceStore(self.root / "evidence")
        authority = LocalEvidenceStore(self.root / "create-authority")
        now = dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc)
        campaign = "c" * 64
        scope = (
            "dt/v2/11111111-1111-4111-8111-111111111111/"
            "22222222-2222-4222-8222-222222222222/"
            "33333333-3333-4333-8333-333333333333/"
            "44444444-4444-4444-8444-444444444444"
        )
        identity = store_identity_sha256("account", "evidence", "evidence-key")
        location = store_location_sha256("account", "create-authority")
        record = acquire_provider_lease(
            evidence,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now,
        )
        reference = create_provider_create_authorization(
            authority,
            record,
            scope,
            identity,
            location,
            {
                "schema_version": "milk.provider-create-gate.v1",
                "requested": True,
                "confirmation_valid": True,
                "granted": True,
                "confirmation_sha256": HEX,
            },
            True,
        )
        token = provider_lease_token(record, scope, identity, location, reference)
        with self.assertRaises(FileNotFoundError):
            verify_provider_lease_token(
                evidence,
                evidence,
                token,
                campaign,
                scope,
                identity,
                location,
                now=lambda: now,
            )
        verified = verify_provider_lease_token(
            evidence,
            authority,
            token,
            campaign,
            scope,
            identity,
            location,
            now=lambda: now,
        )
        self.assertTrue(verified["create_authorization"]["granted"])
        with self.assertRaisesRegex(ValueError, "digest differs"):
            verify_provider_lease_token(
                evidence,
                authority,
                {**token, "create_authorization_sha256": "f" * 64},
                campaign,
                scope,
                identity,
                location,
                now=lambda: now,
            )

    def test_reconcile_only_pass_has_no_create_authority_or_credentials(self):
        evidence = LocalEvidenceStore(self.root / "evidence")
        now = dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc)
        campaign = "c" * 64
        scope = (
            "dt/v2/11111111-1111-4111-8111-111111111111/"
            "22222222-2222-4222-8222-222222222222/"
            "33333333-3333-4333-8333-333333333333/"
            "44444444-4444-4444-8444-444444444444"
        )
        identity = store_identity_sha256("account", "evidence", "evidence-key")
        record = acquire_provider_lease(
            evidence,
            campaign,
            "1" * 64,
            "2" * 64,
            now=lambda: now,
        )
        token = provider_lease_token(record, scope, identity, None, None)
        self.assertEqual(
            (
                token["create_authority_store_location_sha256"],
                token["create_authorization_object_key"],
                token["create_authorization_sha256"],
            ),
            (None, None, None),
        )
        verified = verify_provider_lease_token(
            evidence,
            None,
            token,
            campaign,
            scope,
            identity,
            None,
            now=lambda: now + dt.timedelta(seconds=1),
        )
        self.assertIsNone(verified["create_authorization"])
        claim = claim_provider_jobs_pass(
            evidence,
            None,
            token,
            campaign,
            scope,
            identity,
            None,
            now=lambda: now + dt.timedelta(seconds=1),
        )
        self.assertFalse(claim["provider_creates_authorized"])
        self.assertIsNone(claim["create_authorization_sha256"])
        self.assertIsNone(claim["create_authority_store_location_sha256"])
        with self.assertRaisesRegex(ValueError, "must not receive"):
            verify_provider_lease_token(
                evidence,
                evidence,
                token,
                campaign,
                scope,
                identity,
                None,
                now=lambda: now + dt.timedelta(seconds=1),
            )
        for field, replacement in (
            ("create_authority_store_location_sha256", "a" * 64),
            ("create_authorization_object_key", "unexpected"),
            ("create_authorization_sha256", "b" * 64),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "token is invalid"
            ):
                provider_lease_token_value = {**token, field: replacement}
                path = self.root / f"{field}.json"
                path.write_bytes(canonical_json(provider_lease_token_value))
                load_provider_lease_token(path)
        with mock.patch.dict(os.environ, {}, clear=True):
            _require_no_create_authority_environment()
        with (
            mock.patch.dict(
                os.environ,
                {"MILK_CREATE_AUTHORITY_READ_R2_ACCESS_KEY_ID": "unexpected"},
                clear=True,
            ),
            self.assertRaisesRegex(ValueError, "must not receive"),
        ):
            _require_no_create_authority_environment()

    def test_provider_create_authority_has_distinct_read_and_write_credentials(self):
        environment = {
            "MILK_CREATE_AUTHORITY_WRITE_R2_ACCOUNT_ID": "account",
            "MILK_CREATE_AUTHORITY_WRITE_R2_BUCKET": "create-authority",
            "MILK_CREATE_AUTHORITY_WRITE_R2_ACCESS_KEY_ID": "writer",
            "MILK_CREATE_AUTHORITY_READ_R2_ACCOUNT_ID": "account",
            "MILK_CREATE_AUTHORITY_READ_R2_BUCKET": "create-authority",
            "MILK_CREATE_AUTHORITY_READ_R2_ACCESS_KEY_ID": "reader",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(
                _validate_create_authority_environment(),
                store_location_sha256("account", "create-authority"),
            )
        for changed in (
            {"MILK_CREATE_AUTHORITY_READ_R2_BUCKET": "another"},
            {"MILK_CREATE_AUTHORITY_READ_R2_ACCESS_KEY_ID": "writer"},
        ):
            with (
                self.subTest(changed=changed),
                mock.patch.dict(os.environ, {**environment, **changed}, clear=True),
                self.assertRaisesRegex(ValueError, "distinct read/write"),
            ):
                _validate_create_authority_environment()

    def test_confirmation_manifest_binds_actual_code_config_and_provider_inputs(self):
        root = Path(__file__).parents[1]
        source_commit = "1" * 40
        ids = [f"00000000-0000-4000-8000-0000000000{value:02d}" for value in range(10, 14)]
        scope = "dt/v2/" + "/".join(ids)
        images = {
            "gateway": "ghcr.io/milkinfrastructure/milk-gateway@sha256:" + HEX,
            "jobs": "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + HEX,
            "student_train": (
                "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "a" * 64
            ),
            "student_branch": (
                "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "b" * 64
            ),
            "teacher": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:" + HEX,
        }
        contract = {
            "teacher_runtime_image_reference": images["teacher"],
            "teacher_max_gpu_seconds": 3600,
            "teacher_max_calls": 10,
            "teacher_max_parallel_runs": 1,
            "teacher_max_decisions": 4096,
            "student_train_runtime_image_reference": images["student_train"],
            "student_branch_runtime_image_reference": images["student_branch"],
            "student_recipe_sha256": "b" * 64,
            "teacher_deployment_sha256": "c" * 64,
            "teacher_terms_sha256": "d" * 64,
        }
        gateway_deployment = {
            "source_commit": "2" * 40,
            "image_admission_sha256": "3" * 64,
            "release_sha256": "4" * 64,
            "application_id": "00000000-0000-4000-8000-000000000020",
            "application_version": 7,
            "container_image": "registry.cloudflare.com/milk-gateway@sha256:" + "5" * 64,
            "worker_version_id": "00000000-0000-4000-8000-000000000021",
        }
        gateway_config = {
            "tenant_id": ids[0],
            "project_id": ids[1],
            "environment_id": ids[2],
            "workload_id": ids[3],
            "stores": {
                "capture": {
                    "type": "cloudflare_r2",
                    "account_id": "account",
                    "bucket": "capture",
                },
                "control": {
                    "type": "cloudflare_r2",
                    "account_id": "account",
                    "bucket": "control",
                },
                "routes": {
                    "type": "cloudflare_r2",
                    "account_id": "account",
                    "bucket": "routes",
                },
            },
            "teacher": {
                "max_decisions": 4096,
                "execution": {
                    "type": "gpu_job",
                    "runtime_image_reference": images["teacher"],
                    "max_gpu_seconds": 3600,
                    "max_calls": 10,
                    "max_parallel_runs": 1,
                },
                "student_train_runtime_image_reference": images["student_train"],
                "student_branch_runtime_image_reference": images["student_branch"],
                "student_recipe_sha256": "b" * 64,
                "deployment_sha256": "c" * 64,
                "terms_sha256": "d" * 64,
            },
        }
        gateway_raw = canonical_json(gateway_config)
        artifacts = {
            relative: hashlib.sha256(root.joinpath(relative).read_bytes()).hexdigest()
            for relative in RUN_ARTIFACTS
        }
        provider_secrets = {
            "registry": "registry-secret",
            "config": "config-secret",
            "capture_access": "capture-access-secret",
            "capture_secret": "capture-secret-secret",
            "capture_session": None,
            "control_access": "control-access-secret",
            "control_secret": "control-secret-secret",
            "control_session": None,
        }
        provider_runtime = {
            "baseten_team_name": "milk-production",
            "winner_model_alias": "milk-student",
            "modal_workspace_id": "ws-1",
            "modal_workspace_name": "milk-workspace",
            "modal_environment_id": "en-1",
            "modal_environment_name": "milk-production",
            "modal_app_id": "ap-1",
            "modal_app_name": "milk-gpu",
            "modal_registry_secret_name": "modal-registry",
            "modal_registry_secret_id": "st-registry",
            "modal_config_secret_name": "modal-config",
            "modal_config_secret_id": "st-config",
            "modal_control_secret_name": "modal-control",
            "modal_control_secret_id": "st-control",
            "modal_capture_secret_name": "modal-capture",
            "modal_capture_secret_id": "st-capture",
            "modal_candidate_secret_name": "modal-candidate",
            "modal_candidate_secret_id": "st-candidate",
            "modal_teacher_volume_name": "modal-teacher",
            "modal_teacher_volume_id": "vo-teacher",
            "modal_student_train_volume_name": "modal-student",
            "modal_student_train_volume_id": "vo-student",
        }
        store_identities = {
            "gateway_capture": store_identity_sha256(
                "account", "capture", "gateway-capture-access"
            ),
            "gateway_control": store_identity_sha256(
                "account", "control", "gateway-control-access"
            ),
            "provider_control": store_identity_sha256(
                "account", "control", "provider-control-access"
            ),
            "evidence": store_identity_sha256(
                "account", "evidence", "evidence-access"
            ),
            "ops_log": store_identity_sha256("account", "ops", "ops-access"),
            "create_authority_write": store_identity_sha256(
                "account", "create-authority", "create-authority-write"
            ),
            "create_authority_read": store_identity_sha256(
                "account", "create-authority", "create-authority-read"
            ),
            "gateway_ingest_control": store_identity_sha256(
                "account", "control", "gateway-ingest-control"
            ),
            "gateway_ingest_route": store_identity_sha256(
                "account", "routes", "gateway-ingest-route"
            ),
            "route_control": store_identity_sha256(
                "account", "control", "route-control-access"
            ),
            "route_routes": store_identity_sha256(
                "account", "routes", "route-routes-access"
            ),
            "route_evidence": store_identity_sha256(
                "account", "route-evidence", "route-evidence-access"
            ),
        }
        manifest = {
            "schema_version": "milk.confirmed-production-run-config.v5",
            "provider_policy": {"primary": "baseten", "fallback": "modal"},
            "harness_source_commit": source_commit,
            "campaign_id": "e" * 64,
            "provider_project_id": "project_1",
            "provider_runtime": provider_runtime,
            "scope_prefix": scope,
            "images": images,
            "image_release_sha256": "f" * 64,
            "gateway_config_sha256": hashlib.sha256(gateway_raw).hexdigest(),
            "gateway_deployment": gateway_deployment,
            "gateway_job_contract": contract,
            "provider_secret_names": provider_secrets,
            "store_identity_sha256s": store_identities,
            "harness_artifact_sha256s": artifacts,
            "limits": RUN_LIMITS,
        }
        manifest_raw = canonical_json(manifest)
        confirmed = hashlib.sha256(manifest_raw).hexdigest()
        same_student_digest = {
            **manifest,
            "images": {
                **images,
                "student_branch": (
                    "ghcr.io/milkinfrastructure/milk-student-branch@sha256:"
                    + "a" * 64
                ),
            },
        }
        same_student_digest_raw = canonical_json(same_student_digest)
        with self.assertRaisesRegex(ValueError, "distinct digests"):
            verify_gateway_run_config(
                same_student_digest_raw,
                hashlib.sha256(same_student_digest_raw).hexdigest(),
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                "gateway-capture-access",
                "gateway-control-access",
            )
        self.assertEqual(
            verify_gateway_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                "gateway-capture-access",
                "gateway-control-access",
            ),
            manifest,
        )
        for invalid in (0, 4097, True):
            invalid_contract = {
                **contract,
                "teacher_max_decisions": invalid,
            }
            invalid_manifest = {
                **manifest,
                "gateway_job_contract": invalid_contract,
            }
            invalid_raw = canonical_json(invalid_manifest)
            with self.subTest(max_decisions=invalid), self.assertRaisesRegex(
                ValueError, "confirmed gateway GPU job contract is invalid"
            ):
                verify_gateway_run_config(
                    invalid_raw,
                    hashlib.sha256(invalid_raw).hexdigest(),
                    source_commit,
                    root,
                    gateway_raw,
                    images["gateway"],
                    "gateway-capture-access",
                    "gateway-control-access",
                )
        missing_decisions = dict(contract)
        del missing_decisions["teacher_max_decisions"]
        missing_manifest = {**manifest, "gateway_job_contract": missing_decisions}
        missing_raw = canonical_json(missing_manifest)
        with self.assertRaisesRegex(
            ValueError, "confirmed gateway GPU job contract is invalid"
        ):
            verify_gateway_run_config(
                missing_raw,
                hashlib.sha256(missing_raw).hexdigest(),
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                "gateway-capture-access",
                "gateway-control-access",
            )
        drifted_config = {
            **gateway_config,
            "teacher": {**gateway_config["teacher"], "max_decisions": 4095},
        }
        drifted_raw = canonical_json(drifted_config)
        drifted_manifest = {
            **manifest,
            "gateway_config_sha256": hashlib.sha256(drifted_raw).hexdigest(),
        }
        drifted_manifest_raw = canonical_json(drifted_manifest)
        with self.assertRaisesRegex(ValueError, "actual gateway config differs"):
            verify_gateway_run_config(
                drifted_manifest_raw,
                hashlib.sha256(drifted_manifest_raw).hexdigest(),
                source_commit,
                root,
                drifted_raw,
                images["gateway"],
                "gateway-capture-access",
                "gateway-control-access",
            )
        self.assertEqual(
            verify_gateway_ingest_run_config(
                manifest_raw,
                confirmed,
                gateway_raw,
                images["gateway"],
                "gateway-ingest-control",
                "gateway-ingest-route",
            ),
            manifest,
        )
        with self.assertRaises(ValueError):
            verify_gateway_ingest_run_config(
                manifest_raw,
                confirmed,
                gateway_raw,
                images["gateway"],
                "wrong-control-authority",
                "gateway-ingest-route",
            )
        self.assertEqual(
            verify_route_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                gateway_deployment,
                "route-control-access",
                "route-routes-access",
                store_identities["route_evidence"],
            ),
            manifest,
        )
        changed_gateway_deployment = {
            **gateway_deployment,
            "worker_version_id": "00000000-0000-4000-8000-000000000022",
        }
        with self.assertRaisesRegex(ValueError, "route gateway settings"):
            verify_route_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                changed_gateway_deployment,
                "route-control-access",
                "route-routes-access",
                store_identities["route_evidence"],
            )
        with self.assertRaisesRegex(ValueError, "route gateway settings"):
            verify_route_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                gateway_deployment,
                "route-control-access",
                "route-routes-access",
                "0" * 64,
            )
        v4_manifest = {
            **manifest,
            "schema_version": "milk.confirmed-production-run-config.v4",
        }
        v4_raw = canonical_json(v4_manifest)
        with self.assertRaisesRegex(ValueError, "confirmed run config is invalid"):
            verify_gateway_run_config(
                v4_raw,
                hashlib.sha256(v4_raw).hexdigest(),
                source_commit,
                root,
                gateway_raw,
                images["gateway"],
                "gateway-capture-access",
                "gateway-control-access",
            )
        self.assertEqual(
            verify_provider_run_config(
                manifest_raw.removesuffix(b"\n"),
                confirmed,
                source_commit,
                root,
                campaign_id="e" * 64,
                provider_project_id="project_1",
                scope_prefix=scope,
                images=images,
                image_release_sha256="f" * 64,
                gateway_deployment=gateway_deployment,
                provider_runtime=provider_runtime,
                provider_secret_names=provider_secrets,
                store_identities={
                    name: store_identities[name]
                    for name in (
                        "provider_control",
                        "evidence",
                        "ops_log",
                        "create_authority_write",
                        "create_authority_read",
                        "gateway_ingest_control",
                        "gateway_ingest_route",
                    )
                },
            ),
            manifest,
        )
        base_store_identities = {
            name: store_identities[name]
            for name in (
                "provider_control",
                "evidence",
                "ops_log",
                "gateway_ingest_control",
                "gateway_ingest_route",
            )
        }
        self.assertEqual(
            verify_provider_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                campaign_id="e" * 64,
                provider_project_id="project_1",
                scope_prefix=scope,
                images=images,
                image_release_sha256="f" * 64,
                gateway_deployment=gateway_deployment,
                provider_runtime=provider_runtime,
                provider_secret_names=provider_secrets,
                store_identities=base_store_identities,
                include_create_authority=False,
            ),
            manifest,
        )
        with self.assertRaises(ValueError):
            verify_provider_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                campaign_id="0" * 64,
                provider_project_id="project_1",
                scope_prefix=scope,
                images=images,
                image_release_sha256="f" * 64,
                gateway_deployment=gateway_deployment,
                provider_runtime=provider_runtime,
                provider_secret_names=provider_secrets,
                store_identities={
                    name: store_identities[name]
                    for name in (
                        "provider_control",
                        "evidence",
                        "ops_log",
                        "create_authority_write",
                        "create_authority_read",
                        "gateway_ingest_control",
                        "gateway_ingest_route",
                    )
                },
            )
        changed_runtime = dict(provider_runtime)
        changed_runtime["modal_app_id"] = "ap-other"
        with self.assertRaises(ValueError):
            verify_provider_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                campaign_id="e" * 64,
                provider_project_id="project_1",
                scope_prefix=scope,
                images=images,
                image_release_sha256="f" * 64,
                gateway_deployment=gateway_deployment,
                provider_runtime=changed_runtime,
                provider_secret_names=provider_secrets,
                store_identities={
                    name: store_identities[name]
                    for name in (
                        "provider_control",
                        "evidence",
                        "ops_log",
                        "create_authority_write",
                        "create_authority_read",
                        "gateway_ingest_control",
                        "gateway_ingest_route",
                    )
                },
            )
        invalid_provider_runtime = {**provider_runtime, "gpu_provider": "baseten"}
        with self.assertRaisesRegex(ValueError, "provider runtime setting"):
            verify_provider_run_config(
                manifest_raw,
                confirmed,
                source_commit,
                root,
                campaign_id="e" * 64,
                provider_project_id="project_1",
                scope_prefix=scope,
                images=images,
                image_release_sha256="f" * 64,
                gateway_deployment=gateway_deployment,
                provider_runtime=invalid_provider_runtime,
                provider_secret_names=provider_secrets,
                store_identities={
                    name: store_identities[name]
                    for name in (
                        "provider_control",
                        "evidence",
                        "ops_log",
                        "create_authority_write",
                        "create_authority_read",
                        "gateway_ingest_control",
                        "gateway_ingest_route",
                    )
                },
            )

    def test_summary_whitelists_and_discards_raw_streams(self):
        out, err = self.streams(
            b'{"schema_version":"dragontales.teacher-gpu-run-launch.v1",'
            b'"state":"ready","prompt":"must-not-survive"}\n',
            b"Authorization: Bearer must-not-survive",
        )
        summary = summarize_step(
            "gateway", out, err, 0, "2026-08-27T00:00:00Z", "2026-08-27T00:00:01Z"
        )
        encoded = canonical_json(summary)
        self.assertEqual(summary["state"], "succeeded")
        self.assertEqual(
            summary["accepted_fields"],
            {
                "schema_version": "dragontales.teacher-gpu-run-launch.v1",
                "action": None,
                "state": "ready",
            },
        )
        self.assertNotIn(b"must-not-survive", encoded)
        self.assertEqual(decode_gateway_summary(encode_summary(summary)), summary)
        winner_out, winner_err = self.streams(
            b'{"schema_version":"dragontales.student-winner-deployment-launch.v2"}\n'
        )
        self.assertEqual(
            summarize_step(
                "gateway",
                winner_out,
                winner_err,
                0,
                "2026-08-27T00:00:02Z",
                "2026-08-27T00:00:03Z",
            )["state"],
            "succeeded",
        )
        rejection_out, rejection_err = self.streams(
            b'{"schema_version":'
            b'"dragontales.snapshot-analysis-local-rejection-receipt.v1"}\n'
        )
        self.assertEqual(
            summarize_step(
                "gateway",
                rejection_out,
                rejection_err,
                0,
                "2026-08-27T00:00:04Z",
                "2026-08-27T00:00:05Z",
            )["accepted_fields"]["schema_version"],
            "dragontales.snapshot-analysis-local-rejection-receipt.v1",
        )

    def test_gateway_ingest_stages_only_exact_evidence_references(self):
        evidence = LocalEvidenceStore(self.root / "ingest-evidence")
        campaign = "c" * 64
        run_id = "d" * 64
        scope = "dt/v2/a/b/c/d"
        winner_key = (
            f"campaigns/v1/{campaign}/winner-jobs/{run_id}/gateway-result.json"
        )
        teardown_key = (
            f"campaigns/v1/{campaign}/winner-jobs/{run_id}/"
            "gateway-teardown-result.json"
        )
        winner_raw = b'{"winner":"exact"}\n'
        teardown_raw = b'{"teardown":"exact"}\n'
        delivery_raw = b'{"delivery":"redacted"}\n'
        evidence.create(winner_key, winner_raw, "application/json")
        evidence.create(teardown_key, teardown_raw, "application/json")
        delivery_key = (
            f"campaigns/v1/{campaign}/winner-jobs/{run_id}/"
            "candidate-key/deliveries/00.json"
        )
        evidence.create(delivery_key, delivery_raw, "application/json")

        def reference(raw, key, identity_name, identity):
            return {
                "student_job_id": "1" * 64,
                identity_name: identity,
                "object_key": key,
                "object_sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }

        value = {
            "schema_version": "milk.jobs-pass.v3",
            "provider_policy": {"primary": "baseten", "fallback": "modal"},
            "scope_prefix": scope,
            "provider_creates_authorized": False,
            "provider_pass_claim_sha256": HEX,
            "create_authorization_sha256": None,
            "reconciliation": {},
            "dispatched": None,
            "teardown": {},
            "winner_result_references": [
                reference(winner_raw, winner_key, "claim_sha256", "2" * 64)
            ],
            "candidate_key_delivery_references": [
                reference(delivery_raw, delivery_key, "claim_sha256", "2" * 64)
            ],
            "teardown_result_references": [
                reference(
                    teardown_raw,
                    teardown_key,
                    "authorization_sha256",
                    "3" * 64,
                )
            ],
        }
        output_dir = self.root / "gateway-ingest"
        output_dir.mkdir(mode=0o700)
        staged = stage_gateway_ingest(
            evidence,
            canonical_json(value),
            campaign,
            scope,
            output_dir,
        )
        self.assertEqual(
            [entry["kind"] for entry in staged["entries"]],
            ["winner", "teardown"],
        )
        for entry, raw in zip(staged["entries"], (winner_raw, teardown_raw)):
            path = output_dir / entry["file"]
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)

        bad = json.loads(canonical_json(value))
        bad["winner_result_references"][0]["object_sha256"] = "4" * 64
        bad_dir = self.root / "bad-gateway-ingest"
        bad_dir.mkdir(mode=0o700)
        with self.assertRaises(ValueError):
            stage_gateway_ingest(
                evidence,
                canonical_json(bad),
                campaign,
                scope,
                bad_dir,
            )
        bad = json.loads(canonical_json(value))
        bad["candidate_key_delivery_references"][0]["object_key"] = (
            f"campaigns/v1/{campaign}/winner-jobs/{run_id}/gateway-result.json"
        )
        bad_dir = self.root / "bad-candidate-delivery"
        bad_dir.mkdir(mode=0o700)
        with self.assertRaises(ValueError):
            stage_gateway_ingest(
                evidence,
                canonical_json(bad),
                campaign,
                scope,
                bad_dir,
            )
        bad = json.loads(canonical_json(value))
        bad["candidate_key_delivery_references"][0]["object_sha256"] = "4" * 64
        bad_dir = self.root / "bad-candidate-delivery-object"
        bad_dir.mkdir(mode=0o700)
        with self.assertRaises(ValueError):
            stage_gateway_ingest(
                evidence,
                canonical_json(bad),
                campaign,
                scope,
                bad_dir,
            )
        bad = json.loads(canonical_json(value))
        bad["candidate_key_delivery_references"][0]["claim_sha256"] = "5" * 64
        bad_dir = self.root / "unmatched-candidate-delivery"
        bad_dir.mkdir(mode=0o700)
        with self.assertRaises(ValueError):
            stage_gateway_ingest(
                evidence,
                canonical_json(bad),
                campaign,
                scope,
                bad_dir,
            )

    def test_invalid_success_is_not_accepted(self):
        out, err = self.streams(b'{"prompt":"not-a-tick-result"}\n')
        summary = summarize_step(
            "gateway", out, err, 0, "2026-08-27T00:00:00Z", "2026-08-27T00:00:01Z"
        )
        self.assertEqual(summary["state"], "invalid_result")
        self.assertIsNone(summary["accepted_fields"])
        duplicate = base64.urlsafe_b64encode(b'{"source":"gateway","source":"jobs"}\n').decode()
        with self.assertRaises(ValueError):
            decode_gateway_summary(duplicate)

        injected = self.gateway_summary()
        injected["accepted_fields"]["prompt"] = "must-not-survive"
        with self.assertRaises(ValueError):
            encode_summary(injected)

        out, err = self.streams(b'{"schema_version":"dragontales.unknown.v1"}\n')
        self.assertEqual(
            summarize_step(
                "gateway",
                out,
                err,
                0,
                "2026-08-27T00:00:00Z",
                "2026-08-27T00:00:01Z",
            )["state"],
            "invalid_result",
        )

    def test_archive_is_create_only_and_reference_is_content_free(self):
        ops = LocalEvidenceStore(self.root / "ops")
        evidence = LocalEvidenceStore(self.root / "evidence")
        campaign_id = "c" * 64
        pass_id = scheduler_pass_id("123", "456", "1")
        first_holder = "1" * 64
        acquire_provider_lease(evidence, campaign_id, pass_id, first_holder)
        arguments = {
            "ops_store": ops,
            "evidence_store": evidence,
            "repository_id": "123",
            "run_id": "456",
            "run_attempt": "1",
            "event": "schedule",
            "gateway_summary": self.gateway_summary(),
            "provider_summary": self.provider_summary(),
            "gateway_image": "ghcr.io/milkinfrastructure/milk-gateway@sha256:" + HEX,
            "jobs_image": "ghcr.io/milkinfrastructure/milk-jobs@sha256:" + HEX,
            "student_train_image": (
                "ghcr.io/milkinfrastructure/milk-student-train@sha256:" + "a" * 64
            ),
            "student_branch_image": (
                "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "b" * 64
            ),
            "teacher_image": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:" + HEX,
            "image_release_sha256": HEX,
            "campaign_id": campaign_id,
            "provider_lease": active_provider_lease_reference(
                evidence, campaign_id, pass_id, first_holder
            ),
            "provider_create_requested": "false",
            "provider_confirmation_valid": "false",
            "provider_configuration_verified": "false",
            "provider_create_granted": "false",
            "confirmation_sha256": "",
            "expected_confirmation_sha256": "",
        }
        same_student_digest = dict(arguments)
        same_student_digest["student_branch_image"] = (
            "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "a" * 64
        )
        with self.assertRaisesRegex(ValueError, "distinct digests"):
            archive_scheduler_pass(**same_student_digest)
        reference = archive_scheduler_pass(**arguments)
        self.assertEqual(reference, archive_scheduler_pass(**arguments))
        artifact_raw = ops.get(f"operational/v1/scheduler-passes/{pass_id}/archive.json")
        authority_raw = evidence.get(
            f"operational-log-references/v1/scheduler-passes/{pass_id}.json"
        )
        artifact = json.loads(artifact_raw)
        authority = json.loads(authority_raw)
        self.assertEqual(
            authority["ops_object_sha256"], hashlib.sha256(artifact_raw).hexdigest()
        )
        self.assertEqual(authority["sanitization_profile_sha256"], SANITIZATION_PROFILE_SHA256)
        self.assertEqual(
            authority["provider_create_authority"],
            {
                "requested": False,
                "confirmation_valid": False,
                "configuration_verified": False,
                "granted": False,
                "confirmation_sha256": None,
            },
        )
        self.assertNotIn("steps", authority)
        self.assertEqual(len(artifact["coverage"]), 9)

        changed = dict(arguments)
        changed["event"] = "workflow_dispatch"
        with self.assertRaises(ValueError):
            archive_scheduler_pass(**changed)

        paid = dict(arguments)
        release_provider_lease(evidence, campaign_id, pass_id, first_holder)
        paid_pass_id = scheduler_pass_id("123", "456", "2")
        paid_holder = "2" * 64
        acquire_provider_lease(evidence, campaign_id, paid_pass_id, paid_holder)
        paid.update(
            run_attempt="2",
            event="workflow_dispatch",
            provider_summary=self.provider_summary(True),
            provider_create_requested="true",
            provider_confirmation_valid="true",
            provider_configuration_verified="true",
            provider_create_granted="true",
            confirmation_sha256=HEX,
            expected_confirmation_sha256=HEX,
            provider_lease=active_provider_lease_reference(
                evidence, campaign_id, paid_pass_id, paid_holder
            ),
        )
        paid_reference = archive_scheduler_pass(**paid)
        self.assertTrue(paid_reference["provider_create_authority"]["granted"])

    def test_empty_gateway_transfer_is_an_explicit_gap(self):
        missing = decode_gateway_summary("")
        self.assertEqual(missing["state"], "missing")
        self.assertEqual(
            missing["gaps"][0]["reason"],
            "gateway_job_ended_before_sanitized_summary",
        )


if __name__ == "__main__":
    unittest.main()
