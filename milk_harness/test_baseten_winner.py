import datetime as dt
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest import mock

from deploy.baseten import winner as contract
from milk_harness import baseten_winner
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.provider_acceptance import encode as acceptance_bytes
from milk_harness.provider_acceptance import sha256 as acceptance_sha256


UTC = dt.timezone.utc
CAMPAIGN = "a" * 64
OUTBOX = "b" * 64
TEAM = "milk"
STUDENT = "c" * 64
STUDENT_RESULT = "d" * 64
CLAIM_BINDING = "e" * 64
IMAGE_RELEASE = "f" * 64
IMAGE_ADMISSION = "1" * 64
IMAGE = "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "2" * 64
MODEL_MANIFEST = "3" * 64
DEV_RECEIPT = "4" * 64
MODEL_ID = "model123"
DEPLOYMENT_ID = "deployment_123"
KEY = "candidate-key"
CLAIMED = dt.datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
RESERVED = CLAIMED + dt.timedelta(minutes=1)
ACCEPTED = RESERVED
PROVIDER_DEADLINE = CLAIMED + dt.timedelta(minutes=30)
CLAIM_EXPIRES = CLAIMED + dt.timedelta(hours=1)
SCOPE = {
    "tenant_id": "11111111-1111-4111-8111-111111111111",
    "project_id": "22222222-2222-4222-8222-222222222222",
    "environment_id": "33333333-3333-4333-8333-333333333333",
    "workload_id": "44444444-4444-4444-8444-444444444444",
    "eval_id": CAMPAIGN,
}
SCOPE_PREFIX = "dt/v3/" + "/".join(
    (
        CAMPAIGN,
        SCOPE["tenant_id"],
        SCOPE["project_id"],
        SCOPE["environment_id"],
        SCOPE["workload_id"],
    )
)


def utc(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def gateway_line(value, *, newline=False):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return raw + (b"\n" if newline else b"")


def gateway_claim():
    authority = {
        "schema_version": "dragontales.winner-deployment-authority.v3",
        "provider_policy": {"only": "baseten"},
        "provider_terms_sha256": "5" * 64,
        "student_branch_runtime_image_reference": IMAGE,
        "admission_program_sha256": hashlib.sha256(
            contract.ADMISSION_PATH.read_bytes()
        ).hexdigest(),
        "authorization_not_after": utc(CLAIM_EXPIRES),
        "max_wall_seconds": 1800,
        "max_cost_microusd": 10_000_000,
        "allow_private_candidate_http": False,
        "signing_public_key_hex": "6" * 64,
        "signing_key_id": "route-key-1",
        "candidate_max_in_flight": 1,
        "canary_candidate_basis_points": 100,
        "canary_valid_for_seconds": 900,
        "max_input_utf8_bytes": 2048,
        "max_input_messages": 16,
        "max_input_request_bytes": 16384,
        "route_schema_version": "dragontales.route.v4",
        "winner_admission_schema_version": contract.RECEIPT_SCHEMA,
    }
    claim = {
        "schema_version": "dragontales.student-winner-deployment-claim.v2",
        "scope": SCOPE,
        "student_job_id": STUDENT,
        "student_claim_sha256": "7" * 64,
        "student_result_sha256": STUDENT_RESULT,
        "winner": "static_fp8",
        "model_manifest": {
            "object_key": "models/winner/manifest.json",
            "sha256": MODEL_MANIFEST,
            "bytes": 128,
        },
        "dev_receipt": {
            "object_key": "models/winner/dev.json",
            "sha256": DEV_RECEIPT,
            "bytes": 64,
        },
        "student_branch_runtime_image_reference": IMAGE,
        "provider_binding_sha256": CLAIM_BINDING,
        "authority": authority,
        "claimed_at": utc(CLAIMED),
        "expires_at": utc(CLAIM_EXPIRES),
    }
    claim_raw = gateway_line(claim)
    launch = {
        "schema_version": "dragontales.student-winner-deployment-launch.v2",
        "student_job_id": STUDENT,
        "student_result_sha256": STUDENT_RESULT,
        "winner": "static_fp8",
        "deployment_claim_object_key": (
            f"{SCOPE_PREFIX}/jobs/student/{STUDENT}/winner-deployment/claim.json"
        ),
        "deployment_claim_sha256": hashlib.sha256(claim_raw).hexdigest(),
        "provider_binding_sha256": CLAIM_BINDING,
        "provider_policy": {"only": "baseten"},
        "student_branch_runtime_image_reference": IMAGE,
        "max_wall_seconds": 1800,
        "max_cost_microusd": 10_000_000,
        "expires_at": utc(CLAIM_EXPIRES),
    }
    return gateway_line(launch), claim_raw


def create_authorities():
    scope_prefix = SCOPE_PREFIX
    authorization = {
        "schema_version": "milk.provider-create-authorization.v1",
        "campaign_id": CAMPAIGN,
        "scope_prefix": scope_prefix,
        "owner_id": "8" * 64,
        "holder_id": "9" * 64,
        "lease_revision": 1,
        "lease_acquired_at": utc(CLAIMED - dt.timedelta(minutes=1)),
        "lease_expires_at": utc(CLAIMED + dt.timedelta(minutes=10)),
        "evidence_store_identity_sha256": "a" * 64,
        "create_authority_store_location_sha256": "d" * 64,
        "requested": True,
        "confirmation_valid": True,
        "gateway_gate_granted": True,
        "configuration_verified": True,
        "granted": True,
        "confirmation_sha256": "b" * 64,
    }
    authorization_raw = canonical_json(authorization)
    provider_pass = {
        "schema_version": "milk.provider-jobs-pass-claim.v1",
        "campaign_id": CAMPAIGN,
        "scope_prefix": scope_prefix,
        "owner_id": authorization["owner_id"],
        "holder_id": authorization["holder_id"],
        "lease_revision": authorization["lease_revision"],
        "lease_expires_at": authorization["lease_expires_at"],
        "lease_token_sha256": "c" * 64,
        "create_authorization_sha256": hashlib.sha256(
            authorization_raw
        ).hexdigest(),
        "create_authority_store_location_sha256": authorization[
            "create_authority_store_location_sha256"
        ],
        "provider_creates_authorized": True,
        "evidence_store_identity_sha256": authorization[
            "evidence_store_identity_sha256"
        ],
        "claimed_at": utc(CLAIMED),
    }
    return canonical_json(provider_pass), authorization_raw


class Clock:
    def __init__(self):
        self.value = CLAIMED + dt.timedelta(minutes=2)

    def __call__(self):
        return self.value


class RuntimeHeaders(dict):
    pass


class RuntimeResponse:
    def __init__(self, raw, *, status=200, headers=None):
        self.raw = raw
        self.status = status
        self.headers = RuntimeHeaders(
            {"Content-Type": "application/json"}
            if headers is None
            else headers
        )
        self.closed = False

    def getcode(self):
        return self.status

    def getheader(self, name, default=None):
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return default

    def read(self, maximum):
        return self.raw[:maximum]

    def close(self):
        self.closed = True


class RuntimeOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(
            (
                request.get_method(),
                request.full_url,
                timeout,
                request.get_header("Authorization"),
                request.data,
            )
        )
        if not self.responses:
            raise AssertionError("unexpected Baseten request")
        response = self.responses.pop(0)
        return response if isinstance(response, RuntimeResponse) else RuntimeResponse(response)


def capable_truss_result(argv):
    if tuple(argv) == contract.TRUSS_VERSION_ARGV:
        stdout = contract.TRUSS_VERSION_OUTPUT
    elif tuple(argv) == contract.TRUSS_RUNTIME_PROBE_ARGV:
        stdout = contract.TRUSS_RUNTIME_PROBE_OUTPUT
    elif tuple(argv) == contract.TRUSS_PUSH_HELP_ARGV:
        stdout = ("\n".join(contract.TRUSS_PUSH_REQUIRED_OPTIONS) + "\n").encode()
    else:
        raise AssertionError(f"unexpected Truss capability command: {argv!r}")
    return mock.Mock(returncode=0, stdout=stdout, stderr=b"")


def capable_truss_runner(argv, **unused_kwargs):
    return capable_truss_result(argv)


class FailCreateResultStore(LocalEvidenceStore):
    fail_create_result = False
    fail_delivery_receipt = False
    fail_gateway_result = False

    def create(self, key, body, content_type="application/octet-stream"):
        if self.fail_create_result and key.endswith("/create-result.json"):
            self.fail_create_result = False
            raise OSError("simulated evidence interruption")
        if self.fail_delivery_receipt and "/candidate-key/deliveries/" in key:
            self.fail_delivery_receipt = False
            raise OSError("simulated delivery receipt interruption")
        if self.fail_gateway_result and key.endswith("/gateway-result.json"):
            self.fail_gateway_result = False
            raise OSError("simulated gateway result interruption")
        return super().create(key, body, content_type)


class FakeBaseten:
    def __init__(self, case, store, run_id, clock):
        self.case = case
        self.store = store
        self.run_id = run_id
        self.clock = clock
        self.created = False
        self.autoscaling = {
            **contract.SERVING_AUTOSCALING,
            "min_replica": 1,
        }
        self.status = "ACTIVE"
        self.replicas = 1
        self.pushes = 0
        self.deactivations = 0
        self.fail_push = False
        self.timeout_after_create = False
        self.never_deactivate = False
        self.candidate_keys = {}
        self.key_creates = 0
        self.key_revocations = 0
        self.key_create_names = []
        self.fail_key_create_after_create = False
        self.fail_key_delete = False
        self.fail_delivery = False
        self.deliveries = []
        self.gateway_candidate_key_sha256 = None
        self.gateway_release_generation = 0
        self.gateway_release_id = "11111111-1111-4111-8111-111111111111"
        self.gateway_release_sha256 = None
        self.delivery_verified_at = None
        self.calls = []
        self.guard_count = 0
        self.create_authorized = True

    def guard(self):
        self.guard_count += 1
        if not self.create_authorized:
            return None
        return {
            "provider_pass_claim_sha256": hashlib.sha256(
                self.case.pass_raw
            ).hexdigest(),
            "create_authorization_sha256": hashlib.sha256(
                self.case.authorization_raw
            ).hexdigest(),
        }

    def called(self, name):
        self.calls.append(name)
        self.case.assertEqual(self.guard_count, len(self.calls))

    def push_output(self):
        return {
            "model_id": MODEL_ID,
            "model_version_id": DEPLOYMENT_ID,
            "predict_url": f"https://model-{MODEL_ID}.api.baseten.co/predict",
            "logs_url": "https://app.baseten.co/models/logs",
            "is_draft": False,
        }

    def lookup(self, team_name, model_name, execution_name, labels):
        self.called("lookup")
        self.case.assertEqual(team_name, TEAM)
        self.case.assertEqual(model_name, contract.model_name(self.run_id))
        self.case.assertEqual(execution_name, contract.execution_name(self.run_id))
        self.case.assertEqual(labels, contract.labels(self.run_id, self.case.claim_sha))
        return self.push_output() if self.created else None

    def push(self, team_name, truss, argv):
        self.called("push")
        self.pushes += 1
        self.case.assertEqual(team_name, TEAM)
        prefix = f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
        self.store.get(f"{prefix}/create-intent.json")
        self.case.assertIn(f"model_name: {contract.model_name(self.run_id)}\n", truss)
        self.case.assertNotIn("--promote", argv)
        self.case.assertEqual(argv[argv.index("--team") + 1], TEAM)
        if self.fail_push:
            raise TimeoutError("ambiguous")
        self.created = True
        if self.timeout_after_create:
            raise TimeoutError("Truss wait reached its hard cap")
        return self.push_output()

    def deployment(self):
        return {
            "id": DEPLOYMENT_ID,
            "created_at": utc(CLAIMED),
            "name": contract.execution_name(self.run_id),
            "model_id": MODEL_ID,
            "is_production": True,
            "is_development": False,
            "status": self.status,
            "active_replica_count": self.replicas,
            "autoscaling_settings": dict(self.autoscaling),
            "instance_type_name": "H100",
            "environment": "production",
            "labels": contract.labels(self.run_id, self.case.claim_sha),
        }

    def inspect(self, team_name, model_id, deployment_id):
        self.called("inspect")
        self.case.assertEqual((team_name, model_id, deployment_id), (TEAM, MODEL_ID, DEPLOYMENT_ID))
        return self.deployment()

    def update(self, team_name, model_id, deployment_id, settings):
        self.called("update")
        keys = self.store.list(
            f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}",
        )
        self.case.assertTrue(any("/intents/" in key for key in keys))
        self.autoscaling = dict(settings)
        return {"status": "ACCEPTED", "message": "queued"}

    def key_metadata(self, team_name, model_id):
        self.called("key_metadata")
        return {"keys": list(self.candidate_keys.values())}

    def create_key(self, team_name, team_id, model_id, name):
        self.called("create_key")
        self.case.assertEqual(team_id, "team_1")
        self.key_creates += 1
        self.key_create_names.append(name)
        attempt = int(name[-2:])
        prefix = f"candidate{attempt:02d}"
        key = prefix + "-secret"
        intent_key = (
            f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/"
            f"candidate-key/create-intents/{attempt:02d}.json"
        )
        self.store.get(intent_key)
        self.candidate_keys[name] = {
            "prefix": prefix,
            "name": name,
            "type": "WORKSPACE_INVOKE",
            "model_ids": [model_id],
            "team_name": TEAM,
            "owner": None,
        }
        if self.fail_key_create_after_create:
            self.fail_key_create_after_create = False
            raise TimeoutError("ambiguous candidate-key create")
        return key

    def delete_key(self, team_name, prefix):
        self.called("delete_key")
        self.key_revocations += 1
        if self.fail_key_delete:
            raise TimeoutError("ambiguous candidate-key revocation")
        matches = [
            name
            for name, item in self.candidate_keys.items()
            if item["prefix"] == prefix
        ]
        self.case.assertEqual(len(matches), 1)
        del self.candidate_keys[matches[0]]
        return {"prefix": prefix}

    def deliver(self, raw):
        if self.fail_delivery:
            raise BrokenPipeError("candidate-key sink closed")
        value = json.loads(raw)
        if value["schema_version"] == "milk.baseten-candidate-key-delivery.v1":
            self.case.assertEqual(
                hashlib.sha256(value["candidate_api_key"].encode()).hexdigest(),
                value["candidate_key_sha256"],
            )
            self.gateway_candidate_key_sha256 = value["candidate_key_sha256"]
            self.deliveries.append(bytes(raw))
            payload_sha256 = hashlib.sha256(raw).hexdigest()
            payload_bytes = len(raw)
            state = "installed"
        elif value["schema_version"] == "milk.baseten-candidate-key-delivery-verify.v1":
            payload_sha256 = value["payload_sha256"]
            payload_bytes = value["payload_bytes"]
            state = (
                "installed"
                if self.gateway_candidate_key_sha256
                == value["candidate_key_sha256"]
                else "absent"
            )
        elif value["schema_version"] == "milk.baseten-candidate-key-remove.v1":
            self.case.assertEqual(
                self.gateway_candidate_key_sha256,
                value["candidate_key_sha256"],
            )
            self.case.assertEqual(
                value["gateway_release_id"], self.gateway_release_id
            )
            self.case.assertEqual(
                value["gateway_release_sha256"], self.gateway_release_sha256
            )
            self.gateway_candidate_key_sha256 = None
            payload_sha256 = value["payload_sha256"]
            payload_bytes = value["payload_bytes"]
            state = "absent"
        else:
            raise AssertionError("unexpected candidate-key helper request")
        if state == "installed":
            self.gateway_release_generation += 1
            self.gateway_release_sha256 = (
                f"{self.gateway_release_generation:064x}"
            )
        return canonical_json(
            {
                "schema_version": "milk.baseten-candidate-key-delivery-ack.v1",
                "run_id": value["run_id"],
                "provider": "baseten",
                "team_name": value["team_name"],
                "model_id": value["model_id"],
                "key_name": value["key_name"],
                "key_prefix": value["key_prefix"],
                "candidate_key_sha256": value["candidate_key_sha256"],
                "payload_sha256": payload_sha256,
                "payload_bytes": payload_bytes,
                "gateway_release_id": (
                    self.gateway_release_id
                    if state == "installed"
                    else None
                ),
                "gateway_release_sha256": (
                    self.gateway_release_sha256 if state == "installed" else None
                ),
                "verified_at": utc(self.delivery_verified_at or self.clock.value),
                "state": state,
            }
        )

    def logs(self, team_name, model_id, deployment_id, limit):
        self.called("logs")
        self.case.assertEqual(limit, baseten_winner.MAX_LOG_RECORDS)
        receipt = {
            "schema_version": contract.MATERIALIZATION_SCHEMA,
            "student_job_id": STUDENT,
            "variant": "static_fp8",
            "model_manifest_sha256": MODEL_MANIFEST,
            "file_count": 3,
            "artifact_bytes": 4096,
            "stage_path": "/tmp/dragontales-winner",
            "model_path": "/tmp/dragontales-winner/model",
            "model_manifest_path": "/tmp/dragontales-winner/model-manifest.json",
        }
        return {
            "logs": [
                {
                    "timestamp": utc(self.clock()),
                    "message": json.dumps(receipt, separators=(",", ":")),
                    "replica": "replica-1",
                }
            ]
        }

    def probe(self, endpoint, candidate_key, student_job_id, alias, materialization):
        self.called("probe")
        self.case.assertEqual(materialization["student_job_id"], student_job_id)
        return {
            "schema_version": contract.PROBE_SCHEMA,
            "student_job_id": student_job_id,
            "student_variant": "static_fp8",
            "model_manifest_sha256": MODEL_MANIFEST,
            "model_alias_sha256": hashlib.sha256(alias.encode()).hexdigest(),
            "candidate_api_key_sha256": hashlib.sha256(candidate_key.encode()).hexdigest(),
            "models_response_sha256": "d" * 64,
            "chat_request_sha256": "e" * 64,
            "chat_response_sha256": "f" * 64,
        }

    def deactivate(self, team_name, model_id, deployment_id):
        self.called("deactivate")
        self.deactivations += 1
        prefix = f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
        self.case.assertTrue(
            any("deactivation/intents" in key for key in self.store.list(prefix))
        )
        if not self.never_deactivate:
            self.status = "INACTIVE"
            self.replicas = 0
        return {"message": "deactivation queued"}

    def billing(self, team_name, source, accepted_at, provider_not_after):
        self.called("billing")
        self.case.assertEqual(team_name, TEAM)
        return b'{"cost_microusd":1234,"token":"must-not-be-retained"}'


class BasetenWinnerLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = FailCreateResultStore(self.temporary.name)
        self.clock = Clock()
        self.launch_raw, self.claim_raw = gateway_claim()
        self.claim_sha = hashlib.sha256(self.claim_raw).hexdigest()
        self.pass_raw, self.authorization_raw = create_authorities()
        self.values = contract.settings(
            IMAGE,
            STUDENT,
            "milk-winner",
            "DOCKER_REGISTRY_ghcr.io",
            "gateway_config",
            "control-account",
            "6" * 64,
            "control_access",
            "control_secret",
            "control_session",
        )
        self.run_id = baseten_winner.winner_run_id(
            campaign_id=CAMPAIGN,
            launch_raw=self.launch_raw,
            claim_raw=self.claim_raw,
            outbox_sha256=OUTBOX,
            image_release_sha256=IMAGE_RELEASE,
            image_admission_sha256=IMAGE_ADMISSION,
        )
        self.reservation = {
            "schema_version": "milk.campaign-budget-reservation.v4",
            "campaign_id": CAMPAIGN,
            "run_id": self.run_id,
            "amount_microusd": baseten_winner.reservation_microusd(
                self.launch_raw,
                self.claim_raw,
            ),
            "preparation_sha256": "5" * 64,
            "provider_deadline_epoch_seconds": int(PROVIDER_DEADLINE.timestamp()),
            "intent_sha256": "6" * 64,
            "state": "reserved",
            "provider_role": "serving",
            "provider_identity": {"provider": "baseten", "team_name": TEAM},
            "pricing_receipt_sha256": "7" * 64,
            "provider_rate_microusd_per_minute": 108_330,
            "reservation_rate_microusd_per_minute": 125_000,
        }
        self.preflight = baseten_winner.baseten_preflight_receipt(
            TEAM,
            "team_1",
            "8" * 64,
            "9" * 64,
            "0" * 64,
            CLAIMED,
        )
        self.acceptance = baseten_winner.provider_acceptance(
            campaign_id=CAMPAIGN,
            run_id=self.run_id,
            team_name=TEAM,
            launch_raw=self.launch_raw,
            claim_raw=self.claim_raw,
            outbox_sha256=OUTBOX,
            image_release_sha256=IMAGE_RELEASE,
            image_admission_sha256=IMAGE_ADMISSION,
            preflight=self.preflight,
            reservation=self.reservation,
            provider_pass_claim_raw=self.pass_raw,
            create_authorization_raw=self.authorization_raw,
            reserved_at=RESERVED,
            accepted_at=ACCEPTED,
        )
        self.release = {
            "images": {
                "student-branch": {
                    "admission_sha256": IMAGE_ADMISSION,
                    "image_reference": IMAGE,
                }
            }
        }
        self.release_patch = mock.patch.object(
            baseten_winner,
            "load_published_private_image_release",
            return_value=self.release,
        )
        self.release_patch.start()

    def tearDown(self):
        self.release_patch.stop()
        self.temporary.cleanup()

    def lifecycle(self, provider):
        return baseten_winner.BasetenWinnerLifecycle(
            store=self.store,
            campaign_id=CAMPAIGN,
            team_name=TEAM,
            request_guard=provider.guard,
            now=self.clock,
        )

    def prepare(self, lifecycle):
        return lifecycle.prepare(
            launch_raw=self.launch_raw,
            claim_raw=self.claim_raw,
            outbox_sha256=OUTBOX,
            acceptance=self.acceptance,
            preflight=self.preflight,
            reservation=self.reservation,
            provider_pass_claim_raw=self.pass_raw,
            create_authorization_raw=self.authorization_raw,
            values=self.values,
        )

    def admit(self, lifecycle, provider):
        return lifecycle.admit(
            acceptance=self.acceptance,
            values=self.values,
            lookup_exact=provider.lookup,
            push_truss=provider.push,
            inspect_deployment=provider.inspect,
            update_autoscaling=provider.update,
            read_key_metadata=provider.key_metadata,
            create_model_scoped_key=provider.create_key,
            delete_api_key=provider.delete_key,
            deliver_candidate_key=provider.deliver,
            read_logs=provider.logs,
            run_probe=provider.probe,
            observed_cost_microusd=1234,
        )

    def route_receipts(self):
        prefix = SCOPE_PREFIX

        def receipt(revision, basis_points, previous):
            return gateway_line(
                {
                    "schema_version": "dragontales.route-publication-receipt.v2",
                    "route_revision": revision,
                    "student_job_id": STUDENT,
                    "student_result_sha256": STUDENT_RESULT,
                    "model_manifest_sha256": MODEL_MANIFEST,
                    "dev_receipt_sha256": DEV_RECEIPT,
                    "previous_route_revision": previous,
                    "candidate_basis_points": basis_points,
                    "manifest_object_key": f"{prefix}/routes/versions/{revision}.json",
                    "signature_object_key": (
                        f"{prefix}/routes/signatures/{revision}/{'9' * 64}.ed25519"
                    ),
                    "live_pointer_object_key": f"{prefix}/routes/current.json",
                    "state": "active",
                },
                newline=True,
            )

        canary_revision = "a" * 64
        return receipt(canary_revision, 100, None), receipt(
            "b" * 64,
            0,
            canary_revision,
        )

    def test_acceptance_wire_binds_create_authorities_exactly(self):
        raw = acceptance_bytes(self.acceptance)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertEqual(raw.count(b"\n"), 1)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), acceptance_sha256(self.acceptance))
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "559eab9eb3debbda1935cffcdd4d71b5cb17444c214c8887b688821e1206e789",
        )
        self.assertEqual(
            self.acceptance["provider_pass_claim_sha256"],
            hashlib.sha256(self.pass_raw).hexdigest(),
        )
        reordered = dict(reversed(tuple(self.acceptance.items())))
        with self.assertRaisesRegex(ValueError, "order"):
            acceptance_bytes(reordered)
        tampered = dict(self.acceptance)
        tampered["create_authorization_sha256"] = None
        with self.assertRaises(ValueError):
            acceptance_bytes(tampered)

    def test_prior_or_fallback_winner_authority_is_rejected(self):
        for label, mutation in (
            (
                "v2",
                lambda authority: authority.__setitem__(
                    "schema_version",
                    "dragontales.winner-deployment-authority.v2",
                ),
            ),
            (
                "fallback",
                lambda authority: authority.__setitem__(
                    "provider_policy",
                    {"primary": "baseten", "fallback": "modal"},
                ),
            ),
        ):
            claim = json.loads(self.claim_raw)
            mutation(claim["authority"])
            with (
                self.subTest(label=label),
                self.assertRaisesRegex(ValueError, "winner deployment authority"),
            ):
                baseten_winner.winner_run_id(
                    campaign_id=CAMPAIGN,
                    launch_raw=self.launch_raw,
                    claim_raw=gateway_line(claim),
                    outbox_sha256=OUTBOX,
                    image_release_sha256=IMAGE_RELEASE,
                    image_admission_sha256=IMAGE_ADMISSION,
                )

    def test_create_authority_rejects_another_eval_namespace(self):
        with self.assertRaisesRegex(ValueError, "create authority"):
            baseten_winner._validate_creation_authorities(
                self.pass_raw,
                self.authorization_raw,
                CAMPAIGN,
                {**SCOPE, "eval_id": "0" * 64},
            )
        with self.assertRaisesRegex(ValueError, "eval differs"):
            baseten_winner.winner_run_id(
                campaign_id="0" * 64,
                launch_raw=self.launch_raw,
                claim_raw=self.claim_raw,
                outbox_sha256=OUTBOX,
                image_release_sha256=IMAGE_RELEASE,
                image_admission_sha256=IMAGE_ADMISSION,
            )

    def test_concrete_runtime_preflight_proves_team_and_full_access_key(self):
        teams_raw = json.dumps(
            {
                "teams": [
                    {
                        "id": "team_1",
                        "name": TEAM,
                        "default": True,
                        "created_at": utc(CLAIMED),
                    }
                ]
            }
        ).encode()
        keys_raw = json.dumps(
            {
                "keys": [
                    {
                        "prefix": "manageprefix",
                        "name": "milk-jobs",
                        "type": "WORKSPACE_MANAGE_ALL",
                        "model_ids": None,
                        "team_name": TEAM,
                        "owner": None,
                    }
                ]
            }
        ).encode()
        opener = RuntimeOpener([teams_raw, keys_raw])
        runtime = baseten_winner.BasetenWinnerRuntime(
            api_key="manageprefix-secret",
            team_name=TEAM,
            opener=opener,
            runner=capable_truss_runner,
        )
        guards = []
        lifecycle = baseten_winner.BasetenWinnerLifecycle(
            store=self.store,
            campaign_id=CAMPAIGN,
            team_name=TEAM,
            request_guard=lambda: guards.append(True),
            runtime=runtime,
            now=self.clock,
        )

        preflight = lifecycle.preflight()

        self.assertEqual(preflight["team_id"], "team_1")
        self.assertEqual(preflight["team_name"], TEAM)
        self.assertEqual(len(guards), 1)
        self.assertTrue(opener.requests[0][1].endswith("/teams?name=milk"))
        self.assertTrue(opener.requests[1][1].endswith("/api_keys"))
        self.assertEqual(
            opener.requests[0][3],
            "Api-Key manageprefix-secret",
        )
        self.assertEqual(
            preflight["management_openapi_sha256"],
            baseten_winner.MANAGEMENT_OPENAPI_SHA256,
        )

    def test_preflight_returns_only_typed_retryable_unavailability(self):
        rate_limit = urllib.error.HTTPError(
            baseten_winner.API_ROOT + "/teams?name=milk",
            429,
            "rate limited",
            {},
            io.BytesIO(b"{}"),
        )
        rate_limited = mock.Mock()
        rate_limited.open.side_effect = rate_limit
        timeout = mock.Mock()
        timeout.open.side_effect = TimeoutError("timed out")
        cases = (
            (rate_limited, "rate_limited", 429),
            (
                RuntimeOpener([RuntimeResponse(b"{}", status=503)]),
                "server_unavailable",
                503,
            ),
            (timeout, "timeout", None),
        )
        for opener, reason, status in cases:
            with self.subTest(reason=reason, status=status):
                guards = []
                lifecycle = baseten_winner.BasetenWinnerLifecycle(
                    store=self.store,
                    campaign_id=CAMPAIGN,
                    team_name=TEAM,
                    request_guard=lambda: guards.append(True),
                    runtime=baseten_winner.BasetenWinnerRuntime(
                        api_key="manageprefix-secret",
                        team_name=TEAM,
                        opener=opener,
                        runner=capable_truss_runner,
                    ),
                    now=self.clock,
                )

                result = lifecycle.preflight()

                self.assertEqual(
                    tuple(result),
                    (
                        "schema_version",
                        "provider",
                        "team_name",
                        "management_openapi_sha256",
                        "outcome",
                        "reason",
                        "status",
                        "evidence_sha256",
                        "observed_at",
                    ),
                )
                self.assertEqual(result["outcome"], "retryable_unavailable")
                self.assertEqual((result["reason"], result["status"]), (reason, status))
                self.assertEqual(len(guards), 1)

        lifecycle = baseten_winner.BasetenWinnerLifecycle(
            store=self.store,
            campaign_id=CAMPAIGN,
            team_name=TEAM,
            request_guard=lambda: None,
            runtime=baseten_winner.BasetenWinnerRuntime(
                api_key="manageprefix-secret",
                team_name=TEAM,
                opener=RuntimeOpener([RuntimeResponse(b"{}", status=401)]),
                runner=capable_truss_runner,
            ),
            now=self.clock,
        )
        with self.assertRaisesRegex(RuntimeError, "hard non-200"):
            lifecycle.preflight()

    def test_preflight_reports_capability_unavailable_without_provider_calls(self):
        opener = RuntimeOpener([])
        guards = []
        lifecycle = baseten_winner.BasetenWinnerLifecycle(
            store=self.store,
            campaign_id=CAMPAIGN,
            team_name=TEAM,
            request_guard=lambda: guards.append(True),
            runtime=baseten_winner.BasetenWinnerRuntime(
                api_key="manageprefix-secret",
                team_name=TEAM,
                opener=opener,
                runner=lambda unused_argv, **unused_kwargs: mock.Mock(
                    returncode=0,
                    stdout=b"wrong Truss version\n",
                    stderr=b"",
                ),
            ),
            now=self.clock,
        )

        result = lifecycle.preflight()

        self.assertEqual(result["outcome"], "retryable_unavailable")
        self.assertEqual(result["reason"], "capability_unavailable")
        self.assertIsNone(result["status"])
        self.assertEqual(guards, [])
        self.assertEqual(opener.requests, [])

    def test_concrete_runtime_uses_only_pinned_truss_and_exact_team(self):
        output = json.dumps(
            {
                "model_id": MODEL_ID,
                "model_version_id": DEPLOYMENT_ID,
                "predict_url": f"https://model-{MODEL_ID}.api.baseten.co/predict",
                "logs_url": (
                    f"https://app.baseten.co/models/{MODEL_ID}/logs/{DEPLOYMENT_ID}"
                ),
                "is_draft": False,
            },
            separators=(",", ":"),
        ).encode()
        calls = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            if tuple(argv) in {
                contract.TRUSS_VERSION_ARGV,
                contract.TRUSS_RUNTIME_PROBE_ARGV,
                contract.TRUSS_PUSH_HELP_ARGV,
            }:
                return capable_truss_result(argv)
            self.assertEqual(argv[argv.index("--team") + 1], TEAM)
            self.assertIn(
                f"model_name: {contract.model_name(self.run_id)}",
                Path(argv[3], "config.yaml").read_text(),
            )
            return mock.Mock(returncode=0, stdout=output, stderr=b"")

        runtime = baseten_winner.BasetenWinnerRuntime(
            api_key="manageprefix-secret",
            team_name=TEAM,
            opener=RuntimeOpener([]),
            runner=runner,
        )
        truss = contract.truss_config(self.values, self.run_id)
        argv = contract.push_argv(
            f"/tmp/milk-winner/{self.run_id}/truss",
            self.run_id,
            self.claim_sha,
            TEAM,
        )

        self.assertEqual(runtime._push_truss(TEAM, truss, argv), output)
        self.assertEqual(len(calls), 4)
        self.assertTrue(all(call[1]["env"]["COLUMNS"] == "240" for call in calls))
        self.assertEqual(calls[3][1]["timeout"], 600)
        self.assertNotIn("shell", calls[3][1])
        self.assertFalse(Path(f"/tmp/milk-winner/{self.run_id}").exists())

    def test_concrete_runtime_never_pushes_without_exact_direct_image_capability(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            return mock.Mock(returncode=0, stdout=b"wrong\n", stderr=b"")

        runtime = baseten_winner.BasetenWinnerRuntime(
            api_key="manageprefix-secret",
            team_name=TEAM,
            opener=RuntimeOpener([]),
            runner=runner,
        )
        truss = contract.truss_config(self.values, self.run_id)
        argv = contract.push_argv(
            f"/tmp/milk-winner/{self.run_id}/truss",
            self.run_id,
            self.claim_sha,
            TEAM,
        )

        with self.assertRaisesRegex(RuntimeError, "capability is unavailable"):
            runtime._push_truss(TEAM, truss, argv)

        self.assertEqual([call[0] for call in calls], [list(contract.TRUSS_VERSION_ARGV)])
        self.assertFalse(Path(f"/tmp/milk-winner/{self.run_id}").exists())

    def test_concrete_runtime_creates_and_revokes_only_the_exact_model_key(self):
        model = {
            "id": MODEL_ID,
            "created_at": utc(CLAIMED),
            "name": contract.model_name(self.run_id),
            "deployments_count": 1,
            "production_deployment_id": DEPLOYMENT_ID,
            "development_deployment_id": None,
            "instance_type_name": "H100",
            "team_name": TEAM,
        }
        opener = RuntimeOpener(
            [
                json.dumps(model).encode(),
                b'{"api_key":"candidate00-secret"}',
                b'{"prefix":"candidate00"}',
            ]
        )
        runtime = baseten_winner.BasetenWinnerRuntime(
            api_key="manageprefix-secret",
            team_name=TEAM,
            opener=opener,
        )
        name = baseten_winner._candidate_key_name(self.run_id, 0)

        key = runtime._create_model_scoped_key(TEAM, "team_1", MODEL_ID, name)
        runtime._delete_api_key(TEAM, "candidate00")

        self.assertEqual(key, "candidate00-secret")
        self.assertEqual(opener.requests[1][0], "POST")
        self.assertEqual(
            opener.requests[1][1],
            baseten_winner.API_ROOT + "/teams/team_1/api_keys",
        )
        self.assertEqual(opener.requests[1][3], "Api-Key manageprefix-secret")
        self.assertEqual(
            json.loads(opener.requests[1][4]),
            {
                "name": name,
                "type": "WORKSPACE_INVOKE",
                "model_ids": [MODEL_ID],
            },
        )
        self.assertEqual(opener.requests[2][0], "DELETE")
        self.assertTrue(opener.requests[2][1].endswith("/api_keys/candidate00"))

    def test_key_metadata_accepts_provider_key_management_category(self):
        keys = baseten_winner._api_key_metadata(
            {
                "keys": [
                    {
                        "prefix": "manager",
                        "name": "key-manager",
                        "type": "WORKSPACE_MANAGE_API_KEYS",
                        "model_ids": None,
                        "team_name": None,
                        "owner": None,
                    }
                ]
            }
        )

        self.assertEqual(keys[0]["type"], "WORKSPACE_MANAGE_API_KEYS")

    def test_concrete_lookup_reproves_model_team_and_exact_deployment(self):
        model = {
            "id": MODEL_ID,
            "created_at": utc(CLAIMED),
            "name": contract.model_name(self.run_id),
            "deployments_count": 1,
            "production_deployment_id": DEPLOYMENT_ID,
            "development_deployment_id": None,
            "instance_type_name": "H100",
            "team_name": TEAM,
        }
        deployment = FakeBaseten(
            self,
            self.store,
            self.run_id,
            self.clock,
        ).deployment()
        opener = RuntimeOpener(
            [
                json.dumps({"models": [model]}).encode(),
                json.dumps(model).encode(),
                json.dumps({"deployments": [deployment]}).encode(),
                json.dumps(deployment).encode(),
            ]
        )
        runtime = baseten_winner.BasetenWinnerRuntime(
            api_key="manageprefix-secret",
            team_name=TEAM,
            opener=opener,
        )

        raw = runtime._lookup_exact(
            TEAM,
            contract.model_name(self.run_id),
            contract.execution_name(self.run_id),
            contract.labels(self.run_id, self.claim_sha),
        )

        self.assertEqual(raw["model_id"], MODEL_ID)
        self.assertEqual(raw["model_version_id"], DEPLOYMENT_ID)
        self.assertEqual(len(opener.requests), 4)

    def test_concrete_runtime_rejects_headers_redirects_and_oversized_bodies(self):
        cases = (
            RuntimeResponse(b"{}", headers={"Content-Type": "text/plain"}),
            RuntimeResponse(b"{}", status=302),
            RuntimeResponse(b"x" * (baseten_winner.MAX_PROVIDER_BYTES + 1)),
            RuntimeResponse(
                b"{}",
                headers={
                    "Content-Type": "application/json",
                    "X-Oversized": "x" * baseten_winner.MAX_PROVIDER_HEADER_BYTES,
                },
            ),
        )
        for response in cases:
            with self.subTest(status=response.status, size=len(response.raw)):
                runtime = baseten_winner.BasetenWinnerRuntime(
                    api_key="manageprefix-secret",
                    team_name=TEAM,
                    opener=RuntimeOpener([response]),
                )
                with self.assertRaises((RuntimeError, ValueError)):
                    runtime._request("GET", "/teams")

    def test_acceptance_rejects_provider_derived_run_id(self):
        provider_run_id = hashlib.sha256(
            (self.run_id + ":baseten:" + TEAM).encode()
        ).hexdigest()

        with self.assertRaisesRegex(ValueError, "provider-neutral"):
            baseten_winner.provider_acceptance(
                campaign_id=CAMPAIGN,
                run_id=provider_run_id,
                team_name=TEAM,
                launch_raw=self.launch_raw,
                claim_raw=self.claim_raw,
                outbox_sha256=OUTBOX,
                image_release_sha256=IMAGE_RELEASE,
                image_admission_sha256=IMAGE_ADMISSION,
                preflight=self.preflight,
                reservation=self.reservation,
                provider_pass_claim_raw=self.pass_raw,
                create_authorization_raw=self.authorization_raw,
                reserved_at=RESERVED,
                accepted_at=ACCEPTED,
            )

    def test_prepare_rejects_nonmatching_private_student_image(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.release["images"]["student-branch"]["image_reference"] = (
            "ghcr.io/milkinfrastructure/milk-student-branch@sha256:" + "0" * 64
        )
        with self.assertRaisesRegex(ValueError, "private student image"):
            self.prepare(lifecycle)

    def test_create_intent_precedes_push_and_key_creation_precedes_admission(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        result = self.admit(lifecycle, provider)
        self.assertEqual(result["state"], "admitted")
        self.assertEqual(provider.pushes, 1)
        self.assertEqual(provider.key_creates, 1)
        self.assertEqual(len(provider.deliveries), 1)
        self.assertEqual(provider.guard_count, len(provider.calls))
        self.assertEqual(provider.autoscaling, contract.SERVING_AUTOSCALING)

    def test_ambiguous_key_create_is_reconciled_by_revoke_then_fresh_create(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.fail_key_create_after_create = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)

        first = self.admit(lifecycle, provider)
        second = self.admit(lifecycle, provider)

        self.assertEqual(first["state"], "candidate_key_create_ambiguous_no_replay")
        self.assertEqual(second["state"], "admitted")
        self.assertEqual(provider.key_creates, 2)
        self.assertEqual(provider.key_revocations, 1)
        self.assertNotEqual(provider.key_create_names[0], provider.key_create_names[1])

    def test_failed_key_delivery_revokes_before_admission(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.fail_delivery = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)

        result = self.admit(lifecycle, provider)

        self.assertEqual(result["state"], "candidate_key_delivery_failed_revoked")
        self.assertEqual(provider.key_revocations, 1)
        self.assertFalse(provider.candidate_keys)
        with self.assertRaises(FileNotFoundError):
            self.store.get(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/gateway-result.json"
            )

    def test_late_gateway_install_is_revoked_before_admission(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.delivery_verified_at = PROVIDER_DEADLINE - dt.timedelta(minutes=14)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)

        result = self.admit(lifecycle, provider)

        self.assertEqual(result["state"], "candidate_key_delivery_failed_revoked")
        self.assertEqual(provider.key_revocations, 1)
        with self.assertRaises(FileNotFoundError):
            self.store.get(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/gateway-result.json"
            )

    def test_ambiguous_key_revoke_is_never_replayed(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.fail_delivery = True
        provider.fail_key_delete = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)

        first = self.admit(lifecycle, provider)
        second = self.admit(lifecycle, provider)

        self.assertEqual(
            first["state"],
            "candidate_key_delivery_failed_revoke_ambiguous",
        )
        self.assertEqual(
            second["state"],
            "candidate_key_delivery_recovery_failed_revoke_ambiguous",
        )
        self.assertEqual(provider.key_revocations, 1)

    def test_crash_after_push_recovers_by_lookup_without_replay(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.store.fail_create_result = True
        with self.assertRaisesRegex(OSError, "simulated"):
            self.admit(lifecycle, provider)
        result = self.admit(lifecycle, provider)
        self.assertEqual(result["state"], "admitted")
        self.assertEqual(provider.pushes, 1)
        stored = json.loads(
            self.store.get(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/create-result.json"
            )
        )
        self.assertEqual(stored["state"], "recovered")

    def test_crash_after_gateway_install_recovers_by_hash_without_plaintext(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.store.fail_delivery_receipt = True
        with self.assertRaisesRegex(OSError, "delivery receipt interruption"):
            self.admit(lifecycle, provider)

        provider.create_authorized = False
        recovered = self.admit(lifecycle, provider)

        self.assertEqual(recovered["state"], "admitted")
        self.assertEqual(provider.key_creates, 1)
        self.assertEqual(len(provider.deliveries), 1)
        self.assertEqual(provider.key_revocations, 0)

    def test_crash_after_delivery_receipt_reverifies_before_result(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.store.fail_gateway_result = True
        with self.assertRaisesRegex(OSError, "gateway result interruption"):
            self.admit(lifecycle, provider)

        provider.create_authorized = False
        recovered = self.admit(lifecycle, provider)

        self.assertEqual(recovered["state"], "admitted")
        self.assertEqual(provider.key_creates, 1)
        self.assertEqual(len(provider.deliveries), 1)
        self.assertEqual(provider.key_revocations, 0)

    def test_truss_timeout_recovers_only_by_exact_lookup_without_replay(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.timeout_after_create = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)

        with self.assertRaisesRegex(TimeoutError, "hard cap"):
            self.admit(lifecycle, provider)
        result = self.admit(lifecycle, provider)

        self.assertEqual(result["state"], "admitted")
        self.assertEqual(provider.pushes, 1)
        stored = json.loads(
            self.store.get(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/create-result.json"
            )
        )
        self.assertEqual(stored["state"], "recovered")

    def test_ambiguous_create_is_never_replayed(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.fail_push = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        with self.assertRaises(TimeoutError):
            self.admit(lifecycle, provider)
        result = self.admit(lifecycle, provider)
        self.assertEqual(result["state"], "create_ambiguous_no_replay")
        self.assertEqual(provider.pushes, 1)

    def test_full_admission_embeds_acceptance_and_retains_only_redacted_logs(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        result = self.admit(lifecycle, provider)
        self.assertEqual(result["state"], "admitted")
        raw = self.store.get(
            f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/gateway-result.json"
        )
        gateway_result = json.loads(raw)
        self.assertEqual(gateway_result["provider_acceptance"], self.acceptance)
        self.assertNotIn("provider_acceptance_sha256", gateway_result)
        self.assertEqual(
            gateway_result["admission"]["service_not_after"],
            utc(PROVIDER_DEADLINE),
        )
        retained = b"".join(
            self.store.get(key)
            for key in self.store.list(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
            )
        )
        delivered_key = json.loads(provider.deliveries[0])["candidate_api_key"]
        self.assertNotIn(delivered_key.encode(), retained)
        self.assertEqual(provider.guard_count, len(provider.calls))

    def test_billing_cross_check_retains_only_a_bounded_digest(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        receipt = lifecycle.billing_cross_check(
            self.acceptance,
            self.values,
            "deployment",
            provider.billing,
        )
        self.assertFalse(receipt["authoritative_for_budget"])
        retained = b"".join(
            self.store.get(key)
            for key in self.store.list(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
            )
        )
        self.assertNotIn(b"must-not-be-retained", retained)

    def test_signed_zero_route_is_required_before_bounded_deactivation(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.admit(lifecycle, provider)
        with self.assertRaisesRegex(RuntimeError, "zero-basis-point"):
            lifecycle.teardown(
                acceptance=self.acceptance,
                values=self.values,
                lookup_exact=provider.lookup,
                inspect_deployment=provider.inspect,
                update_autoscaling=provider.update,
                deactivate_exact=provider.deactivate,
                read_key_metadata=provider.key_metadata,
                delete_api_key=provider.delete_key,
            )
        canary, zero = self.route_receipts()
        result = lifecycle.teardown(
            acceptance=self.acceptance,
            values=self.values,
            lookup_exact=provider.lookup,
            inspect_deployment=provider.inspect,
            update_autoscaling=provider.update,
            deactivate_exact=provider.deactivate,
            read_key_metadata=provider.key_metadata,
            delete_api_key=provider.delete_key,
            canary_route_raw=canary,
            zero_route_raw=zero,
        )
        self.assertEqual(result["state"], "zero")
        self.assertEqual(provider.deactivations, 1)
        self.assertEqual(provider.replicas, 0)
        self.assertEqual(provider.key_revocations, 1)
        self.assertFalse(provider.candidate_keys)

    def test_gateway_candidate_key_is_removed_under_signed_zero_authority(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.store.fail_gateway_result = True
        with self.assertRaisesRegex(OSError, "gateway result interruption"):
            self.admit(lifecycle, provider)
        self.admit(lifecycle, provider)
        recovery_release = json.loads(
            self.store.get(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/"
                "candidate-key/recovery-releases/00.json"
            )
        )
        self.assertEqual(
            recovery_release["gateway_release_sha256"],
            provider.gateway_release_sha256,
        )
        canary, zero = self.route_receipts()
        gateway_result_raw = self.store.get(
            f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}/gateway-result.json"
        )
        gateway_result = json.loads(gateway_result_raw)
        authorization = {
            "schema_version": "dragontales.provider-teardown-authorization.v1",
            "scope": SCOPE,
            "student_job_id": STUDENT,
            "claim_sha256": self.claim_sha,
            "winner_result_object_key": f"{SCOPE_PREFIX}/winner-result.json",
            "winner_result_sha256": hashlib.sha256(gateway_result_raw).hexdigest(),
            "provider_acceptance_sha256": acceptance_sha256(self.acceptance),
            "run_id": self.run_id,
            "selected_provider": "baseten",
            "execution_id": DEPLOYMENT_ID,
            "trigger": {
                "kind": "route_zero",
                "retirement_object_key": f"{SCOPE_PREFIX}/retirement.json",
                "retirement_sha256": "8" * 64,
                "zero_route_revision": "b" * 64,
                "canary_route_receipt": json.loads(canary),
                "zero_route_receipt": json.loads(zero),
            },
            "authorized_at": utc(self.clock.value),
        }
        authorization_raw = gateway_line(authorization)

        removed = lifecycle.remove_candidate_key_from_gateway(
            acceptance=self.acceptance,
            values=self.values,
            gateway_cleanup_authorization_raw=authorization_raw,
            gateway_cleanup_authorization_sha256=hashlib.sha256(
                authorization_raw
            ).hexdigest(),
            canary_route_raw=canary,
            zero_route_raw=zero,
            candidate_key_sink=provider.deliver,
        )

        self.assertEqual(removed["state"], "absent")
        self.assertIsNone(provider.gateway_candidate_key_sha256)

    def test_new_reconcile_lease_can_teardown_after_original_create_lease_expires(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        create_lifecycle = self.lifecycle(provider)
        self.prepare(create_lifecycle)
        self.admit(create_lifecycle, provider)
        self.clock.value = CLAIMED + dt.timedelta(minutes=11)
        provider.calls.clear()
        provider.guard_count = 0
        provider.create_authorized = False
        reconcile_lifecycle = self.lifecycle(provider)
        canary, zero = self.route_receipts()
        result = reconcile_lifecycle.teardown(
            acceptance=self.acceptance,
            values=self.values,
            lookup_exact=provider.lookup,
            inspect_deployment=provider.inspect,
            update_autoscaling=provider.update,
            deactivate_exact=provider.deactivate,
            read_key_metadata=provider.key_metadata,
            delete_api_key=provider.delete_key,
            canary_route_raw=canary,
            zero_route_raw=zero,
        )
        self.assertEqual(result["state"], "zero")
        self.assertEqual(provider.guard_count, len(provider.calls))
        self.assertEqual(provider.key_revocations, 1)

    def test_deactivation_attempts_are_durably_bounded(self):
        provider = FakeBaseten(self, self.store, self.run_id, self.clock)
        provider.never_deactivate = True
        lifecycle = self.lifecycle(provider)
        self.prepare(lifecycle)
        self.admit(lifecycle, provider)
        canary, zero = self.route_receipts()
        with self.assertRaisesRegex(ValueError, "not inactive"):
            lifecycle.teardown(
                acceptance=self.acceptance,
                values=self.values,
                lookup_exact=provider.lookup,
                inspect_deployment=provider.inspect,
                update_autoscaling=provider.update,
                deactivate_exact=provider.deactivate,
                read_key_metadata=provider.key_metadata,
                delete_api_key=provider.delete_key,
                canary_route_raw=canary,
                zero_route_raw=zero,
            )
        self.assertEqual(provider.deactivations, baseten_winner.MAX_MUTATION_ATTEMPTS)


if __name__ == "__main__":
    unittest.main()
