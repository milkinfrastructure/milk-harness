import datetime as dt
import hashlib
import json
import tempfile
import unittest
from unittest import mock

from deploy.baseten import winner as winner_contract
from milk_harness import baseten_winner
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.jobs import (
    _selection_key,
    _winner_run_from_launch,
    BasetenJobs,
    dispatch_baseten_outboxes,
    dispatch_provider_teardowns,
    discover_provider_teardown_authorizations,
    discover_gpu_launches,
    launch_baseten_winner,
    recover_gateway_winner_result_references,
    select_winner_baseten_only,
    store_gateway_winner_result_handoff,
    teardown_baseten_winner,
    teardown_authorized_baseten_winner,
    validate_winner_result_references,
)
from milk_harness.test_jobs import (
    CAMPAIGN,
    NOW,
    PROJECT,
    SCOPE,
    SCOPE_PREFIX,
    TEST_IMAGE_ADMISSIONS,
    TEST_IMAGE_RELEASE_SHA256,
    authorize,
    control_launch,
    retained_time,
    workload,
)
from milk_harness.test_baseten_winner import (
    gateway_claim as strict_winner_gateway_claim,
)
from milk_harness.test_provider_acceptance import (
    FROZEN_GATEWAY_BASETEN_ACCEPTANCE,
)


UTC = dt.timezone.utc
TEAM = "milk"
STUDENT_BRANCH_IMAGE = TEST_IMAGE_ADMISSIONS["student-branch"]["image_reference"]


class RecordingStore:
    def __init__(self, root, events):
        self.inner = LocalEvidenceStore(root)
        self.events = events

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def create(self, key, body, content_type="application/octet-stream"):
        self.events.append(("store-create", key))
        return self.inner.create(key, body, content_type)

    def replace(self, key, body, etag, content_type="application/octet-stream"):
        self.events.append(("store-replace", key))
        return self.inner.replace(key, body, etag, content_type)




def baseten_preflight(outcome="retryable_unavailable"):
    value = {
        "schema_version": "milk.baseten-training-preflight.v1",
        "provider": "baseten",
        "team_name": TEAM,
        "project_id": PROJECT,
        "outcome": outcome,
        "evidence_sha256": "8" * 64,
        "observed_at": "2026-08-27T19:58:00Z",
    }
    if outcome != "ready":
        value.update(reason="server_unavailable", status=503)
    return value




def create_authorities():
    authorization = {
        "schema_version": "milk.provider-create-authorization.v1",
        "campaign_id": CAMPAIGN,
        "scope_prefix": SCOPE_PREFIX,
        "owner_id": "1" * 64,
        "holder_id": "2" * 64,
        "lease_revision": 1,
        "lease_acquired_at": "2026-08-27T19:55:00Z",
        "lease_expires_at": "2026-08-27T21:00:00Z",
        "evidence_store_identity_sha256": "3" * 64,
        "create_authority_store_location_sha256": "4" * 64,
        "requested": True,
        "confirmation_valid": True,
        "gateway_gate_granted": True,
        "configuration_verified": True,
        "granted": True,
        "confirmation_sha256": "5" * 64,
    }
    authorization_raw = canonical_json(authorization)
    claim = {
        "schema_version": "milk.provider-jobs-pass-claim.v1",
        "campaign_id": CAMPAIGN,
        "scope_prefix": SCOPE_PREFIX,
        "owner_id": authorization["owner_id"],
        "holder_id": authorization["holder_id"],
        "lease_revision": authorization["lease_revision"],
        "lease_expires_at": authorization["lease_expires_at"],
        "lease_token_sha256": "6" * 64,
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
        "claimed_at": "2026-08-27T19:57:00Z",
    }
    return canonical_json(claim), authorization_raw


def teardown_record(store, run_id, selected_provider):
    result_raw = store.get(
        f"campaigns/v1/{CAMPAIGN}/winner-jobs/{run_id}/gateway-result.json"
    )
    result = json.loads(result_raw)
    result_raw = canonical_json(result)
    acceptance = result["provider_acceptance"]
    authorization = {
        "schema_version": "dragontales.provider-teardown-authorization.v1",
        "scope": result["scope"],
        "student_job_id": result["student_job_id"],
        "claim_sha256": result["claim_sha256"],
        "winner_result_object_key": "control/winner-result.json",
        "winner_result_sha256": hashlib.sha256(result_raw).hexdigest(),
        "provider_acceptance_sha256": (
            baseten_winner.provider_acceptance_sha256(acceptance)
        ),
        "run_id": run_id,
        "selected_provider": selected_provider,
        "execution_id": result["admission"]["execution_id"],
        "trigger": {
            "kind": "service_expired",
            "service_not_after": result["admission"]["service_not_after"],
        },
        "authorized_at": result["admission"]["service_not_after"],
    }
    authorization_raw = canonical_json(authorization)
    return {
        "authorization_sha256": hashlib.sha256(
            authorization_raw
        ).hexdigest(),
        "authorization_raw": authorization_raw,
        "authorization": authorization,
        "winner_result_raw": result_raw,
        "winner_result": result,
        "gateway_claim_raw": None,
        "canary_route_raw": None,
        "zero_route_raw": None,
    }










class BasetenWinnerLifecycle:
    def __init__(self, store, launch, run_id, events):
        self.store = store
        self.launch = launch
        self.run_id = run_id
        self.events = events
        self.campaign_id = CAMPAIGN
        self.team_name = TEAM

    def prepare(self, *, acceptance, **unused):
        prefix = f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
        self.store.get(
            f"campaigns/v1/{CAMPAIGN}/reservation-intents/{self.run_id}.json"
        )
        self.events.append(("winner-prepare", self.run_id))
        self.store.create(
            f"{prefix}/provider-acceptance.json",
            baseten_winner.provider_acceptance_bytes(acceptance),
            "application/json",
        )
        self.store.create(
            f"{prefix}/gateway-claim.json",
            self.launch["gateway_claim_raw"],
            "application/json",
        )
        self.store.create(
            f"{prefix}/deployment-definition.json",
            canonical_json(
                {
                    "schema_version": "milk.test-winner-definition.v1",
                    "run_id": self.run_id,
                    "provider_acceptance_sha256": (
                        baseten_winner.provider_acceptance_sha256(acceptance)
                    ),
                }
            ),
            "application/json",
        )
        return {"state": "prepared"}

    def admit(self, *, acceptance, observed_cost_microusd, **unused):
        prefix = f"campaigns/v1/{CAMPAIGN}/winner-jobs/{self.run_id}"
        self.store.get(f"{prefix}/provider-acceptance.json")
        self.store.get(f"{prefix}/deployment-definition.json")
        self.events.append(("provider-create", self.run_id))
        operation = self.launch["operation"]
        admitted = NOW + dt.timedelta(minutes=2)
        admission = {
            "schema_version": winner_contract.RECEIPT_SCHEMA,
            "provider": "baseten",
            "student_job_id": operation["student_job_id"],
            "student_variant": operation["winner"],
            "model_manifest_sha256": "4" * 64,
            "model_alias": "milk-student",
            "model_alias_sha256": "5" * 64,
            "candidate_api_key_sha256": "6" * 64,
            "student_branch_runtime_image_reference": self.launch[
                "runtime_image_reference"
            ],
            "admission_program_sha256": "7" * 64,
            "execution_id": "deployment_1",
            "execution_name": "milk-winner",
            "chat_completions_url": (
                "https://model.example.invalid/v1/chat/completions"
            ),
            "models_response_sha256": "8" * 64,
            "chat_request_sha256": "9" * 64,
            "chat_response_sha256": "a" * 64,
            "launch_started_at": NOW.isoformat().replace("+00:00", "Z"),
            "ready_at": (NOW + dt.timedelta(minutes=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "admitted_at": admitted.isoformat().replace("+00:00", "Z"),
            "service_not_after": (admitted + dt.timedelta(minutes=15))
            .isoformat()
            .replace("+00:00", "Z"),
        }
        result = {
            "schema_version": winner_contract.RESULT_SCHEMA,
            "scope": self.launch["launch_source"]["scope"],
            "student_job_id": operation["student_job_id"],
            "claim_sha256": self.launch["launch_source"]["claim_sha256"],
            "provider_binding_sha256": operation[
                "provider_binding_sha256"
            ],
            "provider_acceptance": acceptance,
            "observed_cost_microusd": observed_cost_microusd,
            "admission": admission,
        }
        self.store.create(
            f"{prefix}/gateway-result.json",
            winner_contract.result_bytes(result),
            "application/json",
        )
        self.store.create(
            f"{prefix}/candidate-key/deliveries/00.json",
            canonical_json(
                {
                    "schema_version": (
                        "milk.baseten-candidate-key-delivery-receipt.v1"
                    ),
                    "provider_acceptance_sha256": (
                        baseten_winner.provider_acceptance_sha256(acceptance)
                    ),
                    "run_id": self.run_id,
                    "provider": "baseten",
                    "team_name": TEAM,
                    "model_id": "model1",
                    "key_name": baseten_winner._candidate_key_name(
                        self.run_id,
                        0,
                    ),
                    "key_prefix": "candidate",
                    "candidate_key_sha256": "6" * 64,
                    "payload_sha256": "b" * 64,
                    "payload_bytes": 256,
                    "gateway_release_id": (
                        "11111111-1111-4111-8111-111111111111"
                    ),
                    "gateway_release_sha256": "c" * 64,
                    "acknowledgement_sha256": "d" * 64,
                    "delivered_at": admitted.isoformat().replace("+00:00", "Z"),
                    "state": "delivered",
                }
            ),
            "application/json",
        )
        return {"state": "admitted"}


class LaterLeaseBasetenWinnerLifecycle:
    def __init__(self):
        self.calls = []

    def remove_candidate_key_from_gateway(self, **unused):
        return {"state": "absent"}

    def teardown(self, *, acceptance, values, canary_route_raw, zero_route_raw):
        self.calls.append(
            (
                acceptance["run_id"],
                values,
                canary_route_raw,
                zero_route_raw,
            )
        )
        return {"state": "zero"}




class UnavailableBasetenWinnerLifecycle:
    def __init__(self, events=None):
        self.calls = 0
        self.events = events

    def preflight(self):
        self.calls += 1
        if self.events is not None:
            self.events.append(("baseten-winner-preflight", None))
        return baseten_winner.baseten_unavailable_preflight_receipt(
            TEAM,
            "capability_unavailable",
            None,
            NOW,
        )


class ReadOnlyControlStore:
    def __init__(self, inner):
        self.inner = inner

    def get(self, *args, **kwargs):
        return self.inner.get(*args, **kwargs)

    def list(self, *args, **kwargs):
        return self.inner.list(*args, **kwargs)

    def create(self, *unused, **unused_keywords):
        raise AssertionError("jobs attempted to mutate gateway control")

    def replace(self, *unused, **unused_keywords):
        raise AssertionError("jobs attempted to mutate gateway control")


class JobsProviderIntegrationTest(unittest.TestCase):
    @staticmethod
    def _winner_launch(control, nonce):
        control_launch(control, "student_winner_deployment", nonce)
        launches = discover_gpu_launches(control, SCOPE_PREFIX, NOW)
        winners = [
            launch
            for launch in launches
            if launch["operation"]["kind"]
            == "student_winner_deployment"
        ]
        if len(winners) != 1:
            raise AssertionError("winner fixture was not discovered exactly once")
        return winners[0]

    @staticmethod
    def _strict_winner_launch(control):
        gateway_launch_raw, claim_raw = strict_winner_gateway_claim()
        gateway_launch = json.loads(gateway_launch_raw)
        claim = json.loads(claim_raw)
        claim["scope"] = dict(SCOPE)
        gateway_launch["deployment_claim_object_key"] = (
            f"{SCOPE_PREFIX}/jobs/student/{claim['student_job_id']}/"
            "winner-deployment/claim.json"
        )
        for field in ("model_manifest", "dev_receipt"):
            claim[field]["object_key"] = (
                f"{SCOPE_PREFIX}/artifacts/{claim['student_job_id']}/"
                f"{claim[field]['sha256']}"
            )
        provider_binding = hashlib.sha256(
            b"dragontales.winner-provider-binding.v1\0"
            + json.dumps(
                claim["authority"],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        claim["provider_binding_sha256"] = provider_binding
        gateway_launch["provider_binding_sha256"] = provider_binding
        claim_raw = json.dumps(claim, separators=(",", ":")).encode()
        claim_sha256 = hashlib.sha256(claim_raw).hexdigest()
        gateway_launch["deployment_claim_sha256"] = claim_sha256
        gateway_launch_raw = json.dumps(
            gateway_launch,
            separators=(",", ":"),
        ).encode()
        operation = {
            "kind": "student_winner_deployment",
            "student_job_id": claim["student_job_id"],
            "student_result_sha256": claim["student_result_sha256"],
            "winner": claim["winner"],
            "provider_binding_sha256": claim[
                "provider_binding_sha256"
            ],
            "provider_policy": claim["authority"]["provider_policy"],
            "max_wall_seconds": claim["authority"]["max_wall_seconds"],
            "max_cost_microusd": claim["authority"]["max_cost_microusd"],
        }
        claim_key = gateway_launch["deployment_claim_object_key"]
        outbox_key = claim_key.removesuffix("claim.json") + "launch-outbox.json"
        outbox = {
            "schema_version": "dragontales.gpu-launch-outbox.v1",
            "scope": claim["scope"],
            "dispatch_id": claim_sha256,
            "claim_object_key": claim_key,
            "claim_sha256": claim_sha256,
            "runtime_image_reference": claim[
                "student_branch_runtime_image_reference"
            ],
            "created_at": claim["claimed_at"],
            "expires_at": claim["expires_at"],
            "operation": operation,
        }
        outbox_raw = json.dumps(outbox, separators=(",", ":")).encode()
        expires = dt.datetime.fromisoformat(
            claim["expires_at"].replace("Z", "+00:00")
        )
        frontier = {
            "schema_version": "dragontales.gpu-launch-frontier.v1",
            "scope": claim["scope"],
            "dispatch_id": claim_sha256,
            "outbox_object_key": outbox_key,
            "outbox_sha256": hashlib.sha256(outbox_raw).hexdigest(),
            "expires_at": claim["expires_at"],
        }
        frontier_key = (
            f"{SCOPE_PREFIX}/frontier/gpu-launch/{retained_time(expires)}/"
            f"{claim_sha256}.json"
        )
        for key, raw in (
            (claim_key, claim_raw),
            (outbox_key, outbox_raw),
            (frontier_key, json.dumps(frontier, separators=(",", ":")).encode()),
        ):
            if not control.create(key, raw, "application/json"):
                raise AssertionError("strict winner control fixture collided")
        launches = discover_gpu_launches(control, SCOPE_PREFIX, NOW)
        if len(launches) != 1:
            raise AssertionError("strict winner fixture was not discovered once")
        return launches[0]





    def test_baseten_winner_orders_authority_and_emits_content_free_handoff(self):
        with tempfile.TemporaryDirectory() as root:
            events = []
            store = RecordingStore(root, events)
            control = LocalEvidenceStore(f"{root}/control")
            authorize(store)
            launch = self._strict_winner_launch(control)
            run_id = _winner_run_from_launch(
                CAMPAIGN,
                launch,
                TEST_IMAGE_RELEASE_SHA256,
                TEST_IMAGE_ADMISSIONS["student-branch"]["sha256"],
            )
            ready = baseten_winner.baseten_preflight_receipt(
                TEAM,
                "team_1",
                "1" * 64,
                "2" * 64,
                "3" * 64,
                NOW,
            )
            selection = select_winner_baseten_only(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                launch_source=launch["launch_source"],
                baseten_team_name=TEAM,
                baseten_preflight=lambda: ready,
            )
            provider_pass_raw, authorization_raw = create_authorities()
            lifecycle = BasetenWinnerLifecycle(
                store,
                launch,
                run_id,
                events,
            )
            result = launch_baseten_winner(
                lifecycle,
                store=store,
                campaign_id=CAMPAIGN,
                launch=launch,
                selection_record=selection,
                image_release_sha256=TEST_IMAGE_RELEASE_SHA256,
                image_admission_sha256=TEST_IMAGE_ADMISSIONS["student-branch"][
                    "sha256"
                ],
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                values={"runtime": "fixed"},
                observed_cost_microusd=1234,
                now=lambda: NOW,
            )
            self.assertEqual(result["state"], "admitted")
            self.assertEqual(len(result["winner_result_references"]), 1)
            self.assertEqual(len(result["candidate_key_delivery_references"]), 1)
            self.assertEqual(
                recover_gateway_winner_result_references(
                    store,
                    ReadOnlyControlStore(control),
                    campaign_id=CAMPAIGN,
                    scope_prefix=SCOPE_PREFIX,
                    image_release_sha256=TEST_IMAGE_RELEASE_SHA256,
                    image_admission_sha256=TEST_IMAGE_ADMISSIONS["student-branch"][
                        "sha256"
                    ],
                    current=NOW,
                ),
                result["winner_result_references"],
            )
            reference = result["winner_result_references"][0]
            self.assertEqual(
                tuple(reference),
                (
                    "student_job_id",
                    "claim_sha256",
                    "object_key",
                    "object_sha256",
                    "bytes",
                ),
            )
            raw = store.get(reference["object_key"])
            embedded = json.loads(raw)["provider_acceptance"]
            self.assertEqual(
                baseten_winner.provider_acceptance_bytes(embedded),
                store.get(
                    f"campaigns/v1/{CAMPAIGN}/winner-jobs/{run_id}/"
                    "provider-acceptance.json"
                ),
            )
            selection_index = next(
                index
                for index, event in enumerate(events)
                if event
                == ("store-create", _selection_key(CAMPAIGN, run_id))
            )
            reservation_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and "/reservation-intents/" in event[1]
            )
            acceptance_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and event[1].endswith(
                    "/winner-jobs/"
                    + run_id
                    + "/provider-acceptance.json"
                )
            )
            definition_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and event[1].endswith("/deployment-definition.json")
            )
            mutation_index = events.index(("provider-create", run_id))
            self.assertLess(selection_index, reservation_index)
            self.assertLess(reservation_index, acceptance_index)
            self.assertLess(acceptance_index, definition_index)
            self.assertLess(definition_index, mutation_index)

            later = LaterLeaseBasetenWinnerLifecycle()
            teardown = teardown_baseten_winner(
                later,
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                values={"runtime": "fixed"},
                canary_route_raw=b"canary\n",
                zero_route_raw=b"zero\n",
            )
            self.assertEqual(teardown["state"], "zero")
            self.assertEqual(later.calls[0][0], run_id)
            authorized_later = LaterLeaseBasetenWinnerLifecycle()
            authorized_teardown = teardown_authorized_baseten_winner(
                authorized_later,
                store=store,
                campaign_id=CAMPAIGN,
                authorization_record=teardown_record(
                    store,
                    run_id,
                    "baseten",
                ),
                values={"runtime": "fixed"},
            )
            self.assertEqual(authorized_teardown["state"], "zero")
            self.assertEqual(authorized_later.calls[0][0], run_id)


    def test_gateway_control_is_strictly_read_only(self):
        with tempfile.TemporaryDirectory() as root:
            control = LocalEvidenceStore(root)
            self._winner_launch(control, 73)
            launches = discover_gpu_launches(
                ReadOnlyControlStore(control),
                SCOPE_PREFIX,
                NOW,
            )
            self.assertEqual(len(launches), 1)
            self.assertEqual(
                launches[0]["operation"]["kind"],
                "student_winner_deployment",
            )

    def test_production_teardown_discovery_rejects_modal_authority(self):
        with tempfile.TemporaryDirectory() as root:
            control = LocalEvidenceStore(root)
            student_job_id = "b" * 64
            winner_key = (
                f"{SCOPE_PREFIX}/jobs/student/{student_job_id}/"
                "winner-deployment/result.json"
            )
            authorization = {
                "schema_version": (
                    "dragontales.provider-teardown-authorization.v1"
                ),
                "scope": SCOPE,
                "student_job_id": student_job_id,
                "claim_sha256": "1" * 64,
                "winner_result_object_key": winner_key,
                "winner_result_sha256": "2" * 64,
                "provider_acceptance_sha256": "3" * 64,
                "run_id": "4" * 64,
                "selected_provider": "modal",
                "execution_id": "legacy-modal-winner",
                "trigger": {
                    "kind": "service_expired",
                    "service_not_after": "2026-08-27T20:17:00Z",
                },
                "authorized_at": "2026-08-27T20:17:00Z",
            }
            authorization_raw = json.dumps(
                authorization,
                separators=(",", ":"),
            ).encode()
            authorization_key = (
                f"{SCOPE_PREFIX}/jobs/student/{student_job_id}/"
                "winner-deployment/teardown-authorization.json"
            )
            control.create(
                authorization_key,
                authorization_raw,
                "application/json",
            )
            frontier = {
                "schema_version": "dragontales.provider-teardown-frontier.v1",
                "scope": SCOPE,
                "student_job_id": student_job_id,
                "authorization_object_key": authorization_key,
                "authorization_sha256": hashlib.sha256(
                    authorization_raw
                ).hexdigest(),
            }
            control.create(
                (
                    f"{SCOPE_PREFIX}/frontier/provider-teardown/"
                    f"{student_job_id}.json"
                ),
                json.dumps(frontier, separators=(",", ":")).encode(),
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "teardown authorization"):
                discover_provider_teardown_authorizations(
                    ReadOnlyControlStore(control),
                    SCOPE_PREFIX,
                    dt.datetime(2026, 8, 27, 20, 18, tzinfo=UTC),
                )

    def test_baseten_hold_is_side_effect_free_then_dispatches_all_operations(self):
        with tempfile.TemporaryDirectory() as root:
            events = []
            store = RecordingStore(f"{root}/evidence", events)
            control = LocalEvidenceStore(f"{root}/control")
            authorize(store)
            control_launch(control, "teacher_run", 79)
            control_launch(control, "student_train_merge", 80)
            control_launch(control, "student_fanout", 82)
            control_launch(control, "student_winner_deployment", 81)
            settings = {
                "student_train_image": TEST_IMAGE_ADMISSIONS["student-train"][
                    "image_reference"
                ],
                "student_branch_image": TEST_IMAGE_ADMISSIONS["student-branch"][
                    "image_reference"
                ],
                "teacher_image": TEST_IMAGE_ADMISSIONS["teacher-gpt-oss"][
                    "image_reference"
                ],
                "jobs_image": (
                    "ghcr.io/milkinfrastructure/milk-jobs@sha256:"
                    + "4" * 64
                ),
                "image_release_sha256": TEST_IMAGE_RELEASE_SHA256,
                "student_train_image_admission_sha256": TEST_IMAGE_ADMISSIONS[
                    "student-train"
                ]["sha256"],
                "student_branch_image_admission_sha256": TEST_IMAGE_ADMISSIONS[
                    "student-branch"
                ]["sha256"],
                "teacher_image_admission_sha256": TEST_IMAGE_ADMISSIONS[
                    "teacher-gpt-oss"
                ]["sha256"],
                "registry_secret": "milk-registry",
                "config_secret": "milk-config",
                "capture_store_account_id": "capture-account",
                "capture_store_identity_sha256": "5" * 64,
                "capture_store_access_key_secret": "capture-access",
                "capture_store_secret_key_secret": "capture-secret",
                "capture_store_session_token_secret": None,
                "control_store_account_id": "control-account",
                "control_store_identity_sha256": "6" * 64,
                "control_store_access_key_secret": "control-access",
                "control_store_secret_key_secret": "control-secret",
                "control_store_session_token_secret": None,
            }

            class NoNetworkTransport:
                def request(self, *unused, **unused_keywords):
                    raise AssertionError("provider network call escaped test seam")

            jobs = BasetenJobs(
                store=store,
                campaign_id=CAMPAIGN,
                project_id=PROJECT,
                transport=NoNetworkTransport(),
                now=lambda: NOW,
            )
            ordinary_preflights = []

            def unavailable_training(team_name, secret_names):
                ordinary_preflights.append(team_name)
                self.assertEqual(
                    secret_names,
                    (
                        "milk-registry",
                        "milk-config",
                        "capture-access",
                        "capture-secret",
                        "control-access",
                        "control-secret",
                    ),
                )
                events.append(("baseten-training-preflight", None))
                return {
                    **baseten_preflight(),
                    "observed_at": NOW.isoformat().replace("+00:00", "Z"),
                }

            jobs.preflight = unavailable_training
            provider_pass_raw, authorization_raw = create_authorities()
            winner_lifecycle = UnavailableBasetenWinnerLifecycle(events)

            result = dispatch_baseten_outboxes(
                jobs,
                winner_lifecycle,
                control_store=ReadOnlyControlStore(control),
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
                baseten_team_name=TEAM,
                winner_model_alias="milk-student",
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
            )
            self.assertEqual(result["verified"], 4)
            self.assertEqual(result["dispatched"], 0)
            self.assertEqual(result["held"], 4)
            self.assertEqual(result["state"], "hold")
            self.assertEqual(
                result["provider_policy"],
                {"only": "baseten"},
            )
            self.assertEqual(result["winner_result_references"], [])
            self.assertEqual(ordinary_preflights, [TEAM, TEAM, TEAM])
            self.assertEqual(winner_lifecycle.calls, 1)
            self.assertEqual(
                {hold["operation"] for hold in result["holds"]},
                {
                    "teacher_run",
                    "student_train_merge",
                    "student_fanout",
                    "student_winner_deployment",
                },
            )
            preflight_events = [
                event[0]
                for event in events
                if event[0]
                in {
                    "baseten-training-preflight",
                    "baseten-winner-preflight",
                }
            ]
            self.assertEqual(preflight_events.count("baseten-training-preflight"), 3)
            self.assertEqual(preflight_events.count("baseten-winner-preflight"), 1)
            selection_indexes = [
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and "/provider-selections/" in event[1]
                and "/preflights/" not in event[1]
            ]
            reservation_indexes = [
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and "/reservation-intents/" in event[1]
            ]
            self.assertEqual(selection_indexes, [])
            self.assertEqual(reservation_indexes, [])

            ready_events = []
            ordinary_kinds = []

            def ready_training(unused_team_name, unused_secret_names):
                ready_events.append("preflight")
                return {
                    **baseten_preflight("ready"),
                    "observed_at": NOW.isoformat().replace("+00:00", "Z"),
                }

            def ready_launch(workload_value, unused_entries, **unused):
                ready_events.append("launch")
                ordinary_kinds.append(workload_value["type"])
                return {
                    "schema_version": "milk.test-baseten-launch.v1",
                    "operation": workload_value["type"],
                    "state": "created",
                }

            def ready_winner_launch(*unused, **unused_keywords):
                ready_events.append("launch")
                return {
                    "schema_version": "milk.test-baseten-winner-launch.v1",
                    "state": "admitted",
                    "winner_result_references": [],
                    "candidate_key_delivery_references": [],
                }

            jobs.preflight = ready_training
            jobs.launch = ready_launch
            ready_winner = mock.Mock()
            ready_winner.preflight.side_effect = lambda: (
                ready_events.append("preflight")
                or baseten_winner.baseten_preflight_receipt(
                    TEAM,
                    "team_1",
                    "1" * 64,
                    "2" * 64,
                    "3" * 64,
                    NOW,
                )
            )
            with mock.patch(
                "milk_harness.jobs.launch_baseten_winner",
                side_effect=ready_winner_launch,
            ):
                ready = dispatch_baseten_outboxes(
                    jobs,
                    ready_winner,
                    control_store=ReadOnlyControlStore(control),
                    scope_prefix=SCOPE_PREFIX,
                    settings=settings,
                    baseten_team_name=TEAM,
                    winner_model_alias="milk-student",
                    provider_pass_claim_raw=provider_pass_raw,
                    create_authorization_raw=authorization_raw,
                )
            self.assertEqual(ready["verified"], 4)
            self.assertEqual(ready["dispatched"], 4)
            self.assertEqual(ready["held"], 0)
            self.assertEqual(ready["state"], "complete")
            self.assertEqual(
                set(ordinary_kinds),
                {"teacher_run", "student_train_merge", "student_fanout"},
            )
            self.assertEqual(ready_events[:4], ["preflight"] * 4)
            self.assertEqual(ready_events[4:], ["launch"] * 4)

    def test_winner_result_reference_embeds_full_canonical_acceptance(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            acceptance = json.loads(FROZEN_GATEWAY_BASETEN_ACCEPTANCE)
            admission = {
                "schema_version": winner_contract.RECEIPT_SCHEMA,
                "provider": "baseten",
                "student_job_id": "b" * 64,
                "student_variant": "static_fp8",
                "model_manifest_sha256": "4" * 64,
                "model_alias": "milk-student",
                "model_alias_sha256": "5" * 64,
                "candidate_api_key_sha256": "6" * 64,
                "student_branch_runtime_image_reference": STUDENT_BRANCH_IMAGE,
                "admission_program_sha256": "7" * 64,
                "execution_id": "deployment_1",
                "execution_name": "milk-winner",
                "chat_completions_url": (
                    "https://model.example.invalid/v1/chat/completions"
                ),
                "models_response_sha256": "8" * 64,
                "chat_request_sha256": "9" * 64,
                "chat_response_sha256": "a" * 64,
                "launch_started_at": "2026-08-27T20:00:00Z",
                "ready_at": "2026-08-27T20:01:00Z",
                "admitted_at": "2026-08-27T20:02:00Z",
                "service_not_after": "2026-08-27T20:17:00Z",
            }
            result = {
                "schema_version": winner_contract.RESULT_SCHEMA,
                "scope": {**SCOPE, "eval_id": acceptance["campaign_id"]},
                "student_job_id": "b" * 64,
                "claim_sha256": acceptance["claim_sha256"],
                "provider_binding_sha256": acceptance[
                    "provider_binding_sha256"
                ],
                "provider_acceptance": acceptance,
                "observed_cost_microusd": 0,
                "admission": admission,
            }
            raw = winner_contract.result_bytes(result)
            key = (
                f"campaigns/v1/{acceptance['campaign_id']}/winner-jobs/"
                f"{acceptance['run_id']}/gateway-result.json"
            )
            store.create(key, raw, "application/json")
            reference = store_gateway_winner_result_handoff(
                store,
                campaign_id=acceptance["campaign_id"],
                run_id=acceptance["run_id"],
            )
            self.assertEqual(
                tuple(reference),
                (
                    "student_job_id",
                    "claim_sha256",
                    "object_key",
                    "object_sha256",
                    "bytes",
                ),
            )
            self.assertEqual(reference["object_key"], key)
            self.assertEqual(reference["object_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(json.loads(store.get(key))["provider_acceptance"], acceptance)
            self.assertEqual(
                validate_winner_result_references(
                    acceptance["campaign_id"],
                    [reference],
                ),
                [reference],
            )


if __name__ == "__main__":
    unittest.main()
