import datetime as dt
import hashlib
import json
import tempfile
import unittest
import urllib.error
from unittest import mock

from deploy.baseten import winner as winner_contract
from milk_harness import baseten_winner
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.jobs import (
    _selection_key,
    _provider_evidence_reference,
    _winner_teardown_evidence,
    _winner_run_from_launch,
    _workload_run_id,
    _workload_and_entries,
    BasetenJobs,
    dispatch_cross_provider_outboxes,
    dispatch_provider_teardowns,
    discover_provider_teardown_authorizations,
    build_provider_teardown_result,
    discover_gpu_launches,
    launch_baseten_winner,
    launch_modal_gpu,
    launch_modal_winner,
    recover_gateway_winner_result_references,
    select_baseten_primary_modal_fallback,
    select_winner_baseten_primary_modal_fallback,
    store_gateway_winner_result_handoff,
    store_gateway_teardown_result_handoff,
    teardown_baseten_winner,
    teardown_authorized_baseten_winner,
    teardown_authorized_modal_winner,
    teardown_modal_gpu,
    validate_winner_result_references,
    validate_teardown_result_references,
    provider_teardown_result_bytes,
)
from milk_harness.test_jobs import (
    CAMPAIGN,
    MODAL_APP,
    MODAL_ENVIRONMENT,
    MODAL_WORKSPACE,
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
from milk_harness.test_modal_jobs import winner_acceptance


UTC = dt.timezone.utc
TEAM = "milk"
STUDENT_TRAIN_IMAGE = TEST_IMAGE_ADMISSIONS["student-train"]["image_reference"]
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


def modal_identity():
    return {
        "provider": "modal",
        "workspace_id": MODAL_WORKSPACE,
        "workspace_name": "milk-production",
        "environment_id": "en-main",
        "environment_name": MODAL_ENVIRONMENT,
        "app_id": "ap-milk-gpu-jobs",
        "app_name": MODAL_APP,
    }


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


def modal_preflight():
    return {
        "schema_version": "milk.modal-preflight.v1",
        "provider": "modal",
        "provider_identity": modal_identity(),
        "outcome": "ready",
        "evidence_sha256": "9" * 64,
        "observed_at": "2026-08-27T19:59:00Z",
    }


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


def modal_absence_ack(record):
    winner = record["winner_result"]
    admission = winner["admission"]
    verified = dt.datetime.fromisoformat(
        record["authorization"]["authorized_at"].replace("Z", "+00:00")
    ) + dt.timedelta(seconds=1)
    return canonical_json(
        {
            "schema_version": "milk.modal-candidate-key-ack.v1",
            "run_id": record["authorization"]["run_id"],
            "selected_provider": "modal",
            "winner_admission_sha256": hashlib.sha256(
                winner_contract.receipt_bytes(admission)
            ).hexdigest(),
            "gateway_result_sha256": hashlib.sha256(
                winner_contract.result_bytes(winner)
            ).hexdigest(),
            "candidate_key_sha256": admission["candidate_api_key_sha256"],
            "service_not_after": admission["service_not_after"],
            "gateway_anchor": {
                "source_commit": "1" * 40,
                "image_admission_sha256": "2" * 64,
                "release_sha256": "3" * 64,
                "application_id": "11111111-1111-1111-1111-111111111111",
                "application_version": 1,
                "container_image": "registry.cloudflare.com/gateway:1",
                "worker_version_id": "22222222-2222-2222-2222-222222222222",
            },
            "state": "absent",
            "gateway_release_id": "33333333-3333-3333-3333-333333333333",
            "gateway_release_sha256": "4" * 64,
            "verified_at": verified.isoformat().replace("+00:00", "Z"),
        }
    )


def resources():
    return {
        "registry_secret": {
            "name": "milk-registry",
            "object_id": "st-registry",
            "required_keys": ["REGISTRY_PASSWORD", "REGISTRY_USERNAME"],
        },
        "secrets": [
            {
                "name": "milk-student-job",
                "object_id": "st-student-job",
                "required_keys": ["DRAGONTALES_CONFIG_JSON"],
            }
        ],
        "volumes": [
            {
                "name": "milk-student-model",
                "object_id": "vo-student-model",
                "mount_path": "/model",
                "read_only": True,
            }
        ],
    }


class ModalLifecycle:
    def __init__(self, store, events):
        self.store = store
        self.events = events

    def launch(self, acceptance, definition, command, environment):
        prefix = (
            f"campaigns/v1/{CAMPAIGN}/jobs/{acceptance['run_id']}/modal"
        )
        self.store.get(f"{prefix}/provider-acceptance.json")
        self.store.get(
            f"{prefix}/execution-definitions/{definition['ordinal']:02d}.json"
        )
        self.events.append(("provider-create", definition["ordinal"]))
        return {"ordinal": definition["ordinal"], "state": "created"}

    def admit_winner(
        self,
        acceptance,
        definition,
        command,
        environment,
        *,
        gateway_claim_raw,
        observed_cost_microusd=0,
    ):
        del command, environment
        claim = json.loads(gateway_claim_raw)
        admitted = NOW + dt.timedelta(minutes=2)
        admission = {
            "schema_version": winner_contract.RECEIPT_SCHEMA,
            "provider": "modal",
            "student_job_id": definition["unit"]["student_job_id"],
            "student_variant": definition["unit"]["winner"],
            "model_manifest_sha256": claim["model_manifest"]["sha256"],
            "model_alias": "milk-student",
            "model_alias_sha256": "5" * 64,
            "candidate_api_key_sha256": "6" * 64,
            "student_branch_runtime_image_reference": definition[
                "runtime_image_reference"
            ],
            "admission_program_sha256": claim["authority"][
                "admission_program_sha256"
            ],
            "execution_id": "sb-winner",
            "execution_name": "milk-winner",
            "chat_completions_url": (
                "https://unit.w.modal.host/v1/chat/completions"
            ),
            "models_response_sha256": "8" * 64,
            "chat_request_sha256": "9" * 64,
            "chat_response_sha256": "a" * 64,
            "launch_started_at": NOW.isoformat().replace("+00:00", "Z"),
            "ready_at": (NOW + dt.timedelta(minutes=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "admitted_at": admitted.isoformat().replace("+00:00", "Z"),
            "service_not_after": acceptance["provider_not_after"],
        }
        result = {
            "schema_version": winner_contract.RESULT_SCHEMA,
            "scope": claim["scope"],
            "student_job_id": definition["unit"]["student_job_id"],
            "claim_sha256": acceptance["claim_sha256"],
            "provider_binding_sha256": acceptance[
                "provider_binding_sha256"
            ],
            "provider_acceptance": acceptance,
            "observed_cost_microusd": observed_cost_microusd,
            "admission": admission,
        }
        prefix = (
            f"campaigns/v1/{acceptance['campaign_id']}/winner-jobs/"
            f"{acceptance['run_id']}"
        )
        self.store.create(
            f"{prefix}/winner-admission.json",
            winner_contract.receipt_bytes(admission),
            "application/json",
        )
        raw = winner_contract.result_bytes(result)
        self.store.create(
            f"{prefix}/gateway-result.json",
            raw,
            "application/json",
        )
        self.events.append(("winner-admit", acceptance["run_id"]))
        return {"state": "admitted"}


class LaterLeaseModalLifecycle:
    def __init__(self):
        self.calls = []

    def terminate(self, acceptance, definition, command, environment):
        self.calls.append(
            (
                acceptance["provider_pass_claim_sha256"],
                definition["ordinal"],
                command,
                environment,
            )
        )
        return {"ordinal": definition["ordinal"], "state": "zero"}


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


class LaterLeaseModalWinnerLifecycle:
    def __init__(self):
        self.calls = []

    def teardown_winner(
        self,
        acceptance,
        definition,
        command,
        environment,
        *,
        gateway_claim_raw,
        canary_route_raw,
        zero_route_raw,
    ):
        self.calls.append(
            (
                acceptance["run_id"],
                definition["unit"]["student_job_id"],
                command,
                environment,
                gateway_claim_raw,
                canary_route_raw,
                zero_route_raw,
            )
        )
        return {"state": "zero"}


class ModalFallbackWinnerLifecycle:
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

    def test_selection_precedes_reservation_acceptance_definition_and_create(self):
        with tempfile.TemporaryDirectory() as root:
            events = []
            store = RecordingStore(root, events)
            authorize(store)
            source = workload("a", "student_train_merge")
            run_id = _workload_run_id(CAMPAIGN, source)

            def primary():
                events.append(("baseten-preflight", None))
                return baseten_preflight()

            def fallback():
                events.append(("modal-preflight", None))
                return modal_preflight()

            selection = select_baseten_primary_modal_fallback(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                launch_source=source["launch_source"],
                baseten_team_name=TEAM,
                baseten_project_id=PROJECT,
                modal_identity=modal_identity(),
                baseten_preflight=primary,
                modal_preflight=fallback,
            )
            replayed_selection = select_baseten_primary_modal_fallback(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                launch_source=source["launch_source"],
                baseten_team_name=TEAM,
                baseten_project_id=PROJECT,
                modal_identity=modal_identity(),
                baseten_preflight=lambda: self.fail(
                    "immutable selection repeated Baseten preflight"
                ),
                modal_preflight=lambda: self.fail(
                    "immutable selection repeated Modal preflight"
                ),
            )
            self.assertEqual(replayed_selection, selection)
            self.assertEqual(
                tuple(replayed_selection["selection"]),
                (
                    "selected_provider",
                    "provider_identity",
                    "primary_preflight",
                    "fallback_preflight",
                ),
            )
            plan = {
                "schema_version": "milk.modal-execution-plan.v1",
                "campaign_id": CAMPAIGN,
                "run_id": run_id,
                "executions": [
                    {
                        "ordinal": 0,
                        "command": [
                            "/opt/dragontales/deploy/student-job.sh",
                            "train",
                            "--config",
                            "/tmp/dragontales/gateway.json",
                            "--student-job-id",
                            source["student_job_id"],
                            "--work-dir",
                            "/tmp/dragontales-work",
                        ],
                        "environment": {},
                        "resources": resources(),
                    }
                ],
            }
            provider_pass_raw, authorization_raw = create_authorities()
            result = launch_modal_gpu(
                ModalLifecycle(store, events),
                store=store,
                campaign_id=CAMPAIGN,
                workload=source,
                runtime_image_reference=STUDENT_TRAIN_IMAGE,
                selection_record=selection,
                execution_plan=plan,
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                now=lambda: NOW,
            )
            self.assertEqual(result["state"], "created")
            replay = launch_modal_gpu(
                ModalLifecycle(store, events),
                store=store,
                campaign_id=CAMPAIGN,
                workload=source,
                runtime_image_reference=STUDENT_TRAIN_IMAGE,
                selection_record=replayed_selection,
                execution_plan=plan,
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                now=lambda: NOW,
            )
            self.assertEqual(replay["state"], "reconcile_only")
            labels = [event[0] for event in events]
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
                and event[1].endswith("/modal/provider-acceptance.json")
            )
            definition_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and "/execution-definitions/" in event[1]
            )
            create_index = labels.index("provider-create")
            self.assertLess(labels.index("baseten-preflight"), labels.index("modal-preflight"))
            self.assertLess(selection_index, reservation_index)
            self.assertLess(reservation_index, acceptance_index)
            self.assertLess(acceptance_index, definition_index)
            self.assertLess(definition_index, create_index)

            later = LaterLeaseModalLifecycle()
            teardown = teardown_modal_gpu(
                later,
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
            )
            self.assertEqual(teardown["state"], "zero")
            self.assertEqual(len(later.calls), 1)
            self.assertEqual(
                later.calls[0][0],
                hashlib.sha256(provider_pass_raw).hexdigest(),
            )

    def test_ready_baseten_training_skips_modal_preflight(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            source = workload("b", "student_train_merge")
            selection = select_baseten_primary_modal_fallback(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=_workload_run_id(CAMPAIGN, source),
                launch_source=source["launch_source"],
                baseten_team_name=TEAM,
                baseten_project_id=PROJECT,
                modal_identity=modal_identity(),
                baseten_preflight=lambda: baseten_preflight("ready"),
                modal_preflight=lambda: self.fail(
                    "ready Baseten selection ran Modal preflight"
                ),
            )
            self.assertEqual(
                selection["selection"]["selected_provider"],
                "baseten",
            )

    def test_modal_fallback_is_forbidden_after_any_baseten_budget_intent(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            source = workload("b", "student_train_merge")
            run_id = _workload_run_id(CAMPAIGN, source)
            store.create(
                f"campaigns/v1/{CAMPAIGN}/reservation-intents/{run_id}.json",
                b"{}",
                "application/json",
            )
            called = []
            with self.assertRaisesRegex(RuntimeError, "fallback is forbidden"):
                select_baseten_primary_modal_fallback(
                    store=store,
                    campaign_id=CAMPAIGN,
                    run_id=run_id,
                    launch_source=source["launch_source"],
                    baseten_team_name=TEAM,
                    baseten_project_id=PROJECT,
                    modal_identity=modal_identity(),
                    baseten_preflight=baseten_preflight,
                    modal_preflight=lambda: called.append(True) or modal_preflight(),
                )
            self.assertEqual(called, [])

    def test_modal_winner_fallback_is_forbidden_after_baseten_create_intent(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(f"{root}/evidence")
            control = LocalEvidenceStore(f"{root}/control")
            launch = self._winner_launch(control, 71)
            run_id = _winner_run_from_launch(
                CAMPAIGN,
                launch,
                TEST_IMAGE_RELEASE_SHA256,
                TEST_IMAGE_ADMISSIONS["student-branch"]["sha256"],
            )
            store.create(
                f"campaigns/v1/{CAMPAIGN}/winner-jobs/{run_id}/create-intent.json",
                b"{}",
                "application/json",
            )
            modal_calls = []
            unavailable = baseten_winner.baseten_unavailable_preflight_receipt(
                TEAM,
                "server_unavailable",
                503,
                NOW,
            )
            with self.assertRaisesRegex(RuntimeError, "fallback is forbidden"):
                select_winner_baseten_primary_modal_fallback(
                    store=store,
                    campaign_id=CAMPAIGN,
                    run_id=run_id,
                    launch_source=launch["launch_source"],
                    baseten_team_name=TEAM,
                    modal_identity=modal_identity(),
                    baseten_preflight=lambda: unavailable,
                    modal_preflight=lambda: modal_calls.append(True),
                )
            self.assertEqual(modal_calls, [])

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
            modal_calls = []
            selection = select_winner_baseten_primary_modal_fallback(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                launch_source=launch["launch_source"],
                baseten_team_name=TEAM,
                modal_identity=modal_identity(),
                baseten_preflight=lambda: ready,
                modal_preflight=lambda: modal_calls.append(True),
            )
            self.assertEqual(modal_calls, [])
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

    def test_modal_winner_admits_and_emits_content_free_handoff(self):
        with tempfile.TemporaryDirectory() as root:
            events = []
            store = RecordingStore(root, events)
            control = LocalEvidenceStore(f"{root}/control")
            authorize(store)
            launch = self._winner_launch(control, 72)
            run_id = _winner_run_from_launch(
                CAMPAIGN,
                launch,
                TEST_IMAGE_RELEASE_SHA256,
                TEST_IMAGE_ADMISSIONS["student-branch"]["sha256"],
            )
            unavailable = baseten_winner.baseten_unavailable_preflight_receipt(
                TEAM,
                "server_unavailable",
                503,
                NOW,
            )
            selection = select_winner_baseten_primary_modal_fallback(
                store=store,
                campaign_id=CAMPAIGN,
                run_id=run_id,
                launch_source=launch["launch_source"],
                baseten_team_name=TEAM,
                modal_identity=modal_identity(),
                baseten_preflight=lambda: unavailable,
                modal_preflight=lambda: {
                    **modal_preflight(),
                    "observed_at": NOW.isoformat().replace("+00:00", "Z"),
                },
            )
            command = [
                "/bin/sh",
                "-c",
                (
                    "umask 077; printf %s \"$DRAGONTALES_CANDIDATE_API_KEY\" "
                    ">/tmp/dragontales-candidate.key; chmod 0400 "
                    "/tmp/dragontales-candidate.key; unset "
                    "DRAGONTALES_CANDIDATE_API_KEY; exec "
                    "/opt/dragontales/deploy/student-job.sh serve --model "
                    "/tmp/dragontales-winner/model --model-manifest "
                    "/tmp/dragontales-winner/model-manifest.json "
                    "--model-alias \"$DRAGONTALES_MODEL_ALIAS\" "
                    "--api-key-file /tmp/dragontales-candidate.key"
                ),
            ]
            plan = {
                "schema_version": "milk.modal-execution-plan.v1",
                "campaign_id": CAMPAIGN,
                "run_id": run_id,
                "executions": [
                    {
                        "ordinal": 0,
                        "command": command,
                        "environment": {
                            "DRAGONTALES_MODEL_ALIAS": "milk-student"
                        },
                        "resources": {
                            "registry_secret": resources()["registry_secret"],
                            "secrets": [
                                {
                                    "name": "milk-winner-candidate",
                                    "object_id": "st-winner-candidate",
                                    "required_keys": [
                                        "DRAGONTALES_CANDIDATE_API_KEY"
                                    ],
                                }
                            ],
                            "volumes": [],
                        },
                    }
                ],
            }
            provider_pass_raw, authorization_raw = create_authorities()
            result = launch_modal_winner(
                ModalLifecycle(store, events),
                store=store,
                campaign_id=CAMPAIGN,
                launch=launch,
                selection_record=selection,
                execution_plan=plan,
                image_release_sha256=TEST_IMAGE_RELEASE_SHA256,
                image_admission_sha256=TEST_IMAGE_ADMISSIONS["student-branch"][
                    "sha256"
                ],
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                now=lambda: NOW,
            )
            self.assertEqual(result["state"], "admitted")
            self.assertEqual(len(result["winner_result_references"]), 1)
            create_events = events.count(("provider-create", 0))
            replay = launch_modal_winner(
                ModalLifecycle(store, events),
                store=store,
                campaign_id=CAMPAIGN,
                launch=launch,
                selection_record=selection,
                execution_plan=plan,
                image_release_sha256=TEST_IMAGE_RELEASE_SHA256,
                image_admission_sha256=TEST_IMAGE_ADMISSIONS["student-branch"][
                    "sha256"
                ],
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                now=lambda: NOW,
            )
            self.assertEqual(
                replay["winner_result_references"],
                result["winner_result_references"],
            )
            self.assertEqual(events.count(("provider-create", 0)), create_events)
            acceptance_raw = store.get(
                f"campaigns/v1/{CAMPAIGN}/jobs/{run_id}/modal/"
                "provider-acceptance.json"
            )
            acceptance = json.loads(acceptance_raw)
            self.assertEqual(
                acceptance["selection"]["selected_provider"],
                "modal",
            )
            self.assertEqual(
                acceptance["provider_pass_claim_sha256"],
                hashlib.sha256(provider_pass_raw).hexdigest(),
            )
            definition_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "store-create"
                and "/execution-definitions/" in event[1]
            )
            mutation_index = events.index(("provider-create", 0))
            self.assertLess(definition_index, mutation_index)
            later = LaterLeaseModalWinnerLifecycle()
            record = teardown_record(store, run_id, "modal")
            with mock.patch(
                "milk_harness.jobs.discover_provider_teardown_authorizations",
                return_value=[record],
            ):
                missing_ack = mock.Mock()
                missing_ack.get.side_effect = urllib.error.HTTPError(
                    "https://control.example/absent-ack.json",
                    404,
                    "not found",
                    {},
                    None,
                )
                pending = dispatch_provider_teardowns(
                    object(),
                    later,
                    store=store,
                    control_store=missing_ack,
                    campaign_id=CAMPAIGN,
                    scope_prefix=SCOPE_PREFIX,
                    evidence_store_identity_sha256="d" * 64,
                    baseten_values_by_run_id={},
                    now=lambda: NOW,
                )
            self.assertEqual(pending["state"], "hold")
            self.assertEqual(pending["verified"], 1)
            self.assertEqual(pending["completed"], 0)
            self.assertEqual(pending["results"], [])
            self.assertEqual(pending["teardown_result_references"], [])
            self.assertEqual(later.calls, [])
            unsafe_ack = json.loads(modal_absence_ack(record))
            self.assertNotEqual(
                unsafe_ack["gateway_result_sha256"],
                record["authorization"]["winner_result_sha256"],
            )
            self.assertEqual(
                unsafe_ack["gateway_result_sha256"],
                hashlib.sha256(
                    winner_contract.result_bytes(record["winner_result"])
                ).hexdigest(),
            )
            unsafe_ack["state"] = "installed"
            with self.assertRaisesRegex(ValueError, "absence acknowledgement"):
                teardown_authorized_modal_winner(
                    later,
                    store=store,
                    campaign_id=CAMPAIGN,
                    authorization_record=record,
                    candidate_absence_raw=canonical_json(unsafe_ack),
                )
            self.assertEqual(later.calls, [])
            teardown = teardown_authorized_modal_winner(
                later,
                store=store,
                campaign_id=CAMPAIGN,
                authorization_record=record,
                candidate_absence_raw=modal_absence_ack(record),
            )
            self.assertEqual(teardown["state"], "zero")
            self.assertEqual(later.calls[0][0], run_id)
            self.assertEqual(later.calls[0][4], launch["gateway_claim_raw"])
            record = teardown_record(store, run_id, "modal")
            acceptance_sha256 = baseten_winner.provider_acceptance_sha256(
                acceptance
            )
            observed = dt.datetime.fromisoformat(
                record["authorization"]["authorized_at"].replace(
                    "Z",
                    "+00:00",
                )
            ) + dt.timedelta(minutes=1)
            observed_at = observed.isoformat().replace("+00:00", "Z")
            zero = {
                "schema_version": "milk.modal-winner-teardown-result.v1",
                "run_id": run_id,
                "provider_acceptance_sha256": acceptance_sha256,
                "zero_gpu_verification": {
                    "observed_at": observed_at,
                    "state": "zero",
                },
                "state": "zero",
            }
            winner_prefix = f"campaigns/v1/{CAMPAIGN}/winner-jobs/{run_id}"
            store.create(
                f"{winner_prefix}/modal-zero-gpu.json",
                canonical_json(zero),
                "application/json",
            )
            stream_id = "e" * 64
            modal_prefix = f"campaigns/v1/{CAMPAIGN}/jobs/{run_id}/modal"
            store.create(
                f"{modal_prefix}/winner/log-plans/{stream_id}.json",
                canonical_json({"stream_id": stream_id}),
                "application/json",
            )
            store.create(
                f"{modal_prefix}/logs/modal/{stream_id}/index.json",
                canonical_json({"stream_id": stream_id}),
                "application/json",
            )
            teardown_reference = _winner_teardown_evidence(
                store,
                campaign_id=CAMPAIGN,
                authorization_record=record,
                evidence_store_identity_sha256="d" * 64,
            )
            teardown_result = json.loads(
                store.get(teardown_reference["object_key"])
            )
            self.assertEqual(
                teardown_result["accounted_microusd"],
                acceptance["reserved_microusd"],
            )

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

    def test_teardown_authority_is_read_only_and_emits_only_evidence_reference(self):
        with tempfile.TemporaryDirectory() as root:
            control = LocalEvidenceStore(f"{root}/control")
            evidence = LocalEvidenceStore(f"{root}/evidence")
            acceptance = winner_acceptance()
            acceptance["campaign_id"] = CAMPAIGN
            student_job_id = "b" * 64
            admission = {
                "schema_version": winner_contract.RECEIPT_SCHEMA,
                "provider": "modal",
                "student_job_id": student_job_id,
                "student_variant": "static_fp8",
                "model_manifest_sha256": "4" * 64,
                "model_alias": "milk-student",
                "model_alias_sha256": "5" * 64,
                "candidate_api_key_sha256": "6" * 64,
                "student_branch_runtime_image_reference": STUDENT_BRANCH_IMAGE,
                "admission_program_sha256": "7" * 64,
                "execution_id": "sb-winner",
                "execution_name": "milk-winner",
                "chat_completions_url": (
                    "https://unit.w.modal.host/v1/chat/completions"
                ),
                "models_response_sha256": "8" * 64,
                "chat_request_sha256": "9" * 64,
                "chat_response_sha256": "a" * 64,
                "launch_started_at": "2026-08-27T20:00:00Z",
                "ready_at": "2026-08-27T20:01:00Z",
                "admitted_at": "2026-08-27T20:02:00Z",
                "service_not_after": "2026-08-27T20:17:00Z",
            }
            winner = {
                "schema_version": winner_contract.RESULT_SCHEMA,
                "scope": SCOPE,
                "student_job_id": student_job_id,
                "claim_sha256": acceptance["claim_sha256"],
                "provider_binding_sha256": acceptance[
                    "provider_binding_sha256"
                ],
                "provider_acceptance": acceptance,
                "observed_cost_microusd": 1234,
                "admission": admission,
            }
            winner_raw = winner_contract.result_bytes(winner)[:-1]
            winner_key = (
                f"{SCOPE_PREFIX}/jobs/student/{student_job_id}/"
                "winner-deployment/result.json"
            )
            control.create(winner_key, winner_raw, "application/json")
            authorization = {
                "schema_version": (
                    "dragontales.provider-teardown-authorization.v1"
                ),
                "scope": SCOPE,
                "student_job_id": student_job_id,
                "claim_sha256": acceptance["claim_sha256"],
                "winner_result_object_key": winner_key,
                "winner_result_sha256": hashlib.sha256(
                    winner_raw
                ).hexdigest(),
                "provider_acceptance_sha256": (
                    baseten_winner.provider_acceptance_sha256(acceptance)
                ),
                "run_id": acceptance["run_id"],
                "selected_provider": "modal",
                "execution_id": admission["execution_id"],
                "trigger": {
                    "kind": "service_expired",
                    "service_not_after": admission["service_not_after"],
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
            discovered = discover_provider_teardown_authorizations(
                ReadOnlyControlStore(control),
                SCOPE_PREFIX,
                dt.datetime(2026, 8, 27, 20, 18, tzinfo=UTC),
            )
            self.assertEqual(len(discovered), 1)
            store_identity = "d" * 64
            keys = {
                "zero": (
                    f"campaigns/v1/{acceptance['campaign_id']}/winner-jobs/"
                    f"{acceptance['run_id']}/zero-gpu.json"
                ),
                "logs": (
                    f"campaigns/v1/{acceptance['campaign_id']}/winner-jobs/"
                    f"{acceptance['run_id']}/logs/private-index.json"
                ),
                "settlement": (
                    f"campaigns/v1/{acceptance['campaign_id']}/settlements/"
                    f"{acceptance['run_id']}.json"
                ),
            }
            for label, key in keys.items():
                evidence.create(
                    key,
                    canonical_json({"evidence": label}),
                    "application/json",
                )
            result = build_provider_teardown_result(
                discovered[0],
                accounted_microusd=1234,
                provider_zero_evidence=_provider_evidence_reference(
                    evidence,
                    store_identity,
                    keys["zero"],
                ),
                private_log_artifact=_provider_evidence_reference(
                    evidence,
                    store_identity,
                    keys["logs"],
                ),
                budget_settlement=_provider_evidence_reference(
                    evidence,
                    store_identity,
                    keys["settlement"],
                ),
                terminated_at=dt.datetime(
                    2026,
                    8,
                    27,
                    20,
                    18,
                    tzinfo=UTC,
                ),
                verified_zero_at=dt.datetime(
                    2026,
                    8,
                    27,
                    20,
                    19,
                    tzinfo=UTC,
                ),
            )
            raw = provider_teardown_result_bytes(result)
            self.assertTrue(raw.endswith(b"\n"))
            reference = store_gateway_teardown_result_handoff(
                evidence,
                campaign_id=acceptance["campaign_id"],
                result=result,
            )
            self.assertEqual(
                validate_teardown_result_references(
                    acceptance["campaign_id"],
                    [reference],
                ),
                [reference],
            )
            self.assertNotIn("execution_id", reference)
            self.assertNotIn("provider", reference)

    def test_cross_provider_dispatch_selects_every_run_before_any_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            events = []
            store = RecordingStore(f"{root}/evidence", events)
            control = LocalEvidenceStore(f"{root}/control")
            authorize(store)
            control_launch(control, "student_train_merge", 80)
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
            launches = discover_gpu_launches(control, SCOPE_PREFIX, NOW)
            plans = {}
            winner_values = {}
            for launch in launches:
                if (
                    launch["operation"]["kind"]
                    == "student_winner_deployment"
                ):
                    run_id = _winner_run_from_launch(
                        CAMPAIGN,
                        launch,
                        TEST_IMAGE_RELEASE_SHA256,
                        TEST_IMAGE_ADMISSIONS["student-branch"]["sha256"],
                    )
                    student_job_id = launch["operation"]["student_job_id"]
                    command = [
                        "/bin/sh",
                        "-c",
                        (
                            "set -eu; umask 077; mkdir -m 0700 -p "
                            "/tmp/dragontales /tmp/dragontales-winner; "
                            "printf %s \"$DRAGONTALES_CONFIG_JSON\" >"
                            "/tmp/dragontales/gateway.json; chmod 0400 "
                            "/tmp/dragontales/gateway.json; printf %s "
                            "\"$DRAGONTALES_CANDIDATE_API_KEY\" >"
                            "/tmp/dragontales-candidate.key; chmod 0400 "
                            "/tmp/dragontales-candidate.key; "
                            "/usr/local/bin/dragontales-gateway --config "
                            "/tmp/dragontales/gateway.json "
                            "materialize-student-winner --student-job-id "
                            "\"$DRAGONTALES_STUDENT_JOB_ID\" --stage-dir "
                            "/tmp/dragontales-winner >"
                            "/tmp/dragontales/materialize.stdout; unset "
                            "DRAGONTALES_CONFIG_JSON "
                            "DRAGONTALES_CANDIDATE_API_KEY "
                            "MILK_CONTROL_STORE_ACCESS_KEY_ID "
                            "MILK_CONTROL_STORE_SECRET_ACCESS_KEY; cat "
                            "/tmp/dragontales/materialize.stdout; exec "
                            "/opt/dragontales/deploy/student-job.sh serve "
                            "--model /tmp/dragontales-winner/model "
                            "--model-manifest /tmp/dragontales-winner/"
                            "model-manifest.json --model-alias "
                            "\"$DRAGONTALES_MODEL_ALIAS\" --api-key-file "
                            "/tmp/dragontales-candidate.key"
                        ),
                    ]
                    plans[run_id] = {
                        "schema_version": "milk.modal-execution-plan.v1",
                        "campaign_id": CAMPAIGN,
                        "run_id": run_id,
                        "executions": [
                            {
                                "ordinal": 0,
                                "command": command,
                                "environment": {
                                    "DRAGONTALES_MODEL_ALIAS": "milk-student",
                                    "DRAGONTALES_STUDENT_JOB_ID": student_job_id,
                                },
                                "resources": {
                                    "registry_secret": resources()[
                                        "registry_secret"
                                    ],
                                    "secrets": [
                                        {
                                            "name": "milk-config",
                                            "object_id": "st-config",
                                            "required_keys": [
                                                "DRAGONTALES_CONFIG_JSON"
                                            ],
                                        },
                                        {
                                            "name": "milk-control",
                                            "object_id": "st-control",
                                            "required_keys": [
                                                "MILK_CONTROL_STORE_ACCESS_KEY_ID",
                                                "MILK_CONTROL_STORE_SECRET_ACCESS_KEY",
                                            ],
                                        },
                                        {
                                            "name": "milk-winner-candidate",
                                            "object_id": "st-winner-candidate",
                                            "required_keys": [
                                                "DRAGONTALES_CANDIDATE_API_KEY"
                                            ],
                                        },
                                    ],
                                    "volumes": [],
                                },
                            }
                        ],
                    }
                    winner_values[run_id] = winner_contract.settings(
                        launch["runtime_image_reference"],
                        student_job_id,
                        "milk-student",
                        "DOCKER_REGISTRY_ghcr.io",
                        "gateway_config",
                        "control-account",
                        "6" * 64,
                        "control_access",
                        "control_secret",
                    )
                else:
                    workload_value, unused_entries = _workload_and_entries(
                        launch,
                        settings,
                    )
                    del unused_entries
                    run_id = _workload_run_id(CAMPAIGN, workload_value)
                    plans[run_id] = {
                        "schema_version": "milk.modal-execution-plan.v1",
                        "campaign_id": CAMPAIGN,
                        "run_id": run_id,
                        "executions": [
                            {
                                "ordinal": 0,
                                "command": ["/bin/true"],
                                "environment": {},
                                "resources": resources(),
                            }
                        ],
                    }
            provider_pass_raw, authorization_raw = create_authorities()
            winner_lifecycle = ModalFallbackWinnerLifecycle(events)

            def ready_modal():
                events.append(("modal-preflight", None))
                return {
                    **modal_preflight(),
                    "observed_at": NOW.isoformat().replace("+00:00", "Z"),
                }

            result = dispatch_cross_provider_outboxes(
                jobs,
                ModalLifecycle(store, events),
                winner_lifecycle,
                control_store=ReadOnlyControlStore(control),
                scope_prefix=SCOPE_PREFIX,
                settings=settings,
                baseten_team_name=TEAM,
                modal_identity=modal_identity(),
                modal_preflight=ready_modal,
                provider_pass_claim_raw=provider_pass_raw,
                create_authorization_raw=authorization_raw,
                modal_plans_by_run_id=plans,
                winner_values_by_run_id=winner_values,
            )
            self.assertEqual(result["verified"], 2)
            self.assertEqual(result["dispatched"], 2)
            self.assertEqual(
                result["provider_policy"],
                {"primary": "baseten", "fallback": "modal"},
            )
            self.assertEqual(len(result["winner_result_references"]), 1)
            self.assertEqual(ordinary_preflights, [TEAM])
            self.assertEqual(winner_lifecycle.calls, 1)
            preflight_events = [
                event[0]
                for event in events
                if event[0]
                in {
                    "baseten-training-preflight",
                    "baseten-winner-preflight",
                    "modal-preflight",
                }
            ]
            self.assertEqual(len(preflight_events), 4)
            self.assertEqual(preflight_events[1::2], ["modal-preflight"] * 2)
            self.assertTrue(
                all(
                    event.startswith("baseten-")
                    for event in preflight_events[::2]
                )
            )
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
            self.assertEqual(len(selection_indexes), 2)
            self.assertEqual(len(reservation_indexes), 2)
            self.assertLess(max(selection_indexes), min(reservation_indexes))

    def test_winner_result_reference_embeds_full_canonical_acceptance(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            acceptance = winner_acceptance()
            acceptance["campaign_id"] = CAMPAIGN
            admission = {
                "schema_version": winner_contract.RECEIPT_SCHEMA,
                "provider": "modal",
                "student_job_id": "b" * 64,
                "student_variant": "static_fp8",
                "model_manifest_sha256": "4" * 64,
                "model_alias": "milk-student",
                "model_alias_sha256": "5" * 64,
                "candidate_api_key_sha256": "6" * 64,
                "student_branch_runtime_image_reference": STUDENT_BRANCH_IMAGE,
                "admission_program_sha256": "7" * 64,
                "execution_id": "sb-winner",
                "execution_name": "milk-winner",
                "chat_completions_url": (
                    "https://unit.w.modal.host/v1/chat/completions"
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
                "scope": SCOPE,
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
