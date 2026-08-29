import datetime as dt
import hashlib
import json
import tempfile
import threading
import unittest

from milk_harness.budget import (
    ABSOLUTE_CEILING_MICROUSD,
    ACTIVE_CAMPAIGN_AUTHORITY_KEY,
    ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY,
    ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY,
    BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE,
    BASETEN_H100_PRICING_SOURCE,
    BASETEN_PRICING_OBSERVATION_PREFIX,
    BASETEN_PRICING_RECEIPT_PREFIX,
    BASETEN_PRICING_SOURCE_BODY_PREFIX,
    H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
    LAUNCH_CUTOFF_MICROUSD,
    PRICING_RECEIPT_MAX_AGE_SECONDS,
    TEARDOWN_RESERVE_MICROUSD,
    CampaignBudget,
    baseten_pricing_observation,
    baseten_provider_identity,
    baseten_serving_provider_identity,
    prepare_campaign_authority,
    refresh_baseten_pricing,
)
from milk_harness.evidence import LocalEvidenceStore, canonical_json, create_same


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 27, 20, 0, 0, tzinfo=UTC)
CAMPAIGN = "c" * 64
PROVIDER_RATE_MICROUSD_PER_MINUTE = (
    BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE
)
PROJECT = "project_123"
TEAM = "milk-infrastructure"
BASETEN_IDENTITY = baseten_provider_identity(PROJECT)
BASETEN_SERVING_IDENTITY = baseten_serving_provider_identity(TEAM)
BASETEN_PRICING_SOURCE_BODY = (
    b"https://docs.baseten.co/deployment/resources\nH100 $0.10833/min\n"
)
PREPARATION_DEADLINE = int((NOW + dt.timedelta(minutes=1)).timestamp())
PROVIDER_DEADLINE = int((NOW + dt.timedelta(hours=1)).timestamp())


def baseten_pricing_observation_raw(
    observed_at=NOW,
    *,
    campaign_id=CAMPAIGN,
    source_body=BASETEN_PRICING_SOURCE_BODY,
):
    return canonical_json(
        baseten_pricing_observation(
            campaign_id,
            hashlib.sha256(source_body).hexdigest(),
            observed_at,
        )
    )


def reserve(budget, run_id, amount_microusd):
    preparation_sha256 = hashlib.sha256(run_id.encode()).hexdigest()
    budget.prepare(
        run_id,
        preparation_sha256,
        PREPARATION_DEADLINE,
        PROVIDER_DEADLINE,
    )
    return budget.reserve(run_id, amount_microusd, preparation_sha256)


class CampaignBudgetTests(unittest.TestCase):
    def authorize(self, store, campaign_id=CAMPAIGN, observed_at=NOW, now=NOW):
        return prepare_campaign_authority(
            store,
            campaign_id,
            PROJECT,
            TEAM,
            baseten_pricing_observation_raw=baseten_pricing_observation_raw(
                observed_at,
                campaign_id=campaign_id,
            ),
            baseten_pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
            now=lambda: now,
        )

    def budget(self, root):
        store = LocalEvidenceStore(root)
        self.authorize(store)
        budget = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
        budget.initialize()
        return budget

    def test_initialize_requires_singleton_current_authority(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            budget = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            with self.assertRaisesRegex(ValueError, "authority is missing"):
                budget.initialize()
            self.assertEqual(self.authorize(store), "created")
            self.assertEqual(self.authorize(store), "existing")
            authority = json.loads(store.get(ACTIVE_CAMPAIGN_AUTHORITY_KEY))
            self.assertEqual(
                authority,
                {
                    "schema_version": "milk.active-campaign-authority.v4",
                    "campaign_id": CAMPAIGN,
                    "baseten_project_id": PROJECT,
                    "baseten_team_name": TEAM,
                    "absolute_ceiling_microusd": ABSOLUTE_CEILING_MICROUSD,
                    "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
                    "teardown_reserve_microusd": TEARDOWN_RESERVE_MICROUSD,
                },
            )
            pricing = json.loads(
                store.get(ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY)
            )
            self.assertEqual(
                pricing,
                {
                    "schema_version": "milk.baseten-h100-pricing-receipt.v4",
                    "campaign_id": CAMPAIGN,
                    "provider_role": "training",
                    "provider_identity": BASETEN_IDENTITY,
                    "service": "training",
                    "gpu_type": "H100",
                    "unit": "gpu_minute",
                    "source_url": BASETEN_H100_PRICING_SOURCE,
                    "source_observation_sha256": hashlib.sha256(
                        baseten_pricing_observation_raw()
                    ).hexdigest(),
                    "observed_at": "2026-08-27T20:00:00Z",
                    "provider_rate_microusd_per_minute": PROVIDER_RATE_MICROUSD_PER_MINUTE,
                    "reservation_rate_microusd_per_minute": H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
                    "max_age_seconds": PRICING_RECEIPT_MAX_AGE_SECONDS,
                },
            )
            serving = json.loads(
                store.get(ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY)
            )
            self.assertEqual(serving["provider_role"], "serving")
            self.assertEqual(serving["provider_identity"], BASETEN_SERVING_IDENTITY)
            self.assertEqual(serving["service"], "dedicated_serving")
            self.assertEqual(
                serving["provider_rate_microusd_per_minute"],
                PROVIDER_RATE_MICROUSD_PER_MINUTE,
            )
            self.assertEqual(
                store.list("state/v1"),
                [
                    ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY,
                    ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY,
                ],
            )
            budget.initialize()
            self.assertEqual(
                json.loads(store.get(f"campaigns/v1/{CAMPAIGN}/policy.json")),
                {
                    "schema_version": "milk.campaign-budget-policy.v4",
                    "campaign_id": CAMPAIGN,
                    "baseten_project_id": PROJECT,
                    "baseten_team_name": TEAM,
                    "absolute_ceiling_microusd": ABSOLUTE_CEILING_MICROUSD,
                    "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
                    "teardown_reserve_microusd": TEARDOWN_RESERVE_MICROUSD,
                },
            )

    def test_modal_identity_and_authority_fields_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            with self.assertRaisesRegex(ValueError, "Modal.*not authorized"):
                CampaignBudget(
                    store,
                    CAMPAIGN,
                    {
                        "provider": "modal",
                        "workspace_id": "ws_123",
                        "environment_name": "main",
                        "app_name": "milk-gpu-jobs",
                    },
                    now=lambda: NOW,
                )
            authority_raw, etag = store.get_versioned(
                ACTIVE_CAMPAIGN_AUTHORITY_KEY
            )
            authority = json.loads(authority_raw)
            authority["modal_workspace_id"] = "ws_123"
            self.assertTrue(
                store.replace(
                    ACTIVE_CAMPAIGN_AUTHORITY_KEY,
                    canonical_json(authority),
                    etag,
                    "application/json",
                )
            )
            with self.assertRaisesRegex(ValueError, "authority is invalid"):
                CampaignBudget(
                    store,
                    CAMPAIGN,
                    BASETEN_IDENTITY,
                    now=lambda: NOW,
                ).initialize()

    def test_provider_switch_is_allowed_only_before_reservation_intent(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            training = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            training.initialize()
            serving.initialize()
            run_id = "a" * 64
            preparation_sha256 = hashlib.sha256(run_id.encode()).hexdigest()
            self.assertEqual(
                training.prepare(
                    run_id,
                    preparation_sha256,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                ),
                "created",
            )
            self.assertEqual(
                serving.prepare(
                    run_id,
                    preparation_sha256,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                ),
                "switched",
            )
            reservation = serving.reserve(
                run_id,
                100_000_000,
                preparation_sha256,
            )
            self.assertEqual(
                reservation["provider_identity"],
                BASETEN_SERVING_IDENTITY,
            )
            with self.assertRaisesRegex(ValueError, "provider is frozen"):
                training.prepare(
                    run_id,
                    preparation_sha256,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                )

    def test_serving_role_identity_and_price_are_frozen(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            training = CampaignBudget(
                store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW
            )
            serving = CampaignBudget(
                store, CAMPAIGN, BASETEN_SERVING_IDENTITY, now=lambda: NOW
            )
            training.initialize()
            serving.initialize()
            run_id = "9" * 64
            preparation = hashlib.sha256(run_id.encode()).hexdigest()
            self.assertEqual(
                training.prepare(
                    run_id,
                    preparation,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                ),
                "created",
            )
            self.assertEqual(
                serving.prepare(
                    run_id,
                    preparation,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                ),
                "switched",
            )
            pricing_raw = store.get(
                ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY
            )
            reservation = serving.reserve(run_id, 100_000_000, preparation)
            self.assertEqual(reservation["provider_role"], "serving")
            self.assertEqual(
                reservation["provider_identity"], BASETEN_SERVING_IDENTITY
            )
            self.assertEqual(
                reservation["pricing_receipt_sha256"],
                hashlib.sha256(pricing_raw).hexdigest(),
            )
            self.assertEqual(
                reservation["provider_rate_microusd_per_minute"],
                PROVIDER_RATE_MICROUSD_PER_MINUTE,
            )
            with self.assertRaisesRegex(ValueError, "provider is frozen"):
                training.prepare(
                    run_id,
                    preparation,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                )
            with self.assertRaisesRegex(ValueError, "different provider"):
                training.settle(run_id, 90_000_000)
            settlement = serving.settle(run_id, 90_000_000)
            self.assertEqual(settlement["provider_role"], "serving")
            self.assertEqual(
                serving.finalize(run_id)["provider_role"], "serving"
            )

    def test_serving_rejects_cross_role_and_forged_pricing_receipts(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            serving = CampaignBudget(
                store, CAMPAIGN, BASETEN_SERVING_IDENTITY, now=lambda: NOW
            )
            serving.initialize()
            unused_raw, etag = store.get_versioned(
                ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY
            )
            del unused_raw
            self.assertTrue(
                store.replace(
                    ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY,
                    store.get(ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY),
                    etag,
                    "application/json",
                )
            )
            run_id = "8" * 64
            preparation = hashlib.sha256(run_id.encode()).hexdigest()
            serving.prepare(
                run_id,
                preparation,
                PREPARATION_DEADLINE,
                PROVIDER_DEADLINE,
            )
            with self.assertRaisesRegex(ValueError, "wrong provider identity"):
                serving.reserve(run_id, 1, preparation)

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            raw, etag = store.get_versioned(
                ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY
            )
            forged = json.loads(raw)
            forged["provider_rate_microusd_per_minute"] -= 1
            forged_raw = canonical_json(forged)
            forged_sha256 = hashlib.sha256(forged_raw).hexdigest()
            create_same(
                store,
                f"{BASETEN_PRICING_RECEIPT_PREFIX}/{forged_sha256}.json",
                forged_raw,
                "application/json",
            )
            self.assertTrue(
                store.replace(
                    ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY,
                    forged_raw,
                    etag,
                    "application/json",
                )
            )
            serving = CampaignBudget(
                store, CAMPAIGN, BASETEN_SERVING_IDENTITY, now=lambda: NOW
            )
            serving.initialize()
            run_id = "7" * 64
            preparation = hashlib.sha256(run_id.encode()).hexdigest()
            serving.prepare(
                run_id,
                preparation,
                PREPARATION_DEADLINE,
                PROVIDER_DEADLINE,
            )
            with self.assertRaisesRegex(ValueError, "pricing receipt is invalid"):
                serving.reserve(run_id, 1, preparation)

    def test_provider_race_is_resolved_by_first_immutable_intent(self):
        class PauseIntentCreate:
            def __init__(self, inner):
                self.inner = inner
                self.before = threading.Event()
                self.release_create = threading.Event()
                self.created = threading.Event()
                self.release_return = threading.Event()

            def __getattr__(self, name):
                return getattr(self.inner, name)

            def create(self, key, body, content_type="application/octet-stream"):
                if key.endswith(f"/reservation-intents/{'a' * 64}.json"):
                    self.before.set()
                    if not self.release_create.wait(5):
                        raise RuntimeError("intent create was not released")
                    result = self.inner.create(key, body, content_type)
                    self.created.set()
                    if not self.release_return.wait(5):
                        raise RuntimeError("intent return was not released")
                    return result
                return self.inner.create(key, body, content_type)

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            paused_store = PauseIntentCreate(store)
            serving = CampaignBudget(
                paused_store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            training = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            serving.initialize()
            training.initialize()
            run_id = "a" * 64
            preparation_sha256 = hashlib.sha256(run_id.encode()).hexdigest()
            serving.prepare(
                run_id,
                preparation_sha256,
                PREPARATION_DEADLINE,
                PROVIDER_DEADLINE,
            )
            results = []
            failures = []

            def reserve_serving():
                try:
                    results.append(
                        serving.reserve(
                            run_id,
                            100_000_000,
                            preparation_sha256,
                        )
                    )
                except Exception as error:
                    failures.append(error)

            thread = threading.Thread(target=reserve_serving)
            thread.start()
            self.assertTrue(paused_store.before.wait(5))
            self.assertEqual(
                training.prepare(
                    run_id,
                    preparation_sha256,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                ),
                "switched",
            )
            paused_store.release_create.set()
            self.assertTrue(paused_store.created.wait(5))
            with self.assertRaisesRegex(
                ValueError,
                "different reservation intent",
            ):
                training.reserve(run_id, 100_000_000, preparation_sha256)
            paused_store.release_return.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(len(results), 1)
            self.assertEqual(
                results[0]["provider_identity"], BASETEN_SERVING_IDENTITY
            )
            status = serving.status()
            self.assertEqual(status["reserved_microusd"], 100_000_000)
            self.assertEqual(
                status["outstanding"][run_id]["provider_identity"],
                BASETEN_SERVING_IDENTITY,
            )

    def test_training_and_serving_pricing_are_role_local(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            serving.initialize()
            original = reserve(serving, "a" * 64, 100_000_000)
            stale_now = NOW + dt.timedelta(
                seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1
            )
            stale_serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: stale_now,
            )
            stale_serving.initialize()
            preparation_sha256 = hashlib.sha256(("b" * 64).encode()).hexdigest()
            stale_serving.prepare(
                "b" * 64,
                preparation_sha256,
                int((stale_now + dt.timedelta(minutes=1)).timestamp()),
                int((stale_now + dt.timedelta(hours=1)).timestamp()),
            )
            with self.assertRaisesRegex(ValueError, "stale"):
                stale_serving.reserve("b" * 64, 1, preparation_sha256)
            settlement = stale_serving.settle("a" * 64, 90_000_000)
            self.assertEqual(
                settlement["pricing_receipt_sha256"],
                original["pricing_receipt_sha256"],
            )
            stale_serving.finalize("a" * 64)

            refresh_baseten_pricing(
                store,
                CAMPAIGN,
                "training",
                pricing_observation_raw=baseten_pricing_observation_raw(
                    stale_now,
                ),
                pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                now=lambda: stale_now,
            )
            training = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: stale_now,
            )
            training.initialize()
            training_preparation = hashlib.sha256(("c" * 64).encode()).hexdigest()
            training.prepare(
                "c" * 64,
                training_preparation,
                int((stale_now + dt.timedelta(minutes=1)).timestamp()),
                int((stale_now + dt.timedelta(hours=1)).timestamp()),
            )
            self.assertEqual(
                training.reserve("c" * 64, 1, training_preparation)[
                    "provider_identity"
                ],
                BASETEN_IDENTITY,
            )
            with self.assertRaisesRegex(ValueError, "stale"):
                stale_serving.reserve("b" * 64, 1, preparation_sha256)

            later = stale_now + dt.timedelta(seconds=1)
            self.assertEqual(
                refresh_baseten_pricing(
                    store,
                    CAMPAIGN,
                    "serving",
                    pricing_observation_raw=baseten_pricing_observation_raw(later),
                    pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                    now=lambda: later,
                ),
                "updated",
            )
            refreshed_serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: later,
            )
            refreshed_serving.initialize()
            self.assertEqual(
                refreshed_serving.reserve("b" * 64, 1, preparation_sha256)[
                    "provider_identity"
                ],
                BASETEN_SERVING_IDENTITY,
            )

    def test_v4_keys_roles_and_price_provenance_are_exact(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            budget = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            head = budget.initialize()
            self.assertEqual(head["schema_version"], "milk.campaign-budget-head.v4")
            self.assertEqual(head["committed_microusd"], 0)
            self.assertNotIn("spent_microusd", head)
            self.assertEqual(
                json.loads(
                    store.get(
                        f"state/v1/campaigns/{CAMPAIGN}/budget-head.json"
                    )
                ),
                head,
            )
            with self.assertRaises(FileNotFoundError):
                store.get(f"campaigns/v1/{CAMPAIGN}/head.json")
            with self.assertRaises(FileNotFoundError):
                store.get("authority/v1/active-baseten-h100-pricing.json")

            pricing_raw = store.get(
                ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY
            )
            pricing_sha256 = hashlib.sha256(pricing_raw).hexdigest()
            self.assertEqual(
                store.get(f"{BASETEN_PRICING_RECEIPT_PREFIX}/{pricing_sha256}.json"),
                pricing_raw,
            )
            reservation = reserve(budget, "a" * 64, 100_000_000)
            self.assertEqual(
                reservation["schema_version"],
                "milk.campaign-budget-reservation.v4",
            )
            self.assertEqual(reservation["pricing_receipt_sha256"], pricing_sha256)
            self.assertEqual(
                reservation["provider_rate_microusd_per_minute"],
                PROVIDER_RATE_MICROUSD_PER_MINUTE,
            )
            self.assertEqual(
                reservation["reservation_rate_microusd_per_minute"],
                H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
            )
            reservation_intent = json.loads(
                store.get(
                    f"campaigns/v1/{CAMPAIGN}/reservation-intents/{'a' * 64}.json"
                )
            )
            self.assertEqual(
                reservation_intent["schema_version"],
                "milk.campaign-budget-reservation-intent.v4",
            )
            self.assertEqual(
                reservation_intent["pricing_receipt_sha256"], pricing_sha256
            )

            settlement = budget.settle("a" * 64, 90_000_000)
            self.assertEqual(
                settlement["schema_version"],
                "milk.campaign-budget-settlement.v4",
            )
            self.assertEqual(settlement["accounted_microusd"], 90_000_000)
            self.assertNotIn("actual_microusd", settlement)
            self.assertEqual(settlement["pricing_receipt_sha256"], pricing_sha256)
            settlement_intent = json.loads(
                store.get(
                    f"campaigns/v1/{CAMPAIGN}/settlement-intents/{'a' * 64}.json"
                )
            )
            self.assertEqual(
                settlement_intent["schema_version"],
                "milk.campaign-budget-settlement-intent.v4",
            )
            self.assertEqual(
                settlement_intent["pricing_receipt_sha256"], pricing_sha256
            )
            finalization = budget.finalize("a" * 64)
            self.assertEqual(
                finalization["schema_version"],
                "milk.campaign-budget-finalization.v4",
            )
            self.assertEqual(budget.status()["committed_microusd"], 90_000_000)

    def test_reservation_intent_replay_keeps_original_price_binding(self):
        class CrashBeforeBudgetReplace:
            def __init__(self, inner):
                self.inner = inner
                self.armed = True

            def __getattr__(self, name):
                return getattr(self.inner, name)

            def replace(self, key, body, etag, content_type="application/octet-stream"):
                if self.armed and key.endswith("/budget-head.json"):
                    self.armed = False
                    raise RuntimeError("simulated crash before reservation CAS")
                return self.inner.replace(key, body, etag, content_type)

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            budget = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            budget.initialize()
            run_id = "a" * 64
            preparation_sha256 = hashlib.sha256(run_id.encode()).hexdigest()
            budget.prepare(
                run_id,
                preparation_sha256,
                PREPARATION_DEADLINE,
                PROVIDER_DEADLINE,
            )
            old_pricing_sha256 = hashlib.sha256(
                store.get(ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY)
            ).hexdigest()
            crashing = CampaignBudget(
                CrashBeforeBudgetReplace(store),
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            crashing.initialize()
            with self.assertRaisesRegex(RuntimeError, "before reservation CAS"):
                crashing.reserve(run_id, 100_000_000, preparation_sha256)

            later = NOW + dt.timedelta(seconds=1)
            refresh_baseten_pricing(
                store,
                CAMPAIGN,
                "serving",
                pricing_observation_raw=baseten_pricing_observation_raw(later),
                pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                now=lambda: later,
            )
            repaired = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: later,
            )
            repaired.initialize()
            receipt = repaired.reserve(
                run_id,
                100_000_000,
                preparation_sha256,
            )
            self.assertEqual(receipt["pricing_receipt_sha256"], old_pricing_sha256)
            self.assertEqual(
                receipt["provider_rate_microusd_per_minute"],
                PROVIDER_RATE_MICROUSD_PER_MINUTE,
            )
            settlement = repaired.settle(run_id, 90_000_000)
            self.assertEqual(
                settlement["pricing_receipt_sha256"], old_pricing_sha256
            )
            self.assertEqual(
                settlement["provider_rate_microusd_per_minute"],
                PROVIDER_RATE_MICROUSD_PER_MINUTE,
            )

    def test_v1_budget_head_and_settlement_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            budget = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
            budget.initialize()
            head_key = f"state/v1/campaigns/{CAMPAIGN}/budget-head.json"
            raw, etag = store.get_versioned(head_key)
            head = json.loads(raw)
            head["schema_version"] = "milk.campaign-budget-head.v1"
            head["spent_microusd"] = head.pop("committed_microusd")
            self.assertTrue(
                store.replace(
                    head_key,
                    json.dumps(head, separators=(",", ":")).encode(),
                    etag,
                    "application/json",
                )
            )
            with self.assertRaisesRegex(ValueError, "invalid fields"):
                budget.status()

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            budget = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
            budget.initialize()
            reserve(budget, "a" * 64, 100_000_000)
            create_same(
                store,
                f"campaigns/v1/{CAMPAIGN}/settlements/{'a' * 64}.json",
                json.dumps(
                    {
                        "schema_version": "milk.campaign-budget-settlement.v1",
                        "campaign_id": CAMPAIGN,
                        "run_id": "a" * 64,
                        "reserved_microusd": 100_000_000,
                        "actual_microusd": 90_000_000,
                        "intent_sha256": "1" * 64,
                        "state": "settled",
                    },
                    separators=(",", ":"),
                ).encode(),
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "settlement is invalid"):
                budget.settlement("a" * 64)

    def test_arbitrary_campaign_cannot_reset_store_budget(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            first = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
            first.initialize()
            reserve(first, "a" * 64, 800_000_000)
            other_campaign = "d" * 64
            with self.assertRaisesRegex(ValueError, "different campaign"):
                CampaignBudget(
                    store,
                    other_campaign,
                    BASETEN_IDENTITY,
                    now=lambda: NOW,
                ).initialize()
            with self.assertRaisesRegex(ValueError, "existing evidence object differs"):
                self.authorize(store, other_campaign)
            with self.assertRaises(FileNotFoundError):
                store.get(
                    f"state/v1/campaigns/{other_campaign}/budget-head.json"
                )
            self.assertEqual(first.status()["reserved_microusd"], 800_000_000)

    def test_authority_is_bound_to_one_provider_project(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            with self.assertRaisesRegex(ValueError, "differs from campaign authority"):
                CampaignBudget(
                    store,
                    CAMPAIGN,
                    baseten_provider_identity("another_project"),
                    now=lambda: NOW,
                ).initialize()
            with self.assertRaisesRegex(ValueError, "pricing role is invalid"):
                refresh_baseten_pricing(
                    store,
                    CAMPAIGN,
                    "wrong-role",
                    pricing_observation_raw=baseten_pricing_observation_raw(
                        NOW + dt.timedelta(seconds=1),
                    ),
                    pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                    now=lambda: NOW + dt.timedelta(seconds=1),
                )
            with self.assertRaisesRegex(ValueError, "differs from campaign authority"):
                CampaignBudget(
                    store,
                    CAMPAIGN,
                    baseten_serving_provider_identity("another-team"),
                    now=lambda: NOW,
                ).initialize()

    def test_stale_future_and_invalid_authority_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            with self.assertRaisesRegex(ValueError, "stale"):
                self.authorize(
                    store,
                    observed_at=NOW,
                    now=NOW + dt.timedelta(seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1),
                )
            with self.assertRaisesRegex(ValueError, "future"):
                self.authorize(store, observed_at=NOW + dt.timedelta(seconds=1))
            with self.assertRaisesRegex(ValueError, "team name is invalid"):
                prepare_campaign_authority(
                    store,
                    CAMPAIGN,
                    PROJECT,
                    "-invalid",
                    baseten_pricing_observation_raw=(
                        baseten_pricing_observation_raw()
                    ),
                    baseten_pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                    now=lambda: NOW,
                )
            self.assertEqual(store.list("authority/v1"), [])

            self.authorize(store)
            stale = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW
                + dt.timedelta(seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1),
            )
            stale.initialize()
            future = NOW + dt.timedelta(
                seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1
            )
            preparation_sha256 = hashlib.sha256(("f" * 64).encode()).hexdigest()
            stale.prepare(
                "f" * 64,
                preparation_sha256,
                int((future + dt.timedelta(minutes=1)).timestamp()),
                int((future + dt.timedelta(hours=1)).timestamp()),
            )
            with self.assertRaisesRegex(ValueError, "stale"):
                stale.reserve("f" * 64, 1, preparation_sha256)
            self.assertEqual(stale.status()["open"], {})
            self.assertEqual(
                refresh_baseten_pricing(
                    store,
                    CAMPAIGN,
                    "training",
                    pricing_observation_raw=baseten_pricing_observation_raw(
                        NOW
                        + dt.timedelta(
                            seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1,
                        ),
                    ),
                    pricing_source_body=BASETEN_PRICING_SOURCE_BODY,
                    now=lambda: NOW
                    + dt.timedelta(seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1),
                ),
                "updated",
            )
            stale.reserve("f" * 64, 1, preparation_sha256)

    def test_stale_pricing_blocks_new_work_but_not_teardown(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            live = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
            live.initialize()
            reserve(live, "a" * 64, 100_000_000)

            stale_now = NOW + dt.timedelta(
                seconds=PRICING_RECEIPT_MAX_AGE_SECONDS + 1
            )
            stale = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: stale_now,
            )
            stale.initialize()
            preparation_sha256 = hashlib.sha256(b"b" * 64).hexdigest()
            self.assertEqual(
                stale.prepare(
                    "b" * 64,
                    preparation_sha256,
                    int((stale_now + dt.timedelta(minutes=1)).timestamp()),
                    int((stale_now + dt.timedelta(hours=1)).timestamp()),
                ),
                "created",
            )
            with self.assertRaisesRegex(ValueError, "stale"):
                stale.reserve("b" * 64, 1, preparation_sha256)
            settlement = stale.settle("a" * 64, 90_000_000)
            self.assertEqual(settlement["state"], "settled")
            self.assertEqual(stale.finalize("a" * 64)["state"], "committed")
            self.assertEqual(
                set(stale.status()["outstanding"]),
                {"b" * 64},
            )

    def test_reserve_settle_and_no_replay(self):
        with tempfile.TemporaryDirectory() as root:
            budget = self.budget(root)
            reservation = reserve(budget, "a" * 64, 400_000_000)
            self.assertEqual(reservation["state"], "reserved")
            self.assertEqual(reserve(budget, "a" * 64, 400_000_000)["state"], "reserved")
            with self.assertRaisesRegex(ValueError, "differ"):
                reserve(budget, "a" * 64, 1)
            settlement = budget.settle("a" * 64, 350_000_000)
            self.assertEqual(settlement["state"], "settled")
            self.assertEqual(budget.settle("a" * 64, 350_000_000), settlement)
            with self.assertRaisesRegex(ValueError, "settled"):
                reserve(budget, "a" * 64, 400_000_000)
            status = budget.status()
            self.assertEqual(status["committed_microusd"], 350_000_000)
            self.assertEqual(status["reserved_microusd"], 0)
            budget.finalize("a" * 64)
            with self.assertRaisesRegex(ValueError, "settled run"):
                reserve(budget, "a" * 64, 400_000_000)
            self.assertEqual(budget.status()["outstanding"], {})

    def test_launch_cutoff_keeps_teardown_reserve(self):
        with tempfile.TemporaryDirectory() as root:
            budget = self.budget(root)
            self.assertEqual(reserve(budget, "a" * 64, 800_000_000)["state"], "reserved")
            blocked = reserve(budget, "b" * 64, 50_000_001)
            self.assertEqual(blocked["state"], "blocked")
            self.assertEqual(blocked["launch_cutoff_microusd"], 850_000_000)

    def test_concurrent_reservations_cannot_cross_cutoff(self):
        with tempfile.TemporaryDirectory() as root:
            budget = self.budget(root)
            barrier = threading.Barrier(3)
            results = []
            failures = []

            def reserve_one(run_id):
                try:
                    barrier.wait()
                    results.append(reserve(budget, run_id, 600_000_000))
                except Exception as error:
                    failures.append(error)

            threads = [
                threading.Thread(target=reserve_one, args=("a" * 64,)),
                threading.Thread(target=reserve_one, args=("b" * 64,)),
            ]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()
            self.assertEqual(failures, [])
            self.assertEqual(sorted(result["state"] for result in results), ["blocked", "reserved"])
            status = budget.status()
            self.assertEqual(status["reserved_microusd"], 600_000_000)
            self.assertEqual(len(status["open"]), 1)

    def test_training_and_serving_share_one_concurrent_cutoff(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            training = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            training.initialize()
            serving.initialize()
            budgets = (
                (training, "a" * 64),
                (serving, "b" * 64),
            )
            preparations = {}
            for budget, run_id in budgets:
                preparation = hashlib.sha256(run_id.encode()).hexdigest()
                preparations[run_id] = preparation
                budget.prepare(
                    run_id,
                    preparation,
                    PREPARATION_DEADLINE,
                    PROVIDER_DEADLINE,
                )
            barrier = threading.Barrier(3)
            results = []
            failures = []

            def reserve_one(budget, run_id):
                try:
                    barrier.wait()
                    results.append(
                        budget.reserve(
                            run_id,
                            500_000_000,
                            preparations[run_id],
                        )
                    )
                except Exception as error:
                    failures.append(error)

            threads = [
                threading.Thread(target=reserve_one, args=item)
                for item in budgets
            ]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()
            self.assertEqual(failures, [])
            self.assertEqual(
                sorted(result["state"] for result in results),
                ["blocked", "reserved"],
            )
            self.assertEqual(training.status()["reserved_microusd"], 500_000_000)

    def test_exact_mechanics_reservation_uses_one_shared_head(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            training = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            serving = CampaignBudget(
                store,
                CAMPAIGN,
                BASETEN_SERVING_IDENTITY,
                now=lambda: NOW,
            )
            training.initialize()
            serving.initialize()
            for budget, run_id, amount_microusd in (
                (training, "1" * 64, 120_000_000),
                (training, "2" * 64, 3_750_000),
                (training, "3" * 64, 11_250_000),
                (serving, "4" * 64, 7_500_000),
            ):
                self.assertEqual(
                    reserve(budget, run_id, amount_microusd)["state"],
                    "reserved",
                )
            self.assertEqual(
                training.status()["reserved_microusd"],
                142_500_000,
            )
            self.assertEqual(serving.status(), training.status())

    def test_reservation_receipt_is_stable_after_another_head_revision(self):
        with tempfile.TemporaryDirectory() as root:
            budget = self.budget(root)
            first = reserve(budget, "a" * 64, 100_000_000)
            reserve(budget, "b" * 64, 100_000_000)
            self.assertEqual(reserve(budget, "a" * 64, 100_000_000), first)

    def test_reservation_and_settlement_repair_after_committed_cas_crash(self):
        class CrashOnce:
            def __init__(self, inner):
                self.inner = inner
                self.armed = True

            def __getattr__(self, name):
                return getattr(self.inner, name)

            def replace(self, *arguments, **keywords):
                replaced = self.inner.replace(*arguments, **keywords)
                if replaced and self.armed:
                    self.armed = False
                    raise RuntimeError("simulated crash after committed CAS")
                return replaced

        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.authorize(store)
            budget = CampaignBudget(store, CAMPAIGN, BASETEN_IDENTITY, now=lambda: NOW)
            budget.initialize()
            preparation_sha256 = hashlib.sha256(("a" * 64).encode()).hexdigest()
            budget.prepare(
                "a" * 64,
                preparation_sha256,
                PREPARATION_DEADLINE,
                PROVIDER_DEADLINE,
            )
            crashing = CampaignBudget(
                CrashOnce(store),
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            crashing.initialize()
            with self.assertRaisesRegex(RuntimeError, "simulated"):
                crashing.reserve("a" * 64, 400_000_000, preparation_sha256)
            repaired = budget.reserve("a" * 64, 400_000_000, preparation_sha256)
            self.assertEqual(repaired["state"], "reserved")
            crashing = CampaignBudget(
                CrashOnce(store),
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            crashing.initialize()
            with self.assertRaisesRegex(RuntimeError, "simulated"):
                crashing.settle("a" * 64, 350_000_000)
            repaired = budget.settle("a" * 64, 350_000_000)
            self.assertEqual(repaired["state"], "settled")
            status = budget.status()
            self.assertEqual(status["open"], {})
            self.assertIn("a" * 64, status["pending_settlements"])
            self.assertEqual(status["committed_microusd"], 350_000_000)

            crashing = CampaignBudget(
                CrashOnce(store),
                CAMPAIGN,
                BASETEN_IDENTITY,
                now=lambda: NOW,
            )
            crashing.initialize()
            with self.assertRaisesRegex(RuntimeError, "simulated"):
                crashing.finalize("a" * 64)
            finalization = budget.finalize("a" * 64)
            self.assertEqual(finalization["state"], "committed")
            self.assertEqual(budget.finalize("a" * 64), finalization)
            self.assertEqual(budget.status()["pending_settlements"], {})
            self.assertEqual(budget.settle("a" * 64, 350_000_000), repaired)
            self.assertEqual(budget.status()["pending_settlements"], {})


if __name__ == "__main__":
    unittest.main()
