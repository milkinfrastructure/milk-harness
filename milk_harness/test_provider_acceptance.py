import copy
import hashlib
import json
import unittest

from milk_harness import provider_acceptance

FROZEN_GATEWAY_BASETEN_ACCEPTANCE = (
    b'{"schema_version":"milk.winner-provider-acceptance.v1"'
    b',"campaign_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'
    b',"run_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"'
    b',"claim_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"'
    b',"outbox_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"'
    b',"provider_binding_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"'
    b',"selection":{"selected_provider":"baseten"'
    b',"provider_identity":{"provider":"baseten","team_name":"milk-production"}'
    b',"primary_preflight":{"provider":"baseten","outcome":"ready"'
    b',"evidence_sha256":"5555555555555555555555555555555555555555555555555555555555555555"'
    b',"observed_at":"2026-08-27T20:00:00Z"}}'
    b',"image_release_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"'
    b',"image_admission_sha256":"1111111111111111111111111111111111111111111111111111111111111111"'
    b',"provider_pass_claim_sha256":"2222222222222222222222222222222222222222222222222222222222222222"'
    b',"create_authorization_sha256":"3333333333333333333333333333333333333333333333333333333333333333"'
    b',"budget_reservation_sha256":"4444444444444444444444444444444444444444444444444444444444444444"'
    b',"reserved_microusd":3750000'
    b',"reserved_at":"2026-08-27T20:01:00Z"'
    b',"accepted_at":"2026-08-27T20:02:00Z"'
    b',"create_not_after":"2026-08-27T20:03:00Z"'
    b',"provider_not_after":"2026-08-27T20:32:00Z"'
    b',"max_wall_seconds":1800'
    b',"max_cost_microusd":10000000'
    b',"state":"accepted"}\n'
)


def gpu_acceptance():
    return {
        "schema_version": "milk.gpu-provider-acceptance.v1",
        "campaign_id": "1" * 64,
        "run_id": "2" * 64,
        "claim_sha256": "3" * 64,
        "outbox_sha256": "4" * 64,
        "operation": {
            "kind": "teacher_run",
            "teacher_run_id": "5" * 64,
            "provider_binding_sha256": "6" * 64,
            "slot": 0,
            "call_count": 1,
            "max_gpu_seconds": 600,
        },
        "selection": {
            "selected_provider": "baseten",
            "provider_identity": {
                "provider": "baseten",
                "team_name": "milk",
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "ready",
                "evidence_sha256": "7" * 64,
                "observed_at": "2026-08-27T12:00:00Z",
            },
        },
        "image_release_sha256": "8" * 64,
        "image_admission_sha256": "9" * 64,
        "provider_pass_claim_sha256": "a" * 64,
        "create_authorization_sha256": "b" * 64,
        "budget_reservation_sha256": "c" * 64,
        "reserved_microusd": 10_000,
        "reserved_at": "2026-08-27T12:00:01Z",
        "accepted_at": "2026-08-27T12:00:02Z",
        "create_not_after": "2026-08-27T12:01:00Z",
        "provider_not_after": "2026-08-27T12:10:00Z",
        "max_wall_seconds": 600,
        "max_cost_microusd": 20_000,
        "state": "accepted",
    }


class ProviderAcceptanceTest(unittest.TestCase):
    def test_frozen_gateway_baseten_wire_and_neutral_run_vector(self):
        acceptance = json.loads(FROZEN_GATEWAY_BASETEN_ACCEPTANCE)

        self.assertEqual(
            provider_acceptance.encode(acceptance),
            FROZEN_GATEWAY_BASETEN_ACCEPTANCE,
        )
        self.assertEqual(
            hashlib.sha256(FROZEN_GATEWAY_BASETEN_ACCEPTANCE).hexdigest(),
            "54c4871a351a20d8e3599f2a761a21bc19b95069dbb140e8c9fae2f85505b314",
        )
        self.assertEqual(
            provider_acceptance.winner_run_id(
                campaign_id=acceptance["campaign_id"],
                claim_sha256=acceptance["claim_sha256"],
                outbox_sha256=acceptance["outbox_sha256"],
                student_job_id="8" * 64,
                student_result_sha256="9" * 64,
                winner="static_fp8",
                max_wall_seconds=acceptance["max_wall_seconds"],
                max_cost_microusd=acceptance["max_cost_microusd"],
                image_release_sha256=acceptance["image_release_sha256"],
                image_admission_sha256=acceptance["image_admission_sha256"],
            ),
            "856f94e25504165c35175edbc6e576b136ca9b69c046af05b9b92c0df18d1683",
        )

    def test_winner_run_id_is_identical_before_baseten_or_modal_selection(self):
        inputs = {
            "campaign_id": "1" * 64,
            "claim_sha256": "2" * 64,
            "outbox_sha256": "3" * 64,
            "student_job_id": "4" * 64,
            "student_result_sha256": "5" * 64,
            "winner": "static_fp8",
            "max_wall_seconds": 1_800,
            "max_cost_microusd": 1_000_000,
            "image_release_sha256": "6" * 64,
            "image_admission_sha256": "7" * 64,
        }

        baseten_run = provider_acceptance.winner_run_id(**inputs)
        modal_run = provider_acceptance.winner_run_id(**dict(inputs))
        provider_derived = hashlib.sha256(
            (baseten_run + ":baseten:milk").encode()
        ).hexdigest()

        self.assertEqual(baseten_run, modal_run)
        self.assertNotEqual(baseten_run, provider_derived)

    def test_gpu_accepts_exact_baseten_primary_selection(self):
        value = gpu_acceptance()

        raw = provider_acceptance.encode(value)

        self.assertEqual(raw[-1:], b"\n")
        self.assertEqual(raw.count(b"\n"), 1)
        self.assertEqual(provider_acceptance.validate(value), value)

    def test_gpu_rejects_training_project_as_serving_identity(self):
        value = gpu_acceptance()
        value["selection"]["provider_identity"] = {
            "provider": "baseten",
            "project_id": "project_1",
        }

        with self.assertRaisesRegex(ValueError, "Baseten winner provider selection"):
            provider_acceptance.encode(value)

    def test_gpu_rejects_non_string_team_without_type_error(self):
        value = copy.deepcopy(gpu_acceptance())
        value["selection"]["provider_identity"]["team_name"] = None

        with self.assertRaises(ValueError):
            provider_acceptance.encode(value)

    def test_modal_accepts_explicit_baseten_capability_absence(self):
        value = gpu_acceptance()
        value["selection"] = {
            "selected_provider": "modal",
            "provider_identity": {
                "provider": "modal",
                "workspace_id": "ws-production",
                "workspace_name": "milk-production",
                "environment_id": "en-production",
                "environment_name": "production",
                "app_id": "ap-production",
                "app_name": "milk-gpu-jobs",
            },
            "primary_preflight": {
                "provider": "baseten",
                "outcome": "retryable_unavailable",
                "reason": "capability_unavailable",
                "status": None,
                "evidence_sha256": "d" * 64,
                "observed_at": "2026-08-27T12:00:00Z",
            },
            "fallback_preflight": {
                "provider": "modal",
                "outcome": "ready",
                "evidence_sha256": "e" * 64,
                "observed_at": "2026-08-27T12:00:01Z",
            },
        }

        self.assertEqual(provider_acceptance.validate(value), value)

        value["selection"]["primary_preflight"]["status"] = 503
        with self.assertRaisesRegex(ValueError, "fallback-safe"):
            provider_acceptance.validate(value)


if __name__ == "__main__":
    unittest.main()
