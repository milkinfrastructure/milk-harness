import contextlib
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import milk_harness.budget_operator as budget_operator
from milk_harness.budget import (
    ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY,
    ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY,
    ACTIVE_CAMPAIGN_AUTHORITY_KEY,
    ACTIVE_MODAL_PRICING_RECEIPT_KEY,
    BASETEN_PRICING_OBSERVATION_PREFIX,
    BASETEN_PRICING_RECEIPT_PREFIX,
    BASETEN_PRICING_SOURCE_BODY_PREFIX,
    MODAL_PRICING_RECEIPT_PREFIX,
    MODAL_RATES_SOURCE_BODY_PREFIX,
    MODAL_RATES_SOURCE_COMMAND,
    CampaignBudget,
    baseten_pricing_observation,
    baseten_provider_identity,
    modal_provider_identity,
)
from milk_harness.evidence import LocalEvidenceStore, canonical_json


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 27, 20, 0, 0, tzinfo=UTC)
CAMPAIGN = "c" * 64
PROJECT = "project_123"
TEAM = "milk-infrastructure"
MODAL_WORKSPACE = "ws_123"
MODAL_ENVIRONMENT = "main"
MODAL_APP = "milk-gpu-jobs"
BASETEN_SOURCE_BODY = (
    b"https://docs.baseten.co/deployment/resources\nH100 $0.10833/min\n"
)
MODAL_SOURCE_BODY = (
    b'{"gpu":{"H100":"0.001097"},"sandbox_cpu":"0.00003942",'
    b'"sandbox_memory":"0.00000667"}\n'
)


def operator_input(observed_at=NOW, source_body=BASETEN_SOURCE_BODY):
    observed = observed_at.isoformat(timespec="seconds").replace("+00:00", "Z")
    return canonical_json(
        {
            "schema_version": "milk.budget-authority-operator-input.v1",
            "campaign_id": CAMPAIGN,
            "baseten_project_id": PROJECT,
            "baseten_team_name": TEAM,
            "baseten_pricing_observation": baseten_pricing_observation(
                CAMPAIGN,
                hashlib.sha256(source_body).hexdigest(),
                observed_at,
            ),
            "modal_rates_receipt": {
                "schema_version": "milk.modal-workspace-rates.v1",
                "campaign_id": CAMPAIGN,
                "provider_identity": modal_provider_identity(
                    MODAL_WORKSPACE,
                    MODAL_ENVIRONMENT,
                    MODAL_APP,
                ),
                "source_command": MODAL_RATES_SOURCE_COMMAND,
                "source_sha256": hashlib.sha256(MODAL_SOURCE_BODY).hexdigest(),
                "observed_at": observed,
                "currency": "USD",
                "rate_unit": "second",
                "h100_usd_per_second": "0.001097",
                "sandbox_cpu_usd_per_physical_core_second": "0.00003942",
                "sandbox_memory_usd_per_gib_second": "0.00000667",
                "region_mode": "default",
                "region_multiplier": "1",
            },
        }
    )


class BudgetOperatorTests(unittest.TestCase):
    def apply(self, store, observed_at=NOW, source_body=BASETEN_SOURCE_BODY):
        return budget_operator.apply_budget_authority(
            store,
            operator_input(observed_at, source_body),
            source_body,
            MODAL_SOURCE_BODY,
            confirmed_campaign_id=CAMPAIGN,
            now=lambda: observed_at,
        )

    def test_fresh_bootstrap_and_23_hour_refresh_preserve_exact_provenance(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            first = self.apply(store)
            self.assertEqual(first["state"], "ready")
            self.assertEqual(
                store.get(
                    f"{budget_operator.INPUT_PREFIX}/"
                    f"{first['operator_input_sha256']}.json"
                ),
                operator_input(),
            )
            self.assertEqual(
                store.get(
                    f"{budget_operator.RESULT_PREFIX}/"
                    f"{first['operator_input_sha256']}.json"
                ),
                canonical_json(first),
            )
            training = json.loads(
                store.get(ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY)
            )
            serving = json.loads(
                store.get(ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY)
            )
            self.assertEqual(training["schema_version"], "milk.baseten-h100-pricing-receipt.v4")
            self.assertEqual(
                training["source_observation_sha256"],
                serving["source_observation_sha256"],
            )
            observation_sha256 = training["source_observation_sha256"]
            observation_raw = store.get(
                f"{BASETEN_PRICING_OBSERVATION_PREFIX}/{observation_sha256}.json"
            )
            observation = json.loads(observation_raw)
            self.assertEqual(hashlib.sha256(observation_raw).hexdigest(), observation_sha256)
            self.assertEqual(
                store.get(
                    f"{BASETEN_PRICING_SOURCE_BODY_PREFIX}/"
                    f"{observation['source_body_sha256']}.bin"
                ),
                BASETEN_SOURCE_BODY,
            )
            self.assertEqual(
                store.get(
                    f"{MODAL_RATES_SOURCE_BODY_PREFIX}/"
                    f"{hashlib.sha256(MODAL_SOURCE_BODY).hexdigest()}.bin"
                ),
                MODAL_SOURCE_BODY,
            )

            later = NOW + dt.timedelta(hours=23)
            second = self.apply(store, later)
            self.assertNotEqual(
                first["operator_input_sha256"],
                second["operator_input_sha256"],
            )
            for role in ("baseten_training", "baseten_serving", "modal"):
                self.assertNotEqual(
                    first["active_pricing_receipt_sha256s"][role],
                    second["active_pricing_receipt_sha256s"][role],
                )
            self.assertEqual(
                first["campaign_authority_sha256"],
                second["campaign_authority_sha256"],
            )
            for role, prefix in (
                ("baseten_training", BASETEN_PRICING_RECEIPT_PREFIX),
                ("baseten_serving", BASETEN_PRICING_RECEIPT_PREFIX),
                ("modal", MODAL_PRICING_RECEIPT_PREFIX),
            ):
                digest = first["active_pricing_receipt_sha256s"][role]
                self.assertEqual(
                    hashlib.sha256(store.get(f"{prefix}/{digest}.json")).hexdigest(),
                    digest,
                )
            budget = CampaignBudget(
                store,
                CAMPAIGN,
                baseten_provider_identity(PROJECT),
                now=lambda: later,
            )
            budget.initialize()
            run_id = "a" * 64
            preparation = hashlib.sha256(run_id.encode()).hexdigest()
            budget.prepare(
                run_id,
                preparation,
                int((later + dt.timedelta(minutes=1)).timestamp()),
                int((later + dt.timedelta(hours=1)).timestamp()),
            )
            budget.reserve(run_id, 1, preparation)

    def test_invalid_inputs_fail_before_campaign_authority(self):
        cases = (
            (
                operator_input(),
                b"wrong source body",
                MODAL_SOURCE_BODY,
                CAMPAIGN,
                NOW,
                "differs",
            ),
            (
                operator_input().rstrip(),
                BASETEN_SOURCE_BODY,
                MODAL_SOURCE_BODY,
                CAMPAIGN,
                NOW,
                "canonical",
            ),
            (
                operator_input(),
                BASETEN_SOURCE_BODY,
                MODAL_SOURCE_BODY,
                "d" * 64,
                NOW,
                "confirmation",
            ),
            (
                operator_input(),
                BASETEN_SOURCE_BODY,
                MODAL_SOURCE_BODY,
                CAMPAIGN,
                NOW + dt.timedelta(hours=24, seconds=1),
                "stale",
            ),
            (
                operator_input(),
                BASETEN_SOURCE_BODY,
                b"wrong Modal source body",
                CAMPAIGN,
                NOW,
                "Modal rates source body differs",
            ),
        )
        for input_raw, body, modal_body, confirmation, current, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as root:
                store = LocalEvidenceStore(root)
                with self.assertRaisesRegex(ValueError, message):
                    budget_operator.apply_budget_authority(
                        store,
                        input_raw,
                        body,
                        modal_body,
                        confirmed_campaign_id=confirmation,
                        now=lambda: current,
                    )
                with self.assertRaises(FileNotFoundError):
                    store.get(ACTIVE_CAMPAIGN_AUTHORITY_KEY)
                self.assertEqual(
                    store.list(BASETEN_PRICING_SOURCE_BODY_PREFIX),
                    [],
                )

    def test_active_baseten_receipt_rechecks_observation_and_source_body(self):
        for target in ("observation", "source_body"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as root:
                store = LocalEvidenceStore(root)
                self.apply(store)
                receipt = json.loads(
                    store.get(ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY)
                )
                observation_key = (
                    f"{BASETEN_PRICING_OBSERVATION_PREFIX}/"
                    f"{receipt['source_observation_sha256']}.json"
                )
                observation = json.loads(store.get(observation_key))
                key = (
                    observation_key
                    if target == "observation"
                    else f"{BASETEN_PRICING_SOURCE_BODY_PREFIX}/"
                    f"{observation['source_body_sha256']}.bin"
                )
                store._path(key).write_bytes(b"tampered")
                campaign = CampaignBudget(
                    store,
                    CAMPAIGN,
                    baseten_provider_identity(PROJECT),
                    now=lambda: NOW,
                )
                campaign.initialize()
                run_id = "b" * 64
                preparation = hashlib.sha256(run_id.encode()).hexdigest()
                campaign.prepare(
                    run_id,
                    preparation,
                    int((NOW + dt.timedelta(minutes=1)).timestamp()),
                    int((NOW + dt.timedelta(hours=1)).timestamp()),
                )
                with self.assertRaisesRegex(ValueError, "digest|differs"):
                    campaign.reserve(run_id, 1, preparation)

    def test_active_modal_receipt_rechecks_source_body(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            self.apply(store)
            source_sha256 = hashlib.sha256(MODAL_SOURCE_BODY).hexdigest()
            store._path(
                f"{MODAL_RATES_SOURCE_BODY_PREFIX}/{source_sha256}.bin"
            ).write_bytes(b"tampered")
            campaign = CampaignBudget(
                store,
                CAMPAIGN,
                modal_provider_identity(
                    MODAL_WORKSPACE,
                    MODAL_ENVIRONMENT,
                    MODAL_APP,
                ),
                now=lambda: NOW,
            )
            campaign.initialize()
            run_id = "d" * 64
            preparation = hashlib.sha256(run_id.encode()).hexdigest()
            campaign.prepare(
                run_id,
                preparation,
                int((NOW + dt.timedelta(minutes=1)).timestamp()),
                int((NOW + dt.timedelta(hours=1)).timestamp()),
            )
            with self.assertRaisesRegex(ValueError, "source body differs"):
                campaign.reserve(run_id, 1, preparation)

    def test_command_requires_explicit_store_and_jobs_image_excludes_operator(self):
        current = dt.datetime.now(UTC).replace(microsecond=0)
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            input_path = root_path / "input.json"
            source_path = root_path / "source.html"
            modal_source_path = root_path / "modal-rates.json"
            input_path.write_bytes(operator_input(current))
            source_path.write_bytes(BASETEN_SOURCE_BODY)
            modal_source_path.write_bytes(MODAL_SOURCE_BODY)
            with (
                mock.patch.object(
                    budget_operator.R2EvidenceStore,
                    "from_environment",
                ) as r2,
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                budget_operator.main(
                    [
                        "--input",
                        str(input_path),
                        "--baseten-source-body",
                        str(source_path),
                        "--modal-rates-source-body",
                        str(modal_source_path),
                        "--confirm-campaign-id",
                        CAMPAIGN,
                    ]
                )
            r2.assert_not_called()

            stdout = types.SimpleNamespace(buffer=io.BytesIO())
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    budget_operator.R2EvidenceStore,
                    "from_environment",
                ) as r2,
                mock.patch.object(budget_operator.os.sys, "stdout", stdout),
            ):
                self.assertEqual(
                    budget_operator.main(
                        [
                            "--input",
                            str(input_path),
                            "--baseten-source-body",
                            str(source_path),
                            "--modal-rates-source-body",
                            str(modal_source_path),
                            "--confirm-campaign-id",
                            CAMPAIGN,
                            "--local-store",
                            str(root_path / "evidence"),
                        ]
                    ),
                    0,
                )
            r2.assert_not_called()
            self.assertEqual(json.loads(stdout.buffer.getvalue())["state"], "ready")

            r2_store = LocalEvidenceStore(root_path / "r2-evidence")
            stdout = types.SimpleNamespace(buffer=io.BytesIO())
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    budget_operator.R2EvidenceStore,
                    "from_environment",
                    return_value=r2_store,
                ) as r2,
                mock.patch.object(budget_operator.os.sys, "stdout", stdout),
            ):
                self.assertEqual(
                    budget_operator.main(
                        [
                            "--input",
                            str(input_path),
                            "--baseten-source-body",
                            str(source_path),
                            "--modal-rates-source-body",
                            str(modal_source_path),
                            "--confirm-campaign-id",
                            CAMPAIGN,
                            "--r2",
                        ]
                    ),
                    0,
                )
            r2.assert_called_once_with()
            self.assertEqual(json.loads(stdout.buffer.getvalue())["state"], "ready")

        repository = Path(__file__).resolve().parents[1]
        self.assertNotIn(
            "budget_operator",
            repository.joinpath("Dockerfile.jobs").read_text(encoding="utf-8"),
        )
        self.assertNotIn(
            "!milk_harness/budget_operator.py",
            repository.joinpath("Dockerfile.jobs.dockerignore").read_text(
                encoding="utf-8"
            ),
        )

    def test_operator_rejects_provider_and_jobs_authorities(self):
        safe = {
            "MILK_EVIDENCE_R2_ACCOUNT_ID": "0" * 32,
            "MILK_EVIDENCE_R2_BUCKET": "evidence",
            "MILK_EVIDENCE_R2_ACCESS_KEY_ID": "access",
            "MILK_EVIDENCE_R2_SECRET_ACCESS_KEY": "secret",
        }
        budget_operator._reject_ambient_authorities(safe)
        for name in (
            "BASETEN_API_KEY",
            "MODAL_TOKEN_SECRET",
            "MILK_PROVIDER_LEASE_TOKEN_FILE",
            "MILK_CREATE_AUTHORITY_READ_R2_SECRET_ACCESS_KEY",
            "MILK_JOB_SECRET",
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                budget_operator._reject_ambient_authorities({**safe, name: "secret"})


if __name__ == "__main__":
    unittest.main()
