from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
from pathlib import Path
import urllib.error

from milk_harness import budget
from milk_harness.evidence import (
    HEX64,
    MAX_OBJECT_BYTES,
    LocalEvidenceStore,
    R2EvidenceStore,
    canonical_json,
    create_same,
)
from milk_harness.publish_image_admission import (
    _reject_ambient_authorities as _reject_image_authorities,
)


INPUT_PREFIX = "authority/v1/budget-operator-inputs"
RESULT_PREFIX = "authority/v1/budget-operator-results"


def _operator_input(raw):
    if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_OBJECT_BYTES:
        raise ValueError("budget operator input must be bounded exact bytes")
    value = budget._object(raw)
    if canonical_json(value) != raw or set(value) != {
        "schema_version",
        "campaign_id",
        "baseten_project_id",
        "baseten_team_name",
        "baseten_pricing_observation",
    }:
        raise ValueError("budget operator input must be one canonical object")
    if value.get("schema_version") != "milk.budget-authority-operator-input.v2":
        raise ValueError("budget operator input schema is invalid")
    if HEX64.fullmatch(value.get("campaign_id", "")) is None:
        raise ValueError("budget operator campaign ID is invalid")
    if not isinstance(value.get("baseten_pricing_observation"), dict):
        raise ValueError("Baseten pricing observation is invalid")
    return value


def apply_budget_authority(
    store,
    input_raw,
    baseten_source_body,
    *,
    confirmed_campaign_id,
    now=lambda: dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
):
    value = _operator_input(input_raw)
    campaign_id = value["campaign_id"]
    if confirmed_campaign_id != campaign_id:
        raise ValueError("campaign confirmation differs from operator input")
    current = budget._utc(now(), "clock")
    observation_raw = canonical_json(value["baseten_pricing_observation"])
    input_sha256 = hashlib.sha256(input_raw).hexdigest()
    create_same(
        store,
        f"{INPUT_PREFIX}/{input_sha256}.json",
        input_raw,
        "application/json",
    )
    budget.prepare_campaign_authority(
        store,
        campaign_id,
        value["baseten_project_id"],
        value["baseten_team_name"],
        baseten_pricing_observation_raw=observation_raw,
        baseten_pricing_source_body=baseten_source_body,
        now=lambda: current,
    )
    authority_raw = store.get(budget.ACTIVE_CAMPAIGN_AUTHORITY_KEY)
    authority = budget._validate_authority(
        budget._object(authority_raw),
        campaign_id,
    )
    active = {}
    for name, key in (
        ("baseten_training", budget.ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY),
        ("baseten_serving", budget.ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY),
    ):
        raw = store.get(key)
        unused_receipt, digest = budget._immutable_baseten_pricing(
            store,
            raw,
            authority,
            current,
        )
        del unused_receipt
        active[name] = digest
    result = {
        "schema_version": "milk.budget-authority-operator-result.v2",
        "campaign_id": campaign_id,
        "operator_input_sha256": input_sha256,
        "campaign_authority_sha256": hashlib.sha256(authority_raw).hexdigest(),
        "active_pricing_receipt_sha256s": active,
        "state": "ready",
    }
    result_raw = canonical_json(result)
    create_same(
        store,
        f"{RESULT_PREFIX}/{input_sha256}.json",
        result_raw,
        "application/json",
    )
    return result


def _read_exact_file(value, label):
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be an absolute regular file")
    with path.open("rb") as source:
        raw = source.read(MAX_OBJECT_BYTES + 1)
    if not 1 <= len(raw) <= MAX_OBJECT_BYTES:
        raise ValueError(f"{label} must be bounded nonempty bytes")
    return raw


def _reject_ambient_authorities(environ):
    _reject_image_authorities(environ)
    forbidden = (
        "MILK_CREATE_AUTHORITY_",
        "MILK_GATEWAY_",
        "MILK_JOB_",
        "MILK_OPS_LOG_",
        "MILK_PROVIDER_",
    )
    if any(value and name.startswith(forbidden) for name, value in environ.items()):
        raise ValueError(
            "budget operator must not receive provider, jobs, gateway, create, or log authority"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Create or refresh one Milk campaign budget authority"
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--baseten-source-body", required=True)
    parser.add_argument("--confirm-campaign-id", required=True)
    store = parser.add_mutually_exclusive_group(required=True)
    store.add_argument("--local-store")
    store.add_argument("--r2", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        _reject_ambient_authorities(os.environ)
        evidence = (
            R2EvidenceStore.from_environment()
            if arguments.r2
            else LocalEvidenceStore(arguments.local_store)
        )
        result = apply_budget_authority(
            evidence,
            _read_exact_file(arguments.input, "budget operator input"),
            _read_exact_file(
                arguments.baseten_source_body,
                "Baseten pricing source body",
            ),
            confirmed_campaign_id=arguments.confirm_campaign_id,
        )
    except (
        OSError,
        RuntimeError,
        UnicodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"milk-budget-authority: {error}", file=os.sys.stderr)
        return 64
    os.sys.stdout.buffer.write(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
