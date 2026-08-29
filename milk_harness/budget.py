from __future__ import annotations

import datetime as dt
import hashlib
import json
import re

from milk_harness.evidence import HEX64, MAX_OBJECT_BYTES, canonical_json, create_same


ABSOLUTE_CEILING_MICROUSD = 1_000_000_000
LAUNCH_CUTOFF_MICROUSD = 850_000_000
TEARDOWN_RESERVE_MICROUSD = 150_000_000
BASETEN_H100_PRICING_SOURCE = "https://docs.baseten.co/deployment/resources"
BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE = 108_330
H100_RESERVATION_RATE_MICROUSD_PER_MINUTE = 125_000
PRICING_RECEIPT_MAX_AGE_SECONDS = 24 * 60 * 60
ACTIVE_CAMPAIGN_AUTHORITY_KEY = "authority/v1/active-campaign.json"
ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY = (
    "state/v1/active-baseten-training-h100-pricing.json"
)
ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY = (
    "state/v1/active-baseten-serving-h100-pricing.json"
)
BASETEN_PRICING_RECEIPT_PREFIX = (
    "authority/v1/baseten-h100-pricing-receipts"
)
BASETEN_PRICING_OBSERVATION_PREFIX = (
    "authority/v1/baseten-h100-pricing-observations"
)
BASETEN_PRICING_SOURCE_BODY_PREFIX = (
    "authority/v1/baseten-h100-pricing-source-bodies"
)
# Kept only for parsing historical jobs evidence while that surface is removed.
# Active budget authority never accepts Modal pricing or identities.
MODAL_RATES_SOURCE_COMMAND = "modal billing rates --json"
MAX_CAS_ATTEMPTS = 32
PROJECT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
TEAM_NAME = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9 ._-]{0,126}[A-Za-z0-9])?\Z"
)


def _object(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("budget JSON contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)))
    if not isinstance(value, dict):
        raise ValueError("budget JSON must be an object")
    return value


def _utc(value, label):
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC")
    value = value.astimezone(dt.timezone.utc)
    if value.microsecond:
        raise ValueError(f"{label} must have whole-second precision")
    return value


def _utc_text(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc(value, label):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    parsed = _utc(parsed, label)
    if _utc_text(parsed) != value:
        raise ValueError(f"{label} must be canonical UTC RFC3339")
    return parsed


def _opaque_identity(value, label):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 256
        or value != value.strip()
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise ValueError(f"{label} is invalid")
    return value


def baseten_provider_identity(project_id):
    if not isinstance(project_id, str) or PROJECT_ID.fullmatch(project_id) is None:
        raise ValueError("Baseten training project ID is invalid")
    return {"provider": "baseten", "project_id": project_id}


def baseten_serving_provider_identity(team_name):
    if not isinstance(team_name, str) or TEAM_NAME.fullmatch(team_name) is None:
        raise ValueError("Baseten serving team name is invalid")
    return {"provider": "baseten", "team_name": team_name}


def modal_provider_identity(workspace_id, environment_name, app_name):
    return {
        "provider": "modal",
        "workspace_id": _opaque_identity(workspace_id, "Modal workspace ID"),
        "environment_name": _opaque_identity(
            environment_name,
            "Modal environment name",
        ),
        "app_name": _opaque_identity(app_name, "Modal app name"),
    }


def _validate_provider_identity(value, authority=None):
    if not isinstance(value, dict):
        raise ValueError("provider identity is invalid")
    provider = value.get("provider")
    if provider == "baseten" and set(value) == {"provider", "project_id"}:
        expected = baseten_provider_identity(value.get("project_id"))
        if authority is not None and value["project_id"] != authority["baseten_project_id"]:
            raise ValueError("Baseten training identity differs from campaign authority")
    elif provider == "baseten" and set(value) == {"provider", "team_name"}:
        expected = baseten_serving_provider_identity(value.get("team_name"))
        if authority is not None and value["team_name"] != authority["baseten_team_name"]:
            raise ValueError("Baseten serving identity differs from campaign authority")
    elif provider == "modal":
        raise ValueError("Modal provider identity is not authorized")
    else:
        raise ValueError("provider identity is invalid")
    if value != expected:
        raise ValueError("provider identity is invalid")
    return dict(value)


def _provider_role(identity):
    return "training" if "project_id" in identity else "serving"


def _authority(
    campaign_id,
    baseten_project_id,
    baseten_team_name,
):
    return {
        "schema_version": "milk.active-campaign-authority.v4",
        "campaign_id": campaign_id,
        "baseten_project_id": baseten_project_id,
        "baseten_team_name": baseten_team_name,
        "absolute_ceiling_microusd": ABSOLUTE_CEILING_MICROUSD,
        "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
        "teardown_reserve_microusd": TEARDOWN_RESERVE_MICROUSD,
    }


def baseten_pricing_observation(
    campaign_id,
    source_body_sha256,
    observed_at,
):
    if not isinstance(campaign_id, str) or HEX64.fullmatch(campaign_id) is None:
        raise ValueError("campaign ID must be lowercase hex64")
    if (
        not isinstance(source_body_sha256, str)
        or HEX64.fullmatch(source_body_sha256) is None
    ):
        raise ValueError("Baseten pricing source body SHA-256 is invalid")
    return {
        "schema_version": "milk.baseten-h100-pricing-observation.v1",
        "campaign_id": campaign_id,
        "source_url": BASETEN_H100_PRICING_SOURCE,
        "source_body_sha256": source_body_sha256,
        "observed_at": _utc_text(_utc(observed_at, "pricing observation")),
        "gpu_type": "H100",
        "unit": "gpu_minute",
        "provider_rate_microusd_per_minute": BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE,
    }


def _baseten_pricing_receipt(
    campaign_id,
    provider_role,
    provider_identity,
    source_observation_sha256,
    observation,
):
    service = "training" if provider_role == "training" else "dedicated_serving"
    return {
        "schema_version": "milk.baseten-h100-pricing-receipt.v4",
        "campaign_id": campaign_id,
        "provider_role": provider_role,
        "provider_identity": provider_identity,
        "service": service,
        "gpu_type": "H100",
        "unit": "gpu_minute",
        "source_url": observation["source_url"],
        "source_observation_sha256": source_observation_sha256,
        "observed_at": observation["observed_at"],
        "provider_rate_microusd_per_minute": BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE,
        "reservation_rate_microusd_per_minute": H100_RESERVATION_RATE_MICROUSD_PER_MINUTE,
        "max_age_seconds": PRICING_RECEIPT_MAX_AGE_SECONDS,
    }


def _validate_authority(value, campaign_id):
    fields = {
        "schema_version",
        "campaign_id",
        "baseten_project_id",
        "baseten_team_name",
        "absolute_ceiling_microusd",
        "launch_cutoff_microusd",
        "teardown_reserve_microusd",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("active campaign authority is invalid")
    if value.get("campaign_id") != campaign_id:
        raise ValueError("active campaign authority belongs to a different campaign")
    try:
        baseten_provider_identity(value.get("baseten_project_id"))
        baseten_serving_provider_identity(value.get("baseten_team_name"))
    except ValueError as error:
        raise ValueError("active campaign authority has invalid provider identities") from error
    if (
        value.get("schema_version") != "milk.active-campaign-authority.v4"
        or value.get("absolute_ceiling_microusd") != ABSOLUTE_CEILING_MICROUSD
        or value.get("launch_cutoff_microusd") != LAUNCH_CUTOFF_MICROUSD
        or value.get("teardown_reserve_microusd") != TEARDOWN_RESERVE_MICROUSD
    ):
        raise ValueError("active campaign authority is invalid")
    return value


def _validate_receipt_age(receipt, current):
    observed = _parse_utc(receipt.get("observed_at"), "pricing observation")
    if current is not None:
        if observed > current:
            raise ValueError("active campaign pricing receipt is from the future")
        if (current - observed).total_seconds() > PRICING_RECEIPT_MAX_AGE_SECONDS:
            raise ValueError("active campaign pricing receipt is stale")


def _validate_baseten_pricing_observation(observation, authority, current=None):
    if (
        not isinstance(observation, dict)
        or set(observation)
        != {
            "schema_version",
            "campaign_id",
            "source_url",
            "source_body_sha256",
            "observed_at",
            "gpu_type",
            "unit",
            "provider_rate_microusd_per_minute",
        }
        or observation.get("schema_version")
        != "milk.baseten-h100-pricing-observation.v1"
        or observation.get("campaign_id") != authority["campaign_id"]
        or observation.get("source_url") != BASETEN_H100_PRICING_SOURCE
        or not isinstance(observation.get("source_body_sha256"), str)
        or HEX64.fullmatch(observation["source_body_sha256"]) is None
        or observation.get("gpu_type") != "H100"
        or observation.get("unit") != "gpu_minute"
        or observation.get("provider_rate_microusd_per_minute")
        != BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE
    ):
        raise ValueError("Baseten pricing observation is invalid")
    _validate_receipt_age(observation, current)
    return observation


def _validate_baseten_pricing_source_body(source_body, observation):
    if (
        not isinstance(source_body, bytes)
        or not 1 <= len(source_body) <= MAX_OBJECT_BYTES
        or hashlib.sha256(source_body).hexdigest()
        != observation["source_body_sha256"]
    ):
        raise ValueError("Baseten pricing source body differs from its observation")


def _validate_baseten_pricing_receipt(
    receipt,
    authority,
    observation,
    current=None,
):
    if not isinstance(receipt, dict) or set(receipt) != {
        "schema_version",
        "campaign_id",
        "provider_role",
        "provider_identity",
        "service",
        "gpu_type",
        "unit",
        "source_url",
        "source_observation_sha256",
        "observed_at",
        "provider_rate_microusd_per_minute",
        "reservation_rate_microusd_per_minute",
        "max_age_seconds",
    }:
        raise ValueError("active Baseten pricing receipt is invalid")
    provider_rate = receipt.get("provider_rate_microusd_per_minute")
    if (
        receipt.get("schema_version") != "milk.baseten-h100-pricing-receipt.v4"
        or receipt.get("campaign_id") != authority["campaign_id"]
        or receipt.get("provider_role") not in {"training", "serving"}
        or receipt.get("service")
        != (
            "training"
            if receipt.get("provider_role") == "training"
            else "dedicated_serving"
        )
        or receipt.get("gpu_type") != "H100"
        or receipt.get("unit") != "gpu_minute"
        or receipt.get("source_url") != BASETEN_H100_PRICING_SOURCE
        or not isinstance(receipt.get("source_observation_sha256"), str)
        or HEX64.fullmatch(receipt["source_observation_sha256"]) is None
        or receipt["source_observation_sha256"]
        != hashlib.sha256(canonical_json(observation)).hexdigest()
        or receipt["source_url"] != observation["source_url"]
        or receipt["observed_at"] != observation["observed_at"]
        or receipt.get("reservation_rate_microusd_per_minute")
        != H100_RESERVATION_RATE_MICROUSD_PER_MINUTE
        or receipt.get("max_age_seconds") != PRICING_RECEIPT_MAX_AGE_SECONDS
        or provider_rate != BASETEN_H100_PROVIDER_RATE_MICROUSD_PER_MINUTE
    ):
        raise ValueError("active Baseten pricing receipt is invalid")
    identity = _validate_provider_identity(receipt["provider_identity"], authority)
    if (
        identity["provider"] != "baseten"
        or receipt["provider_role"] != _provider_role(identity)
    ):
        raise ValueError("active Baseten pricing receipt is invalid")
    _validate_receipt_age(receipt, current)
    return receipt


def _immutable_baseten_pricing_observation(
    store,
    digest,
    authority,
    current=None,
):
    if not isinstance(digest, str) or HEX64.fullmatch(digest) is None:
        raise ValueError("Baseten pricing observation digest is invalid")
    try:
        raw = store.get(f"{BASETEN_PRICING_OBSERVATION_PREFIX}/{digest}.json")
    except FileNotFoundError as error:
        raise ValueError("Baseten pricing observation is not immutable") from error
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("Baseten pricing observation digest is invalid")
    observation = _object(raw)
    if canonical_json(observation) != raw:
        raise ValueError("Baseten pricing observation is not canonical")
    _validate_baseten_pricing_observation(observation, authority, current)
    try:
        source_body = store.get(
            f"{BASETEN_PRICING_SOURCE_BODY_PREFIX}/"
            f"{observation['source_body_sha256']}.bin"
        )
    except FileNotFoundError as error:
        raise ValueError("Baseten pricing source body is not immutable") from error
    _validate_baseten_pricing_source_body(source_body, observation)
    return observation


def _immutable_baseten_pricing(store, raw, authority, current=None):
    receipt = _object(raw)
    if canonical_json(receipt) != raw:
        raise ValueError("active Baseten pricing receipt is not canonical")
    observation = _immutable_baseten_pricing_observation(
        store,
        receipt.get("source_observation_sha256"),
        authority,
        current,
    )
    _validate_baseten_pricing_receipt(
        receipt,
        authority,
        observation,
        current,
    )
    digest = hashlib.sha256(raw).hexdigest()
    try:
        immutable = store.get(f"{BASETEN_PRICING_RECEIPT_PREFIX}/{digest}.json")
    except FileNotFoundError as error:
        raise ValueError("active Baseten pricing receipt is not immutable") from error
    if immutable != raw:
        raise ValueError("active Baseten pricing differs from immutable receipt")
    return receipt, digest


def refresh_baseten_pricing(
    store,
    campaign_id,
    provider_role,
    *,
    pricing_observation_raw,
    pricing_source_body,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    authority = _validate_authority(
        _object(store.get(ACTIVE_CAMPAIGN_AUTHORITY_KEY)),
        campaign_id,
    )
    if provider_role == "training":
        identity = baseten_provider_identity(authority["baseten_project_id"])
        active_key = ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY
    elif provider_role == "serving":
        identity = baseten_serving_provider_identity(authority["baseten_team_name"])
        active_key = ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY
    else:
        raise ValueError("Baseten pricing role is invalid")
    current = _utc(now(), "clock")
    if not isinstance(pricing_observation_raw, bytes):
        raise ValueError("Baseten pricing observation must be exact bytes")
    observation = _object(pricing_observation_raw)
    if canonical_json(observation) != pricing_observation_raw:
        raise ValueError("Baseten pricing observation is not canonical")
    _validate_baseten_pricing_observation(observation, authority, current)
    _validate_baseten_pricing_source_body(pricing_source_body, observation)
    observation_digest = hashlib.sha256(pricing_observation_raw).hexdigest()
    receipt = _baseten_pricing_receipt(
        campaign_id,
        provider_role,
        identity,
        observation_digest,
        observation,
    )
    _validate_baseten_pricing_receipt(
        receipt,
        authority,
        observation,
        current,
    )
    create_same(
        store,
        f"{BASETEN_PRICING_SOURCE_BODY_PREFIX}/"
        f"{observation['source_body_sha256']}.bin",
        pricing_source_body,
        "application/octet-stream",
    )
    create_same(
        store,
        f"{BASETEN_PRICING_OBSERVATION_PREFIX}/{observation_digest}.json",
        pricing_observation_raw,
        "application/json",
    )
    raw = canonical_json(receipt)
    digest = hashlib.sha256(raw).hexdigest()
    create_same(
        store,
        f"{BASETEN_PRICING_RECEIPT_PREFIX}/{digest}.json",
        raw,
        "application/json",
    )
    if store.create(active_key, raw, "application/json"):
        return "created"
    for _ in range(MAX_CAS_ATTEMPTS):
        existing_raw, etag = store.get_versioned(active_key)
        existing, unused_digest = _immutable_baseten_pricing(
            store,
            existing_raw,
            authority,
        )
        del unused_digest
        if (
            existing["provider_role"] != provider_role
            or existing["provider_identity"] != identity
        ):
            raise ValueError("active Baseten pricing has the wrong role or identity")
        if existing_raw == raw:
            return "existing"
        if _parse_utc(existing["observed_at"], "pricing observation") >= _parse_utc(
            receipt["observed_at"], "pricing observation"
        ):
            raise ValueError("pricing refresh must advance the observation time")
        if store.replace(
            active_key,
            raw,
            etag,
            "application/json",
        ):
            return "updated"
    raise RuntimeError("Baseten pricing receipt CAS contention exceeded its bound")


def prepare_campaign_authority(
    store,
    campaign_id,
    baseten_project_id,
    baseten_team_name,
    *,
    baseten_pricing_observation_raw,
    baseten_pricing_source_body,
    now=lambda: dt.datetime.now(dt.timezone.utc),
):
    """Create the singleton authority; call only from trusted operator tooling."""
    if not isinstance(campaign_id, str) or HEX64.fullmatch(campaign_id) is None:
        raise ValueError("campaign ID must be lowercase hex64")
    if not callable(now):
        raise ValueError("clock must be callable")
    baseten_provider_identity(baseten_project_id)
    baseten_serving_provider_identity(baseten_team_name)
    current = _utc(now(), "clock")
    value = _authority(
        campaign_id,
        baseten_project_id,
        baseten_team_name,
    )
    _validate_authority(value, campaign_id)
    if not isinstance(baseten_pricing_observation_raw, bytes):
        raise ValueError("Baseten pricing observation must be exact bytes")
    baseten_observation = _object(baseten_pricing_observation_raw)
    if canonical_json(baseten_observation) != baseten_pricing_observation_raw:
        raise ValueError("Baseten pricing observation is not canonical")
    _validate_baseten_pricing_observation(
        baseten_observation,
        value,
        current,
    )
    _validate_baseten_pricing_source_body(
        baseten_pricing_source_body,
        baseten_observation,
    )
    baseten_observation_sha256 = hashlib.sha256(
        baseten_pricing_observation_raw
    ).hexdigest()
    for provider_role, identity in (
        ("training", baseten_provider_identity(baseten_project_id)),
        ("serving", baseten_serving_provider_identity(baseten_team_name)),
    ):
        _validate_baseten_pricing_receipt(
            _baseten_pricing_receipt(
                campaign_id,
                provider_role,
                identity,
                baseten_observation_sha256,
                baseten_observation,
            ),
            value,
            baseten_observation,
            current,
        )
    state = create_same(
        store,
        ACTIVE_CAMPAIGN_AUTHORITY_KEY,
        canonical_json(value),
        "application/json",
    )
    for provider_role in ("training", "serving"):
        refresh_baseten_pricing(
            store,
            campaign_id,
            provider_role,
            pricing_observation_raw=baseten_pricing_observation_raw,
            pricing_source_body=baseten_pricing_source_body,
            now=lambda: current,
        )
    return state


class CampaignBudget:
    def __init__(
        self,
        store,
        campaign_id,
        provider_identity,
        *,
        now=lambda: dt.datetime.now(dt.timezone.utc),
    ):
        if not isinstance(campaign_id, str) or HEX64.fullmatch(campaign_id) is None:
            raise ValueError("campaign ID must be lowercase hex64")
        if not callable(now):
            raise ValueError("clock must be callable")
        self.store = store
        self.campaign_id = campaign_id
        self.provider_identity = _validate_provider_identity(provider_identity)
        self.provider_role = _provider_role(self.provider_identity)
        self.now = now
        self.authority = None
        self.prefix = f"campaigns/v1/{campaign_id}"
        self.policy_key = f"{self.prefix}/policy.json"
        self.head_key = f"state/v1/campaigns/{campaign_id}/budget-head.json"
        self.policy = None

    def initialize(self):
        try:
            authority = _object(self.store.get(ACTIVE_CAMPAIGN_AUTHORITY_KEY))
        except FileNotFoundError as error:
            raise ValueError("active campaign authority is missing") from error
        self.authority = _validate_authority(authority, self.campaign_id)
        _validate_provider_identity(self.provider_identity, self.authority)
        self.policy = {
            "schema_version": "milk.campaign-budget-policy.v4",
            "campaign_id": self.campaign_id,
            "baseten_project_id": self.authority["baseten_project_id"],
            "baseten_team_name": self.authority["baseten_team_name"],
            "absolute_ceiling_microusd": ABSOLUTE_CEILING_MICROUSD,
            "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
            "teardown_reserve_microusd": TEARDOWN_RESERVE_MICROUSD,
        }
        create_same(self.store, self.policy_key, canonical_json(self.policy), "application/json")
        initial = {
            "schema_version": "milk.campaign-budget-head.v4",
            "campaign_id": self.campaign_id,
            "revision": 0,
            "committed_microusd": 0,
            "reserved_microusd": 0,
            "open": {},
            "pending_settlements": {},
            "outstanding": {},
            "ceiling_breached": False,
        }
        self.store.create(self.head_key, canonical_json(initial), "application/json")
        head, unused_etag = self._head()
        del unused_etag
        return head

    def _authorized(self):
        if self.authority is None:
            raise ValueError("campaign budget is not initialized")
        return self.authority

    def _current_pricing(self):
        authority = self._authorized()
        if self.provider_role == "training":
            key = ACTIVE_BASETEN_TRAINING_PRICING_RECEIPT_KEY
        else:
            key = ACTIVE_BASETEN_SERVING_PRICING_RECEIPT_KEY
        try:
            raw = self.store.get(key)
        except FileNotFoundError as error:
            raise ValueError("active Baseten pricing receipt is missing") from error
        current = _utc(self.now(), "clock")
        receipt, digest = _immutable_baseten_pricing(
            self.store,
            raw,
            authority,
            current,
        )
        if receipt["provider_identity"] != self.provider_identity:
            raise ValueError("active pricing receipt has the wrong provider identity")
        if receipt["provider_role"] != self.provider_role:
            raise ValueError("active pricing receipt has the wrong provider role")
        return self._pricing_binding(receipt, digest)

    def _pricing_binding(self, receipt, digest):
        return {
            "provider_role": receipt["provider_role"],
            "provider_identity": receipt["provider_identity"],
            "pricing_receipt_sha256": digest,
            "provider_rate_microusd_per_minute": receipt[
                "provider_rate_microusd_per_minute"
            ],
            "reservation_rate_microusd_per_minute": receipt[
                "reservation_rate_microusd_per_minute"
            ],
        }

    def _validate_pricing_binding(self, value):
        authority = self._authorized()
        identity = _validate_provider_identity(
            value.get("provider_identity"),
            authority,
        )
        provider_role = value.get("provider_role")
        digest = value.get("pricing_receipt_sha256")
        provider_rate = value.get("provider_rate_microusd_per_minute")
        reservation_rate = value.get("reservation_rate_microusd_per_minute")
        if (
            not isinstance(digest, str)
            or HEX64.fullmatch(digest) is None
            or provider_role != _provider_role(identity)
            or type(provider_rate) is not int
            or type(reservation_rate) is not int
        ):
            raise ValueError("budget pricing binding is invalid")
        try:
            raw = self.store.get(f"{BASETEN_PRICING_RECEIPT_PREFIX}/{digest}.json")
        except FileNotFoundError as error:
            raise ValueError("budget pricing receipt is missing") from error
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("budget pricing receipt digest is invalid")
        receipt, unused_digest = _immutable_baseten_pricing(
            self.store,
            raw,
            authority,
        )
        del unused_digest
        if receipt["provider_identity"] != identity:
            raise ValueError("budget pricing receipt has the wrong provider identity")
        if receipt["provider_role"] != provider_role:
            raise ValueError("budget pricing receipt has the wrong provider role")
        expected = self._pricing_binding(receipt, digest)
        if any(value.get(key) != item for key, item in expected.items()):
            raise ValueError("budget pricing binding differs from its receipt")
        return expected

    def _head(self):
        self._authorized()
        raw, etag = self.store.get_versioned(self.head_key)
        head = _object(raw)
        if set(head) != {
            "schema_version",
            "campaign_id",
            "revision",
            "committed_microusd",
            "reserved_microusd",
            "open",
            "pending_settlements",
            "outstanding",
            "ceiling_breached",
        }:
            raise ValueError("budget head has invalid fields")
        open_runs = head["open"]
        pending_settlements = head["pending_settlements"]
        outstanding = head["outstanding"]
        if (
            head["schema_version"] != "milk.campaign-budget-head.v4"
            or head["campaign_id"] != self.campaign_id
            or type(head["revision"]) is not int
            or head["revision"] < 0
            or type(head["committed_microusd"]) is not int
            or head["committed_microusd"] < 0
            or type(head["reserved_microusd"]) is not int
            or head["reserved_microusd"] < 0
            or type(head["ceiling_breached"]) is not bool
            or not isinstance(open_runs, dict)
            or any(
                not isinstance(run_id, str)
                or HEX64.fullmatch(run_id) is None
                or type(amount) is not int
                or amount <= 0
                for run_id, amount in open_runs.items()
            )
            or not isinstance(pending_settlements, dict)
            or any(
                not isinstance(run_id, str)
                or HEX64.fullmatch(run_id) is None
                or not isinstance(settlement, dict)
                or set(settlement)
                != {
                    "reserved_microusd",
                    "accounted_microusd",
                    "reservation_sha256",
                    "provider_role",
                    "provider_identity",
                    "pricing_receipt_sha256",
                    "provider_rate_microusd_per_minute",
                    "reservation_rate_microusd_per_minute",
                    "intent_sha256",
                }
                or type(settlement["reserved_microusd"]) is not int
                or settlement["reserved_microusd"] <= 0
                or type(settlement["accounted_microusd"]) is not int
                or settlement["accounted_microusd"] < 0
                or HEX64.fullmatch(settlement.get("reservation_sha256", "")) is None
                or HEX64.fullmatch(settlement.get("pricing_receipt_sha256", "")) is None
                or type(settlement.get("provider_rate_microusd_per_minute")) is not int
                or type(settlement.get("reservation_rate_microusd_per_minute")) is not int
                or not isinstance(settlement["intent_sha256"], str)
                or HEX64.fullmatch(settlement["intent_sha256"]) is None
                for run_id, settlement in pending_settlements.items()
            )
            or not isinstance(outstanding, dict)
            or any(
                not isinstance(run_id, str)
                or HEX64.fullmatch(run_id) is None
                or not isinstance(entry, dict)
                or set(entry)
                != {
                    "preparation_sha256",
                    "preparation_deadline_epoch_seconds",
                    "provider_deadline_epoch_seconds",
                    "provider_role",
                    "provider_identity",
                    "state",
                }
                or not isinstance(entry.get("preparation_sha256"), str)
                or HEX64.fullmatch(entry["preparation_sha256"]) is None
                or type(entry.get("preparation_deadline_epoch_seconds")) is not int
                or entry["preparation_deadline_epoch_seconds"] <= 0
                or type(entry.get("provider_deadline_epoch_seconds")) is not int
                or entry["provider_deadline_epoch_seconds"]
                < entry["preparation_deadline_epoch_seconds"]
                or not isinstance(entry.get("provider_identity"), dict)
                or entry.get("state") not in {"preparing", "reserved"}
                for run_id, entry in outstanding.items()
            )
            or set(open_runs).intersection(pending_settlements)
            or not set(open_runs).issubset(outstanding)
            or not set(pending_settlements).issubset(outstanding)
            or any(outstanding[run_id]["state"] != "reserved" for run_id in open_runs)
            or any(
                outstanding[run_id]["state"] != "reserved"
                for run_id in pending_settlements
            )
            or sum(open_runs.values()) != head["reserved_microusd"]
        ):
            raise ValueError("budget head is invalid")
        for entry in outstanding.values():
            identity = _validate_provider_identity(
                entry["provider_identity"], self.authority
            )
            if entry["provider_role"] != _provider_role(identity):
                raise ValueError("budget head has an invalid provider role")
        for run_id, amount_microusd in open_runs.items():
            preparation = outstanding[run_id]
            self._load_reservation_intent(
                run_id,
                amount_microusd,
                preparation["preparation_sha256"],
                preparation["provider_deadline_epoch_seconds"],
                preparation["provider_identity"],
            )
        for run_id, settlement in pending_settlements.items():
            self._validate_pricing_binding(settlement)
            reservation = self._reservation(run_id)
            if (
                settlement["reservation_sha256"]
                != hashlib.sha256(canonical_json(reservation)).hexdigest()
                or settlement["reserved_microusd"]
                != reservation["amount_microusd"]
                or self._pricing_binding_from(settlement)
                != self._pricing_binding_from(reservation)
                or settlement["provider_identity"]
                != outstanding[run_id]["provider_identity"]
                or settlement["provider_role"]
                != outstanding[run_id]["provider_role"]
            ):
                raise ValueError("budget pending settlement differs from reservation")
        if head["committed_microusd"] + head["reserved_microusd"] > ABSOLUTE_CEILING_MICROUSD:
            if not head["ceiling_breached"]:
                raise ValueError("budget head exceeds its ceiling without a breach marker")
        return head, etag

    def _proposal(self, head, etag):
        body = canonical_json(head)
        digest = hashlib.sha256(body).hexdigest()
        key = f"{self.prefix}/revisions/{head['revision']:020d}-{digest}.json"
        proposal = {
            "schema_version": "milk.campaign-budget-revision.v4",
            "campaign_id": self.campaign_id,
            "parent_etag": etag,
            "head_sha256": digest,
            "head": head,
        }
        create_same(self.store, key, canonical_json(proposal), "application/json")
        return body, digest

    def prepare(
        self,
        run_id,
        preparation_sha256,
        preparation_deadline_epoch_seconds,
        provider_deadline_epoch_seconds,
    ):
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        if not isinstance(preparation_sha256, str) or HEX64.fullmatch(preparation_sha256) is None:
            raise ValueError("preparation SHA-256 is invalid")
        if (
            type(preparation_deadline_epoch_seconds) is not int
            or type(provider_deadline_epoch_seconds) is not int
            or preparation_deadline_epoch_seconds <= 0
            or provider_deadline_epoch_seconds < preparation_deadline_epoch_seconds
        ):
            raise ValueError("preparation deadlines are invalid")
        entry = {
            "preparation_sha256": preparation_sha256,
            "preparation_deadline_epoch_seconds": preparation_deadline_epoch_seconds,
            "provider_deadline_epoch_seconds": provider_deadline_epoch_seconds,
            "provider_role": self.provider_role,
            "provider_identity": self.provider_identity,
            "state": "preparing",
        }
        try:
            self.store.get(f"{self.prefix}/settlements/{run_id}.json")
        except FileNotFoundError:
            pass
        else:
            raise ValueError("settled run cannot be prepared again")
        try:
            self.store.get(f"{self.prefix}/reservation-rejections/{run_id}.json")
        except FileNotFoundError:
            pass
        else:
            raise ValueError("rejected run cannot be prepared again")
        current = _utc(self.now(), "clock")
        if preparation_deadline_epoch_seconds <= int(current.timestamp()):
            raise ValueError("preparation deadlines are invalid")
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            existing = head["outstanding"].get(run_id)
            if existing is not None:
                existing_base = {
                    **existing,
                    "provider_role": self.provider_role,
                    "provider_identity": self.provider_identity,
                    "state": "preparing",
                }
                if existing_base != entry:
                    raise ValueError("run already has a different preparation")
                if (
                    existing["provider_role"] == self.provider_role
                    and existing["provider_identity"] == self.provider_identity
                ):
                    return "existing"
                if (
                    existing["state"] != "preparing"
                    or run_id in head["open"]
                    or run_id in head["pending_settlements"]
                ):
                    raise ValueError("run provider is frozen")
                try:
                    self.store.get(
                        f"{self.prefix}/reservation-intents/{run_id}.json"
                    )
                except FileNotFoundError:
                    pass
                else:
                    raise ValueError("run provider is frozen by its reservation intent")
                next_head = dict(head)
                next_head["revision"] += 1
                next_head["outstanding"] = dict(head["outstanding"])
                next_head["outstanding"][run_id] = entry
                body, unused_head_sha256 = self._proposal(next_head, etag)
                del unused_head_sha256
                if self.store.replace(
                    self.head_key,
                    body,
                    etag,
                    "application/json",
                ):
                    return "switched"
                continue
            if run_id in head["open"] or run_id in head["pending_settlements"]:
                raise ValueError("run has budget state without preparation")
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["outstanding"] = dict(head["outstanding"])
            next_head["outstanding"][run_id] = entry
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                return "created"
        raise RuntimeError("budget preparation CAS contention exceeded its bound")

    def reserve(self, run_id, amount_microusd, preparation_sha256):
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        if type(amount_microusd) is not int or not 1 <= amount_microusd <= ABSOLUTE_CEILING_MICROUSD:
            raise ValueError("reservation amount is invalid")
        if not isinstance(preparation_sha256, str) or HEX64.fullmatch(preparation_sha256) is None:
            raise ValueError("preparation SHA-256 is invalid")
        settlement_key = f"{self.prefix}/settlements/{run_id}.json"
        try:
            self.store.get(settlement_key)
        except FileNotFoundError:
            pass
        else:
            raise ValueError("settled run cannot be reserved again")
        head, unused_etag = self._head()
        del unused_etag
        preparation = head["outstanding"].get(run_id)
        if preparation is None:
            raise ValueError("run has no budget preparation")
        if preparation["preparation_sha256"] != preparation_sha256:
            raise ValueError("run has a different preparation")
        if preparation["provider_identity"] != self.provider_identity:
            raise ValueError("run is prepared for a different provider")
        if preparation["provider_role"] != self.provider_role:
            raise ValueError("run is prepared for a different provider role")
        provider_deadline_epoch_seconds = preparation["provider_deadline_epoch_seconds"]
        intent_key = f"{self.prefix}/reservation-intents/{run_id}.json"
        rejection_key = f"{self.prefix}/reservation-rejections/{run_id}.json"
        try:
            rejection = _object(self.store.get(rejection_key))
        except FileNotFoundError:
            rejection = None
        if rejection is not None:
            self._validate_reservation_rejection(
                rejection,
                run_id,
                amount_microusd,
                preparation_sha256,
                provider_deadline_epoch_seconds,
                self.provider_identity,
            )
            return rejection
        try:
            intent = self._load_reservation_intent(
                run_id,
                amount_microusd,
                preparation_sha256,
                provider_deadline_epoch_seconds,
                self.provider_identity,
            )
        except FileNotFoundError:
            intent = None
        existing = head["open"].get(run_id)
        if existing is not None:
            if existing != amount_microusd:
                raise ValueError("run already has a different reservation")
            if preparation["state"] != "reserved":
                raise ValueError("run reservation state is invalid")
            if intent is None:
                raise ValueError("reserved run has no reservation intent")
            receipt = self._reservation_receipt(intent)
            create_same(
                self.store,
                f"{self.prefix}/reservations/{run_id}.json",
                canonical_json(receipt),
                "application/json",
            )
            return receipt
        if run_id in head["pending_settlements"]:
            raise ValueError("settled run cannot be reserved again")
        if preparation["state"] != "preparing":
            raise ValueError("run budget preparation is not open")
        current = _utc(self.now(), "clock")
        if preparation["preparation_deadline_epoch_seconds"] <= int(current.timestamp()):
            raise ValueError("budget preparation expired")
        if intent is None:
            intent = {
                "schema_version": "milk.campaign-budget-reservation-intent.v4",
                "campaign_id": self.campaign_id,
                "run_id": run_id,
                "amount_microusd": amount_microusd,
                "preparation_sha256": preparation_sha256,
                "provider_deadline_epoch_seconds": provider_deadline_epoch_seconds,
                **self._current_pricing(),
            }
        intent_raw = canonical_json(intent)
        intent_sha256 = hashlib.sha256(intent_raw).hexdigest()
        create_same(
            self.store,
            intent_key,
            intent_raw,
            "application/json",
        )
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            existing = head["open"].get(run_id)
            if existing is not None:
                if existing != amount_microusd:
                    raise ValueError("run already has a different reservation")
                if head["outstanding"].get(run_id) != {
                    **preparation,
                    "state": "reserved",
                }:
                    raise ValueError("run already has a different preparation")
                receipt = self._reservation_receipt(intent)
                create_same(
                    self.store,
                    f"{self.prefix}/reservations/{run_id}.json",
                    canonical_json(receipt),
                    "application/json",
                )
                return receipt
            if run_id in head["pending_settlements"]:
                raise ValueError("settled run cannot be reserved again")
            current_preparation = head["outstanding"].get(run_id)
            if current_preparation != preparation:
                intent_preparation = {
                    **current_preparation,
                    "provider_role": self.provider_role,
                    "provider_identity": self.provider_identity,
                } if isinstance(current_preparation, dict) else None
                if (
                    intent_preparation != preparation
                    or current_preparation["state"] != "preparing"
                    or run_id in head["open"]
                    or run_id in head["pending_settlements"]
                ):
                    raise ValueError("run budget preparation changed")
                next_head = dict(head)
                next_head["revision"] += 1
                next_head["outstanding"] = dict(head["outstanding"])
                next_head["outstanding"][run_id] = preparation
                body, unused_head_sha256 = self._proposal(next_head, etag)
                del unused_head_sha256
                self.store.replace(
                    self.head_key,
                    body,
                    etag,
                    "application/json",
                )
                continue
            projected = (
                head["committed_microusd"]
                + head["reserved_microusd"]
                + amount_microusd
            )
            if head["ceiling_breached"] or projected > LAUNCH_CUTOFF_MICROUSD:
                rejection = {
                    "schema_version": "milk.campaign-budget-reservation-rejection.v4",
                    "campaign_id": self.campaign_id,
                    "run_id": run_id,
                    "amount_microusd": amount_microusd,
                    "preparation_sha256": preparation_sha256,
                    "provider_deadline_epoch_seconds": provider_deadline_epoch_seconds,
                    "intent_sha256": intent_sha256,
                    "observed_revision": head["revision"],
                    "projected_microusd": projected,
                    "launch_cutoff_microusd": LAUNCH_CUTOFF_MICROUSD,
                    "state": "blocked",
                    **self._pricing_binding_from(intent),
                }
                create_same(
                    self.store,
                    rejection_key,
                    canonical_json(rejection),
                    "application/json",
                )
                return rejection
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["open"] = dict(head["open"])
            next_head["open"][run_id] = amount_microusd
            next_head["outstanding"] = dict(head["outstanding"])
            next_head["outstanding"][run_id] = {
                **preparation,
                "state": "reserved",
            }
            next_head["reserved_microusd"] += amount_microusd
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                receipt = self._reservation_receipt(intent)
                create_same(
                    self.store,
                    f"{self.prefix}/reservations/{run_id}.json",
                    canonical_json(receipt),
                    "application/json",
                )
                return receipt
        raise RuntimeError("budget reservation CAS contention exceeded its bound")

    def _pricing_binding_from(self, value):
        return {
            key: value[key]
            for key in (
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
            )
        }

    def _validate_reservation_intent(
        self,
        value,
        run_id,
        amount_microusd,
        preparation_sha256,
        provider_deadline_epoch_seconds,
        provider_identity,
    ):
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "amount_microusd",
                "preparation_sha256",
                "provider_deadline_epoch_seconds",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
            }
            or value.get("schema_version")
            != "milk.campaign-budget-reservation-intent.v4"
            or value.get("campaign_id") != self.campaign_id
            or value.get("run_id") != run_id
            or value.get("amount_microusd") != amount_microusd
            or value.get("preparation_sha256") != preparation_sha256
            or value.get("provider_deadline_epoch_seconds")
            != provider_deadline_epoch_seconds
            or value.get("provider_identity") != provider_identity
            or value.get("provider_role") != _provider_role(provider_identity)
        ):
            raise ValueError("run already has a different reservation intent")
        self._validate_pricing_binding(value)
        return value

    def _load_reservation_intent(
        self,
        run_id,
        amount_microusd,
        preparation_sha256,
        provider_deadline_epoch_seconds,
        provider_identity,
    ):
        raw = self.store.get(f"{self.prefix}/reservation-intents/{run_id}.json")
        value = _object(raw)
        if canonical_json(value) != raw:
            raise ValueError("stored reservation intent is not canonical")
        return self._validate_reservation_intent(
            value,
            run_id,
            amount_microusd,
            preparation_sha256,
            provider_deadline_epoch_seconds,
            provider_identity,
        )

    def _validate_reservation_rejection(
        self,
        value,
        run_id,
        amount_microusd,
        preparation_sha256,
        provider_deadline_epoch_seconds,
        provider_identity,
    ):
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "amount_microusd",
                "preparation_sha256",
                "provider_deadline_epoch_seconds",
                "intent_sha256",
                "observed_revision",
                "projected_microusd",
                "launch_cutoff_microusd",
                "state",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
            }
            or value.get("schema_version")
            != "milk.campaign-budget-reservation-rejection.v4"
            or value.get("campaign_id") != self.campaign_id
            or value.get("run_id") != run_id
            or value.get("amount_microusd") != amount_microusd
            or value.get("preparation_sha256") != preparation_sha256
            or value.get("provider_deadline_epoch_seconds")
            != provider_deadline_epoch_seconds
            or value.get("provider_identity") != provider_identity
            or value.get("provider_role") != _provider_role(provider_identity)
            or HEX64.fullmatch(value.get("intent_sha256", "")) is None
            or type(value.get("observed_revision")) is not int
            or value["observed_revision"] < 0
            or type(value.get("projected_microusd")) is not int
            or value["projected_microusd"] <= LAUNCH_CUTOFF_MICROUSD
            or value.get("launch_cutoff_microusd") != LAUNCH_CUTOFF_MICROUSD
            or value.get("state") != "blocked"
        ):
            raise ValueError("stored rejected reservation is invalid")
        self._validate_pricing_binding(value)
        try:
            intent = self._load_reservation_intent(
                run_id,
                amount_microusd,
                preparation_sha256,
                provider_deadline_epoch_seconds,
                provider_identity,
            )
        except FileNotFoundError as error:
            raise ValueError("stored rejected reservation intent is missing") from error
        if (
            value["intent_sha256"]
            != hashlib.sha256(canonical_json(intent)).hexdigest()
            or self._pricing_binding_from(value)
            != self._pricing_binding_from(intent)
        ):
            raise ValueError("stored rejected reservation differs from its intent")
        return value

    def _reservation_receipt(self, intent):
        return {
            "schema_version": "milk.campaign-budget-reservation.v4",
            "campaign_id": self.campaign_id,
            "run_id": intent["run_id"],
            "amount_microusd": intent["amount_microusd"],
            "preparation_sha256": intent["preparation_sha256"],
            "provider_deadline_epoch_seconds": intent[
                "provider_deadline_epoch_seconds"
            ],
            "intent_sha256": hashlib.sha256(canonical_json(intent)).hexdigest(),
            "state": "reserved",
            **self._pricing_binding_from(intent),
        }

    def _reservation(self, run_id):
        key = f"{self.prefix}/reservations/{run_id}.json"
        try:
            raw = self.store.get(key)
        except FileNotFoundError as error:
            raise ValueError("run has no reservation receipt") from error
        value = _object(raw)
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "amount_microusd",
                "preparation_sha256",
                "provider_deadline_epoch_seconds",
                "intent_sha256",
                "state",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
            }
            or value.get("schema_version")
            != "milk.campaign-budget-reservation.v4"
            or value.get("campaign_id") != self.campaign_id
            or value.get("run_id") != run_id
            or type(value.get("amount_microusd")) is not int
            or not 1 <= value["amount_microusd"] <= ABSOLUTE_CEILING_MICROUSD
            or HEX64.fullmatch(value.get("preparation_sha256", "")) is None
            or type(value.get("provider_deadline_epoch_seconds")) is not int
            or value["provider_deadline_epoch_seconds"] <= 0
            or HEX64.fullmatch(value.get("intent_sha256", "")) is None
            or value.get("state") != "reserved"
        ):
            raise ValueError("stored budget reservation is invalid")
        self._validate_pricing_binding(value)
        try:
            intent = self._load_reservation_intent(
                run_id,
                value["amount_microusd"],
                value["preparation_sha256"],
                value["provider_deadline_epoch_seconds"],
                value["provider_identity"],
            )
        except FileNotFoundError as error:
            raise ValueError("stored budget reservation intent is missing") from error
        if value != self._reservation_receipt(intent) or raw != canonical_json(value):
            raise ValueError("stored budget reservation is invalid")
        return value

    def _pending_settlement(self, settlement):
        return {
            key: settlement[key]
            for key in (
                "reserved_microusd",
                "accounted_microusd",
                "reservation_sha256",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
                "intent_sha256",
            )
        }

    def _settlement_receipt(self, reservation, accounted_microusd, intent_sha256):
        return {
            "schema_version": "milk.campaign-budget-settlement.v4",
            "campaign_id": self.campaign_id,
            "run_id": reservation["run_id"],
            "reserved_microusd": reservation["amount_microusd"],
            "accounted_microusd": accounted_microusd,
            "reservation_sha256": hashlib.sha256(
                canonical_json(reservation)
            ).hexdigest(),
            "intent_sha256": intent_sha256,
            "state": "settled",
            **self._pricing_binding_from(reservation),
        }

    def settle(self, run_id, accounted_microusd):
        self._authorized()
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        if type(accounted_microusd) is not int or accounted_microusd < 0:
            raise ValueError("accounted cost is invalid")
        settlement_key = f"{self.prefix}/settlements/{run_id}.json"
        existing = self.settlement(run_id)
        if existing is not None:
            if existing["provider_identity"] != self.provider_identity:
                raise ValueError("run settlement belongs to a different provider")
            if existing["accounted_microusd"] != accounted_microusd:
                raise ValueError("run already has a different settlement")
            self._ensure_pending_settlement(run_id, existing)
            return existing
        reservation = self._reservation(run_id)
        if reservation["provider_identity"] != self.provider_identity:
            raise ValueError("run reservation belongs to a different provider")
        reservation_sha256 = hashlib.sha256(canonical_json(reservation)).hexdigest()
        intent = {
            "schema_version": "milk.campaign-budget-settlement-intent.v4",
            "campaign_id": self.campaign_id,
            "run_id": run_id,
            "accounted_microusd": accounted_microusd,
            "reservation_sha256": reservation_sha256,
            **self._pricing_binding_from(reservation),
        }
        intent_raw = canonical_json(intent)
        intent_sha256 = hashlib.sha256(intent_raw).hexdigest()
        create_same(
            self.store,
            f"{self.prefix}/settlement-intents/{run_id}.json",
            intent_raw,
            "application/json",
        )
        receipt = self._settlement_receipt(
            reservation,
            accounted_microusd,
            intent_sha256,
        )
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            pending = head["pending_settlements"].get(run_id)
            if pending is not None:
                if pending != self._pending_settlement(receipt):
                    raise ValueError("run has a different pending settlement")
                create_same(
                    self.store,
                    settlement_key,
                    canonical_json(receipt),
                    "application/json",
                )
                return receipt
            reserved = head["open"].get(run_id)
            if reserved is None:
                raise ValueError("run has no open reservation")
            if reserved != reservation["amount_microusd"]:
                raise ValueError("budget head differs from reservation receipt")
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["open"] = dict(head["open"])
            del next_head["open"][run_id]
            next_head["pending_settlements"] = dict(head["pending_settlements"])
            next_head["pending_settlements"][run_id] = self._pending_settlement(
                receipt
            )
            next_head["reserved_microusd"] -= reserved
            next_head["committed_microusd"] += accounted_microusd
            next_head["ceiling_breached"] = (
                next_head["committed_microusd"]
                + next_head["reserved_microusd"]
                > ABSOLUTE_CEILING_MICROUSD
            )
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                continue
        raise RuntimeError("budget settlement CAS contention exceeded its bound")

    def _ensure_pending_settlement(self, run_id, settlement):
        finalization_key = f"{self.prefix}/finalizations/{run_id}.json"
        expected_finalization = self._finalization(run_id, settlement)
        try:
            if self.store.get(finalization_key) != canonical_json(expected_finalization):
                raise ValueError("run has a different finalization intent")
            return
        except FileNotFoundError:
            pass
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            pending = head["pending_settlements"].get(run_id)
            if pending is not None:
                if pending != self._pending_settlement(settlement):
                    raise ValueError("run has a different pending settlement")
                return
            if run_id in head["open"]:
                raise ValueError("settlement exists while reservation remains open")
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["pending_settlements"] = dict(head["pending_settlements"])
            next_head["pending_settlements"][run_id] = self._pending_settlement(
                settlement
            )
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                return
        raise RuntimeError("budget settlement repair CAS contention exceeded its bound")

    def finalize(self, run_id):
        self._authorized()
        settlement = self.settlement(run_id)
        if settlement is None:
            raise ValueError("run has no settlement")
        if settlement["provider_identity"] != self.provider_identity:
            raise ValueError("run settlement belongs to a different provider")
        receipt = self._finalization(run_id, settlement)
        create_same(
            self.store,
            f"{self.prefix}/finalizations/{run_id}.json",
            canonical_json(receipt),
            "application/json",
        )
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            pending = head["pending_settlements"].get(run_id)
            if pending is None:
                if run_id in head["open"]:
                    raise ValueError("finalized run still has an open reservation")
                if run_id not in head["outstanding"]:
                    return receipt
            if pending != self._pending_settlement(settlement):
                raise ValueError("run has a different pending settlement")
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["pending_settlements"] = dict(head["pending_settlements"])
            if pending is not None:
                del next_head["pending_settlements"][run_id]
            next_head["outstanding"] = dict(head["outstanding"])
            next_head["outstanding"].pop(run_id, None)
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                break
        else:
            raise RuntimeError("budget finalization CAS contention exceeded its bound")
        return receipt

    def _finalization(self, run_id, settlement):
        return {
            "schema_version": "milk.campaign-budget-finalization.v4",
            "campaign_id": self.campaign_id,
            "run_id": run_id,
            "provider_role": settlement["provider_role"],
            "provider_identity": settlement["provider_identity"],
            "settlement_sha256": hashlib.sha256(canonical_json(settlement)).hexdigest(),
            "state": "committed",
        }

    def complete_unpaid(self, run_id):
        self._authorized()
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        for _ in range(MAX_CAS_ATTEMPTS):
            head, etag = self._head()
            if run_id in head["open"] or run_id in head["pending_settlements"]:
                raise ValueError("paid run cannot be completed without settlement")
            if run_id not in head["outstanding"]:
                return
            if (
                head["outstanding"][run_id]["provider_identity"]
                != self.provider_identity
                or head["outstanding"][run_id]["provider_role"]
                != self.provider_role
            ):
                raise ValueError("run preparation belongs to a different provider")
            next_head = dict(head)
            next_head["revision"] += 1
            next_head["outstanding"] = dict(head["outstanding"])
            del next_head["outstanding"][run_id]
            body, unused_head_sha256 = self._proposal(next_head, etag)
            del unused_head_sha256
            if self.store.replace(self.head_key, body, etag, "application/json"):
                return
        raise RuntimeError("budget unpaid completion CAS contention exceeded its bound")

    def settlement(self, run_id):
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        key = f"{self.prefix}/settlements/{run_id}.json"
        try:
            raw = self.store.get(key)
        except FileNotFoundError:
            return None
        value = _object(raw)
        if (
            set(value)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "reserved_microusd",
                "accounted_microusd",
                "reservation_sha256",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
                "intent_sha256",
                "state",
            }
            or value.get("schema_version") != "milk.campaign-budget-settlement.v4"
            or value.get("campaign_id") != self.campaign_id
            or value.get("run_id") != run_id
            or type(value.get("reserved_microusd")) is not int
            or value["reserved_microusd"] <= 0
            or type(value.get("accounted_microusd")) is not int
            or value["accounted_microusd"] < 0
            or HEX64.fullmatch(value.get("reservation_sha256", "")) is None
            or not isinstance(value.get("intent_sha256"), str)
            or HEX64.fullmatch(value["intent_sha256"]) is None
            or value.get("state") != "settled"
        ):
            raise ValueError("stored budget settlement is invalid")
        self._validate_pricing_binding(value)
        reservation = self._reservation(run_id)
        reservation_sha256 = hashlib.sha256(canonical_json(reservation)).hexdigest()
        try:
            intent_raw = self.store.get(
                f"{self.prefix}/settlement-intents/{run_id}.json"
            )
        except FileNotFoundError as error:
            raise ValueError("stored budget settlement intent is missing") from error
        intent = _object(intent_raw)
        if (
            set(intent)
            != {
                "schema_version",
                "campaign_id",
                "run_id",
                "accounted_microusd",
                "reservation_sha256",
                "provider_role",
                "provider_identity",
                "pricing_receipt_sha256",
                "provider_rate_microusd_per_minute",
                "reservation_rate_microusd_per_minute",
            }
            or intent.get("schema_version")
            != "milk.campaign-budget-settlement-intent.v4"
            or intent.get("campaign_id") != self.campaign_id
            or intent.get("run_id") != run_id
            or intent.get("accounted_microusd") != value["accounted_microusd"]
            or intent.get("reservation_sha256") != reservation_sha256
            or canonical_json(intent) != intent_raw
        ):
            raise ValueError("stored budget settlement intent is invalid")
        self._validate_pricing_binding(intent)
        intent_sha256 = hashlib.sha256(intent_raw).hexdigest()
        if (
            value
            != self._settlement_receipt(
                reservation,
                value["accounted_microusd"],
                intent_sha256,
            )
            or raw != canonical_json(value)
        ):
            raise ValueError("stored budget settlement is invalid")
        return value

    def status(self):
        head, unused_etag = self._head()
        del unused_etag
        return head
