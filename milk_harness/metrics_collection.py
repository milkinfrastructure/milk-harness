from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re

from milk_harness.evidence import HEX64, canonical_json


MAX_PROVIDER_BYTES = 1024 * 1024
MAX_SERIES_BYTES = MAX_PROVIDER_BYTES - 128 * 1024
MAX_ATTEMPTS = 3
MAX_EXECUTIONS = 3
MAX_NODES = 100_000
MAX_DEPTH = 12
STEP_SECONDS = 60
TAIL_MILLISECONDS = 60_000
PROVIDER_CLOCK_SKEW_MILLISECONDS = 300_000
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
UTC_RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z"
)
ATTEMPT_FILE = re.compile(r"(0[0-2])-(intent|result)\.json\Z")
TERMINAL_STATUSES = {
    "TRAINING_JOB_COMPLETED",
    "TRAINING_JOB_DEPLOY_FAILED",
    "TRAINING_JOB_FAILED",
    "TRAINING_JOB_PREEMPTED",
    "TRAINING_JOB_STOPPED",
}
ENVELOPE_KEYS = {
    "gpu_memory_usage_bytes",
    "gpu_utilization",
    "cpu_usage",
    "cpu_memory_usage_bytes",
    "ephemeral_storage",
    "training_job",
    "cache",
    "per_node_metrics",
}
JOB_KEYS = {
    "id",
    "created_at",
    "current_status",
    "instance_type",
    "updated_at",
    "training_project_id",
    "training_project",
    "error_message",
    "node_count",
    "name",
    "checkpoint_sync_status",
    "priority",
    "availability_model",
    "user",
}
JOB_REQUIRED_KEYS = {
    "id",
    "created_at",
    "current_status",
    "instance_type",
    "updated_at",
    "training_project_id",
    "training_project",
    "node_count",
    "name",
    "availability_model",
}
DUPLICATE_KEYS = {
    "state",
    "ordinal",
    "execution_run_id",
    "execution_name",
    "duplicate_set_sha256",
    "max_wall_seconds",
    "matches",
    "billed_minutes",
    "accounted_microusd",
}
DUPLICATE_MATCH_KEYS = {
    "observation_sha256",
    "provider",
    "project_id",
    "execution_id",
    "execution_name",
    "gpu_type",
    "gpu_count",
    "availability_model",
    "node_count",
    "status",
    "created_at",
    "updated_at",
    "elapsed_seconds",
    "billed_minutes",
    "accounted_microusd",
}
INSTANCE_KEYS = {
    "id",
    "name",
    "memory_limit_mib",
    "millicpu_limit",
    "gpu_count",
    "gpu_type",
    "gpu_memory_limit_mib",
}
METRIC_KEYS = {
    "gpu_memory_usage_bytes",
    "gpu_utilization",
    "cpu_usage",
    "cpu_memory_usage_bytes",
    "ephemeral_storage",
}
NORMALIZED_KEYS = METRIC_KEYS | {
    "cache",
    "per_node_metrics",
    "usage_bytes",
    "utilization",
    "gpu_rank",
    "samples",
    "timestamp_epoch_millis",
    "value_int",
    "value_decimal",
    "node_id_bytes",
    "node_id_sha256",
    "metrics",
}


def _strict_json(raw):
    if not isinstance(raw, (bytes, bytearray)) or len(raw) > MAX_PROVIDER_BYTES:
        raise ValueError("provider metrics must be bounded bytes")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("provider metrics contain a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict):
        raise ValueError("provider metrics must be an object")
    return value


def _milliseconds(value, label):
    if not isinstance(value, str) or UTC_RFC3339.fullmatch(value) is None:
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC RFC3339")
    delta = parsed - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    millis = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    if millis < 0:
        raise ValueError(f"{label} predates the Unix epoch")
    return millis


def _current_millis(now):
    if not callable(now):
        raise ValueError("metrics clock must be callable")
    current = now()
    if (
        not isinstance(current, dt.datetime)
        or current.tzinfo is None
        or current.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("metrics clock must return UTC")
    return _milliseconds(
        current.astimezone(dt.timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "metrics clock",
    )


def _identity(evidence, project_id):
    prefix = getattr(evidence, "prefix", None)
    parts = prefix.split("/") if isinstance(prefix, str) else []
    if (
        len(parts) != 5
        or parts[:2] != ["campaigns", "v1"]
        or parts[3] != "jobs"
        or HEX64.fullmatch(parts[2]) is None
        or HEX64.fullmatch(parts[4]) is None
        or getattr(evidence, "run_id", None) != parts[4]
        or not hasattr(evidence, "store")
        or not isinstance(project_id, str)
        or IDENTIFIER.fullmatch(project_id) is None
    ):
        raise ValueError("metrics evidence or Baseten project identity is invalid")
    return parts[2], parts[4]


def _load(evidence, relative):
    return _strict_json(evidence.store.get(f"{evidence.prefix}/{relative}"))


def _maybe(evidence, relative):
    try:
        return _load(evidence, relative)
    except FileNotFoundError:
        return None


def _put(evidence, relative, value):
    key = f"{evidence.prefix}/{relative}"
    body = canonical_json(value)
    if evidence.store.create(key, body, "application/json"):
        return value, True
    return _strict_json(evidence.store.get(key)), False


def _plan(evidence, campaign_id, run_id, project_id, executions, observed_at):
    observed_millis = _milliseconds(observed_at, "terminal observed_at")
    if not isinstance(executions, list) or not 1 <= len(executions) <= MAX_EXECUTIONS:
        raise ValueError("one to three terminal metric executions are required")
    planned, ordinals, execution_ids = [], set(), set()
    for execution in executions:
        if not isinstance(execution, dict):
            raise ValueError("terminal metric execution is invalid")
        ordinal = execution.get("ordinal")
        if type(ordinal) is not int or not 0 <= ordinal <= 99 or ordinal in ordinals:
            raise ValueError("terminal metric execution ordinal is invalid")
        if execution.get("state") == "duplicate_create":
            name = execution.get("execution_name")
            matches = execution.get("matches")
            if (
                set(execution) != DUPLICATE_KEYS
                or not isinstance(name, str)
                or IDENTIFIER.fullmatch(name) is None
                or HEX64.fullmatch(execution.get("execution_run_id") or "") is None
                or HEX64.fullmatch(execution.get("duplicate_set_sha256") or "") is None
                or type(execution.get("max_wall_seconds")) is not int
                or execution["max_wall_seconds"] <= 0
                or not isinstance(matches, list)
                or len(matches) < 2
                or type(execution.get("billed_minutes")) is not int
                or execution["billed_minutes"] < 0
                or type(execution.get("accounted_microusd")) is not int
                or execution["accounted_microusd"] < 0
            ):
                raise ValueError("duplicate metric execution is invalid")
            execution_id_digests = []
            seen_match_ids = set()
            for match in matches:
                if (
                    not isinstance(match, dict)
                    or set(match) != DUPLICATE_MATCH_KEYS
                    or HEX64.fullmatch(match.get("observation_sha256") or "") is None
                    or match.get("provider") != "baseten"
                    or match.get("project_id") != project_id
                    or not isinstance(match.get("execution_id"), str)
                    or IDENTIFIER.fullmatch(match["execution_id"]) is None
                    or match["execution_id"] in seen_match_ids
                    or match["execution_id"] in execution_ids
                    or match.get("execution_name") != name
                    or match.get("gpu_type") != "H100"
                    or type(match.get("gpu_count")) is not int
                    or match["gpu_count"] != 1
                    or match.get("availability_model") != "dedicated"
                    or type(match.get("node_count")) is not int
                    or match["node_count"] != 1
                    or match.get("status") not in TERMINAL_STATUSES
                    or type(match.get("elapsed_seconds")) is not int
                    or match["elapsed_seconds"] <= 0
                    or type(match.get("billed_minutes")) is not int
                    or match["billed_minutes"] <= 0
                    or type(match.get("accounted_microusd")) is not int
                    or match["accounted_microusd"] < 0
                ):
                    raise ValueError("duplicate metric execution match is invalid")
                created = _milliseconds(match.get("created_at"), "provider created_at")
                updated = _milliseconds(
                    match.get("updated_at"), "provider terminal updated_at"
                )
                if updated < created:
                    raise ValueError("provider terminal updated_at precedes created_at")
                seen_match_ids.add(match["execution_id"])
                execution_id_digests.append(
                    hashlib.sha256(match["execution_id"].encode()).hexdigest()
                )
            execution_ids.update(seen_match_ids)
            planned.append(
                {
                    "ordinal": ordinal,
                    "execution_id": None,
                    "execution_name": name,
                    "status": None,
                    "created_at": None,
                    "terminal_updated_at": None,
                    "start_epoch_millis": None,
                    "end_epoch_millis": None,
                    "not_before": None,
                    "step_seconds": STEP_SECONDS,
                    "availability": "unavailable",
                    "duplicate_set_sha256": execution["duplicate_set_sha256"],
                    "duplicate_execution_id_sha256s": sorted(execution_id_digests),
                }
            )
            ordinals.add(ordinal)
            continue
        execution_id = execution.get("execution_id")
        name = execution.get("execution_name")
        if (
            execution.get("state") != "observed_terminal"
            or execution.get("provider") != "baseten"
            or execution.get("project_id") != project_id
            or not isinstance(execution_id, str)
            or IDENTIFIER.fullmatch(execution_id) is None
            or execution_id in execution_ids
            or not isinstance(name, str)
            or IDENTIFIER.fullmatch(name) is None
            or execution.get("gpu_type") != "H100"
            or type(execution.get("gpu_count")) is not int
            or execution["gpu_count"] != 1
            or execution.get("availability_model") != "dedicated"
            or type(execution.get("node_count")) is not int
            or execution["node_count"] != 1
            or execution.get("status") not in TERMINAL_STATUSES
        ):
            raise ValueError("terminal metric execution identity is invalid")
        start = _milliseconds(execution.get("created_at"), "provider created_at")
        updated = _milliseconds(
            execution.get("updated_at"), "provider terminal updated_at"
        )
        if updated < start:
            raise ValueError("provider terminal updated_at precedes created_at")
        end = max(updated, observed_millis) + TAIL_MILLISECONDS
        planned.append(
            {
                "ordinal": ordinal,
                "execution_id": execution_id,
                "execution_name": name,
                "status": execution["status"],
                "created_at": execution["created_at"],
                "terminal_updated_at": execution["updated_at"],
                "start_epoch_millis": start,
                "end_epoch_millis": end,
                "not_before": end,
                "step_seconds": STEP_SECONDS,
                "availability": "resolved",
                "duplicate_set_sha256": None,
                "duplicate_execution_id_sha256s": [],
            }
        )
        ordinals.add(ordinal)
        execution_ids.add(execution_id)
    proposal = {
        "schema_version": "milk.baseten-metric-collection-plan.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "project_id": project_id,
        "terminal_observed_at": observed_at,
        "executions": sorted(planned, key=lambda item: item["ordinal"]),
    }
    stored = _maybe(evidence, "metric-collections/plan.json")
    if stored is None:
        stored, unused_created = _put(evidence, "metric-collections/plan.json", proposal)
        del unused_created
    if stored != proposal:
        raise ValueError("terminal observations differ from the immutable metrics plan")
    return stored


def _count(counter, depth):
    counter[0] += 1
    if counter[0] > MAX_NODES or depth > MAX_DEPTH:
        raise ValueError("Baseten metrics exceed their structural bound")


def _samples(value, start, end, counter, depth):
    _count(counter, depth)
    if not isinstance(value, list):
        raise ValueError("Baseten metric series must be a list")
    normalized = []
    seen = set()
    for item in value:
        _count(counter, depth + 1)
        if not isinstance(item, dict) or set(item) != {"value", "timestamp"}:
            raise ValueError("Baseten metric sample is invalid")
        timestamp = _milliseconds(item["timestamp"], "Baseten metric timestamp")
        metric = item["value"]
        if timestamp in seen or not start <= timestamp <= end:
            raise ValueError("Baseten metric timestamps are duplicate or outside the plan")
        if type(metric) is int:
            sample = {"timestamp_epoch_millis": timestamp, "value_int": metric}
        elif isinstance(metric, float) and math.isfinite(metric):
            sample = {
                "timestamp_epoch_millis": timestamp,
                "value_decimal": format(metric, ".17g"),
            }
        else:
            raise ValueError("Baseten metric sample value is not finite numeric data")
        normalized.append(sample)
        seen.add(timestamp)
    return sorted(normalized, key=lambda item: item["timestamp_epoch_millis"])


def _gpu_series(value, start, end, counter, depth):
    _count(counter, depth)
    if not isinstance(value, dict):
        raise ValueError("Baseten GPU metrics must be a rank map")
    normalized = []
    for rank, samples in value.items():
        if (
            not isinstance(rank, str)
            or len(rank.encode("utf-8")) > 256
            or not rank.isascii()
            or not rank.isdecimal()
            or str(int(rank)) != rank
        ):
            raise ValueError("Baseten GPU rank is invalid")
        normalized.append(
            {
                "gpu_rank": int(rank),
                "samples": _samples(samples, start, end, counter, depth + 1),
            }
        )
    return sorted(normalized, key=lambda item: item["gpu_rank"])


def _storage(value, start, end, counter, depth):
    _count(counter, depth)
    if not isinstance(value, dict) or set(value) != {"usage_bytes", "utilization"}:
        raise ValueError("Baseten storage metrics are invalid")
    return {
        "usage_bytes": _samples(value["usage_bytes"], start, end, counter, depth + 1),
        "utilization": _samples(value["utilization"], start, end, counter, depth + 1),
    }


def _metric_set(value, start, end, counter, depth):
    _count(counter, depth)
    if not isinstance(value, dict) or set(value) != METRIC_KEYS:
        raise ValueError("Baseten node metrics are invalid")
    return {
        "gpu_memory_usage_bytes": _gpu_series(
            value["gpu_memory_usage_bytes"], start, end, counter, depth + 1
        ),
        "gpu_utilization": _gpu_series(
            value["gpu_utilization"], start, end, counter, depth + 1
        ),
        "cpu_usage": _samples(value["cpu_usage"], start, end, counter, depth + 1),
        "cpu_memory_usage_bytes": _samples(
            value["cpu_memory_usage_bytes"], start, end, counter, depth + 1
        ),
        "ephemeral_storage": _storage(
            value["ephemeral_storage"], start, end, counter, depth + 1
        ),
    }


def _job(job, plan, project_id, current_millis):
    if (
        not isinstance(job, dict)
        or not JOB_REQUIRED_KEYS.issubset(job)
        or not set(job).issubset(JOB_KEYS)
        or job.get("id") != plan["execution_id"]
        or job.get("training_project_id") != project_id
        or job.get("name") != plan["execution_name"]
        or job.get("current_status") != plan["status"]
        or job.get("created_at") != plan["created_at"]
        or type(job.get("node_count")) is not int
        or job["node_count"] != 1
        or job.get("availability_model") != "dedicated"
    ):
        raise ValueError("Baseten metrics training-job identity is invalid")
    updated = _milliseconds(job.get("updated_at"), "metrics job updated_at")
    if (
        updated < _milliseconds(plan["terminal_updated_at"], "planned terminal updated_at")
        or updated > current_millis + PROVIDER_CLOCK_SKEW_MILLISECONDS
    ):
        raise ValueError("Baseten metrics job updated_at is outside its observation window")
    project = job.get("training_project")
    instance = job.get("instance_type")
    if (
        not isinstance(project, dict)
        or set(project) != {"id", "name"}
        or project.get("id") != project_id
        or not isinstance(project.get("name"), str)
        or not project["name"]
        or not isinstance(instance, dict)
        or set(instance) != INSTANCE_KEYS
        or instance.get("gpu_type") != "H100"
        or type(instance.get("gpu_count")) is not int
        or instance["gpu_count"] != 1
        or any(
            type(instance.get(key)) is not int or instance[key] <= 0
            for key in ("memory_limit_mib", "millicpu_limit", "gpu_memory_limit_mib")
        )
        or not isinstance(instance.get("id"), str)
        or not instance["id"]
        or not isinstance(instance.get("name"), str)
        or not instance["name"]
    ):
        raise ValueError("Baseten metrics training-job hardware is invalid")
    return _execution_identity(plan, project_id)


def _execution_identity(plan, project_id):
    return {
        "provider": "baseten",
        "project_id": project_id,
        "execution_id": plan["execution_id"],
        "execution_name": plan["execution_name"],
        "gpu_type": "H100",
        "gpu_count": 1,
        "availability_model": "dedicated",
        "node_count": 1,
        "status": plan["status"],
        "created_at": plan["created_at"],
        "terminal_updated_at": plan["terminal_updated_at"],
    }


def _check_normalized(value, *, key=None, depth=0, counter=None):
    if counter is None:
        counter = [0]
    _count(counter, depth)
    if value is None or type(value) is int:
        return
    if isinstance(value, str):
        if key == "node_id_sha256" and HEX64.fullmatch(value) is not None:
            return
        if key == "value_decimal" and len(value) <= 64:
            try:
                parsed = float(value)
            except ValueError:
                parsed = math.nan
            if math.isfinite(parsed):
                return
        raise ValueError("normalized Baseten metrics contain free text")
    if isinstance(value, list):
        for item in value:
            _check_normalized(item, depth=depth + 1, counter=counter)
        return
    if isinstance(value, dict) and set(value).issubset(NORMALIZED_KEYS):
        for name, item in value.items():
            _check_normalized(item, key=name, depth=depth + 1, counter=counter)
        return
    raise ValueError("normalized Baseten metrics are invalid")


def _check_samples(value, start, end):
    if not isinstance(value, list):
        raise ValueError("stored Baseten metric samples are invalid")
    previous = None
    for sample in value:
        if not isinstance(sample, dict) or set(sample) not in (
            {"timestamp_epoch_millis", "value_int"},
            {"timestamp_epoch_millis", "value_decimal"},
        ):
            raise ValueError("stored Baseten metric sample is invalid")
        timestamp = sample["timestamp_epoch_millis"]
        if (
            type(timestamp) is not int
            or not start <= timestamp <= end
            or previous is not None
            and timestamp <= previous
        ):
            raise ValueError("stored Baseten metric timestamp is invalid")
        if "value_int" in sample:
            if type(sample["value_int"]) is not int:
                raise ValueError("stored Baseten integer metric is invalid")
        else:
            decimal = sample["value_decimal"]
            try:
                parsed = float(decimal)
            except (TypeError, ValueError):
                parsed = math.nan
            if (
                not isinstance(decimal, str)
                or len(decimal) > 64
                or not math.isfinite(parsed)
                or format(parsed, ".17g") != decimal
            ):
                raise ValueError("stored Baseten decimal metric is invalid")
        previous = timestamp


def _check_gpu_series(value, start, end):
    if not isinstance(value, list):
        raise ValueError("stored Baseten GPU metrics are invalid")
    previous = None
    for item in value:
        if (
            not isinstance(item, dict)
            or set(item) != {"gpu_rank", "samples"}
            or type(item["gpu_rank"]) is not int
            or item["gpu_rank"] < 0
            or previous is not None
            and item["gpu_rank"] <= previous
        ):
            raise ValueError("stored Baseten GPU rank is invalid")
        _check_samples(item["samples"], start, end)
        previous = item["gpu_rank"]


def _check_storage(value, start, end):
    if not isinstance(value, dict) or set(value) != {"usage_bytes", "utilization"}:
        raise ValueError("stored Baseten storage metrics are invalid")
    _check_samples(value["usage_bytes"], start, end)
    _check_samples(value["utilization"], start, end)


def _check_metric_set(value, start, end):
    if not isinstance(value, dict) or set(value) != METRIC_KEYS:
        raise ValueError("stored Baseten metric set is invalid")
    _check_gpu_series(value["gpu_memory_usage_bytes"], start, end)
    _check_gpu_series(value["gpu_utilization"], start, end)
    _check_samples(value["cpu_usage"], start, end)
    _check_samples(value["cpu_memory_usage_bytes"], start, end)
    _check_storage(value["ephemeral_storage"], start, end)


def _check_series(value, plan):
    if not isinstance(value, dict) or set(value) != METRIC_KEYS | {
        "cache",
        "per_node_metrics",
    }:
        raise ValueError("stored Baseten metric series are invalid")
    start, end = plan["start_epoch_millis"], plan["end_epoch_millis"]
    _check_metric_set({key: value[key] for key in METRIC_KEYS}, start, end)
    if value["cache"] is not None:
        _check_storage(value["cache"], start, end)
    nodes = value["per_node_metrics"]
    if not isinstance(nodes, list):
        raise ValueError("stored Baseten per-node metrics are invalid")
    previous = None
    for node in nodes:
        if (
            not isinstance(node, dict)
            or set(node) != {"node_id_bytes", "node_id_sha256", "metrics"}
            or type(node["node_id_bytes"]) is not int
            or not 1 <= node["node_id_bytes"] <= 256
            or HEX64.fullmatch(node.get("node_id_sha256") or "") is None
            or previous is not None
            and node["node_id_sha256"] <= previous
        ):
            raise ValueError("stored Baseten metric node is invalid")
        _check_metric_set(node["metrics"], start, end)
        previous = node["node_id_sha256"]
    if len(canonical_json(value)) > MAX_SERIES_BYTES:
        raise ValueError("stored Baseten metric series exceed their evidence bound")
    _check_normalized(value)


def _normalize(raw, plan, project_id, current_millis):
    value = _strict_json(raw)
    if set(value) != ENVELOPE_KEYS:
        raise ValueError("Baseten metrics envelope is invalid")
    counter = [0]
    root = {key: value[key] for key in METRIC_KEYS}
    series = _metric_set(
        root,
        plan["start_epoch_millis"],
        plan["end_epoch_millis"],
        counter,
        0,
    )
    cache = value["cache"]
    series["cache"] = (
        None
        if cache is None
        else _storage(
            cache,
            plan["start_epoch_millis"],
            plan["end_epoch_millis"],
            counter,
            1,
        )
    )
    nodes = value["per_node_metrics"]
    if not isinstance(nodes, list):
        raise ValueError("Baseten per-node metrics are invalid")
    normalized_nodes = []
    node_ids = set()
    node_hashes = set()
    for node in nodes:
        _count(counter, 1)
        if not isinstance(node, dict) or set(node) != {"node_id", "metrics"}:
            raise ValueError("Baseten per-node metric entry is invalid")
        node_id = node["node_id"]
        encoded = node_id.encode("utf-8") if isinstance(node_id, str) else b""
        node_sha256 = hashlib.sha256(encoded).hexdigest()
        if (
            not encoded
            or len(encoded) > 256
            or any(ord(character) < 32 or ord(character) == 127 for character in node_id)
            or node_id in node_ids
            or node_sha256 in node_hashes
        ):
            raise ValueError("Baseten metric node identity is invalid")
        normalized_nodes.append(
            {
                "node_id_bytes": len(encoded),
                "node_id_sha256": node_sha256,
                "metrics": _metric_set(
                    node["metrics"],
                    plan["start_epoch_millis"],
                    plan["end_epoch_millis"],
                    counter,
                    2,
                ),
            }
        )
        node_ids.add(node_id)
        node_hashes.add(node_sha256)
    series["per_node_metrics"] = sorted(
        normalized_nodes, key=lambda item: item["node_id_sha256"]
    )
    _check_series(series, plan)
    return _job(value["training_job"], plan, project_id, current_millis), series


def _attempts(evidence, run_id, plan, project_id):
    prefix = f"{evidence.prefix}/metric-collections/{plan['ordinal']:02d}/attempts"
    keys = evidence.store.list(prefix, limit=7)
    if len(keys) > 6:
        raise ValueError("Baseten metric attempts exceed their bound")
    intents, results = {}, {}
    for key in keys:
        match = ATTEMPT_FILE.fullmatch(key.rsplit("/", 1)[-1])
        if match is None:
            raise ValueError("Baseten metric attempt path is invalid")
        attempt = int(match.group(1))
        target = intents if match.group(2) == "intent" else results
        target[attempt] = _strict_json(evidence.store.get(key))
    if sorted(intents) != list(range(len(intents))) or not set(results).issubset(intents):
        raise ValueError("Baseten metric attempt slots are invalid")
    if plan["availability"] == "unavailable" and (intents or results):
        raise ValueError("unavailable Baseten execution has metric attempts")
    expected_intent = {
        "schema_version": "milk.baseten-metric-attempt.v1",
        "run_id": run_id,
        "ordinal": plan["ordinal"],
        "execution_id": plan["execution_id"],
        "start_epoch_millis": plan["start_epoch_millis"],
        "end_epoch_millis": plan["end_epoch_millis"],
        "step_seconds": STEP_SECONDS,
    }
    for attempt, intent in intents.items():
        if intent != {**expected_intent, "attempt": attempt}:
            raise ValueError("stored Baseten metric attempt is invalid")
    for attempt, result in results.items():
        common = {
            "schema_version",
            "run_id",
            "ordinal",
            "attempt",
            "state",
            "response_bytes",
            "response_sha256",
            "error_type",
            "error_sha256",
            "execution",
            "series",
            "series_sha256",
        }
        if (
            not isinstance(result, dict)
            or set(result) != common
            or result.get("schema_version") != "milk.baseten-metric-attempt-result.v1"
            or result.get("run_id") != run_id
            or result.get("ordinal") != plan["ordinal"]
            or result.get("attempt") != attempt
            or result.get("state") not in {"complete", "failed"}
        ):
            raise ValueError("stored Baseten metric attempt result is invalid")
        response_bytes = result["response_bytes"]
        response_valid = (
            type(response_bytes) is int
            and 0 <= response_bytes <= MAX_PROVIDER_BYTES
            and HEX64.fullmatch(result.get("response_sha256") or "") is not None
        )
        if result["state"] == "complete":
            if (
                not response_valid
                or response_bytes == 0
                or result["error_type"] is not None
                or result["error_sha256"] is not None
                or not isinstance(result["execution"], dict)
                or not isinstance(result["series"], dict)
                or result["execution"] != _execution_identity(plan, project_id)
                or hashlib.sha256(canonical_json(result["series"])).hexdigest()
                != result["series_sha256"]
                or len(canonical_json(result["series"])) > MAX_SERIES_BYTES
            ):
                raise ValueError("stored complete Baseten metrics are invalid")
            _check_series(result["series"], plan)
        elif (
            (response_bytes is not None and not response_valid)
            or (response_bytes is None and result["response_sha256"] is not None)
            or not isinstance(result["error_type"], str)
            or IDENTIFIER.fullmatch(result["error_type"]) is None
            or HEX64.fullmatch(result.get("error_sha256") or "") is None
            or any(result[name] is not None for name in ("execution", "series", "series_sha256"))
        ):
            raise ValueError("stored failed Baseten metrics are invalid")
    successful = [attempt for attempt, result in results.items() if result["state"] == "complete"]
    if successful and any(attempt > min(successful) for attempt in intents):
        raise ValueError("Baseten metric attempt follows a successful response")
    return intents, results


def _execution_summary_value(run_id, plan, intents, results, source):
    known = [result["response_bytes"] for result in results.values() if result["response_bytes"] is not None]
    if source is None:
        return {
            "schema_version": "milk.baseten-metric-execution-summary.v1",
            "run_id": run_id,
            **plan,
            "source_complete": False,
            "missing_reason": "provider_execution_unavailable"
            if plan["availability"] == "unavailable"
            else "request_attempt_bound",
            "attempts_used": len(intents),
            "attempts_without_known_response": len(intents) - len(known),
            "pessimistic_provider_bytes": len(intents) * MAX_PROVIDER_BYTES,
            "actual_response_bytes": sum(known),
            "response_bytes": None,
            "response_sha256": None,
            "execution": None,
            "series": None,
            "series_sha256": None,
        }
    return {
        "schema_version": "milk.baseten-metric-execution-summary.v1",
        "run_id": run_id,
        **plan,
        "source_complete": True,
        "missing_reason": None,
        "attempts_used": len(intents),
        "attempts_without_known_response": len(intents) - len(known),
        "pessimistic_provider_bytes": len(intents) * MAX_PROVIDER_BYTES,
        "actual_response_bytes": sum(known),
        "response_bytes": source["response_bytes"],
        "response_sha256": source["response_sha256"],
        "execution": source["execution"],
        "series": source["series"],
        "series_sha256": source["series_sha256"],
    }


def _execution_summary(evidence, run_id, plan, intents, results, source):
    value = _execution_summary_value(run_id, plan, intents, results, source)
    relative = f"metric-collections/{plan['ordinal']:02d}/summary.json"
    stored, unused_created = _put(evidence, relative, value)
    del unused_created
    if stored != value:
        raise ValueError("stored Baseten metric execution summary differs")
    return stored


def _collect_execution(
    evidence, request, project_id, run_id, plan, current_millis, continue_collection
):
    relative = f"metric-collections/{plan['ordinal']:02d}/summary.json"
    existing = _maybe(evidence, relative)
    intents, results = _attempts(evidence, run_id, plan, project_id)
    if existing is not None:
        source = next(
            (results[index] for index in sorted(results) if results[index]["state"] == "complete"),
            None,
        )
        if source is None and plan["availability"] == "resolved" and len(intents) < MAX_ATTEMPTS:
            raise ValueError("stored incomplete metric summary precedes its attempt bound")
        expected = _execution_summary_value(run_id, plan, intents, results, source)
        if existing != expected:
            raise ValueError("stored Baseten metric execution summary is invalid")
        return existing
    if plan["availability"] == "unavailable":
        return _execution_summary(evidence, run_id, plan, intents, results, None)
    if current_millis < plan["not_before"]:
        return {
            "schema_version": "milk.baseten-metric-collection-pending.v1",
            "run_id": run_id,
            "ordinal": plan["ordinal"],
            "state": "not_before",
            "next_not_before": plan["not_before"],
            "attempts_used": len(intents),
        }
    while True:
        source = next(
            (results[index] for index in sorted(results) if results[index]["state"] == "complete"),
            None,
        )
        if source is not None:
            intents, results = _attempts(evidence, run_id, plan, project_id)
            source = next(
                result
                for index, result in sorted(results.items())
                if result["state"] == "complete"
            )
            return _execution_summary(evidence, run_id, plan, intents, results, source)
        if len(intents) >= MAX_ATTEMPTS:
            intents, results = _attempts(evidence, run_id, plan, project_id)
            source = next(
                (
                    result
                    for index, result in sorted(results.items())
                    if result["state"] == "complete"
                ),
                None,
            )
            return _execution_summary(evidence, run_id, plan, intents, results, source)
        if not continue_collection():
            return {
                "schema_version": "milk.baseten-metric-collection-pending.v1",
                "run_id": run_id,
                "ordinal": plan["ordinal"],
                "state": "pass_deadline",
                "attempts_used": len(intents),
            }
        attempt = len(intents)
        intent = {
            "schema_version": "milk.baseten-metric-attempt.v1",
            "run_id": run_id,
            "ordinal": plan["ordinal"],
            "attempt": attempt,
            "execution_id": plan["execution_id"],
            "start_epoch_millis": plan["start_epoch_millis"],
            "end_epoch_millis": plan["end_epoch_millis"],
            "step_seconds": STEP_SECONDS,
        }
        prefix = f"metric-collections/{plan['ordinal']:02d}/attempts/{attempt:02d}"
        stored_intent, created = _put(evidence, prefix + "-intent.json", intent)
        if stored_intent != intent:
            raise ValueError("concurrent Baseten metric attempt differs")
        intents[attempt] = intent
        if not created:
            continue
        raw = None
        try:
            raw = request(
                "GET",
                f"/training_projects/{project_id}/jobs/{plan['execution_id']}/metrics",
                query={
                    "start_epoch_millis": plan["start_epoch_millis"],
                    "end_epoch_millis": plan["end_epoch_millis"],
                    "step_seconds": STEP_SECONDS,
                },
            )
            execution, series = _normalize(raw, plan, project_id, current_millis)
            result = {
                "schema_version": "milk.baseten-metric-attempt-result.v1",
                "run_id": run_id,
                "ordinal": plan["ordinal"],
                "attempt": attempt,
                "state": "complete",
                "response_bytes": len(raw),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                "error_type": None,
                "error_sha256": None,
                "execution": execution,
                "series": series,
                "series_sha256": hashlib.sha256(canonical_json(series)).hexdigest(),
            }
            canonical_json(result)
        except Exception as error:
            bounded = raw if isinstance(raw, (bytes, bytearray)) and len(raw) <= MAX_PROVIDER_BYTES else None
            error_type = type(error).__name__
            if IDENTIFIER.fullmatch(error_type or "") is None:
                error_type = "Exception"
            result = {
                "schema_version": "milk.baseten-metric-attempt-result.v1",
                "run_id": run_id,
                "ordinal": plan["ordinal"],
                "attempt": attempt,
                "state": "failed",
                "response_bytes": None if bounded is None else len(bounded),
                "response_sha256": None if bounded is None else hashlib.sha256(bounded).hexdigest(),
                "error_type": error_type,
                "error_sha256": hashlib.sha256(
                    f"{type(error).__name__}:{error}".encode()
                ).hexdigest(),
                "execution": None,
                "series": None,
                "series_sha256": None,
            }
        stored_result, unused_created = _put(evidence, prefix + "-result.json", result)
        del unused_created
        if stored_result != result:
            raise ValueError("stored Baseten metric attempt result differs")
        results[attempt] = result


def _aggregate(evidence, plan, execution_summaries):
    entries = [
        {
            "ordinal": item["ordinal"],
            "execution_id": item["execution_id"],
            "execution_name": item["execution_name"],
            "start_epoch_millis": item["start_epoch_millis"],
            "end_epoch_millis": item["end_epoch_millis"],
            "not_before": item["not_before"],
            "step_seconds": item["step_seconds"],
            "duplicate_set_sha256": item["duplicate_set_sha256"],
            "duplicate_execution_id_sha256s": item[
                "duplicate_execution_id_sha256s"
            ],
            "summary_sha256": hashlib.sha256(canonical_json(item)).hexdigest(),
            "source_complete": item["source_complete"],
            "attempts_used": item["attempts_used"],
            "response_sha256": item["response_sha256"],
            "series_sha256": item["series_sha256"],
        }
        for item in execution_summaries
    ]
    value = {
        "schema_version": "milk.baseten-metric-collection-summary.v1",
        "campaign_id": plan["campaign_id"],
        "run_id": plan["run_id"],
        "project_id": plan["project_id"],
        "plan_sha256": hashlib.sha256(canonical_json(plan)).hexdigest(),
        "executions": entries,
        "attempts_used": sum(item["attempts_used"] for item in execution_summaries),
        "pessimistic_provider_bytes": sum(
            item["pessimistic_provider_bytes"] for item in execution_summaries
        ),
        "actual_response_bytes": sum(
            item["actual_response_bytes"] for item in execution_summaries
        ),
        "source_complete": all(item["source_complete"] for item in execution_summaries),
    }
    stored, unused_created = _put(evidence, "metric-collections/summary.json", value)
    del unused_created
    if stored != value:
        raise ValueError("stored Baseten metric collection summary differs")
    return stored


def collect_baseten_terminal_metrics(
    *,
    evidence,
    request,
    project_id,
    executions,
    observed_at,
    now=lambda: dt.datetime.now(dt.timezone.utc),
    continue_collection=lambda: True,
):
    """Collect immutable numeric Baseten metrics for a terminal launch group."""
    if not callable(request) or not callable(continue_collection):
        raise ValueError("metrics request callback or continuation predicate is invalid")
    campaign_id, run_id = _identity(evidence, project_id)
    plan = _plan(evidence, campaign_id, run_id, project_id, executions, observed_at)
    current_millis = _current_millis(now)
    summaries = []
    for execution in plan["executions"]:
        result = _collect_execution(
            evidence,
            request,
            project_id,
            run_id,
            execution,
            current_millis,
            continue_collection,
        )
        if result.get("schema_version") == "milk.baseten-metric-collection-pending.v1":
            return result
        summaries.append(result)
    summaries = [
        _load(evidence, f"metric-collections/{item['ordinal']:02d}/summary.json")
        for item in summaries
    ]
    return _aggregate(evidence, plan, summaries)
