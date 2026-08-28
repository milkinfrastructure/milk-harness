from __future__ import annotations

import datetime as dt
import hashlib
import json
import re

from milk_harness.evidence import HEX64, canonical_json
from milk_harness.logs import LogArchive, REDACTION_PROFILE_SHA256


MAX_PROVIDER_BYTES = 1024 * 1024
MAX_RECORDS = 1000
MAX_WINDOW_ATTEMPTS = 3
MAX_RUN_ATTEMPTS = 100
TAIL_MILLISECONDS = 60_000
MAX_EPOCH_MILLISECONDS = 9_999_999_999_999
TERMINAL_STATUSES = {
    "TRAINING_JOB_COMPLETED",
    "TRAINING_JOB_DEPLOY_FAILED",
    "TRAINING_JOB_FAILED",
    "TRAINING_JOB_PREEMPTED",
    "TRAINING_JOB_STOPPED",
}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
ATTEMPT_FILE = re.compile(r"([0-9]{3})-(intent|result)\.json\Z")
DECISION_KEYS = {
    "schema_version", "run_id", "ordinal", "start_epoch_millis",
    "end_epoch_millis", "state", "response_bytes", "response_sha256",
    "record_count", "children", "gap", "archive", "redaction_profile_sha256",
}
ARCHIVE_KEYS = {
    "schema_version", "run_id", "source", "stream_id", "redaction_profile_sha256",
    "first_at", "last_at", "last_provider_cursor", "original_bytes", "stored_records",
    "redacted_records", "truncated", "source_complete", "missing_reason", "chunks",
}


def _strict_json(raw):
    if not isinstance(raw, (bytes, bytearray)) or len(raw) > MAX_PROVIDER_BYTES:
        raise ValueError("bounded JSON bytes are required")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("JSON contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
    if not isinstance(value, dict):
        raise ValueError("JSON must be an object")
    return value


def _milliseconds(value, label):
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 64:
        raise ValueError(f"{label} must be UTC RFC3339")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{label} must be UTC RFC3339") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{label} must be UTC RFC3339")
    delta = parsed - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    millis = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    if not 0 <= millis <= MAX_EPOCH_MILLISECONDS:
        raise ValueError(f"{label} is outside the supported interval")
    return millis, parsed


def _current_millis(now):
    if not callable(now):
        raise ValueError("collection clock must be callable")
    current = now()
    if (
        not isinstance(current, dt.datetime)
        or current.tzinfo is None
        or current.utcoffset() != dt.timedelta(0)
    ):
        raise ValueError("collection clock must return UTC")
    return _milliseconds(
        current.astimezone(dt.timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "collection clock",
    )[0]


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
        or not callable(getattr(evidence, "bytes", None))
        or not isinstance(project_id, str)
        or IDENTIFIER.fullmatch(project_id) is None
    ):
        raise ValueError("job evidence or Baseten project identity is invalid")
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
    if evidence.store.create(key, canonical_json(value), "application/json"):
        return value, True
    return _strict_json(evidence.store.get(key)), False


def _plan(evidence, campaign_id, run_id, project_id, executions, observed_at):
    observed_millis, unused = _milliseconds(observed_at, "terminal observed_at")
    del unused
    if not isinstance(executions, list) or not 1 <= len(executions) <= 3:
        raise ValueError("one to three terminal executions are required")
    roots, ordinals, execution_ids = [], set(), set()
    for execution in executions:
        if not isinstance(execution, dict):
            raise ValueError("terminal execution is invalid")
        ordinal = execution.get("ordinal")
        execution_id = execution.get("execution_id")
        if (
            type(ordinal) is not int
            or not 0 <= ordinal <= 99
            or not isinstance(execution_id, str)
            or IDENTIFIER.fullmatch(execution_id) is None
            or ordinal in ordinals
            or execution_id in execution_ids
            or execution.get("status") not in TERMINAL_STATUSES
        ):
            raise ValueError("terminal execution identity is invalid")
        start, unused = _milliseconds(execution.get("created_at"), "provider created_at")
        del unused
        updated, unused = _milliseconds(
            execution.get("updated_at"), "provider terminal updated_at"
        )
        del unused
        end = max(updated, observed_millis) + TAIL_MILLISECONDS
        if updated < start or end > MAX_EPOCH_MILLISECONDS:
            raise ValueError("terminal execution interval is invalid")
        roots.append(
            {
                "ordinal": ordinal,
                "execution_id": execution_id,
                "start_epoch_millis": start,
                "end_epoch_millis": end,
                "not_before": end,
            }
        )
        ordinals.add(ordinal)
        execution_ids.add(execution_id)
    proposal = {
        "schema_version": "milk.baseten-log-collection-plan.v1",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "project_id": project_id,
        "terminal_observed_at": observed_at,
        "executions": sorted(roots, key=lambda item: item["ordinal"]),
    }
    stored = _maybe(evidence, "log-collections/plan.json")
    if stored is None:
        stored, unused_created = _put(evidence, "log-collections/plan.json", proposal)
        del unused_created
    if stored != proposal:
        raise ValueError("terminal observations differ from the immutable log plan")
    return stored


def _window_path(ordinal, start, end):
    return f"log-collections/{ordinal:02d}/windows/{start:013d}-{end:013d}.json"


def _stream_id(run_id, ordinal, start, end, response_sha256):
    return hashlib.sha256(
        canonical_json(
            {
                "run_id": run_id,
                "ordinal": ordinal,
                "start_epoch_millis": start,
                "end_epoch_millis": end,
                "response_sha256": response_sha256,
            }
        )
    ).hexdigest()


def _response_fields(value):
    return (
        type(value.get("response_bytes")) is int
        and 0 <= value["response_bytes"] <= MAX_PROVIDER_BYTES
        and HEX64.fullmatch(value.get("response_sha256") or "") is not None
        and type(value.get("record_count")) is int
        and 0 <= value["record_count"] <= MAX_RECORDS
    )


def _check_archive(archive, run_id, stream_id, records, complete):
    if (
        not isinstance(archive, dict)
        or set(archive) != ARCHIVE_KEYS
        or archive.get("schema_version") != "milk.redacted-log-index.v1"
        or archive.get("run_id") != run_id
        or archive.get("source") != "baseten"
        or archive.get("stream_id") != stream_id
        or archive.get("redaction_profile_sha256") != REDACTION_PROFILE_SHA256
        or archive.get("stored_records") != records
        or archive.get("redacted_records") != records
        or archive.get("source_complete") is not complete
        or archive.get("missing_reason")
        != (None if complete else "provider_limit_same_millisecond")
        or not isinstance(archive.get("chunks"), list)
    ):
        raise ValueError("stored redacted log archive is invalid")


def _check_decision(value, run_id, window):
    ordinal, start, end = window
    if (
        type(ordinal) is not int
        or type(start) is not int
        or type(end) is not int
        or not 0 <= start <= end <= MAX_EPOCH_MILLISECONDS
        or not isinstance(value, dict)
        or set(value) != DECISION_KEYS
        or value.get("schema_version") != "milk.baseten-log-window.v1"
        or value.get("run_id") != run_id
        or value.get("ordinal") != ordinal
        or value.get("start_epoch_millis") != start
        or value.get("end_epoch_millis") != end
        or value.get("state") not in {"complete", "split", "gap"}
        or value.get("redaction_profile_sha256") != REDACTION_PROFILE_SHA256
    ):
        raise ValueError("stored log window decision is invalid")
    state = value["state"]
    if state == "split":
        middle = (start + end) // 2
        if (
            start >= end
            or not _response_fields(value)
            or value["record_count"] != MAX_RECORDS
            or value["children"]
            != [
                {"start_epoch_millis": start, "end_epoch_millis": middle},
                {"start_epoch_millis": middle + 1, "end_epoch_millis": end},
            ]
            or value["gap"] is not None
            or value["archive"] is not None
        ):
            raise ValueError("stored split log window is invalid")
        return value
    if state == "complete":
        if (
            not _response_fields(value)
            or value["record_count"] >= MAX_RECORDS
            or value["children"] is not None
            or value["gap"] is not None
        ):
            raise ValueError("stored complete log window is invalid")
        complete = True
    else:
        gap = value["gap"]
        same_millisecond = start == end and gap == {
            "reason": "provider_limit_same_millisecond",
            "at_epoch_millis": start,
            "observed_records": MAX_RECORDS,
            "missing_records": None,
        }
        bounded = gap in (
            {"reason": "request_attempt_bound", "observed_records": None, "missing_records": None},
            {"reason": "run_attempt_bound", "observed_records": None, "missing_records": None},
        )
        if value["children"] is not None or not (same_millisecond or bounded):
            raise ValueError("stored log gap is invalid")
        if bounded:
            if any(
                value[name] is not None
                for name in ("response_bytes", "response_sha256", "record_count", "archive")
            ):
                raise ValueError("stored bounded log gap is invalid")
            return value
        if not _response_fields(value) or value["record_count"] != MAX_RECORDS:
            raise ValueError("stored same-millisecond log gap is invalid")
        complete = False
    stream_id = _stream_id(run_id, ordinal, start, end, value["response_sha256"])
    _check_archive(value["archive"], run_id, stream_id, value["record_count"], complete)
    return value


def _seal(evidence, decision):
    window = (
        decision["ordinal"], decision["start_epoch_millis"], decision["end_epoch_millis"]
    )
    stored, unused_created = _put(evidence, _window_path(*window), decision)
    del unused_created
    return _check_decision(stored, decision["run_id"], window)


def _records(raw, start, end):
    value = _strict_json(raw)
    logs = value.get("logs")
    if set(value) != {"logs"} or not isinstance(logs, list) or len(logs) > MAX_RECORDS:
        raise ValueError("Baseten logs response is invalid")
    records, previous = [], None
    for item in logs:
        if (
            not isinstance(item, dict)
            or set(item) != {"timestamp", "message", "replica", "request_id", "level"}
            or any(not isinstance(item.get(name), str) for name in item)
        ):
            raise ValueError("Baseten log record is invalid")
        millis, parsed = _milliseconds(item["timestamp"], "Baseten log timestamp")
        if not start <= millis <= end or (previous is not None and parsed < previous):
            raise ValueError("Baseten log timestamps are outside or unordered")
        previous = parsed
        cursor = hashlib.sha256(
            canonical_json(
                {name: item[name] for name in ("timestamp", "replica", "request_id", "level")}
            )
        ).hexdigest()
        records.append((item["timestamp"], item["message"], cursor))
    return records


def _archive(evidence, run_id, window, response_sha256, records, complete):
    stream_id = _stream_id(run_id, *window, response_sha256)
    archive = LogArchive(evidence, "baseten", stream_id=stream_id)
    for at, message, cursor in records:
        if not archive.write(at=at, line=message, provider_cursor=cursor):
            raise ValueError("bounded response exceeded the log archive limit")
    value = archive.close(
        source_complete=complete,
        missing_reason=None if complete else "provider_limit_same_millisecond",
    )
    _check_archive(value, run_id, stream_id, len(records), complete)
    return value


def _success(evidence, run_id, window, raw, records):
    ordinal, start, end = window
    response_sha256 = hashlib.sha256(raw).hexdigest()
    common = {
        "schema_version": "milk.baseten-log-window.v1",
        "run_id": run_id,
        "ordinal": ordinal,
        "start_epoch_millis": start,
        "end_epoch_millis": end,
        "response_bytes": len(raw),
        "response_sha256": response_sha256,
        "record_count": len(records),
        "redaction_profile_sha256": REDACTION_PROFILE_SHA256,
    }
    if len(records) == MAX_RECORDS and start < end:
        middle = (start + end) // 2
        return {
            **common,
            "state": "split",
            "children": [
                {"start_epoch_millis": start, "end_epoch_millis": middle},
                {"start_epoch_millis": middle + 1, "end_epoch_millis": end},
            ],
            "gap": None,
            "archive": None,
        }
    complete = len(records) < MAX_RECORDS
    return {
        **common,
        "state": "complete" if complete else "gap",
        "children": None,
        "gap": None
        if complete
        else {
            "reason": "provider_limit_same_millisecond",
            "at_epoch_millis": start,
            "observed_records": MAX_RECORDS,
            "missing_records": None,
        },
        "archive": _archive(evidence, run_id, window, response_sha256, records, complete),
    }


def _gap(run_id, window, reason):
    ordinal, start, end = window
    return {
        "schema_version": "milk.baseten-log-window.v1",
        "run_id": run_id,
        "ordinal": ordinal,
        "start_epoch_millis": start,
        "end_epoch_millis": end,
        "state": "gap",
        "response_bytes": None,
        "response_sha256": None,
        "record_count": None,
        "children": None,
        "gap": {"reason": reason, "observed_records": None, "missing_records": None},
        "archive": None,
        "redaction_profile_sha256": REDACTION_PROFILE_SHA256,
    }


def _attempt_evidence(evidence, run_id):
    keys = evidence.store.list(f"{evidence.prefix}/log-collections/attempts", limit=201)
    if len(keys) > 200:
        raise ValueError("Baseten log attempt evidence exceeds its bound")
    intents, results = {}, {}
    for key in keys:
        match = ATTEMPT_FILE.fullmatch(key.rsplit("/", 1)[-1])
        if match is None or int(match.group(1)) >= MAX_RUN_ATTEMPTS:
            raise ValueError("Baseten log attempt evidence path is invalid")
        slot = int(match.group(1))
        target = intents if match.group(2) == "intent" else results
        if slot in target:
            raise ValueError("duplicate Baseten log attempt evidence")
        target[slot] = _strict_json(evidence.store.get(key))
    if sorted(intents) != list(range(len(intents))) or not set(results).issubset(intents):
        raise ValueError("Baseten log attempt slots are invalid")
    per_window = {}
    for slot, intent in sorted(intents.items()):
        if (
            set(intent)
            != {
                "schema_version", "run_id", "slot", "ordinal", "start_epoch_millis",
                "end_epoch_millis", "window_attempt",
            }
            or intent.get("schema_version") != "milk.baseten-log-attempt.v1"
            or intent.get("run_id") != run_id
            or intent.get("slot") != slot
            or any(
                type(intent.get(name)) is not int
                for name in ("ordinal", "start_epoch_millis", "end_epoch_millis", "window_attempt")
            )
            or not 0 <= intent["ordinal"] <= 99
            or not 0 <= intent["start_epoch_millis"] <= intent["end_epoch_millis"] <= MAX_EPOCH_MILLISECONDS
            or not 0 <= intent["window_attempt"] < MAX_WINDOW_ATTEMPTS
        ):
            raise ValueError("stored Baseten log attempt is invalid")
        window = (intent["ordinal"], intent["start_epoch_millis"], intent["end_epoch_millis"])
        per_window.setdefault(window, []).append(slot)
    for slots in per_window.values():
        if [intents[slot]["window_attempt"] for slot in slots] != list(range(len(slots))):
            raise ValueError("Baseten log window attempts are not contiguous")
    for slot, result in results.items():
        intent = intents[slot]
        window = (intent["ordinal"], intent["start_epoch_millis"], intent["end_epoch_millis"])
        if (
            set(result)
            != {
                "schema_version", "run_id", "slot", "state", "response_bytes",
                "response_sha256", "failure_type", "failure_sha256", "decision",
            }
            or result.get("schema_version") != "milk.baseten-log-attempt-result.v1"
            or result.get("run_id") != run_id
            or result.get("slot") != slot
            or result.get("state") not in {"success", "failed"}
        ):
            raise ValueError("stored Baseten log attempt result is invalid")
        response_bytes = result["response_bytes"]
        response_valid = (
            type(response_bytes) is int
            and 0 <= response_bytes <= MAX_PROVIDER_BYTES
            and HEX64.fullmatch(result.get("response_sha256") or "") is not None
        )
        if result["state"] == "success":
            decision = _check_decision(result["decision"], run_id, window)
            if (
                not response_valid
                or result["failure_type"] is not None
                or result["failure_sha256"] is not None
                or decision["response_bytes"] != response_bytes
                or decision["response_sha256"] != result["response_sha256"]
            ):
                raise ValueError("stored successful Baseten log attempt is invalid")
        elif (
            (response_bytes is not None and not response_valid)
            or (response_bytes is None and result["response_sha256"] is not None)
            or not isinstance(result["failure_type"], str)
            or not result["failure_type"]
            or len(result["failure_type"]) > 128
            or HEX64.fullmatch(result.get("failure_sha256") or "") is None
            or result["decision"] is not None
        ):
            raise ValueError("stored failed Baseten log attempt is invalid")
    return intents, results, per_window


def _frontier(evidence, plan, run_id, cache):
    queue = sorted(
        (
            root["ordinal"], root["start_epoch_millis"],
            root["end_epoch_millis"], root["not_before"],
        )
        for root in plan["executions"]
    )
    leaves, unresolved = [], []
    while queue:
        ordinal, start, end, not_before = queue.pop(0)
        window = (ordinal, start, end)
        if window not in cache:
            stored = _maybe(evidence, _window_path(*window))
            cache[window] = None if stored is None else _check_decision(stored, run_id, window)
        decision = cache[window]
        if decision is None:
            unresolved.append((ordinal, start, end, not_before))
        elif decision["state"] == "split":
            queue.extend(
                (ordinal, child["start_epoch_millis"], child["end_epoch_millis"], not_before)
                for child in decision["children"]
            )
            queue.sort()
        else:
            leaves.append(decision)
    key = lambda item: (item["ordinal"], item["start_epoch_millis"], item["end_epoch_millis"])
    return sorted(leaves, key=key), sorted(unresolved)


def _execution_summaries(plan, leaves):
    values = []
    for root in plan["executions"]:
        parts = [item for item in leaves if item["ordinal"] == root["ordinal"]]
        cursor = root["start_epoch_millis"]
        for part in parts:
            if part["start_epoch_millis"] != cursor:
                raise ValueError("terminal log leaves do not exactly cover their plan")
            cursor = part["end_epoch_millis"] + 1
        if not parts or cursor != root["end_epoch_millis"] + 1:
            raise ValueError("terminal log leaves do not exactly cover their plan")
        values.append(
            {**root, "source_complete": all(part["state"] == "complete" for part in parts)}
        )
    return values


def _check_summary(value, plan, run_id):
    keys = {
        "schema_version", "campaign_id", "run_id", "project_id", "plan_sha256",
        "attempts_used", "attempts_without_known_response", "pessimistic_provider_bytes",
        "actual_response_bytes", "actual_response_bytes_complete", "records_hashed",
        "executions", "leaf_decisions", "source_complete",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema_version") != "milk.baseten-log-collection-summary.v1"
        or value.get("campaign_id") != plan["campaign_id"]
        or value.get("run_id") != run_id
        or value.get("project_id") != plan["project_id"]
        or value.get("plan_sha256") != hashlib.sha256(canonical_json(plan)).hexdigest()
        or type(value.get("attempts_used")) is not int
        or not 0 <= value["attempts_used"] <= MAX_RUN_ATTEMPTS
        or type(value.get("attempts_without_known_response")) is not int
        or not 0 <= value["attempts_without_known_response"] <= value["attempts_used"]
        or value.get("pessimistic_provider_bytes") != value["attempts_used"] * MAX_PROVIDER_BYTES
        or type(value.get("actual_response_bytes")) is not int
        or not 0 <= value["actual_response_bytes"] <= value["pessimistic_provider_bytes"]
        or type(value.get("actual_response_bytes_complete")) is not bool
        or value["actual_response_bytes_complete"]
        != (value["attempts_without_known_response"] == 0)
        or not isinstance(value.get("leaf_decisions"), list)
        or type(value.get("source_complete")) is not bool
    ):
        raise ValueError("stored Baseten log summary is invalid")
    leaves = [
        _check_decision(
            item,
            run_id,
            (item.get("ordinal"), item.get("start_epoch_millis"), item.get("end_epoch_millis")),
        )
        for item in value["leaf_decisions"]
        if isinstance(item, dict)
    ]
    key = lambda item: (item["ordinal"], item["start_epoch_millis"], item["end_epoch_millis"])
    records_hashed = sum(
        item["archive"]["stored_records"] for item in leaves if item["archive"] is not None
    )
    if (
        len(leaves) != len(value["leaf_decisions"])
        or leaves != sorted(leaves, key=key)
        or value.get("executions") != _execution_summaries(plan, leaves)
        or value.get("records_hashed") != records_hashed
        or value["source_complete"] != all(item["state"] == "complete" for item in leaves)
    ):
        raise ValueError("stored Baseten log summary is inconsistent")
    return value


def _summary(evidence, plan, run_id, attempts, results, leaves):
    unknown = sum(
        slot not in results or results[slot]["response_bytes"] is None for slot in attempts
    )
    value = {
        "schema_version": "milk.baseten-log-collection-summary.v1",
        "campaign_id": plan["campaign_id"],
        "run_id": run_id,
        "project_id": plan["project_id"],
        "plan_sha256": hashlib.sha256(canonical_json(plan)).hexdigest(),
        "attempts_used": len(attempts),
        "attempts_without_known_response": unknown,
        "pessimistic_provider_bytes": len(attempts) * MAX_PROVIDER_BYTES,
        "actual_response_bytes": sum(
            result["response_bytes"]
            for result in results.values()
            if result["response_bytes"] is not None
        ),
        "actual_response_bytes_complete": unknown == 0,
        "records_hashed": sum(
            item["archive"]["stored_records"] for item in leaves if item["archive"] is not None
        ),
        "executions": _execution_summaries(plan, leaves),
        "leaf_decisions": leaves,
        "source_complete": all(item["state"] == "complete" for item in leaves),
    }
    stored, unused_created = _put(evidence, "log-collections/summary.json", value)
    del unused_created
    return _check_summary(stored, plan, run_id)


def collect_baseten_terminal_logs(
    *,
    evidence,
    request,
    project_id,
    executions,
    observed_at,
    now=lambda: dt.datetime.now(dt.timezone.utc),
    continue_collection=lambda: True,
):
    """Collect one immutable, bounded Baseten terminal-log archive."""
    if not callable(request) or not callable(continue_collection):
        raise ValueError("Baseten request callback or continuation predicate is invalid")
    campaign_id, run_id = _identity(evidence, project_id)
    plan = _plan(evidence, campaign_id, run_id, project_id, executions, observed_at)
    existing = _maybe(evidence, "log-collections/summary.json")
    if existing is not None:
        return _check_summary(existing, plan, run_id)

    current_millis = _current_millis(now)
    attempts, results, per_window = _attempt_evidence(evidence, run_id)
    decisions = {}
    while True:
        leaves, unresolved = _frontier(evidence, plan, run_id, decisions)
        if not unresolved:
            attempts, results, per_window = _attempt_evidence(evidence, run_id)
            del per_window
            return _summary(evidence, plan, run_id, attempts, results, leaves)
        ordinal, start, end, not_before = unresolved[0]
        if current_millis < not_before:
            return {
                "schema_version": "milk.baseten-log-collection-pending.v1",
                "run_id": run_id,
                "state": "not_before",
                "next_not_before": not_before,
                "attempts_used": len(attempts),
            }
        window = (ordinal, start, end)
        window_slots = per_window.get(window, [])
        successful = next(
            (results[slot] for slot in window_slots if results.get(slot, {}).get("state") == "success"),
            None,
        )
        if successful is not None:
            decisions[window] = _seal(
                evidence, _check_decision(successful["decision"], run_id, window)
            )
            continue
        if len(window_slots) >= MAX_WINDOW_ATTEMPTS:
            decisions[window] = _seal(
                evidence, _gap(run_id, window, "request_attempt_bound")
            )
            continue
        if len(attempts) >= MAX_RUN_ATTEMPTS:
            decisions[window] = _seal(evidence, _gap(run_id, window, "run_attempt_bound"))
            continue
        if not continue_collection():
            return {
                "schema_version": "milk.baseten-log-collection-pending.v1",
                "run_id": run_id,
                "state": "pass_deadline",
                "attempts_used": len(attempts),
            }

        slot = len(attempts)
        intent = {
            "schema_version": "milk.baseten-log-attempt.v1",
            "run_id": run_id,
            "slot": slot,
            "ordinal": ordinal,
            "start_epoch_millis": start,
            "end_epoch_millis": end,
            "window_attempt": len(window_slots),
        }
        stored_intent, created = _put(
            evidence, f"log-collections/attempts/{slot:03d}-intent.json", intent
        )
        if stored_intent != intent:
            raise ValueError("concurrent Baseten log attempt differs")
        attempts[slot] = intent
        per_window.setdefault(window, []).append(slot)
        if not created:
            continue

        raw = None
        try:
            execution_id = next(
                root["execution_id"]
                for root in plan["executions"]
                if root["ordinal"] == ordinal
            )
            raw = request(
                "GET",
                f"/training_projects/{project_id}/jobs/{execution_id}/logs",
                query={
                    "start_epoch_millis": start,
                    "end_epoch_millis": end,
                    "direction": "asc",
                    "limit": MAX_RECORDS,
                },
            )
            records = _records(raw, start, end)
        except Exception as error:
            bounded = (
                raw
                if isinstance(raw, (bytes, bytearray)) and len(raw) <= MAX_PROVIDER_BYTES
                else None
            )
            failure_type = type(error).__name__
            if not failure_type or len(failure_type) > 128:
                failure_type = "Exception"
            result = {
                "schema_version": "milk.baseten-log-attempt-result.v1",
                "run_id": run_id,
                "slot": slot,
                "state": "failed",
                "response_bytes": None if bounded is None else len(bounded),
                "response_sha256": None
                if bounded is None
                else hashlib.sha256(bounded).hexdigest(),
                "failure_type": failure_type,
                "failure_sha256": hashlib.sha256(
                    f"{type(error).__name__}:{error}".encode()
                ).hexdigest(),
                "decision": None,
            }
        else:
            decision = _success(evidence, run_id, window, raw, records)
            result = {
                "schema_version": "milk.baseten-log-attempt-result.v1",
                "run_id": run_id,
                "slot": slot,
                "state": "success",
                "response_bytes": len(raw),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                "failure_type": None,
                "failure_sha256": None,
                "decision": decision,
            }
        stored_result, unused_created = _put(
            evidence, f"log-collections/attempts/{slot:03d}-result.json", result
        )
        del unused_created
        if stored_result != result:
            raise ValueError("Baseten log attempt result differs")
        results[slot] = result
        if result["state"] == "success":
            decisions[window] = _seal(evidence, decision)
