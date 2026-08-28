from __future__ import annotations

import copy
import hashlib
import json
import tempfile
from pathlib import Path

import contract


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"


def bundle() -> dict:
    target = "teacher answer"
    target_sha256 = hashlib.sha256(target.encode()).hexdigest()
    entry = {
        "trace_id": "00000000-0000-7000-8000-000000000001",
        "request_sha256": "",
        "request": {
            "model": "baseline",
            "messages": [
                {"role": "system", "content": "answer briefly"},
                {"role": "developer", "content": "use the supplied context"},
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "first answer"},
                {"role": "user", "content": "follow-up question"},
            ],
        },
        "trace_payload_sha256": "2" * 64,
        "outcome_payload_sha256": "3" * 64,
        "outcome_submission_sha256": "4" * 64,
        "target_source": {
            "kind": "teacher_distillation",
            "distillation_run_receipt_sha256": "5" * 64,
            "teacher_response_sha256": target_sha256,
        },
        "target": target,
        "target_sha256": target_sha256,
    }
    entry["request_sha256"] = hashlib.sha256(canonical(entry["request"])[:-1]).hexdigest()
    entries = []
    for index in range(50):
        row = copy.deepcopy(entry)
        row["trace_id"] = f"00000000-0000-7000-8000-{index:012d}"
        row["outcome_submission_sha256"] = f"{index + 101:064x}"
        entries.append(row)
    return {
        "schema_version": "dragontales.adapter-train-curriculum-bundle.v1",
        "scope": {},
        "release_sha256": "6" * 64,
        "release_seed_sha256": "7" * 64,
        "analysis_id": "8" * 64,
        "analysis_proposal_sha256": "9" * 64,
        "analysis_provider_binding_sha256": "a" * 64,
        "entries": entries,
    }


def rejected(value: dict, expected: str) -> None:
    try:
        contract.eligible_rows(value)
    except ValueError as error:
        assert expected in str(error)
    else:
        raise AssertionError(f"invalid bundle was accepted: {expected}")


def main() -> None:
    source = Path(__file__).resolve().parent
    builder_sha256 = contract.package_sha256(source)
    value = bundle()
    raw = canonical(value)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        bundle_path = root / "bundle.json"
        bundle_path.write_bytes(raw)
        contract.prepare(
            bundle_path,
            hashlib.sha256(raw).hexdigest(),
            root / "run",
            "b" * 64,
            "c" * 64,
            builder_sha256,
        )
        rows = contract.read_prepared(root / "run/launcher/prepared/train.jsonl")
        assert len(rows) == 50
        assert rows[0]["messages"][-1] == {"role": "assistant", "content": "teacher answer"}
        assert rows[0]["target_kind"] == "teacher_distillation"
        config = (root / "run/launcher/dragontales-sft.toml").read_text()
        for expected in (
            "max_steps = 100",
            "num_train_gpus = 1",
            'name = "/model"',
            'name = "qwen3"',
            "enable_thinking = true",
            "rank = 32",
            "alpha = 64",
            'name = "/runs/run/launcher/prepared"',
            "batch_size = 8",
            "seq_len = 4096",
            "DRAGONTALES_MODEL_PROFILE_SHA256",
        ):
            assert expected in config
        assert "inference" not in config and "orchestrator" not in config and "verifiers" not in config

    mixed = bundle()
    mixed["entries"][0]["target_source"] = {
        "kind": "independent_correction",
        "verifier_id": "customer-correction-v1",
    }
    rejected(mixed, "mixes target source kinds")
    tool_source = bundle()
    tool_source["entries"][0]["request"]["messages"].append(
        {"role": "tool", "content": "tool output"}
    )
    tool_source["entries"][0]["request_sha256"] = hashlib.sha256(
        canonical(tool_source["entries"][0]["request"])[:-1]
    ).hexdigest()
    rejected(tool_source, "ordinary text Chat Completions thread")
    wrong_teacher = bundle()
    wrong_teacher["entries"][0]["target_source"]["teacher_response_sha256"] = "d" * 64
    rejected(wrong_teacher, "teacher response digest")
    wrong_request = bundle()
    wrong_request["entries"][0]["request"]["messages"][2]["content"] = "changed"
    rejected(wrong_request, "request digest")
    extra_request = bundle()
    extra_request["entries"][0]["request"]["temperature"] = 0.1
    extra_request["entries"][0]["request_sha256"] = hashlib.sha256(
        canonical(extra_request["entries"][0]["request"])[:-1]
    ).hexdigest()
    rejected(extra_request, "Chat Completions JSON")
    short = bundle()
    short["entries"] = short["entries"][:49]
    rejected(short, "50..=10000")


if __name__ == "__main__":
    main()
