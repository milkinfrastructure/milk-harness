#!/usr/bin/env python3
"""Populate and verify the admitted Qwen student base in a Modal volume."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import runpy
import shutil
import stat

import modal


MODEL_REPOSITORY = "Qwen/Qwen3-4B-Instruct-2507"
MODEL_REVISION = "cdbee75f17c01a7cc42f958dc650907174af0554"
MODEL_MANIFEST_SHA256 = "910bc2b47b9d0b77208c01a968f824ca3f0f8785f5ddf99dd62c15a320306e8b"
MODEL_FILE_COUNT = 13
MODEL_BYTES = 8_060_917_568
MODEL_ROOT = Path("/model")
VOLUME_NAME = os.environ.get(
    "MILK_MODAL_STUDENT_TRAIN_VOLUME",
    "milk-prod-student-train",
)

app = modal.App("milk-populate-student-volume")
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "huggingface_hub==1.28.0"
)
volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_manifest(value: object) -> list[dict]:
    if (
        not isinstance(value, dict)
        or value.get("repository") != MODEL_REPOSITORY
        or value.get("revision") != MODEL_REVISION
        or not isinstance(value.get("files"), list)
        or len(value["files"]) != MODEL_FILE_COUNT
    ):
        raise ValueError("student model manifest identity differs")
    files = value["files"]
    names = []
    for item in files:
        raw_path = item.get("path") if isinstance(item, dict) else None
        path = PurePosixPath(raw_path) if isinstance(raw_path, str) else None
        if (
            path is None
            or path.is_absolute()
            or len(path.parts) != 1
            or path.name in {"", ".", ".."}
            or type(item.get("bytes")) is not int
            or item["bytes"] <= 0
            or not isinstance(item.get("sha256"), str)
            or len(item["sha256"]) != 64
            or any(character not in "0123456789abcdef" for character in item["sha256"])
        ):
            raise ValueError("student model manifest file is invalid")
        names.append(path.name)
    inventory = b"".join(
        f"{item['sha256']}  {item['path']}\n".encode("ascii") for item in files
    )
    if (
        names != sorted(set(names))
        or sum(item["bytes"] for item in files) != MODEL_BYTES
        or hashlib.sha256(inventory).hexdigest() != MODEL_MANIFEST_SHA256
    ):
        raise ValueError("student model manifest differs from the admitted inventory")
    return files


def _manifest_from_verifier(path: Path) -> bytes:
    verifier = runpy.run_path(str(path))
    receipt = verifier["verify"]()
    if (
        verifier["EXPECTED_PROFILE"]["model_repository"] != MODEL_REPOSITORY
        or verifier["EXPECTED_PROFILE"]["model_revision"] != MODEL_REVISION
        or receipt["files"] != MODEL_FILE_COUNT
        or receipt["model_artifact_bytes"] != MODEL_BYTES
        or receipt["model_manifest_sha256"] != MODEL_MANIFEST_SHA256
    ):
        raise ValueError("student verifier identity differs")
    value = {
        "repository": MODEL_REPOSITORY,
        "revision": MODEL_REVISION,
        "files": [
            {"path": name, "sha256": digest, "bytes": size}
            for name, digest, size, _source in verifier["FILES"]
        ],
    }
    _validate_manifest(value)
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


@app.function(
    image=image,
    cpu=4.0,
    memory=8192,
    timeout=24 * 60 * 60,
    volumes={str(MODEL_ROOT): volume},
)
def populate(manifest_raw: bytes) -> dict:
    from huggingface_hub import snapshot_download

    files = _validate_manifest(json.loads(manifest_raw))
    names = [item["path"] for item in files]
    snapshot_download(
        repo_id=MODEL_REPOSITORY,
        revision=MODEL_REVISION,
        allow_patterns=names,
        local_dir=MODEL_ROOT,
        max_workers=8,
    )
    cache = MODEL_ROOT / ".cache"
    if cache.exists():
        shutil.rmtree(cache)
    entries = sorted(MODEL_ROOT.iterdir(), key=lambda path: path.name)
    if [path.name for path in entries] != names:
        raise RuntimeError("student model volume differs from the exact inventory")
    for path, item in zip(entries, files):
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size != item["bytes"]
            or _sha256(path) != item["sha256"]
        ):
            raise RuntimeError(f"student model file differs: {path.name}")
    volume.commit()
    return {
        "schema_version": "milk.modal-student-volume-population.v1",
        "repository": MODEL_REPOSITORY,
        "revision": MODEL_REVISION,
        "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "file_count": len(files),
        "bytes": sum(item["bytes"] for item in files),
        "volume_name": VOLUME_NAME,
        "verified": True,
    }


@app.local_entrypoint()
def main(
    verifier: str = "deploy/models/qwen3-4b-instruct-2507/verify.py",
) -> None:
    raw = _manifest_from_verifier(Path(verifier).resolve())
    print(json.dumps(populate.remote(raw), sort_keys=True, separators=(",", ":")))
