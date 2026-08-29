#!/usr/bin/env python3
"""Populate and verify the admitted GPT-OSS teacher model in a Modal volume."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat

import modal


MODEL_REPOSITORY = "openai/gpt-oss-120b"
MODEL_REVISION = "b5c939de8f754692c1647ca79fbf85e8c1e70f8a"
MODEL_ROOT = Path("/models")
VOLUME_NAME = os.environ.get(
    "MILK_MODAL_TEACHER_VOLUME",
    "milk-prod-teacher-cache",
)

app = modal.App("milk-populate-teacher-volume")
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
        or len(value["files"]) != 24
    ):
        raise ValueError("teacher model manifest identity differs")
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
            raise ValueError("teacher model manifest file is invalid")
        names.append(path.name)
    if names != sorted(set(names)):
        raise ValueError("teacher model manifest names are not canonical")
    return files


@app.function(
    image=image,
    cpu=4.0,
    memory=8192,
    timeout=24 * 60 * 60,
    volumes={str(MODEL_ROOT): volume},
)
def populate(manifest_raw: bytes) -> dict:
    from huggingface_hub import snapshot_download

    manifest = json.loads(manifest_raw)
    files = _validate_manifest(manifest)
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
        raise RuntimeError("teacher model volume differs from the exact inventory")
    for path, item in zip(entries, files):
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size != item["bytes"]
            or _sha256(path) != item["sha256"]
        ):
            raise RuntimeError(f"teacher model file differs: {path.name}")
    volume.commit()
    return {
        "schema_version": "milk.modal-teacher-volume-population.v1",
        "repository": MODEL_REPOSITORY,
        "revision": MODEL_REVISION,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "file_count": len(files),
        "bytes": sum(item["bytes"] for item in files),
        "volume_name": VOLUME_NAME,
        "verified": True,
    }


@app.local_entrypoint()
def main(
    manifest: str = "deploy/teacher/gpt-oss-120b/model.manifest.json",
) -> None:
    path = Path(manifest).resolve()
    raw = path.read_bytes()
    value = json.loads(raw)
    _validate_manifest(value)
    print(json.dumps(populate.remote(raw), sort_keys=True, separators=(",", ":")))
