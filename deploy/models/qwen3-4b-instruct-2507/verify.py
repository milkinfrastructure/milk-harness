#!/usr/bin/python3
"""Offline release check for the fixed Dragontales Qwen3 student profile."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parent
MODEL_OWNER_UID = 0
PROFILE_KEYS = (
    "schema_version",
    "artifact_role",
    "model_repository",
    "model_revision",
    "model_license",
    "model_license_sha256",
    "model_manifest_sha256",
    "model_artifact_bytes",
    "model_modality",
    "context_window_tokens",
    "gpu_count",
    "gpu_model",
    "minimum_gpu_memory_gib",
    "model_alias",
)
EXPECTED_PROFILE = {
    "schema_version": "dragontales.student-model-profile.v1",
    "artifact_role": "student_adapter_base",
    "model_repository": "Qwen/Qwen3-4B-Instruct-2507",
    "model_revision": "cdbee75f17c01a7cc42f958dc650907174af0554",
    "model_license": "Apache-2.0",
    "model_license_sha256": "832dd9e00a68dd83b3c3fb9f5588dad7dcf337a0db50f7d9483f310cd292e92e",
    "model_manifest_sha256": "910bc2b47b9d0b77208c01a968f824ca3f0f8785f5ddf99dd62c15a320306e8b",
    "model_artifact_bytes": 8_060_917_568,
    "model_modality": "text",
    "context_window_tokens": 4_096,
    "gpu_count": 1,
    "gpu_model": "NVIDIA H100",
    "minimum_gpu_memory_gib": 80,
    "model_alias": "dragontales-max-v1",
}
FILES = (
    (".gitattributes", "34448b82c17d60fec9b65b1f093c115ddbaadc04beb1b0140b6bfed2e012a930", 1_570, "bytes"),
    ("LICENSE", "832dd9e00a68dd83b3c3fb9f5588dad7dcf337a0db50f7d9483f310cd292e92e", 11_343, "bytes"),
    ("README.md", "8e3dd0c3b5b11897cc71092ccfe517bb7a9783479baa3665aad73c8d1a2041cd", 8_168, "bytes"),
    ("config.json", "5beea1a4a34c62782bfb2f911c606741a3bab8f92d80a118fa053c28af12e8ba", 727, "bytes"),
    ("generation_config.json", "835fffe355c9438e7a25be099b3fccaa98350b83451f9fd2d99512e74f1ade48", 238, "bytes"),
    ("merges.txt", "599bab54075088774b1733fde865d5bd747cbcc7a547c5bc12610e874e26f5e3", 1_671_839, "bytes"),
    ("model-00001-of-00003.safetensors", "75311d91bb08cf0b882913da464a1e722a31fb44db35208663487efb7a3d8ed6", 3_957_900_840, "lfs"),
    ("model-00002-of-00003.safetensors", "0b48adbb1f60e901153d91907ba11ce63bd4b8b584482e730f48808d055dfba1", 3_987_450_520, "lfs"),
    ("model-00003-of-00003.safetensors", "7dd39ccca5e4de123c74c14af44c9bf2eb75df33b4614382af0134528e060d5d", 99_630_640, "lfs"),
    ("model.safetensors.index.json", "d6c42883a895dfef5b0080ed2116a1bcd764f558406b98923d675978a1abf29c", 32_819, "bytes"),
    ("tokenizer.json", "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4", 11_422_654, "lfs"),
    ("tokenizer_config.json", "a62ff0a2472a0fa1b8eaabcb57c59b58afa42a22831dc141400b6e0cf2b65ce3", 9_377, "bytes"),
    ("vocab.json", "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910", 2_776_833, "bytes"),
)


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _canonical(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def verify() -> dict[str, int | str]:
    profile_raw = (ROOT / "profile.json").read_bytes()
    profile = json.loads(profile_raw, object_pairs_hook=_object)
    if tuple(profile) != PROFILE_KEYS or profile != EXPECTED_PROFILE or profile_raw != _canonical(profile):
        raise ValueError("profile.json is not the exact canonical student profile")

    manifest = b"".join(
        f"{digest}  {name}\n".encode("ascii") for name, digest, _size, _source in FILES
    )
    if (ROOT / "model.manifest.sha256").read_bytes() != manifest:
        raise ValueError("model.manifest.sha256 does not match the exact sorted inventory")
    manifest_sha256 = hashlib.sha256(manifest).hexdigest()
    if manifest_sha256 != profile["model_manifest_sha256"]:
        raise ValueError("model manifest digest does not match profile.json")
    if len(FILES) != 13 or sum(size for _name, _digest, size, _source in FILES) != profile["model_artifact_bytes"]:
        raise ValueError("model inventory count or byte total is invalid")
    if FILES[1][1] != profile["model_license_sha256"]:
        raise ValueError("LICENSE digest does not match profile.json")
    if sum(source == "lfs" for _name, _digest, _size, source in FILES) != 4:
        raise ValueError("model inventory verification provenance is invalid")

    return {
        "schema_version": "dragontales.student-model-profile-verification.v1",
        "files": len(FILES),
        "model_artifact_bytes": profile["model_artifact_bytes"],
        "model_manifest_sha256": manifest_sha256,
        "profile_sha256": hashlib.sha256(profile_raw).hexdigest(),
    }


def _hash_model_file(path: Path, expected_size: int) -> str:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    before = os.fstat(descriptor)
    digest = hashlib.sha256()
    size = 0
    try:
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != MODEL_OWNER_UID
            or before.st_nlink != 1
            or before.st_size != expected_size
            or before.st_mode & 0o022
        ):
            raise ValueError(f"model file is not one owned immutable regular file: {path.name}")
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if size != expected_size or any(
        getattr(before, field) != getattr(after, field) for field in stable
    ):
        raise ValueError(f"model file changed while hashing: {path.name}")
    return digest.hexdigest()


def verify_tree(model: Path) -> dict[str, int | str]:
    receipt = verify()
    if not model.is_absolute() or model.is_symlink() or not model.is_dir():
        raise ValueError("model must be one absolute non-symlink directory")
    metadata = model.lstat()
    if metadata.st_uid != MODEL_OWNER_UID or metadata.st_mode & 0o022:
        raise ValueError("model root must be root-owned and immutable to group and other users")
    entries = sorted(model.iterdir(), key=lambda item: item.name)
    if [item.name for item in entries] != [item[0] for item in FILES]:
        raise ValueError("model tree differs from the exact Qwen inventory")
    for path, (_name, expected_digest, expected_size, _source) in zip(entries, FILES):
        if _hash_model_file(path, expected_size) != expected_digest:
            raise ValueError(f"model file digest differs from the exact Qwen release: {path.name}")
    return receipt


if __name__ == "__main__":
    print(json.dumps(verify(), separators=(",", ":")))
