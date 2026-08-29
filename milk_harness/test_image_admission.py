import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import milk_harness.publish_image_admission as publish_image_admission
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.image_admission import (
    ARTIFACT_PREFIX,
    RELEASE_PREFIX,
    load_local_private_gateway_release,
    load_local_private_image_release,
    load_published_private_gateway_release,
    load_published_private_image_release,
)
from milk_harness.publish_image_admission import (
    _reject_ambient_authorities,
    publish_private_gateway_release,
    publish_private_image_release,
)
from milk_harness.test_jobs import private_image_release


def private_gateway_release(root):
    root = Path(root)
    root.mkdir(parents=True)
    buildkit = "moby/buildkit@sha256:" + "6" * 64
    frontend = "docker/dockerfile:1.7@sha256:" + "7" * 64
    image = "ghcr.io/milkinfrastructure/milk-gateway@sha256:" + "1" * 64
    build_log_raw = canonical_json(
        {
            "schema_version": "milk.content-free-build-log.v1",
            "artifact": "gateway",
            "exit_code": 0,
            "started_at": "2026-08-29T14:00:10Z",
            "completed_at": "2026-08-29T14:01:50Z",
            "sha256": "2" * 64,
            "bytes": 123,
            "content_retained": False,
        }
    )
    ops_log_raw = canonical_json(
        {
            "schema_version": "milk.private-ops-log-reference.v1",
            "authority": "private-release-evidence",
            "reference": "build-log.json",
            "receipt_sha256": hashlib.sha256(build_log_raw).hexdigest(),
            "immutable": True,
            "content_retained": False,
        }
    )
    admission_raw = canonical_json(
        {
            "schema_version": "milk.private-image-admission.v1",
            "artifact": "gateway",
            "repository": "ghcr.io/milkinfrastructure/milk-gateway",
            "image_reference": image,
            "source_repository": "https://github.com/milkinfrastructure/milk-gateway",
            "source_commit": "8" * 40,
            "source_context_method": "git-archive-tar-v1",
            "source_context_sha256": "9" * 64,
            "gateway_image_reference": None,
            "index_sha256": "1" * 64,
            "amd64_manifest_sha256": "a" * 64,
            "config_sha256": "b" * 64,
            "attestation_manifest_sha256": "c" * 64,
            "attestations": [
                {
                    "layer_sha256": "d" * 64,
                    "predicate_type": "https://slsa.dev/provenance/v1",
                },
                {
                    "layer_sha256": "e" * 64,
                    "predicate_type": "https://spdx.dev/Document",
                },
            ],
            "platform": "linux/amd64",
            "visibility": "private",
            "builder": {
                "authority": "local-socket",
                "driver": "docker-container",
                "endpoint_kind": "local-socket",
                "buildkit_image_reference": buildkit,
                "buildkit_version": "v0.23.2",
                "dockerfile_frontend_reference": frontend,
                "provenance_mode": "max",
                "provenance_version": "v1",
                "sbom": True,
            },
        }
    )
    (root / "admission.json").write_bytes(admission_raw)
    (root / "build-log.json").write_bytes(build_log_raw)
    (root / "ops-log-reference.json").write_bytes(ops_log_raw)
    (root / "release.json").write_bytes(
        canonical_json(
            {
                "schema_version": "milk.private-gateway-release.v1",
                "source_commit": "8" * 40,
                "source_date_epoch": 1_777_777_777,
                "source_repository": "https://github.com/milkinfrastructure/milk-gateway",
                "buildkit_image_reference": buildkit,
                "dockerfile_frontend_reference": frontend,
                "build_authority": "local-socket",
                "platform": "linux/amd64",
                "image": {
                    "admission_sha256": hashlib.sha256(admission_raw).hexdigest(),
                    "artifact": "gateway",
                    "image_reference": image,
                },
                "ops_log_reference_sha256": hashlib.sha256(
                    ops_log_raw
                ).hexdigest(),
                "started_at": "2026-08-29T14:00:00Z",
                "completed_at": "2026-08-29T14:02:00Z",
            }
        )
    )
    return str(root)


class FailedCreatedReadbackStore(LocalEvidenceStore):
    def __init__(self, root):
        super().__init__(root)
        self.failed_key = None

    def create(self, key, body, content_type="application/octet-stream"):
        created = super().create(key, body, content_type)
        if created and self.failed_key is None:
            self.failed_key = key
        return created

    def get(self, key):
        if key == self.failed_key:
            return b"{}\n"
        return super().get(key)


class ImageAdmissionTests(unittest.TestCase):
    def fixture(self, root):
        directory = Path(root) / "release"
        release = load_local_private_image_release(private_image_release(directory))
        return directory, release

    def gateway_fixture(self, root):
        directory = Path(root) / "gateway-release"
        release = load_local_private_gateway_release(
            private_gateway_release(directory)
        )
        return directory, release

    def test_missing_release_or_admission_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            directory, release = self.fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            with self.assertRaises(FileNotFoundError):
                load_published_private_image_release(store, release["release_sha256"])
            store.create(
                f"{RELEASE_PREFIX}/{release['release_sha256']}.json",
                (directory / "release.json").read_bytes(),
                "application/json",
            )
            with self.assertRaises(FileNotFoundError):
                load_published_private_image_release(store, release["release_sha256"])

    def test_release_sha_cannot_be_cross_bound(self):
        with tempfile.TemporaryDirectory() as root:
            unused_directory, release = self.fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            requested = "f" * 64
            store.create(
                f"{RELEASE_PREFIX}/{requested}.json",
                release["release_raw"],
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "authority key"):
                load_published_private_image_release(store, requested)

    def test_admission_sha_cannot_be_missing_corrupt_or_cross_bound(self):
        with tempfile.TemporaryDirectory() as root:
            unused_directory, release = self.fixture(root)
            release_value = json.loads(release["release_raw"])
            foreign_sha = "f" * 64
            release_value["images"][0]["admission_sha256"] = foreign_sha
            release_raw = canonical_json(release_value)
            release_sha = hashlib.sha256(release_raw).hexdigest()
            store = LocalEvidenceStore(Path(root) / "evidence")
            store.create(
                f"{RELEASE_PREFIX}/{release_sha}.json",
                release_raw,
                "application/json",
            )
            store.create(
                f"{ARTIFACT_PREFIX}/{foreign_sha}.json",
                release["images"]["student-train"]["raw"],
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "admission is invalid"):
                load_published_private_image_release(store, release_sha)

    def test_publish_is_create_only_and_replay_safe(self):
        with tempfile.TemporaryDirectory() as root:
            unused_directory, release = self.fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            receipt = publish_private_image_release(store, release)
            self.assertEqual(publish_private_image_release(store, release), receipt)
            admission = release["images"]["student-train"]
            conflicting = LocalEvidenceStore(Path(root) / "conflicting")
            conflicting.create(
                f"{ARTIFACT_PREFIX}/{admission['admission_sha256']}.json",
                b"{}\n",
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "differs"):
                publish_private_image_release(conflicting, release)
            with self.assertRaises(FileNotFoundError):
                conflicting.get(f"{RELEASE_PREFIX}/{release['release_sha256']}.json")

    def test_gateway_release_is_valid_locally_and_when_published(self):
        with tempfile.TemporaryDirectory() as root:
            unused_directory, release = self.gateway_fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            receipt = publish_private_gateway_release(store, release)
            self.assertEqual(
                receipt["artifact_admission_sha256s"],
                {"gateway": release["image"]["admission_sha256"]},
            )
            self.assertEqual(
                load_published_private_gateway_release(
                    store,
                    release["release_sha256"],
                ),
                release,
            )

    def test_gateway_publication_is_create_only_and_replay_safe(self):
        with tempfile.TemporaryDirectory() as root:
            unused_directory, release = self.gateway_fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            receipt = publish_private_gateway_release(store, release)
            self.assertEqual(publish_private_gateway_release(store, release), receipt)
            admission = release["image"]
            conflicting = LocalEvidenceStore(Path(root) / "conflicting")
            conflicting.create(
                f"{ARTIFACT_PREFIX}/{admission['admission_sha256']}.json",
                b"{}\n",
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "differs"):
                publish_private_gateway_release(conflicting, release)
            with self.assertRaises(FileNotFoundError):
                conflicting.get(f"{RELEASE_PREFIX}/{release['release_sha256']}.json")

    def test_gateway_release_cannot_cross_bind_admission_or_ops_log(self):
        with tempfile.TemporaryDirectory() as root:
            directory, release = self.gateway_fixture(root)
            value = json.loads(release["release_raw"])
            foreign_sha = "f" * 64
            value["image"]["admission_sha256"] = foreign_sha
            release_raw = canonical_json(value)
            release_sha = hashlib.sha256(release_raw).hexdigest()
            store = LocalEvidenceStore(Path(root) / "evidence")
            store.create(
                f"{RELEASE_PREFIX}/{release_sha}.json",
                release_raw,
                "application/json",
            )
            store.create(
                f"{ARTIFACT_PREFIX}/{foreign_sha}.json",
                release["image"]["raw"],
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "gateway admission is invalid"):
                load_published_private_gateway_release(store, release_sha)

            local_value = json.loads((directory / "release.json").read_bytes())
            local_value["ops_log_reference_sha256"] = "f" * 64
            (directory / "release.json").write_bytes(canonical_json(local_value))
            with self.assertRaisesRegex(ValueError, "ops-log reference"):
                load_local_private_gateway_release(str(directory))

    def test_newly_created_publication_requires_full_body_readback(self):
        for gateway in (False, True):
            with self.subTest(gateway=gateway), tempfile.TemporaryDirectory() as root:
                if gateway:
                    unused_directory, release = self.gateway_fixture(root)
                    publisher = publish_private_gateway_release
                else:
                    unused_directory, release = self.fixture(root)
                    publisher = publish_private_image_release
                store = FailedCreatedReadbackStore(Path(root) / "evidence")
                with self.assertRaisesRegex(ValueError, "full-body readback"):
                    publisher(store, release)

    def test_verified_v3_release_remains_publishable(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "release-v3"
            release = load_local_private_image_release(
                private_image_release(
                    directory,
                    schema_version="milk.private-harness-release.v3",
                )
            )
            self.assertEqual(
                set(release["images"]),
                {
                    "student-train",
                    "student-branch",
                    "teacher-gpt-oss",
                    "planner",
                    "jobs",
                },
            )
            store = LocalEvidenceStore(Path(root) / "evidence")
            receipt = publish_private_image_release(store, release)
            self.assertEqual(
                set(receipt["artifact_admission_sha256s"]),
                set(release["images"]),
            )
            self.assertEqual(
                load_published_private_image_release(
                    store,
                    release["release_sha256"],
                ),
                release,
            )

    def test_v5_release_binds_each_admission_to_its_source_commit(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "release-v5"
            release = load_local_private_image_release(
                private_image_release(
                    directory,
                    schema_version="milk.private-harness-release.v5",
                )
            )
            value = json.loads(release["release_raw"])
            source_commits = {
                item["artifact"]: item["source_commit"]
                for item in value["images"]
            }
            self.assertEqual(source_commits["student-train"], "8" * 40)
            self.assertEqual(source_commits["student-branch"], "8" * 40)
            self.assertEqual(source_commits["teacher-gpt-oss"], "8" * 40)
            self.assertEqual(source_commits["jobs"], "a" * 40)
            store = LocalEvidenceStore(Path(root) / "evidence")
            publish_private_image_release(store, release)
            self.assertEqual(
                load_published_private_image_release(
                    store,
                    release["release_sha256"],
                ),
                release,
            )

    def test_v5_release_rejects_a_cross_bound_source_commit(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "release-v5"
            private_image_release(
                directory,
                schema_version="milk.private-harness-release.v5",
            )
            release_path = directory / "release.json"
            value = json.loads(release_path.read_bytes())
            value["images"][0]["source_commit"] = "f" * 40
            release_path.write_bytes(canonical_json(value))
            with self.assertRaisesRegex(ValueError, "admission is invalid"):
                load_local_private_image_release(str(directory))

    def test_local_bundle_must_be_canonical(self):
        with tempfile.TemporaryDirectory() as root:
            directory, unused_release = self.fixture(root)
            value = json.loads((directory / "release.json").read_bytes())
            (directory / "release.json").write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "canonical"):
                load_local_private_image_release(str(directory))

    def test_local_bundle_rejects_unbound_ops_log_reference(self):
        with tempfile.TemporaryDirectory() as root:
            directory, unused_release = self.fixture(root)
            path = directory / "student-train" / "ops-log-reference.json"
            value = json.loads(path.read_bytes())
            value["receipt_sha256"] = "f" * 64
            path.write_bytes(canonical_json(value))
            with self.assertRaisesRegex(ValueError, "ops-log reference"):
                load_local_private_image_release(str(directory))

    def test_publisher_rejects_other_authorities(self):
        clean = {
            "MILK_EVIDENCE_R2_ACCOUNT_ID": "0" * 32,
            "MILK_EVIDENCE_R2_BUCKET": "evidence",
            "MILK_EVIDENCE_R2_ACCESS_KEY_ID": "access",
            "MILK_EVIDENCE_R2_SECRET_ACCESS_KEY": "secret",
        }
        _reject_ambient_authorities(clean)
        for name in (
            "BASETEN_API_KEY",
            "MODAL_TOKEN_SECRET",
            "GH_TOKEN",
            "MILK_CONTROL_R2_SECRET_ACCESS_KEY",
            "MILK_OPS_LOG_R2_SECRET_ACCESS_KEY",
            "MILK_CREATE_AUTHORITY_WRITE_R2_SECRET_ACCESS_KEY",
            "MILK_PROVIDER_GPU_CAPTURE_R2_ACCESS_KEY_ID",
            "MILK_GATEWAY_INGEST_ROUTE_R2_SECRET_ACCESS_KEY",
            "MILK_EVIDENCE_R2_UNRECOGNIZED",
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError,
                "only its evidence authority",
            ):
                _reject_ambient_authorities({**clean, name: "secret"})

    def test_operator_command_publishes_the_validated_bundle(self):
        with tempfile.TemporaryDirectory() as root:
            directory, release = self.fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            stdout = types.SimpleNamespace(buffer=io.BytesIO())
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    publish_image_admission.R2EvidenceStore,
                    "from_environment",
                    return_value=store,
                ),
                mock.patch.object(publish_image_admission.os.sys, "stdout", stdout),
            ):
                self.assertEqual(
                    publish_image_admission.main(["--release-dir", str(directory)]),
                    0,
                )
            receipt = json.loads(stdout.buffer.getvalue())
            self.assertEqual(receipt["release_sha256"], release["release_sha256"])
            self.assertEqual(
                load_published_private_image_release(
                    store,
                    release["release_sha256"],
                ),
                release,
            )

    def test_operator_command_publishes_the_validated_gateway_bundle(self):
        with tempfile.TemporaryDirectory() as root:
            directory, release = self.gateway_fixture(root)
            store = LocalEvidenceStore(Path(root) / "evidence")
            stdout = types.SimpleNamespace(buffer=io.BytesIO())
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    publish_image_admission.R2EvidenceStore,
                    "from_environment",
                    return_value=store,
                ),
                mock.patch.object(publish_image_admission.os.sys, "stdout", stdout),
            ):
                self.assertEqual(
                    publish_image_admission.main(
                        ["--gateway-release-dir", str(directory)]
                    ),
                    0,
                )
            receipt = json.loads(stdout.buffer.getvalue())
            self.assertEqual(receipt["release_sha256"], release["release_sha256"])
            self.assertEqual(
                load_published_private_gateway_release(
                    store,
                    release["release_sha256"],
                ),
                release,
            )
if __name__ == "__main__":
    unittest.main()
