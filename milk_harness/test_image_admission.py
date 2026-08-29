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
    load_local_private_image_release,
    load_published_private_image_release,
)
from milk_harness.publish_image_admission import (
    _reject_ambient_authorities,
    publish_private_image_release,
)
from milk_harness.test_jobs import private_image_release


class ImageAdmissionTests(unittest.TestCase):
    def fixture(self, root):
        directory = Path(root) / "release"
        release = load_local_private_image_release(private_image_release(directory))
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
if __name__ == "__main__":
    unittest.main()
