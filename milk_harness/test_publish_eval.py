import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import milk_harness.publish_eval as publish_eval
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.publish_eval import EVAL_DOCUMENT_PREFIX, publish_eval_document
from milk_harness.test_eval_config import ROOT, eval_document


HARNESS_SOURCE_COMMIT = "d" * 40


class MissingReadbackStore:
    def create(self, key, body, content_type):
        return True

    def get(self, key):
        raise FileNotFoundError(key)


class PublishEvalTests(unittest.TestCase):
    def fixture(self):
        value = eval_document()
        raw = canonical_json(value)
        eval_id = value["manifest"]["campaign_id"]
        outer_sha256 = hashlib.sha256(raw).hexdigest()
        key = f"{EVAL_DOCUMENT_PREFIX}/{eval_id}/{outer_sha256}.json"
        return raw, eval_id, outer_sha256, key

    def test_publish_is_create_only_replay_safe_and_body_read_back(self):
        raw, eval_id, outer_sha256, key = self.fixture()
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(Path(root) / "evidence")
            receipt = publish_eval_document(
                store, raw, eval_id, HARNESS_SOURCE_COMMIT, ROOT
            )
            self.assertEqual(
                receipt,
                {
                    "schema_version": "milk.eval-document-published.v1",
                    "eval_id": eval_id,
                    "outer_document_sha256": outer_sha256,
                    "object_key": key,
                    "state": "published",
                },
            )
            self.assertEqual(store.get(key), raw)
            self.assertEqual(
                publish_eval_document(
                    store, raw, eval_id, HARNESS_SOURCE_COMMIT, ROOT
                ),
                receipt,
            )

    def test_collision_fails_closed(self):
        raw, eval_id, unused_outer_sha256, key = self.fixture()
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(Path(root) / "evidence")
            store.create(key, b"{}\n", "application/json")
            with self.assertRaisesRegex(ValueError, "differs"):
                publish_eval_document(
                    store, raw, eval_id, HARNESS_SOURCE_COMMIT, ROOT
                )

    def test_fresh_create_requires_body_readback(self):
        raw, eval_id, unused_outer_sha256, unused_key = self.fixture()
        with self.assertRaises(FileNotFoundError):
            publish_eval_document(
                MissingReadbackStore(), raw, eval_id, HARNESS_SOURCE_COMMIT, ROOT
            )

    def test_invalid_or_noncanonical_document_fails_before_create(self):
        raw, eval_id, unused_outer_sha256, unused_key = self.fixture()
        value = json.loads(raw)
        value["schema_version"] = "milk.eval.invalid"
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(Path(root) / "evidence")
            with self.assertRaisesRegex(ValueError, "eval document is invalid"):
                publish_eval_document(
                    store,
                    canonical_json(value),
                    eval_id,
                    HARNESS_SOURCE_COMMIT,
                    ROOT,
                )
            with self.assertRaisesRegex(ValueError, "canonical"):
                publish_eval_document(
                    store,
                    json.dumps(json.loads(raw)).encode(),
                    eval_id,
                    HARNESS_SOURCE_COMMIT,
                    ROOT,
                )
            self.assertEqual(store.list(EVAL_DOCUMENT_PREFIX), [])

    def test_operator_command_publishes_and_reads_back_exact_bytes(self):
        raw, eval_id, outer_sha256, key = self.fixture()
        with tempfile.TemporaryDirectory() as root:
            document = Path(root) / "eval.json"
            document.write_bytes(raw)
            store = LocalEvidenceStore(Path(root) / "evidence")
            stdout = types.SimpleNamespace(buffer=io.BytesIO())
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    publish_eval.R2EvidenceStore,
                    "from_environment",
                    return_value=store,
                ),
                mock.patch.object(publish_eval.os.sys, "stdout", stdout),
            ):
                self.assertEqual(
                    publish_eval.main(
                        [
                            "--document",
                            str(document),
                            "--eval-id",
                            eval_id,
                            "--harness-source-commit",
                            HARNESS_SOURCE_COMMIT,
                            "--root",
                            str(ROOT),
                        ]
                    ),
                    0,
                )
            receipt = json.loads(stdout.buffer.getvalue())
            self.assertEqual(receipt["outer_document_sha256"], outer_sha256)
            self.assertEqual(receipt["object_key"], key)
            self.assertEqual(store.get(key), raw)


if __name__ == "__main__":
    unittest.main()
