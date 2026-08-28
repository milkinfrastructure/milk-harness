import datetime as dt
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error

from milk_harness.evidence import (
    LocalEvidenceStore,
    R2EvidenceStore,
    RunEvidence,
    canonical_json,
    create_same,
)


UTC = dt.timezone.utc


class _Response:
    def __init__(self, code=200, body=b""):
        self.code = code
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def getcode(self):
        return self.code

    def read(self, limit=-1):
        if limit < 0:
            return self.body
        return self.body[:limit]


class _Opener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class EvidenceTests(unittest.TestCase):
    def test_local_create_same_is_immutable_and_manifest_is_exact(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            evidence = RunEvidence(
                store,
                "a" * 64,
                dt.datetime(2026, 8, 27, 20, 1, 2, tzinfo=UTC),
            )
            intent = {
                "schema_version": "milk.run-intent.v1",
                "run_id": "a" * 64,
                "max_cost_microusd": 10,
            }
            self.assertEqual(evidence.json("intent.json", intent), "created")
            self.assertEqual(evidence.json("intent.json", intent), "existing")
            with self.assertRaisesRegex(ValueError, "differs"):
                evidence.json("intent.json", {**intent, "max_cost_microusd": 11})
            terminal = canonical_json({"schema_version": "milk.run-terminal.v1", "state": "ready"})
            evidence.bytes("terminal.json", terminal, "application/json")
            manifest = evidence.manifest()
            self.assertEqual(
                [item["key"].rsplit("/", 1)[1] for item in manifest["objects"]],
                ["intent.json", "terminal.json"],
            )
            for item in manifest["objects"]:
                body = store.get(item["key"])
                self.assertEqual(item["sha256"], hashlib.sha256(body).hexdigest())
            self.assertEqual(evidence.manifest(), manifest)
            self.assertEqual(Path(root).stat().st_mode & 0o777, 0o700)

    def test_canonical_json_rejects_floats_and_unsafe_paths(self):
        with self.assertRaisesRegex(ValueError, "canonical"):
            canonical_json({"cost": 1.5})
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            with self.assertRaisesRegex(ValueError, "key"):
                store.create("../secret", b"x")

    def test_local_list_start_after_is_bounded_to_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalEvidenceStore(root)
            for key in ("frontier/000/a", "frontier/001/a", "other/000/a"):
                self.assertTrue(store.create(key, b"{}\n"))
            self.assertEqual(
                store.list("frontier", limit=1, start_after="frontier/000/a"),
                ["frontier/001/a"],
            )
            with self.assertRaisesRegex(ValueError, "outside the prefix"):
                store.list("frontier", start_after="other/000/a")
            with self.assertRaises(TypeError):
                store.list("frontier", 1, "frontier/000/a")

    def test_r2_create_is_signed_and_412_is_existing(self):
        fixed = dt.datetime(2026, 8, 27, 20, 1, 2, tzinfo=UTC)
        conflict = urllib.error.HTTPError("https://example", 412, "exists", {}, None)
        opener = _Opener([_Response(200), conflict])
        store = R2EvidenceStore(
            account_id="0" * 32,
            bucket="milk-evidence",
            access_key_id="ACCESS",
            secret_access_key="SECRET",
            session_token="SESSION",
            opener=opener,
            now=lambda: fixed,
        )
        self.assertTrue(store.create("runs/v1/a.json", b"{}\n", "application/json"))
        self.assertFalse(store.create("runs/v1/a.json", b"{}\n", "application/json"))
        request, timeout = opener.requests[0]
        self.assertEqual(timeout, 30)
        self.assertEqual(request.full_url, "https://" + "0" * 32 + ".r2.cloudflarestorage.com/milk-evidence/runs/v1/a.json")
        headers = {name.lower(): value for name, value in request.header_items()}
        self.assertEqual(headers["if-none-match"], "*")
        self.assertEqual(headers["x-amz-security-token"], "SESSION")
        self.assertTrue(headers["authorization"].startswith("AWS4-HMAC-SHA256 Credential=ACCESS/20260827/auto/s3/aws4_request,"))
        self.assertNotIn("SECRET", json.dumps(headers))

    def test_r2_list_start_after_is_bounded_sorted_and_signed(self):
        body = b'''<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>false</IsTruncated>
<Contents><Key>runs/v1/c</Key></Contents>
<Contents><Key>runs/v1/b</Key></Contents>
</ListBucketResult>'''
        opener = _Opener([_Response(200, body)])
        store = R2EvidenceStore(
            account_id="0" * 32,
            bucket="milk-evidence",
            access_key_id="ACCESS",
            secret_access_key="SECRET",
            opener=opener,
            now=lambda: dt.datetime(2026, 8, 27, tzinfo=UTC),
        )
        self.assertEqual(
            store.list("runs/v1", limit=19, start_after="runs/v1/a"),
            ["runs/v1/b", "runs/v1/c"],
        )
        request = opener.requests[0][0]
        self.assertIn("list-type=2", request.full_url)
        self.assertIn("max-keys=19", request.full_url)
        self.assertIn("prefix=runs%2Fv1%2F", request.full_url)
        self.assertIn("start-after=runs%2Fv1%2Fa", request.full_url)
        headers = {name.lower(): value for name, value in request.header_items()}
        self.assertEqual(
            headers["authorization"],
            "AWS4-HMAC-SHA256 Credential=ACCESS/20260827/auto/s3/aws4_request,"
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            "Signature=8f8147fea74e3f54df06d4f72795c77a09c74d41c774c77cdeb2d44074b3a631",
        )
        with self.assertRaisesRegex(ValueError, "outside the prefix"):
            store.list("runs/v1", start_after="runs/v10/a")


if __name__ == "__main__":
    unittest.main()
