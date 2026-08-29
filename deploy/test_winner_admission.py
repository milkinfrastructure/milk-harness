import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import winner_admission


JOB_ID = "a" * 64
KEY = "candidate-secret"


class WinnerAdmissionTest(unittest.TestCase):
    def test_baseten_endpoint_is_authenticated_and_content_free(self):
        calls = []
        models_raw = json.dumps(
            {"object": "list", "data": [{"id": "customer-model", "object": "model"}]},
            separators=(",", ":"),
        ).encode()
        chat_raw = json.dumps(
            {
                "id": "chatcmpl-unit",
                "object": "chat.completion",
                "model": "customer-model",
                "choices": [{"message": {"role": "assistant", "content": "OK"}}],
            },
            separators=(",", ":"),
        ).encode()
        responses = [(403, b""), (200, models_raw), (200, chat_raw)]

        class Response:
            def __init__(self, status, raw):
                self.status = status
                self.raw = raw

            def read(self, _maximum):
                return self.raw

            def getheader(self, name, default):
                return "application/json" if name == "Content-Type" else default

        class Connection:
            def __init__(self, host, port, **_options):
                calls.append(("connect", host, port))

            def request(self, method, path, body=None, headers=None):
                calls.append(("request", method, path, body, headers))

            def getresponse(self):
                return Response(*responses.pop(0))

            def close(self):
                calls.append(("close",))

        manifest = b'{"schema_version":"dragontales.student-model-manifest.v1"}'
        manifest_sha256 = hashlib.sha256(manifest).hexdigest()
        receipt = {
            "schema_version": "dragontales.student-winner-materialization-receipt.v1",
            "student_job_id": JOB_ID,
            "variant": "static_fp8",
            "model_manifest_sha256": manifest_sha256,
            "file_count": 13,
            "artifact_bytes": 1024,
            "stage_path": "/tmp/dragontales-winner",
            "model_path": "/tmp/dragontales-winner/model",
            "model_manifest_path": "/tmp/dragontales-winner/model-manifest.json",
        }
        with tempfile.TemporaryDirectory() as directory:
            receipt_path = Path(directory) / "receipt.json"
            receipt_path.write_text(
                json.dumps(receipt, separators=(",", ":")) + "\n", encoding="utf-8"
            )
            result = winner_admission.qualify(
                "https://model-unit.api.baseten.co/environments/production/sync/v1/chat/completions",
                "customer-model",
                JOB_ID,
                candidate_key=KEY,
                materialize_path=receipt_path,
                expected_manifest_sha256=manifest_sha256,
                connection_factory=Connection,
            )

        requests = [call for call in calls if call[0] == "request"]
        self.assertEqual(
            [(call[1], call[2]) for call in requests],
            [
                ("GET", "/environments/production/sync/v1/models"),
                ("GET", "/environments/production/sync/v1/models"),
                ("POST", "/environments/production/sync/v1/chat/completions"),
            ],
        )
        self.assertNotIn("Authorization", requests[0][4])
        self.assertTrue(
            all(call[4]["Authorization"] == f"Bearer {KEY}" for call in requests[1:])
        )
        self.assertEqual(result["schema_version"], "dragontales.winner-admission-probe.v1")
        self.assertEqual(result["model_manifest_sha256"], manifest_sha256)
        self.assertEqual(result["candidate_api_key_sha256"], hashlib.sha256(KEY.encode()).hexdigest())
        self.assertNotIn("OK", json.dumps(result))

    def test_invalid_endpoints_fail_closed(self):
        for endpoint in (
            "http://model-unit.api.baseten.co/v1/chat/completions",
            "https://user@model-unit.api.baseten.co/v1/chat/completions",
            "https://model-unit.api.baseten.co/v1/chat/completions?x=1",
            "https://model-unit.api.baseten.co/environments/../sync/v1/chat/completions",
            "https://model-unit.api.baseten.co/v1/chat/completions/extra",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaisesRegex(ValueError, "endpoint"):
                winner_admission.qualify(endpoint, "customer-model", JOB_ID, candidate_key=KEY)


if __name__ == "__main__":
    unittest.main()
