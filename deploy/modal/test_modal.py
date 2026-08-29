import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from deploy import winner_admission as admit

JOB = "a" * 64
CANDIDATE_KEY = b"candidate-secret"


class AdmissionProgramTests(unittest.TestCase):
    def test_authenticated_tls_models_and_chat_are_content_free(self):
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
        responses = [(401, b'{"error":"unauthorized"}'), (200, models_raw), (200, chat_raw)]

        class Response:
            def __init__(self, status, raw):
                self.status = status
                self.raw = raw

            def read(self, maximum):
                self.maximum = maximum
                return self.raw

            def getheader(self, name, default):
                return "application/json; charset=utf-8" if name == "Content-Type" else default

        class Connection:
            def __init__(self, host, port, **kwargs):
                calls.append(("connect", host, port, kwargs))

            def request(self, method, path, body=None, headers=None):
                calls.append(("request", method, path, body, headers))

            def getresponse(self):
                return Response(*responses.pop(0))

            def close(self):
                calls.append(("close",))

        manifest = b'{"schema_version":"dragontales.student-model-manifest.v1"}'
        materialize_receipt = {
            "schema_version": "dragontales.student-winner-materialization-receipt.v1",
            "student_job_id": JOB,
            "variant": "static_fp8",
            "model_manifest_sha256": hashlib.sha256(manifest).hexdigest(),
            "file_count": 13,
            "artifact_bytes": 1024,
            "stage_path": "/tmp/dragontales-winner",
            "model_path": "/tmp/dragontales-winner/model",
            "model_manifest_path": "/tmp/dragontales-winner/model-manifest.json",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key_path = root / "key"
            receipt_path = root / "materialize.stdout"
            manifest_path = root / "model-manifest.json"
            key_path.write_bytes(CANDIDATE_KEY)
            manifest_path.write_bytes(manifest)
            receipt_path.write_text(
                json.dumps(materialize_receipt, separators=(",", ":"))
                + "\n",
                encoding="utf-8",
            )
            result = admit.qualify(
                "https://unit.w.modal.host/v1/chat/completions",
                "customer-model",
                JOB,
                key_path=root / "must-not-be-read",
                candidate_key=CANDIDATE_KEY.decode("ascii"),
                materialize_path=receipt_path,
                manifest_path=manifest_path,
                connection_factory=Connection,
            )

        requests = [item for item in calls if item[0] == "request"]
        self.assertEqual([(item[1], item[2]) for item in requests], [
            ("GET", "/v1/models"),
            ("GET", "/v1/models"),
            ("POST", "/v1/chat/completions"),
        ])
        self.assertNotIn("Authorization", requests[0][4])
        self.assertTrue(
            all(
                item[4]["Authorization"] == "Bearer candidate-secret"
                for item in requests[1:]
            )
        )
        smoke = json.loads(requests[2][3])
        self.assertEqual(smoke["max_tokens"], 8)
        self.assertFalse(smoke["stream"])
        self.assertEqual(result["model_manifest_sha256"], hashlib.sha256(manifest).hexdigest())
        self.assertEqual(
            result["candidate_api_key_sha256"], hashlib.sha256(CANDIDATE_KEY).hexdigest()
        )
        self.assertEqual(result["models_response_sha256"], hashlib.sha256(models_raw).hexdigest())
        self.assertEqual(result["chat_response_sha256"], hashlib.sha256(chat_raw).hexdigest())
        self.assertNotIn("OK", json.dumps(result))

        responses.extend(
            [(403, b""), (200, models_raw), (200, chat_raw)]
        )
        calls.clear()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key_path = root / "key"
            receipt_path = root / "materialize.stdout"
            key_path.write_bytes(CANDIDATE_KEY)
            receipt_path.write_text(
                json.dumps(materialize_receipt, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            result = admit.qualify(
                "https://model-unit.api.baseten.co/environments/production/sync/v1/chat/completions",
                "customer-model",
                JOB,
                key_path=key_path,
                materialize_path=receipt_path,
                expected_manifest_sha256=hashlib.sha256(manifest).hexdigest(),
                connection_factory=Connection,
            )
        requests = [item for item in calls if item[0] == "request"]
        self.assertEqual(
            [(item[1], item[2]) for item in requests],
            [
                ("GET", "/environments/production/sync/v1/models"),
                ("GET", "/environments/production/sync/v1/models"),
                ("POST", "/environments/production/sync/v1/chat/completions"),
            ],
        )
        self.assertEqual(result["schema_version"], "dragontales.winner-admission-probe.v1")

        for endpoint in (
            "https://model-unit.api.baseten.co/v1/chat/completions?x=1",
            "https://user@model-unit.api.baseten.co/v1/chat/completions",
            "https://model-unit.api.baseten.co/environments//production/sync/v1/chat/completions",
            "https://model-unit.api.baseten.co/environments/../sync/v1/chat/completions",
            "https://model-unit.api.baseten.co/v1/chat/completions/extra",
        ):
            with self.assertRaisesRegex(ValueError, "endpoint"):
                admit.qualify(
                    endpoint,
                    "customer-model",
                    JOB,
                    key_path=key_path,
                    materialize_path=receipt_path,
                    manifest_path=manifest_path,
                    connection_factory=Connection,
                )
if __name__ == "__main__":
    unittest.main()
