import gzip
import json
import tempfile
import unittest

from milk_harness.evidence import LocalEvidenceStore, RunEvidence
from milk_harness.logs import LogArchive, REDACTION_PROFILE_SHA256, redact_line


class LogArchiveTests(unittest.TestCase):
    def evidence(self, root):
        import datetime as dt

        return RunEvidence(
            LocalEvidenceStore(root),
            "a" * 64,
            dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc),
        )

    def test_secrets_and_content_are_not_stored(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            logs = LogArchive(evidence, "baseten")
            logs.write(at="2026-08-27T20:00:00Z", line="Authorization: Bearer SECRET")
            logs.write(at="2026-08-27T20:00:01Z", line="prompt=private text")
            logs.write(at="2026-08-27T20:00:02Z", line="stage completed")
            index = logs.close(source_complete=True)
            self.assertEqual(index["redaction_profile_sha256"], REDACTION_PROFILE_SHA256)
            self.assertEqual(index["stream_id"], "0" * 64)
            compressed = evidence.store.get(index["chunks"][0]["key"])
            raw = gzip.decompress(compressed)
            self.assertNotIn(b"SECRET", raw)
            self.assertNotIn(b"private text", raw)
            records = [json.loads(line) for line in raw.splitlines()]
            self.assertEqual(records[0]["line"], "[SENSITIVE_WITHHELD]")
            self.assertEqual(records[1]["line"], "[CONTENT_WITHHELD]")
            self.assertEqual(records[2]["line"], "[LOG_TEXT_WITHHELD]")
            self.assertEqual(
                records[2]["line_sha256"],
                __import__("hashlib").sha256(b"stage completed").hexdigest(),
            )
            self.assertFalse(records[2]["line_truncated"])

    def test_limit_records_truncation_and_missing_source(self):
        with tempfile.TemporaryDirectory() as root:
            logs = LogArchive(self.evidence(root), "modal", max_run_bytes=5)
            self.assertTrue(logs.write(at="2026-08-27T20:00:00Z", line="12345"))
            self.assertFalse(logs.write(at="2026-08-27T20:00:01Z", line="6"))
            index = logs.close(source_complete=False, missing_reason="provider_stream_ended")
            self.assertTrue(index["truncated"])
            self.assertFalse(index["source_complete"])
            self.assertEqual(index["stored_records"], 1)

    def test_url_query_and_tokens_are_redacted(self):
        line, redacted, unused = redact_line("GET https://example.test/a?X-Amz-Signature=secret ghp_abcdefghijk")
        self.assertTrue(redacted)
        self.assertNotIn("secret", line)
        self.assertNotIn("ghp_", line)
        self.assertEqual(line, "[SENSITIVE_WITHHELD]")

    def test_distinct_streams_do_not_overwrite_one_another(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = self.evidence(root)
            one = LogArchive(evidence, "baseten", stream_id="1" * 64)
            two = LogArchive(evidence, "baseten", stream_id="2" * 64)
            one.write(at="2026-08-27T20:00:00Z", line="one")
            two.write(at="2026-08-27T20:00:01Z", line="two")
            first = one.close(source_complete=True)
            second = two.close(source_complete=True)
            self.assertNotEqual(first["chunks"][0]["key"], second["chunks"][0]["key"])


if __name__ == "__main__":
    unittest.main()
