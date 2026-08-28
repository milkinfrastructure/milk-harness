from __future__ import annotations

import gzip
import hashlib
import io
import re

from milk_harness.evidence import canonical_json


MAX_LINE_BYTES = 64 * 1024
MAX_CHUNK_BYTES = 1024 * 1024
MAX_RUN_LOG_BYTES = 100 * 1024 * 1024
SOURCES = {"local", "ci", "ghcr", "cloudflare", "baseten", "modal", "gateway"}
STREAM_ID = re.compile(r"[0-9a-f]{64}\Z")
CONTENT_WORDS = re.compile(
    r"(?i)(prompt|request[_ -]?body|response[_ -]?body|messages|dataset[_ -]?(?:row|sample)|training[_ -]?example)"
)
SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*)([^\s,;]+(?:\s+[^\s,;]+)?)"),
    re.compile(r"(?i)(api[-_ ]?key\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)((?:secret|password|token|credential)\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"\b(?:sk-|ghp_|github_pat_|xox[baprs]-)[-A-Za-z0-9_]{8,}\b"),
    re.compile(r"\bAKIA[A-Z0-9]{16}\b"),
    re.compile(r"(?i)(https?://[^\s?]+)\?[^\s]+"),
)
REDACTION_PROFILE = {
    "schema_version": "milk.log-redaction-profile.v1",
    "content_line_policy": "withhold_all_free_form_text_preserve_sha256",
    "secret_policy": "withhold_all_free_form_text",
    "max_line_bytes": MAX_LINE_BYTES,
    "max_chunk_bytes": MAX_CHUNK_BYTES,
    "max_run_log_bytes": MAX_RUN_LOG_BYTES,
}
REDACTION_PROFILE_SHA256 = hashlib.sha256(canonical_json(REDACTION_PROFILE)).hexdigest()


def redact_line(value):
    if not isinstance(value, str):
        raise ValueError("log line must be text")
    raw = value.encode("utf-8")
    original_bytes = len(raw)
    if len(raw) > MAX_LINE_BYTES:
        value = raw[:MAX_LINE_BYTES].decode("utf-8", "ignore") + " [LINE_TRUNCATED]"
    classified_content = CONTENT_WORDS.search(value) is not None
    classified_secret = False
    for pattern in SECRET_PATTERNS:
        if pattern.pattern.startswith("(?i)(") and pattern.groups >= 2:
            unused_value, count = pattern.subn(
                lambda match: match.group(1) + "[REDACTED]", value
            )
        else:
            unused_value, count = pattern.subn("[REDACTED]", value)
        del unused_value
        classified_secret = classified_secret or count > 0
    if classified_content:
        replacement = "[CONTENT_WITHHELD]"
    elif classified_secret:
        replacement = "[SENSITIVE_WITHHELD]"
    else:
        replacement = "[LOG_TEXT_WITHHELD]"
    return replacement, True, original_bytes


class LogArchive:
    def __init__(
        self,
        evidence,
        source,
        *,
        stream_id="0" * 64,
        max_run_bytes=MAX_RUN_LOG_BYTES,
    ):
        if source not in SOURCES:
            raise ValueError("log source is invalid")
        if not isinstance(stream_id, str) or STREAM_ID.fullmatch(stream_id) is None:
            raise ValueError("log stream ID is invalid")
        if type(max_run_bytes) is not int or not 1 <= max_run_bytes <= MAX_RUN_LOG_BYTES:
            raise ValueError("log byte limit is invalid")
        self.evidence = evidence
        self.source = source
        self.stream_id = stream_id
        self.max_run_bytes = max_run_bytes
        self.buffer = bytearray()
        self.chunks = []
        self.original_bytes = 0
        self.stored_records = 0
        self.redacted_records = 0
        self.truncated = False
        self.closed = False
        self.first_at = None
        self.last_at = None
        self.last_cursor = None

    def write(self, *, at, line, provider_cursor=None):
        if self.closed:
            raise ValueError("log archive is closed")
        if not isinstance(at, str) or not at.endswith("Z"):
            raise ValueError("log timestamp must be canonical UTC text")
        if provider_cursor is not None and (not isinstance(provider_cursor, str) or len(provider_cursor) > 1024):
            raise ValueError("provider cursor is invalid")
        encoded_line = line.encode("utf-8")
        sanitized, redacted, original_bytes = redact_line(line)
        if self.original_bytes + original_bytes > self.max_run_bytes:
            self.truncated = True
            return False
        record = {
            "schema_version": "milk.redacted-log-record.v1",
            "at": at,
            "source": self.source,
            "line": sanitized,
            "line_sha256": hashlib.sha256(encoded_line).hexdigest(),
            "line_truncated": original_bytes > MAX_LINE_BYTES,
            "redacted": redacted,
            "original_bytes": original_bytes,
        }
        if provider_cursor is not None:
            record["provider_cursor"] = provider_cursor
        encoded = canonical_json(record)
        if len(self.buffer) + len(encoded) > MAX_CHUNK_BYTES and self.buffer:
            self._flush()
        self.buffer.extend(encoded)
        self.original_bytes += original_bytes
        self.stored_records += 1
        self.redacted_records += int(redacted)
        self.first_at = self.first_at or at
        self.last_at = at
        self.last_cursor = provider_cursor
        return True

    def _flush(self):
        if not self.buffer:
            return
        output = io.BytesIO()
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0, filename="") as archive:
            archive.write(self.buffer)
        compressed = output.getvalue()
        if len(compressed) > MAX_CHUNK_BYTES:
            raise ValueError("compressed log chunk exceeds its bound")
        sequence = len(self.chunks)
        relative = (
            f"logs/{self.source}/{self.stream_id}/{sequence:06d}.jsonl.gz"
        )
        self.evidence.bytes(relative, compressed, "application/gzip")
        self.chunks.append(
            {
                "key": f"{self.evidence.prefix}/{relative}",
                "bytes": len(compressed),
                "sha256": hashlib.sha256(compressed).hexdigest(),
                "records_through": self.stored_records,
            }
        )
        self.buffer.clear()

    def close(self, *, source_complete, missing_reason=None):
        if self.closed:
            raise ValueError("log archive is already closed")
        if type(source_complete) is not bool:
            raise ValueError("source_complete must be boolean")
        if source_complete and missing_reason is not None:
            raise ValueError("complete log source cannot have a missing reason")
        if not source_complete and (not isinstance(missing_reason, str) or not missing_reason):
            raise ValueError("incomplete log source requires a reason")
        self._flush()
        index = {
            "schema_version": "milk.redacted-log-index.v1",
            "run_id": self.evidence.run_id,
            "source": self.source,
            "stream_id": self.stream_id,
            "redaction_profile_sha256": REDACTION_PROFILE_SHA256,
            "first_at": self.first_at,
            "last_at": self.last_at,
            "last_provider_cursor": self.last_cursor,
            "original_bytes": self.original_bytes,
            "stored_records": self.stored_records,
            "redacted_records": self.redacted_records,
            "truncated": self.truncated,
            "source_complete": source_complete,
            "missing_reason": missing_reason,
            "chunks": self.chunks,
        }
        self.evidence.bytes(
            f"logs/{self.source}/{self.stream_id}/index.json",
            canonical_json(index),
            "application/json",
        )
        self.closed = True
        return index
