from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


MAX_OBJECT_BYTES = 1024 * 1024
MAX_LIST_KEYS = 1000
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,1023}\Z")


def _validate_value(value):
    if value is None or isinstance(value, (str, bool)):
        return
    if type(value) is int:
        return
    if isinstance(value, list):
        for item in value:
            _validate_value(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError("canonical JSON keys must be nonempty strings")
            _validate_value(item)
        return
    raise ValueError("canonical evidence JSON supports null, strings, booleans, integers, lists, and objects")


def canonical_json(value):
    _validate_value(value)
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    if len(raw) > MAX_OBJECT_BYTES:
        raise ValueError("evidence object is oversized")
    return raw


def _key(value):
    if (
        not isinstance(value, str)
        or SAFE_KEY.fullmatch(value) is None
        or value.startswith("/")
        or value.endswith("/")
        or "//" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("evidence key is invalid")
    return value


class LocalEvidenceStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.is_symlink() or not self.root.is_dir():
            raise ValueError("local evidence root is unsafe")
        os.chmod(self.root, 0o700)

    def _path(self, key):
        path = self.root.joinpath(*_key(key).split("/"))
        if self.root not in path.parents:
            raise ValueError("evidence key escapes its root")
        return path

    def create(self, key, body, content_type="application/octet-stream"):
        del content_type
        if not isinstance(body, bytes) or len(body) > MAX_OBJECT_BYTES:
            raise ValueError("evidence body must be bounded bytes")
        path = self._path(key)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=path.name + ".",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as output:
                descriptor = None
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                return False
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return True
        finally:
            if descriptor is not None:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    def get(self, key):
        path = self._path(key)
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(key)
        with path.open("rb") as source:
            body = source.read(MAX_OBJECT_BYTES + 1)
        if len(body) > MAX_OBJECT_BYTES:
            raise ValueError("stored evidence object is oversized")
        return body

    def get_versioned(self, key):
        body = self.get(key)
        return body, '"' + hashlib.sha256(body).hexdigest() + '"'

    def replace(self, key, body, etag, content_type="application/octet-stream"):
        del content_type
        if not isinstance(body, bytes) or len(body) > MAX_OBJECT_BYTES:
            raise ValueError("evidence body must be bounded bytes")
        if not isinstance(etag, str) or not etag:
            raise ValueError("evidence ETag is invalid")
        path = self._path(key)
        lock_root = self.root / ".locks"
        lock_root.mkdir(mode=0o700, exist_ok=True)
        lock_name = hashlib.sha256(key.encode()).hexdigest() + ".lock"
        lock_path = lock_root / lock_name
        with lock_path.open("a+b", buffering=0) as lock:
            os.chmod(lock_path, 0o600)
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                current, current_etag = self.get_versioned(key)
                del current
                if current_etag != etag:
                    return False
                temporary = path.with_name(path.name + "." + hashlib.sha256(body).hexdigest() + ".tmp")
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    with os.fdopen(descriptor, "wb", closefd=True) as output:
                        descriptor = None
                        output.write(body)
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(temporary, path)
                    directory = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    if descriptor is not None:
                        os.close(descriptor)
                    if temporary.exists():
                        temporary.unlink()
                return True
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def list(self, prefix, limit=None, *, start_after=None):
        if limit is not None and (type(limit) is not int or not 1 <= limit <= MAX_LIST_KEYS):
            raise ValueError("evidence list limit is invalid")
        prefix = _key(prefix.rstrip("/") + "/sentinel").removesuffix("sentinel")
        if start_after is not None:
            start_after = _key(start_after)
            if not start_after.startswith(prefix):
                raise ValueError("evidence list start-after is outside the prefix")
        keys = []
        for path in self.root.rglob("*"):
            if path.is_symlink():
                raise ValueError("local evidence contains a symlink")
            if path.is_file():
                key = path.relative_to(self.root).as_posix()
                if key.startswith(prefix) and (start_after is None or key > start_after):
                    keys.append(key)
        return sorted(keys)[:limit]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


class R2EvidenceStore:
    def __init__(
        self,
        *,
        account_id,
        bucket,
        access_key_id,
        secret_access_key,
        session_token=None,
        timeout_seconds=30,
        opener=None,
        now=lambda: dt.datetime.now(dt.timezone.utc),
    ):
        for value, label in (
            (account_id, "account ID"),
            (bucket, "bucket"),
            (access_key_id, "access key ID"),
            (secret_access_key, "secret access key"),
        ):
            if not isinstance(value, str) or not value or any(character.isspace() for character in value):
                raise ValueError(f"R2 {label} is invalid")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120:
            raise ValueError("R2 timeout must be in 1..=120 seconds")
        if session_token is not None and (not isinstance(session_token, str) or not session_token):
            raise ValueError("R2 session token is invalid")
        self.host = f"{account_id}.r2.cloudflarestorage.com"
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_NoRedirect)
        self.now = now

    @classmethod
    def from_environment(cls, prefix="MILK_EVIDENCE_R2_"):
        def required(name):
            value = os.environ.get(prefix + name, "")
            if not value:
                raise ValueError(f"{prefix}{name} is required")
            return value

        return cls(
            account_id=required("ACCOUNT_ID"),
            bucket=required("BUCKET"),
            access_key_id=required("ACCESS_KEY_ID"),
            secret_access_key=required("SECRET_ACCESS_KEY"),
            session_token=os.environ.get(prefix + "SESSION_TOKEN") or None,
        )

    def _request(self, method, key=None, body=b"", query=(), headers=()):
        if not isinstance(body, bytes) or len(body) > MAX_OBJECT_BYTES:
            raise ValueError("R2 request body must be bounded bytes")
        current = self.now()
        if not isinstance(current, dt.datetime) or current.tzinfo is None or current.utcoffset() != dt.timedelta(0):
            raise ValueError("R2 clock must return UTC")
        current = current.astimezone(dt.timezone.utc)
        amz_date = current.strftime("%Y%m%dT%H%M%SZ")
        date = current.strftime("%Y%m%d")
        encoded_bucket = urllib.parse.quote(self.bucket, safe="-_.~")
        canonical_uri = f"/{encoded_bucket}"
        if key is not None:
            canonical_uri += "/" + "/".join(
                urllib.parse.quote(part, safe="-_.~") for part in _key(key).split("/")
            )
        encoded_query = sorted(
            (urllib.parse.quote(str(name), safe="-_.~"), urllib.parse.quote(str(value), safe="-_.~"))
            for name, value in query
        )
        canonical_query = "&".join(f"{name}={value}" for name, value in encoded_query)
        payload_sha256 = hashlib.sha256(body).hexdigest()
        signed = {
            "host": self.host,
            "x-amz-content-sha256": payload_sha256,
            "x-amz-date": amz_date,
        }
        if self.session_token is not None:
            signed["x-amz-security-token"] = self.session_token
        for name, value in headers:
            lowered = name.strip().lower()
            normalized = " ".join(value.strip().split())
            if lowered in signed or not lowered or not normalized:
                raise ValueError("R2 request header is invalid")
            signed[lowered] = normalized
        canonical_headers = "".join(f"{name}:{signed[name]}\n" for name in sorted(signed))
        signed_headers = ";".join(sorted(signed))
        canonical_request = "\n".join(
            (method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_sha256)
        )
        scope = f"{date}/auto/s3/aws4_request"
        string_to_sign = "\n".join(
            (
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            )
        )

        def sign(key_bytes, value):
            return hmac.new(key_bytes, value.encode(), hashlib.sha256).digest()

        signing_key = sign(
            sign(sign(sign(("AWS4" + self.secret_access_key).encode(), date), "auto"), "s3"),
            "aws4_request",
        )
        signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        request_headers = {name: value for name, value in signed.items() if name != "host"}
        request_headers["Authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.access_key_id}/{scope},"
            f"SignedHeaders={signed_headers},Signature={signature}"
        )
        url = f"https://{self.host}{canonical_uri}"
        if canonical_query:
            url += "?" + canonical_query
        return urllib.request.Request(url, data=body if method in {"PUT", "POST"} else None, method=method, headers=request_headers)

    def create(self, key, body, content_type="application/octet-stream"):
        request = self._request(
            "PUT",
            key,
            body,
            headers=(("content-type", content_type), ("if-none-match", "*")),
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                response.read(1)
                if response.getcode() not in {200, 201}:
                    raise RuntimeError("R2 create returned an unexpected status")
            return True
        except urllib.error.HTTPError as error:
            if error.code == 412:
                return False
            raise

    def get(self, key):
        body, unused_etag = self.get_versioned(key)
        del unused_etag
        return body

    def get_versioned(self, key):
        request = self._request("GET", key)
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                if response.getcode() != 200:
                    raise RuntimeError("R2 get returned an unexpected status")
                body = response.read(MAX_OBJECT_BYTES + 1)
                etag = response.headers.get("ETag")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise FileNotFoundError(key) from error
            raise
        if len(body) > MAX_OBJECT_BYTES:
            raise ValueError("stored evidence object is oversized")
        if not isinstance(etag, str) or not etag:
            raise ValueError("R2 get response has no ETag")
        return body, etag

    def replace(self, key, body, etag, content_type="application/octet-stream"):
        if not isinstance(etag, str) or not etag:
            raise ValueError("R2 ETag is invalid")
        request = self._request(
            "PUT",
            key,
            body,
            headers=(("content-type", content_type), ("if-match", etag)),
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                response.read(1)
                if response.getcode() not in {200, 201}:
                    raise RuntimeError("R2 replace returned an unexpected status")
            return True
        except urllib.error.HTTPError as error:
            if error.code == 412:
                return False
            raise

    def list(self, prefix, limit=None, *, start_after=None):
        if limit is not None and (type(limit) is not int or not 1 <= limit <= MAX_LIST_KEYS):
            raise ValueError("R2 list limit is invalid")
        prefix = _key(prefix.rstrip("/") + "/sentinel").removesuffix("sentinel")
        if start_after is not None:
            start_after = _key(start_after)
            if not start_after.startswith(prefix):
                raise ValueError("R2 list start-after is outside the prefix")
        continuation = None
        keys = []
        while True:
            page_limit = MAX_LIST_KEYS if limit is None else min(MAX_LIST_KEYS, limit - len(keys))
            query = [("list-type", "2"), ("max-keys", str(page_limit)), ("prefix", prefix)]
            if continuation is not None:
                query.append(("continuation-token", continuation))
            elif start_after is not None:
                query.append(("start-after", start_after))
            request = self._request("GET", query=query)
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                raw = response.read(MAX_OBJECT_BYTES + 1)
            if len(raw) > MAX_OBJECT_BYTES:
                raise ValueError("R2 list response is oversized")
            root = ET.fromstring(raw)
            namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            page = [node.text for node in root.findall("s3:Contents/s3:Key", namespace)]
            if len(page) > page_limit:
                raise ValueError("R2 list returned too many keys")
            if any(
                not isinstance(key, str)
                or not key.startswith(prefix)
                or (start_after is not None and key <= start_after)
                for key in page
            ):
                raise ValueError("R2 list returned an invalid key")
            keys.extend(page)
            if limit is not None and len(keys) >= limit:
                break
            truncated = root.findtext("s3:IsTruncated", default="false", namespaces=namespace)
            if truncated == "false":
                break
            if truncated != "true":
                raise ValueError("R2 list truncation flag is invalid")
            continuation = root.findtext("s3:NextContinuationToken", namespaces=namespace)
            if not continuation:
                raise ValueError("R2 list is missing a continuation token")
        if len(keys) != len(set(keys)):
            raise ValueError("R2 list returned duplicate keys")
        return sorted(keys)


def create_same(store, key, body, content_type="application/octet-stream"):
    if store.create(key, body, content_type):
        return "created"
    if store.get(key) != body:
        raise ValueError("existing evidence object differs")
    return "existing"


class RunEvidence:
    def __init__(self, store, run_id, occurred_at):
        if not isinstance(run_id, str) or HEX64.fullmatch(run_id) is None:
            raise ValueError("run ID must be lowercase hex64")
        if not isinstance(occurred_at, dt.datetime) or occurred_at.tzinfo is None or occurred_at.utcoffset() != dt.timedelta(0):
            raise ValueError("run timestamp must be UTC")
        self.store = store
        self.run_id = run_id
        self.prefix = f"runs/v1/{occurred_at:%Y/%m/%d}/{run_id}"

    def json(self, name, value):
        if not isinstance(name, str) or not name.endswith(".json") or "/" in name:
            raise ValueError("evidence JSON name is invalid")
        return create_same(self.store, f"{self.prefix}/{name}", canonical_json(value), "application/json")

    def bytes(self, relative, body, content_type="application/octet-stream"):
        if not isinstance(relative, str) or relative.startswith("/") or ".." in relative.split("/"):
            raise ValueError("evidence relative path is invalid")
        return create_same(self.store, f"{self.prefix}/{relative}", body, content_type)

    def manifest(self):
        keys = [key for key in self.store.list(self.prefix) if not key.endswith("/manifest.json")]
        objects = []
        for key in keys:
            body = self.store.get(key)
            objects.append(
                {
                    "key": key,
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                }
            )
        value = {
            "schema_version": "milk.run-evidence-manifest.v1",
            "run_id": self.run_id,
            "objects": objects,
        }
        self.json("manifest.json", value)
        return value
