from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import urllib.error

from milk_harness.eval_config import validate_eval_document
from milk_harness.evidence import R2EvidenceStore, canonical_json, create_same
from milk_harness.publish_image_admission import _reject_ambient_authorities


EVAL_DOCUMENT_PREFIX = "eval-documents/v1"


def publish_eval_document(store, raw, eval_id, harness_source_commit, root):
    validate_eval_document(raw, eval_id, harness_source_commit, root)
    outer_sha256 = hashlib.sha256(raw).hexdigest()
    key = f"{EVAL_DOCUMENT_PREFIX}/{eval_id}/{outer_sha256}.json"
    create_same(store, key, raw, "application/json")
    if store.get(key) != raw:
        raise ValueError("published eval document readback differs")
    return {
        "schema_version": "milk.eval-document-published.v1",
        "eval_id": eval_id,
        "outer_document_sha256": outer_sha256,
        "object_key": key,
        "state": "published",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Publish one immutable Milk eval document to evidence R2"
    )
    parser.add_argument("--document", required=True)
    parser.add_argument("--eval-id", required=True)
    parser.add_argument("--harness-source-commit", required=True)
    parser.add_argument("--root", required=True)
    arguments = parser.parse_args(argv)
    document = Path(arguments.document)
    try:
        _reject_ambient_authorities(os.environ)
        if document.is_symlink() or not document.is_file():
            raise ValueError("eval document must be a regular file")
        receipt = publish_eval_document(
            R2EvidenceStore.from_environment(),
            document.read_bytes(),
            arguments.eval_id,
            arguments.harness_source_commit,
            arguments.root,
        )
    except (
        OSError,
        RuntimeError,
        UnicodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"milk-eval-publication: {error}", file=os.sys.stderr)
        return 64
    os.sys.stdout.buffer.write(canonical_json(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
