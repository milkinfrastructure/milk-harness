#!/usr/bin/env python3
"""Offline contract test for the Modal student-volume population helper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types
import unittest


ROOT = Path(__file__).resolve().parents[2]
HELPER = Path(__file__).with_name("populate_student_volume.py")
VERIFIER = ROOT / "deploy/models/qwen3-4b-instruct-2507/verify.py"


class _Image:
    def pip_install(self, *_packages):
        return self


class _App:
    def function(self, **_options):
        return lambda function: function

    def local_entrypoint(self):
        return lambda function: function


def _load_helper():
    fake_modal = types.SimpleNamespace(
        App=lambda _name: _App(),
        Image=types.SimpleNamespace(debian_slim=lambda **_options: _Image()),
        Volume=types.SimpleNamespace(
            from_name=lambda _name, **_options: types.SimpleNamespace(commit=lambda: None)
        ),
    )
    previous = sys.modules.get("modal")
    sys.modules["modal"] = fake_modal
    try:
        spec = importlib.util.spec_from_file_location("populate_student_volume", HELPER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            del sys.modules["modal"]
        else:
            sys.modules["modal"] = previous


class PopulateStudentVolumeTest(unittest.TestCase):
    def test_manifest_is_derived_from_and_bound_to_the_admitted_verifier(self):
        helper = _load_helper()
        raw = helper._manifest_from_verifier(VERIFIER)
        files = helper._validate_manifest(__import__("json").loads(raw))
        self.assertEqual(len(files), 13)
        self.assertEqual(sum(item["bytes"] for item in files), 8_060_917_568)
        changed = {**files[0], "sha256": "0" * 64}
        with self.assertRaisesRegex(ValueError, "admitted inventory"):
            helper._validate_manifest(
                {
                    "repository": helper.MODEL_REPOSITORY,
                    "revision": helper.MODEL_REVISION,
                    "files": [changed, *files[1:]],
                }
            )
        source = HELPER.read_text()
        self.assertNotIn("gpu=", source)
        self.assertIn("create_if_missing=False", source)
        self.assertIn("volume.commit()", source)
        self.assertIn('MODEL_ROOT / ".cache"', source)


if __name__ == "__main__":
    unittest.main()
