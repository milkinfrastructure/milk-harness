import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
import urllib.parse
import urllib.request
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
VERIFY_PATH = ROOT / "deploy" / "verify-private-image.py"
sys.path.insert(0, str(VERIFY_PATH.parent))
SPEC = importlib.util.spec_from_file_location("verify_private_image", VERIFY_PATH)
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


class VerifierFixture:
    def __init__(self, root, artifact="student-train"):
        self.root = Path(root)
        self.evidence = self.root / "evidence"
        self.docker_config = self.root / "docker-config"
        self.evidence.mkdir()
        self.docker_config.mkdir()
        self.artifact_name = artifact
        self.artifact = VERIFY.ARTIFACTS[artifact]
        self.repository = self.artifact["repository"]
        self.package = self.repository.rsplit("/", 1)[1]
        self.commit = "1" * 40
        self.context_sha256 = "2" * 64
        self.source_epoch = 1_700_000_000
        self.gateway = (
            VERIFY.GATEWAY_REPOSITORY + "@sha256:" + "9" * 64
            if self.artifact["gateway"]
            else None
        )
        build_log_raw = (
            _json(
                {
                    "schema_version": "milk.content-free-build-log.v1",
                    "artifact": artifact,
                    "exit_code": 0,
                    "started_at": "2026-01-01T00:00:00Z",
                    "completed_at": "2026-01-01T00:01:00Z",
                    "sha256": "3" * 64,
                    "bytes": 123,
                    "content_retained": False,
                }
            )
            + b"\n"
        )
        (self.evidence / "build-log.json").write_bytes(build_log_raw)
        (self.evidence / "ops-log-reference.json").write_bytes(
            _json(
                {
                    "schema_version": "milk.private-ops-log-reference.v1",
                    "authority": "private-release-evidence",
                    "reference": "build-log.json",
                    "receipt_sha256": hashlib.sha256(build_log_raw).hexdigest(),
                    "immutable": True,
                    "content_retained": False,
                }
            )
            + b"\n"
        )
        self.config = {
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Labels": {
                    "org.opencontainers.image.source": VERIFY.SOURCE_REPOSITORY,
                    "org.opencontainers.image.revision": self.commit,
                }
            },
            "rootfs": {"type": "layers", "diff_ids": []},
        }
        self.slsa_predicate = {
            "buildDefinition": {
                "buildType": VERIFY.BUILDKIT_BUILD_TYPE,
                "externalParameters": {
                    "configSource": {
                        "path": Path(self.artifact["dockerfile"]).name,
                    },
                    "request": {
                        "frontend": "gateway.v0",
                        "args": self._release_args(),
                        "locals": [{"name": "context"}, {"name": "dockerfile"}],
                    },
                },
                "internalParameters": {
                    "builderPlatform": "linux/amd64",
                    "buildConfig": self._build_config(),
                },
                "resolvedDependencies": [
                    {
                        "uri": f"pkg:docker/dependency-{index}",
                        "digest": {"sha256": digest},
                    }
                    for index, digest in enumerate(
                        sorted(self._expected_dependency_sha256())
                    )
                ],
            },
            "runDetails": {
                "builder": {"id": ""},
                "metadata": {
                    "invocationID": "fixture-build",
                    "startedOn": "2026-01-01T00:00:00Z",
                    "finishedOn": "2026-01-01T00:01:00Z",
                    "buildkit_completeness": {
                        "request": True,
                        "resolvedDependencies": False,
                    },
                    "buildkit_metadata": self._buildkit_metadata(),
                },
            },
        }
        self.spdx_predicate = {
            "spdxVersion": "SPDX-2.3",
            "SPDXID": "SPDXRef-DOCUMENT",
            "packages": [
                {
                    "SPDXID": "SPDXRef-Package-image",
                    "name": self.package,
                    "checksums": [
                        {"algorithm": "SHA256", "checksumValue": "4" * 64}
                    ],
                }
            ],
        }
        self._assemble()

    def _release_args(self):
        value = {
            "build-arg:BUILDKIT_SYNTAX": VERIFY.DOCKERFILE_FRONTEND,
            "build-arg:MILK_SOURCE_CONTEXT_SHA256": self.context_sha256,
            "build-arg:SOURCE_DATE_EPOCH": str(self.source_epoch),
            "label:org.opencontainers.image.revision": self.commit,
            "label:org.opencontainers.image.source": VERIFY.SOURCE_REPOSITORY,
            "cmdline": VERIFY.DOCKERFILE_FRONTEND,
            "source": VERIFY.DOCKERFILE_FRONTEND,
        }
        if self.gateway:
            value["build-arg:MILK_GATEWAY_IMAGE"] = self.gateway
        return value

    def _expected_dependency_sha256(self):
        value = set(self.artifact["base_sha256"])
        value.add(VERIFY.DOCKERFILE_FRONTEND.rsplit("@sha256:", 1)[1])
        value.add(VERIFY.SBOM_SCANNER_SHA256)
        if self.gateway:
            value.add(self.gateway.rsplit("@sha256:", 1)[1])
        return value

    @staticmethod
    def _build_config():
        return {
            "llbDefinition": [{"id": "step0", "op": {"Op": {}}}],
            "digestMapping": {"sha256:" + "7" * 64: "step0"},
        }

    def _buildkit_metadata(self):
        dockerfile = ROOT.joinpath(self.artifact["dockerfile"])
        return {
            "source": {
                "infos": [
                    {
                        "filename": dockerfile.name,
                        "language": "Dockerfile",
                        "data": base64.b64encode(dockerfile.read_bytes()).decode(),
                        "llbDefinition": [
                            {"id": "source-step0", "op": {"Op": {}}}
                        ],
                        "digestMapping": {
                            "sha256:" + "8" * 64: "source-step0"
                        },
                    }
                ],
                "locations": {"step0": {}},
            }
        }

    def _assemble(self):
        self.raw_config = _json(self.config)
        self.config_digest = _digest(self.raw_config)
        self.raw_manifest = _json(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": self.config_digest,
                    "size": len(self.raw_config),
                },
                "layers": [],
            }
        )
        self.manifest_digest = _digest(self.raw_manifest)
        subject = [
            {
                "name": self.repository,
                "digest": {
                    "sha256": self.manifest_digest.removeprefix("sha256:")
                },
            }
        ]
        self.raw_slsa = _json(
            {
                "_type": "https://in-toto.io/Statement/v0.1",
                "subject": subject,
                "predicateType": VERIFY.SLSA_V1,
                "predicate": self.slsa_predicate,
            }
        )
        self.raw_spdx = _json(
            {
                "_type": "https://in-toto.io/Statement/v0.1",
                "subject": subject,
                "predicateType": VERIFY.SPDX,
                "predicate": self.spdx_predicate,
            }
        )
        self.slsa_digest = _digest(self.raw_slsa)
        self.spdx_digest = _digest(self.raw_spdx)
        self.raw_attestation_config = _json(
            {
                "architecture": "unknown",
                "config": {},
                "os": "unknown",
                "rootfs": {
                    "diff_ids": [self.slsa_digest, self.spdx_digest],
                    "type": "layers",
                },
            }
        )
        self.attestation_config_digest = _digest(self.raw_attestation_config)
        self.raw_attestation = _json(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": self.attestation_config_digest,
                    "size": len(self.raw_attestation_config),
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.in-toto+json",
                        "digest": self.slsa_digest,
                        "size": len(self.raw_slsa),
                        "annotations": {
                            "in-toto.io/predicate-type": VERIFY.SLSA_V1
                        },
                    },
                    {
                        "mediaType": "application/vnd.in-toto+json",
                        "digest": self.spdx_digest,
                        "size": len(self.raw_spdx),
                        "annotations": {"in-toto.io/predicate-type": VERIFY.SPDX},
                    },
                ],
            }
        )
        self.attestation_digest = _digest(self.raw_attestation)
        self.raw_index = _json(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [
                    {
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": self.manifest_digest,
                        "size": len(self.raw_manifest),
                        "platform": {"architecture": "amd64", "os": "linux"},
                    },
                    {
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": self.attestation_digest,
                        "size": len(self.raw_attestation),
                        "annotations": {
                            "vnd.docker.reference.type": "attestation-manifest",
                            "vnd.docker.reference.digest": self.manifest_digest,
                        },
                        "platform": {"architecture": "unknown", "os": "unknown"},
                    },
                ],
            }
        )
        self.index_digest = _digest(self.raw_index)
        self.metadata = self.evidence / "metadata.json"
        self.metadata.write_bytes(
            _json({"containerimage.digest": self.index_digest}) + b"\n"
        )
        self.args = argparse.Namespace(
            artifact=self.artifact_name,
            tagged_reference=self.repository + ":source-" + self.commit,
            source_commit=self.commit,
            source_date_epoch=self.source_epoch,
            source_context_sha256=self.context_sha256,
            gateway_image_reference=self.gateway,
            metadata=self.metadata,
            docker_config=self.docker_config,
            evidence_dir=self.evidence,
            registry_token_stdin=True,
        )
        self.blobs = {
            self.config_digest: self.raw_config,
            self.attestation_config_digest: self.raw_attestation_config,
            self.slsa_digest: self.raw_slsa,
            self.spdx_digest: self.raw_spdx,
        }

    def run(self, visibility="private"):
        references = {
            self.repository + "@" + self.index_digest: self.raw_index,
            self.repository + "@" + self.manifest_digest: self.raw_manifest,
            self.repository + "@" + self.attestation_digest: self.raw_attestation,
        }

        def command(*arguments):
            return references[arguments[-1]]

        def blob(_bearer, package, digest):
            if package != self.package:
                raise ValueError("wrong package scope")
            raw = self.blobs[digest]
            if _digest(raw) != digest:
                raise ValueError("fixture blob digest mismatch")
            return raw

        with mock.patch.object(VERIFY, "_run", side_effect=command), mock.patch.object(
            VERIFY, "_registry_bearer", return_value="bounded-bearer"
        ) as bearer, mock.patch.object(
            VERIFY, "_registry_blob", side_effect=blob
        ), mock.patch.object(
            VERIFY.github_rest, "package_visibility", return_value=visibility
        ) as package_visibility:
            result = VERIFY.verify(self.args, "bounded-github-token")
        bearer.assert_called_once_with("bounded-github-token", self.package)
        package_visibility.assert_called_once_with(
            "bounded-github-token", self.package
        )
        return result


class PrivateHarnessVerifierTests(unittest.TestCase):
    def test_all_artifacts_write_content_verified_admissions(self):
        for artifact in VERIFY.ARTIFACTS:
            with self.subTest(artifact=artifact), tempfile.TemporaryDirectory() as directory:
                fixture = VerifierFixture(directory, artifact)
                image_reference, admission_sha256, ops_log_reference_sha256 = (
                    fixture.run()
                )
                admission_raw = (fixture.evidence / "admission.json").read_bytes()
                admission = json.loads(admission_raw)
                receipt = json.loads(
                    (fixture.evidence / "receipt.json").read_bytes()
                )
                self.assertEqual(
                    image_reference, fixture.repository + "@" + fixture.index_digest
                )
                self.assertEqual(
                    admission_sha256, hashlib.sha256(admission_raw).hexdigest()
                )
                self.assertEqual(
                    ops_log_reference_sha256,
                    hashlib.sha256(
                        (fixture.evidence / "ops-log-reference.json").read_bytes()
                    ).hexdigest(),
                )
                self.assertEqual(
                    receipt["ops_log_reference_sha256"],
                    ops_log_reference_sha256,
                )
                self.assertEqual(
                    admission["ops_log_reference_sha256"],
                    ops_log_reference_sha256,
                )
                self.assertEqual(
                    admission["ops_log_reference"],
                    receipt["ops_log_reference"],
                )
                self.assertEqual(admission["artifact"], artifact)
                self.assertEqual(
                    admission["gateway_image_reference"], fixture.gateway
                )
                self.assertEqual(admission["visibility"], "private")
                for name, raw in {
                    "index.json": fixture.raw_index,
                    "amd64-manifest.json": fixture.raw_manifest,
                    "config.json": fixture.raw_config,
                    "attestation-manifest.json": fixture.raw_attestation,
                    "slsa-provenance.json": fixture.raw_slsa,
                    "spdx-sbom.json": fixture.raw_spdx,
                }.items():
                    self.assertEqual((fixture.evidence / name).read_bytes(), raw)

    def test_rejects_unbound_ops_log_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            reference = json.loads(
                (fixture.evidence / "ops-log-reference.json").read_bytes()
            )
            reference["receipt_sha256"] = "f" * 64
            (fixture.evidence / "ops-log-reference.json").write_bytes(
                _json(reference) + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "ops-log reference"):
                fixture.run()

    def test_rejects_descriptor_size_and_blob_digest_mismatches(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            index = json.loads(fixture.raw_index)
            index["manifests"][0]["size"] += 1
            fixture.raw_index = _json(index)
            fixture.index_digest = _digest(fixture.raw_index)
            fixture.metadata.write_bytes(
                _json({"containerimage.digest": fixture.index_digest}) + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "differs from its descriptor"):
                fixture.run()
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            fixture.blobs[fixture.config_digest] += b" "
            with self.assertRaisesRegex(ValueError, "blob digest mismatch"):
                fixture.run()
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            attestation = json.loads(fixture.raw_attestation)
            attestation["config"]["size"] = 3
            fixture.raw_attestation = _json(attestation)
            old_digest = fixture.attestation_digest
            fixture.attestation_digest = _digest(fixture.raw_attestation)
            index = json.loads(fixture.raw_index)
            descriptor = index["manifests"][1]
            self.assertEqual(descriptor["digest"], old_digest)
            descriptor["digest"] = fixture.attestation_digest
            descriptor["size"] = len(fixture.raw_attestation)
            fixture.raw_index = _json(index)
            fixture.index_digest = _digest(fixture.raw_index)
            fixture.metadata.write_bytes(
                _json({"containerimage.digest": fixture.index_digest}) + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "size differs"):
                fixture.run()

    def test_rejects_wrong_runtime_platform_source_revision_and_public_package(self):
        for field, value, message in (
            ("architecture", "arm64", "platform"),
            ("source", "https://github.com/example/repo", "source label"),
            ("revision", "0" * 40, "revision label"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                fixture = VerifierFixture(directory)
                if field == "architecture":
                    fixture.config[field] = value
                else:
                    fixture.config["config"]["Labels"][
                        "org.opencontainers.image." + field
                    ] = value
                fixture._assemble()
                with self.assertRaisesRegex(ValueError, message):
                    fixture.run()
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            with self.assertRaisesRegex(ValueError, "not private"):
                fixture.run("public")

    def test_slsa_rejects_wrong_args_extra_args_and_missing_base_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            VERIFY._validate_slsa(fixture.slsa_predicate, fixture.args, fixture.artifact)
            request = fixture.slsa_predicate["buildDefinition"]["externalParameters"][
                "request"
            ]
            arguments = request["args"]
            arguments["build-arg:MILK_GATEWAY_IMAGE"] = (
                VERIFY.GATEWAY_REPOSITORY + "@sha256:" + "8" * 64
            )
            with self.assertRaisesRegex(ValueError, "bind release inputs"):
                VERIFY._validate_slsa(
                    fixture.slsa_predicate, fixture.args, fixture.artifact
                )
            arguments["build-arg:MILK_GATEWAY_IMAGE"] = fixture.gateway
            arguments["build-arg:UNDECLARED_SECRET"] = "value"
            with self.assertRaisesRegex(ValueError, "bind release inputs"):
                VERIFY._validate_slsa(
                    fixture.slsa_predicate, fixture.args, fixture.artifact
                )
            del arguments["build-arg:UNDECLARED_SECRET"]
            dependencies = fixture.slsa_predicate["buildDefinition"][
                "resolvedDependencies"
            ]
            required = next(iter(fixture.artifact["base_sha256"]))
            dependencies[:] = [
                item for item in dependencies if item["digest"]["sha256"] != required
            ]
            with self.assertRaisesRegex(ValueError, "every base image"):
                VERIFY._validate_slsa(
                    fixture.slsa_predicate, fixture.args, fixture.artifact
                )
            request["locals"] = [
                {"name": {"not": "text"}},
                {"name": "dockerfile"},
            ]
            with self.assertRaisesRegex(ValueError, "local inputs"):
                VERIFY._validate_slsa(
                    fixture.slsa_predicate, fixture.args, fixture.artifact
                )

    def test_slsa_requires_buildkit_v0232_v1_wire_contract_and_max_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            mutations = (
                (
                    "legacy build type",
                    lambda value: value["buildDefinition"].__setitem__(
                        "buildType", "https://mobyproject.org/buildkit@v1"
                    ),
                    "build type",
                ),
                (
                    "legacy external parameters",
                    lambda value: value["buildDefinition"].__setitem__(
                        "externalParameters",
                        {
                            "source": None,
                            "frontend": "dockerfile.v0",
                            "args": fixture._release_args(),
                            "locals": [
                                {"name": "context"},
                                {"name": "dockerfile"},
                            ],
                        },
                    ),
                    "external parameters",
                ),
                (
                    "lowercase invocation key",
                    lambda value: value["runDetails"]["metadata"].__setitem__(
                        "invocationId",
                        value["runDetails"]["metadata"].pop("invocationID"),
                    ),
                    "run details",
                ),
                (
                    "missing max build config",
                    lambda value: value["buildDefinition"][
                        "internalParameters"
                    ].pop("buildConfig"),
                    "internal parameters",
                ),
                (
                    "incomplete max request",
                    lambda value: value["runDetails"]["metadata"][
                        "buildkit_completeness"
                    ].__setitem__("request", False),
                    "completeness",
                ),
            )
            for label, mutate, message in mutations:
                with self.subTest(label=label):
                    predicate = json.loads(json.dumps(fixture.slsa_predicate))
                    mutate(predicate)
                    with self.assertRaisesRegex(ValueError, message):
                        VERIFY._validate_slsa(
                            predicate, fixture.args, fixture.artifact
                        )

    def test_spdx_rejects_duplicate_and_unchecked_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = VerifierFixture(directory)
            VERIFY._validate_spdx(fixture.spdx_predicate)
            fixture.spdx_predicate["packages"].append(
                dict(fixture.spdx_predicate["packages"][0])
            )
            with self.assertRaisesRegex(ValueError, "package identity"):
                VERIFY._validate_spdx(fixture.spdx_predicate)
            fixture.spdx_predicate["packages"].pop()
            fixture.spdx_predicate["packages"][0]["checksums"] = []
            with self.assertRaisesRegex(ValueError, "no image-bound"):
                VERIFY._validate_spdx(fixture.spdx_predicate)

    def test_in_toto_subject_must_be_exactly_the_child(self):
        child = "sha256:" + "a" * 64
        valid = {
            "subject": [
                {"name": "image", "digest": {"sha256": child.removeprefix("sha256:")}}
            ]
        }
        self.assertTrue(VERIFY._subject_binds_exactly(valid, child))
        for subject in (
            [],
            valid["subject"] * 2,
            [{"name": "image", "digest": {"sha256": "b" * 64}}],
            [
                {
                    "name": "image",
                    "digest": {"sha256": child.removeprefix("sha256:")},
                    "extra": True,
                }
            ],
        ):
            with self.subTest(subject=subject):
                self.assertFalse(
                    VERIFY._subject_binds_exactly({"subject": subject}, child)
                )

    def test_ghcr_bearer_request_is_scoped_to_one_private_package(self):
        seen = {}

        def open_bytes(request, _limit, _label):
            seen["url"] = request.full_url
            seen["authorization"] = request.get_header("Authorization")
            return b'{"token":"registry-bearer"}'

        with mock.patch.object(VERIFY, "_open_bytes", side_effect=open_bytes):
            self.assertEqual(
                VERIFY._registry_bearer("github-secret", "milk-student-train"),
                "registry-bearer",
            )
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(seen["url"]).query)
        self.assertEqual(
            query["scope"],
            ["repository:milkinfrastructure/milk-student-train:pull"],
        )
        decoded = base64.b64decode(
            seen["authorization"].removeprefix("Basic ")
        ).decode()
        self.assertEqual(decoded, VERIFY.GITHUB_USERNAME + ":github-secret")
        self.assertNotIn("github-secret", seen["url"])

    def test_redirect_never_sends_credentials_outside_https(self):
        handler = VERIFY._SafeRedirect()
        request = urllib.request.Request(
            "https://ghcr.io/source",
            headers={"Authorization": "Bearer registry-secret"},
        )
        with self.assertRaisesRegex(ValueError, "left HTTPS"):
            handler.redirect_request(
                request, None, 302, "Found", {}, "http://ghcr.io/target"
            )
        redirected = handler.redirect_request(
            request, None, 302, "Found", {}, "https://objects.example/target"
        )
        self.assertIsNotNone(redirected)
        self.assertIsNone(redirected.get_header("Authorization"))

    def test_gateway_binding_is_required_only_for_gpu_images(self):
        with tempfile.TemporaryDirectory() as directory:
            student_train = VerifierFixture(directory, "student-train")
            student_train.args.gateway_image_reference = None
            with self.assertRaisesRegex(ValueError, "gateway image"):
                VERIFY._validate_release_arguments(student_train.args)
        with tempfile.TemporaryDirectory() as directory:
            jobs = VerifierFixture(directory, "jobs")
            jobs.args.gateway_image_reference = (
                VERIFY.GATEWAY_REPOSITORY + "@sha256:" + "9" * 64
            )
            with self.assertRaisesRegex(ValueError, "must not carry"):
                VERIFY._validate_release_arguments(jobs.args)

    def test_artifact_base_dependency_contract_matches_every_pinned_from(self):
        dockerfiles = {
            "student-train": "deploy/student-train/Dockerfile",
            "student-branch": "deploy/student-branch/Dockerfile",
            "teacher-gpt-oss": "deploy/teacher/gpt-oss-120b/Dockerfile",
            "jobs": "Dockerfile.jobs",
        }
        for artifact, relative in dockerfiles.items():
            with self.subTest(artifact=artifact):
                raw = ROOT.joinpath(relative).read_text(encoding="utf-8")
                pinned = set(
                    re.findall(r"^FROM [^\n]*@sha256:([0-9a-f]{64})(?:\s|$)", raw, re.M)
                )
                self.assertEqual(pinned, VERIFY.ARTIFACTS[artifact]["base_sha256"])

    def test_build_script_streams_registry_credentials_only_on_stdin(self):
        script = ROOT.joinpath("deploy/build-images.sh").read_text(encoding="utf-8")
        self.assertIn('"$python" "$@" <"$github_token_file"', script)
        self.assertIn('<"$github_token_file" >/dev/null', script)
        self.assertIn('"$repo/deploy/verify-private-image.py"', script)
        self.assertNotRegex(script, r"(?:token|password)=\$\(")
        self.assertNotRegex(script, r"\bgh\b")


if __name__ == "__main__":
    unittest.main()
