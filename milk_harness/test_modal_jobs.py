import copy
import datetime as dt
import gzip
import hashlib
import json
import tempfile
import types
import unittest
from unittest import mock

import milk_harness.modal_jobs as modal_jobs
from milk_harness.evidence import LocalEvidenceStore, canonical_json
from milk_harness.modal_jobs import (
    MAX_TERMINATE_ATTEMPTS,
    ModalJobs,
    acceptance_key,
    definition_key,
    sandbox_name,
    sandbox_tags,
    validate_acceptance,
    validate_definition,
)
from milk_harness.provider_acceptance import (
    encode as encode_provider_acceptance,
    sha256 as provider_acceptance_sha256,
)


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 27, 20, 0, 30, tzinfo=UTC)
COMMAND = ["/opt/milk/deploy/student-job.sh", "run"]
ENVIRONMENT = {"MILK_RUN_ID": "b" * 64}
WINNER_COMMAND = ["/bin/sh", "-c", "exec /opt/dragontales/deploy/student-job.sh serve"]
WINNER_ENVIRONMENT = {
    "DRAGONTALES_MODEL_ALIAS": "milk-student",
    "DRAGONTALES_STUDENT_JOB_ID": "b" * 64,
}
VARIANTS = ("bf16", "dynamic_fp8", "static_fp8")


def digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def provider_selection():
    return {
        "selected_provider": "modal",
        "provider_identity": {
            "provider": "modal",
            "workspace_id": "ws-production",
            "workspace_name": "milk-production",
            "environment_id": "en-main",
            "environment_name": "main",
            "app_id": "ap-milk-gpu-jobs",
            "app_name": "milk-gpu-jobs",
        },
        "primary_preflight": {
            "provider": "baseten",
            "outcome": "retryable_unavailable",
            "reason": "server_unavailable",
            "status": 503,
            "evidence_sha256": "8" * 64,
            "observed_at": "2026-08-27T19:58:00Z",
        },
        "fallback_preflight": {
            "provider": "modal",
            "outcome": "ready",
            "evidence_sha256": "9" * 64,
            "observed_at": "2026-08-27T19:59:00Z",
        },
    }


def gpu_operation(kind):
    if kind == "teacher_run":
        return {
            "kind": kind,
            "teacher_run_id": "a" * 64,
            "provider_binding_sha256": "7" * 64,
            "slot": 0,
            "call_count": 12,
            "max_gpu_seconds": 3_600,
        }
    if kind == "student_train_merge":
        return {
            "kind": kind,
            "student_job_id": "b" * 64,
            "counts": {"train": 50, "dev": 73, "calibration": 128},
            "max_gpu_seconds": 1_800,
        }
    if kind == "student_fanout":
        return {
            "kind": kind,
            "student_job_id": "b" * 64,
            "max_total_gpu_seconds": 7_200,
            "branches": [
                {
                    "schema_version": "dragontales.student-branch-claim.v1",
                    "branch_id": character * 64,
                    "variant": variant,
                    "max_gpu_seconds": 1_800,
                }
                for character, variant in zip("123", VARIANTS)
            ],
        }
    raise AssertionError(kind)


def acceptance(kind="student_train_merge"):
    value = {
        "schema_version": "milk.gpu-provider-acceptance.v1",
        "campaign_id": "a" * 64,
        "run_id": "b" * 64,
        "claim_sha256": "c" * 64,
        "outbox_sha256": "d" * 64,
        "operation": gpu_operation(kind),
        "selection": provider_selection(),
        "image_release_sha256": "e" * 64,
        "image_admission_sha256": "f" * 64,
        "provider_pass_claim_sha256": "1" * 64,
        "create_authorization_sha256": "2" * 64,
        "budget_reservation_sha256": "3" * 64,
        "reserved_microusd": 10_000_000,
        "reserved_at": "2026-08-27T19:59:30Z",
        "accepted_at": "2026-08-27T20:00:00Z",
        "create_not_after": "2026-08-27T20:01:00Z",
        "provider_not_after": (
            "2026-08-27T21:00:00Z"
            if kind == "teacher_run"
            else "2026-08-27T20:30:00Z"
        ),
        "max_wall_seconds": 3_600 if kind == "teacher_run" else 1_800,
        "max_cost_microusd": 10_000_000,
        "state": "accepted",
    }
    return value


def winner_acceptance():
    gpu = acceptance()
    return {
        "schema_version": "milk.winner-provider-acceptance.v1",
        "campaign_id": gpu["campaign_id"],
        "run_id": gpu["run_id"],
        "claim_sha256": gpu["claim_sha256"],
        "outbox_sha256": gpu["outbox_sha256"],
        "provider_binding_sha256": "7" * 64,
        "selection": gpu["selection"],
        "image_release_sha256": gpu["image_release_sha256"],
        "image_admission_sha256": gpu["image_admission_sha256"],
        "provider_pass_claim_sha256": gpu["provider_pass_claim_sha256"],
        "create_authorization_sha256": gpu["create_authorization_sha256"],
        "budget_reservation_sha256": gpu["budget_reservation_sha256"],
        "reserved_microusd": gpu["reserved_microusd"],
        "reserved_at": gpu["reserved_at"],
        "accepted_at": gpu["accepted_at"],
        "create_not_after": gpu["create_not_after"],
        "provider_not_after": gpu["provider_not_after"],
        "max_wall_seconds": gpu["max_wall_seconds"],
        "max_cost_microusd": gpu["max_cost_microusd"],
        "state": gpu["state"],
    }


def named_secret(name, object_id, keys):
    return {"name": name, "object_id": object_id, "required_keys": sorted(keys)}


def definition(value, ordinal=0):
    schema = value["schema_version"]
    operation = value.get("operation")
    if schema == "milk.winner-provider-acceptance.v1":
        unit = {
            "kind": "student_winner_deployment",
            "student_job_id": "b" * 64,
            "student_result_sha256": "6" * 64,
            "winner": "static_fp8",
        }
    elif operation["kind"] == "student_fanout":
        branch = operation["branches"][ordinal]
        unit = {
            "kind": "student_branch",
            "student_job_id": operation["student_job_id"],
            "branch_id": branch["branch_id"],
            "variant": branch["variant"],
            "max_gpu_seconds": branch["max_gpu_seconds"],
        }
    else:
        unit = copy.deepcopy(operation)
    is_teacher = unit["kind"] == "teacher_run"
    return {
        "schema_version": "milk.modal-execution-definition.v1",
        "campaign_id": value["campaign_id"],
        "run_id": value["run_id"],
        "ordinal": ordinal,
        "provider_acceptance_sha256": provider_acceptance_sha256(value),
        "unit": unit,
        "runtime_image_reference": (
            "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss@sha256:"
            if is_teacher
            else "ghcr.io/milkinfrastructure/milk-student@sha256:"
        )
        + "4" * 64,
        "sandbox": {
            "cpu_physical_cores": 8,
            "memory_mib": 65_536,
            "timeout_seconds": 3_540 if is_teacher else 1_740,
            "workdir": "/opt/milk/deploy",
            "command_sha256": digest(COMMAND),
            "environment_sha256": digest(ENVIRONMENT),
        },
        "resources": {
            "registry_secret": named_secret(
                "milk-registry",
                "st-registry",
                ["REGISTRY_USERNAME", "REGISTRY_PASSWORD"],
            ),
            "secrets": [
                named_secret("milk-job-config", "st-job-config", ["MILK_CONFIG_JSON"])
            ],
            "volumes": (
                [
                    {
                        "name": "milk-teacher-model",
                        "object_id": "vo-teacher-model",
                        "mount_path": "/models/gpt-oss-120b",
                        "read_only": True,
                    }
                ]
                if is_teacher
                else []
            ),
        },
    }


def winner_fixture():
    value = winner_acceptance()
    execution = definition(value)
    execution["sandbox"]["command_sha256"] = digest(WINNER_COMMAND)
    execution["sandbox"]["environment_sha256"] = digest(WINNER_ENVIRONMENT)
    execution["resources"]["secrets"].append(
        named_secret(
            "milk-winner-candidate",
            "st-winner-candidate",
            [modal_jobs.WINNER_API_KEY_ENVIRONMENT],
        )
    )
    authority = {
        "schema_version": "dragontales.winner-deployment-authority.v1",
        "provider_policy": {"primary": "baseten", "fallback": "modal"},
        "provider_terms_sha256": "1" * 64,
        "runtime_image_reference": execution["runtime_image_reference"],
        "admission_program_sha256": hashlib.sha256(
            modal_jobs.winner_contract.ADMISSION_PATH.read_bytes()
        ).hexdigest(),
        "authorization_not_after": "2026-08-27T20:30:00Z",
        "max_wall_seconds": value["max_wall_seconds"],
        "max_cost_microusd": value["max_cost_microusd"],
        "allow_private_candidate_http": False,
        "signing_public_key_hex": "2" * 64,
        "signing_key_id": "winner-test-key",
        "candidate_max_in_flight": 1,
        "canary_candidate_basis_points": 100,
        "canary_valid_for_seconds": 900,
        "max_input_utf8_bytes": 2_048,
        "max_input_messages": 16,
        "max_input_request_bytes": 16_384,
        "route_schema_version": "dragontales.route.v3",
        "winner_admission_schema_version": (
            "dragontales.winner-admission-receipt.v1"
        ),
    }
    binding = hashlib.sha256(
        b"dragontales.winner-provider-binding.v1\0"
        + json.dumps(authority, separators=(",", ":")).encode()
    ).hexdigest()
    claim = {
        "schema_version": "dragontales.student-winner-deployment-claim.v1",
        "scope": {
            "tenant_id": "11111111-1111-4111-8111-111111111111",
            "project_id": "22222222-2222-4222-8222-222222222222",
            "environment_id": "33333333-3333-4333-8333-333333333333",
            "workload_id": "44444444-4444-4444-8444-444444444444",
        },
        "student_job_id": execution["unit"]["student_job_id"],
        "student_claim_sha256": "3" * 64,
        "student_result_sha256": execution["unit"]["student_result_sha256"],
        "winner": execution["unit"]["winner"],
        "model_manifest": {
            "object_key": "dt/v2/model-manifest",
            "sha256": "a" * 64,
            "bytes": 1,
        },
        "dev_receipt": {
            "object_key": "dt/v2/dev-receipt",
            "sha256": "d" * 64,
            "bytes": 1,
        },
        "runtime_image_reference": execution["runtime_image_reference"],
        "provider_binding_sha256": binding,
        "authority": authority,
        "claimed_at": "2026-08-27T19:55:00Z",
        "expires_at": "2026-08-27T20:30:00Z",
    }
    claim_raw = json.dumps(claim, separators=(",", ":")).encode()
    value["claim_sha256"] = hashlib.sha256(claim_raw).hexdigest()
    value["provider_binding_sha256"] = binding
    execution["provider_acceptance_sha256"] = provider_acceptance_sha256(value)
    materialization = {
        "schema_version": modal_jobs.winner_contract.MATERIALIZATION_SCHEMA,
        "student_job_id": execution["unit"]["student_job_id"],
        "variant": execution["unit"]["winner"],
        "model_manifest_sha256": claim["model_manifest"]["sha256"],
        "file_count": 1,
        "artifact_bytes": 1,
        "stage_path": "/tmp/dragontales-winner",
        "model_path": "/tmp/dragontales-winner/model",
        "model_manifest_path": "/tmp/dragontales-winner/model-manifest.json",
    }
    probe = {
        "schema_version": modal_jobs.winner_contract.PROBE_SCHEMA,
        "student_job_id": execution["unit"]["student_job_id"],
        "student_variant": execution["unit"]["winner"],
        "model_manifest_sha256": claim["model_manifest"]["sha256"],
        "model_alias_sha256": hashlib.sha256(
            WINNER_ENVIRONMENT["DRAGONTALES_MODEL_ALIAS"].encode()
        ).hexdigest(),
        "candidate_api_key_sha256": "4" * 64,
        "models_response_sha256": "5" * 64,
        "chat_request_sha256": "6" * 64,
        "chat_response_sha256": "7" * 64,
    }
    return value, execution, claim, claim_raw, materialization, probe


def route_receipts(claim):
    scope_prefix = "dt/v2/" + "/".join(claim["scope"].values())

    def receipt(revision, basis_points, previous):
        value = {
            "schema_version": "dragontales.route-publication-receipt.v2",
            "route_revision": revision,
            "student_job_id": claim["student_job_id"],
            "student_result_sha256": claim["student_result_sha256"],
            "model_manifest_sha256": claim["model_manifest"]["sha256"],
            "dev_receipt_sha256": claim["dev_receipt"]["sha256"],
            "previous_route_revision": previous,
            "candidate_basis_points": basis_points,
            "manifest_object_key": f"{scope_prefix}/routes/manifests/{revision}.json",
            "signature_object_key": (
                f"{scope_prefix}/routes/signatures/{revision}/{'9' * 64}.ed25519"
            ),
            "live_pointer_object_key": f"{scope_prefix}/routes/live.json",
            "state": "active",
        }
        return (json.dumps(value, separators=(",", ":")) + "\n").encode()

    canary_revision = "e" * 64
    return receipt(canary_revision, 100, None), receipt(
        "f" * 64,
        0,
        canary_revision,
    )


class NotFoundError(Exception):
    pass


class LogEntry:
    def __init__(self, message, timestamp=NOW, source="stdout"):
        self.message = message
        self.timestamp = timestamp
        self.source = source
        self.object_id = "sb-execution"
        self.context_ids = ["sb-execution"]


class Handle:
    def __init__(self, object_id, name=None, calls=None):
        self.object_id = object_id
        self.name = name
        self.calls = calls

    def hydrate(self):
        self.calls.append(("hydrate", self.object_id))


class ImageHandle:
    def __init__(self, calls):
        self.calls = calls

    def entrypoint(self, value):
        self.calls.append(("entrypoint", value))
        return self


class VolumeHandle(Handle):
    def with_mount_options(self, **kwargs):
        self.calls.append(("mount_options", kwargs))
        return self


class SandboxHandle:
    def __init__(self, calls, *, tags, name, owner=None, object_id="sb-execution"):
        self.calls = calls
        self.tags = tags
        self.name = name
        self.owner = owner
        self.object_id = object_id
        self.exit_code = None
        self.terminate_error = None

    def get_tags(self):
        self.calls.append(("get_tags", self.object_id))
        return self.tags

    def poll(self):
        self.calls.append(("poll", self.object_id))
        return self.exit_code

    def detach(self):
        self.calls.append(("detach", self.object_id))

    def terminate(self, **kwargs):
        self.calls.append(("terminate", kwargs))
        if self.terminate_error is not None:
            raise self.terminate_error
        self.exit_code = 143

    def wait_until_ready(self, **kwargs):
        self.calls.append(("wait_until_ready", kwargs))

    def tunnels(self, **kwargs):
        self.calls.append(("tunnels", kwargs))
        return self.owner.tunnels

    def exec(self, *command, **kwargs):
        self.calls.append(("exec", command, kwargs))
        return Process(
            self.owner.probe_stdout,
            self.owner.probe_stderr,
            self.owner.probe_exit_code,
        )


class Stream:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class Process:
    def __init__(self, stdout, stderr=b"", returncode=0):
        self.stdout = Stream(stdout)
        self.stderr = Stream(stderr)
        self.returncode = returncode

    def wait(self):
        return self.returncode


class Tunnel:
    def __init__(self, host="unit.w.modal.host", port=443):
        self.host = host
        self.port = port
        self.unencrypted_host = ""
        self.unencrypted_port = 0
        self.url = f"https://{host}"
        self.tls_socket = (host, port)


class FakeModal:
    __version__ = "1.5.4"
    exception = types.SimpleNamespace(NotFoundError=NotFoundError)
    types = types.SimpleNamespace(LogEntry=LogEntry)
    Tunnel = Tunnel

    def __init__(self, acceptance_value, definition_value, store):
        self.value = modal_jobs._normalized(acceptance_value, definition_value)
        self.store = store
        self.calls = []
        self.created = None
        self.create_error = None
        self.logs = []
        self.probe_stdout = b""
        self.probe_stderr = b""
        self.probe_exit_code = 0
        self.tunnels = {8000: Tunnel()}
        outer = self
        identity = self.value["provider_identity"]
        resources = self.value["resources"]

        class Workspace:
            @staticmethod
            def from_context():
                outer.calls.append(("workspace",))
                return Handle(
                    identity["workspace_id"], identity["workspace_name"], outer.calls
                )

        class Environment:
            @staticmethod
            def from_name(name, **kwargs):
                outer.calls.append(("environment", name, kwargs))
                return Handle(identity["environment_id"], name, outer.calls)

        class Logs:
            def fetch(self, **kwargs):
                outer.calls.append(("logs", kwargs))
                return iter(outer.logs)

        class App:
            @staticmethod
            def lookup(name, **kwargs):
                outer.calls.append(("app", name, kwargs))
                handle = Handle(identity["app_id"], name, outer.calls)
                handle.logs = Logs()
                return handle

        class Secret:
            @staticmethod
            def from_name(name, **kwargs):
                outer.calls.append(("secret", name, kwargs))
                specs = [resources["registry_secret"], *resources["secrets"]]
                spec = next(item for item in specs if item["name"] == name)
                return Handle(spec["object_id"], calls=outer.calls)

        class Image:
            @staticmethod
            def from_registry(reference, **kwargs):
                outer.calls.append(("image", reference, kwargs))
                return ImageHandle(outer.calls)

        class Volume:
            @staticmethod
            def from_name(name, **kwargs):
                outer.calls.append(("volume", name, kwargs))
                spec = next(item for item in resources["volumes"] if item["name"] == name)
                return VolumeHandle(spec["object_id"], calls=outer.calls)

        class Sandbox:
            @staticmethod
            def create(*command, **kwargs):
                outer.calls.append(("create", command, kwargs))
                prefix = (
                    f"campaigns/v1/{outer.value['campaign_id']}/jobs/"
                    f"{outer.value['run_id']}/modal/create-intents/"
                    f"{outer.value['ordinal']:02d}.json"
                )
                store.get(prefix)
                if outer.create_error is not None:
                    raise outer.create_error
                outer.created = SandboxHandle(
                    outer.calls,
                    tags=kwargs["tags"],
                    name=kwargs["name"],
                    owner=outer,
                )
                return outer.created

            @staticmethod
            def from_name(app_name, name, **kwargs):
                outer.calls.append(("from_name", app_name, name, kwargs))
                if outer.created is None:
                    raise NotFoundError(name)
                return outer.created

            @staticmethod
            def list(**kwargs):
                outer.calls.append(("list", kwargs))
                return [] if outer.created is None else [outer.created]

        class Probe:
            @staticmethod
            def with_tcp(port):
                outer.calls.append(("probe", port))
                return ("tcp", port)

        self.Workspace = Workspace
        self.Environment = Environment
        self.App = App
        self.Secret = Secret
        self.Image = Image
        self.Volume = Volume
        self.Sandbox = Sandbox
        self.Probe = Probe


class ModalJobsTests(unittest.TestCase):
    def setup(self, root, value=None, execution=None):
        value = acceptance() if value is None else value
        execution = definition(value) if execution is None else execution
        store = LocalEvidenceStore(root)
        store.create(
            acceptance_key(value),
            encode_provider_acceptance(value),
            "application/json",
        )
        store.create(
            definition_key(value, execution), canonical_json(execution), "application/json"
        )
        modal = FakeModal(value, execution, store)
        guards = []
        authority = {
            "provider_pass_claim_sha256": value["provider_pass_claim_sha256"],
            "create_authorization_sha256": value["create_authorization_sha256"],
        }

        def guard():
            guards.append("guard")
            return authority

        jobs = ModalJobs(
            store=store,
            request_guard=guard,
            modal_sdk=modal,
            now=lambda: NOW,
        )
        return value, execution, store, modal, guards, jobs

    def admit_winner(self, root):
        (
            value,
            execution,
            claim,
            claim_raw,
            materialization,
            probe,
        ) = winner_fixture()
        value, execution, store, modal, guards, jobs = self.setup(
            root,
            value,
            execution,
        )
        jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
        modal.logs = [
            LogEntry(json.dumps(materialization, separators=(",", ":")) + "\n")
        ]
        modal.probe_stdout = (
            json.dumps(probe, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()
        admitted = jobs.admit_winner(
            value,
            execution,
            WINNER_COMMAND,
            WINNER_ENVIRONMENT,
            gateway_claim_raw=claim_raw,
        )
        return (
            value,
            execution,
            claim,
            claim_raw,
            store,
            modal,
            guards,
            jobs,
            admitted,
        )

    def test_teacher_train_fanout_and_winner_acceptances_are_disjoint(self):
        for kind in ("teacher_run", "student_train_merge"):
            value = acceptance(kind)
            execution = definition(value)
            self.assertIs(validate_acceptance(value), value)
            self.assertIs(validate_definition(value, execution), execution)
        fanout = acceptance("student_fanout")
        for ordinal in range(3):
            validate_definition(fanout, definition(fanout, ordinal))
        winner = winner_acceptance()
        validate_definition(winner, definition(winner))
        self.assertNotIn("operation", winner)
        self.assertNotIn("provider_binding_sha256", fanout)

    def test_provider_acceptance_wire_is_ordered_compact_and_line_terminated(self):
        for value in (acceptance(), winner_acceptance()):
            raw = encode_provider_acceptance(value)
            self.assertEqual(raw[-1:], b"\n")
            self.assertFalse(raw.endswith(b"\n\n"))
            self.assertNotIn(b": ", raw)
            self.assertEqual(tuple(json.loads(raw)), tuple(value))
            self.assertEqual(hashlib.sha256(raw).hexdigest(), provider_acceptance_sha256(value))
            reordered = {key: value[key] for key in reversed(value)}
            with self.assertRaisesRegex(ValueError, "order"):
                validate_acceptance(reordered)

    def test_create_is_intent_first_exact_and_never_replayed(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, guards, jobs = self.setup(root)
            result = jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(result["state"], "created")
            create = next(call for call in modal.calls if call[0] == "create")
            kwargs = create[2]
            self.assertEqual(create[1], tuple(COMMAND))
            self.assertEqual(kwargs["gpu"], "H100!")
            self.assertEqual(kwargs["cpu"], 8)
            self.assertEqual(kwargs["memory"], 65_536)
            self.assertEqual(kwargs["timeout"], 1_740)
            self.assertEqual(kwargs["name"], sandbox_name(value, execution))
            self.assertEqual(kwargs["tags"], sandbox_tags(value, execution))
            self.assertTrue(guards)
            recovered = jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(recovered["state"], "active")
            self.assertEqual(len([call for call in modal.calls if call[0] == "create"]), 1)

    def test_modal_fallback_never_cross_provider_replays(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            modal.create_error = TimeoutError("provider included secret in error")
            with self.assertRaises(TimeoutError):
                jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            modal.create_error = None
            result = jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(result["state"], "ambiguous_not_observed")
            self.assertEqual(len([call for call in modal.calls if call[0] == "create"]), 1)
            invalid = copy.deepcopy(value)
            invalid["selection"]["selected_provider"] = "baseten"
            with self.assertRaises(ValueError):
                validate_acceptance(invalid)

    def test_winner_launch_has_only_its_accepted_port_and_probe(self):
        with tempfile.TemporaryDirectory() as root:
            value = winner_acceptance()
            execution = definition(value)
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            create = next(call for call in modal.calls if call[0] == "create")
            self.assertEqual(create[2]["encrypted_ports"], [8000])
            self.assertEqual(create[2]["readiness_probe"], ("tcp", 8000))
            self.assertIn(("probe", 8000), modal.calls)

    def test_winner_admission_is_exact_authenticated_bounded_and_canonical(self):
        with tempfile.TemporaryDirectory() as root:
            (
                value,
                execution,
                unused_claim,
                claim_raw,
                store,
                modal,
                guards,
                jobs,
                admitted,
            ) = self.admit_winner(root)
            self.assertEqual(admitted["state"], "admitted")
            self.assertEqual(
                admitted["service_not_after"],
                "2026-08-27T20:29:30Z",
            )
            self.assertIn(
                ("wait_until_ready", {"timeout": 300}),
                modal.calls,
            )
            self.assertIn(
                ("tunnels", {"timeout": modal_jobs.WINNER_TUNNEL_TIMEOUT_SECONDS}),
                modal.calls,
            )
            execution_call = next(call for call in modal.calls if call[0] == "exec")
            self.assertEqual(execution_call[1][0:2], ("python3", "-c"))
            self.assertEqual(
                hashlib.sha256(execution_call[1][2].encode()).hexdigest(),
                hashlib.sha256(
                    modal_jobs.winner_contract.ADMISSION_PATH.read_bytes()
                ).hexdigest(),
            )
            self.assertEqual(execution_call[2]["timeout"], 120)
            self.assertFalse(execution_call[2]["text"])
            self.assertEqual(
                [secret.object_id for secret in execution_call[2]["secrets"]],
                ["st-winner-candidate"],
            )
            result_key = (
                f"campaigns/v1/{value['campaign_id']}/winner-jobs/"
                f"{value['run_id']}/gateway-result.json"
            )
            result_raw = store.get(result_key)
            result = json.loads(result_raw)
            self.assertEqual(
                modal_jobs.winner_contract.result_bytes(result),
                result_raw,
            )
            self.assertEqual(result["provider_acceptance"], value)
            self.assertEqual(result["claim_sha256"], hashlib.sha256(claim_raw).hexdigest())
            self.assertEqual(result["admission"]["provider"], "modal")
            self.assertEqual(
                result["admission"]["chat_completions_url"],
                "https://unit.w.modal.host/v1/chat/completions",
            )
            provider_calls = len(modal.calls)
            second = jobs.admit_winner(
                value,
                execution,
                WINNER_COMMAND,
                WINNER_ENVIRONMENT,
                gateway_claim_raw=claim_raw,
            )
            self.assertEqual(second, admitted)
            self.assertEqual(len(modal.calls), provider_calls)
            self.assertGreater(len(guards), 10)

    def test_delayed_winner_admission_keeps_a_full_route_interval(self):
        with tempfile.TemporaryDirectory() as root:
            (
                value,
                execution,
                unused_claim,
                claim_raw,
                materialization,
                probe,
            ) = winner_fixture()
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
            modal.logs = [
                LogEntry(json.dumps(materialization, separators=(",", ":")) + "\n")
            ]
            modal.probe_stdout = (
                json.dumps(probe, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode()
            jobs.now = lambda: NOW + dt.timedelta(minutes=5)

            admitted = jobs.admit_winner(
                value,
                execution,
                WINNER_COMMAND,
                WINNER_ENVIRONMENT,
                gateway_claim_raw=claim_raw,
            )

            self.assertEqual(admitted["service_not_after"], "2026-08-27T20:29:30Z")
            self.assertGreaterEqual(
                dt.datetime.fromisoformat(
                    admitted["service_not_after"].replace("Z", "+00:00")
                )
                - (NOW + dt.timedelta(minutes=5)),
                dt.timedelta(seconds=modal_jobs.WINNER_CANARY_SECONDS),
            )

    def test_winner_admission_reserves_time_for_signed_zero(self):
        with tempfile.TemporaryDirectory() as root:
            (
                value,
                execution,
                unused_claim,
                claim_raw,
                materialization,
                probe,
            ) = winner_fixture()
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
            modal.logs = [
                LogEntry(json.dumps(materialization, separators=(",", ":")) + "\n")
            ]
            modal.probe_stdout = (
                json.dumps(probe, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode()
            jobs.now = lambda: NOW + dt.timedelta(minutes=14)

            with self.assertRaisesRegex(RuntimeError, "canary runway"):
                jobs.admit_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )

    def test_winner_admission_fails_closed_on_tunnel_logs_probe_and_secret(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_claim, claim_raw, materialization, probe = (
                winner_fixture()
            )
            execution["resources"]["secrets"] = [
                execution["resources"]["secrets"][0]
            ]
            value, execution, unused_store, unused_modal, unused_guards, jobs = (
                self.setup(root, value, execution)
            )
            jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
            with self.assertRaisesRegex(ValueError, "application-key secret"):
                jobs.admit_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )

        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_claim, claim_raw, materialization, probe = (
                winner_fixture()
            )
            value, execution, store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
            modal.logs = [
                LogEntry(json.dumps(materialization, separators=(",", ":")) + "\n")
            ]
            modal.probe_stdout = (
                json.dumps(probe, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode()
            modal.tunnels[9000] = Tunnel("other.w.modal.host")
            with self.assertRaisesRegex(ValueError, "tunnel lookup"):
                jobs.admit_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )
            self.assertFalse(any(call[0] == "exec" for call in modal.calls))

            modal.tunnels.pop(9000)
            bad_materialization = dict(materialization)
            bad_materialization["model_manifest_sha256"] = "f" * 64
            modal.logs = [
                LogEntry(
                    json.dumps(bad_materialization, separators=(",", ":")) + "\n"
                )
            ]
            with self.assertRaisesRegex(ValueError, "winner claim"):
                jobs.admit_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )

        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_claim, claim_raw, materialization, probe = (
                winner_fixture()
            )
            value, execution, store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, WINNER_COMMAND, WINNER_ENVIRONMENT)
            modal.logs = [
                LogEntry(json.dumps(materialization, separators=(",", ":")) + "\n")
            ]
            modal.probe_stdout = (
                json.dumps(probe, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode()
            modal.probe_stderr = b"candidate-secret-must-not-persist"
            with self.assertRaisesRegex(RuntimeError, "probe was rejected"):
                jobs.admit_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )
            prefix = f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}/modal"
            for key in store.list(prefix):
                self.assertNotIn(b"candidate-secret", store.get(key))

    def test_winner_teardown_requires_signed_zero_then_verifies_zero_gpu(self):
        with tempfile.TemporaryDirectory() as root:
            (
                value,
                execution,
                claim,
                claim_raw,
                store,
                modal,
                unused_guards,
                jobs,
                unused_admitted,
            ) = self.admit_winner(root)
            jobs.request_guard = lambda: None
            with self.assertRaisesRegex(RuntimeError, "signed zero"):
                jobs.teardown_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                )
            self.assertFalse(any(call[0] == "terminate" for call in modal.calls))
            canary, zero = route_receipts(claim)
            bad_zero = json.loads(zero)
            bad_zero["previous_route_revision"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "signed route"):
                jobs.teardown_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                    canary_route_raw=canary,
                    zero_route_raw=(
                        json.dumps(bad_zero, separators=(",", ":")) + "\n"
                    ).encode(),
                )
            self.assertFalse(any(call[0] == "terminate" for call in modal.calls))
            result = jobs.teardown_winner(
                value,
                execution,
                WINNER_COMMAND,
                WINNER_ENVIRONMENT,
                gateway_claim_raw=claim_raw,
                canary_route_raw=canary,
                zero_route_raw=zero,
            )
            self.assertEqual(result["state"], "zero")
            self.assertEqual(result["teardown_authority"], "signed_zero_route")
            self.assertEqual(result["zero_gpu_verification"]["state"], "zero")
            self.assertEqual(
                store.get(
                    f"campaigns/v1/{value['campaign_id']}/winner-jobs/"
                    f"{value['run_id']}/routes/zero-receipt.json"
                ),
                zero,
            )
            provider_calls = len(modal.calls)
            self.assertEqual(
                jobs.teardown_winner(
                    value,
                    execution,
                    WINNER_COMMAND,
                    WINNER_ENVIRONMENT,
                    gateway_claim_raw=claim_raw,
                ),
                result,
            )
            self.assertEqual(len(modal.calls), provider_calls)

    def test_expired_winner_teardown_needs_no_route_but_still_verifies_zero(self):
        with tempfile.TemporaryDirectory() as root:
            (
                value,
                execution,
                unused_claim,
                claim_raw,
                unused_store,
                unused_modal,
                unused_guards,
                jobs,
                unused_admitted,
            ) = self.admit_winner(root)
            jobs.request_guard = lambda: None
            jobs.now = lambda: NOW + dt.timedelta(minutes=30)
            result = jobs.teardown_winner(
                value,
                execution,
                WINNER_COMMAND,
                WINNER_ENVIRONMENT,
                gateway_claim_raw=claim_raw,
            )
            self.assertEqual(result["state"], "zero")
            self.assertEqual(
                result["teardown_authority"],
                "service_authority_expired",
            )

    def test_exact_existing_teacher_resources_are_read_before_create(self):
        with tempfile.TemporaryDirectory() as root:
            value = acceptance("teacher_run")
            execution = definition(value)
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(
                root, value, execution
            )
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertIn(("environment", "main", {"create_if_missing": False}), modal.calls)
            self.assertIn(
                (
                    "app",
                    "milk-gpu-jobs",
                    {"create_if_missing": False, "environment_name": "main"},
                ),
                modal.calls,
            )
            self.assertIn(
                (
                    "volume",
                    "milk-teacher-model",
                    {"create_if_missing": False, "environment_name": "main"},
                ),
                modal.calls,
            )
            create = next(call for call in modal.calls if call[0] == "create")
            self.assertEqual(set(create[2]["volumes"]), {"/models/gpt-oss-120b"})

    def test_bounds_action_authority_and_inputs_fail_closed(self):
        invalid = acceptance()
        invalid["create_authorization_sha256"] = None
        with self.assertRaises(ValueError):
            validate_acceptance(invalid)
        invalid = acceptance()
        invalid["selection"]["primary_preflight"]["reason"] = "unauthorized"
        with self.assertRaisesRegex(ValueError, "fallback-safe"):
            validate_acceptance(invalid)
        value = acceptance()
        execution = definition(value)
        execution["resources"]["secrets"].append(
            named_secret("duplicate-key", "st-duplicate", ["MILK_CONFIG_JSON"])
        )
        with self.assertRaisesRegex(ValueError, "resource identities"):
            validate_definition(value, execution)
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            with self.assertRaisesRegex(ValueError, "inputs differ"):
                jobs.launch(value, execution, [*COMMAND, "changed"], ENVIRONMENT)
            self.assertFalse(modal.calls)
            with self.assertRaisesRegex(ValueError, "shadows"):
                jobs.launch(
                    value,
                    execution,
                    COMMAND,
                    {**ENVIRONMENT, "MILK_CONFIG_JSON": "not-secret"},
                )
            self.assertFalse(modal.calls)

    def test_create_authority_is_exact_but_reconcile_uses_the_current_pass(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            jobs.request_guard = lambda: {
                "provider_pass_claim_sha256": "f" * 64,
                "create_authorization_sha256": value[
                    "create_authorization_sha256"
                ],
            }
            with self.assertRaisesRegex(ValueError, "create authority differs"):
                jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertFalse(modal.calls)

            expected = {
                "provider_pass_claim_sha256": value[
                    "provider_pass_claim_sha256"
                ],
                "create_authorization_sha256": value[
                    "create_authorization_sha256"
                ],
            }
            jobs.request_guard = lambda: expected
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            jobs.request_guard = lambda: None
            self.assertEqual(
                jobs.recover(value, execution, COMMAND, ENVIRONMENT)["state"],
                "active",
            )

    def test_recovery_requires_exact_name_and_tags(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            modal.created.tags = {"milk-run-id": "wrong"}
            with self.assertRaisesRegex(ValueError, "tags differ"):
                jobs.recover(value, execution, COMMAND, ENVIRONMENT)

    def test_create_result_requires_intent_and_binds_execution_id(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, store, modal, unused_guards, jobs = self.setup(root)
            normalized = modal_jobs._normalized(value, execution)
            unused_command, unused_environment, request = jobs._accepted_request(
                normalized, COMMAND, ENVIRONMENT
            )
            result = {
                "schema_version": "milk.modal-sandbox-create-result.v1",
                "provider_acceptance_sha256": provider_acceptance_sha256(value),
                "request_sha256": digest(request),
                "execution_name": request["execution_name"],
                "execution_id": "sb-impossible",
                "tags_sha256": digest(request["tags"]),
                "observed_at": "2026-08-27T20:00:30.000000Z",
                "state": "created",
            }
            prefix = (
                f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}"
                "/modal/create-results/00.json"
            )
            store.create(prefix, canonical_json(result), "application/json")
            with self.assertRaisesRegex(ValueError, "no create intent"):
                jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            self.assertFalse(modal.calls)

        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            modal.created.object_id = "sb-different"
            with self.assertRaisesRegex(ValueError, "ID differs"):
                jobs.recover(value, execution, COMMAND, ENVIRONMENT)

    def test_termination_is_bounded_and_zero_is_provider_observed(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            result = jobs.terminate(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(result["state"], "terminated")
            zero = jobs.verify_zero(value, execution)
            self.assertEqual(zero["state"], "zero")
            self.assertEqual(zero["matched_sandboxes"], 1)
            modal.Sandbox.list = staticmethod(
                lambda **unused_kwargs: [modal.created, modal.created]
            )
            with self.assertRaisesRegex(ValueError, "not unique"):
                jobs.verify_zero(value, execution)

        with tempfile.TemporaryDirectory() as root:
            value, execution, unused_store, modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            modal.created.terminate_error = TimeoutError("provider error")
            for _ in range(MAX_TERMINATE_ATTEMPTS):
                with self.assertRaises(TimeoutError):
                    jobs.terminate(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(
                jobs.terminate(value, execution, COMMAND, ENVIRONMENT)["state"],
                "attempts_exhausted",
            )
            self.assertEqual(
                len([call for call in modal.calls if call[0] == "terminate"]),
                MAX_TERMINATE_ATTEMPTS,
            )

    def test_termination_never_replays_an_intent_without_a_result(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, store, modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            normalized = modal_jobs._normalized(value, execution)
            unused_command, unused_environment, request = jobs._accepted_request(
                normalized, COMMAND, ENVIRONMENT
            )
            first_intent = {
                "schema_version": "milk.modal-sandbox-terminate-intent.v1",
                "provider_acceptance_sha256": provider_acceptance_sha256(value),
                "request_sha256": digest(request),
                "execution_name": request["execution_name"],
                "execution_id": modal.created.object_id,
                "attempt": 0,
                "requested_at": "2026-08-27T20:00:30.000000Z",
            }
            prefix = f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}/modal"
            store.create(
                f"{prefix}/terminate-intents/00/00.json",
                canonical_json(first_intent),
                "application/json",
            )
            result = jobs.terminate(value, execution, COMMAND, ENVIRONMENT)
            self.assertEqual(result["attempt"], 1)
            self.assertEqual(
                len([call for call in modal.calls if call[0] == "terminate"]), 1
            )

        with tempfile.TemporaryDirectory() as root:
            value, execution, store, unused_modal, unused_guards, jobs = self.setup(root)
            jobs.launch(value, execution, COMMAND, ENVIRONMENT)
            prefix = f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}/modal"
            store.create(
                f"{prefix}/terminate-results/00/00.json",
                canonical_json({"state": "terminated"}),
                "application/json",
            )
            with self.assertRaisesRegex(ValueError, "no terminate intent"):
                jobs.terminate(value, execution, COMMAND, ENVIRONMENT)

    def test_logs_and_billing_evidence_are_bounded_hash_only(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, store, modal, guards, jobs = self.setup(root)
            modal.logs = [
                LogEntry("Authorization: Bearer top-secret"),
                LogEntry("prompt=private customer text", source="stderr"),
            ]
            index = jobs.collect_logs(
                value,
                execution,
                since=NOW - dt.timedelta(minutes=5),
                until=NOW,
            )
            raw = gzip.decompress(store.get(index["chunks"][0]["key"]))
            self.assertNotIn(b"top-secret", raw)
            self.assertNotIn(b"private customer text", raw)
            self.assertGreaterEqual(len(guards), 2)
            billing = jobs.record_billing_cross_check(
                value,
                execution,
                source="summary",
                raw=b'{"amount":"9.12","customer":"private"}',
            )
            self.assertFalse(billing["authoritative_for_budget"])

    def test_log_records_are_chronological_and_count_bounded(self):
        with tempfile.TemporaryDirectory() as root:
            value, execution, store, modal, unused_guards, jobs = self.setup(root)
            modal.logs = [LogEntry("one"), LogEntry("two")]
            with mock.patch.object(modal_jobs, "MAX_LOG_RECORDS", 1):
                index = jobs.collect_logs(
                    value,
                    execution,
                    since=NOW - dt.timedelta(minutes=5),
                    until=NOW,
                )
            self.assertFalse(index["source_complete"])
            self.assertEqual(index["stored_records"], 1)
            self.assertEqual(index["missing_reason"], "bounded_log_record_limit")
            self.assertEqual(len(store.list(index["chunks"][0]["key"].rsplit("/", 1)[0])), 2)

        with tempfile.TemporaryDirectory() as root:
            value, execution, store, modal, unused_guards, jobs = self.setup(root)
            modal.logs = [
                LogEntry("later", timestamp=NOW),
                LogEntry("earlier", timestamp=NOW - dt.timedelta(seconds=1)),
            ]
            with self.assertRaisesRegex(ValueError, "log record"):
                jobs.collect_logs(
                    value,
                    execution,
                    since=NOW - dt.timedelta(minutes=5),
                    until=NOW,
                )
            index_key = next(
                key for key in store.list(
                    f"campaigns/v1/{value['campaign_id']}/jobs/{value['run_id']}"
                    "/modal/logs/modal"
                )
                if key.endswith("index.json")
            )
            self.assertEqual(
                json.loads(store.get(index_key))["missing_reason"],
                "provider_log_read_failed",
            )


if __name__ == "__main__":
    unittest.main()
