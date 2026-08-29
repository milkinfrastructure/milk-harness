import hashlib
import http.client
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
SPEC = importlib.util.spec_from_file_location("gpt_oss_teacher_runtime", HERE / "runtime.py")
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


def conformance_contract():
    fixture = json.loads((REPO / "contracts/snapshot-analyzer.json").read_text("utf-8"))
    if fixture["schema_version"] != "milk.snapshot-analyzer-conformance.v1":
        raise ValueError("snapshot analyzer conformance fixture has the wrong schema")
    source = fixture["source_gateway"]
    if len(source["source_commit"]) != 40 or len(source["source_file_sha256"]) != 64:
        raise ValueError("snapshot analyzer conformance fixture has invalid source identity")
    return fixture["system_prompt"], fixture["response_format"]


def teacher_request():
    prompt, response_format = conformance_contract()
    return {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": '{"snapshots":[]}'},
        ],
        "n": 1,
        "stream": False,
        "temperature": 1.0,
        "top_p": 0.95,
        "reasoning_effort": "high",
        "max_tokens": 4096,
        "response_format": response_format,
    }


class FakeResponse:
    def __init__(self, body=b'{"ok":true}', status=200, content_type="application/json"):
        self._body = io.BytesIO(body)
        self.status = status
        self.headers = {"Content-Type": content_type}
        self.closed = False

    def read(self, size=-1):
        return self._body.read(size)

    def close(self):
        self.closed = True


class CountingOpener:
    def __init__(self, response=None, error=None):
        self.calls = []
        self.response = response
        self.error = error

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        if self.error:
            raise self.error
        return self.response


class RuntimeTest(unittest.TestCase):
    def test_contract_hashes_match_fixture_and_reasoning_is_not_rewritten(self):
        prompt, response_format = conformance_contract()
        self.assertEqual(
            hashlib.sha256(prompt.encode()).hexdigest(),
            runtime.PROFILE["contract"]["system_prompt_sha256"],
        )
        self.assertEqual(
            runtime._canonical_sha256(response_format),
            runtime.PROFILE["contract"]["response_format_sha256"],
        )
        raw = json.dumps(teacher_request(), separators=(",", ":")).encode()
        forwarded = json.loads(runtime.prepare_forward(raw))
        self.assertEqual(forwarded["reasoning_effort"], "high")
        self.assertIs(forwarded["include_reasoning"], False)
        self.assertEqual(set(forwarded), set(teacher_request()) | {"include_reasoning"})

    def test_contract_rejects_drift_before_forwarding(self):
        mutations = [
            lambda value: value.update(reasoning_effort="max"),
            lambda value: value.update(stream=True),
            lambda value: value.update(extra=True),
            lambda value: value["messages"][0].update(content="different"),
            lambda value: value["response_format"]["json_schema"].update(strict=False),
            lambda value: value.update(max_tokens=True),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                request = teacher_request()
                mutate(request)
                with self.assertRaises(runtime.RequestRejected):
                    runtime.prepare_forward(json.dumps(request).encode())
        duplicate = b'{"model":"a","model":"b"}'
        with self.assertRaises(runtime.RequestRejected):
            runtime.prepare_forward(duplicate)

    def test_forward_is_one_loopback_call_and_has_no_retry(self):
        body = runtime.prepare_forward(json.dumps(teacher_request()).encode())
        response = FakeResponse(b'{"id":"exact"}')
        opener = CountingOpener(response=response)
        status, content_type, result = runtime.forward_once(body, opener)
        self.assertEqual((status, content_type, result), (200, "application/json", b'{"id":"exact"}'))
        self.assertTrue(response.closed)
        self.assertEqual(len(opener.calls), 1)
        request, timeout = opener.calls[0]
        self.assertEqual(request.full_url, runtime.UPSTREAM_URL)
        self.assertEqual(request.method, "POST")
        self.assertEqual(timeout, runtime.UPSTREAM_TIMEOUT_SECONDS)
        self.assertEqual(request.data, body)

        failed = CountingOpener(error=urllib.error.URLError("unit failure"))
        with self.assertRaises(runtime.UpstreamFailed):
            runtime.forward_once(body, failed)
        self.assertEqual(len(failed.calls), 1)

    def test_response_is_bounded(self):
        with self.assertRaises(runtime.RequestRejected):
            runtime.prepare_forward(b" " * (runtime.MAX_REQUEST_BYTES + 1))
        response = FakeResponse(b"x" * (runtime.MAX_RESPONSE_BYTES + 1))
        opener = CountingOpener(response=response)
        with self.assertRaises(runtime.UpstreamFailed):
            runtime.forward_once(b"{}", opener)
        self.assertEqual(len(opener.calls), 1)
        self.assertTrue(response.closed)

    def test_ingress_exits_when_vllm_exits(self):
        class Child:
            polls = 0

            def poll(self):
                self.polls += 1
                return None if self.polls == 1 else 1

        class Server:
            timeout = None
            requests = 0

            def handle_request(self):
                self.requests += 1

        child = Child()
        server = Server()
        with self.assertRaisesRegex(RuntimeError, "vLLM exited after readiness"):
            runtime._serve_while_child(server, child)
        self.assertEqual(server.timeout, 0.5)
        self.assertEqual(server.requests, 1)

    def test_shutdown_covers_vllm_process_group_and_key_is_not_inherited(self):
        class Process:
            pid = 4242

            def __init__(self):
                self.waits = 0

            def poll(self):
                return None

            def wait(self, timeout):
                self.waits += 1
                if self.waits == 1:
                    raise runtime.subprocess.TimeoutExpired("vllm", timeout)
                return -9

        old_killpg = runtime.os.killpg
        old_key = os.environ.get(runtime.API_KEY_ENV)
        old_baseten_key = os.environ.get("BASETEN_API_KEY")
        signals = []
        try:
            runtime.os.killpg = lambda pid, sent: signals.append((pid, sent))
            os.environ[runtime.API_KEY_ENV] = "must-not-reach-vllm"
            os.environ["BASETEN_API_KEY"] = "also-must-not-reach-vllm"
            self.assertNotIn(runtime.API_KEY_ENV, runtime._vllm_environment())
            self.assertNotIn("BASETEN_API_KEY", runtime._vllm_environment())
            process = Process()
            runtime._stop(process)
            self.assertEqual(
                signals,
                [(4242, runtime.signal.SIGTERM), (4242, runtime.signal.SIGKILL)],
            )
            self.assertEqual(process.waits, 2)
        finally:
            runtime.os.killpg = old_killpg
            if old_key is None:
                os.environ.pop(runtime.API_KEY_ENV, None)
            else:
                os.environ[runtime.API_KEY_ENV] = old_key
            if old_baseten_key is None:
                os.environ.pop("BASETEN_API_KEY", None)
            else:
                os.environ["BASETEN_API_KEY"] = old_baseten_key

    def test_authenticated_http_ingress_forwards_only_valid_contract(self):
        old_key = os.environ.get(runtime.API_KEY_ENV)
        old_forward = runtime.forward_once
        calls = []

        def fake_forward(body):
            calls.append(json.loads(body))
            return 200, "application/json", b'{"id":"passed-through"}'

        os.environ[runtime.API_KEY_ENV] = "unit-secret"
        runtime.forward_once = fake_forward
        server = runtime.HTTPServer(("127.0.0.1", 0), runtime.TeacherHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            body = json.dumps(teacher_request()).encode()
            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request(
                "POST",
                runtime.CHAT_PATH,
                body=body,
                headers={"Content-Type": "application/json", "Authorization": "Bearer wrong"},
            )
            self.assertEqual(connection.getresponse().status, 401)
            connection.close()
            self.assertEqual(calls, [])

            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request(
                "POST",
                runtime.CHAT_PATH,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                    "Authorization": "Bearer unit-secret",
                },
            )
            self.assertEqual(connection.getresponse().status, 415)
            connection.close()
            self.assertEqual(calls, [])

            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request(
                "POST",
                runtime.CHAT_PATH,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer unit-secret",
                },
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), b'{"id":"passed-through"}')
            connection.close()
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["reasoning_effort"], "high")
            self.assertIs(calls[0]["include_reasoning"], False)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            runtime.forward_once = old_forward
            if old_key is None:
                os.environ.pop(runtime.API_KEY_ENV, None)
            else:
                os.environ[runtime.API_KEY_ENV] = old_key

    def test_authenticated_readiness_is_exact_and_does_not_infer(self):
        old_key = os.environ.get(runtime.API_KEY_ENV)
        old_forward = runtime.forward_once
        os.environ[runtime.API_KEY_ENV] = "unit-secret"
        runtime.forward_once = lambda _body: self.fail("readiness called vLLM")
        server = runtime.HTTPServer(("127.0.0.1", 0), runtime.TeacherHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request(
                "GET",
                runtime.HEALTH_PATH,
                headers={"Authorization": "Bearer unit-secret"},
            )
            response = connection.getresponse()
            expected = (
                b'{"schema_version":"milk.teacher-readiness.v1",'
                b'"model":"openai/gpt-oss-120b","profile_sha256":"'
                + hashlib.sha256((HERE / "profile.json").read_bytes()).hexdigest().encode()
                + b'","state":"ready"}'
            )
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), expected)
            self.assertLessEqual(len(expected), 4096)
            self.assertEqual(response.getheader("Content-Type"), "application/json")
            self.assertEqual(response.getheader("Content-Encoding"), "identity")
            self.assertEqual(response.getheader("Cache-Control"), "no-store")
            self.assertIsNone(response.getheader("Date"))
            self.assertIsNone(response.getheader("Server"))
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            runtime.forward_once = old_forward
            if old_key is None:
                os.environ.pop(runtime.API_KEY_ENV, None)
            else:
                os.environ[runtime.API_KEY_ENV] = old_key

    def test_job_readiness_probe_is_authenticated_and_exact(self):
        old_key = os.environ.get(runtime.API_KEY_ENV)
        response = FakeResponse(runtime.READINESS_BODY)
        response.headers["Content-Encoding"] = "identity"
        opener = CountingOpener(response=response)
        try:
            os.environ[runtime.API_KEY_ENV] = "internal-only"
            runtime.wait_ready(
                timeout_seconds=1,
                opener=opener,
                sleep=lambda _seconds: self.fail("ready response retried"),
            )
        finally:
            if old_key is None:
                os.environ.pop(runtime.API_KEY_ENV, None)
            else:
                os.environ[runtime.API_KEY_ENV] = old_key
        self.assertTrue(response.closed)
        self.assertEqual(len(opener.calls), 1)
        request, timeout = opener.calls[0]
        self.assertEqual(request.full_url, runtime.INGRESS_HEALTH_URL)
        self.assertEqual(request.get_header("Authorization"), "Bearer internal-only")
        self.assertEqual(timeout, 1)

    def test_readiness_rejects_other_requests(self):
        old_key = os.environ.get(runtime.API_KEY_ENV)
        os.environ[runtime.API_KEY_ENV] = "unit-secret"
        server = runtime.HTTPServer(("127.0.0.1", 0), runtime.TeacherHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            cases = [
                ("/other", {"Authorization": "Bearer unit-secret"}, 404),
                (runtime.HEALTH_PATH, {}, 401),
                (runtime.HEALTH_PATH, {"Authorization": "Bearer wrong"}, 401),
                (
                    runtime.HEALTH_PATH,
                    {
                        "Authorization": "Bearer unit-secret",
                        "Transfer-Encoding": "chunked",
                    },
                    400,
                ),
                (
                    runtime.HEALTH_PATH,
                    {"Authorization": "Bearer unit-secret", "Content-Length": "1"},
                    400,
                ),
                (
                    runtime.HEALTH_PATH,
                    {"Authorization": "Bearer unit-secret", "Content-Length": "no"},
                    400,
                ),
            ]
            for path, headers, expected_status in cases:
                with self.subTest(path=path, headers=headers):
                    connection = http.client.HTTPConnection(
                        *server.server_address, timeout=3
                    )
                    connection.request("GET", path, headers=headers)
                    response = connection.getresponse()
                    self.assertEqual(response.status, expected_status)
                    response.read()
                    connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            if old_key is None:
                os.environ.pop(runtime.API_KEY_ENV, None)
            else:
                os.environ[runtime.API_KEY_ENV] = old_key

    def test_model_verifier_hashes_files_and_checks_index(self):
        original = runtime.INVENTORY
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                names = [f"model-{index:05d}-of-00014.safetensors" for index in range(15)]
                for index, name in enumerate(names):
                    (root / name).write_bytes(bytes([index]))
                index_value = {
                    "metadata": {"total_size": 15},
                    "weight_map": {f"weight.{index}": name for index, name in enumerate(names)},
                }
                index_raw = json.dumps(index_value).encode()
                (root / "model.safetensors.index.json").write_bytes(index_raw)
                files = [
                    {
                        "path": name,
                        "bytes": 1,
                        "sha256": hashlib.sha256(bytes([index])).hexdigest(),
                    }
                    for index, name in enumerate(names)
                ]
                files.append(
                    {
                        "path": "model.safetensors.index.json",
                        "bytes": len(index_raw),
                        "sha256": hashlib.sha256(index_raw).hexdigest(),
                    }
                )
                runtime.INVENTORY = {
                    "files": files,
                    "weights": {"files": names, "file_bytes": 15, "tensor_bytes": 15},
                }
                runtime.verify_model(root, require_readonly=False)
                for extra in (
                    root / "surplus-file",
                    root / "surplus-directory",
                    root / "surplus-symlink",
                ):
                    if extra.name == "surplus-file":
                        extra.write_bytes(b"surplus")
                    elif extra.name == "surplus-directory":
                        extra.mkdir()
                    else:
                        extra.symlink_to(names[0])
                    with self.assertRaisesRegex(RuntimeError, "exact inventory"):
                        runtime.verify_model(root, require_readonly=False)
                    if extra.is_dir() and not extra.is_symlink():
                        extra.rmdir()
                    else:
                        extra.unlink()
                (root / names[0]).write_bytes(b"x")
                with self.assertRaisesRegex(RuntimeError, "digest differs"):
                    runtime.verify_model(root, require_readonly=False)
        finally:
            runtime.INVENTORY = original

    def test_inventory_provenance_and_container_are_fixed(self):
        inventory_raw = (HERE / "model.manifest.json").read_bytes()
        provenance = json.loads((HERE / "provenance.json").read_text("utf-8"))
        inventory = json.loads(inventory_raw)
        weights = inventory["weights"]
        entries = {item["path"]: item for item in inventory["files"]}
        self.assertEqual(inventory["revision"], runtime.PROFILE["model"]["revision"])
        self.assertEqual(len(weights["files"]), 15)
        self.assertEqual(sum(entries[name]["bytes"] for name in weights["files"]), 65248893184)
        self.assertEqual(weights["tensor_bytes"], 65248815744)
        self.assertEqual(
            hashlib.sha256(inventory_raw).hexdigest(),
            provenance["model"]["inventory_sha256"],
        )
        self.assertEqual(entries["LICENSE"]["sha256"], provenance["license"]["sha256"])
        self.assertEqual(
            entries["USAGE_POLICY"]["sha256"], provenance["usage_policy"]["sha256"]
        )
        bundled_license = (HERE / "LICENSE.openai-gpt-oss").read_bytes()
        bundled_policy = (HERE / "USAGE_POLICY.openai-gpt-oss").read_bytes()
        self.assertEqual(len(bundled_license), provenance["license"]["bytes"])
        self.assertEqual(
            hashlib.sha256(bundled_license).hexdigest(), provenance["license"]["sha256"]
        )
        self.assertEqual(len(bundled_policy), provenance["usage_policy"]["bytes"])
        self.assertEqual(
            hashlib.sha256(bundled_policy).hexdigest(),
            provenance["usage_policy"]["sha256"],
        )

        dockerfile = (HERE / "Dockerfile").read_text("utf-8")
        self.assertIn(runtime.PROFILE["vllm"]["image"], dockerfile)
        self.assertIn("VLLM_SYSTEM_START_DATE=2025-08-05", dockerfile)
        self.assertEqual(runtime.PROFILE["ingress"]["host"], "127.0.0.1")
        self.assertNotIn("EXPOSE", dockerfile)
        self.assertIn("ARG MILK_GATEWAY_IMAGE", dockerfile)
        self.assertIn("COPY --from=gateway", dockerfile)
        copy_instructions = [
            line for line in dockerfile.splitlines() if line.startswith("COPY ")
        ]
        self.assertTrue(copy_instructions)
        self.assertTrue(
            all("--link" in instruction.split() for instruction in copy_instructions)
        )
        self.assertNotRegex(dockerfile, r"(?m)^(?:ADD|RUN)\s")
        self.assertIn('ENTRYPOINT ["/opt/dragontales/job.sh"]', dockerfile)
        self.assertNotRegex(dockerfile, r"(?m)^(COPY|ADD).*models/gpt-oss")
        self.assertNotIn("--enable-auto-tool-choice", json.dumps(runtime.PROFILE))
        self.assertNotIn("--tool-call-parser", json.dumps(runtime.PROFILE))
        argv = runtime.PROFILE["vllm"]["argv"]
        self.assertEqual(argv[argv.index("--host") + 1], "127.0.0.1")
        self.assertEqual(argv[argv.index("--tensor-parallel-size") + 1], "1")
        self.assertEqual(argv[argv.index("--max-num-seqs") + 1], "1")
        self.assertEqual(argv[argv.index("--max-num-batched-tokens") + 1], "8192")
        self.assertEqual(argv[argv.index("--max-cudagraph-capture-size") + 1], "2048")
        self.assertIn("--no-enable-prefix-caching", argv)
        contract = runtime.PROFILE["contract"]
        self.assertEqual(
            contract["max_input_tokens"]
            + contract["max_output_tokens"]
            + contract["minimum_context_margin_tokens"],
            contract["max_model_len"],
        )
        self.assertEqual(contract["max_request_bytes"], contract["max_input_tokens"])
        self.assertEqual(contract["min_output_tokens"], contract["max_output_tokens"])
        self.assertEqual(contract["max_output_tokens"], 4096)
        self.assertLessEqual(
            contract["max_projected_bytes"] * 8 + 16384,
            contract["max_request_bytes"],
        )
        precondition = runtime.PROFILE["core_precondition"]
        self.assertEqual(precondition["status"], "implemented_verified_offline")
        self.assertEqual(
            precondition["maximum_projected_bytes"], contract["max_projected_bytes"]
        )
        self.assertEqual(
            precondition["maximum_provider_request_bytes"],
            contract["max_request_bytes"],
        )
        self.assertIn("provider-bound local rejection", precondition["requirement"])
        self.assertIn("without a teacher claim or provider call", precondition["requirement"])
        gate = runtime.PROFILE["live_admission_gate"]
        self.assertEqual(gate["per_call_deadline_seconds"], 180)
        self.assertEqual(gate["per_call_target_seconds"], 150)
        self.assertNotIn("maximum_total_seconds", gate)
        self.assertEqual(
            runtime.PROFILE["live_admission_gate"]["status"],
            "unqualified_pending_live_gate",
        )
        self.assertIn("LICENSE.openai-gpt-oss", dockerfile)
        self.assertIn("USAGE_POLICY.openai-gpt-oss", dockerfile)
        job = (HERE / "job.sh").read_text("utf-8")
        self.assertLess(
            job.index("begin-teacher-run"),
            job.index('python3 "$runtime" serve'),
        )
        self.assertLess(
            job.index('python3 "$runtime" serve'),
            job.index("run_execute_gateway &"),
        )
        self.assertIn("secrets.token_urlsafe(32)", job)
        self.assertIn("-u MILK_CAPTURE_STORE_SECRET_ACCESS_KEY", job)
        self.assertIn("-u MILK_CONTROL_STORE_SECRET_ACCESS_KEY", job)
        self.assertNotIn("MILK_CARTON_TEACHER_API_KEY", dockerfile)
        runtime_source = (HERE / "runtime.py").read_text("utf-8")
        self.assertNotIn("print(", runtime_source)
        self.assertNotIn("--enable-log-requests", json.dumps(runtime.PROFILE))
        self.assertLess(
            runtime_source.index("signal.signal(signal.SIGTERM"),
            runtime_source.index("process = subprocess.Popen"),
        )

    def test_job_failures_terminalize_once_and_remain_nonzero(self):
        source = (HERE / "job.sh").read_text(encoding="utf-8")
        for failure, expected_status in (
            ("readiness_failure", 71),
            ("readiness_signal", 143),
            ("execute_signal", 143),
        ):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                gateway = root / "gateway"
                fake_runtime = root / "runtime.py"
                execute_marker = root / "execute"
                execute_stopped_marker = root / "execute-stopped"
                readiness_marker = root / "readiness"
                readiness_stopped_marker = root / "readiness-stopped"
                terminal_marker = root / "terminal"
                ordering_violation = root / "ordering-violation"
                gateway_source = """#!/bin/sh
[ "${MILK_CONTROL_STORE_ACCESS_KEY_ID:-}" = control-access ] || exit 65
[ "${MILK_CONTROL_STORE_SECRET_ACCESS_KEY:-}" = control-secret ] || exit 66
[ "${MILK_CONTROL_STORE_SESSION_TOKEN:-}" = control-session ] || exit 67
case " $* " in
  *" begin-teacher-run "*)
    [ -z "${MILK_CAPTURE_STORE_ACCESS_KEY_ID+x}" ] || exit 68
    exit 0
    ;;
  *" execute-teacher-run "*)
    [ "${MILK_CAPTURE_STORE_ACCESS_KEY_ID:-}" = capture-access ] || exit 69
    [ "${MILK_CAPTURE_STORE_SECRET_ACCESS_KEY:-}" = capture-secret ] || exit 70
    [ "${MILK_CAPTURE_STORE_SESSION_TOKEN:-}" = capture-session ] || exit 71
    trap ': > "@EXECUTE_STOPPED@"; exit 143' TERM
    : > "@EXECUTE@"
    while :; do sleep 1; done
    ;;
  *" terminalize-teacher-run "*)
    [ -z "${MILK_CAPTURE_STORE_ACCESS_KEY_ID+x}" ] || exit 72
    if [ -f "@EXECUTE@" ] && [ ! -f "@EXECUTE_STOPPED@" ]; then
      : > "@ORDERING_VIOLATION@"
      exit 90
    fi
    if [ -f "@READINESS@" ] && [ ! -f "@READINESS_STOPPED@" ]; then
      : > "@ORDERING_VIOLATION@"
      exit 91
    fi
    : > "@TERMINAL@"
    printf '%s\\n' '{"state":"terminal"}'
    exit 0
    ;;
  *) exit 64 ;;
esac
"""
                gateway.write_text(
                    gateway_source.replace("@EXECUTE@", str(execute_marker))
                    .replace("@EXECUTE_STOPPED@", str(execute_stopped_marker))
                    .replace("@READINESS@", str(readiness_marker))
                    .replace("@READINESS_STOPPED@", str(readiness_stopped_marker))
                    .replace("@TERMINAL@", str(terminal_marker))
                    .replace("@ORDERING_VIOLATION@", str(ordering_violation)),
                    encoding="utf-8",
                )
                gateway.chmod(0o755)
                fake_runtime.write_text(
                    """import os
import signal
import sys
import time

if any(
    name.startswith("MILK_") and name != "MILK_CARTON_TEACHER_API_KEY"
    for name in os.environ
):
    raise SystemExit(72)
if len(os.environ.get("MILK_CARTON_TEACHER_API_KEY", "")) < 32:
    raise SystemExit(73)
if sys.argv[1] == "wait-ready":
    if os.environ.get("FAIL_READY") == "1":
        raise SystemExit(71)
    if os.environ.get("BLOCK_READY") == "1":
        open(os.environ["READINESS_MARKER"], "w").close()
        def stop(_signum, _frame):
            open(os.environ["READINESS_STOPPED_MARKER"], "w").close()
            raise SystemExit(143)
        signal.signal(signal.SIGTERM, stop)
        while True:
            time.sleep(1)
    raise SystemExit(0)
if sys.argv[1] != "serve":
    raise SystemExit(64)
signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
while True:
    time.sleep(1)
""",
                    encoding="utf-8",
                )
                job = root / "job.sh"
                job.write_text(
                    source.replace(
                        "gateway=/usr/local/bin/milk-carton",
                        f"gateway={gateway}",
                    )
                    .replace("runtime=/opt/dragontales/runtime.py", f"runtime={fake_runtime}")
                    .replace(
                        "work_dir=/tmp/dragontales/teacher",
                        f"work_dir={root / 'work'}",
                    ),
                    encoding="utf-8",
                )
                config = root / "config.json"
                config.write_text("{}", encoding="utf-8")
                environment = {
                    **os.environ,
                    "MILK_CAPTURE_STORE_ACCESS_KEY_ID": "capture-access",
                    "MILK_CAPTURE_STORE_SECRET_ACCESS_KEY": "capture-secret",
                    "MILK_CAPTURE_STORE_SESSION_TOKEN": "capture-session",
                    "MILK_CONTROL_STORE_ACCESS_KEY_ID": "control-access",
                    "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": "control-secret",
                    "MILK_CONTROL_STORE_SESSION_TOKEN": "control-session",
                    "MILK_ROUTE_STORE_ACCESS_KEY_ID": "route-poison",
                    "MILK_ROUTE_STORE_SECRET_ACCESS_KEY": "route-poison",
                    "MILK_ROUTE_STORE_SESSION_TOKEN": "route-poison",
                    "READINESS_MARKER": str(readiness_marker),
                    "READINESS_STOPPED_MARKER": str(readiness_stopped_marker),
                    "FAIL_READY": "1" if failure == "readiness_failure" else "0",
                    "BLOCK_READY": "1" if failure == "readiness_signal" else "0",
                }
                process = subprocess.Popen(
                    [
                        "/bin/dash" if Path("/bin/dash").exists() else "/bin/sh",
                        str(job),
                        "--config",
                        str(config),
                        "--teacher-run-id",
                        "8" * 64,
                    ],
                    env=environment,
                    start_new_session=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if failure == "readiness_signal":
                    deadline = time.monotonic() + 5
                    while not readiness_marker.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(readiness_marker.exists())
                    process.terminate()
                elif failure == "execute_signal":
                    deadline = time.monotonic() + 5
                    while not execute_marker.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(execute_marker.exists())
                    process.terminate()
                self.assertEqual(process.wait(timeout=10), expected_status)
                self.assertTrue(terminal_marker.exists())
                self.assertFalse(ordering_violation.exists())
                if failure == "readiness_failure":
                    self.assertFalse(execute_marker.exists())
                elif failure == "readiness_signal":
                    self.assertTrue(readiness_stopped_marker.exists())
                    self.assertFalse(execute_marker.exists())
                else:
                    self.assertTrue(execute_stopped_marker.exists())


if __name__ == "__main__":
    unittest.main()
