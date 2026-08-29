import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


PATH = Path(__file__).with_name("github_rest.py")
SPEC = importlib.util.spec_from_file_location("github_rest", PATH)
REST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REST)
EVAL_ID = "a" * 64
REQUEST_ID = EVAL_ID + "-" + "b" * 32


def raw(value):
    return json.dumps(value, separators=(",", ":")).encode()


class GithubRestTest(unittest.TestCase):
    def test_owner_only_token_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "token")
            path.write_text("github-secret\n", encoding="ascii")
            path.chmod(0o400)
            self.assertEqual(REST.read_token_file(str(path)), "github-secret")
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "credential file"):
                REST.read_token_file(str(path))
            path.chmod(0o400)
            link = Path(directory, "link")
            link.symlink_to(path)
            with self.assertRaises((OSError, ValueError)):
                REST.read_token_file(str(link))

    def test_request_keeps_token_out_of_url_and_body(self):
        seen = {}

        class Response:
            url = "https://api.github.com/repos/owner/repo/actions/runs/1"
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_unused):
                return None

            def getcode(self):
                return 200

            def read(self, _limit):
                return b"{}"

        class Opener:
            def open(self, request, timeout):
                seen["request"] = request
                seen["timeout"] = timeout
                return Response()

        with mock.patch.object(REST.urllib.request, "build_opener", return_value=Opener()):
            self.assertEqual(
                REST.request("github-secret", "GET", "/repos/owner/repo/actions/runs/1"),
                b"{}",
            )
        self.assertEqual(seen["request"].get_header("Authorization"), "Bearer github-secret")
        self.assertNotIn("github-secret", seen["request"].full_url)
        self.assertIsNone(seen["request"].data)

    def test_private_package_reads_and_truncation_fail_closed(self):
        listed = [{"name": "milk-jobs", "package_type": "container", "visibility": "private"}]
        with mock.patch.object(REST, "request", return_value=raw(listed)):
            self.assertEqual(REST.packages("token"), {"milk-jobs": "private"})
        detail = {"name": "milk-jobs", "package_type": "container", "visibility": "private"}
        with mock.patch.object(REST, "request", return_value=raw(detail)):
            self.assertEqual(REST.package_visibility("token", "milk-jobs"), "private")
        with mock.patch.object(REST, "request", return_value=raw(listed * 100)):
            with self.assertRaisesRegex(ValueError, "truncated"):
                REST.packages("token")

    def test_exact_correlation_view_and_dispatch(self):
        title = f"Milk production loop [managed:{REQUEST_ID}]"
        runs = {"workflow_runs": [{"id": 77, "display_title": title, "event": "workflow_dispatch", "head_branch": "main"}]}
        with mock.patch.object(REST, "request", return_value=raw(runs)):
            self.assertEqual(REST.correlate("token", "owner", "repo", "loop.yml", "main", REQUEST_ID), "77")
        runs["workflow_runs"].append(dict(runs["workflow_runs"][0], id=78))
        with mock.patch.object(REST, "request", return_value=raw(runs)):
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                REST.correlate("token", "owner", "repo", "loop.yml", "main", REQUEST_ID)

        run = {"id": 77, "display_title": title, "event": "workflow_dispatch", "head_branch": "main", "path": ".github/workflows/loop.yml", "status": "completed", "conclusion": "success"}
        jobs = {"jobs": [{"name": "Provider reconciliation and dispatch [generation_done=true]"}]}
        with mock.patch.object(REST, "request", side_effect=(raw(run), raw(jobs))):
            self.assertEqual(REST.view("token", "owner", "repo", "loop.yml", "main", "77", REQUEST_ID), ("completed:success", jobs["jobs"][0]["name"]))

        with mock.patch.object(REST, "request", return_value=b"") as request:
            REST.dispatch("token", "owner", "repo", "loop.yml", "main", "true", "c" * 64, EVAL_ID, REQUEST_ID)
        self.assertEqual(request.call_args.kwargs["status"], 204)
        self.assertEqual(request.call_args.kwargs["body"]["inputs"]["managed_request_id"], REQUEST_ID)


if __name__ == "__main__":
    unittest.main()
