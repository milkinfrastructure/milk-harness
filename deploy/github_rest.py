#!/usr/bin/env python3
"""Fixed GitHub REST operations used by Milk release and Exo control."""
import json
import os
from pathlib import Path
import re
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request


API = "https://api.github.com"
MAX_BYTES = 1024 * 1024
MAX_TOKEN_BYTES = 8192
COMPONENT = re.compile(r"[A-Za-z0-9._][A-Za-z0-9._-]{0,254}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
REQUEST_ID = re.compile(r"([0-9a-f]{64})-([0-9a-f]{32})\Z")
JOB_PREFIX = "Provider reconciliation and dispatch [generation_done="
JOB_NAMES = {JOB_PREFIX + "false]", JOB_PREFIX + "true]"}

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None

def read_token_file(path_text):
    path = Path(path_text)
    if not path.is_absolute():
        raise ValueError("GitHub credential file is invalid")
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}
        or not 1 <= before.st_size <= MAX_TOKEN_BYTES
    ):
        raise ValueError("GitHub credential file is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        after = os.fstat(descriptor)
        opened = (after.st_dev, after.st_ino, after.st_uid, stat.S_IMODE(after.st_mode))
        expected = (before.st_dev, before.st_ino, os.geteuid(), stat.S_IMODE(before.st_mode))
        if opened != expected:
            raise ValueError("GitHub credential file changed")
        raw = os.read(descriptor, MAX_TOKEN_BYTES + 1)
    finally:
        os.close(descriptor)
    value = raw[:-1] if raw.endswith(b"\n") else raw
    if not value or b"\n" in value or b"\r" in value or any(byte < 33 or byte > 126 for byte in value):
        raise ValueError("GitHub credential is invalid")
    return value.decode("ascii")

def _load(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("GitHub response contains a duplicate key")
            value[key] = item
        return value

    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("GitHub response is invalid") from error

def request(token, method, path, *, query=None, body=None, status=200):
    url = API + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + token,
        "User-Agent": "milk-control/1",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request_ = urllib.request.Request(url, data=data, headers=headers, method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    try:
        with opener.open(request_, timeout=30) as response:
            final = urllib.parse.urlsplit(response.url)
            if response.getcode() != status or final.scheme != "https" or final.hostname != "api.github.com":
                raise ValueError("GitHub response is invalid")
            raw = response.read(MAX_BYTES + 1)
    except (OSError, urllib.error.URLError):
        raise ValueError("GitHub request failed") from None
    if len(raw) > MAX_BYTES or status == 204 and raw:
        raise ValueError("GitHub response is invalid")
    return raw

def packages(token):
    value = _load(request(token, "GET", "/orgs/milkinfrastructure/packages", query={"package_type": "container", "per_page": 100}))
    if not isinstance(value, list) or len(value) >= 100:
        raise ValueError("GitHub packages are invalid or truncated")
    result = {}
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("GitHub package is invalid")
        name, visibility = item.get("name"), item.get("visibility")
        if (
            not isinstance(name, str)
            or COMPONENT.fullmatch(name) is None
            or item.get("package_type") != "container"
            or visibility not in {"private", "public", "internal"}
            or name in result
        ):
            raise ValueError("GitHub package is invalid")
        result[name] = visibility
    return result

def package_visibility(token, package):
    if COMPONENT.fullmatch(package) is None:
        raise ValueError("GitHub package name is invalid")
    path = "/orgs/milkinfrastructure/packages/container/" + urllib.parse.quote(package, safe="")
    value = _load(request(token, "GET", path))
    if not isinstance(value, dict) or value.get("name") != package or value.get("package_type") != "container":
        raise ValueError("GitHub package identity is invalid")
    visibility = value.get("visibility")
    if visibility not in {"private", "public", "internal"}:
        raise ValueError("GitHub package visibility is invalid")
    return visibility

def _target(owner, repository, workflow, ref):
    if any(COMPONENT.fullmatch(value) is None for value in (owner, repository, workflow)):
        raise ValueError("GitHub target is invalid")
    if not workflow.endswith((".yml", ".yaml")) or not ref or len(ref) > 255:
        raise ValueError("GitHub target is invalid")
    base = f"/repos/{urllib.parse.quote(owner, safe='')}/{urllib.parse.quote(repository, safe='')}"
    workflow_path = base + "/actions/workflows/" + urllib.parse.quote(workflow, safe="")
    return base, workflow_path

def correlate(token, owner, repository, workflow, ref, request_id):
    _, workflow_path = _target(owner, repository, workflow, ref)
    if REQUEST_ID.fullmatch(request_id) is None:
        raise ValueError("managed request ID is invalid")
    query = {"branch": ref, "event": "workflow_dispatch", "per_page": 100}
    value = _load(request(token, "GET", workflow_path + "/runs", query=query))
    runs = value.get("workflow_runs") if isinstance(value, dict) else None
    if not isinstance(runs, list) or len(runs) > 100:
        raise ValueError("GitHub runs are invalid")
    title = f"Milk production loop [managed:{request_id}]"
    matches = [run for run in runs if isinstance(run, dict) and run.get("display_title") == title]
    if len(matches) > 1:
        raise ValueError("managed run correlation is ambiguous")
    if not matches:
        return ""
    run = matches[0]
    if (
        not isinstance(run.get("id"), int)
        or isinstance(run["id"], bool)
        or run["id"] <= 0
        or run.get("event") != "workflow_dispatch"
        or run.get("head_branch") != ref
    ):
        raise ValueError("managed run identity is invalid")
    return str(run["id"])

def view(token, owner, repository, workflow, ref, database_id, request_id):
    base, _ = _target(owner, repository, workflow, ref)
    if not database_id.isdigit() or int(database_id) <= 0 or REQUEST_ID.fullmatch(request_id) is None:
        raise ValueError("managed run identity is invalid")
    run = _load(request(token, "GET", base + "/actions/runs/" + database_id))
    path = run.get("path") if isinstance(run, dict) else None
    if isinstance(path, str):
        path = path.split("@", 1)[0]
    if (
        not isinstance(run, dict)
        or run.get("id") != int(database_id)
        or run.get("display_title") != f"Milk production loop [managed:{request_id}]"
        or run.get("event") != "workflow_dispatch"
        or run.get("head_branch") != ref
        or path != f".github/workflows/{workflow}"
        or not isinstance(run.get("status"), str)
        or not isinstance(run.get("conclusion"), (str, type(None)))
    ):
        raise ValueError("managed run identity is invalid")
    observation = run["status"] + ":" + (run["conclusion"] or "")
    if observation != "completed:success":
        return observation, None
    jobs = _load(request(token, "GET", base + f"/actions/runs/{database_id}/jobs", query={"filter": "latest", "per_page": 100}))
    jobs = jobs.get("jobs") if isinstance(jobs, dict) else None
    if not isinstance(jobs, list) or len(jobs) >= 100:
        raise ValueError("GitHub jobs are invalid or truncated")
    names = [job.get("name") for job in jobs if isinstance(job, dict) and isinstance(job.get("name"), str) and job["name"].startswith(JOB_PREFIX)]
    if len(names) != 1 or names[0] not in JOB_NAMES:
        raise ValueError("generation completion job is invalid")
    return observation, names[0]

def dispatch(token, owner, repository, workflow, ref, authorized, confirmation, eval_id, request_id):
    _, workflow_path = _target(owner, repository, workflow, ref)
    match = REQUEST_ID.fullmatch(request_id)
    if authorized not in {"true", "false"} or HEX64.fullmatch(confirmation) is None or HEX64.fullmatch(eval_id) is None or match is None or match.group(1) != eval_id:
        raise ValueError("managed dispatch is invalid")
    request(
        token,
        "POST",
        workflow_path + "/dispatches",
        body={"ref": ref, "inputs": {"authorize_provider_creates": authorized, "confirmed_run_config_sha256": confirmation, "managed_eval_id": eval_id, "managed_request_id": request_id}},
        status=204,
    )

def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        operation, token_path, *values = args
        token = read_token_file(token_path)
        if operation == "check" and not values:
            return 0
        if operation == "packages" and not values:
            for name, visibility in sorted(packages(token).items()):
                print(f"{name}\t{visibility}")
            return 0
        if operation == "correlate" and len(values) == 5:
            database_id = correlate(token, *values)
            if database_id:
                print(database_id)
            return 0
        if operation == "view" and len(values) == 6:
            observation, job = view(token, *values)
            print(observation)
            if job:
                print(job)
            return 0
        if operation == "dispatch" and len(values) == 8:
            dispatch(token, *values)
            return 0
        raise ValueError("operation is invalid")
    except (OSError, UnicodeError, ValueError):
        print("github-rest: request failed", file=sys.stderr)
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
