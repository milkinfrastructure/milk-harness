#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
builder=$root/deploy/build-images.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/milk-build-images-test.XXXXXX")

cleanup() {
  case "$test_root" in
    "${TMPDIR:-/tmp}"/milk-build-images-test.*) rm -rf -- "$test_root" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

python3 - "$root" <<'PY'
import ast
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
dockerfile = root.joinpath("Dockerfile.jobs").read_text(encoding="utf-8")
context = root.joinpath("Dockerfile.jobs.dockerignore").read_text(encoding="utf-8")
lock = root.joinpath(
    "third_party/modal/requirements-linux-amd64-cp312.lock"
).read_text(encoding="utf-8")
notices = root.joinpath("third_party/modal/THIRD_PARTY_NOTICES.md").read_text(
    encoding="utf-8"
)

requirements = {}
for line in lock.splitlines():
    if not line or line.startswith("#"):
        continue
    match = re.fullmatch(
        r"([a-z0-9-]+)==([^ ]+) --hash=sha256:([0-9a-f]{64})", line
    )
    assert match, line
    name, version, digest = match.groups()
    assert name not in requirements
    requirements[name] = (version, digest)
assert len(requirements) == 29
assert list(requirements) == sorted(requirements)
assert requirements["modal"] == (
    "1.5.4",
    "3e54e26037c445af42f9a9ef9862b66bdd2e0b1faeced5fcc7adf3e5f59e44ed",
)
assert requirements["rich"][0] == "15.0.0"
assert requirements["watchfiles"][0] == "1.2.0"
assert not {
    "aiohttp-socks",
    "async-timeout",
    "backports-tarfile",
    "exceptiongroup",
    "importlib-metadata",
    "python-socks",
    "truss",
    "truss-transfer",
    "zipp",
} & requirements.keys()
assert "Public PyPI wheels only: 29 files, 7,020,953 bytes" in lock
assert "--require-hashes --requirement requirements-linux-amd64-cp312.lock" in lock

assert "python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7" in dockerfile
assert dockerfile.count("python3 -m pip --isolated install") == 1
for required in (
    "--index-url https://pypi.org/simple",
    "--only-binary=:all:",
    "--require-hashes",
    "requirements-linux-amd64-cp312.lock",
    "THIRD_PARTY_NOTICES.md",
    "JOBS-PYTHON-THIRD-PARTY-NOTICES.md",
    "milk_harness/provider_acceptance.py",
    "milk_harness/baseten_winner.py",
    "milk_harness/modal_jobs.py",
    "milk_harness/scheduler.py",
    "deploy/baseten/winner.py",
    "deploy/baseten/adapter.py",
    "deploy/modal/admit.py",
    "import modal",
    'version("modal") == "1.5.4"',
):
    assert required in dockerfile, required
for forbidden in (
    "publish_image_admission",
    "BASETEN_API_KEY",
    "MODAL_TOKEN_ID",
    "MODAL_TOKEN_SECRET",
    "PIP_EXTRA_INDEX_URL",
    "import modal, truss",
    'version("truss")',
    "/usr/local/bin/truss",
):
    assert forbidden not in dockerfile

for included in (
    "!milk_harness/provider_acceptance.py",
    "!milk_harness/baseten_winner.py",
    "!milk_harness/modal_jobs.py",
    "!milk_harness/scheduler.py",
    "!deploy/baseten/winner.py",
    "!deploy/baseten/adapter.py",
    "!deploy/modal/admit.py",
    "!third_party/modal/requirements-linux-amd64-cp312.lock",
    "!third_party/modal/THIRD_PARTY_NOTICES.md",
):
    assert included in context
assert "publish_image_admission" not in context

copied_paths = set(
    re.findall(r"(?:milk_harness|deploy)/[a-z0-9_/]+\.py", dockerfile)
)
required_runtime_paths = {
    "milk_harness/provider_acceptance.py",
    "milk_harness/baseten_winner.py",
    "milk_harness/modal_jobs.py",
    "milk_harness/scheduler.py",
    "milk_harness/jobs.py",
    "deploy/baseten/winner.py",
    "deploy/baseten/adapter.py",
    "deploy/modal/admit.py",
}
assert required_runtime_paths <= copied_paths

required_paths = set()
for relative in copied_paths:
    tree = ast.parse(root.joinpath(relative).read_text(encoding="utf-8"))
    package = tuple(Path(relative).parent.parts)
    for node in ast.walk(tree):
        candidates = []
        if isinstance(node, ast.Import):
            candidates.extend(alias.name.split(".") for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = tuple(node.module.split(".")) if node.module else ()
            if node.level:
                keep = len(package) - node.level + 1
                base = package[:keep] + module
            else:
                base = module
            candidates.append(base)
            candidates.extend(base + tuple(alias.name.split(".")) for alias in node.names)
        for parts in candidates:
            if not parts or parts[0] not in {"milk_harness", "deploy"}:
                continue
            candidate = root.joinpath(*parts).with_suffix(".py")
            if candidate.is_file():
                required_paths.add(candidate.relative_to(root).as_posix())
assert required_paths <= copied_paths, sorted(required_paths - copied_paths)

notice_entries = re.findall(
    r"^\| ([^|]+) \| ([^|]+) \| ([^|]+) \|$",
    notices,
    flags=re.MULTILINE,
)[2:]
assert len(notice_entries) == len(requirements)
notice_rows = {package.rsplit(" ", 1)[0] for package, _, _ in notice_entries}
assert notice_rows == requirements.keys()
assert all(
    license_name.strip()
    and (re.search(r"`[0-9a-f]{64}`", evidence) or evidence.startswith("Metadata only;"))
    for _, license_name, evidence in notice_entries
)
assert "| rich 15.0.0 | MIT |" in notices
assert "| watchfiles 1.2.0 | MIT |" in notices
assert "| truss " not in notices
PY

mkdir "$test_root/bin"
mkdir -p "$test_root/home/.docker/cli-plugins"
cat >"$test_root/home/.docker/cli-plugins/docker-buildx" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod 0700 "$test_root/home/.docker/cli-plugins/docker-buildx"
revision=1111111111111111111111111111111111111111
gateway=ghcr.io/milkinfrastructure/milk-gateway@sha256:$(python3 -c 'print("5" * 64)')
source_context=$test_root/source-context.tar
python3 - "$source_context" <<'PY'
import io
import sys
import tarfile

raw = b"deterministic committed source context"
item = tarfile.TarInfo("context-marker")
item.mode = 0o644
item.mtime = 1_700_000_000
item.size = len(raw)
with tarfile.open(sys.argv[1], "w", format=tarfile.USTAR_FORMAT) as archive:
    archive.addfile(item, io.BytesIO(raw))
PY
source_context_digest=$(python3 - "$source_context" <<'PY'
import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)

manifest=$test_root/manifest.json
printf '%s' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":2},"layers":[]}' >"$manifest"
manifest_digest=$(python3 - "$manifest" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)
attestation=$test_root/attestation.json
printf '%s' "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"artifactType\":\"application/vnd.docker.attestation.manifest.v1+json\",\"config\":{\"mediaType\":\"application/vnd.oci.empty.v1+json\",\"digest\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\",\"size\":2},\"layers\":[{\"mediaType\":\"application/vnd.in-toto+json\",\"digest\":\"sha256:3333333333333333333333333333333333333333333333333333333333333333\",\"size\":2,\"annotations\":{\"in-toto.io/predicate-type\":\"https://spdx.dev/Document\"}},{\"mediaType\":\"application/vnd.in-toto+json\",\"digest\":\"sha256:4444444444444444444444444444444444444444444444444444444444444444\",\"size\":2,\"annotations\":{\"in-toto.io/predicate-type\":\"https://slsa.dev/provenance/v1\"}}],\"subject\":{\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"digest\":\"sha256:$manifest_digest\",\"size\":256}}" >"$attestation"
attestation_digest=$(python3 - "$attestation" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)
index=$test_root/index.json
printf '%s' "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.index.v1+json\",\"manifests\":[{\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"digest\":\"sha256:$manifest_digest\",\"size\":256,\"platform\":{\"architecture\":\"amd64\",\"os\":\"linux\"}},{\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"digest\":\"sha256:$attestation_digest\",\"size\":128,\"annotations\":{\"vnd.docker.reference.type\":\"attestation-manifest\",\"vnd.docker.reference.digest\":\"sha256:$manifest_digest\"},\"platform\":{\"architecture\":\"unknown\",\"os\":\"unknown\"}}]}" >"$index"
index_digest=$(python3 - "$index" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)

fake_verifier=$test_root/fake-verifier.py
cat >"$fake_verifier" <<'PY'
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

repositories = {
    "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
    "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
    "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
}
parser = argparse.ArgumentParser()
parser.add_argument("--artifact", required=True, choices=tuple(repositories))
parser.add_argument("--tagged-reference", required=True)
parser.add_argument("--source-commit", required=True)
parser.add_argument("--source-date-epoch", type=int, required=True)
parser.add_argument("--source-context-sha256", required=True)
parser.add_argument("--gateway-image-reference")
parser.add_argument("--metadata", required=True)
parser.add_argument("--docker-config", required=True)
parser.add_argument("--evidence-dir", required=True)
parser.add_argument("--registry-token-stdin", action="store_true", required=True)
arguments = parser.parse_args()
token = sys.stdin.read().strip()
if token != "ephemeral-test-password":
    raise SystemExit(93)
if token in repr(vars(arguments)) or token in repr(dict(os.environ)):
    raise SystemExit(94)
if os.environ.get("TEST_FAIL_VERIFY") == "1":
    raise SystemExit(70)

repository = repositories[arguments.artifact]
index_digest = "sha256:" + os.environ["TEST_INDEX_DIGEST"]
manifest_digest = "sha256:" + os.environ["TEST_MANIFEST_DIGEST"]
attestation_digest = "sha256:" + os.environ["TEST_ATTESTATION_DIGEST"]
for reference in (
    repository + "@" + index_digest,
    repository + "@" + manifest_digest,
    repository + "@" + attestation_digest,
):
    subprocess.run(
        [
            "docker", "--config", arguments.docker_config, "buildx", "imagetools",
            "inspect", "--raw", reference,
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

root = Path(arguments.evidence_dir)
for name, source in (
    ("index.json", os.environ["TEST_INDEX"]),
    ("amd64-manifest.json", os.environ["TEST_MANIFEST"]),
    ("attestation-manifest.json", os.environ["TEST_ATTESTATION"]),
):
    root.joinpath(name).write_bytes(Path(source).read_bytes())
config_raw = b"{}"
statement_raw = b"{}"
root.joinpath("config.json").write_bytes(config_raw)
root.joinpath("slsa-provenance.json").write_bytes(statement_raw)
root.joinpath("spdx-sbom.json").write_bytes(statement_raw)
log = json.loads(root.joinpath("build-log.json").read_bytes())
ops_log_raw = root.joinpath("ops-log-reference.json").read_bytes()
ops_log_reference = json.loads(ops_log_raw)
gateway = arguments.gateway_image_reference
image_reference = repository + "@" + index_digest
attestations = [
    {
        "layer_sha256": hashlib.sha256(statement_raw).hexdigest(),
        "predicate_type": "https://slsa.dev/provenance/v1",
    },
    {
        "layer_sha256": hashlib.sha256(statement_raw).hexdigest(),
        "predicate_type": "https://spdx.dev/Document",
    },
]
receipt = {
    "schema_version": "milk.private-harness-image-build.v1",
    "artifact": arguments.artifact,
    "source_commit": arguments.source_commit,
    "source_date_epoch": arguments.source_date_epoch,
    "source_repository": "https://github.com/milkinfrastructure/milk-harness",
    "tagged_reference": arguments.tagged_reference,
    "image_reference": image_reference,
    "index_sha256": os.environ["TEST_INDEX_DIGEST"],
    "amd64_manifest_sha256": os.environ["TEST_MANIFEST_DIGEST"],
    "attestation_manifest_sha256": os.environ["TEST_ATTESTATION_DIGEST"],
    "attestation_predicates": [item["predicate_type"] for item in attestations],
    "config_sha256": hashlib.sha256(config_raw).hexdigest(),
    "gateway_image_reference": gateway,
    "build_log": log,
    "ops_log_reference": ops_log_reference,
    "ops_log_reference_sha256": hashlib.sha256(ops_log_raw).hexdigest(),
    "visibility": "private",
    "build_authority": "local-socket",
    "buildkit_image_reference": "moby/buildkit@sha256:ddd1ca44b21eda906e81ab14a3d467fa6c39cd73b9a39df1196210edcb8db59e",
    "dockerfile_frontend_reference": "docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e",
    "platform": "linux/amd64",
    "provenance": "max",
    "provenance_version": "v1",
    "sbom": True,
}
root.joinpath("receipt.json").write_text(
    json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
admission = {
    "schema_version": "milk.private-image-admission.v1",
    "artifact": arguments.artifact,
    "repository": repository,
    "image_reference": image_reference,
    "source_repository": "https://github.com/milkinfrastructure/milk-harness",
    "source_commit": arguments.source_commit,
    "source_context_method": "git-archive-tar-v1",
    "source_context_sha256": arguments.source_context_sha256,
    "gateway_image_reference": gateway,
    "index_sha256": os.environ["TEST_INDEX_DIGEST"],
    "amd64_manifest_sha256": os.environ["TEST_MANIFEST_DIGEST"],
    "config_sha256": hashlib.sha256(config_raw).hexdigest(),
    "attestation_manifest_sha256": os.environ["TEST_ATTESTATION_DIGEST"],
    "attestations": attestations,
    "ops_log_reference": ops_log_reference,
    "ops_log_reference_sha256": hashlib.sha256(ops_log_raw).hexdigest(),
    "platform": "linux/amd64",
    "visibility": "private",
    "builder": {
        "authority": "local-socket",
        "driver": "docker-container",
        "endpoint_kind": "local-socket",
        "buildkit_image_reference": receipt["buildkit_image_reference"],
        "buildkit_version": "v0.23.2",
        "dockerfile_frontend_reference": receipt["dockerfile_frontend_reference"],
        "provenance_mode": "max",
        "provenance_version": "v1",
        "sbom": True,
    },
}
admission_raw = json.dumps(admission, sort_keys=True, separators=(",", ":")) + "\n"
root.joinpath("admission.json").write_text(admission_raw, encoding="utf-8")
with Path(os.environ["TEST_COMMAND_LOG"]).open("a", encoding="utf-8") as output:
    output.write("verifier|" + "|".join(sys.argv[1:]) + "\n")
print(
    image_reference
    + "\t"
    + hashlib.sha256(admission_raw.encode()).hexdigest()
    + "\t"
    + hashlib.sha256(ops_log_raw).hexdigest()
)
PY

cat >"$test_root/bin/git" <<'EOF'
#!/bin/sh
set -eu
if [ "${1:-}" = archive ]; then
  output=
  for argument do
    case "$argument" in --output=*) output=${argument#--output=} ;; esac
  done
  [ -n "$output" ]
  cp "$TEST_SOURCE_CONTEXT" "$output"
  exit 0
fi
case "$*" in
  'rev-parse --show-toplevel') printf '%s\n' "$TEST_REPO" ;;
  "rev-parse --verify HEAD^{commit}") printf '%s\n' "$TEST_REVISION" ;;
  "show -s --format=%ct $TEST_REVISION") printf '%s\n' '1700000000' ;;
  'status --porcelain=v1 --untracked-files=all')
    [ "${TEST_GIT_DIRTY:-0}" -eq 0 ] || printf '%s\n' ' M dirty'
    ;;
  'remote get-url origin') printf '%s\n' "${TEST_ORIGIN:-git@github.com:milkinfrastructure/milk-harness.git}" ;;
  'ls-remote --exit-code origin HEAD')
    printf '%s\tHEAD\n' "${TEST_REMOTE_REVISION:-$TEST_REVISION}"
    ;;
  *) exit 90 ;;
esac
EOF

cat >"$test_root/bin/gh" <<'EOF'
#!/bin/sh
set -eu
printf 'gh' >>"$TEST_COMMAND_LOG"
for argument do printf '|%s' "$argument" >>"$TEST_COMMAND_LOG"; done
printf '\n' >>"$TEST_COMMAND_LOG"
case "$*" in
  'auth token --hostname github.com') printf '%s\n' 'ephemeral-test-password' ;;
  *'/orgs/milkinfrastructure/packages?package_type=container&per_page=100'*)
    printf 'milk-gateway\tprivate\n'
    if [ "${TEST_PUBLIC_PACKAGE:-0}" -eq 0 ]; then
      printf 'milk-student-train\tprivate\n'
    else
      printf 'milk-student-train\tpublic\n'
    fi
    printf 'milk-student-branch\tprivate\n'
    printf 'milk-teacher-gpt-oss\tprivate\n'
    printf 'milk-jobs\tprivate\n'
    ;;
  *'/orgs/milkinfrastructure/packages/container/'*) printf '%s\n' 'private' ;;
  *) exit 91 ;;
esac
EOF

cat >"$test_root/bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf 'docker' >>"$TEST_COMMAND_LOG"
for argument do printf '|%s' "$argument" >>"$TEST_COMMAND_LOG"; done
printf '\n' >>"$TEST_COMMAND_LOG"
if [ "${1:-}" = --config ]; then
  config=$2
  [ -x "$config/cli-plugins/docker-buildx" ]
  [ "$(readlink "$config/cli-plugins/docker-buildx")" = "$HOME/.docker/cli-plugins/docker-buildx" ]
  shift 2
fi
case "${1:-} ${2:-}" in
  'context show') printf '%s\n' 'desktop-linux' ;;
  'context inspect') printf '%s\n' 'unix:///tmp/docker.sock' ;;
  'buildx version') printf '%s\n' 'github.com/docker/buildx v0.25.0' ;;
  'login ghcr.io')
    read -r password
    [ "$password" = ephemeral-test-password ]
    ;;
  'buildx create') printf '%s\n' 'test-builder' ;;
  'buildx inspect')
    printf '%s\n' \
      'Name: test-builder' \
      'Driver: docker-container' \
      'Endpoint: unix:///tmp/docker.sock' \
      'BuildKit version: v0.23.2'
    ;;
  'buildx build')
    metadata=
    tag=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --metadata-file ]; then metadata=$2; shift 2; continue; fi
      if [ "$1" = --tag ]; then tag=$2; shift 2; continue; fi
      shift
    done
    if [ "${TEST_FAIL_BUILD:-0}" -eq 1 ]; then
      printf '%s\n' 'raw failing build output must not persist'
      exit 55
    fi
    [ -n "$metadata" ]
    [ -n "$tag" ]
    printf '{"containerimage.digest":"sha256:%s","image.name":"%s"}\n' \
      "$TEST_INDEX_DIGEST" "$tag" >"$metadata"
    printf '%s\n' 'contentful fake build output'
    ;;
  'buildx imagetools')
    reference=
    for argument do reference=$argument; done
    case "$reference" in
      *@sha256:"$TEST_MANIFEST_DIGEST") cat "$TEST_MANIFEST" ;;
      *@sha256:"$TEST_ATTESTATION_DIGEST") cat "$TEST_ATTESTATION" ;;
      *) cat "$TEST_INDEX" ;;
    esac
    ;;
  'buildx rm') ;;
  *) exit 92 ;;
esac
EOF

cat >"$test_root/bin/python3" <<'EOF'
#!/bin/sh
set -eu
if [ "${1:-}" = "$TEST_REPO/deploy/verify-private-image.py" ]; then
  shift
  exec "$TEST_REAL_PYTHON" "$TEST_FAKE_VERIFIER" "$@"
fi
exec "$TEST_REAL_PYTHON" "$@"
EOF
chmod 0700 \
  "$test_root/bin/git" "$test_root/bin/gh" "$test_root/bin/docker" \
  "$test_root/bin/python3"

run_builder() {
  HOME="$test_root/home" \
  PATH="$test_root/bin:$PATH" \
  TEST_REPO="$root" \
  TEST_REAL_PYTHON="$(command -v python3)" \
  TEST_FAKE_VERIFIER="$fake_verifier" \
  TEST_REVISION="$revision" \
  TEST_SOURCE_CONTEXT="$source_context" \
  TEST_COMMAND_LOG="$test_root/commands.log" \
  TEST_INDEX="$index" \
  TEST_INDEX_DIGEST="$index_digest" \
  TEST_MANIFEST="$manifest" \
  TEST_MANIFEST_DIGEST="$manifest_digest" \
  TEST_ATTESTATION="$attestation" \
  TEST_ATTESTATION_DIGEST="$attestation_digest" \
    "$builder" "$@"
}

output=$test_root/evidence
run_builder "$gateway" "$output" >"$test_root/stdout"

[ -s "$output/input.json" ]
[ -s "$output/builder.json" ]
[ -s "$output/release.json" ]
[ ! -e "$output/failure.json" ]
for artifact in student-train student-branch teacher-gpt-oss jobs; do
  [ -s "$output/$artifact/metadata.json" ]
  [ -s "$output/$artifact/index.json" ]
  [ -s "$output/$artifact/amd64-manifest.json" ]
  [ -s "$output/$artifact/config.json" ]
  [ -s "$output/$artifact/attestation-manifest.json" ]
  [ -s "$output/$artifact/slsa-provenance.json" ]
  [ -s "$output/$artifact/spdx-sbom.json" ]
  [ -s "$output/$artifact/build-log.json" ]
  [ -s "$output/$artifact/ops-log-reference.json" ]
  [ -s "$output/$artifact/receipt.json" ]
  [ -s "$output/$artifact/admission.json" ]
done
[ -z "$(find "$output" -type f -name '*.log' -print)" ]
! grep -R -Fq 'ephemeral-test-password' "$output" "$test_root/commands.log"

[ "$(grep -c '^docker|.*buildx|build|' "$test_root/commands.log")" -eq 4 ]
[ "$(grep -c '^docker|.*buildx|imagetools|inspect|--raw|' "$test_root/commands.log")" -eq 12 ]
[ "$(grep -c '^verifier|' "$test_root/commands.log")" -eq 4 ]
[ "$(grep -c '^gh|auth|token|--hostname|github.com$' "$test_root/commands.log")" -eq 5 ]
build_order=$(grep '^docker|.*buildx|build|' "$test_root/commands.log" | \
  sed -n 's/.*|--tag|ghcr.io\/milkinfrastructure\/milk-\([^:|]*\):source-[^|]*|.*/\1/p')
[ "$build_order" = "$(printf '%s\n' jobs student-train student-branch teacher-gpt-oss)" ]
[ "$(grep -o -- '--platform|linux/amd64' "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- '--provenance=mode=max,version=v1' "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- '--sbom=true' "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- '--build-arg|BUILDKIT_SYNTAX=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e' "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- "--build-arg|MILK_SOURCE_CONTEXT_SHA256=$source_context_digest" "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- '--build-arg|SOURCE_DATE_EPOCH=1700000000' "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 4 ]
[ "$(grep -o -- "--build-arg|MILK_GATEWAY_IMAGE=$gateway" "$test_root/commands.log" | wc -l | tr -d ' ')" -eq 3 ]
grep -Fq -- "--driver-opt|image=moby/buildkit@sha256:ddd1ca44b21eda906e81ab14a3d467fa6c39cd73b9a39df1196210edcb8db59e" "$test_root/commands.log"
grep -Fq -- '--driver|docker-container' "$test_root/commands.log"
grep -Eq '^docker\|--config\|[^|]+\|buildx\|create\|.*\|unix:///tmp/docker\.sock$' "$test_root/commands.log"
grep -Fq -- '--push' "$test_root/commands.log"
grep -Fq -- "--tag|ghcr.io/milkinfrastructure/milk-student-train:source-$revision" "$test_root/commands.log"
grep -Fq -- "--tag|ghcr.io/milkinfrastructure/milk-student-branch:source-$revision" "$test_root/commands.log"
grep -Fq -- "--tag|ghcr.io/milkinfrastructure/milk-teacher-gpt-oss:source-$revision" "$test_root/commands.log"
grep -Fq -- "--tag|ghcr.io/milkinfrastructure/milk-jobs:source-$revision" "$test_root/commands.log"

python3 - "$output" "$revision" "$gateway" "$index_digest" "$source_context_digest" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root, revision, gateway, digest, context_sha256 = sys.argv[1:]
root = Path(root)
release = json.loads(root.joinpath("release.json").read_bytes())
assert release["source_commit"] == revision
assert release["gateway_image_reference"] == gateway
assert release["build_authority"] == "local-socket"
assert release["platform"] == "linux/amd64"
assert release["schema_version"] == "milk.private-harness-release.v4"
assert [item["artifact"] for item in release["images"]] == [
    "student-train", "student-branch", "teacher-gpt-oss", "jobs",
]
expected_repositories = {
    "student-train": "ghcr.io/milkinfrastructure/milk-student-train",
    "student-branch": "ghcr.io/milkinfrastructure/milk-student-branch",
    "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
}
expected_builder = {
    "authority": "local-socket",
    "driver": "docker-container",
    "endpoint_kind": "local-socket",
    "buildkit_image_reference": "moby/buildkit@sha256:ddd1ca44b21eda906e81ab14a3d467fa6c39cd73b9a39df1196210edcb8db59e",
    "buildkit_version": "v0.23.2",
    "dockerfile_frontend_reference": "docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e",
    "provenance_mode": "max",
    "provenance_version": "v1",
    "sbom": True,
}
expected_attestations = [
    {
        "layer_sha256": hashlib.sha256(b"{}").hexdigest(),
        "predicate_type": "https://slsa.dev/provenance/v1",
    },
    {
        "layer_sha256": hashlib.sha256(b"{}").hexdigest(),
        "predicate_type": "https://spdx.dev/Document",
    },
]
input_receipt = json.loads(root.joinpath("input.json").read_bytes())
assert input_receipt["source_context_method"] == "git-archive-tar-v1"
assert input_receipt["source_context_sha256"] == context_sha256
release_by_artifact = {item["artifact"]: item for item in release["images"]}
for artifact in ("student-train", "student-branch", "teacher-gpt-oss", "jobs"):
    receipt = json.loads(root.joinpath(artifact, "receipt.json").read_bytes())
    assert receipt["source_commit"] == revision
    assert receipt["visibility"] == "private"
    assert receipt["provenance"] == "max"
    assert receipt["provenance_version"] == "v1"
    assert receipt["sbom"] is True
    assert receipt["attestation_predicates"] == [
        "https://slsa.dev/provenance/v1", "https://spdx.dev/Document",
    ]
    assert receipt["build_log"]["content_retained"] is False
    assert receipt["build_log"]["bytes"] > 0
    ops_log_raw = root.joinpath(artifact, "ops-log-reference.json").read_bytes()
    ops_log = json.loads(ops_log_raw)
    assert ops_log == {
        "schema_version": "milk.private-ops-log-reference.v1",
        "authority": "private-release-evidence",
        "reference": "build-log.json",
        "receipt_sha256": hashlib.sha256(
            root.joinpath(artifact, "build-log.json").read_bytes()
        ).hexdigest(),
        "immutable": True,
        "content_retained": False,
    }
    assert receipt["ops_log_reference"] == ops_log
    assert receipt["ops_log_reference_sha256"] == hashlib.sha256(
        ops_log_raw
    ).hexdigest()
    admission_path = root.joinpath(artifact, "admission.json")
    admission_raw = admission_path.read_bytes()
    admission = json.loads(admission_raw)
    repository = expected_repositories[artifact]
    image_reference = repository + "@sha256:" + digest
    assert set(admission) == {
        "schema_version", "artifact", "repository", "image_reference",
        "source_repository", "source_commit", "source_context_method",
        "source_context_sha256", "gateway_image_reference", "index_sha256",
        "amd64_manifest_sha256", "config_sha256",
        "attestation_manifest_sha256", "attestations", "ops_log_reference",
        "ops_log_reference_sha256", "platform", "visibility", "builder",
    }
    assert admission["schema_version"] == "milk.private-image-admission.v1"
    assert admission["artifact"] == artifact
    assert admission["repository"] == repository
    assert admission["image_reference"] == image_reference
    assert admission["source_repository"] == "https://github.com/milkinfrastructure/milk-harness"
    assert admission["source_commit"] == revision
    assert admission["source_context_method"] == "git-archive-tar-v1"
    assert admission["source_context_sha256"] == context_sha256
    assert admission["index_sha256"] == hashlib.sha256(
        root.joinpath(artifact, "index.json").read_bytes()
    ).hexdigest() == digest
    assert admission["amd64_manifest_sha256"] == hashlib.sha256(
        root.joinpath(artifact, "amd64-manifest.json").read_bytes()
    ).hexdigest()
    assert admission["attestation_manifest_sha256"] == hashlib.sha256(
        root.joinpath(artifact, "attestation-manifest.json").read_bytes()
    ).hexdigest()
    assert admission["attestations"] == expected_attestations
    assert admission["ops_log_reference"] == ops_log
    assert admission["ops_log_reference_sha256"] == hashlib.sha256(
        ops_log_raw
    ).hexdigest()
    assert admission["platform"] == "linux/amd64"
    assert admission["visibility"] == "private"
    assert admission["builder"] == expected_builder
    release_item = release_by_artifact[artifact]
    assert set(release_item) == {
        "admission_sha256", "artifact", "image_reference",
        "ops_log_reference_sha256",
    }
    assert release_item["image_reference"] == image_reference
    assert release_item["admission_sha256"] == hashlib.sha256(admission_raw).hexdigest()
    assert release_item["ops_log_reference_sha256"] == hashlib.sha256(
        ops_log_raw
    ).hexdigest()
    if artifact == "jobs":
        assert receipt["gateway_image_reference"] is None
        assert admission["gateway_image_reference"] is None
    else:
        assert receipt["gateway_image_reference"] == gateway
        assert admission["gateway_image_reference"] == gateway
PY

repeat=$test_root/evidence-repeat
run_builder "$gateway" "$repeat" >/dev/null
python3 - "$output" "$repeat" <<'PY'
import json
import sys
from pathlib import Path

first, second = map(Path, sys.argv[1:])
for artifact in ("student-train", "student-branch", "teacher-gpt-oss", "jobs"):
    left = json.loads(first.joinpath(artifact, "admission.json").read_bytes())
    right = json.loads(second.joinpath(artifact, "admission.json").read_bytes())
    for value in (left, right):
        value.pop("ops_log_reference")
        value.pop("ops_log_reference_sha256")
    assert left == right
PY

seed=$test_root/verified-v5
PYTHONPATH=$root python3 - "$seed" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
import tarfile

from milk_harness.test_jobs import private_image_release

seed = Path(sys.argv[1])
harness = seed / "evidence" / "harness"
private_image_release(harness, "milk.private-harness-release.v5")
release_raw = (harness / "release.json").read_bytes()
(seed / "evidence" / "native-builder-result.json").write_text(
    json.dumps(
        {
            "schema_version": "milk.native-amd64-builder-result.v1",
            "platform": "linux/amd64",
            "harness_release_sha256": hashlib.sha256(release_raw).hexdigest(),
            "harness_source_commit": "a" * 40,
        },
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n",
    encoding="utf-8",
)
archive = seed / "evidence.tar.gz"
with tarfile.open(archive, "w:gz") as bundle:
    bundle.add(seed / "evidence", arcname="evidence")
(seed / "evidence-archive.json").write_bytes(
    (json.dumps({
        "schema_version": "milk.native-builder-evidence-archive.v1",
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "bytes": archive.stat().st_size,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
)
PY

rm -f -- "$test_root/commands.log"
selective=$test_root/evidence-selective
run_builder --reuse-release-dir "$seed" "$gateway" "$selective" \
  >"$test_root/selective-stdout"
[ "$(grep -c '^docker|.*buildx|build|' "$test_root/commands.log")" -eq 1 ]
[ "$(grep -c '^docker|.*buildx|imagetools|inspect|--raw|' "$test_root/commands.log")" -eq 3 ]
[ "$(grep -c '^verifier|' "$test_root/commands.log")" -eq 1 ]
grep -Fq -- "--tag|ghcr.io/milkinfrastructure/milk-jobs:source-$revision" \
  "$test_root/commands.log"
! grep '^docker|.*buildx|build|' "$test_root/commands.log" | \
  grep -Eq 'milk-(student-train|student-branch|teacher-gpt-oss)'
[ ! -e "$selective/planner" ]
for artifact in student-train student-branch teacher-gpt-oss; do
  cmp "$seed/evidence/harness/$artifact/admission.json" \
    "$selective/$artifact/admission.json"
done
python3 - "$selective" "$revision" "$gateway" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root, revision, gateway = sys.argv[1:]
root = Path(root)
release_raw = root.joinpath("release.json").read_bytes()
release = json.loads(release_raw)
assert release["schema_version"] == "milk.private-harness-release.v5"
assert release["source_commit"] == revision
assert release["gateway_image_reference"] == gateway
assert [item["artifact"] for item in release["images"]] == [
    "student-train", "student-branch", "teacher-gpt-oss", "jobs",
]
for item in release["images"]:
    admission_raw = root.joinpath(item["artifact"], "admission.json").read_bytes()
    admission = json.loads(admission_raw)
    assert set(item) == {
        "admission_sha256", "artifact", "image_reference",
        "ops_log_reference_sha256", "source_commit",
    }
    assert item["source_commit"] == admission["source_commit"]
    assert item["admission_sha256"] == hashlib.sha256(admission_raw).hexdigest()
    if item["artifact"] == "jobs":
        assert item["source_commit"] == revision
    else:
        assert item["source_commit"] == "8" * 40
PY

tampered_seed=$test_root/tampered-v5
python3 - "$seed" "$tampered_seed" <<'PY'
from pathlib import Path
import shutil
import sys

source, target = map(Path, sys.argv[1:])
shutil.copytree(source, target)
path = target / "evidence/harness/student-train/build-log.json"
path.write_bytes(path.read_bytes() + b" ")
PY
rm -f -- "$test_root/commands.log"
set +e
run_builder --reuse-release-dir "$tampered_seed" "$gateway" \
  "$test_root/tampered-evidence" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 70 ]
[ "$(grep -c '^docker|.*buildx|build|' "$test_root/commands.log" || true)" -eq 0 ]
python3 - "$test_root/tampered-evidence/failure.json" <<'PY'
import json
import sys
from pathlib import Path

failure = json.loads(Path(sys.argv[1]).read_bytes())
assert failure["stage"] == "verify-reused-release"
PY

assert_rejected() {
  name=$1
  shift
  rm -f -- "$test_root/commands.log"
  set +e
  "$@" >/dev/null 2>&1
  status=$?
  set -e
  [ "$status" -eq 64 ]
  [ ! -e "$test_root/commands.log" ]
  [ ! -e "$test_root/rejected-$name" ]
}

base_env="HOME=$test_root/home PATH=$test_root/bin:$PATH TEST_REPO=$root TEST_REAL_PYTHON=$(command -v python3) TEST_FAKE_VERIFIER=$fake_verifier TEST_REVISION=$revision TEST_SOURCE_CONTEXT=$source_context TEST_COMMAND_LOG=$test_root/commands.log TEST_INDEX=$index TEST_INDEX_DIGEST=$index_digest TEST_MANIFEST=$manifest TEST_MANIFEST_DIGEST=$manifest_digest TEST_ATTESTATION=$attestation TEST_ATTESTATION_DIGEST=$attestation_digest"

assert_rejected source-revision env $base_env \
  "$builder" "$gateway" "$revision" "$test_root/rejected-source-revision"
assert_rejected mutable-gateway env $base_env \
  "$builder" ghcr.io/milkinfrastructure/milk-gateway:latest "$test_root/rejected-mutable-gateway"
assert_rejected wrong-gateway env $base_env \
  "$builder" ghcr.io/shantanujoshi/milk-gateway@sha256:$(printf '%064d' 8) "$test_root/rejected-wrong-gateway"
assert_rejected dirty env $base_env TEST_GIT_DIRTY=1 \
  "$builder" "$gateway" "$test_root/rejected-dirty"
assert_rejected origin env $base_env TEST_ORIGIN=https://github.com/example/milk-harness.git \
  "$builder" "$gateway" "$test_root/rejected-origin"
assert_rejected unpublished env $base_env TEST_REMOTE_REVISION=$(printf '%040d' 9) \
  "$builder" "$gateway" "$test_root/rejected-unpublished"
mkdir "$test_root/existing"
assert_rejected existing env $base_env \
  "$builder" "$gateway" "$test_root/existing"
assert_rejected registry-credential env $base_env GH_TOKEN=secret \
  "$builder" "$gateway" "$test_root/rejected-registry-credential"
assert_rejected provider-credential env $base_env BASETEN_API_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-provider-credential"
assert_rejected store-credential env $base_env MILK_CONTROL_STORE_SESSION_TOKEN=secret \
  "$builder" "$gateway" "$test_root/rejected-store-credential"
assert_rejected r2-credential env $base_env MILK_EVIDENCE_R2_SECRET_ACCESS_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-r2-credential"
assert_rejected create-authority-credential env $base_env \
  MILK_CREATE_AUTHORITY_WRITE_R2_SECRET_ACCESS_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-create-authority-credential"
assert_rejected teacher-credential env $base_env TEACHER_API_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-teacher-credential"
assert_rejected openai-credential env $base_env OPENAI_API_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-openai-credential"
assert_rejected codex-credential env $base_env CODEX_API_KEY=secret \
  "$builder" "$gateway" "$test_root/rejected-codex-credential"
assert_rejected remote-docker env $base_env DOCKER_HOST=tcp://builder.example:2376 \
  "$builder" "$gateway" "$test_root/rejected-remote-docker"

rm -f -- "$test_root/commands.log"
set +e
env $base_env TEST_PUBLIC_PACKAGE=1 \
  "$builder" "$gateway" "$test_root/public-package" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 77 ]
[ -s "$test_root/public-package/failure.json" ]
[ "$(grep -c 'buildx|build|' "$test_root/commands.log" || true)" -eq 0 ]

rm -f -- "$test_root/commands.log"
set +e
env $base_env TEST_FAIL_BUILD=1 \
  "$builder" "$gateway" "$test_root/build-failure" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 70 ]
[ -s "$test_root/build-failure/failure.json" ]
[ -s "$test_root/build-failure/jobs/build-log.json" ]
[ -s "$test_root/build-failure/jobs/ops-log-reference.json" ]
[ -z "$(find "$test_root/build-failure" -type f -name '*.log' -print)" ]
! grep -R -Fq 'raw failing build output must not persist' "$test_root/build-failure"
python3 - "$test_root/build-failure" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
failure = json.loads(root.joinpath("failure.json").read_bytes())
observation = json.loads(root.joinpath("jobs", "build-log.json").read_bytes())
assert failure["stage"] == "build-jobs"
assert observation["exit_code"] == 55
assert observation["content_retained"] is False
assert observation["bytes"] > 0
PY

rm -f -- "$test_root/commands.log"
set +e
env $base_env TEST_FAIL_VERIFY=1 \
  "$builder" "$gateway" "$test_root/verify-failure" >/dev/null 2>&1
status=$?
set -e
[ "$status" -eq 70 ]
[ -s "$test_root/verify-failure/failure.json" ]
[ ! -e "$test_root/verify-failure/release.json" ]
python3 - "$test_root/verify-failure" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
failure = json.loads(root.joinpath("failure.json").read_bytes())
assert failure["stage"] == "verify-jobs"
PY
! grep -R -Fq 'ephemeral-test-password' \
  "$test_root/verify-failure" "$test_root/commands.log"

for dockerfile in \
  "$root/deploy/student-train/Dockerfile" \
  "$root/deploy/student-branch/Dockerfile" \
  "$root/deploy/teacher/gpt-oss-120b/Dockerfile" \
  "$root/Dockerfile.jobs"; do
  grep -Fxq '# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e' \
    "$dockerfile"
done
teacher_dockerfile=$root/deploy/teacher/gpt-oss-120b/Dockerfile
! grep -Eq '^(ADD|RUN)[[:space:]]' "$teacher_dockerfile"
[ "$(grep -c '^COPY .*--link' "$teacher_dockerfile")" -eq \
  "$(grep -c '^COPY ' "$teacher_dockerfile")" ]
grep -Fq -- '--sbom=true' "$builder"
grep -Fxq 'USER 65532:65532' "$root/Dockerfile.jobs"
grep -Fq 'import modal' "$root/Dockerfile.jobs"
! grep -Fq 'truss' "$root/Dockerfile.jobs"
grep -Fq 'milk_harness/image_admission.py' "$root/Dockerfile.jobs"
grep -Fq 'milk_harness/provider_acceptance.py' "$root/Dockerfile.jobs"
grep -Fq 'deploy/baseten/winner.py' "$root/Dockerfile.jobs"
grep -Fq 'deploy/modal/admit.py' "$root/Dockerfile.jobs"
! grep -Fq 'publish_image_admission.py' "$root/Dockerfile.jobs"
! grep -Eq 'MILK_GATEWAY_IMAGE|dragontales-gateway|--from=gateway|/usr/share/licenses/dragontales|gpt-oss-120b/profile' \
  "$root/Dockerfile.jobs"
grep -Fxq '**' "$root/Dockerfile.jobs.dockerignore"
grep -Fxq '!milk_harness/image_admission.py' "$root/Dockerfile.jobs.dockerignore"
! grep -Fq '!milk_harness/publish_image_admission.py' "$root/Dockerfile.jobs.dockerignore"
! grep -Fq '!deploy/teacher/' "$root/Dockerfile.jobs.dockerignore"

sh -n "$builder" "$0"
python3 -m py_compile "$root/deploy/verify-private-image.py"
python3 -m unittest "$root/deploy/test_verify_private_image.py"
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -s sh "$builder" "$0"
fi
printf 'private local image release tests: ok\n'
