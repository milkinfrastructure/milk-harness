#!/bin/sh
set -eu
umask 077

GATEWAY_REPOSITORY=ghcr.io/milkinfrastructure/milk-gateway
SOURCE_REPOSITORY=https://github.com/milkinfrastructure/milk-harness
BUILDKIT_IMAGE=moby/buildkit@sha256:ddd1ca44b21eda906e81ab14a3d467fa6c39cd73b9a39df1196210edcb8db59e
BUILDKIT_VERSION=v0.23.2
DOCKERFILE_FRONTEND=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

fail() {
  printf 'build-images: %s\n' "$1" >&2
  exit "${2:-1}"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is unavailable" 69
}

[ "$#" -eq 2 ] || fail 'usage: build-images.sh GATEWAY_IMAGE NEW_EVIDENCE_DIR' 64
gateway_image=$1
requested_evidence_dir=$2

case "$gateway_image" in
  "$GATEWAY_REPOSITORY"@sha256:*) ;;
  *) fail "gateway image must be an immutable $GATEWAY_REPOSITORY@sha256 reference" 64 ;;
esac
gateway_digest=${gateway_image##*@sha256:}
[ "${#gateway_digest}" -eq 64 ] || fail 'gateway image digest must contain 64 lowercase hex characters' 64
case "$gateway_digest" in
  *[!0-9a-f]*) fail 'gateway image digest must contain 64 lowercase hex characters' 64 ;;
esac

case "$requested_evidence_dir" in
  /*) ;;
  *) fail 'evidence directory must be absolute' 64 ;;
esac
[ ! -e "$requested_evidence_dir" ] || fail 'evidence directory must be new' 64
evidence_parent=$(dirname -- "$requested_evidence_dir")
[ -d "$evidence_parent" ] || fail 'evidence directory parent does not exist' 64
evidence_parent=$(CDPATH= cd -- "$evidence_parent" && pwd -P)
evidence_dir=$evidence_parent/$(basename -- "$requested_evidence_dir")

for command_name in date docker env gh git grep ln python3 sed tar; do
  require_command "$command_name"
done

credential_names=$(env | sed 's/=.*//' | LC_ALL=C sort)
if printf '%s\n' "$credential_names" | grep -Eq \
  '^(AWS_|AZURE_|GCP_|BASETEN_|MODAL_|OPENAI_|R2_|CLOUDFLARE_|TEACHER_|MILK_TEACHER_|WANDB_|DRAGONTALES_|DOCKER_|BUILDX_|BUILDKIT_).*|^(GOOGLE_APPLICATION_CREDENTIALS|HF_TOKEN|HUGGING_FACE_HUB_TOKEN|NVIDIA_API_KEY|NGC_API_KEY|CODEX_API_KEY|CODEX_AUTH_TOKEN|CODEX_TOKEN|GH_TOKEN|GITHUB_TOKEN|GH_ENTERPRISE_TOKEN|GITHUB_ENTERPRISE_TOKEN|CR_PAT|CI_JOB_TOKEN|CI_REGISTRY_PASSWORD|NPM_TOKEN|PYPI_TOKEN|PIP_INDEX_URL|PIP_EXTRA_INDEX_URL|REGISTRY_AUTH_FILE|HTTP_PROXY|HTTPS_PROXY|FTP_PROXY|ALL_PROXY|NO_PROXY|http_proxy|https_proxy|ftp_proxy|all_proxy|no_proxy)$|^CARGO_REGISTRIES_.*_TOKEN$|^MILK_(CAPTURE|CONTROL|EVIDENCE|ROUTE|CREATE_AUTHORITY_(READ|WRITE))_(R2|STORE)_|^MILK_.*STORE.*(ACCESS|KEY|SECRET|TOKEN|CREDENTIAL)'; then
  fail 'build shell contains an ambient registry, provider, store, teacher, OpenAI, or Codex credential/configuration' 64
fi

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$repo"
top_level=$(git rev-parse --show-toplevel 2>/dev/null) || fail 'release source is not a git checkout' 64
[ "$top_level" = "$repo" ] || fail 'release script must run from the milk-harness checkout' 64
commit=$(git rev-parse --verify 'HEAD^{commit}' 2>/dev/null) || fail 'release checkout has no commit' 64
[ "${#commit}" -eq 40 ] || fail 'source commit must be a full SHA-1' 64
case "$commit" in
  *[!0-9a-f]*) fail 'source commit must be a full lowercase SHA-1' 64 ;;
esac
source_epoch=$(git show -s --format=%ct "$commit" 2>/dev/null) || fail 'cannot derive source commit time' 64
case "$source_epoch" in
  ''|*[!0-9]*) fail 'source commit time is invalid' 64 ;;
esac
[ -z "$(git status --porcelain=v1 --untracked-files=all)" ] || fail 'release checkout must be clean' 64
origin=$(git remote get-url origin 2>/dev/null) || fail 'release checkout has no origin' 64
case "$origin" in
  git@github.com:milkinfrastructure/milk-harness.git|https://github.com/milkinfrastructure/milk-harness.git) ;;
  *) fail 'origin must be milkinfrastructure/milk-harness' 64 ;;
esac
remote_head=$(git ls-remote --exit-code origin HEAD 2>/dev/null) || \
  fail 'cannot resolve origin HEAD' 64
set -- $remote_head
[ "$#" -eq 2 ] && [ "$1" = "$commit" ] && [ "$2" = HEAD ] || \
  fail 'local HEAD must equal the published origin HEAD' 64
case "$evidence_dir/" in
  "$repo"/*) fail 'evidence directory must be outside the release checkout' 64 ;;
esac

for release_input in \
  deploy/student/Dockerfile \
  deploy/teacher/gpt-oss-120b/Dockerfile \
  Dockerfile.planner \
  Dockerfile.jobs; do
  [ -f "$release_input" ] || fail "missing release input $release_input" 66
done

docker=$(command -v docker)
gh=$(command -v gh)
python=$(command -v python3)
context=$("$docker" context show 2>/dev/null) || fail 'cannot resolve the Docker context' 69
[ -n "$context" ] || fail 'Docker context is empty' 69
endpoint=$("$docker" context inspect "$context" --format '{{ (index .Endpoints "docker").Host }}' 2>/dev/null) || \
  fail 'cannot inspect the Docker context' 69
case "$endpoint" in
  unix://*|npipe://*) ;;
  *) fail 'Docker context must use a local socket' 64 ;;
esac
"$docker" buildx version >/dev/null 2>&1 || fail 'docker buildx is unavailable' 69
buildx_plugin=
if [ -n "${HOME:-}" ] && [ -x "$HOME/.docker/cli-plugins/docker-buildx" ]; then
  buildx_plugin=$HOME/.docker/cli-plugins/docker-buildx
else
  for candidate in \
    /opt/homebrew/lib/docker/cli-plugins/docker-buildx \
    /usr/local/lib/docker/cli-plugins/docker-buildx \
    /usr/libexec/docker/cli-plugins/docker-buildx \
    /usr/lib/docker/cli-plugins/docker-buildx; do
    if [ -x "$candidate" ]; then
      buildx_plugin=$candidate
      break
    fi
  done
fi
[ -n "$buildx_plugin" ] || fail 'cannot locate the docker buildx plugin executable' 69

mkdir -m 0700 -- "$evidence_dir" || fail 'could not create evidence directory' 73
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
failure_stage=initialization
release_complete=0
builder_created=0
scratch=
docker_config=
builder=

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ "$status" -ne 0 ] && [ "$release_complete" -eq 0 ]; then
    completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf 'unknown')
    "$python" - "$evidence_dir/failure.json" "$commit" "$failure_stage" \
      "$status" "$started_at" "$completed_at" <<'PY' >/dev/null 2>&1 || :
import json
import sys
from pathlib import Path

path, commit, stage, status, started_at, completed_at = sys.argv[1:]
Path(path).write_text(json.dumps({
    "schema_version": "milk.private-harness-release-failure.v1",
    "source_commit": commit,
    "stage": stage,
    "exit_code": int(status),
    "started_at": started_at,
    "completed_at": completed_at,
}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
  fi
  if [ "$builder_created" -eq 1 ]; then
    "$docker" --config "$docker_config" buildx rm --force "$builder" >/dev/null 2>&1 || :
  fi
  case "$scratch" in
    "${TMPDIR:-/tmp}"/milk-harness-release.*) rm -rf -- "$scratch" ;;
  esac
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

scratch=$(mktemp -d "${TMPDIR:-/tmp}/milk-harness-release.XXXXXX") || \
  fail 'cannot create release scratch directory' 73
docker_config=$scratch/docker-config
mkdir -m 0700 -- "$docker_config" || fail 'cannot create isolated Docker configuration' 73
mkdir -m 0700 -- "$docker_config/cli-plugins" || fail 'cannot create isolated Docker plugin directory' 73
ln -s -- "$buildx_plugin" "$docker_config/cli-plugins/docker-buildx" || \
  fail 'cannot install docker buildx into the isolated configuration' 73
"$docker" --config "$docker_config" buildx version >/dev/null 2>&1 || \
  fail 'docker buildx is unavailable in the isolated configuration' 69
builder=milk-harness-$(printf '%s' "$commit" | cut -c1-12)-$$

failure_stage=source-context
source_context=$scratch/source-context.tar
git archive --format=tar --output="$source_context" "$commit" || \
  fail 'cannot materialize the committed source context' 70
context_sha256=$(
  "$python" - "$source_context" <<'PY'
import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
) || fail 'cannot hash the committed source context' 70
build_context=$scratch/context
mkdir -m 0700 -- "$build_context" || fail 'cannot create the committed build context' 73
tar -xf "$source_context" -C "$build_context" || \
  fail 'cannot extract the committed build context' 70
rm -f -- "$source_context"

"$python" - "$evidence_dir/input.json" "$commit" "$source_epoch" "$gateway_image" \
  "$context_sha256" "$BUILDKIT_IMAGE" "$DOCKERFILE_FRONTEND" "$started_at" <<'PY'
import json
import sys
from pathlib import Path

path, commit, source_epoch, gateway, context_sha256, buildkit, frontend, started_at = sys.argv[1:]
Path(path).write_text(json.dumps({
    "schema_version": "milk.private-harness-release-input.v1",
    "source_commit": commit,
    "source_date_epoch": int(source_epoch),
    "source_repository": "https://github.com/milkinfrastructure/milk-harness",
    "source_context_method": "git-archive-tar-v1",
    "source_context_sha256": context_sha256,
    "gateway_image_reference": gateway,
    "buildkit_image_reference": buildkit,
    "dockerfile_frontend_reference": frontend,
    "platform": "linux/amd64",
    "started_at": started_at,
}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY

failure_stage=registry-login
if ! "$gh" auth token --hostname github.com | \
  "$docker" --config "$docker_config" login ghcr.io --username ShantanuJoshi --password-stdin >/dev/null; then
  fail 'private GHCR login failed' 77
fi

failure_stage=package-preflight
packages=$scratch/packages.tsv
"$gh" api --hostname github.com --paginate \
  '/orgs/milkinfrastructure/packages?package_type=container&per_page=100' \
  --jq '.[] | [.name, .visibility] | @tsv' >"$packages" || fail 'cannot inspect Milk container packages' 77
"$python" - "$packages" <<'PY' || fail 'gateway package is missing/private check failed, or an existing release target is not private' 77
import sys
from pathlib import Path

targets = {"milk-student", "milk-teacher-gpt-oss", "milk-planner", "milk-jobs"}
seen = {}
for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    fields = line.split("\t")
    if len(fields) != 2:
        raise SystemExit(1)
    name, visibility = fields
    if name in seen:
        raise SystemExit(1)
    seen[name] = visibility
if seen.get("milk-gateway") != "private":
    raise SystemExit(1)
for name in targets & seen.keys():
    if seen[name] != "private":
        raise SystemExit(1)
PY

failure_stage=builder-create
"$docker" --config "$docker_config" buildx create \
  --name "$builder" \
  --driver docker-container \
  --driver-opt "image=$BUILDKIT_IMAGE" \
  "$context" >/dev/null || fail 'cannot create the pinned local BuildKit builder' 70
builder_created=1
bootstrap_log=$scratch/builder.log
failure_stage=builder-bootstrap
if ! "$docker" --config "$docker_config" buildx inspect "$builder" --bootstrap >"$bootstrap_log" 2>&1; then
  cat "$bootstrap_log" >&2 || :
  fail 'cannot bootstrap the pinned local BuildKit builder' 70
fi
cat "$bootstrap_log" || :
builder_driver=$(sed -n 's/^Driver:[[:space:]]*//p' "$bootstrap_log")
builder_endpoint=$(sed -n 's/^Endpoint:[[:space:]]*//p' "$bootstrap_log")
builder_version=$(sed -n 's/^BuildKit version:[[:space:]]*//p' "$bootstrap_log")
[ "$builder_driver" = docker-container ] || fail 'builder driver is not docker-container' 70
[ "$builder_endpoint" = "$context" ] || fail 'builder endpoint differs from the local Docker context' 70
[ "$builder_version" = "$BUILDKIT_VERSION" ] || fail "builder version differs from $BUILDKIT_VERSION" 70
"$python" - "$bootstrap_log" "$evidence_dir/builder.json" "$BUILDKIT_IMAGE" \
  "$BUILDKIT_VERSION" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

source, target, image, version = sys.argv[1:]
raw = Path(source).read_bytes()
Path(target).write_text(json.dumps({
    "schema_version": "milk.local-builder-observation.v1",
    "authority": "local-socket",
    "driver": "docker-container",
    "buildkit_image_reference": image,
    "buildkit_version": version,
    "observation_sha256": hashlib.sha256(raw).hexdigest(),
    "observation_bytes": len(raw),
}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
rm -f -- "$bootstrap_log"

references=$scratch/references.tsv
: >"$references"

build_one() {
  artifact=$1
  dockerfile=$2
  repository=$3
  gateway_required=$4
  package=${repository##*/}
  artifact_dir=$evidence_dir/$artifact
  mkdir -m 0700 -- "$artifact_dir"
  tagged=$repository:source-$commit
  metadata=$artifact_dir/metadata.json
  build_log=$scratch/$artifact.build.log
  build_started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  set -- \
    --builder "$builder" \
    --platform linux/amd64 \
    --file "$build_context/$dockerfile" \
    --label "org.opencontainers.image.revision=$commit" \
    --label "org.opencontainers.image.source=$SOURCE_REPOSITORY" \
    --provenance=mode=max,version=v1 \
    --sbom=true \
    --build-arg "BUILDKIT_SYNTAX=$DOCKERFILE_FRONTEND" \
    --build-arg "MILK_SOURCE_CONTEXT_SHA256=$context_sha256" \
    --build-arg "SOURCE_DATE_EPOCH=$source_epoch" \
    --metadata-file "$metadata" \
    --tag "$tagged" \
    --push
  if [ "$gateway_required" = yes ]; then
    set -- "$@" --build-arg "MILK_GATEWAY_IMAGE=$gateway_image"
  fi
  set -- "$@" "$build_context"

  failure_stage=build-$artifact
  set +e
  "$docker" --config "$docker_config" buildx build "$@" >"$build_log" 2>&1
  build_status=$?
  set -e
  build_completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  cat "$build_log" || :
  "$python" - "$build_log" "$artifact_dir/build-log.json" "$artifact" \
    "$build_status" "$build_started_at" "$build_completed_at" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

source, target, artifact, status, started_at, completed_at = sys.argv[1:]
raw = Path(source).read_bytes()
Path(target).write_text(json.dumps({
    "schema_version": "milk.content-free-build-log.v1",
    "artifact": artifact,
    "exit_code": int(status),
    "started_at": started_at,
    "completed_at": completed_at,
    "sha256": hashlib.sha256(raw).hexdigest(),
    "bytes": len(raw),
    "content_retained": False,
}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
  rm -f -- "$build_log"
  [ "$build_status" -eq 0 ] || fail "$artifact image build failed" 70

  failure_stage=verify-$artifact
  set -- "$repo/deploy/verify-private-image.py" \
    --artifact "$artifact" \
    --tagged-reference "$tagged" \
    --source-commit "$commit" \
    --source-date-epoch "$source_epoch" \
    --source-context-sha256 "$context_sha256" \
    --metadata "$metadata" \
    --docker-config "$docker_config" \
    --evidence-dir "$artifact_dir" \
    --registry-token-stdin
  if [ "$gateway_required" = yes ]; then
    set -- "$@" --gateway-image-reference "$gateway_image"
  fi
  immutable=$(
    "$gh" auth token --hostname github.com | "$python" "$@"
  ) || fail "$artifact immutable manifest verification failed" 70
  set -- $immutable
  [ "$#" -eq 2 ] || fail "$artifact admission receipt is invalid" 70
  immutable=$1
  admission_sha256=$2
  printf '%s\t%s\t%s\n' "$artifact" "$immutable" "$admission_sha256" >>"$references"
  printf '%s\n' "$immutable"
}

build_one student deploy/student/Dockerfile \
  ghcr.io/milkinfrastructure/milk-student yes
build_one teacher-gpt-oss deploy/teacher/gpt-oss-120b/Dockerfile \
  ghcr.io/milkinfrastructure/milk-teacher-gpt-oss yes
build_one planner Dockerfile.planner \
  ghcr.io/milkinfrastructure/milk-planner no
build_one jobs Dockerfile.jobs \
  ghcr.io/milkinfrastructure/milk-jobs no

failure_stage=release-receipt
completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
"$python" - "$references" "$evidence_dir/release.json" "$commit" "$source_epoch" \
  "$gateway_image" "$BUILDKIT_IMAGE" "$DOCKERFILE_FRONTEND" "$started_at" "$completed_at" <<'PY'
import json
import re
import sys
from pathlib import Path

references_path, receipt_path, commit, source_epoch, gateway, buildkit, frontend, started_at, completed_at = sys.argv[1:]
expected = {
    "student": "ghcr.io/milkinfrastructure/milk-student",
    "teacher-gpt-oss": "ghcr.io/milkinfrastructure/milk-teacher-gpt-oss",
    "planner": "ghcr.io/milkinfrastructure/milk-planner",
    "jobs": "ghcr.io/milkinfrastructure/milk-jobs",
}
digest = re.compile(r"sha256:[0-9a-f]{64}\Z")
sha256 = re.compile(r"[0-9a-f]{64}\Z")
images = []
for line in Path(references_path).read_text(encoding="utf-8").splitlines():
    fields = line.split("\t")
    if len(fields) != 3 or fields[0] not in expected:
        raise SystemExit(1)
    prefix = expected[fields[0]] + "@"
    if not fields[1].startswith(prefix) or digest.fullmatch(fields[1].removeprefix(prefix)) is None:
        raise SystemExit(1)
    if sha256.fullmatch(fields[2]) is None:
        raise SystemExit(1)
    images.append({
        "admission_sha256": fields[2],
        "artifact": fields[0],
        "image_reference": fields[1],
    })
if [item["artifact"] for item in images] != list(expected):
    raise SystemExit(1)
Path(receipt_path).write_text(json.dumps({
    "schema_version": "milk.private-harness-release.v1",
    "source_commit": commit,
    "source_date_epoch": int(source_epoch),
    "source_repository": "https://github.com/milkinfrastructure/milk-harness",
    "gateway_image_reference": gateway,
    "buildkit_image_reference": buildkit,
    "dockerfile_frontend_reference": frontend,
    "build_authority": "local-socket",
    "platform": "linux/amd64",
    "images": images,
    "started_at": started_at,
    "completed_at": completed_at,
}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
release_complete=1
printf 'private harness image release verified at %s\n' "$evidence_dir"
