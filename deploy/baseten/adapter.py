#!/usr/bin/env python3
import hashlib
import json
import re


MAX_GPU_SECONDS = 1_800
TERMINALIZATION_GRACE_SECONDS = 900
OUTER_TIMEOUT_SECONDS = MAX_GPU_SECONDS + TERMINALIZATION_GRACE_SECONDS
TEACHER_TERMINALIZATION_GRACE_SECONDS = 300
TEACHER_MODEL_SOURCE = (
    "hf://openai/gpt-oss-120b@b5c939de8f754692c1647ca79fbf85e8c1e70f8a"
)
TEACHER_MODEL_MOUNT = "/models/gpt-oss-120b"
TEACHER_MODEL_ALLOW_PATTERNS = (
    "LICENSE",
    "USAGE_POLICY",
    "chat_template.jinja",
    "config.json",
    "generation_config.json",
    *(f"model-{index:05d}-of-00014.safetensors" for index in range(15)),
    "model.safetensors.index.json",
    "special_tokens_map.json",
    "tokenizer.json",
    "tokenizer_config.json",
)
STUDENT_MODEL_SOURCE = (
    "hf://Qwen/Qwen3-4B-Instruct-2507@cdbee75f17c01a7cc42f958dc650907174af0554"
)
STUDENT_MODEL_MOUNT = "/model"
STUDENT_MODEL_ALLOW_PATTERNS = (
    ".gitattributes",
    "LICENSE",
    "README.md",
    "config.json",
    "generation_config.json",
    "merges.txt",
    "model-00001-of-00003.safetensors",
    "model-00002-of-00003.safetensors",
    "model-00003-of-00003.safetensors",
    "model.safetensors.index.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "vocab.json",
)
VARIANTS = ("bf16", "dynamic_fp8", "static_fp8")
HEX64 = re.compile(r"[0-9a-f]{64}")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
JOB_KEYS = {
    "id",
    "created_at",
    "current_status",
    "instance_type",
    "updated_at",
    "training_project_id",
    "training_project",
    "error_message",
    "node_count",
    "name",
    "checkpoint_sync_status",
    "priority",
    "availability_model",
    "user",
}
JOB_REQUIRED_KEYS = {
    "id",
    "created_at",
    "current_status",
    "instance_type",
    "updated_at",
    "training_project_id",
    "training_project",
    "node_count",
    "name",
    "availability_model",
}
INSTANCE_KEYS = {
    "id",
    "name",
    "memory_limit_mib",
    "millicpu_limit",
    "gpu_count",
    "gpu_type",
    "gpu_memory_limit_mib",
}


def _canonical(value):
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _strict_object(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    def invalid_constant(value):
        raise ValueError(f"invalid JSON constant {value}")

    def object_pairs(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("JSON object contains a duplicate key")
            value[key] = item
        return value

    value = json.loads(
        raw,
        object_pairs_hook=object_pairs,
        parse_constant=invalid_constant,
    )
    if not isinstance(value, dict):
        raise ValueError("JSON value must be an object")
    return value


def _is_hex64(value):
    return isinstance(value, str) and HEX64.fullmatch(value) is not None


def immutable_image(value):
    if not isinstance(value, str):
        raise ValueError("runtime image must be an immutable @sha256 reference")
    name, separator, digest = value.rpartition("@sha256:")
    if (
        not separator
        or not name
        or any(character.isspace() for character in name)
        or not _is_hex64(digest)
    ):
        raise ValueError("runtime image must be an immutable @sha256 reference")
    return value


def immutable_ghcr_image(value):
    value = immutable_image(value)
    if not value.startswith("ghcr.io/"):
        raise ValueError("runtime image must be an immutable ghcr.io @sha256 reference")
    return value


def identifier(value, label):
    if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{label} is invalid")
    return value


def _job_name(student_job_id, variant=None, branch_id=None):
    if variant is None:
        return f"dt-train-{student_job_id}"
    identity = hashlib.sha256(
        f"{student_job_id}:{variant}:{branch_id}".encode()
    ).hexdigest()
    return f"dt-{variant.replace('_', '-')}-{identity}"


def _start_command(stage):
    if stage == "train_merge":
        arguments = (
            'train --config /tmp/dragontales/gateway.json --student-job-id '
            '"$DRAGONTALES_STUDENT_JOB_ID" --work-dir /tmp/dragontales-work'
        )
    elif stage in VARIANTS:
        arguments = (
            'branch --config /tmp/dragontales/gateway.json --student-job-id '
            f'"$DRAGONTALES_STUDENT_JOB_ID" --variant {stage} '
            "--work-dir /tmp/dragontales-work"
        )
    else:
        raise ValueError("student stage is invalid")
    return "\n".join(
        (
            '[ "${BT_RETRY_COUNT:-}" = "0" ] || '
            '{ printf "%s\\n" "BT_RETRY_COUNT must be zero" >&2; exit 75; }',
            "umask 077",
            "mkdir -m 0700 /tmp/dragontales",
            'printf "%s" "$DRAGONTALES_CONFIG_JSON" > /tmp/dragontales/gateway.json',
            "chmod 0400 /tmp/dragontales/gateway.json",
            "unset DRAGONTALES_CONFIG_JSON",
            f"exec /usr/bin/timeout --signal=TERM --kill-after=30s {OUTER_TIMEOUT_SECONDS}s "
            f"/opt/dragontales/deploy/student-job.sh {arguments}",
        )
    )


def _teacher_start_command(max_gpu_seconds):
    return "\n".join(
        (
            '[ "${BT_RETRY_COUNT:-}" = "0" ] || '
            '{ printf "%s\\n" "BT_RETRY_COUNT must be zero" >&2; exit 75; }',
            "umask 077",
            "mkdir -m 0700 /tmp/dragontales",
            'printf "%s" "$DRAGONTALES_CONFIG_JSON" > /tmp/dragontales/gateway.json',
            "chmod 0400 /tmp/dragontales/gateway.json",
            "unset DRAGONTALES_CONFIG_JSON",
            "exec /usr/bin/timeout --signal=TERM "
            f"--kill-after={TEACHER_TERMINALIZATION_GRACE_SECONDS}s "
            f"{max_gpu_seconds}s "
            "/opt/dragontales/job.sh --config /tmp/dragontales/gateway.json "
            '--teacher-run-id "$DRAGONTALES_TEACHER_RUN_ID"',
        )
    )


def _secret(name):
    return {"name": name}


def _store_environment(settings, include_capture):
    environment = {
        "MILK_CONTROL_STORE_ACCESS_KEY_ID": _secret(
            settings["control_store_access_key_secret"]
        ),
        "MILK_CONTROL_STORE_SECRET_ACCESS_KEY": _secret(
            settings["control_store_secret_key_secret"]
        ),
    }
    control_session = settings.get("control_store_session_token_secret")
    if control_session is not None:
        environment["MILK_CONTROL_STORE_SESSION_TOKEN"] = _secret(control_session)
    if include_capture:
        environment.update(
            {
                "MILK_CAPTURE_STORE_ACCESS_KEY_ID": _secret(
                    settings["capture_store_access_key_secret"]
                ),
                "MILK_CAPTURE_STORE_SECRET_ACCESS_KEY": _secret(
                    settings["capture_store_secret_key_secret"]
                ),
            }
        )
        capture_session = settings.get("capture_store_session_token_secret")
        if capture_session is not None:
            environment["MILK_CAPTURE_STORE_SESSION_TOKEN"] = _secret(capture_session)
    return environment


def _request_body(settings, student_job_id, variant=None, branch_id=None):
    name = _job_name(student_job_id, variant, branch_id)
    stage = "train_merge" if variant is None else variant
    environment = {
        **_store_environment(settings, False),
        "DRAGONTALES_CONFIG_JSON": _secret(settings["config_secret"]),
        "DRAGONTALES_STUDENT_JOB_ID": student_job_id,
    }
    body = {
        "training_job": {
            "image": {
                "base_image": settings[
                    "student_train_image" if variant is None else "student_branch_image"
                ],
                "docker_auth": {
                    "registry": "ghcr.io",
                    "auth_method": "REGISTRY_SECRET",
                    "registry_secret_docker_auth": {
                        "secret_ref": _secret(settings["registry_secret"])
                    },
                },
            },
            "compute": {
                "node_count": 1,
                "cpu_count": 8,
                "memory": "64Gi",
                "accelerator": {"accelerator": "H100", "count": 1},
                "availability_model": "dedicated",
            },
            "runtime": {
                "start_commands": [_start_command(stage)],
                "environment_variables": environment,
                "cache_config": {"enabled": False},
                "checkpointing_config": {"enabled": False},
            },
            "name": name,
            "enable_baseten_workdir": False,
            "priority": 0,
        }
    }
    if variant is None:
        body["training_job"]["weights"] = [
            {
                "source": STUDENT_MODEL_SOURCE,
                "mount_location": STUDENT_MODEL_MOUNT,
                "allow_patterns": list(STUDENT_MODEL_ALLOW_PATTERNS),
            }
        ]
    return name, body


def _teacher_request_body(settings, launch):
    teacher_run_id = launch["teacher_run_id"]
    name = f"dt-teacher-{teacher_run_id}"
    environment = {
        **_store_environment(settings, True),
        "DRAGONTALES_CONFIG_JSON": _secret(settings["config_secret"]),
        "DRAGONTALES_TEACHER_RUN_ID": teacher_run_id,
    }
    return name, {
        "training_job": {
            "image": {
                "base_image": settings["teacher_image"],
                "docker_auth": {
                    "registry": "ghcr.io",
                    "auth_method": "REGISTRY_SECRET",
                    "registry_secret_docker_auth": {
                        "secret_ref": _secret(settings["registry_secret"])
                    },
                },
            },
            "compute": {
                "node_count": 1,
                "cpu_count": 8,
                "memory": "64Gi",
                "accelerator": {"accelerator": "H100", "count": 1},
                "availability_model": "dedicated",
            },
            "runtime": {
                "start_commands": [_teacher_start_command(launch["max_gpu_seconds"])],
                "environment_variables": environment,
                "cache_config": {"enabled": False},
                "checkpointing_config": {"enabled": False},
            },
            "name": name,
            "weights": [
                {
                    "source": TEACHER_MODEL_SOURCE,
                    "mount_location": TEACHER_MODEL_MOUNT,
                    "allow_patterns": list(TEACHER_MODEL_ALLOW_PATTERNS),
                }
            ],
            "enable_baseten_workdir": False,
            "priority": 0,
        }
    }



def _provider_execution(
    raw,
    expected_project,
    expected_name,
    expected_id=None,
    allowed_statuses=frozenset({"TRAINING_JOB_PENDING", "TRAINING_JOB_CREATED"}),
):
    value = _strict_object(raw)
    if set(value) != {"training_job"} or not isinstance(value["training_job"], dict):
        raise ValueError("Baseten response is not a typed training job")
    job = value["training_job"]
    if (
        not JOB_REQUIRED_KEYS.issubset(job)
        or not set(job).issubset(JOB_KEYS)
        or identifier(job.get("id"), "Baseten training job ID") != job["id"]
        or (expected_id is not None and job["id"] != expected_id)
        or job.get("training_project_id") != expected_project
        or job.get("name") != expected_name
        or type(job.get("node_count")) is not int
        or job["node_count"] != 1
        or job.get("availability_model") != "dedicated"
        or (
            job.get("current_status") not in allowed_statuses
            if allowed_statuses is not None
            else not isinstance(job.get("current_status"), str)
            or not job["current_status"]
        )
        or not isinstance(job.get("created_at"), str)
        or not job["created_at"]
        or not isinstance(job.get("updated_at"), str)
        or not job["updated_at"]
    ):
        raise ValueError("Baseten training job identity is invalid")
    project = job["training_project"]
    if (
        not isinstance(project, dict)
        or set(project) != {"id", "name"}
        or project.get("id") != expected_project
        or not isinstance(project.get("name"), str)
        or not project["name"]
    ):
        raise ValueError("Baseten training project identity is invalid")
    instance = job["instance_type"]
    if (
        not isinstance(instance, dict)
        or set(instance) != INSTANCE_KEYS
        or instance.get("gpu_type") != "H100"
        or type(instance.get("gpu_count")) is not int
        or instance["gpu_count"] != 1
        or any(
            type(instance.get(key)) is not int or instance[key] <= 0
            for key in ("memory_limit_mib", "millicpu_limit", "gpu_memory_limit_mib")
        )
        or not isinstance(instance.get("id"), str)
        or not instance["id"]
        or not isinstance(instance.get("name"), str)
        or not instance["name"]
    ):
        raise ValueError("Baseten training job hardware is invalid")
    return {"execution_id": job["id"], "execution_name": expected_name}
