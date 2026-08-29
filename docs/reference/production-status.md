# Production qualification

Last updated: 2026-08-29.

## Current status

The gateway and harness functional source commits are `9a0afa8fbf9ad6b5c49e050e27b1cf23cf6ddac2` and `0eebb1aa0326b1cf13403b885295b99cc02fd71b`; both are public, and both repositories' post-merge offline CI and CodeQL runs are green. Image release v5 selectively rebuilds only `milk-jobs` from the harness functional commit. It retains the initial v4 gateway and GPU images bound to gateway `bc1b53c45c337d95daa38cd8170da46c246e5a70` and harness `aa294358bede782d1e533fc1f6432615b5366a82`. The hosted system is not deployed or production-qualified, and runtime OCI images remain private.

The candidate uses one stable eval identity across the manifest, gateway config, scheduler, and durable object keys: `MILK_EVAL_ID`, `manifest.campaign_id`, and `gateway_config.eval_id` are equal. The separately computed SHA-256 of the canonical outer eval document is the paid-work authorization boundary. Private-image build, admission, and Cloudflare publication plus metadata/ETag readback of the five v5 release objects are complete; least-privilege S3 body readback, exact eval publication/readback, and the complete cloud proof remain required before the candidate can be called released.

No paid teacher run or complete provider proof is recorded here. No local GPU has run or is required.

## Verified release-preparation evidence

- The MIT-licensed source repositories are public in GitHub. Runtime OCI packages remain private.
- Released functional commits `9a0afa8fbf9ad6b5c49e050e27b1cf23cf6ddac2` for the gateway and `0eebb1aa0326b1cf13403b885295b99cc02fd71b` for the harness passed offline CI and CodeQL.
- A CPU-only native builder produced and independently validated the initial v4 set of five private `linux/amd64` images, their OCI manifests, SLSA provenance, SPDX SBOMs, admission receipts, and immutable source bindings.
- A selective v5 jobs-only rebuild from harness `0eebb1aa0326b1cf13403b885295b99cc02fd71b` completed in 3 minutes 12 seconds with no local GPU and no active builder left behind. Its release SHA-256 is `cd8a756a384780977a0f9c33cff17297844ed94562bb1bb5f6cd2878d20bf30c`, jobs admission is `a6e3b0bb7a09f9db31cd76b9c0238655141391c9c63e969c5175306112f30790`, and jobs context is `0dbd7543c82077868a4b6a4440be4dfc0d323ccfeb3eff22e2ccf08d1b69b536`.
- Exact v5 images are `milk-gateway@sha256:f5fd6786a5d36870045c9fc8271ac28940ae88809569f5c3fb8fbb2d2582ca4c`, `milk-jobs@sha256:c80c438845462cf2d3dbe74d49bb08a1c8b109b5ff83f551619271cc92e59260`, `milk-teacher-gpt-oss@sha256:d07a0f794e3a273d4c883606c2fa44728925c2a97379507d35a164b2c9293016`, `milk-student-train@sha256:74890a2b0524ac80067d6ffb499a176e5d70e735c1feddb825b25716de1092c4`, and `milk-student-branch@sha256:d8588f986aaaeefa34b32a43c3970579bb174b975517513ad3b741d73727e948`. Only the jobs digest changed from v4.
- Recorded compressed sizes are 12.05 MiB for `milk-gateway`, 58.23 MiB for `milk-jobs`, 10,420.52 MiB for `milk-teacher-gpt-oss`, 6,371.98 MiB for `milk-student-train`, and 10,836.95 MiB for `milk-student-branch`. Shared vLLM layers reduce unique content across all images to 16.88 GiB.
- Seven production R2 buckets and the three protected-main-only GitHub production environments exist. Live readback from Cloudflare account `d8a5175f959d3dbd4084db9fcab1c44c` confirms that all seven `milk-prod-*` buckets remain private; six are empty and `milk-prod-evidence` contains exactly five objects.
- All seven buckets have an enabled one-day (`86400` seconds) abort-incomplete-multipart lifecycle. `milk-prod-ops-log` has an enabled 90-day (`7776000` seconds) lock and 90-day deletion lifecycle on `operational/v1/scheduler-passes/`; `milk-prod-evidence` has an enabled indefinite lock on `operational-log-references/v1/scheduler-passes/`; and `milk-prod-route-evidence` has an enabled indefinite whole-bucket lock.
- Four artifact-admission objects ending in `b6cbbedd71ecfe64873045655a09931d0b86ac67a450e5263459fa946e543290`, `14b76ef0f21a6d599fe4d8d3af99088964325494e202468c716239a69fd66c8b`, `0e36e77a9f26d86f189e1b4e98a10978be9e6e950d704691aedbf3bec832d18b`, and `a6e3b0bb7a09f9db31cd76b9c0238655141391c9c63e969c5175306112f30790`, plus the v5 release object ending in `cd8a756a384780977a0f9c33cff17297844ed94562bb1bb5f6cd2878d20bf30c`, are published in `milk-prod-evidence`. Cloudflare returned HTTP 200 for every upload. Independent list and metadata readback returned exactly those five keys with exact local byte sizes and matching single-part ETags/MD5; every content-addressed key suffix equals the local SHA-256. This proves publication and metadata/ETag readback, not least-privilege S3 credential or body readback.
- The production workflow remains disabled. Production least-privilege credentials, eval variables, deployment, provider pull, and paid proof are still missing.
- Modal teacher and student volumes were populated with the pinned external model files, hash-verified, and observed with zero active tasks. Model weights are not stored in the images.
- Baseten-primary/Modal-fallback policy is implemented: Modal is eligible only after a validated retryable Baseten failure and never after ambiguous or accepted Baseten create authority.
- Current evidence does not prove least-privilege production S3 body readback, eval binding, provider pull, deployment, paid execution, route behavior, or provider zero.

Alpine cannot materially reduce the pinned CUDA, PyTorch, and vLLM layers in the GPU images. The CPU images are already small; the GPU images are large because of their runtime stacks, while model weights remain external.

## Hosted pilot boundary

The pilot is single tenant: one gateway deployment owns one tenant, project, environment, and workload scope. A customer points the official OpenAI SDK at that gateway and sends one `dt_live_...` Bearer key. That key authorizes chat traffic only.

Milk Infrastructure is the hosted operator and owns the admitted eval document, storage credentials, provider credentials, route authority, and release evidence. The harness watches that admitted document and completed captured traffic. It stops creating teacher work at the eval's `max_decisions` limit while continuing bounded reconciliation and teardown. Scheduled runs cannot authorize provider creates.

One paid eval is planned but not yet executed. The synthetic cloud-mechanics eval uses ID `959caacb397004bf3e60f13613da50f4ed3160a65d18b178c3d996398e29b5a0`, 320 decisions partitioned as 63 TRAIN, 91 DEV, and 166 CALIBRATION, `max_calls=20`, `max_gpu_seconds=3600`, and `max_parallel_runs=1`. No later create is authorized until a Baseten-selected 20-call job proves the exact private image and profile, 20 ready calls within the live target, `logs_source_complete=true`, `oom_source_complete=true`, `oom_matched_record_count=0`, terminal zero compute, and no ambiguous or not-started call. A Modal-selected result cannot pass the Baseten gate. With a 3,600-second winner bound, the eval's pessimistic GPU reservation is `$160`. This proves cloud mechanics only; generated traffic and results do not satisfy real-traffic production qualification. The existing `$1,000` ceiling and `$850` new-work cutoff govern the run.

## Work required before activation

1. Create least-privilege production storage credentials, body-read the five published release objects through S3, and publish and read back the exact eval documents.
2. Create provider resources and record deployment receipts. Continue verifying the installed retention and privacy controls by live readback.
3. Materialize the exact bounded cloud-mechanics eval above with its outer-document approval digest, 3,600-second winner bound, limits, admission gate, and budget.
4. Deploy the admitted gateway and enable the scheduler only after every secret, resource identity, and configuration digest is verified.

## Remaining live gates

1. The Cloudflare Worker and Container run the newly admitted gateway image on the production hostname.
2. An official OpenAI SDK request returns a valid completion through the gateway and its immutable completed trace is present.
3. The first Baseten-selected 20-call job pulls the exact private image, passes the committed live teacher profile, and returns to zero compute before another create is authorized.
4. The student eval retains at least 251 usable results: 50 TRAIN, 73 DEV, and 128 CALIBRATION. The current partition should plan for roughly 1,280 eligible captures; skipped traffic may require more. Generated traffic and local fixtures do not count.
5. One train/merge job and the BF16, dynamic-FP8, and static-FP8 branches complete against the same ordered DEV set.
6. A deterministic winner receives an authenticated 100-bps canary, and genuine candidate saturation falls back to the baseline.
7. Route control publishes and verifies the signed zero successor, removes the candidate credential, tears down the winner, and observes zero provider compute.
8. The final budget remains below the `$1,000` ceiling, and no new paid work starts at or above `$850`.
9. GitHub, R2, Cloudflare, Baseten, Modal, and release receipts agree on the exact eval, source, image, and deployment identities.
10. The public-release audit of repository history, assets, examples, install documentation, and GitHub settings is completed and its findings are recorded.

Anything short of these live gates is release preparation, not production qualification.
