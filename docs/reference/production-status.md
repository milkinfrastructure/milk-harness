# Production qualification

Last updated: 2026-08-29.

## Current status

The gateway and harness source repositories are public. The current source remains a release candidate, not a deployed or production-qualified release, and all runtime OCI images remain private.

The candidate uses one stable eval identity across the manifest, gateway config, scheduler, and durable object keys: `MILK_EVAL_ID`, `manifest.campaign_id`, and `gateway_config.eval_id` are equal. The separately computed SHA-256 of the canonical outer eval document is the paid-work authorization boundary. Existing private image admissions predate this contract. New private images, admissions, production publication/readback, and complete cloud proof are therefore required before the candidate can be called released.

No paid teacher run or complete provider proof is recorded here. No local GPU has run or is required.

## Verified release-preparation evidence

- The MIT-licensed source repositories are public in GitHub. Runtime OCI packages remain private.
- Earlier source passed the offline gateway and harness gates, and a selective `linux/amd64` jobs build produced local admission, provenance, and SBOM evidence. That evidence is a build-system baseline only; it does not qualify the current candidate.
- Recorded compressed image sizes are 12.02 MiB for `milk-gateway`, 58.23 MiB for `milk-jobs`, 10,420.49 MiB for `milk-teacher-gpt-oss`, 6,371.95 MiB for `milk-student-train`, and 10,836.92 MiB for `milk-student-branch`.
- Seven production R2 buckets and the three GitHub production environments were created. Actions remained disabled when this evidence was collected.
- Modal teacher and student volumes were populated with the pinned external model files, hash-verified, and observed with zero active tasks. Model weights are not stored in the images.
- Baseten-primary/Modal-fallback policy is implemented: Modal is eligible only after a validated retryable Baseten failure and never after ambiguous or accepted Baseten create authority.
- Local evidence does not prove production R2 publication/readback, eval binding, provider pull, deployment, paid execution, route behavior, or provider zero.

Alpine cannot materially reduce the pinned CUDA, PyTorch, and vLLM layers in the GPU images. The CPU images are already small; the GPU images are large because of their runtime stacks, while model weights remain external.

## Hosted pilot boundary

The pilot is single tenant: one gateway deployment owns one tenant, project, environment, and workload scope. A customer points the official OpenAI SDK at that gateway and sends one `dt_live_...` Bearer key. That key authorizes chat traffic only.

Milk Infrastructure is the hosted operator and owns the admitted eval document, storage credentials, provider credentials, route authority, and release evidence. The harness watches that admitted document and completed captured traffic. It stops creating teacher work at the eval's `max_decisions` limit while continuing bounded reconciliation and teardown. Scheduled runs cannot authorize provider creates.

## Work required before activation

1. Rebuild all affected private images from the current published source commits, verify immutable revisions, and create fresh admissions and one release document.
2. Publish and read back the exact release, admissions, and eval documents in production evidence storage.
3. Finish least-privilege production storage credentials, privacy controls, Baseten resources, and deployment receipts.
4. Materialize a fresh one-request teacher qualification eval and a separate student-capable eval, each with its own stable campaign ID, exact outer-document approval digest, per-eval limits, and bounded budget.
5. Deploy the admitted gateway and enable the scheduler only after every secret, resource identity, and configuration digest is verified.

## Remaining live gates

1. The Cloudflare Worker and Container run the newly admitted gateway image on the production hostname.
2. An official OpenAI SDK request returns a valid completion through the gateway and its immutable completed trace is present.
3. The one-request teacher qualification produces one terminal result under the live Baseten-primary/Modal-fallback policy, followed by observed zero compute on both providers.
4. The student eval retains at least 251 usable results: 50 TRAIN, 73 DEV, and 128 CALIBRATION. Partitioning or skipped traffic may require more requests; generated traffic does not count.
5. One train/merge job and the BF16, dynamic-FP8, and static-FP8 branches complete against the same ordered DEV set.
6. A deterministic winner receives an authenticated 100-bps canary, and genuine candidate saturation falls back to the baseline.
7. Route control publishes and verifies the signed zero successor, removes the candidate credential, tears down the winner, and observes zero provider compute.
8. The final budget remains below the `$1,000` ceiling, and no new paid work starts at or above `$850`.
9. GitHub, R2, Cloudflare, Baseten, Modal, and release receipts agree on the exact eval, source, image, and deployment identities.
10. The public-release audit of repository history, assets, examples, install documentation, and GitHub settings is completed and its findings are recorded.

Anything short of these live gates is release preparation, not production qualification.
