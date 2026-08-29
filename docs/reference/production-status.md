# Production qualification

Last updated: 2026-08-29.

## Current status

The gateway and harness source repositories are public. Exact current-source private images have passed native `linux/amd64` build and admission, but the hosted system is not deployed or production-qualified. Runtime OCI images remain private.

The candidate uses one stable eval identity across the manifest, gateway config, scheduler, and durable object keys: `MILK_EVAL_ID`, `manifest.campaign_id`, and `gateway_config.eval_id` are equal. The separately computed SHA-256 of the canonical outer eval document is the paid-work authorization boundary. Private-image build and admission are complete; production publication/readback and the complete cloud proof remain required before the candidate can be called released.

No paid teacher run or complete provider proof is recorded here. No local GPU has run or is required.

## Verified release-preparation evidence

- The MIT-licensed source repositories are public in GitHub. Runtime OCI packages remain private.
- Gateway commit `bc1b53c45c337d95daa38cd8170da46c246e5a70` and harness commit `aa294358bede782d1e533fc1f6432615b5366a82` passed their complete offline CI and CodeQL gates.
- A CPU-only native builder produced and independently validated all five private `linux/amd64` images, their OCI manifests, SLSA provenance, SPDX SBOMs, admission receipts, and immutable source bindings. It then terminated with no active release builder.
- Exact images are `milk-gateway@sha256:f5fd6786a5d36870045c9fc8271ac28940ae88809569f5c3fb8fbb2d2582ca4c`, `milk-jobs@sha256:3c59d72bce1d9ab3c2710a2080f36628a8e9605fef924c1f728302db1eaec9a4`, `milk-teacher-gpt-oss@sha256:d07a0f794e3a273d4c883606c2fa44728925c2a97379507d35a164b2c9293016`, `milk-student-train@sha256:74890a2b0524ac80067d6ffb499a176e5d70e735c1feddb825b25716de1092c4`, and `milk-student-branch@sha256:d8588f986aaaeefa34b32a43c3970579bb174b975517513ad3b741d73727e948`.
- Recorded compressed sizes are 12.05 MiB for `milk-gateway`, 58.23 MiB for `milk-jobs`, 10,420.52 MiB for `milk-teacher-gpt-oss`, 6,371.98 MiB for `milk-student-train`, and 10,836.95 MiB for `milk-student-branch`. Shared vLLM layers reduce unique content across all images to 16.88 GiB.
- Seven production R2 buckets and the three protected-main-only GitHub production environments exist. The production workflow remains disabled; required retention rules, credentials, and eval variables are not installed.
- Modal teacher and student volumes were populated with the pinned external model files, hash-verified, and observed with zero active tasks. Model weights are not stored in the images.
- Baseten-primary/Modal-fallback policy is implemented: Modal is eligible only after a validated retryable Baseten failure and never after ambiguous or accepted Baseten create authority.
- Local evidence does not prove production R2 publication/readback, eval binding, provider pull, deployment, paid execution, route behavior, or provider zero.

Alpine cannot materially reduce the pinned CUDA, PyTorch, and vLLM layers in the GPU images. The CPU images are already small; the GPU images are large because of their runtime stacks, while model weights remain external.

## Hosted pilot boundary

The pilot is single tenant: one gateway deployment owns one tenant, project, environment, and workload scope. A customer points the official OpenAI SDK at that gateway and sends one `dt_live_...` Bearer key. That key authorizes chat traffic only.

Milk Infrastructure is the hosted operator and owns the admitted eval document, storage credentials, provider credentials, route authority, and release evidence. The harness watches that admitted document and completed captured traffic. It stops creating teacher work at the eval's `max_decisions` limit while continuing bounded reconciliation and teardown. Scheduled runs cannot authorize provider creates.

## Work required before activation

1. Publish and read back the exact release, admissions, and eval documents in production evidence storage.
2. Finish least-privilege production storage credentials, retention/privacy controls, provider resources, and deployment receipts.
3. Materialize a fresh one-request teacher qualification eval and a separate student-capable eval, each with its own stable campaign ID, exact outer-document approval digest, per-eval limits, and bounded budget.
4. Deploy the admitted gateway and enable the scheduler only after every secret, resource identity, and configuration digest is verified.

## Remaining live gates

1. The Cloudflare Worker and Container run the newly admitted gateway image on the production hostname.
2. An official OpenAI SDK request returns a valid completion through the gateway and its immutable completed trace is present.
3. The one-request teacher qualification produces one terminal result under the live Baseten-primary/Modal-fallback policy, followed by observed zero compute on both providers.
4. The student eval retains at least 251 usable results: 50 TRAIN, 73 DEV, and 128 CALIBRATION. The current partition should plan for roughly 1,280 eligible captures; skipped traffic may require more. Generated traffic and local fixtures do not count.
5. One train/merge job and the BF16, dynamic-FP8, and static-FP8 branches complete against the same ordered DEV set.
6. A deterministic winner receives an authenticated 100-bps canary, and genuine candidate saturation falls back to the baseline.
7. Route control publishes and verifies the signed zero successor, removes the candidate credential, tears down the winner, and observes zero provider compute.
8. The final budget remains below the `$1,000` ceiling, and no new paid work starts at or above `$850`.
9. GitHub, R2, Cloudflare, Baseten, Modal, and release receipts agree on the exact eval, source, image, and deployment identities.
10. The public-release audit of repository history, assets, examples, install documentation, and GitHub settings is completed and its findings are recorded.

Anything short of these live gates is release preparation, not production qualification.
