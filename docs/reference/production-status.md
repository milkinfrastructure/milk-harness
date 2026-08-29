# Production qualification

Last updated: 2026-08-29 00:20 America/Los_Angeles.

## Goal

Prove the complete hosted path: an official OpenAI SDK request enters the Milk gateway, selected traffic becomes one bounded eval, one confirmed Modal teacher job produces an authoritative result, and the provider returns to zero GPU. Then publish the gateway and harness as a clean MIT-licensed install surface whose documentation matches that proof.

Budget guard: `$1,000` absolute campaign ceiling, stop new paid work at `$850`, retain `$150` for running work and teardown.

## Verified

- `milk-gateway` and `milk-harness` are published in GitHub.
- Gateway release source: `1e6ac86b9806486a7fdfa88051e2f8446bf884c0`.
- Gateway image: `ghcr.io/milkinfrastructure/milk-gateway@sha256:f1aaafee000af3973eb9ca1d880ce71335089d574a8c56591146354e75a60a30`.
- Harness release: `704e517b00d129b1a52c4e4f2885ae38407e1effd052c9641d7416bd09c1d420`.
- Jobs image: `ghcr.io/milkinfrastructure/milk-jobs@sha256:439133b7ac8735676dcfc9f51b62b9bb3d4e15d298ebac72f6bfd87fc6a5494a`.
- Gateway compressed size: 12.02 MiB. Jobs compressed size: 58.23 MiB.
- The full offline gateway and harness test gates passed for those releases.
- Seven empty production R2 buckets exist: capture, control, routes, evidence, ops log, create authority, and route evidence.
- The three GitHub production environments exist. Actions remain disabled.
- Shared GHCR/source credentials and the Modal control-plane token are installed in the appropriate GitHub environments.
- Modal app `milk-prod-gpu` and separate teacher/student volumes exist.
- No paid teacher GPU has run. No local GPU has run.

## In progress

- Replace the gateway's current release with one containing the published-release ancestry fix and public-facing README.
- Create distinct least-privilege R2 credentials for each runtime role.
- Populate the teacher volume with the admitted 60.794 GiB `openai/gpt-oss-120b` revision.
- Materialize and bind the first fresh `milk.eval.v1` document with `max_calls = max_decisions = 1` and `gpu_provider = modal`.
- Deploy the gateway and enable the scheduler only after all secrets and config are complete.

## Remaining live gates

1. Cloudflare Worker and Container are present on the admitted image and custom domain.
2. Official SDK smoke returns one valid completion through the gateway.
3. Captured traffic creates exactly one teacher claim and launch record.
4. One confirmed Modal job writes one ready result within the campaign guard.
5. Status reports the configured limit reached with no ambiguous or not-started result.
6. Modal reports zero active GPU work after teardown.
7. GitHub, R2, Cloudflare, Modal, and release receipts agree on the exact eval and image identities.

Anything short of those live gates is release preparation, not production qualification.
