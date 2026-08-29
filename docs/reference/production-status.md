# Production qualification

Last updated: 2026-08-29 01:18 America/Los_Angeles.

## Goal

Prove the complete hosted path: official OpenAI SDK traffic enters the Milk gateway, retained traffic reaches the fixed 50 TRAIN / 73 DEV / 128 CALIBRATION student gate, disposable provider jobs train and evaluate the three admitted branches, the deterministic winner receives a signed canary, saturation falls back to the baseline, a signed zero route is published, and every provider returns to zero GPU. Then publish the gateway and harness as clean MIT-licensed install surfaces whose documentation matches that proof.

The first one-request teacher run is only the provider qualification phase. Baseten runs when its live preflight is healthy; Modal runs only after a genuine retryable Baseten failure. This phase does not by itself qualify the student, route, fallback, teardown, or complete product loop.

Budget guard: `$1,000` absolute campaign ceiling, stop new paid work at `$850`, retain `$150` for running work and teardown.

## Verified

- Private `milk-gateway` and `milk-harness` repositories are published in GitHub. They are not public yet.
- Gateway release source: `1e6ac86b9806486a7fdfa88051e2f8446bf884c0`.
- Gateway image: `ghcr.io/milkinfrastructure/milk-gateway@sha256:f1aaafee000af3973eb9ca1d880ce71335089d574a8c56591146354e75a60a30`.
- Harness release: `648a08231b77af78655dff40d17a4430fbe608aa76240a88120286bdce29640b`.
- Jobs image: `ghcr.io/milkinfrastructure/milk-jobs@sha256:1a2aa53ad8283eb123f9f2dba39228c94190e999ed5e360a4edf0f4ee0a0d19f`.
- Gateway compressed size: 12.02 MiB. Jobs compressed size: 58.23 MiB.
- The full offline gateway and 212-test harness gates passed before the selective jobs rebuild. Its content-free build receipt records exit code 0.
- Seven empty production R2 buckets exist: capture, control, routes, evidence, ops log, create authority, and route evidence.
- The three GitHub production environments exist. Actions remain disabled.
- Shared GHCR/source credentials and the Modal control-plane token are installed in the appropriate GitHub environments.
- Modal app `milk-prod-gpu` and separate teacher/student volumes exist.
- `milk-gateway` main includes the deploy ancestry fix, 120-second official-SDK timeout, hosted/self-hosted contract, and full qualification gates at `464b350`.
- `milk-harness` main and release branch contain the hosted/self-hosted contract, production log, permanent-R2 fix, student-volume helper, real provider fallback, and selective release reuse fix at `f023aac38efe4d71b263c3551300c1cded0257aa`.
- The selective `linux/amd64` build produced private jobs admission `e62a59523ded42549f3e91cfb0bb3ddefbca502733b15fa81f990974b5e0b58c` with SLSA v1 provenance and SPDX SBOM attestations. The v5 release reuses the exact admitted student-train, student-branch, and teacher images; only jobs was rebuilt.
- The admitted gateway and three digest-bound GPU images remain unchanged.
- This local release evidence does not prove production R2 publication/readback, eval binding, provider pull, deployment, or any cloud execution.
- Modal volume `milk-prod-teacher-cache` contains exactly the 24 admitted `openai/gpt-oss-120b` files at revision `b5c939de8f754692c1647ca79fbf85e8c1e70f8a`, totaling 65,276,850,729 bytes. The population app stopped with zero tasks.
- Modal volume `milk-prod-student-train` contains exactly the 13 admitted `Qwen/Qwen3-4B-Instruct-2507` files at revision `cdbee75f17c01a7cc42f958dc650907174af0554`, totaling 8,060,917,568 bytes. A separate hash pass verified every file and all Modal apps report zero active tasks.
- Fresh smoke, capture, outcome, candidate, container-admin, route, and signing credentials exist only in the owner-only qualification evidence directory. They are not committed.
- Production workflow commit `d33304478c95a53ea68d34d7acb52942b7fad1c3` omits absent R2 session-token variables, so permanent R2 credentials no longer reach containers as invalid empty tokens. Its 17 focused workflow tests pass.
- Commit `d65e239` removes the redundant pinned provider, performs live Baseten preflight first for ordinary and winner work, permits Modal only after validated retryable unavailability, and preserves the no-fallback rule after Baseten create authority or ambiguity. The full 212-test Python gate passes.
- Cloudflare preflight found no `default` AI Gateway, no production Worker, Container, route, or DNS record, zero prepaid AI credits, and Containers disabled until Workers Paid is enabled. No request was sent into Cloudflare's default payload-logging behavior.
- No paid teacher GPU has run. No local GPU has run.

## In progress

- Publish and exact-readback the v5 release plus all four admissions in production evidence R2, then bind the exact jobs image, release, and admissions into both fresh evals.
- Create distinct least-privilege permanent R2 credentials for each runtime role. Permanent credentials omit all session-token secrets.
- Enable Workers Paid and fund bounded AI credits, then create the Cloudflare AI Gateway with payload logging disabled, zero-data-retention enabled, retries disabled, and an account spend limit before the first request.
- Create the Baseten project, team, and API credential required for the live primary preflight.
- Materialize two fresh immutable evals: a one-request teacher qualification, then the student-capable campaign with route authority and bounded spend.
- Deploy the gateway and enable the scheduler only after all secrets, privacy controls, and config are complete.

## Remaining live gates

1. Cloudflare Worker and Container are present on the admitted image and production hostname.
2. Official SDK smoke returns a valid completion through the gateway and its immutable trace is present.
3. The one-request teacher qualification produces one terminal result on the provider selected by the live Baseten-primary/Modal-fallback policy, and both providers are observed at zero GPU afterward.
4. The student campaign records at least 50 TRAIN, 73 automatic DEV, and exactly 128 CALIBRATION inputs under the same immutable eval.
5. One train/merge job and the BF16, dynamic-FP8, and static-FP8 branches complete against the same ordered DEV set; branch intervals prove the intended fanout.
6. The deterministic winner is admitted, a signed 100-bps canary is exercised through the official SDK, and genuine saturation falls back to the baseline.
7. Route control publishes and verifies the signed zero successor, removes the candidate credential, tears down the winner, and observes zero provider compute.
8. The final budget head stays below the `$1,000` ceiling and no new work starts at or above `$850`.
9. GitHub, R2, Cloudflare, Baseten, Modal, and release receipts agree on the exact eval and image identities.
10. Repository history, assets, examples, and install documentation pass the public-release security gate before either repository becomes public.

Anything short of those live gates is release preparation, not production qualification.
