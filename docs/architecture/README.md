# Architecture authority

The source of truth is the directly inspected whiteboard [`IMG_4239.HEIC`](IMG_4239.HEIC). [`IMG_4239-display.jpg`](IMG_4239-display.jpg) is a smaller display render. OCR is not used for architecture decisions.

The color boundaries are preserved:

- blue: official SDK, gateway, sampled traffic, and object-store data path;
- purple: generated data and artifact lifecycle around external large-model inference;
- teal: disposable GPU/model jobs and the evaluated promotion path;
- black: the harness, jobs extension, event observation, tests, and eval control loop.

The implementation has exactly two code repositories:

- [`milkinfrastructure/milk-gateway`](https://github.com/milkinfrastructure/milk-gateway) owns the blue data plane plus immutable claims, results, routes, and GPU launch outbox.
- [`milkinfrastructure/milk-harness`](https://github.com/milkinfrastructure/milk-harness) owns the black harness and jobs extension, including purple/teal worker implementations.

R2 buckets, private OCI packages, Cloudflare, Baseten, and Modal are infrastructure or artifacts, not additional repositories.

The existing Exo agent loop is the whiteboard's manager harness. Milk extends it with one typed tool backed by a fixed host command; the command can inspect state, run reconciliation, or consume a one-use host approval before dispatching paid work. This adds no resident service or model-controlled shell surface.

The product surface is one OpenAI-compatible API. Each production eval is a separate immutable campaign configuration and scope: it binds the teacher endpoint, model and execution profile; token, call, concurrency, wall-time and spend ceilings; deterministic train/dev/calibration partitioning; runtime images; and deployment identities. The loop stops at those limits. Provider credentials remain deployment inputs and never become worker inputs.

The first production proof qualifies the existing disposable GPU teacher profile. Hosted GLM is the next teacher profile to qualify on the same campaign contract after that proof. It must use explicit endpoint/model/auth and bounded-generation fields; it does not justify another repository, service, queue, or generic provider framework. The current gateway intentionally accepts only the GPU-job execution variant, so GLM hosting is a recorded next qualification rather than an implied capability.

Worker source lives in `milk-harness`; versioned worker contracts and the immutable runtime-image binding live in `milk-gateway`. This keeps GPU execution extensible without giving workers spend or route authority.

The gateway has three distinct object-store authorities: capture for sampled interactions and outcomes, control for claims/results/artifacts/frontiers, and routes for signed route state. `serve` has capture read-write and routes read-only; `tick --once` has capture/control read-write; `status` has all three read-only.

The cross-repository handoff is durable data, not a co-process call. Gateway `tick --once` writes an immutable `dragontales.gpu-launch-outbox.v1` and hashed `dragontales.gpu-launch-frontier.v1` pointer. A separate one-shot jobs image reads that chain and the canonical claim through read-only control credentials, then uses its own evidence/budget authority and provider credentials. It never receives gateway config, capture, routes, or signing credentials. The non-overlapping scheduler returns bounded winner and teardown results through separately credentialed gateway ingestion. Private builds, deployment, and the live end-to-end proof remain release gates.
