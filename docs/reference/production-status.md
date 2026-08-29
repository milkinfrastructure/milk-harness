# Production status

Observed 2026-08-29. This file records provider and release evidence; it is not a
substitute for the live proof.

## Source

- Public main before this cutover: gateway `24473db`, harness `b374be4`.
- Baseten-only gateway and harness changes are under review and are not yet the
  production release.
- The active contract is `milk.confirmed-production-run-config.v7` with provider
  policy exactly `{"only":"baseten"}`.
- Modal is not an authorized production provider. Ambient Modal credentials are
  rejected by the jobs entry point.

## Infrastructure verified

- Cloudflare Workers Paid is active for the production account.
- Seven R2 buckets exist for capture, control, routes, evidence, route evidence,
  create authority, and operational logs.
- Fourteen distinct least-privilege R2 identities were exercised against their
  assigned bucket and a forbidden bucket. Read/write roles wrote successfully;
  read-only roles denied writes; cross-bucket access was denied.
- The route-evidence bucket's object lock correctly prevented deletion of the
  final credential probe. One zero-part multipart upload is pending its one-day
  incomplete-upload lifecycle cleanup.
- The Cloudflare deployer token and narrower candidate-controller token are
  active and stored only in their intended operator surfaces.
- All five GHCR runtime packages are private.
- Baseten has one active production operator key. The capture and control R2
  credential pairs exist as four separate Baseten secrets.
- The protected GitHub environments contain the required R2, Baseten,
  Cloudflare, source, and private-registry credentials. Registry credentials are
  synced to Baseten only through the manual eval-bound bootstrap workflow.

No secret values are recorded here.

## Image evidence

- The last admitted gateway runtime is about 12.05 MiB compressed. Rust and
  Debian exist only in the build stage; runtime is the shell-free Chainguard
  glibc image.
- The Baseten-only jobs image has been built for `linux/amd64`, runs as UID/GID
  65532, contains no Modal package or source, and is 45.8 MB compressed before
  the pinned winner deployment client is added.
- The verified winner-capable jobs image is 119.1 MB compressed and includes the
  pinned Truss 0.18.25 closure. Its direct-image contract uses
  `DockerServer(no_build=True)` and passed as UID/GID 65532.
- GPU images retain their pinned CUDA, PyTorch, Prime-RL, and vLLM bases. Model
  weights are external. No local GPU is used for builds or tests.

These are candidate observations. New immutable gateway and harness release
records, GHCR digests, admissions, and R2 readback are still required after the
source changes merge.

## Provider gate

Baseten currently returns HTTP 403 `PERMISSION_DENIED` for
`POST /v1/training_projects`: the organization is not authorized for Baseten
Training. Support issue `#28317` requests Training API access and confirmation
that the organization may use no-build direct-image winner serving. No human
response has arrived yet.

Baseten showed `$0.92` remaining credits and no editable hard budget in the UI.
The application therefore remains the enforceable spend boundary: `$1,000`
absolute GPU authority, `$850` launch cutoff, `$150` teardown reserve, and a
separate `$175` first-proof envelope. No GPU reservation or paid Baseten job has
been created.

## Remaining release gates

1. Merge both Baseten-only source changes after protected CI passes.
2. Build and admit the new `linux/amd64` gateway and jobs images; retain the
   unchanged admitted GPU images only if their exact dependency contract still
   validates.
3. Publish release evidence to R2 and verify metadata plus least-privilege body
   readback.
4. Create the canonical v7 eval from the final image, provider project, team,
   secret, store, and gateway deployment identities.
5. Run the manual Baseten registry bootstrap and install the exact config secret.
6. Deploy the gateway to Cloudflare Containers and pass the official-SDK
   baseline smoke.
7. After Baseten enables Training, run one explicitly confirmed mechanics proof
   inside the `$175` envelope.
8. Verify terminal job evidence, candidate canary, baseline fallback, signed
   zero, zero Baseten compute, and retained R2 evidence.

Generated mechanics traffic proves the cloud path only. Production qualification
also requires retained complete real traffic before generated evals may affect a
student route.
