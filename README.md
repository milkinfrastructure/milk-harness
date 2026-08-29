# Milk Harness

Milk Harness is the operator-side control loop for [`milk-gateway`](https://github.com/milkinfrastructure/milk-gateway). Each pass validates one operator-admitted eval document, runs `gateway tick --once` over completed traffic and durable per-eval counts in R2, reconciles disposable provider jobs and route state, then exits. The fixed [production workflow](.github/workflows/production-loop.yml) is the only clock. There is no resident manager, database, queue, or third service.

The current hosted pilot is Stripe-like at the SDK boundary but single-tenant: one gateway deployment owns one tenant, project, environment, and workload scope. It is not a shared multi-customer endpoint.

The customer path is three steps:

1. Point the official OpenAI SDK at the Milk gateway.
2. Send one `dt_live_...` key as the Bearer credential.
3. Send normal application traffic.

Milk Infrastructure is the operator for the hosted pilot and owns its eval configuration, storage and provider credentials, route policy, and operator-only status/results inspection. A self-host operator supplies the equivalent configuration and secrets. Customers receive only the `dt_live_...` traffic key; it authorizes chat traffic, not provider work or status/results access. Customer traffic keys never enter the harness or provider jobs.

For the shortest no-cloud start, run the bounded config/control smoke in [`examples/self-host`](examples/self-host). It validates and materializes the example eval and exercises the fixed Exo host command without contacting a provider. Full provider execution remains Milk-managed because production admission accepts only Milk release receipts and immutable Milk image repositories.

No local Mac GPU is used. The gateway and scheduler are CPU-only; only an explicitly confirmed provider job can start a cloud GPU.

The MIT-licensed gateway and harness source repositories are public. Runtime OCI images remain private during production qualification. Source publication does not imply a production-qualified hosted release; the complete cloud proof below is still incomplete.

## Product contract

One canonical `milk.eval.v1` document contains every non-secret setting for one Milk-managed eval:

- tenant, project, environment, and workload scope;
- hard call and decision limits;
- gateway stores and immutable release identities;
- Baseten-primary/Modal-fallback provider policy and exact provider resources;
- teacher, student, route, and budget policy.

Production admits the exact document through two repository variables:

```text
MILK_EVAL_CONFIG_JSON=<canonical milk.eval.v1 JSON>
MILK_EVAL_ID=<stable campaign/eval ID embedded in the manifest and gateway config>
```

`MILK_EVAL_ID` is the stable campaign identity: it must equal both `manifest.campaign_id` and `gateway_config.eval_id`. It is not the document hash. Validation separately computes `MILK_EVAL_CONFIG_SHA256` over the exact canonical outer document; an explicitly confirmed manual dispatch must match that digest before paid work can start.

The workflow does not discover arbitrary configs from object storage. Each pass revalidates those admitted bytes, then reads completed traces and per-eval control state from R2. This keeps configuration explicit while R2 remains the authority for counts, claims, budgets, results, and routes.

Every manual dispatch also carries that stable identity as `managed_eval_id`.
All three production jobs run only when it equals the active repository
`MILK_EVAL_ID`; a missing or stale manual identity runs nothing. Scheduled runs
use the active repository identity directly.

Credentials remain individual operator-owned masked environment secrets. They are not embedded in the eval document, written to R2, bundled into JSON secret blobs, or exposed to model tool calls. Only the provider-reconciliation job receives provider API credentials.

The scheduled loop performs three bounded, separately credentialed steps:

1. `gateway tick --once` discovers or repairs work from captured traffic.
2. `jobs` reconciles existing provider state and, only with one-use authorization, uses Baseten first or Modal as the fallback.
3. Route control ingests verified results, advances or rolls back a canary, and proves provider zero.

GitHub Actions is only the clock. Exo can request only `status`, `reconcile`, or one explicitly approved `run_confirmed`; it cannot submit arbitrary commands or credentials.

## Limits and spend

`teacher.max_decisions` is the teacher-decision limit for one eval. Durable gateway keys include the eval ID before the tenant, project, environment, and workload IDs, so another eval cannot reuse its claims or limit. The gateway stops creating new teacher claims at the limit while allowing reconciliation and teardown to finish.

Paid work has three gates:

- an immutable price receipt for the exact provider;
- a pessimistic reservation inside the campaign budget;
- a confirmed manual dispatch whose hash matches the exact admitted outer eval document.

The current campaign ceiling is `$1,000`. New paid work stops at `$850`, preserving `$150` for running work and teardown. Scheduled runs cannot authorize provider creates.

The cloud-mechanics eval starts with one 20-call teacher job. No later provider create is authorized until a Baseten-selected job proves the exact private image and profile, all 20 calls ready within the live latency target, `logs_source_complete=true`, `oom_source_complete=true`, `oom_matched_record_count=0`, and zero compute after termination. A Modal-selected job can produce mechanics results but cannot pass this Baseten gate.

## Exo integration

[`tools/exo`](tools/exo) adds one narrow `milk` tool to an existing Exo harness. Its input is only:

```json
{"action":"status|reconcile|run_confirmed","eval_id":"<campaign ID>"}
```

The host command accepts only an operator-admitted config under `/etc/milk/evals/<eval_id>.json`. Exo receives no provider token, arbitrary command, route key, or spending authority.

The host operator may set a GitHub repository, workflow file, and ref for a fork. Those values come only from the service environment; they are never model inputs. Milk production remains the default. See [`tools/exo/README.md`](tools/exo/README.md).

## Cloud deployment

Production uses three GitHub environments:

- `milk-gateway-tick-prod`
- `milk-provider-jobs-prod`
- `milk-route-control-prod`

The production workflow is [`production-loop.yml`](.github/workflows/production-loop.yml). Keep only that production workflow disabled until all three environments, the two eval variables, provider resources, and object-store credentials are complete. Offline CI in [`offline-gates.yml`](.github/workflows/offline-gates.yml) can run without production credentials or paid work. Enabling the production workflow starts the five-minute reconciliation clock; it does not by itself authorize paid work.

The current release-candidate admission records these compressed `linux/amd64` sizes:

| Image | Compressed size |
| --- | ---: |
| CPU `milk-gateway` | 12.05 MiB |
| CPU `milk-jobs` | 58.23 MiB |
| GPU `milk-teacher-gpt-oss` | 10,420.52 MiB |
| GPU `milk-student-train` | 6,371.98 MiB |
| GPU `milk-student-branch` | 10,836.95 MiB |

Modal and Baseten pull those exact images. They do not rebuild them. The local Mac does not build or run GPU images.

Alpine cannot materially shrink the pinned CUDA, PyTorch, and vLLM layers; model weights remain external and are mounted and hash-verified at runtime. The teacher and branch share the same vLLM layers, so all five images contain 16.88 GiB of unique compressed content rather than their 27.05 GiB arithmetic sum. Milk adds 463.57 MiB across the five images; the remaining unique bytes are pinned upstream runtimes.

Image release v5 (`cd8a756a384780977a0f9c33cff17297844ed94562bb1bb5f6cd2878d20bf30c`) retains the initial v4 gateway and GPU images bound to gateway `bc1b53c45c337d95daa38cd8170da46c246e5a70` and harness `aa294358bede782d1e533fc1f6432615b5366a82`, and selectively replaces only `milk-jobs` from harness `0eebb1aa0326b1cf13403b885295b99cc02fd71b`. The new 58.23 MiB jobs image is `milk-jobs@sha256:c80c438845462cf2d3dbe74d49bb08a1c8b109b5ff83f551619271cc92e59260`; its admission is `a6e3b0bb7a09f9db31cd76b9c0238655141391c9c63e969c5175306112f30790` and build context is `0dbd7543c82077868a4b6a4440be4dfc0d323ccfeb3eff22e2ccf08d1b69b536`. The native selective rebuild took 3 minutes 12 seconds, used no local GPU, and left no active builder. The hosted release remains a candidate until production publication/readback and the complete cloud proof below pass.

See [`docs/reference/production-scheduler.md`](docs/reference/production-scheduler.md) for the credential boundaries and [`docs/reference/spend-policy.md`](docs/reference/spend-policy.md) for budget semantics.

## Development

The harness uses the Python standard library for the control path. Run the complete offline suite with:

```sh
python3 -m unittest discover -s milk_harness -p 'test_*.py'
sh deploy/build-images.test.sh
sh tools/exo/milk-managed.test.sh
node --test tools/exo/index.test.mjs
```

Builds run on a clean CPU-only x86 host. The release script accepts no provider, R2-authority, route-signing, OpenAI, or local-GPU credentials.

Current qualification evidence and remaining gates are recorded in [`docs/reference/production-status.md`](docs/reference/production-status.md).

## Live-proof boundary

Offline tests, the self-host smoke, public source, published images, and generated mechanics traffic do not prove a production deployment. One passing 20-call teacher job is only the first provider gate.

A synthetic cloud-mechanics eval uses ID `959caacb397004bf3e60f13613da50f4ed3160a65d18b178c3d996398e29b5a0`, 320 decisions partitioned as 63 TRAIN, 91 DEV, and 166 CALIBRATION, `max_calls=20`, `max_gpu_seconds=3600`, and `max_parallel_runs=1`. Its first Baseten-selected job is the teacher admission gate above. With a 3,600-second winner bound, its pessimistic GPU reservation is `$160`: `$130` for 16 teacher jobs, `$5.625` for train/merge, `$16.875` for three branches, and `$7.50` for the winner. It tests the cloud chain only; none of its generated traffic or results satisfy the real-traffic production gates below. The run remains inside the `$1,000` ceiling and `$850` new-work cutoff.

Production qualification requires one complete live chain:

1. A normal official-SDK response and its persisted completed trace.
2. At least 251 retained teacher results: 50 TRAIN, 73 DEV, and 128 CALIBRATION. The current 80/10/10 partition should plan for roughly 1,280 eligible captures to obtain 128 CALIBRATION rows; skipped traffic can require more. Generated traffic and local fixtures do not count.
3. One trained and merged student plus BF16, dynamic FP8, and static FP8 evaluations on the same ordered DEV set.
4. A deterministic winner, authenticated canary, and verified baseline fallback.
5. An active signed zero route and both Baseten and Modal observed at zero compute.
