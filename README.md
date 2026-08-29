# Milk Harness

Milk Harness watches the traffic captured by [`milk-gateway`](https://github.com/milkinfrastructure/milk-gateway), generates evaluations until a configured limit is reached, and exits. It runs disposable provider jobs; it is not a standing model server or control service.

The current hosted pilot is Stripe-like at the SDK boundary but single-tenant: one gateway deployment owns one tenant, project, environment, and workload scope. It is not a shared multi-customer endpoint.

1. Point the official OpenAI SDK at the Milk gateway.
2. Set one `dt_live_...` key.
3. Send normal application traffic.

Milk Infrastructure owns the hosted eval configuration, object stores, provider credentials, route policy, and operator-only status/results inspection. The `dt_live_...` key authorizes chat traffic only; there is no customer status/results API yet.

The forkable self-host surface currently covers canonical config validation and the Exo control boundary. Full provider execution remains Milk-managed because production admission accepts only Milk release receipts and immutable Milk image repositories. The bounded local smoke is in [`examples/self-host`](examples/self-host).

No local Mac GPU is used. The gateway and scheduler are CPU-only; only an explicitly confirmed provider job can start a cloud GPU.

The code is MIT licensed, but this source repository and its OCI images remain private during production qualification and security review. This is not yet a public open-source release.

## Product contract

One canonical `milk.eval.v1` document contains every non-secret setting for one Milk-managed eval:

- tenant, project, environment, and workload scope;
- hard call and decision limits;
- gateway stores and immutable release identities;
- Baseten-primary/Modal-fallback provider policy and exact provider resources;
- teacher, student, route, and budget policy.

Production reads it from two repository variables:

```text
MILK_EVAL_CONFIG_JSON=<canonical milk.eval.v1 JSON>
MILK_EVAL_ID=<stable campaign/eval ID embedded in the manifest and gateway config>
```

Credentials remain individual masked secrets. They are not embedded in the eval document or bundled into JSON secret blobs.

The scheduled loop performs three bounded, separately credentialed steps:

1. `gateway tick --once` discovers or repairs work from captured traffic.
2. `jobs` reconciles existing provider state and, only with one-use authorization, uses Baseten first or Modal as the fallback.
3. Route control ingests verified results, advances or rolls back a canary, and proves provider zero.

R2 records are the authority for leases, claims, budgets, results, and routes. GitHub Actions is only the clock.

## Limits and spend

`teacher.max_decisions` is the teacher-decision limit for one eval. Durable gateway keys include the eval ID before the tenant, project, environment, and workload IDs, so another eval cannot reuse its claims or limit. The gateway stops creating new teacher claims at the limit while allowing reconciliation and teardown to finish.

Paid work has three gates:

- an immutable price receipt for the exact provider;
- a pessimistic reservation inside the campaign budget;
- a confirmed manual dispatch whose hash matches the active canonical eval document.

The current campaign ceiling is `$1,000`. New paid work stops at `$850`, preserving `$150` for running work and teardown. Scheduled runs cannot authorize provider creates.

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

The workflow is [`production-loop.yml`](.github/workflows/production-loop.yml). Keep Actions disabled until all three environments, the two eval variables, provider resources, and object-store credentials are complete. Enabling the workflow starts the five-minute reconciliation clock; it does not by itself authorize paid work.

Provider work uses immutable private `linux/amd64` images:

| Image | Compressed size |
| --- | ---: |
| CPU `milk-gateway` | 12.02 MiB |
| CPU `milk-jobs` | 58.23 MiB |
| GPU `milk-teacher-gpt-oss` | 10,420.49 MiB |
| GPU `milk-student-train` | 6,371.95 MiB |
| GPU `milk-student-branch` | 10,836.92 MiB |

Modal and Baseten pull those exact images. They do not rebuild them. The local Mac does not build or run GPU images.

Alpine cannot materially shrink the pinned CUDA, PyTorch, and vLLM layers; model weights remain external and are mounted and hash-verified at runtime.

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

Current live qualification and exact release IDs are recorded in [`docs/reference/production-status.md`](docs/reference/production-status.md).

## Production qualification

One paid teacher result is the first provider gate, not production qualification. The complete cloud proof requires:

1. A normal official-SDK response and its persisted completed trace.
2. At least 251 retained teacher results: 50 TRAIN, 73 DEV, and 128 CALIBRATION. Partitioning or skipped traffic can require more requests. Generated traffic does not count.
3. One trained and merged student plus BF16, dynamic FP8, and static FP8 evaluations on the same ordered DEV set.
4. A deterministic winner, authenticated canary, and verified baseline fallback.
5. An active signed zero route and both Baseten and Modal observed at zero compute.
