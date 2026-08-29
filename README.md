# Milk Harness

Milk Harness watches the traffic captured by [`milk-gateway`](https://github.com/milkinfrastructure/milk-gateway), generates evaluations until a configured limit is reached, and exits. It runs disposable provider jobs; it is not a standing model server or control service.

The intended customer experience is close to Stripe:

1. Point the official OpenAI SDK at the Milk gateway.
2. Provide one eval configuration and the required keys.
3. Send normal application traffic.
4. Read bounded status and evaluation results.

No local Mac GPU is used. The gateway and scheduler are CPU-only; only an explicitly confirmed provider job can start a cloud GPU.

![Architecture whiteboard](docs/architecture/IMG_4239-display.jpg)

## Product contract

One canonical `milk.eval.v1` document contains every non-secret setting for one eval:

- tenant, project, environment, and workload scope;
- hard call and decision limits;
- gateway stores and immutable release identities;
- selected GPU provider and exact provider resources;
- teacher, student, route, and budget policy.

Production reads it from two repository variables:

```text
MILK_EVAL_CONFIG_JSON=<canonical milk.eval.v1 JSON>
MILK_EVAL_ID=<sha256 of those exact bytes>
```

Credentials remain individual masked secrets. They are not embedded in the eval document or bundled into JSON secret blobs.

The scheduled loop performs three bounded, separately credentialed steps:

1. `gateway tick --once` discovers or repairs work from captured traffic.
2. `jobs` reconciles existing provider state and, only with one-use authorization, creates the selected Modal or Baseten job.
3. Route control ingests verified results, advances or rolls back a canary, and proves provider zero.

R2 records are the authority for leases, claims, budgets, results, and routes. GitHub Actions is only the clock.

## Limits and spend

`teacher.max_decisions` is the eval-generation limit. The gateway stops creating new teacher claims at that limit while allowing reconciliation and teardown to finish.

Paid work has three gates:

- an immutable price receipt for the exact provider;
- a pessimistic reservation inside the campaign budget;
- a confirmed manual dispatch whose hash matches the active eval.

The current campaign ceiling is `$1,000`. New paid work stops at `$850`, preserving `$150` for running work and teardown. Scheduled runs cannot authorize provider creates.

## Exo integration

[`tools/exo`](tools/exo) adds one narrow `milk` tool to an existing Exo harness. Its input is only:

```json
{"action":"status|reconcile|run_confirmed","eval_id":"<sha256>"}
```

The host command accepts only an operator-admitted config under `/etc/milk/evals/<eval_id>.json`. Exo receives no provider token, arbitrary command, route key, or spending authority.

## Cloud deployment

Production uses three GitHub environments:

- `milk-gateway-tick-prod`
- `milk-provider-jobs-prod`
- `milk-route-control-prod`

The workflow is [`production-loop.yml`](.github/workflows/production-loop.yml). Keep Actions disabled until all three environments, the two eval variables, provider resources, and object-store credentials are complete. Enabling the workflow starts the five-minute reconciliation clock; it does not by itself authorize paid work.

Provider work uses immutable private `linux/amd64` images:

- `milk-jobs`
- `milk-teacher-gpt-oss`
- `milk-student-train`
- `milk-student-branch`

Modal and Baseten pull those exact images. They do not rebuild them. The local Mac does not build or run GPU images.

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

This repository is MIT licensed. It is not production-qualified until a real official-SDK request produces one paid evaluation result and the provider is then observed at zero GPU.
