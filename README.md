# Milk Harness

Milk Harness is the operator loop for
[`milk-gateway`](https://github.com/milkinfrastructure/milk-gateway). It turns
completed gateway traffic into bounded teacher, training, evaluation, and route
jobs, then exits. GitHub Actions is the clock. R2 is the durable authority.
Baseten is the only GPU provider.

There is no resident manager, database, queue, or local GPU dependency. The
public system is two repositories:

- `milk-gateway`: the OpenAI-compatible CPU gateway and R2 state machine;
- `milk-harness`: eval configuration, disposable jobs, release checks, and the
  production workflow.

The hosted pilot is intentionally single-tenant. A customer points an official
OpenAI SDK at the gateway, supplies one `dt_live_...` bearer key, and sends
normal chat-completions traffic. That key cannot inspect results, change routes,
or start paid work.

Source is MIT-licensed. Production OCI images remain private. The hosted release
is not production-qualified until the complete cloud proof has passed.

## Control loop

Every pass performs three bounded steps:

1. `gateway tick --once` discovers or repairs work from completed R2 captures.
2. `jobs` reconciles existing Baseten work and, only with one-use authority,
   creates the next admitted job.
3. Route control ingests verified results, advances or rolls back a canary, and
   proves signed zero after teardown.

Scheduled passes can reconcile and clean up. They cannot authorize a provider
create or paid proof traffic. A manual pass must match the exact active eval ID
and canonical eval SHA-256 before either action is enabled.

## Eval contract

One canonical `milk.eval.v1` document contains every non-secret setting for one
eval:

- tenant, project, environment, and workload scope;
- hard call, wall-time, concurrency, and cost limits;
- R2 locations and immutable release identities;
- exact Baseten team, project, image, and secret names;
- teacher, student, route, and budget policy.

Production admits it through two repository variables:

```text
MILK_EVAL_CONFIG_JSON=<canonical milk.eval.v1 JSON>
MILK_EVAL_ID=<stable eval ID embedded in the manifest and gateway config>
```

`MILK_EVAL_ID`, `manifest.campaign_id`, and `gateway_config.eval_id` must be
identical. The provider policy is exactly `{"only":"baseten"}`. Fallback fields,
Modal identities, and Modal pricing are rejected.

Secrets are separate masked environment values. They are not embedded in the
eval, written to R2, placed in model inputs, or bundled into one shared secret.
Provider workloads receive only the credential roles and secret names required
for that operation.

## Spend limits

Every create requires all of the following:

- an immutable Baseten price receipt;
- a pessimistic reservation in the shared campaign ledger;
- one create-only authority object;
- an explicitly confirmed manual dispatch for the exact eval digest.

The GPU ledger ceiling is `$1,000`. New work stops at `$850`, preserving `$150`
for running work and teardown. The first cloud proof is narrower: `$160` maximum
GPU reservation plus `$15` external reserve, or `$175` total authorization.

The proof starts with one 20-call teacher job. No later create is admitted until
that job proves the exact private image and profile, all calls terminal, complete
logs and OOM evidence, and zero Baseten compute after termination.

## Production setup

Production uses three GitHub environments:

- `milk-gateway-tick-prod`
- `milk-provider-jobs-prod`
- `milk-route-control-prod`

They bind fourteen least-privilege R2 identities across seven buckets. The two
GPU identities live in Baseten Secrets; the scheduler sees only their access-key
IDs for identity verification.

[`bootstrap-baseten-registry.yml`](.github/workflows/bootstrap-baseten-registry.yml)
is a manual, eval-bound credential sync. It sends the existing read-only GHCR
credential directly from the protected GitHub environment to Baseten and verifies
only secret metadata. It cannot start a job or expose the credential value.

[`production-loop.yml`](.github/workflows/production-loop.yml) is the only
production clock. Follow the ordered
[`production runbook`](docs/reference/production-runbook.md) before enabling it.
Current provider and release evidence is tracked in
[`production status`](docs/reference/production-status.md).

No local Mac GPU is used. CPU images are built for `linux/amd64`; the gateway
runtime is a shell-free Chainguard image. GPU images use pinned CUDA, PyTorch,
Prime-RL, and vLLM layers, so Alpine is not a compatible substitute. Model
weights stay outside the images and are mounted and hash-verified at runtime.

The mechanics proof uses the typed GPT-OSS teacher profile. Hosted GLM is the
next typed teacher profile after the mechanics gate passes; it uses the same eval
and job contract.

## Harness tool

[`tools/exo`](tools/exo) exposes one narrow host command:

```json
{"action":"status|reconcile|run_confirmed","eval_id":"<eval ID>"}
```

It accepts only an operator-admitted config. The model receives no provider
token, shell command, route key, or spending authority.

## Development

The control path uses the Python standard library. Run the offline gates with:

```sh
python3 -m unittest discover -s milk_harness -p 'test_*.py'
python3 -m unittest discover -s deploy/baseten -p 'test_*.py'
sh deploy/build-images.test.sh
sh tools/exo/milk-managed.test.sh
node --test tools/exo/index.test.mjs
```

Offline tests, public source, published images, and generated mechanics traffic
do not prove production. Qualification requires a normal SDK capture, retained
real-traffic teacher results, one trained student, the three admitted evaluation
variants, a deterministic winner, an authenticated canary with baseline
fallback, signed zero, and verified zero Baseten compute.
