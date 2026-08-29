# Production scheduler

`.github/workflows/production-loop.yml` is the only production scheduler. It
runs every five minutes, validates the exact active `milk.eval.v1` document,
performs one bounded pass, and exits. There is no resident scheduler service.

The concurrency key includes `MILK_EVAL_ID` and does not cancel an in-flight
pass. Every manual run must supply the same eval ID. A missing or stale ID runs
no gateway, provider, or route job.

## Jobs

The workflow has three separately credentialed jobs.

### Gateway tick

`milk-gateway-tick-prod` receives the private gateway pull credential and only
the capture/control R2 roles needed by `gateway tick --once`. It discovers work
from completed captures, enforces durable per-eval limits, writes immutable
claims and launch outboxes, and exits.

It receives no Baseten key, evidence writer, route signing key, or provider-side
R2 secret value.

### Baseten reconciliation

`milk-provider-jobs-prod` receives:

- one Baseten management key;
- a private GHCR pull identity;
- read-only gateway control discovery;
- create-authority read/write roles separated by operation;
- evidence and operational-log writers;
- the expected access-key IDs and hashes for the two GPU R2 identities.

The full GPU capture/control credential pairs live in Baseten Secrets. A job
request binds their exact secret names, account IDs, buckets, and expected
identity hashes. The provider wrapper checks the injected access-key ID before
storage or model work starts.

The jobs entry point requires `BASETEN_API_KEY` and rejects ambient
`MODAL_TOKEN_ID` or `MODAL_TOKEN_SECRET`. The active provider policy is exactly
`{"only":"baseten"}`. Modal fields, selections, pricing, and stored provider
authority cannot be used to create work.

Before a create, the scheduler verifies:

1. the canonical v7 eval and exact harness source;
2. immutable image digests, admissions, and release digest;
3. live Baseten team, project, secret metadata, and H100 capacity;
4. one unexpired provider-pass lease;
5. one create-only authority object;
6. one pessimistic shared-budget reservation;
7. the confirmed manual eval SHA-256.

Every provider mutation is preceded by a durable intent. Ambiguous creates are
resolved by read-only identity search and are never blindly replayed. Teardown
remains authorized after the create window closes. A pass does not release its
lease until the child exits, gateway handoffs are ingested, and the pass archive
is written.

### Route control

`milk-route-control-prod` receives the route writer/signing key, verified result
reader, official-SDK proof credential, and the narrow Cloudflare candidate-secret
controller. It receives no Baseten management key or provider-create authority.

The gateway owns the candidate-key state machine. Route control asks it for one
bounded install, verify, or remove operation, applies that operation to the
admitted Baseten winner, and ingests the hash-only acknowledgement before another
mutation. An unacknowledged operation is verified on restart rather than replayed.

Scheduled recovery may advance an existing canary directly to signed zero,
adopt an existing zero, remove an expired candidate credential, and complete
teardown. It cannot send paid proof traffic or create a provider job.

## Manual authority

Manual dispatch has two independent booleans:

- `authorize_provider_creates`
- `authorize_mechanics_traffic`

Either requires `confirmed_run_config_sha256` to equal the SHA-256 of the exact
active canonical eval. Neither authority is inferred from the schedule, a model
tool call, an earlier run, or a provider credential.

The first mechanics proof is capped at 324 official-SDK calls and `$175` total:
`$160` GPU authorization plus `$15` external reserve. The validator derives a
`$142.50` maximum GPU reservation from the fixed teacher, train, three-branch,
and winner bounds. The broader campaign ledger remains `$1,000` absolute with an
`$850` launch cutoff and `$150` teardown reserve.

## Evidence

R2 is authoritative for counts, claims, reservations, provider results, route
state, and terminal evidence. GitHub logs are operational evidence only.

Each pass emits a content-free archive that identifies the eval, workflow run,
source, images, provider operation IDs, object keys, digests, byte counts, and
known collection gaps. Prompts, model outputs, secret values, and raw provider
logs do not enter that archive.

The paid proof is complete only after the official-SDK baseline, admitted
teacher/train/eval/winner results, authenticated canary, the OpenAI baseline
under candidate saturation, active signed-zero route, candidate credential
removal, winner termination, and zero Baseten compute all have matching
immutable evidence.

## Credential inventory

Production uses fourteen distinct R2 identities across seven buckets. Validate
each identity against its assigned bucket and one forbidden bucket before
activation. Read-only identities must deny writes. No pair may be reused for a
different bucket or phase.

The manual
[`bootstrap-baseten-registry.yml`](../../.github/workflows/bootstrap-baseten-registry.yml)
copies the protected GHCR pull credential directly to the configured Baseten
team as the training and winner registry secrets. It validates the active eval
and reads back secret metadata only. It cannot create GPU work.
