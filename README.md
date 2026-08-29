# Milk Harness

Milk Harness is a temporary implementation bridge, not a third product or a
standing service. Milk has two products:

- [`milk-carton`](https://github.com/milkinfrastructure/milk-carton): the Rust
  API, routing, capture, object-store, and signed-route data plane;
- [`milk-man`](https://github.com/milkinfrastructure/milk-man): the agentic
  harness that invokes deterministic jobs as bounded tool calls.

This repository currently holds the deterministic reconciliation and worker
code being called from Milk Man while that code is moved behind its fixed job
interface. It watches Carton's S3-compatible object store, processes closed
hourly windows, writes derived objects, then exits. It has no route-signing or
spend authority and does not add a resident manager, database, or queue.

## One pass

Run one bounded reconciliation pass with:

```sh
python -m milk_harness run-once --config /absolute/path/run-config.json
```

For each new closed window, the harness:

1. reads bounded `stats` and sampled `traffic` objects under
   `milk/v1/scopes/<scope UUID>/`;
2. writes a structural summary with traffic volume, independent-session count,
   endpoint and workload mix, quality, latency, token, tool-use, structured
   output, refusal, and failure distributions;
3. asks the configured GLM teacher to classify a deterministic trace sample;
4. computes readiness from fixed gates: 100 independent sessions for the
   mechanics profile or 750 for production, plus capture and parse quality;
5. when ready, asks the teacher for representative and tail eval cases and
   writes an unsigned candidate route proposal.

Claims, results, summaries, eval versions, and proposals are content-addressed
or create-only. Small `current.json` objects are compare-and-swap pointers. A
replay with the same source objects makes no teacher call and produces the same
identities.

The bridge never receives a route-signing key and cannot activate traffic.
An operator reviews the proposal, signs a gateway route manifest, and publishes
it through `milk-carton`.

## Configuration

[`deploy/run-once.production.json`](deploy/run-once.production.json) is the
active public `milk.harness-run-config.v1` document. It binds one production
scope to Baseten's OpenAI-compatible `zai-org/GLM-5.3-Flash` endpoint and the
qualified input/output rates used for budget reservation. Keep secrets out of
the document; it names environment variables instead. Review provider rates
before changing the deployed config.

Production fixes `source.max_traces` at 3,000, providing four retained traces
per required independent session while keeping one bounded source manifest. It
also fixes both classification sample fields at 750, and readiness requires 750
classified independent sessions.
Set `candidate_basis_points` to `0` when the desired proposal is baseline-only.

The fixed reconciliation function uses these environment values:

```text
MILK_CONTROL_R2_ACCOUNT_ID
MILK_CONTROL_R2_BUCKET
MILK_CONTROL_R2_ACCESS_KEY_ID
MILK_CONTROL_R2_SECRET_ACCESS_KEY
MILK_CONTROL_R2_SESSION_TOKEN    # optional
BASETEN_API_KEY
```

The R2 identity needs read/write access only to the configured scope prefix.
The teacher must expose the configured HTTPS Chat Completions endpoint and
return usage counts plus JSON output. The checked-in example reserves at most
two calls per pass, stops new calls at $20 cumulative accounted spend, and has
an absolute $25 ceiling. Token rates must be set to current conservative rates
for those limits to be meaningful.

## Invocation boundary

Milk Man owns scheduled invocation. [`production-loop.yml`](.github/workflows/production-loop.yml)
is a manual bridge for verification and rollback only. Its fixed concurrency
group allows only one pass at a time. All object-store and provider credentials,
including account and bucket, come from the selected GitHub environment. The
checked-in run config fixes scope, model, sample limits, call limits, and spend
limits.

Milk Man and the manual bridge call the same deterministic function. An LLM may
propose a later config, but changing the active config remains an operator
action.

Manual dispatch may select the checked-in `mechanics` profile. It uses a
different scope UUID, requires 100 independent sessions, and may emit a 5%
unsigned candidate proposal. It cannot contribute to production readiness or
activate that proposal. Production remains the default manual profile.

## Development

The bridge control path uses the Python standard library plus the `zstd` executable
for production trace and result compression. Run the offline tests with:

```sh
python -m unittest discover -s milk_harness -p 'test_*.py'
```

The `mechanics` profile and generated traffic prove object-store mechanics.
Production qualification requires real gateway traffic, 750 independent
sessions, a successful hosted teacher pass, eval generation, an operator-signed
route, and a live routing/fallback check.
