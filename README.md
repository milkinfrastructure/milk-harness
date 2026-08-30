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
4. computes readiness from fixed gates: 32 successful independent sessions for
   the mechanics profile or 750 for production, plus capture and parse quality;
5. when ready, asks the teacher for representative and tail eval cases and
   limits production evals to text-only cases with a reference answer;
6. runs a separate teacher validation job that rejects unsupported, copied,
   leaked, unanswerable, or vacuous cases;
7. scores a deterministic held-out subset against the incumbent and candidate
   under configured error, latency, and reference-similarity thresholds; and
8. writes an unsigned candidate route proposal only after both gates pass.

Claims, results, summaries, eval versions, and proposals are content-addressed
or create-only. Small `current.json` objects are compare-and-swap pointers. A
replay with the same source objects makes no teacher call and produces the same
identities. A non-ready, validation-failed, or score-failed pass retains its
pending source and advances the current proposal pointer to an explicit blocked
record, so an older proposal cannot remain current.

`milk.run-once-report.v2.artifact_refs` derives source, pointer/version, and job
roots from admitted IDs. Callers never supply keys; job child names are fixed.

The checked-in two-call teacher limit intentionally splits a ready source across
two invocations: classification plus generation, then cached reconciliation plus
validation. Candidate scoring has its own exact call and token limits. The
reported `statistically_qualified` field covers traffic sufficiency only; it is
not route qualification.

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
classified independent sessions. `source.eval_trace_bytes` bounds the request
and response context supplied for each generated eval.
Set `candidate_basis_points` to `0` when the desired proposal is baseline-only.

Candidate reference scoring uses deterministic normalized token F1 with an
explicit negation mismatch guard. It is a conservative reference-similarity
proxy, not a general semantic judge or sufficient evidence by itself for a
production route. Independent teacher validation and operator review remain
required.

The fixed reconciliation function uses these environment values:

```text
MILK_CONTROL_R2_ACCOUNT_ID
MILK_CONTROL_R2_BUCKET
MILK_CONTROL_R2_ACCESS_KEY_ID
MILK_CONTROL_R2_SECRET_ACCESS_KEY
MILK_CONTROL_R2_SESSION_TOKEN    # optional
BASETEN_API_KEY
MILK_GATEWAY_API_KEY
MILK_HARNESS_REVISION            # exact 40-character deployed Git revision
```

The R2 identity needs read/write access only to the configured scope prefix.
The teacher must expose the configured HTTPS Chat Completions endpoint and
return usage counts plus JSON output. The checked-in example reserves at most
two teacher calls and sixteen scoring calls per pass, stops new calls at $20
cumulative accounted spend, and has an absolute $25 ceiling. Token rates must
be set to current conservative rates for those limits to be meaningful.

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
different scope UUID, requires 32 successful independent sessions, and may emit
a 5% unsigned candidate proposal. It cannot contribute to production readiness
or activate that proposal. Production remains the default manual profile.

## Development

The bridge control path uses the Python standard library plus the `zstd` executable
for production trace and result compression. Run the offline tests with:

```sh
python -m unittest discover -s milk_harness -p 'test_*.py'
```

The `mechanics` profile and generated traffic prove object-store mechanics.
Production qualification requires real gateway traffic, 750 independent
sessions, hosted generation and validation, a passing incumbent/candidate score,
an operator-signed route, and a live routing/fallback check. Offline token-F1
mechanics do not establish semantic production quality.
