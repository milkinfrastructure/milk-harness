# Paid-run policy

The provider GPU budget authority is implemented in `milk_harness/budget.py`. It bounds authorized Baseten GPU work; it does not claim to cap a cloud-provider invoice or unrelated Cloudflare, OpenAI, GitHub, registry, storage, or network charges.

- absolute ceiling: $1,000 (`1,000,000,000` microusd);
- new-launch cutoff: $850 (`850,000,000` microusd);
- protected running-work and teardown reserve: $150 (`150,000,000` microusd).

The first cloud-mechanics proof has a separate `$15` external reserve: `$10` for one month of [Cloudflare Workers Paid and bounded included-resource use](https://developers.cloudflare.com/workers/platform/pricing/), `$2` for the exact capped [OpenAI baseline calls](https://developers.openai.com/api/docs/models/gpt-5.4), and `$3` contingency. [Public-repository standard GitHub-hosted Actions](https://docs.github.com/en/billing/concepts/product-billing/github-actions) and [Container registry bandwidth](https://docs.github.com/en/billing/concepts/product-billing/github-packages) are currently unbilled, and the proof stays inside [R2's included monthly storage and operation allowances](https://developers.cloudflare.com/r2/pricing/). The fixed eval validator derives a `$142.50` maximum GPU reservation from 16 one-hour teacher jobs, one 30-minute train job, three 30-minute branch jobs, and one one-hour winner. The `$160` GPU ceiling leaves `$17.50` of headroom. The exact proof therefore authorizes at most `$175` all-in: `$160` GPU plus `$15` external. Rerunning any paid or SDK step requires a new action-time confirmation; after terminal signed-zero evidence, the five-minute production schedule is disabled. Continued hosted operation uses a separate monthly operating authorization rather than silently extending this proof envelope.

The external reserve admits one fixed `gpt-5.4` SDK contract, SHA-256 `cf9e41c3220544bc163a6dfb82721154a8e078c9db3c9fa86a148a84ea275263`: one deployment-baseline call, 320 generated-mechanics calls, one candidate call, and two saturation-fallback calls. That is exactly 324 calls, split 322 baseline and two candidate. Generated mechanics writes a create-only intent before traffic and a content-free receipt after success; an intent without a receipt is ambiguous and is never replayed. The cloud proof has not run, so this budget is an authorization ceiling, not recorded spend.

The immutable campaign authority binds one Baseten training project and one Baseten serving team. Both roles share the same mutable R2 `state/v1/campaigns/{campaign_id}/budget-head.json` `If-Match` CAS head; there is no second provider or serving budget. A run may switch roles only while preparing. Its first immutable reservation intent freezes the exact role and identity across an ambiguous create or crash. Modal identities and pricing inputs are rejected.

Every provider create requires both a one-time externally written action authorization and a pessimistic reservation before the request. The jobs container has read-only access to the separate authorization bucket and cannot turn a reconcile-only pass into a paid pass. A new reservation uses the role's current receipt: `state/v1/active-baseten-training-h100-pricing.json` or `state/v1/active-baseten-serving-h100-pricing.json`. Each must be no more than 24 hours old. The two Baseten v4 receipts independently bind training-project and serving-team identity to the current official H100 rate of `108,330` microusd/minute and the conservative `125,000` microusd/minute reservation rate. Each also binds a canonical operator observation containing the exact `https://docs.baseten.co/deployment/resources` URL, observation time, parsed rate, and SHA-256 of the archived source body. Active-receipt validation reloads and hashes the receipt, observation, and source body. The exact provider role, identity, immutable pricing digest, provider rate, and reservation rate are frozen into reservations and settlements. A pricing refresh cannot change a run already in progress.

Terminal execution intervals settle to conservative `accounted_minutes` and `accounted_microusd`; these values are not provider invoice or observed billing truth. The budget head records this as `committed_microusd`. Unknown or ambiguous execution retains the full reservation. Expired current pricing blocks only new reservations; reconciliation, settlement, finalization, stop, and teardown continue from the immutable price binding.

Evidence failure blocks new launches but reconciliation continues. A Baseten stop POST requires a successfully created immutable attempt intent. If intent persistence fails, no stop request is sent. Each execution has at most three durable stop-attempt slots; an intent without a result is burned rather than replayed blindly. The immutable worker deadline remains the cost backstop until evidence storage recovers.

Reconciliation is role-scoped. Baseten training considers only outstanding entries bound to the exact training project; Baseten serving considers only the exact serving team. Both reconcile against the same campaign budget head without consuming the other role's work.

## Operator bootstrap and refresh

`milk_harness.budget_operator` is the only command for creating the singleton campaign authority and refreshing both current pricing receipts. It accepts no provider credentials, makes no provider request, and cannot create compute. The jobs image does not contain this module.

Prepare canonical `milk.budget-authority-operator-input.v2` bytes using `milk_harness.evidence.canonical_json`. The exact fields are `schema_version`, `campaign_id`, `baseten_project_id`, `baseten_team_name`, and `baseten_pricing_observation`. Build `baseten_pricing_observation` with `milk_harness.budget.baseten_pricing_observation(campaign_id, sha256(source_body), observed_at)`. Keep the exact downloaded Baseten source body in a separate regular file; its digest must match the nested observation.

Rehearse against a local evidence directory:

```sh
python3 -m milk_harness.budget_operator \
  --input /absolute/path/to/budget-operator-input.json \
  --baseten-source-body /absolute/path/to/baseten-resources.html \
  --confirm-campaign-id CAMPAIGN_SHA256 \
  --local-store /absolute/path/to/local-evidence
```

Production R2 access is explicit. In a credential-clean shell containing only `MILK_EVIDENCE_R2_*` create/get/replace authority, replace `--local-store ...` with `--r2`. The command archives the canonical input, content-addressed source evidence, immutable receipts, and a content-addressed result before printing the result. Malformed, stale, future, mismatched, conflicting, or noncanonical evidence fails before provider work; this command has no provider-work path.

Run the command with a strictly newer Baseten observation before the current receipts reach 24 hours. Replaying the same current input is safe. An older or same-time refresh is rejected, and expired receipts block new reservations without blocking teardown.
