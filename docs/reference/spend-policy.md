# Paid-run policy

The campaign budget authority is implemented in `milk_harness/budget.py`.

- absolute ceiling: $1,000 (`1,000,000,000` microusd);
- new-launch cutoff: $850 (`850,000,000` microusd);
- protected running-work and teardown reserve: $150 (`150,000,000` microusd).

The immutable campaign authority binds one Baseten training project, one Baseten serving team, and one Modal workspace, environment, and app. All three roles share the same mutable R2 `state/v1/campaigns/{campaign_id}/budget-head.json` `If-Match` CAS head; there is no second provider or serving budget. A run may switch roles only while preparing. Its first immutable reservation intent freezes the exact role and identity across an ambiguous create or crash.

Every provider create requires both a one-time externally written action authorization and a pessimistic reservation before the request. The jobs container has read-only access to the separate authorization bucket and cannot turn a reconcile-only pass into a paid pass. A new reservation uses the role's current receipt: `state/v1/active-baseten-training-h100-pricing.json`, `state/v1/active-baseten-serving-h100-pricing.json`, or `state/v1/active-modal-h100-sandbox-pricing.json`. Each must be no more than 24 hours old. The two Baseten v4 receipts independently bind training-project and serving-team identity to the current official H100 rate of `108,330` microusd/minute and the conservative `125,000` microusd/minute reservation rate. Each also binds a canonical operator observation containing the exact `https://docs.baseten.co/deployment/resources` URL, observation time, parsed rate, and SHA-256 of the archived source body. Active-receipt validation reloads and hashes the receipt, observation, and source body. The Modal receipt is normalized from an explicit immutable `modal billing rates --json` receipt and binds the archived exact command output by SHA-256; active validation reloads both. It proves that one H100 plus hard limits of eight physical CPU cores and 65,536 MiB memory in the default region costs at most that same reservation rate. The exact provider role, identity, immutable pricing digest, provider rate, and reservation rate are frozen into reservations and settlements. A pricing refresh cannot change a run already in progress.

Terminal execution intervals settle to conservative `accounted_minutes` and `accounted_microusd`; these values are not provider invoice or observed billing truth. The budget head records this as `committed_microusd`. Unknown or ambiguous execution retains the full reservation. Expired current pricing blocks only new reservations; reconciliation, settlement, finalization, stop, and teardown continue from the immutable price binding.

Evidence failure blocks new launches but reconciliation continues. A Baseten stop POST requires a successfully created immutable attempt intent. If intent persistence fails, no stop request is sent. Each execution has at most three durable stop-attempt slots; an intent without a result is burned rather than replayed blindly. The immutable worker deadline remains the cost backstop until evidence storage recovers.

Reconciliation is role-scoped. Baseten training considers only outstanding entries bound to the exact training project; Baseten serving considers only the exact serving team; Modal considers only its exact workspace, environment, and app. All reconcile against the same campaign budget head without consuming another role's work.

## Operator bootstrap and refresh

`milk_harness.budget_operator` is the only command for creating the singleton campaign authority and refreshing all three current pricing receipts. It accepts no provider credentials, makes no provider request, and cannot create compute. The jobs image does not contain this module.

Prepare canonical `milk.budget-authority-operator-input.v1` bytes using `milk_harness.evidence.canonical_json`. The exact fields are `schema_version`, `campaign_id`, `baseten_project_id`, `baseten_team_name`, `baseten_pricing_observation`, and `modal_rates_receipt`. Build `baseten_pricing_observation` with `milk_harness.budget.baseten_pricing_observation(campaign_id, sha256(source_body), observed_at)`. Supply the canonical `milk.modal-workspace-rates.v1` receipt as `modal_rates_receipt`. Keep the exact downloaded Baseten source body and exact `modal billing rates --json` output in separate regular files; their digests must match the nested observations.

Rehearse against a local evidence directory:

```sh
python3 -m milk_harness.budget_operator \
  --input /absolute/path/to/budget-operator-input.json \
  --baseten-source-body /absolute/path/to/baseten-resources.html \
  --modal-rates-source-body /absolute/path/to/modal-rates.json \
  --confirm-campaign-id CAMPAIGN_SHA256 \
  --local-store /absolute/path/to/local-evidence
```

Production R2 access is explicit. In a credential-clean shell containing only `MILK_EVIDENCE_R2_*` create/get/replace authority, replace `--local-store ...` with `--r2`. The command archives the canonical input, content-addressed source evidence, immutable receipts, and a content-addressed result before printing the result. Malformed, stale, future, mismatched, conflicting, or noncanonical evidence fails before provider work; this command has no provider-work path.

Run the command with strictly newer Baseten and Modal observations before the current receipts reach 24 hours. Replaying the same current input is safe. An older or same-time refresh is rejected, and expired receipts block new reservations without blocking teardown.
