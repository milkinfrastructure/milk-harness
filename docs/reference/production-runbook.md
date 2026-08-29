# Production runbook

This is the ordered operator path for one Milk deployment. The commands are CPU-only. Provider GPU work begins only at the separately confirmed paid dispatch.

## 1. Release immutable images

From clean published `main` checkouts:

```sh
cd /absolute/path/to/milk-gateway
tools/build-private-gateway.sh /absolute/new/gateway-release-evidence

cd /absolute/path/to/milk-harness
deploy/build-images.sh \
  <admitted-gateway-image-by-digest> \
  /absolute/new/harness-release-evidence
```

Both commands produce content-free local receipts. Do not continue until each image is private, digest-addressed, registry-read back, and admitted by its release receipt. A selective harness rebuild may use `--reuse-release-dir`; it must pass that script's immutable dependency checks.

## 2. Deploy the gateway

Bootstrap the first Cloudflare application:

```sh
cd /absolute/path/to/milk-gateway
tools/deploy-private-gateway.sh --bootstrap \
  /absolute/gateway-release-evidence \
  /absolute/new/gateway-deploy-evidence \
  /absolute/mechanics-credential.json \
  /absolute/bootstrap-secrets.json
```

For an existing application, deploy one atomic image and config version:

```sh
tools/deploy-private-gateway.sh \
  /absolute/gateway-release-evidence \
  <cloudflare-application-id> \
  /absolute/new/gateway-deploy-evidence \
  /absolute/mechanics-credential.json \
  /absolute/gateway-config.json
```

The deploy command verifies exact config health and the official-SDK baseline before sealing `current.json`. A failed non-bootstrap deploy restores and verifies the prior version.

## 3. Admit one eval

Provision the three GitHub environments and distinct credentials in [`production-scheduler.md`](production-scheduler.md). Then set only the two repository variables:

```sh
cd /absolute/path/to/milk-harness
gh variable set MILK_EVAL_ID --body '<stable-eval-id>'
gh variable set MILK_EVAL_CONFIG_JSON </absolute/eval.json
```

Verify the canonical outer document locally and retain its printed SHA-256. The eval must bind the exact gateway deployment receipt, release receipts, image digests, provider resources, credential identities, and fixed proof contract.

## 4. Start reconciliation

```sh
gh workflow enable production-loop.yml
gh workflow run production-loop.yml --ref main \
  -f authorize_provider_creates=false \
  -f authorize_mechanics_traffic=false \
  -f managed_eval_id='<stable-eval-id>'
```

The five-minute schedule reloads the exact repository config, observes completed R2 traffic, reconciles existing work, and exits. It cannot authorize a provider create.

## 5. Run the bounded mechanics proof

After explicit action-time confirmation, write the exact eval digest into both one-use dispatches. Generate traffic once:

```sh
gh workflow run production-loop.yml --ref main \
  -f authorize_provider_creates=false \
  -f authorize_mechanics_traffic=true \
  -f confirmed_run_config_sha256='<canonical-eval-sha256>' \
  -f managed_eval_id='<stable-eval-id>'
```

Then authorize one provider pass at a time:

```sh
gh workflow run production-loop.yml --ref main \
  -f authorize_provider_creates=true \
  -f authorize_mechanics_traffic=false \
  -f confirmed_run_config_sha256='<canonical-eval-sha256>' \
  -f managed_eval_id='<stable-eval-id>'
```

The fixed proof is at most 324 official-SDK calls and a `$175` all-in envelope: `$160` GPU authorization with a calculated `$142.50` maximum workload, plus `$15` external reserve. Scheduled runs never extend that authority.

## 6. Close the proof

Do not call the release qualified until immutable receipts agree on the exact eval, sources, images, deployment, candidate, route, and provider state. Required terminal evidence is:

1. completed baseline capture;
2. admitted Baseten teacher result;
3. train/merge and three ordered eval branches;
4. deterministic winner, authenticated canary, and observed fallback;
5. active signed-zero route, removed candidate credential, terminated winner, and both providers observed at zero compute.

Record the exact receipts and remaining gaps in [`production-status.md`](production-status.md). Generated mechanics proves the cloud machinery only; real captured traffic remains the production-qualification gate.
