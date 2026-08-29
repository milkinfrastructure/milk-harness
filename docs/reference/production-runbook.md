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

From a credential-clean shell containing only the `MILK_EVIDENCE_R2_*` authority, publish both exact local releases:

```sh
cd /absolute/path/to/milk-harness
python3 -m milk_harness.publish_image_admission \
  --gateway-release-dir /absolute/new/gateway-release-evidence
python3 -m milk_harness.publish_image_admission \
  --release-dir /absolute/new/harness-release-evidence
```

Each command uses create-only object keys and succeeds only after reading back the exact admission and release bodies. Do not supply builder, provider, traffic, GitHub, or other object-store credentials to either process.

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

After the verified gateway deployment exists, materialize the exact canonical eval document. From a credential-clean shell containing only the `MILK_EVIDENCE_R2_*` authority, publish and read it back before setting repository variables or authorizing paid work:

```sh
cd /absolute/path/to/milk-harness
python3 -m milk_harness.publish_eval \
  --document /absolute/eval.json \
  --eval-id '<stable-eval-id>' \
  --harness-source-commit "$(git rev-parse HEAD)" \
  --root .
```

The publisher validates the document against the checked-out harness source, creates its content-addressed object once, and succeeds only after exact body readback. Retain the printed outer-document SHA-256.

Provision the three GitHub environments and distinct credentials in
[`production-scheduler.md`](production-scheduler.md). In GitHub, open
**Settings -> Secrets and variables -> Actions -> Variables** and set only:

- `MILK_EVAL_ID` to the stable eval ID;
- `MILK_EVAL_CONFIG_JSON` to the exact canonical contents of `eval.json`.

The eval must bind the exact gateway deployment receipt, release receipts, image digests, provider resources, credential identities, and fixed proof contract.

## 4. Start reconciliation

In GitHub, open **Actions -> Production loop**, enable the workflow if needed,
then choose **Run workflow** on `main` with provider creates and mechanics
traffic both disabled. Set `managed_eval_id` to the stable eval ID and leave the
confirmation hash empty.

The five-minute schedule reloads the exact repository config, observes completed R2 traffic, reconciles existing work, and exits. It cannot authorize a provider create.

## 5. Run the bounded mechanics proof

After explicit action-time confirmation, use **Run workflow** twice with the
same stable eval ID and canonical eval SHA-256. First enable mechanics traffic
only. After its immutable receipt is present, run again with provider creates
only. Never enable both booleans in one dispatch and never reuse either one-use
authority.

The fixed proof is at most 324 official-SDK calls and a `$175` all-in envelope: `$160` GPU authorization with a calculated `$142.50` maximum workload, plus `$15` external reserve. Scheduled runs never extend that authority.

## 6. Close the proof

Do not call the release qualified until immutable receipts agree on the exact eval, sources, images, deployment, candidate, route, and provider state. Required terminal evidence is:

1. completed baseline capture;
2. admitted Baseten teacher result;
3. train/merge and three ordered eval branches;
4. deterministic winner, authenticated canary, and observed fallback;
5. active signed-zero route, removed candidate credential, terminated winner,
   and Baseten observed at zero compute.

Record the exact receipts and remaining gaps in [`production-status.md`](production-status.md). Generated mechanics proves the cloud machinery only; real captured traffic remains the production-qualification gate.
