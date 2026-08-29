# Self-host control smoke

This smoke validates and materializes a canonical `milk.eval.v1` document, then exercises the local Exo host-command boundary. It is a config/control smoke, not provider execution or production qualification. It does not contact GitHub, Cloudflare, or Baseten, cannot create paid work, and uses no GPU.

Run it from the repository root with Python 3:

```sh
smoke_root=$(mktemp -d)
smoke_root=$(CDPATH= cd -- "$smoke_root" && pwd -P)
eval_document=$PWD/examples/self-host/milk.eval.example.json
eval_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest"]["campaign_id"])' "$eval_document")
harness_commit=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest"]["harness_source_commit"])' "$eval_document")

python3 -m milk_harness.eval_config \
  --document "$eval_document" \
  --eval-id "$eval_id" \
  --harness-source-commit "$harness_commit" \
  --root "$PWD" \
  --manifest-output "$smoke_root/manifest.json" \
  --gateway-config-output "$smoke_root/gateway.json" \
  --github-env "$smoke_root/github-env" \
  --github-output "$smoke_root/github-output"

mkdir -m 0750 "$smoke_root/evals"
mkdir -m 0700 "$smoke_root/state"
cp "$eval_document" "$smoke_root/evals/$eval_id.json"
chmod 0440 "$smoke_root/evals/$eval_id.json"

MILK_MANAGED_REPOSITORY=github.com/example/milk-harness \
MILK_MANAGED_EVAL_DIR="$smoke_root/evals" \
MILK_MANAGED_STATE_ROOT="$smoke_root/state" \
MILK_MANAGED_EVAL_OWNER_UID=$(id -u) \
  tools/exo/milk-managed status "$eval_id"
```

The final output is an `idle` status object. Remove `$smoke_root` when finished.

Every identifier, image digest, deployment receipt, store name, and provider resource in the example is fake. Do not install it as a repository variable and do not dispatch `reconcile` or `run_confirmed` with it.

The stable eval ID is exactly the example manifest's `campaign_id` and must also match `gateway_config.eval_id`. It is not the config hash. Validation separately computes the canonical outer document's SHA-256, which production uses as the explicit paid-dispatch confirmation.

The current provider workflow remains Milk-managed. It admits only Milk release receipts and immutable Milk image repositories. A fork may point `milk-managed` at an operator-owned GitHub repository, workflow file, and ref, but that workflow is operator-supplied. A self-host operator owns its eval configuration, storage credentials, provider credentials, images, domains, and spend controls; this repository does not yet include a turnkey custom-image or custom-domain provider deployment.
