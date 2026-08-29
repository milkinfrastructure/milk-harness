# Run evidence

[`baseline-builds.json`](baseline-builds.json) is the content-free manifest for the two pre-Milk Modal image builds. They are retained only as engineering evidence and are not releasable Milk artifacts.

The two recorded raw Modal build streams are held locally under ignored `evidence-local/baseline/modal/`; their current byte counts and SHA-256 digests match the manifest. They have not been copied to a private Milk bucket and are not committed to Git.

Baseten job evidence uses bounded typed records under:

```text
campaigns/v1/{campaign_id}/jobs/{run_id}/definition.json
campaigns/v1/{campaign_id}/jobs/{run_id}/reservation.json
campaigns/v1/{campaign_id}/jobs/{run_id}/launch-result.json
campaigns/v1/{campaign_id}/jobs/{run_id}/observations/...
campaigns/v1/{campaign_id}/jobs/{run_id}/logs/baseten/...
campaigns/v1/{campaign_id}/jobs/{run_id}/terminal.json
campaigns/v1/{campaign_id}/jobs/{run_id}/manifest-pages/...
campaigns/v1/{campaign_id}/jobs/{run_id}/manifest.json
```

Terminal Baseten logs are collected into bounded content-free records with timestamps, provider cursors, original byte counts, line SHA-256 digests, and explicit completeness/truncation state. Free-form line text is always withheld. Job manifests are paged and bounded to 4,096 referenced objects and 128 MiB. Provider status and stop attempts are durable and bounded; every stop POST requires an immutable attempt intent first.

This is not a raw operational-log archive. Local release, CI, scheduler, provider, gateway-ingest, deploy, rollback, and teardown paths retain bounded sanitized summaries or private artifact references; free-form build and command streams are hashed or discarded. Cloudflare native Container retention/export still cannot prove literal durable all-platform completeness. The separate private, non-authoritative operational-log bucket therefore needs live access-policy, object-lock, lifecycle, and readback qualification. Do not store prompts, model outputs, datasets, credentials, authorization headers, or signed URLs there; prompts and model outputs remain under gateway capture policy.

Offline CI writes a content-free receipt and private GitHub Actions log reference retained for 30 days. Route control creates and reads back one immutable content-free receipt at `route-operations/v1/{student_job_id}/{workflow_run_id}-{run_attempt}.json` in the dedicated route-evidence bucket, including the private GitHub Actions log reference and signed-zero outcome.

The authority store contains only typed content-free receipts and immutable hash references. It must not be described as an all-logs archive. The current `campaigns/v1/` prefix also contains mutable budget and reconcile heads; do not apply an indefinite retention lock to that mixed prefix until mutable state is separated.

Baseten winner serving is hard-disabled before deployment while the admitted image requires a root bootstrap to isolate control-store secrets and then drops to UID 65532. Baseten's [no-build custom-server contract](https://docs.baseten.co/development/model/custom-server) requires support enablement, forbids root as `run_as_user_id`, and leaves declared secrets mounted for the server lifetime. `student-branch` inherits UID 2000, but the current Truss path cannot invoke it safely: a future path must set `run_as_user_id: 2000`, start `student-run.sh serve` directly with an immutable pre-materialized model and manifest, and mount no control-store secrets. Re-enable the source gate only after that path and the team entitlement are verified.
