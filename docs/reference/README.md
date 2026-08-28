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

The authority store contains only typed content-free receipts and immutable hash references. It must not be described as an all-logs archive. The current `campaigns/v1/` prefix also contains mutable budget and reconcile heads; do not apply an indefinite retention lock to that mixed prefix until mutable state is separated.
