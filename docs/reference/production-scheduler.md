# Manual bridge entry point

`.github/workflows/production-loop.yml` is manual-only. It runs one bounded
reconciliation pass and exits. Milk Man owns the schedule and invokes the same
fixed job call. This repository is not a third product or a resident scheduler.

The workflow has one `run-once` job in the existing
`milk-provider-jobs-prod` environment. It checks out the exact source, verifies
that `zstd` is available, and runs:

```sh
python -m milk_harness run-once --config deploy/run-once.${MILK_RUN_PROFILE}.json
```

Manual runs may select `production` or the isolated `mechanics` profile. The
concurrency group permits only one pass at a time and does not cancel an
in-flight pass.

The job receives only the Baseten teacher key and the scoped control-store
credentials named in the workflow. Account and bucket also come from the
selected environment. Checked-in configuration fixes scope, model, sample
limits, call limits, and spend limits. The bridge cannot sign or publish
routes, alter its configuration, dispatch arbitrary commands, or authorize
work outside those bounds.
