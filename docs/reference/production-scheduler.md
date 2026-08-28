# Production scheduler and operational records

`.github/workflows/production-loop.yml` is the only scheduler. Its fixed concurrency group queues overlapping scheduled, manual, and rerun attempts instead of cancelling them. That GitHub setting is defense in depth, not the singleton authority.

The `2/5 * * * *` schedule runs provider reconciliation and teardown only. It skips gateway `tick`, so an unattended run cannot create a launch claim or paid provider work. A manual dispatch can create work only when all three checks pass:

1. `authorize_provider_creates` is true;
2. `confirmed_run_config_sha256` exactly matches the reviewed `MILK_CONFIRMED_RUN_CONFIG_SHA256` environment value;
3. the separately credentialed gateway tick returns an accepted typed result.

The input hash alone is insufficient. `MILK_CONFIRMED_RUN_CONFIG_JSON` must be canonical `milk.confirmed-production-run-config.v2` JSON whose SHA-256 equals the input and `MILK_CONFIRMED_RUN_CONFIG_SHA256`. It fixes the Baseten-primary/Modal-fallback policy, Baseten team, winner alias, exact Modal workspace/environment/app and secret/volume identities, all five runtime image references, budget and timeout limits, harness commit, committed prompt/profile hashes, provider secret names, and every store identity. Modal remains available for ordinary GPU jobs, but paid Modal winner fallback fails before Modal preflight or launch until its host-only gateway credential handoff is crash-safe. Before `tick`, the gateway job verifies the canonical config, scope, gateway and teacher/student settings, and capture/control stores. Before granting provider create authority, the scheduler independently verifies the actual provider settings, images, evidence/control/operational-log/create-authority stores, and the gateway-ingest control-write and route-read stores.

The fixed artifact inventory is `prompts/iterate.txt`, `decision.schema.json`, `contracts/snapshot-analyzer.json`, the teacher profile and model manifest, both student image provenance records, the shared chat template, Qwen profile, and Qwen model manifest. The Prime-only student-train image receives the pinned Qwen base as a read-only mount. The vLLM-only student-branch image receives only gateway-materialized merged candidate files. The worker verifies the committed 13-file Qwen manifest before any training work. The manifest stores hashes only, never prompt text or gateway secrets.

Every provider pass, including reconcile-only cron, must first acquire the evidence-R2 compare-and-swap lease at `state/v1/campaigns/{campaign_id}/provider-pass-lease.json`. The canonical token binds the campaign, scope, owner, holder, revision, acquisition and expiry, exact evidence-store identity, and one immutable create-authorization object. That authorization is written through a dedicated credential in a separate private bucket. The jobs container receives a different read-only credential for that bucket, so it can verify authorization but cannot mint it. There is no caller flag that enables provider creation.

`python3 -m milk_harness.jobs` requires the token, verifies both live records before setup and every provider request, and burns a create-only claim so the token cannot start a second process. The same pass reconciles Baseten and Modal, tears down authorized winner deployments, and emits immutable evidence references for gateway-owned winner and teardown results. The 15-minute lease exceeds the workflow's 12-minute hard timeout; the workflow does not release it until the child has exited, gateway handoffs have run, and the pass is archived. An expired lease can be replaced atomically. Production assumes participating runner clocks remain within 30 seconds; the three-minute timeout margin exceeds that bound, while verification still fails at the exact recorded expiry. A local operator must use the same scheduler boundary around exactly one jobs pass; direct jobs invocation is reconcile-only unless it carries a valid external authorization.

The pass receipt also records the create request, input-hash result, actual-configuration verification, final grant, and confirmed configuration SHA-256. A failed or mismatched confirmation still permits reconciliation, never provider creation.

## Credential boundaries

`milk-gateway-tick-prod` contains one private GHCR pull identity, the gateway tick configuration, and capture/control object-store credentials. It has no provider, evidence, operational-log, route, signing, or registry-write authority.

`milk-provider-jobs-prod` contains a private GHCR pull identity, read-only gateway control credentials for discovery, read-write evidence credentials, Baseten and Modal API credentials, and a dedicated operational-log R2 identity. The scheduler host alone receives the create-authority writer; the jobs child receives only the distinct read-only credential when create authority was granted. It receives provider-side secret names, not their values, for the GPU workloads.

The same host also holds `MILK_GATEWAY_INGEST_CONFIG_JSON`, a dedicated gateway control writer under `MILK_GATEWAY_INGEST_CONTROL_R2_*`, and a dedicated route reader under `MILK_GATEWAY_INGEST_ROUTE_R2_*`. None enters the jobs container. After a successful jobs result, the host downloads only the evidence objects named by its bounded typed references and verifies every key, byte count, and SHA-256. It then starts the exact gateway image once per object. Winner ingestion receives only the control writer. Teardown ingestion also receives the route reader. Those gateway containers receive no Baseten token, Modal token, provider-control reader, evidence credential, create-authority credential, signing key, capture credential, or route writer.

The gateway-ingest config and its two credential identities are checked against the same confirmed v2 manifest before the first mutation. Each result is mounted as one current-owner `0600` file, as required by the gateway input boundary. Each gateway process has a 90-second hard timeout plus a 10-second termination grace period inside the workflow's 12-minute cap. A failed or partial ingest marks the provider pass failed before summary and archive. Recovered references are emitted again on the next pass, and gateway ingestion is immutable and retry-safe.

Baseten winner credentials use the gateway repository's `manage-candidate-credential.py`, never jobs code. The workflow checks out the exact `MILK_GATEWAY_SOURCE_COMMIT`, requires it to equal the private gateway image's `org.opencontainers.image.revision`, and installs only the Wrangler version fixed by the gateway repository's lockfile. The helper process receives a cleared environment containing only PATH, the account ID, and a dedicated Cloudflare candidate-secret token. The permanent container-admin key crosses into the helper through an anonymous pipe on descriptor 3; it is never a helper argument, environment value, or file. The helper runs as UID 65532, owns a fresh `0700` runtime directory, and exposes one `0600` Unix socket. The jobs container keeps its admitted UID and receives only that socket at `/run/milk-candidate-key.sock`, not either Cloudflare credential.

Each helper invocation accepts at most one canonical Baseten install, verify, or remove request. It removes the socket immediately after accepting the verified same-UID client. Therefore a socket that remains after jobs exit proves the helper was idle and can be stopped; an absent socket means a request is in flight and the workflow waits for the bounded helper result. A pass with no candidate transaction stops the idle listener immediately instead of waiting for its outer timeout. Helper and package-manager streams are truncated in private scratch space and only a generic failure contributes to the sanitized scheduler summary.

The provider job emits only a lowercase-hex student job ID plus `route_required` and `route_candidate_ready` booleans derived from its whitelisted winner and candidate-credential evidence counts. A winner never reaches traffic until its gateway credential proof is present.

The separate `milk-route-control-prod` job runs only in the explicitly confirmed manual workflow that created the winner. It extracts the gateway binary from the exact admitted private image, requires the image revision to equal the exact private gateway source checkout, and uses that checkout's signer and locked OpenAI Node SDK. It has route control/read-write credentials and a signing key, but no Baseten token, Modal token, provider create authority, container-admin key, or candidate-provider key.

`advance-winner-route` is the route authority. With no live route it prepares one fixed 100-basis-point, 900-second canary. A retry adopts the exact existing revision and original deadline instead of extending it. Before a new canary is published, the job proves that the owner-only SDK credential is a configured traffic key, its cohort falls inside that exact 1% HMAC sample, and its model and reasoning setting match the signed manifest. The signer publishes the manifest, and a second advance call must observe the same revision and deadline.

Before the one paid request, the job creates an immutable content-free intent in the dedicated route-evidence bucket. An existing intent without a receipt is ambiguous and is never replayed, so a crash cannot silently issue a second paid request. The exact OpenAI Node SDK request must return the expected route revision, `candidate` target, and candidate, artifact, and deployment digests. Its content-free receipt is stored create-only and read back. On success the job waits only until the original canary deadline. On any smoke or workflow failure after a canary may be live, it attempts signed zero immediately; normal completion publishes the fixed 0-basis-point, 60-second successor and verifies that the gateway reports it done. There is no synthetic fallback injection.

Signed zero makes the next queued, scheduled, or manually started provider pass reconcile-only for a Baseten winner. That pass consumes the zero-route authorization, removes the gateway credential, proves zero provider compute, ingests the immutable teardown result, and only then frees the slot. Paid Modal winner fallback remains disabled because the current jobs pass cannot install its pre-provisioned key through the host-only Cloudflare helper or prove removal before Modal teardown. The paid proof is complete only when the final provider receipt is present.

For the first controlled proof, `MILK_GATEWAY_WORKER_VERSION_ID` is the exact Worker version from the verified deployment receipt. Candidate removal changes the verified release lineage, so that static anchor is intentionally one-cycle authority. Every later create-capable run fails closed until an operator verifies the current Cloudflare release and replaces the protected variable. This release does not add another lineage service or database.

Both environments require exact digest references for `milk-gateway`, `milk-jobs`, `milk-student-train`, `milk-student-branch`, and `milk-teacher-gpt-oss`. Provider jobs also require `MILK_IMAGE_RELEASE_SHA256`; the jobs image loads and verifies that immutable release and each image admission from evidence before provider setup.

## Operational-log boundary

Provider stdout is limited to canonical `milk.jobs-pass.v3`; provider stderr is never uploaded. Gateway-ingest stdout and stderr are captured in the private scratch directory and truncated immediately. The scheduler accepts only fixed GitHub metadata and whitelisted typed result fields. It discards streams after bounded validation without retaining their contents or hashes. Prompts, model outputs, datasets, secrets, headers, signed URLs, and environment dumps are forbidden.

Each attempt writes one create-only sanitized artifact:

```text
operational/v1/scheduler-passes/{pass_id}/archive.json
```

The separate evidence bucket receives only a create-only, content-free reference:

```text
operational-log-references/v1/scheduler-passes/{pass_id}.json
```

`pass_id` is derived from the GitHub repository ID, run ID, and run attempt. A retry with different bytes fails instead of overwriting either object. The artifact lists explicit gaps for failures before the archive store is reachable, the current GitHub run log, GHCR client stream, Cloudflare runtime, Baseten terminal stream, Modal, local operator runs, gateway runtime, and live retention-policy observation. It must not be described as literal all-platform log completeness.

## R2 release gate

Before enabling the workflow:

- use a private R2 bucket dedicated to `MILK_OPS_LOG_R2_*`, with an access key different from control and evidence;
- use another private R2 bucket for provider create authorizations, with distinct write and read-only credentials; pass only `MILK_CREATE_AUTHORITY_READ_R2_*` into the jobs container and verify that credential cannot create or replace objects;
- provision Baseten payment-linked resources and long-lived provider credentials only after action-time operator confirmation; the workflow consumes existing credentials and does not create them;
- restrict `MILK_GATEWAY_INGEST_CONTROL_R2_*` to the gateway control bucket with object read/write and restrict `MILK_GATEWAY_INGEST_ROUTE_R2_*` to route reads; verify neither credential can access the other partition;
- verify `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET` are limited to the confirmed workspace and environment and that every configured Modal name matches its immutable object ID;
- provision separate teacher and student-train Modal volumes, bind their exact names and object IDs as `MILK_MODAL_TEACHER_VOLUME_*` and `MILK_MODAL_STUDENT_TRAIN_VOLUME_*`, and populate the student-train volume root with only the pinned Qwen manifest files;
- set `MILK_GATEWAY_SOURCE_COMMIT` to the exact 40-character commit recorded on the admitted gateway image; provide a contents-read-only `MILK_GATEWAY_SOURCE_READ_TOKEN` for that private repository;
- set the exact Cloudflare application ID/version, container image, Worker version ID, account ID, a candidate-secret-only API token, and the Worker-only container-admin key used by the bounded helper;
- create `milk-route-control-prod` with a canonical gateway config, the exact 32-byte lowercase-hex route secret, an owner-only Ed25519 signing key, and an owner-only SDK credential whose traffic cohort is preselected for the fixed 1% canary;
- restrict `MILK_ROUTE_CONTROL_R2_*` and `MILK_ROUTE_ROUTE_R2_*` to the configured control and route buckets with the read/write access required by gateway publication, and verify neither identity can access the other partition;
- use a dedicated private `MILK_ROUTE_EVIDENCE_R2_*` bucket or prefix for create-only smoke intents and content-free receipts; lock those records indefinitely and keep its credential out of the gateway process;
- after the first candidate removal, verify the current Cloudflare application/container/Worker release receipt and replace `MILK_GATEWAY_WORKER_VERSION_ID` before approving another create-capable run;
- disable public development URLs and custom domains on the private buckets;
- configure a 90-day bucket lock and a 90-day lifecycle deletion rule for `operational/v1/scheduler-passes/`;
- configure an indefinite bucket lock for `operational-log-references/v1/scheduler-passes/` in the evidence bucket;
- leave the mutable `state/v1/campaigns/` lease and budget prefixes outside object-lock rules so conditional replacement remains possible;
- verify both rules and retain the verification receipt outside the mutable budget/state prefixes.

Cloudflare documents [bucket lock rules](https://developers.cloudflare.com/r2/buckets/bucket-locks/) and [object lifecycle rules](https://developers.cloudflare.com/r2/buckets/object-lifecycles/). The scheduler writer deliberately has no bucket-policy authority and records that it did not observe those rules. Live policy setup and read-back verification remain a deployment gate, not an application assertion.
