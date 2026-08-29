# Exo tool

This directory provides one `milk` model tool. Every call requires exactly:

```json
{"action":"status","eval_id":"<64 lowercase hex>"}
```

`action` is `status`, `reconcile`, or `run_confirmed`. `eval_id` is the stable
campaign/eval ID of one exact document admitted by an operator. The model cannot
provide a path, repository, workflow, branch, provider, or credential.

## Install

Install the host command from the same exact checkout. The installer fixes the
command at `/opt/milk/bin/milk-managed`, creates the root-owned admission
directory `/etc/milk/evals`, and creates the service-owned state root
`/var/lib/milk/evals`.

```sh
EXO_SERVICE_USER=exo
sudo tools/exo/install-host-command "$EXO_SERVICE_USER"
```

The Exo service environment needs authenticated `gh` access to the target
repository. Tool initialization supplies only the fixed command path. Replace
`YOUR_ORG` with the owner of the checkout:

```json
{
  "action": "install",
  "toolId": null,
  "source": {
    "type": "git",
    "repository": "git@github.com:YOUR_ORG/milk-harness.git",
    "commit": "<exact-40-character-commit-sha>",
    "subdirectory": "tools/exo"
  },
  "initialization": "{\"command\":\"/opt/milk/bin/milk-managed\"}"
}
```

Exo checks out that commit, copies only `tools/exo`, validates the manifest and
schemas, and loads `index.mjs` on the next model round. The command path and
host environment are not model arguments.

## Fork target

The installed command defaults to Milk production. A self-host operator may
set these values in the Exo service environment:

```text
MILK_MANAGED_REPOSITORY=github.com/YOUR_ORG/milk-harness
MILK_MANAGED_WORKFLOW=self-host-loop.yml
MILK_MANAGED_WORKFLOW_REF=main
```

The repository must be `github.com/OWNER/REPOSITORY`; the workflow must be a
`.yml` or `.yaml` filename; and the ref must be a simple branch or tag. The
model cannot override them. The bundled `production-loop.yml` remains a
Milk-managed production workflow with strict Milk image and release admission;
it is not a custom-image self-host template.

Run the non-dispatching config/control smoke in
[`examples/self-host`](../../examples/self-host) before installing the command.

## Admit one eval

Read the stable eval ID from the exact reviewed document, then install it under
that ID. The
directory and document remain root-owned; the Exo service group has read-only
access.

```sh
EVAL_DOCUMENT=/absolute/path/reviewed-eval.json
EVAL_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest"]["campaign_id"])' "$EVAL_DOCUMENT")
EXO_SERVICE_USER=exo
sudo install -o root -g "$(id -gn "$EXO_SERVICE_USER")" -m 0440 \
  "$EVAL_DOCUMENT" "/etc/milk/evals/$EVAL_ID.json"
```

The host command accepts only `/etc/milk/evals/$EVAL_ID.json`. Before every
action it rejects symbolic links, wrong ownership or modes, oversized files,
and any document whose manifest campaign ID or gateway eval ID differs from
`eval_id`. At dispatch it computes the exact document SHA-256 and sends that as
the workflow's manual confirmation.

State, lock, and approval are isolated under
`/var/lib/milk/evals/$EVAL_ID/`. The command creates that `0700` service-owned
directory on the first call. A request ID is `$EVAL_ID-<128-bit nonce>`, so
GitHub run correlation is also eval-specific.

## Confirm one paid pass

`run_confirmed` never creates approval. After calling `status` once for the
eval, an operator may install one service-owned approval containing the
SHA-256 of the exact admitted document bytes and one newline:

```sh
approval_source=$(mktemp)
chmod 0600 "$approval_source"
EVAL_CONFIRMATION_SHA256=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$EVAL_DOCUMENT")
printf '%s\n' "$EVAL_CONFIRMATION_SHA256" >"$approval_source"
sudo install -o "$EXO_SERVICE_USER" -g "$(id -gn "$EXO_SERVICE_USER")" -m 0600 \
  "$approval_source" \
  "/var/lib/milk/evals/$EVAL_ID/run-confirmed.approval"
rm -f -- "$approval_source"
```

The command atomically consumes that file and requires it to match the admitted
document bytes validated for that call before it dispatches paid work. Changing
the document after approval blocks dispatch. Approval remains one-use; there is
no persistent activation.

Before any dispatch, the command writes a `pending` record, then resolves and
persists exactly one GitHub Actions database ID for `main` and
`workflow_dispatch`. A missing or ambiguous correlation stays pending and
blocks another dispatch. An atomic per-eval lock serializes calls for that eval
without blocking other evals. A hard kill may leave the lock behind; an
operator must inspect the process and GitHub run before removing it.

## Status contract

The fixed command writes one content-free JSON object on stdout:

```json
{"ok":true,"eval_id":"<64 lowercase hex>","state":"idle","dispatch_state":"idle","generation_done":false,"changed":false,"approval_required":false}
```

`state` is `idle`, `ready`, `waiting_for_confirmation`, `running`, `blocked`,
or `failed`. `dispatch_state` is `idle`, `pending`, `running`, `succeeded`,
`action_required`, `failed`, or `unknown`.

A successful GitHub workflow pass reports `state=ready`,
`dispatch_state=succeeded`, and `generation_done=false`. It never reports
generation complete because the workflow does not yet publish a typed,
content-free completion receipt bound to the GitHub run ID, eval ID, and exact
document SHA-256. That receipt must carry the gateway's authoritative
`max_decisions`, `claimed_decisions`, and `remaining_decisions`; only a validated
zero `remaining_decisions` may set `generation_done=true`. Workflow success and
free-form logs are not completion authority, so the command fails closed until
that receipt exists.

Unknown fields, malformed output, stderr, and process errors are never returned
to the model.
