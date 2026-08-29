# Exo tool

This source installs one `milk` model tool. Its only argument is `action`, one
of `status`, `reconcile`, or `run_confirmed`. The fixed host command receives
that value as its only subcommand. `run_confirmed` does not create approval; the
host command must consume an existing one-use confirmation before paid work.

Install the host command from the same exact checkout. The installer fixes the
command at `/opt/milk/bin/milk-managed` and creates `/var/lib/milk` as a `0700`
directory owned by the Exo service user. It never creates approval.

```sh
EXO_SERVICE_USER=exo
sudo tools/exo/install-host-command "$EXO_SERVICE_USER"
```

The Exo service environment must provide authenticated `gh` access to the
private repository and the exact `MILK_CONFIRMED_RUN_CONFIG_SHA256`. Tool
initialization supplies only the fixed command path; it does not inject either.

Install from the private repository at one exact 40-character commit with
Exo's `manage_tool`:

```json
{
  "action": "install",
  "toolId": null,
  "source": {
    "type": "git",
    "repository": "git@github.com:milkinfrastructure/milk-harness.git",
    "commit": "<exact-commit-sha>",
    "subdirectory": "tools/exo"
  },
  "initialization": "{\"command\":\"/opt/milk/bin/milk-managed\"}"
}
```

Exo checks out that commit, copies only `tools/exo` into its managed tool
store, validates `exo-tool.json` and the schemas, then loads `index.mjs` on the
next model round. The initialized command path is not exposed as a model
argument.

For one paid pass, an operator writes the reviewed configuration SHA followed
by one newline as a `0600` file owned by the service user. The parent directory
is already private, and the command atomically renames this file before it
validates or dispatches it.

```sh
APPROVED_SHA256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
approval_source=$(mktemp)
chmod 0600 "$approval_source"
printf '%s\n' "$APPROVED_SHA256" >"$approval_source"
sudo install -o "$EXO_SERVICE_USER" -g "$(id -gn "$EXO_SERVICE_USER")" -m 0600 \
  "$approval_source" /var/lib/milk/run-confirmed.approval
rm -f -- "$approval_source"
```

Before any dispatch, the command persists a unique 128-bit request ID and a
`pending` record in `/var/lib/milk/managed-dispatch.state`. The workflow places
that ID in its run name. The command then resolves and persists exactly one
GitHub Actions database ID for `main` and `workflow_dispatch`; `status` reads
only that run ID. A missing or ambiguous correlation remains pending and blocks
another dispatch. Only an exact run with conclusion `success` reports
`complete`. An atomic `managed-dispatch.state.lock` directory serializes all
host-command calls. A hard-kill may leave that lock behind; it is never removed
automatically, so an operator must inspect the process and GitHub run before
removing it.

The command must write exactly this content-free JSON object on stdout:

```json
{"ok":true,"state":"idle","changed":false,"approval_required":false}
```

Allowed states are `idle`, `ready`, `waiting_for_confirmation`, `running`,
`complete`, `blocked`, and `failed`. Unknown fields, malformed output, stderr,
and process errors are never returned to the model.
