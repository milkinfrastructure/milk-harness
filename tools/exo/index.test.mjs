import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import milkTool, { runFixedCommand } from "./index.mjs";

const VALID = {
  ok: true,
  state: "idle",
  changed: false,
  approval_required: false,
};

test("manifest and model arguments stay minimal", async () => {
  const manifest = JSON.parse(
    await readFile(new URL("./exo-tool.json", import.meta.url), "utf8"),
  );
  assert.deepEqual(Object.keys(manifest).sort(), [
    "id",
    "module",
    "schemaVersion",
  ]);
  assert.deepEqual(Object.keys(milkTool.definition.parameters.properties), [
    "action",
  ]);
  assert.deepEqual(milkTool.definition.parameters.required, ["action"]);
  assert.equal(milkTool.definition.parameters.additionalProperties, false);
  assert.deepEqual(milkTool.definition.parameters.properties.action.enum, [
    "status",
    "reconcile",
    "run_confirmed",
  ]);
});

test("initialization requires one fixed absolute command", async () => {
  assert.throws(
    () => milkTool.initialize({ command: "relative-command" }),
    /absolute path/,
  );
  const command = await executable(`
if (process.argv.length !== 3) process.exit(64);
process.stdout.write(JSON.stringify(${JSON.stringify(VALID)}));
`);
  const handler = milkTool.initialize({ command });
  for (const action of ["status", "reconcile", "run_confirmed"]) {
    assert.deepEqual(await handler.execute({ action }), {
      ok: true,
      action,
      state: "idle",
      changed: false,
      approvalRequired: false,
      code: "ok",
    });
  }
  assert.throws(
    () => handler.execute({ action: "status", provider: "not-allowed" }),
    /only a supported action/,
  );
});

test("command output cannot pass arbitrary content into the model", async () => {
  const secret = "ambient-secret-must-not-return";
  const command = await executable(`
process.stdout.write(JSON.stringify({ ...${JSON.stringify(VALID)}, secret: ${JSON.stringify(secret)} }));
`);
  const result = await runFixedCommand(command, "status");
  assert.deepEqual(result, {
    ok: false,
    action: "status",
    state: "failed",
    changed: false,
    approvalRequired: false,
    code: "invalid_output",
  });
  assert.doesNotMatch(JSON.stringify(result), new RegExp(secret));
});

test("timeout, process failure, and oversized output return fixed errors", async () => {
  const slow = await executable("setTimeout(() => {}, 1_000);");
  assert.equal(
    (await runFixedCommand(slow, "reconcile", { timeoutMs: 20 })).code,
    "timeout",
  );

  const failed = await executable(`
process.stderr.write("ambient-secret-must-not-return");
process.exit(1);
`);
  assert.equal(
    (await runFixedCommand(failed, "status")).code,
    "process_failed",
  );

  const noisy = await executable('process.stdout.write("x".repeat(4096));');
  assert.equal(
    (await runFixedCommand(noisy, "status", { maxBuffer: 128 })).code,
    "output_too_large",
  );
});

async function executable(source) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "milk-exo-test-"));
  const command = path.join(directory, "command.mjs");
  await writeFile(command, `#!/usr/bin/env node\n${source}\n`);
  await chmod(command, 0o700);
  test.after(() => rm(directory, { recursive: true, force: true }));
  return command;
}
