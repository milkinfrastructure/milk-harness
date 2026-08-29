import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import milkTool, { runFixedCommand } from "./index.mjs";

const EVAL_ID = "a".repeat(64);
const VALID = {
  ok: true,
  eval_id: EVAL_ID,
  state: "idle",
  dispatch_state: "idle",
  generation_done: false,
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
  assert.deepEqual(
    Object.keys(milkTool.definition.parameters.properties).sort(),
    ["action", "eval_id"],
  );
  assert.deepEqual(milkTool.definition.parameters.required, [
    "action",
    "eval_id",
  ]);
  assert.equal(milkTool.definition.parameters.additionalProperties, false);
  assert.deepEqual(milkTool.definition.parameters.properties.action.enum, [
    "status",
    "reconcile",
    "run_confirmed",
  ]);
  assert.equal(
    milkTool.definition.parameters.properties.eval_id.pattern,
    "^[0-9a-f]{64}$",
  );
});

test("initialization requires one fixed absolute command", async () => {
  assert.throws(
    () => milkTool.initialize({ command: "relative-command" }),
    /absolute path/,
  );
  const command = await executable(`
if (process.argv.length !== 4) process.exit(64);
process.stdout.write(JSON.stringify(${JSON.stringify(VALID)}));
`);
  const handler = milkTool.initialize({ command });
  for (const action of ["status", "reconcile", "run_confirmed"]) {
    assert.deepEqual(await handler.execute({ action, eval_id: EVAL_ID }), {
      ok: true,
      action,
      evalId: EVAL_ID,
      state: "idle",
      dispatchState: "idle",
      generationDone: false,
      changed: false,
      approvalRequired: false,
      code: "ok",
    });
  }
  for (const args of [
    { action: "status" },
    { action: "status", eval_id: "A".repeat(64) },
    { action: "status", eval_id: "a".repeat(63) },
    { action: "status", eval_id: EVAL_ID, provider: "not-allowed" },
  ]) {
    assert.throws(
      () => handler.execute(args),
      /requires one supported action and eval_id/,
    );
  }
});

test("command output cannot pass arbitrary or cross-eval content", async () => {
  const secret = "ambient-secret-must-not-return";
  const extraField = await executable(`
process.stdout.write(JSON.stringify({ ...${JSON.stringify(VALID)}, secret: ${JSON.stringify(secret)} }));
`);
  const extraResult = await runFixedCommand(extraField, "status", EVAL_ID);
  assert.deepEqual(extraResult, failure("status", "invalid_output"));
  assert.doesNotMatch(JSON.stringify(extraResult), new RegExp(secret));

  const wrongEval = await executable(`
process.stdout.write(JSON.stringify({ ...${JSON.stringify(VALID)}, eval_id: "${"b".repeat(64)}" }));
`);
  assert.deepEqual(
    await runFixedCommand(wrongEval, "status", EVAL_ID),
    failure("status", "invalid_output"),
  );

  const falseCompletion = await executable(`
process.stdout.write(JSON.stringify({ ...${JSON.stringify(VALID)}, state: "complete", generation_done: true }));
`);
  assert.deepEqual(
    await runFixedCommand(falseCompletion, "status", EVAL_ID),
    failure("status", "invalid_output"),
  );

  const completed = await executable(`
process.stdout.write(JSON.stringify({ ...${JSON.stringify(VALID)}, state: "ready", dispatch_state: "succeeded", generation_done: true }));
`);
  assert.deepEqual(await runFixedCommand(completed, "status", EVAL_ID), {
    ok: true,
    action: "status",
    evalId: EVAL_ID,
    state: "ready",
    dispatchState: "succeeded",
    generationDone: true,
    changed: false,
    approvalRequired: false,
    code: "ok",
  });
});

test("timeout, process failure, and oversized output return fixed errors", async () => {
  const slow = await executable("setTimeout(() => {}, 1_000);");
  assert.equal(
    (
      await runFixedCommand(slow, "reconcile", EVAL_ID, { timeoutMs: 20 })
    ).code,
    "timeout",
  );

  const failed = await executable(`
process.stderr.write("ambient-secret-must-not-return");
process.exit(1);
`);
  assert.equal(
    (await runFixedCommand(failed, "status", EVAL_ID)).code,
    "process_failed",
  );

  const noisy = await executable('process.stdout.write("x".repeat(4096));');
  assert.equal(
    (
      await runFixedCommand(noisy, "status", EVAL_ID, { maxBuffer: 128 })
    ).code,
    "output_too_large",
  );
});

function failure(action, code) {
  return {
    ok: false,
    action,
    evalId: EVAL_ID,
    state: "failed",
    dispatchState: "unknown",
    generationDone: false,
    changed: false,
    approvalRequired: false,
    code,
  };
}

async function executable(source) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "milk-exo-test-"));
  const command = path.join(directory, "command.mjs");
  await writeFile(command, `#!/usr/bin/env node\n${source}\n`);
  await chmod(command, 0o700);
  test.after(() => rm(directory, { recursive: true, force: true }));
  return command;
}
