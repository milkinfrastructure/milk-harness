import { execFile } from "node:child_process";
import path from "node:path";

const ACTIONS = ["status", "reconcile", "run_confirmed"];
const EVAL_ID = /^[0-9a-f]{64}$/;
const STATES = [
  "idle",
  "ready",
  "waiting_for_confirmation",
  "running",
  "blocked",
  "failed",
];
const DISPATCH_STATES = [
  "idle",
  "pending",
  "running",
  "succeeded",
  "action_required",
  "failed",
  "unknown",
];
const OUTPUT_KEYS = [
  "approval_required",
  "changed",
  "dispatch_state",
  "eval_id",
  "generation_done",
  "ok",
  "state",
];
const TIMEOUT_MS = 30_000;
const MAX_BUFFER_BYTES = 8 * 1024;

export const milkTool = {
  definition: {
    name: "milk",
    description:
      "Inspect or advance one admitted Milk eval. Paid work still requires a host-side one-use confirmation.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        action: { type: "string", enum: ACTIONS },
        eval_id: {
          type: "string",
          pattern: "^[0-9a-f]{64}$",
          description: "Stable campaign ID of the host-admitted eval.",
        },
      },
      required: ["action", "eval_id"],
    },
    outputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        ok: { type: "boolean" },
        action: { type: "string", enum: ACTIONS },
        evalId: { type: "string", pattern: "^[0-9a-f]{64}$" },
        state: { type: "string", enum: STATES },
        dispatchState: { type: "string", enum: DISPATCH_STATES },
        generationDone: { type: "boolean" },
        changed: { type: "boolean" },
        approvalRequired: { type: "boolean" },
        code: {
          type: "string",
          enum: [
            "ok",
            "reported_failure",
            "process_failed",
            "timeout",
            "output_too_large",
            "invalid_output",
          ],
        },
      },
      required: [
        "ok",
        "action",
        "evalId",
        "state",
        "dispatchState",
        "generationDone",
        "changed",
        "approvalRequired",
        "code",
      ],
    },
  },
  initializationParameters: {
    type: "object",
    additionalProperties: false,
    properties: {
      command: {
        type: "string",
        description: "Fixed absolute path to the host-managed Milk command.",
      },
    },
    required: ["command"],
  },
  initialize(initialization) {
    const command = initialization.command;
    if (typeof command !== "string" || !path.isAbsolute(command)) {
      throw new Error("milk tool command must be an absolute path");
    }
    return {
      execute(args) {
        const { action, evalId } = parseArguments(args);
        return runFixedCommand(command, action, evalId);
      },
    };
  },
};

export default milkTool;

export function runFixedCommand(
  command,
  action,
  evalId,
  { timeoutMs = TIMEOUT_MS, maxBuffer = MAX_BUFFER_BYTES } = {},
) {
  if (!ACTIONS.includes(action) || !EVAL_ID.test(evalId)) {
    throw new Error("milk tool arguments are invalid");
  }
  return new Promise((resolve) => {
    execFile(
      command,
      [action, evalId],
      { encoding: "utf8", timeout: timeoutMs, maxBuffer },
      (error, stdout) => {
        if (error) {
          const code =
            error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER"
              ? "output_too_large"
              : error.killed || error.signal === "SIGTERM"
                ? "timeout"
                : "process_failed";
          resolve(failure(action, evalId, code));
          return;
        }
        resolve(parseOutput(action, evalId, stdout));
      },
    );
  });
}

function parseArguments(args) {
  if (
    !args ||
    typeof args !== "object" ||
    Array.isArray(args) ||
    Object.keys(args).sort().join("\n") !== "action\neval_id" ||
    !ACTIONS.includes(args.action) ||
    typeof args.eval_id !== "string" ||
    !EVAL_ID.test(args.eval_id)
  ) {
    throw new Error("milk tool requires one supported action and eval_id");
  }
  return { action: args.action, evalId: args.eval_id };
}

function parseOutput(action, evalId, stdout) {
  let value;
  try {
    value = JSON.parse(stdout);
  } catch {
    return failure(action, evalId, "invalid_output");
  }
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).sort().join("\n") !== OUTPUT_KEYS.join("\n") ||
    typeof value.ok !== "boolean" ||
    value.eval_id !== evalId ||
    !STATES.includes(value.state) ||
    !DISPATCH_STATES.includes(value.dispatch_state) ||
    typeof value.generation_done !== "boolean" ||
    typeof value.changed !== "boolean" ||
    typeof value.approval_required !== "boolean"
  ) {
    return failure(action, evalId, "invalid_output");
  }
  return {
    ok: value.ok,
    action,
    evalId,
    state: value.state,
    dispatchState: value.dispatch_state,
    generationDone: value.generation_done,
    changed: value.changed,
    approvalRequired: value.approval_required,
    code: value.ok ? "ok" : "reported_failure",
  };
}

function failure(action, evalId, code) {
  return {
    ok: false,
    action,
    evalId,
    state: "failed",
    dispatchState: "unknown",
    generationDone: false,
    changed: false,
    approvalRequired: false,
    code,
  };
}
