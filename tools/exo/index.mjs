import { execFile } from "node:child_process";
import path from "node:path";

const ACTIONS = ["status", "reconcile", "run_confirmed"];
const STATES = [
  "idle",
  "ready",
  "waiting_for_confirmation",
  "running",
  "complete",
  "blocked",
  "failed",
];
const OUTPUT_KEYS = ["approval_required", "changed", "ok", "state"];
const TIMEOUT_MS = 30_000;
const MAX_BUFFER_BYTES = 8 * 1024;

export const milkTool = {
  definition: {
    name: "milk",
    description:
      "Inspect or advance the fixed Milk workload. Paid work still requires a host-side one-use confirmation.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        action: { type: "string", enum: ACTIONS },
      },
      required: ["action"],
    },
    outputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        ok: { type: "boolean" },
        action: { type: "string", enum: ACTIONS },
        state: { type: "string", enum: STATES },
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
        "state",
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
        const action = parseAction(args);
        return runFixedCommand(command, action);
      },
    };
  },
};

export default milkTool;

export function runFixedCommand(
  command,
  action,
  { timeoutMs = TIMEOUT_MS, maxBuffer = MAX_BUFFER_BYTES } = {},
) {
  return new Promise((resolve) => {
    execFile(
      command,
      [action],
      { encoding: "utf8", timeout: timeoutMs, maxBuffer },
      (error, stdout) => {
        if (error) {
          const code =
            error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER"
              ? "output_too_large"
              : error.killed || error.signal === "SIGTERM"
                ? "timeout"
                : "process_failed";
          resolve(failure(action, code));
          return;
        }
        resolve(parseOutput(action, stdout));
      },
    );
  });
}

function parseAction(args) {
  if (
    !args ||
    typeof args !== "object" ||
    Array.isArray(args) ||
    Object.keys(args).length !== 1 ||
    !ACTIONS.includes(args.action)
  ) {
    throw new Error("milk tool accepts only a supported action");
  }
  return args.action;
}

function parseOutput(action, stdout) {
  let value;
  try {
    value = JSON.parse(stdout);
  } catch {
    return failure(action, "invalid_output");
  }
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).sort().join("\n") !== OUTPUT_KEYS.join("\n") ||
    typeof value.ok !== "boolean" ||
    !STATES.includes(value.state) ||
    typeof value.changed !== "boolean" ||
    typeof value.approval_required !== "boolean"
  ) {
    return failure(action, "invalid_output");
  }
  return {
    ok: value.ok,
    action,
    state: value.state,
    changed: value.changed,
    approvalRequired: value.approval_required,
    code: value.ok ? "ok" : "reported_failure",
  };
}

function failure(action, code) {
  return {
    ok: false,
    action,
    state: "failed",
    changed: false,
    approvalRequired: false,
    code,
  };
}
