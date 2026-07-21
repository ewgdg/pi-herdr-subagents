import { existsSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { assertSessionUuid } from "./workflow-identity.ts";
import { WorkflowProtocolError } from "./workflow-types.ts";

const BINDING_SUFFIX = ".workflow.json";

export interface WorkflowSessionBinding {
  workflowOwnerId: string;
  agentId: string;
  sessionPath: string;
}

export function bindNewWorkflowSession(input: WorkflowSessionBinding): WorkflowSessionBinding {
  const binding = canonicalBinding(input);
  const path = bindingPath(binding.sessionPath);
  if (existsSync(path)) {
    assertBindingFile(path, binding);
    return binding;
  }
  writeFileSync(path, `${JSON.stringify(binding)}\n`, { encoding: "utf8", flag: "wx" });
  return binding;
}

export function assertWorkflowSessionBinding(
  candidate: WorkflowSessionBinding | undefined,
  expected: WorkflowSessionBinding,
): WorkflowSessionBinding {
  if (!candidate) {
    throw new WorkflowProtocolError(
      "WorkflowMismatch",
      `Agent session is not bound to Workflow ${expected.workflowOwnerId}`,
    );
  }
  const binding = canonicalBinding(candidate);
  const canonicalExpected = canonicalBinding(expected);
  if (!sameBinding(binding, canonicalExpected)) {
    throw new WorkflowProtocolError(
      "WorkflowMismatch",
      `Agent ${expected.agentId} is bound to a different Workflow or transcript`,
    );
  }
  assertBindingFile(bindingPath(binding.sessionPath), canonicalExpected);
  return canonicalExpected;
}

function canonicalBinding(binding: WorkflowSessionBinding): WorkflowSessionBinding {
  assertSessionUuid(binding.workflowOwnerId);
  assertSessionUuid(binding.agentId);
  return { ...binding, sessionPath: realpathSync(binding.sessionPath) };
}

function bindingPath(sessionPath: string): string {
  return `${sessionPath}${BINDING_SUFFIX}`;
}

function assertBindingFile(path: string, expected: WorkflowSessionBinding): void {
  let stored: WorkflowSessionBinding;
  try {
    stored = JSON.parse(readFileSync(path, "utf8")) as WorkflowSessionBinding;
  } catch {
    throw new WorkflowProtocolError("WorkflowMismatch", `Invalid Workflow binding: ${path}`);
  }
  const canonicalStored = canonicalBinding(stored);
  if (!sameBinding(canonicalStored, expected)) {
    throw new WorkflowProtocolError(
      "WorkflowMismatch",
      `Workflow binding does not match Agent ${expected.agentId}: ${path}`,
    );
  }
}

function sameBinding(left: WorkflowSessionBinding, right: WorkflowSessionBinding): boolean {
  return left.workflowOwnerId === right.workflowOwnerId &&
    left.agentId === right.agentId &&
    left.sessionPath === right.sessionPath;
}
