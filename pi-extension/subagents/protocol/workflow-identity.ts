import { readFileSync } from "node:fs";
import { WorkflowProtocolError } from "./workflow-types.ts";

const SESSION_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function assertSessionUuid(value: string): void {
  if (!SESSION_UUID_PATTERN.test(value)) {
    throw new WorkflowProtocolError(
      "InvalidSessionIdentity",
      `Pi session identity must be a UUID: ${value}`,
    );
  }
}

export function readPiSessionUuid(sessionPath: string): string {
  const firstLine = readFileSync(sessionPath, "utf8")
    .split("\n")
    .find((line) => line.trim());
  if (!firstLine) {
    throw new WorkflowProtocolError(
      "InvalidSessionIdentity",
      `Pi session transcript has no session header: ${sessionPath}`,
    );
  }

  let header: unknown;
  try {
    header = JSON.parse(firstLine);
  } catch {
    throw new WorkflowProtocolError(
      "InvalidSessionIdentity",
      `Pi session transcript starts with invalid JSON: ${sessionPath}`,
    );
  }

  const candidate = header as { type?: unknown; id?: unknown };
  if (candidate.type !== "session" || typeof candidate.id !== "string") {
    throw new WorkflowProtocolError(
      "InvalidSessionIdentity",
      `Pi session transcript does not start with a session identity: ${sessionPath}`,
    );
  }
  assertSessionUuid(candidate.id);
  return candidate.id;
}
