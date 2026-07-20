import { mkdirSync, realpathSync } from "node:fs";
import { dirname, isAbsolute, join, relative, sep } from "node:path";
import { assertSessionUuid } from "./workflow-identity.ts";
import { WorkflowProtocolError, type WorkflowRecord } from "./workflow-types.ts";

const COORDINATION_DATABASE_NAME = "coordination.sqlite";
const DESCENDANT_SESSIONS_DIRECTORY_NAME = "sessions";

export function createWorkflowLayout(input: {
  ownerSessionId: string;
  ownerSessionPath: string;
  createdAtMs: number;
}): WorkflowRecord {
  assertSessionUuid(input.ownerSessionId);
  const ownerSessionPath = realpathSync(input.ownerSessionPath);
  const ownerSessionDirectory = dirname(ownerSessionPath);
  const directory = join(ownerSessionDirectory, input.ownerSessionId);
  const sessionsDirectory = join(directory, DESCENDANT_SESSIONS_DIRECTORY_NAME);
  mkdirSync(sessionsDirectory, { recursive: true });

  return {
    ownerAgentId: input.ownerSessionId,
    ownerSessionPath,
    directory,
    sessionsDirectory,
    databasePath: join(directory, COORDINATION_DATABASE_NAME),
    createdAtMs: input.createdAtMs,
  };
}

export function assertDescendantTranscriptPath(
  workflow: WorkflowRecord,
  sessionPath: string,
): string {
  const normalizedPath = realpathSync(sessionPath);
  const canonicalSessionsDirectory = realpathSync(workflow.sessionsDirectory);
  const relativePath = relative(canonicalSessionsDirectory, normalizedPath);
  if (
    relativePath === "" ||
    relativePath === ".." ||
    relativePath.startsWith(`..${sep}`) ||
    isAbsolute(relativePath)
  ) {
    throw new WorkflowProtocolError(
      "TranscriptOutsideWorkflow",
      `Descendant transcript must be inside Workflow sessions directory: ${normalizedPath}`,
    );
  }
  return normalizedPath;
}
