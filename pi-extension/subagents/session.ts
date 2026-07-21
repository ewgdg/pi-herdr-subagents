import { appendFileSync, copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes, randomUUID } from "node:crypto";
import { dirname, join } from "node:path";

export interface SessionEntry {
  type: string;
  id: string;
  parentId?: string;
  [key: string]: unknown;
}

export interface MessageEntry extends SessionEntry {
  type: "message";
  message: {
    role: "user" | "assistant" | "toolResult";
    content: Array<{ type: string; text?: string; [key: string]: unknown }>;
  };
}

export type SubagentSessionMode = "standalone" | "lineage-only" | "fork";
export type SeededSubagentSessionMode = Exclude<SubagentSessionMode, "standalone">;

function getForkContentLines(parentSessionFile: string): string[] {
  const raw = readFileSync(parentSessionFile, "utf8");
  const lines = raw.split("\n").filter((line) => line.trim());

  let truncateAt = lines.length;
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      const entry = JSON.parse(lines[i]);
      if (entry.type === "message" && entry.message?.role === "user") {
        truncateAt = i;
        break;
      }
    } catch {
      // ignore malformed lines
    }
  }

  return lines.slice(0, truncateAt).filter((line) => {
    try {
      return JSON.parse(line).type !== "session";
    } catch {
      return true;
    }
  });
}

export function seedSubagentSessionFile(params: {
  mode: SeededSubagentSessionMode;
  parentSessionFile: string;
  childSessionFile: string;
  childCwd: string;
}): void {
  initializeSubagentSessionFile(params);
}

export function initializeSubagentSessionFile(params: {
  mode: SubagentSessionMode;
  parentSessionFile?: string;
  childSessionFile: string;
  childCwd: string;
  childSessionId?: string;
  timestamp?: string;
}): string {
  if (params.mode !== "standalone" && !params.parentSessionFile) {
    throw new Error(`${params.mode} session creation requires a parent session file`);
  }
  const childSessionId = params.childSessionId ?? randomUUID();
  const header = {
    type: "session",
    version: 3,
    id: childSessionId,
    timestamp: params.timestamp ?? new Date().toISOString(),
    cwd: params.childCwd,
    ...(params.mode === "standalone"
      ? {}
      : { parentSession: params.parentSessionFile }),
  };
  const contentLines = params.mode === "fork"
    ? getForkContentLines(params.parentSessionFile!)
    : [];
  const lines = [JSON.stringify(header), ...contentLines];

  mkdirSync(dirname(params.childSessionFile), { recursive: true });
  writeFileSync(params.childSessionFile, lines.join("\n") + "\n", "utf8");
  return childSessionId;
}

function readEntries(sessionFile: string): SessionEntry[] {
  const raw = readFileSync(sessionFile, "utf8");
  return raw
    .split("\n")
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line) as SessionEntry);
}

/**
 * Return the id of the last entry in the session file (current branch point / leaf).
 */
export function getLeafId(sessionFile: string): string | null {
  const entries = readEntries(sessionFile);
  return entries.length > 0 ? entries[entries.length - 1].id : null;
}

/**
 * Return entries added after `afterLine` (1-indexed count of existing entries).
 */
export function getNewEntries(sessionFile: string, afterLine: number): SessionEntry[] {
  const raw = readFileSync(sessionFile, "utf8");
  const lines = raw.split("\n").filter((line) => line.trim());
  return lines.slice(afterLine).map((line) => JSON.parse(line) as SessionEntry);
}

/**
 * Find the last assistant message text in a list of entries.
 *
 * Falls back to the `errorMessage` field when the last assistant message has
 * `stopReason: "error"` and no usable text content — this happens when
 * auto-retry exhausts on a provider overload / rate limit / server error, and
 * without this fallback the parent would silently see a stale earlier message.
 */
export interface ObservedSessionRuntime {
  provider?: string;
  modelId?: string;
  thinking?: string;
}

/** Read the effective model and thinking entries recorded by Pi at session startup. */
export function findObservedSessionRuntime(entries: SessionEntry[]): ObservedSessionRuntime {
  const observed: ObservedSessionRuntime = {};
  for (const entry of entries) {
    if (entry.type === "model_change") {
      if (typeof entry.provider === "string") observed.provider = entry.provider;
      if (typeof entry.modelId === "string") observed.modelId = entry.modelId;
    } else if (
      entry.type === "thinking_level_change" &&
      typeof entry.thinkingLevel === "string"
    ) {
      observed.thinking = entry.thinkingLevel;
    }
  }
  return observed;
}

export function findLastAssistantMessage(entries: SessionEntry[]): string | null {
  for (let i = entries.length - 1; i >= 0; i--) {
    const entry = entries[i];
    if (entry.type !== "message") continue;
    const msg = entry as MessageEntry;
    if (msg.message.role !== "assistant") continue;

    const texts = msg.message.content
      .filter(
        (block) =>
          block.type === "text" && typeof block.text === "string" && block.text.trim() !== "",
      )
      .map((block) => block.text as string);

    if (texts.length > 0 && texts.join("").trim()) return texts.join("\n");

    const stopReason = (msg.message as { stopReason?: unknown }).stopReason;
    const errorMessage = (msg.message as { errorMessage?: unknown }).errorMessage;
    if (
      stopReason === "error" &&
      typeof errorMessage === "string" &&
      errorMessage.trim() !== ""
    ) {
      return `Subagent error: ${errorMessage.trim()}`;
    }
  }
  return null;
}

/**
 * Append a branch_summary entry to the session file.
 * Returns the new entry's id.
 */
export function appendBranchSummary(
  sessionFile: string,
  branchPointId: string,
  fromId: string | null,
  summary: string,
): string {
  const id = randomBytes(4).toString("hex");
  const entry = {
    type: "branch_summary",
    id,
    parentId: branchPointId,
    timestamp: new Date().toISOString(),
    fromId: fromId ?? branchPointId,
    summary,
  };
  appendFileSync(sessionFile, JSON.stringify(entry) + "\n", "utf8");
  return id;
}

/**
 * Copy the session file to destDir for parallel worker isolation.
 * Returns the path of the copy.
 */
export function copySessionFile(sessionFile: string, destDir: string): string {
  const id = randomBytes(4).toString("hex");
  const dest = join(destDir, `subagent-${id}.jsonl`);
  copyFileSync(sessionFile, dest);
  return dest;
}

/**
 * Clone a session transcript with a fresh Pi session identity.
 *
 * A byte-for-byte copy is not a new Agent: Pi keys Agent identity from the
 * session header UUID. Keep copySessionFile available for file isolation, but
 * use this helper whenever a new session/Agent is intended.
 */
export function cloneSessionFile(
  sessionFile: string,
  destination: string,
  options: { sessionId?: string; timestamp?: string } = {},
): string {
  const lines = readFileSync(sessionFile, "utf8").split("\n");
  const headerIndex = lines.findIndex((line) => line.trim());
  if (headerIndex < 0) throw new Error(`Session transcript is empty: ${sessionFile}`);

  let header: Record<string, unknown>;
  try {
    header = JSON.parse(lines[headerIndex]) as Record<string, unknown>;
  } catch {
    throw new Error(`Session transcript starts with invalid JSON: ${sessionFile}`);
  }
  if (header.type !== "session") {
    throw new Error(`Session transcript has no session header: ${sessionFile}`);
  }

  lines[headerIndex] = JSON.stringify({
    ...header,
    id: options.sessionId ?? randomUUID(),
    timestamp: options.timestamp ?? new Date().toISOString(),
    parentSession: sessionFile,
  });
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, lines.join("\n"), "utf8");
  return destination;
}

/**
 * Read new entries from sourceFile (after afterLine), append them to targetFile.
 * Returns the appended entries.
 */
export function mergeNewEntries(
  sourceFile: string,
  targetFile: string,
  afterLine: number,
): SessionEntry[] {
  const entries = getNewEntries(sourceFile, afterLine);
  for (const entry of entries) {
    appendFileSync(targetFile, JSON.stringify(entry) + "\n", "utf8");
  }
  return entries;
}
