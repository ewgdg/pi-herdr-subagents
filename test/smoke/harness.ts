import { spawn, type ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { SMOKE_SCRIPT_FILENAME, type SmokeScript } from "./script.ts";

export const SMOKE_HARD_TIMEOUT_MS = 30_000;
export const SMOKE_RUNTIME_TARGET_MS = 10_000;

const COMPLETION_DEADLINE_RESERVE_MS = 4_000;
const COMPLETION_TIMEOUT_MS = SMOKE_HARD_TIMEOUT_MS - COMPLETION_DEADLINE_RESERVE_MS;
const HERDR_COMMAND_TIMEOUT_MS = 3_000;
const DIAGNOSTIC_COMMAND_TIMEOUT_MS = 750;
const SERVER_CONTROL_TIMEOUT_MS = 500;
const SERVER_READY_POLL_MS = 50;
const PROCESS_SIGTERM_GRACE_MS = 100;
const PROCESS_KILL_VERIFY_MS = 100;
const POLL_INTERVAL_MS = 100;
const SHELL_READY_MS = 500;
const TEST_DIRECTORY = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(TEST_DIRECTORY, "../..");
const SUBAGENTS_EXTENSION = join(PROJECT_ROOT, "pi-extension", "subagents", "index.ts");
const FAUX_PROVIDER_EXTENSION = join(TEST_DIRECTORY, "faux-provider.ts");

interface SmokeEnvironment {
  root: string;
  projectDirectory: string;
  agentDirectory: string;
  markerPath: string;
  markerText: string;
  completionText: string;
  childCompletionText: string;
  ownerSessionPath: string;
  workspaceId: string;
  herdrSession: HeadlessHerdrSession;
  ownerPane?: string;
}

interface HeadlessHerdrSession {
  name: string;
  process: ChildProcess;
  output: string[];
  spawnError?: Error;
}

interface SessionEntry {
  type?: string;
  customType?: string;
  content?: string;
  details?: { exitCode?: number };
  message?: {
    role?: string;
    stopReason?: string;
    errorMessage?: string;
    content?: Array<{ type?: string; text?: string }>;
  };
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function remainingMs(deadline: number, operation: string): number {
  const remaining = deadline - Date.now();
  if (remaining <= 0) {
    throw new Error(`Smoke test deadline reached while ${operation}.`);
  }
  return remaining;
}

function boundedTimeout(deadline: number, operation: string, maximum: number): number {
  return Math.max(1, Math.min(maximum, remainingMs(deadline, operation)));
}

function signalProcessGroup(
  processGroupId: number,
  child: ChildProcess,
  signal: NodeJS.Signals,
): void {
  try {
    if (process.platform === "win32") child.kill(signal);
    else process.kill(-processGroupId, signal);
  } catch {
    // The process may have exited between the status check and signal delivery.
  }
}

function childHasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

function processGroupExists(processGroupId: number, child: ChildProcess): boolean {
  if (process.platform === "win32") return !childHasExited(child);
  try {
    process.kill(-processGroupId, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

async function waitForProcessGroupExit(
  processGroupId: number,
  child: ChildProcess,
  timeoutMs: number,
): Promise<boolean> {
  const deadline = Date.now() + Math.max(1, timeoutMs);
  while (processGroupExists(processGroupId, child)) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) return false;
    await new Promise((resolve) => setTimeout(resolve, Math.min(10, remaining)));
  }
  return true;
}

async function terminateProcessGroup(
  child: ChildProcess,
  deadline: number,
  operation: string,
): Promise<void> {
  const processGroupId = child.pid;
  if (!processGroupId || !processGroupExists(processGroupId, child)) return;
  signalProcessGroup(processGroupId, child, "SIGTERM");
  const gracefulWait = Math.min(
    PROCESS_SIGTERM_GRACE_MS,
    Math.max(0, remainingMs(deadline, operation) - PROCESS_KILL_VERIFY_MS),
  );
  if (
    gracefulWait > 0 &&
    (await waitForProcessGroupExit(processGroupId, child, gracefulWait))
  ) {
    return;
  }
  signalProcessGroup(processGroupId, child, "SIGKILL");
  if (
    !(await waitForProcessGroupExit(
      processGroupId,
      child,
      Math.max(1, deadline - Date.now()),
    ))
  ) {
    throw new Error(`Process group did not exit while ${operation}.`);
  }
}

async function runHerdr(
  args: string[],
  deadline: number,
  operation: string,
  maximumTimeout = HERDR_COMMAND_TIMEOUT_MS,
  sessionName?: string,
): Promise<string> {
  const commandBudgetMs = boundedTimeout(deadline, operation, maximumTimeout);
  const terminationReserveMs = Math.min(
    PROCESS_SIGTERM_GRACE_MS + PROCESS_KILL_VERIFY_MS,
    Math.max(0, commandBudgetMs - 1),
  );
  const timeoutMs = Math.max(1, commandBudgetMs - terminationReserveMs);
  const commandDeadline = Date.now() + commandBudgetMs;
  const child = spawn("herdr", sessionName ? ["--session", sessionName, ...args] : args, {
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  let terminationPromise: Promise<void> | undefined;
  let terminationError: unknown;
  const append = (current: string, chunk: Buffer | string) =>
    `${current}${chunk.toString()}`.slice(-50_000);
  child.stdout?.on("data", (chunk) => {
    stdout = append(stdout, chunk);
  });
  child.stderr?.on("data", (chunk) => {
    stderr = append(stderr, chunk);
  });

  const timeout = setTimeout(() => {
    timedOut = true;
    terminationPromise = terminateProcessGroup(
      child,
      commandDeadline,
      `terminating timed-out herdr ${operation}`,
    ).catch((error) => {
      terminationError = error;
    });
  }, timeoutMs);

  const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    },
  ).finally(() => {
    clearTimeout(timeout);
  });

  if (timedOut) {
    await terminationPromise;
    if (terminationError) {
      throw new Error(
        `herdr ${operation} timed out and process-group cleanup failed: ${String(terminationError)}`,
      );
    }
    throw new Error(
      `herdr ${operation} timed out after ${commandBudgetMs}ms.` +
        `${stderr ? `\nstderr:\n${stderr}` : ""}${stdout ? `\nstdout:\n${stdout}` : ""}`,
    );
  }
  if (result.code !== 0) {
    throw new Error(
      `herdr ${operation} exited with code ${result.code ?? `signal ${result.signal}`}.` +
        `${stderr ? `\nstderr:\n${stderr}` : ""}${stdout ? `\nstdout:\n${stdout}` : ""}`,
    );
  }
  return stdout;
}

async function requireHerdrBinary(deadline: number): Promise<void> {
  await runHerdr(["--help"], deadline, "preflight", DIAGNOSTIC_COMMAND_TIMEOUT_MS);
}

async function stopHeadlessHerdr(session: HeadlessHerdrSession, deadline: number): Promise<void> {
  try {
    await runHerdr(
      ["session", "stop", session.name, "--json"],
      deadline,
      "headless server stop",
      SERVER_CONTROL_TIMEOUT_MS,
    );
  } catch {}
  await terminateProcessGroup(session.process, deadline, "stopping the headless herdr server");
  try {
    await runHerdr(
      ["session", "delete", session.name, "--json"],
      deadline,
      "headless session deletion",
      SERVER_CONTROL_TIMEOUT_MS,
    );
  } catch (error) {
    throw new Error(`Unable to delete headless herdr session ${session.name}: ${String(error)}`);
  }
}

function startHeadlessHerdr(): HeadlessHerdrSession {
  const name = `pi-smoke-${process.pid}-${Date.now()}`;
  const server = spawn("herdr", ["--session", name, "server"], {
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });
  const session: HeadlessHerdrSession = { name, process: server, output: [] };
  server.once("error", (error) => {
    session.spawnError = error;
  });
  const recordOutput = (source: string) => (chunk: Buffer | string) => {
    session.output.push(`${source}: ${chunk.toString()}`);
    while (session.output.join("").length > 20_000) session.output.shift();
  };
  server.stdout?.on("data", recordOutput("stdout"));
  server.stderr?.on("data", recordOutput("stderr"));

  return session;
}

async function waitForHeadlessHerdr(
  session: HeadlessHerdrSession,
  deadline: number,
): Promise<void> {
  while (Date.now() < deadline) {
    if (session.spawnError) throw session.spawnError;
    if (session.process.exitCode !== null) {
      throw new Error(
        `Headless herdr server exited with code ${session.process.exitCode}.\n${session.output.join("")}`,
      );
    }
    try {
      const status = await runHerdr(
        ["status", "server"],
        deadline,
        "headless server readiness",
        DIAGNOSTIC_COMMAND_TIMEOUT_MS,
        session.name,
      );
      if (/^status:\s+running$/m.test(status)) return;
    } catch {
      // The named session socket may not exist during the first startup poll.
    }
    await new Promise((resolve) => setTimeout(resolve, SERVER_READY_POLL_MS));
  }
  throw new Error(
    `Headless herdr server did not become ready before the smoke completion deadline.\n` +
      `${session.output.join("") || "(no server output)"}`,
  );
}

async function createWorkspace(
  sessionName: string,
  cwd: string,
  deadline: number,
): Promise<string> {
  const output = await runHerdr(
    [
      "workspace",
      "create",
      "--cwd",
      cwd,
      "--label",
      `pi-smoke-${Date.now()}`,
      "--env",
      "PI_OFFLINE=1",
      "--env",
      "PI_TELEMETRY=0",
      "--no-focus",
    ],
    deadline,
    "workspace creation",
    HERDR_COMMAND_TIMEOUT_MS,
    sessionName,
  );
  const workspaceId = JSON.parse(output)?.result?.workspace?.workspace_id;
  if (typeof workspaceId !== "string" || workspaceId.length === 0) {
    throw new Error(`Unexpected herdr workspace output: ${output.trim() || "(empty)"}`);
  }
  return workspaceId;
}

async function createOwnerPane(environment: SmokeEnvironment, deadline: number): Promise<string> {
  const output = await runHerdr(
    [
      "tab",
      "create",
      "--workspace",
      environment.workspaceId,
      "--cwd",
      environment.projectDirectory,
      "--label",
      "deterministic-smoke",
      "--no-focus",
    ],
    deadline,
    "Owner pane creation",
    HERDR_COMMAND_TIMEOUT_MS,
    environment.herdrSession.name,
  );
  const paneId = JSON.parse(output)?.result?.root_pane?.pane_id;
  if (typeof paneId !== "string" || paneId.length === 0) {
    throw new Error(`Unexpected herdr tab create output: ${output.trim() || "(empty)"}`);
  }
  return paneId;
}

async function runOwnerScript(environment: SmokeEnvironment, deadline: number): Promise<void> {
  const scriptPath = join(environment.root, "launch-owner.sh");
  const command = [
    `cd ${shellQuote(environment.projectDirectory)} &&`,
    `PI_CODING_AGENT_DIR=${shellQuote(environment.agentDirectory)}`,
    "PI_TELEMETRY=0",
    "PI_SUBAGENT_ID=",
    "PI_SUBAGENT_SESSION=",
    "pi",
    "--offline",
    `--session ${shellQuote(environment.ownerSessionPath)}`,
    "-ne",
    `-e ${shellQuote(SUBAGENTS_EXTENSION)}`,
    `-e ${shellQuote(FAUX_PROVIDER_EXTENSION)}`,
    "--model smoke-faux/scripted",
    shellQuote("RUN_DETERMINISTIC_SMOKE"),
    `; echo '__SMOKE_OWNER_EXIT_'$?'__'`,
  ].join(" ");
  writeFileSync(scriptPath, `#!/bin/bash\n${command}\n`, { mode: 0o755 });
  await runHerdr(
    ["pane", "run", environment.ownerPane!, `bash ${shellQuote(scriptPath)}`],
    deadline,
    "Owner launch",
    HERDR_COMMAND_TIMEOUT_MS,
    environment.herdrSession.name,
  );
}

async function createSmokeEnvironment(
  herdrSession: HeadlessHerdrSession,
  deadline: number,
): Promise<SmokeEnvironment> {
  const root = mkdtempSync(join(tmpdir(), "pi-smoke-"));
  try {
    const projectDirectory = join(root, "project");
    const agentDirectory = join(root, "agent");
    const markerPath = join(root, "child-marker.txt");
    const ownerSessionPath = join(root, "owner.jsonl");
    const runId = Date.now();
    const markerText = `SMOKE_MARKER_${runId}`;
    const completionText = `SMOKE_COMPLETE_${runId}`;
    const childCompletionText = `CHILD_COMPLETE_${runId}`;
    const childTask = `Write ${markerText} to ${markerPath}.`;
    mkdirSync(projectDirectory, { recursive: true });
    mkdirSync(agentDirectory, { recursive: true });

    writeFileSync(
      join(agentDirectory, "settings.json"),
      `${JSON.stringify(
        {
          extensions: [FAUX_PROVIDER_EXTENSION],
          retry: { enabled: false },
          compaction: { enabled: false },
        },
        null,
        2,
      )}\n`,
    );

    const script: SmokeScript = {
      owner: [
        {
          expect: { contextIncludes: "RUN_DETERMINISTIC_SMOKE" },
          respond: {
            tool: "subagent",
            arguments: { name: "Smoke Child", task: childTask },
          },
        },
        {
          expect: { toolResult: "subagent" },
          respond: { text: "WAITING_FOR_SMOKE_CHILD" },
        },
        {
          expect: { contextIncludes: childCompletionText },
          respond: { text: completionText },
        },
      ],
      child: [
        {
          expect: { contextIncludes: markerText },
          respond: {
            tool: "bash",
            arguments: { command: `printf '%s\\n' '${markerText}' > '${markerPath}'` },
          },
        },
        {
          expect: { toolResult: "bash" },
          respond: { text: childCompletionText, tool: "subagent_done", arguments: {} },
        },
        {
          expect: { toolResult: "subagent_done" },
          respond: { text: childCompletionText },
        },
      ],
    };
    writeFileSync(
      join(agentDirectory, SMOKE_SCRIPT_FILENAME),
      `${JSON.stringify(script, null, 2)}\n`,
    );

    return {
      root,
      projectDirectory,
      agentDirectory,
      markerPath,
      markerText,
      completionText,
      childCompletionText,
      ownerSessionPath,
      workspaceId: await createWorkspace(herdrSession.name, projectDirectory, deadline),
      herdrSession,
    };
  } catch (error) {
    rmSync(root, { recursive: true, force: true });
    throw error;
  }
}

async function cleanupEnvironment(environment: SmokeEnvironment, deadline: number): Promise<void> {
  try {
    await stopHeadlessHerdr(environment.herdrSession, deadline);
  } finally {
    rmSync(environment.root, { recursive: true, force: true });
  }
}

function readSessionEntries(path: string): SessionEntry[] {
  if (!existsSync(path)) return [];
  const lines = readFileSync(path, "utf8").split("\n").filter(Boolean);
  return lines.flatMap((line, index) => {
    try {
      return [JSON.parse(line) as SessionEntry];
    } catch (error) {
      // The polling read can race the final append. Ignore only an incomplete
      // trailing line; malformed committed entries must still fail loudly.
      if (index === lines.length - 1) return [];
      throw error;
    }
  });
}

function assistantText(entry: SessionEntry): string {
  if (entry.type !== "message" || entry.message?.role !== "assistant") return "";
  return entry.message.content
    ?.filter((block) => block.type === "text")
    .map((block) => block.text ?? "")
    .join("\n") ?? "";
}

function smokeCompleted(environment: SmokeEnvironment, entries: SessionEntry[]): boolean {
  const resultIndex = entries.findIndex(
    (entry) =>
      entry.type === "custom_message" &&
      entry.customType === "subagent_result" &&
      entry.details?.exitCode === 0 &&
      entry.content?.includes(environment.childCompletionText),
  );
  if (resultIndex < 0) return false;
  return entries.slice(resultIndex + 1).some((entry) => assistantText(entry).includes(environment.completionText));
}

async function readPane(
  sessionName: string,
  paneId: string,
  deadline: number,
  operation: string,
): Promise<string> {
  return runHerdr(
    ["pane", "read", paneId, "--source", "visible", "--lines", "300"],
    deadline,
    operation,
    DIAGNOSTIC_COMMAND_TIMEOUT_MS,
    sessionName,
  );
}

async function waitForSmoke(environment: SmokeEnvironment, deadline: number): Promise<void> {
  while (Date.now() < deadline) {
    const entries = readSessionEntries(environment.ownerSessionPath);
    if (existsSync(environment.markerPath) && smokeCompleted(environment, entries)) return;

    const failedResult = entries.find(
      (entry) =>
        entry.type === "custom_message" &&
        entry.customType === "subagent_result" &&
        entry.details?.exitCode !== undefined &&
        entry.details.exitCode !== 0,
    );
    if (failedResult) throw new Error(failedResult.content ?? "Smoke child failed.");

    const providerError = entries.findLast(
      (entry) => entry.message?.role === "assistant" && entry.message.stopReason === "error",
    );
    if (providerError?.message?.errorMessage) throw new Error(providerError.message.errorMessage);

    if (environment.ownerPane) {
      let screen: string;
      try {
        screen = await readPane(
          environment.herdrSession.name,
          environment.ownerPane,
          deadline,
          "Owner pane polling",
        );
      } catch (error) {
        if (Date.now() >= deadline) break;
        throw error;
      }
      const exit = screen.match(/__SMOKE_OWNER_EXIT_(\d+)__/);
      if (exit) throw new Error(`Owner Pi exited with code ${exit[1]}.`);
    }
    const delay = Math.min(POLL_INTERVAL_MS, Math.max(0, deadline - Date.now()));
    if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
  }
  throw new Error(
    `Smoke completion exceeded its ${COMPLETION_TIMEOUT_MS}ms budget; ` +
      `${COMPLETION_DEADLINE_RESERVE_MS}ms is reserved for diagnostics and cleanup.`,
  );
}

function collectFiles(directory: string, extension: string): string[] {
  const files: string[] = [];
  const visit = (path: string) => {
    if (!existsSync(path)) return;
    for (const entry of readdirSync(path)) {
      const child = join(path, entry);
      if (statSync(child).isDirectory()) visit(child);
      else if (child.endsWith(extension)) files.push(child);
    }
  };
  visit(directory);
  return files;
}

function collectFileContents(directory: string, extension: string, emptyMessage: string): string {
  const files = collectFiles(directory, extension);
  return files.length === 0
    ? emptyMessage
    : files.map((path) => `--- ${path} ---\n${readFileSync(path, "utf8")}`).join("\n");
}

async function collectPaneScreens(
  sessionName: string,
  workspaceId: string,
  deadline: number,
): Promise<string> {
  try {
    const output = await runHerdr(
      ["pane", "list", "--workspace", workspaceId],
      deadline,
      "diagnostic pane listing",
      DIAGNOSTIC_COMMAND_TIMEOUT_MS,
      sessionName,
    );
    const panes = JSON.parse(output)?.result?.panes;
    if (!Array.isArray(panes)) return `Unexpected pane list output: ${output.trim()}`;
    return (
      await Promise.all(
        panes.map(async (pane: { pane_id?: unknown }) => {
          if (typeof pane.pane_id !== "string") return "(pane without id)";
          try {
            return `--- ${pane.pane_id} ---\n${await readPane(sessionName, pane.pane_id, deadline, "diagnostic pane read")}`;
          } catch (error) {
            return `--- ${pane.pane_id} ---\n${String(error)}`;
          }
        }),
      )
    ).join("\n");
  } catch (error) {
    return `Unable to list workspace panes: ${String(error)}`;
  }
}

async function failureDiagnostics(
  herdrSession: HeadlessHerdrSession,
  environment: SmokeEnvironment | undefined,
  deadline: number,
): Promise<string> {
  return [
    `Headless herdr server:\n${herdrSession.output.join("") || "(no server output)"}`,
    `Workspace panes:\n${environment ? await collectPaneScreens(herdrSession.name, environment.workspaceId, deadline) : "(workspace not created)"}`,
    `Launch scripts:\n${environment ? collectFileContents(environment.root, ".sh", "(no launch scripts)") : "(environment not created)"}`,
    `Session files:\n${environment ? collectFileContents(environment.root, ".jsonl", "(no session files)") : "(environment not created)"}`,
  ].join("\n\n");
}

export async function runDeterministicSmoke(): Promise<void> {
  const startedAt = Date.now();
  const hardDeadline = startedAt + SMOKE_HARD_TIMEOUT_MS;
  const completionDeadline = startedAt + COMPLETION_TIMEOUT_MS;
  await requireHerdrBinary(hardDeadline);
  const herdrSession = startHeadlessHerdr();
  let environment: SmokeEnvironment | undefined;
  let failure: Error | undefined;
  try {
    await waitForHeadlessHerdr(herdrSession, completionDeadline);
    environment = await createSmokeEnvironment(herdrSession, completionDeadline);
    environment.ownerPane = await createOwnerPane(environment, completionDeadline);
    await new Promise((resolve) => setTimeout(resolve, Math.min(SHELL_READY_MS, remainingMs(completionDeadline, "waiting for the Owner shell"))));
    await runOwnerScript(environment, completionDeadline);
    await waitForSmoke(environment, completionDeadline);

    const markerContent = readFileSync(environment.markerPath, "utf8").trim();
    if (markerContent !== environment.markerText) {
      throw new Error(`Marker mismatch: expected ${environment.markerText}, got ${markerContent || "(empty)"}.`);
    }
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= SMOKE_RUNTIME_TARGET_MS) {
      throw new Error(`Smoke test took ${elapsedMs}ms; target is under ${SMOKE_RUNTIME_TARGET_MS}ms.`);
    }
  } catch (error) {
    const diagnostics = await failureDiagnostics(herdrSession, environment, hardDeadline);
    failure = new Error(`${(error as Error).message}\n\n${diagnostics}`);
  }
  try {
    if (environment) await cleanupEnvironment(environment, hardDeadline);
    else await stopHeadlessHerdr(herdrSession, hardDeadline);
  } catch (error) {
    failure = new Error(
      `${failure ? `${failure.message}\n\n` : ""}Headless session cleanup failed: ${String(error)}`,
    );
  }
  if (failure) throw failure;
}
