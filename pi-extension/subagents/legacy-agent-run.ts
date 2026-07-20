import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { dirname, join } from "node:path";
import { mkdirSync, writeFileSync } from "node:fs";
import {
  createSubagentPane,
  runScriptInPane,
  shellQuote,
} from "./terminal.ts";
import {
  findLastAssistantMessage,
  getNewEntries,
} from "./session.ts";
import { getSubagentActivityFile, type SubagentActivityState } from "./activity.ts";
import {
  createLifecycle,
  markDelivery,
  type LifecycleProjection,
  type SubagentLifecycle,
} from "./lifecycle.ts";
import type { ResolvedRuntimePlan, ThinkingLevel } from "./runtime-routing.ts";
import type { SubagentStatusState } from "./status.ts";

export interface LegacyRunningAgentRun {
  abortController?: AbortController;
}

export interface LegacyAgentRunResult {
  name: string;
  task: string;
  summary: string;
  sessionFile?: string;
  claudeSessionId?: string;
  exitCode: number;
  elapsed: number;
  error?: string;
  errorMessage?: string;
  ping?: { name: string; message: string };
}

export interface LegacyRunningSubagent extends LegacyRunningAgentRun {
  id: string;
  name: string;
  task: string;
  agent?: string;
  surface: string;
  startTime: number;
  sessionFile: string;
  launchScriptFile?: string;
  activityFile?: string;
  activity?: SubagentActivityState;
  activityRead?: {
    ok: boolean;
    reason?: "missing" | "invalid" | "wrong-id";
    error?: string;
  };
  cli?: string;
  sentinelFile?: string;
  statusState?: SubagentStatusState;
  lifecycle: SubagentLifecycle;
  lastProjectedKind?: LifecycleProjection["kind"];
  interactive: boolean;
  runtimePlan: ResolvedRuntimePlan | undefined;
  launchKind: "spawn" | "resume";
  resultStartEntryCount?: number;
}

export interface LegacyLaunchContext {
  sessionManager: { getSessionFile(): string | null; getSessionId(): string; getSessionDir(): string };
  cwd: string;
  model?: { provider: string; id: string };
  modelRegistry: {
    find(provider: string, modelId: string): any;
    getAvailable?: () => any[];
    getAll?: () => any[];
    hasConfiguredAuth?: (model: any) => boolean;
  };
}

export interface LegacyResumeContext {
  sessionManager: { getSessionId(): string; getSessionDir(): string };
}

export interface LegacyResumeParams {
  sessionPath: string;
  name?: string;
  message?: string;
  autoExit?: boolean;
}

export interface LegacySpawnRequest<SpawnParams> {
  params: SpawnParams;
  context: LegacyLaunchContext;
  parentThinking: ThinkingLevel;
}

export interface LegacyResumeRequest {
  params: LegacyResumeParams;
  context: LegacyResumeContext;
}

export interface LegacyAgentRunLauncher<SpawnRequest, ResumeRequest, Run> {
  launch(request: SpawnRequest): Promise<Run>;
  resume(request: ResumeRequest): Promise<Run>;
}

export interface LegacyAgentRunSupervisor<Run, Result> {
  watch(run: Run, signal: AbortSignal): Promise<Result>;
}

export interface LegacyAgentRunResultRelay<Run, Result> {
  completed(run: Run, result: Result): void | Promise<void>;
  failed(run: Run, error: unknown): void | Promise<void>;
}

export interface LegacyAgentRunUi<Run, SessionContext = unknown> {
  sessionStarted(context: SessionContext): void;
  sessionShutdown(reason: unknown): void;
  runStarted(run: Run): void;
}

export interface LegacyAgentRunAdapters<Run extends LegacyRunningAgentRun, Result> {
  supervisor: LegacyAgentRunSupervisor<Run, Result>;
  resultRelay: LegacyAgentRunResultRelay<Run, Result>;
  ui: Pick<LegacyAgentRunUi<Run>, "runStarted">;
}

export interface LegacyAgentRunRuntimeAdapters<
  SpawnRequest,
  ResumeRequest,
  Run extends LegacyRunningAgentRun,
  Result,
  SessionContext,
> extends LegacyAgentRunAdapters<Run, Result> {
  launcher: LegacyAgentRunLauncher<SpawnRequest, ResumeRequest, Run>;
  ui: LegacyAgentRunUi<Run, SessionContext>;
}

export interface LegacyAgentRunAdapterOptions<SpawnParams> {
  pi: ExtensionAPI;
  subagentsDir: string;
  runningSubagents: Map<string, LegacyRunningSubagent>;
  currentExtensionApi(): ExtensionAPI | undefined;
  launch(request: LegacySpawnRequest<SpawnParams>): Promise<LegacyRunningSubagent>;
  watch(run: LegacyRunningSubagent, signal: AbortSignal): Promise<LegacyAgentRunResult>;
  getArtifactDir(sessionDir: string, sessionId: string): string;
  getShellReadyDelayMs(): number;
  resolveResumeLaunchBehavior(params: LegacyResumeParams): { autoExit: boolean; interactive: boolean };
  resolveResultPresentation(result: LegacyAgentRunResult, name: string): string;
  shouldDeliverCompletion(run: LegacyRunningSubagent): boolean;
  selectCompletionApi(previous: ExtensionAPI, current: ExtensionAPI | undefined): ExtensionAPI;
  formatElapsed(seconds: number): string;
  updateWidget(): void;
  sessionStarted(context: ExtensionContext): void;
  sessionShutdown(reason: unknown): void;
  runStarted(): void;
}

/**
 * Connect one legacy Agent Run to supervision, result relay, and presentation.
 * Production callers intentionally do not await this so launch and resume stay
 * fire-and-forget; tests await it to verify exact relay behavior.
 */
export async function superviseLegacyAgentRun<Run extends LegacyRunningAgentRun, Result>(
  run: Run,
  adapters: LegacyAgentRunAdapters<Run, Result>,
): Promise<void> {
  const watcherAbort = new AbortController();
  run.abortController = watcherAbort;
  adapters.ui.runStarted(run);

  let result: Result;
  try {
    result = await adapters.supervisor.watch(run, watcherAbort.signal);
  } catch (error) {
    await adapters.resultRelay.failed(run, error);
    return;
  }

  await adapters.resultRelay.completed(run, result);
}

async function resumeLegacyAgentRun<SpawnParams>(
  params: LegacyResumeParams,
  context: LegacyResumeContext,
  options: LegacyAgentRunAdapterOptions<SpawnParams>,
): Promise<LegacyRunningSubagent> {
  const name = params.name ?? "Resume";
  const { autoExit, interactive } = options.resolveResumeLaunchBehavior(params);
  const startTime = Date.now();
  const id = Math.random().toString(16).slice(2, 10);
  const resultStartEntryCount = getNewEntries(params.sessionPath, 0).length;

  const surface = createSubagentPane(name);
  await new Promise<void>((resolve) => setTimeout(resolve, options.getShellReadyDelayMs()));

  const parts = ["pi", "--session", shellQuote(params.sessionPath)];
  parts.push("-e", shellQuote(join(options.subagentsDir, "subagent-done.ts")));

  const sessionId = context.sessionManager.getSessionId();
  const artifactDir = options.getArtifactDir(context.sessionManager.getSessionDir(), sessionId);
  const activityFile = getSubagentActivityFile(artifactDir, id);
  mkdirSync(dirname(activityFile), { recursive: true });

  let resumeMsgFile: string | undefined;
  if (params.message) {
    const msgTimestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    resumeMsgFile = join(
      artifactDir,
      "subagent-resume",
      `${name
        .toLowerCase()
        .replace(/[^a-z0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-")
        .replace(/^-|-$/g, "") || "resume"}-${msgTimestamp}.md`,
    );
    mkdirSync(dirname(resumeMsgFile), { recursive: true });
    writeFileSync(resumeMsgFile, params.message, "utf8");
    parts.push(shellQuote(`@${resumeMsgFile}`));
  }

  const environment: string[] = [];
  if (process.env.PI_CODING_AGENT_DIR) {
    environment.push(`PI_CODING_AGENT_DIR=${shellQuote(process.env.PI_CODING_AGENT_DIR)}`);
  }
  environment.push(`PI_SUBAGENT_NAME=${shellQuote(name)}`);
  environment.push(`PI_SUBAGENT_SESSION=${shellQuote(params.sessionPath)}`);
  environment.push(`PI_SUBAGENT_ID=${shellQuote(id)}`);
  environment.push(`PI_SUBAGENT_ACTIVITY_FILE=${shellQuote(activityFile)}`);
  if (autoExit) environment.push("PI_SUBAGENT_AUTO_EXIT=1");

  const command = `${environment.join(" ")} ${parts.join(" ")}; echo '__SUBAGENT_DONE_'$?'__'`;
  const launchScriptFile = join(
    artifactDir,
    "subagent-scripts",
    `${name
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") || "resume"}-resume-${Date.now()}.sh`,
  );
  runScriptInPane(surface, command, {
    scriptPath: launchScriptFile,
    scriptPreamble: [
      `# Subagent resume script for ${name}`,
      `# Generated: ${new Date().toISOString()}`,
      `# Session: ${params.sessionPath}`,
      `# Surface: ${surface}`,
      ...(resumeMsgFile ? [`# Resume message file: ${resumeMsgFile}`] : []),
    ].join("\n"),
  });

  const running: LegacyRunningSubagent = {
    id,
    name,
    task: params.message ?? "resumed session",
    surface,
    startTime,
    sessionFile: params.sessionPath,
    launchScriptFile,
    activityFile,
    interactive,
    runtimePlan: undefined,
    launchKind: "resume",
    resultStartEntryCount,
    lifecycle: createLifecycle(startTime),
  };
  options.runningSubagents.set(id, running);
  return running;
}

function removeLegacyAgentRun<SpawnParams>(
  running: LegacyRunningSubagent,
  delivery: "delivered" | "suppressed",
  options: LegacyAgentRunAdapterOptions<SpawnParams>,
): void {
  running.lifecycle = markDelivery(running.lifecycle, delivery);
  options.runningSubagents.delete(running.id);
  options.updateWidget();
}

function relayLegacyAgentRunCompletion<SpawnParams>(
  running: LegacyRunningSubagent,
  result: LegacyAgentRunResult,
  options: LegacyAgentRunAdapterOptions<SpawnParams>,
): void {
  if (!options.shouldDeliverCompletion(running)) {
    removeLegacyAgentRun(running, "suppressed", options);
    return;
  }

  removeLegacyAgentRun(running, "delivered", options);
  const completionApi = options.selectCompletionApi(options.pi, options.currentExtensionApi());

  if (result.ping) {
    const sessionFile = result.sessionFile ?? running.sessionFile;
    const sessionRef = `\n\nSession: ${sessionFile}\nResume: pi --session ${sessionFile}`;
    completionApi.sendMessage(
      {
        customType: "subagent_ping",
        content: `Sub-agent "${result.ping.name}" needs help (${options.formatElapsed(result.elapsed)}):\n\n${result.ping.message}${sessionRef}`,
        display: true,
        details: {
          name: result.ping.name,
          message: result.ping.message,
          ...(running.launchKind === "spawn" ? { agent: running.agent } : {}),
          sessionFile,
        },
      },
      { triggerTurn: true, deliverAs: "steer" },
    );
    return;
  }

  let relayedResult = result;
  if (running.launchKind === "resume") {
    const entries = getNewEntries(running.sessionFile, running.resultStartEntryCount ?? 0);
    const summary = findLastAssistantMessage(entries) ??
      (result.errorMessage
        ? `Subagent error: ${result.errorMessage}`
        : result.exitCode !== 0
          ? `Resumed session exited with code ${result.exitCode}`
          : "Resumed session exited without new output");
    relayedResult = { ...result, summary, sessionFile: running.sessionFile };
  }

  const basePresentation = options.resolveResultPresentation(relayedResult, running.name);
  const presentation = running.runtimePlan?.runtimeMismatch
    ? `${basePresentation}\n\nRuntime warning: ${running.runtimePlan.runtimeMismatch}`
    : basePresentation;

  completionApi.sendMessage(
    {
      customType: "subagent_result",
      content: presentation,
      display: true,
      details: {
        name: running.name,
        task: running.task,
        ...(running.launchKind === "spawn" ? { agent: running.agent } : {}),
        exitCode: result.exitCode,
        elapsed: result.elapsed,
        sessionFile: relayedResult.sessionFile,
        ...(result.errorMessage ? { errorMessage: result.errorMessage } : {}),
        ...(running.launchKind === "spawn" && result.claudeSessionId
          ? { claudeSessionId: result.claudeSessionId }
          : {}),
        ...(running.runtimePlan ? { runtimePlan: running.runtimePlan } : {}),
      },
    },
    { triggerTurn: true, deliverAs: "steer" },
  );
}

function relayLegacyAgentRunFailure<SpawnParams>(
  running: LegacyRunningSubagent,
  error: unknown,
  options: LegacyAgentRunAdapterOptions<SpawnParams>,
): void {
  if (!options.shouldDeliverCompletion(running)) {
    removeLegacyAgentRun(running, "suppressed", options);
    return;
  }

  removeLegacyAgentRun(running, "delivered", options);
  const rawMessage = (error as { message?: unknown } | null)?.message;
  const detail = rawMessage == null ? String(error) : String(rawMessage);
  options.selectCompletionApi(options.pi, options.currentExtensionApi()).sendMessage(
    {
      customType: "subagent_result",
      content: running.launchKind === "resume"
        ? `Resume error: ${detail}`
        : `Sub-agent "${running.name}" error: ${detail}`,
      display: true,
      details: {
        name: running.name,
        ...(running.launchKind === "spawn" ? { task: running.task } : {}),
        error: rawMessage,
      },
    },
    { triggerTurn: true, deliverAs: "steer" },
  );
}

export function createLegacyAgentRunAdapters<SpawnParams>(
  options: LegacyAgentRunAdapterOptions<SpawnParams>,
): LegacyAgentRunRuntimeAdapters<
  LegacySpawnRequest<SpawnParams>,
  LegacyResumeRequest,
  LegacyRunningSubagent,
  LegacyAgentRunResult,
  ExtensionContext
> {
  return {
    launcher: {
      launch: options.launch,
      resume: ({ params, context }) => resumeLegacyAgentRun(params, context, options),
    },
    supervisor: { watch: options.watch },
    resultRelay: {
      completed: (running, result) => relayLegacyAgentRunCompletion(running, result, options),
      failed: (running, error) => relayLegacyAgentRunFailure(running, error, options),
    },
    ui: {
      sessionStarted: options.sessionStarted,
      sessionShutdown: options.sessionShutdown,
      runStarted: () => options.runStarted(),
    },
  };
}
