import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { keyHint } from "@earendil-works/pi-coding-agent";
import { Type, type Static } from "@sinclair/typebox";
import { Box, Text, truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import {
  readdirSync,
  readFileSync,
  writeFileSync,
  existsSync,
  mkdirSync,
  copyFileSync,
  unlinkSync,
} from "node:fs";
import { homedir } from "node:os";
import {
  isTerminalAvailable,
  terminalSetupHint,
  createSubagentPane,
  runScriptInPane,
  closePane,
  interruptPane,
  shellQuote,
  getInheritedPiEnvironment,
  readPane,
  readPaneAsync,
  inspectPane,
} from "./terminal.ts";
import { waitForCompletion } from "./completion.ts";
import {
  buildAuthenticatedModelCatalog,
  resolveRuntimePlan,
  wrapPiModelRegistry,
  THINKING_LEVELS,
  type ThinkingLevel,
} from "./runtime-routing.ts";
import { loadModelConfig, resolveModelDefault } from "./model-config.ts";

import {
  findLastAssistantMessage,
  findObservedSessionRuntime,
  getNewEntries,
  initializeSubagentSessionFile,
  seedSubagentSessionFile,
  type SubagentSessionMode,
} from "./session.ts";
import {
  capStatusLines,
  formatElapsedDuration,
  formatStatusAggregate,
  normalizeStatusName,
  loadStatusConfig,
} from "./status.ts";
import {
  getSubagentActivityFile,
  readSubagentActivityFile,
  type ActivityReadResult,
} from "./activity.ts";
import {
  createLifecycle,
  formatLifecycleTransitionLine,
  lifecycleTransition,
  markCompleted,
  markCompletionDetected,
  markDelivery,
  markFailed,
  markInterruptRequested,
  markProcessRunning,
  observeActivity,
  observePaneInspection,
  projectLifecycle,
  type SubagentLifecycle,
  type PaneInspection,
} from "./lifecycle.ts";
import {
  createLegacyAgentRunAdapters,
  reconcilePreparedAgentRunStartupFailure,
  hasConfirmedAgentRunTermination,
  superviseLegacyAgentRun,
  type LegacyAgentRunResult,
  type LegacyAgentRunRuntimeAdapters,
  type LegacyLaunchContext,
  type LegacyResumeRequest,
  type LegacyRunningSubagent,
  type LegacySpawnRequest,
} from "./legacy-agent-run.ts";
import {
  WorkflowBootstrap,
  WORKFLOW_AGENT_SESSION_ID_ENV,
  WORKFLOW_AGENT_ROLE_ENV,
  WORKFLOW_OWNER_SESSION_ID_ENV,
  WORKFLOW_OWNER_SESSION_PATH_ENV,
  humanInterruptActorRoleFromMembership,
} from "./protocol/workflow-bootstrap.ts";
import {
  ProvisionalSpawnGate,
  PROVISIONAL_SPAWN_ENDPOINT_ENV,
} from "./protocol/provisional-spawn.ts";
import { latestAssistantTurnWasAborted } from "./protocol/pi-activation-events.ts";
import { bindNewWorkflowSession } from "./protocol/workflow-session-binding.ts";
import { digestPayload } from "./protocol/direct-signal-transcript.ts";
import { DirectSignalStore } from "./protocol/sqlite-message-store.ts";
import {
  confirmProjectedInboxBatches,
  registerAgentSendTool,
  startDirectSignalRouter,
} from "./protocol/direct-signal-extension.ts";
import { registerAgentInspectTool } from "./protocol/agent-inspect-extension.ts";
import { registerAgentCancelTool } from "./protocol/agent-cancel-extension.ts";

/** Absolute path to `pi-extension/subagents`. https://github.com/nodejs/node/issues/37845 */
const SUBAGENTS_DIR = dirname(fileURLToPath(import.meta.url));

// Survive /reload: replace presentation timers while keeping active completion
// watchers and their registry alive. Old module closures continue watching the
// children; the reloaded module adopts the shared registry for status/interrupts.
const WIDGET_INTERVAL_KEY = Symbol.for("pi-subagents/widget-interval");
const STATUS_INTERVAL_KEY = Symbol.for("pi-subagents/status-interval");
const RUNTIME_KEY = Symbol.for("pi-subagents/runtime");

{
  const prevInterval = (globalThis as any)[WIDGET_INTERVAL_KEY];
  if (prevInterval) {
    clearInterval(prevInterval);
    (globalThis as any)[WIDGET_INTERVAL_KEY] = null;
  }
  const prevStatusInterval = (globalThis as any)[STATUS_INTERVAL_KEY];
  if (prevStatusInterval) {
    clearInterval(prevStatusInterval);
    (globalThis as any)[STATUS_INTERVAL_KEY] = null;
  }
}

function buildSubagentRoutingGuidelines(catalog?: string): string[] {
  return [
    "For subagent model and thinking selection, inherit the parent runtime by omitting both fields unless the task warrants an override.",
    "For subagent tasks, prefer changing thinking before changing models: minimal/low for bounded mechanical work, medium for ordinary implementation or review, and high+ for architecture, concurrency, security, or hard diagnosis.",
    "When overriding a subagent model, use an exact authenticated provider/model-id from the live catalog below. Do not invent aliases or fuzzy names.",
    catalog ?? "Authenticated subagent model catalog becomes available after session start.",
  ];
}

const subagentRoutingGuidelines = buildSubagentRoutingGuidelines();

const ThinkingLevelSchema = Type.Union(
  THINKING_LEVELS.map((level) => Type.Literal(level)),
  {
    description:
      "Pi thinking level. Omit to inherit the parent level. Prefer changing thinking before changing models: minimal/low for bounded mechanical work, medium for ordinary implementation or review, high+ for architecture, concurrency, security, or hard diagnosis.",
  },
);

const SubagentParams = Type.Object({
  name: Type.String({ description: "Display name for the subagent" }),
  task: Type.String({ description: "Task/prompt for the sub-agent" }),
  agent: Type.Optional(
    Type.String({
      description:
        "Agent name to load defaults from (e.g. 'worker', 'scout', 'reviewer'). Reads ~/.pi/agent/agents/<name>.md for model, tools, skills.",
    }),
  ),
  systemPrompt: Type.Optional(
    Type.String({ description: "Appended to system prompt (role instructions)" }),
  ),
  model: Type.Optional(
    Type.String({
      description:
        "Exact authenticated provider/model-id. Omit to inherit the parent model. Select another model only when task capability, speed, cost, modality, or context requirements warrant it.",
    }),
  ),
  thinking: Type.Optional(ThinkingLevelSchema),
  skills: Type.Optional(
    Type.String({ description: "Comma-separated skills (overrides agent default)" }),
  ),
  tools: Type.Optional(
    Type.String({ description: "Comma-separated tools (overrides agent default)" }),
  ),
  cwd: Type.Optional(
    Type.String({
      description:
        "Working directory for the sub-agent. The agent starts in this folder and picks up its local .pi/ config, CLAUDE.md, skills, and extensions. Use for role-specific subfolders.",
    }),
  ),
  fork: Type.Optional(
    Type.Boolean({
      description:
        "Force the full-context fork mode for this spawn. The sub-agent inherits the current session conversation, overriding any agent frontmatter session-mode.",
    }),
  ),
  interactive: Type.Optional(
    Type.Boolean({
      description:
        "Mark the subagent as interactive (long-running, user drives the conversation in its own pane). When true, the main session is not woken by status transitions (stalled/recovered) for this subagent. If omitted, falls back to the agent's `interactive` frontmatter, otherwise the inverse of `auto-exit` (agents that auto-exit are autonomous and get stall pings; agents that don't are interactive and stay quiet).",
    }),
  ),
  resumeSessionId: Type.Optional(
    Type.String({
      description:
        "Resume a previous Claude Code session by its ID. Loads the conversation history and continues where it left off. The session ID is returned in details of every claude tool call. Use this to retry cancelled runs or ask follow-up questions.",
    }),
  ),
});

const SubagentResumeParams = Type.Object({
  sessionPath: Type.String({ description: "Path to the session .jsonl file to resume" }),
  name: Type.Optional(
    Type.String({ description: "Display name for the terminal tab. Default: 'Resume'" }),
  ),
  message: Type.Optional(
    Type.String({
      description: "Optional message to send after resuming (e.g. follow-up instructions)",
    }),
  ),
  autoExit: Type.Optional(
    Type.Boolean({
      description:
        "Whether the resumed session should automatically exit after completing its response. Defaults to true for autonomous follow-up work; set false for interactive resumed sessions.",
    }),
  ),
});

interface AgentDefaults {
  model?: string;
  tools?: string;
  skills?: string;
  thinking?: string;
  denyTools?: string;
  spawning?: boolean;
  autoExit?: boolean;
  interactive?: boolean;
  systemPromptMode?: "append" | "replace";
  sessionMode?: SubagentSessionMode;
  cwd?: string;
  cli?: string;
  body?: string;
  disableModelInvocation?: boolean;
}

type AgentSource = "package" | "global" | "project";

interface AgentDefinition extends AgentDefaults {
  name: string;
  description?: string;
  disableModelInvocation: boolean;
}

interface ListedAgentDefinition extends AgentDefinition {
  source: AgentSource;
}

/** Tools that are gated by `spawning: false` */
const SPAWNING_TOOLS = new Set([
  "subagent",
  "subagent_interrupt",
  "subagents_list",
  "subagent_resume",
]);

/**
 * Resolve the effective set of denied tool names from agent defaults.
 * `spawning: false` expands to all SPAWNING_TOOLS.
 * `deny-tools` adds individual tool names on top.
 */
function resolveDenyTools(agentDefs: AgentDefaults | null): Set<string> {
  const denied = new Set<string>();
  if (!agentDefs) return denied;

  // spawning: false → deny all spawning tools
  if (agentDefs.spawning === false) {
    for (const t of SPAWNING_TOOLS) denied.add(t);
  }

  // deny-tools: explicit list
  if (agentDefs.denyTools) {
    for (const t of agentDefs.denyTools
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)) {
      denied.add(t);
    }
  }

  return denied;
}

/** Resolve the global agent config directory, respecting PI_CODING_AGENT_DIR. */
function getAgentConfigDir(): string {
  return process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
}

function getBundledAgentsDir(): string {
  return join(SUBAGENTS_DIR, "../../agents");
}

function getFrontmatterValue(frontmatter: string, key: string): string | undefined {
  const match = frontmatter.match(new RegExp(`^${key}:\\s*(.+)$`, "m"));
  return match ? match[1].trim() : undefined;
}

function parseOptionalBoolean(value: string | undefined): boolean | undefined {
  return value != null ? value === "true" : undefined;
}

function parseSessionMode(value: string | undefined): SubagentSessionMode | undefined {
  if (value === "standalone" || value === "lineage-only" || value === "fork") {
    return value;
  }
  return undefined;
}

function parseAgentDefinition(content: string, fallbackName: string): AgentDefinition | null {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return null;

  const frontmatter = match[1];
  const body = content.replace(/^---\n[\s\S]*?\n---\n*/, "").trim();
  const systemPromptMode = getFrontmatterValue(frontmatter, "system-prompt");

  return {
    name: getFrontmatterValue(frontmatter, "name") ?? fallbackName,
    description: getFrontmatterValue(frontmatter, "description"),
    model: getFrontmatterValue(frontmatter, "model"),
    tools: getFrontmatterValue(frontmatter, "tools"),
    systemPromptMode:
      systemPromptMode === "replace"
        ? "replace"
        : systemPromptMode === "append"
          ? "append"
          : undefined,
    skills: getFrontmatterValue(frontmatter, "skill") ?? getFrontmatterValue(frontmatter, "skills"),
    thinking: getFrontmatterValue(frontmatter, "thinking"),
    denyTools: getFrontmatterValue(frontmatter, "deny-tools"),
    spawning: parseOptionalBoolean(getFrontmatterValue(frontmatter, "spawning")),
    autoExit: parseOptionalBoolean(getFrontmatterValue(frontmatter, "auto-exit")),
    interactive: parseOptionalBoolean(getFrontmatterValue(frontmatter, "interactive")),
    sessionMode: parseSessionMode(getFrontmatterValue(frontmatter, "session-mode")),
    cwd: getFrontmatterValue(frontmatter, "cwd"),
    cli: getFrontmatterValue(frontmatter, "cli"),
    body: body || undefined,
    disableModelInvocation:
      getFrontmatterValue(frontmatter, "disable-model-invocation")?.toLowerCase() === "true",
  };
}

function discoverAgentDefinitions(): ListedAgentDefinition[] {
  const agents = new Map<string, ListedAgentDefinition>();
  const dirs: Array<{ path: string; source: AgentSource }> = [
    { path: getBundledAgentsDir(), source: "package" },
    { path: join(getAgentConfigDir(), "agents"), source: "global" },
    { path: join(process.cwd(), ".pi", "agents"), source: "project" },
  ];

  for (const { path: dir, source } of dirs) {
    if (!existsSync(dir)) continue;
    for (const file of readdirSync(dir).filter((entry) => entry.endsWith(".md"))) {
      const parsed = parseAgentDefinition(
        readFileSync(join(dir, file), "utf8"),
        file.replace(/\.md$/, ""),
      );
      if (!parsed) continue;
      agents.set(parsed.name, { ...parsed, source });
    }
  }

  return [...agents.values()];
}

function resolveSubagentPaths(
  params: Static<typeof SubagentParams>,
  agentDefs: AgentDefaults | null,
): { effectiveCwd: string | null; localAgentDir: string | null; effectiveAgentDir: string } {
  const rawCwd = params.cwd ?? agentDefs?.cwd ?? null;
  const cwdIsFromAgent = !params.cwd && agentDefs?.cwd != null;
  const cwdBase = cwdIsFromAgent ? getAgentConfigDir() : process.cwd();
  const effectiveCwd = rawCwd
    ? rawCwd.startsWith("/")
      ? rawCwd
      : join(cwdBase, rawCwd)
    : null;
  const localAgentDir = effectiveCwd ? join(effectiveCwd, ".pi", "agent") : null;
  const effectiveAgentDir =
    localAgentDir && existsSync(localAgentDir) ? localAgentDir : getAgentConfigDir();
  return { effectiveCwd, localAgentDir, effectiveAgentDir };
}

function getDefaultSessionDirFor(cwd: string, agentDir: string): string {
  const safePath = `--${cwd.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
  const sessionDir = join(agentDir, "sessions", safePath);
  if (!existsSync(sessionDir)) {
    mkdirSync(sessionDir, { recursive: true });
  }
  return sessionDir;
}

function resolveEffectiveSessionMode(
  params: Static<typeof SubagentParams>,
  agentDefs: AgentDefaults | null,
): SubagentSessionMode {
  if (params.fork) return "fork";
  return agentDefs?.sessionMode ?? "standalone";
}

function resolveLaunchBehavior(
  params: Static<typeof SubagentParams>,
  agentDefs: AgentDefaults | null,
): {
  sessionMode: SubagentSessionMode;
  seededSessionMode: "lineage-only" | "fork" | null;
  inheritsConversationContext: boolean;
  taskDelivery: "direct" | "artifact";
} {
  const sessionMode = resolveEffectiveSessionMode(params, agentDefs);
  const inheritsConversationContext = sessionMode === "fork";
  return {
    sessionMode,
    seededSessionMode: sessionMode === "standalone" ? null : sessionMode,
    inheritsConversationContext,
    taskDelivery: inheritsConversationContext ? "direct" : "artifact",
  };
}

/**
 * Decide whether a subagent is interactive (user-driven, long-running).
 *
 * Resolution order:
 *   1. Explicit `interactive` tool parameter wins.
 *   2. Explicit `interactive` frontmatter field on the agent.
 *   3. Default: the inverse of `auto-exit`. Agents that auto-exit are
 *      autonomous (scout, worker, reviewer) and the parent session should be
 *      woken on stall/recovery transitions. Agents that don't auto-exit are
 *      driven by the user in their own pane (planner, iterate/fork) and
 *      stall pings are noise.
 *
 * When no agent defs exist at all (bare `subagent({ name, task })` call,
 * typical for `/iterate` with `fork: true`), `autoExit` is undefined and the
 * subagent is treated as interactive — matching the intent of iterate.
 */
function resolveEffectiveAutoExit(
  params: Static<typeof SubagentParams>,
  agentDefs: AgentDefaults | null,
): boolean {
  // Named agents preserve their declared behavior. Bare tool calls are
  // autonomous by default, including full-context forks: `fork` controls
  // context inheritance, not whether the child should remain open. Interactive
  // flows such as /iterate opt out explicitly with `interactive: true`.
  if (agentDefs) return agentDefs.autoExit ?? false;
  return params.interactive !== true;
}

function resolveEffectiveInteractive(
  params: Static<typeof SubagentParams>,
  agentDefs: AgentDefaults | null,
): boolean {
  if (params.interactive != null) return params.interactive;
  if (agentDefs?.interactive != null) return agentDefs.interactive;
  return !resolveEffectiveAutoExit(params, agentDefs);
}

function loadAgentDefaults(agentName: string): AgentDefaults | null {
  const configDir = getAgentConfigDir();
  const paths = [
    join(process.cwd(), ".pi", "agents", `${agentName}.md`),
    join(configDir, "agents", `${agentName}.md`),
    join(getBundledAgentsDir(), `${agentName}.md`),
  ];

  for (const p of paths) {
    if (!existsSync(p)) continue;
    const parsed = parseAgentDefinition(readFileSync(p, "utf8"), agentName);
    if (parsed) return parsed;
  }

  return null;
}

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s}s`;
}

/**
 * Wait long enough for a freshly created pane to finish shell startup.
 *
 * Some environments do extra shell-init work before the prompt is ready
 * (for example direnv/devenv), so the delay is configurable for users who hit
 * dropped commands. Keep the historical default at 500ms.
 */
function getShellReadyDelayMs(): number {
  const raw = process.env.PI_SUBAGENT_SHELL_READY_DELAY_MS?.trim();
  const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN;
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 500;
}

function muxUnavailableResult() {
  return {
    content: [
      {
        type: "text" as const,
        text: `Subagents require herdr. ${terminalSetupHint()}`,
      },
    ],
    details: { error: "herdr not available" },
  };
}

/**
 * Build the internal artifact directory path for the current session.
 * Used by the subagents extension to stash task files, system prompts, and
 * launch scripts for sub-agents. Path convention:
 *   <sessionDir>/artifacts/<session-id>/
 */
function getArtifactDir(sessionDir: string, sessionId: string): string {
  return join(sessionDir, "artifacts", sessionId);
}

const statusConfig = loadStatusConfig();
const modelConfig = loadModelConfig();

function resolveResultPresentation(
  result: Pick<
    SubagentResult,
    "exitCode" | "elapsed" | "summary" | "sessionFile" | "errorMessage"
  >,
  name: string,
): string {
  const sessionRef = result.sessionFile
    ? `\n\nSession: ${result.sessionFile}\nResume: pi --session ${result.sessionFile}`
    : "";

  if (result.errorMessage) {
    // Auto-retry exhausted or other agent-loop error. The subagent did not
    // produce a usable result — surface the underlying provider/network
    // failure so the orchestrator can decide whether to retry, resume, or
    // change approach instead of silently treating the run as completed.
    return (
      `Sub-agent "${name}" failed after ${formatElapsed(result.elapsed)} ` +
      `(provider/agent error — auto-retry exhausted).\n\n` +
      `Error: ${result.errorMessage}\n\n` +
      `The subagent did not produce a result. You can retry by spawning a new ` +
      `subagent or resume the session with subagent_resume.${sessionRef}`
    );
  }

  return result.exitCode !== 0
    ? `Sub-agent "${name}" failed (exit code ${result.exitCode}).\n\n${result.summary}${sessionRef}`
    : `Sub-agent "${name}" completed (${formatElapsed(result.elapsed)}).\n\n${result.summary}${sessionRef}`;
}

type SubagentResult = LegacyAgentRunResult;
type RunningSubagent = LegacyRunningSubagent;

interface SubagentRuntime {
  runningSubagents: Map<string, RunningSubagent>;
  pendingRequestReactivations: Map<string, Promise<import("./protocol/direct-signal-types.ts").QueuedSignalReceipt>>;
  pi?: ExtensionAPI;
  latestCtx?: ExtensionContext;
  modelCatalog?: string;
  workflowBootstrap: WorkflowBootstrap;
}

type ConfiguredLegacyAgentRunAdapters = LegacyAgentRunRuntimeAdapters<
  LegacySpawnRequest<Static<typeof SubagentParams>>,
  LegacyResumeRequest,
  RunningSubagent,
  SubagentResult,
  ExtensionContext
>;

export interface SubagentsExtensionOptions {
  legacyAgentRunAdapters?: (pi: ExtensionAPI) => ConfiguredLegacyAgentRunAdapters;
}

function createWorkflowBootstrap(): WorkflowBootstrap {
  return new WorkflowBootstrap({
    async confirmRunTerminated(locator) {
      const inspection = await inspectPane(locator.surface);
      if (inspection.kind === "missing") return true;
      if (inspection.kind === "unavailable") return false;
      try {
        return /__SUBAGENT_DONE_\d+__/.test(await readPaneAsync(locator.surface, 10));
      } catch {
        return false;
      }
    },
  });
}

function createSubagentRuntime(): SubagentRuntime {
  return {
    runningSubagents: new Map<string, RunningSubagent>(),
    pendingRequestReactivations: new Map(),
    workflowBootstrap: createWorkflowBootstrap(),
  };
}

/** Runtime state preserved across /reload. */
const runtime: SubagentRuntime =
  (globalThis as any)[RUNTIME_KEY] ??
  ((globalThis as any)[RUNTIME_KEY] = createSubagentRuntime());
runtime.workflowBootstrap ??= createWorkflowBootstrap();
const runningSubagents = runtime.runningSubagents;

export function shouldPreserveSubagentsOnShutdown(reason: unknown): boolean {
  return reason === "reload";
}

export function cleanupSubagentsForShutdown(
  reason: unknown,
  agents: Map<string, Pick<RunningSubagent, "abortController" | "lifecycle">>,
): void {
  if (shouldPreserveSubagentsOnShutdown(reason)) return;

  for (const agent of agents.values()) {
    if (agent.lifecycle) {
      agent.lifecycle = markDelivery(agent.lifecycle, "suppressed");
    }
    agent.abortController?.abort();
  }
  agents.clear();
}

export function shouldDeliverSubagentCompletion(
  running: Pick<RunningSubagent, "lifecycle">,
): boolean {
  // Authoritative gate: only pending deliveries may be sent.
  // Missing lifecycle (pre-migration fixtures) defaults to pending/true.
  return (running.lifecycle?.delivery ?? "pending") === "pending";
}

export function selectCompletionApi<T>(previous: T, current: T | undefined): T {
  return current ?? previous;
}

// ── Widget management ──

/** Interval timer for widget re-renders. */
let widgetInterval: ReturnType<typeof setInterval> | null = null;

/** Interval timer for status transition checks. */
let statusInterval: ReturnType<typeof setInterval> | null = null;

function formatElapsedMMSS(startTime: number, endTime = Date.now()): string {
  const seconds = Math.floor((endTime - startTime) / 1000);
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

const ACTIVE_ACCENT = "\x1b[38;2;77;163;255m";
const OPEN_ACCENT = "\x1b[38;2;214;158;46m";
const RST = "\x1b[0m";

/**
 * Build a bordered content line: │left          right│
 * Left content is truncated if needed, right is preserved, padded to fill width.
 */
function borderLine(left: string, right: string, width: number, accent = ACTIVE_ACCENT): string {
  if (width <= 0) return "";
  if (width === 1) return `${accent}│${RST}`;

  // width = total visible chars for the whole line including │ and │
  const contentWidth = Math.max(0, width - 2); // space inside the two │ chars
  const rightVis = visibleWidth(right);

  // If the status chunk alone is too wide, prefer preserving it in compact form
  // rather than overflowing the terminal.
  if (rightVis >= contentWidth) {
    const truncRight = truncateToWidth(right, contentWidth);
    const rightPad = Math.max(0, contentWidth - visibleWidth(truncRight));
    return `${accent}│${RST}${truncRight}${" ".repeat(rightPad)}${accent}│${RST}`;
  }

  const maxLeft = Math.max(0, contentWidth - rightVis);
  const truncLeft = truncateToWidth(left, maxLeft);
  const leftVis = visibleWidth(truncLeft);
  const pad = Math.max(0, contentWidth - leftVis - rightVis);
  return `${accent}│${RST}${truncLeft}${" ".repeat(pad)}${right}${accent}│${RST}`;
}

/**
 * Build the bordered top line: ╭─ Title ──── info ─╮
 * All chars are accounted for within `width`.
 */
function borderTop(title: string, info: string, width: number, accent = ACTIVE_ACCENT): string {
  if (width <= 0) return "";
  if (width === 1) return `${accent}╭${RST}`;

  // ╭─ Title ───...─── info ─╮
  // overhead: ╭─ (2) + space around title (2) + space around info (2) + ─╮ (2) = but we simplify
  const inner = Math.max(0, width - 2); // inside ╭ and ╮
  const titlePart = `─ ${title} `;
  const infoPart = ` ${info} ─`;
  const fillLen = Math.max(0, inner - titlePart.length - infoPart.length);
  const fill = "─".repeat(fillLen);
  const content = `${titlePart}${fill}${infoPart}`.slice(0, inner).padEnd(inner, "─");
  return `${accent}╭${content}╮${RST}`;
}

/**
 * Build the bordered bottom line: ╰──────────────────╯
 */
function borderBottom(width: number, accent = ACTIVE_ACCENT): string {
  if (width <= 0) return "";
  if (width === 1) return `${accent}╰${RST}`;

  const inner = Math.max(0, width - 2);
  return `${accent}╰${"─".repeat(inner)}╯${RST}`;
}

function formatLifecycleWidgetLabel(
  projection: ReturnType<typeof projectLifecycle>,
  now: number,
): string {
  const duration = projection.stateDurationSince == null
    ? ""
    : ` ${formatElapsedDuration(now - projection.stateDurationSince)}`;
  if (projection.kind === "active") return projection.label
    ? ` active · ${projection.label}${duration} `
    : ` active${duration} `;
  if (projection.kind === "blocked") return ` blocked${duration} `;
  if (projection.kind === "running") return " running… ";
  if (projection.kind === "waiting") return ` waiting${duration} `;
  if (projection.kind === "interrupted") return ` interrupted${duration} `;
  if (projection.kind === "stalled") return ` stalled${duration} `;
  // completed/failed exist as lifecycle projections for delivery bookkeeping,
  // but the row is removed immediately after result delivery — so the only
  // visible terminal handoff label is finalizing.
  if (
    projection.kind === "finalizing" ||
    projection.kind === "completed" ||
    projection.kind === "failed"
  ) {
    return " finalizing… ";
  }
  return " starting… ";
}

function renderSubagentWidgetLines(agents: RunningSubagent[], width: number): string[] {
  const now = Date.now();
  const rendered = agents.map((agent) => ({ agent, projection: projectLifecycle(ensureLifecycle(agent), now) }));
  const activeCount = rendered.filter(({ projection }) =>
    projection.kind === "active" ||
    projection.kind === "starting" ||
    projection.kind === "running" ||
    projection.kind === "blocked"
  ).length;
  const openCount = agents.length - activeCount;
  const info = activeCount > 0
    ? openCount > 0 ? `${activeCount} active · ${openCount} open` : `${activeCount} active`
    : `${openCount} open`;
  const accent = activeCount > 0 ? ACTIVE_ACCENT : OPEN_ACCENT;

  const lines: string[] = [borderTop("Subagents", info, width, accent)];

  for (const { agent, projection } of rendered) {
    const elapsed = formatElapsedMMSS(agent.startTime, projection.runtimeEndedAt ?? now);
    const agentTag = agent.agent ? ` (${agent.agent})` : "";
    const left = ` ${elapsed}  ${agent.name}${agentTag} `;
    const runtimeTag = agent.runtimePlan
      ? `${agent.runtimePlan.modelId}|${agent.runtimePlan.thinking} · `
      : "";
    const right = statusConfig.enabled
      ? ` ${runtimeTag}${formatLifecycleWidgetLabel(projection, now).trim()} `
      : agent.cli === "claude"
        ? ` ${runtimeTag}running… `
        : ` ${runtimeTag}starting… `;

    lines.push(borderLine(left, right, width, accent));
  }

  lines.push(borderBottom(width, accent));
  return lines;
}

function updateWidget() {
  const latestCtx = runtime.latestCtx;
  if (!latestCtx?.hasUI) return;

  if (runningSubagents.size === 0) {
    latestCtx.ui.setWidget("subagent-status", undefined);
    if (widgetInterval) {
      clearInterval(widgetInterval);
      widgetInterval = null;
      (globalThis as any)[WIDGET_INTERVAL_KEY] = null;
    }
    return;
  }

  latestCtx.ui.setWidget(
    "subagent-status",
    (_tui: any, _theme: any) => {
      return {
        invalidate() {},
        render(width: number) {
          return renderSubagentWidgetLines(Array.from(runningSubagents.values()), width);
        },
      };
    },
    { placement: "aboveEditor" },
  );
}

/**
 * Build the positional prompt args for a Pi CLI subagent launch.
 *
 * In artifact-backed launches (lineage-only, standalone), Pi's buildInitialMessage()
 * concatenates @file content with messages[0] into one initial prompt. That breaks
 * /skill: expansion because the message no longer starts with "/skill:". Only
 * messages[1..] are sent as separate follow-up prompts where /skill: is recognized.
 *
 * When there are skill prompts AND artifact-backed delivery, we prepend an empty
 * first positional message so that /skill: args land in messages[1..] and arrive
 * as standalone prompts in the child session.
 */
const SUBAGENT_CONTROL_TOOLS = ["caller_ping", "subagent_done", "agent_complete"] as const;

/**
 * Build the child --tools allowlist.
 *
 * Pi 0.70+ applies --tools to built-in, extension, and custom tools. If a
 * subagent definition restricts tools to e.g. "read,bash,write", the child
 * control tools from subagent-done.ts would otherwise be hidden, leaving a
 * manually resumed or user-touched subagent unable to call subagent_done.
 */
function buildSubagentToolAllowlist(effectiveTools?: string): string | null {
  const requested = (effectiveTools ?? "")
    .split(",")
    .map((tool) => tool.trim())
    .filter(Boolean);

  if (requested.length === 0) return null;

  const allow = new Set(requested);
  for (const tool of SUBAGENT_CONTROL_TOOLS) {
    allow.add(tool);
  }

  return [...allow].join(",");
}

type LaunchPolicy = import("./protocol/workflow-types.ts").AgentLaunchPolicy;

/** Capture the exact least-privilege boundary with the durable child membership. */
function buildLaunchPolicy(input: {
  effectiveTools?: string;
  denyTools: ReadonlySet<string>;
  localAgentDir: string | null;
}): LaunchPolicy {
  const toolAllowlist = buildSubagentToolAllowlist(input.effectiveTools);
  const codingAgentDir = input.localAgentDir && existsSync(input.localAgentDir)
    ? input.localAgentDir
    : process.env.PI_CODING_AGENT_DIR;
  return {
    ...(toolAllowlist ? { toolAllowlist } : {}),
    denyTools: [...input.denyTools],
    ...(codingAgentDir ? { codingAgentDir } : {}),
  };
}

function appendLaunchPolicyEnvironment(parts: string[], policy: LaunchPolicy): void {
  if (policy.codingAgentDir) parts.push(`PI_CODING_AGENT_DIR=${shellQuote(policy.codingAgentDir)}`);
  if (policy.denyTools.length > 0) parts.push(`PI_DENY_TOOLS=${shellQuote(policy.denyTools.join(","))}`);
}

function buildRequestReactivationCommand(input: {
  environment: string[];
  policy: LaunchPolicy;
  sessionPath: string;
}): string {
  const policyEnvironment: string[] = [];
  appendLaunchPolicyEnvironment(policyEnvironment, input.policy);
  const toolArgs = input.policy.toolAllowlist ? ` --tools ${shellQuote(input.policy.toolAllowlist)}` : "";
  return `${[...input.environment, ...policyEnvironment].join(" ")} pi --session ${shellQuote(input.sessionPath)} -e ${shellQuote(join(SUBAGENTS_DIR, "subagent-done.ts"))}${toolArgs}; echo '__SUBAGENT_DONE_'$?'__'`;
}

function buildPiPromptArgs(params: {
  effectiveSkills?: string;
  taskDelivery: "direct" | "artifact";
  taskArg: string;
}): string[] {
  const skillPrompts = (params.effectiveSkills ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((skill) => `/skill:${skill}`);

  const needsSeparator = params.taskDelivery === "artifact" && skillPrompts.length > 0;

  return [
    ...(needsSeparator ? [""] : []),
    ...skillPrompts,
    params.taskArg,
  ];
}

function ensureLifecycle(running: RunningSubagent): SubagentLifecycle {
  if (running.lifecycle) return running.lifecycle;
  let lifecycle = createLifecycle(running.startTime);
  // Claude agents have no activity snapshots; treat confirmed launch as running.
  if (running.cli === "claude") {
    lifecycle = markProcessRunning(lifecycle, running.startTime);
    running.lifecycle = lifecycle;
    return lifecycle;
  }
  const state = running.statusState;
  if (state?.activityLabel === "interrupted" && state.localOverrideAtMs != null) {
    lifecycle = markInterruptRequested(lifecycle, state.localOverrideAtMs);
  } else if (state?.phase === "done") {
    // Legacy activity "done" means the turn ended, not that completion
    // evidence was recorded. Hydrate as Herdr-style waiting and let the
    // preserved watcher consume sidecar/sentinel evidence.
    const observedAt = state.lastActivityAtMs ?? running.startTime;
    lifecycle = observePaneInspection(
      lifecycle,
      { kind: "present", observedAt, agentStatus: "done" },
      observedAt,
    );
  } else if (state?.phase === "active" || state?.phase === "waiting" || state?.phase === "starting") {
    lifecycle = observeActivity(lifecycle, {
      ok: true,
      activity: {
        version: 1,
        runningChildId: running.id,
        createdAt: running.startTime,
        updatedAt: state.lastActivityAtMs ?? running.startTime,
        sequence: state.lastActivitySequence ?? 0,
        latestEvent: state.latestEvent === "agent_end" ? "agent_end" : "agent_start",
        phase: state.phase,
        agentActive: state.phase === "active",
        turnActive: state.phase === "active",
        providerActive: false,
        toolActive: state.activeScope === "tool",
        ...(state.activeScope ? { activeScope: state.activeScope as any } : {}),
        ...(state.activeSinceMs != null ? { activeSince: state.activeSinceMs } : {}),
        ...(state.waitingSinceMs != null ? { waitingSince: state.waitingSinceMs } : {}),
        ...(state.activityLabel && state.activeScope === "tool" ? { toolName: state.activityLabel } : {}),
      },
    }, state.lastActivityAtMs ?? running.startTime);
  } else if (state?.source === "claude" || running.startTime) {
    // Pre-lifecycle Pi agents without a known phase still get a running process.
    lifecycle = markProcessRunning(lifecycle, running.startTime);
  }
  running.lifecycle = lifecycle;
  return lifecycle;
}

function observeRunningSubagent(running: RunningSubagent, observedAt = Date.now()) {
  ensureLifecycle(running);
  if (running.cli === "claude") return;

  const activityFile = running.activityFile;
  const read: ActivityReadResult = activityFile
    ? readSubagentActivityFile(activityFile, running.id)
    : { ok: false, reason: "missing" };

  running.activityRead = read.ok
    ? { ok: true }
    : { ok: false, reason: read.reason, error: read.error };

  if (read.ok) running.activity = read.activity;
  running.lifecycle = observeActivity(ensureLifecycle(running), read, observedAt);
}

function resolveInterruptTarget(params: { id?: string; name?: string }):
  | { running: RunningSubagent }
  | { error: string } {
  const requestedId = params.id?.trim();
  if (requestedId) {
    const running = runningSubagents.get(requestedId);
    return running ? { running } : { error: `No running subagent with id "${requestedId}".` };
  }

  const requestedName = params.name?.trim();
  if (!requestedName) {
    return { error: "Provide a running subagent id or exact display name." };
  }

  const matches = Array.from(runningSubagents.values()).filter((running) => running.name === requestedName);
  if (matches.length === 1) return { running: matches[0] };
  if (matches.length === 0) {
    return { error: `No running subagent named "${requestedName}".` };
  }

  const candidates = matches.map((running) => `${running.name} [${running.id}]`).join(", ");
  return { error: `Ambiguous subagent name "${requestedName}". Matches: ${candidates}` };
}

function requestSubagentInterrupt(
  running: RunningSubagent,
  interruptPaneKey: (surface: string) => void = interruptPane,
): { ok: true } | { error: string } {
  try {
    // Pi's modal editor may consume the first Escape before its active-operation
    // abort handler sees input. A second Escape in the same request reliably
    // reaches the turn interrupt path without closing the Agent Run.
    interruptPaneKey(running.surface);
    interruptPaneKey(running.surface);
    return { ok: true };
  } catch (error: any) {
    return {
      error:
        `Failed to send Escape to subagent "${running.name}" via herdr: ` +
        `${error?.message ?? String(error)}`,
    };
  }
}

function handleSubagentInterrupt(
  params: { id?: string; name?: string },
  interruptPaneKey: (surface: string) => void = interruptPane,
) {
  const resolved = resolveInterruptTarget(params);
  if ("error" in resolved) {
    return {
      content: [{ type: "text" as const, text: resolved.error }],
      details: { error: resolved.error },
    };
  }

  const running = resolved.running;
  if (running.cli === "claude") {
    return {
      content: [{
        type: "text" as const,
        text:
          "Turn-only Escape interrupt is currently supported only for Pi-backed subagents. Claude-backed semantics have not been verified yet.",
      }],
      details: { error: "claude interrupt unsupported", id: running.id, name: running.name },
    };
  }

  const now = Date.now();
  observeRunningSubagent(running, now);

  if (running.workflowOwnership) {
    try {
      runtime.workflowBootstrap.requestInterruption(running.workflowOwnership);
    } catch (error: any) {
      const message = error?.message ?? String(error);
      return {
        content: [{ type: "text" as const, text: message }],
        details: { error: message, id: running.id, name: running.name },
      };
    }
  }

  const interruption = requestSubagentInterrupt(running, interruptPaneKey);
  if ("error" in interruption) {
    return {
      content: [{ type: "text" as const, text: interruption.error }],
      details: { error: interruption.error, id: running.id, name: running.name },
    };
  }

  updateWidget();

  return {
    content: [{ type: "text" as const, text: `Interrupt requested for subagent "${running.name}".` }],
    details: { id: running.id, name: running.name, status: "interrupt_requested" },
  };
}

function startStatusRefresh(pi: ExtensionAPI) {
  if (!statusConfig.enabled || statusInterval) return;

  statusInterval = setInterval(() => {
    if (runningSubagents.size === 0) {
      if (statusInterval) {
        clearInterval(statusInterval);
        statusInterval = null;
        (globalThis as any)[STATUS_INTERVAL_KEY] = null;
      }
      return;
    }

    const transitionLines: string[] = [];
    const now = Date.now();
    let shouldRefreshWidget = false;

    for (const running of runningSubagents.values()) {
      // Dual-writes lifecycle + statusState for reload hydration; steers use lifecycle only.
      observeRunningSubagent(running, now);
      const projection = projectLifecycle(ensureLifecycle(running), now);
      const transition = lifecycleTransition(running.lastProjectedKind, projection.kind);
      if (running.lastProjectedKind !== projection.kind) {
        shouldRefreshWidget = true;
      }
      running.lastProjectedKind = projection.kind;

      // Interactive subagents (long-running, user-driven) intentionally don't
      // wake the parent session on stalled/recovered transitions — the user is
      // working in the subagent's pane, and a steer message here would burn an
      // orchestrator turn on a no-op "still waiting" ping. Widget still updates.
      if (transition && !running.interactive) {
        transitionLines.push(
          formatLifecycleTransitionLine(
            normalizeStatusName(running.name),
            projection,
            transition,
            now,
            running.startTime,
            formatElapsedDuration,
          ),
        );
      }
    }

    if (shouldRefreshWidget) updateWidget();

    if (transitionLines.length > 0) {
      const capped = capStatusLines(transitionLines, statusConfig.lineLimit);
      pi.sendMessage(
        {
          customType: "subagent_status",
          content: formatStatusAggregate(transitionLines, statusConfig.lineLimit),
          display: true,
          details: { lines: capped.visibleLines, overflow: capped.overflow },
        },
        { triggerTurn: true, deliverAs: "steer" },
      );
    }
  }, 1000);

  (globalThis as any)[STATUS_INTERVAL_KEY] = statusInterval;
}

function resolveResumeLaunchBehavior(params: { autoExit?: boolean }): { autoExit: boolean; interactive: boolean } {
  const autoExit = params.autoExit ?? true;
  return { autoExit, interactive: !autoExit };
}

export const __test__ = {
  borderLine,
  getShellReadyDelayMs,
  renderSubagentWidgetLines,
  loadAgentDefaults,
  discoverAgentDefinitions,
  resolveEffectiveSessionMode,
  resolveLaunchBehavior,
  resolveEffectiveAutoExit,
  resolveEffectiveInteractive,
  buildSubagentToolAllowlist,
  buildRequestReactivationCommand,
  buildPiPromptArgs,
  observeRunningSubagent,
  resolveDenyTools,
  resolveInterruptTarget,
  requestSubagentInterrupt,
  handleSubagentInterrupt,
  resolveResultPresentation,
  resolveResumeLaunchBehavior,
  runningSubagents,
  formatElapsed,
  finalizeCommittedRequestReactivation,
};

function startWidgetRefresh() {
  if (widgetInterval) return;
  updateWidget(); // immediate first render
  widgetInterval = setInterval(() => {
    updateWidget();
  }, 1000);
  (globalThis as any)[WIDGET_INTERVAL_KEY] = widgetInterval;
}

/**
 * Launch a subagent: creates the herdr pane, builds the command, and
 * sends it. Returns a RunningSubagent — does NOT poll.
 *
 * Call watchSubagent() on the returned object to observe completion.
 */
async function launchSubagent(
  params: typeof SubagentParams.static,
  ctx: LegacyLaunchContext,
  parentThinking: ThinkingLevel,
  options?: {
    surface?: string;
    spawnedInitialRequest?: { messageId: string; sourceEntryId: string; message: string };
  },
): Promise<RunningSubagent> {
  const startTime = Date.now();
  const id = Math.random().toString(16).slice(2, 10);
  const spawnedInitialRequest = options?.spawnedInitialRequest;
  await runtime.workflowBootstrap.waitUntilReady(ctx);

  const agentDefs = params.agent ? loadAgentDefaults(params.agent) : null;
  if (!ctx.model) throw new Error("Subagent launch requires a resolved parent model");
  const runtimePlan = resolveRuntimePlan(
    { model: params.model, thinking: params.thinking },
    {
      model: resolveModelDefault(params.agent, agentDefs?.model, modelConfig),
      thinking: agentDefs?.thinking,
    },
    { provider: ctx.model.provider, modelId: ctx.model.id, thinking: parentThinking },
    wrapPiModelRegistry(ctx.modelRegistry),
  );
  const effectiveModel = runtimePlan.model;
  const effectiveTools = params.tools ?? agentDefs?.tools;
  const effectiveSkills = params.skills ?? agentDefs?.skills;
  const effectiveThinking = runtimePlan.thinking;
  const effectiveAutoExit = resolveEffectiveAutoExit(params, agentDefs);
  const effectiveInteractive = resolveEffectiveInteractive(params, agentDefs);
  const launchBehavior = resolveLaunchBehavior(params, agentDefs);

  const sessionFile = ctx.sessionManager.getSessionFile();
  if (!sessionFile) throw new Error("No session file");
  const sessionId = ctx.sessionManager.getSessionId();
  const artifactDir = getArtifactDir(ctx.sessionManager.getSessionDir(), sessionId);

  const { effectiveCwd, localAgentDir, effectiveAgentDir } = resolveSubagentPaths(params, agentDefs);
  const denySet = resolveDenyTools(agentDefs);
  const launchPolicy = buildLaunchPolicy({ effectiveTools, denyTools: denySet, localAgentDir });
  const targetCwdForSession = effectiveCwd ?? ctx.cwd;
  const workflowSessionsDirectory = agentDefs?.cli === "claude"
    ? undefined
    : runtime.workflowBootstrap.workflow?.sessionsDirectory;
  const sessionDir = workflowSessionsDirectory ??
    getDefaultSessionDirFor(targetCwdForSession, effectiveAgentDir);

  if (spawnedInitialRequest && agentDefs?.cli === "claude") {
    throw new Error("Spawned Initial Request requires a Pi-backed Agent Definition");
  }

  // Generate a deterministic session file path for this subagent.
  // This eliminates race conditions when multiple agents launch simultaneously —
  // each agent knows exactly which file is theirs.
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 23) + "Z";
  const agentSessionId = randomUUID();
  const subagentSessionFile = join(sessionDir, `${timestamp}_${agentSessionId}.jsonl`);

  if (
    agentDefs?.cli === "claude" &&
    (runtimePlan.thinkingSource !== "parent" || runtimePlan.thinking !== parentThinking)
  ) {
    throw new Error(
      "Thinking-level overrides are not supported for Claude CLI subagents; omit thinking or use a Pi-backed agent.",
    );
  }

  let workflowSessionBinding: ReturnType<typeof bindNewWorkflowSession> | undefined;
  try {
    if (workflowSessionsDirectory) {
      initializeSubagentSessionFile({
        mode: launchBehavior.sessionMode,
        ...(launchBehavior.sessionMode === "standalone"
          ? {}
          : { parentSessionFile: sessionFile }),
        childSessionFile: subagentSessionFile,
        childCwd: targetCwdForSession,
        childSessionId: agentSessionId,
      });
      workflowSessionBinding = bindNewWorkflowSession({
        workflowOwnerId: runtime.workflowBootstrap.workflow!.ownerAgentId,
        agentId: agentSessionId,
        sessionPath: subagentSessionFile,
      });
    } else if (launchBehavior.seededSessionMode) {
      seedSubagentSessionFile({
        mode: launchBehavior.seededSessionMode,
        parentSessionFile: sessionFile,
        childSessionFile: subagentSessionFile,
        childCwd: targetCwdForSession,
      });
    }
  } catch (error) {
    if (workflowSessionsDirectory) runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
    throw error;
  }

  // Use pre-created surface (parallel mode) or create a new one.
  // For new surfaces, pause briefly so the shell is ready before sending the command.
  const surfacePreCreated = !!options?.surface;
  let surface: string;
  try {
    surface = options?.surface ?? createSubagentPane(params.name);
    if (!surfacePreCreated) {
      await new Promise<void>((resolve) => setTimeout(resolve, getShellReadyDelayMs()));
    }
  } catch (error) {
    if (workflowSessionsDirectory) runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
    throw error;
  }

  let provisionalSpawn: ProvisionalSpawnGate | undefined;
  if (spawnedInitialRequest) {
    try {
      provisionalSpawn = await ProvisionalSpawnGate.create();
    } catch (error) {
      try { closePane(surface); } catch {}
      runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
      throw error;
    }
  }

  let preparedWorkflowRun: ReturnType<WorkflowBootstrap["prepareSpawn"]> | undefined;
  if (workflowSessionsDirectory && !spawnedInitialRequest) {
    try {
      preparedWorkflowRun = runtime.workflowBootstrap.prepareSpawn({
        agentId: agentSessionId,
        sessionPath: subagentSessionFile,
        runId: id,
        name: params.name,
        agentDefinition: params.agent,
        capabilities: { spawning: agentDefs?.spawning !== false },
        launchPolicy,
        sessionBinding: workflowSessionBinding!,
        surface,
      });
    } catch (error) {
      try {
        closePane(surface);
      } catch {}
      runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
      throw error;
    }
  }

  const abandonPreparedWorkflowRun = () => {
    void provisionalSpawn?.abort();
    void provisionalSpawn?.close();
    if (!preparedWorkflowRun) {
      if (spawnedInitialRequest) runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
      return;
    }
    try {
      closePane(surface);
    } catch {}
    runtime.workflowBootstrap.abandonPreparedRun(preparedWorkflowRun);
  };

  const activityFile = getSubagentActivityFile(artifactDir, id);
  try {
    mkdirSync(dirname(activityFile), { recursive: true });
  } catch (error) {
    abandonPreparedWorkflowRun();
    throw error;
  }
  const { inheritsConversationContext } = launchBehavior;

  // Build the task message
  // Only full-context fork mode inherits prior conversation state.
  // Blank-session modes need the wrapper instructions and artifact-backed handoff.
  const modeHint = effectiveAutoExit
    ? "Complete your task autonomously."
    : "Complete your task. When finished, call the subagent_done tool. The user can interact with you at any time.";
  const summaryInstruction = effectiveAutoExit
    ? "Your FINAL assistant message should summarize what you accomplished."
    : "Your FINAL assistant message (before calling subagent_done or before the user exits) should summarize what you accomplished.";
  const identity = agentDefs?.body ?? params.systemPrompt ?? null;
  const systemPromptMode = agentDefs?.systemPromptMode;
  const identityInSystemPrompt = systemPromptMode && identity;
  const roleBlock = identity && !identityInSystemPrompt ? `\n\n${identity}` : "";
  const fullTask = inheritsConversationContext
    ? params.task
    : `${roleBlock}\n\n${modeHint}\n\n${params.task}\n\n${summaryInstruction}`;
  // ── Claude Code CLI path ──
  if (agentDefs?.cli === "claude") {
    const sentinelFile = `/tmp/pi-claude-${id}-done`;
    const pluginDir = join(SUBAGENTS_DIR, "plugin");

    const cmdParts: string[] = [];
    cmdParts.push(`PI_CLAUDE_SENTINEL=${shellQuote(sentinelFile)}`);
    cmdParts.push("claude");
    cmdParts.push("--dangerously-skip-permissions");

    if (existsSync(pluginDir)) {
      cmdParts.push("--plugin-dir", shellQuote(pluginDir));
    }

    if (effectiveModel) {
      cmdParts.push("--model", shellQuote(effectiveModel));
    }

    const sp = params.systemPrompt ?? agentDefs.body;
    if (sp) {
      cmdParts.push("--append-system-prompt", shellQuote(sp));
    }

    if (params.resumeSessionId) {
      cmdParts.push("--resume", shellQuote(params.resumeSessionId));
    }

    // Always pass the task as the prompt — even for resumed sessions,
    // the caller's task is the follow-up instruction.
    cmdParts.push(shellQuote(params.task));

    const cdPrefix = effectiveCwd ? `cd ${shellQuote(effectiveCwd)} && ` : "";
    const command = `${cdPrefix}${cmdParts.join(" ")}; echo '__SUBAGENT_DONE_'$?'__'`;

    const launchScriptName = `${(params.name || "subagent")
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") || "subagent"}-${id}.sh`;
    const launchScriptFile = join(artifactDir, "subagent-scripts", launchScriptName);

    runScriptInPane(surface, command, {
      scriptPath: launchScriptFile,
      scriptPreamble: [
        `# Claude Code subagent launch script for ${params.name}`,
        `# Generated: ${new Date().toISOString()}`,
        `# Surface: ${surface}`,
      ].join("\n"),
    });

    const running: RunningSubagent = {
      id,
      name: params.name,
      task: params.task,
      agent: params.agent,
      surface,
      startTime,
      sessionFile: subagentSessionFile,
      launchScriptFile,
      cli: "claude",
      sentinelFile,
      interactive: effectiveInteractive,
      runtimePlan,
      launchKind: "spawn",
      lifecycle: markProcessRunning(createLifecycle(startTime), Date.now()),
    };

    runningSubagents.set(id, running);
    return running;
  }

  // ── Pi CLI path ──

  // Build pi command
  const parts: string[] = ["pi"];
  parts.push("--session", shellQuote(subagentSessionFile));

  const subagentDonePath = join(SUBAGENTS_DIR, "subagent-done.ts");
  parts.push("-e", shellQuote(subagentDonePath));

  if (effectiveModel) {
    parts.push("--model", shellQuote(effectiveModel));
  }
  if (effectiveThinking) {
    parts.push("--thinking", shellQuote(effectiveThinking));
  }

  // Pass agent body as system prompt via file to avoid shell escaping issues
  // with multiline content. Pi's --append-system-prompt and --system-prompt
  // auto-detect file paths and read their contents.
  if (identityInSystemPrompt && identity) {
    const flag = systemPromptMode === "replace" ? "--system-prompt" : "--append-system-prompt";
    const spTimestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    const spSafeName = params.name
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
    const syspromptPath = join(artifactDir, `context/${spSafeName || "subagent"}-sysprompt-${spTimestamp}.md`);
    try {
      mkdirSync(dirname(syspromptPath), { recursive: true });
      writeFileSync(syspromptPath, identity, "utf8");
    } catch (error) {
      abandonPreparedWorkflowRun();
      throw error;
    }
    parts.push(flag, shellQuote(syspromptPath));
  }

  if (launchPolicy.toolAllowlist) {
    parts.push("--tools", shellQuote(launchPolicy.toolAllowlist));
  }

  // Build env prefix: denied tools + subagent identity + config dir propagation
  const envParts: string[] = [];

  appendLaunchPolicyEnvironment(envParts, launchPolicy);
  envParts.push(...getInheritedPiEnvironment());
  envParts.push(`PI_SUBAGENT_NAME=${shellQuote(params.name)}`);
  if (params.agent) {
    envParts.push(`PI_SUBAGENT_AGENT=${shellQuote(params.agent)}`);
  }
  if (effectiveAutoExit && !preparedWorkflowRun && !spawnedInitialRequest) {
    envParts.push(`PI_SUBAGENT_AUTO_EXIT=1`);
  }
  envParts.push(`PI_SUBAGENT_SESSION=${shellQuote(subagentSessionFile)}`);
  envParts.push(`PI_SUBAGENT_ID=${shellQuote(id)}`);
  envParts.push(`PI_SUBAGENT_ACTIVITY_FILE=${shellQuote(activityFile)}`);
  envParts.push(`PI_SUBAGENT_SURFACE=${shellQuote(surface)}`);
  if (provisionalSpawn) {
    const workflow = runtime.workflowBootstrap.workflow!;
    envParts.push(`${WORKFLOW_OWNER_SESSION_ID_ENV}=${shellQuote(workflow.ownerAgentId)}`);
    envParts.push(`${WORKFLOW_OWNER_SESSION_PATH_ENV}=${shellQuote(workflow.ownerSessionPath)}`);
    envParts.push(`${WORKFLOW_AGENT_SESSION_ID_ENV}=${shellQuote(agentSessionId)}`);
    envParts.push(`${PROVISIONAL_SPAWN_ENDPOINT_ENV}=${shellQuote(provisionalSpawn.endpoint)}`);
  }

  // Pass task and skill prompts to the sub-agent.
  // Only full-context fork mode gets a direct task argument because it already
  // inherits the parent conversation. Blank-session modes use artifact-backed
  // handoff so the wrapper instructions arrive as the initial user message.
  let taskArg: string | undefined;
  if (spawnedInitialRequest) {
    // The accepted Inbox Batch is the child's sole initial task context.
    taskArg = undefined;
  } else if (launchBehavior.taskDelivery === "direct") {
    taskArg = fullTask;
  } else {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    const safeName = params.name
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "") // strip everything except alphanumeric, spaces, hyphens
      .replace(/\s+/g, "-") // spaces to hyphens
      .replace(/-+/g, "-") // collapse multiple hyphens
      .replace(/^-|-$/g, ""); // trim leading/trailing hyphens
    const artifactName = `context/${safeName || "subagent"}-${timestamp}.md`;
    const artifactPath = join(artifactDir, artifactName);
    try {
      mkdirSync(dirname(artifactPath), { recursive: true });
      writeFileSync(artifactPath, fullTask, "utf8");
    } catch (error) {
      abandonPreparedWorkflowRun();
      throw error;
    }
    taskArg = `@${artifactPath}`;
  }

  if (taskArg !== undefined) {
    for (const promptArg of buildPiPromptArgs({
      effectiveSkills,
      taskDelivery: launchBehavior.taskDelivery,
      taskArg,
    })) {
      parts.push(shellQuote(promptArg));
    }
  }

  if (preparedWorkflowRun) {
    for (const [key, value] of Object.entries(preparedWorkflowRun.environment)) {
      envParts.push(`${key}=${shellQuote(value)}`);
    }
  }
  const envPrefix = envParts.join(" ") + " ";

  // Resolve cwd — param overrides agent default, supports absolute and relative paths.
  // This was already computed above so session placement, PI_CODING_AGENT_DIR, and cd agree.
  const cdPrefix = effectiveCwd ? `cd ${shellQuote(effectiveCwd)} && ` : "";

  const piCommand = cdPrefix + envPrefix + parts.join(" ");
  const command = `${piCommand}; echo '__SUBAGENT_DONE_'$?'__'`;
  const launchScriptName = `${(params.name || "subagent")
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "") || "subagent"}-${id}.sh`;
  const launchScriptFile = join(artifactDir, "subagent-scripts", launchScriptName);
  let commandAcknowledged = false;
  let spawnedOwnership: import("./protocol/workflow-types.ts").AgentRunOwnership | undefined;
  let provisionalPhase: "precommit" | "committed" = "precommit";
  try {
    runScriptInPane(surface, command, {
      scriptPath: launchScriptFile,
      scriptPreamble: [
        `# Subagent launch script for ${params.name}`,
        `# Generated: ${new Date().toISOString()}`,
        `# Session: ${subagentSessionFile}`,
        `# Surface: ${surface}`,
      ].join("\n"),
    });
    commandAcknowledged = true;
    if (preparedWorkflowRun) {
      runtime.workflowBootstrap.runStarted(preparedWorkflowRun.ownership);
    }
    if (provisionalSpawn && spawnedInitialRequest && workflowSessionBinding) {
      const ready = await provisionalSpawn.waitUntilReady();
      await provisionalSpawn.project({
        senderSessionPath: sessionFile,
        messageId: spawnedInitialRequest.messageId,
        sourceEntryId: spawnedInitialRequest.sourceEntryId,
        senderAgentId: runtime.workflowBootstrap.currentAgentId!,
        recipientAgentId: agentSessionId,
        payloadDigest: digestPayload(spawnedInitialRequest.message),
        agentDefinition: params.agent!,
        agentName: params.name,
      });
      const receipt = runtime.workflowBootstrap.spawnInitialRequest({
        agentId: agentSessionId,
        sessionPath: subagentSessionFile,
        runId: id,
        messageId: spawnedInitialRequest.messageId,
        sourceEntryId: spawnedInitialRequest.sourceEntryId,
        message: spawnedInitialRequest.message,
        name: params.name,
        agentDefinition: params.agent!,
        capabilities: { spawning: agentDefs?.spawning !== false },
        launchPolicy,
        sessionBinding: workflowSessionBinding,
        routerEndpoint: ready.routerEndpoint,
      });
      spawnedOwnership = {
        workflowOwnerId: runtime.workflowBootstrap.workflow!.ownerAgentId,
        agentId: receipt.childAgentId,
        runId: receipt.runId,
        epoch: receipt.fencingEpoch,
        resourceId: `agent-run:${runtime.workflowBootstrap.workflow!.ownerAgentId}:${receipt.childAgentId}`,
      };
      provisionalPhase = "committed";
      try {
        await provisionalSpawn.release({ runId: receipt.runId, fencingEpoch: receipt.fencingEpoch });
      } catch (releaseError) {
        // The transaction is authoritative. Keep the exact session/pane so a
        // retry can reconcile it instead of creating another child.
        process.emitWarning(`Committed Spawn release acknowledgement lost: ${(releaseError as Error).message}`);
      }
      await provisionalSpawn.close();
    }
  } catch (error) {
    if (provisionalSpawn) {
      if (provisionalPhase === "committed") {
        await provisionalSpawn.close();
        throw error;
      }
      await provisionalSpawn.abort();
      await provisionalSpawn.close();
      try { closePane(surface); } catch {}
      runtime.workflowBootstrap.abandonUnpreparedSpawn(subagentSessionFile);
      throw error;
    }
    throw reconcilePreparedAgentRunStartupFailure({
      surface,
      ownership: preparedWorkflowRun?.ownership,
      commandAcknowledged,
      startupError: error,
      closePane,
      abandonPreparedRun: preparedWorkflowRun
        ? () => runtime.workflowBootstrap.abandonPreparedRun(preparedWorkflowRun)
        : undefined,
      terminatePreparedRun: preparedWorkflowRun
        ? (ownership, startupError) => runtime.workflowBootstrap.runTerminated(ownership, true, {
          error: `Agent Run startup failed: ${(startupError as Error).message ?? String(startupError)}`,
        })
        : undefined,
    });
  }

  const running: RunningSubagent = {
    id,
    name: params.name,
    task: params.task,
    agent: params.agent,
    surface,
    startTime,
    sessionFile: subagentSessionFile,
    launchScriptFile,
    activityFile,
    interactive: effectiveInteractive,
    runtimePlan,
    launchKind: "spawn",
    workflowOwnership: spawnedOwnership ?? preparedWorkflowRun?.ownership,
    lifecycle: createLifecycle(startTime),
  };

  runningSubagents.set(id, running);
  return running;
}

/**
 * Watch a launched subagent until it exits. Polls for completion, extracts
 * the summary from the session file, cleans up the surface,
 * and removes the entry from runningSubagents.
 */
const CLAUDE_SESSIONS_DIR = join(
  process.env.HOME ?? "/tmp",
  ".pi", "agent", "sessions", "claude-code",
);

function copyClaudeSession(sentinelFile: string): string | null {
  try {
    const transcriptFile = sentinelFile + ".transcript";
    if (!existsSync(transcriptFile)) return null;
    const transcriptPath = readFileSync(transcriptFile, "utf-8").trim();
    if (!transcriptPath || !existsSync(transcriptPath)) return null;
    mkdirSync(CLAUDE_SESSIONS_DIR, { recursive: true });
    const filename = transcriptPath.split("/").pop() ?? `claude-${Date.now()}.jsonl`;
    const dest = join(CLAUDE_SESSIONS_DIR, filename);
    copyFileSync(transcriptPath, dest);
    return filename;
  } catch {
    return null;
  }
}

async function watchSubagent(
  running: RunningSubagent,
  signal: AbortSignal,
): Promise<SubagentResult> {
  const { name, task, surface, startTime, sessionFile } = running;
  let termination: SubagentResult["termination"] = "uncertain";

  try {
    const result = await waitForCompletion(signal, {
      intervalMs: 1000,
      sessionFile,
      sentinelFile: running.sentinelFile,
      readTerminalTail: () => readPaneAsync(surface, 5),
      inspectPane: async () => inspectPane(surface),
      onPaneInspection: (inspection: PaneInspection, observedAt: number) => {
        ensureLifecycle(running);
        running.lifecycle = observePaneInspection(running.lifecycle, inspection, observedAt);
        updateWidget();
      },
      onTick() {
        observeRunningSubagent(running);
      },
    });

    if (
      result.reason === "error" &&
      result.errorMessage === "Subagent pane disappeared before completion evidence was recorded."
    ) {
      termination = "confirmed";
    }

    const detectedAt = Date.now();
    running.lifecycle = markCompletionDetected(running.lifecycle, result, detectedAt);
    updateWidget();
    const elapsed = Math.floor((detectedAt - startTime) / 1000);

    if (running.cli === "claude") {
      // Claude Code result extraction
      let summary = "";

      if (running.sentinelFile) {
        try {
          summary = readFileSync(running.sentinelFile, "utf-8").trim();
        } catch {}
      }

      if (!summary) {
        summary = readPane(surface, 200)
          .replace(/__SUBAGENT_DONE_\d+__/, "")
          .trimEnd();
      }

      if (!summary) {
        summary = result.exitCode !== 0
          ? `Claude Code exited with code ${result.exitCode}`
          : "Claude Code exited without output";
      }

      // Copy Claude session transcript
      let sessionId: string | null = null;
      if (running.sentinelFile) {
        sessionId = copyClaudeSession(running.sentinelFile);
        try { unlinkSync(running.sentinelFile); } catch {}
        try { unlinkSync(running.sentinelFile + ".transcript"); } catch {}
      }

      closePane(surface);
      termination = "confirmed";
      running.lifecycle = result.exitCode === 0
        ? markCompleted(running.lifecycle, Date.now())
        : markFailed(running.lifecycle, result.errorMessage ?? summary, Date.now(), result.exitCode);

      return {
        name,
        task,
        summary,
        exitCode: result.exitCode,
        elapsed,
        termination,
        ...(sessionId ? { claudeSessionId: sessionId } : {}),
      };
    }

    // Pi subagent result extraction
    let summary: string;
    if (existsSync(sessionFile)) {
      const allEntries = getNewEntries(sessionFile, 0);
      const observed = findObservedSessionRuntime(allEntries);
      if (running.runtimePlan && observed.provider && observed.modelId) {
        const observedModel = `${observed.provider}/${observed.modelId}`;
        const observedThinking =
          observed.thinking === "off" ||
          observed.thinking === "minimal" ||
          observed.thinking === "low" ||
          observed.thinking === "medium" ||
          observed.thinking === "high" ||
          observed.thinking === "xhigh" ||
          observed.thinking === "max"
            ? observed.thinking
            : undefined;
        const mismatch = observedModel !== running.runtimePlan.model
          ? `Resolved model ${running.runtimePlan.model} but child reported ${observedModel}`
          : undefined;
        running.runtimePlan = {
          ...running.runtimePlan,
          ...(observedThinking ? { thinking: observedThinking } : {}),
          observed: {
            model: observedModel,
            ...(observedThinking ? { thinking: observedThinking } : {}),
          },
          ...(mismatch ? { runtimeMismatch: mismatch } : {}),
        };
      }
      summary =
        findLastAssistantMessage(allEntries) ??
        (result.errorMessage
          ? `Subagent error: ${result.errorMessage}`
          : result.exitCode !== 0
            ? `Sub-agent exited with code ${result.exitCode}`
            : "Sub-agent exited without output");
    } else {
      summary = result.errorMessage
        ? `Subagent error: ${result.errorMessage}`
        : result.exitCode !== 0
          ? `Sub-agent exited with code ${result.exitCode}`
          : "Sub-agent exited without output";
    }

    closePane(surface);
    termination = "confirmed";
    running.lifecycle = result.exitCode === 0
      ? markCompleted(running.lifecycle, Date.now())
      : markFailed(running.lifecycle, result.errorMessage ?? summary, Date.now(), result.exitCode);

    return {
      name,
      task,
      summary,
      sessionFile,
      exitCode: result.exitCode,
      elapsed,
      termination,
      ping: result.ping,
      ...(result.errorMessage ? { errorMessage: result.errorMessage } : {}),
    };
  } catch (err: any) {
    try {
      closePane(surface);
      termination = "confirmed";
    } catch {}
    running.lifecycle = markFailed(
      running.lifecycle,
      signal.aborted ? "Subagent cancelled." : err?.message ?? String(err),
      Date.now(),
      1,
    );
    updateWidget();

    if (signal.aborted) {
      return {
        name,
        task,
        summary: "Subagent cancelled.",
        exitCode: 1,
        elapsed: Math.floor((Date.now() - startTime) / 1000),
        error: "cancelled",
        sessionFile,
        termination,
      };
    }
    return {
      name,
      task,
      summary: `Subagent error: ${err?.message ?? String(err)}`,
      exitCode: 1,
      elapsed: Math.floor((Date.now() - startTime) / 1000),
      error: err?.message ?? String(err),
      termination,
    };
  }
}

async function finalizeCommittedRequestReactivation(input: {
  gate: Pick<ProvisionalSpawnGate, "release" | "close">;
  ownership: import("./protocol/workflow-types.ts").AgentRunOwnership;
  registerRunning(): void;
}): Promise<void> {
  try {
    await input.gate.release({ runId: input.ownership.runId, fencingEpoch: input.ownership.epoch });
  } catch (error) {
    process.emitWarning(`Committed Request reactivation release acknowledgement lost: ${(error as Error).message}`);
  }
  await input.gate.close().catch((error) => {
    process.emitWarning(`Committed Request reactivation gate cleanup failed: ${(error as Error).message}`);
  });
  input.registerRunning();
}

async function reactivateEndedRecipientForRequest(
  request: import("./protocol/direct-signal-types.ts").SignalAcceptRequest,
  context: ExtensionContext,
): Promise<import("./protocol/direct-signal-types.ts").QueuedSignalReceipt> {
  const key = `${request.senderAgentId}:${request.sourceEntryId}`;
  const existing = runtime.pendingRequestReactivations.get(key);
  if (existing) return existing;
  const preparation = (async () => {
    if (!isTerminalAvailable()) throw new Error(terminalSetupHint());
    const member = runtime.workflowBootstrap.inspect(request.recipientAgentId);
    const workflow = runtime.workflowBootstrap.workflow;
    if (!workflow) throw new Error("Workflow is unavailable for Request reactivation");
    const id = Math.random().toString(16).slice(2, 10);
    if (!member.launchPolicy) {
      throw new Error(`Ended Agent ${member.agentId} has no durable launch policy; refusing to resume with expanded privileges`);
    }
    const surface = createSubagentPane(member.name);
    const gate = await ProvisionalSpawnGate.create();
    const artifactDir = getArtifactDir(context.sessionManager.getSessionDir(), context.sessionManager.getSessionId());
    const activityFile = getSubagentActivityFile(artifactDir, id);
    mkdirSync(dirname(activityFile), { recursive: true });
    const env = [
      ...getInheritedPiEnvironment(),
      `PI_SUBAGENT_NAME=${shellQuote(member.name)}`,
      `PI_SUBAGENT_SESSION=${shellQuote(member.sessionPath)}`,
      `PI_SUBAGENT_ID=${shellQuote(id)}`,
      `PI_SUBAGENT_ACTIVITY_FILE=${shellQuote(activityFile)}`,
      `PI_SUBAGENT_SURFACE=${shellQuote(surface)}`,
      `${WORKFLOW_OWNER_SESSION_ID_ENV}=${shellQuote(workflow.ownerAgentId)}`,
      `${WORKFLOW_OWNER_SESSION_PATH_ENV}=${shellQuote(workflow.ownerSessionPath)}`,
      `${WORKFLOW_AGENT_SESSION_ID_ENV}=${shellQuote(member.agentId)}`,
      `${WORKFLOW_AGENT_ROLE_ENV}=${shellQuote(humanInterruptActorRoleFromMembership(member))}`,
      `${PROVISIONAL_SPAWN_ENDPOINT_ENV}=${shellQuote(gate.endpoint)}`,
      `PI_WORKFLOW_PROVISIONAL_RUN_KIND=resume`,
    ];
    const command = buildRequestReactivationCommand({
      environment: env,
      policy: member.launchPolicy,
      sessionPath: member.sessionPath,
    });
    let ownership: import("./protocol/workflow-types.ts").AgentRunOwnership | undefined;
    try {
      runScriptInPane(surface, command, {
        scriptPath: join(artifactDir, "subagent-scripts", `${member.name}-request-${id}.sh`),
        scriptPreamble: `# Internal no-prompt Request reactivation for ${member.agentId}`,
      });
      const ready = await gate.waitUntilReady();
      const store = new DirectSignalStore(workflow.databasePath);
      let accepted: import("./protocol/sqlite-message-store.ts").EndedRecipientRequestReceipt;
      try {
        accepted = store.acceptEndedRecipientRequest({
          request,
          recipient: { workflowOwnerId: workflow.ownerAgentId, agentId: member.agentId },
          endpoint: ready.routerEndpoint,
          runId: id,
          checkpoint: JSON.stringify({ surface }),
          acceptedAtMs: Date.now(),
        });
      } finally {
        store.close();
      }
      if (!accepted.committedByThisPreparation) {
        // Another concurrent provisional resume owns the durable Router/run.
        // This pane never committed and must not RELEASE, watch, or terminate it.
        await gate.abort();
        await gate.close();
        try { closePane(surface); } catch {}
        return accepted;
      }
      ownership = accepted.ownership;
      const running: RunningSubagent = {
        id, name: member.name, task: "Request-driven reactivation", agent: member.agentDefinition,
        surface, startTime: Date.now(), sessionFile: member.sessionPath, activityFile,
        interactive: false, runtimePlan: undefined, launchKind: "resume", workflowOwnership: ownership,
        lifecycle: createLifecycle(Date.now()),
      };
      await finalizeCommittedRequestReactivation({
        gate, ownership,
        registerRunning() {
          runningSubagents.set(id, running);
          // This is an internal transport run: preserve lifecycle fencing without
          // relaying an unrelated legacy subagent result to the requester.
          void watchSubagent(running, new AbortController().signal).then((result) => {
            runningSubagents.delete(id);
            runtime.workflowBootstrap.runTerminated(ownership!, hasConfirmedAgentRunTermination(result), {
              error: result.errorMessage ?? `Request-driven Agent Run exited (exit ${result.exitCode})`,
              exitCode: result.exitCode,
            });
          });
        },
      });
      return accepted;
    } catch (error) {
      if (ownership) {
        // Durable commit won. Never ABORT or destroy the session/pane; another
        // source retry must reconcile this exact accepted Request.
        throw error;
      }
      await gate.abort();
      await gate.close();
      try { closePane(surface); } catch {}
      throw error;
    }
  })().finally(() => runtime.pendingRequestReactivations.delete(key));
  runtime.pendingRequestReactivations.set(key, preparation);
  return preparation;
}

function createDefaultLegacyAgentRunAdapters(pi: ExtensionAPI): ConfiguredLegacyAgentRunAdapters {
  return createLegacyAgentRunAdapters({
    pi,
    subagentsDir: SUBAGENTS_DIR,
    runningSubagents,
    currentExtensionApi: () => runtime.pi,
    launch: ({ params, context, parentThinking, spawnedInitialRequest }) => launchSubagent(
      params,
      context,
      parentThinking,
      spawnedInitialRequest ? { spawnedInitialRequest } : undefined,
    ),
    watch: watchSubagent,
    getArtifactDir,
    getShellReadyDelayMs,
    resolveResumeLaunchBehavior,
    resolveResultPresentation,
    shouldDeliverCompletion: shouldDeliverSubagentCompletion,
    selectCompletionApi,
    formatElapsed,
    updateWidget,
    sessionStarted(ctx) {
      runtime.workflowBootstrap.sessionStarted(ctx);
      runtime.latestCtx = ctx;
      runtime.modelCatalog = buildAuthenticatedModelCatalog(wrapPiModelRegistry(ctx.modelRegistry));
      const refreshedGuidelines = buildSubagentRoutingGuidelines(runtime.modelCatalog);
      subagentRoutingGuidelines.splice(0, subagentRoutingGuidelines.length, ...refreshedGuidelines);
      if (runningSubagents.size > 0) {
        startWidgetRefresh();
        startStatusRefresh(pi);
        updateWidget();
      }
    },
    sessionShutdown(reason) {
      if (widgetInterval) {
        clearInterval(widgetInterval);
        widgetInterval = null;
        (globalThis as any)[WIDGET_INTERVAL_KEY] = null;
      }
      if (statusInterval) {
        clearInterval(statusInterval);
        statusInterval = null;
        (globalThis as any)[STATUS_INTERVAL_KEY] = null;
      }
      cleanupSubagentsForShutdown(reason, runningSubagents);
      if (!shouldPreserveSubagentsOnShutdown(reason)) runtime.workflowBootstrap.close();
    },
    runStarted(_running) {
      startWidgetRefresh();
      startStatusRefresh(pi);
    },
    prepareResume(params, _context, runId, surface) {
      return runtime.workflowBootstrap.prepareResume({
        sessionPath: params.sessionPath,
        runId,
        surface,
      });
    },
    abandonPreparedRun(ownership) {
      runtime.workflowBootstrap.runTerminated(ownership, true);
    },
    activatePreparedRun(ownership) {
      runtime.workflowBootstrap.runStarted(ownership);
    },
    terminatePreparedRun(ownership, error) {
      runtime.workflowBootstrap.runTerminated(ownership, true, {
        error: `Agent Run startup failed: ${(error as Error).message ?? String(error)}`,
      });
    },
    watchCompleted(running, result) {
      if (running.workflowOwnership) {
        if (runtime.workflowBootstrap.wasProtocolCompleted(running.workflowOwnership)) return false;
        runtime.workflowBootstrap.runTerminated(
          running.workflowOwnership,
          hasConfirmedAgentRunTermination(result),
          {
            error: result.errorMessage ??
              `Agent Run exited without committed completion or cancellation (exit ${result.exitCode})`,
            exitCode: result.exitCode,
          },
        );
      }
    },
  });
}

function subagentsExtensionWithOptions(
  pi: ExtensionAPI,
  options: SubagentsExtensionOptions = {},
) {
  runtime.pi = pi;
  const legacyAgentRunAdapters =
    options.legacyAgentRunAdapters?.(pi) ?? createDefaultLegacyAgentRunAdapters(pi);

  // Capture the UI context for widget updates and restore presentation for
  // subagents whose watchers survived a reload.
  pi.on("session_start", async (_event, ctx) => {
    legacyAgentRunAdapters.ui.sessionStarted(ctx);
    try {
      runtime.workflowBootstrap.sessionStarted(ctx);
      const sessionFile = ctx.sessionManager?.getSessionFile?.();
      if (sessionFile && existsSync(sessionFile)) {
        await startDirectSignalRouter(pi, runtime.workflowBootstrap, ctx);
      }
    } catch (error) {
      ctx.ui.notify(`Workflow startup failed: ${(error as Error).message}`, "error");
      ctx.shutdown();
      throw error;
    }
  });

  pi.on("before_agent_start", async (_event, ctx) => {
    try {
      runtime.workflowBootstrap.sessionStarted(ctx);
      const sessionFile = ctx.sessionManager?.getSessionFile?.();
      if (sessionFile && existsSync(sessionFile)) {
        await startDirectSignalRouter(pi, runtime.workflowBootstrap, ctx);
      }
      if (runtime.workflowBootstrap.workflow) {
        confirmProjectedInboxBatches(
          runtime.workflowBootstrap,
          ctx.sessionManager.getEntries(),
        );
      }
    } catch (error) {
      ctx.ui.notify(`Workflow turn preparation failed: ${(error as Error).message}`, "error");
      ctx.shutdown();
      throw error;
    }
  });

  pi.on("context", (event) => {
    if (runtime.workflowBootstrap.workflow) {
      confirmProjectedInboxBatches(runtime.workflowBootstrap, event.messages);
    }
  });

  if (!process.env.PI_SUBAGENT_SESSION) {
    let latestAgentRunWasAborted = false;
    pi.on("agent_start", (_event, ctx) => {
      runtime.workflowBootstrap.sessionStarted(ctx);
      if (runtime.workflowBootstrap.workflow) {
        confirmProjectedInboxBatches(
          runtime.workflowBootstrap,
          ctx.sessionManager.getEntries(),
        );
        runtime.workflowBootstrap.currentTurnStarted();
      }
    });

    pi.on("agent_end", (event) => {
      latestAgentRunWasAborted = latestAssistantTurnWasAborted(
        (event as { messages?: unknown[] }).messages,
      );
    });

    pi.on("agent_settled", (_event, ctx) => {
      runtime.workflowBootstrap.sessionStarted(ctx);
      if (runtime.workflowBootstrap.workflow) {
        runtime.workflowBootstrap.currentTurnSettled(latestAgentRunWasAborted);
        runtime.workflowBootstrap.releaseDeferredSignals();
      }
      latestAgentRunWasAborted = false;
    });
  }

  // Clean up on session shutdown
  pi.on("session_shutdown", async (event, _ctx) => {
    const reason = (event as any).reason;
    try {
      if (!shouldPreserveSubagentsOnShutdown(reason)) {
        await runtime.workflowBootstrap.closeDirectSignalRouter();
      }
    } finally {
      legacyAgentRunAdapters.ui.sessionShutdown(reason);
    }
  });

  // Tools denied via PI_DENY_TOOLS env var (set by parent agent based on frontmatter)
  const deniedTools = new Set(
    (process.env.PI_SUBAGENT_ID ? process.env.PI_DENY_TOOLS ?? "" : "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );

  const shouldRegister = (name: string) => !deniedTools.has(name);

  registerAgentInspectTool(pi, runtime.workflowBootstrap, shouldRegister("agent_inspect"));
  registerAgentCancelTool(pi, runtime.workflowBootstrap, shouldRegister("agent_cancel"));
  registerAgentSendTool(pi, runtime.workflowBootstrap, shouldRegister("agent_send"), {
    async reconcileSpawnedInitialRequest(input) {
      const agentDefinition = loadAgentDefaults(input.agent);
      return runtime.workflowBootstrap.reconcileSpawnedInitialRequest({
        sourceEntryId: input.sourceEntryId,
        agentDefinition: input.agent,
        name: input.name ?? input.agent,
        message: input.message,
        capabilities: { spawning: agentDefinition?.spawning !== false },
      });
    },
    async spawnInitialRequest(input) {
      if (!isTerminalAvailable()) throw new Error(terminalSetupHint());
      const parentThinking = pi.getThinkingLevel();
      if (!THINKING_LEVELS.includes(parentThinking as ThinkingLevel)) {
        throw new Error(`Unsupported parent thinking level: ${parentThinking}`);
      }
      const running = await legacyAgentRunAdapters.launcher.launch({
        params: {
          name: input.name ?? input.agent,
          task: input.message,
          agent: input.agent,
        },
        context: input.context,
        parentThinking: parentThinking as ThinkingLevel,
        spawnedInitialRequest: {
          messageId: input.messageId,
          sourceEntryId: input.sourceEntryId,
          message: input.message,
        },
      });
      void superviseLegacyAgentRun(running, legacyAgentRunAdapters);
      const ownership = running.workflowOwnership;
      if (!ownership) throw new Error("Spawned Initial Request did not produce durable Agent Run ownership");
      return {
        status: "delivered" as const,
        messageId: input.messageId,
        recipientAgentId: ownership.agentId,
        acceptanceSequence: 1,
      };
    },
    prepareEndedRecipient(input) {
      return reactivateEndedRecipientForRequest(input.request, input.context);
    },
  });

  // ── subagent tool ──
  if (shouldRegister("subagent"))
    pi.registerTool({
      name: "subagent",
      label: "Subagent",
      description:
        "Spawn a sub-agent in a dedicated terminal herdr pane. " +
        "This is a fire-and-forget async tool: the call returns immediately with only an acknowledgement. " +
        "When the sub-agent finishes, the harness AUTOMATICALLY delivers its result as a steer message that wakes you up and starts a new turn — you do not need to do anything to receive it. " +
        "DO NOT write polling loops, sleep/wait commands, tail/watch scripts, or repeatedly read session/log files to detect completion. DO NOT call subagents_list or any other tool to 'check' status. All of that is wasted work — the harness handles delivery for you. " +
        "DO NOT fabricate, assume, or summarize results after calling this tool. " +
        "After spawning, either end your turn immediately, or work on other independent tasks (including spawning more subagents in parallel). The harness will wake you with the result when it is ready.",
      promptSnippet:
        "Spawn a sub-agent in a dedicated terminal herdr pane. " +
        "This is a fire-and-forget async tool: the call returns immediately with only an acknowledgement. " +
        "When the sub-agent finishes, the harness AUTOMATICALLY delivers its result as a steer message that wakes you up and starts a new turn — you do not need to do anything to receive it. " +
        "DO NOT write polling loops, sleep/wait commands, tail/watch scripts, or repeatedly read session/log files to detect completion. DO NOT call subagents_list or any other tool to 'check' status. All of that is wasted work — the harness handles delivery for you. " +
        "DO NOT fabricate, assume, or summarize results after calling this tool. " +
        "After spawning, either end your turn immediately, or work on other independent tasks (including spawning more subagents in parallel). The harness will wake you with the result when it is ready.",
      promptGuidelines: subagentRoutingGuidelines,
      parameters: SubagentParams,

      async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
        // Prevent self-spawning (e.g. planner spawning another planner)
        const currentAgent = process.env.PI_SUBAGENT_AGENT;
        if (params.agent && currentAgent && params.agent === currentAgent) {
          return {
            content: [
              {
                type: "text",
                text: `You are the ${currentAgent} agent — do not start another ${currentAgent}. You were spawned to do this work yourself. Complete the task directly.`,
              },
            ],
            details: { error: "self-spawn blocked" },
          };
        }

        // Validate prerequisites
        if (!isTerminalAvailable()) {
          return muxUnavailableResult();
        }

        if (!ctx.sessionManager.getSessionFile()) {
          return {
            content: [
              {
                type: "text",
                text: "Error: no session file. Start pi with a persistent session to use subagents.",
              },
            ],
            details: { error: "no session file" },
          };
        }

        // Launch the subagent (creates pane, sends command)
        const parentThinking = pi.getThinkingLevel();
        if (
          parentThinking !== "off" &&
          parentThinking !== "minimal" &&
          parentThinking !== "low" &&
          parentThinking !== "medium" &&
          parentThinking !== "high" &&
          parentThinking !== "xhigh" &&
          parentThinking !== "max"
        ) {
          throw new Error(`Unsupported parent thinking level: ${parentThinking}`);
        }
        const running = await legacyAgentRunAdapters.launcher.launch({
          params,
          context: ctx,
          parentThinking,
        });
        void superviseLegacyAgentRun(running, legacyAgentRunAdapters);
        const workflowAgentId = running.workflowOwnership?.agentId;

        // Return immediately
        return {
          content: [
            {
              type: "text",
              text:
                `Sub-agent "${params.name}" launched and is now running in the background. ` +
                (workflowAgentId ? `Its Workflow Agent ID is ${workflowAgentId}. ` : "") +
                `Do NOT generate or assume any results — you have no idea what the sub-agent will do or produce. ` +
                `The results will be delivered to you automatically as a steer message when the sub-agent finishes. ` +
                `Until then, move on to other work or tell the user you're waiting.`,
            },
          ],
          details: {
            id: running.id,
            name: params.name,
            task: params.task,
            agent: params.agent,
            agentId: workflowAgentId,
            sessionFile: running.sessionFile,
            launchScriptFile: running.launchScriptFile,
            model: running.runtimePlan?.model,
            thinking: running.runtimePlan?.thinking,
            runtimePlan: running.runtimePlan,
            status: "started",
          },
        };
      },

      renderCall(args, theme) {
        const partialArgs = args as Record<string, unknown>;
        const name = typeof partialArgs.name === "string" && partialArgs.name ? partialArgs.name : "(unnamed)";
        const task = typeof partialArgs.task === "string" ? partialArgs.task : "";
        const agent = typeof partialArgs.agent === "string" && partialArgs.agent
          ? theme.fg("dim", ` (${partialArgs.agent})`)
          : "";
        const cwdHint = typeof partialArgs.cwd === "string" && partialArgs.cwd
          ? theme.fg("dim", ` in ${partialArgs.cwd}`)
          : "";
        let text =
          "▸ " +
          theme.fg("toolTitle", theme.bold(name)) +
          agent +
          cwdHint;

        // Show a one-line task preview. renderCall is called repeatedly as the
        // LLM generates tool arguments, so args.task grows token by token.
        // We keep it compact here — Ctrl+O on renderResult expands the full content.
        if (task) {
          const firstLine = task.split("\n").find((l: string) => l.trim()) ?? "";
          const preview = firstLine.length > 100 ? firstLine.slice(0, 100) + "…" : firstLine;
          if (preview) {
            text += "\n" + theme.fg("toolOutput", preview);
          }
          const totalLines = task.split("\n").length;
          if (totalLines > 1) {
            text += theme.fg("muted", ` (${totalLines} lines)`);
          }
        }

        return new Text(text, 0, 0);
      },

      renderResult(result, _opts, theme) {
        const details = result.details as any;
        const name = details?.name ?? "(unnamed)";

        // "Started" result — tool returned immediately
        if (details?.status === "started") {
          const runtime = details?.model
            ? ` — ${details.model}${details.thinking ? ` · ${details.thinking}` : ""}`
            : " — started";
          return new Text(
            theme.fg("accent", "▸") +
              " " +
              theme.fg("toolTitle", theme.bold(name)) +
              theme.fg("dim", runtime),
            0,
            0,
          );
        }

        // Fallback (shouldn't happen)
        const text = typeof result.content[0]?.text === "string" ? result.content[0].text : "";
        return new Text(theme.fg("dim", text), 0, 0);
      },
    });

  // ── subagent_interrupt tool ──
  if (shouldRegister("subagent_interrupt"))
    pi.registerTool({
      name: "subagent_interrupt",
      label: "Interrupt Subagent",
      description:
        "Send Escape to the active turn of a currently running Pi-backed subagent. " +
        "The child pane, session, watcher, and running entry remain alive; this returns only a local acknowledgement " +
        "and does not emit a subagent_result solely because of this request.",
      promptSnippet:
        "Send Escape to the active turn of a currently running Pi-backed subagent. " +
        "The child pane, session, watcher, and running entry remain alive; this returns only a local acknowledgement " +
        "and does not emit a subagent_result solely because of this request.",
      parameters: Type.Object({
        id: Type.Optional(Type.String({ description: "Exact running subagent id" })),
        name: Type.Optional(Type.String({ description: "Exact running subagent display name" })),
      }),

      async execute(_toolCallId, params) {
        return handleSubagentInterrupt(params);
      },

      renderCall(args, theme) {
        const target = args.id ? `${args.id}` : args.name ?? "(unknown)";
        return new Text(
          theme.fg("accent", "▸") +
            " " +
            theme.fg("toolTitle", theme.bold(target)) +
            theme.fg("dim", " — interrupt turn"),
          0,
          0,
        );
      },

      renderResult(result, _opts, theme) {
        const details = result.details as any;
        if (details?.status === "interrupt_requested") {
          return new Text(
            theme.fg("accent", "▸") +
              " " +
              theme.fg("toolTitle", theme.bold(details.name ?? details.id ?? "subagent")) +
              theme.fg("dim", " — interrupt requested"),
            0,
            0,
          );
        }

        const text = typeof result.content[0]?.text === "string" ? result.content[0].text : "";
        return new Text(theme.fg("dim", text), 0, 0);
      },
    });

  // ── subagents_list tool ──
  if (shouldRegister("subagents_list"))
    pi.registerTool({
      name: "subagents_list",
      label: "List Subagents",
      description:
        "List all available subagent definitions. " +
        "Scans project-local .pi/agents/ and global ~/.pi/agent/agents/. " +
        "Project-local agents override global ones with the same name.",
      promptSnippet:
        "List all available subagent definitions. " +
        "Scans project-local .pi/agents/ and global ~/.pi/agent/agents/. " +
        "Project-local agents override global ones with the same name.",
      parameters: Type.Object({}),

      async execute() {
        const list = discoverAgentDefinitions().filter((agent) => !agent.disableModelInvocation);

        if (list.length === 0) {
          return {
            content: [{ type: "text", text: "No subagent definitions found." }],
            details: { agents: [] },
          };
        }

        const lines = list.map((a) => {
          const badge = a.source === "project" ? " (project)" : "";
          const desc = a.description ? ` — ${a.description}` : "";
          const model = a.model ? ` [${a.model}]` : "";
          return `• ${a.name}${badge}${model}${desc}`;
        });

        return {
          content: [{ type: "text", text: lines.join("\n") }],
          details: { agents: list },
        };
      },

      renderResult(result, _opts, theme) {
        const details = result.details as any;
        const agents = details?.agents ?? [];
        if (agents.length === 0) {
          return new Text(theme.fg("dim", "No subagent definitions found."), 0, 0);
        }
        const lines = agents.map((a: any) => {
          const badge = a.source === "project" ? theme.fg("accent", " (project)") : "";
          const desc = a.description ? theme.fg("dim", ` — ${a.description}`) : "";
          const model = a.model ? theme.fg("dim", ` [${a.model}]`) : "";
          return `  ${theme.fg("toolTitle", theme.bold(a.name))}${badge}${model}${desc}`;
        });
        return new Text(lines.join("\n"), 0, 0);
      },
    });



  // ── subagent_resume tool ──
  if (shouldRegister("subagent_resume"))
    pi.registerTool({
      name: "subagent_resume",
      label: "Resume Subagent",
      description:
        "Resume a previous sub-agent session in a new herdr pane. " +
        "This is a fire-and-forget async tool: the call returns immediately with only an acknowledgement. " +
        "When the resumed sub-agent finishes, the harness AUTOMATICALLY delivers its result as a steer message that wakes you up and starts a new turn — you do not need to do anything to receive it. " +
        "DO NOT write polling loops, sleep/wait commands, tail/watch scripts, or repeatedly read session/log files to detect completion. DO NOT poll for status. All of that is wasted work — the harness handles delivery for you. " +
        "DO NOT fabricate or assume results. After resuming, either end your turn or work on other independent tasks; the harness will wake you when the result is ready. " +
        "Use when a sub-agent was cancelled or needs follow-up work.",
      promptSnippet:
        "Resume a previous sub-agent session in a new herdr pane. " +
        "This is a fire-and-forget async tool: the call returns immediately with only an acknowledgement. " +
        "When the resumed sub-agent finishes, the harness AUTOMATICALLY delivers its result as a steer message that wakes you up and starts a new turn — you do not need to do anything to receive it. " +
        "DO NOT write polling loops, sleep/wait commands, tail/watch scripts, or repeatedly read session/log files to detect completion. DO NOT poll for status. All of that is wasted work — the harness handles delivery for you. " +
        "DO NOT fabricate or assume results. After resuming, either end your turn or work on other independent tasks; the harness will wake you when the result is ready. " +
        "Use when a sub-agent was cancelled or needs follow-up work.",
      parameters: SubagentResumeParams,

      renderCall(args, theme) {
        const name = args.name ?? "Resume";
        const text =
          "▸ " +
          theme.fg("toolTitle", theme.bold(name)) +
          theme.fg("dim", " — resuming session");
        return new Text(text, 0, 0);
      },

      renderResult(result, _opts, theme) {
        const details = result.details as any;
        const name = details?.name ?? "Resume";

        if (details?.status === "started") {
          return new Text(
            theme.fg("accent", "▸") +
              " " +
              theme.fg("toolTitle", theme.bold(name)) +
              theme.fg("dim", " — resumed"),
            0,
            0,
          );
        }

        // Fallback
        const text = typeof result.content[0]?.text === "string" ? result.content[0].text : "";
        return new Text(theme.fg("dim", text), 0, 0);
      },

      async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
        const name = params.name ?? "Resume";

        if (!isTerminalAvailable()) {
          return muxUnavailableResult();
        }

        if (!existsSync(params.sessionPath)) {
          return {
            content: [
              { type: "text", text: `Error: session file not found: ${params.sessionPath}` },
            ],
            details: { error: "session not found" },
          };
        }
        runtime.workflowBootstrap.sessionStarted(ctx);
        const running = await legacyAgentRunAdapters.launcher.resume({ params, context: ctx });
        void superviseLegacyAgentRun(running, legacyAgentRunAdapters);

        return {
          content: [{ type: "text", text: `Session "${name}" resumed.` }],
          details: {
            id: running.id,
            name,
            sessionPath: params.sessionPath,
            launchScriptFile: running.launchScriptFile,
            status: "started",
          },
        };
      },
    });

  // /iterate command — fork the session into a subagent
  pi.registerCommand("iterate", {
    description: "Fork session into a subagent for focused work (bugfixes, iteration)",
    handler: async (args, _ctx) => {
      const task = args.trim() || "";
      const toolCall = task
        ? `Use subagent to fork an interactive session. fork: true, interactive: true, name: "Iterate", task: ${JSON.stringify(task)}`
        : `Use subagent to fork an interactive session. fork: true, interactive: true, name: "Iterate", task: "The user wants to do some hands-on work. Help them with whatever they need."`;
      pi.sendUserMessage(toolCall);
    },
  });

  // /subagent command — spawn a subagent by name
  pi.registerCommand("subagent", {
    description: "Spawn a subagent: /subagent <agent> <task>",
    handler: async (args, ctx) => {
      const trimmed = args.trim();
      if (!trimmed) {
        ctx.ui.notify("Usage: /subagent <agent> [task]", "warning");
        return;
      }

      const spaceIdx = trimmed.indexOf(" ");
      const agentName = spaceIdx === -1 ? trimmed : trimmed.slice(0, spaceIdx);
      const task = spaceIdx === -1 ? "" : trimmed.slice(spaceIdx + 1).trim();

      const defs = loadAgentDefaults(agentName);
      if (!defs) {
        ctx.ui.notify(
          `Agent "${agentName}" not found in ~/.pi/agent/agents/ or .pi/agents/`,
          "error",
        );
        return;
      }

      const taskText = task || `You are the ${agentName} agent. Wait for instructions.`;
      const displayName = agentName[0].toUpperCase() + agentName.slice(1);
      const toolCall = `Use subagent with agent: "${agentName}", name: "${displayName}", task: ${JSON.stringify(taskText)}`;
      pi.sendUserMessage(toolCall);
    },
  });

  // ── subagent_result message renderer ──
  pi.registerMessageRenderer("subagent_result", (message, options, theme) => {
    const details = message.details as any;
    if (!details) return undefined;

    return {
      render(width: number): string[] {
        const name = details.name ?? "subagent";
        const exitCode = details.exitCode ?? 0;
        const errorMessage = typeof details.errorMessage === "string" ? details.errorMessage : "";
        const failed = exitCode !== 0 || !!errorMessage;
        const elapsed = details.elapsed != null ? formatElapsed(details.elapsed) : "?";
        const bgFn = failed
          ? (text: string) => theme.bg("toolErrorBg", text)
          : (text: string) => theme.bg("toolSuccessBg", text);
        const icon = failed
          ? theme.fg("error", "✗")
          : theme.fg("success", "✓");
        const status = errorMessage
          ? "failed (provider/agent error)"
          : failed
            ? `failed (exit ${exitCode})`
            : "completed";
        const agentTag = details.agent ? theme.fg("dim", ` (${details.agent})`) : "";

        const header = `${icon} ${theme.fg("toolTitle", theme.bold(name))}${agentTag} ${theme.fg("dim", "—")} ${status} ${theme.fg("dim", `(${elapsed})`)}`;
        const rawContent = typeof message.content === "string" ? message.content : "";

        // Clean summary (remove session ref and leading label for display)
        const summary = rawContent
          .replace(/\n\nSession: .+\nResume: .+$/, "")
          .replace(`Sub-agent "${name}" completed (${elapsed}).\n\n`, "")
          .replace(`Sub-agent "${name}" failed (exit code ${exitCode}).\n\n`, "")
          .replace(
            new RegExp(
              `^Sub-agent "${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}" failed after ${elapsed} \\(provider/agent error — auto-retry exhausted\\)\\.\\n\\n`,
            ),
            "",
          );

        // Build content for the box
        const contentLines = [header];

        if (options.expanded) {
          // Full view: complete summary + session info
          if (summary) {
            for (const line of summary.split("\n")) {
              contentLines.push(line.slice(0, width - 6));
            }
          }
          if (details.sessionFile) {
            contentLines.push("");
            contentLines.push(theme.fg("dim", `Session: ${details.sessionFile}`));
            contentLines.push(theme.fg("dim", `Resume:  pi --session ${details.sessionFile}`));
          }
        } else {
          // Collapsed: preview + expand hint
          if (summary) {
            const previewLines = summary.split("\n").slice(0, 5);
            for (const line of previewLines) {
              contentLines.push(theme.fg("dim", line.slice(0, width - 6)));
            }
            const totalLines = summary.split("\n").length;
            if (totalLines > 5) {
              contentLines.push(theme.fg("muted", `… ${totalLines - 5} more lines`));
            }
          }
          contentLines.push(theme.fg("muted", keyHint("app.tools.expand", "to expand")));
        }

        // Render via Box for background + padding, with blank line above for separation
        const box = new Box(1, 1, bgFn);
        box.addChild(new Text(contentLines.join("\n"), 0, 0));
        return ["", ...box.render(width)];
      },
    };
  });

  // ── subagent_status message renderer ──
  pi.registerMessageRenderer("subagent_status", (message, options, theme) => {
    const details = message.details as any;
    const lines = Array.isArray(details?.lines) ? details.lines : [];
    const overflow = typeof details?.overflow === "number" ? details.overflow : 0;
    if (lines.length === 0 && overflow === 0) return undefined;

    return {
      render(width: number): string[] {
        const lineWidth = Math.max(0, width - 6);
        const contentLines = [
          `${theme.fg("accent", "•")} ${theme.fg("toolTitle", theme.bold("Subagent status"))}`,
          ...lines.map((line: string) => theme.fg("dim", truncateToWidth(line, lineWidth))),
        ];

        if (overflow > 0) {
          contentLines.push(theme.fg("muted", `+${overflow} more running.`));
        }
        if (!options.expanded) {
          contentLines.push(theme.fg("muted", keyHint("app.tools.expand", "to expand")));
        }

        const box = new Box(1, 1, (text: string) => theme.bg("customMessageBg", text));
        box.addChild(new Text(contentLines.join("\n"), 0, 0));
        return ["", ...box.render(width)];
      },
    };
  });

  // ── subagent_ping message renderer ──
  pi.registerMessageRenderer("subagent_ping", (message, options, theme) => {
    const details = message.details as any;
    if (!details) return undefined;

    return {
      render(width: number): string[] {
        const name = details.name ?? "subagent";
        const agentTag = details.agent ? theme.fg("dim", ` (${details.agent})`) : "";
        const bgFn = (text: string) => theme.bg("toolSuccessBg", text);

        const icon = theme.fg("accent", "?");
        const header = `${icon} ${theme.fg("toolTitle", theme.bold(name))}${agentTag} ${theme.fg("dim", "— needs help")}`;

        const contentLines = [header];

        if (options.expanded) {
          contentLines.push("");
          contentLines.push(details.message ?? "");
          if (details.sessionFile) {
            contentLines.push("");
            contentLines.push(theme.fg("dim", `Session: ${details.sessionFile}`));
          }
        } else {
          const preview = (details.message ?? "").split("\n")[0].slice(0, width - 10);
          contentLines.push(theme.fg("dim", preview));
          contentLines.push(theme.fg("muted", keyHint("app.tools.expand", "to expand")));
        }

        const box = new Box(1, 1, bgFn);
        box.addChild(new Text(contentLines.join("\n"), 0, 0));
        return ["", ...box.render(width)];
      },
    };
  });

  // /plan command — start the full planning workflow
  pi.registerCommand("plan", {
    description: "Start a planning session: /plan <what to build>",
    handler: async (args, ctx) => {
      const task = args.trim();
      if (!task) {
        ctx.ui.notify("Usage: /plan <what to build>", "warning");
        return;
      }

      // Load the plan skill from the subagents extension directory
      const planSkillPath = join(SUBAGENTS_DIR, "plan-skill.md");
      let content = readFileSync(planSkillPath, "utf8");
      content = content.replace(/^---\n[\s\S]*?\n---\n*/, "");
      pi.sendUserMessage(
        `<skill name="plan" location="${planSkillPath}">\n${content.trim()}\n</skill>\n\n${task}`,
      );
    },
  });
}

export function createSubagentsExtension(options: SubagentsExtensionOptions = {}) {
  return (pi: ExtensionAPI) => subagentsExtensionWithOptions(pi, options);
}

export default createSubagentsExtension();
