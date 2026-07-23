import { mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  closeHerdrSurface,
  createHerdrSurface,
  createHerdrSurfaceSplit,
  isHerdrAvailable,
  readHerdrScreen,
  readHerdrScreenAsync,
  inspectHerdrPane,
  getHerdrPaneCreationContext,
  listHerdrPanes,
  listHerdrTabs,
  renameHerdrTab,
  renameHerdrWorkspace,
  sendHerdrCommand,
  sendHerdrEscape,
} from "./herdr.ts";

export type PaneId = string;
export type SplitDirection = "right" | "down";

export interface RecoveryPaneDiscoveryLocator {
  workspaceId: string;
  label: string;
  cwd: string;
  surface?: string;
}

export type RecoveryPaneDiscovery =
  | { kind: "present"; surface: string }
  | { kind: "missing" }
  | { kind: "unavailable"; error: string }
  | { kind: "ambiguous"; error: string };

const SETUP_HINT = "Start pi inside herdr (`herdr`, then run `pi`).";

export function isTerminalAvailable(): boolean {
  return isHerdrAvailable();
}

export function terminalSetupHint(): string {
  return SETUP_HINT;
}

function assertTerminalAvailable(): void {
  if (!isTerminalAvailable()) throw new Error(`herdr is not available. ${SETUP_HINT}`);
}

export function shellQuote(value: string): string {
  return "'" + value.replace(/'/g, "'\\''") + "'";
}

/** Preserve process-wide Pi startup constraints in separately created panes. */
export function getInheritedPiEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): string[] {
  return ["PI_OFFLINE", "PI_SKIP_VERSION_CHECK", "PI_TELEMETRY"].flatMap((name) => {
    const value = environment[name];
    return value ? [`${name}=${shellQuote(value)}`] : [];
  });
}

/** Create a new herdr tab and return its root pane ID. */
export function createSubagentPane(name: string): PaneId {
  assertTerminalAvailable();
  return createHerdrSurface(name);
}

export function getSubagentPaneCreationContext(): { workspaceId: string; cwd: string } {
  assertTerminalAvailable();
  return getHerdrPaneCreationContext();
}

export function createRecoveryPane(
  label: string,
  locator: Pick<RecoveryPaneDiscoveryLocator, "workspaceId" | "cwd">,
): PaneId {
  assertTerminalAvailable();
  return createHerdrSurface(label, locator);
}

/** Discover by the durable label/cwd identity, never by a guessed pane id. */
export async function discoverRecoveryPane(
  locator: RecoveryPaneDiscoveryLocator,
): Promise<RecoveryPaneDiscovery> {
  assertTerminalAvailable();
  let panes;
  try {
    panes = await listHerdrPanes(locator.workspaceId);
  } catch (error) {
    return { kind: "unavailable", error: (error as Error).message };
  }
  let tabLabels = new Map<string, string>();
  if (!locator.surface && panes.some((pane) => !pane.label && pane.tabId)) {
    try {
      tabLabels = new Map(
        (await listHerdrTabs(locator.workspaceId))
          .flatMap((tab) => tab.label ? [[tab.tabId, tab.label] as const] : []),
      );
    } catch (error) {
      return { kind: "unavailable", error: (error as Error).message };
    }
  }
  const candidates = panes.filter((pane) => {
    const label = pane.label ?? (pane.tabId ? tabLabels.get(pane.tabId) : undefined);
    if (label !== locator.label) return false;
    const paneCwd = pane.cwd ?? pane.foregroundCwd;
    return paneCwd === locator.cwd;
  });
  if (candidates.length === 0) {
    // A missing label/cwd match is safe absence only because pane list itself
    // was authoritative. A known surface with a changed identity is a fence,
    // not permission to close a different pane.
    if (locator.surface && panes.some((pane) => pane.paneId === locator.surface)) {
      return { kind: "ambiguous", error: "Durable recovery pane identity no longer matches its label/cwd" };
    }
    return { kind: "missing" };
  }
  if (candidates.length !== 1) {
    return { kind: "ambiguous", error: `Found ${candidates.length} panes with the exact recovery identity` };
  }
  const [candidate] = candidates;
  if (locator.surface && candidate.paneId !== locator.surface) {
    return { kind: "ambiguous", error: "Durable recovery pane surface does not match its exact label identity" };
  }
  return { kind: "present", surface: candidate.paneId };
}

/** Split the current herdr pane and return the child pane ID. */
export function splitCurrentPane(name: string, direction: SplitDirection): PaneId {
  assertTerminalAvailable();
  return createHerdrSurfaceSplit(name, direction);
}

export function renameCurrentTab(title: string): void {
  assertTerminalAvailable();
  renameHerdrTab(title);
}

export function renameCurrentWorkspace(title: string): void {
  assertTerminalAvailable();
  renameHerdrWorkspace(title);
}

export function runInPane(paneId: PaneId, command: string): void {
  assertTerminalAvailable();
  sendHerdrCommand(paneId, command);
}

export function interruptPane(paneId: PaneId): void {
  assertTerminalAvailable();
  sendHerdrEscape(paneId);
}

export function runScriptInPane(
  paneId: PaneId,
  command: string,
  options?: { scriptPath?: string; scriptPreamble?: string },
): string {
  const scriptPath =
    options?.scriptPath ??
    join(
      tmpdir(),
      "pi-herdr-subagent-scripts",
      `cmd-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.sh`,
    );
  mkdirSync(dirname(scriptPath), { recursive: true });

  const scriptLines = ["#!/bin/bash"];
  if (options?.scriptPreamble) scriptLines.push(options.scriptPreamble.trimEnd());
  scriptLines.push(command);
  writeFileSync(scriptPath, `${scriptLines.join("\n")}\n`, { mode: 0o755 });

  runInPane(paneId, `bash ${shellQuote(scriptPath)}`);
  return scriptPath;
}

export function readPane(paneId: PaneId, lines = 50): string {
  assertTerminalAvailable();
  return readHerdrScreen(paneId, lines);
}

export async function readPaneAsync(paneId: PaneId, lines = 50): Promise<string> {
  assertTerminalAvailable();
  return readHerdrScreenAsync(paneId, lines);
}

export type { PaneInspection, HerdrAgentStatus } from "./lifecycle.ts";

export async function inspectPane(paneId: PaneId): Promise<import("./lifecycle.ts").PaneInspection> {
  assertTerminalAvailable();
  const result = await inspectHerdrPane(paneId);
  if (result.kind === "present") {
    return { kind: "present", observedAt: Date.now(), ...result };
  }
  return result;
}

export function closePane(paneId: PaneId): void {
  assertTerminalAvailable();
  closeHerdrSurface(paneId);
}
