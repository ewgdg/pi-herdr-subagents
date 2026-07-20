import { existsSync, realpathSync, unlinkSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { readPiSessionUuid } from "./workflow-identity.ts";
import type { WorkflowSessionBinding } from "./workflow-session-binding.ts";
import { AgentRunOwnershipStore } from "./agent-run-ownership.ts";
import {
  WorkflowControlPlane,
  WorkflowProtocolError,
  type AgentCapabilityConfiguration,
  type AgentRecord,
  type AgentRunOwnership,
  type WorkflowRecord,
} from "./workflow-control-plane.ts";

export const WORKFLOW_OWNER_SESSION_ID_ENV = "PI_WORKFLOW_OWNER_SESSION_ID";
export const WORKFLOW_OWNER_SESSION_PATH_ENV = "PI_WORKFLOW_OWNER_SESSION_PATH";
export const WORKFLOW_AGENT_SESSION_ID_ENV = "PI_WORKFLOW_AGENT_SESSION_ID";
export const WORKFLOW_RUN_ID_ENV = "PI_WORKFLOW_RUN_ID";
export const WORKFLOW_FENCING_EPOCH_ENV = "PI_WORKFLOW_FENCING_EPOCH";
const SESSION_BOOTSTRAP_RETRY_MS = 25;

export interface WorkflowBootstrapContext {
  sessionManager?: {
    getSessionId(): string;
    getSessionFile(): string | null | undefined;
  };
}

export interface PreparedAgentRun {
  ownership: AgentRunOwnership;
  environment: Record<string, string>;
  sessionPath: string;
}

export interface PrepareSpawnInput {
  agentId: string;
  sessionPath: string;
  runId: string;
  name: string;
  agentDefinition?: string;
  capabilities?: AgentCapabilityConfiguration;
  sessionBinding: WorkflowSessionBinding;
  surface: string;
}

export interface PrepareResumeInput {
  sessionPath: string;
  runId: string;
  surface: string;
}

export interface AgentRunLocator {
  surface: string;
}

export class WorkflowBootstrap {
  readonly #now: () => number;
  readonly #confirmRunTerminated: ((locator: AgentRunLocator) => Promise<boolean>) | undefined;
  #controlPlane: WorkflowControlPlane | undefined;
  #sessionId: string | undefined;
  #sessionPath: string | undefined;
  #selfOwnership: AgentRunOwnership | undefined;
  #sessionBootstrapTimer: ReturnType<typeof setTimeout> | undefined;
  readonly #workflowDatabases = new Map<string, string>();

  constructor(options: {
    now?: () => number;
    confirmRunTerminated?: (locator: AgentRunLocator) => Promise<boolean>;
  } = {}) {
    this.#now = options.now ?? Date.now;
    this.#confirmRunTerminated = options.confirmRunTerminated;
  }

  get workflow(): WorkflowRecord | undefined {
    return this.#controlPlane?.workflow;
  }

  get currentAgentId(): string | undefined {
    return this.#controlPlane?.currentAgent.agentId;
  }

  sessionStarted(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined> = process.env,
  ): void {
    if (!context.sessionManager) return;
    const sessionPath = context.sessionManager.getSessionFile();
    if (!sessionPath) return;
    if (!existsSync(sessionPath)) {
      this.#scheduleSessionBootstrap(context, environment);
      return;
    }
    const sessionId = context.sessionManager.getSessionId();
    if (this.#sessionId === sessionId && this.#sessionPath === sessionPath && this.#controlPlane) {
      return;
    }

    this.close();
    const ownerSessionId = environment[WORKFLOW_OWNER_SESSION_ID_ENV];
    const ownerSessionPath = environment[WORKFLOW_OWNER_SESSION_PATH_ENV];
    const expectedAgentId = environment[WORKFLOW_AGENT_SESSION_ID_ENV];
    if (ownerSessionId || ownerSessionPath || expectedAgentId) {
      if (!ownerSessionId || !ownerSessionPath || !expectedAgentId) {
        throw new Error("Incomplete durable Workflow bootstrap environment");
      }
      if (expectedAgentId !== sessionId) {
        throw new Error(
          `Workflow bootstrap expected Agent ${expectedAgentId}, but Pi opened ${sessionId}`,
        );
      }
      this.#controlPlane = WorkflowControlPlane.openAgent({
        ownerSessionId,
        ownerSessionPath,
        agentSessionId: sessionId,
        agentSessionPath: sessionPath,
        now: this.#now,
      });
      const ownership = ownershipFromEnvironment(environment, ownerSessionId, sessionId);
      if (ownership) this.#controlPlane.assertCurrentAgentRun(ownership);
    } else {
      this.#controlPlane = WorkflowControlPlane.openAgentFromSession({
        agentSessionId: sessionId,
        agentSessionPath: sessionPath,
        now: this.#now,
      });
      if (this.#controlPlane) {
        this.#selfOwnership = this.#controlPlane.acquireCurrentAgentRun(randomUUID());
      } else {
        this.#controlPlane = WorkflowControlPlane.startOwner({
          ownerSessionId: sessionId,
          ownerSessionPath: sessionPath,
          now: this.#now,
        });
      }
    }
    this.#sessionId = sessionId;
    this.#sessionPath = sessionPath;
    this.#workflowDatabases.set(
      this.#controlPlane.workflow.ownerAgentId,
      this.#controlPlane.workflow.databasePath,
    );
  }

  /** Wait until Pi has persisted the current session before launching work. */
  async waitUntilReady(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined> = process.env,
  ): Promise<void> {
    this.sessionStarted(context, environment);
    while (!this.#controlPlane) {
      await new Promise<void>((resolve) => setTimeout(resolve, SESSION_BOOTSTRAP_RETRY_MS));
      this.sessionStarted(context, environment);
    }
  }

  close(): void {
    let releaseError: unknown;
    try {
      if (this.#controlPlane && this.#selfOwnership) {
        this.#controlPlane.releaseAgentRun(this.#selfOwnership);
      }
    } catch (error) {
      releaseError = error;
    }
    if (this.#sessionBootstrapTimer) clearTimeout(this.#sessionBootstrapTimer);
    this.#sessionBootstrapTimer = undefined;
    this.#controlPlane?.close();
    this.#controlPlane = undefined;
    this.#selfOwnership = undefined;
    this.#sessionId = undefined;
    this.#sessionPath = undefined;
    if (releaseError) throw releaseError;
  }

  inspect(agentId: string): AgentRecord {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.inspectAgent(controlPlane.agent(agentId));
  }

  prepareSpawn(input: PrepareSpawnInput): PreparedAgentRun {
    const controlPlane = this.#requireControlPlane();
    let member: AgentRecord;
    try {
      member = controlPlane.addAgent({
        agentId: input.agentId,
        sessionPath: input.sessionPath,
        spawner: controlPlane.currentAgent,
        name: input.name,
        agentDefinition: input.agentDefinition,
        capabilities: input.capabilities,
        sessionBinding: input.sessionBinding,
      });
    } catch (error) {
      removePreparedSessionArtifacts(input.sessionPath);
      throw error;
    }
    let ownership: AgentRunOwnership | undefined;
    try {
      ownership = controlPlane.acquireAgentRun(
        controlPlane.agent(member.agentId),
        input.runId,
      );
      controlPlane.writeAgentRunCheckpoint(
        ownership,
        serializeRunLocator({ surface: input.surface }),
      );
    } catch (error) {
      if (ownership) controlPlane.releaseAgentRun(ownership);
      controlPlane.removeAgent(controlPlane.agent(member.agentId));
      removePreparedSessionArtifacts(member.sessionPath);
      throw error;
    }
    return {
      ownership: ownership!,
      environment: buildEnvironment(controlPlane.workflow, ownership!),
      sessionPath: member.sessionPath,
    };
  }

  /** Roll back a prepared spawn that never became a running Agent Run. */
  abandonPreparedRun(prepared: PreparedAgentRun): void {
    const controlPlane = this.#requireControlPlane();
    controlPlane.releaseAgentRun(prepared.ownership);
    controlPlane.removeAgent(controlPlane.agent(prepared.ownership.agentId));
    removePreparedSessionArtifacts(prepared.sessionPath);
  }

  /** Remove session artifacts when spawn preparation failed before membership. */
  abandonUnpreparedSpawn(sessionPath: string): void {
    removePreparedSessionArtifacts(sessionPath);
  }

  async prepareResume(input: PrepareResumeInput): Promise<PreparedAgentRun> {
    const controlPlane = this.#requireControlPlane();
    const requestedSessionPath = realpathSync(input.sessionPath);
    const agentId = readPiSessionUuid(requestedSessionPath);
    const member = controlPlane.inspectAgent(controlPlane.agent(agentId));
    if (member.sessionPath !== requestedSessionPath) {
      throw new WorkflowProtocolError(
        "InvalidSessionIdentity",
        `Resume transcript does not match durable Agent ${agentId}: ${requestedSessionPath}`,
      );
    }
    const agent = controlPlane.agent(member.agentId);
    let ownership: AgentRunOwnership;
    try {
      ownership = controlPlane.acquireAgentRun(agent, input.runId);
    } catch (error) {
      if (
        !(error instanceof WorkflowProtocolError) ||
        error.code !== "AgentRunAlreadyOwned" ||
        !this.#confirmRunTerminated
      ) {
        throw error;
      }
      const current = controlPlane.currentAgentRun(agent);
      const checkpoint = controlPlane.readAgentRunCheckpoint(agent);
      if (!current || !checkpoint || checkpoint.fencingEpoch !== current.epoch) throw error;
      const locator = parseRunLocator(checkpoint.value);
      if (!locator || !(await this.#confirmRunTerminated(locator))) throw error;
      controlPlane.releaseAgentRun(current);
      ownership = controlPlane.acquireAgentRun(agent, input.runId);
    }
    controlPlane.writeAgentRunCheckpoint(
      ownership,
      serializeRunLocator({ surface: input.surface }),
    );
    return {
      ownership,
      environment: buildEnvironment(controlPlane.workflow, ownership),
      sessionPath: member.sessionPath,
    };
  }

  runTerminated(ownership: AgentRunOwnership, confirmed: boolean): void {
    if (!confirmed) return;
    if (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId) {
      this.#controlPlane.releaseAgentRun(ownership);
      return;
    }
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId);
    if (!databasePath) {
      throw new Error(`Unknown durable Workflow for Agent Run ${ownership.runId}`);
    }
    const ownershipStore = new AgentRunOwnershipStore(databasePath);
    try {
      ownershipStore.release(ownership);
    } finally {
      ownershipStore.close();
    }
  }

  #requireControlPlane(): WorkflowControlPlane {
    if (!this.#controlPlane) {
      throw new Error("Durable Workflow is unavailable before persistent session startup");
    }
    return this.#controlPlane;
  }

  #scheduleSessionBootstrap(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined>,
  ): void {
    if (this.#sessionBootstrapTimer) return;
    const environmentSnapshot = { ...environment };
    this.#sessionBootstrapTimer = setTimeout(() => {
      this.#sessionBootstrapTimer = undefined;
      this.sessionStarted(context, environmentSnapshot);
    }, SESSION_BOOTSTRAP_RETRY_MS);
    this.#sessionBootstrapTimer.unref?.();
  }
}

function buildEnvironment(
  workflow: WorkflowRecord,
  ownership: AgentRunOwnership,
): Record<string, string> {
  return {
    [WORKFLOW_OWNER_SESSION_ID_ENV]: workflow.ownerAgentId,
    [WORKFLOW_OWNER_SESSION_PATH_ENV]: workflow.ownerSessionPath,
    [WORKFLOW_AGENT_SESSION_ID_ENV]: ownership.agentId,
    [WORKFLOW_RUN_ID_ENV]: ownership.runId,
    [WORKFLOW_FENCING_EPOCH_ENV]: String(ownership.epoch),
  };
}

function ownershipFromEnvironment(
  environment: NodeJS.ProcessEnv | Record<string, string | undefined>,
  workflowOwnerId: string,
  agentId: string,
): AgentRunOwnership | undefined {
  const runId = environment[WORKFLOW_RUN_ID_ENV];
  const epochText = environment[WORKFLOW_FENCING_EPOCH_ENV];
  if (!runId && !epochText) return undefined;
  if (!runId || !epochText) throw new Error("Incomplete Agent Run ownership environment");
  const epoch = Number(epochText);
  if (!Number.isSafeInteger(epoch) || epoch <= 0) {
    throw new Error(`Invalid Agent Run fencing epoch: ${epochText}`);
  }
  return {
    workflowOwnerId,
    agentId,
    runId,
    epoch,
    resourceId: `agent-run:${workflowOwnerId}:${agentId}`,
  };
}

function serializeRunLocator(locator: AgentRunLocator): string {
  return JSON.stringify(locator);
}

function parseRunLocator(value: string): AgentRunLocator | undefined {
  try {
    const candidate = JSON.parse(value) as { surface?: unknown };
    return typeof candidate.surface === "string" && candidate.surface
      ? { surface: candidate.surface }
      : undefined;
  } catch {
    return undefined;
  }
}

function removePreparedSessionArtifacts(sessionPath: string): void {
  for (const path of [sessionPath, `${sessionPath}.workflow.json`]) {
    try {
      unlinkSync(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
}
