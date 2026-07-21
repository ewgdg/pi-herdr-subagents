import { existsSync, realpathSync, unlinkSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { readPiSessionUuid } from "./workflow-identity.ts";
import type { WorkflowSessionBinding } from "./workflow-session-binding.ts";
import {
  ActivationLifecycleStore,
  type ActivationRecord,
  type FailedExit,
  type InterruptionRequest,
} from "./activation-lifecycle.ts";
import {
  WorkflowControlPlane,
  WorkflowProtocolError,
  type AgentCapabilityConfiguration,
  type AgentRecord,
  type AgentRunOwnership,
  type WorkflowRecord,
} from "./workflow-control-plane.ts";
import {
  DirectSignalRuntime,
  type InboxBatch,
  type QueuedSignalReceipt,
} from "./direct-signal.ts";

export const WORKFLOW_OWNER_SESSION_ID_ENV = "PI_WORKFLOW_OWNER_SESSION_ID";
export const WORKFLOW_OWNER_SESSION_PATH_ENV = "PI_WORKFLOW_OWNER_SESSION_PATH";
export const WORKFLOW_AGENT_SESSION_ID_ENV = "PI_WORKFLOW_AGENT_SESSION_ID";
export const WORKFLOW_RUN_ID_ENV = "PI_WORKFLOW_RUN_ID";
export const WORKFLOW_FENCING_EPOCH_ENV = "PI_WORKFLOW_FENCING_EPOCH";
export const WORKFLOW_ACTIVATION_ID_ENV = "PI_WORKFLOW_ACTIVATION_ID";
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
  #directSignals: DirectSignalRuntime | undefined;
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
      if (ownership) {
        this.#controlPlane.assertCurrentAgentRun(ownership);
        this.#selfOwnership = ownership;
      }
    } else {
      this.#controlPlane = WorkflowControlPlane.openAgentFromSession({
        agentSessionId: sessionId,
        agentSessionPath: sessionPath,
        now: this.#now,
      });
      if (this.#controlPlane) {
        const ownership = this.#controlPlane.acquireCurrentAgentRun(randomUUID());
        try {
          this.#controlPlane.startActivation(ownership);
          this.#selfOwnership = ownership;
        } catch (activationError) {
          let releaseError: unknown;
          try {
            this.#controlPlane.releaseAgentRun(ownership);
          } catch (error) {
            releaseError = error;
          }
          this.#controlPlane.close();
          this.#controlPlane = undefined;
          if (releaseError) {
            throw new AggregateError(
              [activationError, releaseError],
              `Manual Agent Run activation startup failed and ownership release failed for ${sessionId}`,
            );
          }
          throw activationError;
        }
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
    if (!context.sessionManager?.getSessionFile()) {
      throw new Error("Durable Workflow requires a persistent Pi session file");
    }
    this.sessionStarted(context, environment);
    while (!this.#controlPlane) {
      await new Promise<void>((resolve) => setTimeout(resolve, SESSION_BOOTSTRAP_RETRY_MS));
      this.sessionStarted(context, environment);
    }
  }

  close(): void {
    if (this.#directSignals) {
      void Promise.resolve(this.#directSignals.close()).catch((error) => {
        process.emitWarning(`Direct Signal Router cleanup failed: ${(error as Error).message}`);
      });
    }
    this.#directSignals = undefined;
    if (this.#controlPlane && this.#selfOwnership) {
      const activation = this.#controlPlane.inspectActivation(
        this.#controlPlane.agent(this.#selfOwnership.agentId),
      );
      if (activation?.runId === this.#selfOwnership.runId && activation.state.kind !== "ended") {
        this.#controlPlane.failAgentRun(this.#selfOwnership, {
          error: "Agent Run runtime closed without committed completion or cancellation",
        });
      }
    }
    if (this.#sessionBootstrapTimer) clearTimeout(this.#sessionBootstrapTimer);
    this.#sessionBootstrapTimer = undefined;
    this.#controlPlane?.close();
    this.#controlPlane = undefined;
    this.#selfOwnership = undefined;
    this.#sessionId = undefined;
    this.#sessionPath = undefined;
  }

  inspect(agentId: string): AgentRecord {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.inspectAgent(controlPlane.agent(agentId));
  }

  inspectActivation(agentId: string): ActivationRecord | undefined {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.inspectActivation(controlPlane.agent(agentId));
  }

  async startDirectSignalRouter(input: {
    projectInboxBatch(batch: InboxBatch): void;
    hasProjectedMessage?(messageId: string): boolean;
    wakeRecipient?: () => void;
  }): Promise<void> {
    const runtime = this.#directSignalRuntime();
    runtime.configureInboxDelivery(input);
    await runtime.start();
  }

  sendDirectSignal(input: {
    targetAgentId: string;
    message: string;
    sourceEntryId: string;
    deliveryTiming?: "steer" | "deferred";
  }): Promise<QueuedSignalReceipt> {
    const controlPlane = this.#requireControlPlane();
    return this.#directSignalRuntime().sendSignal({
      target: controlPlane.agent(input.targetAgentId),
      message: input.message,
      sourceEntryId: input.sourceEntryId,
      deliveryTiming: input.deliveryTiming,
    });
  }

  sendDirectMessage(input: {
    target: { agentId: string } | { requestId: string };
    message: string;
    sourceEntryId: string;
    deliveryTiming?: "steer" | "deferred";
    responseRequired?: boolean;
  }): Promise<QueuedSignalReceipt> {
    return this.#directSignalRuntime().sendMessage(input);
  }

  confirmDirectSignalDelivery(messageId: string): boolean {
    return this.#directSignalRuntime().confirmDelivery(messageId);
  }

  releaseDeferredSignals(): void {
    this.#directSignals?.releaseDeferred();
  }

  async closeDirectSignalRouter(): Promise<void> {
    const runtime = this.#directSignals;
    this.#directSignals = undefined;
    if (runtime) await runtime.close();
  }

  runStarted(ownership: AgentRunOwnership): ActivationRecord {
    return this.#requireControlPlane().startActivation(ownership);
  }

  currentTurnStarted(): ActivationRecord | undefined {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId) return undefined;
    const ownership = this.#requireSelfOwnership();
    const activation = controlPlane.inspectActivation(controlPlane.currentAgent);
    if (!activation) return controlPlane.startActivation(ownership);
    return controlPlane.activateTurn(ownership);
  }

  currentTurnSettled(interrupted: boolean): ActivationRecord | { kind: "owner-turn-settled" } {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId) {
      return controlPlane.settleOwnerTurn();
    }
    const ownership = this.#requireSelfOwnership();
    return interrupted
      ? controlPlane.confirmInterruption(ownership)
      : controlPlane.settleActivation(ownership);
  }

  requestInterruption(ownership: AgentRunOwnership): InterruptionRequest {
    return this.#requireControlPlane().requestInterruption(ownership);
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
      const activation = controlPlane.inspectActivation(agent);
      if (activation?.runId === current.runId && activation.state.kind !== "ended") {
        controlPlane.failAgentRun(current, {
          error: "Previous Agent Run termination was confirmed during resume reconciliation",
        });
      } else {
        controlPlane.releaseAgentRun(current);
      }
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

  runTerminated(
    ownership: AgentRunOwnership,
    confirmed: boolean,
    failure: FailedExit = {
      error: "Agent Run exited without committed completion or cancellation",
    },
  ): ActivationRecord | undefined {
    if (!confirmed) return;
    if (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId) {
      const activation = this.#controlPlane.inspectActivation(
        this.#controlPlane.agent(ownership.agentId),
      );
      if (activation?.runId === ownership.runId && activation.state.kind !== "ended") {
        return this.#controlPlane.failAgentRun(ownership, failure);
      }
      if (activation?.runId === ownership.runId && activation.state.kind === "ended") {
        return undefined;
      }
      this.#controlPlane.releaseAgentRun(ownership);
      return undefined;
    }
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId);
    if (!databasePath) {
      throw new Error(`Unknown durable Workflow for Agent Run ${ownership.runId}`);
    }
    const activationStore = new ActivationLifecycleStore(databasePath);
    try {
      const activation = activationStore.inspect(ownership);
      if (activation?.runId === ownership.runId && activation.state.kind !== "ended") {
        return activationStore.failAndRelease(ownership, failure, this.#now());
      }
      if (activation?.runId === ownership.runId && activation.state.kind === "ended") {
        return undefined;
      }
      activationStore.releaseWithoutActivation(ownership);
      return undefined;
    } finally {
      activationStore.close();
    }
  }

  #requireControlPlane(): WorkflowControlPlane {
    if (!this.#controlPlane) {
      throw new Error("Durable Workflow is unavailable before persistent session startup");
    }
    return this.#controlPlane;
  }

  #requireSelfOwnership(): AgentRunOwnership {
    if (!this.#selfOwnership) {
      throw new Error("Current Subagent Agent Run ownership is unavailable");
    }
    return this.#selfOwnership;
  }

  #directSignalRuntime(): DirectSignalRuntime {
    if (this.#directSignals) return this.#directSignals;
    const controlPlane = this.#requireControlPlane();
    this.#directSignals = new DirectSignalRuntime({
      controlPlane,
      ownership: this.#selfOwnership,
      now: this.#now,
    });
    return this.#directSignals;
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
    [WORKFLOW_ACTIVATION_ID_ENV]: ownership.runId,
  };
}

function ownershipFromEnvironment(
  environment: NodeJS.ProcessEnv | Record<string, string | undefined>,
  workflowOwnerId: string,
  agentId: string,
): AgentRunOwnership | undefined {
  const runId = environment[WORKFLOW_RUN_ID_ENV];
  const epochText = environment[WORKFLOW_FENCING_EPOCH_ENV];
  const activationId = environment[WORKFLOW_ACTIVATION_ID_ENV];
  if (!runId && !epochText && !activationId) return undefined;
  if (!runId || !epochText || !activationId) {
    throw new Error("Incomplete Agent Run ownership environment");
  }
  if (activationId !== runId) throw new Error("Agent Run and activation identities do not match");
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
