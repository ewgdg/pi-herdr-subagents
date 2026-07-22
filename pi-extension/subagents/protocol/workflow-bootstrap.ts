import { existsSync, realpathSync, unlinkSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { readPiSessionUuid } from "./workflow-identity.ts";
import { createWorkflowLayout } from "./workflow-layout.ts";
import { RecipientInboxRouter } from "./recipient-inbox-router.ts";
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
import {
  awaitProvisionalSpawnCommit,
  PROVISIONAL_AGENT_RUN_KIND_ENV,
  PROVISIONAL_SPAWN_ENDPOINT_ENV,
  type ProvisionalSpawnProjection,
} from "./provisional-spawn.ts";
import { resolveCanonicalSpawnedInitialMessage } from "./direct-signal-transcript.ts";
import { DirectSignalStore } from "./sqlite-message-store.ts";
import type { InspectionTarget } from "./workflow-inspection.ts";
import { CompletionGateStore, type CompletionSource } from "./completion-gate.ts";

export const WORKFLOW_OWNER_SESSION_ID_ENV = "PI_WORKFLOW_OWNER_SESSION_ID";
export const WORKFLOW_OWNER_SESSION_PATH_ENV = "PI_WORKFLOW_OWNER_SESSION_PATH";
export const WORKFLOW_AGENT_SESSION_ID_ENV = "PI_WORKFLOW_AGENT_SESSION_ID";
export const WORKFLOW_RUN_ID_ENV = "PI_WORKFLOW_RUN_ID";
export const WORKFLOW_FENCING_EPOCH_ENV = "PI_WORKFLOW_FENCING_EPOCH";
export const WORKFLOW_ACTIVATION_ID_ENV = "PI_WORKFLOW_ACTIVATION_ID";
export const WORKFLOW_AGENT_ROLE_ENV = "PI_WORKFLOW_AGENT_ROLE";
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
  launchPolicy?: import("./workflow-types.ts").AgentLaunchPolicy;
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
  #humanInterruptActorRole: "ordinary" | "moderator" = "ordinary";
  #sessionBootstrapTimer: ReturnType<typeof setTimeout> | undefined;
  #directSignals: DirectSignalRuntime | undefined;
  #provisionalBootstrap: Promise<void> | undefined;
  #preparedRecipientRouter: RecipientInboxRouter | undefined;
  #preparedRouterCommitted = false;
  #provisionalInboxProjector: ((batch: InboxBatch) => Promise<void>) | undefined;
  #provisionalInboxRelease: (() => void) | undefined;
  #provisionalRouterStartup: {
    projectInboxBatch(batch: InboxBatch): void;
    hasProjectedMessage?(messageId: string): boolean;
    wakeRecipient?: () => void;
  } | undefined;
  #provisionalAgentId: string | undefined;
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

  get humanInterruptActorRole(): "ordinary" | "moderator" {
    return this.#humanInterruptActorRole;
  }

  sessionStarted(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined> = process.env,
  ): void {
    if (!context.sessionManager) return;
    const sessionPath = context.sessionManager.getSessionFile();
    if (!sessionPath) return;
    const provisionalEndpoint = environment[PROVISIONAL_SPAWN_ENDPOINT_ENV];
    if (provisionalEndpoint) {
      this.#awaitProvisionalSpawn(context, environment, provisionalEndpoint);
      return;
    }
    if (!existsSync(sessionPath)) {
      this.#scheduleSessionBootstrap(context, environment);
      return;
    }
    const sessionId = context.sessionManager.getSessionId();
    if (this.#sessionId === sessionId && this.#sessionPath === sessionPath && this.#controlPlane) {
      return;
    }

    const ownerSessionId = environment[WORKFLOW_OWNER_SESSION_ID_ENV];
    const ownerSessionPath = environment[WORKFLOW_OWNER_SESSION_PATH_ENV];
    const expectedAgentId = environment[WORKFLOW_AGENT_SESSION_ID_ENV];
    // RELEASE adopts the listener prepared before COMMIT; ordinary bootstrap
    // teardown would close it before the transaction-owned Router can fence it.
    const adoptsPreparedRouter = Boolean(this.#preparedRecipientRouter && ownerSessionId && expectedAgentId);
    if (!adoptsPreparedRouter) this.close();
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
        // The child process may reach session_start before the launcher returns
        // from writing its command. Start/transfer the activation here so
        // reconciliation never observes ownership without its recovery state.
        this.#controlPlane.startActivation(ownership);
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
    this.#humanInterruptActorRole = humanInterruptActorRoleFromMembership(
      this.#controlPlane.inspectAgent(this.#controlPlane.currentAgent),
    );
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
    if (this.#provisionalBootstrap) return;
    while (!this.#controlPlane) {
      await new Promise<void>((resolve) => setTimeout(resolve, SESSION_BOOTSTRAP_RETRY_MS));
      this.sessionStarted(context, environment);
    }
  }

  /** A provisional child has no trusted membership role until RELEASE commits. */
  async waitUntilHumanInterruptRoleReady(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined> = process.env,
  ): Promise<"ordinary" | "moderator" | undefined> {
    await this.waitUntilReady(context, environment);
    const provisionalBootstrap = this.#provisionalBootstrap;
    if (provisionalBootstrap) await provisionalBootstrap;
    return this.#controlPlane ? this.#humanInterruptActorRole : undefined;
  }

  close(): void {
    if (this.#directSignals) {
      void Promise.resolve(this.#directSignals.close()).catch((error) => {
        process.emitWarning(`Direct Signal Router cleanup failed: ${(error as Error).message}`);
      });
    }
    this.#directSignals = undefined;
    this.#provisionalBootstrap = undefined;
    if (this.#preparedRecipientRouter) {
      void this.#preparedRecipientRouter.close().catch((error) => {
        process.emitWarning(`Prepared Inbox Router cleanup failed: ${(error as Error).message}`);
      });
    }
    this.#preparedRecipientRouter = undefined;
    this.#preparedRouterCommitted = false;
    this.#provisionalInboxProjector = undefined;
    this.#provisionalRouterStartup = undefined;
    this.#provisionalAgentId = undefined;
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
    this.#humanInterruptActorRole = "ordinary";
  }

  inspect(agentId: string): AgentRecord {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.inspectAgent(controlPlane.agent(agentId));
  }

  inspectTarget(target: InspectionTarget): unknown {
    return this.#requireControlPlane().inspectTarget(target);
  }

  inspectActivation(agentId: string): ActivationRecord | undefined {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.inspectActivation(controlPlane.agent(agentId));
  }

  async startDirectSignalRouter(input: {
    projectInboxBatch(batch: InboxBatch): void;
    hasProjectedMessage?(messageId: string): boolean;
    wakeRecipient?: () => void;
    projectInitialInboxBatch?(batch: InboxBatch): Promise<void>;
    releaseInitialInboxBatch?(): void;
    onTerminalCompletion?(): void;
  }): Promise<void> {
    if (this.#provisionalBootstrap) {
      if (input.projectInitialInboxBatch) this.#provisionalInboxProjector = input.projectInitialInboxBatch;
      this.#provisionalInboxRelease = input.releaseInitialInboxBatch;
      this.#provisionalRouterStartup = input;
      await this.#provisionalBootstrap;
      return this.startDirectSignalRouter(input);
    }
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
    onAccepted: "continue" | "complete";
    prepareEndedRecipient?: Parameters<DirectSignalRuntime["sendMessage"]>[0]["prepareEndedRecipient"];
  }): Promise<QueuedSignalReceipt> {
    return this.#directSignalRuntime().sendMessage(input);
  }

  spawnInitialRequest(input: Omit<import("./workflow-control-plane.ts").SpawnedInitialRequestInput, "capabilities"> & {
    capabilities?: AgentCapabilityConfiguration;
  }) {
    return this.#requireControlPlane().spawnInitialRequest(input);
  }

  reconcileSpawnedInitialRequest(input: {
    sourceEntryId: string;
    agentDefinition: string;
    name: string;
    message: string;
    capabilities?: AgentCapabilityConfiguration;
  }) {
    return this.#requireControlPlane().reconcileSpawnedInitialRequest(input);
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

  reconcilePendingDirectSignals(options: { waitForResolution?: boolean } = {}) {
    return this.#directSignalRuntime().reconcilePendingAcceptances(options);
  }

  completeCurrentActivation(source: CompletionSource) {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Workflow Owner cannot complete");
    }
    const gate = new CompletionGateStore(controlPlane.workflow.databasePath);
    try {
      return gate.complete(this.#requireSelfOwnership(), source, this.#now());
    } finally {
      gate.close();
    }
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
    if (activation.state.kind === "waiting" && activation.state.dependencies.some((dependency) => dependency.kind === "human")) {
      return activation;
    }
    return controlPlane.activateTurn(ownership);
  }

  currentTurnSettled(interrupted: boolean): ActivationRecord | { kind: "owner-turn-settled" } {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId) {
      return controlPlane.settleOwnerTurn();
    }
    const ownership = this.#requireSelfOwnership();
    const completed = this.#inspectRun(ownership);
    if (completed?.state.kind === "ended" && completed.state.outcome === "completed") return completed;
    return interrupted
      ? controlPlane.confirmInterruption(ownership)
      : controlPlane.settleActivation(ownership, undefined, this.#humanInterruptActorRole);
  }

  beginHumanInterrupt(toolCallId: string) {
    return this.#requireControlPlane().beginHumanInterrupt(
      this.#requireSelfOwnership(),
      toolCallId,
      this.#humanInterruptActorRole,
    );
  }

  bindHumanResponse(toolCallId: string, responseInputId: string) {
    return this.#requireControlPlane().bindHumanResponse(
      this.#requireSelfOwnership(),
      toolCallId,
      responseInputId,
    );
  }

  prepareHumanResponseResult(toolCallId: string) {
    return this.#requireControlPlane().prepareHumanResponseResult(this.#requireSelfOwnership(), toolCallId);
  }

  resumeHumanResponseResult(toolCallId: string) {
    return this.#requireControlPlane().resumeHumanResponseResult(this.#requireSelfOwnership(), toolCallId);
  }

  confirmHumanResponseResult(toolCallId: string) {
    return this.#requireControlPlane().confirmHumanResponseResult(this.#requireSelfOwnership(), toolCallId);
  }

  currentHumanInterrupt() {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId
      ? undefined
      : controlPlane.inspectHumanInterrupt(controlPlane.currentAgent);
  }

  hasHumanAttention(): boolean {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId
      && controlPlane.hasHumanAttention(controlPlane.currentAgent);
  }

  pendingUndeclaredNotice() {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId
      ? undefined
      : controlPlane.pendingUndeclaredNotice(controlPlane.currentAgent);
  }

  confirmUndeclaredNotice(episodeId: string): boolean {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.confirmUndeclaredNotice(controlPlane.currentAgent, episodeId);
  }

  queueUndeclaredNotice(episodeId: string) {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.queueUndeclaredNotice(controlPlane.currentAgent, episodeId);
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
        launchPolicy: input.launchPolicy,
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
      environment: buildEnvironment(controlPlane.workflow, ownership!, member),
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
      environment: buildEnvironment(controlPlane.workflow, ownership, member),
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
    const completed = this.#inspectRun(ownership);
    if (completed?.state.kind === "ended" && completed.state.outcome === "completed") return undefined;
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

  wasProtocolCompleted(ownership: AgentRunOwnership): boolean {
    const exact = this.#inspectRun(ownership);
    if (exact?.state.kind === "ended" && exact.state.outcome === "completed") return true;
    if (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId) {
      const activation = this.#controlPlane.inspectActivation(this.#controlPlane.agent(ownership.agentId));
      return activation?.runId === ownership.runId
        && activation.state.kind === "ended"
        && activation.state.outcome === "completed";
    }
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId);
    if (!databasePath) return false;
    const store = new ActivationLifecycleStore(databasePath);
    try {
      const activation = store.inspect(ownership);
      return activation?.runId === ownership.runId
        && activation.state.kind === "ended"
        && activation.state.outcome === "completed";
    } finally {
      store.close();
    }
  }

  #inspectRun(ownership: AgentRunOwnership): ActivationRecord | undefined {
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId)
      ?? (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId ? this.#controlPlane.workflow.databasePath : undefined);
    if (!databasePath) return undefined;
    const store = new ActivationLifecycleStore(databasePath);
    try { return store.inspectRun(ownership); } finally { store.close(); }
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
      preparedRouter: this.#preparedRecipientRouter,
      preparedRouterCommitted: this.#preparedRouterCommitted,
    });
    this.#preparedRecipientRouter = undefined;
    this.#preparedRouterCommitted = false;
    return this.#directSignals;
  }

  #awaitProvisionalSpawn(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined>,
    endpoint: string,
  ): void {
    if (this.#provisionalBootstrap) return;
    const ownerAgentId = environment[WORKFLOW_OWNER_SESSION_ID_ENV]!;
    const ownerSessionPath = environment[WORKFLOW_OWNER_SESSION_PATH_ENV]!;
    const agentId = environment[WORKFLOW_AGENT_SESSION_ID_ENV]!;
    const workflow = createWorkflowLayout({ ownerSessionId: ownerAgentId, ownerSessionPath, createdAtMs: this.#now() });
    this.#provisionalBootstrap = (async () => {
      const router = new RecipientInboxRouter({
        workflowOwnerId: ownerAgentId,
        recipient: { workflowOwnerId: ownerAgentId, agentId },
        databasePath: workflow.databasePath,
        projectInboxBatch() {},
        now: this.#now,
      });
      await router.prepare();
      this.#preparedRecipientRouter = router;
      this.#provisionalAgentId = agentId;
      const isPreparedResume = environment[PROVISIONAL_AGENT_RUN_KIND_ENV] === "resume";
      if (!isPreparedResume) await this.#waitForProvisionalInboxProjector();
      await awaitProvisionalSpawnCommit(endpoint, { routerEndpoint: router.endpoint }, {
        ...(isPreparedResume ? {} : { project: async (plan: ProvisionalSpawnProjection) => this.#projectSpawnedInitialInboxBatch(plan) }),
        release: async (released) => this.#adoptProvisionalRouter(context, environment, released),
      });
    })().catch(async (error) => {
      let ownership: AgentRunOwnership | undefined;
      try {
        ownership = this.#reconcilePreparedRouterAfterProvisionalFailure(ownerAgentId, workflow.databasePath, agentId);
      } catch (reconciliationError) {
        // A partial/conflicting footprint is durable evidence. Keep the
        // prepared Router fenced rather than falsely treating it as rollback.
        process.emitWarning(`Provisional Spawn reconciliation failed closed: ${(reconciliationError as Error).message}`);
        return;
      }
      if (ownership) {
        try {
          await this.#adoptProvisionalRouter(context, environment, { runId: ownership.runId, fencingEpoch: ownership.epoch });
        } catch (adoptionError) {
          process.emitWarning(`Committed Provisional Spawn adoption failed closed: ${(adoptionError as Error).message}`);
        }
        return;
      }
      void this.#preparedRecipientRouter?.close().catch(() => undefined);
      this.#preparedRecipientRouter = undefined;
      process.emitWarning(`Provisional Spawn startup failed: ${(error as Error).message}`);
    }).finally(() => { this.#provisionalBootstrap = undefined; });
  }

  #reconcilePreparedRouterAfterProvisionalFailure(
    workflowOwnerId: string,
    databasePath: string,
    agentId: string,
  ): AgentRunOwnership | undefined {
    const router = this.#preparedRecipientRouter;
    if (!router) return undefined;
    const store = new DirectSignalStore(databasePath);
    try {
      return store.reconcilePreparedRecipientRouter({
        recipient: { workflowOwnerId, agentId }, endpoint: router.endpoint,
      });
    } finally {
      store.close();
    }
  }

  async #projectSpawnedInitialInboxBatch(plan: ProvisionalSpawnProjection): Promise<void> {
    if (!this.#provisionalInboxProjector) {
      throw new Error("Provisional Spawn received its Inbox Batch before Pi installed a projector");
    }
    if (plan.recipientAgentId !== this.#provisionalAgentId) {
      throw new WorkflowProtocolError("InvalidMessageSource", "Provisional Spawn projection is addressed to another Agent");
    }
    const message = resolveCanonicalSpawnedInitialMessage({
      sessionPath: plan.senderSessionPath,
      sourceEntryId: plan.sourceEntryId,
      agentDefinition: plan.agentDefinition,
      name: plan.agentName,
      payloadDigest: plan.payloadDigest,
    });
    await this.#provisionalInboxProjector({
      deliveryTiming: "steer",
      messages: [{
        kind: "request",
        messageId: plan.messageId,
        senderAgentId: plan.senderAgentId,
        recipientAgentId: plan.recipientAgentId,
        deliveryTiming: "steer",
        message,
        responseRequired: true,
      }],
    });
  }

  async #waitForProvisionalInboxProjector(): Promise<void> {
    while (!this.#provisionalInboxProjector) {
      await new Promise<void>((resolve) => setTimeout(resolve, SESSION_BOOTSTRAP_RETRY_MS));
    }
  }

  async #adoptProvisionalRouter(
    context: WorkflowBootstrapContext,
    environment: NodeJS.ProcessEnv | Record<string, string | undefined>,
    commit: { runId: string; fencingEpoch: number },
  ): Promise<void> {
    this.#preparedRouterCommitted = true;
    this.sessionStarted(context, {
      ...environment,
      [PROVISIONAL_SPAWN_ENDPOINT_ENV]: undefined,
      [WORKFLOW_RUN_ID_ENV]: commit.runId,
      [WORKFLOW_FENCING_EPOCH_ENV]: String(commit.fencingEpoch),
      [WORKFLOW_ACTIVATION_ID_ENV]: commit.runId,
    });
    const startup = this.#provisionalRouterStartup;
    if (!startup) throw new Error("Provisional Spawn has no Router adoption configuration");
    const runtime = this.#directSignalRuntime();
    runtime.configureInboxDelivery(startup);
    await runtime.start();
    this.#provisionalInboxRelease?.();
    this.#provisionalInboxRelease = undefined;
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
  member: AgentRecord,
): Record<string, string> {
  return {
    [WORKFLOW_OWNER_SESSION_ID_ENV]: workflow.ownerAgentId,
    [WORKFLOW_OWNER_SESSION_PATH_ENV]: workflow.ownerSessionPath,
    [WORKFLOW_AGENT_SESSION_ID_ENV]: ownership.agentId,
    [WORKFLOW_RUN_ID_ENV]: ownership.runId,
    [WORKFLOW_FENCING_EPOCH_ENV]: String(ownership.epoch),
    [WORKFLOW_ACTIVATION_ID_ENV]: ownership.runId,
    [WORKFLOW_AGENT_ROLE_ENV]: humanInterruptActorRoleFromMembership(member),
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

export function humanInterruptActorRoleFromMembership(
  member: Pick<AgentRecord, "agentDefinition">,
): "ordinary" | "moderator" {
  return member.agentDefinition === "moderator" ? "moderator" : "ordinary";
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
