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
  ActivationRecoveryStore,
  type RecoveryPaneIntent,
} from "./activation-recovery.ts";
import { notifyWorkflowOwnerOfAutomaticRecovery } from "./activation-recovery-notification.ts";
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
import {
  ActivationCancellationService,
  ActivationCancellationInspectionStore,
  type AgentRunTerminator,
} from "./activation-cancellation.ts";
import type {
  OperationReviewPolicy,
} from "./operation-review.ts";
import {
  assertOperationReconciliationConfigured,
  reconcileKnownOperation,
  type ExtensionOperationReconciler,
} from "./operation-reconciliation.ts";

export const WORKFLOW_OWNER_SESSION_ID_ENV = "PI_WORKFLOW_OWNER_SESSION_ID";
export const WORKFLOW_OWNER_SESSION_PATH_ENV = "PI_WORKFLOW_OWNER_SESSION_PATH";
export const WORKFLOW_AGENT_SESSION_ID_ENV = "PI_WORKFLOW_AGENT_SESSION_ID";
export const WORKFLOW_RUN_ID_ENV = "PI_WORKFLOW_RUN_ID";
export const WORKFLOW_FENCING_EPOCH_ENV = "PI_WORKFLOW_FENCING_EPOCH";
export const WORKFLOW_ACTIVATION_ID_ENV = "PI_WORKFLOW_ACTIVATION_ID";
export const WORKFLOW_AGENT_ROLE_ENV = "PI_WORKFLOW_AGENT_ROLE";
const SESSION_BOOTSTRAP_RETRY_MS = 25;
const AUTOMATIC_RECOVERY_RECONCILIATION_RETRY_MS = 25;
const AUTOMATIC_RECOVERY_RECONCILIATION_MAX_ATTEMPTS = 8;
const OPERATION_REVIEW_DISCOVERY_INTERVAL_MS = 1_000;

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

/** A durable claim plus the exact fenced run that will fulfill it. */
export interface PreparedAutomaticRecoveryRun extends PreparedAgentRun {
  failedActivationId: string;
  member: AgentRecord;
}

export interface AutomaticRecoveryRunReconciliation {
  kind: "live" | "unknown" | "pending" | "exhausted";
  failedActivationId: string;
  ownership: AgentRunOwnership;
  member: AgentRecord;
  locator?: AgentRunLocator;
  detail?: string;
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

export interface RecoveryPaneLocator {
  discover(locator: RecoveryPaneDiscoveryLocator): Promise<RecoveryPaneDiscovery>;
}

export interface AutomaticRecoveryPaneReconciliation {
  kind: "present" | "missing" | "cleaned" | "promoted" | "unknown";
  intent: RecoveryPaneIntent;
  surface?: string;
  detail?: string;
}

/** Durable phases around the external recovery command submission. */
export type AutomaticRecoveryDispatchPhase = "prepared" | "dispatching" | "dispatched";

interface AutomaticRecoveryCheckpoint extends AgentRunLocator {
  kind: "automatic-recovery";
  runId: string;
  fencingEpoch: number;
  phase: AutomaticRecoveryDispatchPhase;
}

type RecoveryLaunchReconciliation = {
  kind: "requeued" | "raced" | "unknown";
  detail?: string;
};

export class WorkflowBootstrap {
  readonly #now: () => number;
  readonly #confirmRunTerminated: ((locator: AgentRunLocator) => Promise<boolean>) | undefined;
  readonly #agentRunTerminator: AgentRunTerminator;
  readonly #recoveryPaneLocator: RecoveryPaneLocator | undefined;
  readonly #extensionOperationReconciler: ExtensionOperationReconciler | undefined;
  readonly #operationReviewPolicy: OperationReviewPolicy | undefined;
  #controlPlane: WorkflowControlPlane | undefined;
  #sessionId: string | undefined;
  #sessionPath: string | undefined;
  #selfOwnership: AgentRunOwnership | undefined;
  #recoveryActivationNotNeeded = false;
  #humanInterruptActorRole: "ordinary" | "moderator" = "ordinary";
  #sessionBootstrapTimer: ReturnType<typeof setTimeout> | undefined;
  #automaticRecoveryReconciliationTimer: ReturnType<typeof setTimeout> | undefined;
  #automaticRecoveryReconciliationPromise: Promise<void> | undefined;
  #automaticRecoveryReconciliationGeneration = 0;
  #automaticRecoveryReconciliationAttempts = 0;
  #operationReviewTimer: ReturnType<typeof setTimeout> | undefined;
  #operationReviewPromise: Promise<void> | undefined;
  #operationReviewGeneration = 0;
  #automaticRecoveryRequested: (() => void | Promise<void>) | undefined;
  #automaticRecoveryReconciled: ((results: AutomaticRecoveryRunReconciliation[]) => void | Promise<void>) | undefined;
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
    agentRunTerminator?: AgentRunTerminator;
    recoveryPaneLocator?: RecoveryPaneLocator;
    extensionOperationReconciler?: ExtensionOperationReconciler;
    operationReviewPolicy?: OperationReviewPolicy;
  } = {}) {
    this.#now = options.now ?? Date.now;
    this.#confirmRunTerminated = options.confirmRunTerminated;
    this.#agentRunTerminator = options.agentRunTerminator ?? {
      async inspect() { return { kind: "unavailable", error: "Agent Run terminator is unavailable" }; },
      async close() { throw new Error("Agent Run terminator is unavailable"); },
    };
    this.#recoveryPaneLocator = options.recoveryPaneLocator;
    this.#extensionOperationReconciler = options.extensionOperationReconciler;
    this.#operationReviewPolicy = options.operationReviewPolicy;
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

  /** True when this recovery pane found its claimed work cancelled before start. */
  get recoveryActivationNotNeeded(): boolean {
    return this.#recoveryActivationNotNeeded;
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
    this.#recoveryActivationNotNeeded = false;
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
        // The child process may reach session_start before the launcher returns
        // from writing its command. Start/transfer the activation here so
        // reconciliation never observes ownership without its recovery state.
        const activationStart = this.#controlPlane.startRecoveryActivation(ownership);
        if ("kind" in activationStart && activationStart.kind === "not-needed") {
          this.#recoveryActivationNotNeeded = true;
        } else {
          this.#selfOwnership = ownership;
        }
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
          operationReviewPolicy: this.#operationReviewPolicy,
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
    this.#stopAutomaticRecoveryReconciliation();
    this.#stopOperationReviewReconciliation();
    if (this.#directSignals) {
      const preserveRouterRegistration = Boolean(
        this.#selfOwnership && this.isCancellationOwnedRun(this.#selfOwnership),
      );
      void Promise.resolve(this.#directSignals.close({ preserveRouterRegistration })).catch((error) => {
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
      if (activation?.runId === this.#selfOwnership.runId
        && activation.state.kind !== "ended"
        && !this.isCancellationOwnedRun(this.#selfOwnership)) {
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
    this.#recoveryActivationNotNeeded = false;
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
    onAutomaticRecoveryRequested?: () => void | Promise<void>;
    onAutomaticRecoveryReconciled?: (results: AutomaticRecoveryRunReconciliation[]) => void | Promise<void>;
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
    this.#assertOperationReviewConfiguration();
    const runtime = this.#directSignalRuntime();
    runtime.configureInboxDelivery(input);
    await runtime.start();
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId
      && controlPlane.claimableRecoveryEpisodes().length > 0) {
      await input.onAutomaticRecoveryRequested?.();
    }
    this.#scheduleAutomaticRecoveryReconciliation(
      input.onAutomaticRecoveryRequested,
      input.onAutomaticRecoveryReconciled,
    );
    await this.#restartOperationReviewReconciliation();
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

  cancelRequest(requestId: string) {
    return this.#directSignalRuntime().cancelRequest(requestId);
  }

  async cancelActivation(agentId: string, sourceId: string) {
    const controlPlane = this.#requireControlPlane();
    const service = new ActivationCancellationService({
      databasePath: controlPlane.workflow.databasePath,
      actor: controlPlane.currentAgent,
      terminator: this.#agentRunTerminator,
      now: this.#now,
      allocateOperationId: randomUUID,
    });
    try {
      return await service.cancel({ target: controlPlane.agent(agentId), sourceId });
    } finally {
      service.close();
    }
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
    this.#stopAutomaticRecoveryReconciliation();
    this.#stopOperationReviewReconciliation();
    const runtime = this.#directSignals;
    this.#directSignals = undefined;
    const preserveRouterRegistration = Boolean(
      this.#selfOwnership && this.isCancellationOwnedRun(this.#selfOwnership),
    );
    if (runtime) await runtime.close({ preserveRouterRegistration });
  }

  reconcilePendingDirectSignals(options: { waitForResolution?: boolean } = {}) {
    return this.#directSignalRuntime().reconcilePendingAcceptances(options);
  }

  listOperationIncidentTriggers() {
    return this.#requireControlPlane().listOperationIncidentTriggers();
  }

  listWorkflowAttention() {
    return this.#requireControlPlane().listWorkflowAttention();
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

  runStarted(ownership: AgentRunOwnership) {
    return this.#requireControlPlane().startRecoveryActivation(ownership);
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

  humanInterruptByToolCall(toolCallId: string) {
    const controlPlane = this.#requireControlPlane();
    return controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId
      ? undefined
      : controlPlane.inspectHumanInterruptToolCall(controlPlane.currentAgent, toolCallId);
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

  /**
   * Persist the exact workspace/label identity before asking Herdr to create a
   * recovery pane. The returned intent is stable across Owner restarts; an
   * existing intent always wins over a newly proposed run id.
   */
  prepareAutomaticRecoveryPane(input: {
    failedActivationId: string;
    runId: string;
    workspaceId: string;
    label: string;
    cwd: string;
    intentId?: string;
  }): RecoveryPaneIntent | undefined {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return undefined;
    return controlPlane.prepareRecoveryPaneIntent({
      failedActivationId: input.failedActivationId,
      intentId: input.intentId ?? randomUUID(),
      runId: input.runId,
      workspaceId: input.workspaceId,
      label: input.label,
      cwd: input.cwd,
    });
  }

  beginAutomaticRecoveryPaneCreation(intent: Pick<RecoveryPaneIntent, "intentId" | "runId">): RecoveryPaneIntent {
    return this.#requireControlPlane().beginRecoveryPaneCreation(intent.intentId, intent.runId);
  }

  recordAutomaticRecoveryPaneCreated(
    intent: Pick<RecoveryPaneIntent, "intentId" | "runId">,
    surface: string,
  ): RecoveryPaneIntent {
    return this.#requireControlPlane().recordRecoveryPaneCreated(intent.intentId, intent.runId, surface);
  }

  inspectAutomaticRecoveryPaneIntent(intentId: string): RecoveryPaneIntent | undefined {
    return this.#requireControlPlane().inspectRecoveryPaneIntent(intentId);
  }

  beginRecoveryPaneCleanup(intentId: string, detail: string): RecoveryPaneIntent | undefined {
    return this.#requireControlPlane().beginRecoveryPaneCleanup(intentId, detail);
  }

  completeRecoveryPaneCleanup(intentId: string, expectedSurface?: string): boolean {
    return this.#requireControlPlane().completeRecoveryPaneCleanup(intentId, expectedSurface);
  }

  promoteAutomaticRecoveryPane(input: {
    failedActivationId: string;
    intentId: string;
    runId: string;
    surface: string;
  }): PreparedAutomaticRecoveryRun | undefined {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return undefined;
    const claim = controlPlane.promoteRecoveryPaneIntent(input);
    if (!claim) return undefined;
    const member = controlPlane.inspectAgent(controlPlane.agent(claim.ownership.agentId));
    return {
      ownership: claim.ownership,
      environment: buildEnvironment(controlPlane.workflow, claim.ownership, member),
      sessionPath: member.sessionPath,
      failedActivationId: input.failedActivationId,
      member,
    };
  }

  /**
   * Reconcile provisional pane records before claimable recovery launch. A
   * missing pane is safe to forget; a present pane is acknowledged and reused;
   * ambiguity/unavailable inspection keeps the intent fenced.
   */
  async reconcileAutomaticRecoveryPaneIntents(): Promise<AutomaticRecoveryPaneReconciliation[]> {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return [];
    const intents = controlPlane.recoveryPaneIntents();
    const results: AutomaticRecoveryPaneReconciliation[] = [];
    for (const intent of intents) {
      const recovery = controlPlane.inspectActivationRecovery(controlPlane.agent(intent.agentId));
      if ((recovery?.state === "launching" || recovery?.state === "active")
        && recovery.replacementRunId
        && recovery.replacementRunId !== intent.runId) {
        results.push({
          kind: "unknown",
          intent,
          detail: "Recovery pane intent lost its exact Agent Run claim",
        });
        continue;
      }
      if (intent.state === "promoted") {
        // A settled replacement owns its normal pane lifecycle; retire only
        // this preparatory row and never close that live child surface.
        if (recovery?.replacementActivationId && recovery.state === "resolved") {
          controlPlane.retireRecoveryPaneIntent(intent.intentId);
          results.push({ kind: "cleaned", intent });
          continue;
        }
        if (recovery?.state === "launching" || recovery?.state === "active") continue;
      }

      const cleanupRequired = !recovery
        || (recovery.state !== "pending" && recovery.state !== "launching")
        || intent.state === "cleanup-pending";
      const discovery = this.#recoveryPaneLocator
        ? await this.#discoverRecoveryPane(intent)
        : { kind: "unavailable" as const, error: "Recovery pane discovery is unavailable" };
      if (discovery.kind === "unavailable" || discovery.kind === "ambiguous") {
        results.push({ kind: "unknown", intent, detail: discovery.error });
        continue;
      }
      if (discovery.kind === "missing") {
        try {
          if (intent.state === "promoted") {
            controlPlane.retireRecoveryPaneIntent(intent.intentId);
          } else if (cleanupRequired || intent.state !== "promoted") {
            controlPlane.beginRecoveryPaneCleanup(intent.intentId, "Confirmed exact recovery pane absence");
            controlPlane.completeRecoveryPaneCleanup(intent.intentId, intent.surface);
          }
        } catch (error) {
          results.push({ kind: "unknown", intent, detail: (error as Error).message });
          continue;
        }
        results.push({ kind: "missing", intent });
        continue;
      }

      if (!cleanupRequired && intent.state !== "promoted") {
        try {
          const acknowledged = controlPlane.recordRecoveryPaneCreated(
            intent.intentId,
            intent.runId,
            discovery.surface,
          );
          results.push({ kind: "present", intent: acknowledged, surface: discovery.surface });
        } catch (error) {
          results.push({ kind: "unknown", intent, surface: discovery.surface, detail: (error as Error).message });
        }
        continue;
      }

      // Stale unpromoted records are closed only through the exact identity
      // returned by the durable label discovery. Failed close/absence leaves
      // cleanup-pending for a later Owner rather than guessing.
      if (intent.state !== "promoted") {
        try {
          const cleanup = controlPlane.beginRecoveryPaneCleanup(intent.intentId, "Cleaning stale recovery pane intent");
          if (!cleanup || cleanup.state === "promoted") {
            results.push({ kind: "unknown", intent, detail: "Recovery pane intent changed during stale cleanup" });
            continue;
          }
          if (cleanup.surface && cleanup.surface !== discovery.surface) {
            results.push({ kind: "unknown", intent, surface: discovery.surface, detail: "Recovery pane surface changed during stale cleanup" });
            continue;
          }
          await this.#agentRunTerminator.close({ surface: discovery.surface });
          const afterClose = await this.#agentRunTerminator.inspect({ surface: discovery.surface });
          if (afterClose.kind !== "missing") {
            results.push({
              kind: "unknown",
              intent,
              surface: discovery.surface,
              detail: afterClose.kind === "unavailable"
                ? afterClose.error ?? "Recovery pane absence is unavailable after close"
                : "Recovery pane termination is unconfirmed",
            });
            continue;
          }
          controlPlane.completeRecoveryPaneCleanup(intent.intentId, discovery.surface);
          results.push({ kind: "cleaned", intent, surface: discovery.surface });
        } catch (error) {
          results.push({ kind: "unknown", intent, surface: discovery.surface, detail: (error as Error).message });
        }
      } else {
        try {
          await this.#agentRunTerminator.close({ surface: discovery.surface });
          const afterClose = await this.#agentRunTerminator.inspect({ surface: discovery.surface });
          if (afterClose.kind !== "missing") {
            results.push({
              kind: "unknown",
              intent,
              surface: discovery.surface,
              detail: afterClose.kind === "unavailable"
                ? afterClose.error ?? "Recovery pane liveness is unavailable after close"
                : "Recovery pane termination is unconfirmed",
            });
            continue;
          }
          controlPlane.retireRecoveryPaneIntent(intent.intentId);
          results.push({ kind: "cleaned", intent, surface: discovery.surface });
        } catch (error) {
          results.push({ kind: "unknown", intent, surface: discovery.surface, detail: (error as Error).message });
        }
      }
    }
    return results;
  }

  /**
   * Reserve one recovery episode before preparing its replacement. The Owner
   * holds the durable claim while the external pane is being started, so a
   * manual resume cannot race a second automatic replacement into existence.
   */
  async prepareAutomaticRecovery(
    input: PrepareResumeInput & { failedActivationId: string },
  ): Promise<PreparedAutomaticRecoveryRun | undefined> {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return undefined;
    const requestedSessionPath = realpathSync(input.sessionPath);
    const agentId = readPiSessionUuid(requestedSessionPath);
    const member = controlPlane.inspectAgent(controlPlane.agent(agentId));
    if (member.sessionPath !== requestedSessionPath) {
      throw new WorkflowProtocolError(
        "InvalidSessionIdentity",
        `Resume transcript does not match durable Agent ${agentId}: ${requestedSessionPath}`,
      );
    }
    // The pane was created by the launcher before this call. The claim method
    // commits its exact locator with ownership, so restart never observes an
    // automatic recovery owner without a checkpoint to reconcile.
    const claim = controlPlane.claimRecoveryRun(input.failedActivationId, input.runId, input.surface);
    if (!claim) return undefined;
    return {
      ownership: claim.ownership,
      environment: buildEnvironment(controlPlane.workflow, claim.ownership, member),
      sessionPath: member.sessionPath,
      failedActivationId: input.failedActivationId,
      member,
    };
  }

  /** Persist dispatch intent before handing the command to the external pane. */
  beginAutomaticRecoveryDispatch(ownership: AgentRunOwnership): void {
    this.#transitionAutomaticRecoveryDispatch(ownership, "prepared", "dispatching");
  }

  /** Persist command-submission evidence after the pane accepts the command. */
  confirmAutomaticRecoveryDispatch(ownership: AgentRunOwnership): void {
    this.#transitionAutomaticRecoveryDispatch(ownership, "dispatching", "dispatched");
  }

  abandonAutomaticRecovery(input: { failedActivationId: string; ownership: AgentRunOwnership; detail: string }): void {
    this.#requireControlPlane().abandonRecoveryEpisodeLaunch(
      input.failedActivationId,
      input.ownership,
      input.detail,
    );
  }

  claimableAutomaticRecoveries() {
    return this.#requireControlPlane().claimableRecoveryEpisodes();
  }

  /**
   * Reconcile exact external replacement runs before the Owner may launch
   * another pane. Unknown liveness preserves the ownership fence; exact
   * pre-bootstrap termination can safely return the same claim to pending.
   */
  async reconcileAutomaticRecoveryRuns(): Promise<AutomaticRecoveryRunReconciliation[]> {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return [];
    const reconciled: AutomaticRecoveryRunReconciliation[] = [];
    for (const recovery of controlPlane.inFlightRecoveryEpisodes()) {
      if (!recovery.replacementRunId || recovery.replacementFencingEpoch === undefined) continue;
      const member = controlPlane.inspectAgent(controlPlane.agent(recovery.agentId));
      const ownership = controlPlane.currentAgentRun(member);
      if (!ownership || ownership.runId !== recovery.replacementRunId
        || ownership.epoch !== recovery.replacementFencingEpoch) {
        reconciled.push({
          kind: "unknown",
          failedActivationId: recovery.failedActivationId,
          ownership: {
            workflowOwnerId: controlPlane.workflow.ownerAgentId,
            agentId: recovery.agentId,
            runId: recovery.replacementRunId,
            resourceId: `agent-run:${controlPlane.workflow.ownerAgentId}:${recovery.agentId}`,
            epoch: recovery.replacementFencingEpoch,
          },
          member,
          detail: "Exact recovery ownership is unavailable",
        });
        continue;
      }
      const checkpoint = controlPlane.readAgentRunCheckpoint(member);
      const recoveryCheckpoint = checkpoint?.fencingEpoch === ownership.epoch
        ? parseAutomaticRecoveryCheckpoint(checkpoint.value)
        : undefined;
      const exactCheckpoint = recoveryCheckpoint
        && recoveryCheckpoint.runId === ownership.runId
        && recoveryCheckpoint.fencingEpoch === ownership.epoch
        ? recoveryCheckpoint
        : undefined;
      const locator = exactCheckpoint ? { surface: exactCheckpoint.surface } : undefined;
      if (!exactCheckpoint || !locator) {
        reconciled.push({
          kind: "unknown",
          failedActivationId: recovery.failedActivationId,
          ownership,
          member,
          detail: "Exact durable Agent Run locator is unavailable",
        });
        continue;
      }
      let inspection: Awaited<ReturnType<AgentRunTerminator["inspect"]>>;
      try {
        inspection = await this.#agentRunTerminator.inspect(locator);
      } catch (error) {
        inspection = { kind: "unavailable", error: (error as Error).message };
      }

      // Before child bootstrap, both prepared and dispatching panes are safe
      // to reconcile only after exact termination. For dispatching, closing
      // and confirming absence fences a command that may already have been
      // submitted; a missing pane is already that exact absence evidence.
      const preBootstrap = recovery.state === "launching"
        && (exactCheckpoint.phase === "prepared" || exactCheckpoint.phase === "dispatching");
      if (preBootstrap) {
        const cleanup = await this.#reconcileAutomaticRecoveryLaunch({
          failedActivationId: recovery.failedActivationId,
          ownership,
          checkpoint: checkpoint.value,
          locator,
          initialInspection: inspection,
          detail: exactCheckpoint.phase === "prepared"
            ? "Confirmed prepared pane closed before recovery command dispatch"
            : "Confirmed dispatching pane closed before replacement child bootstrap",
        });
        if (cleanup.kind === "requeued") {
          reconciled.push({ kind: "pending", failedActivationId: recovery.failedActivationId, ownership, member, locator });
        } else {
          // Child bootstrap can race the close/absence confirmation. Keep the
          // exact ownership fence rather than allowing a duplicate replacement.
          reconciled.push({
            kind: "unknown",
            failedActivationId: recovery.failedActivationId,
            ownership,
            member,
            locator,
            detail: cleanup.detail,
          });
        }
        continue;
      }

      if (inspection.kind === "present") {
        reconciled.push({ kind: "live", failedActivationId: recovery.failedActivationId, ownership, member, locator });
        continue;
      }
      if (inspection.kind === "unavailable") {
        reconciled.push({
          kind: "unknown",
          failedActivationId: recovery.failedActivationId,
          ownership,
          member,
          locator,
          detail: inspection.error ?? "Agent Run liveness is unavailable",
        });
        continue;
      }
      if (recovery.state === "launching") {
        const cleanup = await this.#reconcileAutomaticRecoveryLaunch({
          failedActivationId: recovery.failedActivationId,
          ownership,
          checkpoint: checkpoint.value,
          locator,
          initialInspection: inspection,
          detail: "Confirmed absent before replacement child bootstrap",
        });
        if (cleanup.kind === "requeued") {
          reconciled.push({ kind: "pending", failedActivationId: recovery.failedActivationId, ownership, member, locator });
        } else {
          // Child bootstrap can race the liveness observation. Preserve the
          // exact fence rather than converting that race into a duplicate.
          reconciled.push({
            kind: "unknown",
            failedActivationId: recovery.failedActivationId,
            ownership,
            member,
            locator,
            detail: cleanup.detail,
          });
        }
        continue;
      }
      try {
        controlPlane.failAgentRun(ownership, {
          error: "Automatic recovery replacement was confirmed absent during Owner reconciliation",
        });
        reconciled.push({ kind: "exhausted", failedActivationId: recovery.failedActivationId, ownership, member, locator });
      } catch (error) {
        reconciled.push({
          kind: "unknown",
          failedActivationId: recovery.failedActivationId,
          ownership,
          member,
          locator,
          detail: (error as Error).message,
        });
      }
    }
    return reconciled;
  }

  /**
   * Retry bounded liveness reconciliation only while this Owner's router is
   * live. Unknown observations retain their fence; a later exact absence can
   * safely requeue the same episode without inferring failure from elapsed time.
   */
  #scheduleAutomaticRecoveryReconciliation(
    onAutomaticRecoveryRequested?: () => void | Promise<void>,
    onAutomaticRecoveryReconciled?: (results: AutomaticRecoveryRunReconciliation[]) => void | Promise<void>,
  ): void {
    const controlPlane = this.#controlPlane;
    if (!controlPlane || controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    this.#automaticRecoveryRequested = onAutomaticRecoveryRequested;
    this.#automaticRecoveryReconciled = onAutomaticRecoveryReconciled;
    if (this.#automaticRecoveryReconciliationTimer || this.#automaticRecoveryReconciliationPromise) return;
    this.#automaticRecoveryReconciliationAttempts = 0;
    const generation = ++this.#automaticRecoveryReconciliationGeneration;
    this.#automaticRecoveryReconciliationPromise = this.#runAutomaticRecoveryReconciliation(generation)
      .catch((error) => {
        process.emitWarning(
          `Initial automatic recovery reconciliation failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      })
      .finally(() => {
        if (this.#automaticRecoveryReconciliationGeneration === generation) {
          this.#automaticRecoveryReconciliationPromise = undefined;
        }
      });
  }

  async #runAutomaticRecoveryReconciliation(generation: number): Promise<void> {
    if (generation !== this.#automaticRecoveryReconciliationGeneration) return;
    const results = await this.reconcileAutomaticRecoveryRuns();
    if (generation !== this.#automaticRecoveryReconciliationGeneration) return;
    try {
      await this.#automaticRecoveryReconciled?.(results);
    } catch {
      // Watcher registration is observability; it must not stop durable
      // liveness reconciliation or turn an unknown pane into a launch.
    }
    if (generation !== this.#automaticRecoveryReconciliationGeneration) return;
    if (results.some((result) => result.kind === "pending")) {
      await this.#automaticRecoveryRequested?.();
    }
    if (generation !== this.#automaticRecoveryReconciliationGeneration) return;
    const controlPlane = this.#controlPlane;
    if (!controlPlane || controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    if (controlPlane.inFlightRecoveryEpisodes().length === 0) {
      this.#automaticRecoveryReconciliationAttempts = 0;
      return;
    }
    if (this.#automaticRecoveryReconciliationAttempts >= AUTOMATIC_RECOVERY_RECONCILIATION_MAX_ATTEMPTS) {
      return;
    }
    this.#automaticRecoveryReconciliationAttempts += 1;
    this.#automaticRecoveryReconciliationTimer = setTimeout(() => {
      this.#automaticRecoveryReconciliationTimer = undefined;
      if (generation !== this.#automaticRecoveryReconciliationGeneration) return;
      this.#automaticRecoveryReconciliationPromise = this.#runAutomaticRecoveryReconciliation(generation)
        .catch((error) => {
          process.emitWarning(
            `Scheduled automatic recovery reconciliation failed: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        })
        .finally(() => {
          if (this.#automaticRecoveryReconciliationGeneration === generation) {
            this.#automaticRecoveryReconciliationPromise = undefined;
          }
        });
    }, AUTOMATIC_RECOVERY_RECONCILIATION_RETRY_MS);
    this.#automaticRecoveryReconciliationTimer.unref?.();
  }

  #stopAutomaticRecoveryReconciliation(): void {
    this.#automaticRecoveryReconciliationGeneration += 1;
    if (this.#automaticRecoveryReconciliationTimer) {
      clearTimeout(this.#automaticRecoveryReconciliationTimer);
      this.#automaticRecoveryReconciliationTimer = undefined;
    }
    this.#automaticRecoveryReconciliationPromise = undefined;
    this.#automaticRecoveryReconciliationAttempts = 0;
    this.#automaticRecoveryRequested = undefined;
    this.#automaticRecoveryReconciled = undefined;
  }

  async #restartOperationReviewReconciliation(): Promise<void> {
    this.#stopOperationReviewReconciliation();
    const controlPlane = this.#controlPlane;
    if (!controlPlane || controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    const generation = ++this.#operationReviewGeneration;
    const reconciliation = this.#runOperationReviewReconciliation(generation);
    this.#operationReviewPromise = reconciliation;
    try {
      await reconciliation;
    } finally {
      if (this.#operationReviewGeneration === generation) {
        this.#operationReviewPromise = undefined;
      }
    }
  }

  async #runOperationReviewReconciliation(generation: number): Promise<void> {
    if (generation !== this.#operationReviewGeneration) return;
    const controlPlane = this.#controlPlane;
    if (!controlPlane || controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    await controlPlane.reconcileOperationReviews(
      (review) => reconcileKnownOperation({
        databasePath: controlPlane.workflow.databasePath,
        workflowOwnerId: controlPlane.workflow.ownerAgentId,
        agentRunTerminator: this.#agentRunTerminator,
        now: this.#now,
      }, review, this.#extensionOperationReconciler),
      { dueOnly: true },
    );
    if (generation !== this.#operationReviewGeneration) return;
    const deadlines = controlPlane.listWorkflowAttention()
      .map((attention) => attention.reviewDeadlineAtMs);
    const untilNextDeadline = deadlines.length > 0
      ? Math.max(0, Math.min(...deadlines) - this.#now())
      : OPERATION_REVIEW_DISCOVERY_INTERVAL_MS;
    const delay = Math.min(
      OPERATION_REVIEW_DISCOVERY_INTERVAL_MS,
      untilNextDeadline,
    );
    this.#scheduleOperationReviewReconciliation(generation, delay);
  }

  #scheduleOperationReviewReconciliation(generation: number, delay: number): void {
    if (generation !== this.#operationReviewGeneration || this.#operationReviewTimer) return;
    const controlPlane = this.#controlPlane;
    if (!controlPlane || controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    this.#operationReviewTimer = setTimeout(() => {
      this.#operationReviewTimer = undefined;
      if (generation !== this.#operationReviewGeneration) return;
      const reconciliation = this.#runOperationReviewReconciliation(generation)
        .catch((error) => {
          process.emitWarning(
            `Scheduled Operation Review reconciliation failed: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
          this.#scheduleOperationReviewReconciliation(
            generation,
            OPERATION_REVIEW_DISCOVERY_INTERVAL_MS,
          );
        })
        .finally(() => {
          if (
            this.#operationReviewGeneration === generation
            && this.#operationReviewPromise === reconciliation
          ) {
            this.#operationReviewPromise = undefined;
          }
        });
      this.#operationReviewPromise = reconciliation;
    }, delay);
    this.#operationReviewTimer.unref?.();
  }

  #stopOperationReviewReconciliation(): void {
    this.#operationReviewGeneration += 1;
    if (this.#operationReviewTimer) {
      clearTimeout(this.#operationReviewTimer);
      this.#operationReviewTimer = undefined;
    }
    this.#operationReviewPromise = undefined;
  }

  #assertOperationReviewConfiguration(): void {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId !== controlPlane.workflow.ownerAgentId) return;
    const unresolvedReviews = controlPlane
      .listWorkflow(controlPlane.owner)
      .flatMap((agent) => controlPlane.listOperationReviews(controlPlane.agent(agent.agentId)))
      .filter((review) => review.status !== "resolved");
    assertOperationReconciliationConfigured(
      unresolvedReviews,
      this.#extensionOperationReconciler,
    );
  }

  /** Notify the live Owner to reconcile its durable recovery queue. */
  async notifyOwnerOfAutomaticRecovery(
    ownership: AgentRunOwnership,
  ): Promise<"owner" | "notified" | "offline"> {
    // A nested child watcher can outlive the direct Spawner's bootstrap. Keep
    // the database locator independent from that in-memory control plane so
    // the same durable pending episode can still reach the live Owner.
    const controlPlane = this.#controlPlane;
    const databasePath = controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId
      ? controlPlane.workflow.databasePath
      : this.#workflowDatabases.get(ownership.workflowOwnerId);
    if (!databasePath) return "offline";
    try {
      const recoveries = new ActivationRecoveryStore(databasePath);
      try {
        if (!recoveries.isPendingFailedActivation(ownership.agentId, ownership.runId)) return "offline";
      } finally {
        recoveries.close();
      }
    } catch {
      return "offline";
    }
    if (controlPlane?.currentAgent.agentId === ownership.workflowOwnerId) return "owner";
    return notifyWorkflowOwnerOfAutomaticRecovery({
      databasePath,
      workflowOwnerId: ownership.workflowOwnerId,
    });
  }

  /** Release deferred-only recovery so its actual Inbox Batch can start the model. */
  releaseAutomaticRecoveryDeferredProjection(): ActivationRecord | undefined {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId || !this.#selfOwnership) return undefined;
    return controlPlane.releaseAutomaticRecoveryDeferredProjection(this.#selfOwnership);
  }

  /** Claim the single non-actionable Pi turn required by recovered visible work. */
  claimAutomaticRecoveryContinuation() {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId || !this.#selfOwnership) return false;
    return controlPlane.claimAutomaticRecoveryContinuation(this.#selfOwnership);
  }

  /** Retry only across a fresh process boundary, never within a live Pi run. */
  rearmAutomaticRecoveryContinuation(): boolean {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId || !this.#selfOwnership) return false;
    return controlPlane.rearmAutomaticRecoveryContinuation(this.#selfOwnership);
  }

  confirmAutomaticRecoveryContinuationContext(observedProjectionIds: string[]): boolean {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId || !this.#selfOwnership) return false;
    return controlPlane.confirmAutomaticRecoveryContinuationContext(
      this.#selfOwnership,
      observedProjectionIds,
    );
  }

  abandonAutomaticRecoveryContinuation(): void {
    const controlPlane = this.#requireControlPlane();
    if (controlPlane.currentAgent.agentId === controlPlane.workflow.ownerAgentId || !this.#selfOwnership) return;
    controlPlane.abandonAutomaticRecoveryContinuation(this.#selfOwnership);
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
      if (!(error instanceof WorkflowProtocolError) || error.code !== "AgentRunAlreadyOwned") {
        throw error;
      }
      const current = controlPlane.currentAgentRun(agent);
      const checkpoint = controlPlane.readAgentRunCheckpoint(agent);
      if (!current || !checkpoint || checkpoint.fencingEpoch !== current.epoch) throw error;
      const recoveryLaunch = controlPlane.isAutomaticRecoveryLaunch(current);
      if (recoveryLaunch) {
        const recoveryCheckpoint = parseAutomaticRecoveryCheckpoint(checkpoint.value);
        const recovery = controlPlane.inspectActivationRecovery(agent);
        if (!recoveryCheckpoint
          || recoveryCheckpoint.runId !== current.runId
          || recoveryCheckpoint.fencingEpoch !== current.epoch
          || !recovery
          || recovery.state !== "launching"
          || recovery.replacementRunId !== current.runId
          || recovery.replacementFencingEpoch !== current.epoch) {
          // A missing or ambiguous dispatch phase cannot be safely superseded
          // by manual resume: the old command may still create a child later.
          throw error;
        }
        const cleanup = await this.#reconcileAutomaticRecoveryLaunch({
          failedActivationId: recovery.failedActivationId,
          ownership: current,
          checkpoint: checkpoint.value,
          locator: { surface: recoveryCheckpoint.surface },
          detail: "Manual resume confirmed the pre-activation recovery pane was absent",
        });
        if (cleanup.kind !== "requeued") throw error;
      } else {
        if (!this.#confirmRunTerminated) throw error;
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
      }
      // Automatic recovery must be durably requeued before this new manual
      // epoch is acquired; otherwise a second automatic claim can race it.
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
    if (this.isCancellationOwnedRun(ownership)) return;
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
      const current = this.#controlPlane.currentAgentRun(this.#controlPlane.agent(ownership.agentId));
      if (!current || current.runId !== ownership.runId || current.epoch !== ownership.epoch) return undefined;
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

  /** The exact failed run is still owned by a durable recovery episode. */
  isRecoveryOwnedFailedRun(ownership: AgentRunOwnership): boolean {
    if (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId) {
      return this.#controlPlane.isRecoveryOwnedFailedRun(ownership);
    }
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId);
    if (!databasePath) return false;
    const activationStore = new ActivationLifecycleStore(databasePath);
    try {
      const activation = activationStore.inspectRun(ownership);
      if (activation?.state.kind !== "ended" || activation.state.outcome !== "failed") return false;
      const recoveries = new ActivationRecoveryStore(databasePath);
      try {
        return recoveries.ownsFailedActivation(ownership.agentId, activation.activationId);
      } finally {
        recoveries.close();
      }
    } finally {
      activationStore.close();
    }
  }

  isCancellationOwnedRun(ownership: AgentRunOwnership): boolean {
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId)
      ?? (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId
        ? this.#controlPlane.workflow.databasePath
        : undefined);
    if (!databasePath) return false;
    const store = new ActivationCancellationInspectionStore(databasePath);
    try {
      return Boolean(store.inspectForRun({
        workflowOwnerId: ownership.workflowOwnerId,
        agentId: ownership.agentId,
        runId: ownership.runId,
        fencingEpoch: ownership.epoch,
      }));
    } finally {
      store.close();
    }
  }

  wasProtocolCancelled(ownership: AgentRunOwnership): boolean {
    const databasePath = this.#workflowDatabases.get(ownership.workflowOwnerId)
      ?? (this.#controlPlane?.workflow.ownerAgentId === ownership.workflowOwnerId
        ? this.#controlPlane.workflow.databasePath
        : undefined);
    if (!databasePath) return false;
    const store = new ActivationCancellationInspectionStore(databasePath);
    try {
      return store.inspectForRun({
        workflowOwnerId: ownership.workflowOwnerId,
        agentId: ownership.agentId,
        runId: ownership.runId,
        fencingEpoch: ownership.epoch,
      })?.state === "committed";
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

  async #discoverRecoveryPane(intent: RecoveryPaneIntent): Promise<RecoveryPaneDiscovery> {
    if (!this.#recoveryPaneLocator) {
      return { kind: "unavailable", error: "Recovery pane discovery is unavailable" };
    }
    try {
      return await this.#recoveryPaneLocator.discover({
        workspaceId: intent.workspaceId,
        label: intent.label,
        cwd: intent.cwd,
        ...(intent.surface ? { surface: intent.surface } : {}),
      });
    } catch (error) {
      return { kind: "unavailable", error: (error as Error).message };
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

  /**
   * Requeue one exact pre-bootstrap recovery launch only after its pane is
   * known absent. The ownership/recovery CAS rejects a child bootstrap that
   * wins between inspection and cleanup, preserving the launch fence.
   */
  async #reconcileAutomaticRecoveryLaunch(input: {
    failedActivationId: string;
    ownership: AgentRunOwnership;
    checkpoint: string;
    locator: AgentRunLocator;
    initialInspection?: Awaited<ReturnType<AgentRunTerminator["inspect"]>>;
    detail: string;
  }): Promise<RecoveryLaunchReconciliation> {
    let inspection = input.initialInspection;
    if (!inspection) {
      try {
        inspection = await this.#agentRunTerminator.inspect(input.locator);
      } catch (error) {
        return { kind: "unknown", detail: (error as Error).message };
      }
    }
    if (inspection.kind === "unavailable") {
      return { kind: "unknown", detail: inspection.error ?? "Recovery pane liveness is unavailable" };
    }
    if (inspection.kind === "present") {
      try {
        await this.#agentRunTerminator.close(input.locator);
        inspection = await this.#agentRunTerminator.inspect(input.locator);
      } catch (error) {
        return { kind: "unknown", detail: (error as Error).message };
      }
      if (inspection.kind !== "missing") {
        return {
          kind: "unknown",
          detail: inspection.kind === "unavailable"
            ? inspection.error ?? "Recovery pane liveness is unavailable after close"
            : "Recovery pane termination is unconfirmed",
        };
      }
    }
    try {
      this.#requireControlPlane().abandonRecoveryEpisodeLaunch(
        input.failedActivationId,
        input.ownership,
        input.detail,
        input.checkpoint,
      );
      return { kind: "requeued" };
    } catch (error) {
      if (error instanceof WorkflowProtocolError
        && (error.code === "RecoveryActivationClaimed" || error.code === "OwnershipLost")) {
        return { kind: "raced", detail: error.message };
      }
      throw error;
    }
  }

  #transitionAutomaticRecoveryDispatch(
    ownership: AgentRunOwnership,
    from: AutomaticRecoveryDispatchPhase,
    to: AutomaticRecoveryDispatchPhase,
  ): void {
    const controlPlane = this.#requireControlPlane();
    const checkpoint = controlPlane.readAgentRunCheckpoint(controlPlane.agent(ownership.agentId));
    const current = checkpoint && checkpoint.fencingEpoch === ownership.epoch
      ? parseAutomaticRecoveryCheckpoint(checkpoint.value)
      : undefined;
    if (!current || current.runId !== ownership.runId || current.fencingEpoch !== ownership.epoch) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        "Automatic recovery dispatch no longer has the exact Agent Run checkpoint",
      );
    }
    if (current.phase === to) return;
    if (current.phase !== from) {
      throw new WorkflowProtocolError(
        "RecoveryActivationClaimed",
        `Automatic recovery dispatch is already ${current.phase}`,
      );
    }
    const updated = controlPlane.compareAndSetAgentRunCheckpoint(
      ownership,
      checkpoint!.value,
      serializeAutomaticRecoveryCheckpoint(ownership, current.surface, to),
    );
    if (!updated) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        "Automatic recovery dispatch changed before its exact phase transition",
      );
    }
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

function serializeAutomaticRecoveryCheckpoint(
  ownership: AgentRunOwnership,
  surface: string,
  phase: AutomaticRecoveryDispatchPhase,
): string {
  return JSON.stringify({
    kind: "automatic-recovery",
    surface,
    runId: ownership.runId,
    fencingEpoch: ownership.epoch,
    phase,
  } satisfies AutomaticRecoveryCheckpoint);
}

function parseAutomaticRecoveryCheckpoint(value: string): AutomaticRecoveryCheckpoint | undefined {
  try {
    const candidate = JSON.parse(value) as Partial<AutomaticRecoveryCheckpoint>;
    if (candidate.kind !== "automatic-recovery"
      || typeof candidate.surface !== "string" || !candidate.surface
      || typeof candidate.runId !== "string" || !candidate.runId
      || !Number.isSafeInteger(candidate.fencingEpoch) || candidate.fencingEpoch <= 0
      || (candidate.phase !== "prepared" && candidate.phase !== "dispatching" && candidate.phase !== "dispatched")) {
      return undefined;
    }
    return candidate as AutomaticRecoveryCheckpoint;
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
