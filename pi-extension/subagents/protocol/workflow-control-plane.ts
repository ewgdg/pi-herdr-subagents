import { existsSync, realpathSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { AgentRunOwnershipStore } from "./agent-run-ownership.ts";
import {
  ActivationLifecycleStore,
  type ActivationRecord,
  type ActivationStartResult,
  type DeclaredActivationDependency,
  type FailedExit,
  type HumanInterruptRecord,
  type InterruptionRequest,
  type UndeclaredSettlementEpisode,
} from "./activation-lifecycle.ts";
import { readPiSessionUuid, assertSessionUuid } from "./workflow-identity.ts";
import { WorkflowInspection, type InspectionTarget } from "./workflow-inspection.ts";
import { assertDescendantTranscriptPath, createWorkflowLayout } from "./workflow-layout.ts";
import { SQLiteWorkflowStore } from "./sqlite-workflow-store.ts";
import {
  DirectSignalStore,
  DirectSignalInspectionStore,
  type SpawnedInitialRequestReceipt,
} from "./sqlite-message-store.ts";
import {
  digestPayload,
  resolveCanonicalSpawnedInitialRequest,
} from "./direct-signal-transcript.ts";
import {
  assertWorkflowSessionBinding,
  type WorkflowSessionBinding,
} from "./workflow-session-binding.ts";
import {
  WorkflowProtocolError,
  type AgentCapabilityConfiguration,
  type AgentRecord,
  type AgentReference,
  type AgentRunOwnership,
  type WorkflowRecord,
} from "./workflow-types.ts";
import { ActivationCancellationStore } from "./activation-cancellation.ts";
import {
  ActivationRecoveryStore,
  type ActivationRecoveryRecord,
  type RecoveryPaneIntent,
} from "./activation-recovery.ts";

export type {
  AgentCapabilityConfiguration,
  AgentRecord,
  AgentReference,
  AgentRunOwnership,
  WorkflowRecord,
} from "./workflow-types.ts";
export { WorkflowProtocolError } from "./workflow-types.ts";
export type {
  ActivationDependency,
  ActivationRecord,
  ActivationStartResult,
  ActivationState,
  DeclaredActivationDependency,
  FailedExit,
  HumanInterruptRecord,
  InterruptionRequest,
  UndeclaredSettlementEpisode,
} from "./activation-lifecycle.ts";

const DEFAULT_CAPABILITIES: AgentCapabilityConfiguration = { spawning: true };

export interface StartWorkflowOwnerOptions {
  ownerSessionId: string;
  ownerSessionPath: string;
  ownerName?: string;
  ownerCapabilities?: AgentCapabilityConfiguration;
  now?: () => number;
}

export interface OpenWorkflowAgentOptions {
  ownerSessionId: string;
  ownerSessionPath: string;
  agentSessionId: string;
  agentSessionPath: string;
  now?: () => number;
}

export interface OpenCurrentWorkflowAgentOptions {
  agentSessionId: string;
  agentSessionPath: string;
  now?: () => number;
}

export interface AddWorkflowAgentInput {
  agentId: string;
  sessionPath: string;
  spawner: AgentReference;
  name: string;
  agentDefinition?: string;
  capabilities?: AgentCapabilityConfiguration;
  launchPolicy?: import("./workflow-types.ts").AgentLaunchPolicy;
  sessionBinding: WorkflowSessionBinding;
}

export interface SpawnedInitialRequestInput {
  agentId: string;
  sessionPath: string;
  runId: string;
  messageId: string;
  sourceEntryId: string;
  message: string;
  name: string;
  agentDefinition: string;
  capabilities?: AgentCapabilityConfiguration;
  launchPolicy?: import("./workflow-types.ts").AgentLaunchPolicy;
  sessionBinding: WorkflowSessionBinding;
  /** Presence proves the external child Router completed its prepare phase. */
  routerEndpoint?: string;
  /** Durable process locator captured by the atomic spawn commit. */
  checkpoint?: string;
}

export class WorkflowControlPlane {
  readonly workflow: WorkflowRecord;
  readonly #store: SQLiteWorkflowStore;
  readonly #ownership: AgentRunOwnershipStore;
  readonly #activations: ActivationLifecycleStore;
  readonly #messageInspection: DirectSignalInspectionStore;
  readonly #cancellations: ActivationCancellationStore;
  readonly #recoveries: ActivationRecoveryStore;
  readonly #now: () => number;
  readonly #currentAgentId: string;
  #closed = false;

  private constructor(
    workflow: WorkflowRecord,
    store: SQLiteWorkflowStore,
    ownership: AgentRunOwnershipStore,
    activations: ActivationLifecycleStore,
    now: () => number,
    currentAgentId: string,
  ) {
    this.workflow = workflow;
    this.#store = store;
    this.#ownership = ownership;
    this.#activations = activations;
    this.#messageInspection = new DirectSignalInspectionStore(workflow.databasePath);
    this.#cancellations = new ActivationCancellationStore(workflow.databasePath);
    this.#recoveries = new ActivationRecoveryStore(workflow.databasePath);
    this.#now = now;
    this.#currentAgentId = currentAgentId;
  }

  static startOwner(options: StartWorkflowOwnerOptions): WorkflowControlPlane {
    assertSessionUuid(options.ownerSessionId);
    const ownerSessionPath = resolve(options.ownerSessionPath);
    const transcriptSessionId = readPiSessionUuid(ownerSessionPath);
    if (transcriptSessionId !== options.ownerSessionId) {
      throw new WorkflowProtocolError(
        "InvalidSessionIdentity",
        `Owner session UUID ${options.ownerSessionId} does not match transcript ${transcriptSessionId}`,
      );
    }

    const now = options.now ?? Date.now;
    const proposedWorkflow = createWorkflowLayout({
      ownerSessionId: options.ownerSessionId,
      ownerSessionPath,
      createdAtMs: now(),
    });
    const store = new SQLiteWorkflowStore(proposedWorkflow.databasePath);
    let workflow: WorkflowRecord;
    try {
      workflow = store.openOwner({
        workflow: proposedWorkflow,
        ownerName: options.ownerName ?? "Workflow Owner",
        capabilities: options.ownerCapabilities ?? DEFAULT_CAPABILITIES,
      });
    } catch (error) {
      store.close();
      throw error;
    }

    const ownership = new AgentRunOwnershipStore(workflow.databasePath);
    return new WorkflowControlPlane(
      workflow,
      store,
      ownership,
      new ActivationLifecycleStore(workflow.databasePath),
      now,
      workflow.ownerAgentId,
    );
  }

  static openAgent(options: OpenWorkflowAgentOptions): WorkflowControlPlane {
    assertSessionUuid(options.ownerSessionId);
    assertSessionUuid(options.agentSessionId);
    const agentSessionPath = resolve(options.agentSessionPath);
    if (readPiSessionUuid(agentSessionPath) !== options.agentSessionId) {
      throw new WorkflowProtocolError(
        "InvalidSessionIdentity",
        `Agent session UUID ${options.agentSessionId} does not match its transcript`,
      );
    }

    const now = options.now ?? Date.now;
    const proposedWorkflow = createWorkflowLayout({
      ownerSessionId: options.ownerSessionId,
      ownerSessionPath: options.ownerSessionPath,
      createdAtMs: now(),
    });
    const store = new SQLiteWorkflowStore(proposedWorkflow.databasePath);
    let workflow: WorkflowRecord;
    try {
      workflow = store.openExisting(proposedWorkflow);
      const member = store.inspectAgent(workflow.ownerAgentId, options.agentSessionId);
      if (member.sessionPath !== realpathSync(agentSessionPath)) {
        throw new WorkflowProtocolError(
          "InvalidSessionIdentity",
          `Agent transcript path conflicts with durable membership ${options.agentSessionId}`,
        );
      }
    } catch (error) {
      store.close();
      throw error;
    }

    return new WorkflowControlPlane(
      workflow,
      store,
      new AgentRunOwnershipStore(workflow.databasePath),
      new ActivationLifecycleStore(workflow.databasePath),
      now,
      options.agentSessionId,
    );
  }

  static openAgentFromSession(
    options: OpenCurrentWorkflowAgentOptions,
  ): WorkflowControlPlane | undefined {
    assertSessionUuid(options.agentSessionId);
    const agentSessionPath = realpathSync(options.agentSessionPath);
    const sessionsDirectory = dirname(agentSessionPath);
    if (basename(sessionsDirectory) !== "sessions") return undefined;
    const workflowDirectory = dirname(sessionsDirectory);
    const databasePath = join(workflowDirectory, "coordination.sqlite");
    if (!existsSync(databasePath)) return undefined;

    const store = new SQLiteWorkflowStore(databasePath);
    try {
      const identity = store.readWorkflowIdentity();
      if (!identity) {
        store.close();
        return undefined;
      }
      const now = options.now ?? Date.now;
      const proposedWorkflow = createWorkflowLayout({
        ownerSessionId: identity.ownerAgentId,
        ownerSessionPath: identity.ownerSessionPath,
        createdAtMs: now(),
      });
      if (realpathSync(proposedWorkflow.directory) !== realpathSync(workflowDirectory)) {
        throw new WorkflowProtocolError(
          "WorkflowMismatch",
          `Agent transcript is not inside Workflow ${identity.ownerAgentId}`,
        );
      }
      const workflow = store.openExisting(proposedWorkflow);
      const member = store.inspectAgent(workflow.ownerAgentId, options.agentSessionId);
      if (member.sessionPath !== agentSessionPath) {
        throw new WorkflowProtocolError(
          "InvalidSessionIdentity",
          `Agent transcript path conflicts with durable membership ${options.agentSessionId}`,
        );
      }
      return new WorkflowControlPlane(
        workflow,
        store,
        new AgentRunOwnershipStore(workflow.databasePath),
        new ActivationLifecycleStore(workflow.databasePath),
        now,
        options.agentSessionId,
      );
    } catch (error) {
      store.close();
      throw error;
    }
  }

  get owner(): AgentReference {
    return this.agent(this.workflow.ownerAgentId);
  }

  get currentAgent(): AgentReference {
    return this.agent(this.#currentAgentId);
  }

  agent(agentId: string): AgentReference {
    assertSessionUuid(agentId);
    return { workflowOwnerId: this.workflow.ownerAgentId, agentId };
  }

  close(): void {
    if (this.#closed) return;
    this.#ownership.close();
    this.#activations.close();
    this.#messageInspection.close();
    this.#cancellations.close();
    this.#recoveries.close();
    this.#store.close();
    this.#closed = true;
  }

  addAgent(input: AddWorkflowAgentInput): AgentRecord {
    this.#assertOpen();
    this.#assertReference(input.spawner);
    if (input.spawner.agentId !== this.#currentAgentId) {
      throw new WorkflowProtocolError(
        "InvalidSpawner",
        `Current Agent ${this.#currentAgentId} cannot create a child for ${input.spawner.agentId}`,
      );
    }
    assertSessionUuid(input.agentId);
    const sessionPath = assertDescendantTranscriptPath(this.workflow, input.sessionPath);
    assertWorkflowSessionBinding(input.sessionBinding, {
      workflowOwnerId: this.workflow.ownerAgentId,
      agentId: input.agentId,
      sessionPath,
    });
    const transcriptSessionId = readPiSessionUuid(sessionPath);
    if (transcriptSessionId !== input.agentId) {
      throw new WorkflowProtocolError(
        "InvalidSessionIdentity",
        `Agent session UUID ${input.agentId} does not match transcript ${transcriptSessionId}`,
      );
    }

    return this.#store.addAgent({
      workflowOwnerId: this.workflow.ownerAgentId,
      agentId: input.agentId,
      sessionPath,
      name: input.name,
      agentDefinition: input.agentDefinition,
      spawnerAgentId: input.spawner.agentId,
      capabilities: input.capabilities ?? DEFAULT_CAPABILITIES,
      launchPolicy: input.launchPolicy,
      createdAtMs: this.#now(),
    });
  }

  spawnInitialRequest(input: SpawnedInitialRequestInput): SpawnedInitialRequestReceipt {
    this.#assertOpen();
    assertSessionUuid(input.agentId);
    const sessionPath = assertDescendantTranscriptPath(this.workflow, input.sessionPath);
    assertWorkflowSessionBinding(input.sessionBinding, {
      workflowOwnerId: this.workflow.ownerAgentId,
      agentId: input.agentId,
      sessionPath,
    });
    if (readPiSessionUuid(sessionPath) !== input.agentId) {
      throw new WorkflowProtocolError("InvalidSessionIdentity", `Agent session UUID ${input.agentId} does not match transcript ${sessionPath}`);
    }
    resolveCanonicalSpawnedInitialRequest({
      sessionPath: this.currentAgent.agentId === this.workflow.ownerAgentId
        ? this.workflow.ownerSessionPath
        : this.#store.inspectAgent(this.workflow.ownerAgentId, this.currentAgent.agentId).sessionPath,
      sourceEntryId: input.sourceEntryId,
      agentDefinition: input.agentDefinition,
      name: input.name,
      message: input.message,
    });
    const messages = new DirectSignalStore(this.workflow.databasePath);
    try {
      return messages.acceptSpawnedInitialRequest({
        spawner: this.currentAgent,
        child: {
          agentId: input.agentId,
          sessionPath,
          name: input.name,
          agentDefinition: input.agentDefinition,
          capabilities: input.capabilities ?? DEFAULT_CAPABILITIES,
          launchPolicy: input.launchPolicy,
        },
        runId: input.runId,
        messageId: input.messageId,
        sourceEntryId: input.sourceEntryId,
        payloadDigest: digestPayload(input.message),
        routerEndpoint: input.routerEndpoint,
        checkpoint: input.checkpoint,
        createdAtMs: this.#now(),
      });
    } finally {
      messages.close();
    }
  }

  reconcileSpawnedInitialRequest(input: {
    sourceEntryId: string;
    agentDefinition: string;
    name: string;
    message: string;
    capabilities?: AgentCapabilityConfiguration;
  }) {
    const messages = new DirectSignalStore(this.workflow.databasePath);
    try {
      return messages.reconcileSpawnedInitialRequest({
        spawner: this.currentAgent,
        sourceEntryId: input.sourceEntryId,
        payloadDigest: digestPayload(input.message),
        agentDefinition: input.agentDefinition,
        name: input.name,
        capabilities: input.capabilities ?? DEFAULT_CAPABILITIES,
      });
    } finally {
      messages.close();
    }
  }

  removeAgent(reference: AgentReference): void {
    this.#assertOpen();
    this.#assertReference(reference);
    if (reference.agentId === this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("InvalidSpawner", "Workflow Owner cannot be removed");
    }
    const member = this.#store.inspectAgent(reference.workflowOwnerId, reference.agentId);
    if (
      this.#currentAgentId !== this.workflow.ownerAgentId &&
      member.spawnerAgentId !== this.#currentAgentId
    ) {
      throw new WorkflowProtocolError(
        "InvalidSpawner",
        `Current Agent ${this.#currentAgentId} cannot remove Agent ${reference.agentId}`,
      );
    }
    this.#store.removeAgent(reference.workflowOwnerId, reference.agentId);
  }

  inspectAgent(reference: AgentReference): AgentRecord {
    this.#assertOpen();
    this.#assertReference(reference);
    return this.#store.inspectAgent(reference.workflowOwnerId, reference.agentId);
  }

  inspectTarget(target: InspectionTarget): unknown {
    this.#assertOpen();
    return new WorkflowInspection({
      workflow: this.workflow,
      caller: this.currentAgent,
      agents: this.#store,
      inspectActivation: (agent) => this.#activations.inspect(agent),
      inspectHumanInterrupt: (agent) => this.#activations.inspectHumanInterrupt(agent),
      inspectUndeclaredEpisode: (agent) => this.#activations.inspectUndeclaredEpisode(agent),
      inspectActivationCancellation: (agent) => this.#cancellations.inspectForAgent(agent),
      inspectActivationRecovery: (agent) => this.#recoveries.inspect(agent),
      inspectRequestProjection: (requestId) => this.#messageInspection.inspectRequestProjection(this.workflow.ownerAgentId, requestId),
      now: this.#now,
    }).inspect(target);
  }

  listDirectChildren(spawner: AgentReference): AgentRecord[] {
    this.#assertOpen();
    this.#assertReference(spawner);
    if (
      this.#currentAgentId !== this.workflow.ownerAgentId &&
      spawner.agentId !== this.#currentAgentId
    ) {
      throw new WorkflowProtocolError(
        "InvalidSpawner",
        `Agent ${this.#currentAgentId} cannot enumerate children of ${spawner.agentId}`,
      );
    }
    return this.#store.listDirectChildren(spawner.workflowOwnerId, spawner.agentId);
  }

  listWorkflow(owner: AgentReference): AgentRecord[] {
    this.#assertOpen();
    this.#assertReference(owner);
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Agent ${this.#currentAgentId} cannot enumerate Workflow ${this.workflow.ownerAgentId}`,
      );
    }
    if (owner.agentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Only Workflow Owner ${this.workflow.ownerAgentId} identifies this Workflow`,
      );
    }
    return this.#store.listWorkflow(owner.workflowOwnerId);
  }

  authorizeDirectTarget(sender: AgentReference, target: AgentReference): AgentRecord {
    this.#assertOpen();
    this.#assertReference(sender);
    this.#assertReference(target);
    if (sender.agentId !== this.#currentAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Current Agent ${this.#currentAgentId} cannot route as ${sender.agentId}`,
      );
    }
    this.#store.inspectAgent(sender.workflowOwnerId, sender.agentId);
    return this.#store.inspectAgent(target.workflowOwnerId, target.agentId);
  }

  acquireAgentRun(agent: AgentReference, runId: string): AgentRunOwnership {
    this.#assertOpen();
    const member = this.inspectAgent(agent);
    if (member.agentId === this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "OwnerActivationForbidden",
        "Workflow Owner does not have a Subagent Agent Run or activation lifecycle",
      );
    }
    if (
      this.#currentAgentId !== this.workflow.ownerAgentId &&
      member.spawnerAgentId !== this.#currentAgentId
    ) {
      throw new WorkflowProtocolError(
        "InvalidSpawner",
        `Agent ${this.#currentAgentId} cannot start Agent Run for ${member.agentId}`,
      );
    }
    return this.#ownership.acquire(
      { workflowOwnerId: member.workflowOwnerId, agentId: member.agentId },
      runId,
    );
  }

  startActivation(ownership: AgentRunOwnership): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.start(ownership, this.#now());
  }

  /** Start a claimed recovery and report if cancellation made it unnecessary. */
  startRecoveryActivation(ownership: AgentRunOwnership): ActivationStartResult {
    this.#assertOwnershipReference(ownership);
    try {
      return this.#activations.startRecovery(ownership, this.#now());
    } catch (error) {
      // The pane can bootstrap before its Owner returns from runStarted(). If
      // that pane already resolved and released this exact unneeded claim,
      // its late launcher observes the same explicit outcome rather than
      // treating the intentional release as a failed recovery startup.
      if (error instanceof WorkflowProtocolError && error.code === "OwnershipLost") {
        const failedActivationId = this.#recoveries.resolvedUnneededReplacement(ownership);
        if (failedActivationId) return { kind: "not-needed", failedActivationId };
      }
      throw error;
    }
  }

  inspectActivation(agent: AgentReference): ActivationRecord | undefined {
    this.#assertOpen();
    this.#assertReference(agent);
    return this.#activations.inspect(agent);
  }

  addActivationDependency(
    ownership: AgentRunOwnership,
    dependency: DeclaredActivationDependency,
    expectedRevision?: number,
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.addDependency(ownership, dependency, this.#now(), expectedRevision);
  }

  removeActivationDependency(
    ownership: AgentRunOwnership,
    dependency: Pick<DeclaredActivationDependency, "kind" | "dependencyId">,
    expectedRevision?: number,
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.removeDependency(ownership, dependency, this.#now(), expectedRevision);
  }

  satisfyActivationDependency(
    ownership: AgentRunOwnership,
    dependency: Pick<DeclaredActivationDependency, "kind" | "dependencyId">,
    expectedRevision?: number,
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.satisfyDependency(ownership, dependency, this.#now(), expectedRevision);
  }

  settleActivation(
    ownership: AgentRunOwnership,
    expectedRevision?: number,
    actorRole: "ordinary" | "moderator" = "ordinary",
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.settle(ownership, this.#now(), expectedRevision, actorRole);
  }

  settleOwnerTurn(): { kind: "owner-turn-settled" } {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "OwnerActivationForbidden",
        "Only the Workflow Owner can settle an Owner turn",
      );
    }
    return { kind: "owner-turn-settled" };
  }

  activateTurn(
    ownership: AgentRunOwnership,
    expectedRevision?: number,
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.activateTurn(ownership, this.#now(), expectedRevision);
  }

  beginHumanInterrupt(
    ownership: AgentRunOwnership,
    toolCallId: string,
    actorRole: "ordinary" | "moderator" = "ordinary",
  ): HumanInterruptRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.beginHumanInterrupt(ownership, toolCallId, this.#now(), actorRole);
  }

  bindHumanResponse(
    ownership: AgentRunOwnership,
    toolCallId: string,
    responseInputId: string,
  ): HumanInterruptRecord | undefined {
    this.#assertOwnershipReference(ownership);
    return this.#activations.bindHumanResponse(ownership, toolCallId, responseInputId, this.#now());
  }

  prepareHumanResponseResult(ownership: AgentRunOwnership, toolCallId: string): HumanInterruptRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.prepareHumanResponseResult(ownership, toolCallId, this.#now());
  }

  resumeHumanResponseResult(ownership: AgentRunOwnership, toolCallId: string): HumanInterruptRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.resumeHumanResponseResult(ownership, toolCallId, this.#now());
  }

  confirmHumanResponseResult(ownership: AgentRunOwnership, toolCallId: string): HumanInterruptRecord | undefined {
    this.#assertOwnershipReference(ownership);
    return this.#activations.confirmHumanResponseResult(ownership, toolCallId, this.#now());
  }

  inspectHumanInterrupt(agent: AgentReference): HumanInterruptRecord | undefined {
    this.#assertReference(agent);
    return this.#activations.inspectHumanInterrupt(agent);
  }

  inspectHumanInterruptToolCall(
    agent: AgentReference,
    toolCallId: string,
  ): HumanInterruptRecord | undefined {
    this.#assertReference(agent);
    return this.#activations.inspectHumanInterruptToolCall(agent, toolCallId);
  }

  hasHumanAttention(agent: AgentReference): boolean {
    this.#assertReference(agent);
    return this.#activations.humanAttention(agent);
  }

  pendingUndeclaredNotice(agent: AgentReference): UndeclaredSettlementEpisode | undefined {
    this.#assertReference(agent);
    return this.#activations.pendingUndeclaredNotice(agent);
  }

  confirmUndeclaredNotice(agent: AgentReference, episodeId: string): boolean {
    this.#assertReference(agent);
    return this.#activations.confirmUndeclaredNotice(agent, episodeId, this.#now());
  }

  queueUndeclaredNotice(agent: AgentReference, episodeId: string): UndeclaredSettlementEpisode | undefined {
    this.#assertReference(agent);
    return this.#activations.queueUndeclaredNotice(agent, episodeId, this.#now());
  }

  inspectUndeclaredEpisode(agent: AgentReference): UndeclaredSettlementEpisode | undefined {
    this.#assertReference(agent);
    return this.#activations.inspectUndeclaredEpisode(agent);
  }

  requestInterruption(
    ownership: AgentRunOwnership,
    expectedRevision?: number,
  ): InterruptionRequest {
    this.#assertOwnershipReference(ownership);
    return this.#activations.requestInterruption(ownership, this.#now(), expectedRevision);
  }

  confirmInterruption(
    ownership: AgentRunOwnership,
    confirmation?: InterruptionRequest,
    expectedRevision?: number,
  ): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.confirmInterruption(
      ownership,
      confirmation,
      this.#now(),
      expectedRevision,
    );
  }

  failAgentRun(ownership: AgentRunOwnership, failure: FailedExit): ActivationRecord {
    this.#assertOwnershipReference(ownership);
    return this.#activations.failAndRelease(ownership, failure, this.#now());
  }

  claimableRecoveryEpisodes(): ActivationRecoveryRecord[] {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) return [];
    return this.#recoveries.listClaimable(this.workflow.ownerAgentId);
  }

  inFlightRecoveryEpisodes(): ActivationRecoveryRecord[] {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) return [];
    return this.#recoveries.listInFlight(this.workflow.ownerAgentId);
  }

  claimRecoveryRun(
    failedActivationId: string,
    runId: string,
    preparedSurface?: string,
  ): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can launch automatic recovery");
    }
    return this.#recoveries.claimRun(
      this.workflow.ownerAgentId,
      failedActivationId,
      runId,
      this.#now(),
      preparedSurface,
    );
  }

  prepareRecoveryPaneIntent(input: {
    failedActivationId: string;
    intentId: string;
    runId: string;
    workspaceId: string;
    label: string;
    cwd: string;
  }): RecoveryPaneIntent | undefined {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can prepare automatic recovery");
    }
    return this.#recoveries.preparePaneIntent({
      workflowOwnerId: this.workflow.ownerAgentId,
      ...input,
      now: this.#now(),
    });
  }

  beginRecoveryPaneCreation(intentId: string, runId: string): RecoveryPaneIntent {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can create automatic recovery panes");
    }
    return this.#recoveries.beginPaneCreation({ intentId, runId, now: this.#now() });
  }

  recordRecoveryPaneCreated(intentId: string, runId: string, surface: string): RecoveryPaneIntent {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can acknowledge automatic recovery panes");
    }
    return this.#recoveries.recordPaneCreated({ intentId, runId, surface, now: this.#now() });
  }

  promoteRecoveryPaneIntent(input: {
    failedActivationId: string;
    intentId: string;
    runId: string;
    surface: string;
  }): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can promote automatic recovery");
    }
    return this.#recoveries.promotePaneIntent({
      workflowOwnerId: this.workflow.ownerAgentId,
      ...input,
      now: this.#now(),
    });
  }

  beginRecoveryPaneCleanup(intentId: string, detail: string): RecoveryPaneIntent | undefined {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can clean automatic recovery panes");
    }
    return this.#recoveries.beginPaneCleanup({ intentId, detail, now: this.#now() });
  }

  completeRecoveryPaneCleanup(intentId: string, expectedSurface?: string): boolean {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can clean automatic recovery panes");
    }
    return this.#recoveries.completePaneCleanup({ intentId, expectedSurface });
  }

  retireRecoveryPaneIntent(intentId: string): boolean {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can retire automatic recovery intents");
    }
    return this.#recoveries.retirePaneIntent(intentId);
  }

  inspectRecoveryPaneIntent(intentId: string): RecoveryPaneIntent | undefined {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) return undefined;
    return this.#recoveries.inspectPaneIntent(intentId);
  }

  recoveryPaneIntents(): RecoveryPaneIntent[] {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) return [];
    return this.#recoveries.listPaneIntents(this.workflow.ownerAgentId);
  }

  abandonRecoveryEpisodeLaunch(
    failedActivationId: string,
    ownership: AgentRunOwnership,
    detail: string,
    expectedCheckpoint?: string,
  ): void {
    this.#assertOpen();
    if (this.#currentAgentId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Only the Workflow Owner can manage automatic recovery");
    }
    this.#assertOwnershipReference(ownership);
    this.#recoveries.abandon(failedActivationId, ownership, this.#now(), detail, expectedCheckpoint);
  }

  inspectActivationRecovery(agent: AgentReference): ActivationRecoveryRecord | undefined {
    this.#assertReference(agent);
    return this.#recoveries.inspect(agent);
  }

  isRecoveryOwnedFailedRun(ownership: AgentRunOwnership): boolean {
    this.#assertOwnershipReference(ownership);
    const activation = this.#activations.inspectRun(ownership);
    return activation?.state.kind === "ended"
      && activation.state.outcome === "failed"
      && this.#recoveries.ownsFailedActivation(ownership.agentId, activation.activationId);
  }

  isAutomaticRecoveryLaunch(ownership: AgentRunOwnership): boolean {
    this.#assertOwnershipReference(ownership);
    return this.#recoveries.isLaunchingReplacement(ownership);
  }

  releaseAutomaticRecoveryDeferredProjection(ownership: AgentRunOwnership): ActivationRecord | undefined {
    this.#assertOwnershipReference(ownership);
    if (!this.#recoveries.releaseDeferredProjection(ownership, this.#now())) return undefined;
    return this.#activations.inspectRun(ownership);
  }

  claimAutomaticRecoveryContinuation(ownership: AgentRunOwnership) {
    this.#assertOwnershipReference(ownership);
    return this.#recoveries.claimContinuation(ownership, this.#now()) ?? false;
  }

  rearmAutomaticRecoveryContinuation(ownership: AgentRunOwnership): boolean {
    this.#assertOwnershipReference(ownership);
    return this.#recoveries.rearmContinuation(ownership, this.#now());
  }

  confirmAutomaticRecoveryContinuationContext(
    ownership: AgentRunOwnership,
    observedProjectionIds: string[],
  ): boolean {
    this.#assertOwnershipReference(ownership);
    return this.#recoveries.confirmContinuationContext(
      ownership,
      observedProjectionIds,
      this.#now(),
    );
  }

  abandonAutomaticRecoveryContinuation(ownership: AgentRunOwnership): void {
    this.#assertOwnershipReference(ownership);
    this.#recoveries.abandonContinuation(ownership, this.#now());
  }

  acquireCurrentAgentRun(runId: string): AgentRunOwnership {
    this.#assertOpen();
    const member = this.inspectAgent(this.currentAgent);
    return this.#ownership.acquire(
      { workflowOwnerId: member.workflowOwnerId, agentId: member.agentId },
      runId,
    );
  }

  currentAgentRun(agent: AgentReference): AgentRunOwnership | undefined {
    this.#assertOpen();
    const member = this.inspectAgent(agent);
    return this.#ownership.current({
      workflowOwnerId: member.workflowOwnerId,
      agentId: member.agentId,
    });
  }

  releaseAgentRun(ownership: AgentRunOwnership): void {
    this.#assertOwnershipReference(ownership);
    this.inspectAgent(ownership);
    this.#activations.releaseWithoutActivation(ownership);
  }

  assertCurrentAgentRun(ownership: AgentRunOwnership): void {
    this.#assertOwnershipReference(ownership);
    this.inspectAgent(ownership);
    this.#ownership.assertCurrent(ownership);
  }

  writeAgentRunCheckpoint(ownership: AgentRunOwnership, value: string): void {
    this.#assertOwnershipReference(ownership);
    this.inspectAgent(ownership);
    this.#ownership.writeCheckpoint(ownership, value);
  }

  compareAndSetAgentRunCheckpoint(
    ownership: AgentRunOwnership,
    expectedValue: string,
    value: string,
  ): boolean {
    this.#assertOwnershipReference(ownership);
    this.inspectAgent(ownership);
    return this.#ownership.compareAndSetCheckpoint(ownership, expectedValue, value);
  }

  readAgentRunCheckpoint(
    agent: AgentReference,
  ): { value: string; fencingEpoch: number } | undefined {
    const member = this.inspectAgent(agent);
    return this.#ownership.readCheckpoint(member);
  }

  #assertReference(reference: AgentReference): void {
    assertSessionUuid(reference.agentId);
    assertSessionUuid(reference.workflowOwnerId);
    if (reference.workflowOwnerId !== this.workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Identity ${reference.agentId} belongs to Workflow ${reference.workflowOwnerId}, not ${this.workflow.ownerAgentId}`,
      );
    }
  }

  #assertOwnershipReference(ownership: AgentRunOwnership): void {
    this.#assertOpen();
    this.#assertReference(ownership);
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Workflow Control Plane is closed");
  }
}
