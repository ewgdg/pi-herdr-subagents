import { appendFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { cloneSessionFile, initializeSubagentSessionFile } from "../../pi-extension/subagents/session.ts";
import {
  bindNewWorkflowSession,
  type WorkflowSessionBinding,
} from "../../pi-extension/subagents/protocol/workflow-session-binding.ts";
import {
  WorkflowControlPlane,
  type AgentCapabilityConfiguration,
  type AgentRecord,
  type AgentReference,
  type AgentRunOwnership,
  type ActivationRecord,
  type DeclaredActivationDependency,
  type FailedExit,
  type InterruptionRequest,
  type WorkflowRecord,
} from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import type { InboxBatch } from "../../pi-extension/subagents/protocol/direct-signal.ts";
import { projectInboxBatch } from "../../pi-extension/subagents/protocol/direct-signal-extension.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { ActivationCancellationStore } from "../../pi-extension/subagents/protocol/activation-cancellation.ts";
import type {
  OperationEvidence,
  OperationReviewPolicy,
  OperationReconciliationOutcome,
  OperationReviewRecord,
} from "../../pi-extension/subagents/protocol/operation-review.ts";

export class ManualClock {
  #now: number;

  constructor(now = 1_000) {
    this.#now = now;
  }

  now = (): number => this.#now;

  advance(milliseconds = 1): void {
    this.#now += milliseconds;
  }
}

export class DeterministicIdentityFactory {
  #next = 1;

  next(): string {
    const suffix = this.#next.toString(16).padStart(12, "0");
    this.#next += 1;
    return `00000000-0000-4000-8000-${suffix}`;
  }
}

export interface ScenarioSession {
  agentId: string;
  sessionPath: string;
  workflowBinding?: WorkflowSessionBinding;
}

export class ControllableTranscriptAdapter {
  readonly #identityFactory: DeterministicIdentityFactory;
  readonly #clock: ManualClock;

  constructor(identityFactory: DeterministicIdentityFactory, clock: ManualClock) {
    this.#identityFactory = identityFactory;
    this.#clock = clock;
  }

  create(sessionPath: string): ScenarioSession {
    const agentId = this.#identityFactory.next();
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: sessionPath,
      childCwd: dirname(sessionPath),
      childSessionId: agentId,
      timestamp: new Date(this.#clock.now()).toISOString(),
    });
    return { agentId, sessionPath };
  }

  resume(sessionPath: string): ScenarioSession {
    const header = JSON.parse(readFileSync(sessionPath, "utf8").split("\n")[0]) as {
      id: string;
    };
    return { agentId: header.id, sessionPath };
  }

  fork(source: ScenarioSession, sessionPath: string): ScenarioSession {
    const agentId = this.#identityFactory.next();
    initializeSubagentSessionFile({
      mode: "fork",
      parentSessionFile: source.sessionPath,
      childSessionFile: sessionPath,
      childCwd: dirname(sessionPath),
      childSessionId: agentId,
      timestamp: new Date(this.#clock.now()).toISOString(),
    });
    return { agentId, sessionPath };
  }

  clone(source: ScenarioSession, sessionPath: string): ScenarioSession {
    const identity = this.#identityFactory.next();
    cloneSessionFile(source.sessionPath, sessionPath, {
      sessionId: identity,
      timestamp: new Date(this.#clock.now()).toISOString(),
    });
    return { agentId: identity, sessionPath };
  }

  appendAgentSend(
    session: ScenarioSession,
    input: {
      sourceEntryId?: string;
      targetAgentId?: string;
      targetRequestId?: string;
      targetSpawn?: { agent: string; name?: string };
      message: string;
      timing?: "steer" | "deferred";
      responseRequired?: boolean;
      onAccepted?: "continue" | "complete";
    },
  ): string {
    const sourceEntryId = input.sourceEntryId ?? `tool-${this.#identityFactory.next()}`;
    this.#append(session.sessionPath, {
      type: "message",
      id: `entry-${this.#identityFactory.next()}`,
      timestamp: new Date(this.#clock.now()).toISOString(),
      message: {
        role: "assistant",
        content: [{
          type: "toolCall",
          id: sourceEntryId,
          name: "agent_send",
          arguments: {
            target: input.targetSpawn
              ? { spawn: input.targetSpawn }
              : input.targetRequestId
                ? { request: input.targetRequestId }
                : { agent: input.targetAgentId },
            message: input.message,
            ...(input.timing === undefined ? {} : { timing: input.timing }),
            ...(input.responseRequired === undefined
              ? input.targetSpawn ? { responseRequired: true } : {}
              : { responseRequired: input.responseRequired }),
            ...(input.onAccepted ? { onAccepted: input.onAccepted } : {}),
          },
        }],
      },
    });
    return sourceEntryId;
  }

  appendInboxBatch(session: ScenarioSession, batch: InboxBatch): void {
    const projected = projectInboxBatch(batch);
    this.#append(session.sessionPath, {
      type: "custom_message",
      id: `entry-${this.#identityFactory.next()}`,
      timestamp: new Date(this.#clock.now()).toISOString(),
      ...projected,
    });
  }

  #append(sessionPath: string, entry: Record<string, unknown>): void {
    appendFileSync(sessionPath, `${JSON.stringify(entry)}\n`, "utf8");
  }

}

interface ControllableProcess {
  processId: string;
  agentId: string;
  active: boolean;
  observable: boolean;
}

export class ControllableProcessAdapter {
  #next = 1;
  readonly processes = new Map<string, ControllableProcess>();

  prepare(agentId: string): ControllableProcess {
    const process: ControllableProcess = {
      processId: `runtime-${this.#next++}`,
      agentId,
      active: false,
      observable: true,
    };
    this.processes.set(process.processId, process);
    return process;
  }

  activate(processId: string): void {
    this.#require(processId).active = true;
  }

  discard(processId: string): void {
    this.processes.delete(processId);
  }

  confirmExit(processId: string): void {
    this.#require(processId).active = false;
  }

  loseObservation(processId: string): void {
    this.#require(processId).observable = false;
  }

  isActive(processId: string): boolean {
    return this.#require(processId).active;
  }

  #require(processId: string): ControllableProcess {
    const process = this.processes.get(processId);
    if (!process) throw new Error(`Unknown controllable process: ${processId}`);
    return process;
  }
}

export interface ScenarioAgentRun {
  processId: string;
  ownership: AgentRunOwnership;
}

export class ControllableRuntimeAdapter {
  readonly processAdapter: ControllableProcessAdapter;
  #controlPlane: WorkflowControlPlane;
  readonly #reopen: () => WorkflowControlPlane;
  readonly #now: () => number;

  constructor(
    controlPlane: WorkflowControlPlane,
    processAdapter: ControllableProcessAdapter,
    reopen: () => WorkflowControlPlane,
    now: () => number,
  ) {
    this.#controlPlane = controlPlane;
    this.processAdapter = processAdapter;
    this.#reopen = reopen;
    this.#now = now;
  }

  get controlPlane(): WorkflowControlPlane {
    return this.#controlPlane;
  }

  get workflow(): WorkflowRecord {
    return this.#controlPlane.workflow;
  }

  close(): void {
    this.#controlPlane.close();
  }

  restart(): void {
    this.#controlPlane.close();
    this.#controlPlane = this.#reopen();
  }

  agent(agentId: string): AgentReference {
    return this.#controlPlane.agent(agentId);
  }

  owner(): AgentReference {
    return this.#controlPlane.owner;
  }

  inspectTarget(target: import("../../pi-extension/subagents/protocol/workflow-inspection.ts").InspectionTarget) {
    return this.#controlPlane.inspectTarget(target);
  }

  snapshotDurableState() {
    const agents = this.#controlPlane.listWorkflow(this.#controlPlane.owner);
    return agents.map((agent) => ({
      agent,
      activation: this.#controlPlane.inspectActivation(this.#controlPlane.agent(agent.agentId)),
      ownership: this.#controlPlane.currentAgentRun(this.#controlPlane.agent(agent.agentId)),
      human: this.#controlPlane.inspectHumanInterrupt(this.#controlPlane.agent(agent.agentId)),
      undeclared: this.#controlPlane.inspectUndeclaredEpisode(this.#controlPlane.agent(agent.agentId)),
    }));
  }

  inspect(reference: AgentReference): AgentRecord {
    return this.#controlPlane.inspectAgent(reference);
  }

  descendants(): AgentRecord[] {
    return this.#controlPlane.listWorkflow(this.#controlPlane.owner);
  }

  directChildren(spawner: AgentReference): AgentRecord[] {
    return this.#controlPlane.listDirectChildren(spawner);
  }

  spawnInitialRequest(input: {
    child: ScenarioSession;
    runId: string;
    messageId: string;
    sourceEntryId: string;
    message: string;
    name: string;
    agentDefinition: string;
    launchPolicy?: import("../../pi-extension/subagents/protocol/workflow-types.ts").AgentLaunchPolicy;
    routerEndpoint?: string;
    checkpoint?: string;
  }) {
    if (!input.child.workflowBinding) throw new Error(`Scenario session is not bound to a Workflow: ${input.child.sessionPath}`);
    return this.#controlPlane.spawnInitialRequest({
      agentId: input.child.agentId,
      sessionPath: input.child.sessionPath,
      runId: input.runId,
      messageId: input.messageId,
      sourceEntryId: input.sourceEntryId,
      message: input.message,
      name: input.name,
      agentDefinition: input.agentDefinition,
      launchPolicy: input.launchPolicy,
      sessionBinding: input.child.workflowBinding,
      routerEndpoint: input.routerEndpoint,
      checkpoint: input.checkpoint,
    });
  }

  listRequests(requester: AgentReference) {
    const store = new DirectSignalStore(this.workflow.databasePath);
    try { return store.listRequests(requester); } finally { store.close(); }
  }

  listPending(recipient: AgentReference) {
    const store = new DirectSignalStore(this.workflow.databasePath);
    try { return store.listPending(recipient); } finally { store.close(); }
  }

  addAgent(input: {
    session: ScenarioSession;
    spawner: AgentReference;
    name: string;
    agentDefinition?: string;
    capabilities?: AgentCapabilityConfiguration;
    launchPolicy?: import("../../pi-extension/subagents/protocol/workflow-types.ts").AgentLaunchPolicy;
  }): AgentRecord {
    if (!input.session.workflowBinding) {
      throw new Error(`Scenario session is not bound to a Workflow: ${input.session.sessionPath}`);
    }
    return this.#controlPlane.addAgent({
      agentId: input.session.agentId,
      sessionPath: input.session.sessionPath,
      spawner: input.spawner,
      name: input.name,
      agentDefinition: input.agentDefinition,
      capabilities: input.capabilities,
      launchPolicy: input.launchPolicy,
      sessionBinding: input.session.workflowBinding,
    });
  }

  authorizeDirectTarget(sender: AgentReference, target: AgentReference): AgentRecord {
    return this.#controlPlane.authorizeDirectTarget(sender, target);
  }

  startAgentRun(agent: AgentReference): ScenarioAgentRun {
    const process = this.processAdapter.prepare(agent.agentId);
    let ownership: AgentRunOwnership | undefined;
    try {
      ownership = this.#controlPlane.acquireAgentRun(agent, process.processId);
      this.processAdapter.activate(process.processId);
      this.#controlPlane.startActivation(ownership);
      return { processId: process.processId, ownership };
    } catch (error) {
      if (ownership) this.#controlPlane.releaseAgentRun(ownership);
      this.processAdapter.discard(process.processId);
      throw error;
    }
  }

  currentAgentRun(agent: AgentReference): AgentRunOwnership | undefined {
    return this.#controlPlane.currentAgentRun(agent);
  }

  inspectActivation(agent: AgentReference): ActivationRecord | undefined {
    return this.#controlPlane.inspectActivation(agent);
  }

  inspectOperationReview(operationReviewId: number) {
    return this.#controlPlane.inspectOperationReview(operationReviewId);
  }

  listOperationReviews(agent: AgentReference) {
    return this.#controlPlane.listOperationReviews(agent);
  }

  listOperationReviewEvidence(operationReviewId: number) {
    return this.#controlPlane.listOperationReviewEvidence(operationReviewId);
  }

  recordOperationEvidence(
    operationReviewId: number,
    evidence: Omit<OperationEvidence, "observedAtMs">,
  ) {
    return this.#controlPlane.recordOperationEvidence(operationReviewId, evidence);
  }

  listWorkflowAttention() {
    return this.#controlPlane.listWorkflowAttention();
  }

  reconcileOperationReviews(
    reconcile: (
      review: OperationReviewRecord,
    ) => OperationReconciliationOutcome | Promise<OperationReconciliationOutcome>,
  ) {
    return this.#controlPlane.reconcileOperationReviews(reconcile);
  }

  listOperationIncidentTriggers() {
    return this.#controlPlane.listOperationIncidentTriggers();
  }

  reconcileOperationalIncidents() {
    return this.#controlPlane.reconcileOperationalIncidents();
  }

  listOperationalIncidents() {
    return this.#controlPlane.listOperationalIncidents();
  }

  inspectOperationalIncident(incidentId: string) {
    return this.#controlPlane.inspectOperationalIncident(incidentId);
  }

  inspectIncidentBrief(incidentId: string) {
    return this.#controlPlane.inspectIncidentBrief(incidentId);
  }

  addActivationDependency(
    run: ScenarioAgentRun,
    dependency: DeclaredActivationDependency,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#controlPlane.addActivationDependency(
      run.ownership,
      dependency,
      expectedRevision,
    );
  }

  removeActivationDependency(
    run: ScenarioAgentRun,
    dependency: Pick<DeclaredActivationDependency, "kind" | "dependencyId">,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#controlPlane.removeActivationDependency(
      run.ownership,
      dependency,
      expectedRevision,
    );
  }

  satisfyActivationDependency(
    run: ScenarioAgentRun,
    dependency: Pick<DeclaredActivationDependency, "kind" | "dependencyId">,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#controlPlane.satisfyActivationDependency(run.ownership, dependency, expectedRevision);
  }

  settleActivation(
    run: ScenarioAgentRun,
    expectedRevision?: number,
    actorRole: "ordinary" | "moderator" = "ordinary",
  ): ActivationRecord {
    return this.#controlPlane.settleActivation(run.ownership, expectedRevision, actorRole);
  }

  settleOwnerTurn(): { kind: "owner-turn-settled" } {
    return this.#controlPlane.settleOwnerTurn();
  }

  activateTurn(run: ScenarioAgentRun, expectedRevision?: number): ActivationRecord {
    return this.#controlPlane.activateTurn(run.ownership, expectedRevision);
  }

  requestInterruption(
    run: ScenarioAgentRun,
    expectedRevision?: number,
  ): InterruptionRequest {
    return this.#controlPlane.requestInterruption(run.ownership, expectedRevision);
  }

  confirmInterruption(
    run: ScenarioAgentRun,
    request?: InterruptionRequest,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#controlPlane.confirmInterruption(run.ownership, request, expectedRevision);
  }

  beginHumanInterrupt(run: ScenarioAgentRun, toolCallId: string, actorRole: "ordinary" | "moderator" = "ordinary") {
    return this.#controlPlane.beginHumanInterrupt(run.ownership, toolCallId, actorRole);
  }

  bindHumanResponse(run: ScenarioAgentRun, toolCallId: string, responseInputId: string) {
    return this.#controlPlane.bindHumanResponse(run.ownership, toolCallId, responseInputId);
  }

  prepareHumanResponseResult(run: ScenarioAgentRun, toolCallId: string) {
    return this.#controlPlane.prepareHumanResponseResult(run.ownership, toolCallId);
  }

  resumeHumanResponseResult(run: ScenarioAgentRun, toolCallId: string) {
    return this.#controlPlane.resumeHumanResponseResult(run.ownership, toolCallId);
  }

  confirmHumanResponseResult(run: ScenarioAgentRun, toolCallId: string) {
    return this.#controlPlane.confirmHumanResponseResult(run.ownership, toolCallId);
  }

  inspectHumanInterrupt(reference: AgentReference) {
    return this.#controlPlane.inspectHumanInterrupt(reference);
  }

  hasHumanAttention(reference: AgentReference): boolean {
    return this.#controlPlane.hasHumanAttention(reference);
  }

  pendingUndeclaredNotice(reference: AgentReference) {
    return this.#controlPlane.pendingUndeclaredNotice(reference);
  }

  confirmUndeclaredNotice(reference: AgentReference, episodeId: string): boolean {
    return this.#controlPlane.confirmUndeclaredNotice(reference, episodeId);
  }

  acceptUndeclaredNotice(reference: AgentReference, episodeId: string) {
    return this.#controlPlane.acceptUndeclaredNotice(reference, episodeId);
  }

  inspectUndeclaredEpisode(reference: AgentReference) {
    return this.#controlPlane.inspectUndeclaredEpisode(reference);
  }

  reportUnconfirmedAgentRunExit(_run: ScenarioAgentRun): void {
    // Observation alone is intentionally not lifecycle authority.
  }

  confirmAgentRunExit(run: ScenarioAgentRun, failure: FailedExit): ActivationRecord {
    this.processAdapter.confirmExit(run.processId);
    return this.#controlPlane.failAgentRun(run.ownership, failure);
  }

  cancelActivation(run: ScenarioAgentRun): ActivationRecord {
    this.#controlPlane.writeAgentRunCheckpoint(run.ownership, JSON.stringify({ surface: run.processId }));
    const cancellation = new ActivationCancellationStore(this.workflow.databasePath);
    let operationId: string;
    try {
      const claim = cancellation.claim({
        actor: this.#controlPlane.currentAgent,
        target: this.#controlPlane.agent(run.ownership.agentId),
        sourceId: `${run.ownership.runId}:scenario-cancel`,
        operationId: `${run.ownership.runId}:scenario-cancel`,
        now: this.#now(),
      });
      operationId = claim.operationId;
      this.processAdapter.confirmExit(run.processId);
      cancellation.markReady(operationId, this.#now());
      cancellation.finalize(operationId, this.#now());
    } finally {
      cancellation.close();
    }
    return this.#controlPlane.inspectActivation(this.#controlPlane.agent(run.ownership.agentId))!;
  }

  checkpoint(run: ScenarioAgentRun, value: string): void {
    this.#controlPlane.writeAgentRunCheckpoint(run.ownership, value);
  }

  readCheckpoint(agent: AgentReference): { value: string; fencingEpoch: number } | undefined {
    return this.#controlPlane.readAgentRunCheckpoint(agent);
  }

  exitAgentRun(run: ScenarioAgentRun): void {
    this.confirmAgentRunExit(run, {
      error: "Agent Run exited without committed completion or cancellation",
    });
  }
}

export interface WorkflowScenarioOptions {
  rootDirectory: string;
  clock?: ManualClock;
  identityFactory?: DeterministicIdentityFactory;
  processAdapter?: ControllableProcessAdapter;
  operationReviewPolicy?: OperationReviewPolicy;
}

export class WorkflowScenario {
  readonly rootDirectory: string;
  readonly clock: ManualClock;
  readonly identities: DeterministicIdentityFactory;
  readonly transcripts: ControllableTranscriptAdapter;
  readonly processes: ControllableProcessAdapter;
  readonly operationReviewPolicy: OperationReviewPolicy | undefined;

  constructor(options: WorkflowScenarioOptions) {
    this.rootDirectory = options.rootDirectory;
    this.clock = options.clock ?? new ManualClock();
    this.identities = options.identityFactory ?? new DeterministicIdentityFactory();
    this.transcripts = new ControllableTranscriptAdapter(this.identities, this.clock);
    this.processes = options.processAdapter ?? new ControllableProcessAdapter();
    this.operationReviewPolicy = options.operationReviewPolicy;
  }

  createOwner(name = "Owner"): { session: ScenarioSession; runtime: ControllableRuntimeAdapter } {
    const owner = this.transcripts.create(join(this.rootDirectory, `${name}.jsonl`));
    return { session: owner, runtime: this.startOwner(owner, name) };
  }

  startOwner(owner: ScenarioSession, name = "Owner"): ControllableRuntimeAdapter {
    const open = () => WorkflowControlPlane.startOwner({
      ownerSessionId: owner.agentId,
      ownerSessionPath: owner.sessionPath,
      ownerName: name,
      operationReviewPolicy: this.operationReviewPolicy,
      now: this.clock.now,
    });
    return new ControllableRuntimeAdapter(open(), this.processes, open, this.clock.now);
  }

  startAgent(workflow: WorkflowRecord, agent: ScenarioSession): ControllableRuntimeAdapter {
    const open = () => WorkflowControlPlane.openAgent({
      ownerSessionId: workflow.ownerAgentId,
      ownerSessionPath: workflow.ownerSessionPath,
      agentSessionId: agent.agentId,
      agentSessionPath: agent.sessionPath,
      now: this.clock.now,
    });
    return new ControllableRuntimeAdapter(open(), this.processes, open, this.clock.now);
  }

  childSession(runtime: ControllableRuntimeAdapter, label: string): ScenarioSession {
    return this.#bindSession(
      runtime,
      this.transcripts.create(join(runtime.workflow.sessionsDirectory, `${label}.jsonl`)),
    );
  }

  forkSession(
    runtime: ControllableRuntimeAdapter,
    source: ScenarioSession,
    label: string,
  ): ScenarioSession {
    return this.#bindSession(
      runtime,
      this.transcripts.fork(
        source,
        join(runtime.workflow.sessionsDirectory, `${label}.jsonl`),
      ),
    );
  }

  cloneSession(
    runtime: ControllableRuntimeAdapter,
    source: ScenarioSession,
    label: string,
  ): ScenarioSession {
    return this.#bindSession(
      runtime,
      this.transcripts.clone(
        source,
        join(runtime.workflow.sessionsDirectory, `${label}.jsonl`),
      ),
    );
  }

  #bindSession(runtime: ControllableRuntimeAdapter, session: ScenarioSession): ScenarioSession {
    return {
      ...session,
      workflowBinding: bindNewWorkflowSession({
        workflowOwnerId: runtime.workflow.ownerAgentId,
        agentId: session.agentId,
        sessionPath: session.sessionPath,
      }),
    };
  }
}
