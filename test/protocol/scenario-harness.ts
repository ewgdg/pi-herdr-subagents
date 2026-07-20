import { readFileSync } from "node:fs";
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

  constructor(
    controlPlane: WorkflowControlPlane,
    processAdapter: ControllableProcessAdapter,
    reopen: () => WorkflowControlPlane,
  ) {
    this.#controlPlane = controlPlane;
    this.processAdapter = processAdapter;
    this.#reopen = reopen;
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

  inspect(reference: AgentReference): AgentRecord {
    return this.#controlPlane.inspectAgent(reference);
  }

  descendants(): AgentRecord[] {
    return this.#controlPlane.listWorkflow(this.#controlPlane.owner);
  }

  directChildren(spawner: AgentReference): AgentRecord[] {
    return this.#controlPlane.listDirectChildren(spawner);
  }

  addAgent(input: {
    session: ScenarioSession;
    spawner: AgentReference;
    name: string;
    agentDefinition?: string;
    capabilities?: AgentCapabilityConfiguration;
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

  settleActivation(run: ScenarioAgentRun, expectedRevision?: number): ActivationRecord {
    return this.#controlPlane.settleActivation(run.ownership, expectedRevision);
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

  reportUnconfirmedAgentRunExit(_run: ScenarioAgentRun): void {
    // Observation alone is intentionally not lifecycle authority.
  }

  confirmAgentRunExit(run: ScenarioAgentRun, failure: FailedExit): ActivationRecord {
    this.processAdapter.confirmExit(run.processId);
    return this.#controlPlane.failAgentRun(run.ownership, failure);
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
}

export class WorkflowScenario {
  readonly rootDirectory: string;
  readonly clock: ManualClock;
  readonly identities: DeterministicIdentityFactory;
  readonly transcripts: ControllableTranscriptAdapter;
  readonly processes: ControllableProcessAdapter;

  constructor(options: WorkflowScenarioOptions) {
    this.rootDirectory = options.rootDirectory;
    this.clock = options.clock ?? new ManualClock();
    this.identities = options.identityFactory ?? new DeterministicIdentityFactory();
    this.transcripts = new ControllableTranscriptAdapter(this.identities, this.clock);
    this.processes = options.processAdapter ?? new ControllableProcessAdapter();
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
      now: this.clock.now,
    });
    return new ControllableRuntimeAdapter(open(), this.processes, open);
  }

  startAgent(workflow: WorkflowRecord, agent: ScenarioSession): ControllableRuntimeAdapter {
    const open = () => WorkflowControlPlane.openAgent({
      ownerSessionId: workflow.ownerAgentId,
      ownerSessionPath: workflow.ownerSessionPath,
      agentSessionId: agent.agentId,
      agentSessionPath: agent.sessionPath,
      now: this.clock.now,
    });
    return new ControllableRuntimeAdapter(open(), this.processes, open);
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
