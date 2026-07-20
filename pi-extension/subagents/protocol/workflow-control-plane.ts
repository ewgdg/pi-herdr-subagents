import { existsSync, realpathSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { AgentRunOwnershipStore } from "./agent-run-ownership.ts";
import { readPiSessionUuid, assertSessionUuid } from "./workflow-identity.ts";
import { assertDescendantTranscriptPath, createWorkflowLayout } from "./workflow-layout.ts";
import { SQLiteWorkflowStore } from "./sqlite-workflow-store.ts";
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

export type {
  AgentCapabilityConfiguration,
  AgentRecord,
  AgentReference,
  AgentRunOwnership,
  WorkflowRecord,
} from "./workflow-types.ts";
export { WorkflowProtocolError } from "./workflow-types.ts";

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
  sessionBinding: WorkflowSessionBinding;
}

export class WorkflowControlPlane {
  readonly workflow: WorkflowRecord;
  readonly #store: SQLiteWorkflowStore;
  readonly #ownership: AgentRunOwnershipStore;
  readonly #now: () => number;
  readonly #currentAgentId: string;
  #closed = false;

  private constructor(
    workflow: WorkflowRecord,
    store: SQLiteWorkflowStore,
    ownership: AgentRunOwnershipStore,
    now: () => number,
    currentAgentId: string,
  ) {
    this.workflow = workflow;
    this.#store = store;
    this.#ownership = ownership;
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
      createdAtMs: this.#now(),
    });
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
    this.#ownership.release(ownership);
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
