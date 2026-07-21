import {
  SQLiteCoordinationStore,
  type OwnershipToken,
} from "../coordination/sqlite-coordination.ts";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";

const CHECKPOINT_STATE_KEY = "agent-run-checkpoint";

export class AgentRunOwnershipStore {
  readonly #coordination: SQLiteCoordinationStore;

  constructor(databasePath: string) {
    this.#coordination = new SQLiteCoordinationStore(databasePath);
  }

  close(): void {
    this.#coordination.close();
  }

  acquire(agent: AgentReference, runId: string): AgentRunOwnership {
    const resourceId = resourceIdFor(agent);
    const acquisition = this.#coordination.acquireOwnership(resourceId, runId);
    if (!acquisition.acquired) {
      throw new WorkflowProtocolError(
        "AgentRunAlreadyOwned",
        `Agent ${agent.agentId} is already owned by Agent Run ${acquisition.currentOwner.ownerId}`,
      );
    }
    return {
      ...agent,
      runId,
      resourceId,
      epoch: acquisition.token.epoch,
    };
  }

  release(ownership: AgentRunOwnership): void {
    if (!this.#coordination.releaseOwnership(toCoordinationToken(ownership))) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Agent Run no longer owns ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
      );
    }
  }

  assertCurrent(ownership: AgentRunOwnership): void {
    if (!this.#coordination.owns(toCoordinationToken(ownership))) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Agent Run no longer owns ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
      );
    }
  }

  current(agent: AgentReference): AgentRunOwnership | undefined {
    const token = this.#coordination.readOwnership(resourceIdFor(agent));
    return token
      ? {
          ...agent,
          runId: token.ownerId,
          resourceId: token.resourceId,
          epoch: token.epoch,
        }
      : undefined;
  }

  writeCheckpoint(ownership: AgentRunOwnership, value: string): void {
    if (!this.#coordination.writeFencedState(
      toCoordinationToken(ownership),
      CHECKPOINT_STATE_KEY,
      value,
    )) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Agent Run lost ownership of ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
      );
    }
  }

  readCheckpoint(agent: AgentReference): { value: string; fencingEpoch: number } | undefined {
    return this.#coordination.readFencedState(resourceIdFor(agent), CHECKPOINT_STATE_KEY);
  }
}

function resourceIdFor(agent: AgentReference): string {
  return `agent-run:${agent.workflowOwnerId}:${agent.agentId}`;
}

function toCoordinationToken(ownership: AgentRunOwnership): OwnershipToken {
  const expectedResourceId = resourceIdFor(ownership);
  if (ownership.resourceId !== expectedResourceId) {
    throw new WorkflowProtocolError(
      "WorkflowMismatch",
      `Agent Run ownership token does not belong to Workflow ${ownership.workflowOwnerId}`,
    );
  }
  return {
    resourceId: ownership.resourceId,
    ownerId: ownership.runId,
    epoch: ownership.epoch,
  };
}
