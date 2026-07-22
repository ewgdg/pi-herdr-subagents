import type { ActivationRecord, HumanInterruptRecord, UndeclaredSettlementEpisode } from "./activation-lifecycle.ts";
import type { DirectSignalRecord, RequestRecord } from "./direct-signal-types.ts";
import type { SQLiteWorkflowStore } from "./sqlite-workflow-store.ts";
import { WorkflowProtocolError, type AgentRecord, type AgentReference, type WorkflowRecord } from "./workflow-types.ts";

export type InspectionTarget =
  | { agent: string }
  | { request: string }
  | { directChildren: true }
  | { workflow: true };

export interface InspectionSources {
  workflow: WorkflowRecord;
  caller: AgentReference;
  agents: Pick<SQLiteWorkflowStore, "inspectAgent" | "listDirectChildren" | "listWorkflow">;
  inspectActivation(agent: AgentReference): ActivationRecord | undefined;
  inspectHumanInterrupt(agent: AgentReference): HumanInterruptRecord | undefined;
  inspectUndeclaredEpisode(agent: AgentReference): UndeclaredSettlementEpisode | undefined;
  inspectRequestProjection(requestId: string): {
    request: RequestRecord;
    requestDeliveryStatus?: DirectSignalRecord["deliveryStatus"];
    answerDeliveryStatus?: DirectSignalRecord["deliveryStatus"];
  } | undefined;
  now(): number;
}

export class WorkflowInspection {
  readonly #sources: InspectionSources;

  constructor(sources: InspectionSources) { this.#sources = sources; }

  inspect(target: InspectionTarget): unknown {
    if ("agent" in target) return this.#projectKnownAgent(target.agent);
    if ("request" in target) return this.#projectKnownRequest(target.request);
    if ("directChildren" in target) {
      return { kind: "agent-list", scope: "direct-children", agents: this.#sources.agents
        .listDirectChildren(this.#sources.workflow.ownerAgentId, this.#sources.caller.agentId)
        .map((agent) => this.#projectAgent(agent)) };
    }
    if (this.#sources.caller.agentId !== this.#sources.workflow.ownerAgentId) {
      throw new WorkflowProtocolError("WorkflowMismatch", "Current Agent cannot enumerate this Workflow");
    }
    return { kind: "agent-list", scope: "workflow", agents: this.#sources.agents
      .listWorkflow(this.#sources.workflow.ownerAgentId)
      .map((agent) => this.#projectAgent(agent)) };
  }

  #projectKnownAgent(agentId: string): unknown {
    let agent: AgentRecord;
    try {
      agent = this.#sources.agents.inspectAgent(this.#sources.workflow.ownerAgentId, agentId);
    } catch (error) {
      if (!(error instanceof WorkflowProtocolError) || error.code !== "UnknownAgent") throw error;
      throw new WorkflowProtocolError("UnknownAgent", "Agent is not inspectable in the current Workflow");
    }
    return this.#projectAgent(agent);
  }

  #projectAgent(agent: AgentRecord): unknown {
    const reference = { workflowOwnerId: this.#sources.workflow.ownerAgentId, agentId: agent.agentId };
    const activation = agent.agentId === this.#sources.workflow.ownerAgentId
      ? undefined
      : this.#sources.inspectActivation(reference);
    const human = activation ? this.#sources.inspectHumanInterrupt(reference) : undefined;
    const undeclared = activation ? this.#sources.inspectUndeclaredEpisode(reference) : undefined;
    const dependencies = activation?.state.kind === "waiting"
      ? activation.state.dependencies.map((dependency) => dependency.kind === "agent"
        ? { kind: "agent" as const, dependencyId: dependency.dependencyId, agentId: dependency.agentId }
        : dependency.kind === "operation"
          ? { kind: "operation" as const, dependencyId: dependency.dependencyId }
          : { kind: dependency.kind })
      : [];
    return {
      kind: "agent",
      agentId: agent.agentId,
      name: agent.name,
      ...(agent.agentDefinition ? { definition: agent.agentDefinition } : {}),
      role: agent.agentId === this.#sources.workflow.ownerAgentId
        ? "workflow-owner"
        : agent.agentDefinition === "moderator" ? "moderator" : "ordinary",
      state: activation ? projectActivationState(activation) : { kind: agent.agentId === this.#sources.workflow.ownerAgentId ? "owner" : "inactive" },
      elapsedMs: activationDurationMs(activation, this.#sources.now()),
      ...(dependencies.length ? { waitingReason: waitingReason(dependencies), dependencies } : {}),
      callerAuthority: authority(this.#sources, agent),
      transcriptPath: agent.sessionPath,
      ...(human && (human.status === "pending" || human.status === "response-bound" || human.status === "result-pending")
        ? { humanInterrupt: { state: human.status === "pending" ? "awaiting-response" : "response-bound-awaiting-resume" } }
        : {}),
      ...(undeclared ? { undeclaredSettlement: {
        status: undeclared.status,
        allowanceConsumed: true,
        repeatTriggered: undeclared.repeatTriggered,
      } } : {}),
    };
  }

  #projectKnownRequest(requestId: string): unknown {
    const projection = this.#sources.inspectRequestProjection(requestId);
    if (!projection) throw new WorkflowProtocolError("UnknownRequest", "Request is not inspectable in the current Workflow");
    const { request, requestDeliveryStatus, answerDeliveryStatus } = projection;
    const callerId = this.#sources.caller.agentId;
    const requester = { workflowOwnerId: this.#sources.workflow.ownerAgentId, agentId: request.requesterAgentId };
    const requesterActivation = request.requesterAgentId === this.#sources.workflow.ownerAgentId
      ? undefined : this.#sources.inspectActivation(requester);
    const dependency = requesterActivation?.state.kind === "waiting"
      ? requesterActivation.state.dependencies.find((item) => item.kind === "agent" && item.dependencyId === request.requestId)
      : undefined;
    return {
      kind: "request",
      requestId: request.requestId,
      correlation: { requesterAgentId: request.requesterAgentId, responderAgentId: request.responderAgentId },
      status: request.status,
      answer: request.answerMessageId ? { messageId: request.answerMessageId } : null,
      delivery: {
        request: requestDeliveryStatus ?? "unknown",
        answer: answerDeliveryStatus ?? (request.answerMessageId ? "unknown" : "not-created"),
      },
      ...(request.cancellationNotice ? { cancellation: {
        noticeMessageId: request.cancellationNotice.messageId,
        delivery: request.cancellationNotice.deliveryStatus,
      } } : {}),
      requesterDependency: request.status === "resolved" || request.status === "cancelled" ? "satisfied" : "unresolved",
      requesterLifecycleDependency: dependency ? "waiting" : "not-waiting",
      callerAuthority: {
        inspect: true,
        relationship: callerId === this.#sources.workflow.ownerAgentId ? "workflow-owner"
          : callerId === request.requesterAgentId ? "requester"
            : callerId === request.responderAgentId ? "responder" : "known-request",
        workflowOwner: callerId === this.#sources.workflow.ownerAgentId,
        requester: callerId === request.requesterAgentId,
        responder: callerId === request.responderAgentId,
        cancelRequest: callerId === request.requesterAgentId && request.status === "open",
      },
    };
  }
}

function authority(sources: InspectionSources, agent: AgentRecord) {
  const callerId = sources.caller.agentId;
  return {
    inspect: true,
    relationship: callerId === agent.agentId ? "self"
      : callerId === sources.workflow.ownerAgentId ? "workflow-owner"
      : agent.spawnerAgentId === callerId ? "spawner" : "known-agent",
    enumerateDirectChildren: callerId === agent.agentId,
    enumerateWorkflow: callerId === sources.workflow.ownerAgentId,
  };
}

function waitingReason(dependencies: Array<{ kind: string }>): string {
  if (dependencies.some((item) => item.kind === "human")) return "human-interrupt";
  if (dependencies.some((item) => item.kind === "undeclared")) return "undeclared-settlement-correction";
  if (dependencies.some((item) => item.kind === "operation")) return "operation-dependency";
  return "agent-dependency";
}

function projectActivationState(activation: ActivationRecord): object {
  const state = activation.state;
  if (state.kind !== "ended") return { kind: state.kind };
  return { kind: "ended", outcome: state.outcome };
}

function activationDurationMs(activation: ActivationRecord | undefined, now: number): number {
  if (!activation) return 0;
  if (activation.state.kind === "ended") return Math.max(0, activation.updatedAtMs - activation.createdAtMs);
  return Math.max(0, now - activation.createdAtMs);
}
