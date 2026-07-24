import type { DatabaseSync } from "node:sqlite";
import type { OperationReviewPolicy } from "./operation-review.ts";
import type {
  IncidentBrief,
  IncidentBriefAgent,
  OperationalIncidentTrigger,
} from "./operational-incidents.ts";

interface RequestRow {
  request_id: string;
  requester_agent_id: string;
  responder_agent_id: string;
  requester_activation_id: string | null;
  status: "open" | "answered" | "orphaned";
}

interface AgentRow {
  agent_id: string;
  name: string;
  spawner_agent_id: string | null;
}

/** Builds one compact, immutable creation-time snapshot from durable facts. */
export class IncidentBriefBuilder {
  readonly #database: DatabaseSync;

  constructor(database: DatabaseSync) {
    this.#database = database;
  }

  build(
    incidentId: string,
    trigger: OperationalIncidentTrigger,
    scopeAgentIds: string[],
    now: number,
  ): IncidentBrief {
    const scoped = new Set(scopeAgentIds);
    const roster = this.#agents()
      .filter((agent) => scoped.has(agent.agent_id))
      .map((agent) => ({
        agentId: agent.agent_id,
        name: agent.name,
        ...(agent.spawner_agent_id ? { spawnerAgentId: agent.spawner_agent_id } : {}),
        transcriptPointer: `agent-transcript:${agent.agent_id}`,
        ...this.#activationBrief(agent.agent_id),
      }));
    const unresolvedRequests = this.#unresolvedRequests()
      .filter((request) => scoped.has(request.requester_agent_id) || scoped.has(request.responder_agent_id))
      .map((request) => ({
        requestId: request.request_id,
        requesterAgentId: request.requester_agent_id,
        responderAgentId: request.responder_agent_id,
        status: request.status,
      }));
    const evidence = this.#triggerEvidence(trigger);
    const operationReviewIds = this.#relevantOperationReviewIds(trigger, scopeAgentIds);
    const prior = this.#priorRecoveryOrReconciliation(trigger, operationReviewIds);
    const operationPointers = this.#operationPointers(trigger, operationReviewIds);
    const toolPointers = this.#toolCallPointers(scopeAgentIds);
    const policy = operationReviewIds.length > 0 ? this.#operationReviewPolicy() : undefined;
    return {
      incidentId,
      operationalQuestion: operationalQuestion(trigger.kind),
      trigger,
      triggerEvidence: evidence,
      scope: { roster, unresolvedRequests },
      priorRecoveryOrReconciliation: prior,
      ...(policy ? { applicableReviewPolicy: policy } : {}),
      persistedOperationPointers: operationPointers,
      persistedToolCallPointers: toolPointers,
      authorityBoundaries: [
        "Incident Control is limited to non-Owner Agents in the persisted Incident Scope.",
        "A Moderator cannot answer or cancel another Agent's Request, alter identity or transcripts, or widen scope manually.",
        "Domain judgment, policy changes, uncertain irreversible effects, and control of the Workflow Owner require Owner Handoff.",
      ],
      allowedOutcomes: ["operationally-resolved", "owner-handoff"],
      terminationConditions: [
        "Operational resolution requires runtime verification that the blocking or unsafe condition has cleared.",
        "Owner Handoff requires durable acceptance of the incident escalation before Moderator authority can end.",
        "Every Moderator-created Request must be answered or cancelled before a voluntary outcome.",
      ],
      diagnosticPointers: [
        `operational-incident:${incidentId}`,
        ...scopeAgentIds.map((agentId) => `workflow-agent:${agentId}`),
        ...unresolvedRequests.map((request) => `workflow-request:${request.requestId}`),
        ...operationPointers,
        ...toolPointers,
      ],
      createdAtMs: now,
    };
  }

  #triggerEvidence(trigger: OperationalIncidentTrigger): unknown {
    if (trigger.kind === "dependency-deadlock") {
      return {
        closedComponentAgentIds: trigger.seedAgentIds,
        unresolvedInternalRequestIds: trigger.requestIds,
        durableCondition: "every Agent is waiting solely on unresolved Requests within the component",
      };
    }
    if (trigger.kind === "repeated-undeclared-settlement") {
      const row = this.#database.prepare(`
        SELECT notice_accepted, notice_delivered, repeat_triggered
        FROM undeclared_settlement_episodes WHERE episode_id = ?
      `).get(trigger.episodeId) as {
        notice_accepted: number;
        notice_delivered: number;
        repeat_triggered: number;
      };
      const declaredDependencyPointers = (this.#database.prepare(`
        SELECT dependency_kind, dependency_id
        FROM undeclared_settlement_dependencies
        WHERE episode_id = ? ORDER BY dependency_kind, dependency_id
      `).all(trigger.episodeId) as Array<{
        dependency_kind: string;
        dependency_id: string;
      }>).map((dependency) => `${dependency.dependency_kind}:${dependency.dependency_id}`);
      return {
        episodeId: trigger.episodeId,
        correctionNoticeId: `${trigger.episodeId}:notice`,
        correctionNoticeAccepted: Number(row.notice_accepted) === 1,
        correctionNoticeDelivered: Number(row.notice_delivered) === 1,
        correctionAllowanceConsumed: Number(row.repeat_triggered) === 1,
        declaredDependencyPointers,
      };
    }
    if (trigger.kind === "automatic-recovery-exhausted") {
      const row = this.#database.prepare(`
        SELECT detail FROM activation_recoveries WHERE failed_activation_id = ?
      `).get(trigger.failedActivationId) as { detail: string | null };
      return {
        failedActivationId: trigger.failedActivationId,
        exhaustionActivationId: trigger.exhaustionActivationId,
        detail: row.detail,
      };
    }
    const row = this.#database.prepare(`
      SELECT dependency_id, operation_kind, original_identity, status,
             review_deadline_at_ms, reconciliation_attempts
      FROM operation_reviews WHERE operation_review_id = ?
    `).get(trigger.operationReviewId) as Record<string, unknown>;
    return {
      operationReviewId: trigger.operationReviewId,
      dependencyId: row.dependency_id,
      operationKind: row.operation_kind,
      originalIdentity: row.original_identity,
      status: row.status,
      reviewDeadlineAtMs: Number(row.review_deadline_at_ms),
      reconciliationAttempts: Number(row.reconciliation_attempts),
    };
  }

  #relevantOperationReviewIds(
    trigger: OperationalIncidentTrigger,
    scopeAgentIds: string[],
  ): number[] {
    const ids = new Set<number>();
    if ("operationReviewId" in trigger) ids.add(trigger.operationReviewId);
    if (scopeAgentIds.length > 0) {
      const placeholders = scopeAgentIds.map(() => "?").join(", ");
      for (const row of this.#database.prepare(`
        SELECT operation_review_id FROM operation_reviews
        WHERE agent_id IN (${placeholders}) AND status != 'resolved'
        ORDER BY operation_review_id
      `).all(...scopeAgentIds) as Array<{ operation_review_id: number }>) {
        ids.add(Number(row.operation_review_id));
      }
    }
    return [...ids].sort((left, right) => left - right);
  }

  #priorRecoveryOrReconciliation(
    trigger: OperationalIncidentTrigger,
    operationReviewIds: number[],
  ): string[] {
    const prior: string[] = [];
    if (trigger.kind === "automatic-recovery-exhausted") {
      const row = this.#database.prepare(`
        SELECT replacement_activation_id, exhaustion_activation_id, detail
        FROM activation_recoveries WHERE failed_activation_id = ?
      `).get(trigger.failedActivationId) as {
        replacement_activation_id: string;
        exhaustion_activation_id: string;
        detail: string | null;
      };
      prior.push(
        `automatic-recovery:${trigger.failedActivationId}`,
        `replacement-activation:${row.replacement_activation_id}`,
        `exhaustion-activation:${row.exhaustion_activation_id}`,
        ...(row.detail ? [row.detail] : []),
      );
    }
    for (const operationReviewId of operationReviewIds) {
      prior.push(...(this.#database.prepare(`
        SELECT evidence_kind, detail, observed_at_ms
        FROM operation_review_evidence
        WHERE operation_review_id = ?
        ORDER BY evidence_sequence
      `).all(operationReviewId) as Array<{
        evidence_kind: string;
        detail: string;
        observed_at_ms: number;
      }>).map((row) =>
        `operation-review:${operationReviewId}:${row.evidence_kind}@${row.observed_at_ms}: ${row.detail}`));
    }
    return prior;
  }

  #operationPointers(
    trigger: OperationalIncidentTrigger,
    operationReviewIds: number[],
  ): string[] {
    const pointers = trigger.kind === "automatic-recovery-exhausted"
      ? [`activation-recovery:${trigger.failedActivationId}`]
      : [];
    for (const operationReviewId of operationReviewIds) {
      const row = this.#database.prepare(`
        SELECT dependency_id, operation_kind, original_identity
        FROM operation_reviews WHERE operation_review_id = ?
      `).get(operationReviewId) as {
        dependency_id: string;
        operation_kind: string;
        original_identity: string;
      };
      pointers.push(
        `operation-review:${operationReviewId}`,
        `operation-dependency:${row.dependency_id}`,
        `${row.operation_kind}-operation:${row.original_identity}`,
      );
    }
    return [...new Set(pointers)];
  }

  #toolCallPointers(scopeAgentIds: string[]): string[] {
    if (scopeAgentIds.length === 0) return [];
    const placeholders = scopeAgentIds.map(() => "?").join(", ");
    return (this.#database.prepare(`
      SELECT agent_id, tool_call_id FROM human_interrupts
      WHERE agent_id IN (${placeholders})
        AND status IN ('pending', 'response-bound', 'result-pending')
      ORDER BY agent_id, created_at_ms, tool_call_id
    `).all(...scopeAgentIds) as Array<{ agent_id: string; tool_call_id: string }>).map(
      (row) => `human-tool-call:${row.agent_id}:${row.tool_call_id}`,
    );
  }

  #activationBrief(agentId: string): Pick<IncidentBriefAgent, "activation"> | Record<string, never> {
    const activation = this.#database.prepare(`
      SELECT activation_id, run_id, fencing_epoch, phase, open_state, ended_outcome
      FROM agent_activations WHERE agent_id = ?
      ORDER BY activation_sequence DESC LIMIT 1
    `).get(agentId) as {
      activation_id: string;
      run_id: string;
      fencing_epoch: number;
      phase: "open" | "ended";
      open_state: string | null;
      ended_outcome: string | null;
    } | undefined;
    if (!activation) return {};
    const dependencyPointers = (this.#database.prepare(`
      SELECT dependency_kind, dependency_id FROM activation_dependencies
      WHERE activation_id = ? ORDER BY dependency_kind, dependency_id
    `).all(activation.activation_id) as Array<{
      dependency_kind: string;
      dependency_id: string;
    }>).map((dependency) => `${dependency.dependency_kind}:${dependency.dependency_id}`);
    for (const request of this.#unresolvedRequests()) {
      if (request.requester_activation_id === activation.activation_id) {
        dependencyPointers.push(`request:${request.request_id}`);
      }
    }
    const human = this.#database.prepare(`
      SELECT tool_call_id FROM human_interrupts
      WHERE agent_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
      ORDER BY created_at_ms DESC LIMIT 1
    `).get(agentId) as { tool_call_id: string } | undefined;
    if (human) dependencyPointers.push(`human-tool-call:${human.tool_call_id}`);
    const undeclared = this.#database.prepare(`
      SELECT episode_id FROM undeclared_settlement_episodes
      WHERE agent_id = ? AND status = 'open'
    `).get(agentId) as { episode_id: string } | undefined;
    if (undeclared) dependencyPointers.push(`undeclared-settlement:${undeclared.episode_id}`);
    return {
      activation: {
        activationId: activation.activation_id,
        runId: activation.run_id,
        fencingEpoch: Number(activation.fencing_epoch),
        state: activation.phase === "open" ? activation.open_state! : `ended:${activation.ended_outcome}`,
        dependencyPointers,
      },
    };
  }

  #operationReviewPolicy(): OperationReviewPolicy {
    const row = this.#database.prepare(
      "SELECT * FROM workflow_operation_review_policy WHERE singleton = 1",
    ).get() as Record<string, number>;
    return {
      maximumUnattendedIntervalMs: Number(row.maximum_unattended_interval_ms),
      intervalsMs: {
        acceptance: Number(row.acceptance_interval_ms),
        cancellation: Number(row.cancellation_interval_ms),
        ownership: Number(row.ownership_interval_ms),
        "external-side-effect": Number(row.external_side_effect_interval_ms),
        generic: Number(row.generic_interval_ms),
      },
    };
  }

  #unresolvedRequests(): RequestRow[] {
    return this.#database.prepare(`
      SELECT request_id, requester_agent_id, responder_agent_id,
             requester_activation_id, status
      FROM workflow_requests
      WHERE status IN ('open', 'answered')
        OR (status = 'orphaned' AND orphan_notice_delivery_status = 'accepted')
      ORDER BY request_id
    `).all() as unknown as RequestRow[];
  }

  #agents(): AgentRow[] {
    return this.#database.prepare(`
      SELECT agent_id, name, spawner_agent_id
      FROM workflow_agents ORDER BY created_at_ms, agent_id
    `).all() as unknown as AgentRow[];
  }
}

function operationalQuestion(kind: OperationalIncidentTrigger["kind"]): string {
  switch (kind) {
    case "dependency-deadlock":
      return "How should the confirmed Dependency Deadlock be cleared without violating Request ownership?";
    case "repeated-undeclared-settlement":
      return "How should repeated Undeclared Settlement be resolved after its correction allowance was consumed?";
    case "automatic-recovery-exhausted":
      return "Can the recovery-pending work be recovered safely, or must control pass to the Workflow Owner?";
    case "persistent-operation-uncertainty":
      return "Can persistent operational uncertainty be reconciled safely, renewed under policy, or handed to the Workflow Owner?";
    case "operation-review-expired":
      return "Can the expired unresolved Operation Dependency be reconciled safely, renewed under policy, or handed to the Workflow Owner?";
  }
}
