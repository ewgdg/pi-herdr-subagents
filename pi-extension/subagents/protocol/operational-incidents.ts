import { DatabaseSync } from "node:sqlite";
import { WorkflowProtocolError, type AgentReference } from "./workflow-types.ts";
import type { OperationReviewPolicy } from "./operation-review.ts";
import { IncidentBriefBuilder } from "./incident-brief.ts";
import {
  detectDependencyDeadlocks,
  type DetectedDependencyDeadlock,
} from "./dependency-deadlock-detection.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;

export type OperationalIncidentStatus = "open";

export type OperationalIncidentTrigger =
  | {
      kind: "dependency-deadlock";
      episodeId: number;
      seedAgentIds: string[];
      requestIds: string[];
    }
  | {
      kind: "repeated-undeclared-settlement";
      episodeId: string;
      seedAgentIds: [string];
    }
  | {
      kind: "automatic-recovery-exhausted";
      failedActivationId: string;
      exhaustionActivationId: string;
      seedAgentIds: [string];
    }
  | {
      kind: "persistent-operation-uncertainty" | "operation-review-expired";
      operationReviewId: number;
      seedAgentIds: [string];
    };

export interface OperationalIncident {
  incidentId: string;
  episodeKey: string;
  status: OperationalIncidentStatus;
  trigger: OperationalIncidentTrigger;
  scopeAgentIds: string[];
  createdAtMs: number;
  updatedAtMs: number;
}

export interface IncidentBriefAgent {
  agentId: string;
  name: string;
  spawnerAgentId?: string;
  transcriptPointer: string;
  activation?: {
    activationId: string;
    runId: string;
    fencingEpoch: number;
    state: string;
    dependencyPointers: string[];
  };
}

export interface IncidentBriefRequest {
  requestId: string;
  requesterAgentId: string;
  responderAgentId: string;
  status: "open" | "answered" | "orphaned";
}

export interface IncidentBrief {
  incidentId: string;
  operationalQuestion: string;
  trigger: OperationalIncidentTrigger;
  triggerEvidence: unknown;
  scope: {
    roster: IncidentBriefAgent[];
    unresolvedRequests: IncidentBriefRequest[];
  };
  priorRecoveryOrReconciliation: string[];
  applicableReviewPolicy?: OperationReviewPolicy;
  persistedOperationPointers: string[];
  persistedToolCallPointers: string[];
  authorityBoundaries: string[];
  allowedOutcomes: ["operationally-resolved", "owner-handoff"];
  terminationConditions: string[];
  diagnosticPointers: string[];
  createdAtMs: number;
}

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
  session_path: string;
}

interface IncidentRow {
  incident_id: string;
  episode_key: string;
  status: OperationalIncidentStatus;
  trigger_json: string;
  created_at_ms: number;
  updated_at_ms: number;
}

interface DetectedTrigger {
  episodeKey: string;
  trigger: OperationalIncidentTrigger;
}

export function initializeOperationalIncidentSchema(database: DatabaseSync): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS deadlock_detection_episodes (
      deadlock_episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
      component_signature TEXT NOT NULL,
      witness_key TEXT NOT NULL,
      seed_agent_ids_json TEXT NOT NULL,
      active INTEGER NOT NULL CHECK (active IN (0, 1)),
      detected_at_ms INTEGER NOT NULL,
      ended_at_ms INTEGER
    ) STRICT;

    CREATE UNIQUE INDEX IF NOT EXISTS deadlock_detection_one_active_signature
    ON deadlock_detection_episodes (component_signature)
    WHERE active = 1;

    CREATE TABLE IF NOT EXISTS operational_incidents (
      incident_id TEXT PRIMARY KEY,
      episode_key TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (status IN ('open')),
      trigger_json TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE IF NOT EXISTS operational_incident_scope (
      incident_id TEXT NOT NULL REFERENCES operational_incidents(incident_id),
      agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
      added_at_ms INTEGER NOT NULL,
      PRIMARY KEY (incident_id, agent_id)
    ) STRICT;

    CREATE INDEX IF NOT EXISTS operational_incident_scope_agent
    ON operational_incident_scope (agent_id, incident_id);

    CREATE TABLE IF NOT EXISTS operational_incident_briefs (
      incident_id TEXT PRIMARY KEY REFERENCES operational_incidents(incident_id),
      brief_json TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL
    ) STRICT;
  `);
}

/**
 * Owns incident detection and persistence as one deep durable boundary. Raw
 * lifecycle stores expose facts; only this store decides whether those facts
 * form an Operational Incident and what its monotonic scope contains.
 */
export class OperationalIncidentStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    initializeOperationalIncidentSchema(this.#database);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  reconcile(caller: AgentReference, now: number): OperationalIncident[] {
    this.#assertOwner(caller);
    return this.#withImmediateTransaction(() => {
      const deadlocks = detectDependencyDeadlocks(this.#database);
      const deadlockTriggers = this.#reconcileDeadlockEpisodes(deadlocks, now);
      const triggers = [
        ...deadlockTriggers,
        ...this.#repeatedUndeclaredTriggers(),
        ...this.#exhaustedRecoveryTriggers(),
        ...this.#operationReviewTriggers(),
      ];

      for (const detected of triggers) this.#createIncidentIfMissing(detected, now);
      for (const incident of this.#listRows()) this.#expandScope(incident, now);
      return this.list(caller);
    });
  }

  list(caller: AgentReference): OperationalIncident[] {
    this.#assertOwner(caller);
    return this.#listRows().map((row) => this.#mapIncident(row));
  }

  inspect(caller: AgentReference, incidentId: string): OperationalIncident | undefined {
    this.#assertOwner(caller);
    const row = this.#database.prepare(
      "SELECT * FROM operational_incidents WHERE incident_id = ?",
    ).get(incidentId) as IncidentRow | undefined;
    return row ? this.#mapIncident(row) : undefined;
  }

  inspectBrief(caller: AgentReference, incidentId: string): IncidentBrief | undefined {
    this.#assertOwner(caller);
    const row = this.#database.prepare(
      "SELECT brief_json FROM operational_incident_briefs WHERE incident_id = ?",
    ).get(incidentId) as { brief_json: string } | undefined;
    return row ? JSON.parse(row.brief_json) as IncidentBrief : undefined;
  }

  #reconcileDeadlockEpisodes(
    deadlocks: DetectedDependencyDeadlock[],
    now: number,
  ): DetectedTrigger[] {
    const currentBySignature = new Map(deadlocks.map((deadlock) => [
      deadlock.seedAgentIds.join("\n"),
      deadlock,
    ]));
    const active = this.#database.prepare(`
      SELECT deadlock_episode_id, component_signature, witness_key
      FROM deadlock_detection_episodes WHERE active = 1
    `).all() as Array<{
      deadlock_episode_id: number;
      component_signature: string;
      witness_key: string;
    }>;
    for (const episode of active) {
      const current = currentBySignature.get(episode.component_signature);
      if (current?.witnessKey === episode.witness_key) continue;
      // A changed activation revision or Request witness proves that durable
      // progress occurred even when polling did not observe the gap directly.
      this.#database.prepare(`
        UPDATE deadlock_detection_episodes
        SET active = 0, ended_at_ms = ?
        WHERE deadlock_episode_id = ? AND active = 1
      `).run(now, episode.deadlock_episode_id);
    }

    const triggers: DetectedTrigger[] = [];
    for (const deadlock of deadlocks) {
      const signature = deadlock.seedAgentIds.join("\n");
      let episode = this.#database.prepare(`
        SELECT deadlock_episode_id FROM deadlock_detection_episodes
        WHERE component_signature = ? AND active = 1
      `).get(signature) as { deadlock_episode_id: number } | undefined;
      if (!episode) {
        const result = this.#database.prepare(`
          INSERT INTO deadlock_detection_episodes (
            component_signature, witness_key, seed_agent_ids_json,
            active, detected_at_ms, ended_at_ms
          ) VALUES (?, ?, ?, 1, ?, NULL)
        `).run(signature, deadlock.witnessKey, JSON.stringify(deadlock.seedAgentIds), now);
        episode = { deadlock_episode_id: Number(result.lastInsertRowid) };
      }
      triggers.push({
        episodeKey: `dependency-deadlock:${episode.deadlock_episode_id}`,
        trigger: {
          kind: "dependency-deadlock",
          episodeId: Number(episode.deadlock_episode_id),
          seedAgentIds: deadlock.seedAgentIds,
          requestIds: deadlock.requestIds,
        },
      });
    }
    return triggers;
  }

  #repeatedUndeclaredTriggers(): DetectedTrigger[] {
    return (this.#database.prepare(`
      SELECT episode_id, agent_id
      FROM undeclared_settlement_episodes
      WHERE repeat_triggered = 1 AND trigger_kind = 'incident'
      ORDER BY created_at_ms, episode_id
    `).all() as Array<{ episode_id: string; agent_id: string }>).map((row) => ({
      episodeKey: `repeated-undeclared-settlement:${row.episode_id}`,
      trigger: {
        kind: "repeated-undeclared-settlement",
        episodeId: row.episode_id,
        seedAgentIds: [row.agent_id],
      },
    }));
  }

  #exhaustedRecoveryTriggers(): DetectedTrigger[] {
    return (this.#database.prepare(`
      SELECT failed_activation_id, agent_id, exhaustion_activation_id
      FROM activation_recoveries
      WHERE state = 'exhausted'
      ORDER BY created_at_ms, failed_activation_id
    `).all() as Array<{
      failed_activation_id: string;
      agent_id: string;
      exhaustion_activation_id: string;
    }>).map((row) => ({
      episodeKey: `automatic-recovery-exhausted:${row.failed_activation_id}`,
      trigger: {
        kind: "automatic-recovery-exhausted",
        failedActivationId: row.failed_activation_id,
        exhaustionActivationId: row.exhaustion_activation_id,
        seedAgentIds: [row.agent_id],
      },
    }));
  }

  #operationReviewTriggers(): DetectedTrigger[] {
    return (this.#database.prepare(`
      SELECT trigger.trigger_key, trigger.operation_review_id, trigger.reason, review.agent_id
      FROM operation_incident_triggers trigger
      JOIN operation_reviews review ON review.operation_review_id = trigger.operation_review_id
      ORDER BY trigger.triggered_at_ms, trigger.trigger_key
    `).all() as Array<{
      trigger_key: string;
      operation_review_id: number;
      reason: "reconciliation-exhausted" | "review-deadline-expired";
      agent_id: string;
    }>).map((row) => ({
      episodeKey: row.trigger_key,
      trigger: {
        kind: row.reason === "reconciliation-exhausted"
          ? "persistent-operation-uncertainty"
          : "operation-review-expired",
        operationReviewId: Number(row.operation_review_id),
        seedAgentIds: [row.agent_id],
      },
    }));
  }

  #createIncidentIfMissing(detected: DetectedTrigger, now: number): void {
    if (this.#database.prepare(
      "SELECT 1 FROM operational_incidents WHERE episode_key = ?",
    ).get(detected.episodeKey)) return;
    const incidentId = `incident:${detected.episodeKey}`;
    this.#database.prepare(`
      INSERT INTO operational_incidents (
        incident_id, episode_key, status, trigger_json, created_at_ms, updated_at_ms
      ) VALUES (?, ?, 'open', ?, ?, ?)
    `).run(incidentId, detected.episodeKey, JSON.stringify(detected.trigger), now, now);
    const scope = this.#scopeClosure(detected.trigger.seedAgentIds);
    this.#insertScope(incidentId, scope, now);
    const brief = new IncidentBriefBuilder(this.#database).build(
      incidentId,
      detected.trigger,
      scope,
      now,
    );
    this.#database.prepare(`
      INSERT INTO operational_incident_briefs (incident_id, brief_json, created_at_ms)
      VALUES (?, ?, ?)
    `).run(incidentId, JSON.stringify(brief), now);
  }

  #expandScope(incident: IncidentRow, now: number): void {
    const trigger = JSON.parse(incident.trigger_json) as OperationalIncidentTrigger;
    const existing = this.#scopeAgentIds(incident.incident_id);
    const expanded = this.#scopeClosure(trigger.seedAgentIds, existing);
    const added = expanded.filter((agentId) => !existing.includes(agentId));
    if (added.length === 0) return;
    this.#insertScope(incident.incident_id, added, now);
    this.#database.prepare(
      "UPDATE operational_incidents SET updated_at_ms = ? WHERE incident_id = ?",
    ).run(now, incident.incident_id);
  }

  #scopeClosure(seedAgentIds: string[], existingAgentIds: string[] = []): string[] {
    const agents = this.#agents();
    const known = new Set(agents.map((agent) => agent.agent_id));
    const seeds = new Set(seedAgentIds.filter((agentId) => known.has(agentId)));
    const seedDescendants = new Set(seeds);
    let foundDescendant = true;
    while (foundDescendant) {
      foundDescendant = false;
      for (const agent of agents) {
        if (agent.spawner_agent_id
          && seedDescendants.has(agent.spawner_agent_id)
          && !seedDescendants.has(agent.agent_id)) {
          seedDescendants.add(agent.agent_id);
          foundDescendant = true;
        }
      }
    }

    const scope = new Set([
      ...existingAgentIds.filter((agentId) => known.has(agentId)),
      ...seedDescendants,
    ]);
    const requests = this.#unresolvedRequests();
    let foundNeighbor = true;
    while (foundNeighbor) {
      foundNeighbor = false;
      for (const request of requests) {
        if (!scope.has(request.requester_agent_id) && !scope.has(request.responder_agent_id)) continue;
        if (!scope.has(request.requester_agent_id)) {
          scope.add(request.requester_agent_id);
          foundNeighbor = true;
        }
        if (!scope.has(request.responder_agent_id)) {
          scope.add(request.responder_agent_id);
          foundNeighbor = true;
        }
      }
    }
    return [...scope].sort();
  }

  #insertScope(incidentId: string, agentIds: string[], now: number): void {
    const insert = this.#database.prepare(`
      INSERT INTO operational_incident_scope (incident_id, agent_id, added_at_ms)
      VALUES (?, ?, ?)
      ON CONFLICT (incident_id, agent_id) DO NOTHING
    `);
    for (const agentId of agentIds) insert.run(incidentId, agentId, now);
  }

  #scopeAgentIds(incidentId: string): string[] {
    return (this.#database.prepare(`
      SELECT agent_id FROM operational_incident_scope
      WHERE incident_id = ? ORDER BY agent_id
    `).all(incidentId) as Array<{ agent_id: string }>).map((row) => row.agent_id);
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
      SELECT agent_id, name, spawner_agent_id, session_path
      FROM workflow_agents ORDER BY created_at_ms, agent_id
    `).all() as unknown as AgentRow[];
  }

  #listRows(): IncidentRow[] {
    return this.#database.prepare(`
      SELECT incident_id, episode_key, status, trigger_json, created_at_ms, updated_at_ms
      FROM operational_incidents ORDER BY incident_id
    `).all() as unknown as IncidentRow[];
  }

  #mapIncident(row: IncidentRow): OperationalIncident {
    return {
      incidentId: row.incident_id,
      episodeKey: row.episode_key,
      status: row.status,
      trigger: JSON.parse(row.trigger_json) as OperationalIncidentTrigger,
      scopeAgentIds: this.#scopeAgentIds(row.incident_id),
      createdAtMs: Number(row.created_at_ms),
      updatedAtMs: Number(row.updated_at_ms),
    };
  }

  #assertOwner(caller: AgentReference): void {
    const owner = this.#database.prepare(
      "SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1",
    ).get() as { owner_agent_id: string } | undefined;
    if (!owner || caller.workflowOwnerId !== owner.owner_agent_id || caller.agentId !== owner.owner_agent_id) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        "Only the Workflow Owner can reconcile or inspect Operational Incidents",
      );
    }
  }

  #withImmediateTransaction<T>(operation: () => T): T {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }
}
