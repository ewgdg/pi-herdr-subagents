import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentReference,
} from "./workflow-types.ts";

const SECOND_MS = 1_000;
const MINUTE_MS = 60 * SECOND_MS;
const DEFAULT_BUSY_TIMEOUT_MS = 5 * SECOND_MS;

export type OperationKind =
  | "acceptance"
  | "cancellation"
  | "ownership"
  | "external-side-effect"
  | "generic";

export interface OperationReviewPolicy {
  maximumUnattendedIntervalMs: number;
  intervalsMs: Record<OperationKind, number>;
}

export const DEFAULT_OPERATION_REVIEW_POLICY: OperationReviewPolicy = {
  maximumUnattendedIntervalMs: 15 * MINUTE_MS,
  intervalsMs: {
    acceptance: 2 * MINUTE_MS,
    cancellation: 5 * MINUTE_MS,
    ownership: 2 * MINUTE_MS,
    "external-side-effect": 15 * MINUTE_MS,
    generic: 10 * MINUTE_MS,
  },
};

export type OperationReviewStatus = "reconciling" | "awaiting-judgment" | "resolved";

export interface OperationEvidence {
  kind: string;
  detail: string;
  observedAtMs: number;
}

export interface OperationReviewRecord {
  operationReviewId: number;
  dependencyId: string;
  operationKind: OperationKind;
  originalIdentity: string;
  agentId: string;
  activationId: string;
  ownership: {
    runId: string;
    fencingEpoch: number;
  };
  status: OperationReviewStatus;
  reviewStartedAtMs: number;
  reviewDeadlineAtMs: number;
  reconciliationAttempts: number;
  evidenceCount: number;
  latestEvidence?: OperationEvidence;
}

export interface WatchAttention {
  kind: "WATCH";
  key: string;
  operationReviewId: number;
  agentId: string;
  dependencyId: string;
  reviewDeadlineAtMs: number;
}

export interface OperationIncidentTrigger {
  triggerKey: string;
  operationReviewId: number;
  dependencyId: string;
  reason: "reconciliation-exhausted" | "review-deadline-expired";
  triggeredAtMs: number;
}

export type OperationReconciliationOutcome =
  | {
      kind: "resolved";
      evidence: Omit<OperationEvidence, "observedAtMs">;
    }
  | {
      kind: "unresolved";
      eligibility: "eligible" | "exhausted";
      evidence: Omit<OperationEvidence, "observedAtMs">;
    };

interface OperationReviewRow {
  operation_review_id: number;
  dependency_id: string;
  operation_kind: OperationKind;
  original_identity: string;
  agent_id: string;
  original_activation_id: string;
  current_activation_id: string;
  run_id: string;
  fencing_epoch: number;
  status: OperationReviewStatus;
  review_started_at_ms: number;
  review_deadline_at_ms: number;
  reconciliation_attempts: number;
}

interface OperationEvidenceRow {
  evidence_kind: string;
  detail: string;
  observed_at_ms: number;
}

export function initializeOperationReviewSchema(database: DatabaseSync): void {
  const defaults = DEFAULT_OPERATION_REVIEW_POLICY;
  database.exec(`
    CREATE TABLE IF NOT EXISTS workflow_operation_review_policy (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      maximum_unattended_interval_ms INTEGER NOT NULL CHECK (maximum_unattended_interval_ms > 0),
      acceptance_interval_ms INTEGER NOT NULL CHECK (acceptance_interval_ms > 0),
      cancellation_interval_ms INTEGER NOT NULL CHECK (cancellation_interval_ms > 0),
      ownership_interval_ms INTEGER NOT NULL CHECK (ownership_interval_ms > 0),
      external_side_effect_interval_ms INTEGER NOT NULL CHECK (external_side_effect_interval_ms > 0),
      generic_interval_ms INTEGER NOT NULL CHECK (generic_interval_ms > 0)
    ) STRICT;

    INSERT OR IGNORE INTO workflow_operation_review_policy (
      singleton, maximum_unattended_interval_ms, acceptance_interval_ms,
      cancellation_interval_ms, ownership_interval_ms,
      external_side_effect_interval_ms, generic_interval_ms
    ) VALUES (
      1,
      ${defaults.maximumUnattendedIntervalMs},
      ${defaults.intervalsMs.acceptance},
      ${defaults.intervalsMs.cancellation},
      ${defaults.intervalsMs.ownership},
      ${defaults.intervalsMs["external-side-effect"]},
      ${defaults.intervalsMs.generic}
    );

    CREATE TABLE IF NOT EXISTS operation_reviews (
      operation_review_id INTEGER PRIMARY KEY AUTOINCREMENT,
      dependency_id TEXT NOT NULL,
      operation_kind TEXT NOT NULL CHECK (
        operation_kind IN ('acceptance', 'cancellation', 'ownership', 'external-side-effect', 'generic')
      ),
      original_identity TEXT NOT NULL,
      agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
      original_activation_id TEXT NOT NULL REFERENCES agent_activations(activation_id),
      current_activation_id TEXT NOT NULL REFERENCES agent_activations(activation_id),
      run_id TEXT NOT NULL,
      fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0),
      status TEXT NOT NULL CHECK (status IN ('reconciling', 'awaiting-judgment', 'resolved')),
      review_started_at_ms INTEGER NOT NULL,
      review_deadline_at_ms INTEGER NOT NULL,
      reconciliation_attempts INTEGER NOT NULL DEFAULT 0 CHECK (reconciliation_attempts >= 0),
      resolved_at_ms INTEGER,
      CHECK ((status = 'resolved' AND resolved_at_ms IS NOT NULL)
        OR (status != 'resolved' AND resolved_at_ms IS NULL))
    ) STRICT;

    CREATE INDEX IF NOT EXISTS operation_reviews_agent_status
    ON operation_reviews (agent_id, status, review_deadline_at_ms);

    CREATE UNIQUE INDEX IF NOT EXISTS operation_reviews_one_unresolved_dependency
    ON operation_reviews (current_activation_id, dependency_id)
    WHERE status != 'resolved';

    CREATE TABLE IF NOT EXISTS operation_review_evidence (
      evidence_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      operation_review_id INTEGER NOT NULL REFERENCES operation_reviews(operation_review_id),
      evidence_kind TEXT NOT NULL,
      detail TEXT NOT NULL,
      observed_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE INDEX IF NOT EXISTS operation_review_evidence_operation
    ON operation_review_evidence (operation_review_id, evidence_sequence);

    CREATE TABLE IF NOT EXISTS operation_incident_triggers (
      trigger_key TEXT PRIMARY KEY,
      operation_review_id INTEGER NOT NULL REFERENCES operation_reviews(operation_review_id),
      triggered_at_ms INTEGER NOT NULL,
      reason TEXT NOT NULL CHECK (reason IN ('reconciliation-exhausted', 'review-deadline-expired'))
    ) STRICT;

    CREATE TRIGGER IF NOT EXISTS operation_review_dependency_inserted
    AFTER INSERT ON activation_dependencies
    WHEN NEW.dependency_kind = 'operation'
    BEGIN
      INSERT INTO operation_reviews (
        dependency_id, operation_kind, original_identity, agent_id,
        original_activation_id, current_activation_id, run_id, fencing_epoch,
        status, review_started_at_ms, review_deadline_at_ms,
        reconciliation_attempts, resolved_at_ms
      )
      SELECT
        NEW.dependency_id,
        CASE
          WHEN NEW.dependency_id LIKE 'acceptance:%' THEN 'acceptance'
          WHEN NEW.dependency_id LIKE 'cancellation:%' THEN 'cancellation'
          WHEN NEW.dependency_id LIKE 'ownership:%' THEN 'ownership'
          WHEN NEW.dependency_id LIKE 'side-effect:%' THEN 'external-side-effect'
          ELSE 'generic'
        END,
        CASE
          WHEN instr(NEW.dependency_id, ':') > 0
            THEN substr(NEW.dependency_id, instr(NEW.dependency_id, ':') + 1)
          ELSE NEW.dependency_id
        END,
        activation.agent_id,
        activation.activation_id,
        activation.activation_id,
        activation.run_id,
        activation.fencing_epoch,
        'reconciling',
        NEW.created_at_ms,
        NEW.created_at_ms + min(
          policy.maximum_unattended_interval_ms,
          CASE
            WHEN NEW.dependency_id LIKE 'acceptance:%' THEN policy.acceptance_interval_ms
            WHEN NEW.dependency_id LIKE 'cancellation:%' THEN policy.cancellation_interval_ms
            WHEN NEW.dependency_id LIKE 'ownership:%' THEN policy.ownership_interval_ms
            WHEN NEW.dependency_id LIKE 'side-effect:%' THEN policy.external_side_effect_interval_ms
            ELSE policy.generic_interval_ms
          END
        ),
        0,
        NULL
      FROM agent_activations activation
      CROSS JOIN workflow_operation_review_policy policy
      WHERE activation.activation_id = NEW.activation_id
        AND NOT EXISTS (
          SELECT 1 FROM operation_reviews
          WHERE current_activation_id = NEW.activation_id
            AND dependency_id = NEW.dependency_id
            AND status != 'resolved'
        );
    END;

    CREATE TRIGGER IF NOT EXISTS operation_review_dependency_removed
    AFTER DELETE ON activation_dependencies
    WHEN OLD.dependency_kind = 'operation'
    BEGIN
      UPDATE operation_reviews
      SET status = 'resolved',
          resolved_at_ms = max(
            review_started_at_ms,
            COALESCE((
              SELECT updated_at_ms FROM agent_activations
              WHERE activation_id = OLD.activation_id
            ), review_started_at_ms)
          )
      WHERE current_activation_id = OLD.activation_id
        AND dependency_id = OLD.dependency_id
        AND status != 'resolved';
    END;
  `);
}

export function transferOperationReviewsInTransaction(
  database: DatabaseSync,
  input: {
    sourceActivationId: string;
    targetActivationId: string;
    ownership: { runId: string; fencingEpoch: number };
  },
): void {
  if (!database.isTransaction) {
    throw new Error("Operation Review transfer requires an active SQLite transaction");
  }
  database.prepare(`
    UPDATE operation_reviews
    SET current_activation_id = ?, run_id = ?, fencing_epoch = ?
    WHERE current_activation_id = ? AND status != 'resolved'
  `).run(
    input.targetActivationId,
    input.ownership.runId,
    input.ownership.fencingEpoch,
    input.sourceActivationId,
  );
}

export function recordOperationReviewEvidenceInTransaction(
  database: DatabaseSync,
  input: {
    operationReviewId: number;
    dependencyId: string;
    evidence: Omit<OperationEvidence, "observedAtMs">;
    observedAtMs: number;
  },
): void {
  if (!database.isTransaction) {
    throw new Error("Operation Review evidence requires an active SQLite transaction");
  }
  assertNonEmpty(input.evidence.kind, "Operation evidence kind");
  assertNonEmpty(input.evidence.detail, "Operation evidence detail");
  const review = database.prepare(`
    SELECT 1
    FROM operation_reviews
    WHERE operation_review_id = ?
      AND dependency_id = ?
      AND status != 'resolved'
  `).get(input.operationReviewId, input.dependencyId);
  if (!review) {
    throw new WorkflowProtocolError(
      "UnknownLifecycleDependency",
      `Operation Review ${input.operationReviewId} is not active for ${input.dependencyId}`,
    );
  }
  database.prepare(`
    INSERT INTO operation_review_evidence (
      operation_review_id, evidence_kind, detail, observed_at_ms
    ) VALUES (?, ?, ?, ?)
  `).run(
    input.operationReviewId,
    input.evidence.kind,
    input.evidence.detail,
    input.observedAtMs,
  );
}

export function recordActiveOperationReviewEvidenceInTransaction(
  database: DatabaseSync,
  input: {
    activationId: string;
    dependencyId: string;
    evidence: Omit<OperationEvidence, "observedAtMs">;
    observedAtMs: number;
  },
): number {
  if (!database.isTransaction) {
    throw new Error("Operation Review evidence requires an active SQLite transaction");
  }
  const review = database.prepare(`
    SELECT operation_review_id
    FROM operation_reviews
    WHERE current_activation_id = ?
      AND dependency_id = ?
      AND status != 'resolved'
  `).get(input.activationId, input.dependencyId) as {
    operation_review_id: number;
  } | undefined;
  if (!review) {
    throw new WorkflowProtocolError(
      "UnknownLifecycleDependency",
      `Activation ${input.activationId} has no active Operation Review for ${input.dependencyId}`,
    );
  }
  recordOperationReviewEvidenceInTransaction(database, {
    operationReviewId: Number(review.operation_review_id),
    dependencyId: input.dependencyId,
    evidence: input.evidence,
    observedAtMs: input.observedAtMs,
  });
  return Number(review.operation_review_id);
}

export class OperationReviewStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    initializeOperationReviewSchema(this.#database);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  configurePolicy(policy: OperationReviewPolicy): void {
    assertPolicy(policy);
    this.#database.prepare(`
      UPDATE workflow_operation_review_policy
      SET maximum_unattended_interval_ms = ?,
          acceptance_interval_ms = ?,
          cancellation_interval_ms = ?,
          ownership_interval_ms = ?,
          external_side_effect_interval_ms = ?,
          generic_interval_ms = ?
      WHERE singleton = 1
    `).run(
      policy.maximumUnattendedIntervalMs,
      policy.intervalsMs.acceptance,
      policy.intervalsMs.cancellation,
      policy.intervalsMs.ownership,
      policy.intervalsMs["external-side-effect"],
      policy.intervalsMs.generic,
    );
  }

  inspect(operationReviewId: number): OperationReviewRecord | undefined {
    const row = this.#database.prepare(
      "SELECT * FROM operation_reviews WHERE operation_review_id = ?",
    ).get(operationReviewId) as OperationReviewRow | undefined;
    return row ? this.#map(row) : undefined;
  }

  listEvidence(operationReviewId: number): OperationEvidence[] {
    return (this.#database.prepare(`
      SELECT evidence_kind, detail, observed_at_ms
      FROM operation_review_evidence
      WHERE operation_review_id = ?
      ORDER BY evidence_sequence
    `).all(operationReviewId) as unknown as OperationEvidenceRow[]).map((row) => ({
      kind: row.evidence_kind,
      detail: row.detail,
      observedAtMs: Number(row.observed_at_ms),
    }));
  }

  listForAgent(agentId: string): OperationReviewRecord[] {
    return (this.#database.prepare(`
      SELECT * FROM operation_reviews
      WHERE agent_id = ? AND status != 'resolved'
      ORDER BY review_started_at_ms, operation_review_id
    `).all(agentId) as unknown as OperationReviewRow[]).map((row) => this.#map(row));
  }

  listHistoryForAgent(agentId: string): OperationReviewRecord[] {
    return (this.#database.prepare(`
      SELECT * FROM operation_reviews
      WHERE agent_id = ?
      ORDER BY review_started_at_ms, operation_review_id
    `).all(agentId) as unknown as OperationReviewRow[]).map((row) => this.#map(row));
  }

  recordEvidence(
    caller: AgentReference,
    operationReviewId: number,
    evidence: Omit<OperationEvidence, "observedAtMs">,
    observedAtMs: number,
  ): OperationReviewRecord {
    assertNonEmpty(evidence.kind, "Operation evidence kind");
    assertNonEmpty(evidence.detail, "Operation evidence detail");
    const ownerAgentId = this.#workflowOwnerId();
    if (caller.workflowOwnerId !== ownerAgentId) {
      throw new WorkflowProtocolError("WorkflowMismatch", "Operation evidence belongs to another Workflow");
    }
    const row = this.#database.prepare(
      "SELECT * FROM operation_reviews WHERE operation_review_id = ?",
    ).get(operationReviewId) as OperationReviewRow | undefined;
    if (!row) {
      throw new WorkflowProtocolError(
        "UnknownLifecycleDependency",
        `Unknown Operation Review ${operationReviewId}`,
      );
    }
    if (caller.agentId !== ownerAgentId && caller.agentId !== row.agent_id) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Agent ${caller.agentId} cannot record evidence for Operation Review ${operationReviewId}`,
      );
    }
    this.#database.prepare(`
      INSERT INTO operation_review_evidence (
        operation_review_id, evidence_kind, detail, observed_at_ms
      ) VALUES (?, ?, ?, ?)
    `).run(operationReviewId, evidence.kind, evidence.detail, observedAtMs);
    return this.inspect(operationReviewId)!;
  }

  listWatchAttention(): WatchAttention[] {
    return (this.#database.prepare(`
      SELECT operation_review_id, dependency_id, agent_id, review_deadline_at_ms
      FROM operation_reviews
      WHERE status = 'reconciling'
      ORDER BY review_deadline_at_ms, operation_review_id
    `).all() as Array<{
      operation_review_id: number;
      dependency_id: string;
      agent_id: string;
      review_deadline_at_ms: number;
    }>).map((row) => ({
      kind: "WATCH",
      key: `operation-review:${row.operation_review_id}`,
      operationReviewId: Number(row.operation_review_id),
      agentId: row.agent_id,
      dependencyId: row.dependency_id,
      reviewDeadlineAtMs: Number(row.review_deadline_at_ms),
    }));
  }

  listReconcilable(): OperationReviewRecord[] {
    return (this.#database.prepare(`
      SELECT * FROM operation_reviews
      WHERE status = 'reconciling'
      ORDER BY review_deadline_at_ms, operation_review_id
    `).all() as unknown as OperationReviewRow[]).map((row) => this.#map(row));
  }

  listDue(now: number): OperationReviewRecord[] {
    return (this.#database.prepare(`
      SELECT * FROM operation_reviews
      WHERE status = 'reconciling'
        AND (reconciliation_attempts = 0 OR review_deadline_at_ms <= ?)
      ORDER BY review_deadline_at_ms, operation_review_id
    `).all(now) as unknown as OperationReviewRow[]).map((row) => this.#map(row));
  }

  applyReconciliation(
    caller: AgentReference,
    expected: OperationReviewRecord,
    outcome: OperationReconciliationOutcome,
    now: number,
  ): OperationReviewRecord | undefined {
    const ownerAgentId = this.#workflowOwnerId();
    if (caller.workflowOwnerId !== ownerAgentId || caller.agentId !== ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        "Only the live Workflow Owner can run workflow-wide Operation Review",
      );
    }
    return this.#withImmediateTransaction(() => {
      const row = this.#database.prepare(
        "SELECT * FROM operation_reviews WHERE operation_review_id = ?",
      ).get(expected.operationReviewId) as OperationReviewRow | undefined;
      if (!row) return undefined;
      if (
        row.current_activation_id !== expected.activationId
        || row.run_id !== expected.ownership.runId
        || Number(row.fencing_epoch) !== expected.ownership.fencingEpoch
      ) {
        return this.#map(row);
      }
      if (row.status !== "reconciling") {
        if (row.status === "resolved" && outcome.kind === "resolved") {
          assertNonEmpty(outcome.evidence.kind, "Operation evidence kind");
          assertNonEmpty(outcome.evidence.detail, "Operation evidence detail");
          this.#database.prepare(`
            INSERT INTO operation_review_evidence (
              operation_review_id, evidence_kind, detail, observed_at_ms
            ) VALUES (?, ?, ?, ?)
          `).run(
            row.operation_review_id,
            outcome.evidence.kind,
            outcome.evidence.detail,
            now,
          );
          this.#database.prepare(`
            UPDATE operation_reviews
            SET reconciliation_attempts = reconciliation_attempts + 1
            WHERE operation_review_id = ?
          `).run(row.operation_review_id);
        }
        const observed = this.#database.prepare(
          "SELECT * FROM operation_reviews WHERE operation_review_id = ?",
        ).get(row.operation_review_id) as OperationReviewRow;
        return this.#map(observed);
      }
      assertNonEmpty(outcome.evidence.kind, "Operation evidence kind");
      assertNonEmpty(outcome.evidence.detail, "Operation evidence detail");
      this.#database.prepare(`
        INSERT INTO operation_review_evidence (
          operation_review_id, evidence_kind, detail, observed_at_ms
        ) VALUES (?, ?, ?, ?)
      `).run(
        row.operation_review_id,
        outcome.evidence.kind,
        outcome.evidence.detail,
        now,
      );
      this.#database.prepare(`
        UPDATE operation_reviews
        SET reconciliation_attempts = reconciliation_attempts + 1
        WHERE operation_review_id = ? AND status = 'reconciling'
      `).run(row.operation_review_id);

      if (outcome.kind === "resolved") {
        this.#resolveWithCurrentOwnership(row, now);
      } else if (
        outcome.eligibility === "exhausted"
        || now >= Number(row.review_deadline_at_ms)
      ) {
        const reason = outcome.eligibility === "exhausted"
          ? "reconciliation-exhausted"
          : "review-deadline-expired";
        this.#database.prepare(`
          UPDATE operation_reviews
          SET status = 'awaiting-judgment'
          WHERE operation_review_id = ? AND status = 'reconciling'
        `).run(row.operation_review_id);
        this.#database.prepare(`
          INSERT INTO operation_incident_triggers (
            trigger_key, operation_review_id, triggered_at_ms, reason
          ) VALUES (?, ?, ?, ?)
          ON CONFLICT (trigger_key) DO NOTHING
        `).run(
          `operation-review:${row.operation_review_id}`,
          row.operation_review_id,
          now,
          reason,
        );
      }
      const updated = this.#database.prepare(
        "SELECT * FROM operation_reviews WHERE operation_review_id = ?",
      ).get(row.operation_review_id) as OperationReviewRow;
      return this.#map(updated);
    });
  }

  listIncidentTriggers(): OperationIncidentTrigger[] {
    return (this.#database.prepare(`
      SELECT trigger.trigger_key, trigger.operation_review_id,
             review.dependency_id, trigger.reason, trigger.triggered_at_ms
      FROM operation_incident_triggers trigger
      JOIN operation_reviews review
        ON review.operation_review_id = trigger.operation_review_id
      ORDER BY triggered_at_ms, trigger_key
    `).all() as Array<{
      trigger_key: string;
      operation_review_id: number;
      dependency_id: string;
      reason: OperationIncidentTrigger["reason"];
      triggered_at_ms: number;
    }>).map((row) => ({
      triggerKey: row.trigger_key,
      operationReviewId: Number(row.operation_review_id),
      dependencyId: row.dependency_id,
      reason: row.reason,
      triggeredAtMs: Number(row.triggered_at_ms),
    }));
  }

  #resolveWithCurrentOwnership(row: OperationReviewRow, now: number): void {
    const resourceId = `agent-run:${this.#workflowOwnerId()}:${row.agent_id}`;
    const ownership = this.#database.prepare(`
      SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?
    `).get(resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
    if (
      !ownership
      || ownership.owner_id !== row.run_id
      || Number(ownership.fencing_epoch) !== Number(row.fencing_epoch)
    ) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Operation Dependency ${row.dependency_id} no longer has its current ownership fence`,
      );
    }
    const removed = this.#database.prepare(`
      DELETE FROM activation_dependencies
      WHERE activation_id = ? AND dependency_kind = 'operation' AND dependency_id = ?
    `).run(row.current_activation_id, row.dependency_id);
    if (Number(removed.changes) !== 1) {
      throw new WorkflowProtocolError(
        "UnknownLifecycleDependency",
        `Operation Dependency ${row.dependency_id} is no longer attached to its activation`,
      );
    }
    this.#database.prepare(`
      UPDATE operation_reviews SET resolved_at_ms = ?
      WHERE operation_review_id = ? AND status = 'resolved'
    `).run(now, row.operation_review_id);
    this.#database.prepare(`
      UPDATE agent_activations SET open_state = 'active', revision = revision + 1, updated_at_ms = ?
      WHERE activation_id = ? AND phase = 'open' AND open_state = 'waiting'
        AND NOT EXISTS (
          SELECT 1 FROM activation_dependencies
          WHERE activation_id = agent_activations.activation_id
        )
        AND NOT EXISTS (
          SELECT 1 FROM human_interrupts
          WHERE agent_id = agent_activations.agent_id
            AND status IN ('pending', 'response-bound', 'result-pending')
        )
        AND NOT EXISTS (
          SELECT 1 FROM workflow_requests
          WHERE requester_activation_id = agent_activations.activation_id
            AND (status IN ('open', 'answered')
              OR (status = 'orphaned' AND orphan_notice_delivery_status = 'accepted'))
        )
    `).run(now, row.current_activation_id);
  }

  #map(row: OperationReviewRow): OperationReviewRecord {
    const evidenceCount = Number((this.#database.prepare(`
      SELECT count(*) AS count FROM operation_review_evidence WHERE operation_review_id = ?
    `).get(row.operation_review_id) as { count: number }).count);
    const latest = this.#database.prepare(`
      SELECT evidence_kind, detail, observed_at_ms
      FROM operation_review_evidence
      WHERE operation_review_id = ?
      ORDER BY evidence_sequence DESC LIMIT 1
    `).get(row.operation_review_id) as OperationEvidenceRow | undefined;
    return {
      operationReviewId: Number(row.operation_review_id),
      dependencyId: row.dependency_id,
      operationKind: row.operation_kind,
      originalIdentity: row.original_identity,
      agentId: row.agent_id,
      activationId: row.current_activation_id,
      ownership: {
        runId: row.run_id,
        fencingEpoch: Number(row.fencing_epoch),
      },
      status: row.status,
      reviewStartedAtMs: Number(row.review_started_at_ms),
      reviewDeadlineAtMs: Number(row.review_deadline_at_ms),
      reconciliationAttempts: Number(row.reconciliation_attempts),
      evidenceCount,
      ...(latest ? {
        latestEvidence: {
          kind: latest.evidence_kind,
          detail: latest.detail,
          observedAtMs: Number(latest.observed_at_ms),
        },
      } : {}),
    };
  }

  #workflowOwnerId(): string {
    const row = this.#database.prepare(
      "SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1",
    ).get() as { owner_agent_id: string } | undefined;
    if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
    return row.owner_agent_id;
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

function assertPolicy(policy: OperationReviewPolicy): void {
  assertPositiveInteger(policy.maximumUnattendedIntervalMs, "Workflow maximum unattended interval");
  for (const kind of [
    "acceptance",
    "cancellation",
    "ownership",
    "external-side-effect",
    "generic",
  ] as const) {
    assertPositiveInteger(policy.intervalsMs?.[kind], `${kind} Operation Review interval`);
  }
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${label} must be a positive integer`);
  }
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new TypeError(`${label} must not be empty`);
}
