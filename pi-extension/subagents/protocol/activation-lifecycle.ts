import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const HUMAN_DEPENDENCY_ID = "human";

export type ActivationDependency =
  | { kind: "human"; dependencyId: typeof HUMAN_DEPENDENCY_ID }
  | { kind: "agent"; dependencyId: string; agentId: string }
  | { kind: "operation"; dependencyId: string };

export type DeclaredActivationDependency = Exclude<ActivationDependency, { kind: "human" }>;

export type ActivationState =
  | { kind: "active" }
  | { kind: "waiting"; dependencies: ActivationDependency[] }
  | { kind: "interrupted" }
  | { kind: "ended"; outcome: "completed" | "cancelled" }
  | {
      kind: "ended";
      outcome: "failed";
      error: string;
      exitCode?: number;
    };

export interface ActivationRecord extends AgentReference {
  activationId: string;
  runId: string;
  fencingEpoch: number;
  sequence: number;
  revision: number;
  turnSequence: number;
  state: ActivationState;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface InterruptionRequest {
  activationId: string;
  turnSequence: number;
  requestedAtMs: number;
}

export interface FailedExit {
  error: string;
  exitCode?: number;
}

interface ActivationRow {
  activation_id: string;
  agent_id: string;
  run_id: string;
  fencing_epoch: number;
  activation_sequence: number;
  revision: number;
  turn_sequence: number;
  phase: "open" | "ended";
  open_state: "active" | "waiting" | "interrupted" | null;
  ended_outcome: "completed" | "failed" | "cancelled" | null;
  failure_error: string | null;
  failure_exit_code: number | null;
  interrupt_turn_sequence: number | null;
  interrupt_requested_at_ms: number | null;
  created_at_ms: number;
  updated_at_ms: number;
}

interface DependencyRow {
  dependency_kind: "agent" | "operation";
  dependency_id: string;
  agent_id: string | null;
}

interface OwnerRow {
  owner_id: string;
  fencing_epoch: number;
}

export class ActivationLifecycleStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS agent_activations (
        activation_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        run_id TEXT NOT NULL UNIQUE,
        fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0),
        activation_sequence INTEGER NOT NULL CHECK (activation_sequence > 0),
        revision INTEGER NOT NULL CHECK (revision > 0),
        turn_sequence INTEGER NOT NULL CHECK (turn_sequence > 0),
        phase TEXT NOT NULL CHECK (phase IN ('open', 'ended')),
        open_state TEXT CHECK (open_state IN ('active', 'waiting', 'interrupted')),
        ended_outcome TEXT CHECK (ended_outcome IN ('completed', 'failed', 'cancelled')),
        failure_error TEXT,
        failure_exit_code INTEGER,
        interrupt_turn_sequence INTEGER,
        interrupt_requested_at_ms INTEGER,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        UNIQUE (agent_id, activation_sequence),
        CHECK (
          (phase = 'open' AND open_state IS NOT NULL AND ended_outcome IS NULL AND failure_error IS NULL)
          OR
          (phase = 'ended' AND open_state IS NULL AND (
            (ended_outcome = 'failed' AND failure_error IS NOT NULL)
            OR
            (ended_outcome IN ('completed', 'cancelled') AND failure_error IS NULL AND failure_exit_code IS NULL)
          ))
        )
      ) STRICT;

      CREATE INDEX IF NOT EXISTS agent_activations_current
      ON agent_activations (agent_id, activation_sequence DESC);

      CREATE TABLE IF NOT EXISTS activation_dependencies (
        activation_id TEXT NOT NULL REFERENCES agent_activations(activation_id),
        dependency_kind TEXT NOT NULL CHECK (dependency_kind IN ('agent', 'operation')),
        dependency_id TEXT NOT NULL,
        dependency_agent_id TEXT,
        created_at_ms INTEGER NOT NULL,
        PRIMARY KEY (activation_id, dependency_kind, dependency_id),
        CHECK (
          (dependency_kind = 'agent' AND dependency_agent_id IS NOT NULL)
          OR
          (dependency_kind = 'operation' AND dependency_agent_id IS NULL)
        )
      ) STRICT;
    `);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  start(ownership: AgentRunOwnership, now: number): ActivationRecord {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      this.#assertSubagent(ownership);
      const current = this.#readCurrentRow(ownership.agentId);
      if (current?.phase === "open") {
        if (
          current.run_id === ownership.runId
          && Number(current.fencing_epoch) === ownership.epoch
        ) {
          return this.#mapRow(current, ownership.workflowOwnerId);
        }
        throw new WorkflowProtocolError(
          "ActivationAlreadyOpen",
          `Agent ${ownership.agentId} already has open activation ${current.activation_id}`,
        );
      }
      const sequence = Number(current?.activation_sequence ?? 0) + 1;
      this.#database.prepare(`
        INSERT INTO agent_activations (
          activation_id, agent_id, run_id, fencing_epoch, activation_sequence,
          revision, turn_sequence, phase, open_state, ended_outcome,
          failure_error, failure_exit_code, interrupt_turn_sequence,
          interrupt_requested_at_ms, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, 1, 1, 'open', 'active', NULL, NULL, NULL, NULL, NULL, ?, ?)
      `).run(
        ownership.runId,
        ownership.agentId,
        ownership.runId,
        ownership.epoch,
        sequence,
        now,
        now,
      );
      return this.#requireCurrent(ownership.agentId, ownership.workflowOwnerId);
    });
  }

  inspect(reference: AgentReference): ActivationRecord | undefined {
    const owner = this.#workflowOwnerId();
    if (reference.workflowOwnerId !== owner) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Identity ${reference.agentId} belongs to Workflow ${reference.workflowOwnerId}, not ${owner}`,
      );
    }
    this.#requireAgent(reference.agentId);
    if (reference.agentId === owner) return undefined;
    const row = this.#readCurrentRow(reference.agentId);
    return row ? this.#mapRow(row, owner) : undefined;
  }

  addDependency(
    ownership: AgentRunOwnership,
    dependency: DeclaredActivationDependency,
    now: number,
    expectedRevision?: number,
  ): ActivationRecord {
    assertDependency(dependency);
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      this.#database.prepare(`
        INSERT INTO activation_dependencies (
          activation_id, dependency_kind, dependency_id, dependency_agent_id, created_at_ms
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (activation_id, dependency_kind, dependency_id) DO UPDATE SET
          dependency_agent_id = excluded.dependency_agent_id
      `).run(
        row.activation_id,
        dependency.kind,
        dependency.dependencyId,
        dependency.kind === "agent" ? dependency.agentId : null,
        now,
      );
      this.#touch(row, now);
    });
  }

  removeDependency(
    ownership: AgentRunOwnership,
    dependency: Pick<DeclaredActivationDependency, "kind" | "dependencyId">,
    now: number,
    expectedRevision?: number,
  ): ActivationRecord {
    assertNonEmpty(dependency.dependencyId, "dependency ID");
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      const result = this.#database.prepare(`
        DELETE FROM activation_dependencies
        WHERE activation_id = ? AND dependency_kind = ? AND dependency_id = ?
      `).run(row.activation_id, dependency.kind, dependency.dependencyId);
      if (Number(result.changes) === 0) {
        throw new WorkflowProtocolError(
          "UnknownLifecycleDependency",
          `Activation ${row.activation_id} has no ${dependency.kind} dependency ${dependency.dependencyId}`,
        );
      }
      this.#touch(row, now);
    });
  }

  settle(
    ownership: AgentRunOwnership,
    now: number,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      if (row.open_state !== "active") {
        throw this.#invalidTransition(row, "settle");
      }
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'waiting', revision = revision + 1,
            interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
    });
  }

  activateTurn(
    ownership: AgentRunOwnership,
    now: number,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      if (row.open_state === "active") return;
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'active', revision = revision + 1,
            turn_sequence = turn_sequence + 1,
            interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
    }, { allowNoop: true });
  }

  requestInterruption(
    ownership: AgentRunOwnership,
    now: number,
    expectedRevision?: number,
  ): InterruptionRequest {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      this.#assertRevision(row, expectedRevision);
      if (row.open_state !== "active") throw this.#invalidTransition(row, "request interruption");
      this.#database.prepare(`
        UPDATE agent_activations
        SET revision = revision + 1,
            interrupt_turn_sequence = turn_sequence,
            interrupt_requested_at_ms = ?,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, now, row.activation_id);
      return {
        activationId: row.activation_id,
        turnSequence: Number(row.turn_sequence),
        requestedAtMs: now,
      };
    });
  }

  confirmInterruption(
    ownership: AgentRunOwnership,
    confirmation: InterruptionRequest | undefined,
    now: number,
    expectedRevision?: number,
  ): ActivationRecord {
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      if (row.open_state !== "active") throw this.#invalidTransition(row, "confirm interruption");
      if (confirmation) {
        const matches = confirmation.activationId === row.activation_id
          && confirmation.turnSequence === Number(row.turn_sequence)
          && confirmation.turnSequence === Number(row.interrupt_turn_sequence)
          && confirmation.requestedAtMs === Number(row.interrupt_requested_at_ms);
        if (!matches) {
          throw new WorkflowProtocolError(
            "StaleLifecycleTransition",
            `Interruption confirmation no longer matches active turn ${row.turn_sequence}`,
          );
        }
      }
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'interrupted', revision = revision + 1,
            interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
    });
  }

  failAndRelease(
    ownership: AgentRunOwnership,
    failure: FailedExit,
    now: number,
  ): ActivationRecord {
    assertNonEmpty(failure.error, "failure error");
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      this.#database.prepare(`
        UPDATE agent_activations
        SET phase = 'ended', open_state = NULL, ended_outcome = 'failed',
            failure_error = ?, failure_exit_code = ?, revision = revision + 1,
            interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(failure.error, failure.exitCode ?? null, now, row.activation_id);
      const released = this.#database.prepare(`
        DELETE FROM ownership
        WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?
      `).run(ownership.resourceId, ownership.runId, ownership.epoch);
      if (Number(released.changes) !== 1) {
        throw new WorkflowProtocolError(
          "OwnershipLost",
          `Agent Run no longer owns ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
        );
      }
      return this.#requireCurrent(ownership.agentId, ownership.workflowOwnerId);
    });
  }

  releaseWithoutActivation(ownership: AgentRunOwnership): void {
    this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#readCurrentRow(ownership.agentId);
      if (
        row?.phase === "open"
        && row.run_id === ownership.runId
        && Number(row.fencing_epoch) === ownership.epoch
      ) {
        throw new WorkflowProtocolError(
          "InvalidLifecycleTransition",
          `Open activation ${row.activation_id} must end before Agent Run ownership is released`,
        );
      }
      const released = this.#database.prepare(`
        DELETE FROM ownership
        WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?
      `).run(ownership.resourceId, ownership.runId, ownership.epoch);
      if (Number(released.changes) !== 1) {
        throw new WorkflowProtocolError(
          "OwnershipLost",
          `Agent Run no longer owns ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
        );
      }
    });
  }

  #mutateOpen(
    ownership: AgentRunOwnership,
    expectedRevision: number | undefined,
    mutation: (row: ActivationRow) => void,
    options: { allowNoop?: boolean } = {},
  ): ActivationRecord {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      this.#assertRevision(row, expectedRevision);
      const beforeRevision = Number(row.revision);
      mutation(row);
      const record = this.#requireCurrent(ownership.agentId, ownership.workflowOwnerId);
      if (!options.allowNoop && record.revision === beforeRevision) {
        throw new Error(`Lifecycle mutation did not advance revision for ${row.activation_id}`);
      }
      return record;
    });
  }

  #touch(row: ActivationRow, now: number): void {
    this.#database.prepare(`
      UPDATE agent_activations
      SET revision = revision + 1, updated_at_ms = ?
      WHERE activation_id = ?
    `).run(now, row.activation_id);
  }

  #assertCurrentOwnership(ownership: AgentRunOwnership): void {
    const expectedResourceId = `agent-run:${ownership.workflowOwnerId}:${ownership.agentId}`;
    if (ownership.resourceId !== expectedResourceId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Agent Run ownership token does not belong to Workflow ${ownership.workflowOwnerId}`,
      );
    }
    const row = this.#database.prepare(`
      SELECT owner_id, fencing_epoch
      FROM ownership
      WHERE resource_id = ?
    `).get(ownership.resourceId) as OwnerRow | undefined;
    if (
      !row
      || row.owner_id !== ownership.runId
      || Number(row.fencing_epoch) !== ownership.epoch
    ) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Agent Run no longer owns ${ownership.agentId} at fencing epoch ${ownership.epoch}`,
      );
    }
  }

  #assertSubagent(reference: AgentReference): void {
    const owner = this.#workflowOwnerId();
    if (reference.workflowOwnerId !== owner) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Identity ${reference.agentId} belongs to Workflow ${reference.workflowOwnerId}, not ${owner}`,
      );
    }
    this.#requireAgent(reference.agentId);
    if (reference.agentId === owner) {
      throw new WorkflowProtocolError(
        "OwnerActivationForbidden",
        "Workflow Owner does not have a Subagent activation lifecycle",
      );
    }
  }

  #requireOwnedOpenRow(ownership: AgentRunOwnership): ActivationRow {
    this.#assertSubagent(ownership);
    const row = this.#readCurrentRow(ownership.agentId);
    if (
      !row
      || row.phase !== "open"
      || row.run_id !== ownership.runId
      || Number(row.fencing_epoch) !== ownership.epoch
    ) {
      throw new WorkflowProtocolError(
        "StaleLifecycleTransition",
        `Agent Run ${ownership.runId} does not own the current open activation for ${ownership.agentId}`,
      );
    }
    return row;
  }

  #assertRevision(row: ActivationRow, expectedRevision: number | undefined): void {
    if (expectedRevision == null) return;
    if (Number(row.revision) !== expectedRevision) {
      throw new WorkflowProtocolError(
        "StaleLifecycleTransition",
        `Activation ${row.activation_id} is at revision ${row.revision}, not ${expectedRevision}`,
      );
    }
  }

  #invalidTransition(row: ActivationRow, operation: string): WorkflowProtocolError {
    return new WorkflowProtocolError(
      "InvalidLifecycleTransition",
      `Cannot ${operation} activation ${row.activation_id} from ${row.phase === "open" ? row.open_state : row.ended_outcome}`,
    );
  }

  #workflowOwnerId(): string {
    const row = this.#database.prepare(`
      SELECT owner_agent_id
      FROM workflow_metadata
      WHERE singleton = 1
    `).get() as { owner_agent_id: string } | undefined;
    if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
    return row.owner_agent_id;
  }

  #requireAgent(agentId: string): void {
    const row = this.#database.prepare(`
      SELECT 1 AS present
      FROM workflow_agents
      WHERE agent_id = ?
    `).get(agentId) as { present: number } | undefined;
    if (!row) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${agentId}`);
  }

  #readCurrentRow(agentId: string): ActivationRow | undefined {
    return this.#database.prepare(`
      SELECT activation_id, agent_id, run_id, fencing_epoch, activation_sequence,
             revision, turn_sequence, phase, open_state, ended_outcome,
             failure_error, failure_exit_code, interrupt_turn_sequence,
             interrupt_requested_at_ms, created_at_ms, updated_at_ms
      FROM agent_activations
      WHERE agent_id = ?
      ORDER BY activation_sequence DESC
      LIMIT 1
    `).get(agentId) as ActivationRow | undefined;
  }

  #requireCurrent(agentId: string, workflowOwnerId: string): ActivationRecord {
    const row = this.#readCurrentRow(agentId);
    if (!row) throw new Error(`Activation was not persisted for Agent ${agentId}`);
    return this.#mapRow(row, workflowOwnerId);
  }

  #mapRow(row: ActivationRow, workflowOwnerId: string): ActivationRecord {
    const state: ActivationState = row.phase === "ended"
      ? row.ended_outcome === "failed"
        ? {
            kind: "ended",
            outcome: "failed",
            error: row.failure_error!,
            ...(row.failure_exit_code == null ? {} : { exitCode: Number(row.failure_exit_code) }),
          }
        : { kind: "ended", outcome: row.ended_outcome! }
      : row.open_state === "waiting"
        ? {
            kind: "waiting",
            dependencies: this.#readDependencies(row.activation_id),
          }
        : row.open_state === "interrupted"
          ? { kind: "interrupted" }
          : { kind: "active" };
    return {
      workflowOwnerId,
      agentId: row.agent_id,
      activationId: row.activation_id,
      runId: row.run_id,
      fencingEpoch: Number(row.fencing_epoch),
      sequence: Number(row.activation_sequence),
      revision: Number(row.revision),
      turnSequence: Number(row.turn_sequence),
      state,
      createdAtMs: Number(row.created_at_ms),
      updatedAtMs: Number(row.updated_at_ms),
    };
  }

  #readDependencies(activationId: string): ActivationDependency[] {
    const rows = this.#database.prepare(`
      SELECT dependency_kind, dependency_id, dependency_agent_id AS agent_id
      FROM activation_dependencies
      WHERE activation_id = ?
      ORDER BY created_at_ms, dependency_kind, dependency_id
    `).all(activationId) as unknown as DependencyRow[];
    if (rows.length === 0) return [{ kind: "human", dependencyId: HUMAN_DEPENDENCY_ID }];
    return rows.map((row) => row.dependency_kind === "agent"
      ? { kind: "agent", dependencyId: row.dependency_id, agentId: row.agent_id! }
      : { kind: "operation", dependencyId: row.dependency_id });
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

function assertDependency(dependency: DeclaredActivationDependency): void {
  assertNonEmpty(dependency.dependencyId, "dependency ID");
  if (dependency.kind === "agent") assertNonEmpty(dependency.agentId, "dependency Agent ID");
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new Error(`Activation ${label} must not be empty`);
}
