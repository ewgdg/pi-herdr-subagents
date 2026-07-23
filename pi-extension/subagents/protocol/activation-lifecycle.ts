import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
import {
  activateRecoveryReplacement,
  exhaustRecoveryForReplacement,
  initializeActivationRecoverySchema,
  recordRecoveryContinuationEvidence,
  recordRecoveryEpisodeForFailedActivation,
  resolveRecoveryReplacementIfWorkIsGone,
  resolveActiveRecovery,
} from "./activation-recovery.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const HUMAN_DEPENDENCY_ID = "human";
const UNDECLARED_DEPENDENCY_ID = "undeclared";
const UNDECLARED_SETTLEMENT_NOTICE_TEXT = "Your last settlement declared no Human Interrupt, Agent dependency, or operation dependency. Continue by declaring a real dependency, or complete the activation through its lifecycle action.";

export type ActivationDependency =
  | { kind: "human"; dependencyId: typeof HUMAN_DEPENDENCY_ID }
  | { kind: "undeclared"; dependencyId: typeof UNDECLARED_DEPENDENCY_ID }
  | { kind: "agent"; dependencyId: string; agentId: string }
  | { kind: "operation"; dependencyId: string };

export type DeclaredActivationDependency = Exclude<ActivationDependency, { kind: "human" | "undeclared" }>;

export interface HumanInterruptRecord {
  toolCallId: string;
  status: "pending" | "response-bound" | "result-pending" | "consumed" | "terminal";
  responseInputId?: string;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface UndeclaredSettlementEpisode {
  episodeId: string;
  noticeId: string;
  noticeText: string;
  agentId: string;
  status: "open" | "closed";
  noticeQueued: boolean;
  noticeDelivered: boolean;
  repeatTriggered: boolean;
  triggerKind?: "incident" | "owner-handoff";
  createdAtMs: number;
  updatedAtMs: number;
}

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

/** A recovery pane claimed correctly but found no remaining durable work. */
export interface RecoveryActivationNotNeeded {
  kind: "not-needed";
  failedActivationId: string;
}

export type ActivationStartResult = ActivationRecord | RecoveryActivationNotNeeded;

export function isRecoveryActivationNotNeeded(
  result: ActivationStartResult,
): result is RecoveryActivationNotNeeded {
  return "kind" in result && result.kind === "not-needed";
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

interface HumanInterruptRow {
  agent_id: string;
  activation_id: string;
  tool_call_id: string;
  status: "pending" | "response-bound" | "result-pending" | "consumed" | "terminal";
  response_input_id: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  terminal_reason: string | null;
}

interface UndeclaredEpisodeRow {
  episode_id: string;
  agent_id: string;
  status: "open" | "closed";
  notice_queued: number;
  notice_delivered: number;
  notice_text: string;
  declared_waiting: number;
  repeat_triggered: number;
  trigger_kind: "incident" | "owner-handoff" | null;
  created_at_ms: number;
  updated_at_ms: number;
}

/**
 * Start the next activation after its ownership claim is durable. Callers use
 * this inside their existing write transaction so recovery fencing and all
 * failed-activation obligations move with the activation atomically.
 */
export function startOwnedActivationInTransaction(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  now: number,
): RecoveryActivationNotNeeded | undefined {
  if (!database.isTransaction) throw new Error("Activation start requires an active SQLite transaction");
  const current = database.prepare(`SELECT activation_id, run_id, fencing_epoch,
      activation_sequence, phase, ended_outcome
    FROM agent_activations WHERE agent_id = ?
    ORDER BY activation_sequence DESC LIMIT 1`).get(ownership.agentId) as {
      activation_id: string;
      run_id: string;
      fencing_epoch: number;
      activation_sequence: number;
      phase: "open" | "ended";
      ended_outcome: "completed" | "failed" | "cancelled" | null;
    } | undefined;
  if (current?.phase === "open") {
    if (current.run_id === ownership.runId && Number(current.fencing_epoch) === ownership.epoch) return;
    throw new WorkflowProtocolError(
      "ActivationAlreadyOpen",
      `Agent ${ownership.agentId} already has open activation ${current.activation_id}`,
    );
  }

  if (current?.ended_outcome === "failed" && resolveRecoveryReplacementIfWorkIsGone(database, {
    failedActivationId: current.activation_id,
    ownership,
    now,
  })) {
    return { kind: "not-needed", failedActivationId: current.activation_id };
  }

  const recoveredHuman = database.prepare(`SELECT tool_call_id FROM human_interrupts
    WHERE agent_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
    ORDER BY created_at_ms DESC LIMIT 1`).get(ownership.agentId) as { tool_call_id: string } | undefined;
  const recoveredOperations = current?.ended_outcome === "failed"
    ? database.prepare(`SELECT dependency_id FROM activation_dependencies
        WHERE activation_id = ? AND dependency_kind = 'operation'
        ORDER BY dependency_id`).all(current.activation_id) as Array<{ dependency_id: string }>
    : [];
  const recoveredOutgoingRequests = current?.ended_outcome === "failed"
    ? database.prepare(`SELECT request_id FROM workflow_requests
        WHERE requester_activation_id = ?
          AND (status IN ('open', 'answered')
            OR (status = 'orphaned' AND orphan_notice_delivery_status = 'queued'))
        ORDER BY request_id`).all(current.activation_id) as Array<{ request_id: string }>
    : [];

  database.prepare(`INSERT INTO agent_activations (
      activation_id, agent_id, run_id, fencing_epoch, activation_sequence,
      revision, turn_sequence, phase, open_state, ended_outcome,
      failure_error, failure_exit_code, interrupt_turn_sequence,
      interrupt_requested_at_ms, created_at_ms, updated_at_ms
    ) VALUES (?, ?, ?, ?, ?, 1, 1, 'open', ?, NULL, NULL, NULL, NULL, NULL, ?, ?)`)
    .run(
      ownership.runId,
      ownership.agentId,
      ownership.runId,
      ownership.epoch,
      Number(current?.activation_sequence ?? 0) + 1,
      recoveredHuman || recoveredOperations.length > 0 || recoveredOutgoingRequests.length > 0
        ? "waiting"
        : "active",
      now,
      now,
    );

  if (current?.ended_outcome === "failed") {
    const recovery = database.prepare(`SELECT state FROM activation_recoveries
      WHERE failed_activation_id = ?`).get(current.activation_id) as { state: string } | undefined;
    if (recovery?.state === "launching") {
      activateRecoveryReplacement(database, {
        failedActivationId: current.activation_id,
        replacementRunId: ownership.runId,
        replacementFencingEpoch: ownership.epoch,
        replacementActivationId: ownership.runId,
        now,
      });
    } else if (recovery?.state === "pending" || recovery?.state === "blocked-policy") {
      database.prepare(`UPDATE activation_recoveries
        SET state = 'resolved', detail = 'Manual resume superseded automatic recovery', updated_at_ms = ?
        WHERE failed_activation_id = ? AND state IN ('pending', 'blocked-policy')`).run(now, current.activation_id);
    }
    database.prepare(`UPDATE workflow_requests SET requester_activation_id = ?
      WHERE requester_activation_id = ? AND (status IN ('open', 'answered')
        OR (status = 'orphaned' AND orphan_notice_delivery_status = 'queued'))`)
      .run(ownership.runId, current.activation_id);
    database.prepare(`UPDATE workflow_requests SET responder_activation_id = ?
      WHERE responder_activation_id = ? AND status = 'open'`)
      .run(ownership.runId, current.activation_id);
  }
  if (recoveredHuman) {
    database.prepare(`UPDATE human_interrupts SET activation_id = ?, updated_at_ms = ?
      WHERE agent_id = ? AND tool_call_id = ?`)
      .run(ownership.runId, now, ownership.agentId, recoveredHuman.tool_call_id);
  }
  for (const operation of recoveredOperations) {
    database.prepare(`INSERT INTO activation_dependencies (
      activation_id, dependency_kind, dependency_id, dependency_agent_id, created_at_ms
    ) VALUES (?, 'operation', ?, NULL, ?)`)
      .run(ownership.runId, operation.dependency_id, now);
  }
  if (recoveredOperations.length > 0) {
    database.prepare("DELETE FROM activation_dependencies WHERE activation_id = ? AND dependency_kind = 'operation'")
      .run(current!.activation_id);
  }
  return undefined;
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

      CREATE TABLE IF NOT EXISTS human_interrupts (
        agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        activation_id TEXT NOT NULL REFERENCES agent_activations(activation_id),
        tool_call_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'response-bound', 'result-pending', 'consumed', 'terminal')),
        response_input_id TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        terminal_reason TEXT,
        PRIMARY KEY (agent_id, tool_call_id),
        CHECK ((status IN ('response-bound', 'result-pending', 'consumed') AND response_input_id IS NOT NULL)
          OR (status IN ('pending', 'terminal') AND response_input_id IS NULL)),
        CHECK ((status = 'terminal' AND terminal_reason IS NOT NULL)
          OR (status != 'terminal' AND terminal_reason IS NULL))
      ) STRICT;

      CREATE UNIQUE INDEX IF NOT EXISTS human_interrupts_one_open
      ON human_interrupts (agent_id)
      WHERE status IN ('pending', 'response-bound');

      CREATE UNIQUE INDEX IF NOT EXISTS human_interrupts_response_input
      ON human_interrupts (response_input_id)
      WHERE response_input_id IS NOT NULL;

      CREATE TABLE IF NOT EXISTS human_attention (
        agent_id TEXT PRIMARY KEY REFERENCES workflow_agents(agent_id),
        tool_call_id TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL
      ) STRICT;

      CREATE TABLE IF NOT EXISTS undeclared_settlement_episodes (
        episode_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
        notice_queued INTEGER NOT NULL DEFAULT 0 CHECK (notice_queued IN (0, 1)),
        notice_delivered INTEGER NOT NULL DEFAULT 0 CHECK (notice_delivered IN (0, 1)),
        notice_text TEXT NOT NULL,
        declared_waiting INTEGER NOT NULL DEFAULT 0 CHECK (declared_waiting IN (0, 1)),
        repeat_triggered INTEGER NOT NULL DEFAULT 0 CHECK (repeat_triggered IN (0, 1)),
        trigger_kind TEXT CHECK (trigger_kind IN ('incident', 'owner-handoff')),
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      ) STRICT;

      CREATE UNIQUE INDEX IF NOT EXISTS undeclared_settlement_one_open
      ON undeclared_settlement_episodes (agent_id)
      WHERE status = 'open';

      CREATE TABLE IF NOT EXISTS undeclared_settlement_dependencies (
        episode_id TEXT NOT NULL REFERENCES undeclared_settlement_episodes(episode_id),
        dependency_kind TEXT NOT NULL CHECK (dependency_kind IN ('agent', 'operation', 'human')),
        dependency_id TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        PRIMARY KEY (episode_id, dependency_kind, dependency_id)
      ) STRICT;

    `);
    initializeActivationRecoverySchema(this.#database);
    const episodeColumns = this.#database.prepare("PRAGMA table_info(undeclared_settlement_episodes)").all() as Array<{ name: string }>;
    if (!episodeColumns.some((column) => column.name === "notice_text")) {
      this.#database.exec(`ALTER TABLE undeclared_settlement_episodes ADD COLUMN notice_text TEXT NOT NULL DEFAULT '${UNDECLARED_SETTLEMENT_NOTICE_TEXT.replace(/'/g, "''")}'`);
    }
    if (!episodeColumns.some((column) => column.name === "notice_queued")) {
      this.#database.exec("ALTER TABLE undeclared_settlement_episodes ADD COLUMN notice_queued INTEGER NOT NULL DEFAULT 0");
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  start(ownership: AgentRunOwnership, now: number): ActivationRecord {
    const result = this.startRecovery(ownership, now);
    if (isRecoveryActivationNotNeeded(result)) {
      throw new WorkflowProtocolError(
        "RecoveryActivationClaimed",
        `Automatic recovery for ${result.failedActivationId} no longer has durable work`,
      );
    }
    return result;
  }

  startRecovery(ownership: AgentRunOwnership, now: number): ActivationStartResult {
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
      const outcome = startOwnedActivationInTransaction(this.#database, ownership, now);
      if (outcome) return outcome;
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

  inspectRun(ownership: AgentRunOwnership): ActivationRecord | undefined {
    const row = this.#database.prepare(`SELECT * FROM agent_activations
      WHERE agent_id = ? AND run_id = ? AND fencing_epoch = ? LIMIT 1`).get(
        ownership.agentId, ownership.runId, ownership.epoch,
      ) as ActivationRow | undefined;
    return row ? this.#mapRow(row, ownership.workflowOwnerId) : undefined;
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

  satisfyDependency(
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
      this.#satisfyUndeclaredDependency(row.agent_id, dependency.kind, dependency.dependencyId, now);
      this.#touch(row, now);
    });
  }

  settle(
    ownership: AgentRunOwnership,
    now: number,
    expectedRevision?: number,
    actorRole: "ordinary" | "moderator" = "ordinary",
  ): ActivationRecord {
    return this.#mutateOpen(ownership, expectedRevision, (row) => {
      if (row.open_state !== "active") {
        throw this.#invalidTransition(row, "settle");
      }
      const declaredDependencies = this.#readDeclaredDependencies(row.activation_id);
      if (declaredDependencies.length === 0 && !this.#openHumanInterrupt(row.agent_id)) {
        this.#recordUndeclaredSettlement(row, now, actorRole);
      } else if (declaredDependencies.length > 0) {
        this.#declareUndeclaredDependencies(row.agent_id, declaredDependencies, now);
        resolveActiveRecovery(this.#database, {
          activationId: row.activation_id,
          now,
          detail: "Replacement made a durable declared settlement",
        });
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
      if (this.#unresolvedHumanInterrupt(row.agent_id)) {
        throw new WorkflowProtocolError(
          "InvalidLifecycleTransition",
          `Activation ${row.activation_id} is waiting for a Human Interrupt result`,
        );
      }
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

  beginHumanInterrupt(
    ownership: AgentRunOwnership,
    toolCallId: string,
    now: number,
    actorRole: "ordinary" | "moderator" = "ordinary",
  ): HumanInterruptRecord {
    assertNonEmpty(toolCallId, "Human Interrupt tool-call ID");
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      this.#assertOrdinarySubagent(row.agent_id, actorRole);
      const existing = this.#humanInterrupt(row.agent_id, toolCallId);
      if (existing) return mapHumanInterrupt(existing);
      const unresolved = this.#unresolvedHumanInterrupt(row.agent_id);
      if (unresolved) {
        throw new WorkflowProtocolError(
          "HumanInterruptAlreadyPending",
          `Agent ${row.agent_id} already has a pending Human Interrupt`,
        );
      }
      if (row.open_state !== "active") throw this.#invalidTransition(row, "ask a human");
      this.#database.prepare(`
        INSERT INTO human_interrupts (
          agent_id, activation_id, tool_call_id, status, response_input_id,
          created_at_ms, updated_at_ms, terminal_reason
        ) VALUES (?, ?, ?, 'pending', NULL, ?, ?, NULL)
      `).run(row.agent_id, row.activation_id, toolCallId, now, now);
      this.#declareUndeclaredDependency(row.agent_id, "human", toolCallId, now);
      this.#database.prepare(`
        INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms)
        VALUES (?, ?, ?)
      `).run(row.agent_id, toolCallId, now);
      resolveActiveRecovery(this.#database, {
        activationId: row.activation_id,
        now,
        detail: "Replacement made a durable Human settlement",
      });
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'waiting', revision = revision + 1,
            updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
      return mapHumanInterrupt(this.#humanInterrupt(row.agent_id, toolCallId)!);
    });
  }

  bindHumanResponse(
    ownership: AgentRunOwnership,
    toolCallId: string,
    responseInputId: string,
    now: number,
  ): HumanInterruptRecord | undefined {
    assertNonEmpty(responseInputId, "Human response input ID");
    assertNonEmpty(toolCallId, "Human Interrupt tool-call ID");
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      const interrupt = this.#humanInterrupt(row.agent_id, toolCallId);
      if (!interrupt || interrupt.status !== "pending") return undefined;
      if (row.open_state !== "waiting") throw this.#invalidTransition(row, "bind human input");
      const result = this.#database.prepare(`
        UPDATE human_interrupts
        SET status = 'response-bound', response_input_id = ?, updated_at_ms = ?
        WHERE agent_id = ? AND tool_call_id = ? AND status = 'pending'
      `).run(responseInputId, now, row.agent_id, toolCallId);
      if (Number(result.changes) !== 1) throw new WorkflowProtocolError("HumanInterruptTerminal", "Human Interrupt no longer accepts input");
      this.#database.prepare("DELETE FROM human_attention WHERE agent_id = ?").run(row.agent_id);
      return mapHumanInterrupt(this.#humanInterrupt(row.agent_id, toolCallId)!);
    });
  }

  prepareHumanResponseResult(
    ownership: AgentRunOwnership,
    toolCallId: string,
    now: number,
  ): HumanInterruptRecord {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      const interrupt = this.#humanInterrupt(row.agent_id, toolCallId);
      if (!interrupt || interrupt.status === "terminal") {
        throw new WorkflowProtocolError("HumanInterruptTerminal", `Human Interrupt ${toolCallId} is terminal`);
      }
      if (interrupt.status !== "response-bound") {
        throw new WorkflowProtocolError("HumanInterruptResponseMissing", `Human Interrupt ${toolCallId} has no bound response`);
      }
      if (row.open_state !== "waiting") throw this.#invalidTransition(row, "resume Human Interrupt");
      this.#database.prepare(`
        UPDATE human_interrupts
        SET status = 'result-pending', updated_at_ms = ?
        WHERE agent_id = ? AND tool_call_id = ? AND status = 'response-bound'
      `).run(now, row.agent_id, toolCallId);
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'active', revision = revision + 1,
            turn_sequence = turn_sequence + 1, updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
      return mapHumanInterrupt(this.#humanInterrupt(row.agent_id, toolCallId)!);
    });
  }

  resumeHumanResponseResult(
    ownership: AgentRunOwnership,
    toolCallId: string,
    now: number,
  ): HumanInterruptRecord {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentOwnership(ownership);
      const row = this.#requireOwnedOpenRow(ownership);
      const interrupt = this.#humanInterrupt(row.agent_id, toolCallId);
      if (!interrupt || interrupt.status !== "result-pending") {
        throw new WorkflowProtocolError("HumanInterruptResponseMissing", `Human Interrupt ${toolCallId} has no pending result`);
      }
      // Recovery projection delivery is retryable until context observation.
      // Reconciliation may therefore resume the same result more than once.
      if (row.open_state === "active") return mapHumanInterrupt(interrupt);
      if (row.open_state !== "waiting") throw this.#invalidTransition(row, "replay Human Interrupt result");
      this.#database.prepare(`
        UPDATE agent_activations
        SET open_state = 'active', revision = revision + 1,
            turn_sequence = turn_sequence + 1, updated_at_ms = ?
        WHERE activation_id = ?
      `).run(now, row.activation_id);
      return mapHumanInterrupt(interrupt);
    });
  }

  confirmHumanResponseResult(
    ownership: AgentRunOwnership,
    toolCallId: string,
    now: number,
  ): HumanInterruptRecord | undefined {
    return this.#withImmediateTransaction(() => {
      this.#assertAgentReference(ownership);
      const result = this.#database.prepare(`
        UPDATE human_interrupts
        SET status = 'consumed', updated_at_ms = ?
        WHERE agent_id = ? AND tool_call_id = ? AND status = 'result-pending'
      `).run(now, ownership.agentId, toolCallId);
      if (Number(result.changes) === 0) return undefined;
      const consumed = this.#humanInterrupt(ownership.agentId, toolCallId)!;
      recordRecoveryContinuationEvidence(this.#database, {
        activationId: consumed.activation_id,
        evidenceKind: "human-tool-result",
        evidenceId: toolCallId,
        now,
      });
      // result-pending is the durable authorization fence. Pi may persist the
      // already-returned result after cancellation releases this run's lease.
      this.#satisfyUndeclaredDependency(ownership.agentId, "human", toolCallId, now);
      return mapHumanInterrupt(this.#humanInterrupt(ownership.agentId, toolCallId)!);
    });
  }

  inspectHumanInterrupt(agent: AgentReference): HumanInterruptRecord | undefined {
    this.#assertAgentReference(agent);
    const row = this.#database.prepare(`
      SELECT agent_id, activation_id, tool_call_id, status, response_input_id,
             created_at_ms, updated_at_ms, terminal_reason
      FROM human_interrupts WHERE agent_id = ?
      ORDER BY updated_at_ms DESC, tool_call_id DESC LIMIT 1
    `).get(agent.agentId) as HumanInterruptRow | undefined;
    return row ? mapHumanInterrupt(row) : undefined;
  }

  inspectHumanInterruptToolCall(
    agent: AgentReference,
    toolCallId: string,
  ): HumanInterruptRecord | undefined {
    this.#assertAgentReference(agent);
    const row = this.#humanInterrupt(agent.agentId, toolCallId);
    return row ? mapHumanInterrupt(row) : undefined;
  }

  humanAttention(agent: AgentReference): boolean {
    this.#assertAgentReference(agent);
    return Boolean(this.#database.prepare("SELECT 1 FROM human_attention WHERE agent_id = ?").get(agent.agentId));
  }

  pendingUndeclaredNotice(agent: AgentReference): UndeclaredSettlementEpisode | undefined {
    this.#assertAgentReference(agent);
    const row = this.#openUndeclaredEpisode(agent.agentId);
    return row && !Number(row.notice_delivered) ? mapUndeclaredEpisode(row) : undefined;
  }

  confirmUndeclaredNotice(agent: AgentReference, episodeId: string, now: number): boolean {
    this.#assertAgentReference(agent);
    const result = this.#database.prepare(`
      UPDATE undeclared_settlement_episodes
      SET notice_delivered = 1, updated_at_ms = ?
      WHERE episode_id = ? AND agent_id = ? AND status = 'open' AND notice_delivered = 0
    `).run(now, episodeId, agent.agentId);
    return Number(result.changes) === 1;
  }

  queueUndeclaredNotice(agent: AgentReference, episodeId: string, now: number): UndeclaredSettlementEpisode | undefined {
    this.#assertAgentReference(agent);
    return this.#withImmediateTransaction(() => {
      const result = this.#database.prepare(`
        UPDATE undeclared_settlement_episodes
        SET notice_queued = 1, updated_at_ms = ?
        WHERE episode_id = ? AND agent_id = ? AND status = 'open' AND notice_delivered = 0
      `).run(now, episodeId, agent.agentId);
      if (Number(result.changes) !== 1) return undefined;
      return mapUndeclaredEpisode(this.#openUndeclaredEpisode(agent.agentId)!);
    });
  }

  inspectUndeclaredEpisode(agent: AgentReference): UndeclaredSettlementEpisode | undefined {
    this.#assertAgentReference(agent);
    const row = this.#database.prepare(`
      SELECT episode_id, agent_id, status, notice_queued, notice_delivered,
             notice_text, declared_waiting, repeat_triggered, trigger_kind,
             created_at_ms, updated_at_ms
      FROM undeclared_settlement_episodes WHERE agent_id = ?
      ORDER BY created_at_ms DESC LIMIT 1
    `).get(agent.agentId) as UndeclaredEpisodeRow | undefined;
    return row ? mapUndeclaredEpisode(row) : undefined;
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
      exhaustRecoveryForReplacement(this.#database, {
        activationId: row.activation_id,
        now,
        detail: `Replacement failed: ${failure.error}`,
      });
      recordRecoveryEpisodeForFailedActivation(this.#database, {
        activationId: row.activation_id,
        agentId: row.agent_id,
        now,
      });
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
    const activation = this.#database.prepare("SELECT agent_id FROM agent_activations WHERE activation_id = ?").get(activationId) as { agent_id: string } | undefined;
    if (!activation) throw new Error(`Activation ${activationId} is missing`);
    if (this.#unresolvedHumanInterrupt(activation.agent_id)) {
      return [{ kind: "human", dependencyId: HUMAN_DEPENDENCY_ID }];
    }
    const rows = this.#database.prepare(`
      SELECT dependency_kind, dependency_id, dependency_agent_id AS agent_id
      FROM activation_dependencies
      WHERE activation_id = ?
      UNION ALL
      SELECT 'agent' AS dependency_kind, request_id AS dependency_id,
             responder_agent_id AS agent_id
      FROM workflow_requests
      WHERE requester_activation_id = ?
        AND (status IN ('open', 'answered')
          OR (status = 'orphaned' AND orphan_notice_delivery_status = 'queued'))
      ORDER BY dependency_kind, dependency_id
    `).all(activationId, activationId) as unknown as DependencyRow[];
    if (rows.length === 0) return [{ kind: "undeclared", dependencyId: UNDECLARED_DEPENDENCY_ID }];
    return rows.map((row) => row.dependency_kind === "agent"
      ? { kind: "agent", dependencyId: row.dependency_id, agentId: row.agent_id! }
      : { kind: "operation", dependencyId: row.dependency_id });
  }

  #readDeclaredDependencies(activationId: string): DependencyRow[] {
    return this.#database.prepare(`
      SELECT dependency_kind, dependency_id, dependency_agent_id AS agent_id
      FROM activation_dependencies WHERE activation_id = ?
      UNION ALL
      SELECT 'agent' AS dependency_kind, request_id AS dependency_id,
             responder_agent_id AS agent_id
      FROM workflow_requests
      WHERE requester_activation_id = ?
        AND (status IN ('open', 'answered')
          OR (status = 'orphaned' AND orphan_notice_delivery_status = 'queued'))
    `).all(activationId, activationId) as unknown as DependencyRow[];
  }

  #humanInterrupt(agentId: string, toolCallId: string): HumanInterruptRow | undefined {
    return this.#database.prepare(`
      SELECT agent_id, activation_id, tool_call_id, status, response_input_id,
             created_at_ms, updated_at_ms, terminal_reason
      FROM human_interrupts WHERE agent_id = ? AND tool_call_id = ?
    `).get(agentId, toolCallId) as HumanInterruptRow | undefined;
  }

  #openHumanInterrupt(agentId: string): HumanInterruptRow | undefined {
    return this.#database.prepare(`
      SELECT agent_id, activation_id, tool_call_id, status, response_input_id,
             created_at_ms, updated_at_ms, terminal_reason
      FROM human_interrupts
      WHERE agent_id = ? AND status IN ('pending', 'response-bound')
      ORDER BY created_at_ms DESC LIMIT 1
    `).get(agentId) as HumanInterruptRow | undefined;
  }

  #unresolvedHumanInterrupt(agentId: string): HumanInterruptRow | undefined {
    return this.#database.prepare(`
      SELECT agent_id, activation_id, tool_call_id, status, response_input_id,
             created_at_ms, updated_at_ms, terminal_reason
      FROM human_interrupts
      WHERE agent_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
      ORDER BY created_at_ms DESC LIMIT 1
    `).get(agentId) as HumanInterruptRow | undefined;
  }

  #openUndeclaredEpisode(agentId: string): UndeclaredEpisodeRow | undefined {
    return this.#database.prepare(`
      SELECT episode_id, agent_id, status, notice_queued, notice_delivered,
             notice_text, declared_waiting, repeat_triggered, trigger_kind,
             created_at_ms, updated_at_ms
      FROM undeclared_settlement_episodes
      WHERE agent_id = ? AND status = 'open'
    `).get(agentId) as UndeclaredEpisodeRow | undefined;
  }

  #recordUndeclaredSettlement(
    row: ActivationRow,
    now: number,
    actorRole: "ordinary" | "moderator",
  ): void {
    const existing = this.#openUndeclaredEpisode(row.agent_id);
    if (!existing) {
      this.#database.prepare(`
        INSERT INTO undeclared_settlement_episodes (
          episode_id, agent_id, status, notice_queued, notice_delivered,
          notice_text, declared_waiting, repeat_triggered, trigger_kind,
          created_at_ms, updated_at_ms
        ) VALUES (?, ?, 'open', 0, 0, ?, 0, 0, NULL, ?, ?)
      `).run(`${row.activation_id}:undeclared`, row.agent_id, UNDECLARED_SETTLEMENT_NOTICE_TEXT, now, now);
      return;
    }
    if (Number(existing.notice_delivered) === 0 || Number(existing.repeat_triggered) === 1) return;
    this.#database.prepare(`
      UPDATE undeclared_settlement_episodes
      SET repeat_triggered = 1, trigger_kind = ?, updated_at_ms = ?
      WHERE episode_id = ? AND repeat_triggered = 0
    `).run(actorRole === "moderator" ? "owner-handoff" : "incident", now, existing.episode_id);
  }

  #declareUndeclaredDependencies(
    agentId: string,
    dependencies: DependencyRow[],
    now: number,
  ): void {
    for (const dependency of dependencies) {
      this.#declareUndeclaredDependency(agentId, dependency.dependency_kind, dependency.dependency_id, now);
    }
  }

  #declareUndeclaredDependency(
    agentId: string,
    kind: "agent" | "operation" | "human",
    dependencyId: string,
    now: number,
  ): void {
    const episode = this.#openUndeclaredEpisode(agentId);
    if (!episode) return;
    this.#database.prepare(`
      INSERT INTO undeclared_settlement_dependencies (
        episode_id, dependency_kind, dependency_id, created_at_ms
      ) VALUES (?, ?, ?, ?)
      ON CONFLICT (episode_id, dependency_kind, dependency_id) DO NOTHING
    `).run(episode.episode_id, kind, dependencyId, now);
  }

  #satisfyUndeclaredDependency(
    agentId: string,
    kind: "agent" | "operation" | "human",
    dependencyId: string,
    now: number,
  ): boolean {
    const episode = this.#openUndeclaredEpisode(agentId);
    if (!episode) return false;
    const dependency = this.#database.prepare(`
      DELETE FROM undeclared_settlement_dependencies
      WHERE episode_id = ? AND dependency_kind = ? AND dependency_id = ?
    `).run(episode.episode_id, kind, dependencyId);
    if (Number(dependency.changes) === 0) return false;
    this.#database.prepare(`
      UPDATE undeclared_settlement_episodes
      SET status = 'closed', updated_at_ms = ?
      WHERE episode_id = ? AND status = 'open'
    `).run(now, episode.episode_id);
    return true;
  }

  #assertOrdinarySubagent(agentId: string, actorRole: "ordinary" | "moderator"): void {
    if (agentId === this.#workflowOwnerId() || actorRole !== "ordinary") {
      throw new WorkflowProtocolError("HumanInterruptForbidden", "Only an ordinary Subagent can ask a human");
    }
  }

  #assertAgentReference(reference: AgentReference): void {
    if (reference.workflowOwnerId !== this.#workflowOwnerId()) {
      throw new WorkflowProtocolError("WorkflowMismatch", `Identity ${reference.agentId} belongs to another Workflow`);
    }
    this.#requireAgent(reference.agentId);
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

function mapHumanInterrupt(row: HumanInterruptRow): HumanInterruptRecord {
  return {
    toolCallId: row.tool_call_id,
    status: row.status,
    ...(row.response_input_id ? { responseInputId: row.response_input_id } : {}),
    createdAtMs: Number(row.created_at_ms),
    updatedAtMs: Number(row.updated_at_ms),
  };
}

function mapUndeclaredEpisode(row: UndeclaredEpisodeRow): UndeclaredSettlementEpisode {
  return {
    episodeId: row.episode_id,
    noticeId: `${row.episode_id}:notice`,
    noticeText: row.notice_text,
    agentId: row.agent_id,
    status: row.status,
    noticeQueued: Number(row.notice_queued) === 1,
    noticeDelivered: Number(row.notice_delivered) === 1,
    repeatTriggered: Number(row.repeat_triggered) === 1,
    ...(row.trigger_kind ? { triggerKind: row.trigger_kind } : {}),
    createdAtMs: Number(row.created_at_ms),
    updatedAtMs: Number(row.updated_at_ms),
  };
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new Error(`Activation ${label} must not be empty`);
}
