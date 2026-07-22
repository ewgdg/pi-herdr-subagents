import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { DirectSignalStore } from "./sqlite-message-store.ts";
import { cancelOpenRequestInTransaction } from "./request-cancellation-transition.ts";
import {
  WorkflowProtocolError,
  type AgentReference,
} from "./workflow-types.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const CANCELLATION_DEPENDENCY_PREFIX = "cancellation:";

export type CancellationOperationState =
  | "terminating"
  | "ready-to-commit"
  | "in-doubt"
  | "committed";

export type CancellationAuthority =
  | { kind: "workflow-owner" }
  | { kind: "direct-spawner" }
  | { kind: "incident-control"; incidentId: string; rationale: string }
  /** Runtime-attributed propagation from a committed root cancellation. */
  | { kind: "cascade"; rootOperationId: string };

export interface AgentRunLocator {
  surface: string;
}

export type AgentRunInspection =
  | { kind: "present" }
  | { kind: "missing" }
  | { kind: "unavailable"; error?: string };

export interface AgentRunTerminator {
  inspect(locator: AgentRunLocator): Promise<AgentRunInspection>;
  close(locator: AgentRunLocator): Promise<void>;
}

export interface ActivationCancellationRecord {
  operationId: string;
  actorAgentId: string;
  sourceId: string;
  authority: CancellationAuthority;
  targetAgentId: string;
  activationId: string;
  runId: string;
  fencingEpoch: number;
  activationRevision: number;
  runLocator?: string;
  state: CancellationOperationState;
  terminationAttempts: number;
  lastError?: string;
  createdAtMs: number;
  updatedAtMs: number;
  committedAtMs?: number;
}

export class CancellationInDoubtError extends WorkflowProtocolError {
  readonly operation: ActivationCancellationRecord;

  constructor(operation: ActivationCancellationRecord) {
    super(
      "CancellationInDoubt",
      `Termination of activation ${operation.activationId} is unconfirmed; cancellation operation ${operation.operationId} remains in doubt`,
    );
    this.operation = operation;
  }
}

interface CancellationRow {
  operation_id: string;
  actor_agent_id: string;
  source_id: string;
  authority_kind: "workflow-owner" | "direct-spawner" | "incident-control" | "cascade";
  incident_id: string | null;
  rationale: string | null;
  root_operation_id: string | null;
  target_agent_id: string;
  activation_id: string;
  run_id: string;
  fencing_epoch: number;
  activation_revision: number;
  run_locator: string | null;
  state: CancellationOperationState;
  termination_attempts: number;
  last_error: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  committed_at_ms: number | null;
}

interface ActivationRow {
  activation_id: string;
  agent_id: string;
  run_id: string;
  fencing_epoch: number;
  revision: number;
  phase: "open" | "ended";
  ended_outcome: "completed" | "failed" | "cancelled" | null;
}

interface RequestTransformationRow {
  request_id: string;
  requester_agent_id: string;
  responder_agent_id: string;
  delivery_status: "bound" | "queued" | "delivered" | "suppressed";
  in_reply_to_request_id: string | null;
}

export interface DescendantCancellationPlan {
  rootOperationId: string;
  cancelled: Array<{ agentId: string; activationId: string }>;
  survivors: string[];
}

export class ActivationCancellationStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    // Messaging owns the shared Router/Request tables used by the finalizer.
    // Opening it here also applies additive notice-column compatibility.
    const messages = new DirectSignalStore(databasePath, busyTimeoutMs);
    messages.close();
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    this.#migrateAuthoritySchema();
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS activation_cancellations (
        operation_id TEXT PRIMARY KEY,
        actor_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        source_id TEXT NOT NULL,
        authority_kind TEXT NOT NULL CHECK (authority_kind IN ('workflow-owner', 'direct-spawner', 'incident-control', 'cascade')),
        incident_id TEXT,
        rationale TEXT,
        root_operation_id TEXT REFERENCES activation_cancellations(operation_id),
        target_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        activation_id TEXT NOT NULL UNIQUE REFERENCES agent_activations(activation_id),
        run_id TEXT NOT NULL,
        fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0),
        activation_revision INTEGER NOT NULL CHECK (activation_revision > 0),
        run_locator TEXT,
        state TEXT NOT NULL CHECK (state IN ('terminating', 'ready-to-commit', 'in-doubt', 'committed')),
        termination_attempts INTEGER NOT NULL DEFAULT 0 CHECK (termination_attempts >= 0),
        last_error TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        committed_at_ms INTEGER,
        UNIQUE (actor_agent_id, source_id),
        CHECK ((authority_kind = 'incident-control' AND incident_id IS NOT NULL AND rationale IS NOT NULL AND root_operation_id IS NULL)
          OR (authority_kind = 'cascade' AND incident_id IS NULL AND rationale IS NULL AND root_operation_id IS NOT NULL)
          OR (authority_kind IN ('workflow-owner', 'direct-spawner') AND incident_id IS NULL AND rationale IS NULL AND root_operation_id IS NULL)),
        CHECK ((state = 'committed' AND committed_at_ms IS NOT NULL)
          OR (state != 'committed' AND committed_at_ms IS NULL))
      ) STRICT;
      CREATE INDEX IF NOT EXISTS activation_cancellations_target_state
        ON activation_cancellations (target_agent_id, state);
      CREATE TABLE IF NOT EXISTS activation_cancellation_sources (
        actor_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        source_id TEXT NOT NULL,
        operation_id TEXT NOT NULL REFERENCES activation_cancellations(operation_id),
        bound_at_ms INTEGER NOT NULL,
        PRIMARY KEY (actor_agent_id, source_id)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS activation_cancellation_cascades (
        root_operation_id TEXT PRIMARY KEY REFERENCES activation_cancellations(operation_id),
        planned_at_ms INTEGER NOT NULL
      ) STRICT;
      CREATE TABLE IF NOT EXISTS activation_cancellation_descendants (
        root_operation_id TEXT NOT NULL REFERENCES activation_cancellations(operation_id),
        agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        disposition TEXT NOT NULL CHECK (disposition IN ('cancelled', 'survives')),
        activation_id TEXT REFERENCES agent_activations(activation_id),
        cascade_state TEXT NOT NULL DEFAULT 'pending'
          CHECK (cascade_state IN ('pending', 'cancelled', 'skipped', 'survives')),
        depth INTEGER NOT NULL CHECK (depth > 0),
        PRIMARY KEY (root_operation_id, agent_id),
        CHECK ((disposition = 'cancelled' AND activation_id IS NOT NULL)
          OR (disposition = 'survives' AND activation_id IS NULL)),
        CHECK ((disposition = 'cancelled' AND cascade_state IN ('pending', 'cancelled', 'skipped'))
          OR (disposition = 'survives' AND cascade_state = 'survives'))
      ) STRICT;
      CREATE INDEX IF NOT EXISTS activation_cancellation_descendants_root_disposition
        ON activation_cancellation_descendants (root_operation_id, disposition, depth, agent_id);
      INSERT OR IGNORE INTO activation_cancellation_sources (
        actor_agent_id, source_id, operation_id, bound_at_ms
      ) SELECT actor_agent_id, source_id, operation_id, created_at_ms
      FROM activation_cancellations;
    `);
    this.#initializeDescendantCascadeState();
  }

  #initializeDescendantCascadeState(): void {
    const columns = this.#database.prepare("PRAGMA table_info(activation_cancellation_descendants)")
      .all() as Array<{ name: string }>;
    if (columns.some((column) => column.name === "cascade_state")) return;
    this.#database.exec(`ALTER TABLE activation_cancellation_descendants
      ADD COLUMN cascade_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (cascade_state IN ('pending', 'cancelled', 'skipped', 'survives'))`);
    this.#database.prepare(`UPDATE activation_cancellation_descendants
      SET cascade_state = 'survives' WHERE disposition = 'survives'`
    ).run();
  }

  /** Rebuild once because SQLite cannot widen an existing CHECK constraint. */
  #migrateAuthoritySchema(): void {
    const observed = this.#database.prepare(
      "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'activation_cancellations'",
    ).get() as { sql: string } | undefined;
    if (!observed || (observed.sql.includes("'cascade'") && observed.sql.includes("root_operation_id"))) return;

    this.#database.exec("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE");
    try {
      // A concurrent opener may have completed this migration while this
      // connection waited for the write lock. Snapshot only after rechecking.
      const table = this.#database.prepare(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'activation_cancellations'",
      ).get() as { sql: string } | undefined;
      if (!table || (table.sql.includes("'cascade'") && table.sql.includes("root_operation_id"))) {
        this.#database.exec("COMMIT");
        return;
      }
      const sourceTable = this.#database.prepare(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'activation_cancellation_sources'",
      ).get();
      const sources = sourceTable
        ? this.#database.prepare(`SELECT actor_agent_id, source_id, operation_id, bound_at_ms
            FROM activation_cancellation_sources`
          ).all() as Array<{
            actor_agent_id: string;
            source_id: string;
            operation_id: string;
            bound_at_ms: number;
          }>
        : [];
      this.#database.exec(`
        CREATE TABLE activation_cancellations_v2 (
          operation_id TEXT PRIMARY KEY,
          actor_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
          source_id TEXT NOT NULL,
          authority_kind TEXT NOT NULL CHECK (authority_kind IN ('workflow-owner', 'direct-spawner', 'incident-control', 'cascade')),
          incident_id TEXT,
          rationale TEXT,
          root_operation_id TEXT REFERENCES activation_cancellations_v2(operation_id),
          target_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
          activation_id TEXT NOT NULL UNIQUE REFERENCES agent_activations(activation_id),
          run_id TEXT NOT NULL,
          fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0),
          activation_revision INTEGER NOT NULL CHECK (activation_revision > 0),
          run_locator TEXT,
          state TEXT NOT NULL CHECK (state IN ('terminating', 'ready-to-commit', 'in-doubt', 'committed')),
          termination_attempts INTEGER NOT NULL DEFAULT 0 CHECK (termination_attempts >= 0),
          last_error TEXT,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          committed_at_ms INTEGER,
          UNIQUE (actor_agent_id, source_id),
          CHECK ((authority_kind = 'incident-control' AND incident_id IS NOT NULL AND rationale IS NOT NULL AND root_operation_id IS NULL)
            OR (authority_kind = 'cascade' AND incident_id IS NULL AND rationale IS NULL AND root_operation_id IS NOT NULL)
            OR (authority_kind IN ('workflow-owner', 'direct-spawner') AND incident_id IS NULL AND rationale IS NULL AND root_operation_id IS NULL)),
          CHECK ((state = 'committed' AND committed_at_ms IS NOT NULL)
            OR (state != 'committed' AND committed_at_ms IS NULL))
        ) STRICT;
        INSERT INTO activation_cancellations_v2 (
          operation_id, actor_agent_id, source_id, authority_kind, incident_id, rationale, root_operation_id,
          target_agent_id, activation_id, run_id, fencing_epoch, activation_revision, run_locator,
          state, termination_attempts, last_error, created_at_ms, updated_at_ms, committed_at_ms
        ) SELECT
          operation_id, actor_agent_id, source_id, authority_kind, incident_id, rationale, NULL,
          target_agent_id, activation_id, run_id, fencing_epoch, activation_revision, run_locator,
          state, termination_attempts, last_error, created_at_ms, updated_at_ms, committed_at_ms
        FROM activation_cancellations;
        DROP TABLE IF EXISTS activation_cancellation_sources;
        DROP TABLE activation_cancellations;
        ALTER TABLE activation_cancellations_v2 RENAME TO activation_cancellations;
        CREATE TABLE activation_cancellation_sources (
          actor_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
          source_id TEXT NOT NULL,
          operation_id TEXT NOT NULL REFERENCES activation_cancellations(operation_id),
          bound_at_ms INTEGER NOT NULL,
          PRIMARY KEY (actor_agent_id, source_id)
        ) STRICT;
      `);
      const insertSource = this.#database.prepare(`INSERT INTO activation_cancellation_sources (
        actor_agent_id, source_id, operation_id, bound_at_ms
      ) VALUES (?, ?, ?, ?)`);
      for (const source of sources) {
        insertSource.run(source.actor_agent_id, source.source_id, source.operation_id, source.bound_at_ms);
      }
      this.#database.exec("COMMIT");
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
      throw error;
    } finally {
      this.#database.exec("PRAGMA foreign_keys = ON");
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  claim(input: {
    actor: AgentReference;
    target: AgentReference;
    sourceId: string;
    operationId: string;
    authority?: CancellationAuthority;
    incidentControlAuthorized?: boolean;
    cascadeAuthorized?: boolean;
    /** A cascade must never cancel work that began after its root was planned. */
    expectedActivationId?: string;
    now: number;
  }): ActivationCancellationRecord {
    assertNonEmpty(input.sourceId, "Cancellation source ID");
    assertNonEmpty(input.operationId, "Cancellation operation ID");
    return this.#withImmediateTransaction(() => {
      const ownerAgentId = this.#workflowOwnerId();
      this.#assertReference(input.actor, ownerAgentId);
      this.#assertReference(input.target, ownerAgentId);
      if (input.target.agentId === ownerAgentId) {
        throw new WorkflowProtocolError("OwnerActivationForbidden", "Workflow Owner has no cancellable activation");
      }
      const sourceBound = this.#database.prepare(`SELECT operation.*
        FROM activation_cancellation_sources source
        JOIN activation_cancellations operation ON operation.operation_id = source.operation_id
        WHERE source.actor_agent_id = ? AND source.source_id = ?`
      ).get(input.actor.agentId, input.sourceId) as CancellationRow | undefined;
      if (sourceBound) {
        if (sourceBound.target_agent_id !== input.target.agentId) {
          throw new WorkflowProtocolError(
            "ActivationCancellationConflict",
            `Cancellation source ${input.sourceId} is already bound to Agent ${sourceBound.target_agent_id}`,
          );
        }
        return mapCancellation(sourceBound);
      }
      const target = this.#database.prepare(
        "SELECT spawner_agent_id FROM workflow_agents WHERE agent_id = ?",
      ).get(input.target.agentId) as { spawner_agent_id: string | null } | undefined;
      if (!target) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${input.target.agentId}`);
      const authority = input.authority ?? (input.actor.agentId === ownerAgentId
        ? { kind: "workflow-owner" as const }
        : target.spawner_agent_id === input.actor.agentId
          ? { kind: "direct-spawner" as const }
          : undefined);
      if (!authority
        || (authority.kind === "incident-control" && input.incidentControlAuthorized !== true)
        || (authority.kind === "cascade" && input.cascadeAuthorized !== true)) {
        throw new WorkflowProtocolError(
          "ActivationCancellationUnauthorized",
          `Agent ${input.actor.agentId} cannot cancel activation owned by Agent ${input.target.agentId}`,
        );
      }
      if (authority.kind === "workflow-owner" && input.actor.agentId !== ownerAgentId) {
        throw new WorkflowProtocolError("ActivationCancellationUnauthorized", "Workflow Owner authority attribution does not match the actor");
      }
      if (authority.kind === "direct-spawner" && target.spawner_agent_id !== input.actor.agentId) {
        throw new WorkflowProtocolError("ActivationCancellationUnauthorized", "Direct Spawner authority attribution does not match the actor");
      }
      if (authority.kind === "cascade") this.#assertCascadeAuthority(input.actor, input.target, authority);

      if (authority.kind === "cascade" && input.expectedActivationId) {
        const inherited = this.#readByActivation(input.expectedActivationId);
        if (inherited?.target_agent_id === input.target.agentId
          && inherited.root_operation_id === authority.rootOperationId) {
          const bound = this.#database.prepare(`INSERT INTO activation_cancellation_sources (
            actor_agent_id, source_id, operation_id, bound_at_ms
          ) VALUES (?, ?, ?, ?)`
          ).run(input.actor.agentId, input.sourceId, inherited.operation_id, input.now);
          if (Number(bound.changes) !== 1) throw new Error(`Cascade source ${input.sourceId} was not bound`);
          return mapCancellation(inherited);
        }
      }

      const activation = this.#currentActivation(input.target.agentId);
      if (!activation || activation.phase !== "open") {
        throw new WorkflowProtocolError(
          "InvalidLifecycleTransition",
          `Agent ${input.target.agentId} has no open activation to cancel`,
        );
      }
      if (input.expectedActivationId && activation.activation_id !== input.expectedActivationId) {
        throw new WorkflowProtocolError(
          "InvalidLifecycleTransition",
          `Agent ${input.target.agentId} no longer has cascade activation ${input.expectedActivationId}`,
        );
      }
      const existing = this.#readByActivation(activation.activation_id);
      if (existing) {
        if (existing.state !== "committed") {
          if (existing.actor_agent_id !== input.actor.agentId) {
            throw new WorkflowProtocolError(
              "ActivationCancellationUnauthorized",
              `Cancellation ${existing.operation_id} remains controlled by its original actor ${existing.actor_agent_id}`,
            );
          }
          // A fresh Pi tool call gets a fresh source ID after an in-doubt
          // result. Authorization is rechecked above, but the original
          // operation identity remains the sole claim on this activation.
          const bound = this.#database.prepare(`INSERT INTO activation_cancellation_sources (
            actor_agent_id, source_id, operation_id, bound_at_ms
          ) VALUES (?, ?, ?, ?)`
          ).run(input.actor.agentId, input.sourceId, existing.operation_id, input.now);
          if (Number(bound.changes) !== 1) throw new Error(`Cancellation source ${input.sourceId} was not bound`);
          return mapCancellation(existing);
        }
        throw new WorkflowProtocolError(
          "ActivationCancellationConflict",
          `Activation ${activation.activation_id} already has cancellation operation ${existing.operation_id}`,
        );
      }

      const ownership = this.#database.prepare(`SELECT owner_id, fencing_epoch FROM ownership
        WHERE resource_id = ?`
      ).get(runResourceId(ownerAgentId, input.target.agentId)) as { owner_id: string; fencing_epoch: number } | undefined;
      if (!ownership || ownership.owner_id !== activation.run_id
        || Number(ownership.fencing_epoch) !== Number(activation.fencing_epoch)) {
        throw new WorkflowProtocolError("OwnershipLost", `Activation ${activation.activation_id} has no exact Agent Run ownership`);
      }
      const checkpoint = this.#database.prepare(`SELECT value, fencing_epoch FROM fenced_state
        WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'`
      ).get(runResourceId(ownerAgentId, input.target.agentId)) as { value: string; fencing_epoch: number } | undefined;
      const runLocator = checkpoint && Number(checkpoint.fencing_epoch) === Number(activation.fencing_epoch)
        ? checkpoint.value
        : null;
      const activationRevision = Number(activation.revision) + 1;
      this.#database.prepare(`INSERT INTO activation_cancellations (
        operation_id, actor_agent_id, source_id, authority_kind, incident_id, rationale, root_operation_id,
        target_agent_id, activation_id, run_id, fencing_epoch, activation_revision,
        run_locator, state, termination_attempts, last_error, created_at_ms, updated_at_ms, committed_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'terminating', 0, NULL, ?, ?, NULL)`
      ).run(
        input.operationId,
        input.actor.agentId,
        input.sourceId,
        authority.kind,
        authority.kind === "incident-control" ? authority.incidentId : null,
        authority.kind === "incident-control" ? authority.rationale : null,
        authority.kind === "cascade" ? authority.rootOperationId : null,
        input.target.agentId,
        activation.activation_id,
        activation.run_id,
        activation.fencing_epoch,
        activationRevision,
        runLocator,
        input.now,
        input.now,
      );
      const sourceBinding = this.#database.prepare(`INSERT INTO activation_cancellation_sources (
        actor_agent_id, source_id, operation_id, bound_at_ms
      ) VALUES (?, ?, ?, ?)`
      ).run(input.actor.agentId, input.sourceId, input.operationId, input.now);
      if (Number(sourceBinding.changes) !== 1) throw new Error(`Cancellation source ${input.sourceId} was not bound`);
      this.#database.prepare(`INSERT INTO activation_dependencies (
        activation_id, dependency_kind, dependency_id, dependency_agent_id, created_at_ms
      ) VALUES (?, 'operation', ?, NULL, ?)`
      ).run(activation.activation_id, `${CANCELLATION_DEPENDENCY_PREFIX}${input.operationId}`, input.now);
      const revised = this.#database.prepare(`UPDATE agent_activations
        SET revision = revision + 1, updated_at_ms = ?
        WHERE activation_id = ? AND phase = 'open' AND revision = ?`
      ).run(input.now, activation.activation_id, activation.revision);
      if (Number(revised.changes) !== 1) {
        throw new WorkflowProtocolError("StaleLifecycleTransition", `Activation ${activation.activation_id} changed while cancellation was claimed`);
      }
      return mapCancellation(this.#readRequired(input.operationId));
    });
  }

  inspectOperation(operationId: string): ActivationCancellationRecord | undefined {
    const row = this.#database.prepare("SELECT * FROM activation_cancellations WHERE operation_id = ?")
      .get(operationId) as CancellationRow | undefined;
    return row ? mapCancellation(row) : undefined;
  }

  inspectForAgent(target: AgentReference): ActivationCancellationRecord | undefined {
    this.#assertReference(target, this.#workflowOwnerId());
    const row = this.#database.prepare(`SELECT * FROM activation_cancellations
      WHERE target_agent_id = ? ORDER BY created_at_ms DESC, operation_id DESC LIMIT 1`
    ).get(target.agentId) as CancellationRow | undefined;
    return row ? mapCancellation(row) : undefined;
  }

  inspectForRun(input: { workflowOwnerId: string; agentId: string; runId: string; fencingEpoch: number }): ActivationCancellationRecord | undefined {
    if (input.workflowOwnerId !== this.#workflowOwnerId()) return undefined;
    const row = this.#database.prepare(`SELECT * FROM activation_cancellations
      WHERE target_agent_id = ? AND run_id = ? AND fencing_epoch = ?
      ORDER BY created_at_ms DESC LIMIT 1`
    ).get(input.agentId, input.runId, input.fencingEpoch) as CancellationRow | undefined;
    return row ? mapCancellation(row) : undefined;
  }

  /**
   * Freeze the only Request edges that keep a descendant outside the cascade.
   * The root itself cannot survive: its direct cancellation has already won.
   */
  planDescendantCancellation(rootOperationId: string, now: number): DescendantCancellationPlan {
    return this.#withImmediateTransaction(() => {
      const root = this.#readRequired(rootOperationId);
      if (root.authority_kind === "cascade") {
        throw new WorkflowProtocolError("ActivationCancellationConflict", "A cascade operation cannot start another cascade");
      }
      const planned = this.#database.prepare(`SELECT 1 FROM activation_cancellation_cascades
        WHERE root_operation_id = ?`
      ).get(rootOperationId);
      if (planned) return this.#readDescendantPlan(rootOperationId);

      const agents = this.#database.prepare(`SELECT agent_id, spawner_agent_id
        FROM workflow_agents ORDER BY agent_id`
      ).all() as Array<{ agent_id: string; spawner_agent_id: string | null }>;
      const children = new Map<string, string[]>();
      for (const agent of agents) {
        if (!agent.spawner_agent_id) continue;
        const siblings = children.get(agent.spawner_agent_id) ?? [];
        siblings.push(agent.agent_id);
        children.set(agent.spawner_agent_id, siblings);
      }
      const descendants: Array<{ agentId: string; depth: number }> = [];
      const queue = (children.get(root.target_agent_id) ?? []).map((agentId) => ({ agentId, depth: 1 }));
      for (let index = 0; index < queue.length; index += 1) {
        const candidate = queue[index]!;
        descendants.push(candidate);
        for (const child of children.get(candidate.agentId) ?? []) {
          queue.push({ agentId: child, depth: candidate.depth + 1 });
        }
      }
      const subtree = new Set([root.target_agent_id, ...descendants.map((item) => item.agentId)]);
      const descendantIds = new Set(descendants.map((item) => item.agentId));
      const requests = this.#database.prepare(`SELECT requester_agent_id, responder_agent_id
        FROM workflow_requests WHERE status = 'open'`
      ).all() as Array<{ requester_agent_id: string; responder_agent_id: string }>;
      const survivors = new Set<string>();
      for (const request of requests) {
        if (descendantIds.has(request.responder_agent_id) && !subtree.has(request.requester_agent_id)) {
          survivors.add(request.responder_agent_id);
        }
      }
      let changed = true;
      while (changed) {
        changed = false;
        for (const request of requests) {
          if (survivors.has(request.requester_agent_id)
            && descendantIds.has(request.responder_agent_id)
            && !survivors.has(request.responder_agent_id)) {
            survivors.add(request.responder_agent_id);
            changed = true;
          }
        }
      }

      this.#database.prepare(`INSERT INTO activation_cancellation_cascades (
        root_operation_id, planned_at_ms
      ) VALUES (?, ?)`
      ).run(rootOperationId, now);
      const insert = this.#database.prepare(`INSERT INTO activation_cancellation_descendants (
        root_operation_id, agent_id, disposition, activation_id, cascade_state, depth
      ) VALUES (?, ?, ?, ?, ?, ?)`);
      for (const descendant of descendants) {
        if (survivors.has(descendant.agentId)) {
          insert.run(rootOperationId, descendant.agentId, "survives", null, "survives", descendant.depth);
          continue;
        }
        const activation = this.#currentActivation(descendant.agentId);
        if (activation?.phase === "open") {
          insert.run(rootOperationId, descendant.agentId, "cancelled", activation.activation_id, "pending", descendant.depth);
        }
      }
      return this.#readDescendantPlan(rootOperationId);
    });
  }

  /** Bind a later public tool call to an incomplete cascade whose root already ended. */
  resumeIncompleteCascade(input: {
    actor: AgentReference;
    target: AgentReference;
    sourceId: string;
    now: number;
  }): ActivationCancellationRecord | undefined {
    return this.#withImmediateTransaction(() => {
      const ownerAgentId = this.#workflowOwnerId();
      this.#assertReference(input.actor, ownerAgentId);
      this.#assertReference(input.target, ownerAgentId);
      this.#assertHistoricalCancellationAuthority(input.actor, input.target.agentId, ownerAgentId);
      const root = this.#database.prepare(`SELECT operation.*
        FROM activation_cancellations operation
        JOIN activation_cancellation_cascades cascade
          ON cascade.root_operation_id = operation.operation_id
        WHERE operation.target_agent_id = ?
          AND operation.state = 'committed'
          AND operation.authority_kind != 'cascade'
          AND EXISTS (
            SELECT 1 FROM activation_cancellation_descendants descendant
            WHERE descendant.root_operation_id = operation.operation_id
              AND descendant.disposition = 'cancelled'
              AND descendant.cascade_state = 'pending'
          )
        ORDER BY operation.committed_at_ms DESC, operation.operation_id DESC
        LIMIT 1`
      ).get(input.target.agentId) as CancellationRow | undefined;
      if (!root) return undefined;
      const existingSource = this.#database.prepare(`SELECT operation.*
        FROM activation_cancellation_sources source
        JOIN activation_cancellations operation ON operation.operation_id = source.operation_id
        WHERE source.actor_agent_id = ? AND source.source_id = ?`
      ).get(input.actor.agentId, input.sourceId) as CancellationRow | undefined;
      if (existingSource) {
        if (existingSource.operation_id !== root.operation_id) {
          throw new WorkflowProtocolError(
            "ActivationCancellationConflict",
            `Cancellation source ${input.sourceId} is already bound to a different cancellation operation`,
          );
        }
        return mapCancellation(root);
      }
      const bound = this.#database.prepare(`INSERT INTO activation_cancellation_sources (
        actor_agent_id, source_id, operation_id, bound_at_ms
      ) VALUES (?, ?, ?, ?)`
      ).run(input.actor.agentId, input.sourceId, root.operation_id, input.now);
      if (Number(bound.changes) !== 1) throw new Error(`Cascade retry source ${input.sourceId} was not bound`);
      return mapCancellation(root);
    });
  }

  markPlannedDescendantCancelled(input: {
    rootOperationId: string;
    agentId: string;
    activationId: string;
  }): void {
    this.#setPlannedDescendantState({ ...input, state: "cancelled" });
  }

  /** Mark only a definitively ended or replaced planned activation as harmless. */
  skipStalePlannedDescendant(input: {
    rootOperationId: string;
    agentId: string;
    activationId: string;
  }): boolean {
    return this.#withImmediateTransaction(() => {
      const planned = this.#database.prepare(`SELECT cascade_state FROM activation_cancellation_descendants
        WHERE root_operation_id = ? AND agent_id = ? AND disposition = 'cancelled' AND activation_id = ?`
      ).get(input.rootOperationId, input.agentId, input.activationId) as { cascade_state: string } | undefined;
      if (!planned) throw new Error(`Cascade plan has no matching descendant ${input.agentId}`);
      if (planned.cascade_state === "skipped") return true;
      if (planned.cascade_state !== "pending") return false;
      const plannedActivation = this.#database.prepare(`SELECT phase FROM agent_activations
        WHERE activation_id = ? AND agent_id = ?`
      ).get(input.activationId, input.agentId) as { phase: "open" | "ended" } | undefined;
      const current = this.#currentActivation(input.agentId);
      if (!plannedActivation || (plannedActivation.phase !== "ended" && current?.activation_id === input.activationId)) {
        return false;
      }
      const skipped = this.#database.prepare(`UPDATE activation_cancellation_descendants
        SET cascade_state = 'skipped'
        WHERE root_operation_id = ? AND agent_id = ? AND activation_id = ?
          AND disposition = 'cancelled' AND cascade_state = 'pending'`
      ).run(input.rootOperationId, input.agentId, input.activationId);
      if (Number(skipped.changes) !== 1) throw new Error(`Cascade plan lost stale descendant ${input.agentId}`);
      return true;
    });
  }

  markReady(operationId: string, now: number): ActivationCancellationRecord {
    return this.#withImmediateTransaction(() => {
      const operation = this.#readRequired(operationId);
      if (operation.state === "committed" || operation.state === "ready-to-commit") return mapCancellation(operation);
      this.#database.prepare(`UPDATE activation_cancellations
        SET state = 'ready-to-commit', termination_attempts = termination_attempts + 1,
            last_error = NULL, updated_at_ms = ?
        WHERE operation_id = ? AND state IN ('terminating', 'in-doubt')`
      ).run(now, operationId);
      return mapCancellation(this.#readRequired(operationId));
    });
  }

  markInDoubt(operationId: string, error: string, now: number): ActivationCancellationRecord {
    assertNonEmpty(error, "Cancellation uncertainty");
    return this.#withImmediateTransaction(() => {
      const operation = this.#readRequired(operationId);
      if (operation.state === "committed") return mapCancellation(operation);
      this.#database.prepare(`UPDATE activation_cancellations
        SET state = 'in-doubt', termination_attempts = termination_attempts + 1,
            last_error = ?, updated_at_ms = ?
        WHERE operation_id = ? AND state != 'committed'`
      ).run(error, now, operationId);
      return mapCancellation(this.#readRequired(operationId));
    });
  }

  /** One write transaction owns all cancellation effects and completion arbitration. */
  finalize(operationId: string, now: number): ActivationCancellationRecord {
    return this.#withImmediateTransaction(() => {
      const operation = this.#readRequired(operationId);
      if (operation.state === "committed") return mapCancellation(operation);
      if (operation.state !== "ready-to-commit") {
        throw new WorkflowProtocolError("CancellationInDoubt", `Cancellation ${operationId} has no confirmed termination`);
      }
      const ownerAgentId = this.#workflowOwnerId();
      const activation = this.#database.prepare(`SELECT activation_id, agent_id, run_id, fencing_epoch,
          revision, phase, ended_outcome
        FROM agent_activations WHERE activation_id = ?`
      ).get(operation.activation_id) as ActivationRow | undefined;
      const ownership = this.#database.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?")
        .get(runResourceId(ownerAgentId, operation.target_agent_id)) as { owner_id: string; fencing_epoch: number } | undefined;
      const checkpoint = this.#database.prepare(`SELECT value, fencing_epoch FROM fenced_state
        WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'`
      ).get(runResourceId(ownerAgentId, operation.target_agent_id)) as { value: string; fencing_epoch: number } | undefined;
      const exact = activation
        && activation.agent_id === operation.target_agent_id
        && activation.run_id === operation.run_id
        && Number(activation.fencing_epoch) === Number(operation.fencing_epoch)
        && Number(activation.revision) === Number(operation.activation_revision)
        && activation.phase === "open"
        && ownership?.owner_id === operation.run_id
        && Number(ownership?.fencing_epoch) === Number(operation.fencing_epoch)
        && checkpoint?.value === operation.run_locator
        && Number(checkpoint?.fencing_epoch) === Number(operation.fencing_epoch);
      if (!exact) {
        this.#database.prepare(`UPDATE activation_cancellations
          SET state = 'in-doubt', last_error = ?, updated_at_ms = ?
          WHERE operation_id = ? AND state != 'committed'`
        ).run("Exact activation/run/epoch/revision/checkpoint revalidation failed", now, operationId);
        return mapCancellation(this.#readRequired(operationId));
      }

      this.#orphanIncomingRequests(operation, now);
      this.#discardBoundOutboundMessages(operation);
      this.#cancelOpenOutgoingRequests(operation, now);
      this.#database.prepare(`UPDATE human_interrupts
        SET status = 'terminal', response_input_id = NULL,
            terminal_reason = 'activation-cancelled', updated_at_ms = ?
        WHERE activation_id = ? AND status IN ('pending', 'response-bound', 'result-pending')`
      ).run(now, operation.activation_id);
      this.#database.prepare("DELETE FROM human_attention WHERE agent_id = ?").run(operation.target_agent_id);
      this.#database.prepare(`UPDATE undeclared_settlement_episodes
        SET status = 'closed', updated_at_ms = ?
        WHERE agent_id = ? AND status = 'open'`
      ).run(now, operation.target_agent_id);
      const cancellationDependency = this.#database.prepare(`DELETE FROM activation_dependencies
        WHERE activation_id = ? AND dependency_kind = 'operation' AND dependency_id = ?`
      ).run(operation.activation_id, `${CANCELLATION_DEPENDENCY_PREFIX}${operation.operation_id}`);
      if (Number(cancellationDependency.changes) !== 1) {
        throw new Error(`Cancellation dependency is missing for operation ${operation.operation_id}`);
      }
      const ended = this.#database.prepare(`UPDATE agent_activations
        SET phase = 'ended', open_state = NULL, ended_outcome = 'cancelled',
            failure_error = NULL, failure_exit_code = NULL, revision = revision + 1,
            interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
            updated_at_ms = ?
        WHERE activation_id = ? AND phase = 'open' AND run_id = ?
          AND fencing_epoch = ? AND revision = ?`
      ).run(now, operation.activation_id, operation.run_id, operation.fencing_epoch, operation.activation_revision);
      if (Number(ended.changes) !== 1) {
        throw new WorkflowProtocolError("StaleLifecycleTransition", `Cancellation lost activation ${operation.activation_id}`);
      }
      this.#database.prepare(`DELETE FROM recipient_inbox_routers
        WHERE agent_id = ? AND run_id = ? AND fencing_epoch = ?`
      ).run(operation.target_agent_id, operation.run_id, operation.fencing_epoch);
      const released = this.#database.prepare(`DELETE FROM ownership
        WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?`
      ).run(runResourceId(ownerAgentId, operation.target_agent_id), operation.run_id, operation.fencing_epoch);
      if (Number(released.changes) !== 1) {
        throw new WorkflowProtocolError("OwnershipLost", `Cancellation lost ownership of Agent ${operation.target_agent_id}`);
      }
      const committed = this.#database.prepare(`UPDATE activation_cancellations
        SET state = 'committed', last_error = NULL, updated_at_ms = ?, committed_at_ms = ?
        WHERE operation_id = ? AND state = 'ready-to-commit'`
      ).run(now, now, operationId);
      if (Number(committed.changes) !== 1) throw new Error(`Cancellation ${operationId} lost its commit state`);
      return mapCancellation(this.#readRequired(operationId));
    });
  }

  #orphanIncomingRequests(operation: CancellationRow, now: number): void {
    const rows = this.#database.prepare(`SELECT r.request_id, r.requester_agent_id, r.responder_agent_id,
        message.delivery_status, message.in_reply_to_request_id
      FROM workflow_requests r
      JOIN direct_signal_messages message ON message.message_id = r.request_id
      WHERE r.responder_activation_id = ? AND r.status = 'open'
      ORDER BY r.request_id`
    ).all(operation.activation_id) as unknown as RequestTransformationRow[];
    for (const request of rows) {
      if (request.delivery_status === "queued" && request.in_reply_to_request_id === null) {
        const pointer = this.#database.prepare("DELETE FROM pending_message_pointers WHERE message_id = ?")
          .run(request.request_id);
        if (Number(pointer.changes) !== 1) throw new Error(`Pending pointer is missing for incoming Request ${request.request_id}`);
        const suppressed = this.#database.prepare(`UPDATE direct_signal_messages SET delivery_status = 'suppressed'
          WHERE message_id = ? AND delivery_status = 'queued'`
        ).run(request.request_id);
        if (Number(suppressed.changes) !== 1) throw new Error(`Incoming Request ${request.request_id} could not be suppressed`);
      }
      const noticeMessageId = `${operation.operation_id}:orphan:${request.request_id}`;
      const payload = orphanNoticePayload(request.request_id, operation.target_agent_id);
      this.#insertOrphanNotice({
        operation,
        requestId: request.request_id,
        requesterAgentId: request.requester_agent_id,
        noticeMessageId,
        payload,
        now,
      });
      const orphaned = this.#database.prepare(`UPDATE workflow_requests
        SET status = 'orphaned', orphaned_at_ms = ?, orphaned_by_cancellation_operation_id = ?,
            orphan_notice_message_id = ?, orphan_notice_payload = ?, orphan_notice_delivery_status = 'queued'
        WHERE request_id = ? AND status = 'open' AND responder_activation_id = ?`
      ).run(now, operation.operation_id, noticeMessageId, payload, request.request_id, operation.activation_id);
      if (Number(orphaned.changes) !== 1) throw new Error(`Incoming Request ${request.request_id} lost orphan arbitration`);
    }
  }

  #insertOrphanNotice(input: {
    operation: CancellationRow;
    requestId: string;
    requesterAgentId: string;
    noticeMessageId: string;
    payload: string;
    now: number;
  }): void {
    const digest = digestPayload(input.payload);
    const sequence = this.#nextAcceptanceSequence(input.requesterAgentId);
    const notice = this.#database.prepare(`INSERT INTO direct_signal_messages (
      message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest,
      delivery_timing, response_required, on_accepted, reactivates_recipient,
      in_reply_to_request_id, acceptance_sequence, delivery_status,
      created_at_ms, accepted_at_ms, delivered_at_ms,
      activation_notice_kind, activation_notice_request_id
    ) VALUES (?, ?, ?, ?, ?, 'steer', 0, 'continue', 0, NULL, ?, 'queued', ?, ?, NULL,
      'request-orphaned', ?)`
    ).run(
      input.noticeMessageId,
      input.operation.target_agent_id,
      input.requesterAgentId,
      input.noticeMessageId,
      digest,
      sequence,
      input.now,
      input.now,
      input.requestId,
    );
    if (Number(notice.changes) !== 1) throw new Error(`Orphan notice was not created for Request ${input.requestId}`);
    const pointer = this.#database.prepare(`INSERT INTO pending_message_pointers (
      message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest,
      delivery_timing, response_required, reactivates_recipient, in_reply_to_request_id,
      acceptance_sequence, accepted_at_ms, activation_notice_kind, activation_notice_request_id
    ) VALUES (?, ?, ?, ?, ?, 'steer', 0, 0, NULL, ?, ?, 'request-orphaned', ?)`
    ).run(
      input.noticeMessageId,
      input.operation.target_agent_id,
      input.requesterAgentId,
      input.noticeMessageId,
      digest,
      sequence,
      input.now,
      input.requestId,
    );
    if (Number(pointer.changes) !== 1) throw new Error(`Orphan notice pointer was not created for Request ${input.requestId}`);
  }

  #cancelOpenOutgoingRequests(operation: CancellationRow, now: number): void {
    const rows = this.#database.prepare(`SELECT request_id
      FROM workflow_requests
      WHERE requester_activation_id = ? AND status = 'open'
      ORDER BY request_id`
    ).all(operation.activation_id) as Array<{ request_id: string }>;
    for (const request of rows) {
      const noticeMessageId = `${operation.operation_id}:cancel:${request.request_id}`;
      cancelOpenRequestInTransaction(this.#database, {
        requestId: request.request_id,
        requesterAgentId: operation.target_agent_id,
        requesterActivationId: operation.activation_id,
        noticeMessageId,
        cancelledAtMs: now,
      });
    }
  }

  #discardBoundOutboundMessages(operation: CancellationRow): void {
    const dependencies = this.#database.prepare(`SELECT dependency_id
      FROM activation_dependencies
      WHERE activation_id = ? AND dependency_kind = 'operation'
        AND dependency_id GLOB 'acceptance:*'
      ORDER BY dependency_id`
    ).all(operation.activation_id) as Array<{ dependency_id: string }>;
    for (const dependency of dependencies) {
      const messageId = dependency.dependency_id.slice("acceptance:".length);
      const message = this.#database.prepare(`SELECT message_id
        FROM direct_signal_messages
        WHERE message_id = ? AND sender_agent_id = ? AND delivery_status = 'bound'`
      ).get(messageId, operation.target_agent_id) as { message_id: string } | undefined;
      if (!message) {
        throw new Error(`Acceptance dependency ${dependency.dependency_id} has no exact bound outbound Message`);
      }
      const discarded = this.#database.prepare(`DELETE FROM direct_signal_messages
        WHERE message_id = ? AND sender_agent_id = ? AND delivery_status = 'bound'`
      ).run(messageId, operation.target_agent_id);
      if (Number(discarded.changes) !== 1) throw new Error(`Bound Message ${messageId} lost cancellation arbitration`);
      const dependencyRemoved = this.#database.prepare(`DELETE FROM activation_dependencies
        WHERE activation_id = ? AND dependency_kind = 'operation' AND dependency_id = ?`
      ).run(operation.activation_id, dependency.dependency_id);
      if (Number(dependencyRemoved.changes) !== 1) {
        throw new Error(`Acceptance dependency ${dependency.dependency_id} lost cancellation arbitration`);
      }
    }

    const untracked = this.#database.prepare(`SELECT message_id FROM direct_signal_messages message
      WHERE sender_agent_id = ? AND delivery_status = 'bound'
        AND NOT EXISTS (
          SELECT 1 FROM activation_dependencies dependency
          WHERE dependency.activation_id = ? AND dependency.dependency_kind = 'operation'
            AND dependency.dependency_id = 'acceptance:' || message.message_id
        ) LIMIT 1`
    ).get(operation.target_agent_id, operation.activation_id) as { message_id: string } | undefined;
    if (untracked) throw new Error(`Bound outbound Message ${untracked.message_id} has no exact activation acceptance dependency`);
    const staleDependency = this.#database.prepare(`SELECT dependency.dependency_id
      FROM activation_dependencies dependency
      JOIN agent_activations activation ON activation.activation_id = dependency.activation_id
      WHERE activation.agent_id = ? AND dependency.dependency_kind = 'operation'
        AND dependency.dependency_id GLOB 'acceptance:*'
      LIMIT 1`
    ).get(operation.target_agent_id) as { dependency_id: string } | undefined;
    if (staleDependency) {
      throw new Error(`Acceptance dependency ${staleDependency.dependency_id} survived outbound cancellation arbitration`);
    }
  }

  #nextAcceptanceSequence(agentId: string): number {
    const row = this.#database.prepare("SELECT last_sequence FROM recipient_acceptance_counters WHERE agent_id = ?")
      .get(agentId) as { last_sequence: number } | undefined;
    const next = Number(row?.last_sequence ?? 0) + 1;
    const updated = this.#database.prepare(`INSERT INTO recipient_acceptance_counters (agent_id, last_sequence)
      VALUES (?, ?) ON CONFLICT (agent_id) DO UPDATE SET last_sequence = excluded.last_sequence`
    ).run(agentId, next);
    if (Number(updated.changes) !== 1) throw new Error(`Acceptance sequence was not advanced for Agent ${agentId}`);
    return next;
  }

  #currentActivation(agentId: string): ActivationRow | undefined {
    return this.#database.prepare(`SELECT activation_id, agent_id, run_id, fencing_epoch,
        revision, phase, ended_outcome
      FROM agent_activations WHERE agent_id = ?
      ORDER BY activation_sequence DESC LIMIT 1`
    ).get(agentId) as ActivationRow | undefined;
  }

  #readDescendantPlan(rootOperationId: string): DescendantCancellationPlan {
    const rows = this.#database.prepare(`SELECT agent_id, disposition, activation_id, cascade_state
      FROM activation_cancellation_descendants
      WHERE root_operation_id = ? ORDER BY depth, agent_id`
    ).all(rootOperationId) as Array<{
      agent_id: string;
      disposition: "cancelled" | "survives";
      activation_id: string | null;
      cascade_state: "pending" | "cancelled" | "skipped" | "survives";
    }>;
    return {
      rootOperationId,
      cancelled: rows
        .filter((row) => row.disposition === "cancelled" && row.cascade_state === "pending")
        .map((row) => ({ agentId: row.agent_id, activationId: row.activation_id! })),
      survivors: rows.filter((row) => row.disposition === "survives").map((row) => row.agent_id),
    };
  }

  #setPlannedDescendantState(input: {
    rootOperationId: string;
    agentId: string;
    activationId: string;
    state: "cancelled";
  }): void {
    this.#withImmediateTransaction(() => {
      const updated = this.#database.prepare(`UPDATE activation_cancellation_descendants
        SET cascade_state = ?
        WHERE root_operation_id = ? AND agent_id = ? AND activation_id = ?
          AND disposition = 'cancelled' AND cascade_state = 'pending'`
      ).run(input.state, input.rootOperationId, input.agentId, input.activationId);
      if (Number(updated.changes) === 1) return;
      const row = this.#database.prepare(`SELECT cascade_state FROM activation_cancellation_descendants
        WHERE root_operation_id = ? AND agent_id = ? AND activation_id = ? AND disposition = 'cancelled'`
      ).get(input.rootOperationId, input.agentId, input.activationId) as { cascade_state: string } | undefined;
      if (row?.cascade_state !== input.state) {
        throw new Error(`Cascade plan lost cancelled descendant ${input.agentId}`);
      }
    });
  }

  #assertCascadeAuthority(
    actor: AgentReference,
    target: AgentReference,
    authority: Extract<CancellationAuthority, { kind: "cascade" }>,
  ): void {
    const root = this.#readRequired(authority.rootOperationId);
    if (root.state !== "committed" || root.authority_kind === "cascade") {
      throw new WorkflowProtocolError("ActivationCancellationUnauthorized", "Cascade authority does not match a committed root cancellation");
    }
    this.#assertHistoricalCancellationAuthority(actor, root.target_agent_id, actor.workflowOwnerId);
    let ancestor = target.agentId;
    while (true) {
      const row = this.#database.prepare("SELECT spawner_agent_id FROM workflow_agents WHERE agent_id = ?")
        .get(ancestor) as { spawner_agent_id: string | null } | undefined;
      if (!row?.spawner_agent_id) break;
      if (row.spawner_agent_id === root.target_agent_id) return;
      ancestor = row.spawner_agent_id;
    }
    throw new WorkflowProtocolError(
      "ActivationCancellationUnauthorized",
      `Agent ${target.agentId} is not a descendant of cascade root ${root.target_agent_id}`,
    );
  }

  /** The Owner already has authority; a direct Spawner retains its own Child Control. */
  #assertHistoricalCancellationAuthority(
    actor: AgentReference,
    targetAgentId: string,
    ownerAgentId: string,
  ): void {
    if (actor.agentId === ownerAgentId) return;
    const target = this.#database.prepare("SELECT spawner_agent_id FROM workflow_agents WHERE agent_id = ?")
      .get(targetAgentId) as { spawner_agent_id: string | null } | undefined;
    if (target?.spawner_agent_id === actor.agentId) return;
    throw new WorkflowProtocolError(
      "ActivationCancellationUnauthorized",
      `Agent ${actor.agentId} cannot resume cancellation of Agent ${targetAgentId}`,
    );
  }

  #readByActivation(activationId: string): CancellationRow | undefined {
    return this.#database.prepare("SELECT * FROM activation_cancellations WHERE activation_id = ?")
      .get(activationId) as CancellationRow | undefined;
  }

  #readRequired(operationId: string): CancellationRow {
    const row = this.#database.prepare("SELECT * FROM activation_cancellations WHERE operation_id = ?")
      .get(operationId) as CancellationRow | undefined;
    if (!row) throw new WorkflowProtocolError("UnknownAgent", `Unknown cancellation operation ${operationId}`);
    return row;
  }

  #workflowOwnerId(): string {
    const row = this.#database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1")
      .get() as { owner_agent_id: string } | undefined;
    if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
    return row.owner_agent_id;
  }

  #assertReference(reference: AgentReference, ownerAgentId: string): void {
    if (reference.workflowOwnerId !== ownerAgentId) {
      throw new WorkflowProtocolError("WorkflowMismatch", `Agent ${reference.agentId} belongs to another Workflow`);
    }
    if (!this.#database.prepare("SELECT 1 FROM workflow_agents WHERE agent_id = ?").get(reference.agentId)) {
      throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${reference.agentId}`);
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

/** Query-only cancellation ownership for watchers and shutdown reconciliation. */
export class ActivationCancellationInspectionStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs, readOnly: true });
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  inspectForRun(input: {
    workflowOwnerId: string;
    agentId: string;
    runId: string;
    fencingEpoch: number;
  }): ActivationCancellationRecord | undefined {
    const workflow = this.#database.prepare(
      "SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1",
    ).get() as { owner_agent_id: string } | undefined;
    if (workflow?.owner_agent_id !== input.workflowOwnerId) return undefined;
    const row = this.#database.prepare(`SELECT * FROM activation_cancellations
      WHERE target_agent_id = ? AND run_id = ? AND fencing_epoch = ?
      ORDER BY created_at_ms DESC LIMIT 1`
    ).get(input.agentId, input.runId, input.fencingEpoch) as CancellationRow | undefined;
    return row ? mapCancellation(row) : undefined;
  }
}

export class ActivationCancellationService {
  readonly #store: ActivationCancellationStore;
  readonly #actor: AgentReference;
  readonly #terminator: AgentRunTerminator;
  readonly #now: () => number;
  readonly #allocateOperationId: () => string;
  readonly #authorizeIncidentControl: ((input: {
    actor: AgentReference;
    target: AgentReference;
    authority: Extract<CancellationAuthority, { kind: "incident-control" }>;
  }) => boolean) | undefined;

  constructor(options: {
    databasePath: string;
    actor: AgentReference;
    terminator: AgentRunTerminator;
    now?: () => number;
    allocateOperationId: () => string;
    authorizeIncidentControl?: (input: {
      actor: AgentReference;
      target: AgentReference;
      authority: Extract<CancellationAuthority, { kind: "incident-control" }>;
    }) => boolean;
  }) {
    this.#store = new ActivationCancellationStore(options.databasePath);
    this.#actor = options.actor;
    this.#terminator = options.terminator;
    this.#now = options.now ?? Date.now;
    this.#allocateOperationId = options.allocateOperationId;
    this.#authorizeIncidentControl = options.authorizeIncidentControl;
  }

  close(): void { this.#store.close(); }

  async cancel(input: {
    target: AgentReference;
    sourceId: string;
    authority?: Extract<CancellationAuthority, { kind: "incident-control" }>;
  }): Promise<ActivationCancellationRecord> {
    let operation: ActivationCancellationRecord;
    try {
      operation = this.#store.claim({
        actor: this.#actor,
        target: input.target,
        sourceId: input.sourceId,
        operationId: this.#allocateOperationId(),
        authority: input.authority,
        incidentControlAuthorized: input.authority
          ? this.#authorizeIncidentControl?.({ actor: this.#actor, target: input.target, authority: input.authority }) === true
          : undefined,
        now: this.#now(),
      });
    } catch (error) {
      if (!(error instanceof WorkflowProtocolError) || error.code !== "InvalidLifecycleTransition") throw error;
      const historicalRoot = this.#store.resumeIncompleteCascade({
        actor: this.#actor,
        target: input.target,
        sourceId: input.sourceId,
        now: this.#now(),
      });
      if (!historicalRoot) throw error;
      operation = historicalRoot;
    }
    return this.#completeRootCascade(operation);
  }

  async retry(operationId: string): Promise<ActivationCancellationRecord> {
    const operation = this.#store.inspectOperation(operationId);
    if (!operation) throw new WorkflowProtocolError("UnknownAgent", `Unknown cancellation operation ${operationId}`);
    if (operation.actorAgentId !== this.#actor.agentId) {
      throw new WorkflowProtocolError("ActivationCancellationUnauthorized", `Agent ${this.#actor.agentId} cannot retry cancellation ${operationId}`);
    }
    return operation.authority.kind === "cascade"
      ? this.#attempt(operation)
      : this.#completeRootCascade(operation);
  }

  async #completeRootCascade(operation: ActivationCancellationRecord): Promise<ActivationCancellationRecord> {
    const plan = this.#store.planDescendantCancellation(operation.operationId, this.#now());
    const receipt = await this.#attempt(operation);
    for (const descendant of plan.cancelled) {
      try {
        const cascade = this.#store.claim({
          actor: this.#actor,
          target: {
            workflowOwnerId: this.#actor.workflowOwnerId,
            agentId: descendant.agentId,
          },
          sourceId: `${operation.operationId}:cascade:${descendant.agentId}`,
          operationId: this.#allocateOperationId(),
          authority: { kind: "cascade", rootOperationId: operation.operationId },
          cascadeAuthorized: true,
          expectedActivationId: descendant.activationId,
          now: this.#now(),
        });
        await this.#attempt(cascade);
        this.#store.markPlannedDescendantCancelled({
          rootOperationId: operation.operationId,
          agentId: descendant.agentId,
          activationId: descendant.activationId,
        });
      } catch (error) {
        const staleLifecycle = error instanceof WorkflowProtocolError
          && error.code === "InvalidLifecycleTransition";
        if ((staleLifecycle || error instanceof CancellationInDoubtError)
          && this.#store.skipStalePlannedDescendant({
            rootOperationId: operation.operationId,
            agentId: descendant.agentId,
            activationId: descendant.activationId,
          })) {
          continue;
        }
        throw error;
      }
    }
    return receipt;
  }

  async #attempt(operation: ActivationCancellationRecord): Promise<ActivationCancellationRecord> {
    if (operation.state === "committed") return operation;
    if (operation.state === "ready-to-commit") return this.#commitOrThrow(operation.operationId);
    const locator = operation.runLocator ? parseAgentRunLocator(operation.runLocator) : undefined;
    if (!locator) {
      throw new CancellationInDoubtError(this.#store.markInDoubt(
        operation.operationId,
        "Exact durable Agent Run locator is unavailable",
        this.#now(),
      ));
    }

    const initial = await this.#inspect(locator);
    if (initial.kind === "unavailable") {
      throw new CancellationInDoubtError(this.#store.markInDoubt(
        operation.operationId,
        initial.error ?? "Initial Agent Run inspection is unavailable",
        this.#now(),
      ));
    }
    if (initial.kind === "present") {
      let closeError: unknown;
      try { await this.#terminator.close(locator); } catch (error) { closeError = error; }
      const afterClose = await this.#inspect(locator);
      if (afterClose.kind !== "missing") {
        const detail = afterClose.kind === "unavailable"
          ? afterClose.error ?? "Post-close Agent Run inspection is unavailable"
          : "Agent Run remains present after close";
        const suffix = closeError ? `; close failed: ${errorMessage(closeError)}` : "";
        throw new CancellationInDoubtError(this.#store.markInDoubt(
          operation.operationId,
          `${detail}${suffix}`,
          this.#now(),
        ));
      }
    }
    this.#store.markReady(operation.operationId, this.#now());
    return this.#commitOrThrow(operation.operationId);
  }

  #commitOrThrow(operationId: string): ActivationCancellationRecord {
    const finalized = this.#store.finalize(operationId, this.#now());
    if (finalized.state !== "committed") throw new CancellationInDoubtError(finalized);
    return finalized;
  }

  async #inspect(locator: AgentRunLocator): Promise<AgentRunInspection> {
    try { return await this.#terminator.inspect(locator); }
    catch (error) { return { kind: "unavailable", error: errorMessage(error) }; }
  }
}

export function parseAgentRunLocator(value: string): AgentRunLocator | undefined {
  try {
    const parsed = JSON.parse(value) as { surface?: unknown };
    return typeof parsed.surface === "string" && parsed.surface ? { surface: parsed.surface } : undefined;
  } catch {
    return undefined;
  }
}

function mapCancellation(row: CancellationRow): ActivationCancellationRecord {
  const authority: CancellationAuthority = row.authority_kind === "incident-control"
    ? { kind: "incident-control", incidentId: row.incident_id!, rationale: row.rationale! }
    : row.authority_kind === "cascade"
      ? { kind: "cascade", rootOperationId: row.root_operation_id! }
      : { kind: row.authority_kind };
  return {
    operationId: row.operation_id,
    actorAgentId: row.actor_agent_id,
    sourceId: row.source_id,
    authority,
    targetAgentId: row.target_agent_id,
    activationId: row.activation_id,
    runId: row.run_id,
    fencingEpoch: Number(row.fencing_epoch),
    activationRevision: Number(row.activation_revision),
    ...(row.run_locator ? { runLocator: row.run_locator } : {}),
    state: row.state,
    terminationAttempts: Number(row.termination_attempts),
    ...(row.last_error ? { lastError: row.last_error } : {}),
    createdAtMs: Number(row.created_at_ms),
    updatedAtMs: Number(row.updated_at_ms),
    ...(row.committed_at_ms == null ? {} : { committedAtMs: Number(row.committed_at_ms) }),
  };
}

function runResourceId(workflowOwnerId: string, agentId: string): string {
  return `agent-run:${workflowOwnerId}:${agentId}`;
}

function digestPayload(payload: string): string {
  return createHash("sha256").update(payload, "utf8").digest("hex");
}

function orphanNoticePayload(requestId: string, responderAgentId: string): string {
  return [
    `Request ${requestId} was orphaned because responder Agent ${responderAgentId}'s activation was cancelled.`,
    "No Answer was fabricated. Create a new Request if replacement work is needed.",
  ].join("\n");
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new TypeError(`${label} must not be empty`);
}
