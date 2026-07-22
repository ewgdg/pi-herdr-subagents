import { DatabaseSync } from "node:sqlite";
import { WorkflowProtocolError, type AgentRunOwnership } from "./workflow-types.ts";

export type CompletionSource =
  | { kind: "standalone"; toolCallId: string }
  | { kind: "terminal-message"; messageId: string; sourceEntryId: string };

export type CompletionBlocker =
  | { kind: "incoming-request"; requestId: string }
  | { kind: "outgoing-request"; requestId: string }
  | { kind: "recovery-pending-request"; requestId: string }
  | { kind: "accepted-undelivered-input"; messageId: string }
  | { kind: "human-interrupt"; toolCallId: string; status: string }
  | { kind: "operation-dependency"; dependencyId: string }
  | { kind: "acceptance-uncertainty"; dependencyId: string }
  | { kind: "cancellation-uncertainty"; dependencyId: string }
  | { kind: "ownership-uncertainty"; dependencyId: string }
  | { kind: "side-effect-uncertainty"; dependencyId: string };

export interface CompletionRecord {
  activationId: string;
  agentId: string;
  completedAtMs: number;
  source: CompletionSource;
}

export class CompletionRejectedError extends WorkflowProtocolError {
  readonly blockers: CompletionBlocker[];
  constructor(blockers: CompletionBlocker[]) {
    super("CompletionBlocked", `Completion blocked:\n${blockers.map((blocker) => `- ${JSON.stringify(blocker)}`).join("\n")}`);
    this.blockers = blockers;
  }
}

export class CompletionGateStore {
  readonly #database: DatabaseSync;
  #closed = false;
  constructor(databasePath: string) {
    this.#database = new DatabaseSync(databasePath, { timeout: 5_000 });
    this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    initializeCompletionSchema(this.#database);
  }
  close(): void { if (!this.#closed) { this.#database.close(); this.#closed = true; } }
  complete(ownership: AgentRunOwnership, source: CompletionSource, completedAtMs: number): CompletionRecord {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = commitMechanicalCompletion(this.#database, ownership, source, completedAtMs);
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }
  completedRun(ownership: AgentRunOwnership): CompletionRecord | undefined {
    return readCompletionForRun(this.#database, ownership);
  }
}

export function initializeCompletionSchema(database: DatabaseSync): void {
  database.exec(`CREATE TABLE IF NOT EXISTS activation_completions (
    activation_id TEXT PRIMARY KEY REFERENCES agent_activations(activation_id),
    agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('standalone', 'terminal-message')),
    source_identity TEXT NOT NULL,
    source_entry_id TEXT,
    completed_at_ms INTEGER NOT NULL
  ) STRICT;`);
}

/** Caller must hold the Workflow database write transaction. */
export function commitMechanicalCompletion(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  source: CompletionSource,
  completedAtMs: number,
): CompletionRecord {
  const owner = database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1").get() as { owner_agent_id: string } | undefined;
  if (!owner) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
  if (ownership.agentId === owner.owner_agent_id) throw new WorkflowProtocolError("OwnerActivationForbidden", "Workflow Owner cannot complete");
  const resourceId = `agent-run:${ownership.workflowOwnerId}:${ownership.agentId}`;
  if (ownership.resourceId !== resourceId) throw new WorkflowProtocolError("OwnershipLost", "Completion ownership does not belong to this Agent");
  const priorCompletion = readCompletionForRun(database, ownership);
  if (priorCompletion) {
    if (!sameCompletionSource(priorCompletion.source, source)) {
      throw new WorkflowProtocolError("InvalidCompletionMessage", "Agent Run was completed by a different durable source");
    }
    return priorCompletion;
  }
  const currentOwner = database.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?").get(resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
  if (!currentOwner || currentOwner.owner_id !== ownership.runId || Number(currentOwner.fencing_epoch) !== ownership.epoch) {
    throw new WorkflowProtocolError("OwnershipLost", `Agent Run no longer owns ${ownership.agentId}`);
  }
  const activation = database.prepare(`SELECT activation_id, phase FROM agent_activations
    WHERE agent_id = ? AND run_id = ? AND fencing_epoch = ? ORDER BY activation_sequence DESC LIMIT 1`).get(
      ownership.agentId, ownership.runId, ownership.epoch,
    ) as { activation_id: string; phase: "open" | "ended" } | undefined;
  if (!activation || activation.phase !== "open") throw new WorkflowProtocolError("InvalidLifecycleTransition", "Completion requires the current open activation");

  const blockers = collectCompletionBlockers(database, ownership.agentId);
  if (blockers.length) throw new CompletionRejectedError(blockers);

  database.prepare("UPDATE undeclared_settlement_episodes SET status = 'closed', updated_at_ms = ? WHERE agent_id = ? AND status = 'open'").run(completedAtMs, ownership.agentId);
  database.prepare(`UPDATE agent_activations SET phase = 'ended', open_state = NULL, ended_outcome = 'completed',
    failure_error = NULL, failure_exit_code = NULL, revision = revision + 1,
    interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL, updated_at_ms = ? WHERE activation_id = ?`).run(completedAtMs, activation.activation_id);
  database.prepare("DELETE FROM recipient_inbox_routers WHERE agent_id = ? AND run_id = ? AND fencing_epoch = ?").run(ownership.agentId, ownership.runId, ownership.epoch);
  const released = database.prepare("DELETE FROM ownership WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?").run(resourceId, ownership.runId, ownership.epoch);
  if (Number(released.changes) !== 1) throw new WorkflowProtocolError("OwnershipLost", `Agent Run no longer owns ${ownership.agentId}`);
  const identity = source.kind === "standalone" ? source.toolCallId : source.messageId;
  database.prepare(`INSERT INTO activation_completions (activation_id, agent_id, source_kind, source_identity, source_entry_id, completed_at_ms)
    VALUES (?, ?, ?, ?, ?, ?)`).run(activation.activation_id, ownership.agentId, source.kind, identity, source.kind === "terminal-message" ? source.sourceEntryId : null, completedAtMs);
  return { activationId: activation.activation_id, agentId: ownership.agentId, completedAtMs, source };
}

function collectCompletionBlockers(database: DatabaseSync, agentId: string): CompletionBlocker[] {
  const blockers: CompletionBlocker[] = [];
  const failed = database.prepare("SELECT ended_outcome FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 2").all(agentId) as Array<{ ended_outcome: string | null }>;
  const recoveryPending = failed.some((row) => row.ended_outcome === "failed");
  for (const row of database.prepare("SELECT request_id FROM workflow_requests WHERE responder_agent_id = ? AND status = 'open' ORDER BY request_id").all(agentId) as Array<{ request_id: string }>) {
    blockers.push({ kind: recoveryPending ? "recovery-pending-request" : "incoming-request", requestId: row.request_id });
  }
  for (const row of database.prepare("SELECT request_id FROM workflow_requests WHERE requester_agent_id = ? AND status IN ('open', 'answered') ORDER BY request_id").all(agentId) as Array<{ request_id: string }>) blockers.push({ kind: "outgoing-request", requestId: row.request_id });
  for (const row of database.prepare("SELECT message_id FROM pending_message_pointers WHERE recipient_agent_id = ? ORDER BY acceptance_sequence").all(agentId) as Array<{ message_id: string }>) blockers.push({ kind: "accepted-undelivered-input", messageId: row.message_id });
  for (const row of database.prepare("SELECT tool_call_id, status FROM human_interrupts WHERE agent_id = ? AND status IN ('pending','response-bound','result-pending') ORDER BY tool_call_id").all(agentId) as Array<{ tool_call_id: string; status: string }>) blockers.push({ kind: "human-interrupt", toolCallId: row.tool_call_id, status: row.status });
  for (const row of database.prepare(`SELECT DISTINCT dependency_id FROM activation_dependencies
    WHERE dependency_kind = 'operation' AND activation_id IN (
      SELECT activation_id FROM agent_activations WHERE agent_id = ?
    ) ORDER BY dependency_id`).all(agentId) as Array<{ dependency_id: string }>) blockers.push(classifyOperation(row.dependency_id));
  return blockers;
}

function readCompletionForRun(database: DatabaseSync, ownership: AgentRunOwnership): CompletionRecord | undefined {
  const row = database.prepare(`SELECT c.activation_id, c.agent_id, c.source_kind, c.source_identity, c.source_entry_id, c.completed_at_ms
    FROM activation_completions c JOIN agent_activations a ON a.activation_id = c.activation_id
    WHERE a.agent_id = ? AND a.run_id = ? AND a.fencing_epoch = ?`).get(
      ownership.agentId, ownership.runId, ownership.epoch,
    ) as { activation_id: string; agent_id: string; source_kind: "standalone" | "terminal-message"; source_identity: string; source_entry_id: string | null; completed_at_ms: number } | undefined;
  if (!row) return undefined;
  return { activationId: row.activation_id, agentId: row.agent_id, completedAtMs: Number(row.completed_at_ms), source: row.source_kind === "standalone"
    ? { kind: "standalone", toolCallId: row.source_identity }
    : { kind: "terminal-message", messageId: row.source_identity, sourceEntryId: row.source_entry_id! } };
}

function sameCompletionSource(left: CompletionSource, right: CompletionSource): boolean {
  return left.kind === right.kind && (left.kind === "standalone"
    ? left.toolCallId === (right as { toolCallId?: string }).toolCallId
    : left.messageId === (right as { messageId?: string }).messageId && left.sourceEntryId === (right as { sourceEntryId?: string }).sourceEntryId);
}

function classifyOperation(dependencyId: string): CompletionBlocker {
  if (dependencyId.startsWith("acceptance:")) return { kind: "acceptance-uncertainty", dependencyId };
  if (dependencyId.startsWith("cancellation:")) return { kind: "cancellation-uncertainty", dependencyId };
  if (dependencyId.startsWith("ownership:")) return { kind: "ownership-uncertainty", dependencyId };
  if (dependencyId.startsWith("side-effect:")) return { kind: "side-effect-uncertainty", dependencyId };
  return { kind: "operation-dependency", dependencyId };
}
