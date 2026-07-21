import { DatabaseSync } from "node:sqlite";
import { WorkflowProtocolError, type AgentReference, type AgentRunOwnership } from "./workflow-types.ts";
import type {
  AcceptedSignal,
  DirectSignalRecord,
  PendingMessagePointer,
  RequestRecord,
  SignalAcceptRequest,
  SignalDeliveryTiming,
} from "./direct-signal-types.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const SCHEMA_INITIALIZATION_MAX_ATTEMPTS = 5;
const SCHEMA_INITIALIZATION_RETRY_DELAY_MS = 10;

interface RouterRow { endpoint: string; run_id: string | null; fencing_epoch: number | null; }
interface MessageRow {
  message_id: string; sender_agent_id: string; recipient_agent_id: string; source_entry_id: string;
  payload_digest: string; delivery_timing: SignalDeliveryTiming; response_required: number;
  in_reply_to_request_id: string | null; acceptance_sequence: number | null;
  delivery_status: "bound" | "queued" | "delivered"; created_at_ms: number;
  accepted_at_ms: number | null; delivered_at_ms: number | null;
}
interface PointerRow {
  message_id: string; sender_agent_id: string; recipient_agent_id: string; source_entry_id: string;
  payload_digest: string; delivery_timing: SignalDeliveryTiming; response_required: number;
  in_reply_to_request_id: string | null; acceptance_sequence: number; accepted_at_ms: number;
}
interface RequestRow {
  request_id: string; requester_agent_id: string; responder_agent_id: string;
  answer_delivery_timing: SignalDeliveryTiming; status: "open" | "answered" | "resolved";
  answer_message_id: string | null;
}

/** Durable inbox pointers and Agent-scoped Request obligations. */
export class DirectSignalStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    try {
      this.#initializeSchemaWithRetry();
    } catch (error) {
      this.#database.close();
      this.#closed = true;
      throw error;
    }
  }

  close(): void { if (!this.#closed) { this.#database.close(); this.#closed = true; } }

  registerRouter(input: { recipient: AgentReference; ownership?: AgentRunOwnership; endpoint: string; registeredAtMs: number }): void {
    this.#withTransaction(() => {
      this.#assertWorkflow(input.recipient.workflowOwnerId);
      this.#requireAgent(input.recipient.agentId);
      this.#assertRouterOwnership(input.recipient, input.ownership);
      this.#database.prepare(`
        INSERT INTO recipient_inbox_routers (agent_id, endpoint, run_id, fencing_epoch, registered_at_ms)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (agent_id) DO UPDATE SET endpoint = excluded.endpoint, run_id = excluded.run_id,
          fencing_epoch = excluded.fencing_epoch, registered_at_ms = excluded.registered_at_ms
      `).run(input.recipient.agentId, input.endpoint, input.ownership?.runId ?? null, input.ownership?.epoch ?? null, input.registeredAtMs);
    });
  }

  unregisterRouter(recipient: AgentReference, endpoint: string): void {
    this.#database.prepare("DELETE FROM recipient_inbox_routers WHERE agent_id = ? AND endpoint = ?").run(recipient.agentId, endpoint);
  }

  readRouter(recipient: AgentReference): { endpoint: string } | undefined {
    this.#assertWorkflow(recipient.workflowOwnerId);
    this.#requireAgent(recipient.agentId);
    return this.#database.prepare("SELECT endpoint FROM recipient_inbox_routers WHERE agent_id = ?").get(recipient.agentId) as { endpoint: string } | undefined;
  }

  findMessageBySource(input: {
    sender: AgentReference; recipient: AgentReference; sourceEntryId: string; payloadDigest: string;
    deliveryTiming: SignalDeliveryTiming; responseRequired: boolean; inReplyToRequestId?: string;
  }): DirectSignalRecord | undefined {
    this.#assertWorkflow(input.sender.workflowOwnerId);
    const row = this.#readMessageBySource(input.sender.agentId, input.sourceEntryId);
    if (!row) return undefined;
    assertSameBinding(row, input);
    return mapMessage(row);
  }

  bindMessage(input: {
    messageId: string; sender: AgentReference; recipient: AgentReference; sourceEntryId: string; payloadDigest: string;
    deliveryTiming: SignalDeliveryTiming; responseRequired: boolean; inReplyToRequestId?: string; createdAtMs: number;
  }): DirectSignalRecord {
    return this.#withTransaction(() => {
      this.#assertWorkflow(input.sender.workflowOwnerId);
      if (input.recipient.workflowOwnerId !== input.sender.workflowOwnerId) throw new WorkflowProtocolError("WorkflowMismatch", `Message target ${input.recipient.agentId} belongs to another Workflow`);
      this.#requireAgent(input.sender.agentId);
      this.#requireAgent(input.recipient.agentId);
      const sameIdentity = this.#readMessage(input.messageId);
      if (sameIdentity) { assertSameBinding(sameIdentity, input); return mapMessage(sameIdentity); }
      const prior = this.#readMessageBySource(input.sender.agentId, input.sourceEntryId);
      if (prior) { assertSameBinding(prior, input); return mapMessage(prior); }
      this.#database.prepare(`
        INSERT INTO direct_signal_messages (
          message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing,
          response_required, in_reply_to_request_id, acceptance_sequence, delivery_status, created_at_ms, accepted_at_ms, delivered_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'bound', ?, NULL, NULL)
      `).run(input.messageId, input.sender.agentId, input.recipient.agentId, input.sourceEntryId, input.payloadDigest,
        input.deliveryTiming, input.responseRequired ? 1 : 0, input.inReplyToRequestId ?? null, input.createdAtMs);
      return mapMessage(this.#readMessage(input.messageId)!);
    });
  }

  discardUnacceptedMessage(sender: AgentReference, messageId: string): boolean {
    this.#assertWorkflow(sender.workflowOwnerId);
    return Number(this.#database.prepare("DELETE FROM direct_signal_messages WHERE message_id = ? AND sender_agent_id = ? AND delivery_status = 'bound'").run(messageId, sender.agentId).changes) === 1;
  }

  /** Assert authority and derive a Request Answer's non-caller-controlled route. */
  requireAnswerTarget(sender: AgentReference, requestId: string, allowClosed = false): RequestRecord {
    this.#assertWorkflow(sender.workflowOwnerId);
    const request = this.#readRequest(requestId);
    if (!request) throw new WorkflowProtocolError("UnknownRequest", `Unknown Request ${requestId}`);
    if (request.responder_agent_id !== sender.agentId) throw new WorkflowProtocolError("AnswerUnauthorized", `Agent ${sender.agentId} is not addressed by Request ${requestId}`);
    if (!allowClosed && request.status !== "open") throw new WorkflowProtocolError("AnswerAlreadyClosed", `Request ${requestId} already has a terminal Answer`);
    return mapRequest(request);
  }

  acceptSignal(input: { request: SignalAcceptRequest; recipient: AgentReference; ownership?: AgentRunOwnership; endpoint: string; acceptedAtMs: number }): AcceptedSignal {
    return this.#withTransaction(() => {
      this.#assertWorkflow(input.request.workflowOwnerId);
      this.#assertCurrentRouter(input.recipient, input.ownership, input.endpoint);
      const existing = this.#readMessage(input.request.messageId);
      if (!existing) throw new WorkflowProtocolError("InvalidMessageSource", `Message ${input.request.messageId} has no durable sender binding`);
      assertSameBinding(existing, input.request);
      if (existing.delivery_status !== "bound") return { receipt: receiptFor(existing), delivery: "schedule" };
      if (this.#recipientLifecycle(input.recipient) === "ended") throw new WorkflowProtocolError("RecipientEnded", `Ended Agent ${input.recipient.agentId} cannot accept a Signal`);

      if (input.request.inReplyToRequestId) this.#claimAnswerSlot(input.request);
      const sequence = this.#nextAcceptanceSequence(input.recipient.agentId);
      this.#database.prepare(`UPDATE direct_signal_messages SET acceptance_sequence = ?, delivery_status = 'queued', accepted_at_ms = ? WHERE message_id = ? AND delivery_status = 'bound'`).run(sequence, input.acceptedAtMs, input.request.messageId);
      if (input.request.responseRequired) this.#createRequest(input.request);
      this.#database.prepare(`
        INSERT INTO pending_message_pointers (
          message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing,
          response_required, in_reply_to_request_id, acceptance_sequence, accepted_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(input.request.messageId, input.request.senderAgentId, input.request.recipientAgentId, input.request.sourceEntryId,
        input.request.payloadDigest, input.request.deliveryTiming, input.request.responseRequired ? 1 : 0,
        input.request.inReplyToRequestId ?? null, sequence, input.acceptedAtMs);
      return { receipt: { status: "queued", messageId: input.request.messageId, recipientAgentId: input.request.recipientAgentId, acceptanceSequence: sequence }, delivery: "schedule" };
    });
  }

  commitDelivery(input: { recipient: AgentReference; ownership?: AgentRunOwnership; endpoint: string; messageId: string; deliveredAtMs: number }): "newly-delivered" | "already-delivered" | "not-deliverable" {
    return this.#withTransaction(() => {
      this.#assertCurrentRouter(input.recipient, input.ownership, input.endpoint);
      const message = this.#readMessage(input.messageId);
      if (!message || message.recipient_agent_id !== input.recipient.agentId || message.delivery_status === "bound") return "not-deliverable";
      if (message.delivery_status === "delivered") return "already-delivered";
      const removed = this.#database.prepare("DELETE FROM pending_message_pointers WHERE message_id = ? AND recipient_agent_id = ?").run(input.messageId, input.recipient.agentId);
      if (Number(removed.changes) !== 1) throw new Error(`Pending pointer is missing for Message ${input.messageId}`);
      const delivered = this.#database.prepare("UPDATE direct_signal_messages SET delivery_status = 'delivered', delivered_at_ms = ? WHERE message_id = ? AND delivery_status = 'queued'").run(input.deliveredAtMs, input.messageId);
      if (Number(delivered.changes) !== 1) throw new Error(`Message ${input.messageId} could not transition from queued to delivered`);
      if (message.in_reply_to_request_id) {
        const updated = this.#database.prepare(`UPDATE workflow_requests SET status = 'resolved' WHERE request_id = ? AND answer_message_id = ? AND status = 'answered'`).run(message.in_reply_to_request_id, message.message_id);
        if (Number(updated.changes) !== 1) throw new Error(`Answer ${message.message_id} no longer owns Request ${message.in_reply_to_request_id}`);
      }
      return "newly-delivered";
    });
  }

  senderSessionPath(workflowOwnerId: string, senderAgentId: string): string {
    this.#assertWorkflow(workflowOwnerId);
    const row = this.#database.prepare("SELECT session_path FROM workflow_agents WHERE agent_id = ?").get(senderAgentId) as { session_path: string } | undefined;
    if (!row) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${senderAgentId}`);
    return row.session_path;
  }

  recipientLifecycle(recipient: AgentReference): "owner" | "active" | "waiting" | "interrupted" | "ended" { this.#assertWorkflow(recipient.workflowOwnerId); return this.#recipientLifecycle(recipient); }
  inspectMessage(owner: string, messageId: string): DirectSignalRecord | undefined { this.#assertWorkflow(owner); const row = this.#readMessage(messageId); return row ? mapMessage(row) : undefined; }
  listMessages(owner: string): DirectSignalRecord[] { this.#assertWorkflow(owner); return (this.#database.prepare("SELECT * FROM direct_signal_messages ORDER BY created_at_ms, message_id").all() as unknown as MessageRow[]).map(mapMessage); }
  inspectRequest(owner: string, requestId: string): RequestRecord | undefined { this.#assertWorkflow(owner); const row = this.#readRequest(requestId); return row ? mapRequest(row) : undefined; }
  listRequests(requester: AgentReference): RequestRecord[] { this.#assertWorkflow(requester.workflowOwnerId); return (this.#database.prepare("SELECT * FROM workflow_requests WHERE requester_agent_id = ? ORDER BY request_id").all(requester.agentId) as unknown as RequestRow[]).map(mapRequest); }
  listPending(recipient: AgentReference): PendingMessagePointer[] { this.#assertWorkflow(recipient.workflowOwnerId); this.#requireAgent(recipient.agentId); return (this.#database.prepare("SELECT * FROM pending_message_pointers WHERE recipient_agent_id = ? ORDER BY acceptance_sequence").all(recipient.agentId) as unknown as PointerRow[]).map(mapPointer); }

  #claimAnswerSlot(request: SignalAcceptRequest): void {
    const target = this.#readRequest(request.inReplyToRequestId!);
    if (!target) throw new WorkflowProtocolError("UnknownRequest", `Unknown Request ${request.inReplyToRequestId}`);
    if (target.responder_agent_id !== request.senderAgentId) throw new WorkflowProtocolError("AnswerUnauthorized", `Agent ${request.senderAgentId} is not addressed by Request ${target.request_id}`);
    if (target.requester_agent_id !== request.recipientAgentId) throw new WorkflowProtocolError("AnswerUnauthorized", `Answer to Request ${target.request_id} must be delivered to requester ${target.requester_agent_id}`);
    if (target.answer_delivery_timing !== request.deliveryTiming) throw new WorkflowProtocolError("InvalidMessageSource", `Answer to Request ${target.request_id} must use ${target.answer_delivery_timing} delivery timing`);
    if (target.status === "open") {
      const result = this.#database.prepare("UPDATE workflow_requests SET status = 'answered', answer_message_id = ? WHERE request_id = ? AND status = 'open'").run(request.messageId, target.request_id);
      if (Number(result.changes) === 1) return;
    }
    if (target.answer_message_id === request.messageId) return;
    throw new WorkflowProtocolError("AnswerAlreadyClosed", `Request ${target.request_id} already has a terminal Answer`);
  }

  #createRequest(request: SignalAcceptRequest): void {
    this.#database.prepare(`
      INSERT INTO workflow_requests (request_id, requester_agent_id, responder_agent_id, answer_delivery_timing, status, answer_message_id)
      VALUES (?, ?, ?, ?, 'open', NULL)
    `).run(request.messageId, request.senderAgentId, request.recipientAgentId, request.deliveryTiming);
  }

  #initializeSchema(): void {
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS recipient_inbox_routers (
        agent_id TEXT PRIMARY KEY REFERENCES workflow_agents(agent_id), endpoint TEXT NOT NULL UNIQUE,
        run_id TEXT, fencing_epoch INTEGER, registered_at_ms INTEGER NOT NULL,
        CHECK ((run_id IS NULL AND fencing_epoch IS NULL) OR (run_id IS NOT NULL AND fencing_epoch IS NOT NULL AND fencing_epoch > 0))
      ) STRICT;
      CREATE TABLE IF NOT EXISTS direct_signal_messages (
        message_id TEXT PRIMARY KEY, sender_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        recipient_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id), source_entry_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL, delivery_timing TEXT NOT NULL CHECK (delivery_timing IN ('steer', 'deferred')),
        response_required INTEGER NOT NULL CHECK (response_required IN (0, 1)), in_reply_to_request_id TEXT,
        acceptance_sequence INTEGER, delivery_status TEXT NOT NULL CHECK (delivery_status IN ('bound', 'queued', 'delivered')),
        created_at_ms INTEGER NOT NULL, accepted_at_ms INTEGER, delivered_at_ms INTEGER,
        UNIQUE (sender_agent_id, source_entry_id),
        CHECK ((delivery_status = 'bound' AND acceptance_sequence IS NULL AND accepted_at_ms IS NULL AND delivered_at_ms IS NULL)
          OR (delivery_status = 'queued' AND acceptance_sequence IS NOT NULL AND accepted_at_ms IS NOT NULL AND delivered_at_ms IS NULL)
          OR (delivery_status = 'delivered' AND acceptance_sequence IS NOT NULL AND accepted_at_ms IS NOT NULL AND delivered_at_ms IS NOT NULL))
      ) STRICT;
      CREATE TABLE IF NOT EXISTS recipient_acceptance_counters (agent_id TEXT PRIMARY KEY REFERENCES workflow_agents(agent_id), last_sequence INTEGER NOT NULL CHECK (last_sequence > 0)) STRICT;
      CREATE TABLE IF NOT EXISTS pending_message_pointers (
        message_id TEXT PRIMARY KEY REFERENCES direct_signal_messages(message_id), sender_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        recipient_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id), source_entry_id TEXT NOT NULL, payload_digest TEXT NOT NULL,
        delivery_timing TEXT NOT NULL CHECK (delivery_timing IN ('steer', 'deferred')), response_required INTEGER NOT NULL CHECK (response_required IN (0, 1)),
        in_reply_to_request_id TEXT, acceptance_sequence INTEGER NOT NULL CHECK (acceptance_sequence > 0), accepted_at_ms INTEGER NOT NULL,
        UNIQUE (recipient_agent_id, acceptance_sequence)
      ) STRICT;
      CREATE INDEX IF NOT EXISTS pending_message_recipient_order ON pending_message_pointers (recipient_agent_id, acceptance_sequence);
    `);
  }

  #initializeSchemaWithRetry(): void {
    let lastError: unknown;
    for (let attempt = 0; attempt < SCHEMA_INITIALIZATION_MAX_ATTEMPTS; attempt += 1) {
      try {
        this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
        this.#database.exec("BEGIN IMMEDIATE");
        try {
          this.#initializeSchema();
          this.#database.exec("COMMIT");
          return;
        } catch (error) {
          if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
          throw error;
        }
      } catch (error) {
        lastError = error;
        if (!isTransientSqliteLock(error) || attempt + 1 === SCHEMA_INITIALIZATION_MAX_ATTEMPTS) throw error;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)), 0, 0, SCHEMA_INITIALIZATION_RETRY_DELAY_MS);
      }
    }
    throw lastError;
  }

  #assertCurrentRouter(recipient: AgentReference, ownership: AgentRunOwnership | undefined, endpoint: string): void {
    this.#assertWorkflow(recipient.workflowOwnerId); this.#requireAgent(recipient.agentId); this.#assertRouterOwnership(recipient, ownership);
    const row = this.#database.prepare("SELECT endpoint, run_id, fencing_epoch FROM recipient_inbox_routers WHERE agent_id = ?").get(recipient.agentId) as RouterRow | undefined;
    if (!row || row.endpoint !== endpoint || row.run_id !== (ownership?.runId ?? null) || (row.fencing_epoch == null ? ownership != null : Number(row.fencing_epoch) !== ownership?.epoch)) throw new WorkflowProtocolError("RecipientUnreachable", `Recipient Inbox Router is no longer current for Agent ${recipient.agentId}`);
  }
  #assertRouterOwnership(recipient: AgentReference, ownership: AgentRunOwnership | undefined): void {
    const owner = this.#workflowOwnerId();
    if (recipient.agentId === owner) { if (ownership) throw new WorkflowProtocolError("OwnerActivationForbidden", "Workflow Owner Inbox Router cannot use Subagent Agent Run ownership"); return; }
    if (!ownership || ownership.agentId !== recipient.agentId || ownership.workflowOwnerId !== owner) throw new WorkflowProtocolError("OwnershipLost", `Recipient Inbox Router lacks Agent Run ownership for ${recipient.agentId}`);
    const row = this.#database.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?").get(ownership.resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
    if (!row || row.owner_id !== ownership.runId || Number(row.fencing_epoch) !== ownership.epoch) throw new WorkflowProtocolError("OwnershipLost", `Recipient Inbox Router no longer owns ${recipient.agentId} at fencing epoch ${ownership.epoch}`);
  }
  #recipientLifecycle(recipient: AgentReference): "owner" | "active" | "waiting" | "interrupted" | "ended" {
    if (recipient.agentId === this.#workflowOwnerId()) return "owner";
    const row = this.#database.prepare("SELECT phase, open_state FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1").get(recipient.agentId) as { phase: "open" | "ended"; open_state: "active" | "waiting" | "interrupted" | null } | undefined;
    return !row || row.phase === "ended" ? "ended" : row.open_state ?? "ended";
  }
  #nextAcceptanceSequence(agentId: string): number { const row = this.#database.prepare("SELECT last_sequence FROM recipient_acceptance_counters WHERE agent_id = ?").get(agentId) as { last_sequence: number } | undefined; const next = Number(row?.last_sequence ?? 0) + 1; this.#database.prepare("INSERT INTO recipient_acceptance_counters (agent_id, last_sequence) VALUES (?, ?) ON CONFLICT (agent_id) DO UPDATE SET last_sequence = excluded.last_sequence").run(agentId, next); return next; }
  #readMessage(id: string): MessageRow | undefined { return this.#database.prepare("SELECT * FROM direct_signal_messages WHERE message_id = ?").get(id) as MessageRow | undefined; }
  #readMessageBySource(sender: string, source: string): MessageRow | undefined { return this.#database.prepare("SELECT * FROM direct_signal_messages WHERE sender_agent_id = ? AND source_entry_id = ?").get(sender, source) as MessageRow | undefined; }
  #readRequest(id: string): RequestRow | undefined { return this.#database.prepare("SELECT * FROM workflow_requests WHERE request_id = ?").get(id) as RequestRow | undefined; }
  #assertWorkflow(workflowOwnerId: string): void { const owner = this.#workflowOwnerId(); if (owner !== workflowOwnerId) throw new WorkflowProtocolError("WorkflowMismatch", `Workflow store belongs to ${owner}, not ${workflowOwnerId}`); }
  #workflowOwnerId(): string { const row = this.#database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1").get() as { owner_agent_id: string } | undefined; if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized"); return row.owner_agent_id; }
  #requireAgent(agentId: string): void { if (!this.#database.prepare("SELECT 1 AS present FROM workflow_agents WHERE agent_id = ?").get(agentId)) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${agentId}`); }
  #withTransaction<T>(operation: () => T): T { this.#database.exec("BEGIN IMMEDIATE"); try { const result = operation(); this.#database.exec("COMMIT"); return result; } catch (error) { if (this.#database.isTransaction) this.#database.exec("ROLLBACK"); throw error; } }
}

function mapMessage(row: MessageRow): DirectSignalRecord {
  const answer = row.in_reply_to_request_id ? { kind: "answer" as const, inReplyToRequestId: row.in_reply_to_request_id } : row.response_required ? { kind: "request" as const } : { kind: "signal" as const };
  return { messageId: row.message_id, ...answer, senderAgentId: row.sender_agent_id, recipientAgentId: row.recipient_agent_id, sourceEntryId: row.source_entry_id, payloadDigest: row.payload_digest, deliveryTiming: row.delivery_timing, responseRequired: Number(row.response_required) === 1, ...(row.acceptance_sequence == null ? {} : { acceptanceSequence: Number(row.acceptance_sequence) }), deliveryStatus: row.delivery_status, createdAtMs: Number(row.created_at_ms), ...(row.accepted_at_ms == null ? {} : { acceptedAtMs: Number(row.accepted_at_ms) }), ...(row.delivered_at_ms == null ? {} : { deliveredAtMs: Number(row.delivered_at_ms) }) };
}
function mapPointer(row: PointerRow): PendingMessagePointer { return { messageId: row.message_id, senderAgentId: row.sender_agent_id, recipientAgentId: row.recipient_agent_id, sourceEntryId: row.source_entry_id, payloadDigest: row.payload_digest, deliveryTiming: row.delivery_timing, responseRequired: Number(row.response_required) === 1, ...(row.in_reply_to_request_id ? { inReplyToRequestId: row.in_reply_to_request_id } : {}), acceptanceSequence: Number(row.acceptance_sequence), acceptedAtMs: Number(row.accepted_at_ms) }; }
function mapRequest(row: RequestRow): RequestRecord { return { requestId: row.request_id, requesterAgentId: row.requester_agent_id, responderAgentId: row.responder_agent_id, answerDeliveryTiming: row.answer_delivery_timing, status: row.status, ...(row.answer_message_id ? { answerMessageId: row.answer_message_id } : {}) }; }
function receiptFor(row: MessageRow) { if (row.acceptance_sequence == null) throw new Error(`Unaccepted Message ${row.message_id} has no receipt`); return { status: "queued" as const, messageId: row.message_id, recipientAgentId: row.recipient_agent_id, acceptanceSequence: Number(row.acceptance_sequence) }; }
function assertSameBinding(row: MessageRow, request: { sender: AgentReference; recipient: AgentReference; sourceEntryId: string; payloadDigest: string; deliveryTiming: SignalDeliveryTiming; responseRequired: boolean; inReplyToRequestId?: string } | SignalAcceptRequest): void {
  const sender = "sender" in request ? request.sender.agentId : request.senderAgentId; const recipient = "recipient" in request ? request.recipient.agentId : request.recipientAgentId;
  const matches = row.sender_agent_id === sender && row.recipient_agent_id === recipient && row.source_entry_id === request.sourceEntryId && row.payload_digest === request.payloadDigest && row.delivery_timing === request.deliveryTiming && Number(row.response_required) === (request.responseRequired ? 1 : 0) && row.in_reply_to_request_id === (request.inReplyToRequestId ?? null);
  if (!matches) throw new WorkflowProtocolError("MessageIdentityConflict", `Message Identity ${"messageId" in request ? request.messageId : row.message_id} is already bound to different routing or source metadata`);
}

function isTransientSqliteLock(error: unknown): boolean {
  const candidate = error as { code?: unknown; message?: unknown };
  return candidate.code === "SQLITE_BUSY" || candidate.code === "SQLITE_LOCKED"
    || (typeof candidate.message === "string" && /database is (busy|locked)/i.test(candidate.message));
}
