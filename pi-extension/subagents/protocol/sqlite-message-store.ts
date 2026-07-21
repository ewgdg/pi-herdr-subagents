import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
import type {
  AcceptedSignal,
  DirectSignalRecord,
  PendingMessagePointer,
  SignalAcceptRequest,
} from "./direct-signal-types.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;

interface RouterRow {
  endpoint: string;
  run_id: string | null;
  fencing_epoch: number | null;
}

interface MessageRow {
  message_id: string;
  sender_agent_id: string;
  recipient_agent_id: string;
  source_entry_id: string;
  payload_digest: string;
  acceptance_sequence: number | null;
  delivery_status: "bound" | "queued" | "delivered";
  created_at_ms: number;
  accepted_at_ms: number | null;
  delivered_at_ms: number | null;
}

interface PointerRow {
  message_id: string;
  sender_agent_id: string;
  recipient_agent_id: string;
  source_entry_id: string;
  payload_digest: string;
  acceptance_sequence: number;
  accepted_at_ms: number;
}
export class DirectSignalStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS recipient_inbox_routers (
        agent_id TEXT PRIMARY KEY REFERENCES workflow_agents(agent_id),
        endpoint TEXT NOT NULL UNIQUE,
        run_id TEXT,
        fencing_epoch INTEGER,
        registered_at_ms INTEGER NOT NULL,
        CHECK (
          (run_id IS NULL AND fencing_epoch IS NULL)
          OR (run_id IS NOT NULL AND fencing_epoch IS NOT NULL AND fencing_epoch > 0)
        )
      ) STRICT;

      CREATE TABLE IF NOT EXISTS direct_signal_messages (
        message_id TEXT PRIMARY KEY,
        sender_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        recipient_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        source_entry_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        acceptance_sequence INTEGER,
        delivery_status TEXT NOT NULL CHECK (delivery_status IN ('bound', 'queued', 'delivered')),
        created_at_ms INTEGER NOT NULL,
        accepted_at_ms INTEGER,
        delivered_at_ms INTEGER,
        UNIQUE (sender_agent_id, source_entry_id),
        CHECK (
          (delivery_status = 'bound' AND acceptance_sequence IS NULL AND accepted_at_ms IS NULL AND delivered_at_ms IS NULL)
          OR (delivery_status = 'queued' AND acceptance_sequence IS NOT NULL AND accepted_at_ms IS NOT NULL AND delivered_at_ms IS NULL)
          OR (delivery_status = 'delivered' AND acceptance_sequence IS NOT NULL AND accepted_at_ms IS NOT NULL AND delivered_at_ms IS NOT NULL)
        )
      ) STRICT;

      CREATE TABLE IF NOT EXISTS recipient_acceptance_counters (
        agent_id TEXT PRIMARY KEY REFERENCES workflow_agents(agent_id),
        last_sequence INTEGER NOT NULL CHECK (last_sequence > 0)
      ) STRICT;

      CREATE TABLE IF NOT EXISTS pending_message_pointers (
        message_id TEXT PRIMARY KEY REFERENCES direct_signal_messages(message_id),
        sender_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        recipient_agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
        source_entry_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        acceptance_sequence INTEGER NOT NULL CHECK (acceptance_sequence > 0),
        accepted_at_ms INTEGER NOT NULL,
        UNIQUE (recipient_agent_id, acceptance_sequence)
      ) STRICT;

      CREATE INDEX IF NOT EXISTS pending_message_recipient_order
      ON pending_message_pointers (recipient_agent_id, acceptance_sequence);
    `);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  registerRouter(input: {
    recipient: AgentReference;
    ownership?: AgentRunOwnership;
    endpoint: string;
    registeredAtMs: number;
  }): void {
    this.#withImmediateTransaction(() => {
      this.#assertWorkflow(input.recipient.workflowOwnerId);
      this.#requireAgent(input.recipient.agentId);
      this.#assertRouterOwnership(input.recipient, input.ownership);
      this.#database.prepare(`
        INSERT INTO recipient_inbox_routers (
          agent_id, endpoint, run_id, fencing_epoch, registered_at_ms
        ) VALUES (?, ?, ?, ?, ?)
      `).run(
        input.recipient.agentId,
        input.endpoint,
        input.ownership?.runId ?? null,
        input.ownership?.epoch ?? null,
        input.registeredAtMs,
      );
    });
  }

  unregisterRouter(recipient: AgentReference, endpoint: string): void {
    this.#database.prepare(`
      DELETE FROM recipient_inbox_routers
      WHERE agent_id = ? AND endpoint = ?
    `).run(recipient.agentId, endpoint);
  }

  readRouter(recipient: AgentReference): { endpoint: string } | undefined {
    this.#assertWorkflow(recipient.workflowOwnerId);
    this.#requireAgent(recipient.agentId);
    const row = this.#database.prepare(`
      SELECT endpoint, run_id, fencing_epoch
      FROM recipient_inbox_routers
      WHERE agent_id = ?
    `).get(recipient.agentId) as RouterRow | undefined;
    return row ? { endpoint: row.endpoint } : undefined;
  }

  bindSignal(input: {
    messageId: string;
    sender: AgentReference;
    recipient: AgentReference;
    sourceEntryId: string;
    payloadDigest: string;
    createdAtMs: number;
  }): void {
    this.#withImmediateTransaction(() => {
      this.#assertWorkflow(input.sender.workflowOwnerId);
      if (input.recipient.workflowOwnerId !== input.sender.workflowOwnerId) {
        throw new WorkflowProtocolError(
          "WorkflowMismatch",
          `Signal target ${input.recipient.agentId} belongs to another Workflow`,
        );
      }
      this.#requireAgent(input.sender.agentId);
      this.#requireAgent(input.recipient.agentId);
      this.#database.prepare(`
        INSERT INTO direct_signal_messages (
          message_id, sender_agent_id, recipient_agent_id, source_entry_id,
          payload_digest, acceptance_sequence, delivery_status,
          created_at_ms, accepted_at_ms, delivered_at_ms
        ) VALUES (?, ?, ?, ?, ?, NULL, 'bound', ?, NULL, NULL)
      `).run(
        input.messageId,
        input.sender.agentId,
        input.recipient.agentId,
        input.sourceEntryId,
        input.payloadDigest,
        input.createdAtMs,
      );
    });
  }

  discardUnacceptedSignal(sender: AgentReference, messageId: string): boolean {
    this.#assertWorkflow(sender.workflowOwnerId);
    const removed = this.#database.prepare(`
      DELETE FROM direct_signal_messages
      WHERE message_id = ? AND sender_agent_id = ? AND delivery_status = 'bound'
    `).run(messageId, sender.agentId);
    return Number(removed.changes) === 1;
  }

  acceptSignal(input: {
    request: SignalAcceptRequest;
    recipient: AgentReference;
    ownership?: AgentRunOwnership;
    endpoint: string;
    acceptedAtMs: number;
  }): AcceptedSignal {
    return this.#withImmediateTransaction(() => {
      this.#assertWorkflow(input.request.workflowOwnerId);
      this.#assertCurrentRouter(input.recipient, input.ownership, input.endpoint);
      const existing = this.#readMessage(input.request.messageId);
      if (!existing) {
        throw new WorkflowProtocolError(
          "InvalidMessageSource",
          `Signal ${input.request.messageId} has no durable sender binding`,
        );
      }
      assertSameBinding(existing, input.request);
      if (existing.delivery_status !== "bound") {
        throw new WorkflowProtocolError(
          "MessageIdentityConflict",
          `Signal ${input.request.messageId} was already accepted`,
        );
      }

      const lifecycle = this.#recipientLifecycle(input.recipient);
      if (lifecycle === "ended") {
        throw new WorkflowProtocolError(
          "RecipientEnded",
          `Ended Agent ${input.recipient.agentId} cannot accept a Signal`,
        );
      }
      if (lifecycle === "interrupted") {
        throw new WorkflowProtocolError(
          "RecipientUnreachable",
          `Interrupted Agent ${input.recipient.agentId} cannot consume a direct Signal`,
        );
      }
      const acceptanceSequence = this.#nextAcceptanceSequence(input.recipient.agentId);
      this.#database.prepare(`
        UPDATE direct_signal_messages
        SET acceptance_sequence = ?, delivery_status = 'queued', accepted_at_ms = ?
        WHERE message_id = ? AND delivery_status = 'bound'
      `).run(acceptanceSequence, input.acceptedAtMs, input.request.messageId);
      this.#database.prepare(`
        INSERT INTO pending_message_pointers (
          message_id, sender_agent_id, recipient_agent_id, source_entry_id,
          payload_digest, acceptance_sequence, accepted_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.request.messageId,
        input.request.senderAgentId,
        input.request.recipientAgentId,
        input.request.sourceEntryId,
        input.request.payloadDigest,
        acceptanceSequence,
        input.acceptedAtMs,
      );
      return {
        receipt: {
          status: "queued",
          messageId: input.request.messageId,
          recipientAgentId: input.request.recipientAgentId,
          acceptanceSequence,
        },
        delivery: "project",
        wakeRecipient: lifecycle === "waiting" || lifecycle === "owner",
      };
    });
  }

  commitDelivery(input: {
    recipient: AgentReference;
    ownership?: AgentRunOwnership;
    endpoint: string;
    messageId: string;
    deliveredAtMs: number;
  }): boolean {
    return this.#withImmediateTransaction(() => {
      this.#assertCurrentRouter(input.recipient, input.ownership, input.endpoint);
      const message = this.#readMessage(input.messageId);
      if (!message || message.recipient_agent_id !== input.recipient.agentId) {
        return false;
      }
      if (message.delivery_status === "delivered") return true;
      const removed = this.#database.prepare(`
        DELETE FROM pending_message_pointers
        WHERE message_id = ? AND recipient_agent_id = ?
      `).run(input.messageId, input.recipient.agentId);
      if (Number(removed.changes) !== 1) {
        throw new Error(`Pending pointer is missing for Signal ${input.messageId}`);
      }
      this.#database.prepare(`
        UPDATE direct_signal_messages
        SET delivery_status = 'delivered', delivered_at_ms = ?
        WHERE message_id = ? AND delivery_status = 'queued'
      `).run(input.deliveredAtMs, input.messageId);
      return true;
    });
  }

  inspectMessage(workflowOwnerId: string, messageId: string): DirectSignalRecord | undefined {
    this.#assertWorkflow(workflowOwnerId);
    const row = this.#readMessage(messageId);
    return row ? mapMessage(row) : undefined;
  }

  listMessages(workflowOwnerId: string): DirectSignalRecord[] {
    this.#assertWorkflow(workflowOwnerId);
    return (this.#database.prepare(`
      SELECT message_id, sender_agent_id, recipient_agent_id, source_entry_id,
             payload_digest, acceptance_sequence, delivery_status,
             created_at_ms, accepted_at_ms, delivered_at_ms
      FROM direct_signal_messages
      ORDER BY created_at_ms, message_id
    `).all() as unknown as MessageRow[]).map(mapMessage);
  }

  listPending(recipient: AgentReference): PendingMessagePointer[] {
    this.#assertWorkflow(recipient.workflowOwnerId);
    this.#requireAgent(recipient.agentId);
    return (this.#database.prepare(`
      SELECT message_id, sender_agent_id, recipient_agent_id, source_entry_id,
             payload_digest, acceptance_sequence, accepted_at_ms
      FROM pending_message_pointers
      WHERE recipient_agent_id = ?
      ORDER BY acceptance_sequence
    `).all(recipient.agentId) as unknown as PointerRow[]).map((row) => ({
      messageId: row.message_id,
      senderAgentId: row.sender_agent_id,
      recipientAgentId: row.recipient_agent_id,
      sourceEntryId: row.source_entry_id,
      payloadDigest: row.payload_digest,
      acceptanceSequence: Number(row.acceptance_sequence),
      acceptedAtMs: Number(row.accepted_at_ms),
    }));
  }

  #assertCurrentRouter(
    recipient: AgentReference,
    ownership: AgentRunOwnership | undefined,
    endpoint: string,
  ): void {
    this.#assertWorkflow(recipient.workflowOwnerId);
    this.#requireAgent(recipient.agentId);
    this.#assertRouterOwnership(recipient, ownership);
    const row = this.#database.prepare(`
      SELECT endpoint, run_id, fencing_epoch
      FROM recipient_inbox_routers
      WHERE agent_id = ?
    `).get(recipient.agentId) as RouterRow | undefined;
    const matches = row?.endpoint === endpoint
      && row.run_id === (ownership?.runId ?? null)
      && (row.fencing_epoch == null ? ownership == null : Number(row.fencing_epoch) === ownership?.epoch);
    if (!matches) {
      throw new WorkflowProtocolError(
        "RecipientUnreachable",
        `Recipient Inbox Router is no longer current for Agent ${recipient.agentId}`,
      );
    }
  }

  #assertRouterOwnership(
    recipient: AgentReference,
    ownership: AgentRunOwnership | undefined,
  ): void {
    const owner = this.#workflowOwnerId();
    if (recipient.agentId === owner) {
      if (ownership) {
        throw new WorkflowProtocolError(
          "OwnerActivationForbidden",
          "Workflow Owner Inbox Router cannot use Subagent Agent Run ownership",
        );
      }
      return;
    }
    if (!ownership || ownership.agentId !== recipient.agentId || ownership.workflowOwnerId !== owner) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Recipient Inbox Router lacks Agent Run ownership for ${recipient.agentId}`,
      );
    }
    const row = this.#database.prepare(`
      SELECT owner_id, fencing_epoch
      FROM ownership
      WHERE resource_id = ?
    `).get(ownership.resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
    if (!row || row.owner_id !== ownership.runId || Number(row.fencing_epoch) !== ownership.epoch) {
      throw new WorkflowProtocolError(
        "OwnershipLost",
        `Recipient Inbox Router no longer owns ${recipient.agentId} at fencing epoch ${ownership.epoch}`,
      );
    }
  }

  #recipientLifecycle(recipient: AgentReference): "owner" | "active" | "waiting" | "interrupted" | "ended" {
    if (recipient.agentId === this.#workflowOwnerId()) return "owner";
    const row = this.#database.prepare(`
      SELECT phase, open_state
      FROM agent_activations
      WHERE agent_id = ?
      ORDER BY activation_sequence DESC
      LIMIT 1
    `).get(recipient.agentId) as {
      phase: "open" | "ended";
      open_state: "active" | "waiting" | "interrupted" | null;
    } | undefined;
    if (!row || row.phase === "ended") return "ended";
    return row.open_state ?? "ended";
  }

  #nextAcceptanceSequence(agentId: string): number {
    const row = this.#database.prepare(`
      SELECT last_sequence
      FROM recipient_acceptance_counters
      WHERE agent_id = ?
    `).get(agentId) as { last_sequence: number } | undefined;
    const sequence = Number(row?.last_sequence ?? 0) + 1;
    this.#database.prepare(`
      INSERT INTO recipient_acceptance_counters (agent_id, last_sequence)
      VALUES (?, ?)
      ON CONFLICT (agent_id) DO UPDATE SET last_sequence = excluded.last_sequence
    `).run(agentId, sequence);
    return sequence;
  }

  #readMessage(messageId: string): MessageRow | undefined {
    return this.#database.prepare(`
      SELECT message_id, sender_agent_id, recipient_agent_id, source_entry_id,
             payload_digest, acceptance_sequence, delivery_status,
             created_at_ms, accepted_at_ms, delivered_at_ms
      FROM direct_signal_messages
      WHERE message_id = ?
    `).get(messageId) as MessageRow | undefined;
  }

  #assertWorkflow(workflowOwnerId: string): void {
    const owner = this.#workflowOwnerId();
    if (owner !== workflowOwnerId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Workflow store belongs to ${owner}, not ${workflowOwnerId}`,
      );
    }
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

function mapMessage(row: MessageRow): DirectSignalRecord {
  return {
    messageId: row.message_id,
    senderAgentId: row.sender_agent_id,
    recipientAgentId: row.recipient_agent_id,
    sourceEntryId: row.source_entry_id,
    payloadDigest: row.payload_digest,
    ...(row.acceptance_sequence == null ? {} : { acceptanceSequence: Number(row.acceptance_sequence) }),
    deliveryStatus: row.delivery_status,
    createdAtMs: Number(row.created_at_ms),
    ...(row.accepted_at_ms == null ? {} : { acceptedAtMs: Number(row.accepted_at_ms) }),
    ...(row.delivered_at_ms == null ? {} : { deliveredAtMs: Number(row.delivered_at_ms) }),
  };
}

function assertSameBinding(row: MessageRow, request: SignalAcceptRequest): void {
  const matches = row.sender_agent_id === request.senderAgentId
    && row.recipient_agent_id === request.recipientAgentId
    && row.source_entry_id === request.sourceEntryId
    && row.payload_digest === request.payloadDigest;
  if (!matches) {
    throw new WorkflowProtocolError(
      "MessageIdentityConflict",
      `Message Identity ${request.messageId} is already bound to different routing or source metadata`,
    );
  }
}
