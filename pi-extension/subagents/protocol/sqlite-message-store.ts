import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentCapabilityConfiguration,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
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
  reactivates_recipient: number;
  in_reply_to_request_id: string | null; acceptance_sequence: number | null;
  delivery_status: "bound" | "queued" | "delivered"; created_at_ms: number;
  accepted_at_ms: number | null; delivered_at_ms: number | null;
}
interface PointerRow {
  message_id: string; sender_agent_id: string; recipient_agent_id: string; source_entry_id: string;
  payload_digest: string; delivery_timing: SignalDeliveryTiming; response_required: number;
  reactivates_recipient: number;
  in_reply_to_request_id: string | null; acceptance_sequence: number; accepted_at_ms: number;
}
interface RequestRow {
  request_id: string; requester_agent_id: string; responder_agent_id: string;
  answer_delivery_timing: SignalDeliveryTiming; status: "open" | "answered" | "resolved";
  answer_message_id: string | null;
}

export interface SpawnedInitialRequestInput {
  spawner: AgentReference;
  child: {
    agentId: string;
    sessionPath: string;
    name: string;
    agentDefinition: string;
    capabilities: AgentCapabilityConfiguration;
    launchPolicy?: import("./workflow-types.ts").AgentLaunchPolicy;
  };
  runId: string;
  messageId: string;
  sourceEntryId: string;
  payloadDigest: string;
  routerEndpoint?: string;
  createdAtMs: number;
}

export interface SpawnedInitialRequestReceipt extends QueuedSignalReceipt {
  childAgentId: string;
  runId: string;
  fencingEpoch: number;
}

/** Immutable spawn metadata used to reconcile an acknowledgement-lost retry. */
export interface SpawnedInitialRequestReconciliation {
  spawner: AgentReference;
  sourceEntryId: string;
  payloadDigest: string;
  agentDefinition: string;
  name: string;
  capabilities: AgentCapabilityConfiguration;
}

export interface EndedRecipientRequestInput {
  request: SignalAcceptRequest;
  recipient: AgentReference;
  endpoint: string;
  runId: string;
  checkpoint: string;
  acceptedAtMs: number;
}

export interface EndedRecipientRequestReceipt extends QueuedSignalReceipt {
  ownership: AgentRunOwnership;
  committedByThisPreparation: boolean;
}

/** Query-only Request/Message access. Opens SQLite read-only and performs no schema initialization or PRAGMA writes. */
export class DirectSignalInspectionStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs, readOnly: true });
  }

  close(): void {
    if (!this.#closed) { this.#database.close(); this.#closed = true; }
  }

  inspectMessage(owner: string, messageId: string): DirectSignalRecord | undefined {
    this.#assertWorkflow(owner);
    const row = this.#database.prepare("SELECT * FROM direct_signal_messages WHERE message_id = ?").get(messageId) as MessageRow | undefined;
    return row ? mapMessage(row) : undefined;
  }

  inspectRequest(owner: string, requestId: string): RequestRecord | undefined {
    this.#assertWorkflow(owner);
    const row = this.#database.prepare("SELECT * FROM workflow_requests WHERE request_id = ?").get(requestId) as RequestRow | undefined;
    return row ? mapRequest(row) : undefined;
  }

  inspectRequestProjection(owner: string, requestId: string): {
    request: RequestRecord;
    requestDeliveryStatus?: DirectSignalRecord["deliveryStatus"];
    answerDeliveryStatus?: DirectSignalRecord["deliveryStatus"];
  } | undefined {
    this.#assertWorkflow(owner);
    const messageTable = this.#database.prepare(
      "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'direct_signal_messages'",
    ).get();
    if (!messageTable) return undefined;
    const row = this.#database.prepare(`
      SELECT r.*, request_message.delivery_status AS request_delivery_status,
        answer_message.delivery_status AS answer_delivery_status
      FROM workflow_requests r
      LEFT JOIN direct_signal_messages request_message ON request_message.message_id = r.request_id
      LEFT JOIN direct_signal_messages answer_message ON answer_message.message_id = r.answer_message_id
      WHERE r.request_id = ?
    `).get(requestId) as (RequestRow & {
      request_delivery_status: DirectSignalRecord["deliveryStatus"] | null;
      answer_delivery_status: DirectSignalRecord["deliveryStatus"] | null;
    }) | undefined;
    if (!row) return undefined;
    return {
      request: mapRequest(row),
      ...(row.request_delivery_status ? { requestDeliveryStatus: row.request_delivery_status } : {}),
      ...(row.answer_delivery_status ? { answerDeliveryStatus: row.answer_delivery_status } : {}),
    };
  }

  #assertWorkflow(owner: string): void {
    const row = this.#database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1").get() as { owner_agent_id: string } | undefined;
    if (!row || row.owner_agent_id !== owner) {
      throw new WorkflowProtocolError("WorkflowMismatch", "Inspection source does not belong to the current Workflow");
    }
  }
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

  /** The ready Router endpoint is an external-process prepare barrier. */
  acceptSpawnedInitialRequest(input: SpawnedInitialRequestInput): SpawnedInitialRequestReceipt {
    if (!input.routerEndpoint) {
      throw new WorkflowProtocolError("RecipientUnreachable", `Recipient Inbox Router is unavailable for Agent ${input.child.agentId}`);
    }
    return this.#withTransaction(() => {
      this.#assertWorkflow(input.spawner.workflowOwnerId);
      const existing = this.#readMessageBySource(input.spawner.agentId, input.sourceEntryId);
      if (existing) {
        assertSameBinding(existing, {
          sender: input.spawner,
          recipient: { workflowOwnerId: input.spawner.workflowOwnerId, agentId: input.child.agentId },
          sourceEntryId: input.sourceEntryId,
          payloadDigest: input.payloadDigest,
          deliveryTiming: "steer",
          responseRequired: true,
        });
        this.#assertSpawnBinding(existing, input);
        const receipt = receiptFor(existing);
        return { ...receipt, childAgentId: input.child.agentId, runId: input.runId, fencingEpoch: this.#spawnOwnershipEpoch(input.spawner.workflowOwnerId, input.child.agentId, input.runId) };
      }
      const spawner = this.#readAgent(input.spawner.agentId);
      if (!spawner) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${input.spawner.agentId}`);
      const capabilities = JSON.parse(spawner.capabilities_json) as AgentCapabilityConfiguration;
      if (!capabilities.spawning) throw new WorkflowProtocolError("SpawnerCapabilityRequired", `Agent ${input.spawner.agentId} does not have spawning capability`);
      if (this.#readAgent(input.child.agentId)) throw new WorkflowProtocolError("AgentAlreadyExists", `Agent is already a member of Workflow ${input.spawner.workflowOwnerId}: ${input.child.agentId}`);
      if (this.#readMessage(input.messageId)) throw new WorkflowProtocolError("MessageIdentityConflict", `Message Identity ${input.messageId} is already bound to different routing or source metadata`);

      this.#database.prepare(`
        INSERT INTO workflow_agents (agent_id, session_path, name, agent_definition, spawner_agent_id, capabilities_json, launch_policy_json, created_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(input.child.agentId, input.child.sessionPath, input.child.name, input.child.agentDefinition, input.spawner.agentId, JSON.stringify({ spawning: input.child.capabilities.spawning }), input.child.launchPolicy ? JSON.stringify(input.child.launchPolicy) : null, input.createdAtMs);
      const resourceId = `agent-run:${input.spawner.workflowOwnerId}:${input.child.agentId}`;
      const fencingEpoch = 1;
      this.#database.prepare("INSERT INTO ownership_epochs (resource_id, last_epoch) VALUES (?, ?)").run(resourceId, fencingEpoch);
      this.#database.prepare("INSERT INTO ownership (resource_id, owner_id, fencing_epoch) VALUES (?, ?, ?)").run(resourceId, input.runId, fencingEpoch);
      this.#database.prepare(`
        INSERT INTO agent_activations (activation_id, agent_id, run_id, fencing_epoch, activation_sequence, revision, turn_sequence, phase, open_state, ended_outcome, failure_error, failure_exit_code, interrupt_turn_sequence, interrupt_requested_at_ms, created_at_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, 1, 1, 1, 'open', 'active', NULL, NULL, NULL, NULL, NULL, ?, ?)
      `).run(input.runId, input.child.agentId, input.runId, fencingEpoch, input.createdAtMs, input.createdAtMs);
      this.#database.prepare(`
        INSERT INTO recipient_inbox_routers (agent_id, endpoint, run_id, fencing_epoch, registered_at_ms)
        VALUES (?, ?, ?, ?, ?)
      `).run(input.child.agentId, input.routerEndpoint, input.runId, fencingEpoch, input.createdAtMs);
      this.#database.prepare(`
        INSERT INTO direct_signal_messages (message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing, response_required, in_reply_to_request_id, acceptance_sequence, delivery_status, created_at_ms, accepted_at_ms, delivered_at_ms)
        VALUES (?, ?, ?, ?, ?, 'steer', 1, NULL, 1, 'delivered', ?, ?, ?)
      `).run(input.messageId, input.spawner.agentId, input.child.agentId, input.sourceEntryId, input.payloadDigest, input.createdAtMs, input.createdAtMs, input.createdAtMs);
      this.#database.prepare("INSERT INTO recipient_acceptance_counters (agent_id, last_sequence) VALUES (?, 1)").run(input.child.agentId);
      this.#createRequest({ workflowOwnerId: input.spawner.workflowOwnerId, messageId: input.messageId, senderAgentId: input.spawner.agentId, recipientAgentId: input.child.agentId, sourceEntryId: input.sourceEntryId, payloadDigest: input.payloadDigest, deliveryTiming: "steer", responseRequired: true, message: "" });
      return { status: "delivered", messageId: input.messageId, recipientAgentId: input.child.agentId, acceptanceSequence: 1, childAgentId: input.child.agentId, runId: input.runId, fencingEpoch };
    });
  }

  /**
   * Reconcile an acknowledgement-lost spawn without treating an arbitrary
   * same-source Request as a committed child creation.
   */
  reconcileSpawnedInitialRequest(
    input: SpawnedInitialRequestReconciliation,
  ): QueuedSignalReceipt | undefined {
    this.#assertWorkflow(input.spawner.workflowOwnerId);
    const existing = this.#readMessageBySource(input.spawner.agentId, input.sourceEntryId);
    if (!existing) return undefined;
    this.#assertSpawnReconciliationBinding(existing, input);
    return receiptFor(existing);
  }

  /** Atomically resume an ended Agent and enqueue its first new Request. */
  acceptEndedRecipientRequest(input: EndedRecipientRequestInput): EndedRecipientRequestReceipt {
    return this.#withTransaction(() => {
      this.#assertWorkflow(input.request.workflowOwnerId);
      this.#requireAgent(input.recipient.agentId);
      this.#assertReactivationAuthorized(input.request.senderAgentId, input.recipient);
      const existing = this.#readMessageBySource(input.request.senderAgentId, input.request.sourceEntryId);
      if (existing && existing.delivery_status !== "bound") {
        assertSameBinding(existing, input.request);
        const router = this.#database.prepare("SELECT endpoint FROM recipient_inbox_routers WHERE agent_id = ?").get(input.recipient.agentId) as { endpoint: string } | undefined;
        if (!router) throw new WorkflowProtocolError("RecipientUnreachable", `Ended Agent ${input.recipient.agentId} has no current resumed Router`);
        const ownership = this.reconcilePreparedRecipientRouter({ recipient: input.recipient, endpoint: router.endpoint });
        if (!ownership) throw new WorkflowProtocolError("RecipientUnreachable", `Ended Agent ${input.recipient.agentId} has no current resumed run`);
        return {
          ...receiptFor(existing), ownership,
          committedByThisPreparation: router.endpoint === input.endpoint && ownership.runId === input.runId,
        };
      }
      if (existing) {
        assertSameBinding(existing, input.request);
        if (existing.message_id !== input.request.messageId) {
          throw new WorkflowProtocolError("MessageIdentityConflict", `Prepared reactivation Message ${input.request.messageId} does not match bound Message ${existing.message_id}`);
        }
      }
      if (this.#recipientLifecycle(input.recipient) !== "ended") {
        throw new WorkflowProtocolError("InvalidLifecycleTransition", `Agent ${input.recipient.agentId} is no longer ended`);
      }
      const resourceId = `agent-run:${input.recipient.workflowOwnerId}:${input.recipient.agentId}`;
      if (this.#database.prepare("SELECT 1 FROM ownership WHERE resource_id = ?").get(resourceId)) {
        throw new WorkflowProtocolError("AgentRunAlreadyOwned", `Agent ${input.recipient.agentId} already has a running Agent Run`);
      }
      const epochRow = this.#database.prepare("SELECT last_epoch FROM ownership_epochs WHERE resource_id = ?").get(resourceId) as { last_epoch: number } | undefined;
      const epoch = Number(epochRow?.last_epoch ?? 0) + 1;
      this.#database.prepare("INSERT INTO ownership_epochs (resource_id, last_epoch) VALUES (?, ?) ON CONFLICT (resource_id) DO UPDATE SET last_epoch = excluded.last_epoch").run(resourceId, epoch);
      this.#database.prepare("INSERT INTO ownership (resource_id, owner_id, fencing_epoch) VALUES (?, ?, ?)").run(resourceId, input.runId, epoch);
      this.#database.prepare("INSERT INTO fenced_state (resource_id, state_key, value, fencing_epoch) VALUES (?, 'agent-run-checkpoint', ?, ?) ON CONFLICT (resource_id, state_key) DO UPDATE SET value = excluded.value, fencing_epoch = excluded.fencing_epoch").run(resourceId, input.checkpoint, epoch);
      const prior = this.#database.prepare("SELECT activation_sequence FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1").get(input.recipient.agentId) as { activation_sequence: number } | undefined;
      if (!prior) throw new WorkflowProtocolError("RecipientEnded", `Agent ${input.recipient.agentId} has no activation to reactivate`);
      this.#database.prepare(`INSERT INTO agent_activations (activation_id, agent_id, run_id, fencing_epoch, activation_sequence, revision, turn_sequence, phase, open_state, ended_outcome, failure_error, failure_exit_code, interrupt_turn_sequence, interrupt_requested_at_ms, created_at_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, 1, 1, 'open', 'active', NULL, NULL, NULL, NULL, NULL, ?, ?)`).run(input.runId, input.recipient.agentId, input.runId, epoch, Number(prior.activation_sequence) + 1, input.acceptedAtMs, input.acceptedAtMs);
      this.#database.prepare(`INSERT INTO recipient_inbox_routers (agent_id, endpoint, run_id, fencing_epoch, registered_at_ms)
        VALUES (?, ?, ?, ?, ?) ON CONFLICT (agent_id) DO UPDATE SET endpoint = excluded.endpoint, run_id = excluded.run_id, fencing_epoch = excluded.fencing_epoch, registered_at_ms = excluded.registered_at_ms`).run(input.recipient.agentId, input.endpoint, input.runId, epoch, input.acceptedAtMs);
      const sequence = this.#nextAcceptanceSequence(input.recipient.agentId);
      if (existing) {
        this.#database.prepare(`UPDATE direct_signal_messages
          SET acceptance_sequence = ?, delivery_status = 'queued', accepted_at_ms = ?, reactivates_recipient = 1
          WHERE message_id = ? AND delivery_status = 'bound'`).run(sequence, input.acceptedAtMs, existing.message_id);
      } else {
        this.#database.prepare(`INSERT INTO direct_signal_messages (message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing, response_required, reactivates_recipient, in_reply_to_request_id, acceptance_sequence, delivery_status, created_at_ms, accepted_at_ms, delivered_at_ms)
          VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, 'queued', ?, ?, NULL)`).run(input.request.messageId, input.request.senderAgentId, input.request.recipientAgentId, input.request.sourceEntryId, input.request.payloadDigest, input.request.deliveryTiming, input.request.inReplyToRequestId ?? null, sequence, input.acceptedAtMs, input.acceptedAtMs);
      }
      if (input.request.inReplyToRequestId) this.#claimAnswerSlot(input.request);
      this.#createRequest(input.request);
      this.#database.prepare(`INSERT INTO pending_message_pointers (message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing, response_required, reactivates_recipient, in_reply_to_request_id, acceptance_sequence, accepted_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)`).run(input.request.messageId, input.request.senderAgentId, input.request.recipientAgentId, input.request.sourceEntryId, input.request.payloadDigest, input.request.deliveryTiming, input.request.inReplyToRequestId ?? null, sequence, input.acceptedAtMs);
      return {
        status: "queued", messageId: input.request.messageId, recipientAgentId: input.recipient.agentId, acceptanceSequence: sequence,
        ownership: { workflowOwnerId: input.recipient.workflowOwnerId, agentId: input.recipient.agentId, runId: input.runId, epoch, resourceId },
        committedByThisPreparation: true,
      };
    });
  }

  assertEndedRecipientRequestAuthorized(sender: AgentReference, recipient: AgentReference): void {
    this.#assertWorkflow(sender.workflowOwnerId);
    this.#assertReactivationAuthorized(sender.agentId, recipient);
  }

  /** Reconcile only an exact Router/ownership/activation footprint after IPC loss. */
  reconcilePreparedRecipientRouter(input: { recipient: AgentReference; endpoint: string }): AgentRunOwnership | undefined {
    this.#assertWorkflow(input.recipient.workflowOwnerId);
    const resourceId = `agent-run:${input.recipient.workflowOwnerId}:${input.recipient.agentId}`;
    const router = this.#database.prepare("SELECT endpoint, run_id, fencing_epoch FROM recipient_inbox_routers WHERE agent_id = ?").get(input.recipient.agentId) as { endpoint: string; run_id: string; fencing_epoch: number } | undefined;
    const ownership = this.#database.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?").get(resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
    if (!router && !ownership) return undefined;
    if (!router || !ownership || router.endpoint !== input.endpoint || router.run_id !== ownership.owner_id || Number(router.fencing_epoch) !== Number(ownership.fencing_epoch)) {
      throw new WorkflowProtocolError("AgentRunAlreadyOwned", `Prepared Router for Agent ${input.recipient.agentId} conflicts with durable ownership`);
    }
    const activation = this.#database.prepare("SELECT run_id, fencing_epoch, phase, open_state FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1").get(input.recipient.agentId) as { run_id: string; fencing_epoch: number; phase: string; open_state: string | null } | undefined;
    if (!activation || activation.run_id !== ownership.owner_id || Number(activation.fencing_epoch) !== Number(ownership.fencing_epoch) || activation.phase !== "open" || activation.open_state !== "active") {
      throw new WorkflowProtocolError("AgentRunAlreadyOwned", `Prepared Router for Agent ${input.recipient.agentId} has no matching active activation`);
    }
    return { ...input.recipient, runId: ownership.owner_id, epoch: Number(ownership.fencing_epoch), resourceId };
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
      const lifecycle = this.#recipientLifecycle(input.recipient);
      const reactivatesRecipient = (lifecycle === "interrupted" || lifecycle === "ended") && input.request.responseRequired;
      if (lifecycle === "ended" && !input.request.responseRequired) {
        throw new WorkflowProtocolError("RecipientEnded", `Ended Agent ${input.recipient.agentId} cannot accept a Signal`);
      }
      if (reactivatesRecipient) {
        this.#reactivateForAuthorizedRequest(input);
      }

      if (input.request.inReplyToRequestId) this.#claimAnswerSlot(input.request);
      const sequence = this.#nextAcceptanceSequence(input.recipient.agentId);
      this.#database.prepare(`UPDATE direct_signal_messages SET acceptance_sequence = ?, delivery_status = 'queued', accepted_at_ms = ?, reactivates_recipient = ? WHERE message_id = ? AND delivery_status = 'bound'`).run(sequence, input.acceptedAtMs, reactivatesRecipient ? 1 : 0, input.request.messageId);
      if (input.request.responseRequired) this.#createRequest(input.request);
      this.#database.prepare(`
        INSERT INTO pending_message_pointers (
          message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest, delivery_timing,
          response_required, reactivates_recipient, in_reply_to_request_id, acceptance_sequence, accepted_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(input.request.messageId, input.request.senderAgentId, input.request.recipientAgentId, input.request.sourceEntryId,
        input.request.payloadDigest, input.request.deliveryTiming, input.request.responseRequired ? 1 : 0,
        reactivatesRecipient ? 1 : 0, input.request.inReplyToRequestId ?? null, sequence, input.acceptedAtMs);
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
        this.#database.prepare(`
          UPDATE undeclared_settlement_episodes
          SET status = 'closed', updated_at_ms = ?
          WHERE agent_id = (
            SELECT requester_agent_id FROM workflow_requests WHERE request_id = ?
          ) AND status = 'open' AND EXISTS (
            SELECT 1 FROM undeclared_settlement_dependencies
            WHERE episode_id = undeclared_settlement_episodes.episode_id
              AND dependency_kind = 'agent' AND dependency_id = ?
          )
        `).run(input.deliveredAtMs, message.in_reply_to_request_id, message.in_reply_to_request_id);
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

  recipientLifecycle(recipient: AgentReference): "owner" | "active" | "waiting" | "waiting-human" | "interrupted" | "ended" { this.#assertWorkflow(recipient.workflowOwnerId); return this.#recipientLifecycle(recipient); }
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
        response_required INTEGER NOT NULL CHECK (response_required IN (0, 1)),
        reactivates_recipient INTEGER NOT NULL DEFAULT 0 CHECK (reactivates_recipient IN (0, 1)), in_reply_to_request_id TEXT,
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
        reactivates_recipient INTEGER NOT NULL DEFAULT 0 CHECK (reactivates_recipient IN (0, 1)), in_reply_to_request_id TEXT, acceptance_sequence INTEGER NOT NULL CHECK (acceptance_sequence > 0), accepted_at_ms INTEGER NOT NULL,
        UNIQUE (recipient_agent_id, acceptance_sequence)
      ) STRICT;
      CREATE INDEX IF NOT EXISTS pending_message_recipient_order ON pending_message_pointers (recipient_agent_id, acceptance_sequence);
    `);
    this.#ensureColumn("direct_signal_messages", "reactivates_recipient", "INTEGER NOT NULL DEFAULT 0 CHECK (reactivates_recipient IN (0, 1))");
    this.#ensureColumn("pending_message_pointers", "reactivates_recipient", "INTEGER NOT NULL DEFAULT 0 CHECK (reactivates_recipient IN (0, 1))");
  }

  #ensureColumn(table: string, column: string, definition: string): void {
    const columns = this.#database.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
    if (!columns.some((candidate) => candidate.name === column)) {
      this.#database.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
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
  #recipientLifecycle(recipient: AgentReference): "owner" | "active" | "waiting" | "waiting-human" | "interrupted" | "ended" {
    if (recipient.agentId === this.#workflowOwnerId()) return "owner";
    const row = this.#database.prepare("SELECT phase, open_state FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1").get(recipient.agentId) as { phase: "open" | "ended"; open_state: "active" | "waiting" | "interrupted" | null } | undefined;
    if (!row || row.phase === "ended") return "ended";
    // The returned Human result remains non-durable until Pi appends it. It
    // must fence Inbox projection even during the active tool-resume turn.
    const human = this.#database.prepare(`
      SELECT 1 FROM human_interrupts
      WHERE agent_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
    `).get(recipient.agentId);
    if (human) return "waiting-human";
    return row.open_state ?? "ended";
  }
  #nextAcceptanceSequence(agentId: string): number { const row = this.#database.prepare("SELECT last_sequence FROM recipient_acceptance_counters WHERE agent_id = ?").get(agentId) as { last_sequence: number } | undefined; const next = Number(row?.last_sequence ?? 0) + 1; this.#database.prepare("INSERT INTO recipient_acceptance_counters (agent_id, last_sequence) VALUES (?, ?) ON CONFLICT (agent_id) DO UPDATE SET last_sequence = excluded.last_sequence").run(agentId, next); return next; }
  #reactivateForAuthorizedRequest(input: { request: SignalAcceptRequest; recipient: AgentReference; ownership?: AgentRunOwnership; acceptedAtMs: number }): void {
    this.#assertReactivationAuthorized(input.request.senderAgentId, input.recipient);
    const current = this.#database.prepare("SELECT phase, open_state, activation_sequence FROM agent_activations WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1").get(input.recipient.agentId) as { phase: "open" | "ended"; open_state: "active" | "waiting" | "interrupted" | null; activation_sequence: number } | undefined;
    if (!current) throw new WorkflowProtocolError("RecipientEnded", `Agent ${input.recipient.agentId} has no activation to reactivate`);
    if (current.phase === "open" && current.open_state === "interrupted") {
      this.#database.prepare("UPDATE agent_activations SET open_state = 'active', revision = revision + 1, turn_sequence = turn_sequence + 1, updated_at_ms = ? WHERE agent_id = ? AND activation_sequence = ?").run(input.acceptedAtMs, input.recipient.agentId, current.activation_sequence);
      return;
    }
    if (current.phase === "ended") {
      if (!input.ownership) throw new WorkflowProtocolError("RecipientUnreachable", `Recipient Inbox Router is unavailable for Agent ${input.recipient.agentId}`);
      this.#database.prepare(`
        INSERT INTO agent_activations (activation_id, agent_id, run_id, fencing_epoch, activation_sequence, revision, turn_sequence, phase, open_state, ended_outcome, failure_error, failure_exit_code, interrupt_turn_sequence, interrupt_requested_at_ms, created_at_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, 1, 1, 'open', 'active', NULL, NULL, NULL, NULL, NULL, ?, ?)
      `).run(input.ownership.runId, input.recipient.agentId, input.ownership.runId, input.ownership.epoch, Number(current.activation_sequence) + 1, input.acceptedAtMs, input.acceptedAtMs);
    }
  }
  #assertReactivationAuthorized(senderAgentId: string, recipient: AgentReference): void {
    const child = this.#database.prepare("SELECT spawner_agent_id FROM workflow_agents WHERE agent_id = ?").get(recipient.agentId) as { spawner_agent_id: string | null } | undefined;
    const owner = this.#workflowOwnerId();
    if (!child || (senderAgentId !== owner && senderAgentId !== child.spawner_agent_id)) {
      throw new WorkflowProtocolError("RecipientReactivationUnauthorized", `Agent ${senderAgentId} cannot reactivate Agent ${recipient.agentId}`);
    }
  }
  #readMessage(id: string): MessageRow | undefined { return this.#database.prepare("SELECT * FROM direct_signal_messages WHERE message_id = ?").get(id) as MessageRow | undefined; }
  #readMessageBySource(sender: string, source: string): MessageRow | undefined { return this.#database.prepare("SELECT * FROM direct_signal_messages WHERE sender_agent_id = ? AND source_entry_id = ?").get(sender, source) as MessageRow | undefined; }
  #readRequest(id: string): RequestRow | undefined { return this.#database.prepare("SELECT * FROM workflow_requests WHERE request_id = ?").get(id) as RequestRow | undefined; }
  #readAgent(agentId: string): { capabilities_json: string } | undefined { return this.#database.prepare("SELECT capabilities_json FROM workflow_agents WHERE agent_id = ?").get(agentId) as { capabilities_json: string } | undefined; }
  #spawnOwnershipEpoch(workflowOwnerId: string, agentId: string, runId: string): number {
    const resourceId = `agent-run:${workflowOwnerId}:${agentId}`;
    const row = this.#database.prepare("SELECT fencing_epoch FROM ownership WHERE resource_id = ? AND owner_id = ?").get(resourceId, runId) as { fencing_epoch: number } | undefined;
    if (!row) throw new WorkflowProtocolError("MessageIdentityConflict", `Spawn source is bound to a different Agent Run for ${agentId}`);
    return Number(row.fencing_epoch);
  }
  #assertSpawnBinding(existing: MessageRow, input: SpawnedInitialRequestInput): void {
    const child = this.#database.prepare(`
      SELECT session_path, name, agent_definition, capabilities_json, spawner_agent_id
      FROM workflow_agents WHERE agent_id = ?
    `).get(input.child.agentId) as {
      session_path: string; name: string; agent_definition: string | null;
      capabilities_json: string; spawner_agent_id: string | null;
    } | undefined;
    const matches = child
      && child.session_path === input.child.sessionPath
      && child.name === input.child.name
      && child.agent_definition === input.child.agentDefinition
      && child.spawner_agent_id === input.spawner.agentId
      && child.capabilities_json === JSON.stringify({ spawning: input.child.capabilities.spawning });
    if (!matches) {
      throw new WorkflowProtocolError("MessageIdentityConflict", `Spawn source ${input.sourceEntryId} is already bound to different child metadata`);
    }
  }
  #assertSpawnReconciliationBinding(
    existing: MessageRow,
    input: SpawnedInitialRequestReconciliation,
  ): void {
    const child = this.#database.prepare(`
      SELECT name, agent_definition, spawner_agent_id, capabilities_json
      FROM workflow_agents WHERE agent_id = ?
    `).get(existing.recipient_agent_id) as {
      name: string; agent_definition: string | null; spawner_agent_id: string | null; capabilities_json: string;
    } | undefined;
    const activation = this.#database.prepare("SELECT run_id, fencing_epoch FROM agent_activations WHERE agent_id = ? AND activation_sequence = 1").get(existing.recipient_agent_id) as { run_id: string; fencing_epoch: number } | undefined;
    const request = this.#database.prepare("SELECT requester_agent_id, responder_agent_id FROM workflow_requests WHERE request_id = ?").get(existing.message_id) as { requester_agent_id: string; responder_agent_id: string } | undefined;
    const matches = existing.sender_agent_id === input.spawner.agentId
      && existing.source_entry_id === input.sourceEntryId
      && existing.payload_digest === input.payloadDigest
      && existing.delivery_timing === "steer"
      && Number(existing.response_required) === 1
      && existing.in_reply_to_request_id === null
      && existing.delivery_status === "delivered"
      && Number(existing.acceptance_sequence) === 1
      && child?.name === input.name
      && child.agent_definition === input.agentDefinition
      && child.spawner_agent_id === input.spawner.agentId
      && child.capabilities_json === JSON.stringify({ spawning: input.capabilities.spawning })
      && request?.requester_agent_id === input.spawner.agentId
      && request.responder_agent_id === existing.recipient_agent_id
      && Boolean(activation?.run_id)
      && Number(activation?.fencing_epoch) > 0;
    if (!matches) {
      throw new WorkflowProtocolError(
        "MessageIdentityConflict",
        `Spawn source ${input.sourceEntryId} is already bound to a different durable message or child`,
      );
    }
  }
  #assertWorkflow(workflowOwnerId: string): void { const owner = this.#workflowOwnerId(); if (owner !== workflowOwnerId) throw new WorkflowProtocolError("WorkflowMismatch", `Workflow store belongs to ${owner}, not ${workflowOwnerId}`); }
  #workflowOwnerId(): string { const row = this.#database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1").get() as { owner_agent_id: string } | undefined; if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized"); return row.owner_agent_id; }
  #requireAgent(agentId: string): void { if (!this.#database.prepare("SELECT 1 AS present FROM workflow_agents WHERE agent_id = ?").get(agentId)) throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${agentId}`); }
  #withTransaction<T>(operation: () => T): T { this.#database.exec("BEGIN IMMEDIATE"); try { const result = operation(); this.#database.exec("COMMIT"); return result; } catch (error) { if (this.#database.isTransaction) this.#database.exec("ROLLBACK"); throw error; } }
}

function mapMessage(row: MessageRow): DirectSignalRecord {
  const answer = row.in_reply_to_request_id ? { kind: "answer" as const, inReplyToRequestId: row.in_reply_to_request_id } : row.response_required ? { kind: "request" as const } : { kind: "signal" as const };
  return { messageId: row.message_id, ...answer, senderAgentId: row.sender_agent_id, recipientAgentId: row.recipient_agent_id, sourceEntryId: row.source_entry_id, payloadDigest: row.payload_digest, deliveryTiming: row.delivery_timing, responseRequired: Number(row.response_required) === 1, ...(row.acceptance_sequence == null ? {} : { acceptanceSequence: Number(row.acceptance_sequence) }), deliveryStatus: row.delivery_status, createdAtMs: Number(row.created_at_ms), ...(row.accepted_at_ms == null ? {} : { acceptedAtMs: Number(row.accepted_at_ms) }), ...(row.delivered_at_ms == null ? {} : { deliveredAtMs: Number(row.delivered_at_ms) }) };
}
function mapPointer(row: PointerRow): PendingMessagePointer { return { messageId: row.message_id, senderAgentId: row.sender_agent_id, recipientAgentId: row.recipient_agent_id, sourceEntryId: row.source_entry_id, payloadDigest: row.payload_digest, deliveryTiming: row.delivery_timing, responseRequired: Number(row.response_required) === 1, reactivatesRecipient: Number(row.reactivates_recipient) === 1, ...(row.in_reply_to_request_id ? { inReplyToRequestId: row.in_reply_to_request_id } : {}), acceptanceSequence: Number(row.acceptance_sequence), acceptedAtMs: Number(row.accepted_at_ms) }; }
function mapRequest(row: RequestRow): RequestRecord { return { requestId: row.request_id, requesterAgentId: row.requester_agent_id, responderAgentId: row.responder_agent_id, answerDeliveryTiming: row.answer_delivery_timing, status: row.status, ...(row.answer_message_id ? { answerMessageId: row.answer_message_id } : {}) }; }
function receiptFor(row: MessageRow) { if (row.acceptance_sequence == null) throw new Error(`Unaccepted Message ${row.message_id} has no receipt`); return { status: row.delivery_status === "delivered" ? "delivered" as const : "queued" as const, messageId: row.message_id, recipientAgentId: row.recipient_agent_id, acceptanceSequence: Number(row.acceptance_sequence) }; }
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
