import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import type { RequestCancellationReceipt } from "./direct-signal-types.ts";

interface OpenRequestRow {
  request_id: string;
  requester_agent_id: string;
  responder_agent_id: string;
  requester_activation_id: string | null;
  message_id: string | null;
  delivery_status: "bound" | "accepted" | "delivered" | "suppressed" | null;
  projection_claimed: number | null;
  projection_committed: number | null;
  in_reply_to_request_id: string | null;
}

/**
 * Transform one exact open Request while the caller owns a SQLite write
 * transaction. Both public Request cancellation and activation finalization
 * use this primitive so projection arbitration cannot diverge.
 */
export function cancelOpenRequestInTransaction(
  database: DatabaseSync,
  input: {
    requestId: string;
    requesterAgentId: string;
    requesterActivationId?: string;
    noticeMessageId: string;
    cancelledAtMs: number;
  },
): RequestCancellationReceipt {
  if (!database.isTransaction) throw new Error("Request cancellation transition requires an active SQLite transaction");
  const activationPredicate = input.requesterActivationId
    ? "AND request.requester_activation_id = ?"
    : "";
  const parameters = input.requesterActivationId
    ? [input.requestId, input.requesterAgentId, input.requesterActivationId]
    : [input.requestId, input.requesterAgentId];
  const request = database.prepare(`SELECT request.request_id, request.requester_agent_id,
      request.responder_agent_id, request.requester_activation_id,
      message.message_id, message.delivery_status, message.projection_claimed,
      message.projection_committed, message.in_reply_to_request_id
    FROM workflow_requests request
    LEFT JOIN direct_signal_messages message
      ON message.message_id = request.request_id
      AND message.sender_agent_id = request.requester_agent_id
      AND message.recipient_agent_id = request.responder_agent_id
      AND message.response_required = 1
    WHERE request.request_id = ? AND request.status = 'open'
      AND request.requester_agent_id = ? ${activationPredicate}`
  ).get(...parameters) as OpenRequestRow | undefined;
  if (!request) throw new Error(`Open Request ${input.requestId} lost cancellation arbitration`);
  if (!request.message_id || request.delivery_status === "bound") {
    throw new Error(`Open Request ${input.requestId} has no accepted Request message`);
  }

  const updateActivationPredicate = input.requesterActivationId
    ? "AND requester_activation_id = ?"
    : "";
  const updateParameters = input.requesterActivationId
    ? [input.cancelledAtMs, input.requestId, input.requesterAgentId, input.requesterActivationId]
    : [input.cancelledAtMs, input.requestId, input.requesterAgentId];
  if (request.delivery_status === "accepted"
    && Number(request.projection_claimed) === 0
    && Number(request.projection_committed) === 0
    && request.in_reply_to_request_id === null) {
    const removed = database.prepare(`DELETE FROM pending_message_pointers
      WHERE message_id = ? AND recipient_agent_id = ?`
    ).run(input.requestId, request.responder_agent_id);
    if (Number(removed.changes) !== 1) throw new Error(`Pending pointer is missing for Request ${input.requestId}`);
    const suppressed = database.prepare(`UPDATE direct_signal_messages SET delivery_status = 'suppressed'
      WHERE message_id = ? AND delivery_status = 'accepted'
        AND projection_claimed = 0 AND projection_committed = 0`
    ).run(input.requestId);
    if (Number(suppressed.changes) !== 1) throw new Error(`Request ${input.requestId} could not be suppressed`);
    const cancelled = database.prepare(`UPDATE workflow_requests
      SET status = 'cancelled', cancelled_at_ms = ?
      WHERE request_id = ? AND status = 'open' AND requester_agent_id = ?
        ${updateActivationPredicate}`
    ).run(...updateParameters);
    if (Number(cancelled.changes) !== 1) throw new Error(`Request ${input.requestId} cancellation lost arbitration`);
    return { requestId: input.requestId, status: "cancelled", delivery: "suppressed" };
  }

  const acceptedMustBePreserved = request.delivery_status === "accepted"
    && (Number(request.projection_claimed) === 1 || request.in_reply_to_request_id !== null);
  if (request.delivery_status !== "delivered" && !acceptedMustBePreserved) {
    throw new Error(`Open Request ${input.requestId} has invalid delivery state ${request.delivery_status}`);
  }
  if (acceptedMustBePreserved && !database.prepare(
    "SELECT 1 FROM pending_message_pointers WHERE message_id = ? AND recipient_agent_id = ?",
  ).get(input.requestId, request.responder_agent_id)) {
    throw new Error(`Pending pointer is missing for projected Request ${input.requestId}`);
  }
  if (request.delivery_status === "delivered" && database.prepare(
    "SELECT 1 FROM pending_message_pointers WHERE message_id = ?",
  ).get(input.requestId)) {
    throw new Error(`Delivered Request ${input.requestId} still has a pending pointer`);
  }

  const noticePayload = requestCancellationNoticePayload(input.requestId, input.requesterAgentId);
  const noticeDigest = createHash("sha256").update(noticePayload, "utf8").digest("hex");
  const sequence = nextAcceptanceSequence(database, request.responder_agent_id);
  // The Request owns canonical notice content. Message rows only provide
  // ordering and delivery; projection deliberately omits sender attribution.
  const notice = database.prepare(`INSERT INTO direct_signal_messages (
    message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest,
    delivery_timing, response_required, in_reply_to_request_id, acceptance_sequence,
    delivery_status, created_at_ms, accepted_at_ms, delivered_at_ms,
    protocol_notice_kind, canonical_request_id
  ) VALUES (?, ?, ?, ?, ?, 'steer', 0, NULL, ?, 'accepted', ?, ?, NULL,
    'request-cancelled', ?)`
  ).run(
    input.noticeMessageId,
    input.requesterAgentId,
    request.responder_agent_id,
    input.noticeMessageId,
    noticeDigest,
    sequence,
    input.cancelledAtMs,
    input.cancelledAtMs,
    input.requestId,
  );
  if (Number(notice.changes) !== 1) throw new Error(`Cancellation notice was not created for Request ${input.requestId}`);
  const pointer = database.prepare(`INSERT INTO pending_message_pointers (
    message_id, sender_agent_id, recipient_agent_id, source_entry_id, payload_digest,
    delivery_timing, response_required, reactivates_recipient, in_reply_to_request_id,
    acceptance_sequence, accepted_at_ms, protocol_notice_kind, canonical_request_id
  ) VALUES (?, ?, ?, ?, ?, 'steer', 0, 0, NULL, ?, ?, 'request-cancelled', ?)`
  ).run(
    input.noticeMessageId,
    input.requesterAgentId,
    request.responder_agent_id,
    input.noticeMessageId,
    noticeDigest,
    sequence,
    input.cancelledAtMs,
    input.requestId,
  );
  if (Number(pointer.changes) !== 1) throw new Error(`Cancellation notice pointer was not created for Request ${input.requestId}`);
  const noticeUpdateParameters = input.requesterActivationId
    ? [input.cancelledAtMs, input.noticeMessageId, noticePayload, input.requestId, input.requesterAgentId, input.requesterActivationId]
    : [input.cancelledAtMs, input.noticeMessageId, noticePayload, input.requestId, input.requesterAgentId];
  const cancelled = database.prepare(`UPDATE workflow_requests
    SET status = 'cancelled', cancelled_at_ms = ?, cancellation_notice_message_id = ?,
      cancellation_notice_payload = ?, cancellation_notice_delivery_status = 'accepted'
    WHERE request_id = ? AND status = 'open' AND requester_agent_id = ?
      ${updateActivationPredicate}`
  ).run(...noticeUpdateParameters);
  if (Number(cancelled.changes) !== 1) throw new Error(`Request ${input.requestId} cancellation lost arbitration`);
  return {
    requestId: input.requestId,
    status: "cancelled",
    delivery: "notice-accepted",
    noticeMessageId: input.noticeMessageId,
  };
}

function requestCancellationNoticePayload(requestId: string, requesterAgentId: string): string {
  return [
    `Request ${requestId} was cancelled by requester Agent ${requesterAgentId}.`,
    "Stop work for this Request if possible and do not send an Answer.",
    "Cancellation does not roll back completed work or external side effects.",
  ].join("\n");
}

function nextAcceptanceSequence(database: DatabaseSync, agentId: string): number {
  const row = database.prepare("SELECT last_sequence FROM recipient_acceptance_counters WHERE agent_id = ?")
    .get(agentId) as { last_sequence: number } | undefined;
  const next = Number(row?.last_sequence ?? 0) + 1;
  const updated = database.prepare(`INSERT INTO recipient_acceptance_counters (agent_id, last_sequence)
    VALUES (?, ?) ON CONFLICT (agent_id) DO UPDATE SET last_sequence = excluded.last_sequence`
  ).run(agentId, next);
  if (Number(updated.changes) !== 1) throw new Error(`Acceptance sequence was not advanced for Agent ${agentId}`);
  return next;
}
