export type SignalDeliveryTiming = "steer" | "deferred";
export type AgentMessageKind = "signal" | "request" | "answer";
export type ActionableMessageKind = AgentMessageKind | "protocol-notice";

export interface AgentDirectSignalMessage {
  kind: AgentMessageKind;
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  deliveryTiming: SignalDeliveryTiming;
  message: string;
  responseRequired?: true;
  inReplyToRequestId?: string;
}

export interface ProtocolNoticeMessage {
  kind: "protocol-notice";
  noticeKind: "request-cancelled" | "request-orphaned";
  messageId: string;
  requestId: string;
  recipientAgentId: string;
  deliveryTiming: "steer";
  message: string;
}

export type DirectSignalMessage = AgentDirectSignalMessage | ProtocolNoticeMessage;

export interface InboxBatch {
  deliveryTiming: SignalDeliveryTiming;
  messages: DirectSignalMessage[];
}

export interface QueuedSignalReceipt {
  /** Initial spawned Requests are already represented in the child JSONL. */
  status: "queued" | "delivered";
  messageId: string;
  recipientAgentId: string;
  acceptanceSequence: number;
}

export interface DirectSignalRecord {
  messageId: string;
  kind: ActionableMessageKind;
  senderAgentId: string;
  recipientAgentId: string;
  sourceEntryId: string;
  payloadDigest: string;
  deliveryTiming: SignalDeliveryTiming;
  responseRequired: boolean;
  onAccepted: "continue" | "complete";
  inReplyToRequestId?: string;
  acceptanceSequence?: number;
  deliveryStatus: "bound" | "queued" | "delivered" | "suppressed";
  protocolNoticeKind?: "request-cancelled" | "request-orphaned";
  canonicalRequestId?: string;
  createdAtMs: number;
  acceptedAtMs?: number;
  deliveredAtMs?: number;
}

export interface RequestRecord {
  requestId: string;
  requesterAgentId: string;
  responderAgentId: string;
  answerDeliveryTiming: SignalDeliveryTiming;
  status: "open" | "answered" | "resolved" | "cancelled" | "orphaned";
  requesterActivationId?: string;
  responderActivationId?: string;
  answerMessageId?: string;
  cancelledAtMs?: number;
  cancellationNotice?: {
    messageId: string;
    message: string;
    deliveryStatus: "queued" | "delivered";
    deliveredAtMs?: number;
  };
  orphanedAtMs?: number;
  orphanNotice?: {
    messageId: string;
    message: string;
    deliveryStatus: "queued" | "delivered";
    deliveredAtMs?: number;
  };
}

export interface RequestCancellationReceipt {
  requestId: string;
  status: "cancelled";
  delivery: "suppressed" | "notice-queued" | "notice-delivered";
  noticeMessageId?: string;
}

export interface PendingMessagePointer {
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  sourceEntryId: string;
  payloadDigest: string;
  deliveryTiming: SignalDeliveryTiming;
  responseRequired: boolean;
  /** An authorized Request that reopened an interrupted or ended recipient. */
  reactivatesRecipient: boolean;
  inReplyToRequestId?: string;
  acceptanceSequence: number;
  acceptedAtMs: number;
  protocolNoticeKind?: "request-cancelled" | "request-orphaned";
  canonicalRequestId?: string;
  /** Durable evidence that transcript projection may already have occurred. */
  projectionClaimed: boolean;
  projectionCommitted: boolean;
}

export interface SignalAcceptRequest {
  workflowOwnerId: string;
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  sourceEntryId: string;
  payloadDigest: string;
  deliveryTiming: SignalDeliveryTiming;
  responseRequired: boolean;
  onAccepted?: "continue" | "complete";
  inReplyToRequestId?: string;
  message: string;
  completion?: {
    ownership: import("./workflow-types.ts").AgentRunOwnership;
  };
}

export interface SignalReceiptReply {
  accepted: boolean;
  receipt?: QueuedSignalReceipt;
  error?: { code?: string; message: string; blockers?: import("./completion-gate.ts").CompletionBlocker[] };
}

export interface AcceptedSignal {
  receipt: QueuedSignalReceipt;
  delivery: "schedule";
}
