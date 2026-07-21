export type SignalDeliveryTiming = "steer" | "deferred";
export type ActionableMessageKind = "signal" | "request" | "answer";

export interface DirectSignalMessage {
  kind: ActionableMessageKind;
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  deliveryTiming: SignalDeliveryTiming;
  message: string;
  responseRequired?: true;
  inReplyToRequestId?: string;
}

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
  inReplyToRequestId?: string;
  acceptanceSequence?: number;
  deliveryStatus: "bound" | "queued" | "delivered";
  createdAtMs: number;
  acceptedAtMs?: number;
  deliveredAtMs?: number;
}

export interface RequestRecord {
  requestId: string;
  requesterAgentId: string;
  responderAgentId: string;
  answerDeliveryTiming: SignalDeliveryTiming;
  status: "open" | "answered" | "resolved";
  answerMessageId?: string;
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
  inReplyToRequestId?: string;
  message: string;
}

export interface SignalReceiptReply {
  accepted: boolean;
  receipt?: QueuedSignalReceipt;
  error?: { code?: string; message: string };
}

export interface AcceptedSignal {
  receipt: QueuedSignalReceipt;
  delivery: "schedule";
}
