export type SignalDeliveryTiming = "steer" | "deferred";

export interface DirectSignalMessage {
  kind: "signal";
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  deliveryTiming: SignalDeliveryTiming;
  message: string;
}

export interface InboxBatch {
  deliveryTiming: SignalDeliveryTiming;
  messages: DirectSignalMessage[];
}

export interface QueuedSignalReceipt {
  status: "queued";
  messageId: string;
  recipientAgentId: string;
  acceptanceSequence: number;
}

export interface DirectSignalRecord {
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  sourceEntryId: string;
  payloadDigest: string;
  deliveryTiming: SignalDeliveryTiming;
  acceptanceSequence?: number;
  deliveryStatus: "bound" | "queued" | "delivered";
  createdAtMs: number;
  acceptedAtMs?: number;
  deliveredAtMs?: number;
}

export interface PendingMessagePointer {
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  sourceEntryId: string;
  payloadDigest: string;
  deliveryTiming: SignalDeliveryTiming;
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
