export interface DirectSignalMessage {
  kind: "signal";
  messageId: string;
  senderAgentId: string;
  recipientAgentId: string;
  message: string;
}

export interface InboxBatch {
  messages: [DirectSignalMessage];
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
  message: string;
}

export interface SignalReceiptReply {
  accepted: boolean;
  receipt?: QueuedSignalReceipt;
  error?: { code?: string; message: string };
}

export interface AcceptedSignal {
  receipt: QueuedSignalReceipt;
  delivery: "project";
  wakeRecipient: boolean;
}
