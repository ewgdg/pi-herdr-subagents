import { randomUUID } from "node:crypto";
import {
  CURRENT_IPC_VERSION,
  connectFramedIpc,
  type FramedIpcConnection,
} from "../coordination/framed-ipc.ts";
import { digestPayload } from "./direct-signal-transcript.ts";
import type { WorkflowControlPlane } from "./workflow-control-plane.ts";
import { RecipientInboxRouter } from "./recipient-inbox-router.ts";
import { DirectSignalStore } from "./sqlite-message-store.ts";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
import type {
  DirectSignalRecord,
  InboxBatch,
  PendingMessagePointer,
  QueuedSignalReceipt,
  RequestRecord,
  SignalAcceptRequest,
  SignalDeliveryTiming,
  SignalReceiptReply,
} from "./direct-signal-types.ts";

export type {
  ActionableMessageKind,
  DirectSignalMessage,
  DirectSignalRecord,
  InboxBatch,
  PendingMessagePointer,
  QueuedSignalReceipt,
  RequestRecord,
  SignalDeliveryTiming,
} from "./direct-signal-types.ts";

const ACCEPTANCE_TIMEOUT_MS = 5_000;
const SIGNAL_REQUEST_TYPE = "direct-signal.accept";
const SIGNAL_RECEIPT_TYPE = "direct-signal.receipt";

export interface DirectSignalRuntimeOptions {
  controlPlane: WorkflowControlPlane;
  ownership?: AgentRunOwnership;
  allocateMessageId?: () => string;
  projectInboxBatch?: (batch: InboxBatch) => void;
  hasProjectedMessage?: (messageId: string) => boolean;
  wakeRecipient?: () => void;
  now?: () => number;
}

type MessageTarget = { agentId: string; workflowOwnerId?: string } | { requestId: string };

export class DirectSignalRuntime {
  readonly #controlPlane: WorkflowControlPlane;
  readonly #ownership: AgentRunOwnership | undefined;
  readonly #allocateMessageId: () => string;
  #projectInboxBatch: ((batch: InboxBatch) => void) | undefined;
  #hasProjectedMessage: ((messageId: string) => boolean) | undefined;
  #wakeRecipient: (() => void) | undefined;
  readonly #now: () => number;
  readonly #store: DirectSignalStore;
  #router: RecipientInboxRouter | undefined;
  #routerStartup: Promise<void> | undefined;
  #closePromise: Promise<void> | undefined;
  #closed = false;

  constructor(options: DirectSignalRuntimeOptions) {
    this.#controlPlane = options.controlPlane;
    this.#ownership = options.ownership;
    this.#allocateMessageId = options.allocateMessageId ?? randomUUID;
    this.#projectInboxBatch = options.projectInboxBatch;
    this.#hasProjectedMessage = options.hasProjectedMessage;
    this.#wakeRecipient = options.wakeRecipient;
    this.#now = options.now ?? Date.now;
    this.#store = new DirectSignalStore(options.controlPlane.workflow.databasePath);
  }

  async start(): Promise<void> {
    this.#assertOpen();
    if (this.#router) return;
    if (this.#routerStartup) return this.#routerStartup;
    if (!this.#projectInboxBatch) throw new Error("Recipient Inbox Router requires an Inbox Batch projector");
    this.#routerStartup = (async () => {
      const router = new RecipientInboxRouter({
        workflowOwnerId: this.#controlPlane.workflow.ownerAgentId,
        recipient: this.#controlPlane.currentAgent,
        ownership: this.#ownership,
        databasePath: this.#controlPlane.workflow.databasePath,
        projectInboxBatch: this.#projectInboxBatch!,
        hasProjectedMessage: this.#hasProjectedMessage,
        wakeRecipient: this.#wakeRecipient,
        now: this.#now,
      });
      await router.start();
      if (this.#closed) {
        await router.close();
        return;
      }
      this.#router = router;
    })();
    try {
      await this.#routerStartup;
    } finally {
      this.#routerStartup = undefined;
    }
  }

  configureInboxDelivery(input: {
    projectInboxBatch(batch: InboxBatch): void;
    hasProjectedMessage?(messageId: string): boolean;
    wakeRecipient?: () => void;
  }): void {
    this.#assertOpen();
    this.#projectInboxBatch = input.projectInboxBatch;
    this.#hasProjectedMessage = input.hasProjectedMessage;
    this.#wakeRecipient = input.wakeRecipient;
    this.#router?.configureDelivery(input);
  }

  /** Send a Signal, Request, Answer, or Answer-and-Request. */
  async sendMessage(input: {
    target: MessageTarget;
    message: string;
    sourceEntryId: string;
    deliveryTiming?: SignalDeliveryTiming;
    responseRequired?: boolean;
  }): Promise<QueuedSignalReceipt> {
    this.#assertOpen();
    assertNonEmpty(input.message, "Message");
    assertNonEmpty(input.sourceEntryId, "Message source entry ID");
    const sender = this.#controlPlane.currentAgent;
    const responseRequired = input.responseRequired === true;
    const target = this.#resolveTarget(sender, input.target, input.deliveryTiming);
    const payloadDigest = digestPayload(input.message);
    const existing = this.#store.findMessageBySource({
      sender,
      recipient: target.recipient,
      sourceEntryId: input.sourceEntryId,
      payloadDigest,
      deliveryTiming: target.deliveryTiming,
      responseRequired,
      inReplyToRequestId: target.inReplyToRequestId,
    });
    if (existing && existing.deliveryStatus !== "bound") return receiptFor(existing);
    if (target.inReplyToRequestId && this.#store.inspectRequest(sender.workflowOwnerId, target.inReplyToRequestId)?.status !== "open") {
      throw new WorkflowProtocolError("AnswerAlreadyClosed", `Request ${target.inReplyToRequestId} already has a terminal Answer`);
    }

    const route = this.#store.readRouter(target.recipient);
    if (!route) throw recipientUnreachable(target.recipient.agentId);
    const bound = existing ?? this.#store.bindMessage({
      messageId: this.#allocateMessageId(),
      sender,
      recipient: target.recipient,
      sourceEntryId: input.sourceEntryId,
      payloadDigest,
      deliveryTiming: target.deliveryTiming,
      responseRequired,
      inReplyToRequestId: target.inReplyToRequestId,
      createdAtMs: this.#now(),
    });
    if (bound.deliveryStatus !== "bound") return receiptFor(bound);
    const request: SignalAcceptRequest = {
      workflowOwnerId: sender.workflowOwnerId,
      messageId: bound.messageId,
      senderAgentId: sender.agentId,
      recipientAgentId: target.recipient.agentId,
      sourceEntryId: input.sourceEntryId,
      payloadDigest,
      deliveryTiming: target.deliveryTiming,
      responseRequired,
      ...(target.inReplyToRequestId ? { inReplyToRequestId: target.inReplyToRequestId } : {}),
      message: input.message,
    };

    let reply: SignalReceiptReply;
    try {
      reply = await this.#requestReceipt(route.endpoint, request);
    } catch (error) {
      const reconciled = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
      if (reconciled && reconciled.deliveryStatus !== "bound") return receiptFor(reconciled);
      this.#store.discardUnacceptedMessage(sender, bound.messageId);
      throw recipientUnreachable(target.recipient.agentId, error);
    }
    if (!reply.accepted || !reply.receipt) {
      this.#store.discardUnacceptedMessage(sender, bound.messageId);
      throw replyError(reply.error);
    }
    return reply.receipt;
  }

  /** Compatibility-shaped convenience for ordinary Signals. */
  sendSignal(input: {
    target: AgentReference;
    message: string;
    sourceEntryId: string;
    deliveryTiming?: SignalDeliveryTiming;
  }): Promise<QueuedSignalReceipt> {
    return this.sendMessage({
      target: { agentId: input.target.agentId, workflowOwnerId: input.target.workflowOwnerId },
      message: input.message,
      sourceEntryId: input.sourceEntryId,
      deliveryTiming: input.deliveryTiming,
    });
  }

  inspectMessage(messageId: string): DirectSignalRecord | undefined {
    this.#assertOpen();
    return this.#store.inspectMessage(this.#controlPlane.workflow.ownerAgentId, messageId);
  }

  listMessages(): DirectSignalRecord[] {
    this.#assertOpen();
    return this.#store.listMessages(this.#controlPlane.workflow.ownerAgentId);
  }

  inspectRequest(requestId: string): RequestRecord | undefined {
    this.#assertOpen();
    return this.#store.inspectRequest(this.#controlPlane.workflow.ownerAgentId, requestId);
  }

  listRequests(requester: AgentReference): RequestRecord[] {
    this.#assertOpen();
    return this.#store.listRequests(requester);
  }

  listPending(recipient: AgentReference): PendingMessagePointer[] {
    this.#assertOpen();
    return this.#store.listPending(recipient);
  }

  confirmDelivery(messageId: string): boolean {
    this.#assertOpen();
    return this.#router?.confirmDelivery(messageId) ?? false;
  }

  releaseDeferred(): void {
    this.#assertOpen();
    this.#router?.releaseDeferred();
  }

  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    this.#closePromise = this.#close();
    return this.#closePromise;
  }

  async #close(): Promise<void> {
    // Router startup reports its own failure; closing must still release the
    // store when that startup raced with shutdown.
    await this.#routerStartup?.catch(() => undefined);
    const router = this.#router;
    this.#router = undefined;
    try {
      await router?.close();
    } finally {
      this.#store.close();
    }
  }

  #resolveTarget(
    sender: AgentReference,
    target: MessageTarget,
    deliveryTiming: SignalDeliveryTiming | undefined,
  ): { recipient: AgentReference; deliveryTiming: SignalDeliveryTiming; inReplyToRequestId?: string } {
    if ("agentId" in target) {
      const recipient = target.workflowOwnerId
        ? { workflowOwnerId: target.workflowOwnerId, agentId: target.agentId }
        : this.#controlPlane.agent(target.agentId);
      this.#controlPlane.authorizeDirectTarget(sender, recipient);
      return { recipient, deliveryTiming: deliveryTiming ?? "steer" };
    }
    if (deliveryTiming !== undefined) {
      throw new WorkflowProtocolError(
        "InvalidMessageSource",
        "Request-targeted messages derive delivery timing from the referenced Request",
      );
    }
    const request = this.#store.requireAnswerTarget(sender, target.requestId, true);
    return {
      recipient: this.#controlPlane.agent(request.requesterAgentId),
      deliveryTiming: request.answerDeliveryTiming,
      inReplyToRequestId: request.requestId,
    };
  }

  async #requestReceipt(endpoint: string, request: SignalAcceptRequest): Promise<SignalReceiptReply> {
    let connection: FramedIpcConnection;
    try {
      connection = await connectFramedIpc(endpoint);
    } catch (error) {
      throw recipientUnreachable(request.recipientAgentId, error);
    }
    try {
      const receiptWaiter = createReceiptWaiter(connection);
      try {
        await connection.send({ version: CURRENT_IPC_VERSION, type: SIGNAL_REQUEST_TYPE, payload: request });
      } catch (error) {
        receiptWaiter.cancel();
        throw error;
      }
      const reply = await receiptWaiter.promise;
      if (!reply) throw new Error("Signal receipt wait was cancelled unexpectedly");
      return reply;
    } finally {
      connection.end();
    }
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Direct Signal runtime is closed");
  }
}

function createReceiptWaiter(connection: FramedIpcConnection): { promise: Promise<SignalReceiptReply | undefined>; cancel(): void } {
  let resolvePromise!: (receipt: SignalReceiptReply | undefined) => void;
  let rejectPromise!: (error: Error) => void;
  let settled = false;
  let deferMessageUnsubscribe = false;
  let deferErrorUnsubscribe = false;
  let unsubscribeMessage = () => { deferMessageUnsubscribe = true; };
  let unsubscribeError = () => { deferErrorUnsubscribe = true; };
  const promise = new Promise<SignalReceiptReply | undefined>((resolve, reject) => { resolvePromise = resolve; rejectPromise = reject; });
  const timeout = setTimeout(() => rejectOnce(new Error("Timed out waiting for durable Signal acceptance receipt")), ACCEPTANCE_TIMEOUT_MS);
  const cleanup = () => { clearTimeout(timeout); unsubscribeMessage(); unsubscribeError(); };
  const resolveOnce = (receipt: SignalReceiptReply | undefined) => {
    if (settled) return;
    settled = true;
    cleanup();
    resolvePromise(receipt);
  };
  function rejectOnce(error: Error): void {
    if (settled) return;
    settled = true;
    cleanup();
    rejectPromise(error);
  }
  const removeMessageListener = connection.onMessage((frame) => {
    if (frame.type === SIGNAL_RECEIPT_TYPE) resolveOnce(frame.payload as SignalReceiptReply);
  });
  unsubscribeMessage = removeMessageListener;
  if (deferMessageUnsubscribe) removeMessageListener();
  if (!settled) {
    const removeErrorListener = connection.onError(rejectOnce);
    unsubscribeError = removeErrorListener;
    if (deferErrorUnsubscribe) removeErrorListener();
  }
  void connection.closed.then((result) => {
    if (result.kind === "failed") rejectOnce(result.error);
    else rejectOnce(new Error("Recipient closed before returning a Signal receipt"));
  });
  return { promise, cancel: () => resolveOnce(undefined) };
}

function receiptFor(record: DirectSignalRecord): QueuedSignalReceipt {
  if (record.acceptanceSequence === undefined) throw new Error(`Message ${record.messageId} is not durably accepted`);
  return { status: "queued", messageId: record.messageId, recipientAgentId: record.recipientAgentId, acceptanceSequence: record.acceptanceSequence };
}

function replyError(error: SignalReceiptReply["error"]): Error {
  if (!error) return new Error("Recipient Inbox Router rejected message acceptance");
  if (isWorkflowProtocolErrorCode(error.code)) return new WorkflowProtocolError(error.code, error.message);
  return new Error(error.message);
}

function isWorkflowProtocolErrorCode(code: string | undefined): code is ConstructorParameters<typeof WorkflowProtocolError>[0] {
  return code === "WorkflowMismatch" || code === "UnknownAgent" || code === "OwnershipLost"
    || code === "RecipientUnreachable" || code === "RecipientEnded" || code === "MessageIdentityConflict"
    || code === "InvalidMessageSource" || code === "AnswerUnauthorized" || code === "AnswerAlreadyClosed" || code === "UnknownRequest";
}

function recipientUnreachable(agentId: string, cause?: unknown): WorkflowProtocolError {
  const detail = cause instanceof Error ? `: ${cause.message}` : "";
  return new WorkflowProtocolError("RecipientUnreachable", `Recipient Inbox Router is unavailable for Agent ${agentId}${detail}`);
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new Error(`${label} must not be empty`);
}
