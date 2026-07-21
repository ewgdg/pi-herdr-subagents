import { createHash, randomUUID } from "node:crypto";
import {
  CURRENT_IPC_VERSION,
  connectFramedIpc,
  type FramedIpcConnection,
} from "../coordination/framed-ipc.ts";
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
  SignalAcceptRequest,
  SignalReceiptReply,
} from "./direct-signal-types.ts";

export type {
  DirectSignalMessage,
  DirectSignalRecord,
  InboxBatch,
  PendingMessagePointer,
  QueuedSignalReceipt,
} from "./direct-signal-types.ts";

const ACCEPTANCE_TIMEOUT_MS = 5_000;
const SIGNAL_REQUEST_TYPE = "direct-signal.accept";
const SIGNAL_RECEIPT_TYPE = "direct-signal.receipt";

export interface DirectSignalRuntimeOptions {
  controlPlane: WorkflowControlPlane;
  ownership?: AgentRunOwnership;
  allocateMessageId?: () => string;
  projectInboxBatch?: (batch: InboxBatch) => void;
  wakeRecipient?: () => void;
  now?: () => number;
}

export class DirectSignalRuntime {
  readonly #controlPlane: WorkflowControlPlane;
  readonly #ownership: AgentRunOwnership | undefined;
  readonly #allocateMessageId: () => string;
  #projectInboxBatch: ((batch: InboxBatch) => void) | undefined;
  #wakeRecipient: (() => void) | undefined;
  readonly #now: () => number;
  readonly #store: DirectSignalStore;
  #router: RecipientInboxRouter | undefined;
  #routerStartup: Promise<void> | undefined;
  #closed = false;

  constructor(options: DirectSignalRuntimeOptions) {
    this.#controlPlane = options.controlPlane;
    this.#ownership = options.ownership;
    this.#allocateMessageId = options.allocateMessageId ?? randomUUID;
    this.#projectInboxBatch = options.projectInboxBatch;
    this.#wakeRecipient = options.wakeRecipient;
    this.#now = options.now ?? Date.now;
    this.#store = new DirectSignalStore(options.controlPlane.workflow.databasePath);
  }

  async start(): Promise<void> {
    this.#assertOpen();
    if (this.#router) return;
    if (this.#routerStartup) return this.#routerStartup;
    if (!this.#projectInboxBatch) {
      throw new Error("Recipient Inbox Router requires an Inbox Batch projector");
    }
    this.#routerStartup = (async () => {
      const router = new RecipientInboxRouter({
        workflowOwnerId: this.#controlPlane.workflow.ownerAgentId,
        recipient: this.#controlPlane.currentAgent,
        ownership: this.#ownership,
        databasePath: this.#controlPlane.workflow.databasePath,
        projectInboxBatch: this.#projectInboxBatch!,
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
    wakeRecipient?: () => void;
  }): void {
    this.#assertOpen();
    this.#projectInboxBatch = input.projectInboxBatch;
    this.#wakeRecipient = input.wakeRecipient;
    this.#router?.configureDelivery(input);
  }

  async sendSignal(input: {
    target: AgentReference;
    message: string;
    sourceEntryId: string;
  }): Promise<QueuedSignalReceipt> {
    this.#assertOpen();
    assertNonEmpty(input.message, "Signal message");
    assertNonEmpty(input.sourceEntryId, "Signal source entry ID");
    const sender = this.#controlPlane.currentAgent;
    this.#controlPlane.authorizeDirectTarget(sender, input.target);
    const route = this.#store.readRouter(input.target);
    if (!route) throw recipientUnreachable(input.target.agentId);

    const messageId = this.#allocateMessageId();
    const payloadDigest = digestPayload(input.message);
    this.#store.bindSignal({
      messageId,
      sender,
      recipient: input.target,
      sourceEntryId: input.sourceEntryId,
      payloadDigest,
      createdAtMs: this.#now(),
    });

    let connection: FramedIpcConnection;
    try {
      connection = await connectFramedIpc(route.endpoint);
    } catch (error) {
      this.#store.discardUnacceptedSignal(sender, messageId);
      throw recipientUnreachable(input.target.agentId, error);
    }

    try {
      const receiptWaiter = createReceiptWaiter(connection);
      try {
        await connection.send({
          version: CURRENT_IPC_VERSION,
          type: SIGNAL_REQUEST_TYPE,
          payload: {
            workflowOwnerId: sender.workflowOwnerId,
            messageId,
            senderAgentId: sender.agentId,
            recipientAgentId: input.target.agentId,
            sourceEntryId: input.sourceEntryId,
            payloadDigest,
            message: input.message,
          } satisfies SignalAcceptRequest,
        });
      } catch (error) {
        receiptWaiter.cancel();
        this.#store.discardUnacceptedSignal(sender, messageId);
        throw recipientUnreachable(input.target.agentId, error);
      }
      let result: SignalReceiptReply | undefined;
      try {
        result = await receiptWaiter.promise;
      } catch (error) {
        if (this.#store.discardUnacceptedSignal(sender, messageId)) {
          throw recipientUnreachable(input.target.agentId, error);
        }
        throw error;
      }
      if (!result) throw new Error("Signal receipt wait was cancelled unexpectedly");
      if (!result.accepted || !result.receipt) {
        this.#store.discardUnacceptedSignal(sender, messageId);
        throw replyError(result.error);
      }
      return result.receipt;
    } finally {
      connection.end();
    }
  }

  inspectMessage(messageId: string): DirectSignalRecord | undefined {
    this.#assertOpen();
    return this.#store.inspectMessage(this.#controlPlane.workflow.ownerAgentId, messageId);
  }

  listMessages(): DirectSignalRecord[] {
    this.#assertOpen();
    return this.#store.listMessages(this.#controlPlane.workflow.ownerAgentId);
  }

  listPending(recipient: AgentReference): PendingMessagePointer[] {
    this.#assertOpen();
    return this.#store.listPending(recipient);
  }

  confirmDelivery(messageId: string): boolean {
    this.#assertOpen();
    if (!this.#router) return false;
    return this.#router.confirmDelivery(messageId);
  }

  close(): void | Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (!this.#router) {
      this.#store.close();
      return;
    }
    const router = this.#router;
    this.#router = undefined;
    return router.close().finally(() => this.#store.close());
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Direct Signal runtime is closed");
  }
}

function createReceiptWaiter(connection: FramedIpcConnection): {
  promise: Promise<SignalReceiptReply | undefined>;
  cancel(): void;
} {
  let resolvePromise!: (receipt: SignalReceiptReply | undefined) => void;
  let rejectPromise!: (error: Error) => void;
  let settled = false;
  let deferMessageUnsubscribe = false;
  let deferErrorUnsubscribe = false;
  let unsubscribeMessage = () => {
    deferMessageUnsubscribe = true;
  };
  let unsubscribeError = () => {
    deferErrorUnsubscribe = true;
  };
  const promise = new Promise<SignalReceiptReply | undefined>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  const timeout = setTimeout(() => {
    rejectOnce(new Error("Timed out waiting for durable Signal acceptance receipt"));
  }, ACCEPTANCE_TIMEOUT_MS);

  const cleanup = () => {
    clearTimeout(timeout);
    unsubscribeMessage();
    unsubscribeError();
  };
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
    if (frame.type === SIGNAL_RECEIPT_TYPE) {
      resolveOnce(frame.payload as SignalReceiptReply);
    }
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

function replyError(error: SignalReceiptReply["error"]): Error {
  if (!error) return new Error("Recipient Inbox Router rejected Signal acceptance");
  if (isWorkflowProtocolErrorCode(error.code)) {
    return new WorkflowProtocolError(error.code, error.message);
  }
  return new Error(error.message);
}

function isWorkflowProtocolErrorCode(
  code: string | undefined,
): code is ConstructorParameters<typeof WorkflowProtocolError>[0] {
  return code === "WorkflowMismatch"
    || code === "UnknownAgent"
    || code === "OwnershipLost"
    || code === "RecipientUnreachable"
    || code === "RecipientEnded"
    || code === "MessageIdentityConflict"
    || code === "InvalidMessageSource";
}

function recipientUnreachable(agentId: string, cause?: unknown): WorkflowProtocolError {
  const detail = cause instanceof Error ? `: ${cause.message}` : "";
  return new WorkflowProtocolError(
    "RecipientUnreachable",
    `Recipient Inbox Router is unavailable for Agent ${agentId}${detail}`,
  );
}

function digestPayload(message: string): string {
  return createHash("sha256").update(message, "utf8").digest("hex");
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new Error(`${label} must not be empty`);
}
