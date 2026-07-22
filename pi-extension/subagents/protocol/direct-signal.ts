import { randomUUID } from "node:crypto";
import {
  CURRENT_IPC_VERSION,
  connectFramedIpc,
  type FramedIpcConnection,
} from "../coordination/framed-ipc.ts";
import { digestPayload, resolveCanonicalSignal } from "./direct-signal-transcript.ts";
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
  RequestCancellationReceipt,
  RequestRecord,
  SignalAcceptRequest,
  SignalDeliveryTiming,
  SignalReceiptReply,
} from "./direct-signal-types.ts";
import { CompletionRejectedError } from "./completion-gate.ts";

export type {
  ActionableMessageKind,
  DirectSignalMessage,
  DirectSignalRecord,
  InboxBatch,
  PendingMessagePointer,
  QueuedSignalReceipt,
  RequestRecord,
  RequestCancellationReceipt,
  SignalDeliveryTiming,
} from "./direct-signal-types.ts";

const ACCEPTANCE_TIMEOUT_MS = 5_000;
const ACCEPTANCE_RETRY_INITIAL_DELAY_MS = 50;
const ACCEPTANCE_RETRY_MAXIMUM_DELAY_MS = 5_000;
const ACCEPTANCE_RETRY_BACKOFF_FACTOR = 2;
const SIGNAL_REQUEST_TYPE = "direct-signal.accept";
const SIGNAL_RECEIPT_TYPE = "direct-signal.receipt";
const INBOX_SCHEDULE_TYPE = "recipient-inbox.schedule";

export interface DirectSignalRuntimeOptions {
  controlPlane: WorkflowControlPlane;
  ownership?: AgentRunOwnership;
  allocateMessageId?: () => string;
  projectInboxBatch?: (batch: InboxBatch) => void;
  hasProjectedMessage?: (messageId: string) => boolean;
  wakeRecipient?: () => void;
  now?: () => number;
  preparedRouter?: RecipientInboxRouter;
  preparedRouterCommitted?: boolean;
}

type MessageTarget = { agentId: string; workflowOwnerId?: string } | { requestId: string };
type AcceptanceReconciliationResult = { terminalCompletion: boolean };
type AcceptanceResolutionWaiter = {
  resolve(result: AcceptanceReconciliationResult): void;
  reject(error: Error): void;
};

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
  readonly #preparedRouter: RecipientInboxRouter | undefined;
  readonly #preparedRouterCommitted: boolean;
  #routerStartup: Promise<void> | undefined;
  #closePromise: Promise<void> | undefined;
  #closed = false;
  readonly #acceptanceReconciliations = new Map<string, Promise<QueuedSignalReceipt | undefined>>();
  #acceptanceRetryTimer: ReturnType<typeof setTimeout> | undefined;
  #acceptanceRetryDelayMs = ACCEPTANCE_RETRY_INITIAL_DELAY_MS;
  readonly #acceptanceResolutionWaiters = new Set<AcceptanceResolutionWaiter>();
  #onTerminalCompletion: (() => void) | undefined;

  constructor(options: DirectSignalRuntimeOptions) {
    this.#controlPlane = options.controlPlane;
    this.#ownership = options.ownership;
    this.#allocateMessageId = options.allocateMessageId ?? randomUUID;
    this.#projectInboxBatch = options.projectInboxBatch;
    this.#hasProjectedMessage = options.hasProjectedMessage;
    this.#wakeRecipient = options.wakeRecipient;
    this.#now = options.now ?? Date.now;
    this.#preparedRouter = options.preparedRouter;
    this.#preparedRouterCommitted = options.preparedRouterCommitted === true;
    this.#store = new DirectSignalStore(options.controlPlane.workflow.databasePath);
  }

  async start(): Promise<void> {
    this.#assertOpen();
    if (this.#router) return;
    if (this.#routerStartup) return this.#routerStartup;
    if (!this.#projectInboxBatch) throw new Error("Recipient Inbox Router requires an Inbox Batch projector");
    this.#routerStartup = (async () => {
      if (this.#preparedRouter) {
        this.#preparedRouter.configureDelivery({
          projectInboxBatch: this.#projectInboxBatch!,
          hasProjectedMessage: this.#hasProjectedMessage,
          wakeRecipient: this.#wakeRecipient,
        });
        if (!this.#ownership) throw new Error("Prepared Recipient Inbox Router requires Agent Run ownership");
        this.#preparedRouter.activatePrepared(this.#ownership, !this.#preparedRouterCommitted);
        this.#router = this.#preparedRouter;
        return;
      }
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
    onTerminalCompletion?: () => void;
  }): void {
    this.#assertOpen();
    this.#projectInboxBatch = input.projectInboxBatch;
    this.#hasProjectedMessage = input.hasProjectedMessage;
    this.#wakeRecipient = input.wakeRecipient;
    this.#onTerminalCompletion = input.onTerminalCompletion;
    this.#router?.configureDelivery(input);
  }

  /** Send a Signal, Request, Answer, or Answer-and-Request. */
  async sendMessage(input: {
    target: MessageTarget;
    message: string;
    sourceEntryId: string;
    deliveryTiming?: SignalDeliveryTiming;
    responseRequired?: boolean;
    onAccepted: "continue" | "complete";
    prepareEndedRecipient?: (request: SignalAcceptRequest) => Promise<QueuedSignalReceipt>;
  }): Promise<QueuedSignalReceipt> {
    this.#assertOpen();
    assertNonEmpty(input.message, "Message");
    assertNonEmpty(input.sourceEntryId, "Message source entry ID");
    const sender = this.#controlPlane.currentAgent;
    const responseRequired = input.responseRequired === true;
    const onAccepted = input.onAccepted ?? "continue";
    if (onAccepted === "complete" && responseRequired) {
      throw new WorkflowProtocolError("InvalidCompletionMessage", "Terminal completion cannot create a Response Requirement");
    }
    if (onAccepted === "complete" && !this.#ownership) {
      throw new WorkflowProtocolError("OwnerActivationForbidden", "Workflow Owner cannot complete");
    }
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
      onAccepted,
    });
    if (existing && existing.deliveryStatus !== "bound") {
      return this.#store.reconcileAcceptedMessage(sender, existing.messageId, this.#ownership) ?? receiptFor(existing);
    }
    if (target.inReplyToRequestId && this.#store.inspectRequest(sender.workflowOwnerId, target.inReplyToRequestId)?.status !== "open") {
      throw new WorkflowProtocolError("AnswerAlreadyClosed", `Request ${target.inReplyToRequestId} already has a terminal outcome`);
    }

    const route = this.#store.readRouter(target.recipient);
    if (this.#store.recipientLifecycle(target.recipient) === "ended") {
      if (!responseRequired) {
        throw recipientUnreachable(target.recipient.agentId);
      }
      this.#store.assertEndedRecipientRequestAuthorized(sender, target.recipient);
      if (!input.prepareEndedRecipient) throw recipientUnreachable(target.recipient.agentId);
      const request: SignalAcceptRequest = {
        workflowOwnerId: sender.workflowOwnerId,
        messageId: existing?.messageId ?? this.#allocateMessageId(),
        senderAgentId: sender.agentId,
        recipientAgentId: target.recipient.agentId,
        sourceEntryId: input.sourceEntryId,
        payloadDigest,
        deliveryTiming: target.deliveryTiming,
        responseRequired: true,
        onAccepted,
        ...(target.inReplyToRequestId ? { inReplyToRequestId: target.inReplyToRequestId } : {}),
        message: input.message,
      };
      return input.prepareEndedRecipient(request);
    }

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
      onAccepted,
      ...(this.#ownership ? { ownership: this.#ownership } : {}),
      createdAtMs: this.#now(),
    });
    if (bound.deliveryStatus !== "bound") {
      return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(bound);
    }
    const request: SignalAcceptRequest = {
      workflowOwnerId: sender.workflowOwnerId,
      messageId: bound.messageId,
      senderAgentId: sender.agentId,
      recipientAgentId: target.recipient.agentId,
      sourceEntryId: input.sourceEntryId,
      payloadDigest,
      deliveryTiming: target.deliveryTiming,
      responseRequired,
      onAccepted,
      ...(target.inReplyToRequestId ? { inReplyToRequestId: target.inReplyToRequestId } : {}),
      message: input.message,
      ...(onAccepted === "complete" ? { completion: { ownership: this.#ownership! } } : {}),
    };

    let reply: SignalReceiptReply;
    try {
      reply = await this.#requestReceipt(route.endpoint, request);
    } catch (error) {
      const reconciled = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
      if (reconciled && reconciled.deliveryStatus !== "bound") {
        return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(reconciled);
      }
      if (!this.#store.discardUnacceptedMessage(sender, bound.messageId)) {
        const acceptedAfterDiscard = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
        if (acceptedAfterDiscard && acceptedAfterDiscard.deliveryStatus !== "bound") {
          return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(acceptedAfterDiscard);
        }
        if (this.#ownership) {
          this.#controlPlane.addActivationDependency(this.#ownership, {
            kind: "operation", dependencyId: `acceptance:${bound.messageId}`,
          });
        }
        this.#scheduleAcceptanceRetry();
        throw new WorkflowProtocolError("AcceptanceInDoubt", `Acceptance remains uncertain for Message ${bound.messageId}`);
      }
      throw recipientUnreachable(target.recipient.agentId, error);
    }
    if (!reply.accepted || !reply.receipt) {
      this.#store.discardUnacceptedMessage(sender, bound.messageId);
      throw replyError(reply.error);
    }
    return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? reply.receipt;
  }

  async reconcilePendingAcceptances(
    options: { waitForResolution?: boolean } = {},
  ): Promise<AcceptanceReconciliationResult> {
    const initiallyBound = this.#store.listBoundMessages(this.#controlPlane.currentAgent).length;
    let terminalCompletion = false;
    for (const bound of this.#store.listBoundMessages(this.#controlPlane.currentAgent)) {
      const pending = this.#acceptanceReconciliations.get(bound.messageId) ?? this.#reconcileBoundMessage(bound);
      this.#acceptanceReconciliations.set(bound.messageId, pending);
      try {
        const receipt = await pending;
        if (receipt && bound.onAccepted === "complete") terminalCompletion = true;
      } finally {
        if (this.#acceptanceReconciliations.get(bound.messageId) === pending) this.#acceptanceReconciliations.delete(bound.messageId);
      }
    }
    if (terminalCompletion) this.#onTerminalCompletion?.();
    const remainingBound = this.#store.listBoundMessages(this.#controlPlane.currentAgent).length;
    const result = { terminalCompletion };
    if (terminalCompletion || remainingBound === 0) {
      this.#cancelAcceptanceRetry();
      this.#resolveAcceptanceWaiters(result);
    } else {
      if (remainingBound < initiallyBound) this.#acceptanceRetryDelayMs = ACCEPTANCE_RETRY_INITIAL_DELAY_MS;
      this.#scheduleAcceptanceRetry();
      if (options.waitForResolution) return this.#waitForAcceptanceResolution();
    }
    return result;
  }

  async #reconcileBoundMessage(bound: DirectSignalRecord): Promise<QueuedSignalReceipt | undefined> {
    const sender = this.#controlPlane.currentAgent;
    const observed = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
    if (observed?.deliveryStatus !== "bound") {
      if (bound.onAccepted === "complete" && this.#store.terminalMessageCompleted(sender, bound.messageId)) return receiptFor(observed);
      return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(observed!);
    }
    const route = this.#store.readRouter({ workflowOwnerId: sender.workflowOwnerId, agentId: bound.recipientAgentId });
    if (!route) return undefined;
    const completion = bound.onAccepted === "complete" ? { ownership: this.#ownership! } : undefined;
    const canonical = {
      messageId: bound.messageId, sourceEntryId: bound.sourceEntryId, recipientAgentId: bound.recipientAgentId,
      payloadDigest: bound.payloadDigest, deliveryTiming: bound.deliveryTiming, responseRequired: bound.responseRequired,
      onAccepted: bound.onAccepted, ...(bound.inReplyToRequestId ? { inReplyToRequestId: bound.inReplyToRequestId } : {}),
      ...(completion ? { completion } : {}),
    };
    const message = resolveCanonicalSignal(this.#store.senderSessionPath(sender.workflowOwnerId, sender.agentId), canonical);
    const request: SignalAcceptRequest = {
      workflowOwnerId: sender.workflowOwnerId, messageId: bound.messageId, senderAgentId: sender.agentId,
      recipientAgentId: bound.recipientAgentId, sourceEntryId: bound.sourceEntryId, payloadDigest: bound.payloadDigest,
      deliveryTiming: bound.deliveryTiming, responseRequired: bound.responseRequired, onAccepted: bound.onAccepted,
      ...(bound.inReplyToRequestId ? { inReplyToRequestId: bound.inReplyToRequestId } : {}), message,
      ...(completion ? { completion } : {}),
    };
    try { await this.#requestReceipt(route.endpoint, request); } catch { /* Probe durable acceptance below. */ }
    const accepted = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
    if (accepted?.deliveryStatus !== "bound") {
      if (bound.onAccepted === "complete" && this.#store.terminalMessageCompleted(sender, bound.messageId)) return receiptFor(accepted);
      return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(accepted!);
    }
    if (this.#store.discardUnacceptedMessage(sender, bound.messageId)) return undefined;
    const raced = this.#store.inspectMessage(sender.workflowOwnerId, bound.messageId);
    if (raced?.deliveryStatus !== "bound") {
      if (bound.onAccepted === "complete" && this.#store.terminalMessageCompleted(sender, bound.messageId)) return receiptFor(raced);
      return this.#store.reconcileAcceptedMessage(sender, bound.messageId, this.#ownership) ?? receiptFor(raced!);
    }
    return undefined;
  }

  #scheduleAcceptanceRetry(): void {
    if (this.#closed || this.#acceptanceRetryTimer) return;
    const delayMs = this.#acceptanceRetryDelayMs;
    this.#acceptanceRetryDelayMs = Math.min(
      ACCEPTANCE_RETRY_MAXIMUM_DELAY_MS,
      delayMs * ACCEPTANCE_RETRY_BACKOFF_FACTOR,
    );
    this.#acceptanceRetryTimer = setTimeout(() => {
      this.#acceptanceRetryTimer = undefined;
      if (this.#closed) return;
      void this.reconcilePendingAcceptances().catch(() => {
        if (this.#closed) return;
        if (this.#store.listBoundMessages(this.#controlPlane.currentAgent).length > 0) this.#scheduleAcceptanceRetry();
      });
    }, delayMs);
    this.#acceptanceRetryTimer.unref?.();
  }

  #cancelAcceptanceRetry(): void {
    if (this.#acceptanceRetryTimer) clearTimeout(this.#acceptanceRetryTimer);
    this.#acceptanceRetryTimer = undefined;
    this.#acceptanceRetryDelayMs = ACCEPTANCE_RETRY_INITIAL_DELAY_MS;
  }

  #waitForAcceptanceResolution(): Promise<AcceptanceReconciliationResult> {
    return new Promise((resolve, reject) => {
      this.#acceptanceResolutionWaiters.add({ resolve, reject });
    });
  }

  #resolveAcceptanceWaiters(result: AcceptanceReconciliationResult): void {
    for (const waiter of this.#acceptanceResolutionWaiters) waiter.resolve(result);
    this.#acceptanceResolutionWaiters.clear();
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
      onAccepted: "continue",
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

  async cancelRequest(requestId: string): Promise<RequestCancellationReceipt> {
    this.#assertOpen();
    assertNonEmpty(requestId, "Request ID");
    const requester = this.#controlPlane.currentAgent;
    const receipt = this.#store.cancelRequest({
      requester,
      requestId,
      noticeMessageId: this.#allocateMessageId(),
      cancelledAtMs: this.#now(),
    });
    if (receipt.delivery === "notice-queued") {
      const request = this.#store.inspectRequest(requester.workflowOwnerId, requestId)!;
      const route = this.#store.readRouter(this.#controlPlane.agent(request.responderAgentId));
      if (route) {
        try {
          const connection = await connectFramedIpc(route.endpoint);
          try {
            await connection.send({
              version: CURRENT_IPC_VERSION,
              type: INBOX_SCHEDULE_TYPE,
              payload: { workflowOwnerId: requester.workflowOwnerId, recipientAgentId: request.responderAgentId },
            });
          } finally {
            connection.end();
          }
        } catch {
          // The notice is already durable. Router restart/reconciliation will
          // deliver it even when this best-effort scheduling hint is lost.
        }
      }
    }
    return receipt;
  }

  confirmDelivery(messageId: string): boolean {
    this.#assertOpen();
    return this.#router?.confirmDelivery(messageId) ?? false;
  }

  releaseDeferred(): void {
    this.#assertOpen();
    this.#router?.releaseDeferred();
  }

  close(options: { preserveRouterRegistration?: boolean } = {}): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    this.#cancelAcceptanceRetry();
    for (const waiter of this.#acceptanceResolutionWaiters) waiter.reject(new Error("Direct Signal runtime closed before acceptance reconciliation completed"));
    this.#acceptanceResolutionWaiters.clear();
    this.#closePromise = this.#close(options);
    return this.#closePromise;
  }

  async #close(options: { preserveRouterRegistration?: boolean }): Promise<void> {
    // Router startup reports its own failure; closing must still release the
    // store when that startup raced with shutdown.
    await this.#routerStartup?.catch(() => undefined);
    const router = this.#router;
    this.#router = undefined;
    try {
      await router?.close({ preserveRegistration: options.preserveRouterRegistration });
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
  if (error.code === "CompletionBlocked" && error.blockers) return new CompletionRejectedError(error.blockers);
  if (isWorkflowProtocolErrorCode(error.code)) return new WorkflowProtocolError(error.code, error.message);
  return new Error(error.message);
}

function isWorkflowProtocolErrorCode(code: string | undefined): code is ConstructorParameters<typeof WorkflowProtocolError>[0] {
  return code === "WorkflowMismatch" || code === "UnknownAgent" || code === "OwnershipLost"
    || code === "RecipientUnreachable" || code === "RecipientEnded" || code === "MessageIdentityConflict"
    || code === "RecipientReactivationUnauthorized"
    || code === "InvalidCompletionMessage" || code === "CompletionBlocked" || code === "AcceptanceInDoubt"
    || code === "InvalidMessageSource" || code === "AnswerUnauthorized" || code === "AnswerAlreadyClosed" || code === "UnknownRequest"
    || code === "RequestCancellationUnauthorized" || code === "RequestAlreadyClosed";
}

function recipientUnreachable(agentId: string, cause?: unknown): WorkflowProtocolError {
  const detail = cause instanceof Error ? `: ${cause.message}` : "";
  return new WorkflowProtocolError("RecipientUnreachable", `Recipient Inbox Router is unavailable for Agent ${agentId}${detail}`);
}

function assertNonEmpty(value: string, label: string): void {
  if (!value.trim()) throw new Error(`${label} must not be empty`);
}
