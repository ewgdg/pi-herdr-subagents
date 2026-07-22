import { createHash, randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CURRENT_IPC_VERSION,
  listenForFramedIpc,
  type FramedIpcConnection,
  type FramedIpcMessage,
  type FramedIpcServer,
} from "../coordination/framed-ipc.ts";
import { resolveCanonicalSignal, signalDeliveryTiming } from "./direct-signal-transcript.ts";
import { DirectSignalStore } from "./sqlite-message-store.ts";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
import type {
  AcceptedSignal,
  DirectSignalMessage,
  InboxBatch,
  PendingMessagePointer,
  SignalAcceptRequest,
  SignalReceiptReply,
} from "./direct-signal-types.ts";

const SIGNAL_REQUEST_TYPE = "direct-signal.accept";
const SIGNAL_RECEIPT_TYPE = "direct-signal.receipt";

export interface RecipientInboxRouterOptions {
  workflowOwnerId: string;
  recipient: AgentReference;
  ownership?: AgentRunOwnership;
  databasePath: string;
  projectInboxBatch(batch: InboxBatch): void;
  hasProjectedMessage?(messageId: string): boolean;
  wakeRecipient?: () => void;
  now: () => number;
}

export class RecipientInboxRouter {
  #options: RecipientInboxRouterOptions;
  readonly #store: DirectSignalStore;
  readonly #endpoint: string;
  #server: FramedIpcServer | undefined;
  #closed = false;
  #prepared = false;
  #scheduling = false;
  readonly #projectedMessageIds = new Set<string>();

  constructor(options: RecipientInboxRouterOptions) {
    this.#options = options;
    this.#store = new DirectSignalStore(options.databasePath);
    this.#endpoint = createRouterEndpoint(options.workflowOwnerId, options.recipient.agentId);
  }

  get endpoint(): string { return this.#endpoint; }

  /** Start the recipient-owned listener before durable membership exists. */
  async prepare(): Promise<void> {
    this.#prepared = true;
    await this.#startListener(false);
  }

  /** Fence and register a listener prepared before the atomic spawn commit. */
  activatePrepared(ownership: AgentRunOwnership, register = true): void {
    if (!this.#prepared || !this.#server) throw new Error("Recipient Inbox Router was not prepared");
    this.#options = { ...this.#options, ownership };
    if (register) {
      this.#store.registerRouter({ recipient: this.#options.recipient, ownership, endpoint: this.#endpoint, registeredAtMs: this.#options.now() });
    }
    this.#prepared = false;
    this.#schedule();
  }

  async start(): Promise<void> {
    if (this.#server) return;
    await this.#startListener(true);
  }

  async #startListener(register: boolean): Promise<void> {
    if (this.#server) return;
    let server: FramedIpcServer | undefined;
    try {
      server = await listenForFramedIpc(this.#endpoint, (connection) => {
        connection.onMessage((message) => {
          void this.#handle(connection, message).catch(() => connection.end());
        });
      });
      if (register) this.#store.registerRouter({
        recipient: this.#options.recipient,
        ownership: this.#options.ownership,
        endpoint: this.#endpoint,
        registeredAtMs: this.#options.now(),
      });
      this.#server = server;
      if (register) this.#schedule();
    } catch (error) {
      await this.#cleanupFailedStart(server, error);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const errors: unknown[] = [];
    try {
      this.#store.unregisterRouter(this.#options.recipient, this.#endpoint);
    } catch (error) {
      errors.push(error);
    }
    try {
      if (this.#server) await this.#server.close();
    } catch (error) {
      errors.push(error);
    } finally {
      this.#server = undefined;
      try {
        this.#store.close();
      } catch (error) {
        errors.push(error);
      }
      try {
        removeSocketFile(this.#endpoint);
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, `Failed to close Inbox Router ${this.#endpoint}`);
    }
  }

  configureDelivery(input: {
    projectInboxBatch(batch: InboxBatch): void;
    hasProjectedMessage?(messageId: string): boolean;
    wakeRecipient?: () => void;
  }): void {
    this.#options = { ...this.#options, ...input };
    this.#schedule();
  }

  confirmDelivery(messageId: string): boolean {
    const message = this.#store.inspectMessage(this.#options.workflowOwnerId, messageId);
    const delivery = this.#store.commitDelivery({
      recipient: this.#options.recipient,
      ownership: this.#options.ownership,
      endpoint: this.#endpoint,
      messageId,
      deliveredAtMs: this.#options.now(),
    });
    const newlyDelivered = delivery === "newly-delivered";
    if (newlyDelivered) this.#projectedMessageIds.delete(messageId);
    if (newlyDelivered && message) {
      const lifecycle = this.#store.recipientLifecycle(this.#options.recipient);
      if (lifecycle === "waiting" || lifecycle === "owner") this.#options.wakeRecipient?.();
    }
    return newlyDelivered;
  }

  releaseDeferred(): void {
    this.#schedule();
  }

  async #handle(connection: FramedIpcConnection, frame: FramedIpcMessage): Promise<void> {
    if (frame.type !== SIGNAL_REQUEST_TYPE) {
      await sendRejected(connection, { message: `Unsupported Inbox Router request: ${frame.type}` });
      return;
    }

    let accepted: AcceptedSignal;
    try {
      if (this.#prepared) {
        throw new WorkflowProtocolError("RecipientUnreachable", "Recipient Inbox Router is not activated");
      }
      const request = parseSignalRequest(frame.payload);
      if (request.workflowOwnerId !== this.#options.workflowOwnerId) {
        throw new WorkflowProtocolError(
          "WorkflowMismatch",
          `Signal belongs to Workflow ${request.workflowOwnerId}, not ${this.#options.workflowOwnerId}`,
        );
      }
      if (request.recipientAgentId !== this.#options.recipient.agentId) {
        throw new WorkflowProtocolError(
          "UnknownAgent",
          `Inbox Router for ${this.#options.recipient.agentId} cannot accept Signal for ${request.recipientAgentId}`,
        );
      }
      if (digestPayload(request.message) !== request.payloadDigest) {
        throw new WorkflowProtocolError(
          "InvalidMessageSource",
          `Signal ${request.messageId} payload does not match its durable source binding`,
        );
      }
      const senderSessionPath = this.#store.senderSessionPath(request.workflowOwnerId, request.senderAgentId);
      resolveCanonicalSignal(senderSessionPath, request);
      accepted = this.#store.acceptSignal({
        request,
        recipient: this.#options.recipient,
        ownership: this.#options.ownership,
        endpoint: this.#endpoint,
        acceptedAtMs: this.#options.now(),
      });
    } catch (error) {
      await sendRejected(connection, normalizeReplyError(error));
      return;
    }
    await connection.send({
      version: CURRENT_IPC_VERSION,
      type: SIGNAL_RECEIPT_TYPE,
      payload: { accepted: true, receipt: accepted.receipt } satisfies SignalReceiptReply,
    });
    connection.end();
    this.#schedule();
  }

  #schedule(): void {
    if (this.#closed || this.#scheduling || !this.#server) return;
    this.#scheduling = true;
    queueMicrotask(() => {
      try {
        this.#deliverEligible();
      } finally {
        this.#scheduling = false;
      }
    });
  }

  #deliverEligible(): void {
    if (this.#closed) return;
    const pending = this.#store.listPending(this.#options.recipient);
    for (const pointer of pending) {
      if (this.#options.hasProjectedMessage?.(pointer.messageId)) {
        this.confirmDelivery(pointer.messageId);
      }
    }
    const lifecycle = this.#store.recipientLifecycle(this.#options.recipient);
    if (lifecycle === "ended" || lifecycle === "interrupted" || lifecycle === "waiting-human") return;
    const queued = this.#store.listPending(this.#options.recipient);
    const eligible = (lifecycle === "active"
      ? queued.filter((pointer) => pointer.deliveryTiming === "steer" || pointer.reactivatesRecipient)
      : queued).filter((pointer) => !this.#projectedMessageIds.has(pointer.messageId));
    if (eligible.length === 0) return;

    const messages = eligible.map((pointer) => this.#resolve(pointer));
    const batch: InboxBatch = {
      // A mixed batch can only be released while idle. Mark it deferred so the
      // Pi bridge never treats it as permission to interrupt active work.
      deliveryTiming: messages.some((message) => message.deliveryTiming === "deferred") ? "deferred" : "steer",
      messages,
    };
    try {
      this.#options.projectInboxBatch(batch);
    } catch {
      // Projection is the durability boundary. Keep every pointer until a
      // later Router startup or delivery point can safely project the batch.
      return;
    }
    for (const message of messages) this.#projectedMessageIds.add(message.messageId);
  }

  #resolve(pointer: PendingMessagePointer): DirectSignalMessage {
    const sessionPath = this.#store.senderSessionPath(
      this.#options.workflowOwnerId,
      pointer.senderAgentId,
    );
    const record = this.#store.inspectMessage(this.#options.workflowOwnerId, pointer.messageId);
    if (!record) throw new Error(`Queued Message ${pointer.messageId} is missing`);
    return {
      kind: record.kind,
      messageId: pointer.messageId,
      senderAgentId: pointer.senderAgentId,
      recipientAgentId: pointer.recipientAgentId,
      deliveryTiming: pointer.deliveryTiming,
      message: resolveCanonicalSignal(sessionPath, pointer),
      ...(pointer.responseRequired ? { responseRequired: true as const } : {}),
      ...(pointer.inReplyToRequestId ? { inReplyToRequestId: pointer.inReplyToRequestId } : {}),
    };
  }

  async #cleanupFailedStart(server: FramedIpcServer | undefined, cause: unknown): Promise<never> {
    this.#closed = true;
    const errors = [cause];
    try {
      await server?.close();
    } catch (error) {
      errors.push(error);
    }
    try {
      this.#store.close();
    } catch (error) {
      errors.push(error);
    }
    try {
      removeSocketFile(this.#endpoint);
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 1) throw cause;
    throw new AggregateError(errors, `Failed to start Inbox Router ${this.#endpoint}`);
  }
}

function parseSignalRequest(value: unknown): SignalAcceptRequest {
  if (!value || typeof value !== "object") throw new TypeError("Signal acceptance payload must be an object");
  const candidate = value as Record<string, unknown>;
  for (const field of [
    "workflowOwnerId",
    "messageId",
    "senderAgentId",
    "recipientAgentId",
    "sourceEntryId",
    "payloadDigest",
    "message",
  ] as const) {
    if (typeof candidate[field] !== "string" || !candidate[field]) {
      throw new TypeError(`Signal acceptance ${field} must be a non-empty string`);
    }
  }
  if (candidate.responseRequired !== undefined && typeof candidate.responseRequired !== "boolean") {
    throw new TypeError("Signal acceptance responseRequired must be a boolean");
  }
  if (candidate.inReplyToRequestId !== undefined && (typeof candidate.inReplyToRequestId !== "string" || !candidate.inReplyToRequestId)) {
    throw new TypeError("Signal acceptance inReplyToRequestId must be a non-empty string");
  }
  return {
    ...candidate,
    deliveryTiming: signalDeliveryTiming(candidate.deliveryTiming),
    responseRequired: candidate.responseRequired === true,
    ...(typeof candidate.inReplyToRequestId === "string" ? { inReplyToRequestId: candidate.inReplyToRequestId } : {}),
  } as SignalAcceptRequest;
}

async function sendRejected(
  connection: FramedIpcConnection,
  error: { code?: string; message: string },
): Promise<void> {
  await connection.send({
    version: CURRENT_IPC_VERSION,
    type: SIGNAL_RECEIPT_TYPE,
    payload: { accepted: false, error } satisfies SignalReceiptReply,
  });
  connection.end();
}

function normalizeReplyError(error: unknown): { code?: string; message: string } {
  if (error instanceof WorkflowProtocolError) return { code: error.code, message: error.message };
  return { message: error instanceof Error ? error.message : String(error) };
}

function digestPayload(message: string): string {
  return createHash("sha256").update(message, "utf8").digest("hex");
}

function createRouterEndpoint(workflowOwnerId: string, agentId: string): string {
  const identity = createHash("sha256")
    .update(`${workflowOwnerId}:${agentId}:${process.pid}:${randomUUID()}`)
    .digest("hex")
    .slice(0, 24);
  return process.platform === "win32"
    ? `\\\\.\\pipe\\pi-herdr-signal-${identity}`
    : join(tmpdir(), `pi-herdr-signal-${identity}.sock`);
}

function removeSocketFile(endpoint: string): void {
  if (process.platform === "win32") return;
  try {
    unlinkSync(endpoint);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}
