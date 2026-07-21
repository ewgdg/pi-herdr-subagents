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
import { DirectSignalStore } from "./sqlite-message-store.ts";
import {
  WorkflowProtocolError,
  type AgentReference,
  type AgentRunOwnership,
} from "./workflow-types.ts";
import type {
  AcceptedSignal,
  InboxBatch,
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
  wakeRecipient?: () => void;
  now: () => number;
}
export class RecipientInboxRouter {
  #options: RecipientInboxRouterOptions;
  readonly #store: DirectSignalStore;
  readonly #endpoint: string;
  #server: FramedIpcServer | undefined;
  #closed = false;

  constructor(options: RecipientInboxRouterOptions) {
    this.#options = options;
    this.#store = new DirectSignalStore(options.databasePath);
    this.#endpoint = createRouterEndpoint(options.workflowOwnerId, options.recipient.agentId);
  }

  async start(): Promise<void> {
    if (this.#server) return;
    const server = await listenForFramedIpc(this.#endpoint, (connection) => {
      connection.onMessage((message) => {
        void this.#handle(connection, message).catch(() => {
          connection.end();
        });
      });
    });
    try {
      this.#store.registerRouter({
        recipient: this.#options.recipient,
        ownership: this.#options.ownership,
        endpoint: this.#endpoint,
        registeredAtMs: this.#options.now(),
      });
      this.#server = server;
    } catch (error) {
      await server.close();
      removeSocketFile(this.#endpoint);
      throw error;
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
    wakeRecipient?: () => void;
  }): void {
    this.#options = { ...this.#options, ...input };
  }

  confirmDelivery(messageId: string): boolean {
    return this.#store.commitDelivery({
      recipient: this.#options.recipient,
      ownership: this.#options.ownership,
      endpoint: this.#endpoint,
      messageId,
      deliveredAtMs: this.#options.now(),
    });
  }

  async #handle(connection: FramedIpcConnection, frame: FramedIpcMessage): Promise<void> {
    if (frame.type !== SIGNAL_REQUEST_TYPE) {
      await sendRejected(connection, { message: `Unsupported Inbox Router request: ${frame.type}` });
      return;
    }

    let request: SignalAcceptRequest;
    let accepted: AcceptedSignal;
    try {
      request = parseSignalRequest(frame.payload);
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
    if (accepted.delivery === "project") {
      try {
        this.#project(request, accepted);
      } catch {
        // Acceptance is already durable. Leave the pointer queued rather than
        // misreporting delivery or sending a second acceptance response.
      }
    }
  }

  #project(request: SignalAcceptRequest, accepted: AcceptedSignal): void {
    const batch: InboxBatch = {
      messages: [{
        kind: "signal",
        messageId: request.messageId,
        senderAgentId: request.senderAgentId,
        recipientAgentId: request.recipientAgentId,
        message: request.message,
      }],
    };
    this.#options.projectInboxBatch(batch);
    if (accepted.wakeRecipient) this.#options.wakeRecipient?.();
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
  return candidate as unknown as SignalAcceptRequest;
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
