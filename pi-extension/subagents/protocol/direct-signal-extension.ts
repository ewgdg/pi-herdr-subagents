import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { randomUUID } from "node:crypto";
import type { InboxBatch } from "./direct-signal.ts";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";
import { WorkflowProtocolError } from "./workflow-types.ts";
import { assertSoleToolCall, findAgentSendToolCall } from "./direct-signal-transcript.ts";
import { CompletionRejectedError } from "./completion-gate.ts";
import { completionBlockedResult } from "./completion-extension.ts";

const INBOX_BATCH_CUSTOM_TYPE = "agent_inbox_batch";

const AgentTarget = Type.Object({
  agent: Type.String({ description: "Known Agent session UUID in the current Workflow" }),
}, { additionalProperties: false });
const RequestTarget = Type.Object({
  request: Type.String({ description: "Request ID to Answer; routing is derived from that Request" }),
}, { additionalProperties: false });
const SpawnSpec = Type.Object({
  agent: Type.String({ minLength: 1, description: "Agent Definition name for the direct child" }),
  name: Type.Optional(Type.String({ minLength: 1, description: "Optional display name for the direct child" })),
}, { additionalProperties: false });
const SpawnTarget = Type.Object({ spawn: SpawnSpec }, { additionalProperties: false });
const Message = Type.String({ minLength: 1, description: "Plain actionable message content" });
const Timing = Type.Optional(Type.Union([Type.Literal("steer"), Type.Literal("deferred")]));
const Continue = Type.Literal("continue");
const TerminalDisposition = Type.Union([Continue, Type.Literal("complete")]);

/** Complete legal send forms; Request targets derive timing from their Request. */
export const AgentSendParams = Type.Union([
  Type.Object({ target: AgentTarget, message: Message, timing: Timing, responseRequired: Type.Optional(Type.Literal(false)), onAccepted: TerminalDisposition }, { additionalProperties: false }),
  Type.Object({ target: AgentTarget, message: Message, timing: Timing, responseRequired: Type.Literal(true), onAccepted: Continue }, { additionalProperties: false }),
  Type.Object({ target: RequestTarget, message: Message, responseRequired: Type.Optional(Type.Literal(false)), onAccepted: TerminalDisposition }, { additionalProperties: false }),
  Type.Object({ target: RequestTarget, message: Message, responseRequired: Type.Literal(true), onAccepted: Continue }, { additionalProperties: false }),
  Type.Object({ target: SpawnTarget, message: Message, responseRequired: Type.Literal(true), onAccepted: Continue }, { additionalProperties: false }),
]);

export async function startDirectSignalRouter(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  context: ExtensionContext,
): Promise<void> {
  await workflowBootstrap.waitUntilReady(context);
  await workflowBootstrap.startDirectSignalRouter({
    projectInboxBatch(batch) {
      pi.sendMessage(projectInboxBatch(batch), {
        triggerTurn: true,
        // Deferred batches are selected only after durable settlement. At the
        // Pi boundary they are safe to inject as a non-aborting steer.
        deliverAs: "steer",
      });
    },
    hasProjectedMessage(messageId) {
      return sessionContainsInboxMessage(context.sessionManager.getEntries(), messageId);
    },
    async projectInitialInboxBatch(batch) {
      pi.sendMessage(projectInboxBatch(batch), {
        // The PREPARE/PROJECT child must append durable JSONL before the
        // Spawner transaction, but cannot invoke a provider before RELEASE.
        triggerTurn: false,
        deliverAs: "steer",
      });
      const messageId = batch.messages[0]?.messageId;
      if (!messageId) throw new Error("Initial Inbox Batch has no Message Identity");
      await waitForProjectedInboxMessage(context, messageId);
    },
    releaseInitialInboxBatch() {
      // Pi only starts a turn through a message delivery. This marker carries
      // no actionable payload; the already-persisted Inbox Batch is the sole
      // request context and remains blocked until RELEASE.
      pi.sendMessage({
        customType: "agent_inbox_release",
        content: "",
        display: false,
        details: {},
      }, { triggerTurn: true, deliverAs: "steer" });
    },
    onTerminalCompletion() { context.shutdown(); },
  });
  await workflowBootstrap.reconcilePendingDirectSignals?.({ waitForResolution: true });
}

async function waitForProjectedInboxMessage(context: ExtensionContext, messageId: string): Promise<void> {
  const timeoutAt = Date.now() + 5_000;
  while (!sessionContainsInboxMessage(context.sessionManager.getEntries(), messageId)) {
    if (Date.now() >= timeoutAt) {
      throw new Error(`Timed out waiting for durable Inbox Batch ${messageId}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

export function confirmProjectedInboxBatches(
  workflowBootstrap: WorkflowBootstrap,
  entries: unknown[],
): number {
  let confirmed = 0;
  for (const messageId of inboxMessageIds(entries)) {
    if (workflowBootstrap.confirmDirectSignalDelivery(messageId)) confirmed += 1;
  }
  return confirmed;
}

export function registerAgentSendTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  enabled = true,
  options: {
    spawnInitialRequest?(input: {
      agent: string;
      name?: string;
      message: string;
      messageId: string;
      sourceEntryId: string;
      context: ExtensionContext;
    }): Promise<{ status: "queued" | "delivered"; messageId: string; recipientAgentId: string; acceptanceSequence: number }>;
    reconcileSpawnedInitialRequest?(input: {
      agent: string;
      name?: string;
      message: string;
      sourceEntryId: string;
      context: ExtensionContext;
    }): Promise<{ status: "queued" | "delivered"; messageId: string; recipientAgentId: string; acceptanceSequence: number } | undefined>;
    prepareEndedRecipient?(input: {
      request: import("./direct-signal-types.ts").SignalAcceptRequest;
      context: ExtensionContext;
    }): Promise<import("./direct-signal-types.ts").QueuedSignalReceipt>;
  } = {},
): void {
  if (!enabled) return;
  const ownerHint = process.env.PI_WORKFLOW_OWNER_SESSION_ID
    ? ` The Workflow Owner Agent ID is ${process.env.PI_WORKFLOW_OWNER_SESSION_ID}.`
    : "";

  pi.registerTool({
    name: "agent_send",
    label: "Send Message",
    description:
      "Send a Signal or Request to an addressable Agent, or Answer a Request. " +
      "A successful result is only a durable queued receipt; it does not claim that the recipient understood or acted on the message." +
      ownerHint,
    promptSnippet:
      "Send a Signal or Request to a known Agent, or Answer a Request ID. " +
      "An Answer derives its return route and timing from the Request. The result is a durable queued receipt, not a read receipt.",
    parameters: AgentSendParams,
    async execute(toolCallId, params, _signal, _onUpdate, context) {
      await workflowBootstrap.waitUntilReady(context);
      if ("spawn" in params.target) {
        assertCanonicalAgentSendSource(
          context.sessionManager.getEntries(),
          toolCallId,
          params.target,
          params.message,
          undefined,
          true,
          params.onAccepted,
        );
        if (!options.spawnInitialRequest) {
          throw new WorkflowProtocolError("RecipientUnreachable", "Spawned Initial Request launcher is unavailable");
        }
        const reconciled = await options.reconcileSpawnedInitialRequest?.({
          agent: params.target.spawn.agent,
          name: params.target.spawn.name,
          message: params.message,
          sourceEntryId: toolCallId,
          context,
        });
        if (reconciled) {
          return {
            content: [{
              type: "text",
              text: `Request ${reconciled.messageId} ${reconciled.status} for spawned Agent ${reconciled.recipientAgentId} (acceptance sequence ${reconciled.acceptanceSequence}).`,
            }],
            details: reconciled,
          };
        }
        const receipt = await options.spawnInitialRequest({
          agent: params.target.spawn.agent,
          name: params.target.spawn.name,
          message: params.message,
          messageId: randomUUID(),
          sourceEntryId: toolCallId,
          context,
        });
        return {
          content: [{
            type: "text",
            text: `Request ${receipt.messageId} ${receipt.status} for spawned Agent ${receipt.recipientAgentId} (acceptance sequence ${receipt.acceptanceSequence}).`,
          }],
          details: receipt,
        };
      }
      await startDirectSignalRouter(pi, workflowBootstrap, context);
      assertCanonicalAgentSendSource(
        context.sessionManager.getEntries(),
        toolCallId,
        params.target,
        params.message,
        "agent" in params.target ? params.timing : undefined,
        params.responseRequired === true,
        params.onAccepted,
      );
      if (params.onAccepted === "complete") assertSoleToolCall(context.sessionManager.getEntries(), toolCallId);
      let receipt;
      try {
        receipt = await workflowBootstrap.sendDirectMessage({
          target: "agent" in params.target
            ? { agentId: params.target.agent }
            : { requestId: params.target.request },
          message: params.message,
          sourceEntryId: toolCallId,
          deliveryTiming: "agent" in params.target ? params.timing : undefined,
          responseRequired: params.responseRequired,
          onAccepted: params.onAccepted,
          ...(options.prepareEndedRecipient && "agent" in params.target
            ? { prepareEndedRecipient: (request) => options.prepareEndedRecipient!({ request, context }) }
            : {}),
        });
      } catch (error) {
        if (error instanceof CompletionRejectedError) return completionBlockedResult(error);
        throw error;
      }
      if (params.onAccepted === "complete") {
        try {
          await workflowBootstrap.closeDirectSignalRouter();
        } catch (error) {
          console.warn("Activation completed, but local Router cleanup failed", error);
        }
        queueMicrotask(() => context.shutdown());
      }
      return {
        content: [{
          type: "text",
          text:
            `Message ${receipt.messageId} queued for Agent ${receipt.recipientAgentId} ` +
            `(acceptance sequence ${receipt.acceptanceSequence}).`,
        }],
        details: receipt,
        ...(params.onAccepted === "complete" ? { terminate: true } : {}),
      };
    },
  });
}

export function projectInboxBatch(batch: InboxBatch): {
  customType: typeof INBOX_BATCH_CUSTOM_TYPE;
  content: string;
  display: true;
  details: {
    messages: Array<({
      kind: "signal" | "request" | "answer";
      messageId: string;
      senderAgentId: string;
      recipientAgentId: string;
      deliveryTiming: "steer" | "deferred";
      responseRequired?: true;
      inReplyToRequestId?: string;
    } | {
      kind: "protocol-notice";
      noticeKind: "request-cancelled";
      messageId: string;
      requestId: string;
      recipientAgentId: string;
      deliveryTiming: "steer";
    })>;
  };
} {
  const content = batch.messages.map(projectInboxMessage).join("\n\n---\n\n");
  return {
    customType: INBOX_BATCH_CUSTOM_TYPE,
    content,
    display: true,
    details: {
      messages: batch.messages.map((signal) => ({
        kind: signal.kind,
        messageId: signal.messageId,
        recipientAgentId: signal.recipientAgentId,
        deliveryTiming: signal.deliveryTiming,
        ...(signal.kind === "protocol-notice"
          ? { noticeKind: signal.noticeKind, requestId: signal.requestId }
          : {
              senderAgentId: signal.senderAgentId,
              ...(signal.responseRequired ? { responseRequired: true as const } : {}),
              ...(signal.inReplyToRequestId ? { inReplyToRequestId: signal.inReplyToRequestId } : {}),
            }),
      })),
    },
  };
}

export function sessionContainsInboxMessage(entries: unknown[], messageId: string): boolean {
  return inboxMessageIds(entries).includes(messageId);
}

function projectInboxMessage(message: InboxBatch["messages"][number]): string {
  if (message.kind === "protocol-notice") {
    return [
      `Protocol Notice [Notice ID: ${message.messageId}]`,
      `Request ID: ${message.requestId}`,
      "",
      message.message,
    ].join("\n");
  }
  const identity = message.kind === "answer"
    ? `Answer ID: ${message.messageId}`
    : message.kind === "request" ? `Request ID: ${message.messageId}` : `Message ID: ${message.messageId}`;
  const label = message.kind === "answer" && message.responseRequired
    ? "Answer + Request"
    : message.kind === "answer" ? "Answer" : message.kind === "request" ? "Request" : "Signal";
  const requirement = message.responseRequired
    ? message.kind === "answer"
      ? `Response Requirement: New Request ID ${message.messageId} requires one terminal Answer.`
      : `Response Requirement: Request ID ${message.messageId} requires one terminal Answer.`
    : undefined;
  return [
    `${label} from Agent ${message.senderAgentId} [${identity}]`,
    ...(message.inReplyToRequestId ? [`inReplyToRequestId: ${message.inReplyToRequestId}`] : []),
    ...(requirement ? [requirement] : []),
    "",
    message.message,
  ].join("\n");
}

function inboxMessageIds(entries: unknown[]): string[] {
  return entries.flatMap((entry) => {
    if (!entry || typeof entry !== "object") return [];
    const record = entry as { type?: unknown; role?: unknown; message?: unknown };
    const candidate = (
      record.type === "custom_message" || record.role === "custom"
        ? record
        : record.message
    ) as {
      role?: unknown;
      customType?: unknown;
      details?: { messages?: Array<{ messageId?: unknown }> };
    };
    if (
      (!candidate || typeof candidate !== "object")
      || (record.type !== "custom_message" && candidate.role !== "custom")
      || candidate.customType !== INBOX_BATCH_CUSTOM_TYPE
    ) {
      return [];
    }
    return (candidate.details?.messages ?? []).flatMap((item) =>
      typeof item.messageId === "string" ? [item.messageId] : []);
  });
}

function assertCanonicalAgentSendSource(
  entries: unknown[],
  toolCallId: string,
  target: { agent?: string; request?: string; spawn?: { agent: string; name?: string } },
  message: string,
  deliveryTiming: "steer" | "deferred" | undefined,
  responseRequired: boolean,
  onAccepted: "continue" | "complete",
): void {
  const toolCall = findAgentSendToolCall(entries, toolCallId);
  const found = toolCall?.arguments.target?.agent === target.agent
    && toolCall.arguments.target?.request === target.request
    && toolCall.arguments.target?.spawn?.agent === target.spawn?.agent
    && toolCall.arguments.target?.spawn?.name === target.spawn?.name
    && toolCall.arguments.message === message
    && toolCall.arguments.timing === deliveryTiming
    && (toolCall.arguments.responseRequired === true) === responseRequired
    && toolCall.arguments.onAccepted === onAccepted;
  if (!found) {
    throw new Error(`agent_send tool call ${toolCallId} is not durable in the sender transcript`);
  }
}
