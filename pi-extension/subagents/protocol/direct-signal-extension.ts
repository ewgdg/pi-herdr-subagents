import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import type { InboxBatch } from "./direct-signal.ts";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";

const INBOX_BATCH_CUSTOM_TYPE = "agent_inbox_batch";

const AgentTarget = Type.Object({
  agent: Type.String({ description: "Known Agent session UUID in the current Workflow" }),
}, { additionalProperties: false });
const RequestTarget = Type.Object({
  request: Type.String({ description: "Request ID to Answer; routing is derived from that Request" }),
}, { additionalProperties: false });
const Message = Type.String({ minLength: 1, description: "Plain actionable message content" });
const Timing = Type.Optional(Type.Union([Type.Literal("steer"), Type.Literal("deferred")]));

/** Complete legal send forms; Request targets derive timing from their Request. */
export const AgentSendParams = Type.Union([
  Type.Object({ target: AgentTarget, message: Message, timing: Timing, responseRequired: Type.Optional(Type.Literal(false)) }, { additionalProperties: false }),
  Type.Object({ target: AgentTarget, message: Message, timing: Timing, responseRequired: Type.Literal(true) }, { additionalProperties: false }),
  Type.Object({ target: RequestTarget, message: Message, responseRequired: Type.Optional(Type.Literal(false)) }, { additionalProperties: false }),
  Type.Object({ target: RequestTarget, message: Message, responseRequired: Type.Literal(true) }, { additionalProperties: false }),
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
  });
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
      await startDirectSignalRouter(pi, workflowBootstrap, context);
      assertCanonicalAgentSendSource(
        context.sessionManager.getEntries(),
        toolCallId,
        params.target,
        params.message,
        "agent" in params.target ? params.timing : undefined,
        params.responseRequired === true,
      );
      const receipt = await workflowBootstrap.sendDirectMessage({
        target: "agent" in params.target
          ? { agentId: params.target.agent }
          : { requestId: params.target.request },
        message: params.message,
        sourceEntryId: toolCallId,
        deliveryTiming: "agent" in params.target ? params.timing : undefined,
        responseRequired: params.responseRequired,
      });
      return {
        content: [{
          type: "text",
          text:
            `Message ${receipt.messageId} queued for Agent ${receipt.recipientAgentId} ` +
            `(acceptance sequence ${receipt.acceptanceSequence}).`,
        }],
        details: receipt,
      };
    },
  });
}

export function projectInboxBatch(batch: InboxBatch): {
  customType: typeof INBOX_BATCH_CUSTOM_TYPE;
  content: string;
  display: true;
  details: {
    messages: Array<{
      kind: "signal" | "request" | "answer";
      messageId: string;
      senderAgentId: string;
      recipientAgentId: string;
      deliveryTiming: "steer" | "deferred";
      responseRequired?: true;
      inReplyToRequestId?: string;
    }>;
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
        senderAgentId: signal.senderAgentId,
        recipientAgentId: signal.recipientAgentId,
        deliveryTiming: signal.deliveryTiming,
        ...(signal.responseRequired ? { responseRequired: true as const } : {}),
        ...(signal.inReplyToRequestId ? { inReplyToRequestId: signal.inReplyToRequestId } : {}),
      })),
    },
  };
}

export function sessionContainsInboxMessage(entries: unknown[], messageId: string): boolean {
  return inboxMessageIds(entries).includes(messageId);
}

function projectInboxMessage(message: InboxBatch["messages"][number]): string {
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
  target: { agent?: string; request?: string },
  message: string,
  deliveryTiming: "steer" | "deferred" | undefined,
  responseRequired: boolean,
): void {
  const found = entries.some((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const transcriptMessage = (entry as { message?: unknown }).message;
    if (!transcriptMessage || typeof transcriptMessage !== "object") return false;
    const content = (transcriptMessage as { content?: unknown }).content;
    if (!Array.isArray(content)) return false;
    return content.some((block) => {
      if (!block || typeof block !== "object") return false;
      const candidate = block as {
        type?: unknown;
        id?: unknown;
        name?: unknown;
        arguments?: { target?: { agent?: unknown; request?: unknown }; message?: unknown; timing?: unknown; responseRequired?: unknown };
      };
      return candidate.type === "toolCall"
        && candidate.id === toolCallId
        && candidate.name === "agent_send"
        && candidate.arguments?.target?.agent === target.agent
        && candidate.arguments?.target?.request === target.request
        && candidate.arguments?.message === message
        && candidate.arguments?.timing === deliveryTiming
        && (candidate.arguments?.responseRequired === true) === responseRequired;
    });
  });
  if (!found) {
    throw new Error(`agent_send tool call ${toolCallId} is not durable in the sender transcript`);
  }
}
