import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import type { InboxBatch } from "./direct-signal.ts";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";

const INBOX_BATCH_CUSTOM_TYPE = "agent_inbox_batch";

const AgentSendParams = Type.Object({
  target: Type.Object({
    agent: Type.String({
      description: "Known Agent session UUID in the current Workflow",
    }),
  }, { additionalProperties: false }),
  message: Type.String({
    minLength: 1,
    description: "Plain actionable Signal content",
  }),
  timing: Type.Optional(Type.Union([
    Type.Literal("steer"),
    Type.Literal("deferred"),
  ])),
}, { additionalProperties: false });

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
    label: "Send Signal",
    description:
      "Send one direct Signal to a known, addressable Agent in the same Workflow. " +
      "A successful result is only a durable queued receipt; it does not claim that the recipient understood or acted on the Signal." +
      ownerHint,
    promptSnippet:
      "Send one direct Signal to a known Agent session UUID in the same Workflow. " +
      "The result is a durable queued receipt, not a read receipt.",
    parameters: AgentSendParams,
    async execute(toolCallId, params, _signal, _onUpdate, context) {
      await workflowBootstrap.waitUntilReady(context);
      await startDirectSignalRouter(pi, workflowBootstrap, context);
      assertCanonicalAgentSendSource(
        context.sessionManager.getEntries(),
        toolCallId,
        params.target.agent,
        params.message,
        params.timing ?? "steer",
      );
      const receipt = await workflowBootstrap.sendDirectSignal({
        targetAgentId: params.target.agent,
        message: params.message,
        sourceEntryId: toolCallId,
        deliveryTiming: params.timing,
      });
      return {
        content: [{
          type: "text",
          text:
            `Signal ${receipt.messageId} queued for Agent ${receipt.recipientAgentId} ` +
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
      kind: "signal";
      messageId: string;
      senderAgentId: string;
      recipientAgentId: string;
      deliveryTiming: "steer" | "deferred";
    }>;
  };
} {
  const content = batch.messages.map((signal) =>
    `Signal from Agent ${signal.senderAgentId} [${signal.messageId}]\n\n${signal.message}`,
  ).join("\n\n---\n\n");
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
      })),
    },
  };
}

export function sessionContainsInboxMessage(entries: unknown[], messageId: string): boolean {
  return inboxMessageIds(entries).includes(messageId);
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
  targetAgentId: string,
  message: string,
  deliveryTiming: "steer" | "deferred",
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
        arguments?: { target?: { agent?: unknown }; message?: unknown; timing?: unknown };
      };
      return candidate.type === "toolCall"
        && candidate.id === toolCallId
        && candidate.name === "agent_send"
        && candidate.arguments?.target?.agent === targetAgentId
        && candidate.arguments?.message === message
        && (candidate.arguments?.timing ?? "steer") === deliveryTiming;
    });
  });
  if (!found) {
    throw new Error(`agent_send tool call ${toolCallId} is not durable in the sender transcript`);
  }
}
