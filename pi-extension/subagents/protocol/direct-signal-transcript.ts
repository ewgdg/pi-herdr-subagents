import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { WorkflowProtocolError } from "./workflow-types.ts";
import type { DelegationPolicy } from "./workflow-types.ts";
import type { SignalAcceptRequest, SignalDeliveryTiming } from "./direct-signal-types.ts";

export interface CanonicalAgentSendToolCall {
  id: string;
  arguments: {
    target?: { agent?: unknown; spawn?: { agent?: unknown; name?: unknown; delegationPolicy?: unknown }; request?: unknown };
    message?: unknown;
    timing?: unknown;
    responseRequired?: unknown;
    onAccepted?: unknown;
    activation?: { intent?: unknown };
  };
}

export interface CanonicalAgentSendContent {
  message: string;
  activationIntent?: string;
}

/** One canonical traversal for JSONL and in-memory sender transcript entries. */
export function findAgentSendToolCall(entries: unknown[], sourceEntryId: string): CanonicalAgentSendToolCall | undefined {
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as { message?: unknown };
    const message = record.message && typeof record.message === "object" ? record.message as { content?: unknown } : record;
    const content = message.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (!block || typeof block !== "object") continue;
      const candidate = block as { type?: unknown; id?: unknown; name?: unknown; arguments?: CanonicalAgentSendToolCall["arguments"] };
      if (candidate.type === "toolCall" && candidate.name === "agent_send" && candidate.id === sourceEntryId) {
        return { id: sourceEntryId, arguments: candidate.arguments ?? {} };
      }
    }
  }
}

export function assertSoleToolCall(entries: unknown[], sourceEntryId: string): void {
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as { message?: unknown };
    const message = record.message && typeof record.message === "object" ? record.message as { content?: unknown } : record;
    if (!Array.isArray(message.content)) continue;
    const calls = message.content.filter((block): block is { type: "toolCall"; id?: unknown } => Boolean(block && typeof block === "object" && (block as { type?: unknown }).type === "toolCall"));
    if (calls.some((call) => call.id === sourceEntryId)) {
      if (calls.length !== 1) throw new WorkflowProtocolError("InvalidCompletionMessage", "A terminal completion call must be the sole tool call in its assistant turn");
      return;
    }
  }
  throw new WorkflowProtocolError("InvalidCompletionMessage", `Terminal tool call ${sourceEntryId} is absent from the canonical assistant turn`);
}

function senderToolCall(sessionPath: string, sourceEntryId: string): CanonicalAgentSendToolCall | undefined {
  const entries = readFileSync(sessionPath, "utf8").split("\n").flatMap((line) => line ? [JSON.parse(line)] : []);
  return findAgentSendToolCall(entries, sourceEntryId);
}

/** Resolve the source tool call rather than persisting actionable payloads in coordination state. */
export function resolveCanonicalSignal(
  sessionPath: string,
  input: Pick<SignalAcceptRequest, "messageId" | "sourceEntryId" | "recipientAgentId" | "payloadDigest" | "deliveryTiming" | "responseRequired" | "activationIntent" | "inReplyToRequestId" | "onAccepted" | "completion">,
): CanonicalAgentSendContent {
  const toolCall = senderToolCall(sessionPath, input.sourceEntryId);
  const target = toolCall?.arguments.target;
  const matchesTarget = input.inReplyToRequestId
    ? target?.request === input.inReplyToRequestId && toolCall?.arguments.timing === undefined
    : target?.agent === input.recipientAgentId && (toolCall?.arguments.timing ?? "steer") === input.deliveryTiming;
  const activationIntent = typeof toolCall?.arguments.activation?.intent === "string"
    ? toolCall.arguments.activation.intent
    : undefined;
  if (typeof toolCall?.arguments.message === "string" && matchesTarget
    && (toolCall.arguments.responseRequired === true) === input.responseRequired
    && activationIntent === input.activationIntent
    && (input.onAccepted === undefined || (toolCall.arguments.onAccepted ?? "continue") === input.onAccepted)
    && (!("completion" in input) || (input.onAccepted === "complete") === Boolean(input.completion))
    && digestPayload(toolCall.arguments.message) === input.payloadDigest) {
    return {
      message: toolCall.arguments.message,
      ...(activationIntent ? { activationIntent } : {}),
    };
  }
  throw new WorkflowProtocolError("InvalidMessageSource", `Message ${input.messageId} does not match its canonical sender transcript entry`);
}

/** Validate Spawn metadata against the sender-owned agent_send transcript entry. */
export function resolveCanonicalSpawnedInitialRequest(input: {
  sessionPath: string; sourceEntryId: string; agentDefinition: string; name: string; message: string; activationIntent: string; delegationPolicy?: DelegationPolicy;
}): void {
  resolveCanonicalSpawnedInitialMessage({
    ...input,
    payloadDigest: digestPayload(input.message),
  });
}

/** Resolve a Spawn request from its sender transcript without transporting payload. */
export function resolveCanonicalSpawnedInitialMessage(input: {
  sessionPath: string; sourceEntryId: string; agentDefinition: string; name: string; payloadDigest: string; activationIntent: string; delegationPolicy?: DelegationPolicy;
}): CanonicalAgentSendContent {
  const toolCall = senderToolCall(input.sessionPath, input.sourceEntryId);
  const spawn = toolCall?.arguments.target?.spawn;
  const activationIntent = typeof toolCall?.arguments.activation?.intent === "string"
    ? toolCall.arguments.activation.intent
    : undefined;
  if (spawn?.agent === input.agentDefinition && (spawn.name ?? spawn.agent) === input.name
    && spawn.delegationPolicy === input.delegationPolicy
    && activationIntent === input.activationIntent
    && typeof toolCall?.arguments.message === "string" && digestPayload(toolCall.arguments.message) === input.payloadDigest
    && toolCall.arguments.responseRequired === true && toolCall.arguments.timing === undefined) {
    return { message: toolCall.arguments.message, activationIntent: input.activationIntent };
  }
  throw new WorkflowProtocolError("InvalidMessageSource", `Spawned Initial Request ${input.sourceEntryId} does not match its canonical sender transcript entry`);
}

export function digestPayload(message: string): string { return createHash("sha256").update(message, "utf8").digest("hex"); }
export function signalDeliveryTiming(value: unknown): SignalDeliveryTiming { if (value === undefined || value === "steer") return "steer"; if (value === "deferred") return "deferred"; throw new TypeError("Signal delivery timing must be steer or deferred"); }
