import type { ContextEvent, ExtensionAPI } from "@earendil-works/pi-coding-agent";

export const HUMAN_INTERRUPT_RECOVERY_CONTINUATION = "human_interrupt_recovery_continuation";

type ProviderMessage = ContextEvent["messages"][number];

/** Canonical Human answer metadata copied into a stable hidden projection marker. */
export interface HumanInterruptRecoveryContinuation {
  projectionId: string;
  toolCallId: string;
  responseInputId: string;
  response: string;
  timestamp: number;
}

export type CanonicalHumanInterruptRecovery = Omit<HumanInterruptRecoveryContinuation, "projectionId">;

export function humanInterruptRecoveryProjectionId(toolCallId: string, responseInputId: string): string {
  return `human-interrupt-result:${toolCallId}:${responseInputId}`;
}

export function createHumanInterruptRecoveryContinuation(
  input: CanonicalHumanInterruptRecovery,
): HumanInterruptRecoveryContinuation {
  return {
    ...input,
    projectionId: humanInterruptRecoveryProjectionId(input.toolCallId, input.responseInputId),
  };
}

/**
 * Pi exposes no supported tool-result injection API. The hidden custom marker
 * is therefore the durable projection identity and scheduler input; it is
 * replaced only in the read-only provider context built for each model call.
 */
export function sendHumanInterruptRecoveryContinuation(
  pi: Pick<ExtensionAPI, "sendMessage">,
  canonical: CanonicalHumanInterruptRecovery,
  inFlight: Set<string>,
): boolean {
  const marker = createHumanInterruptRecoveryContinuation(canonical);
  if (inFlight.has(marker.projectionId)) return false;
  inFlight.add(marker.projectionId);
  try {
    pi.sendMessage({
      customType: HUMAN_INTERRUPT_RECOVERY_CONTINUATION,
      content: "",
      display: false,
      details: marker,
    }, { triggerTurn: true, deliverAs: "steer" });
    return true;
  } catch {
    // A thrown send has no projection evidence, so this runtime may retry. A
    // successful void send stays fenced until the context hook observes it.
    inFlight.delete(marker.projectionId);
    return false;
  }
}

/**
 * Strip every recovery marker and derive one canonical Human tool result at
 * its sole assistant tool call.
 */
export function projectHumanInterruptRecoveryContinuations(
  messages: ContextEvent["messages"],
  canonical?: CanonicalHumanInterruptRecovery | CanonicalHumanInterruptRecovery[],
): {
  messages: ContextEvent["messages"];
  markerObserved: boolean;
  observedProjectionIds: string[];
  projected: boolean;
  projectedToolCallIds: string[];
} {
  let projectedMessages = messages.filter((message) => !isRecoveryContinuationMarker(message));
  const canonicalAnswers = canonical ? (Array.isArray(canonical) ? canonical : [canonical]) : [];
  const observedMarkers = messages.flatMap((message) => {
    const continuation = continuationFromUnknown(message);
    return continuation ? [continuation] : [];
  });
  let markerObserved = false;
  const observedProjectionIds: string[] = [];
  const projectedToolCallIds: string[] = [];

  for (const answer of canonicalAnswers) {
    const expected = createHumanInterruptRecoveryContinuation(answer);
    if (!observedMarkers.some((marker) => continuationsEqual(marker, expected))) continue;
    markerObserved = true;
    observedProjectionIds.push(expected.projectionId);
    if (projectedMessages.some((message) => isSuccessfulHumanResult(message, answer.toolCallId))) continue;
    const projection = insertCanonicalHumanResult(projectedMessages, answer);
    if (!projection) continue;
    projectedMessages = projection;
    projectedToolCallIds.push(answer.toolCallId);
  }

  return {
    messages: projectedMessages,
    markerObserved,
    observedProjectionIds,
    projected: projectedToolCallIds.length > 0,
    projectedToolCallIds,
  };
}

function insertCanonicalHumanResult(
  messages: ContextEvent["messages"],
  canonical: CanonicalHumanInterruptRecovery,
): ContextEvent["messages"] | undefined {
  const assistantIndex = messages.findIndex((message) => assistantCallsTool(message, canonical.toolCallId));
  if (assistantIndex < 0) return undefined;

  const synthetic = {
    role: "toolResult",
    toolCallId: canonical.toolCallId,
    toolName: "agent_ask_user",
    content: [{ type: "text", text: canonical.response }],
    isError: false,
    timestamp: canonical.timestamp,
  } as ProviderMessage;

  return [
    ...messages.slice(0, assistantIndex + 1),
    synthetic,
    ...messages.slice(assistantIndex + 1),
  ];
}

function isRecoveryContinuationMarker(value: unknown): boolean {
  const message = customMessageFromUnknown(value);
  return message?.customType === HUMAN_INTERRUPT_RECOVERY_CONTINUATION;
}

function continuationFromUnknown(value: unknown): HumanInterruptRecoveryContinuation | undefined {
  const candidate = customMessageFromUnknown(value);
  if (candidate?.customType !== HUMAN_INTERRUPT_RECOVERY_CONTINUATION) return undefined;
  const details = candidate.details;
  if (!details || typeof details !== "object") return undefined;
  const continuation = details as Partial<HumanInterruptRecoveryContinuation>;
  if (
    typeof continuation.projectionId !== "string"
    || typeof continuation.toolCallId !== "string"
    || typeof continuation.responseInputId !== "string"
    || typeof continuation.response !== "string"
    || typeof continuation.timestamp !== "number"
    || !Number.isFinite(continuation.timestamp)
  ) return undefined;
  if (continuation.projectionId !== humanInterruptRecoveryProjectionId(continuation.toolCallId, continuation.responseInputId)) return undefined;
  return continuation as HumanInterruptRecoveryContinuation;
}

function customMessageFromUnknown(value: unknown): { customType?: unknown; details?: unknown } | undefined {
  if (!value || typeof value !== "object") return undefined;
  const record = value as { role?: unknown; customType?: unknown; details?: unknown; message?: unknown; type?: unknown };
  const message = record.type === "custom_message" || record.role === "custom" ? record : record.message;
  if (!message || typeof message !== "object") return undefined;
  const candidate = message as { role?: unknown; customType?: unknown; details?: unknown };
  if (candidate.role !== "custom" && record.type !== "custom_message") return undefined;
  return candidate;
}

function continuationsEqual(
  observed: HumanInterruptRecoveryContinuation,
  expected: HumanInterruptRecoveryContinuation,
): boolean {
  return observed.projectionId === expected.projectionId
    && observed.toolCallId === expected.toolCallId
    && observed.responseInputId === expected.responseInputId
    && observed.response === expected.response
    && observed.timestamp === expected.timestamp;
}

function assistantCallsTool(message: ProviderMessage, toolCallId: string): boolean {
  const assistant = message as { role?: unknown; content?: Array<{ type?: unknown; id?: unknown; name?: unknown }> };
  return assistant.role === "assistant" && Array.isArray(assistant.content)
    && assistant.content.some((block) => block.type === "toolCall"
      && block.id === toolCallId && block.name === "agent_ask_user");
}

function isSuccessfulHumanResult(message: ProviderMessage, toolCallId: string): boolean {
  const result = message as { role?: unknown; toolCallId?: unknown; toolName?: unknown; isError?: unknown };
  return result.role === "toolResult" && result.toolCallId === toolCallId
    && result.toolName === "agent_ask_user" && result.isError !== true;
}

