import type { ContextEvent } from "@earendil-works/pi-coding-agent";

export const AUTOMATIC_RECOVERY_CONTINUATION = "automatic_recovery_continuation";

type ProviderMessage = ContextEvent["messages"][number];

/** Stable identity for the one provider continuation owned by a recovery episode. */
export interface AutomaticRecoveryContinuation {
  projectionId: string;
  failedActivationId: string;
  replacementActivationId: string;
}

export function automaticRecoveryContinuationProjectionId(
  failedActivationId: string,
  replacementActivationId: string,
): string {
  return `automatic-recovery-continuation:${failedActivationId}:${replacementActivationId}`;
}

export function createAutomaticRecoveryContinuation(input: {
  failedActivationId: string;
  replacementActivationId: string;
}): AutomaticRecoveryContinuation {
  return {
    ...input,
    projectionId: automaticRecoveryContinuationProjectionId(
      input.failedActivationId,
      input.replacementActivationId,
    ),
  };
}

/** Scheduler markers are durable wake evidence, never provider-facing payload. */
export function projectAutomaticRecoveryContinuationContext(
  messages: ContextEvent["messages"],
): {
  messages: ContextEvent["messages"];
  observedProjectionIds: string[];
} {
  const observedProjectionIds = new Set<string>();
  const projectedMessages: ProviderMessage[] = [];
  for (const message of messages) {
    const continuation = automaticRecoveryContinuationFromUnknown(message);
    if (continuation) observedProjectionIds.add(continuation.projectionId);
    if (isAutomaticRecoveryContinuationMarker(message)) continue;
    projectedMessages.push(message);
  }
  return { messages: projectedMessages, observedProjectionIds: [...observedProjectionIds] };
}

function isAutomaticRecoveryContinuationMarker(value: unknown): boolean {
  return customMessageFromUnknown(value)?.customType === AUTOMATIC_RECOVERY_CONTINUATION;
}

function automaticRecoveryContinuationFromUnknown(
  value: unknown,
): AutomaticRecoveryContinuation | undefined {
  const message = customMessageFromUnknown(value);
  if (message?.customType !== AUTOMATIC_RECOVERY_CONTINUATION) return undefined;
  const details = message.details;
  if (!details || typeof details !== "object") return undefined;
  const continuation = details as Partial<AutomaticRecoveryContinuation>;
  if (
    typeof continuation.projectionId !== "string"
    || typeof continuation.failedActivationId !== "string"
    || typeof continuation.replacementActivationId !== "string"
    || continuation.projectionId !== automaticRecoveryContinuationProjectionId(
      continuation.failedActivationId,
      continuation.replacementActivationId,
    )
  ) return undefined;
  return continuation as AutomaticRecoveryContinuation;
}

function customMessageFromUnknown(
  value: unknown,
): { customType?: unknown; details?: unknown } | undefined {
  if (!value || typeof value !== "object") return undefined;
  const record = value as {
    type?: unknown;
    role?: unknown;
    customType?: unknown;
    details?: unknown;
    message?: unknown;
  };
  const message = record.type === "custom_message" || record.role === "custom"
    ? record
    : record.message;
  if (!message || typeof message !== "object") return undefined;
  const candidate = message as { role?: unknown; customType?: unknown; details?: unknown };
  if (candidate.role !== "custom" && record.type !== "custom_message") return undefined;
  return candidate;
}
