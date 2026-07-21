import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { WorkflowProtocolError } from "./workflow-types.ts";
import type { SignalAcceptRequest, SignalDeliveryTiming } from "./direct-signal-types.ts";

export function resolveCanonicalSignal(
  sessionPath: string,
  input: Pick<
    SignalAcceptRequest,
    "messageId" | "sourceEntryId" | "recipientAgentId" | "payloadDigest" | "deliveryTiming"
  >,
): string {
  const entries = readFileSync(sessionPath, "utf8").split("\n");
  for (const line of entries) {
    if (!line) continue;
    const candidate = JSON.parse(line) as { message?: unknown };
    const content = (candidate.message as { content?: unknown } | undefined)?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      const toolCall = block as {
        type?: unknown;
        id?: unknown;
        name?: unknown;
        arguments?: { target?: { agent?: unknown }; message?: unknown; timing?: unknown };
      };
      if (toolCall.type !== "toolCall" || toolCall.id !== input.sourceEntryId || toolCall.name !== "agent_send") {
        continue;
      }
      const message = toolCall.arguments?.message;
      const target = toolCall.arguments?.target?.agent;
      const timing = toolCall.arguments?.timing ?? "steer";
      if (
        typeof message !== "string"
        || target !== input.recipientAgentId
        || timing !== input.deliveryTiming
        || digestPayload(message) !== input.payloadDigest
      ) {
        break;
      }
      return message;
    }
  }
  throw new WorkflowProtocolError(
    "InvalidMessageSource",
    `Signal ${input.messageId} does not match its canonical sender transcript entry`,
  );
}

export function digestPayload(message: string): string {
  return createHash("sha256").update(message, "utf8").digest("hex");
}

export function signalDeliveryTiming(value: unknown): SignalDeliveryTiming {
  if (value === undefined || value === "steer") return "steer";
  if (value === "deferred") return "deferred";
  throw new TypeError("Signal delivery timing must be steer or deferred");
}
