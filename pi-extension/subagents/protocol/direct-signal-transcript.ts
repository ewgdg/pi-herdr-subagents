import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { WorkflowProtocolError } from "./workflow-types.ts";
import type { SignalAcceptRequest, SignalDeliveryTiming } from "./direct-signal-types.ts";

/** Resolve the source tool call rather than persisting actionable payloads in coordination state. */
export function resolveCanonicalSignal(
  sessionPath: string,
  input: Pick<SignalAcceptRequest, "messageId" | "sourceEntryId" | "recipientAgentId" | "payloadDigest" | "deliveryTiming" | "responseRequired" | "inReplyToRequestId">,
): string {
  for (const line of readFileSync(sessionPath, "utf8").split("\n")) {
    if (!line) continue;
    const candidate = JSON.parse(line) as { message?: unknown };
    const content = (candidate.message as { content?: unknown } | undefined)?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      const toolCall = block as {
        type?: unknown; id?: unknown; name?: unknown;
        arguments?: { target?: { agent?: unknown; request?: unknown }; message?: unknown; timing?: unknown; responseRequired?: unknown };
      };
      if (toolCall.type !== "toolCall" || toolCall.id !== input.sourceEntryId || toolCall.name !== "agent_send") continue;
      const target = toolCall.arguments?.target;
      const matchesTarget = input.inReplyToRequestId
        ? target?.request === input.inReplyToRequestId && toolCall.arguments?.timing === undefined
        : target?.agent === input.recipientAgentId && (toolCall.arguments?.timing ?? "steer") === input.deliveryTiming;
      if (typeof toolCall.arguments?.message !== "string" || !matchesTarget
        || (toolCall.arguments?.responseRequired === true) !== input.responseRequired
        || digestPayload(toolCall.arguments.message) !== input.payloadDigest) break;
      return toolCall.arguments.message;
    }
  }
  throw new WorkflowProtocolError("InvalidMessageSource", `Message ${input.messageId} does not match its canonical sender transcript entry`);
}

export function digestPayload(message: string): string { return createHash("sha256").update(message, "utf8").digest("hex"); }
export function signalDeliveryTiming(value: unknown): SignalDeliveryTiming {
  if (value === undefined || value === "steer") return "steer";
  if (value === "deferred") return "deferred";
  throw new TypeError("Signal delivery timing must be steer or deferred");
}
