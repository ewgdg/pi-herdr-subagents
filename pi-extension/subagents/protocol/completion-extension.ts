import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import type { WorkflowBootstrap } from "./workflow-bootstrap.ts";
import { CompletionRejectedError } from "./completion-gate.ts";
import { assertSoleToolCall } from "./direct-signal-transcript.ts";

export function registerAgentCompleteTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  enabled = true,
): void {
  if (!enabled) return;
  pi.registerTool({
    name: "agent_complete",
    label: "Complete Activation",
    description: "Complete the current Subagent activation after all protocol obligations are resolved. Takes no arguments.",
    promptSnippet: "Complete this Subagent activation only after all useful output has been accepted and all durable obligations are resolved.",
    promptGuidelines: [
      "Call agent_complete only as the sole final tool action; do not place sibling tool calls beside it.",
      "After agent_complete succeeds, do not emit another assistant response.",
    ],
    parameters: Type.Object({}, { additionalProperties: false }),
    async execute(toolCallId, _params, _signal, _onUpdate, context) {
      await workflowBootstrap.waitUntilReady(context);
      assertSoleToolCall(context.sessionManager.getEntries(), toolCallId);
      let completion;
      try {
        completion = workflowBootstrap.completeCurrentActivation({ kind: "standalone", toolCallId });
      } catch (error) {
        if (error instanceof CompletionRejectedError) return completionBlockedResult(error);
        throw error;
      }
      try {
        await workflowBootstrap.closeDirectSignalRouter();
      } catch (error) {
        console.warn("Activation completed, but local Router cleanup failed", error);
      }
      queueMicrotask(() => context.shutdown());
      return {
        content: [{ type: "text", text: "Activation completed. Session will shut down gracefully." }],
        details: completion,
        terminate: true,
      };
    },
  });
}

export function completionBlockedResult(error: CompletionRejectedError) {
  const details = { code: "CompletionBlocked" as const, blockers: error.blockers };
  return {
    content: [{ type: "text" as const, text: `Completion blocked:\n${details.blockers.map((blocker) => `- ${JSON.stringify(blocker)}`).join("\n")}` }],
    details,
  };
}
