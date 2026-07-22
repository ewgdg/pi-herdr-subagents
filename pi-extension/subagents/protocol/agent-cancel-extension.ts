import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import type { WorkflowBootstrap } from "./workflow-bootstrap.ts";

export const AgentCancelParams = Type.Object({
  request: Type.String({ minLength: 1, description: "Unresolved Request ID owned by the current Agent" }),
}, { additionalProperties: false });

/** Register requester-authorized Request cancellation. Agent cancellation is intentionally out of scope. */
export function registerAgentCancelTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  enabled = true,
): void {
  if (!enabled) return;
  pi.registerTool({
    name: "agent_cancel",
    label: "Cancel Request",
    description:
      "Cancel an unresolved Request created by the current Agent. " +
      "Cancellation removes the response dependency but cannot roll back completed work or external side effects.",
    promptSnippet:
      "Cancel one unresolved Request you created. Undelivered work is suppressed; delivered work receives an actionable cancellation notice.",
    parameters: AgentCancelParams,
    async execute(_toolCallId, params, _signal, _onUpdate, context) {
      await workflowBootstrap.waitUntilReady(context);
      const receipt = await workflowBootstrap.cancelRequest(params.request);
      const delivery = receipt.delivery === "suppressed"
        ? " before delivery"
        : receipt.delivery === "notice-delivered"
          ? "; its cancellation notice is delivered"
          : "; its cancellation notice is queued";
      return {
        content: [{ type: "text", text: `Request ${receipt.requestId} cancelled${delivery}.` }],
        details: receipt,
      };
    },
  });
}
