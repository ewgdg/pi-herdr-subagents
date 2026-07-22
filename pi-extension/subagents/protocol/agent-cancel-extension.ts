import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import type { WorkflowBootstrap } from "./workflow-bootstrap.ts";

const AgentCancellationTarget = Type.Object({
  agent: Type.String({ minLength: 1, description: "Workflow Agent ID whose current open activation will be cancelled" }),
}, { additionalProperties: false });
const RequestCancellationTarget = Type.Object({
  request: Type.String({ minLength: 1, description: "Unresolved Request ID owned by the current Agent" }),
}, { additionalProperties: false });

export const AgentCancelParams = Type.Object({
  target: Type.Union([AgentCancellationTarget, RequestCancellationTarget]),
}, { additionalProperties: false });

/** Register strict Request or single-activation cancellation. */
export function registerAgentCancelTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  enabled = true,
): void {
  if (!enabled) return;
  pi.registerTool({
    name: "agent_cancel",
    label: "Cancel Request or Activation",
    description:
      "Cancel either one unresolved Request created by the current Agent or one authorized open Subagent activation. " +
      "Activation cancellation requires Workflow Owner or direct Spawner authority, confirms exact process termination, and does not cascade to descendants.",
    promptSnippet:
      "Cancel one Request you created or one authorized Agent activation. Use the durable Workflow Agent ID, not a pane, display name, or legacy running ID.",
    parameters: AgentCancelParams,
    async execute(toolCallId, params, _signal, _onUpdate, context) {
      await workflowBootstrap.waitUntilReady(context);
      if ("request" in params.target) {
        const receipt = await workflowBootstrap.cancelRequest(params.target.request);
        const delivery = receipt.delivery === "suppressed"
          ? " before delivery"
          : receipt.delivery === "notice-delivered"
            ? "; its cancellation notice is delivered"
            : "; its cancellation notice is queued";
        return {
          content: [{ type: "text", text: `Request ${receipt.requestId} cancelled${delivery}.` }],
          details: receipt,
        };
      }

      const receipt = await workflowBootstrap.cancelActivation(params.target.agent, toolCallId);
      return {
        content: [{
          type: "text",
          text: `Activation ${receipt.activationId} cancelled for Agent ${receipt.targetAgentId}.`,
        }],
        details: receipt,
      };
    },
  });
}
