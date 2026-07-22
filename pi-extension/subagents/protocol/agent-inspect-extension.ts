import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";

const AgentTarget = Type.Object({ agent: Type.String({ minLength: 1 }) }, { additionalProperties: false });
const RequestTarget = Type.Object({ request: Type.String({ minLength: 1 }) }, { additionalProperties: false });
const DirectChildrenTarget = Type.Object({ directChildren: Type.Literal(true) }, { additionalProperties: false });
const WorkflowTarget = Type.Object({ workflow: Type.Literal(true) }, { additionalProperties: false });

export const AgentInspectParams = Type.Object({
  target: Type.Union([AgentTarget, RequestTarget, DirectChildrenTarget, WorkflowTarget]),
}, { additionalProperties: false });

export function registerAgentInspectTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  enabled = true,
): void {
  if (!enabled) return;
  pi.registerTool({
    name: "agent_inspect",
    label: "Inspect Workflow",
    description: "Read currently persisted Agent or Request state, direct children, or the caller-owned Workflow. This tool is read-only and capability-filtered.",
    promptSnippet: "Inspect known durable Workflow state without waking or controlling Agents.",
    parameters: AgentInspectParams,
    async execute(_toolCallId, params, _signal, _onUpdate, context: ExtensionContext) {
      await workflowBootstrap.waitUntilReady(context);
      const projection = workflowBootstrap.inspectTarget(params.target);
      return {
        content: [{ type: "text", text: JSON.stringify(projection, null, 2) }],
        details: projection,
      };
    },
  });
}
