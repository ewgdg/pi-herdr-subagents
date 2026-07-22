import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { randomUUID } from "node:crypto";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";

export const HUMAN_RESPONSE_ENTRY = "agent_human_interrupt_response";

export const AgentAskUserParams = Type.Object({
  question: Type.String({ minLength: 1, description: "Plain-text question for the human in this pane" }),
}, { additionalProperties: false });

interface PendingResponse {
  resolve(response: string): void;
}

export interface HumanInterruptResponseEntry {
  toolCallId: string;
  responseInputId: string;
  response: string;
}

/** Pane-local bridge: response identities are durable but never model-facing. */
export class HumanInterruptInputBridge {
  readonly #waiters = new Map<string, PendingResponse[]>();

  install(pi: ExtensionAPI, workflowBootstrap: WorkflowBootstrap): void {
    pi.on("input", async (event, ctx) => {
      if (event.source !== "interactive") return { action: "continue" };
      await workflowBootstrap.waitUntilReady(ctx);
      const interrupt = workflowBootstrap.currentHumanInterrupt();
      if (interrupt?.status !== "pending") return { action: "continue" };

      const toolCallId = interrupt.toolCallId;
      const responseInputId = randomUUID();
      // The custom entry is the canonical input. SQLite stores only its stable
      // identity, allowing recovery without copying answer payloads.
      pi.appendEntry(HUMAN_RESPONSE_ENTRY, { toolCallId, responseInputId, response: event.text });
      try {
        const bound = workflowBootstrap.bindHumanResponse(toolCallId, responseInputId);
        if (!bound) return { action: "handled" };
      } catch {
        // A cancellation/new interrupt race may win after append. This response
        // remains stale evidence and cannot bind to a newer tool call.
        return { action: "handled" };
      }
      this.#resolve(toolCallId, event.text);
      return { action: "handled" };
    });
  }

  async reconcile(context: ExtensionContext, workflowBootstrap: WorkflowBootstrap): Promise<void> {
    await workflowBootstrap.waitUntilReady(context);
    const entries = context.sessionManager.getEntries();
    for (const response of humanInterruptResponses(entries)) {
      const interrupt = workflowBootstrap.currentHumanInterrupt();
      if (interrupt?.status !== "pending" || interrupt.toolCallId !== response.toolCallId) continue;
      try {
        const bound = workflowBootstrap.bindHumanResponse(response.toolCallId, response.responseInputId);
        if (bound) this.#resolve(response.toolCallId, response.response);
      } catch {
        // A terminal or mismatched response is stale durable evidence, not a
        // reason to bind it to whatever interrupt happens to be current.
      }
    }
    for (const toolCallId of completedHumanInterruptToolCalls(entries)) {
      if (workflowBootstrap.confirmHumanResponseResult(toolCallId)) {
        workflowBootstrap.releaseDeferredSignals();
      }
    }
  }

  wait(toolCallId: string): Promise<string> {
    return new Promise((resolve) => {
      const waiters = this.#waiters.get(toolCallId) ?? [];
      waiters.push({ resolve });
      this.#waiters.set(toolCallId, waiters);
    });
  }

  #resolve(toolCallId: string, response: string): void {
    const waiters = this.#waiters.get(toolCallId) ?? [];
    this.#waiters.delete(toolCallId);
    for (const waiter of waiters) waiter.resolve(response);
  }
}

export function registerAgentAskUserTool(
  pi: ExtensionAPI,
  workflowBootstrap: WorkflowBootstrap,
  bridge: HumanInterruptInputBridge,
  enabled = true,
  actorRole: "ordinary" | "moderator" | "owner" = "ordinary",
): void {
  if (!enabled || actorRole !== "ordinary") return;
  pi.registerTool({
    name: "agent_ask_user",
    label: "Ask User",
    description: "Ask the human in this Subagent pane one plain-text question and wait for their answer.",
    promptSnippet: "Ask the human in this Subagent pane one plain-text question. Use the returned answer directly; unclear answers require a new agent_ask_user call.",
    parameters: AgentAskUserParams,
    async execute(toolCallId, params, _signal, _onUpdate, context) {
      if (!params.question.trim()) throw new Error("Human Interrupt question must not be empty");
      assertCanonicalAskUserSource(context.sessionManager.getEntries(), toolCallId, params.question);
      await workflowBootstrap.waitUntilReady(context);
      let interrupt = workflowBootstrap.beginHumanInterrupt(toolCallId);
      if (interrupt.status === "pending") {
        await bridge.reconcile(context, workflowBootstrap);
        const reconciled = workflowBootstrap.currentHumanInterrupt();
        if (reconciled?.toolCallId === toolCallId) interrupt = reconciled;
      }
      if (interrupt.status === "terminal") throw new Error("Human Interrupt is terminal");
      if (interrupt.status === "consumed") {
        throw new Error("Human Interrupt tool result is already durable");
      }

      const response = interrupt.status === "pending"
        ? await bridge.wait(toolCallId)
        : readBoundResponse(context.sessionManager.getEntries(), interrupt.responseInputId!);
      if (interrupt.status === "result-pending") {
        workflowBootstrap.resumeHumanResponseResult(toolCallId);
      } else {
        workflowBootstrap.prepareHumanResponseResult(toolCallId);
      }
      return {
        content: [{ type: "text", text: response }],
        details: {},
      };
    },
  });
}

export function humanInterruptResponses(entries: unknown[]): HumanInterruptResponseEntry[] {
  const responses: HumanInterruptResponseEntry[] = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const candidate = entry as { customType?: unknown; data?: unknown; details?: unknown };
    if (candidate.customType !== HUMAN_RESPONSE_ENTRY) continue;
    const value = candidate.data ?? candidate.details;
    if (!value || typeof value !== "object") continue;
    const response = value as Partial<HumanInterruptResponseEntry>;
    if (typeof response.toolCallId === "string" && typeof response.responseInputId === "string" && typeof response.response === "string") {
      responses.push(response as HumanInterruptResponseEntry);
    }
  }
  return responses;
}

export function findHumanInterruptResponse(entries: unknown[], responseInputId: string): string | undefined {
  return humanInterruptResponses(entries).find((response) => response.responseInputId === responseInputId)?.response;
}

export function completedHumanInterruptToolCalls(entries: unknown[]): string[] {
  const toolCallIds = new Set<string>();
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as { message?: unknown; role?: unknown; toolCallId?: unknown; toolName?: unknown; isError?: unknown };
    const message = record.message && typeof record.message === "object" ? record.message as typeof record : record;
    if (message.role === "toolResult" && message.toolName === "agent_ask_user"
      && message.isError !== true && typeof message.toolCallId === "string") {
      toolCallIds.add(message.toolCallId);
    }
  }
  return [...toolCallIds];
}

function readBoundResponse(entries: unknown[], responseInputId: string): string {
  const response = findHumanInterruptResponse(entries, responseInputId);
  if (response === undefined) throw new Error("Durably bound Human Interrupt input is missing from the pane transcript");
  return response;
}

function assertCanonicalAskUserSource(entries: unknown[], toolCallId: string, question: string): void {
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as { message?: { content?: unknown } };
    if (!Array.isArray(record.message?.content)) continue;
    for (const block of record.message.content) {
      const call = block as { type?: unknown; id?: unknown; name?: unknown; arguments?: { question?: unknown } };
      if (call.type === "toolCall" && call.id === toolCallId && call.name === "agent_ask_user"
        && call.arguments?.question === question) return;
    }
  }
  throw new Error(`agent_ask_user tool call ${toolCallId} is not durable in the Subagent transcript`);
}
