import type {
  ContextEvent,
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { randomUUID } from "node:crypto";
import {
  createHumanInterruptRecoveryContinuation,
  humanInterruptRecoveryProjectionId,
  projectHumanInterruptRecoveryContinuations,
  sendHumanInterruptRecoveryContinuation,
} from "./human-interrupt-recovery-continuation.ts";
import { WorkflowBootstrap } from "./workflow-bootstrap.ts";

export { HUMAN_INTERRUPT_RECOVERY_CONTINUATION } from "./human-interrupt-recovery-continuation.ts";
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
  timestamp?: number;
}

/** Pane-local bridge: response identities are durable but never model-facing. */
export class HumanInterruptInputBridge {
  readonly #waiters = new Map<string, PendingResponse[]>();
  readonly #executingToolCalls = new Set<string>();
  readonly #awaitingResultPersistence = new Set<string>();
  readonly #recoveryContinuationSends = new Set<string>();
  readonly #now: () => number;

  constructor(now: () => number = Date.now) {
    this.#now = now;
  }

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
      pi.appendEntry(HUMAN_RESPONSE_ENTRY, {
        toolCallId,
        responseInputId,
        response: event.text,
        timestamp: this.#now(),
      });
      try {
        const bound = workflowBootstrap.bindHumanResponse(toolCallId, responseInputId);
        if (!bound) return { action: "handled" };
      } catch {
        // A cancellation/new interrupt race may win after append. This response
        // remains stale evidence and cannot bind to a newer tool call.
        return { action: "handled" };
      }
      if (!this.#resolve(toolCallId, event.text) && !this.#executingToolCalls.has(toolCallId)) {
        this.#scheduleRecoveredHumanResult(ctx, workflowBootstrap, pi, toolCallId);
      }
      return { action: "handled" };
    });
  }

  async reconcile(
    context: ExtensionContext,
    workflowBootstrap: WorkflowBootstrap,
    pi?: Pick<ExtensionAPI, "sendMessage">,
  ): Promise<void> {
    await workflowBootstrap.waitUntilReady(context);
    const entries = context.sessionManager.getEntries();
    for (const toolCallId of completedHumanInterruptToolCalls(entries)) {
      this.#awaitingResultPersistence.delete(toolCallId);
      if (workflowBootstrap.confirmHumanResponseResult(toolCallId)) {
        workflowBootstrap.reevaluateInboxEligibility();
      }
    }
    for (const response of humanInterruptResponses(entries)) {
      const interrupt = workflowBootstrap.currentHumanInterrupt();
      if (interrupt?.status !== "pending" || interrupt.toolCallId !== response.toolCallId) continue;
      try {
        const bound = workflowBootstrap.bindHumanResponse(response.toolCallId, response.responseInputId);
        if (bound && !this.#resolve(response.toolCallId, response.response)
          && !this.#hasLocalExecution(response.toolCallId) && pi) {
          this.#scheduleRecoveredHumanResult(context, workflowBootstrap, pi, response.toolCallId);
        }
      } catch {
        // A terminal or mismatched response is stale durable evidence, not a
        // reason to bind it to whatever interrupt happens to be current.
      }
    }
    const interrupt = workflowBootstrap.currentHumanInterrupt();
    if (pi && !this.#hasLocalExecution(interrupt?.toolCallId)
      && (interrupt?.status === "response-bound" || interrupt?.status === "result-pending")) {
      this.#scheduleRecoveredHumanResult(context, workflowBootstrap, pi, interrupt.toolCallId);
    }
  }

  /**
   * Replace recovery markers only in outgoing provider context. The marker is
   * durable delivery evidence; Pi exposes no supported transcript mutation or
   * tool-result injection API to extensions.
   */
  projectRecoveryContinuationContext(
    event: ContextEvent,
    context: ExtensionContext,
    workflowBootstrap: WorkflowBootstrap,
  ): ContextEvent["messages"] | undefined {
    const interrupt = workflowBootstrap.currentHumanInterrupt();
    const canonicalAnswers = humanInterruptResponses(context.sessionManager.getEntries())
      .filter((response): response is HumanInterruptResponseEntry & { timestamp: number } => {
        if (response.timestamp === undefined) return false;
        const durableInterrupt = workflowBootstrap.humanInterruptByToolCall?.(response.toolCallId)
          ?? (response.toolCallId === interrupt?.toolCallId ? interrupt : undefined);
        return durableInterrupt?.responseInputId === response.responseInputId
          && (durableInterrupt.status === "result-pending" || durableInterrupt.status === "consumed");
      })
      .map((response) => ({
        toolCallId: response.toolCallId,
        responseInputId: response.responseInputId,
        response: response.response,
        timestamp: response.timestamp,
      }));
    const projection = projectHumanInterruptRecoveryContinuations(event.messages, canonicalAnswers);
    let confirmedCurrentProjection = interrupt?.status !== "result-pending";
    if (interrupt?.status === "result-pending"
      && projection.projectedToolCallIds.includes(interrupt.toolCallId)) {
      confirmedCurrentProjection = Boolean(workflowBootstrap.confirmHumanResponseResult(interrupt.toolCallId));
      if (confirmedCurrentProjection) workflowBootstrap.reevaluateInboxEligibility();
    }
    const currentProjectionId = interrupt?.responseInputId
      ? humanInterruptRecoveryProjectionId(interrupt.toolCallId, interrupt.responseInputId)
      : undefined;
    for (const projectionId of projection.observedProjectionIds) {
      if (projectionId !== currentProjectionId || confirmedCurrentProjection) {
        this.#recoveryContinuationSends.delete(projectionId);
      }
    }
    return projection.messages.length !== event.messages.length || projection.projected
      ? projection.messages
      : undefined;
  }

  wait(toolCallId: string): Promise<string> {
    return new Promise((resolve) => {
      const waiters = this.#waiters.get(toolCallId) ?? [];
      waiters.push({ resolve });
      this.#waiters.set(toolCallId, waiters);
    });
  }

  beginExecution(toolCallId: string): void {
    this.#executingToolCalls.add(toolCallId);
  }

  finishExecution(toolCallId: string): void {
    this.#executingToolCalls.delete(toolCallId);
  }

  awaitResultPersistence(toolCallId: string): void {
    this.#awaitingResultPersistence.add(toolCallId);
  }

  #scheduleRecoveredHumanResult(
    context: ExtensionContext,
    workflowBootstrap: WorkflowBootstrap,
    pi: Pick<ExtensionAPI, "sendMessage">,
    toolCallId: string,
  ): void {
    // Scheduling from before_agent_start/context would enqueue another marker
    // into the run that is already consuming the first one.
    if (context.isIdle?.() === false) return;
    const interrupt = workflowBootstrap.currentHumanInterrupt();
    if (!interrupt || interrupt.toolCallId !== toolCallId) return;
    if (interrupt.status !== "response-bound" && interrupt.status !== "result-pending") return;
    const entries = context.sessionManager.getEntries();
    if (completedHumanInterruptToolCalls(entries).includes(toolCallId)) return;
    const response = readBoundResponseEntry(entries, interrupt.responseInputId!);
    if (response.timestamp === undefined) {
      throw new Error("Durably bound Human Interrupt input is missing its canonical timestamp");
    }
    const canonical = {
      toolCallId,
      responseInputId: interrupt.responseInputId!,
      response: response.response,
      timestamp: response.timestamp,
    };
    const projectionId = createHumanInterruptRecoveryContinuation(canonical).projectionId;
    if (this.#recoveryContinuationSends.has(projectionId)) return;
    prepareHumanToolResult(workflowBootstrap, toolCallId, interrupt.status);
    sendHumanInterruptRecoveryContinuation(pi, canonical, this.#recoveryContinuationSends);
  }

  #hasLocalExecution(toolCallId: string | undefined): boolean {
    return Boolean(toolCallId) && (
      this.#executingToolCalls.has(toolCallId)
      || this.#awaitingResultPersistence.has(toolCallId)
    );
  }

  #resolve(toolCallId: string, response: string): boolean {
    const waiters = this.#waiters.get(toolCallId) ?? [];
    this.#waiters.delete(toolCallId);
    for (const waiter of waiters) waiter.resolve(response);
    return waiters.length > 0;
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
    description: "Ask the human in this Subagent pane one plain-text question and wait for their answer. This must be the assistant message's sole tool call.",
    promptSnippet: "Ask the human in this Subagent pane one plain-text question. Call agent_ask_user alone, with no sibling tool calls. Use the returned answer directly; unclear answers require a new agent_ask_user call.",
    parameters: AgentAskUserParams,
    async execute(toolCallId, params, _signal, _onUpdate, context) {
      if (!params.question.trim()) throw new Error("Human Interrupt question must not be empty");
      assertCanonicalAskUserSource(context.sessionManager.getEntries(), toolCallId, params.question);
      bridge.beginExecution(toolCallId);
      try {
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
        prepareHumanToolResult(workflowBootstrap, toolCallId, interrupt.status);
        bridge.awaitResultPersistence(toolCallId);
        return {
          content: [{ type: "text", text: response }],
          details: {},
        };
      } finally {
        bridge.finishExecution(toolCallId);
      }
    },
  });
}

function prepareHumanToolResult(
  workflowBootstrap: Pick<WorkflowBootstrap, "prepareHumanResponseResult" | "resumeHumanResponseResult">,
  toolCallId: string,
  status: "pending" | "response-bound" | "result-pending" | "consumed" | "terminal",
): void {
  if (status === "result-pending") {
    workflowBootstrap.resumeHumanResponseResult(toolCallId);
    return;
  }
  if (status === "response-bound" || status === "pending") {
    workflowBootstrap.prepareHumanResponseResult(toolCallId);
    return;
  }
  throw new Error(`Human Interrupt ${toolCallId} cannot produce a result from ${status}`);
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
    if (typeof response.toolCallId === "string" && typeof response.responseInputId === "string" && typeof response.response === "string"
      && (response.timestamp === undefined || (typeof response.timestamp === "number" && Number.isFinite(response.timestamp)))) {
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

function findHumanInterruptResponseEntry(
  entries: unknown[],
  responseInputId: string,
): HumanInterruptResponseEntry | undefined {
  return humanInterruptResponses(entries).find((response) => response.responseInputId === responseInputId);
}

function readBoundResponseEntry(entries: unknown[], responseInputId: string): HumanInterruptResponseEntry {
  const response = findHumanInterruptResponseEntry(entries, responseInputId);
  if (!response) throw new Error("Durably bound Human Interrupt input is missing from the pane transcript");
  return response;
}

function readBoundResponse(entries: unknown[], responseInputId: string): string {
  return readBoundResponseEntry(entries, responseInputId).response;
}

function assertCanonicalAskUserSource(entries: unknown[], toolCallId: string, question: string): void {
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as { message?: { content?: unknown } };
    if (!Array.isArray(record.message?.content)) continue;
    const toolCalls = record.message.content.filter((block) => {
      return Boolean(block) && typeof block === "object" && (block as { type?: unknown }).type === "toolCall";
    });
    const canonicalCall = toolCalls.find((block) => {
      const call = block as { id?: unknown; name?: unknown; arguments?: { question?: unknown } };
      return call.id === toolCallId && call.name === "agent_ask_user" && call.arguments?.question === question;
    });
    if (!canonicalCall) continue;
    if (toolCalls.length !== 1) {
      throw new Error(
        "agent_ask_user must be the sole tool call in its assistant message; retry agent_ask_user alone without sibling tool calls",
      );
    }
    return;
  }
  throw new Error(`agent_ask_user tool call ${toolCallId} is not durable in the Subagent transcript`);
}
