import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Value } from "@sinclair/typebox/value";
import {
  AgentAskUserParams,
  completedHumanInterruptToolCalls,
  findHumanInterruptResponse,
  HumanInterruptInputBridge,
  registerAgentAskUserTool,
} from "../../pi-extension/subagents/protocol/human-interrupt-extension.ts";

describe("Human Interrupt Pi extension", () => {
  it("accepts only one non-empty plain-text question", () => {
    assert.equal(Value.Check(AgentAskUserParams, { question: "Proceed?" }), true);
    assert.equal(Value.Check(AgentAskUserParams, {}), false);
    assert.equal(Value.Check(AgentAskUserParams, { question: "", choices: ["yes"] }), false);
  });

  it("binds pane-local input to its original tool call and confirms only persisted tool results", async () => {
    let tool: any;
    let inputHandler: any;
    let status: "pending" | "response-bound" | "result-pending" | "consumed" = "pending";
    let responseInputId: string | undefined;
    const entries: any[] = [{ message: { content: [{
      type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: { question: "Need a decision?" },
    }] } }];
    const bound: Array<[string, string]> = [];
    const prepared: string[] = [];
    const confirmed: string[] = [];
    let releases = 0;
    const bootstrap = {
      async waitUntilReady() {},
      currentHumanInterrupt() {
        return { toolCallId: "ask-1", status, ...(responseInputId ? { responseInputId } : {}) };
      },
      beginHumanInterrupt(toolCallId: string) {
        assert.equal(toolCallId, "ask-1");
        return { toolCallId, status, ...(responseInputId ? { responseInputId } : {}) };
      },
      bindHumanResponse(toolCallId: string, inputId: string) {
        bound.push([toolCallId, inputId]);
        if (toolCallId !== "ask-1" || status !== "pending") return undefined;
        status = "response-bound";
        responseInputId = inputId;
        return { toolCallId, status, responseInputId: inputId };
      },
      prepareHumanResponseResult(toolCallId: string) {
        prepared.push(toolCallId);
        status = "result-pending";
        return { toolCallId, status, responseInputId };
      },
      confirmHumanResponseResult(toolCallId: string) {
        confirmed.push(toolCallId);
        if (status !== "result-pending") return undefined;
        status = "consumed";
        return { toolCallId, status, responseInputId };
      },
      releaseDeferredSignals() { releases += 1; },
    };
    const pi = {
      on(event: string, handler: unknown) { if (event === "input") inputHandler = handler; },
      registerTool(value: unknown) { tool = value; },
      appendEntry(customType: string, data: unknown) { entries.push({ customType, data }); },
    };
    const bridge = new HumanInterruptInputBridge();
    bridge.install(pi as never, bootstrap as never);
    registerAgentAskUserTool(pi as never, bootstrap as never, bridge);

    const resultPromise = tool.execute("ask-1", { question: "Need a decision?" }, undefined, undefined, {
      sessionManager: { getEntries: () => entries },
    });
    const inputResult = await inputHandler({ source: "interactive", text: "Ship it." }, {
      sessionManager: { getEntries: () => entries },
    });
    const result = await resultPromise;

    assert.deepEqual(inputResult, { action: "handled" });
    assert.equal(bound.length, 1);
    assert.equal(bound[0][0], "ask-1");
    assert.deepEqual(prepared, ["ask-1"]);
    assert.equal(status, "result-pending");
    assert.equal(result.content[0].text, "Ship it.");
    assert.equal(JSON.stringify(entries[1]).includes("ask-1"), true, "identity remains in non-LLM durable input metadata");

    entries.push({ message: { role: "toolResult", toolCallId: "ask-1", toolName: "agent_ask_user", isError: false } });
    await bridge.reconcile({ sessionManager: { getEntries: () => entries } } as never, bootstrap as never);
    assert.deepEqual(confirmed, ["ask-1"]);
    assert.equal(status, "consumed");
    assert.equal(releases, 1);
  });

  it("reconciles a transcript-appended response after a bind crash", async () => {
    const entries = [{
      customType: "agent_human_interrupt_response",
      data: { toolCallId: "ask-1", responseInputId: "input-1", response: "recovered" },
    }];
    const calls: Array<[string, string]> = [];
    const bridge = new HumanInterruptInputBridge();
    await bridge.reconcile({ sessionManager: { getEntries: () => entries } } as never, {
      async waitUntilReady() {},
      currentHumanInterrupt() { return { toolCallId: "ask-1", status: "pending" }; },
      bindHumanResponse(toolCallId: string, inputId: string) {
        calls.push([toolCallId, inputId]);
        return { toolCallId, status: "response-bound", responseInputId: inputId };
      },
      confirmHumanResponseResult() { return undefined; },
      releaseDeferredSignals() {},
    } as never);
    assert.deepEqual(calls, [["ask-1", "input-1"]]);
  });

  it("keeps a recovered Human answer result-pending until context observes its hidden projection", async () => {
    let inputHandler: any;
    let status: "pending" | "response-bound" | "result-pending" | "consumed" = "pending";
    let responseInputId: string | undefined;
    const entries: any[] = [{ message: { content: [{
      type: "toolCall", id: "ask-recovered", name: "agent_ask_user", arguments: { question: "Recover after failure?" },
    }] } }];
    const lifecycle: string[] = [];
    const continuations: any[] = [];
    const bootstrap = {
      async waitUntilReady() {},
      currentHumanInterrupt() {
        return { toolCallId: "ask-recovered", status, ...(responseInputId ? { responseInputId } : {}) };
      },
      bindHumanResponse(_toolCallId: string, inputId: string) {
        if (status !== "pending") return undefined;
        status = "response-bound";
        responseInputId = inputId;
        return { toolCallId: "ask-recovered", status, responseInputId };
      },
      prepareHumanResponseResult(toolCallId: string) {
        lifecycle.push(`prepare:${toolCallId}`);
        status = "result-pending";
        return { toolCallId, status, responseInputId };
      },
      resumeHumanResponseResult(toolCallId: string) {
        lifecycle.push(`resume:${toolCallId}`);
        return { toolCallId, status, responseInputId };
      },
      confirmHumanResponseResult(toolCallId: string) {
        lifecycle.push(`confirm:${toolCallId}`);
        if (status !== "result-pending") return undefined;
        status = "consumed";
        return { toolCallId, status, responseInputId };
      },
      releaseDeferredSignals() { lifecycle.push("released"); },
    };
    const pi = {
      on(event: string, handler: unknown) { if (event === "input") inputHandler = handler; },
      appendEntry(customType: string, data: unknown) { entries.push({ customType, data }); },
      sendMessage(message: unknown, options: unknown) { continuations.push({ message, options }); },
    };
    const context = { sessionManager: { getEntries: () => entries } };
    const bridge = new HumanInterruptInputBridge(() => 1_700_000_000_000);
    bridge.install(pi as never, bootstrap as never);

    assert.deepEqual(await inputHandler({ source: "interactive", text: "yes" }, context), { action: "handled" });
    assert.deepEqual(lifecycle, ["prepare:ask-recovered"]);
    assert.equal(status, "result-pending", "void sendMessage invocation is not a delivery acknowledgement");
    assert.equal(entries.some((entry) => entry.message?.role === "toolResult"), false,
      "read-only SessionManager must never be mutated");
    assert.equal(continuations.length, 1);
    const sent = continuations[0];
    assert.deepEqual(sent.options, { triggerTurn: true, deliverAs: "steer" });
    assert.equal(sent.message.customType, "human_interrupt_recovery_continuation");
    assert.deepEqual(sent.message.details, {
      projectionId: `human-interrupt-result:ask-recovered:${responseInputId}`,
      toolCallId: "ask-recovered",
      responseInputId,
      response: "yes",
      timestamp: 1_700_000_000_000,
    });

    const marker = { role: "custom", ...sent.message, timestamp: 1_700_000_000_001 };
    const messages = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-recovered", name: "agent_ask_user", arguments: {} }] },
      marker,
    ] as any[];
    const projected = bridge.projectRecoveryContinuationContext(
      { messages } as never,
      context as never,
      bootstrap as never,
    );
    assert.equal(projected?.filter((message) => message.role === "toolResult").length, 1);
    assert.equal((projected![1] as any).content[0].text, "yes");
    assert.deepEqual(lifecycle, ["prepare:ask-recovered", "confirm:ask-recovered", "released"]);
    assert.equal(status, "consumed");
  });

  it("keeps one recovered Human marker fenced through before-start and context projection", async () => {
    let idle = true;
    let status: "response-bound" | "result-pending" | "consumed" = "response-bound";
    const events: string[] = [];
    const sent: any[] = [];
    const entries: any[] = [
      { message: { role: "assistant", content: [{
        type: "toolCall", id: "ask-fenced", name: "agent_ask_user", arguments: { question: "Continue?" },
      }] } },
      { customType: "agent_human_interrupt_response", data: {
        toolCallId: "ask-fenced",
        responseInputId: "input-fenced",
        response: "yes",
        timestamp: 1_700_000_000_050,
      } },
    ];
    const bootstrap = {
      async waitUntilReady() {},
      currentHumanInterrupt() {
        return { toolCallId: "ask-fenced", status, responseInputId: "input-fenced" };
      },
      bindHumanResponse() { assert.fail("the response is already bound"); },
      prepareHumanResponseResult() { events.push("prepare"); status = "result-pending"; },
      resumeHumanResponseResult() { events.push("resume"); },
      confirmHumanResponseResult() { events.push("confirm"); status = "consumed"; return { status }; },
      releaseDeferredSignals() { events.push("release"); },
    };
    const context = {
      isIdle: () => idle,
      sessionManager: { getEntries: () => entries },
    };
    const pi = {
      sendMessage(message: unknown, options: unknown) {
        events.push("send");
        sent.push({ message, options });
      },
    };
    const bridge = new HumanInterruptInputBridge();

    await bridge.reconcile(context as never, bootstrap as never, pi as never);
    await Promise.resolve();
    idle = false;
    events.push("before_agent_start");
    await bridge.reconcile(context as never, bootstrap as never, pi as never);
    events.push("context");
    await bridge.reconcile(context as never, bootstrap as never, pi as never);

    const projected = bridge.projectRecoveryContinuationContext({ messages: [
      entries[0].message,
      { role: "custom", ...sent[0].message, timestamp: 1_700_000_000_051 },
    ] } as never, context as never, bootstrap as never);
    events.push("provider");

    assert.equal(sent.length, 1, "one marker must schedule exactly one model turn");
    assert.equal(projected?.filter((message) => message.role === "toolResult").length, 1);
    assert.deepEqual(events, [
      "prepare",
      "send",
      "before_agent_start",
      "context",
      "confirm",
      "release",
      "provider",
    ]);
  });

  it("retries a result-pending Human projection after send failure and process restart", async () => {
    let attempts = 0;
    const entries: any[] = [
      { message: { content: [{
        type: "toolCall", id: "ask-result-pending", name: "agent_ask_user", arguments: { question: "Recover bound result?" },
      }] } },
      { customType: "agent_human_interrupt_response", data: {
        toolCallId: "ask-result-pending",
        responseInputId: "response-bound-before-failure",
        response: "continue",
        timestamp: 1_700_000_000_100,
      } },
    ];
    const bootstrap = {
      async waitUntilReady() {},
      currentHumanInterrupt() {
        return { toolCallId: "ask-result-pending", status: "result-pending", responseInputId: "response-bound-before-failure" };
      },
      bindHumanResponse() { assert.fail("bound input must not be rebound"); },
      prepareHumanResponseResult() { assert.fail("result-pending input must not be prepared again"); },
      resumeHumanResponseResult() {},
      confirmHumanResponseResult() { assert.fail("send invocation must not consume the result"); },
      releaseDeferredSignals() {},
    };
    const context = { sessionManager: { getEntries: () => entries } };
    const failingPi = { sendMessage() { attempts += 1; throw new Error("injected send failure"); } };
    await new HumanInterruptInputBridge().reconcile(context as never, bootstrap as never, failingPi as never);

    const restartedPi = { sendMessage() { attempts += 1; } };
    await new HumanInterruptInputBridge().reconcile(context as never, bootstrap as never, restartedPi as never);
    assert.equal(attempts, 2, "a fresh runtime must retry the durable result-pending projection");
    assert.equal(entries.some((entry) => entry.message?.role === "toolResult"), false);
  });

  it("strips a terminal Human marker without reviving its stale result", () => {
    const entries: any[] = [{
      customType: "agent_human_interrupt_response",
      data: {
        toolCallId: "ask-cancelled",
        responseInputId: "input-cancelled",
        response: "too late",
        timestamp: 1_700_000_000_300,
      },
    }];
    const marker = {
      role: "custom",
      customType: "human_interrupt_recovery_continuation",
      content: "",
      display: false,
      details: {
        projectionId: "human-interrupt-result:ask-cancelled:input-cancelled",
        toolCallId: "ask-cancelled",
        responseInputId: "input-cancelled",
        response: "too late",
        timestamp: 1_700_000_000_300,
      },
    };
    const messages = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-cancelled", name: "agent_ask_user", arguments: {} }] },
      marker,
    ] as any[];
    const bridge = new HumanInterruptInputBridge();

    const projected = bridge.projectRecoveryContinuationContext(
      { messages } as never,
      { sessionManager: { getEntries: () => entries } } as never,
      {
        currentHumanInterrupt() { return { toolCallId: "ask-new", status: "pending" }; },
        humanInterruptByToolCall() { return { toolCallId: "ask-cancelled", status: "terminal" }; },
        confirmHumanResponseResult() { assert.fail("terminal result must not be consumed"); },
        releaseDeferredSignals() {},
      } as never,
    );

    assert.deepEqual(projected?.map((message) => message.role), ["assistant"]);
  });

  it("replays a result-pending Human Interrupt before returning its recovered result", async () => {
    let tool: any;
    const resumed: string[] = [];
    const entries = [
      { message: { content: [{ type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: { question: "Recover?" } }] } },
      { customType: "agent_human_interrupt_response", data: { toolCallId: "ask-1", responseInputId: "input-1", response: "yes" } },
    ];
    const pi = { registerTool(value: unknown) { tool = value; } };
    registerAgentAskUserTool(pi as never, {
      async waitUntilReady() {},
      beginHumanInterrupt() { return { toolCallId: "ask-1", status: "result-pending", responseInputId: "input-1" }; },
      resumeHumanResponseResult(toolCallId: string) { resumed.push(toolCallId); },
    } as never, new HumanInterruptInputBridge());

    const result = await tool.execute("ask-1", { question: "Recover?" }, undefined, undefined, {
      sessionManager: { getEntries: () => entries },
    });
    assert.deepEqual(resumed, ["ask-1"]);
    assert.equal(result.content[0].text, "yes");
  });

  it("rejects agent_ask_user siblings before creating Human state, then accepts a sole call", async () => {
    let tool: any;
    let humanStatesCreated = 0;
    const entries: any[] = [{ message: { role: "assistant", content: [
      { type: "toolCall", id: "ask-batch", name: "agent_ask_user", arguments: { question: "Choose?" } },
      { type: "toolCall", id: "read-sibling", name: "read", arguments: { path: "README.md" } },
    ] } }];
    const bootstrap = {
      async waitUntilReady() {},
      beginHumanInterrupt() {
        humanStatesCreated += 1;
        return { toolCallId: "ask-batch", status: "response-bound", responseInputId: "input-sole" };
      },
      prepareHumanResponseResult() {},
    };
    registerAgentAskUserTool({ registerTool(value: unknown) { tool = value; } } as never,
      bootstrap as never, new HumanInterruptInputBridge());
    const context = { sessionManager: { getEntries: () => entries } };

    await assert.rejects(
      tool.execute("ask-batch", { question: "Choose?" }, undefined, undefined, context),
      /sole tool call.*retry.*alone/i,
    );
    assert.equal(humanStatesCreated, 0, "a rejected parallel batch must not create Human or DECIDE state");

    entries[0].message.content = [entries[0].message.content[0]];
    entries.push({ customType: "agent_human_interrupt_response", data: {
      toolCallId: "ask-batch", responseInputId: "input-sole", response: "proceed",
    } });
    const result = await tool.execute("ask-batch", { question: "Choose?" }, undefined, undefined, context);
    assert.equal(result.content[0].text, "proceed");
    assert.equal(humanStatesCreated, 1);
  });

  it("does not rebind a stale response to a newer interrupt", async () => {
    let inputHandler: any;
    let current = "ask-1";
    const bound: string[] = [];
    const pi = {
      on(event: string, handler: unknown) { if (event === "input") inputHandler = handler; },
      appendEntry() { current = "ask-2"; },
    };
    const bridge = new HumanInterruptInputBridge();
    bridge.install(pi as never, {
      async waitUntilReady() {},
      currentHumanInterrupt() { return { toolCallId: current, status: "pending" }; },
      bindHumanResponse(toolCallId: string) {
        bound.push(toolCallId);
        return toolCallId === "ask-1" && current === "ask-1" ? {} : undefined;
      },
    } as never);
    const result = await inputHandler({ source: "interactive", text: "late" }, {});
    assert.deepEqual(result, { action: "handled" });
    assert.deepEqual(bound, ["ask-1"]);
  });

  it("does not register agent_ask_user for Moderator or Owner contexts", () => {
    for (const actorRole of ["moderator", "owner"] as const) {
      let registered = false;
      registerAgentAskUserTool({ registerTool() { registered = true; } } as never, {} as never, new HumanInterruptInputBridge(), true, actorRole);
      assert.equal(registered, false, actorRole);
    }
  });

  it("reads response and result evidence only from canonical durable entries", () => {
    const entries = [
      { customType: "agent_human_interrupt_response", data: { toolCallId: "ask-a", responseInputId: "input-a", response: "first" } },
      { customType: "agent_human_interrupt_response", data: { toolCallId: "ask-b", responseInputId: "input-b", response: "second" } },
      { message: { role: "toolResult", toolCallId: "ask-b", toolName: "agent_ask_user", isError: false } },
    ];
    assert.equal(findHumanInterruptResponse(entries, "input-b"), "second");
    assert.equal(findHumanInterruptResponse(entries, "missing"), undefined);
    assert.deepEqual(completedHumanInterruptToolCalls(entries), ["ask-b"]);
  });
});
