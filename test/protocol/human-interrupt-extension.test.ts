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
