import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Value } from "@sinclair/typebox/value";
import { AgentCancelParams, registerAgentCancelTool } from "../../pi-extension/subagents/protocol/agent-cancel-extension.ts";

describe("agent_cancel Pi extension", () => {
  it("accepts only a strict Agent-or-Request target", () => {
    assert.equal(Value.Check(AgentCancelParams, { target: { request: "request-1" } }), true);
    assert.equal(Value.Check(AgentCancelParams, { target: { agent: "agent-1" } }), true);
    for (const value of [
      {},
      { target: {} },
      { target: { request: "" } },
      { target: { agent: "" } },
      { request: "request-1" },
      { agent: "agent-1" },
      { target: { request: "request-1", agent: "agent-1" } },
      { target: { request: "request-1", extra: true } },
      { target: { request: "request-1" }, extra: true },
    ]) assert.equal(Value.Check(AgentCancelParams, value), false, JSON.stringify(value));
  });

  it("cancels Requests and activations through their durable Workflow operations", async () => {
    let tool: any;
    const calls: string[] = [];
    const bootstrap = {
      async waitUntilReady() { calls.push("ready"); },
      async cancelRequest(requestId: string) {
        calls.push(requestId);
        return { requestId, status: "cancelled", delivery: "notice-accepted", noticeMessageId: "notice-1" };
      },
      async cancelActivation(agentId: string, sourceId: string) {
        calls.push(agentId, sourceId);
        return { operationId: "operation-1", targetAgentId: agentId, activationId: "activation-1", state: "committed" };
      },
    };
    registerAgentCancelTool({ registerTool(value: unknown) { tool = value; } } as never, bootstrap as never);
    assert.equal(tool.name, "agent_cancel");
    assert.match(tool.description, /Request/);
    assert.match(tool.description, /activation/i);

    const result = await tool.execute("cancel-call", { target: { request: "request-1" } }, undefined, undefined, {});
    assert.deepEqual(calls, ["ready", "request-1"]);
    assert.match(result.content[0].text, /Request request-1 cancelled/);
    assert.deepEqual(result.details, {
      requestId: "request-1",
      status: "cancelled",
      delivery: "notice-accepted",
      noticeMessageId: "notice-1",
    });

    const activation = await tool.execute("cancel-agent-call", { target: { agent: "agent-1" } }, undefined, undefined, {});
    assert.deepEqual(calls, ["ready", "request-1", "ready", "agent-1", "cancel-agent-call"]);
    assert.match(activation.content[0].text, /Activation activation-1 cancelled/);
    assert.equal(activation.details.state, "committed");
  });

  it("does not register when explicitly disabled", () => {
    let registrations = 0;
    registerAgentCancelTool({ registerTool() { registrations += 1; } } as never, {} as never, false);
    assert.equal(registrations, 0);
  });

  it("forwards a later tool-call identity after an in-doubt activation attempt", async () => {
    let tool: any;
    const sourceIds: string[] = [];
    const bootstrap = {
      async waitUntilReady() {},
      async cancelActivation(agentId: string, sourceId: string) {
        sourceIds.push(sourceId);
        if (sourceIds.length === 1) throw new Error("cancellation remains in doubt");
        return {
          operationId: "original-operation",
          sourceId: "first-tool-call",
          targetAgentId: agentId,
          activationId: "original-activation",
          state: "committed",
        };
      },
    };
    registerAgentCancelTool({ registerTool(value: unknown) { tool = value; } } as never, bootstrap as never);

    await assert.rejects(
      tool.execute("first-tool-call", { target: { agent: "agent-1" } }, undefined, undefined, {}),
      /in doubt/,
    );
    const result = await tool.execute(
      "later-tool-call",
      { target: { agent: "agent-1" } },
      undefined,
      undefined,
      {},
    );
    assert.deepEqual(sourceIds, ["first-tool-call", "later-tool-call"]);
    assert.equal(result.details.operationId, "original-operation");
    assert.equal(result.details.sourceId, "first-tool-call");
  });
});
