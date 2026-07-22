import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Value } from "@sinclair/typebox/value";
import { AgentCancelParams, registerAgentCancelTool } from "../../pi-extension/subagents/protocol/agent-cancel-extension.ts";

describe("agent_cancel Pi extension", () => {
  it("accepts only a strict Request target", () => {
    assert.equal(Value.Check(AgentCancelParams, { request: "request-1" }), true);
    for (const value of [
      {},
      { request: "" },
      { agent: "agent-1" },
      { request: "request-1", agent: "agent-1" },
      { request: "request-1", extra: true },
    ]) assert.equal(Value.Check(AgentCancelParams, value), false, JSON.stringify(value));
  });

  it("cancels through the durable Workflow runtime and returns structured receipt details", async () => {
    let tool: any;
    const calls: string[] = [];
    const bootstrap = {
      async waitUntilReady() { calls.push("ready"); },
      async cancelRequest(requestId: string) {
        calls.push(requestId);
        return { requestId, status: "cancelled", delivery: "notice-queued", noticeMessageId: "notice-1" };
      },
    };
    registerAgentCancelTool({ registerTool(value: unknown) { tool = value; } } as never, bootstrap as never);
    assert.equal(tool.name, "agent_cancel");
    assert.match(tool.description, /Request/);
    assert.doesNotMatch(tool.description, /activation/i);

    const result = await tool.execute("cancel-call", { request: "request-1" }, undefined, undefined, {});
    assert.deepEqual(calls, ["ready", "request-1"]);
    assert.match(result.content[0].text, /Request request-1 cancelled/);
    assert.deepEqual(result.details, {
      requestId: "request-1",
      status: "cancelled",
      delivery: "notice-queued",
      noticeMessageId: "notice-1",
    });
  });

  it("does not register when explicitly disabled", () => {
    let registrations = 0;
    registerAgentCancelTool({ registerTool() { registrations += 1; } } as never, {} as never, false);
    assert.equal(registrations, 0);
  });
});
