import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Value } from "@sinclair/typebox/value";
import { AgentInspectParams, registerAgentInspectTool } from "../../pi-extension/subagents/protocol/agent-inspect-extension.ts";

describe("agent_inspect Pi extension", () => {
  it("accepts exactly one strict discriminated target form", () => {
    for (const target of [
      { agent: "00000000-0000-4000-8000-000000000001" },
      { request: "request-1" },
      { directChildren: true },
      { workflow: true },
    ]) assert.equal(Value.Check(AgentInspectParams, { target }), true, JSON.stringify(target));

    for (const target of [
      {},
      { agent: "a", request: "r" },
      { directChildren: false },
      { workflow: true, extra: true },
    ]) assert.equal(Value.Check(AgentInspectParams, { target }), false, JSON.stringify(target));
  });

  it("returns deterministic JSON in both model content and details without mutation hooks", async () => {
    let tool: any;
    const projection = { kind: "agent", agentId: "agent-1", state: { kind: "active" } };
    const bootstrap = {
      async waitUntilReady() {},
      inspectTarget(target: unknown) { assert.deepEqual(target, { agent: "agent-1" }); return projection; },
    };
    registerAgentInspectTool({ registerTool(value: unknown) { tool = value; } } as never, bootstrap as never);
    const result = await tool.execute("call-1", { target: { agent: "agent-1" } }, undefined, undefined, {});
    const json = JSON.stringify(projection, null, 2);
    assert.equal(result.content[0].text, json);
    assert.deepEqual(result.details, projection);
  });
});
