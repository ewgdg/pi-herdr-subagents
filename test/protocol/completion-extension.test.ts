import assert from "node:assert/strict";
import test from "node:test";
import { registerAgentCompleteTool } from "../../pi-extension/subagents/protocol/completion-extension.ts";
import { CompletionRejectedError } from "../../pi-extension/subagents/protocol/completion-gate.ts";

test("agent_complete returns a terminating result after durable completion", async () => {
  let tool: any;
  let shutdown = 0;
  registerAgentCompleteTool({ registerTool(value: unknown) { tool = value; } } as never, {
    async waitUntilReady() {},
    completeCurrentActivation() { return { activationId: "a", agentId: "worker", completedAtMs: 1, source: { kind: "standalone", toolCallId: "complete-1" } }; },
    async closeDirectSignalRouter() {},
  } as never);
  const result = await tool.execute("complete-1", {}, undefined, undefined, { sessionManager: { getEntries: () => [{ message: { content: [{ type: "toolCall", id: "complete-1", name: "agent_complete", arguments: {} }] } }] }, shutdown() { shutdown += 1; } });
  await new Promise<void>((resolve) => queueMicrotask(resolve));
  assert.equal(result.terminate, true);
  assert.equal(shutdown, 1);
});

test("agent_complete returns exact structured blocker details instead of throwing across Pi", async () => {
  let tool: any;
  const blockers = [{ kind: "operation-dependency" as const, dependencyId: "tool-1" }];
  registerAgentCompleteTool({ registerTool(value: unknown) { tool = value; } } as never, {
    async waitUntilReady() {},
    completeCurrentActivation() { throw new CompletionRejectedError(blockers); },
  } as never);
  const result = await tool.execute("complete-1", {}, undefined, undefined, { sessionManager: { getEntries: () => [{ message: { content: [{ type: "toolCall", id: "complete-1", name: "agent_complete", arguments: {} }] } }] }, shutdown() { assert.fail("blocked completion must not shut down"); } });
  assert.deepEqual(result.details, { code: "CompletionBlocked", blockers });
  assert.match(result.content[0].text, /operation-dependency/);
  assert.equal(result.terminate, undefined);
});

test("agent_complete rejects a sibling tool call before completion mutates", async () => {
  let tool: any;
  let completions = 0;
  registerAgentCompleteTool({ registerTool(value: unknown) { tool = value; } } as never, {
    async waitUntilReady() {}, completeCurrentActivation() { completions += 1; },
  } as never);
  await assert.rejects(() => tool.execute("complete-1", {}, undefined, undefined, {
    sessionManager: { getEntries: () => [{ message: { content: [
      { type: "toolCall", id: "ordinary-1", name: "read", arguments: {} },
      { type: "toolCall", id: "complete-1", name: "agent_complete", arguments: {} },
    ] } }] }, shutdown() {},
  }), /sole tool call/);
  assert.equal(completions, 0);
});

test("agent_complete remains terminating when Router cleanup fails after commit", async () => {
  let tool: any;
  let shutdown = 0;
  registerAgentCompleteTool({ registerTool(value: unknown) { tool = value; } } as never, {
    async waitUntilReady() {}, completeCurrentActivation() { return { activationId: "a" }; },
    async closeDirectSignalRouter() { throw new Error("cleanup failed"); },
  } as never);
  const originalWarn = console.warn;
  console.warn = () => {};
  try {
    const result = await tool.execute("complete-1", {}, undefined, undefined, {
      sessionManager: { getEntries: () => [{ message: { content: [{ type: "toolCall", id: "complete-1", name: "agent_complete", arguments: {} }] } }] },
      shutdown() { shutdown += 1; },
    });
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    assert.equal(result.terminate, true);
    assert.equal(shutdown, 1);
  } finally { console.warn = originalWarn; }
});
