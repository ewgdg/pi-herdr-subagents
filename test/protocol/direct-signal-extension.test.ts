import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Value } from "@sinclair/typebox/value";
import { convertToLlm } from "@earendil-works/pi-coding-agent";
import {
  AgentSendParams,
  confirmProjectedInboxBatches,
  projectInboxBatch,
  registerAgentSendTool,
  sessionContainsInboxMessage,
  startDirectSignalRouter,
} from "../../pi-extension/subagents/protocol/direct-signal-extension.ts";

describe("direct Signal Pi transcript projection", () => {
  it("bridges a released Deferred batch through Pi as a non-aborting Steer", async () => {
    let projector: ((batch: Parameters<typeof projectInboxBatch>[0]) => void) | undefined;
    const workflowBootstrap = {
      async waitUntilReady() {},
      async startDirectSignalRouter(input: { projectInboxBatch(batch: Parameters<typeof projectInboxBatch>[0]): void }) {
        projector = input.projectInboxBatch;
      },
    };
    const sent: unknown[] = [];
    const pi = {
      sendMessage(message: unknown, options: unknown) { sent.push({ message, options }); },
    };
    const context = { sessionManager: { getEntries: () => [] } };

    await startDirectSignalRouter(pi as never, workflowBootstrap as never, context as never);
    projector?.({
      deliveryTiming: "deferred",
      messages: [{
        kind: "signal", messageId: "deferred-1", senderAgentId: "sender", recipientAgentId: "recipient",
        deliveryTiming: "deferred", message: "deferred payload",
      }],
    });

    assert.deepEqual(sent, [{
      message: projectInboxBatch({
        deliveryTiming: "deferred",
        messages: [{
          kind: "signal", messageId: "deferred-1", senderAgentId: "sender", recipientAgentId: "recipient",
          deliveryTiming: "deferred", message: "deferred payload",
        }],
      }),
      options: { triggerTurn: true, deliverAs: "steer" },
    }]);
  });

  it("projects payload once while keeping identity and routing metadata structured", () => {
    const payload = "ONLY_ONCE_IN_ACTIONABLE_CONTENT";
    const projected = projectInboxBatch({
      messages: [{
        kind: "signal",
        messageId: "message-1",
        senderAgentId: "sender-1",
        recipientAgentId: "recipient-1",
        deliveryTiming: "steer",
        message: payload,
      }],
    });

    assert.equal(projected.customType, "agent_inbox_batch");
    assert.equal(projected.content.split(payload).length - 1, 1);
    assert.equal(JSON.stringify(projected.details).includes(payload), false);
    assert.deepEqual(projected.details.messages, [{
      kind: "signal",
      messageId: "message-1",
      senderAgentId: "sender-1",
      recipientAgentId: "recipient-1",
      deliveryTiming: "steer",
    }]);
  });

  it("makes a Request's identity and response requirement visible to the model", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "request", messageId: "request-1", senderAgentId: "sender", recipientAgentId: "recipient",
        message: "REQUEST_PAYLOAD", responseRequired: true,
      }],
    });

    assert.match(projected.content, /Request ID: request-1/);
    assert.match(projected.content, /Response Requirement: Request ID request-1 requires one terminal Answer\./);
    assert.equal(llmVisibleContent(projected), projected.content);
  });

  it("makes an Answer's identity and Request correlation visible to the model", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "answer", messageId: "answer-1", senderAgentId: "sender", recipientAgentId: "recipient",
        message: "ANSWER_PAYLOAD", inReplyToRequestId: "request-1",
      }],
    });

    assert.match(projected.content, /Answer ID: answer-1/);
    assert.match(projected.content, /inReplyToRequestId: request-1/);
    assert.equal(llmVisibleContent(projected), projected.content);
  });

  it("makes an Answer-plus-Request's correlation and response requirement visible to the model", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "answer", messageId: "answer-request-1", senderAgentId: "sender", recipientAgentId: "recipient",
        message: "ANSWER_REQUEST_PAYLOAD", inReplyToRequestId: "request-1", responseRequired: true,
      }],
    });

    assert.match(projected.content, /Answer ID: answer-request-1/);
    assert.match(projected.content, /inReplyToRequestId: request-1/);
    assert.match(projected.content, /Response Requirement: New Request ID answer-request-1 requires one terminal Answer\./);
    assert.equal(llmVisibleContent(projected), projected.content);
  });

  it("validates only the agent_send variants legal at runtime", () => {
    for (const params of [
      { target: { agent: "agent" }, message: "signal" },
      { target: { agent: "agent" }, message: "signal", timing: "deferred", responseRequired: false },
      { target: { agent: "agent" }, message: "request", timing: "steer", responseRequired: true },
      { target: { request: "request" }, message: "answer" },
      { target: { request: "request" }, message: "answer", responseRequired: false },
      { target: { request: "request" }, message: "answer and request", responseRequired: true },
      { target: { spawn: { agent: "worker" } }, message: "initial request", responseRequired: true },
      { target: { spawn: { agent: "worker", name: "Research helper" } }, message: "initial request", responseRequired: true },
    ]) assert.equal(Value.Check(AgentSendParams, params), true, JSON.stringify(params));

    assert.equal(Value.Check(AgentSendParams, {
      target: { request: "request" }, message: "illegal caller timing", timing: "steer",
    }), false, "Request-target timing must be rejected by the registered schema");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "empty child", responseRequired: false,
    }), false, "Spawn must always create its initial Request");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "illegal spawn timing", timing: "steer", responseRequired: true,
    }), false, "Spawn-target timing must be rejected by the registered schema");
  });

  it("routes a legal spawn form through the prepared Spawned Initial Request launcher", async () => {
    let registered: any;
    const pi = { registerTool(tool: unknown) { registered = tool; } };
    const calls: Array<Record<string, unknown>> = [];
    registerAgentSendTool(pi as never, { async waitUntilReady() {} } as never, true, {
      async spawnInitialRequest(input) {
        calls.push(input);
        return { status: "delivered", messageId: "request-1", recipientAgentId: "child-1", acceptanceSequence: 1 };
      },
    });

    const spawnParams = {
      target: { spawn: { agent: "worker", name: "Worker" } },
      message: "Initial work.",
      responseRequired: true,
    };
    const result = await registered.execute("tool-1", spawnParams, undefined, undefined, {
      sessionManager: {
        getEntries: () => [{ message: { content: [{
          type: "toolCall", id: "tool-1", name: "agent_send", arguments: spawnParams,
        }] } }],
      },
    });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].agent, "worker");
    assert.equal(calls[0].name, "Worker");
    assert.equal(calls[0].message, "Initial work.");
    assert.match(calls[0].messageId as string, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    assert.notEqual(calls[0].messageId, calls[0].sourceEntryId);
    assert.equal(calls[0].sourceEntryId, "tool-1");
    assert.match(result.content[0].text, /Request request-1 delivered/);
  });

  it("returns a durable spawned-request reconciliation without invoking the launcher", async () => {
    let registered: any;
    let launchAttempts = 0;
    const reconciliationInputs: Array<Record<string, unknown>> = [];
    const pi = { registerTool(tool: unknown) { registered = tool; } };
    registerAgentSendTool(pi as never, { async waitUntilReady() {} } as never, true, {
      async reconcileSpawnedInitialRequest(input) {
        reconciliationInputs.push(input);
        return { status: "delivered", messageId: "original-request", recipientAgentId: "original-child", acceptanceSequence: 1 };
      },
      async spawnInitialRequest() {
        launchAttempts += 1;
        throw new Error("A reconciled Spawned Initial Request must not launch a child");
      },
    });

    const spawnParams = {
      target: { spawn: { agent: "worker", name: "Worker" } },
      message: "Initial work.",
      responseRequired: true,
    };
    const result = await registered.execute("tool-1", spawnParams, undefined, undefined, {
      sessionManager: {
        getEntries: () => [{ message: { content: [{
          type: "toolCall", id: "tool-1", name: "agent_send", arguments: spawnParams,
        }] } }],
      },
    });

    assert.equal(launchAttempts, 0);
    assert.deepEqual(reconciliationInputs.map(({ context: _context, ...input }) => input), [{
      agent: "worker", name: "Worker", message: "Initial work.", sourceEntryId: "tool-1",
    }]);
    assert.equal(result.details.messageId, "original-request");
  });

  it("recognizes durable Inbox Batch evidence by Message Identity", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "signal",
        messageId: "message-2",
        senderAgentId: "sender-2",
        recipientAgentId: "recipient-2",
        message: "payload",
      }],
    });
    const entries = [{ type: "custom_message", ...projected }];

    assert.equal(sessionContainsInboxMessage(entries, "message-2"), true);
    assert.equal(sessionContainsInboxMessage([{ role: "custom", ...projected }], "message-2"), true);
    assert.equal(sessionContainsInboxMessage(entries, "message-missing"), false);
  });

  it("projects every message in an ordered Inbox Batch with metadata-only details", () => {
    const projected = projectInboxBatch({
      deliveryTiming: "deferred",
      messages: [
        {
          kind: "signal", messageId: "first", senderAgentId: "sender", recipientAgentId: "recipient",
          deliveryTiming: "deferred", message: "FIRST_PAYLOAD",
        },
        {
          kind: "signal", messageId: "second", senderAgentId: "sender", recipientAgentId: "recipient",
          deliveryTiming: "steer", message: "SECOND_PAYLOAD",
        },
      ],
    });

    assert.ok(projected.content.indexOf("FIRST_PAYLOAD") < projected.content.indexOf("SECOND_PAYLOAD"));
    assert.equal(projected.content.split("FIRST_PAYLOAD").length - 1, 1);
    assert.equal(projected.content.split("SECOND_PAYLOAD").length - 1, 1);
    assert.deepEqual(projected.details.messages.map((message) => message.messageId), ["first", "second"]);
    assert.equal(JSON.stringify(projected.details).includes("PAYLOAD"), false);
  });

  it("commits delivery only from durable projected Message Identities", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "signal",
        messageId: "message-3",
        senderAgentId: "sender-3",
        recipientAgentId: "recipient-3",
        message: "payload",
      }],
    });
    const confirmed: string[] = [];
    const workflowBootstrap = {
      confirmDirectSignalDelivery(messageId: string) {
        confirmed.push(messageId);
        return messageId === "message-3";
      },
    };

    assert.equal(confirmProjectedInboxBatches(
      workflowBootstrap as never,
      [{ type: "custom_message", ...projected }],
    ), 1);
    assert.deepEqual(confirmed, ["message-3"]);
  });
});

function llmVisibleContent(projected: ReturnType<typeof projectInboxBatch>): string {
  const [message] = convertToLlm([{
    role: "custom",
    ...projected,
    timestamp: 0,
  }] as never) as Array<{ content: Array<{ type: string; text?: string }> }>;
  return message.content[0].text!;
}
