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
import { CompletionRejectedError } from "../../pi-extension/subagents/protocol/completion-gate.ts";

describe("direct Signal Pi transcript projection", () => {
  it("returns terminating fused-completion results and structured blocker details", async () => {
    for (const blocked of [false, true]) {
      let tool: any;
      const params = { target: { agent: "recipient" }, message: "final", onAccepted: "complete" as const };
      registerAgentSendTool({ registerTool(value: unknown) { tool = value; }, sendMessage() {} } as never, {
        async waitUntilReady() {}, async startDirectSignalRouter() {},
        async sendDirectMessage() {
          if (blocked) throw new CompletionRejectedError([{ kind: "operation-dependency", dependencyId: "tool-1" }]);
          return { status: "accepted", messageId: "message-1", recipientAgentId: "recipient", acceptanceSequence: 1 };
        },
        async closeDirectSignalRouter() {},
      } as never);
      const result = await tool.execute("send-1", params, undefined, undefined, {
        sessionManager: { getSessionFile: () => "session.jsonl", getEntries: () => [{ message: { content: [{ type: "toolCall", id: "send-1", name: "agent_send", arguments: params }] } }] },
        shutdown() {},
      });
      if (blocked) assert.deepEqual(result.details, { code: "CompletionBlocked", blockers: [{ kind: "operation-dependency", dependencyId: "tool-1" }] });
      else assert.equal(result.terminate, true);
    }
  });

  it("rejects fused completion with a sibling tool before sending", async () => {
    let tool: any;
    let sends = 0;
    const params = { target: { agent: "recipient" }, message: "final", onAccepted: "complete" as const };
    registerAgentSendTool({ registerTool(value: unknown) { tool = value; }, sendMessage() {} } as never, {
      async waitUntilReady() {}, async startDirectSignalRouter() {}, async sendDirectMessage() { sends += 1; },
    } as never);
    await assert.rejects(() => tool.execute("send-1", params, undefined, undefined, {
      sessionManager: { getSessionFile: () => "session.jsonl", getEntries: () => [{ message: { content: [
        { type: "toolCall", id: "send-1", name: "agent_send", arguments: params },
        { type: "toolCall", id: "other-1", name: "read", arguments: {} },
      ] } }] }, shutdown() {},
    }), /sole tool call/);
    assert.equal(sends, 0);
  });

  it("returns terminal fused success when post-commit Router cleanup fails", async () => {
    let tool: any;
    let shutdown = 0;
    const params = { target: { agent: "recipient" }, message: "final", onAccepted: "complete" as const };
    registerAgentSendTool({ registerTool(value: unknown) { tool = value; }, sendMessage() {} } as never, {
      async waitUntilReady() {}, async startDirectSignalRouter() {},
      async sendDirectMessage() { return { status: "accepted", messageId: "message-1", recipientAgentId: "recipient", acceptanceSequence: 1 }; },
      async closeDirectSignalRouter() { throw new Error("cleanup failed"); },
    } as never);
    const originalWarn = console.warn;
    console.warn = () => {};
    try {
      const result = await tool.execute("send-1", params, undefined, undefined, {
        sessionManager: { getSessionFile: () => "session.jsonl", getEntries: () => [{ message: { content: [{ type: "toolCall", id: "send-1", name: "agent_send", arguments: params }] } }] },
        shutdown() { shutdown += 1; },
      });
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      assert.equal(result.terminate, true);
      assert.equal(shutdown, 1);
    } finally { console.warn = originalWarn; }
  });

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

  it("keeps startup behind reconciliation and shuts down terminal recovery while idle", async () => {
    let finishReconciliation!: () => void;
    const reconciliation = new Promise<void>((resolve) => { finishReconciliation = resolve; });
    let shutdown = 0;
    let terminalCompletion: (() => void) | undefined;
    const bootstrap = {
      async waitUntilReady() {},
      async startDirectSignalRouter(input: { onTerminalCompletion(): void }) {
        terminalCompletion = input.onTerminalCompletion;
      },
      async reconcilePendingDirectSignals(options: { waitForResolution?: boolean }) {
        assert.deepEqual(options, { waitForResolution: true });
        await reconciliation;
        terminalCompletion?.();
        return { terminalCompletion: true };
      },
    };
    const pending = startDirectSignalRouter({ sendMessage() {} } as never, bootstrap as never, {
      sessionManager: { getEntries: () => [] }, shutdown() { shutdown += 1; },
    } as never);
    let resolved = false;
    void pending.then(() => { resolved = true; });
    await Promise.resolve();
    assert.equal(resolved, false);
    assert.equal(shutdown, 0);
    finishReconciliation();
    await pending;
    assert.equal(shutdown, 1);
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

  it("projects Activation Intent alongside the detailed Request message", () => {
    const projected = projectInboxBatch({
      messages: [{
        kind: "request",
        messageId: "request-activation-1",
        senderAgentId: "sender",
        recipientAgentId: "recipient",
        activationIntent: "Resume audit",
        message: "Continue the audit from the saved checkpoint.",
        responseRequired: true,
      }],
    });

    assert.match(projected.content, /Activation Intent: Resume audit/);
    assert.equal(projected.details.messages[0].activationIntent, "Resume audit");
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

  it("projects a runtime-authored Request cancellation notice without fabricating an Agent sender", () => {
    const projected = projectInboxBatch({
      deliveryTiming: "steer",
      messages: [{
        kind: "protocol-notice",
        noticeKind: "request-cancelled",
        messageId: "notice-1",
        requestId: "request-1",
        recipientAgentId: "responder",
        deliveryTiming: "steer",
        message: "CANONICAL CANCELLATION PAYLOAD",
      }],
    });

    assert.match(projected.content, /Protocol Notice/);
    assert.match(projected.content, /Request ID: request-1/);
    assert.equal(projected.content.split("CANONICAL CANCELLATION PAYLOAD").length - 1, 1);
    assert.deepEqual(projected.details.messages, [{
      kind: "protocol-notice",
      noticeKind: "request-cancelled",
      messageId: "notice-1",
      requestId: "request-1",
      recipientAgentId: "responder",
      deliveryTiming: "steer",
    }]);
    assert.equal("senderAgentId" in projected.details.messages[0], false);
    assert.equal(llmVisibleContent(projected), projected.content);
  });

  it("validates only the agent_send variants legal at runtime", () => {
    for (const params of [
      { target: { agent: "agent" }, message: "signal", onAccepted: "continue" },
      { target: { agent: "agent" }, message: "signal", timing: "deferred", responseRequired: false, onAccepted: "complete" },
      { target: { agent: "agent" }, message: "request", timing: "steer", responseRequired: true, onAccepted: "continue" },
      { target: { agent: "agent" }, message: "activation", timing: "deferred", responseRequired: true, activation: { intent: "Resume for the next step" }, onAccepted: "continue" },
      { target: { request: "request" }, message: "answer", onAccepted: "complete" },
      { target: { request: "request" }, message: "answer", responseRequired: false, onAccepted: "continue" },
      { target: { request: "request" }, message: "answer and request", responseRequired: true, onAccepted: "continue" },
      { target: { spawn: { agent: "worker" } }, message: "initial request", responseRequired: true, activation: { intent: "Investigate the failure" }, onAccepted: "continue" },
      { target: { spawn: { agent: "worker", name: "Research helper" } }, message: "initial request", responseRequired: true, activation: { intent: "Investigate the failure" }, onAccepted: "continue" },
      { target: { spawn: { agent: "worker", delegationPolicy: "disabled" } }, message: "initial request", responseRequired: true, activation: { intent: "Investigate the failure" }, onAccepted: "continue" },
    ]) assert.equal(Value.Check(AgentSendParams, params), true, JSON.stringify(params));

    assert.equal(Value.Check(AgentSendParams, {
      target: { request: "request" }, message: "illegal caller timing", timing: "steer",
    }), false, "Request-target timing must be rejected by the registered schema");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "empty child", responseRequired: false,
    }), false, "Spawn must always create its initial Request");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "missing activation", responseRequired: true, onAccepted: "continue",
    }), false, "Spawn must carry an Activation Intent");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "illegal spawn timing", timing: "steer", responseRequired: true,
    }), false, "Spawn-target timing must be rejected by the registered schema");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker", delegationPolicy: "forbidden" } }, message: "illegal policy", responseRequired: true, onAccepted: "continue",
    }), false, "Spawn-target delegationPolicy must be one of the supported literals");
    assert.equal(Value.Check(AgentSendParams, {
      target: { agent: "agent" }, message: "ignored settlement", onAccepted: "settle",
    }), false, "Settle must not be silently accepted");
    assert.equal(Value.Check(AgentSendParams, {
      target: { agent: "agent" }, message: "request", responseRequired: true, onAccepted: "complete",
    }), false, "A terminal message cannot create a Response Requirement");
    assert.equal(Value.Check(AgentSendParams, {
      target: { spawn: { agent: "worker" } }, message: "request", responseRequired: true,
      activation: { intent: "   " }, onAccepted: "continue",
    }), false, "Activation Intent must contain non-whitespace text");
    assert.equal(Value.Check(AgentSendParams, {
      target: { agent: "agent" }, message: "signal", activation: { intent: "Forbidden" }, onAccepted: "continue",
    }), false, "Signals must reject Activation Intent");
    assert.equal(Value.Check(AgentSendParams, {
      target: { request: "request" }, message: "answer", activation: { intent: "Forbidden" }, onAccepted: "continue",
    }), false, "Answers must reject Activation Intent");
    assert.equal(Value.Check(AgentSendParams, {
      target: { request: "request" }, message: "answer and request", responseRequired: true, activation: { intent: "Forbidden" }, onAccepted: "continue",
    }), false, "Answer-plus-Request must reject Activation Intent");
    assert.equal(Value.Check(AgentSendParams, {
      target: { agent: "agent" }, message: "blank activation", responseRequired: true, activation: { intent: "" }, onAccepted: "continue",
    }), false, "Activation Intent must be non-empty");
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
      target: { spawn: { agent: "worker", name: "Worker", delegationPolicy: "autonomous" } },
      message: "Initial work.",
      responseRequired: true,
      activation: { intent: "Investigate initial work" },
      onAccepted: "continue" as const,
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
    assert.equal(calls[0].delegationPolicy, "autonomous");
    assert.equal(calls[0].activationIntent, "Investigate initial work");
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
      target: { spawn: { agent: "worker", name: "Worker", delegationPolicy: "disabled" } },
      message: "Initial work.",
      responseRequired: true,
      activation: { intent: "Resume recovered work" },
      onAccepted: "continue" as const,
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
      agent: "worker", name: "Worker", delegationPolicy: "disabled", activationIntent: "Resume recovered work", message: "Initial work.", sourceEntryId: "tool-1",
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
