import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  confirmProjectedInboxBatches,
  projectInboxBatch,
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
