import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  confirmProjectedInboxBatches,
  projectInboxBatch,
  sessionContainsInboxMessage,
} from "../../pi-extension/subagents/protocol/direct-signal-extension.ts";

describe("direct Signal Pi transcript projection", () => {
  it("projects payload once while keeping identity and routing metadata structured", () => {
    const payload = "ONLY_ONCE_IN_ACTIONABLE_CONTENT";
    const projected = projectInboxBatch({
      messages: [{
        kind: "signal",
        messageId: "message-1",
        senderAgentId: "sender-1",
        recipientAgentId: "recipient-1",
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
