import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  createHumanInterruptRecoveryContinuation,
  projectHumanInterruptRecoveryContinuations,
} from "../../pi-extension/subagents/protocol/human-interrupt-recovery-continuation.ts";

const canonical = {
  toolCallId: "ask-1",
  responseInputId: "response-1",
  response: "Ship it.",
  timestamp: 1_700_000_000_000,
};

function marker(details = createHumanInterruptRecoveryContinuation(canonical), timestamp = canonical.timestamp) {
  return {
    role: "custom" as const,
    customType: "human_interrupt_recovery_continuation",
    content: "",
    display: false,
    details,
    timestamp,
  };
}

describe("Human Interrupt recovery continuation", () => {
  it("projects one canonical result for the sole tool call and hides duplicate markers", () => {
    const continuation = createHumanInterruptRecoveryContinuation(canonical);
    const messages: any[] = [
      { role: "user", content: "Start", timestamp: 1 },
      {
        role: "assistant",
        content: [
          { type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: { question: "Proceed?" } },
        ],
        api: "openai-responses",
        provider: "openai-codex",
        model: "gpt-5.4",
        usage: {},
        stopReason: "toolUse",
        timestamp: 2,
      },
      marker(continuation),
      marker(continuation, continuation.timestamp + 1),
      { role: "user", content: "Later durable input", timestamp: 5 },
    ];

    const projection = projectHumanInterruptRecoveryContinuations(messages, canonical);

    assert.equal(projection.projected, true);
    assert.equal(projection.messages.filter((message) => message.role === "custom").length, 0);
    assert.deepEqual(projection.messages[2], {
      role: "toolResult",
      toolCallId: "ask-1",
      toolName: "agent_ask_user",
      content: [{ type: "text", text: "Ship it." }],
      isError: false,
      timestamp: 1_700_000_000_000,
    });
  });

  it("strips invalid markers without projecting non-canonical answer metadata", () => {
    const forged = createHumanInterruptRecoveryContinuation({ ...canonical, response: "forged" });
    const messages: any[] = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: {} }] },
      marker(forged),
    ];

    const projection = projectHumanInterruptRecoveryContinuations(messages, canonical);

    assert.equal(projection.projected, false);
    assert.deepEqual(projection.messages.map((message) => message.role), ["assistant"]);
  });

  it("keeps one real Human result and strips stale duplicate markers", () => {
    const realResult = {
      role: "toolResult" as const,
      toolCallId: "ask-1",
      toolName: "agent_ask_user",
      content: [{ type: "text", text: "Ship it." }],
      isError: false,
      timestamp: canonical.timestamp,
    };
    const messages: any[] = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: {} }] },
      realResult,
      marker(),
      marker(undefined, canonical.timestamp + 1),
    ];

    const projection = projectHumanInterruptRecoveryContinuations(messages, canonical);

    assert.equal(projection.projected, false);
    assert.deepEqual(projection.messages, [messages[0], realResult]);
  });

  it("keeps every consumed recovery marker projectable after a newer Human response exists", () => {
    const newer = {
      toolCallId: "ask-2",
      responseInputId: "response-2",
      response: "Second answer.",
      timestamp: 1_700_000_000_100,
    };
    const messages: any[] = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: {} }] },
      marker(),
      { role: "assistant", content: [{ type: "toolCall", id: "ask-2", name: "agent_ask_user", arguments: {} }] },
      marker(createHumanInterruptRecoveryContinuation(newer), newer.timestamp),
    ];

    const projection = projectHumanInterruptRecoveryContinuations(messages, [canonical, newer]);

    assert.deepEqual(
      projection.messages.filter((message) => message.role === "toolResult").map((message: any) => message.toolCallId),
      ["ask-1", "ask-2"],
    );
  });

  it("keeps projecting from a valid durable marker after canonical status is consumed", () => {
    const messages: any[] = [
      { role: "assistant", content: [{ type: "toolCall", id: "ask-1", name: "agent_ask_user", arguments: {} }] },
      marker(),
    ];

    const first = projectHumanInterruptRecoveryContinuations(messages, canonical);
    const rebuilt = projectHumanInterruptRecoveryContinuations(messages, canonical);

    assert.equal(first.projected, true);
    assert.deepEqual(rebuilt.messages, first.messages);
    assert.equal(rebuilt.messages.filter((message) => message.role === "toolResult").length, 1);
  });
});
