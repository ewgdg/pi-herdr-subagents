import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  AUTOMATIC_RECOVERY_CONTINUATION,
  createAutomaticRecoveryContinuation,
  projectAutomaticRecoveryContinuationContext,
} from "../../pi-extension/subagents/protocol/automatic-recovery-continuation.ts";

const continuation = createAutomaticRecoveryContinuation({
  failedActivationId: "failed-activation",
  replacementActivationId: "replacement-activation",
});

function marker(details: unknown = continuation) {
  return {
    role: "custom" as const,
    customType: AUTOMATIC_RECOVERY_CONTINUATION,
    content: "",
    display: false,
    details,
    timestamp: 1,
  };
}

describe("automatic recovery continuation projection", () => {
  it("deduplicates stable scheduler evidence and removes it from provider context", () => {
    const providerMessage = { role: "user" as const, content: "canonical work", timestamp: 2 };
    const projection = projectAutomaticRecoveryContinuationContext([
      marker(),
      marker(),
      providerMessage,
    ] as never);

    assert.deepEqual(projection.messages, [providerMessage]);
    assert.deepEqual(projection.observedProjectionIds, [continuation.projectionId]);
  });

  it("strips forged scheduler markers without acknowledging them", () => {
    const projection = projectAutomaticRecoveryContinuationContext([
      marker({ ...continuation, projectionId: "forged" }),
    ] as never);

    assert.deepEqual(projection.messages, []);
    assert.deepEqual(projection.observedProjectionIds, []);
  });
});
