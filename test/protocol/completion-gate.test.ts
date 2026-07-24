import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { WorkflowControlPlane } from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import { CompletionGateStore, CompletionRejectedError } from "../../pi-extension/subagents/protocol/completion-gate.ts";
import { initializeSubagentSessionFile } from "../../pi-extension/subagents/session.ts";
import { bindNewWorkflowSession } from "../../pi-extension/subagents/protocol/workflow-session-binding.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { digestPayload } from "../../pi-extension/subagents/protocol/direct-signal-transcript.ts";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "completion-gate-"));
  const ownerPath = join(root, "owner.jsonl");
  initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: "00000000-0000-4000-8000-000000000001" });
  const owner = WorkflowControlPlane.startOwner({ ownerSessionId: "00000000-0000-4000-8000-000000000001", ownerSessionPath: ownerPath });
  const workerPath = join(owner.workflow.sessionsDirectory, "worker.jsonl");
  initializeSubagentSessionFile({ mode: "standalone", childSessionFile: workerPath, childCwd: root, childSessionId: "00000000-0000-4000-8000-000000000002" });
  const sessionBinding = bindNewWorkflowSession({ workflowOwnerId: owner.workflow.ownerAgentId, agentId: "00000000-0000-4000-8000-000000000002", sessionPath: workerPath });
  const worker = owner.addAgent({ agentId: "00000000-0000-4000-8000-000000000002", sessionPath: workerPath, sessionBinding, spawner: owner.currentAgent, name: "worker", delegationPolicy: "disabled" });
  const ownership = owner.acquireAgentRun(owner.agent(worker.agentId), "00000000-0000-4000-8000-000000000003");
  owner.startActivation(ownership);
  const signals = new DirectSignalStore(owner.workflow.databasePath);
  signals.registerRouter({ recipient: owner.currentAgent, endpoint: "owner://router", registeredAtMs: 1 });
  signals.registerRouter({ recipient: owner.agent(worker.agentId), ownership, endpoint: "worker://router", registeredAtMs: 1 });
  const gate = new CompletionGateStore(owner.workflow.databasePath);
  return { owner, worker, ownership, gate, signals };
}

test("standalone completion atomically ends the activation and releases routing ownership", () => {
  const f = fixture();
  try {
    const completed = f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "complete-1" }, 123);
    assert.deepEqual(completed.source, { kind: "standalone", toolCallId: "complete-1" });
    assert.equal(f.owner.inspectActivation(f.owner.agent(f.worker.agentId))?.state.kind, "ended");
    assert.equal(f.owner.currentAgentRun(f.owner.agent(f.worker.agentId)), undefined);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("exact standalone completion retry is idempotent after ownership release", () => {
  const f = fixture();
  try {
    const source = { kind: "standalone" as const, toolCallId: "complete-retry" };
    const first = f.gate.complete(f.ownership, source, 123);
    assert.deepEqual(f.gate.complete(f.ownership, source, 999), first);
    assert.throws(() => f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "conflict" }, 999),
      (error: unknown) => (error as { code?: string }).code === "InvalidCompletionMessage");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("completion reports independent blockers and rolls back every completion mutation", () => {
  const f = fixture();
  try {
    f.owner.addActivationDependency(f.ownership, { kind: "operation", dependencyId: "acceptance:message-1" });
    assert.throws(() => f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "complete-1" }, 123), (error: unknown) => {
      assert.ok(error instanceof CompletionRejectedError);
      assert.deepEqual(error.blockers, [{ kind: "acceptance-uncertainty", dependencyId: "acceptance:message-1" }]);
      return true;
    });
    assert.equal(f.owner.inspectActivation(f.owner.agent(f.worker.agentId))?.state.kind, "active");
    assert.equal(f.owner.currentAgentRun(f.owner.agent(f.worker.agentId))?.runId, f.ownership.runId);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("cancelled outgoing Requests no longer block dependency settlement or completion", () => {
  const f = fixture();
  try {
    const worker = f.owner.agent(f.worker.agentId);
    const message = "outgoing work no longer needed";
    f.signals.bindMessage({ messageId: "cancelled-outgoing", sender: worker, recipient: f.owner.currentAgent,
      sourceEntryId: "cancelled-outgoing-source", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: true, createdAtMs: 2 });
    f.signals.acceptSignal({ recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 3,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "cancelled-outgoing", senderAgentId: worker.agentId,
        recipientAgentId: f.owner.currentAgent.agentId, sourceEntryId: "cancelled-outgoing-source", payloadDigest: digestPayload(message),
        deliveryTiming: "steer", responseRequired: true, onAccepted: "continue", message } });
    f.signals.cancelRequest({ requester: worker, requestId: "cancelled-outgoing", noticeMessageId: "unused-notice", cancelledAtMs: 4 });
    f.owner.settleActivation(f.ownership);
    assert.deepEqual(f.owner.inspectActivation(worker)?.state, { kind: "waiting", dependencies: [{ kind: "undeclared", dependencyId: "undeclared" }] });
    f.owner.activateTurn(f.ownership);
    const completed = f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "complete-after-cancel" }, 5);
    assert.equal(completed.agentId, worker.agentId);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("Workflow Owner completion is rejected", () => {
  const f = fixture();
  try {
    assert.throws(() => f.gate.complete({ ...f.ownership, agentId: f.owner.workflow.ownerAgentId }, { kind: "standalone", toolCallId: "owner" }, 1), /Workflow Owner/);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("fused final Signal acceptance and completion commit together", () => {
  const f = fixture();
  try {
    const sender = f.owner.agent(f.worker.agentId);
    const message = "final status";
    f.signals.bindMessage({
      messageId: "final-signal", sender, recipient: f.owner.currentAgent,
      sourceEntryId: "send-1", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: false, onAccepted: "complete", createdAtMs: 2,
    });
    const accepted = f.signals.acceptSignal({
      recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 3,
      request: {
        workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "final-signal",
        senderAgentId: sender.agentId, recipientAgentId: f.owner.currentAgent.agentId,
        sourceEntryId: "send-1", payloadDigest: digestPayload(message), deliveryTiming: "steer",
        responseRequired: false, onAccepted: "complete", message,
        completion: { ownership: f.ownership },
      },
    });
    assert.equal(accepted.receipt.messageId, "final-signal");
    assert.equal(f.owner.inspectActivation(sender)?.state.kind, "ended");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("blocked fused completion rolls message acceptance back and exposes exact blockers", () => {
  const f = fixture();
  try {
    f.owner.addActivationDependency(f.ownership, {
      kind: "operation",
      dependencyId: "side-effect:still-unknown",
    });
    f.owner.beginHumanInterrupt(f.ownership, "human-1");
    const sender = f.owner.agent(f.worker.agentId);
    const message = "cannot finish yet";
    f.signals.bindMessage({ messageId: "blocked-final", sender, recipient: f.owner.currentAgent,
      sourceEntryId: "send-blocked", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: false, onAccepted: "complete", createdAtMs: 2 });
    assert.throws(() => f.signals.acceptSignal({ recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 3,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "blocked-final", senderAgentId: sender.agentId,
        recipientAgentId: f.owner.currentAgent.agentId, sourceEntryId: "send-blocked", payloadDigest: digestPayload(message),
        deliveryTiming: "steer", responseRequired: false, onAccepted: "complete", message, completion: { ownership: f.ownership } } }),
    (error: unknown) => error instanceof CompletionRejectedError
      && error.blockers.some((blocker) =>
        blocker.kind === "human-interrupt" && blocker.toolCallId === "human-1")
      && error.blockers.some((blocker) =>
        blocker.kind === "side-effect-uncertainty"
        && blocker.dependencyId === "side-effect:still-unknown"));
    assert.equal(f.signals.inspectMessage(f.owner.workflow.ownerAgentId, "blocked-final")?.deliveryStatus, "bound");
    assert.equal(f.owner.inspectActivation(sender)?.state.kind, "waiting");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("fused final Answer closes its incoming Request before the gate evaluates", () => {
  const f = fixture();
  try {
    const worker = f.owner.agent(f.worker.agentId);
    const requestText = "report";
    f.signals.bindMessage({ messageId: "request-1", sender: f.owner.currentAgent, recipient: worker,
      sourceEntryId: "owner-send", payloadDigest: digestPayload(requestText), deliveryTiming: "steer",
      responseRequired: true, createdAtMs: 2 });
    f.signals.acceptSignal({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", acceptedAtMs: 3,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "request-1", senderAgentId: f.owner.currentAgent.agentId,
        recipientAgentId: worker.agentId, sourceEntryId: "owner-send", payloadDigest: digestPayload(requestText),
        deliveryTiming: "steer", responseRequired: true, onAccepted: "continue", message: requestText } });
    f.signals.commitDelivery({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", messageId: "request-1", deliveredAtMs: 4 });

    const answerText = "done";
    f.signals.bindMessage({ messageId: "answer-1", sender: worker, recipient: f.owner.currentAgent,
      sourceEntryId: "worker-answer", payloadDigest: digestPayload(answerText), deliveryTiming: "steer",
      responseRequired: false, inReplyToRequestId: "request-1", onAccepted: "complete", createdAtMs: 5 });
    f.signals.acceptSignal({ recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 6,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "answer-1", senderAgentId: worker.agentId,
        recipientAgentId: f.owner.currentAgent.agentId, sourceEntryId: "worker-answer", payloadDigest: digestPayload(answerText),
        deliveryTiming: "steer", responseRequired: false, inReplyToRequestId: "request-1", onAccepted: "complete",
        message: answerText, completion: { ownership: f.ownership } } });
    assert.equal(f.signals.inspectRequest(f.owner.workflow.ownerAgentId, "request-1")?.status, "answered");
    assert.equal(f.owner.inspectActivation(worker)?.state.kind, "ended");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("blocked fused final Answer rolls back its Answer slot and message acceptance", () => {
  const f = fixture();
  try {
    const worker = f.owner.agent(f.worker.agentId);
    const requestText = "report before completion";
    f.signals.bindMessage({ messageId: "blocked-answer-request", sender: f.owner.currentAgent, recipient: worker,
      sourceEntryId: "blocked-answer-owner-send", payloadDigest: digestPayload(requestText), deliveryTiming: "steer",
      responseRequired: true, createdAtMs: 2 });
    f.signals.acceptSignal({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", acceptedAtMs: 3,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "blocked-answer-request", senderAgentId: f.owner.currentAgent.agentId,
        recipientAgentId: worker.agentId, sourceEntryId: "blocked-answer-owner-send", payloadDigest: digestPayload(requestText),
        deliveryTiming: "steer", responseRequired: true, onAccepted: "continue", message: requestText } });
    f.signals.commitDelivery({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", messageId: "blocked-answer-request", deliveredAtMs: 4 });
    f.owner.beginHumanInterrupt(f.ownership, "blocked-answer-human");

    const answerText = "cannot finish yet";
    f.signals.bindMessage({ messageId: "blocked-answer", sender: worker, recipient: f.owner.currentAgent,
      sourceEntryId: "blocked-answer-send", payloadDigest: digestPayload(answerText), deliveryTiming: "steer",
      responseRequired: false, inReplyToRequestId: "blocked-answer-request", onAccepted: "complete", createdAtMs: 5 });
    assert.throws(() => f.signals.acceptSignal({ recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 6,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "blocked-answer", senderAgentId: worker.agentId,
        recipientAgentId: f.owner.currentAgent.agentId, sourceEntryId: "blocked-answer-send", payloadDigest: digestPayload(answerText),
        deliveryTiming: "steer", responseRequired: false, inReplyToRequestId: "blocked-answer-request", onAccepted: "complete",
        message: answerText, completion: { ownership: f.ownership } } }), CompletionRejectedError);
    assert.deepEqual(f.signals.inspectRequest(f.owner.workflow.ownerAgentId, "blocked-answer-request"), {
      requestId: "blocked-answer-request",
      requesterAgentId: f.owner.currentAgent.agentId,
      responderAgentId: worker.agentId,
      responderActivationId: f.ownership.runId,
      answerDeliveryTiming: "steer",
      status: "open",
    });
    assert.equal(f.signals.inspectMessage(f.owner.workflow.ownerAgentId, "blocked-answer")?.deliveryStatus, "bound");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("completion and inbound acceptance honor both first-commit-wins orders", () => {
  {
    const f = fixture();
    try {
      const worker = f.owner.agent(f.worker.agentId);
      const message = "late inbound";
      f.signals.bindMessage({ messageId: "completion-first-inbound", sender: f.owner.currentAgent, recipient: worker,
        sourceEntryId: "completion-first-source", payloadDigest: digestPayload(message), deliveryTiming: "steer",
        responseRequired: false, createdAtMs: 2 });
      f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "completion-first" }, 3);
      assert.throws(() => f.signals.acceptSignal({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", acceptedAtMs: 4,
        request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "completion-first-inbound", senderAgentId: f.owner.currentAgent.agentId,
          recipientAgentId: worker.agentId, sourceEntryId: "completion-first-source", payloadDigest: digestPayload(message),
          deliveryTiming: "steer", responseRequired: false, onAccepted: "continue", message } }),
      (error: unknown) => ["OwnershipLost", "RecipientUnreachable"].includes((error as { code?: string }).code ?? ""));
      assert.equal(f.signals.inspectMessage(f.owner.workflow.ownerAgentId, "completion-first-inbound")?.deliveryStatus, "bound");
    } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
  }
  {
    const f = fixture();
    try {
      const worker = f.owner.agent(f.worker.agentId);
      const message = "accepted inbound";
      f.signals.bindMessage({ messageId: "acceptance-first-inbound", sender: f.owner.currentAgent, recipient: worker,
        sourceEntryId: "acceptance-first-source", payloadDigest: digestPayload(message), deliveryTiming: "steer",
        responseRequired: false, createdAtMs: 2 });
      f.signals.acceptSignal({ recipient: worker, ownership: f.ownership, endpoint: "worker://router", acceptedAtMs: 3,
        request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "acceptance-first-inbound", senderAgentId: f.owner.currentAgent.agentId,
          recipientAgentId: worker.agentId, sourceEntryId: "acceptance-first-source", payloadDigest: digestPayload(message),
          deliveryTiming: "steer", responseRequired: false, onAccepted: "continue", message } });
      assert.throws(() => f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "acceptance-first" }, 4),
        (error: unknown) => error instanceof CompletionRejectedError
          && error.blockers.some((blocker) => blocker.kind === "accepted-undelivered-input" && blocker.messageId === "acceptance-first-inbound"));
      assert.equal(f.owner.inspectActivation(worker)?.state.kind, "active");
    } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
  }
});

test("completion closes an open undeclared-settlement correction episode", () => {
  const f = fixture();
  try {
    f.owner.settleActivation(f.ownership);
    assert.equal(f.owner.inspectUndeclaredEpisode(f.owner.agent(f.worker.agentId))?.status, "open");
    f.owner.activateTurn(f.ownership);
    f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "complete-correction" }, 10);
    assert.equal(f.owner.inspectUndeclaredEpisode(f.owner.agent(f.worker.agentId))?.status, "closed");
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("durable message identity cannot be replayed with a different completion disposition", () => {
  const f = fixture();
  try {
    const sender = f.owner.agent(f.worker.agentId);
    const message = "same payload";
    f.signals.bindMessage({ messageId: "identity-1", sender, recipient: f.owner.currentAgent,
      sourceEntryId: "source-1", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: false, onAccepted: "continue", createdAtMs: 2 });
    assert.throws(() => f.signals.findMessageBySource({ sender, recipient: f.owner.currentAgent,
      sourceEntryId: "source-1", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: false, onAccepted: "complete" }), /different routing or source metadata/);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("operation uncertainty transfers to a recovery activation and still blocks completion", () => {
  const f = fixture();
  try {
    f.owner.addActivationDependency(f.ownership, { kind: "operation", dependencyId: "acceptance:uncertain-1" });
    f.owner.failAgentRun(f.ownership, { error: "crash" });
    const replacement = f.owner.acquireAgentRun(f.owner.agent(f.worker.agentId), "00000000-0000-4000-8000-000000000004");
    const activation = f.owner.startActivation(replacement);
    assert.deepEqual(activation.state, { kind: "waiting", dependencies: [{ kind: "operation", dependencyId: "acceptance:uncertain-1" }] });
    assert.throws(() => f.gate.complete(replacement, { kind: "standalone", toolCallId: "complete-recovery" }, 20),
      (error: unknown) => error instanceof CompletionRejectedError
        && error.blockers.some((blocker) => blocker.kind === "acceptance-uncertainty"));
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("same-identity accepted-message reconciliation removes its acceptance uncertainty", () => {
  const f = fixture();
  try {
    const sender = f.owner.agent(f.worker.agentId);
    const message = "accepted despite lost acknowledgement";
    f.owner.addActivationDependency(f.ownership, { kind: "operation", dependencyId: "acceptance:accepted-1" });
    f.signals.bindMessage({ messageId: "accepted-1", sender, recipient: f.owner.currentAgent,
      sourceEntryId: "accepted-source", payloadDigest: digestPayload(message), deliveryTiming: "steer",
      responseRequired: false, createdAtMs: 2 });
    f.signals.acceptSignal({ recipient: f.owner.currentAgent, endpoint: "owner://router", acceptedAtMs: 3,
      request: { workflowOwnerId: f.owner.workflow.ownerAgentId, messageId: "accepted-1", senderAgentId: sender.agentId,
        recipientAgentId: f.owner.currentAgent.agentId, sourceEntryId: "accepted-source", payloadDigest: digestPayload(message),
        deliveryTiming: "steer", responseRequired: false, onAccepted: "continue", message } });
    assert.ok(f.signals.reconcileAcceptedMessage(sender, "accepted-1", f.ownership));
    assert.deepEqual(f.owner.inspectActivation(sender)?.state, { kind: "active" });
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});

test("safe rejection atomically removes the bound operation and its exact dependency", () => {
  const f = fixture();
  try {
    const sender = f.owner.agent(f.worker.agentId);
    f.signals.bindMessage({ messageId: "rejected-1", sender, recipient: f.owner.currentAgent,
      sourceEntryId: "rejected-source", payloadDigest: digestPayload("rejected"), deliveryTiming: "steer",
      responseRequired: false, onAccepted: "continue", ownership: f.ownership, createdAtMs: 2 });
    assert.throws(() => f.gate.complete(f.ownership, { kind: "standalone", toolCallId: "blocked-by-bound" }, 3),
      (error: unknown) => error instanceof CompletionRejectedError
        && error.blockers.some((blocker) => blocker.kind === "acceptance-uncertainty"));
    assert.equal(f.signals.discardUnacceptedMessage(sender, "rejected-1"), true);
    assert.deepEqual(f.owner.inspectActivation(sender)?.state, { kind: "active" });
    assert.equal(f.signals.inspectMessage(sender.workflowOwnerId, "rejected-1"), undefined);
  } finally { f.gate.close(); f.signals.close(); f.owner.close(); }
});
