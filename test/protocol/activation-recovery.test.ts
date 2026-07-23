import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, it } from "node:test";
import { WorkflowScenario } from "./scenario-harness.ts";
import { CompletionGateStore } from "../../pi-extension/subagents/protocol/completion-gate.ts";
import { DirectSignalRuntime, type InboxBatch } from "../../pi-extension/subagents/protocol/direct-signal.ts";
import { digestPayload } from "../../pi-extension/subagents/protocol/direct-signal-transcript.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { HumanInterruptInputBridge } from "../../pi-extension/subagents/protocol/human-interrupt-extension.ts";
import {
  projectAutomaticRecoveryContinuationContext,
  triggerAutomaticRecoveryContinuation,
} from "../../pi-extension/subagents/subagent-done.ts";
import { handleAgentRunWatcherCompletion } from "../../pi-extension/subagents/index.ts";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-activation-recovery-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

function persistLaunchPolicy(databasePath: string, agentId: string): void {
  const database = new DatabaseSync(databasePath);
  try {
    database.prepare("UPDATE workflow_agents SET launch_policy_json = ? WHERE agent_id = ?")
      .run(JSON.stringify({ denyTools: [] }), agentId);
  } finally {
    database.close();
  }
}

describe("automatic activation recovery", () => {
  it("closes a stale episode when its only queued Request is cancelled before claim", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "stale-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Stale Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "stale-worker-router",
        registeredAtMs: scenario.clock.now(),
      });
      messages.bindMessage({
        messageId: "cancel-before-recovery",
        sender: runtime.owner(),
        recipient: reference,
        sourceEntryId: "cancel-before-recovery-source",
        payloadDigest: "cancel-before-recovery-digest",
        deliveryTiming: "steer",
        responseRequired: true,
        createdAtMs: scenario.clock.now(),
      });
      messages.acceptSignal({
        request: {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: "cancel-before-recovery",
          senderAgentId: runtime.workflow.ownerAgentId,
          recipientAgentId: agent.agentId,
          sourceEntryId: "cancel-before-recovery-source",
          payloadDigest: "cancel-before-recovery-digest",
          deliveryTiming: "steer",
          responseRequired: true,
          message: "work that is cancelled while the Owner is offline",
        },
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "stale-worker-router",
        acceptedAtMs: scenario.clock.now(),
      });
      runtime.confirmAgentRunExit(failed, { error: "worker failed while request was queued" });
      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "pending");

      assert.equal(messages.cancelRequest({
        requester: runtime.owner(),
        requestId: "cancel-before-recovery",
        noticeMessageId: "unused-cancellation-notice",
        cancelledAtMs: scenario.clock.now(),
      }).delivery, "suppressed");

      assert.equal(runtime.controlPlane.claimRecoveryRun(failed.ownership.runId, "needless-replacement"), undefined);
      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
      assert.equal(runtime.currentAgentRun(reference), undefined);
    } finally {
      messages.close();
      runtime.close();
    }
  });

  it("resolves a claimed replacement when its last queued Request is cancelled before activation start", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "claim-cancel-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Claim Cancel Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "claim-cancel-worker-router",
        registeredAtMs: scenario.clock.now(),
      });
      messages.bindMessage({
        messageId: "claim-then-cancel",
        sender: runtime.owner(),
        recipient: reference,
        sourceEntryId: "claim-then-cancel-source",
        payloadDigest: "claim-then-cancel-digest",
        deliveryTiming: "steer",
        responseRequired: true,
        createdAtMs: scenario.clock.now(),
      });
      messages.acceptSignal({
        request: {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: "claim-then-cancel",
          senderAgentId: runtime.workflow.ownerAgentId,
          recipientAgentId: agent.agentId,
          sourceEntryId: "claim-then-cancel-source",
          payloadDigest: "claim-then-cancel-digest",
          deliveryTiming: "steer",
          responseRequired: true,
          message: "work cancelled after the recovery claim",
        },
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "claim-cancel-worker-router",
        acceptedAtMs: scenario.clock.now(),
      });
      runtime.confirmAgentRunExit(failed, { error: "worker failed while Request was queued" });
      const claim = runtime.controlPlane.claimRecoveryRun(failed.ownership.runId, "claim-cancel-replacement")!;
      assert.equal(claim.recovery.state, "launching");

      messages.cancelRequest({
        requester: runtime.owner(),
        requestId: "claim-then-cancel",
        noticeMessageId: "claim-then-cancel-notice",
        cancelledAtMs: scenario.clock.now(),
      });

      assert.deepEqual(runtime.controlPlane.startRecoveryActivation(claim.ownership), {
        kind: "not-needed",
        failedActivationId: failed.ownership.runId,
      });
      assert.equal(runtime.inspectActivation(reference)?.runId, failed.ownership.runId);
      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
      assert.equal(runtime.currentAgentRun(reference), undefined);
    } finally {
      messages.close();
      runtime.close();
    }
  });

  it("recovers an unresolved operation dependency without inventing a continuation turn", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.addActivationDependency(run, { kind: "operation", dependencyId: "operation-only" });
    runtime.confirmAgentRunExit(run, { error: "process lost" });

    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "pending");
    const replacement = runtime.controlPlane.claimRecoveryRun(run.ownership.runId, "operation-replacement")!;
    runtime.controlPlane.startActivation(replacement.ownership);
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting",
      dependencies: [{ kind: "operation", dependencyId: "operation-only" }],
    });
    assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(replacement.ownership), false);
    runtime.close();
  });

  it("preserves delivered incoming and outgoing Requests plus a response-bound Human input without fabricating protocol effects", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({ recipient: runtime.owner(), endpoint: "owner-router", registeredAtMs: scenario.clock.now() });
      messages.registerRouter({ recipient: reference, ownership: first.ownership, endpoint: "worker-router", registeredAtMs: scenario.clock.now() });
      const accept = (input: {
        messageId: string; senderAgentId: string; recipientAgentId: string; endpoint: string;
        recipient: typeof reference | ReturnType<typeof runtime.owner>;
        ownership?: typeof first.ownership;
        deliver?: boolean;
      }) => {
        messages.bindMessage({
          messageId: input.messageId,
          sender: runtime.agent(input.senderAgentId),
          recipient: input.recipient,
          sourceEntryId: `${input.messageId}-source`,
          payloadDigest: `${input.messageId}-digest`,
          deliveryTiming: "steer",
          responseRequired: true,
          createdAtMs: scenario.clock.now(),
        });
        messages.acceptSignal({
          request: {
            workflowOwnerId: runtime.workflow.ownerAgentId,
            messageId: input.messageId,
            senderAgentId: input.senderAgentId,
            recipientAgentId: input.recipientAgentId,
            sourceEntryId: `${input.messageId}-source`,
            payloadDigest: `${input.messageId}-digest`,
            deliveryTiming: "steer",
            responseRequired: true,
            message: "canonical payload",
          },
          recipient: input.recipient,
          ...(input.ownership ? { ownership: input.ownership } : {}),
          endpoint: input.endpoint,
          acceptedAtMs: scenario.clock.now(),
        });
        if (input.deliver !== false) {
          messages.commitDelivery({
            recipient: input.recipient,
            ...(input.ownership ? { ownership: input.ownership } : {}),
            endpoint: input.endpoint,
            messageId: input.messageId,
            deliveredAtMs: scenario.clock.now(),
          });
        }
      };
      accept({
        messageId: "incoming-request", senderAgentId: runtime.workflow.ownerAgentId,
        recipientAgentId: agent.agentId, recipient: reference, endpoint: "worker-router", ownership: first.ownership,
      });
      accept({
        messageId: "outgoing-request", senderAgentId: agent.agentId,
        recipientAgentId: runtime.workflow.ownerAgentId, recipient: runtime.owner(), endpoint: "owner-router",
      });
      accept({
        messageId: "accepted-pending-request", senderAgentId: runtime.workflow.ownerAgentId,
        recipientAgentId: agent.agentId, recipient: reference, endpoint: "worker-router",
        ownership: first.ownership, deliver: false,
      });
      runtime.beginHumanInterrupt(first, "ask-user");
      runtime.bindHumanResponse(first, "ask-user", "accepted-input");
      const messagesBeforeRecovery = messages.listMessages(runtime.workflow.ownerAgentId);

      runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });
      const claim = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "automatic-replacement")!;
      runtime.controlPlane.startActivation(claim.ownership);

      assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(claim.ownership), false,
        "the Human bridge owns replay and model continuation after it commits the canonical tool result");
      assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(claim.ownership), false);
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "incoming-request")?.responderActivationId, claim.ownership.runId);
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "outgoing-request")?.requesterActivationId, claim.ownership.runId);
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "incoming-request")?.status, "open");
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "outgoing-request")?.status, "open");
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "accepted-pending-request")?.responderActivationId, claim.ownership.runId);
      assert.deepEqual(messages.listPending(reference).map((pointer) => pointer.messageId), ["accepted-pending-request"]);
      assert.deepEqual(runtime.inspectHumanInterrupt(reference), {
        toolCallId: "ask-user", status: "response-bound", responseInputId: "accepted-input",
        createdAtMs: scenario.clock.now(), updatedAtMs: scenario.clock.now(),
      });
      assert.deepEqual(messages.listMessages(runtime.workflow.ownerAgentId), messagesBeforeRecovery, "recovery must not fabricate answers, notices, or relays");
    } finally {
      messages.close();
      runtime.close();
    }
  });

  it("continues a delivered incoming Request once while leaving an outgoing Request waiting", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const incomingSession = scenario.childSession(runtime, "incoming-worker");
    const outgoingSession = scenario.childSession(runtime, "outgoing-worker");
    const incomingAgent = runtime.addAgent({ session: incomingSession, spawner: runtime.owner(), name: "Incoming Worker" });
    const outgoingAgent = runtime.addAgent({ session: outgoingSession, spawner: runtime.owner(), name: "Outgoing Worker" });
    const incomingReference = runtime.agent(incomingAgent.agentId);
    const outgoingReference = runtime.agent(outgoingAgent.agentId);
    const incomingRun = runtime.startAgentRun(incomingReference);
    const outgoingRun = runtime.startAgentRun(outgoingReference);
    persistLaunchPolicy(runtime.workflow.databasePath, incomingAgent.agentId);
    persistLaunchPolicy(runtime.workflow.databasePath, outgoingAgent.agentId);

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({ recipient: runtime.owner(), endpoint: "owner-router", registeredAtMs: scenario.clock.now() });
      messages.registerRouter({ recipient: incomingReference, ownership: incomingRun.ownership, endpoint: "incoming-router", registeredAtMs: scenario.clock.now() });
      const acceptRequest = (input: {
        messageId: string; sender: typeof incomingReference; recipient: typeof incomingReference;
        endpoint: string; ownership?: typeof incomingRun.ownership;
      }) => {
        const request = {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: input.messageId,
          senderAgentId: input.sender.agentId,
          recipientAgentId: input.recipient.agentId,
          sourceEntryId: `${input.messageId}-source`,
          payloadDigest: `${input.messageId}-digest`,
          deliveryTiming: "steer" as const,
          responseRequired: true,
          message: "canonical request",
        };
        messages.bindMessage({
          messageId: request.messageId, sender: input.sender, recipient: input.recipient,
          sourceEntryId: request.sourceEntryId, payloadDigest: request.payloadDigest,
          deliveryTiming: request.deliveryTiming, responseRequired: true, createdAtMs: scenario.clock.now(),
        });
        messages.acceptSignal({
          request, recipient: input.recipient, ...(input.ownership ? { ownership: input.ownership } : {}),
          endpoint: input.endpoint, acceptedAtMs: scenario.clock.now(),
        });
        messages.commitDelivery({
          recipient: input.recipient, ...(input.ownership ? { ownership: input.ownership } : {}),
          endpoint: input.endpoint, messageId: input.messageId, deliveredAtMs: scenario.clock.now(),
        });
      };
      acceptRequest({
        messageId: "delivered-incoming", sender: runtime.owner(), recipient: incomingReference,
        endpoint: "incoming-router", ownership: incomingRun.ownership,
      });
      acceptRequest({
        messageId: "outgoing-awaiting-answer", sender: outgoingReference, recipient: runtime.owner(),
        endpoint: "owner-router",
      });
      const messagesBeforeRecovery = messages.listMessages(runtime.workflow.ownerAgentId);

      runtime.confirmAgentRunExit(incomingRun, { error: "incoming worker crashed" });
      runtime.confirmAgentRunExit(outgoingRun, { error: "outgoing worker crashed" });
      const incomingReplacement = runtime.controlPlane.claimRecoveryRun(incomingRun.ownership.runId, "incoming-replacement")!;
      const outgoingReplacement = runtime.controlPlane.claimRecoveryRun(outgoingRun.ownership.runId, "outgoing-replacement")!;
      runtime.controlPlane.startActivation(incomingReplacement.ownership);
      runtime.controlPlane.startActivation(outgoingReplacement.ownership);

      const continuationMessages: Array<{ message: any; options: unknown }> = [];
      const recoveryBootstrap = {
        claimAutomaticRecoveryContinuation: () => runtime.controlPlane.claimAutomaticRecoveryContinuation(incomingReplacement.ownership),
        abandonAutomaticRecoveryContinuation: () => runtime.controlPlane.abandonAutomaticRecoveryContinuation(incomingReplacement.ownership),
      };
      assert.equal(triggerAutomaticRecoveryContinuation({
        sendMessage(message: any, options: unknown) { continuationMessages.push({ message, options }); },
      } as never, recoveryBootstrap), true);
      assert.equal(triggerAutomaticRecoveryContinuation({ sendMessage() {} } as never, recoveryBootstrap), false);
      assert.deepEqual(continuationMessages, [{
        message: {
          customType: "automatic_recovery_continuation",
          content: "",
          display: false,
          details: {
            projectionId: "automatic-recovery-continuation:runtime-1:incoming-replacement",
            failedActivationId: "runtime-1",
            replacementActivationId: "incoming-replacement",
          },
        },
        options: { triggerTurn: true, deliverAs: "steer" },
      }]);
      assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(outgoingReplacement.ownership), false);
      assert.deepEqual(runtime.inspectActivation(outgoingReference)?.state, {
        kind: "waiting",
        dependencies: [{ kind: "agent", dependencyId: "outgoing-awaiting-answer", agentId: runtime.workflow.ownerAgentId }],
      });
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "delivered-incoming")?.responderActivationId, "incoming-replacement");
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "outgoing-awaiting-answer")?.requesterActivationId, "outgoing-replacement");
      assert.deepEqual(messages.listMessages(runtime.workflow.ownerAgentId), messagesBeforeRecovery);
    } finally {
      messages.close();
      runtime.close();
    }
  });

  it("continues once so deferred accepted work can project after recovery settles", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: ownerSession, runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "deferred-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Deferred Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    const payload = "deferred work accepted before failure";
    scenario.transcripts.appendAgentSend(ownerSession, {
      sourceEntryId: "deferred-recovery-source",
      targetAgentId: agent.agentId,
      message: payload,
      timing: "deferred",
      responseRequired: false,
    });

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "deferred-worker-router",
        registeredAtMs: scenario.clock.now(),
      });
      messages.bindMessage({
        messageId: "deferred-recovery-signal",
        sender: runtime.owner(),
        recipient: reference,
        sourceEntryId: "deferred-recovery-source",
        payloadDigest: digestPayload(payload),
        deliveryTiming: "deferred",
        responseRequired: false,
        createdAtMs: scenario.clock.now(),
      });
      messages.acceptSignal({
        request: {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: "deferred-recovery-signal",
          senderAgentId: runtime.workflow.ownerAgentId,
          recipientAgentId: agent.agentId,
          sourceEntryId: "deferred-recovery-source",
          payloadDigest: digestPayload(payload),
          deliveryTiming: "deferred",
          responseRequired: false,
          message: payload,
        },
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "deferred-worker-router",
        acceptedAtMs: scenario.clock.now(),
      });
    } finally {
      messages.close();
    }

    runtime.confirmAgentRunExit(failed, { error: "worker failed before reaching deferred work" });
    const replacement = runtime.controlPlane.claimRecoveryRun(failed.ownership.runId, "deferred-replacement")!;
    runtime.controlPlane.startActivation(replacement.ownership);
    const released = runtime.controlPlane.releaseAutomaticRecoveryDeferredProjection(replacement.ownership);
    assert.equal(released?.state.kind, "waiting");
    assert.equal(runtime.inspectUndeclaredEpisode(reference), undefined,
      "the mechanical release must not consume the undeclared-settlement correction allowance");
    assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(replacement.ownership), false,
      "deferred-only recovery must not schedule an empty provider turn");
    const childRuntime = scenario.startAgent(runtime.workflow, session);
    const projected: InboxBatch[] = [];
    const inbox = new DirectSignalRuntime({
      controlPlane: childRuntime.controlPlane,
      ownership: replacement.ownership,
      projectInboxBatch(batch) { projected.push(batch); },
      now: scenario.clock.now,
    });
    try {
      await inbox.start();
      for (let attempt = 0; projected.length === 0 && attempt < 100; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.deepEqual(projected.flatMap((batch) => batch.messages.map((message) => message.messageId)), ["deferred-recovery-signal"]);
      assert.equal(inbox.confirmDelivery("deferred-recovery-signal"), true);
      runtime.controlPlane.activateTurn(replacement.ownership);
      runtime.controlPlane.addActivationDependency(replacement.ownership, {
        kind: "operation",
        dependencyId: "useful-deferred-work",
      });
      runtime.controlPlane.settleActivation(replacement.ownership);
      assert.equal(runtime.inspectActivation(reference)?.turnSequence, 2, "deferred input gets exactly one useful turn");
      assert.equal(runtime.inspectUndeclaredEpisode(reference), undefined,
        "useful work must retain the unused correction allowance");
      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
      inbox.releaseDeferred();
      await new Promise((resolve) => setTimeout(resolve, 0));
      assert.equal(projected.flatMap((batch) => batch.messages).length, 1, "settlement must not project or wake a duplicate turn");
    } finally {
      await inbox.close();
      childRuntime.close();
      runtime.close();
    }
  });

  it("retries one transcript-evidenced continuation when a persisted Human result outlives its run", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "persisted-human-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Persisted Human Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(failed, "persisted-human-result");
    runtime.bindHumanResponse(failed, "persisted-human-result", "persisted-human-input");
    runtime.prepareHumanResponseResult(failed, "persisted-human-result");
    runtime.confirmAgentRunExit(failed, { error: "crashed after Pi persisted the Human tool result" });

    const replacement = runtime.controlPlane.claimRecoveryRun(failed.ownership.runId, "persisted-human-replacement")!;
    runtime.controlPlane.startActivation(replacement.ownership);
    const persistedEntries = [{ message: {
      role: "toolResult",
      toolCallId: "persisted-human-result",
      toolName: "agent_ask_user",
      content: [{ type: "text", text: "continue" }],
      isError: false,
    } }];
    await new HumanInterruptInputBridge().reconcile({
      sessionManager: { getEntries: () => persistedEntries },
    } as never, {
      async waitUntilReady() {},
      currentHumanInterrupt: () => runtime.controlPlane.inspectHumanInterrupt(reference),
      bindHumanResponse() { assert.fail("persisted result must not rebind Human input"); },
      confirmHumanResponseResult: (toolCallId: string) => runtime.controlPlane.confirmHumanResponseResult(
        replacement.ownership,
        toolCallId,
      ),
      releaseDeferredSignals() {},
    } as never);
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "consumed",
      "replacement startup confirms the canonical transcript result");

    const sent: Array<{ message: any; options: unknown }> = [];
    const recoveryBootstrap = {
      claimAutomaticRecoveryContinuation: () => runtime.controlPlane.claimAutomaticRecoveryContinuation(replacement.ownership),
      abandonAutomaticRecoveryContinuation: () => runtime.controlPlane.abandonAutomaticRecoveryContinuation(replacement.ownership),
    };
    assert.equal(triggerAutomaticRecoveryContinuation({
      sendMessage(message: any, options: unknown) { sent.push({ message, options }); },
    } as never, recoveryBootstrap), true, "persisted Human result evidence must durably require a continuation");
    assert.equal(triggerAutomaticRecoveryContinuation({ sendMessage() {} } as never, recoveryBootstrap), false);

    // The first scheduler marker reached the transcript, but its process died
    // before a context/provider boundary. A fresh runtime must re-arm that same
    // durable continuation rather than treating sendMessage() as consumption.
    runtime.controlPlane.rearmAutomaticRecoveryContinuation(replacement.ownership);
    assert.equal(triggerAutomaticRecoveryContinuation({
      sendMessage(message: any, options: unknown) { sent.push({ message, options }); },
    } as never, recoveryBootstrap), true);
    assert.equal(sent.length, 2, "one retry replaces the pre-crash scheduling attempt");
    assert.equal(sent[0].message.details.projectionId, sent[1].message.details.projectionId);

    const projected = projectAutomaticRecoveryContinuationContext([
      { role: "custom", ...sent[0].message, timestamp: 1 },
      { role: "custom", ...sent[1].message, timestamp: 2 },
    ] as never);
    assert.deepEqual(projected.messages, [], "scheduler markers never enter provider context");
    assert.deepEqual(projected.observedProjectionIds, [sent[0].message.details.projectionId]);
    assert.equal(runtime.controlPlane.confirmAutomaticRecoveryContinuationContext(
      replacement.ownership,
      projected.observedProjectionIds,
    ), true);
    assert.equal(triggerAutomaticRecoveryContinuation({ sendMessage() {} } as never, recoveryBootstrap), false,
      "provider-context confirmation permanently fences duplicate turns");

    runtime.controlPlane.activateTurn(replacement.ownership);
    runtime.controlPlane.addActivationDependency(replacement.ownership, {
      kind: "operation",
      dependencyId: "continued-after-persisted-human-result",
    });
    runtime.controlPlane.settleActivation(replacement.ownership);
    assert.equal(runtime.inspectActivation(reference)?.turnSequence, 2, "replacement receives exactly one model continuation");
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    runtime.close();
  });

  it("wakes once from a pre-crash deferred Inbox Batch without projecting it twice", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: ownerSession, runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "persisted-inbox-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Persisted Inbox Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    const payload = "deferred Signal projected before the crash";
    scenario.transcripts.appendAgentSend(ownerSession, {
      sourceEntryId: "persisted-inbox-source",
      targetAgentId: agent.agentId,
      message: payload,
      timing: "deferred",
      responseRequired: false,
    });

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      messages.registerRouter({
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "persisted-inbox-first-router",
        registeredAtMs: scenario.clock.now(),
      });
      messages.bindMessage({
        messageId: "persisted-inbox-signal",
        sender: runtime.owner(),
        recipient: reference,
        sourceEntryId: "persisted-inbox-source",
        payloadDigest: digestPayload(payload),
        deliveryTiming: "deferred",
        responseRequired: false,
        createdAtMs: scenario.clock.now(),
      });
      messages.acceptSignal({
        request: {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: "persisted-inbox-signal",
          senderAgentId: runtime.workflow.ownerAgentId,
          recipientAgentId: agent.agentId,
          sourceEntryId: "persisted-inbox-source",
          payloadDigest: digestPayload(payload),
          deliveryTiming: "deferred",
          responseRequired: false,
          message: payload,
        },
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "persisted-inbox-first-router",
        acceptedAtMs: scenario.clock.now(),
      });
      const claimed = messages.commitInboxProjection({
        recipient: reference,
        ownership: failed.ownership,
        endpoint: "persisted-inbox-first-router",
        messageIds: ["persisted-inbox-signal"],
        project(messageIds) {
          assert.deepEqual(messageIds, ["persisted-inbox-signal"]);
          scenario.transcripts.appendInboxBatch(session, {
            deliveryTiming: "deferred",
            messages: [{
              kind: "signal",
              messageId: "persisted-inbox-signal",
              senderAgentId: runtime.workflow.ownerAgentId,
              recipientAgentId: agent.agentId,
              deliveryTiming: "deferred",
              message: payload,
            }],
          });
        },
      });
      assert.deepEqual(claimed, ["persisted-inbox-signal"]);
    } finally {
      messages.close();
    }

    runtime.confirmAgentRunExit(failed, { error: "crashed after Pi persisted the Inbox Batch" });
    const replacement = runtime.controlPlane.claimRecoveryRun(failed.ownership.runId, "persisted-inbox-replacement")!;
    runtime.controlPlane.startActivation(replacement.ownership);
    runtime.controlPlane.releaseAutomaticRecoveryDeferredProjection(replacement.ownership);

    const childRuntime = scenario.startAgent(runtime.workflow, session);
    const duplicateBatches: InboxBatch[] = [];
    const inbox = new DirectSignalRuntime({
      controlPlane: childRuntime.controlPlane,
      ownership: replacement.ownership,
      projectInboxBatch(batch) { duplicateBatches.push(batch); },
      hasProjectedMessage(messageId) { return messageId === "persisted-inbox-signal"; },
      now: scenario.clock.now,
    });
    const sent: Array<{ message: any; options: unknown }> = [];
    try {
      await inbox.start();
      for (let attempt = 0; childRuntime.listPending(reference).length > 0 && attempt < 100; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.deepEqual(childRuntime.listPending(reference), [], "transcript evidence confirms and removes the old pointer");
      assert.deepEqual(duplicateBatches, [], "replacement must not redeliver the persisted Inbox Batch");

      const recoveryBootstrap = {
        claimAutomaticRecoveryContinuation: () => runtime.controlPlane.claimAutomaticRecoveryContinuation(replacement.ownership),
        abandonAutomaticRecoveryContinuation: () => runtime.controlPlane.abandonAutomaticRecoveryContinuation(replacement.ownership),
      };
      assert.equal(triggerAutomaticRecoveryContinuation({
        sendMessage(message: any, options: unknown) { sent.push({ message, options }); },
      } as never, recoveryBootstrap), true, "confirmed pre-crash Inbox evidence must wake non-Request Signals too");
      assert.equal(triggerAutomaticRecoveryContinuation({ sendMessage() {} } as never, recoveryBootstrap), false);
      const projected = projectAutomaticRecoveryContinuationContext([
        { role: "custom", ...sent[0].message, timestamp: 1 },
      ] as never);
      assert.equal(runtime.controlPlane.confirmAutomaticRecoveryContinuationContext(
        replacement.ownership,
        projected.observedProjectionIds,
      ), true);

      runtime.controlPlane.activateTurn(replacement.ownership);
      runtime.controlPlane.addActivationDependency(replacement.ownership, {
        kind: "operation",
        dependencyId: "continued-after-persisted-inbox-batch",
      });
      runtime.controlPlane.settleActivation(replacement.ownership);
      assert.equal(runtime.inspectActivation(reference)?.turnSequence, 2);
      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
      assert.equal(sent.length, 1, "one provider turn consumes the already-persisted batch");
    } finally {
      await inbox.close();
      childRuntime.close();
      runtime.close();
    }
  });

  it("continues a result-pending Human tool replay but leaves a pending Human Interrupt owner-paused", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "answer-ready");
    runtime.bindHumanResponse(first, "answer-ready", "accepted-input");
    runtime.prepareHumanResponseResult(first, "answer-ready");
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });

    const replay = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "replay-replacement")!;
    runtime.controlPlane.startActivation(replay.ownership);
    assert.equal(runtime.controlPlane.claimAutomaticRecoveryContinuation(replay.ownership), false,
      "the Human bridge must append the canonical tool result before it wakes a recovery turn");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "result-pending");
    runtime.controlPlane.resumeHumanResponseResult(replay.ownership, "answer-ready");
    runtime.controlPlane.resumeHumanResponseResult(replay.ownership, "answer-ready");
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active",
      "reconciliation may retry while sendMessage remains unacknowledged");

    runtime.controlPlane.failAgentRun(replay.ownership, { error: "replacement failed" });
    runtime.close();

    const pendingScenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: pendingRuntime } = pendingScenario.createOwner();
    const pendingSession = pendingScenario.childSession(pendingRuntime, "worker");
    const pendingAgent = pendingRuntime.addAgent({ session: pendingSession, spawner: pendingRuntime.owner(), name: "Worker" });
    const pendingReference = pendingRuntime.agent(pendingAgent.agentId);
    const pendingFirst = pendingRuntime.startAgentRun(pendingReference);
    persistLaunchPolicy(pendingRuntime.workflow.databasePath, pendingAgent.agentId);
    pendingRuntime.beginHumanInterrupt(pendingFirst, "awaiting-user");
    pendingRuntime.confirmAgentRunExit(pendingFirst, { error: "runtime disappeared" });
    const pendingReplacement = pendingRuntime.controlPlane.claimRecoveryRun(pendingFirst.ownership.runId, "pending-replacement")!;
    pendingRuntime.controlPlane.startActivation(pendingReplacement.ownership);
    assert.equal(pendingRuntime.controlPlane.claimAutomaticRecoveryContinuation(pendingReplacement.ownership), false);
    assert.deepEqual(pendingRuntime.inspectActivation(pendingReference)?.state, {
      kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    pendingRuntime.close();
  });

  it("resolves active recovery when the replacement declares a new Human wait", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.addActivationDependency(first, {
      kind: "operation",
      dependencyId: "operation-completed-before-replacement",
    });
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });

    const replacement = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "human-wait-replacement")!;
    runtime.controlPlane.startActivation(replacement.ownership);
    runtime.controlPlane.satisfyActivationDependency(replacement.ownership, {
      kind: "operation",
      dependencyId: "operation-completed-before-replacement",
    });
    runtime.controlPlane.activateTurn(replacement.ownership);
    runtime.controlPlane.beginHumanInterrupt(replacement.ownership, "new-human-decision");

    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting",
      dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    runtime.close();
  });

  it("preserves a pending Human Interrupt and exhausts recovery after the only replacement fails", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);

    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "ask-user");
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });

    assert.deepEqual(runtime.controlPlane.inspectActivationRecovery(reference), {
      failedActivationId: first.ownership.runId,
      agentId: agent.agentId,
      state: "pending",
      createdAtMs: scenario.clock.now(),
      updatedAtMs: scenario.clock.now(),
    });
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    assert.equal(runtime.hasHumanAttention(reference), true);
    assert.equal(runtime.controlPlane.isRecoveryOwnedFailedRun(first.ownership), true);
    assert.deepEqual((runtime.inspectTarget({ agent: agent.agentId }) as any).recovery, { state: "pending" });

    const claim = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "automatic-replacement");
    assert.equal(claim?.recovery.state, "launching");
    assert.equal(runtime.controlPlane.isRecoveryOwnedFailedRun(first.ownership), true,
      "Owner launch claim must retain suppression ownership of the failed run");
    assert.equal(runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "duplicate"), undefined);
    const ownership = claim!.ownership;
    runtime.controlPlane.startActivation(ownership);
    assert.equal(runtime.controlPlane.isRecoveryOwnedFailedRun(first.ownership), true,
      "active replacement must retain suppression ownership of the failed run");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    assert.equal(runtime.hasHumanAttention(reference), true);

    runtime.controlPlane.failAgentRun(ownership, { error: "replacement disappeared" });
    const recovery = runtime.controlPlane.inspectActivationRecovery(reference);
    assert.equal(recovery?.state, "exhausted");
    assert.equal(recovery?.exhaustionActivationId, "automatic-replacement");
    assert.deepEqual(runtime.controlPlane.claimableRecoveryEpisodes(), []);
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    runtime.close();
  });

  it("suppresses legacy relay when the Owner claim interleaves after watcher failure", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "interleaved-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Interleaved Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(failed, "interleaved-ask");
    let claimed = false;
    let resultMessages = 0;
    let wakes = 0;

    const shouldRelay = handleAgentRunWatcherCompletion({
      isCancellationOwnedRun() { return false; },
      wasProtocolCompleted() { return false; },
      runTerminated(ownership, confirmed, failure) {
        assert.equal(ownership, failed.ownership);
        assert.equal(confirmed, true);
        runtime.confirmAgentRunExit(failed, failure);
        claimed = Boolean(runtime.controlPlane.claimRecoveryRun(
          failed.ownership.runId,
          "interleaved-replacement",
        ));
      },
      isRecoveryOwnedFailedRun(ownership) {
        return runtime.controlPlane.isRecoveryOwnedFailedRun(ownership);
      },
    }, failed.ownership, {
      termination: "confirmed",
      exitCode: 1,
      errorMessage: "watcher observed failure",
    }, () => {});
    if (shouldRelay) {
      resultMessages += 1;
      wakes += 1;
    }

    assert.equal(claimed, true);
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "launching");
    assert.equal(shouldRelay, false);
    assert.equal(resultMessages, 0, "the legacy watcher must not emit subagent_result");
    assert.equal(wakes, 0, "the legacy watcher must not wake the Owner");
    runtime.close();
  });

  it("does not requeue a recovery launch after its exact ownership is lost", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "stale-launch-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Stale Launch Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "stale-launch-ask");
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });

    const claim = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "stale-launch-replacement")!;
    runtime.controlPlane.releaseAgentRun(claim.ownership);
    assert.throws(
      () => runtime.controlPlane.abandonRecoveryEpisodeLaunch(
        first.ownership.runId,
        claim.ownership,
        "stale launch cleanup",
      ),
      (error: unknown) => (error as { code?: string }).code === "OwnershipLost",
    );
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "launching");
    runtime.close();
  });

  it("preserves an interrupted launch fence across Owner restart until exact liveness is reconciled", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "recover-after-crash");
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });

    const interrupted = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "interrupted-replacement")!;
    assert.equal(interrupted.recovery.state, "launching");
    assert.equal(runtime.currentAgentRun(reference)?.runId, "interrupted-replacement");

    runtime.restart();

    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "launching");
    assert.equal(runtime.currentAgentRun(reference)?.runId, "interrupted-replacement");
    assert.equal(runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "duplicate"), undefined,
      "unknown process liveness must preserve the fence and prevent a duplicate launch");
    runtime.controlPlane.startActivation(interrupted.ownership);
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "active");

    runtime.restart();
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "active");
    assert.equal(runtime.currentAgentRun(reference)?.runId, "interrupted-replacement");
    runtime.close();
  });

  it("transfers pending recovery work into a Request-driven resume before Human input binds", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "request-resumed-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Request Resumed Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(failed, "request-resume-human");
    runtime.confirmAgentRunExit(failed, { error: "worker failed with Human input pending" });
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "pending");

    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      const accepted = messages.acceptEndedRecipientRequest({
        request: {
          workflowOwnerId: runtime.workflow.ownerAgentId,
          messageId: "request-driven-resume",
          senderAgentId: runtime.workflow.ownerAgentId,
          recipientAgentId: agent.agentId,
          sourceEntryId: "request-driven-resume-source",
          payloadDigest: "request-driven-resume-digest",
          deliveryTiming: "steer",
          responseRequired: true,
          message: "resume after the Human answers",
        },
        recipient: reference,
        endpoint: "prepared://request-driven-resume",
        runId: "request-driven-replacement",
        checkpoint: JSON.stringify({ surface: "request-driven-replacement" }),
        acceptedAtMs: scenario.clock.now(),
      });
      const resumed = { processId: accepted.ownership.runId, ownership: accepted.ownership };

      assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
      assert.equal(runtime.inspectActivation(reference)?.runId, accepted.ownership.runId);
      assert.deepEqual(runtime.inspectActivation(reference)?.state, {
        kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
      });
      assert.equal(messages.inspectRequest(runtime.workflow.ownerAgentId, "request-driven-resume")?.responderActivationId, accepted.ownership.runId);
      assert.equal(runtime.controlPlane.startActivation(accepted.ownership).activationId, accepted.ownership.runId,
        "child bootstrap must bind to the existing Request-driven activation rather than create another");
      assert.equal(runtime.inspectActivation(reference)?.sequence, 2);

      assert.equal(runtime.bindHumanResponse(resumed, "request-resume-human", "request-resume-input")?.status, "response-bound");
      assert.equal(runtime.prepareHumanResponseResult(resumed, "request-resume-human").status, "result-pending");
      assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    } finally {
      messages.close();
      runtime.close();
    }
  });

  it("resolves pending and blocked-policy recovery inspection when a manual activation supersedes it", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "legacy-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Legacy Worker" });
    const reference = runtime.agent(agent.agentId);
    const failed = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(failed, "legacy-ask");
    runtime.confirmAgentRunExit(failed, { error: "legacy runtime lost" });

    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "blocked-policy");
    const manual = runtime.startAgentRun(reference);
    assert.equal((runtime.inspectTarget({ agent: agent.agentId }) as any).recovery.state, "resolved");
    runtime.cancelActivation(manual);
    runtime.close();
  });

  it("lets a manual resume supersede an unclaimed automatic episode", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "manual-ask");
    runtime.confirmAgentRunExit(first, { error: "runtime lost" });
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "pending");

    const manual = runtime.startAgentRun(reference);
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    runtime.cancelActivation(manual);
    runtime.close();
  });

  it("resolves a true automatic replacement on completion or cancellation without another claim", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.settleActivation(first);
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });
    const completionReplacement = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "completion-replacement")!;
    runtime.controlPlane.startActivation(completionReplacement.ownership);
    const completion = new CompletionGateStore(runtime.workflow.databasePath);
    try {
      completion.complete(completionReplacement.ownership, { kind: "standalone", toolCallId: "complete-replacement" }, scenario.clock.now());
    } finally {
      completion.close();
    }
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    assert.equal(runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "later-claim"), undefined);

    const second = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(second, "cancel-ask");
    runtime.confirmAgentRunExit(second, { error: "runtime disappeared again" });
    const replacementProcess = runtime.processAdapter.prepare(agent.agentId);
    const cancellationReplacement = runtime.controlPlane.claimRecoveryRun(second.ownership.runId, replacementProcess.processId)!;
    runtime.controlPlane.startActivation(cancellationReplacement.ownership);
    runtime.processAdapter.activate(replacementProcess.processId);
    runtime.cancelActivation({ processId: replacementProcess.processId, ownership: cancellationReplacement.ownership });
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    assert.equal(runtime.controlPlane.claimRecoveryRun(second.ownership.runId, "later-cancel-claim"), undefined);
    runtime.close();
  });

  it("fences a pre-activation automatic claim against manual resume and stale start", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);
    runtime.beginHumanInterrupt(first, "fenced-ask");
    runtime.confirmAgentRunExit(first, { error: "runtime disappeared" });
    const automatic = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "automatic-before-start")!;

    assert.throws(() => runtime.startAgentRun(reference), /already owned/i);
    runtime.controlPlane.abandonRecoveryEpisodeLaunch(first.ownership.runId, automatic.ownership, "pane never started");
    assert.throws(() => runtime.controlPlane.startActivation(automatic.ownership), /no longer owns/i);

    const manual = runtime.startAgentRun(reference);
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    runtime.cancelActivation(manual);
    runtime.close();
  });

  it("retains an undeclared correction episode until replacement settlement declares work", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    persistLaunchPolicy(runtime.workflow.databasePath, agent.agentId);

    runtime.settleActivation(first);
    const correction = runtime.inspectUndeclaredEpisode(reference)!;
    runtime.confirmAgentRunExit(first, { error: "lost during correction" });
    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "pending");
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.episodeId, correction.episodeId);
    assert.equal(runtime.pendingUndeclaredNotice(reference)?.episodeId, correction.episodeId);

    const claim = runtime.controlPlane.claimRecoveryRun(first.ownership.runId, "declaring-replacement");
    assert.equal(claim?.recovery.state, "launching");
    const ownership = claim!.ownership;
    runtime.controlPlane.startActivation(ownership);
    runtime.controlPlane.addActivationDependency(ownership, { kind: "operation", dependencyId: "durable-work" });
    runtime.controlPlane.settleActivation(ownership);

    assert.equal(runtime.controlPlane.inspectActivationRecovery(reference)?.state, "resolved");
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.episodeId, correction.episodeId);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.status, "open");
    runtime.close();
  });

  it("blocks automatic recovery when a legacy Agent has no persisted launch policy", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "legacy-worker");
    const agent = runtime.addAgent({ session, spawner: runtime.owner(), name: "Legacy Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(run, "legacy-ask");
    runtime.confirmAgentRunExit(run, { error: "legacy runtime lost" });

    const recovery = runtime.controlPlane.inspectActivationRecovery(reference);
    assert.equal(recovery?.state, "blocked-policy");
    assert.match(recovery?.detail ?? "", /persisted launch policy/i);
    assert.equal(runtime.controlPlane.isRecoveryOwnedFailedRun(run.ownership), true,
      "blocked recovery still owns the failure and must not fabricate a legacy result");
    assert.deepEqual(runtime.controlPlane.claimableRecoveryEpisodes(), []);
    runtime.close();
  });
});
