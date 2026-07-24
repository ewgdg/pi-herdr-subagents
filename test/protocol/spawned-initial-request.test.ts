import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, it } from "node:test";
import { WorkflowProtocolError } from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import { DirectSignalRuntime } from "../../pi-extension/subagents/protocol/direct-signal.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { digestPayload } from "../../pi-extension/subagents/protocol/direct-signal-transcript.ts";
import { WorkflowScenario } from "./scenario-harness.ts";

async function scenarioForTest(): Promise<WorkflowScenario> {
  return new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "pi-herdr-spawn-request-")) });
}

function isProtocolError(code: WorkflowProtocolError["code"]): (error: unknown) => boolean {
  return (error) => error instanceof WorkflowProtocolError && error.code === code;
}

describe("Spawned Initial Request", () => {
  it("atomically creates the direct child and its initial Request", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" },
      activationIntent: "Investigate failing test",
      message: "Investigate the failing test.",
    });
    const messageId = scenario.identities.next();
    const launchPolicy = {
      toolAllowlist: "read,caller_ping,subagent_done",
      denyTools: ["subagent", "subagent_interrupt", "subagent_resume"],
      codingAgentDir: "/restricted/.pi/agent",
    };

    const receipt = runtime.spawnInitialRequest({
      child,
      runId: scenario.identities.next(),
      messageId,
      sourceEntryId,
      message: "Investigate the failing test.",
      activationIntent: "Investigate failing test",
      agentDefinition: "worker",
      name: "Worker",
      launchPolicy,
      routerEndpoint: "ready://worker",
    });

    assert.equal(receipt.status, "delivered");
    assert.equal(receipt.messageId, messageId);
    assert.notEqual(receipt.messageId, sourceEntryId);
    assert.equal(runtime.directChildren(runtime.agent(runtime.workflow.ownerAgentId)).length, 1);
    assert.equal(runtime.inspect(runtime.agent(child.agentId)).spawnerAgentId, runtime.workflow.ownerAgentId);
    assert.equal(runtime.inspect(runtime.agent(child.agentId)).delegationPolicy, "approval-required");
    assert.deepEqual(runtime.inspect(runtime.agent(child.agentId)).launchPolicy, launchPolicy);
    assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "active");
    assert.deepEqual(runtime.listRequests(runtime.agent(runtime.workflow.ownerAgentId)), [{
      requestId: receipt.messageId,
      requesterAgentId: runtime.workflow.ownerAgentId,
      responderAgentId: child.agentId,
      responderActivationId: receipt.runId,
      answerDeliveryTiming: "steer",
      status: "open",
    }]);
    assert.deepEqual(runtime.listPending(runtime.agent(child.agentId)), []);
  });

  it("lets an autonomous ordinary Agent atomically spawn its direct child", async () => {
    const scenario = await scenarioForTest();
    const { runtime } = scenario.createOwner();
    const parentSession = scenario.childSession(runtime, "autonomous-parent");
    const parent = runtime.addAgent({
      session: parentSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Autonomous Parent",
      delegationPolicy: "autonomous",
    });
    runtime.startAgentRun(runtime.agent(parent.agentId));
    const parentRuntime = scenario.startAgent(runtime.workflow, parentSession);
    const child = scenario.childSession(parentRuntime, "autonomous-child");
    const sourceEntryId = scenario.transcripts.appendAgentSend(parentSession, {
      targetSpawn: { agent: "worker", name: "Nested Worker" },
      activationIntent: "Handle nested work",
      message: "Handle nested work.",
    });

    const receipt = parentRuntime.spawnInitialRequest({
      child,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Handle nested work.",
      activationIntent: "Handle nested work",
      agentDefinition: "worker",
      name: "Nested Worker",
      routerEndpoint: "ready://nested-worker",
    });

    assert.equal(receipt.status, "delivered");
    assert.equal(runtime.inspect(runtime.agent(child.agentId)).spawnerAgentId, parent.agentId);
    parentRuntime.close();
    runtime.close();
  });

  it("rejects an ordinary Spawned Initial Request when delegation policy requires approval", async () => {
    const scenario = await scenarioForTest();
    const { runtime } = scenario.createOwner();
    const parentSession = scenario.childSession(runtime, "approval-parent");
    const parent = runtime.addAgent({
      session: parentSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Approval Parent",
    });
    const parentRuntime = scenario.startAgent(runtime.workflow, parentSession);
    const child = scenario.childSession(parentRuntime, "approval-child");
    const sourceEntryId = scenario.transcripts.appendAgentSend(parentSession, {
      targetSpawn: { agent: "worker", name: "Held Worker" },
      activationIntent: "Held nested work",
      message: "Held nested work.",
    });

    assert.throws(() => parentRuntime.spawnInitialRequest({
      child,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Held nested work.",
      activationIntent: "Held nested work",
      agentDefinition: "worker",
      name: "Held Worker",
      routerEndpoint: "ready://held-worker",
    }), isProtocolError("DelegatedActivationApprovalRequired"));
    assert.equal(runtime.descendants().some((agent) => agent.agentId === child.agentId), false);
    assert.deepEqual(parentRuntime.listRequests(parentRuntime.agent(parent.agentId)), []);
    parentRuntime.close();
    runtime.close();
  });

  it("rejects an ordinary Spawned Initial Request when delegation policy is disabled", async () => {
    const scenario = await scenarioForTest();
    const { runtime } = scenario.createOwner();
    const parentSession = scenario.childSession(runtime, "disabled-parent");
    const parent = runtime.addAgent({
      session: parentSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Disabled Parent",
      delegationPolicy: "disabled",
    });
    const parentRuntime = scenario.startAgent(runtime.workflow, parentSession);
    const child = scenario.childSession(parentRuntime, "disabled-child");
    const sourceEntryId = scenario.transcripts.appendAgentSend(parentSession, {
      targetSpawn: { agent: "worker", name: "Blocked Worker" },
      activationIntent: "Blocked nested work",
      message: "Blocked nested work.",
    });

    assert.throws(() => parentRuntime.spawnInitialRequest({
      child,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Blocked nested work.",
      activationIntent: "Blocked nested work",
      agentDefinition: "worker",
      name: "Blocked Worker",
      routerEndpoint: "ready://blocked-worker",
    }), isProtocolError("SpawnerDelegationDisabled"));
    assert.equal(runtime.descendants().some((agent) => agent.agentId === child.agentId), false);
    assert.deepEqual(parentRuntime.listRequests(parentRuntime.agent(parent.agentId)), []);
    parentRuntime.close();
    runtime.close();
  });

  it("keeps Spawned Request identities Workflow-unique when Agents share a tool-call ID", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" },
      activationIntent: "Handle shared source identity",
      message: "Shared source identity.",
    });
    const clonedSpawner = scenario.cloneSession(runtime, owner, "cloned-spawner");
    runtime.addAgent({
      session: clonedSpawner,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Cloned spawner",
      delegationPolicy: "autonomous",
    });
    const clonedRuntime = scenario.startAgent(runtime.workflow, clonedSpawner);
    runtime.startAgentRun(runtime.agent(clonedSpawner.agentId));
    const ownerChild = scenario.childSession(runtime, "owner-worker");
    const clonedChild = scenario.childSession(clonedRuntime, "cloned-worker");
    const ownerReceipt = runtime.spawnInitialRequest({
      child: ownerChild,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Shared source identity.",
      activationIntent: "Handle shared source identity",
      agentDefinition: "worker",
      name: "Worker",
      routerEndpoint: "ready://owner-worker",
    });
    const clonedReceipt = clonedRuntime.spawnInitialRequest({
      child: clonedChild,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Shared source identity.",
      activationIntent: "Handle shared source identity",
      agentDefinition: "worker",
      name: "Worker",
      routerEndpoint: "ready://cloned-worker",
    });

    assert.notEqual(ownerReceipt.messageId, clonedReceipt.messageId);
  });

  it("reconciles only the exact committed prepared Router footprint", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Reconcile spawned request", message: "Reconcile me.",
    });
    const receipt = runtime.spawnInitialRequest({
      child, runId: "committed-run", messageId: scenario.identities.next(), sourceEntryId, message: "Reconcile me.", activationIntent: "Reconcile spawned request",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "prepared://worker",
    });
    const store = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      assert.deepEqual(store.reconcilePreparedRecipientRouter({
        recipient: runtime.agent(child.agentId), endpoint: "prepared://worker",
      }), {
        workflowOwnerId: runtime.workflow.ownerAgentId, agentId: child.agentId,
        runId: receipt.runId, epoch: receipt.fencingEpoch,
        resourceId: `agent-run:${runtime.workflow.ownerAgentId}:${child.agentId}`,
      });
      assert.throws(() => store.reconcilePreparedRecipientRouter({
        recipient: runtime.agent(child.agentId), endpoint: "stale://worker",
      }), isProtocolError("AgentRunAlreadyOwned"));
    } finally {
      store.close();
    }
  });

  it("proves a disconnected precommit Router has no committed footprint", async () => {
    const scenario = await scenarioForTest();
    const { runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "uncommitted-worker");
    const store = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      assert.equal(store.reconcilePreparedRecipientRouter({
        recipient: runtime.agent(child.agentId), endpoint: "prepared://uncommitted",
      }), undefined);
    } finally {
      store.close();
    }
  });

  it("leaves no durable child effects when readiness is unavailable", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "unready-worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Unready worker" },
      activationIntent: "Do not start unready worker",
      message: "Do not start.",
    });

    assert.throws(() => runtime.spawnInitialRequest({
      child,
      runId: scenario.identities.next(),
      messageId: scenario.identities.next(),
      sourceEntryId,
      message: "Do not start.",
      activationIntent: "Do not start unready worker",
      agentDefinition: "worker",
      name: "Unready worker",
    }), isProtocolError("RecipientUnreachable"));

    assert.equal(runtime.directChildren(runtime.agent(runtime.workflow.ownerAgentId)).length, 0);
    assert.deepEqual(runtime.listRequests(runtime.agent(runtime.workflow.ownerAgentId)), []);
  });

  it("rolls back membership, activation, and Request after a late durable failure", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "rollback-worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Rollback worker" }, activationIntent: "Rollback spawned worker", message: "Rollback this spawn.",
    });
    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    messages.close();
    const database = new DatabaseSync(runtime.workflow.databasePath);
    database.exec(`
      CREATE TRIGGER reject_spawn_message
      BEFORE INSERT ON direct_signal_messages
      WHEN NEW.recipient_agent_id = '${child.agentId}'
      BEGIN SELECT RAISE(ABORT, 'forced spawned Request failure'); END;
    `);
    try {
      assert.throws(() => runtime.spawnInitialRequest({
        child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId, message: "Rollback this spawn.", activationIntent: "Rollback spawned worker",
        agentDefinition: "worker", name: "Rollback worker", routerEndpoint: "ready://rollback-worker",
      }), /forced spawned Request failure/);
    } finally {
      database.exec("DROP TRIGGER reject_spawn_message");
      database.close();
    }
    assert.equal(runtime.directChildren(runtime.agent(runtime.workflow.ownerAgentId)).length, 0);
    assert.deepEqual(runtime.listRequests(runtime.agent(runtime.workflow.ownerAgentId)), []);
  });

  it("reconciles a lost acknowledgement and rejects changed spawn bindings", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Work once", message: "Work once.",
    });
    const input = {
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId, message: "Work once.", activationIntent: "Work once",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "ready://worker",
    };

    const first = runtime.spawnInitialRequest(input);
    const retried = runtime.spawnInitialRequest({ ...input, messageId: scenario.identities.next() });
    assert.deepEqual(retried, first);
    assert.equal(runtime.directChildren(runtime.agent(runtime.workflow.ownerAgentId)).length, 1);
    assert.equal(runtime.listRequests(runtime.agent(runtime.workflow.ownerAgentId)).length, 1);
    assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.sequence, 1);

    const changedChild = scenario.childSession(runtime, "changed-worker");
    assert.throws(() => runtime.spawnInitialRequest({
      ...input, child: changedChild, runId: scenario.identities.next(), routerEndpoint: "ready://changed-worker",
    }), isProtocolError("MessageIdentityConflict"));
  });

  it("reconciles only an exact committed Spawned Initial Request before launch", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker", delegationPolicy: "autonomous" }, activationIntent: "Do this once", message: "Do this once.",
    });

    assert.equal(runtime.controlPlane.reconcileSpawnedInitialRequest({
      sourceEntryId: "absent-source", agentDefinition: "worker", name: "Worker", message: "Do this once.", activationIntent: "Do this once",
      delegationPolicy: "autonomous",
    }), undefined);

    const receipt = runtime.spawnInitialRequest({
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId,
      message: "Do this once.", activationIntent: "Do this once", agentDefinition: "worker", name: "Worker", delegationPolicy: "autonomous", routerEndpoint: "ready://worker",
    });
    assert.deepEqual(runtime.controlPlane.reconcileSpawnedInitialRequest({
      sourceEntryId, agentDefinition: "worker", name: "Worker", message: "Do this once.", activationIntent: "Do this once",
      delegationPolicy: "autonomous",
    }), {
      status: "delivered", messageId: receipt.messageId, recipientAgentId: child.agentId, acceptanceSequence: 1,
    });

    runtime.controlPlane.failAgentRun({
      workflowOwnerId: runtime.workflow.ownerAgentId, agentId: child.agentId, runId: receipt.runId,
      epoch: receipt.fencingEpoch, resourceId: `agent-run:${runtime.workflow.ownerAgentId}:${child.agentId}`,
    }, { error: "first activation ended" });
    const second = runtime.controlPlane.acquireAgentRun(runtime.agent(child.agentId), scenario.identities.next());
    runtime.controlPlane.startActivation(second);
    assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.sequence, 2);
    assert.deepEqual(runtime.controlPlane.reconcileSpawnedInitialRequest({
      sourceEntryId, agentDefinition: "worker", name: "Worker", message: "Do this once.", activationIntent: "Do this once", delegationPolicy: "autonomous",
    }), { status: "delivered", messageId: receipt.messageId, recipientAgentId: child.agentId, acceptanceSequence: 1 });
    assert.throws(() => runtime.controlPlane.reconcileSpawnedInitialRequest({
      sourceEntryId, agentDefinition: "worker", name: "Worker", message: "Do this once.", activationIntent: "Do this once",
      delegationPolicy: "disabled",
    }), isProtocolError("MessageIdentityConflict"));

    for (const changed of [
      { agentDefinition: "reviewer", name: "Worker", message: "Do this once." },
      { agentDefinition: "worker", name: "Other Worker", message: "Do this once." },
      { agentDefinition: "worker", name: "Worker", message: "Do something else." },
    ]) {
      assert.throws(() => runtime.controlPlane.reconcileSpawnedInitialRequest({ sourceEntryId, ...changed, activationIntent: "Do this once", delegationPolicy: "autonomous" }), isProtocolError("MessageIdentityConflict"));
    }
  });

  it("does not reconcile an ordinary same-source Request as a spawn", async () => {
    const scenario = await scenarioForTest();
    const { runtime } = scenario.createOwner();
    const sourceEntryId = "ordinary-request-source";
    const store = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      store.bindMessage({
        messageId: scenario.identities.next(), sender: runtime.agent(runtime.workflow.ownerAgentId),
        recipient: runtime.agent(runtime.workflow.ownerAgentId), sourceEntryId,
        payloadDigest: digestPayload("Ordinary Request."), deliveryTiming: "steer", responseRequired: true,
        createdAtMs: scenario.clock.now(),
      });
    } finally {
      store.close();
    }

    assert.throws(() => runtime.controlPlane.reconcileSpawnedInitialRequest({
      sourceEntryId, agentDefinition: "worker", name: "Worker", message: "Ordinary Request.",
      activationIntent: "Ordinary request",
      delegationPolicy: "autonomous",
    }), isProtocolError("MessageIdentityConflict"));
  });

  it("preserves the spawned Agent identity on durable session resume", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Persist identity", message: "Persist identity.",
    });
    runtime.spawnInitialRequest({
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId, message: "Persist identity.", activationIntent: "Persist identity",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "ready://worker",
    });

    const resumed = scenario.transcripts.resume(child.sessionPath);
    assert.equal(resumed.agentId, child.agentId);
    assert.equal(scenario.startAgent(runtime.workflow, resumed).controlPlane.currentAgent.agentId, child.agentId);
  });

  it("lets only the direct Spawner reactivate interrupted work with a Request", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const initialSource = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Initial work", message: "Initial work.",
    });
    runtime.spawnInitialRequest({
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId: initialSource, message: "Initial work.", activationIntent: "Initial work",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "ready://worker",
    });
    const childRuntime = scenario.startAgent(runtime.workflow, child);
    const childOwnership = runtime.currentAgentRun(runtime.agent(child.agentId))!;
    const projected: import("../../pi-extension/subagents/protocol/direct-signal-types.ts").InboxBatch[] = [];
    const childMessages = new DirectSignalRuntime({
      controlPlane: childRuntime.controlPlane,
      ownership: childOwnership,
      projectInboxBatch(batch) { projected.push(batch); },
      now: scenario.clock.now,
    });
    const ownerMessages = new DirectSignalRuntime({
      controlPlane: runtime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch() {},
      now: scenario.clock.now,
    });
    try {
      await childMessages.start();
      await ownerMessages.start();
      const interruption = runtime.requestInterruption({ processId: "spawned", ownership: childOwnership });
      runtime.confirmInterruption({ processId: "spawned", ownership: childOwnership }, interruption);
      assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "interrupted");

      const source = scenario.transcripts.appendAgentSend(owner, {
        targetAgentId: child.agentId, message: "Resume this work.", timing: "deferred", responseRequired: true,
      });
      const receipt = await ownerMessages.sendMessage({
        target: { agentId: child.agentId }, message: "Resume this work.", sourceEntryId: source,
        deliveryTiming: "deferred", responseRequired: true,
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "active");
      assert.equal(projected.length, 1);
      assert.equal(projected[0].messages[0].messageId, receipt.messageId);
      assert.equal(childMessages.confirmDelivery(receipt.messageId), true);
      assert.deepEqual(runtime.listPending(runtime.agent(child.agentId)), []);
    } finally {
      await ownerMessages.close();
      await childMessages.close();
    }
  });

  it("keeps interrupted work interrupted for peer Signals and rejects peer Requests", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const initialSource = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Initial work", message: "Initial work.",
    });
    runtime.spawnInitialRequest({
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId: initialSource, message: "Initial work.", activationIntent: "Initial work",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "ready://worker",
    });
    const peerSession = scenario.childSession(runtime, "peer");
    const ownerRef = runtime.agent(runtime.workflow.ownerAgentId);
    runtime.addAgent({ session: peerSession, spawner: ownerRef, name: "Peer" });
    const childRuntime = scenario.startAgent(runtime.workflow, child);
    const peerRuntime = scenario.startAgent(runtime.workflow, peerSession);
    const childOwnership = runtime.currentAgentRun(runtime.agent(child.agentId))!;
    const childMessages = new DirectSignalRuntime({
      controlPlane: childRuntime.controlPlane, ownership: childOwnership,
      projectInboxBatch() {}, hasProjectedMessage() { return true; }, now: scenario.clock.now,
    });
    const peerMessages = new DirectSignalRuntime({
      controlPlane: peerRuntime.controlPlane, allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch() {}, now: scenario.clock.now,
    });
    try {
      await childMessages.start();
      const interruption = runtime.requestInterruption({ processId: "spawned", ownership: childOwnership });
      runtime.confirmInterruption({ processId: "spawned", ownership: childOwnership }, interruption);
      const signalSource = scenario.transcripts.appendAgentSend(peerSession, {
        targetAgentId: child.agentId, message: "Peer signal.",
      });
      await peerMessages.sendMessage({
        target: { agentId: child.agentId }, message: "Peer signal.", sourceEntryId: signalSource,
      });
      assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "interrupted");

      const requestSource = scenario.transcripts.appendAgentSend(peerSession, {
        targetAgentId: child.agentId, message: "Peer request.", responseRequired: true,
      });
      await assert.rejects(() => peerMessages.sendMessage({
        target: { agentId: child.agentId }, message: "Peer request.", sourceEntryId: requestSource, responseRequired: true,
      }), isProtocolError("RecipientReactivationUnauthorized"));
      assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "interrupted");
    } finally {
      await peerMessages.close();
      await childMessages.close();
    }
  });

  it("lets the Workflow Owner create a new activation for ended work through a Request", async () => {
    const scenario = await scenarioForTest();
    const { session: owner, runtime } = scenario.createOwner();
    const child = scenario.childSession(runtime, "worker");
    const initialSource = scenario.transcripts.appendAgentSend(owner, {
      targetSpawn: { agent: "worker", name: "Worker" }, activationIntent: "Initial work", message: "Initial work.",
    });
    runtime.spawnInitialRequest({
      child, runId: scenario.identities.next(), messageId: scenario.identities.next(), sourceEntryId: initialSource, message: "Initial work.", activationIntent: "Initial work",
      agentDefinition: "worker", name: "Worker", routerEndpoint: "ready://worker",
    });
    const firstOwnership = runtime.currentAgentRun(runtime.agent(child.agentId))!;
    const ownerMessages = new DirectSignalRuntime({
      controlPlane: runtime.controlPlane, allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch() {}, now: scenario.clock.now,
    });
    try {
      runtime.controlPlane.failAgentRun(firstOwnership, { error: "ended before a new Request" });
      const source = scenario.transcripts.appendAgentSend(owner, {
        targetAgentId: child.agentId, message: "New work after ending.", activationIntent: "Restart ended work", responseRequired: true,
      });
      await ownerMessages.sendMessage({
        target: { agentId: child.agentId }, message: "New work after ending.", sourceEntryId: source, responseRequired: true, activationIntent: "Restart ended work",
        async prepareEndedRecipient(request) {
          const store = new DirectSignalStore(runtime.workflow.databasePath);
          try {
            return store.acceptEndedRecipientRequest({
              request,
              recipient: runtime.agent(child.agentId),
              endpoint: "prepared://ended-worker",
              runId: scenario.identities.next(),
              checkpoint: JSON.stringify({ surface: "ended-worker" }),
              acceptedAtMs: scenario.clock.now(),
            });
          } finally {
            store.close();
          }
        },
      });
      const activation = runtime.inspectActivation(runtime.agent(child.agentId));
      assert.equal(activation?.state.kind, "active");
      assert.equal(activation?.sequence, 2);
      assert.notEqual(activation?.runId, firstOwnership.runId);
    } finally {
      await ownerMessages.close();
    }
  });
});
