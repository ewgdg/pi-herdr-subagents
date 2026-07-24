import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, it } from "node:test";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { WorkflowScenario, type ControllableRuntimeAdapter, type ScenarioAgentRun } from "./scenario-harness.ts";

const temporaryDirectories: string[] = [];

async function scenario() {
  const rootDirectory = await mkdtemp(join(tmpdir(), "operational-incidents-"));
  temporaryDirectories.push(rootDirectory);
  return new WorkflowScenario({ rootDirectory });
}

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((directory) =>
    rm(directory, { recursive: true, force: true })));
});

function acceptDeliveredRequest(input: {
  runtime: ControllableRuntimeAdapter;
  senderRun: ScenarioAgentRun;
  recipientRun: ScenarioAgentRun;
  messageId: string;
  responseRequired?: boolean;
}): void {
  const { runtime, senderRun, recipientRun, messageId } = input;
  const responseRequired = input.responseRequired ?? true;
  const store = new DirectSignalStore(runtime.workflow.databasePath);
  const sender = runtime.agent(senderRun.ownership.agentId);
  const recipient = runtime.agent(recipientRun.ownership.agentId);
  const endpoint = `router:${recipient.agentId}`;
  const senderEndpoint = `router:${sender.agentId}`;
  try {
    store.registerRouter({ recipient: sender, ownership: senderRun.ownership, endpoint: senderEndpoint, registeredAtMs: 1 });
    store.registerRouter({ recipient, ownership: recipientRun.ownership, endpoint, registeredAtMs: 1 });
    store.bindMessage({
      messageId, sender, recipient, sourceEntryId: `source:${messageId}`,
      payloadDigest: `digest:${messageId}`, deliveryTiming: "steer",
      responseRequired, createdAtMs: 1, ownership: senderRun.ownership,
    });
    store.acceptSignal({
      request: {
        workflowOwnerId: runtime.workflow.ownerAgentId,
        messageId,
        senderAgentId: sender.agentId,
        recipientAgentId: recipient.agentId,
        sourceEntryId: `source:${messageId}`,
        payloadDigest: `digest:${messageId}`,
        deliveryTiming: "steer",
        responseRequired,
        message: messageId,
      },
      recipient, ownership: recipientRun.ownership, endpoint, acceptedAtMs: 2,
    });
    assert.equal(store.commitDelivery({
      recipient, ownership: recipientRun.ownership, endpoint, messageId, deliveredAtMs: 3,
    }), "newly-delivered");
  } finally {
    store.close();
  }
}

describe("Operational Incidents", () => {
  it("confirms only a closed component waiting solely on its internal Requests", async () => {
    const workflow = await scenario();
    const { session: ownerSession, runtime } = workflow.createOwner();
    const firstSession = workflow.childSession(runtime, "first");
    const secondSession = workflow.childSession(runtime, "second");
    const first = runtime.addAgent({ session: firstSession, spawner: runtime.owner(), name: "First" });
    const second = runtime.addAgent({ session: secondSession, spawner: runtime.owner(), name: "Second" });
    const firstRun = runtime.startAgentRun(runtime.agent(first.agentId));
    const secondRun = runtime.startAgentRun(runtime.agent(second.agentId));
    acceptDeliveredRequest({ runtime, senderRun: firstRun, recipientRun: secondRun, messageId: "first-to-second" });
    acceptDeliveredRequest({ runtime, senderRun: secondRun, recipientRun: firstRun, messageId: "second-to-first" });

    assert.deepEqual(runtime.reconcileOperationalIncidents(), []);
    runtime.settleActivation(firstRun);
    assert.deepEqual(runtime.reconcileOperationalIncidents(), []);
    runtime.settleActivation(secondRun);

    const competingOwner = workflow.startOwner(ownerSession);
    const [incident] = runtime.reconcileOperationalIncidents();
    assert.deepEqual(
      competingOwner.reconcileOperationalIncidents().map((item) => item.incidentId),
      [incident.incidentId],
      "a competing Owner connection observes the same atomically created incident and brief",
    );
    competingOwner.close();
    assert.equal(incident.trigger.kind, "dependency-deadlock");
    assert.deepEqual(incident.trigger.seedAgentIds, [first.agentId, second.agentId].sort());
    assert.deepEqual(incident.scopeAgentIds, [first.agentId, second.agentId].sort());
    assert.deepEqual(runtime.reconcileOperationalIncidents().map((item) => item.incidentId), [incident.incidentId]);

    const brief = runtime.inspectIncidentBrief(incident.incidentId)!;
    assert.match(brief.operationalQuestion, /deadlock/i);
    assert.deepEqual(brief.scope.roster.map((agent) => agent.agentId).sort(), [first.agentId, second.agentId].sort());
    assert.deepEqual(brief.scope.unresolvedRequests.map((request) => request.requestId).sort(), ["first-to-second", "second-to-first"]);
    assert.deepEqual(brief.allowedOutcomes, ["operationally-resolved", "owner-handoff"]);
    assert.ok(brief.authorityBoundaries.length > 0);
    assert.ok(brief.diagnosticPointers.length > 0);
    runtime.restart();
    assert.deepEqual(runtime.inspectIncidentBrief(incident.incidentId), brief, "the brief survives restart unchanged");
    assert.equal(runtime.listOperationalIncidents().length, 1);

    runtime.activateTurn(firstRun);
    runtime.settleActivation(firstRun);
    const recurring = runtime.reconcileOperationalIncidents();
    assert.equal(recurring.length, 2, "durable progress starts a new episode even when polling misses the gap");
    assert.notEqual(recurring[0].incidentId, recurring[1].incidentId);
  });

  it("keeps a Request cycle legal while one member has an external Request dependency", async () => {
    const workflow = await scenario();
    const { runtime } = workflow.createOwner();
    const firstSession = workflow.childSession(runtime, "external-first");
    const secondSession = workflow.childSession(runtime, "external-second");
    const externalSession = workflow.childSession(runtime, "external-progress");
    const first = runtime.addAgent({ session: firstSession, spawner: runtime.owner(), name: "First" });
    const second = runtime.addAgent({ session: secondSession, spawner: runtime.owner(), name: "Second" });
    const external = runtime.addAgent({ session: externalSession, spawner: runtime.owner(), name: "External" });
    const firstRun = runtime.startAgentRun(runtime.agent(first.agentId));
    const secondRun = runtime.startAgentRun(runtime.agent(second.agentId));
    const externalRun = runtime.startAgentRun(runtime.agent(external.agentId));
    acceptDeliveredRequest({ runtime, senderRun: firstRun, recipientRun: secondRun, messageId: "external-cycle-a" });
    acceptDeliveredRequest({ runtime, senderRun: secondRun, recipientRun: firstRun, messageId: "external-cycle-b" });
    acceptDeliveredRequest({ runtime, senderRun: firstRun, recipientRun: externalRun, messageId: "external-dependency" });
    runtime.settleActivation(firstRun);
    runtime.settleActivation(secondRun);

    assert.deepEqual(runtime.reconcileOperationalIncidents(), []);
  });

  it("treats Human Interrupts and operation dependencies as external progress sources", async () => {
    const workflow = await scenario();
    const { runtime } = workflow.createOwner();
    const humanSession = workflow.childSession(runtime, "human");
    const operationSession = workflow.childSession(runtime, "operation");
    const human = runtime.addAgent({ session: humanSession, spawner: runtime.owner(), name: "Human" });
    const operation = runtime.addAgent({ session: operationSession, spawner: runtime.owner(), name: "Operation" });
    const humanRun = runtime.startAgentRun(runtime.agent(human.agentId));
    const operationRun = runtime.startAgentRun(runtime.agent(operation.agentId));
    runtime.beginHumanInterrupt(humanRun, "ask-user");
    runtime.addActivationDependency(operationRun, { kind: "operation", dependencyId: "side-effect:charge" });
    runtime.settleActivation(operationRun);
    workflow.clock.advance(1_000_000);
    assert.deepEqual(runtime.reconcileOperationalIncidents(), []);
  });

  it("creates one incident for each non-deadlock trigger episode", async () => {
    const workflow = await scenario();
    const { runtime } = workflow.createOwner();
    const undeclaredSession = workflow.childSession(runtime, "undeclared");
    const undeclared = runtime.addAgent({ session: undeclaredSession, spawner: runtime.owner(), name: "Undeclared" });
    const undeclaredRun = runtime.startAgentRun(runtime.agent(undeclared.agentId));
    runtime.settleActivation(undeclaredRun);
    const episode = runtime.pendingUndeclaredNotice(runtime.agent(undeclared.agentId))!;
    runtime.acceptUndeclaredNotice(runtime.agent(undeclared.agentId), episode.episodeId);
    runtime.confirmUndeclaredNotice(runtime.agent(undeclared.agentId), episode.episodeId);
    runtime.activateTurn(undeclaredRun);
    runtime.settleActivation(undeclaredRun);

    const operationSession = workflow.childSession(runtime, "uncertain");
    const operation = runtime.addAgent({ session: operationSession, spawner: runtime.owner(), name: "Uncertain" });
    const operationRun = runtime.startAgentRun(runtime.agent(operation.agentId));
    runtime.addActivationDependency(operationRun, { kind: "operation", dependencyId: "ownership:handoff" });
    runtime.settleActivation(operationRun);
    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved", eligibility: "exhausted",
      evidence: { kind: "probe", detail: "ownership remains ambiguous" },
    }));

    const recoverySession = workflow.childSession(runtime, "recovery");
    const recovery = runtime.addAgent({
      session: recoverySession, spawner: runtime.owner(), name: "Recovery",
      launchPolicy: { denyTools: [] },
    });
    const recoveryRun = runtime.startAgentRun(runtime.agent(recovery.agentId));
    runtime.addActivationDependency(recoveryRun, { kind: "operation", dependencyId: "generic:recovery-work" });
    runtime.confirmAgentRunExit(recoveryRun, { error: "first failure" });
    const claim = runtime.controlPlane.claimRecoveryRun(recoveryRun.ownership.runId, "replacement-run")!;
    runtime.controlPlane.startRecoveryActivation(claim.ownership);
    runtime.controlPlane.failAgentRun(claim.ownership, { error: "replacement failure" });

    const incidents = runtime.reconcileOperationalIncidents();
    assert.deepEqual(incidents.map((incident) => incident.trigger.kind).sort(), [
      "automatic-recovery-exhausted",
      "persistent-operation-uncertainty",
      "repeated-undeclared-settlement",
    ]);
    assert.equal(runtime.reconcileOperationalIncidents().length, 3);
    const undeclaredIncident = incidents.find((incident) => incident.trigger.kind === "repeated-undeclared-settlement")!;
    assert.deepEqual(runtime.inspectIncidentBrief(undeclaredIncident.incidentId)!.triggerEvidence, {
      episodeId: episode.episodeId,
      correctionNoticeId: episode.noticeId,
      correctionNoticeAccepted: true,
      correctionNoticeDelivered: true,
      correctionAllowanceConsumed: true,
      declaredDependencyPointers: [],
    });
    const operationIncident = incidents.find((incident) => incident.trigger.kind === "persistent-operation-uncertainty")!;
    const operationBrief = runtime.inspectIncidentBrief(operationIncident.incidentId)!;
    assert.ok(operationBrief.persistedOperationPointers.some((pointer) => pointer.includes("operation-review:")));
    assert.ok(operationBrief.applicableReviewPolicy);
    const recoveryIncident = incidents.find((incident) => incident.trigger.kind === "automatic-recovery-exhausted")!;
    const recoveryBrief = runtime.inspectIncidentBrief(recoveryIncident.incidentId)!;
    assert.ok(recoveryBrief.persistedOperationPointers.some((pointer) => pointer.includes("operation-review:")));
    assert.ok(recoveryBrief.persistedOperationPointers.some((pointer) => pointer.includes("generic:recovery-work")));
    assert.ok(recoveryBrief.applicableReviewPolicy);
  });

  it("creates an expired-review incident only after deadline reconciliation remains unresolved", async () => {
    const workflow = await scenario();
    const { runtime } = workflow.createOwner();
    const workerSession = workflow.childSession(runtime, "deadline-worker");
    const worker = runtime.addAgent({ session: workerSession, spawner: runtime.owner(), name: "Deadline Worker" });
    const run = runtime.startAgentRun(runtime.agent(worker.agentId));
    runtime.addActivationDependency(run, { kind: "operation", dependencyId: "generic:deadline" });
    runtime.settleActivation(run);
    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved", eligibility: "eligible",
      evidence: { kind: "probe", detail: "retry remains permitted" },
    }));
    assert.deepEqual(runtime.reconcileOperationalIncidents(), []);

    workflow.clock.advance(1_000_000);
    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved", eligibility: "eligible",
      evidence: { kind: "probe", detail: "still unresolved at deadline" },
    }));
    const [incident] = runtime.reconcileOperationalIncidents();
    assert.equal(incident.trigger.kind, "operation-review-expired");
    assert.equal(runtime.reconcileOperationalIncidents().length, 1);
  });

  it("expands scope monotonically without merging overlapping incidents", async () => {
    const workflow = await scenario();
    const { runtime } = workflow.createOwner();
    const firstSession = workflow.childSession(runtime, "scope-first");
    const secondSession = workflow.childSession(runtime, "scope-second");
    const neighborSession = workflow.childSession(runtime, "scope-neighbor");
    const unrelatedSession = workflow.childSession(runtime, "scope-unrelated");
    const first = runtime.addAgent({ session: firstSession, spawner: runtime.owner(), name: "Scope First" });
    const second = runtime.addAgent({ session: secondSession, spawner: runtime.owner(), name: "Scope Second" });
    const neighbor = runtime.addAgent({ session: neighborSession, spawner: runtime.owner(), name: "Scope Neighbor" });
    const unrelated = runtime.addAgent({ session: unrelatedSession, spawner: runtime.owner(), name: "Scope Unrelated" });
    const firstRun = runtime.startAgentRun(runtime.agent(first.agentId));
    const secondRun = runtime.startAgentRun(runtime.agent(second.agentId));
    const neighborRun = runtime.startAgentRun(runtime.agent(neighbor.agentId));
    const unrelatedRun = runtime.startAgentRun(runtime.agent(unrelated.agentId));
    const neighborRuntime = workflow.startAgent(runtime.workflow, neighborSession);
    const neighborChildSession = workflow.childSession(neighborRuntime, "neighbor-child");
    const neighborChild = neighborRuntime.addAgent({
      session: neighborChildSession,
      spawner: neighborRuntime.agent(neighbor.agentId),
      name: "Neighbor Child",
    });
    neighborRuntime.close();
    acceptDeliveredRequest({ runtime, senderRun: firstRun, recipientRun: secondRun, messageId: "scope-a" });
    acceptDeliveredRequest({ runtime, senderRun: secondRun, recipientRun: firstRun, messageId: "scope-b" });
    runtime.settleActivation(firstRun);
    runtime.settleActivation(secondRun);
    const deadlock = runtime.reconcileOperationalIncidents()[0];

    const firstRuntime = workflow.startAgent(runtime.workflow, firstSession);
    const descendantSession = workflow.childSession(firstRuntime, "new-descendant");
    const descendant = firstRuntime.addAgent({
      session: descendantSession, spawner: firstRuntime.agent(first.agentId), name: "New Descendant",
    });
    firstRuntime.close();
    acceptDeliveredRequest({
      runtime, senderRun: neighborRun, recipientRun: firstRun, messageId: "neighbor-to-scope",
    });
    acceptDeliveredRequest({
      runtime, senderRun: unrelatedRun, recipientRun: firstRun, messageId: "unrelated-signal", responseRequired: false,
    });
    runtime.reconcileOperationalIncidents();
    const expandedScope = runtime.inspectOperationalIncident(deadlock.incidentId)!.scopeAgentIds;
    assert.ok(expandedScope.includes(descendant.agentId));
    assert.ok(expandedScope.includes(neighbor.agentId), "unresolved Request neighbors expand scope in either direction");
    assert.ok(!expandedScope.includes(neighborChild.agentId), "only seeded descendant subtrees expand automatically");
    assert.ok(!expandedScope.includes(unrelated.agentId), "Signals do not expand Incident Scope");
    assert.deepEqual(runtime.inspectIncidentBrief(deadlock.incidentId)!.scope.roster.map((agent) => agent.agentId).sort(), [
      first.agentId, second.agentId,
    ].sort());

    runtime.activateTurn(firstRun);
    runtime.addActivationDependency(firstRun, { kind: "operation", dependencyId: "ownership:overlap" });
    runtime.settleActivation(firstRun);
    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved", eligibility: "exhausted",
      evidence: { kind: "probe", detail: "still uncertain" },
    }));
    const incidents = runtime.reconcileOperationalIncidents();
    assert.equal(incidents.length, 2);
    assert.notEqual(incidents[0].incidentId, incidents[1].incidentId);
    assert.ok(incidents.every((incident) => incident.scopeAgentIds.includes(first.agentId)));
  });
});
