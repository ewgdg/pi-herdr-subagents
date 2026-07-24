import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, it } from "node:test";
import {
  ActivationCancellationService,
  ActivationCancellationStore,
  CancellationInDoubtError,
  type AgentRunTerminator,
} from "../../pi-extension/subagents/protocol/activation-cancellation.ts";
import { CompletionGateStore, CompletionRejectedError } from "../../pi-extension/subagents/protocol/completion-gate.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { RecipientInboxRouter } from "../../pi-extension/subagents/protocol/recipient-inbox-router.ts";
import type { InboxBatch } from "../../pi-extension/subagents/protocol/direct-signal-types.ts";
import { WorkflowProtocolError, type AgentReference, type AgentRunOwnership } from "../../pi-extension/subagents/protocol/workflow-types.ts";
import { WorkflowScenario, type ControllableRuntimeAdapter } from "./scenario-harness.ts";

async function setupCancellation(test: { after(fn: () => void): void }) {
  const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "activation-cancellation-")) });
  const { runtime } = scenario.createOwner();
  const childSession = scenario.childSession(runtime, "child");
  const child = runtime.addAgent({ session: childSession, spawner: runtime.owner(), name: "Child" });
  const run = runtime.startAgentRun(runtime.agent(child.agentId));
  runtime.checkpoint(run, JSON.stringify({ surface: `pane:${child.agentId}` }));
  const routerStore = new DirectSignalStore(runtime.workflow.databasePath);
  routerStore.registerRouter({
    recipient: runtime.agent(child.agentId),
    ownership: run.ownership,
    endpoint: `router:${child.agentId}`,
    registeredAtMs: scenario.clock.now(),
  });
  routerStore.close();
  const observations: Array<"present" | "missing" | "unavailable"> = ["present", "missing"];
  const closed: string[] = [];
  const terminator: AgentRunTerminator = {
    async inspect(_locator) { return { kind: observations.shift() ?? "missing" }; },
    async close(locator) { closed.push(locator.surface); },
  };
  let operationSequence = 0;
  const service = new ActivationCancellationService({
    databasePath: runtime.workflow.databasePath,
    actor: runtime.owner(),
    terminator,
    now: scenario.clock.now,
    allocateOperationId: () => `cancellation-operation-${++operationSequence}`,
  });
  test.after(() => { service.close(); runtime.close(); });
  return { scenario, runtime, child, run, service, observations, closed };
}

function acceptRequest(input: {
  store: DirectSignalStore;
  runtime: ControllableRuntimeAdapter;
  sender: AgentReference;
  recipient: AgentReference;
  recipientOwnership?: AgentRunOwnership;
  endpoint: string;
  id: string;
  acceptedAtMs?: number;
}) {
  const message = `request:${input.id}`;
  input.store.bindMessage({
    messageId: input.id,
    sender: input.sender,
    recipient: input.recipient,
    sourceEntryId: `source:${input.id}`,
    payloadDigest: `digest:${input.id}`,
    deliveryTiming: "steer",
    responseRequired: true,
    createdAtMs: input.acceptedAtMs ?? 1,
  });
  input.store.acceptSignal({
    request: {
      workflowOwnerId: input.runtime.workflow.ownerAgentId,
      messageId: input.id,
      senderAgentId: input.sender.agentId,
      recipientAgentId: input.recipient.agentId,
      sourceEntryId: `source:${input.id}`,
      payloadDigest: `digest:${input.id}`,
      deliveryTiming: "steer",
      responseRequired: true,
      onAccepted: "continue",
      message,
    },
    recipient: input.recipient,
    ownership: input.recipientOwnership,
    endpoint: input.endpoint,
    acceptedAtMs: input.acceptedAtMs ?? 2,
  });
}

describe("Activation Cancellation", () => {
  it("lets the Workflow Owner cancel exactly one open activation after inspect-close-inspect confirmation", async (test) => {
    const setup = await setupCancellation(test);
    const receipt = await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "tool-call-1" });

    assert.equal(receipt.state, "committed");
    assert.equal(receipt.activationId, setup.run.ownership.runId);
    assert.deepEqual(setup.closed, [`pane:${setup.child.agentId}`]);
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
    assert.equal(setup.runtime.currentAgentRun(setup.runtime.agent(setup.child.agentId)), undefined);
    assert.equal(setup.scenario.processes.isActive(setup.run.processId), true, "only the injected terminator owns process evidence");

    const stored = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    try {
      assert.equal(stored.inspectOperation(receipt.operationId)?.state, "committed");
    } finally { stored.close(); }
    assert.deepEqual(
      await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "tool-call-1" }),
      receipt,
      "the exact tool source remains bound after the activation ends",
    );
  });

  it("resumes a pending descendant cascade through a later cancellation of its ended root", async (test) => {
    const setup = await setupCancellation(test);
    const rootSession = setup.scenario.transcripts.resume(
      setup.runtime.inspect(setup.runtime.agent(setup.child.agentId)).sessionPath,
    );
    const rootRuntime = setup.scenario.startAgent(setup.runtime.workflow, rootSession);
    test.after(() => rootRuntime.close());
    const descendantSession = setup.scenario.childSession(rootRuntime, "pending-cascade-descendant");
    const descendant = rootRuntime.addAgent({
      session: descendantSession,
      spawner: rootRuntime.agent(setup.child.agentId),
      name: "Pending Cascade Descendant",
    });
    const descendantRun = rootRuntime.startAgentRun(rootRuntime.agent(descendant.agentId));
    rootRuntime.checkpoint(descendantRun, JSON.stringify({ surface: "pending-cascade-descendant" }));

    // The root is already durable when this descendant's termination becomes uncertain.
    setup.observations.splice(0, setup.observations.length, "present", "missing", "present", "unavailable");
    await assert.rejects(
      setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "root-cascade-first-call" }),
      (error: unknown) => error instanceof CancellationInDoubtError,
    );
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
    assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(descendant.agentId))?.state.kind, "active");

    setup.observations.push("missing");
    const resumed = await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "root-cascade-later-call",
    });
    assert.equal(resumed.operationId, "cancellation-operation-1");
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(descendant.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
  });

  it("lets the Workflow Owner resume a direct Spawner's pending cascade but rejects unrelated peers", async (test) => {
    const setup = await setupCancellation(test);
    const spawnerSession = setup.scenario.transcripts.resume(
      setup.runtime.inspect(setup.runtime.agent(setup.child.agentId)).sessionPath,
    );
    const spawnerRuntime = setup.scenario.startAgent(setup.runtime.workflow, spawnerSession);
    test.after(() => spawnerRuntime.close());
    const rootSession = setup.scenario.childSession(spawnerRuntime, "spawner-cancelled-root");
    const root = spawnerRuntime.addAgent({
      session: rootSession,
      spawner: spawnerRuntime.agent(setup.child.agentId),
      name: "Spawner-cancelled Root",
    });
    const rootRun = spawnerRuntime.startAgentRun(spawnerRuntime.agent(root.agentId));
    spawnerRuntime.checkpoint(rootRun, JSON.stringify({ surface: "spawner-cancelled-root" }));
    const rootRuntime = setup.scenario.startAgent(setup.runtime.workflow, rootSession);
    test.after(() => rootRuntime.close());
    const descendantSession = setup.scenario.childSession(rootRuntime, "spawner-cascade-descendant");
    const descendant = rootRuntime.addAgent({
      session: descendantSession,
      spawner: rootRuntime.agent(root.agentId),
      name: "Spawner Cascade Descendant",
    });
    const descendantRun = rootRuntime.startAgentRun(rootRuntime.agent(descendant.agentId));
    rootRuntime.checkpoint(descendantRun, JSON.stringify({ surface: "spawner-cascade-descendant" }));

    const observations: Array<"present" | "missing" | "unavailable"> = ["present", "missing", "present", "unavailable"];
    const spawnerService = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: spawnerRuntime.agent(setup.child.agentId),
      terminator: {
        async inspect() { return { kind: observations.shift() ?? "missing" }; },
        async close() {},
      },
      allocateOperationId: (() => {
        let sequence = 0;
        return () => `spawner-cascade-operation-${++sequence}`;
      })(),
    });
    test.after(() => spawnerService.close());
    await assert.rejects(
      spawnerService.cancel({ target: spawnerRuntime.agent(root.agentId), sourceId: "spawner-first-call" }),
      (error: unknown) => error instanceof CancellationInDoubtError,
    );

    const peerSession = setup.scenario.childSession(setup.runtime, "unrelated-cascade-peer");
    const peer = setup.runtime.addAgent({ session: peerSession, spawner: setup.runtime.owner(), name: "Unrelated Peer" });
    const peerService = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: setup.runtime.agent(peer.agentId),
      terminator: { async inspect() { return { kind: "missing" }; }, async close() {} },
      allocateOperationId: () => "unrelated-peer-operation",
    });
    test.after(() => peerService.close());
    await assert.rejects(
      peerService.cancel({ target: setup.runtime.agent(root.agentId), sourceId: "unrelated-peer-retry" }),
      (error: unknown) => error instanceof WorkflowProtocolError && error.code === "ActivationCancellationUnauthorized",
    );

    const ownerService = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: setup.runtime.owner(),
      terminator: { async inspect() { return { kind: "missing" }; }, async close() {} },
      allocateOperationId: (() => {
        let sequence = 0;
        return () => `owner-resume-operation-${++sequence}`;
      })(),
    });
    test.after(() => ownerService.close());
    const resumed = await ownerService.cancel({
      target: setup.runtime.agent(root.agentId),
      sourceId: "owner-resumes-spawner-cascade",
    });
    assert.equal(resumed.operationId, "spawner-cascade-operation-1");
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(descendant.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
  });

  it("skips stale planned descendants and continues cancelling later siblings", async (test) => {
    const setup = await setupCancellation(test);
    const rootSession = setup.scenario.transcripts.resume(
      setup.runtime.inspect(setup.runtime.agent(setup.child.agentId)).sessionPath,
    );
    const rootRuntime = setup.scenario.startAgent(setup.runtime.workflow, rootSession);
    test.after(() => rootRuntime.close());
    const firstSession = setup.scenario.childSession(rootRuntime, "stale-first-descendant");
    const first = rootRuntime.addAgent({
      session: firstSession,
      spawner: rootRuntime.agent(setup.child.agentId),
      name: "Stale First Descendant",
    });
    const firstRun = rootRuntime.startAgentRun(rootRuntime.agent(first.agentId));
    rootRuntime.checkpoint(firstRun, JSON.stringify({ surface: "stale-first-descendant" }));
    const secondSession = setup.scenario.childSession(rootRuntime, "later-sibling");
    const second = rootRuntime.addAgent({
      session: secondSession,
      spawner: rootRuntime.agent(setup.child.agentId),
      name: "Later Sibling",
    });
    const secondRun = rootRuntime.startAgentRun(rootRuntime.agent(second.agentId));
    rootRuntime.checkpoint(secondRun, JSON.stringify({ surface: "later-sibling" }));

    let rootClosed = false;
    const service = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: setup.runtime.owner(),
      allocateOperationId: (() => {
        let sequence = 0;
        return () => `stale-sibling-operation-${++sequence}`;
      })(),
      terminator: {
        async inspect(locator) {
          return locator.surface === `pane:${setup.child.agentId}` && !rootClosed
            ? { kind: "present" as const }
            : { kind: "missing" as const };
        },
        async close(locator) {
          if (locator.surface !== `pane:${setup.child.agentId}`) return;
          rootClosed = true;
          setup.runtime.cancelActivation(firstRun);
        },
      },
    });
    test.after(() => service.close());

    await service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "stale-sibling-root" });
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(first.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(second.agentId))?.state, {
      kind: "ended", outcome: "cancelled",
    });
    const database = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    const planState = database.prepare(`SELECT cascade_state FROM activation_cancellation_descendants
      WHERE root_operation_id = 'stale-sibling-operation-1' AND agent_id = ?`
    ).get(first.agentId) as { cascade_state: string };
    database.close();
    assert.equal(planState.cascade_state, "skipped");
  });

  it("migrates cancellation source fences under the migration write lock", async (test) => {
    const setup = await setupCancellation(test);
    const legacy = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    const claim = legacy.claim({
      actor: setup.runtime.owner(),
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "legacy-source-fence",
      operationId: "legacy-source-operation",
      now: setup.scenario.clock.now(),
    });
    legacy.close();
    const database = new DatabaseSync(setup.runtime.workflow.databasePath);
    database.exec(`
      PRAGMA foreign_keys = OFF;
      BEGIN IMMEDIATE;
      CREATE TABLE activation_cancellations_v1 AS
        SELECT operation_id, actor_agent_id, source_id, authority_kind, incident_id, rationale,
          target_agent_id, activation_id, run_id, fencing_epoch, activation_revision, run_locator,
          state, termination_attempts, last_error, created_at_ms, updated_at_ms, committed_at_ms
        FROM activation_cancellations;
      DROP TABLE activation_cancellation_sources;
      DROP TABLE activation_cancellation_cascades;
      DROP TABLE activation_cancellation_descendants;
      DROP TABLE activation_cancellations;
      ALTER TABLE activation_cancellations_v1 RENAME TO activation_cancellations;
      CREATE TABLE activation_cancellation_sources (
        actor_agent_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        bound_at_ms INTEGER NOT NULL,
        PRIMARY KEY (actor_agent_id, source_id)
      ) STRICT;
      INSERT INTO activation_cancellation_sources VALUES ('${setup.runtime.workflow.ownerAgentId}', 'legacy-source-fence', 'legacy-source-operation', 1);
      COMMIT;
      PRAGMA foreign_keys = ON;
    `);
    database.close();

    const migrated = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    test.after(() => migrated.close());
    const replay = migrated.claim({
      actor: setup.runtime.owner(),
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "legacy-source-fence",
      operationId: "different-operation",
      now: setup.scenario.clock.now(),
    });
    assert.equal(replay.operationId, claim.operationId);
  });

  it("retains the activation, Router, and ownership in doubt, then retries the same durable claim", async (test) => {
    const setup = await setupCancellation(test);
    setup.observations.splice(0, setup.observations.length, "present", "unavailable");

    let operationId = "";
    await assert.rejects(
      setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "tool-call-in-doubt" }),
      (error: unknown) => {
        assert.ok(error instanceof CancellationInDoubtError);
        operationId = error.operation.operationId;
        return true;
      },
    );
    assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state.kind, "active");
    assert.deepEqual(setup.closed, [`pane:${setup.child.agentId}`]);
    assert.equal(setup.runtime.currentAgentRun(setup.runtime.agent(setup.child.agentId))?.runId, setup.run.ownership.runId);
    const projected = setup.runtime.inspectTarget({ agent: setup.child.agentId }) as any;
    assert.equal(projected.callerAuthority.cancelActivation, true);
    assert.equal(projected.cancellation.operationId, operationId);
    assert.equal(projected.cancellation.state, "in-doubt");
    const operationReviewId = projected.operationReviews.find(
      (review: { dependencyId: string }) => review.dependencyId === `cancellation:${operationId}`,
    ).operationReviewId as number;
    const review = setup.runtime.inspectOperationReview(operationReviewId);
    assert.equal(review?.status, "reconciling");
    assert.equal(review?.evidenceCount, 1);
    assert.deepEqual(review?.latestEvidence, {
      kind: "cancellation-uncertainty",
      detail: "Post-close Agent Run inspection is unavailable",
      observedAtMs: setup.scenario.clock.now(),
    });
    assert.equal(
      review?.reviewDeadlineAtMs,
      (review?.reviewStartedAtMs ?? 0) + 5 * 60_000,
      "process activity and repeated termination attempts do not renew review policy",
    );
    const database = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    assert.ok(database.prepare("SELECT 1 FROM recipient_inbox_routers WHERE agent_id = ?").get(setup.child.agentId));
    database.close();

    setup.observations.push("missing");
    const retried = await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "tool-call-retry-with-new-id",
    });
    assert.equal(retried.state, "committed");
    assert.equal(retried.operationId, operationId);
    assert.equal(retried.sourceId, "tool-call-in-doubt", "public retry must not rebind the durable operation identity");
    assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state, { kind: "ended", outcome: "cancelled" });

    const replacement = setup.runtime.startAgentRun(setup.runtime.agent(setup.child.agentId));
    setup.runtime.checkpoint(replacement, JSON.stringify({ surface: "replacement-pane" }));
    assert.equal((await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "tool-call-retry-with-new-id",
    })).operationId, operationId, "the retry source remains fenced to the original activation");
    assert.equal((await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "tool-call-in-doubt",
    })).operationId, operationId, "the original source remains fenced to the original activation");
    assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.activationId, replacement.ownership.runId);

    setup.observations.push("missing");
    const replacementCancellation = await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "tool-call-for-replacement",
    });
    assert.equal(replacementCancellation.activationId, replacement.ownership.runId);
    assert.notEqual(replacementCancellation.operationId, operationId);
  });

  it("authorizes only the Workflow Owner or direct Spawner and carries a dormant Incident Control seam", async (test) => {
    const setup = await setupCancellation(test);
    const peerSession = setup.scenario.childSession(setup.runtime, "peer");
    const peer = setup.runtime.addAgent({ session: peerSession, spawner: setup.runtime.owner(), name: "Peer" });
    const peerRuntime = setup.scenario.startAgent(setup.runtime.workflow, peerSession);
    test.after(() => peerRuntime.close());
    const unauthorized = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: peerRuntime.agent(peer.agentId),
      terminator: { async inspect() { return { kind: "missing" }; }, async close() {} },
      allocateOperationId: () => "unauthorized-operation",
    });
    test.after(() => unauthorized.close());
    await assert.rejects(
      unauthorized.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "unauthorized-source" }),
      (error: unknown) => error instanceof WorkflowProtocolError && error.code === "ActivationCancellationUnauthorized",
    );

    const parentSession = setup.scenario.childSession(setup.runtime, "parent");
    const parent = setup.runtime.addAgent({ session: parentSession, spawner: setup.runtime.owner(), name: "Parent" });
    const parentRun = setup.runtime.startAgentRun(setup.runtime.agent(parent.agentId));
    const parentRuntime = setup.scenario.startAgent(setup.runtime.workflow, parentSession);
    const grandchildSession = setup.scenario.childSession(parentRuntime, "grandchild");
    const grandchild = parentRuntime.addAgent({ session: grandchildSession, spawner: parentRuntime.agent(parent.agentId), name: "Grandchild" });
    const grandchildRun = parentRuntime.startAgentRun(parentRuntime.agent(grandchild.agentId));
    parentRuntime.checkpoint(grandchildRun, JSON.stringify({ surface: "grandchild-pane" }));
    const directSpawner = new ActivationCancellationService({
      databasePath: setup.runtime.workflow.databasePath,
      actor: parentRuntime.agent(parent.agentId),
      terminator: { async inspect() { return { kind: "missing" }; }, async close() {} },
      allocateOperationId: () => "direct-spawner-operation",
    });
    test.after(() => { directSpawner.close(); parentRuntime.close(); });
    const ownerClaimStore = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    const ownerClaim = ownerClaimStore.claim({
      actor: setup.runtime.owner(),
      target: parentRuntime.agent(grandchild.agentId),
      sourceId: "owner-controlled-source",
      operationId: "owner-controlled-operation",
      now: 1,
    });
    await assert.rejects(
      directSpawner.cancel({ target: parentRuntime.agent(grandchild.agentId), sourceId: "different-authorized-actor" }),
      (error: unknown) => error instanceof WorkflowProtocolError && error.code === "ActivationCancellationUnauthorized",
    );
    ownerClaimStore.markReady(ownerClaim.operationId, 2);
    assert.equal(ownerClaimStore.finalize(ownerClaim.operationId, 3).state, "committed");
    ownerClaimStore.close();

    const secondGrandchildSession = setup.scenario.childSession(parentRuntime, "second-grandchild");
    const secondGrandchild = parentRuntime.addAgent({
      session: secondGrandchildSession,
      spawner: parentRuntime.agent(parent.agentId),
      name: "Second Grandchild",
    });
    const secondGrandchildRun = parentRuntime.startAgentRun(parentRuntime.agent(secondGrandchild.agentId));
    parentRuntime.checkpoint(secondGrandchildRun, JSON.stringify({ surface: "second-grandchild-pane" }));
    assert.equal((await directSpawner.cancel({
      target: parentRuntime.agent(secondGrandchild.agentId),
      sourceId: "direct-spawner-source",
    })).state, "committed");
    assert.equal(parentRun.ownership.agentId, parent.agentId);

    const incidentStore = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    try {
      assert.throws(() => incidentStore.claim({
        actor: peerRuntime.agent(peer.agentId),
        target: setup.runtime.agent(setup.child.agentId),
        sourceId: "incident-source",
        operationId: "incident-operation",
        authority: { kind: "incident-control", incidentId: "incident-31", rationale: "dormant seam" },
        incidentControlAuthorized: false,
        now: 10,
      }), (error: unknown) => error instanceof WorkflowProtocolError && error.code === "ActivationCancellationUnauthorized");
    } finally { incidentStore.close(); }
  });

  it("cancels descendants while retaining the external Request-dependency closure", async (test) => {
    const setup = await setupCancellation(test);
    const rootSession = setup.scenario.transcripts.resume(
      setup.runtime.inspect(setup.runtime.agent(setup.child.agentId)).sessionPath,
    );
    const rootRuntime = setup.scenario.startAgent(setup.runtime.workflow, rootSession);
    test.after(() => rootRuntime.close());

    const peerSession = setup.scenario.childSession(setup.runtime, "external-peer");
    const peer = setup.runtime.addAgent({ session: peerSession, spawner: setup.runtime.owner(), name: "External Peer" });
    const peerRun = setup.runtime.startAgentRun(setup.runtime.agent(peer.agentId));

    const seedSession = setup.scenario.childSession(rootRuntime, "survivor-seed");
    const seed = rootRuntime.addAgent({ session: seedSession, spawner: rootRuntime.agent(setup.child.agentId), name: "Survivor Seed" });
    const seedRun = rootRuntime.startAgentRun(rootRuntime.agent(seed.agentId));
    rootRuntime.checkpoint(seedRun, JSON.stringify({ surface: "survivor-seed" }));

    const retainedSession = setup.scenario.childSession(rootRuntime, "retained-dependency");
    const retained = rootRuntime.addAgent({ session: retainedSession, spawner: rootRuntime.agent(setup.child.agentId), name: "Retained Dependency" });
    const retainedRun = rootRuntime.startAgentRun(rootRuntime.agent(retained.agentId));
    rootRuntime.checkpoint(retainedRun, JSON.stringify({ surface: "retained-dependency" }));
    const retainedRuntime = setup.scenario.startAgent(setup.runtime.workflow, retainedSession);
    test.after(() => retainedRuntime.close());

    const transitiveSession = setup.scenario.childSession(retainedRuntime, "transitive-dependency");
    const transitive = retainedRuntime.addAgent({
      session: transitiveSession,
      spawner: retainedRuntime.agent(retained.agentId),
      name: "Transitive Dependency",
    });
    const transitiveRun = retainedRuntime.startAgentRun(retainedRuntime.agent(transitive.agentId));
    retainedRuntime.checkpoint(transitiveRun, JSON.stringify({ surface: "transitive-dependency" }));

    const signalOnlySession = setup.scenario.childSession(rootRuntime, "signal-only-descendant");
    const signalOnly = rootRuntime.addAgent({
      session: signalOnlySession,
      spawner: rootRuntime.agent(setup.child.agentId),
      name: "Signal-only Descendant",
    });
    const signalOnlyRun = rootRuntime.startAgentRun(rootRuntime.agent(signalOnly.agentId));
    rootRuntime.checkpoint(signalOnlyRun, JSON.stringify({ surface: "signal-only-descendant" }));

    const prunableSession = setup.scenario.childSession(rootRuntime, "prunable-descendant");
    const prunable = rootRuntime.addAgent({
      session: prunableSession,
      spawner: rootRuntime.agent(setup.child.agentId),
      name: "Prunable Descendant",
    });
    const prunableRun = rootRuntime.startAgentRun(rootRuntime.agent(prunable.agentId));
    rootRuntime.checkpoint(prunableRun, JSON.stringify({ surface: "prunable-descendant" }));
    const prunableRuntime = setup.scenario.startAgent(setup.runtime.workflow, prunableSession);
    test.after(() => prunableRuntime.close());

    const requesterSession = setup.scenario.childSession(prunableRuntime, "internal-requester");
    const requester = prunableRuntime.addAgent({
      session: requesterSession,
      spawner: prunableRuntime.agent(prunable.agentId),
      name: "Internal Requester",
    });
    const requesterRun = prunableRuntime.startAgentRun(prunableRuntime.agent(requester.agentId));
    prunableRuntime.checkpoint(requesterRun, JSON.stringify({ surface: "internal-requester" }));

    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    const routers = [
      [setup.runtime.agent(peer.agentId), peerRun.ownership, "external-peer-router"],
      [rootRuntime.agent(seed.agentId), seedRun.ownership, "survivor-seed-router"],
      [rootRuntime.agent(retained.agentId), retainedRun.ownership, "retained-dependency-router"],
      [retainedRuntime.agent(transitive.agentId), transitiveRun.ownership, "transitive-dependency-router"],
      [rootRuntime.agent(signalOnly.agentId), signalOnlyRun.ownership, "signal-only-descendant-router"],
      [rootRuntime.agent(prunable.agentId), prunableRun.ownership, "prunable-descendant-router"],
      [prunableRuntime.agent(requester.agentId), requesterRun.ownership, "internal-requester-router"],
    ] as const;
    for (const [recipient, ownership, endpoint] of routers) {
      messages.registerRouter({ recipient, ownership, endpoint, registeredAtMs: setup.scenario.clock.now() });
    }

    acceptRequest({
      store: messages, runtime: setup.runtime, sender: setup.runtime.agent(peer.agentId),
      recipient: rootRuntime.agent(seed.agentId), recipientOwnership: seedRun.ownership,
      endpoint: "survivor-seed-router", id: "external-request",
    });
    acceptRequest({
      store: messages, runtime: setup.runtime, sender: rootRuntime.agent(seed.agentId),
      recipient: rootRuntime.agent(retained.agentId), recipientOwnership: retainedRun.ownership,
      endpoint: "retained-dependency-router", id: "seed-dependency-request",
    });
    acceptRequest({
      store: messages, runtime: setup.runtime, sender: rootRuntime.agent(retained.agentId),
      recipient: retainedRuntime.agent(transitive.agentId), recipientOwnership: transitiveRun.ownership,
      endpoint: "transitive-dependency-router", id: "retained-dependency-request",
    });
    acceptRequest({
      store: messages, runtime: setup.runtime, sender: rootRuntime.agent(prunable.agentId),
      recipient: setup.runtime.agent(peer.agentId), recipientOwnership: peerRun.ownership,
      endpoint: "external-peer-router", id: "prunable-outgoing-request",
    });
    acceptRequest({
      store: messages, runtime: setup.runtime, sender: prunableRuntime.agent(requester.agentId),
      recipient: rootRuntime.agent(prunable.agentId), recipientOwnership: prunableRun.ownership,
      endpoint: "prunable-descendant-router", id: "prunable-incoming-request",
    });
    acceptRequest({
      store: messages, runtime: setup.runtime, sender: prunableRuntime.agent(requester.agentId),
      recipient: setup.runtime.agent(peer.agentId), recipientOwnership: peerRun.ownership,
      endpoint: "external-peer-router", id: "requester-outgoing-request",
    });
    messages.bindMessage({
      messageId: "signal-only-edge", sender: rootRuntime.agent(seed.agentId), recipient: rootRuntime.agent(signalOnly.agentId),
      sourceEntryId: "signal-only-source", payloadDigest: "signal-only-digest", deliveryTiming: "steer",
      responseRequired: false, createdAtMs: setup.scenario.clock.now(),
    });
    messages.acceptSignal({
      request: {
        workflowOwnerId: setup.runtime.workflow.ownerAgentId, messageId: "signal-only-edge",
        senderAgentId: seed.agentId, recipientAgentId: signalOnly.agentId,
        sourceEntryId: "signal-only-source", payloadDigest: "signal-only-digest", deliveryTiming: "steer",
        responseRequired: false, onAccepted: "continue", message: "A Signal must not retain this Agent.",
      },
      recipient: rootRuntime.agent(signalOnly.agentId), ownership: signalOnlyRun.ownership,
      endpoint: "signal-only-descendant-router", acceptedAtMs: setup.scenario.clock.now(),
    });

    const database = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    const turnSequence = (agentId: string) => Number((database.prepare(`SELECT turn_sequence FROM agent_activations
      WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1`).get(agentId) as { turn_sequence: number }).turn_sequence);
    const survivorTurns = new Map([
      [seed.agentId, turnSequence(seed.agentId)],
      [retained.agentId, turnSequence(retained.agentId)],
      [transitive.agentId, turnSequence(transitive.agentId)],
    ]);
    database.close();

    await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "cancel-root-with-closure" });

    for (const agent of [setup.child, prunable, requester, signalOnly]) {
      assert.deepEqual(setup.runtime.inspectActivation(setup.runtime.agent(agent.agentId))?.state, {
        kind: "ended", outcome: "cancelled",
      });
    }
    for (const agent of [seed, retained, transitive]) {
      assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(agent.agentId))?.state.kind, "active");
      assert.equal(existsSync(setup.runtime.inspect(setup.runtime.agent(agent.agentId)).sessionPath), true);
      assert.equal(setup.runtime.hasHumanAttention(setup.runtime.agent(agent.agentId)), false);
    }
    assert.equal(setup.runtime.inspect(setup.runtime.agent(seed.agentId)).spawnerAgentId, setup.child.agentId);
    assert.equal(setup.runtime.inspect(setup.runtime.agent(retained.agentId)).spawnerAgentId, setup.child.agentId);
    assert.equal(setup.runtime.inspect(setup.runtime.agent(transitive.agentId)).spawnerAgentId, retained.agentId);

    const after = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    for (const [agentId, before] of survivorTurns) {
      const current = Number((after.prepare(`SELECT turn_sequence FROM agent_activations
        WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1`).get(agentId) as { turn_sequence: number }).turn_sequence);
      assert.equal(current, before, "fallback supervision must not create a model turn");
    }
    after.close();

    for (const requestId of ["external-request", "seed-dependency-request", "retained-dependency-request"]) {
      assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, requestId)?.status, "open");
    }
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "prunable-outgoing-request")?.status, "cancelled");
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "requester-outgoing-request")?.status, "cancelled");
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "prunable-incoming-request")?.status, "orphaned");
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "signal-only-edge")?.deliveryStatus, "queued");

    const cancellation = messages.cancelRequest({
      requester: rootRuntime.agent(seed.agentId), requestId: "seed-dependency-request",
      noticeMessageId: "seed-cancels-retained-request", cancelledAtMs: setup.scenario.clock.now(),
    });
    assert.equal(cancellation.status, "cancelled");
  });

  it("makes completion and cancellation first-commit-wins through the cancellation operation dependency", async (test) => {
    const setup = await setupCancellation(test);
    const store = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    const gate = new CompletionGateStore(setup.runtime.workflow.databasePath);
    test.after(() => { store.close(); gate.close(); });
    const claim = store.claim({
      actor: setup.runtime.owner(), target: setup.runtime.agent(setup.child.agentId), sourceId: "race-source",
      operationId: "race-operation", now: 3,
    });
    assert.equal(claim.state, "terminating");
    assert.throws(
      () => gate.complete(setup.run.ownership, { kind: "standalone", toolCallId: "complete-race" }, 4),
      (error: unknown) => error instanceof CompletionRejectedError
        && error.blockers.some((blocker) => blocker.kind === "cancellation-uncertainty"),
    );

    const second = await setupCancellation(test);
    const secondGate = new CompletionGateStore(second.runtime.workflow.databasePath);
    const secondStore = new ActivationCancellationStore(second.runtime.workflow.databasePath);
    test.after(() => { secondGate.close(); secondStore.close(); });
    secondGate.complete(second.run.ownership, { kind: "standalone", toolCallId: "completion-first" }, 5);
    assert.throws(() => secondStore.claim({
      actor: second.runtime.owner(), target: second.runtime.agent(second.child.agentId), sourceId: "late-cancel",
      operationId: "late-operation", now: 6,
    }), (error: unknown) => error instanceof WorkflowProtocolError && error.code === "InvalidLifecycleTransition");
  });

  it("fails finalization closed when the exact checkpointed lifecycle revision changes", async (test) => {
    const setup = await setupCancellation(test);
    const store = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    test.after(() => store.close());
    const claim = store.claim({
      actor: setup.runtime.owner(),
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "stale-finalizer-source",
      operationId: "stale-finalizer-operation",
      now: 10,
    });
    setup.runtime.addActivationDependency(setup.run, { kind: "operation", dependencyId: "side-effect:concurrent" });
    const cancellationReviewId = (
      setup.runtime.inspectTarget({ agent: setup.child.agentId }) as any
    ).operationReviews.find(
      (review: { dependencyId: string }) => review.dependencyId === `cancellation:${claim.operationId}`,
    ).operationReviewId as number;
    store.markReady(claim.operationId, 11);
    const finalization = store.finalize(claim.operationId, 12);
    assert.equal(finalization.state, "in-doubt");
    assert.match(finalization.lastError ?? "", /revalidation failed/);
    assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state.kind, "active");
    assert.equal(setup.runtime.currentAgentRun(setup.runtime.agent(setup.child.agentId))?.runId, setup.run.ownership.runId);
    const review = setup.runtime.inspectOperationReview(cancellationReviewId);
    assert.equal(review?.evidenceCount, 1);
    assert.deepEqual(review?.latestEvidence, {
      kind: "cancellation-uncertainty",
      detail: "Exact activation/run/epoch/revision/checkpoint revalidation failed",
      observedAtMs: 12,
    });
  });

  it("atomically orphans incoming Requests, cancels open outgoing Requests, and preserves answered undelivered Answers", async (test) => {
    const setup = await setupCancellation(test);
    const peerSession = setup.scenario.childSession(setup.runtime, "obligation-peer");
    const peer = setup.runtime.addAgent({ session: peerSession, spawner: setup.runtime.owner(), name: "Peer" });
    const peerRun = setup.runtime.startAgentRun(setup.runtime.agent(peer.agentId));
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    messages.registerRouter({ recipient: setup.runtime.owner(), endpoint: "owner-router", registeredAtMs: 1 });
    messages.registerRouter({ recipient: setup.runtime.agent(setup.child.agentId), ownership: setup.run.ownership, endpoint: "child-router", registeredAtMs: 1 });
    messages.registerRouter({ recipient: setup.runtime.agent(peer.agentId), ownership: peerRun.ownership, endpoint: "peer-router", registeredAtMs: 1 });

    acceptRequest({ store: messages, runtime: setup.runtime, sender: setup.runtime.owner(), recipient: setup.runtime.agent(setup.child.agentId), recipientOwnership: setup.run.ownership, endpoint: "child-router", id: "incoming-open" });
    acceptRequest({ store: messages, runtime: setup.runtime, sender: setup.runtime.agent(setup.child.agentId), recipient: setup.runtime.agent(peer.agentId), recipientOwnership: peerRun.ownership, endpoint: "peer-router", id: "outgoing-open" });
    acceptRequest({ store: messages, runtime: setup.runtime, sender: setup.runtime.agent(setup.child.agentId), recipient: setup.runtime.owner(), endpoint: "owner-router", id: "outgoing-answered" });
    messages.commitDelivery({ recipient: setup.runtime.owner(), endpoint: "owner-router", messageId: "outgoing-answered", deliveredAtMs: 3 });
    messages.bindMessage({
      messageId: "answer-undelivered", sender: setup.runtime.owner(), recipient: setup.runtime.agent(setup.child.agentId),
      sourceEntryId: "answer-source", payloadDigest: "answer-digest", deliveryTiming: "steer", responseRequired: true,
      inReplyToRequestId: "outgoing-answered", createdAtMs: 4,
    });
    messages.acceptSignal({
      request: {
        workflowOwnerId: setup.runtime.workflow.ownerAgentId, messageId: "answer-undelivered",
        senderAgentId: setup.runtime.workflow.ownerAgentId, recipientAgentId: setup.child.agentId,
        sourceEntryId: "answer-source", payloadDigest: "answer-digest", deliveryTiming: "steer",
        responseRequired: true, inReplyToRequestId: "outgoing-answered", onAccepted: "continue", message: "answer and follow-up",
      },
      recipient: setup.runtime.agent(setup.child.agentId), ownership: setup.run.ownership,
      endpoint: "child-router", acceptedAtMs: 5,
    });

    const receipt = await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "obligation-cancel" });
    assert.equal(receipt.state, "committed");
    const incoming = messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "incoming-open")!;
    const outgoingOpen = messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "outgoing-open")!;
    const outgoingAnswered = messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "outgoing-answered")!;
    assert.equal(incoming.status, "orphaned");
    assert.equal(incoming.orphanNotice?.deliveryStatus, "queued");
    const orphanProjection = setup.runtime.inspectTarget({ request: "incoming-open" }) as any;
    assert.equal(orphanProjection.status, "orphaned");
    assert.equal(orphanProjection.requesterDependency, "unresolved");
    assert.equal(orphanProjection.orphaning.noticeMessageId, incoming.orphanNotice?.messageId);
    assert.equal(JSON.stringify(orphanProjection).includes("No Answer was fabricated"), false);
    assert.equal(outgoingOpen.status, "cancelled");
    assert.equal(outgoingAnswered.status, "answered");
    assert.equal(outgoingAnswered.answerMessageId, "answer-undelivered");
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "answer-undelivered")?.deliveryStatus, "queued");
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "answer-undelivered")?.status, "orphaned");

    const ownerNotices = messages.listPending(setup.runtime.owner()).filter((pointer) => pointer.protocolNoticeKind === "request-orphaned");
    assert.equal(ownerNotices.length, 2);
    const incomingNotice = ownerNotices.find((notice) => notice.canonicalRequestId === "incoming-open")!;
    messages.commitDelivery({ recipient: setup.runtime.owner(), endpoint: "owner-router", messageId: incomingNotice.messageId, deliveredAtMs: 8 });
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "incoming-open")?.orphanNotice?.deliveryStatus, "delivered");
    assert.equal((setup.runtime.inspectTarget({ request: "incoming-open" }) as any).requesterDependency, "satisfied");
    const laterActivation = setup.runtime.startAgentRun(setup.runtime.agent(setup.child.agentId));
    assert.throws(
      () => messages.requireAnswerTarget(setup.runtime.agent(setup.child.agentId), "incoming-open"),
      (error: unknown) => error instanceof WorkflowProtocolError && error.code === "AnswerAlreadyClosed",
    );
    assert.notEqual(laterActivation.ownership.runId, setup.run.ownership.runId);

    const cancellationStore = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    try {
      assert.deepEqual(cancellationStore.finalize(receipt.operationId, 9), receipt, "finalizer retry is idempotent and does not duplicate notices");
    } finally { cancellationStore.close(); }
    assert.equal(messages.listMessages(setup.runtime.workflow.ownerAgentId).filter((message) => message.protocolNoticeKind === "request-orphaned").length, 2);
  });

  it("discards a still-bound outbound acceptance and fences its source from replacement activation reuse", async (test) => {
    const setup = await setupCancellation(test);
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    messages.registerRouter({ recipient: setup.runtime.owner(), endpoint: "bound-cancellation-owner-router", registeredAtMs: 1 });
    messages.bindMessage({
      messageId: "bound-at-cancellation",
      sender: setup.runtime.agent(setup.child.agentId),
      recipient: setup.runtime.owner(),
      sourceEntryId: "bound-at-cancellation-source",
      payloadDigest: "bound-at-cancellation-digest",
      deliveryTiming: "steer",
      responseRequired: true,
      ownership: setup.run.ownership,
      createdAtMs: setup.scenario.clock.now(),
    });

    await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "cancel-bound-acceptance" });
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "bound-at-cancellation"), undefined);
    const database = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    assert.equal(Number((database.prepare(`SELECT COUNT(*) AS count FROM activation_dependencies
      WHERE dependency_kind = 'operation' AND dependency_id = 'acceptance:bound-at-cancellation'`
    ).get() as { count: number }).count), 0);
    database.close();
    assert.throws(() => messages.acceptSignal({
      request: {
        workflowOwnerId: setup.runtime.workflow.ownerAgentId,
        messageId: "bound-at-cancellation",
        senderAgentId: setup.child.agentId,
        recipientAgentId: setup.runtime.workflow.ownerAgentId,
        sourceEntryId: "bound-at-cancellation-source",
        payloadDigest: "bound-at-cancellation-digest",
        deliveryTiming: "steer",
        responseRequired: true,
        onAccepted: "continue",
        message: "late acceptance",
      },
      recipient: setup.runtime.owner(),
      endpoint: "bound-cancellation-owner-router",
      acceptedAtMs: setup.scenario.clock.now(),
    }), (error: unknown) => error instanceof WorkflowProtocolError && error.code === "InvalidMessageSource");

    setup.runtime.startAgentRun(setup.runtime.agent(setup.child.agentId));
    assert.deepEqual(messages.listBoundMessages(setup.runtime.agent(setup.child.agentId)), [],
      "replacement activation reconciliation cannot discover the discarded binding");
  });

  it("discards an outbound binding created after the cancellation claim but before finalization", async (test) => {
    const setup = await setupCancellation(test);
    const cancellation = new ActivationCancellationStore(setup.runtime.workflow.databasePath);
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => { cancellation.close(); messages.close(); });
    const claim = cancellation.claim({
      actor: setup.runtime.owner(),
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "claim-before-bind-source",
      operationId: "claim-before-bind-operation",
      now: 1,
    });
    messages.bindMessage({
      messageId: "bound-after-claim",
      sender: setup.runtime.agent(setup.child.agentId),
      recipient: setup.runtime.owner(),
      sourceEntryId: "bound-after-claim-source",
      payloadDigest: "bound-after-claim-digest",
      deliveryTiming: "steer",
      responseRequired: false,
      ownership: setup.run.ownership,
      createdAtMs: 2,
    });

    cancellation.markReady(claim.operationId, 3);
    assert.equal(cancellation.finalize(claim.operationId, 4).state, "committed");
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "bound-after-claim"), undefined);
  });

  it("cancels the resulting Request when outbound acceptance commits before activation cancellation", async (test) => {
    const setup = await setupCancellation(test);
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    messages.registerRouter({ recipient: setup.runtime.owner(), endpoint: "acceptance-wins-owner-router", registeredAtMs: 1 });
    acceptRequest({
      store: messages,
      runtime: setup.runtime,
      sender: setup.runtime.agent(setup.child.agentId),
      recipient: setup.runtime.owner(),
      endpoint: "acceptance-wins-owner-router",
      id: "acceptance-wins-request",
    });

    await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "cancel-after-acceptance" });
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "acceptance-wins-request")?.status, "cancelled");
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "acceptance-wins-request")?.deliveryStatus, "suppressed");
    const database = new DatabaseSync(setup.runtime.workflow.databasePath, { readOnly: true });
    assert.equal(Number((database.prepare(`SELECT COUNT(*) AS count FROM activation_dependencies
      WHERE dependency_kind = 'operation' AND dependency_id = 'acceptance:acceptance-wins-request'`
    ).get() as { count: number }).count), 0);
    database.close();
  });

  it("serializes bound and accepted Answer-and-Request effects against activation cancellation", async (test) => {
    const cancellationWins = await setupCancellation(test);
    const losingMessages = new DirectSignalStore(cancellationWins.runtime.workflow.databasePath);
    test.after(() => losingMessages.close());
    losingMessages.registerRouter({ recipient: cancellationWins.runtime.owner(), endpoint: "losing-answer-owner-router", registeredAtMs: 1 });
    acceptRequest({
      store: losingMessages,
      runtime: cancellationWins.runtime,
      sender: cancellationWins.runtime.owner(),
      recipient: cancellationWins.runtime.agent(cancellationWins.child.agentId),
      recipientOwnership: cancellationWins.run.ownership,
      endpoint: `router:${cancellationWins.child.agentId}`,
      id: "incoming-for-bound-answer",
    });
    losingMessages.bindMessage({
      messageId: "bound-answer-and-request",
      sender: cancellationWins.runtime.agent(cancellationWins.child.agentId),
      recipient: cancellationWins.runtime.owner(),
      sourceEntryId: "bound-answer-and-request-source",
      payloadDigest: "bound-answer-and-request-digest",
      deliveryTiming: "steer",
      responseRequired: true,
      inReplyToRequestId: "incoming-for-bound-answer",
      ownership: cancellationWins.run.ownership,
      createdAtMs: 3,
    });
    await cancellationWins.service.cancel({
      target: cancellationWins.runtime.agent(cancellationWins.child.agentId),
      sourceId: "cancellation-wins-answer-race",
    });
    assert.equal(losingMessages.inspectMessage(cancellationWins.runtime.workflow.ownerAgentId, "bound-answer-and-request"), undefined);
    assert.equal(losingMessages.inspectRequest(cancellationWins.runtime.workflow.ownerAgentId, "incoming-for-bound-answer")?.status, "orphaned");
    assert.equal(losingMessages.inspectRequest(cancellationWins.runtime.workflow.ownerAgentId, "bound-answer-and-request"), undefined);

    const acceptanceWins = await setupCancellation(test);
    const winningMessages = new DirectSignalStore(acceptanceWins.runtime.workflow.databasePath);
    test.after(() => winningMessages.close());
    winningMessages.registerRouter({ recipient: acceptanceWins.runtime.owner(), endpoint: "winning-answer-owner-router", registeredAtMs: 1 });
    acceptRequest({
      store: winningMessages,
      runtime: acceptanceWins.runtime,
      sender: acceptanceWins.runtime.owner(),
      recipient: acceptanceWins.runtime.agent(acceptanceWins.child.agentId),
      recipientOwnership: acceptanceWins.run.ownership,
      endpoint: `router:${acceptanceWins.child.agentId}`,
      id: "incoming-for-accepted-answer",
    });
    winningMessages.bindMessage({
      messageId: "accepted-answer-and-request",
      sender: acceptanceWins.runtime.agent(acceptanceWins.child.agentId),
      recipient: acceptanceWins.runtime.owner(),
      sourceEntryId: "accepted-answer-and-request-source",
      payloadDigest: "accepted-answer-and-request-digest",
      deliveryTiming: "steer",
      responseRequired: true,
      inReplyToRequestId: "incoming-for-accepted-answer",
      ownership: acceptanceWins.run.ownership,
      createdAtMs: 3,
    });
    winningMessages.acceptSignal({
      request: {
        workflowOwnerId: acceptanceWins.runtime.workflow.ownerAgentId,
        messageId: "accepted-answer-and-request",
        senderAgentId: acceptanceWins.child.agentId,
        recipientAgentId: acceptanceWins.runtime.workflow.ownerAgentId,
        sourceEntryId: "accepted-answer-and-request-source",
        payloadDigest: "accepted-answer-and-request-digest",
        deliveryTiming: "steer",
        responseRequired: true,
        inReplyToRequestId: "incoming-for-accepted-answer",
        onAccepted: "continue",
        message: "accepted answer and follow-up",
      },
      recipient: acceptanceWins.runtime.owner(),
      endpoint: "winning-answer-owner-router",
      acceptedAtMs: 4,
    });
    await acceptanceWins.service.cancel({
      target: acceptanceWins.runtime.agent(acceptanceWins.child.agentId),
      sourceId: "acceptance-wins-answer-race",
    });
    assert.equal(winningMessages.inspectRequest(acceptanceWins.runtime.workflow.ownerAgentId, "incoming-for-accepted-answer")?.status, "answered");
    assert.equal(winningMessages.inspectMessage(acceptanceWins.runtime.workflow.ownerAgentId, "accepted-answer-and-request")?.deliveryStatus, "queued");
    assert.equal(winningMessages.inspectRequest(acceptanceWins.runtime.workflow.ownerAgentId, "accepted-answer-and-request")?.status, "cancelled");
  });

  it("rolls back activation cancellation when an outgoing Request invariant is broken", async (test) => {
    const setup = await setupCancellation(test);
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    messages.registerRouter({ recipient: setup.runtime.owner(), endpoint: "broken-request-owner-router", registeredAtMs: 1 });
    acceptRequest({
      store: messages,
      runtime: setup.runtime,
      sender: setup.runtime.agent(setup.child.agentId),
      recipient: setup.runtime.owner(),
      endpoint: "broken-request-owner-router",
      id: "broken-outgoing-request",
    });
    const database = new DatabaseSync(setup.runtime.workflow.databasePath);
    database.prepare("DELETE FROM pending_message_pointers WHERE message_id = 'broken-outgoing-request'").run();
    database.close();

    await assert.rejects(
      setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "cancel-broken-request" }),
      /Pending pointer is missing/,
    );
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "broken-outgoing-request")?.status, "open");
    assert.equal(messages.inspectMessage(setup.runtime.workflow.ownerAgentId, "broken-outgoing-request")?.deliveryStatus, "queued");
    assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state.kind, "active");
  });

  it("fails closed when a bound outbound Message and its acceptance dependency diverge", async (test) => {
    for (const missing of ["dependency", "message"] as const) {
      const setup = await setupCancellation(test);
      const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
      test.after(() => messages.close());
      const messageId = `divergent-bound-${missing}`;
      messages.bindMessage({
        messageId,
        sender: setup.runtime.agent(setup.child.agentId),
        recipient: setup.runtime.owner(),
        sourceEntryId: `divergent-source-${missing}`,
        payloadDigest: `divergent-digest-${missing}`,
        deliveryTiming: "steer",
        responseRequired: false,
        ownership: setup.run.ownership,
        createdAtMs: 1,
      });
      const database = new DatabaseSync(setup.runtime.workflow.databasePath);
      if (missing === "dependency") {
        database.prepare(`DELETE FROM activation_dependencies
          WHERE activation_id = ? AND dependency_kind = 'operation' AND dependency_id = ?`
        ).run(setup.run.ownership.runId, `acceptance:${messageId}`);
      } else {
        database.prepare("DELETE FROM direct_signal_messages WHERE message_id = ?").run(messageId);
      }
      database.close();

      await assert.rejects(
        setup.service.cancel({
          target: setup.runtime.agent(setup.child.agentId),
          sourceId: `cancel-divergent-${missing}`,
        }),
        missing === "dependency" ? /no exact activation acceptance dependency/ : /no exact bound outbound Message/,
      );
      assert.equal(setup.runtime.inspectActivation(setup.runtime.agent(setup.child.agentId))?.state.kind, "active");
    }
  });

  it("terminalizes Human and undeclared correction state and rejects late Human input", async (test) => {
    const setup = await setupCancellation(test);
    setup.runtime.beginHumanInterrupt(setup.run, "human-tool-call");
    setup.runtime.bindHumanResponse(setup.run, "human-tool-call", "human-input");
    setup.runtime.prepareHumanResponseResult(setup.run, "human-tool-call");
    assert.equal(setup.runtime.hasHumanAttention(setup.runtime.agent(setup.child.agentId)), false);

    await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "human-cancel" });
    assert.equal(setup.runtime.inspectHumanInterrupt(setup.runtime.agent(setup.child.agentId))?.status, "terminal");
    assert.equal(setup.runtime.inspectHumanInterrupt(setup.runtime.agent(setup.child.agentId))?.responseInputId, undefined);
    assert.throws(
      () => setup.runtime.bindHumanResponse(setup.run, "human-tool-call", "late-input"),
      (error: unknown) => error instanceof WorkflowProtocolError && error.code === "OwnershipLost",
    );

    const second = await setupCancellation(test);
    second.runtime.settleActivation(second.run);
    const episode = second.runtime.pendingUndeclaredNotice(second.runtime.agent(second.child.agentId));
    assert.ok(episode);
    await second.service.cancel({ target: second.runtime.agent(second.child.agentId), sourceId: "undeclared-cancel" });
    assert.equal(second.runtime.inspectUndeclaredEpisode(second.runtime.agent(second.child.agentId))?.status, "closed");
  });

  it("projects one canonical sender-free orphan Protocol Notice and resolves it on delivery", async (test) => {
    const setup = await setupCancellation(test);
    const batches: InboxBatch[] = [];
    const ownerRouter = new RecipientInboxRouter({
      workflowOwnerId: setup.runtime.workflow.ownerAgentId,
      recipient: setup.runtime.owner(),
      databasePath: setup.runtime.workflow.databasePath,
      projectInboxBatch(batch) { batches.push(batch); },
      now: setup.scenario.clock.now,
    });
    await ownerRouter.start();
    test.after(async () => { await ownerRouter.close(); });
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    acceptRequest({
      store: messages,
      runtime: setup.runtime,
      sender: setup.runtime.owner(),
      recipient: setup.runtime.agent(setup.child.agentId),
      recipientOwnership: setup.run.ownership,
      endpoint: `router:${setup.child.agentId}`,
      id: "canonical-orphan",
      acceptedAtMs: setup.scenario.clock.now(),
    });

    await setup.service.cancel({ target: setup.runtime.agent(setup.child.agentId), sourceId: "canonical-orphan-cancel" });
    await waitFor(() => batches.length === 1);
    assert.equal(batches[0].messages.length, 1);
    assert.deepEqual(batches[0].messages[0], {
      kind: "protocol-notice",
      noticeKind: "request-orphaned",
      messageId: messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "canonical-orphan")?.orphanNotice?.messageId,
      requestId: "canonical-orphan",
      recipientAgentId: setup.runtime.workflow.ownerAgentId,
      deliveryTiming: "steer",
      message: messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "canonical-orphan")?.orphanNotice?.message,
    });
    assert.equal("senderAgentId" in batches[0].messages[0], false);
    assert.equal(ownerRouter.confirmDelivery(batches[0].messages[0].messageId), true);
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "canonical-orphan")?.orphanNotice?.deliveryStatus, "delivered");
  });

  it("moves recovery-pending Request provenance to the replacement activation before cancellation", async (test) => {
    const setup = await setupCancellation(test);
    const messages = new DirectSignalStore(setup.runtime.workflow.databasePath);
    test.after(() => messages.close());
    acceptRequest({
      store: messages,
      runtime: setup.runtime,
      sender: setup.runtime.owner(),
      recipient: setup.runtime.agent(setup.child.agentId),
      recipientOwnership: setup.run.ownership,
      endpoint: `router:${setup.child.agentId}`,
      id: "recovery-incoming",
      acceptedAtMs: setup.scenario.clock.now(),
    });
    setup.runtime.confirmAgentRunExit(setup.run, { error: "recover this obligation" });
    const replacement = setup.runtime.startAgentRun(setup.runtime.agent(setup.child.agentId));
    setup.runtime.checkpoint(replacement, JSON.stringify({ surface: `replacement:${setup.child.agentId}` }));
    const request = messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "recovery-incoming");
    assert.equal(request?.responderActivationId, replacement.ownership.runId);

    setup.observations.splice(0, setup.observations.length, "present", "missing");
    const receipt = await setup.service.cancel({
      target: setup.runtime.agent(setup.child.agentId),
      sourceId: "cancel-recovery",
    });
    assert.equal(receipt.activationId, replacement.ownership.runId);
    assert.equal(messages.inspectRequest(setup.runtime.workflow.ownerAgentId, "recovery-incoming")?.status, "orphaned");
  });

  it("upgrades existing Request rows with activation provenance and orphan-compatible state", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "activation-cancellation-migration-")) });
    const { session: ownerSession, runtime } = scenario.createOwner();
    const childSession = scenario.childSession(runtime, "migration-child");
    const child = runtime.addAgent({ session: childSession, spawner: runtime.owner(), name: "Child" });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    messages.registerRouter({ recipient: runtime.agent(child.agentId), ownership: run.ownership, endpoint: "migration-router", registeredAtMs: 1_000 });
    acceptRequest({
      store: messages,
      runtime,
      sender: runtime.owner(),
      recipient: runtime.agent(child.agentId),
      recipientOwnership: run.ownership,
      endpoint: "migration-router",
      id: "legacy-request",
      acceptedAtMs: 1_001,
    });
    messages.close();
    const databasePath = runtime.workflow.databasePath;
    runtime.close();

    const legacy = new DatabaseSync(databasePath);
    legacy.exec(`
      PRAGMA foreign_keys = OFF;
      BEGIN IMMEDIATE;
      CREATE TABLE workflow_requests_old (
        request_id TEXT PRIMARY KEY,
        requester_agent_id TEXT NOT NULL,
        responder_agent_id TEXT NOT NULL,
        answer_delivery_timing TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('open', 'answered', 'resolved', 'cancelled')),
        answer_message_id TEXT,
        cancelled_at_ms INTEGER,
        cancellation_notice_message_id TEXT,
        cancellation_notice_payload TEXT,
        cancellation_notice_delivery_status TEXT,
        cancellation_notice_delivered_at_ms INTEGER
      ) STRICT;
      INSERT INTO workflow_requests_old SELECT
        request_id, requester_agent_id, responder_agent_id, answer_delivery_timing,
        status, answer_message_id, cancelled_at_ms, cancellation_notice_message_id,
        cancellation_notice_payload, cancellation_notice_delivery_status,
        cancellation_notice_delivered_at_ms
      FROM workflow_requests;
      DROP TABLE workflow_requests;
      ALTER TABLE workflow_requests_old RENAME TO workflow_requests;
      COMMIT;
    `);
    legacy.close();

    const reopened = scenario.startOwner(ownerSession);
    test.after(() => reopened.close());
    const migrated = new DirectSignalStore(databasePath);
    test.after(() => migrated.close());
    const request = migrated.inspectRequest(reopened.workflow.ownerAgentId, "legacy-request");
    assert.equal(request?.responderActivationId, run.ownership.runId);
    const schema = new DatabaseSync(databasePath, { readOnly: true });
    test.after(() => schema.close());
    const table = schema.prepare("SELECT sql FROM sqlite_schema WHERE name = 'workflow_requests'").get() as { sql: string };
    assert.match(table.sql, /'orphaned'/);
    assert.match(table.sql, /requester_activation_id/);
  });

  it("migrates recovery-pending Request provenance to an existing replacement activation", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "activation-cancellation-recovery-migration-")) });
    const { session: ownerSession, runtime } = scenario.createOwner();
    const childSession = scenario.childSession(runtime, "recovery-migration-child");
    const child = runtime.addAgent({ session: childSession, spawner: runtime.owner(), name: "Child" });
    const failedRun = runtime.startAgentRun(runtime.agent(child.agentId));
    const messages = new DirectSignalStore(runtime.workflow.databasePath);
    messages.registerRouter({
      recipient: runtime.agent(child.agentId),
      ownership: failedRun.ownership,
      endpoint: "recovery-migration-router",
      registeredAtMs: scenario.clock.now(),
    });
    messages.registerRouter({ recipient: runtime.owner(), endpoint: "recovery-migration-owner-router", registeredAtMs: scenario.clock.now() });
    acceptRequest({
      store: messages,
      runtime,
      sender: runtime.owner(),
      recipient: runtime.agent(child.agentId),
      recipientOwnership: failedRun.ownership,
      endpoint: "recovery-migration-router",
      id: "legacy-recovery-request",
      acceptedAtMs: scenario.clock.now(),
    });
    acceptRequest({
      store: messages,
      runtime,
      sender: runtime.agent(child.agentId),
      recipient: runtime.owner(),
      endpoint: "recovery-migration-owner-router",
      id: "legacy-recovery-outgoing-request",
      acceptedAtMs: scenario.clock.now(),
    });
    scenario.clock.advance(10);
    runtime.confirmAgentRunExit(failedRun, { error: "failed before schema upgrade" });
    const firstReplacement = runtime.startAgentRun(runtime.agent(child.agentId));
    scenario.clock.advance(10);
    runtime.confirmAgentRunExit(firstReplacement, { error: "replacement also failed before schema upgrade" });
    const currentReplacement = runtime.startAgentRun(runtime.agent(child.agentId));

    const barrierSession = scenario.childSession(runtime, "migration-barrier-child");
    const barrierChild = runtime.addAgent({ session: barrierSession, spawner: runtime.owner(), name: "Barrier Child" });
    const barrierFailedRun = runtime.startAgentRun(runtime.agent(barrierChild.agentId));
    messages.registerRouter({
      recipient: runtime.agent(barrierChild.agentId),
      ownership: barrierFailedRun.ownership,
      endpoint: "migration-barrier-router",
      registeredAtMs: scenario.clock.now(),
    });
    acceptRequest({
      store: messages,
      runtime,
      sender: runtime.owner(),
      recipient: runtime.agent(barrierChild.agentId),
      recipientOwnership: barrierFailedRun.ownership,
      endpoint: "migration-barrier-router",
      id: "legacy-barrier-request",
      acceptedAtMs: scenario.clock.now(),
    });
    scenario.clock.advance(10);
    runtime.confirmAgentRunExit(barrierFailedRun, { error: "transfer to one replacement" });
    const nonFailedBarrier = runtime.startAgentRun(runtime.agent(barrierChild.agentId));
    const barrierDatabase = new DatabaseSync(runtime.workflow.databasePath);
    barrierDatabase.prepare(`UPDATE agent_activations
      SET phase = 'ended', open_state = NULL, ended_outcome = 'completed'
      WHERE activation_id = ?`
    ).run(nonFailedBarrier.ownership.runId);
    barrierDatabase.prepare("DELETE FROM ownership WHERE resource_id = ?").run(nonFailedBarrier.ownership.resourceId);
    barrierDatabase.close();
    runtime.startAgentRun(runtime.agent(barrierChild.agentId));
    messages.close();
    const databasePath = runtime.workflow.databasePath;
    runtime.close();

    const legacy = new DatabaseSync(databasePath);
    legacy.exec(`
      PRAGMA foreign_keys = OFF;
      BEGIN IMMEDIATE;
      CREATE TABLE workflow_requests_old (
        request_id TEXT PRIMARY KEY,
        requester_agent_id TEXT NOT NULL,
        responder_agent_id TEXT NOT NULL,
        answer_delivery_timing TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('open', 'answered', 'resolved', 'cancelled')),
        answer_message_id TEXT,
        cancelled_at_ms INTEGER,
        cancellation_notice_message_id TEXT,
        cancellation_notice_payload TEXT,
        cancellation_notice_delivery_status TEXT,
        cancellation_notice_delivered_at_ms INTEGER
      ) STRICT;
      INSERT INTO workflow_requests_old SELECT
        request_id, requester_agent_id, responder_agent_id, answer_delivery_timing,
        status, answer_message_id, cancelled_at_ms, cancellation_notice_message_id,
        cancellation_notice_payload, cancellation_notice_delivery_status,
        cancellation_notice_delivered_at_ms
      FROM workflow_requests;
      DROP TABLE workflow_requests;
      ALTER TABLE workflow_requests_old RENAME TO workflow_requests;
      COMMIT;
    `);
    legacy.close();

    const reopened = scenario.startOwner(ownerSession);
    test.after(() => reopened.close());
    const migrated = new DirectSignalStore(databasePath);
    test.after(() => migrated.close());
    assert.equal(
      migrated.inspectRequest(reopened.workflow.ownerAgentId, "legacy-recovery-request")?.responderActivationId,
      currentReplacement.ownership.runId,
    );
    assert.equal(
      migrated.inspectRequest(reopened.workflow.ownerAgentId, "legacy-recovery-outgoing-request")?.requesterActivationId,
      currentReplacement.ownership.runId,
    );
    assert.equal(
      migrated.inspectRequest(reopened.workflow.ownerAgentId, "legacy-barrier-request")?.responderActivationId,
      nonFailedBarrier.ownership.runId,
      "migration must not jump across a non-failed activation into unrelated later work",
    );
  });
});

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for protocol projection");
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}
