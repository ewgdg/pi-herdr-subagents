import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { WorkflowScenario } from "./scenario-harness.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { WorkflowInspection } from "../../pi-extension/subagents/protocol/workflow-inspection.ts";

describe("Workflow inspection", () => {
  it("does not disguise operational Agent lookup failures as unknown identities", () => {
    const failure = new Error("sqlite corruption");
    const inspection = new WorkflowInspection({
      workflow: { ownerAgentId: "00000000-0000-4000-8000-000000000001", ownerSessionPath: "", directory: "", sessionsDirectory: "", databasePath: "", createdAtMs: 0 },
      caller: { workflowOwnerId: "00000000-0000-4000-8000-000000000001", agentId: "00000000-0000-4000-8000-000000000001" },
      agents: { inspectAgent() { throw failure; }, listDirectChildren() { return []; }, listWorkflow() { return []; } },
      inspectActivation() { return undefined; }, inspectHumanInterrupt() { return undefined; }, inspectUndeclaredEpisode() { return undefined; },
      inspectRequestProjection() { return undefined; }, now: () => 0,
    });
    assert.throws(() => inspection.inspect({ agent: "00000000-0000-4000-8000-000000000002" }), (error) => error === failure);
  });

  it("projects known Agents with authorization, elapsed time, waiting state, and redacted correction state", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({ session: childSession, spawner: runtime.owner(), name: "Worker One", agentDefinition: "worker" });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    scenario.clock.advance(2_500);
    runtime.settleActivation(run);
    const episode = runtime.inspectUndeclaredEpisode(runtime.agent(child.agentId))!;

    const before = runtime.snapshotDurableState();
    const projected = runtime.inspectTarget({ agent: child.agentId }) as any;
    const after = runtime.snapshotDurableState();

    assert.equal(projected.kind, "agent");
    assert.equal(projected.agentId, child.agentId);
    assert.equal(projected.name, "Worker One");
    assert.equal(projected.definition, "worker");
    assert.equal(projected.role, "ordinary");
    assert.equal(projected.elapsedMs, 2_500);
    assert.equal(projected.state.kind, "waiting");
    assert.deepEqual(projected.dependencies, [{ kind: "undeclared" }]);
    assert.equal(projected.waitingReason, "undeclared-settlement-correction");
    assert.deepEqual(projected.callerAuthority, { inspect: true, relationship: "workflow-owner", enumerateDirectChildren: false, enumerateWorkflow: true });
    assert.deepEqual(projected.undeclaredSettlement, { status: episode.status, allowanceConsumed: true, repeatTriggered: false });
    assert.equal(JSON.stringify(projected).includes(episode.noticeId), false);
    assert.equal(JSON.stringify(projected).includes(episode.noticeText), false);
    assert.deepEqual(after, before, "inspection must not mutate durable state");
  });

  it("uses current state duration and freezes completed activation duration", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-duration-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const session = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({ session, spawner: runtime.owner(), name: "Child" });
    scenario.clock.advance(10_000);
    assert.equal((runtime.inspectTarget({ agent: child.agentId }) as any).elapsedMs, 0, "inactive duration has no persisted state-entry timestamp");
    assert.equal((runtime.inspectTarget({ agent: runtime.workflow.ownerAgentId }) as any).elapsedMs, 0, "owner has no activation duration");
    const first = runtime.startAgentRun(runtime.agent(child.agentId));
    scenario.clock.advance(3_000);
    assert.equal((runtime.inspectTarget({ agent: child.agentId }) as any).elapsedMs, 3_000);
    runtime.addActivationDependency(first, { kind: "agent", dependencyId: "duration-a", agentId: runtime.workflow.ownerAgentId });
    scenario.clock.advance(2_000);
    assert.equal((runtime.inspectTarget({ agent: child.agentId }) as any).elapsedMs, 5_000, "active dependency mutation does not reset activation duration");
    runtime.addActivationDependency(first, { kind: "operation", dependencyId: "duration-b" });
    runtime.settleActivation(first);
    scenario.clock.advance(1_500);
    runtime.satisfyActivationDependency(first, { kind: "agent", dependencyId: "duration-a" });
    scenario.clock.advance(500);
    assert.equal((runtime.inspectTarget({ agent: child.agentId }) as any).elapsedMs, 7_000, "waiting dependency mutation does not reset activation duration");
    runtime.confirmAgentRunExit(first, { error: "SECRET_PROVIDER_FAILURE", exitCode: 19 });
    scenario.clock.advance(7_000);
    const ended = runtime.inspectTarget({ agent: child.agentId }) as any;
    assert.equal(ended.elapsedMs, 7_000);
    assert.deepEqual(ended.state, { kind: "ended", outcome: "failed" });
    assert.equal(JSON.stringify(ended).includes("SECRET_PROVIDER_FAILURE"), false);
    const replacement = runtime.startAgentRun(runtime.agent(child.agentId));
    scenario.clock.advance(500);
    assert.equal((runtime.inspectTarget({ agent: child.agentId }) as any).elapsedMs, 500);
    runtime.cancelActivation(replacement);
  });

  it("redacts Human Interrupt identities and distinguishes pending from response-bound awaiting resume", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-human-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const session = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({ session, spawner: runtime.owner(), name: "Child" });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.beginHumanInterrupt(run, "SECRET_TOOL_CALL");
    let projection = runtime.inspectTarget({ agent: child.agentId }) as any;
    assert.deepEqual(projection.humanInterrupt, { state: "awaiting-response" });
    runtime.bindHumanResponse(run, "SECRET_TOOL_CALL", "SECRET_RESPONSE_INPUT");
    projection = runtime.inspectTarget({ agent: child.agentId }) as any;
    assert.deepEqual(projection.humanInterrupt, { state: "response-bound-awaiting-resume" });
    assert.equal(JSON.stringify(projection).includes("SECRET"), false);
  });

  it("limits enumeration to caller direct children and owner Workflow", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-enum-")) });
    const { runtime: owner } = scenario.createOwner(); test.after(() => owner.close());
    const parentSession = scenario.childSession(owner, "parent");
    const parent = owner.addAgent({ session: parentSession, spawner: owner.owner(), name: "Parent" });
    const siblingSession = scenario.childSession(owner, "sibling");
    owner.addAgent({ session: siblingSession, spawner: owner.owner(), name: "Sibling" });
    const parentRuntime = scenario.startAgent(owner.workflow, parentSession); test.after(() => parentRuntime.close());
    const childSession = scenario.childSession(parentRuntime, "grandchild");
    const child = parentRuntime.addAgent({ session: childSession, spawner: parentRuntime.agent(parent.agentId), name: "Grandchild" });

    assert.deepEqual((parentRuntime.inspectTarget({ directChildren: true }) as any).agents.map((a: any) => a.agentId), [child.agentId]);
    assert.throws(() => parentRuntime.inspectTarget({ workflow: true }), /cannot enumerate/i);
    assert.equal((parentRuntime.inspectTarget({ agent: siblingSession.agentId }) as any).agentId, siblingSession.agentId, "known ID grants inspection");
    assert.equal((owner.inspectTarget({ workflow: true }) as any).agents.length, 4);
  });

  it("projects existing Request correlation, delivery, dependency, and answer identity without payload duplication", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const requesterSession = scenario.childSession(runtime, "requester");
    const requester = runtime.addAgent({ session: requesterSession, spawner: runtime.owner(), name: "Requester" });
    const run = runtime.startAgentRun(runtime.agent(requester.agentId));
    runtime.addActivationDependency(run, { kind: "agent", dependencyId: "request-1", agentId: runtime.workflow.ownerAgentId });
    runtime.settleActivation(run);

    const store = new DirectSignalStore(runtime.workflow.databasePath); test.after(() => store.close());
    store.registerRouter({ recipient: runtime.owner(), endpoint: "owner-router", registeredAtMs: scenario.clock.now() });
    const request = {
      workflowOwnerId: runtime.workflow.ownerAgentId,
      messageId: "request-1",
      senderAgentId: requester.agentId,
      recipientAgentId: runtime.workflow.ownerAgentId,
      sourceEntryId: "source-1",
      payloadDigest: "SECRET_PAYLOAD_DIGEST",
      deliveryTiming: "deferred" as const,
      responseRequired: true,
      message: "SECRET_REQUEST_PAYLOAD",
    };
    store.bindMessage({
      messageId: request.messageId, sender: runtime.agent(requester.agentId), recipient: runtime.owner(),
      sourceEntryId: request.sourceEntryId, payloadDigest: request.payloadDigest,
      deliveryTiming: request.deliveryTiming, responseRequired: true, createdAtMs: scenario.clock.now(),
    });
    store.acceptSignal({ request, recipient: runtime.owner(), endpoint: "owner-router", acceptedAtMs: scenario.clock.now() });

    const projection = runtime.inspectTarget({ request: "request-1" }) as any;
    assert.deepEqual(projection.correlation, { requesterAgentId: requester.agentId, responderAgentId: runtime.workflow.ownerAgentId });
    assert.equal(projection.status, "open");
    assert.equal(projection.answer, null);
    assert.deepEqual(projection.delivery, { request: "queued", answer: "not-created" });
    assert.equal(projection.requesterDependency, "unresolved");
    assert.equal(projection.requesterLifecycleDependency, "waiting");
    assert.equal(JSON.stringify(projection).includes("SECRET"), false);

    const writer = new DatabaseSync(runtime.workflow.databasePath); test.after(() => writer.close());
    writer.exec("BEGIN IMMEDIATE");
    writer.prepare("UPDATE direct_signal_messages SET delivery_status = 'delivered', delivered_at_ms = 10 WHERE message_id = ?").run(request.messageId);
    assert.equal((runtime.inspectTarget({ request: request.messageId }) as any).delivery.request, "queued", "joined read sees the last committed snapshot during a concurrent write");
    writer.exec("COMMIT");
    assert.equal((runtime.inspectTarget({ request: request.messageId }) as any).delivery.request, "delivered");
  });

  it("derives Request obligation from open, answered, and resolved status independently of requester lifecycle", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-states-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const requesterSession = scenario.childSession(runtime, "requester");
    const responderSession = scenario.childSession(runtime, "responder");
    const requester = runtime.addAgent({ session: requesterSession, spawner: runtime.owner(), name: "Requester" });
    const responder = runtime.addAgent({ session: responderSession, spawner: runtime.owner(), name: "Responder" });
    const requesterRun = runtime.startAgentRun(runtime.agent(requester.agentId));
    const responderRun = runtime.startAgentRun(runtime.agent(responder.agentId));
    const store = new DirectSignalStore(runtime.workflow.databasePath); test.after(() => store.close());
    store.registerRouter({ recipient: runtime.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "responder-router", registeredAtMs: 0 });
    store.registerRouter({ recipient: runtime.agent(requester.agentId), ownership: requesterRun.ownership, endpoint: "requester-router", registeredAtMs: 0 });
    const bindAccept = (messageId: string, senderAgentId: string, recipientAgentId: string, endpoint: string, ownership: typeof requesterRun.ownership, inReplyToRequestId?: string) => {
      const request = { workflowOwnerId: runtime.workflow.ownerAgentId, messageId, senderAgentId, recipientAgentId, sourceEntryId: `${messageId}-source`, payloadDigest: `${messageId}-digest`, deliveryTiming: "deferred" as const, responseRequired: !inReplyToRequestId, ...(inReplyToRequestId ? { inReplyToRequestId } : {}), message: "payload" };
      store.bindMessage({ messageId, sender: runtime.agent(senderAgentId), recipient: runtime.agent(recipientAgentId), sourceEntryId: request.sourceEntryId, payloadDigest: request.payloadDigest, deliveryTiming: "deferred", responseRequired: request.responseRequired, ...(inReplyToRequestId ? { inReplyToRequestId } : {}), createdAtMs: 0 });
      store.acceptSignal({ request, recipient: runtime.agent(recipientAgentId), ownership, endpoint, acceptedAtMs: 0 });
    };
    bindAccept("request-states", requester.agentId, responder.agentId, "responder-router", responderRun.ownership);
    let projection = runtime.inspectTarget({ request: "request-states" }) as any;
    assert.equal(projection.requesterDependency, "unresolved");
    assert.equal(projection.requesterLifecycleDependency, "not-waiting");
    runtime.addActivationDependency(requesterRun, { kind: "agent", dependencyId: "request-states", agentId: responder.agentId });
    runtime.settleActivation(requesterRun);
    projection = runtime.inspectTarget({ request: "request-states" }) as any;
    assert.equal(projection.requesterDependency, "unresolved");
    assert.equal(projection.requesterLifecycleDependency, "waiting");
    bindAccept("answer-states", responder.agentId, requester.agentId, "requester-router", requesterRun.ownership, "request-states");
    projection = runtime.inspectTarget({ request: "request-states" }) as any;
    assert.equal(projection.status, "answered");
    assert.equal(projection.requesterDependency, "unresolved");
    store.commitDelivery({ recipient: runtime.agent(requester.agentId), ownership: requesterRun.ownership, endpoint: "requester-router", messageId: "answer-states", deliveredAtMs: 1 });
    projection = runtime.inspectTarget({ request: "request-states" }) as any;
    assert.equal(projection.status, "resolved");
    assert.equal(projection.requesterDependency, "satisfied");
  });

  it("projects cancelled Requests as satisfied with notice delivery and requester-only cancellation authority", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-cancelled-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const responderSession = scenario.childSession(runtime, "responder");
    const unrelatedSession = scenario.childSession(runtime, "unrelated");
    const responder = runtime.addAgent({ session: responderSession, spawner: runtime.owner(), name: "Responder" });
    runtime.addAgent({ session: unrelatedSession, spawner: runtime.owner(), name: "Unrelated" });
    const responderRun = runtime.startAgentRun(runtime.agent(responder.agentId));
    const store = new DirectSignalStore(runtime.workflow.databasePath); test.after(() => store.close());
    store.registerRouter({ recipient: runtime.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "cancel-inspection-router", registeredAtMs: 0 });
    const message = { workflowOwnerId: runtime.workflow.ownerAgentId, messageId: "cancelled-request", senderAgentId: runtime.workflow.ownerAgentId, recipientAgentId: responder.agentId, sourceEntryId: "cancelled-source", payloadDigest: "cancelled-digest", deliveryTiming: "steer" as const, responseRequired: true, onAccepted: "continue" as const, message: "SECRET REQUEST" };
    store.bindMessage({ messageId: message.messageId, sender: runtime.owner(), recipient: runtime.agent(responder.agentId), sourceEntryId: message.sourceEntryId, payloadDigest: message.payloadDigest, deliveryTiming: "steer", responseRequired: true, createdAtMs: 0 });
    store.acceptSignal({ request: message, recipient: runtime.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "cancel-inspection-router", acceptedAtMs: 0 });
    store.commitDelivery({ recipient: runtime.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "cancel-inspection-router", messageId: message.messageId, deliveredAtMs: 1 });
    store.cancelRequest({ requester: runtime.owner(), requestId: message.messageId, noticeMessageId: "cancellation-notice", cancelledAtMs: 2 });

    let projection = runtime.inspectTarget({ request: message.messageId }) as any;
    assert.equal(projection.status, "cancelled");
    assert.equal(projection.answer, null);
    assert.equal(projection.requesterDependency, "satisfied");
    assert.equal(projection.requesterLifecycleDependency, "not-waiting");
    assert.deepEqual(projection.cancellation, { noticeMessageId: "cancellation-notice", delivery: "queued" });
    assert.equal(projection.callerAuthority.cancelRequest, false, "terminal Requests cannot be cancelled again through projected authority");
    assert.equal(JSON.stringify(projection).includes("SECRET"), false);

    const unrelated = scenario.startAgent(runtime.workflow, unrelatedSession); test.after(() => unrelated.close());
    projection = unrelated.inspectTarget({ request: message.messageId }) as any;
    assert.equal(projection.callerAuthority.cancelRequest, false);
  });

  it("does not alter schema or block a concurrent WAL writer while inspecting Requests", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-purity-")) });
    const { runtime } = scenario.createOwner(); test.after(() => runtime.close());
    const initializedMessages = new DirectSignalStore(runtime.workflow.databasePath);
    initializedMessages.close();
    const beforeDatabase = new DatabaseSync(runtime.workflow.databasePath);
    const schemaBefore = beforeDatabase.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name").all();
    const versionsBefore = beforeDatabase.prepare("PRAGMA schema_version").get();
    const messagesBefore = beforeDatabase.prepare("SELECT * FROM direct_signal_messages ORDER BY message_id").all();
    const requestsBefore = beforeDatabase.prepare("SELECT * FROM workflow_requests ORDER BY request_id").all();
    beforeDatabase.close();
    const transcriptBefore = await readFile(runtime.workflow.ownerSessionPath, "utf8");
    assert.throws(() => runtime.inspectTarget({ request: "missing-request" }), /not inspectable/);
    const writer = new DatabaseSync(runtime.workflow.databasePath, { timeout: 100 }); test.after(() => writer.close());
    writer.exec("BEGIN IMMEDIATE");
    try { assert.throws(() => runtime.inspectTarget({ request: "missing-during-write" }), /not inspectable/); }
    finally { writer.exec("ROLLBACK"); }
    const afterDatabase = new DatabaseSync(runtime.workflow.databasePath);
    assert.deepEqual(afterDatabase.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name").all(), schemaBefore);
    assert.deepEqual(afterDatabase.prepare("PRAGMA schema_version").get(), versionsBefore);
    assert.deepEqual(afterDatabase.prepare("SELECT * FROM direct_signal_messages ORDER BY message_id").all(), messagesBefore);
    assert.deepEqual(afterDatabase.prepare("SELECT * FROM workflow_requests ORDER BY request_id").all(), requestsBefore);
    afterDatabase.close();
    assert.equal(await readFile(runtime.workflow.ownerSessionPath, "utf8"), transcriptBefore);
  });

  it("rejects known identities from another Workflow without protected-state disclosure", async (test) => {
    const first = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-cross-a-")) });
    const second = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-cross-b-")) });
    const { runtime: runtimeA } = first.createOwner("Owner A"); test.after(() => runtimeA.close());
    const { runtime: runtimeB } = second.createOwner("Owner B"); test.after(() => runtimeB.close());
    const foreignSession = second.childSession(runtimeB, "foreign");
    runtimeB.addAgent({ session: foreignSession, spawner: runtimeB.owner(), name: "Protected Foreign Name" });

    assert.throws(
      () => runtimeA.inspectTarget({ agent: foreignSession.agentId }),
      (error: Error) => /not inspectable in the current Workflow/.test(error.message)
        && !error.message.includes("Protected Foreign Name")
        && !error.message.includes(runtimeB.workflow.ownerAgentId),
    );
  });

  it("rejects a Request ID from another Workflow without disclosure", async (test) => {
    const first = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-cross-a-")) });
    const second = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-cross-b-")) });
    const { runtime: runtimeA } = first.createOwner("Owner A"); test.after(() => runtimeA.close());
    const { runtime: runtimeB } = second.createOwner("Owner B"); test.after(() => runtimeB.close());
    const responderSession = second.childSession(runtimeB, "foreign-responder");
    const responder = runtimeB.addAgent({ session: responderSession, spawner: runtimeB.owner(), name: "Protected Responder" });
    const responderRun = runtimeB.startAgentRun(runtimeB.agent(responder.agentId));
    const store = new DirectSignalStore(runtimeB.workflow.databasePath); test.after(() => store.close());
    store.registerRouter({ recipient: runtimeB.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "foreign-router", registeredAtMs: 0 });
    const request = { workflowOwnerId: runtimeB.workflow.ownerAgentId, messageId: "foreign-secret-request", senderAgentId: runtimeB.workflow.ownerAgentId, recipientAgentId: responder.agentId, sourceEntryId: "foreign-source", payloadDigest: "foreign-digest", deliveryTiming: "deferred" as const, responseRequired: true, message: "PROTECTED REQUEST PAYLOAD" };
    store.bindMessage({ messageId: request.messageId, sender: runtimeB.owner(), recipient: runtimeB.agent(responder.agentId), sourceEntryId: request.sourceEntryId, payloadDigest: request.payloadDigest, deliveryTiming: request.deliveryTiming, responseRequired: true, createdAtMs: 0 });
    store.acceptSignal({ request, recipient: runtimeB.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "foreign-router", acceptedAtMs: 0 });
    assert.throws(() => runtimeA.inspectTarget({ request: request.messageId }), (error: Error) =>
      /not inspectable in the current Workflow/.test(error.message) && !error.message.includes(runtimeB.workflow.ownerAgentId));
  });

  it("allows an unrelated same-Workflow Agent to inspect a known Request without gaining participant authority", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "inspection-request-known-")) });
    const { runtime: owner } = scenario.createOwner(); test.after(() => owner.close());
    const responderSession = scenario.childSession(owner, "responder");
    const unrelatedSession = scenario.childSession(owner, "unrelated");
    const responder = owner.addAgent({ session: responderSession, spawner: owner.owner(), name: "Responder" });
    owner.addAgent({ session: unrelatedSession, spawner: owner.owner(), name: "Unrelated" });
    const responderRun = owner.startAgentRun(owner.agent(responder.agentId));
    const store = new DirectSignalStore(owner.workflow.databasePath); test.after(() => store.close());
    store.registerRouter({ recipient: owner.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "known-router", registeredAtMs: 0 });
    const request = { workflowOwnerId: owner.workflow.ownerAgentId, messageId: "known-request", senderAgentId: owner.workflow.ownerAgentId, recipientAgentId: responder.agentId, sourceEntryId: "known-source", payloadDigest: "known-digest", deliveryTiming: "deferred" as const, responseRequired: true, message: "payload" };
    store.bindMessage({ messageId: request.messageId, sender: owner.owner(), recipient: owner.agent(responder.agentId), sourceEntryId: request.sourceEntryId, payloadDigest: request.payloadDigest, deliveryTiming: request.deliveryTiming, responseRequired: true, createdAtMs: 0 });
    store.acceptSignal({ request, recipient: owner.agent(responder.agentId), ownership: responderRun.ownership, endpoint: "known-router", acceptedAtMs: 0 });
    const unrelated = scenario.startAgent(owner.workflow, unrelatedSession); test.after(() => unrelated.close());
    const projection = unrelated.inspectTarget({ request: request.messageId }) as any;
    assert.deepEqual(projection.callerAuthority, { inspect: true, relationship: "known-request", workflowOwner: false, requester: false, responder: false, cancelRequest: false });
  });
});
