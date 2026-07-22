import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, it, type TestContext } from "node:test";
import { DirectSignalRuntime, type InboxBatch } from "../../pi-extension/subagents/protocol/direct-signal.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { WorkflowScenario } from "./scenario-harness.ts";

function closeAfter<T extends { close(): void | Promise<void> }>(test: TestContext, resource: T): T {
  test.after(async () => { await resource.close(); });
  return resource;
}

async function waitFor(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("condition was not satisfied before timeout");
}

async function cancellationScenario(
  test: TestContext,
  projectResponder: boolean | ((batch: InboxBatch) => void) = true,
) {
  const scenario = new WorkflowScenario({ rootDirectory: await mkdtemp(join(tmpdir(), "request-cancellation-")) });
  const { runtime: ownerRuntime } = scenario.createOwner();
  test.after(() => ownerRuntime.close());
  const owner = ownerRuntime.owner();
  const responderSession = scenario.childSession(ownerRuntime, "responder");
  const responder = ownerRuntime.addAgent({ session: responderSession, spawner: owner, name: "Responder" });
  const responderRuntime = scenario.startAgent(ownerRuntime.workflow, responderSession);
  test.after(() => responderRuntime.close());
  const responderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(responder.agentId));
  if (projectResponder === false) ownerRuntime.beginHumanInterrupt(responderRun, "hold-inbox-projection");
  const responderBatches: InboxBatch[] = [];
  let responderWakes = 0;
  const ownerMessages = closeAfter(test, new DirectSignalRuntime({
    controlPlane: ownerRuntime.controlPlane,
    allocateMessageId: () => scenario.identities.next(),
    projectInboxBatch() {},
  }));
  const responderMessages = closeAfter(test, new DirectSignalRuntime({
    controlPlane: responderRuntime.controlPlane,
    ownership: responderRun.ownership,
    allocateMessageId: () => scenario.identities.next(),
    projectInboxBatch(batch) {
      if (!projectResponder) throw new Error("projection unavailable");
      responderBatches.push(batch);
      if (typeof projectResponder === "function") projectResponder(batch);
    },
    wakeRecipient() { responderWakes += 1; },
  }));
  await ownerMessages.start();
  await responderMessages.start();
  return {
    scenario, ownerRuntime, owner, responder, responderSession, responderRuntime, responderRun,
    ownerMessages, responderMessages, responderBatches,
    responderWakes: () => responderWakes,
  };
}

async function sendRequest(setup: Awaited<ReturnType<typeof cancellationScenario>>, message = "Perform the requested work") {
  const sourceEntryId = setup.scenario.transcripts.appendAgentSend(
    { agentId: setup.owner.agentId, sessionPath: setup.ownerRuntime.workflow.ownerSessionPath },
    { targetAgentId: setup.responder.agentId, message, responseRequired: true },
  );
  return setup.ownerMessages.sendMessage({
    target: { agentId: setup.responder.agentId },
    message,
    sourceEntryId,
    responseRequired: true,
    onAccepted: "continue",
  });
}

describe("Request cancellation", () => {
  it("allows only the original requester and suppresses an undelivered Request without a notice", async (test) => {
    const setup = await cancellationScenario(test, false);
    const request = await sendRequest(setup);
    const intruderSession = setup.scenario.childSession(setup.ownerRuntime, "intruder");
    setup.ownerRuntime.addAgent({ session: intruderSession, spawner: setup.owner, name: "Intruder" });
    const intruderRuntime = setup.scenario.startAgent(setup.ownerRuntime.workflow, intruderSession);
    test.after(() => intruderRuntime.close());
    const intruderMessages = closeAfter(test, new DirectSignalRuntime({ controlPlane: intruderRuntime.controlPlane }));

    await assert.rejects(
      intruderMessages.cancelRequest(request.messageId),
      (error: unknown) => (error as { code?: string }).code === "RequestCancellationUnauthorized",
    );

    const result = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.deepEqual(result, {
      requestId: request.messageId,
      status: "cancelled",
      delivery: "suppressed",
    });
    assert.equal(setup.ownerMessages.inspectMessage(request.messageId)?.deliveryStatus, "suppressed");
    assert.equal(setup.responderMessages.listPending(setup.ownerRuntime.agent(setup.responder.agentId)).length, 0);
    assert.equal(setup.ownerMessages.inspectRequest(request.messageId)?.status, "cancelled");
    assert.equal(setup.ownerMessages.inspectRequest(request.messageId)?.cancellationNotice, undefined);

    assert.deepEqual(await setup.ownerMessages.cancelRequest(request.messageId), result, "requester retries are idempotent");
    assert.equal(setup.ownerMessages.listMessages().length, 1, "suppression never creates a Protocol Notice");
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    assert.equal(setup.responderBatches.flatMap((batch) => batch.messages)
      .some((message) => message.messageId === request.messageId), false);
    assert.equal(setup.responderWakes(), 0);
  });

  it("fails atomically when an unprojected Request loses its pending pointer", async (test) => {
    const setup = await cancellationScenario(test, false);
    const request = await sendRequest(setup);
    const database = new DatabaseSync(setup.ownerRuntime.workflow.databasePath);
    database.prepare("DELETE FROM pending_message_pointers WHERE message_id = ?").run(request.messageId);
    database.close();

    await assert.rejects(setup.ownerMessages.cancelRequest(request.messageId), /Pending pointer is missing/);
    assert.equal(setup.ownerMessages.inspectRequest(request.messageId)?.status, "open");
    assert.equal(setup.ownerMessages.inspectMessage(request.messageId)?.deliveryStatus, "queued");
  });

  it("queues exactly one correlated runtime-authored Steer notice after delivery and wakes a waiting responder", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === request.messageId)));
    assert.equal(setup.responderMessages.confirmDelivery(request.messageId), true);
    setup.ownerRuntime.settleActivation(setup.responderRun);

    const result = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.equal(result.status, "cancelled");
    assert.equal(result.delivery, "notice-queued");
    assert.ok(result.noticeMessageId);
    assert.deepEqual(await setup.ownerMessages.cancelRequest(request.messageId), result);

    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === result.noticeMessageId)));
    const notices = setup.responderBatches.flatMap((batch) => batch.messages)
      .filter((message) => message.kind === "protocol-notice" && message.messageId === result.noticeMessageId);
    assert.equal(notices.length, 1);
    assert.deepEqual(notices[0], {
      kind: "protocol-notice",
      noticeKind: "request-cancelled",
      messageId: result.noticeMessageId,
      requestId: request.messageId,
      recipientAgentId: setup.responder.agentId,
      deliveryTiming: "steer",
      message: setup.ownerMessages.inspectRequest(request.messageId)?.cancellationNotice?.message,
    });
    assert.equal(setup.ownerMessages.listMessages().filter((message) => message.kind === "protocol-notice").length, 1);
    assert.equal(setup.responderMessages.confirmDelivery(result.noticeMessageId!), true);
    assert.equal(setup.responderWakes(), 1);
    assert.equal(setup.ownerMessages.inspectRequest(request.messageId)?.cancellationNotice?.deliveryStatus, "delivered");
    assert.equal(setup.responderMessages.confirmDelivery(result.noticeMessageId!), false);
    assert.deepEqual(await setup.ownerMessages.cancelRequest(request.messageId), {
      ...result,
      delivery: "notice-delivered",
    });
    assert.equal(setup.ownerMessages.listMessages().filter((message) => message.kind === "protocol-notice").length, 1);
    assert.equal(setup.responderWakes(), 1, "delivery confirmation is idempotent");
  });

  it("does not suppress a Request whose transcript projection already began", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === request.messageId)));

    const result = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.equal(result.delivery, "notice-queued");
    assert.ok(result.noticeMessageId);
    assert.equal(setup.ownerMessages.inspectMessage(request.messageId)?.deliveryStatus, "queued");
    assert.equal(setup.responderMessages.listPending(setup.ownerRuntime.agent(setup.responder.agentId))
      .some((pointer) => pointer.messageId === request.messageId), true,
    "a projected Request must remain confirmable instead of disappearing from under the recipient transcript");
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === result.noticeMessageId)));
  });

  it("delivers a queued cancellation notice after the responder Router restarts", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === request.messageId)));
    setup.responderMessages.confirmDelivery(request.messageId);
    await setup.responderMessages.close();

    const result = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.equal(result.delivery, "notice-queued");
    const recoveredBatches: InboxBatch[] = [];
    const restarted = closeAfter(test, new DirectSignalRuntime({
      controlPlane: setup.responderRuntime.controlPlane,
      ownership: setup.responderRun.ownership,
      projectInboxBatch(batch) { recoveredBatches.push(batch); },
    }));
    await restarted.start();
    await waitFor(() => recoveredBatches.some((batch) => batch.messages.some((message) => message.messageId === result.noticeMessageId)));
    assert.equal(recoveredBatches.flatMap((batch) => batch.messages)
      .filter((message) => message.messageId === result.noticeMessageId).length, 1);
  });

  it("recovers a pre-persistence projection claim by reprojecting the Request before its cancellation notice", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === request.messageId)));
    await setup.responderMessages.close();
    const cancellation = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.equal(cancellation.delivery, "notice-queued");

    const recoveredBatches: InboxBatch[] = [];
    const restarted = closeAfter(test, new DirectSignalRuntime({
      controlPlane: setup.responderRuntime.controlPlane,
      ownership: setup.responderRun.ownership,
      projectInboxBatch(batch) { recoveredBatches.push(batch); },
      hasProjectedMessage() { return false; },
    }));
    await restarted.start();
    await waitFor(() => recoveredBatches.flatMap((batch) => batch.messages)
      .some((message) => message.messageId === cancellation.noticeMessageId));
    assert.deepEqual(
      recoveredBatches.flatMap((batch) => batch.messages).map((message) => message.messageId),
      [request.messageId, cancellation.noticeMessageId],
      "recovery must preserve original acceptance order without losing the Request",
    );
  });

  it("queues a notice when transcript persistence escapes the projection-marker transaction", async (test) => {
    const transcriptEvidence = new Set<string>();
    const setup = await cancellationScenario(test, (batch) => {
      for (const message of batch.messages) transcriptEvidence.add(message.messageId);
      throw new Error("crash after transcript persistence");
    });
    const request = await sendRequest(setup);
    await waitFor(() => transcriptEvidence.has(request.messageId));
    await setup.responderMessages.close();
    const ambiguousPointer = setup.ownerMessages.listPending(setup.ownerRuntime.agent(setup.responder.agentId))
      .find((pointer) => pointer.messageId === request.messageId);
    assert.equal(ambiguousPointer?.projectionClaimed, true);
    assert.equal(ambiguousPointer?.projectionCommitted, false);
    assert.equal(
      setup.ownerMessages.inspectMessage(request.messageId)?.deliveryStatus,
      "queued",
      "the transcript-persisted Request has not reached SQLite delivery confirmation",
    );

    const cancellation = await setup.ownerMessages.cancelRequest(request.messageId);
    assert.equal(cancellation.delivery, "notice-queued");
    assert.ok(cancellation.noticeMessageId);
    assert.equal(setup.ownerMessages.inspectMessage(request.messageId)?.deliveryStatus, "queued");

    const recoveredBatches: InboxBatch[] = [];
    const restarted = closeAfter(test, new DirectSignalRuntime({
      controlPlane: setup.responderRuntime.controlPlane,
      ownership: setup.responderRun.ownership,
      projectInboxBatch(batch) { recoveredBatches.push(batch); },
      hasProjectedMessage(messageId) { return transcriptEvidence.has(messageId); },
    }));
    await restarted.start();
    await waitFor(() => recoveredBatches.flatMap((batch) => batch.messages)
      .some((message) => message.messageId === cancellation.noticeMessageId));
    assert.deepEqual(
      recoveredBatches.flatMap((batch) => batch.messages).map((message) => message.messageId),
      [cancellation.noticeMessageId],
      "recovery confirms transcript evidence instead of projecting the Request twice",
    );
  });

  it("drains a durable cancellation notice even when its scheduling hint is lost", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    await waitFor(() => setup.responderBatches.some((batch) => batch.messages.some((message) => message.messageId === request.messageId)));
    setup.responderMessages.confirmDelivery(request.messageId);
    setup.ownerRuntime.settleActivation(setup.responderRun);
    const store = closeAfter(test, new DirectSignalStore(setup.ownerRuntime.workflow.databasePath));
    const result = store.cancelRequest({
      requester: setup.owner,
      requestId: request.messageId,
      noticeMessageId: "notice-with-lost-schedule-hint",
      cancelledAtMs: setup.scenario.clock.now(),
    });
    assert.equal(result.delivery, "notice-queued");

    await waitFor(() => setup.responderBatches.flatMap((batch) => batch.messages)
      .some((message) => message.messageId === result.noticeMessageId));
  });

  it("lets cancellation win before Answer acceptance without partial Answer effects", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    setup.responderMessages.confirmDelivery(request.messageId);
    await setup.ownerMessages.cancelRequest(request.messageId);
    const answerSource = setup.scenario.transcripts.appendAgentSend(setup.responderSession, {
      targetRequestId: request.messageId,
      message: "late answer",
      responseRequired: true,
    });

    await assert.rejects(
      setup.responderMessages.sendMessage({
        target: { requestId: request.messageId }, message: "late answer", sourceEntryId: answerSource,
        responseRequired: true, onAccepted: "continue",
      }),
      (error: unknown) => (error as { code?: string }).code === "AnswerAlreadyClosed",
    );
    const record = setup.ownerMessages.inspectRequest(request.messageId)!;
    assert.equal(record.status, "cancelled");
    assert.equal(record.answerMessageId, undefined);
    assert.deepEqual(setup.responderMessages.listRequests(setup.ownerRuntime.agent(setup.responder.agentId)), [],
      "the losing Answer-and-Request creates no follow-up Request");
  });

  it("lets an accepted Answer win before cancellation without partial cancellation effects", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    setup.responderMessages.confirmDelivery(request.messageId);
    const answerSource = setup.scenario.transcripts.appendAgentSend(setup.responderSession, {
      targetRequestId: request.messageId,
      message: "answer wins",
      responseRequired: true,
    });
    const answer = await setup.responderMessages.sendMessage({
      target: { requestId: request.messageId }, message: "answer wins", sourceEntryId: answerSource,
      responseRequired: true, onAccepted: "continue",
    });

    await assert.rejects(
      setup.ownerMessages.cancelRequest(request.messageId),
      (error: unknown) => (error as { code?: string }).code === "RequestAlreadyClosed",
    );
    const record = setup.ownerMessages.inspectRequest(request.messageId)!;
    assert.equal(record.status, "answered");
    assert.equal(record.answerMessageId, answer.messageId);
    assert.equal(record.cancellationNotice, undefined);
    assert.equal(setup.responderMessages.inspectRequest(answer.messageId)?.status, "open",
      "the winning Answer-and-Request keeps its atomic follow-up Request");
  });

  it("serializes the Answer and cancellation commit when stores race", async (test) => {
    const setup = await cancellationScenario(test);
    const request = await sendRequest(setup);
    setup.responderMessages.confirmDelivery(request.messageId);
    const answerSource = setup.scenario.transcripts.appendAgentSend(setup.responderSession, {
      targetRequestId: request.messageId,
      message: "racing answer",
    });
    const answerPromise = setup.responderMessages.sendMessage({
      target: { requestId: request.messageId }, message: "racing answer", sourceEntryId: answerSource, onAccepted: "continue",
    });
    const cancellationPromise = setup.ownerMessages.cancelRequest(request.messageId);
    const outcomes = await Promise.allSettled([answerPromise, cancellationPromise]);
    assert.equal(outcomes.filter((outcome) => outcome.status === "fulfilled").length, 1);
    assert.equal(outcomes.filter((outcome) => outcome.status === "rejected").length, 1);
    const record = setup.ownerMessages.inspectRequest(request.messageId)!;
    assert.ok(record.status === "answered" || record.status === "cancelled");
    assert.equal(Boolean(record.answerMessageId), record.status === "answered");
    assert.equal(Boolean(record.cancellationNotice), record.status === "cancelled");
  });

  it("persists cancellation across store restart", async (test) => {
    const setup = await cancellationScenario(test, false);
    const request = await sendRequest(setup);
    const result = await setup.ownerMessages.cancelRequest(request.messageId);
    const reopened = closeAfter(test, new DirectSignalStore(setup.ownerRuntime.workflow.databasePath));
    assert.equal(reopened.inspectRequest(setup.owner.workflowOwnerId, request.messageId)?.status, "cancelled");
    assert.equal(reopened.inspectMessage(setup.owner.workflowOwnerId, request.messageId)?.deliveryStatus, "suppressed");
    assert.equal(result.delivery, "suppressed");
  });
});
