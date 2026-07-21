import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, it, type TestContext } from "node:test";
import {
  DEFAULT_MAXIMUM_FRAME_BYTES,
  listenForFramedIpc,
} from "../../pi-extension/subagents/coordination/framed-ipc.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import {
  DirectSignalRuntime,
  type InboxBatch,
} from "../../pi-extension/subagents/protocol/direct-signal.ts";
import { RecipientInboxRouter } from "../../pi-extension/subagents/protocol/recipient-inbox-router.ts";
import { digestPayload } from "../../pi-extension/subagents/protocol/direct-signal-transcript.ts";
import {
  DeterministicIdentityFactory,
  WorkflowScenario,
} from "./scenario-harness.ts";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-signal-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })),
  );
});

async function waitFor(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("condition was not satisfied before timeout");
}

function closeAfter<T extends { close(): void | Promise<void> }>(test: TestContext, resource: T): T {
  test.after(async () => { await resource.close(); });
  return resource;
}

function directSignalRuntime(test: TestContext, options: ConstructorParameters<typeof DirectSignalRuntime>[0]): DirectSignalRuntime {
  return closeAfter(test, new DirectSignalRuntime(options));
}

describe("direct Signal protocol scenarios", () => {
  it("accepts a Request atomically, then an Answer resolves only its durable requester dependency on delivery", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const responderSession = scenario.childSession(ownerRuntime, "responder");
    const responder = ownerRuntime.addAgent({ session: responderSession, spawner: owner, name: "Responder" });
    const responderRuntime = scenario.startAgent(ownerRuntime.workflow, responderSession);
    const responderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(responder.agentId));
    ownerRuntime.settleActivation(responderRun);
    const ownerBatches: InboxBatch[] = [];
    let ownerWakes = 0;
    const ownerMessages = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch(batch) { ownerBatches.push(batch); },
      wakeRecipient() { ownerWakes += 1; },
    });
    await ownerMessages.start();
    const responderMessages = directSignalRuntime(test, {
      controlPlane: responderRuntime.controlPlane,
      ownership: responderRun.ownership,
      allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch(batch) { scenario.transcripts.appendInboxBatch(responderSession, batch); },
    });
    await responderMessages.start();
    const requestSource = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: responder.agentId, message: "do the work", timing: "deferred", responseRequired: true },
    );
    const otherSource = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: responder.agentId, message: "other work", responseRequired: true },
    );

    const request = await ownerMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "do the work", sourceEntryId: requestSource,
      deliveryTiming: "deferred", responseRequired: true,
    });
    await ownerMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "other work", sourceEntryId: otherSource, responseRequired: true,
    });
    assert.deepEqual(ownerMessages.inspectRequest(request.messageId), {
      requestId: request.messageId,
      requesterAgentId: owner.agentId,
      responderAgentId: responder.agentId,
      answerDeliveryTiming: "deferred",
      status: "open",
    });
    assert.deepEqual(ownerRuntime.inspectActivation(owner)?.state, undefined);
    ownerRuntime.settleOwnerTurn();

    const answerSource = scenario.transcripts.appendAgentSend(responderSession, {
      targetRequestId: request.messageId, message: "done",
    });
    const answer = await responderMessages.sendMessage({
      target: { requestId: request.messageId }, message: "done", sourceEntryId: answerSource,
    });
    assert.equal(ownerMessages.inspectRequest(request.messageId)?.status, "answered");
    await waitFor(() => ownerBatches.length === 1);
    assert.equal(ownerWakes, 0);
    assert.equal(ownerMessages.confirmDelivery(answer.messageId), true);
    assert.equal(ownerWakes, 1);
    assert.equal(ownerMessages.inspectRequest(request.messageId)?.status, "resolved");
    assert.deepEqual(
      ownerMessages.listRequests(owner).map((item) => ({ requestId: item.requestId, status: item.status })),
      [{ requestId: request.messageId, status: "resolved" }, { requestId: ownerMessages.listRequests(owner)[1].requestId, status: "open" }],
    );

    await ownerMessages.close();
    await responderMessages.close();
    ownerRuntime.confirmAgentRunExit(responderRun, { error: "test cleanup" });
    responderRuntime.close();
    ownerRuntime.close();
  });

  it("restricts Answers to the addressed responder, closes the slot once, and permits Answer-plus-Request", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const responderSession = scenario.childSession(ownerRuntime, "responder");
    const intruderSession = scenario.childSession(ownerRuntime, "intruder");
    const responder = ownerRuntime.addAgent({ session: responderSession, spawner: owner, name: "Responder" });
    const intruder = ownerRuntime.addAgent({ session: intruderSession, spawner: owner, name: "Intruder" });
    const responderRuntime = scenario.startAgent(ownerRuntime.workflow, responderSession);
    const intruderRuntime = scenario.startAgent(ownerRuntime.workflow, intruderSession);
    const responderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(responder.agentId));
    const intruderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(intruder.agentId));
    ownerRuntime.settleActivation(responderRun);
    const ownerMessages = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
      projectInboxBatch() {},
    });
    const responderMessages = directSignalRuntime(test, {
      controlPlane: responderRuntime.controlPlane, ownership: responderRun.ownership,
      allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
    });
    const intruderMessages = directSignalRuntime(test, {
      controlPlane: intruderRuntime.controlPlane, ownership: intruderRun.ownership,
      allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
    });
    await ownerMessages.start();
    await responderMessages.start();
    const requestSource = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: responder.agentId, message: "request", timing: "deferred", responseRequired: true },
    );
    const request = await ownerMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "request", sourceEntryId: requestSource,
      deliveryTiming: "deferred", responseRequired: true,
    });
    const intruderSource = scenario.transcripts.appendAgentSend(intruderSession, {
      targetRequestId: request.messageId, message: "forged answer",
    });
    await assert.rejects(
      intruderMessages.sendMessage({ target: { requestId: request.messageId }, message: "forged answer", sourceEntryId: intruderSource }),
      (error: unknown) => (error as { code?: string }).code === "AnswerUnauthorized",
    );
    assert.equal(intruderMessages.listMessages().length, 1, "only the Request should be durable before an authorized Answer");
    const answerSource = scenario.transcripts.appendAgentSend(responderSession, {
      targetRequestId: request.messageId, message: "answer and ask", responseRequired: true,
    });
    const answer = await responderMessages.sendMessage({
      target: { requestId: request.messageId }, message: "answer and ask", sourceEntryId: answerSource, responseRequired: true,
    });
    const retry = await responderMessages.sendMessage({
      target: { requestId: request.messageId }, message: "answer and ask", sourceEntryId: answerSource, responseRequired: true,
    });
    assert.deepEqual(retry, answer);
    assert.deepEqual(ownerMessages.inspectRequest(request.messageId), {
      requestId: request.messageId, requesterAgentId: owner.agentId, responderAgentId: responder.agentId,
      answerDeliveryTiming: "deferred", status: "answered", answerMessageId: answer.messageId,
    });
    assert.deepEqual(ownerMessages.inspectRequest(answer.messageId), {
      requestId: answer.messageId, requesterAgentId: responder.agentId, responderAgentId: owner.agentId,
      answerDeliveryTiming: "deferred", status: "open",
    });
    const laterSource = scenario.transcripts.appendAgentSend(responderSession, {
      targetRequestId: request.messageId, message: "late answer",
    });
    await assert.rejects(
      responderMessages.sendMessage({ target: { requestId: request.messageId }, message: "late answer", sourceEntryId: laterSource }),
      (error: unknown) => (error as { code?: string }).code === "AnswerAlreadyClosed",
    );
    await assert.rejects(
      responderMessages.sendMessage({ target: { requestId: answer.messageId }, message: "wrong timing", sourceEntryId: laterSource, deliveryTiming: "steer" }),
      /derive delivery timing/,
    );

    await ownerMessages.close();
    await responderMessages.close();
    await intruderMessages.close();
    ownerRuntime.confirmAgentRunExit(responderRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(intruderRun, { error: "test cleanup" });
    responderRuntime.close();
    intruderRuntime.close();
    ownerRuntime.close();
  });

  it("wakes a waiting requester once for a newly delivered Answer without resolving its other Requests", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const requesterSession = scenario.childSession(ownerRuntime, "requester");
    const responderSession = scenario.childSession(ownerRuntime, "responder");
    const requester = ownerRuntime.addAgent({ session: requesterSession, spawner: owner, name: "Requester" });
    const responder = ownerRuntime.addAgent({ session: responderSession, spawner: owner, name: "Responder" });
    const requesterRuntime = scenario.startAgent(ownerRuntime.workflow, requesterSession);
    const responderRuntime = scenario.startAgent(ownerRuntime.workflow, responderSession);
    const requesterRun = ownerRuntime.startAgentRun(ownerRuntime.agent(requester.agentId));
    const responderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(responder.agentId));
    ownerRuntime.settleActivation(requesterRun);
    ownerRuntime.settleActivation(responderRun);
    let wakeCount = 0;
    const requesterMessages = directSignalRuntime(test, {
      controlPlane: requesterRuntime.controlPlane, ownership: requesterRun.ownership,
      allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
      wakeRecipient() { wakeCount += 1; ownerRuntime.activateTurn(requesterRun); },
    });
    const responderMessages = directSignalRuntime(test, {
      controlPlane: responderRuntime.controlPlane, ownership: responderRun.ownership,
      allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
    });
    await requesterMessages.start();
    await responderMessages.start();
    const firstSource = scenario.transcripts.appendAgentSend(requesterSession, {
      targetAgentId: responder.agentId, message: "first", responseRequired: true,
    });
    const secondSource = scenario.transcripts.appendAgentSend(requesterSession, {
      targetAgentId: responder.agentId, message: "second", responseRequired: true,
    });
    const first = await requesterMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "first", sourceEntryId: firstSource, responseRequired: true,
    });
    const second = await requesterMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "second", sourceEntryId: secondSource, responseRequired: true,
    });
    assert.deepEqual(ownerRuntime.inspectActivation(ownerRuntime.agent(requester.agentId))?.state, {
      kind: "waiting",
      dependencies: [
        { kind: "agent", dependencyId: first.messageId, agentId: responder.agentId },
        { kind: "agent", dependencyId: second.messageId, agentId: responder.agentId },
      ],
    });
    const answerSource = scenario.transcripts.appendAgentSend(responderSession, {
      targetRequestId: first.messageId, message: "first answer",
    });
    const answer = await responderMessages.sendMessage({
      target: { requestId: first.messageId }, message: "first answer", sourceEntryId: answerSource,
    });
    await waitFor(() => requesterMessages.inspectMessage(answer.messageId)?.deliveryStatus === "queued");
    assert.equal(ownerRuntime.inspectActivation(ownerRuntime.agent(requester.agentId))?.state.kind, "waiting");
    assert.equal(requesterMessages.confirmDelivery(answer.messageId), true);
    assert.equal(wakeCount, 1);
    assert.equal(ownerRuntime.inspectActivation(ownerRuntime.agent(requester.agentId))?.state.kind, "active");
    ownerRuntime.settleActivation(requesterRun);
    assert.deepEqual(ownerRuntime.inspectActivation(ownerRuntime.agent(requester.agentId))?.state, {
      kind: "waiting",
      dependencies: [{ kind: "agent", dependencyId: second.messageId, agentId: responder.agentId }],
    });
    assert.equal(requesterMessages.confirmDelivery(answer.messageId), false);
    assert.equal(wakeCount, 1, "an idempotent delivery confirmation must not wake another turn");
    assert.deepEqual(ownerRuntime.inspectActivation(ownerRuntime.agent(requester.agentId))?.state, {
      kind: "waiting",
      dependencies: [{ kind: "agent", dependencyId: second.messageId, agentId: responder.agentId }],
    });

    await requesterMessages.close();
    await responderMessages.close();
    ownerRuntime.confirmAgentRunExit(requesterRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(responderRun, { error: "test cleanup" });
    requesterRuntime.close();
    responderRuntime.close();
    ownerRuntime.close();
  });

  it("rejects forged Answer recipient and timing at the durable acceptance boundary", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const responderSession = scenario.childSession(ownerRuntime, "responder");
    const intruderSession = scenario.childSession(ownerRuntime, "intruder");
    const responder = ownerRuntime.addAgent({ session: responderSession, spawner: owner, name: "Responder" });
    const intruder = ownerRuntime.addAgent({ session: intruderSession, spawner: owner, name: "Intruder" });
    const responderRuntime = scenario.startAgent(ownerRuntime.workflow, responderSession);
    const intruderRuntime = scenario.startAgent(ownerRuntime.workflow, intruderSession);
    const responderRun = ownerRuntime.startAgentRun(responder);
    const intruderRun = ownerRuntime.startAgentRun(intruder);
    const ownerMessages = directSignalRuntime(test, { controlPlane: ownerRuntime.controlPlane, projectInboxBatch() {} });
    const responderMessages = directSignalRuntime(test, {
      controlPlane: responderRuntime.controlPlane, ownership: responderRun.ownership, projectInboxBatch() {},
    });
    const intruderMessages = directSignalRuntime(test, {
      controlPlane: intruderRuntime.controlPlane, ownership: intruderRun.ownership, projectInboxBatch() {},
    });
    await ownerMessages.start();
    await responderMessages.start();
    await intruderMessages.start();
    const requestSource = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: responder.agentId, message: "request", timing: "deferred", responseRequired: true },
    );
    const request = await ownerMessages.sendMessage({
      target: { agentId: responder.agentId }, message: "request", sourceEntryId: requestSource,
      deliveryTiming: "deferred", responseRequired: true,
    });
    const store = closeAfter(test, new DirectSignalStore(ownerRuntime.workflow.databasePath));
    const bindAnswer = (messageId: string, recipientAgentId: string, deliveryTiming: "steer" | "deferred") => {
      store.bindMessage({
        messageId, sender: responder, recipient: ownerRuntime.agent(recipientAgentId), sourceEntryId: `${messageId}-source`,
        payloadDigest: `${messageId}-digest`, deliveryTiming, responseRequired: false,
        inReplyToRequestId: request.messageId, createdAtMs: scenario.clock.now(),
      });
    };

    bindAnswer("wrong-recipient", intruder.agentId, "deferred");
    assert.throws(() => store.acceptSignal({
      request: {
        workflowOwnerId: owner.workflowOwnerId, messageId: "wrong-recipient", senderAgentId: responder.agentId,
        recipientAgentId: intruder.agentId, sourceEntryId: "wrong-recipient-source", payloadDigest: "wrong-recipient-digest",
        deliveryTiming: "deferred", responseRequired: false, inReplyToRequestId: request.messageId, message: "forged recipient",
      },
      recipient: intruder, ownership: intruderRun.ownership, endpoint: store.readRouter(intruder)!.endpoint,
      acceptedAtMs: scenario.clock.now(),
    }), (error: unknown) => (error as { code?: string }).code === "AnswerUnauthorized");
    assert.equal(store.inspectRequest(owner.workflowOwnerId, request.messageId)?.status, "open");
    assert.equal(store.inspectMessage(owner.workflowOwnerId, "wrong-recipient")?.deliveryStatus, "bound");
    assert.equal(store.discardUnacceptedMessage(responder, "wrong-recipient"), true);

    bindAnswer("wrong-timing", owner.agentId, "steer");
    assert.throws(() => store.acceptSignal({
      request: {
        workflowOwnerId: owner.workflowOwnerId, messageId: "wrong-timing", senderAgentId: responder.agentId,
        recipientAgentId: owner.agentId, sourceEntryId: "wrong-timing-source", payloadDigest: "wrong-timing-digest",
        deliveryTiming: "steer", responseRequired: false, inReplyToRequestId: request.messageId, message: "forged timing",
      },
      recipient: owner, endpoint: store.readRouter(owner)!.endpoint, acceptedAtMs: scenario.clock.now(),
    }), (error: unknown) => (error as { code?: string }).code === "InvalidMessageSource");
    assert.equal(store.inspectRequest(owner.workflowOwnerId, request.messageId)?.status, "open");
    assert.equal(store.inspectMessage(owner.workflowOwnerId, "wrong-timing")?.deliveryStatus, "bound");

    await ownerMessages.close();
    await responderMessages.close();
    await intruderMessages.close();
    ownerRuntime.confirmAgentRunExit(responderRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(intruderRun, { error: "test cleanup" });
    responderRuntime.close();
    intruderRuntime.close();
    ownerRuntime.close();
  });

  it("queues one direct Signal, projects one Inbox Batch, and wakes a waiting recipient", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const senderSession = scenario.childSession(ownerRuntime, "sender");
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const sender = ownerRuntime.addAgent({ session: senderSession, spawner: owner, name: "Sender" });
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const senderRuntime = scenario.startAgent(ownerRuntime.workflow, senderSession);
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const senderRun = ownerRuntime.startAgentRun(ownerRuntime.agent(sender.agentId));
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    const batches: InboxBatch[] = [];
    let wakeCount = 0;
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) {
        batches.push(batch);
        scenario.transcripts.appendInboxBatch(recipientSession, batch);
      },
      wakeRecipient() {
        wakeCount += 1;
        ownerRuntime.activateTurn(recipientRun);
      },
    });
    await recipientSignals.start();
    const senderSignals = directSignalRuntime(test, {
      controlPlane: senderRuntime.controlPlane,
      ownership: senderRun.ownership,
      allocateMessageId: () => scenario.identities.next(),
    });
    const sourceEntryId = scenario.transcripts.appendAgentSend(senderSession, {
      targetAgentId: recipient.agentId,
      message: "DIRECT_SIGNAL_PAYLOAD",
    });

    const receipt = await senderSignals.sendSignal({
      target: senderRuntime.agent(recipient.agentId),
      message: "DIRECT_SIGNAL_PAYLOAD",
      sourceEntryId,
    });
    await waitFor(() => batches.length === 1);
    recipientSignals.releaseDeferred();
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(batches.length, 1, "unconfirmed projection must not be injected twice in one Router run");
    assert.equal(recipientSignals.confirmDelivery(receipt.messageId), true);

    assert.equal(receipt.status, "queued");
    assert.equal(receipt.recipientAgentId, recipient.agentId);
    assert.equal(receipt.acceptanceSequence, 1);
    assert.deepEqual(batches, [{
      deliveryTiming: "steer",
      messages: [{
        kind: "signal",
        messageId: receipt.messageId,
        senderAgentId: sender.agentId,
        recipientAgentId: recipient.agentId,
        deliveryTiming: "steer",
        message: "DIRECT_SIGNAL_PAYLOAD",
      }],
    }]);
    assert.equal(wakeCount, 1);
    assert.equal(recipientRuntime.inspectActivation(recipientRuntime.agent(recipient.agentId))?.state.kind, "active");
    assert.equal(
      readFileSync(recipientSession.sessionPath, "utf8").split("DIRECT_SIGNAL_PAYLOAD").length - 1,
      1,
    );

    await senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(senderRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    senderRuntime.close();
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("accepts and resolves a productive cycle of Requests", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const firstSession = scenario.childSession(ownerRuntime, "first");
    const secondSession = scenario.childSession(ownerRuntime, "second");
    const first = ownerRuntime.addAgent({ session: firstSession, spawner: owner, name: "First" });
    const second = ownerRuntime.addAgent({ session: secondSession, spawner: owner, name: "Second" });
    const firstRuntime = scenario.startAgent(ownerRuntime.workflow, firstSession);
    const secondRuntime = scenario.startAgent(ownerRuntime.workflow, secondSession);
    const firstRun = ownerRuntime.startAgentRun(first);
    const secondRun = ownerRuntime.startAgentRun(second);
    const firstMessages = directSignalRuntime(test, {
      controlPlane: firstRuntime.controlPlane, ownership: firstRun.ownership, projectInboxBatch() {},
      wakeRecipient() { firstRuntime.activateTurn(firstRun); },
    });
    const secondMessages = directSignalRuntime(test, {
      controlPlane: secondRuntime.controlPlane, ownership: secondRun.ownership, projectInboxBatch() {},
      wakeRecipient() { secondRuntime.activateTurn(secondRun); },
    });
    await firstMessages.start();
    await secondMessages.start();
    const firstRequestSource = scenario.transcripts.appendAgentSend(firstSession, {
      targetAgentId: second.agentId, message: "first request", responseRequired: true,
    });
    const firstRequest = await firstMessages.sendMessage({
      target: { agentId: second.agentId }, message: "first request", sourceEntryId: firstRequestSource, responseRequired: true,
    });
    const secondRequestSource = scenario.transcripts.appendAgentSend(secondSession, {
      targetAgentId: first.agentId, message: "second request", responseRequired: true,
    });
    const secondRequest = await secondMessages.sendMessage({
      target: { agentId: first.agentId }, message: "second request", sourceEntryId: secondRequestSource, responseRequired: true,
    });
    firstRuntime.settleActivation(firstRun);
    secondRuntime.settleActivation(secondRun);
    assert.deepEqual(firstRuntime.inspectActivation(first)?.state, {
      kind: "waiting", dependencies: [{ kind: "agent", dependencyId: firstRequest.messageId, agentId: second.agentId }],
    });
    assert.deepEqual(secondRuntime.inspectActivation(second)?.state, {
      kind: "waiting", dependencies: [{ kind: "agent", dependencyId: secondRequest.messageId, agentId: first.agentId }],
    });

    const firstAnswerSource = scenario.transcripts.appendAgentSend(firstSession, {
      targetRequestId: secondRequest.messageId, message: "first answer",
    });
    const firstAnswer = await firstMessages.sendMessage({
      target: { requestId: secondRequest.messageId }, message: "first answer", sourceEntryId: firstAnswerSource,
    });
    const secondAnswerSource = scenario.transcripts.appendAgentSend(secondSession, {
      targetRequestId: firstRequest.messageId, message: "second answer",
    });
    const secondAnswer = await secondMessages.sendMessage({
      target: { requestId: firstRequest.messageId }, message: "second answer", sourceEntryId: secondAnswerSource,
    });
    await waitFor(() => firstMessages.inspectMessage(secondAnswer.messageId)?.deliveryStatus === "queued");
    await waitFor(() => secondMessages.inspectMessage(firstAnswer.messageId)?.deliveryStatus === "queued");
    assert.equal(firstMessages.confirmDelivery(secondAnswer.messageId), true);
    assert.equal(secondMessages.confirmDelivery(firstAnswer.messageId), true);
    assert.equal(firstMessages.inspectRequest(firstRequest.messageId)?.status, "resolved");
    assert.equal(secondMessages.inspectRequest(secondRequest.messageId)?.status, "resolved");
    assert.equal(firstRuntime.inspectActivation(first)?.state.kind, "active");
    assert.equal(secondRuntime.inspectActivation(second)?.state.kind, "active");

    await firstMessages.close();
    await secondMessages.close();
    ownerRuntime.confirmAgentRunExit(firstRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(secondRun, { error: "test cleanup" });
    firstRuntime.close();
    secondRuntime.close();
    ownerRuntime.close();
  });

  it("stores message identity and routing metadata without copying the Signal payload", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) {
        scenario.transcripts.appendInboxBatch(recipientSession, batch);
      },
      wakeRecipient() {
        ownerRuntime.activateTurn(recipientRun);
      },
    });
    await recipientSignals.start();
    const ownerSignals = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });
    const payload = "PAYLOAD_MUST_NOT_EXIST_IN_COORDINATION_STATE";
    const sourceEntryId = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: recipient.agentId, message: payload },
    );

    const receipt = await ownerSignals.sendSignal({
      target: ownerRuntime.agent(recipient.agentId),
      message: payload,
      sourceEntryId,
    });
    await waitFor(() => readFileSync(recipientSession.sessionPath, "utf8").includes(payload));
    assert.equal(recipientSignals.confirmDelivery(receipt.messageId), true);
    const record = ownerSignals.inspectMessage(receipt.messageId);

    assert.equal(record?.messageId, receipt.messageId);
    assert.equal(record?.senderAgentId, owner.agentId);
    assert.equal(record?.recipientAgentId, recipient.agentId);
    assert.equal(record?.sourceEntryId, sourceEntryId);
    assert.equal(record?.deliveryStatus, "delivered");
    assert.equal(JSON.stringify(record).includes(payload), false);
    for (const path of [
      ownerRuntime.workflow.databasePath,
      `${ownerRuntime.workflow.databasePath}-wal`,
    ]) {
      try {
        assert.equal(readFileSync(path).includes(Buffer.from(payload)), false);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }

    await ownerSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("rejects cross-Workflow targets without message, queue, transcript, or lifecycle effects", async (test) => {
    const identities = new DeterministicIdentityFactory();
    const workflowA = new WorkflowScenario({
      rootDirectory: await temporaryDirectory(),
      identityFactory: identities,
    });
    const workflowB = new WorkflowScenario({
      rootDirectory: await temporaryDirectory(),
      identityFactory: identities,
    });
    const { runtime: runtimeA } = workflowA.createOwner("Owner A");
    const { runtime: runtimeB } = workflowB.createOwner("Owner B");
    const recipientSession = workflowB.childSession(runtimeB, "recipient-b");
    const recipient = runtimeB.addAgent({
      session: recipientSession,
      spawner: runtimeB.agent(runtimeB.workflow.ownerAgentId),
      name: "Recipient B",
    });
    const beforeTranscript = readFileSync(recipientSession.sessionPath, "utf8");
    const senderSignals = directSignalRuntime(test, {
      controlPlane: runtimeA.controlPlane,
      allocateMessageId: () => identities.next(),
    });

    await assert.rejects(
      senderSignals.sendSignal({
        target: runtimeB.agent(recipient.agentId),
        message: "must not cross workflows",
        sourceEntryId: "cross-workflow-source",
      }),
      (error: unknown) => (error as { code?: string }).code === "WorkflowMismatch",
    );

    assert.deepEqual(senderSignals.listMessages(), []);
    assert.equal(readFileSync(recipientSession.sessionPath, "utf8"), beforeTranscript);
    assert.equal(runtimeB.inspectActivation(runtimeB.agent(recipient.agentId)), undefined);
    await senderSignals.close();
    runtimeA.close();
    runtimeB.close();
  });

  it("fails fast when the recipient Router is unavailable and removes the pre-IPC identity binding", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const recipientSession = scenario.childSession(runtime, "offline-recipient");
    const recipient = runtime.addAgent({
      session: recipientSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Offline Recipient",
    });
    const beforeTranscript = readFileSync(recipientSession.sessionPath, "utf8");
    let allocations = 0;
    const senderSignals = directSignalRuntime(test, {
      controlPlane: runtime.controlPlane,
      allocateMessageId() {
        allocations += 1;
        return scenario.identities.next();
      },
    });

    await assert.rejects(
      senderSignals.sendSignal({
        target: runtime.agent(recipient.agentId),
        message: "unavailable payload",
        sourceEntryId: "unavailable-source",
      }),
      (error: unknown) => (error as { code?: string }).code === "RecipientUnreachable",
    );

    assert.equal(allocations, 0);
    assert.deepEqual(senderSignals.listMessages(), []);
    assert.equal(readFileSync(recipientSession.sessionPath, "utf8"), beforeTranscript);
    assert.equal(runtime.inspectActivation(runtime.agent(recipient.agentId)), undefined);
    await senderSignals.close();
    runtime.close();
  });

  it("uses the ended-recipient preparation seam once before atomically accepting an authorized Request", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: owner, runtime } = scenario.createOwner();
    const recipientSession = scenario.childSession(runtime, "ended-recipient");
    const recipient = runtime.addAgent({
      session: recipientSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Ended Recipient",
    });
    const endedRun = runtime.controlPlane.acquireAgentRun(runtime.agent(recipient.agentId), scenario.identities.next());
    runtime.controlPlane.startActivation(endedRun);
    runtime.controlPlane.failAgentRun(endedRun, { error: "ended before Request" });
    const sourceEntryId = scenario.transcripts.appendAgentSend(owner, {
      targetAgentId: recipient.agentId, message: "Resume with this Request.", responseRequired: true,
    });
    let preparations = 0;
    const senderSignals = directSignalRuntime(test, {
      controlPlane: runtime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });
    const receipt = await senderSignals.sendMessage({
      target: { agentId: recipient.agentId }, message: "Resume with this Request.", sourceEntryId, responseRequired: true,
      async prepareEndedRecipient(request) {
        preparations += 1;
        const store = new DirectSignalStore(runtime.workflow.databasePath);
        try {
          return store.acceptEndedRecipientRequest({
            request,
            recipient: runtime.agent(recipient.agentId),
            endpoint: "prepared://ended-recipient",
            runId: scenario.identities.next(),
            checkpoint: JSON.stringify({ surface: "ended-recipient" }),
            acceptedAtMs: scenario.clock.now(),
          });
        } finally {
          store.close();
        }
      },
    });

    assert.equal(preparations, 1);
    assert.equal(receipt.status, "queued");
    assert.equal(runtime.inspectActivation(runtime.agent(recipient.agentId))?.sequence, 2);
    assert.equal(runtime.inspectActivation(runtime.agent(recipient.agentId))?.state.kind, "active");
    assert.equal(senderSignals.listPending(runtime.agent(recipient.agentId)).length, 1);

    const retry = await senderSignals.sendMessage({
      target: { agentId: recipient.agentId }, message: "Resume with this Request.", sourceEntryId, responseRequired: true,
      async prepareEndedRecipient() { throw new Error("same-source retry must not prepare again"); },
    });
    assert.deepEqual(retry, {
      status: receipt.status,
      messageId: receipt.messageId,
      recipientAgentId: receipt.recipientAgentId,
      acceptanceSequence: receipt.acceptanceSequence,
    });
    assert.equal(preparations, 1);
    await senderSignals.close();
    runtime.close();
  });

  it("reconciles a concurrent ended-Request preparation without letting the loser own the resumed run", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: ownerSession, runtime } = scenario.createOwner();
    const owner = runtime.agent(runtime.workflow.ownerAgentId);
    const childSession = scenario.childSession(runtime, "concurrent-ended-recipient");
    const child = runtime.addAgent({ session: childSession, spawner: owner, name: "Concurrent Recipient" });
    const oldRun = runtime.controlPlane.acquireAgentRun(runtime.agent(child.agentId), scenario.identities.next());
    runtime.controlPlane.startActivation(oldRun);
    runtime.controlPlane.failAgentRun(oldRun, { error: "ended before concurrent Request" });
    const sourceEntryId = scenario.transcripts.appendAgentSend(ownerSession, {
      targetAgentId: child.agentId, message: "Resume once.", responseRequired: true,
    });
    const request = {
      workflowOwnerId: runtime.workflow.ownerAgentId, messageId: scenario.identities.next(), senderAgentId: owner.agentId,
      recipientAgentId: child.agentId, sourceEntryId, payloadDigest: digestPayload("Resume once."),
      deliveryTiming: "steer" as const, responseRequired: true, message: "Resume once.",
    };
    const store = new DirectSignalStore(runtime.workflow.databasePath);
    try {
      const winner = store.acceptEndedRecipientRequest({
        request, recipient: runtime.agent(child.agentId), endpoint: "prepared://winner", runId: scenario.identities.next(),
        checkpoint: JSON.stringify({ surface: "winner" }), acceptedAtMs: scenario.clock.now(),
      });
      const loser = store.acceptEndedRecipientRequest({
        request, recipient: runtime.agent(child.agentId), endpoint: "prepared://loser", runId: scenario.identities.next(),
        checkpoint: JSON.stringify({ surface: "loser" }), acceptedAtMs: scenario.clock.now(),
      });
      assert.equal(winner.committedByThisPreparation, true);
      assert.equal(loser.committedByThisPreparation, false);
      assert.deepEqual(
        { status: loser.status, messageId: loser.messageId, recipientAgentId: loser.recipientAgentId, acceptanceSequence: loser.acceptanceSequence },
        { status: winner.status, messageId: winner.messageId, recipientAgentId: winner.recipientAgentId, acceptanceSequence: winner.acceptanceSequence },
      );
      assert.equal(loser.ownership.runId, winner.ownership.runId);
      assert.equal(store.reconcilePreparedRecipientRouter({ recipient: runtime.agent(child.agentId), endpoint: "prepared://winner" })?.runId, winner.ownership.runId);
      assert.equal(runtime.inspectActivation(runtime.agent(child.agentId))?.state.kind, "active");
      assert.equal(store.listPending(runtime.agent(child.agentId)).length, 1);
    } finally { store.close(); }
    runtime.close();
  });

  it("atomically reactivates an ended recipient for an Answer-plus-Request and preserves its reply slot", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "ended-answer-recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Ended Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const run = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    const ownerMessages = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane, allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
    });
    const recipientMessages = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane, ownership: run.ownership,
      allocateMessageId: () => scenario.identities.next(), projectInboxBatch() {},
    });
    await ownerMessages.start();
    await recipientMessages.start();
    const originalSource = scenario.transcripts.appendAgentSend(recipientSession, {
      targetAgentId: owner.agentId, message: "Please decide.", responseRequired: true,
    });
    const original = await recipientMessages.sendMessage({
      target: { agentId: owner.agentId }, message: "Please decide.", sourceEntryId: originalSource, responseRequired: true,
    });
    ownerRuntime.confirmAgentRunExit(run, { error: "recipient ended" });
    const answerSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetRequestId: original.messageId, message: "Decision and follow-up.", responseRequired: true,
    });
    const boundMessageId = scenario.identities.next();
    const boundStore = new DirectSignalStore(ownerRuntime.workflow.databasePath);
    try {
      boundStore.bindMessage({
        messageId: boundMessageId, sender: owner, recipient: ownerRuntime.agent(recipient.agentId),
        sourceEntryId: answerSource, payloadDigest: digestPayload("Decision and follow-up."),
        deliveryTiming: "steer", responseRequired: true, inReplyToRequestId: original.messageId, createdAtMs: scenario.clock.now(),
      });
    } finally { boundStore.close(); }
    let preparations = 0;
    const answer = await ownerMessages.sendMessage({
      target: { requestId: original.messageId }, message: "Decision and follow-up.", sourceEntryId: answerSource, responseRequired: true,
      async prepareEndedRecipient(request) {
        preparations += 1;
        const store = new DirectSignalStore(ownerRuntime.workflow.databasePath);
        try {
          return store.acceptEndedRecipientRequest({
            request, recipient: ownerRuntime.agent(recipient.agentId), endpoint: "prepared://ended-answer",
            runId: scenario.identities.next(), checkpoint: JSON.stringify({ surface: "ended-answer" }), acceptedAtMs: scenario.clock.now(),
          });
        } finally { store.close(); }
      },
    });
    const retry = await ownerMessages.sendMessage({
      target: { requestId: original.messageId }, message: "Decision and follow-up.", sourceEntryId: answerSource, responseRequired: true,
      async prepareEndedRecipient() { throw new Error("retry must reconcile durable acceptance"); },
    });

    assert.deepEqual(retry, {
      status: answer.status, messageId: answer.messageId, recipientAgentId: answer.recipientAgentId,
      acceptanceSequence: answer.acceptanceSequence,
    });
    assert.equal(preparations, 1);
    assert.equal(answer.messageId, boundMessageId);
    assert.equal(ownerMessages.inspectRequest(original.messageId)?.answerMessageId, answer.messageId);
    assert.equal(ownerMessages.inspectRequest(original.messageId)?.status, "answered");
    assert.deepEqual(ownerMessages.inspectRequest(answer.messageId), {
      requestId: answer.messageId, requesterAgentId: owner.agentId, responderAgentId: recipient.agentId,
      answerDeliveryTiming: "steer", status: "open",
    });
    assert.equal(ownerMessages.listPending(ownerRuntime.agent(recipient.agentId))[0]?.inReplyToRequestId, original.messageId);

    const staleSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetRequestId: original.messageId, message: "Late collision.", responseRequired: true,
    });
    await assert.rejects(ownerMessages.sendMessage({
      target: { requestId: original.messageId }, message: "Late collision.", sourceEntryId: staleSource, responseRequired: true,
    }), (error: unknown) => (error as { code?: string }).code === "AnswerAlreadyClosed");
    assert.equal(ownerMessages.listMessages().length, 2);
    await recipientMessages.close();
    await ownerMessages.close();
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("cleans up a listener and store when Router startup cannot acquire ownership", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const recipientSession = scenario.childSession(runtime, "recipient");
    const recipient = runtime.addAgent({
      session: recipientSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Recipient",
    });
    const router = closeAfter(test, new RecipientInboxRouter({
      workflowOwnerId: runtime.workflow.ownerAgentId,
      recipient: runtime.agent(recipient.agentId),
      databasePath: runtime.workflow.databasePath,
      projectInboxBatch() {},
      now: scenario.clock.now,
    }));

    await assert.rejects(router.start(), (error: unknown) => (error as { code?: string }).code === "OwnershipLost");
    await router.close();
    const reopened = closeAfter(test, new DirectSignalStore(runtime.workflow.databasePath));
    reopened.close();
    runtime.close();
  });

  it("removes the identity binding when the request cannot be written before acceptance", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({
      session: recipientSession,
      spawner: ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId),
      name: "Recipient",
    });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    let projected = 0;
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch() {
        projected += 1;
      },
    });
    await recipientSignals.start();
    const senderSignals = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });

    await assert.rejects(
      senderSignals.sendSignal({
        target: ownerRuntime.agent(recipient.agentId),
        message: "x".repeat(DEFAULT_MAXIMUM_FRAME_BYTES),
        sourceEntryId: "oversized-source",
      }),
      (error: unknown) => (error as { code?: string }).code === "RecipientUnreachable",
    );

    assert.equal(projected, 0);
    assert.deepEqual(senderSignals.listMessages(), []);
    assert.deepEqual(recipientSignals.listPending(recipientRuntime.agent(recipient.agentId)), []);
    await senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("removes a still-bound identity when the recipient disconnects before acceptance", async (test) => {
    const rootDirectory = await temporaryDirectory();
    const scenario = new WorkflowScenario({ rootDirectory });
    const { runtime } = scenario.createOwner();
    const recipientSession = scenario.childSession(runtime, "disconnecting-recipient");
    const recipient = runtime.addAgent({
      session: recipientSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Disconnecting Recipient",
    });
    const recipientRun = runtime.startAgentRun(runtime.agent(recipient.agentId));
    const endpoint = process.platform === "win32"
      ? `\\\\.\\pipe\\pi-herdr-disconnect-${process.pid}-${Date.now()}`
      : join(rootDirectory, "disconnect.sock");
    closeAfter(test, await listenForFramedIpc(endpoint, (connection) => {
      connection.onMessage(() => connection.end());
    }));
    const store = closeAfter(test, new DirectSignalStore(runtime.workflow.databasePath));
    store.registerRouter({
      recipient: runtime.agent(recipient.agentId),
      ownership: recipientRun.ownership,
      endpoint,
      registeredAtMs: scenario.clock.now(),
    });
    const senderSignals = directSignalRuntime(test, {
      controlPlane: runtime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });

    await assert.rejects(
      senderSignals.sendSignal({
        target: runtime.agent(recipient.agentId),
        message: "disconnect before acceptance",
        sourceEntryId: "disconnect-source",
      }),
      (error: unknown) => (error as { code?: string }).code === "RecipientUnreachable",
    );

    assert.deepEqual(senderSignals.listMessages(), []);
    await senderSignals.close();
    store.unregisterRouter(runtime.agent(recipient.agentId), endpoint);
    store.close();
    runtime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    runtime.close();
  });

  it("reconciles a lost acknowledgement through its original Message Identity", async (test) => {
    const rootDirectory = await temporaryDirectory();
    const scenario = new WorkflowScenario({ rootDirectory });
    const { runtime } = scenario.createOwner();
    const recipientSession = scenario.childSession(runtime, "recipient");
    const recipient = runtime.addAgent({
      session: recipientSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Recipient",
    });
    const recipientRun = runtime.startAgentRun(runtime.agent(recipient.agentId));
    const endpoint = process.platform === "win32"
      ? `\\\\.\\pipe\\pi-herdr-lost-ack-${process.pid}-${Date.now()}`
      : join(rootDirectory, "lost-ack.sock");
    const store = closeAfter(test, new DirectSignalStore(runtime.workflow.databasePath));
    store.registerRouter({
      recipient: runtime.agent(recipient.agentId),
      ownership: recipientRun.ownership,
      endpoint,
      registeredAtMs: scenario.clock.now(),
    });
    closeAfter(test, await listenForFramedIpc(endpoint, (connection) => {
      connection.onMessage((frame) => {
        store.acceptSignal({
          request: frame.payload as import("../../pi-extension/subagents/protocol/direct-signal-types.ts").SignalAcceptRequest,
          recipient: runtime.agent(recipient.agentId),
          ownership: recipientRun.ownership,
          endpoint,
          acceptedAtMs: scenario.clock.now(),
        });
        // Deliberately omit the receipt after the durable acceptance commit.
        connection.end();
      });
    }));
    let allocations = 0;
    const senderSignals = directSignalRuntime(test, {
      controlPlane: runtime.controlPlane,
      allocateMessageId() {
        allocations += 1;
        return scenario.identities.next();
      },
    });
    const sourceEntryId = scenario.transcripts.appendAgentSend(
      { agentId: runtime.workflow.ownerAgentId, sessionPath: runtime.workflow.ownerSessionPath },
      { targetAgentId: recipient.agentId, message: "lost acknowledgement" },
    );

    const first = await senderSignals.sendSignal({
      target: runtime.agent(recipient.agentId), message: "lost acknowledgement", sourceEntryId,
    });
    const second = await senderSignals.sendSignal({
      target: runtime.agent(recipient.agentId), message: "lost acknowledgement", sourceEntryId,
    });
    assert.deepEqual(second, first);
    assert.equal(allocations, 1);
    assert.deepEqual(senderSignals.listMessages().map((message) => ({
      messageId: message.messageId,
      deliveryStatus: message.deliveryStatus,
      acceptanceSequence: message.acceptanceSequence,
    })), [{ messageId: first.messageId, deliveryStatus: "queued", acceptanceSequence: 1 }]);

    await senderSignals.close();
    store.unregisterRouter(runtime.agent(recipient.agentId), endpoint);
    store.close();
    runtime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    runtime.close();
  });

  it("rolls back recipient acceptance atomically when pointer persistence fails", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({
      session: recipientSession,
      spawner: ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId),
      name: "Recipient",
    });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    let projected = 0;
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch() {
        projected += 1;
      },
      wakeRecipient() {
        assert.fail("failed acceptance must not wake the recipient");
      },
    });
    await recipientSignals.start();
    const database = closeAfter(test, new DatabaseSync(ownerRuntime.workflow.databasePath));
    database.exec(`
      CREATE TRIGGER reject_signal_pointer
      BEFORE INSERT ON pending_message_pointers
      BEGIN
        SELECT RAISE(ABORT, 'forced pointer failure');
      END
    `);
    const senderSignals = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });
    const sourceEntryId = scenario.transcripts.appendAgentSend(
      { agentId: ownerRuntime.workflow.ownerAgentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: recipient.agentId, message: "atomic payload", responseRequired: true },
    );

    await assert.rejects(
      senderSignals.sendMessage({
        target: { agentId: recipient.agentId },
        message: "atomic payload",
        sourceEntryId,
        responseRequired: true,
      }),
      /forced pointer failure/,
    );

    assert.equal(projected, 0);
    assert.deepEqual(senderSignals.listMessages(), []);
    assert.deepEqual(recipientSignals.listPending(recipientRuntime.agent(recipient.agentId)), []);
    assert.deepEqual(recipientRuntime.inspectActivation(recipientRuntime.agent(recipient.agentId))?.state, {
      kind: "waiting",
      dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    database.exec("DROP TRIGGER reject_signal_pointer");
    await senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("batches deferred Signals in acceptance order when the recipient settles", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    const batches: InboxBatch[] = [];
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) { batches.push(batch); },
    });
    await recipientSignals.start();
    const ownerSignals = directSignalRuntime(test, {
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });
    const ownerSession = { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath };
    const firstSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetAgentId: recipient.agentId, message: "first deferred", timing: "deferred",
    });
    const secondSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetAgentId: recipient.agentId, message: "second deferred", timing: "deferred",
    });
    const first = await ownerSignals.sendSignal({
      target: ownerRuntime.agent(recipient.agentId), message: "first deferred", sourceEntryId: firstSource, deliveryTiming: "deferred",
    });
    const second = await ownerSignals.sendSignal({
      target: ownerRuntime.agent(recipient.agentId), message: "second deferred", sourceEntryId: secondSource, deliveryTiming: "deferred",
    });
    assert.equal(batches.length, 0);
    assert.deepEqual([first.acceptanceSequence, second.acceptanceSequence], [1, 2]);

    ownerRuntime.settleActivation(recipientRun);
    recipientSignals.releaseDeferred();
    await waitFor(() => batches.length === 1);
    assert.equal(batches[0].deliveryTiming, "deferred");
    assert.deepEqual(batches[0].messages.map((message) => message.messageId), [first.messageId, second.messageId]);
    assert.deepEqual(batches[0].messages.map((message) => message.message), ["first deferred", "second deferred"]);
    for (const message of batches[0].messages) assert.equal(recipientSignals.confirmDelivery(message.messageId), true);
    assert.deepEqual(recipientSignals.listPending(recipientRuntime.agent(recipient.agentId)), []);

    await ownerSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("lets active Steer delivery overtake earlier Deferred work", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    const batches: InboxBatch[] = [];
    const recipientSignals = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) { batches.push(batch); },
    });
    await recipientSignals.start();
    const ownerSignals = directSignalRuntime(test, { controlPlane: ownerRuntime.controlPlane, allocateMessageId: () => scenario.identities.next() });
    const ownerSession = { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath };
    const deferredSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetAgentId: recipient.agentId, message: "deferred", timing: "deferred",
    });
    const deferred = await ownerSignals.sendSignal({
      target: ownerRuntime.agent(recipient.agentId), message: "deferred", sourceEntryId: deferredSource, deliveryTiming: "deferred",
    });
    const steerSource = scenario.transcripts.appendAgentSend(ownerSession, {
      targetAgentId: recipient.agentId, message: "steer", timing: "steer",
    });
    const steer = await ownerSignals.sendSignal({
      target: ownerRuntime.agent(recipient.agentId), message: "steer", sourceEntryId: steerSource,
    });
    await waitFor(() => batches.length === 1);
    assert.deepEqual(batches[0].messages.map((message) => message.messageId), [steer.messageId]);
    assert.equal(recipientSignals.confirmDelivery(steer.messageId), true);
    ownerRuntime.settleActivation(recipientRun);
    recipientSignals.releaseDeferred();
    await waitFor(() => batches.length === 2);
    assert.deepEqual(batches[1].messages.map((message) => message.messageId), [deferred.messageId]);
    assert.equal(recipientSignals.confirmDelivery(deferred.messageId), true);

    await ownerSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("reconciles transcript evidence after projection commits before delivery confirmation", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    const firstRouter = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) { scenario.transcripts.appendInboxBatch(recipientSession, batch); },
    });
    await firstRouter.start();
    const ownerSignals = directSignalRuntime(test, { controlPlane: ownerRuntime.controlPlane, allocateMessageId: () => scenario.identities.next() });
    const sourceEntryId = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: recipient.agentId, message: "durable once" },
    );
    const receipt = await ownerSignals.sendSignal({ target: ownerRuntime.agent(recipient.agentId), message: "durable once", sourceEntryId });
    await waitFor(() => readFileSync(recipientSession.sessionPath, "utf8").includes(receipt.messageId));
    await firstRouter.close();
    let replayed = 0;
    const recoveredRouter = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch() { replayed += 1; },
      hasProjectedMessage(messageId) { return readFileSync(recipientSession.sessionPath, "utf8").includes(messageId); },
    });
    await recoveredRouter.start();
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(replayed, 0);
    assert.equal(recoveredRouter.inspectMessage(receipt.messageId)?.deliveryStatus, "delivered");
    assert.deepEqual(recoveredRouter.listPending(recipientRuntime.agent(recipient.agentId)), []);

    await ownerSignals.close();
    await recoveredRouter.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("retains a queued pointer when projection fails and recovers it after restart", async (test) => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    const failingRouter = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch() { throw new Error("projection crash"); },
    });
    await failingRouter.start();
    const ownerSignals = directSignalRuntime(test, { controlPlane: ownerRuntime.controlPlane, allocateMessageId: () => scenario.identities.next() });
    const sourceEntryId = scenario.transcripts.appendAgentSend(
      { agentId: owner.agentId, sessionPath: ownerRuntime.workflow.ownerSessionPath },
      { targetAgentId: recipient.agentId, message: "recover me" },
    );
    const receipt = await ownerSignals.sendSignal({ target: ownerRuntime.agent(recipient.agentId), message: "recover me", sourceEntryId });
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(failingRouter.inspectMessage(receipt.messageId)?.deliveryStatus, "queued");
    await failingRouter.close();
    const recovered: InboxBatch[] = [];
    const recoveredRouter = directSignalRuntime(test, {
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch(batch) { recovered.push(batch); },
    });
    await recoveredRouter.start();
    await waitFor(() => recovered.length === 1);
    assert.deepEqual(recovered[0].messages.map((message) => message.messageId), [receipt.messageId]);
    assert.equal(recoveredRouter.confirmDelivery(receipt.messageId), true);

    await ownerSignals.close();
    await recoveredRouter.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

});
