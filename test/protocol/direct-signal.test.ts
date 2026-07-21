import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, it } from "node:test";
import {
  DEFAULT_MAXIMUM_FRAME_BYTES,
  listenForFramedIpc,
} from "../../pi-extension/subagents/coordination/framed-ipc.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import {
  DirectSignalRuntime,
  type InboxBatch,
} from "../../pi-extension/subagents/protocol/direct-signal.ts";
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

describe("direct Signal protocol scenarios", () => {
  it("queues one direct Signal, projects one Inbox Batch, and wakes a waiting recipient", async () => {
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
    const recipientSignals = new DirectSignalRuntime({
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
    const senderSignals = new DirectSignalRuntime({
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
    assert.equal(recipientSignals.confirmDelivery(receipt.messageId), true);

    assert.equal(receipt.status, "queued");
    assert.equal(receipt.recipientAgentId, recipient.agentId);
    assert.equal(receipt.acceptanceSequence, 1);
    assert.deepEqual(batches, [{
      messages: [{
        kind: "signal",
        messageId: receipt.messageId,
        senderAgentId: sender.agentId,
        recipientAgentId: recipient.agentId,
        message: "DIRECT_SIGNAL_PAYLOAD",
      }],
    }]);
    assert.equal(wakeCount, 1);
    assert.equal(recipientRuntime.inspectActivation(recipientRuntime.agent(recipient.agentId))?.state.kind, "active");
    assert.equal(
      readFileSync(recipientSession.sessionPath, "utf8").split("DIRECT_SIGNAL_PAYLOAD").length - 1,
      1,
    );

    senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(senderRun, { error: "test cleanup" });
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    senderRuntime.close();
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("stores message identity and routing metadata without copying the Signal payload", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime: ownerRuntime } = scenario.createOwner();
    const owner = ownerRuntime.agent(ownerRuntime.workflow.ownerAgentId);
    const recipientSession = scenario.childSession(ownerRuntime, "recipient");
    const recipient = ownerRuntime.addAgent({ session: recipientSession, spawner: owner, name: "Recipient" });
    const recipientRuntime = scenario.startAgent(ownerRuntime.workflow, recipientSession);
    const recipientRun = ownerRuntime.startAgentRun(ownerRuntime.agent(recipient.agentId));
    ownerRuntime.settleActivation(recipientRun);
    const recipientSignals = new DirectSignalRuntime({
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
    const ownerSignals = new DirectSignalRuntime({
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

    ownerSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("rejects cross-Workflow targets without message, queue, transcript, or lifecycle effects", async () => {
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
    const senderSignals = new DirectSignalRuntime({
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
    senderSignals.close();
    runtimeA.close();
    runtimeB.close();
  });

  it("fails fast when the recipient Router is unavailable and removes the pre-IPC identity binding", async () => {
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
    const senderSignals = new DirectSignalRuntime({
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
    senderSignals.close();
    runtime.close();
  });

  it("removes the identity binding when the request cannot be written before acceptance", async () => {
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
    const recipientSignals = new DirectSignalRuntime({
      controlPlane: recipientRuntime.controlPlane,
      ownership: recipientRun.ownership,
      projectInboxBatch() {
        projected += 1;
      },
    });
    await recipientSignals.start();
    const senderSignals = new DirectSignalRuntime({
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
    senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });

  it("removes a still-bound identity when the recipient disconnects before acceptance", async () => {
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
    const fakeRouter = await listenForFramedIpc(endpoint, (connection) => {
      connection.onMessage(() => connection.end());
    });
    const store = new DirectSignalStore(runtime.workflow.databasePath);
    store.registerRouter({
      recipient: runtime.agent(recipient.agentId),
      ownership: recipientRun.ownership,
      endpoint,
      registeredAtMs: scenario.clock.now(),
    });
    const senderSignals = new DirectSignalRuntime({
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
    senderSignals.close();
    store.unregisterRouter(runtime.agent(recipient.agentId), endpoint);
    store.close();
    await fakeRouter.close();
    runtime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    runtime.close();
  });

  it("rolls back recipient acceptance atomically when pointer persistence fails", async () => {
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
    const recipientSignals = new DirectSignalRuntime({
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
    const database = new DatabaseSync(ownerRuntime.workflow.databasePath);
    database.exec(`
      CREATE TRIGGER reject_signal_pointer
      BEFORE INSERT ON pending_message_pointers
      BEGIN
        SELECT RAISE(ABORT, 'forced pointer failure');
      END
    `);
    const senderSignals = new DirectSignalRuntime({
      controlPlane: ownerRuntime.controlPlane,
      allocateMessageId: () => scenario.identities.next(),
    });

    await assert.rejects(
      senderSignals.sendSignal({
        target: ownerRuntime.agent(recipient.agentId),
        message: "atomic payload",
        sourceEntryId: "atomic-source",
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
    database.close();
    senderSignals.close();
    await recipientSignals.close();
    ownerRuntime.confirmAgentRunExit(recipientRun, { error: "test cleanup" });
    recipientRuntime.close();
    ownerRuntime.close();
  });
});
