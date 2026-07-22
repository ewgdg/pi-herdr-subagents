import assert from "node:assert/strict";
import { appendFileSync, copyFileSync, existsSync, symlinkSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, it } from "node:test";
import {
  hasConfirmedAgentRunTermination,
  superviseLegacyAgentRun,
} from "../../pi-extension/subagents/legacy-agent-run.ts";
import {
  WorkflowBootstrap,
  WORKFLOW_AGENT_SESSION_ID_ENV,
  WORKFLOW_AGENT_ROLE_ENV,
  WORKFLOW_OWNER_SESSION_ID_ENV,
  WORKFLOW_OWNER_SESSION_PATH_ENV,
  type WorkflowBootstrapContext,
} from "../../pi-extension/subagents/protocol/workflow-bootstrap.ts";
import { PROVISIONAL_AGENT_RUN_KIND_ENV, PROVISIONAL_SPAWN_ENDPOINT_ENV, ProvisionalSpawnGate } from "../../pi-extension/subagents/protocol/provisional-spawn.ts";
import { HumanInterruptInputBridge, registerAgentAskUserTool } from "../../pi-extension/subagents/protocol/human-interrupt-extension.ts";
import { initializeSubagentSessionFile } from "../../pi-extension/subagents/session.ts";
import { bindNewWorkflowSession } from "../../pi-extension/subagents/protocol/workflow-session-binding.ts";
import { DeterministicIdentityFactory, ManualClock } from "./scenario-harness.ts";
import {
  ActivationCancellationStore,
  CancellationInDoubtError,
} from "../../pi-extension/subagents/protocol/activation-cancellation.ts";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-bootstrap-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })),
  );
});

function context(sessionId: string, sessionPath: string): WorkflowBootstrapContext {
  return {
    sessionManager: {
      getSessionId: () => sessionId,
      getSessionFile: () => sessionPath,
    },
  };
}

describe("production Workflow bootstrap", () => {
  it("resumes an in-doubt activation cancellation from a later public tool call", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const observations = ["present", "unavailable", "missing"] as const;
    let observation = 0;
    const owner = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect() { return { kind: observations[observation++] }; },
        async close() {},
      },
    });
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "retry-cancellation-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const prepared = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "retry-cancellation-run",
      surface: "retry-cancellation-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(prepared.ownership);

    let firstOperationId = "";
    await assert.rejects(owner.cancelActivation(childId, "first-tool-call"), (error: unknown) => {
      assert.ok(error instanceof CancellationInDoubtError);
      firstOperationId = error.operation.operationId;
      return true;
    });
    const result = await owner.cancelActivation(childId, "later-tool-call");
    assert.equal(result.state, "committed");
    assert.equal(result.operationId, firstOperationId);
    assert.equal(result.sourceId, "first-tool-call");
    owner.close();
  });

  it("does not let target shutdown reclassify a cancellation-owned run as failed", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "cancellation-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const prepared = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "cancellation-run",
      surface: "cancellation-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(prepared.ownership);
    const child = new WorkflowBootstrap();
    child.sessionStarted(context(childId, childPath), prepared.environment);
    await child.startDirectSignalRouter({ projectInboxBatch() {} });
    const cancellation = new ActivationCancellationStore(owner.workflow!.databasePath);
    const claim = cancellation.claim({
      actor: { workflowOwnerId: ownerId, agentId: ownerId },
      target: { workflowOwnerId: ownerId, agentId: childId },
      sourceId: "shutdown-race-source",
      operationId: "shutdown-race-operation",
      now: 1,
    });

    await child.closeDirectSignalRouter();
    const database = new DatabaseSync(owner.workflow!.databasePath, { readOnly: true });
    assert.ok(database.prepare("SELECT 1 FROM recipient_inbox_routers WHERE agent_id = ?").get(childId));
    database.close();
    child.close();
    assert.equal(owner.inspectActivation(childId)?.state.kind, "active");
    assert.equal(owner.isCancellationOwnedRun(prepared.ownership), true);
    cancellation.markReady(claim.operationId, 2);
    assert.equal(cancellation.finalize(claim.operationId, 3).state, "committed");
    cancellation.close();
    owner.close();
  });

  it("starts recovery activation and transfers operation dependencies before launcher acknowledgement", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "first-run",
      surface: "first-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO activation_dependencies
      (activation_id, dependency_kind, dependency_id, dependency_agent_id, created_at_ms)
      VALUES (?, 'operation', 'acceptance:recovery-message', NULL, ?)`
    ).run(first.ownership.runId, Date.now());
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const recovery = await owner.prepareResume({ sessionPath: childPath, runId: "recovery-run", surface: "recovery-surface" });

    const child = new WorkflowBootstrap();
    try {
      child.sessionStarted(context(childId, childPath), recovery.environment);
      assert.deepEqual(child.inspectActivation(childId)?.state, {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "acceptance:recovery-message" }],
      });
      assert.equal(owner.runStarted(recovery.ownership).runId, recovery.ownership.runId);
    } finally {
      child.close();
      owner.runTerminated(recovery.ownership, true, { error: "test cleanup" });
      owner.close();
    }
  });

  it("cleans up an uncommitted provisional Router after a startup disconnect", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const gate = await ProvisionalSpawnGate.create();
    const child = new WorkflowBootstrap();
    try {
      child.sessionStarted(context(childId, childPath), {
        [WORKFLOW_OWNER_SESSION_ID_ENV]: ownerId, [WORKFLOW_OWNER_SESSION_PATH_ENV]: ownerPath,
        [WORKFLOW_AGENT_SESSION_ID_ENV]: childId, [PROVISIONAL_SPAWN_ENDPOINT_ENV]: gate.endpoint,
        [PROVISIONAL_AGENT_RUN_KIND_ENV]: "resume",
      });
      await gate.waitUntilReady();
      await gate.close();
      await new Promise((resolve) => setTimeout(resolve, 10));
      assert.equal(child.workflow, undefined);
    } finally {
      child.close();
      owner.close();
      await gate.close();
    }
  });

  it("adopts an exact committed provisional Router after RELEASE disconnect", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const gate = await ProvisionalSpawnGate.create();
    const child = new WorkflowBootstrap();
    try {
      child.sessionStarted(context(childId, childPath), {
        [WORKFLOW_OWNER_SESSION_ID_ENV]: ownerId, [WORKFLOW_OWNER_SESSION_PATH_ENV]: ownerPath,
        [WORKFLOW_AGENT_SESSION_ID_ENV]: childId, [PROVISIONAL_SPAWN_ENDPOINT_ENV]: gate.endpoint,
        [PROVISIONAL_AGENT_RUN_KIND_ENV]: "resume",
      });
      const routerStarted = child.startDirectSignalRouter({ projectInboxBatch() {} });
      const ready = await gate.waitUntilReady();
      appendFileSync(ownerPath, `${JSON.stringify({ message: { content: [{ type: "toolCall", id: "tool-call", name: "agent_send", arguments: { target: { spawn: { agent: "worker", name: "Child" } }, message: "Recover after disconnect.", responseRequired: true } }] } })}\n`);
      const receipt = owner.spawnInitialRequest({
        agentId: childId, sessionPath: childPath, runId: "committed-run", messageId: "committed-message",
        sourceEntryId: "tool-call", message: "Recover after disconnect.", name: "Child", agentDefinition: "worker",
        sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
        routerEndpoint: ready.routerEndpoint,
      });
      await gate.close();
      await routerStarted;
      assert.equal(child.workflow?.ownerAgentId, ownerId);
      assert.equal(child.currentTurnStarted()?.state.kind, "active");
      assert.equal(owner.inspect(childId).agentId, receipt.childAgentId);
    } finally {
      child.close();
      owner.close();
      await gate.close();
    }
  });

  it("projects a canonical Spawned Initial Request through PROJECT before COMMIT and RELEASE", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner-project.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "project-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const gate = await ProvisionalSpawnGate.create();
    const child = new WorkflowBootstrap();
    const projected: unknown[] = [];
    try {
      child.sessionStarted(context(childId, childPath), {
        [WORKFLOW_OWNER_SESSION_ID_ENV]: ownerId, [WORKFLOW_OWNER_SESSION_PATH_ENV]: ownerPath,
        [WORKFLOW_AGENT_SESSION_ID_ENV]: childId, [PROVISIONAL_SPAWN_ENDPOINT_ENV]: gate.endpoint,
      });
      const routerStarted = child.startDirectSignalRouter({ projectInboxBatch() {}, async projectInitialInboxBatch(batch) { projected.push(batch); } });
      const ready = await gate.waitUntilReady();
      const message = "Canonical spawn work.";
      appendFileSync(ownerPath, `${JSON.stringify({ message: { content: [{ type: "toolCall", id: "spawn-source", name: "agent_send", arguments: { target: { spawn: { agent: "worker", name: "Child" } }, message, responseRequired: true } }] } })}\n`);
      const messageId = "spawn-message";
      const { digestPayload } = await import("../../pi-extension/subagents/protocol/direct-signal-transcript.ts");
      await gate.project({ senderSessionPath: ownerPath, messageId, sourceEntryId: "spawn-source", senderAgentId: ownerId, recipientAgentId: childId, payloadDigest: digestPayload(message), agentDefinition: "worker", agentName: "Child" });
      const receipt = owner.spawnInitialRequest({
        agentId: childId, sessionPath: childPath, runId: "spawn-run", messageId, sourceEntryId: "spawn-source", message,
        name: "Child", agentDefinition: "worker", sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }), routerEndpoint: ready.routerEndpoint,
      });
      await gate.release({ runId: receipt.runId, fencingEpoch: receipt.fencingEpoch });
      await routerStarted;
      assert.equal(receipt.status, "delivered");
      assert.equal((projected[0] as { messages: Array<{ message: string }> }).messages[0]?.message, message);
    } finally {
      child.close();
      owner.close();
      await gate.close();
    }
  });

  it("leaves ephemeral Pi sessions unbound without retrying forever", async () => {
    const bootstrap = new WorkflowBootstrap();
    const ephemeralContext = {
      sessionManager: {
        getSessionId: () => "00000000-0000-4000-8000-000000000001",
        getSessionFile: () => null,
      },
    };
    bootstrap.sessionStarted(ephemeralContext);

    assert.equal(bootstrap.workflow, undefined);
    await assert.rejects(
      bootstrap.waitUntilReady(ephemeralContext),
      /requires a persistent Pi session file/,
    );
    bootstrap.close();
  });

  it("retries Owner bootstrap after Pi creates the persistent transcript", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "delayed-owner.jsonl");
    const bootstrap = new WorkflowBootstrap();

    bootstrap.sessionStarted(context(ownerId, ownerPath));
    assert.equal(bootstrap.workflow, undefined);

    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    await new Promise((resolve) => setTimeout(resolve, 75));

    assert.equal(bootstrap.workflow?.ownerAgentId, ownerId);
    bootstrap.close();
  });

  it("waits for delayed session persistence before reporting readiness", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "delayed-owner.jsonl");
    const bootstrap = new WorkflowBootstrap();
    const ready = bootstrap.waitUntilReady(context(ownerId, ownerPath));

    await new Promise((resolve) => setTimeout(resolve, 30));
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    await ready;

    assert.equal(bootstrap.workflow?.ownerAgentId, ownerId);
    bootstrap.close();
  });

  it("rolls back membership and session artifacts for an abandoned spawn", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const bootstrap = new WorkflowBootstrap();
    bootstrap.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(bootstrap.workflow!.sessionsDirectory, "abandoned.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });
    const prepared = bootstrap.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      name: "Abandoned",
      runId: "abandoned-run",
      surface: "abandoned-surface",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });

    bootstrap.abandonPreparedRun(prepared);

    assert.equal(existsSync(childPath), false);
    assert.equal(existsSync(`${childPath}.workflow.json`), false);
    assert.throws(() => bootstrap.inspect(childId), (error: unknown) =>
      (error as { code?: string }).code === "UnknownAgent");
    bootstrap.close();
  });

  it("cleans session artifacts when a direct Spawner rejects a child", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));

    const parentId = identities.next();
    const parentPath = join(owner.workflow!.sessionsDirectory, "parent.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: parentPath, childCwd: root, childSessionId: parentId });
    const parentRun = owner.prepareSpawn({
      agentId: parentId,
      sessionPath: parentPath,
      runId: "parent-run",
      name: "Parent",
      capabilities: { spawning: false },
      surface: "parent-surface",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: parentId, sessionPath: parentPath }),
    });
    const parent = new WorkflowBootstrap();
    parent.sessionStarted(context(parentId, parentPath), parentRun.environment);

    const childId = identities.next();
    const childPath = join(parent.workflow!.sessionsDirectory, "rejected.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const binding = bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath });
    assert.throws(
      () => parent.prepareSpawn({ agentId: childId, sessionPath: childPath, name: "Rejected", runId: "rejected-run", surface: "rejected-surface", sessionBinding: binding }),
      (error: unknown) => (error as { code?: string }).code === "SpawnerCapabilityRequired",
    );
    assert.equal(existsSync(childPath), false);
    assert.equal(existsSync(`${childPath}.workflow.json`), false);

    parent.close();
    owner.runTerminated(parentRun.ownership, true);
    owner.close();
  });

  it("opens durable Owner state and prepares fenced direct-child runs", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const clock = new ManualClock();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
      timestamp: new Date(clock.now()).toISOString(),
    });
    const bootstrap = new WorkflowBootstrap({ now: clock.now });
    bootstrap.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(bootstrap.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
      timestamp: new Date(clock.now()).toISOString(),
    });

    const prepared = bootstrap.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "run-one",
      name: "Child",
      agentDefinition: "worker",
      capabilities: { spawning: false },
      surface: "child-surface",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });

    assert.equal(bootstrap.inspect(childId).spawnerAgentId, ownerId);
    assert.deepEqual(bootstrap.inspect(childId).capabilities, { spawning: false });
    assert.equal(prepared.environment.PI_WORKFLOW_OWNER_SESSION_ID, ownerId);
    assert.equal(prepared.environment.PI_WORKFLOW_OWNER_SESSION_PATH, ownerPath);
    assert.equal(prepared.environment.PI_WORKFLOW_RUN_ID, "run-one");

    const competing = new WorkflowBootstrap({ now: clock.now });
    competing.sessionStarted(context(ownerId, ownerPath));
    const copiedSessionPath = join(root, "copied-child.jsonl");
    copyFileSync(childPath, copiedSessionPath);
    await assert.rejects(
      competing.prepareResume({
        sessionPath: copiedSessionPath,
        runId: "copied-run",
        surface: "copied-surface",
      }),
      (error: unknown) => (error as { code?: string }).code === "InvalidSessionIdentity",
    );
    await assert.rejects(
      competing.prepareResume({
        sessionPath: childPath,
        runId: "run-two",
        surface: "run-two-surface",
      }),
      (error: unknown) => (error as { code?: string }).code === "AgentRunAlreadyOwned",
    );

    bootstrap.runTerminated(prepared.ownership, false);
    await assert.rejects(
      competing.prepareResume({
        sessionPath: childPath,
        runId: "still-blocked",
        surface: "blocked-surface",
      }),
      (error: unknown) => (error as { code?: string }).code === "AgentRunAlreadyOwned",
    );
    bootstrap.runTerminated(prepared.ownership, true);
    const symlinkedSessionPath = join(root, "linked-child.jsonl");
    symlinkSync(childPath, symlinkedSessionPath);
    const replacement = await competing.prepareResume({
      sessionPath: symlinkedSessionPath,
      runId: "run-three",
      surface: "run-three-surface",
    });
    assert.equal(replacement.sessionPath, childPath);
    assert.ok(replacement.ownership.epoch > prepared.ownership.epoch);
    competing.runTerminated(replacement.ownership, true);
    competing.close();
    bootstrap.close();
  });

  it("derives Human Interrupt role from durable membership across launch and bootstrap", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));

    const moderatorId = identities.next();
    const moderatorPath = join(owner.workflow!.sessionsDirectory, "moderator.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: moderatorPath, childCwd: root, childSessionId: moderatorId });
    const preparedModerator = owner.prepareSpawn({
      agentId: moderatorId, sessionPath: moderatorPath, runId: "moderator-run", surface: "moderator-surface",
      name: "Moderator", agentDefinition: "moderator",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: moderatorId, sessionPath: moderatorPath }),
    });
    assert.equal(preparedModerator.environment[WORKFLOW_AGENT_ROLE_ENV], "moderator");
    owner.runStarted(preparedModerator.ownership);

    const moderator = new WorkflowBootstrap();
    moderator.sessionStarted(context(moderatorId, moderatorPath), {
      ...preparedModerator.environment,
      [WORKFLOW_AGENT_ROLE_ENV]: "ordinary",
    });
    assert.equal(moderator.humanInterruptActorRole, "moderator");
    let moderatorAskUserRegistered = false;
    registerAgentAskUserTool(
      { registerTool() { moderatorAskUserRegistered = true; } } as never,
      moderator,
      new HumanInterruptInputBridge(),
      true,
      moderator.humanInterruptActorRole,
    );
    assert.equal(moderatorAskUserRegistered, false);
    assert.throws(() => moderator.beginHumanInterrupt("ask-moderator"), (error: unknown) =>
      (error as { code?: string }).code === "HumanInterruptForbidden");
    moderator.close();

    const workerId = identities.next();
    const workerPath = join(owner.workflow!.sessionsDirectory, "worker.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: workerPath, childCwd: root, childSessionId: workerId });
    const preparedWorker = owner.prepareSpawn({
      agentId: workerId, sessionPath: workerPath, runId: "worker-run", surface: "worker-surface",
      name: "Worker", agentDefinition: "worker",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: workerId, sessionPath: workerPath }),
    });
    assert.equal(preparedWorker.environment[WORKFLOW_AGENT_ROLE_ENV], "ordinary");
    owner.runStarted(preparedWorker.ownership);

    const worker = new WorkflowBootstrap();
    worker.sessionStarted(context(workerId, workerPath), {
      ...preparedWorker.environment,
      [WORKFLOW_AGENT_ROLE_ENV]: "moderator",
    });
    assert.equal(worker.humanInterruptActorRole, "ordinary");
    let workerAskUserRegistered = false;
    registerAgentAskUserTool(
      { registerTool() { workerAskUserRegistered = true; } } as never,
      worker,
      new HumanInterruptInputBridge(),
      true,
      worker.humanInterruptActorRole,
    );
    assert.equal(workerAskUserRegistered, true);
    assert.equal(worker.beginHumanInterrupt("ask-worker").status, "pending");

    worker.close();
    owner.close();
  });

  it("opens a spawned member session as the direct Spawner for nested work", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const ownerBootstrap = new WorkflowBootstrap();
    ownerBootstrap.sessionStarted(context(ownerId, ownerPath));
    const parentId = identities.next();
    const parentPath = join(ownerBootstrap.workflow!.sessionsDirectory, "parent.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: parentPath,
      childCwd: root,
      childSessionId: parentId,
    });
    const parentRun = ownerBootstrap.prepareSpawn({
      agentId: parentId,
      sessionPath: parentPath,
      runId: "parent-run",
      name: "Parent",
      capabilities: { spawning: true },
      surface: "parent-surface",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: parentId,
        sessionPath: parentPath,
      }),
    });
    const parentBootstrap = new WorkflowBootstrap();
    parentBootstrap.sessionStarted(context(parentId, parentPath), parentRun.environment);
    const childId = identities.next();
    const childPath = join(parentBootstrap.workflow!.sessionsDirectory, "nested.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });

    const nestedRun = parentBootstrap.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "nested-run",
      name: "Nested",
      surface: "nested-surface",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });

    assert.equal(ownerBootstrap.inspect(childId).spawnerAgentId, parentId);
    parentBootstrap.runTerminated(nestedRun.ownership, true);
    parentBootstrap.close();
    ownerBootstrap.runTerminated(parentRun.ownership, true);

    const resumedParent = new WorkflowBootstrap();
    resumedParent.sessionStarted(context(parentId, parentPath));
    assert.equal(resumedParent.currentAgentId, parentId);
    assert.equal(resumedParent.workflow?.ownerAgentId, ownerId);
    assert.equal(resumedParent.inspect(childId).spawnerAgentId, parentId);
    resumedParent.close();
    ownerBootstrap.close();
  });

  it("projects durable DECIDE attention through the production bootstrap query", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const prepared = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "child-run", surface: "child-surface", name: "Child",
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(prepared.ownership);
    const child = new WorkflowBootstrap();
    child.sessionStarted(context(childId, childPath), prepared.environment);
    child.beginHumanInterrupt("ask-1");
    assert.equal(child.hasHumanAttention(), true);
    child.bindHumanResponse("ask-1", "input-1");
    assert.equal(child.hasHumanAttention(), false);
    child.prepareHumanResponseResult("ask-1");
    child.confirmHumanResponseResult("ask-1");
    child.close();
    owner.runTerminated(prepared.ownership, true);
    owner.close();
  });

  it("binds Pi turn events to the durable activation lifecycle", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    assert.equal(owner.currentTurnStarted(), undefined);
    assert.deepEqual(owner.currentTurnSettled(false), { kind: "owner-turn-settled" });

    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });
    const prepared = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "child-run",
      surface: "child-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });
    owner.runStarted(prepared.ownership);

    const child = new WorkflowBootstrap();
    child.sessionStarted(context(childId, childPath), prepared.environment);
    assert.equal(child.currentTurnStarted()?.state.kind, "active");
    const waiting = child.currentTurnSettled(false);
    assert.equal("state" in waiting, true);
    if (!("state" in waiting)) assert.fail("Subagent settlement must return an activation");
    assert.deepEqual(waiting.state, {
      kind: "waiting",
      dependencies: [{ kind: "undeclared", dependencyId: "undeclared" }],
    });

    child.currentTurnStarted();
    owner.requestInterruption(prepared.ownership);
    assert.equal(owner.inspectActivation(childId)?.state.kind, "active");
    const interrupted = child.currentTurnSettled(true);
    assert.equal("state" in interrupted, true);
    if (!("state" in interrupted)) assert.fail("Subagent interruption must return an activation");
    assert.equal(interrupted.state.kind, "interrupted");

    child.close();
    owner.runTerminated(prepared.ownership, true);
    assert.deepEqual(owner.inspectActivation(childId)?.state, {
      kind: "ended",
      outcome: "failed",
      error: "Agent Run runtime closed without committed completion or cancellation",
    });
    owner.close();
  });

  it("fails and releases a manually opened descendant when its runtime closes", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });
    const prepared = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "prepared-run",
      surface: "prepared-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });
    owner.runTerminated(prepared.ownership, true);

    const manual = new WorkflowBootstrap();
    manual.sessionStarted(context(childId, childPath));
    assert.equal(manual.inspectActivation(childId)?.state.kind, "active");
    manual.close();
    assert.equal(owner.inspectActivation(childId)?.state.kind, "ended");

    const resumed = await owner.prepareResume({
      sessionPath: childPath,
      runId: "resumed-run",
      surface: "resumed-surface",
    });
    owner.runTerminated(resumed.ownership, true);
    owner.close();
  });

  it("releases manual ownership when activation startup fails", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });
    const prepared = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "prepared-run",
      surface: "prepared-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });
    owner.runTerminated(prepared.ownership, true);

    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.exec(`
      CREATE TRIGGER reject_manual_activation
      BEFORE INSERT ON agent_activations
      BEGIN
        SELECT RAISE(ABORT, 'forced activation startup failure');
      END
    `);
    const manual = new WorkflowBootstrap();
    assert.throws(
      () => manual.sessionStarted(context(childId, childPath)),
      /forced activation startup failure/,
    );
    database.exec("DROP TRIGGER reject_manual_activation");
    database.close();

    const resumed = await owner.prepareResume({
      sessionPath: childPath,
      runId: "resumed-after-failure",
      surface: "resumed-surface",
    });
    owner.runTerminated(resumed.ownership, true);
    manual.close();
    owner.close();
  });

  it("releases after closed-bootstrap confirmation and reconciles later confirmed process exit", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: ownerPath,
      childCwd: root,
      childSessionId: ownerId,
    });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "child.jsonl");
    initializeSubagentSessionFile({
      mode: "standalone",
      childSessionFile: childPath,
      childCwd: root,
      childSessionId: childId,
    });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "first-run",
      surface: "first-surface",
      name: "Child",
      sessionBinding: bindNewWorkflowSession({
        workflowOwnerId: ownerId,
        agentId: childId,
        sessionPath: childPath,
      }),
    });

    owner.close();
    owner.runTerminated(first.ownership, true);

    const strandedOwner = new WorkflowBootstrap();
    strandedOwner.sessionStarted(context(ownerId, ownerPath));
    const stranded = await strandedOwner.prepareResume({
      sessionPath: childPath,
      runId: "stranded-run",
      surface: "stranded-surface",
    });
    strandedOwner.close();

    let inspectedSurface: string | undefined;
    const recoveredOwner = new WorkflowBootstrap({
      async confirmRunTerminated(locator) {
        inspectedSurface = locator.surface;
        return true;
      },
    });
    recoveredOwner.sessionStarted(context(ownerId, ownerPath));
    const recovered = await recoveredOwner.prepareResume({
      sessionPath: childPath,
      runId: "recovered-run",
      surface: "recovered-surface",
    });

    assert.equal(inspectedSurface, "stranded-surface");
    assert.ok(recovered.ownership.epoch > stranded.ownership.epoch);
    recoveredOwner.runTerminated(recovered.ownership, true);
    recoveredOwner.close();
  });

  it("runs the ownership completion hook before legacy result relay", async () => {
    const events: string[] = [];
    const run = {};
    await superviseLegacyAgentRun(run, {
      supervisor: {
        async watch() {
          events.push("watched");
          return { exitCode: 0 };
        },
      },
      ownership: {
        watchCompleted() {
          events.push("released");
        },
      },
      resultRelay: {
        completed() {
          events.push("relayed");
        },
        failed() {
          assert.fail("successful supervision must not relay failure");
        },
      },
      ui: { runStarted() {} },
    });

    assert.deepEqual(events, ["watched", "released", "relayed"]);
  });

  it("releases ownership only from explicit confirmed termination evidence", () => {
    assert.equal(hasConfirmedAgentRunTermination({ termination: "confirmed" }), true);
    assert.equal(hasConfirmedAgentRunTermination({ termination: "uncertain" }), false);
  });
});
