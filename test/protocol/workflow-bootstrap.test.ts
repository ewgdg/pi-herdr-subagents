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
  handleAgentRunWatcherCompletion,
  handleRequestReactivationWatcherCompletion,
  shouldCreateAutomaticRecoveryPane,
} from "../../pi-extension/subagents/index.ts";
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
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";
import { ActivationRecoveryStore } from "../../pi-extension/subagents/protocol/activation-recovery.ts";
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

async function pendingRecoveryFixture(prefix: string) {
  const root = await temporaryDirectory();
  const identities = new DeterministicIdentityFactory();
  const ownerId = identities.next();
  const ownerPath = join(root, `${prefix}-owner.jsonl`);
  initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
  const owner = new WorkflowBootstrap();
  owner.sessionStarted(context(ownerId, ownerPath));
  const childId = identities.next();
  const childPath = join(owner.workflow!.sessionsDirectory, `${prefix}-child.jsonl`);
  initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
  const first = owner.prepareSpawn({
    agentId: childId,
    sessionPath: childPath,
    runId: `${prefix}-first`,
    surface: `${prefix}-first-surface`,
    name: "Child",
    launchPolicy: { denyTools: [] },
    sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
  });
  owner.runStarted(first.ownership);
  const database = new DatabaseSync(owner.workflow!.databasePath);
  database.prepare(`INSERT INTO human_interrupts (
    agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
  ) VALUES (?, ?, ?, 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId, `${prefix}-ask`);
  database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, ?, 1)").run(childId, `${prefix}-ask`);
  database.close();
  owner.runTerminated(first.ownership, true, { error: `${prefix} first run failed` });
  return { root, owner, ownerId, ownerPath, childId, childPath, first };
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

  it("claims one automatic recovery before preparing its persisted-policy replacement", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "automatic-recovery-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "automatic-first",
      surface: "automatic-first-surface",
      name: "Child",
      launchPolicy: { denyTools: ["subagent"], toolAllowlist: "read,agent_complete" },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'recovery-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'recovery-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });

    const [episode] = owner.claimableAutomaticRecoveries();
    assert.equal(episode?.failedActivationId, first.ownership.runId);
    const replacement = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "automatic-replacement",
      surface: "automatic-replacement-surface",
    });
    assert.ok(replacement);
    assert.equal(replacement.member.launchPolicy?.toolAllowlist, "read,agent_complete");
    assert.equal(owner.claimableAutomaticRecoveries().length, 0);
    assert.equal(await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "automatic-duplicate",
      surface: "automatic-duplicate-surface",
    }), undefined);

    const child = new WorkflowBootstrap();
    try {
      child.sessionStarted(context(childId, childPath), replacement.environment);
      assert.deepEqual(owner.inspectActivation(childId)?.state, {
        kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
      });
    } finally {
      child.close();
      owner.close();
    }
  });

  it("persists the exact recovery pane intent before any pane is created", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "provisional-intent-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "provisional-intent-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "provisional-intent-first",
      surface: "provisional-intent-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'provisional-intent-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'provisional-intent-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });

    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "provisional-intent-replacement",
      workspaceId: "workspace-1",
      label: "pi-recovery-provisional-intent-replacement",
      cwd: root,
    });
    assert.ok(intent);
    const persisted = new DatabaseSync(owner.workflow!.databasePath, { readOnly: true });
    assert.deepEqual({
      ...persisted.prepare(`SELECT failed_activation_id, intent_id, run_id, workspace_id, label, cwd, surface, state
        FROM recovery_pane_intents WHERE failed_activation_id = ?`).get(first.ownership.runId) as Record<string, unknown>,
    }, {
      failed_activation_id: first.ownership.runId,
      intent_id: intent.intentId,
      run_id: "provisional-intent-replacement",
      workspace_id: "workspace-1",
      label: "pi-recovery-provisional-intent-replacement",
      cwd: root,
      surface: null,
      state: "prepared",
    });
    persisted.close();
    owner.close();
  });

  it("discovers and promotes a pane created immediately before Owner death", async () => {
    const fixture = await pendingRecoveryFixture("owner-death-pane");
    const { owner, ownerId, ownerPath, first, childId } = fixture;
    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "owner-death-replacement",
      workspaceId: "workspace-owner-death",
      label: "pi-recovery-owner-death",
      cwd: fixture.root,
    });
    assert.ok(intent);
    owner.beginAutomaticRecoveryPaneCreation(intent);
    owner.close();

    let discoveries = 0;
    const restarted = new WorkflowBootstrap({
      recoveryPaneLocator: {
        async discover(locator) {
          discoveries += 1;
          assert.equal(locator.workspaceId, "workspace-owner-death");
          assert.equal(locator.label, "pi-recovery-owner-death");
          assert.equal(locator.cwd, fixture.root);
          assert.equal(locator.surface, undefined, "the crash happened before pane-id acknowledgement");
          return { kind: "present", surface: "discovered-owner-death-pane" };
        },
      },
      agentRunTerminator: {
        async inspect() { return { kind: "present" }; },
        async close() {},
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [reconciled] = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(reconciled?.kind, "present");
      assert.equal(discoveries, 1);
      const recoveredIntent = restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId);
      assert.equal(recoveredIntent?.state, "created");
      assert.equal(recoveredIntent?.surface, "discovered-owner-death-pane");

      const prepared = restarted.promoteAutomaticRecoveryPane({
        failedActivationId: first.ownership.runId,
        intentId: intent.intentId,
        runId: intent.runId,
        surface: "discovered-owner-death-pane",
      });
      assert.ok(prepared, "restart must adopt the exact discovered pane instead of creating another");
      assert.equal(prepared.ownership.runId, intent.runId);
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "launching");
      const projection = restarted.inspectTarget({ agent: childId }) as any;
      assert.equal(projection.recovery.state, "launching");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "promoted");
      const durable = new DatabaseSync(restarted.workflow!.databasePath, { readOnly: true });
      const ownership = durable.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?")
        .get(prepared.ownership.resourceId) as { owner_id: string; fencing_epoch: number };
      const checkpoint = durable.prepare("SELECT value, fencing_epoch FROM fenced_state WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'")
        .get(prepared.ownership.resourceId) as { value: string; fencing_epoch: number };
      assert.equal(ownership.owner_id, intent.runId);
      assert.equal(Number(ownership.fencing_epoch), prepared.ownership.epoch);
      assert.deepEqual(JSON.parse(checkpoint.value), {
        kind: "automatic-recovery",
        surface: "discovered-owner-death-pane",
        runId: intent.runId,
        fencingEpoch: prepared.ownership.epoch,
        phase: "prepared",
      });
      durable.close();
      assert.equal(restarted.inspectActivation(childId)?.runId, first.ownership.runId,
        "pane discovery alone must not acknowledge child bootstrap");
    } finally {
      restarted.close();
    }
  });

  it("keeps an ambiguous recovery creation fenced without closing either pane", async () => {
    const fixture = await pendingRecoveryFixture("ambiguous-pane");
    const { owner, ownerId, ownerPath, first } = fixture;
    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "ambiguous-pane-replacement",
      workspaceId: "workspace-ambiguous",
      label: "pi-recovery-ambiguous",
      cwd: fixture.root,
    });
    assert.ok(intent);
    owner.beginAutomaticRecoveryPaneCreation(intent);
    owner.close();

    let closeCalls = 0;
    const restarted = new WorkflowBootstrap({
      recoveryPaneLocator: {
        async discover() { return { kind: "ambiguous", error: "two exact labels" }; },
      },
      agentRunTerminator: {
        async inspect() { return { kind: "present" }; },
        async close() { closeCalls += 1; },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(result?.kind, "unknown");
      assert.equal(closeCalls, 0);
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "creating");
      assert.equal(restarted.claimableAutomaticRecoveries().length, 1);
    } finally {
      restarted.close();
    }
  });

  it("keeps a creating pane intent fenced across unavailable and ambiguous discovery until exact promotion", async () => {
    const fixture = await pendingRecoveryFixture("creating-discovery");
    const { owner, ownerId, ownerPath, first } = fixture;
    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "creating-discovery-replacement",
      workspaceId: "workspace-creating-discovery",
      label: "pi-recovery-creating-discovery",
      cwd: fixture.root,
    });
    assert.ok(intent);
    owner.beginAutomaticRecoveryPaneCreation(intent);
    owner.close();

    let discovery: "unavailable" | "ambiguous" | "present" = "unavailable";
    let discoveries = 0;
    let createAttempts = 0;
    const restarted = new WorkflowBootstrap({
      recoveryPaneLocator: {
        async discover(locator) {
          discoveries += 1;
          assert.equal(locator.surface, undefined, "unacknowledged create must not guess a pane id");
          if (discovery === "present") return { kind: "present", surface: "original-creating-pane" };
          return discovery === "ambiguous"
            ? { kind: "ambiguous", error: "two exact recovery panes" }
            : { kind: "unavailable", error: "pane discovery offline" };
        },
      },
      agentRunTerminator: {
        async inspect() { return { kind: "present" }; },
        async close() { throw new Error("the original pane must not be guessed or closed"); },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const firstReconciliation = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(firstReconciliation[0]?.kind, "unknown");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "creating");
      const attemptLaunch = () => {
        const currentIntent = restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)!;
        if (shouldCreateAutomaticRecoveryPane(currentIntent)) createAttempts += 1;
      };
      assert.equal(shouldCreateAutomaticRecoveryPane(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)!), false);
      attemptLaunch();

      discovery = "ambiguous";
      const secondReconciliation = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(secondReconciliation[0]?.kind, "unknown");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "creating");
      assert.equal(shouldCreateAutomaticRecoveryPane(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)!), false);
      attemptLaunch();
      assert.equal(discoveries, 2);
      assert.equal(createAttempts, 0, "repeated launch attempts must not create a second pane");

      discovery = "present";
      const [adopted] = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(adopted?.kind, "present");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "created");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.surface, "original-creating-pane");
      const promoted = restarted.promoteAutomaticRecoveryPane({
        failedActivationId: first.ownership.runId,
        intentId: intent.intentId,
        runId: intent.runId,
        surface: "original-creating-pane",
      });
      assert.ok(promoted, "exact discovery must allow the original pane to promote");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "promoted");
      assert.equal(discoveries, 3);
    } finally {
      restarted.close();
    }
  });

  it("retains a cleanup intent after close failure and closes it after Owner restart", async () => {
    const fixture = await pendingRecoveryFixture("cleanup-restart");
    const { owner, ownerId, ownerPath, first } = fixture;
    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "cleanup-restart-replacement",
      workspaceId: "workspace-cleanup",
      label: "pi-recovery-cleanup",
      cwd: fixture.root,
    });
    assert.ok(intent);
    owner.beginAutomaticRecoveryPaneCreation(intent);
    owner.recordAutomaticRecoveryPaneCreated(intent, "cleanup-pane");
    owner.beginRecoveryPaneCleanup(intent.intentId, "test cleanup");
    owner.close();

    let closeAttempts = 0;
    const restarted = new WorkflowBootstrap({
      recoveryPaneLocator: {
        async discover() { return { kind: "present", surface: "cleanup-pane" }; },
      },
      agentRunTerminator: {
        async inspect() {
          return closeAttempts < 2 ? { kind: "present" } : { kind: "missing" };
        },
        async close() {
          closeAttempts += 1;
          if (closeAttempts === 1) throw new Error("close failed once");
        },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const firstReconciliation = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(firstReconciliation[0]?.kind, "unknown");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "cleanup-pending");
      assert.equal(closeAttempts, 1);

      const secondReconciliation = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(secondReconciliation[0]?.kind, "cleaned");
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId), undefined);
      assert.equal(restarted.claimableAutomaticRecoveries().length, 1);
    } finally {
      restarted.close();
    }
  });

  it("rejects a competing claim without closing a provisional pane", async () => {
    const fixture = await pendingRecoveryFixture("lost-claim-pane");
    const { owner, ownerId, ownerPath, first, childPath } = fixture;
    const intent = owner.prepareAutomaticRecoveryPane({
      failedActivationId: first.ownership.runId,
      runId: "lost-claim-intent",
      workspaceId: "workspace-lost-claim",
      label: "pi-recovery-lost-claim",
      cwd: fixture.root,
    });
    assert.ok(intent);
    owner.beginAutomaticRecoveryPaneCreation(intent);
    owner.recordAutomaticRecoveryPaneCreated(intent, "lost-claim-pane");
    const otherClaim = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "other-owner-claim",
      surface: "other-owner-pane",
    });
    assert.equal(otherClaim, undefined, "a provisional intent fences competing recovery claims");
    owner.close();

    let closeCalls = 0;
    const restarted = new WorkflowBootstrap({
      recoveryPaneLocator: {
        async discover() { return { kind: "present", surface: "lost-claim-pane" }; },
      },
      agentRunTerminator: {
        async inspect() { return { kind: "present" }; },
        async close() { closeCalls += 1; },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryPaneIntents();
      assert.equal(result?.kind, "present");
      assert.equal(closeCalls, 0);
      assert.equal(restarted.inspectAutomaticRecoveryPaneIntent(intent.intentId)?.state, "created");
    } finally {
      restarted.close();
    }
  });

  it("commits recovery ownership and the exact prepared locator as one durable claim", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "atomic-recovery-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "atomic-recovery-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "atomic-recovery-first",
      surface: "atomic-recovery-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'atomic-recovery-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'atomic-recovery-ask', 1)").run(childId);
    database.exec(`
      CREATE TRIGGER reject_recovery_checkpoint
      BEFORE UPDATE OF value ON fenced_state
      WHEN NEW.state_key = 'agent-run-checkpoint' AND NEW.value LIKE '%atomic-recovery-surface%'
      BEGIN
        SELECT RAISE(ABORT, 'forced prepared-checkpoint failure');
      END
    `);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });

    const recoveries = new ActivationRecoveryStore(owner.workflow!.databasePath);
    try {
      assert.throws(
        () => recoveries.claimRun(ownerId, first.ownership.runId, "atomic-recovery-run", Date.now(), "atomic-recovery-surface"),
        /forced prepared-checkpoint failure/,
      );
      const failedClaimDatabase = new DatabaseSync(owner.workflow!.databasePath, { readOnly: true });
      assert.equal(failedClaimDatabase.prepare("SELECT 1 FROM ownership WHERE owner_id = ?").get("atomic-recovery-run"), undefined,
        "a failed prepared-checkpoint commit must not leave ownership behind");
      assert.equal(failedClaimDatabase.prepare("SELECT state FROM activation_recoveries WHERE failed_activation_id = ?").get(first.ownership.runId)?.state, "pending",
        "a failed prepared-checkpoint commit must return the exact episode to its prior state");
      failedClaimDatabase.close();
    } finally {
      recoveries.close();
    }

    const retryDatabase = new DatabaseSync(owner.workflow!.databasePath);
    retryDatabase.exec("DROP TRIGGER reject_recovery_checkpoint");
    retryDatabase.close();
    const claimStore = new ActivationRecoveryStore(owner.workflow!.databasePath);
    try {
      const claim = claimStore.claimRun(ownerId, first.ownership.runId, "atomic-recovery-run", Date.now(), "atomic-recovery-surface");
      assert.ok(claim);
      const committed = new DatabaseSync(owner.workflow!.databasePath, { readOnly: true });
      const ownership = committed.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?").get(claim.ownership.resourceId) as { owner_id: string; fencing_epoch: number };
      const checkpoint = committed.prepare("SELECT value, fencing_epoch FROM fenced_state WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'").get(claim.ownership.resourceId) as { value: string; fencing_epoch: number };
      assert.equal(ownership.owner_id, "atomic-recovery-run");
      assert.equal(Number(ownership.fencing_epoch), claim.ownership.epoch);
      assert.equal(Number(checkpoint.fencing_epoch), claim.ownership.epoch);
      assert.deepEqual(JSON.parse(checkpoint.value), {
        kind: "automatic-recovery",
        surface: "atomic-recovery-surface",
        runId: "atomic-recovery-run",
        fencingEpoch: claim.ownership.epoch,
        phase: "prepared",
      });
      committed.close();
    } finally {
      claimStore.close();
      owner.close();
    }
  });

  it("closes a merely prepared recovery pane before returning the exact claim to pending", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner-prepared-recovery.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "prepared-recovery-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "prepared-recovery-first",
      surface: "prepared-recovery-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'prepared-recovery-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'prepared-recovery-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "prepared-recovery-replacement",
      surface: "prepared-recovery-surface",
    });
    assert.ok(prepared);
    owner.close();

    const closed: string[] = [];
    let inspections = 0;
    const restarted = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "prepared-recovery-surface");
          inspections += 1;
          return inspections === 1 ? { kind: "present" } : { kind: "missing" };
        },
        async close(locator) {
          closed.push(locator.surface);
        },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryRuns();
      assert.equal(result?.kind, "pending");
      assert.deepEqual(closed, ["prepared-recovery-surface"]);
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
      assert.equal(restarted.claimableAutomaticRecoveries().length, 1,
        "the same recovery episode remains claimable after confirmed prepared-pane cleanup");
      assert.throws(
        () => restarted.beginAutomaticRecoveryDispatch(prepared.ownership),
        (error: unknown) => (error as { code?: string }).code === "OwnershipLost",
        "the stale launcher cannot acknowledge dispatch after the exact claim was requeued",
      );
    } finally {
      restarted.close();
    }
  });

  it("retains and reattaches a live dispatched recovery after Owner exit before child bootstrap", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "owner-exit-before-bootstrap.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "owner-exit-first",
      surface: "owner-exit-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'owner-exit-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'owner-exit-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "owner-exit-replacement",
      surface: "owner-exit-replacement-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.confirmAutomaticRecoveryDispatch(prepared.ownership);
    const checkpointDatabase = new DatabaseSync(owner.workflow!.databasePath, { readOnly: true });
    const checkpoint = checkpointDatabase.prepare(`SELECT value FROM fenced_state
      WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'`).get(
      prepared.ownership.resourceId,
    ) as { value: string };
    assert.equal(JSON.parse(checkpoint.value).phase, "dispatched");
    checkpointDatabase.close();
    assert.equal((owner.inspectTarget({ agent: childId }) as any).recovery.state, "launching",
      "Owner dispatch must not acknowledge child bootstrap");
    owner.close();

    const restarted = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "owner-exit-replacement-surface");
          return { kind: "present" };
        },
        async close() {},
      },
    });
    restarted.sessionStarted(context(ownerId, ownerPath));
    const reconciliation = await restarted.reconcileAutomaticRecoveryRuns();
    assert.equal(reconciliation[0]?.kind, "live");
    assert.equal(reconciliation[0]?.ownership.runId, "owner-exit-replacement");
    assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "launching");
    assert.deepEqual(restarted.claimableAutomaticRecoveries(), []);

    const child = new WorkflowBootstrap();
    try {
      child.sessionStarted(context(childId, childPath), prepared.environment);
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "active");
    } finally {
      child.close();
      restarted.close();
    }
  });

  it("requeues a pre-bootstrap dispatching recovery after exact pane absence", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "ambiguous-dispatch-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "ambiguous-dispatch-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "ambiguous-dispatch-first",
      surface: "ambiguous-dispatch-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'ambiguous-dispatch-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'ambiguous-dispatch-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "ambiguous-dispatch-replacement",
      surface: "ambiguous-dispatch-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.close();

    const restarted = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "ambiguous-dispatch-surface");
          return { kind: "missing" };
        },
        async close() {
          assert.fail("ambiguous dispatch must not close or requeue the pane");
        },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryRuns();
      assert.equal(result?.kind, "pending");
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
      assert.equal(restarted.claimableAutomaticRecoveries().length, 1,
        "exact pane absence fences the ambiguous dispatch before requeue");
    } finally {
      restarted.close();
    }
  });

  it("closes and requeues a pre-bootstrap dispatching recovery after exact pane termination", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "dispatching-present-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "dispatching-present-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "dispatching-present-first", surface: "dispatching-present-first-surface",
      name: "Child", launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'dispatching-present-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'dispatching-present-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "dispatching-present-replacement",
      surface: "dispatching-present-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.close();

    const closed: string[] = [];
    let inspections = 0;
    const restarted = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "dispatching-present-surface");
          inspections += 1;
          return inspections === 1 ? { kind: "present" } : { kind: "missing" };
        },
        async close(locator) { closed.push(locator.surface); },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryRuns();
      assert.equal(result?.kind, "pending");
      assert.deepEqual(closed, ["dispatching-present-surface"]);
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
      assert.equal(restarted.claimableAutomaticRecoveries().length, 1);
    } finally {
      restarted.close();
    }
  });

  it("keeps the recovery fence when child bootstrap races dispatching-pane cleanup", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "bootstrap-race-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "bootstrap-race-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "bootstrap-race-first",
      surface: "bootstrap-race-first-surface",
      name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'bootstrap-race-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'bootstrap-race-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "bootstrap-race-replacement",
      surface: "bootstrap-race-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.close();

    const child = new WorkflowBootstrap();
    let inspections = 0;
    const restarted = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "bootstrap-race-surface");
          inspections += 1;
          return inspections === 1 ? { kind: "present" } : { kind: "missing" };
        },
        async close(locator) {
          child.sessionStarted(context(childId, childPath), prepared.environment);
          assert.equal(locator.surface, "bootstrap-race-surface");
        },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryRuns();
      assert.equal(result?.kind, "unknown");
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "active");
      assert.equal(restarted.inspectActivation(childId)?.runId, prepared.ownership.runId);
      assert.deepEqual(restarted.claimableAutomaticRecoveries(), [],
        "the raced activation keeps ownership and prevents a duplicate replacement");
    } finally {
      child.close();
      restarted.close();
    }
  });

  it("requires exact pane absence before manual resume supersedes a dispatching recovery claim", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "manual-requeue-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "manual-requeue-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "manual-requeue-first", surface: "manual-requeue-first-surface",
      name: "Child", launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'manual-requeue-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'manual-requeue-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "manual-requeue-recovery",
      surface: "manual-requeue-recovery-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.close();

    let inspections = 0;
    const closed: string[] = [];
    const restarted = new WorkflowBootstrap({
      confirmRunTerminated: async () => true,
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "manual-requeue-recovery-surface");
          inspections += 1;
          return inspections === 1 ? { kind: "present" } : { kind: "missing" };
        },
        async close(locator) { closed.push(locator.surface); },
      },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const resumed = await restarted.prepareResume({
        sessionPath: childPath,
        runId: "manual-resume-after-requeue",
        surface: "manual-resume-surface",
      });
      assert.deepEqual(closed, ["manual-requeue-recovery-surface"]);
      assert.equal(inspections, 2, "manual resume must confirm exact absence after closing the old pane");
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
      restarted.runStarted(resumed.ownership);
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "resolved");
    } finally {
      restarted.close();
    }
  });

  it("reconciles confirmed-absent and unknown automatic recovery locators without duplicate launches", async () => {
    for (const liveness of ["missing", "unavailable"] as const) {
      const root = await temporaryDirectory();
      const identities = new DeterministicIdentityFactory();
      const ownerId = identities.next();
      const ownerPath = join(root, `owner-${liveness}.jsonl`);
      initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
      const owner = new WorkflowBootstrap();
      owner.sessionStarted(context(ownerId, ownerPath));
      const childId = identities.next();
      const childPath = join(owner.workflow!.sessionsDirectory, `child-${liveness}.jsonl`);
      initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
      const first = owner.prepareSpawn({
        agentId: childId, sessionPath: childPath, runId: `first-${liveness}`, surface: `first-surface-${liveness}`,
        name: "Child", launchPolicy: { denyTools: [] },
        sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
      });
      owner.runStarted(first.ownership);
      const database = new DatabaseSync(owner.workflow!.databasePath);
      database.prepare(`INSERT INTO human_interrupts (
        agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
      ) VALUES (?, ?, ?, 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId, `ask-${liveness}`);
      database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, ?, 1)").run(childId, `ask-${liveness}`);
      database.close();
      owner.runTerminated(first.ownership, true, { error: "first failed" });
      const prepared = await owner.prepareAutomaticRecovery({
        failedActivationId: first.ownership.runId,
        sessionPath: childPath,
        runId: `replacement-${liveness}`,
        surface: `replacement-surface-${liveness}`,
      });
      assert.ok(prepared);
      owner.close();

      let closeCalls = 0;
      const restarted = new WorkflowBootstrap({
        agentRunTerminator: {
          async inspect() {
            return liveness === "missing"
              ? { kind: "missing" }
              : { kind: "unavailable", error: "liveness backend offline" };
          },
          async close() { closeCalls += 1; },
        },
      });
      try {
        restarted.sessionStarted(context(ownerId, ownerPath));
        const [result] = await restarted.reconcileAutomaticRecoveryRuns();
        if (liveness === "missing") {
          assert.equal(result?.kind, "pending");
          assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
          assert.equal(restarted.claimableAutomaticRecoveries().length, 1,
            "confirmed absent unbootstrapped launch returns the same episode to pending");
        } else {
          assert.equal(result?.kind, "unknown");
          assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "launching");
          assert.equal(closeCalls, 0, "unknown liveness must not attempt pane cleanup");
          assert.deepEqual(restarted.claimableAutomaticRecoveries(), [],
            "unknown liveness preserves the ownership fence");
        }
      } finally {
        restarted.close();
      }
    }
  });

  it("exhausts a confirmed-dead activated automatic replacement", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner-dead-active.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "dead-active-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "dead-active-first", surface: "dead-active-first-surface",
      name: "Child", launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'dead-active-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'dead-active-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "dead-active-replacement",
      surface: "dead-active-replacement-surface",
    });
    assert.ok(prepared);
    const child = new WorkflowBootstrap();
    child.sessionStarted(context(childId, childPath), prepared.environment);
    owner.close();

    const restarted = new WorkflowBootstrap({
      agentRunTerminator: { async inspect() { return { kind: "missing" }; }, async close() {} },
    });
    try {
      restarted.sessionStarted(context(ownerId, ownerPath));
      const [result] = await restarted.reconcileAutomaticRecoveryRuns();
      assert.equal(result?.kind, "exhausted");
      assert.equal((restarted.inspectTarget({ agent: childId }) as any).recovery.state, "exhausted");
      assert.deepEqual(restarted.claimableAutomaticRecoveries(), []);
    } finally {
      child.close();
      restarted.close();
    }
  });

  it("reconciles pending recovery when the Owner router returns after an offline interval", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "offline-recovery-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "offline-first", surface: "offline-first-surface", name: "Offline Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'offline-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'offline-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "failed while Owner router was offline" });
    assert.equal(owner.claimableAutomaticRecoveries().length, 1, "Owner absence must leave the episode paused and durable");

    let launches = 0;
    let replacement: import("../../pi-extension/subagents/protocol/workflow-bootstrap.ts").PreparedAutomaticRecoveryRun | undefined;
    try {
      await owner.startDirectSignalRouter({
        projectInboxBatch() {},
        async onAutomaticRecoveryRequested() {
          launches += 1;
          replacement = await owner.prepareAutomaticRecovery({
            failedActivationId: first.ownership.runId,
            sessionPath: childPath,
            runId: "offline-replacement",
            surface: "offline-replacement-surface",
          });
        },
      });
      assert.ok(replacement);
      assert.equal(launches, 1);
      await owner.startDirectSignalRouter({ projectInboxBatch() {}, onAutomaticRecoveryRequested() { launches += 1; } });
      assert.equal(launches, 1, "a claimed recovery must not be scheduled twice");
    } finally {
      await owner.closeDirectSignalRouter();
      owner.close();
    }
  });

  it("retries an Owner-live unavailable recovery locator without restart", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "live-retry-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    let inspections = 0;
    const owner = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "live-retry-surface");
          inspections += 1;
          return inspections === 1
            ? { kind: "unavailable", error: "transient liveness outage" }
            : { kind: "missing" };
        },
        async close() {},
      },
    });
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "live-retry-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "live-retry-first", surface: "live-retry-first-surface",
      name: "Child", launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'live-retry-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'live-retry-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "live-retry-replacement",
      surface: "live-retry-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);

    let launches = 0;
    let resolveLaunch!: () => void;
    const launchObserved = new Promise<void>((resolve) => { resolveLaunch = resolve; });
    try {
      await owner.startDirectSignalRouter({
        projectInboxBatch() {},
        onAutomaticRecoveryRequested() {
          launches += 1;
          resolveLaunch();
        },
      });
      const timeout = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error("Owner-live recovery reconciliation did not retry")), 500).unref?.();
      });
      await Promise.race([launchObserved, timeout]);
      assert.equal(inspections, 2);
      assert.equal(launches, 1);
      assert.equal((owner.inspectTarget({ agent: childId }) as any).recovery.state, "pending");
      assert.equal(owner.claimableAutomaticRecoveries().length, 1);
    } finally {
      await owner.closeDirectSignalRouter();
      owner.close();
    }
  });

  it("re-registers a live replacement watcher after an unavailable Owner-live inspection", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "live-watcher-owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    let inspections = 0;
    const owner = new WorkflowBootstrap({
      agentRunTerminator: {
        async inspect(locator) {
          assert.equal(locator.surface, "live-watcher-surface");
          inspections += 1;
          return inspections === 1
            ? { kind: "unavailable", error: "transient liveness outage" }
            : { kind: "present" };
        },
        async close() {},
      },
    });
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "live-watcher-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "live-watcher-first", surface: "live-watcher-first-surface",
      name: "Child", launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'live-watcher-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, first.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'live-watcher-ask', 1)").run(childId);
    database.close();
    owner.runTerminated(first.ownership, true, { error: "first run failed" });
    const prepared = await owner.prepareAutomaticRecovery({
      failedActivationId: first.ownership.runId,
      sessionPath: childPath,
      runId: "live-watcher-replacement",
      surface: "live-watcher-surface",
    });
    assert.ok(prepared);
    owner.beginAutomaticRecoveryDispatch(prepared.ownership);
    owner.confirmAutomaticRecoveryDispatch(prepared.ownership);

    let resolveLive!: () => void;
    const liveObserved = new Promise<void>((resolve) => { resolveLive = resolve; });
    const reconciliations: import("../../pi-extension/subagents/protocol/workflow-bootstrap.ts").AutomaticRecoveryRunReconciliation[][] = [];
    try {
      await owner.startDirectSignalRouter({
        projectInboxBatch() {},
        onAutomaticRecoveryReconciled(results) {
          reconciliations.push(results);
          if (results.some((result) => result.kind === "live")) resolveLive();
        },
      });
      const timeout = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error("Owner-live watcher reconciliation did not observe the live pane")), 500).unref?.();
      });
      await Promise.race([liveObserved, timeout]);
      assert.equal(inspections, 2);
      assert.equal(reconciliations.at(-1)?.[0]?.kind, "live");
      assert.equal((owner.inspectTarget({ agent: childId }) as any).recovery.state, "launching");
    } finally {
      await owner.closeDirectSignalRouter();
      owner.close();
    }
  });

  it("notifies the live Owner from a nested watcher failure and claims one durable replacement", async () => {
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
      agentId: parentId, sessionPath: parentPath, runId: "parent-run", surface: "parent-surface", name: "Parent",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: parentId, sessionPath: parentPath }),
    });
    owner.runStarted(parentRun.ownership);
    const parent = new WorkflowBootstrap();
    parent.sessionStarted(context(parentId, parentPath), parentRun.environment);
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "nested-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const childRun = parent.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "nested-failed-run", surface: "nested-surface", name: "Nested Child",
      launchPolicy: { denyTools: ["subagent"] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    parent.runStarted(childRun.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'nested-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, childRun.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'nested-ask', 1)").run(childId);
    database.close();

    let replacement: Promise<import("../../pi-extension/subagents/protocol/workflow-bootstrap.ts").PreparedAutomaticRecoveryRun | undefined> | undefined;
    let notification: Promise<unknown> | undefined;
    const child = new WorkflowBootstrap();
    try {
      await owner.startDirectSignalRouter({
        projectInboxBatch() {},
        async onAutomaticRecoveryRequested() {
          const [episode] = owner.claimableAutomaticRecoveries();
          replacement ??= owner.prepareAutomaticRecovery({
            failedActivationId: episode!.failedActivationId,
            sessionPath: childPath,
            runId: "nested-replacement",
            surface: "nested-replacement-surface",
          });
          await replacement;
        },
      });
      const relays: string[] = [];
      await superviseLegacyAgentRun({ abortController: undefined }, {
        supervisor: {
          async watch() {
            return { termination: "confirmed" as const, exitCode: 1, errorMessage: "nested watcher observed failure" };
          },
        },
        ownership: {
          watchCompleted(_running, result) {
            return handleAgentRunWatcherCompletion(parent, childRun.ownership, result, () => {
              notification = parent.notifyOwnerOfAutomaticRecovery(childRun.ownership);
            });
          },
        },
        resultRelay: {
          completed() { assert.fail("recovery-pending failure must not fabricate a parent result"); },
          suppressed() { relays.push("suppressed"); },
          failed() { assert.fail("watcher completed normally"); },
        },
        ui: { runStarted() {} },
      });
      assert.deepEqual(relays, ["suppressed"]);
      await notification;
      const prepared = await replacement;
      assert.ok(prepared, "the still-live Owner must claim the durable episode without restart");
      assert.equal(owner.claimableAutomaticRecoveries().length, 0);
      child.sessionStarted(context(childId, childPath), prepared.environment);
      assert.equal(owner.inspectActivation(childId)?.runId, "nested-replacement");
      assert.equal((owner.inspectTarget({ agent: childId }) as any).recovery.state, "active");
    } finally {
      child.close();
      parent.close();
      await owner.closeDirectSignalRouter();
      owner.close();
    }
  });

  it("notifies a live Owner after the direct spawner bootstrap has already closed", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const parentId = identities.next();
    const parentPath = join(owner.workflow!.sessionsDirectory, "closed-parent.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: parentPath, childCwd: root, childSessionId: parentId });
    const parentRun = owner.prepareSpawn({
      agentId: parentId, sessionPath: parentPath, runId: "closed-parent-run", surface: "closed-parent-surface", name: "Parent",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: parentId, sessionPath: parentPath }),
    });
    owner.runStarted(parentRun.ownership);
    const parent = new WorkflowBootstrap();
    parent.sessionStarted(context(parentId, parentPath), parentRun.environment);
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "closed-spawner-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const childRun = parent.prepareSpawn({
      agentId: childId, sessionPath: childPath, runId: "closed-spawner-child-run", surface: "closed-spawner-child-surface", name: "Child",
      launchPolicy: { denyTools: [] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    parent.runStarted(childRun.ownership);
    const database = new DatabaseSync(owner.workflow!.databasePath);
    database.prepare(`INSERT INTO human_interrupts (
      agent_id, activation_id, tool_call_id, status, response_input_id, created_at_ms, updated_at_ms, terminal_reason
    ) VALUES (?, ?, 'closed-spawner-ask', 'pending', NULL, 1, 1, NULL)`).run(childId, childRun.ownership.runId);
    database.prepare("INSERT INTO human_attention (agent_id, tool_call_id, created_at_ms) VALUES (?, 'closed-spawner-ask', 1)").run(childId);
    database.close();

    let notifications = 0;
    let replacement: Promise<import("../../pi-extension/subagents/protocol/workflow-bootstrap.ts").PreparedAutomaticRecoveryRun | undefined> | undefined;
    const child = new WorkflowBootstrap();
    try {
      await owner.startDirectSignalRouter({
        projectInboxBatch() {},
        async onAutomaticRecoveryRequested() {
          notifications += 1;
          const episode = owner.claimableAutomaticRecoveries()
            .find((candidate) => candidate.failedActivationId === childRun.ownership.runId);
          if (!episode) return;
          replacement ??= owner.prepareAutomaticRecovery({
            failedActivationId: episode.failedActivationId,
            sessionPath: childPath,
            runId: "closed-spawner-replacement",
            surface: "closed-spawner-replacement-surface",
          });
          await replacement;
        },
      });
      parent.runTerminated(childRun.ownership, true, { error: "child failed as parent closed" });
      parent.close();

      assert.equal(await parent.notifyOwnerOfAutomaticRecovery(childRun.ownership), "notified");
      const prepared = await replacement;
      assert.ok(prepared);
      assert.equal(notifications, 1);
      child.sessionStarted(context(childId, childPath), prepared.environment);
      assert.equal(owner.inspectActivation(childId)?.runId, "closed-spawner-replacement");
    } finally {
      child.close();
      await owner.closeDirectSignalRouter();
      owner.close();
    }
  });

  it("starts exactly one replacement when a Request-reactivated run fails while the Owner is live", async () => {
    const root = await temporaryDirectory();
    const identities = new DeterministicIdentityFactory();
    const ownerId = identities.next();
    const ownerPath = join(root, "owner.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: ownerPath, childCwd: root, childSessionId: ownerId });
    const owner = new WorkflowBootstrap();
    owner.sessionStarted(context(ownerId, ownerPath));
    const childId = identities.next();
    const childPath = join(owner.workflow!.sessionsDirectory, "request-reactivated-child.jsonl");
    initializeSubagentSessionFile({ mode: "standalone", childSessionFile: childPath, childCwd: root, childSessionId: childId });
    const first = owner.prepareSpawn({
      agentId: childId,
      sessionPath: childPath,
      runId: "request-reactivation-first",
      surface: "request-reactivation-first-surface",
      name: "Request Child",
      launchPolicy: { denyTools: ["subagent"] },
      sessionBinding: bindNewWorkflowSession({ workflowOwnerId: ownerId, agentId: childId, sessionPath: childPath }),
    });
    owner.runStarted(first.ownership);
    owner.runTerminated(first.ownership, true, { error: "first run ended before a new Request" });

    const messages = new DirectSignalStore(owner.workflow!.databasePath);
    const resumed = messages.acceptEndedRecipientRequest({
      request: {
        workflowOwnerId: ownerId,
        messageId: "request-reactivation-work",
        senderAgentId: ownerId,
        recipientAgentId: childId,
        sourceEntryId: "request-reactivation-source",
        payloadDigest: "request-reactivation-digest",
        deliveryTiming: "steer",
        responseRequired: true,
        message: "resume and handle this Request",
      },
      recipient: { workflowOwnerId: ownerId, agentId: childId },
      endpoint: "prepared://request-reactivation",
      runId: "request-reactivation-run",
      checkpoint: JSON.stringify({ surface: "request-reactivation-surface" }),
      acceptedAtMs: 1,
    });
    messages.close();

    let launches = 0;
    let replacement: Promise<import("../../pi-extension/subagents/protocol/workflow-bootstrap.ts").PreparedAutomaticRecoveryRun | undefined> | undefined;
    let reconciliation: Promise<void> | undefined;
    const relays: string[] = [];
    await superviseLegacyAgentRun({ abortController: undefined }, {
      supervisor: {
        async watch() {
          return { termination: "confirmed" as const, exitCode: 1, errorMessage: "Request-reactivated run failed" };
        },
      },
      ownership: {
        watchCompleted(_running, result) {
          return handleRequestReactivationWatcherCompletion(owner, resumed.ownership, result, () => {
            reconciliation = (async () => {
              assert.equal(await owner.notifyOwnerOfAutomaticRecovery(resumed.ownership), "owner");
              launches += 1;
              const [episode] = owner.claimableAutomaticRecoveries();
              replacement = owner.prepareAutomaticRecovery({
                failedActivationId: episode!.failedActivationId,
                sessionPath: childPath,
                runId: "request-reactivation-replacement",
                surface: "request-reactivation-replacement-surface",
              });
              await replacement;
            })();
          });
        },
      },
      resultRelay: {
        completed() { assert.fail("internal Request reactivation must not relay a legacy result"); },
        suppressed() { relays.push("suppressed"); },
        failed() { assert.fail("watcher completed normally"); },
      },
      ui: { runStarted() {} },
    });

    const child = new WorkflowBootstrap();
    try {
      await reconciliation;
      const prepared = await replacement;
      assert.ok(prepared);
      assert.equal(launches, 1);
      assert.deepEqual(relays, ["suppressed"]);
      child.sessionStarted(context(childId, childPath), prepared.environment);
      assert.equal(owner.inspectActivation(childId)?.runId, "request-reactivation-replacement");
      assert.equal((owner.inspectTarget({ agent: childId }) as any).recovery.state, "active");
      assert.deepEqual(owner.claimableAutomaticRecoveries(), []);
    } finally {
      child.close();
      owner.close();
    }
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
