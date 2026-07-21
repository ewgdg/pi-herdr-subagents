import assert from "node:assert/strict";
import { copyFile, mkdtemp, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { afterEach, describe, it } from "node:test";
import {
  DeterministicIdentityFactory,
  WorkflowScenario,
} from "./scenario-harness.ts";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-workflow-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })),
  );
});

describe("durable Workflow identity scenarios", () => {
  it("creates or reopens one Workflow keyed by the Owner Pi session UUID", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: owner, runtime } = scenario.createOwner();
    const first = runtime.workflow;

    runtime.close();
    scenario.clock.advance(5_000);
    const reopenedRuntime = scenario.startOwner(scenario.transcripts.resume(owner.sessionPath));
    const reopened = reopenedRuntime.workflow;

    assert.equal(first.ownerAgentId, owner.agentId);
    assert.equal(reopened.ownerAgentId, owner.agentId);
    assert.equal(reopened.createdAtMs, first.createdAtMs);
    assert.equal(reopened.databasePath, first.databasePath);
    assert.equal(basename(reopened.directory), owner.agentId);
    assert.equal(dirname(reopened.directory), scenario.rootDirectory);
    assert.equal(dirname(reopened.sessionsDirectory), reopened.directory);
    reopenedRuntime.close();
  });

  it("preserves Agent identity on resume and allocates new identities for create, fork, and clone", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const created = scenario.childSession(runtime, "created");
    const createdAgent = runtime.addAgent({
      session: created,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Created",
    });

    const resumed = scenario.transcripts.resume(created.sessionPath);
    const forked = scenario.forkSession(runtime, created, "forked");
    const cloned = scenario.cloneSession(runtime, created, "cloned");
    const forkedAgent = runtime.addAgent({
      session: forked,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Forked",
    });
    const clonedAgent = runtime.addAgent({
      session: cloned,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Cloned",
    });

    assert.equal(resumed.agentId, createdAgent.agentId);
    assert.notEqual(forkedAgent.agentId, createdAgent.agentId);
    assert.notEqual(clonedAgent.agentId, createdAgent.agentId);
    assert.notEqual(clonedAgent.agentId, forkedAgent.agentId);
    runtime.close();
  });

  it("persists membership, direct Spawners, capability configuration, and one Owner across nesting", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const owner = runtime.agent(runtime.workflow.ownerAgentId);
    const parentSession = scenario.childSession(runtime, "parent");
    const parent = runtime.addAgent({
      session: parentSession,
      spawner: owner,
      name: "Parent",
      agentDefinition: "worker",
      capabilities: { spawning: true },
    });
    scenario.clock.advance();
    const parentRuntime = scenario.startAgent(runtime.workflow, parentSession);
    const childSession = scenario.childSession(runtime, "nested-child");
    const child = parentRuntime.addAgent({
      session: childSession,
      spawner: parentRuntime.agent(parent.agentId),
      name: "Nested Child",
      agentDefinition: "scout",
      capabilities: { spawning: false },
    });

    runtime.restart();
    const reopenedParent = runtime.inspect(runtime.agent(parent.agentId));
    const reopenedChild = runtime.inspect(runtime.agent(child.agentId));

    assert.equal(reopenedParent.workflowOwnerId, owner.agentId);
    assert.equal(reopenedParent.spawnerAgentId, owner.agentId);
    assert.equal(reopenedParent.agentDefinition, "worker");
    assert.deepEqual(reopenedParent.capabilities, { spawning: true });
    assert.equal(reopenedChild.workflowOwnerId, owner.agentId);
    assert.equal(reopenedChild.spawnerAgentId, parent.agentId);
    assert.equal(reopenedChild.agentDefinition, "scout");
    assert.deepEqual(reopenedChild.capabilities, { spawning: false });
    assert.deepEqual(runtime.directChildren(owner).map((agent) => agent.agentId), [parent.agentId]);
    assert.deepEqual(runtime.directChildren(runtime.agent(parent.agentId)).map((agent) => agent.agentId), [child.agentId]);
    assert.deepEqual(new Set(runtime.descendants().map((agent) => agent.workflowOwnerId)), new Set([owner.agentId]));
    parentRuntime.close();
    runtime.close();
  });

  it("rejects spawning when the durable Spawner capability is disabled", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const parentSession = scenario.childSession(runtime, "non-spawner");
    const parent = runtime.addAgent({
      session: parentSession,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Non-spawner",
      capabilities: { spawning: false },
    });
    const rejectedChild = scenario.childSession(runtime, "rejected-child");
    const parentRuntime = scenario.startAgent(runtime.workflow, parentSession);

    assert.throws(
      () => parentRuntime.addAgent({
        session: rejectedChild,
        spawner: parentRuntime.agent(parent.agentId),
        name: "Rejected Child",
      }),
      (error: unknown) => (error as { code?: string }).code === "SpawnerCapabilityRequired",
    );
    assert.equal(runtime.descendants().some((agent) => agent.agentId === rejectedChild.agentId), false);
    parentRuntime.close();
    runtime.close();
  });

  it("rejects concurrent Agent Runs and fences stale mutations after ownership transfer", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "owned-agent");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Owned Agent",
    });
    const reference = runtime.agent(agent.agentId);
    const firstRun = runtime.startAgentRun(reference);

    assert.equal(scenario.processes.isActive(firstRun.processId), true);
    assert.throws(
      () => runtime.startAgentRun(reference),
      (error: unknown) => (error as { code?: string }).code === "AgentRunAlreadyOwned",
    );

    runtime.checkpoint(firstRun, "first");
    runtime.exitAgentRun(firstRun);
    const replacementRun = runtime.startAgentRun(reference);
    assert.ok(replacementRun.ownership.epoch > firstRun.ownership.epoch);
    assert.throws(
      () => runtime.checkpoint(firstRun, "stale"),
      (error: unknown) => (error as { code?: string }).code === "OwnershipLost",
    );
    runtime.checkpoint(replacementRun, "replacement");
    assert.deepEqual(runtime.readCheckpoint(reference), {
      value: "replacement",
      fencingEpoch: replacementRun.ownership.epoch,
    });

    runtime.exitAgentRun(replacementRun);
    runtime.close();
  });

  it("coordinates deterministic ownership across independent runtimes sharing durable state", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { session: owner, runtime: firstRuntime } = scenario.createOwner();
    const secondRuntime = scenario.startOwner(owner);
    const session = scenario.childSession(firstRuntime, "shared-agent");
    const agent = firstRuntime.addAgent({
      session,
      spawner: firstRuntime.agent(firstRuntime.workflow.ownerAgentId),
      name: "Shared Agent",
    });
    const firstReference = firstRuntime.agent(agent.agentId);
    const secondReference = secondRuntime.agent(agent.agentId);
    const firstRun = firstRuntime.startAgentRun(firstReference);

    assert.equal(secondRuntime.inspect(secondReference).agentId, agent.agentId);
    assert.throws(
      () => secondRuntime.startAgentRun(secondReference),
      (error: unknown) => (error as { code?: string }).code === "AgentRunAlreadyOwned",
    );

    firstRuntime.exitAgentRun(firstRun);
    const replacement = secondRuntime.startAgentRun(secondReference);
    assert.ok(replacement.ownership.epoch > firstRun.ownership.epoch);
    secondRuntime.exitAgentRun(replacement);
    firstRuntime.close();
    secondRuntime.close();
  });

  it("rejects cross-Workflow membership, direct targets, and ownership without partial effects", async () => {
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
    const ownerA = runtimeA.agent(runtimeA.workflow.ownerAgentId);
    const ownerB = runtimeB.agent(runtimeB.workflow.ownerAgentId);
    const candidate = workflowA.childSession(runtimeA, "candidate");
    const memberBSession = workflowB.childSession(runtimeB, "member-b");
    runtimeB.addAgent({ session: memberBSession, spawner: ownerB, name: "Member B" });
    const symlinkedMemberPath = join(runtimeA.workflow.sessionsDirectory, "linked-member-b.jsonl");
    await symlink(memberBSession.sessionPath, symlinkedMemberPath);
    const copiedMemberPath = join(runtimeA.workflow.sessionsDirectory, "copied-member-b.jsonl");
    await copyFile(memberBSession.sessionPath, copiedMemberPath);
    const before = runtimeA.descendants().map((agent) => agent.agentId);

    assert.throws(
      () => runtimeA.addAgent({ session: candidate, spawner: ownerB, name: "Wrong Workflow" }),
      (error: unknown) => (error as { code?: string }).code === "WorkflowMismatch",
    );
    assert.throws(
      () => runtimeA.authorizeDirectTarget(ownerA, ownerB),
      (error: unknown) => (error as { code?: string }).code === "WorkflowMismatch",
    );
    assert.throws(
      () => runtimeA.addAgent({ session: memberBSession, spawner: ownerA, name: "Imported B" }),
      (error: unknown) => (error as { code?: string }).code === "TranscriptOutsideWorkflow",
    );
    assert.throws(
      () => runtimeA.addAgent({
        session: { ...memberBSession, sessionPath: symlinkedMemberPath },
        spawner: ownerA,
        name: "Symlinked B",
      }),
      (error: unknown) => (error as { code?: string }).code === "TranscriptOutsideWorkflow",
    );
    assert.throws(
      () => runtimeA.addAgent({
        session: { ...memberBSession, sessionPath: copiedMemberPath },
        spawner: ownerA,
        name: "Copied B",
      }),
      (error: unknown) => (error as { code?: string }).code === "WorkflowMismatch",
    );
    assert.throws(
      () => runtimeA.startAgentRun(ownerB),
      (error: unknown) => (error as { code?: string }).code === "WorkflowMismatch",
    );
    assert.deepEqual(runtimeA.descendants().map((agent) => agent.agentId), before);
    assert.equal(workflowA.processes.processes.size, 0);

    runtimeA.close();
    runtimeB.close();
  });
});
