import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, it } from "node:test";
import { WorkflowProtocolError } from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import { latestAssistantTurnWasAborted } from "../../pi-extension/subagents/protocol/pi-activation-events.ts";
import { WorkflowScenario } from "./scenario-harness.ts";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-activation-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })),
  );
});

function errorCode(code: string): (error: unknown) => boolean {
  return (error) => error instanceof WorkflowProtocolError && error.code === code;
}

describe("canonical activation lifecycle scenarios", () => {
  it("recognizes Pi abort evidence only from the latest assistant turn", () => {
    assert.equal(latestAssistantTurnWasAborted(undefined), false);
    assert.equal(latestAssistantTurnWasAborted([{ role: "assistant", stopReason: "stop" }]), false);
    assert.equal(
      latestAssistantTurnWasAborted([
        { role: "assistant", stopReason: "aborted" },
        { role: "user", content: "later input" },
      ]),
      true,
    );
    assert.equal(
      latestAssistantTurnWasAborted([
        { role: "assistant", stopReason: "aborted" },
        { role: "assistant", stopReason: "error" },
      ]),
      false,
    );
  });

  it("persists one active Subagent activation independently of runtime observation", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    const active = runtime.inspectActivation(reference);
    assert.equal(active?.activationId, run.ownership.runId);
    assert.equal(active?.state.kind, "active");

    scenario.processes.loseObservation(run.processId);
    runtime.restart();
    assert.deepEqual(runtime.inspectActivation(reference), active);
    assert.equal(runtime.currentAgentRun(reference)?.runId, run.ownership.runId);

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("derives typed waiting from independent Agent and operation dependencies", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    runtime.addActivationDependency(run, {
      kind: "agent",
      dependencyId: "request-a",
      agentId: "00000000-0000-4000-8000-0000000000aa",
    });
    runtime.addActivationDependency(run, {
      kind: "agent",
      dependencyId: "request-b",
      agentId: "00000000-0000-4000-8000-0000000000bb",
    });
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "acceptance-c",
    });

    const waiting = runtime.settleActivation(run);
    assert.deepEqual(waiting.state, {
      kind: "waiting",
      dependencies: [
        {
          kind: "agent",
          dependencyId: "request-a",
          agentId: "00000000-0000-4000-8000-0000000000aa",
        },
        {
          kind: "agent",
          dependencyId: "request-b",
          agentId: "00000000-0000-4000-8000-0000000000bb",
        },
        { kind: "operation", dependencyId: "acceptance-c" },
      ],
    });

    runtime.removeActivationDependency(run, {
      kind: "agent",
      dependencyId: "request-a",
    });
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting",
      dependencies: [
        {
          kind: "agent",
          dependencyId: "request-b",
          agentId: "00000000-0000-4000-8000-0000000000bb",
        },
        { kind: "operation", dependencyId: "acceptance-c" },
      ],
    });

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("derives human waiting when settlement has no Agent or operation dependency", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    const waiting = runtime.settleActivation(run);
    assert.deepEqual(waiting.state, {
      kind: "waiting",
      dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    assert.equal(scenario.processes.isActive(run.processId), true);
    assert.throws(() => runtime.startAgentRun(reference), errorCode("AgentRunAlreadyOwned"));

    runtime.activateTurn(run);
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("settles the Workflow Owner turn without creating a Subagent activation", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const owner = runtime.agent(runtime.workflow.ownerAgentId);

    assert.deepEqual(runtime.settleOwnerTurn(), { kind: "owner-turn-settled" });
    assert.equal(runtime.inspectActivation(owner), undefined);
    assert.throws(
      () => runtime.startAgentRun(owner),
      errorCode("OwnerActivationForbidden"),
    );

    runtime.close();
  });

  it("makes interruption canonical only after the active turn abort is confirmed", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    const request = runtime.requestInterruption(run);
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");

    const interrupted = runtime.confirmInterruption(run, request);
    assert.equal(interrupted.state.kind, "interrupted");

    runtime.activateTurn(run);
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    assert.throws(
      () => runtime.confirmInterruption(run, request),
      errorCode("StaleLifecycleTransition"),
    );
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("records confirmed process exit as ended(failed) and permits a new fenced activation", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const firstRun = runtime.startAgentRun(reference);

    runtime.reportUnconfirmedAgentRunExit(firstRun);
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    assert.equal(runtime.currentAgentRun(reference)?.runId, firstRun.ownership.runId);

    const failed = runtime.confirmAgentRunExit(firstRun, {
      error: "process exited without a terminal lifecycle commit",
      exitCode: 17,
    });
    assert.deepEqual(failed.state, {
      kind: "ended",
      outcome: "failed",
      error: "process exited without a terminal lifecycle commit",
      exitCode: 17,
    });
    assert.equal(runtime.currentAgentRun(reference), undefined);

    const replacement = runtime.startAgentRun(reference);
    assert.ok(replacement.ownership.epoch > firstRun.ownership.epoch);
    assert.equal(runtime.inspectActivation(reference)?.activationId, replacement.ownership.runId);
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");

    runtime.confirmAgentRunExit(replacement, { error: "test cleanup" });
    runtime.close();
  });

  it("does not leak dependencies from a failed activation into unrelated later work", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const firstRun = runtime.startAgentRun(reference);
    runtime.addActivationDependency(firstRun, {
      kind: "operation",
      dependencyId: "old-operation",
    });
    runtime.confirmAgentRunExit(firstRun, { error: "first activation failed" });

    const replacement = runtime.startAgentRun(reference);
    assert.deepEqual(runtime.settleActivation(replacement).state, {
      kind: "waiting",
      dependencies: [{ kind: "human", dependencyId: "human" }],
    });

    runtime.confirmAgentRunExit(replacement, { error: "test cleanup" });
    runtime.close();
  });

  it("rejects stale lifecycle revisions and stale run ownership without partial changes", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({
      session,
      spawner: runtime.agent(runtime.workflow.ownerAgentId),
      name: "Worker",
    });
    const reference = runtime.agent(agent.agentId);
    const firstRun = runtime.startAgentRun(reference);
    const first = runtime.inspectActivation(reference)!;

    runtime.addActivationDependency(firstRun, {
      kind: "operation",
      dependencyId: "operation-one",
    });
    const afterDependency = runtime.inspectActivation(reference)!;
    assert.throws(
      () => runtime.settleActivation(firstRun, first.revision),
      errorCode("StaleLifecycleTransition"),
    );
    assert.deepEqual(runtime.inspectActivation(reference), afterDependency);

    runtime.confirmAgentRunExit(firstRun, { error: "first failed" });
    const replacement = runtime.startAgentRun(reference);
    const replacementState = runtime.inspectActivation(reference)!;
    assert.throws(
      () => runtime.settleActivation(firstRun),
      errorCode("OwnershipLost"),
    );
    assert.deepEqual(runtime.inspectActivation(reference), replacementState);

    runtime.confirmAgentRunExit(replacement, { error: "test cleanup" });
    runtime.close();
  });
});
