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

  it("records one undeclared-settlement correction instead of inferring human waiting", async () => {
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

    assert.deepEqual(runtime.settleActivation(run).state, {
      kind: "waiting",
      dependencies: [{ kind: "undeclared", dependencyId: "undeclared" }],
    });
    const episode = runtime.pendingUndeclaredNotice(reference);
    assert.ok(episode);
    assert.equal(runtime.confirmUndeclaredNotice(reference, episode.episodeId), true);
    runtime.activateTurn(run);
    runtime.settleActivation(run);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.repeatTriggered, true);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.triggerKind, "incident");

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("retains an undeclared notice as queued until transcript evidence confirms delivery", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);
    runtime.settleActivation(run);

    const episode = runtime.pendingUndeclaredNotice(reference)!;
    assert.equal(episode.noticeQueued, false);
    assert.equal(runtime.queueUndeclaredNotice(reference, episode.episodeId)?.noticeQueued, true);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.noticeDelivered, false);
    assert.equal(runtime.confirmUndeclaredNotice(reference, episode.episodeId), true);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.noticeDelivered, true);

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("keeps the bound response durable until matching tool-result persistence confirms it", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    assert.equal(runtime.beginHumanInterrupt(run, "ask-1").status, "pending");
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    assert.equal(runtime.hasHumanAttention(reference), true);
    scenario.clock.advance(86_400_000);
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    assert.equal(runtime.pendingUndeclaredNotice(reference), undefined);
    assert.equal(runtime.bindHumanResponse(run, "ask-1", "input-1")?.responseInputId, "input-1");
    assert.equal(runtime.hasHumanAttention(reference), false);
    const prepared = runtime.prepareHumanResponseResult(run, "ask-1");
    assert.equal(prepared.toolCallId, "ask-1");
    assert.equal(prepared.status, "result-pending");
    assert.equal(prepared.responseInputId, "input-1");
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    assert.equal(runtime.confirmHumanResponseResult(run, "ask-1")?.status, "consumed");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.responseInputId, "input-1");
    assert.equal(runtime.confirmHumanResponseResult(run, "ask-1"), undefined);

    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("makes an unconsumed prepared Human result stale when cancellation commits", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    runtime.beginHumanInterrupt(run, "ask-1");
    runtime.bindHumanResponse(run, "ask-1", "input-1");
    assert.equal(runtime.prepareHumanResponseResult(run, "ask-1").status, "result-pending");

    assert.equal(runtime.cancelActivation(run).state.outcome, "cancelled");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "terminal");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.responseInputId, undefined);
    assert.equal(runtime.confirmHumanResponseResult(run, "ask-1"), undefined);
    runtime.close();
  });

  it("prevents Human result preparation when cancellation commits first", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);

    runtime.beginHumanInterrupt(run, "ask-1");
    runtime.bindHumanResponse(run, "ask-1", "input-1");
    assert.equal(runtime.cancelActivation(run).state.outcome, "cancelled");

    assert.throws(
      () => runtime.prepareHumanResponseResult(run, "ask-1"),
      errorCode("OwnershipLost"),
    );
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "terminal");
    runtime.close();
  });

  it("replays a result-pending Human Interrupt before a recovery turn can continue", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(first, "ask-1");
    runtime.bindHumanResponse(first, "ask-1", "input-1");
    runtime.confirmAgentRunExit(first, { error: "crash after binding" });

    const resumed = runtime.startAgentRun(reference);
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    runtime.prepareHumanResponseResult(resumed, "ask-1");
    runtime.confirmAgentRunExit(resumed, { error: "crash before tool-result persistence" });

    const resultRecovery = runtime.startAgentRun(reference);
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting", dependencies: [{ kind: "human", dependencyId: "human" }],
    });
    assert.throws(() => runtime.activateTurn(resultRecovery), errorCode("InvalidLifecycleTransition"));
    runtime.resumeHumanResponseResult(resultRecovery, "ask-1");
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    assert.equal(runtime.confirmHumanResponseResult(resultRecovery, "ask-1")?.status, "consumed");
    runtime.beginHumanInterrupt(resultRecovery, "ask-2");
    assert.equal(runtime.cancelActivation(resultRecovery).state.outcome, "cancelled");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "terminal");
    runtime.close();
  });

  it("reconciles a persisted Human result after the prepared run crashes", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);

    runtime.beginHumanInterrupt(first, "ask-1");
    runtime.bindHumanResponse(first, "ask-1", "input-1");
    runtime.prepareHumanResponseResult(first, "ask-1");
    runtime.confirmAgentRunExit(first, { error: "crash after Pi persisted the tool result" });

    const recovered = runtime.startAgentRun(reference);
    assert.equal(runtime.confirmHumanResponseResult(recovered, "ask-1")?.status, "consumed");
    runtime.confirmAgentRunExit(recovered, { error: "test cleanup" });
    runtime.close();
  });

  it("does not bind stale Human input to a later interrupt", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const first = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(first, "ask-1");
    runtime.cancelActivation(first);
    const later = runtime.startAgentRun(reference);
    runtime.beginHumanInterrupt(later, "ask-2");
    assert.equal(runtime.bindHumanResponse(later, "ask-1", "stale-input"), undefined);
    assert.equal(runtime.inspectHumanInterrupt(reference)?.toolCallId, "ask-2");
    assert.equal(runtime.inspectHumanInterrupt(reference)?.status, "pending");
    runtime.confirmAgentRunExit(later, { error: "test cleanup" });
    runtime.close();
  });

  it("rejects Human Interrupts from Moderator contexts before effects", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "moderator");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Moderator" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);
    assert.throws(() => runtime.beginHumanInterrupt(run, "ask-1", "moderator"), errorCode("HumanInterruptForbidden"));
    assert.equal(runtime.inspectActivation(reference)?.state.kind, "active");
    runtime.settleActivation(run, undefined, "moderator");
    const episode = runtime.pendingUndeclaredNotice(reference)!;
    runtime.confirmUndeclaredNotice(reference, episode.episodeId);
    runtime.activateTurn(run);
    runtime.settleActivation(run, undefined, "moderator");
    runtime.activateTurn(run);
    runtime.settleActivation(run, undefined, "moderator");
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.triggerKind, "owner-handoff");
    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("does not reset an undeclared episode until its exact declared operation is satisfied", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);
    runtime.settleActivation(run);
    const episode = runtime.pendingUndeclaredNotice(reference)!;
    runtime.confirmUndeclaredNotice(reference, episode.episodeId);
    runtime.activateTurn(run);
    runtime.addActivationDependency(run, { kind: "operation", dependencyId: "still-running" });
    runtime.settleActivation(run);
    // A turn activation alone is not evidence that the operation completed.
    runtime.activateTurn(run);
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.status, "open");
    runtime.satisfyActivationDependency(run, { kind: "operation", dependencyId: "still-running" });
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.status, "closed");
    runtime.confirmAgentRunExit(run, { error: "test cleanup" });
    runtime.close();
  });

  it("resets an undeclared correction episode only after the Human tool result commits", async () => {
    const scenario = new WorkflowScenario({ rootDirectory: await temporaryDirectory() });
    const { runtime } = scenario.createOwner();
    const session = scenario.childSession(runtime, "worker");
    const agent = runtime.addAgent({ session, spawner: runtime.agent(runtime.workflow.ownerAgentId), name: "Worker" });
    const reference = runtime.agent(agent.agentId);
    const run = runtime.startAgentRun(reference);
    runtime.settleActivation(run);
    const episode = runtime.pendingUndeclaredNotice(reference)!;
    runtime.confirmUndeclaredNotice(reference, episode.episodeId);
    runtime.activateTurn(run);
    runtime.beginHumanInterrupt(run, "ask-1");
    runtime.bindHumanResponse(run, "ask-1", "input-1");
    runtime.prepareHumanResponseResult(run, "ask-1");
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.status, "open");
    runtime.confirmHumanResponseResult(run, "ask-1");
    assert.equal(runtime.inspectUndeclaredEpisode(reference)?.status, "closed");
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

  it("carries unresolved operation dependencies into recovery work", async () => {
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
    assert.deepEqual(runtime.inspectActivation(reference)?.state, {
      kind: "waiting",
      dependencies: [{ kind: "operation", dependencyId: "old-operation" }],
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
