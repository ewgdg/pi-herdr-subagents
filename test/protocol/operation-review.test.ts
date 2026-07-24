import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { WorkflowScenario } from "./scenario-harness.ts";
import { WorkflowBootstrap } from "../../pi-extension/subagents/protocol/workflow-bootstrap.ts";
import {
  ActivationCancellationService,
  CancellationInDoubtError,
} from "../../pi-extension/subagents/protocol/activation-cancellation.ts";

describe("Operation Review", () => {
  it("reviews the same dependency ID independently across concurrent activations", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-concurrent-")),
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const firstSession = scenario.childSession(runtime, "first");
    const secondSession = scenario.childSession(runtime, "second");
    const first = runtime.addAgent({
      session: firstSession,
      spawner: runtime.owner(),
      name: "First",
    });
    const second = runtime.addAgent({
      session: secondSession,
      spawner: runtime.owner(),
      name: "Second",
    });
    const firstRun = runtime.startAgentRun(runtime.agent(first.agentId));
    const secondRun = runtime.startAgentRun(runtime.agent(second.agentId));
    const sharedDependency = {
      kind: "operation" as const,
      dependencyId: "side-effect:shared-provider-key",
    };

    runtime.addActivationDependency(firstRun, sharedDependency);
    runtime.addActivationDependency(secondRun, sharedDependency);
    runtime.settleActivation(firstRun);
    runtime.settleActivation(secondRun);

    const firstReview = (runtime.inspectTarget({ agent: first.agentId }) as any)
      .operationReviews[0];
    const secondReview = (runtime.inspectTarget({ agent: second.agentId }) as any)
      .operationReviews[0];
    assert.notEqual(firstReview.operationReviewId, secondReview.operationReviewId);
    assert.equal(firstReview.dependencyId, sharedDependency.dependencyId);
    assert.equal(secondReview.dependencyId, sharedDependency.dependencyId);
    assert.deepEqual(
      runtime.listWorkflowAttention().map((attention) => attention.operationReviewId),
      [firstReview.operationReviewId, secondReview.operationReviewId],
    );

    runtime.satisfyActivationDependency(firstRun, sharedDependency);

    assert.equal(
      (runtime.inspectTarget({ agent: first.agentId }) as any).operationReviews,
      undefined,
    );
    assert.deepEqual(
      (runtime.inspectTarget({ agent: second.agentId }) as any).operationReviews,
      [secondReview],
    );
    assert.deepEqual(runtime.listWorkflowAttention(), [{
      kind: "WATCH",
      key: `operation-review:${secondReview.operationReviewId}`,
      operationReviewId: secondReview.operationReviewId,
      agentId: second.agentId,
      dependencyId: sharedDependency.dependencyId,
      reviewDeadlineAtMs: secondReview.reviewDeadlineAtMs,
    }]);
  });

  it("starts a fresh episode when a resolved dependency ID is reused", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-reused-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 2_000,
        intervalsMs: {
          acceptance: 2_000,
          cancellation: 2_000,
          ownership: 2_000,
          "external-side-effect": 2_000,
          generic: 2_000,
        },
      },
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    const dependency = {
      kind: "operation" as const,
      dependencyId: "side-effect:reused-provider-key",
    };
    runtime.addActivationDependency(run, dependency);
    runtime.settleActivation(run);
    const firstReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    runtime.recordOperationEvidence(firstReviewId, {
      kind: "provider-probe",
      detail: "The first operation is being inspected",
    });
    await runtime.reconcileOperationReviews(() => ({
      kind: "resolved",
      evidence: {
        kind: "provider-result",
        detail: "The first operation committed",
      },
    }));
    const firstHistory = runtime.inspectOperationReview(firstReviewId);

    scenario.clock.advance(400);
    runtime.addActivationDependency(run, dependency);
    runtime.settleActivation(run);
    const secondProjection = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0];
    const secondReview = runtime.inspectOperationReview(secondProjection.operationReviewId);

    assert.notEqual(secondProjection.operationReviewId, firstReviewId);
    assert.deepEqual(secondReview, {
      operationReviewId: secondProjection.operationReviewId,
      dependencyId: dependency.dependencyId,
      operationKind: "external-side-effect",
      originalIdentity: "reused-provider-key",
      agentId: child.agentId,
      activationId: run.ownership.runId,
      ownership: {
        runId: run.ownership.runId,
        fencingEpoch: run.ownership.epoch,
      },
      status: "reconciling",
      reviewStartedAtMs: 1_400,
      reviewDeadlineAtMs: 3_400,
      reconciliationAttempts: 0,
      evidenceCount: 0,
    });
    assert.deepEqual(runtime.inspectOperationReview(firstReviewId), firstHistory);
    assert.equal(firstHistory?.status, "resolved");
    assert.equal(firstHistory?.evidenceCount, 2);
    assert.deepEqual(runtime.listWorkflowAttention(), [{
      kind: "WATCH",
      key: `operation-review:${secondProjection.operationReviewId}`,
      operationReviewId: secondProjection.operationReviewId,
      agentId: child.agentId,
      dependencyId: dependency.dependencyId,
      reviewDeadlineAtMs: 3_400,
    }]);
  });

  it("preserves review state when the reconciliation callback throws", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-probe-error-")),
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "ownership:probe-error",
    });
    runtime.settleActivation(run);
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    const before = runtime.inspectOperationReview(operationReviewId);
    const sentinel = new Error("sentinel reconciliation failure");

    await assert.rejects(
      runtime.reconcileOperationReviews(() => {
        throw sentinel;
      }),
      (error: unknown) => error === sentinel,
    );

    assert.deepEqual(runtime.inspectOperationReview(operationReviewId), before);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "ownership:probe-error" }],
      },
    );
  });

  it("rolls back unchanged when a reconciliation outcome is malformed", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-malformed-outcome-")),
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "side-effect:malformed-outcome",
    });
    runtime.settleActivation(run);
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    const before = runtime.inspectOperationReview(operationReviewId);

    await assert.rejects(
      runtime.reconcileOperationReviews(() => (
        { kind: "resolved" } as never
      )),
    );

    assert.deepEqual(runtime.inspectOperationReview(operationReviewId), before);
    assert.deepEqual(runtime.listOperationIncidentTriggers(), []);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "side-effect:malformed-outcome" }],
      },
    );
  });

  it("assigns a capped runtime deadline and projects WATCH without activity renewal", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 500,
        intervalsMs: {
          acceptance: 2_000,
          cancellation: 3_000,
          ownership: 4_000,
          "external-side-effect": 5_000,
          generic: 6_000,
        },
      },
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));

    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "acceptance:message-1",
    });
    runtime.settleActivation(run);

    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    const created = runtime.inspectOperationReview(operationReviewId);
    assert.deepEqual(created, {
      operationReviewId,
      dependencyId: "acceptance:message-1",
      operationKind: "acceptance",
      originalIdentity: "message-1",
      agentId: child.agentId,
      activationId: run.ownership.runId,
      ownership: {
        runId: run.ownership.runId,
        fencingEpoch: run.ownership.epoch,
      },
      status: "reconciling",
      reviewStartedAtMs: 1_000,
      reviewDeadlineAtMs: 1_500,
      reconciliationAttempts: 0,
      evidenceCount: 0,
    });
    assert.deepEqual(runtime.listWorkflowAttention(), [{
      kind: "WATCH",
      key: `operation-review:${operationReviewId}`,
      operationReviewId,
      agentId: child.agentId,
      dependencyId: "acceptance:message-1",
      reviewDeadlineAtMs: 1_500,
    }]);
    assert.deepEqual(
      (runtime.inspectTarget({ agent: child.agentId }) as any).operationReviews,
      [{
        operationReviewId,
        dependencyId: "acceptance:message-1",
        operationKind: "acceptance",
        status: "reconciling",
        reviewDeadlineAtMs: 1_500,
        reconciliationAttempts: 0,
        evidenceCount: 0,
      }],
    );
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "acceptance:message-1" }],
      },
    );

    scenario.clock.advance(400);
    runtime.recordOperationEvidence(operationReviewId, {
      kind: "heartbeat",
      detail: "The process is still producing output",
    });
    runtime.recordOperationEvidence(operationReviewId, {
      kind: "tool-timeout-argument",
      detail: "Agent supplied timeout=999999",
    });

    assert.deepEqual(runtime.inspectOperationReview(operationReviewId), {
      ...created,
      evidenceCount: 2,
      latestEvidence: {
        kind: "tool-timeout-argument",
        detail: "Agent supplied timeout=999999",
        observedAtMs: 1_400,
      },
    });
    assert.equal(
      runtime.inspectOperationReview(operationReviewId)?.reviewDeadlineAtMs,
      1_500,
      "output, heartbeats, runtime activity, and tool arguments never renew review policy",
    );
  });

  it("keeps expired uncertainty unresolved and emits one incident trigger after reconciliation", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-expiry-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 500,
        intervalsMs: {
          acceptance: 500,
          cancellation: 500,
          ownership: 500,
          "external-side-effect": 500,
          generic: 500,
        },
      },
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "ownership:handoff-1",
    });
    runtime.settleActivation(run);
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    scenario.clock.advance(500);
    let probes = 0;

    await runtime.reconcileOperationReviews(async (review) => {
      probes += 1;
      assert.equal(review.dependencyId, "ownership:handoff-1");
      assert.equal(review.originalIdentity, "handoff-1");
      assert.deepEqual(review.ownership, {
        runId: run.ownership.runId,
        fencingEpoch: run.ownership.epoch,
      });
      return {
        kind: "unresolved",
        eligibility: "eligible",
        evidence: {
          kind: "ownership-probe",
          detail: "The exact current owner cannot be established",
        },
      };
    });

    assert.equal(probes, 1);
    assert.deepEqual(runtime.inspectOperationReview(operationReviewId), {
      operationReviewId,
      dependencyId: "ownership:handoff-1",
      operationKind: "ownership",
      originalIdentity: "handoff-1",
      agentId: child.agentId,
      activationId: run.ownership.runId,
      ownership: {
        runId: run.ownership.runId,
        fencingEpoch: run.ownership.epoch,
      },
      status: "awaiting-judgment",
      reviewStartedAtMs: 1_000,
      reviewDeadlineAtMs: 1_500,
      reconciliationAttempts: 1,
      evidenceCount: 1,
      latestEvidence: {
        kind: "ownership-probe",
        detail: "The exact current owner cannot be established",
        observedAtMs: 1_500,
      },
    });
    assert.deepEqual(runtime.listWorkflowAttention(), []);
    assert.deepEqual(runtime.listOperationIncidentTriggers(), [{
      triggerKey: `operation-review:${operationReviewId}`,
      operationReviewId,
      dependencyId: "ownership:handoff-1",
      reason: "review-deadline-expired",
      triggeredAtMs: 1_500,
    }]);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "ownership:handoff-1" }],
      },
      "elapsed time and incident triggering must not invent ownership transfer",
    );

    await runtime.reconcileOperationReviews(async () => {
      probes += 1;
      throw new Error("an awaiting-judgment episode must not be probed again");
    });
    assert.equal(probes, 1);
    assert.equal(runtime.listOperationIncidentTriggers().length, 1);
  });

  it("clears WATCH and the exact dependency only when reconciliation establishes a result", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-resolved-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 2_000,
        intervalsMs: {
          acceptance: 2_000,
          cancellation: 2_000,
          ownership: 2_000,
          "external-side-effect": 2_000,
          generic: 2_000,
        },
      },
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "side-effect:charge-1",
    });
    runtime.settleActivation(run);
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    scenario.clock.advance(250);

    await runtime.reconcileOperationReviews((review) => {
      assert.equal(review.dependencyId, "side-effect:charge-1");
      return {
        kind: "resolved",
        evidence: {
          kind: "provider-idempotency-probe",
          detail: "Provider confirms charge ch_1 committed once",
        },
      };
    });

    assert.deepEqual(runtime.inspectOperationReview(operationReviewId), {
      operationReviewId,
      dependencyId: "side-effect:charge-1",
      operationKind: "external-side-effect",
      originalIdentity: "charge-1",
      agentId: child.agentId,
      activationId: run.ownership.runId,
      ownership: {
        runId: run.ownership.runId,
        fencingEpoch: run.ownership.epoch,
      },
      status: "resolved",
      reviewStartedAtMs: 1_000,
      reviewDeadlineAtMs: 3_000,
      reconciliationAttempts: 1,
      evidenceCount: 1,
      latestEvidence: {
        kind: "provider-idempotency-probe",
        detail: "Provider confirms charge ch_1 committed once",
        observedAtMs: 1_250,
      },
    });
    assert.deepEqual(runtime.listWorkflowAttention(), []);
    assert.deepEqual(runtime.listOperationIncidentTriggers(), []);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      { kind: "active" },
    );
  });

  it("moves one review to recovery ownership without resetting identity or deadline", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-recovery-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 2_000,
        intervalsMs: {
          acceptance: 2_000,
          cancellation: 2_000,
          ownership: 2_000,
          "external-side-effect": 2_000,
          generic: 2_000,
        },
      },
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const first = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(first, {
      kind: "operation",
      dependencyId: "acceptance:message-recovery",
    });
    runtime.settleActivation(first);
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    scenario.clock.advance(200);
    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved",
      eligibility: "eligible",
      evidence: {
        kind: "same-message-identity-probe",
        detail: "The original Message Identity remains uncertain",
      },
    }));
    runtime.confirmAgentRunExit(first, { error: "process exited during acceptance" });
    const replacement = runtime.startAgentRun(runtime.agent(child.agentId));

    const review = runtime.inspectOperationReview(operationReviewId);
    assert.equal(review?.operationReviewId, operationReviewId);
    assert.equal(review?.originalIdentity, "message-recovery");
    assert.equal(review?.reviewStartedAtMs, 1_000);
    assert.equal(review?.reviewDeadlineAtMs, 3_000);
    assert.equal(review?.reconciliationAttempts, 1);
    assert.equal(review?.evidenceCount, 1);
    assert.deepEqual(review?.latestEvidence, {
      kind: "same-message-identity-probe",
      detail: "The original Message Identity remains uncertain",
      observedAtMs: 1_200,
    });
    assert.equal(review?.activationId, replacement.ownership.runId);
    assert.deepEqual(review?.ownership, {
      runId: replacement.ownership.runId,
      fencingEpoch: replacement.ownership.epoch,
    });
    assert.deepEqual(runtime.listWorkflowAttention(), [{
      kind: "WATCH",
      key: `operation-review:${operationReviewId}`,
      operationReviewId,
      agentId: child.agentId,
      dependencyId: "acceptance:message-recovery",
      reviewDeadlineAtMs: 3_000,
    }]);

    await runtime.reconcileOperationReviews(() => ({
      kind: "resolved",
      evidence: {
        kind: "same-message-identity-probe",
        detail: "The original Message Identity is durably accepted",
      },
    }));
    assert.equal(
      runtime.inspectOperationReview(operationReviewId)?.status,
      "resolved",
    );
    assert.deepEqual(runtime.listOperationIncidentTriggers(), []);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      { kind: "active" },
    );
  });

  it("emits one trigger when deterministic reconciliation is exhausted before the deadline", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-exhausted-")),
    });
    const { runtime } = scenario.createOwner();
    test.after(() => runtime.close());
    const childSession = scenario.childSession(runtime, "child");
    const child = runtime.addAgent({
      session: childSession,
      spawner: runtime.owner(),
      name: "Child",
    });
    const run = runtime.startAgentRun(runtime.agent(child.agentId));
    runtime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "cancellation:cancel-1",
    });
    const operationReviewId = (runtime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;

    await runtime.reconcileOperationReviews(() => ({
      kind: "unresolved",
      eligibility: "exhausted",
      evidence: {
        kind: "cancellation-probe",
        detail: "No safe deterministic probe remains",
      },
    }));
    await runtime.reconcileOperationReviews(() => {
      throw new Error("exhausted reconciliation must not repeat");
    });

    assert.equal(
      runtime.inspectOperationReview(operationReviewId)?.status,
      "awaiting-judgment",
    );
    assert.deepEqual(runtime.listWorkflowAttention(), []);
    assert.deepEqual(runtime.listOperationIncidentTriggers(), [{
      triggerKey: `operation-review:${operationReviewId}`,
      operationReviewId,
      dependencyId: "cancellation:cancel-1",
      reason: "reconciliation-exhausted",
      triggeredAtMs: 1_000,
    }]);
    assert.deepEqual(
      runtime.inspectActivation(runtime.agent(child.agentId))?.state,
      { kind: "active" },
      "incident triggering does not settle, cancel, or complete the activation",
    );
  });

  it("rejects Owner router startup unchanged when extension reviews lack an adapter", async (test) => {
    for (const operation of [
      { dependencyId: "ownership:missing-adapter", operationKind: "ownership" },
      {
        dependencyId: "side-effect:missing-adapter",
        operationKind: "external-side-effect",
      },
      { dependencyId: "generic-missing-adapter", operationKind: "generic" },
    ] as const) {
      await test.test(operation.operationKind, async (subtest) => {
        const scenario = new WorkflowScenario({
          rootDirectory: await mkdtemp(join(
            tmpdir(),
            `operation-review-missing-${operation.operationKind}-`,
          )),
        });
        const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
        const childSession = scenario.childSession(ownerRuntime, "child");
        const child = ownerRuntime.addAgent({
          session: childSession,
          spawner: ownerRuntime.owner(),
          name: "Child",
        });
        const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
        const observer = scenario.startAgent(ownerRuntime.workflow, childSession);
        subtest.after(() => observer.close());
        ownerRuntime.addActivationDependency(run, {
          kind: "operation",
          dependencyId: operation.dependencyId,
        });
        ownerRuntime.settleActivation(run);
        const operationReviewId = (ownerRuntime.inspectTarget({ agent: child.agentId }) as any)
          .operationReviews[0].operationReviewId as number;
        const before = ownerRuntime.inspectOperationReview(operationReviewId);
        const beforeActivation = ownerRuntime.inspectActivation(ownerRuntime.agent(child.agentId));
        const beforeWatch = ownerRuntime.listWorkflowAttention();
        const beforeTriggers = ownerRuntime.listOperationIncidentTriggers();
        ownerRuntime.close();

        const bootstrap = new WorkflowBootstrap({ now: scenario.clock.now });
        subtest.after(() => bootstrap.close());
        bootstrap.sessionStarted({
          sessionManager: {
            getSessionId: () => ownerSession.agentId,
            getSessionFile: () => ownerSession.sessionPath,
          },
        }, {});

        await assert.rejects(
          bootstrap.startDirectSignalRouter({ projectInboxBatch() {} }),
          new RegExp(
            `Operation Review extension adapter is required for ${operation.operationKind} review ${operationReviewId}`,
          ),
        );

        assert.deepEqual(observer.inspectOperationReview(operationReviewId), before);
        assert.deepEqual(
          bootstrap.inspectActivation(child.agentId),
          beforeActivation,
        );
        assert.deepEqual(bootstrap.listWorkflowAttention(), beforeWatch);
        assert.deepEqual(bootstrap.listOperationIncidentTriggers(), beforeTriggers);
      });
    }
  });

  it("dispatches extension reviews with their exact identity and current fence", async (test) => {
    for (const operation of [
      { dependencyId: "ownership:adapter-identity", operationKind: "ownership" },
      {
        dependencyId: "side-effect:adapter-identity",
        operationKind: "external-side-effect",
      },
      { dependencyId: "generic:adapter-identity", operationKind: "generic" },
    ] as const) {
      await test.test(operation.operationKind, async (subtest) => {
        const scenario = new WorkflowScenario({
          rootDirectory: await mkdtemp(join(
            tmpdir(),
            `operation-review-adapter-${operation.operationKind}-`,
          )),
        });
        const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
        const childSession = scenario.childSession(ownerRuntime, "child");
        const child = ownerRuntime.addAgent({
          session: childSession,
          spawner: ownerRuntime.owner(),
          name: "Child",
        });
        const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
        const observer = scenario.startAgent(ownerRuntime.workflow, childSession);
        subtest.after(() => observer.close());
        ownerRuntime.addActivationDependency(run, {
          kind: "operation",
          dependencyId: operation.dependencyId,
        });
        ownerRuntime.settleActivation(run);
        const operationReviewId = (ownerRuntime.inspectTarget({ agent: child.agentId }) as any)
          .operationReviews[0].operationReviewId as number;
        ownerRuntime.close();
        const observed: Array<{
          operationReviewId: number;
          operationKind: string;
          originalIdentity: string;
          ownership: { runId: string; fencingEpoch: number };
        }> = [];
        const bootstrap = new WorkflowBootstrap({
          now: scenario.clock.now,
          extensionOperationReconciler: (review) => {
            observed.push({
              operationReviewId: review.operationReviewId,
              operationKind: review.operationKind,
              originalIdentity: review.originalIdentity,
              ownership: review.ownership,
            });
            return {
              kind: "resolved",
              evidence: {
                kind: "extension-adapter-probe",
                detail: "The registered adapter established the operation result",
              },
            };
          },
        });
        subtest.after(() => bootstrap.close());
        bootstrap.sessionStarted({
          sessionManager: {
            getSessionId: () => ownerSession.agentId,
            getSessionFile: () => ownerSession.sessionPath,
          },
        }, {});

        await bootstrap.startDirectSignalRouter({ projectInboxBatch() {} });

        assert.deepEqual(observed, [{
          operationReviewId,
          operationKind: operation.operationKind,
          originalIdentity: "adapter-identity",
          ownership: {
            runId: run.ownership.runId,
            fencingEpoch: run.ownership.epoch,
          },
        }]);
        assert.equal(
          (bootstrap.inspectTarget({ agent: child.agentId }) as any).operationReviews,
          undefined,
        );
        assert.deepEqual(
          bootstrap.inspectActivation(child.agentId)?.state,
          { kind: "active" },
        );
        assert.equal(observer.inspectOperationReview(operationReviewId)?.status, "resolved");
        assert.deepEqual(bootstrap.listOperationIncidentTriggers(), []);
      });
    }
  });

  it("does not apply an extension outcome after recovery transfers the ownership fence", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-extension-race-")),
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    test.after(() => ownerRuntime.close());
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const original = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    ownerRuntime.addActivationDependency(original, {
      kind: "operation",
      dependencyId: "ownership:recovery-race",
    });
    ownerRuntime.settleActivation(original);
    const operationReviewId = (ownerRuntime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    const before = ownerRuntime.inspectOperationReview(operationReviewId)!;
    let adapterStartedResolve!: () => void;
    const adapterStarted = new Promise<void>((resolve) => {
      adapterStartedResolve = resolve;
    });
    let releaseAdapterResolve!: () => void;
    const releaseAdapter = new Promise<void>((resolve) => {
      releaseAdapterResolve = resolve;
    });
    const bootstrap = new WorkflowBootstrap({
      now: scenario.clock.now,
      extensionOperationReconciler: async (review) => {
        assert.equal(review.operationReviewId, operationReviewId);
        assert.deepEqual(review.ownership, before.ownership);
        adapterStartedResolve();
        await releaseAdapter;
        return {
          kind: "resolved",
          evidence: {
            kind: "stale-extension-probe",
            detail: "This result was computed under the original fence",
          },
        };
      },
    });
    test.after(() => bootstrap.close());
    bootstrap.sessionStarted({
      sessionManager: {
        getSessionId: () => ownerSession.agentId,
        getSessionFile: () => ownerSession.sessionPath,
      },
    }, {});
    const startup = bootstrap.startDirectSignalRouter({ projectInboxBatch() {} });
    await adapterStarted;

    ownerRuntime.confirmAgentRunExit(original, {
      error: "the original run failed while the extension probe was in flight",
    });
    const replacement = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    releaseAdapterResolve();
    await startup;

    const after = ownerRuntime.inspectOperationReview(operationReviewId)!;
    assert.equal(after.operationReviewId, operationReviewId);
    assert.equal(after.activationId, replacement.ownership.runId);
    assert.deepEqual(after.ownership, {
      runId: replacement.ownership.runId,
      fencingEpoch: replacement.ownership.epoch,
    });
    assert.equal(after.status, "reconciling");
    assert.equal(after.reconciliationAttempts, before.reconciliationAttempts);
    assert.equal(after.evidenceCount, before.evidenceCount);
    assert.deepEqual(
      ownerRuntime.inspectActivation(ownerRuntime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{ kind: "operation", dependencyId: "ownership:recovery-race" }],
      },
    );
    assert.deepEqual(ownerRuntime.listWorkflowAttention(), [{
      kind: "WATCH",
      key: `operation-review:${operationReviewId}`,
      operationReviewId,
      agentId: child.agentId,
      dependencyId: "ownership:recovery-race",
      reviewDeadlineAtMs: before.reviewDeadlineAtMs,
    }]);
    assert.deepEqual(ownerRuntime.listOperationIncidentTriggers(), []);
  });

  it("reports a due extension review without an adapter and leaves it unchanged", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-due-extension-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 20,
        intervalsMs: {
          acceptance: 20,
          cancellation: 20,
          ownership: 20,
          "external-side-effect": 20,
          generic: 20,
        },
      },
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    test.after(() => ownerRuntime.close());
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    const bootstrap = new WorkflowBootstrap({ now: scenario.clock.now });
    test.after(() => bootstrap.close());
    bootstrap.sessionStarted({
      sessionManager: {
        getSessionId: () => ownerSession.agentId,
        getSessionFile: () => ownerSession.sessionPath,
      },
    }, {});
    await bootstrap.startDirectSignalRouter({ projectInboxBatch() {} });

    ownerRuntime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "generic:appeared-after-startup",
    });
    ownerRuntime.settleActivation(run);
    const operationReviewId = (ownerRuntime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    const before = ownerRuntime.inspectOperationReview(operationReviewId);
    const beforeWatch = ownerRuntime.listWorkflowAttention();
    scenario.clock.advance(20);
    let warningObservedResolve!: () => void;
    const warningObserved = new Promise<void>((resolve) => {
      warningObservedResolve = resolve;
    });
    const warningListener = (warning: Error) => {
      if (!warning.message.includes(
        `Operation Review extension adapter is required for generic review ${operationReviewId}`,
      )) return;
      warningObservedResolve();
    };
    process.on("warning", warningListener);
    test.after(() => process.off("warning", warningListener));
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(
        () => reject(new Error("Due extension configuration failure was not reported")),
        1_500,
      );
    });
    try {
      await Promise.race([warningObserved, timeout]);
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }

    assert.deepEqual(ownerRuntime.inspectOperationReview(operationReviewId), before);
    assert.deepEqual(ownerRuntime.listWorkflowAttention(), beforeWatch);
    assert.deepEqual(ownerRuntime.listOperationIncidentTriggers(), []);
    assert.deepEqual(
      ownerRuntime.inspectActivation(ownerRuntime.agent(child.agentId))?.state,
      {
        kind: "waiting",
        dependencies: [{
          kind: "operation",
          dependencyId: "generic:appeared-after-startup",
        }],
      },
    );
  });

  it("restarts workflow-wide reconciliation when the Owner resumes without duplicating the episode", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-owner-resume-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 500,
        intervalsMs: {
          acceptance: 500,
          cancellation: 500,
          ownership: 500,
          "external-side-effect": 500,
          generic: 500,
        },
      },
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    const childRuntime = scenario.startAgent(ownerRuntime.workflow, childSession);
    test.after(() => childRuntime.close());
    ownerRuntime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "ownership:offline-owner",
    });
    ownerRuntime.settleActivation(run);
    ownerRuntime.close();
    await assert.rejects(
      childRuntime.reconcileOperationReviews(() => ({
        kind: "resolved",
        evidence: { kind: "forbidden", detail: "child attempted Workflow review" },
      })),
      /Only the live Workflow Owner/,
    );
    scenario.clock.advance(500);
    let probes = 0;
    const sessionManager = {
      getSessionId: () => ownerSession.agentId,
      getSessionFile: () => ownerSession.sessionPath,
    };
    const reconcile = async () => {
      probes += 1;
      return {
        kind: "unresolved" as const,
        eligibility: "eligible" as const,
        evidence: {
          kind: "owner-resume-probe",
          detail: "The ownership result remains unknown",
        },
      };
    };

    const firstResume = new WorkflowBootstrap({
      now: scenario.clock.now,
      extensionOperationReconciler: reconcile,
    });
    firstResume.sessionStarted({ sessionManager }, {});
    await firstResume.startDirectSignalRouter({ projectInboxBatch() {} });
    await firstResume.closeDirectSignalRouter();
    firstResume.close();

    const secondResume = new WorkflowBootstrap({
      now: scenario.clock.now,
      extensionOperationReconciler: reconcile,
    });
    test.after(() => secondResume.close());
    secondResume.sessionStarted({ sessionManager }, {});
    await secondResume.startDirectSignalRouter({ projectInboxBatch() {} });

    assert.equal(probes, 1);
    assert.equal(
      (secondResume.inspectTarget({ agent: child.agentId }) as any)
        .operationReviews[0].status,
      "awaiting-judgment",
    );
    assert.equal(secondResume.listOperationIncidentTriggers().length, 1);
  });

  it("reports a scheduled reconciliation failure and retries while the Owner stays live", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-scheduled-retry-")),
      operationReviewPolicy: {
        maximumUnattendedIntervalMs: 20,
        intervalsMs: {
          acceptance: 20,
          cancellation: 20,
          ownership: 20,
          "external-side-effect": 20,
          generic: 20,
        },
      },
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    ownerRuntime.addActivationDependency(run, {
      kind: "operation",
      dependencyId: "ownership:transient-scheduled-failure",
    });
    ownerRuntime.settleActivation(run);
    const operationReviewId = (ownerRuntime.inspectTarget({ agent: child.agentId }) as any)
      .operationReviews[0].operationReviewId as number;
    ownerRuntime.close();

    const warnings: string[] = [];
    let warningObservedResolve!: () => void;
    const warningObserved = new Promise<void>((resolve) => {
      warningObservedResolve = resolve;
    });
    const warningListener = (warning: Error) => {
      if (!warning.message.includes("Scheduled Operation Review reconciliation failed")) return;
      warnings.push(warning.message);
      warningObservedResolve();
    };
    process.on("warning", warningListener);
    test.after(() => process.off("warning", warningListener));

    let probes = 0;
    let progressObservedResolve!: () => void;
    const progressObserved = new Promise<void>((resolve) => {
      progressObservedResolve = resolve;
    });
    const bootstrap = new WorkflowBootstrap({
      now: scenario.clock.now,
      extensionOperationReconciler: () => {
        probes += 1;
        if (probes === 1) {
          return {
            kind: "unresolved",
            eligibility: "eligible",
            evidence: {
              kind: "initial-probe",
              detail: "The first pass schedules the deadline probe",
            },
          };
        }
        if (probes === 2) throw new Error("transient scheduled probe failure");
        if (probes === 3) {
          progressObservedResolve();
          return {
            kind: "resolved",
            evidence: {
              kind: "retry-probe",
              detail: "The retry established the ownership result",
            },
          };
        }
        throw new Error("Operation Review scheduled duplicate reconciliation work");
      },
    });
    test.after(() => bootstrap.close());
    bootstrap.sessionStarted({
      sessionManager: {
        getSessionId: () => ownerSession.agentId,
        getSessionFile: () => ownerSession.sessionPath,
      },
    }, {});
    await bootstrap.startDirectSignalRouter({ projectInboxBatch() {} });
    scenario.clock.advance(20);

    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(
        () => reject(new Error("Scheduled Operation Review reconciliation did not retry")),
        1_500,
      );
    });
    try {
      await Promise.race([Promise.all([warningObserved, progressObserved]), timeout]);
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));

    assert.equal(probes, 3);
    assert.equal(warnings.length, 1);
    assert.match(warnings[0]!, /transient scheduled probe failure/);
    assert.equal(
      (bootstrap.inspectTarget({ agent: child.agentId }) as any).operationReviews,
      undefined,
    );
    assert.deepEqual(bootstrap.listOperationIncidentTriggers(), []);
    assert.equal(operationReviewId > 0, true);
  });

  it("reconciles an in-doubt cancellation by probing the original fenced process", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-cancellation-")),
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const run = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    ownerRuntime.checkpoint(run, JSON.stringify({ surface: "cancellation-pane" }));
    const service = new ActivationCancellationService({
      databasePath: ownerRuntime.workflow.databasePath,
      actor: ownerRuntime.owner(),
      terminator: {
        async inspect() {
          return { kind: "unavailable", error: "process service offline" };
        },
        async close() {
          throw new Error("close must not run after unavailable inspection");
        },
      },
      now: scenario.clock.now,
      allocateOperationId: () => "cancel-original",
    });
    await assert.rejects(
      service.cancel({
        target: ownerRuntime.agent(child.agentId),
        sourceId: "cancel-tool-call",
      }),
      (error: unknown) => error instanceof CancellationInDoubtError,
    );
    service.close();
    ownerRuntime.close();
    let closes = 0;
    const resumed = new WorkflowBootstrap({
      now: scenario.clock.now,
      agentRunTerminator: {
        async inspect(locator) {
          assert.deepEqual(locator, { surface: "cancellation-pane" });
          return { kind: "missing" };
        },
        async close() {
          closes += 1;
        },
      },
    });
    test.after(() => resumed.close());
    resumed.sessionStarted({
      sessionManager: {
        getSessionId: () => ownerSession.agentId,
        getSessionFile: () => ownerSession.sessionPath,
      },
    }, {});
    await resumed.startDirectSignalRouter({ projectInboxBatch() {} });

    const projection = resumed.inspectTarget({ agent: child.agentId }) as any;
    assert.deepEqual(projection.state, { kind: "ended", outcome: "cancelled" });
    assert.equal(projection.cancellation.operationId, "cancel-original");
    assert.equal(projection.cancellation.state, "committed");
    assert.equal(closes, 0, "deadline reconciliation may inspect but must not repeat an external close");
    assert.deepEqual(resumed.listOperationIncidentTriggers(), []);
  });

  it("keeps one cancellation review episode when reconciliation resumes after activation recovery", async (test) => {
    const scenario = new WorkflowScenario({
      rootDirectory: await mkdtemp(join(tmpdir(), "operation-review-cancellation-recovery-")),
    });
    const { session: ownerSession, runtime: ownerRuntime } = scenario.createOwner();
    const childSession = scenario.childSession(ownerRuntime, "child");
    const child = ownerRuntime.addAgent({
      session: childSession,
      spawner: ownerRuntime.owner(),
      name: "Child",
    });
    const original = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    ownerRuntime.checkpoint(original, JSON.stringify({ surface: "recovered-cancellation-pane" }));
    const observer = scenario.startAgent(ownerRuntime.workflow, childSession);
    test.after(() => observer.close());
    const service = new ActivationCancellationService({
      databasePath: ownerRuntime.workflow.databasePath,
      actor: ownerRuntime.owner(),
      terminator: {
        async inspect() {
          return { kind: "unavailable", error: "initial process inspection unavailable" };
        },
        async close() {
          throw new Error("close must not run after unavailable inspection");
        },
      },
      now: scenario.clock.now,
      allocateOperationId: () => "cancel-recovered",
    });
    await assert.rejects(
      service.cancel({
        target: ownerRuntime.agent(child.agentId),
        sourceId: "cancel-recovered-source",
      }),
      (error: unknown) => error instanceof CancellationInDoubtError,
    );
    service.close();

    const beforeProjection = ownerRuntime.inspectTarget({ agent: child.agentId }) as any;
    const operationReviewId = beforeProjection.operationReviews[0].operationReviewId as number;
    const before = ownerRuntime.inspectOperationReview(operationReviewId)!;
    const beforeWatch = ownerRuntime.listWorkflowAttention();
    const beforeTriggers = ownerRuntime.listOperationIncidentTriggers();

    ownerRuntime.confirmAgentRunExit(original, {
      error: "original run failed with cancellation in doubt",
    });
    const replacement = ownerRuntime.startAgentRun(ownerRuntime.agent(child.agentId));
    const transferred = ownerRuntime.inspectOperationReview(operationReviewId)!;
    assert.equal(transferred.operationReviewId, operationReviewId);
    assert.equal(transferred.reviewStartedAtMs, before.reviewStartedAtMs);
    assert.equal(transferred.reviewDeadlineAtMs, before.reviewDeadlineAtMs);
    assert.equal(transferred.reconciliationAttempts, before.reconciliationAttempts);
    assert.equal(transferred.evidenceCount, before.evidenceCount);
    assert.deepEqual(transferred.latestEvidence, before.latestEvidence);
    assert.equal(transferred.activationId, replacement.ownership.runId);
    assert.deepEqual(ownerRuntime.listWorkflowAttention(), beforeWatch);
    assert.deepEqual(ownerRuntime.listOperationIncidentTriggers(), beforeTriggers);
    ownerRuntime.close();

    const resumed = new WorkflowBootstrap({
      now: scenario.clock.now,
      agentRunTerminator: {
        async inspect(locator) {
          assert.deepEqual(locator, { surface: "recovered-cancellation-pane" });
          return { kind: "missing" };
        },
        async close() {
          throw new Error("reconciliation must not repeat the external close");
        },
      },
    });
    test.after(() => resumed.close());
    resumed.sessionStarted({
      sessionManager: {
        getSessionId: () => ownerSession.agentId,
        getSessionFile: () => ownerSession.sessionPath,
      },
    }, {});

    await resumed.startDirectSignalRouter({ projectInboxBatch() {} });

    const after = observer.inspectOperationReview(operationReviewId)!;
    assert.equal(after.operationReviewId, operationReviewId);
    assert.equal(after.reviewStartedAtMs, before.reviewStartedAtMs);
    assert.equal(after.reviewDeadlineAtMs, before.reviewDeadlineAtMs);
    assert.equal(after.reconciliationAttempts, before.reconciliationAttempts + 1);
    assert.equal(after.evidenceCount, before.evidenceCount + 2);
    assert.deepEqual(after.latestEvidence, {
      kind: "cancellation-state-probe",
      detail: "Original cancellation cancel-recovered remains in doubt after exact revalidation",
      observedAtMs: scenario.clock.now(),
    });
    assert.deepEqual(resumed.listWorkflowAttention(), beforeWatch);
    assert.deepEqual(resumed.listOperationIncidentTriggers(), beforeTriggers);
    const canonicalEvidence = observer.listOperationReviewEvidence(operationReviewId)
      .filter((evidence) =>
        evidence.kind === "cancellation-uncertainty"
        && evidence.detail === "Exact activation/run/epoch/revision/checkpoint revalidation failed");
    assert.deepEqual(canonicalEvidence, [{
      kind: "cancellation-uncertainty",
      detail: "Exact activation/run/epoch/revision/checkpoint revalidation failed",
      observedAtMs: scenario.clock.now(),
    }]);
  });
});
