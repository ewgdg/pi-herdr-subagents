import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  awaitProvisionalSpawnCommit,
  ProvisionalSpawnGate,
} from "../../pi-extension/subagents/protocol/provisional-spawn.ts";
import {
  connectFramedIpc,
  CURRENT_IPC_VERSION,
} from "../../pi-extension/subagents/coordination/framed-ipc.ts";

describe("provisional Spawned Initial Request startup fence", () => {
  it("bounds READY when a child never reports readiness", async () => {
    const gate = await ProvisionalSpawnGate.create({ phaseTimeoutMs: 5 });
    try {
      await assert.rejects(gate.waitUntilReady(), /READY phase timed out/);
    } finally {
      await gate.close();
    }
  });

  it("requires child extension readiness before the Spawner can commit its durable run identity", async () => {
    const gate = await ProvisionalSpawnGate.create();
    try {
      const childCommit = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" });
      assert.deepEqual(await gate.waitUntilReady(), { routerEndpoint: "child-router" });
      await gate.release({ runId: "run-1", fencingEpoch: 1 });
      assert.deepEqual(await childCommit, { runId: "run-1", fencingEpoch: 1 });
    } finally {
      await gate.close();
    }
  });

  it("projects once before release and waits for recipient adoption", async () => {
    const gate = await ProvisionalSpawnGate.create();
    const projected: unknown[] = [];
    let adopted = false;
    try {
      const childCommit = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, {
        async project(plan) { projected.push(plan); },
        async release() { adopted = true; },
      });
      await gate.waitUntilReady();
      const plan = {
        senderSessionPath: "/sender.jsonl", messageId: "message-1", sourceEntryId: "message-1",
        senderAgentId: "sender", recipientAgentId: "recipient", payloadDigest: "digest", agentDefinition: "worker", agentName: "Worker",
      };
      await gate.project(plan);
      await gate.project(plan);
      assert.deepEqual(projected, [plan]);
      await assert.rejects(() => gate.project({ ...plan, recipientAgentId: "other" }), /Conflicting/);
      await gate.release({ runId: "run-1", fencingEpoch: 1 });
      assert.equal(adopted, true);
      assert.deepEqual(await childCommit, { runId: "run-1", fencingEpoch: 1 });
    } finally {
      await gate.close();
    }
  });

  it("rejects PROJECT when the child disconnects during projection", async () => {
    const gate = await ProvisionalSpawnGate.create();
    try {
      const child = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, {
        project() { throw new Error("child project failure"); },
      });
      void child.catch(() => undefined);
      await gate.waitUntilReady();
      await assert.rejects(gate.project({
        senderSessionPath: "/sender.jsonl", messageId: "message-1", sourceEntryId: "message-1",
        senderAgentId: "sender", recipientAgentId: "recipient", payloadDigest: "digest", agentDefinition: "worker", agentName: "Worker",
      }), /child project failure|disconnected/);
      await assert.rejects(child, /child project failure/);
    } finally {
      await gate.close();
    }
  });

  it("rejects RELEASE when the child disconnects before acknowledging adoption", async () => {
    const gate = await ProvisionalSpawnGate.create();
    try {
      const child = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, {
        release() { throw new Error("child release failure"); },
      });
      void child.catch(() => undefined);
      await gate.waitUntilReady();
      await assert.rejects(gate.release({ runId: "run-1", fencingEpoch: 1 }), /child release failure|disconnected/);
      await assert.rejects(child, /child release failure/);
    } finally {
      await gate.close();
    }
  });

  it("reports a lost postcommit RELEASE acknowledgement after the child observes committed ownership", async () => {
    const gate = await ProvisionalSpawnGate.create();
    const child = await connectFramedIpc(gate.endpoint);
    const observedCommits: unknown[] = [];
    try {
      child.onMessage((frame) => {
        if (frame.type === "provisional-spawn.project") {
          void child.send({ version: CURRENT_IPC_VERSION, type: "provisional-spawn.projected" });
          return;
        }
        if (frame.type === "provisional-spawn.release") {
          // Model child adoption completing before its RELEASED acknowledgement is lost.
          observedCommits.push(frame.payload);
          child.end();
          return;
        }
        assert.fail(`unexpected provisional frame: ${frame.type}`);
      });
      await child.send({
        version: CURRENT_IPC_VERSION,
        type: "provisional-spawn.ready",
        payload: { routerEndpoint: "child-router" },
      });
      await gate.waitUntilReady();
      await gate.project({
        senderSessionPath: "/sender.jsonl", messageId: "message-1", sourceEntryId: "message-1",
        senderAgentId: "sender", recipientAgentId: "recipient", payloadDigest: "digest", agentDefinition: "worker", agentName: "Worker",
      });

      const commit = { runId: "run-1", fencingEpoch: 1 };
      await assert.rejects(gate.release(commit), /child disconnected before completing its phase/);
      assert.deepEqual(observedCommits, [commit]);
    } finally {
      child.end();
      await gate.close();
    }
  });

  it("rejects the child promptly when the Spawner disconnects after READY", async () => {
    const gate = await ProvisionalSpawnGate.create({ phaseTimeoutMs: 10_000 });
    try {
      const child = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, { phaseTimeoutMs: 10_000 });
      void child.catch(() => undefined);
      await gate.waitUntilReady();
      await gate.close();
      await assert.rejects(child, /Spawner disconnected/);
    } finally {
      await gate.close();
    }
  });

  it("rejects the child during PROJECT disconnect without acknowledging PROJECTED", async () => {
    const gate = await ProvisionalSpawnGate.create({ phaseTimeoutMs: 10_000 });
    let projectStarted!: () => void;
    let releaseProject!: () => void;
    const projectStartedPromise = new Promise<void>((resolve) => { projectStarted = resolve; });
    const projectBarrier = new Promise<void>((resolve) => { releaseProject = resolve; });
    try {
      const child = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, {
        phaseTimeoutMs: 10_000,
        async project() { projectStarted(); await projectBarrier; },
      });
      void child.catch(() => undefined);
      await gate.waitUntilReady();
      const project = gate.project({
        senderSessionPath: "/sender.jsonl", messageId: "message-1", sourceEntryId: "source-1",
        senderAgentId: "sender", recipientAgentId: "recipient", payloadDigest: "digest", agentDefinition: "worker", agentName: "Worker",
      });
      void project.catch(() => undefined);
      await projectStartedPromise;
      await gate.close();
      await assert.rejects(child, /Spawner disconnected/);
      releaseProject();
      await assert.rejects(project, /Spawner closed|disconnected/);
    } finally {
      releaseProject?.();
      await gate.close();
    }
  });

  it("rejects the child during RELEASE disconnect without acknowledging RELEASED", async () => {
    const gate = await ProvisionalSpawnGate.create({ phaseTimeoutMs: 10_000 });
    let releaseStarted!: () => void;
    let releaseAdoption!: () => void;
    const releaseStartedPromise = new Promise<void>((resolve) => { releaseStarted = resolve; });
    const releaseBarrier = new Promise<void>((resolve) => { releaseAdoption = resolve; });
    try {
      const child = awaitProvisionalSpawnCommit(gate.endpoint, { routerEndpoint: "child-router" }, {
        phaseTimeoutMs: 10_000,
        async release() { releaseStarted(); await releaseBarrier; },
      });
      void child.catch(() => undefined);
      await gate.waitUntilReady();
      const release = gate.release({ runId: "run-1", fencingEpoch: 1 });
      void release.catch(() => undefined);
      await releaseStartedPromise;
      await gate.close();
      await assert.rejects(child, /Spawner disconnected/);
      releaseAdoption();
      await assert.rejects(release, /Spawner closed|disconnected/);
    } finally {
      releaseAdoption?.();
      await gate.close();
    }
  });
});
