import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { SQLiteWorkflowStore } from "../../pi-extension/subagents/protocol/sqlite-workflow-store.ts";
import { WorkflowControlPlane } from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import {
  PI_TIMEOUT,
  closePane,
  cleanupTestEnv,
  createTestEnv,
  createTrackedSurface,
  getAvailableBackends,
  interruptPane,
  readPane,
  restoreBackend,
  setBackend,
  sleep,
  startPi,
  trackTempFile,
  uniqueId,
  waitForFile,
  type TestEnv,
} from "./harness.ts";

for (const backend of getAvailableBackends()) {
  describe(`Workflow foundation [${backend}]`, { timeout: PI_TIMEOUT }, () => {
    let previousBackend: string | undefined;
    let environment: TestEnv;

    before(() => {
      previousBackend = setBackend(backend);
      environment = createTestEnv(backend);
    });

    after(() => {
      cleanupTestEnv(environment);
      restoreBackend(previousBackend);
    });

    it("bootstraps durable membership through the real Owner and child Agent Runs", async () => {
      const id = uniqueId();
      const markerFile = `/tmp/pi-integ-workflow-${id}.txt`;
      trackTempFile(environment, markerFile);
      const surface = createTrackedSurface(environment, `workflow-${id}`);
      await sleep(1_000);

      startPi(surface, environment.dir, [
        "Call the subagent tool exactly once with:",
        `  name: "Workflow-${id}"`,
        "  agent: \"test-echo\"",
        `  task: "Run this bash command: echo 'WORKFLOW_${id}' > '${markerFile}'"`,
        "Do not send another prompt to the child after it settles.",
      ].join("\n"));

      await waitForFile(markerFile, PI_TIMEOUT, /WORKFLOW_/);
      const safeProjectPath = `--${environment.dir.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
      const sessionDirectory = join(
        process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent"),
        "sessions",
        safeProjectPath,
      );
      const stores = readdirSync(sessionDirectory, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => join(sessionDirectory, entry.name, "coordination.sqlite"))
        .filter(existsSync)
        .map((databasePath) => new SQLiteWorkflowStore(databasePath));
      const match = stores.map((store) => {
        const workflow = store.readWorkflowIdentity();
        const child = workflow
          ? store.listWorkflow(workflow.ownerAgentId).find((agent) => agent.name === `Workflow-${id}`)
          : undefined;
        return { store, workflow, child };
      }).find((candidate) => candidate.workflow && candidate.child);
      assert.ok(match?.workflow && match.child, "durable Workflow membership should be discoverable");
      for (const store of stores) store.close();

      const childHeader = JSON.parse(readFileSync(match.child.sessionPath, "utf8").split("\n")[0]) as {
        id: string;
      };
      assert.equal(childHeader.id, match.child.agentId);
      const child = match.child;
      const workflow = match.workflow;
      assert.equal(child.workflowOwnerId, workflow.ownerAgentId);
      assert.equal(child.spawnerAgentId, workflow.ownerAgentId);
      assert.equal(child.agentDefinition, "test-echo");

      const controlPlane = WorkflowControlPlane.startOwner({
        ownerSessionId: workflow.ownerAgentId,
        ownerSessionPath: workflow.ownerSessionPath,
      });
      const childReference = controlPlane.agent(child.agentId);
      let activation = controlPlane.inspectActivation(childReference);
      for (let attempt = 0; attempt < 120; attempt += 1) {
        if (activation?.state.kind === "waiting") break;
        await sleep(500);
        activation = controlPlane.inspectActivation(childReference);
      }
      assert.deepEqual(activation?.state, {
        kind: "waiting",
        dependencies: [{ kind: "human", dependencyId: "human" }],
      });
      assert.ok(
        controlPlane.currentAgentRun(childReference),
        "settled child must retain Agent Run ownership",
      );

      const checkpoint = controlPlane.readAgentRunCheckpoint(childReference);
      assert.ok(checkpoint, "active Agent Run should retain its surface checkpoint");
      const locator = JSON.parse(checkpoint.value) as { surface: string };
      closePane(locator.surface);

      for (let attempt = 0; attempt < 120; attempt += 1) {
        if (!controlPlane.currentAgentRun(childReference)) break;
        await sleep(500);
      }
      assert.equal(controlPlane.currentAgentRun(childReference), undefined);
      const failed = controlPlane.inspectActivation(childReference);
      assert.equal(failed?.state.kind, "ended");
      if (failed?.state.kind !== "ended") {
        assert.fail("closed child should end its canonical activation");
      }
      assert.equal(failed.state.outcome, "failed");
      controlPlane.close();
    });

    it("commits interruption only after Pi confirms the active turn aborted", async () => {
      const id = uniqueId();
      const markerFile = `/tmp/pi-integ-interrupt-${id}.txt`;
      trackTempFile(environment, markerFile);
      const surface = createTrackedSurface(environment, `interrupt-${id}`);
      await sleep(1_000);

      startPi(surface, environment.dir, [
        "Call the subagent tool exactly once with:",
        `  name: "Interrupt-${id}"`,
        "  agent: \"test-echo\"",
        `  task: "Run: echo 'START_${id}' > '${markerFile}'; sleep 120"`,
        "Do not send another prompt to the child.",
      ].join("\n"));

      await waitForFile(markerFile, PI_TIMEOUT, /START_/);
      const safeProjectPath = `--${environment.dir.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
      const sessionDirectory = join(
        process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent"),
        "sessions",
        safeProjectPath,
      );
      const stores = readdirSync(sessionDirectory, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => join(sessionDirectory, entry.name, "coordination.sqlite"))
        .filter(existsSync)
        .map((databasePath) => new SQLiteWorkflowStore(databasePath));
      const match = stores.map((store) => {
        const workflow = store.readWorkflowIdentity();
        const child = workflow
          ? store.listWorkflow(workflow.ownerAgentId).find((agent) => agent.name === `Interrupt-${id}`)
          : undefined;
        return { workflow, child };
      }).find((candidate) => candidate.workflow && candidate.child);
      for (const store of stores) store.close();
      assert.ok(match?.workflow && match.child, "interrupt target should be durable");

      const controlPlane = WorkflowControlPlane.startOwner({
        ownerSessionId: match.workflow.ownerAgentId,
        ownerSessionPath: match.workflow.ownerSessionPath,
      });
      const childReference = controlPlane.agent(match.child.agentId);
      const ownership = controlPlane.currentAgentRun(childReference);
      assert.ok(ownership, "active child should retain ownership before interruption");
      const checkpoint = controlPlane.readAgentRunCheckpoint(childReference);
      assert.ok(checkpoint);
      const locator = JSON.parse(checkpoint.value) as { surface: string };

      controlPlane.requestInterruption(ownership);
      assert.equal(controlPlane.inspectActivation(childReference)?.state.kind, "active");
      interruptPane(locator.surface);
      interruptPane(locator.surface);

      let activation = controlPlane.inspectActivation(childReference);
      for (let attempt = 0; attempt < 120; attempt += 1) {
        if (activation?.state.kind === "interrupted") break;
        await sleep(500);
        activation = controlPlane.inspectActivation(childReference);
      }
      assert.equal(
        activation?.state.kind,
        "interrupted",
        `Child screen after Escape:\n${readPane(locator.surface, 120)}\nTranscript tail:\n${readFileSync(match.child.sessionPath, "utf8").split("\n").slice(-8).join("\n")}`,
      );
      assert.equal(controlPlane.currentAgentRun(childReference)?.runId, ownership.runId);

      closePane(locator.surface);
      for (let attempt = 0; attempt < 120; attempt += 1) {
        if (!controlPlane.currentAgentRun(childReference)) break;
        await sleep(500);
      }
      assert.equal(controlPlane.currentAgentRun(childReference), undefined);
      controlPlane.close();
    });
  });
}
