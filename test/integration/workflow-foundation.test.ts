import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { SQLiteWorkflowStore } from "../../pi-extension/subagents/protocol/sqlite-workflow-store.ts";
import { WorkflowControlPlane } from "../../pi-extension/subagents/protocol/workflow-control-plane.ts";
import {
  PI_TIMEOUT,
  cleanupTestEnv,
  createTestEnv,
  createTrackedSurface,
  getAvailableBackends,
  restoreBackend,
  setBackend,
  sleep,
  startPi,
  trackTempFile,
  uniqueId,
  waitForFile,
  waitForScreen,
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
        "After receiving the result, say WORKFLOW_FOUNDATION_COMPLETE.",
      ].join("\n"));

      await waitForFile(markerFile, PI_TIMEOUT, /WORKFLOW_/);
      await waitForScreen(
        surface,
        /WORKFLOW_FOUNDATION_COMPLETE/,
        PI_TIMEOUT,
      );
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
      let replacement;
      for (let attempt = 0; attempt < 30; attempt += 1) {
        try {
          replacement = controlPlane.acquireAgentRun(
            controlPlane.agent(child.agentId),
            `integration-check-${id}`,
          );
          break;
        } catch (error) {
          if ((error as { code?: string }).code !== "AgentRunAlreadyOwned") throw error;
          await sleep(500);
        }
      }
      assert.ok(replacement, "completed child Agent Run should eventually release ownership");
      controlPlane.releaseAgentRun(replacement!);
      controlPlane.close();
    });
  });
}
