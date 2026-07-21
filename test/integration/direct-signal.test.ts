import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { after, before, describe, it } from "node:test";
import { SQLiteWorkflowStore } from "../../pi-extension/subagents/protocol/sqlite-workflow-store.ts";
import {
  PI_TIMEOUT,
  cleanupTestEnv,
  createTestEnv,
  createTrackedSurface,
  getAvailableBackends,
  restoreBackend,
  readPane,
  setBackend,
  sleep,
  startPi,
  trackTempFile,
  uniqueId,
  waitForFile,
  type TestEnv,
} from "./harness.ts";

for (const backend of getAvailableBackends()) {
  describe(`Direct Signal delivery [${backend}]`, { timeout: PI_TIMEOUT + 30_000 }, () => {
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

    it("delivers one queued Signal through real IPC into one recipient Inbox Batch", async () => {
      const id = uniqueId();
      const readyFile = `/tmp/pi-integ-signal-ready-${id}.txt`;
      const deliveredFile = `/tmp/pi-integ-signal-delivered-${id}.txt`;
      const payload = `DIRECT_SIGNAL_${id}`;
      trackTempFile(environment, readyFile);
      trackTempFile(environment, deliveredFile);
      const surface = createTrackedSurface(environment, `signal-${id}`);
      await sleep(1_000);

      startPi(surface, environment.dir, [
        "Call the subagent tool exactly once with:",
        `  name: "Signal-${id}"`,
        "  agent: \"test-signal-recipient\"",
        `  task: "First run: echo READY > '${readyFile}'. Then wait. When a Signal arrives, write the exact Signal payload to '${deliveredFile}'."`,
        "Read the Workflow Agent ID from the subagent launch result.",
        "Then run this bash command once: sleep 5",
        "Then call agent_send exactly once with:",
        "  target.agent: the Workflow Agent ID from the launch result",
        `  message: "${payload}"`,
        "After the queued receipt, stop. Do not send any other message to the child.",
      ].join("\n"));

      try {
        await waitForFile(readyFile, PI_TIMEOUT, /READY/);
      } catch (error) {
        let screen = "(parent pane unavailable)";
        try {
          screen = readPane(surface, 200);
        } catch {}
        throw new Error(
          `${(error as Error).message}\nParent Pi screen:\n${screen}`,
        );
      }
      assert.equal((await waitForFile(deliveredFile, PI_TIMEOUT, new RegExp(payload))).trim(), payload);

      const safeProjectPath = `--${environment.dir.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
      const sessionDirectory = join(
        process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent"),
        "sessions",
        safeProjectPath,
      );
      const databasePaths = readdirSync(sessionDirectory, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => join(sessionDirectory, entry.name, "coordination.sqlite"))
        .filter(existsSync);
      const match = databasePaths.map((databasePath) => {
        const store = new SQLiteWorkflowStore(databasePath);
        const workflow = store.readWorkflowIdentity();
        const recipient = workflow
          ? store.listWorkflow(workflow.ownerAgentId).find((agent) => agent.name === `Signal-${id}`)
          : undefined;
        store.close();
        return { databasePath, workflow, recipient };
      }).find((candidate) => candidate.workflow && candidate.recipient);
      assert.ok(match?.workflow && match.recipient, "Signal recipient should be durable Workflow member");

      const entries = readFileSync(match.recipient.sessionPath, "utf8")
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as {
          type?: string;
          customType?: string;
          content?: string;
          details?: { messages?: Array<{ messageId?: string }> };
          message?: {
            role?: string;
            customType?: string;
            content?: string;
            details?: { messages?: Array<{ messageId?: string }> };
          };
        });
      const inboxEntries = entries.filter((entry) =>
        entry.type === "custom_message"
        && entry.customType === "agent_inbox_batch"
        && entry.content?.includes(payload));
      assert.equal(inboxEntries.length, 1);
      assert.equal(inboxEntries[0].details?.messages?.length, 1);
      const messageId = inboxEntries[0].details?.messages?.[0]?.messageId;
      assert.ok(messageId, "Inbox Batch should retain the Signal Message Identity");

      const database = new DatabaseSync(match.databasePath);
      const row = database.prepare(`
        SELECT message_id, sender_agent_id, recipient_agent_id, source_entry_id,
               payload_digest, acceptance_sequence, delivery_status
        FROM direct_signal_messages
        WHERE message_id = ?
      `).get(messageId) as Record<string, unknown> | undefined;
      assert.equal(row?.message_id, messageId);
      assert.equal(row?.recipient_agent_id, match.recipient.agentId);
      assert.equal(row?.delivery_status, "delivered");
      assert.equal(JSON.stringify(row).includes(payload), false);
      assert.equal(
        database.prepare("SELECT COUNT(*) AS count FROM pending_message_pointers WHERE message_id = ?")
          .get(messageId)!.count,
        0,
      );
      database.close();
    });
  });
}
