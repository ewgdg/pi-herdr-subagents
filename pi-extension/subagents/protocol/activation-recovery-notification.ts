import {
  CURRENT_IPC_VERSION,
  connectFramedIpc,
} from "../coordination/framed-ipc.ts";
import { DirectSignalStore } from "./sqlite-message-store.ts";

/** A non-message scheduling hint; durable recovery rows remain authoritative. */
export const AUTOMATIC_RECOVERY_SCHEDULE_TYPE = "activation-recovery.schedule";

/**
 * Notify the current Owner runtime to reconcile its durable recovery queue.
 * A missing or stale router is deliberately harmless: the pending recovery
 * episode remains durable and is reconciled when the Owner becomes available.
 */
export async function notifyWorkflowOwnerOfAutomaticRecovery(input: {
  databasePath: string;
  workflowOwnerId: string;
}): Promise<"notified" | "offline"> {
  const store = new DirectSignalStore(input.databasePath);
  let endpoint: string | undefined;
  try {
    endpoint = store.readRouter({
      workflowOwnerId: input.workflowOwnerId,
      agentId: input.workflowOwnerId,
    })?.endpoint;
  } finally {
    store.close();
  }
  if (!endpoint) return "offline";
  try {
    const connection = await connectFramedIpc(endpoint);
    try {
      await connection.send({
        version: CURRENT_IPC_VERSION,
        type: AUTOMATIC_RECOVERY_SCHEDULE_TYPE,
        payload: { workflowOwnerId: input.workflowOwnerId },
      });
      // The Owner closes this one-shot connection only after its scheduling
      // callback has observed the durable queue. Waiting for that close prevents
      // a nested watcher from outrunning the live Owner's reconciliation.
      const closure = await connection.closed;
      if (closure.kind === "failed") throw closure.error;
      return "notified";
    } finally {
      connection.end();
    }
  } catch {
    // The durable pending episode is the retry mechanism. An IPC hint must
    // never manufacture a protocol message or turn an Owner-offline case into
    // an error in the failed child watcher.
    return "offline";
  }
}
