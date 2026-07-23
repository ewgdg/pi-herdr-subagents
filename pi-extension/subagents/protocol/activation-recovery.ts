import { DatabaseSync } from "node:sqlite";
import { createAutomaticRecoveryContinuation } from "./automatic-recovery-continuation.ts";
import { AGENT_RUN_CHECKPOINT_STATE_KEY } from "./agent-run-ownership.ts";
import { WorkflowProtocolError, type AgentReference, type AgentRunOwnership } from "./workflow-types.ts";

export type RecoveryState = "pending" | "launching" | "active" | "resolved" | "exhausted" | "blocked-policy";
type RecoveryContinuationState = "none" | "pending" | "projecting" | "consumed";
export type RecoveryContinuationEvidenceKind = "human-tool-result" | "inbox-batch";
export type RecoveryPaneIntentState = "prepared" | "creating" | "created" | "promoted" | "cleanup-pending";

/** Durable identity written before the external Herdr create request. */
export interface RecoveryPaneIntent {
  intentId: string;
  failedActivationId: string;
  agentId: string;
  runId: string;
  workspaceId: string;
  label: string;
  cwd: string;
  surface?: string;
  state: RecoveryPaneIntentState;
  detail?: string;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface RecoveryContinuationClaim {
  projectionId: string;
  failedActivationId: string;
  replacementActivationId: string;
}

export interface ActivationRecoveryRecord {
  failedActivationId: string;
  agentId: string;
  state: RecoveryState;
  replacementRunId?: string;
  replacementFencingEpoch?: number;
  replacementActivationId?: string;
  exhaustionActivationId?: string;
  detail?: string;
  createdAtMs: number;
  updatedAtMs: number;
}

interface RecoveryRow {
  failed_activation_id: string;
  agent_id: string;
  state: RecoveryState;
  replacement_run_id: string | null;
  replacement_fencing_epoch: number | null;
  replacement_activation_id: string | null;
  exhaustion_activation_id: string | null;
  detail: string | null;
  continuation_state: RecoveryContinuationState;
  continuation_evidence_kind: RecoveryContinuationEvidenceKind | null;
  continuation_evidence_id: string | null;
  created_at_ms: number;
  updated_at_ms: number;
}

interface RecoveryPaneIntentRow {
  intent_id: string;
  failed_activation_id: string;
  agent_id: string;
  run_id: string;
  workspace_id: string;
  label: string;
  cwd: string;
  surface: string | null;
  state: RecoveryPaneIntentState;
  detail: string | null;
  created_at_ms: number;
  updated_at_ms: number;
}

export function initializeActivationRecoverySchema(database: DatabaseSync): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS activation_recoveries (
      failed_activation_id TEXT PRIMARY KEY REFERENCES agent_activations(activation_id),
      agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
      state TEXT NOT NULL CHECK (state IN ('pending', 'launching', 'active', 'resolved', 'exhausted', 'blocked-policy')),
      replacement_run_id TEXT UNIQUE,
      replacement_fencing_epoch INTEGER,
      replacement_activation_id TEXT UNIQUE REFERENCES agent_activations(activation_id),
      exhaustion_activation_id TEXT UNIQUE REFERENCES agent_activations(activation_id),
      detail TEXT,
      continuation_state TEXT NOT NULL DEFAULT 'none' CHECK (continuation_state IN ('none', 'pending', 'projecting', 'consumed')),
      continuation_evidence_kind TEXT CHECK (continuation_evidence_kind IN ('human-tool-result', 'inbox-batch')),
      continuation_evidence_id TEXT,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL,
      CHECK ((continuation_evidence_kind IS NULL AND continuation_evidence_id IS NULL)
        OR (continuation_evidence_kind IS NOT NULL AND continuation_evidence_id IS NOT NULL)),
      CHECK ((state = 'launching' AND replacement_run_id IS NOT NULL AND replacement_fencing_epoch IS NOT NULL AND replacement_activation_id IS NULL)
        OR (state = 'active' AND replacement_run_id IS NOT NULL AND replacement_fencing_epoch IS NOT NULL AND replacement_activation_id IS NOT NULL)
        OR (state IN ('pending', 'blocked-policy') AND replacement_run_id IS NULL AND replacement_fencing_epoch IS NULL AND replacement_activation_id IS NULL AND exhaustion_activation_id IS NULL)
        OR (state = 'resolved')
        OR (state = 'exhausted' AND replacement_activation_id IS NOT NULL AND exhaustion_activation_id IS NOT NULL))
    ) STRICT;
    CREATE INDEX IF NOT EXISTS activation_recoveries_agent_state
      ON activation_recoveries (agent_id, state, created_at_ms);

    CREATE TABLE IF NOT EXISTS recovery_pane_intents (
      intent_id TEXT PRIMARY KEY,
      failed_activation_id TEXT NOT NULL UNIQUE REFERENCES activation_recoveries(failed_activation_id),
      agent_id TEXT NOT NULL REFERENCES workflow_agents(agent_id),
      run_id TEXT NOT NULL UNIQUE,
      workspace_id TEXT NOT NULL,
      label TEXT NOT NULL,
      cwd TEXT NOT NULL,
      surface TEXT,
      state TEXT NOT NULL CHECK (state IN ('prepared', 'creating', 'created', 'promoted', 'cleanup-pending')),
      detail TEXT,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;
    CREATE UNIQUE INDEX IF NOT EXISTS recovery_pane_intents_locator
      ON recovery_pane_intents (workspace_id, label);
  `);
}

/** Called inside the failure transaction after the activation is durably ended. */
export function recordRecoveryEpisodeForFailedActivation(
  database: DatabaseSync,
  input: { activationId: string; agentId: string; now: number },
): ActivationRecoveryRecord | undefined {
  // A replacement consumes the sole automatic attempt; its failure is an
  // exhaustion fact, never the seed of a third retry episode.
  if (database.prepare("SELECT 1 FROM activation_recoveries WHERE replacement_activation_id = ? LIMIT 1")
    .get(input.activationId)) return undefined;
  if (!hasRecoveryPendingWork(database, input.activationId, input.agentId)) return undefined;
  const policy = database.prepare("SELECT launch_policy_json FROM workflow_agents WHERE agent_id = ?")
    .get(input.agentId) as { launch_policy_json: string | null } | undefined;
  if (!policy) throw new Error(`Recovery Agent ${input.agentId} is missing`);
  const state: RecoveryState = policy.launch_policy_json == null ? "blocked-policy" : "pending";
  database.prepare(`INSERT OR IGNORE INTO activation_recoveries (
    failed_activation_id, agent_id, state, replacement_run_id, replacement_fencing_epoch,
    replacement_activation_id, exhaustion_activation_id, detail, continuation_state,
    continuation_evidence_kind, continuation_evidence_id, created_at_ms, updated_at_ms
  ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, ?, 'none', NULL, NULL, ?, ?)`)
    .run(
      input.activationId,
      input.agentId,
      state,
      state === "blocked-policy" ? "Automatic recovery requires the Agent's persisted launch policy" : null,
      input.now,
      input.now,
    );
  return readRecovery(database, input.activationId);
}

/**
 * Reserve the exact Herdr identity before making the external pane request.
 * The row is intentionally separate from Agent Run ownership: a pane may
 * exist before its generated pane id has been durably acknowledged, while the
 * stable workspace/label pair remains enough for a restarted Owner to find it.
 */
export function prepareRecoveryPaneIntent(
  database: DatabaseSync,
  input: {
    workflowOwnerId: string;
    failedActivationId: string;
    intentId: string;
    runId: string;
    workspaceId: string;
    label: string;
    cwd: string;
    now: number;
  },
): RecoveryPaneIntent | undefined {
  assertPaneIntentInput(input);
  assertWorkflowOwner(database, input.workflowOwnerId);
  const existing = readRecoveryPaneIntent(database, input.failedActivationId);
  if (existing) {
    const existingAgentId = recoveryAgentId(database, input.failedActivationId);
    if (existing.agentId !== existingAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        "Recovery pane intent belongs to another Agent",
      );
    }
    if (database.prepare("SELECT 1 FROM ownership WHERE resource_id = ? LIMIT 1")
      .get(`agent-run:${input.workflowOwnerId}:${existing.agentId}`)) return undefined;
    return existing;
  }

  const recovery = readRecovery(database, input.failedActivationId);
  if (!recovery || recovery.state !== "pending") return undefined;
  const currentOwnership = database.prepare(`SELECT 1 FROM ownership
    WHERE resource_id = ? LIMIT 1`).get(`agent-run:${input.workflowOwnerId}:${recovery.agentId}`);
  if (currentOwnership) return undefined;

  database.prepare(`INSERT INTO recovery_pane_intents (
    intent_id, failed_activation_id, agent_id, run_id, workspace_id, label, cwd,
    surface, state, detail, created_at_ms, updated_at_ms
  ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'prepared', NULL, ?, ?)`)
    .run(
      input.intentId,
      input.failedActivationId,
      recovery.agentId,
      input.runId,
      input.workspaceId,
      input.label,
      input.cwd,
      input.now,
      input.now,
    );
  return readRecoveryPaneIntent(database, input.failedActivationId);
}

/** Fence the external create interval. Repeated calls by the same intent are idempotent. */
export function beginRecoveryPaneCreation(
  database: DatabaseSync,
  input: { intentId: string; runId: string; now: number },
): RecoveryPaneIntent {
  const intent = requireRecoveryPaneIntent(database, input.intentId);
  if (intent.runId !== input.runId) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane creation is fenced to another launch intent",
    );
  }
  if (intent.state === "prepared") {
    database.prepare(`UPDATE recovery_pane_intents
      SET state = 'creating', updated_at_ms = ?
      WHERE intent_id = ? AND state = 'prepared'`).run(input.now, input.intentId);
  } else if (intent.state !== "creating" && intent.state !== "created") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      `Recovery pane intent is already ${intent.state}`,
    );
  }
  return requireRecoveryPaneIntent(database, input.intentId);
}

/** Record the generated Herdr pane id without losing the stable pre-create identity. */
export function recordRecoveryPaneCreated(
  database: DatabaseSync,
  input: { intentId: string; runId: string; surface: string; now: number },
): RecoveryPaneIntent {
  if (!input.surface) throw new Error("Recovery pane surface must not be empty");
  const intent = requireRecoveryPaneIntent(database, input.intentId);
  if (intent.runId !== input.runId) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane acknowledgement is fenced to another launch intent",
    );
  }
  if (intent.surface && intent.surface !== input.surface) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane acknowledgement changed its exact pane identity",
    );
  }
  if (intent.state === "promoted") return intent;
  if (intent.state === "cleanup-pending") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane intent is already being cleaned up",
    );
  }
  database.prepare(`UPDATE recovery_pane_intents
    SET surface = ?, state = 'created', detail = NULL, updated_at_ms = ?
    WHERE intent_id = ? AND run_id = ? AND state IN ('prepared', 'creating', 'created')`)
    .run(input.surface, input.now, input.intentId, input.runId);
  return requireRecoveryPaneIntent(database, input.intentId);
}

/**
 * Promote a discovered/acknowledged pane into the fenced recovery claim. The
 * recovery row, Agent Run ownership, prepared checkpoint, and intent state all
 * commit in the same SQLite transaction through ActivationRecoveryStore.
 */
export function promoteRecoveryPaneIntent(
  database: DatabaseSync,
  input: {
    workflowOwnerId: string;
    failedActivationId: string;
    intentId: string;
    runId: string;
    surface: string;
    now: number;
  },
): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
  if (!input.surface) throw new Error("Recovery pane surface must not be empty");
  const intent = requireRecoveryPaneIntent(database, input.intentId);
  if (intent.failedActivationId !== input.failedActivationId || intent.runId !== input.runId) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane promotion is fenced to another launch intent",
    );
  }
  if (intent.surface && intent.surface !== input.surface) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane promotion changed its exact pane identity",
    );
  }
  if (intent.state === "promoted") return undefined;
  if (intent.state === "cleanup-pending") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane intent is already being cleaned up",
    );
  }
  if (intent.state !== "prepared" && intent.state !== "creating" && intent.state !== "created") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      `Recovery pane intent cannot be promoted from ${intent.state}`,
    );
  }
  if (!intent.surface) {
    database.prepare(`UPDATE recovery_pane_intents
      SET surface = ?, state = 'created', updated_at_ms = ?
      WHERE intent_id = ? AND run_id = ?`).run(input.surface, input.now, input.intentId, input.runId);
  }
  const claim = claimRecoveryRun(database, {
    workflowOwnerId: input.workflowOwnerId,
    failedActivationId: input.failedActivationId,
    runId: input.runId,
    now: input.now,
    preparedSurface: input.surface,
    provisionalIntentId: input.intentId,
  });
  if (!claim) return undefined;
  return claim;
}

/** Mark an unpromoted intent as cleanup-pending before an external close. */
export function beginRecoveryPaneCleanup(
  database: DatabaseSync,
  input: { intentId: string; now: number; detail: string },
): RecoveryPaneIntent | undefined {
  const intent = readRecoveryPaneIntentById(database, input.intentId);
  if (!intent) return undefined;
  if (intent.state === "promoted") return intent;
  const recovery = readRecovery(database, intent.failedActivationId);
  if (!recovery) return intent;
  const currentOwnership = database.prepare(`SELECT owner_id, fencing_epoch FROM ownership
    WHERE resource_id = ?`).get(`agent-run:${intentAgentOwnerId(database)}:${intent.agentId}`) as {
      owner_id: string;
      fencing_epoch: number;
    } | undefined;
  if (currentOwnership || recovery.state === "launching" || recovery.state === "active") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane intent cannot be cleaned after its Agent Run claim changed",
    );
  }
  if (intent.state !== "cleanup-pending") {
    database.prepare(`UPDATE recovery_pane_intents
      SET state = 'cleanup-pending', detail = ?, updated_at_ms = ?
      WHERE intent_id = ? AND state IN ('prepared', 'creating', 'created')`)
      .run(input.detail, input.now, input.intentId);
  }
  return readRecoveryPaneIntentById(database, input.intentId);
}

/** Delete a provisional intent only after exact external pane absence is confirmed. */
export function completeRecoveryPaneCleanup(
  database: DatabaseSync,
  input: { intentId: string; expectedSurface?: string },
): boolean {
  const result = database.prepare(`DELETE FROM recovery_pane_intents
    WHERE intent_id = ? AND state = 'cleanup-pending'
      AND (? IS NULL OR surface IS NULL OR surface = ?)`)
    .run(input.intentId, input.expectedSurface ?? null, input.expectedSurface ?? null);
  return Number(result.changes) === 1;
}

/** Retire the preparatory row after a replacement activation is authoritative. */
export function retireRecoveryPaneIntent(database: DatabaseSync, intentId: string): boolean {
  const result = database.prepare(`DELETE FROM recovery_pane_intents
    WHERE intent_id = ? AND state = 'promoted'`).run(intentId);
  return Number(result.changes) === 1;
}

/**
 * Atomically fence the automatic launch, its exact ownership, and the locator
 * that lets a later Owner reconcile the provisional pane.
 */
export function claimRecoveryRun(
  database: DatabaseSync,
  input: {
    workflowOwnerId: string;
    failedActivationId: string;
    runId: string;
    now: number;
    preparedSurface?: string;
    provisionalIntentId?: string;
  },
): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
  const row = readRecovery(database, input.failedActivationId);
  if (!row || row.state !== "pending") return undefined;
  const paneIntent = readRecoveryPaneIntent(database, input.failedActivationId);
  if (paneIntent && paneIntent.state !== "promoted" && paneIntent.runId !== input.runId) return undefined;
  if (input.provisionalIntentId
    && (!paneIntent
      || paneIntent.intentId !== input.provisionalIntentId
      || paneIntent.runId !== input.runId
      || paneIntent.state === "promoted")) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Recovery pane intent no longer matches the exact promotion claim",
    );
  }
  const current = database.prepare(`SELECT activation_id, phase, ended_outcome FROM agent_activations
    WHERE agent_id = ? ORDER BY activation_sequence DESC LIMIT 1`).get(row.agentId) as {
      activation_id: string; phase: "open" | "ended"; ended_outcome: string | null;
    } | undefined;
  if (!current || current.activation_id !== row.failedActivationId || current.phase !== "ended" || current.ended_outcome !== "failed") {
    database.prepare(`UPDATE activation_recoveries SET state = 'resolved', detail = ?, updated_at_ms = ?
      WHERE failed_activation_id = ? AND state = 'pending'`)
      .run("A later activation superseded automatic recovery", input.now, row.failedActivationId);
    return undefined;
  }
  const owner = database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1")
    .get() as { owner_agent_id: string } | undefined;
  if (!owner || owner.owner_agent_id !== input.workflowOwnerId) {
    throw new WorkflowProtocolError("WorkflowMismatch", "Recovery claim belongs to another Workflow");
  }
  const resourceId = `agent-run:${input.workflowOwnerId}:${row.agentId}`;
  if (database.prepare("SELECT 1 FROM ownership WHERE resource_id = ?").get(resourceId)) return undefined;
  if (!hasRecoveryPendingWork(database, row.failedActivationId, row.agentId)) {
    database.prepare(`UPDATE activation_recoveries SET state = 'resolved', detail = ?, updated_at_ms = ?
      WHERE failed_activation_id = ? AND state = 'pending'`)
      .run("Recovery-pending work no longer exists", input.now, row.failedActivationId);
    return undefined;
  }
  const epochRow = database.prepare("SELECT last_epoch FROM ownership_epochs WHERE resource_id = ?")
    .get(resourceId) as { last_epoch: number } | undefined;
  const epoch = Number(epochRow?.last_epoch ?? 0) + 1;
  const claimed = database.prepare(`UPDATE activation_recoveries
    SET state = 'launching', replacement_run_id = ?, replacement_fencing_epoch = ?, detail = NULL, updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'pending'`)
    .run(input.runId, epoch, input.now, input.failedActivationId);
  if (Number(claimed.changes) !== 1) return undefined;
  database.prepare(`INSERT INTO ownership_epochs (resource_id, last_epoch) VALUES (?, ?)
    ON CONFLICT (resource_id) DO UPDATE SET last_epoch = excluded.last_epoch`).run(resourceId, epoch);
  database.prepare("INSERT INTO ownership (resource_id, owner_id, fencing_epoch) VALUES (?, ?, ?)")
    .run(resourceId, input.runId, epoch);
  database.prepare(`INSERT INTO fenced_state (resource_id, state_key, value, fencing_epoch)
    VALUES (?, ?, ?, ?)
    ON CONFLICT (resource_id, state_key) DO UPDATE SET
      value = excluded.value,
      fencing_epoch = excluded.fencing_epoch`).run(
    resourceId,
    AGENT_RUN_CHECKPOINT_STATE_KEY,
    serializePreparedRecoveryCheckpoint({
      surface: input.preparedSurface ?? `recovery://${input.runId}`,
      runId: input.runId,
      fencingEpoch: epoch,
    }),
    epoch,
  );
  if (input.provisionalIntentId) {
    const promoted = database.prepare(`UPDATE recovery_pane_intents
      SET surface = ?, state = 'promoted', detail = NULL, updated_at_ms = ?
      WHERE intent_id = ? AND failed_activation_id = ? AND run_id = ?
        AND state IN ('prepared', 'creating', 'created')`).run(
      input.preparedSurface,
      input.now,
      input.provisionalIntentId,
      input.failedActivationId,
      input.runId,
    );
    if (Number(promoted.changes) !== 1) {
      throw new WorkflowProtocolError(
        "RecoveryActivationClaimed",
        "Recovery pane intent changed during atomic promotion",
      );
    }
  }
  return {
    recovery: readRecovery(database, input.failedActivationId)!,
    ownership: { workflowOwnerId: input.workflowOwnerId, agentId: row.agentId, runId: input.runId, resourceId, epoch },
  };
}

/** Release only a pane launch that never created an activation. */
export function abandonRecoveryLaunch(
  database: DatabaseSync,
  input: {
    failedActivationId: string;
    ownership: AgentRunOwnership;
    now: number;
    detail: string;
    expectedCheckpoint?: string;
  },
): void {
  const expectedResourceId = `agent-run:${input.ownership.workflowOwnerId}:${input.ownership.agentId}`;
  if (input.ownership.resourceId !== expectedResourceId) {
    throw new WorkflowProtocolError(
      "WorkflowMismatch",
      "Recovery launch ownership token does not belong to its Agent resource",
    );
  }
  // A concurrent child bootstrap changes the row to active before this
  // cleanup transaction can run. Treat every exact-claim mismatch as a race,
  // not as a successful pending transition, so callers keep the fence.
  const recovery = database.prepare(`SELECT agent_id, state FROM activation_recoveries
    WHERE failed_activation_id = ? AND replacement_run_id = ? AND replacement_fencing_epoch = ?`).get(
    input.failedActivationId,
    input.ownership.runId,
    input.ownership.epoch,
  ) as { agent_id: string; state: RecoveryState } | undefined;
  if (!recovery) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "Automatic recovery launch changed before its exact pending transition",
    );
  }
  if (recovery.agent_id !== input.ownership.agentId) {
    throw new WorkflowProtocolError("WorkflowMismatch", "Recovery launch belongs to another Agent");
  }
  if (recovery.state !== "launching") {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      "An automatic recovery launch is no longer pending reconciliation",
    );
  }
  const currentOwnership = database.prepare("SELECT owner_id, fencing_epoch FROM ownership WHERE resource_id = ?")
    .get(input.ownership.resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
  if (!currentOwnership
    || currentOwnership.owner_id !== input.ownership.runId
    || Number(currentOwnership.fencing_epoch) !== input.ownership.epoch) {
    throw new WorkflowProtocolError("OwnershipLost", "Automatic recovery launch lost its exact Agent Run ownership");
  }
  if (input.expectedCheckpoint !== undefined) {
    const checkpoint = database.prepare(`SELECT value, fencing_epoch FROM fenced_state
      WHERE resource_id = ? AND state_key = 'agent-run-checkpoint'`).get(
      input.ownership.resourceId,
    ) as { value: string; fencing_epoch: number } | undefined;
    if (!checkpoint || Number(checkpoint.fencing_epoch) !== input.ownership.epoch
      || checkpoint.value !== input.expectedCheckpoint) {
      throw new WorkflowProtocolError(
        "RecoveryActivationClaimed",
        "Automatic recovery dispatch changed before its exact pending transition",
      );
    }
  }
  const activation = database.prepare(`SELECT 1 FROM agent_activations
    WHERE agent_id = ? AND run_id = ? AND fencing_epoch = ? LIMIT 1`).get(
    input.ownership.agentId,
    input.ownership.runId,
    input.ownership.epoch,
  );
  if (activation) {
    throw new WorkflowProtocolError("RecoveryActivationClaimed", "An activated replacement cannot return to pending");
  }
  database.prepare(`DELETE FROM ownership
    WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?`).run(
    input.ownership.resourceId,
    input.ownership.runId,
    input.ownership.epoch,
  );
  const updated = database.prepare(`UPDATE activation_recoveries
    SET state = 'pending', replacement_run_id = NULL, replacement_fencing_epoch = NULL, detail = ?, updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'launching' AND replacement_run_id = ? AND replacement_fencing_epoch = ?`)
    .run(input.detail, input.now, input.failedActivationId, input.ownership.runId, input.ownership.epoch);
  if (Number(updated.changes) !== 1) {
    throw new WorkflowProtocolError("RecoveryActivationClaimed", "Recovery launch changed during pending reconciliation");
  }
  // Keep the same durable label/intent for the retry, but forget the pane id
  // only after the caller has confirmed the old exact pane is absent.
  database.prepare(`UPDATE recovery_pane_intents
    SET surface = NULL, state = 'prepared', detail = ?, updated_at_ms = ?
    WHERE failed_activation_id = ? AND run_id = ? AND state = 'promoted'`)
    .run(input.detail, input.now, input.failedActivationId, input.ownership.runId);
}

/**
 * Recheck the durable work set at the final recovery-start boundary. A Request
 * can be cancelled after the Owner claims a pane but before either runtime
 * creates its activation; in that case the claim must not become idle work.
 */
export function resolveRecoveryReplacementIfWorkIsGone(
  database: DatabaseSync,
  input: {
    failedActivationId: string;
    ownership: AgentRunOwnership;
    now: number;
  },
): boolean {
  const recovery = database.prepare(`SELECT agent_id FROM activation_recoveries
    WHERE failed_activation_id = ? AND state = 'launching'
      AND replacement_run_id = ? AND replacement_fencing_epoch = ?`).get(
    input.failedActivationId,
    input.ownership.runId,
    input.ownership.epoch,
  ) as { agent_id: string } | undefined;
  if (!recovery) return false;
  if (recovery.agent_id !== input.ownership.agentId) {
    throw new WorkflowProtocolError("WorkflowMismatch", "Recovery replacement belongs to another Agent");
  }
  if (hasRecoveryPendingWork(database, input.failedActivationId, recovery.agent_id)) return false;
  const resolved = database.prepare(`UPDATE activation_recoveries
    SET state = 'resolved', detail = ?, updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'launching'
      AND replacement_run_id = ? AND replacement_fencing_epoch = ?`).run(
    "Recovery-pending work no longer exists before replacement activation start",
    input.now,
    input.failedActivationId,
    input.ownership.runId,
    input.ownership.epoch,
  );
  if (Number(resolved.changes) !== 1) {
    throw new WorkflowProtocolError("RecoveryActivationClaimed", "Automatic recovery launch no longer owns this failed activation");
  }
  const released = database.prepare(`DELETE FROM ownership
    WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?`).run(
    input.ownership.resourceId,
    input.ownership.runId,
    input.ownership.epoch,
  );
  if (Number(released.changes) !== 1) {
    throw new WorkflowProtocolError("OwnershipLost", "Automatic recovery launch lost its exact Agent Run ownership");
  }
  return true;
}

/** Called inside the replacement start transaction. */
export function activateRecoveryReplacement(
  database: DatabaseSync,
  input: { failedActivationId: string; replacementRunId: string; replacementFencingEpoch: number; replacementActivationId: string; now: number },
): void {
  const updated = database.prepare(`UPDATE activation_recoveries
    SET state = 'active', replacement_activation_id = ?, continuation_state = 'pending',
        continuation_evidence_kind = NULL, continuation_evidence_id = NULL,
        detail = NULL, updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'launching' AND replacement_run_id = ? AND replacement_fencing_epoch = ?`)
    .run(input.replacementActivationId, input.now, input.failedActivationId, input.replacementRunId, input.replacementFencingEpoch);
  if (Number(updated.changes) !== 1) {
    throw new WorkflowProtocolError("RecoveryActivationClaimed", "Automatic recovery launch no longer owns this failed activation");
  }
}

/** A declared settlement or completion ends the retry episode without changing obligations. */
export function resolveActiveRecovery(
  database: DatabaseSync,
  input: { activationId: string; now: number; detail: string },
): void {
  database.prepare(`UPDATE activation_recoveries
    SET state = 'resolved', detail = ?, updated_at_ms = ?
    WHERE replacement_activation_id = ? AND state = 'active'`)
    .run(input.detail, input.now, input.activationId);
}

/** A replacement activation consumed the sole automatic attempt. */
export function exhaustRecoveryForReplacement(
  database: DatabaseSync,
  input: { activationId: string; now: number; detail: string },
): void {
  database.prepare(`UPDATE activation_recoveries
    SET state = 'exhausted', exhaustion_activation_id = ?, detail = ?, updated_at_ms = ?
    WHERE replacement_activation_id = ? AND state = 'active'`)
    .run(input.activationId, input.detail, input.now, input.activationId);
}

/**
 * Move a deferred-only replacement to the idle projection boundary without an
 * empty provider turn or an ordinary Agent settlement.
 */
export function releaseDeferredRecoveryProjection(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  now: number,
): boolean {
  const currentOwnership = database.prepare(`SELECT owner_id, fencing_epoch FROM ownership
    WHERE resource_id = ?`).get(ownership.resourceId) as { owner_id: string; fencing_epoch: number } | undefined;
  if (!currentOwnership || currentOwnership.owner_id !== ownership.runId
    || Number(currentOwnership.fencing_epoch) !== ownership.epoch) return false;
  const recovery = database.prepare(`SELECT failed_activation_id, replacement_activation_id
    FROM activation_recoveries
    WHERE state = 'active' AND replacement_activation_id = ? AND replacement_run_id = ?
      AND replacement_fencing_epoch = ? AND continuation_state = 'pending'`).get(
    ownership.runId, ownership.runId, ownership.epoch,
  ) as { failed_activation_id: string; replacement_activation_id: string } | undefined;
  if (!recovery) return false;
  const activation = database.prepare(`SELECT phase, open_state FROM agent_activations
    WHERE activation_id = ? AND agent_id = ? AND run_id = ? AND fencing_epoch = ?`).get(
    recovery.replacement_activation_id, ownership.agentId, ownership.runId, ownership.epoch,
  ) as { phase: "open" | "ended"; open_state: "active" | "waiting" | "interrupted" | null } | undefined;
  if (!activation || activation.phase !== "open" || activation.open_state !== "active") return false;
  const human = database.prepare(`SELECT 1 FROM human_interrupts
    WHERE agent_id = ? AND activation_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
    LIMIT 1`).get(ownership.agentId, recovery.replacement_activation_id);
  const projectablePendingInput = database.prepare(`SELECT 1 FROM pending_message_pointers
    WHERE recipient_agent_id = ? AND (delivery_timing = 'steer' OR reactivates_recipient = 1)
    LIMIT 1`).get(ownership.agentId);
  const deferredPendingInput = database.prepare(`SELECT 1 FROM pending_message_pointers
    WHERE recipient_agent_id = ? AND delivery_timing = 'deferred' AND reactivates_recipient = 0
    LIMIT 1`).get(ownership.agentId);
  const deliveredIncomingRequest = database.prepare(`SELECT 1
    FROM workflow_requests AS request
    JOIN direct_signal_messages AS message ON message.message_id = request.request_id
    WHERE request.responder_agent_id = ? AND request.responder_activation_id = ?
      AND request.status = 'open' AND message.delivery_status = 'delivered'
    LIMIT 1`).get(ownership.agentId, recovery.replacement_activation_id);
  const undeclaredCorrection = database.prepare(`SELECT 1 FROM undeclared_settlement_episodes
    WHERE agent_id = ? AND status = 'open' LIMIT 1`).get(ownership.agentId);
  if (human || projectablePendingInput || !deferredPendingInput || deliveredIncomingRequest || undeclaredCorrection) {
    return false;
  }
  const claimed = database.prepare(`UPDATE activation_recoveries
    SET continuation_state = 'none', updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'active' AND continuation_state = 'pending'`).run(
    now, recovery.failed_activation_id,
  );
  if (Number(claimed.changes) !== 1) return false;
  const released = database.prepare(`UPDATE agent_activations
    SET open_state = 'waiting', revision = revision + 1,
        interrupt_turn_sequence = NULL, interrupt_requested_at_ms = NULL,
        updated_at_ms = ?
    WHERE activation_id = ? AND phase = 'open' AND open_state = 'active'`).run(
    now, recovery.replacement_activation_id,
  );
  if (Number(released.changes) !== 1) {
    throw new WorkflowProtocolError("StaleLifecycleTransition", "Deferred recovery projection lost its active replacement");
  }
  return true;
}

/**
 * Record transcript evidence that was projected before this replacement could
 * consume it. The first such fact is enough: one provider continuation sees
 * every canonical item already present in the transcript.
 */
export function recordRecoveryContinuationEvidence(
  database: DatabaseSync,
  input: {
    activationId: string;
    evidenceKind: RecoveryContinuationEvidenceKind;
    evidenceId: string;
    now: number;
  },
): boolean {
  const updated = database.prepare(`UPDATE activation_recoveries
    SET continuation_state = CASE
          WHEN continuation_state IN ('none', 'consumed') THEN 'pending'
          ELSE continuation_state
        END,
        continuation_evidence_kind = ?, continuation_evidence_id = ?, updated_at_ms = ?
    WHERE replacement_activation_id = ? AND state = 'active'`).run(
    input.evidenceKind,
    input.evidenceId,
    input.now,
    input.activationId,
  );
  return Number(updated.changes) === 1;
}

/** Claim the one non-actionable Pi turn needed to resume recovered visible work. */
export function claimRecoveryContinuation(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  now: number,
): RecoveryContinuationClaim | undefined {
  const recovery = database.prepare(`SELECT failed_activation_id, replacement_activation_id,
      continuation_evidence_id
    FROM activation_recoveries
    WHERE state = 'active' AND replacement_activation_id = ? AND replacement_run_id = ?
      AND replacement_fencing_epoch = ? AND continuation_state = 'pending'`).get(
    ownership.runId, ownership.runId, ownership.epoch,
  ) as {
    failed_activation_id: string;
    replacement_activation_id: string;
    continuation_evidence_id: string | null;
  } | undefined;
  if (!recovery) return undefined;
  const activation = database.prepare(`SELECT phase, open_state FROM agent_activations
    WHERE activation_id = ? AND agent_id = ? AND run_id = ? AND fencing_epoch = ?`).get(
    recovery.replacement_activation_id, ownership.agentId, ownership.runId, ownership.epoch,
  ) as { phase: "open" | "ended"; open_state: "active" | "waiting" | "interrupted" | null } | undefined;
  if (!activation || activation.phase !== "open") return undefined;
  const human = database.prepare(`SELECT status FROM human_interrupts
    WHERE agent_id = ? AND activation_id = ? AND status IN ('pending', 'response-bound', 'result-pending')
    LIMIT 1`).get(ownership.agentId, recovery.replacement_activation_id) as {
      status: "pending" | "response-bound" | "result-pending";
    } | undefined;
  // A pending Human Interrupt remains owner-paused (DECIDE). Bound input and
  // result replay are scheduled only by the Human bridge, which can project the
  // canonical tool result before it wakes the model.
  if (human) return undefined;
  // A remaining pointer is still owned by the Inbox Router. Once a pre-crash
  // projection is confirmed, pointer removal records explicit evidence below;
  // otherwise the actual Inbox Batch owns the useful model turn.
  const hasPendingInput = Boolean(database.prepare(`SELECT 1 FROM pending_message_pointers
    WHERE recipient_agent_id = ? LIMIT 1`).get(ownership.agentId));
  if (hasPendingInput) return undefined;
  const deliveredIncomingRequest = Boolean(database.prepare(`SELECT 1
    FROM workflow_requests AS request
    JOIN direct_signal_messages AS message ON message.message_id = request.request_id
    WHERE request.responder_agent_id = ? AND request.responder_activation_id = ?
      AND request.status = 'open' AND message.delivery_status = 'delivered'
    LIMIT 1`).get(ownership.agentId, recovery.replacement_activation_id));
  const deliveredCorrection = Boolean(database.prepare(`SELECT 1 FROM undeclared_settlement_episodes
    WHERE agent_id = ? AND status = 'open' AND notice_delivered = 1 LIMIT 1`).get(ownership.agentId));
  if (!recovery.continuation_evidence_id && !deliveredIncomingRequest && !deliveredCorrection) return undefined;
  const claimed = database.prepare(`UPDATE activation_recoveries
    SET continuation_state = 'projecting', updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'active' AND continuation_state = 'pending'`).run(
    now, recovery.failed_activation_id,
  );
  if (Number(claimed.changes) !== 1) return undefined;
  return createAutomaticRecoveryContinuation({
    failedActivationId: recovery.failed_activation_id,
    replacementActivationId: recovery.replacement_activation_id,
  });
}

/** A new process may retry a scheduler send that never reached a context hook. */
export function rearmRecoveryContinuation(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  now: number,
): boolean {
  const updated = database.prepare(`UPDATE activation_recoveries
    SET continuation_state = 'pending', updated_at_ms = ?
    WHERE state = 'active' AND replacement_activation_id = ? AND replacement_run_id = ?
      AND replacement_fencing_epoch = ? AND continuation_state = 'projecting'`).run(
    now,
    ownership.runId,
    ownership.runId,
    ownership.epoch,
  );
  return Number(updated.changes) === 1;
}

/** Provider-context observation is the only durable consumption acknowledgement. */
export function confirmRecoveryContinuationContext(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  observedProjectionIds: string[],
  now: number,
): boolean {
  const recovery = database.prepare(`SELECT failed_activation_id, replacement_activation_id,
      continuation_state, continuation_evidence_id
    FROM activation_recoveries
    WHERE state = 'active' AND replacement_activation_id = ? AND replacement_run_id = ?
      AND replacement_fencing_epoch = ?`).get(
    ownership.runId,
    ownership.runId,
    ownership.epoch,
  ) as {
    failed_activation_id: string;
    replacement_activation_id: string;
    continuation_state: RecoveryContinuationState;
    continuation_evidence_id: string | null;
  } | undefined;
  if (!recovery) return false;
  const expectedProjectionId = createAutomaticRecoveryContinuation({
    failedActivationId: recovery.failed_activation_id,
    replacementActivationId: recovery.replacement_activation_id,
  }).projectionId;
  const schedulerObserved = recovery.continuation_state === "projecting"
    && observedProjectionIds.includes(expectedProjectionId);
  const evidenceObserved = recovery.continuation_state === "pending"
    && recovery.continuation_evidence_id !== null;
  if (!schedulerObserved && !evidenceObserved) return false;
  const updated = database.prepare(`UPDATE activation_recoveries
    SET continuation_state = 'consumed', updated_at_ms = ?
    WHERE failed_activation_id = ? AND state = 'active' AND continuation_state = ?`).run(
    now,
    recovery.failed_activation_id,
    recovery.continuation_state,
  );
  return Number(updated.changes) === 1;
}

export function abandonRecoveryContinuation(
  database: DatabaseSync,
  ownership: AgentRunOwnership,
  now: number,
): void {
  database.prepare(`UPDATE activation_recoveries SET continuation_state = 'pending', updated_at_ms = ?
    WHERE state = 'active' AND replacement_activation_id = ? AND replacement_run_id = ?
      AND replacement_fencing_epoch = ? AND continuation_state = 'projecting'`).run(
    now, ownership.runId, ownership.runId, ownership.epoch,
  );
}

export function recoveryForAgent(database: DatabaseSync, agent: AgentReference): ActivationRecoveryRecord | undefined {
  const owner = database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1")
    .get() as { owner_agent_id: string } | undefined;
  if (!owner || owner.owner_agent_id !== agent.workflowOwnerId) {
    throw new WorkflowProtocolError("WorkflowMismatch", "Recovery inspection belongs to another Workflow");
  }
  const row = database.prepare(`SELECT * FROM activation_recoveries WHERE agent_id = ?
    ORDER BY created_at_ms DESC, failed_activation_id DESC LIMIT 1`).get(agent.agentId) as RecoveryRow | undefined;
  return row ? mapRecovery(row) : undefined;
}

export class ActivationRecoveryStore {
  readonly #database: DatabaseSync;
  #closed = false;
  constructor(databasePath: string) {
    this.#database = new DatabaseSync(databasePath, { timeout: 5_000 });
    this.#database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;");
    initializeActivationRecoverySchema(this.#database);
  }
  close(): void { if (!this.#closed) { this.#database.close(); this.#closed = true; } }
  listClaimable(workflowOwnerId: string): ActivationRecoveryRecord[] {
    return (this.#database.prepare(`SELECT * FROM activation_recoveries WHERE state = 'pending'
      ORDER BY created_at_ms, failed_activation_id`).all() as unknown as RecoveryRow[])
      .filter(() => this.#workflowOwnerId() === workflowOwnerId)
      .map(mapRecovery);
  }
  listInFlight(workflowOwnerId: string): ActivationRecoveryRecord[] {
    if (this.#workflowOwnerId() !== workflowOwnerId) {
      throw new WorkflowProtocolError("WorkflowMismatch", "Recovery reconciliation belongs to another Workflow");
    }
    return (this.#database.prepare(`SELECT * FROM activation_recoveries
      WHERE state IN ('launching', 'active')
      ORDER BY created_at_ms, failed_activation_id`).all() as unknown as RecoveryRow[]).map(mapRecovery);
  }
  claimRun(
    workflowOwnerId: string,
    failedActivationId: string,
    runId: string,
    now: number,
    preparedSurface?: string,
  ): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
    return this.#transaction(() => claimRecoveryRun(this.#database, {
      workflowOwnerId,
      failedActivationId,
      runId,
      now,
      preparedSurface,
    }));
  }
  preparePaneIntent(input: {
    workflowOwnerId: string;
    failedActivationId: string;
    intentId: string;
    runId: string;
    workspaceId: string;
    label: string;
    cwd: string;
    now: number;
  }): RecoveryPaneIntent | undefined {
    return this.#transaction(() => prepareRecoveryPaneIntent(this.#database, input));
  }
  beginPaneCreation(input: { intentId: string; runId: string; now: number }): RecoveryPaneIntent {
    return this.#transaction(() => beginRecoveryPaneCreation(this.#database, input));
  }
  recordPaneCreated(input: { intentId: string; runId: string; surface: string; now: number }): RecoveryPaneIntent {
    return this.#transaction(() => recordRecoveryPaneCreated(this.#database, input));
  }
  promotePaneIntent(input: {
    workflowOwnerId: string;
    failedActivationId: string;
    intentId: string;
    runId: string;
    surface: string;
    now: number;
  }): { recovery: ActivationRecoveryRecord; ownership: AgentRunOwnership } | undefined {
    return this.#transaction(() => promoteRecoveryPaneIntent(this.#database, input));
  }
  beginPaneCleanup(input: { intentId: string; now: number; detail: string }): RecoveryPaneIntent | undefined {
    return this.#transaction(() => beginRecoveryPaneCleanup(this.#database, input));
  }
  completePaneCleanup(input: { intentId: string; expectedSurface?: string }): boolean {
    return this.#transaction(() => completeRecoveryPaneCleanup(this.#database, input));
  }
  retirePaneIntent(intentId: string): boolean {
    return this.#transaction(() => retireRecoveryPaneIntent(this.#database, intentId));
  }
  inspectPaneIntent(intentId: string): RecoveryPaneIntent | undefined {
    return readRecoveryPaneIntentById(this.#database, intentId);
  }
  listPaneIntents(workflowOwnerId: string): RecoveryPaneIntent[] {
    if (this.#workflowOwnerId() !== workflowOwnerId) {
      throw new WorkflowProtocolError("WorkflowMismatch", "Recovery pane intent belongs to another Workflow");
    }
    return (this.#database.prepare(`SELECT * FROM recovery_pane_intents
      ORDER BY created_at_ms, intent_id`).all() as unknown as RecoveryPaneIntentRow[]).map(mapRecoveryPaneIntent);
  }
  abandon(
    failedActivationId: string,
    ownership: AgentRunOwnership,
    now: number,
    detail: string,
    expectedCheckpoint?: string,
  ): void {
    this.#transaction(() => abandonRecoveryLaunch(this.#database, {
      failedActivationId,
      ownership,
      now,
      detail,
      expectedCheckpoint,
    }));
  }
  inspect(agent: AgentReference): ActivationRecoveryRecord | undefined { return recoveryForAgent(this.#database, agent); }
  isPendingFailedActivation(agentId: string, failedActivationId: string): boolean {
    return Boolean(this.#database.prepare(`SELECT 1 FROM activation_recoveries
      WHERE agent_id = ? AND failed_activation_id = ? AND state = 'pending'`).get(agentId, failedActivationId));
  }
  ownsFailedActivation(agentId: string, failedActivationId: string): boolean {
    // Once a recovery episode owns a failed activation, its legacy watcher
    // result is permanently superseded—even after replacement resolution.
    return Boolean(this.#database.prepare(`SELECT 1 FROM activation_recoveries
      WHERE agent_id = ? AND failed_activation_id = ?`)
      .get(agentId, failedActivationId));
  }
  isLaunchingReplacement(ownership: AgentRunOwnership): boolean {
    return Boolean(this.#database.prepare(`SELECT 1 FROM activation_recoveries
      WHERE state = 'launching' AND replacement_run_id = ? AND replacement_fencing_epoch = ?
        AND agent_id = ?`).get(ownership.runId, ownership.epoch, ownership.agentId));
  }
  resolvedUnneededReplacement(ownership: AgentRunOwnership): string | undefined {
    const row = this.#database.prepare(`SELECT failed_activation_id FROM activation_recoveries
      WHERE state = 'resolved' AND agent_id = ? AND replacement_run_id = ?
        AND replacement_fencing_epoch = ? AND replacement_activation_id IS NULL`).get(
      ownership.agentId,
      ownership.runId,
      ownership.epoch,
    ) as { failed_activation_id: string } | undefined;
    return row?.failed_activation_id;
  }
  releaseDeferredProjection(ownership: AgentRunOwnership, now: number): boolean {
    return this.#transaction(() => releaseDeferredRecoveryProjection(this.#database, ownership, now));
  }
  claimContinuation(ownership: AgentRunOwnership, now: number): RecoveryContinuationClaim | undefined {
    return this.#transaction(() => claimRecoveryContinuation(this.#database, ownership, now));
  }
  rearmContinuation(ownership: AgentRunOwnership, now: number): boolean {
    return this.#transaction(() => rearmRecoveryContinuation(this.#database, ownership, now));
  }
  confirmContinuationContext(
    ownership: AgentRunOwnership,
    observedProjectionIds: string[],
    now: number,
  ): boolean {
    return this.#transaction(() => confirmRecoveryContinuationContext(
      this.#database,
      ownership,
      observedProjectionIds,
      now,
    ));
  }
  abandonContinuation(ownership: AgentRunOwnership, now: number): void {
    this.#transaction(() => abandonRecoveryContinuation(this.#database, ownership, now));
  }
  #workflowOwnerId(): string {
    const row = this.#database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1").get() as { owner_agent_id: string } | undefined;
    if (!row) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
    return row.owner_agent_id;
  }
  #transaction<T>(work: () => T): T {
    this.#database.exec("BEGIN IMMEDIATE");
    try { const result = work(); this.#database.exec("COMMIT"); return result; }
    catch (error) { if (this.#database.isTransaction) this.#database.exec("ROLLBACK"); throw error; }
  }
}

export function serializePreparedRecoveryCheckpoint(input: {
  surface: string;
  runId: string;
  fencingEpoch: number;
}): string {
  return JSON.stringify({
    kind: "automatic-recovery",
    surface: input.surface,
    runId: input.runId,
    fencingEpoch: input.fencingEpoch,
    phase: "prepared",
  });
}

function hasRecoveryPendingWork(database: DatabaseSync, activationId: string, agentId: string): boolean {
  const request = database.prepare(`SELECT 1 FROM workflow_requests WHERE
    (responder_activation_id = ? AND status = 'open') OR
    (requester_activation_id = ? AND (status IN ('open', 'answered') OR (status = 'orphaned' AND orphan_notice_delivery_status = 'queued')))
    LIMIT 1`).get(activationId, activationId);
  if (request) return true;
  if (database.prepare("SELECT 1 FROM pending_message_pointers WHERE recipient_agent_id = ? LIMIT 1").get(agentId)) return true;
  if (database.prepare("SELECT 1 FROM human_interrupts WHERE agent_id = ? AND status IN ('pending', 'response-bound', 'result-pending') LIMIT 1").get(agentId)) return true;
  if (database.prepare("SELECT 1 FROM undeclared_settlement_episodes WHERE agent_id = ? AND status = 'open' LIMIT 1").get(agentId)) return true;
  return Boolean(database.prepare(`SELECT 1 FROM activation_dependencies
    WHERE activation_id = ? AND dependency_kind = 'operation' LIMIT 1`).get(activationId));
}

function readRecovery(database: DatabaseSync, failedActivationId: string): ActivationRecoveryRecord | undefined {
  const row = database.prepare("SELECT * FROM activation_recoveries WHERE failed_activation_id = ?")
    .get(failedActivationId) as RecoveryRow | undefined;
  return row ? mapRecovery(row) : undefined;
}

function assertWorkflowOwner(database: DatabaseSync, workflowOwnerId: string): void {
  const owner = database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1")
    .get() as { owner_agent_id: string } | undefined;
  if (!owner || owner.owner_agent_id !== workflowOwnerId) {
    throw new WorkflowProtocolError("WorkflowMismatch", "Recovery pane intent belongs to another Workflow");
  }
}

function intentAgentOwnerId(database: DatabaseSync): string {
  const owner = database.prepare("SELECT owner_agent_id FROM workflow_metadata WHERE singleton = 1")
    .get() as { owner_agent_id: string } | undefined;
  if (!owner) throw new WorkflowProtocolError("WorkflowMismatch", "Durable Workflow is not initialized");
  return owner.owner_agent_id;
}

function assertPaneIntentInput(input: {
  intentId: string;
  runId: string;
  workspaceId: string;
  label: string;
  cwd: string;
}): void {
  for (const [name, value] of Object.entries(input)) {
    if (name === "intentId" || name === "runId" || name === "workspaceId" || name === "label" || name === "cwd") {
      if (!value) throw new Error(`Recovery pane intent ${name} must not be empty`);
    }
  }
}

function recoveryAgentId(database: DatabaseSync, failedActivationId: string): string | undefined {
  return (database.prepare(`SELECT agent_id FROM activation_recoveries
    WHERE failed_activation_id = ?`).get(failedActivationId) as { agent_id: string } | undefined)?.agent_id;
}

function readRecoveryPaneIntent(
  database: DatabaseSync,
  failedActivationId: string,
): RecoveryPaneIntent | undefined {
  const row = database.prepare(`SELECT * FROM recovery_pane_intents
    WHERE failed_activation_id = ?`).get(failedActivationId) as RecoveryPaneIntentRow | undefined;
  return row ? mapRecoveryPaneIntent(row) : undefined;
}

function readRecoveryPaneIntentById(
  database: DatabaseSync,
  intentId: string,
): RecoveryPaneIntent | undefined {
  const row = database.prepare(`SELECT * FROM recovery_pane_intents
    WHERE intent_id = ?`).get(intentId) as RecoveryPaneIntentRow | undefined;
  return row ? mapRecoveryPaneIntent(row) : undefined;
}

function requireRecoveryPaneIntent(database: DatabaseSync, intentId: string): RecoveryPaneIntent {
  const intent = readRecoveryPaneIntentById(database, intentId);
  if (!intent) {
    throw new WorkflowProtocolError(
      "RecoveryActivationClaimed",
      `Unknown recovery pane intent ${intentId}`,
    );
  }
  return intent;
}

function mapRecovery(row: RecoveryRow): ActivationRecoveryRecord {
  return {
    failedActivationId: row.failed_activation_id,
    agentId: row.agent_id,
    state: row.state,
    ...(row.replacement_run_id ? { replacementRunId: row.replacement_run_id } : {}),
    ...(row.replacement_fencing_epoch != null ? { replacementFencingEpoch: Number(row.replacement_fencing_epoch) } : {}),
    ...(row.replacement_activation_id ? { replacementActivationId: row.replacement_activation_id } : {}),
    ...(row.exhaustion_activation_id ? { exhaustionActivationId: row.exhaustion_activation_id } : {}),
    ...(row.detail ? { detail: row.detail } : {}),
    createdAtMs: Number(row.created_at_ms),
    updatedAtMs: Number(row.updated_at_ms),
  };
}

function mapRecoveryPaneIntent(row: RecoveryPaneIntentRow): RecoveryPaneIntent {
  return {
    intentId: row.intent_id,
    failedActivationId: row.failed_activation_id,
    agentId: row.agent_id,
    runId: row.run_id,
    workspaceId: row.workspace_id,
    label: row.label,
    cwd: row.cwd,
    ...(row.surface ? { surface: row.surface } : {}),
    state: row.state,
    ...(row.detail ? { detail: row.detail } : {}),
    createdAtMs: Number(row.created_at_ms),
    updatedAtMs: Number(row.updated_at_ms),
  };
}
