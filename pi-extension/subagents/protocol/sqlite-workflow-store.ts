import { DatabaseSync } from "node:sqlite";
import {
  WorkflowProtocolError,
  type AgentCapabilityConfiguration,
  type AgentRecord,
  type WorkflowRecord,
} from "./workflow-types.ts";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;

interface WorkflowRow {
  owner_agent_id: string;
  owner_session_path: string;
  created_at_ms: number;
}

export interface StoredWorkflowIdentity {
  ownerAgentId: string;
  ownerSessionPath: string;
  createdAtMs: number;
}

interface AgentRow {
  agent_id: string;
  session_path: string;
  name: string;
  agent_definition: string | null;
  spawner_agent_id: string | null;
  capabilities_json: string;
  created_at_ms: number;
}

export interface OpenOwnerInput {
  workflow: WorkflowRecord;
  ownerName: string;
  capabilities: AgentCapabilityConfiguration;
}

export interface AddAgentInput {
  workflowOwnerId: string;
  agentId: string;
  sessionPath: string;
  name: string;
  agentDefinition?: string;
  spawnerAgentId: string;
  capabilities: AgentCapabilityConfiguration;
  createdAtMs: number;
}

export class SQLiteWorkflowStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS workflow_metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        owner_agent_id TEXT NOT NULL,
        owner_session_path TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL
      ) STRICT;

      CREATE TABLE IF NOT EXISTS workflow_agents (
        agent_id TEXT PRIMARY KEY,
        session_path TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        agent_definition TEXT,
        spawner_agent_id TEXT REFERENCES workflow_agents(agent_id),
        capabilities_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL
      ) STRICT;

      CREATE INDEX IF NOT EXISTS workflow_agents_spawner
      ON workflow_agents (spawner_agent_id, created_at_ms, agent_id);
    `);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  readWorkflowIdentity(): StoredWorkflowIdentity | undefined {
    const workflow = this.#readWorkflowRow();
    return workflow
      ? {
          ownerAgentId: workflow.owner_agent_id,
          ownerSessionPath: workflow.owner_session_path,
          createdAtMs: Number(workflow.created_at_ms),
        }
      : undefined;
  }

  openOwner(input: OpenOwnerInput): WorkflowRecord {
    const createdAtMs = this.#withImmediateTransaction(() => {
      const existing = this.#readWorkflowRow();
      if (existing) {
        if (existing.owner_agent_id !== input.workflow.ownerAgentId) {
          throw new WorkflowProtocolError(
            "WorkflowMismatch",
            `Workflow belongs to ${existing.owner_agent_id}, not ${input.workflow.ownerAgentId}`,
          );
        }
        if (existing.owner_session_path !== input.workflow.ownerSessionPath) {
          throw new WorkflowProtocolError(
            "WorkflowMismatch",
            `Owner session path conflicts with durable Workflow ${input.workflow.ownerAgentId}`,
          );
        }
        const owner = this.#readAgent(existing.owner_agent_id);
        if (!owner || owner.spawnerAgentId) {
          throw new Error(`Workflow Owner record is missing or invalid: ${existing.owner_agent_id}`);
        }
        return Number(existing.created_at_ms);
      }

      this.#database.prepare(`
        INSERT INTO workflow_metadata (
          singleton, owner_agent_id, owner_session_path, created_at_ms
        ) VALUES (1, ?, ?, ?)
      `).run(
        input.workflow.ownerAgentId,
        input.workflow.ownerSessionPath,
        input.workflow.createdAtMs,
      );
      this.#database.prepare(`
        INSERT INTO workflow_agents (
          agent_id, session_path, name, agent_definition,
          spawner_agent_id, capabilities_json, created_at_ms
        ) VALUES (?, ?, ?, NULL, NULL, ?, ?)
      `).run(
        input.workflow.ownerAgentId,
        input.workflow.ownerSessionPath,
        input.ownerName,
        serializeCapabilities(input.capabilities),
        input.workflow.createdAtMs,
      );
      return input.workflow.createdAtMs;
    });

    return { ...input.workflow, createdAtMs };
  }

  openExisting(workflow: WorkflowRecord): WorkflowRecord {
    const existing = this.#readWorkflowRow();
    if (!existing || existing.owner_agent_id !== workflow.ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Durable Workflow does not belong to Owner ${workflow.ownerAgentId}`,
      );
    }
    if (existing.owner_session_path !== workflow.ownerSessionPath) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Owner session path conflicts with durable Workflow ${workflow.ownerAgentId}`,
      );
    }
    this.#requireAgent(workflow.ownerAgentId, workflow.ownerAgentId);
    return { ...workflow, createdAtMs: Number(existing.created_at_ms) };
  }

  addAgent(input: AddAgentInput): AgentRecord {
    return this.#withImmediateTransaction(() => {
      const workflow = this.#requireWorkflow(input.workflowOwnerId);
      const spawner = this.#readAgent(input.spawnerAgentId);
      if (!spawner) {
        throw new WorkflowProtocolError(
          "InvalidSpawner",
          `Spawner is not a member of Workflow ${workflow.owner_agent_id}: ${input.spawnerAgentId}`,
        );
      }
      if (!spawner.capabilities.spawning) {
        throw new WorkflowProtocolError(
          "SpawnerCapabilityRequired",
          `Agent ${input.spawnerAgentId} does not have spawning capability`,
        );
      }
      const existing = this.#readAgent(input.agentId);
      if (existing) {
        throw new WorkflowProtocolError(
          "AgentAlreadyExists",
          `Agent is already a member of Workflow ${existing.workflowOwnerId}: ${input.agentId}`,
        );
      }

      this.#database.prepare(`
        INSERT INTO workflow_agents (
          agent_id, session_path, name, agent_definition,
          spawner_agent_id, capabilities_json, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.agentId,
        input.sessionPath,
        input.name,
        input.agentDefinition ?? null,
        input.spawnerAgentId,
        serializeCapabilities(input.capabilities),
        input.createdAtMs,
      );
      return this.#requireAgent(workflow.owner_agent_id, input.agentId);
    });
  }

  removeAgent(workflowOwnerId: string, agentId: string): void {
    this.#withImmediateTransaction(() => {
      this.#requireWorkflow(workflowOwnerId);
      this.#requireAgent(workflowOwnerId, agentId);
      const child = this.#database.prepare(
        "SELECT 1 AS present FROM workflow_agents WHERE spawner_agent_id = ? LIMIT 1",
      ).get(agentId) as { present: number } | undefined;
      if (child) throw new Error(`Cannot remove Agent with durable descendants: ${agentId}`);
      this.#database.prepare("DELETE FROM workflow_agents WHERE agent_id = ?").run(agentId);
    });
  }

  inspectAgent(workflowOwnerId: string, agentId: string): AgentRecord {
    this.#requireWorkflow(workflowOwnerId);
    return this.#requireAgent(workflowOwnerId, agentId);
  }

  listDirectChildren(workflowOwnerId: string, spawnerAgentId: string): AgentRecord[] {
    this.inspectAgent(workflowOwnerId, spawnerAgentId);
    return (this.#database.prepare(`
      SELECT agent_id, session_path, name, agent_definition,
             spawner_agent_id, capabilities_json, created_at_ms
      FROM workflow_agents
      WHERE spawner_agent_id = ?
      ORDER BY created_at_ms, agent_id
    `).all(spawnerAgentId) as unknown as AgentRow[]).map((row) => mapAgentRow(row, workflowOwnerId));
  }

  listWorkflow(workflowOwnerId: string): AgentRecord[] {
    this.#requireWorkflow(workflowOwnerId);
    return (this.#database.prepare(`
      SELECT agent_id, session_path, name, agent_definition,
             spawner_agent_id, capabilities_json, created_at_ms
      FROM workflow_agents
      ORDER BY created_at_ms, agent_id
    `).all() as unknown as AgentRow[]).map((row) => mapAgentRow(row, workflowOwnerId));
  }

  #requireWorkflow(ownerAgentId: string): WorkflowRow {
    const workflow = this.#readWorkflowRow();
    if (!workflow || workflow.owner_agent_id !== ownerAgentId) {
      throw new WorkflowProtocolError(
        "WorkflowMismatch",
        `Workflow identity is not open in this durable store: ${ownerAgentId}`,
      );
    }
    return workflow;
  }

  #readWorkflowRow(): WorkflowRow | undefined {
    return this.#database.prepare(`
      SELECT owner_agent_id, owner_session_path, created_at_ms
      FROM workflow_metadata
      WHERE singleton = 1
    `).get() as WorkflowRow | undefined;
  }

  #requireAgent(workflowOwnerId: string, agentId: string): AgentRecord {
    const agent = this.#readAgent(agentId, workflowOwnerId);
    if (!agent) {
      throw new WorkflowProtocolError("UnknownAgent", `Unknown Workflow Agent: ${agentId}`);
    }
    return agent;
  }

  #readAgent(agentId: string, workflowOwnerId = this.#readWorkflowRow()?.owner_agent_id): AgentRecord | undefined {
    if (!workflowOwnerId) return undefined;
    const row = this.#database.prepare(`
      SELECT agent_id, session_path, name, agent_definition,
             spawner_agent_id, capabilities_json, created_at_ms
      FROM workflow_agents
      WHERE agent_id = ?
    `).get(agentId) as AgentRow | undefined;
    return row ? mapAgentRow(row, workflowOwnerId) : undefined;
  }

  #withImmediateTransaction<T>(operation: () => T): T {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }
}

function serializeCapabilities(capabilities: AgentCapabilityConfiguration): string {
  return JSON.stringify({ spawning: capabilities.spawning });
}

function mapAgentRow(row: AgentRow, workflowOwnerId: string): AgentRecord {
  const capabilities = JSON.parse(row.capabilities_json) as AgentCapabilityConfiguration;
  return {
    workflowOwnerId,
    agentId: row.agent_id,
    sessionPath: row.session_path,
    name: row.name,
    ...(row.agent_definition ? { agentDefinition: row.agent_definition } : {}),
    ...(row.spawner_agent_id ? { spawnerAgentId: row.spawner_agent_id } : {}),
    capabilities: { spawning: capabilities.spawning === true },
    createdAtMs: Number(row.created_at_ms),
  };
}
