export interface AgentCapabilityConfiguration {
  spawning: boolean;
}

export interface WorkflowRecord {
  ownerAgentId: string;
  ownerSessionPath: string;
  directory: string;
  sessionsDirectory: string;
  databasePath: string;
  createdAtMs: number;
}

export interface AgentReference {
  workflowOwnerId: string;
  agentId: string;
}

export interface AgentRecord extends AgentReference {
  sessionPath: string;
  name: string;
  agentDefinition?: string;
  spawnerAgentId?: string;
  capabilities: AgentCapabilityConfiguration;
  createdAtMs: number;
}

export interface AgentRunOwnership extends AgentReference {
  runId: string;
  resourceId: string;
  epoch: number;
}

export type WorkflowProtocolErrorCode =
  | "InvalidSessionIdentity"
  | "WorkflowMismatch"
  | "UnknownAgent"
  | "AgentAlreadyExists"
  | "SpawnerCapabilityRequired"
  | "InvalidSpawner"
  | "TranscriptOutsideWorkflow"
  | "AgentRunAlreadyOwned"
  | "OwnershipLost"
  | "OwnerActivationForbidden"
  | "ActivationAlreadyOpen"
  | "InvalidLifecycleTransition"
  | "StaleLifecycleTransition"
  | "UnknownLifecycleDependency"
  | "RecipientUnreachable"
  | "RecipientEnded"
  | "MessageIdentityConflict"
  | "InvalidMessageSource";

export class WorkflowProtocolError extends Error {
  readonly code: WorkflowProtocolErrorCode;

  constructor(code: WorkflowProtocolErrorCode, message: string) {
    super(message);
    this.name = "WorkflowProtocolError";
    this.code = code;
  }
}
