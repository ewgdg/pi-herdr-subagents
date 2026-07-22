export interface AgentCapabilityConfiguration {
  spawning: boolean;
}

/** Immutable least-privilege policy captured when a Pi-backed Agent is created. */
export interface AgentLaunchPolicy {
  toolAllowlist?: string;
  denyTools: string[];
  codingAgentDir?: string;
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
  launchPolicy?: AgentLaunchPolicy;
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
  | "HumanInterruptForbidden"
  | "HumanInterruptAlreadyPending"
  | "HumanInterruptAlreadyBound"
  | "HumanInterruptTerminal"
  | "HumanInterruptResponseMissing"
  | "RecipientUnreachable"
  | "RecipientEnded"
  | "RecipientReactivationUnauthorized"
  | "MessageIdentityConflict"
  | "InvalidMessageSource"
  | "InvalidCompletionMessage"
  | "CompletionBlocked"
  | "AcceptanceInDoubt"
  | "AnswerUnauthorized"
  | "AnswerAlreadyClosed"
  | "UnknownRequest";

export class WorkflowProtocolError extends Error {
  readonly code: WorkflowProtocolErrorCode;

  constructor(code: WorkflowProtocolErrorCode, message: string) {
    super(message);
    this.name = "WorkflowProtocolError";
    this.code = code;
  }
}
