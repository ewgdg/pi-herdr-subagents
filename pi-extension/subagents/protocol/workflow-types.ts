export const DELEGATION_POLICIES = ["disabled", "approval-required", "autonomous"] as const;
export type DelegationPolicy = typeof DELEGATION_POLICIES[number];
export const DEFAULT_DELEGATION_POLICY: DelegationPolicy = "approval-required";

export function isDelegationPolicy(value: string | undefined): value is DelegationPolicy {
  return value !== undefined && (DELEGATION_POLICIES as readonly string[]).includes(value);
}

export function persistedDelegationPolicyFor(
  agentDefinition: string | undefined,
  requestedPolicy: DelegationPolicy | undefined,
): DelegationPolicy | undefined {
  if (agentDefinition === "moderator") return undefined;
  return requestedPolicy ?? DEFAULT_DELEGATION_POLICY;
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
  delegationPolicy?: DelegationPolicy;
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
  | "SpawnerDelegationDisabled"
  | "DelegatedActivationApprovalRequired"
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
  | "ActivationIntentRequired"
  | "ActivationIntentForbidden"
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
  | "RequestCancellationUnauthorized"
  | "RequestAlreadyClosed"
  | "ActivationCancellationUnauthorized"
  | "ActivationCancellationConflict"
  | "CancellationInDoubt"
  | "RecoveryActivationClaimed"
  | "UnknownRequest";

export class WorkflowProtocolError extends Error {
  readonly code: WorkflowProtocolErrorCode;

  constructor(code: WorkflowProtocolErrorCode, message: string) {
    super(message);
    this.name = "WorkflowProtocolError";
    this.code = code;
  }
}
