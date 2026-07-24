import {
  ActivationCancellationStore,
  parseAgentRunLocator,
  type AgentRunTerminator,
} from "./activation-cancellation.ts";
import {
  type OperationReconciliationOutcome,
  type OperationKind,
  type OperationReviewRecord,
} from "./operation-review.ts";
import { DirectSignalStore } from "./sqlite-message-store.ts";

export interface OperationReconciliationContext {
  databasePath: string;
  workflowOwnerId: string;
  agentRunTerminator: AgentRunTerminator;
  now(): number;
}

export type ExtensionOperationKind = Exclude<
  OperationKind,
  "acceptance" | "cancellation"
>;

export type ExtensionOperationReview = OperationReviewRecord & {
  operationKind: ExtensionOperationKind;
};

export type ExtensionOperationReconciler = (
  review: ExtensionOperationReview,
) => OperationReconciliationOutcome | Promise<OperationReconciliationOutcome>;

export function assertOperationReconciliationConfigured(
  reviews: OperationReviewRecord[],
  extensionOperationReconciler: ExtensionOperationReconciler | undefined,
): void {
  if (extensionOperationReconciler) return;
  const extensionReview = reviews.find(isExtensionOperationReview);
  if (!extensionReview) return;
  throw missingExtensionAdapter(extensionReview);
}

export async function reconcileKnownOperation(
  context: OperationReconciliationContext,
  review: OperationReviewRecord,
  extensionOperationReconciler?: ExtensionOperationReconciler,
): Promise<OperationReconciliationOutcome> {
  if (review.operationKind === "acceptance") {
    return reconcileAcceptance(context, review);
  }
  if (review.operationKind === "cancellation") {
    return reconcileCancellation(context, review);
  }
  if (!extensionOperationReconciler) throw missingExtensionAdapter(review);
  return extensionOperationReconciler(review);
}

function isExtensionOperationReview(
  review: OperationReviewRecord,
): review is ExtensionOperationReview {
  return review.operationKind !== "acceptance"
    && review.operationKind !== "cancellation";
}

function missingExtensionAdapter(review: ExtensionOperationReview): Error {
  return new Error(
    `Operation Review extension adapter is required for ${review.operationKind} review ${review.operationReviewId}`,
  );
}

function reconcileAcceptance(
  context: OperationReconciliationContext,
  review: OperationReviewRecord,
): OperationReconciliationOutcome {
  const messages = new DirectSignalStore(context.databasePath);
  try {
    const message = messages.inspectMessage(
      context.workflowOwnerId,
      review.originalIdentity,
    );
    if (!message) {
      return {
        kind: "unresolved",
        eligibility: "exhausted",
        evidence: {
          kind: "acceptance-state-probe",
          detail: `Original Message Identity ${review.originalIdentity} has no durable binding`,
        },
      };
    }
    if (message.deliveryStatus !== "bound") {
      return {
        kind: "resolved",
        evidence: {
          kind: "acceptance-state-probe",
          detail: `Original Message Identity ${review.originalIdentity} is durably ${message.deliveryStatus}`,
        },
      };
    }
    return {
      kind: "unresolved",
      eligibility: "eligible",
      evidence: {
        kind: "acceptance-state-probe",
        detail: `Original Message Identity ${review.originalIdentity} remains durably bound`,
      },
    };
  } finally {
    messages.close();
  }
}

async function reconcileCancellation(
  context: OperationReconciliationContext,
  review: OperationReviewRecord,
): Promise<OperationReconciliationOutcome> {
  const cancellations = new ActivationCancellationStore(context.databasePath);
  try {
    const operation = cancellations.inspectOperation(review.originalIdentity);
    if (!operation) {
      return {
        kind: "unresolved",
        eligibility: "exhausted",
        evidence: {
          kind: "cancellation-state-probe",
          detail: `Original cancellation ${review.originalIdentity} is missing`,
        },
      };
    }
    if (operation.state === "committed") {
      return {
        kind: "resolved",
        evidence: {
          kind: "cancellation-state-probe",
          detail: `Original cancellation ${review.originalIdentity} is durably committed`,
        },
      };
    }
    if (operation.state === "ready-to-commit") {
      const finalized = cancellations.finalize(
        operation.operationId,
        context.now(),
        review.operationReviewId,
      );
      return cancellationFinalizationOutcome(review, finalized.state);
    }
    const locator = operation.runLocator
      ? parseAgentRunLocator(operation.runLocator)
      : undefined;
    if (!locator) {
      return {
        kind: "unresolved",
        eligibility: "eligible",
        evidence: {
          kind: "cancellation-process-probe",
          detail: `Original cancellation ${review.originalIdentity} has no exact process locator`,
        },
      };
    }
    let inspection;
    try {
      inspection = await context.agentRunTerminator.inspect(locator);
    } catch (error) {
      inspection = {
        kind: "unavailable" as const,
        error: error instanceof Error ? error.message : String(error),
      };
    }
    if (inspection.kind === "missing") {
      cancellations.markReady(operation.operationId, context.now());
      const finalized = cancellations.finalize(
        operation.operationId,
        context.now(),
        review.operationReviewId,
      );
      return finalized.state === "committed"
        ? {
            kind: "resolved",
            evidence: {
              kind: "cancellation-process-probe",
              detail: `Exact process for cancellation ${review.originalIdentity} is confirmed absent`,
            },
          }
        : cancellationFinalizationOutcome(review, finalized.state);
    }
    return {
      kind: "unresolved",
      eligibility: "eligible",
      evidence: {
        kind: "cancellation-process-probe",
        detail: inspection.kind === "present"
          ? `Exact process for cancellation ${review.originalIdentity} remains present`
          : inspection.error
            ?? `Exact process for cancellation ${review.originalIdentity} is unobservable`,
      },
    };
  } finally {
    cancellations.close();
  }
}

function cancellationFinalizationOutcome(
  review: OperationReviewRecord,
  state: "terminating" | "ready-to-commit" | "in-doubt" | "committed",
): OperationReconciliationOutcome {
  return state === "committed"
    ? {
        kind: "resolved",
        evidence: {
          kind: "cancellation-state-probe",
          detail: `Original cancellation ${review.originalIdentity} retained confirmed termination`,
        },
      }
    : {
        kind: "unresolved",
        eligibility: "eligible",
        evidence: {
          kind: "cancellation-state-probe",
          detail: `Original cancellation ${review.originalIdentity} remains in doubt after exact revalidation`,
        },
      };
}
