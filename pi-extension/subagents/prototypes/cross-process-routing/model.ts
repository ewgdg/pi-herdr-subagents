/**
 * PROTOTYPE — throwaway state model for GitHub issue:
 * "Prototype cross-process routing and delivery guarantees"
 *
 * Question: Does recipient-finalized local IPC + transactional coordination
 * contract remain coherent through lost acknowledgements, process crashes,
 * transcript/pointer crash windows, duplicate resumes, and terminal recipient
 * disappearance?
 *
 * This is a pure simulator. It performs no filesystem, socket, SQLite, or Pi I/O.
 */

export type DeliveryTiming = "steer" | "deferred";
export type ActivationState = "open" | "failed" | "completed";
export type WorkState = "active" | "waiting" | "interrupted";
export type RunState = "running" | "absent";
export type SenderOutcome =
  | "none"
  | "queued"
  | "acceptance_unknown"
  | "recipient_unreachable"
  | "rejected_permanent";

export interface SimulatedMessage {
  id: string;
  delivery: DeliveryTiming;
  canonicalTranscript: "present";
  acceptanceSequence?: number;
  pointer: "absent" | "pending";
  recipientTranscript: "absent" | "committed";
  senderOutcome: SenderOutcome;
  replayedAcknowledgement: boolean;
  batchId?: string;
}

export interface PrototypeState {
  activation: ActivationState;
  work: WorkState;
  run: RunState;
  runId?: string;
  endpoint: "bound" | "absent";
  ownershipLock: "held" | "free";
  ownershipEpoch: number;
  wake: "none" | "scheduled";
  messages: SimulatedMessage[];
  nextMessage: number;
  nextSequence: number;
  nextBatch: number;
  nextRun: number;
  loseNextAcknowledgement: boolean;
  crashAfterNextTranscriptCommit: boolean;
  events: string[];
}

export type PrototypeAction =
  | { type: "send"; delivery: DeliveryTiming }
  | { type: "retry" }
  | { type: "turnBoundary" }
  | { type: "settle" }
  | { type: "startWake" }
  | { type: "crash" }
  | { type: "resume" }
  | { type: "duplicateResume" }
  | { type: "staleOwnerCommit" }
  | { type: "interrupt" }
  | { type: "complete" }
  | { type: "loseNextAcknowledgement" }
  | { type: "crashAfterNextTranscriptCommit" }
  | { type: "reset" };

const MAX_EVENTS = 7;

export function initialState(): PrototypeState {
  return {
    activation: "open",
    work: "active",
    run: "running",
    runId: "run-1",
    endpoint: "bound",
    ownershipLock: "held",
    ownershipEpoch: 1,
    wake: "none",
    messages: [],
    nextMessage: 1,
    nextSequence: 1,
    nextBatch: 1,
    nextRun: 2,
    loseNextAcknowledgement: false,
    crashAfterNextTranscriptCommit: false,
    events: ["Recipient Agent Run started; Router endpoint bound."],
  };
}

function record(state: PrototypeState, event: string): PrototypeState {
  return { ...state, events: [...state.events, event].slice(-MAX_EVENTS) };
}

function hasPending(state: PrototypeState): boolean {
  return state.messages.some((message) => message.pointer === "pending");
}

function scheduleWakeIfWaiting(state: PrototypeState): PrototypeState {
  if (
    state.activation === "open" &&
    state.run === "running" &&
    state.work === "waiting" &&
    hasPending(state)
  ) {
    return { ...state, wake: "scheduled" };
  }
  return state;
}

function acceptExisting(
  state: PrototypeState,
  messageIndex: number,
  replayed: boolean,
): PrototypeState {
  const message = state.messages[messageIndex];
  const messages = [...state.messages];

  if (message.pointer === "pending" || message.recipientTranscript === "committed") {
    messages[messageIndex] = {
      ...message,
      senderOutcome: "queued",
      replayedAcknowledgement: true,
    };
    return record(
      scheduleWakeIfWaiting({ ...state, messages }),
      `${message.id}: retry reconciled the lost acknowledgement; no duplicate pointer.`,
    );
  }

  if (state.activation === "completed") {
    messages[messageIndex] = {
      ...message,
      senderOutcome: "rejected_permanent",
      replayedAcknowledgement: false,
    };
    return record(
      { ...state, messages },
      `${message.id}: rejected — recipient activation is completed.`,
    );
  }

  if (state.run !== "running" || state.endpoint !== "bound") {
    messages[messageIndex] = {
      ...message,
      senderOutcome: "recipient_unreachable",
      replayedAcknowledgement: false,
    };
    return record(
      { ...state, messages },
      `${message.id}: Router unreachable; canonical call remains retryable.`,
    );
  }

  const loseAck = state.loseNextAcknowledgement;
  messages[messageIndex] = {
    ...message,
    acceptanceSequence: state.nextSequence,
    pointer: "pending",
    senderOutcome: loseAck ? "acceptance_unknown" : "queued",
    replayedAcknowledgement: replayed,
  };

  const accepted = scheduleWakeIfWaiting({
    ...state,
    messages,
    nextSequence: state.nextSequence + 1,
    loseNextAcknowledgement: false,
  });

  return record(
    accepted,
    loseAck
      ? `${message.id}: pointer committed as sequence ${state.nextSequence}, but acknowledgement was lost.`
      : `${message.id}: durably queued as sequence ${state.nextSequence}.`,
  );
}

function send(state: PrototypeState, delivery: DeliveryTiming): PrototypeState {
  const message: SimulatedMessage = {
    id: `msg-${state.nextMessage}`,
    delivery,
    canonicalTranscript: "present",
    pointer: "absent",
    recipientTranscript: "absent",
    senderOutcome: "none",
    replayedAcknowledgement: false,
  };
  const withCanonicalCall = {
    ...state,
    messages: [...state.messages, message],
    nextMessage: state.nextMessage + 1,
  };
  return acceptExisting(withCanonicalCall, withCanonicalCall.messages.length - 1, false);
}

function retryLast(state: PrototypeState): PrototypeState {
  for (let index = state.messages.length - 1; index >= 0; index--) {
    const message = state.messages[index];
    if (
      message.senderOutcome === "acceptance_unknown" ||
      message.senderOutcome === "recipient_unreachable"
    ) {
      return acceptExisting(state, index, true);
    }
  }
  return record(state, "No ambiguous or retryable message exists.");
}

function commitEligibleBatch(
  state: PrototypeState,
  deliveryPoint: "turn-boundary" | "wake",
): PrototypeState {
  if (state.activation !== "open" || state.run !== "running") {
    return record(state, "No live open recipient Router can commit a batch.");
  }
  if (state.work === "interrupted") {
    return record(state, "Interrupted work may queue messages but messaging cannot resume it.");
  }

  const eligible = state.messages.filter(
    (message) =>
      message.pointer === "pending" &&
      message.recipientTranscript === "absent" &&
      (deliveryPoint === "wake" || message.delivery === "steer"),
  );

  if (eligible.length === 0) {
    return record(state, `No messages are eligible at this ${deliveryPoint}.`);
  }

  const batchId = `batch-${state.nextBatch}`;
  const eligibleIds = new Set(eligible.map((message) => message.id));
  const crashWindow = state.crashAfterNextTranscriptCommit;
  const messages = state.messages.map((message) => {
    if (!eligibleIds.has(message.id)) return message;
    return {
      ...message,
      recipientTranscript: "committed" as const,
      pointer: crashWindow ? ("pending" as const) : ("absent" as const),
      batchId,
    };
  });

  if (crashWindow) {
    return record(
      {
        ...state,
        messages,
        nextBatch: state.nextBatch + 1,
        crashAfterNextTranscriptCommit: false,
        activation: "failed",
        run: "absent",
        runId: undefined,
        endpoint: "absent",
        ownershipLock: "free",
        wake: "none",
      },
      `${batchId}: transcript committed, then the process crashed before pointer deletion.`,
    );
  }

  return record(
    {
      ...state,
      messages,
      nextBatch: state.nextBatch + 1,
      wake: "none",
    },
    `${batchId}: committed ${eligible.length} message(s); pointers removed.`,
  );
}

function crash(state: PrototypeState): PrototypeState {
  if (state.run === "absent") return record(state, "No recipient process is running.");
  return record(
    {
      ...state,
      activation: "failed",
      run: "absent",
      runId: undefined,
      endpoint: "absent",
      ownershipLock: "free",
      wake: "none",
    },
    "Recipient process crashed; accepted pointers remain durable.",
  );
}

function resume(state: PrototypeState): PrototypeState {
  if (state.activation === "completed") {
    return record(state, "Resume rejected: completed recipient requires an explicit new activation decision.");
  }
  if (state.run === "running" || state.ownershipLock === "held") {
    return record(state, "Resume rejected: SessionOwned by the existing Agent Run.");
  }

  let reconciled = 0;
  const messages = state.messages.map((message) => {
    if (message.pointer === "pending" && message.recipientTranscript === "committed") {
      reconciled++;
      return { ...message, pointer: "absent" as const };
    }
    return message;
  });
  const runId = `run-${state.nextRun}`;
  let resumed: PrototypeState = {
    ...state,
    activation: "open",
    work: "waiting",
    run: "running",
    runId,
    nextRun: state.nextRun + 1,
    endpoint: "bound",
    ownershipLock: "held",
    ownershipEpoch: state.ownershipEpoch + 1,
    messages,
    wake: "none",
  };
  resumed = scheduleWakeIfWaiting(resumed);
  return record(
    resumed,
    reconciled > 0
      ? `${runId}: recovered; transcript scan reconciled ${reconciled} committed pointer(s).`
      : `${runId}: recovered; pending queue rebuilt and one wake may be scheduled.`,
  );
}

function complete(state: PrototypeState): PrototypeState {
  if (state.activation !== "open" || state.run !== "running") {
    return record(state, "Completion requires a live open activation.");
  }
  if (hasPending(state)) {
    return record(state, "Completion blocked: accepted inbox work must commit first.");
  }
  return record(
    {
      ...state,
      activation: "completed",
      run: "absent",
      runId: undefined,
      endpoint: "absent",
      ownershipLock: "free",
      wake: "none",
    },
    "Activation completed; later messages are permanently rejected.",
  );
}

export function reduce(state: PrototypeState, action: PrototypeAction): PrototypeState {
  switch (action.type) {
    case "send":
      return send(state, action.delivery);
    case "retry":
      return retryLast(state);
    case "turnBoundary":
      return commitEligibleBatch(state, "turn-boundary");
    case "settle": {
      if (state.activation !== "open" || state.run !== "running") {
        return record(state, "Only a live open activation can settle.");
      }
      if (state.work === "interrupted") {
        return record(state, "Interrupted work does not settle into an automatic wake.");
      }
      const waiting = scheduleWakeIfWaiting({ ...state, work: "waiting" });
      return record(waiting, "Agent settled; Deferred messages are now eligible.");
    }
    case "startWake": {
      if (state.wake !== "scheduled") return record(state, "No wake is scheduled.");
      const active = { ...state, work: "active" as const, wake: "none" as const };
      return commitEligibleBatch(active, "wake");
    }
    case "crash":
      return crash(state);
    case "resume":
      return resume(state);
    case "duplicateResume":
      return state.run === "running" || state.ownershipLock === "held"
        ? record(state, "Concurrent resume rejected: kernel ownership is already held.")
        : resume(state);
    case "staleOwnerCommit":
      return state.ownershipEpoch === 1
        ? record(state, "No superseded owner exists yet; crash and resume first.")
        : record(
            state,
            `Stale owner epoch ${state.ownershipEpoch - 1} rejected with OwnershipLost; current epoch is ${state.ownershipEpoch}.`,
          );
    case "interrupt": {
      if (state.activation !== "open" || state.run !== "running") {
        return record(state, "No live activation can be interrupted.");
      }
      const work = state.work === "interrupted" ? "waiting" : "interrupted";
      return record(
        { ...state, work, wake: work === "interrupted" ? "none" : state.wake },
        work === "interrupted"
          ? "Turn interrupted; queued messages cannot resume it."
          : "Operator cleared interruption; recipient is waiting again.",
      );
    }
    case "complete":
      return complete(state);
    case "loseNextAcknowledgement":
      return record(
        { ...state, loseNextAcknowledgement: true },
        "Fault armed: next committed acceptance loses its reply.",
      );
    case "crashAfterNextTranscriptCommit":
      return record(
        { ...state, crashAfterNextTranscriptCommit: true },
        "Fault armed: crash after next Inbox Batch transcript commit.",
      );
    case "reset":
      return initialState();
  }
}
