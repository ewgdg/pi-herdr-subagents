# Operation Review

An Operation Dependency represents a runtime operation whose result is not yet established. It remains attached to one activation and blocks both completion paths until durable evidence resolves it.

## Episode identity

Each continuous uncertainty episode has an immutable `operationReviewId`. The dependency ID and original operation identity are attributes of that episode; neither is the episode identity.

Two activations may use the same dependency ID concurrently and receive independent reviews. After one episode resolves, the same activation may use that dependency ID again and receive a fresh review with a new start, deadline, evidence history, reconciliation count, WATCH entry, and incident-trigger allowance.

An activation can have at most one unresolved review for an exact dependency ID. Resolving or deleting a dependency affects only the review attached to that activation.

Recovery transfers the existing dependency and review to the replacement activation and ownership fence. The review ID, start, deadline, attempts, evidence, WATCH episode, and trigger identity do not change.

## Durable state

An Operation Review records:

- its immutable review ID;
- the dependency ID and original operation identity;
- the current activation and Agent Run ownership fence;
- the runtime-selected operation kind;
- the original review start and deadline;
- reconciliation attempts and append-only evidence;
- whether deterministic reconciliation remains eligible;
- the incident trigger for this continuous episode, if one has been emitted.

Evidence and incident triggers reference the review ID. Public inspection and mutation APIs also use the review ID whenever a dependency ID could identify multiple episodes.

## Deadline policy

Runtime policy supplies separate review intervals for message acceptance, activation cancellation, ownership, tracked external side effects, and generic operations. Workflow policy supplies the maximum unattended interval. The persisted deadline is the smaller interval.

Agent messages, tool-call arguments, tool timeouts, output, heartbeats, process activity, and later evidence cannot configure, disable, extend, or reset that deadline. Evidence remains useful for diagnosis without becoming oversight policy.

## Reconciliation

The live Workflow Owner runs workflow-wide Operation Review. A new or due review receives a deterministic probe using its original operation identity and current ownership fence.

Message acceptance and activation cancellation have built-in deterministic adapters backed by their authoritative durable stores. Acceptance probes the original Message Identity. Cancellation probes the original cancellation identity and exact process locator; it may inspect process state but never repeats an external close.

Ownership, external-side-effect, and generic reviews require an `extensionOperationReconciler`. The adapter receives the full immutable review, including `operationReviewId`, `originalIdentity`, operation kind, current activation, and current ownership fence. It must return an explicit reconciliation outcome for that operation kind.

Owner router startup fails before reconciliation timers start when restored unresolved extension reviews exist without the adapter. If an extension review appears later without configuration, scheduled reconciliation reports the configuration failure and retries at the normal discovery interval. Missing configuration never records evidence, increments attempts, resolves a dependency, or emits an incident trigger.

A probe has three legal results:

- **Resolved** — durable evidence establishes the result, so the exact Operation Dependency is removed.
- **Still eligible** — uncertainty and WATCH remain before the deadline. At or after the deadline, one final permitted probe leaves the dependency unresolved and emits the incident trigger.
- **Exhausted** — no safe deterministic reconciliation remains, so WATCH clears and the incident trigger is emitted immediately.

Probe or commit failures reject the reconciliation attempt and leave the review unchanged. They are not converted into evidence. Malformed outcomes fail transactionally.

Elapsed time never supplies a resolved result. Triggering an incident never implies acceptance, cancellation, ownership transfer, external-side-effect success, or activation completion.

## Owner availability

Workflow-wide review timers and incident-trigger production run only while the Workflow Owner router is live. Stopping the Owner pauses that automation without changing durable review state or deadlines.

Recipient Inbox Routers remain independent. Reachable Agents can continue direct message acceptance and delivery while the Owner is offline. Resuming the Owner immediately reconciles new or due reviews, then continues deadline scheduling. Scheduled reconciliation failures are reported through the runtime error-reporting seam.

## Observability

WATCH is projected only for unresolved reviews that remain eligible for deterministic reconciliation. Its key includes `operationReviewId`.

Agent inspection exposes bounded review metadata, including the review ID, without copying evidence details into model context. Review history can be inspected by Agent, and one exact review or its evidence can be inspected by review ID. Mutations also use the review ID. Full evidence remains durable for operational diagnosis.
