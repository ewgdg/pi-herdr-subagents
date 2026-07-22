# Goal

Implement issue #25 end to end: requester-only durable Request cancellation, atomic arbitration with Answers, suppression before Request delivery, actionable cancellation notice delivery after Request delivery, and all resulting tool and state projections.

# Intention

Make cancellation a terminal Request outcome rather than an Answer or an Agent activation operation. The Request row is the canonical cancellation record. Existing inbox ordering and delivery machinery carries a runtime-authored Protocol Notice only when the responder has already consumed the Request.

# Scope and Constraints

- Only the original requester may cancel an unresolved Request.
- The first durable Answer acceptance or Request cancellation commit wins atomically.
- An accepted but undelivered Request is suppressed without waking its responder.
- A delivered Request creates exactly one correlated Steer Protocol Notice whose payload is canonical in the Request record.
- Repeated cancellation by the requester is idempotent.
- Cancelled Requests no longer block requester completion or appear as requester dependencies.
- `agent_cancel` cancels Requests only. Agent activation cancellation from issue #26 is out of scope.
- No compatibility migration for older coordination databases is included.
- Tests are written red-first at the protocol runtime and Pi tool seams.

# Work Plan

1. Add failing protocol tests for authority, durable/idempotent cancellation, undelivered suppression, delivered notice delivery/wake, and Answer-versus-cancellation arbitration.
2. Implement the Request cancellation state and atomic store operation, then route durable Protocol Notices through the recipient inbox.
3. Add failing projection/completion tests and update inspection, dependency, and completion queries.
4. Add failing extension tests and implement/register the request-only `agent_cancel` tool.
5. Document Request cancellation, tool behavior, and notice semantics in README.
6. Run targeted tests, the full test suite, lint, and `git diff --check`.

# Validation

- `node --test test/protocol/request-cancellation.test.ts test/protocol/agent-cancel-extension.test.ts test/protocol/workflow-inspection.test.ts test/protocol/completion-gate.test.ts`
- `npm test`
- `npm run lint`
- `git diff --check`

# Progress

- 2026-07-22: Reviewed issue #25, accepted protocol vocabulary, current messaging/inbox persistence, inspection, completion gate, and extension registration. Created this plan.
- 2026-07-22: Added protocol tests first, then implemented the cancelled Request state, suppression, Answer arbitration, canonical notice persistence, Router delivery, and restart recovery.
- 2026-07-22: Added `agent_cancel`, cancellation-aware inspection/dependency/completion projections, tool registration/deny behavior, and README documentation.
- 2026-07-22: Review found a cancellation-versus-Inbox-projection race. Added durable projection arbitration, stale-snapshot revalidation, suppressed-confirmation handling, and a regression test.
- 2026-07-22: Follow-up review found a projection-persistence crash window, missing child `agent_cancel` registration, and lossy one-shot notice scheduling. Added recoverable projection claims, child registration/denial, and periodic durable inbox draining with deterministic regression tests.
- 2026-07-22: Targeted cancellation, extension, inspection, completion, routing, and registration tests pass. Full suite passes all 378 tests; lint and diff checks pass.
- 2026-07-22: Final review found that transcript persistence could escape the transaction that wrote `projection_committed`. Added a separately committed pre-projection claim so cancellation queues a notice throughout that ambiguity window, plus an exact transcript-present/marker-absent regression.
- 2026-07-22: Final targeted tests pass. Full suite passes all 379 tests; lint and diff checks pass.

# Decisions

- Keep cancellation as a fourth terminal Request state (`cancelled`); do not fabricate an Answer.
- Persist cancellation notice identity, payload, and delivery facts on the Request. A normal message row and pending pointer provide ordering only.
- Reuse the recipient acceptance sequence and Inbox Batch delivery path so cancellation notices deduplicate, survive restart, and wake with ordinary Steer semantics.
- The public `agent_cancel` target is a Request ID only, preventing accidental expansion into issue #26 Agent activation cancellation.

# Surprises and Discoveries

- The existing Request dependency projection treated every non-resolved status as unresolved. Cancellation required narrowing all dependency and completion queries to the genuinely unresolved `open|answered` states, including accepted-message reconciliation.
- A best-effort IPC scheduling hint is sufficient after delivered-Request cancellation because the notice pointer is already durable; Router startup independently drains it after process loss.
- Protocol Notices can share the existing message ordering table without pretending to be Agent-authored by keeping their canonical payload on the Request and projecting a distinct sender-free Inbox message shape.
- Inbox projection first commits a durable `projection_claimed` intent, then runs the synchronous transcript projector inside a second SQLite write fence before marking `projection_committed`. Cancellation suppresses only an unclaimed Request, so an external append that escapes rollback always receives a notice.
- A pre-projection claim means delivery may have occurred, not that transcript persistence completed. A replacement Router confirms transcript evidence when present; otherwise it releases the claim and reprojects pending inputs in acceptance order.
- Schedule IPC remains a latency optimization. Each live Router periodically drains its durable inbox, so a lost scheduling hint cannot strand a cancellation notice while its recipient waits.

# Outcomes and Retrospective

Issue #25 is implemented within its accepted boundary. Request cancellation is requester-only, durable, idempotent, and serialized with Answer acceptance. Undelivered Requests are suppressed; delivered Requests receive one durable, correlated Steer Protocol Notice. Cancelled Requests disappear from requester dependency and completion blockers while pending notice delivery remains an ordinary accepted-input blocker for the responder. Inspection and README now expose the resulting state without duplicating canonical notice payloads. Agent activation cancellation remains untouched for issue #26.
