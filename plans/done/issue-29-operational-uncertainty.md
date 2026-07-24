# Goal

Implement GitHub issue #29 so unresolved runtime operations remain durable, receive runtime-owned review deadlines, project WATCH only while deterministic reconciliation is eligible, and emit one durable incident trigger when reconciliation requires operational judgment.

# Intention

Make operational uncertainty a first-class durable protocol record attached to the existing activation Operation Dependency. Keep identity, ownership fencing, evidence, deadline policy, reconciliation eligibility, and incident-trigger deduplication together without turning elapsed time into an operation result.

# Scope & Constraints

- Cover acceptance, activation cancellation, ownership, tracked external side effects, and otherwise-classified operation dependencies.
- Preserve the existing Message Identity and cancellation operation identity during retries.
- The Workflow Owner runtime owns workflow-wide review. Closing the Owner pauses review; direct recipient-router communication remains independent.
- Issue #29 emits durable incident triggers only. Incident creation, scope, briefs, and Moderators remain issue #30/#31 work.
- Runtime policy selects operation-specific intervals; Workflow policy caps them. Agent input, tool-call arguments, and activity evidence cannot mutate deadlines.
- Every continuous episode has an immutable generated Operation Review ID. Dependency IDs and original operation identities are attributes and may be reused.
- At most one unresolved review exists for an exact activation and dependency ID.
- Tests use the parent issue's confirmed black-box protocol scenario seam plus focused production integration tests for existing acceptance/cancellation paths.
- Do not add compatibility or migration behavior for removed protocol designs.

# Work Plan

1. Add one failing scenario for a durable operation review record, capped deadline, stable deadline under evidence/activity, WATCH projection, expiry reconciliation, and deduplicated incident trigger.
2. Implement generated episode identity, exact active-episode uniqueness, and review-ID-based evidence, WATCH, triggers, inspection, and mutation.
3. Add failing scenarios for concurrent dependency reuse, later reuse after resolution, resolution, restart/offline pause, exhausted reconciliation, and activation recovery transfer.
4. Wire operation review lifecycle to every Operation Dependency creation/removal path without resetting identity or deadline.
5. Make callback and commit failures reject without recording synthetic evidence.
6. Route cancellation uncertainty through an Operation Review-owned transaction primitive.
7. Extract deterministic acceptance and cancellation reconciliation into its focused module while bootstrap retains scheduling.
8. Document the observable contract and run focused, full, and lint validation.

# Validation

- Concurrent reuse red: the second activation had no projected review because dependency ID was the review primary key. Green: the public scenario passes with two review IDs and exact independent resolution.
- Later reuse red exposed that direct satisfaction does not reactivate a settled activation. Green: resolving through public reconciliation permits a fresh same-activation episode with new state and unchanged history.
- Callback failure red: reconciliation resolved after converting the sentinel into evidence. Green: the exact sentinel rejects and durable state is unchanged.
- Malformed outcome red: commit failure was converted into synthetic evidence. Green: the promise rejects and the transaction rolls back unchanged.
- Cancellation revalidation red: the review had zero evidence entries. Green: one exact cancellation-uncertainty entry is visible through public inspection.
- Scheduled reconciliation retry red: one transient failure emitted a warning and then permanently stopped review. Green: one generation-fenced discovery retry progresses the live Owner without duplicate WATCH or triggers.
- Recovered cancellation red: finalization searched for the transferred review under the original activation and rejected `UnknownLifecycleDependency`. Green: the immutable review ID crosses the reconciliation boundary and receives one canonical uncertainty entry.
- Missing extension adapter red: Owner startup silently used a generic eligible fallback for ownership, external-side-effect, and generic reviews. Green: startup rejects unchanged for all three kinds.
- Extension adapter contract: all three extension kinds receive the exact original identity and current fence; an explicit resolved outcome removes only the exact dependency.
- Stale adapter race: an outcome computed under the original fence cannot mutate or resolve the review after recovery transfer.
- Late extension configuration failure: a due review appearing after startup reports visibly and remains unchanged without fallback evidence or a trigger.
- Focused Operation Review and activation cancellation suites: 33 tests passed.
- Focused Operation Review and Workflow bootstrap suites: 54 tests passed.
- Focused Operation Review, activation cancellation, Direct Signal, and completion suites: 81 tests passed.
- Final focused Operation Review suite: 23 tests passed.
- Final focused Operation Review, bootstrap, activation cancellation, Direct Signal, and completion suites: 136 tests passed.
- `npm test`: passed.
- `npm run lint`: passed with no diagnostics.
- `git diff --check`: passed.

# Progress

- [x] Read issue #29, parent #14, successor #30/#31 boundaries, `CONTEXT.md`, current lifecycle/messaging/cancellation/recovery code, and repository ExecPlan/TDD instructions.
- [x] Confirm the existing black-box protocol scenario harness as the primary test seam.
- [x] Complete the first red/green Operation Review slice.
- [x] Give concurrent and later reused dependency IDs independent review episodes.
- [x] Preserve one continuous review across recovery ownership transfer.
- [x] Make reconciliation callback and commit failures fail transactionally.
- [x] Record cancellation uncertainty through the Operation Review transaction seam.
- [x] Extract deterministic reconciliation from bootstrap scheduling.
- [x] Re-arm scheduled reconciliation after visible transient failure.
- [x] Preserve cancellation evidence recording after review transfer.
- [x] Require one explicit extension adapter for ownership, external-side-effect, and generic reviews.
- [x] Fail restored extension review startup before timers when the adapter is absent.
- [x] Preserve stale-fence compare-and-swap behavior for extension outcomes.
- [x] Integrate all uncertainty producers and Owner runtime review.
- [x] Update documentation and complete validation.

# Surprises & Discoveries

- Existing message binding and cancellation claims already create their exact Operation Dependencies transactionally. SQLite dependency triggers provide the smallest generic seam for guaranteeing review creation and exact resolution across every producer.
- Recovery explicitly transfers active reviews to the replacement activation before moving dependency rows, so dependency triggers neither close nor recreate the episode.
- Cancellation deadline reconciliation can safely inspect an exact process locator and finalize confirmed absence. It must not repeat the external close operation.
- Acceptance and cancellation have authoritative built-in adapters. Other operation kinds remain opaque to the protocol and require the embedding's explicit extension adapter.

# Outcomes & Retrospective

- Every continuous Operation Dependency episode now receives one durable generated review ID with an operation-specific interval capped by Workflow policy.
- Concurrent activations can use the same dependency ID independently, and later reuse creates fresh history.
- Evidence and runtime activity do not alter deadlines.
- Infrastructure and missing-adapter failures remain outside operation evidence and outcomes.
- WATCH, resolution, expiry, exhaustion, incident-trigger deduplication, recovery transfer, reconciliation rollback, cancellation evidence, Owner offline pause/resume, direct offline routing, and both completion paths have observable tests.
- The implementation produces the durable incident trigger consumed by the incident subsystem without implementing incident scope, briefs, or Moderators.
- Final validation passed with `npm test`, `npm run lint`, and `git diff --check`.

# Decisions

- A durable incident trigger is the terminal output; full Operational Incident creation remains in the incident subsystem.
- Operation Review must attach automatically to Operation Dependencies so no producer can forget the deadline.
- Operation Review ID is immutable episode identity; dependency ID and original identity are reusable attributes.
- Reconciliation dispatch is exhaustive: acceptance and cancellation use built-ins, while ownership, external-side-effect, and generic use the required extension adapter.
- Evidence is append-only review input. It can resolve an operation only through an explicit reconciliation outcome; recording activity never changes the deadline or invents a result.
