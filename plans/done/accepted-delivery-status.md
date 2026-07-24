# Accepted Delivery Status

## Goal

Keep `steer | deferred` as the immutable delivery-timing policy while renaming the durable post-acceptance message state from `queued` to `accepted`. Rename stale internal symbols so message kind, timing, transport state, and current inbox eligibility remain visibly separate.

## Intention

`Deferred Delivery` correctly describes preserving active work until settlement. `accepted` more accurately describes the atomic recipient-owned guarantee and remains true during projection ambiguity, while `queued` overstates the current material state. The resulting protocol reads:

```text
timing: steer | deferred
state:  bound -> accepted -> delivered | suppressed
```

A Request becomes `answered` when its Answer is accepted and `resolved` when that Answer is delivered.

## Scope & Constraints

- Replace protocol delivery-status literal `queued` with `accepted` for actionable messages and Protocol Notices.
- Rename `QueuedSignalReceipt` to `DurableAcceptanceReceipt` and generic Signal-oriented receipt references accordingly.
- Rename generic `releaseDeferred*` scheduler methods to `reevaluateInboxEligibility*` where they process all pending actionable inputs; retain recovery-specific names that truly select Deferred timing.
- Keep `SignalDeliveryTiming = "steer" | "deferred"` and Deferred Delivery behavior unchanged.
- Keep `PendingMessagePointer`; it accurately denotes the pointer-only durable inbox representation.
- Keep Request states `open | answered | resolved | cancelled | orphaned`.
- Do not rename unrelated natural-language queue operations, Pi `deliverAs`, completion-delivery state, or `defer*Unsubscribe` cleanup helpers.
- No compatibility path: update current schema and fixtures directly. Do not accept, migrate, or mention the removed delivery-status literal in runtime code.
- Preserve the existing uncommitted nested-delegation glossary changes in `CONTEXT.md`.

## Work Plan

1. **Lock behavior in tests**
   - Update direct messaging, tool schema, spawned/reactivated Request, cancellation/orphan notice, Human Interrupt, recovery, inspection, and scenario-harness expectations from accepted-message `queued` state to `accepted`.
   - Add assertions that Deferred timing remains unchanged and independent from accepted/delivered state.
   - Add naming coverage for the generic inbox eligibility reevaluation path.

2. **Deepen the shared message contract**
   - In `pi-extension/subagents/protocol/direct-signal-types.ts`, rename the receipt and replace actionable-message and notice delivery-state unions.
   - Update exports/imports through `direct-signal.ts`, `workflow-bootstrap.ts`, `direct-signal-extension.ts`, and `index.ts`.
   - Prefer `Message*` or domain names over legacy `Signal*` names only where the abstraction already covers Signals, Requests, Answers, and Protocol Notices; avoid unrelated opportunistic renames.

3. **Update persistence and transitions**
   - Change SQLite constraints, writes, reads, mapping helpers, transition predicates, and diagnostics in `sqlite-message-store.ts` and `sqlite-workflow-store.ts`.
   - Preserve transition semantics: `bound -> accepted -> delivered`, with `suppressed` as the alternative accepted-undelivered terminal path.
   - Update cancellation/orphan/undeclared-notice status fields and receipt values consistently (`notice-accepted`, `notice-delivered` where exposed).
   - Keep acceptance sequence, accepted timestamps, pending pointers, projection claims, and delivery confirmation behavior unchanged.

4. **Rename generic scheduling APIs**
   - Replace `releaseDeferred()` / `releaseDeferredSignals()` at router, runtime, bootstrap, settlement, and Human Interrupt boundaries with `reevaluateInboxEligibility()` naming.
   - Keep `releaseDeferredRecoveryProjection()` and equivalent recovery-specific functions when their SQL/logic explicitly selects Deferred timing.
   - Rename local collections such as `queued` to `pending` where they contain both Steer and Deferred pointers.

5. **Update public copy and projections**
   - Change `agent_send` descriptions and receipts from “queued receipt” to “Durable Acceptance Receipt” / `accepted` transport status.
   - Update inspection projections, Inbox Batch metadata, cancellation/orphan notice diagnostics, and error text without implying read, understanding, or side-effect completion.

6. **Synchronize durable specification**
   - Update `CONTEXT.md` to define `accepted` as durable recipient acceptance and `delivered` as recipient-session transcript commitment; explicitly keep timing, state, and eligibility orthogonal.
   - Update local public copy and this ExecPlan. GitHub tracker changes are explicitly excluded from this implementation run.

## Validation

- Targeted tests:
  - `node --test --test-reporter=dot test/protocol/direct-signal.test.ts`
  - `node --test --test-reporter=dot test/protocol/direct-signal-extension.test.ts`
  - `node --test --test-reporter=dot test/protocol/spawned-initial-request.test.ts`
  - `node --test --test-reporter=dot test/protocol/request-cancellation.test.ts`
  - `node --test --test-reporter=dot test/protocol/activation-cancellation.test.ts`
  - `node --test --test-reporter=dot test/protocol/activation-recovery.test.ts`
  - `node --test --test-reporter=dot test/protocol/workflow-inspection.test.ts`
- Full validation: `npm test`, `npm run lint`, and `npm run test:smoke` when the environment supports it.
- Search for stale protocol literals and names. Any remaining `queued` must describe an actual queue operation or non-message domain, not actionable-message delivery state.
- Verify SQLite constraints and runtime transitions use only `bound | accepted | delivered | suppressed` for message delivery state.
- Verify `deferred` remains present in timing schemas, persisted timing columns, router eligibility logic, recovery-specific Deferred selection, and tests.
- Do not modify GitHub issues or dependency relationships during this implementation run.

## Progress

- [x] Explored timing, delivery state, persistence, routing, recovery, tests, and tracker terminology.
- [x] Ran an adversarial terminology debate.
- [x] Chose the hybrid design: retain Deferred timing; adopt accepted delivery state.
- [x] Updated behavioral tests first and observed the expected failures for old status literals and generic release APIs.
- [x] Updated implementation, persistence constraints/transitions, notice state, receipts, inspection, fixtures, and public copy.
- [x] Synchronized `CONTEXT.md`, `README.md`, and this ExecPlan; GitHub tracker changes were intentionally excluded.
- [x] Completed targeted tests, full tests, lint, smoke tests, whitespace checks, and stale-terminology audits.
- [x] Applied review fixes for shared receipt typing, undeclared-notice acceptance naming, and explicit Deferred status-transition coverage.
- [x] Ran an import-aware ad hoc TypeScript check; the repository has no `tsconfig.json`, and the full source/test check reports 109 existing diagnostics but no missing-name, missing-export, missing-module, or `DurableAcceptanceReceipt` diagnostics.

## Discoveries

- The settlement and Human Interrupt release hooks process all pending actionable inputs, including Steer messages, so `reevaluateInboxEligibility` is the accurate generic name.
- Automatic Recovery's Deferred projection release explicitly selects Deferred timing and remains recovery-specific.
- The undeclared-settlement `notice_queued` flag represented durable acceptance rather than current queue placement, so it was renamed to `notice_accepted` with no compatibility column.
- The red-phase targeted run failed on the old `queued` values and missing renamed APIs before implementation changed.
- Runtime tests strip TypeScript types, so shared receipt imports need an explicit compiler check; `sqlite-message-store.ts` now imports `DurableAcceptanceReceipt`, and extension callback seams reuse that shared type.
- `queueUndeclaredNotice` performed durable notice acceptance rather than a generic queue operation, so the API is now `acceptUndeclaredNotice` from lifecycle storage through tests.
- Deferred behavioral tests now prove that accepted work can remain ineligible, remains accepted after projection begins, and changes to delivered only after `confirmDelivery`.

## Decisions

- Deferred Delivery remains canonical because it names the release condition, not storage mechanism.
- `accepted` replaces `queued` as the durable post-acceptance state because it names the atomic guarantee and remains accurate during projection ambiguity.
- Inbox eligibility remains a derived scheduling fact, not a delivery state.
- Existing durable artifacts receive no compatibility handling; the source, schema, fixtures, and normative contract cut over together.

## Outcomes & Retrospective

The protocol now exposes durable recipient ownership as `accepted`, transcript commitment as `delivered`, and inbox scheduling through `reevaluateInboxEligibility`, while retaining `steer | deferred` timing and recovery-specific Deferred projection names. SQLite schemas and transitions cut over directly with no old-literal compatibility path. Targeted tests, `npm test`, `npm run lint`, and `npm run test:smoke` pass. The repository has no TypeScript project configuration; an ad hoc import-aware full check remains non-clean because of 109 existing diagnostics, but it reports no missing receipt import/export/module errors. Remaining queue terminology is limited to this refactor record, a historical completed plan, and an unrelated in-memory coordination queue fixture.
