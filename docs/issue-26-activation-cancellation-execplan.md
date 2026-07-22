# Goal

Implement issue #26 end to end: authorized cancellation of one open Subagent activation, confirmed process termination, atomic obligation transformation, and durable reconciliation when termination cannot be confirmed.

# Scope and Constraints

- `agent_cancel` accepts exactly one `target`, either `{ agent }` or `{ request }`; no aliases.
- Activation authority is limited to the Workflow Owner and direct Spawner. The persistence/API model carries incident-control attribution for issue #31, but no role-name authorization is added here.
- Cancellation applies to one activation only. Descendant pruning/cascade belongs to issue #27.
- The activation and Agent Run ownership remain open while cancellation is `terminating`, `ready-to-commit`, or `in-doubt`.
- Process termination is confirmed only against the exact fenced run locator using inspect-close-inspect. An unavailable inspection is not confirmation.
- One `BEGIN IMMEDIATE` finalizer revalidates the activation/run/epoch/revision and atomically transforms activation-scoped obligations, cleans up Human/undeclared state, ends the activation as cancelled, removes routing, releases ownership, and commits the cancellation operation.
- Incoming open Requests become orphaned and own one canonical orphan Protocol Notice. Outgoing open Requests are cancelled. Answered outgoing Requests and their undelivered Answers are preserved.
- Completion and cancellation serialize through the Workflow database. A `cancellation:*` operation dependency blocks completion after the cancellation claim.
- Existing Workflow databases are upgraded in place with Request activation provenance and the orphaned Request outcome.

# Work Plan

1. Add failing protocol and extension tests for strict tool targets, authority, process confirmation, durable in-doubt retry, completion arbitration, obligation transformation, Human/undeclared cleanup, orphan notice delivery, inspection, and watcher suppression.
2. Add Request activation provenance and orphan metadata with an in-place schema migration and update Request creation/projection/delivery paths.
3. Implement the durable activation-cancellation operation store, process terminator, and atomic finalizer.
4. Integrate cancellation through Workflow bootstrap/control-plane and `agent_cancel`; add exact run-locator persistence for provisional spawns.
5. Suppress legacy watcher result delivery whenever cancellation owns or has committed the exact run.
6. Update inspection and README documentation.
7. Run focused tests, the full suite, lint, and `git diff --check`.

# Validation

- `node --test test/protocol/activation-cancellation.test.ts test/protocol/agent-cancel-extension.test.ts test/protocol/completion-gate.test.ts test/protocol/request-cancellation.test.ts test/protocol/workflow-inspection.test.ts`
- `npm test`
- `npm run lint`
- `git diff --check`

# Progress

- 2026-07-22: Read issue #26, the domain context, issue #25 implementation notes, current lifecycle/messaging/router/watcher code, and existing tests. Recorded the implementation boundary and atomic finalization design.
- 2026-07-22: Added red-first protocol coverage for authority, inspect-close-inspect confirmation, durable in-doubt retry, completion arbitration, exact finalizer revalidation, no descendant cascade, obligation transformation, Human/undeclared cleanup, orphan notice projection, recovery provenance, migration, inspection, and watcher shutdown races.
- 2026-07-22: Implemented durable activation cancellation operations and the atomic finalizer. Added strict `agent_cancel({ target: { agent | request } })`, exact checkpointed termination, owner/direct-Spawner authorization, dormant incident-control attribution, and watcher/self-shutdown suppression.
- 2026-07-22: Added Request activation provenance, recovery transfer, the orphaned outcome and canonical notice metadata, sender-free orphan Protocol Notice delivery, and in-place compatibility migration for existing Request tables.
- 2026-07-22: Updated README protocol/tool documentation. Focused cancellation, bootstrap, messaging, completion, inspection, and watcher tests pass.
- 2026-07-22: Final validation passes: the complete `npm test` suite, `npm run lint`, and `git diff --check` report no failures.
- 2026-07-22: Review follow-up added durable aliases for later public retry tool-call IDs, recovery-chain-aware migration provenance, bound outbound acceptance arbitration, and one shared fail-fast Request cancellation transition used by both public Request cancellation and activation finalization.
- 2026-07-22: Review follow-up validation passes: all 402 tests in the complete suite, focused cancellation/Request/bootstrap/extension/race suites, `npm run lint`, and `git diff --check`.

# Decisions

- Keep the operation source bound to one target activation even after it commits, so a replayed tool call cannot cancel a later activation of the same Agent.
- Preserve the durable Router registration while target shutdown and the legacy watcher observe a cancellation-owned run. Only the atomic finalizer removes it.
- Transfer unresolved Request provenance only across failed-work recovery. A new activation after completion or cancellation receives preserved pending inputs without inheriting ownership of the ended activation's obligations.
- Use additive notice columns on Message and pointer rows for orphan notices, avoiding a destructive rebuild of the already deployed Message tables. The constrained Request table is rebuilt transactionally because its status check must admit `orphaned`.
- Bind every public retry source to the original nonterminal cancellation operation. Only its original actor may add an alias; replay of either source stays fenced to the original activation.
- Treat an `acceptance:*` dependency and its bound Message as one activation-owned prepare record. Cancellation discards both only when acceptance has not committed; accepted Signals and Answers win, while any accepted open Request uses the shared Request cancellation transition.
