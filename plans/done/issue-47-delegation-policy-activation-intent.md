# Issue #47 — Delegation Policy and explicit Activation Intent

## Goal

Replace the static `spawning` boolean with each ordinary Agent's effective Delegation Policy and make activation-creating `agent_send` calls explicit through one canonical Activation Intent.

## Intention

Keep policy selection at child creation, activation intent at the sender-owned `agent_send` call, and lifecycle/persistence enforcement in the durable Workflow protocol. Workflow Owner delegation remains policy-exempt. Moderators remain unable to create children.

## Scope & Constraints

- Effective policies: `disabled`, `approval-required`, `autonomous` only.
- Resolution: SpawnSpec override → Agent Definition default → `approval-required`.
- Persist policy on ordinary child Agent records; Owner and Moderator have no policy.
- Remove all static `spawning` parsing, persistence, capability checks, deny expansion, docs, and tests without compatibility behavior.
- Strict `agent_send` union requires `activation: { intent }` for spawn and permits it only on Agent-targeted Requests that create ended-child activations.
- Ordinary open-activation Requests omit activation; Signals, Answers, and Answer-plus-Request reject it.
- Activation intent is canonical in the tool call and projected as activated-Agent context and labels without a second message/dependency.
- Preserve atomic autonomous/Owner spawn and reactivation behavior.
- Implement the approval-required preflight boundary needed by this ticket without weakening atomicity or creating target effects before approval.
- Test observable protocol and extension seams, not private helpers.

## Pre-agreed Test Seams

The issue's acceptance criteria define these public seams:

1. Agent Definition parsing and tool schema exposed through the extension test API / registered `agent_send` schema.
2. Workflow membership inspection and persisted reopen behavior through `WorkflowControlPlane` / `WorkflowBootstrap`.
3. Atomic child creation and ended-child activation through `DirectSignalRuntime` and the scenario harness.
4. Production launch integration through registered extension tools and prepared launch inputs.

## Work Plan

1. Add failing tests for strict policy parsing/resolution, persistence, and complete removal of `spawning` behavior.
2. Replace capability types/storage with optional effective delegation policy and resolve policy at spawn selection.
3. Add failing schema/canonical-source tests for Activation Intent legality and omission.
4. Thread Activation Intent through spawn/reactivation protocol data, transcript verification, Inbox projection, and pane/work labels.
5. Add policy enforcement tests for Owner exemption, autonomous execution, disabled no-effects rejection, and approval-required no-target-effects identification.
6. Implement policy preflight and the required held approval path at the existing durable activation boundary; add decision/inspection surface only to the extent required by the source decisions and acceptance behavior.
7. Remove static spawning-driven deny logic, fixtures, docs, and bundled frontmatter.
8. Run targeted tests and lint/type checks throughout; run the full suite once at the end.
9. Run `/code-review`, fix findings, move this plan to `plans/done/`, and commit with a semantic message referencing issue #47.

## Validation

- Targeted protocol and extension test files after each vertical slice.
- `npm run lint` regularly.
- Type checking via the repository's executable TypeScript tests/imports (no dedicated typecheck script exists); add `tsc --noEmit` only if configuration supports it without inventing project settings.
- Full `npm test` once after implementation and review fixes.
- `rg 'spawning'` must find no static field, compatibility code, docs, or tests.

## Progress

- [x] Read issue #47, source decisions #40/#41/#45, domain context, and existing atomic spawn/reactivation implementation.
- [x] Identified public seams from the acceptance criteria and current scenario/extension harnesses.
- [x] Policy model and storage.
- [x] Activation Intent API and canonical projection.
- [x] Policy enforcement / approval-path identification.
- [x] Legacy removal and docs.
- [x] Two-axis code review and review fixes.
- [x] Full validation.
- [x] Commit prepared on the current branch.

## Surprises & Discoveries

- The approval holding/decision/lifecycle subsystem is intentionally split into issues #48–#50. This ticket identifies `approval-required` attempts with no target effects but does not create approval records.
- Initial review found policy checks after pane/process preparation, missing Activation Intent identity in pending/provisional delivery, and Moderator policy persistence. The review-fix pass moved preflight ahead of launch and added canonical delivery/retry coverage.

## Decisions

- No compatibility migration or warning path is retained; this repository explicitly rejects legacy handling.
- Existing Workflow databases are disposable under this change: schema code represents only the new model rather than translating persisted static capability state.
- Activation Intent is required to contain non-whitespace text at the registered schema and protocol boundaries.

## Outcomes & Retrospective

- Ordinary children persist exactly one effective Delegation Policy; Owner and Moderator records do not.
- Spawn and ended-child Request activation are explicit, canonical, intent-labelled operations.
- Owner and autonomous attempts preserve atomic activation-plus-Request behavior; disabled and approval-required ordinary attempts fail before launch effects.
- Static capability parsing, persistence, and deny expansion were removed.
- Final validation passed: targeted protocol/extension tests, `npm run lint`, `git diff --check`, and the full `npm test` suite.
