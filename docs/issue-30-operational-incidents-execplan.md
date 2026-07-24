# Goal

Implement issue #30: detect durable Dependency Deadlocks and the other specified operational trigger episodes, persist one independent Operational Incident for each continuous episode, compute its monotonic Incident Scope, and create a compact Incident Brief before any future Moderator launch.

# Scope & Constraints

- Detection reads only durable Workflow state. Process activity, elapsed activation duration, Request-chain depth, Signals, Addressability, and transcript references are not incident triggers.
- A Dependency Deadlock requires a closed strongly connected Request-dependency component whose Agents are all durably waiting solely on unresolved Requests inside that component and have no eligible accepted input.
- Human Interrupts, operation dependencies, external Request dependencies, active Agents, and recoverable failed work are progress sources rather than deadlock evidence.
- Repeated Undeclared Settlement, exhausted Automatic Recovery, persistent operational uncertainty, and expired unresolved Operation Review use their existing durable episode identities.
- Distinct trigger episodes remain distinct even when Incident Scopes overlap.
- Incident Scope is monotonic. It starts from the trigger seed and closes transitively over seeded descendants and unresolved Request neighbors in both directions; later reconciliation may only add newly implicated members.
- This issue persists incidents and briefs only. Moderator creation and incident resolution belong to #31 and #32.

# Public Test Seams

- `WorkflowControlPlane.reconcileOperationalIncidents()` performs Owner-only durable detection/reconciliation.
- `WorkflowControlPlane.listOperationalIncidents()` and `inspectOperationalIncident()` expose persisted incident identity, trigger, status, and current monotonic scope.
- `WorkflowControlPlane.inspectIncidentBrief()` exposes the immutable creation-time Incident Brief.
- Protocol scenario tests exercise these control-plane seams while constructing lifecycle and Request state through existing public scenario APIs.

# Work Plan

1. Add an Operational Incident module that owns schema, trigger detection, continuous deadlock episodes, incident persistence, brief construction, and monotonic scope expansion.
2. Detect closed Request components from current durable activation/dependency state and exclude every specified progress source.
3. Convert existing repeated-undeclared, exhausted-recovery, and Operation Review trigger records into deduplicated incidents.
4. Build initial Incident Briefs atomically with incident creation, including trigger evidence, roster/dependency snapshot, prior recovery/reconciliation, policy, durable pointers, authority boundaries, allowed outcomes, and diagnostics.
5. Integrate Owner-only reconciliation and inspection through the Workflow Control Plane and bootstrap facade; run incident reconciliation from the Owner's scheduled durable workflow pulse.
6. Add behavior-first protocol tests at the seams above, then run focused tests, lint, the full suite, and diff validation.
7. Review the completed diff against repository standards and issue #30, fix findings, and commit.

# Progress

- [x] Persist Operational Incidents, continuous deadlock episodes, scope membership, and Incident Briefs.
- [x] Detect only closed, solely Request-waiting deadlock components.
- [x] Exclude active, eligible-input, Human, operation, external-dependency, and recovery progress.
- [x] Materialize repeated Undeclared Settlement, exhausted recovery, persistent uncertainty, and expired review triggers once per episode.
- [x] Expand Incident Scope monotonically through descendants and unresolved Request neighbors.
- [x] Keep overlapping incidents independent.
- [x] Integrate control-plane/bootstrap reconciliation and inspection.
- [x] Pass focused tests, full tests, lint, review, and diff validation.

# Validation

- `node --test --test-reporter=dot test/protocol/operational-incidents.test.ts`
- `node --test --test-reporter=dot test/protocol/operation-review.test.ts test/protocol/activation-lifecycle.test.ts test/protocol/activation-recovery.test.ts test/protocol/workflow-bootstrap.test.ts`
- `npm run lint`
- `npm test`
- `git diff --check`

# Decisions

- Incident creation is a durable Owner reconciliation step on its own scheduled failure domain, independent of Operation Review reconciliation. Future Moderator creation consumes persisted incidents rather than raw trigger rows.
- Deadlock episode identity uses the sorted Agent set plus a durable activation-revision and Request witness. The witness distinguishes a later recurrence even when polling does not observe the active progress gap.
- Incident Briefs are immutable creation-time snapshots. Current Incident Scope is stored separately and expands monotonically.
- Only trigger seeds root descendant expansion; Agents reached through unresolved Requests do not implicitly contribute unrelated descendant subtrees.
- Operation Review trigger reason distinguishes persistent uncertainty (`reconciliation-exhausted`) from deadline expiry (`review-deadline-expired`).
- A policy-blocked recovery is not treated as exhaustion; issue #30 consumes only the protocol's existing durable `exhausted` state.
