# Obligation-Preserving Descendant Cancellation

## Goal

Implement issue #27: cancelling a Subagent activation prunes its descendant activations while retaining the transitive unresolved-Request closure needed for obligations that originate outside the cancelled subtree.

## Scope & Constraints

- The direct target always cancels.
- Only unresolved Requests retain descendants; Signals, Addressability, and transcript references do not.
- Survivors keep their Agent identity, Spawner relationship, session, and Requests.
- Fallback to the Workflow Owner changes no lifecycle, ownership, authority, model-turn, or attention state.
- Each cancelled activation continues to use the existing atomic Request cancellation/orphaning finalizer.
- A durable plan must remain bound to the root cancellation and must not cancel a later replacement activation.

## Work Plan

1. Add a red protocol scenario covering default pruning, external incoming Request seeds, transitive outgoing Request dependencies, Signal exclusion, durable survivor identity, no fallback turn/attention, per-activation obligation cleanup, and later ordinary Request cancellation.
2. Persist a root cancellation's descendant plan and add cascade-attributed descendant cancellation operations.
3. Compute the survivor closure from open Request edges, snapshot planned activation IDs, and execute prunable descendants in parent-first order after the root commits.
4. Upgrade existing cancellation-operation tables in place to admit cascade attribution.
5. Update tool and README documentation.
6. Run focused and full tests, lint, and diff checks.

## Validation

- `node --test --test-reporter=spec test/protocol/activation-cancellation.test.ts`
- `npm test`
- `npm run lint`
- `git diff --check`

## Outcome

The persisted cascade plan preserves the required closure and fences each descendant operation to its originally planned activation. Pending cascades are resumable by a later authorized cancellation of their ended root, while ended or replaced planned activations are recorded as skipped so later siblings still progress. The focused protocol scenario and complete test suite pass.
