# Goal

Implement issue #28: automatically resume a failed Subagent once when it has durable recovery-pending work, while preserving all obligations and respecting the persisted launch policy.

# Scope & Constraints

- Recovery is durable, fenced, and owned by the Workflow Owner runtime.
- Only one automatic replacement activation may be created for each failed activation's recovery episode.
- Existing Request, Human Interrupt, accepted-inbox, undeclared-settlement, and operation-dependency state remains authoritative; recovery does not fabricate protocol effects.
- Automatic recovery uses the persisted launch policy. Missing policy leaves the episode unresolved and visible rather than widening privileges.
- #26 cancellation remains terminal and owns its existing obligation transformations.
- Manual resume is not redesigned; it must not duplicate an already claimed automatic recovery launch.

# Work Plan

1. Add recovery-episode persistence and lifecycle hooks for failure, replacement claim/activation, declared settlement, replacement completion, and exhaustion.
2. Surface recovery state through the control plane and inspection.
3. Reuse fenced resume launch infrastructure for an Owner-only durable recovery coordinator, including persisted launch-policy application.
4. Add observable protocol/bootstrap/watcher tests, then update concise documentation.
5. Run focused tests, full tests, lint, and diff validation.

# Progress

- [x] Persist and fence one recovery episode and replacement activation.
- [x] Notify a live Owner across nested spawners, while retaining durable pending state when the Owner is absent.
- [x] Resume delivered incoming Request work and project bound Human results exactly once without waking pending Human Interrupts.
- [x] Derive recovered Human tool results from stable hidden markers in the read-only provider context; never mutate Pi transcripts through private APIs.
- [x] Keep replacement dispatch in `launching` until child bootstrap, then reconcile exact run locators across Owner restart.
- [x] Preserve Request provenance, accepted input, Human state, undeclared correction state, and operation dependencies.
- [x] Launch with the persisted tool policy and named Agent definition.
- [x] Resolve on declared settlement, completion, or cancellation; exhaust after replacement failure.
- [x] Fence recovered Human projection until context confirmation and reject parallel Human tool batches before durable state.
- [x] Release deferred-only recovery mechanically so the actual Inbox Batch owns the single useful model turn.
- [x] Arbitrate durable recovery ownership again at the final legacy relay boundary.
- [x] Wake once when replacement startup confirms a canonical Human tool result persisted before failure.
- [x] Wake once from a pre-crash Inbox Batch marker without reprojecting its Signal or Request.
- [x] Cover public inspection, launch fencing, manual resume, stale start, and live watcher behavior.
- [x] Fence the durable provisional pane intent, pane-prepared, dispatch-intent, and dispatch-evidence crash windows with exact locator/epoch reconciliation.
- [x] Pass focused tests, full tests, lint, and diff validation.

# Validation

- `node --test test/protocol/activation-recovery.test.ts test/protocol/automatic-recovery-continuation.test.ts test/protocol/workflow-bootstrap.test.ts test/protocol/activation-cancellation.test.ts test/protocol/human-interrupt-extension.test.ts test/protocol/human-interrupt-recovery-continuation.test.ts`
- `npm test`
- `npm run lint`
- `git diff --check`

# Decisions

- A durable declared settlement resolves the recovery episode; undeclared settlement retains its existing correction flow.
- A replacement failure exhausts automatic recovery but does not abandon or transform obligations.
- A missing persisted policy blocks automatic launch and is reported through inspection.
- A recovered Human answer remains canonical in its durable input entry. `sendMessage()` carries a stable hidden projection marker and is not an acknowledgement; its identity stays fenced until the context hook confirms the marker and derives the sole synthetic tool result on every rebuild.
- Canonical Human tool-result and Inbox Batch evidence share one durable recovery-continuation fence. Claim precedes the hidden scheduler marker, a fresh process may re-arm an unconfirmed projection, and only provider-context observation consumes it.
- `agent_ask_user` must be the assistant message's sole tool call because recovery cannot reconstruct absent parallel sibling results.
- Deferred-only recovery moves through a fenced mechanical projection release; it creates no empty provider turn and consumes no undeclared-settlement allowance.
- A failed run's durable recovery episode permanently suppresses its legacy watcher relay, including after an Owner claim or replacement resolution.
- Owner dispatch keeps a replacement `launching`. Before Herdr creation, the Owner durably records a unique workspace/label/cwd provisional intent. Herdr tab creation uses that exact label; a returned pane id is only an acknowledgement, so a crash or ambiguous create remains discoverable through `pane list` without guessing generated ids. A present intent is adopted and atomically promoted into recovery claim, ownership, and the `prepared` checkpoint; a missing intent is safely retired for retry. Claim loss, ambiguous identity, failed close, and unavailable discovery retain the intent/fence and never close another run's pane. Child bootstrap is the activation acknowledgement. Restart reconciliation retains live exact locators, closes and confirms absence for stale provisional or pre-bootstrap `prepared`/`dispatching` panes before returning the exact claim to the same pending episode, exhausts confirmed-dead activated replacements, and preserves unknown liveness fences.
- Recovery checkpoints record `prepared` before external submission, `dispatching` before the command call, and `dispatched` only after submission returns. A pre-bootstrap dispatching pane may have accepted a command, so present panes are closed and absence is confirmed before requeue; an already missing exact pane is safely CAS-requeued. Unavailable inspection retains the fence and receives bounded Owner-live retries; manual resume remains fenced until exact termination/requeue.
