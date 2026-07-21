# Atomic spawned initial request

## Goal
Finish issue #22 on top of `ae4ae4e`: atomically create spawned children with their initial Request, and make production `agent_send` reactivate authorized interrupted/ended recipients.

## Intention
Use a PREPARE → PROJECT → COMMIT → RELEASE protocol. A child first prepares its actual recipient-owned Inbox Router without SQLite registration. The Spawner projects exactly one canonical Inbox Batch and verifies JSONL evidence, then commits all durable state in one transaction, and finally releases/adopts the committed Router and provider gate. Retries reconcile by immutable source binding.

## Scope & constraints
- Preserve legacy public tools; no cutover work.
- Canonical payload remains the sender `agent_send` transcript tool call; SQLite holds metadata/digest only.
- Child Control remains derived from `spawner_agent_id`.
- Use stable `WorkflowProtocolError` codes; no broad cleanup catch as a substitute for transaction design.
- Amend `ae4ae4e` only after the full suite, isolated-home suite, lint, and diff checks pass.

## Work plan
1. Inspect existing partial Router/gate changes and protocol/store/test seams.
2. Establish red public-seam tests for atomic spawn, retry/conflict, prepared Router lifecycle, and production send reactivation.
3. Implement and verify PREPARE/PROJECT/COMMIT/RELEASE, including durable JSONL evidence, direct delivered initial message, no pointer, gate release, and reconciliation.
4. Implement authorized interrupted/ended `agent_send` reactivation through production registration paths; deduplicate canonical transcript scanning.
5. Run focused tests, `npm run test:protocol`, isolated `HOME` full suite, lint, and `git diff --check`; inspect scope and amend commit.

## Validation
- Focused protocol tests during each vertical slice.
- `npm run test:protocol`
- `tmp_home=$(mktemp -d); HOME="$tmp_home" npm test`
- `npm run lint`
- `git diff --check`
- `git diff 5ed418a...HEAD`

## Progress
- 2026-07-21: Created plan; inspecting existing partial implementation and test seams.
- 2026-07-21: Implemented prepared Router PREPARE/PROJECT/RELEASE IPC with projection and release acknowledgements; initial spawned Requests now commit delivered with no pending pointer.
- 2026-07-21: Added full immutable persisted child-binding validation for retry, canonical spawn validation in the production tool path, and rejected use of spawn transcripts as ordinary messages.
- 2026-07-21: Focused protocol suite and lint pass; running isolated full suite and final scope checks.
- 2026-07-21: Reopened after review: production Request-driven reactivation of ended recipients remains incomplete. Implementing a prepared-resume path and atomic ended-request acceptance.
- 2026-07-21: Added an internal no-prompt provisional resume path, ended-only sender seam, and atomic ownership/checkpoint/activation/router/Request transaction. Retry reuses durable source identity before launching; interrupted recipients remain on the ordinary Router path.
- 2026-07-21: Final review hardening: bounded READY/PROJECT/RELEASE deadlines, immediate disconnect rejection, explicit postcommit release-loss preservation, canonical transcript scanner reuse, and compatibility-alias removal.
- 2026-07-21: Runtime-usability follow-up: RELEASE now triggers one empty internal marker turn after the single committed Inbox Batch, ended lifecycle overrides stale Router rows, and spawn receipts always report delivered.
- 2026-07-21: Postcommit reconciliation: ended Request reactivation retains its pane/session and installs supervision after RELEASE acknowledgement loss; children reconcile exact prepared Router ownership after IPC loss, while conflicts remain fenced for investigation.
- 2026-07-21: Final identity/disconnect hardening: spawned Request IDs are UUID Message identities rather than sender-local tool-call IDs; provisional child waits fail immediately on peer close in READY/PROJECT/RELEASE; bootstrap recovery shares immutable context across the async failure boundary.
- 2026-07-21: Canonical spawn hardening: PROJECT carries spawn metadata and resolves its payload against the sender's `target.spawn` transcript entry; retries reconcile only a fully matching durable Spawned Initial Request and never treat an ordinary Request as a child creation.
- 2026-07-21: Retry lifecycle correction: spawn retry reconciliation anchors to the persisted first activation rather than the recipient's later current activation or ownership.

## Surprises & discoveries
- Existing working tree has partial prepared-Router changes in spawn, bootstrap, direct signal, index, and provisional-spawn tests; must be reworked against full transaction semantics.

## Decisions
- The provisional child appends the one Inbox Batch with `triggerTurn: false`, waits for JSONL evidence, then adopts the transaction-owned Router without rewriting its Router row after RELEASE.
- Ended reactivation uses the same provisional Router protocol but leaves Request projection to ordinary post-RELEASE Router delivery, because the message/pointer is created in the one resumed-activation transaction.

## Outcomes & retrospective
- Spawned initial Requests and ended-recipient Request reactivation both now use prepared real Routers and release/adoption handshakes. Ended reactivation atomically creates the next fenced activation and queued Request; focused protocol tests, isolated-home full tests, lint, and whitespace checks passed.
