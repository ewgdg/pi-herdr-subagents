# Goal
Implement the bounded, read-only core of issue #23: capability-filtered inspection for persisted Agent and Request state, direct-child and owner Workflow enumeration, and the `agent_inspect` Pi tool.

# Intention
Provide one centralized inspection projection that authorizes before reading protected state, redacts canonical payload-bearing fields, and performs no protocol/runtime mutation.

# Scope & Constraints
- Supported targets only: known Agent, known Request, caller direct children, owner Workflow.
- Request states are the currently durable `open|answered|resolved` states only.
- Human Interrupt and undeclared-settlement projections use existing persisted producer state and omit model-facing IDs/payloads.
- Do not implement cancellation/orphan, Operation Review, Incident Visibility producers, or placeholder persistence owned by #25/#26/#29/#30.
- Strict TypeBox unions reject mixed and extra target forms.
- Test-first; no commit.

# Work Plan
1. Add failing protocol tests for projections, authorization, cross-workflow rejection, redaction, elapsed clock, and mutation purity.
2. Add failing extension tests for strict schema, registration, and deterministic JSON output/details.
3. Implement a focused workflow-inspection module and expose it through control plane/bootstrap.
4. Register/export `agent_inspect` alongside `agent_send`.
5. Document bounded behavior in README.
6. Run focused tests, full tests, lint, and diff check; move this plan to done.

# Validation
- `node --test test/protocol/workflow-inspection.test.ts test/protocol/agent-inspect-extension.test.ts`
- `npm test`
- `npm run lint`
- `git diff --check`

# Progress
- 2026-07-22: Read project instructions, issue #23, current protocol stores/control plane/bootstrap, and extension patterns. Plan created.

# Decisions
- Authorization and redaction live in `protocol/workflow-inspection.ts`; callers receive projections, never raw lifecycle/message records.
- Knowing an in-workflow Agent ID permits that Agent projection only. Enumeration remains caller-relative.

# Outcomes & Retrospective
Pending.

# Progress
- 2026-07-22: Added protocol and Pi-extension tests first, then implemented centralized inspection projections and tool registration.
- 2026-07-22: Added bounded README documentation and updated the public tool registration test.
- 2026-07-22: Focused tests, all 319 tests, lint, and diff checks pass.

# Surprises & Discoveries
- Existing Request persistence already contains sufficient correlation and message delivery pointers for bounded inspection; no new schema or placeholder state was needed.
- Human Interrupt and undeclared-settlement records contain canonical payload-bearing fields, so projection must remain outside the raw store API.

# Outcomes & Retrospective
Completed the bounded core. `agent_inspect` supports strict Agent, Request, direct-child, and Workflow targets; authorization/redaction is centralized; inspection is read-only against currently durable states. Issue #23 must remain open for cancellation/orphan, Operation Review, and Incident Visibility producer work tracked by #25/#26/#29/#30.

# Review Follow-up — 2026-07-22

## Progress
- Removed raw failed-activation error and exit details from addressability-level projections.
- Replaced per-inspection mutable `DirectSignalStore` construction with one control-plane-owned `DirectSignalInspectionStore` opened with SQLite `readOnly: true`; it performs no schema initialization, write PRAGMAs, or transactions.
- Changed elapsed time to current activation duration: owner and inactive projections report `0`; open activations measure `now - createdAtMs`; ended activations freeze at `updatedAtMs - createdAtMs`.
- Split Request obligation (`unresolved|satisfied`, derived from Request status) from requester lifecycle relationship (`waiting|not-waiting`).
- Preserved explicit child `--tools` allowlists exactly: `agent_inspect` is available without an allowlist or when explicitly requested, and remains removable through `deny-tools`.
- Expanded tests for replacement/ended activations, all persisted Request states, WAL writer coexistence, schema/message/Request purity, cross-Workflow Requests, and operational lookup error propagation.

## Decisions
- Inspection owns a dedicated long-lived read-only SQLite connection rather than borrowing the mutation store. Request correlation and both delivery statuses are read by one joined query, keeping each projection internally consistent without DDL or writer locks.
- Failed activation projections expose only `{ kind: "ended", outcome: "failed" }`; diagnostic error text remains outside addressability-level inspection.
- Request status is the authoritative obligation state. Lifecycle dependency presence is supplementary and cannot mark an answered-but-undelivered Request satisfied.
