# Subagent Collaboration

The language for Pi sessions collaborating as a capability-scoped workflow.

## Language

**Agent Definition**:
A reusable role configuration such as `worker`, `scout`, or `reviewer`; many Agents may use the same Agent Definition.
_Avoid_: Agent type, subtype

**Agent**:
A logical workflow participant represented by exactly one Pi session and identified by that session's UUID. Resuming preserves the Agent; creating, forking, or cloning a session creates a different Agent.
_Avoid_: Process, pane, Agent Definition

**Agent Run**:
One process and terminal surface currently executing an Agent's Pi session. An Agent may have successive Agent Runs as it is resumed.
_Avoid_: Agent, session

**Workflow**:
The ownership and audit scope rooted in a main Agent. Every descendant Agent belongs to the same Workflow, while communication and visibility are granted separately.
_Avoid_: Spawn tree, communication group

**Workflow Owner**:
The main Agent whose Pi session identifies the Workflow. Ownership remains with this Agent regardless of which other Agents receive orchestration capabilities.
_Avoid_: Spawner, parent

**Moderator**:
A fresh Agent created for one operational incident so diagnostic work does not pollute the Workflow Owner's task context. It receives temporary administrative visibility and lifecycle control only over the incident's participant subtree and Request-dependency closure, never ownership or control of the Workflow Owner. It resolves operational matters independently and escalates only domain judgment; its authority ends with the incident.
_Avoid_: Workflow Owner, permanent supervisor, workflow-wide administrator

**Addressability**:
The ability to resolve an Agent's minimal identity and status and exchange messages with it. Within one Workflow, knowing the Agent's Pi session ID grants Addressability, which may be delegated by sharing that ID but grants no lifecycle control or ownership.
_Avoid_: Discovery, control authority

**Spawner**:
The Agent that directly creates another Agent. A Spawner has bounded lifecycle control over its direct children, but not over further descendants or the Workflow itself.
_Avoid_: Workflow Owner, session parent

**Child Control**:
The bounded, non-transferable authority to inspect, interrupt, cancel, resume, and message an Agent directly created by the Spawner. It cannot declare the child complete, alter its identity or transcript, or rewrite its Agent Definition or capability configuration; the Workflow Owner independently retains control.
_Avoid_: Ownership, addressability

**Cooperative Trust Model**:
The assumption that Agents follow their instructions rather than acting as adversaries. Capability and ownership rules prevent routing mistakes and accidental authority transfer; they are not a security sandbox.
_Avoid_: Permission sandbox, adversarial isolation

**Message Target**:
The routing intent of one actionable message: an addressable existing Agent, a new direct child created with the message as its initial work, or an existing Request whose requester becomes the recipient. A Request target supplies Answer authority and return routing; an Agent ID alone never supplies lifecycle authority.
_Avoid_: Caller-selected Answer destination, implicit spawn, implicit control

**Response Requirement**:
Whether an actionable message creates one correlated Answer obligation. It is independent of the Message Target, so a message targeting a Request may simultaneously Answer that Request and create a new Request.
_Avoid_: Message kind switch, conversation thread

**Signal**:
An actionable message to an Agent with no Response Requirement. Sending a Signal creates no dependency for the sender.
_Avoid_: Notification, event

**Request**:
The Answer obligation created by an actionable message with a Response Requirement. The message ID is the Request ID; acceptance records one sender dependency and selects the eventual Answer's delivery timing. A message may create a Request while simultaneously answering an earlier Request.
_Avoid_: Signal, passive status update

**Answer**:
An actionable message targeting one Request, sent only by the Agent that received it. Acceptance terminally closes that Request, while delivery resolves the requester's dependency. Its plain message content may report completion, inability, or any other terminal response; adding a Response Requirement makes the same message a new Request.
_Avoid_: Progress update, uncorrelated reply

**Spawned Initial Request**:
The first Request carried by creation of a direct child Agent. Agent creation, Spawner relationship, Child Control, dependency creation, initial-context delivery, and first activation form one operation; an empty spawn followed by a separate task message is not exposed.
_Avoid_: Empty Agent, post-spawn kickoff

**Post-Acceptance Disposition**:
The sender's declared action after durable message acceptance: `continue` permits another model turn, `settle` ends the current run and derives typed waiting, and `complete` ends the activation. Acceptance failure applies no disposition. Completion is invalid when the same message creates a Response Requirement.
_Avoid_: Wait operation, delivery status, implicit auto-exit

**Terminal Message Completion**:
Completion of a subagent activation atomically combined with durable acceptance of its final outbound Signal or Answer through Post-Acceptance Disposition `complete`. It must satisfy the common Completion Gate.
_Avoid_: Duplicate completion-result message

**Standalone Completion**:
A trust-based, no-argument `agent_complete()` lifecycle action for an Agent that has already communicated everything useful through any number of messages. The runtime enforces the common Completion Gate but does not identify, rank, or semantically validate a result message and does not prompt for confirmation.
_Avoid_: Result-message selection, semantic output validation, human confirmation

**Completion Gate**:
The mechanical safety conditions shared by fused and standalone completion. After the completion commit, the Agent must have no unresolved incoming, outgoing, or recovery-pending Request obligations; no accepted but undelivered actionable inbox messages; and no unresolved operation, message acceptance, cancellation, ownership, or side-effect uncertainty. Unneeded work must be explicitly cancelled or abandoned first. Concurrent inbound acceptance and completion are serialized, and the first durable commit determines whether completion succeeds or is blocked.
_Avoid_: Semantic result validation, silent obligation abandonment

**Activation Cancellation**:
An authorized, deliberate termination of an open Agent activation, recorded as `ended(cancelled)` rather than failure. The first durable completion or cancellation commit wins; a cancellation that cannot confirm termination remains in doubt rather than being reported as successful. Cancellation does not fabricate Answers, roll back completed work, or erase unresolved Request obligations.
_Avoid_: Failure, Request Cancellation, interruption

**Obligation-Preserving Cancellation**:
Cancellation always terminates its target and propagates through the target's descendants except for the transitive Request-dependency closure rooted at descendants with unresolved incoming Requests from outside the cancelled subtree. Those descendants and any descendants they currently depend on survive; Signals do not extend the closure. A survivor retains its own Requests and falls back to Workflow Owner supervision without transferring ownership, changing its Spawner identity, or creating a model turn or human-attention item. Every Agent that actually cancels still cancels its own outgoing Requests. After consuming a cancellation notice, a survivor may semantically determine that one of its retained dependencies is no longer needed and cancel that Request normally.
_Avoid_: Unconditional cascade, dependency-count lifetime, ownership transfer

**Steer Delivery**:
Delivery of an actionable message to an active Agent at the next LLM-turn boundary, after the current generation's tool calls finish and before the next generation begins. It does not abort generation or tool execution.
_Avoid_: Mid-turn injection, interrupt

**Deferred Delivery**:
Delivery that preserves an active Agent's current work until `agent_settled`, then starts a new Agent run with the actionable message. When the recipient is already waiting, there is no active work to preserve and delivery may start immediately.
_Avoid_: Passive notification, delayed observability

**Inbox Batch**:
The ordered set of actionable messages eligible at one delivery point and injected together for one Agent run. Messages retain their individual identities and correlations. Ordering follows inbox acceptance sequence, except that Steer Delivery may intentionally overtake earlier messages awaiting Deferred Delivery.
_Avoid_: Conversation thread, merged message

**Request Resolution**:
Removal of a Request's Agent dependency when its Answer is committed to the requester's session for model consumption. Transport acceptance marks the Request answered but does not resolve it; until delivery succeeds, the requester may remain waiting on the Agent dependency.
_Avoid_: Transport acknowledgement, answer creation

**Message Delivery Status**:
Passive transport and session facts: `queued` means durably accepted into the recipient inbox, while `delivered` means committed to the recipient session for model consumption. A Request becomes `answered` when its correlated Answer is queued and `resolved` when that Answer is delivered. There is no model-level read receipt.
_Avoid_: Agent lifecycle state, acknowledgement message

**Message Wake Rule**:
An actionable message schedules immediate work for a waiting Agent and enters an active Agent according to its delivery timing. Signals may queue for an interrupted Agent and are rejected for an ended Agent. A Request from a direct Spawner or Workflow Owner may restart interrupted work or create a new activation for an ended Agent; the same Request from a merely addressable peer is rejected.
_Avoid_: Addressability as resume authority, revival by Signal

**Independent Dependency Wakeup**:
Delivery of any Answer starts its requester independently of other outstanding Requests. Unresolved Requests remain dependencies, and the Agent may return to `waiting(agent)` after processing the delivered Answer; transport provides no `all` or `any` synchronization.
_Avoid_: Barrier, join policy

**Dependency Deadlock**:
A closed component of the Request dependency graph in which every participating Agent is durably waiting exclusively on unresolved Requests within that same component, with no active participant, eligible queued actionable message, unresolved external or operation dependency, or recovery path capable of waking it. Request cycles are permitted and remain productive whenever any such source of progress exists.
_Avoid_: Cycle rejection, timeout-only diagnosis, active collaboration

**Autonomous Request Chain**:
A causal sequence in which an Answer also creates the next Response Requirement. Chain depth is observable but not capped by the protocol because hop count does not distinguish useful iteration from waste. Explicit Workflow resource policy may impose operational limits without changing Request semantics.
_Avoid_: Mandatory hop limit, conversation thread, deadlock proxy

**Deadlock Escalation**:
One deduplicated runtime-generated operational incident for each confirmed Dependency Deadlock episode. A fresh incident-scoped Moderator handles it without waking the Workflow Owner when available; otherwise a compact decision packet wakes or reactivates the Owner. Full diagnostics remain outside Owner model context for on-demand inspection. If neither route is available, the incident remains durably pending and surfaces through human attention until delivered exactly once. Detection and bookkeeping are runtime-owned but their module placement is implementation-defined.
_Avoid_: Repeated alerts, permanent supervisor, full Owner transcript injection

**Message Identity**:
One immutable, Workflow-unique ID assigned to every actionable message. A Request's message ID is also its Request ID. An Answer has its own ID and references the Request it closes; if that Answer also has a Response Requirement, its own ID is simultaneously the new Request ID.
_Avoid_: Separate correlation ID, thread ID

**Answer Authority**:
Only the Agent addressed by a Request may answer it. The Answer's `inReplyTo` identity determines the requester and return route, so the responder supplies no separate destination; lifecycle controllers cannot answer on another Agent's behalf.
_Avoid_: Proxy answer, caller-selected reply destination

**Answer Finality**:
The first durably queued Answer closes a Request's answer slot. Retrying the same Answer ID is idempotent, while a different later Answer is rejected; subsequent corrections require a new Signal or Request.
_Avoid_: Answer revision, multiple terminal answers

**Request Cancellation**:
A terminal operation available only to the original requester while its Request remains unresolved. Durable cancellation removes the requester's dependency and cannot undo work already performed. If the Request has not been delivered, cancellation suppresses it without waking the recipient; if delivered, it queues a correlated actionable cancellation notice. The first durable Answer or cancellation wins their race. Cancelling an Agent also cancels every unresolved outgoing Request owned by each activation that actually enters `ended(cancelled)`; surviving descendants retain theirs.
_Avoid_: Answer, work rollback

**Orphaned Request**:
A terminal Request outcome produced when its recipient is deliberately cancelled while still owing the Answer. The runtime does not fabricate an Answer: it delivers a correlated actionable orphan notice to the requester, whose delivery resolves the dependency. Reactivating the former recipient cannot reopen or Answer the Request; replacement work requires a new Request.
_Avoid_: Proxy Answer, unresolved dependency, automatic retry

**Recovery-Pending Request**:
An unresolved incoming or outgoing Request preserved after an Agent activation ends in failure because the durable Agent may still be reactivated. Recovery does not itself wake requesters or terminate obligations. Abandoning recovery terminally orphans incoming Requests and cancels outgoing Requests.
_Avoid_: Orphaned Request, automatic Answer, activation-local Request

**Automatic Recovery**:
Runtime-driven creation of a replacement activation after failure when the Agent retains open obligations or accepted pending messages. One replacement activation is allowed by default per recovery episode, configurable at Workflow level. The episode ends when the replacement reaches durable settlement or completion; another failure exhausts the automatic budget and creates an operational incident for Moderator review. Exhaustion does not abandon recovery, terminate Request obligations, or prune descendants.
_Avoid_: Unbounded restart, automatic abandonment, recovery without pending work

**Recovery Abandonment**:
An explicit incident verdict that the current failed work will no longer be recovered. Only an authorized Moderator or the Workflow Owner fallback may make it; retry exhaustion, elapsed time, and observation loss cannot. Abandonment terminally orphans incoming Requests, cancels outgoing Requests, and applies obligation-preserving pruning to descendants while the failed Agent itself remains `ended(failed)` and may still receive unrelated future work through a new activation.
_Avoid_: Retry exhaustion, Agent deletion, implicit timeout

**Transcript Projection**:
The sender records its messaging tool call and immediate result, while the recipient records an Inbox Batch only when delivery commits it for model consumption. Queueing, acknowledgement, retry, and delivery-status changes remain runtime observability and are not copied into Agent or Workflow Owner model context.
_Avoid_: Audit-log duplication, status notification message

**Atomic Messaging Acceptance**:
A messaging operation succeeds only when its durable effects commit together: a Request records its dependency, an Answer closes its target Request, an Answer-and-Request also creates the new dependency, a Spawned Initial Request creates and starts its child, an authorized Request may create a new activation, and cancellation removes its dependency. Failure leaves none of those effects behind; delivery is not part of caller-facing acceptance.
_Avoid_: Partial send, delivery acknowledgement

**Recipient Unavailability**:
The pre-acceptance condition where no authorized Recipient Inbox Router can durably accept an actionable message. The send fails fast with no message, dependency, queue, retry, or lifecycle effects. An authorized Request may first create or recover an activation, but succeeds only after its Router becomes ready. Only an ambiguous acknowledgement after possible acceptance uses same-identity probe or retry.
_Avoid_: Hidden retry queue, partial Request, elapsed-time acceptance

**Pending Message Pointer**:
A temporary durable recipient-inbox record that references the canonical outbound messaging tool call in the sender transcript. It stores only routing, ordering, and delivery metadata—not message payload—and is removed after delivery commits to the recipient transcript. The live queue is rebuilt from these pointers.
_Avoid_: Duplicate message store, permanent workflow audit log

**Recipient Inbox Router**:
The extension-owned routing module in an Agent Run that validates inbound messages, owns that Agent's Pending Message Pointers and acceptance ordering, rebuilds its in-memory queue, selects delivery points, commits Inbox Batches to its session, and deduplicates delivery. The sender and Workflow Owner do not write the recipient transcript or relay its messages.
_Avoid_: Central workflow broker, sender-owned inbox
