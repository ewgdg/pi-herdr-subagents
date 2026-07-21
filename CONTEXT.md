# Subagent Collaboration

The language for Pi sessions collaborating as a capability-scoped workflow.

## Language

**Agent Definition**:
A reusable role configuration such as `worker`, `scout`, or `reviewer`; many Agents may use the same Agent Definition.
_Avoid_: Agent type, subtype

**Agent**:
A logical workflow participant represented by exactly one Pi session and identified by that session's UUID. Resuming preserves the Agent; creating, forking, or cloning a session creates a different Agent.
_Avoid_: Process, pane, Agent Definition

**Subagent**:
Any non-Owner Agent governed by the protocol's activation lifecycle, including ordinary delegated Agents and incident Moderators. The Workflow Owner participates in collaboration but remains a normal user-driven Pi session rather than a Subagent.
_Avoid_: Workflow Owner, Agent Definition

**Agent Run**:
One process and terminal surface currently executing a Subagent's Pi session. A Subagent may have successive Agent Runs as it is resumed.
_Avoid_: Agent, session

**Workflow**:
The ownership and audit scope rooted in a main Agent. Every descendant Agent belongs to the same Workflow, while communication and visibility are granted separately.
_Avoid_: Spawn tree, communication group

**Workflow Owner**:
The normal user-driven Pi session whose identity roots the Workflow. It hosts the Workflow Control Plane while running but has no Subagent activation lifecycle, completion action, typed waiting state, or automatic recovery. Ownership remains with it regardless of which other Agents receive orchestration capabilities.
_Avoid_: Spawner, parent

**Workflow Control Plane**:
The extension-owned coordination module hosted by the live Workflow Owner process. It owns Workflow membership, authority, ownership fences, Request and incident state, pending-input metadata, recovery, deadlock detection, and workflow-wide attention without relaying actionable payloads or writing recipient transcripts. Durable state survives Owner shutdown, while workflow-wide automation pauses until the user resumes the Owner session.
_Avoid_: Message broker, Workflow Owner model context

**Moderator**:
A fresh runtime-created administrative Agent for one Operational Incident, with no Spawner or Child Control relationship. It receives a fixed Moderator Agent Definition plus Incident Visibility and Incident Control, cannot spawn children, and uses one durable session identity across its single permitted recovery activation. The Workflow Owner retains workflow-wide control without receiving automatic model context. The Moderator resolves operational matters independently, hands domain judgment to the Owner, and is never reused across incidents.
_Avoid_: Workflow Owner, permanent supervisor, workflow-wide administrator

**Addressability**:
The ability to resolve an Agent's identity and status, exchange messages with it, and obtain the read-only path to its complete durable transcript. Within one Workflow, knowing an Agent's Pi session ID grants Addressability, which may be delegated by sharing that ID but grants no enumeration, lifecycle control, transcript mutation, or ownership.
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

**Protocol Notice**:
A runtime-authored actionable input whose canonical payload lives in a Request, Operational Incident, or Undeclared Settlement episode record rather than a sender transcript. It has a Workflow-unique identity and uses ordinary recipient ordering, Inbox Batch, deduplication, wake, and transcript-commit rules. Cancellation notices, orphan notices, Undeclared Settlement Notices, and Incident Escalations are Protocol Notices; passive lifecycle and delivery facts are not.
_Avoid_: Signal, passive runtime event, fabricated Answer

**Spawned Initial Request**:
The first Request carried by creation of a direct child Agent. Agent creation, Spawner relationship, Child Control, dependency creation, initial-context delivery, and first activation form one operation; an empty spawn followed by a separate task message is not exposed.
_Avoid_: Empty Agent, post-spawn kickoff

**Post-Acceptance Disposition**:
The sender's declared action after durable message acceptance: `continue` permits another model turn, `settle` suppresses the automatic follow-up model turn and derives typed waiting without ending the Agent Run, and `complete` ends a Subagent activation. For the Workflow Owner, `settle` ends only the current Pi turn and `complete` is invalid. Acceptance failure applies no disposition. Completion is invalid when the same message creates a Response Requirement.
_Avoid_: Generic wait operation, delivery status, implicit auto-exit

**Human Interrupt**:
The durable pause created when an ordinary Subagent calls `agent_ask_user({ question })`. The required plain-text question is canonical in that tool call and is not parsed or semantically validated. The call atomically checkpoints the session, records one pending interrupt identified internally by the tool-call ID, enters `waiting(human)`, and settles. The human answers normally in that Subagent's pane; submission durably binds the input and clears `DECIDE`, but the interrupt remains a response-bound recovery obligation until resume commits the original tool call's result. An unclear response still resolves that interaction, so the Subagent asks again with a new call when needed. Workflow Owners and Moderators cannot create Human Interrupts. While awaiting response, other actionable inputs remain queued and do not wake the Subagent. Human-wait duration alone never escalates.
_Avoid_: Agent Request, concurrently mutable dependency, model-decided resolution, generic wait marker

**Undeclared Settlement**:
A Subagent settlement with no terminal action and no declared Agent or operation dependency or Human Interrupt. It is a protocol error represented as `waiting(undeclared)`, never `waiting(human)`. The runtime delivers one durable Undeclared Settlement Notice per continuous episode. Completion or activation cancellation closes the episode; failure preserves it as recovery-pending work without granting another correction allowance. A declared dependency resets the allowance only after new input satisfies it. Another undeclared settlement creates an Operational Incident, except that a Moderator uses the existing Owner Handoff fallback rather than creating nested moderation.
_Avoid_: Human waiting, elapsed-time stall, silent retry loop

**Undeclared Settlement Notice**:
The runtime-authored Protocol Notice that starts the single self-correction turn for an Undeclared Settlement episode. Its canonical payload lives in that episode's durable record so the correction is auditable and effectively once rather than a silent resume.
_Avoid_: Human prompt, passive status, repeated retry loop

**Terminal Message Completion**:
Completion of a subagent activation atomically combined with durable acceptance of its final outbound Signal or Answer through Post-Acceptance Disposition `complete`. It must satisfy the common Completion Gate.
_Avoid_: Duplicate completion-result message

**Standalone Completion**:
A trust-based, no-argument `agent_complete()` lifecycle action for a Subagent that has already communicated everything useful through any number of messages. The runtime enforces the common Completion Gate but does not identify, rank, or semantically validate a result message and does not prompt for confirmation. The Workflow Owner has no completion action.
_Avoid_: Result-message selection, semantic output validation, human confirmation

**Completion Gate**:
The mechanical safety conditions shared by fused and standalone Subagent completion. After the completion commit, the Subagent must have no unresolved incoming, outgoing, or recovery-pending Request obligations; no pending Human Interrupt or response-bound result awaiting resume; no accepted but undelivered actionable inbox inputs; and no unresolved operation, message acceptance, cancellation, ownership, or side-effect uncertainty. Unneeded work must be explicitly cancelled or abandoned first. Completion from an Undeclared Settlement correction turn atomically closes that episode. Concurrent inbound acceptance and completion are serialized, and the first durable commit determines whether completion succeeds or is blocked.
_Avoid_: Semantic result validation, silent obligation abandonment

**Activation Cancellation**:
An authorized, deliberate termination of an open Agent activation, recorded as `ended(cancelled)` rather than failure. The first durable completion or cancellation commit wins; a cancellation that cannot confirm termination remains in doubt rather than being reported as successful. Cancellation terminalizes any pending or response-bound Human Interrupt and any Undeclared Settlement episode, clears their attention projections, and marks an unconsumed human result stale and non-deliverable. Late human input cannot revive the cancelled work. Cancellation does not fabricate Answers, roll back completed work, or erase unresolved Request obligations.
_Avoid_: Failure, Request Cancellation, interruption

**Obligation-Preserving Cancellation**:
Cancellation always terminates its target and propagates through the target's descendants except for the transitive Request-dependency closure rooted at descendants with unresolved incoming Requests from outside the cancelled subtree. Those descendants and any descendants they currently depend on survive; Signals do not extend the closure. A survivor retains its own Requests and falls back to Workflow Owner supervision without transferring ownership, changing its Spawner identity, or creating a model turn or human-attention item. Every Agent that actually cancels still cancels its own outgoing Requests. After consuming a cancellation notice, a survivor may semantically determine that one of its retained dependencies is no longer needed and cancel that Request normally.
_Avoid_: Unconditional cascade, dependency-count lifetime, ownership transfer

**Steer Delivery**:
Delivery of an actionable message to an active Agent at the next LLM-turn boundary, after the current generation's tool calls finish and before the next generation begins. It does not abort generation or tool execution.
_Avoid_: Mid-turn injection, interrupt

**Deferred Delivery**:
Delivery that preserves an active Agent's current work until `agent_settled`, then starts a new Agent run with the actionable message. When the recipient is already waiting without a Human Interrupt, there is no active work to preserve and delivery may start immediately. A pending Human Interrupt keeps non-human input queued until its resume boundary.
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
An actionable message schedules immediate work for an ordinarily waiting Agent and enters an active Agent according to its delivery timing. A pending Human Interrupt instead queues non-human input until the human-resume boundary. Signals may queue for an interrupted Agent and are rejected for an ended Agent. A Request from a direct Spawner or Workflow Owner may restart interrupted work or create a new activation for an ended Agent; the same Request from a merely addressable peer is rejected.
_Avoid_: Addressability as resume authority, revival by Signal

**Independent Dependency Wakeup**:
Outside a Human Interrupt, delivery of any Answer starts its requester independently of other outstanding Requests. While a Human Interrupt is pending, Agent and operation inputs remain durably queued without waking the Subagent; the human response resumes the suspended tool call and makes queued eligible inputs available at that resumed model boundary. Transport provides no `all` or `any` synchronization.
_Avoid_: Barrier, join policy

**Dependency Deadlock**:
A closed component of the Request dependency graph in which every participating Agent is durably waiting exclusively on unresolved Requests within that same component, with no active participant, eligible queued actionable message, unresolved external or operation dependency, or recovery path capable of waking it. Request cycles are permitted and remain productive whenever any such source of progress exists.
_Avoid_: Cycle rejection, timeout-only diagnosis, active collaboration

**Autonomous Request Chain**:
A causal sequence in which an Answer also creates the next Response Requirement. Chain depth is observable but not capped by the protocol because hop count does not distinguish useful iteration from waste. Explicit Workflow resource policy may impose operational limits without changing Request semantics.
_Avoid_: Mandatory hop limit, conversation thread, deadlock proxy

**Operational Incident**:
One deduplicated episode where a runtime-confirmed operational condition blocks progress or safe cleanup, deterministic recovery or reconciliation is exhausted or cannot choose safely, and resolution requires temporary diagnostic visibility or lifecycle authority. Confirmed Dependency Deadlock, repeated Undeclared Settlement after its single correction, exhausted Automatic Recovery, and persistent cancellation, ownership, acceptance, or side-effect uncertainty after bounded reconciliation qualify. A Moderator's repeated Undeclared Settlement uses Owner Handoff fallback instead of creating another incident. Ordinary watch conditions, domain decisions, bare timeouts, and failures with recovery still available do not.
_Avoid_: Suspicion, attention item, timeout alert, domain escalation

**Incident Scope**:
The durable authority boundary of one Operational Incident. Its seed is the confirmed deadlock component, recovery-exhausted Agent, or Agents, Runs, messages, and operations implicated in persistent uncertainty. It includes seeded Agents' descendant subtrees and the transitive Agents connected by unresolved Requests in either direction. While the incident is open, newly implicated descendants and Request neighbors are added monotonically; the scope never expands through Signals, Addressability, or transcript references and never shrinks before termination. A Moderator cannot widen it manually.
_Avoid_: Workflow-wide access, mutable allowlist, message-history reachability

**Incident Visibility**:
Temporary read access granted to a Moderator over runtime records and configuration belonging to Agents within its Incident Scope. Transcript access follows ordinary Addressability, including explicit read-only access to the Workflow Owner transcript when the Moderator knows its ID. The Moderator initially receives only incident-facing Requests, relevant state, and a compact diagnostic summary, then pulls broader material on demand; nothing is injected automatically into either Moderator or Owner model context.
_Avoid_: Eager transcript injection, automatic context sharing, workflow-wide control

**Incident Control**:
Temporary operational authority granted to a Moderator over non-Owner Agents within its Incident Scope. It may message them, interrupt active work, restart interrupted work, create recovery activations for failed Agents, cancel activations, and declare Recovery Abandonment. Every mutation is durably attributed to the incident with the Moderator's rationale. It cannot answer or cancel a Request for its owner, declare another Agent complete, alter identity, transcripts, Agent Definitions, tools, or capabilities, spawn unrelated Agents, reparent Agents, transfer authority, widen Incident Scope, or control the Workflow Owner.
_Avoid_: Child Control, Workflow ownership, proxy Request authority, unrestricted administration

**Incident Brief**:
The compact durable initial context for a fresh Moderator. It identifies the incident and operational question; captures triggering evidence, the Incident Scope roster and dependency graph, incident-facing Requests, relevant runtime state, prior deterministic recovery attempts, applicable policies, authority boundaries, allowed verdicts, and termination conditions; and supplies durable pointers for broader inspection. The embedded state is a consistent creation-time snapshot, while inspection reads current live state. Large transcripts and unrelated history are not loaded eagerly.
_Avoid_: Full transcript dump, live state substitute, Workflow Owner context injection

**Moderator Outcome**:
The terminal result of one Moderator engagement. Every valid outcome completes the Moderator activation and atomically closes any Undeclared Settlement episode. `Operationally resolved` records the rationale, revokes the Moderator's authority, ends it, and closes the incident after runtime verification confirms the blocking or unsafe condition cleared. `Owner handoff` durably delivers a compact actionable escalation, revokes Moderator authority, ends it, transfers Incident Control to the Workflow Owner, and leaves the incident open as `owner-handled`. A voluntary outcome requires all Moderator-created Requests to be answered or cancelled. Failure or process loss is not an outcome.
_Avoid_: Silent completion, timeout closure, incident closure on handoff

**Owner Handoff**:
Transfer of an unresolved Operational Incident from its Moderator to the Workflow Owner after durable acceptance of an actionable escalation packet. The incident remains open and retains an `ACT` human-attention entry until the Owner records operational resolution, even after the packet reaches Owner context. Full diagnostics remain available through pointers rather than eager context injection.
_Avoid_: Incident resolution, passive notification, cleared human attention

**Incident Escalation**:
The exactly-once Protocol Notice that performs Owner Handoff. Its canonical compact packet lives in the incident record and reaches the Workflow Owner without creating an Answer obligation; it wakes a live idle Owner or remains durably pending until the user resumes a stopped Owner session. A voluntary Moderator remains active until durable acceptance, while failure fallback remains pending without a Moderator. Acceptance transfers Incident Control and ends the Moderator engagement; the Owner acts through incident operations rather than replying to the former Moderator.
_Avoid_: Signal, Request, Answer, passive runtime event

**Moderator Failure Fallback**:
The non-recursive recovery path when a Moderator cannot start, exhausts its single automatic replacement activation, repeats Undeclared Settlement after correction, or otherwise cannot produce a valid outcome. The runtime produces one deduplicated Owner Handoff packet containing the required decision, immediate risk, attempted actions, recommended choices and consequences, and pointers to the Incident Brief, Moderator transcript, and diagnostics. Until the Workflow Owner accepts it, the incident stays open without usable Moderator authority and surfaces as `ACT`; failed delivery remains durably pending rather than spawning another Moderator.
_Avoid_: Nested moderation, incident closure, repeated Owner alerts, full diagnostic injection

**Moderator Escalation Boundary**:
The boundary between operational judgment a Moderator may exercise and domain judgment reserved for the Workflow Owner and human. A Moderator may apply protocol invariants and existing Workflow policy, including Recovery Abandonment when evidence establishes that recovery is unavailable or policy-forbidden. It must use Owner Handoff when choices depend on task value or intent, change goals, priorities, policy, or risk tolerance, authorize uncertain irreversible side effects, sacrifice potentially valuable work without operational necessity, require action beyond Incident Scope or against the Owner, or cannot be mechanically verified as safe.
_Avoid_: Domain decision by Moderator, escalation of routine operational choice

**Risk Acceptance**:
The Workflow Owner's human-confirmed terminal decision to close an Operational Incident whose remaining uncertainty cannot be eliminated or mechanically verified. It records what remains unknown, possible consequences, attempted recovery or mitigation, and the chosen next action, then closes the incident and clears `ACT` without representing the uncertain operation as successful. A Moderator may recommend Risk Acceptance but cannot commit it.
_Avoid_: Successful resolution, Moderator verdict, silent dismissal, uncertainty erasure

**Overlapping Incidents**:
Distinct Operational Incidents whose Incident Scopes share one or more Agents. Scope overlap alone does not merge them, while repeated detection of the same condition episode is deduplicated. Their Moderators may inspect the same Agent, but mutating Incident Control operations use ownership fences and state preconditions so one conflicting operation commits and stale competitors must re-inspect. Each Moderator sees incident-control mutations affecting its scope and must independently verify and record resolution even when another incident's action clears its trigger.
_Avoid_: Scope-based incident merging, unfenced concurrent control, implicit incident closure

**Deadlock Escalation**:
One deduplicated runtime-generated operational incident for each confirmed Dependency Deadlock episode. A fresh incident-scoped Moderator handles it without waking the Workflow Owner when available; otherwise a compact decision packet wakes a live idle Owner or remains pending until the user resumes the Owner session. Full diagnostics remain outside Owner model context unless explicitly inspected. If neither route is available, the incident remains durably pending and surfaces through human attention until delivered exactly once.
_Avoid_: Repeated alerts, permanent supervisor, full Owner transcript injection

**Message Identity**:
One immutable, Workflow-unique ID assigned to every actionable message and Protocol Notice. A Request's message ID is also its Request ID. An Answer has its own ID and references the Request it closes; if that Answer also has a Response Requirement, its own ID is simultaneously the new Request ID.
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
Runtime-driven creation of exactly one replacement activation after failure when the Subagent retains recovery-pending Requests, a pending Human Interrupt or response-bound result awaiting resume, an Undeclared Settlement episode, or accepted pending inputs. A pending Human Interrupt continues to surface `DECIDE`; a bound result remains a recovery obligation without `DECIDE`; a preserved Undeclared Settlement episode receives no additional correction allowance. The episode ends when the replacement reaches durable declared settlement or completion; another failure exhausts automatic recovery and creates an Operational Incident for Moderator review. The retry count is not configurable by this protocol. Exhaustion does not abandon recovery, terminate Request obligations, or prune descendants.
_Avoid_: Unbounded restart, automatic abandonment, recovery without pending work

**Recovery Abandonment**:
An explicit incident verdict that the current failed work will no longer be recovered. Only an authorized Moderator or the Workflow Owner fallback may make it; retry exhaustion, elapsed time, and observation loss cannot. Abandonment terminally orphans incoming Requests, cancels outgoing Requests, terminalizes any pending or response-bound Human Interrupt and any Undeclared Settlement episode, clears their attention projections, marks unconsumed human results stale and non-deliverable, and applies obligation-preserving pruning to descendants. Late human input cannot revive the abandoned work. The failed Agent remains `ended(failed)` and may still receive unrelated future work through a new activation.
_Avoid_: Retry exhaustion, Agent deletion, implicit timeout

**Transcript Projection**:
An Agent-originated message is canonical in the sender's messaging tool call, a Human Interrupt question is canonical in its `agent_ask_user` call, its response is canonical as durable user input bound to that tool-call ID, and a Protocol Notice is canonical in its Request, incident, or Undeclared Settlement episode record. The recipient records actionable input in an Inbox Batch only when delivery commits it for model consumption. Queueing, acknowledgement, retry, and delivery-status changes remain runtime observability and are not copied into Agent or Workflow Owner model context.
_Avoid_: Audit-log duplication, status notification message

**Atomic Messaging Acceptance**:
A messaging operation succeeds only when its durable effects commit together: a Request records its dependency, an Answer closes its target Request, an Answer-and-Request also creates the new dependency, a Spawned Initial Request creates and starts its child, an authorized Request may create a new activation, and cancellation removes its dependency. Failure leaves none of those effects behind; delivery is not part of caller-facing acceptance.
_Avoid_: Partial send, delivery acknowledgement

**Recipient Unavailability**:
The pre-acceptance condition where no authorized Recipient Inbox Router can durably accept an Agent-originated actionable message. The send fails fast with no message, dependency, queue, retry, or lifecycle effects. An authorized Request may first create or recover an activation, but succeeds only after its Router becomes ready. Only an ambiguous acknowledgement after possible acceptance uses runtime-owned same-identity reconciliation; while unresolved, it is an operational dependency and no Post-Acceptance Disposition applies.
_Avoid_: Hidden retry queue, partial Request, elapsed-time acceptance

**Pending Message Pointer**:
A temporary durable recipient-inbox record that references either the canonical outbound messaging tool call in a sender transcript or a canonical Protocol Notice record. It stores only routing, ordering, and delivery metadata—not message payload—and is removed after delivery commits to the recipient transcript. The live queue is rebuilt from these pointers.
_Avoid_: Duplicate message store, permanent workflow audit log

**Recipient Inbox Router**:
The extension-owned routing module in a recipient Subagent Run or live Workflow Owner process. It validates inbound actionable inputs, finalizes ordinary message acceptance, owns recipient ordering, rebuilds the in-memory queue from Pending Message Pointers, selects delivery points, commits Inbox Batches to its session, and deduplicates delivery. Neither senders nor the Workflow Control Plane write recipient transcripts or relay Agent-originated payloads.
_Avoid_: Central workflow broker, sender-owned inbox
