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

**Signal**:
An actionable direct message that gives another Agent work or context without requiring an answer. Sending a Signal creates no dependency for the sender.
_Avoid_: Notification, event

**Request**:
An actionable direct message that requires one correlated answer from another Agent. Sending a Request records an Agent dependency and may select when its eventual Answer is delivered; if the dependency remains unresolved when the sender settles, the sender waits on it.
_Avoid_: Signal, passive status update

**Answer**:
The single terminal response that resolves one Request by its request ID. An Answer reports either fulfillment or explicit inability and uses the delivery timing chosen by the requester, defaulting to Steer Delivery; clarification and follow-up use new Requests rather than partial responses or conversation threads.
_Avoid_: Progress update, uncorrelated reply

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
An actionable message schedules immediate work for a waiting Agent and enters an active Agent according to its delivery timing. It may queue for an interrupted Agent but cannot resume it, and it is rejected for an Agent whose activation has ended; messaging alone grants no lifecycle authority.
_Avoid_: Implicit activation, interrupt cancellation

**Independent Dependency Wakeup**:
Delivery of any Answer starts its requester independently of other outstanding Requests. Unresolved Requests remain dependencies, and the Agent may return to `waiting(agent)` after processing the delivered Answer; transport provides no `all` or `any` synchronization.
_Avoid_: Barrier, join policy

**Message Identity**:
One immutable, Workflow-unique ID assigned to every actionable message. A Request's message ID is also its request ID; an Answer has its own message ID and references that Request with `inReplyTo`. Signals have no correlation or conversation-thread identity beyond their own message ID.
_Avoid_: Separate correlation ID, thread ID

**Answer Authority**:
Only the Agent addressed by a Request may answer it. The Answer's `inReplyTo` identity determines the requester and return route, so the responder supplies no separate destination; lifecycle controllers cannot answer on another Agent's behalf.
_Avoid_: Proxy answer, caller-selected reply destination

**Answer Finality**:
The first durably queued Answer closes a Request's answer slot. Retrying the same Answer ID is idempotent, while a different later Answer is rejected; subsequent corrections require a new Signal or Request.
_Avoid_: Answer revision, multiple terminal answers

**Request Cancellation**:
A terminal operation available only to the original requester while its Request remains unresolved. Durable cancellation removes the requester's dependency and sends the recipient an actionable cancellation notice, but cannot undo work already performed. The first durable Answer or cancellation wins their race.
_Avoid_: Answer, work rollback

**Transcript Projection**:
The sender records its messaging tool call and immediate result, while the recipient records an Inbox Batch only when delivery commits it for model consumption. Queueing, acknowledgement, retry, and delivery-status changes remain runtime observability and are not copied into Agent or Workflow Owner model context.
_Avoid_: Audit-log duplication, status notification message

**Atomic Messaging Acceptance**:
A messaging operation succeeds only when its durable effects commit together: queueing a Request also records its dependency, queueing an Answer also marks its Request answered, and cancelling a Request also removes its dependency. Failure leaves none of those effects behind; delivery is not part of caller-facing acceptance.
_Avoid_: Partial send, delivery acknowledgement

**Pending Message Pointer**:
A temporary durable recipient-inbox record that references the canonical outbound messaging tool call in the sender transcript. It stores only routing, ordering, and delivery metadata—not message payload—and is removed after delivery commits to the recipient transcript. The live queue is rebuilt from these pointers.
_Avoid_: Duplicate message store, permanent workflow audit log

**Recipient Inbox Router**:
The extension-owned routing module in an Agent Run that validates inbound messages, owns that Agent's Pending Message Pointers and acceptance ordering, rebuilds its in-memory queue, selects delivery points, commits Inbox Batches to its session, and deduplicates delivery. The sender and Workflow Owner do not write the recipient transcript or relay its messages.
_Avoid_: Central workflow broker, sender-owned inbox
