# PROTOTYPE — minimal agent-facing control interface

Throwaway logic prototype for **Prototype the minimal agent-facing control API**.

## Question

What is the smallest coherent agent-facing interface for explicit completion, actionable messages, Answers, escalation, and post-message disposition across autonomous, human-in-the-loop, and reviewer–implementer workflows?

## Run

```sh
npm run prototype:agent-control
```

Choose a workflow, press `n` to advance it, and inspect the full Agent and Request state after every action. Press `v` to compare separate completion with a fused post-message completion disposition.

## Recommended candidate

Three model-facing tools backed by one deep Agent Control module:

```ts
agent_send({
  kind: "signal" | "request",
  to: AgentId,
  message: string,
  delivery?: "steer" | "deferred",
  answerDelivery?: "steer" | "deferred", // Request only; default: steer
  after: "continue" | "settle",
})

agent_answer({
  request: RequestId,
  outcome: "fulfilled" | "unable",
  message: string,
  after: "continue" | "settle",
})

agent_complete({ result: string })
```

Receipts report durable queue acceptance and runtime-generated identities. They do not report delivery, reading, understanding, or execution.

### Why three tools

- `agent_send` combines Signal and Request because both have a recipient and delivery choice.
- `agent_answer` remains separate because the Request determines authority, destination, correlation, and return delivery timing. Accepting a recipient here would weaken the established Answer contract.
- `agent_complete` remains separate because completion is the sole agent-declared terminal lifecycle action.

This is a small external interface over a deep implementation: workflow validation, IDs, durable acceptance, dependencies, inbox routing, delivery timing, wake scheduling, transcript projection, recovery, and ownership fencing remain behind the seam.

## Post-message disposition

`after` must be explicit:

- `continue` returns the receipt to the model and permits another model turn.
- `settle` returns a terminating tool result after durable acceptance, suppressing Pi's otherwise automatic follow-up model turn. At `agent_settled`, unresolved Requests produce `waiting(agent)`; otherwise the open activation becomes `waiting(human)` unless an operation dependency exists.

There is no standalone `wait`, `yield`, or `settle` tool.

Pi requires this field in practice. A normal tool result causes another model turn. Pi skips that turn only when every result in the tool batch is terminating, so “send a Request and just end the turn” is not an agent action unless disposition reaches the tool result.

## Escalation

Escalation is not another message kind or tool. It is an ordinary Request to an Agent whose address is already known—normally the direct Spawner.

There is no implicit Workflow Owner alias. A nested Agent can contact the Workflow Owner directly only if the Owner's session UUID was explicitly shared. Otherwise escalation follows the Spawner chain.

Human waiting also needs no tool. An Agent asks its question in ordinary assistant text and naturally settles with no terminal action or unresolved dependency.

## Deliberately excluded

- `inspect` and passive status queries: observability decision
- interrupt, lifecycle cancel, and resume: Child Control interface
- Request cancellation and deadlock policy: failure-handling decision
- caller-selected Message IDs: retry and transport detail
- Answer destination or delivery fields: already fixed by the Request
- opaque Answer Slots or Escalation Routes: conflict with established Request-ID correlation and UUID addressability
- global discovery, Workflow Owner relay, `interactive`, `autoExit`, and `wait`

## Interface alternatives considered

### One `perform` / `dispatch` tool

This has the fewest entry points but produces one large discriminated schema containing unrelated recipient-bearing sends, recipient-free Answers, cancellation, and completion. It is deeper as a TypeScript module than as a model-facing tool. The recommended interface keeps one internal dispatch seam while presenting three clearer tool adapters.

### Five semantic methods

Separate `signal`, `request`, `answer`, `cancelRequest`, and `complete` calls are highly discoverable, but Request cancellation belongs to the failure ticket and separate Signal/Request methods duplicate recipient and delivery concepts.

### Capability objects

Opaque Answer Slots and Escalation Routes make authority structural, but they replace decisions already made: an Agent's session UUID is its bearer address, and `inReplyTo` Request identity determines Answer authority and routing. Child Control remains a genuinely separate capability-bearing interface.

## Completion comparison

The recommended variant keeps `agent_complete` separate. A final Answer followed by completion may require one additional model turn. This preserves one explicit terminal lifecycle operation and leaves a safe, visible partial outcome if the Agent crashes after the Answer is accepted but before completion.

The prototype's comparison variant allows:

```ts
agent_answer({
  request,
  outcome: "fulfilled",
  message: "Approved.",
  after: { complete: "Review finished." },
})
```

That saves a model turn and can define message-acceptance-before-completion ordering, but it makes messaging a second completion entry point and enlarges every messaging disposition. The prototype exists to decide whether that optimization earns its interface cost.

## Workflow verdicts represented

- **Autonomous:** ordinary work needs only `agent_complete`; completion result replaces a redundant parent notification.
- **Human-in-the-loop:** a nested Agent sends a Request to its Spawner with `after: "settle"`; the Spawner asks the human with ordinary text, then Answers by Request ID.
- **Reviewer–implementer:** change requests use `after: "settle"` so the reviewer remains open and message-wakeable for another revision; final approval exposes the separate-versus-fused completion tradeoff.

## Prototype boundaries

This simulator performs no Pi, IPC, SQLite, transcript, or lifecycle operations. It renders the proposed calls and reduces them into visible Agent work states and Request states. It is intentionally throwaway and must not be merged into production code.
