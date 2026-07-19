# PROTOTYPE — minimal agent-facing messaging interface

Throwaway logic prototype for **Prototype the minimal agent-facing control API**.

## Question

Can one explicit message operation cover Signal, Request, Answer, spawn-with-initial-work, authorized automatic resume, post-acceptance settlement, and completion without exposing transport or lifecycle machinery?

## Run

```sh
npm run prototype:agent-control
```

Controls:

- `1` — autonomous worker
- `2` — human-in-the-loop escalation
- `3` — reviewer–implementer with automatic resume
- `n` — advance one step
- `z` — reset
- `q` — quit

The simulator renders every call and the complete Agent, activation, waiting-reason, and Request state after each transition.

## Candidate interface

One model-facing communication tool backed by a deep Agent Messaging module:

```ts
agent_send({
  target:
    | { agent: AgentId }
    | { spawn: SpawnSpec }
    | { request: RequestId },

  message: string,

  response?:
    | "none"
    | {
        required: true,
        delivery: "steer" | "deferred",
      },

  delivery?: "steer" | "deferred",

  onAccepted:
    | "continue"
    | "settle"
    | "complete",
})
```

This is represented internally as a discriminated union, not a permissive bag of optional fields. Each target branch admits only its valid fields.

## Derivation rules

### Signal

An existing Agent target with no Answer requirement:

```ts
agent_send({
  target: { agent: reviewerId },
  message: "Revision def456 is ready.",
  response: "none",
  delivery: "steer",
  onAccepted: "continue",
})
```

### Request

An existing Agent target with a required Answer:

```ts
agent_send({
  target: { agent: reviewerId },
  message: "Review revision def456.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle",
})
```

The `response.delivery` value is the eventual Answer's delivery timing. The Request creates one sender dependency atomically with acceptance.

### Spawn with initial Request

```ts
agent_send({
  target: {
    spawn: {
      agent: "worker",
      name: "Pagination worker",
    },
  },
  message: "Implement pagination and run the tests.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle",
})
```

The operation atomically creates the Agent and Spawner relationship, establishes Child Control, records the Request dependency, places the Request in the child's initial context, and starts its first activation. There is no empty Agent followed by a second task-message call.

A spawn target must require an Answer so the initial work has an explicit result route.

### Answer

A Request target identifies a terminal Answer:

```ts
agent_send({
  target: { request: requestId },
  message: "Implemented pagination; all tests pass.",
  onAccepted: "complete",
})
```

The Request determines Answer authority, destination, correlation, and return delivery timing. The content remains one plain `message` string. There is no `outcome`, `result`, `unable`, recipient, response, or delivery field on this branch.

Every accepted Answer terminally closes its Request. An inability is expressed directly:

```ts
agent_send({
  target: { request: requestId },
  message: "Cannot access the deployment credentials.",
  onAccepted: "complete",
})
```

The requester wakes and interprets the content. The runtime does not parse prose into work-success categories.

## Post-acceptance disposition

`onAccepted` is evaluated only after durable message acceptance succeeds:

- `continue` — return the acceptance receipt and permit another model turn.
- `settle` — suppress the automatic follow-up model turn and derive typed waiting from unresolved dependencies.
- `complete` — atomically accept the final outbound Signal or Answer and end the sender's activation.

Acceptance failure never settles or completes the sender.

`complete` is valid only for a Signal or Answer. It is invalid for a Request because a Request creates a new unresolved dependency.

There is no `agent_complete`, `agent.complete`, standalone wait, yield, or settle operation. Every completing subagent sends a useful final outbound Signal or Answer.

## Automatic resume on Request

A Request to an existing Agent behaves according to recipient state and caller authority:

| Recipient | Signal | Request |
|---|---|---|
| active | queue/deliver | queue/deliver |
| waiting | wake | wake |
| interrupted | queue without resume | create/start work only with Child Control or Workflow Owner authority |
| ended | reject | create a new activation only with Child Control or Workflow Owner authority |

An ended activation remains immutable. Authorized Request delivery creates a new activation for the same durable Agent/session.

Activation creation, Request acceptance, dependency creation, and ownership fencing must commit as one idempotent operation. Retrying the same canonical tool call cannot create duplicate activations.

Addressability alone never grants automatic resume. An unauthorized peer Request to an interrupted or ended Agent is rejected.

## Why one tool remains coherent

Unlike a generic `agent_control` envelope, `agent_send` performs exactly one conceptual operation: send one actionable message. Target and response shape derive the message kind and any necessary recipient creation or activation.

The deep implementation hides:

- Message and Request identity allocation
- Workflow membership and authority validation
- spawn and activation creation
- Child Control checks
- atomic Request dependency creation
- Answer authority and routing
- durable recipient acceptance
- Pending Message Pointers
- Steer and Deferred delivery
- typed waiting and wake scheduling
- completion preconditions and commitment
- retries, crash recovery, and Agent Run ownership fencing

Transport, SQLite, transcript paths, processes, panes, and session files do not cross the interface.

## Workflow verdicts represented

- **Autonomous:** spawn carries the initial Request; the worker's terminal Answer is also completion.
- **Human-in-the-loop:** a nested Request wakes the Spawner; the Spawner asks the human with ordinary assistant text and returns the decision through a plain Answer.
- **Reviewer–implementer:** the Implementer spawns a Reviewer through the first review Request; a later authorized Request automatically starts a new Reviewer activation after its earlier completion.

## Prototype boundaries

This simulator performs no Pi, IPC, SQLite, transcript, process, or lifecycle operations. It reduces proposed calls into visible domain state. It is intentionally throwaway and must not be merged into production code.
