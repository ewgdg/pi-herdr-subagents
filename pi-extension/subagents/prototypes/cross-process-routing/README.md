# PROTOTYPE — cross-process routing and delivery

Throwaway logic prototype for **Prototype cross-process routing and delivery guarantees**.

## Question

Does a recipient-finalized acceptance contract, initially implemented with local IPC, SQLite transactions, and fenced Agent Run ownership, remain coherent through lost acknowledgements, process crashes, restart recovery, duplicate resumes, the transcript/pointer crash window, and permanently completed recipients?

This simulator performs no real socket, SQLite, filesystem, or Pi operations. Its pure reducer models the proposed contract while the terminal shell exposes every relevant state transition.

## Run

```sh
npm run prototype:routing
```

Enter one action key and press Return.

## Useful scenarios

### Lost acceptance acknowledgement

1. `l` — lose the next acknowledgement.
2. `s` — send a Steer message. The pointer commits, but the sender sees `acceptance_unknown`.
3. `r` — retry. The original sequence is returned without creating another pointer.

### Recipient process crash

1. `s` — accept a message.
2. `c` — crash the process. The accepted pointer remains.
3. `o` — authorized resume. The queue is rebuilt and one wake is scheduled.
4. `w` — start the wake and commit the Inbox Batch.

### Transcript/pointer crash window

1. `s` — accept a message.
2. `k` — arm a crash after transcript commit.
3. `b` — commit the batch, then crash before deleting its pointer.
4. `o` — resume. Recovery sees transcript evidence and removes the pointer without reinjection.

### Duplicate resume prevention

1. `u` — attempt a second resume while the first Agent Run owns the session.
2. Observe `SessionOwned`; the existing run remains unchanged.
3. `c`, `o`, then `f` — a superseded owner tries to commit with the old epoch and receives `OwnershipLost`.

### Process disappearance versus completed recipient

1. `c`, then `s` — the canonical message is rejected as retryable while no Router is reachable.
2. `o`, then `r` — the same canonical message can be accepted after authorized resume.
3. `z`, then `e`, then `s` — a completed activation rejects the new message permanently.

## Candidate contract represented

- The recipient's atomic pointer transaction is the acceptance commit point.
- A **Durable Acceptance Receipt** reports `queued`, message identity, and acceptance sequence only after that commit.
- Direct per-recipient local IPC: Unix domain sockets on POSIX and named pipes on Windows.
- Versioned, length-prefixed request/reply frames; neither Pi RPC nor gRPC is part of the initial transport.
- SQLite transactions contain pointers and protocol state, never payload copies.
- Sender transcript tool call is the canonical payload.
- `queued` acknowledges durable acceptance, not delivery.
- At-least-once transport attempts with idempotent acceptance.
- Effectively exactly-once recipient transcript commit by message ID.
- Kernel-backed exclusive ownership prevents duplicate Agent Runs; a monotonic fencing epoch rejects stale owners.
- A process crash is recoverable; a completed activation is unavailable to messaging.
- Accepted work emits passive recovery-needed state; an authorized Spawner or Workflow Owner starts the replacement Agent Run.
- Exactly-once model reasoning and external side effects are explicitly not promised.

## Transport comparison

The prototype uses the recommended initial Adapter: local IPC plus SQLite.

- **Local IPC + SQLite:** one recipient-finalized transaction gives clear acceptance, sequencing, request arbitration, and receipt reconciliation. Node can present one transport seam backed by Unix domain sockets on POSIX and named pipes on Windows. It has the strongest locality and the least custom recovery code.
- **Filesystem rendezvous + commit markers:** viable without SQLite, but requires file and directory syncing, polling fallback, prepared-transaction cleanup, source pins, and a custom multi-record commit protocol.
- **Staged per-Agent stores:** preserves future deployment flexibility through recipient-finalized commit records, but introduces in-doubt preparations. Keep it at contract level until a second deployment topology makes the seam real.
- **gRPC:** reconsider only if remote-host or multi-language Agents enter scope; it does not replace acceptance persistence, deduplication, recovery, or ownership fencing.

The caller Interface should not expose which Adapter is used. Hidden transport failover is rejected because it obscures diagnosis and ordering.

The architecture is actor-model inspired: Agents have durable identities, Agent Runs are temporary activations, Recipient Inbox Routers are mailboxes, and lifecycle authorities supervise recovery. Actor terminology remains an analogy rather than the domain Interface.
